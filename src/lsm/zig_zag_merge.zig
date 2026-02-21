const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const stdx = @import("stdx");

const Direction = @import("../direction.zig").Direction;
const Pending = error{Pending};

/// ZigZag merge join.
/// Resources:
/// https://github.com/objectify/objectify/wiki/Concepts#indexes.
/// https://youtu.be/AgaL6NGpkB8?t=26m10s
pub fn ZigZagMergeIteratorType(
    comptime Context: type,
    comptime Key: type,
    comptime Value: type,
    comptime key_from_value: fn (*const Value) callconv(.@"inline") Key,
    comptime streams_max: u32,
    /// Peek the next key in the stream identified by `stream_index`.
    /// For example, `peek(stream_index=2)` returns `user_streams[2][0]`.
    /// Returns `Pending` if the stream was consumed and must be refilled
    /// before calling `peek()` again.
    /// Returns null if the stream was fully consumed and reached the end.
    comptime stream_peek: fn (
        context: *Context,
        stream_index: u32,
    ) Pending!?Key,
    /// Consumes the current value and moves the stream identified by `stream_index`.
    /// Pop is always called after `peek()`, it is not expected that the stream be `Empty`
    /// or `Pending`.
    comptime stream_pop: fn (context: *Context, stream_index: u32) Value,
    /// Probes the stream identified by `stream_index` causing it to move to the next value such
    /// that `value.key >= probe_key` (ascending) or `value.key <= probe_key` (descending).
    /// Should not be called when the current key already matches the probe.
    /// The stream may become empty or `Pending` _after_ probing.
    comptime stream_probe: fn (context: *Context, stream_index: u32, probe_key: Key) void,
) type {
    return struct {
        const ZigZagMergeIterator = @This();

        context: *Context,
        streams_count: u32,
        direction: Direction,
        probe_key_previous: ?Key = null,
        key_popped: ?Key = null,
        key_peeked: ?Key = null,

        /// At least two scans are required for zig-zag merge.
        pub fn init(
            context: *Context,
            streams_count: u32,
            direction: Direction,
        ) ZigZagMergeIterator {
            assert(streams_count <= streams_max);
            assert(streams_count > 1);

            return .{
                .context = context,
                .streams_count = streams_count,
                .direction = direction,
            };
        }

        // Resets the iterator when the underlying streams are moved.
        // It's not necessary for ZigZagMerge, but it follows the same API for all MergeIterators.
        pub fn reset(it: *ZigZagMergeIterator) void {
            _ = it;
        }

        pub fn pop(it: *ZigZagMergeIterator) Pending!?Value {
            const key = try it.peek_key() orelse
                return null;

            if (it.key_popped) |previous| {
                // Duplicate values are not expected.
                assert(it.direction.cmp(previous, .@"<", key));
            }
            it.key_popped = key;

            const value = stream_pop(it.context, 0);
            assert(key_from_value(&value) == key);
            for (1..it.streams_count) |stream_index| {
                const value_other = stream_pop(it.context, @intCast(stream_index));
                assert(key_from_value(&value_other) == key);

                // Differently from K-way merge, there's no precedence between streams
                // in Zig-Zag merge. It's assumed that all streams will produce the same
                // value during a key intersection.
                assert(stdx.equal_bytes(Value, &value, &value_other));
            }

            return value;
        }

        /// Zig zig-zag join algorithm: finding the next _common_ key.
        /// Algorithm is conflict driven --- if any two streams disagree
        /// on the next key, one of the streams can be advanced (probed).
        /// In particular, if any stream is empty, there are no common keys.
        /// Converesly, the algorithm finishes when there is no disagreement:
        /// - some streams are pending (need IO to fetch next key from disk),
        /// - _all_ other streams agree on the key.
        ///
        /// The schedule to interrogate the streams is arbitrary. We use
        /// simple round-robin: going in circles, reseting the "tour" every
        /// time a conflict is detected, until we complete a full circle
        /// without a reset. The schedule ensures that any pending stream is
        /// probed with our best guess for optimal IO.
        fn peek_key(it: *ZigZagMergeIterator) Pending!?Key {
            assert(it.streams_count > 1);
            assert(it.streams_count <= streams_max);

            // NB: We could start with `it.key_peeked`, but starting
            // from zero tightens assertions on the underlying streams.
            var candidate: Key = switch (it.direction) {
                .ascending => 0,
                .descending => std.math.maxInt(Key),
            };

            var tour_index: u32 = 0;
            var tour_total: u32 = 0;
            var tour_equal: u32 = 0;
            var tour_pending: u32 = 0;

            // Ideally, the bound should be computed dynamically,
            // as a sum of streams' bounds, but even a big magic
            // constant is better than `maxInt(u32)`.
            const safety_bound = 10_000_000;
            for (0..safety_bound) |index| {
                tour_index = @as(u32, @intCast(index)) % it.streams_count;

                assert(tour_total == tour_equal + tour_pending);
                assert(tour_total <= it.streams_count);
                if (tour_total == it.streams_count) break;

                stream_probe(it.context, tour_index, candidate);
                const key: Key = stream_peek(it.context, tour_index) catch |err| {
                    switch (err) {
                        error.Pending => {
                            tour_total += 1;
                            tour_pending += 1;
                            continue;
                        },
                    }
                } orelse
                    // An empty stream short-circuits the entire thing.
                    return null;

                assert(it.direction.cmp(candidate, .@"<=", key));
                if (it.key_peeked) |key_peeked| {
                    assert(it.direction.cmp(key_peeked, .@"<=", key));
                }

                if (it.direction.cmp(candidate, .@"<", key)) {
                    // The guess turned out to be wrong, reset the tour.
                    // Importantly, we will re-probe all "preceding"
                    // streams if we are to complete this new tour.
                    candidate = key;
                    tour_total = 1;
                    tour_equal = 1;
                    tour_pending = 0;
                } else {
                    assert(candidate == key);
                    tour_total += 1;
                    tour_equal += 1;
                }
                if (tour_total == it.streams_count) break;
            } else @panic("zig-zag loop outrun safety counter");
            assert(tour_total == tour_equal + tour_pending);
            assert(tour_total == it.streams_count);

            if (tour_pending > 0) return error.Pending;

            if (it.key_peeked) |key_peeked| {
                assert(it.direction.cmp(key_peeked, .@"<=", candidate));
            }
            it.key_peeked = candidate;
            return candidate;
        }
    };
}

fn TestContextType(comptime streams_max: u32) type {
    const testing = std.testing;

    return struct {
        const TestContext = @This();

        // Using `u128` simplifies the fuzzer, avoiding undesirable matches
        // and duplicate elements when generating random values.
        const Key = u128;
        const Value = u128;

        inline fn key_from_value(value: *const Value) Key {
            return value.*;
        }

        streams: [streams_max][]const Value,
        direction: Direction,

        fn stream_peek(
            context: *const TestContext,
            stream_index: u32,
        ) Pending!?Key {
            const stream = context.streams[stream_index];
            if (stream.len == 0) return null;
            return switch (context.direction) {
                .ascending => key_from_value(&stream[0]),
                .descending => key_from_value(&stream[stream.len - 1]),
            };
        }

        fn stream_pop(context: *TestContext, stream_index: u32) Value {
            const stream = context.streams[stream_index];

            switch (context.direction) {
                .ascending => {
                    context.streams[stream_index] = stream[1..];
                    return stream[0];
                },
                .descending => {
                    context.streams[stream_index] = stream[0 .. stream.len - 1];
                    return stream[stream.len - 1];
                },
            }
        }

        fn stream_probe(context: *TestContext, stream_index: u32, probe_key: Key) void {
            while (true) {
                const key = stream_peek(context, stream_index) catch |err| switch (err) {
                    error.Pending => unreachable,
                } orelse return;

                if (switch (context.direction) {
                    .ascending => key >= probe_key,
                    .descending => key <= probe_key,
                }) break;

                const value = stream_pop(context, stream_index);
                assert(key == key_from_value(&value));
            }
        }

        fn merge(
            streams: []const []const Value,
            expect: []const Value,
        ) !void {
            const ZigZagMerge = ZigZagMergeIteratorType(
                TestContext,
                Key,
                Value,
                key_from_value,
                streams_max,
                stream_peek,
                stream_pop,
                stream_probe,
            );

            for (std.enums.values(Direction)) |direction| {
                var actual = std.ArrayList(Value).init(testing.allocator);
                defer actual.deinit();

                var context: TestContext = .{
                    .streams = undefined,
                    .direction = direction,
                };
                for (streams, 0..) |stream, i| {
                    context.streams[i] = stream;
                }

                var it = ZigZagMerge.init(&context, @intCast(streams.len), direction);
                while (try it.pop()) |value| {
                    try actual.append(value);
                }

                if (direction == .descending) std.mem.reverse(Value, actual.items);
                try testing.expectEqualSlices(Value, expect, actual.items);
            }
        }

        fn fuzz(prng: *stdx.PRNG, stream_key_count_max: u32) !void {
            const allocator = testing.allocator;
            var streams: [streams_max][]Value = undefined;

            const streams_buffer = try allocator.alloc(Value, streams_max * stream_key_count_max);
            defer allocator.free(streams_buffer);

            const intersection_buffer = try allocator.alloc(Value, stream_key_count_max);
            defer allocator.free(intersection_buffer);

            const intersection_len_min = 5;
            for (2..streams_max + 1) |streams_count| {
                var stream_len_min: u32 = stream_key_count_max;
                for (0..streams_count) |stream_index| {
                    const len = prng.range_inclusive(
                        u32,
                        intersection_len_min,
                        stream_key_count_max,
                    );
                    if (len < stream_len_min) stream_len_min = len;

                    streams[stream_index] =
                        streams_buffer[stream_index * stream_key_count_max ..][0..len];
                }

                const intersection = intersection_buffer[0..prng.range_inclusive(
                    u32,
                    intersection_len_min,
                    stream_len_min,
                )];
                assert(intersection.len >= intersection_len_min and
                    intersection.len <= stream_len_min);

                fuzz_make_intersection(
                    prng,
                    streams[0..streams_count],
                    intersection,
                );

                // Positive space.
                try merge(streams[0..streams_count], intersection);

                // Negative space: disjoint stream.
                {
                    var dummy: [10]Value = .{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
                    const replaced = streams[streams_count - 1];
                    defer streams[streams_count - 1] = replaced;

                    streams[streams_count - 1] = &dummy;
                    try merge(streams[0..streams_count], &.{});
                }

                // Negative space: empty stream.
                {
                    const empty: [0]Value = .{};
                    const replaced = streams[streams_count - 1];
                    defer streams[streams_count - 1] = replaced;

                    streams[streams_count - 1] = &empty;
                    try merge(streams[0..streams_count], &.{});
                }
            }
        }

        fn fuzz_make_intersection(
            prng: *stdx.PRNG,
            streams: []const []Value,
            intersection: []Value,
        ) void {
            const less_than = struct {
                fn less_than(_: void, lhs: Value, rhs: Value) bool {
                    return lhs < rhs;
                }
            }.less_than;

            // Starting with the values we want to be the intersection:
            prng.fill(mem.sliceAsBytes(intersection));
            std.mem.sort(
                Value,
                intersection,
                {},
                less_than,
            );

            // Then injecting the intersection into the each stream and filling the rest with
            // random values:
            for (streams) |stream| {
                assert(intersection.len <= stream.len);
                stdx.copy_disjoint(.exact, Value, stream[0..intersection.len], intersection);
                if (stream.len > intersection.len) {
                    prng.fill(mem.sliceAsBytes(stream[intersection.len..]));
                }
                std.mem.sort(
                    Value,
                    stream,
                    {},
                    less_than,
                );
            }
        }
    };
}

test "zig_zag_merge: unit" {
    const Context = TestContextType(10);

    // Equal streams:
    try Context.merge(
        &[_][]const Context.Value{
            &.{ 1, 2, 3, 4, 5 },
            &.{ 1, 2, 3, 4, 5 },
            &.{ 1, 2, 3, 4, 5 },
        },
        &.{ 1, 2, 3, 4, 5 },
    );

    // Disjoint streams:
    try Context.merge(
        &[_][]const Context.Value{
            &.{ 1, 3, 5, 7, 9 },
            &.{ 2, 4, 6, 8, 10 },
        },
        &.{},
    );

    // Equal and disjoint streams:
    try Context.merge(
        &[_][]const Context.Value{
            &.{ 1, 3, 5, 7, 9 },
            &.{ 1, 3, 5, 7, 9 },
            &.{ 2, 4, 6, 8, 10 },
            &.{ 2, 4, 6, 8, 10 },
        },
        &.{},
    );

    // Intersection with an empty stream:
    try Context.merge(
        &[_][]const Context.Value{
            &.{ 2, 4, 6, 8, 10 },
            &.{ 2, 4, 6, 8, 10 },
            &.{},
        },
        &.{},
    );

    // Partial intersection:
    try Context.merge(
        &[_][]const Context.Value{
            &.{ 1, 2, 3, 4, 5 },
            &.{ 2, 3, 4, 5, 6 },
            &.{ 3, 4, 5, 6, 7 },
            &.{ 4, 5, 6, 7, 8 },
        },
        &.{ 4, 5 },
    );

    // Intersection with streams of different sizes:
    try Context.merge(
        &[_][]const Context.Value{
            // {1, 2, 3, ..., 1000}.
            comptime blk: {
                @setEvalBranchQuota(2_000);
                var array: [1000]Context.Value = undefined;
                for (0..1000) |i| array[i] = @intCast(i + 1);
                break :blk stdx.comptime_slice(&array, array.len);
            },
            // {10, 20, 30, ..., 1000}.
            comptime blk: {
                var array: [100]Context.Value = undefined;
                for (0..100) |i| array[i] = @intCast(10 * (i + 1));
                break :blk stdx.comptime_slice(&array, array.len);
            },
            // {1, 10, 100, 1000, ..., 10 ^ 10}.
            comptime blk: {
                var array: [10]Context.Value = undefined;
                for (0..10) |i| array[i] = std.math.pow(Context.Value, 10, i);
                break :blk stdx.comptime_slice(&array, array.len);
            },
        },
        &.{ 10, 100, 1000 },
    );

    // Sparse matching values: {1, 2, 3, ..., 100} ∩ {100, 101, 102, ..., 199} = {100}.
    try Context.merge(
        &[_][]const Context.Value{
            // {1, 2, 3, ..., 100}.
            comptime blk: {
                var array: [100]Context.Value = undefined;
                for (0..100) |i| array[i] = @intCast(i + 1);
                break :blk stdx.comptime_slice(&array, array.len);
            },
            // {100, 101, 102, ..., 199}.
            comptime blk: {
                var array: [100]Context.Value = undefined;
                for (0..100) |i| array[i] = @intCast(i + 100);
                break :blk stdx.comptime_slice(&array, array.len);
            },
        },
        &.{100},
    );

    // Sparse matching values: {100, 101, 102, ..., 199} ∩ {1, 2, 3, ..., 100}  = {100}.
    try Context.merge(
        &[_][]const Context.Value{
            // {100, 101, 102, ..., 199}.
            comptime blk: {
                var array: [100]Context.Value = undefined;
                for (0..100) |i| array[i] = @intCast(i + 100);
                break :blk stdx.comptime_slice(&array, array.len);
            },
            // {1, 2, 3, ..., 100}.
            comptime blk: {
                var array: [100]Context.Value = undefined;
                for (0..100) |i| array[i] = @intCast(i + 1);
                break :blk stdx.comptime_slice(&array, array.len);
            },
        },
        &.{100},
    );
}

test "zig_zag_merge: fuzz" {
    const seed = std.crypto.random.int(u64);
    errdefer std.debug.print("\nTEST FAILED: seed = {}\n", .{seed});

    var prng = stdx.PRNG.from_seed(seed);
    try TestContextType(32).fuzz(&prng, 256);
}
