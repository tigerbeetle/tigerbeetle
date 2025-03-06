const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");

const Direction = @import("../direction.zig").Direction;

/// ZigZag merge join.
/// Resources:
/// https://github.com/objectify/objectify/wiki/Concepts#indexes.
/// https://youtu.be/AgaL6NGpkB8?t=26m10s
pub fn ZigZagMergeIteratorType(
    comptime Context: type,
    comptime Key: type,
    comptime Value: type,
    comptime key_from_value: fn (*const Value) callconv(.Inline) Key,
    comptime streams_max: u32,
    /// Peek the next key in the stream identified by `stream_index`.
    /// For example, `peek(stream_index=2)` returns `user_streams[2][0]`.
    /// Returns `Drained` if the stream was consumed and must be refilled
    /// before calling `peek()` again.
    /// Returns `Empty` if the stream was fully consumed and reached the end.
    comptime stream_peek: fn (
        context: *Context,
        stream_index: u32,
    ) error{ Empty, Drained }!Key,
    /// Consumes the current value and moves the stream identified by `stream_index`.
    /// Pop is always called after `peek()`, it is not expected that the stream be `Empty`
    /// or `Drained`.
    comptime stream_pop: fn (context: *Context, stream_index: u32) Value,
    /// Probes the stream identified by `stream_index` causing it to move to the next value such
    /// that `value.key >= probe_key` (ascending) or `value.key <= probe_key` (descending).
    /// Should not be called when the current key already matches the probe.
    /// The stream may become `Empty` or `Drained` _after_ probing.
    comptime stream_probe: fn (context: *Context, stream_index: u32, probe_key: Key) void,
) type {
    return struct {
        const ZigZagMergeIterator = @This();
        const BitSet = std.bit_set.IntegerBitSet(streams_max);

        context: *Context,
        streams_count: u32,
        direction: Direction,
        probe_key_previous: ?Key = null,
        key_popped: ?Key = null,

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

        pub fn pop(it: *ZigZagMergeIterator) error{Drained}!?Value {
            while (try it.peek_key()) |key| {
                const value = stream_pop(it.context, 0);
                assert(key_from_value(&value) == key);
                for (1..it.streams_count) |stream_index| {
                    const value_other = stream_pop(it.context, @intCast(stream_index));
                    assert(key_from_value(&value_other) == key);

                    if (constants.verify) {
                        // Differently from K-way merge, there's no precedence between streams
                        // in Zig-Zag merge. It's assumed that all streams will produce the same
                        // value during a key intersection.
                        assert(stdx.equal_bytes(Value, &value, &value_other));
                    }
                }

                if (it.key_popped) |previous| {
                    switch (std.math.order(previous, key)) {
                        .lt => assert(it.direction == .ascending),
                        // Duplicate values are not expected.
                        .eq => unreachable,
                        .gt => assert(it.direction == .descending),
                    }
                }
                it.key_popped = key;

                return value;
            }

            return null;
        }

        fn peek_key(it: *ZigZagMergeIterator) error{Drained}!?Key {
            assert(it.streams_count <= streams_max);
            assert(it.streams_count > 1);

            const key_min: Key = switch (it.direction) {
                .ascending => 0,
                .descending => std.math.maxInt(Key),
            };

            var drained: BitSet = BitSet.initEmpty();
            var probe_key: Key = key_min;

            var probing: BitSet = BitSet.initFull();
            while (probing.count() > 0) {
                // Looking into all non-drained streams for a match, while accumulating
                // the most ahead key to probe the streams behind.
                probing = BitSet.initEmpty();
                for (0..it.streams_count) |stream_index| {
                    if (drained.isSet(stream_index)) continue;

                    const key = stream_peek(it.context, @intCast(stream_index)) catch |err| {
                        switch (err) {
                            // Return immediately on empty streams.
                            // If any one stream is empty, then there can be no value remaining
                            // in the intersection.
                            error.Empty => return null,
                            // Skipping `Drained` streams. The goal is to match all buffered streams
                            // first so that the drained ones can read from a narrower key range.
                            error.Drained => {
                                drained.set(stream_index);
                                continue;
                            },
                        }
                    };

                    // The stream cannot regress.
                    assert(
                        it.probe_key_previous == null or
                            key == it.probe_key_previous.? or
                            it.key_ahead(.{
                            .key_after = key,
                            .key_before = it.probe_key_previous.?,
                        }),
                    );

                    // The keys matches, continuing to the next stream.
                    if (key == probe_key) continue;

                    if (it.key_ahead(.{ .key_after = key, .key_before = probe_key })) {
                        // The stream is ahead, it will be the probe key,
                        // meaning all streams before must be probed.
                        probe_key = key;

                        // Setting all previous streams as `true` except the drained ones.
                        probing.setRangeValue(.{ .start = 0, .end = stream_index }, true);
                        probing.setIntersection(drained.complement());
                        assert(!probing.isSet(stream_index));
                    } else {
                        // The stream is behind and needs to be probed.
                        probing.set(stream_index);
                    }
                }

                // Probing the buffered streams that did not match the key.
                var probing_iterator = probing.iterator(.{ .kind = .set });
                while (probing_iterator.next()) |stream_index| {
                    stream_probe(it.context, @intCast(stream_index), probe_key);

                    const key = stream_peek(it.context, @intCast(stream_index)) catch |err| {
                        switch (err) {
                            error.Empty => return null,
                            error.Drained => {
                                drained.set(stream_index);
                                probing.unset(stream_index);
                                continue;
                            },
                        }
                    };

                    // After probed, the stream must either match the key or be ahead.
                    if (key == probe_key) {
                        probing.unset(stream_index);
                    } else {
                        assert(it.key_ahead(.{ .key_after = key, .key_before = probe_key }));
                    }
                }
            }

            if (drained.count() == it.streams_count) {
                // Can't probe if all streams are drained.
                assert(probe_key == key_min);
                return error.Drained;
            }

            assert(probe_key != key_min);
            for (0..it.streams_count) |stream_index| {
                if (drained.isSet(stream_index)) {
                    // Probing the drained stream will update the key range for the next read.
                    stream_probe(it.context, @intCast(stream_index), probe_key);
                    // The stream must remain drained after probed.
                    assert(stream_peek(it.context, @intCast(stream_index)) == error.Drained);
                } else {
                    // At this point, all the buffered streams must have produced a matching key.
                    assert(stream_peek(it.context, @intCast(stream_index)) catch {
                        unreachable;
                    } == probe_key);
                }
            }

            // The iterator cannot regress.
            assert(it.probe_key_previous == null or
                probe_key == it.probe_key_previous.? or
                it.key_ahead(.{ .key_after = probe_key, .key_before = it.probe_key_previous.? }));

            it.probe_key_previous = probe_key;
            return if (drained.count() == 0) probe_key else error.Drained;
        }

        /// Returns true if `key_after` is ahead of `key_before` depending on the direction,
        /// that is `key_after > key_before` (ascending) or `key_after < key_before` (descending).
        inline fn key_ahead(
            it: *const ZigZagMergeIterator,
            keys: struct { key_after: Key, key_before: Key },
        ) bool {
            return switch (it.direction) {
                .ascending => keys.key_after > keys.key_before,
                .descending => keys.key_after < keys.key_before,
            };
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
        ) error{ Empty, Drained }!Key {
            const stream = context.streams[stream_index];
            if (stream.len == 0) return error.Empty;
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
                const key = stream_peek(context, stream_index) catch |err| {
                    switch (err) {
                        error.Drained => unreachable,
                        error.Empty => return,
                    }
                };

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

        fn fuzz(random: std.rand.Random, stream_key_count_max: u32) !void {
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
                    const len = random.intRangeAtMostBiased(
                        u32,
                        intersection_len_min,
                        stream_key_count_max,
                    );
                    if (len < stream_len_min) stream_len_min = len;

                    streams[stream_index] =
                        streams_buffer[stream_index * stream_key_count_max ..][0..len];
                }

                const intersection = intersection_buffer[0..random.intRangeAtMostBiased(
                    u32,
                    intersection_len_min,
                    stream_len_min,
                )];
                assert(intersection.len >= intersection_len_min and
                    intersection.len <= stream_len_min);

                fuzz_make_intersection(
                    random,
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
            random: std.rand.Random,
            streams: []const []Value,
            intersection: []Value,
        ) void {
            const less_than = struct {
                fn less_than(_: void, lhs: Value, rhs: Value) bool {
                    return lhs < rhs;
                }
            }.less_than;

            // Starting with the values we want to be the intersection:
            random.bytes(mem.sliceAsBytes(intersection));
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
                    random.bytes(mem.sliceAsBytes(stream[intersection.len..]));
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

    var prng = std.rand.DefaultPrng.init(seed);
    const random = prng.random();

    try TestContextType(32).fuzz(random, 256);
}
