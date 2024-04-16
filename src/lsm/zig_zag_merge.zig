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
    /// as `>=key` or `<=key` depending on the iterator direction.
    /// Should not be called when the current key already matches the probe.
    /// The stream may become `Empty` or `Drained` _after_ probing.
    comptime stream_probe: fn (context: *Context, stream_index: u32, key: Key) void,
) type {
    return struct {
        const ZigZagMergeIterator = @This();
        const BitSet = std.bit_set.IntegerBitSet(streams_max);

        context: *Context,
        streams_count: u32,
        direction: Direction,
        previous_key_popped: ?Key = null,

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
                        // It's assumed that streams will produce the same value.
                        assert(stdx.equal_bytes(Value, &value, &value_other));
                    }
                }

                if (it.previous_key_popped) |previous| {
                    switch (std.math.order(previous, key)) {
                        .lt => assert(it.direction == .ascending),
                        // Discard duplicate values.
                        .eq => continue,
                        .gt => assert(it.direction == .descending),
                    }
                }
                it.previous_key_popped = key;

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
                            error.Empty => return null,
                            // Skipping `Drained` streams. The goal is to match all buffered streams
                            // first so that the drained ones can read from a narrower key range.
                            error.Drained => {
                                drained.set(stream_index);
                                continue;
                            },
                        }
                    };

                    if (switch (it.direction) {
                        .ascending => key > probe_key,
                        .descending => key < probe_key,
                    }) {
                        // The stream is ahead, it will be the probe key,
                        // meaning all streams before must be probed.
                        probe_key = key;

                        // Setting all previous streams as `true` except the drained ones.
                        probing.setRangeValue(.{ .start = 0, .end = stream_index }, true);
                        probing.setIntersection(drained.complement());
                    } else if (switch (it.direction) {
                        .ascending => key < probe_key,
                        .descending => key > probe_key,
                    }) {
                        // The stream is behind.
                        probing.set(stream_index);
                    } else {
                        // The key matches.
                        assert(key == probe_key);
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

                    if (key == probe_key) {
                        probing.unset(stream_index);
                    } else {
                        assert(switch (it.direction) {
                            .ascending => key > probe_key,
                            .descending => key < probe_key,
                        });
                    }
                }
            }

            if (drained.count() == it.streams_count) {
                //Can't probe if all streams are drained.
                assert(probe_key == key_min);
                return error.Drained;
            }

            assert(probe_key != key_min);

            // At this point, all the buffered streams have produced a match
            // that will be used to probe the drained streams.
            if (drained.count() > 0) {
                var drained_iterator = drained.iterator(.{ .kind = .set });
                while (drained_iterator.next()) |stream_index| {
                    stream_probe(it.context, @intCast(stream_index), probe_key);

                    // The stream must remain drained after probed.
                    assert(stream_peek(it.context, @intCast(stream_index)) == error.Drained);
                }
                return error.Drained;
            }

            return probe_key;
        }

        fn peek_key_classic(it: *ZigZagMergeIterator) error{Drained}!?Key {
            assert(it.streams_count <= streams_max);
            assert(it.streams_count > 1);

            // Starting with the first non-drained stream as the probe key:
            var probe_index: u32 = 0;
            var probe_key: Key = while (probe_index < it.streams_count) : (probe_index += 1) {
                break stream_peek(it.context, probe_index) catch |err| {
                    switch (err) {
                        // Return immediately on empty streams.
                        error.Empty => return null,
                        // It's fine to skip `Drained` streams. The goal is to match all
                        // buffered streams first so that the drained ones can read from
                        // a narrower key range.
                        error.Drained => continue,
                    }
                };
            } else return error.Drained;

            var drained: bool = false;
            var stream_index: u32 = 0;
            while (stream_index < it.streams_count) {
                // Skipping the current probe stream.
                if (stream_index == probe_index) {
                    stream_index += 1;
                    continue;
                }

                var key = stream_peek(it.context, stream_index) catch |err| {
                    switch (err) {
                        // Return immediately on empty streams.
                        error.Empty => return null,
                        error.Drained => {
                            // Drained streams can be probed several times without a `peek`.
                            // Since they have no buffered data to seek, the operation simply
                            // updates the key range for the next read.
                            stream_probe(it.context, stream_index, probe_key);
                            stream_index += 1;
                            drained = true;
                            continue;
                        },
                    }
                };

                if (switch (it.direction) {
                    .ascending => key < probe_key,
                    .descending => key > probe_key,
                }) {
                    // Probing the stream if it is behind the probe.
                    stream_probe(it.context, stream_index, probe_key);
                    key = stream_peek(it.context, stream_index) catch |err| {
                        switch (err) {
                            // Return immediately on empty streams.
                            error.Empty => return null,
                            error.Drained => {
                                // Accumulating `Drained` errors to the end.
                                stream_index += 1;
                                drained = true;
                                continue;
                            },
                        }
                    };
                }

                if (key == probe_key) {
                    // The stream matches the probe key.
                    stream_index += 1;
                    continue;
                }

                assert(switch (it.direction) {
                    .ascending => key > probe_key,
                    .descending => key < probe_key,
                });

                // The stream is ahead of the current probe, it will be the
                // probe in the next iteration over all other streams again.
                assert(probe_index < it.streams_count);
                stream_probe(it.context, probe_index, key);

                probe_index = stream_index;
                probe_key = key;
                stream_index = 0;
            }

            if (constants.verify) {
                const drained_any = for (0..it.streams_count) |index| {
                    _ = stream_peek(it.context, @intCast(index)) catch |err| {
                        switch (err) {
                            error.Empty => unreachable,
                            error.Drained => break true,
                        }
                    };
                } else false;
                assert(drained == drained_any);
            }

            return if (drained)
                error.Drained
            else
                probe_key;
        }
    };
}

fn TestContext(comptime streams_max: u32) type {
    const testing = std.testing;

    return struct {
        const ZigZagMergeIterator = @This();

        const log = false;

        //
        const Key = u128;
        const Value = u128;

        inline fn key_from_value(value: *const Value) Key {
            return value.*;
        }

        streams: [streams_max][]const Value,
        direction: Direction,

        fn stream_peek(
            context: *const ZigZagMergeIterator,
            stream_index: u32,
        ) error{ Empty, Drained }!Key {
            const stream = context.streams[stream_index];
            if (stream.len == 0) return error.Empty;
            return switch (context.direction) {
                .ascending => key_from_value(&stream[0]),
                .descending => key_from_value(&stream[stream.len - 1]),
            };
        }

        fn stream_pop(context: *ZigZagMergeIterator, stream_index: u32) Value {
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

        fn stream_probe(context: *ZigZagMergeIterator, stream_index: u32, probe_key: Key) void {
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
                ZigZagMergeIterator,
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

                var context: ZigZagMergeIterator = .{
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
                @memcpy(stream[0..intersection.len], intersection);
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
    const Context = TestContext(10);

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

    // Unique and repeated elements:
    try Context.merge(
        &[_][]const Context.Value{
            &.{ 1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 5 },
            &.{ 1, 1, 2, 2, 3, 3, 4, 4, 5, 5 },
            &.{ 1, 2, 3, 4, 5 },
        },
        &.{ 1, 2, 3, 4, 5 },
    );

    // Repeated elements:
    try Context.merge(
        &[_][]const Context.Value{
            &.{ 1, 1, 2, 2, 3, 3, 4, 4, 5, 5 },
            &.{ 1, 1, 3, 3, 5, 5, 7, 7, 9, 9 },
        },
        &.{ 1, 3, 5 },
    );

    // Intersection with streams of different sizes:
    try Context.merge(
        &[_][]const Context.Value{
            // 1, 2, 3 ... 1000
            comptime blk: {
                @setEvalBranchQuota(2_000);
                var array: [1000]Context.Value = undefined;
                for (0..1000) |i| array[i] = @intCast(i + 1);
                break :blk &array;
            },
            // 10, 20, 30, ... 1000
            comptime blk: {
                var array: [100]Context.Value = undefined;
                for (0..100) |i| array[i] = @intCast(10 * (i + 1));
                break :blk &array;
            },
            // 1, 10, 100, 1000 ... 10 ^ 10
            comptime blk: {
                var array: [10]Context.Value = undefined;
                for (0..10) |i| array[i] = std.math.pow(Context.Value, 10, i);
                break :blk &array;
            },
        },
        &.{ 10, 100, 1000 },
    );

    // Sparse matching values: {100..199} ∩ {1..100} = {100}
    // First scan is the "zig".
    try Context.merge(
        &[_][]const Context.Value{
            // 100..199
            comptime blk: {
                var array: [100]Context.Value = undefined;
                for (0..100) |i| array[i] = @intCast(i + 100);
                break :blk &array;
            },
            // 1 ... 100
            comptime blk: {
                var array: [100]Context.Value = undefined;
                for (0..100) |i| array[i] = @intCast(i + 1);
                break :blk &array;
            },
        },
        &.{100},
    );

    // Sparse matching values: {1..100} ∩ {100..199} = {100}
    // First scan is the "zag".
    try Context.merge(
        &[_][]const Context.Value{
            // 1 ... 100
            comptime blk: {
                var array: [100]Context.Value = undefined;
                for (0..100) |i| array[i] = @intCast(i + 1);
                break :blk &array;
            },
            // 100..199
            comptime blk: {
                var array: [100]Context.Value = undefined;
                for (0..100) |i| array[i] = @intCast(i + 100);
                break :blk &array;
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

    try TestContext(32).fuzz(random, 256);
}
