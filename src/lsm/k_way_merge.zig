const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const Direction = @import("../direction.zig").Direction;

pub fn KWayMergeIteratorType(
    comptime Context: type,
    comptime Key: type,
    comptime Value: type,
    comptime key_from_value: fn (*const Value) callconv(.Inline) Key,
    comptime k_max: u32,
    /// Peek the next key in the stream identified by stream_index.
    /// For example, peek(stream_index=2) returns user_streams[2][0].
    /// Returns Drained if the stream was consumed and
    /// must be refilled before calling peek() again.
    /// Returns Empty if the stream was fully consumed and reached the end.
    comptime stream_peek: fn (
        context: *Context,
        stream_index: u32,
    ) error{ Empty, Drained }!Key,
    comptime stream_pop: fn (context: *Context, stream_index: u32) Value,
    /// Returns true if stream A has higher precedence than stream B.
    /// This is used to deduplicate values across streams.
    comptime stream_precedence: fn (context: *const Context, a: u32, b: u32) bool,
) type {
    return struct {
        const Self = @This();

        context: *Context,

        /// Array of keys, with each key representing the next key in each stream.
        ///
        /// `keys` is *almost* structured as a binary heap — to become a heap, streams[0] must be
        /// peeked and sifted (see pop_internal()).
        ///
        /// * When `direction=ascending`, keys are ordered low-to-high.
        /// * When `direction=descending`, keys are ordered high-to-low.
        /// * Equivalent keys are ordered from high precedence to low.
        keys: [k_max]Key,

        /// For each key in keys above, the corresponding index of the stream containing that key.
        /// This decouples the order and storage of streams, the user being responsible for storage.
        /// The user's streams array is never reordered while keys are swapped, only this mapping.
        streams: [k_max]u32,

        /// The number of streams remaining in the iterator.
        k: u32,

        direction: Direction,
        previous_key_popped: ?Key = null,

        /// This function may create an Iterator with k being less than stream_count_max if
        /// stream_peek() for one of the streams immediately returns null.
        pub fn init(context: *Context, stream_count_max: u32, direction: Direction) Self {
            // TODO Do we ever expect stream_count_max to be 0?
            assert(stream_count_max <= k_max);

            var it: Self = .{
                .context = context,
                .keys = undefined,
                .streams = undefined,
                .k = 0,
                .direction = direction,
            };

            // We must loop on stream_index but assign at it.k, as k may be less than stream_index
            // when there are empty streams.
            // TODO Do we have test coverage for this edge case?
            var stream_index: u32 = 0;
            while (stream_index < stream_count_max) : (stream_index += 1) {
                it.keys[it.k] = stream_peek(context, stream_index) catch |err| switch (err) {
                    // On initialization, the streams should either have data already
                    // buffered up to peek or be empty and have no more values to produce.
                    error.Drained => unreachable,
                    error.Empty => continue,
                };
                it.streams[it.k] = stream_index;
                it.up_heap(it.k);
                it.k += 1;
            }

            return it;
        }

        pub fn empty(it: Self) bool {
            return it.k == 0;
        }

        pub fn pop(it: *Self) error{Drained}!?Value {
            while (try it.pop_internal()) |value| {
                const key = key_from_value(&value);
                if (it.previous_key_popped) |previous| {
                    switch (std.math.order(previous, key)) {
                        .lt => assert(it.direction == .ascending),
                        // Discard this value and pop the next one.
                        .eq => continue,
                        .gt => assert(it.direction == .descending),
                    }
                }
                it.previous_key_popped = key;
                return value;
            }

            return null;
        }

        fn pop_internal(it: *Self) error{Drained}!?Value {
            if (it.k == 0) return null;

            // We update the heap prior to removing the value from the stream. If we updated after
            // stream_pop() instead, when stream_peek() returns Drained we would be unable to order
            // the heap, and when the stream does buffer data it would be out of position.
            if (stream_peek(it.context, it.streams[0])) |key| {
                it.keys[0] = key;
                it.down_heap();
            } else |err| switch (err) {
                error.Drained => return error.Drained,
                error.Empty => {
                    it.swap(0, it.k - 1);
                    it.k -= 1;
                    it.down_heap();
                },
            }
            if (it.k == 0) return null;

            const root = it.streams[0];
            const value = stream_pop(it.context, root);

            return value;
        }

        fn up_heap(it: *Self, start: u32) void {
            var i = start;
            while (parent(i)) |p| : (i = p) {
                if (it.ordered(p, i)) break;
                it.swap(p, i);
            }
        }

        // Start at the root node.
        // Compare the current node with its children, if the order is correct stop.
        // If the order is incorrect, swap the current node with the appropriate child.
        fn down_heap(it: *Self) void {
            if (it.k == 0) return;
            var i: u32 = 0;
            // A maximum of height iterations are required. After height iterations we are
            // guaranteed to have reached a leaf node, in which case we are always done.
            var safety_count: u32 = 0;
            const binary_tree_height = math.log2_int(u32, it.k) + 1;
            while (safety_count < binary_tree_height) : (safety_count += 1) {
                const left = left_child(i, it.k);
                const right = right_child(i, it.k);

                if (it.ordered(i, left)) {
                    if (it.ordered(i, right)) {
                        break;
                    } else {
                        it.swap(i, right.?);
                        i = right.?;
                    }
                } else if (it.ordered(i, right)) {
                    it.swap(i, left.?);
                    i = left.?;
                } else if (it.ordered(left.?, right.?)) {
                    it.swap(i, left.?);
                    i = left.?;
                } else {
                    it.swap(i, right.?);
                    i = right.?;
                }
            }
            assert(safety_count < binary_tree_height);
        }

        fn parent(node: u32) ?u32 {
            if (node == 0) return null;
            return (node - 1) / 2;
        }

        fn left_child(node: u32, k: u32) ?u32 {
            const child = 2 * node + 1;
            return if (child < k) child else null;
        }

        fn right_child(node: u32, k: u32) ?u32 {
            const child = 2 * node + 2;
            return if (child < k) child else null;
        }

        fn swap(it: *Self, a: u32, b: u32) void {
            mem.swap(Key, &it.keys[a], &it.keys[b]);
            mem.swap(u32, &it.streams[a], &it.streams[b]);
        }

        inline fn ordered(it: Self, a: u32, b: ?u32) bool {
            return b == null or switch (std.math.order(it.keys[a], it.keys[b.?])) {
                .lt => it.direction == .ascending,
                .eq => stream_precedence(it.context, it.streams[a], it.streams[b.?]),
                .gt => it.direction == .descending,
            };
        }
    };
}

fn TestContext(comptime k_max: u32) type {
    const testing = std.testing;

    return struct {
        const Self = @This();

        const log = false;

        const Value = struct {
            key: u32,
            version: u32,

            inline fn to_key(v: *const Value) u32 {
                return v.key;
            }
        };

        streams: [k_max][]const Value,

        fn stream_peek(context: *const Self, stream_index: u32) error{ Empty, Drained }!u32 {
            // TODO: test for Drained somehow as well.
            const stream = context.streams[stream_index];
            if (stream.len == 0) return error.Empty;
            return stream[0].key;
        }

        fn stream_pop(context: *Self, stream_index: u32) Value {
            const stream = context.streams[stream_index];
            context.streams[stream_index] = stream[1..];
            return stream[0];
        }

        fn stream_precedence(context: *const Self, a: u32, b: u32) bool {
            _ = context;

            // Higher streams have higher precedence.
            return a > b;
        }

        fn merge(
            direction: Direction,
            streams_keys: []const []const u32,
            expect: []const Value,
        ) !void {
            const KWay = KWayMergeIteratorType(
                Self,
                u32,
                Value,
                Value.to_key,
                k_max,
                stream_peek,
                stream_pop,
                stream_precedence,
            );
            var actual = std.ArrayList(Value).init(testing.allocator);
            defer actual.deinit();

            var streams: [k_max][]Value = undefined;

            for (streams_keys, 0..) |stream_keys, i| {
                errdefer for (streams[0..i]) |s| testing.allocator.free(s);
                streams[i] = try testing.allocator.alloc(Value, stream_keys.len);
                for (stream_keys, 0..) |key, j| {
                    streams[i][j] = .{
                        .key = key,
                        .version = @as(u32, @intCast(i)),
                    };
                }
            }
            defer for (streams[0..streams_keys.len]) |s| testing.allocator.free(s);

            var context: Self = .{ .streams = streams };
            var kway = KWay.init(&context, @as(u32, @intCast(streams_keys.len)), direction);

            while (try kway.pop()) |value| {
                try actual.append(value);
            }

            try testing.expectEqualSlices(Value, expect, actual.items);
        }

        fn fuzz(random: std.rand.Random, stream_key_count_max: u32) !void {
            if (log) std.debug.print("\n", .{});
            const allocator = testing.allocator;

            var streams: [k_max][]u32 = undefined;

            const streams_buffer = try allocator.alloc(u32, k_max * stream_key_count_max);
            defer allocator.free(streams_buffer);

            const expect_buffer = try allocator.alloc(Value, k_max * stream_key_count_max);
            defer allocator.free(expect_buffer);

            var k: u32 = 0;
            while (k < k_max) : (k += 1) {
                if (log) std.debug.print("k = {}\n", .{k});
                {
                    var i: u32 = 0;
                    while (i < k) : (i += 1) {
                        const len = fuzz_stream_len(random, stream_key_count_max);
                        streams[i] = streams_buffer[i * stream_key_count_max ..][0..len];
                        fuzz_stream_keys(random, streams[i]);

                        if (log) {
                            std.debug.print("stream {} = ", .{i});
                            for (streams[i]) |key| std.debug.print("{},", .{key});
                            std.debug.print("\n", .{});
                        }
                    }
                }

                var expect_buffer_len: usize = 0;
                for (streams[0..k], 0..) |stream, version| {
                    for (stream) |key| {
                        expect_buffer[expect_buffer_len] = .{
                            .key = key,
                            .version = @as(u32, @intCast(version)),
                        };
                        expect_buffer_len += 1;
                    }
                }
                const expect_with_duplicates = expect_buffer[0..expect_buffer_len];
                std.mem.sort(Value, expect_with_duplicates, {}, value_less_than);

                var target: usize = 0;
                var previous_key: ?u32 = null;
                for (expect_with_duplicates) |value| {
                    if (previous_key) |p| {
                        if (value.key == p) continue;
                    }
                    previous_key = value.key;
                    expect_with_duplicates[target] = value;
                    target += 1;
                }
                const expect = expect_with_duplicates[0..target];

                if (log) {
                    std.debug.print("expect = ", .{});
                    for (expect) |value| std.debug.print("({},{}),", .{ value.key, value.version });
                    std.debug.print("\n", .{});
                }

                try merge(.ascending, streams[0..k], expect);

                for (streams[0..k]) |stream| mem.reverse(u32, stream);
                mem.reverse(Value, expect);

                try merge(.descending, streams[0..k], expect);

                if (log) std.debug.print("\n", .{});
            }
        }

        fn fuzz_stream_len(random: std.rand.Random, stream_key_count_max: u32) u32 {
            return switch (random.uintLessThanBiased(u8, 100)) {
                0...4 => 0,
                5...9 => stream_key_count_max,
                else => random.uintAtMostBiased(u32, stream_key_count_max),
            };
        }

        fn fuzz_stream_keys(random: std.rand.Random, stream: []u32) void {
            const key_max = random.intRangeLessThanBiased(u32, 512, 1024);
            switch (random.uintLessThanBiased(u8, 100)) {
                0...4 => {
                    @memset(stream, random.int(u32));
                },
                else => {
                    random.bytes(mem.sliceAsBytes(stream));
                },
            }
            for (stream) |*key| key.* = key.* % key_max;
            std.mem.sort(u32, stream, {}, key_less_than);
        }

        fn key_less_than(_: void, a: u32, b: u32) bool {
            return a < b;
        }

        fn value_less_than(_: void, a: Value, b: Value) bool {
            return switch (math.order(a.key, b.key)) {
                .lt => true,
                .eq => a.version > b.version,
                .gt => false,
            };
        }
    };
}

test "k_way_merge: unit" {
    try TestContext(1).merge(
        .ascending,
        &[_][]const u32{
            &[_]u32{ 0, 3, 4, 8 },
        },
        &[_]TestContext(1).Value{
            .{ .key = 0, .version = 0 },
            .{ .key = 3, .version = 0 },
            .{ .key = 4, .version = 0 },
            .{ .key = 8, .version = 0 },
        },
    );
    try TestContext(1).merge(
        .descending,
        &[_][]const u32{
            &[_]u32{ 8, 4, 3, 0 },
        },
        &[_]TestContext(1).Value{
            .{ .key = 8, .version = 0 },
            .{ .key = 4, .version = 0 },
            .{ .key = 3, .version = 0 },
            .{ .key = 0, .version = 0 },
        },
    );
    try TestContext(3).merge(
        .ascending,
        &[_][]const u32{
            &[_]u32{ 0, 3, 4, 8, 11 },
            &[_]u32{ 2, 11, 12, 13, 15 },
            &[_]u32{ 1, 2, 11 },
        },
        &[_]TestContext(3).Value{
            .{ .key = 0, .version = 0 },
            .{ .key = 1, .version = 2 },
            .{ .key = 2, .version = 2 },
            .{ .key = 3, .version = 0 },
            .{ .key = 4, .version = 0 },
            .{ .key = 8, .version = 0 },
            .{ .key = 11, .version = 2 },
            .{ .key = 12, .version = 1 },
            .{ .key = 13, .version = 1 },
            .{ .key = 15, .version = 1 },
        },
    );
    try TestContext(3).merge(
        .descending,
        &[_][]const u32{
            &[_]u32{ 11, 8, 4, 3, 0 },
            &[_]u32{ 15, 13, 12, 11, 2 },
            &[_]u32{ 11, 2, 1 },
        },
        &[_]TestContext(3).Value{
            .{ .key = 15, .version = 1 },
            .{ .key = 13, .version = 1 },
            .{ .key = 12, .version = 1 },
            .{ .key = 11, .version = 2 },
            .{ .key = 8, .version = 0 },
            .{ .key = 4, .version = 0 },
            .{ .key = 3, .version = 0 },
            .{ .key = 2, .version = 2 },
            .{ .key = 1, .version = 2 },
            .{ .key = 0, .version = 0 },
        },
    );
}

test "k_way_merge: fuzz" {
    const seed = std.crypto.random.int(u64);
    errdefer std.debug.print("\nTEST FAILED: seed = {}\n", .{seed});

    var prng = std.rand.DefaultPrng.init(seed);
    const random = prng.random();

    try TestContext(32).fuzz(random, 256);
}
