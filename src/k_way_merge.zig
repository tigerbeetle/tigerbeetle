const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

pub fn KWayMergeIterator(
    comptime Context: type,
    comptime Key: type,
    comptime Value: type,
    comptime keys_ordered: fn (Key, Key) bool,
    comptime k_max: u32,
    comptime stream_peek: fn (context: *Context, stream_id: u32) ?Key,
    comptime stream_pop: fn (context: *Context, stream_id: u32) Value,
) type {
    return struct {
        const Self = @This();

        context: *Context,
        keys: [k_max]Key,
        stream_ids: [k_max]u32,
        k: u32,

        pub fn init(context: *Context, stream_ids: []const u32) Self {
            var it: Self = .{
                .context = context,
                .keys = undefined,
                .stream_ids = undefined,
                .k = 0,
            };

            for (stream_ids) |stream_id| {
                it.keys[it.k] = stream_peek(context, stream_id) orelse continue;
                it.stream_ids[it.k] = stream_id;
                it.up_heap(it.k);
                it.k += 1;
            }

            return it;
        }

        pub fn pop(it: *Self) ?Value {
            if (it.k == 0) return null;

            const root = it.stream_ids[0];
            // We know that each input iterator is sorted, so we don't need to compare the next
            // key on that iterator with the current min, we know it is greater.
            const value = stream_pop(it.context, root);

            if (stream_peek(it.context, root)) |key| {
                it.keys[0] = key;
                it.down_heap();
            } else {
                it.swap(0, it.k - 1);
                it.k -= 1;
                it.down_heap();
            }

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
        // If the order is incorrect, swap the current node with the smaller child.
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
            mem.swap(u32, &it.stream_ids[a], &it.stream_ids[b]);
        }

        inline fn ordered(it: Self, a: u32, b: ?u32) bool {
            return b == null or keys_ordered(it.keys[a], it.keys[b.?]);
        }
    };
}

fn TestContext(comptime k_max: u32) type {
    const testing = std.testing;
    return struct {
        const Self = @This();

        const log = false;

        streams: [k_max][]const u32,

        fn keys_ordered(a: u32, b: u32) bool {
            return a < b;
        }

        fn stream_peek(context: *Self, id: u32) ?u32 {
            const index = id - k_max;
            const stream = context.streams[index];
            if (stream.len == 0) return null;
            return stream[0];
        }

        fn stream_pop(context: *Self, id: u32) u32 {
            const index = id - k_max;
            const stream = context.streams[index];
            context.streams[index] = stream[1..];
            return stream[0];
        }

        fn merge(streams: []const []const u32, expect: []const u32) !void {
            const KWay = KWayMergeIterator(
                Self,
                u32,
                u32,
                keys_ordered,
                k_max,
                stream_peek,
                stream_pop,
            );
            var actual = std.ArrayList(u32).init(testing.allocator);
            defer actual.deinit();

            var context: Self = .{ .streams = undefined };
            mem.copy([]const u32, &context.streams, streams);

            var stream_ids_buffer: [k_max]u32 = undefined;
            const stream_ids = stream_ids_buffer[0..streams.len];
            for (stream_ids) |*id, i| id.* = @intCast(u32, i) + k_max;

            var kway = KWay.init(&context, stream_ids);

            while (kway.pop()) |value| {
                try actual.append(value);
            }

            try testing.expectEqualSlices(u32, expect, actual.items);
        }

        fn fuzz(random: *std.rand.Random, stream_key_count_max: u32) !void {
            if (log) std.debug.print("\n", .{});
            const allocator = testing.allocator;

            var streams: [k_max][]u32 = undefined;

            const streams_buffer = try allocator.alloc(u32, k_max * stream_key_count_max);
            defer allocator.free(streams_buffer);

            const expect_buffer = try allocator.alloc(u32, k_max * stream_key_count_max);
            defer allocator.free(expect_buffer);

            var k: u32 = 0;
            while (k < k_max) : (k += 1) {
                if (log) std.debug.print("k = {}\n", .{k});
                var i: u32 = 0;
                while (i < k) : (i += 1) {
                    const len = fuzz_stream_len(random, stream_key_count_max);
                    streams[i] = streams_buffer[i * stream_key_count_max ..][0..len];
                    fuzz_stream_values(random, streams[i]);

                    if (log) {
                        std.debug.print("stream {} = ", .{i});
                        for (streams[i]) |key| std.debug.print("{},", .{key});
                        std.debug.print("\n", .{});
                    }
                }

                var expect_len: usize = 0;
                for (streams[0..k]) |stream| {
                    mem.copy(u32, expect_buffer[expect_len..], stream);
                    expect_len += stream.len;
                }
                const expect = expect_buffer[0..expect_len];
                std.sort.sort(u32, expect, {}, less_than);

                try merge(streams[0..k], expect);
                if (log) std.debug.print("\n", .{});
            }
        }

        fn fuzz_stream_len(random: *std.rand.Random, stream_key_count_max: u32) u32 {
            return switch (random.uintLessThanBiased(u8, 100)) {
                0...4 => 0,
                5...9 => stream_key_count_max,
                else => random.uintAtMostBiased(u32, stream_key_count_max),
            };
        }

        fn fuzz_stream_values(random: *std.rand.Random, stream: []u32) void {
            const key_max = random.intRangeLessThanBiased(u32, 512, 1024);
            switch (random.uintLessThanBiased(u8, 100)) {
                0...4 => {
                    mem.set(u32, stream, random.int(u32));
                },
                else => {
                    random.bytes(mem.sliceAsBytes(stream));
                },
            }
            for (stream) |*key| key.* = key.* % key_max;
            std.sort.sort(u32, stream, {}, less_than);
        }

        fn less_than(_: void, a: u32, b: u32) bool {
            return a < b;
        }
    };
}

test "k_way_merge: unit" {
    try TestContext(1).merge(
        &[_][]const u32{
            &[_]u32{ 0, 3, 4, 8 },
        },
        &[_]u32{ 0, 3, 4, 8 },
    );
    try TestContext(3).merge(
        &[_][]const u32{
            &[_]u32{ 0, 3, 4, 8 },
            &[_]u32{ 1, 2, 11 },
            &[_]u32{ 2, 12, 13, 15 },
        },
        &[_]u32{ 0, 1, 2, 2, 3, 4, 8, 11, 12, 13, 15 },
    );
}

test "k_way_merge: fuzz" {
    const seed = std.crypto.random.int(u64);
    errdefer std.debug.print("\nTEST FAILED: seed = {}\n", .{seed});

    var prng = std.rand.DefaultPrng.init(seed);
    try TestContext(32).fuzz(&prng.random, 256);
}
