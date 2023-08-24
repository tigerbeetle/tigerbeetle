const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;
const meta = std.meta;

pub fn NodePool(comptime _node_size: u32, comptime _node_alignment: u13) type {
    return struct {
        const Self = @This();

        pub const node_size = _node_size;
        pub const node_alignment = _node_alignment;
        pub const Node = *align(node_alignment) [node_size]u8;

        comptime {
            assert(node_size > 0);
            assert(node_alignment > 0);
            assert(node_alignment <= 4096);
            assert(math.isPowerOfTwo(node_size));
            assert(math.isPowerOfTwo(node_alignment));
            assert(node_size % node_alignment == 0);
        }

        buffer: []align(node_alignment) u8,
        free: std.bit_set.DynamicBitSetUnmanaged,

        pub fn init(allocator: mem.Allocator, node_count: u32) !Self {
            assert(node_count > 0);

            const size = node_size * node_count;
            const buffer = try allocator.alignedAlloc(u8, node_alignment, size);
            errdefer allocator.free(buffer);

            const free = try std.bit_set.DynamicBitSetUnmanaged.initFull(allocator, node_count);
            errdefer free.deinit(allocator);

            return Self{
                .buffer = buffer,
                .free = free,
            };
        }

        pub fn deinit(pool: *Self, allocator: mem.Allocator) void {
            // If the NodePool is being deinitialized, all nodes should have already been
            // released to the pool.
            assert(pool.free.count() == pool.free.bit_length);

            allocator.free(pool.buffer);
            pool.free.deinit(allocator);
        }

        pub fn reset(pool: *Self) void {
            pool.free.setRangeValue(.{ .start = 0, .end = pool.free.capacity() }, true);

            pool.* = .{
                .buffer = pool.buffer,
                .free = pool.free,
            };
        }

        pub fn acquire(pool: *Self) Node {
            // TODO: To ensure this "unreachable" is never reached, the primary must reject
            // new requests when storage space is too low to fulfill them.
            const node_index = pool.free.findFirstSet() orelse unreachable;
            assert(pool.free.isSet(node_index));
            pool.free.unset(node_index);

            const node = pool.buffer[node_index * node_size ..][0..node_size];
            return @alignCast(node);
        }

        pub fn release(pool: *Self, node: Node) void {
            // Our pointer arithmetic assumes that the unit of node_size is a u8.
            comptime assert(meta.Elem(Node) == u8);
            comptime assert(meta.Elem(@TypeOf(pool.buffer)) == u8);

            assert(@intFromPtr(node) >= @intFromPtr(pool.buffer.ptr));
            assert(@intFromPtr(node) + node_size <= @intFromPtr(pool.buffer.ptr) + pool.buffer.len);

            const node_offset = @intFromPtr(node) - @intFromPtr(pool.buffer.ptr);
            const node_index = @divExact(node_offset, node_size);
            assert(!pool.free.isSet(node_index));
            pool.free.set(node_index);
        }
    };
}

fn TestContext(comptime node_size: usize, comptime node_alignment: u12) type {
    const testing = std.testing;
    const TestPool = NodePool(node_size, node_alignment);

    const log = false;

    return struct {
        const Self = @This();

        node_count: u32,
        random: std.rand.Random,
        node_pool: TestPool,
        node_map: std.AutoArrayHashMap(TestPool.Node, u64),
        sentinel: u64,

        acquires: u64 = 0,
        releases: u64 = 0,

        fn init(random: std.rand.Random, node_count: u32) !Self {
            var node_pool = try TestPool.init(testing.allocator, node_count);
            errdefer node_pool.deinit(testing.allocator);

            var node_map = std.AutoArrayHashMap(TestPool.Node, u64).init(testing.allocator);
            errdefer node_map.deinit();

            const sentinel = random.int(u64);
            @memset(mem.bytesAsSlice(u64, node_pool.buffer), sentinel);

            return Self{
                .node_count = node_count,
                .random = random,
                .node_pool = node_pool,
                .node_map = node_map,
                .sentinel = sentinel,
            };
        }

        fn deinit(context: *Self) void {
            context.node_pool.deinit(testing.allocator);
            context.node_map.deinit();
        }

        fn run(context: *Self) !void {
            {
                var i: usize = 0;
                while (i < context.node_count * 4) : (i += 1) {
                    switch (context.random.uintLessThanBiased(u32, 100)) {
                        0...59 => try context.acquire(),
                        60...99 => try context.release(),
                        else => unreachable,
                    }
                }
            }

            {
                var i: usize = 0;
                while (i < context.node_count * 4) : (i += 1) {
                    switch (context.random.uintLessThanBiased(u32, 100)) {
                        0...39 => try context.acquire(),
                        40...99 => try context.release(),
                        else => unreachable,
                    }
                }
            }

            try context.release_all();
        }

        fn acquire(context: *Self) !void {
            if (context.node_map.count() == context.node_count) return;

            const node = context.node_pool.acquire();

            // Verify that this node has not already been acquired.
            for (mem.bytesAsSlice(u64, node)) |word| {
                try testing.expectEqual(context.sentinel, word);
            }

            const gop = try context.node_map.getOrPut(node);
            try testing.expect(!gop.found_existing);

            // Write unique data into the node so we can test that it doesn't get overwritten.
            const id = context.random.int(u64);
            @memset(mem.bytesAsSlice(u64, node), id);
            gop.value_ptr.* = id;

            context.acquires += 1;
        }

        fn release(context: *Self) !void {
            if (context.node_map.count() == 0) return;

            const index = context.random.uintLessThanBiased(usize, context.node_map.count());
            const node = context.node_map.keys()[index];
            const id = context.node_map.values()[index];

            // Verify that the data of this node has not been overwritten since we acquired it.
            for (mem.bytesAsSlice(u64, node)) |word| {
                try testing.expectEqual(id, word);
            }

            @memset(mem.bytesAsSlice(u64, node), context.sentinel);
            context.node_pool.release(node);
            context.node_map.swapRemoveAt(index);

            context.releases += 1;
        }

        fn release_all(context: *Self) !void {
            while (context.node_map.count() > 0) try context.release();

            // Verify that nothing in the entire buffer has been acquired.
            for (mem.bytesAsSlice(u64, context.node_pool.buffer)) |word| {
                try testing.expectEqual(context.sentinel, word);
            }

            if (log) {
                std.debug.print("\nacquires: {}, releases: {}\n", .{
                    context.acquires,
                    context.releases,
                });
            }

            try testing.expect(context.acquires > 0);
            try testing.expect(context.acquires == context.releases);
        }
    };
}

test "NodePool" {
    const seed = 42;

    var prng = std.rand.DefaultPrng.init(seed);
    const random = prng.random();

    const Tuple = struct {
        node_size: u32,
        node_alignment: u12,
    };

    inline for (.{
        Tuple{ .node_size = 8, .node_alignment = 8 },
        Tuple{ .node_size = 16, .node_alignment = 8 },
        Tuple{ .node_size = 64, .node_alignment = 8 },
        Tuple{ .node_size = 16, .node_alignment = 16 },
        Tuple{ .node_size = 32, .node_alignment = 16 },
        Tuple{ .node_size = 128, .node_alignment = 16 },
    }) |tuple| {
        const Context = TestContext(tuple.node_size, tuple.node_alignment);

        var i: u32 = 1;
        while (i < 64) : (i += 1) {
            var context = try Context.init(random, i);
            defer context.deinit();

            try context.run();
        }
    }
}
