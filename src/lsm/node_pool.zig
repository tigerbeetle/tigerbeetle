const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;
const meta = std.meta;

const vsr = @import("../vsr.zig");
const stdx = @import("stdx");

pub fn NodePoolType(comptime _node_size: u32, comptime _node_alignment: u13) type {
    return struct {
        const NodePool = @This();

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

        pub fn init(pool: *NodePool, allocator: mem.Allocator, node_count: u32) !void {
            assert(node_count > 0);

            pool.* = .{
                .buffer = undefined,
                .free = undefined,
            };
            const size = node_size * node_count;
            pool.buffer = try allocator.alignedAlloc(u8, node_alignment, size);
            errdefer allocator.free(pool.buffer);

            pool.free = try std.bit_set.DynamicBitSetUnmanaged.initFull(allocator, node_count);
            errdefer pool.free.deinit(allocator);
        }

        pub fn deinit(pool: *NodePool, allocator: mem.Allocator) void {
            // If the NodePool is being deinitialized, all nodes should have already been
            // released to the pool.
            assert(pool.free.count() == pool.free.bit_length);

            allocator.free(pool.buffer);
            pool.free.deinit(allocator);
        }

        pub fn reset(pool: *NodePool) void {
            pool.free.setRangeValue(.{ .start = 0, .end = pool.free.capacity() }, true);

            pool.* = .{
                .buffer = pool.buffer,
                .free = pool.free,
            };
        }

        pub fn acquire(pool: *NodePool) Node {
            // TODO: To ensure this "unreachable" is never reached, the primary must reject
            // new requests when storage space is too low to fulfill them.
            const node_index = pool.free.findFirstSet() orelse vsr.fatal(
                .manifest_node_pool_exhausted,
                "out of memory for manifest, " ++
                    "restart the replica increasing '--memory-lsm-manifest'",
                .{},
            );
            assert(pool.free.isSet(node_index));
            pool.free.unset(node_index);

            const node = pool.buffer[node_index * node_size ..][0..node_size];
            return @alignCast(node);
        }

        pub fn release(pool: *NodePool, node: Node) void {
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

fn TestContextType(comptime node_size: usize, comptime node_alignment: u12) type {
    const testing = std.testing;
    const TestPool = NodePoolType(node_size, node_alignment);

    const log = false;

    return struct {
        const TestContext = @This();

        node_count: u32,
        prng: *stdx.PRNG,
        sentinel: u64,
        node_pool: TestPool,
        node_map: std.AutoArrayHashMap(TestPool.Node, u64),

        acquires: u64 = 0,
        releases: u64 = 0,

        fn init(context: *TestContext, prng: *stdx.PRNG, node_count: u32) !void {
            context.* = .{
                .node_count = node_count,
                .prng = prng,
                .sentinel = prng.int(u64),

                .node_pool = undefined,
                .node_map = undefined,
            };

            try context.node_pool.init(testing.allocator, node_count);
            errdefer context.node_pool.deinit(testing.allocator);
            @memset(mem.bytesAsSlice(u64, context.node_pool.buffer), context.sentinel);

            context.node_map = std.AutoArrayHashMap(TestPool.Node, u64).init(testing.allocator);
            errdefer context.node_map.deinit();
        }

        fn deinit(context: *TestContext) void {
            context.node_pool.deinit(testing.allocator);
            context.node_map.deinit();
        }

        fn run(context: *TestContext) !void {
            const Action = enum { acquire, release };
            {
                var i: usize = 0;
                while (i < context.node_count * 4) : (i += 1) {
                    switch (context.prng.enum_weighted(Action, .{
                        .acquire = 60,
                        .release = 40,
                    })) {
                        .acquire => try context.acquire(),
                        .release => try context.release(),
                    }
                }
            }

            {
                var i: usize = 0;
                while (i < context.node_count * 4) : (i += 1) {
                    switch (context.prng.enum_weighted(Action, .{
                        .acquire = 40,
                        .release = 60,
                    })) {
                        .acquire => try context.acquire(),
                        .release => try context.release(),
                    }
                }
            }

            try context.release_all();
        }

        fn acquire(context: *TestContext) !void {
            if (context.node_map.count() == context.node_count) return;

            const node = context.node_pool.acquire();

            // Verify that this node has not already been acquired.
            for (mem.bytesAsSlice(u64, node)) |word| {
                try testing.expectEqual(context.sentinel, word);
            }

            const gop = try context.node_map.getOrPut(node);
            try testing.expect(!gop.found_existing);

            // Write unique data into the node so we can test that it doesn't get overwritten.
            const id = context.prng.int(u64);
            @memset(mem.bytesAsSlice(u64, node), id);
            gop.value_ptr.* = id;

            context.acquires += 1;
        }

        fn release(context: *TestContext) !void {
            if (context.node_map.count() == 0) return;

            const index = context.prng.index(context.node_map.keys());
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

        fn release_all(context: *TestContext) !void {
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

    var prng = stdx.PRNG.from_seed(seed);
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
        const TestContext = TestContextType(tuple.node_size, tuple.node_alignment);

        var i: u32 = 1;
        while (i < 64) : (i += 1) {
            var context: TestContext = undefined;
            try context.init(&prng, i);
            defer context.deinit();

            try context.run();
        }
    }
}
