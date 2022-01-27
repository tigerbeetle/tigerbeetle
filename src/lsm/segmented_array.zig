const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const meta = std.meta;

const utils = @import("../utils.zig");

pub const Direction = enum {
    ascending,
    descending,

    pub fn reverse(d: Direction) Direction {
        return switch (d) {
            .ascending => .descending,
            .descending => .ascending,
        };
    }
};

pub const Cursor = struct {
    node: u32,
    relative_index: u32,
};

// TODO:
// 1 Implement remove()
// 2 Merge nodes if they become half full (check during remove)
// 3 Unit tests for SegmentedArray
// - Special Level 0 ManifestLevel that allows overlapping tables.
// - Go back to compaction and track tables during the compaction to be inserted/removed.

pub fn SegmentedArray(
    comptime T: type,
    comptime NodePool: type,
    comptime element_count_max: u32,
) type {
    return struct {
        const Self = @This();

        // We can't use @divExact() here as we store TableInfo structs of various sizes in this
        // data structure. This means that there may be padding at the end of the node.
        pub const node_capacity = blk: {
            const max = NodePool.node_size / @sizeOf(T);

            // We require that the node is evenly divisible by 2 to simplify our code
            // that splits/joins nodes at the midpoint.
            const capacity = if (max % 2 == 0) max else max - 1;

            assert(capacity >= 2);
            assert(capacity % 2 == 0);
            break :blk capacity;
        };

        comptime {
            // If this assert fails, we should be using a non-segmented array instead!
            assert(element_count_max > node_capacity);
        }

        pub const node_count_max = blk: {
            // If a node fills up it is divided into two new nodes. Therefore,
            // the worst possible space overhead is when all nodes are half full.
            // This uses flooring division, we want to examine the worst case here.
            const elements_per_node_min = node_capacity / 2;
            // TODO Can we get rid of this +1?
            break :blk utils.div_ceil(element_count_max, elements_per_node_min) + 1;
        };

        node_count: u32 = 0,
        /// This is the segmented array, the first key_node_count pointers are non-null.
        /// The rest are null. We only use optional pointers here to get safety checks.
        nodes: *[node_count_max]?*[node_capacity]T,
        // TODO Get rid of this as it is redundant with key_node_start_index
        counts: *[node_count_max]u32,
        /// Since nodes in a segmented array are usually not full, computing the absolute index
        /// of an element in the full array is O(N) over the number of nodes. To avoid this cost
        /// we precompute the absolute index of the first element of each node.
        indexes: *[node_count_max]u32,

        pub fn init(allocator: mem.Allocator) !Self {
            const nodes = try allocator.create([node_count_max]?*[node_capacity]T);
            errdefer allocator.destroy(nodes);

            const counts = try allocator.create([node_count_max]u32);
            errdefer allocator.destroy(counts);

            const indexes = try allocator.create([node_count_max]u32);
            errdefer allocator.destroy(indexes);

            mem.set(?*[node_capacity]T, nodes, null);

            return Self{
                .nodes = nodes,
                .counts = counts,
                .indexes = indexes,
            };
        }

        pub fn deinit(array: Self, allocator: mem.Allocator, node_pool: *NodePool) void {
            for (array.nodes[0..array.node_count]) |node| {
                node_pool.release(@ptrCast(NodePool.Node, node.?));
            }
            allocator.free(array.nodes);
            allocator.free(array.counts);
            allocator.free(array.indexes);
        }

        pub fn insert_elements(
            array: *Self,
            node_pool: *NodePool,
            absolute_index: u32,
            elements: []T,
        ) void {
            // TODO add a comptime assert to check the relation between growth factor and this
            // runtime assert.
            assert(elements.len <= node_capacity);
            assert(absolute_index + elements.len <= element_count_max);

            for (elements) |element, i| {
                array.insert_element(node_pool, absolute_index + @intCast(u32, i), element);
            }
        }

        /// The `absolute_index` argument is the index the new element will take and may
        /// therefore be one past the end of the array.
        pub fn insert_element(
            array: *Self,
            node_pool: *NodePool,
            absolute_index: u32,
            element: T,
        ) void {
            assert(absolute_index < element_count_max);

            if (array.node_count == 0) {
                assert(absolute_index == 0);

                array.node_count = 1;
                array.nodes[0] = @ptrCast(*[node_capacity]T, node_pool.acquire());
                array.counts[0] = 0;
                array.indexes[0] = 0;
            }

            const cursor = array.split_node_if_full(node_pool, absolute_index);
            assert(cursor.node < array.node_count);
            assert(array.counts[cursor.node] < node_capacity);
            if (cursor.node == array.node_count - 1) {
                // Insertion may target the index one past the end of the array.
                assert(cursor.relative_index <= array.counts[cursor.node]);
            } else {
                assert(cursor.relative_index < array.counts[cursor.node]);
            }

            const pointer = array.nodes[cursor.node].?;
            if (cursor.relative_index + 1 < node_capacity) {
                mem.copyBackwards(
                    T,
                    pointer[cursor.relative_index + 1 .. array.counts[cursor.node] + 1],
                    pointer[cursor.relative_index..array.counts[cursor.node]],
                );
            }

            pointer[cursor.relative_index] = element;
            array.counts[cursor.node] += 1;
            if (cursor.node < array.node_count - 1) {
                for (array.indexes[cursor.node + 1 .. array.node_count]) |*i| {
                    i.* += 1;
                }
            }
        }

        fn split_node_if_full(array: *Self, node_pool: *NodePool, absolute_index: u32) Cursor {
            assert(absolute_index < element_count_max);

            const cursor = array.cursor_for_absolute_index(absolute_index);

            assert(cursor.node < array.node_count);
            if (array.counts[cursor.node] < node_capacity) return cursor;
            assert(array.counts[cursor.node] == node_capacity);

            array.split_node(node_pool, cursor.node);

            // Splitting the node invalidates the cursor. We could avoid calling
            // cursor_for_absolute_index() here and instead use our knowledge of how splitting
            // is implemented to calculate the new cursor in constant time, but that would be
            // more error prone.
            // TODO We think that such an optimization wouldn't be worthwhile as it doesn't affect
            // the data plane enough.
            return array.cursor_for_absolute_index(absolute_index);
        }

        /// Split the node at index `node` into two nodes, inserting the new node directly after
        /// `node`. This invalidates all cursors into the SegmentedArray but does not affect
        /// absolute indexes.
        fn split_node(array: *Self, node_pool: *NodePool, node: u32) void {
            assert(node < array.node_count);
            assert(array.counts[node] == node_capacity);

            // Insert a new node after the node being split.
            const new_node = node + 1;
            array.insert_empty_node_at(node_pool, new_node);

            const half = node_capacity / 2;
            comptime assert(node_capacity % 2 == 0);

            const pointer = array.nodes[node].?;
            const new_pointer = array.nodes[new_node].?;

            // We can do new_pointer[0..half] here because we assert node_capacity is even.
            // If it was odd, this redundant bounds check would fail.
            mem.copyBackwards(T, new_pointer[0..half], pointer[half..]);

            array.counts[node] = half;
            array.counts[new_node] = node_capacity - half;
            array.indexes[new_node] = array.indexes[node] + half;
        }

        /// Insert an empty node at index `node`.
        fn insert_empty_node_at(array: *Self, node_pool: *NodePool, node: u32) void {
            assert(array.node_count > 0);
            assert(array.node_count + 1 < node_count_max);
            // The first node can only be created by the special case in `insert_element()`.
            assert(node > 0);
            assert(node <= array.node_count);

            if (node < array.node_count) {
                mem.copyBackwards(
                    ?*[node_capacity]T,
                    array.nodes[node + 1 .. array.node_count + 1],
                    array.nodes[node..array.node_count],
                );
                mem.copyBackwards(
                    u32,
                    array.counts[node + 1 .. array.node_count + 1],
                    array.counts[node..array.node_count],
                );
                mem.copyBackwards(
                    u32,
                    array.indexes[node + 1 .. array.node_count + 1],
                    array.indexes[node..array.node_count],
                );
            }

            array.node_count += 1;
            array.nodes[node] = @ptrCast(*[node_capacity]T, node_pool.acquire());
            array.counts[node] = 0;
            array.indexes[node] = array.indexes[node - 1] + array.counts[node - 1];
        }

        pub fn remove_elements(
            array: *Self,
            node_pool: *NodePool,
            absolute_index: u32,
            count: u32,
        ) void {
            assert(absolute_index + count <= element_count_max);
            assert(count <= node_capacity);

            var i: u32 = count;
            while (i > 0) {
                i -= 1;

                array.remove_element(node_pool, absolute_index + i);
            }
        }

        // To remove an element, we simply find the node it is in and delete it from
        // the elements array, decrementing numElements. If this reduces the node to less
        // than half-full, then we move elements from the next node to fill it back up
        // above half. If this leaves the next node less than half full, then we move
        // all its remaining elements into the current node, then bypass and delete it.
        pub fn remove_element(
            array: *Self,
            node_pool: *NodePool,
            absolute_index: u32,
        ) void {
            assert(array.node_count > 0);
            assert(absolute_index < element_count_max);
            assert(absolute_index <
                array.indexes[array.node_count - 1] + array.counts[array.node_count - 1]);

            const cursor = array.cursor_for_absolute_index(absolute_index);
            assert(cursor.node < array.node_count);

            const b = cursor.node;
            const b_elements = array.nodes[b].?;

            // If we are removing the last element, nothing needs to be shifted.
            assert(cursor.relative_index <= array.counts[b] - 1);
            if (cursor.relative_index < array.counts[b] - 1) {
                mem.copy(
                    T,
                    b_elements[cursor.relative_index .. array.counts[b] - 1],
                    b_elements[cursor.relative_index + 1 .. array.counts[b]],
                );
            }
            array.counts[b] -= 1;
            b_elements[array.counts[b]] = undefined;

            // Removing an element requires the start indexes of subsequent nodes to be shifted.
            // If removing from the last node however, there is nothing to shift.
            assert(b <= array.node_count);
            if (b < array.node_count - 1) {
                for (array.indexes[b + 1 .. array.node_count]) |*i| i.* -= 1;
            }

            // If node_capacity is 2 and the node was half full, the element we just removed
            // made the node empty. Otherwise, we only allow the last node to be less than
            // half full so only the last node could have become empty.
            if (array.counts[b] == 0) {
                assert(b == array.node_count - 1 or node_capacity == 2);
                array.remove_empty_node_at(node_pool, b);
                return;
            }

            // The last node is allowed to be less than half full.
            if (b == array.node_count - 1) return;

            const half = node_capacity / 2;

            const b_count = array.counts[b];
            assert(b_count >= half - 1);
            assert(b_count <= node_capacity);

            const c = b + 1;
            const c_elements = array.nodes[c].?;
            const c_count = array.counts[c];
            assert(c_count >= half or c == array.node_count - 1);
            assert(c_count <= node_capacity);

            if (b_count + c_count <= node_capacity) {
                mem.copy(
                    T,
                    b_elements[b_count..][0..c_count],
                    c_elements[0..c_count],
                );
                array.counts[b] += c_count;
                array.counts[c] = 0;
                array.remove_empty_node_at(node_pool, c);
            } else if (b_count < half) {
                assert(b_count == half - 1);
                assert(c_count > half + 1);

                b_elements[b_count] = c_elements[0];
                mem.copy(
                    T,
                    c_elements[0 .. c_count - 1],
                    c_elements[1..c_count],
                );

                array.counts[b] += 1;
                array.counts[c] -= 1;
                array.indexes[c] += 1;

                assert(array.counts[b] == half);
                assert(array.counts[c] > half);
            }

            assert(array.counts[b] >= half);
        }

        /// Remove an empty node at index `node`.
        fn remove_empty_node_at(array: *Self, node_pool: *NodePool, node: u32) void {
            assert(array.node_count > 0);
            assert(node < array.node_count);
            assert(array.counts[node] == 0);

            node_pool.release(@ptrCast(NodePool.Node, array.nodes[node].?));

            // If we are removing the last node, nothing needs to be shifted.
            assert(node <= array.node_count - 1);
            if (node < array.node_count - 1) {
                mem.copy(
                    ?*[node_capacity]T,
                    array.nodes[node .. array.node_count - 1],
                    array.nodes[node + 1 .. array.node_count],
                );
                mem.copy(
                    u32,
                    array.counts[node .. array.node_count - 1],
                    array.counts[node + 1 .. array.node_count],
                );
                mem.copy(
                    u32,
                    array.indexes[node .. array.node_count - 1],
                    array.indexes[node + 1 .. array.node_count],
                );
            }

            array.node_count -= 1;
            array.nodes[array.node_count] = null;
            array.counts[array.node_count] = undefined;
            array.indexes[array.node_count] = undefined;
        }

        pub fn node_elements(array: Self, node: u32) []T {
            assert(node < array.node_count);
            return array.nodes[node].?[0..array.counts[node]];
        }

        pub fn element_at_cursor(array: Self, cursor: Cursor) T {
            return array.node_elements(cursor.node)[cursor.relative_index];
        }

        pub fn first(_: Self) Cursor {
            return .{
                .node = 0,
                .relative_index = 0,
            };
        }

        pub fn last(array: Self) Cursor {
            if (array.node_count == 0) return array.first();

            return .{
                .node = array.node_count - 1,
                .relative_index = array.counts[array.node_count - 1] - 1,
            };
        }

        pub fn len(array: Self) u32 {
            if (array.node_count == 0) return 0;
            const result = array.indexes[array.node_count - 1] + array.counts[array.node_count - 1];
            assert(result <= element_count_max);
            return result;
        }

        // TODO Consider enabling ReleaseFast for this once tested.
        pub fn absolute_index_for_cursor(array: Self, cursor: Cursor) u32 {
            if (array.node_count == 0) {
                assert(cursor.node == 0);
                assert(cursor.relative_index == 0);
                return 0;
            }
            assert(cursor.node < array.node_count);
            if (cursor.node == array.node_count - 1) {
                // Insertion may target the index one past the end of the array.
                assert(cursor.relative_index <= array.counts[cursor.node]);
            } else {
                assert(cursor.relative_index < array.counts[cursor.node]);
            }
            return array.indexes[cursor.node] + cursor.relative_index;
        }

        fn cursor_for_absolute_index(array: Self, absolute_index: u32) Cursor {
            // This function could handle node_count == 0 by returning a zero Cursor.
            // However, this is an internal function and we don't require this behavior.
            assert(array.node_count > 0);

            assert(absolute_index < element_count_max);
            assert(absolute_index <= array.len());

            var node: u32 = 0;
            while (node + 1 < array.node_count and
                absolute_index >= array.indexes[node + 1])
            {
                node += 1;
            }
            assert(node < array.node_count);

            const relative_index = absolute_index - array.indexes[node];

            if (node == array.node_count - 1) {
                // Insertion may target the index one past the end of the array.
                assert(relative_index <= array.counts[node]);
            } else {
                assert(relative_index < array.counts[node]);
            }

            return .{
                .node = node,
                .relative_index = relative_index,
            };
        }

        pub const Iterator = struct {
            array: *const Self,
            direction: Direction,

            cursor: Cursor,

            /// The user may set this early to stop iteration. For example,
            /// if the returned table info is outside the key range.
            done: bool = false,

            pub fn next(it: *Iterator) ?*const T {
                if (it.done) return null;

                assert(it.cursor.node < it.array.node_count);

                const elements = it.array.node_elements(it.cursor.node);
                const element = &elements[it.cursor.relative_index];

                switch (it.direction) {
                    .ascending => {
                        if (it.cursor.relative_index == elements.len - 1) {
                            if (it.cursor.node == it.array.node_count - 1) {
                                it.done = true;
                            } else {
                                it.cursor.node += 1;
                                it.cursor.relative_index = 0;
                            }
                        } else {
                            it.cursor.relative_index += 1;
                        }
                    },
                    .descending => {
                        if (it.cursor.relative_index == 0) {
                            if (it.cursor.node == 0) {
                                it.done = true;
                            } else {
                                it.cursor.node -= 1;
                                it.cursor.relative_index = it.array.counts[it.cursor.node] - 1;
                            }
                        } else {
                            it.cursor.relative_index -= 1;
                        }
                    },
                }

                return element;
            }
        };

        pub fn iterator(
            array: *const Self,
            /// Absolute index to start iteration at.
            absolute_index: u32,
            /// The start node allows us to skip over nodes as an optimization.
            /// If ascending start from the first element of the start node and ascend.
            /// If descending start from the last element of the start node and descend.
            start_node: u32,
            direction: Direction,
        ) Iterator {
            if (array.node_count == 0) {
                assert(absolute_index == 0);
                assert(start_node == 0);

                return Iterator{
                    .array = array,
                    .direction = direction,
                    .cursor = .{ .node = 0, .relative_index = 0 },
                    .done = true,
                };
            }

            assert(start_node < array.node_count);
            assert(absolute_index < element_count_max);
            assert(absolute_index <
                array.indexes[array.node_count - 1] + array.counts[array.node_count - 1]);

            switch (direction) {
                .ascending => {
                    assert(absolute_index >= array.indexes[start_node]);

                    var node = start_node;
                    while (node + 1 < array.node_count and
                        absolute_index >= array.indexes[node + 1])
                    {
                        node += 1;
                    }
                    assert(node < array.node_count);

                    const relative_index = absolute_index - array.indexes[node];
                    assert(relative_index < array.counts[node]);

                    return .{
                        .array = array,
                        .direction = direction,
                        .cursor = .{
                            .node = node,
                            .relative_index = relative_index,
                        },
                    };
                },
                .descending => {
                    assert(absolute_index < array.indexes[start_node] + array.counts[start_node]);

                    var node = start_node;
                    while (node > 0 and absolute_index < array.indexes[node]) {
                        node -= 1;
                    }

                    const relative_index = absolute_index - array.indexes[node];
                    assert(relative_index < array.counts[node]);

                    return .{
                        .array = array,
                        .direction = direction,
                        .cursor = .{
                            .node = node,
                            .relative_index = relative_index,
                        },
                    };
                },
            }
        }
    };
}

fn TestContext(
    comptime T: type,
    comptime node_size: u32,
    comptime element_count_max: u32,
) type {
    return struct {
        const Self = @This();

        const testing = std.testing;

        const log = false;

        const NodePool = @import("node_pool.zig").NodePool;

        const TestPool = NodePool(node_size, @alignOf(T));
        const TestArray = SegmentedArray(T, TestPool, element_count_max);

        random: std.rand.Random,

        pool: TestPool,
        array: TestArray,

        reference: std.ArrayList(T),

        inserts: u64 = 0,
        removes: u64 = 0,

        fn init(random: std.rand.Random) !Self {
            var pool = try TestPool.init(testing.allocator, TestArray.node_count_max);
            errdefer pool.deinit(testing.allocator);

            var array = try TestArray.init(testing.allocator);
            errdefer array.deinit(testing.allocator, &pool);

            var reference = std.ArrayList(T).init(testing.allocator);
            errdefer reference.deinit();

            try reference.ensureTotalCapacity(element_count_max);

            return Self{
                .random = random,
                .pool = pool,
                .array = array,
                .reference = reference,
            };
        }

        fn deinit(context: *Self) void {
            context.array.deinit(testing.allocator, &context.pool);
            context.pool.deinit(testing.allocator);

            context.reference.deinit();
        }

        fn run(context: *Self) !void {
            {
                var i: usize = 0;
                while (i < element_count_max * 2) : (i += 1) {
                    switch (context.random.uintLessThanBiased(u32, 100)) {
                        0...59 => try context.insert(),
                        60...99 => try context.remove(),
                        else => unreachable,
                    }
                }
            }

            {
                var i: usize = 0;
                while (i < element_count_max * 2) : (i += 1) {
                    switch (context.random.uintLessThanBiased(u32, 100)) {
                        0...39 => try context.insert(),
                        40...99 => try context.remove(),
                        else => unreachable,
                    }
                }
            }

            try context.remove_all();
        }

        fn insert(context: *Self) !void {
            const reference_len = @intCast(u32, context.reference.items.len);
            const count_free = element_count_max - reference_len;

            if (count_free == 0) return;

            var buffer: [TestArray.node_capacity]T = undefined;

            const count_max = @minimum(count_free, TestArray.node_capacity);
            const count = context.random.uintAtMostBiased(u32, count_max - 1) + 1;
            context.random.bytes(mem.sliceAsBytes(buffer[0..count]));

            assert(context.reference.items.len <= element_count_max);
            const index = context.random.uintAtMostBiased(u32, reference_len);

            context.array.insert_elements(&context.pool, index, buffer[0..count]);

            // TODO the standard library could use an AssumeCapacity variant of this.
            context.reference.insertSlice(index, buffer[0..count]) catch unreachable;

            context.inserts += count;

            try context.verify();
        }

        fn remove(context: *Self) !void {
            const reference_len = @intCast(u32, context.reference.items.len);
            if (reference_len == 0) return;

            const count_max = @minimum(reference_len, TestArray.node_capacity);
            const count = context.random.uintAtMostBiased(u32, count_max - 1) + 1;

            assert(context.reference.items.len <= element_count_max);
            const index = context.random.uintAtMostBiased(u32, reference_len - count);

            context.array.remove_elements(&context.pool, index, count);

            context.reference.replaceRange(index, count, &[0]T{}) catch unreachable;

            context.removes += count;

            try context.verify();
        }

        fn remove_all(context: *Self) !void {
            while (context.reference.items.len > 0) try context.remove();

            try testing.expectEqual(@as(u32, 0), context.array.len());
            try testing.expect(context.inserts > 0);
            try testing.expect(context.inserts == context.removes);

            if (log) {
                std.debug.print("\ninserts: {}, removes: {}\n", .{
                    context.inserts,
                    context.removes,
                });
            }

            try context.verify();
        }

        fn verify(context: *Self) !void {
            try testing.expectEqual(context.reference.items.len, context.array.len());

            {
                var it = context.array.iterator(0, 0, .ascending);

                for (context.reference.items) |expect| {
                    const actual = it.next() orelse return error.TestUnexpectedResult;
                    try testing.expectEqual(expect, actual.*);
                }
                try testing.expectEqual(@as(?*const T, null), it.next());
            }

            {
                var it = context.array.iterator(
                    @intCast(u32, context.reference.items.len) -| 1,
                    context.array.last().node,
                    .descending,
                );

                var i = context.reference.items.len;
                while (i > 0) {
                    i -= 1;

                    const expect = context.reference.items[i];
                    const actual = it.next() orelse return error.TestUnexpectedResult;
                    try testing.expectEqual(expect, actual.*);
                }
                try testing.expectEqual(@as(?*const T, null), it.next());
            }

            try testing.expectEqual(context.reference.items.len, context.array.len());

            if (context.array.len() == 0) {
                try testing.expectEqual(@as(u32, 0), context.array.node_count);
            }

            for (context.array.nodes[context.array.node_count..]) |node| {
                try testing.expectEqual(@as(?*[TestArray.node_capacity]T, null), node);
            }
        }
    };
}

test "SegmentedArray" {
    const seed = 42;

    var prng = std.rand.DefaultPrng.init(seed);
    const random = prng.random();

    // TODO Import this type from lsm/tree.zig.
    const TableInfo = extern struct {
        checksum: u128,
        key_min: u128,
        key_max: u128,

        address: u64,

        /// Set to the current snapshot tick on creation.
        snapshot_min: u64,
        /// Initially math.maxInt(u64) on creation, set to the current
        /// snapshot tick on deletion.
        snapshot_max: u64,
        flags: u64 = 0,
    };

    comptime {
        assert(@sizeOf(TableInfo) == 48 + @sizeOf(u128) * 2);
        assert(@alignOf(TableInfo) == 16);
    }

    const Options = struct {
        element_type: type,
        node_size: u32,
        element_count_max: u32,
    };

    var tested_padding = false;
    var tested_capacity_min = false;

    inline for (.{
        Options{ .element_type = u32, .node_size = 16, .element_count_max = 512 },
        Options{ .element_type = u32, .node_size = 8, .element_count_max = 512 },
        Options{ .element_type = u32, .node_size = 8, .element_count_max = 3 },
        Options{ .element_type = TableInfo, .node_size = 256, .element_count_max = 3 },
        Options{ .element_type = TableInfo, .node_size = 256, .element_count_max = 4 },
        Options{ .element_type = TableInfo, .node_size = 256, .element_count_max = 1024 },
        Options{ .element_type = TableInfo, .node_size = 1024, .element_count_max = 1024 },
    }) |options| {
        const Context = TestContext(
            options.element_type,
            options.node_size,
            options.element_count_max,
        );

        var context = try Context.init(random);
        defer context.deinit();

        try context.run();

        if (options.node_size % @sizeOf(options.element_type) != 0) tested_padding = true;
        if (Context.TestArray.node_capacity == 2) tested_capacity_min = true;
    }

    assert(tested_padding);
    assert(tested_capacity_min);
}
