const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
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
            assert(elements.len > 0);
            assert(absolute_index + elements.len <= element_count_max);

            var i: u32 = 0;
            while (i < elements.len) {
                const batch = math.min(node_capacity, elements.len - i);
                array.insert_elements_batch(
                    node_pool,
                    absolute_index + i,
                    elements[i..][0..batch],
                );
                i += batch;
            }
            assert(i == elements.len);
        }

        fn insert_elements_batch(
            array: *Self,
            node_pool: *NodePool,
            absolute_index: u32,
            elements: []T,
        ) void {
            assert(elements.len > 0);
            assert(elements.len <= node_capacity);
            assert(absolute_index + elements.len <= element_count_max);

            if (array.node_count == 0) {
                assert(absolute_index == 0);

                array.insert_empty_node_at(node_pool, 0);

                assert(array.node_count == 1);
                assert(array.nodes[0] != null);
                assert(array.counts[0] == 0);
                assert(array.indexes[0] == 0);
            }

            const cursor = array.cursor_for_absolute_index(absolute_index);
            assert(cursor.node < array.node_count);

            const total = array.counts[cursor.node] + @intCast(u32, elements.len);
            if (total <= node_capacity) {
                const pointer = array.nodes[cursor.node].?;
                mem.copyBackwards(
                    T,
                    pointer[cursor.relative_index + elements.len ..],
                    pointer[cursor.relative_index..array.counts[cursor.node]],
                );
                mem.copy(T, pointer[cursor.relative_index..], elements);
                array.counts[cursor.node] += @intCast(u32, elements.len);
                if (cursor.node < array.node_count - 1) {
                    for (array.indexes[cursor.node + 1 .. array.node_count]) |*i| {
                        i.* += @intCast(u32, elements.len);
                    }
                }
                return;
            }

            // Insert a new node after the node being split.
            const a = cursor.node;
            const b = a + 1;
            array.insert_empty_node_at(node_pool, b);

            const a_half = utils.div_ceil(total, 2);
            const b_half = total - a_half;
            assert(a_half >= b_half);
            assert(a_half + b_half == total);

            const a_pointer = array.nodes[a].?;
            const b_pointer = array.nodes[b].?;

            // The 1st case can be seen as a special case of the 2nd.
            // The 5th case can be seen as a special case of the 4th.
            //
            // elements: [yyyyyy], relative_index: 0
            // [xxxxx_]
            // [yyyyyy][xxxxx_]
            //
            // elements: [yy], relative_index: 1
            // [xxxxx_]
            // [xyyx__][xxx___]
            //
            // elements: [yy], relative_index: 2
            // [xxx_]
            // [xxy_][yx__]
            //
            // elements: [yy], relative_index: 5
            // [xxxxxx]
            // [xxxx__][xyyx__]
            //
            // elements: [yyyyy_], relative_index: 5
            // [xxxxx_]
            // [xxxxx_][yyyyy_]

            assert(cursor.relative_index <= array.counts[a]);

            const existing_a_head = a_pointer[0..@minimum(a_half, cursor.relative_index)];
            const existing_b_head = a_pointer[existing_a_head.len..cursor.relative_index];

            const existing_a_tail = a_pointer[existing_a_head.len..][0..a_half -|
                (cursor.relative_index + elements.len)];
            const existing_b_tail = a_pointer[existing_a_head.len + existing_a_tail.len +
                existing_b_head.len .. array.counts[a]];

            const elements_a = elements[0 .. a_half - existing_a_head.len - existing_a_tail.len];
            const elements_b = elements[elements_a.len..];

            assert(array.counts[a] == existing_a_head.len + existing_b_head.len +
                existing_a_tail.len + existing_b_tail.len);
            assert(elements.len == elements_a.len + elements_b.len);
            assert(a_half == existing_a_head.len + elements_a.len + existing_a_tail.len);
            assert(b_half == existing_b_head.len + elements_b.len + existing_b_tail.len);
            assert(total == a_half + b_half);

            if (existing_a_tail.len > 0) assert(existing_b_head.len == 0 and elements_b.len == 0);
            if (existing_b_head.len > 0) assert(elements_a.len == 0 and existing_a_tail.len == 0);
            if (elements_a.len > 0) assert(existing_b_head.len == 0);
            if (elements_b.len > 0) assert(existing_a_tail.len == 0);

            assert(existing_a_head.ptr == a_pointer);
            assert(existing_a_head.ptr + existing_a_head.len == existing_a_tail.ptr);
            assert(existing_a_head.ptr + existing_a_head.len == existing_b_head.ptr);
            if (existing_b_head.len > 0) {
                assert(existing_b_head.ptr + existing_b_head.len == existing_b_tail.ptr);
            } else {
                assert(existing_a_tail.ptr + existing_a_tail.len == existing_b_tail.ptr);
            }

            mem.copy(T, b_pointer[existing_b_head.len + elements_b.len ..], existing_b_tail);
            mem.copy(T, b_pointer[existing_b_head.len..], elements_b);
            mem.copy(T, b_pointer, existing_b_head);

            mem.copyBackwards(
                T,
                a_pointer[existing_a_head.len + elements_a.len ..],
                existing_a_tail,
            );
            mem.copy(T, a_pointer[existing_a_head.len..], elements_a);

            array.counts[a] = a_half;
            array.counts[b] = b_half;

            array.indexes[b] = array.indexes[a] + array.counts[a];
            if (b < array.node_count - 1) {
                for (array.indexes[b + 1 .. array.node_count]) |*i| {
                    i.* += @intCast(u32, elements.len);
                }
            }
        }

        /// Insert an empty node at index `node`.
        fn insert_empty_node_at(array: *Self, node_pool: *NodePool, node: u32) void {
            assert(array.node_count + 1 < node_count_max);

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
            array.indexes[node] = if (node == 0) 0 else array.indexes[node - 1] + array.counts[node - 1];
        }

        pub fn remove_elements(
            array: *Self,
            node_pool: *NodePool,
            absolute_index: u32,
            count: u32,
        ) void {
            assert(absolute_index + count <= element_count_max);
            assert(count > 0);

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

            var buffer: [TestArray.node_capacity * 3]T = undefined;

            const count_max = @minimum(count_free, TestArray.node_capacity * 3);
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

            const count_max = @minimum(reference_len, TestArray.node_capacity * 3);
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
            if (log) {
                std.debug.print("expect: ", .{});
                for (context.reference.items) |i| std.debug.print("{}, ", .{i});

                std.debug.print("\nactual: ", .{});
                var it = context.array.iterator(0, 0, .ascending);
                while (it.next()) |i| std.debug.print("{}, ", .{i.*});
                std.debug.print("\n", .{});
            }

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

            if (context.reference.items.len > 0) {
                const reference_len = @intCast(u32, context.reference.items.len);
                const index = context.random.uintLessThanBiased(u32, reference_len);
                const cursor = context.array.cursor_for_absolute_index(index);

                {
                    const start_node = context.random.uintAtMostBiased(u32, cursor.node);

                    var it = context.array.iterator(index, start_node, .ascending);

                    for (context.reference.items[index..]) |expect| {
                        const actual = it.next() orelse return error.TestUnexpectedResult;
                        try testing.expectEqual(expect, actual.*);
                    }
                    try testing.expectEqual(@as(?*const T, null), it.next());
                }

                {
                    const start_node = cursor.node + context.random.uintAtMostBiased(
                        u32,
                        context.array.node_count - 1 - cursor.node,
                    );
                    assert(start_node >= cursor.node);
                    assert(start_node < context.array.node_count);

                    var it = context.array.iterator(index, start_node, .descending);

                    var i = index + 1;
                    while (i > 0) {
                        i -= 1;

                        const expect = context.reference.items[i];
                        const actual = it.next() orelse return error.TestUnexpectedResult;
                        try testing.expectEqual(expect, actual.*);
                    }
                    try testing.expectEqual(@as(?*const T, null), it.next());
                }
            }

            for (context.array.counts[0..context.array.node_count -| 1]) |count| {
                try testing.expect(count >= @divExact(TestArray.node_capacity, 2));
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
