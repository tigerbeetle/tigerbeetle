const std = @import("std");

const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const div_ceil = @import("../util.zig").div_ceil;
const binary_search_values_raw = @import("binary_search.zig").binary_search_values_raw;
const binary_search_keys = @import("binary_search.zig").binary_search_keys;
const Direction = @import("direction.zig").Direction;

pub const Cursor = struct {
    node: u32,
    relative_index: u32,
};

pub fn SegmentedArray(
    comptime T: type,
    comptime NodePool: type,
    comptime element_count_max: u32,
) type {
    return SegmentedArrayType(T, NodePool, element_count_max, null, {}, {});
}

pub fn SortedSegmentedArray(
    comptime T: type,
    comptime NodePool: type,
    comptime element_count_max: u32,
    comptime Key: type,
    comptime key_from_value: fn (*const T) callconv(.Inline) Key,
    comptime compare_keys: fn (Key, Key) callconv(.Inline) math.Order,
) type {
    return SegmentedArrayType(T, NodePool, element_count_max, Key, key_from_value, compare_keys);
}

fn SegmentedArrayType(
    comptime T: type,
    comptime NodePool: type,
    comptime element_count_max: u32,
    // Set when the SegmentedArray is ordered:
    comptime Key: ?type,
    comptime key_from_value: if (Key) |K| (fn (*const T) callconv(.Inline) K) else void,
    comptime compare_keys: if (Key) |K| (fn (K, K) callconv(.Inline) math.Order) else void,
) type {
    return struct {
        const Self = @This();

        // We can't use @divExact() here as we store TableInfo structs of various sizes in this
        // data structure. This means that there may be padding at the end of the node.
        pub const node_capacity = blk: {
            const max = @divFloor(NodePool.node_size, @sizeOf(T));

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
            const elements_per_node_min = @divExact(node_capacity, 2);
            break :blk div_ceil(element_count_max, elements_per_node_min);
        };

        node_count: u32 = 0,
        /// This is the segmented array, the first key_node_count pointers are non-null.
        /// The rest are null. We only use optional pointers here to get safety checks.
        nodes: *[node_count_max]?*[node_capacity]T,
        /// Since nodes in a segmented array are usually not full, computing the absolute index
        /// of an element in the full array is O(N) over the number of nodes. To avoid this cost
        /// we precompute the absolute index of the first element of each node.
        /// To avoid a separate counts field, we derive the number of elements in a node from the
        /// index of that node and the next node.
        /// To avoid special casing the count() function for the last node, we increase the array
        /// length by 1 and store the total element count in the last slot.
        indexes: *[node_count_max + 1]u32,

        pub fn init(allocator: mem.Allocator) !Self {
            const nodes = try allocator.create([node_count_max]?*[node_capacity]T);
            errdefer allocator.destroy(nodes);

            const indexes = try allocator.create([node_count_max + 1]u32);
            errdefer allocator.destroy(indexes);

            mem.set(?*[node_capacity]T, nodes, null);
            indexes[0] = 0;

            return Self{
                .nodes = nodes,
                .indexes = indexes,
            };
        }

        pub fn deinit(array: Self, allocator: mem.Allocator, node_pool: ?*NodePool) void {
            for (array.nodes[0..array.node_count]) |node| {
                node_pool.?.release(
                    @ptrCast(NodePool.Node, @alignCast(NodePool.node_alignment, node.?)),
                );
            }
            allocator.free(array.nodes);
            allocator.free(array.indexes);
        }

        pub usingnamespace if (Key) |_| struct {
            /// Returns the absolute index of the element being inserted.
            pub fn insert_element(
                array: *Self,
                node_pool: *NodePool,
                element: T,
            ) u32 {
                const cursor = array.search(key_from_value(&element));
                const absolute_index = array.absolute_index_for_cursor(cursor);
                array.insert_elements_at_absolute_index(node_pool, absolute_index, &[_]T{element});
                return absolute_index;
            }
        } else struct {
            pub fn insert_elements(
                array: *Self,
                node_pool: *NodePool,
                absolute_index: u32,
                elements: []const T,
            ) void {
                return array.insert_elements_at_absolute_index(
                    node_pool,
                    absolute_index,
                    elements,
                );
            }
        };

        fn insert_elements_at_absolute_index(
            array: *Self,
            node_pool: *NodePool,
            absolute_index: u32,
            elements: []const T,
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
            elements: []const T,
        ) void {
            assert(elements.len > 0);
            assert(elements.len <= node_capacity);
            assert(absolute_index + elements.len <= element_count_max);

            if (array.node_count == 0) {
                assert(absolute_index == 0);

                array.insert_empty_node_at(node_pool, 0);

                assert(array.node_count == 1);
                assert(array.nodes[0] != null);
                assert(array.indexes[0] == 0);
                assert(array.indexes[1] == 0);
            }

            const cursor = array.cursor_for_absolute_index(absolute_index);
            assert(cursor.node < array.node_count);

            const a = cursor.node;
            const a_pointer = array.nodes[a].?;

            const total = array.count(a) + @intCast(u32, elements.len);
            if (total <= node_capacity) {
                mem.copyBackwards(
                    T,
                    a_pointer[cursor.relative_index + elements.len ..],
                    a_pointer[cursor.relative_index..array.count(a)],
                );
                mem.copy(T, a_pointer[cursor.relative_index..], elements);

                array.increment_indexes_after(a, @intCast(u32, elements.len));
                return;
            }

            // Insert a new node after the node being split.
            const b = a + 1;
            array.insert_empty_node_at(node_pool, b);
            const b_pointer = array.nodes[b].?;

            const a_half = div_ceil(total, 2);
            const b_half = total - a_half;
            assert(a_half >= b_half);
            assert(a_half + b_half == total);

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

            assert(cursor.relative_index <= array.count(a));

            const existing_a_head = a_pointer[0..@minimum(a_half, cursor.relative_index)];
            const existing_b_head = a_pointer[existing_a_head.len..cursor.relative_index];

            const existing_a_tail = a_pointer[existing_a_head.len..][0..a_half -|
                (cursor.relative_index + elements.len)];
            const existing_b_tail = a_pointer[existing_a_head.len + existing_a_tail.len +
                existing_b_head.len .. array.count(a)];

            const elements_a = elements[0 .. a_half - existing_a_head.len - existing_a_tail.len];
            const elements_b = elements[elements_a.len..];

            assert(array.count(a) == existing_a_head.len + existing_b_head.len +
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

            array.indexes[b] = array.indexes[a] + a_half;
            array.increment_indexes_after(b, @intCast(u32, elements.len));
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
            }
            mem.copyBackwards(
                u32,
                array.indexes[node + 1 .. array.node_count + 2],
                array.indexes[node .. array.node_count + 1],
            );

            array.node_count += 1;
            array.nodes[node] = @ptrCast(*[node_capacity]T, node_pool.acquire());
            assert(array.indexes[node] == array.indexes[node + 1]);
        }

        pub fn remove_elements(
            array: *Self,
            node_pool: *NodePool,
            absolute_index: u32,
            remove_count: u32,
        ) void {
            assert(array.node_count > 0);
            assert(remove_count > 0);
            assert(absolute_index + remove_count <= element_count_max);
            assert(absolute_index + remove_count <= array.indexes[array.node_count]);

            const half = @divExact(node_capacity, 2);

            var i: u32 = remove_count;
            while (i > 0) {
                const batch = math.min(half, i);
                array.remove_elements_batch(node_pool, absolute_index, batch);
                i -= batch;
            }
        }

        fn remove_elements_batch(
            array: *Self,
            node_pool: *NodePool,
            absolute_index: u32,
            remove_count: u32,
        ) void {
            assert(array.node_count > 0);

            // Restricting the batch size to half node capacity ensures that elements
            // are removed from at most two nodes.
            const half = @divExact(node_capacity, 2);
            assert(remove_count <= half);
            assert(remove_count > 0);

            assert(absolute_index + remove_count <= element_count_max);
            assert(absolute_index + remove_count <= array.indexes[array.node_count]);

            const cursor = array.cursor_for_absolute_index(absolute_index);
            assert(cursor.node < array.node_count);

            const a = cursor.node;
            const a_pointer = array.nodes[a].?;
            const a_remaining = cursor.relative_index;

            // Remove elements from exactly one node:
            if (a_remaining + remove_count <= array.count(a)) {
                mem.copy(
                    T,
                    a_pointer[a_remaining..],
                    a_pointer[a_remaining + remove_count .. array.count(a)],
                );

                array.decrement_indexes_after(a, remove_count);

                array.maybe_remove_or_merge_node_with_next(node_pool, a);
                return;
            }

            // Remove elements from exactly two nodes:

            const b = a + 1;
            const b_pointer = array.nodes[b].?;
            const b_remaining = b_pointer[remove_count -
                (array.count(a) - a_remaining) .. array.count(b)];

            assert(@ptrToInt(b_remaining.ptr) > @ptrToInt(b_pointer));

            // Only one of these nodes may become empty, as we limit batch size to
            // half node capacity.
            assert(a_remaining > 0 or b_remaining.len > 0);

            if (a_remaining >= half) {
                mem.copy(T, b_pointer, b_remaining);

                array.indexes[b] = array.indexes[a] + a_remaining;
                array.decrement_indexes_after(b, remove_count);

                array.maybe_remove_or_merge_node_with_next(node_pool, b);
            } else if (b_remaining.len >= half) {
                assert(a_remaining < half);

                array.indexes[b] = array.indexes[a] + a_remaining;
                array.decrement_indexes_after(b, remove_count);

                array.maybe_merge_nodes(node_pool, a, b_remaining);
            } else {
                assert(a_remaining < half and b_remaining.len < half);
                assert(a_remaining + b_remaining.len <= node_capacity);

                mem.copy(T, a_pointer[a_remaining..], b_remaining);

                array.indexes[b] = array.indexes[a] + a_remaining + @intCast(u32, b_remaining.len);
                array.decrement_indexes_after(b, remove_count);

                array.remove_empty_node_at(node_pool, b);
                array.maybe_remove_or_merge_node_with_next(node_pool, a);
            }
        }

        fn maybe_remove_or_merge_node_with_next(
            array: *Self,
            node_pool: *NodePool,
            node: u32,
        ) void {
            assert(node < array.node_count);

            if (array.count(node) == 0) {
                array.remove_empty_node_at(node_pool, node);
                return;
            }

            if (node == array.node_count - 1) return;

            const next_elements = array.nodes[node + 1].?[0..array.count(node + 1)];
            array.maybe_merge_nodes(node_pool, node, next_elements);
        }

        fn maybe_merge_nodes(
            array: *Self,
            node_pool: *NodePool,
            node: u32,
            elements_next_node: []T,
        ) void {
            const half = @divExact(node_capacity, 2);

            const a = node;
            const a_pointer = array.nodes[a].?;
            assert(array.count(a) <= node_capacity);

            // The elements_next_node slice may not be at the start of the node,
            // but the length of the slice will match count(b).
            const b = a + 1;
            const b_pointer = array.nodes[b].?;
            const b_elements = elements_next_node;
            assert(b_elements.len == array.count(b));
            assert(b_elements.len > 0);
            assert(b_elements.len >= half or b == array.node_count - 1);
            assert(b_elements.len <= node_capacity);
            assert(@ptrToInt(b_elements.ptr) >= @ptrToInt(b_pointer));

            // Our function would still be correct if this assert fails, but we would
            // unnecessarily copy all elements of b to node a and then delete b
            // instead of simply deleting a.
            assert(!(array.count(a) == 0 and b_pointer == b_elements.ptr));

            const total = array.count(a) + @intCast(u32, b_elements.len);
            if (total <= node_capacity) {
                mem.copy(T, a_pointer[array.count(a)..], b_elements);

                array.indexes[b] = array.indexes[b + 1];
                array.remove_empty_node_at(node_pool, b);

                assert(array.count(a) >= half or a == array.node_count - 1);
            } else if (array.count(a) < half) {
                const a_half = div_ceil(total, 2);
                const b_half = total - a_half;
                assert(a_half >= b_half);
                assert(a_half + b_half == total);

                mem.copy(
                    T,
                    a_pointer[array.count(a)..a_half],
                    b_elements[0 .. a_half - array.count(a)],
                );
                mem.copy(T, b_pointer, b_elements[a_half - array.count(a) ..]);

                array.indexes[b] = array.indexes[a] + a_half;

                assert(array.count(a) >= half);
                assert(array.count(b) >= half);
            } else {
                assert(b_pointer == b_elements.ptr);
                assert(array.indexes[b] + b_elements.len == array.indexes[b + 1]);
            }
        }

        /// Remove an empty node at index `node`.
        fn remove_empty_node_at(array: *Self, node_pool: *NodePool, node: u32) void {
            assert(array.node_count > 0);
            assert(node < array.node_count);
            assert(array.count(node) == 0);

            node_pool.release(
                @ptrCast(NodePool.Node, @alignCast(NodePool.node_alignment, array.nodes[node].?)),
            );

            assert(node <= array.node_count - 1);
            if (node < array.node_count - 1) {
                mem.copy(
                    ?*[node_capacity]T,
                    array.nodes[node .. array.node_count - 1],
                    array.nodes[node + 1 .. array.node_count],
                );
            }
            mem.copy(
                u32,
                array.indexes[node..array.node_count],
                array.indexes[node + 1 .. array.node_count + 1],
            );

            array.node_count -= 1;
            array.nodes[array.node_count] = null;
            array.indexes[array.node_count + 1] = undefined;
        }

        inline fn count(array: Self, node: u32) u32 {
            const result = array.indexes[node + 1] - array.indexes[node];
            assert(result <= node_capacity);
            return result;
        }

        inline fn increment_indexes_after(array: *Self, node: u32, delta: u32) void {
            for (array.indexes[node + 1 .. array.node_count + 1]) |*i| i.* += delta;
        }

        inline fn decrement_indexes_after(array: *Self, node: u32, delta: u32) void {
            for (array.indexes[node + 1 .. array.node_count + 1]) |*i| i.* -= delta;
        }

        pub inline fn node_elements(array: Self, node: u32) []T {
            assert(node < array.node_count);
            return array.nodes[node].?[0..array.count(node)];
        }

        pub inline fn node_last_element(array: Self, node: u32) T {
            return array.node_elements(node)[array.count(node) - 1];
        }

        pub inline fn element_at_cursor(array: Self, cursor: Cursor) T {
            return array.node_elements(cursor.node)[cursor.relative_index];
        }

        pub inline fn first(_: Self) Cursor {
            return .{
                .node = 0,
                .relative_index = 0,
            };
        }

        pub inline fn last(array: Self) Cursor {
            if (array.node_count == 0) return array.first();

            return .{
                .node = array.node_count - 1,
                .relative_index = array.count(array.node_count - 1) - 1,
            };
        }

        pub inline fn len(array: Self) u32 {
            const result = array.indexes[array.node_count];
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
                assert(cursor.relative_index <= array.count(cursor.node));
            } else {
                assert(cursor.relative_index < array.count(cursor.node));
            }
            return array.indexes[cursor.node] + cursor.relative_index;
        }

        fn cursor_for_absolute_index(array: Self, absolute_index: u32) Cursor {
            // This function could handle node_count == 0 by returning a zero Cursor.
            // However, this is an internal function and we don't require this behavior.
            assert(array.node_count > 0);

            assert(absolute_index < element_count_max);
            assert(absolute_index <= array.len());

            const result = binary_search_keys(u32, struct {
                inline fn compare(a: u32, b: u32) math.Order {
                    return math.order(a, b);
                }
            }.compare, array.indexes[0..array.node_count], absolute_index);

            if (result.exact) {
                return .{
                    .node = result.index,
                    .relative_index = 0,
                };
            } else {
                const node = result.index - 1;
                const relative_index = absolute_index - array.indexes[node];
                if (node == array.node_count - 1) {
                    // Insertion may target the index one past the end of the array.
                    assert(relative_index <= array.count(node));
                } else {
                    assert(relative_index < array.count(node));
                }
                return .{
                    .node = node,
                    .relative_index = relative_index,
                };
            }
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
                                it.cursor.relative_index = it.array.count(it.cursor.node) - 1;
                            }
                        } else {
                            it.cursor.relative_index -= 1;
                        }
                    },
                }

                return element;
            }
        };

        pub fn iterator_from_cursor(
            array: *const Self,
            /// First element of iteration.
            cursor: Cursor,
            direction: Direction,
        ) Iterator {
            if (array.node_count == 0) {
                assert(cursor.node == 0);
                assert(cursor.relative_index == 0);

                return Iterator{
                    .array = array,
                    .direction = direction,
                    .cursor = .{ .node = 0, .relative_index = 0 },
                    .done = true,
                };
            } else {
                assert(cursor.node < array.node_count);
                assert(cursor.relative_index < array.count(cursor.node));

                return .{
                    .array = array,
                    .direction = direction,
                    .cursor = cursor,
                };
            }
        }

        pub fn iterator_from_index(
            array: *const Self,
            /// First element of iteration.
            absolute_index: u32,
            direction: Direction,
        ) Iterator {
            assert(absolute_index < element_count_max);

            if (array.node_count == 0) {
                assert(absolute_index == 0);

                return Iterator{
                    .array = array,
                    .direction = direction,
                    .cursor = .{ .node = 0, .relative_index = 0 },
                    .done = true,
                };
            } else {
                assert(absolute_index < array.len());

                return Iterator{
                    .array = array,
                    .direction = direction,
                    .cursor = array.cursor_for_absolute_index(absolute_index),
                };
            }
        }

        pub usingnamespace if (Key) |K| struct {
            /// Returns a cursor to the index of the key either exactly equal to the target key or,
            /// if there is no exact match, the next greatest key.
            pub fn search(
                array: *const Self,
                key: K,
            ) Cursor {
                if (array.node_count == 0) {
                    return .{
                        .node = 0,
                        .relative_index = 0,
                    };
                }

                var offset: usize = 0;
                var length: usize = array.node_count;
                while (length > 1) {
                    const half = length / 2;
                    const mid = offset + half;

                    const node = &array.nodes[mid].?[0];
                    // This trick seems to be what's needed to get llvm to emit branchless code for this,
                    // a ternary-style if expression was generated as a jump here for whatever reason.
                    const next_offsets = [_]usize{ offset, mid };
                    offset = next_offsets[@boolToInt(compare_keys(key_from_value(node), key) == .lt)];

                    length -= half;
                }

                // Unlike a normal binary search, don't increment the offset when "key" is higher
                // than the element — "round down" to the previous node.
                // This guarantees that the node result is never "== node_count".
                //
                // (If there are two adjacent nodes starting with keys A and C, and we search B,
                // we want to pick the A node.)
                const node = @intCast(u32, offset);
                assert(node < array.node_count);

                const relative_index = binary_search_values_raw(
                    K,
                    T,
                    key_from_value,
                    compare_keys,
                    array.node_elements(node),
                    key,
                );

                // Follow the same rule as absolute_index_for_cursor:
                // only return relative_index==array.count() at the last node.
                if (node + 1 < array.node_count and
                    relative_index == array.count(node))
                {
                    return .{
                        .node = node + 1,
                        .relative_index = 0,
                    };
                } else {
                    return .{
                        .node = node,
                        .relative_index = relative_index,
                    };
                }
            }
        } else struct {};
    };
}

fn TestContext(
    comptime T: type,
    comptime node_size: u32,
    comptime element_count_max: u32,
    comptime Key: type,
    comptime key_from_value: fn (*const T) callconv(.Inline) Key,
    comptime compare_keys: fn (Key, Key) callconv(.Inline) math.Order,
    comptime element_order: enum { sorted, unsorted },
) type {
    return struct {
        const Self = @This();

        const testing = std.testing;

        const log = false;

        const NodePool = @import("node_pool.zig").NodePool;

        // Test overaligned nodes to catch compile errors for missing @alignCast()
        const TestPool = NodePool(node_size, 2 * @alignOf(T));
        const TestArray = switch (element_order) {
            .sorted => SortedSegmentedArray(T, TestPool, element_count_max, Key, key_from_value, compare_keys),
            .unsorted => SegmentedArray(T, TestPool, element_count_max),
        };

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

            switch (element_order) {
                .unsorted => {
                    const index = context.random.uintAtMostBiased(u32, reference_len);

                    context.array.insert_elements(&context.pool, index, buffer[0..count]);
                    // TODO the standard library could use an AssumeCapacity variant of this.
                    context.reference.insertSlice(index, buffer[0..count]) catch unreachable;
                },
                .sorted => {
                    for (buffer[0..count]) |value| {
                        const index_actual = context.array.insert_element(&context.pool, value);
                        const index_expect = context.reference_index(key_from_value(&value));
                       // const index_expect = if (context.reference.items.len == 0) 0
                       //     else binary_search_values_raw(Key, T, key_from_value, compare_keys, context.reference.items, key_from_value(&value));
                        context.reference.insert(index_expect, value) catch unreachable;
                        try std.testing.expectEqual(index_expect, index_actual);
                    }
                },
            }
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
                var it = context.array.iterator_from_index(0, .ascending);
                while (it.next()) |i| std.debug.print("{}, ", .{i.*});
                std.debug.print("\n", .{});
            }

            try testing.expectEqual(context.reference.items.len, context.array.len());

            {
                var it = context.array.iterator_from_index(0, .ascending);

                for (context.reference.items) |expect| {
                    const actual = it.next() orelse return error.TestUnexpectedResult;
                    try testing.expectEqual(expect, actual.*);
                }
                try testing.expectEqual(@as(?*const T, null), it.next());
            }

            {
                var it = context.array.iterator_from_index(
                    @intCast(u32, context.reference.items.len) -| 1,
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

            {
                for (context.reference.items) |_, i| {
                    try testing.expect(std.meta.eql(
                        i,
                        context.array.absolute_index_for_cursor(
                            context.array.cursor_for_absolute_index(@intCast(u32, i)),
                        ),
                    ));
                }
            }

            if (element_order == .sorted) {
                for (context.reference.items) |*expect, i| {
                    if (i == 0) continue;
                    try testing.expect(compare_keys(
                        key_from_value(&context.reference.items[i - 1]),
                        key_from_value(expect),
                    ) != .gt);
                }
            }

            if (context.array.len() == 0) {
                try testing.expectEqual(@as(u32, 0), context.array.node_count);
            }

            for (context.array.nodes[context.array.node_count..]) |node| {
                try testing.expectEqual(@as(?*[TestArray.node_capacity]T, null), node);
            }

            {
                var i: u32 = 0;
                while (i < context.array.node_count -| 1) : (i += 1) {
                    try testing.expect(context.array.count(i) >=
                        @divExact(TestArray.node_capacity, 2));
                }
            }
            if (element_order == .sorted) try context.verify_search();
        }

        fn verify_search(context: *Self) !void {
            var queries: [20]Key = undefined;
            context.random.bytes(mem.sliceAsBytes(&queries));

            // Test min/max exceptional values on different SegmentedArray shapes.
            queries[0] = 0;
            queries[1] = math.maxInt(Key);

            for (queries) |query| {
                try testing.expectEqual(
                    context.reference_index(query),
                    context.array.absolute_index_for_cursor(context.array.search(query)),
                );
            }
        }

        fn reference_index(context: *const Self, key: Key) u32 {
            if (context.reference.items.len == 0) {
                return 0;
            } else {
                return binary_search_values_raw(
                    Key,
                    T,
                    key_from_value,
                    compare_keys,
                    context.reference.items,
                    key,
                );
            }
        }
    };
}

test "SegmentedArray" {
    const seed = 42;

    var prng = std.rand.DefaultPrng.init(seed);
    const random = prng.random();

    const Key = @import("composite_key.zig").CompositeKey(u64);
    const TableType = @import("table.zig").TableType;
    const TableInfoType = @import("manifest.zig").TableInfoType;
    const TableInfo = TableInfoType(TableType(
        Key,
        Key.Value,
        Key.compare_keys,
        Key.key_from_value,
        Key.sentinel_key,
        Key.tombstone,
        Key.tombstone_from_key,
    ));

    const CompareInt = struct {
        inline fn compare_keys(a: u32, b: u32) std.math.Order {
            return std.math.order(a, b);
        }

        inline fn key_from_value(value: *const u32) u32 {
            return value.*;
        }
    };

    const CompareTable = struct {
        inline fn compare_keys(a: u64, b: u64) std.math.Order {
            return std.math.order(a, b);
        }

        inline fn key_from_value(value: *const TableInfo) u64 {
            return value.address;
        }
    };

    comptime {
        assert(@sizeOf(TableInfo) == 48 + @sizeOf(u128) * 2);
        assert(@alignOf(TableInfo) == 16);
        assert(@bitSizeOf(TableInfo) == @sizeOf(TableInfo) * 8);
    }

    const Options = struct {
        element_type: type,
        node_size: u32,
        element_count_max: u32,
    };

    var tested_padding = false;
    var tested_node_capacity_min = false;

    // We want to explore not just the bottom boundary but also the surrounding area
    // as it may also have interesting edge cases.
    inline for (.{
        Options{ .element_type = u32, .node_size = 8, .element_count_max = 3 },
        Options{ .element_type = u32, .node_size = 8, .element_count_max = 4 },
        Options{ .element_type = u32, .node_size = 8, .element_count_max = 5 },
        Options{ .element_type = u32, .node_size = 8, .element_count_max = 6 },
        Options{ .element_type = u32, .node_size = 8, .element_count_max = 1024 },
        Options{ .element_type = u32, .node_size = 16, .element_count_max = 1024 },
        Options{ .element_type = u32, .node_size = 32, .element_count_max = 1024 },
        Options{ .element_type = u32, .node_size = 64, .element_count_max = 1024 },
        Options{ .element_type = TableInfo, .node_size = 256, .element_count_max = 3 },
        Options{ .element_type = TableInfo, .node_size = 256, .element_count_max = 4 },
        Options{ .element_type = TableInfo, .node_size = 256, .element_count_max = 1024 },
        Options{ .element_type = TableInfo, .node_size = 512, .element_count_max = 1024 },
        Options{ .element_type = TableInfo, .node_size = 1024, .element_count_max = 1024 },
    }) |options| {
        inline for (.{ .sorted, .unsorted }) |order| {
            const Context = TestContext(
                options.element_type,
                options.node_size,
                options.element_count_max,
                if (options.element_type == u32) u32 else u64,
                if (options.element_type == u32) CompareInt.key_from_value else CompareTable.key_from_value,
                if (options.element_type == u32) CompareInt.compare_keys else CompareTable.compare_keys,
                order,
            );

            var context = try Context.init(random);
            defer context.deinit();

            try context.run();

            if (options.node_size % @sizeOf(options.element_type) != 0) tested_padding = true;
            if (Context.TestArray.node_capacity == 2) tested_node_capacity_min = true;
        }
    }

    assert(tested_padding);
    assert(tested_node_capacity_min);
}
