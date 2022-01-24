const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const Direction = @import("tree.zig").Direction;

pub const SegmentedArrayCursor = struct {
    node: u32,
    relative_index: u32,
};

pub fn SegmentedArray(
    comptime T: type,
    comptime node_size: u32,
    comptime element_count_max: u32,
) type {
    const node_capacity = node_size / @sizeOf(T);

    return struct {
        const Self = @This();

        pub const node_count_max = blk: {
            // If a node fills up it is divided into two new nodes. Therefore,
            // the worst possible space overhead is when all nodes are half full.
            // This uses flooring division, we want to examine the worst case here.
            const min_elements_per_node = node_capacity / 2;
            // TODO Can we get rid of this +1?
            break :blk div_ceil(element_count_max, min_elements_per_node) + 1;
        };

        node_count: u32,
        /// This is the segmented array, the first key_node_count pointers are non-null.
        /// The rest are null. We only use optional pointers here to get safety checks.
        nodes: *[node_count_max]?*[node_capacity]T,
        // TODO Get rid of this as it is redundant with key_node_start_index
        counts: *[node_count_max]u32,
        /// Since nodes in a segmented array are usually not full, computing the absolute index
        /// of an element in the full array is O(N) over the number of nodes. To avoid this cost
        /// we precompute the absolute index of the first element of each node.
        absolute_index_of_first_element: *[node_count_max]u32,

        pub fn init(allocator: mem.Allocator) !Self {}
        pub fn deinit(allocator: mem.Allocator) void {}

        pub fn node_elements(array: Self, node: u32) []T {
            assert(node < array.node_count);
            return array.nodes[node].?[0..array.counts[node]];
        }

        pub fn element(array: Self, cursor: SegmentedArrayCursor) T {
            return array.node_elements(cursor.node)[cursor.relative_index];
        }

        pub fn last_node(array: Self) u32 {
            return array.node_count - 1;
        }

        pub fn first(_: Self) SegmentedArrayCursor {
            return .{
                .node = 0,
                .relative_index = 0,
            };
        }

        pub fn last(array: Self) SegmentedArrayCursor {
            return .{
                .node = array.node_count - 1,
                .relative_index = array.node_elements(array.node_count - 1).len - 1,
            };
        }

        // TODO consider enabling ReleaseFast for this once tested
        pub fn absolute_index_for_cursor(array: Self, cursor: SegmentedArrayCursor) u32 {
            assert(node < array.node_count);
            assert(relative_index < array.counts[node]);
            return array.first_absolute_index(node) + relative_index;
        }

        pub const Iterator = struct {
            array: *const Self,
            direction: Direction,

            cursor: SegmentedArrayCursor,

            /// The user may set this early to stop iteration. For example,
            /// if the returned table info is outside the key range.
            done: bool = false,

            pub fn next(it: *Iterator) ?*const T {
                if (it.done) return null;

                assert(it.cursor.relative_index < elements.len);
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
            // By asserting that the absolute index and start node are in bounds, we know that
            // the iterator will not be initialized in the `done` state and will yield at least
            // one element.
            assert(start_node < array.node_count);
            assert(absolute_index <= array.last_absolute_index(array.node_count - 1));
            switch (direction) {
                .ascending => {
                    assert(absolute_index >= array.first_absolute_index(start_node));

                    var node = start_node;
                    while (node + 1 < array.node_count and
                        absolute_index >= array.first_absolute_index(node + 1))
                    {
                        node += 1;
                    }
                    assert(node < array.node_count);

                    const relative_index = absolute_index - array.first_absolute_index(node);
                    assert(relative_index < array.counts[node]);

                    // TODO dead code:
                    const done = relative_index >= array.counts[node];
                    if (done) assert(node + 1 == array.node_count);

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
                    assert(absolute_index <= array.last_absolute_index(start_node));

                    var node = start_node;
                    while (node > 0 and absolute_index <= array.last_absolute_index(node - 1)) {
                        node -= 1;
                    }

                    const relative_index = absolute_index - array.first_absolute_index(node);
                    assert(relative_index < array.counts[node]);

                    // TODO dead code:
                    const done = relative_index >= array.counts[node];
                    if (done) assert(node + 1 == array.node_count);

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

        fn first_absolute_index(array: Self, node: u32) u32 {
            assert(node < array.node_count);
            return array.absolute_index_of_first_element[node];
        }

        fn last_absolute_index(array: Self, node: u32) u32 {
            assert(node < array.node_count);
            return array.absolute_index_of_first_element[node] + array.counts[node] - 1;
        }
    };
}
