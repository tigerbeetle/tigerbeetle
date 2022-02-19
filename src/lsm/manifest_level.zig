const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;
const meta = std.meta;

const config = @import("../config.zig");
const lsm = @import("tree.zig");
const binary_search = @import("binary_search").binary_search;

const Direction = @import("tree.zig").Direction;
const SegmentedArray = @import("segmented_array.zig").SegmentedArray;
const SegmentedArrayCursor = @import("segmented_array.zig").Cursor;

fn div_ceil(numerator: anytype, denominator: anytype) @TypeOf(numerator, denominator) {
    const T = @TypeOf(numerator, denominator);
    return math.divCeil(T, numerator, denominator) catch unreachable;
}

pub fn ManifestLevel(
    comptime Key: type,
    comptime TableInfo: type,
    comptime compare_keys: fn (Key, Key) math.Order,
) type {
    return struct {
        const Self = @This();

        const Keys = SegmentedArray(Key, node_size, lsm.table_count_max);
        const Tables = SegmentedArray(TableInfo, node_size, lsm.table_count_max);

        /// The minimum key of each key node in the keys segmented array.
        /// This is the starting point of our tiered lookup approach.
        /// Only the first keys.node_count elements are valid.
        root_keys_array: *[Keys.node_count_max]Key,
        /// This is the index of the table node containing the TableInfo corresponding to a given
        /// root key. This allows us to skip table nodes which cannot contain the target TableInfo
        /// when searching for the TableInfo with a given absolute index.
        root_table_nodes_array: *[Keys.node_count_max]u32,

        // These two segmented arrays are parallel. That is, the absolute indexes of key and
        // corresponding TableInfo are the same. However, the number of nodes, node index, and
        // relative index into the node differ as the elements per node are different.
        keys: Keys,
        tables: Tables,

        fn init(allocator: *mem.Allocator, level: u8) !Self {}

        pub const Iterator = struct {
            level: *const Self,
            inner: Tables.Iterator,

            /// May pass math.maxInt(u64)-1 if there is no snapshot.
            snapshot: u64,
            key_min: Key,
            key_max: Key,
            direction: Direction,

            pub fn next(it: *Iterator) ?*const TableInfo {
                while (it.inner.next()) |table_info| {
                    // We can't assert that it.inner.done == false as inner.next() may set done
                    // before returning.

                    assert(table_info.snapshot_min < table_info.snapshot_max);

                    if (it.snapshot < table_info.snapshot_min) continue;
                    assert(it.snapshot != table_info.snapshot_min);

                    if (it.snapshot > table_info.snapshot_max) continue;
                    assert(it.snapshot != table_info.snapshot_max);

                    switch (direction) {
                        .ascending => {
                            // Unlike in the descending case, it is not guaranteed that
                            // table_info.key_max is less than it.key_min on the first iteration
                            // as only the key_min of a table is stored in our root/key nodes.
                            // On subsequent iterations this check will always be true.
                            if (compare_keys(table_info.key_max, it.key_min) == .lt or
                                compare_keys(table_info.key_min, it.key_max) == .gt)
                            {
                                it.inner.done = true;
                                return null;
                            }
                        },
                        .descending => {
                            // We can assert this as it is exactly the same key comparison we
                            // perform when doing binary search in iterator_start(), and since
                            // we move in descending order this remains true beyond the first
                            // iteration.
                            assert(compare_keys(table_info.key_min, it.key_max) != .gt);
                            if (compare_keys(table_info.key_max, it.key_min) == .lt) {
                                it.inner.done = true;
                                return null;
                            }
                        },
                    }

                    return table_info;
                }

                assert(it.inner.done);
                return null;
            }
        };

        pub fn iterator(
            level: *const Self,
            /// May pass math.maxInt(u64) if there is no snapshot.
            snapshot: u64,
            key_min: Key,
            key_max: Key,
            direction: Direction,
        ) Iterator {
            const inner = blk: {
                if (level.iterator_start(key_min, key_max, direction)) |start| {
                    break :blk level.keys.iterator(
                        level.keys.absolute_index_for_cursor(start),
                        level.iterator_start_table_node_for_key_node(start.node, direction),
                        direction,
                    );
                } else {
                    break :blk Tables.Iterator{
                        .array = &level.tables,
                        .direction = direction,
                        .cursor = .{ .node = 0, .relative_index = 0 },
                        .done = true,
                    };
                }
            };

            return .{
                .level = level,
                .inner = inner,
                .snapshot = snapshot,
                .key_min = key_min,
                .key_max = key_max,
                .direction = direction,
            };
        }

        /// Returns the table segmented array cursor at which iteration should be started.
        /// May return null if there is nothing to iterate because we know for sure that the key
        /// range is disjoint with the tables stored in this level.
        /// However, the cursor returned is not guaranteed to be in range for the query as only
        /// the key_min is stored in the index structures, not the key_max.
        fn iterator_start(
            level: Self,
            key_min: Key,
            key_max: Key,
            direction: Direction,
        ) ?SegmentedArrayCursor {
            const root = level.root_keys();
            if (root.len == 0) return null;

            const key = switch (direction) {
                .ascending => key_min,
                .descending => key_max,
            };

            const root_result = binary_search(root, key);
            if (root_result.exact) {
                return level.iterator_start_boundary(
                    .{
                        .node = root_result.index,
                        .relative_index = 0,
                    },
                    direction,
                );
            } else if (root_result.index == 0) {
                // Out of bounds to the left.
                switch (direction) {
                    // In the case of an ascending search, we start at the first table in the level
                    // since the target key_min is less than the key_min of the first table.
                    .ascending => return .{
                        .node = 0,
                        .relative_index = 0,
                    },
                    // In the case of a descending search, we are already finished because the first
                    // key_min in the entire level is greater than the target key_max.
                    .descending => return null,
                }
            } else {
                // Since there was not an exact match, the binary search returns the index of
                // the next greatest key. We must therefore subtract one to perform the next
                // binary search in the key node which may contain `key`.
                const key_node = root_result.index - 1;
                // In the rather unlikely event that two or more key_nodes share the same key_min
                // a simple root_result.index - 1 isn't sufficient to get to the next lowest key_min,
                // unless our binary search implementation guarantees to always return the immediate
                // next greatest key even in the presence of duplicates, which it does.
                assert(compare_keys(root[key_node], root[root_result.index]) == .lt);

                const keys = level.keys.node_elements(key_node);
                const keys_result = binary_search(keys, key);

                // Since we didn't have an exact match in the previous binary search, and since
                // we've already handled the case of being out of bounds to the left with an
                // early return, we know that the target key_min is strictly greater than the
                // first key in the key node.
                assert(keys_result.index != 0);

                // In the case of an inexact match, the binary search returns the index of
                // the next greatest key. We must therefore subtract one as it is possible for
                // the previous table to contain `key`. That is, the key_max of the previous
                // table could be greater than `key`.
                const relative_index = keys_result.index - @boolToInt(!keys_result.exact);

                // This mirrors the earlier assertion on root[key_node], root[root_result.index].
                if (!keys_result.exact) {
                    assert(compare_keys(keys[relative_index], keys[keys_result.index]) == .lt);
                }

                return level.iterator_start_boundary(
                    .{
                        .node = key_node,
                        .relative_index = relative_index,
                    },
                    direction,
                );
            }
        }

        fn iterator_start_boundary(
            level: Self,
            key_cursor: SegmentedArrayCursor,
            direction: Direction,
        ) SegmentedArrayCursor {
            const reverse = level.keys.iterator(
                level.keys.absolute_index_for_cursor(key_cursor),
                key_cursor.node,
                direction.reverse(),
            );

            assert(meta.eql(reverse.cursor, key_cursor));
            // This cursor will always point to a key equal to start_key.
            var adjusted = reverse.cursor;
            const start_key = reverse.next().?;
            assert(compare_keys(start_key, level.keys.element_at_cursor(adjusted)) == .eq);

            var adjusted_next = reverse.cursor;
            while (reverse.next()) |k| {
                if (compare_keys(start_key, k) != .eq) break;
                adjusted = adjusted_next;
                adjusted_next = reverse.cursor;
            } else {
                switch (direction) {
                    .ascending => assert(meta.eql(adjusted, keys.last())),
                    .descending => assert(meta.eql(adjusted, keys.first())),
                }
            }
            assert(compare_keys(start_key, level.keys.element_at_cursor(adjusted)) == .eq);

            return adjusted;
        }

        inline fn iterator_start_table_node_for_key_node(
            level: Self,
            key_node: u32,
            direction: Direction,
        ) u32 {
            assert(key_node < level.keys.node_count);

            if (direction == .descending) {
                // Since the corresponding table node in the root_table_nodes_array is a lower
                // bound, we need to add one to make it an upper bound when descending.
                key_node += 1;

                if (key_node == level.keys.node_count) {
                    // If we were already at the last key node, then we
                    // should instead return the very last table node.
                    return level.tables.node_count - 1;
                }
            }

            return level.root_table_nodes_array[key_node];
        }

        const BinarySearchResult = struct {
            index: usize,
            exact: bool,
        };

        // TODO(ifreund) move this back to binary_search.zig and allow max key searching.
        // Once this is in binary_search.zig we can then use it within tree.zig to find data blocks.
        // We also need test coverage for this.
        fn binary_search(keys: []const Key, key: Key) BinarySearchResult {
            assert(keys.len > 0);

            var offset: usize = 0;
            var length: usize = keys.len;
            while (length > 1) {
                const half = length / 2;
                const mid = offset + half;

                // This trick seems to be what's needed to get llvm to emit branchless code for this,
                // a ternary-style if expression was generated as a jump here for whatever reason.
                const next_offsets = [_]usize{ offset, mid };
                offset = next_offsets[@boolToInt(compare_keys(keys[mid], key) == .lt)];

                length -= half;
            }
            const exact = compare_keys(keys[offset], key) == .eq;
            return .{
                .index = offset + @boolToInt(!exact),
                .exact = exact,
            };
        }

        inline fn root_keys(level: Self) []Key {
            return level.root_keys_array[0..level.keys.node_count];
        }
    };
}
