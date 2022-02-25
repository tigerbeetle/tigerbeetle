const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;
const meta = std.meta;

const config = @import("../config.zig");
const lsm = @import("tree.zig");
const binary_search_keys_raw = @import("binary_search.zig").binary_search_keys_raw;

const Direction = @import("direction.zig").Direction;
const SegmentedArray = @import("segmented_array.zig").SegmentedArray;
const SegmentedArrayCursor = @import("segmented_array.zig").Cursor;

pub fn ManifestLevel(
    comptime NodePool: type,
    comptime Key: type,
    comptime TableInfo: type,
    comptime compare_keys: fn (Key, Key) callconv(.Inline) math.Order,
) type {
    return struct {
        const Self = @This();

        const Keys = SegmentedArray(Key, NodePool, lsm.table_count_max);
        const Tables = SegmentedArray(TableInfo, NodePool, lsm.table_count_max);

        /// The maximum key of each key node in the keys segmented array.
        /// This is the starting point of our tiered lookup approach.
        /// Only the first keys.node_count elements are valid.
        root_keys_array: *[Keys.node_count_max]Key,

        /// This is the index of the table node containing the TableInfo corresponding to a given
        /// root key. This allows us to skip table nodes which cannot contain the target TableInfo
        /// when searching for the TableInfo with a given absolute index.
        root_table_nodes_array: *[Keys.node_count_max]u32,

        // These two segmented arrays are parallel. That is, the absolute indexes of maximum key
        // and corresponding TableInfo are the same. However, the number of nodes, node index, and
        // relative index into the node differ as the elements per node are different.
        keys: Keys,
        tables: Tables,

        pub fn init(allocator: mem.Allocator) !Self {
            var root_keys_array = try allocator.create([Keys.node_count_max]Key);
            errdefer allocator.destroy(root_keys_array);

            var root_table_nodes_array = try allocator.create([Keys.node_count_max]u32);
            errdefer allocator.destroy(root_table_nodes_array);

            var keys = try Keys.init(allocator);
            errdefer keys.deinit(allocator, null);

            var tables = try Tables.init(allocator);
            errdefer tables.deinit(allocator, null);

            return Self{
                .root_keys_array = root_keys_array,
                .root_table_nodes_array = root_table_nodes_array,
                .keys = keys,
                .tables = tables,
            };
        }

        pub fn deinit(level: *Self, allocator: mem.Allocator, node_pool: *NodePool) void {
            allocator.destroy(level.root_keys_array);
            allocator.destroy(level.root_table_nodes_array);
            level.keys.deinit(allocator, node_pool);
            level.tables.deinit(allocator, node_pool);
        }

        pub const Iterator = struct {
            level: *const Self,
            inner: Tables.Iterator,
            snapshot: u64,
            key_min: Key,
            key_max: Key,
            direction: Direction,

            pub fn next(it: *Iterator) ?*const TableInfo {
                while (it.inner.next()) |table| {
                    // We can't assert !it.inner.done as inner.next() may set done before returning.

                    if (!table.visible(it.snapshot)) continue;

                    switch (it.direction) {
                        .ascending => {
                            // Assert that the table is not out of bounds to the left.
                            //
                            // We can assert this as it is exactly the same key comparison when we
                            // binary search in iterator_start(), and since we move in ascending
                            // order this also remains true beyond the first iteration.
                            assert(compare_keys(table.key_max, it.key_min) != .lt);

                            // Check if the table is out of bounds to the right.
                            if (compare_keys(table.key_min, it.key_max) == .gt) {
                                it.inner.done = true;
                                return null;
                            }
                        },
                        .descending => {
                            // Check if the table is out of bounds to the right.
                            //
                            // Unlike in the ascending case, it is not guaranteed that
                            // table.key_min is less than or equal to it.key_max on the
                            // first iteration as only the key_max of a table is stored in our
                            // root/key nodes. On subsequent iterations this check will always
                            // be false.
                            if (compare_keys(table.key_min, it.key_max) == .gt) {
                                continue;
                            }

                            // Check if the table is out of bounds to the left.
                            if (compare_keys(table.key_max, it.key_min) == .lt) {
                                it.inner.done = true;
                                return null;
                            }
                        },
                    }

                    return table;
                }

                assert(it.inner.done);
                return null;
            }
        };

        pub fn iterator(
            level: *const Self,
            snapshot: u64,
            key_min: Key,
            key_max: Key,
            direction: Direction,
        ) Iterator {
            assert(snapshot <= lsm.snapshot_latest);
            assert(compare_keys(key_min, key_max) != .gt);

            const inner = blk: {
                if (level.iterator_start(key_min, key_max, direction)) |start| {
                    break :blk level.tables.iterator(
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
        /// the key_max is stored in the index structures, not the key_min, and only the start
        /// bound for the given direction is checked here.
        fn iterator_start(
            level: Self,
            key_min: Key,
            key_max: Key,
            direction: Direction,
        ) ?SegmentedArrayCursor {
            assert(compare_keys(key_min, key_max) != .gt);

            const root = level.root_keys();
            if (root.len == 0) return null;

            const key = switch (direction) {
                .ascending => key_min,
                .descending => key_max,
            };

            const key_node = binary_search_keys_raw(Key, compare_keys, root, key);
            assert(key_node <= level.keys.node_count);
            if (key_node == level.keys.node_count) {
                switch (direction) {
                    // The key_min of the target range is greater than the key_max of the last
                    // table in the level and we are ascending, so this range matches no tables
                    // on this level.
                    .ascending => return null,
                    // The key_max of the target range is greater than the key_max of the last
                    // table in the level and we are desceneding, so we need to start iteration
                    // at the last table in the level.
                    .descending => return level.keys.last(),
                }
            }

            const keys = level.keys.node_elements(key_node);
            const relative_index = binary_search_keys_raw(Key, compare_keys, keys, key);

            // The key must be less than or equal to the maximum key of this key node since the
            // first binary search checked this exact condition.
            assert(relative_index < keys.len);

            return level.iterator_start_boundary(
                .{
                    .node = key_node,
                    .relative_index = relative_index,
                },
                direction,
            );
        }

        /// This function exists because there may be tables in the level with the same
        /// key_max but non-overlapping snapshot visibility.
        fn iterator_start_boundary(
            level: Self,
            key_cursor: SegmentedArrayCursor,
            direction: Direction,
        ) SegmentedArrayCursor {
            var reverse = level.keys.iterator(
                level.keys.absolute_index_for_cursor(key_cursor),
                key_cursor.node,
                direction.reverse(),
            );

            assert(meta.eql(reverse.cursor, key_cursor));
            // This cursor will always point to a key equal to start_key.
            var adjusted = reverse.cursor;
            const start_key = reverse.next().?.*;
            assert(compare_keys(start_key, level.keys.element_at_cursor(adjusted)) == .eq);

            var adjusted_next = reverse.cursor;
            while (reverse.next()) |k| {
                if (compare_keys(start_key, k.*) != .eq) break;
                adjusted = adjusted_next;
                adjusted_next = reverse.cursor;
            } else {
                switch (direction) {
                    .ascending => assert(meta.eql(adjusted, level.keys.last())),
                    .descending => assert(meta.eql(adjusted, level.keys.first())),
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

            switch (direction) {
                .ascending => return level.root_table_nodes_array[key_node],
                .descending => {
                    if (key_node + 1 < level.keys.node_count) {
                        // Since the corresponding node in root_table_nodes_array is a lower bound,
                        // we must add one to make it an upper bound when descending.
                        return level.root_table_nodes_array[key_node + 1];
                    } else {
                        // However, if we are at the last key node, then return the last table node.
                        return level.tables.node_count - 1;
                    }
                },
            }
        }

        inline fn root_keys(level: Self) []Key {
            return level.root_keys_array[0..level.keys.node_count];
        }
    };
}
