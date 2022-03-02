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

        /// This is the index of the first table node that might contain the TableInfo
        /// corresponding to a given key node. This allows us to skip table nodes which cannot
        /// contain the target TableInfo when searching for the TableInfo with a given absolute
        /// index. Only the first keys.node_count elements are valid.
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

        /// Insert a batch of tables into the tables segmented array then update the metadata/indexes.
        pub fn insert_tables(level: *Self, node_pool: *NodePool, tables: []const TableInfo) void {
            assert(tables.len > 0);
            assert(level.keys.len() == level.tables.len());

            if (lsm.verify and tables.len > 1) {
                var a = tables[0];
                assert(compare_keys(a.key_min, a.key_max) == .lt);
                for (tables[1..]) |b| {
                    assert(compare_keys(a.key_max, b.key_min) == .lt);
                    assert(compare_keys(b.key_min, b.key_max) == .lt);
                    a = b;
                }
            }

            // TODO: insert multiple elements at once into the segmented arrays if possible as an
            // optimization. We can't always do this because we must maintain sorted order and
            // there may be duplicate keys due to snapshots.

            var absolute_index = level.absolute_index_for_insert(tables[0].key_max);
            var i: usize = 0;
            while (i < tables.len) : (i += 1) {
                const table = &tables[i];

                // Increment absolute_index until the key_max at absolute_index is greater than
                // or equal to table.key_max. This is the index we want to insert the table at.
                {
                    var it = level.keys.iterator(absolute_index, 0, .ascending);
                    while (it.next()) |key_max| : (absolute_index += 1) {
                        if (compare_keys(key_max.*, table.key_max) != .lt) break;
                    }
                }

                level.keys.insert_elements(node_pool, absolute_index, &[_]Key{table.key_max});
                level.tables.insert_elements(node_pool, absolute_index, tables[i..][0..1]);
            }

            assert(level.keys.len() == level.tables.len());

            level.rebuild_root();
        }

        /// Return the index at which to insert a new table given the table's key_max.
        /// Requires all metadata/indexes to be valid.
        fn absolute_index_for_insert(level: Self, key_max: Key) u32 {
            const root = level.root_keys();
            if (root.len == 0) {
                assert(level.keys.len() == 0);
                assert(level.tables.len() == 0);
                return 0;
            }

            const key_node = binary_search_keys_raw(Key, compare_keys, root, key_max);
            assert(key_node <= level.keys.node_count);
            if (key_node == level.keys.node_count) {
                assert(level.keys.len() == level.tables.len());
                return level.keys.len();
            }

            const keys = level.keys.node_elements(key_node);
            const relative_index = binary_search_keys_raw(Key, compare_keys, keys, key_max);

            // The key must be less than or equal to the maximum key of this key node since the
            // first binary search checked this exact condition.
            assert(relative_index < keys.len);

            return level.keys.absolute_index_for_cursor(.{
                .node = key_node,
                .relative_index = relative_index,
            });
        }

        /// Rebuilds the root_keys and root_table_nodes arrays based on the current state of the
        /// keys and tables segmented arrays.
        fn rebuild_root(level: *Self) void {
            assert(level.keys.len() == level.tables.len());

            {
                level.root_keys_array = undefined;
                var key_node: u32 = 0;
                while (key_node < level.keys.node_count) : (key_node += 1) {
                    level.root_keys_array[key_node] = level.keys.node_last_element(key_node);
                }
            }

            {
                level.root_table_nodes_array = undefined;
                var key_node: u32 = 0;
                var table_node: u32 = 0;
                while (key_node < level.keys.node_count) : (key_node += 1) {
                    const key_node_first_key = level.keys.node_elements(key_node)[0];

                    // While the key_max of the table node is less than the first key_max of the
                    // key_node, increment table_node.
                    while (table_node < level.tables.node_count) : (table_node += 1) {
                        const table_node_table_max = level.tables.node_last_element(table_node);
                        const table_node_key_max = table_node_table_max.key_max;
                        if (compare_keys(table_node_key_max, key_node_first_key) != .lt) {
                            break;
                        }
                    } else {
                        // Assert that we found the appropriate table_node and hit the break above.
                        unreachable;
                    }

                    level.root_table_nodes_array[key_node] = table_node;
                }
            }
        }

        /// Set snapshot_max to new_snapshot_max for tables with snapshot_max of math.maxInt(u64)
        /// and matching the given key range.
        /// Asserts that exactly cardnality tables are modified.
        pub fn set_snapshot_max(
            level: Self,
            new_snapshot_max: u64,
            key_min: Key,
            key_max: Key,
            cardnality: u32,
        ) void {
            assert(new_snapshot_max <= lsm.snapshot_latest);
            assert(compare_keys(key_min, key_max) != .gt);

            var it = level.iterator(lsm.snapshot_latest, key_min, key_max, .ascending);
            var modified: u32 = 0;
            while (it.next()) |table_const| {
                // This const cast is safe as we know that the memory pointed to is in fact
                // mutable. That is, the table is not in the .text or .rodata section. We do this
                // to avoid duplicating the iterator code in order to expose only a const iterator
                // in the public API.
                const table = @intToPtr(*TableInfo, @ptrToInt(table_const));

                assert(compare_keys(key_min, table.key_min) == .lt);
                assert(compare_keys(key_max, table.key_max) == .gt);

                assert(table.snapshot_max == math.maxInt(u64));
                table.snapshot_max = new_snapshot_max;
                modified += 1;
            }

            assert(modified == cardnality);
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
            if (root.len == 0) {
                assert(level.keys.len() == 0);
                assert(level.tables.len() == 0);
                return null;
            }

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
