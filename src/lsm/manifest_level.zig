const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;
const meta = std.meta;

const config = @import("../config.zig");
const lsm = @import("tree.zig");
const binary_search = @import("binary_search.zig");
const binary_search_keys_raw = binary_search.binary_search_keys_raw;

const Direction = @import("direction.zig").Direction;
const SegmentedArray = @import("segmented_array.zig").SegmentedArray;
const SegmentedArrayCursor = @import("segmented_array.zig").Cursor;

pub fn ManifestLevel(
    comptime NodePool: type,
    comptime Key: type,
    comptime TableInfo: type,
    comptime compare_keys: fn (Key, Key) callconv(.Inline) math.Order,
    comptime table_count_max: u32,
) type {
    return struct {
        const Self = @This();

        const Keys = SegmentedArray(Key, NodePool, table_count_max);
        const Tables = SegmentedArray(TableInfo, NodePool, table_count_max);

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
                assert(compare_keys(a.key_min, a.key_max) != .gt);
                for (tables[1..]) |b| {
                    assert(compare_keys(a.key_max, b.key_min) == .lt);
                    assert(compare_keys(b.key_min, b.key_max) != .gt);
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
                if (absolute_index < level.keys.len()) {
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
                mem.set(Key, level.root_keys_array, undefined);
                var key_node: u32 = 0;
                while (key_node < level.keys.node_count) : (key_node += 1) {
                    level.root_keys_array[key_node] = level.keys.node_last_element(key_node);
                }
            }

            if (lsm.verify and level.keys.node_count > 1) {
                var a = level.root_keys_array[0];
                for (level.root_keys_array[1..level.keys.node_count]) |b| {
                    assert(compare_keys(a, b) != .gt);
                    a = b;
                }
            }

            {
                mem.set(u32, level.root_table_nodes_array, undefined);
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

            if (lsm.verify and level.keys.node_count > 1) {
                var a = level.root_table_nodes_array[0];
                for (level.root_table_nodes_array[1..level.keys.node_count]) |b| {
                    assert(a <= b);
                    a = b;
                }
            }

            if (lsm.verify) {
                // Assert that the first key in each key node is in the range of the table
                // directly mapped to by root_table_nodes_array.
                for (level.root_table_nodes_array[0..level.keys.node_count]) |table_node, i| {
                    const key_node = @intCast(u32, i);
                    const key_node_first_key = level.keys.node_elements(key_node)[0];

                    const table_node_key_min = level.tables.node_elements(table_node)[0].key_min;
                    const table_node_key_max = level.tables.node_last_element(table_node).key_max;

                    assert(compare_keys(table_node_key_min, table_node_key_max) != .gt);

                    assert(compare_keys(key_node_first_key, table_node_key_min) != .lt);
                    assert(compare_keys(key_node_first_key, table_node_key_max) != .gt);
                }
            }
        }

        /// Set snapshot_max to new_snapshot_max for tables with snapshot_max of math.maxInt(u64)
        /// and matching the given key range.
        /// Asserts that exactly cardinality tables are modified.
        pub fn set_snapshot_max(
            level: Self,
            new_snapshot_max: u64,
            key_min: Key,
            key_max: Key,
            cardinality: u32,
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

                // Assert that the table overlaps with the given key range.
                assert(compare_keys(key_min, table.key_max) != .gt);
                assert(compare_keys(key_max, table.key_min) != .lt);

                assert(table.snapshot_max == math.maxInt(u64));
                table.snapshot_max = new_snapshot_max;
                modified += 1;
            }

            assert(modified == cardinality);
        }

        /// Remove tables matching the given key range with table.snapshot_max <= snapshot_max.
        /// Asserts that the key_min/key_max bounds exactly match the first/last table to be removed.
        /// Asserts that exactly cardinality tables are removed.
        pub fn remove_tables(
            level: *Self,
            node_pool: *NodePool,
            snapshot_max: u64,
            key_min: Key,
            key_max: Key,
            cardinality: u32,
        ) void {
            assert(cardinality > 0);
            assert(level.keys.len() == level.tables.len());
            assert(level.keys.len() >= cardinality);

            var absolute_index = level.absolute_index_for_remove(key_min);

            {
                var it = level.tables.iterator(absolute_index, 0, .ascending);
                while (it.next()) |table| : (absolute_index += 1) {
                    if (table.snapshot_max <= snapshot_max) {
                        // We require the key_min/key_max to be exact, so the first table
                        // matching the snapshot must have the provided key_min.
                        assert(compare_keys(key_min, table.key_min) == .eq);
                        break;
                    }
                } else {
                    unreachable;
                }
            }

            var removed: u32 = 0;
            var safety_counter: u32 = 0;
            outer: while (safety_counter < cardinality) : (safety_counter += 1) {
                var it = level.tables.iterator(absolute_index, 0, .ascending);
                inner: while (it.next()) |table| : (absolute_index += 1) {
                    if (table.snapshot_max <= snapshot_max) {
                        const table_key_max = table.key_max;
                        level.keys.remove_elements(node_pool, absolute_index, 1);
                        level.tables.remove_elements(node_pool, absolute_index, 1);
                        removed += 1;

                        switch (compare_keys(table_key_max, key_max)) {
                            .lt => break :inner,
                            .eq => break :outer,
                            // We require the key_min/key_max to be exact, so the last table
                            // matching the snapshot must have the provided key_max.
                            .gt => unreachable,
                        }
                    } else {
                        // We handle the first table to be removed specially before this main loop
                        // in order to check for an exact key_min match.
                        assert(removed > 0);
                    }
                } else {
                    unreachable;
                }
            } else {
                unreachable;
            }
            assert(removed == cardinality);
            // The loop will never terminate naturally, only through the `break :outer`, which
            // means the +1 here is required as the continue safety_counter += 1 continue
            // expression isn't run on the last iteration of the loop.
            assert(safety_counter + 1 == cardinality);

            assert(level.keys.len() == level.tables.len());

            level.rebuild_root();
        }

        /// Return the index of the first table that could have the given key_min.
        /// Requires all metadata/indexes to be valid.
        fn absolute_index_for_remove(level: Self, key_min: Key) u32 {
            const root = level.root_keys();
            assert(root.len > 0);

            const key_node = binary_search_keys_raw(Key, compare_keys, root, key_min);
            assert(key_node < level.keys.node_count);

            const keys = level.keys.node_elements(key_node);
            assert(keys.len > 0);

            const relative_index = binary_search_keys_raw(Key, compare_keys, keys, key_min);
            assert(relative_index < keys.len);

            return level.keys.absolute_index_for_cursor(level.iterator_start_boundary(
                .{
                    .node = key_node,
                    .relative_index = relative_index,
                },
                .ascending,
            ));
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
                    .ascending => assert(meta.eql(adjusted, level.keys.first())),
                    .descending => assert(meta.eql(adjusted, level.keys.last())),
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

pub fn TestContext(
    comptime node_size: u32,
    comptime Key: type,
    comptime table_count_max: u32,
) type {
    return struct {
        const Self = @This();

        const testing = std.testing;

        const log = false;

        inline fn compare_keys(a: Key, b: Key) math.Order {
            return math.order(a, b);
        }

        // TODO Import this type from lsm/tree.zig.
        const TableInfo = extern struct {
            checksum: u128,
            address: u64,
            flags: u64 = 0,

            /// The minimum snapshot that can see this table (with exclusive bounds).
            /// This value is set to the current snapshot tick on table creation.
            snapshot_min: u64,

            /// The maximum snapshot that can see this table (with exclusive bounds).
            /// This value is set to the current snapshot tick on table deletion.
            snapshot_max: u64 = math.maxInt(u64),

            key_min: Key,
            key_max: Key,

            comptime {
                assert(@sizeOf(TableInfo) == 48 + @sizeOf(Key) * 2);
                assert(@alignOf(TableInfo) == 16);
            }

            pub fn visible(table: *const @This(), snapshot: u64) bool {
                assert(table.address != 0);
                assert(table.snapshot_min < table.snapshot_max);
                assert(snapshot <= lsm.snapshot_latest);

                assert(snapshot != table.snapshot_min);
                assert(snapshot != table.snapshot_max);

                return table.snapshot_min < snapshot and snapshot < table.snapshot_max;
            }
        };

        const NodePool = @import("node_pool.zig").NodePool;

        const TestPool = NodePool(node_size, @alignOf(TableInfo));
        const TestLevel = ManifestLevel(TestPool, Key, TableInfo, compare_keys, table_count_max);

        random: std.rand.Random,

        pool: TestPool,
        level: TestLevel,
        snapshot_tick: u64 = 0,

        reference: std.ArrayList(TableInfo),

        inserts: u64 = 0,
        removes: u64 = 0,

        fn init(random: std.rand.Random) !Self {
            var pool = try TestPool.init(
                testing.allocator,
                TestLevel.Keys.node_count_max + TestLevel.Tables.node_count_max,
            );
            errdefer pool.deinit(testing.allocator);

            var level = try TestLevel.init(testing.allocator);
            errdefer level.deinit(testing.allocator, &pool);

            var reference = try std.ArrayList(TableInfo).initCapacity(
                testing.allocator,
                table_count_max,
            );
            errdefer reference.deinit();

            return Self{
                .random = random,
                .pool = pool,
                .level = level,
                .reference = reference,
            };
        }

        fn deinit(context: *Self) void {
            context.level.deinit(testing.allocator, &context.pool);
            context.pool.deinit(testing.allocator);

            context.reference.deinit();
        }

        fn run(context: *Self) !void {
            if (log) std.debug.print("\n", .{});

            {
                var i: usize = 0;
                while (i < table_count_max * 2) : (i += 1) {
                    switch (context.random.uintLessThanBiased(u32, 100)) {
                        0...59 => try context.insert(),
                        60...99 => try context.remove(),
                        else => unreachable,
                    }
                }
            }

            {
                var i: usize = 0;
                while (i < table_count_max * 2) : (i += 1) {
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
            const count_free = table_count_max - reference_len;

            if (count_free == 0) return;

            var buffer: [13]TableInfo = undefined;

            const count_max = @minimum(count_free, 13);
            const count = context.random.uintAtMostBiased(u32, count_max - 1) + 1;

            {
                var key: Key = context.random.uintAtMostBiased(Key, table_count_max * 64);

                for (buffer[0..count]) |*table| {
                    table.* = context.random_greater_non_overlapping_table(key);
                    key = table.key_max;
                }
            }

            context.level.insert_tables(&context.pool, buffer[0..count]);

            for (buffer[0..count]) |table| {
                const index = blk: {
                    if (context.reference.items.len == 0) {
                        break :blk 0;
                    } else {
                        break :blk binary_search.binary_search_values_raw(
                            Key,
                            TableInfo,
                            key_min_from_table,
                            compare_keys,
                            context.reference.items,
                            table.key_max,
                        );
                    }
                };
                // Can't be equal as the tables may not overlap
                if (index < context.reference.items.len) {
                    assert(context.reference.items[index].key_min > table.key_max);
                }
                context.reference.insert(index, table) catch unreachable;
            }

            context.inserts += count;

            try context.verify();
        }

        fn random_greater_non_overlapping_table(context: Self, key: Key) TableInfo {
            var new_key_min = key + context.random.uintLessThanBiased(Key, 31) + 1;

            assert(compare_keys(new_key_min, key) == .gt);

            var i = blk: {
                if (context.reference.items.len == 0) {
                    break :blk 0;
                } else {
                    break :blk binary_search.binary_search_values_raw(
                        Key,
                        TableInfo,
                        key_min_from_table,
                        compare_keys,
                        context.reference.items,
                        new_key_min,
                    );
                }
            };

            if (i > 0) {
                if (compare_keys(new_key_min, context.reference.items[i - 1].key_max) != .gt) {
                    new_key_min = context.reference.items[i - 1].key_max + 1;
                }
            }

            const next_key_min = for (context.reference.items[i..]) |table| {
                switch (compare_keys(new_key_min, table.key_min)) {
                    .lt => break table.key_min,
                    .eq => new_key_min = table.key_max + 1,
                    .gt => unreachable,
                }
            } else math.maxInt(Key);

            const max_delta = @minimum(32, next_key_min - 1 - new_key_min);
            const new_key_max = new_key_min + context.random.uintAtMostBiased(Key, max_delta);

            return .{
                .checksum = context.random.int(u128),
                .address = context.random.int(u64),
                .snapshot_min = context.snapshot_tick,
                .key_min = new_key_min,
                .key_max = new_key_max,
            };
        }

        fn remove(context: *Self) !void {
            const reference_len = @intCast(u32, context.reference.items.len);
            if (reference_len == 0) return;

            const count_max = @minimum(reference_len, 13);
            const count = context.random.uintAtMostBiased(u32, count_max - 1) + 1;

            assert(context.reference.items.len <= table_count_max);
            const index = context.random.uintAtMostBiased(u32, reference_len - count);

            const key_min = context.reference.items[index].key_min;
            const key_max = context.reference.items[index + count - 1].key_max;

            if (log) std.debug.print("Removing {} to {}, cardinality {}\n", .{ key_min, key_max, count });

            context.level.remove_tables(&context.pool, math.maxInt(u64), key_min, key_max, count);

            context.reference.replaceRange(index, count, &[0]TableInfo{}) catch unreachable;

            context.removes += count;

            try context.verify();
        }

        fn remove_all(context: *Self) !void {
            while (context.reference.items.len > 0) try context.remove();

            try testing.expectEqual(@as(u32, 0), context.level.keys.len());
            try testing.expectEqual(@as(u32, 0), context.level.tables.len());
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
                for (context.reference.items) |i| std.debug.print("[{},{}], ", .{ i.key_min, i.key_max });

                std.debug.print("\nactual: ", .{});
                var it = context.level.iterator(
                    lsm.snapshot_latest,
                    0,
                    math.maxInt(Key),
                    .ascending,
                );
                while (it.next()) |i| std.debug.print("[{},{}], ", .{ i.key_min, i.key_max });
                std.debug.print("\n", .{});
            }

            try testing.expectEqual(context.reference.items.len, context.level.keys.len());
            try testing.expectEqual(context.reference.items.len, context.level.tables.len());

            {
                var it = context.level.iterator(
                    lsm.snapshot_latest,
                    0,
                    math.maxInt(Key),
                    .ascending,
                );

                for (context.reference.items) |expect| {
                    const actual = it.next() orelse return error.TestUnexpectedResult;
                    try testing.expectEqual(expect, actual.*);
                }
                try testing.expectEqual(@as(?*const TableInfo, null), it.next());
            }

            {
                var it = context.level.iterator(
                    lsm.snapshot_latest,
                    0,
                    math.maxInt(Key),
                    .descending,
                );

                var i = context.reference.items.len;
                while (i > 0) {
                    i -= 1;

                    const expect = context.reference.items[i];
                    const actual = it.next() orelse return error.TestUnexpectedResult;
                    try testing.expectEqual(expect, actual.*);
                }
                try testing.expectEqual(@as(?*const TableInfo, null), it.next());
            }

            try testing.expectEqual(context.reference.items.len, context.level.keys.len());
            try testing.expectEqual(context.reference.items.len, context.level.tables.len());

            if (context.reference.items.len > 0) {
                const reference_len = @intCast(u32, context.reference.items.len);
                const start = context.random.uintLessThanBiased(u32, reference_len);
                const end = context.random.uintLessThanBiased(u32, reference_len - start) + start;

                const key_min = context.reference.items[start].key_min;
                const key_max = context.reference.items[end].key_max;

                {
                    var it = context.level.iterator(
                        lsm.snapshot_latest,
                        key_min,
                        key_max,
                        .ascending,
                    );

                    for (context.reference.items[start .. end + 1]) |expect| {
                        const actual = it.next() orelse return error.TestUnexpectedResult;
                        try testing.expectEqual(expect, actual.*);
                    }
                    try testing.expectEqual(@as(?*const TableInfo, null), it.next());
                }

                {
                    var it = context.level.iterator(
                        lsm.snapshot_latest,
                        key_min,
                        key_max,
                        .descending,
                    );

                    var i = end + 1;
                    while (i > start) {
                        i -= 1;

                        const expect = context.reference.items[i];
                        const actual = it.next() orelse return error.TestUnexpectedResult;
                        try testing.expectEqual(expect, actual.*);
                    }
                    try testing.expectEqual(@as(?*const TableInfo, null), it.next());
                }
            }
        }

        inline fn key_min_from_table(table: TableInfo) Key {
            return table.key_min;
        }
    };
}

test "ManifestLevel" {
    const seed = 42;

    var prng = std.rand.DefaultPrng.init(seed);
    const random = prng.random();

    const Options = struct {
        key_type: type,
        node_size: u32,
        table_count_max: u32,
    };

    inline for (.{
        Options{ .key_type = u64, .node_size = 256, .table_count_max = 33 },
        Options{ .key_type = u64, .node_size = 256, .table_count_max = 34 },
        Options{ .key_type = u64, .node_size = 256, .table_count_max = 1024 },
        Options{ .key_type = u64, .node_size = 512, .table_count_max = 1024 },
        Options{ .key_type = u64, .node_size = 1024, .table_count_max = 1024 },
    }) |options| {
        const Context = TestContext(
            options.node_size,
            options.key_type,
            options.table_count_max,
        );

        var context = try Context.init(random);
        defer context.deinit();

        try context.run();
    }
}
