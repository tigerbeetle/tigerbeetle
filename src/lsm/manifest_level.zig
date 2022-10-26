//! A ManifestLevel is an in-memory collection of the table metadata for a single level of a tree.
//!
//! For a given level and snapshot, there may be gaps in the key ranges of the visible tables,
//! but the key ranges are disjoint.
//!
//! A level's tables can be visualized in 2D as a partitioned rectangle.
//! For example, given the ManifestLevel tables (with values chosen for visualization, not realism):
//!
//!          label   A   B   C   D   E   F   G   H   I   J   K   L   M
//!        key_min   0   4  12  16   4   8  12  26   4  25   4  16  24
//!        key_max   3  11  15  19   7  11  15  27   7  27  11  19  27
//!   snapshot_min   1   1   1   1   3   3   3   3   5   5   7   7   7
//!   snapshot_max   9   3   3   7   5   7   9   5   7   7   9   9   9
//!
//!     0         1         2
//!     0   4   8   2   6   0   4   8
//!   9┌───┬───────┬───┬───┬───┬───┐
//!    │   │   K   │   │ L │###│ M │
//!   7│   ├───┬───┤   ├───┤###└┬──┤
//!    │   │ I │   │ G │   │####│ J│
//!   5│ A ├───┤ F │   │   │####└┬─┤
//!    │   │ E │   │   │ D │#####│H│
//!   3│   ├───┴───┼───┤   │#####└─┤
//!    │   │   B   │ C │   │#######│
//!   1└───┴───────┴───┴───┴───────┘
//!
//! Example iterations:
//!
//!   visibility  snapshots   direction  key_min  key_max  tables
//!      visible          2   ascending        0       28  A, B, C, D
//!      visible          4   ascending        0       28  A, E, F, G, D, H
//!      visible          6  descending       12       28  J, D, G
//!      visible          8   ascending        0       28  A, K, G, L, M
//!    invisible    2, 4, 6   ascending        0       28  K, L, M
//!
//! Legend:
//!
//! * "#" represents a gap — no tables cover these keys during the snapshot.
//! * The horizontal axis represents the key range.
//! * The vertical axis represents the snapshot range.
//! * Each rectangle is a table within the manifest level.
//! * The sides of each rectangle depict:
//!   * left:   table.key_min (the diagram is inclusive, and the table.key_min is inclusive)
//!   * right:  table.key_max (the diagram is EXCLUSIVE, but the table.key_max is INCLUSIVE)
//!   * bottom: table.snapshot_min (inclusive)
//!   * top:    table.snapshot_max (inclusive)
//! * (Not depicted: tables may have `table.key_min == table.key_max`.)
//! * (Not depicted: the newest set of tables would have `table.snapshot_max == maxInt(u64)`.)
//!
const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;
const meta = std.meta;

const config = @import("../config.zig");
const lsm = @import("tree.zig");
const binary_search = @import("binary_search.zig");

const Direction = @import("direction.zig").Direction;
const SegmentedArray = @import("segmented_array.zig").SegmentedArray;
const SortedSegmentedArray = @import("segmented_array.zig").SortedSegmentedArray;

pub fn ManifestLevelType(
    comptime NodePool: type,
    comptime Key: type,
    comptime TableInfo: type,
    comptime compare_keys: fn (Key, Key) callconv(.Inline) math.Order,
    comptime table_count_max: u32,
) type {
    return struct {
        const Self = @This();

        pub const Keys = SortedSegmentedArray(
            Key,
            NodePool,
            table_count_max,
            Key,
            struct {
                inline fn key_from_value(value: *const Key) Key {
                    return value.*;
                }
            }.key_from_value,
            compare_keys,
            .{},
        );

        pub const Tables = SegmentedArray(TableInfo, NodePool, table_count_max, .{});

        // These two segmented arrays are parallel. That is, the absolute indexes of maximum key
        // and corresponding TableInfo are the same. However, the number of nodes, node index, and
        // relative index into the node differ as the elements per node are different.
        //
        // Ordered by ascending (maximum) key. Keys may repeat due to snapshots.
        keys: Keys,
        tables: Tables,

        /// The number of tables visible to snapshot_latest.
        /// Used to enforce table_count_max_for_level().
        table_count_visible: u32 = 0,

        pub fn init(allocator: mem.Allocator) !Self {
            var keys = try Keys.init(allocator);
            errdefer keys.deinit(allocator, null);

            var tables = try Tables.init(allocator);
            errdefer tables.deinit(allocator, null);

            return Self{
                .keys = keys,
                .tables = tables,
            };
        }

        pub fn deinit(level: *Self, allocator: mem.Allocator, node_pool: *NodePool) void {
            level.keys.deinit(allocator, node_pool);
            level.tables.deinit(allocator, node_pool);
        }

        /// Inserts an ordered batch of tables into the level, then rebuilds the indexes.
        pub fn insert_table(level: *Self, node_pool: *NodePool, table: *const TableInfo) void {
            assert(level.keys.len() == level.tables.len());

            const absolute_index = level.keys.insert_element(node_pool, table.key_max);
            assert(absolute_index < level.keys.len());
            level.tables.insert_elements(node_pool, absolute_index, &[_]TableInfo{table.*});

            if (table.visible(lsm.snapshot_latest)) level.table_count_visible += 1;

            assert(level.keys.len() == level.tables.len());
        }

        /// Set snapshot_max for the given table in the ManifestLevel.
        ///
        /// * The table is mutable so that this function can update its snapshot.
        /// * Asserts that the table currently has snapshot_max of math.maxInt(u64).
        /// * Asserts that the table exists in the manifest.
        pub fn set_snapshot_max(level: *Self, snapshot: u64, table: *TableInfo) void {
            assert(snapshot < lsm.snapshot_latest);
            assert(table.snapshot_max == math.maxInt(u64));

            const key_min = table.key_min;
            const key_max = table.key_max;
            assert(compare_keys(key_min, key_max) != .gt);

            var it = level.iterator(
                .visible,
                @as(*const [1]u64, &lsm.snapshot_latest),
                .ascending,
                KeyRange{ .key_min = key_min, .key_max = key_max },
            );

            const level_table_const = it.next().?;
            // This const cast is safe as we know that the memory pointed to is in fact
            // mutable. That is, the table is not in the .text or .rodata section. We do this
            // to avoid duplicating the iterator code in order to expose only a const iterator
            // in the public API.
            const level_table = @intToPtr(*TableInfo, @ptrToInt(level_table_const));
            assert(level_table.equal(table));
            assert(level_table.snapshot_max == math.maxInt(u64));

            level_table.snapshot_max = snapshot;
            table.snapshot_max = snapshot;

            assert(it.next() == null);
            level.table_count_visible -= 1;
        }

        /// Remove the given table from the ManifestLevel, asserting that it is not visible
        /// by any snapshot in `snapshots` or by `lsm.snapshot_latest`.
        pub fn remove_table(
            level: *Self,
            node_pool: *NodePool,
            snapshots: []const u64,
            table: *const TableInfo,
        ) void {
            assert(level.keys.len() == level.tables.len());
            // The batch may contain a single table, with a single key, i.e. key_min == key_max:
            assert(compare_keys(table.key_min, table.key_max) != .gt);

            // Use `key_min` for both ends of the iterator; we are looking for a single table.
            const cursor_start = level.iterator_start(table.key_min, table.key_min, .ascending).?;
            var absolute_index = level.keys.absolute_index_for_cursor(cursor_start);

            var it = level.tables.iterator_from_index(absolute_index, .ascending);
            while (it.next()) |level_table| : (absolute_index += 1) {
                if (level_table.invisible(snapshots)) {
                    assert(level_table.equal(table));

                    level.keys.remove_elements(node_pool, absolute_index, 1);
                    level.tables.remove_elements(node_pool, absolute_index, 1);
                    break;
                }
            } else {
                unreachable;
            }

            assert(level.keys.len() == level.tables.len());
        }

        pub const Visibility = enum {
            visible,
            invisible,
        };

        pub const KeyRange = struct {
            key_min: Key, // Inclusive.
            key_max: Key, // Inclusive.
        };

        pub fn iterator(
            level: *const Self,
            visibility: Visibility,
            snapshots: []const u64,
            direction: Direction,
            key_range: ?KeyRange,
        ) Iterator {
            for (snapshots) |snapshot| {
                assert(snapshot <= lsm.snapshot_latest);
            }

            const inner = blk: {
                if (key_range) |range| {
                    assert(compare_keys(range.key_min, range.key_max) != .gt);

                    if (level.iterator_start(range.key_min, range.key_max, direction)) |start| {
                        break :blk level.tables.iterator_from_index(
                            level.keys.absolute_index_for_cursor(start),
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
                } else {
                    switch (direction) {
                        .ascending => break :blk level.tables.iterator_from_index(0, direction),
                        .descending => {
                            break :blk level.tables.iterator_from_cursor(level.tables.last(), .descending);
                        },
                    }
                }
            };

            return .{
                .level = level,
                .inner = inner,
                .visibility = visibility,
                .snapshots = snapshots,
                .direction = direction,
                .key_range = key_range,
            };
        }

        pub const Iterator = struct {
            level: *const Self,
            inner: Tables.Iterator,
            visibility: Visibility,
            snapshots: []const u64,
            direction: Direction,
            key_range: ?KeyRange,

            pub fn next(it: *Iterator) ?*const TableInfo {
                while (it.inner.next()) |table| {
                    // We can't assert !it.inner.done as inner.next() may set done before returning.

                    // Skip tables that don't match the provided visibility interests.
                    switch (it.visibility) {
                        .invisible => blk: {
                            if (table.invisible(it.snapshots)) break :blk;
                            continue;
                        },
                        .visible => blk: {
                            for (it.snapshots) |snapshot| {
                                if (table.visible(snapshot)) break :blk;
                            }
                            continue;
                        },
                    }

                    // Filter the table using the key range if provided.
                    if (it.key_range) |key_range| {
                        switch (it.direction) {
                            .ascending => {
                                // Assert that the table is not out of bounds to the left.
                                //
                                // We can assert this as it is exactly the same key comparison when
                                // we binary search in iterator_start(), and since we move in
                                // ascending order this remains true beyond the first iteration.
                                assert(compare_keys(table.key_max, key_range.key_min) != .lt);

                                // Check if the table is out of bounds to the right.
                                if (compare_keys(table.key_min, key_range.key_max) == .gt) {
                                    it.inner.done = true;
                                    return null;
                                }
                            },
                            .descending => {
                                // Check if the table is out of bounds to the right.
                                //
                                // Unlike in the ascending case, it is not guaranteed that
                                // table.key_min is less than or equal to key_range.key_max on the
                                // first iteration as only the key_max of a table is stored in our
                                // key nodes. On subsequent iterations this check will always
                                // be false.
                                if (compare_keys(table.key_min, key_range.key_max) == .gt) {
                                    continue;
                                }

                                // Check if the table is out of bounds to the left.
                                if (compare_keys(table.key_max, key_range.key_min) == .lt) {
                                    it.inner.done = true;
                                    return null;
                                }
                            },
                        }
                    }

                    return table;
                }

                assert(it.inner.done);
                return null;
            }
        };

        /// Returns the keys segmented array cursor at which iteration should be started.
        /// May return null if there is nothing to iterate because we know for sure that the key
        /// range is disjoint with the tables stored in this level.
        ///
        /// However, the cursor returned is not guaranteed to be in range for the query as only
        /// the key_max is stored in the index structures, not the key_min, and only the start
        /// bound for the given direction is checked here.
        fn iterator_start(
            level: Self,
            key_min: Key,
            key_max: Key,
            direction: Direction,
        ) ?Keys.Cursor {
            assert(compare_keys(key_min, key_max) != .gt);
            assert(level.keys.len() == level.tables.len());

            if (level.keys.len() == 0) return null;

            // Ascending:  Find the first table where table.key_max ≤ iterator.key_min.
            // Descending: Find the first table where table.key_max ≤ iterator.key_max.
            const target = level.keys.search(switch (direction) {
                .ascending => key_min,
                .descending => key_max,
            });
            assert(target.node <= level.keys.node_count);

            if (level.keys.absolute_index_for_cursor(target) == level.keys.len()) {
                return switch (direction) {
                    // The key_min of the target range is greater than the key_max of the last
                    // table in the level and we are ascending, so this range matches no tables
                    // on this level.
                    .ascending => null,
                    // The key_max of the target range is greater than the key_max of the last
                    // table in the level and we are descending, so we need to start iteration
                    // at the last table in the level.
                    .descending => level.keys.last(),
                };
            } else {
                // Multiple tables in the level may share a key.
                // Scan to the edge so that the iterator will cover them all.
                return level.iterator_start_boundary(target, direction);
            }
        }

        /// This function exists because there may be tables in the level with the same
        /// key_max but non-overlapping snapshot visibility.
        ///
        /// Put differently, there may be several tables with different snapshots but the same
        /// `key_max`, and `iterator_start`'s binary search (`key_cursor`) may have landed in the
        /// middle of them.
        fn iterator_start_boundary(
            level: Self,
            key_cursor: Keys.Cursor,
            direction: Direction,
        ) Keys.Cursor {
            var reverse = level.keys.iterator_from_cursor(key_cursor, direction.reverse());
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

        /// The function is only used for verification; it is not performance-critical.
        pub fn contains(level: Self, table: *const TableInfo) bool {
            assert(config.verify);

            var level_tables = level.iterator(.visible, &.{
                table.snapshot_min,
            }, .ascending, KeyRange{
                .key_min = table.key_min,
                .key_max = table.key_max,
            });
            while (level_tables.next()) |level_table| {
                if (level_table.equal(table)) return true;
            }
            return false;
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

        const Value = struct {
            key: Key,
            tombstone: bool,
        };

        inline fn compare_keys(a: Key, b: Key) math.Order {
            return math.order(a, b);
        }

        inline fn key_from_value(value: *const Value) Key {
            return value.key;
        }

        inline fn tombstone_from_key(key: Key) Value {
            return .{ .key = key, .tombstone = true };
        }

        inline fn tombstone(value: *const Value) bool {
            return value.tombstone;
        }

        const Table = @import("table.zig").TableType(
            Key,
            Value,
            compare_keys,
            key_from_value,
            std.math.maxInt(Key),
            tombstone,
            tombstone_from_key,
        );

        const TableInfo = @import("manifest.zig").TableInfoType(Table);
        const NodePool = @import("node_pool.zig").NodePool;

        const TestPool = NodePool(node_size, @alignOf(TableInfo));
        const TestLevel = ManifestLevelType(TestPool, Key, TableInfo, compare_keys, table_count_max);
        const KeyRange = TestLevel.KeyRange;

        random: std.rand.Random,

        pool: TestPool,
        level: TestLevel,

        snapshot_max: u64 = 1,
        snapshots: std.BoundedArray(u64, 8) = .{ .buffer = undefined },
        snapshot_tables: std.BoundedArray(std.ArrayList(TableInfo), 8) = .{ .buffer = undefined },

        /// Contains only tables with snapshot_max == lsm.snapshot_latest
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

            var reference = std.ArrayList(TableInfo).init(testing.allocator);
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

            for (context.snapshot_tables.slice()) |tables| tables.deinit();

            context.reference.deinit();
        }

        fn run(context: *Self) !void {
            if (log) std.debug.print("\n", .{});

            {
                var i: usize = 0;
                while (i < table_count_max * 2) : (i += 1) {
                    switch (context.random.uintLessThanBiased(u32, 100)) {
                        0...59 => try context.insert_tables(),
                        60...69 => try context.create_snapshot(),
                        70...94 => try context.delete_tables(),
                        95...99 => try context.drop_snapshot(),
                        else => unreachable,
                    }
                }
            }

            {
                var i: usize = 0;
                while (i < table_count_max * 2) : (i += 1) {
                    switch (context.random.uintLessThanBiased(u32, 100)) {
                        0...34 => try context.insert_tables(),
                        35...39 => try context.create_snapshot(),
                        40...89 => try context.delete_tables(),
                        90...99 => try context.drop_snapshot(),
                        else => unreachable,
                    }
                }
            }

            try context.remove_all();
        }

        fn insert_tables(context: *Self) !void {
            const count_free = table_count_max - context.level.keys.len();

            if (count_free == 0) return;

            var buffer: [13]TableInfo = undefined;

            const count_max = @min(count_free, 13);
            const count = context.random.uintAtMostBiased(u32, count_max - 1) + 1;

            {
                var key: Key = context.random.uintAtMostBiased(Key, table_count_max * 64);

                for (buffer[0..count]) |*table| {
                    table.* = context.random_greater_non_overlapping_table(key);
                    key = table.key_max;
                }
            }

            for (buffer[0..count]) |*table| {
                context.level.insert_table(&context.pool, table);
            }

            for (buffer[0..count]) |table| {
                const index = binary_search.binary_search_values_raw(
                    Key,
                    TableInfo,
                    key_min_from_table,
                    compare_keys,
                    context.reference.items,
                    table.key_max,
                    .{},
                );
                // Can't be equal as the tables may not overlap
                if (index < context.reference.items.len) {
                    assert(context.reference.items[index].key_min > table.key_max);
                }
                context.reference.insert(index, table) catch unreachable;
            }

            context.inserts += count;

            try context.verify();
        }

        fn random_greater_non_overlapping_table(context: *Self, key: Key) TableInfo {
            var new_key_min = key + context.random.uintLessThanBiased(Key, 31) + 1;

            assert(compare_keys(new_key_min, key) == .gt);

            var i = binary_search.binary_search_values_raw(
                Key,
                TableInfo,
                key_min_from_table,
                compare_keys,
                context.reference.items,
                new_key_min,
                .{},
            );

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

            const max_delta = @min(32, next_key_min - 1 - new_key_min);
            const new_key_max = new_key_min + context.random.uintAtMostBiased(Key, max_delta);

            return .{
                .checksum = context.random.int(u128),
                .address = context.random.int(u64),
                .snapshot_min = context.take_snapshot(),
                .key_min = new_key_min,
                .key_max = new_key_max,
            };
        }

        /// See Manifest.take_snapshot()
        fn take_snapshot(context: *Self) u64 {
            // A snapshot cannot be 0 as this is a reserved value in the superblock.
            assert(context.snapshot_max > 0);
            // The constant snapshot_latest must compare greater than any issued snapshot.
            // This also ensures that we are not about to overflow the u64 counter.
            assert(context.snapshot_max < lsm.snapshot_latest - 1);

            context.snapshot_max += 1;

            return context.snapshot_max;
        }

        fn create_snapshot(context: *Self) !void {
            if (context.snapshots.len == context.snapshots.capacity()) return;

            context.snapshots.appendAssumeCapacity(context.take_snapshot());

            const tables = context.snapshot_tables.addOneAssumeCapacity();
            tables.* = std.ArrayList(TableInfo).init(testing.allocator);
            try tables.insertSlice(0, context.reference.items);
        }

        fn drop_snapshot(context: *Self) !void {
            if (context.snapshots.len == 0) return;

            const index = context.random.uintLessThanBiased(usize, context.snapshots.len);

            _ = context.snapshots.swapRemove(index);
            var tables = context.snapshot_tables.swapRemove(index);
            defer tables.deinit();

            // Use this memory as a scratch buffer since it's conveniently already allocated.
            tables.clearRetainingCapacity();

            const snapshots = context.snapshots.slice();

            // Ensure that iteration with a null key range in both directions is tested.
            if (context.random.boolean()) {
                var it = context.level.iterator(.invisible, snapshots, .ascending, null);
                while (it.next()) |table| try tables.append(table.*);
            } else {
                var it = context.level.iterator(.invisible, snapshots, .descending, null);
                while (it.next()) |table| try tables.append(table.*);
                mem.reverse(TableInfo, tables.items);
            }

            if (tables.items.len > 0) {
                for (tables.items) |*table| {
                    context.level.remove_table(&context.pool, snapshots, table);
                }
            }
        }

        fn delete_tables(context: *Self) !void {
            const reference_len = @intCast(u32, context.reference.items.len);
            if (reference_len == 0) return;

            const count_max = @min(reference_len, 13);
            const count = context.random.uintAtMostBiased(u32, count_max - 1) + 1;

            assert(context.reference.items.len <= table_count_max);
            const index = context.random.uintAtMostBiased(u32, reference_len - count);

            const snapshot = context.take_snapshot();

            for (context.reference.items[index..][0..count]) |*table| {
                context.level.set_snapshot_max(snapshot, table);
            }

            for (context.snapshot_tables.slice()) |tables| {
                for (tables.items) |*table| {
                    for (context.reference.items[index..][0..count]) |modified| {
                        if (table.address == modified.address) {
                            table.snapshot_max = snapshot;
                            assert(table.equal(&modified));
                        }
                    }
                }
            }

            {
                var to_remove = std.ArrayList(TableInfo).init(testing.allocator);
                defer to_remove.deinit();

                for (context.reference.items[index..][0..count]) |table| {
                    if (table.invisible(context.snapshots.slice())) {
                        try to_remove.append(table);
                    }
                }

                if (log) {
                    std.debug.print("Removing tables: ", .{});
                    for (to_remove.items) |t| {
                        std.debug.print("[{},{}], ", .{ t.key_min, t.key_max });
                    }
                    std.debug.print("\n", .{});
                    std.debug.print("\nactual: ", .{});
                    var it = context.level.iterator(
                        .invisible,
                        context.snapshots.slice(),
                        .ascending,
                        KeyRange{ .key_min = 0, .key_max = math.maxInt(Key) },
                    );
                    while (it.next()) |t| std.debug.print("[{},{}], ", .{ t.key_min, t.key_max });
                    std.debug.print("\n", .{});
                }

                if (to_remove.items.len > 0) {
                    for (to_remove.items) |*table| {
                        context.level.remove_table(
                            &context.pool,
                            context.snapshots.slice(),
                            table,
                        );
                    }
                }
            }

            context.reference.replaceRange(index, count, &[0]TableInfo{}) catch unreachable;

            context.removes += count;

            try context.verify();
        }

        fn remove_all(context: *Self) !void {
            while (context.snapshots.len > 0) try context.drop_snapshot();
            while (context.reference.items.len > 0) try context.delete_tables();

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
            try context.verify_snapshot(lsm.snapshot_latest, context.reference.items);

            for (context.snapshots.slice()) |snapshot, i| {
                try context.verify_snapshot(snapshot, context.snapshot_tables.get(i).items);
            }
        }

        fn verify_snapshot(context: *Self, snapshot: u64, reference: []const TableInfo) !void {
            if (log) {
                std.debug.print("\nsnapshot: {}\n", .{snapshot});
                std.debug.print("expect: ", .{});
                for (reference) |t| std.debug.print("[{},{}], ", .{ t.key_min, t.key_max });

                std.debug.print("\nactual: ", .{});
                var it = context.level.iterator(
                    .visible,
                    @as(*const [1]u64, &snapshot),
                    .ascending,
                    KeyRange{ .key_min = 0, .key_max = math.maxInt(Key) },
                );
                while (it.next()) |t| std.debug.print("[{},{}], ", .{ t.key_min, t.key_max });
                std.debug.print("\n", .{});
            }

            {
                var it = context.level.iterator(
                    .visible,
                    @as(*const [1]u64, &snapshot),
                    .ascending,
                    KeyRange{ .key_min = 0, .key_max = math.maxInt(Key) },
                );

                for (reference) |expect| {
                    const actual = it.next() orelse return error.TestUnexpectedResult;
                    try testing.expectEqual(expect, actual.*);
                }
                try testing.expectEqual(@as(?*const TableInfo, null), it.next());
            }

            {
                var it = context.level.iterator(
                    .visible,
                    @as(*const [1]u64, &snapshot),
                    .descending,
                    KeyRange{ .key_min = 0, .key_max = math.maxInt(Key) },
                );

                var i = reference.len;
                while (i > 0) {
                    i -= 1;

                    const expect = reference[i];
                    const actual = it.next() orelse return error.TestUnexpectedResult;
                    try testing.expectEqual(expect, actual.*);
                }
                try testing.expectEqual(@as(?*const TableInfo, null), it.next());
            }

            if (reference.len > 0) {
                const reference_len = @intCast(u32, reference.len);
                const start = context.random.uintLessThanBiased(u32, reference_len);
                const end = context.random.uintLessThanBiased(u32, reference_len - start) + start;

                const key_min = reference[start].key_min;
                const key_max = reference[end].key_max;

                {
                    var it = context.level.iterator(
                        .visible,
                        @as(*const [1]u64, &snapshot),
                        .ascending,
                        KeyRange{ .key_min = key_min, .key_max = key_max },
                    );

                    for (reference[start .. end + 1]) |expect| {
                        const actual = it.next() orelse return error.TestUnexpectedResult;
                        try testing.expectEqual(expect, actual.*);
                    }
                    try testing.expectEqual(@as(?*const TableInfo, null), it.next());
                }

                {
                    var it = context.level.iterator(
                        .visible,
                        @as(*const [1]u64, &snapshot),
                        .descending,
                        KeyRange{ .key_min = key_min, .key_max = key_max },
                    );

                    var i = end + 1;
                    while (i > start) {
                        i -= 1;

                        const expect = reference[i];
                        const actual = it.next() orelse return error.TestUnexpectedResult;
                        try testing.expectEqual(expect, actual.*);
                    }
                    try testing.expectEqual(@as(?*const TableInfo, null), it.next());
                }
            }
        }

        inline fn key_min_from_table(table: *const TableInfo) Key {
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
