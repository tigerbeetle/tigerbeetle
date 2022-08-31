const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const config = @import("../config.zig");
const growth_factor = config.lsm_growth_factor;

const table_count_max = @import("tree.zig").table_count_max;
const table_count_max_for_level = @import("tree.zig").table_count_max_for_level;
const snapshot_latest = @import("tree.zig").snapshot_latest;

const Direction = @import("direction.zig").Direction;
const GridType = @import("grid.zig").GridType;
const ManifestLogType = @import("manifest_log.zig").ManifestLogType;
const ManifestLevelType = @import("manifest_level.zig").ManifestLevelType;
const NodePool = @import("node_pool.zig").NodePool(config.lsm_manifest_node_size, 16);
const SegmentedArray = @import("segmented_array.zig").SegmentedArray;

pub fn TableInfoType(comptime Table: type) type {
    const Key = Table.Key;
    const compare_keys = Table.compare_keys;

    return extern struct {
        const TableInfo = @This();

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
            assert(@sizeOf(TableInfo) == 48 + Table.key_size * 2);
            assert(@alignOf(TableInfo) == 16);
            // Assert that there is no implicit padding in the struct.
            assert(@bitSizeOf(TableInfo) == @sizeOf(TableInfo) * 8);
        }

        pub fn visible(table: *const TableInfo, snapshot: u64) bool {
            assert(table.address != 0);
            assert(table.snapshot_min < table.snapshot_max);
            assert(snapshot <= snapshot_latest);

            assert(snapshot != table.snapshot_min);
            assert(snapshot != table.snapshot_max);

            return table.snapshot_min < snapshot and snapshot < table.snapshot_max;
        }

        pub fn invisible(table: *const TableInfo, snapshots: []const u64) bool {
            // Return early and do not iterate all snapshots if the table was never deleted:
            if (table.visible(snapshot_latest)) return false;
            for (snapshots) |snapshot| if (table.visible(snapshot)) return false;
            assert(table.snapshot_max < math.maxInt(u64));
            return true;
        }

        pub fn equal(table: *const TableInfo, other: *const TableInfo) bool {
            // TODO Since the layout of TableInfo is well defined, a direct memcmp may be faster
            // here. However, it's not clear if we can make the assumption that compare_keys()
            // will return .eq exactly when the memory of the keys are equal.
            // Consider defining the API to allow this.
            return table.checksum == other.checksum and
                table.address == other.address and
                table.flags == other.flags and
                table.snapshot_min == other.snapshot_min and
                table.snapshot_max == other.snapshot_max and
                compare_keys(table.key_min, other.key_min) == .eq and
                compare_keys(table.key_max, other.key_max) == .eq;
        }
    };
}

pub fn TableInfoBufferType(comptime Table: type, comptime sort_direction: ?Direction) type {
    const TableInfo = TableInfoType(Table);
    const compare_keys = Table.compare_keys;

    return struct {
        array: []TableInfo,
        count: usize = 0,

        const TableInfoBuffer = @This();

        pub fn init(allocator: mem.Allocator, count_max: usize) !TableInfoBuffer {
            const array = try allocator.alloc(TableInfo, count_max);
            errdefer allocator.free(array);

            return TableInfoBuffer{ .array = array };
        }

        pub fn deinit(buffer: *TableInfoBuffer, allocator: mem.Allocator) void {
            allocator.free(buffer.array);
        }

        pub fn full(buffer: *const TableInfoBuffer) bool {
            assert(buffer.count <= buffer.array.len);
            return buffer.count == buffer.array.len;
        }

        /// Asserts that tables are pushed in sort order.
        pub fn push(buffer: *TableInfoBuffer, table: *const TableInfo) void {
            assert(!buffer.full());

            if (sort_direction) |direction| {
                if (buffer.count > 0) {
                    const tail = &buffer.array[buffer.count - 1];
                    switch (direction) {
                        .ascending => assert(compare_keys(table.key_min, tail.key_min) != .lt),
                        .descending => assert(compare_keys(table.key_max, tail.key_max) != .gt),
                    }
                }
            }

            buffer.array[buffer.count] = table.*;
            buffer.count += 1;
        }

        pub fn drain(buffer: *TableInfoBuffer) []TableInfo {
            assert(buffer.count <= buffer.array.len);

            defer buffer.count = 0;
            // Slice on array.ptr instead of array to avoid
            // having stage1 give us an array.ptr=undefined when buffer.count=0.
            return buffer.array.ptr[0..buffer.count];
        }
    };
}

pub fn ManifestType(comptime Table: type, comptime Storage: type) type {
    const Key = Table.Key;
    const compare_keys = Table.compare_keys;

    return struct {
        const Manifest = @This();

        pub const TableInfo = TableInfoType(Table);
        const TableInfoBuffer = TableInfoBufferType(Table, null);

        const Grid = GridType(Storage);
        const Callback = fn (*Manifest) void;

        /// Levels beyond level 0 have tables with disjoint key ranges.
        /// Here, we use a structure with indexes over the segmented array for performance.
        const Level = ManifestLevelType(NodePool, Key, TableInfo, compare_keys, table_count_max);
        const KeyRange = Level.KeyRange;

        const ManifestLog = ManifestLogType(Storage, TableInfo);

        node_pool: *NodePool,

        levels: [config.lsm_levels]Level,

        open_buffers: [config.lsm_levels]TableInfoBuffer,

        // TODO Set this at startup when reading in the manifest.
        // This should be the greatest TableInfo.snapshot_min/snapshot_max (if deleted) or
        // registered snapshot seen so far.
        snapshot_max: u64 = 1,

        manifest_log: ManifestLog,

        open_callback: ?Callback = null,
        compact_callback: ?Callback = null,
        checkpoint_callback: ?Callback = null,

        pub fn init(
            allocator: mem.Allocator,
            node_pool: *NodePool,
            grid: *Grid,
            tree_hash: u128,
        ) !Manifest {
            var levels: [config.lsm_levels]Level = undefined;
            for (levels) |*level, i| {
                errdefer for (levels[0..i]) |*l| l.deinit(allocator, node_pool);
                level.* = try Level.init(allocator);
            }
            errdefer for (levels) |*l| l.deinit(allocator, node_pool);

            var open_buffers: [config.lsm_levels]TableInfoBuffer = undefined;
            for (open_buffers) |*buffer, i| {
                errdefer for (open_buffers[0..i]) |*b| b.deinit(allocator);
                buffer.* = try TableInfoBuffer.init(allocator, 32);
            }
            errdefer for (open_buffers) |*b| b.deinit(allocator);

            var manifest_log = try ManifestLog.init(allocator, grid, tree_hash);
            errdefer manifest_log.deinit(allocator);

            return Manifest{
                .node_pool = node_pool,
                .levels = levels,
                .manifest_log = manifest_log,
                .open_buffers = open_buffers,
            };
        }

        pub fn deinit(manifest: *Manifest, allocator: mem.Allocator) void {
            for (manifest.levels) |*l| l.deinit(allocator, manifest.node_pool);
            for (manifest.open_buffers) |*b| b.deinit(allocator);

            manifest.manifest_log.deinit(allocator);
        }

        pub fn open(manifest: *Manifest, callback: Callback) void {
            assert(manifest.open_callback == null);
            manifest.open_callback = callback;

            manifest.manifest_log.open(manifest_log_open_event, manifest_log_open_callback);
        }

        fn manifest_log_open_event(
            manifest_log: *ManifestLog,
            level: u7,
            table: *const TableInfo,
        ) void {
            const manifest = @fieldParentPtr(Manifest, "manifest_log", manifest_log);
            assert(manifest.open_callback != null);

            assert(level < config.lsm_levels);
            const buffer = &manifest.open_buffers[level];

            // Make sure theres room in the open buffer to push the table to.
            if (buffer.full()) manifest.drain_open_buffer(buffer, level);

            buffer.push(table);
        }

        fn manifest_log_open_callback(manifest_log: *ManifestLog) void {
            const manifest = @fieldParentPtr(Manifest, "manifest_log", manifest_log);
            assert(manifest.open_callback != null);

            // Insert all left-over pushed open tables into the ManifestLevels.
            for (manifest.open_buffers) |*buffer, level| {
                manifest.drain_open_buffer(buffer, @intCast(u7, level));
            }

            const callback = manifest.open_callback.?;
            manifest.open_callback = null;
            callback(manifest);
        }

        fn drain_open_buffer(manifest: *Manifest, buffer: *TableInfoBuffer, level: u7) void {
            assert(level < config.lsm_levels);
            assert(buffer == &manifest.open_buffers[level]);

            const tables = buffer.drain();
            if (tables.len == 0) return;

            // open() reports table in reverse sorted order
            std.mem.reverse(TableInfo, tables);
            manifest.levels[level].insert_tables(manifest.node_pool, tables);
        }

        pub fn insert_tables(
            manifest: *Manifest,
            level: u8,
            tables: []const TableInfo,
        ) void {
            assert(tables.len > 0);

            const manifest_level = &manifest.levels[level];
            manifest_level.insert_tables(manifest.node_pool, tables);

            // Appends insert changes to the manifest log
            for (tables) |*table| {
                const log_level = @intCast(u7, level);
                manifest.manifest_log.insert(log_level, table);
            }

            // TODO Verify that tables can be found exactly before returning.
        }

        pub fn update_tables(
            manifest: *Manifest,
            level: u8,
            snapshot: u64,
            tables: []const TableInfo,
        ) void {
            assert(tables.len > 0);

            const manifest_level = &manifest.levels[level];
            manifest_level.set_snapshot_max(snapshot, tables);

            // Appends update changes to the manifest log
            for (tables) |*table| {
                const log_level = @intCast(u7, level);
                manifest.manifest_log.insert(log_level, table);
            }
        }

        /// Moves the table at the address/checksum pair from one level to another.
        /// Unlike `update_tables`, this avoids leaving the same TableInfo with different snapshots
        /// in both levels by removing it from level_a before inserting to level_b.
        pub fn move_table(
            manifest: *Manifest,
            level_a: u8,
            level_b: u8,
            snapshot: u64,
            address: u64,
            checksum: u128,
        ) void {
            assert(level_a < config.lsm_levels);
            assert(level_b < config.lsm_levels);
            assert(level_a + 1 == level_b);

            const table_info: TableInfo = blk: {
                _ = address;
                _ = checksum;
                break :blk @panic("TODO(Joran): lookup using address/checksum");
            };

            const tables = [_]TableInfo{table_info};
            manifest.levels[level_a].remove_tables(manifest.node_pool, &.{snapshot}, &tables);
            manifest.levels[level_b].insert_tables(manifest.node_pool, &tables);

            // Appends move changes to the manifest log. (A move is only recorded as an insert).
            for (tables) |*table| {
                const log_level = @intCast(u7, level_b);
                manifest.manifest_log.insert(log_level, table);
            }
        }

        pub fn remove_invisible_tables(
            manifest: *Manifest,
            level: u8,
            snapshot: u64,
            key_min: Key,
            key_max: Key,
        ) void {
            assert(level < config.lsm_levels);
            assert(compare_keys(key_min, key_max) != .gt);

            const direction = .ascending;
            const snapshots = [_]u64{snapshot};
            const manifest_level = &manifest.levels[level];

            var array: [64]TableInfo = undefined;
            var buffer = TableInfoBufferType(Table, direction){ .array = &array };
            var it = manifest_level.iterator(
                .invisible,
                &snapshots,
                direction,
                KeyRange{ .key_min = key_min, .key_max = key_max },
            );

            while (it.next()) |table| {
                assert(table.invisible(&snapshots));
                assert(compare_keys(key_min, table.key_max) != .gt);
                assert(compare_keys(key_max, table.key_min) != .lt);

                // Append remove changes to the manifest log.
                const log_level = @intCast(u7, level);
                manifest.manifest_log.remove(log_level, table);

                if (buffer.full()) {
                    manifest_level.remove_tables(manifest.node_pool, &snapshots, buffer.drain());
                }
                buffer.push(table);
            }
            
            const tables = buffer.drain();
            if (tables.len > 0) {
                manifest_level.remove_tables(manifest.node_pool, &snapshots, tables);
            }
        }

        pub fn lookup(manifest: *Manifest, snapshot: u64, key: Key) LookupIterator {
            return .{
                .manifest = manifest,
                .snapshot = snapshot,
                .key = key,
            };
        }

        pub const LookupIterator = struct {
            manifest: *Manifest,
            snapshot: u64,
            key: Key,
            level: u8 = 0,
            inner: ?Level.Iterator = null,
            precedence: ?u64 = null,

            pub fn next(it: *LookupIterator) ?*const TableInfo {
                while (it.level < config.lsm_levels) : (it.level += 1) {
                    const level = &it.manifest.levels[it.level];

                    var inner = level.iterator(
                        .visible,
                        @as(*const [1]u64, &it.snapshot),
                        .ascending,
                        KeyRange{ .key_min = it.key, .key_max = it.key },
                    );

                    if (inner.next()) |table| {
                        if (it.precedence) |p| assert(p > table.snapshot_min);
                        it.precedence = table.snapshot_min;

                        assert(table.visible(it.snapshot));
                        assert(compare_keys(it.key, table.key_min) != .lt);
                        assert(compare_keys(it.key, table.key_max) != .gt);
                        assert(inner.next() == null);

                        it.level += 1;
                        return table;
                    }
                }

                assert(it.level == config.lsm_levels);
                return null;
            }
        };

        pub fn assert_level_table_counts(manifest: *const Manifest) void {
            for (manifest.levels) |*manifest_level, index| {
                const level = @intCast(u8, index);
                const table_count_visible_max = table_count_max_for_level(growth_factor, level);
                assert(manifest_level.table_count_visible <= table_count_visible_max);
            }
        }

        pub fn assert_no_invisible_tables(manifest: *const Manifest, snapshot: u64) void {
            for (manifest.levels) |*manifest_level| {
                var it = manifest_level.iterator(
                    .invisible,
                    @as(*const [1]u64, &snapshot),
                    .ascending,
                    null,
                );
                assert(it.next() == null);
            }
        }

        /// Returns the next table in the range, after `key_exclusive` if provided.
        pub fn next_table(
            manifest: *const Manifest,
            level: u8,
            snapshot: u64,
            key_min: Key,
            key_max: Key,
            key_exclusive: ?Key,
            direction: Direction,
        ) ?*const TableInfo {
            assert(level < config.lsm_levels);
            assert(compare_keys(key_min, key_max) != .gt);

            const snapshots = [_]u64{snapshot};

            if (key_exclusive == null) {
                return manifest.levels[level].iterator(
                    .visible,
                    &snapshots,
                    direction,
                    KeyRange{ .key_min = key_min, .key_max = key_max },
                ).next();
            }

            assert(compare_keys(key_exclusive.?, key_min) != .lt);
            assert(compare_keys(key_exclusive.?, key_max) != .gt);

            const key_min_exclusive = if (direction == .ascending) key_exclusive.? else key_min;
            const key_max_exclusive = if (direction == .descending) key_exclusive.? else key_max;
            assert(compare_keys(key_min_exclusive, key_max_exclusive) != .gt);

            var it = manifest.levels[level].iterator(
                .visible,
                &snapshots,
                direction,
                KeyRange{ .key_min = key_min_exclusive, .key_max = key_max_exclusive },
            );

            while (it.next()) |table| {
                assert(table.visible(snapshot));
                assert(compare_keys(table.key_min, table.key_max) != .gt);
                assert(compare_keys(table.key_max, key_min_exclusive) != .lt);
                assert(compare_keys(table.key_min, key_max_exclusive) != .gt);

                const next = switch (direction) {
                    .ascending => compare_keys(table.key_min, key_exclusive.?) == .gt,
                    .descending => compare_keys(table.key_max, key_exclusive.?) == .lt,
                };
                if (next) return table;
            }

            return null;
        }

        /// Returns the most optimal table for compaction from a level that is due for compaction.
        /// Returns null if the level is not due for compaction (table_count_visible < count_max).
        pub fn compaction_table(manifest: *const Manifest, level_a: u8) ?CompactionTableRange {
            assert(level_a < config.lsm_levels - 1); // The last level is not compacted into another.

            const table_count_visible_max = table_count_max_for_level(growth_factor, level_a);
            assert(table_count_visible_max > 0);

            const manifest_level: *const Level = &manifest.levels[level_a];
            if (manifest_level.table_count_visible < table_count_visible_max) return null;
            // If even levels are compacted ahead of odd levels, then odd levels may burst.
            assert(manifest_level.table_count_visible <= table_count_visible_max + 1);

            var optimal: ?CompactionTableRange = null;

            const snapshots = [1]u64{snapshot_latest};
            var iterations: usize = 0;
            var it = manifest.levels[level_a].iterator(
                .visible,
                &snapshots,
                .ascending,
                null, // All visible tables in the level therefore no KeyRange filter.
            );

            while (it.next()) |table| {
                iterations += 1;

                const range = manifest.compaction_range(level_a + 1, table.key_min, table.key_max);
                if (optimal == null or range.table_count < optimal.?.range.table_count) {
                    optimal = .{
                        .table = table,
                        .range = range,
                    };
                }
                // If the table can be moved directly between levels then that is already optimal.
                if (optimal.?.range.table_count == 1) break;
            }
            assert(iterations > 0);
            assert(iterations == manifest_level.table_count_visible or
                optimal.?.range.table_count == 1);

            return optimal.?;
        }

        pub const CompactionTableRange = struct {
            table: *const TableInfo,
            range: CompactionRange,
        };

        pub const CompactionRange = struct {
            /// The total number of tables in the compaction across both levels, always at least 1.
            table_count: usize,
            /// The minimum key across both levels.
            key_min: Key,
            /// The maximum key across both levels.
            key_max: Key,
        };

        /// Returns the smallest visible range across level A and B that overlaps key_min/max.
        ///
        /// For example, for a table in level 2, count how many tables overlap in level 3, and
        /// determine the span of their entire key range, which may be broader or narrower.
        ///
        /// The range.table_count includes the input table from level A represented by key_min/max.
        /// Thus range.table_count=1 means that the table may be moved directly between levels.
        ///
        /// The range keys are guaranteed to encompass all the relevant level A and level B tables:
        ///   range.key_min = min(a.key_min, b.key_min)
        ///   range.key_max = max(a.key_max, b.key_max)
        ///
        /// This last invariant is critical to ensuring that tombstones are dropped correctly.
        pub fn compaction_range(
            manifest: *const Manifest,
            level_b: u8,
            key_min: Key,
            key_max: Key,
        ) CompactionRange {
            assert(level_b < config.lsm_levels);
            assert(compare_keys(key_min, key_max) != .gt);

            var range = CompactionRange{
                .table_count = 1,
                .key_min = key_min,
                .key_max = key_max,
            };

            const snapshots = [_]u64{snapshot_latest};
            var it = manifest.levels[level_b].iterator(
                .visible,
                &snapshots,
                .ascending,
                KeyRange{ .key_min = range.key_min, .key_max = range.key_max },
            );

            while (it.next()) |table| : (range.table_count += 1) {
                assert(table.visible(snapshot_latest));
                assert(compare_keys(table.key_min, table.key_max) != .gt);
                assert(compare_keys(table.key_max, range.key_min) != .lt);
                assert(compare_keys(table.key_min, range.key_max) != .gt);

                // The first iterated table.key_min/max may overlap range.key_min/max entirely.
                if (compare_keys(table.key_min, range.key_min) == .lt) {
                    range.key_min = table.key_min;
                }

                // Thereafter, iterated tables may/may not extend the range in ascending order.
                if (compare_keys(table.key_max, range.key_max) == .gt) {
                    range.key_max = table.key_max;
                }
            }

            assert(range.table_count > 0);
            assert(compare_keys(range.key_min, range.key_max) != .gt);
            assert(compare_keys(range.key_min, key_min) != .gt);
            assert(compare_keys(range.key_max, key_max) != .lt);

            return range;
        }

        /// If no subsequent levels have any overlap, then tombstones must be dropped.
        pub fn compaction_must_drop_tombstones(
            manifest: *const Manifest,
            level_b: u8,
            range: CompactionRange,
        ) bool {
            assert(level_b < config.lsm_levels);
            assert(range.table_count > 0);
            assert(compare_keys(range.key_min, range.key_max) != .gt);

            var level_c: u8 = level_b + 1;
            while (level_c < config.lsm_levels) : (level_c += 1) {
                const snapshots = [_]u64{snapshot_latest};

                var it = manifest.levels[level_c].iterator(
                    .visible,
                    &snapshots,
                    .ascending,
                    KeyRange{ .key_min = range.key_min, .key_max = range.key_max },
                );
                if (it.next() == null) {
                    // If the range is being compacted into the last level then this is unreachable,
                    // as the last level has no subsequent levels and must always drop tombstones.
                    assert(level_b != config.lsm_levels - 1);
                    return false;
                }
            }

            assert(level_c == config.lsm_levels);
            return true;
        }

        pub fn compact(manifest: *Manifest, callback: Callback) void {
            assert(manifest.compact_callback == null);
            manifest.compact_callback = callback;

            manifest.manifest_log.compact(manifest_log_compact_callback);
        }

        fn manifest_log_compact_callback(manifest_log: *ManifestLog) void {
            const manifest = @fieldParentPtr(Manifest, "manifest_log", manifest_log);
            assert(manifest.compact_callback != null);

            const callback = manifest.compact_callback.?;
            manifest.compact_callback = null;
            callback(manifest);
        }

        pub fn checkpoint(manifest: *Manifest, callback: Callback) void {
            assert(manifest.checkpoint_callback == null);
            manifest.checkpoint_callback = callback;

            manifest.manifest_log.checkpoint(manifest_log_checkpoint_callback);
        }

        fn manifest_log_checkpoint_callback(manifest_log: *ManifestLog) void {
            const manifest = @fieldParentPtr(Manifest, "manifest_log", manifest_log);
            assert(manifest.checkpoint_callback != null);

            const callback = manifest.checkpoint_callback.?;
            manifest.checkpoint_callback = null;
            callback(manifest);
        }
    };
}
