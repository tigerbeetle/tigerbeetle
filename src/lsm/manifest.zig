const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const growth_factor = constants.lsm_growth_factor;

const table_count_max = @import("tree.zig").table_count_max;
const table_count_max_for_level = @import("tree.zig").table_count_max_for_level;
const snapshot_latest = @import("tree.zig").snapshot_latest;

const Direction = @import("direction.zig").Direction;
const GridType = @import("grid.zig").GridType;
const ManifestLogType = @import("manifest_log.zig").ManifestLogType;
const ManifestLevelType = @import("manifest_level.zig").ManifestLevelType;
const NodePool = @import("node_pool.zig").NodePool(constants.lsm_manifest_node_size, 16);

pub fn TableInfoType(comptime Table: type) type {
    const Key = Table.Key;
    const compare_keys = Table.compare_keys;

    return extern struct {
        const TableInfo = @This();

        /// Checksum of the table's index block.
        checksum: u128,
        /// Address of the table's index block.
        address: u64,
        /// Unused.
        flags: u64 = 0,

        /// The minimum snapshot that can see this table (with exclusive bounds).
        /// - This value is set to the current snapshot tick on table creation.
        snapshot_min: u64,

        /// The maximum snapshot that can see this table (with inclusive bounds).
        /// - This value is set to maxInt(64) when the table is created (output) by compaction.
        /// - This value is set to the current snapshot tick when the table is processed (input) by
        ///   compaction.
        snapshot_max: u64 = math.maxInt(u64),

        key_min: Key, // Inclusive.
        key_max: Key, // Inclusive.

        comptime {
            assert(@sizeOf(TableInfo) == 48 + Table.key_size * 2);
            assert(@alignOf(TableInfo) == 16);
            assert(@bitSizeOf(TableInfo) == @sizeOf(TableInfo) * 8);
        }

        /// Every query targets a particular snapshot. The snapshot determines which tables are
        /// visible to the query — i.e., which tables are accessed to answer the query.
        ///
        /// A table is "visible" to a snapshot if the snapshot lies within the table's
        /// snapshot_min/snapshot_max interval.
        ///
        /// Snapshot visibility is:
        /// - inclusive to snapshot_min.
        ///   (New tables are inserted with `snapshot_min = compaction.snapshot + 1`).
        /// - inclusive to snapshot_max.
        ///   (Tables are made invisible by setting `snapshot_max = compaction.snapshot`).
        ///
        /// Prefetch does not query the output tables of an ongoing compaction, because the output
        /// tables are not ready. Output tables are added to the manifest before being written to
        /// disk.
        ///
        /// Instead, prefetch will continue to query the compaction's input tables until the
        /// half-bar of compaction completes. At that point `tree.prefetch_snapshot_max` is
        /// updated (to the compaction's `compaction_op`), simultaneously rendering the old (input)
        /// tables invisible, and the new (output) tables visible.
        pub fn visible(table: *const TableInfo, snapshot: u64) bool {
            assert(table.address != 0);
            assert(table.snapshot_min <= table.snapshot_max);
            assert(snapshot <= snapshot_latest);

            return table.snapshot_min <= snapshot and snapshot <= table.snapshot_max;
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

pub fn ManifestType(comptime Table: type, comptime Storage: type) type {
    const Key = Table.Key;
    const compare_keys = Table.compare_keys;

    return struct {
        const Manifest = @This();

        pub const TableInfo = TableInfoType(Table);

        const Grid = GridType(Storage);
        const Callback = fn (*Manifest) void;

        /// Here, we use a structure with indexes over the segmented array for performance.
        const Level = ManifestLevelType(NodePool, Key, TableInfo, compare_keys, table_count_max);
        const KeyRange = Level.KeyRange;

        const ManifestLog = ManifestLogType(Storage, TableInfo);

        node_pool: *NodePool,

        levels: [constants.lsm_levels]Level,

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
            var levels: [constants.lsm_levels]Level = undefined;
            for (levels) |*level, i| {
                errdefer for (levels[0..i]) |*l| l.deinit(allocator, node_pool);
                level.* = try Level.init(allocator);
            }
            errdefer for (levels) |*l| l.deinit(allocator, node_pool);

            var manifest_log = try ManifestLog.init(allocator, grid, tree_hash);
            errdefer manifest_log.deinit(allocator);

            return Manifest{
                .node_pool = node_pool,
                .levels = levels,
                .manifest_log = manifest_log,
            };
        }

        pub fn deinit(manifest: *Manifest, allocator: mem.Allocator) void {
            for (manifest.levels) |*l| l.deinit(allocator, manifest.node_pool);

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

            assert(level < constants.lsm_levels);
            manifest.levels[level].insert_table(manifest.node_pool, table);
        }

        fn manifest_log_open_callback(manifest_log: *ManifestLog) void {
            const manifest = @fieldParentPtr(Manifest, "manifest_log", manifest_log);
            assert(manifest.open_callback != null);

            const callback = manifest.open_callback.?;
            manifest.open_callback = null;
            callback(manifest);
        }

        pub fn insert_table(
            manifest: *Manifest,
            level: u8,
            table: *const TableInfo,
        ) void {
            const manifest_level = &manifest.levels[level];
            manifest_level.insert_table(manifest.node_pool, table);

            // Append insert changes to the manifest log.
            const log_level = @intCast(u7, level);
            manifest.manifest_log.insert(log_level, table);

            if (constants.verify) {
                assert(manifest_level.contains(table));
            }
        }

        /// Updates the snapshot_max on the provide table for the given level.
        /// The table provided is mutable to allow its snapshot_max to be updated.
        pub fn update_table(
            manifest: *Manifest,
            level: u8,
            snapshot: u64,
            table: *TableInfo,
        ) void {
            const manifest_level = &manifest.levels[level];

            assert(table.snapshot_max >= snapshot);
            manifest_level.set_snapshot_max(snapshot, table);
            assert(table.snapshot_max == snapshot);

            // Append update changes to the manifest log.
            const log_level = @intCast(u7, level);
            manifest.manifest_log.insert(log_level, table);
        }

        pub fn move_table(
            manifest: *Manifest,
            level_a: u8,
            level_b: u8,
            table: *const TableInfo,
        ) void {
            assert(level_b == level_a + 1);
            assert(level_b < constants.lsm_levels);

            const manifest_level_a = &manifest.levels[level_a];
            const manifest_level_b = &manifest.levels[level_b];

            if (constants.verify) {
                assert(manifest_level_a.contains(table));
                assert(!manifest_level_b.contains(table));
            }

            // First, remove the table from level A without appending changes to the manifest log.
            const removed = manifest_level_a.remove_table_visible(manifest.node_pool, table);
            assert(table.equal(removed));

            // Then, insert the table into level B and append these changes to the manifest log.
            // To move a table w.r.t manifest log, a "remove" change should NOT be appended for
            // the previous level A; When replaying the log from open(), inserts are processed in
            // LIFO order and duplicates are ignored. This means the table will only be replayed in
            // level B instead of the old one in level A.
            manifest_level_b.insert_table(manifest.node_pool, table);
            manifest.manifest_log.insert(@intCast(u7, level_b), table);

            if (constants.verify) {
                assert(!manifest_level_a.contains(table));
                assert(manifest_level_b.contains(table));
            }
        }

        pub fn remove_invisible_tables(
            manifest: *Manifest,
            level: u8,
            snapshot: u64,
            key_min: Key,
            key_max: Key,
        ) void {
            assert(level < constants.lsm_levels);
            assert(compare_keys(key_min, key_max) != .gt);

            // Remove tables in descending order to avoid desynchronizing the iterator from
            // the ManifestLevel.
            const direction = .descending;
            const snapshots = [_]u64{snapshot};
            const manifest_level = &manifest.levels[level];

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

                // Append remove changes to the manifest log and purge from memory (ManifestLevel):
                manifest.manifest_log.remove(@intCast(u7, level), table);
                manifest_level.remove_table_invisible(manifest.node_pool, &snapshots, table);
            }

            if (constants.verify) manifest.assert_no_invisible_tables_at_level(level, snapshot);
        }

        /// Returns an iterator over tables that might contain `key` (but are not guaranteed to).
        pub fn lookup(manifest: *Manifest, snapshot: u64, key: Key) LookupIterator {
            return .{
                .manifest = manifest,
                .snapshot = snapshot,
                .key = key,
            };
        }

        pub const LookupIterator = struct {
            manifest: *const Manifest,
            snapshot: u64,
            key: Key,
            level: u8 = 0,
            inner: ?Level.Iterator = null,

            pub fn next(it: *LookupIterator) ?*const TableInfo {
                while (it.level < constants.lsm_levels) : (it.level += 1) {
                    const level = &it.manifest.levels[it.level];

                    var inner = level.iterator(
                        .visible,
                        @as(*const [1]u64, &it.snapshot),
                        .ascending,
                        KeyRange{ .key_min = it.key, .key_max = it.key },
                    );

                    if (inner.next()) |table| {
                        assert(table.visible(it.snapshot));
                        assert(compare_keys(it.key, table.key_min) != .lt);
                        assert(compare_keys(it.key, table.key_max) != .gt);
                        assert(inner.next() == null);

                        it.level += 1;
                        return table;
                    }
                }

                assert(it.level == constants.lsm_levels);
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
            for (manifest.levels) |_, level| {
                manifest.assert_no_invisible_tables_at_level(@intCast(u8, level), snapshot);
            }
        }

        fn assert_no_invisible_tables_at_level(
            manifest: *const Manifest,
            level: u8,
            snapshot: u64,
        ) void {
            var it = manifest.levels[level].iterator(
                .invisible,
                @as(*const [1]u64, &snapshot),
                .ascending,
                null,
            );
            assert(it.next() == null);
        }

        /// Returns the next table in the range, after `key_exclusive` if provided.
        ///
        /// * The table returned is visible to `snapshot`.
        pub fn next_table(
            manifest: *const Manifest,
            level: u8,
            snapshot: u64,
            key_min: Key,
            key_max: Key,
            key_exclusive: ?Key,
            direction: Direction,
        ) ?*const TableInfo {
            assert(level < constants.lsm_levels);
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
            // The last level is not compacted into another.
            assert(level_a < constants.lsm_levels - 1);

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
                        .table = table.*,
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
            table: TableInfo,
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
            assert(level_b < constants.lsm_levels);
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
            assert(level_b < constants.lsm_levels);
            assert(range.table_count > 0);
            assert(compare_keys(range.key_min, range.key_max) != .gt);

            var level_c: u8 = level_b + 1;
            while (level_c < constants.lsm_levels) : (level_c += 1) {
                const snapshots = [_]u64{snapshot_latest};

                var it = manifest.levels[level_c].iterator(
                    .visible,
                    &snapshots,
                    .ascending,
                    KeyRange{ .key_min = range.key_min, .key_max = range.key_max },
                );
                if (it.next() != null) {
                    // If the range is being compacted into the last level then this is unreachable,
                    // as the last level has no subsequent levels and must always drop tombstones.
                    assert(level_b != constants.lsm_levels - 1);
                    return false;
                }
            }

            assert(level_c == constants.lsm_levels);
            return true;
        }

        pub fn reserve(manifest: *Manifest) void {
            assert(manifest.compact_callback == null);
            assert(manifest.checkpoint_callback == null);

            manifest.manifest_log.reserve();
        }

        pub fn compact(manifest: *Manifest, callback: Callback) void {
            assert(manifest.compact_callback == null);
            assert(manifest.checkpoint_callback == null);
            manifest.compact_callback = callback;

            manifest.manifest_log.compact(manifest_log_compact_callback);
        }

        fn manifest_log_compact_callback(manifest_log: *ManifestLog) void {
            const manifest = @fieldParentPtr(Manifest, "manifest_log", manifest_log);
            assert(manifest.compact_callback != null);
            assert(manifest.checkpoint_callback == null);

            const callback = manifest.compact_callback.?;
            manifest.compact_callback = null;
            callback(manifest);
        }

        pub fn checkpoint(manifest: *Manifest, callback: Callback) void {
            assert(manifest.compact_callback == null);
            assert(manifest.checkpoint_callback == null);
            manifest.checkpoint_callback = callback;

            manifest.manifest_log.checkpoint(manifest_log_checkpoint_callback);
        }

        fn manifest_log_checkpoint_callback(manifest_log: *ManifestLog) void {
            const manifest = @fieldParentPtr(Manifest, "manifest_log", manifest_log);
            assert(manifest.compact_callback == null);
            assert(manifest.checkpoint_callback != null);

            const callback = manifest.checkpoint_callback.?;
            manifest.checkpoint_callback = null;
            callback(manifest);
        }

        pub fn verify(manifest: *Manifest, snapshot: u64) void {
            for (manifest.levels) |*level| {
                var key_max_prev: ?Key = null;
                var table_info_iter = level.iterator(
                    .visible,
                    &.{snapshot},
                    .ascending,
                    null,
                );
                while (table_info_iter.next()) |table_info| {
                    if (key_max_prev) |k| {
                        assert(compare_keys(k, table_info.key_min) == .lt);
                    }
                    // We could have key_min == key_max if there is only one value.
                    assert(compare_keys(table_info.key_min, table_info.key_max) != .gt);
                    key_max_prev = table_info.key_max;

                    Table.verify(
                        Storage,
                        manifest.manifest_log.grid.superblock.storage,
                        table_info.address,
                        table_info.key_min,
                        table_info.key_max,
                    );
                }
            }
        }
    };
}
