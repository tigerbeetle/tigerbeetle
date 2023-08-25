const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
const growth_factor = constants.lsm_growth_factor;

const table_count_max = @import("tree.zig").table_count_max;
const table_count_max_for_level = @import("tree.zig").table_count_max_for_level;
const snapshot_latest = @import("tree.zig").snapshot_latest;
const schema = @import("schema.zig");

const TreeConfig = @import("tree.zig").TreeConfig;
const Direction = @import("direction.zig").Direction;
const GridType = @import("../vsr/grid.zig").GridType;
const ManifestLogType = @import("manifest_log.zig").ManifestLogType;
const ManifestLevelType = @import("manifest_level.zig").ManifestLevelType;
const NodePool = @import("node_pool.zig").NodePool(constants.lsm_manifest_node_size, 16);
const TableInfo = schema.ManifestLog.TableInfo;

pub fn TreeTableInfoType(comptime Table: type) type {
    const Key = Table.Key;
    const compare_keys = Table.compare_keys;

    return struct {
        const TreeTableInfo = @This();

        /// Checksum of the table's index block.
        checksum: u128,
        /// Address of the table's index block.
        address: u64,

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
        pub fn visible(table: *const TreeTableInfo, snapshot: u64) bool {
            assert(table.address != 0);
            assert(table.snapshot_min <= table.snapshot_max);
            assert(snapshot <= snapshot_latest);

            return table.snapshot_min <= snapshot and snapshot <= table.snapshot_max;
        }

        pub fn invisible(table: *const TreeTableInfo, snapshots: []const u64) bool {
            // Return early and do not iterate all snapshots if the table was never deleted:
            if (table.visible(snapshot_latest)) return false;
            for (snapshots) |snapshot| if (table.visible(snapshot)) return false;
            assert(table.snapshot_max < math.maxInt(u64));
            return true;
        }

        pub fn equal(table: *const TreeTableInfo, other: *const TreeTableInfo) bool {
            // TODO Since the layout of TreeTableInfo is well defined, a direct memcmp may be faster
            // here. However, it's not clear if we can make the assumption that compare_keys()
            // will return .eq exactly when the memory of the keys are equal.
            // Consider defining the API to allow this.
            return table.checksum == other.checksum and
                table.address == other.address and
                table.snapshot_min == other.snapshot_min and
                table.snapshot_max == other.snapshot_max and
                compare_keys(table.key_min, other.key_min) == .eq and
                compare_keys(table.key_max, other.key_max) == .eq;
        }

        pub fn decode(table: *const TableInfo) TreeTableInfo {
            assert(stdx.zeroed(&table.reserved));

            const key_min = std.mem.bytesAsValue(Key, table.key_min[0..@sizeOf(Key)]);
            const key_max = std.mem.bytesAsValue(Key, table.key_max[0..@sizeOf(Key)]);

            assert(stdx.zeroed(table.key_min[@sizeOf(Key)..]));
            assert(stdx.zeroed(table.key_max[@sizeOf(Key)..]));

            return .{
                .checksum = table.checksum,
                .address = table.address,
                .snapshot_min = table.snapshot_min,
                .snapshot_max = table.snapshot_max,
                .key_min = key_min.*,
                .key_max = key_max.*,
            };
        }

        pub fn encode(table: *const TreeTableInfo, tree_id: u16) TableInfo {
            var key_min = std.mem.zeroes(TableInfo.KeyPadded);
            var key_max = std.mem.zeroes(TableInfo.KeyPadded);

            stdx.copy_disjoint(.inexact, u8, &key_min, std.mem.asBytes(&table.key_min));
            stdx.copy_disjoint(.inexact, u8, &key_max, std.mem.asBytes(&table.key_max));

            return .{
                .checksum = table.checksum,
                .address = table.address,
                .snapshot_min = table.snapshot_min,
                .snapshot_max = table.snapshot_max,
                .tree_id = tree_id,
                .key_min = key_min,
                .key_max = key_max,
            };
        }
    };
}

pub fn ManifestType(comptime Table: type, comptime Storage: type) type {
    const Key = Table.Key;
    const compare_keys = Table.compare_keys;

    return struct {
        const Manifest = @This();

        pub const TreeTableInfo = TreeTableInfoType(Table);
        pub const LevelIterator = Level.Iterator;
        pub const TableInfoReference = Level.TableInfoReference;
        pub const KeyRange = Level.KeyRange;
        pub const ManifestLog = ManifestLogType(Storage);

        const Grid = GridType(Storage);
        const Callback = *const fn (*Manifest) void;

        const Level =
            ManifestLevelType(NodePool, Key, TreeTableInfo, compare_keys, table_count_max);

        const CompactionTableRange = struct {
            table_a: TableInfoReference,
            range_b: CompactionRange,
        };

        pub const CompactionRange = struct {
            /// The minimum key across both levels.
            key_min: Key,
            /// The maximum key across both levels.
            key_max: Key,
            // References to tables in level B that intersect with the chosen table in level A.
            tables: stdx.BoundedArray(TableInfoReference, constants.lsm_growth_factor),
        };

        node_pool: *NodePool,
        config: TreeConfig,

        levels: [constants.lsm_levels]Level,

        // TODO Set this at startup when reading in the manifest.
        // This should be the greatest TableInfo.snapshot_min/snapshot_max (if deleted) or
        // registered snapshot seen so far.
        snapshot_max: u64 = 1,

        manifest_log: ?*ManifestLog = null,

        open_callback: ?Callback = null,
        compact_callback: ?Callback = null,
        checkpoint_callback: ?Callback = null,

        pub fn init(allocator: mem.Allocator, node_pool: *NodePool, config: TreeConfig) !Manifest {
            var levels: [constants.lsm_levels]Level = undefined;
            for (&levels, 0..) |*level, i| {
                errdefer for (levels[0..i]) |*l| l.deinit(allocator, node_pool);
                level.* = try Level.init(allocator);
            }
            errdefer for (&levels) |*level| level.deinit(allocator, node_pool);

            return Manifest{
                .node_pool = node_pool,
                .config = config,
                .levels = levels,
            };
        }

        pub fn deinit(manifest: *Manifest, allocator: mem.Allocator) void {
            for (&manifest.levels) |*level| level.deinit(allocator, manifest.node_pool);
        }

        pub fn reset(manifest: *Manifest) void {
            for (&manifest.levels) |*level| level.reset();

            manifest.* = .{
                .node_pool = manifest.node_pool,
                .config = manifest.config,
                .levels = manifest.levels,
            };
        }

        pub fn open_commence(manifest: *Manifest, manifest_log: *ManifestLog) void {
            assert(manifest.manifest_log == null);
            assert(!manifest_log.opened);

            manifest.manifest_log = manifest_log;
        }

        pub fn insert_table(
            manifest: *Manifest,
            level: u8,
            table: *const TreeTableInfo,
        ) void {
            const manifest_level = &manifest.levels[level];
            if (constants.verify) {
                assert(!manifest_level.contains(table));
            }

            manifest_level.insert_table(manifest.node_pool, table);

            // Append insert changes to the manifest log.
            const log_level = @as(u7, @intCast(level));
            manifest.manifest_log.?.insert(log_level, &table.encode(manifest.config.id));

            if (constants.verify) {
                assert(manifest_level.contains(table));
            }
        }

        /// Updates the snapshot_max on the provided table for the given level.
        pub fn update_table(
            manifest: *Manifest,
            level: u8,
            snapshot: u64,
            table_ref: TableInfoReference,
        ) void {
            assert(manifest.manifest_log.?.opened);
            const manifest_level = &manifest.levels[level];

            var table = table_ref.table_info;
            if (constants.verify) {
                assert(manifest_level.contains(table));
            }
            assert(table.snapshot_max >= snapshot);
            manifest_level.set_snapshot_max(snapshot, table_ref);
            assert(table.snapshot_max == snapshot);

            // Append update changes to the manifest log.
            const log_level = @as(u7, @intCast(level));
            manifest.manifest_log.?.insert(log_level, &table.encode(manifest.config.id));
        }

        pub fn move_table(
            manifest: *Manifest,
            level_a: u8,
            level_b: u8,
            table: *const TreeTableInfo,
        ) void {
            assert(manifest.manifest_log.?.opened);
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
            manifest.manifest_log.?.insert(
                @as(u7, @intCast(level_b)),
                &table.encode(manifest.config.id),
            );

            if (constants.verify) {
                assert(!manifest_level_a.contains(table));
                assert(manifest_level_b.contains(table));
            }
        }

        /// Returns the key range spanned by all ManifestLevels.
        pub fn key_range(manifest: *Manifest) ?KeyRange {
            assert(manifest.manifest_log.?.opened);

            var manifest_range: ?KeyRange = null;
            for (&manifest.levels) |*level| {
                if (level.key_range.key_range) |level_range| {
                    if (manifest_range) |*range| {
                        if (compare_keys(level_range.key_min, range.key_min) == .lt) {
                            range.key_min = level_range.key_min;
                        }
                        if (compare_keys(level_range.key_max, range.key_max) == .gt) {
                            range.key_max = level_range.key_max;
                        }
                    } else {
                        manifest_range = level_range;
                    }
                }
            }
            return manifest_range;
        }

        pub fn remove_invisible_tables(
            manifest: *Manifest,
            level: u8,
            snapshot: u64,
            key_min: Key,
            key_max: Key,
        ) void {
            assert(manifest.manifest_log.?.opened);
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
                manifest.manifest_log.?.remove(
                    @as(u7, @intCast(level)),
                    &table.encode(manifest.config.id),
                );
                manifest_level.remove_table_invisible(manifest.node_pool, &snapshots, table);
            }

            if (constants.verify) manifest.assert_no_invisible_tables_at_level(level, snapshot);
        }

        /// Returns an iterator over the tables visible to `snapshot` that may contain `key`
        /// (but are not guaranteed to), across all levels > `level_min`.
        pub fn lookup(manifest: *Manifest, snapshot: u64, key: Key, level_min: u8) LookupIterator {
            return .{
                .manifest = manifest,
                .snapshot = snapshot,
                .key = key,
                .level = level_min,
            };
        }

        pub const LookupIterator = struct {
            manifest: *const Manifest,
            snapshot: u64,
            key: Key,
            level: u8,
            inner: ?Level.Iterator = null,

            pub fn next(it: *LookupIterator) ?*const TreeTableInfo {
                while (it.level < constants.lsm_levels) : (it.level += 1) {
                    const level = &it.manifest.levels[it.level];
                    if (!level.key_range_contains(it.snapshot, it.key)) continue;

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
            for (&manifest.levels, 0..) |*manifest_level, index| {
                const level = @as(u8, @intCast(index));
                const table_count_visible_max = table_count_max_for_level(growth_factor, level);
                assert(manifest_level.table_count_visible <= table_count_visible_max);
            }
        }

        pub fn assert_no_invisible_tables(manifest: *const Manifest, snapshot: u64) void {
            for (manifest.levels, 0..) |_, level| {
                manifest.assert_no_invisible_tables_at_level(@as(u8, @intCast(level)), snapshot);
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
        pub fn next_table(manifest: *const Manifest, parameters: struct {
            level: u8,
            snapshot: u64,
            key_min: Key,
            key_max: Key,
            key_exclusive: ?Key,
            direction: Direction,
        }) ?*const TreeTableInfo {
            assert(parameters.level < constants.lsm_levels);
            assert(compare_keys(parameters.key_min, parameters.key_max) != .gt);
            return manifest.levels[parameters.level].next_table(.{
                .snapshot = parameters.snapshot,
                .key_min = parameters.key_min,
                .key_max = parameters.key_max,
                .key_exclusive = parameters.key_exclusive,
                .direction = parameters.direction,
            });
        }

        /// Returns the most optimal table from a level that is due for compaction.
        /// The optimal compaction table is one that overlaps with the least number
        /// of tables in the next level.
        /// Returns null if the level is not due for compaction (table_count_visible < count_max).
        pub fn compaction_table(manifest: *const Manifest, level_a: u8) ?CompactionTableRange {
            // The last level is not compacted into another.
            assert(level_a < constants.lsm_levels - 1);

            const table_count_visible_max = table_count_max_for_level(growth_factor, level_a);
            assert(table_count_visible_max > 0);

            const manifest_level_a: *const Level = &manifest.levels[level_a];
            const manifest_level_b: *const Level = &manifest.levels[level_a + 1];
            if (manifest_level_a.table_count_visible < table_count_visible_max) return null;
            // If even levels are compacted ahead of odd levels, then odd levels may burst.
            assert(manifest_level_a.table_count_visible <= table_count_visible_max + 1);

            const least_overlap_table = manifest_level_a.table_with_least_overlap(
                manifest_level_b,
                snapshot_latest,
                growth_factor,
            ) orelse return null;

            const compaction_table_range = CompactionTableRange{
                .table_a = least_overlap_table.table,
                .range_b = CompactionRange{
                    .key_min = least_overlap_table.range.key_min,
                    .key_max = least_overlap_table.range.key_max,
                    .tables = least_overlap_table.range.tables,
                },
            };
            return compaction_table_range;
        }

        /// Returns the smallest visible range of tables across the immutable table
        /// and Level 0 that overlaps with the given key range: [key_min, key_max].
        pub fn immutable_table_compaction_range(
            manifest: *const Manifest,
            key_min: Key,
            key_max: Key,
        ) CompactionRange {
            assert(compare_keys(key_min, key_max) != .gt);
            const level_b = 0;
            const manifest_level: *const Level = &manifest.levels[level_b];

            // We are guaranteed to get a non-null range because Level 0 has
            // lsm_growth_factor number of tables, so the number of tables that intersect
            // with the immutable table can be no more than lsm_growth_factor.
            const range = manifest_level.tables_overlapping_with_key_range(
                key_min,
                key_max,
                snapshot_latest,
                growth_factor,
            ).?;

            assert(compare_keys(range.key_min, range.key_max) != .gt);
            assert(compare_keys(range.key_min, key_min) != .gt);
            assert(compare_keys(range.key_max, key_max) != .lt);

            return .{
                .key_min = range.key_min,
                .key_max = range.key_max,
                .tables = range.tables,
            };
        }

        /// If no subsequent levels have any overlap, then tombstones must be dropped.
        pub fn compaction_must_drop_tombstones(
            manifest: *const Manifest,
            level_b: u8,
            range: CompactionRange,
        ) bool {
            assert(level_b < constants.lsm_levels);
            assert(compare_keys(range.key_min, range.key_max) != .gt);

            var level_c: u8 = level_b + 1;
            while (level_c < constants.lsm_levels) : (level_c += 1) {
                const manifest_level: *const Level = &manifest.levels[level_c];
                if (manifest_level.next_table(.{
                    .snapshot = snapshot_latest,
                    .direction = .ascending,
                    .key_min = range.key_min,
                    .key_max = range.key_max,
                    .key_exclusive = null,
                }) != null) {
                    // If the range is being compacted into the last level then this is unreachable,
                    // as the last level has no subsequent levels and must always drop tombstones.
                    assert(level_b != constants.lsm_levels - 1);
                    return false;
                }
            }

            assert(level_c == constants.lsm_levels);
            return true;
        }

        pub fn verify(manifest: *Manifest, snapshot: u64) void {
            assert(snapshot <= snapshot_latest);

            // TODO: When state sync is proactive, re-enable this.
            if (true) return;

            // TODO s/prev/previous; s/iter/iterator.
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
                        manifest.manifest_log.?.grid.superblock.storage,
                        table_info.address,
                        table_info.key_min,
                        table_info.key_max,
                    );
                }
            }
        }
    };
}
