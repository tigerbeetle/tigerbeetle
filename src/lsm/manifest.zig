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
const ManifestLevel = @import("manifest_level.zig").ManifestLevel;
const NodePool = @import("node_pool.zig").NodePool(config.lsm_manifest_node_size, 16);
const SegmentedArray = @import("segmented_array.zig").SegmentedArray;

pub fn ManifestType(comptime Table: type) type {
    const Key = Table.Key;
    const compare_keys = Table.compare_keys;

    return struct {
        const Manifest = @This();

        pub const TableInfo = extern struct {
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

        /// Levels beyond level 0 have tables with disjoint key ranges.
        /// Here, we use a structure with indexes over the segmented array for performance.
        const Level = ManifestLevel(NodePool, Key, TableInfo, compare_keys, table_count_max);

        node_pool: *NodePool,

        levels: [config.lsm_levels]Level,

        // TODO Set this at startup when reading in the manifest.
        // This should be the greatest TableInfo.snapshot_min/snapshot_max (if deleted) or
        // registered snapshot seen so far.
        snapshot_max: u64 = 1,

        pub fn init(allocator: mem.Allocator, node_pool: *NodePool) !Manifest {
            var levels: [config.lsm_levels]Level = undefined;

            for (levels) |*level, i| {
                errdefer for (levels[0..i]) |*l| l.deinit(allocator, node_pool);
                level.* = try Level.init(allocator);
            }

            return Manifest{
                .node_pool = node_pool,
                .levels = levels,
            };
        }

        pub fn deinit(manifest: *Manifest, allocator: mem.Allocator) void {
            for (manifest.levels) |*l| l.deinit(allocator, manifest.node_pool);
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

                    var inner = level.iterator(it.snapshot, it.key, it.key, .ascending);
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

        pub fn insert_tables(
            manifest: *Manifest,
            level: u8,
            tables: []const TableInfo,
        ) void {
            assert(tables.len > 0);

            const manifest_level = &manifest.levels[level];
            manifest_level.insert_tables(manifest.node_pool, tables);

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
        }

        pub fn lookup(manifest: *Manifest, snapshot: u64, key: Key) LookupIterator {
            return .{
                .manifest = manifest,
                .snapshot = snapshot,
                .key = key,
            };
        }

        pub fn assert_visible_tables_are_in_range(manifest: *const Manifest) void {
            for (manifest.levels) |*manifest_level, index| {
                const level = @intCast(u8, index);
                const table_count_visible_max = table_count_max_for_level(growth_factor, level);
                assert(manifest_level.table_count_visible <= table_count_visible_max);
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

            if (key_exclusive == null) {
                return manifest.levels[level].iterator(
                    snapshot,
                    key_min,
                    key_max,
                    direction,
                ).next();
            }

            assert(compare_keys(key_exclusive.?, key_min) != .lt);
            assert(compare_keys(key_exclusive.?, key_max) != .gt);

            const key_min_exclusive = if (direction == .ascending) key_exclusive.? else key_min;
            const key_max_exclusive = if (direction == .descending) key_exclusive.? else key_max;
            assert(compare_keys(key_min_exclusive, key_max_exclusive) != .gt);

            var it = manifest.levels[level].iterator(
                snapshot,
                key_min_exclusive,
                key_max_exclusive,
                direction,
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
            var it = manifest.levels[level_a].iterator_visibility(.visible, &snapshots);
            var iterations: usize = 0;

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

            var it = manifest.levels[level_b].iterator(
                snapshot_latest,
                range.key_min,
                range.key_max,
                .ascending,
            );

            while (it.next()) |table| : (range.table_count += 1) {
                assert(table.visible(snapshot_latest));
                assert(compare_keys(table.key_min, table.key_max) != .gt);
                assert(compare_keys(table.key_max, range.key_min) != .lt);
                assert(compare_keys(table.key_min, range.key_max) != .gt);

                assert(range.table_count > 0);
                if (range.table_count == 1) {
                    // The first iterated table.key_min/max may overlap range.key_min/max entirely.
                    if (compare_keys(table.key_min, range.key_min) == .lt) {
                        range.key_min = table.key_min;
                    }
                    if (compare_keys(table.key_max, range.key_max) == .gt) {
                        range.key_max = table.key_max;
                    }
                } else {
                    // Thereafter, all iterated tables must extend the range in ascending order.
                    assert(compare_keys(table.key_min, range.key_max) == .gt);
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
                var it = manifest.levels[level_c].iterator(
                    snapshot_latest,
                    range.key_min,
                    range.key_max,
                    .ascending,
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

        /// Returns a unique snapshot, incrementing the greatest snapshot value seen so far,
        /// whether this was for a TableInfo.snapshot_min/snapshot_max or registered snapshot.
        pub fn take_snapshot(manifest: *Manifest) u64 {
            // A snapshot cannot be 0 as this is a reserved value in the superblock.
            assert(manifest.snapshot_max > 0);
            // The constant snapshot_latest must compare greater than any issued snapshot.
            // This also ensures that we are not about to overflow the u64 counter.
            assert(manifest.snapshot_max < snapshot_latest - 1);

            manifest.snapshot_max += 1;

            return manifest.snapshot_max;
        }
    };
}
