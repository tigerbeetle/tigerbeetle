const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const config = @import("../config.zig");
const table_count_max = @import("tree.zig").table_count_max;

const ManifestLevel = @import("manifest_level.zig").ManifestLevel;
const NodePool = @import("node_pool.zig").NodePool(config.lsm_manifest_node_size, 16);
const SegmentedArray = @import("segmented_array.zig").SegmentedArray;

pub fn ManifestType(
    comptime Key: type,
    comptime Value: type,
    /// Returns the sort order between two keys.
    comptime compare_keys: fn (Key, Key) callconv(.Inline) math.Order,
) type {
    return struct {
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
                assert(@sizeOf(TableInfo) == 48 + key_size * 2);
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

            pub fn visible_by_any(table: *const TableInfo, snapshots: []const u64) bool {
                for (snapshots) |snapshot| {
                    if (table.visible(snapshot)) return true;
                }
                return table.visible(snapshot_latest);
            }

            pub fn eql(table: *const TableInfo, other: *const TableInfo) bool {
                // TODO since the layout of TableInfo is well defined, a direct memcmp might
                // be faster here. However, it's not clear if we can make the assumption that
                // compare_keys() will return .eq exactly when the memory of the keys are
                // equal. Consider defining the API to allow this and check the generated code.
                return table.checksum == other.checksum and
                    table.address == other.address and
                    table.flags == other.flags and
                    table.snapshot_min == other.snapshot_min and
                    table.snapshot_max == other.snapshot_max and
                    compare_keys(table.key_min, other.key_min) == .eq and
                    compare_keys(table.key_max, other.key_max) == .eq;
            }
        };

        /// Level 0 is special since tables can overlap the same key range.
        /// Here, we simply store tables in reverse order of precedence (i.e. newest first).
        const Conjoint = SegmentedArray(TableInfo, NodePool, table_count_max);

        /// Levels beyond level 0 have tables with disjoint key ranges.
        /// Here, we use a structure with indexes over the segmented array for performance.
        const Disjoint = ManifestLevel(NodePool, Key, TableInfo, compare_keys, table_count_max);

        const Level = union(LevelTag) {
            conjoint: Conjoint,
            disjoint: Disjoint,
        };

        const LevelTag = enum {
            conjoint,
            disjoint,
        };

        node_pool: *NodePool,

        levels: [config.lsm_levels]Level,

        // TODO Set this at startup when reading in the manifest.
        // This should be the greatest TableInfo.snapshot_min/snapshot_max (if deleted) or
        // registered snapshot seen so far.
        snapshot_max: u64 = 1,

        pub fn init(allocator: mem.Allocator, node_pool: *NodePool) !Manifest {
            var levels: [config.lsm_levels]Level = undefined;

            levels[0] = .{ .conjoint = try Conjoint.init(allocator) };
            errdefer levels[0].conjoint.deinit(allocator, node_pool);

            for (levels[1..]) |*level, i| {
                errdefer for (levels[1..i]) |*l| l.disjoint.deinit(allocator, node_pool);

                level.* = .{ .disjoint = try Disjoint.init(allocator) };
            }
            errdefer for (levels[1..]) |*l| l.disjoint.deinit(allocator, node_pool);

            return Manifest{
                .node_pool = node_pool,
                .levels = levels,
            };
        }

        pub fn deinit(manifest: *Manifest, allocator: mem.Allocator) void {
            manifest.levels[0].conjoint.deinit(allocator, manifest.node_pool);
            for (manifest.levels[1..]) |*l| l.disjoint.deinit(allocator, manifest.node_pool);
        }

        pub const LookupIterator = struct {
            manifest: *Manifest,
            snapshot: u64,
            key: Key,
            level: u8 = 0,
            inner: ?Conjoint.Iterator = null,
            precedence: ?u64 = null,

            pub fn next(it: *LookupIterator) ?*const TableInfo {
                while (it.level < config.lsm_levels) : (it.level += 1) {
                    switch (it.manifest.levels[it.level]) {
                        .conjoint => |*level| {
                            assert(it.level == 0);

                            if (it.inner == null) it.inner = level.iterator(0, 0, .ascending);
                            while (it.inner.?.next()) |table| {
                                if (it.precedence) |p| assert(p > table.snapshot_min);
                                it.precedence = table.snapshot_min;

                                if (!table.visible(it.snapshot)) continue;
                                if (compare_keys(it.key, table.key_min) == .lt) continue;
                                if (compare_keys(it.key, table.key_max) == .gt) continue;

                                return table;
                            }
                            assert(it.inner.?.done);
                            it.inner = null;
                        },
                        .disjoint => |*level| {
                            assert(it.level > 0);

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
                        },
                    }
                }

                assert(it.level == config.lsm_levels);
                return null;
            }
        };

        pub fn insert_tables(manifest: *Manifest, level: u8, tables: []const TableInfo) void {
            assert(tables.len > 0);

            switch (manifest.levels[level]) {
                .conjoint => |*segmented_array| {
                    assert(level == 0);

                    // We only expect to flush one table at a time:
                    // We would also otherwise need to think through precedence order.
                    assert(tables.len == 1);

                    const absolute_index = 0;
                    segmented_array.insert_elements(manifest.node_pool, absolute_index, tables);
                },
                .disjoint => |*manifest_level| {
                    assert(level > 0);

                    manifest_level.insert_tables(manifest.node_pool, tables);
                },
            }

            // TODO Verify that tables can be found exactly before returning.
        }

        pub fn lookup(manifest: *Manifest, snapshot: u64, key: Key) LookupIterator {
            return .{
                .manifest = manifest,
                .snapshot = snapshot,
                .key = key,
            };
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