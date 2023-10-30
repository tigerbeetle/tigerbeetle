//! Verify that the ManifestLevels tables are constructed consistently across replicas and after
//! recovering from a restart.
const std = @import("std");
const assert = std.debug.assert;

const vsr = @import("../../vsr.zig");
const constants = @import("../../constants.zig");

pub fn ManifestCheckerType(comptime Forest: type) type {
    return struct {
        const ManifestChecker = @This();

        /// Maps checkpoint op to the cumulative checksum of all trees/levels checksum.
        const Checkpoints = std.AutoHashMap(u64, u128);

        checkpoints: Checkpoints,

        pub fn init(allocator: std.mem.Allocator) ManifestChecker {
            return .{ .checkpoints = Checkpoints.init(allocator) };
        }

        pub fn deinit(checker: *ManifestChecker) void {
            checker.checkpoints.deinit();
        }

        pub fn forest_open(checker: *ManifestChecker, forest: *const Forest) void {
            checker.check(forest);
        }

        pub fn forest_checkpoint(checker: *ManifestChecker, forest: *const Forest) void {
            checker.check(forest);
        }

        fn check(checker: *ManifestChecker, forest: *const Forest) void {
            assert(forest.grid.superblock.opened);
            assert(forest.manifest_log.opened);

            const checkpoint_op = forest.grid.superblock.working.vsr_state.checkpoint.commit_min;
            const checksum_stored = checker.checkpoints.getOrPut(checkpoint_op) catch @panic("oom");
            const checksum_current = manifest_levels_checksum(forest);

            // On open, we will usually have already have a checksum to compare against from a prior
            // checkpoint. But not always: it is possible that we are recovering from a checkpoint
            // that wrote e.g. 3/4 superblock copies and then crashed.
            if (checksum_stored.found_existing) {
                assert(checksum_stored.value_ptr.* == checksum_current);
            } else {
                checksum_stored.value_ptr.* = checksum_current;
            }
        }

        fn manifest_levels_checksum(forest: *const Forest) u128 {
            var checksum_stream = vsr.ChecksumStream.init();
            for (0..constants.lsm_levels) |level| {
                checksum_stream.add(std.mem.asBytes(&level));

                inline for (Forest.tree_id_range.min..Forest.tree_id_range.max + 1) |tree_id| {
                    const tree_level = forest.tree_for_id_const(tree_id).manifest.levels[level];
                    var tree_tables = tree_level.tables.iterator_from_index(0, .ascending);

                    checksum_stream.add(std.mem.asBytes(&tree_id));
                    checksum_stream.add(std.mem.asBytes(&tree_level.table_count_visible));
                    while (tree_tables.next()) |tree_table| {
                        checksum_stream.add(std.mem.asBytes(&tree_table.encode(.{
                            .tree_id = tree_id,
                            .event = .insert, // (Placeholder event).
                            .level = @intCast(level),
                        })));
                    }
                }
            }
            return checksum_stream.checksum();
        }
    };
}
