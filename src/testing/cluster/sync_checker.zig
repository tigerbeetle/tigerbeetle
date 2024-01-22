//! Verification related to state sync.
//!
//! Check:
//! - the number of simultaneously state-syncing replicas.
//! - all replicas encounter the same checkpoints (no divergence)
const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.sync_checker);

const constants = @import("../../constants.zig");
const vsr = @import("../../vsr.zig");

pub fn SyncCheckerType(comptime Replica: type) type {
    return struct {
        const SyncChecker = @This();

        /// A list of checkpoint ids reached by the cluster.
        /// Indexed by checkpoint_index(checkpoint_op).
        checkpoint_ids: std.ArrayList(u128),

        replicas_syncing: std.StaticBitSet(constants.members_max) =
            std.StaticBitSet(constants.members_max).initEmpty(),

        pub fn init(allocator: std.mem.Allocator) SyncChecker {
            var checkpoint_ids = std.ArrayList(u128).init(allocator);
            errdefer checkpoint_ids.deinit();

            return SyncChecker{
                .checkpoint_ids = checkpoint_ids,
            };
        }

        pub fn deinit(checker: *SyncChecker) void {
            checker.checkpoint_ids.deinit();
        }

        /// Verify that the number of simultaneous syncing (non-standby) replicas does not exceed
        /// the safe limit.
        pub fn replica_sync_start(
            checker: *SyncChecker,
            replica: *const Replica,
        ) void {
            if (replica.standby()) return;
            checker.replicas_syncing.set(replica.replica);

            // This implicitly checks that R=1 and R=2 clusters never state sync.
            // Don't count standbys since they aren't part of the replication quorum.
            const quorums = vsr.quorums(replica.replica_count);
            const replicas_syncing_max = replica.replica_count - quorums.replication;

            assert(checker.replicas_syncing.count() <= replicas_syncing_max);
        }

        pub fn replica_sync_done(
            checker: *SyncChecker,
            replica: *const Replica,
        ) void {
            if (replica.standby()) return;
            assert(checker.replicas_syncing.isSet(replica.replica));

            checker.replicas_syncing.unset(replica.replica);
        }

        pub fn check_sync_stage(checker: *SyncChecker, replica: *const Replica) void {
            if (replica.sync_target_max) |*sync_target| {
                checker.check_sync_target(.{
                    .replica = replica.replica,
                    .checkpoint_op = sync_target.checkpoint_op,
                    .checkpoint_id = sync_target.checkpoint_id,
                });
            }

            if (replica.syncing.target()) |sync_target| {
                checker.check_sync_target(.{
                    .replica = replica.replica,
                    .checkpoint_op = sync_target.checkpoint_op,
                    .checkpoint_id = sync_target.checkpoint_id,
                });
            }

            if (replica.op_checkpoint() != 0) {
                checker.check_sync_target(.{
                    .replica = replica.replica,
                    .checkpoint_op = replica.op_checkpoint(),
                    .checkpoint_id = replica.superblock.working.checkpoint_id(),
                });
            }
        }

        fn check_sync_target(checker: *SyncChecker, sync_target: struct {
            replica: u8,
            checkpoint_op: u64,
            checkpoint_id: u128,
        }) void {
            assert(sync_target.checkpoint_op > 0);

            const index = checkpoint_index(sync_target.checkpoint_op);
            if (index < checker.checkpoint_ids.items.len) {
                if (checker.checkpoint_ids.items[index] != sync_target.checkpoint_id) {
                    log.err("{}: check_sync_target: mismatch (op={} got={x:0>32} want={x:0>32})", .{
                        sync_target.replica,
                        sync_target.checkpoint_op,
                        sync_target.checkpoint_id,
                        checker.checkpoint_ids.items[index],
                    });
                }
                assert(checker.checkpoint_ids.items[index] == sync_target.checkpoint_id);
            } else {
                assert(checker.checkpoint_ids.items.len == index);
                checker.checkpoint_ids.append(sync_target.checkpoint_id) catch unreachable;

                log.debug("{}: check_sync_target: discovered checkpoint " ++
                    "(op={} id={x:0>32})", .{
                    sync_target.replica,
                    sync_target.checkpoint_op,
                    sync_target.checkpoint_id,
                });
            }
        }
    };
}

fn checkpoint_index(checkpoint_op: u64) usize {
    assert(checkpoint_op != 0);

    return @divExact(
        checkpoint_op + 1,
        constants.vsr_checkpoint_interval,
    ) - 1;
}
