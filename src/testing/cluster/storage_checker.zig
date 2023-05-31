//! Verify deterministic storage.
//!
//! At each replica compact and checkpoint, check that storage is byte-for-byte identical across
//! replicas.
//!
//! Areas verified at compaction (half-measure):
//! - Acquired Grid blocks (ignores skipped recovery compactions)
//!  TODO Because ManifestLog acquires blocks potentially several beats prior to actually writing
//!  the block, this check will need to be removed or use a different strategy.
//!
//! Areas verified at checkpoint:
//! - SuperBlock Manifest, FreeSet, ClientSessions
//! - ClientReplies
//! - Acquired Grid blocks
//!
//! Areas not verified:
//! - SuperBlock headers, which hold replica-specific state.
//! - WAL headers, which may differ because the WAL writes deliberately corrupt redundant headers
//!   to faulty slots to ensure recovery is consistent.
//! - WAL prepares — a replica can commit + checkpoint an op before it is persisted to the WAL.
//!   (The primary can commit from the pipeline-queue, backups can commit from the pipeline-cache.)
//! - Non-allocated Grid blocks, which may differ due to state sync.
const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.storage_checker);

const constants = @import("../../constants.zig");
const vsr = @import("../../vsr.zig");
const superblock = @import("../../vsr/superblock.zig");
const SuperBlockHeader = superblock.SuperBlockHeader;
const Storage = @import("../storage.zig").Storage;

/// After each compaction half measure, save the cumulative hash of all acquired grid blocks.
///
/// (Track half-measures instead of beats because the on-disk state mid-compaction is
/// nondeterministic; it depends on IO progress.)
const Compactions = std.ArrayList(u128);

/// Maps from op_checkpoint to cumulative storage checksum.
///
/// Not every checkpoint is necessarily recorded — a replica calls on_checkpoint *at most* once.
/// For example, a replica will not call on_checkpoint if it crashes (during a checkpoint) after
/// writing 2 superblock copies. (This could be repeated by other replicas, causing a checkpoint
/// op to be skipped in Checkpoints).
const Checkpoints = std.AutoHashMap(u64, Checkpoint);

const Checkpoint = struct {
    // The superblock trailers are an XOR of all copies of all respective trailers, not the
    // `SuperBlockHeader.{trailer}_checksum`.
    checksum_superblock_manifest: u128,
    checksum_superblock_free_set: u128,
    checksum_superblock_client_sessions: u128,
    checksum_client_replies: u128,
    checksum_grid: u128,
};

pub fn StorageCheckerType(comptime Replica: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        compactions: Compactions,
        checkpoints: Checkpoints,

        pub fn init(allocator: std.mem.Allocator) Self {
            var compactions = Compactions.init(allocator);
            errdefer compactions.deinit();

            var checkpoints = Checkpoints.init(allocator);
            errdefer checkpoints.deinit();

            return Self{
                .allocator = allocator,
                .compactions = compactions,
                .checkpoints = checkpoints,
            };
        }

        pub fn deinit(checker: *Self) void {
            checker.compactions.deinit();
            checker.checkpoints.deinit();
        }

        pub fn replica_compact(checker: *Self, replica: *const Replica) !void {
            // If we are recovering from a crash, don't test the checksum until we are caught up.
            // Until then our grid's checksum is too far ahead.
            if (replica.superblock.working.vsr_state.op_compacted(replica.commit_min)) return;

            const half_measure_beat_count = @divExact(constants.lsm_batch_multiple, 2);
            if ((replica.commit_min + 1) % half_measure_beat_count != 0) return;

            // TODO(Unified Manifest) The issue is:
            // 1. Open manifest log blocks are acquired from the freeset but not written yet.
            // 2. We can't defer acquiring manifest log block addresses until close-time.
            //    This is because `append()` needs the block address in order to update the
            //    superblock manifest table extents.
            // 3. We can't defer the superblock manifest table extent updates because then they
            //    would be out of order with respect to the table extent *removes* performed
            //    during manifest compaction.
            // Hopefully with a unified manifest another approach will open up.
            if (1 == 1) return;

            const checksum = checksum_grid(replica);
            log.debug("{}: replica_compact: op={} area=grid checksum={x:0>32}", .{
                replica.replica,
                replica.commit_min,
                checksum,
            });

            // -1 since we never compact op=1.
            const compactions_index = @divExact(replica.commit_min + 1, half_measure_beat_count) - 1;
            if (compactions_index == checker.compactions.items.len) {
                try checker.compactions.append(checksum);
            } else {
                const checksum_expect = checker.compactions.items[compactions_index];
                if (checksum_expect != checksum) {
                    log.err("{}: replica_compact: mismatch area=grid expect={x:0>32} actual={x:0>32}", .{
                        replica.replica,
                        checksum_expect,
                        checksum,
                    });
                    return error.StorageMismatch;
                }
            }
        }

        pub fn replica_checkpoint(checker: *Self, replica: *const Replica) !void {
            const storage = replica.superblock.storage;
            const working = replica.superblock.working;

            var checkpoint = Checkpoint{
                .checksum_superblock_manifest = 0,
                .checksum_superblock_free_set = 0,
                .checksum_superblock_client_sessions = 0,
                // TODO State sync: Enable this when proactive client replies sync is implemented.
                // .checksum_client_replies = checksum_client_replies(storage),
                .checksum_client_replies = 0,
                // TODO: State sync: Enable this (again) when proactive table sync is implemented.
                // .checksum_grid = checksum_grid(replica),
                .checksum_grid = 0,
            };

            inline for (.{ .manifest, .free_set, .client_sessions }) |trailer| {
                const trailer_area = @field(superblock.areas, @tagName(trailer));
                const trailer_size = @field(working, @tagName(trailer) ++ "_size");
                var copy: u8 = 0;
                while (copy < constants.superblock_copies) : (copy += 1) {
                    @field(checkpoint, "checksum_superblock_" ++ @tagName(trailer)) |=
                        vsr.checksum(storage.memory[trailer_area.offset(copy)..][0..trailer_size]);
                }
            }

            inline for (std.meta.fields(Checkpoint)) |field| {
                log.debug("{}: replica_checkpoint: checkpoint={} area={s} value={x:0>32}", .{
                    replica.replica,
                    replica.op_checkpoint(),
                    field.name,
                    @field(checkpoint, field.name),
                });
            }

            const checkpoint_expect = checker.checkpoints.get(replica.op_checkpoint()) orelse {
                // This replica is the first to reach op_checkpoint.
                try checker.checkpoints.putNoClobber(replica.op_checkpoint(), checkpoint);
                return;
            };

            var fail: bool = false;
            inline for (std.meta.fields(Checkpoint)) |field| {
                const field_actual = @field(checkpoint, field.name);
                const field_expect = @field(checkpoint_expect, field.name);
                if (!std.meta.eql(field_expect, field_actual)) {
                    fail = true;
                    log.err("{}: replica_checkpoint: mismatch area={s} expect={x:0>32} actual={x:0>32}", .{
                        replica.replica,
                        field.name,
                        @field(checkpoint_expect, field.name),
                        @field(checkpoint, field.name),
                    });
                }
            }
            if (fail) return error.StorageMismatch;
        }

        fn checksum_client_replies(storage: *const Storage) u128 {
            const offset = vsr.Zone.client_replies.offset(0);
            const size = constants.clients_max * constants.message_size_max;
            return vsr.checksum(storage.memory[offset..][0..size]);
        }

        fn checksum_grid(replica: *const Replica) u128 {
            const storage = replica.superblock.storage;
            var acquired = replica.superblock.free_set.blocks.iterator(.{});
            var checksum: u128 = 0;
            while (acquired.next()) |address_index| {
                const block = storage.grid_block(address_index + 1);
                const block_header = std.mem.bytesToValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);
                checksum ^= vsr.checksum(block[0..block_header.size]);
            }
            return checksum;
        }
    };
}
