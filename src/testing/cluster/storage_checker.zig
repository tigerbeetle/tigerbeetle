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
//! - SuperBlock trailers (Manifest, FreeSet, ClientSessions)
//! - ClientReplies (when repair finishes)
//! - Acquired Grid blocks (when syncing finishes)
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
const schema = @import("../../lsm/schema.zig");
const BlockType = @import("../../lsm/grid.zig").BlockType;
const TestStorage = @import("../storage.zig").Storage;

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

const CheckpointCheck = enum {
    superblock_manifest,
    superblock_free_set,
    superblock_client_sessions,
    client_replies,
    grid,
};

const Checkpoint = std.enums.EnumArray(CheckpointCheck, u128);

pub fn StorageCheckerType(comptime Storage: type) type {
    return struct {
        const Self = @This();
        const SuperBlock = vsr.SuperBlockType(Storage);

        compactions: Compactions,
        checkpoints: Checkpoints,

        free_set: SuperBlock.FreeSet,

        pub fn init(allocator: std.mem.Allocator) !Self {
            var compactions = Compactions.init(allocator);
            errdefer compactions.deinit();

            var checkpoints = Checkpoints.init(allocator);
            errdefer checkpoints.deinit();

            var free_set = try SuperBlock.FreeSet.init(allocator, vsr.superblock.grid_blocks_max);
            errdefer free_set.deinit(allocator);

            return Self{
                .compactions = compactions,
                .checkpoints = checkpoints,
                .free_set = free_set,
            };
        }

        pub fn deinit(checker: *Self, allocator: std.mem.Allocator) void {
            checker.free_set.deinit(allocator);
            checker.checkpoints.deinit();
            checker.compactions.deinit();
        }

        pub fn check_table(storage: *const Storage, index_address: u64, index_checksum: u128) void {
            if (Storage != TestStorage) return;

            assert(index_address > 0);

            const index_block = storage.grid_block(index_address).?;
            const index_block_header = schema.header_from_block(index_block);
            assert(index_block_header.op == index_address);
            assert(index_block_header.checksum == index_checksum);
            assert(BlockType.from(index_block_header.operation) == .index);

            const index_schema = schema.TableIndex.from(index_block);
            const content_blocks_used = index_schema.content_blocks_used(index_block);
            var content_block_index: usize = 0;
            while (content_block_index < content_blocks_used) : (content_block_index += 1) {
                const content_block_id =
                    index_schema.content_block(index_block, content_block_index);
                const content_block = storage.grid_block(content_block_id.block_address).?;
                const content_block_header = schema.header_from_block(content_block);

                assert(content_block_header.op == content_block_id.block_address);
                assert(content_block_header.checksum == content_block_id.block_checksum);
                assert(BlockType.from(content_block_header.operation) == .filter or
                    BlockType.from(content_block_header.operation) == .data);
            }
        }

        // TODO is this ever used?
        pub fn check_block(storage: *const Storage, address: u64, checksum: u128) void {
            if (Storage != TestStorage) return;

            assert(address > 0);

            const block = storage.grid_block(address).?;
            const block_header = schema.header_from_block(block);
            assert(block_header.op == address);
            assert(block_header.checksum == checksum);

            switch (BlockType.from(block_header.operation)) {
                .reserved => unreachable,
                .manifest => {},
                .table => storage.check_table(address, checksum),
                .data => {},
                .filter => {},
            }
        }

        pub fn replica_checkpoint(checker: *Self, superblock: *const SuperBlock) !void {
            const storage: *const Storage = superblock.storage;
            const syncing = superblock.working.vsr_state.commit_unsynced_max > 0;

            //std.debug.print("{}: syncing={}, commit_unsynced={},{}\n",.{ replica.replica, syncing,
            //    replica.superblock.working.vsr_state.commit_unsynced_min,
            //    replica.superblock.working.vsr_state.commit_unsynced_max,
            //});

            // TODO iterate superblock manifest, check tables not in snapshot range
            const replica_id = superblock.working.vsr_state.replica_id;
            const replica_checkpoint_op = superblock.working.vsr_state.commit_min;

            var checkpoint_actual = std.enums.EnumMap(CheckpointCheck, u128).init(.{
                .superblock_manifest = checksum_trailer(superblock, .manifest),
                .superblock_free_set = checksum_trailer(superblock, .free_set),
                .superblock_client_sessions = checksum_trailer(superblock, .client_sessions),
            });

            if (!syncing) {
                checkpoint_actual.put(.client_replies, checksum_client_replies(storage));
                checkpoint_actual.put(.grid, checker.checksum_grid(superblock));
            }

            for (std.enums.values(CheckpointCheck)) |check| {
                log.debug("{}: replica_checkpoint: checkpoint={} area={s} value={x:0>32}", .{
                    replica_id,
                    replica_checkpoint_op,
                    @tagName(check),
                    checkpoint_actual.get(check),
                });
            }

            const checkpoint_expect = checker.checkpoints.get(replica_checkpoint_op) orelse {
                if (!syncing) {
                    // This replica is the first to reach op_checkpoint.
                    // Save its state for other replicas to check themselves against.
                    var checkpoint: Checkpoint = undefined;
                    for (std.enums.values(CheckpointCheck)) |check| {
                        checkpoint.set(check, checkpoint_actual.get(check).?);
                    }
                    try checker.checkpoints.putNoClobber(replica_checkpoint_op, checkpoint);
                }
                return;
            };

            var mismatch: bool = false;
            for (std.enums.values(CheckpointCheck)) |check| {
                const checksum_actual = checkpoint_actual.get(check) orelse continue;
                const checksum_expect = checkpoint_expect.get(check);
                if (checksum_expect != checksum_actual) {
                    log.warn("{}: replica_checkpoint: mismatch " ++
                        "area={s} expect={x:0>32} actual={x:0>32}", .{
                        replica_id,
                        @tagName(check),
                        checksum_expect,
                        checksum_actual,
                    });

                    mismatch = true;
                }
            }
            if (mismatch) return error.StorageMismatch;
        }

        fn checksum_trailer(superblock: *const SuperBlock, trailer: vsr.SuperBlockTrailer) u128 {
            const trailer_size = superblock.working.trailer_size(trailer);
            const trailer_checksum = superblock.working.trailer_checksum(trailer);

            var copy: u8 = 0;
            while (copy < constants.superblock_copies) : (copy += 1) {
                const trailer_start = trailer.zone().start_for_copy(copy);
                //const trailer_checksum_computed =
                //    vsr.checksum(storage.memory[trailer_start..][0..trailer_size]);

                assert(trailer_checksum ==
                    vsr.checksum(superblock.storage.memory[trailer_start..][0..trailer_size]));
                //if (checkpoint_actual.get(check)) |checksum|
                //if (@field(checkpoint_actual, field)) |checksum| {
                //    assert(trailer_checksum == checksum);
                //} else {
                //    @field(checkpoint_actual, field) = trailer_checksum;
                //}
            }

            return trailer_checksum;
        }

        fn checksum_client_replies(storage: *const TestStorage) u128 {
            const offset = vsr.Zone.client_replies.offset(0);
            const size = constants.clients_max * constants.message_size_max;
            return vsr.checksum(storage.memory[offset..][0..size]);
        }

        fn checksum_grid(checker: *Self, superblock: *const SuperBlock) u128 {
            const free_set_zone = vsr.SuperBlockTrailer.free_set.zone();
            const free_set_offset = vsr.Zone.superblock.offset(free_set_zone.start_for_copy(0));
            const free_set_size = superblock.working.free_set_size;
            const free_set_buffer = superblock.storage.memory[free_set_offset..][0..free_set_size];

            // TODO assert checksum

            checker.free_set.decode(free_set_buffer);
            defer checker.free_set.reset();

            //const storage = replica.superblock.storage;
            //var acquired = replica.superblock.free_set.blocks.iterator(.{});
            var checksum: u128 = 0;
            var acquired = checker.free_set.blocks.iterator(.{});
            while (acquired.next()) |address_index| {
                const block = superblock.storage.grid_block(address_index + 1).?; // TODO "orelse ..."?
                const block_header =
                    std.mem.bytesToValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);
                checksum ^= vsr.checksum(block[0..block_header.size]);
            }
            return checksum;
        }
    };
}
