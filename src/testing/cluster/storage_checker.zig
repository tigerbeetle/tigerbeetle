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
//! - SuperBlock CheckpointState
//! - SuperBlock trailers (FreeSet, ClientSessions)
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
const stdx = @import("../../stdx.zig");
const vsr = @import("../../vsr.zig");
const schema = @import("../../lsm/schema.zig");
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

const CheckpointArea = enum {
    superblock_checkpoint,
    superblock_client_sessions,
    client_replies,
    grid,
};

const Checkpoint = std.enums.EnumMap(CheckpointArea, u128);

pub const StorageChecker = struct {
    const SuperBlock = vsr.SuperBlockType(Storage);

    compactions: Compactions,
    checkpoints: Checkpoints,

    free_set: SuperBlock.FreeSet,
    free_set_buffer: []align(@alignOf(u64)) u8,

    pub fn init(allocator: std.mem.Allocator) !StorageChecker {
        var compactions = Compactions.init(allocator);
        errdefer compactions.deinit();

        var checkpoints = Checkpoints.init(allocator);
        errdefer checkpoints.deinit();

        var free_set = try SuperBlock.FreeSet.init(allocator, vsr.superblock.grid_blocks_max);
        errdefer free_set.deinit(allocator);

        var free_set_buffer = try allocator.alignedAlloc(
            u8,
            @alignOf(u64),
            SuperBlock.FreeSet.encode_size_max(vsr.superblock.grid_blocks_max),
        );
        errdefer allocator.free(free_set);

        return StorageChecker{
            .compactions = compactions,
            .checkpoints = checkpoints,
            .free_set = free_set,
            .free_set_buffer = free_set_buffer,
        };
    }

    pub fn deinit(checker: *StorageChecker, allocator: std.mem.Allocator) void {
        allocator.free(checker.free_set_buffer);
        checker.free_set.deinit(allocator);
        checker.checkpoints.deinit();
        checker.compactions.deinit();
    }

    pub fn replica_checkpoint(checker: *StorageChecker, superblock: *const SuperBlock) !void {
        const syncing = superblock.working.vsr_state.sync_op_max > 0;

        try checker.check(
            "replica_checkpoint",
            superblock,
            std.enums.EnumSet(CheckpointArea).init(.{
                .superblock_checkpoint = true,
                .superblock_client_sessions = true,
                .client_replies = !syncing,
                .grid = !syncing,
            }),
        );

        if (!syncing) assert(checker.checkpoints.count() > 0);
    }

    /// Invoked when both superblock and content sync is complete.
    pub fn replica_sync(checker: *StorageChecker, superblock: *const SuperBlock) !void {
        try checker.check(
            "replica_sync",
            superblock,
            std.enums.EnumSet(CheckpointArea).init(.{
                .superblock_checkpoint = true,
                .superblock_client_sessions = true,
                // The replica may have have already committed some addition prepares atop the
                // checkpoint, so its client-replies zone will have mutated.
                .client_replies = false,
                .grid = true,
            }),
        );
    }

    fn check(
        checker: *StorageChecker,
        caller: []const u8,
        superblock: *const SuperBlock,
        areas: std.enums.EnumSet(CheckpointArea),
    ) !void {
        const checkpoint_actual = checkpoint: {
            var checkpoint = Checkpoint.init(.{});
            if (areas.contains(.superblock_checkpoint)) {
                checkpoint.put(
                    .superblock_checkpoint,
                    vsr.checksum(std.mem.asBytes(&superblock.working.vsr_state.checkpoint)),
                );
            }
            if (areas.contains(.superblock_client_sessions)) {
                checkpoint.put(
                    .superblock_client_sessions,
                    checksum_trailer(superblock, .client_sessions),
                );
            }
            if (areas.contains(.client_replies)) {
                checkpoint.put(.client_replies, checksum_client_replies(superblock));
            }
            if (areas.contains(.grid)) {
                checkpoint.put(.grid, checker.checksum_grid(superblock));
            }
            break :checkpoint checkpoint;
        };

        const replica_checkpoint_op = superblock.working.vsr_state.checkpoint.commit_min;
        for (std.enums.values(CheckpointArea)) |area| {
            log.debug("{}: {s}: checkpoint={} area={s} value={?x:0>32}", .{
                superblock.replica_index.?,
                caller,
                replica_checkpoint_op,
                @tagName(area),
                checkpoint_actual.get(area),
            });
        }

        if (checker.checkpoints.getPtr(replica_checkpoint_op)) |checkpoint_expect| {
            var mismatch: bool = false;
            for (std.enums.values(CheckpointArea)) |area| {
                const checksum_actual = checkpoint_actual.get(area) orelse continue;
                if (checkpoint_expect.fetchPut(area, checksum_actual)) |checksum_expect| {
                    if (checksum_expect != checksum_actual) {
                        log.warn("{}: {s}: mismatch " ++
                            "area={s} expect={x:0>32} actual={x:0>32}", .{
                            superblock.replica_index.?,
                            caller,
                            @tagName(area),
                            checksum_expect,
                            checksum_actual,
                        });

                        mismatch = true;
                    }
                }
            }
            if (mismatch) return error.StorageMismatch;
        } else {
            // This replica is the first to reach op_checkpoint.
            // Save its state for other replicas to check themselves against.
            try checker.checkpoints.putNoClobber(replica_checkpoint_op, checkpoint_actual);
        }
    }

    fn checksum_trailer(superblock: *const SuperBlock, trailer: vsr.SuperBlockTrailer) u128 {
        const trailer_size = superblock.working.trailer_size(trailer);
        const trailer_checksum = superblock.working.trailer_checksum(trailer);

        var copy: u8 = 0;
        while (copy < constants.superblock_copies) : (copy += 1) {
            const trailer_start = trailer.zone().start_for_copy(copy);

            assert(trailer_checksum ==
                vsr.checksum(superblock.storage.memory[trailer_start..][0..trailer_size]));
        }

        return trailer_checksum;
    }

    fn checksum_client_replies(superblock: *const SuperBlock) u128 {
        assert(superblock.working.vsr_state.sync_op_max == 0);

        var checksum: u128 = 0;
        for (superblock.client_sessions.entries, 0..) |client_session, slot| {
            if (client_session.session == 0) {
                // Empty slot.
            } else {
                assert(client_session.header.command == .reply);

                if (client_session.header.size == @sizeOf(vsr.Header)) {
                    // ClientReplies won't store this entry.
                } else {
                    checksum ^= vsr.checksum(superblock.storage.area_memory(
                        .{ .client_replies = .{ .slot = slot } },
                    )[0..vsr.sector_ceil(client_session.header.size)]);
                }
            }
        }
        return checksum;
    }

    fn checksum_grid(checker: *StorageChecker, superblock: *const SuperBlock) u128 {
        const free_set_size = superblock.working.vsr_state.checkpoint.free_set_size;

        if (free_set_size > 0) {
            // Read free set from the grid by manually following the linked list of blocks.
            // Note that free set is written in direct order, and must be read backwards.
            var free_set_block_address =
                superblock.working.vsr_state.checkpoint.free_set_head_address;
            var free_set_block_checksum =
                superblock.working.vsr_state.checkpoint.free_set_head_checksum;
            var free_set_cursor: usize = free_set_size;
            while (true) {
                const block = superblock.storage.grid_block(free_set_block_address).?;
                assert(schema.header_from_block(block).checksum == free_set_block_checksum);

                const encoded_words = schema.FreeSetNode.encoded_words(block);
                free_set_cursor -= encoded_words.len;
                stdx.copy_disjoint(
                    .inexact,
                    u8,
                    checker.free_set_buffer[free_set_cursor..],
                    encoded_words,
                );

                const next = schema.FreeSetNode.next(block) orelse break;
                free_set_block_address = next.address;
                free_set_block_checksum = next.checksum;
            }
            assert(free_set_cursor == 0);
        }

        checker.free_set.decode(checker.free_set_buffer[0..free_set_size]);
        defer checker.free_set.reset();

        var stream = vsr.ChecksumStream.init();
        var blocks_missing: usize = 0;
        var blocks_acquired = checker.free_set.blocks.iterator(.{});
        while (blocks_acquired.next()) |block_address_index| {
            const block_address: u64 = block_address_index + 1;
            const block = superblock.storage.grid_block(block_address) orelse {
                log.err("{}: checksum_grid: missing block_address={}", .{
                    superblock.replica_index.?,
                    block_address,
                });

                blocks_missing += 1;
                continue;
            };

            const block_header = schema.header_from_block(block);
            assert(block_header.address == block_address);

            stream.add(block[0..block_header.size]);
            // Extra guard against identical blocks:
            stream.add(std.mem.asBytes(&block_address));

            // Grid block sector padding is zeroed:
            assert(stdx.zeroed(block[block_header.size..vsr.sector_ceil(block_header.size)]));
        }
        assert(blocks_missing == 0);

        return stream.checksum();
    }
};
