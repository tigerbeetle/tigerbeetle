//! Verify deterministic storage.
//!
//! At each replica checkpoint, check that storage is byte-for-byte identical across replicas.
//!
//! Areas verified:
//! - WAL headers
//! - WAL prepares
//! - SuperBlock Manifest, FreeSet, ClientTable
//! - Allocated Grid blocks
//!
//! Areas not verified:
//! - SuperBlock sectors
//! - Non-allocated Grid blocks
const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.storage_checker);

const config = @import("../config.zig");
const vsr = @import("../vsr.zig");
const SuperBlockFormat = @import("../vsr/superblock.zig").Format;
const FreeSet = @import("../vsr/superblock.zig").SuperBlockFreeSet;
const Replica = @import("cluster.zig").Replica;
const Storage = @import("storage.zig").Storage;

/// Maps from op_checkpoint to storage checksum.
///
/// Not every checkpoint is necessarily recorded — a replica calls on_checkpoint *at most* once.
/// For example, a replica will not call on_checkpoint if it crashes (during a checkpoint) after
/// writing 2 superblock copies. (This could be repeated by other replicas).
const CheckpointChecksums = std.AutoHashMap(u64, u128);

pub const StorageChecker = struct {
    allocator: std.mem.Allocator,
    checkpoints: CheckpointChecksums,

    pub fn init(allocator: std.mem.Allocator) StorageChecker {
        var checkpoints = CheckpointChecksums.init(allocator);
        errdefer checkpoints.deinit();

        return StorageChecker{
            .allocator = allocator,
            .checkpoints = checkpoints,
        };
    }

    pub fn deinit(checker: *StorageChecker) void {
        checker.checkpoints.deinit();
    }

    pub fn check_storage(checker: *StorageChecker, replica: *const Replica) !void {
        const storage: *const Storage = replica.superblock.storage;
        var checksum: u128 = 0;

        log.debug("{}: check_storage: checkpoint={}", .{
            replica.replica,
            replica.op_checkpoint,
        });

        inline for (.{ vsr.Zone.wal_headers, vsr.Zone.wal_prepares }) |zone| {
            const checksum_zone =
                vsr.checksum(storage.memory[zone.offset(0)..][0..zone.size().?]);
            checksum ^= checksum_zone;

            log.debug("{}: check_storage: zone={} checksum={}", .{
                replica.replica,
                checksum_zone,
                zone,
            });
        }

        const working = replica.superblock.working;
        // TODO(Zig): We shouldn't need an explicit type signature here, but there is a runtime
        // segfault without it.
        for (&[_]struct{
            offset: fn (copy: u8, sequence: u64) u64,
            size: u64,
        }{
            .{ .offset = SuperBlockFormat.offset_manifest, .size = working.manifest_size },
            .{ .offset = SuperBlockFormat.offset_free_set, .size = working.free_set_size },
            .{ .offset = SuperBlockFormat.offset_client_table, .size = working.client_table_size },
        }) |trailer| {
            var copy: u8 = 0;
            while (copy < config.superblock_copies * 2) : (copy += 1) {
                const sequence = @divFloor(copy, config.superblock_copies);
                const offset_in_zone = trailer.offset(copy, sequence);
                const offset_in_storage = vsr.Zone.superblock.offset(offset_in_zone);
                const checksum_area =
                    vsr.checksum(storage.memory[offset_in_storage..][0..trailer.size]);
                checksum ^= checksum_area;

                log.debug("{}: check_storage: zone={} offset={} size={} checksum={}", .{
                    replica.replica,
                    checksum_area,
                    vsr.Zone.superblock,
                    offset_in_zone,
                    trailer.size,
                });
            }
        }

        var free_set = try FreeSet.init(storage.allocator, config.block_count_max);
        defer free_set.deinit(storage.allocator);

        const free_set_offset = SuperBlockFormat.offset_free_set(working.copy, working.sequence);
        free_set.decode(storage.memory[free_set_offset..][0..working.free_set_size]);

        var acquired = free_set.blocks.iterator(.{ .kind = .unset });
        var checksum_grid: u128 = 0;
        while (acquired.next()) |address_index| {
            const address: u64 = address_index + 1;

            const block_offset = vsr.Zone.grid.offset(address * config.block_size);
            const block_buffer = storage.memory[block_offset..][0..config.block_size];

            checksum_grid ^= vsr.checksum(block_buffer);
        }
        checksum ^= checksum_grid;

        log.debug("{}: check_storage: zone={} checksum={}", .{
            replica.replica,
            checksum_grid,
            vsr.Zone.grid,
        });

        if (checker.checkpoints.get(replica.op_checkpoint)) |checksum_expect| {
            if (checksum_expect != checksum) {
                log.err("{}: check_storage: mismatch at checkpoint={} " ++
                    "(checksum={} checksum_expect={})", .{
                    replica.replica,
                    replica.op_checkpoint,
                    checksum,
                    checksum_expect,
                });
                // TODO If available, compare storage in more detail to identify differences.

                return error.StorageMismatch;
            }
        } else {
            try checker.checkpoints.putNoClobber(replica.op_checkpoint, checksum);
        }
    }
};
