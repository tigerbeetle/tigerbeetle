const std = @import("std");
const assert = std.debug.assert;
const vsr = @import("../../vsr.zig");

pub const GridChecker = struct {
    const Blocks = std.AutoHashMap(struct {
        checkpoint_id: u128,
        block_address: u64,
        checkpoint_durable: bool,
    }, u128);

    blocks: Blocks,

    pub fn init(allocator: std.mem.Allocator) GridChecker {
        return .{ .blocks = Blocks.init(allocator) };
    }

    pub fn deinit(checker: *GridChecker) void {
        checker.blocks.deinit();
    }

    pub fn assert_coherent(
        checker: *GridChecker,
        checkpoint: *vsr.CheckpointState,
        checkpoint_durable: bool,
        block_address: u64,
        block_checksum: u128,
    ) void {
        const result = checker.blocks.getOrPut(.{
            .checkpoint_id = vsr.checksum(std.mem.asBytes(checkpoint)),
            .block_address = block_address,
            .checkpoint_durable = checkpoint_durable,
        }) catch unreachable;

        if (result.found_existing) {
            assert(result.value_ptr.* == block_checksum);
        } else {
            result.value_ptr.* = block_checksum;
        }

        // Assert that the same version of the block must exist while the current checkpoint is
        // not durable and while the previous checkpoint is durable.
        if (!checkpoint_durable) {
            if (checker.blocks.get(.{
                .checkpoint_id = checkpoint.parent_checkpoint_id,
                .block_address = block_address,
                .checkpoint_durable = true,
            })) |checksum| {
                assert(checksum == block_checksum);
            }
        }
    }
};
