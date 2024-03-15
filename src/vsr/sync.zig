const std = @import("std");
const assert = std.debug.assert;
const maybe = stdx.maybe;

const vsr = @import("../vsr.zig");
const constants = @import("../constants.zig");
const stdx = @import("../stdx.zig");

pub const Stage = union(enum) {
    idle,

    /// The commit lifecycle is in a stage that cannot be interrupted/canceled.
    /// We are waiting until that uninterruptible stage completes.
    /// When it completes, we will abort the commit chain and resume sync.
    /// (State sync will replace any changes the commit made anyway.)
    canceling_commit,

    /// Waiting for `Grid.cancel()`.
    canceling_grid,

    /// We need to sync, but are waiting for a usable `sync_target_max`.
    requesting_target,

    requesting_checkpoint: RequestingCheckpoint,
    updating_superblock: UpdatingSuperBlock,

    pub const RequestingCheckpoint = struct {
        target: Target,
    };

    pub const UpdatingSuperBlock = struct {
        target: Target,
        checkpoint_state: vsr.CheckpointState,
    };

    pub fn valid_transition(from: std.meta.Tag(Stage), to: std.meta.Tag(Stage)) bool {
        return switch (from) {
            .idle => to == .canceling_commit or
                to == .canceling_grid or
                to == .requesting_target,
            .canceling_commit => to == .canceling_grid,
            .canceling_grid => to == .requesting_target,
            .requesting_target => to == .requesting_target or
                to == .requesting_checkpoint,
            .requesting_checkpoint => to == .requesting_checkpoint or
                to == .updating_superblock,
            .updating_superblock => to == .requesting_checkpoint or
                to == .idle,
        };
    }

    pub fn target(stage: *const Stage) ?Target {
        return switch (stage.*) {
            .idle,
            .canceling_commit,
            .canceling_grid,
            .requesting_target,
            => null,
            .requesting_checkpoint => |s| s.target,
            .updating_superblock => |s| s.target,
        };
    }
};

pub const Target = struct {
    /// The target's checkpoint identifier.
    checkpoint_id: u128,
    /// The op_checkpoint() that corresponds to the checkpoint id.
    checkpoint_op: u64,
    /// The view where the target's checkpoint is committed.
    /// It might be greater than `checkpoint.header.view`.
    view: u32,
};
