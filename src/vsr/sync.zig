const std = @import("std");

const vsr = @import("../vsr.zig");

pub const Stage = union(enum) {
    idle,

    /// The commit lifecycle is in a stage that cannot be interrupted/canceled.
    /// We are waiting until that uninterruptible stage completes.
    /// When it completes, we will abort the commit chain and resume sync.
    /// (State sync will replace any changes the commit made anyway.)
    canceling_commit,

    /// Waiting for `Grid.cancel()`.
    canceling_grid,

    /// Superblock is being updated with the new checkpoint and log suffix (view headers).
    updating_checkpoint: vsr.CheckpointState,

    pub fn valid_transition(from: std.meta.Tag(Stage), to: std.meta.Tag(Stage)) bool {
        return switch (from) {
            .idle => to == .canceling_commit or
                to == .canceling_grid,
            .canceling_commit => to == .canceling_grid,
            .canceling_grid => to == .updating_checkpoint,
            .updating_checkpoint => to == .idle,
        };
    }
};
