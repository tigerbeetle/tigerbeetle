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

    /// We received an SV, decided to sync, but were committing at that point. So instead we
    /// requested cancellation of the commit process and entered `awaiting_checkpoint` to get
    /// a new SV in the future, while commit stage is idle.
    ///
    /// TODO: Right now this works by literally requesting a new SV from the primary, but it would
    ///       be better to hold onto the original SV in memory and re-trigger `on_start_view` after
    ///       cancellation is done.
    awaiting_checkpoint,

    /// We received a new checkpoint and a log suffix are in process of writing them to disk.
    updating_checkpoint: vsr.CheckpointState,

    pub fn valid_transition(from: std.meta.Tag(Stage), to: std.meta.Tag(Stage)) bool {
        return switch (from) {
            .idle => to == .canceling_commit or
                to == .canceling_grid or
                to == .awaiting_checkpoint,
            .canceling_commit => to == .canceling_grid,
            .canceling_grid => to == .awaiting_checkpoint,
            .awaiting_checkpoint => to == .awaiting_checkpoint or to == .updating_checkpoint,
            .updating_checkpoint => to == .idle,
        };
    }
};
