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

/// Uses a separate type from Target to make it hard to mix up canonical/candidate targets.
pub const TargetCandidate = struct {
    /// The target's checkpoint identifier.
    checkpoint_id: u128,
    /// The op_checkpoint() that corresponds to the checkpoint id.
    checkpoint_op: u64,

    pub fn canonical(target: TargetCandidate) Target {
        return .{
            .checkpoint_id = target.checkpoint_id,
            .checkpoint_op = target.checkpoint_op,
        };
    }
};

pub const Target = struct {
    /// The target's checkpoint identifier.
    checkpoint_id: u128,
    /// The op_checkpoint() that corresponds to the checkpoint id.
    checkpoint_op: u64,
};

pub const TargetQuorum = struct {
    /// The latest known checkpoint identifier from every *other* replica.
    /// Unlike sync_target_max, these Targets are *not* known to be canonical.
    candidates: [constants.replicas_max]?TargetCandidate =
        [_]?TargetCandidate{null} ** constants.replicas_max,

    pub fn replace(
        quorum: *TargetQuorum,
        replica: u8,
        candidate: *const TargetCandidate,
    ) bool {
        if (quorum.candidates[replica]) |candidate_existing| {
            // Ignore old candidate.
            if (candidate.checkpoint_op < candidate_existing.checkpoint_op) {
                return false;
            }

            // Ignore repeat candidate.
            if (candidate.checkpoint_op == candidate_existing.checkpoint_op and
                candidate.checkpoint_id == candidate_existing.checkpoint_id)
            {
                return false;
            }
        }
        quorum.candidates[replica] = candidate.*;
        return true;
    }

    pub fn count(quorum: *const TargetQuorum, candidate: *const TargetCandidate) usize {
        var candidates_matching: usize = 0;
        for (quorum.candidates) |candidate_target| {
            if (candidate_target != null and
                candidate_target.?.checkpoint_op == candidate.checkpoint_op and
                candidate_target.?.checkpoint_id == candidate.checkpoint_id)
            {
                assert(std.meta.eql(candidate_target, candidate.*));
                candidates_matching += 1;
            }
        }
        assert(candidates_matching > 0);
        return candidates_matching;
    }
};
