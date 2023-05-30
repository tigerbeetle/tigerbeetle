const std = @import("std");
const assert = std.debug.assert;
const maybe = stdx.maybe;

const constants = @import("../constants.zig");
const stdx = @import("../stdx.zig");

pub const SyncTargetCandidate = struct {
    /// The target's checkpoint identifier.
    checkpoint_id: u128,
    /// The op_checkpoint() that corresponds to the checkpoint id.
    checkpoint_op: u64,
    /// The checksum of the prepare corresponding to checkpoint_op.
    checkpoint_op_checksum: u128,

    pub fn canonical(target: SyncTargetCandidate) SyncTargetCanonical {
        return .{
            .checkpoint_id = target.checkpoint_id,
            .checkpoint_op = target.checkpoint_op,
            .checkpoint_op_checksum = target.checkpoint_op_checksum,
        };
    }
};

pub const SyncTargetCanonical = struct {
    /// The target's checkpoint identifier.
    checkpoint_id: u128,
    /// The op_checkpoint() that corresponds to the checkpoint id.
    checkpoint_op: u64,
    /// The checksum of the prepare corresponding to checkpoint_op.
    checkpoint_op_checksum: u128,
};

pub const SyncTargetQuorum = struct {
    /// The latest known checkpoint identifier from every *other* replica.
    /// Unlike sync_target_max, these SyncTargets are *not* known to be canonical.
    candidates: [constants.replicas_max]?SyncTargetCandidate =
        [_]?SyncTargetCandidate{null} ** constants.replicas_max,

    pub fn replace(
        quorum: *SyncTargetQuorum,
        replica: u8,
        candidate: *const SyncTargetCandidate,
    ) bool {
        if (quorum.candidates[replica]) |candidate_existing| {
            // Ignore old candidate.
            if (candidate.checkpoint_op < candidate_existing.checkpoint_op) {
                return false;
            }

            maybe(candidate.checkpoint_op == candidate_existing.checkpoint_op);
        }
        quorum.candidates[replica] = candidate.*;
        return true;
    }

    pub fn count(quorum: *const SyncTargetQuorum, candidate: *const SyncTargetCandidate) usize {
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
        return candidates_matching;
    }
};
