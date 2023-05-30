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
