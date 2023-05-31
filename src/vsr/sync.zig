const std = @import("std");
const assert = std.debug.assert;
const maybe = stdx.maybe;

const vsr = @import("../vsr.zig");
const constants = @import("../constants.zig");
const stdx = @import("../stdx.zig");

/// Initial stage: .none
///
/// Transitions:
///
///   .none                  → .cancel_commit | .cancel_grid | .request_target
///   .cancel_commit         → .cancel_grid
///   .cancel_grid           → .request_target
///   .request_target        → .write_sync_start
///   .write_sync_start      → .write_sync_start | .request_trailers
///   .request_trailers      → .write_sync_start | .cancel_grid | .request_manifest_logs
///   .request_manifest_logs → .write_sync_start | .cancel_grid | .write_sync_done
///   .write_sync_done       → .write_sync_start | .done
///   .done                  → .write_sync_start | .none
///
pub const SyncStage = union(enum) {
    /// Not syncing.
    none,

    /// Waiting for a uninterruptible step of the commit chain.
    cancel_commit,

    /// Waiting for `Grid.cancel()`.
    cancel_grid,

    /// We need to sync, but are waiting for a usable `sync_target_max`.
    request_target,

    /// superblock.sync_start()
    write_sync_start: struct { target: SyncTargetCanonical },

    request_trailers: struct {
        target: SyncTargetCanonical,
        manifest: SyncTrailer = .{},
        free_set: SyncTrailer = .{},
        client_sessions: SyncTrailer = .{},

        pub fn done(self: *const @This()) bool {
            return self.manifest.done and self.free_set.done and self.client_sessions.done;
        }
    },

    request_manifest_logs: struct { target: SyncTargetCanonical },

    /// superblock.sync_done()
    write_sync_done: struct { target: SyncTargetCanonical },

    done: struct { target: SyncTargetCanonical },

    pub fn target(stage: *const SyncStage) ?SyncTargetCanonical {
        return switch (stage.*) {
            .none,
            .cancel_commit,
            .cancel_grid,
            .request_target,
            => null,
            .write_sync_start => |s| s.target,
            .request_trailers => |s| s.target,
            .request_manifest_logs => |s| s.target,
            .write_sync_done => |s| s.target,
            .done => |s| s.target,
        };
    }
};

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

pub const SyncTrailer = struct {
    pub const chunk_size_max = constants.message_body_size_max;

    /// The next offset to fetch.
    offset: u32 = 0,
    done: bool = false,
    final: ?struct { size: u32, checksum: u128 } = null,

    pub fn write(
        trailer: *SyncTrailer,
        data: struct {
            total_buffer: []align(@alignOf(u128)) u8,
            total_size: u32,
            total_checksum: u128,
            chunk: []const u8,
            chunk_offset: u32,
        },
    ) ?[]align(@alignOf(u128)) u8 {
        assert(data.chunk.len <= chunk_size_max);
        assert(data.total_size <= data.total_buffer.len);

        if (trailer.final) |*final| {
            assert(final.checksum == data.total_checksum);
            assert(final.size == data.total_size);
        } else {
            assert(trailer.offset == 0);
            assert(!trailer.done);

            trailer.final = .{
                .checksum = data.total_checksum,
                .size = data.total_size,
            };
        }

        if (trailer.done) return null;

        const total_buffer = data.total_buffer[data.chunk_offset..][0..data.chunk.len];
        if (trailer.offset == data.chunk_offset) {
            stdx.copy_disjoint(.exact, u8, total_buffer, data.chunk);
            trailer.offset += @intCast(u32, data.chunk.len);
            assert(trailer.offset <= data.total_size);

            if (trailer.offset == data.total_size) {
                assert(vsr.checksum(data.total_buffer[0..data.total_size]) == data.total_checksum);

                trailer.done = true;
                return data.total_buffer[0..data.total_size];
            }
        } else {
            if (trailer.offset < data.chunk_offset) {
                // Already received this chunk.
                assert(std.mem.eql(u8, total_buffer, data.chunk));
            } else {
                // We're not ready for this chunk yet.
            }
        }
        return null;
    }
};

test "SyncTrailer" {
    const total_want = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8 };

    var chunk_step: usize = 1;
    while (chunk_step <= total_want.len) : (chunk_step += 1) {
        var total_got: [16]u8 align(@alignOf(u128)) = undefined;
        var trailer = SyncTrailer{};

        var chunk_offset: usize = 0;
        while (chunk_offset < total_want.len) {
            const chunk_size = @minimum(chunk_step, total_want.len - chunk_offset);
            const result = trailer.write(.{
                .total_buffer = total_got[0..],
                .total_size = @intCast(u32, total_want.len),
                .total_checksum = vsr.checksum(total_want[0..]),
                .chunk = total_want[chunk_offset..][0..chunk_size],
                .chunk_offset = @intCast(u32, chunk_offset),
            });

            chunk_offset += chunk_size;

            if (chunk_offset == total_want.len) {
                try std.testing.expect(std.mem.eql(u8, result.?, total_want[0..]));
                break;
            } else {
                try std.testing.expectEqual(result, null);
            }
        } else unreachable;
    }
}
