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

    requesting_trailers: RequestingTrailers,
    updating_superblock: UpdatingSuperBlock,

    pub const RequestingTrailers = struct {
        const Trailers = std.enums.EnumArray(vsr.SuperBlockTrailer, Trailer);

        target: Target,
        trailers: Trailers = Trailers.initFill(.{}),

        pub fn done(self: *const @This()) bool {
            for (std.enums.values(vsr.SuperBlockTrailer)) |trailer| {
                if (!self.trailers.get(trailer).done) return false;
            }
            return true;
        }
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
                to == .requesting_trailers,
            .requesting_trailers => to == .requesting_trailers or
                to == .updating_superblock,
            .updating_superblock => to == .requesting_trailers or
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
            .requesting_trailers => |s| s.target,
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

/// SuperBlock trailers may be too large to fit in a single message body.
/// The Trailer assembles the pieces of these trailers, tracking the progress so that
/// the replica knows how much of the trailer still needs to be requested.
pub const Trailer = struct {
    pub const chunk_size_max = constants.sync_trailer_message_body_size_max;

    /// The next offset to fetch.
    offset: u32 = 0,
    done: bool = false,
    final: ?struct { size: u32, checksum: u128 } = null,

    pub const requests = std.enums.EnumArray(vsr.SuperBlockTrailer, vsr.Command).init(.{
        .client_sessions = .request_sync_client_sessions,
    });

    pub const responses = std.enums.EnumArray(vsr.SuperBlockTrailer, vsr.Command).init(.{
        .client_sessions = .sync_client_sessions,
    });

    pub fn write_chunk(
        trailer: *Trailer,
        destination: struct {
            buffer: []align(@alignOf(u128)) u8,
            size: u32,
            checksum: u128,
        },
        chunk: struct {
            chunk: []const u8,
            chunk_offset: u32,
        },
    ) enum { complete, incomplete, ignore } {
        assert(chunk.chunk.len <= chunk_size_max);
        assert(destination.size <= destination.buffer.len);

        if (trailer.final) |*final| {
            assert(final.checksum == destination.checksum);
            assert(final.size == destination.size);
        } else {
            assert(trailer.offset == 0);
            assert(!trailer.done);

            trailer.final = .{
                .checksum = destination.checksum,
                .size = destination.size,
            };
        }

        if (trailer.done) return .ignore;

        const buffer = destination.buffer[chunk.chunk_offset..][0..chunk.chunk.len];
        if (trailer.offset == chunk.chunk_offset) {
            stdx.copy_disjoint(.exact, u8, buffer, chunk.chunk);
            trailer.offset += @as(u32, @intCast(chunk.chunk.len));
            assert(trailer.offset <= destination.size);

            if (trailer.offset == destination.size) {
                assert(vsr.checksum(destination.buffer[0..destination.size]) == destination.checksum);

                trailer.done = true;
                return .complete;
            } else {
                return .incomplete;
            }
        } else {
            if (trailer.offset < chunk.chunk_offset) {
                // We're not ready for this chunk yet.
            } else {
                // Already received this chunk.
                assert(std.mem.eql(u8, buffer, chunk.chunk));
                assert(chunk.chunk_offset + chunk.chunk.len <= trailer.offset);
            }
            return .ignore;
        }
    }
};

test "sync: Trailer chunk sequence" {
    const total_want = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8 };

    var chunk_step: usize = 1;
    while (chunk_step <= total_want.len) : (chunk_step += 1) {
        var total_got: [16]u8 align(@alignOf(u128)) = undefined;
        var trailer = Trailer{};

        var chunk_offset: usize = 0;
        while (chunk_offset < total_want.len) {
            const chunk_size = @min(chunk_step, total_want.len - chunk_offset);
            const result = trailer.write_chunk(.{
                .buffer = total_got[0..],
                .size = @as(u32, @intCast(total_want.len)),
                .checksum = vsr.checksum(total_want[0..]),
            }, .{
                .chunk = total_want[chunk_offset..][0..chunk_size],
                .chunk_offset = @as(u32, @intCast(chunk_offset)),
            });

            chunk_offset += chunk_size;

            if (chunk_offset == total_want.len) {
                try std.testing.expect(std.mem.eql(
                    u8,
                    total_got[0..total_want.len],
                    total_want[0..],
                ));
                try std.testing.expectEqual(result, .complete);
                break;
            } else {
                try std.testing.expectEqual(result, .incomplete);
            }
        } else unreachable;
    }
}

test "sync: Trailer past/future chunk" {
    const total_want = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8 };
    var total_got: [16]u8 align(@alignOf(u128)) = undefined;
    var trailer = Trailer{};

    // Ignore a repeated chunk.
    inline for (.{ .incomplete, .ignore }) |result_want| {
        const result_got = trailer.write_chunk(.{
            .buffer = total_got[0..],
            .size = @as(u32, @intCast(total_want.len)),
            .checksum = vsr.checksum(total_want[0..]),
        }, .{
            .chunk = total_want[0..2],
            .chunk_offset = 0,
        });
        try std.testing.expectEqual(result_got, result_want);
    }

    {
        // Ignore a chunk we aren't ready for yet.
        const result = trailer.write_chunk(.{
            .buffer = total_got[0..],
            .size = @as(u32, @intCast(total_want.len)),
            .checksum = vsr.checksum(total_want[0..]),
        }, .{
            .chunk = total_want[6..8],
            .chunk_offset = 6,
        });
        try std.testing.expectEqual(result, .ignore);
    }

    const result = trailer.write_chunk(.{
        .buffer = total_got[0..],
        .size = @as(u32, @intCast(total_want.len)),
        .checksum = vsr.checksum(total_want[0..]),
    }, .{
        .chunk = total_want[2..],
        .chunk_offset = 2,
    });
    try std.testing.expectEqual(result, .complete);
    try std.testing.expect(std.mem.eql(
        u8,
        total_got[0..total_want.len],
        total_want[0..],
    ));
}
