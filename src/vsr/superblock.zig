//! SuperBlock invariants:
//!
//!   * vsr_state
//!     - vsr_state.replica and vsr_state.replica_count are immutable for now.
//!     - vsr_state.commit_min is initially 0 (for a newly-formatted replica).
//!     - vsr_state.commit_min ≤ vsr_state.commit_max
//!     - vsr_state.commit_min_before ≤ vsr_state.commit_min
//!     - vsr_state.log_view ≤ vsr_state.view
//!     - vsr_state.sync_op_min ≤ vsr_state.sync_op_max
//!     - checkpoint() must advance the superblock's vsr_state.commit_min.
//!     - view_change() must not advance the superblock's vsr_state.commit_min.
//!     - The following are monotonically increasing:
//!       - vsr_state.log_view
//!       - vsr_state.view
//!       - vsr_state.commit_max
//!     - vsr_state.commit_min may backtrack due to state sync.
//!
const std = @import("std");
const assert = std.debug.assert;
const crypto = std.crypto;
const mem = std.mem;
const meta = std.meta;
const os = std.os;
const maybe = stdx.maybe;

const constants = @import("../constants.zig");
const stdx = @import("../stdx.zig");
const vsr = @import("../vsr.zig");
const log = std.log.scoped(.superblock);

pub const SuperBlockManifest = @import("superblock_manifest.zig").Manifest;
pub const SuperBlockFreeSet = @import("superblock_free_set.zig").FreeSet;
pub const SuperBlockClientSessions = @import("superblock_client_sessions.zig").ClientSessions;
pub const Quorums = @import("superblock_quorums.zig").QuorumsType(.{
    .superblock_copies = constants.superblock_copies,
});

pub const SuperBlockVersion: u16 = 0;

const vsr_headers_reserved_size = constants.sector_size -
    ((constants.view_change_headers_max * @sizeOf(vsr.Header)) % constants.sector_size);

// Fields are aligned to work as an extern or packed struct.
pub const SuperBlockHeader = extern struct {
    checksum: u128 = undefined,

    /// Protects against misdirected reads at startup.
    /// For example, if multiple reads are all misdirected to a single copy of the superblock.
    /// Excluded from the checksum calculation to ensure that all copies have the same checksum.
    /// This simplifies writing and comparing multiple copies.
    /// TODO: u8 should be enough here, we use u16 only for alignment.
    copy: u16 = 0,

    /// The version of the superblock format in use, reserved for major breaking changes.
    version: u16,

    /// Protects against writing to or reading from the wrong data file.
    cluster: u32,

    /// The current size of the data file.
    storage_size: u64,

    /// The maximum possible size of the data file.
    /// The maximum allowed runtime storage_size_limit.
    /// The FreeSet's on-disk size is a function of storage_size_max.
    storage_size_max: u64,

    /// A monotonically increasing counter to locate the latest superblock at startup.
    sequence: u64,

    /// The checksum of the previous superblock to hash chain across sequence numbers.
    parent: u128,

    /// The checksum over the manifest block references in the superblock trailer.
    manifest_checksum: u128,

    /// The checksum over the actual encoded block free set in the superblock trailer.
    free_set_checksum: u128,

    /// The checksum over the client table entries in the superblock trailer.
    client_sessions_checksum: u128,

    /// State stored on stable storage for the Viewstamped Replication consensus protocol.
    vsr_state: VSRState,

    /// Reserved for future minor features (e.g. changing the compression algorithm of the trailer).
    flags: u64 = 0,

    /// A listing of persistent read snapshots that have been issued to clients.
    /// A snapshot.created timestamp of 0 indicates that the snapshot is null.
    snapshots: [constants.lsm_snapshots_max]Snapshot,

    /// The size of the manifest block references stored in the superblock trailer.
    /// The block addresses and checksums in this section of the trailer are laid out as follows:
    /// [manifest_size / (16 + 8 + 1)]u128 checksum
    /// [manifest_size / (16 + 8 + 1)]u64 address
    /// [manifest_size / (16 + 8 + 1)]u8 tree
    manifest_size: u32,

    /// The size of the block free set stored in the superblock trailer.
    free_set_size: u32,

    /// The size of the client table entries stored in the superblock trailer.
    /// (Always superblock_trailer_client_sessions_size_max).
    client_sessions_size: u32,

    /// The number of headers in vsr_headers_all.
    vsr_headers_count: u32,

    reserved: [2888]u8 = [_]u8{0} ** 2888,

    /// SV/DVC header suffix. Headers are ordered from high-to-low op.
    /// Unoccupied headers (after vsr_headers_count) are zeroed.
    ///
    /// When `vsr_state.log_view < vsr_state.view`, the headers are for a DVC.
    /// When `vsr_state.log_view = vsr_state.view`, the headers are for a SV.
    vsr_headers_all: [constants.view_change_headers_max]vsr.Header,
    vsr_headers_reserved: [vsr_headers_reserved_size]u8 =
        [_]u8{0} ** vsr_headers_reserved_size,

    comptime {
        assert(@sizeOf(SuperBlockHeader) % constants.sector_size == 0);
        assert(@divExact(@sizeOf(SuperBlockHeader), constants.sector_size) >= 2);
        assert(@offsetOf(SuperBlockHeader, "vsr_headers_all") == constants.sector_size);
        // Assert that there is no implicit padding in the struct.
        assert(stdx.no_padding(SuperBlockHeader));
    }

    pub const VSRState = extern struct {
        /// The checkpoint_id() of the checkpoint which last updated our commit_min.
        /// Following state sync, this is set to the last checkpoint that we skipped.
        previous_checkpoint_id: u128,

        /// The vsr.Header.checksum of commit_min's message.
        commit_min_checksum: u128,

        /// Globally unique identifier of the replica, must be non-zero.
        replica_id: u128,

        members: vsr.Members,

        /// The highest known canonical checkpoint op.
        /// Usually the previous checkpoint op (i.e. prior to VSRState.commit_min).
        commit_min_canonical: u64,

        /// The last operation committed to the state machine. At startup, replay the log hereafter.
        commit_min: u64,

        /// The highest operation up to which we may commit.
        commit_max: u64,

        /// See `sync_op_max`.
        sync_op_min: u64,

        /// When zero, all of the grid blocks and replies are synced.
        /// (When zero, `sync_op_min` is also zero.)
        ///
        /// When nonzero, we must repair grid-blocks/client-replies that would have been written
        /// during the commits between `sync_op_min` and `sync_op_max` (inclusive).
        /// (Those grid-blocks and client-replies were not written normally because we "skipped"
        /// past them via state sync.)
        sync_op_max: u64,

        /// The last view in which the replica's status was normal.
        log_view: u32,

        /// The view number of the replica.
        view: u32,

        /// Number of replicas (determines sizes of the quorums), part of VSR configuration.
        replica_count: u8,

        reserved: [15]u8 = [_]u8{0} ** 15,

        comptime {
            assert(@sizeOf(VSRState) == 304);
            // Assert that there is no implicit padding in the struct.
            assert(stdx.no_padding(VSRState));
        }

        pub fn root(options: struct {
            cluster: u32,
            replica_id: u128,
            members: vsr.Members,
            replica_count: u8,
        }) VSRState {
            return .{
                .previous_checkpoint_id = 0,
                .replica_id = options.replica_id,
                .members = options.members,
                .replica_count = options.replica_count,
                .commit_min_checksum = vsr.Header.root_prepare(options.cluster).checksum,
                .commit_min_canonical = 0,
                .commit_min = 0,
                .commit_max = 0,
                .sync_op_min = 0,
                .sync_op_max = 0,
                .log_view = 0,
                .view = 0,
            };
        }

        pub fn assert_internally_consistent(state: VSRState) void {
            assert(state.commit_min_canonical <= state.commit_min);
            assert(state.commit_max >= state.commit_min);
            assert(state.sync_op_max >= state.sync_op_min);
            assert(state.view >= state.log_view);
            assert(state.replica_count > 0);
            assert(state.replica_count <= constants.replicas_max);
            assert(vsr.member_index(&state.members, state.replica_id) != null);
        }

        pub fn monotonic(old: VSRState, new: VSRState) bool {
            old.assert_internally_consistent();
            new.assert_internally_consistent();
            if (old.commit_min == new.commit_min) {
                if (old.commit_min_checksum == 0 and old.commit_min == 0) {
                    // "old" is the root VSRState.
                    assert(old.commit_max == 0);
                    assert(old.sync_op_min == 0);
                    assert(old.sync_op_max == 0);
                    assert(old.log_view == 0);
                    assert(old.view == 0);
                } else {
                    assert(old.commit_min_checksum == new.commit_min_checksum);
                    assert(old.previous_checkpoint_id == new.previous_checkpoint_id);
                }
            } else {
                assert(old.commit_min_checksum != new.commit_min_checksum);
                assert(old.previous_checkpoint_id != new.previous_checkpoint_id);
            }
            assert(old.replica_id == new.replica_id);
            assert(old.replica_count == new.replica_count);
            assert(stdx.equal_bytes([constants.members_max]u128, &old.members, &new.members));

            if (old.view > new.view) return false;
            if (old.log_view > new.log_view) return false;
            if (old.commit_min_canonical > new.commit_min_canonical) return false;
            if (old.commit_min > new.commit_min) return false;
            if (old.commit_max > new.commit_max) return false;

            return true;
        }

        pub fn would_be_updated_by(old: VSRState, new: VSRState) bool {
            assert(monotonic(old, new));

            return !stdx.equal_bytes(VSRState, &old, &new);
        }

        /// Compaction is one bar ahead of superblock's commit_min.
        /// The commits from the bar following commit_min were in the mutable table, and
        /// thus not preserved in the checkpoint.
        /// But the corresponding `compact()` updates were preserved, and must not be repeated
        /// to ensure deterministic storage.
        pub fn op_compacted(state: VSRState, op: u64) bool {
            // If commit_min is 0, we have never checkpointed, so no compactions are checkpointed.
            return state.commit_min > 0 and
                op <= vsr.Checkpoint.trigger_for_checkpoint(state.commit_min).?;
        }
    };

    pub const Snapshot = extern struct {
        /// A creation timestamp of 0 indicates that the snapshot is null.
        created: u64,

        /// When a read query last used the snapshot.
        queried: u64,

        /// Snapshots may auto-expire after a timeout of inactivity.
        /// A timeout of 0 indicates that the snapshot must be explicitly released by the user.
        timeout: u64,

        comptime {
            assert(@sizeOf(Snapshot) == 24);
            // Assert that there is no implicit padding in the struct.
            assert(stdx.no_padding(Snapshot));
        }

        pub fn exists(snapshot: Snapshot) bool {
            if (snapshot.created == 0) {
                assert(snapshot.queried == 0);
                assert(snapshot.timeout == 0);

                return false;
            } else {
                return true;
            }
        }
    };

    pub fn calculate_checksum(superblock: *const SuperBlockHeader) u128 {
        comptime assert(meta.fieldIndex(SuperBlockHeader, "checksum") == 0);
        comptime assert(meta.fieldIndex(SuperBlockHeader, "copy") == 1);

        const checksum_size = @sizeOf(@TypeOf(superblock.checksum));
        comptime assert(checksum_size == @sizeOf(u128));

        const copy_size = @sizeOf(@TypeOf(superblock.copy));
        comptime assert(copy_size == 2);

        const ignore_size = checksum_size + copy_size;

        return vsr.checksum(std.mem.asBytes(superblock)[ignore_size..]);
    }

    pub fn set_checksum(superblock: *SuperBlockHeader) void {
        assert(superblock.copy < constants.superblock_copies);
        assert(superblock.version == SuperBlockVersion);
        assert(superblock.flags == 0);

        for (mem.bytesAsSlice(u64, &superblock.reserved)) |word| assert(word == 0);
        for (mem.bytesAsSlice(u8, &superblock.vsr_state.reserved)) |word| assert(word == 0);
        for (mem.bytesAsSlice(u64, &superblock.vsr_headers_reserved)) |word| assert(word == 0);

        superblock.checksum = superblock.calculate_checksum();
    }

    pub fn valid_checksum(superblock: *const SuperBlockHeader) bool {
        return superblock.checksum == superblock.calculate_checksum();
    }

    pub fn checkpoint_id(superblock: *const SuperBlockHeader) u128 {
        return vsr.checksum(std.mem.asBytes(&[_]u128{
            superblock.manifest_checksum,
            superblock.free_set_checksum,
            superblock.client_sessions_checksum,
        }));
    }

    /// Does not consider { checksum, copy } when comparing equality.
    pub fn equal(a: *const SuperBlockHeader, b: *const SuperBlockHeader) bool {
        for (mem.bytesAsSlice(u64, &a.reserved)) |word| assert(word == 0);
        for (mem.bytesAsSlice(u64, &b.reserved)) |word| assert(word == 0);

        for (mem.bytesAsSlice(u8, &a.vsr_state.reserved)) |word| assert(word == 0);
        for (mem.bytesAsSlice(u8, &b.vsr_state.reserved)) |word| assert(word == 0);

        for (mem.bytesAsSlice(u64, &a.vsr_headers_reserved)) |word| assert(word == 0);
        for (mem.bytesAsSlice(u64, &b.vsr_headers_reserved)) |word| assert(word == 0);

        if (a.version != b.version) return false;
        if (a.cluster != b.cluster) return false;
        if (a.storage_size != b.storage_size) return false;
        if (a.storage_size_max != b.storage_size_max) return false;
        if (a.sequence != b.sequence) return false;
        if (a.parent != b.parent) return false;
        if (a.manifest_checksum != b.manifest_checksum) return false;
        if (a.free_set_checksum != b.free_set_checksum) return false;
        if (a.client_sessions_checksum != b.client_sessions_checksum) return false;
        if (!stdx.equal_bytes(VSRState, &a.vsr_state, &b.vsr_state)) return false;
        if (!stdx.equal_bytes(
            [constants.lsm_snapshots_max]Snapshot,
            &a.snapshots,
            &b.snapshots,
        )) return false;
        if (a.manifest_size != b.manifest_size) return false;
        if (a.free_set_size != b.free_set_size) return false;
        if (a.vsr_headers_count != b.vsr_headers_count) return false;
        if (!stdx.equal_bytes(
            [constants.view_change_headers_max]vsr.Header,
            &a.vsr_headers_all,
            &b.vsr_headers_all,
        )) return false;

        return true;
    }

    pub fn vsr_headers(superblock: *const SuperBlockHeader) vsr.Headers.ViewChangeSlice {
        return vsr.Headers.ViewChangeSlice.init(
            if (superblock.vsr_state.log_view < superblock.vsr_state.view)
                .do_view_change
            else
                .start_view,
            superblock.vsr_headers_all[0..superblock.vsr_headers_count],
        );
    }

    pub fn trailer_size(superblock: *const SuperBlockHeader, trailer: Trailer) u32 {
        return switch (trailer) {
            .manifest => superblock.manifest_size,
            .free_set => superblock.free_set_size,
            .client_sessions => superblock.client_sessions_size,
        };
    }

    pub fn trailer_checksum(superblock: *const SuperBlockHeader, trailer: Trailer) u128 {
        return switch (trailer) {
            .manifest => superblock.manifest_checksum,
            .free_set => superblock.free_set_checksum,
            .client_sessions => superblock.client_sessions_checksum,
        };
    }
};

comptime {
    switch (constants.superblock_copies) {
        4, 6, 8 => {},
        else => @compileError("superblock_copies must be either { 4, 6, 8 } for flexible quorums."),
    }
}

/// The size of the entire superblock storage zone.
pub const superblock_zone_size = superblock_copy_size * constants.superblock_copies;

/// The size of an individual superblock including trailer.
pub const superblock_copy_size = @sizeOf(SuperBlockHeader) + superblock_trailer_size_max;
comptime {
    assert(superblock_copy_size % constants.sector_size == 0);
}

/// The maximum possible size of the superblock trailer, following the superblock header.
pub const superblock_trailer_size_max = blk: {
    // To calculate the size of the superblock trailer we need to know:
    // 1. the maximum number of manifest blocks that should be able to be referenced, and
    // 2. the maximum possible size of the EWAH-compressed bit set addressable by the free set.

    assert(superblock_trailer_manifest_size_max > 0);
    assert(superblock_trailer_manifest_size_max % constants.sector_size == 0);
    assert(superblock_trailer_manifest_size_max % SuperBlockManifest.BlockReferenceSize == 0);

    assert(superblock_trailer_free_set_size_max > 0);
    assert(superblock_trailer_free_set_size_max % constants.sector_size == 0);

    assert(superblock_trailer_client_sessions_size_max > 0);
    assert(superblock_trailer_client_sessions_size_max % constants.sector_size == 0);

    // We order the smaller manifest section ahead of the block free set for better access locality.
    // For example, it's cheaper to skip over 1 MiB when reading from disk than to skip over 32 MiB.
    break :blk superblock_trailer_manifest_size_max +
        superblock_trailer_free_set_size_max +
        superblock_trailer_client_sessions_size_max;
};

// A manifest block reference of 24 bytes contains a manifest block checksum and address.
// These references are stored in struct-of-arrays layout in the trailer for the sake of alignment.
const superblock_trailer_manifest_size_max = blk: {
    assert(SuperBlockManifest.BlockReferenceSize == 16 + 8);

    // Use a multiple of sector * reference so that the size is exactly divisible without padding:
    // For example, this 2.5 MiB manifest trailer == 65536 references == 65536 * 511 or 34m tables.
    // TODO Size this relative to the expected number of tables & fragmentation.
    break :blk 16 * constants.sector_size * SuperBlockManifest.BlockReferenceSize;
};

const superblock_trailer_free_set_size_max = blk: {
    const encode_size_max = SuperBlockFreeSet.encode_size_max(grid_blocks_max);
    assert(encode_size_max > 0);

    break :blk vsr.sector_ceil(encode_size_max);
};

const superblock_trailer_client_sessions_size_max = blk: {
    const encode_size_max = SuperBlockClientSessions.encode_size_max;
    assert(encode_size_max > 0);

    break :blk vsr.sector_ceil(encode_size_max);
};

/// The size of a data file that has an empty grid.
pub const data_file_size_min =
    superblock_zone_size + constants.journal_size + constants.client_replies_size;

/// The maximum number of blocks in the grid.
pub const grid_blocks_max = blk: {
    var size = constants.storage_size_max;
    size -= constants.superblock_copies * @sizeOf(SuperBlockHeader);
    size -= constants.superblock_copies * superblock_trailer_client_sessions_size_max;
    size -= constants.superblock_copies * superblock_trailer_manifest_size_max;
    size -= constants.journal_size; // Zone.wal_headers + Zone.wal_prepares
    size -= constants.client_replies_size; // Zone.client_replies
    // At this point, the remainder of size is split between the grid and the freeset copies.
    // The size of a freeset is related to the number of blocks it must store.
    // Maximize the number of grid blocks.

    var shard_count = @divFloor(size, constants.block_size * SuperBlockFreeSet.shard_bits);
    while (true) : (shard_count -= 1) {
        const block_count = shard_count * SuperBlockFreeSet.shard_bits;
        const grid_size = block_count * constants.block_size;
        const free_set_size = vsr.sector_ceil(SuperBlockFreeSet.encode_size_max(block_count));
        const free_sets_size = constants.superblock_copies * free_set_size;
        if (free_sets_size + grid_size <= size) break;
    }
    break :blk shard_count * SuperBlockFreeSet.shard_bits;
};

comptime {
    assert(grid_blocks_max > 0);
    assert(grid_blocks_max * constants.block_size + data_file_size_min <= constants.storage_size_max);
}

/// This table shows the sequence number progression of the SuperBlock's headers.
///
/// action        working  staging  disk
/// format        seq      seq      seq
///                0                 -        Initially the file has no headers.
///                0        1        -
///                0        1        1        Write a copyset for the first sequence.
///                1        1        1        Read quorum; verify 3/4 are valid.
///
/// open          seq      seq      seq
///                                 a
///               a                 a         Read quorum; verify 2/4 are valid.
///               a        (a)      a         Repair any broken copies of `a`.
///
/// checkpoint    seq      seq      seq
/// (or sync)     a        a        a
///               a        a+1
///               a        a+1      a+1
///               a+1      a+1      a+1       Read quorum; verify 3/4 are valid.
///
/// view_change   seq      seq      seq
///               a                 a
///               a        a+1      a         The new sequence reuses the original parent.
///               a        a+1      a+1
///               a+1      a+1      a+1       Read quorum; verify 3/4 are valid.
///               working  staging  disk
///
pub fn SuperBlockType(comptime Storage: type) type {
    return struct {
        const SuperBlock = @This();

        pub const Manifest = SuperBlockManifest;
        pub const FreeSet = SuperBlockFreeSet;
        pub const ClientSessions = SuperBlockClientSessions;

        pub const Context = struct {
            superblock: *SuperBlock,
            callback: *const fn (context: *Context) void,
            caller: Caller,

            trailer: ?Trailer = null,
            write: Storage.Write = undefined,
            read: Storage.Read = undefined,
            read_threshold: ?Quorums.Threshold = null,
            copy: ?u8 = null,
            /// Used by format(), checkpoint(), view_change(), sync().
            vsr_state: ?SuperBlockHeader.VSRState = null,
            /// Used by format() and view_change().
            vsr_headers: ?vsr.Headers.ViewChangeArray = null,
            repairs: ?Quorums.RepairIterator = null, // Used by open().
        };

        storage: *Storage,

        /// The first logical offset that may be written to the superblock storage zone.
        storage_offset: u64 = 0,

        /// The total size of the superblock storage zone after this physical offset.
        storage_size: u64 = superblock_zone_size,

        /// The superblock that was recovered at startup after a crash or that was last written.
        working: *align(constants.sector_size) SuperBlockHeader,

        /// The superblock that will replace the current working superblock once written.
        /// We cannot mutate any working state directly until it is safely on stable storage.
        /// Otherwise, we may accidentally externalize guarantees that are not yet durable.
        staging: *align(constants.sector_size) SuperBlockHeader,

        /// The copies that we read into at startup or when verifying the written superblock.
        reading: []align(constants.sector_size) SuperBlockHeader,

        /// It might seem that, at startup, we simply install the copy with the highest sequence.
        ///
        /// However, there's a scenario where:
        /// 1. We are able to write sequence 7 to 3/4 copies, with the last write being lost.
        /// 2. We startup and read all copies, with reads misdirected to the copy with sequence 6.
        ///
        /// Another scenario:
        /// 1. We begin to write sequence 7 to 1 copy and then crash.
        /// 2. At startup, the read to this copy fails, and we recover at sequence=6.
        /// 3. We then checkpoint another sequence 7 to 3/4 copies and crash.
        /// 4. At startup, we then see 4 copies with the same sequence with 1 checksum different.
        ///
        /// To mitigate these scenarios, we ensure that we are able to read a quorum of copies.
        /// This also gives us confidence that our working superblock has sufficient redundancy.
        quorums: Quorums = Quorums{},

        /// Staging trailers. These are modified between checkpoints, and are persisted on
        /// checkpoint and sync.
        manifest: Manifest,
        free_set: FreeSet,
        client_sessions: ClientSessions,

        /// Updated when:
        /// - the trailer is serialized immediately before checkpoint or sync, and
        /// - used to construct the trailer during state sync.
        manifest_buffer: []align(constants.sector_size) u8,
        free_set_buffer: []align(constants.sector_size) u8,
        client_sessions_buffer: []align(constants.sector_size) u8,

        /// Whether the superblock has been opened. An open superblock may not be formatted.
        opened: bool = false,
        block_count_limit: usize,
        storage_size_limit: u64,

        /// There may only be a single caller queued at a time, to ensure that the VSR protocol is
        /// careful to submit at most one view change at a time.
        queue_head: ?*Context = null,
        queue_tail: ?*Context = null,

        /// Set to non-null after open().
        /// Used for logging.
        replica_index: ?u8 = null,

        pub const Options = struct {
            storage: *Storage,
            storage_size_limit: u64,
        };

        pub fn init(allocator: mem.Allocator, options: Options) !SuperBlock {
            assert(options.storage_size_limit >= data_file_size_min);
            assert(options.storage_size_limit <= constants.storage_size_max);
            assert(options.storage_size_limit % constants.sector_size == 0);

            const shard_count_limit = @as(usize, @intCast(@divFloor(
                options.storage_size_limit - data_file_size_min,
                constants.block_size * FreeSet.shard_bits,
            )));
            const block_count_limit = shard_count_limit * FreeSet.shard_bits;
            assert(block_count_limit <= grid_blocks_max);

            const a = try allocator.alignedAlloc(SuperBlockHeader, constants.sector_size, 1);
            errdefer allocator.free(a);

            const b = try allocator.alignedAlloc(SuperBlockHeader, constants.sector_size, 1);
            errdefer allocator.free(b);

            const reading = try allocator.alignedAlloc(
                [constants.superblock_copies]SuperBlockHeader,
                constants.sector_size,
                1,
            );
            errdefer allocator.free(reading);

            var manifest = try Manifest.init(
                allocator,
                @divExact(
                    superblock_trailer_manifest_size_max,
                    Manifest.BlockReferenceSize,
                ),
                @import("../lsm/tree.zig").table_count_max,
            );
            errdefer manifest.deinit(allocator);

            var free_set = try FreeSet.init(allocator, block_count_limit);
            errdefer free_set.deinit(allocator);

            var client_sessions = try ClientSessions.init(allocator);
            errdefer client_sessions.deinit(allocator);

            const manifest_buffer = try allocator.alignedAlloc(
                u8,
                constants.sector_size,
                superblock_trailer_manifest_size_max,
            );
            errdefer allocator.free(manifest_buffer);

            const free_set_buffer = try allocator.alignedAlloc(
                u8,
                constants.sector_size,
                SuperBlockFreeSet.encode_size_max(block_count_limit),
            );
            errdefer allocator.free(free_set_buffer);

            const client_sessions_buffer = try allocator.alignedAlloc(
                u8,
                constants.sector_size,
                superblock_trailer_client_sessions_size_max,
            );
            errdefer allocator.free(client_sessions_buffer);

            return SuperBlock{
                .storage = options.storage,
                .working = &a[0],
                .staging = &b[0],
                .reading = &reading[0],
                .manifest = manifest,
                .free_set = free_set,
                .client_sessions = client_sessions,
                .manifest_buffer = manifest_buffer,
                .free_set_buffer = free_set_buffer,
                .client_sessions_buffer = client_sessions_buffer,
                .block_count_limit = block_count_limit,
                .storage_size_limit = options.storage_size_limit,
            };
        }

        pub fn deinit(superblock: *SuperBlock, allocator: mem.Allocator) void {
            allocator.destroy(superblock.working);
            allocator.destroy(superblock.staging);
            allocator.free(superblock.reading);

            superblock.manifest.deinit(allocator);
            superblock.free_set.deinit(allocator);
            superblock.client_sessions.deinit(allocator);

            allocator.free(superblock.manifest_buffer);
            allocator.free(superblock.free_set_buffer);
            allocator.free(superblock.client_sessions_buffer);
        }

        pub const FormatOptions = struct {
            cluster: u32,
            replica: u8,
            replica_count: u8,
        };

        pub fn format(
            superblock: *SuperBlock,
            callback: *const fn (context: *Context) void,
            context: *Context,
            options: FormatOptions,
        ) void {
            assert(!superblock.opened);
            assert(superblock.replica_index == null);

            assert(options.replica_count > 0);
            assert(options.replica_count <= constants.replicas_max);
            assert(options.replica < options.replica_count + constants.standbys_max);

            const members = vsr.root_members(options.cluster);
            const replica_id = members[options.replica];

            superblock.replica_index = vsr.member_index(&members, replica_id);

            // This working copy provides the parent checksum, and will not be written to disk.
            // We therefore use zero values to make this parent checksum as stable as possible.
            superblock.working.* = .{
                .copy = 0,
                .version = SuperBlockVersion,
                .sequence = 0,
                .cluster = options.cluster,
                .storage_size = 0,
                .storage_size_max = constants.storage_size_max,
                .parent = 0,
                .manifest_checksum = 0,
                .free_set_checksum = 0,
                .client_sessions_checksum = 0,
                .vsr_state = .{
                    .previous_checkpoint_id = 0,
                    .commit_min_checksum = 0,
                    .replica_id = replica_id,
                    .members = members,
                    .commit_min_canonical = 0,
                    .commit_min = 0,
                    .commit_max = 0,
                    .sync_op_min = 0,
                    .sync_op_max = 0,
                    .log_view = 0,
                    .view = 0,
                    .replica_count = options.replica_count,
                },
                .snapshots = undefined,
                .manifest_size = 0,
                .free_set_size = 0,
                .client_sessions_size = 0,
                .vsr_headers_count = 0,
                .vsr_headers_all = mem.zeroes([constants.view_change_headers_max]vsr.Header),
            };

            @memset(&superblock.working.snapshots, .{
                .created = 0,
                .queried = 0,
                .timeout = 0,
            });

            superblock.working.set_checksum();

            context.* = .{
                .superblock = superblock,
                .callback = callback,
                .caller = .format,
                .vsr_state = SuperBlockHeader.VSRState.root(.{
                    .cluster = options.cluster,
                    .replica_id = replica_id,
                    .members = members,
                    .replica_count = options.replica_count,
                }),
                .vsr_headers = vsr.Headers.ViewChangeArray.root(options.cluster),
            };

            // TODO At a higher layer, we must:
            // 1. verify that there is no valid superblock, and
            // 2. zero the superblock, WAL and client table to ensure storage determinism.

            superblock.acquire(context);
        }

        pub fn open(
            superblock: *SuperBlock,
            callback: *const fn (context: *Context) void,
            context: *Context,
        ) void {
            assert(!superblock.opened);

            context.* = .{
                .superblock = superblock,
                .callback = callback,
                .caller = .open,
            };

            superblock.acquire(context);
        }

        const UpdateCheckpoint = struct {
            commit_min_checksum: u128,
            commit_min: u64,
            commit_max: u64,
            sync_op_min: u64,
            sync_op_max: u64,
        };

        /// Must update the commit_min and commit_min_checksum.
        pub fn checkpoint(
            superblock: *SuperBlock,
            callback: *const fn (context: *Context) void,
            context: *Context,
            update: UpdateCheckpoint,
        ) void {
            assert(superblock.opened);
            assert(update.commit_min <= update.commit_max);
            assert(update.commit_min > superblock.staging.vsr_state.commit_min);
            assert(update.commit_min_checksum != superblock.staging.vsr_state.commit_min_checksum);
            assert(update.sync_op_min <= update.sync_op_max);

            var vsr_state = superblock.staging.vsr_state;
            vsr_state.commit_min_checksum = update.commit_min_checksum;
            vsr_state.commit_min_canonical = vsr_state.commit_min;
            vsr_state.commit_min = update.commit_min;
            vsr_state.commit_max = update.commit_max;
            vsr_state.sync_op_min = update.sync_op_min;
            vsr_state.sync_op_max = update.sync_op_max;
            vsr_state.previous_checkpoint_id = superblock.staging.checkpoint_id();
            assert(superblock.staging.vsr_state.would_be_updated_by(vsr_state));

            context.* = .{
                .superblock = superblock,
                .callback = callback,
                .caller = .checkpoint,
                .vsr_state = vsr_state,
            };
            superblock.log_context(context);
            superblock.acquire(context);
        }

        const UpdateViewChange = struct {
            commit_max: u64,
            log_view: u32,
            view: u32,
            headers: *const vsr.Headers.ViewChangeArray,
        };

        /// The replica calls view_change() to persist its view/log_view — it cannot
        /// advertise either value until it is certain they will never backtrack.
        ///
        /// The update must advance view/log_view (monotonically increasing).
        pub fn view_change(
            superblock: *SuperBlock,
            callback: *const fn (context: *Context) void,
            context: *Context,
            update: UpdateViewChange,
        ) void {
            assert(superblock.opened);
            assert(superblock.staging.vsr_state.commit_max <= update.commit_max);
            assert(superblock.staging.vsr_state.view <= update.view);
            assert(superblock.staging.vsr_state.log_view <= update.log_view);
            assert(superblock.staging.vsr_state.log_view < update.log_view or
                superblock.staging.vsr_state.view < update.view);
            // Usually the op-head is ahead of the commit_min. But this may not be the case, due to
            // state sync that ran after the view_durable_update was queued, but before it executed.
            maybe(superblock.staging.vsr_state.commit_min > update.headers.array.get(0).op); //  TODO???

            update.headers.verify();
            assert(update.view >= update.log_view);

            var vsr_state = superblock.staging.vsr_state;
            vsr_state.commit_max = update.commit_max;
            vsr_state.log_view = update.log_view;
            vsr_state.view = update.view;
            assert(superblock.staging.vsr_state.would_be_updated_by(vsr_state));

            context.* = .{
                .superblock = superblock,
                .callback = callback,
                .caller = .view_change,
                .vsr_state = vsr_state,
                .vsr_headers = update.headers.*,
            };
            superblock.log_context(context);
            superblock.acquire(context);
        }

        const UpdateSync = struct {
            previous_checkpoint_id: u128,
            commit_min_checksum: u128,
            commit_min: u64,
            commit_max: u64,
            sync_op_min: u64,
            sync_op_max: u64,
        };

        pub fn sync(
            superblock: *SuperBlock,
            callback: *const fn (context: *Context) void,
            context: *Context,
            update: UpdateSync,
        ) void {
            assert(superblock.opened);
            assert(update.commit_min >= superblock.staging.vsr_state.commit_min);
            assert(update.commit_min <= update.commit_max);
            assert((update.commit_min == superblock.staging.vsr_state.commit_min) ==
                (update.commit_min_checksum == superblock.staging.vsr_state.commit_min_checksum));
            assert(update.sync_op_min <= update.sync_op_max);
            assert(update.sync_op_max > update.commit_min);

            var vsr_state = superblock.staging.vsr_state;
            vsr_state.previous_checkpoint_id = update.previous_checkpoint_id;
            vsr_state.commit_min_checksum = update.commit_min_checksum;
            vsr_state.commit_min = update.commit_min;
            vsr_state.commit_max = update.commit_max;
            vsr_state.sync_op_min = update.sync_op_min;
            vsr_state.sync_op_max = update.sync_op_max;

            // VSRState is usually updated, but not if we are syncing to the same checkpoint op
            // (i.e. if we are a divergent replica trying).
            maybe(superblock.staging.vsr_state.would_be_updated_by(vsr_state));

            context.* = .{
                .superblock = superblock,
                .callback = callback,
                .caller = .sync,
                .vsr_state = vsr_state,
            };
            superblock.log_context(context);
            superblock.acquire(context);
        }

        pub fn updating(superblock: *const SuperBlock, caller: Caller) bool {
            assert(superblock.opened);

            if (superblock.queue_head) |head| {
                if (head.caller == caller) return true;
            }

            if (superblock.queue_tail) |tail| {
                if (tail.caller == caller) return true;
            }

            return false;
        }

        fn write_staging(superblock: *SuperBlock, context: *Context) void {
            assert(context.caller != .open);
            assert(context.caller == .format or superblock.opened);
            assert(context.copy == null);
            context.vsr_state.?.assert_internally_consistent();
            assert(superblock.queue_head == context);
            assert(superblock.queue_tail == null);

            superblock.staging.* = superblock.working.*;
            superblock.staging.sequence = superblock.staging.sequence + 1;
            superblock.staging.parent = superblock.staging.checksum;
            superblock.staging.vsr_state = context.vsr_state.?;

            if (context.vsr_headers) |*headers| {
                assert(context.caller.updates_vsr_headers());

                superblock.staging.vsr_headers_count = headers.array.count_as(u32);
                stdx.copy_disjoint(
                    .exact,
                    vsr.Header,
                    superblock.staging.vsr_headers_all[0..headers.array.count()],
                    headers.array.const_slice(),
                );
                @memset(
                    superblock.staging.vsr_headers_all[headers.array.count()..],
                    std.mem.zeroes(vsr.Header),
                );
            } else {
                assert(!context.caller.updates_vsr_headers());
            }

            if (context.caller.updates_trailers()) {
                superblock.write_staging_encode_manifest();
                superblock.write_staging_encode_free_set();
                superblock.write_staging_encode_client_sessions();
            }
            superblock.staging.set_checksum();

            context.copy = 0;
            if (context.caller.updates_trailers()) {
                superblock.write_trailers(context);
            } else {
                superblock.write_header(context);
            }
        }

        fn write_staging_encode_manifest(superblock: *SuperBlock) void {
            const staging: *SuperBlockHeader = superblock.staging;
            const target = superblock.manifest_buffer;

            staging.manifest_size = @as(u32, @intCast(superblock.manifest.encode(target)));
            staging.manifest_checksum = vsr.checksum(target[0..staging.manifest_size]);
        }

        fn write_staging_encode_free_set(superblock: *SuperBlock) void {
            const staging: *SuperBlockHeader = superblock.staging;
            const encode_size_max = FreeSet.encode_size_max(superblock.block_count_limit);
            const target = superblock.free_set_buffer[0..encode_size_max];

            superblock.free_set.include_staging();
            defer superblock.free_set.exclude_staging();

            superblock.verify_manifest_blocks_are_acquired_in_free_set();

            staging.storage_size = data_file_size_min;

            if (superblock.free_set.highest_address_acquired()) |address| {
                staging.storage_size += address * constants.block_size;
            }
            assert(staging.storage_size >= data_file_size_min);
            assert(staging.storage_size <= staging.storage_size_max);
            assert(staging.storage_size <= superblock.storage_size_limit);

            if (superblock.free_set.count_acquired() == 0) {
                // EWAH encodes a zero-length bitset to an empty slice anyway, but handle this
                // condition separately so that during formatting it doesn't depend on the choice
                // of storage_size_limit.
                staging.free_set_size = 0;
            } else {
                staging.free_set_size = @as(u32, @intCast(superblock.free_set.encode(target)));
            }
            staging.free_set_checksum = vsr.checksum(target[0..staging.free_set_size]);
        }

        fn write_staging_encode_client_sessions(superblock: *SuperBlock) void {
            const staging: *SuperBlockHeader = superblock.staging;
            const target = superblock.client_sessions_buffer;

            staging.client_sessions_size = @as(u32, @intCast(superblock.client_sessions.encode(target)));
            staging.client_sessions_checksum = vsr.checksum(target[0..staging.client_sessions_size]);

            assert(staging.client_sessions_size == ClientSessions.encode_size_max);
        }

        /// Write each trailer in sequence (for the current copy).
        fn write_trailers(superblock: *SuperBlock, context: *Context) void {
            assert(superblock.queue_head == context);

            context.trailer = trailer_next: {
                if (context.trailer) |trailer_previous| {
                    break :trailer_next switch (trailer_previous) {
                        .manifest => Trailer.free_set,
                        .free_set => Trailer.client_sessions,
                        .client_sessions => null,
                    };
                } else {
                    break :trailer_next Trailer.manifest;
                }
            };

            if (context.trailer) |_| {
                context.superblock.write_trailer_next(context);
            } else {
                context.superblock.write_header(context);
            }
        }

        fn write_trailer_next(superblock: *SuperBlock, context: *Context) void {
            assert(superblock.queue_head == context);
            assert(context.trailer != null);

            const trailer = context.trailer.?;
            const trailer_buffer_all = superblock.trailer_buffer(trailer);
            const trailer_size_ = superblock.staging.trailer_size(trailer);
            const trailer_checksum_ = superblock.staging.trailer_checksum(trailer);

            switch (trailer) {
                .manifest => assert(trailer_size_ % SuperBlockManifest.BlockReferenceSize == 0),
                .free_set => assert(trailer_size_ % @sizeOf(u64) == 0),
                .client_sessions => assert(trailer_size_ % (@sizeOf(vsr.Header) + @sizeOf(u64)) == 0),
            }

            const trailer_size_ceil = vsr.sector_ceil(trailer_size_);
            assert(trailer_size_ceil <= trailer.zone().size_max());

            const buffer = trailer_buffer_all[0..trailer_size_ceil];
            assert(trailer_checksum_ == vsr.checksum(buffer[0..trailer_size_]));

            @memset(buffer[trailer_size_..], 0); // Zero sector padding.

            const offset = trailer.zone().start_for_copy(context.copy.?);
            log.debug("{?}: {s}: write_trailer_next: " ++
                "trailer={s} copy={} checksum={x:0>32} size={} offset={}", .{
                superblock.replica_index,
                @tagName(context.caller),
                @tagName(trailer),
                context.copy.?,
                trailer_checksum_,
                trailer_size_,
                offset,
            });

            superblock.assert_bounds(offset, buffer.len);

            if (buffer.len == 0) {
                write_trailer_callback(&context.write);
                return;
            }

            superblock.storage.write_sectors(
                write_trailer_callback,
                &context.write,
                buffer,
                .superblock,
                offset,
            );
        }

        fn write_trailer_callback(write: *Storage.Write) void {
            const context = @fieldParentPtr(Context, "write", write);
            assert(context.trailer != null);

            context.superblock.write_trailers(context);
        }

        fn write_header(superblock: *SuperBlock, context: *Context) void {
            assert(superblock.queue_head == context);

            // We update the working superblock for a checkpoint/format/view_change:
            // open() does not update the working superblock, since it only writes to repair.
            if (context.caller == .open) {
                assert(superblock.staging.sequence == superblock.working.sequence);
            } else {
                assert(superblock.staging.sequence == superblock.working.sequence + 1);
                assert(superblock.staging.parent == superblock.working.checksum);
            }

            // The superblock cluster and replica should never change once formatted:
            assert(superblock.staging.cluster == superblock.working.cluster);
            assert(superblock.staging.vsr_state.replica_id == superblock.working.vsr_state.replica_id);

            assert(superblock.staging.storage_size >= data_file_size_min);
            assert(superblock.staging.storage_size <= superblock.staging.storage_size_max);

            assert(context.copy.? < constants.superblock_copies);
            superblock.staging.copy = context.copy.?;

            // Updating the copy number should not affect the checksum, which was previously set:
            assert(superblock.staging.valid_checksum());

            const buffer = mem.asBytes(superblock.staging);
            const offset = SuperBlockZone.header.start_for_copy(context.copy.?);

            log.debug("{?}: {s}: write_header: checksum={x:0>32} sequence={} copy={} size={} offset={}", .{
                superblock.replica_index,
                @tagName(context.caller),
                superblock.staging.checksum,
                superblock.staging.sequence,
                context.copy.?,
                buffer.len,
                offset,
            });

            superblock.assert_bounds(offset, buffer.len);

            superblock.storage.write_sectors(
                write_header_callback,
                &context.write,
                buffer,
                .superblock,
                offset,
            );
        }

        fn write_header_callback(write: *Storage.Write) void {
            const context = @fieldParentPtr(Context, "write", write);
            const superblock = context.superblock;
            const copy = context.copy.?;

            assert(superblock.queue_head == context);

            assert(copy < constants.superblock_copies);
            assert(copy == superblock.staging.copy);

            if (context.caller == .open) {
                context.copy = null;
                superblock.repair(context);
                return;
            }

            if (copy + 1 == constants.superblock_copies) {
                context.copy = null;
                superblock.read_working(context, .verify);
            } else {
                context.copy = copy + 1;

                if (context.caller.updates_trailers()) {
                    superblock.write_trailers(context);
                } else {
                    superblock.write_header(context);
                }
            }
        }

        fn read_working(
            superblock: *SuperBlock,
            context: *Context,
            threshold: Quorums.Threshold,
        ) void {
            assert(superblock.queue_head == context);
            assert(context.copy == null);
            assert(context.read_threshold == null);

            // We do not submit reads in parallel, as while this would shave off 1ms, it would also
            // increase the risk that a single fault applies to more reads due to temporal locality.
            // This would make verification reads more flaky when we do experience a read fault.
            // See "An Analysis of Data Corruption in the Storage Stack".

            context.copy = 0;
            context.read_threshold = threshold;
            for (superblock.reading) |*copy| copy.* = undefined;
            superblock.read_header(context);
        }

        fn read_header(superblock: *SuperBlock, context: *Context) void {
            assert(superblock.queue_head == context);
            assert(context.copy.? < constants.superblock_copies);
            assert(context.read_threshold != null);

            const buffer = mem.asBytes(&superblock.reading[context.copy.?]);
            const offset = SuperBlockZone.header.start_for_copy(context.copy.?);

            log.debug("{?}: {s}: read_header: copy={} size={} offset={}", .{
                superblock.replica_index,
                @tagName(context.caller),
                context.copy.?,
                buffer.len,
                offset,
            });

            superblock.assert_bounds(offset, buffer.len);

            superblock.storage.read_sectors(
                read_header_callback,
                &context.read,
                buffer,
                .superblock,
                offset,
            );
        }

        fn read_header_callback(read: *Storage.Read) void {
            const context = @fieldParentPtr(Context, "read", read);
            const superblock = context.superblock;
            const threshold = context.read_threshold.?;

            assert(superblock.queue_head == context);

            assert(context.copy.? < constants.superblock_copies);
            if (context.copy.? + 1 != constants.superblock_copies) {
                context.copy = context.copy.? + 1;
                superblock.read_header(context);
                return;
            }

            context.read_threshold = null;
            context.copy = null;

            if (superblock.quorums.working(superblock.reading, threshold)) |quorum| {
                assert(quorum.valid);
                assert(quorum.copies.count() >= threshold.count());
                assert(quorum.header.storage_size_max == constants.storage_size_max);

                const working = quorum.header;
                if (threshold == .verify) {
                    if (working.checksum != superblock.staging.checksum) {
                        @panic("superblock failed verification after writing");
                    }
                    assert(working.equal(superblock.staging));
                }

                if (context.caller == .format) {
                    assert(working.sequence == 1);
                    assert(working.storage_size == data_file_size_min);
                    assert(working.manifest_size == 0);
                    assert(working.free_set_size == 0);
                    assert(working.client_sessions_size == ClientSessions.encode_size_max);
                    assert(working.vsr_state.commit_min_checksum ==
                        vsr.Header.root_prepare(working.cluster).checksum);
                    assert(working.vsr_state.commit_min == 0);
                    assert(working.vsr_state.commit_max == 0);
                    assert(working.vsr_state.log_view == 0);
                    assert(working.vsr_state.view == 0);
                    assert(working.vsr_headers_count == 1);

                    assert(working.vsr_state.replica_count <= constants.replicas_max);
                    assert(vsr.member_index(
                        &working.vsr_state.members,
                        working.vsr_state.replica_id,
                    ) != null);
                } else if (context.caller == .checkpoint) {
                    superblock.free_set.checkpoint();
                }

                superblock.working.* = working.*;
                superblock.staging.* = working.*;

                log.debug(
                    "{[replica]?}: " ++
                        "{[caller]s}: installed working superblock: checksum={[checksum]x:0>32} " ++
                        "sequence={[sequence]} cluster={[cluster]} replica_id={[replica_id]} " ++
                        "size={[size]} checkpoint_id={[checkpoint_id]x:0>32} " ++
                        "manifest_checksum={[manifest_checksum]x:0>32} " ++
                        "free_set_checksum={[free_set_checksum]x:0>32} " ++
                        "client_sessions_checksum={[client_sessions_checksum]x:0>32} " ++
                        "commit_min_checksum={[commit_min_checksum]} commit_min={[commit_min]} " ++
                        "commit_max={[commit_max]} log_view={[log_view]} view={[view]}",
                    .{
                        .replica = superblock.replica_index,
                        .caller = @tagName(context.caller),
                        .checksum = superblock.working.checksum,
                        .sequence = superblock.working.sequence,
                        .cluster = superblock.working.cluster,
                        .replica_id = superblock.working.vsr_state.replica_id,
                        .size = superblock.working.storage_size,
                        .checkpoint_id = superblock.working.checkpoint_id(),
                        .manifest_checksum = superblock.working.manifest_checksum,
                        .free_set_checksum = superblock.working.free_set_checksum,
                        .client_sessions_checksum = superblock.working.client_sessions_checksum,
                        .commit_min_checksum = superblock.working.vsr_state.commit_min_checksum,
                        .commit_min = superblock.working.vsr_state.commit_min,
                        .commit_max = superblock.working.vsr_state.commit_max,
                        .log_view = superblock.working.vsr_state.log_view,
                        .view = superblock.working.vsr_state.view,
                    },
                );
                for (superblock.working.vsr_headers().slice) |*header| {
                    log.debug("{?}: {s}: vsr_header: op={} checksum={}", .{
                        superblock.replica_index,
                        @tagName(context.caller),
                        header.op,
                        header.checksum,
                    });
                }

                if (context.caller == .open) {
                    if (context.repairs) |_| {
                        // We just verified that the repair completed.
                        assert(threshold == .verify);
                        superblock.release(context);
                    } else {
                        assert(threshold == .open);
                        context.copy = 0;
                        context.repairs = quorum.repairs();
                        superblock.read_trailer(context, .manifest);
                    }
                } else {
                    // TODO Consider calling TRIM() on Grid's free suffix after checkpointing.
                    superblock.release(context);
                }
            } else |err| switch (err) {
                error.Fork => @panic("superblock forked"),
                error.NotFound => @panic("superblock not found"),
                error.QuorumLost => @panic("superblock quorum lost"),
                error.ParentNotConnected => @panic("superblock parent not connected"),
                error.ParentSkipped => @panic("superblock parent superseded"),
                error.VSRStateNotMonotonic => @panic("superblock vsr state not monotonic"),
            }
        }

        fn read_trailer(superblock: *SuperBlock, context: *Context, trailer: Trailer) void {
            assert(context.caller == .open);
            assert(superblock.queue_head == context);
            assert(context.copy.? < constants.superblock_copies);
            assert(context.trailer == null);

            context.trailer = trailer;

            const trailer_buffer_all = superblock.trailer_buffer(trailer);
            const trailer_size_ = superblock.working.trailer_size(trailer);
            const trailer_size_ceil = vsr.sector_ceil(trailer_size_);
            assert(trailer_size_ceil <= trailer.zone().size_max());

            const buffer = trailer_buffer_all[0..trailer_size_ceil];
            const offset = trailer.zone().start_for_copy(context.copy.?);

            log.debug("{?}: {s}: read_trailer: trailer={s} copy={} size={} offset={}", .{
                superblock.replica_index,
                @tagName(context.caller),
                @tagName(trailer),
                context.copy.?,
                buffer.len,
                offset,
            });

            superblock.assert_bounds(offset, buffer.len);

            if (buffer.len == 0) {
                read_trailer_callback(&context.read);
                return;
            }

            superblock.storage.read_sectors(
                read_trailer_callback,
                &context.read,
                buffer,
                .superblock,
                offset,
            );
        }

        fn read_trailer_callback(read: *Storage.Read) void {
            const context = @fieldParentPtr(Context, "read", read);
            const superblock = context.superblock;
            const copy = context.copy.?;
            const trailer = context.trailer.?;

            assert(context.caller == .open);
            assert(superblock.queue_head == context);
            assert(!superblock.opened);

            context.trailer = null;

            const trailer_checksum_expected = superblock.working.trailer_checksum(trailer);
            const trailer_checksum_computed = vsr.checksum(
                superblock.trailer_buffer(trailer)[0..superblock.working.trailer_size(trailer)],
            );

            if (trailer_checksum_computed == trailer_checksum_expected) {
                log.debug("{?}: open: read_trailer: {s}: ok (copy={})", .{
                    superblock.replica_index,
                    @tagName(trailer),
                    copy,
                });

                switch (trailer) {
                    .manifest => {
                        context.copy = 0;
                        superblock.read_trailer(context, .free_set);
                        return;
                    },
                    .free_set => {
                        context.copy = 0;
                        superblock.read_trailer(context, .client_sessions);
                        return;
                    },
                    .client_sessions => {},
                }

                assert(superblock.manifest.count == 0);
                assert(superblock.free_set.count_acquired() == 0);
                assert(superblock.client_sessions.count() == 0);

                const working: *const SuperBlockHeader = superblock.working;
                superblock.manifest.decode(superblock.manifest_buffer[0..working.manifest_size]);
                superblock.free_set.decode(superblock.free_set_buffer[0..working.free_set_size]);
                superblock.client_sessions.decode(superblock.client_sessions_buffer[0..working.client_sessions_size]);

                superblock.verify_manifest_blocks_are_acquired_in_free_set();

                log.debug("{?}: open: read_trailer: manifest: blocks: {}/{}", .{
                    superblock.replica_index,
                    superblock.manifest.count,
                    superblock.manifest.count_max,
                });
                log.debug("{?}: open: read_trailer: free_set: acquired blocks: {}/{}/{}", .{
                    superblock.replica_index,
                    superblock.free_set.count_acquired(),
                    superblock.block_count_limit,
                    grid_blocks_max,
                });
                log.debug("{?}: open: read_trailer: client_sessions: client requests: {}/{}", .{
                    superblock.replica_index,
                    superblock.client_sessions.count(),
                    constants.clients_max,
                });

                // TODO Repair any impaired (trailer) copies before we continue.
                // At present, we repair at the next checkpoint.
                // We do not repair padding.
                context.copy = null;
                superblock.repair(context);
            } else {
                log.debug("{?}: open: read_trailer: {}: corrupt (copy={})", .{
                    superblock.replica_index,
                    trailer,
                    copy,
                });
                if (copy + 1 == constants.superblock_copies) {
                    std.debug.panic("superblock {s} lost", .{@tagName(trailer)});
                } else {
                    context.copy = copy + 1;
                    superblock.read_trailer(context, trailer);
                }
            }
        }

        fn verify_manifest_blocks_are_acquired_in_free_set(superblock: *SuperBlock) void {
            assert(superblock.manifest.count <= superblock.free_set.count_acquired());
            for (superblock.manifest.addresses[0..superblock.manifest.count]) |address| {
                assert(!superblock.free_set.is_free(address));
            }
        }

        fn repair(superblock: *SuperBlock, context: *Context) void {
            assert(context.caller == .open);
            assert(context.copy == null);
            assert(superblock.queue_head == context);

            if (context.repairs.?.next()) |repair_copy| {
                context.copy = repair_copy;
                log.warn("{?}: repair: copy={}", .{ superblock.replica_index, repair_copy });

                superblock.staging.* = superblock.working.*;
                superblock.write_trailers(context);
            } else {
                superblock.release(context);
            }
        }

        fn acquire(superblock: *SuperBlock, context: *Context) void {
            if (superblock.queue_head) |head| {
                // All operations are mutually exclusive with themselves.
                assert(head.caller != context.caller);
                assert(Caller.transitions.get(head.caller).?.contains(context.caller));
                assert(superblock.queue_tail == null);

                log.debug("{?}: {s}: enqueued after {s}", .{
                    superblock.replica_index,
                    @tagName(context.caller),
                    @tagName(head.caller),
                });

                superblock.queue_tail = context;
            } else {
                assert(superblock.queue_tail == null);

                superblock.queue_head = context;
                log.debug("{?}: {s}: started", .{
                    superblock.replica_index,
                    @tagName(context.caller),
                });

                if (Storage == @import("../testing/storage.zig").Storage) {
                    // We should have finished all pending superblock io before starting any more.
                    superblock.storage.assert_no_pending_reads(.superblock);
                    superblock.storage.assert_no_pending_writes(.superblock);
                    if (context.caller != .view_change) {
                        superblock.storage.assert_no_pending_writes(.grid);
                        // (Pending repair-reads are possible.)
                    }
                }

                if (context.caller == .open) {
                    superblock.read_working(context, .open);
                } else {
                    superblock.write_staging(context);
                }
            }
        }

        fn release(superblock: *SuperBlock, context: *Context) void {
            assert(superblock.queue_head == context);

            log.debug("{?}: {s}: complete", .{ superblock.replica_index, @tagName(context.caller) });

            if (Storage == @import("../testing/storage.zig").Storage) {
                // We should have finished all pending io by now.
                superblock.storage.assert_no_pending_reads(.superblock);
                superblock.storage.assert_no_pending_writes(.superblock);
            }

            switch (context.caller) {
                .format => {},
                .open => {
                    assert(!superblock.opened);
                    superblock.opened = true;
                    superblock.replica_index = vsr.member_index(
                        &superblock.working.vsr_state.members,
                        superblock.working.vsr_state.replica_id,
                    ).?;

                    if (superblock.working.manifest_size > 0) {
                        assert(superblock.manifest.count > 0);
                    }
                    // TODO Make the FreeSet encoding format not dependant on the word size.
                    if (superblock.working.free_set_size > @sizeOf(usize)) {
                        assert(superblock.free_set.count_acquired() > 0);
                    }
                },
                .checkpoint,
                .view_change,
                .sync,
                => {
                    assert(stdx.equal_bytes(
                        SuperBlockHeader.VSRState,
                        &superblock.staging.vsr_state,
                        &context.vsr_state.?,
                    ));
                    assert(stdx.equal_bytes(
                        SuperBlockHeader.VSRState,
                        &superblock.working.vsr_state,
                        &context.vsr_state.?,
                    ));
                },
            }

            const queue_tail = superblock.queue_tail;
            superblock.queue_head = null;
            superblock.queue_tail = null;
            if (queue_tail) |tail| superblock.acquire(tail);

            context.callback(context);
        }

        fn assert_bounds(superblock: *SuperBlock, offset: u64, size: u64) void {
            assert(offset >= superblock.storage_offset);
            assert(offset + size <= superblock.storage_offset + superblock.storage_size);
        }

        /// We use flexible quorums for even quorums with write quorum > read quorum, for example:
        /// * When writing, we must verify that at least 3/4 copies were written.
        /// * At startup, we must verify that at least 2/4 copies were read.
        ///
        /// This ensures that our read and write quorums will intersect.
        /// Using flexible quorums in this way increases resiliency of the superblock.
        fn threshold_for_caller(caller: Caller) u8 {
            // Working these threshold out by formula is easy to get wrong, so enumerate them:
            // The rule is that the write quorum plus the read quorum must be exactly copies + 1.

            return switch (caller) {
                .format,
                .checkpoint,
                .view_change,
                .sync,
                => switch (constants.superblock_copies) {
                    4 => 3,
                    6 => 4,
                    8 => 5,
                    else => unreachable,
                },
                // The open quorum must allow for at least two copy faults, because our view change
                // updates an existing set of copies in place, temporarily impairing one copy.
                .open => switch (constants.superblock_copies) {
                    4 => 2,
                    6 => 3,
                    8 => 4,
                    else => unreachable,
                },
            };
        }

        fn log_context(superblock: *const SuperBlock, context: *const Context) void {
            log.debug("{[replica]?}: {[caller]s}: " ++
                "commit_min={[commit_min_old]}..{[commit_min_new]} " ++
                "commit_max={[commit_max_old]}..{[commit_max_new]} " ++
                "commit_min_checksum={[commit_min_checksum_old]}..{[commit_min_checksum_new]} " ++
                "log_view={[log_view_old]}..{[log_view_new]} " ++
                "view={[view_old]}..{[view_new]} " ++
                "head={[head_old]}..{[head_new]?}", .{
                .replica = superblock.replica_index,
                .caller = @tagName(context.caller),

                .commit_min_old = superblock.staging.vsr_state.commit_min,
                .commit_min_new = context.vsr_state.?.commit_min,

                .commit_max_old = superblock.staging.vsr_state.commit_max,
                .commit_max_new = context.vsr_state.?.commit_max,

                .commit_min_checksum_old = superblock.staging.vsr_state.commit_min_checksum,
                .commit_min_checksum_new = context.vsr_state.?.commit_min_checksum,

                .log_view_old = superblock.staging.vsr_state.log_view,
                .log_view_new = context.vsr_state.?.log_view,

                .view_old = superblock.staging.vsr_state.view,
                .view_new = context.vsr_state.?.view,

                .head_old = superblock.staging.vsr_headers().slice[0].checksum,
                .head_new = if (context.vsr_headers) |*headers|
                    @as(?u128, headers.array.get(0).checksum)
                else
                    null,
            });
        }

        pub fn trailer_buffer(
            superblock: *SuperBlock,
            trailer: Trailer,
        ) []align(constants.sector_size) u8 {
            return switch (trailer) {
                .manifest => superblock.manifest_buffer,
                .free_set => superblock.free_set_buffer,
                .client_sessions => superblock.client_sessions_buffer,
            };
        }
    };
}

pub const Caller = enum {
    format,
    open,
    checkpoint,
    view_change,
    sync,

    /// Beyond formatting and opening of the superblock, which are mutually exclusive of all
    /// other operations, only the following queue combinations are allowed:
    ///
    /// from state → to states
    const transitions = sets: {
        const Set = std.enums.EnumSet(Caller);
        break :sets std.enums.EnumMap(Caller, Set).init(.{
            .format = Set.init(.{}),
            .open = Set.init(.{}),
            .checkpoint = Set.init(.{ .view_change = true }),
            .view_change = Set.init(.{
                .checkpoint = true,
                .sync = true,
            }),
            .sync = Set.init(.{ .view_change = true }),
        });
    };

    fn updates_vsr_headers(caller: Caller) bool {
        return switch (caller) {
            .format => true,
            .open => unreachable,
            .checkpoint => false,
            .view_change => true,
            .sync => false,
        };
    }

    fn updates_trailers(caller: Caller) bool {
        return switch (caller) {
            .format => true,
            .open => unreachable,
            .checkpoint => true,
            .view_change => false,
            .sync => true,
        };
    }
};

pub const Trailer = enum {
    manifest,
    free_set,
    client_sessions,

    pub fn zone(trailer: Trailer) SuperBlockZone {
        return switch (trailer) {
            .manifest => .manifest,
            .free_set => .free_set,
            .client_sessions => .client_sessions,
        };
    }
};

pub const SuperBlockZone = enum {
    header,
    manifest,
    free_set,
    client_sessions,

    pub fn start(zone: SuperBlockZone) u64 {
        comptime var start_offset = 0;
        inline for (comptime std.enums.values(SuperBlockZone)) |z| {
            if (z == zone) return start_offset;
            start_offset += comptime size_max(z);
        }
        unreachable;
    }

    pub fn start_for_copy(zone: SuperBlockZone, copy: u8) u64 {
        assert(copy < constants.superblock_copies);

        return superblock_copy_size * @as(u64, copy) + zone.start();
    }

    pub fn size_max(zone: SuperBlockZone) u64 {
        return switch (zone) {
            .header => @sizeOf(SuperBlockHeader),
            .manifest => superblock_trailer_manifest_size_max, // TODO inline these constants?
            .free_set => superblock_trailer_free_set_size_max,
            .client_sessions => superblock_trailer_client_sessions_size_max,
        };
    }
};

test "SuperBlockHeader" {
    const expect = std.testing.expect;

    var a = std.mem.zeroes(SuperBlockHeader);
    a.set_checksum();

    assert(a.copy == 0);
    try expect(a.valid_checksum());

    a.copy += 1;
    try expect(a.valid_checksum());

    a.version += 1;
    try expect(!a.valid_checksum());
}
