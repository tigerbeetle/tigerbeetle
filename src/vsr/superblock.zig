//! SuperBlock invariants:
//!
//!   * vsr_state
//!     - vsr_state.commit_min is initially 0 (for a newly-formatted replica).
//!     - vsr_state.commit_min ≤ vsr_state.commit_max
//!     - vsr_state.log_view ≤ vsr_state.view
//!     - checkpoint() must advance the superblock's vsr_state.commit_min.
//!     - view_change() must not advance the superblock's vsr_state.commit_min.
//!     - All fields of vsr_state except commit_min_checksum are monotonically increasing over
//!       view_change()/checkpoint().
//!
const std = @import("std");
const assert = std.debug.assert;
const crypto = std.crypto;
const mem = std.mem;
const meta = std.meta;
const os = std.os;

const constants = @import("../constants.zig");
const stdx = @import("../stdx.zig");
const vsr = @import("../vsr.zig");
const log = std.log.scoped(.superblock);

const MessagePool = @import("../message_pool.zig").MessagePool;

pub const SuperBlockManifest = @import("superblock_manifest.zig").Manifest;
pub const SuperBlockFreeSet = @import("superblock_free_set.zig").FreeSet;
pub const SuperBlockClientTable = @import("superblock_client_table.zig").ClientTable;
pub const Quorums = @import("superblock_quorums.zig").QuorumsType(.{
    .superblock_copies = constants.superblock_copies,
});

pub const SuperBlockVersion: u16 = 0;

const vsr_headers_reserved_size = constants.sector_size -
    ((constants.view_change_headers_max * @sizeOf(vsr.Header)) % constants.sector_size);

// Fields are aligned to work as an extern or packed struct.
pub const SuperBlockSector = extern struct {
    checksum: u128 = undefined,

    /// Protects against misdirected reads at startup.
    /// For example, if multiple reads are all misdirected to a single copy of the superblock.
    /// Excluded from the checksum calculation to ensure that all copies have the same checksum.
    /// This simplifies writing and comparing multiple copies.
    copy: u8 = 0,

    /// Protects against writing to or reading from the wrong data file.
    replica: u8,

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
    client_table_checksum: u128,

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
    client_table_size: u32,

    /// The number of headers in vsr_headers_all.
    vsr_headers_count: u32,

    reserved: [3144]u8 = [_]u8{0} ** 3144,

    /// SV/DVC header suffix. Headers are ordered from high-to-low op.
    /// Unoccupied headers (after vsr_headers_count) are zeroed.
    ///
    /// When `vsr_state.log_view < vsr_state.view`, the headers are for a DVC.
    /// When `vsr_state.log_view = vsr_state.view`, the headers are for a SV.
    vsr_headers_all: [constants.view_change_headers_max]vsr.Header,
    vsr_headers_reserved: [vsr_headers_reserved_size]u8 =
        [_]u8{0} ** vsr_headers_reserved_size,

    pub const VSRState = extern struct {
        /// The vsr.Header.checksum of commit_min's message.
        commit_min_checksum: u128,

        /// The last operation committed to the state machine. At startup, replay the log hereafter.
        commit_min: u64,

        /// The highest operation up to which we may commit.
        commit_max: u64,

        /// The last view in which the replica's status was normal.
        log_view: u32,

        /// The view number of the replica.
        view: u32,

        reserved: [8]u8 = [_]u8{0} ** 8,

        comptime {
            assert(@sizeOf(VSRState) == 48);
            // Assert that there is no implicit padding in the struct.
            assert(@bitSizeOf(VSRState) == @sizeOf(VSRState) * 8);
        }

        pub fn root(cluster: u32) VSRState {
            return .{
                .commit_min_checksum = vsr.Header.root_prepare(cluster).checksum,
                .commit_min = 0,
                .commit_max = 0,
                .log_view = 0,
                .view = 0,
            };
        }

        pub fn internally_consistent(state: VSRState) bool {
            return state.commit_max >= state.commit_min and state.view >= state.log_view;
        }

        pub fn monotonic(old: VSRState, new: VSRState) bool {
            assert(old.internally_consistent());
            assert(new.internally_consistent());
            // The last case is for when checking monotonic() from the sequence=0 sector.
            assert(old.commit_min != new.commit_min or
                old.commit_min_checksum == new.commit_min_checksum or
                (old.commit_min_checksum == 0 and old.commit_min == 0));

            if (old.view > new.view) return false;
            if (old.log_view > new.log_view) return false;
            if (old.commit_min > new.commit_min) return false;
            if (old.commit_max > new.commit_max) return false;

            return true;
        }

        pub fn would_be_updated_by(old: VSRState, new: VSRState) bool {
            assert(monotonic(old, new));

            return !meta.eql(old, new);
        }

        /// Compaction is one bar ahead of superblock's commit_min.
        /// The commits from the bar following commit_min were in the mutable table, and
        /// thus not preserved in the checkpoint.
        /// But the corresponding `compact()` updates were preserved, and must not be repeated
        /// to ensure determinstic storage.
        pub fn op_compacted(state: VSRState, op: u64) bool {
            // If commit_min is 0, we have never checkpointed, so no compactions are checkpointed.
            return state.commit_min > 0 and op <= state.commit_min + constants.lsm_batch_multiple;
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

        pub fn exists(snapshot: Snapshot) bool {
            if (snapshot.created == 0) {
                assert(snapshot.queried == 0);
                assert(snapshot.timeout == 0);

                return false;
            } else {
                return true;
            }
        }

        comptime {
            assert(@sizeOf(Snapshot) == 24);
            // Assert that there is no implicit padding in the struct.
            assert(@bitSizeOf(Snapshot) == @sizeOf(Snapshot) * 8);
        }
    };

    comptime {
        assert(@sizeOf(SuperBlockSector) % constants.sector_size == 0);
        assert(@divExact(@sizeOf(SuperBlockSector), constants.sector_size) >= 2);
        assert(@offsetOf(SuperBlockSector, "vsr_headers_all") == constants.sector_size);
        // Assert that there is no implicit padding in the struct.
        assert(@bitSizeOf(SuperBlockSector) == @sizeOf(SuperBlockSector) * 8);
    }

    pub fn calculate_checksum(superblock: *const SuperBlockSector) u128 {
        comptime assert(meta.fieldIndex(SuperBlockSector, "checksum") == 0);
        comptime assert(meta.fieldIndex(SuperBlockSector, "copy") == 1);

        const checksum_size = @sizeOf(@TypeOf(superblock.checksum));
        comptime assert(checksum_size == @sizeOf(u128));

        const copy_size = @sizeOf(@TypeOf(superblock.copy));
        comptime assert(copy_size == 1);

        const ignore_size = checksum_size + copy_size;

        return vsr.checksum(std.mem.asBytes(superblock)[ignore_size..]);
    }

    pub fn set_checksum(superblock: *SuperBlockSector) void {
        assert(superblock.copy < constants.superblock_copies);
        assert(superblock.version == SuperBlockVersion);
        assert(superblock.flags == 0);

        for (mem.bytesAsSlice(u64, &superblock.reserved)) |word| assert(word == 0);
        for (mem.bytesAsSlice(u64, &superblock.vsr_state.reserved)) |word| assert(word == 0);
        for (mem.bytesAsSlice(u64, &superblock.vsr_headers_reserved)) |word| assert(word == 0);

        superblock.checksum = superblock.calculate_checksum();
    }

    pub fn valid_checksum(superblock: *const SuperBlockSector) bool {
        return superblock.checksum == superblock.calculate_checksum();
    }

    /// Does not consider { checksum, copy } when comparing equality.
    pub fn equal(a: *const SuperBlockSector, b: *const SuperBlockSector) bool {
        for (mem.bytesAsSlice(u64, &a.reserved)) |word| assert(word == 0);
        for (mem.bytesAsSlice(u64, &b.reserved)) |word| assert(word == 0);

        for (mem.bytesAsSlice(u64, &a.vsr_state.reserved)) |word| assert(word == 0);
        for (mem.bytesAsSlice(u64, &b.vsr_state.reserved)) |word| assert(word == 0);

        for (mem.bytesAsSlice(u64, &a.vsr_headers_reserved)) |word| assert(word == 0);
        for (mem.bytesAsSlice(u64, &b.vsr_headers_reserved)) |word| assert(word == 0);

        if (a.version != b.version) return false;
        if (a.replica != b.replica) return false;
        if (a.cluster != b.cluster) return false;
        if (a.storage_size != b.storage_size) return false;
        if (a.storage_size_max != b.storage_size_max) return false;
        if (a.sequence != b.sequence) return false;
        if (a.parent != b.parent) return false;
        if (a.manifest_checksum != b.manifest_checksum) return false;
        if (a.free_set_checksum != b.free_set_checksum) return false;
        if (a.client_table_checksum != b.client_table_checksum) return false;
        if (!meta.eql(a.vsr_state, b.vsr_state)) return false;
        if (a.flags != b.flags) return false;
        if (!meta.eql(a.snapshots, b.snapshots)) return false;
        if (a.manifest_size != b.manifest_size) return false;
        if (a.free_set_size != b.free_set_size) return false;
        if (a.vsr_headers_count != b.vsr_headers_count) return false;
        if (!meta.eql(a.vsr_headers_all, b.vsr_headers_all)) return false;

        return true;
    }

    pub fn vsr_headers(superblock: *const SuperBlockSector) vsr.ViewChangeHeaders {
        return vsr.ViewChangeHeaders.init(
            superblock.vsr_headers_all[0..superblock.vsr_headers_count],
        );
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
pub const superblock_copy_size = @sizeOf(SuperBlockSector) + superblock_trailer_size_max;
comptime {
    assert(superblock_copy_size % constants.sector_size == 0);
}

/// The maximum possible size of the superblock trailer, following the superblock sector.
pub const superblock_trailer_size_max = blk: {
    // To calculate the size of the superblock trailer we need to know:
    // 1. the maximum number of manifest blocks that should be able to be referenced, and
    // 2. the maximum possible size of the EWAH-compressed bit set addressable by the free set.

    assert(superblock_trailer_manifest_size_max > 0);
    assert(superblock_trailer_manifest_size_max % constants.sector_size == 0);
    assert(superblock_trailer_manifest_size_max % SuperBlockManifest.BlockReferenceSize == 0);

    assert(superblock_trailer_free_set_size_max > 0);
    assert(superblock_trailer_free_set_size_max % constants.sector_size == 0);

    assert(superblock_trailer_client_table_size_max > 0);
    assert(superblock_trailer_client_table_size_max % constants.sector_size == 0);

    // We order the smaller manifest section ahead of the block free set for better access locality.
    // For example, it's cheaper to skip over 1 MiB when reading from disk than to skip over 32 MiB.
    break :blk superblock_trailer_manifest_size_max +
        superblock_trailer_free_set_size_max +
        superblock_trailer_client_table_size_max;
};

// A manifest block reference of 40 bytes contains a tree hash, checksum, and address.
// These references are stored in struct-of-arrays layout in the trailer for the sake of alignment.
const superblock_trailer_manifest_size_max = blk: {
    assert(SuperBlockManifest.BlockReferenceSize == 16 + 16 + 8);

    // Use a multiple of sector * reference so that the size is exactly divisible without padding:
    // For example, this 2.5 MiB manifest trailer == 65536 references == 65536 * 511 or 34m tables.
    // TODO Size this relative to the expected number of tables & fragmentation.
    break :blk 16 * constants.sector_size * SuperBlockManifest.BlockReferenceSize;
};

const superblock_trailer_free_set_size_max = blk: {
    const encode_size_max = SuperBlockFreeSet.encode_size_max(block_count_max);
    assert(encode_size_max > 0);

    break :blk vsr.sector_ceil(encode_size_max);
};

const superblock_trailer_client_table_size_max = blk: {
    const encode_size_max = SuperBlockClientTable.encode_size_max;
    assert(encode_size_max > 0);

    break :blk vsr.sector_ceil(encode_size_max);
};

pub const data_file_size_min = blk: {
    break :blk superblock_zone_size + constants.journal_size_max;
};

/// The maximum number of blocks in the grid.
const block_count_max = blk: {
    var size = constants.storage_size_max;
    size -= constants.superblock_copies * @sizeOf(SuperBlockSector);
    size -= constants.superblock_copies * superblock_trailer_client_table_size_max;
    size -= constants.superblock_copies * superblock_trailer_manifest_size_max;
    size -= constants.journal_size_max;
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
    assert(block_count_max > 0);
    assert(block_count_max * constants.block_size + data_file_size_min <= constants.storage_size_max);
}

/// This table shows the sequence number progression of the SuperBlock's sectors.
///
/// action        working  staging  disk
/// format        seq      seq      seq
///                0                 -        Initially the file has no sectors.
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
///               a        a        a
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
        pub const ClientTable = SuperBlockClientTable;

        pub const Context = struct {
            pub const Caller = enum {
                format,
                open,
                checkpoint,
                view_change,
            };

            superblock: *SuperBlock,
            callback: fn (context: *Context) void,
            caller: Caller,

            write: Storage.Write = undefined,
            read: Storage.Read = undefined,
            read_threshold: ?Quorums.Threshold = null,
            copy: ?u8 = null,
            /// Used by format(), checkpoint(), and view_change().
            vsr_state: ?SuperBlockSector.VSRState = null,
            /// Used by format() and view_change().
            vsr_headers: ?vsr.ViewChangeHeaders.BoundedArray = null,
            repairs: ?Quorums.RepairIterator = null, // Used by open().
        };

        storage: *Storage,

        /// The first logical offset that may be written to the superblock storage zone.
        storage_offset: u64 = 0,

        /// The total size of the superblock storage zone after this physical offset.
        storage_size: u64 = superblock_zone_size,

        /// The superblock that was recovered at startup after a crash or that was last written.
        working: *align(constants.sector_size) SuperBlockSector,

        /// The superblock that will replace the current working superblock once written.
        /// We cannot mutate any working state directly until it is safely on stable storage.
        /// Otherwise, we may accidentally externalize guarantees that are not yet durable.
        staging: *align(constants.sector_size) SuperBlockSector,

        /// The copies that we read into at startup or when verifying the written superblock.
        reading: []align(constants.sector_size) SuperBlockSector,

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

        manifest: Manifest,
        free_set: FreeSet,
        client_table: ClientTable,

        manifest_buffer: []align(constants.sector_size) u8,
        free_set_buffer: []align(constants.sector_size) u8,
        client_table_buffer: []align(constants.sector_size) u8,

        /// Whether the superblock has been opened. An open superblock may not be formatted.
        opened: bool = false,
        block_count_limit: usize,
        storage_size_limit: u64,

        /// Beyond formatting and opening of the superblock, which are mutually exclusive of all
        /// other operations, only the following queue combinations are allowed:
        /// 1. A view change may queue on a checkpoint.
        /// 2. A checkpoint may queue on a view change.
        ///
        /// There may only be a single caller queued at a time, to ensure that the VSR protocol is
        /// careful to submit at most one view change at a time.
        queue_head: ?*Context = null,
        queue_tail: ?*Context = null,

        pub const Options = struct {
            storage: *Storage,
            message_pool: *MessagePool,
            storage_size_limit: u64,
        };

        pub fn init(allocator: mem.Allocator, options: Options) !SuperBlock {
            assert(options.storage_size_limit >= data_file_size_min);
            assert(options.storage_size_limit <= constants.storage_size_max);
            assert(options.storage_size_limit % constants.sector_size == 0);

            const shard_count_limit = @intCast(usize, @divFloor(
                options.storage_size_limit - data_file_size_min,
                constants.block_size * FreeSet.shard_bits,
            ));
            const block_count_limit = shard_count_limit * FreeSet.shard_bits;
            assert(block_count_limit <= block_count_max);

            const a = try allocator.allocAdvanced(SuperBlockSector, constants.sector_size, 1, .exact);
            errdefer allocator.free(a);

            const b = try allocator.allocAdvanced(SuperBlockSector, constants.sector_size, 1, .exact);
            errdefer allocator.free(b);

            const reading = try allocator.allocAdvanced(
                [constants.superblock_copies]SuperBlockSector,
                constants.sector_size,
                1,
                .exact,
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

            var client_table = try ClientTable.init(allocator, options.message_pool);
            errdefer client_table.deinit(allocator);

            const manifest_buffer = try allocator.allocAdvanced(
                u8,
                constants.sector_size,
                superblock_trailer_manifest_size_max,
                .exact,
            );
            errdefer allocator.free(manifest_buffer);

            const free_set_buffer = try allocator.allocAdvanced(
                u8,
                constants.sector_size,
                SuperBlockFreeSet.encode_size_max(block_count_limit),
                .exact,
            );
            errdefer allocator.free(free_set_buffer);

            const client_table_buffer = try allocator.allocAdvanced(
                u8,
                constants.sector_size,
                superblock_trailer_client_table_size_max,
                .exact,
            );
            errdefer allocator.free(client_table_buffer);

            return SuperBlock{
                .storage = options.storage,
                .working = &a[0],
                .staging = &b[0],
                .reading = &reading[0],
                .manifest = manifest,
                .free_set = free_set,
                .client_table = client_table,
                .manifest_buffer = manifest_buffer,
                .free_set_buffer = free_set_buffer,
                .client_table_buffer = client_table_buffer,
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
            superblock.client_table.deinit(allocator);

            allocator.free(superblock.manifest_buffer);
            allocator.free(superblock.free_set_buffer);
            allocator.free(superblock.client_table_buffer);
        }

        pub const FormatOptions = struct {
            cluster: u32,
            replica: u8,
        };

        pub fn format(
            superblock: *SuperBlock,
            callback: fn (context: *Context) void,
            context: *Context,
            options: FormatOptions,
        ) void {
            assert(!superblock.opened);

            assert(options.replica < constants.replicas_max);

            // This working copy provides the parent checksum, and will not be written to disk.
            // We therefore use zero values to make this parent checksum as stable as possible.
            superblock.working.* = .{
                .copy = 0,
                .version = SuperBlockVersion,
                .sequence = 0,
                .replica = options.replica,
                .cluster = options.cluster,
                .storage_size = 0,
                .storage_size_max = constants.storage_size_max,
                .parent = 0,
                .manifest_checksum = 0,
                .free_set_checksum = 0,
                .client_table_checksum = 0,
                .vsr_state = .{
                    .commit_min_checksum = 0,
                    .commit_min = 0,
                    .commit_max = 0,
                    .log_view = 0,
                    .view = 0,
                },
                .snapshots = undefined,
                .manifest_size = 0,
                .free_set_size = 0,
                .client_table_size = 0,
                .vsr_headers_count = 0,
                .vsr_headers_all = mem.zeroes([constants.view_change_headers_max]vsr.Header),
            };

            mem.set(SuperBlockSector.Snapshot, &superblock.working.snapshots, .{
                .created = 0,
                .queried = 0,
                .timeout = 0,
            });

            superblock.working.set_checksum();

            var vsr_headers = vsr.ViewChangeHeaders.BoundedArray{ .buffer = undefined };
            vsr_headers.appendAssumeCapacity(vsr.Header.root_prepare(options.cluster));

            context.* = .{
                .superblock = superblock,
                .callback = callback,
                .caller = .format,
                .vsr_state = SuperBlockSector.VSRState.root(options.cluster),
                .vsr_headers = vsr_headers,
            };

            // TODO At a higher layer, we must:
            // 1. verify that there is no valid superblock, and
            // 2. zero the superblock, WAL and client table to ensure storage determinism.

            superblock.acquire(context);
        }

        pub fn open(
            superblock: *SuperBlock,
            callback: fn (context: *Context) void,
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
        };

        /// The vsr_state must update the commit_min and commit_min_checksum.
        /// The vsr_state must not update view/log_view.
        pub fn checkpoint(
            superblock: *SuperBlock,
            callback: fn (context: *Context) void,
            context: *Context,
            update: UpdateCheckpoint,
        ) void {
            assert(superblock.opened);
            assert(superblock.staging.vsr_state.commit_min < update.commit_min);
            assert(superblock.staging.vsr_state.commit_min_checksum != update.commit_min_checksum);
            assert(update.commit_min <= update.commit_max);

            const vsr_state = SuperBlockSector.VSRState{
                .commit_min_checksum = update.commit_min_checksum,
                .commit_min = update.commit_min,
                .commit_max = update.commit_max,
                .log_view = superblock.staging.vsr_state.log_view,
                .view = superblock.staging.vsr_state.view,
            };
            assert(superblock.staging.vsr_state.would_be_updated_by(vsr_state));

            context.* = .{
                .superblock = superblock,
                .callback = callback,
                .caller = .checkpoint,
                .vsr_state = vsr_state,
            };

            superblock.acquire(context);
        }

        const UpdateViewChange = struct {
            commit_max: u64,
            log_view: u32,
            view: u32,
            headers: vsr.ViewChangeHeaders.BoundedArray,
        };

        /// The replica calls view_change() to persist its view/log_view — it cannot
        /// advertise either value until it is certain they will never backtrack.
        ///
        /// The update must advance view/log_view (monotonically increasing).
        pub fn view_change(
            superblock: *SuperBlock,
            callback: fn (context: *Context) void,
            context: *Context,
            update: UpdateViewChange,
        ) void {
            assert(superblock.opened);
            assert(superblock.staging.vsr_state.commit_min <= update.headers.get(0).op);
            assert(superblock.staging.vsr_state.commit_max <= update.commit_max);
            assert(superblock.staging.vsr_state.view <= update.view);
            assert(superblock.staging.vsr_state.log_view <= update.log_view);
            assert(superblock.staging.vsr_state.log_view < update.log_view or
                superblock.staging.vsr_state.view < update.view);

            vsr.ViewChangeHeaders.verify(update.headers.constSlice());
            assert(update.view >= update.log_view);

            const vsr_state = SuperBlockSector.VSRState{
                .commit_min_checksum = superblock.staging.vsr_state.commit_min_checksum,
                .commit_min = superblock.staging.vsr_state.commit_min,
                .commit_max = update.commit_max,
                .log_view = update.log_view,
                .view = update.view,
            };
            assert(vsr_state.internally_consistent());
            assert(superblock.staging.vsr_state.would_be_updated_by(vsr_state));
            assert(superblock.staging.vsr_state.monotonic(vsr_state));

            log.debug("view_change: commit_max={}..{} log_view={}..{} view={}..{} head={}..{}", .{
                superblock.staging.vsr_state.commit_max,
                update.commit_max,

                superblock.staging.vsr_state.log_view,
                update.log_view,

                superblock.staging.vsr_state.view,
                update.view,

                superblock.staging.vsr_headers().slice[0].checksum,
                update.headers.get(0).checksum,
            });

            context.* = .{
                .superblock = superblock,
                .callback = callback,
                .caller = .view_change,
                .vsr_state = vsr_state,
                .vsr_headers = update.headers,
            };

            superblock.acquire(context);
        }

        pub fn view_change_in_progress(superblock: *const SuperBlock) bool {
            assert(superblock.opened);

            if (superblock.queue_head) |head| {
                if (head.caller == .view_change) return true;
                assert(head.caller == .checkpoint);
            }

            if (superblock.queue_tail) |tail| {
                assert(tail.caller == .view_change);
                return true;
            }

            return false;
        }

        fn write_staging(superblock: *SuperBlock, context: *Context) void {
            assert(context.caller != .open);
            assert(context.caller == .format or superblock.opened);
            assert(context.copy == null);
            assert(context.vsr_state.?.internally_consistent());
            assert(superblock.queue_head == context);
            assert(superblock.queue_tail == null);
            assert(superblock.working.vsr_state.would_be_updated_by(context.vsr_state.?));

            superblock.staging.* = superblock.working.*;
            superblock.staging.sequence = superblock.staging.sequence + 1;
            superblock.staging.parent = superblock.staging.checksum;
            superblock.staging.vsr_state = context.vsr_state.?;

            if (context.vsr_headers) |headers| {
                assert(context.caller == .format or context.caller == .view_change);

                superblock.staging.vsr_headers_count = @intCast(u32, headers.len);
                stdx.copy_disjoint(
                    .exact,
                    vsr.Header,
                    superblock.staging.vsr_headers_all[0..headers.len],
                    headers.constSlice(),
                );
                std.mem.set(
                    vsr.Header,
                    superblock.staging.vsr_headers_all[headers.len..],
                    std.mem.zeroes(vsr.Header),
                );
            } else {
                assert(context.caller == .checkpoint);
            }

            if (context.caller != .view_change) {
                superblock.write_staging_encode_manifest();
                superblock.write_staging_encode_free_set();
                superblock.write_staging_encode_client_table();
            }
            superblock.staging.set_checksum();

            context.copy = 0;
            if (context.caller == .view_change) {
                superblock.write_sector(context);
            } else {
                superblock.write_manifest(context);
            }
        }

        fn write_staging_encode_manifest(superblock: *SuperBlock) void {
            const staging: *SuperBlockSector = superblock.staging;
            const target = superblock.manifest_buffer;

            staging.manifest_size = @intCast(u32, superblock.manifest.encode(target));
            staging.manifest_checksum = vsr.checksum(target[0..staging.manifest_size]);
        }

        fn write_staging_encode_free_set(superblock: *SuperBlock) void {
            const staging: *SuperBlockSector = superblock.staging;
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
                staging.free_set_size = @intCast(u32, superblock.free_set.encode(target));
            }
            staging.free_set_checksum = vsr.checksum(target[0..staging.free_set_size]);
        }

        fn write_staging_encode_client_table(superblock: *SuperBlock) void {
            const staging: *SuperBlockSector = superblock.staging;
            const target = superblock.client_table_buffer;

            staging.client_table_size = @intCast(u32, superblock.client_table.encode(target));
            staging.client_table_checksum = vsr.checksum(target[0..staging.client_table_size]);
        }

        fn write_manifest(superblock: *SuperBlock, context: *Context) void {
            assert(superblock.queue_head == context);

            const size = vsr.sector_ceil(superblock.staging.manifest_size);
            assert(size <= superblock_trailer_manifest_size_max);

            const buffer = superblock.manifest_buffer[0..size];
            const offset = areas.manifest.offset(context.copy.?);

            mem.set(u8, buffer[superblock.staging.manifest_size..], 0); // Zero sector padding.

            assert(superblock.staging.manifest_checksum == vsr.checksum(
                superblock.manifest_buffer[0..superblock.staging.manifest_size],
            ));

            log.debug("{s}: write_manifest: checksum={x} size={} offset={}", .{
                @tagName(context.caller),
                superblock.staging.manifest_checksum,
                superblock.staging.manifest_size,
                offset,
            });

            superblock.assert_bounds(offset, buffer.len);

            if (buffer.len == 0) {
                write_manifest_callback(&context.write);
                return;
            }

            superblock.storage.write_sectors(
                write_manifest_callback,
                &context.write,
                buffer,
                .superblock,
                offset,
            );
        }

        fn write_manifest_callback(write: *Storage.Write) void {
            const context = @fieldParentPtr(Context, "write", write);
            context.superblock.write_free_set(context);
        }

        fn write_free_set(superblock: *SuperBlock, context: *Context) void {
            assert(superblock.queue_head == context);

            const size = vsr.sector_ceil(superblock.staging.free_set_size);
            assert(size <= superblock_trailer_free_set_size_max);

            const buffer = superblock.free_set_buffer[0..size];
            const offset = areas.free_set.offset(context.copy.?);

            mem.set(u8, buffer[superblock.staging.free_set_size..], 0); // Zero sector padding.

            assert(superblock.staging.free_set_checksum == vsr.checksum(
                superblock.free_set_buffer[0..superblock.staging.free_set_size],
            ));

            log.debug("{s}: write_free_set: checksum={x} size={} offset={}", .{
                @tagName(context.caller),
                superblock.staging.free_set_checksum,
                superblock.staging.free_set_size,
                offset,
            });

            superblock.assert_bounds(offset, buffer.len);

            if (buffer.len == 0) {
                write_free_set_callback(&context.write);
                return;
            }

            superblock.storage.write_sectors(
                write_free_set_callback,
                &context.write,
                buffer,
                .superblock,
                offset,
            );
        }

        fn write_free_set_callback(write: *Storage.Write) void {
            const context = @fieldParentPtr(Context, "write", write);
            context.superblock.write_client_table(context);
        }

        fn write_client_table(superblock: *SuperBlock, context: *Context) void {
            assert(superblock.queue_head == context);

            const size = vsr.sector_ceil(superblock.staging.client_table_size);
            assert(size <= superblock_trailer_client_table_size_max);

            const buffer = superblock.client_table_buffer[0..size];
            const offset = areas.client_table.offset(context.copy.?);

            mem.set(u8, buffer[superblock.staging.client_table_size..], 0); // Zero sector padding.

            assert(superblock.staging.client_table_checksum == vsr.checksum(
                superblock.client_table_buffer[0..superblock.staging.client_table_size],
            ));

            log.debug("{s}: write_client_table: checksum={x} size={} offset={}", .{
                @tagName(context.caller),
                superblock.staging.client_table_checksum,
                superblock.staging.client_table_size,
                offset,
            });

            superblock.assert_bounds(offset, buffer.len);

            if (buffer.len == 0) {
                write_client_table_callback(&context.write);
                return;
            }

            superblock.storage.write_sectors(
                write_client_table_callback,
                &context.write,
                buffer,
                .superblock,
                offset,
            );
        }

        fn write_client_table_callback(write: *Storage.Write) void {
            const context = @fieldParentPtr(Context, "write", write);
            context.superblock.write_sector(context);
        }

        fn write_sector(superblock: *SuperBlock, context: *Context) void {
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
            assert(superblock.staging.replica == superblock.working.replica);

            assert(superblock.staging.storage_size >= data_file_size_min);
            assert(superblock.staging.storage_size <= superblock.staging.storage_size_max);

            assert(context.copy.? < constants.superblock_copies);
            superblock.staging.copy = context.copy.?;

            // Updating the copy number should not affect the checksum, which was previously set:
            assert(superblock.staging.valid_checksum());

            const buffer = mem.asBytes(superblock.staging);
            const offset = areas.sector.offset(context.copy.?);

            log.debug("{}: {s}: write_sector: checksum={x} sequence={} copy={} size={} offset={}", .{
                superblock.staging.replica,
                @tagName(context.caller),
                superblock.staging.checksum,
                superblock.staging.sequence,
                context.copy.?,
                buffer.len,
                offset,
            });

            superblock.assert_bounds(offset, buffer.len);

            superblock.storage.write_sectors(
                write_sector_callback,
                &context.write,
                buffer,
                .superblock,
                offset,
            );
        }

        fn write_sector_callback(write: *Storage.Write) void {
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

                switch (context.caller) {
                    .open => unreachable,
                    .format, .checkpoint => superblock.write_manifest(context),
                    .view_change => superblock.write_sector(context),
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
            superblock.read_sector(context);
        }

        fn read_sector(superblock: *SuperBlock, context: *Context) void {
            assert(superblock.queue_head == context);
            assert(context.copy.? < constants.superblock_copies);
            assert(context.read_threshold != null);

            const buffer = mem.asBytes(&superblock.reading[context.copy.?]);
            const offset = areas.sector.offset(context.copy.?);

            log.debug("{s}: read_sector: copy={} size={} offset={}", .{
                @tagName(context.caller),
                context.copy.?,
                buffer.len,
                offset,
            });

            superblock.assert_bounds(offset, buffer.len);

            superblock.storage.read_sectors(
                read_sector_callback,
                &context.read,
                buffer,
                .superblock,
                offset,
            );
        }

        fn read_sector_callback(read: *Storage.Read) void {
            const context = @fieldParentPtr(Context, "read", read);
            const superblock = context.superblock;
            const threshold = context.read_threshold.?;

            assert(superblock.queue_head == context);

            assert(context.copy.? < constants.superblock_copies);
            if (context.copy.? + 1 != constants.superblock_copies) {
                context.copy = context.copy.? + 1;
                superblock.read_sector(context);
                return;
            }

            context.read_threshold = null;
            context.copy = null;

            if (superblock.quorums.working(superblock.reading, threshold)) |quorum| {
                assert(quorum.valid);
                assert(quorum.copies.count() >= threshold.count());
                assert(quorum.sector.storage_size_max == constants.storage_size_max);

                const working = quorum.sector;
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
                    assert(working.client_table_size == 4);
                    assert(working.vsr_state.commit_min_checksum ==
                        vsr.Header.root_prepare(working.cluster).checksum);
                    assert(working.vsr_state.commit_min == 0);
                    assert(working.vsr_state.commit_max == 0);
                    assert(working.vsr_state.log_view == 0);
                    assert(working.vsr_state.view == 0);
                    assert(working.vsr_headers_count == 1);
                } else if (context.caller == .checkpoint) {
                    superblock.free_set.checkpoint();
                }

                superblock.working.* = working.*;
                superblock.staging.* = working.*;
                log.debug(
                    "{s}: installed working superblock: checksum={x} sequence={} cluster={} " ++
                        "replica={} size={} " ++
                        "commit_min_checksum={} commit_min={} commit_max={} " ++
                        "log_view={} view={}",
                    .{
                        @tagName(context.caller),
                        superblock.working.checksum,
                        superblock.working.sequence,
                        superblock.working.cluster,
                        superblock.working.replica,
                        superblock.working.storage_size,
                        superblock.working.vsr_state.commit_min_checksum,
                        superblock.working.vsr_state.commit_min,
                        superblock.working.vsr_state.commit_max,
                        superblock.working.vsr_state.log_view,
                        superblock.working.vsr_state.view,
                    },
                );
                for (superblock.working.vsr_headers().slice) |*header| {
                    log.debug("{s}: vsr_header: {}", .{ @tagName(context.caller), header.* });
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
                        superblock.read_manifest(context);
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

        fn read_manifest(superblock: *SuperBlock, context: *Context) void {
            assert(context.caller == .open);
            assert(superblock.queue_head == context);
            assert(context.copy.? < constants.superblock_copies);

            const size = vsr.sector_ceil(superblock.working.manifest_size);
            assert(size <= superblock_trailer_manifest_size_max);

            const buffer = superblock.manifest_buffer[0..size];
            const offset = areas.manifest.offset(context.copy.?);

            log.debug("{s}: read_manifest: copy={} size={} offset={}", .{
                @tagName(context.caller),
                context.copy.?,
                buffer.len,
                offset,
            });

            superblock.assert_bounds(offset, buffer.len);

            if (buffer.len == 0) {
                read_manifest_callback(&context.read);
                return;
            }

            superblock.storage.read_sectors(
                read_manifest_callback,
                &context.read,
                buffer,
                .superblock,
                offset,
            );
        }

        fn read_manifest_callback(read: *Storage.Read) void {
            const context = @fieldParentPtr(Context, "read", read);
            const superblock = context.superblock;
            const copy = context.copy.?;

            assert(context.caller == .open);
            assert(superblock.queue_head == context);
            assert(!superblock.opened);
            assert(superblock.manifest.count == 0);

            const slice = superblock.manifest_buffer[0..superblock.working.manifest_size];
            if (vsr.checksum(slice) == superblock.working.manifest_checksum) {
                superblock.manifest.decode(slice);

                log.debug("open: read_manifest: manifest blocks: {}/{}", .{
                    superblock.manifest.count,
                    superblock.manifest.count_max,
                });

                // TODO Repair any impaired copies before we continue.
                // At present, we repair at the next checkpoint.
                // We do not repair padding.
                context.copy = 0;
                superblock.read_free_set(context);
            } else {
                log.debug("open: read_manifest: corrupt copy={}", .{copy});
                if (copy + 1 == constants.superblock_copies) {
                    @panic("superblock manifest lost");
                } else {
                    context.copy = copy + 1;
                    superblock.read_manifest(context);
                }
            }
        }

        fn read_free_set(superblock: *SuperBlock, context: *Context) void {
            assert(context.caller == .open);
            assert(superblock.queue_head == context);
            assert(context.copy.? < constants.superblock_copies);

            const size = vsr.sector_ceil(superblock.working.free_set_size);
            assert(size <= superblock_trailer_free_set_size_max);

            const buffer = superblock.free_set_buffer[0..size];
            const offset = areas.free_set.offset(context.copy.?);

            log.debug("{s}: read_free_set: copy={} size={} offset={}", .{
                @tagName(context.caller),
                context.copy.?,
                buffer.len,
                offset,
            });

            superblock.assert_bounds(offset, buffer.len);

            if (buffer.len == 0) {
                read_free_set_callback(&context.read);
                return;
            }

            superblock.storage.read_sectors(
                read_free_set_callback,
                &context.read,
                buffer,
                .superblock,
                offset,
            );
        }

        fn read_free_set_callback(read: *Storage.Read) void {
            const context = @fieldParentPtr(Context, "read", read);
            const superblock = context.superblock;
            const copy = context.copy.?;

            assert(context.caller == .open);
            assert(superblock.queue_head == context);
            assert(!superblock.opened);
            assert(superblock.free_set.count_acquired() == 0);

            const slice = superblock.free_set_buffer[0..superblock.working.free_set_size];
            if (vsr.checksum(slice) == superblock.working.free_set_checksum) {
                superblock.free_set.decode(slice);

                log.debug("open: read_free_set: acquired blocks: {}/{}/{}", .{
                    superblock.free_set.count_acquired(),
                    superblock.block_count_limit,
                    block_count_max,
                });

                superblock.verify_manifest_blocks_are_acquired_in_free_set();

                // TODO Repair any impaired copies before we continue.
                superblock.read_client_table(context);
            } else if (copy + 1 == constants.superblock_copies) {
                @panic("superblock free set lost");
            } else {
                log.debug("open: read_free_set: corrupt copy={}", .{copy});
                context.copy = copy + 1;
                superblock.read_free_set(context);
            }
        }

        fn verify_manifest_blocks_are_acquired_in_free_set(superblock: *SuperBlock) void {
            assert(superblock.manifest.count <= superblock.free_set.count_acquired());
            for (superblock.manifest.addresses[0..superblock.manifest.count]) |address| {
                assert(!superblock.free_set.is_free(address));
            }
        }

        fn read_client_table(superblock: *SuperBlock, context: *Context) void {
            assert(context.caller == .open);
            assert(superblock.queue_head == context);
            assert(context.copy.? < constants.superblock_copies);

            const size = vsr.sector_ceil(superblock.working.client_table_size);
            assert(size <= superblock_trailer_client_table_size_max);

            const buffer = superblock.client_table_buffer[0..size];
            const offset = areas.client_table.offset(context.copy.?);

            log.debug("{s}: read_client_table: copy={} size={} offset={}", .{
                @tagName(context.caller),
                context.copy.?,
                buffer.len,
                offset,
            });

            superblock.assert_bounds(offset, buffer.len);

            if (buffer.len == 0) {
                read_client_table_callback(&context.read);
                return;
            }

            superblock.storage.read_sectors(
                read_client_table_callback,
                &context.read,
                buffer,
                .superblock,
                offset,
            );
        }

        fn read_client_table_callback(read: *Storage.Read) void {
            const context = @fieldParentPtr(Context, "read", read);
            const superblock = context.superblock;
            const copy = context.copy.?;

            assert(context.caller == .open);
            assert(superblock.queue_head == context);
            assert(!superblock.opened);
            assert(superblock.client_table.count() == 0);

            const slice = superblock.client_table_buffer[0..superblock.working.client_table_size];
            if (vsr.checksum(slice) == superblock.working.client_table_checksum) {
                superblock.client_table.decode(slice);

                log.debug("open: read_client_table: client requests: {}/{}", .{
                    superblock.client_table.count(),
                    constants.clients_max,
                });

                context.copy = null;
                superblock.repair(context);
            } else if (copy + 1 == constants.superblock_copies) {
                @panic("superblock client table lost");
            } else {
                log.debug("open: read_client_table: corrupt copy={}", .{copy});
                context.copy = copy + 1;
                superblock.read_client_table(context);
            }
        }

        fn repair(superblock: *SuperBlock, context: *Context) void {
            assert(context.caller == .open);
            assert(context.copy == null);
            assert(superblock.queue_head == context);

            if (context.repairs.?.next()) |repair_copy| {
                context.copy = repair_copy;
                log.warn("repair: copy={}", .{repair_copy});

                superblock.staging.* = superblock.working.*;
                superblock.write_manifest(context);
            } else {
                superblock.release(context);
            }
        }

        fn acquire(superblock: *SuperBlock, context: *Context) void {
            if (superblock.queue_head) |head| {
                // There should be nothing else happening when we format() or open():
                assert(context.caller != .format and context.caller != .open);
                assert(head.caller != .format and head.caller != .open);

                // There may only be one checkpoint() and one view_change() submitted at a time:
                assert(head.caller != context.caller);
                assert(superblock.queue_tail == null);

                log.debug("{s}: enqueued after {s}", .{
                    @tagName(context.caller),
                    @tagName(head.caller),
                });

                superblock.queue_tail = context;
            } else {
                assert(superblock.queue_tail == null);

                superblock.queue_head = context;
                log.debug("{s}: started", .{@tagName(context.caller)});

                if (context.caller == .open) {
                    superblock.read_working(context, .open);
                } else {
                    superblock.write_staging(context);
                }
            }
        }

        fn release(superblock: *SuperBlock, context: *Context) void {
            assert(superblock.queue_head == context);

            log.debug("{s}: complete", .{@tagName(context.caller)});

            switch (context.caller) {
                .format => {},
                .open => {
                    assert(!superblock.opened);
                    superblock.opened = true;

                    if (superblock.working.manifest_size > 0) {
                        assert(superblock.manifest.count > 0);
                    }
                    // TODO Make the FreeSet encoding format not dependant on the word size.
                    if (superblock.working.free_set_size > @sizeOf(usize)) {
                        assert(superblock.free_set.count_acquired() > 0);
                    }
                },
                .checkpoint, .view_change => {
                    assert(meta.eql(superblock.staging.vsr_state, context.vsr_state.?));
                    assert(meta.eql(superblock.working.vsr_state, context.vsr_state.?));
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
        fn threshold_for_caller(caller: Context.Caller) u8 {
            // Working these threshold out by formula is easy to get wrong, so enumerate them:
            // The rule is that the write quorum plus the read quorum must be exactly copies + 1.

            return switch (caller) {
                .format, .checkpoint, .view_change => switch (constants.superblock_copies) {
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
    };
}

pub const Area = enum {
    sector,
    manifest,
    free_set,
    client_table,
};

pub const areas = struct {
    pub const sector = AreaRange{
        .base = 0,
        .size_max = @sizeOf(SuperBlockSector),
    };

    pub const manifest = AreaRange{
        .base = sector.base + sector.size_max,
        .size_max = superblock_trailer_manifest_size_max, // TODO inline these constants?
    };

    pub const free_set = AreaRange{
        .base = manifest.base + manifest.size_max,
        .size_max = superblock_trailer_free_set_size_max,
    };

    pub const client_table = AreaRange{
        .base = free_set.base + free_set.size_max,
        .size_max = superblock_trailer_client_table_size_max,
    };

    const AreaRange = struct {
        base: u64,
        size_max: u64,

        pub fn offset(area: AreaRange, copy: u8) u64 {
            return superblock_copy_size * @as(u64, copy) + area.base;
        }
    };
};

test "SuperBlockSector" {
    const expect = std.testing.expect;

    var a = std.mem.zeroes(SuperBlockSector);
    a.set_checksum();

    assert(a.copy == 0);
    try expect(a.valid_checksum());

    a.copy += 1;
    try expect(a.valid_checksum());

    a.replica += 1;
    try expect(!a.valid_checksum());
}
