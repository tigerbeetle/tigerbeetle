const std = @import("std");
const assert = std.debug.assert;
const crypto = std.crypto;
const mem = std.mem;
const meta = std.meta;
const os = std.os;

const config = @import("../config.zig");
const div_ceil = @import("../util.zig").div_ceil;
const vsr = @import("../vsr.zig");
const log = std.log.scoped(.superblock);

const MessagePool = @import("../message_pool.zig").MessagePool;

pub const SuperBlockManifest = @import("superblock_manifest.zig").Manifest;
pub const SuperBlockFreeSet = @import("superblock_free_set.zig").FreeSet;
pub const SuperBlockClientTable = @import("superblock_client_table.zig").ClientTable;

/// Identifies the type of a sector or block. Protects against misdirected I/O across valid types.
pub const Magic = enum(u8) {
    superblock,
    manifest,
    prepare,
    index,
    filter,
    data,
};

pub const SuperBlockVersion: u8 = 0;

// Fields are aligned to work as an extern or packed struct.
pub const SuperBlockSector = extern struct {
    checksum: u128 = undefined,

    /// Protects against misdirected reads at startup.
    /// For example, if multiple reads are all misdirected to a single copy of the superblock.
    /// Excluded from the checksum calculation to ensure that all copies have the same checksum.
    /// This simplifies writing and comparing multiple copies.
    copy: u8 = 0,

    /// Protects against misdirected I/O for non-superblock sectors that have a valid checksum.
    magic: Magic,

    /// The version of the superblock format in use, reserved for major breaking changes.
    version: u8,

    /// Protects against writing to or reading from the wrong data file.
    replica: u8,
    cluster: u32,

    /// The current size of the data file.
    size: u64,

    /// The maximum size of the data file.
    // TODO Actually limit the file to this size.
    size_max: u64,

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
    snapshots: [config.lsm_snapshots_max]Snapshot,

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

    reserved: [3160]u8 = [_]u8{0} ** 3160,

    pub const VSRState = extern struct {
        /// The last operation committed to the state machine. At startup, replay the log hereafter.
        commit_min: u64,

        /// The highest operation up to which we may commit.
        commit_max: u64,

        /// The last view in which the replica's status was normal.
        view_normal: u32,

        /// The view number of the replica.
        view: u32,

        comptime {
            assert(@sizeOf(VSRState) == 24);
            // Assert that there is no implicit padding in the struct.
            assert(@bitSizeOf(VSRState) == @sizeOf(VSRState) * 8);
        }

        pub fn internally_consistent(state: VSRState) bool {
            return state.commit_max >= state.commit_min and state.view >= state.view_normal;
        }

        pub fn monotonic(old: VSRState, new: VSRState) bool {
            assert(old.internally_consistent());
            assert(new.internally_consistent());

            if (old.view > new.view) return false;
            if (old.view_normal > new.view_normal) return false;
            if (old.commit_min > new.commit_min) return false;
            if (old.commit_max > new.commit_max) return false;

            return true;
        }

        pub fn would_be_updated_by(old: VSRState, new: VSRState) bool {
            assert(monotonic(old, new));

            return !meta.eql(old, new);
        }

        pub fn update(state: *VSRState, new: VSRState) void {
            assert(state.would_be_updated_by(new));
            state.* = new;
        }

        /// Compaction is one measure ahead of superblock's commit_min.
        /// The commits from the measure following commit_min were in the mutable table, and
        /// thus not preserved in the checkpoint.
        /// But the corresponding `compact()` updates were preserved, and must not be repeated
        /// to ensure determinstic storage.
        pub fn op_compacted(state: VSRState, op: u64) bool {
            return state.commit_min > 0 and op <= state.commit_min + config.lsm_batch_multiple;
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
        assert(@sizeOf(SuperBlockSector) == config.sector_size);
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
        assert(superblock.copy < superblock_copies_max);
        assert(superblock.magic == .superblock);
        assert(superblock.version == SuperBlockVersion);
        assert(superblock.flags == 0);

        for (mem.bytesAsSlice(u64, &superblock.reserved)) |word| assert(word == 0);

        superblock.checksum = superblock.calculate_checksum();
    }

    pub fn valid_checksum(superblock: *const SuperBlockSector) bool {
        return superblock.checksum == superblock.calculate_checksum();
    }

    /// Does not consider { checksum, copy } when comparing equality.
    pub fn equal(a: *const SuperBlockSector, b: *const SuperBlockSector) bool {
        assert(a.magic == .superblock);
        assert(b.magic == .superblock);

        if (a.version != b.version) return false;
        if (a.replica != b.replica) return false;
        if (a.cluster != b.cluster) return false;
        if (a.size != b.size) return false;
        if (a.size_max != b.size_max) return false;
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

        for (mem.bytesAsSlice(u64, &a.reserved)) |word| assert(word == 0);
        for (mem.bytesAsSlice(u64, &b.reserved)) |word| assert(word == 0);

        return true;
    }
};

comptime {
    switch (config.superblock_copies) {
        4, 6, 8 => {},
        else => @compileError("superblock_copies must be either { 4, 6, 8 } for flexible quorums."),
    }
}

/// The size of the entire superblock storage zone.
pub const superblock_zone_size = superblock_size * superblock_copies_max;

/// A single set of copies (a copy set) consists of config.superblock_copies of a superblock.
/// At least two copy sets are required for copy-on-write in order not to impair existing copies.
///
/// However, when writing only the superblock sector for a view change, we do update-in-place,
/// which is necessary as we need to continue to reference the existing superblock trailer to
/// decouple view changes from checkpoints, to not force an untimely checkpoint ahead of schedule.
pub const superblock_copies_max = config.superblock_copies * 2;

/// The size of an individual superblock including trailer.
pub const superblock_size = @sizeOf(SuperBlockSector) + superblock_trailer_size_max;
comptime {
    assert(superblock_size % config.sector_size == 0);
}

/// The maximum possible size of the superblock trailer, following the superblock sector.
pub const superblock_trailer_size_max = blk: {
    // To calculate the size of the superblock trailer we need to know:
    // 1. the maximum number of manifest blocks that should be able to be referenced, and
    // 2. the maximum possible size of the EWAH-compressed bit set addressable by the free set.

    assert(superblock_trailer_manifest_size_max > 0);
    assert(superblock_trailer_manifest_size_max % config.sector_size == 0);
    assert(superblock_trailer_manifest_size_max % SuperBlockManifest.BlockReferenceSize == 0);

    assert(superblock_trailer_free_set_size_max > 0);
    assert(superblock_trailer_free_set_size_max % config.sector_size == 0);

    assert(superblock_trailer_client_table_size_max > 0);
    assert(superblock_trailer_client_table_size_max % config.sector_size == 0);

    // We order the smaller manifest section ahead of the block free set for better access locality.
    // For example, it's cheaper to skip over 1 MiB when reading from disk than to skip over 32 MiB.
    break :blk superblock_trailer_manifest_size_max + superblock_trailer_free_set_size_max + superblock_trailer_client_table_size_max;
};

// A manifest block reference of 40 bytes contains a tree hash, checksum, and address.
// These references are stored in struct-of-arrays layout in the trailer for the sake of alignment.
pub const superblock_trailer_manifest_size_max = blk: {
    assert(SuperBlockManifest.BlockReferenceSize == 16 + 16 + 8);

    // Use a multiple of sector * reference so that the size is exactly divisible without padding:
    // For example, this 2.5 MiB manifest trailer == 65536 references == 65536 * 511 or 34m tables.
    break :blk 16 * config.sector_size * SuperBlockManifest.BlockReferenceSize;
};

pub const superblock_trailer_free_set_size_max = blk: {
    const encode_size_max = SuperBlockFreeSet.encode_size_max(config.block_count_max);
    assert(encode_size_max > 0);

    // Round up to the nearest sector:
    break :blk div_ceil(encode_size_max, config.sector_size) * config.sector_size;
};

pub const superblock_trailer_client_table_size_max = blk: {
    const encode_size_max = SuperBlockClientTable.encode_size_max;
    assert(encode_size_max > 0);

    // Round up to the nearest sector:
    break :blk div_ceil(encode_size_max, config.sector_size) * config.sector_size;
};

pub const data_file_size_min = blk: {
    break :blk superblock_zone_size + config.journal_size_max;
};

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
            copy: u8 = undefined,
            vsr_state: SuperBlockSector.VSRState = undefined,
        };

        storage: *Storage,

        /// The first logical offset that may be written to the superblock storage zone.
        storage_offset: u64 = 0,

        /// The total size of the superblock storage zone after this physical offset.
        storage_size: u64 = superblock_zone_size,

        /// The superblock that was recovered at startup after a crash or that was last written.
        working: *align(config.sector_size) SuperBlockSector,

        /// The superblock that will replace the current working superblock once written.
        /// This is used when writing the staging superblock, or when changing views before then.
        /// We cannot mutate any working state directly until it is safely on stable storage.
        /// Otherwise, we may accidentally externalize guarantees that are not yet durable.
        writing: *align(config.sector_size) SuperBlockSector,

        /// The superblock that will be checkpointed next.
        /// This may be updated incrementally several times before the next checkpoint.
        /// For example, to track new snapshots as they are registered.
        staging: *align(config.sector_size) SuperBlockSector,

        /// The copies that we read into at startup or when verifying the written superblock.
        reading: []align(config.sector_size) SuperBlockSector,

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

        manifest_buffer: []align(config.sector_size) u8,
        free_set_buffer: []align(config.sector_size) u8,
        client_table_buffer: []align(config.sector_size) u8,

        /// Whether the superblock has been opened. An open superblock may not be formatted.
        opened: bool = false,

        /// Beyond formatting and opening of the superblock, which are mutually exclusive of all
        /// other operations, only the following queue combinations are allowed:
        /// 1. A view change may queue on a checkpoint.
        /// 2. A checkpoint may queue on a view change.
        /// 
        /// There may only be a single caller queued at a time, to ensure that the VSR protocol is
        /// careful to submit at most one view change at a time.
        queue_head: ?*Context = null,
        queue_tail: ?*Context = null,

        pub fn init(
            allocator: mem.Allocator,
            storage: *Storage,
            message_pool: *MessagePool,
        ) !SuperBlock {
            const a = try allocator.allocAdvanced(SuperBlockSector, config.sector_size, 1, .exact);
            errdefer allocator.free(a);

            const b = try allocator.allocAdvanced(SuperBlockSector, config.sector_size, 1, .exact);
            errdefer allocator.free(b);

            const c = try allocator.allocAdvanced(SuperBlockSector, config.sector_size, 1, .exact);
            errdefer allocator.free(c);

            const reading = try allocator.allocAdvanced(
                [config.superblock_copies * 2]SuperBlockSector,
                config.sector_size,
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

            var free_set = try FreeSet.init(allocator, config.block_count_max);
            errdefer free_set.deinit(allocator);

            var client_table = try ClientTable.init(allocator, message_pool);
            errdefer client_table.deinit(allocator);

            const manifest_buffer = try allocator.allocAdvanced(
                u8,
                config.sector_size,
                superblock_trailer_manifest_size_max,
                .exact,
            );
            errdefer allocator.free(manifest_buffer);

            const free_set_buffer = try allocator.allocAdvanced(
                u8,
                config.sector_size,
                superblock_trailer_free_set_size_max,
                .exact,
            );
            errdefer allocator.free(free_set_buffer);

            const client_table_buffer = try allocator.allocAdvanced(
                u8,
                config.sector_size,
                superblock_trailer_client_table_size_max,
                .exact,
            );
            errdefer allocator.free(client_table_buffer);

            return SuperBlock{
                .storage = storage,
                .working = &a[0],
                .writing = &b[0],
                .staging = &c[0],
                .reading = &reading[0],
                .manifest = manifest,
                .free_set = free_set,
                .client_table = client_table,
                .manifest_buffer = manifest_buffer,
                .free_set_buffer = free_set_buffer,
                .client_table_buffer = client_table_buffer,
            };
        }

        pub fn deinit(superblock: *SuperBlock, allocator: mem.Allocator) void {
            assert(superblock.queue_head == null);
            assert(superblock.queue_tail == null);

            allocator.destroy(superblock.working);
            allocator.destroy(superblock.writing);
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

            /// The maximum size of the entire data file.
            size_max: u64,
        };

        pub fn format(
            superblock: *SuperBlock,
            callback: fn (context: *Context) void,
            context: *Context,
            options: FormatOptions,
        ) void {
            assert(!superblock.opened);

            assert(options.replica < config.replicas_max);
            assert(options.size_max >= data_file_size_min);
            assert(options.size_max % config.sector_size == 0);

            // This working copy provides the parent checksum, and will not be written to disk.
            // We therefore use zero values to make this parent checksum as stable as possible.
            superblock.working.* = .{
                .copy = 0,
                .magic = .superblock,
                .version = SuperBlockVersion,
                .sequence = 0,
                .replica = options.replica,
                .cluster = options.cluster,
                .size = 0,
                .size_max = options.size_max,
                .parent = 0,
                .manifest_checksum = 0,
                .free_set_checksum = 0,
                .client_table_checksum = 0,
                .vsr_state = .{
                    .commit_min = 0,
                    .commit_max = 0,
                    .view_normal = 0,
                    .view = 0,
                },
                .snapshots = undefined,
                .manifest_size = 0,
                .free_set_size = 0,
                .client_table_size = 0,
            };

            mem.set(SuperBlockSector.Snapshot, &superblock.working.snapshots, .{
                .created = 0,
                .queried = 0,
                .timeout = 0,
            });

            superblock.working.set_checksum();

            superblock.staging.* = superblock.working.*;
            superblock.staging.sequence = superblock.working.sequence + 1;
            superblock.staging.parent = superblock.working.checksum;

            context.* = .{
                .superblock = superblock,
                .callback = callback,
                .caller = .format,
                .copy = undefined,
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

        pub fn checkpoint(
            superblock: *SuperBlock,
            callback: fn (context: *Context) void,
            context: *Context,
        ) void {
            assert(superblock.opened);

            context.* = .{
                .superblock = superblock,
                .callback = callback,
                .caller = .checkpoint,
                .copy = undefined,
            };

            superblock.acquire(context);
        }

        pub fn view_change(
            superblock: *SuperBlock,
            callback: fn (context: *Context) void,
            context: *Context,
            vsr_state: SuperBlockSector.VSRState,
        ) void {
            assert(superblock.opened);

            log.debug(
                "view_change: commit_min={}..{} commit_max={}..{} view_normal={}..{} view={}..{}",
                .{
                    superblock.working.vsr_state.commit_min,
                    vsr_state.commit_min,

                    superblock.working.vsr_state.commit_max,
                    vsr_state.commit_max,

                    superblock.working.vsr_state.view_normal,
                    vsr_state.view_normal,

                    superblock.working.vsr_state.view,
                    vsr_state.view,
                },
            );

            assert(vsr_state.internally_consistent());

            context.* = .{
                .superblock = superblock,
                .callback = callback,
                .caller = .view_change,
                .copy = undefined,
                .vsr_state = vsr_state,
            };

            // Only this view_change() function may change the VSR state.
            assert(meta.eql(superblock.working.vsr_state, superblock.staging.vsr_state));

            if (!superblock.working.vsr_state.would_be_updated_by(context.vsr_state)) {
                log.debug("view_change: no change", .{});
                callback(context);
                return;
            }

            superblock.acquire(context);
        }

        pub fn view_change_in_progress(superblock: *SuperBlock) bool {
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
            assert(context.caller == .format or context.caller == .checkpoint);
            assert(context.caller == .format or superblock.opened);
            assert(superblock.queue_head == context);
            assert(superblock.queue_tail == null);

            superblock.write_staging_encode_manifest();
            superblock.write_staging_encode_free_set();
            superblock.write_staging_encode_client_table();

            superblock.writing.* = superblock.staging.*;
            superblock.writing.set_checksum();

            assert(superblock.writing.sequence == superblock.working.sequence + 1);
            assert(superblock.writing.parent == superblock.working.checksum);

            superblock.staging.sequence = superblock.writing.sequence + 1;
            superblock.staging.parent = superblock.writing.checksum;

            assert(superblock.writing.manifest_checksum == superblock.staging.manifest_checksum);
            assert(superblock.writing.free_set_checksum == superblock.staging.free_set_checksum);
            assert(superblock.writing.client_table_checksum == superblock.staging.client_table_checksum);

            assert(superblock.writing.manifest_size == superblock.staging.manifest_size);
            assert(superblock.writing.free_set_size == superblock.staging.free_set_size);
            assert(superblock.writing.client_table_size == superblock.staging.client_table_size);

            context.copy = starting_copy_for_sequence(superblock.writing.sequence);
            superblock.write_manifest(context);
        }

        fn write_staging_encode_manifest(superblock: *SuperBlock) void {
            const staging: *SuperBlockSector = superblock.staging;
            const target = superblock.manifest_buffer;

            staging.manifest_size = @intCast(u32, superblock.manifest.encode(target));
            staging.manifest_checksum = vsr.checksum(target[0..staging.manifest_size]);
        }

        fn write_staging_encode_free_set(superblock: *SuperBlock) void {
            const staging: *SuperBlockSector = superblock.staging;
            const encode_size_max = FreeSet.encode_size_max(config.block_count_max);
            const target = superblock.free_set_buffer[0..encode_size_max];

            superblock.free_set.include_staging();
            defer superblock.free_set.exclude_staging();

            superblock.verify_manifest_blocks_are_acquired_in_free_set();

            staging.size = data_file_size_min;

            if (superblock.free_set.highest_address_acquired()) |address| {
                staging.size += address * config.block_size;
            }
            assert(staging.size >= data_file_size_min);
            assert(staging.size <= staging.size_max);

            staging.free_set_size = @intCast(u32, superblock.free_set.encode(target));
            staging.free_set_checksum = vsr.checksum(target[0..staging.free_set_size]);
        }

        fn write_staging_encode_client_table(superblock: *SuperBlock) void {
            const staging: *SuperBlockSector = superblock.staging;
            const target = superblock.client_table_buffer;

            staging.client_table_size = @intCast(u32, superblock.client_table.encode(target));
            staging.client_table_checksum = vsr.checksum(target[0..staging.client_table_size]);
        }

        fn write_view_change(superblock: *SuperBlock, context: *Context) void {
            assert(context.caller == .view_change);
            assert(superblock.opened);
            assert(superblock.queue_head == context);
            assert(superblock.queue_tail == null);
            assert(context.vsr_state.internally_consistent());
            assert(meta.eql(superblock.working.vsr_state, superblock.staging.vsr_state));
            assert(superblock.working.vsr_state.would_be_updated_by(context.vsr_state));

            superblock.writing.* = superblock.working.*;

            // We cannot increment the sequence number when writing only the superblock sector as
            // this would write the sector to another copy set with different superblock trailers.
            // Instead, we increment twice so that the sector remains in the same copy set.
            superblock.writing.sequence += 2;
            assert(superblock.writing.parent == superblock.working.parent);

            superblock.writing.vsr_state.update(context.vsr_state);
            superblock.staging.vsr_state.update(context.vsr_state);

            superblock.writing.set_checksum();

            superblock.staging.sequence = superblock.writing.sequence + 1;
            superblock.staging.parent = superblock.writing.checksum;

            context.copy = starting_copy_for_sequence(superblock.writing.sequence);
            superblock.write_sector(context);
        }

        fn write_manifest(superblock: *SuperBlock, context: *Context) void {
            assert(superblock.queue_head == context);

            const size = vsr.sector_ceil(superblock.writing.manifest_size);
            assert(size <= superblock_trailer_manifest_size_max);

            const buffer = superblock.manifest_buffer[0..size];
            const offset = offset_manifest(context.copy, superblock.writing.sequence);

            mem.set(u8, buffer[superblock.writing.manifest_size..], 0); // Zero sector padding.

            assert(superblock.writing.manifest_checksum == vsr.checksum(
                superblock.manifest_buffer[0..superblock.writing.manifest_size],
            ));

            log.debug("{s}: write_manifest: checksum={x} size={} offset={}", .{
                @tagName(context.caller),
                superblock.writing.manifest_checksum,
                superblock.writing.manifest_size,
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

            const size = vsr.sector_ceil(superblock.writing.free_set_size);
            assert(size <= superblock_trailer_free_set_size_max);

            const buffer = superblock.free_set_buffer[0..size];
            const offset = offset_free_set(context.copy, superblock.writing.sequence);

            mem.set(u8, buffer[superblock.writing.free_set_size..], 0); // Zero sector padding.

            assert(superblock.writing.free_set_checksum == vsr.checksum(
                superblock.free_set_buffer[0..superblock.writing.free_set_size],
            ));

            log.debug("{s}: write_free_set: checksum={x} size={} offset={}", .{
                @tagName(context.caller),
                superblock.writing.free_set_checksum,
                superblock.writing.free_set_size,
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

            const size = vsr.sector_ceil(superblock.writing.client_table_size);
            assert(size <= superblock_trailer_client_table_size_max);

            const buffer = superblock.client_table_buffer[0..size];
            const offset = offset_client_table(context.copy, superblock.writing.sequence);

            mem.set(u8, buffer[superblock.writing.client_table_size..], 0); // Zero sector padding.

            assert(superblock.writing.client_table_checksum == vsr.checksum(
                superblock.client_table_buffer[0..superblock.writing.client_table_size],
            ));

            log.debug("{s}: write_client_table: checksum={x} size={} offset={}", .{
                @tagName(context.caller),
                superblock.writing.client_table_checksum,
                superblock.writing.client_table_size,
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

            // We either update the working superblock for a checkpoint (+1) or a view change (+2):
            assert(superblock.writing.sequence == superblock.working.sequence + 1 or
                superblock.writing.sequence == superblock.working.sequence + 2);

            // The staging superblock should always be one ahead, with VSR state in sync:
            assert(superblock.staging.sequence == superblock.writing.sequence + 1);
            assert(superblock.staging.parent == superblock.writing.checksum);
            assert(meta.eql(superblock.staging.vsr_state, superblock.writing.vsr_state));

            // The superblock cluster and replica should never change once formatted:
            assert(superblock.writing.cluster == superblock.working.cluster);
            assert(superblock.writing.cluster == superblock.staging.cluster);
            assert(superblock.writing.replica == superblock.working.replica);
            assert(superblock.writing.replica == superblock.staging.replica);

            assert(superblock.writing.size >= data_file_size_min);
            assert(superblock.writing.size <= superblock.writing.size_max);

            assert(context.copy < superblock_copies_max);
            assert(context.copy >= starting_copy_for_sequence(superblock.writing.sequence));
            assert(context.copy <= stopping_copy_for_sequence(superblock.writing.sequence));
            superblock.writing.copy = context.copy;

            // Updating the copy number should not affect the checksum, which was previously set:
            assert(superblock.writing.valid_checksum());

            const buffer = mem.asBytes(superblock.writing);
            const offset = superblock_size * context.copy;

            log.debug("{s}: write_sector: checksum={x} sequence={} copy={} size={} offset={}", .{
                @tagName(context.caller),
                superblock.writing.checksum,
                superblock.writing.sequence,
                context.copy,
                buffer.len,
                offset,
            });

            superblock.assert_bounds(offset, buffer.len + superblock_trailer_size_max);

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

            assert(superblock.queue_head == context);

            assert(context.copy < superblock_copies_max);
            assert(context.copy >= starting_copy_for_sequence(superblock.writing.sequence));
            assert(context.copy <= stopping_copy_for_sequence(superblock.writing.sequence));
            assert(context.copy == superblock.writing.copy);

            if (context.copy == stopping_copy_for_sequence(superblock.writing.sequence)) {
                if (context.caller == .format and superblock.writing.sequence < 2) {
                    assert(superblock.writing.sequence != 0);

                    superblock.working.* = superblock.writing.*;
                    superblock.write_staging(context);
                } else {
                    superblock.read_working(context);
                }
            } else {
                context.copy += 1;

                switch (context.caller) {
                    .open => unreachable,
                    .format, .checkpoint => superblock.write_manifest(context),
                    .view_change => superblock.write_sector(context),
                }
            }
        }

        fn read_working(superblock: *SuperBlock, context: *Context) void {
            assert(superblock.queue_head == context);

            // We do not submit reads in parallel, as while this would shave off 1ms, it would also
            // increase the risk that a single fault applies to more reads due to temporal locality.
            // This would make verification reads more flaky when we do experience a read fault.
            // See "An Analysis of Data Corruption in the Storage Stack".

            context.copy = 0; // Read all copies across all copy sets.
            for (superblock.reading) |*copy| copy.* = undefined;
            superblock.read_sector(context);
        }

        fn read_sector(superblock: *SuperBlock, context: *Context) void {
            assert(superblock.queue_head == context);
            assert(context.copy < superblock_copies_max);

            const buffer = mem.asBytes(&superblock.reading[context.copy]);
            const offset = superblock_size * context.copy;

            log.debug("{s}: read_sector: copy={} size={} offset={}", .{
                @tagName(context.caller),
                context.copy,
                buffer.len,
                offset,
            });

            superblock.assert_bounds(offset, buffer.len + superblock_trailer_size_max);

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

            assert(superblock.queue_head == context);

            assert(context.copy < superblock_copies_max);
            if (context.copy == superblock_copies_max - 1) {
                const threshold = threshold_for_caller(context.caller);

                if (superblock.quorums.working(superblock.reading, threshold)) |working| {
                    switch (context.caller) {
                        .format, .checkpoint, .view_change => {
                            if (working.checksum != superblock.writing.checksum) {
                                @panic("superblock failed verification after writing");
                            }
                            assert(working.equal(superblock.writing));
                            assert(superblock.staging.sequence == working.sequence + 1);
                            assert(superblock.staging.parent == working.checksum);
                        },
                        .open => {
                            superblock.staging.* = working.*;
                            superblock.staging.sequence = working.sequence + 1;
                            superblock.staging.parent = working.checksum;
                        },
                    }

                    if (context.caller == .format) {
                        assert(working.sequence == 2);
                        // TODO Assert working.size.
                        assert(working.manifest_size == 0);
                        assert(working.free_set_size == 8);
                        assert(working.vsr_state.commit_min == 0);
                        assert(working.vsr_state.commit_max == 0);
                        assert(working.vsr_state.view_normal == 0);
                        assert(working.vsr_state.view == 0);
                    } else if (context.caller == .checkpoint) {
                        superblock.free_set.checkpoint();
                    }

                    superblock.working.* = working.*;
                    log.debug(
                        "{s}: installed working superblock: checksum={x} sequence={} cluster={} " ++
                            "replica={} size={} commit_min={} commit_max={} view_normal={} view={}",
                        .{
                            @tagName(context.caller),
                            superblock.working.checksum,
                            superblock.working.sequence,
                            superblock.working.cluster,
                            superblock.working.replica,
                            superblock.working.size,
                            superblock.working.vsr_state.commit_min,
                            superblock.working.vsr_state.commit_max,
                            superblock.working.vsr_state.view_normal,
                            superblock.working.vsr_state.view,
                        },
                    );

                    if (context.caller == .open) {
                        context.copy = starting_copy_for_sequence(superblock.working.sequence);
                        superblock.read_manifest(context);
                    } else {
                        // TODO Consider calling TRIM() on Grid's free suffix after checkpointing.
                        superblock.release(context);
                    }
                } else |err| switch (err) {
                    error.NotFound => @panic("superblock not found"),
                    error.QuorumLost => @panic("superblock quorum lost"),
                    error.ParentNotFound => @panic("superblock parent not found"),
                    error.ParentQuorumLost => @panic("superblock parent quorum lost"),
                    error.VSRStateNotMonotonic => @panic("superblock vsr state not monotonic"),
                    error.SequenceNotMonotonic => @panic("superblock sequence not monotonic"),
                }
            } else {
                context.copy += 1;
                superblock.read_sector(context);
            }
        }

        fn read_manifest(superblock: *SuperBlock, context: *Context) void {
            assert(context.caller == .open);
            assert(superblock.queue_head == context);
            assert(context.copy < superblock_copies_max);

            const size = vsr.sector_ceil(superblock.working.manifest_size);
            assert(size <= superblock_trailer_manifest_size_max);

            const buffer = superblock.manifest_buffer[0..size];
            const offset = offset_manifest(context.copy, superblock.working.sequence);

            log.debug("{s}: read_manifest: copy={} size={} offset={}", .{
                @tagName(context.caller),
                context.copy,
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
                context.copy = starting_copy_for_sequence(superblock.working.sequence);
                superblock.read_free_set(context);
            } else if (context.copy == stopping_copy_for_sequence(superblock.working.sequence)) {
                @panic("superblock manifest lost");
            } else {
                log.debug("open: read_manifest: corrupt copy={}", .{context.copy});
                context.copy += 1;
                superblock.read_manifest(context);
            }
        }

        fn read_free_set(superblock: *SuperBlock, context: *Context) void {
            assert(context.caller == .open);
            assert(superblock.queue_head == context);
            assert(context.copy < superblock_copies_max);

            const size = vsr.sector_ceil(superblock.working.free_set_size);
            assert(size <= superblock_trailer_free_set_size_max);

            const buffer = superblock.free_set_buffer[0..size];
            const offset = offset_free_set(context.copy, superblock.working.sequence);

            log.debug("{s}: read_free_set: copy={} size={} offset={}", .{
                @tagName(context.caller),
                context.copy,
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

            assert(context.caller == .open);
            assert(superblock.queue_head == context);
            assert(!superblock.opened);
            assert(superblock.free_set.count_acquired() == 0);

            const slice = superblock.free_set_buffer[0..superblock.working.free_set_size];
            if (vsr.checksum(slice) == superblock.working.free_set_checksum) {
                superblock.free_set.decode(slice);

                log.debug("open: read_free_set: acquired blocks: {}/{}", .{
                    superblock.free_set.count_acquired(),
                    config.block_count_max,
                });

                superblock.verify_manifest_blocks_are_acquired_in_free_set();

                // TODO Repair any impaired copies before we continue.
                superblock.read_client_table(context);
            } else if (context.copy == stopping_copy_for_sequence(superblock.working.sequence)) {
                @panic("superblock free set lost");
            } else {
                log.debug("open: read_free_set: corrupt copy={}", .{context.copy});
                context.copy += 1;
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
            assert(context.copy < superblock_copies_max);

            const size = vsr.sector_ceil(superblock.working.client_table_size);
            assert(size <= superblock_trailer_client_table_size_max);

            const buffer = superblock.client_table_buffer[0..size];
            const offset = offset_client_table(context.copy, superblock.working.sequence);

            log.debug("{s}: read_client_table: copy={} size={} offset={}", .{
                @tagName(context.caller),
                context.copy,
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

            assert(context.caller == .open);
            assert(superblock.queue_head == context);
            assert(!superblock.opened);
            assert(superblock.client_table.count() == 0);

            const slice = superblock.client_table_buffer[0..superblock.working.client_table_size];
            if (vsr.checksum(slice) == superblock.working.client_table_checksum) {
                superblock.client_table.decode(slice);

                log.debug("open: read_client_table: client requests: {}/{}", .{
                    superblock.client_table.count(),
                    config.clients_max,
                });

                // TODO Repair any impaired copies before we continue.
                superblock.release(context);
            } else if (context.copy == stopping_copy_for_sequence(superblock.working.sequence)) {
                @panic("superblock client table lost");
            } else {
                log.debug("open: read_client_table: corrupt copy={}", .{context.copy});
                context.copy += 1;
                superblock.read_client_table(context);
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

                switch (context.caller) {
                    .format => superblock.write_staging(context),
                    .open => superblock.read_working(context),
                    .checkpoint => superblock.write_staging(context),
                    .view_change => superblock.write_view_change(context),
                }
            }
        }

        fn release(superblock: *SuperBlock, context: *Context) void {
            assert(superblock.queue_head == context);

            log.debug("{s}: complete", .{@tagName(context.caller)});

            if (context.caller == .open) {
                assert(!superblock.opened);
                superblock.opened = true;

                if (superblock.working.manifest_size > 0) {
                    assert(superblock.manifest.count > 0);
                }
                // TODO Make the FreeSet encoding format not dependant on the word size.
                if (superblock.working.free_set_size > @sizeOf(usize)) {
                    assert(superblock.free_set.count_acquired() > 0);
                }
            } else if (context.caller == .view_change) {
                assert(meta.eql(superblock.working.vsr_state, context.vsr_state));
                assert(meta.eql(superblock.staging.vsr_state, context.vsr_state));
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

        fn offset_manifest(copy: u8, sequence: u64) u64 {
            assert(copy >= starting_copy_for_sequence(sequence));
            assert(copy <= stopping_copy_for_sequence(sequence));

            return superblock_size * copy + @sizeOf(SuperBlockSector);
        }

        fn offset_free_set(copy: u8, sequence: u64) u64 {
            assert(copy >= starting_copy_for_sequence(sequence));
            assert(copy <= stopping_copy_for_sequence(sequence));

            return superblock_size * copy + @sizeOf(SuperBlockSector) +
                superblock_trailer_manifest_size_max;
        }

        fn offset_client_table(copy: u8, sequence: u64) u64 {
            assert(copy >= starting_copy_for_sequence(sequence));
            assert(copy <= stopping_copy_for_sequence(sequence));

            return superblock_size * copy + @sizeOf(SuperBlockSector) +
                superblock_trailer_manifest_size_max +
                superblock_trailer_free_set_size_max;
        }

        /// Returns the first copy index (inclusive) to be written for a sequence number.
        fn starting_copy_for_sequence(sequence: u64) u8 {
            return config.superblock_copies * @intCast(u8, sequence % 2);
        }

        /// Returns the last copy index (inclusive) to be written for a sequence number.
        fn stopping_copy_for_sequence(sequence: u64) u8 {
            return starting_copy_for_sequence(sequence) + config.superblock_copies - 1;
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
                .format, .checkpoint, .view_change => switch (config.superblock_copies) {
                    4 => 3,
                    6 => 4,
                    8 => 5,
                    else => unreachable,
                },
                // The open quorum must allow for at least two copy faults, because our view change
                // updates an existing set of copies in place, temporarily impairing one copy.
                .open => switch (config.superblock_copies) {
                    4 => 2,
                    6 => 3,
                    8 => 4,
                    else => unreachable,
                },
            };
        }
    };
}

const Quorums = struct {
    const Quorum = struct {
        sector: *const SuperBlockSector,
        count: QuorumCount = QuorumCount.initEmpty(),
        valid: bool = false,
    };

    const QuorumCount = std.StaticBitSet(superblock_copies_max);

    array: [superblock_copies_max]Quorum = undefined,
    count: u8 = 0,

    pub const Error = error{
        NotFound,
        QuorumLost,
        ParentNotFound,
        ParentQuorumLost,
        SequenceNotMonotonic,
        VSRStateNotMonotonic,
    };

    /// Returns the working superblock according to the quorum with the highest sequence number.
    /// Verifies that the highest quorum is connected, that the previous quorum was not lost.
    /// i.e. Both the working and previous quorum must be valid and intact and connected.
    /// Otherwise, we might regress to a previous working superblock.
    pub fn working(
        quorums: *Quorums,
        copies: []SuperBlockSector,
        threshold: u8,
    ) Error!*const SuperBlockSector {
        assert(copies.len == superblock_copies_max);
        assert(threshold >= 2 and threshold <= 5);

        quorums.array = undefined;
        quorums.count = 0;

        for (copies) |*copy, index| quorums.count_copy(copy, index, threshold);

        std.sort.sort(Quorum, quorums.slice(), {}, sort_priority_descending);

        for (quorums.slice()) |quorum| {
            if (quorum.count.count() == config.superblock_copies) {
                log.debug("quorum: checksum={x} parent={x} sequence={} count={} valid={}", .{
                    quorum.sector.checksum,
                    quorum.sector.parent,
                    quorum.sector.sequence,
                    quorum.count.count(),
                    quorum.valid,
                });
            } else {
                log.err("quorum: checksum={x} parent={x} sequence={} count={} valid={}", .{
                    quorum.sector.checksum,
                    quorum.sector.parent,
                    quorum.sector.sequence,
                    quorum.count.count(),
                    quorum.valid,
                });
            }
        }

        // No working copies of any sequence number exist in the superblock storage zone at all.
        if (quorums.slice().len == 0) return error.NotFound;

        // At least one copy or quorum exists.
        const b = quorums.slice()[0];

        // Verify that the remaining quorums are correctly sorted:
        for (quorums.slice()[1..]) |a| {
            assert(sort_priority_descending({}, b, a));
            assert(a.sector.magic == .superblock);
            assert(a.sector.valid_checksum());
        }

        // Even the best copy with the most quorum still has inadequate quorum.
        if (!b.valid) return error.QuorumLost;

        // The superblock is only partially formatted, not all copies were written.
        if (b.sector.sequence < 2) return error.NotFound;

        // Verify that the parent copy exists:
        for (quorums.slice()[1..]) |a| {
            if (a.sector.cluster != b.sector.cluster) {
                log.err("superblock copy={} has cluster={} instead of {}", .{
                    a.sector.copy,
                    a.sector.cluster,
                    b.sector.cluster,
                });
            } else if (a.sector.replica != b.sector.replica) {
                log.err("superblock copy={} has replica={} instead of {}", .{
                    a.sector.copy,
                    a.sector.replica,
                    b.sector.replica,
                });
            } else if (a.sector.checksum == b.sector.parent) {
                assert(a.sector.checksum != b.sector.checksum);
                assert(a.sector.cluster == b.sector.cluster);
                assert(a.sector.replica == b.sector.replica);

                if (!a.valid) {
                    return error.ParentQuorumLost;
                } else if (a.sector.sequence >= b.sector.sequence) {
                    return error.SequenceNotMonotonic;
                } else if (a.sector.sequence % 2 == b.sector.sequence % 2) {
                    // The parent must reside in the alternate copy to guarantee that we are able to
                    // detect when the working quorum is lost.
                    return error.SequenceNotMonotonic;
                } else if (!a.sector.vsr_state.monotonic(b.sector.vsr_state)) {
                    return error.VSRStateNotMonotonic;
                } else {
                    assert(b.sector.magic == .superblock);
                    assert(b.sector.valid_checksum());

                    return b.sector;
                }
            }
        }

        return error.ParentNotFound;
    }

    fn count_copy(
        quorums: *Quorums,
        copy: *const SuperBlockSector,
        index: usize,
        threshold: u8,
    ) void {
        assert(index < superblock_copies_max);
        assert(threshold >= 2 and threshold <= 5);

        if (!copy.valid_checksum()) {
            log.debug("copy: {}/{}: invalid checksum", .{ index, superblock_copies_max });
            return;
        }

        if (copy.magic != .superblock) {
            log.debug("copy: {}/{}: not a superblock", .{ index, superblock_copies_max });
            return;
        }

        if (copy.copy == index) {
            log.debug("copy: {}/{}: checksum={x} parent={x} sequence={}", .{
                index,
                superblock_copies_max,
                copy.checksum,
                copy.parent,
                copy.sequence,
            });
        } else {
            // If our read was misdirected, we definitely still want to count the copy.
            // We must just be careful to count it idempotently.
            log.err(
                "copy: {}/{}: checksum={x} parent={x} sequence={} misdirected from copy={}",
                .{
                    index,
                    superblock_copies_max,
                    copy.checksum,
                    copy.parent,
                    copy.sequence,
                    copy.copy,
                },
            );
        }

        var quorum = quorums.find_or_insert_quorum_for_copy(copy);
        assert(quorum.sector.checksum == copy.checksum);
        assert(quorum.sector.equal(copy));

        quorum.count.set(copy.copy);
        assert(quorum.count.isSet(copy.copy));

        // In the worst case, all copies may contain divergent forks of the same sequence.
        // However, this should not happen for the same checksum.
        assert(quorum.count.count() <= config.superblock_copies);

        quorum.valid = quorum.count.count() >= threshold;
    }

    fn find_or_insert_quorum_for_copy(quorums: *Quorums, copy: *const SuperBlockSector) *Quorum {
        assert(copy.magic == .superblock);
        assert(copy.valid_checksum());

        for (quorums.array[0..quorums.count]) |*quorum| {
            if (copy.checksum == quorum.sector.checksum) return quorum;
        } else {
            quorums.array[quorums.count] = Quorum{ .sector = copy };
            quorums.count += 1;

            return &quorums.array[quorums.count - 1];
        }
    }

    fn slice(quorums: *Quorums) []Quorum {
        return quorums.array[0..quorums.count];
    }

    fn sort_priority_descending(_: void, a: Quorum, b: Quorum) bool {
        assert(a.sector.checksum != b.sector.checksum);
        assert(a.sector.magic == .superblock);
        assert(b.sector.magic == .superblock);

        if (a.valid and !b.valid) return true;
        if (b.valid and !a.valid) return false;

        if (a.sector.sequence > b.sector.sequence) return true;
        if (b.sector.sequence > a.sector.sequence) return false;

        if (a.count.count() > b.count.count()) return true;
        if (b.count.count() > a.count.count()) return false;

        // The sort order must be stable and deterministic:
        return a.sector.checksum > b.sector.checksum;
    }
};

test "SuperBlockSector" {
    const expect = std.testing.expect;

    var a = std.mem.zeroInit(SuperBlockSector, .{});
    a.set_checksum();

    assert(a.copy == 0);
    try expect(a.valid_checksum());

    a.copy += 1;
    try expect(a.valid_checksum());

    a.replica += 1;
    try expect(!a.valid_checksum());
}

// TODO Add unit tests for Quorums.
// TODO Test invariants and transitions across TestRunner functions.
const TestStorage = @import("../test/storage.zig").Storage;
const TestSuperBlock = SuperBlockType(TestStorage);

const TestRunner = struct {
    superblock: *TestSuperBlock,
    context_format: TestSuperBlock.Context = undefined,
    context_open: TestSuperBlock.Context = undefined,
    context_checkpoint: TestSuperBlock.Context = undefined,
    context_view_change: TestSuperBlock.Context = undefined,
    pending: usize = 0,

    fn format(runner: *TestRunner, options: TestSuperBlock.FormatOptions) void {
        runner.pending += 1;
        runner.superblock.format(format_callback, &runner.context_format, options);
    }

    fn format_callback(context: *TestSuperBlock.Context) void {
        const runner = @fieldParentPtr(TestRunner, "context_format", context);
        runner.pending -= 1;
        runner.open();
    }

    fn open(runner: *TestRunner) void {
        runner.pending += 1;
        runner.superblock.open(open_callback, &runner.context_open);
    }

    fn open_callback(context: *TestSuperBlock.Context) void {
        const runner = @fieldParentPtr(TestRunner, "context_open", context);
        runner.pending -= 1;
        runner.checkpoint();
        runner.view_change();
    }

    fn view_change(runner: *TestRunner) void {
        runner.pending += 1;
        runner.superblock.view_change(
            view_change_callback,
            &runner.context_view_change,
            .{
                .commit_min = runner.superblock.working.vsr_state.commit_min + 1,
                .commit_max = runner.superblock.working.vsr_state.commit_max + 2,
                .view_normal = runner.superblock.working.vsr_state.view_normal + 3,
                .view = runner.superblock.working.vsr_state.view + 4,
            },
        );
    }

    fn view_change_callback(context: *TestSuperBlock.Context) void {
        const runner = @fieldParentPtr(TestRunner, "context_view_change", context);
        runner.pending -= 1;
        runner.checkpoint();
    }

    fn checkpoint(runner: *TestRunner) void {
        runner.pending += 1;
        runner.superblock.checkpoint(checkpoint_callback, &runner.context_checkpoint);
    }

    fn checkpoint_callback(context: *TestSuperBlock.Context) void {
        const runner = @fieldParentPtr(TestRunner, "context_checkpoint", context);
        runner.pending -= 1;
    }
};

test "SuperBlock" {
    const cluster = 32;
    const replica = 4;
    const size_max = data_file_size_min;

    var storage = try TestStorage.init(std.testing.allocator, superblock_zone_size, .{
        .seed = 0,
        .read_latency_min = 1,
        .read_latency_mean = 1,
        .write_latency_min = 1,
        .write_latency_mean = 1,
        .read_fault_probability = 0,
        .write_fault_probability = 0,
    }, replica, .{
        .first_offset = superblock_zone_size,
        .period = 1,
    });
    defer storage.deinit(std.testing.allocator);

    var message_pool = try MessagePool.init(std.testing.allocator, .replica);
    defer message_pool.deinit(std.testing.allocator);

    var superblock = try TestSuperBlock.init(std.testing.allocator, &storage, &message_pool);
    defer superblock.deinit(std.testing.allocator);

    var runner = TestRunner{ .superblock = &superblock };
    runner.format(.{
        .cluster = cluster,
        .replica = replica,
        .size_max = size_max,
    });

    while (runner.pending > 0) storage.tick();
}
