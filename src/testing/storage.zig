//! In-memory storage, with simulated faults and latency.
//!
//!
//! Fault Injection
//!
//! Storage injects faults that the cluster can (i.e. should be able to) recover from.
//! Each zone can tolerate a different pattern of faults.
//!
//! - superblock:
//!   - One read/write fault is permitted per area (section, manifest, …).
//!   - An additional fault is permitted at the target of a pending write during a crash.
//!
//! - wal_headers, wal_prepares:
//!   - Read/write faults are distributed between replicas according to ClusterFaultAtlas, to ensure
//!     that at least one replica will have a valid copy to help others repair.
//!     (See: generate_faulty_wal_areas()).
//!   - When a replica crashes, it may fault the WAL outside of ClusterFaultAtlas.
//!   - When replica_count=1, its WAL can only be corrupted by a crash, never a read/write.
//!     (When replica_count=1, there are no other replicas to assist with repair).
//!
//! - grid: (TODO: Enable grid faults when grid repair is implemented).
//!
const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const FIFO = @import("../fifo.zig").FIFO;
const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const superblock = @import("../vsr/superblock.zig");
const BlockType = @import("../lsm/grid.zig").BlockType;
const stdx = @import("../stdx.zig");
const PriorityQueue = @import("./priority_queue.zig").PriorityQueue;
const fuzz = @import("./fuzz.zig");
const hash_log = @import("./hash_log.zig");

const log = std.log.scoped(.storage);

// TODOs:
// less than a majority of replicas may have corruption
// have an option to enable/disable the following corruption types:
// bitrot
// misdirected read/write
// corrupt sector
// latent sector error
// - emulate by zeroing sector, as this is how we handle this in the real Storage implementation
// - likely that surrounding sectors also corrupt
// - likely that stuff written at the same time is also corrupt even if written to a far away sector
pub const Storage = struct {
    /// Options for fault injection during fuzz testing
    pub const Options = struct {
        /// Seed for the storage PRNG.
        seed: u64 = 0,

        /// Required when `fault_atlas` is set.
        replica_index: ?u8 = null,

        /// Minimum number of ticks it may take to read data.
        read_latency_min: u64,
        /// Average number of ticks it may take to read data. Must be >= read_latency_min.
        read_latency_mean: u64,
        /// Minimum number of ticks it may take to write data.
        write_latency_min: u64,
        /// Average number of ticks it may take to write data. Must be >= write_latency_min.
        write_latency_mean: u64,

        /// Chance out of 100 that a read will corrupt a sector, if the target memory is within
        /// a faulty area of this replica.
        read_fault_probability: u8 = 0,
        /// Chance out of 100 that a write will corrupt a sector, if the target memory is within
        /// a faulty area of this replica.
        write_fault_probability: u8 = 0,
        /// Chance out of 100 that a crash will corrupt a sector of a pending write's target,
        /// if the target memory is within a faulty area of this replica.
        crash_fault_probability: u8 = 0,

        /// Enable/disable automatic read/write faults.
        /// Does not impact crash faults or manual faults.
        fault_atlas: ?*const ClusterFaultAtlas = null,
    };

    /// See usage in Journal.write_sectors() for details.
    /// TODO: allow testing in both modes.
    pub const synchronicity: enum {
        always_synchronous,
        always_asynchronous,
    } = .always_asynchronous;

    pub const Read = struct {
        callback: fn (read: *Storage.Read) void,
        buffer: []u8,
        zone: vsr.Zone,
        /// Relative offset within the zone.
        offset: u64,
        /// Tick at which this read is considered "completed" and the callback should be called.
        done_at_tick: u64,

        fn less_than(context: void, a: *Read, b: *Read) math.Order {
            _ = context;

            return math.order(a.done_at_tick, b.done_at_tick);
        }
    };

    pub const Write = struct {
        callback: fn (write: *Storage.Write) void,
        buffer: []const u8,
        zone: vsr.Zone,
        /// Relative offset within the zone.
        offset: u64,
        /// Tick at which this write is considered "completed" and the callback should be called.
        done_at_tick: u64,

        fn less_than(context: void, a: *Write, b: *Write) math.Order {
            _ = context;

            return math.order(a.done_at_tick, b.done_at_tick);
        }
    };

    pub const NextTick = struct {
        next: ?*NextTick = null,
        callback: fn (next_tick: *NextTick) void,
    };

    allocator: mem.Allocator,

    size: u64,
    options: Options,
    prng: std.rand.DefaultPrng,

    memory: []align(constants.sector_size) u8,
    /// Set bits correspond to sectors that have ever been written to.
    memory_written: std.DynamicBitSetUnmanaged,
    /// Set bits correspond to faulty sectors. The underlying sectors of `memory` is left clean.
    faults: std.DynamicBitSetUnmanaged,

    /// Whether to enable faults (when false, this supersedes `faulty_wal_areas`).
    /// This is used to disable faults during the replica's first startup.
    faulty: bool = true,

    reads: PriorityQueue(*Storage.Read, void, Storage.Read.less_than),
    writes: PriorityQueue(*Storage.Write, void, Storage.Write.less_than),

    ticks: u64 = 0,
    next_tick_queue: FIFO(NextTick) = .{},

    pub fn init(allocator: mem.Allocator, size: u64, options: Storage.Options) !Storage {
        assert(options.write_latency_mean >= options.write_latency_min);
        assert(options.read_latency_mean >= options.read_latency_min);
        assert(options.fault_atlas == null or options.replica_index != null);

        var prng = std.rand.DefaultPrng.init(options.seed);
        const sector_count = @divExact(size, constants.sector_size);
        const memory = try allocator.allocAdvanced(u8, constants.sector_size, size, .exact);
        errdefer allocator.free(memory);
        // TODO: random data
        mem.set(u8, memory, 0);

        var memory_written = try std.DynamicBitSetUnmanaged.initEmpty(allocator, sector_count);
        errdefer memory_written.deinit(allocator);

        var faults = try std.DynamicBitSetUnmanaged.initEmpty(allocator, sector_count);
        errdefer faults.deinit(allocator);

        var reads = PriorityQueue(*Storage.Read, void, Storage.Read.less_than).init(allocator, {});
        errdefer reads.deinit();
        try reads.ensureTotalCapacity(constants.iops_read_max);

        var writes = PriorityQueue(*Storage.Write, void, Storage.Write.less_than).init(allocator, {});
        errdefer writes.deinit();
        try writes.ensureTotalCapacity(constants.iops_write_max);

        return Storage{
            .allocator = allocator,
            .size = size,
            .options = options,
            .prng = prng,
            .memory = memory,
            .memory_written = memory_written,
            .faults = faults,
            .reads = reads,
            .writes = writes,
        };
    }

    pub fn deinit(storage: *Storage, allocator: mem.Allocator) void {
        allocator.free(storage.memory);
        storage.memory_written.deinit(allocator);
        storage.faults.deinit(allocator);
        storage.reads.deinit();
        storage.writes.deinit();
    }

    /// Cancel any currently in-progress reads/writes.
    /// Corrupt the target sectors of any in-progress writes.
    pub fn reset(storage: *Storage) void {
        while (storage.writes.peek()) |_| {
            const write = storage.writes.remove();
            if (!storage.x_in_100(storage.options.crash_fault_probability)) continue;

            // Randomly corrupt one of the faulty sectors the operation targeted.
            // TODO: inject more realistic and varied storage faults as described above.
            const sectors = SectorRange.from_zone(write.zone, write.offset, write.buffer.len);
            storage.fault_sector(write.zone, sectors.random(storage.prng.random()));
        }
        assert(storage.writes.len == 0);

        storage.reads.len = 0;
        storage.next_tick_queue = .{};
    }

    /// Returns the number of bytes that have been written to, assuming that (the simulated)
    /// `fallocate()` creates a sparse file.
    pub fn size_used(storage: *const Storage) usize {
        return storage.memory_written.count() * constants.sector_size;
    }

    /// Copy state from `origin` to `storage`:
    ///
    /// - ticks
    /// - memory
    /// - occupied memory
    /// - faulty sectors
    /// - reads in-progress
    /// - writes in-progress
    ///
    /// Both instances must have an identical size.
    pub fn copy(storage: *Storage, origin: *const Storage) void {
        assert(storage.size == origin.size);

        storage.ticks = origin.ticks;
        stdx.copy_disjoint(.exact, u8, storage.memory, origin.memory);
        storage.memory_written.toggleSet(storage.memory_written);
        storage.memory_written.toggleSet(origin.memory_written);
        storage.faults.toggleSet(storage.faults);
        storage.faults.toggleSet(origin.faults);

        storage.reads.len = 0;
        for (origin.reads.items[0..origin.reads.len]) |read| {
            storage.reads.add(read) catch unreachable;
        }

        storage.writes.len = 0;
        for (origin.writes.items[0..origin.writes.len]) |write| {
            storage.writes.add(write) catch unreachable;
        }
    }

    pub fn tick(storage: *Storage) void {
        storage.ticks += 1;

        while (storage.reads.peek()) |read| {
            if (read.done_at_tick > storage.ticks) break;
            _ = storage.reads.remove();
            storage.read_sectors_finish(read);
        }

        while (storage.writes.peek()) |write| {
            if (write.done_at_tick > storage.ticks) break;
            _ = storage.writes.remove();
            storage.write_sectors_finish(write);
        }

        while (storage.next_tick_queue.pop()) |next_tick| {
            next_tick.callback(next_tick);
        }
    }

    pub fn on_next_tick(
        storage: *Storage,
        callback: fn (next_tick: *Storage.NextTick) void,
        next_tick: *Storage.NextTick,
    ) void {
        next_tick.* = .{ .callback = callback };
        storage.next_tick_queue.push(next_tick);
    }

    /// * Verifies that the read fits within the target sector.
    /// * Verifies that the read targets sectors that have been written to.
    pub fn read_sectors(
        storage: *Storage,
        callback: fn (read: *Storage.Read) void,
        read: *Storage.Read,
        buffer: []u8,
        zone: vsr.Zone,
        offset_in_zone: u64,
    ) void {
        hash_log.emit_autohash(.{ buffer, zone, offset_in_zone }, .DeepRecursive);

        verify_alignment(buffer);

        var sectors = SectorRange.from_zone(zone, offset_in_zone, buffer.len);
        while (sectors.next()) |sector| assert(storage.memory_written.isSet(sector));

        read.* = .{
            .callback = callback,
            .buffer = buffer,
            .zone = zone,
            .offset = offset_in_zone,
            .done_at_tick = storage.ticks + storage.read_latency(),
        };

        // We ensure the capacity is sufficient for constants.iops_read_max in init()
        storage.reads.add(read) catch unreachable;
    }

    fn read_sectors_finish(storage: *Storage, read: *Storage.Read) void {
        hash_log.emit_autohash(.{ read.buffer, read.zone, read.offset }, .DeepRecursive);

        const offset_in_storage = read.zone.offset(read.offset);
        stdx.copy_disjoint(
            .exact,
            u8,
            read.buffer,
            storage.memory[offset_in_storage..][0..read.buffer.len],
        );

        if (storage.x_in_100(storage.options.read_fault_probability)) {
            storage.fault_faulty_sectors(read.zone, read.offset, read.buffer.len);
        }

        if (storage.faulty) {
            // Corrupt faulty sectors.
            var sectors = SectorRange.from_zone(read.zone, read.offset, read.buffer.len);
            const sectors_min = sectors.min;
            while (sectors.next()) |sector| {
                if (storage.faults.isSet(sector)) {
                    const faulty_sector_offset = (sector - sectors_min) * constants.sector_size;
                    const faulty_sector_bytes = read.buffer[faulty_sector_offset..][0..constants.sector_size];
                    storage.prng.random().bytes(faulty_sector_bytes);
                }
            }
        }

        read.callback(read);
    }

    pub fn write_sectors(
        storage: *Storage,
        callback: fn (write: *Storage.Write) void,
        write: *Storage.Write,
        buffer: []const u8,
        zone: vsr.Zone,
        offset_in_zone: u64,
    ) void {
        hash_log.emit_autohash(.{ buffer, zone, offset_in_zone }, .DeepRecursive);

        verify_alignment(buffer);

        // Verify that there are no concurrent overlapping writes.
        var iterator = storage.writes.iterator();
        while (iterator.next()) |other| {
            if (other.zone != zone) continue;
            assert(offset_in_zone + buffer.len <= other.offset or
                other.offset + other.buffer.len <= offset_in_zone);
        }

        write.* = .{
            .callback = callback,
            .buffer = buffer,
            .zone = zone,
            .offset = offset_in_zone,
            .done_at_tick = storage.ticks + storage.write_latency(),
        };

        // We ensure the capacity is sufficient for constants.iops_write_max in init()
        storage.writes.add(write) catch unreachable;
    }

    fn write_sectors_finish(storage: *Storage, write: *Storage.Write) void {
        hash_log.emit_autohash(.{ write.buffer, write.zone, write.offset }, .DeepRecursive);

        const offset_in_storage = write.zone.offset(write.offset);
        stdx.copy_disjoint(
            .exact,
            u8,
            storage.memory[offset_in_storage..][0..write.buffer.len],
            write.buffer,
        );

        var sectors = SectorRange.from_zone(write.zone, write.offset, write.buffer.len);
        while (sectors.next()) |sector| {
            storage.faults.unset(sector);
            storage.memory_written.set(sector);
        }

        if (storage.x_in_100(storage.options.write_fault_probability)) {
            storage.fault_faulty_sectors(write.zone, write.offset, write.buffer.len);
        }

        write.callback(write);
    }

    fn read_latency(storage: *Storage) u64 {
        return storage.latency(storage.options.read_latency_min, storage.options.read_latency_mean);
    }

    fn write_latency(storage: *Storage) u64 {
        return storage.latency(storage.options.write_latency_min, storage.options.write_latency_mean);
    }

    fn latency(storage: *Storage, min: u64, mean: u64) u64 {
        return min + fuzz.random_int_exponential(storage.prng.random(), u64, mean - min);
    }

    /// Return true with probability x/100.
    fn x_in_100(storage: *Storage, x: u8) bool {
        assert(x <= 100);
        return x > storage.prng.random().uintLessThan(u8, 100);
    }

    fn fault_faulty_sectors(storage: *Storage, zone: vsr.Zone, offset_in_zone: u64, size: u64) void {
        const atlas = storage.options.fault_atlas orelse return;
        const replica_index = storage.options.replica_index.?;
        const faulty_sectors = switch (zone) {
            .superblock => atlas.faulty_superblock(replica_index, offset_in_zone, size),
            .wal_headers => atlas.faulty_wal_headers(replica_index, offset_in_zone, size),
            .wal_prepares => atlas.faulty_wal_prepares(replica_index, offset_in_zone, size),
            .grid => null,
        } orelse return;

        // Randomly corrupt one of the faulty sectors the operation targeted.
        // TODO: inject more realistic and varied storage faults as described above.
        storage.fault_sector(zone, faulty_sectors.random(storage.prng.random()));
    }

    fn fault_sector(storage: *Storage, zone: vsr.Zone, sector: usize) void {
        storage.faults.set(sector);
        if (storage.options.replica_index) |replica_index| {
            log.debug("{}: corrupting sector at zone={} offset={}", .{
                replica_index,
                zone,
                sector * constants.sector_size - zone.offset(0),
            });
        }
    }

    pub fn superblock_header(
        storage: *const Storage,
        copy_: u8,
    ) *const superblock.SuperBlockHeader {
        const offset = vsr.Zone.superblock.offset(superblock.areas.header.offset(copy_));
        const bytes = storage.memory[offset..][0..superblock.areas.header.size_max];
        return mem.bytesAsValue(superblock.SuperBlockHeader, bytes);
    }

    pub fn wal_headers(storage: *const Storage) []const vsr.Header {
        const offset = vsr.Zone.wal_headers.offset(0);
        const size = vsr.Zone.wal_headers.size().?;
        return mem.bytesAsSlice(vsr.Header, storage.memory[offset..][0..size]);
    }

    const MessageRaw = extern struct {
        header: vsr.Header,
        body: [constants.message_size_max - @sizeOf(vsr.Header)]u8,

        comptime {
            assert(@sizeOf(MessageRaw) == constants.message_size_max);
            assert(@sizeOf(MessageRaw) * 8 == @bitSizeOf(MessageRaw));
        }
    };

    pub fn wal_prepares(storage: *const Storage) []const MessageRaw {
        const offset = vsr.Zone.wal_prepares.offset(0);
        const size = vsr.Zone.wal_prepares.size().?;
        return mem.bytesAsSlice(MessageRaw, storage.memory[offset..][0..size]);
    }

    pub fn grid_block(
        storage: *const Storage,
        address: u64,
    ) *align(constants.sector_size) [constants.block_size]u8 {
        assert(address > 0);

        const block_offset = vsr.Zone.grid.offset((address - 1) * constants.block_size);
        const block_header = mem.bytesToValue(
            vsr.Header,
            storage.memory[block_offset..][0..@sizeOf(vsr.Header)],
        );
        assert(storage.memory_written.isSet(@divExact(block_offset, constants.sector_size)));
        assert(block_header.valid_checksum());
        assert(block_header.size <= constants.block_size);

        return storage.memory[block_offset..][0..constants.block_size];
    }
};

fn verify_alignment(buffer: []const u8) void {
    assert(buffer.len > 0);

    // Ensure that the read or write is aligned correctly for Direct I/O:
    // If this is not the case, the underlying syscall will return EINVAL.
    assert(@mod(@ptrToInt(buffer.ptr), constants.sector_size) == 0);
    assert(@mod(buffer.len, constants.sector_size) == 0);
}

pub const Area = union(enum) {
    superblock: struct { area: superblock.Area, copy: u8 },
    wal_headers: struct { sector: usize },
    wal_prepares: struct { slot: usize },
    grid: struct { address: u64 },

    fn sectors(area: Area) SectorRange {
        switch (area) {
            .superblock => |data| SectorRange.from_zone(
                .superblock,
                @field(superblock.areas, data.area).offset(data.copy),
                @field(superblock.areas, data.area).size_max,
            ),
            .wal_headers => |data| SectorRange.from_zone(
                .wal_headers,
                constants.sector_size * data.sector,
                constants.sector_size,
            ),
            .wal_prepares => |data| SectorRange.from_zone(
                .wal_prepares,
                constants.message_size_max * data.slot,
                constants.message_size_max,
            ),
            .grid => |data| SectorRange.from_zone(
                .grid,
                constants.block_size * (data.address - 1),
                constants.block_size,
            ),
        }
    }
};

const SectorRange = struct {
    min: usize, // inclusive sector index
    max: usize, // exclusive sector index

    fn from_zone(
        zone: vsr.Zone,
        offset_in_zone: u64,
        size: usize,
    ) SectorRange {
        return from_offset(zone.offset(offset_in_zone), size);
    }

    fn from_offset(offset_in_storage: u64, size: usize) SectorRange {
        return .{
            .min = @divExact(offset_in_storage, constants.sector_size),
            .max = @divExact(offset_in_storage + size, constants.sector_size),
        };
    }

    fn random(range: SectorRange, rand: std.rand.Random) usize {
        return range.min + rand.uintLessThan(usize, range.max - range.min);
    }

    fn next(range: *SectorRange) ?usize {
        if (range.min == range.max) return null;
        defer range.min += 1;
        return range.min;
    }

    fn intersect(a: SectorRange, b: SectorRange) ?SectorRange {
        if (a.max <= b.min) return null;
        if (b.max <= a.min) return null;
        return SectorRange{
            .min = std.math.max(a.min, b.min),
            .max = std.math.min(a.max, b.max),
        };
    }
};

/// To ensure the cluster can recover, each header/prepare/block must be valid (not faulty) at
/// a majority of replicas.
///
/// We can't allow WAL storage faults for the same message in a majority of
/// the replicas as that would make recovery impossible. Instead, we only
/// allow faults in certain areas which differ between replicas.
// TODO Support total superblock corruption, forcing a full state transfer.
pub const ClusterFaultAtlas = struct {
    pub const Options = struct {
        faulty_superblock: bool,
        faulty_wal_headers: bool,
        faulty_wal_prepares: bool,
        // TODO grid
    };

    /// This is the maximum number of faults per-trailer-area that can be safely injected on a read
    /// or write to the superblock zone.
    ///
    /// It does not include the additional "torn write" fault injected upon a crash.
    ///
    /// For SuperBlockHeader, checkpoint() and view_change() require 3/4 valid headers (1
    /// fault). Trailers are likewise 3/4 + 1 fault — consider if two faults were injected:
    /// 1. `SuperBlock.checkpoint()` for sequence=6.
    ///   - write copy 0, corrupt manifest (fault_count=1)
    ///   - write copy 1, corrupt manifest (fault_count=2) !
    /// 2. Crash. Recover.
    /// 3. `SuperBlock.open()`. The highest valid quorum is sequence=6, but there is no
    ///    valid manifest.
    const superblock_trailer_faults_max = @divExact(constants.superblock_copies, 2) - 1;

    comptime {
        assert(superblock_trailer_faults_max >= 1);
    }

    const CopySet = std.StaticBitSet(constants.superblock_copies);
    const ReplicaSet = std.StaticBitSet(constants.replicas_max);
    const headers_per_sector = @divExact(constants.sector_size, @sizeOf(vsr.Header));
    const header_sectors = @divExact(constants.journal_slot_count, headers_per_sector);

    const FaultySuperBlockAreas = std.enums.EnumArray(superblock.Area, CopySet);
    const FaultyWALHeaders = std.StaticBitSet(@divExact(
        constants.journal_size_headers,
        constants.sector_size,
    ));

    options: Options,
    faulty_superblock_areas: FaultySuperBlockAreas =
        FaultySuperBlockAreas.initFill(CopySet.initEmpty()),
    faulty_wal_header_sectors: [constants.replicas_max]FaultyWALHeaders =
        [_]FaultyWALHeaders{FaultyWALHeaders.initEmpty()} ** constants.replicas_max,

    pub fn init(replica_count: u8, random: std.rand.Random, options: Options) ClusterFaultAtlas {
        // If there is only one replica in the cluster, WAL/Grid faults are not recoverable.
        // TODO Can we allow Header faults only?
        assert(replica_count > 1 or options.faulty_wal_headers == false);
        assert(replica_count > 1 or options.faulty_wal_prepares == false);

        var atlas = ClusterFaultAtlas{ .options = options };

        {
            for (&atlas.faulty_superblock_areas.values) |*copies, area| {
                if (area == @enumToInt(superblock.Area.header)) {
                    // Only inject read/write faults into trailers, not the header.
                    // This prevents the quorum from being lost like so:
                    // - copy₀: B (ok)
                    // - copy₁: B (torn write)
                    // - copy₂: A (corrupt)
                    // - copy₃: A (ok)
                } else {
                    var area_faults: usize = 0;
                    while (area_faults < superblock_trailer_faults_max) : (area_faults += 1) {
                        copies.set(random.uintLessThan(usize, constants.superblock_copies));
                    }
                }
            }
        }

        // A cluster-of-2 is special-cased to mirror the special case in replica.zig.
        // See repair_prepare()/on_nack_prepare().
        const quorums = vsr.quorums(replica_count);
        const faults_max = if (replica_count == 2) 1 else replica_count - quorums.replication;
        assert(faults_max < replica_count);
        assert(faults_max > 0 or replica_count == 1);

        var wal_header_sectors = [_]ReplicaSet{ReplicaSet.initEmpty()} ** header_sectors;
        for (wal_header_sectors) |*wal_header_sector, sector| {
            while (wal_header_sector.count() < faults_max) {
                const replica_index = random.uintLessThan(u8, replica_count);
                wal_header_sector.set(replica_index);
                atlas.faulty_wal_header_sectors[replica_index].set(sector);
            }
        }

        return atlas;
    }

    /// Returns a range of faulty sectors which intersect the specified range.
    fn faulty_superblock(
        atlas: ClusterFaultAtlas,
        replica_index: usize,
        offset_in_zone: u64,
        size: u64,
    ) ?SectorRange {
        _ = replica_index;
        if (!atlas.options.faulty_superblock) return null;

        const copy = @divFloor(offset_in_zone, superblock.superblock_copy_size);
        const offset_in_copy = offset_in_zone % superblock.superblock_copy_size;
        const area: superblock.Area = switch (offset_in_copy) {
            superblock.areas.header.base => .header,
            superblock.areas.manifest.base => .manifest,
            superblock.areas.free_set.base => .free_set,
            superblock.areas.client_table.base => .client_table,
            else => unreachable,
        };

        if (atlas.faulty_superblock_areas.get(area).isSet(copy)) {
            return SectorRange.from_zone(.superblock, offset_in_zone, size);
        } else {
            return null;
        }
    }

    /// Returns a range of faulty sectors which intersect the specified range.
    fn faulty_wal_headers(
        atlas: ClusterFaultAtlas,
        replica_index: usize,
        offset_in_zone: u64,
        size: u64,
    ) ?SectorRange {
        if (!atlas.options.faulty_wal_headers) return null;
        return faulty_sectors(
            FaultyWALHeaders.bit_length,
            constants.sector_size,
            .wal_headers,
            &atlas.faulty_wal_header_sectors[replica_index],
            offset_in_zone,
            size,
        );
    }

    /// Returns a range of faulty sectors which intersect the specified range.
    fn faulty_wal_prepares(
        atlas: ClusterFaultAtlas,
        replica_index: usize,
        offset_in_zone: u64,
        size: u64,
    ) ?SectorRange {
        if (!atlas.options.faulty_wal_prepares) return null;
        return faulty_sectors(
            FaultyWALHeaders.bit_length,
            constants.message_size_max * headers_per_sector,
            .wal_prepares,
            &atlas.faulty_wal_header_sectors[replica_index],
            offset_in_zone,
            size,
        );
    }

    fn faulty_sectors(
        comptime chunk_count: usize,
        comptime chunk_size: usize,
        comptime zone: vsr.Zone,
        faulty_chunks: *const std.StaticBitSet(chunk_count),
        offset_in_zone: u64,
        size: u64,
    ) ?SectorRange {
        var fault_start: ?usize = null;
        var fault_count: usize = 0;

        var chunk: usize = @divFloor(offset_in_zone, chunk_size);
        while (chunk * chunk_size < offset_in_zone + size) : (chunk += 1) {
            if (faulty_chunks.isSet(chunk)) {
                if (fault_start == null) fault_start = chunk;
                fault_count += 1;
            } else {
                if (fault_start != null) break;
            }
        }

        if (fault_start) |start| {
            return SectorRange.from_zone(
                zone,
                chunk_size * start,
                chunk_size * fault_count,
            ).intersect(SectorRange.from_zone(zone, offset_in_zone, size)).?;
        } else {
            return null;
        }
    }
};
