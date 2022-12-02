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
//!   - Read/write faults are distributed between replicas according to FaultyAreas, to ensure
//!     that at least one replica will have a valid copy to help others repair.
//!     (See: generate_faulty_wal_areas()).
//!   - When a replica crashes, it may fault the WAL outside of FaultyAreas.
//!   - When replica_count=1, its WAL can only be corrupted by a crash, never a read/write.
//!     (When replica_count=1, there are no other replicas to assist with repair).
//!
//! - grid: (TODO: Enable grid faults when grid repair is implemented).
//!
const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const superblock = @import("../vsr/superblock.zig");
const BlockType = @import("../lsm/grid.zig").BlockType;
const util = @import("../util.zig");

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

        /// (Only used for logging.)
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

        /// Enable/disable SuperBlock zone faults.
        faulty_superblock: bool = false,

        // In the WAL, we can't allow storage faults for the same message in a majority of
        // the replicas as that would make recovery impossible. Instead, we only
        // allow faults in certain areas which differ between replicas.
        faulty_wal_areas: ?FaultyAreas = null,
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

    /// Faulty areas are always sized to message_size_max
    /// If the faulty areas of all replicas are superimposed, the padding between them is always message_size_max.
    /// For a single replica, the padding between faulty areas depends on the number of other replicas.
    pub const FaultyAreas = struct {
        first_offset: u64,
        period: u64,
    };

    allocator: mem.Allocator,

    memory: []align(constants.sector_size) u8,
    /// Set bits correspond to sectors that have ever been written to.
    memory_written: std.DynamicBitSetUnmanaged,
    /// Set bits correspond to faulty sectors. The underlying sectors of `memory` is left clean.
    faults: std.DynamicBitSetUnmanaged,

    size: u64,

    options: Options,
    prng: std.rand.DefaultPrng,

    /// Whether to enable faults (when false, this supersedes `faulty_wal_areas`).
    /// This is used to disable faults during the replica's first startup.
    faulty: bool = true,

    reads: std.PriorityQueue(*Storage.Read, void, Storage.Read.less_than),
    writes: std.PriorityQueue(*Storage.Write, void, Storage.Write.less_than),

    ticks: u64 = 0,

    pub fn init(allocator: mem.Allocator, size: u64, options: Storage.Options) !Storage {
        assert(options.write_latency_mean >= options.write_latency_min);
        assert(options.read_latency_mean >= options.read_latency_min);

        const memory = try allocator.allocAdvanced(u8, constants.sector_size, size, .exact);
        errdefer allocator.free(memory);
        // TODO: random data
        mem.set(u8, memory, 0);

        var memory_written = try std.DynamicBitSetUnmanaged.initEmpty(
            allocator,
            @divExact(size, constants.sector_size),
        );
        errdefer memory_written.deinit(allocator);

        var faults = try std.DynamicBitSetUnmanaged.initEmpty(
            allocator,
            @divExact(size, constants.sector_size),
        );
        errdefer faults.deinit(allocator);

        var reads = std.PriorityQueue(*Storage.Read, void, Storage.Read.less_than).init(allocator, {});
        errdefer reads.deinit();
        try reads.ensureTotalCapacity(constants.iops_read_max);

        var writes = std.PriorityQueue(*Storage.Write, void, Storage.Write.less_than).init(allocator, {});
        errdefer writes.deinit();
        try writes.ensureTotalCapacity(constants.iops_write_max);

        return Storage{
            .allocator = allocator,
            .memory = memory,
            .memory_written = memory_written,
            .faults = faults,
            .size = size,
            .options = options,
            .prng = std.rand.DefaultPrng.init(options.seed),
            .reads = reads,
            .writes = writes,
        };
    }

    /// Cancel any currently in-progress reads/writes.
    /// Corrupt the target sectors of any in-progress writes.
    pub fn reset(storage: *Storage) void {
        while (storage.writes.peek()) |_| {
            const write = storage.writes.remove();
            if (switch (write.zone) {
                .superblock => !storage.options.faulty_superblock,
                // On crash, the WAL may be corrupted outside of the FaultyAreas.
                .wal_headers, .wal_prepares => storage.options.faulty_wal_areas == null,
                // TODO Enable fault injection for grid.
                .grid => true,
            }) continue;

            if (!storage.x_in_100(storage.options.crash_fault_probability)) continue;

            const sector_min = @divExact(write.zone.offset(write.offset), constants.sector_size);
            const sector_max = sector_min + @divExact(write.buffer.len, constants.sector_size);

            // Randomly corrupt one of the faulty sectors the operation targeted.
            // TODO: inject more realistic and varied storage faults as described above.
            storage.fault_sector(
                write.zone,
                storage.random_uint_between(usize, sector_min, sector_max),
            );
        }
        assert(storage.writes.len == 0);

        storage.reads.len = 0;
    }

    pub fn deinit(storage: *Storage, allocator: mem.Allocator) void {
        allocator.free(storage.memory);
        storage.memory_written.deinit(allocator);
        storage.faults.deinit(allocator);
        storage.reads.deinit();
        storage.writes.deinit();
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
        util.copy_disjoint(.exact, u8, storage.memory, origin.memory);
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
        if (zone.size()) |zone_size| {
            assert(offset_in_zone + buffer.len <= zone_size);
        }

        const offset_in_storage = zone.offset(offset_in_zone);
        storage.verify_bounds_and_alignment(buffer, offset_in_storage);

        {
            const sector_min = @divExact(offset_in_storage, constants.sector_size);
            const sector_max = @divExact(offset_in_storage + buffer.len, constants.sector_size);
            var sector: usize = sector_min;
            while (sector < sector_max) : (sector += 1) {
                assert(storage.memory_written.isSet(sector));
            }
        }

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
        const offset_in_storage = read.zone.offset(read.offset);
        util.copy_disjoint(
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
            const sector_min = @divExact(offset_in_storage, constants.sector_size);
            const sector_max = @divExact(offset_in_storage + read.buffer.len, constants.sector_size);
            var sector: usize = sector_min;
            while (sector < sector_max) : (sector += 1) {
                if (storage.faults.isSet(sector)) {
                    const faulty_sector_offset = (sector - sector_min) * constants.sector_size;
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
        if (zone.size()) |zone_size| {
            assert(offset_in_zone + buffer.len <= zone_size);
        }

        storage.verify_bounds_and_alignment(buffer, zone.offset(offset_in_zone));

        // Verify that there are no concurrent overlapping writes.
        var iterator = storage.writes.iterator();
        while (iterator.next()) |other| {
            if (other.zone != zone) continue;
            assert(offset_in_zone + buffer.len <= other.offset or
                other.offset + other.buffer.len <= offset_in_zone);
        }

        switch (zone) {
            .superblock => storage.verify_write_superblock(buffer, offset_in_zone),
            .wal_headers => {
                for (std.mem.bytesAsSlice(vsr.Header, buffer)) |header| {
                    storage.verify_write_wal_header(header);
                }
            },
            else => {},
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
        const offset_in_storage = write.zone.offset(write.offset);
        util.copy_disjoint(
            .exact,
            u8,
            storage.memory[offset_in_storage..][0..write.buffer.len],
            write.buffer,
        );

        const sector_min = @divExact(offset_in_storage, constants.sector_size);
        const sector_max = @divExact(offset_in_storage + write.buffer.len, constants.sector_size);
        var sector: usize = sector_min;
        while (sector < sector_max) : (sector += 1) {
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
        return min + @floatToInt(u64, @intToFloat(f64, mean - min) * storage.prng.random().floatExp(f64));
    }

    /// Return true with probability x/100.
    fn x_in_100(storage: *Storage, x: u8) bool {
        assert(x <= 100);
        return x > storage.prng.random().uintLessThan(u8, 100);
    }

    fn random_uint_between(storage: *Storage, comptime T: type, min: T, max: T) T {
        return min + storage.prng.random().uintLessThan(T, max - min);
    }

    /// The return value is a slice into the provided out array.
    pub fn generate_faulty_wal_areas(
        prng: std.rand.Random,
        size: u64,
        replica_count: u8,
        out: *[constants.replicas_max]FaultyAreas,
    ) []FaultyAreas {
        comptime assert(constants.message_size_max % constants.sector_size == 0);
        const message_size_max = constants.message_size_max;

        // We need to ensure there is message_size_max fault-free padding
        // between faulty areas of memory so that a single message
        // cannot straddle the corruptable areas of a majority of replicas.
        comptime assert(constants.replicas_max == 6);
        switch (replica_count) {
            1 => {
                // If there is only one replica in the cluster, storage faults are not recoverable.
                out[0] = .{ .first_offset = size, .period = 1 };
            },
            2 => {
                //  0123456789
                // 0X   X   X
                // 1  X   X   X
                out[0] = .{ .first_offset = 0 * message_size_max, .period = 4 * message_size_max };
                out[1] = .{ .first_offset = 2 * message_size_max, .period = 4 * message_size_max };
            },
            3 => {
                //  0123456789
                // 0X     X
                // 1  X     X
                // 2    X     X
                out[0] = .{ .first_offset = 0 * message_size_max, .period = 6 * message_size_max };
                out[1] = .{ .first_offset = 2 * message_size_max, .period = 6 * message_size_max };
                out[2] = .{ .first_offset = 4 * message_size_max, .period = 6 * message_size_max };
            },
            4 => {
                //  0123456789
                // 0X   X   X
                // 1X   X   X
                // 2  X   X   X
                // 3  X   X   X
                out[0] = .{ .first_offset = 0 * message_size_max, .period = 4 * message_size_max };
                out[1] = .{ .first_offset = 0 * message_size_max, .period = 4 * message_size_max };
                out[2] = .{ .first_offset = 2 * message_size_max, .period = 4 * message_size_max };
                out[3] = .{ .first_offset = 2 * message_size_max, .period = 4 * message_size_max };
            },
            5 => {
                //  0123456789
                // 0X     X
                // 1X     X
                // 2  X     X
                // 3  X     X
                // 4    X     X
                out[0] = .{ .first_offset = 0 * message_size_max, .period = 6 * message_size_max };
                out[1] = .{ .first_offset = 0 * message_size_max, .period = 6 * message_size_max };
                out[2] = .{ .first_offset = 2 * message_size_max, .period = 6 * message_size_max };
                out[3] = .{ .first_offset = 2 * message_size_max, .period = 6 * message_size_max };
                out[4] = .{ .first_offset = 4 * message_size_max, .period = 6 * message_size_max };
            },
            6 => {
                //  0123456789
                // 0X     X
                // 1X     X
                // 2  X     X
                // 3  X     X
                // 4    X     X
                // 5    X     X
                out[0] = .{ .first_offset = 0 * message_size_max, .period = 6 * message_size_max };
                out[1] = .{ .first_offset = 0 * message_size_max, .period = 6 * message_size_max };
                out[2] = .{ .first_offset = 2 * message_size_max, .period = 6 * message_size_max };
                out[3] = .{ .first_offset = 2 * message_size_max, .period = 6 * message_size_max };
                out[4] = .{ .first_offset = 4 * message_size_max, .period = 6 * message_size_max };
                out[5] = .{ .first_offset = 4 * message_size_max, .period = 6 * message_size_max };
            },
            else => unreachable,
        }

        {
            // Allow at most `f` faulty replicas to ensure the view change can succeed.
            // TODO Allow more than `f` faulty replicas when the fault is to the right of the
            // highest known replica.op (and to the left of the last checkpointed op).
            const majority = @divFloor(replica_count, 2) + 1;
            const quorum_replication = std.math.min(constants.quorum_replication_max, majority);
            const quorum_view_change = std.math.max(
                replica_count - quorum_replication + 1,
                majority,
            );
            var i: usize = quorum_view_change;
            while (i < replica_count) : (i += 1) {
                out[i].first_offset = size;
            }
        }

        prng.shuffle(FaultyAreas, out[0..replica_count]);
        return out[0..replica_count];
    }

    const SectorRange = struct {
        min: usize, // inclusive sector index
        max: usize, // exclusive sector index
    };

    /// Given an offset and size of a read/write, returns the range of any faulty sectors touched
    /// by the read/write.
    fn faulty_sectors(
        storage: *const Storage,
        zone: vsr.Zone,
        offset_in_zone: u64,
        size: u64,
    ) ?SectorRange {
        const offset_in_storage = zone.offset(offset_in_zone);

        if (zone == .superblock) {
            if (!storage.options.faulty_superblock) return null;

            const target_area = SuperBlockArea.from_offset(offset_in_zone);
            // This is the maximum number of faults per-area that can be safely injected on a read
            // or write to the superblock zone.
            //
            // For SuperBlockSector, checkpoint() and view_change() require 3/4 valid sectors (1
            // fault). Trailers are likewise 3/4 + 1 fault — consider if two faults were injected:
            // 1. `SuperBlock.checkpoint()` for sequence=6.
            //   - write copy 0, corrupt manifest (fault_count=1)
            //   - write copy 1, corrupt manifest (fault_count=2) !
            // 2. Crash. Recover.
            // 3. `SuperBlock.open()`. The highest valid quorum is sequence=6, but there is no
            //    valid manifest.
            const fault_count_max = @divExact(constants.superblock_copies, 2) - 1;
            assert(fault_count_max >= 1);

            const fault_count = blk: {
                var fault_count: usize = 0;
                var copy_: u8 = 0;
                while (copy_ < constants.superblock_copies) : (copy_ += 1) {
                    const copy_area = SuperBlockArea{ .group = target_area.group, .copy = copy_ };
                    const copy_area_offset_zone = copy_area.to_offset();
                    const copy_area_offset_storage = zone.offset(copy_area_offset_zone);
                    const copy_area_sector = @divExact(
                        copy_area_offset_storage,
                        constants.sector_size,
                    );
                    fault_count += @boolToInt(storage.faults.isSet(copy_area_sector));
                }
                break :blk fault_count;
            };

            // fault_count may be slightly greater than fault_count_max due to faults added by
            // `Storage.reset()` (a simulated crash).
            assert(fault_count <= fault_count_max + 1);
            if (fault_count >= fault_count_max) return null;

            // Always fault the first sector of the read/write so that we can easily test
            // `storage.faults` to probe the current `fault_count`.
            const sector = @divExact(offset_in_storage, constants.sector_size);
            return SectorRange{
                .min = sector,
                .max = sector + 1,
            };
        }

        if (zone == .wal_headers or zone == .wal_prepares) {
            const faulty_wal_areas = storage.options.faulty_wal_areas orelse return null;
            const message_size_max = constants.message_size_max;
            const period = faulty_wal_areas.period;

            const offset_faulty =
                faulty_wal_areas.first_offset + @divFloor(offset_in_storage, period) * period;

            const offset_start = std.math.max(offset_in_storage, offset_faulty);
            const offset_end = std.math.min(
                offset_in_storage + size,
                offset_faulty + message_size_max,
            );

            // The read/write does not touch any faulty sectors.
            if (offset_start >= offset_end) return null;

            return SectorRange{
                .min = @divExact(offset_start, constants.sector_size),
                .max = @divExact(offset_end, constants.sector_size),
            };
        }

        // TODO Support corruption of the grid.
        assert(zone == .grid);
        return null;
    }

    fn fault_faulty_sectors(storage: *Storage, zone: vsr.Zone, offset_in_zone: u64, size: u64) void {
        const faulty = storage.faulty_sectors(zone, offset_in_zone, size) orelse return;
        const target_sector_min = @divExact(zone.offset(offset_in_zone), constants.sector_size);
        const target_sector_max = target_sector_min + @divExact(size, constants.sector_size);
        assert(faulty.min < faulty.max);
        assert(faulty.min >= target_sector_min);
        assert(faulty.max <= target_sector_max);

        // Randomly corrupt one of the faulty sectors the operation targeted.
        // TODO: inject more realistic and varied storage faults as described above.
        storage.fault_sector(zone, storage.random_uint_between(usize, faulty.min, faulty.max));
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

    fn verify_bounds_and_alignment(storage: *const Storage, buffer: []const u8, offset: u64) void {
        assert(buffer.len > 0);
        assert(offset + buffer.len <= storage.size);

        // Ensure that the read or write is aligned correctly for Direct I/O:
        // If this is not the case, the underlying syscall will return EINVAL.
        assert(@mod(@ptrToInt(buffer.ptr), constants.sector_size) == 0);
        assert(@mod(buffer.len, constants.sector_size) == 0);
        assert(@mod(offset, constants.sector_size) == 0);
    }

    /// Each redundant header written must either:
    /// - match the corresponding (already written) prepare, or
    /// - be a command=reserved header (due to Journal.remove_entries_from), or
    /// - match the old redundant header (i.e. no change).
    ///   This last case applies when an in-memory header is changed after the prepare is written
    ///   but before the redundant header is written, so the journal defers the redundant header
    ///   update until after the new prepare has been written.
    fn verify_write_wal_header(storage: *const Storage, header: vsr.Header) void {
        // The checksum is zero when writing the header of a faulty prepare.
        if (header.checksum == 0) return;

        const header_slot = header.op % constants.journal_slot_count;
        const header_old = storage.wal_headers()[header_slot];

        const prepare_header = storage.wal_prepares()[header_slot].header;
        const prepare_offset = vsr.Zone.wal_prepares.offset(header_slot * constants.message_size_max);
        const prepare_sector = @divExact(prepare_offset, constants.sector_size);

        assert(storage.memory_written.isSet(prepare_sector));
        if (header.command == .prepare) {
            assert(header.checksum == header_old.checksum or
                header.checksum == prepare_header.checksum);
        } else {
            assert(header.command == .reserved);
        }
    }

    /// When a SuperBlock sector is written, verify:
    ///
    /// - There are no other pending writes or reads to the superblock zone.
    /// - All trailers are written.
    /// - All trailers' checksums validate.
    /// - All blocks referenced by the Manifest trailer exist.
    ///
    fn verify_write_superblock(storage: *const Storage, buffer: []const u8, offset_in_zone: u64) void {
        const Layout = superblock.Layout;
        assert(offset_in_zone < vsr.Zone.superblock.size().?);

        // Ignore trailer writes; only check the superblock sector writes.
        if (buffer.len != @sizeOf(superblock.SuperBlockSector)) return;
        var copy_: u8 = 0;
        while (copy_ < constants.superblock_copies) : (copy_ += 1) {
            if (Layout.offset_sector(copy_) == offset_in_zone) break;
        } else return;

        for (storage.reads.items[0..storage.reads.len]) |read| assert(read.zone != .superblock);
        for (storage.writes.items[0..storage.writes.len]) |write| assert(write.zone != .superblock);

        const sector = mem.bytesAsSlice(superblock.SuperBlockSector, buffer)[0];
        assert(sector.valid_checksum());
        assert(sector.vsr_state.internally_consistent());
        assert(sector.copy == copy_);

        const manifest_offset = vsr.Zone.superblock.offset(Layout.offset_manifest(copy_));
        const manifest_buffer = storage.memory[manifest_offset..][0..sector.manifest_size];
        assert(vsr.checksum(manifest_buffer) == sector.manifest_checksum);

        const free_set_offset = vsr.Zone.superblock.offset(Layout.offset_free_set(copy_));
        const free_set_buffer = storage.memory[free_set_offset..][0..sector.free_set_size];
        assert(vsr.checksum(free_set_buffer) == sector.free_set_checksum);

        const client_table_offset = vsr.Zone.superblock.offset(Layout.offset_client_table(copy_));
        const client_table_buffer =
            storage.memory[client_table_offset..][0..sector.client_table_size];
        assert(vsr.checksum(client_table_buffer) == sector.client_table_checksum);

        const Manifest = superblock.SuperBlockManifest;
        var manifest = Manifest.init(
            storage.allocator,
            @divExact(
                superblock.superblock_trailer_manifest_size_max,
                Manifest.BlockReferenceSize,
            ),
            @import("../lsm/tree.zig").table_count_max,
        ) catch unreachable;
        defer manifest.deinit(storage.allocator);

        manifest.decode(manifest_buffer);

        for (manifest.addresses[0..manifest.count]) |block_address, i| {
            const block_offset = vsr.Zone.grid.offset((block_address - 1) * constants.block_size);
            const block_header = mem.bytesAsValue(
                vsr.Header,
                storage.memory[block_offset..][0..@sizeOf(vsr.Header)],
            );
            assert(block_header.op == block_address);
            assert(block_header.checksum == manifest.checksums[i]);
            assert(block_header.operation == BlockType.manifest.operation());
        }
    }

    pub fn superblock_sector(
        storage: *const Storage,
        copy_: u8,
    ) *const superblock.SuperBlockSector {
        const offset = vsr.Zone.superblock.offset(superblock.Layout.offset_sector(copy_));
        const bytes = storage.memory[offset..][0..@sizeOf(superblock.SuperBlockSector)];
        return mem.bytesAsValue(superblock.SuperBlockSector, bytes);
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
    ) *align(constants.sector_size) const [constants.block_size]u8 {
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

const SuperBlockArea = struct {
    const Group = enum { sector, manifest, free_set, client_table };

    group: Group,
    copy: u8,

    fn to_offset(self: SuperBlockArea) u64 {
        return switch (self.group) {
            .sector => superblock.Layout.offset_sector(self.copy),
            .manifest => superblock.Layout.offset_manifest(self.copy),
            .free_set => superblock.Layout.offset_free_set(self.copy),
            .client_table => superblock.Layout.offset_client_table(self.copy),
        };
    }

    fn from_offset(offset: u64) SuperBlockArea {
        var copy: u8 = 0;
        while (copy < constants.superblock_copies) : (copy += 1) {
            for (std.enums.values(Group)) |group| {
                const area = SuperBlockArea{ .group = group, .copy = copy };
                if (area.to_offset() == offset) return area;
            }
        } else unreachable;
    }
};

test "SuperBlockArea" {
    var prng = std.rand.DefaultPrng.init(@intCast(u64, std.time.timestamp()));
    for (std.enums.values(SuperBlockArea.Group)) |group| {
        const area_expect = SuperBlockArea{
            .group = group,
            .copy = prng.random().uintLessThan(u8, constants.superblock_copies),
        };
        const area_actual = SuperBlockArea.from_offset(area_expect.to_offset());

        try std.testing.expectEqual(area_expect, area_actual);
    }
}
