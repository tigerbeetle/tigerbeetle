const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const config = @import("../config.zig");
const vsr = @import("../vsr.zig");

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
        /// Seed for the storage PRNG
        seed: u64,

        /// Minimum number of ticks it may take to read data.
        read_latency_min: u64,
        /// Average number of ticks it may take to read data. Must be >= read_latency_min.
        read_latency_mean: u64,
        /// Minimum number of ticks it may take to write data.
        write_latency_min: u64,
        /// Average number of ticks it may take to write data. Must be >= write_latency_min.
        write_latency_mean: u64,

        /// Chance out of 100 that a read will return incorrect data, if the target memory is within
        /// the faulty area of this replica.
        read_fault_probability: u8,
        /// Chance out of 100 that a read will return incorrect data, if the target memory is within
        /// the faulty area of this replica.
        write_fault_probability: u8,
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
        offset: u64,
        /// Tick at which this read is considered "completed" and the callback should be called.
        done_at_tick: u64,

        fn less_than(storage: *Read, other: *Read) math.Order {
            return math.order(storage.done_at_tick, other.done_at_tick);
        }
    };

    pub const Write = struct {
        callback: fn (write: *Storage.Write) void,
        buffer: []const u8,
        offset: u64,
        /// Tick at which this write is considered "completed" and the callback should be called.
        done_at_tick: u64,

        fn less_than(storage: *Write, other: *Write) math.Order {
            return math.order(storage.done_at_tick, other.done_at_tick);
        }
    };

    /// Faulty areas are always sized to message_size_max
    /// If the faulty areas of all replicas are superimposed, the padding between them is always message_size_max.
    /// For a single replica, the padding between faulty areas depends on the number of other replicas.
    pub const FaultyAreas = struct {
        first_offset: u64,
        period: u64,
    };

    memory: []align(config.sector_size) u8,
    size: u64,

    options: Options,
    replica_index: u8,
    prng: std.rand.DefaultPrng,

    // We can't allow storage faults for the same message in a majority of
    // the replicas as that would make recovery impossible. Instead, we only
    // allow faults in certian areas which differ between replicas.
    faulty_areas: FaultyAreas,

    reads: std.PriorityQueue(*Storage.Read),
    writes: std.PriorityQueue(*Storage.Write),

    ticks: u64 = 0,

    pub fn init(
        allocator: *mem.Allocator,
        size: u64,
        options: Storage.Options,
        replica_index: u8,
        faulty_areas: FaultyAreas,
    ) !Storage {
        assert(options.write_latency_mean >= options.write_latency_min);
        assert(options.read_latency_mean >= options.read_latency_min);

        const memory = try allocator.allocAdvanced(u8, config.sector_size, size, .exact);
        errdefer allocator.free(memory);
        // TODO: random data
        mem.set(u8, memory, 0);

        var reads = std.PriorityQueue(*Storage.Read).init(allocator, Storage.Read.less_than);
        errdefer reads.deinit();
        try reads.ensureCapacity(config.io_depth_read);

        var writes = std.PriorityQueue(*Storage.Write).init(allocator, Storage.Write.less_than);
        errdefer writes.deinit();
        try writes.ensureCapacity(config.io_depth_write);

        return Storage{
            .memory = memory,
            .size = size,
            .options = options,
            .replica_index = replica_index,
            .prng = std.rand.DefaultPrng.init(options.seed),
            .faulty_areas = faulty_areas,
            .reads = reads,
            .writes = writes,
        };
    }

    pub fn deinit(storage: *Storage, allocator: *mem.Allocator) void {
        allocator.free(storage.memory);
        storage.reads.deinit();
        storage.writes.deinit();
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

    pub fn read_sectors(
        storage: *Storage,
        callback: fn (read: *Storage.Read) void,
        read: *Storage.Read,
        buffer: []u8,
        offset: u64,
    ) void {
        storage.assert_bounds_and_alignment(buffer, offset);

        read.* = .{
            .callback = callback,
            .buffer = buffer,
            .offset = offset,
            .done_at_tick = storage.ticks + storage.read_latency(),
        };

        // We ensure the capacity is sufficient for config.io_depth_read in init()
        storage.reads.add(read) catch unreachable;
    }

    fn read_sectors_finish(storage: *Storage, read: *Storage.Read) void {
        const faulty = storage.faulty_sectors(read.offset, read.buffer.len);
        if (faulty.len > 0 and storage.x_in_100(storage.options.write_fault_probability)) {
            // Randomly corrupt one of the faulty sectors the read targeted
            // TODO: inject more realistic and varied storage faults as described above.
            const sector_count = @divExact(faulty.len, config.sector_size);
            const faulty_sector = storage.prng.random.uintLessThan(u64, sector_count);
            const faulty_sector_offset = faulty_sector * config.sector_size;
            const faulty_sector_bytes = faulty[faulty_sector_offset..][0..config.sector_size];

            log.info("corrupting sector at offset {} during read by replica {}", .{
                faulty_sector_offset,
                storage.replica_index,
            });

            storage.prng.random.bytes(faulty_sector_bytes);
        }

        mem.copy(u8, read.buffer, storage.memory[read.offset..][0..read.buffer.len]);
        read.callback(read);
    }

    pub fn write_sectors(
        storage: *Storage,
        callback: fn (write: *Storage.Write) void,
        write: *Storage.Write,
        buffer: []const u8,
        offset: u64,
    ) void {
        storage.assert_bounds_and_alignment(buffer, offset);

        write.* = .{
            .callback = callback,
            .buffer = buffer,
            .offset = offset,
            .done_at_tick = storage.ticks + storage.write_latency(),
        };

        // We ensure the capacity is sufficient for config.io_depth_write in init()
        storage.writes.add(write) catch unreachable;
    }

    fn write_sectors_finish(storage: *Storage, write: *Storage.Write) void {
        mem.copy(u8, storage.memory[write.offset..][0..write.buffer.len], write.buffer);

        const faulty = storage.faulty_sectors(write.offset, write.buffer.len);
        if (faulty.len > 0 and storage.x_in_100(storage.options.write_fault_probability)) {
            // Randomly corrupt one of the faulty sectors the write targeted
            // TODO: inject more realistic and varied storage faults as described above.
            const sector_count = @divExact(faulty.len, config.sector_size);
            const faulty_sector = storage.prng.random.uintLessThan(u64, sector_count);
            const faulty_sector_offset = faulty_sector * config.sector_size;
            const faulty_sector_bytes = faulty[faulty_sector_offset..][0..config.sector_size];

            log.info("corrupting sector at offset {} during write by replica {}", .{
                faulty_sector_offset,
                storage.replica_index,
            });

            storage.prng.random.bytes(faulty_sector_bytes);
        }

        write.callback(write);
    }

    fn assert_bounds_and_alignment(storage: *Storage, buffer: []const u8, offset: u64) void {
        assert(buffer.len > 0);
        assert(offset + buffer.len <= storage.size);

        // Ensure that the read or write is aligned correctly for Direct I/O:
        // If this is not the case, the underlying syscall will return EINVAL.
        assert(@mod(@ptrToInt(buffer.ptr), config.sector_size) == 0);
        assert(@mod(buffer.len, config.sector_size) == 0);
        assert(@mod(offset, config.sector_size) == 0);
    }

    fn read_latency(storage: *Storage) u64 {
        return storage.latency(storage.options.read_latency_min, storage.options.read_latency_mean);
    }

    fn write_latency(storage: *Storage) u64 {
        return storage.latency(storage.options.write_latency_min, storage.options.write_latency_mean);
    }

    fn latency(storage: *Storage, min: u64, mean: u64) u64 {
        return min + @floatToInt(u64, @intToFloat(f64, mean - min) * storage.prng.random.floatExp(f64));
    }

    /// Return true with probability x/100.
    fn x_in_100(storage: *Storage, x: u8) bool {
        assert(x <= 100);
        return x > storage.prng.random.uintLessThan(u8, 100);
    }

    /// The return value is a slice into the provided out array.
    pub fn generate_faulty_areas(
        prng: *std.rand.Random,
        size: u64,
        replica_count: u8,
        out: *[config.replicas_max]FaultyAreas,
    ) []FaultyAreas {
        comptime assert(config.message_size_max % config.sector_size == 0);
        const message_size_max = config.message_size_max;

        // We need to ensure there is message_size_max fault-free padding
        // between faulty areas of memory so that a single message
        // cannot straddle the corruptable areas of a majority of replicas.
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
            else => unreachable,
        }

        prng.shuffle(FaultyAreas, out[0..replica_count]);
        return out[0..replica_count];
    }

    /// Given an offest and size of a read/write, returns a slice into storage.memory of any
    /// faulty sectors touched by the read/write
    fn faulty_sectors(storage: *Storage, offset: u64, size: u64) []align(config.sector_size) u8 {
        assert(size <= config.message_size_max);
        const message_size_max = config.message_size_max;
        const period = storage.faulty_areas.period;

        const faulty_offset = storage.faulty_areas.first_offset + (offset / period) * period;

        const start = std.math.max(offset, faulty_offset);
        const end = std.math.min(offset + size, faulty_offset + message_size_max);

        // The read/write does not touch any faulty sectors
        if (start >= end) return &[0]u8{};

        return storage.memory[start..end];
    }
};
