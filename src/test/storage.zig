const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const config = @import("../config.zig");

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
        seed: u64,

        read_latency_min: u64,
        read_latency_mean: u64,

        write_latency_min: u64,
        write_latency_mean: u64,
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

    memory: []align(config.sector_size) u8,
    size: u64,

    options: Options,
    prng: std.rand.DefaultPrng,
    reads: std.PriorityQueue(*Storage.Read),
    writes: std.PriorityQueue(*Storage.Write),

    ticks: u64 = 0,

    pub fn init(allocator: *mem.Allocator, size: u64, options: Storage.Options) !Storage {
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
            .prng = std.rand.DefaultPrng.init(options.seed),
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

            mem.copy(u8, read.buffer, storage.memory[read.offset..][0..read.buffer.len]);
            read.callback(read);
        }

        while (storage.writes.peek()) |write| {
            if (write.done_at_tick > storage.ticks) break;
            _ = storage.writes.remove();

            mem.copy(u8, storage.memory[write.offset..][0..write.buffer.len], write.buffer);
            write.callback(write);
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
};
