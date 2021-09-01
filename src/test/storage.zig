const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const config = @import("../config.zig");

const log = std.log.scoped(.storage);

// TODOs:
// always random scheduling
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

        fn less_than(self: *Read, other: *Read) math.Order {
            return math.order(self.done_at_tick, other.done_at_tick);
        }
    };

    pub const Write = struct {
        callback: fn (write: *Storage.Write) void,
        buffer: []const u8,
        offset: u64,
        /// Tick at which this write is considered "completed" and the callback should be called.
        done_at_tick: u64,

        fn less_than(self: *Write, other: *Write) math.Order {
            return math.order(self.done_at_tick, other.done_at_tick);
        }
    };

    memory: []align(config.sector_size) u8,
    size: u64,

    reads: std.PriorityQueue(*Storage.Read),
    writes: std.PriorityQueue(*Storage.Write),

    ticks: u64 = 0,

    pub fn init(allocator: *mem.Allocator, size: u64) !Storage {
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
            .reads = reads,
            .writes = writes,
        };
    }

    pub fn deinit(self: *Storage, allocator: *mem.Allocator) void {
        allocator.free(self.memory);
        self.reads.deinit();
        self.writes.deinit();
    }

    pub fn tick(self: *Storage) void {
        self.ticks += 1;

        while (self.reads.peek()) |read| {
            if (read.done_at_tick > self.ticks) break;
            _ = self.reads.remove();

            mem.copy(u8, read.buffer, self.memory[read.offset..][0..read.buffer.len]);
            read.callback(read);
        }

        while (self.writes.peek()) |write| {
            if (write.done_at_tick > self.ticks) break;
            _ = self.writes.remove();

            mem.copy(u8, self.memory[write.offset..][0..write.buffer.len], write.buffer);
            write.callback(write);
        }
    }

    pub fn read_sectors(
        self: *Storage,
        callback: fn (read: *Storage.Read) void,
        read: *Storage.Read,
        buffer: []u8,
        offset: u64,
    ) void {
        self.assert_bounds_and_alignment(buffer, offset);

        read.* = .{
            .callback = callback,
            .buffer = buffer,
            .offset = offset,
            // TODO: make this random with significant outliers
            .done_at_tick = self.ticks + 10,
        };

        // We ensure the capacity is sufficient for config.io_depth_read in init()
        self.reads.add(read) catch unreachable;
    }

    pub fn write_sectors(
        self: *Storage,
        callback: fn (write: *Storage.Write) void,
        write: *Storage.Write,
        buffer: []const u8,
        offset: u64,
    ) void {
        self.assert_bounds_and_alignment(buffer, offset);

        write.* = .{
            .callback = callback,
            .buffer = buffer,
            .offset = offset,
            // TODO: make this random with significant outliers
            .done_at_tick = self.ticks + 10,
        };

        // We ensure the capacity is sufficient for config.io_depth_write in init()
        self.writes.add(write) catch unreachable;
    }

    fn assert_bounds_and_alignment(self: *Storage, buffer: []const u8, offset: u64) void {
        assert(buffer.len > 0);
        assert(offset + buffer.len <= self.size);

        // Ensure that the read or write is aligned correctly for Direct I/O:
        // If this is not the case, the underlying syscall will return EINVAL.
        assert(@mod(@ptrToInt(buffer.ptr), config.sector_size) == 0);
        assert(@mod(buffer.len, config.sector_size) == 0);
        assert(@mod(offset, config.sector_size) == 0);
    }
};
