const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;

const config = @import("../config.zig");

const log = std.log.scoped(.storage);

pub const Storage = struct {
    /// See usage in Journal.write_sectors() for details.
    /// TODO: allow testing in always_asynchronous mode.
    pub const synchronicity: enum {
        always_synchronous,
        always_asynchronous,
    } = .always_synchronous;

    pub const Read = struct {
        // This is a workaround for a stage1 compiler bug with
        // @fieldParentPtr() and 0-sized fields.
        _: u8,
    };

    pub const Write = struct {
        // This is a workaround for a stage1 compiler bug with
        // @fieldParentPtr() and 0-sized fields.
        _: u8,
    };

    allocator: *Allocator,
    memory: []align(config.sector_size) u8,
    size: u64,

    pub fn init(allocator: *Allocator, size: u64) !Storage {
        var memory = try allocator.allocAdvanced(u8, config.sector_size, size, .exact);
        errdefer allocator.free(memory);
        std.mem.set(u8, memory, 0);

        return Storage{
            .allocator = allocator,
            .memory = memory,
            .size = size,
        };
    }

    pub fn deinit(self: *Storage) void {
        self.allocator.free(self.memory);
    }

    pub fn read_sectors(
        self: *Storage,
        callback: fn (read: *Storage.Read) void,
        read: *Storage.Read,
        buffer: []u8,
        offset: u64,
    ) void {
        self.assert_bounds_and_alignment(buffer, offset);
        std.mem.copy(u8, buffer, self.memory[offset..][0..buffer.len]);
        callback(read);
    }

    pub fn write_sectors(
        self: *Storage,
        callback: fn (write: *Storage.Write) void,
        write: *Storage.Write,
        buffer: []const u8,
        offset: u64,
    ) void {
        self.assert_bounds_and_alignment(buffer, offset);
        std.mem.copy(u8, self.memory[offset..][0..buffer.len], buffer);
        callback(write);
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
