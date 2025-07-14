const std = @import("std");
const mem = std.mem;
const Allocator = std.mem.Allocator;

const constants = @import("../constants.zig");

const allocate_block = @import("../vsr/grid.zig").allocate_block;
const BlockPtr = @import("../vsr/grid.zig").BlockPtr;

pub const Error = error{
    ScansMaxExceeded,
};

/// Holds memory for performing scans on all lsm tree levels.
/// TODO: It may be removed once we have ref-counted grid blocks.
pub const ScanBuffer = struct {
    pub const LevelBuffer = struct {
        index_block: BlockPtr,
        value_block: BlockPtr,

        pub fn init(self: *LevelBuffer, allocator: Allocator) !void {
            self.* = .{
                .index_block = undefined,
                .value_block = undefined,
            };

            self.index_block = try allocate_block(allocator);
            errdefer allocator.free(self.index_block);

            self.value_block = try allocate_block(allocator);
            errdefer allocator.free(self.value_block);
        }

        pub fn deinit(self: *LevelBuffer, allocator: Allocator) void {
            allocator.free(self.index_block);
            allocator.free(self.value_block);
        }
    };

    index: u8,
    levels: [constants.lsm_levels]LevelBuffer,

    pub fn init(
        self: *ScanBuffer,
        allocator: Allocator,
        options: struct {
            index: u8,
        },
    ) !void {
        self.* = .{
            .index = options.index,
            .levels = undefined,
        };
        for (&self.levels, 0..) |*level, i| {
            errdefer for (self.levels[0..i]) |*level_| level_.deinit(allocator);
            try level.init(allocator);
        }
        errdefer for (&self.levels) |*level| level.deinit(allocator);
    }

    pub fn deinit(self: *ScanBuffer, allocator: Allocator) void {
        for (&self.levels) |*level| {
            level.deinit(allocator);
        }
    }
};

/// ScanBufferPool holds enough memory to perform up to a max number of
/// scans operations in parallel.
/// This buffer is shared across different trees.
/// TODO: It may be removed once we have ref-counted grid blocks.
pub const ScanBufferPool = struct {
    scan_buffers: [constants.lsm_scans_max]ScanBuffer,
    scan_buffer_used: u8,

    pub fn init(self: *ScanBufferPool, allocator: Allocator) !void {
        self.* = .{
            .scan_buffers = undefined,
            .scan_buffer_used = 0,
        };

        for (&self.scan_buffers, 0..) |*scan_buffer, index| {
            errdefer for (self.scan_buffers[0..index]) |*buffer| buffer.deinit(allocator);
            try scan_buffer.init(
                allocator,
                .{ .index = @intCast(index) },
            );
        }
        errdefer for (&self.scan_buffers) |*buffer| buffer.deinit(allocator);
    }

    pub fn deinit(self: *ScanBufferPool, allocator: Allocator) void {
        for (&self.scan_buffers) |*scan_buffer| {
            scan_buffer.deinit(allocator);
        }
    }

    pub fn reset(self: *ScanBufferPool) void {
        self.* = .{
            .scan_buffers = self.scan_buffers,
            .scan_buffer_used = 0,
        };
    }

    pub fn acquire(self: *ScanBufferPool) Error!*const ScanBuffer {
        if (self.scan_buffer_used == constants.lsm_scans_max) return Error.ScansMaxExceeded;

        defer self.scan_buffer_used += 1;
        return &self.scan_buffers[self.scan_buffer_used];
    }

    pub fn acquire_assume_capacity(self: *ScanBufferPool) *const ScanBuffer {
        return self.acquire() catch unreachable;
    }
};
