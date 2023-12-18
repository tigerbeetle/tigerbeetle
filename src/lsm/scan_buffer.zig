const std = @import("std");
const mem = std.mem;
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

const constants = @import("../constants.zig");
const lsm = @import("tree.zig");

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
        data_block: BlockPtr,

        pub fn init(allocator: Allocator) !LevelBuffer {
            const index_block = try allocate_block(allocator);
            errdefer allocator.free(index_block);

            const data_block = try allocate_block(allocator);
            errdefer allocator.free(data_block);

            return LevelBuffer{
                .index_block = index_block,
                .data_block = data_block,
            };
        }

        pub fn deinit(self: *LevelBuffer, allocator: Allocator) void {
            allocator.free(self.index_block);
            allocator.free(self.data_block);
        }
    };

    levels: [constants.lsm_levels]LevelBuffer,

    pub fn init(allocator: Allocator) !ScanBuffer {
        var self: ScanBuffer = undefined;
        for (&self.levels, 0..) |*level, i| {
            errdefer for (self.levels[0..i]) |*level_| level_.deinit(allocator);
            level.* = try LevelBuffer.init(allocator);
        }
        errdefer for (&self.levels) |*level| level.deinit(allocator);

        return self;
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

    pub fn init(allocator: Allocator) !ScanBufferPool {
        var scan_buffers: [constants.lsm_scans_max]ScanBuffer = undefined;
        for (&scan_buffers, 0..) |*scan_buffer, i| {
            errdefer for (scan_buffers[0..i]) |*buffer| buffer.deinit(allocator);
            scan_buffer.* = try ScanBuffer.init(allocator);
        }
        errdefer for (&scan_buffers) |*buffer| buffer.deinit(allocator);

        return ScanBufferPool{
            .scan_buffers = scan_buffers,
            .scan_buffer_used = 0,
        };
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
