const constants = @import("../constants.zig");
const BlockPtr = @import("../vsr/grid.zig").BlockPtr;

pub const Error = error{
    ScansMaxExceeded,
};

/// Holds memory for performing scans on all lsm tree levels.
/// TODO: It may be removed once we have ref-counted grid blocks.
pub fn ScanBufferType(comptime Grid: type) type {
    return struct {
        pub const LevelBuffer = struct {
            index_block: BlockPtr,
            value_block: BlockPtr,

            pub fn init(self: *LevelBuffer, grid: *Grid) void {
                self.* = .{
                    .index_block = grid.get_block(),
                    .value_block = grid.get_block(),
                };
            }

            pub fn deinit(self: *LevelBuffer, grid: *Grid) void {
                grid.block_unref(self.index_block);
                grid.block_unref(self.value_block);
            }
        };

        index: u8,
        levels: [constants.lsm_levels]LevelBuffer,

        const ScanBuffer = @This();

        pub fn init(
            self: *ScanBuffer,
            grid: *Grid,
            options: struct {
                index: u8,
            },
        ) void {
            self.* = .{
                .index = options.index,
                .levels = undefined,
            };
            for (&self.levels) |*level| level.init(grid);
        }

        pub fn deinit(self: *ScanBuffer, grid: *Grid) void {
            for (&self.levels) |*level| {
                level.deinit(grid);
            }
        }
    };
}

/// ScanBufferPool holds enough memory to perform up to a max number of
/// scans operations in parallel.
/// This buffer is shared across different trees.
/// TODO: It may be removed once we have ref-counted grid blocks.
pub fn ScanBufferPoolType(comptime Grid: type) type {
    return struct {
        scan_buffers: [constants.lsm_scans_max]ScanBuffer,
        scan_buffer_used: u8,

        const ScanBuffer = ScanBufferType(Grid);
        const ScanBufferPool = @This();

        pub fn init(self: *ScanBufferPool, grid: *Grid) void {
            self.* = .{
                .scan_buffers = undefined,
                .scan_buffer_used = 0,
            };

            for (&self.scan_buffers, 0..) |*scan_buffer, index| {
                scan_buffer.init(grid, .{ .index = @intCast(index) });
            }
        }

        pub fn deinit(self: *ScanBufferPool, grid: *Grid) void {
            for (&self.scan_buffers) |*scan_buffer| {
                scan_buffer.deinit(grid);
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
}
