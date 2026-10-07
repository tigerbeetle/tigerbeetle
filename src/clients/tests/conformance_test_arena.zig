const std = @import("std");
const stdx = @import("stdx");
const assert = std.debug.assert;

pub const Arena = struct {
    pub const default_memory_size = 1 * stdx.MiB;

    fba: std.heap.FixedBufferAllocator,

    pub fn init(memory: []u8) Arena {
        return .{ .fba = std.heap.FixedBufferAllocator.init(memory) };
    }

    pub fn allocator(arena: *Arena) std.mem.Allocator {
        return arena.fba.allocator();
    }

    pub fn mark(arena: *const Arena) usize {
        return arena.fba.end_index;
    }

    pub fn release(arena: *Arena, marked: usize) void {
        assert(marked <= arena.fba.end_index);
        arena.fba.end_index = marked;
    }
};
