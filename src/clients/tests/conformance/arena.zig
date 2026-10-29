//! A small arena allocator backed by a `FixedBufferAllocator`.
//!
//!
//! Allows for marking and restoring, so emitters never need more than parsed
//! suite + current case memory.

const std = @import("std");
const stdx = @import("stdx");
const assert = std.debug.assert;
const maybe = stdx.maybe;

pub const Arena = struct {
    pub const default_memory_size = 1 * stdx.MiB;

    fba: std.heap.FixedBufferAllocator,
    marked_index: usize = 0,

    pub fn init(memory: []u8) Arena {
        return .{ .fba = std.heap.FixedBufferAllocator.init(memory) };
    }

    pub fn allocator(arena: *Arena) std.mem.Allocator {
        return arena.fba.allocator();
    }

    pub fn mark(arena: *Arena) void {
        assert(arena.marked_index == 0);
        arena.marked_index = arena.fba.end_index;
    }

    pub fn restore(arena: *Arena) void {
        maybe(arena.marked_index == 0);
        defer arena.marked_index = 0;
        arena.fba.end_index = arena.marked_index;
    }

    pub fn reset(arena: *Arena) void {
        arena.fba.reset();
    }
};
