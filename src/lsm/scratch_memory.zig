//! ScratchMemory is a page-aligned scratch buffer meant for situations where a buffer is required
//! (e.g., radix sort) and can be shared between components.
//! The buffer is page-aligned so that smaller alignments are trivially satisfied. See:
//! https://ziglang.org/documentation/0.14.1/#Alignment
//!
const std = @import("std");
const stdx = @import("stdx");
const assert = std.debug.assert;

buffer: []align(std.heap.page_size_min) u8,
state: enum { free, busy },

pub const ScratchMemory = @This();

pub fn init(gpa: std.mem.Allocator, size: usize) !ScratchMemory {
    assert(size > 0);

    var scratch: ScratchMemory = .{
        .buffer = undefined,
        .state = .free,
    };

    scratch.buffer = try gpa.alignedAlloc(u8, std.heap.page_size_min, size);
    errdefer gpa.free(scratch.buffer);

    return scratch;
}

pub fn deinit(scratch: *ScratchMemory, gpa: std.mem.Allocator) void {
    assert(scratch.state == .free);
    gpa.free(scratch.buffer);
    scratch.* = undefined;
}

pub fn acquire(scratch: *ScratchMemory, T: type, count: usize) []T {
    // A pointer with a larger alignment can be cast into one with a smaller alignment.
    comptime assert(@alignOf(T) < std.heap.page_size_min);
    assert(scratch.state == .free);
    assert(count * @sizeOf(T) <= scratch.buffer.len);
    defer assert(scratch.state == .busy);

    scratch.state = .busy;
    const scratch_size = count * @sizeOf(T);
    const scratch_typed = stdx.bytes_as_slice(
        .exact,
        T,
        scratch.buffer[0..scratch_size],
    );
    assert(std.mem.isAligned(@intFromPtr(scratch_typed.ptr), @alignOf(T)));
    return scratch_typed;
}

pub fn release(scratch: *ScratchMemory, T: type, slice: []T) void {
    comptime assert(@alignOf(T) < std.heap.page_size_min);
    assert(scratch.state == .busy);
    assert(@intFromPtr(slice.ptr) == @intFromPtr(scratch.buffer.ptr));
    assert(slice.len * @sizeOf(T) <= scratch.buffer.len);
    assert(std.mem.isAligned(@intFromPtr(slice.ptr), @alignOf(T)));
    defer assert(scratch.state == .free);

    scratch.state = .free;
}

test "ScratchMemory basic" {
    const testing = std.testing;

    const gpa = testing.allocator;
    const size = @sizeOf(u64) * 10;

    var scratch: ScratchMemory = try .init(
        gpa,
        size,
    );
    defer scratch.deinit(gpa);

    const slice = scratch.acquire(u64, 10);

    for (0..10) |n| {
        const ptr = &slice[n];
        try testing.expect(std.mem.isAligned(@intFromPtr(ptr), @alignOf(u64)));
        ptr.* = n;
    }
    scratch.release(u64, slice);
}
