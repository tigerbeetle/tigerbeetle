//! The "core" allocator responsible for getting memory from the OS,
//! Defaults to std.heap.page_allocator
const std = @import("std");
const builtin = @import("builtin");
const stdx = @import("stdx");

const assert = std.debug.assert;
const os = std.os;
const Allocator = std.mem.Allocator;
const PageAllocator = std.heap.PageAllocator;

pub fn os_allocator() Allocator {
    return switch (builtin.target.os.tag) {
        .linux => MMapAllocator.init().allocator(),
        else => std.heap.page_allocator
    };
}

const MMapAllocator = struct {

    memory_start: *u8,
    
    fn init(size_initial: u64) MMapAllocator {
        assert(size_initial % std.heap.page_size_min == 0);
        const region = PageAllocator.map(size_initial, 16);
        MmapAllocator{
            .memory_start = region
        };
    }

    fn allocator(mmap_allocator: *MMapAllocator) Allocator {
        const vtable: Allocator.VTable = .{
            .alloc = MMapAllocator.alloc,
            .resize = MMapAllocator.resize,
            .remap = MMapAllocator.remap,
            .free = MMapAllocator.free,
        };
        return Allocator{
            .ptr =  mmap_allocator,
            .vtable = vtable
        };
    }
};
