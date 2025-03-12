const std = @import("std");
const Alignment = std.mem.Alignment;

const CountingAllocator = @This();

parent_allocator: std.mem.Allocator,
size: usize = 0,

pub fn init(parent_allocator: std.mem.Allocator) CountingAllocator {
    return .{ .parent_allocator = parent_allocator };
}

pub fn deinit(self: *CountingAllocator) void {
    self.* = undefined;
}

pub fn allocator(self: *CountingAllocator) std.mem.Allocator {
    return .{
        .ptr = self,
        .vtable = &.{
            .alloc = alloc,
            .resize = resize,
            .remap = remap,
            .free = free,
        },
    };
}

fn alloc(ctx: *anyopaque, len: usize, ptr_align: Alignment, ret_addr: usize) ?[*]u8 {
    const self: *CountingAllocator = @alignCast(@ptrCast(ctx));
    self.size += len;
    return self.parent_allocator.rawAlloc(len, ptr_align, ret_addr);
}

fn resize(ctx: *anyopaque, buf: []u8, buf_align: Alignment, new_len: usize, ret_addr: usize) bool {
    const self: *CountingAllocator = @alignCast(@ptrCast(ctx));
    self.size = (self.size - buf.len) + new_len;
    return self.parent_allocator.rawResize(buf, buf_align, new_len, ret_addr);
}

fn remap(ctx: *anyopaque, buf: []u8, buf_align: Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
    const self: *CountingAllocator = @alignCast(@ptrCast(ctx));
    if (self.parent_allocator.rawRemap(buf, buf_align, new_len, ret_addr)) |remapped| {
        self.size = (self.size - buf.len) + new_len;
        return remapped;
    }
    return null;
}

fn free(ctx: *anyopaque, buf: []u8, buf_align: Alignment, ret_addr: usize) void {
    const self: *CountingAllocator = @alignCast(@ptrCast(ctx));
    self.size -= buf.len;
    return self.parent_allocator.rawFree(buf, buf_align, ret_addr);
}
