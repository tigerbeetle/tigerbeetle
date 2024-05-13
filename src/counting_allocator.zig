const std = @import("std");
const assert = std.debug.assert;

const Self = @This();

parent_allocator: std.mem.Allocator,
size: usize = 0,

pub fn init(parent_allocator: std.mem.Allocator) Self {
    return .{ .parent_allocator = parent_allocator };
}

pub fn deinit(self: *Self) void {
    self.* = undefined;
}

pub fn allocator(self: *Self) std.mem.Allocator {
    return .{
        .ptr = self,
        .vtable = &.{
            .alloc = alloc,
            .resize = resize,
            .free = free,
        },
    };
}

fn alloc(ctx: *anyopaque, len: usize, ptr_align: u8, ret_addr: usize) ?[*]u8 {
    const self: *Self = @alignCast(@ptrCast(ctx));
    self.size += len;
    return self.parent_allocator.rawAlloc(len, ptr_align, ret_addr);
}

fn resize(ctx: *anyopaque, buf: []u8, buf_align: u8, new_len: usize, ret_addr: usize) bool {
    const self: *Self = @alignCast(@ptrCast(ctx));
    self.size = (self.size - buf.len) + new_len;
    return self.parent_allocator.rawResize(buf, buf_align, new_len, ret_addr);
}

fn free(ctx: *anyopaque, buf: []u8, buf_align: u8, ret_addr: usize) void {
    const self: *Self = @alignCast(@ptrCast(ctx));
    self.size -= buf.len;
    return self.parent_allocator.rawFree(buf, buf_align, ret_addr);
}
