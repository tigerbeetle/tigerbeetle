//! An allocator wrapper which can be disabled at runtime.
//! We use this for allocating at startup and then
//! disable it to prevent accidental dynamic allocation at runtime.

const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const log = std.log.scoped(.static_allocator);

const Self = @This();
parent_allocator: mem.Allocator,
state: State,

const State = enum {
    /// Allow `alloc` and `resize`.
    /// (To make errdefer cleanup easier to write we also allow calling `free`,
    ///  in which case we switch state to `.deinit` and no longer allow `alloc` or `resize`.)
    init,
    /// Don't allow any calls.
    static,
    /// Allow `free` but not `alloc` and `resize`.
    deinit,
};

pub fn init(parent_allocator: mem.Allocator) Self {
    return .{
        .parent_allocator = parent_allocator,
        .state = .init,
    };
}

pub fn deinit(self: *Self) void {
    self.* = undefined;
}

pub fn transition_from_init_to_static(self: *Self) void {
    assert(self.state == .init);
    self.state = .static;
}

pub fn transition_from_static_to_deinit(self: *Self) void {
    assert(self.state == .static);
    self.state = .deinit;
}

pub fn allocator(self: *Self) mem.Allocator {
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
    assert(self.state == .init);
    return self.parent_allocator.rawAlloc(len, ptr_align, ret_addr);
}

fn resize(ctx: *anyopaque, buf: []u8, buf_align: u8, new_len: usize, ret_addr: usize) bool {
    const self: *Self = @alignCast(@ptrCast(ctx));
    assert(self.state == .init);
    return self.parent_allocator.rawResize(buf, buf_align, new_len, ret_addr);
}

fn free(ctx: *anyopaque, buf: []u8, buf_align: u8, ret_addr: usize) void {
    const self: *Self = @alignCast(@ptrCast(ctx));
    assert(self.state == .init or self.state == .deinit);
    // Once you start freeing, you don't stop.
    self.state = .deinit;
    return self.parent_allocator.rawFree(buf, buf_align, ret_addr);
}
