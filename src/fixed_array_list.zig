const std = @import("std");
const mem = std.mem;

fn FixedArrayList(comptime T: type, comptime size: usize) type {
    return struct {
        const Self = @This();

        items: []T,

        fn init(allocator: *mem.Allocator) !Self {
            var ret = Self{ .items = &[0]T{} };
            ret.items.ptr = try allocator.create([size]T);
            return ret;
        }

        fn deinit(self: *Self, allocator: *std.mem.Allocator) void {
            // Must use "unsafe" slicing here to avoid safety checks
            allocator.free(self.items.ptr[0..size]);
        }

        fn append(self: *Self, item: T) error{NoSpaceLeft}!void {
            if (self.items.len == size) return error.NoSpaceLeft;
            self.items.len += 1;
            self.items[self.items.len - 1] = item;
        }

        fn clear(self: *Self) void {
            self.items.len = 0;
        }
    };
}

test "init/deinit/appendSlice/clear" {
    const testing = std.testing;

    var list = try FixedArrayList(u32, 5).init(testing.allocator);
    defer list.deinit(testing.allocator);

    try list.append(1);
    try list.append(2);
    try list.append(3);

    testing.expectEqualSlices(u32, &[_]u32{ 1, 2, 3 }, list.items);

    try list.append(4);
    try list.append(5);

    testing.expectError(error.NoSpaceLeft, list.append(6));

    list.clear();

    testing.expect(list.items.len == 0);
}
