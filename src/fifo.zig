const std = @import("std");
const assert = std.debug.assert;

/// An intrusive first in/first out linked list.
/// The element type T must have a field called "next" of type ?*T
pub fn FIFO(comptime T: type) type {
    return struct {
        const Self = @This();

        in: ?*T = null,
        out: ?*T = null,

        pub fn push(self: *Self, elem: *T) void {
            assert(elem.next == null);
            if (self.in) |in| {
                in.next = elem;
                self.in = elem;
            } else {
                assert(self.out == null);
                self.in = elem;
                self.out = elem;
            }
        }

        pub fn pop(self: *Self) ?*T {
            const ret = self.out orelse return null;
            self.out = ret.next;
            ret.next = null;
            return ret;
        }

        pub fn peek(self: Self) ?*T {
            return self.out;
        }
    };
}
