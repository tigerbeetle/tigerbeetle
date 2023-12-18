const std = @import("std");
const assert = std.debug.assert;

const constants = @import("./constants.zig");
const tracer = @import("./tracer.zig");

/// An intrusive first in/first out linked list.
/// The element type T must have a field called "next" of type ?*T
pub fn FIFO(comptime T: type) type {
    return struct {
        const Self = @This();

        in: ?*T = null,
        out: ?*T = null,
        count: u64 = 0,
        // This should only be null if you're sure we'll never want to monitor `count`.
        name: ?[]const u8,

        pub fn push(self: *Self, elem: *T) void {
            if (constants.verify) assert(!self.contains(elem));

            assert(elem.next == null);
            if (self.in) |in| {
                in.next = elem;
                self.in = elem;
            } else {
                assert(self.out == null);
                self.in = elem;
                self.out = elem;
            }
            self.count += 1;
            self.plot();
        }

        pub fn pop(self: *Self) ?*T {
            const ret = self.out orelse return null;
            self.out = ret.next;
            ret.next = null;
            if (self.in == ret) self.in = null;
            self.count -= 1;
            self.plot();
            return ret;
        }

        pub fn peek_last(self: Self) ?*T {
            return self.in;
        }

        pub fn peek(self: Self) ?*T {
            return self.out;
        }

        pub fn empty(self: Self) bool {
            return self.peek() == null;
        }

        /// Returns whether the linked list contains the given *exact element* (pointer comparison).
        pub fn contains(self: *const Self, elem_needle: *const T) bool {
            var iterator = self.peek();
            while (iterator) |elem| : (iterator = elem.next) {
                if (elem == elem_needle) return true;
            }
            return false;
        }

        /// Remove an element from the FIFO. Asserts that the element is
        /// in the FIFO. This operation is O(N), if this is done often you
        /// probably want a different data structure.
        pub fn remove(self: *Self, to_remove: *T) void {
            if (to_remove == self.out) {
                _ = self.pop();
                return;
            }
            var it = self.out;
            while (it) |elem| : (it = elem.next) {
                if (to_remove == elem.next) {
                    if (to_remove == self.in) self.in = elem;
                    elem.next = to_remove.next;
                    to_remove.next = null;
                    self.count -= 1;
                    self.plot();
                    break;
                }
            } else unreachable;
        }

        pub fn reset(self: *Self) void {
            self.* = .{ .name = self.name };
        }

        fn plot(self: Self) void {
            if (self.name) |name| {
                tracer.plot(
                    .{ .queue_count = .{ .queue_name = name } },
                    @as(f64, @floatFromInt(self.count)),
                );
            }
        }
    };
}

test "FIFO: push/pop/peek/remove/empty" {
    const testing = @import("std").testing;

    const Foo = struct { next: ?*@This() = null };

    var one: Foo = .{};
    var two: Foo = .{};
    var three: Foo = .{};

    var fifo: FIFO(Foo) = .{ .name = null };
    try testing.expect(fifo.empty());

    fifo.push(&one);
    try testing.expect(!fifo.empty());
    try testing.expectEqual(@as(?*Foo, &one), fifo.peek());
    try testing.expect(fifo.contains(&one));
    try testing.expect(!fifo.contains(&two));
    try testing.expect(!fifo.contains(&three));

    fifo.push(&two);
    fifo.push(&three);
    try testing.expect(!fifo.empty());
    try testing.expectEqual(@as(?*Foo, &one), fifo.peek());
    try testing.expect(fifo.contains(&one));
    try testing.expect(fifo.contains(&two));
    try testing.expect(fifo.contains(&three));

    fifo.remove(&one);
    try testing.expect(!fifo.empty());
    try testing.expectEqual(@as(?*Foo, &two), fifo.pop());
    try testing.expectEqual(@as(?*Foo, &three), fifo.pop());
    try testing.expectEqual(@as(?*Foo, null), fifo.pop());
    try testing.expect(fifo.empty());
    try testing.expect(!fifo.contains(&one));
    try testing.expect(!fifo.contains(&two));
    try testing.expect(!fifo.contains(&three));

    fifo.push(&one);
    fifo.push(&two);
    fifo.push(&three);
    fifo.remove(&two);
    try testing.expect(!fifo.empty());
    try testing.expectEqual(@as(?*Foo, &one), fifo.pop());
    try testing.expectEqual(@as(?*Foo, &three), fifo.pop());
    try testing.expectEqual(@as(?*Foo, null), fifo.pop());
    try testing.expect(fifo.empty());

    fifo.push(&one);
    fifo.push(&two);
    fifo.push(&three);
    fifo.remove(&three);
    try testing.expect(!fifo.empty());
    try testing.expectEqual(@as(?*Foo, &one), fifo.pop());
    try testing.expect(!fifo.empty());
    try testing.expectEqual(@as(?*Foo, &two), fifo.pop());
    try testing.expect(fifo.empty());
    try testing.expectEqual(@as(?*Foo, null), fifo.pop());
    try testing.expect(fifo.empty());

    fifo.push(&one);
    fifo.push(&two);
    fifo.remove(&two);
    fifo.push(&three);
    try testing.expectEqual(@as(?*Foo, &one), fifo.pop());
    try testing.expectEqual(@as(?*Foo, &three), fifo.pop());
    try testing.expectEqual(@as(?*Foo, null), fifo.pop());
    try testing.expect(fifo.empty());
}
