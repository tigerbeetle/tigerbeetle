const std = @import("std");
const assert = std.debug.assert;

const constants = @import("./constants.zig");

/// An intrusive last in/first out (LIFO, stack) linked list.
/// The element type T must have a field called "next" of type ?*T.
pub fn LIFOType(comptime T: type) type {
    return struct {
        const LIFO = @This();

        head: ?*T = null,
        count: u64 = 0,

        // This should only be null if you're sure we'll never want to monitor `count`.
        name: ?[]const u8,

        // If the number of elements is large, the constants.verify check in push() can be too
        // expensive. Allow the user to gate it. Could also be a comptime param?
        verify_push: bool = true,

        pub fn push(self: *LIFO, elem: *T) void {
            if (constants.verify and self.verify_push) assert(!self.contains(elem));
            assert(elem.next == null);
            // Insert the new element at the head.
            elem.next = self.head;
            self.head = elem;
            self.count += 1;
        }

        pub fn pop(self: *LIFO) ?*T {
            const ret = self.head orelse return null;
            self.head = ret.next;
            ret.next = null;
            self.count -= 1;
            return ret;
        }

        pub fn peek(self: LIFO) ?*T {
            return self.head;
        }

        pub fn empty(self: LIFO) bool {
            return self.head == null;
        }

        /// Returns whether the linked list contains the given *exact element* (pointer comparison).
        pub fn contains(self: *const LIFO, elem_needle: *const T) bool {
            var iterator = self.head;
            while (iterator) |elem| : (iterator = elem.next) {
                if (elem == elem_needle) return true;
            }
            return false;
        }

        /// Remove an element from the stack. Asserts that the element is in the stack.
        /// This operation is O(N); if done frequently, consider a different data structure.
        pub fn remove(self: *LIFO, to_remove: *T) void {
            if (to_remove == self.head) {
                _ = self.pop();
                return;
            }
            var current = self.head;
            while (current) |elem| {
                if (elem.next == to_remove) {
                    elem.next = to_remove.next;
                    to_remove.next = null;
                    self.count -= 1;
                    return;
                }
                current = elem.next;
            }
            unreachable; // to_remove must be in the stack.
        }

        pub fn reset(self: *LIFO) void {
            self.* = .{ .name = self.name };
        }
    };
}

test "LIFO: push/pop/peek/remove/empty" {
    const testing = @import("std").testing;

    const Foo = struct { next: ?*@This() = null };

    var one: Foo = .{};
    var two: Foo = .{};
    var three: Foo = .{};

    var lifo: LIFOType(Foo) = .{ .name = null };
    try testing.expect(lifo.empty());

    lifo.push(&one);
    try testing.expect(!lifo.empty());
    try testing.expectEqual(@as(?*Foo, &one), lifo.peek());
    try testing.expect(lifo.contains(&one));
    try testing.expect(!lifo.contains(&two));
    try testing.expect(!lifo.contains(&three));

    lifo.push(&two);
    lifo.push(&three);
    try testing.expect(!lifo.empty());
    try testing.expectEqual(@as(?*Foo, &three), lifo.peek());
    try testing.expect(lifo.contains(&one));
    try testing.expect(lifo.contains(&two));
    try testing.expect(lifo.contains(&three));

    lifo.remove(&one);
    try testing.expect(!lifo.empty());
    try testing.expectEqual(@as(?*Foo, &three), lifo.pop());
    try testing.expectEqual(@as(?*Foo, &two), lifo.pop());
    try testing.expectEqual(@as(?*Foo, null), lifo.pop());
    try testing.expect(lifo.empty());
    try testing.expect(!lifo.contains(&one));
    try testing.expect(!lifo.contains(&two));
    try testing.expect(!lifo.contains(&three));

    lifo.push(&one);
    lifo.push(&two);
    lifo.push(&three);
    lifo.remove(&two);
    try testing.expect(!lifo.empty());
    try testing.expectEqual(@as(?*Foo, &three), lifo.pop());
    try testing.expectEqual(@as(?*Foo, &one), lifo.pop());
    try testing.expectEqual(@as(?*Foo, null), lifo.pop());
    try testing.expect(lifo.empty());

    lifo.push(&one);
    lifo.push(&two);
    lifo.push(&three);
    lifo.remove(&three);
    try testing.expect(!lifo.empty());
    try testing.expectEqual(@as(?*Foo, &two), lifo.pop());
    try testing.expect(!lifo.empty());
    try testing.expectEqual(@as(?*Foo, &one), lifo.pop());
    try testing.expect(lifo.empty());
    try testing.expectEqual(@as(?*Foo, null), lifo.pop());
    try testing.expect(lifo.empty());

    lifo.push(&one);
    lifo.push(&two);
    lifo.push(&three);
    try testing.expectEqual(@as(?*Foo, &three), lifo.pop());
    try testing.expectEqual(@as(?*Foo, &two), lifo.pop());
    try testing.expectEqual(@as(?*Foo, &one), lifo.pop());
    try testing.expectEqual(@as(?*Foo, null), lifo.pop());
    try testing.expect(lifo.empty());
}
