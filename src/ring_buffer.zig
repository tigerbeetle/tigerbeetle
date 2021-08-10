const std = @import("std");
const assert = std.debug.assert;

/// A First In, First Out ring buffer holding at most `size` elements.
pub fn RingBuffer(comptime T: type, comptime size: usize) type {
    return struct {
        const Self = @This();

        buffer: [size]T = undefined,

        /// The index of the slot with the first item, if any.
        index: usize = 0,

        /// The number of items in the buffer.
        count: usize = 0,

        /// Add an element to the RingBuffer. Returns an error if the buffer
        /// is already full and the element could not be added.
        pub fn push(self: *Self, item: T) error{NoSpaceLeft}!void {
            if (self.full()) return error.NoSpaceLeft;
            self.buffer[(self.index + self.count) % self.buffer.len] = item;
            self.count += 1;
        }

        /// Return, but do not remove, the next item, if any.
        pub fn peek(self: *Self) ?T {
            return (self.peek_ptr() orelse return null).*;
        }

        /// Return a pointer to, but do not remove, the next item, if any.
        pub fn peek_ptr(self: *Self) ?*T {
            if (self.empty()) return null;
            return &self.buffer[self.index];
        }

        /// Remove and return the next item, if any.
        pub fn pop(self: *Self) ?T {
            if (self.empty()) return null;
            defer {
                self.index = (self.index + 1) % self.buffer.len;
                self.count -= 1;
            }
            return self.buffer[self.index];
        }

        /// Returns whether the ring buffer is completely full.
        pub fn full(self: *const Self) bool {
            return self.count == self.buffer.len;
        }

        /// Returns whether the ring buffer is completely empty.
        pub fn empty(self: *const Self) bool {
            return self.count == 0;
        }

        pub const Iterator = struct {
            ring: *Self,
            count: usize = 0,

            pub fn next(it: *Iterator) ?T {
                return (it.next_ptr() orelse return null).*;
            }

            pub fn next_ptr(it: *Iterator) ?*T {
                assert(it.count <= it.ring.count);
                if (it.count == it.ring.count) return null;
                defer it.count += 1;
                return &it.ring.buffer[(it.ring.index + it.count) % it.ring.buffer.len];
            }
        };

        /// Returns an iterator to iterate through all `count` items in the ring buffer.
        /// The iterator is invalidated and unsafe if the ring buffer is modified.
        pub fn iterator(self: *Self) Iterator {
            return .{ .ring = self };
        }
    };
}

const testing = std.testing;

test "push/peek/pop/full/empty" {
    var fifo = RingBuffer(u32, 3){};

    try testing.expect(!fifo.full());
    try testing.expect(fifo.empty());

    try fifo.push(1);
    try testing.expectEqual(@as(?u32, 1), fifo.peek());

    try testing.expect(!fifo.full());
    try testing.expect(!fifo.empty());

    try fifo.push(2);
    try testing.expectEqual(@as(?u32, 1), fifo.peek());

    try fifo.push(3);
    try testing.expectError(error.NoSpaceLeft, fifo.push(4));

    try testing.expect(fifo.full());
    try testing.expect(!fifo.empty());

    try testing.expectEqual(@as(?u32, 1), fifo.peek());
    try testing.expectEqual(@as(?u32, 1), fifo.pop());

    try testing.expect(!fifo.full());
    try testing.expect(!fifo.empty());

    fifo.peek_ptr().?.* += 1000;

    try testing.expectEqual(@as(?u32, 1002), fifo.pop());
    try testing.expectEqual(@as(?u32, 3), fifo.pop());
    try testing.expectEqual(@as(?u32, null), fifo.pop());

    try testing.expect(!fifo.full());
    try testing.expect(fifo.empty());
}

fn test_iterator(comptime T: type, ring: *T, values: []const u32) !void {
    const ring_index = ring.index;

    var loops: usize = 0;
    while (loops < 2) : (loops += 1) {
        var iterator = ring.iterator();
        var index: usize = 0;
        while (iterator.next()) |item| {
            try testing.expectEqual(values[index], item);
            index += 1;
        }
        try testing.expectEqual(values.len, index);
    }

    try testing.expectEqual(ring_index, ring.index);
}

test "iterator" {
    const Ring = RingBuffer(u32, 2);

    var ring = Ring{};
    try test_iterator(Ring, &ring, &[_]u32{});

    try ring.push(0);
    try test_iterator(Ring, &ring, &[_]u32{0});

    try ring.push(1);
    try test_iterator(Ring, &ring, &[_]u32{ 0, 1 });

    try testing.expectEqual(@as(?u32, 0), ring.pop());
    try test_iterator(Ring, &ring, &[_]u32{1});

    try ring.push(2);
    try test_iterator(Ring, &ring, &[_]u32{ 1, 2 });

    var iterator = ring.iterator();
    while (iterator.next_ptr()) |item_ptr| {
        item_ptr.* += 1000;
    }

    try testing.expectEqual(@as(?u32, 1001), ring.pop());
    try test_iterator(Ring, &ring, &[_]u32{1002});

    try ring.push(3);
    try test_iterator(Ring, &ring, &[_]u32{ 1002, 3 });

    try testing.expectEqual(@as(?u32, 1002), ring.pop());
    try test_iterator(Ring, &ring, &[_]u32{3});

    try testing.expectEqual(@as(?u32, 3), ring.pop());
    try test_iterator(Ring, &ring, &[_]u32{});
}
