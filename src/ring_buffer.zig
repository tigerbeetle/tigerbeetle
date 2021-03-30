const std = @import("std");

/// A First In/First Out ring buffer holding at most `size` elements.
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

        /// Return but do not remove the next item, if any.
        pub fn peek(self: *Self) ?T {
            if (self.empty()) return null;
            return self.buffer[self.index];
        }

        /// Remove and return the next item, if any.
        pub fn pop(self: *Self) ?T {
            if (self.empty()) return null;
            const ret = self.buffer[self.index];
            self.index = (self.index + 1) % self.buffer.len;
            self.count -= 1;
            return ret;
        }

        pub fn full(self: Self) bool {
            return self.count == self.buffer.len;
        }

        pub fn empty(self: Self) bool {
            return self.count == 0;
        }
    };
}

test "push/peek/pop/full/empty" {
    const testing = std.testing;

    var fifo = RingBuffer(u32, 3){};

    testing.expect(!fifo.full());
    testing.expect(fifo.empty());

    try fifo.push(1);
    testing.expectEqual(@as(?u32, 1), fifo.peek());

    testing.expect(!fifo.full());
    testing.expect(!fifo.empty());

    try fifo.push(2);
    testing.expectEqual(@as(?u32, 1), fifo.peek());

    try fifo.push(3);
    testing.expectError(error.NoSpaceLeft, fifo.push(4));

    testing.expect(fifo.full());
    testing.expect(!fifo.empty());

    testing.expectEqual(@as(?u32, 1), fifo.peek());
    testing.expectEqual(@as(?u32, 1), fifo.pop());

    testing.expect(!fifo.full());
    testing.expect(!fifo.empty());

    testing.expectEqual(@as(?u32, 2), fifo.pop());
    testing.expectEqual(@as(?u32, 3), fifo.pop());
    testing.expectEqual(@as(?u32, null), fifo.pop());

    testing.expect(!fifo.full());
    testing.expect(fifo.empty());
}
