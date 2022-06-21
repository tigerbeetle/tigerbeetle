const std = @import("std");
const assert = std.debug.assert;

/// A First In, First Out ring buffer holding at most `count_max` elements.
pub fn RingBuffer(comptime T: type, comptime count_max: usize) type {
    return struct {
        const Self = @This();

        buffer: [count_max]T = undefined,

        /// The index of the slot with the first item, if any.
        index: usize = 0,

        /// The number of items in the buffer.
        count: usize = 0,

        // TODO add doc comments to these functions:
        pub inline fn head(self: Self) ?T {
            if (self.empty()) return null;
            return self.buffer[self.index];
        }

        pub inline fn head_ptr(self: *Self) ?*T {
            if (self.empty()) return null;
            return &self.buffer[self.index];
        }

        pub inline fn tail(self: Self) ?T {
            if (self.empty()) return null;
            return self.buffer[(self.index + self.count - 1) % self.buffer.len];
        }

        pub inline fn tail_ptr(self: *Self) ?*T {
            if (self.empty()) return null;
            return &self.buffer[(self.index + self.count - 1) % self.buffer.len];
        }

        pub inline fn get_ptr(self: *Self, index: usize) ?*T {
            if (index < self.count) {
                return &self.buffer[(self.index + index) % self.buffer.len];
            } else {
                assert(index < count_max);
                return null;
            }
        }

        pub inline fn next_tail(self: Self) ?T {
            if (self.full()) return null;
            return self.buffer[(self.index + self.count) % self.buffer.len];
        }

        pub inline fn next_tail_ptr(self: *Self) ?*T {
            if (self.full()) return null;
            return &self.buffer[(self.index + self.count) % self.buffer.len];
        }

        pub inline fn advance_head(self: *Self) void {
            self.index += 1;
            self.index %= self.buffer.len;
            self.count -= 1;
        }

        pub inline fn advance_tail(self: *Self) void {
            assert(self.count < self.buffer.len);
            self.count += 1;
        }

        pub inline fn retreat_tail(self: *Self) void {
            self.count -= 1;
        }

        /// Returns whether the ring buffer is completely full.
        pub inline fn full(self: Self) bool {
            return self.count == self.buffer.len;
        }

        /// Returns whether the ring buffer is completely empty.
        pub inline fn empty(self: Self) bool {
            return self.count == 0;
        }

        // Higher level, less error-prone wrappers:

        /// Add an element to the RingBuffer. Returns an error if the buffer
        /// is already full and the element could not be added.
        pub fn push(self: *Self, item: T) error{NoSpaceLeft}!void {
            const ptr = self.next_tail_ptr() orelse return error.NoSpaceLeft;
            ptr.* = item;
            self.advance_tail();
        }

        /// Add an element to a RingBuffer, and assert that the capacity is sufficient.
        pub fn push_assume_capacity(self: *Self, item: T) void {
            self.push(item) catch |err| switch (err) {
                error.NoSpaceLeft => unreachable,
            };
        }

        /// Remove and return the next item, if any.
        pub fn pop(self: *Self) ?T {
            const result = self.head() orelse return null;
            self.advance_head();
            return result;
        }

        /// Remove and return the last item, if any.
        pub fn pop_tail(self: *Self) ?T {
            const result = self.tail() orelse return null;
            self.retreat_tail();
            return result;
        }

        pub const Iterator = struct {
            ring: *Self,
            count: usize = 0,

            pub fn next(it: *Iterator) ?T {
                assert(it.count <= it.ring.count);
                if (it.count == it.ring.count) return null;
                defer it.count += 1;
                return it.ring.buffer[(it.ring.index + it.count) % it.ring.buffer.len];
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

test "RingBuffer: low level interface" {
    const Ring = RingBuffer(u32, 2);

    var ring = Ring{};
    try test_iterator(Ring, &ring, &[_]u32{});

    try testing.expectEqual(@as(?u32, null), ring.head());
    try testing.expectEqual(@as(?*u32, null), ring.head_ptr());
    try testing.expectEqual(@as(?u32, null), ring.tail());
    try testing.expectEqual(@as(?*u32, null), ring.tail_ptr());

    ring.next_tail_ptr().?.* = 0;
    ring.advance_tail();
    try testing.expectEqual(@as(?u32, 0), ring.tail());
    try testing.expectEqual(@as(u32, 0), ring.tail_ptr().?.*);
    try test_iterator(Ring, &ring, &[_]u32{0});

    ring.next_tail_ptr().?.* = 1;
    ring.advance_tail();
    try testing.expectEqual(@as(?u32, 1), ring.tail());
    try testing.expectEqual(@as(u32, 1), ring.tail_ptr().?.*);
    try test_iterator(Ring, &ring, &[_]u32{ 0, 1 });

    try testing.expectEqual(@as(?u32, null), ring.next_tail());
    try testing.expectEqual(@as(?*u32, null), ring.next_tail_ptr());

    try testing.expectEqual(@as(?u32, 0), ring.head());
    try testing.expectEqual(@as(u32, 0), ring.head_ptr().?.*);
    ring.advance_head();
    try test_iterator(Ring, &ring, &[_]u32{1});

    ring.next_tail_ptr().?.* = 2;
    ring.advance_tail();
    try testing.expectEqual(@as(?u32, 2), ring.tail());
    try testing.expectEqual(@as(u32, 2), ring.tail_ptr().?.*);
    try test_iterator(Ring, &ring, &[_]u32{ 1, 2 });

    var iterator = ring.iterator();
    while (iterator.next_ptr()) |item_ptr| {
        item_ptr.* += 1000;
    }

    try testing.expectEqual(@as(?u32, 1001), ring.head());
    try testing.expectEqual(@as(u32, 1001), ring.head_ptr().?.*);
    ring.advance_head();
    try test_iterator(Ring, &ring, &[_]u32{1002});

    ring.next_tail_ptr().?.* = 3;
    ring.advance_tail();
    try testing.expectEqual(@as(?u32, 3), ring.tail());
    try testing.expectEqual(@as(u32, 3), ring.tail_ptr().?.*);
    try test_iterator(Ring, &ring, &[_]u32{ 1002, 3 });

    try testing.expectEqual(@as(?u32, 1002), ring.head());
    try testing.expectEqual(@as(u32, 1002), ring.head_ptr().?.*);
    ring.advance_head();
    try test_iterator(Ring, &ring, &[_]u32{3});

    try testing.expectEqual(@as(?u32, 3), ring.head());
    try testing.expectEqual(@as(u32, 3), ring.head_ptr().?.*);
    ring.advance_head();
    try test_iterator(Ring, &ring, &[_]u32{});

    try testing.expectEqual(@as(?u32, null), ring.head());
    try testing.expectEqual(@as(?*u32, null), ring.head_ptr());
    try testing.expectEqual(@as(?u32, null), ring.tail());
    try testing.expectEqual(@as(?*u32, null), ring.tail_ptr());
}

test "RingBuffer: push/pop high level interface" {
    var fifo = RingBuffer(u32, 3){};

    try testing.expect(!fifo.full());
    try testing.expect(fifo.empty());
    try testing.expectEqual(@as(?*u32, null), fifo.get_ptr(0));
    try testing.expectEqual(@as(?*u32, null), fifo.get_ptr(1));
    try testing.expectEqual(@as(?*u32, null), fifo.get_ptr(2));

    try fifo.push(1);
    try testing.expectEqual(@as(?u32, 1), fifo.head());
    try testing.expectEqual(@as(u32, 1), fifo.get_ptr(0).?.*);
    try testing.expectEqual(@as(?*u32, null), fifo.get_ptr(1));

    try testing.expect(!fifo.full());
    try testing.expect(!fifo.empty());

    try fifo.push(2);
    try testing.expectEqual(@as(?u32, 1), fifo.head());
    try testing.expectEqual(@as(u32, 2), fifo.get_ptr(1).?.*);

    try fifo.push(3);
    try testing.expectError(error.NoSpaceLeft, fifo.push(4));

    try testing.expect(fifo.full());
    try testing.expect(!fifo.empty());

    try testing.expectEqual(@as(?u32, 1), fifo.head());
    try testing.expectEqual(@as(?u32, 1), fifo.pop());
    try testing.expectEqual(@as(u32, 2), fifo.get_ptr(0).?.*);
    try testing.expectEqual(@as(u32, 3), fifo.get_ptr(1).?.*);
    try testing.expectEqual(@as(?*u32, null), fifo.get_ptr(2));

    try testing.expect(!fifo.full());
    try testing.expect(!fifo.empty());

    try fifo.push(4);

    try testing.expectEqual(@as(?u32, 2), fifo.pop());
    try testing.expectEqual(@as(?u32, 3), fifo.pop());
    try testing.expectEqual(@as(?u32, 4), fifo.pop());
    try testing.expectEqual(@as(?u32, null), fifo.pop());

    try testing.expect(!fifo.full());
    try testing.expect(fifo.empty());
}

test "RingBuffer: pop_tail" {
    var lifo = RingBuffer(u32, 3){};
    try lifo.push(1);
    try lifo.push(2);
    try lifo.push(3);
    try testing.expect(lifo.full());

    try testing.expectEqual(@as(?u32, 3), lifo.pop_tail());
    try testing.expectEqual(@as(?u32, 1), lifo.head());
    try testing.expectEqual(@as(?u32, 2), lifo.pop_tail());
    try testing.expectEqual(@as(?u32, 1), lifo.head());
    try testing.expectEqual(@as(?u32, 1), lifo.pop_tail());
    try testing.expectEqual(@as(?u32, null), lifo.pop_tail());
    try testing.expect(lifo.empty());
}
