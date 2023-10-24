const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const stdx = @import("stdx.zig");

/// A First In, First Out ring buffer.
pub fn RingBuffer(
    comptime T: type,
    comptime buffer_type: union(enum) {
        array: usize, // capacity
        slice, // (Capacity is passed to init() at runtime).
    },
) type {
    return struct {
        const Self = @This();

        pub const count_max = switch (buffer_type) {
            .array => |count_max_| count_max_,
            .slice => {},
        };

        buffer: switch (buffer_type) {
            .array => |count_max_| [count_max_]T,
            .slice => []T,
        },

        /// The index of the slot with the first item, if any.
        index: usize = 0,

        /// The number of items in the buffer.
        count: usize = 0,

        pub usingnamespace switch (buffer_type) {
            .array => struct {
                pub fn init() Self {
                    return .{ .buffer = undefined };
                }
            },
            .slice => struct {
                pub fn init(allocator: mem.Allocator, capacity: usize) !Self {
                    assert(capacity > 0);

                    const buffer = try allocator.alloc(T, capacity);
                    errdefer allocator.free(buffer);
                    return Self{ .buffer = buffer };
                }

                pub fn deinit(self: *Self, allocator: mem.Allocator) void {
                    allocator.free(self.buffer);
                }
            },
        };

        pub inline fn clear(self: *Self) void {
            self.index = 0;
            self.count = 0;
        }

        // TODO Add doc comments to these functions:
        pub inline fn head(self: Self) ?T {
            if (self.buffer.len == 0 or self.empty()) return null;
            return self.buffer[self.index];
        }

        pub inline fn head_ptr(self: *Self) ?*T {
            if (self.buffer.len == 0 or self.empty()) return null;
            return &self.buffer[self.index];
        }

        pub inline fn head_ptr_const(self: *const Self) ?*const T {
            if (self.buffer.len == 0 or self.empty()) return null;
            return &self.buffer[self.index];
        }

        pub inline fn tail(self: Self) ?T {
            if (self.buffer.len == 0 or self.empty()) return null;
            return self.buffer[(self.index + self.count - 1) % self.buffer.len];
        }

        pub inline fn tail_ptr(self: *Self) ?*T {
            if (self.buffer.len == 0 or self.empty()) return null;
            return &self.buffer[(self.index + self.count - 1) % self.buffer.len];
        }

        pub inline fn tail_ptr_const(self: *const Self) ?*const T {
            if (self.buffer.len == 0 or self.empty()) return null;
            return &self.buffer[(self.index + self.count - 1) % self.buffer.len];
        }

        pub fn get(self: *const Self, index: usize) ?T {
            if (self.buffer.len == 0) unreachable;

            if (index < self.count) {
                return self.buffer[(self.index + index) % self.buffer.len];
            } else {
                assert(index < self.buffer.len);
                return null;
            }
        }

        pub inline fn get_ptr(self: *Self, index: usize) ?*T {
            if (self.buffer.len == 0) unreachable;

            if (index < self.count) {
                return &self.buffer[(self.index + index) % self.buffer.len];
            } else {
                assert(index < self.buffer.len);
                return null;
            }
        }

        pub inline fn next_tail(self: Self) ?T {
            if (self.buffer.len == 0 or self.full()) return null;
            return self.buffer[(self.index + self.count) % self.buffer.len];
        }

        pub inline fn next_tail_ptr(self: *Self) ?*T {
            if (self.buffer.len == 0 or self.full()) return null;
            return &self.buffer[(self.index + self.count) % self.buffer.len];
        }

        pub inline fn next_tail_ptr_const(self: *const Self) ?*const T {
            if (self.buffer.len == 0 or self.full()) return null;
            return &self.buffer[(self.index + self.count) % self.buffer.len];
        }

        pub inline fn advance_head(self: *Self) void {
            self.index += 1;
            self.index %= self.buffer.len;
            self.count -= 1;
        }

        pub inline fn retreat_head(self: *Self) void {
            assert(self.count < self.buffer.len);

            // This condition is covered by the above assert, but it is necessary to make it
            // explicitly unreachable so that the compiler doesn't error when computing (at
            // comptime) `buffer.len - 1` for a zero-capacity array-backed ring buffer.
            if (self.buffer.len == 0) unreachable;

            self.index += self.buffer.len - 1;
            self.index %= self.buffer.len;
            self.count += 1;
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

        pub fn push_head(self: *Self, item: T) error{NoSpaceLeft}!void {
            if (self.count == self.buffer.len) return error.NoSpaceLeft;
            self.push_head_assume_capacity(item);
        }

        pub fn push_head_assume_capacity(self: *Self, item: T) void {
            assert(self.count < self.buffer.len);

            self.retreat_head();
            self.head_ptr().?.* = item;
        }

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

        pub fn push_slice(self: *Self, items: []const T) error{NoSpaceLeft}!void {
            if (self.buffer.len == 0) return error.NoSpaceLeft;
            if (self.count + items.len > self.buffer.len) return error.NoSpaceLeft;

            const pre_wrap_start = (self.index + self.count) % self.buffer.len;
            const pre_wrap_count = @min(items.len, self.buffer.len - pre_wrap_start);
            const post_wrap_count = items.len - pre_wrap_count;

            stdx.copy_disjoint(.inexact, T, self.buffer[pre_wrap_start..], items[0..pre_wrap_count]);
            stdx.copy_disjoint(.exact, T, self.buffer[0..post_wrap_count], items[pre_wrap_count..]);

            self.count += items.len;
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
            ring: *const Self,
            count: usize = 0,

            pub fn next(it: *Iterator) ?T {
                if (it.ring.buffer.len == 0) return null;
                // TODO Use next_ptr() internally to avoid duplicating this code.
                assert(it.count <= it.ring.count);
                if (it.count == it.ring.count) return null;
                defer it.count += 1;
                return it.ring.buffer[(it.ring.index + it.count) % it.ring.buffer.len];
            }

            pub fn next_ptr(it: *Iterator) ?*const T {
                assert(it.count <= it.ring.count);
                if (it.ring.buffer.len == 0) return null;
                if (it.count == it.ring.count) return null;
                defer it.count += 1;
                return &it.ring.buffer[(it.ring.index + it.count) % it.ring.buffer.len];
            }
        };

        /// Returns an iterator to iterate through all `count` items in the ring buffer.
        /// The iterator is invalidated if the ring buffer is advanced.
        pub fn iterator(self: *const Self) Iterator {
            return .{ .ring = self };
        }

        pub const IteratorMutable = struct {
            ring: *Self,
            count: usize = 0,

            pub fn next_ptr(it: *IteratorMutable) ?*T {
                assert(it.count <= it.ring.count);
                if (it.ring.buffer.len == 0) return null;
                if (it.count == it.ring.count) return null;
                defer it.count += 1;
                return &it.ring.buffer[(it.ring.index + it.count) % it.ring.buffer.len];
            }
        };

        // TODO Add to tests.
        pub fn iterator_mutable(self: *Self) IteratorMutable {
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

fn test_low_level_interface(comptime Ring: type, ring: *Ring) !void {
    try ring.push_slice(&[_]u32{});
    try test_iterator(Ring, ring, &[_]u32{});

    try testing.expectError(error.NoSpaceLeft, ring.push_slice(&[_]u32{ 1, 2, 3 }));

    try ring.push_slice(&[_]u32{1});
    try testing.expectEqual(@as(?u32, 1), ring.tail());
    try testing.expectEqual(@as(u32, 1), ring.tail_ptr().?.*);
    ring.advance_head();

    try testing.expectEqual(@as(usize, 1), ring.index);
    try testing.expectEqual(@as(usize, 0), ring.count);
    try ring.push_slice(&[_]u32{ 1, 2 });
    try test_iterator(Ring, ring, &[_]u32{ 1, 2 });
    ring.advance_head();
    ring.advance_head();

    try testing.expectEqual(@as(usize, 1), ring.index);
    try testing.expectEqual(@as(usize, 0), ring.count);
    try ring.push_slice(&[_]u32{1});
    try testing.expectEqual(@as(?u32, 1), ring.tail());
    try testing.expectEqual(@as(u32, 1), ring.tail_ptr().?.*);
    ring.advance_head();

    try testing.expectEqual(@as(?u32, null), ring.head());
    try testing.expectEqual(@as(?*u32, null), ring.head_ptr());
    try testing.expectEqual(@as(?u32, null), ring.tail());
    try testing.expectEqual(@as(?*u32, null), ring.tail_ptr());

    ring.next_tail_ptr().?.* = 0;
    ring.advance_tail();
    try testing.expectEqual(@as(?u32, 0), ring.tail());
    try testing.expectEqual(@as(u32, 0), ring.tail_ptr().?.*);
    try test_iterator(Ring, ring, &[_]u32{0});

    ring.next_tail_ptr().?.* = 1;
    ring.advance_tail();
    try testing.expectEqual(@as(?u32, 1), ring.tail());
    try testing.expectEqual(@as(u32, 1), ring.tail_ptr().?.*);
    try test_iterator(Ring, ring, &[_]u32{ 0, 1 });

    try testing.expectEqual(@as(?u32, null), ring.next_tail());
    try testing.expectEqual(@as(?*u32, null), ring.next_tail_ptr());

    try testing.expectEqual(@as(?u32, 0), ring.head());
    try testing.expectEqual(@as(u32, 0), ring.head_ptr().?.*);
    ring.advance_head();
    try test_iterator(Ring, ring, &[_]u32{1});

    ring.next_tail_ptr().?.* = 2;
    ring.advance_tail();
    try testing.expectEqual(@as(?u32, 2), ring.tail());
    try testing.expectEqual(@as(u32, 2), ring.tail_ptr().?.*);
    try test_iterator(Ring, ring, &[_]u32{ 1, 2 });

    ring.advance_head();
    try test_iterator(Ring, ring, &[_]u32{2});

    ring.next_tail_ptr().?.* = 3;
    ring.advance_tail();
    try testing.expectEqual(@as(?u32, 3), ring.tail());
    try testing.expectEqual(@as(u32, 3), ring.tail_ptr().?.*);
    try test_iterator(Ring, ring, &[_]u32{ 2, 3 });

    try testing.expectEqual(@as(?u32, 2), ring.head());
    try testing.expectEqual(@as(u32, 2), ring.head_ptr().?.*);
    ring.advance_head();
    try test_iterator(Ring, ring, &[_]u32{3});

    try testing.expectEqual(@as(?u32, 3), ring.head());
    try testing.expectEqual(@as(u32, 3), ring.head_ptr().?.*);
    ring.advance_head();
    try test_iterator(Ring, ring, &[_]u32{});

    try testing.expectEqual(@as(?u32, null), ring.head());
    try testing.expectEqual(@as(?*u32, null), ring.head_ptr());
    try testing.expectEqual(@as(?u32, null), ring.tail());
    try testing.expectEqual(@as(?*u32, null), ring.tail_ptr());
}

test "RingBuffer: low level interface" {
    const ArrayRing = RingBuffer(u32, .{ .array = 2 });
    var array_ring = ArrayRing.init();
    try test_low_level_interface(ArrayRing, &array_ring);

    const PointerRing = RingBuffer(u32, .slice);
    var pointer_ring = try PointerRing.init(testing.allocator, 2);
    defer pointer_ring.deinit(testing.allocator);
    try test_low_level_interface(PointerRing, &pointer_ring);
}

test "RingBuffer: push/pop high level interface" {
    var fifo = RingBuffer(u32, .{ .array = 3 }).init();

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
    var lifo = RingBuffer(u32, .{ .array = 3 }).init();
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

test "RingBuffer: push_head" {
    var ring = RingBuffer(u32, .{ .array = 3 }).init();
    try ring.push_head(1);
    try ring.push(2);
    try ring.push_head(3);
    try testing.expect(ring.full());

    try testing.expectEqual(@as(?u32, 3), ring.pop());
    try testing.expectEqual(@as(?u32, 1), ring.pop());
    try testing.expectEqual(@as(?u32, 2), ring.pop());
    try testing.expect(ring.empty());
}

test "RingBuffer: count_max=0" {
    std.testing.refAllDecls(RingBuffer(u32, .{ .array = 0 }));
}
