const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const stdx = @import("stdx.zig");

/// A First In, First Out ring buffer.
pub fn RingBufferType(
    comptime T: type,
    comptime buffer_type: union(enum) {
        array: usize, // capacity
        slice, // (Capacity is passed to init() at runtime).
    },
) type {
    return struct {
        const RingBuffer = @This();

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

        pub const init = switch (buffer_type) {
            .array => init_array,
            .slice => init_slice,
        };

        fn init_array() RingBuffer {
            comptime assert(buffer_type == .array);
            return .{ .buffer = undefined };
        }

        fn init_slice(allocator: mem.Allocator, capacity: usize) !RingBuffer {
            comptime assert(buffer_type == .slice);
            assert(capacity > 0);

            const buffer = try allocator.alloc(T, capacity);
            errdefer allocator.free(buffer);
            return RingBuffer{ .buffer = buffer };
        }

        pub const deinit = switch (buffer_type) {
            .array => {},
            .slice => deinit_slice,
        };

        fn deinit_slice(self: *RingBuffer, allocator: mem.Allocator) void {
            comptime assert(buffer_type == .slice);
            allocator.free(self.buffer);
        }

        pub inline fn clear(self: *RingBuffer) void {
            self.index = 0;
            self.count = 0;
        }

        // TODO Add doc comments to these functions:
        pub inline fn head(self: RingBuffer) ?T {
            if (self.buffer.len == 0 or self.empty()) return null;
            return self.buffer[self.index];
        }

        pub inline fn head_ptr(self: *RingBuffer) ?*T {
            if (self.buffer.len == 0 or self.empty()) return null;
            return &self.buffer[self.index];
        }

        pub inline fn head_ptr_const(self: *const RingBuffer) ?*const T {
            if (self.buffer.len == 0 or self.empty()) return null;
            return &self.buffer[self.index];
        }

        pub inline fn tail(self: RingBuffer) ?T {
            if (self.buffer.len == 0 or self.empty()) return null;
            return self.buffer[(self.index + self.count - 1) % self.buffer.len];
        }

        pub inline fn tail_ptr(self: *RingBuffer) ?*T {
            if (self.buffer.len == 0 or self.empty()) return null;
            return &self.buffer[(self.index + self.count - 1) % self.buffer.len];
        }

        pub inline fn tail_ptr_const(self: *const RingBuffer) ?*const T {
            if (self.buffer.len == 0 or self.empty()) return null;
            return &self.buffer[(self.index + self.count - 1) % self.buffer.len];
        }

        pub fn get(self: *const RingBuffer, index: usize) ?T {
            if (self.buffer.len == 0) unreachable;

            if (index < self.count) {
                return self.buffer[(self.index + index) % self.buffer.len];
            } else {
                assert(index < self.buffer.len);
                return null;
            }
        }

        pub inline fn get_ptr(self: *RingBuffer, index: usize) ?*T {
            if (self.buffer.len == 0) unreachable;

            if (index < self.count) {
                return &self.buffer[(self.index + index) % self.buffer.len];
            } else {
                assert(index < self.buffer.len);
                return null;
            }
        }

        pub inline fn next_tail(self: RingBuffer) ?T {
            if (self.buffer.len == 0 or self.full()) return null;
            return self.buffer[(self.index + self.count) % self.buffer.len];
        }

        pub inline fn next_tail_ptr(self: *RingBuffer) ?*T {
            if (self.buffer.len == 0 or self.full()) return null;
            return &self.buffer[(self.index + self.count) % self.buffer.len];
        }

        pub inline fn next_tail_ptr_const(self: *const RingBuffer) ?*const T {
            if (self.buffer.len == 0 or self.full()) return null;
            return &self.buffer[(self.index + self.count) % self.buffer.len];
        }

        pub inline fn advance_head(self: *RingBuffer) void {
            self.index += 1;
            self.index %= self.buffer.len;
            self.count -= 1;
        }

        pub fn advance_head_many(self: *RingBuffer, discard: u64) void {
            assert(discard <= self.count);
            if (self.buffer.len == 0) return;

            self.index += discard;
            self.index %= self.buffer.len;
            self.count -= discard;
        }

        pub inline fn retreat_head(self: *RingBuffer) void {
            assert(self.count < self.buffer.len);

            // This condition is covered by the above assert, but it is necessary to make it
            // explicitly unreachable so that the compiler doesn't error when computing (at
            // comptime) `buffer.len - 1` for a zero-capacity array-backed ring buffer.
            if (self.buffer.len == 0) unreachable;

            self.index += self.buffer.len - 1;
            self.index %= self.buffer.len;
            self.count += 1;
        }

        pub inline fn advance_tail(self: *RingBuffer) void {
            assert(self.count < self.buffer.len);
            self.count += 1;
        }

        pub inline fn retreat_tail(self: *RingBuffer) void {
            self.count -= 1;
        }

        /// Returns whether the ring buffer is completely full.
        pub inline fn full(self: RingBuffer) bool {
            return self.count == self.buffer.len;
        }

        pub inline fn spare_capacity(self: RingBuffer) usize {
            return self.buffer.len - self.count;
        }

        /// Returns whether the ring buffer is completely empty.
        pub inline fn empty(self: RingBuffer) bool {
            return self.count == 0;
        }

        // Higher level, less error-prone wrappers:

        pub fn push_head(self: *RingBuffer, item: T) error{NoSpaceLeft}!void {
            if (self.count == self.buffer.len) return error.NoSpaceLeft;
            self.push_head_assume_capacity(item);
        }

        pub fn push_head_assume_capacity(self: *RingBuffer, item: T) void {
            assert(self.count < self.buffer.len);

            self.retreat_head();
            self.head_ptr().?.* = item;
        }

        /// Add an element to the RingBuffer. Returns an error if the buffer
        /// is already full and the element could not be added.
        pub fn push(self: *RingBuffer, item: T) error{NoSpaceLeft}!void {
            const ptr = self.next_tail_ptr() orelse return error.NoSpaceLeft;
            ptr.* = item;
            self.advance_tail();
        }

        /// Add an element to a RingBuffer, and assert that the capacity is sufficient.
        pub fn push_assume_capacity(self: *RingBuffer, item: T) void {
            self.push(item) catch |err| switch (err) {
                error.NoSpaceLeft => unreachable,
            };
        }

        pub fn push_slice(self: *RingBuffer, items: []const T) error{NoSpaceLeft}!void {
            if (self.buffer.len == 0) return error.NoSpaceLeft;
            if (self.count + items.len > self.buffer.len) return error.NoSpaceLeft;

            const pre_wrap_start = (self.index + self.count) % self.buffer.len;
            const pre_wrap_count = @min(items.len, self.buffer.len - pre_wrap_start);
            const post_wrap_count = items.len - pre_wrap_count;

            const pre_wrap_items = items[0..pre_wrap_count];
            const post_wrap_items = items[pre_wrap_count..];
            stdx.copy_disjoint(.inexact, T, self.buffer[pre_wrap_start..], pre_wrap_items);
            stdx.copy_disjoint(.exact, T, self.buffer[0..post_wrap_count], post_wrap_items);

            self.count += items.len;
        }

        /// Remove and return the next item, if any.
        pub fn pop(self: *RingBuffer) ?T {
            const result = self.head() orelse return null;
            self.advance_head();
            return result;
        }

        /// Remove and return the last item, if any.
        pub fn pop_tail(self: *RingBuffer) ?T {
            const result = self.tail() orelse return null;
            self.retreat_tail();
            return result;
        }

        pub const Iterator = struct {
            ring: *const RingBuffer,
            count: usize = 0,

            pub fn next(it: *Iterator) ?T {
                if (it.next_ptr()) |item| {
                    return item.*;
                }
                return null;
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
        pub fn iterator(self: *const RingBuffer) Iterator {
            return .{ .ring = self };
        }

        pub const IteratorMutable = struct {
            ring: *RingBuffer,
            count: usize = 0,

            pub fn next_ptr(it: *IteratorMutable) ?*T {
                assert(it.count <= it.ring.count);
                if (it.ring.buffer.len == 0) return null;
                if (it.count == it.ring.count) return null;
                defer it.count += 1;
                return &it.ring.buffer[(it.ring.index + it.count) % it.ring.buffer.len];
            }
        };

        pub fn iterator_mutable(self: *RingBuffer) IteratorMutable {
            return .{ .ring = self };
        }
    };
}

const testing = std.testing;

fn test_iterator(comptime T: type, ring: *T, values: []const u32) !void {
    const ring_index = ring.index;

    inline for (.{ .immutable, .mutable }) |mutability| {
        for (0..2) |_| {
            var iterator = switch (mutability) {
                .immutable => ring.iterator(),
                .mutable => ring.iterator_mutable(),
                else => unreachable,
            };
            var index: u32 = 0;
            switch (mutability) {
                .immutable => while (iterator.next()) |item| {
                    try testing.expectEqual(values[index], item);
                    index += 1;
                },
                .mutable => {
                    const permutation = @divFloor(std.math.maxInt(u32), 2);
                    while (iterator.next_ptr()) |item| {
                        try testing.expectEqual(values[index], item.*);
                        item.* += permutation + index;
                        index += 1;
                    }
                    iterator = ring.iterator_mutable();
                    var check_index: u32 = 0;
                    while (iterator.next_ptr()) |item| {
                        try testing.expectEqual(
                            values[check_index] + permutation + check_index,
                            item.*,
                        );
                        item.* -= permutation + check_index;
                        check_index += 1;
                    }
                    try testing.expectEqual(index, check_index);
                },
                else => unreachable,
            }
            try testing.expectEqual(values.len, index);
        }

        try testing.expectEqual(ring_index, ring.index);
    }
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
    const ArrayRing = RingBufferType(u32, .{ .array = 2 });
    var array_ring = ArrayRing.init();
    try test_low_level_interface(ArrayRing, &array_ring);

    const PointerRing = RingBufferType(u32, .slice);
    var pointer_ring = try PointerRing.init(testing.allocator, 2);
    defer pointer_ring.deinit(testing.allocator);
    try test_low_level_interface(PointerRing, &pointer_ring);
}

test "RingBuffer: push/pop high level interface" {
    var fifo = RingBufferType(u32, .{ .array = 3 }).init();

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
    var lifo = RingBufferType(u32, .{ .array = 3 }).init();
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
    var ring = RingBufferType(u32, .{ .array = 3 }).init();
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
    std.testing.refAllDecls(RingBufferType(u32, .{ .array = 0 }));
}
