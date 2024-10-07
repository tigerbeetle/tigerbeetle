const std = @import("std");
const stdx = @import("../stdx.zig");
const assert = std.debug.assert;

/// A version of standard `BoundedArray` with TigerBeetle-idiomatic APIs.
///
/// See <https://github.com/tigerbeetle/tigerbeetle/pull/1121> for the original reason for
/// wrapping --- we need an `fn count` which returns an `usize`, instead of potentially much smaller
/// type which stores the length internally.
pub fn BoundedArray(comptime T: type, comptime capacity: usize) type {
    const Inner = @import("std").BoundedArray(T, capacity); // smuggle the std version past tidy

    return struct {
        inner: Inner = Inner{},

        const Self = @This();

        pub inline fn from_slice(items: []const T) error{Overflow}!Self {
            return .{ .inner = try Inner.fromSlice(items) };
        }

        pub inline fn count(array: *const Self) usize {
            return array.inner.len;
        }

        /// Returns count of elements in this BoundedArray in the specified integer types,
        /// checking at compile time that it indeed can represent the length.
        pub inline fn count_as(array: *const Self, comptime Int: type) Int {
            return array.inner.len;
        }

        pub inline fn full(self: Self) bool {
            return self.count() == capacity;
        }

        pub inline fn empty(self: Self) bool {
            return self.count() == 0;
        }

        pub inline fn get(array: *const Self, index: usize) T {
            return array.inner.get(index);
        }

        pub inline fn slice(array: *Self) []T {
            return array.inner.slice();
        }

        pub inline fn const_slice(array: *const Self) []const T {
            return array.inner.constSlice();
        }

        pub inline fn unused_capacity_slice(array: *Self) []T {
            return array.inner.unusedCapacitySlice();
        }

        pub fn resize(array: *Self, len: usize) error{Overflow}!void {
            try array.inner.resize(len);
        }

        pub inline fn add_one_assume_capacity(array: *Self) *T {
            return array.inner.addOneAssumeCapacity();
        }

        pub fn insert_assume_capacity(self: *Self, index: usize, item: T) void {
            assert(self.inner.len < capacity);
            assert(index <= self.inner.len);

            self.inner.len += 1;

            var slice_ = self.slice();
            stdx.copy_right(.exact, T, slice_[index + 1 ..], slice_[index .. slice_.len - 1]);
            slice_[index] = item;
        }

        pub inline fn append_assume_capacity(array: *Self, item: T) void {
            array.inner.appendAssumeCapacity(item);
        }

        pub inline fn append_slice_assume_capacity(array: *Self, items: []const T) void {
            array.inner.appendSliceAssumeCapacity(items);
        }

        pub inline fn writer(self: *Self) Inner.Writer {
            return self.inner.writer();
        }

        pub inline fn swap_remove(array: *Self, index: usize) T {
            return array.inner.swapRemove(index);
        }

        pub inline fn truncate(array: *Self, count_new: usize) void {
            assert(count_new <= array.count());
            array.inner.len = @intCast(count_new); // can't overflow due to check above.
        }

        pub inline fn clear(array: *Self) void {
            array.inner.len = 0;
        }

        pub inline fn pop(array: *Self) T {
            return array.inner.pop();
        }
    };
}

test "BoundedArray.insert_assume_capacity" {
    const items_max = 32;
    const BoundedArrayU64 = BoundedArray(u64, items_max);

    // Test lists of every size (less than the capacity).
    for (0..items_max) |len| {
        var list_base = BoundedArrayU64{};
        for (0..len) |i| {
            list_base.append_assume_capacity(i);
        }

        // Test an insert at every possible position (including an append).
        for (0..list_base.count() + 1) |i| {
            var list = list_base;

            list.insert_assume_capacity(i, 12345);

            // Verify the result:

            try std.testing.expectEqual(list.count(), list_base.count() + 1);
            try std.testing.expectEqual(list.get(i), 12345);

            for (0..i) |j| {
                try std.testing.expectEqual(list.get(j), j);
            }

            for (i + 1..list.count()) |j| {
                try std.testing.expectEqual(list.get(j), j - 1);
            }
        }
    }
}
