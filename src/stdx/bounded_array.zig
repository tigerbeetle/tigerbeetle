const std = @import("std");
const stdx = @import("stdx.zig");
const assert = std.debug.assert;

/// A version of standard `BoundedArray` with TigerBeetle-idiomatic APIs.
///
/// See <https://github.com/tigerbeetle/tigerbeetle/pull/1121> for the original reason for
/// wrapping --- we need an `fn count` which returns an `usize`, instead of potentially much smaller
/// type which stores the length internally.
pub fn BoundedArrayType(comptime T: type, comptime buffer_capacity: usize) type {
    // Smuggle the std version past tidy.
    const Inner = @import("std").BoundedArray(T, buffer_capacity);

    return struct {
        inner: Inner = Inner{},

        const BoundedArray = @This();

        pub inline fn from_slice(items: []const T) error{Overflow}!BoundedArray {
            return .{ .inner = try Inner.fromSlice(items) };
        }

        pub inline fn count(array: *const BoundedArray) usize {
            return array.inner.len;
        }

        /// Returns count of elements in this BoundedArray in the specified integer types,
        /// checking at compile time that it indeed can represent the length.
        pub inline fn count_as(array: *const BoundedArray, comptime Int: type) Int {
            comptime assert(buffer_capacity <= std.math.maxInt(Int));
            return @intCast(array.inner.len);
        }

        pub inline fn full(self: BoundedArray) bool {
            return self.count() == buffer_capacity;
        }

        pub inline fn empty(self: BoundedArray) bool {
            return self.count() == 0;
        }

        pub inline fn get(array: *const BoundedArray, index: usize) T {
            return array.inner.get(index);
        }

        pub inline fn slice(array: *BoundedArray) []T {
            return array.inner.slice();
        }

        pub inline fn const_slice(array: *const BoundedArray) []const T {
            return array.inner.constSlice();
        }

        pub inline fn unused_capacity_slice(array: *BoundedArray) []T {
            return array.inner.unusedCapacitySlice();
        }

        pub fn resize(array: *BoundedArray, len: usize) error{Overflow}!void {
            try array.inner.resize(len);
        }

        pub fn insert_at(array: *BoundedArray, index: usize, item: T) void {
            assert(!array.full());
            assert(index <= array.inner.len);

            array.inner.len += 1;

            var slice_ = array.slice();
            stdx.copy_right(.exact, T, slice_[index + 1 ..], slice_[index .. slice_.len - 1]);
            slice_[index] = item;
        }

        pub fn push(array: *BoundedArray, item: T) void {
            assert(!array.full());
            array.inner.appendAssumeCapacity(item);
        }

        pub fn push_slice(array: *BoundedArray, items: []const T) void {
            assert(array.count() + items.len <= array.capacity());
            array.inner.appendSliceAssumeCapacity(items);
        }

        pub inline fn swap_remove(array: *BoundedArray, index: usize) T {
            return array.inner.swapRemove(index);
        }

        pub inline fn ordered_remove(array: *BoundedArray, index: usize) T {
            return array.inner.orderedRemove(index);
        }

        pub inline fn truncate(array: *BoundedArray, count_new: usize) void {
            assert(count_new <= array.count());
            array.inner.len = @intCast(count_new); // can't overflow due to check above.
        }

        pub inline fn clear(array: *BoundedArray) void {
            array.inner.len = 0;
        }

        pub inline fn pop(array: *BoundedArray) ?T {
            return array.inner.pop();
        }

        pub inline fn capacity(array: *BoundedArray) usize {
            return array.inner.capacity();
        }
    };
}

test "BoundedArray.insert_at" {
    const items_max = 32;
    const BoundedArrayU64 = BoundedArrayType(u64, items_max);

    // Test lists of every size (less than the capacity).
    for (0..items_max) |len| {
        var list_base = BoundedArrayU64{};
        for (0..len) |i| {
            list_base.push(i);
        }

        // Test an insert at every possible position (including an append).
        for (0..list_base.count() + 1) |i| {
            var list = list_base;

            list.insert_at(i, 12345);

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
