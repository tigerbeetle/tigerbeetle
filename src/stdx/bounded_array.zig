const std = @import("std");
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

        pub inline fn add_one_assume_capacity(array: *Self) *T {
            return array.inner.addOneAssumeCapacity();
        }

        pub inline fn append_assume_capacity(array: *Self, item: T) void {
            array.inner.appendAssumeCapacity(item);
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
    };
}
