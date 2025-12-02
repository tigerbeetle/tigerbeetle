const std = @import("std");
const stdx = @import("stdx.zig");
const assert = std.debug.assert;

/// A version of standard `BoundedArray` with TigerBeetle-idiomatic APIs.
pub fn BoundedArrayType(comptime T: type, comptime buffer_capacity: usize) type {
    return struct {
        buffer: [buffer_capacity]T = undefined,
        // Its not clear whether the best type of count is u32, usize, or u`log(buffer_capacity)`.
        // Use an ugly internal name here and expose `pub fn count` as an API.
        count_u32: u32 = 0,

        const BoundedArray = @This();

        pub inline fn from_slice(items: []const T) error{Overflow}!BoundedArray {
            if (items.len <= buffer_capacity) {
                var result: BoundedArray = .{};
                result.push_slice(items);
                return result;
            } else {
                return error.Overflow;
            }
        }

        pub inline fn count(array: *const BoundedArray) usize {
            return array.count_u32;
        }

        /// Returns count of elements in this BoundedArray in the specified integer types,
        /// checking at compile time that it indeed can represent the length.
        pub inline fn count_as(array: *const BoundedArray, comptime Int: type) Int {
            comptime assert(buffer_capacity <= std.math.maxInt(Int));
            return @intCast(array.count_u32);
        }

        pub inline fn full(array: BoundedArray) bool {
            return array.count_u32 == buffer_capacity;
        }

        pub inline fn empty(array: BoundedArray) bool {
            return array.count_u32 == 0;
        }

        pub inline fn get(array: *const BoundedArray, index: usize) T {
            assert(index < array.count_u32);
            return array.buffer[index];
        }

        pub inline fn slice(array: *BoundedArray) []T {
            return array.buffer[0..array.count_u32];
        }

        pub inline fn const_slice(array: *const BoundedArray) []const T {
            return array.buffer[0..array.count_u32];
        }

        pub inline fn unused_capacity_slice(array: *BoundedArray) []T {
            return array.buffer[array.count_u32..];
        }

        pub fn insert_at(array: *BoundedArray, index: usize, item: T) void {
            assert(!array.full());
            assert(index <= array.count_u32);
            stdx.copy_right(
                .exact,
                T,
                array.buffer[index + 1 .. array.count_u32 + 1],
                array.buffer[index..array.count_u32],
            );
            array.buffer[index] = item;
            array.count_u32 += 1;
        }

        pub fn push(array: *BoundedArray, item: T) void {
            assert(!array.full());
            array.buffer[array.count_u32] = item;
            array.count_u32 += 1;
        }

        pub fn push_slice(array: *BoundedArray, items: []const T) void {
            assert(array.count_u32 + items.len <= array.capacity());
            stdx.copy_disjoint(.inexact, T, array.buffer[array.count_u32..], items);
            array.count_u32 += @intCast(items.len);
        }

        pub inline fn swap_remove(array: *BoundedArray, index: usize) T {
            assert(array.count_u32 > 0);
            assert(index < array.count_u32);
            const result = array.buffer[index];
            array.count_u32 -= 1;
            array.buffer[index] = array.buffer[array.count_u32];
            return result;
        }

        pub inline fn ordered_remove(array: *BoundedArray, index: usize) T {
            assert(array.count_u32 > 0);
            assert(index < array.count_u32);
            const result = array.buffer[index];
            stdx.copy_left(
                .exact,
                T,
                array.buffer[index .. array.count_u32 - 1],
                array.buffer[index + 1 .. array.count_u32],
            );
            array.count_u32 -= 1;
            return result;
        }

        pub fn resize(array: *BoundedArray, count_new: usize) error{Overflow}!void {
            if (count_new <= buffer_capacity) {
                array.count_u32 = @intCast(count_new);
            } else {
                return error.Overflow;
            }
        }

        pub inline fn truncate(array: *BoundedArray, count_new: usize) void {
            assert(count_new <= array.count_u32);
            array.count_u32 = @intCast(count_new); // can't overflow due to check above.
        }

        pub inline fn clear(array: *BoundedArray) void {
            array.count_u32 = 0;
        }

        pub inline fn pop(array: *BoundedArray) ?T {
            if (array.count_u32 == 0) return null;
            array.count_u32 -= 1;
            return array.buffer[array.count_u32];
        }

        pub inline fn capacity(_: *BoundedArray) usize {
            return buffer_capacity;
        }
    };
}

test BoundedArrayType {
    const capacity = 8;
    const Array = BoundedArrayType(u8, capacity);
    const Model = std.ArrayListUnmanaged(u8);
    const swarm_count = 10;
    const action_count = 1_000;

    const gpa = std.testing.allocator;

    var array: Array = .{};
    var model: Model = try .initCapacity(gpa, capacity);
    defer model.deinit(gpa);

    var prng = stdx.PRNG.from_seed_testing();

    for (0..swarm_count) |_| {
        const swarm_weights = prng.enum_weights(std.meta.DeclEnum(Array));
        for (0..action_count) |_| {
            const action = prng.enum_weighted(std.meta.DeclEnum(Array), swarm_weights);
            switch (action) {
                .count => assert(array.count() == model.items.len),
                .count_as => assert(array.count_as(u8) == model.items.len),
                .full => assert(array.full() == (model.unusedCapacitySlice().len == 0)),
                .empty => assert(array.empty() == (model.items.len == 0)),
                .get => {
                    if (model.items.len > 0) {
                        const index = prng.index(model.items);
                        assert(array.get(index) == model.items[index]);
                    }
                },
                .slice => assert(std.mem.eql(u8, array.slice(), model.items)),
                .const_slice => assert(std.mem.eql(u8, array.const_slice(), model.items)),
                .unused_capacity_slice => {
                    assert(array.unused_capacity_slice().len == model.unusedCapacitySlice().len);
                },
                .insert_at => {
                    if (model.items.len < model.capacity) {
                        const index = prng.int_inclusive(usize, model.items.len);
                        const value = prng.int(u8);

                        array.insert_at(index, value);
                        model.insertAssumeCapacity(index, value);
                    }
                },
                .push => {
                    if (model.items.len < model.capacity) {
                        const value = prng.int(u8);

                        array.push(value);
                        model.appendAssumeCapacity(value);
                    }
                },
                .push_slice => {
                    var buffer: [capacity]u8 = undefined;
                    const count = prng.int_inclusive(usize, model.capacity - model.items.len);
                    for (0..count) |index| buffer[index] = prng.int(u8);
                    const slice = buffer[0..count];

                    array.push_slice(slice);
                    model.appendSliceAssumeCapacity(slice);
                },
                .swap_remove => {
                    if (model.items.len > 0) {
                        const index = prng.index(model.items);

                        const a = array.swap_remove(index);
                        const b = model.swapRemove(index);
                        assert(a == b);
                    }
                },
                .ordered_remove => {
                    if (model.items.len > 0) {
                        const index = prng.index(model.items);

                        const a = array.ordered_remove(index);
                        const b = model.orderedRemove(index);
                        assert(a == b);
                    }
                },
                .resize => {
                    const count_old = model.items.len;
                    const count_new = prng.int_inclusive(usize, capacity);

                    model.resize(gpa, count_new) catch unreachable;
                    array.resize(count_new) catch unreachable;
                    if (count_old <= count_new) {
                        for (count_old..count_new) |index| {
                            const value = prng.int(u8);
                            model.items[index] = value;
                            array.buffer[index] = value;
                        }
                    }
                },
                .truncate => {
                    const count_new = prng.int_inclusive(usize, model.items.len);
                    array.truncate(count_new);
                    model.resize(gpa, count_new) catch unreachable;
                },
                .clear => {
                    array.clear();
                    model.clearRetainingCapacity();
                },
                .pop => {
                    const b = model.pop();
                    const a = array.pop();
                    assert((a == null and b == null) or (a.? == b.?));
                },
                .capacity => assert(array.capacity() == model.capacity),
                .from_slice => {
                    var buffer: [capacity]u8 = undefined;
                    const count = prng.int_inclusive(usize, model.capacity - model.items.len);
                    for (0..count) |index| buffer[index] = prng.int(u8);
                    const slice = buffer[0..count];

                    array = Array.from_slice(slice) catch unreachable;
                    model.clearRetainingCapacity();
                    model.appendSliceAssumeCapacity(slice);
                },
            }
        }
    }
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
