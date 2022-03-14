const std = @import("std");
const assert = std.debug.assert;

// TODO copy_left(), copy_right()
// Inline wrappers for mem.copy() and mem.copyBackwards() that assert correctness IFF overlap.
// It's otherwise too easy to silently use mem.copy() or mem.copyBackwards() incorrectly.
// Does not assert that there is overlap, only what the direction of the copy should be if there is.
//
// TODO copy()
// Asserts that there is no overlap, or else copy_left() or copy_right() should have been used.
//
// TODO copy_exact(), copy_exact_left(), copy_exact_right()
// Even safer than the above, asserts that the source and target slices have the exact same length.

pub inline fn div_ceil(numerator: anytype, denominator: anytype) @TypeOf(numerator, denominator) {
    comptime {
        switch (@typeInfo(@TypeOf(numerator))) {
            .Int => |int| assert(int.signedness == .unsigned),
            .ComptimeInt => assert(numerator >= 0),
            else => @compileError("div_ceil: invalid numerator type"),
        }

        switch (@typeInfo(@TypeOf(denominator))) {
            .Int => |int| assert(int.signedness == .unsigned),
            .ComptimeInt => assert(denominator > 0),
            else => @compileError("div_ceil: invalid denominator type"),
        }
    }

    assert(denominator > 0);

    if (numerator == 0) return 0;
    return @divFloor(numerator - 1, denominator) + 1;
}

test "div_ceil" {
    // Comptime ints.
    try std.testing.expectEqual(div_ceil(0, 8), 0);
    try std.testing.expectEqual(div_ceil(1, 8), 1);
    try std.testing.expectEqual(div_ceil(7, 8), 1);
    try std.testing.expectEqual(div_ceil(8, 8), 1);
    try std.testing.expectEqual(div_ceil(9, 8), 2);

    // Unsized ints
    const max = std.math.maxInt(u64);
    try std.testing.expectEqual(div_ceil(@as(u64, 0), 8), 0);
    try std.testing.expectEqual(div_ceil(@as(u64, 1), 8), 1);
    try std.testing.expectEqual(div_ceil(@as(u64, max), 2), max / 2 + 1);
    try std.testing.expectEqual(div_ceil(@as(u64, max) - 1, 2), max / 2);
    try std.testing.expectEqual(div_ceil(@as(u64, max) - 2, 2), max / 2);
}
