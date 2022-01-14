const std = @import("std");
const assert = std.debug.assert;

pub inline fn div_ceil(numerator: anytype, denominator: anytype) @TypeOf(numerator, denominator) {
    comptime {
        const T = @TypeOf(numerator, denominator);
        switch (@typeInfo(T)) {
            .Int => |int| assert(int.signedness == .unsigned),
            .ComptimeInt => {
                assert(numerator >= 0);
                assert(denominator > 0);
            },
            else => @compileError("invalid type given to utils.div_ceil"),
        }
    }

    // Only check the denominator at runtime when the it is *not* a comptime_int.
    if (@as(std.builtin.TypeId, @typeInfo(@TypeOf(denominator))) == .Int) {
        assert(denominator > 0);
    }

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
