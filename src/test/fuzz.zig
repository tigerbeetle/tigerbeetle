//! Utils functions for writing fuzzers.

const std = @import("std");
const assert = std.debug.assert;

/// Returns an integer of type `T` with an exponential distribution of rate `avg`.
/// Note: If you specify a very high rate then `std.math.maxInt(T)` may be over-represented.
pub fn random_int_exponential(random: std.rand.Random, comptime T: type, avg: T) T {
    comptime {
        const info = @typeInfo(T);
        assert(info == .Int);
        assert(info.Int.signedness == .unsigned);
    }
    const exp = random.floatExp(f64) * @intToFloat(f64, avg);
    return std.math.lossyCast(T, exp);
}

/// Return a distribution for use with `random_enum`.
pub fn random_enum_distribution(
    random: std.rand.Random,
    comptime Enum: type,
) [@typeInfo(Enum).Enum.fields.len]f64 {
    var distribution: [@typeInfo(Enum).Enum.fields.len]f64 = undefined;
    var total: f64 = 0;
    while (true) {
        for (distribution) |*p| p.* = @intToFloat(f64, random.uintLessThan(u8, 10));
        total = 0;
        for (distribution) |p| total += p;
        if (total != 0) break;
    }
    for (distribution) |*p| p.* = p.* / total;
    return distribution;
}

/// Generate a random `Enum`, given a distribution over the fields of the enum.
pub fn random_enum(
    random: std.rand.Random,
    comptime Enum: type,
    distribution: [@typeInfo(Enum).Enum.fields.len]f64,
) Enum {
    var choice = random.float(f64);
    inline for (@typeInfo(Enum).Enum.fields) |enum_field, i| {
        choice -= distribution[i];
        if (choice < 0) return @intToEnum(Enum, enum_field.value);
    }
    unreachable;
}
