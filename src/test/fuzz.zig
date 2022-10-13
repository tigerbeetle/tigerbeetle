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

/// Return a list of probabilities for use with `random_enum_biased`.
pub fn random_biases(
    random: std.rand.Random,
    comptime Enum: type,
) [@typeInfo(Enum).Enum.fields.len]f64 {
    var biases: [@typeInfo(Enum).Enum.fields.len]f64 = undefined;
    var total: f64 = 0;
    while (true) {
        for (biases) |*bias| bias.* = @intToFloat(f64, random.uintLessThan(u8, 10));
        total = 0;
        for (biases) |bias| total += bias;
        if (total != 0) break;
    }
    for (biases) |*bias| bias.* = bias.* / total;
    return biases;
}

/// Generate a random `Enum`, given a list of probabilities for each field.
pub fn random_enum_biased(
    random: std.rand.Random,
    comptime Enum: type,
    biases: [@typeInfo(Enum).Enum.fields.len]f64,
) Enum {
    var choice = random.float(f64);
    inline for (@typeInfo(Enum).Enum.fields) |enum_field, i| {
        choice -= biases[i];
        if (choice < 0) return @intToEnum(Enum, enum_field.value);
    }
    unreachable;
}
