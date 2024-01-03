//! Utils functions for writing fuzzers.

const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const log = std.log.scoped(.fuzz);

// Use our own allocator in the global scope instead of testing.allocator
// as the latter now @compileError()'s if referenced outside a `test` block.
var gpa = std.heap.GeneralPurposeAllocator(.{}){};
pub const allocator = gpa.allocator();

/// Returns an integer of type `T` with an exponential distribution of rate `avg`.
/// Note: If you specify a very high rate then `std.math.maxInt(T)` may be over-represented.
pub fn random_int_exponential(random: std.rand.Random, comptime T: type, avg: T) T {
    comptime {
        const info = @typeInfo(T);
        assert(info == .Int);
        assert(info.Int.signedness == .unsigned);
    }
    const exp = random.floatExp(f64) * @as(f64, @floatFromInt(avg));
    return std.math.lossyCast(T, exp);
}

pub fn Distribution(comptime Enum: type) type {
    return std.enums.EnumFieldStruct(Enum, f64, null);
}

/// Return a distribution for use with `random_enum`.
pub fn random_enum_distribution(
    random: std.rand.Random,
    comptime Enum: type,
) Distribution(Enum) {
    const fields = @typeInfo(Distribution(Enum)).Struct.fields;
    var distribution: Distribution(Enum) = undefined;
    var total: f64 = 0;
    inline for (fields) |field| {
        const p = @as(f64, @floatFromInt(random.uintLessThan(u8, 10)));
        @field(distribution, field.name) = p;
        total += p;
    }
    // Ensure that at least one field has non-zero probability.
    if (total == 0) {
        @field(distribution, fields[0].name) = 1;
    }
    return distribution;
}

/// Generate a random `Enum`, given a distribution over the fields of the enum.
pub fn random_enum(
    random: std.rand.Random,
    comptime Enum: type,
    distribution: Distribution(Enum),
) Enum {
    const fields = @typeInfo(Enum).Enum.fields;
    var total: f64 = 0;
    inline for (fields) |field| {
        total += @field(distribution, field.name);
    }
    assert(total > 0);
    var choice = random.float(f64) * total;
    inline for (fields) |field| {
        choice -= @field(distribution, field.name);
        if (choice < 0) return @as(Enum, @enumFromInt(field.value));
    }
    unreachable;
}

pub const FuzzArgs = struct {
    seed: u64,
    events_max: ?usize,
};
