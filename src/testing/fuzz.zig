//! Utils functions for writing fuzzers.

const std = @import("std");
const stdx = @import("../stdx.zig");
const assert = std.debug.assert;
const PRNG = stdx.PRNG;

// Use our own allocator in the global scope instead of testing.allocator
// as the latter now @compileError()'s if referenced outside a `test` block.
var gpa = std.heap.GeneralPurposeAllocator(.{}){};
pub const allocator = gpa.allocator();

/// Returns an integer of type `T` with an exponential distribution of rate `avg`.
/// Note: If you specify a very high rate then `std.math.maxInt(T)` may be over-represented.
pub fn random_int_exponential(prng: *stdx.PRNG, comptime T: type, avg: T) T {
    comptime {
        const info = @typeInfo(T);
        assert(info == .Int);
        assert(info.Int.signedness == .unsigned);
    }
    // Note: we use floats and rely on std implementaion. Ideally, we should do neither, but I
    // wasn't able to find a quick way to generate geometrically distributed integers using only
    // integer arithmetic.
    const random = std.Random.init(prng, stdx.PRNG.fill);
    const exp = random.floatExp(f64) * @as(f64, @floatFromInt(avg));
    return std.math.lossyCast(T, exp);
}

/// Return a distribution for use with `random_enum`.
///
/// This is swarm testing: sm   ome variants are disabled completely,
/// and the rest have wildly different probabilites.
pub fn random_enum_weights(
    prng: *stdx.PRNG,
    comptime Enum: type,
) stdx.PRNG.EnumWeightsType(Enum) {
    const fields = std.meta.fieldNames(Enum);

    var combination = stdx.PRNG.Combination.init(.{
        .total_count = fields.len,
        .sample_count = prng.range_inclusive(u32, 1, fields.len),
    });
    defer assert(combination.done());

    var weights: PRNG.EnumWeightsType(Enum) = undefined;
    inline for (fields) |field| {
        @field(weights, field) = if (combination.take(prng))
            prng.int_inclusive(u64, 100)
        else
            0;
    }

    return weights;
}

pub const FuzzArgs = struct {
    seed: u64,
    events_max: ?usize,
};

pub fn parse_seed(bytes: []const u8) u64 {
    if (bytes.len == 40) {
        // Normally, a seed is specified as a base-10 integer. However, as a special case, we allow
        // using a Git hash (a hex string 40 character long). This is used by our CI, which passes
        // current commit hash as a seed --- that way, we run simulator on CI, we run it with
        // different, "random" seeds, but the failures remain reproducible just from the commit
        // hash!
        const commit_hash = std.fmt.parseUnsigned(u160, bytes, 16) catch |err| switch (err) {
            error.Overflow => unreachable,
            error.InvalidCharacter => @panic("commit hash seed contains an invalid character"),
        };
        return @truncate(commit_hash);
    }

    return std.fmt.parseUnsigned(u64, bytes, 10) catch |err| switch (err) {
        error.Overflow => @panic("seed exceeds a 64-bit unsigned integer"),
        error.InvalidCharacter => @panic("seed contains an invalid character"),
    };
}
