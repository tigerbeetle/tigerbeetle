//! Utils functions for writing fuzzers.

const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const log = std.log.scoped(.fuzz);

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

pub const FuzzArgs = struct {
    seed: u64,
};

/// Parse common command-line arguments to fuzzers:
///
///    [--seed u64]
///        Sets the seed used for the random number generator.
pub fn parse_fuzz_args(allocator: mem.Allocator) !FuzzArgs {
    var seed: ?u64 = null;

    var args = std.process.args();

    // Discard executable name.
    allocator.free(try args.next(allocator).?);

    while (args.next(allocator)) |arg_or_err| {
        const arg = try arg_or_err;
        defer allocator.free(arg);

        if (std.mem.eql(u8, arg, "--seed")) {
            const seed_string_or_err = args.next(allocator) orelse
                std.debug.panic("Expected an argument to --seed", .{});
            const seed_string = try seed_string_or_err;
            defer allocator.free(seed_string);

            if (seed != null) {
                std.debug.panic("Received more than one \"--seed\"", .{});
            }
            seed = std.fmt.parseInt(u64, seed_string, 10) catch |err|
                std.debug.panic(
                "Could not parse \"{}\" as an integer seed: {}",
                .{ std.zig.fmtEscapes(seed_string), err },
            );
        } else {
            // When run with `--test-cmd`,
            // `zig run` also passes the location of the zig binary as an extra arg.
            // I don't know how to turn this off, so we just skip such args.
            if (!std.mem.endsWith(u8, arg, "zig")) {
                std.debug.panic("Unrecognized argument: \"{}\"", .{std.zig.fmtEscapes(arg)});
            }
        }
    }

    if (seed == null) {
        // If no seed was given, use a random seed instead.
        var buffer: [@sizeOf(u64)]u8 = undefined;
        try std.os.getrandom(&buffer);
        seed = @bitCast(u64, buffer);
    }

    log.info("Fuzz seed = {}", .{seed.?});

    return FuzzArgs{
        .seed = seed.?,
    };
}
