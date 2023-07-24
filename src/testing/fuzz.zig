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
    const exp = random.floatExp(f64) * @intToFloat(f64, avg);
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
        const p = @intToFloat(f64, random.uintLessThan(u8, 10));
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
        if (choice < 0) return @intToEnum(Enum, field.value);
    }
    unreachable;
}

pub const FuzzArgs = struct {
    seed: u64,
    events_max: ?usize,
};

/// Parse common command-line arguments to fuzzers:
///
///    [--seed u64]
///        Sets the seed used for the random number generator.
///    [--events-max usize]
///        Override the fuzzer's default maximum number of generated events.
pub fn parse_fuzz_args(args_allocator: mem.Allocator) !FuzzArgs {
    var seed: ?u64 = null;
    var events_max: ?usize = null;

    var args = try std.process.argsWithAllocator(args_allocator);
    defer args.deinit();

    // Discard executable name.
    _ = args.next().?;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--seed")) {
            const seed_string = args.next() orelse
                std.debug.panic("Expected an argument to --seed", .{});

            if (seed != null) {
                std.debug.panic("Received more than one \"--seed\"", .{});
            }
            seed = std.fmt.parseInt(u64, seed_string, 10) catch |err|
                std.debug.panic(
                "Could not parse \"{}\" as an integer seed: {}",
                .{ std.zig.fmtEscapes(seed_string), err },
            );
        } else if (std.mem.eql(u8, arg, "--events-max")) {
            const events_string = args.next() orelse
                std.debug.panic("Expected an argument to --events-max", .{});

            if (events_max != null) {
                std.debug.panic("Received more than one \"--events-max\"", .{});
            }
            events_max = std.fmt.parseInt(usize, events_string, 10) catch |err|
                std.debug.panic(
                "Could not parse \"{}\" as an integer events-max: {}",
                .{ std.zig.fmtEscapes(events_string), err },
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
        .events_max = events_max,
    };
}
