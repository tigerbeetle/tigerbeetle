//! Micro benchmarking harness.
//!
//! Goals:
//! - relative (comparative) benchmarking,
//! - manual checks when refactoring/optimizing/upgrading compiler,
//! - no benchmark bitrot.
//!
//! Non-goals:
//! - absolute benchmarking,
//! - continuous benchmarking,
//! - automatic regression detection.
//!
//! If you run
//!     $ ./zig/zig build test
//! the benchmarks are run in "test" mode which uses small inputs, finishes quickly, and doesn't
//! print anything to stdout.
//!
//! If you run
//!     $ ./zig/zig build -Drelease test -- "benchmark: binary search"
//! the benchmark is run for real, with a large input, longer runtime, and results on stderr.
//! The `benchmark` in the test name is the secret code to unlock benchmarking code.

test "benchmark: API tutorial" { // `benchmark:` in the name is important!
    var bench: Bench = .init();
    defer bench.deinit();

    // Parameters are named, and have two default values.
    // The small value is used in tests, to prevent bitrot.
    // The large value is the canonical when running benchmark "for real".
    // You can pass custom values via env variables:
    //    $ a=92 ./zig/zig build test -- "benchmark: tutorial"
    // const a = bench.parameter("a", 1, 1_000);
    // const b = bench.parameter("b", 2, 2_000);

    const params = Bench.parameter2(.{
        .a = .{ 1, 1_000 },
        .b = .{ 1, 1_000 }
    });

    bench.start(); // Built-in timer.
    const c = params.a + params.b;
    const elapsed = bench.stop();

    // Always print a "hash" of the run:
    // - to prevent compiler from optimizing the code away,
    // - to prevent YOU from "optimizing" the code by changing semantics.
    bench.report("hash: {}", .{c});
    // Print the time, and any other metrics you find important.
    bench.report2(@src(), params, .{
        .elapsed_ns = elapsed.ns
    });

    // NB: print as little as possible, because humans read slowly.
    // It's the job of benchmark author to optimize for conciseness.

    // You can compile individual benchmark  via
    //   ./zig/zig build test:unit:build -- "benchmark: binary search"
    // and use the resulting binary with perf/hyperfine/poop.
}

const std = @import("std");
const assert = std.debug.assert;
const stdx = @import("stdx");
const Duration = stdx.Duration;
const Instant = stdx.Instant;
const TimeOS = @import("../time.zig").TimeOS;

const seed_benchmark: u64 = 42;

const mode: enum { smoke, benchmark } =
    // See build.zig for how this is ultimately determined.
    if (@import("test_options").benchmark) .benchmark else .smoke;
const allow_parameter_overrides = !@import("test_options").ci;

seed: u64,
time: TimeOS = .{},
timer: ?Instant = null,

const Bench = @This();

pub fn init() Bench {
    return .{
        // Benchmarks require a fixed seed for reproducibility; smoke mode uses a random seed.
        .seed = if (mode == .benchmark) seed_benchmark else std.testing.random_seed,
    };
}

pub fn deinit(bench: *Bench) void {
    assert(bench.timer == null);
    bench.* = undefined;
}

pub fn parameter(
    b: *const Bench,
    comptime name: []const u8,
    value_smoke: u64,
    value_benchmark: u64,
) u64 {
    _ = b;
    assert(value_smoke < value_benchmark);
    const value = parameter_fallible(name, value_smoke, value_benchmark) catch |err| switch (err) {
        error.InvalidCharacter, error.Overflow => @panic("invalid benchmark parameter value"),
    };
    return value;
}

fn parameter_fallible(
    comptime name: []const u8,
    value_smoke: u64,
    value_benchmark: u64,
) std.fmt.ParseIntError!u64 {
    assert(value_smoke < value_benchmark);
    return switch (mode) {
        .smoke => value_smoke,
        .benchmark => std.process.parseEnvVarInt(name, u64, 10) catch |err| switch (err) {
            error.EnvironmentVariableNotFound => return value_benchmark,
            else => |e| {
                assert(allow_parameter_overrides);
                return e;
            },
        },
    };
}

pub fn start(bench: *Bench) void {
    assert(bench.timer == null);
    defer assert(bench.timer != null);

    bench.timer = bench.time.time().monotonic();
}

pub fn stop(bench: *Bench) Duration {
    assert(bench.timer != null);
    defer assert(bench.timer == null);

    const instant_stop = bench.time.time().monotonic();
    const elapsed = bench.timer.?.elapsed(instant_stop);
    bench.timer = null;
    return elapsed;
}

// Sort the durations and return the third-fastest sample (discarding the two fastest outliers)
// to get a more stable estimate, assuming benchmark timings are roughly log-normal.
// E.g. see https://lemire.me/blog/2018/01/16/microbenchmarking-calls-for-idealized-conditions/
pub fn estimate(bench: *const Bench, durations: []Duration) Duration {
    assert(durations.len >= 8); // Ensure that we have enough samples to get a meaningful result.
    _ = bench;
    std.sort.block(stdx.Duration, durations, {}, stdx.Duration.sort.asc);
    return durations[2];
}

pub fn report(_: *const Bench, comptime fmt: []const u8, args: anytype) void {
    switch (mode) {
        .smoke => {},
        .benchmark => std.debug.print(fmt ++ "\n", args),
    }
}

fn ParameterType(comptime kv_pairs: anytype) type {
    const input_fields = std.meta.fields(@TypeOf(kv_pairs));
    var struct_fields: [input_fields.len]std.builtin.Type.StructField = undefined;

    for (input_fields, &struct_fields) |input_field, *output_field| {
        assert(@typeInfo(input_field.type) == .@"struct");
        const input_struct = @typeInfo(input_field.type).@"struct";
        // each field provides a 'smoke' and 'benchmark' value of the same type
        assert(input_struct.is_tuple);
        assert(input_struct.fields.len == 2);
        assert(input_struct.fields[0].type == input_struct.fields[1].type);
        assert(input_struct.fields[0].default_value_ptr != null);
        assert(input_struct.fields[1].default_value_ptr != null);
        const value_ptr = switch(mode) {
            .smoke =>  input_struct.fields[0].default_value_ptr,
            .benchmark => input_struct.fields[1].default_value_ptr,
        };
        output_field.* = .{
            .name = input_field.name,
            .type = input_struct.fields[0].type,
            .default_value_ptr = value_ptr,
            .is_comptime = false,
            .alignment = @alignOf(input_struct.fields[0].type),
        };
    }

    return @Type(.{ .@"struct" = .{
        .layout = .auto,
        .fields = &struct_fields,
        .decls = &.{},
        .is_tuple = false,
        } });
}

pub fn parameter2(
    comptime kv_pairs: anytype
) ParameterType(kv_pairs) {
    return .{};
}

pub fn report2(_: *const Bench,
               comptime src: std.builtin.SourceLocation,
               comptime parameters: anytype,
               measurements: anytype,
) void {
    assert(std.meta.fields(@TypeOf(measurements)).len > 0);
    switch (mode) {
        .smoke => {},
        .benchmark => {
            var output_buffer: [4096]u8 = undefined;
            var output = std.io.fixedBufferStream(&output_buffer);
            const writer = output.writer();

            writer.print("{s} // ", .{src.fn_name}) catch unreachable;
            inline for (std.meta.fields(@TypeOf(parameters))) |field| {
                writer.print("{s}={any} ", .{
                    field.name,
                    @field(parameters, field.name),
                }) catch unreachable;
            }
            writer.writeAll("// ") catch unreachable;
            inline for (std.meta.fields(@TypeOf(measurements))) |field| {
                const field_type = @typeInfo(field.type);
                comptime assert(field_type == .int or field_type == .float);
                writer.print("{s}={any} ", .{
                    field.name,
                    @field(measurements, field.name),
                }) catch unreachable;
            }
            writer.writeByte('\n') catch unreachable;

            std.debug.print("{s}", .{output.getWritten()});
        },
    }
}
