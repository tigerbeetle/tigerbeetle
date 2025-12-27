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
    const a = bench.parameter("a", 1, 1_000);
    const b = bench.parameter("b", 2, 2_000);

    bench.start(); // Built-in timer.
    const c = a + b;
    const elapsed = bench.stop();

    // Always print a "hash" of the run:
    // - to prevent compiler from optimizing the code away,
    // - to prevent YOU from "optimizing" the code by changing semantics.
    bench.report("hash: {}", .{c});
    // Print the time, and any other metrics you find important.
    bench.report("elapsed: {}", .{elapsed});

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

seed: u64,
time: TimeOS = .{},
instant_start: ?Instant = null,

const Bench = @This();

pub fn init() Bench {
    return .{
        // Benchmarks require a fixed seed for reproducibility; smoke mode uses a random seed.
        .seed = if (mode == .benchmark) seed_benchmark else std.testing.random_seed,
    };
}

pub fn deinit(bench: *Bench) void {
    assert(bench.instant_start == null);
    bench.* = undefined;
}

pub fn parameter(
    b: *const Bench,
    comptime name: []const u8,
    value_smoke: u64,
    value_benchmark: u64,
) u64 {
    assert(value_smoke < value_benchmark);
    const value = parameter_fallible(name, value_smoke, value_benchmark) catch |err| switch (err) {
        error.InvalidCharacter, error.Overflow => @panic("invalid benchmark parameter value"),
    };
    b.report("{s}={}", .{ name, value });
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
            else => |e| return e,
        },
    };
}

pub fn start(bench: *Bench) void {
    assert(bench.instant_start == null);
    defer assert(bench.instant_start != null);

    bench.instant_start = bench.time.time().monotonic();
}

pub fn stop(bench: *Bench) Duration {
    assert(bench.instant_start != null);
    defer assert(bench.instant_start == null);

    const instant_stop = bench.time.time().monotonic();
    const elapsed = instant_stop.duration_since(bench.instant_start.?);
    bench.instant_start = null;
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
