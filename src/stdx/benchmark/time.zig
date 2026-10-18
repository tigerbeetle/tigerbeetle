//! In microbenchmarks, we often measure both time and other performance counters
//! such as how many cycles were used, how many branch misses were incurred, and more.
//! These only count cycles in the current process, and not, for exampole, sleep time.
//! The closest matching clock implementation semantics are provided by CLOCK_MONOTONIC,
//! which is what we use here. To distinguish towards stdx.Time.monotonic(),
//! we call this `benchmark_monotonic()`.
const std = @import("std");
const builtin = @import("builtin");

const stdx = @import("../stdx.zig");

const os = std.os;
const posix = std.posix;
const system = posix.system;
const assert = std.debug.assert;
const is_darwin = builtin.target.os.tag.isDarwin();
const is_windows = builtin.target.os.tag == .windows;
const is_linux = builtin.target.os.tag == .linux;
const Instant = stdx.Instant;

const BenchmarkTime = @This();

// BenchmarkTime is used to test algorithm runtime and is not critical for safety.
// We still guard against non-monotonicity bugs in OS time sources
// to fail fast and keep our sanity when debugging test outputs.
monotonic_guard: u64 = 0,

// TODO use different name to differentiate to vsr time
pub fn benchmark_monotonic(self: *BenchmarkTime) Instant {
    // Since we do not currently optimize for macOS and windows,
    // (especially the I/O interface), one could also argue for
    // failing early here and only allowing measurements on linux.
    const monotonic_timestamp = blk: {
        if (is_windows) break :blk benchmark_monotonic_windows();
        if (is_darwin) break :blk benchmark_monotonic_darwin();
        if (is_linux) break :blk benchmark_monotonic_linux();
        @compileError("unsupported OS");
    };

    // "Oops!...I Did It Again"
    if (monotonic_timestamp < self.monotonic_guard) {
        @panic("a hardware/kernel bug regressed the monotonic clock");
    }
    self.monotonic_guard = monotonic_timestamp;
    return .{ .ns = monotonic_timestamp };
}

fn benchmark_monotonic_windows() u64 {
    comptime assert(is_windows);
    return stdx.os.windows.query_performance_counter_ns();
}

fn benchmark_monotonic_darwin() u64 {
    comptime assert(is_darwin);
    return stdx.os.darwin.mach_absolute_time_ns();
}

fn benchmark_monotonic_linux() u64 {
    comptime assert(is_linux);
    const ts = stdx.os.clock_gettime(.monotonoic);
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}
