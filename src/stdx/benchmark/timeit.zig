const std = @import("std");
const Time = @import("./time.zig");
const Instant = @import("../time_units.zig").Instant;

/// Utility function for quick, cross-platform, ad-hoc timing.
/// For in-depth profiling of performance metrics, use `PerfCounters`.
///
/// `timeit` wraps a time source and handles the boilerplate of
/// printing to stderr and formatting times in some (unspecified) readable way.
pub fn timeit() TimeIt {
    var time: Time = .{};
    return .{ .time = time, .timer = time.benchmark_monotonic() };
}

test "timeit usage" {
    var timer = timeit();
    defer timer.print_if_longer_than_ms(1000, "timeit test");

    const scale = 1_000_000;
    var checksum: u128 = 0;
    for (1..scale) |i| {
        checksum += i * i;
    }
}

const TimeIt = struct {
    time: Time = .{},
    timer: Instant,

    /// Prints elapsed time to stderr and resets the internal timer.
    pub fn print(self: *TimeIt, comptime label: []const u8) void {
        const label_alignment = comptime " " ** (1 + (12 -| label.len));

        const now = self.time.benchmark_monotonic();
        const elapsed = self.timer.elapsed(now);
        self.timer = now;

        std.debug.print(
            label ++ ":" ++ label_alignment ++ "{}\n",
            .{std.fmt.fmtDuration(elapsed.ns)},
        );
    }

    pub fn print_if_longer_than_ms(
        self: *TimeIt,
        threshold_ms: u64,
        comptime label: []const u8,
    ) void {
        self.if_longer_than(label, threshold_ms, false);
    }

    pub fn backtrace_if_longer_than_ms(
        self: *TimeIt,
        threshold_ms: u64,
        comptime label: []const u8,
    ) void {
        self.if_longer_than(label, threshold_ms, true);
    }

    fn if_longer_than(
        self: *TimeIt,
        comptime label: []const u8,
        threshold_ms: u64,
        backtrace: bool,
    ) void {
        const now = self.time.benchmark_monotonic();
        const elapsed = self.timer.elapsed(now);
        self.timer = now;

        if (elapsed.ns > threshold_ms * std.time.ns_per_ms) {
            std.debug.print(label ++ ": {}\n", .{std.fmt.fmtDuration(elapsed.ns)});
            if (backtrace) std.debug.dumpCurrentStackTrace(null);
        }
    }
};
