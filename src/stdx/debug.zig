const std = @import("std");

/// Utility function for ad-hoc profiling.
///
/// A thin wrapper around `std.time.Timer` which handles the boilerplate of
/// printing to stderr and formatting times in some (unspecified) readable way.
pub fn timeit() TimeIt {
    return TimeIt{ .inner = std.time.Timer.start() catch unreachable };
}

const TimeIt = struct {
    inner: std.time.Timer,

    /// Prints elapsed time to stderr and resets the internal timer.
    pub fn print(self: *TimeIt, comptime label: []const u8) void {
        const label_alignment = comptime " " ** (1 + (12 -| label.len));

        const elapsed_ns = self.inner.lap();
        std.debug.print(
            label ++ ":" ++ label_alignment ++ "{}\n",
            .{std.fmt.fmtDuration(elapsed_ns)},
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
        const elapsed_ns = self.inner.lap();
        if (elapsed_ns > threshold_ms * std.time.ns_per_ms) {
            std.debug.print(label ++ ": {}\n", .{std.fmt.fmtDuration(elapsed_ns)});
            if (backtrace) std.debug.dumpCurrentStackTrace(null);
        }
    }
};

/// Utility for print-if debugging, a-la Rust's dbg! macro.
///
/// dbg prints the value with the prefix, while also returning the value, which makes it convenient
/// to drop it in the middle of a complex expression.
pub fn dbg(prefix: []const u8, value: anytype) @TypeOf(value) {
    std.debug.print("{s} = {any}\n", .{
        prefix,
        std.json.fmt(value, .{ .whitespace = .indent_2 }),
    });
    return value;
}
