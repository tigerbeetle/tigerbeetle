const std = @import("std");
const assert = std.debug.assert;

pub const Time = struct {
    const Self = @This();

    /// Hardware and/or software bugs can mean that the monotonic clock may regress.
    /// One example (of many): https://bugzilla.redhat.com/show_bug.cgi?id=448449
    /// We crash the process for safety if this ever happens, to protect against infinite loops.
    /// It's better to crash and come back with a valid monotonic clock than get stuck forever.
    monotonic_guard: u64 = 0,

    /// A timestamp to measure elapsed time, meaningful only on the same system, not across reboots.
    /// Always use a monotonic timestamp if the goal is to measure elapsed time.
    /// This clock is not affected by discontinuous jumps in the system time, for example if the
    /// system administrator manually changes the clock.
    pub fn monotonic(self: *Self) u64 {
        // The true monotonic clock on Linux is not in fact CLOCK_MONOTONIC:
        // CLOCK_MONOTONIC excludes elapsed time while the system is suspended (e.g. VM migration).
        // CLOCK_BOOTTIME is the same as CLOCK_MONOTONIC but includes elapsed time during a suspend.
        // For more detail and why CLOCK_MONOTONIC_RAW is even worse than CLOCK_MONOTONIC,
        // see https://github.com/ziglang/zig/pull/933#discussion_r656021295.
        var ts: std.os.timespec = undefined;
        std.os.clock_gettime(std.os.CLOCK_BOOTTIME, &ts) catch @panic("CLOCK_BOOTTIME required");
        const m = @intCast(u64, ts.tv_sec) * std.time.ns_per_s + @intCast(u64, ts.tv_nsec);
        // "Oops!...I Did It Again"
        if (m < self.monotonic_guard) @panic("a hardware/kernel bug regressed the monotonic clock");
        self.monotonic_guard = m;
        return m;
    }

    /// A timestamp to measure real (i.e. wall clock) time, meaningful across systems, and reboots.
    /// This clock is affected by discontinuous jumps in the system time.
    pub fn realtime(self: *Self) i64 {
        var ts: std.os.timespec = undefined;
        std.os.clock_gettime(std.os.CLOCK_REALTIME, &ts) catch unreachable;
        return @as(i64, ts.tv_sec) * std.time.ns_per_s + ts.tv_nsec;
    }

    pub fn tick(self: *Self) void {}
};

pub const DeterministicTime = struct {
    const Self = @This();

    /// The duration of a single tick in nanoseconds.
    resolution: u64,

    /// The number of ticks elapsed since initialization.
    ticks: u64 = 0,

    /// The instant in time chosen as the origin of this time source.
    epoch: i64 = 0,

    pub fn monotonic(self: *Self) u64 {
        return self.ticks * self.resolution;
    }

    pub fn realtime(self: *Self) i64 {
        return self.epoch + @intCast(i64, self.monotonic());
    }

    pub fn tick(self: *Self) void {
        self.ticks += 1;
    }
};
