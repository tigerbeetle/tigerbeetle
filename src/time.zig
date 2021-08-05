const std = @import("std");
const assert = std.debug.assert;
const config = @import("./config.zig");

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

pub const OffsetType = enum { linear, periodic, step, non_ideal };
pub const DeterministicTime = struct {
    const Self = @This();

    /// The duration of a single tick in nanoseconds.
    resolution: u64,

    offset_type: OffsetType,

    /// Co-efficients to scale the offset according to the `offset_type`.
    /// Linear offset is described as A * x + B: A is the drift per tick and B the initial offset.
    /// Periodic is described as A * sin(x * pi / B): A controls the amplitude and B the period in
    /// terms of ticks.
    /// Step function represents a discontinuous jump in the wall-clock time. B is the period in
    /// which the jumps occur. A is the amplitude of the step.
    /// Non-ideal is similar to periodic except the phase is adjusted using a random number taken
    /// from a normal distribution with mean=0, stddev=10. Finally, a random offset (up to 
    /// offset_coefficientC) is added to the result.
    offset_coefficient_A: i64,
    offset_coefficient_B: i64,
    offset_coefficient_C: u32 = 0,

    prng: std.rand.DefaultPrng = std.rand.DefaultPrng.init(0),

    /// The number of ticks elapsed since initialization.
    ticks: u64 = 0,

    /// The instant in time chosen as the origin of this time source.
    epoch: i64 = 0,

    pub fn monotonic(self: *Self) u64 {
        return self.ticks * self.resolution;
    }

    pub fn realtime(self: *Self) i64 {
        return self.epoch + @intCast(i64, self.monotonic()) - self.offset(self.ticks);
    }

    pub fn offset(self: *Self, ticks: u64) i64 {
        switch (self.offset_type) {
            .linear => {
                const drift_per_tick = self.offset_coefficient_A;
                return @intCast(i64, ticks) * drift_per_tick + @intCast(
                    i64,
                    self.offset_coefficient_B,
                );
            },
            .periodic => {
                const unscaled = std.math.sin(@intToFloat(f64, ticks) * 2 * std.math.pi /
                    @intToFloat(f64, self.offset_coefficient_B));
                const scaled = @intToFloat(f64, self.offset_coefficient_A) * unscaled;
                return @floatToInt(i64, std.math.floor(scaled));
            },
            .step => {
                return if (ticks > self.offset_coefficient_B) self.offset_coefficient_A else 0;
            },
            .non_ideal => {
                const phase: f64 = @intToFloat(f64, ticks) * 2 * std.math.pi /
                    (@intToFloat(f64, self.offset_coefficient_B) + self.prng.random.floatNorm(f64) * 10);
                const unscaled = std.math.sin(phase);
                const scaled = @intToFloat(f64, self.offset_coefficient_A) * unscaled;
                return @floatToInt(i64, std.math.floor(scaled)) +
                    self.prng.random.intRangeAtMost(
                    i64,
                    -@intCast(i64, self.offset_coefficient_C),
                    self.offset_coefficient_C,
                );
            },
        }
    }

    pub fn tick(self: *Self) void {
        self.ticks += 1;
    }
};
