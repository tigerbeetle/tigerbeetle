const std = @import("std");
const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
const Instant = stdx.Instant;

pub const OffsetType = enum {
    linear,
    periodic,
    step,
    non_ideal,
};

pub const Time = struct {
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
    /// offset_coefficient_C) is added to the result.
    offset_coefficient_A: i64,
    offset_coefficient_B: i64,
    offset_coefficient_C: u32 = 0,

    prng: stdx.PRNG = stdx.PRNG.from_seed(0),

    /// The number of ticks elapsed since initialization.
    ticks: u64 = 0,

    /// The instant in time chosen as the origin of this time source.
    epoch: i64 = 0,

    pub fn init_simple() Time {
        return .{
            .resolution = constants.tick_ms * std.time.ns_per_ms,
            .offset_type = .linear,
            .offset_coefficient_A = 0,
            .offset_coefficient_B = 0,
        };
    }

    pub fn monotonic(self: *Time) u64 {
        return self.ticks * self.resolution;
    }

    pub fn monotonic_instant(self: *Time) Instant {
        return .{ .ns = self.monotonic() };
    }

    pub fn realtime(self: *Time) i64 {
        return self.epoch + @as(i64, @intCast(self.monotonic())) - self.offset(self.ticks);
    }

    pub fn offset(self: *Time, ticks: u64) i64 {
        switch (self.offset_type) {
            .linear => {
                const drift_per_tick = self.offset_coefficient_A;
                return @as(i64, @intCast(ticks)) * drift_per_tick + @as(
                    i64,
                    @intCast(self.offset_coefficient_B),
                );
            },
            .periodic => {
                const unscaled = std.math.sin(@as(f64, @floatFromInt(ticks)) * 2 * std.math.pi /
                    @as(f64, @floatFromInt(self.offset_coefficient_B)));
                const scaled = @as(f64, @floatFromInt(self.offset_coefficient_A)) * unscaled;
                return @as(i64, @intFromFloat(std.math.floor(scaled)));
            },
            .step => {
                return if (ticks > self.offset_coefficient_B) self.offset_coefficient_A else 0;
            },
            .non_ideal => {
                const phase: f64 = @as(f64, @floatFromInt(ticks)) * 2 * std.math.pi /
                    (@as(f64, @floatFromInt(self.offset_coefficient_B)) +
                    std.Random.init(&self.prng, stdx.PRNG.fill).floatNorm(f64) * 10);
                const unscaled = std.math.sin(phase);
                const scaled = @as(f64, @floatFromInt(self.offset_coefficient_A)) * unscaled;
                const offest: i64 = -@as(i64, @intCast(self.offset_coefficient_C)) +
                    @as(i64, @intCast(self.prng.int_inclusive(u64, 2 * self.offset_coefficient_C)));
                return @as(i64, @intFromFloat(std.math.floor(scaled))) + offest;
            },
        }
    }

    pub fn tick(self: *Time) void {
        self.ticks += 1;
    }
};
