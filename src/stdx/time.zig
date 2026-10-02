const std = @import("std");
const builtin = @import("builtin");

const stdx = @import("./stdx.zig");

const os = std.os;
const posix = std.posix;
const system = posix.system;
const assert = std.debug.assert;
const is_darwin = builtin.target.os.tag.isDarwin();
const is_windows = builtin.target.os.tag == .windows;
const is_linux = builtin.target.os.tag == .linux;
const Instant = stdx.Instant;
const InstantUnix = stdx.InstantUnix;

pub const Time = struct {
    context: *anyopaque,
    vtable: *const VTable,

    const VTable = struct {
        monotonic: *const fn (*anyopaque) Instant,
        realtime: *const fn (*anyopaque) InstantUnix,
    };

    /// A timestamp to measure elapsed time, meaningful only on the same system, not across reboots.
    /// Always use a monotonic timestamp if the goal is to measure elapsed time.
    /// This clock is not affected by discontinuous jumps in the system time, for example if the
    /// system administrator manually changes the clock.
    pub fn monotonic(self: Time) Instant {
        return self.vtable.monotonic(self.context);
    }

    /// A timestamp to measure real (i.e. wall clock) time, meaningful across systems, and reboots.
    /// This clock is affected by discontinuous jumps in the system time.
    pub fn realtime(self: Time) InstantUnix {
        return self.vtable.realtime(self.context);
    }
};

/// Real Time backed by the operating system.
pub const TimeOS = struct {
    /// Hardware and/or software bugs can mean that the monotonic clock may regress.
    /// One example (of many): https://bugzilla.redhat.com/show_bug.cgi?id=448449
    /// We crash the process for safety if this ever happens, to protect against infinite loops.
    /// It's better to crash and come back with a valid monotonic clock than get stuck forever.
    monotonic_guard: u64 = 0,

    pub fn interface(self: *TimeOS) Time {
        return .{
            .context = self,
            .vtable = &.{
                .monotonic = vtable_monotonic,
                .realtime = vtable_realtime,
            },
        };
    }

    fn vtable_monotonic(context: *anyopaque) Instant {
        const self: *TimeOS = @ptrCast(@alignCast(context));
        return self.monotonic();
    }

    pub fn monotonic(self: *TimeOS) Instant {
        const m = blk: {
            if (is_windows) break :blk monotonic_windows();
            if (is_darwin) break :blk monotonic_darwin();
            if (is_linux) break :blk monotonic_linux();
            @compileError("unsupported OS");
        };

        // "Oops!...I Did It Again"
        if (m < self.monotonic_guard) @panic("a hardware/kernel bug regressed the monotonic clock");
        self.monotonic_guard = m;
        return .{ .ns = m };
    }

    fn monotonic_windows() u64 {
        assert(is_windows);
        // Uses QueryPerformanceCounter() on windows due to it being the highest precision timer
        // available while also accounting for time spent suspended by default:
        //
        // https://docs.microsoft.com/en-us/windows/win32/api/realtimeapiset/nf-realtimeapiset-queryunbiasedinterrupttime#remarks

        // QPF need not be globally cached either as it ends up being a load from read-only memory
        // mapped to all processed by the kernel called KUSER_SHARED_DATA (See "QpcFrequency")
        //
        // https://docs.microsoft.com/en-us/windows-hardware/drivers/ddi/ntddk/ns-ntddk-kuser_shared_data
        // https://www.geoffchappell.com/studies/windows/km/ntoskrnl/inc/api/ntexapi_x/kuser_shared_data/index.htm
        const qpc = stdx.windows.QueryPerformanceCounter();
        const qpf = stdx.windows.QueryPerformanceFrequency();

        // 10Mhz (1 qpc tick every 100ns) is a common QPF on modern systems.
        // We can optimize towards this by converting to ns via a single multiply.
        //
        // https://github.com/microsoft/STL/blob/785143a0c73f030238ef618890fd4d6ae2b3a3a0/stl/inc/chrono#L694-L701
        const common_qpf = 10_000_000;
        if (qpf == common_qpf) return qpc * (std.time.ns_per_s / common_qpf);

        // Convert qpc to nanos using fixed point to avoid expensive extra divs and
        // overflow.
        const scale = (std.time.ns_per_s << 32) / qpf;
        return @as(u64, @truncate((@as(u96, qpc) * scale) >> 32));
    }

    fn monotonic_darwin() u64 {
        assert(is_darwin);
        // Uses mach_continuous_time() instead of mach_absolute_time() as it counts while suspended.
        //
        // https://developer.apple.com/documentation/kernel/1646199-mach_continuous_time
        // https://opensource.apple.com/source/Libc/Libc-1158.1.2/gen/clock_gettime.c.auto.html
        const darwin = struct {
            const mach_timebase_info_t = system.mach_timebase_info_data;
            extern "c" fn mach_timebase_info(info: *mach_timebase_info_t) system.kern_return_t;
            extern "c" fn mach_continuous_time() u64;
        };

        // mach_timebase_info() called through libc already does global caching for us
        //
        // https://opensource.apple.com/source/xnu/xnu-7195.81.3/libsyscall/wrappers/mach_timebase_info.c.auto.html
        var info: darwin.mach_timebase_info_t = undefined;
        if (darwin.mach_timebase_info(&info) != 0) @panic("mach_timebase_info() failed");

        const now = darwin.mach_continuous_time();
        return (now * info.numer) / info.denom;
    }

    fn monotonic_linux() u64 {
        assert(is_linux);
        // The true monotonic clock on Linux is not in fact CLOCK_MONOTONIC:
        //
        // CLOCK_MONOTONIC excludes elapsed time while the system is suspended (e.g. VM migration).
        //
        // CLOCK_BOOTTIME is the same as CLOCK_MONOTONIC but includes elapsed time during a suspend.
        //
        // For more detail and why CLOCK_MONOTONIC_RAW is even worse than CLOCK_MONOTONIC, see
        // https://github.com/ziglang/zig/pull/933#discussion_r656021295.
        var ts: posix.timespec = undefined;
        const rc = std.os.linux.clock_gettime(posix.CLOCK.BOOTTIME, &ts);
        if (std.os.linux.errno(rc) != .SUCCESS) @panic("CLOCK_BOOTTIME required");
        return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
    }

    fn vtable_realtime(context: *anyopaque) InstantUnix {
        const self: *TimeOS = @ptrCast(@alignCast(context));
        return self.realtime();
    }

    pub fn realtime(_: *TimeOS) InstantUnix {
        if (is_windows) return .{ .ns = @intCast(realtime_windows()) };
        // macos has supported clock_gettime() since 10.12:
        // https://opensource.apple.com/source/Libc/Libc-1158.1.2/gen/clock_gettime.3.auto.html
        if (is_darwin or is_linux) return .{ .ns = @intCast(realtime_unix()) };
        @compileError("unsupported OS");
    }

    fn realtime_windows() i64 {
        // TODO(zig): Maybe use `std.time.nanoTimestamp()`.
        // https://github.com/ziglang/zig/pull/22871
        assert(is_windows);
        var ft: os.windows.FILETIME = undefined;
        stdx.windows.GetSystemTimePreciseAsFileTime(&ft);
        const ft64 = (@as(u64, ft.dwHighDateTime) << 32) | ft.dwLowDateTime;

        // FileTime is in units of 100 nanoseconds
        // and uses the NTFS/Windows epoch of 1601-01-01 instead of Unix Epoch 1970-01-01.
        const epoch_adjust = std.time.epoch.windows * (std.time.ns_per_s / 100);
        return (@as(i64, @bitCast(ft64)) + epoch_adjust) * 100;
    }

    fn realtime_unix() i64 {
        assert(is_darwin or is_linux);
        var ts: posix.timespec = undefined;
        const rc = system.clock_gettime(posix.CLOCK.REALTIME, &ts);
        if (posix.errno(rc) != .SUCCESS) unreachable;
        return @as(i64, ts.sec) * std.time.ns_per_s + ts.nsec;
    }
};

test "TimeOS monotonic smoke" {
    var time_os: TimeOS = .{};
    const time = time_os.interface();
    const instant_1 = time.monotonic();
    const instant_2 = time.monotonic();
    assert(instant_1.until(instant_1).ns == 0);
    assert(instant_1.until(instant_2).ns >= 0);
}

test "TimeOS realtime smoke" {
    var time_os: TimeOS = .{};
    const time = time_os.interface();
    const instant = time.realtime();
    assert(instant.date_time().year > 2000);
    assert(instant.date_time().year < 2100);
}

/// Simulated Time for testing.
pub const TimeSim = struct {
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

    pub const OffsetType = enum {
        linear,
        periodic,
        step,
        non_ideal,
    };

    pub fn interface(self: *TimeSim) Time {
        return .{
            .context = self,
            .vtable = &.{
                .monotonic = monotonic,
                .realtime = realtime,
            },
        };
    }

    fn monotonic(context: *anyopaque) Instant {
        const self: *TimeSim = @ptrCast(@alignCast(context));

        return .{ .ns = self.ticks * self.resolution };
    }

    fn realtime(context: *anyopaque) InstantUnix {
        const self: *TimeSim = @ptrCast(@alignCast(context));

        const realtime_true = self.epoch + @as(i64, @intCast(monotonic(context).ns));
        return .{ .ns = @intCast(realtime_true - self.offset(self.ticks)) };
    }

    pub fn offset(self: *TimeSim, ticks: u64) i64 {
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
                const offset_random: i64 = -@as(i64, @intCast(self.offset_coefficient_C)) +
                    @as(i64, @intCast(self.prng.int_inclusive(u64, 2 * self.offset_coefficient_C)));
                return @as(i64, @intFromFloat(std.math.floor(scaled))) + offset_random;
            },
        }
    }

    pub fn tick(self: *TimeSim) void {
        self.ticks += 1;
    }
};

/// Equivalent to `std.time.Timer`,
/// but using the `Time` interface as the source of time.
pub const Timer = struct {
    time: Time,
    started: Instant,

    pub fn init(time: Time) Timer {
        return .{
            .time = time,
            .started = time.monotonic(),
        };
    }

    /// Reads the timer value since start or the last reset.
    pub fn read(self: *Timer) stdx.Duration {
        const current = self.time.monotonic();
        assert(current.ns >= self.started.ns);
        return self.started.until(current);
    }

    /// Resets the timer.
    pub fn reset(self: *Timer) void {
        const current = self.time.monotonic();
        assert(current.ns >= self.started.ns);
        self.started = current;
    }
};

const testing = std.testing;

test Timer {
    var time_sim: TimeSim = (.{
        .resolution = 1,
        .offset_type = .linear,
        .offset_coefficient_A = 0,
        .offset_coefficient_B = 0,
        .offset_coefficient_C = 0,
    });
    const time = time_sim.interface();

    var timer = Timer.init(time);
    // Repeat the cycle read/reset multiple times:
    for (0..3) |_| {
        const time_0 = timer.read();
        try testing.expectEqual(@as(u64, 0), time_0.ns);
        time_sim.tick();

        const time_1 = timer.read();
        try testing.expectEqual(@as(u64, 1), time_1.ns);
        time_sim.tick();

        const time_2 = timer.read();
        try testing.expectEqual(@as(u64, 2), time_2.ns);
        time_sim.tick();

        timer.reset();
    }
}
