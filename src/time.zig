const std = @import("std");
const builtin = @import("builtin");

const stdx = @import("stdx");

const os = std.os;
const posix = std.posix;
const system = posix.system;
const assert = std.debug.assert;
const is_darwin = builtin.target.os.tag.isDarwin();
const is_windows = builtin.target.os.tag == .windows;
const is_linux = builtin.target.os.tag == .linux;
const Instant = stdx.Instant;

pub const Time = struct {
    context: *anyopaque,
    vtable: *const VTable,

    const VTable = struct {
        monotonic: *const fn (*anyopaque) u64,
        realtime: *const fn (*anyopaque) i64,
        tick: *const fn (*anyopaque) void,
    };

    /// A timestamp to measure elapsed time, meaningful only on the same system, not across reboots.
    /// Always use a monotonic timestamp if the goal is to measure elapsed time.
    /// This clock is not affected by discontinuous jumps in the system time, for example if the
    /// system administrator manually changes the clock.
    pub fn monotonic(self: Time) Instant {
        return .{ .ns = self.vtable.monotonic(self.context) };
    }

    /// A timestamp to measure real (i.e. wall clock) time, meaningful across systems, and reboots.
    /// This clock is affected by discontinuous jumps in the system time.
    pub fn realtime(self: Time) i64 {
        return self.vtable.realtime(self.context);
    }

    pub fn tick(self: Time) void {
        self.vtable.tick(self.context);
    }
};

pub const TimeOS = struct {
    /// Hardware and/or software bugs can mean that the monotonic clock may regress.
    /// One example (of many): https://bugzilla.redhat.com/show_bug.cgi?id=448449
    /// We crash the process for safety if this ever happens, to protect against infinite loops.
    /// It's better to crash and come back with a valid monotonic clock than get stuck forever.
    monotonic_guard: u64 = 0,

    pub fn time(self: *TimeOS) Time {
        return .{
            .context = self,
            .vtable = &.{
                .monotonic = monotonic,
                .realtime = realtime,
                .tick = tick,
            },
        };
    }

    fn monotonic(context: *anyopaque) u64 {
        const self: *TimeOS = @ptrCast(@alignCast(context));

        const m = blk: {
            if (is_windows) break :blk monotonic_windows();
            if (is_darwin) break :blk monotonic_darwin();
            if (is_linux) break :blk monotonic_linux();
            @compileError("unsupported OS");
        };

        // "Oops!...I Did It Again"
        if (m < self.monotonic_guard) @panic("a hardware/kernel bug regressed the monotonic clock");
        self.monotonic_guard = m;
        return m;
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
        const qpc = os.windows.QueryPerformanceCounter();
        const qpf = os.windows.QueryPerformanceFrequency();

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
        const ts: posix.timespec = posix.clock_gettime(posix.CLOCK.BOOTTIME) catch {
            @panic("CLOCK_BOOTTIME required");
        };
        return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
    }

    fn realtime(_: *anyopaque) i64 {
        if (is_windows) return realtime_windows();
        // macos has supported clock_gettime() since 10.12:
        // https://opensource.apple.com/source/Libc/Libc-1158.1.2/gen/clock_gettime.3.auto.html
        if (is_darwin or is_linux) return realtime_unix();
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
        const ts: posix.timespec = posix.clock_gettime(posix.CLOCK.REALTIME) catch unreachable;
        return @as(i64, ts.sec) * std.time.ns_per_s + ts.nsec;
    }

    fn tick(_: *anyopaque) void {}
};

test "Time monotonic smoke" {
    var time_os: TimeOS = .{};
    const time = time_os.time();
    const instant_1 = time.monotonic();
    const instant_2 = time.monotonic();
    assert(instant_1.duration_since(instant_1).ns == 0);
    assert(instant_2.duration_since(instant_1).ns >= 0);
}

/// Equivalent to `std.time.Timer`,
/// but using the `vsr.Time` interface as the source of time.
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
        return current.duration_since(self.started);
    }

    /// Resets the timer.
    pub fn reset(self: *Timer) void {
        const current = self.time.monotonic();
        assert(current.ns >= self.started.ns);
        self.started = current;
    }
};

const fixtures = @import("testing/fixtures.zig");
const testing = std.testing;

test Timer {
    var time_sim = fixtures.init_time(.{ .resolution = 1 });
    const time = time_sim.time();

    var timer = Timer.init(time);
    // Repeat the cycle read/reset multiple times:
    for (0..3) |_| {
        const time_0 = timer.read();
        try testing.expectEqual(@as(u64, 0), time_0.ns);
        time.tick();

        const time_1 = timer.read();
        try testing.expectEqual(@as(u64, 1), time_1.ns);
        time.tick();

        const time_2 = timer.read();
        try testing.expectEqual(@as(u64, 2), time_2.ns);
        time.tick();

        timer.reset();
    }
}
