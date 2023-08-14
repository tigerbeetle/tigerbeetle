const std = @import("std");
const builtin = @import("builtin");

const os = std.os;
const assert = std.debug.assert;
const is_darwin = builtin.target.os.tag.isDarwin();
const is_windows = builtin.target.os.tag == .windows;

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
        const m = blk: {
            // Uses QueryPerformanceCounter() on windows due to it being the highest precision timer
            // available while also accounting for time spent suspended by default:
            // https://docs.microsoft.com/en-us/windows/win32/api/realtimeapiset/nf-realtimeapiset-queryunbiasedinterrupttime#remarks
            if (is_windows) {
                // QPF need not be globally cached either as it ends up being a load from read-only
                // memory mapped to all processed by the kernel called KUSER_SHARED_DATA (See "QpcFrequency")
                // https://docs.microsoft.com/en-us/windows-hardware/drivers/ddi/ntddk/ns-ntddk-kuser_shared_data
                // https://www.geoffchappell.com/studies/windows/km/ntoskrnl/inc/api/ntexapi_x/kuser_shared_data/index.htm
                const qpc = os.windows.QueryPerformanceCounter();
                const qpf = os.windows.QueryPerformanceFrequency();

                // 10Mhz (1 qpc tick every 100ns) is a common QPF on modern systems.
                // We can optimize towards this by converting to ns via a single multiply.
                // https://github.com/microsoft/STL/blob/785143a0c73f030238ef618890fd4d6ae2b3a3a0/stl/inc/chrono#L694-L701
                const common_qpf = 10_000_000;
                if (qpf == common_qpf) break :blk qpc * (std.time.ns_per_s / common_qpf);

                // Convert qpc to nanos using fixed point to avoid expensive extra divs and overflow.
                const scale = (std.time.ns_per_s << 32) / qpf;
                break :blk @as(u64, @truncate((@as(u96, qpc) * scale) >> 32));
            }

            // Uses mach_continuous_time() instead of mach_absolute_time() as it counts while suspended.
            // https://developer.apple.com/documentation/kernel/1646199-mach_continuous_time
            // https://opensource.apple.com/source/Libc/Libc-1158.1.2/gen/clock_gettime.c.auto.html
            if (is_darwin) {
                const darwin = struct {
                    const mach_timebase_info_t = os.darwin.mach_timebase_info_data;
                    extern "c" fn mach_timebase_info(info: *mach_timebase_info_t) os.darwin.kern_return_t;
                    extern "c" fn mach_continuous_time() u64;
                };

                // mach_timebase_info() called through libc already does global caching for us
                // https://opensource.apple.com/source/xnu/xnu-7195.81.3/libsyscall/wrappers/mach_timebase_info.c.auto.html
                var info: darwin.mach_timebase_info_t = undefined;
                if (darwin.mach_timebase_info(&info) != 0) @panic("mach_timebase_info() failed");

                const now = darwin.mach_continuous_time();
                return (now * info.numer) / info.denom;
            }

            // The true monotonic clock on Linux is not in fact CLOCK_MONOTONIC:
            // CLOCK_MONOTONIC excludes elapsed time while the system is suspended (e.g. VM migration).
            // CLOCK_BOOTTIME is the same as CLOCK_MONOTONIC but includes elapsed time during a suspend.
            // For more detail and why CLOCK_MONOTONIC_RAW is even worse than CLOCK_MONOTONIC,
            // see https://github.com/ziglang/zig/pull/933#discussion_r656021295.
            var ts: os.timespec = undefined;
            os.clock_gettime(os.CLOCK.BOOTTIME, &ts) catch @panic("CLOCK_BOOTTIME required");
            break :blk @as(u64, @intCast(ts.tv_sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.tv_nsec));
        };

        // "Oops!...I Did It Again"
        if (m < self.monotonic_guard) @panic("a hardware/kernel bug regressed the monotonic clock");
        self.monotonic_guard = m;
        return m;
    }

    /// A timestamp to measure real (i.e. wall clock) time, meaningful across systems, and reboots.
    /// This clock is affected by discontinuous jumps in the system time.
    pub fn realtime(_: *Self) i64 {
        if (is_windows) {
            const kernel32 = struct {
                extern "kernel32" fn GetSystemTimePreciseAsFileTime(
                    lpFileTime: *os.windows.FILETIME,
                ) callconv(os.windows.WINAPI) void;
            };

            var ft: os.windows.FILETIME = undefined;
            kernel32.GetSystemTimePreciseAsFileTime(&ft);
            const ft64 = (@as(u64, ft.dwHighDateTime) << 32) | ft.dwLowDateTime;

            // FileTime is in units of 100 nanoseconds
            // and uses the NTFS/Windows epoch of 1601-01-01 instead of Unix Epoch 1970-01-01.
            const epoch_adjust = std.time.epoch.windows * (std.time.ns_per_s / 100);
            return (@as(i64, @bitCast(ft64)) + epoch_adjust) * 100;
        }

        if (is_darwin) {
            // macos has supported clock_gettime() since 10.12:
            // https://opensource.apple.com/source/Libc/Libc-1158.1.2/gen/clock_gettime.3.auto.html
        }

        var ts: os.timespec = undefined;
        os.clock_gettime(os.CLOCK.REALTIME, &ts) catch unreachable;
        return @as(i64, ts.tv_sec) * std.time.ns_per_s + ts.tv_nsec;
    }

    pub fn tick(_: *Self) void {}
};
