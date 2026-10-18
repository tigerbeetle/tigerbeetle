//! Syscall middle layer.
//!
//! Normally, access to OS faculties should be mediated by instances of types
//! like IO and Time, because such capability-passing interface allows for
//! domain-specific deterministic simulation testing. However, to implement a
//! real IO one needs to do syscalls at the end of the day, and in some rare
//! cases we want to call into OS directly.
//!
//! This file provides such direct access. Its purpose is to provide _well-typed_
//! access to OS:
//! - options structs instead of bitflags,
//! - Zig enums instead of ints,
//! - error unions instead of return codes,
//! - return values rather than out parameters,
//! - fixed-sized integers instead of target-dependent types.
//!
//! Design:
//! - Naming matches original syscalls, but abides by our naming conventions.
//! - Signatures and semantics may be slightly adjusted for convenience. For example, we use UNIX
//!   epoch Windows.
//! - Avoid exposing platform specfic types like `std.posix.clockid_t` publicly.
//! - But code is only opportunistically cross platform, anything non-trivial is caller's job to
//!   unify.
//! - Link to original OS documentation.
//! - Bindings are not comprehensive, only what's used is exposed. The surface area is extended as
//!   needed.
//!
//! This file should eventually subsume posix.zig, windows.zig, linux.zig and darwin.zig.
const std = @import("std");
const builtin = @import("builtin");

const posix = std.posix;
const assert = std.debug.assert;

const native_os = builtin.os.tag;
const is_linux = native_os == .linux;
const is_windows = native_os == .windows;
const is_darwin = native_os.isDarwin();

const system = if (is_linux)
    std.os.linux
else if (is_darwin)
    std.c
else
    void;

pub const ClockID = enum {
    realtime,
    boottime,
    monotonic,
};
pub const TimeSpec = struct {
    sec: i64,
    nsec: i64,
};

/// https://man7.org/linux/man-pages/man3/clock_gettime.3.html
/// https://manp.gs/mac/3/clock_gettime
pub fn clock_gettime(comptime clock: ClockID) error{ClockUnavailable}!TimeSpec {
    comptime assert(is_linux or is_darwin);

    const clock_id = switch (clock) {
        .boottime => posix.CLOCK.BOOTTIME,
        .realtime => posix.CLOCK.REALTIME,
        .monotonic => posix.CLOCK.MONOTONIC,
    };

    var ts: posix.timespec = undefined;
    const rc = system.clock_gettime(clock_id, &ts);
    switch (system.errno(rc)) {
        .SUCCESS => return .{
            .sec = ts.sec,
            .nsec = ts.nsec,
        },
        else => return error.ClockUnavailable,
    }
}

pub const darwin = struct {
    const mach_timebase_info_t = std.c.mach_timebase_info_data;
    extern "c" fn mach_timebase_info(info: *mach_timebase_info_t) std.c.kern_return_t;
    extern "c" fn mach_continuous_time() u64;
    extern "c" fn mach_absolute_time() u64;

    fn timebase_info() mach_timebase_info_t {
        // mach_timebase_info() called through libc already does global caching for us
        //
        // https://opensource.apple.com/source/xnu/xnu-7195.81.3/libsyscall/wrappers/mach_timebase_info.c.auto.html
        var info: mach_timebase_info_t = undefined;
        if (mach_timebase_info(&info) != 0) @panic("mach_timebase_info() failed");
        return info;
    }

    // https://developer.apple.com/documentation/kernel/1646199-mach_continuous_time
    // https://opensource.apple.com/source/Libc/Libc-1158.1.2/gen/clock_gettime.c.auto.html
    pub fn mach_continuous_time_ns() u64 {
        comptime assert(is_darwin);
        const info = timebase_info();
        const ticks = darwin.mach_continuous_time();
        return (ticks * info.numer) / info.denom;
    }

    pub fn mach_absolute_time_ns() u64 {
        comptime assert(is_darwin);
        const info = timebase_info();
        const ticks = darwin.mach_absolute_time();
        return (ticks * info.numer) / info.denom;
    }
};

pub const windows = struct {
    const ntdll = std.os.windows.ntdll;

    const TRUE: std.os.windows.BOOL = .TRUE;
    const FALSE: std.os.windows.BOOL = .FALSE;

    const FILETIME = std.os.windows.FILETIME;
    const LARGE_INTEGER = std.os.windows.LARGE_INTEGER;

    extern "kernel32" fn GetSystemTimePreciseAsFileTime(
        lpFileTime: *FILETIME,
    ) callconv(.winapi) void;

    fn QueryPerformanceFrequency() u64 {
        var result: LARGE_INTEGER = undefined;
        assert(ntdll.RtlQueryPerformanceFrequency(&result) != FALSE);
        return @bitCast(result);
    }

    fn QueryPerformanceCounter() u64 {
        var result: LARGE_INTEGER = undefined;
        assert(ntdll.RtlQueryPerformanceCounter(&result) != FALSE);
        return @bitCast(result);
    }

    pub fn query_performance_counter_ns() u64 {
        comptime assert(is_windows);
        const counter = QueryPerformanceCounter();
        // QPF need not be globally cached as it ends up being a load from read-only memory
        // mapped to all processed by the kernel called KUSER_SHARED_DATA (See "QpcFrequency")
        //
        // https://docs.microsoft.com/en-us/windows-hardware/drivers/ddi/ntddk/ns-ntddk-kuser_shared_data
        // https://www.geoffchappell.com/studies/windows/km/ntoskrnl/inc/api/ntexapi_x/kuser_shared_data/index.htm
        const frequency = QueryPerformanceFrequency();

        // 10Mhz (1 qpc tick every 100ns) is a common QPF on modern systems.
        // We can optimize towards this by converting to ns via a single multiply.
        //
        // https://github.com/microsoft/STL/blob/785143a0c73f030238ef618890fd4d6ae2b3a3a0/stl/inc/chrono#L694-L701
        const common_frequency = 10_000_000;
        if (frequency == common_frequency) return counter * (std.time.ns_per_s / common_frequency);

        // Convert qpc to nanos using fixed point to avoid expensive extra divs and
        // overflow.
        const scale = (std.time.ns_per_s << 32) / frequency;
        return @as(u64, @truncate((@as(u96, counter) * scale) >> 32));
    }

    // TODO(zig): Decide whether we usethis function or `RtlGetSystemTimePrecise` directly.
    // See https://github.com/ziglang/zig/pull/22871.
    pub fn get_system_time_precise_as_file_time_ns() i64 {
        comptime assert(native_os == .windows);
        var ft: FILETIME = undefined;
        GetSystemTimePreciseAsFileTime(&ft);
        const ft64 = (@as(u64, ft.dwHighDateTime) << 32) | ft.dwLowDateTime;
        // FileTime is in units of 100 nanoseconds
        // and uses the NTFS/Windows epoch of 1601-01-01 instead of Unix Epoch 1970-01-01.
        const epoch_adjust = std.time.epoch.windows * (std.time.ns_per_s / 100);
        return (@as(i64, @bitCast(ft64)) + epoch_adjust) * 100;
    }
};
