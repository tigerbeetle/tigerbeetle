/// In microbenchmarks, we often measure both time and other performance counters
/// such as how many cycles were used, how many branch misses were incurred, and more.
/// These only count cycles in the current process, and not, for exampole, sleep time.
/// The closest matching clock implementation semantics are provided by CLOCK_MONOTONIC,
/// which is what we use here. To distinguish towards vsr.time.Time.monotonic(),
/// we call this `benchmark_monotonic()`.
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

fn benchmark_monotonic_darwin() u64 {
    assert(is_darwin);
    // Uses mach_absolute_time() instead of mach_continuous_time() because
    // we do *NOT* want to count while suspended here.
    //
    // https://developer.apple.com/documentation/kernel/1646199-mach_continuous_time
    // https://opensource.apple.com/source/Libc/Libc-1158.1.2/gen/clock_gettime.c.auto.html
    const darwin = struct {
        const mach_timebase_info_t = system.mach_timebase_info_data;
        extern "c" fn mach_timebase_info(info: *mach_timebase_info_t) system.kern_return_t;
        extern "c" fn mach_absolute_time() u64;
    };

    // mach_timebase_info() called through libc already does global caching for us
    //
    // https://opensource.apple.com/source/xnu/xnu-7195.81.3/libsyscall/wrappers/mach_timebase_info.c.auto.html
    var info: darwin.mach_timebase_info_t = undefined;
    if (darwin.mach_timebase_info(&info) != 0) @panic("mach_timebase_info() failed");

    const now = darwin.mach_absolute_time();
    return (now * info.numer) / info.denom;
}

fn benchmark_monotonic_linux() u64 {
    assert(is_linux);
    const ts: posix.timespec = posix.clock_gettime(posix.CLOCK.MONOTONIC) catch {
        @panic("CLOCK_BOOTTIME required");
    };
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}
