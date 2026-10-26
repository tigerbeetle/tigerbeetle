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
//! - For cross-platform functionality, default to syscall name on Linux.
//! - Signatures and semantics may be slightly adjusted for convenience. For example, we use UNIX
//!   epoch on Windows.
//! - Avoid exposing platform specific types like `std.posix.clockid_t` publicly.
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

// Subset of Zig targets, supported by TigerBeetle.
const native_os: enum { linux, darwin, windows } = switch (builtin.os.tag) {
    .linux => .linux,
    .windows => .windows,
    else => |tag| if (tag.isDarwin()) .darwin else @compileError("unsupported os"),
};

const system = switch (native_os) {
    .linux => std.os.linux,
    .darwin => std.c,
    .windows => void,
};

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
    comptime assert(native_os == .linux or native_os == .darwin);

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

pub fn getrandom(buffer: []u8) void {
    comptime assert(native_os == .linux or native_os == .darwin);
    assert(buffer.len > 0);
    assert(buffer.len <= std.math.maxInt(u32));
    switch (native_os) {
        .linux => getrandom_linux(buffer),
        .darwin => getrandom_darwin(buffer),
        .windows => comptime unreachable, // Windows needs a different, stateful API.
    }
}

/// https://man7.org/linux/man-pages/man2/getrandom.2.html
fn getrandom_linux(buffer: []u8) void {
    comptime assert(native_os == .linux);

    assert(buffer.len > 0);
    assert(buffer.len <= std.math.maxInt(u32));

    var remaining: []u8 = buffer;
    // No fixed upper bound because of EINTR.
    while (remaining.len > 0) {
        const flags: u32 = 0;
        const return_code = std.os.linux.getrandom(remaining.ptr, remaining.len, flags);

        const size_filled = switch (std.os.linux.errno(return_code)) {
            .SUCCESS => return_code,
            .INTR => continue,
            else => @panic("getrandom: entropy unavailable"),
        };

        assert(size_filled > 0);
        assert(size_filled <= remaining.len);
        remaining = remaining[size_filled..];
    }
}

/// https://manp.gs/mac/3/arc4random_buf
fn getrandom_darwin(buffer: []u8) void {
    comptime assert(native_os == .darwin);
    // Blessed be sane API design!
    //
    //    These functions are always successful, and
    //    no return value is reserved to indicate an error.
    std.c.arc4random_buf(buffer.ptr, buffer.len);
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
        comptime assert(native_os == .darwin);
        const info = timebase_info();
        const ticks = darwin.mach_continuous_time();
        return (ticks * info.numer) / info.denom;
    }

    pub fn mach_absolute_time_ns() u64 {
        comptime assert(native_os == .darwin);
        const info = timebase_info();
        const ticks = darwin.mach_absolute_time();
        return (ticks * info.numer) / info.denom;
    }
};

pub const windows = struct {
    const ntdll = std.os.windows.ntdll;

    const SECURITY_ATTRIBUTES = std.os.windows.SECURITY_ATTRIBUTES;
    const FILETIME = std.os.windows.FILETIME;
    const HANDLE = std.os.windows.HANDLE;
    const INVALID_HANDLE_VALUE: HANDLE = std.os.windows.INVALID_HANDLE_VALUE;
    const IO_STATUS_BLOCK = std.os.windows.IO_STATUS_BLOCK;

    const FALSE: BOOL = .FALSE;
    const LARGE_INTEGER = std.os.windows.LARGE_INTEGER;
    const UNICODE_STRING = std.os.windows.UNICODE_STRING;
    const SIZE_T = std.os.windows.SIZE_T;
    const DWORD = std.os.windows.DWORD;
    const ULONG_PTR = std.os.windows.ULONG_PTR;
    const ULONGLONG = std.os.windows.ULONGLONG;
    const LPVOID = std.os.windows.LPVOID;
    const BOOL = std.os.windows.BOOL;
    const LPCWSTR = std.os.windows.LPCWSTR;

    const GetLastError = std.os.windows.GetLastError;
    const unexpectedError = std.os.windows.unexpectedError;
    const GetCurrentProcess = std.os.windows.GetCurrentProcess;

    const IOCTL = std.os.windows.IOCTL;

    pub const JobObject = struct {
        handle: HANDLE,

        const ExtendedLimitInformation = extern struct {
            BasicLimitInformation: BasicLimitInformation,
            IoInfo: IOCounters,
            ProcessMemoryLimit: SIZE_T,
            JobMemoryLimit: SIZE_T,
            PeakProcessMemoryUsed: SIZE_T,
            PeakJobMemoryUsed: SIZE_T,
        };

        const BasicLimitInformation = extern struct {
            PerProcessUserTimeLimit: LARGE_INTEGER,
            PerJobUserTimeLimit: LARGE_INTEGER,
            LimitFlags: DWORD,
            MinimumWorkingSetSize: SIZE_T,
            MaximumWorkingSetSize: SIZE_T,
            ActiveProcessLimit: DWORD,
            Affinity: ULONG_PTR,
            PriorityClass: DWORD,
            SchedulingClass: DWORD,
        };

        const IOCounters = extern struct {
            ReadOperationCount: ULONGLONG,
            WriteOperationCount: ULONGLONG,
            OtherOperationCount: ULONGLONG,
            ReadTransferCount: ULONGLONG,
            WriteTransferCount: ULONGLONG,
            OtherTransferCount: ULONGLONG,
        };

        const InformationClass = enum(u8) {
            BasicLimit = 2,
            ExtendedLimit = 9,
        };

        const LimitFlags = enum(u32) {
            KillOnJobClose = 8192,
        };

        extern "kernel32" fn CreateJobObjectW(
            lpJobAttributes: ?*SECURITY_ATTRIBUTES,
            lpName: ?LPCWSTR,
        ) callconv(.winapi) ?HANDLE;

        extern "kernel32" fn AssignProcessToJobObject(
            hJob: HANDLE,
            hProcess: HANDLE,
        ) callconv(.winapi) BOOL;

        extern "kernel32" fn SetInformationJobObject(
            hJob: HANDLE,
            JobObjectInformationClass: JobObject.InformationClass,
            lpJobObjectInformation: LPVOID,
            cbJobObjectInformationLength: DWORD,
        ) callconv(.winapi) BOOL;

        pub fn open(options: struct {
            kill_on_job_close: bool,
            assign_process_to_job_object: bool,
        }) error{ PermissionDenied, Unexpected }!JobObject {
            var job_object: JobObject = .{ .handle = INVALID_HANDLE_VALUE };

            job_object.handle = CreateJobObjectW(null, null) orelse {
                switch (GetLastError()) {
                    .INVALID_HANDLE, .ALREADY_EXISTS => unreachable,
                    else => |err| return unexpectedError(err),
                }
            };
            errdefer job_object.deinit();

            if (options.kill_on_job_close) {
                var extended_limit_information = ExtendedLimitInformation{
                    .BasicLimitInformation = .{
                        .LimitFlags = @intFromEnum(LimitFlags.KillOnJobClose),
                        .PerJobUserTimeLimit = undefined,
                        .ActiveProcessLimit = undefined,
                        .Affinity = undefined,
                        .MaximumWorkingSetSize = undefined,
                        .MinimumWorkingSetSize = undefined,
                        .PerProcessUserTimeLimit = undefined,
                        .PriorityClass = undefined,
                        .SchedulingClass = undefined,
                    },
                    .JobMemoryLimit = undefined,
                    .IoInfo = undefined,
                    .ProcessMemoryLimit = undefined,
                    .PeakJobMemoryUsed = undefined,
                    .PeakProcessMemoryUsed = undefined,
                };

                if (SetInformationJobObject(
                    job_object.handle,
                    InformationClass.ExtendedLimit,
                    &extended_limit_information,
                    @sizeOf(ExtendedLimitInformation),
                ) == FALSE) {
                    switch (GetLastError()) {
                        .ACCESS_DENIED => return error.PermissionDenied,
                        else => |err| return unexpectedError(err),
                    }
                }
            }

            if (options.assign_process_to_job_object) {
                if (AssignProcessToJobObject(job_object.handle, GetCurrentProcess()) == FALSE) {
                    switch (GetLastError()) {
                        .ACCESS_DENIED => return error.PermissionDenied,
                        else => |err| return unexpectedError(err),
                    }
                }
            }

            return job_object;
        }

        pub fn deinit(job_object: *JobObject) void {
            assert(job_object.handle != INVALID_HANDLE_VALUE);
            CloseHandle(job_object.handle);
            job_object.handle = INVALID_HANDLE_VALUE;
        }
    };

    extern "kernel32" fn GetSystemTimePreciseAsFileTime(
        lpFileTime: *FILETIME,
    ) callconv(.winapi) void;

    fn CloseHandle(hObject: HANDLE) void {
        const status = ntdll.NtClose(hObject);
        assert(status == .SUCCESS);
    }

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
        comptime assert(native_os == .windows);
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

    pub const CNGDevice = struct {
        handle: HANDLE,

        pub fn open() error{CNGDeviceNotAvailable}!CNGDevice {
            var result: CNGDevice = .{ .handle = undefined };

            var io_status_block: IO_STATUS_BLOCK = undefined;
            const object_name = UNICODE_STRING.init(
                &.{ '\\', 'D', 'e', 'v', 'i', 'c', 'e', '\\', 'C', 'N', 'G' },
            );
            const status = ntdll.NtOpenFile(
                &result.handle,
                .{
                    .STANDARD = .{ .SYNCHRONIZE = true },
                    .SPECIFIC = .{ .FILE = .{ .READ_DATA = true } },
                },
                &.{ .ObjectName = @constCast(&object_name) },
                &io_status_block,
                .VALID_FLAGS,
                .{ .IO = .SYNCHRONOUS_NONALERT },
            );

            switch (status) {
                .SUCCESS => return result,
                .CANCELLED => unreachable,
                else => return error.CNGDeviceNotAvailable,
            }
        }

        pub fn deinit(cng: *CNGDevice) void {
            assert(cng.handle != INVALID_HANDLE_VALUE);
            CloseHandle(cng.handle);
            cng.handle = INVALID_HANDLE_VALUE;
        }

        pub fn getrandom(cng: *const CNGDevice, buffer: []u8) void {
            assert(buffer.len > 0);
            assert(buffer.len <= std.math.maxInt(u32));
            assert(cng.handle != INVALID_HANDLE_VALUE);

            // https://codeberg.org/ziglang/zig/src/tag/0.16.0/lib/std/Io/Threaded.zig#L17013
            var io_status_block: IO_STATUS_BLOCK = undefined;
            const status = ntdll.NtDeviceIoControlFile(
                cng.handle,
                null,
                null,
                null,
                &io_status_block,
                IOCTL.KSEC.GEN_RANDOM,
                null,
                0,
                buffer.ptr,
                @intCast(buffer.len),
            );
            switch (status) {
                .SUCCESS => {},
                .CANCELLED => unreachable,
                else => @panic("getrandom: entropy unavailable"),
            }
        }
    };
};
