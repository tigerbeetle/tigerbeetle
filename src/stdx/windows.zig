const std = @import("std");
const windows = std.os.windows;

pub extern "kernel32" fn GetSystemTimePreciseAsFileTime(
    lpFileTime: *windows.FILETIME,
) callconv(.winapi) void;

pub extern "kernel32" fn GetCommandLineW() callconv(.winapi) windows.LPWSTR;

pub extern "kernel32" fn GetProcessTimes(
    in_hProcess: windows.HANDLE,
    out_lpCreationTime: *windows.FILETIME,
    out_lpExitTime: *windows.FILETIME,
    out_lpKernelTime: *windows.FILETIME,
    out_lpUserTime: *windows.FILETIME,
) callconv(.winapi) windows.BOOL;

pub extern "kernel32" fn SetProcessWorkingSetSize(
    hProcess: windows.HANDLE,
    dwMinimumWorkingSetSize: windows.SIZE_T,
    dwMaximumWorkingSetSize: windows.SIZE_T,
) callconv(.winapi) windows.BOOL;

pub extern "kernel32" fn GetProcessWorkingSetSize(
    hProcess: windows.HANDLE,
    lpMinimumWorkingSetSize: *windows.SIZE_T,
    lpMaximumWorkingSetSize: *windows.SIZE_T,
) callconv(.winapi) windows.BOOL;

pub const LOCKFILE_EXCLUSIVE_LOCK = 0x2;
pub const LOCKFILE_FAIL_IMMEDIATELY = 0x1;
pub extern "kernel32" fn LockFileEx(
    hFile: windows.HANDLE,
    dwFlags: windows.DWORD,
    dwReserved: windows.DWORD,
    nNumberOfBytesToLockLow: windows.DWORD,
    nNumberOfBytesToLockHigh: windows.DWORD,
    lpOverlapped: ?*windows.OVERLAPPED,
) callconv(.winapi) windows.BOOL;

pub extern "kernel32" fn SetEndOfFile(
    hFile: windows.HANDLE,
) callconv(.winapi) windows.BOOL;

pub extern "kernel32" fn ConnectNamedPipe(
    hNamedPipe: windows.HANDLE,
    lpOverlapped: ?*windows.OVERLAPPED,
) callconv(.winapi) windows.BOOL;
