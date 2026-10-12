const std = @import("std");
const windows = std.os.windows;
const assert = std.debug.assert;

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
    lpOverlapped: ?*OVERLAPPED,
) callconv(.winapi) windows.BOOL;

pub extern "kernel32" fn SetEndOfFile(
    hFile: windows.HANDLE,
) callconv(.winapi) windows.BOOL;

pub extern "kernel32" fn ConnectNamedPipe(
    hNamedPipe: windows.HANDLE,
    lpOverlapped: ?*OVERLAPPED,
) callconv(.winapi) windows.BOOL;

// Zig 0.16 removed most of `std.os.windows`. What TigerBeetle uses is vendored below from Zig
// 0.14.1 into a single namespace, so that `stdx.windows` can stand in for `std.os.windows`.
pub const kernel32 = @This();
pub const ws2_32 = @This();

pub const BOOL = windows.BOOL;
pub const BYTE = windows.BYTE;
pub const DWORD = windows.DWORD;
pub const GUID = windows.GUID;
pub const HANDLE = windows.HANDLE;
pub const IO_STATUS_BLOCK = windows.IO_STATUS_BLOCK;
pub const INVALID_HANDLE_VALUE = windows.INVALID_HANDLE_VALUE;
pub const OBJECT = windows.OBJECT;
pub const ULONG = windows.ULONG;
pub const UNICODE_STRING = windows.UNICODE_STRING;
pub const CloseHandle = windows.CloseHandle;
pub const GetLastError = windows.GetLastError;
pub const ntdll = windows.ntdll;
pub const unexpectedError = windows.unexpectedError;
pub const unexpectedStatus = windows.unexpectedStatus;
pub const SO = windows.ws2_32.SO;
pub const SOL = windows.ws2_32.SOL;
pub const sockaddr = windows.ws2_32.sockaddr;

pub const TRUE: windows.BOOL = .TRUE;
pub const FALSE: windows.BOOL = .FALSE;

pub const INFINITE = 4294967295;

pub const WAIT_ABANDONED = 0x00000080;
pub const WAIT_OBJECT_0 = 0x00000000;
pub const WAIT_TIMEOUT = 0x00000102;
pub const WAIT_FAILED = 0xFFFFFFFF;

pub const GENERIC_READ = 0x80000000;
pub const GENERIC_WRITE = 0x40000000;
pub const SYNCHRONIZE = 0x00100000;

pub const FILE_SHARE_DELETE = 0x00000004;
pub const FILE_SHARE_READ = 0x00000001;
pub const FILE_SHARE_WRITE = 0x00000002;

pub const FILE_CREATE = 2;
pub const FILE_DIRECTORY_FILE = 0x00000001;
pub const FILE_WRITE_THROUGH = 0x00000002;
pub const FILE_NO_INTERMEDIATE_BUFFERING = 0x00000008;
pub const FILE_NON_DIRECTORY_FILE = 0x00000040;
pub const FILE_OPEN_REPARSE_POINT = 0x00200000;

pub const OPEN_EXISTING = 3;
pub const FILE_BEGIN = 0;

pub const FILE_SKIP_COMPLETION_PORT_ON_SUCCESS = 0x1;
pub const FILE_SKIP_SET_EVENT_ON_HANDLE = 0x2;

pub const PIPE_ACCESS_OUTBOUND = 0x00000002;
pub const PIPE_TYPE_BYTE = 0x00000000;
pub const PIPE_WAIT = 0x00000000;

pub const FORMAT_MESSAGE_FROM_SYSTEM = 0x00001000;
pub const FORMAT_MESSAGE_IGNORE_INSERTS = 0x00000200;

pub const OVERLAPPED = extern struct {
    Internal: windows.ULONG_PTR,
    InternalHigh: windows.ULONG_PTR,
    DUMMYUNIONNAME: extern union {
        DUMMYSTRUCTNAME: extern struct {
            Offset: windows.DWORD,
            OffsetHigh: windows.DWORD,
        },
        Pointer: ?windows.PVOID,
    },
    hEvent: ?windows.HANDLE,
};

pub const OVERLAPPED_ENTRY = extern struct {
    lpCompletionKey: windows.ULONG_PTR,
    lpOverlapped: *OVERLAPPED,
    Internal: windows.ULONG_PTR,
    dwNumberOfBytesTransferred: windows.DWORD,
};

pub const OpenError = error{
    IsDir,
    NotDir,
    FileNotFound,
    NoDevice,
    AccessDenied,
    PipeBusy,
    PathAlreadyExists,
    Unexpected,
    NameTooLong,
    WouldBlock,
    NetworkNotFound,
    AntivirusInterference,
    BadPathName,
};

pub const OpenFileOptions = struct {
    access_mask: windows.DWORD,
    dir: ?windows.HANDLE = null,
    sa: ?*windows.SECURITY_ATTRIBUTES = null,
    share_access: windows.ULONG = FILE_SHARE_WRITE | FILE_SHARE_READ | FILE_SHARE_DELETE,
    creation: windows.ULONG,
    filter: enum { file_only, dir_only, any } = .file_only,
    follow_symlinks: bool = true,
};

pub fn sliceToPrefixedFileW(
    dir: ?windows.HANDLE,
    path: []const u8,
) !std.Io.Threaded.WindowsPathSpace {
    return std.Io.Threaded.sliceToPrefixedFileW(dir, path, .{});
}

pub fn wait_for_single_object(
    handle: windows.HANDLE,
    milliseconds: windows.DWORD,
) error{ WaitAbandoned, WaitTimeOut, Unexpected }!void {
    switch (WaitForSingleObjectEx(handle, milliseconds, FALSE)) {
        WAIT_ABANDONED => return error.WaitAbandoned,
        WAIT_OBJECT_0 => return,
        WAIT_TIMEOUT => return error.WaitTimeOut,
        WAIT_FAILED => switch (GetLastError()) {
            else => |err| return unexpectedError(err),
        },
        else => return error.Unexpected,
    }
}

pub const CreateIoCompletionPortError = error{Unexpected};

pub fn CreateIoCompletionPort(
    file_handle: windows.HANDLE,
    existing_completion_port: ?windows.HANDLE,
    completion_key: usize,
    concurrent_thread_count: windows.DWORD,
) CreateIoCompletionPortError!windows.HANDLE {
    const handle = externs.CreateIoCompletionPort(
        file_handle,
        existing_completion_port,
        completion_key,
        concurrent_thread_count,
    ) orelse {
        switch (GetLastError()) {
            .INVALID_PARAMETER => unreachable,
            else => |err| return unexpectedError(err),
        }
    };
    return handle;
}

pub const PostQueuedCompletionStatusError = error{Unexpected};

pub fn PostQueuedCompletionStatus(
    completion_port: windows.HANDLE,
    bytes_transferred_count: windows.DWORD,
    completion_key: usize,
    lpOverlapped: ?*OVERLAPPED,
) PostQueuedCompletionStatusError!void {
    if (externs.PostQueuedCompletionStatus(
        completion_port,
        bytes_transferred_count,
        completion_key,
        lpOverlapped,
    ) == FALSE) {
        switch (GetLastError()) {
            else => |err| return unexpectedError(err),
        }
    }
}

pub const GetQueuedCompletionStatusError = error{
    Aborted,
    Cancelled,
    EOF,
    Timeout,
} || std.posix.UnexpectedError;

pub fn GetQueuedCompletionStatusEx(
    completion_port: windows.HANDLE,
    completion_port_entries: []OVERLAPPED_ENTRY,
    timeout_ms: ?windows.DWORD,
    alertable: bool,
) GetQueuedCompletionStatusError!u32 {
    var num_entries_removed: u32 = 0;

    const success = externs.GetQueuedCompletionStatusEx(
        completion_port,
        completion_port_entries.ptr,
        @as(windows.ULONG, @intCast(completion_port_entries.len)),
        &num_entries_removed,
        timeout_ms orelse INFINITE,
        .fromBool(alertable),
    );

    if (success == FALSE) {
        return switch (GetLastError()) {
            .ABANDONED_WAIT_0 => error.Aborted,
            .OPERATION_ABORTED => error.Cancelled,
            .HANDLE_EOF => error.EOF,
            .WAIT_TIMEOUT => error.Timeout,
            else => |err| unexpectedError(err),
        };
    }

    return num_entries_removed;
}

pub fn read_file_blocking(
    in_hFile: windows.HANDLE,
    buffer: []u8,
    offset: ?u64,
) error{
    BrokenPipe,
    ConnectionResetByPeer,
    OperationAborted,
    LockViolation,
    Unexpected,
}!usize {
    while (true) {
        const want_read_count: windows.DWORD = @min(std.math.maxInt(windows.DWORD), buffer.len);
        var amt_read: windows.DWORD = undefined;
        var overlapped_data: OVERLAPPED = undefined;
        const overlapped: ?*OVERLAPPED = if (offset) |off| blk: {
            overlapped_data = .{
                .Internal = 0,
                .InternalHigh = 0,
                .DUMMYUNIONNAME = .{
                    .DUMMYSTRUCTNAME = .{
                        .Offset = @as(u32, @truncate(off)),
                        .OffsetHigh = @as(u32, @truncate(off >> 32)),
                    },
                },
                .hEvent = null,
            };
            break :blk &overlapped_data;
        } else null;
        if (ReadFile(in_hFile, buffer.ptr, want_read_count, &amt_read, overlapped) == FALSE) {
            switch (GetLastError()) {
                .IO_PENDING => unreachable,
                .OPERATION_ABORTED => continue,
                .BROKEN_PIPE => return 0,
                .HANDLE_EOF => return 0,
                .NETNAME_DELETED => return error.ConnectionResetByPeer,
                .LOCK_VIOLATION => return error.LockViolation,
                else => |err| return unexpectedError(err),
            }
        }
        return amt_read;
    }
}

pub fn write_file_blocking(
    handle: windows.HANDLE,
    bytes: []const u8,
    offset: ?u64,
) error{
    SystemResources,
    OperationAborted,
    BrokenPipe,
    NotOpenForWriting,
    LockViolation,
    ConnectionResetByPeer,
    Unexpected,
}!usize {
    var bytes_written: windows.DWORD = undefined;
    var overlapped_data: OVERLAPPED = undefined;
    const overlapped: ?*OVERLAPPED = if (offset) |off| blk: {
        overlapped_data = .{
            .Internal = 0,
            .InternalHigh = 0,
            .DUMMYUNIONNAME = .{
                .DUMMYSTRUCTNAME = .{
                    .Offset = @truncate(off),
                    .OffsetHigh = @truncate(off >> 32),
                },
            },
            .hEvent = null,
        };
        break :blk &overlapped_data;
    } else null;
    const adjusted_len = std.math.cast(u32, bytes.len) orelse std.math.maxInt(u32);
    if (WriteFile(handle, bytes.ptr, adjusted_len, &bytes_written, overlapped) == FALSE) {
        switch (GetLastError()) {
            .INVALID_USER_BUFFER => return error.SystemResources,
            .NOT_ENOUGH_MEMORY => return error.SystemResources,
            .OPERATION_ABORTED => return error.OperationAborted,
            .NOT_ENOUGH_QUOTA => return error.SystemResources,
            .IO_PENDING => unreachable,
            .NO_DATA => return error.BrokenPipe,
            .INVALID_HANDLE => return error.NotOpenForWriting,
            .LOCK_VIOLATION => return error.LockViolation,
            .NETNAME_DELETED => return error.ConnectionResetByPeer,
            .WORKING_SET_QUOTA => return error.SystemResources,
            else => |err| return unexpectedError(err),
        }
    }
    return bytes_written;
}

pub const GetFileSizeError = error{Unexpected};

pub fn GetFileSizeEx(hFile: windows.HANDLE) GetFileSizeError!u64 {
    var file_size: windows.LARGE_INTEGER = undefined;
    if (externs.GetFileSizeEx(hFile, &file_size) == FALSE) {
        switch (GetLastError()) {
            else => |err| return unexpectedError(err),
        }
    }
    return @as(u64, @bitCast(file_size));
}

pub fn WSAStartup(majorVersion: u8, minorVersion: u8) !WSADATA {
    var wsadata: WSADATA = undefined;
    const version = (@as(windows.WORD, minorVersion) << 8) | majorVersion;
    return switch (externs.WSAStartup(version, &wsadata)) {
        0 => wsadata,
        else => |err_int| switch (@as(WinsockError, @enumFromInt(@as(u16, @intCast(err_int))))) {
            .WSASYSNOTREADY => return error.SystemNotAvailable,
            .WSAVERNOTSUPPORTED => return error.VersionNotSupported,
            .WSAEINPROGRESS => return error.BlockingOperationInProgress,
            .WSAEPROCLIM => return error.ProcessFdQuotaExceeded,
            else => |err| return unexpectedWSAError(err),
        },
    };
}

pub fn WSACleanup() !void {
    return switch (externs.WSACleanup()) {
        0 => {},
        SOCKET_ERROR => switch (WSAGetLastError()) {
            .WSANOTINITIALISED => return error.NotInitialized,
            .WSAENETDOWN => return error.NetworkNotAvailable,
            .WSAEINPROGRESS => return error.BlockingOperationInProgress,
            else => |err| return unexpectedWSAError(err),
        },
        else => unreachable,
    };
}

pub fn WSASocketW(
    af: i32,
    socket_type: i32,
    protocol: i32,
    protocolInfo: ?*anyopaque,
    g: u32,
    dwFlags: windows.DWORD,
) !SOCKET {
    const rc = externs.WSASocketW(af, socket_type, protocol, protocolInfo, g, dwFlags);
    if (rc == INVALID_SOCKET) {
        switch (WSAGetLastError()) {
            .WSAEAFNOSUPPORT => return error.AddressFamilyNotSupported,
            .WSAEMFILE => return error.ProcessFdQuotaExceeded,
            .WSAENOBUFS => return error.SystemResources,
            .WSAEPROTONOSUPPORT => return error.ProtocolNotSupported,
            else => |err| return unexpectedWSAError(err),
        }
    }
    return rc;
}

pub fn sendto(
    s: SOCKET,
    buf: [*]const u8,
    len: usize,
    flags: u32,
    to: ?*const sockaddr,
    to_len: windows.ws2_32.socklen_t,
) i32 {
    var buffer = WSABUF{ .len = @as(u31, @truncate(len)), .buf = @constCast(buf) };
    var bytes_send: windows.DWORD = undefined;
    if (WSASendTo(s, @ptrCast(&buffer), 1, &bytes_send, flags, to, @intCast(to_len), null, null) ==
        SOCKET_ERROR)
    {
        return SOCKET_ERROR;
    } else {
        return @as(i32, @as(u31, @intCast(bytes_send)));
    }
}

pub fn terminate_process(
    hProcess: windows.HANDLE,
    uExitCode: windows.UINT,
) error{ PermissionDenied, Unexpected }!void {
    if (TerminateProcess(hProcess, uExitCode) == FALSE) {
        switch (GetLastError()) {
            .ACCESS_DENIED => return error.PermissionDenied,
            else => |err| return unexpectedError(err),
        }
    }
}

pub fn SetFileCompletionNotificationModes(handle: windows.HANDLE, flags: windows.UCHAR) !void {
    const success = externs.SetFileCompletionNotificationModes(handle, flags);
    if (success == FALSE) {
        return switch (GetLastError()) {
            else => |err| unexpectedError(err),
        };
    }
}

pub fn QueryPerformanceFrequency() u64 {
    var result: windows.LARGE_INTEGER = undefined;
    assert(windows.ntdll.RtlQueryPerformanceFrequency(&result) != FALSE);
    return @bitCast(result);
}

pub fn QueryPerformanceCounter() u64 {
    var result: windows.LARGE_INTEGER = undefined;
    assert(windows.ntdll.RtlQueryPerformanceCounter(&result) != FALSE);
    return @bitCast(result);
}

/// Unlike `unexpectedError`, doesn't format the error code as a `Win32Error` tag name, which
/// doesn't exist for Winsock error codes.
pub fn unexpectedWSAError(err: WinsockError) std.posix.UnexpectedError {
    if (std.options.unexpected_error_tracing) {
        std.debug.print("error.Unexpected: WSAGetLastError({d})\n", .{@intFromEnum(err)});
        std.debug.dumpCurrentStackTrace(.{ .first_address = @returnAddress() });
    }
    return error.Unexpected;
}

pub extern "kernel32" fn CreateFileW(
    lpFileName: windows.LPCWSTR,
    dwDesiredAccess: windows.DWORD,
    dwShareMode: windows.DWORD,
    lpSecurityAttributes: ?*windows.SECURITY_ATTRIBUTES,
    dwCreationDisposition: windows.DWORD,
    dwFlagsAndAttributes: windows.DWORD,
    hTemplateFile: ?windows.HANDLE,
) callconv(.winapi) windows.HANDLE;

pub extern "kernel32" fn CreateNamedPipeW(
    lpName: windows.LPCWSTR,
    dwOpenMode: windows.DWORD,
    dwPipeMode: windows.DWORD,
    nMaxInstances: windows.DWORD,
    nOutBufferSize: windows.DWORD,
    nInBufferSize: windows.DWORD,
    nDefaultTimeOut: windows.DWORD,
    lpSecurityAttributes: ?*const windows.SECURITY_ATTRIBUTES,
) callconv(.winapi) windows.HANDLE;

pub extern "kernel32" fn SetFilePointerEx(
    hFile: windows.HANDLE,
    liDistanceToMove: windows.LARGE_INTEGER,
    lpNewFilePointer: ?*windows.LARGE_INTEGER,
    dwMoveMethod: windows.DWORD,
) callconv(.winapi) windows.BOOL;

pub extern "kernel32" fn WriteFile(
    in_hFile: windows.HANDLE,
    in_lpBuffer: [*]const u8,
    in_nNumberOfBytesToWrite: windows.DWORD,
    out_lpNumberOfBytesWritten: ?*windows.DWORD,
    in_out_lpOverlapped: ?*OVERLAPPED,
) callconv(.winapi) windows.BOOL;

pub extern "kernel32" fn ReadFile(
    hFile: windows.HANDLE,
    lpBuffer: windows.LPVOID,
    nNumberOfBytesToRead: windows.DWORD,
    lpNumberOfBytesRead: ?*windows.DWORD,
    lpOverlapped: ?*OVERLAPPED,
) callconv(.winapi) windows.BOOL;

pub extern "kernel32" fn DuplicateHandle(
    hSourceProcessHandle: windows.HANDLE,
    hSourceHandle: windows.HANDLE,
    hTargetProcessHandle: windows.HANDLE,
    lpTargetHandle: *windows.HANDLE,
    dwDesiredAccess: windows.DWORD,
    bInheritHandle: windows.BOOL,
    dwOptions: windows.DWORD,
) callconv(.winapi) windows.BOOL;

pub extern "kernel32" fn GetOverlappedResult(
    hFile: windows.HANDLE,
    lpOverlapped: *OVERLAPPED,
    lpNumberOfBytesTransferred: *windows.DWORD,
    bWait: windows.BOOL,
) callconv(.winapi) windows.BOOL;

pub extern "kernel32" fn TerminateProcess(
    hProcess: windows.HANDLE,
    uExitCode: windows.UINT,
) callconv(.winapi) windows.BOOL;

pub extern "kernel32" fn WaitForSingleObjectEx(
    hHandle: windows.HANDLE,
    dwMilliseconds: windows.DWORD,
    bAlertable: windows.BOOL,
) callconv(.winapi) windows.DWORD;

pub extern "kernel32" fn Sleep(
    dwMilliseconds: windows.DWORD,
) callconv(.winapi) void;

pub extern "kernel32" fn GetEnvironmentVariableW(
    lpName: ?windows.LPCWSTR,
    lpBuffer: ?[*]windows.WCHAR,
    nSize: windows.DWORD,
) callconv(.winapi) windows.DWORD;

pub extern "kernel32" fn SetEnvironmentVariableW(
    lpName: windows.LPCWSTR,
    lpValue: ?windows.LPCWSTR,
) callconv(.winapi) windows.BOOL;

pub extern "kernel32" fn GetConsoleMode(
    hConsoleHandle: windows.HANDLE,
    lpMode: *windows.DWORD,
) callconv(.winapi) windows.BOOL;

pub extern "kernel32" fn SetConsoleMode(
    hConsoleHandle: windows.HANDLE,
    dwMode: windows.DWORD,
) callconv(.winapi) windows.BOOL;

pub extern "kernel32" fn GetModuleHandleW(
    lpModuleName: ?windows.LPCWSTR,
) callconv(.winapi) ?windows.HMODULE;

pub extern "kernel32" fn GetProcAddress(
    hModule: windows.HMODULE,
    lpProcName: windows.LPCSTR,
) callconv(.winapi) ?windows.FARPROC;

pub extern "kernel32" fn FormatMessageW(
    dwFlags: windows.DWORD,
    lpSource: ?windows.LPCVOID,
    dwMessageId: windows.Win32Error,
    dwLanguageId: windows.DWORD,
    lpBuffer: windows.LPWSTR,
    nSize: windows.DWORD,
    Arguments: ?*windows.va_list,
) callconv(.winapi) windows.DWORD;

// Zig 0.16's `std.posix.socket_t` is a `HANDLE` on Windows.
pub const SOCKET = windows.HANDLE;
pub const INVALID_SOCKET = @as(SOCKET, @ptrFromInt(~@as(usize, 0)));

pub const WSAID_CONNECTEX = windows.GUID{
    .Data1 = 0x25a207b9,
    .Data2 = 0xddf3,
    .Data3 = 0x4660,
    .Data4 = [8]u8{ 0x8e, 0xe9, 0x76, 0xe5, 0x8c, 0x74, 0x06, 0x3e },
};

pub const IOC_WS2 = 134217728;
pub const SIO_GET_EXTENSION_FUNCTION_POINTER = IOC_OUT | IOC_IN | IOC_WS2 | 6;
pub const IOC_OUT = 1073741824;
pub const IOC_IN = 2147483648;

pub const SOCKET_ERROR = -1;

pub const WSA_FLAG_OVERLAPPED = 1;
pub const WSA_FLAG_NO_HANDLE_INHERIT = 128;

const WSADESCRIPTION_LEN = 256;
const WSASYS_STATUS_LEN = 128;

pub const WSADATA = if (@sizeOf(usize) == @sizeOf(u64))
    extern struct {
        wVersion: windows.WORD,
        wHighVersion: windows.WORD,
        iMaxSockets: u16,
        iMaxUdpDg: u16,
        lpVendorInfo: *u8,
        szDescription: [WSADESCRIPTION_LEN + 1]u8,
        szSystemStatus: [WSASYS_STATUS_LEN + 1]u8,
    }
else
    extern struct {
        wVersion: windows.WORD,
        wHighVersion: windows.WORD,
        szDescription: [WSADESCRIPTION_LEN + 1]u8,
        szSystemStatus: [WSASYS_STATUS_LEN + 1]u8,
        iMaxSockets: u16,
        iMaxUdpDg: u16,
        lpVendorInfo: *u8,
    };

pub const WSABUF = extern struct {
    len: windows.ULONG,
    buf: [*]u8,
};

pub const WinsockError = enum(u16) {
    WSA_INVALID_HANDLE = 6,
    WSA_INVALID_PARAMETER = 87,
    WSA_OPERATION_ABORTED = 995,
    WSA_IO_INCOMPLETE = 996,
    WSA_IO_PENDING = 997,
    WSAEINTR = 10004,
    WSAEBADF = 10009,
    WSAEACCES = 10013,
    WSAEFAULT = 10014,
    WSAEINVAL = 10022,
    WSAEMFILE = 10024,
    WSAEWOULDBLOCK = 10035,
    WSAEINPROGRESS = 10036,
    WSAEALREADY = 10037,
    WSAENOTSOCK = 10038,
    WSAEDESTADDRREQ = 10039,
    WSAEMSGSIZE = 10040,
    WSAEPROTOTYPE = 10041,
    WSAENOPROTOOPT = 10042,
    WSAEPROTONOSUPPORT = 10043,
    WSAEOPNOTSUPP = 10045,
    WSAEAFNOSUPPORT = 10047,
    WSAEADDRINUSE = 10048,
    WSAEADDRNOTAVAIL = 10049,
    WSAENETDOWN = 10050,
    WSAENETUNREACH = 10051,
    WSAENETRESET = 10052,
    WSAECONNABORTED = 10053,
    WSAECONNRESET = 10054,
    WSAENOBUFS = 10055,
    WSAEISCONN = 10056,
    WSAENOTCONN = 10057,
    WSAESHUTDOWN = 10058,
    WSAETIMEDOUT = 10060,
    WSAECONNREFUSED = 10061,
    WSAEHOSTUNREACH = 10065,
    WSAEPROCLIM = 10067,
    WSASYSNOTREADY = 10091,
    WSAVERNOTSUPPORTED = 10092,
    WSANOTINITIALISED = 10093,
    WSAEDISCON = 10101,
    _,
};

pub extern "ws2_32" fn bind(
    s: SOCKET,
    name: *const sockaddr,
    namelen: i32,
) callconv(.winapi) i32;

pub extern "ws2_32" fn closesocket(
    s: SOCKET,
) callconv(.winapi) i32;

pub extern "ws2_32" fn connect(
    s: SOCKET,
    name: *const sockaddr,
    namelen: i32,
) callconv(.winapi) i32;

pub extern "ws2_32" fn getsockname(
    s: SOCKET,
    name: *sockaddr,
    namelen: *i32,
) callconv(.winapi) i32;

pub extern "ws2_32" fn getsockopt(
    s: SOCKET,
    level: i32,
    optname: i32,
    optval: [*]u8,
    optlen: *i32,
) callconv(.winapi) i32;

pub extern "ws2_32" fn listen(
    s: SOCKET,
    backlog: i32,
) callconv(.winapi) i32;

pub extern "ws2_32" fn setsockopt(
    s: SOCKET,
    level: i32,
    optname: i32,
    optval: ?[*]const u8,
    optlen: i32,
) callconv(.winapi) i32;

pub extern "ws2_32" fn shutdown(
    s: SOCKET,
    how: i32,
) callconv(.winapi) i32;

pub extern "ws2_32" fn WSAGetLastError() callconv(.winapi) WinsockError;

pub extern "ws2_32" fn WSAGetOverlappedResult(
    s: SOCKET,
    lpOverlapped: *OVERLAPPED,
    lpcbTransfer: *u32,
    fWait: windows.BOOL,
    lpdwFlags: *u32,
) callconv(.winapi) windows.BOOL;

pub extern "ws2_32" fn WSAIoctl(
    s: SOCKET,
    dwIoControlCode: u32,
    lpvInBuffer: ?*const anyopaque,
    cbInBuffer: u32,
    lpvOutbuffer: ?*anyopaque,
    cbOutbuffer: u32,
    lpcbBytesReturned: *u32,
    lpOverlapped: ?*OVERLAPPED,
    lpCompletionRoutine: ?*anyopaque,
) callconv(.winapi) i32;

pub extern "ws2_32" fn WSARecv(
    s: SOCKET,
    lpBuffers: [*]WSABUF,
    dwBufferCouynt: u32,
    lpNumberOfBytesRecv: ?*u32,
    lpFlags: *u32,
    lpOverlapped: ?*OVERLAPPED,
    lpCompletionRoutine: ?*anyopaque,
) callconv(.winapi) i32;

pub extern "ws2_32" fn WSASend(
    s: SOCKET,
    lpBuffers: [*]WSABUF,
    dwBufferCount: u32,
    lpNumberOfBytesSent: ?*u32,
    dwFlags: u32,
    lpOverlapped: ?*OVERLAPPED,
    lpCompletionRoutine: ?*anyopaque,
) callconv(.winapi) i32;

pub extern "ws2_32" fn WSASendTo(
    s: SOCKET,
    lpBuffers: [*]WSABUF,
    dwBufferCount: u32,
    lpNumberOfBytesSent: ?*u32,
    dwFlags: u32,
    lpTo: ?*const sockaddr,
    iToLen: i32,
    lpOverlapped: ?*OVERLAPPED,
    lpCompletionRounte: ?*anyopaque,
) callconv(.winapi) i32;

pub extern "mswsock" fn AcceptEx(
    sListenSocket: SOCKET,
    sAcceptSocket: SOCKET,
    lpOutputBuffer: *anyopaque,
    dwReceiveDataLength: u32,
    dwLocalAddressLength: u32,
    dwRemoteAddressLength: u32,
    lpdwBytesReceived: *u32,
    lpOverlapped: *OVERLAPPED,
) callconv(.winapi) windows.BOOL;

/// The raw functions behind the wrappers of the same name above.
const externs = struct {
    extern "kernel32" fn CreateIoCompletionPort(
        FileHandle: windows.HANDLE,
        ExistingCompletionPort: ?windows.HANDLE,
        CompletionKey: windows.ULONG_PTR,
        NumberOfConcurrentThreads: windows.DWORD,
    ) callconv(.winapi) ?windows.HANDLE;

    extern "kernel32" fn GetFileSizeEx(
        hFile: windows.HANDLE,
        lpFileSize: *windows.LARGE_INTEGER,
    ) callconv(.winapi) windows.BOOL;

    extern "kernel32" fn GetQueuedCompletionStatusEx(
        CompletionPort: windows.HANDLE,
        lpCompletionPortEntries: [*]OVERLAPPED_ENTRY,
        ulCount: windows.ULONG,
        ulNumEntriesRemoved: *windows.ULONG,
        dwMilliseconds: windows.DWORD,
        fAlertable: windows.BOOL,
    ) callconv(.winapi) windows.BOOL;

    extern "kernel32" fn PostQueuedCompletionStatus(
        CompletionPort: windows.HANDLE,
        dwNumberOfBytesTransferred: windows.DWORD,
        dwCompletionKey: windows.ULONG_PTR,
        lpOverlapped: ?*OVERLAPPED,
    ) callconv(.winapi) windows.BOOL;

    extern "kernel32" fn SetFileCompletionNotificationModes(
        FileHandle: windows.HANDLE,
        Flags: windows.UCHAR,
    ) callconv(.winapi) windows.BOOL;

    extern "ws2_32" fn WSAStartup(
        wVersionRequired: windows.WORD,
        lpWSAData: *WSADATA,
    ) callconv(.winapi) i32;

    extern "ws2_32" fn WSACleanup() callconv(.winapi) i32;

    extern "ws2_32" fn WSASocketW(
        af: i32,
        @"type": i32,
        protocol: i32,
        lpProtocolInfo: ?*anyopaque,
        g: u32,
        dwFlags: u32,
    ) callconv(.winapi) SOCKET;
};
