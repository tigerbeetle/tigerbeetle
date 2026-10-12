//! The subset of Zig 0.14's `std.posix` that TigerBeetle uses.
//!
//! Zig 0.16 removed most of the `std.posix` syscall wrappers in favor of `std.Io`, but
//! TigerBeetle's IO implementations talk to the kernel directly. Declarations which are still part
//! of `std.posix` are re-exported as is, the removed wrappers are vendored from Zig 0.14.1
//! (restricted to Linux, macOS, and Windows where TigerBeetle uses them there).

const std = @import("std");
const builtin = @import("builtin");
const stdx = @import("stdx.zig");
const assert = std.debug.assert;
const maxInt = std.math.maxInt;

const native_os = builtin.os.tag;
const linux = std.os.linux;
const windows = std.os.windows;

pub const AF = std.posix.AF;
pub const AT = std.posix.AT;
pub const E = std.posix.E;
pub const F = std.posix.F;
pub const FD_CLOEXEC = std.posix.FD_CLOEXEC;
pub const IPPROTO = std.posix.IPPROTO;
pub const Kevent = std.posix.Kevent;
pub const LOCK = std.posix.LOCK;
pub const MSG = std.posix.MSG;
pub const O = std.posix.O;
pub const OpenError = std.posix.OpenError;
pub const S = std.posix.S;
pub const SEEK = std.posix.SEEK;
pub const SHUT = std.posix.SHUT;
pub const SO = std.posix.SO;
pub const SOCK = std.posix.SOCK;
pub const SOL = std.posix.SOL;
pub const TCP = std.posix.TCP;
pub const UnexpectedError = std.posix.UnexpectedError;
pub const errno = std.posix.errno;
pub const fd_t = std.posix.fd_t;
pub const mode_t = std.posix.mode_t;
pub const off_t = std.posix.off_t;
pub const openat = std.posix.openat;
pub const openatZ = std.posix.openatZ;
pub const read = std.posix.read;
pub const sockaddr = std.posix.sockaddr;
pub const socket_t = std.posix.socket_t;
pub const socklen_t = std.posix.socklen_t;
pub const system = std.posix.system;
pub const timespec = std.posix.timespec;

/// Zig 0.16 removed the `stat` family on Linux in favor of `statx`.
pub const Stat = if (native_os == .linux) linux.Statx else std.posix.Stat;

/// Closes the file descriptor. Asserts the file descriptor is open.
pub fn close(fd: fd_t) void {
    if (native_os == .windows) {
        return windows.CloseHandle(fd);
    }
    switch (errno(system.close(fd))) {
        .BADF => unreachable, // Always a race condition.
        .INTR => return, // This is still a success. See https://github.com/ziglang/zig/issues/2425
        else => return,
    }
}

/// Equivalent to `openat(AT.FDCWD, file_path, flags, perm)`.
pub fn open(file_path: []const u8, flags: O, perm: mode_t) OpenError!fd_t {
    return openat(AT.FDCWD, file_path, flags, perm);
}

pub const TruncateError = error{
    FileTooBig,
    InputOutput,
    FileBusy,
    AccessDenied,
} || UnexpectedError;

pub fn ftruncate(fd: fd_t, length: u64) TruncateError!void {
    const ftruncate_sym = if (std.posix.lfs64_abi) system.ftruncate64 else system.ftruncate;
    while (true) {
        switch (errno(ftruncate_sym(fd, @bitCast(length)))) {
            .SUCCESS => return,
            .INTR => continue,
            .FBIG => return error.FileTooBig,
            .IO => return error.InputOutput,
            .PERM => return error.AccessDenied,
            .TXTBSY => return error.FileBusy,
            .BADF => unreachable, // Handle not open for writing
            .INVAL => unreachable, // Handle not open for writing
            else => |err| return stdx.unexpected_errno("ftruncate", err),
        }
    }
}

pub const WriteError = error{
    DiskQuota,
    FileTooBig,
    InputOutput,
    NoSpaceLeft,
    DeviceBusy,
    InvalidArgument,
    AccessDenied,
    BrokenPipe,
    SystemResources,
    OperationAborted,
    NotOpenForWriting,
    LockViolation,
    WouldBlock,
    ConnectionResetByPeer,
    ProcessNotFound,
    NoDevice,
} || UnexpectedError;

/// Prevent EINVAL.
const write_count_max = switch (native_os) {
    .linux => 0x7ffff000,
    .macos, .ios, .watchos, .tvos, .visionos => maxInt(i32),
    else => maxInt(isize),
};

pub fn write(fd: fd_t, bytes: []const u8) WriteError!usize {
    if (bytes.len == 0) return 0;
    while (true) {
        const rc = system.write(fd, bytes.ptr, @min(bytes.len, write_count_max));
        switch (errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            .INVAL => return error.InvalidArgument,
            .FAULT => unreachable,
            .NOENT => return error.ProcessNotFound,
            .AGAIN => return error.WouldBlock,
            .BADF => return error.NotOpenForWriting, // can be a race condition.
            .DESTADDRREQ => unreachable, // `connect` was never called.
            .DQUOT => return error.DiskQuota,
            .FBIG => return error.FileTooBig,
            .IO => return error.InputOutput,
            .NOSPC => return error.NoSpaceLeft,
            .ACCES => return error.AccessDenied,
            .PERM => return error.AccessDenied,
            .PIPE => return error.BrokenPipe,
            .CONNRESET => return error.ConnectionResetByPeer,
            .BUSY => return error.DeviceBusy,
            .NXIO => return error.NoDevice,
            else => |err| return stdx.unexpected_errno("write", err),
        }
    }
}

pub const PWriteError = WriteError || error{Unseekable};

pub fn pwrite(fd: fd_t, bytes: []const u8, offset: u64) PWriteError!usize {
    if (bytes.len == 0) return 0;
    if (native_os == .windows) {
        return stdx.windows.write_file_blocking(fd, bytes, offset);
    }
    const pwrite_sym = if (std.posix.lfs64_abi) system.pwrite64 else system.pwrite;
    while (true) {
        const rc = pwrite_sym(fd, bytes.ptr, @min(bytes.len, write_count_max), @bitCast(offset));
        switch (errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            .INVAL => return error.InvalidArgument,
            .FAULT => unreachable,
            .NOENT => return error.ProcessNotFound,
            .AGAIN => return error.WouldBlock,
            .BADF => return error.NotOpenForWriting, // Can be a race condition.
            .DESTADDRREQ => unreachable, // `connect` was never called.
            .DQUOT => return error.DiskQuota,
            .FBIG => return error.FileTooBig,
            .IO => return error.InputOutput,
            .NOSPC => return error.NoSpaceLeft,
            .PERM => return error.AccessDenied,
            .PIPE => return error.BrokenPipe,
            .NXIO => return error.Unseekable,
            .SPIPE => return error.Unseekable,
            .OVERFLOW => return error.Unseekable,
            .BUSY => return error.DeviceBusy,
            else => |err| return stdx.unexpected_errno("pwrite", err),
        }
    }
}

pub const SyncError = error{
    InputOutput,
    NoSpaceLeft,
    DiskQuota,
    AccessDenied,
} || UnexpectedError;

pub fn fsync(fd: fd_t) SyncError!void {
    if (native_os == .windows) {
        // Zig 0.14 used `FlushFileBuffers`, which is a thin wrapper around `NtFlushBuffersFile`.
        var io_status_block: windows.IO_STATUS_BLOCK = undefined;
        switch (windows.ntdll.NtFlushBuffersFile(fd, &io_status_block)) {
            .SUCCESS => return,
            .INVALID_HANDLE => unreachable,
            .ACCESS_DENIED => return error.AccessDenied, // The sync was performed, but not atime.
            .UNEXPECTED_NETWORK_ERROR => return error.InputOutput,
            else => return error.InputOutput,
        }
    }
    const rc = system.fsync(fd);
    switch (errno(rc)) {
        .SUCCESS => return,
        .BADF, .INVAL, .ROFS => unreachable,
        .IO => return error.InputOutput,
        .NOSPC => return error.NoSpaceLeft,
        .DQUOT => return error.DiskQuota,
        else => |err| return stdx.unexpected_errno("fsync", err),
    }
}

pub const FStatError = error{
    SystemResources,
    AccessDenied,
} || UnexpectedError;

pub fn fstat(fd: fd_t) FStatError!Stat {
    var stat = std.mem.zeroes(Stat);
    const rc = if (native_os == .linux)
        linux.statx(fd, "", linux.AT.EMPTY_PATH, linux.STATX.BASIC_STATS, &stat)
    else
        system.fstat(fd, &stat);
    const err = if (native_os == .linux) linux.errno(rc) else errno(rc);
    switch (err) {
        .SUCCESS => return stat,
        .INVAL => unreachable,
        .BADF => unreachable, // Always a race condition.
        .NOMEM => return error.SystemResources,
        .ACCES => return error.AccessDenied,
        else => return stdx.unexpected_errno("fstat", err),
    }
}

pub const FStatAtError = FStatError || error{
    NameTooLong,
    FileNotFound,
    SymLinkLoop,
};

pub fn fstatat(dirfd: fd_t, pathname: []const u8, flags: u32) FStatAtError!Stat {
    const pathname_c = try std.posix.toPosixPath(pathname);
    var stat = std.mem.zeroes(Stat);
    const rc = if (native_os == .linux)
        linux.statx(dirfd, &pathname_c, flags, linux.STATX.BASIC_STATS, &stat)
    else
        system.fstatat(dirfd, &pathname_c, &stat, flags);
    const err = if (native_os == .linux) linux.errno(rc) else errno(rc);
    switch (err) {
        .SUCCESS => return stat,
        .INVAL => unreachable,
        .BADF => unreachable, // Always a race condition.
        .NOMEM => return error.SystemResources,
        .ACCES => return error.AccessDenied,
        .PERM => return error.AccessDenied,
        .FAULT => unreachable,
        .NAMETOOLONG => return error.NameTooLong,
        .LOOP => return error.SymLinkLoop,
        .NOENT => return error.FileNotFound,
        .NOTDIR => return error.FileNotFound,
        else => return stdx.unexpected_errno("fstatat", err),
    }
}

pub const SeekError = error{
    Unseekable,
    AccessDenied,
} || UnexpectedError;

/// Repositions read/write file offset relative to the current offset.
pub fn lseek_CUR(fd: fd_t, offset: i64) SeekError!void {
    _ = try lseek_cur(fd, offset);
}

/// Returns the read/write file offset relative to the beginning.
pub fn lseek_CUR_get(fd: fd_t) SeekError!u64 {
    return lseek_cur(fd, 0);
}

fn lseek_cur(fd: fd_t, offset: i64) SeekError!u64 {
    comptime assert(@sizeOf(usize) == 8);
    const lseek_sym = if (std.posix.lfs64_abi) system.lseek64 else system.lseek;
    const rc = lseek_sym(fd, @bitCast(offset), SEEK.CUR);
    switch (errno(rc)) {
        .SUCCESS => return @bitCast(rc),
        .BADF => unreachable, // always a race condition
        .INVAL => return error.Unseekable,
        .OVERFLOW => return error.Unseekable,
        .SPIPE => return error.Unseekable,
        .NXIO => return error.Unseekable,
        else => |err| return stdx.unexpected_errno("lseek", err),
    }
}

pub const FcntlError = error{
    PermissionDenied,
    FileBusy,
    ProcessFdQuotaExceeded,
    Locked,
    DeadLock,
    LockedRegionLimitExceeded,
} || UnexpectedError;

pub fn fcntl(fd: fd_t, cmd: i32, arg: usize) FcntlError!usize {
    while (true) {
        const rc = system.fcntl(fd, cmd, arg);
        switch (errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            .AGAIN, .ACCES => return error.Locked,
            .BADF => unreachable,
            .BUSY => return error.FileBusy,
            .INVAL => unreachable, // invalid parameters
            .PERM => return error.PermissionDenied,
            .MFILE => return error.ProcessFdQuotaExceeded,
            .NOTDIR => unreachable, // invalid parameter
            .DEADLK => return error.DeadLock,
            .NOLCK => return error.LockedRegionLimitExceeded,
            else => |err| return stdx.unexpected_errno("fcntl", err),
        }
    }
}

pub const FlockError = error{
    WouldBlock,
    /// The kernel ran out of memory for allocating file locks
    SystemResources,
    /// The underlying filesystem does not support file locks
    FileLocksNotSupported,
} || UnexpectedError;

pub fn flock(fd: fd_t, operation: i32) FlockError!void {
    while (true) {
        const rc = system.flock(fd, operation);
        switch (errno(rc)) {
            .SUCCESS => return,
            .BADF => unreachable,
            .INTR => continue,
            .INVAL => unreachable, // invalid parameters
            .NOLCK => return error.SystemResources,
            .AGAIN => return error.WouldBlock,
            .OPNOTSUPP => return error.FileLocksNotSupported,
            else => |err| return stdx.unexpected_errno("flock", err),
        }
    }
}

pub const ExecveError = error{
    SystemResources,
    AccessDenied,
    InvalidExe,
    FileSystem,
    IsDir,
    FileNotFound,
    NotDir,
    FileBusy,
    ProcessFdQuotaExceeded,
    SystemFdQuotaExceeded,
    NameTooLong,
} || UnexpectedError;

/// This function ignores PATH environment variable.
pub fn execveZ(
    path: [*:0]const u8,
    child_argv: [*:null]const ?[*:0]const u8,
    envp: [*:null]const ?[*:0]const u8,
) ExecveError {
    switch (errno(system.execve(path, child_argv, envp))) {
        .SUCCESS => unreachable,
        .FAULT => unreachable,
        .@"2BIG" => return error.SystemResources,
        .MFILE => return error.ProcessFdQuotaExceeded,
        .NAMETOOLONG => return error.NameTooLong,
        .NFILE => return error.SystemFdQuotaExceeded,
        .NOMEM => return error.SystemResources,
        .ACCES => return error.AccessDenied,
        .PERM => return error.AccessDenied,
        .INVAL => return error.InvalidExe,
        .NOEXEC => return error.InvalidExe,
        .IO => return error.FileSystem,
        .LOOP => return error.FileSystem,
        .ISDIR => return error.IsDir,
        .NOENT => return error.FileNotFound,
        .NOTDIR => return error.NotDir,
        .TXTBSY => return error.FileBusy,
        else => |err| switch (native_os) {
            .macos => switch (err) {
                .BADEXEC => return error.InvalidExe,
                .BADARCH => return error.InvalidExe,
                else => return stdx.unexpected_errno("execve", err),
            },
            .linux => switch (err) {
                .LIBBAD => return error.InvalidExe,
                else => return stdx.unexpected_errno("execve", err),
            },
            else => return stdx.unexpected_errno("execve", err),
        },
    }
}

pub const EventFdError = error{
    SystemResources,
    ProcessFdQuotaExceeded,
    SystemFdQuotaExceeded,
} || UnexpectedError;

pub fn eventfd(initval: u32, flags: u32) EventFdError!i32 {
    const rc = system.eventfd(initval, flags);
    switch (errno(rc)) {
        .SUCCESS => return @intCast(rc),
        .INVAL => unreachable, // invalid parameters
        .MFILE => return error.ProcessFdQuotaExceeded,
        .NFILE => return error.SystemFdQuotaExceeded,
        .NODEV => return error.SystemResources,
        .NOMEM => return error.SystemResources,
        else => |err| return stdx.unexpected_errno("eventfd", err),
    }
}

pub const SocketError = error{
    PermissionDenied,
    AddressFamilyNotSupported,
    ProtocolFamilyNotAvailable,
    ProcessFdQuotaExceeded,
    SystemFdQuotaExceeded,
    SystemResources,
    ProtocolNotSupported,
    SocketTypeNotSupported,
} || UnexpectedError;

pub fn socket(domain: u32, socket_type: u32, protocol: u32) SocketError!socket_t {
    const have_sock_flags = !native_os.isDarwin();
    const filtered_sock_type = if (!have_sock_flags)
        socket_type & ~@as(u32, SOCK.NONBLOCK | SOCK.CLOEXEC)
    else
        socket_type;
    const rc = system.socket(domain, filtered_sock_type, protocol);
    switch (errno(rc)) {
        .SUCCESS => {
            const fd: fd_t = @intCast(rc);
            errdefer close(fd);
            if (!have_sock_flags) {
                try set_sock_flags(fd, socket_type);
            }
            return fd;
        },
        .ACCES => return error.PermissionDenied,
        .AFNOSUPPORT => return error.AddressFamilyNotSupported,
        .INVAL => return error.ProtocolFamilyNotAvailable,
        .MFILE => return error.ProcessFdQuotaExceeded,
        .NFILE => return error.SystemFdQuotaExceeded,
        .NOBUFS => return error.SystemResources,
        .NOMEM => return error.SystemResources,
        .PROTONOSUPPORT => return error.ProtocolNotSupported,
        .PROTOTYPE => return error.SocketTypeNotSupported,
        else => |err| return stdx.unexpected_errno("socket", err),
    }
}

/// Emulates `SOCK.NONBLOCK` and `SOCK.CLOEXEC` on Darwin.
fn set_sock_flags(sock: socket_t, flags: u32) !void {
    if ((flags & SOCK.CLOEXEC) != 0) {
        var fd_flags = fcntl(sock, F.GETFD, 0) catch |err| switch (err) {
            error.FileBusy => unreachable,
            error.Locked => unreachable,
            error.PermissionDenied => unreachable,
            error.DeadLock => unreachable,
            error.LockedRegionLimitExceeded => unreachable,
            else => |e| return e,
        };
        fd_flags |= FD_CLOEXEC;
        _ = fcntl(sock, F.SETFD, fd_flags) catch |err| switch (err) {
            error.FileBusy => unreachable,
            error.Locked => unreachable,
            error.PermissionDenied => unreachable,
            error.DeadLock => unreachable,
            error.LockedRegionLimitExceeded => unreachable,
            else => |e| return e,
        };
    }
    if ((flags & SOCK.NONBLOCK) != 0) {
        var fl_flags = fcntl(sock, F.GETFL, 0) catch |err| switch (err) {
            error.FileBusy => unreachable,
            error.Locked => unreachable,
            error.PermissionDenied => unreachable,
            error.DeadLock => unreachable,
            error.LockedRegionLimitExceeded => unreachable,
            else => |e| return e,
        };
        fl_flags |= 1 << @bitOffsetOf(O, "NONBLOCK");
        _ = fcntl(sock, F.SETFL, fl_flags) catch |err| switch (err) {
            error.FileBusy => unreachable,
            error.Locked => unreachable,
            error.PermissionDenied => unreachable,
            error.DeadLock => unreachable,
            error.LockedRegionLimitExceeded => unreachable,
            else => |e| return e,
        };
    }
}

pub const ShutdownError = error{
    ConnectionAborted,
    /// Connection was reset by peer, application should close socket as it is no longer usable.
    ConnectionResetByPeer,
    BlockingOperationInProgress,
    /// The network subsystem has failed.
    NetworkSubsystemFailed,
    /// The socket is not connected (connection-oriented sockets only).
    SocketNotConnected,
    SystemResources,
} || UnexpectedError;

pub const ShutdownHow = enum { recv, send, both };

/// Shutdown socket send/receive operations
pub fn shutdown(sock: socket_t, how: ShutdownHow) ShutdownError!void {
    if (native_os == .windows) {
        const result = stdx.windows.shutdown(@ptrCast(sock), switch (how) {
            .recv => @as(c_int, 0), // SD_RECEIVE
            .send => @as(c_int, 1), // SD_SEND
            .both => @as(c_int, 2), // SD_BOTH
        });
        if (0 != result) switch (stdx.windows.WSAGetLastError()) {
            .WSAECONNABORTED => return error.ConnectionAborted,
            .WSAECONNRESET => return error.ConnectionResetByPeer,
            .WSAEINPROGRESS => return error.BlockingOperationInProgress,
            .WSAEINVAL => unreachable,
            .WSAENETDOWN => return error.NetworkSubsystemFailed,
            .WSAENOTCONN => return error.SocketNotConnected,
            .WSAENOTSOCK => unreachable,
            .WSANOTINITIALISED => unreachable,
            else => |err| return stdx.windows.unexpectedWSAError(err),
        };
    } else {
        const rc = system.shutdown(sock, switch (how) {
            .recv => SHUT.RD,
            .send => SHUT.WR,
            .both => SHUT.RDWR,
        });
        switch (errno(rc)) {
            .SUCCESS => return,
            .BADF => unreachable,
            .INVAL => unreachable,
            .NOTCONN => return error.SocketNotConnected,
            .NOTSOCK => unreachable,
            .NOBUFS => return error.SystemResources,
            else => |err| return stdx.unexpected_errno("shutdown", err),
        }
    }
}

pub const BindError = error{
    AccessDenied,
    AddressInUse,
    AddressNotAvailable,
    AddressFamilyNotSupported,
    SymLinkLoop,
    NameTooLong,
    FileNotFound,
    SystemResources,
    NotDir,
    ReadOnlyFileSystem,
    NetworkSubsystemFailed,
    FileDescriptorNotASocket,
    AlreadyBound,
} || UnexpectedError;

pub fn bind(sock: socket_t, addr: *const sockaddr, len: socklen_t) BindError!void {
    if (native_os == .windows) {
        const rc = stdx.windows.bind(@ptrCast(sock), @ptrCast(addr), @intCast(len));
        if (rc == stdx.windows.SOCKET_ERROR) {
            switch (stdx.windows.WSAGetLastError()) {
                .WSANOTINITIALISED => unreachable, // not initialized WSA
                .WSAEACCES => return error.AccessDenied,
                .WSAEADDRINUSE => return error.AddressInUse,
                .WSAEADDRNOTAVAIL => return error.AddressNotAvailable,
                .WSAENOTSOCK => return error.FileDescriptorNotASocket,
                .WSAEFAULT => unreachable, // invalid pointers
                .WSAEINVAL => return error.AlreadyBound,
                .WSAENOBUFS => return error.SystemResources,
                .WSAENETDOWN => return error.NetworkSubsystemFailed,
                else => |err| return stdx.windows.unexpectedWSAError(err),
            }
        }
    } else {
        const rc = system.bind(sock, addr, len);
        switch (errno(rc)) {
            .SUCCESS => return,
            .ACCES, .PERM => return error.AccessDenied,
            .ADDRINUSE => return error.AddressInUse,
            .BADF => unreachable, // always a race condition if this error is returned
            .INVAL => unreachable, // invalid parameters
            .NOTSOCK => unreachable, // invalid `sockfd`
            .AFNOSUPPORT => return error.AddressFamilyNotSupported,
            .ADDRNOTAVAIL => return error.AddressNotAvailable,
            .FAULT => unreachable, // invalid `addr` pointer
            .LOOP => return error.SymLinkLoop,
            .NAMETOOLONG => return error.NameTooLong,
            .NOENT => return error.FileNotFound,
            .NOMEM => return error.SystemResources,
            .NOTDIR => return error.NotDir,
            .ROFS => return error.ReadOnlyFileSystem,
            else => |err| return stdx.unexpected_errno("bind", err),
        }
    }
}

pub const ListenError = error{
    AddressInUse,
    FileDescriptorNotASocket,
    OperationNotSupported,
    NetworkSubsystemFailed,
    SystemResources,
    AlreadyConnected,
    SocketNotBound,
} || UnexpectedError;

pub fn listen(sock: socket_t, backlog: u31) ListenError!void {
    if (native_os == .windows) {
        const rc = stdx.windows.listen(@ptrCast(sock), backlog);
        if (rc == stdx.windows.SOCKET_ERROR) {
            switch (stdx.windows.WSAGetLastError()) {
                .WSANOTINITIALISED => unreachable, // not initialized WSA
                .WSAENETDOWN => return error.NetworkSubsystemFailed,
                .WSAEADDRINUSE => return error.AddressInUse,
                .WSAEISCONN => return error.AlreadyConnected,
                .WSAEINVAL => return error.SocketNotBound,
                .WSAEMFILE, .WSAENOBUFS => return error.SystemResources,
                .WSAENOTSOCK => return error.FileDescriptorNotASocket,
                .WSAEOPNOTSUPP => return error.OperationNotSupported,
                .WSAEINPROGRESS => unreachable,
                else => |err| return stdx.windows.unexpectedWSAError(err),
            }
        }
    } else {
        const rc = system.listen(sock, backlog);
        switch (errno(rc)) {
            .SUCCESS => return,
            .ADDRINUSE => return error.AddressInUse,
            .BADF => unreachable,
            .NOTSOCK => return error.FileDescriptorNotASocket,
            .OPNOTSUPP => return error.OperationNotSupported,
            else => |err| return stdx.unexpected_errno("listen", err),
        }
    }
}

pub const AcceptError = error{
    ConnectionAborted,
    FileDescriptorNotASocket,
    ProcessFdQuotaExceeded,
    SystemFdQuotaExceeded,
    SystemResources,
    SocketNotListening,
    ProtocolFailure,
    BlockedByFirewall,
    WouldBlock,
    ConnectionResetByPeer,
    NetworkSubsystemFailed,
    OperationNotSupported,
} || UnexpectedError;

/// Accept a connection on a socket. `flags` may contain `SOCK.NONBLOCK` and `SOCK.CLOEXEC`.
pub fn accept(
    sock: socket_t,
    addr: ?*sockaddr,
    addr_size: ?*socklen_t,
    flags: u32,
) AcceptError!socket_t {
    const have_accept4 = !native_os.isDarwin();
    assert(0 == (flags & ~@as(u32, SOCK.NONBLOCK | SOCK.CLOEXEC))); // Unsupported flag(s)

    const accepted_sock: socket_t = while (true) {
        const rc = if (have_accept4)
            system.accept4(sock, addr, addr_size, flags)
        else
            system.accept(sock, addr, addr_size);

        switch (errno(rc)) {
            .SUCCESS => break @intCast(rc),
            .INTR => continue,
            .AGAIN => return error.WouldBlock,
            .BADF => unreachable, // always a race condition
            .CONNABORTED => return error.ConnectionAborted,
            .FAULT => unreachable,
            .INVAL => return error.SocketNotListening,
            .NOTSOCK => unreachable,
            .MFILE => return error.ProcessFdQuotaExceeded,
            .NFILE => return error.SystemFdQuotaExceeded,
            .NOBUFS => return error.SystemResources,
            .NOMEM => return error.SystemResources,
            .OPNOTSUPP => unreachable,
            .PROTO => return error.ProtocolFailure,
            .PERM => return error.BlockedByFirewall,
            else => |err| return stdx.unexpected_errno("accept", err),
        }
    };

    errdefer close(accepted_sock);
    if (!have_accept4) {
        try set_sock_flags(accepted_sock, flags);
    }
    return accepted_sock;
}

pub const GetSockNameError = error{
    SystemResources,
    NetworkSubsystemFailed,
    SocketNotBound,
    FileDescriptorNotASocket,
} || UnexpectedError;

pub fn getsockname(sock: socket_t, addr: *sockaddr, addrlen: *socklen_t) GetSockNameError!void {
    if (native_os == .windows) {
        var addrlen_windows: c_int = @intCast(addrlen.*);
        defer addrlen.* = @intCast(addrlen_windows);

        const rc = stdx.windows.getsockname(@ptrCast(sock), @ptrCast(addr), &addrlen_windows);
        if (rc == stdx.windows.SOCKET_ERROR) {
            switch (stdx.windows.WSAGetLastError()) {
                .WSANOTINITIALISED => unreachable,
                .WSAENETDOWN => return error.NetworkSubsystemFailed,
                .WSAEFAULT => unreachable, // addr or addrlen have invalid pointers
                .WSAENOTSOCK => return error.FileDescriptorNotASocket,
                .WSAEINVAL => return error.SocketNotBound,
                else => |err| return stdx.windows.unexpectedWSAError(err),
            }
        }
    } else {
        const rc = system.getsockname(sock, addr, addrlen);
        switch (errno(rc)) {
            .SUCCESS => return,
            .BADF => unreachable, // always a race condition
            .FAULT => unreachable,
            .INVAL => unreachable, // invalid parameters
            .NOTSOCK => return error.FileDescriptorNotASocket,
            .NOBUFS => return error.SystemResources,
            else => |err| return stdx.unexpected_errno("getsockname", err),
        }
    }
}

pub const ConnectError = error{
    PermissionDenied,
    AddressInUse,
    AddressNotAvailable,
    AddressFamilyNotSupported,
    SystemResources,
    ConnectionRefused,
    NetworkUnreachable,
    ConnectionTimedOut,
    WouldBlock,
    FileNotFound,
    ConnectionResetByPeer,
    ConnectionPending,
} || UnexpectedError;

/// Initiate a connection on a socket.
/// If `sock` is opened in non blocking mode, the function will
/// return error.WouldBlock when EAGAIN or EINPROGRESS is received.
pub fn connect(sock: socket_t, sock_addr: *const sockaddr, len: socklen_t) ConnectError!void {
    if (native_os == .windows) {
        const rc = stdx.windows.connect(@ptrCast(sock), @ptrCast(sock_addr), @intCast(len));
        if (rc == 0) return;
        switch (stdx.windows.WSAGetLastError()) {
            .WSAEADDRINUSE => return error.AddressInUse,
            .WSAEADDRNOTAVAIL => return error.AddressNotAvailable,
            .WSAECONNREFUSED => return error.ConnectionRefused,
            .WSAECONNRESET => return error.ConnectionResetByPeer,
            .WSAETIMEDOUT => return error.ConnectionTimedOut,
            .WSAEHOSTUNREACH, .WSAENETUNREACH => return error.NetworkUnreachable,
            .WSAEFAULT => unreachable,
            .WSAEINVAL => unreachable,
            .WSAEISCONN => unreachable,
            .WSAENOTSOCK => unreachable,
            .WSAEWOULDBLOCK => return error.WouldBlock,
            .WSAEACCES => unreachable,
            .WSAENOBUFS => return error.SystemResources,
            .WSAEAFNOSUPPORT => return error.AddressFamilyNotSupported,
            else => |err| return stdx.windows.unexpectedWSAError(err),
        }
    }

    while (true) {
        switch (errno(system.connect(sock, sock_addr, len))) {
            .SUCCESS => return,
            .ACCES => return error.PermissionDenied,
            .PERM => return error.PermissionDenied,
            .ADDRINUSE => return error.AddressInUse,
            .ADDRNOTAVAIL => return error.AddressNotAvailable,
            .AFNOSUPPORT => return error.AddressFamilyNotSupported,
            .AGAIN, .INPROGRESS => return error.WouldBlock,
            .ALREADY => return error.ConnectionPending,
            .BADF => unreachable, // sockfd is not a valid open file descriptor.
            .CONNREFUSED => return error.ConnectionRefused,
            .CONNRESET => return error.ConnectionResetByPeer,
            .FAULT => unreachable, // The socket address is outside the user's address space.
            .INTR => continue,
            .ISCONN => unreachable, // The socket is already connected.
            .HOSTUNREACH => return error.NetworkUnreachable,
            .NETUNREACH => return error.NetworkUnreachable,
            .NOTSOCK => unreachable, // The file descriptor sockfd does not refer to a socket.
            .PROTOTYPE => unreachable, // The socket type does not support the protocol.
            .TIMEDOUT => return error.ConnectionTimedOut,
            .NOENT => return error.FileNotFound, // AF.UNIX socket path does not exist.
            .CONNABORTED => unreachable, // Reused socket that received ConnectionRefused.
            else => |err| return stdx.unexpected_errno("connect", err),
        }
    }
}

/// Returns the result of a non-blocking `connect`.
pub fn getsockoptError(sockfd: fd_t) ConnectError!void {
    var err_code: i32 = undefined;
    var size: u32 = @sizeOf(u32);
    const rc = system.getsockopt(sockfd, SOL.SOCKET, SO.ERROR, @ptrCast(&err_code), &size);
    assert(size == 4);
    switch (errno(rc)) {
        .SUCCESS => switch (@as(E, @enumFromInt(err_code))) {
            .SUCCESS => return,
            .ACCES => return error.PermissionDenied,
            .PERM => return error.PermissionDenied,
            .ADDRINUSE => return error.AddressInUse,
            .ADDRNOTAVAIL => return error.AddressNotAvailable,
            .AFNOSUPPORT => return error.AddressFamilyNotSupported,
            .AGAIN => return error.SystemResources,
            .ALREADY => return error.ConnectionPending,
            .BADF => unreachable, // sockfd is not a valid open file descriptor.
            .CONNREFUSED => return error.ConnectionRefused,
            .FAULT => unreachable, // The socket address is outside the user's address space.
            .ISCONN => unreachable, // The socket is already connected.
            .HOSTUNREACH => return error.NetworkUnreachable,
            .NETUNREACH => return error.NetworkUnreachable,
            .NOTSOCK => unreachable, // The file descriptor sockfd does not refer to a socket.
            .PROTOTYPE => unreachable, // The socket type does not support the protocol.
            .TIMEDOUT => return error.ConnectionTimedOut,
            .CONNRESET => return error.ConnectionResetByPeer,
            else => |err| return stdx.unexpected_errno("getsockopt", err),
        },
        .BADF => unreachable, // The argument sockfd is not a valid file descriptor.
        .FAULT => unreachable, // optval or optlen is not in a valid part of the address space.
        .INVAL => unreachable,
        .NOPROTOOPT => unreachable, // The option is unknown at the level indicated.
        .NOTSOCK => unreachable, // The file descriptor sockfd does not refer to a socket.
        else => |err| return stdx.unexpected_errno("getsockopt", err),
    }
}

pub const GetSockOptError = error{
    /// The calling process does not have the appropriate privileges.
    AccessDenied,
    /// The option is not supported by the protocol.
    InvalidProtocolOption,
    /// Insufficient resources are available in the system to complete the call.
    SystemResources,
} || UnexpectedError;

pub const SetSockOptError = error{
    AlreadyConnected,
    InvalidProtocolOption,
    TimeoutTooBig,
    SystemResources,
    PermissionDenied,
    OperationNotSupported,
    NetworkSubsystemFailed,
    FileDescriptorNotASocket,
    SocketNotBound,
    NoDevice,
} || UnexpectedError;

pub fn setsockopt(fd: socket_t, level: i32, optname: u32, opt: []const u8) SetSockOptError!void {
    if (native_os == .windows) {
        const rc = stdx.windows.setsockopt(
            @ptrCast(fd),
            level,
            @intCast(optname),
            opt.ptr,
            @intCast(opt.len),
        );
        if (rc == stdx.windows.SOCKET_ERROR) {
            switch (stdx.windows.WSAGetLastError()) {
                .WSANOTINITIALISED => unreachable,
                .WSAENETDOWN => return error.NetworkSubsystemFailed,
                .WSAEFAULT => unreachable,
                .WSAENOTSOCK => return error.FileDescriptorNotASocket,
                .WSAEINVAL => return error.SocketNotBound,
                else => |err| return stdx.windows.unexpectedWSAError(err),
            }
        }
    } else {
        switch (errno(system.setsockopt(fd, level, optname, opt.ptr, @intCast(opt.len)))) {
            .SUCCESS => {},
            .BADF => unreachable, // always a race condition
            .NOTSOCK => unreachable, // always a race condition
            .INVAL => unreachable,
            .FAULT => unreachable,
            .DOM => return error.TimeoutTooBig,
            .ISCONN => return error.AlreadyConnected,
            .NOPROTOOPT => return error.InvalidProtocolOption,
            .NOMEM => return error.SystemResources,
            .NOBUFS => return error.SystemResources,
            .PERM => return error.PermissionDenied,
            .NODEV => return error.NoDevice,
            .OPNOTSUPP => return error.OperationNotSupported,
            else => |err| return stdx.unexpected_errno("setsockopt", err),
        }
    }
}

pub const RecvFromError = error{
    WouldBlock,
    ConnectionRefused,
    SystemResources,
    ConnectionResetByPeer,
    ConnectionTimedOut,
    SocketNotBound,
    MessageTooBig,
    NetworkSubsystemFailed,
    SocketNotConnected,
} || UnexpectedError;

pub fn recv(sock: socket_t, buf: []u8, flags: u32) RecvFromError!usize {
    while (true) {
        const rc = system.recvfrom(sock, buf.ptr, buf.len, flags, null, null);
        switch (errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .BADF => unreachable, // always a race condition
            .FAULT => unreachable,
            .INVAL => unreachable,
            .NOTCONN => return error.SocketNotConnected,
            .NOTSOCK => unreachable,
            .INTR => continue,
            .AGAIN => return error.WouldBlock,
            .NOMEM => return error.SystemResources,
            .CONNREFUSED => return error.ConnectionRefused,
            .CONNRESET => return error.ConnectionResetByPeer,
            .TIMEDOUT => return error.ConnectionTimedOut,
            else => |err| return stdx.unexpected_errno("recvfrom", err),
        }
    }
}

pub const SendError = error{
    AccessDenied,
    WouldBlock,
    FastOpenAlreadyInProgress,
    ConnectionResetByPeer,
    MessageTooBig,
    SystemResources,
    BrokenPipe,
    FileDescriptorNotASocket,
    NetworkUnreachable,
    NetworkSubsystemFailed,
} || UnexpectedError;

pub const SendMsgError = SendError || error{
    AddressFamilyNotSupported,
    SymLinkLoop,
    NameTooLong,
    FileNotFound,
    NotDir,
    SocketNotConnected,
    AddressNotAvailable,
};

pub const SendToError = SendMsgError || error{
    /// The destination address is not reachable by the bound address.
    UnreachableAddress,
    /// The destination address is not listening.
    ConnectionRefused,
};

pub fn sendto(
    sockfd: socket_t,
    buf: []const u8,
    flags: u32,
    dest_addr: ?*const sockaddr,
    addrlen: socklen_t,
) SendToError!usize {
    if (native_os == .windows) {
        const rc = stdx.windows.sendto(
            @ptrCast(sockfd),
            buf.ptr,
            buf.len,
            flags,
            @ptrCast(dest_addr),
            addrlen,
        );
        if (rc == stdx.windows.SOCKET_ERROR) {
            switch (stdx.windows.WSAGetLastError()) {
                .WSAEACCES => return error.AccessDenied,
                .WSAEADDRNOTAVAIL => return error.AddressNotAvailable,
                .WSAECONNRESET => return error.ConnectionResetByPeer,
                .WSAEMSGSIZE => return error.MessageTooBig,
                .WSAENOBUFS => return error.SystemResources,
                .WSAENOTSOCK => return error.FileDescriptorNotASocket,
                .WSAEAFNOSUPPORT => return error.AddressFamilyNotSupported,
                .WSAEDESTADDRREQ => unreachable, // A destination address is required.
                .WSAEFAULT => unreachable, // Parameters are not part of the user address space.
                .WSAEHOSTUNREACH => return error.NetworkUnreachable,
                .WSAEINVAL => unreachable,
                .WSAENETDOWN => return error.NetworkSubsystemFailed,
                .WSAENETRESET => return error.ConnectionResetByPeer,
                .WSAENETUNREACH => return error.NetworkUnreachable,
                .WSAENOTCONN => return error.SocketNotConnected,
                .WSAESHUTDOWN => unreachable, // The socket has been shut down.
                .WSAEWOULDBLOCK => return error.WouldBlock,
                .WSANOTINITIALISED => unreachable, // WSAStartup must be called first.
                else => |err| return stdx.windows.unexpectedWSAError(err),
            }
        }
        return @intCast(rc);
    }
    while (true) {
        const rc = system.sendto(sockfd, buf.ptr, buf.len, flags, dest_addr, addrlen);
        switch (errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .ACCES => return error.AccessDenied,
            .AGAIN => return error.WouldBlock,
            .ALREADY => return error.FastOpenAlreadyInProgress,
            .BADF => unreachable, // always a race condition
            .CONNREFUSED => return error.ConnectionRefused,
            .CONNRESET => return error.ConnectionResetByPeer,
            .DESTADDRREQ => unreachable, // The socket is not connection-mode, and no peer address.
            .FAULT => unreachable, // An invalid user space address was specified for an argument.
            .INTR => continue,
            .INVAL => return error.UnreachableAddress,
            .ISCONN => unreachable, // connection-mode socket was connected already
            .MSGSIZE => return error.MessageTooBig,
            .NOBUFS => return error.SystemResources,
            .NOMEM => return error.SystemResources,
            .NOTSOCK => unreachable, // The file descriptor sockfd does not refer to a socket.
            .OPNOTSUPP => unreachable, // Some bit in the flags argument is inappropriate.
            .PIPE => return error.BrokenPipe,
            .AFNOSUPPORT => return error.AddressFamilyNotSupported,
            .LOOP => return error.SymLinkLoop,
            .NAMETOOLONG => return error.NameTooLong,
            .NOENT => return error.FileNotFound,
            .NOTDIR => return error.NotDir,
            .HOSTUNREACH => return error.NetworkUnreachable,
            .NETUNREACH => return error.NetworkUnreachable,
            .NOTCONN => return error.SocketNotConnected,
            .NETDOWN => return error.NetworkSubsystemFailed,
            else => |err| return stdx.unexpected_errno("sendto", err),
        }
    }
}

pub const KQueueError = error{
    /// The per-process limit on the number of open file descriptors has been reached.
    ProcessFdQuotaExceeded,
    /// The system-wide limit on the total number of open files has been reached.
    SystemFdQuotaExceeded,
} || UnexpectedError;

pub fn kqueue() KQueueError!i32 {
    const rc = system.kqueue();
    switch (errno(rc)) {
        .SUCCESS => return @intCast(rc),
        .MFILE => return error.ProcessFdQuotaExceeded,
        .NFILE => return error.SystemFdQuotaExceeded,
        else => |err| return stdx.unexpected_errno("kqueue", err),
    }
}

pub const KEventError = error{
    /// The process does not have permission to register a filter.
    AccessDenied,
    /// The event could not be found to be modified or deleted.
    EventNotFound,
    /// No memory was available to register the event.
    SystemResources,
    /// The specified process to attach to does not exist.
    ProcessNotFound,
    /// changelist or eventlist had too many items on it.
    Overflow,
};

pub fn kevent(
    kq: i32,
    changelist: []const Kevent,
    eventlist: []Kevent,
    timeout: ?*const timespec,
) KEventError!usize {
    while (true) {
        const rc = system.kevent(
            kq,
            changelist.ptr,
            std.math.cast(c_int, changelist.len) orelse return error.Overflow,
            eventlist.ptr,
            std.math.cast(c_int, eventlist.len) orelse return error.Overflow,
            timeout,
        );
        switch (errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .ACCES => return error.AccessDenied,
            .FAULT => unreachable,
            .BADF => unreachable, // Always a race condition.
            .INTR => continue,
            .INVAL => unreachable,
            .NOENT => return error.EventNotFound,
            .NOMEM => return error.SystemResources,
            .SRCH => return error.ProcessNotFound,
            else => unreachable,
        }
    }
}
