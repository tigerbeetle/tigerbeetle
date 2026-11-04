//! Code shared across several IO implementations, because, e.g., it is expressible via POSIX layer.
const builtin = @import("builtin");
const std = @import("std");

const stdx = @import("../stdx.zig");
const posix = stdx.posix;

const assert = std.debug.assert;
const log = std.log.scoped(.io);

const is_linux = builtin.target.os.tag == .linux;

/// The minimum size of an aligned kernel page and an Advanced Format disk sector:
/// This is necessary for direct I/O without the kernel having to fix unaligned pages with a copy.
/// The new Advanced Format sector size is backwards compatible with the old 512 byte sector size.
/// This should therefore never be less than 4 KiB to be future-proof when server disks are swapped.
pub const sector_size = 4096;

pub const TCPOptions = struct {
    rcvbuf: c_int,
    sndbuf: c_int,
    keepalive: ?struct {
        keepidle: c_int,
        keepintvl: c_int,
        keepcnt: c_int,
    },
    user_timeout_ms: c_int,
    nodelay: bool,
};

pub const ListenOptions = struct {
    backlog: u31,
};

pub const NextTickSource = enum { lsm, vsr };

pub const PosixAddress = std.Io.Threaded.PosixAddress;

pub fn address_to_posix(address: stdx.SocketAddress, storage: *PosixAddress) posix.socklen_t {
    return std.Io.Threaded.addressToPosix(&address.to_std(), storage);
}

pub fn address_from_posix(address_posix: *const PosixAddress) stdx.SocketAddress {
    return stdx.SocketAddress.from_std(std.Io.Threaded.addressFromPosix(address_posix));
}

pub fn listen(
    fd: posix.socket_t,
    address: stdx.SocketAddress,
    options: ListenOptions,
) !stdx.SocketAddress {
    try setsockopt(fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, 1);
    var address_posix: PosixAddress = undefined;
    var address_size = address_to_posix(address, &address_posix);
    try posix.bind(fd, &address_posix.any, address_size);

    // Resolve port 0 to an actual port picked by the OS.
    address_size = @sizeOf(@TypeOf(address_posix));
    try posix.getsockname(fd, &address_posix.any, &address_size);
    const address_resolved = address_from_posix(&address_posix);

    try posix.listen(fd, options.backlog);

    assert(address_resolved.ip.family() == address.ip.family());
    assert(std.meta.eql(address.ip, address_resolved.ip));
    if (address.port != address_resolved.port) assert(address.port == 0);
    return address_resolved;
}

/// Sets the socket options.
/// Although some options are generic at the socket level,
/// these settings are intended only for TCP sockets.
pub fn tcp_options(
    fd: posix.socket_t,
    options: TCPOptions,
) !void {
    if (options.rcvbuf > 0) {
        try set_socket_buffer(fd, .receive, options.rcvbuf);
    }

    if (options.sndbuf > 0) {
        try set_socket_buffer(fd, .send, options.sndbuf);
    }

    if (options.keepalive) |keepalive| {
        try setsockopt(fd, posix.SOL.SOCKET, posix.SO.KEEPALIVE, 1);
        if (is_linux) {
            try setsockopt(fd, posix.IPPROTO.TCP, posix.TCP.KEEPIDLE, keepalive.keepidle);
            try setsockopt(fd, posix.IPPROTO.TCP, posix.TCP.KEEPINTVL, keepalive.keepintvl);
            try setsockopt(fd, posix.IPPROTO.TCP, posix.TCP.KEEPCNT, keepalive.keepcnt);
        }
    }

    if (options.user_timeout_ms > 0) {
        if (is_linux) {
            const timeout_ms = options.user_timeout_ms;
            try setsockopt(fd, posix.IPPROTO.TCP, posix.TCP.USER_TIMEOUT, timeout_ms);
        }
    }

    // Set tcp no-delay
    if (options.nodelay) {
        if (is_linux) {
            try setsockopt(fd, posix.IPPROTO.TCP, posix.TCP.NODELAY, 1);
        }
    }
}

pub fn setsockopt(fd: posix.socket_t, level: i32, option: u32, value: c_int) !void {
    try posix.setsockopt(fd, level, option, &std.mem.toBytes(value));
}

const SocketBuffer = enum {
    receive,
    send,

    fn option(buffer: SocketBuffer) u32 {
        return switch (buffer) {
            .receive => posix.SO.RCVBUF,
            .send => posix.SO.SNDBUF,
        };
    }

    fn option_force(buffer: SocketBuffer) u32 {
        assert(is_linux);
        return switch (buffer) {
            .receive => posix.SO.RCVBUFFORCE,
            .send => posix.SO.SNDBUFFORCE,
        };
    }
};

fn set_socket_buffer(fd: posix.socket_t, buffer: SocketBuffer, requested: c_int) !void {
    assert(requested > 0);

    set: {
        if (is_linux) {
            // Requires CAP_NET_ADMIN privilege (settle for SO_RCVBUF/SO_SNDBUF on EPERM):
            if (setsockopt(fd, posix.SOL.SOCKET, buffer.option_force(), requested)) |_| {
                break :set;
            } else |err| switch (err) {
                error.PermissionDenied => {},
                else => |e| return e,
            }
        }
        try setsockopt(fd, posix.SOL.SOCKET, buffer.option(), requested);
    }

    try verify_socket_buffer(fd, buffer, requested);
}

fn verify_socket_buffer(
    fd: posix.socket_t,
    buffer: SocketBuffer,
    requested: c_int,
) !void {
    const effective_raw = try getsockopt(fd, posix.SOL.SOCKET, buffer.option());

    // Linux reports twice the configured buffer size to account for kernel bookkeeping overhead
    // (see https://man7.org/linux/man-pages/man7/socket.7.html).
    const effective = if (is_linux) @divFloor(effective_raw, 2) else effective_raw;

    if (effective < requested) {
        log.warn(
            "TCP {s} buffer is smaller than requested: requested={} effective={}",
            .{ @tagName(buffer), requested, effective },
        );
    }
}

// TODO: Use std.posix.getsockopt when it initializes optlen before calling the system API.
fn getsockopt(
    fd: posix.socket_t,
    level: i32,
    option: u32,
) posix.GetSockOptError!c_int {
    var value: c_int = undefined;

    if (builtin.target.os.tag == .windows) {
        var value_size: i32 = @sizeOf(c_int);
        const rc = stdx.windows.ws2_32.getsockopt(
            @ptrCast(fd),
            level,
            @intCast(option),
            std.mem.asBytes(&value),
            &value_size,
        );
        if (rc != 0) {
            switch (stdx.windows.ws2_32.WSAGetLastError()) {
                .WSAEACCES => return error.AccessDenied,
                .WSAENOPROTOOPT => return error.InvalidProtocolOption,
                .WSAENOBUFS => return error.SystemResources,
                .WSANOTINITIALISED => unreachable,
                .WSAEFAULT => unreachable,
                .WSAEINVAL => unreachable,
                .WSAENOTSOCK => unreachable,
                else => |err| return stdx.windows.unexpectedWSAError(err),
            }
        }
        assert(value_size == @sizeOf(c_int));
        return value;
    }

    var value_size: posix.socklen_t = @sizeOf(c_int);
    switch (posix.errno(posix.system.getsockopt(
        fd,
        level,
        option,
        std.mem.asBytes(&value).ptr,
        &value_size,
    ))) {
        .SUCCESS => assert(value_size == @sizeOf(c_int)),
        .BADF => unreachable,
        .NOTSOCK => unreachable,
        .INVAL => unreachable,
        .FAULT => unreachable,
        .NOPROTOOPT => return error.InvalidProtocolOption,
        .NOMEM, .NOBUFS => return error.SystemResources,
        .ACCES => return error.AccessDenied,
        else => |errno| return stdx.unexpected_errno("getsockopt", errno),
    }
    return value;
}

pub fn aof_blocking_write_all(
    io_std: std.Io,
    fd: posix.fd_t,
    buffer: []const u8,
) std.Io.File.Writer.Error!void {
    const file = std.Io.File{ .handle = fd, .flags = .{ .nonblocking = false } };
    return file.writeStreamingAll(io_std, buffer);
}

pub fn aof_blocking_pread_all(
    io_std: std.Io,
    fd: posix.fd_t,
    buffer: []u8,
    offset: u64,
) std.Io.File.ReadPositionalError!usize {
    const file = std.Io.File{ .handle = fd, .flags = .{ .nonblocking = false } };
    return file.readPositionalAll(io_std, buffer, offset);
}

pub fn aof_blocking_close(io_std: std.Io, fd: posix.fd_t) void {
    const file = std.Io.File{ .handle = fd, .flags = .{ .nonblocking = false } };
    file.close(io_std);
}

pub fn aof_blocking_stat(io_std: std.Io, path: []const u8) std.Io.Dir.StatFileError!std.Io.File.Stat {
    return std.Io.Dir.cwd().statFile(io_std, path, .{});
}

pub fn aof_blocking_fstat(io_std: std.Io, fd: posix.fd_t) std.Io.Dir.StatError!std.Io.File.Stat {
    const file = std.Io.File{ .handle = fd, .flags = .{ .nonblocking = false } };
    return file.stat(io_std);
}

pub fn aof_blocking_open(io_std: std.Io, dir_fd: posix.fd_t, path: []const u8) !posix.fd_t {
    assert(!std.fs.path.isAbsolute(path));

    const dir = std.Io.Dir{ .handle = dir_fd };

    const file = try dir.createFile(io_std, path, .{
        .read = true,
        .truncate = false,
        .exclusive = false,
        .lock = .exclusive,
    });
    errdefer file.close(io_std);

    try file.sync(io_std);

    // We cannot fsync the directory handle on Windows.
    // We have no way to open a directory with write access.
    if (builtin.os.tag != .windows) {
        try posix.fsync(dir_fd);
    }

    var file_writer = file.writerStreaming(io_std, &.{});
    try file_writer.seekTo(try file.length(io_std));

    return file.handle;
}

/// Returns a cryptographically random integer, see `crypto_random_bytes`.
pub fn crypto_random_int(comptime IO: type, comptime T: type, io: *IO) T {
    var buffer: [@sizeOf(T)]u8 = undefined;
    io.crypto_random_bytes(&buffer);
    const value = std.mem.readInt(T, &buffer, .little);

    if (T == u128) {
        // Broken CSPRNG is the likeliest explanation for zero or all ones.
        assert(value != 0);
        assert(value != std.math.maxInt(u128));
    }
    return value;
}

pub const Stats = struct {
    total: Timings = .{},
    window: Timings = .{},

    const Timings = struct {
        time_callbacks: stdx.Duration = .ms(0),
        time_run_for_ns: stdx.Duration = .ms(0),
        time_kernel: stdx.Duration = .ms(0),

        pub fn add(total: *Timings, increment: Timings) void {
            total.time_callbacks.ns +|= increment.time_callbacks.ns;
            total.time_run_for_ns.ns +|= increment.time_run_for_ns.ns;
            total.time_kernel.ns +|= increment.time_kernel.ns;
        }
    };
};

pub fn buffer_limit(buffer_len: usize) usize {
    // Linux limits how much may be written in a `pwrite()/pread()` call, which is `0x7ffff000` on
    // both 64-bit and 32-bit systems, due to using a signed C int as the return value, as well as
    // stuffing the errno codes into the last `4096` values.
    // Darwin limits writes to `0x7fffffff` bytes, more than that returns `EINVAL`.
    // The corresponding POSIX limit is `std.math.maxInt(isize)`.
    const limit = switch (builtin.target.os.tag) {
        .linux => 0x7ffff000,
        .macos, .ios, .watchos, .tvos => std.math.maxInt(i32),
        else => std.math.maxInt(isize),
    };
    return @min(limit, buffer_len);
}

pub const DirectIO = enum {
    direct_io_required,
    direct_io_optional,
    direct_io_disabled,
};

pub const OpenDataFilePurpose = union(enum) {
    format: struct { superblock_zone_size: u32 },
    open,
    inspect,
};
