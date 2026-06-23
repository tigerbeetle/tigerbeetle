//! Code shared across several IO implementations, because, e.g., it is expressible via POSIX layer.
const builtin = @import("builtin");
const std = @import("std");
const posix = std.posix;

const stdx = @import("stdx");

const Tracer = @import("../trace.zig").Tracer;

const assert = std.debug.assert;

const is_linux = builtin.target.os.tag == .linux;

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

pub fn listen(
    fd: posix.socket_t,
    address: stdx.SocketAddress,
    options: ListenOptions,
) !stdx.SocketAddress {
    const address_std = address.to_std();
    try setsockopt(fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, 1);
    try posix.bind(fd, &address_std.any, address_std.getOsSockLen());

    // Resolve port 0 to an actual port picked by the OS.
    var address_resolved_std: std.net.Address = .{ .any = undefined };
    var addrlen: posix.socklen_t = @sizeOf(std.net.Address);
    try posix.getsockname(fd, &address_resolved_std.any, &addrlen);
    assert(address_resolved_std.getOsSockLen() == addrlen);
    assert(address_resolved_std.any.family == address_std.any.family);

    try posix.listen(fd, options.backlog);

    const address_resolved = stdx.SocketAddress.from_std(address_resolved_std) catch |err|
        switch (err) {
            error.UnsupportedFamily => unreachable,
        };

    assert(address.ip.family() == address_resolved.ip.family());
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
    if (options.rcvbuf > 0) rcvbuf: {
        if (is_linux) {
            // Requires CAP_NET_ADMIN privilege (settle for SO_RCVBUF in case of an EPERM):
            if (setsockopt(fd, posix.SOL.SOCKET, posix.SO.RCVBUFFORCE, options.rcvbuf)) |_| {
                break :rcvbuf;
            } else |err| switch (err) {
                error.PermissionDenied => {},
                else => |e| return e,
            }
        }
        try setsockopt(fd, posix.SOL.SOCKET, posix.SO.RCVBUF, options.rcvbuf);
    }

    if (options.sndbuf > 0) sndbuf: {
        if (is_linux) {
            // Requires CAP_NET_ADMIN privilege (settle for SO_SNDBUF in case of an EPERM):
            if (setsockopt(fd, posix.SOL.SOCKET, posix.SO.SNDBUFFORCE, options.sndbuf)) |_| {
                break :sndbuf;
            } else |err| switch (err) {
                error.PermissionDenied => {},
                else => |e| return e,
            }
        }
        try setsockopt(fd, posix.SOL.SOCKET, posix.SO.SNDBUF, options.sndbuf);
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

pub fn aof_blocking_write_all(fd: posix.fd_t, buffer: []const u8) posix.WriteError!void {
    const file = std.fs.File{ .handle = fd };
    return file.writeAll(buffer);
}

pub fn aof_blocking_pread_all(fd: posix.fd_t, buffer: []u8, offset: u64) posix.PReadError!usize {
    const file = std.fs.File{ .handle = fd };
    return file.preadAll(buffer, offset);
}

pub fn aof_blocking_close(fd: posix.fd_t) void {
    const file = std.fs.File{ .handle = fd };
    file.close();
}

pub fn aof_blocking_stat(path: []const u8) std.fs.Dir.StatFileError!std.fs.File.Stat {
    return std.fs.cwd().statFile(path);
}

pub fn aof_blocking_fstat(fd: posix.fd_t) std.fs.Dir.StatError!std.fs.File.Stat {
    const file = std.fs.File{ .handle = fd };
    return file.stat();
}

pub fn aof_blocking_open(dir_fd: posix.fd_t, path: []const u8) !posix.fd_t {
    assert(!std.fs.path.isAbsolute(path));

    const dir = std.fs.Dir{ .fd = dir_fd };

    const file = try dir.createFile(path, .{
        .read = true,
        .truncate = false,
        .exclusive = false,
        .lock = .exclusive,
    });
    errdefer file.close();

    try file.sync();

    // We cannot fsync the directory handle on Windows.
    // We have no way to open a directory with write access.
    if (builtin.os.tag != .windows) {
        try std.posix.fsync(dir_fd);
    }

    try file.seekFromEnd(0);

    return file.handle;
}

pub const Stats = struct {
    tracer: ?*Tracer = null,

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

    pub fn trace(stats: *Stats) void {
        if (stats.tracer) |tracer| {
            tracer.timing(.loop_run_for_ns, stats.window.time_run_for_ns);
            tracer.timing(.loop_callbacks, stats.window.time_callbacks);
            tracer.timing(.loop_kernel, stats.window.time_kernel);
        }
        stats.total.add(stats.window);
        stats.window = .{};
    }
};
