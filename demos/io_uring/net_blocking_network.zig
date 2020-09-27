// MIT License

// Copyright (c) 2020 Felix Queißner

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

const std = @import("std");

comptime {
    std.debug.assert(@sizeOf(std.os.sockaddr) >= @sizeOf(std.os.sockaddr_in));
    // std.debug.assert(@sizeOf(std.os.sockaddr) >= @sizeOf(std.os.sockaddr_in6));
}

const is_windows = std.builtin.os.tag == .windows;
const is_darwin = std.builtin.os.tag.isDarwin();
const is_linux = std.builtin.os.tag == .linux;

pub fn init() error{InitializationError}!void {
    if (is_windows) {
        _ = windows.WSAStartup(2, 2) catch return error.InitializationError;
    }
}

pub fn deinit() void {
    if (is_windows) {
        windows.WSACleanup() catch return;
    }
}

/// A network address abstraction. Contains one member for each possible type of address.
pub const Address = union(AddressFamily) {
    ipv4: IPv4,
    ipv6: IPv6,

    pub const IPv4 = struct {
        const Self = @This();

        pub const any = IPv4.init(0, 0, 0, 0);
        pub const broadcast = IPv4.init(255, 255, 255, 255);
        pub const loopback = IPv4.init(127, 0, 0, 1);

        value: [4]u8,

        pub fn init(a: u8, b: u8, c: u8, d: u8) Self {
            return Self{
                .value = [4]u8{ a, b, c, d },
            };
        }

        pub fn eql(lhs: Self, rhs: Self) bool {
            return std.mem.eql(u8, &lhs.value, &rhs.value);
        }

        pub fn format(value: Self, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
            try writer.print("{}.{}.{}.{}", .{
                value.value[0],
                value.value[1],
                value.value[2],
                value.value[3],
            });
        }
    };

    pub const IPv6 = struct {
        const Self = @This();

        pub const any = std.mem.zeroes(Self);
        pub const loopback = IPv6.init([1]u8{0} ** 15 ++ [1]u8{1}, 0);

        value: [16]u8,
        scope_id: u32,

        pub fn init(value: [16]u8, scope_id: u32) Self {
            return Self{ .value = value, .scope_id = scope_id };
        }

        pub fn eql(lhs: Self, rhs: Self) bool {
            return std.mem.eql(u8, &lhs.value, &rhs.value) and
                lhs.scope_id == rhs.scope_id;
        }

        pub fn format(self: Self, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
            if (std.mem.eql(u8, self.value[0..12], &[_]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff })) {
                try std.fmt.format(writer, "[::ffff:{}.{}.{}.{}]", .{
                    self.value[12],
                    self.value[13],
                    self.value[14],
                    self.value[15],
                });
                return;
            }
            const big_endian_parts = @ptrCast(*align(1) const [8]u16, &self.value);
            const native_endian_parts = switch (std.builtin.endian) {
                .Big => big_endian_parts.*,
                .Little => blk: {
                    var buf: [8]u16 = undefined;
                    for (big_endian_parts) |part, i| {
                        buf[i] = std.mem.bigToNative(u16, part);
                    }
                    break :blk buf;
                },
            };
            try writer.writeAll("[");
            var i: usize = 0;
            var abbrv = false;
            while (i < native_endian_parts.len) : (i += 1) {
                if (native_endian_parts[i] == 0) {
                    if (!abbrv) {
                        try writer.writeAll(if (i == 0) "::" else ":");
                        abbrv = true;
                    }
                    continue;
                }
                try std.fmt.format(writer, "{x}", .{native_endian_parts[i]});
                if (i != native_endian_parts.len - 1) {
                    try writer.writeAll(":");
                }
            }
            try writer.writeAll("]");
        }
    };

    pub fn format(value: @This(), comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        switch (value) {
            .ipv4 => |a| try a.format(fmt, options, writer),
            .ipv6 => |a| try a.format(fmt, options, writer),
        }
    }

    pub fn eql(lhs: @This(), rhs: @This()) bool {
        if (@as(AddressFamily, lhs) != @as(AddressFamily, rhs))
            return false;
        return switch (lhs) {
            .ipv4 => |l| l.eql(rhs.ipv4),
            .ipv6 => |l| l.eql(rhs.ipv6),
        };
    }
};

pub const AddressFamily = enum {
    const Self = @This();

    ipv4,
    ipv6,

    fn toNativeAddressFamily(af: Self) u32 {
        return switch (af) {
            .ipv4 => std.os.AF_INET,
            .ipv6 => std.os.AF_INET6,
        };
    }

    fn fromNativeAddressFamily(af: i32) !Self {
        return switch (af) {
            std.os.AF_INET => .ipv4,
            std.os.AF_INET6 => .ipv6,
            else => return error.UnsupportedAddressFamily,
        };
    }
};

/// Protocols supported by this library.
pub const Protocol = enum {
    const Self = @This();

    tcp,
    udp,

    fn toSocketType(proto: Self) u32 {
        return switch (proto) {
            .tcp => std.os.SOCK_STREAM,
            .udp => std.os.SOCK_DGRAM,
        };
    }
};

/// A network end point. Is composed of an address and a port.
pub const EndPoint = struct {
    const Self = @This();

    address: Address,
    port: u16,

    pub fn format(value: Self, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        try writer.print("{}:{}", .{
            value.address,
            value.port,
        });
    }

    pub fn fromSocketAddress(src: *const std.os.sockaddr, size: usize) !Self {
        switch (src.family) {
            std.os.AF_INET => {
                if (size < @sizeOf(std.os.sockaddr_in))
                    return error.InsufficientBytes;
                const value = @ptrCast(*const std.os.sockaddr_in, @alignCast(4, src));
                return EndPoint{
                    .port = std.mem.bigToNative(u16, value.port),
                    .address = .{
                        .ipv4 = .{
                            .value = @bitCast([4]u8, value.addr),
                        },
                    },
                };
            },
            std.os.AF_INET6 => {
                if (size < @sizeOf(std.os.sockaddr_in6))
                    return error.InsufficientBytes;
                const value = @ptrCast(*const std.os.sockaddr_in6, @alignCast(4, src));
                return EndPoint{
                    .port = std.mem.bigToNative(u16, value.port),
                    .address = .{
                        .ipv6 = .{
                            .value = value.addr,
                            .scope_id = value.scope_id,
                        },
                    },
                };
            },
            else => {
                std.debug.warn("got invalid socket address: {}\n", .{src});
                return error.UnsupportedAddressFamily;
            },
        }
    }

    pub const SockAddr = union(AddressFamily) {
        ipv4: std.os.sockaddr_in,
        ipv6: std.os.sockaddr_in6,
    };

    fn toSocketAddress(self: Self) SockAddr {
        var result: std.os.sockaddr align(8) = undefined;
        return switch (self.address) {
            .ipv4 => |addr| SockAddr{
                .ipv4 = .{
                    .family = std.os.AF_INET,
                    .port = std.mem.nativeToBig(u16, self.port),
                    .addr = @bitCast(u32, addr.value),
                    .zero = [_]u8{0} ** 8,
                },
            },
            .ipv6 => |addr| SockAddr{
                .ipv6 = .{
                    .family = std.os.AF_INET6,
                    .port = std.mem.nativeToBig(u16, self.port),
                    .flowinfo = 0,
                    .addr = addr.value,
                    .scope_id = addr.scope_id,
                },
            },
        };
    }
};

/// A network socket, can receive and send data for TCP/UDP and accept
/// incoming connections if bound as a TCP server.
pub const Socket = struct {
    pub const Reader = std.io.Reader(Socket, std.os.RecvFromError, receive);
    pub const Writer = std.io.Writer(Socket, std.os.SendError, send);

    const Self = @This();
    const NativeSocket = if (is_windows) windows.ws2_32.SOCKET else std.os.fd_t;

    family: AddressFamily,
    internal: NativeSocket,
    endpoint: ?EndPoint,

    /// Spawns a new socket that must be freed with `close()`.
    /// `family` defines the socket family, `protocol` the protocol used.
    pub fn create(family: AddressFamily, protocol: Protocol) !Self {
        const socket_fn = if (is_windows) windows.socket else std.os.socket;

        // std provides a shim for Darwin to set SOCK_NONBLOCK.
        // Socket creation will only set the flag if we provide the shim rather than the actual flag.
        const socket_type = if ((is_darwin or is_linux) and std.io.is_async)
            protocol.toSocketType() | std.os.SOCK_NONBLOCK | std.os.SOCK_CLOEXEC
        else
            protocol.toSocketType();

        return Self{
            .family = family,
            .internal = try socket_fn(family.toNativeAddressFamily(), socket_type, 0),
            .endpoint = null,
        };
    }

    /// Closes the socket and releases its resources.
    pub fn close(self: Self) void {
        const close_fn = if (is_windows) windows.close else std.os.close;
        close_fn(self.internal);
    }

    /// Binds the socket to the given end point.
    pub fn bind(self: Self, ep: EndPoint) !void {
        const bind_fn = if (is_windows) windows.bind else std.os.bind;

        switch (ep.toSocketAddress()) {
            .ipv4 => |sockaddr| try bind_fn(self.internal, @ptrCast(*const std.os.sockaddr, &sockaddr), @sizeOf(@TypeOf(sockaddr))),
            .ipv6 => |sockaddr| try bind_fn(self.internal, @ptrCast(*const std.os.sockaddr, &sockaddr), @sizeOf(@TypeOf(sockaddr))),
        }
    }

    /// Binds the socket to all supported addresses on the local device.
    /// This will use the any IP (`0.0.0.0` for IPv4).
    pub fn bindToPort(self: Self, port: u16) !void {
        return switch (self.family) {
            .ipv4 => self.bind(EndPoint{
                .address = Address{ .ipv4 = Address.IPv4.any },
                .port = port,
            }),
            .ipv6 => self.bind(EndPoint{
                .address = Address{ .ipv6 = Address.IPv6.any },
                .port = port,
            }),
        };
    }

    /// Connects the UDP or TCP socket to a remote server.
    /// The `target` address type must fit the address type of the socket.
    pub fn connect(self: *Self, target: EndPoint) !void {
        if (target.address != self.family)
            return error.AddressFamilyMismach;

        // on darwin you set the NOSIGNAl once, rather than for each message
        if (is_darwin) {
            // set the options to ON
            const value: u32 = 1;
            const SO_NOSIGPIPE = 0x00000800;
            try std.os.setsockopt(self.internal, std.os.SOL_SOCKET, SO_NOSIGPIPE, std.mem.asBytes(&value));
        }

        const connect_fn = if (is_windows) windows.connect else std.os.connect;
        switch (target.toSocketAddress()) {
            .ipv4 => |sockaddr| try connect_fn(self.internal, @ptrCast(*const std.os.sockaddr, &sockaddr), @sizeOf(@TypeOf(sockaddr))),
            .ipv6 => |sockaddr| try connect_fn(self.internal, @ptrCast(*const std.os.sockaddr, &sockaddr), @sizeOf(@TypeOf(sockaddr))),
        }
        self.endpoint = target;
    }

    /// Makes this socket a TCP server and allows others to connect to
    /// this socket.
    /// Call `accept()` to handle incoming connections.
    pub fn listen(self: Self) !void {
        const listen_fn = if (is_windows) windows.listen else std.os.listen;
        try listen_fn(self.internal, 0);
    }

    /// Waits until a new TCP client connects to this socket and accepts the incoming TCP connection.
    /// This function is only allowed for a bound TCP socket. `listen()` must have been called before!
    pub fn accept(self: Self) !Socket {
        const accept4_fn = if (is_windows) windows.accept4 else std.os.accept;
        const close_fn = if (is_windows) windows.close else std.os.close;

        var addr: std.os.sockaddr_in6 = undefined;
        var addr_size: std.os.socklen_t = @sizeOf(std.os.sockaddr_in6);

        const flags = if (is_windows or is_darwin)
            0
        else if (std.io.is_async) std.os.O_NONBLOCK else 0;

        var addr_ptr = @ptrCast(*std.os.sockaddr, &addr);
        const fd = try accept4_fn(self.internal, addr_ptr, &addr_size, flags);
        errdefer close_fn(fd);

        return Socket{
            .family = try AddressFamily.fromNativeAddressFamily(addr_ptr.family),
            .internal = fd,
            .endpoint = null,
        };
    }

    /// Send some data to the connected peer. In UDP, this
    /// will always send full packets, on TCP it will append
    /// to the stream.
    pub fn send(self: Self, data: []const u8) !usize {
        if (self.endpoint) |ep|
            return try self.sendTo(ep, data);
        const send_fn = if (is_windows) windows.send else std.os.send;
        const flags = if (is_windows or is_darwin) 0 else std.os.MSG_NOSIGNAL;
        return try send_fn(self.internal, data, flags);
    }

    /// Blockingly receives some data from the connected peer.
    /// Will read all available data from the TCP stream or
    /// a UDP packet.
    pub fn receive(self: Self, data: []u8) !usize {
        const recvfrom_fn = if (is_windows) windows.recvfrom else std.os.recvfrom;
        const flags = if (is_windows or is_darwin) 0 else std.os.MSG_NOSIGNAL;
        return try recvfrom_fn(self.internal, data, flags, null, null);
    }

    const ReceiveFrom = struct { numberOfBytes: usize, sender: EndPoint };

    /// Same as ´receive`, but will also return the end point from which the data
    /// was received. This is only a valid operation on UDP sockets.
    pub fn receiveFrom(self: Self, data: []u8) !ReceiveFrom {
        const recvfrom_fn = if (is_windows) windows.recvfrom else std.os.recvfrom;
        const flags = if (is_windows or is_darwin) 0 else std.os.MSG_NOSIGNAL;

        // Use the ipv6 sockaddr to gurantee data will fit.
        var addr: std.os.sockaddr_in6 align(4) = undefined;
        var size: std.os.socklen_t = @sizeOf(std.os.sockaddr_in6);

        var addr_ptr = @ptrCast(*std.os.sockaddr, &addr);
        const len = try recvfrom_fn(self.internal, data, flags, addr_ptr, &size);

        return ReceiveFrom{
            .numberOfBytes = len,
            .sender = try EndPoint.fromSocketAddress(addr_ptr, size),
        };
    }

    /// Sends a packet to a given network end point. Behaves the same as `send()`, but will only work for
    /// for UDP sockets.
    pub fn sendTo(self: Self, receiver: EndPoint, data: []const u8) !usize {
        const sendto_fn = if (is_windows) windows.sendto else std.os.sendto;
        const flags = if (is_windows or is_darwin) 0 else std.os.MSG_NOSIGNAL;

        return switch (receiver.toSocketAddress()) {
            .ipv4 => |sockaddr| try sendto_fn(self.internal, data, flags, @ptrCast(*const std.os.sockaddr, &sockaddr), @sizeOf(@TypeOf(sockaddr))),
            .ipv6 => |sockaddr| try sendto_fn(self.internal, data, flags, @ptrCast(*const std.os.sockaddr, &sockaddr), @sizeOf(@TypeOf(sockaddr))),
        };
    }

    /// Sets the socket option `SO_REUSEPORT` which allows
    /// multiple bindings of the same socket to the same address
    /// on UDP sockets and allows quicker re-binding of TCP sockets.
    pub fn enablePortReuse(self: Self, enabled: bool) !void {
        const setsockopt_fn = if (is_windows) windows.setsockopt else std.os.setsockopt;

        var opt: c_int = if (enabled) 1 else 0;
        try setsockopt_fn(self.internal, std.os.SOL_SOCKET, std.os.SO_REUSEADDR, std.mem.asBytes(&opt));
    }

    /// Retrieves the end point to which the socket is bound.
    pub fn getLocalEndPoint(self: Self) !EndPoint {
        const getsockname_fn = if (is_windows) windows.getsockname else std.os.getsockname;

        var addr: std.os.sockaddr_in6 align(4) = undefined;
        var size: std.os.socklen_t = @sizeOf(std.os.sockaddr_in6);

        var addr_ptr = @ptrCast(*std.os.sockaddr, &addr);
        try getsockname_fn(self.internal, addr_ptr, &size);

        return try EndPoint.fromSocketAddress(addr_ptr, size);
    }

    /// Retrieves the end point to which the socket is connected.
    pub fn getRemoteEndPoint(self: Self) !EndPoint {
        const getpeername_fn = if (is_windows) windows.getpeername else getpeername;

        var addr: std.os.sockaddr_in6 align(4) = undefined;
        var size: std.os.socklen_t = @sizeOf(std.os.sockaddr_in6);

        var addr_ptr = @ptrCast(*std.os.sockaddr, &addr);
        try getpeername_fn(self.internal, addr_ptr, &size);

        return try EndPoint.fromSocketAddress(addr_ptr, size);
    }

    pub const MulticastGroup = struct {
        interface: Address.IPv4,
        group: Address.IPv4,
    };

    /// Joins the UDP socket into a multicast group.
    /// Multicast enables sending packets to the group and all joined peers
    /// will receive the sent data.
    pub fn joinMulticastGroup(self: Self, group: MulticastGroup) !void {
        const setsockopt_fn = if (is_windows) windows.setsockopt else std.os.setsockopt;

        const ip_mreq = extern struct {
            imr_multiaddr: u32,
            imr_address: u32,
            imr_ifindex: u32,
        };

        const request = ip_mreq{
            .imr_multiaddr = @bitCast(u32, group.group.value),
            .imr_address = @bitCast(u32, group.interface.value),
            .imr_ifindex = 0, // this cannot be crossplatform, so we set it to zero
        };

        const IP_ADD_MEMBERSHIP = if (is_windows) 5 else 35;

        try setsockopt_fn(self.internal, std.os.SOL_SOCKET, IP_ADD_MEMBERSHIP, std.mem.asBytes(&request));
    }

    /// Gets an reader that allows reading data from the socket.
    pub fn reader(self: Self) Reader {
        return .{
            .context = self,
        };
    }

    /// Gets a writer that allows writing data to the socket.
    pub fn writer(self: Self) Writer {
        return .{
            .context = self,
        };
    }
};

/// A socket event that can be waited for.
pub const SocketEvent = struct {
    /// Wait for data ready to be read.
    read: bool,

    /// Wait for all pending data to be sent and the socket accepting
    /// non-blocking writes.
    write: bool,
};

/// A set of sockets that can be used to query socket readiness.
/// This is similar to `select()´ or `poll()` and provides a way to
/// create non-blocking socket I/O.
/// This is intented to be used with `waitForSocketEvents()`.
pub const SocketSet = struct {
    const Self = @This();

    internal: OSLogic,

    /// Initialize a new socket set. This can be reused for
    /// multiple queries without having to reset the set every time.
    /// Call `deinit()` to free the socket set.
    pub fn init(allocator: *std.mem.Allocator) !Self {
        return Self{
            .internal = try OSLogic.init(allocator),
        };
    }

    /// Frees the contained resources.
    pub fn deinit(self: *Self) void {
        self.internal.deinit();
    }

    /// Removes all sockets from the set.
    pub fn clear(self: *Self) void {
        self.internal.clear();
    }

    /// Adds a socket to the set and enables waiting for any of the events
    /// in `events`.
    pub fn add(self: *Self, sock: Socket, events: SocketEvent) !void {
        try self.internal.add(sock, events);
    }

    /// Removes the socket from the set.
    pub fn remove(self: *Self, sock: Socket) void {
        self.internal.remove(sock);
    }

    /// Checks if the socket is ready to be read.
    /// Only valid after the first call to `waitForSocketEvent()`.
    pub fn isReadyRead(self: Self, sock: Socket) bool {
        return self.internal.isReadyRead(sock);
    }

    /// Checks if the socket is ready to be written.
    /// Only valid after the first call to `waitForSocketEvent()`.
    pub fn isReadyWrite(self: Self, sock: Socket) bool {
        return self.internal.isReadyWrite(sock);
    }

    /// Checks if the socket is faulty and cannot be used anymore.
    /// Only valid after the first call to `waitForSocketEvent()`.
    pub fn isFaulted(self: Self, sock: Socket) bool {
        return self.internal.isFaulted(sock);
    }
};

/// Implementation of SocketSet for each platform,
/// keeps the thing above nice and clean, all functions get inlined.
const OSLogic = switch (std.builtin.os.tag) {
    .windows => WindowsOSLogic,
    .linux => LinuxOSLogic,
    .macosx, .ios, .watchos, .tvos => DarwinOsLogic,
    else => @compileError("unsupported os " ++ @tagName(std.builtin.os.tag) ++ " for SocketSet!"),
};

// Linux uses `poll()` syscall to wait for socket events.
// This allows an arbitrary number of sockets to be handled.
const LinuxOSLogic = struct {
    const Self = @This();
    // use poll on linux

    fds: std.ArrayList(std.os.pollfd),

    inline fn init(allocator: *std.mem.Allocator) !Self {
        return Self{
            .fds = std.ArrayList(std.os.pollfd).init(allocator),
        };
    }

    inline fn deinit(self: Self) void {
        self.fds.deinit();
    }

    inline fn clear(self: *Self) void {
        self.fds.shrink(0);
    }

    inline fn add(self: *Self, sock: Socket, events: SocketEvent) !void {
        // Always poll for errors as this is done anyways
        var mask: i16 = std.os.POLLERR;

        if (events.read)
            mask |= std.os.POLLIN;
        if (events.write)
            mask |= std.os.POLLOUT;

        for (self.fds.items) |*pfd| {
            if (pfd.fd == sock.internal) {
                pfd.events |= mask;
                return;
            }
        }

        try self.fds.append(std.os.pollfd{
            .fd = sock.internal,
            .events = mask,
            .revents = 0,
        });
    }

    inline fn remove(self: *Self, sock: Socket) void {
        const index = for (self.fds.items) |item, i| {
            if (item.fd == sock.internal)
                break i;
        } else null;

        if (index) |idx| {
            _ = self.fds.swapRemove(idx);
        }
    }

    inline fn checkMaskAnyBit(self: Self, sock: Socket, mask: i16) bool {
        for (self.fds.items) |item| {
            if (item.fd != sock.internal)
                continue;

            if ((item.revents & mask) != 0) {
                return true;
            }

            return false;
        }
        return false;
    }

    inline fn isReadyRead(self: Self, sock: Socket) bool {
        return self.checkMaskAnyBit(sock, std.os.POLLIN);
    }

    inline fn isReadyWrite(self: Self, sock: Socket) bool {
        return self.checkMaskAnyBit(sock, std.os.POLLOUT);
    }

    inline fn isFaulted(self: Self, sock: Socket) bool {
        return self.checkMaskAnyBit(sock, std.os.POLLERR);
    }
};

/// Alias to LinuxOSLogic as the logic between the two are shared and both support poll()
const DarwinOsLogic = LinuxOSLogic;

// On windows, we use select()
const WindowsOSLogic = struct {
    // The windows struct fd_set uses a statically size array of 64 sockets by default.
    // However, it is documented that one can create bigger sets and pass them into the functions that use them.
    // Instead, we dynamically allocate the sets and reallocate them as needed.
    // See https://docs.microsoft.com/en-us/windows/win32/winsock/maximum-number-of-sockets-supported-2
    const FdSet = extern struct {
        padding1: c_uint = 0, // This is added to guarantee &size is 8 byte aligned
        capacity: c_uint,
        size: c_uint,
        padding2: c_uint = 0, // This is added to gurantee &fds is 8 byte aligned
        // fds: SOCKET[size]

        fn fdSlice(self: *align(8) FdSet) []windows.ws2_32.SOCKET {
            return @ptrCast([*]windows.ws2_32.SOCKET, @ptrCast([*]u8, self) + 4 * @sizeOf(c_uint))[0..self.size];
        }

        fn make(allocator: *std.mem.Allocator) !*align(8) FdSet {
            // Initialize with enough space for 8 sockets.
            var mem = try allocator.alignedAlloc(u8, 8, 4 * @sizeOf(c_uint) + 8 * @sizeOf(windows.ws2_32.SOCKET));

            var fd_set = @ptrCast(*align(8) FdSet, mem);
            fd_set.* = .{ .capacity = 8, .size = 0 };
            return fd_set;
        }

        fn clear(self: *align(8) FdSet) void {
            self.size = 0;
        }

        fn memSlice(self: *align(8) FdSet) []u8 {
            return @ptrCast([*]u8, self)[0..(4 * @sizeOf(c_uint) + self.capacity * @sizeOf(windows.ws2_32.SOCKET))];
        }

        fn deinit(self: *align(8) FdSet, allocator: *std.mem.Allocator) void {
            allocator.free(self.memSlice());
        }

        fn containsFd(self: *align(8) FdSet, fd: windows.ws2_32.SOCKET) bool {
            for (self.fdSlice()) |ex_fd| {
                if (ex_fd == fd) return true;
            }
            return false;
        }

        fn addFd(fd_set: **align(8) FdSet, allocator: *std.mem.Allocator, new_fd: windows.ws2_32.SOCKET) !void {
            if (fd_set.*.size == fd_set.*.capacity) {
                // Double our capacity.
                const new_mem_size = 4 * @sizeOf(c_uint) + 2 * fd_set.*.capacity * @sizeOf(windows.ws2_32.SOCKET);
                fd_set.* = @ptrCast(*align(8) FdSet, (try allocator.reallocAdvanced(fd_set.*.memSlice(), 8, new_mem_size, .exact)).ptr);
                fd_set.*.capacity *= 2;
            }

            fd_set.*.size += 1;
            fd_set.*.fdSlice()[fd_set.*.size - 1] = new_fd;
        }

        fn getSelectPointer(self: *align(8) FdSet) ?[*]u8 {
            if (self.size == 0) return null;
            return @ptrCast([*]u8, self) + 2 * @sizeOf(c_uint);
        }
    };

    const Self = @This();

    allocator: *std.mem.Allocator,

    read_fds: std.ArrayListUnmanaged(windows.ws2_32.SOCKET),
    write_fds: std.ArrayListUnmanaged(windows.ws2_32.SOCKET),

    read_fd_set: *align(8) FdSet,
    write_fd_set: *align(8) FdSet,
    except_fd_set: *align(8) FdSet,

    inline fn init(allocator: *std.mem.Allocator) !Self {
        // TODO: https://github.com/ziglang/zig/issues/5391
        var read_fds = std.ArrayListUnmanaged(windows.ws2_32.SOCKET){};
        var write_fds = std.ArrayListUnmanaged(windows.ws2_32.SOCKET){};
        try read_fds.ensureCapacity(allocator, 8);
        try write_fds.ensureCapacity(allocator, 8);

        return Self{
            .allocator = allocator,
            .read_fds = read_fds,
            .write_fds = write_fds,
            .read_fd_set = try FdSet.make(allocator),
            .write_fd_set = try FdSet.make(allocator),
            .except_fd_set = try FdSet.make(allocator),
        };
    }

    inline fn deinit(self: *Self) void {
        self.read_fds.deinit(self.allocator);
        self.write_fds.deinit(self.allocator);

        self.read_fd_set.deinit(self.allocator);
        self.write_fd_set.deinit(self.allocator);
        self.except_fd_set.deinit(self.allocator);
    }

    inline fn clear(self: *Self) void {
        self.read_fds.shrink(0);
        self.write_fds.shrink(0);

        self.read_fd_set.clear();
        self.write_fd_set.clear();
        self.except_fd_set.clear();
    }

    inline fn add(self: *Self, sock: Socket, events: SocketEvent) !void {
        if (events.read) read_block: {
            for (self.read_fds.items) |fd| {
                if (fd == sock.internal) break :read_block;
            }
            try self.read_fds.append(self.allocator, sock.internal);
        }
        if (events.write) {
            for (self.write_fds.items) |fd| {
                if (fd == sock.internal) return;
            }
            try self.write_fds.append(self.allocator, sock.internal);
        }
    }

    inline fn remove(self: *Self, sock: Socket) void {
        for (self.read_fds.items) |fd, idx| {
            if (fd == sock.internal) {
                _ = self.read_fds.swapRemove(idx);
                break;
            }
        }
        for (self.write_fds.items) |fd, idx| {
            if (fd == sock.internal) {
                _ = self.write_fds.swapRemove(idx);
                break;
            }
        }
    }

    const Set = enum {
        read,
        write,
        except,
    };

    inline fn getFdSet(self: *Self, comptime set_selection: Set) !?[*]u8 {
        const set_ptr = switch (set_selection) {
            .read => &self.read_fd_set,
            .write => &self.write_fd_set,
            .except => &self.except_fd_set,
        };

        set_ptr.*.clear();
        if (set_selection == .read or set_selection == .except) {
            for (self.read_fds.items) |fd| {
                try FdSet.addFd(set_ptr, self.allocator, fd);
            }
        }

        if (set_selection == .write) {
            for (self.write_fds.items) |fd| {
                try FdSet.addFd(set_ptr, self.allocator, fd);
            }
        } else if (set_selection == .except) {
            for (self.write_fds.items) |fd| {
                if (set_ptr.*.containsFd(fd)) continue;
                try FdSet.addFd(set_ptr, self.allocator, fd);
            }
        }
        return set_ptr.*.getSelectPointer();
    }

    inline fn isReadyRead(self: Self, sock: Socket) bool {
        if (self.read_fd_set.getSelectPointer()) |ptr| {
            return windows.funcs.__WSAFDIsSet(sock.internal, ptr) != 0;
        }
        return false;
    }

    inline fn isReadyWrite(self: Self, sock: Socket) bool {
        if (self.write_fd_set.getSelectPointer()) |ptr| {
            return windows.funcs.__WSAFDIsSet(sock.internal, ptr) != 0;
        }
        return false;
    }

    inline fn isFaulted(self: Self, sock: Socket) bool {
        if (self.except_fd_set.getSelectPointer()) |ptr| {
            return windows.funcs.__WSAFDIsSet(sock.internal, ptr) != 0;
        }
        return false;
    }
};

/// Waits until sockets in SocketSet are ready to read/write or have a fault condition.
/// If `timeout` is not `null`, it describes a timeout in nanoseconds until the function
/// should return.
/// Note that `timeout` granularity may not be available in nanoseconds and larger
/// granularities are used.
/// If the requested timeout interval requires a finer granularity than the implementation supports, the
/// actual timeout interval shall be rounded up to the next supported value.
pub fn waitForSocketEvent(set: *SocketSet, timeout: ?u64) !usize {
    switch (std.builtin.os.tag) {
        .windows => {
            const read_set = try set.internal.getFdSet(.read);
            const write_set = try set.internal.getFdSet(.write);
            const except_set = try set.internal.getFdSet(.except);
            if (read_set == null and write_set == null and except_set == null) return 0;

            const tm: windows.timeval = if (timeout) |tout| block: {
                const secs = @divFloor(tout, std.time.ns_per_s);
                const usecs = @divFloor(tout - secs * std.time.ns_per_s, 1000);
                break :block .{ .tv_sec = @intCast(c_long, secs), .tv_usec = @intCast(c_long, usecs) };
            } else .{ .tv_sec = 0, .tv_usec = 0 };

            // Windows ignores first argument.
            return try windows.select(0, read_set, write_set, except_set, if (timeout != null) &tm else null);
        },
        .linux, .macosx, .ios, .watchos, .tvos => return try std.os.poll(
            set.internal.fds.items,
            if (timeout) |val| @intCast(i32, (val + std.time.ns_per_s - 1) / std.time.ns_per_s) else -1,
        ),
        else => @compileError("unsupported os " ++ @tagName(std.builtin.os.tag) ++ " for SocketSet!"),
    }
}

const GetPeerNameError = error{
    /// Insufficient resources were available in the system to perform the operation.
    SystemResources,
    NotConnected,
} || std.os.UnexpectedError;

fn getpeername(sockfd: std.os.fd_t, addr: *std.os.sockaddr, addrlen: *std.os.socklen_t) GetPeerNameError!void {
    switch (std.os.errno(std.os.system.getpeername(sockfd, addr, addrlen))) {
        0 => return,
        else => |err| return std.os.unexpectedErrno(err),

        std.os.EBADF => unreachable, // always a race condition
        std.os.EFAULT => unreachable,
        std.os.EINVAL => unreachable, // invalid parameters
        std.os.ENOTSOCK => unreachable,
        std.os.ENOBUFS => return error.SystemResources,
        std.os.ENOTCONN => return error.NotConnected,
    }
}

pub fn connectToHost(
    allocator: *std.mem.Allocator,
    name: []const u8,
    port: u16,
    protocol: Protocol,
) !Socket {
    const endpoint_list = try getEndpointList(allocator, name, port);
    defer endpoint_list.deinit();

    for (endpoint_list.endpoints) |endpt| {
        var sock = try Socket.create(@as(AddressFamily, endpt.address), protocol);
        sock.connect(endpt) catch {
            sock.close();
            continue;
        };
        return sock;
    }

    return error.CouldNotConnect;
}

pub const EndpointList = struct {
    arena: std.heap.ArenaAllocator,
    endpoints: []EndPoint,
    canon_name: ?[]u8,

    pub fn deinit(self: *EndpointList) void {
        var arena = self.arena;
        arena.deinit();
    }
};

// Code adapted from std.net

/// Call `EndpointList.deinit` on the result.
pub fn getEndpointList(allocator: *std.mem.Allocator, name: []const u8, port: u16) !*EndpointList {
    const result = blk: {
        var arena = std.heap.ArenaAllocator.init(allocator);
        errdefer arena.deinit();

        const result = try arena.allocator.create(EndpointList);
        result.* = EndpointList{
            .arena = arena,
            .endpoints = undefined,
            .canon_name = null,
        };
        break :blk result;
    };
    const arena = &result.arena.allocator;
    errdefer result.arena.deinit();

    if (std.builtin.link_libc or is_windows) {
        const getaddrinfo_fn = if (is_windows) windows.getaddrinfo else libc_getaddrinfo;
        const freeaddrinfo_fn = if (is_windows) windows.funcs.freeaddrinfo else std.os.system.freeaddrinfo;
        const addrinfo = if (is_windows) windows.addrinfo else std.os.addrinfo;

        const AI_NUMERICSERV = if (is_windows) 0x00000008 else std.c.AI_NUMERICSERV;

        const name_c = try std.cstr.addNullByte(allocator, name);
        defer allocator.free(name_c);

        const port_c = try std.fmt.allocPrint(allocator, "{}\x00", .{port});
        defer allocator.free(port_c);

        const hints = addrinfo{
            .flags = AI_NUMERICSERV,
            .family = std.os.AF_UNSPEC,
            .socktype = std.os.SOCK_STREAM,
            .protocol = std.os.IPPROTO_TCP,
            .canonname = null,
            .addr = null,
            .addrlen = 0,
            .next = null,
        };

        var res: *addrinfo = undefined;
        try getaddrinfo_fn(name_c.ptr, @ptrCast([*:0]const u8, port_c.ptr), &hints, &res);
        defer freeaddrinfo_fn(res);

        const addr_count = blk: {
            var count: usize = 0;
            var it: ?*addrinfo = res;
            while (it) |info| : (it = info.next) {
                if (info.addr != null) {
                    count += 1;
                }
            }
            break :blk count;
        };
        result.endpoints = try arena.alloc(EndPoint, addr_count);

        var it: ?*addrinfo = res;
        var i: usize = 0;
        while (it) |info| : (it = info.next) {
            const sockaddr = info.addr orelse continue;
            const addr: Address = switch (sockaddr.family) {
                std.os.AF_INET => block: {
                    const bytes = @ptrCast(*const [4]u8, sockaddr.data[2..]);
                    break :block .{ .ipv4 = Address.IPv4.init(bytes[0], bytes[1], bytes[2], bytes[3]) };
                },
                std.os.AF_INET6 => block: {
                    const sockaddr_in6 = @ptrCast(*align(1) const std.os.sockaddr_in6, sockaddr);
                    break :block .{ .ipv6 = Address.IPv6.init(sockaddr_in6.addr, sockaddr_in6.scope_id) };
                },
                else => unreachable,
            };

            result.endpoints[i] = .{
                .address = addr,
                .port = port,
            };

            if (info.canonname) |n| {
                if (result.canon_name == null) {
                    result.canon_name = try std.mem.dupe(arena, u8, std.mem.spanZ(n));
                }
            }
            i += 1;
        }

        return result;
    }

    if (std.builtin.os.tag == .linux) {
        // Fall back to std.net
        const address_list = try std.net.getAddressList(allocator, name, port);
        defer address_list.deinit();

        if (address_list.canon_name) |cname| {
            result.canon_name = try std.mem.dupe(arena, u8, cname);
        }

        var count: usize = 0;
        for (address_list.addrs) |net_addr| {
            count += 1;
        }

        result.endpoints = try arena.alloc(EndPoint, count);

        var idx: usize = 0;
        for (address_list.addrs) |net_addr| {
            const addr: Address = switch (net_addr.any.family) {
                std.os.AF_INET => block: {
                    const bytes = @ptrCast(*const [4]u8, &net_addr.in.sa.addr);
                    break :block .{ .ipv4 = Address.IPv4.init(bytes[0], bytes[1], bytes[2], bytes[3]) };
                },
                std.os.AF_INET6 => .{ .ipv6 = Address.IPv6.init(net_addr.in6.sa.addr, net_addr.in6.sa.scope_id) },
                else => unreachable,
            };

            result.endpoints[idx] = EndPoint{
                .address = addr,
                .port = port,
            };
            idx += 1;
        }

        return result;
    }
    @compileError("unsupported os " ++ @tagName(std.builtin.os.tag) ++ " for getEndpointList!");
}

const GetAddrInfoError = error{
    HostLacksNetworkAddresses,
    TemporaryNameServerFailure,
    NameServerFailure,
    AddressFamilyNotSupported,
    OutOfMemory,
    UnknownHostName,
    ServiceUnavailable,
} || std.os.UnexpectedError;

fn libc_getaddrinfo(
    name: [*:0]const u8,
    port: [*:0]const u8,
    hints: *const std.os.addrinfo,
    result: **std.os.addrinfo,
) GetAddrInfoError!void {
    const rc = std.os.system.getaddrinfo(name, port, hints, result);
    if (rc != @intToEnum(std.os.system.EAI, 0))
        return switch (rc) {
            .ADDRFAMILY => return error.HostLacksNetworkAddresses,
            .AGAIN => return error.TemporaryNameServerFailure,
            .BADFLAGS => unreachable, // Invalid hints
            .FAIL => return error.NameServerFailure,
            .FAMILY => return error.AddressFamilyNotSupported,
            .MEMORY => return error.OutOfMemory,
            .NODATA => return error.HostLacksNetworkAddresses,
            .NONAME => return error.UnknownHostName,
            .SERVICE => return error.ServiceUnavailable,
            .SOCKTYPE => unreachable, // Invalid socket type requested in hints
            .SYSTEM => switch (std.os.errno(-1)) {
                else => |e| return std.os.unexpectedErrno(e),
            },
            else => unreachable,
        };
}

const windows = struct {
    usingnamespace std.os.windows;

    const timeval = extern struct {
        tv_sec: c_long,
        tv_usec: c_long,
    };

    const addrinfo = extern struct {
        flags: i32,
        family: i32,
        socktype: i32,
        protocol: i32,
        addrlen: std.os.socklen_t,
        canonname: ?[*:0]u8,
        addr: ?*std.os.sockaddr,
        next: ?*addrinfo,
    };

    const funcs = struct {
        extern "ws2_32" fn sendto(s: ws2_32.SOCKET, buf: [*c]const u8, len: c_int, flags: c_int, to: [*c]const std.os.sockaddr, tolen: std.os.socklen_t) callconv(.Stdcall) c_int;
        extern "ws2_32" fn send(s: ws2_32.SOCKET, buf: [*c]const u8, len: c_int, flags: c_int) callconv(.Stdcall) c_int;
        extern "ws2_32" fn recvfrom(s: ws2_32.SOCKET, buf: [*c]u8, len: c_int, flags: c_int, from: [*c]std.os.sockaddr, fromlen: [*c]std.os.socklen_t) callconv(.Stdcall) c_int;
        extern "ws2_32" fn listen(s: ws2_32.SOCKET, backlog: c_int) callconv(.Stdcall) c_int;
        extern "ws2_32" fn accept(s: ws2_32.SOCKET, addr: [*c]std.os.sockaddr, addrlen: [*c]std.os.socklen_t) callconv(.Stdcall) ws2_32.SOCKET;
        extern "ws2_32" fn setsockopt(s: ws2_32.SOCKET, level: c_int, optname: c_int, optval: [*c]const u8, optlen: c_int) callconv(.Stdcall) c_int;
        extern "ws2_32" fn getsockname(s: ws2_32.SOCKET, name: [*c]std.os.sockaddr, namelen: [*c]std.os.socklen_t) callconv(.Stdcall) c_int;
        extern "ws2_32" fn getpeername(s: ws2_32.SOCKET, name: [*c]std.os.sockaddr, namelen: [*c]std.os.socklen_t) callconv(.Stdcall) c_int;
        extern "ws2_32" fn select(nfds: c_int, readfds: ?*c_void, writefds: ?*c_void, exceptfds: ?*c_void, timeout: [*c]const timeval) callconv(.Stdcall) c_int;
        extern "ws2_32" fn __WSAFDIsSet(arg0: ws2_32.SOCKET, arg1: [*]u8) c_int;
        extern "ws2_32" fn bind(s: ws2_32.SOCKET, addr: [*c]const std.os.sockaddr, namelen: std.os.socklen_t) callconv(.Stdcall) c_int;
        extern "ws2_32" fn getaddrinfo(nodename: [*:0]const u8, servicename: [*:0]const u8, hints: *const addrinfo, result: **addrinfo) callconv(.Stdcall) c_int;
        extern "ws2_32" fn freeaddrinfo(res: *addrinfo) callconv(.Stdcall) void;
    };

    fn socket(addr_family: u32, socket_type: u32, protocol: u32) std.os.SocketError!ws2_32.SOCKET {
        const sock = try WSASocketW(
            @intCast(i32, addr_family),
            @intCast(i32, socket_type),
            @intCast(i32, protocol),
            null,
            0,
            ws2_32.WSA_FLAG_OVERLAPPED,
        );

        if (std.io.is_async and std.event.Loop.instance != null) {
            const loop = std.event.Loop.instance.?;
            _ = try CreateIoCompletionPort(sock, loop.os_data.io_port, undefined, undefined);
        }

        return sock;
    }

    // @TODO Make this, listen, accept, bind etc (all but recv and sendto) asynchronous
    fn connect(sock: ws2_32.SOCKET, sock_addr: *const std.os.sockaddr, len: std.os.socklen_t) std.os.ConnectError!void {
        while (true) if (ws2_32.connect(sock, sock_addr, len) != 0) {
            return switch (ws2_32.WSAGetLastError()) {
                .WSAEACCES => error.PermissionDenied,
                .WSAEADDRINUSE => error.AddressInUse,
                .WSAEINPROGRESS => error.WouldBlock,
                .WSAEALREADY => unreachable,
                .WSAEAFNOSUPPORT => error.AddressFamilyNotSupported,
                .WSAECONNREFUSED => error.ConnectionRefused,
                .WSAEFAULT => unreachable,
                .WSAEINTR => continue,
                .WSAEISCONN => unreachable,
                .WSAENETUNREACH => error.NetworkUnreachable,
                .WSAEHOSTUNREACH => error.NetworkUnreachable,
                .WSAENOTSOCK => unreachable,
                .WSAETIMEDOUT => error.ConnectionTimedOut,
                .WSAEWOULDBLOCK => error.WouldBlock,
                else => |err| return unexpectedWSAError(err),
            };
        } else return;
    }

    fn close(sock: ws2_32.SOCKET) void {
        if (ws2_32.closesocket(sock) != 0) {
            switch (ws2_32.WSAGetLastError()) {
                .WSAENOTSOCK => unreachable,
                .WSAEINPROGRESS => unreachable,
                else => return,
            }
        }
    }

    fn sendto(
        sock: ws2_32.SOCKET,
        buf: []const u8,
        flags: u32,
        dest_addr: ?*const std.os.sockaddr,
        addrlen: std.os.socklen_t,
    ) std.os.SendError!usize {
        if (std.io.is_async and std.event.Loop.instance != null) {
            const loop = std.event.Loop.instance.?;

            const Const_WSABUF = extern struct {
                len: ULONG,
                buf: [*]const u8,
            };

            var wsa_buf = Const_WSABUF{
                .len = @intCast(ULONG, buf.len),
                .buf = buf.ptr,
            };

            var resume_node = std.event.Loop.ResumeNode.Basic{
                .base = .{
                    .id = .Basic,
                    .handle = @frame(),
                    .overlapped = std.event.Loop.ResumeNode.overlapped_init,
                },
            };

            loop.beginOneEvent();
            suspend {
                _ = ws2_32.WSASendTo(
                    sock,
                    @ptrCast([*]ws2_32.WSABUF, &wsa_buf),
                    1,
                    null,
                    @intCast(DWORD, flags),
                    dest_addr,
                    addrlen,
                    @ptrCast(*ws2_32.WSAOVERLAPPED, &resume_node.base.overlapped),
                    null,
                );
            }
            var bytes_transferred: DWORD = undefined;
            if (kernel32.GetOverlappedResult(sock, &resume_node.base.overlapped, &bytes_transferred, FALSE) == 0) {
                switch (kernel32.GetLastError()) {
                    .IO_PENDING => unreachable,
                    // TODO Handle more errors
                    else => |err| return unexpectedError(err),
                }
            }
            return bytes_transferred;
        }

        while (true) {
            const result = funcs.sendto(sock, buf.ptr, @intCast(c_int, buf.len), @intCast(c_int, flags), dest_addr, addrlen);
            if (result == ws2_32.SOCKET_ERROR) {
                return switch (ws2_32.WSAGetLastError()) {
                    .WSAEACCES => error.AccessDenied,
                    .WSAECONNRESET => error.ConnectionResetByPeer,
                    .WSAEDESTADDRREQ => unreachable,
                    .WSAEFAULT => unreachable,
                    .WSAEINTR => continue,
                    .WSAEINVAL => unreachable,
                    .WSAEMSGSIZE => error.MessageTooBig,
                    .WSAENOBUFS => error.SystemResources,
                    .WSAENOTCONN => unreachable,
                    .WSAENOTSOCK => unreachable,
                    .WSAEOPNOTSUPP => unreachable,
                    else => |err| return unexpectedWSAError(err),
                };
            }
            return @intCast(usize, result);
        }
    }

    fn send(
        sock: ws2_32.SOCKET,
        buf: []const u8,
        flags: u32,
    ) std.os.SendError!usize {
        return sendto(sock, buf, flags, null, 0);
    }

    fn recvfrom(
        sock: ws2_32.SOCKET,
        buf: []u8,
        flags: u32,
        src_addr: ?*std.os.sockaddr,
        addrlen: ?*std.os.socklen_t,
    ) std.os.RecvFromError!usize {
        if (std.io.is_async and std.event.Loop.instance != null) {
            const loop = std.event.Loop.instance.?;

            const wsa_buf = ws2_32.WSABUF{
                .len = @intCast(ULONG, buf.len),
                .buf = buf.ptr,
            };
            var lpFlags = @intCast(DWORD, flags);

            var resume_node = std.event.Loop.ResumeNode.Basic{
                .base = .{
                    .id = .Basic,
                    .handle = @frame(),
                    .overlapped = std.event.Loop.ResumeNode.overlapped_init,
                },
            };

            loop.beginOneEvent();
            suspend {
                _ = ws2_32.WSARecvFrom(
                    sock,
                    @ptrCast([*]const ws2_32.WSABUF, &wsa_buf),
                    1,
                    null,
                    &lpFlags,
                    src_addr,
                    addrlen,
                    @ptrCast(*ws2_32.WSAOVERLAPPED, &resume_node.base.overlapped),
                    null,
                );
            }

            var bytes_transferred: DWORD = undefined;
            if (kernel32.GetOverlappedResult(sock, &resume_node.base.overlapped, &bytes_transferred, FALSE) == 0) {
                switch (kernel32.GetLastError()) {
                    .IO_PENDING => unreachable,
                    // TODO Handle more errors here
                    .HANDLE_EOF => return @as(usize, bytes_transferred),
                    else => |err| return unexpectedError(err),
                }
            }
            return @as(usize, bytes_transferred);
        }

        while (true) {
            const result = funcs.recvfrom(sock, buf.ptr, @intCast(c_int, buf.len), @intCast(c_int, flags), src_addr, addrlen);
            if (result == ws2_32.SOCKET_ERROR) {
                return switch (ws2_32.WSAGetLastError()) {
                    .WSAEFAULT => unreachable,
                    .WSAEINVAL => unreachable,
                    .WSAEISCONN => unreachable,
                    .WSAENOTSOCK => unreachable,
                    .WSAESHUTDOWN => unreachable,
                    .WSAEOPNOTSUPP => unreachable,
                    .WSAEWOULDBLOCK => error.WouldBlock,
                    .WSAEINTR => continue,
                    else => |err| return unexpectedWSAError(err),
                };
            }
            return @intCast(usize, result);
        }
    }

    // TODO: std.os.ListenError is not pub.
    const ListenError = error{
        AddressInUse,
        FileDescriptorNotASocket,
        OperationNotSupported,
    } || std.os.UnexpectedError;

    fn listen(sock: ws2_32.SOCKET, backlog: u32) ListenError!void {
        const rc = funcs.listen(sock, @intCast(c_int, backlog));
        if (rc != 0) {
            return switch (ws2_32.WSAGetLastError()) {
                .WSAEADDRINUSE => error.AddressInUse,
                .WSAENOTSOCK => error.FileDescriptorNotASocket,
                .WSAEOPNOTSUPP => error.OperationNotSupported,
                else => |err| return unexpectedWSAError(err),
            };
        }
    }

    /// Ignores flags
    fn accept4(
        sock: ws2_32.SOCKET,
        addr: ?*std.os.sockaddr,
        addr_size: *std.os.socklen_t,
        flags: u32,
    ) std.os.AcceptError!ws2_32.SOCKET {
        while (true) {
            const result = funcs.accept(sock, addr, addr_size);
            if (result == ws2_32.INVALID_SOCKET) {
                return switch (ws2_32.WSAGetLastError()) {
                    .WSAEINTR => continue,
                    .WSAEWOULDBLOCK => error.WouldBlock,
                    .WSAECONNRESET => error.ConnectionAborted,
                    .WSAEFAULT => unreachable,
                    .WSAEINVAL => unreachable,
                    .WSAENOTSOCK => unreachable,
                    .WSAEMFILE => error.ProcessFdQuotaExceeded,
                    .WSAENOBUFS => error.SystemResources,
                    .WSAEOPNOTSUPP => unreachable,
                    else => |err| return unexpectedWSAError(err),
                };
            }

            if (std.io.is_async and std.event.Loop.instance != null) {
                const loop = std.event.Loop.instance.?;
                _ = try CreateIoCompletionPort(result, loop.os_data.io_port, undefined, undefined);
            }

            return result;
        }
    }

    fn setsockopt(sock: ws2_32.SOCKET, level: u32, optname: u32, opt: []const u8) std.os.SetSockOptError!void {
        if (funcs.setsockopt(sock, @intCast(c_int, level), @intCast(c_int, optname), opt.ptr, @intCast(c_int, opt.len)) != 0) {
            return switch (ws2_32.WSAGetLastError()) {
                .WSAENOTSOCK => unreachable,
                .WSAEINVAL => unreachable,
                .WSAEFAULT => unreachable,
                .WSAENOPROTOOPT => error.InvalidProtocolOption,
                else => |err| return unexpectedWSAError(err),
            };
        }
    }

    fn getsockname(sock: ws2_32.SOCKET, addr: *std.os.sockaddr, addrlen: *std.os.socklen_t) std.os.GetSockNameError!void {
        if (funcs.getsockname(sock, addr, addrlen) != 0) {
            return unexpectedWSAError(ws2_32.WSAGetLastError());
        }
    }

    fn getpeername(sock: ws2_32.SOCKET, addr: *std.os.sockaddr, addrlen: *std.os.socklen_t) GetPeerNameError!void {
        if (funcs.getpeername(sock, addr, addrlen) != 0) {
            return switch (ws2_32.WSAGetLastError()) {
                .WSAENOTCONN => error.NotConnected,
                else => |err| return unexpectedWSAError(err),
            };
        }
    }

    pub const SelectError = error{FileDescriptorNotASocket} || std.os.UnexpectedError;

    fn select(nfds: usize, read_fds: ?[*]u8, write_fds: ?[*]u8, except_fds: ?[*]u8, timeout: ?*const timeval) SelectError!usize {
        while (true) {
            // Windows ignores nfds so we just pass zero here.
            const result = funcs.select(0, read_fds, write_fds, except_fds, timeout);
            if (result == ws2_32.SOCKET_ERROR) {
                return switch (ws2_32.WSAGetLastError()) {
                    .WSAEFAULT => unreachable,
                    .WSAEINVAL => unreachable,
                    .WSAEINTR => continue,
                    .WSAENOTSOCK => error.FileDescriptorNotASocket,
                    else => |err| return unexpectedWSAError(err),
                };
            }
            return @intCast(usize, result);
        }
    }

    fn bind(sock: ws2_32.SOCKET, addr: *const std.os.sockaddr, namelen: std.os.socklen_t) std.os.BindError!void {
        if (funcs.bind(sock, addr, namelen) != 0) {
            return switch (ws2_32.WSAGetLastError()) {
                .WSAEACCES => error.AccessDenied,
                .WSAEADDRINUSE => error.AddressInUse,
                .WSAEINVAL => unreachable,
                .WSAENOTSOCK => unreachable,
                .WSAEADDRNOTAVAIL => error.AddressNotAvailable,
                .WSAEFAULT => unreachable,
                else => |err| return unexpectedWSAError(err),
            };
        }
    }

    fn getaddrinfo(
        name: [*:0]const u8,
        port: [*:0]const u8,
        hints: *const addrinfo,
        result: **addrinfo,
    ) GetAddrInfoError!void {
        const rc = funcs.getaddrinfo(name, port, hints, result);
        if (rc != 0)
            return switch (ws2_32.WSAGetLastError()) {
                .WSATRY_AGAIN => error.TemporaryNameServerFailure,
                .WSAEINVAL => unreachable,
                .WSANO_RECOVERY => error.NameServerFailure,
                .WSAEAFNOSUPPORT => error.AddressFamilyNotSupported,
                .WSA_NOT_ENOUGH_MEMORY => error.OutOfMemory,
                .WSAHOST_NOT_FOUND => error.UnknownHostName,
                .WSATYPE_NOT_FOUND => error.ServiceUnavailable,
                .WSAESOCKTNOSUPPORT => unreachable,
                else => |err| return unexpectedWSAError(err),
            };
    }
};
