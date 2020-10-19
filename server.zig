const std = @import("std");
const assert = std.debug.assert;
const builtin = std.builtin;
const log = std.log;
const mem = std.mem;
const net = std.net;
const os = std.os;
const linux = os.linux;

const config = @import("config.zig");

const log_level: std.log.Level = @intToEnum(std.log.Level, config.log_level);

usingnamespace @import("connections.zig");
usingnamespace @import("io_uring.zig");
usingnamespace @import("types.zig");

const Event = packed struct {
    op: enum(u32) {
        Accept,
        Recv,
        Send,
        Close,
    },
    connection_id: u32
};

var connections: Connections = undefined;

fn accept(ring: *IO_Uring, fd: os.fd_t, addr: *os.sockaddr, addr_len: *os.socklen_t) !void {
    assert(connections.accepting == false);
    assert(connections.available());
    // TODO We use a runtime cast of fd to work around a packed struct default value bug here:
    // The Accept op does not need a connection_id, it's meaningless but we must provide something.
    // The compiler error is "TODO buf_read_value_bytes enum auto".
    const event = Event { .op = .Accept, .connection_id = @intCast(u32, fd) };
    const user_data = @bitCast(u64, event);
    _ = try ring.accept(user_data, fd, addr, addr_len, os.SOCK_CLOEXEC);
    connections.accepting = true;
}

fn accept_completed(ring: *IO_Uring, cqe: *const io_uring_cqe) !void {
    // The Accept SQE has been fulfilled, and we are therefore no longer accepting connections:
    // This may change at the end of the event loop, but for now we reflect this state immediately.
    assert(connections.accepting == true);
    connections.accepting = false;
    // TODO Return detailed errors for accept():
    if (cqe.res < 0) return os.unexpectedErrno(@intCast(usize, -cqe.res));
    // Create a connection for this socket fd:
    var connection = try connections.set(cqe.res);
    log.debug("connection {}: accepted fd={}", .{ connection.id, connection.fd });
    assert(connection.fd == cqe.res);
    assert(connection.references == 1);
    assert(connection.recv_size == 0);
    assert(connection.send_offset == 0);
    assert(connection.send_size == 0);
    // Read from this connection:
    try recv(ring, connection);
}

fn recv(ring: *IO_Uring, connection: *Connection) !void {
    const event = Event { .op = .Recv, .connection_id = connection.id };
    const user_data = @bitCast(u64, event);
    assert(connection.fd >= 0);
    assert(connection.references == 1);
    connection.references += 1;
    const offset = connection.recv_size;
    log.debug("connection {}: recv[{}..]", .{ connection.id, offset });
    _ = try ring.recv(user_data, connection.fd, connection.recv[offset..], os.MSG_NOSIGNAL);
}

fn recv_completed(ring: *IO_Uring, cqe: *const io_uring_cqe, connection: *Connection) !void {
    assert(connection.references == 2);
    connection.references -= 1;
    if (cqe.res < 0) {
        switch (-cqe.res) {
            // The connection was forcibly closed by the peer:
            os.ECONNRESET => try close(ring, connection, "ECONNRESET"),
            // The connection timed out due to a transmission timeout:
            os.ETIMEDOUT => try close(ring, connection, "ETIMEDOUT"),
            // Shut everything down, we didn't expect this:
            else => |errno| return os.unexpectedErrno(@intCast(usize, errno))
        }
    } else if (cqe.res == 0) {
        try close(ring, connection, "peer performed an orderly shutdown");
    } else {
        // parse() uses `prev_recv_size` to gauge progress, and whether the header must be verified.
        const prev_recv_size: usize = connection.recv_size;
        connection.recv_size += @intCast(usize, cqe.res);
        try parse(ring, connection, prev_recv_size);
    }
}

fn parse(ring: *IO_Uring, connection: *Connection, prev_recv_size: usize) !void {
    log.debug("connection {}: parse: recv_size={}", .{ connection.id, connection.recv_size });

    assert(connection.references == 1);
    assert(connection.recv_size > 0);
    assert(connection.recv_size <= connection.recv.len);
    assert(connection.send_offset == 0);
    assert(connection.send_size == 0);
    assert(prev_recv_size < connection.recv_size);

    // Header is incomplete, read more...
    if (connection.recv_size < @sizeOf(NetworkHeader)) {
        // The receive buffer must be able to accommodate a header:
        // Otherwise we might enter an infinite recv loop.
        assert(connection.recv.len >= @sizeOf(NetworkHeader));
        return try recv(ring, connection);
    }

    // Slice the receive buffer, then dereference the slice pointer to an array before casting:
    var header = @bitCast(NetworkHeader, connection.recv[0..@sizeOf(NetworkHeader)].*);

    // This is the first time we have the complete header, verify magic and header:
    if (prev_recv_size < @sizeOf(NetworkHeader)) {
        log.debug(
            "connection {}: parse: meta={x} data={x} id={} magic={x} command={} data_size={}",
            .{
                connection.id,
                header.checksum_meta,
                header.checksum_data,
                header.id,
                header.magic,
                header.command,
                header.data_size
            }
        );
        if (header.magic != NetworkMagic) return try close(ring, connection, "corrupt magic");
        if (!header.valid_checksum_meta()) return try close(ring, connection, "corrupt header");
        if (!header.valid_data_size()) return try close(ring, connection, "wrong data size");
    }

    // We can only trust `data_size` here after `checksum_meta` has been verified above:
    // We must be sure that `data_size` is not corrupt, since this influences our calls to recv().
    const request_size: usize = @sizeOf(NetworkHeader) + header.data_size;

    // The peer is attempting to overflow the receive buffer (and `data_size` is not corrupt):
    if (request_size > connection.recv.len) return try close(ring, connection, "excess request");

    // Data is incomplete, read more (we know this can fit in the receive buffer)...
    if (connection.recv_size < request_size) return try recv(ring, connection);

    // We have the complete header and corresponding data, verify data:
    const data = connection.recv[@sizeOf(NetworkHeader)..request_size];
    if (!header.valid_checksum_data(data)) return try close(ring, connection, "corrupt data");
    
    // TODO Journal to disk
    // TODO Apply to state machine

    // Move any pipelined requests to the front of the receive buffer:
    // This allows the client to have requests inflight up to the size of the receive buffer, and to
    // submit another request as each request is acked, without waiting for all inflight requests
    // to be acked. This costs a memcpy but a copy is simpler than the alternative of a ring buffer.
    const pipe_size = connection.recv_size - request_size;
    if (pipe_size > 0) {
        // The source and target slices may overlap and care must therefore be taken to use a
        // forward loop and not a reverse loop so that the source slice is not overwritten when
        // copied to the target slice.
        const source = connection.recv[request_size..connection.recv_size];
        var target = connection.recv[0..pipe_size];
        assert(@ptrToInt(target.ptr) <= @ptrToInt(source.ptr));
        assert(target.len == source.len);
        mem.copy(u8, target, source);
    }

    // When send() completes it will call parse() instead of recv() if `recv_size` is non-zero:
    connection.recv_size = pipe_size;
    log.debug("connection {}: parse: pipe_size={}", .{ connection.id, pipe_size });
    
    // Ack (with zeroes only at present):
    connection.send_offset = 0;
    connection.send_size = 64;
    try send(ring, connection);
}

fn send(ring: *IO_Uring, connection: *Connection) !void {
    const event = Event { .op = .Send, .connection_id = connection.id };
    const user_data = @bitCast(u64, event);
    assert(connection.fd >= 0);
    assert(connection.references == 1);
    connection.references += 1;
    const offset = connection.send_offset;
    const size = connection.send_size;
    log.debug("connection {}: send[{}..{}]", .{ connection.id, offset, size });
    _ = try ring.send(user_data, connection.fd, connection.send[offset..size], os.MSG_NOSIGNAL);
}

fn send_completed(ring: *IO_Uring, cqe: *const io_uring_cqe, connection: *Connection) !void {
    assert(connection.references == 2);
    connection.references -= 1;
    if (cqe.res < 0) {
        switch (-cqe.res) {
            // The connection was forcibly closed by the peer:
            os.ECONNRESET => try close(ring, connection, "ECONNRESET"),
            // The socket is shut down for writing, or the socket is no longer connected.
            os.EPIPE => try close(ring, connection, "EPIPE"),
            // Shut everything down, we didn't expect this:
            else => |errno| return os.unexpectedErrno(@intCast(usize, errno))
        }
    } else {
        connection.send_offset += @intCast(usize, cqe.res);

        // The send buffer was partially written to the kernel buffer (a short write), send more...
        if (connection.send_offset < connection.send_size) return try send(ring, connection);
        assert(connection.send_offset == connection.send_size);

        // Reset the send buffer to prepare for the next response:
        connection.send_offset = 0;
        connection.send_size = 0;

        // We have now done a request-response for this connection using static buffers. We don't
        // queue reads before we have acked a batch, because otherwise a client could run ahead of
        // us and exhaust resources. This gives us safe flow control. In other words, a connection
        // only has a single recv/send SQE pending at any time.
        if (connection.recv_size > 0) {
            try parse(ring, connection, 0);
        } else {
            try recv(ring, connection);
        }
    }
}

fn close(ring: *IO_Uring, connection: *Connection, reason: []const u8) !void {
    log.debug("connection {}: closing after {}...", .{ connection.id, reason });
    const event = Event { .op = .Close, .connection_id = connection.id };
    const user_data = @bitCast(u64, event);
    assert(connection.fd >= 0);
    assert(connection.references == 1);
    connection.references += 1;
    _ = try ring.close(user_data, connection.fd);
}

fn close_completed(ring: *IO_Uring, cqe: *const io_uring_cqe, connection: *Connection) !void {
    assert(connection.references == 2);
    connection.references -= 1;
    if (cqe.res < 0) return os.unexpectedErrno(@intCast(usize, -cqe.res));
    log.debug("connection {}: closed", .{ connection.id });
    try connections.unset(connection.id);
}

fn event_loop(ring: *IO_Uring, server: os.fd_t) !void {
    var cqes: [128]io_uring_cqe = undefined;
    var accept_addr: os.sockaddr = undefined;
    var accept_addr_len: os.socklen_t = @sizeOf(@TypeOf(accept_addr));

    while (true) {
        const count = try ring.copy_cqes(cqes[0..], 0);
        var i: usize = 0;
        while (i < count) : (i += 1) {
            const cqe = cqes[i];
            const event = @bitCast(Event, cqe.user_data);
            switch (event.op) {
                .Accept => try accept_completed(ring, &cqe),
                .Recv => try recv_completed(ring, &cqe, try connections.get(event.connection_id)),
                .Send => try send_completed(ring, &cqe, try connections.get(event.connection_id)),
                .Close => try close_completed(ring, &cqe, try connections.get(event.connection_id))
            }
        }
        // Decide whether or not to accept another connection here in one place, since there are
        // otherwise many branches above where connections can be closed making space available.
        if (!connections.accepting and connections.available()) {
            try accept(ring, server, &accept_addr, &accept_addr_len);
        }
        // Submit all queued syscalls and wait for at least one completion event before looping.
        _ = try ring.submit_and_wait(1);
    }
}

fn tcp_server_init(address: net.Address) !os.fd_t {
    const fd = try os.socket(address.any.family, os.SOCK_STREAM | os.SOCK_CLOEXEC, os.IPPROTO_TCP);
    errdefer os.close(fd);

    try set_socket_option(fd, os.SOL_SOCKET, os.SO_REUSEADDR, 1);

    if (config.tcp_rcvbuf > 0) {
        // Requires CAP_NET_ADMIN privilege (settle for SO_RCVBUF in the event of an EPERM):
        set_socket_option(fd, os.SOL_SOCKET, os.SO_RCVBUFFORCE, config.tcp_rcvbuf) catch |err| {
            if (err != error.PermissionDenied) return err;
            try set_socket_option(fd, os.SOL_SOCKET, os.SO_RCVBUF, config.tcp_rcvbuf);
        };
    }
    if (config.tcp_sndbuf > 0) {
        // Requires CAP_NET_ADMIN privilege (settle for SO_SNDBUF in the event of an EPERM):
        set_socket_option(fd, os.SOL_SOCKET, os.SO_SNDBUFFORCE, config.tcp_sndbuf) catch |err| {
            if (err != error.PermissionDenied) return err;
            try set_socket_option(fd, os.SOL_SOCKET, os.SO_SNDBUF, config.tcp_sndbuf);
        };
    }

    if (config.tcp_keepalive) {
        try set_socket_option(fd, os.SOL_SOCKET, os.SO_KEEPALIVE, 1);
        try set_socket_option(fd, os.IPPROTO_TCP, os.TCP_KEEPIDLE, config.tcp_keepidle);
        try set_socket_option(fd, os.IPPROTO_TCP, os.TCP_KEEPINTVL, config.tcp_keepintvl);
        try set_socket_option(fd, os.IPPROTO_TCP, os.TCP_KEEPCNT, config.tcp_keepcnt);
    }
    if (config.tcp_user_timeout > 0) {
        try set_socket_option(fd, os.IPPROTO_TCP, os.TCP_USER_TIMEOUT, config.tcp_user_timeout);
    }

    if (config.tcp_nodelay) {
        try set_socket_option(fd, os.IPPROTO_TCP, os.TCP_NODELAY, 1);
    }

    // TODO Use getsockopt to log the final value of these socket options.

    try os.bind(fd, &address.any, address.getOsSockLen());
    try os.listen(fd, config.tcp_backlog);
    return fd;
}

// We use a u31 for the option value to keep the sign bit clear when we cast to c_int.
fn set_socket_option(fd: os.fd_t, level: u32, option: u32, value: u31) !void {
    // TODO Submit a PR to handle EPERM in os.zig's definition of setsockopt().
    var value_bytes = mem.toBytes(@as(c_int, value));
    var value_bytes_len = @intCast(linux.socklen_t, value_bytes.len);
    const res = linux.setsockopt(fd, level, option, &value_bytes, value_bytes_len);
    switch (linux.getErrno(res)) {
        0 => {},
        linux.EPERM => return error.PermissionDenied,
        linux.EDOM => return error.TimeoutTooBig,
        linux.EISCONN => return error.AlreadyConnected,
        linux.ENOPROTOOPT => return error.InvalidProtocolOption,
        linux.ENOMEM => return error.SystemResources,
        linux.ENOBUFS => return error.SystemResources,
        else => |errno| return os.unexpectedErrno(errno),
    }
}

pub fn main() !void {
    // TODO Log all config variables at debug level at startup.

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    var allocator = &arena.allocator;

    connections = try Connections.init(allocator, config.tcp_connections_max);
    defer connections.deinit();

    // TODO Formalize the relation between `config.tcp_connections_max` and ring entries:
    var ring = try IO_Uring.init(128, 0);
    defer ring.deinit();

    var addr = try net.Address.parseIp4("0.0.0.0", config.port);
    var server = try tcp_server_init(addr);
    defer os.close(server);
    log.info("listening on {}...", .{ addr });

    try event_loop(&ring, server);
}

comptime {
    switch (config.deployment_environment) {
        .development => {},
        .staging => {},
        .production => {},
        else => @compileError("config: unknown deployment_environment")
    }
    if (
        config.tcp_user_timeout >
        (config.tcp_keepidle + config.tcp_keepintvl * config.tcp_keepcnt) * 1000
    ) {
        @compileError("config: tcp_user_timeout would cause tcp_keepcnt to be extended");
    }
    // TODO Add safety checks on all config variables and interactions between them.
    // TODO Move this to types.zig or somewhere common to all code.
}
