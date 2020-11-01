const std = @import("std");
const assert = std.debug.assert;
const builtin = std.builtin;
const log = std.log;
const mem = std.mem;
const net = std.net;
const os = std.os;
const linux = os.linux;

const config = @import("config.zig");

pub const log_level: std.log.Level = @intToEnum(std.log.Level, config.log_level);

usingnamespace @import("connections.zig");
usingnamespace @import("io_uring.zig");
usingnamespace @import("types.zig");
usingnamespace @import("journal.zig");
usingnamespace @import("master.zig");
usingnamespace @import("state.zig");

var master: Master = undefined;
var state: State = undefined;
var journal: Journal = undefined;
var connections: Connections = undefined;

const Event = packed struct {
    op: enum(u32) {
        Accept,
        Recv,
        Send,
        Close,
    },
    connection_id: u32
};

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
    // We limit requests (as well as all inflight data) to request_size_max and not recv.len:
    // The difference between recv.len and request_size_max is reserved for sector padding.
    _ = try ring.recv(
        user_data,
        connection.fd,
        connection.recv[offset..config.request_size_max],
        os.MSG_NOSIGNAL
    );
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
    assert(connection.recv_size <= config.request_size_max);
    assert(connection.recv.len == config.request_size_max + (config.sector_size * 2));
    assert(connection.send_offset == 0);
    assert(connection.send_size == 0);
    assert(prev_recv_size < connection.recv_size);

    // The request header is incomplete, read more...
    if (connection.recv_size < @sizeOf(NetworkHeader)) return try recv(ring, connection);

    // Deserialize the request header with a single cast and a copy to stack allocated memory:
    // We use bytesToValue() and not bytesAsValue() because we want a copy of the network header to
    // survive the header's memory being reused by Journal.append() for the journal header.
    const request = mem.bytesToValue(NetworkHeader, connection.recv[0..@sizeOf(NetworkHeader)]);

    // This is the first time we have received the complete request header, verify magic and header:
    if (prev_recv_size < @sizeOf(NetworkHeader)) {
        log.debug("connection {}: parse: request: {}", .{ connection.id, request });
        if (request.magic != Magic) return try close(ring, connection, "corrupt magic");
        if (!request.valid_checksum_meta()) return try close(ring, connection, "corrupt header");
        // We can only trust `size` here after `checksum_meta` has been verified above:
        // We must be sure that `size` is not corrupt, since this influences our calls to recv().
        if (!request.valid_size()) return try close(ring, connection, "invalid size");
    }

    // The peer would exceed limits or even overflow the receive buffer (and `size` is not corrupt):
    if (request.size > config.request_size_max) {
        return try close(ring, connection, "request exceeded config.request_size_max");
    }

    // Data is incomplete, read more (we know this can fit in the receive buffer)...
    if (connection.recv_size < request.size) return try recv(ring, connection);

    // We have the complete request header and corresponding data, verify data:
    var request_data = connection.recv[@sizeOf(NetworkHeader)..request.size];
    if (!request.valid_checksum_data(request_data)) {
        return try close(ring, connection, "corrupt data");
    }

    // Assign strictly increasing event timestamps according to the master's clock:
    if (!master.assign_timestamps(request.command, request_data)) {
        return try close(ring, connection, "reserved timestamp not zero");
    }

    // Zero pad the request out to a sector multiple, required by the journal for direct I/O:
    // The journal also adds another sector for the EOF entry.
    const request_entry_size = Journal.entry_size(request.size, config.sector_size);
    if (request.size != request_entry_size) {
        assert(request_entry_size > request.size);
        const padding_size = request_entry_size - request.size;
        assert(connection.recv_size + padding_size <= connection.recv.len);
        // Don't overwrite any pipelined data, shift the pipeline to the right:
        if (connection.recv_size - request.size > 0) {
            // These slices overlap, with dest.ptr > src.ptr, so we must use a reverse loop:
            mem.copyBackwards(
                u8,
                connection.recv[request_entry_size..],
                connection.recv[request.size..connection.recv_size]
            );
        }
        // Zero the padding:
        mem.set(u8, connection.recv[request.size..request_entry_size], 0);
        connection.recv_size += padding_size;
    }

    try journal.append(request.command, request.size, connection.recv[0..request_entry_size]);

    // Apply as input to state machine, writing any response data directly to the send buffer:
    const response_data_size = state.apply(
        request.command,
        request_data,
        connection.send[@sizeOf(NetworkHeader)..]
    );

    // Write the response header to the send buffer:
    var response = mem.bytesAsValue(NetworkHeader, connection.send[0..@sizeOf(NetworkHeader)]);
    response.* = .{
        .id = request.id,
        .command = .ack,
        .size = @sizeOf(NetworkHeader) + @intCast(u32, response_data_size)
    };
    assert(response.valid_size());
    response.set_checksum_data(connection.send[@sizeOf(NetworkHeader)..response.size]);
    response.set_checksum_meta();
    log.debug("connection {}: parse: response: {}", .{ connection.id, response });

    // Move any pipelined requests to the front of the receive buffer:
    // This allows the client to have requests inflight up to the size of the receive buffer, and to
    // submit another request as each request is acked, without waiting for all inflight requests
    // to be acked. This costs a copy, but that is justified by the network performance gain.
    const pipeline_size = connection.recv_size - request_entry_size;
    if (pipeline_size > 0) {
        // These slices overlap, with dest.ptr < src.ptr, so we must use a forward loop:
        mem.copy(
            u8,
            connection.recv[0..],
            connection.recv[request_entry_size..connection.recv_size]
        );
    }

    // When send() completes it will call parse() instead of recv() if `recv_size` is non-zero:
    connection.recv_size = pipeline_size;
    log.debug("connection {}: parse: pipeline_size={}", .{ connection.id, pipeline_size });
    
    // Ack:
    connection.send_offset = 0;
    connection.send_size = response.size;
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
    assert(size > 0);
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

fn tcp_server_init() !os.fd_t {
    var address = try net.Address.parseIp4(config.bind_address, config.port);

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

    var port: u16 = address.getPort();
    var attempts: usize = if (config.port_hopping) 32 else 1;
    while (attempts > 0) {
        attempts -= 1;
        if (os.bind(fd, &address.any, address.getOsSockLen())) {
            try os.listen(fd, config.tcp_backlog);
            log.info("listening on {}", .{ address });
            return fd;
        } else |err| switch (err) {
            error.AddressInUse => {
                if (attempts == 0 or port == 65535) return err;
                port += 1;
                log.info("port {} is in use, port hopping to {}...", .{ address.getPort(), port });
                address.setPort(port);
            },
            else => return err
        }
    }
    unreachable;
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

    master = try Master.init();
    defer master.deinit();

    state = try State.init(allocator, config.accounts_max, config.transfers_max);
    defer state.deinit();

    journal = try Journal.init(&state);
    defer journal.deinit();

    connections = try Connections.init(allocator, config.connections_max);
    defer connections.deinit();

    // TODO Formalize the relation between `config.connections_max` and ring entries:
    var ring = try IO_Uring.init(128, 0);
    defer ring.deinit();

    var server = try tcp_server_init();
    defer os.close(server);

    try event_loop(&ring, server);
}

comptime {
    switch (config.deployment_environment) {
        .development => {},
        .production => {},
        else => @compileError("config: unknown deployment_environment")
    }

    if (
        config.tcp_user_timeout >
        (config.tcp_keepidle + config.tcp_keepintvl * config.tcp_keepcnt) * 1000
    ) {
        @compileError("config: tcp_user_timeout would cause tcp_keepcnt to be extended");
    }

    if (config.sector_size < 4096) {
        @compileError("config: sector_size must be at least 4096 bytes for Advanced Format disks");
    }
    if (!std.math.isPowerOfTwo(config.sector_size)) {
        @compileError("config: sector_size must be a power of two");
    }

    if (config.request_size_max <= @sizeOf(NetworkHeader)) {
        @compileError("config: request_size_max must be more than a network header");
    }
    if (@mod(config.request_size_max, config.sector_size) != 0) {
        @compileError("config: request_size_max must be a multiple of sector_size");
    }

    if (@mod(config.journal_size_max, config.sector_size) != 0) {
        @compileError("config: journal_size_max must be a multiple of sector_size");
    }
    if (@mod(config.journal_size_max, config.request_size_max) != 0) {
        @compileError("config: journal_size_max must be a multiple of request_size_max");
    }
    // TODO Add safety checks on all config variables and interactions between them.
    // TODO Move this to types.zig or somewhere common to all code.
    // TODO Persist critical config variables (e.g. sector_size, request_size_max) to metainfo.
    // TODO Detect changes in critical config variables (check these against metainfo at runtime).
}
