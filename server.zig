const std = @import("std");
const assert = std.debug.assert;
const builtin = std.builtin;
const mem = std.mem;
const net = std.net;
const os = std.os;
const linux = os.linux;

usingnamespace @import("connections.zig");
usingnamespace @import("io_uring.zig");

const port = 3001;
const connections_max = 32;
const tcp_backlog = 64;

const Event = packed struct {
    op: enum(u32) {
        Accept,
        Recv,
        Send
    },
    connection_id: u32
};

var connections: Connections = undefined;

fn accept(ring: *IO_Uring, fd: os.fd_t, addr: *os.sockaddr, addr_len: *os.socklen_t) !void {
    assert(connections.accepting == false);
    // TODO We use a runtime cast of fd to work around a packed struct default value bug here:
    // The Accept op does not need a connection_id, it's meaningless but we must provide something.
    const event = Event { .op = .Accept, .connection_id = @intCast(u32, fd) };
    const user_data = @bitCast(u64, event);
    _ = try ring.accept(user_data, fd, addr, addr_len, 0);
    connections.accepting = true;
}

fn accept_completed(ring: *IO_Uring, cqe: *const io_uring_cqe) !void {
    // The Accept SQE has been fulfilled, and we are therefore no longer accepting connections:
    // This may change at the end of the event loop, but for now we reflect this state immediately.
    assert(connections.accepting == true);
    connections.accepting = false;
    // TODO Return detailed errors for accept():
    // See https://man7.org/linux/man-pages/man2/accept.2.html
    if (cqe.res < 0) return os.unexpectedErrno(@intCast(usize, -cqe.res));
    // Create a connection for this socket fd:
    var connection = try connections.set(cqe.res);
    // Read from this connection:
    _ = try recv(ring, connection, 0);
}

fn recv(ring: *IO_Uring, connection: *Connection, offset: u64) !void {
    const event = Event { .op = .Recv, .connection_id = connection.id };
    const user_data = @bitCast(u64, event);
    assert(connection.fd >= 0);
    assert(connection.references == 1);
    connection.references += 1;
    _ = try ring.recv(user_data, connection.fd, connection.recv[offset..], os.MSG_NOSIGNAL);
}

fn recv_completed(ring: *IO_Uring, cqe: *const io_uring_cqe, connection: *Connection) !void {
    assert(connection.references == 2);
    connection.references -= 1;
    if (cqe.res < 0) {
        // See https://linux.die.net/man/3/recv
        switch (-cqe.res) {
            // The connection was forcibly closed by the peer:
            os.ECONNRESET => try connections.unset(connection.id),
            // The connection timed out due to a transmission timeout:
            os.ETIMEDOUT => try connections.unset(connection.id),
            // Shut everything down, we didn't expect this:
            else => |errno| return os.unexpectedErrno(@intCast(usize, errno))
        }
    } else if (cqe.res == 0) {
        // The peer performed an orderly shutdown.
        try connections.unset(connection.id);
    } else {
        // TODO Handle a short read, where we don't have enough.
        var size = @intCast(usize, cqe.res);
        mem.copy(u8, connection.send[0..size], connection.recv[0..size]);
        _ = try send(ring, connection, 0, size);
    }
}

fn send(ring: *IO_Uring, connection: *Connection, offset: u64, size: usize) !void {
    const event = Event { .op = .Send, .connection_id = connection.id };
    const user_data = @bitCast(u64, event);
    assert(connection.fd >= 0);
    assert(connection.references == 1);
    connection.references += 1;
    _ = try ring.send(user_data, connection.fd, connection.send[offset..size], os.MSG_NOSIGNAL);
}

fn send_completed(ring: *IO_Uring, cqe: *const io_uring_cqe, connection: *Connection) !void {
    assert(connection.references == 2);
    connection.references -= 1;
    if (cqe.res < 0) {
        // See https://linux.die.net/man/3/send
        switch (-cqe.res) {
            // The connection was forcibly closed by the peer:
            os.ECONNRESET => try connections.unset(connection.id),
            // The socket is shut down for writing, or the socket is no longer connected.
            os.EPIPE => try connections.unset(connection.id),
            // Shut everything down, we didn't expect this:
            else => |errno| return os.unexpectedErrno(@intCast(usize, errno))
        }
    } else {
        // We have now done a request-response for this connection using static buffers.
        // We don't queue reads before we have acked a batch, because otherwise a client
        // could run ahead of us and exhaust resources. This gives us safe flow control.
        // In other words, a connection has a single recv/send SQE pending at any time.
        _ = try recv(ring, connection, 0);
    }
}

fn event_loop_init(ring: *IO_Uring, server: os.fd_t) !void {
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
                .Send => try send_completed(ring, &cqe, try connections.get(event.connection_id))
            }
        }
        // Decide whether or not to accept another connection here in one place, since there are
        // otherwise many branches above where connections can be closed making space available.
        if (!connections.accepting and connections.available()) {
            _ = try accept(ring, server, &accept_addr, &accept_addr_len);
        }
        _ = try ring.submit_and_wait(1);
    }
}

fn tcp_server_init(address: net.Address) !os.fd_t {
    const fd = try os.socket(address.any.family, os.SOCK_STREAM | os.SOCK_CLOEXEC, os.IPPROTO_TCP);
    errdefer os.close(fd);
    try os.setsockopt(fd, os.SOL_SOCKET, os.SO_REUSEADDR, &mem.toBytes(@as(c_int, 1)));
    try os.bind(fd, &address.any, address.getOsSockLen());
    try os.listen(fd, tcp_backlog);
    return fd;
}

pub fn main() !void {
    if (builtin.os.tag != .linux) return error.LinuxRequired;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    var allocator = &arena.allocator;

    connections = try Connections.init(allocator, connections_max);
    defer connections.deinit();

    var addr = try net.Address.parseIp4("0.0.0.0", port);
    var server = try tcp_server_init(addr);
    defer os.close(server);

    std.debug.print("listening on {}...\n", .{ addr });
    
    // TODO Formalize the relation between connections_max and ring entries:
    var ring = try IO_Uring.init(128, 0);
    defer ring.deinit();

    try event_loop_init(&ring, server);
}
