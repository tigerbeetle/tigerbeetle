const std = @import("std");
const assert = std.debug.assert;
const builtin = std.builtin;
const mem = std.mem;
const net = std.net;
const os = std.os;
const linux = os.linux;

usingnamespace @import("io_uring.zig");

const port = 3001;
const connections_max = 32;
const connection_buffers = @typeInfo(ConnectionBuffer).Enum.fields.len;
const buffer_max = 4 * 1024 * 1024;
const tcp_backlog = 64;

const ConnectionBuffer = enum(u8) {
    Recv,
    Send
};

const Connections = struct {
    accepting: bool = false,

    count: usize = 0,
    
    fds: [connections_max]os.fd_t = [_]os.fd_t{ -1 } ** connections_max,
    
    // We want send/recv buffers to be next to each other for optimal memory locality.
    // TODO Add test to assert that distance between send/recv buffers is at most buffer_max.
    buffers: [connections_max][connection_buffers][buffer_max]u8 = undefined,

    fn buffer(self: *Connections, connection: u32, purpose: ConnectionBuffer) []u8 {
        return self.buffers[connection][@enumToInt(purpose)][0..];
    }

    fn available(self: *Connections) bool {
        assert(self.count <= self.fds.len);
        return self.count < self.fds.len;
    }

    fn set(self: *Connections, fd: os.fd_t) !u32 {
        assert(fd >= 0);
        // We expect the TigerBeetle client to maintain long-lived connections.
        // We expect a thundering herd of N new connections at most every T seconds.
        // TODO Return an error if a high reconnection rate indicates the lack of client keep alive.
        if (!self.available()) return error.ConnectionLimitReached;
        var find = true;
        var slot: usize = undefined;
        for (self.fds) |value, index| {
            // Loop through all slots to assert against reuse, do not break for a free slot:
            if (value == fd) return error.ConnectionFileDescriptorReused;
            if (find and value == -1) {
                slot = index;
                find = false;
            }
        }
        self.fds[slot] = fd;
        self.count += 1;
        return @intCast(u32, slot);
    }

    fn unset(self: *Connections, connection: u32) void {
        assert(connection < self.fds.len);
        assert(self.count > 0);
        assert(self.fds[connection] >= 0);
        std.debug.print("removing fd {} from connections for connection {}...\n", .{ self.fds[connection], connection });
        self.fds[connection] = -1;
        self.count -= 1;
        std.debug.print("we now have {} connections\n", .{ self.count });
    }
};

var connections = Connections {};

const Event = packed struct {
    op: EventOp,
    connection: u32
};

const EventOp = enum(u32) {
    Accept,
    Recv,
    Send
};

fn accept(ring: *IO_Uring, fd: os.fd_t, addr: *os.sockaddr, addr_len: *os.socklen_t) !void {
    assert(connections.accepting == false);
    const event = Event { .op = .Accept, .connection = @intCast(u32, fd) };
    const user_data = @bitCast(u64, event);
    _ = try ring.accept(user_data, fd, addr, addr_len, 0);
    connections.accepting = true;
}

fn recv(ring: *IO_Uring, connection: u32, offset: u64) !void {
    const event = Event { .op = .Recv, .connection = connection };
    const user_data = @bitCast(u64, event);
    const fd = connections.fds[connection];
    if (fd < 0) return error.ConnectionFileDescriptorNotFound;
    var buffer = connections.buffer(connection, .Recv);
    std.debug.print("receiving into buffer {} from connection {}...\n", .{
        @enumToInt(ConnectionBuffer.Recv),
        connection
    });
    _ = try ring.recv(user_data, fd, buffer[offset..], os.MSG_NOSIGNAL);
}

fn send(ring: *IO_Uring, connection: u32, offset: u64, size: usize) !void {
    const event = Event { .op = .Send, .connection = connection };
    const user_data = @bitCast(u64, event);
    const fd = connections.fds[connection];
    if (fd < 0) return error.ConnectionFileDescriptorNotFound;
    var buffer = connections.buffer(connection, .Send);
    std.debug.print("sending from buffer {} to connection {}...\n", .{
        @enumToInt(ConnectionBuffer.Send),
        connection
    });
    _ = try ring.send(user_data, fd, buffer[offset..size], os.MSG_NOSIGNAL);
}

fn tcp_server_init(address: net.Address) !os.fd_t {
    const fd = try os.socket(address.any.family, os.SOCK_STREAM | os.SOCK_CLOEXEC, os.IPPROTO_TCP);
    errdefer os.close(fd);
    try os.setsockopt(fd, os.SOL_SOCKET, os.SO_REUSEADDR, &mem.toBytes(@as(c_int, 1)));
    try os.bind(fd, &address.any, address.getOsSockLen());
    try os.listen(fd, tcp_backlog);
    return fd;
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
            if (cqe.res < 0) {
                switch (-cqe.res) {
                    os.EPIPE => {
                        std.debug.print("connection destroyed, event={}\n", .{ cqe.user_data });
                    },
                    os.ECONNRESET => {
                        std.debug.print("connection reset, event={}\n", .{ cqe.user_data });
                    },
                    else => {
                        std.debug.print("ERROR user_data={} res={}\n", .{ cqe.user_data, cqe.res });
                        os.exit(1);
                    }
                }
                continue;
            }
            switch (event.op) {
                .Accept => {
                    // The Accept SQE has been fulfilled and we are no longer accepting connections:
                    assert(connections.accepting == true);
                    connections.accepting = false;
                    // TODO Handle errors for accept():
                    assert(cqe.res >= 0);
                    // Create a connection for this socket fd:
                    const connection = try connections.set(cqe.res);
                    // Read from this connection:
                    _ = try recv(ring, connection, 0);
                },
                .Recv => {
                    if (cqe.res == 0) {
                        // The peer performed an orderly shutdown.
                        connections.unset(event.connection);
                    } else {
                        var size = @intCast(usize, cqe.res);
                        
                        var source = connections.buffer(event.connection, .Recv);
                        var target = connections.buffer(event.connection, .Send);
                        mem.copy(u8, target[0..size], source[0..size]);

                        _ = try send(ring, event.connection, 0, size);
                    }
                },
                .Send => {
                    // We have now done a request-response for this connection using static buffers.
                    // We don't queue reads before we have acked a batch, because otherwise a client
                    // could run ahead of us and exhaust resources. This gives us safe flow control.
                    // In other words, a connection only has a single SQE pending at any time.
                    _ = try recv(ring, event.connection, 0);
                }
            }
        }
        // Decide whether or not to accept another connection here in one place, since there are
        // otherwise many branches where connections can be unset.
        if (!connections.accepting and connections.available()) {
            _ = try accept(ring, server, &accept_addr, &accept_addr_len);
        }
        _ = try ring.submit_and_wait(1);
    }
}

pub fn main() !void {
    if (builtin.os.tag != .linux) return error.LinuxRequired;

    var addr = try net.Address.parseIp4("0.0.0.0", port);
    var server = try tcp_server_init(addr);
    defer os.close(server);

    std.debug.print("listening on {}...\n", .{ addr });
    
    // TODO Formalize the relation between connections_max and ring entries:
    var ring = try IO_Uring.init(128, 0);
    defer ring.deinit();

    try event_loop_init(&ring, server);
}
