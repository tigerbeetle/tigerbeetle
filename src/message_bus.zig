const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const os = std.os;

const vr = @import("vr.zig");
const ConfigurationAddress = vr.ConfigurationAddress;
const Header = vr.Header;
const Replica = vr.Replica;
const FIFO = @import("fifo.zig").FIFO;
const IO = @import("io_callbacks.zig").IO;

const log = std.log.scoped(.message_bus);

const tcp_backlog = 64;
const num_connections = 32;

pub const Message = struct {
    header: *Header,
    buffer: []u8 align(vr.sector_size),
    references: usize = 1,
    next: ?*Message = null,
};

const Envelope = struct {
    message: *Message,
    next: ?*Envelope = null,
};

// TODO: use a hasmap to make client lookups faster
pub const MessageBus = struct {
    allocator: *mem.Allocator,
    allocated: usize = 0,
    io: *IO,

    configuration: []std.net.Address,

    /// The replica which is running the server
    server: *Replica,
    server_fd: os.socket_t,
    /// Used to store messages sent by the server to itself for delivery in flush().
    server_send_queue: FIFO(Envelope) = .{},

    accept_completion: IO.Completion = undefined,
    accept_in_progress: bool = true,

    /// This slice is allocated with a fixed size in the init function and never reallocated.
    connections: []Connection,
    /// Number of connections currently in use (i.e. connection.peer != .none).
    connections_in_use: usize = 0,

    /// Map from replica index to the currently active connection for that replica, if any.
    /// The connection for the server replica will always be null.
    replicas: []?*Connection,
    /// Number of currently active replica connections in the connections slice.
    replicas_connected: usize = 0,

    /// Initialize the MessageBus for the given server replica and configuration.
    pub fn init(
        self: *MessageBus,
        allocator: *mem.Allocator,
        io: *IO,
        server: *Replica,
        configuration: []std.net.Address,
    ) !void {
        // There must be enough connections for all replicas and at least one client.
        assert(num_connections > configuration.len);

        const connections = try allocator.alloc(Connection, num_connections);
        errdefer allocator.free(connections);
        mem.set(Connection, connections, .{ .message_bus = self });

        const replicas = try allocator.alloc(?*Connection, configuration.len);
        errdefer allocator.free(replicas);
        mem.set(?*Connection, replicas, null);

        self.* = .{
            .allocator = allocator,
            .io = io,
            .configuration = configuration,
            .server = server,
            .server_fd = try init_tcp(configuration[server.replica]),
            .connections = connections,
            .replicas = replicas,
        };
    }

    fn init_tcp(address: std.net.Address) !os.socket_t {
        const fd = try os.socket(address.any.family, os.SOCK_STREAM | os.SOCK_CLOEXEC, os.IPPROTO_TCP);
        errdefer os.close(fd);
        // TODO: configure RCVBUF, SNDBUF, KEEPALIVE, TIMEOUT, NODELAY
        try os.setsockopt(fd, os.SOL_SOCKET, os.SO_REUSEADDR, &mem.toBytes(@as(c_int, 1)));
        // TODO: port hopping
        try os.bind(fd, &address.any, address.getOsSockLen());
        try os.listen(fd, tcp_backlog);
        return fd;
    }

    pub fn tick(self: *MessageBus) void {
        if (self.replicas_connected < self.replicas.len - 1) {
            // Each replica is responsible for connecting to replicas that come
            // after it in the configuration. This ensures that replicas never try
            // to connect to each other at the same time.
            var replica: u16 = self.server.replica + 1;
            while (replica < self.replicas.len) : (replica += 1) {
                self.maybe_connect_to_replica(replica);
            }
        }
        self.maybe_accept();
    }

    fn maybe_connect_to_replica(self: *MessageBus, replica: u16) void {
        // We already have a connection to the given replica.
        if (self.replicas[replica] != null) return;

        // Obtain a connection struct for our new replica connection.
        // If there is an unused connection, use that. Otherwise drop
        // a client or unknown connection to make space. Prefer dropping
        // a client connection to an unknown one as the unknown peer may
        // be a replica. Since shutting a connection down does not happen
        // instantly, simply return after starting the shutdown and try again
        // on the next tick().
        for (self.connections) |*connection| {
            if (connection.peer == .none) {
                connection.connect_to_replica(replica);
                return;
            }
        }

        log.notice("all connections in use but not all replicas are connected, " ++
            "attempting to disconnect a client", .{});
        for (self.connections) |*connection| {
            if (connection.peer == .client) {
                connection.shutdown();
                return;
            }
        }

        log.notice("failed to disconnect a client as no peer was a known client, " ++
            "attempting to disconnect an unknown peer.", .{});
        for (self.connections) |*connection| {
            if (connection.peer == .unknown) {
                connection.shutdown();
                return;
            }
        }

        // There's a very slim but non zero chance that this code will be ever reached.
        log.warn("failed to disconnect any peer as all peers are replicas " ++
            "or are already being disconnected.", .{});
    }

    fn maybe_accept(self: *MessageBus) void {
        if (self.accept_in_progress) return;
        // All conenctions are currently in use, do nothing.
        if (self.connections_in_use == self.connections.len) return;
        assert(self.connections_in_use < self.connections.len);
        self.accept_in_progress = true;
        self.io.accept(
            *MessageBus,
            self,
            on_accept,
            &self.accept_completion,
            self.server_fd,
            os.SOCK_CLOEXEC,
        );
    }

    fn on_accept(self: *MessageBus, completion: *IO.Completion, result: IO.AcceptError!os.socket_t) void {
        self.accept_in_progress = false;
        const fd = result catch |err| {

            // TODO: some errors should probably be fatal
            log.err("accept failed: {}", .{err});
            return;
        };
        if (self.connections_in_use < self.connections.len) {
            // Find an unused Connection to receive/send from the new peer.
            for (self.connections) |*connection| {
                if (connection.peer == .none) {
                    connection.on_accept(fd);
                    break;
                }
            } else unreachable;
        } else {
            assert(self.connections_in_use == self.connections.len);
            // This should be relatively rare as we check if there is a
            // connection available before starting the accept operation.
            // However this can occur if the connection was used to connect
            // to a replica in the meantime.
            log.info("closing accepted socket as no Connection struct was available", .{});
            self.accept_in_progress = true;
            self.io.close(*MessageBus, self, on_close, &self.accept_completion, fd);
        }
    }

    fn on_close(self: *MessageBus, completion: *IO.Completion, result: IO.CloseError!void) void {
        self.accept_in_progress = false;
        result catch |err| log.err("error closing accepted socket: {}", .{err});
    }

    /// Teardown, using blocking syscalls to close all sockets
    /// Calling IO.run() after this function is illegal.
    /// TODO: should we free memory here or just let the OS clean up?
    pub fn deinit(self: *MessageBus) void {
        os.close(self.server_fd);
        for (self.connections) |connection| {
            if (connection.fd != -1) os.close(connection.fd);
        }
    }

    /// Increment the reference count of the message and return the same pointer passed.
    pub fn ref(self: *MessageBus, message: *Message) *Message {
        message.references += 1;
        return message;
    }

    /// Decrement the reference count of the message, possibly freeing it.
    pub fn unref(self: *MessageBus, message: *Message) void {
        message.references -= 1;
        if (message.references == 0) {
            log.debug("message_bus: freeing {}", .{message.header});
            self.allocator.free(message.buffer);
            self.allocator.destroy(message);
            self.allocated -= 1;
        }
    }

    pub fn send_header_to_replica(self: *MessageBus, replica: u16, header: Header) void {
        assert(header.size == @sizeOf(Header));

        // TODO Pre-allocate messages at startup.
        var message = self.create_message(@sizeOf(Header)) catch unreachable;
        message.header.* = header;

        const body = message.buffer[@sizeOf(Header)..message.header.size];
        // The order matters here because checksum depends on checksum_body:
        message.header.set_checksum_body(body);
        message.header.set_checksum();

        assert(message.references == 0);
        self.send_message_to_replica(replica, message);
    }

    pub fn send_message_to_replica(self: *MessageBus, replica: u16, message: *Message) void {
        // TODO Pre-allocate envelopes at startup.
        const envelope = self.allocator.create(Envelope) catch unreachable;
        envelope.* = .{ .message = self.ref(message) };

        // Messages sent by the server to itself are delivered directly in flush()
        if (replica == self.server.replica) {
            self.server_send_queue.push(envelope);
        } else if (self.replicas[replica]) |connection| {
            connection.send_message(envelope);
        }
    }

    pub fn send_header_to_client(self: *MessageBus, client_id: u128, header: Header) void {
        assert(header.size == @sizeOf(Header));

        // TODO Pre-allocate messages at startup.
        var message = self.create_message(@sizeOf(Header)) catch unreachable;
        message.header.* = header;

        const body = message.buffer[@sizeOf(Header)..message.header.size];
        // The order matters here because checksum depends on checksum_data:
        message.header.set_checksum_data(body);
        message.header.set_checksum();

        assert(message.references == 0);
        self.send_message_to_client(client_id, message);
    }

    /// Try to send the message to the client with the given id.
    /// If the client is not currently connected, the message is silently dropped.
    pub fn send_message_to_client(self: *MessageBus, client_id: u128, message: *Message) void {
        for (self.connections) |*connection| {
            switch (connection.peer) {
                .client => |id| if (id == client_id) {
                    // TODO Pre-allocate envelopes at startup.
                    const envelope = self.allocator.create(Envelope) catch unreachable;
                    envelope.* = .{ .message = self.ref(message) };
                    connection.send_message(envelope);
                    return;
                },
                else => {},
            }
        }
    }

    pub fn flush(self: *MessageBus) void {
        // Deliver messages the server replica has sent to itself.
        // Iterate on a copy to avoid a potential infinite loop.
        var copy = self.server_send_queue;
        self.server_send_queue = .{};
        while (copy.pop()) |envelope| {
            self.server.on_message(envelope.message);
            self.unref(envelope.message);
            self.allocator.destroy(envelope);
        }
    }

    pub fn create_message(self: *MessageBus, size: u32) !*Message {
        assert(size >= @sizeOf(Header));

        var buffer = try self.allocator.allocAdvanced(u8, vr.sector_size, size, .exact);
        errdefer self.allocator.free(buffer);
        mem.set(u8, buffer, 0);

        var message = try self.allocator.create(Message);
        errdefer self.allocator.destroy(message);

        self.allocated += 1;

        message.* = .{
            .header = mem.bytesAsValue(Header, buffer[0..@sizeOf(Header)]),
            .buffer = buffer,
        };

        return message;
    }
};

/// Connection used to recive data from clients and replicas as well as
/// send data to clients. This connection is not re-created if it drops,
/// The other replica or client is responsible for re-connecting.
const Connection = struct {
    message_bus: *MessageBus,

    /// The peer is determined by inspecting the first message header
    /// received.
    peer: union(enum) {
        /// No peer is currently connected.
        none: void,
        /// A connection has been established but the first header has not yet been recieved.
        unknown: void,
        /// The peer is a client with the given id.
        client: u128,
        /// The peer is a replica with the given id.
        replica: u16,
        /// The connection is being terminated but cleanup has not yet finished.
        shutting_down: void,
    } = .none,
    /// This is guaranteed to be valid only while peer is unknown, client, or replica.
    /// It will be reset to -1 during the shutdown process and is always -1 if the
    /// connection is unused (i.e. peer == .none). We use -1 instead of undefined here
    /// for safety to ensure an error if the invalid value is ever used, instead of
    /// potentially preforming an action on an active fd.
    fd: os.socket_t = -1,

    /// This completion is used for all recv operations.
    /// It is also used for the initial connect when establishing a replica connection.
    recv_completion: IO.Completion = undefined,
    /// True exactly when the recv_completion has been submitted to the IO abstraction
    /// but the callback has not yet been run.
    recv_in_progress: bool = false,
    /// Number of bytes of the current header/message that have already been recieved.
    recv_progress: usize = 0,
    incoming_header: Header = undefined,
    incoming_message: *Message = undefined,

    /// This completion is used for all send operations.
    send_completion: IO.Completion = undefined,
    /// True exactly when the send_completion has been submitted to the IO abstraction
    /// but the callback has not yet been run.
    send_in_progress: bool = false,
    /// Number of bytes of the current message that have already been sent.
    send_progress: usize = 0,
    /// The queue of messages to send to the client.
    /// Empty unless peer == .client
    send_queue: FIFO(Envelope) = .{},

    /// Attempt to connect to a replica. Failure is silent and returns the
    /// connection to an unused state.
    pub fn connect_to_replica(self: *Connection, replica: u16) void {
        assert(self.peer == .none);
        assert(self.fd == -1);

        const bus = self.message_bus;
        const server_addr = bus.configuration[bus.server.replica];
        self.fd = os.socket(server_addr.any.family, os.SOCK_STREAM | os.SOCK_CLOEXEC, 0) catch return;

        self.peer = .{ .replica = replica };
        bus.replicas[replica] = self;
        bus.replicas_connected += 1;

        self.recv_in_progress = true;
        bus.io.connect(
            *Connection,
            self,
            on_connect,
            // We need to use the recv_completion here
            &self.recv_completion,
            self.fd,
            // TODO: is this actually safe to pass by value?
            // Should IO just take an std.net.Address or a pointer?
            bus.configuration[replica].any,
            bus.configuration[replica].getOsSockLen(),
        );
    }

    fn on_connect(self: *Connection, completion: *IO.Completion, result: IO.ConnectError!void) void {
        self.recv_in_progress = false;
        result catch |err| {
            log.err("error connecting to {}: {}", .{ self.peer, err });
            self.shutdown();
            return;
        };
        log.debug("connected to {}", .{self.peer});
        self.recv_header();
    }

    /// Given a newly accepted fd, start receiving messages on it.
    /// Callbacks will be continuously re-registered until shutdown() is
    /// called and the connection is terminated.
    pub fn on_accept(self: *Connection, fd: os.socket_t) void {
        assert(self.peer == .none);
        assert(self.fd == -1);
        self.fd = fd;
        self.peer = .unknown;
        self.message_bus.connections_in_use += 1;
        self.recv_header();
    }

    /// Add a message to the connection's send queue, starting a send operation
    /// if the queue was previously empty.
    pub fn send_message(self: *Connection, envelope: *Envelope) void {
        assert(self.peer == .client or self.peer == .replica);
        const queue_was_empty = self.send_queue.out == null;
        self.send_queue.push(envelope);
        // If the queue was not empty, the message will be sent after the
        // messages currently being sent.
        if (queue_was_empty) self.send();
    }

    /// Clean up an active connection and reset it to its initial, unusued, state.
    /// This reset does not happen instantly as currently in progress operations
    /// must first be stopped.
    pub fn shutdown(self: *Connection) void {
        assert(self.peer != .shutting_down and self.peer != .none);
        assert(self.fd != -1);
        // The shutdown syscall will cause currently in progress send/recv
        // operations to be terminated with an error while keeping the fd open.
        const rc = os.linux.shutdown(self.fd, os.SHUT_RDWR);
        switch (os.errno(rc)) {
            0 => {},
            os.EBADF => unreachable,
            os.EINVAL => unreachable,
            os.ENOTCONN => @panic("TODO: is this possible?"),
            os.ENOTSOCK => unreachable,
            else => |err| os.unexpectedErrno(err) catch {},
        }
        if (self.peer == .replica) {
            self.message_bus.replicas[self.peer.replica] = null;
            self.message_bus.replicas_connected -= 1;
        }
        self.peer = .shutting_down;
        self.maybe_close();
    }

    fn recv_header(self: *Connection) void {
        self.recv(on_recv_header, mem.asBytes(&self.incoming_header)[self.recv_progress..]);
    }

    fn recv_body(self: *Connection) void {
        self.recv(
            on_recv_body,
            self.incoming_message.buffer[self.recv_progress..][0..self.incoming_header.size],
        );
    }

    fn recv(
        self: *Connection,
        comptime callback: fn (*Connection, *IO.Completion, IO.RecvError!usize) void,
        buffer: []u8,
    ) void {
        if (self.peer == .shutting_down) return;
        assert(self.fd != -1);
        assert(!self.recv_in_progress);
        self.recv_in_progress = true;
        self.message_bus.io.recv(
            *Connection,
            self,
            on_recv_body,
            &self.recv_completion,
            self.fd,
            self.incoming_message.buffer[self.recv_progress..][0..self.incoming_header.size],
            os.MSG_NOSIGNAL,
        );
    }

    fn on_recv_header(self: *Connection, completion: *IO.Completion, result: IO.RecvError!usize) void {
        self.recv_in_progress = false;
        if (self.peer == .shutting_down) {
            self.maybe_close();
            return;
        }
        self.recv_progress += result catch |err| {
            // TODO: maybe don't need to close on *every* error
            log.err("error receiving body from {}: {}", .{ self.peer, err });
            self.shutdown();
            return;
        };

        if (self.recv_progress < @sizeOf(Header)) {
            // The header has not yet been fully received.
            self.recv_header();
            return;
        }
        assert(self.recv_progress == @sizeOf(Header));

        if (!self.incoming_header.valid_checksum()) {
            log.err("invalid checksum on header received from {}", .{self.peer});
            self.shutdown();
            return;
        }

        assert(self.peer != .none);
        if (self.peer == .unknown) {
            if (self.incoming_header.command == .request) {
                self.peer = .{ .client = self.incoming_header.client };
            } else {
                self.peer = .{ .replica = self.incoming_header.replica };
                self.message_bus.replicas[self.peer.replica] = self;
                self.message_bus.replicas_connected += 1;
            }
        }

        self.incoming_message = self.message_bus.create_message(self.incoming_header.size) catch unreachable;
        self.incoming_message.header.* = self.incoming_header;
        self.recv_body();
    }

    fn on_recv_body(self: *Connection, completion: *IO.Completion, result: IO.RecvError!usize) void {
        self.recv_in_progress = false;
        if (self.peer == .shutting_down) {
            self.maybe_close();
            return;
        }
        self.recv_progress += result catch |err| {
            // TODO: maybe don't need to close on *every* error
            log.err("error receiving body from {}: {}", .{ self.peer, err });
            self.shutdown();
            return;
        };

        if (self.recv_progress < self.incoming_header.size) {
            // The body has not yet been fully received.
            self.recv_body();
            return;
        }
        assert(self.recv_progress == self.incoming_header.size);

        const body = self.incoming_message.buffer[@sizeOf(Header)..self.incoming_header.size];
        if (self.incoming_header.valid_checksum_data(body)) {
            self.message_bus.server.on_message(self.incoming_message);
        } else {
            log.err("invalid checksum on body received from {}", .{self.peer});
            self.shutdown();
        }

        self.message_bus.unref(self.incoming_message);

        // Reset state and try to receive the next message.
        self.incoming_header = undefined;
        self.incoming_message = undefined;
        self.recv_progress = 0;
        self.recv_header();
    }

    fn send(self: *Connection) void {
        if (self.peer == .shutting_down) return;
        const envelope = self.send_queue.out orelse return;
        assert(self.fd != -1);
        assert(!self.send_in_progress);
        self.send_in_progress = true;
        self.message_bus.io.send(
            *Connection,
            self,
            on_send,
            &self.send_completion,
            self.fd,
            envelope.message.buffer[self.send_progress..],
            os.MSG_NOSIGNAL,
        );
    }

    fn on_send(self: *Connection, completion: *IO.Completion, result: IO.SendError!usize) void {
        self.send_in_progress = false;
        if (self.peer == .shutting_down) {
            self.maybe_close();
            return;
        }
        self.send_progress += result catch |err| {
            // TODO: maybe don't need to close on *every* error
            log.err("error sending message to replica at {}: {}", .{ self.peer, err });
            self.shutdown();
            return;
        };
        assert(self.send_progress <= self.send_queue.out.?.message.buffer.len);
        // If the message has been fully sent, move on to the next one.
        if (self.send_progress == self.send_queue.out.?.message.buffer.len) {
            self.send_progress = 0;
            const envelope = self.send_queue.pop().?;
            self.message_bus.unref(envelope.message);
            self.message_bus.allocator.destroy(envelope);
        }
        self.send();
    }

    fn maybe_close(self: *Connection) void {
        assert(self.peer == .shutting_down);
        // If a recv or send operation is currently submitted to the kernel,
        // submiting a close would cause a race. Therefore we must wait  for
        // any currently submitted operation to complete.
        if (self.recv_in_progress or self.send_in_progress) return;
        self.send_in_progress = true;
        self.recv_in_progress = true;
        assert(self.fd != -1);
        defer self.fd = -1;
        // It's OK to use the send completion here as we know that no send
        // operation is currently in progress.
        self.message_bus.io.close(*Connection, self, on_close, &self.send_completion, self.fd);
    }

    fn on_close(self: *Connection, completion: *IO.Completion, result: IO.CloseError!void) void {
        assert(self.peer == .shutting_down);

        // Reset the connection to its initial state.
        defer {
            while (self.send_queue.pop()) |envelope| {
                self.message_bus.unref(envelope.message);
                self.message_bus.allocator.destroy(envelope);
            }
            self.* = .{ .message_bus = self.message_bus };
            self.message_bus.connections_in_use -= 1;
        }

        result catch |err| {
            log.err("error closing connection to {}: {}", .{ self.peer, err });
            return;
        };
        log.debug("closed connection to {}", .{self.peer});
    }
};

test "" {
    std.testing.refAllDecls(@This());
    std.testing.refAllDecls(MessageBus);
    std.testing.refAllDecls(Connection);
}
