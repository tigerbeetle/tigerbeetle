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
    references: usize = 0,
    next: ?*Message = null,
};

pub const MessageBus = struct {
    const Envelope = struct {
        message: *Message,
        next: ?*Envelope = null,
    };

    allocator: *mem.Allocator,
    allocated: usize = 0,
    io: *IO,

    /// The replica which is running the server
    server: *Replica,
    server_fd: os.socket_t,
    server_completion: IO.Completion = undefined,

    /// Send only connections to fellow replicas, automatically reconnected on error.
    /// The slot for the server's replica id is undefined except for its send_queue.
    replicas: []ReplicaConnection,
    /// Receive only connections to fellow replicas and send/receive
    /// connections to clients.
    connections: []Connection,
    /// Number of connections in the connection array currently in use
    connections_used: usize = 0,
    /// Number of currently active replica connections in the connections slice.
    replica_connections: usize = 0,

    /// Initialize the MessageBus for the given server replica and configuration.
    pub fn init(
        self: *MessageBus,
        allocator: *mem.Allocator,
        io: *IO,
        server: *Replica,
        server_index: u16,
        configuration: []std.net.Address,
    ) !void {
        const replicas = try allocator.alloc(ReplicaConnection, configuration.len);
        errdefer allocator.free(replicas);

        for (configuration) |address, i| {
            if (i == server_index) {
                replicas[i].send_queue = .{};
            } else {
                replicas[i] = .{
                    .message_bus = self,
                    .address = address,
                };
                replicas[i].connect();
            }
        }

        // There must be enough connections for all replicas and at least one client.
        assert(num_connections > configuration.len);
        const connections = try allocator.alloc(Connection, num_connections);
        errdefer allocator.free(connections);
        mem.set(Connection, connections, .{ .message_bus = self });

        self.* = .{
            .allocator = allocator,
            .io = io,
            .server = server,
            .server_fd = try init_tcp(configuration[server_index]),
            .replicas = replicas,
            .connections = connections,
        };

        self.maybe_accept();
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

    /// Must be called whenever a Connection in the connections slice is closed.
    fn maybe_accept(self: *MessageBus) void {
        if (self.connections_used == self.connections.len) {
            // If there are no unused connections and all replicas are
            // connected, do nothing.
            if (self.replica_connections == self.replicas.len - 1) return;
            assert(self.replica_connections < self.replicas.len - 1);
            // There are no unused connections but not all replicas are connected.
            // Disconnect a client to make sure there is space for a replica to connect.
            log.info("all connections in use but not all replicas connected, " ++
                "attempting to disconnect a client", .{});
            for (self.connections) |*connection| {
                assert(connection.peer != .none);
                if (connection.peer == .client) {
                    connection.close();
                    return;
                }
            }
            // This code should never be reached in normal circumstances.
            // In the edge case that all client connections are currently
            // waiting on the first message header to determine whether the
            // peer is a client or a replica, disconnect one of these
            // waiting connections to ensure we make progress.
            log.warn("failed to disconnect a client as all peers were replicas or unknown, " ++
                "disconnecting an unknown peer.", .{});
            for (self.connections) |*connection| {
                assert(connection.peer != .none);
                if (connection.peer == .unknown) {
                    connection.close();
                    return;
                }
            }
            // If this is reached, connections.len is too low for the
            // current number of replicas or there is some other bug in
            // the system.
            unreachable;
        }
        assert(self.connections_used < self.connections.len);
        self.io.accept(
            *MessageBus,
            self,
            on_accept,
            &self.server_completion,
            self.server_fd,
            os.SOCK_CLOEXEC,
        );
    }

    fn on_accept(self: *MessageBus, completion: *IO.Completion, result: IO.AcceptError!os.socket_t) void {
        defer self.maybe_accept();
        const fd = result catch |err| {
            // TODO: some errors should probably be fatal
            log.err("accept failed: {}", .{err});
            return;
        };
        assert(self.connections_used < self.connections.len);
        // Find an unused Connection to receive/send from the new peer.
        for (self.connections) |*connection| {
            if (connection.peer == .none) {
                connection.on_accept(fd);
                break;
            }
        } else unreachable;
        self.connections_used += 1;
    }

    /// Teardown, using blocking syscalls to close all sockets
    /// TODO: should we free memory here or just let the OS clean up?
    pub fn deinit(self: *MessageBus) void {
        os.close(self.server_fd);
        for (self.replicas) |connection| {
            if (connection.fd != -1) os.close(connection.fd);
        }
        for (self.connections) |connection| {
            if (connection.fd != -1) os.close(connection.fd);
        }
    }

    /// TODO Detect if gc() called multiple times for message.references == 0.
    pub fn gc(self: *MessageBus, message: *Message) void {
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
        message.references += 1;

        // TODO Pre-allocate envelopes at startup.
        const envelope = self.allocator.create(Envelope) catch unreachable;
        envelope.* = .{ .message = message };

        // Messages sent by the server to itself are delivered directly in flush()
        if (replica == self.server.replica) {
            self.replicas[replica].send_queue.push(envelope);
        } else {
            self.replicas[replica].send_message(envelope);
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
                    message.references += 1;
                    // TODO Pre-allocate envelopes at startup.
                    const envelope = self.allocator.create(Envelope) catch unreachable;
                    envelope.* = .{ .message = message };
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
        var copy = self.replicas[self.server.replica].send_queue;
        self.replicas[self.server.replica].send_queue = .{};
        while (copy.pop()) |envelope| {
            self.server.on_message(envelope.message);
            envelope.message.references -= 1;
            self.gc(envelope.message);
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
            .references = 0,
        };

        return message;
    }

    /// Connection used to send data to another replica
    /// TODO: This connection will be automatically re-created if it drops,
    /// using exponential backoff and full jitter.
    const ReplicaConnection = struct {
        message_bus: *MessageBus,
        address: std.net.Address,
        /// A value of -1 indicates that the connection is not currently active
        fd: os.socket_t = -1,
        completion: IO.Completion = undefined,
        /// Number of bytes of the current message that have already been sent.
        send_progress: usize = 0,
        send_queue: FIFO(Envelope) = .{},

        fn send_message(self: *ReplicaConnection, envelope: *Envelope) void {
            const queue_was_empty = self.send_queue.out == null;
            self.send_queue.push(envelope);
            // If the queue was not empty, the message will be sent after the
            // messages currently being sent.
            if (queue_was_empty) self.send();
        }

        fn connect(self: *ReplicaConnection) void {
            assert(self.fd == -1);
            self.fd = try os.socket(self.address.any.family, os.SOCK_STREAM | os.SOCK_CLOEXEC, 0);
            self.message_bus.io.connect(
                *ReplicaConnection,
                self,
                on_connect,
                &self.completion,
                self.fd,
                // TODO: is this actually safe to pass by value? Should IO just take an std.net.Address?
                self.address.any,
                self.address.getOsSockLen(),
            );
        }

        fn on_connect(self: *ReplicaConnection, completion: *IO.Completion, result: IO.ConnectError!void) void {
            result catch |err| {
                // TODO: maybe don't need to close on *every* error
                log.err("error connecting to {}: {}", .{ self.address, err });
                self.close();
                return;
            };
            log.debug("connected to {}", .{self.address});
            self.send();
        }

        fn close(self: *ReplicaConnection) void {
            // Reset send_progress to 0 so that we resend the full message on reconnect.
            self.send_progress = 0;
            self.message_bus.io.close(
                *ReplicaConnection,
                self,
                on_close,
                &self.completion,
                self.fd,
            );
        }

        fn on_close(self: *ReplicaConnection, completion: *IO.Completion, result: IO.CloseError!void) void {
            result catch |err| {
                log.err("error closing connection to replica at {}: {}", .{ self.address, err });
                return;
            };
            log.debug("closed connection to replica at {}", .{self.address});
            // TODO: add a delay before reconnecting based on exponential backoff/full jitter
            self.connect();
        }

        fn send(self: *ReplicaConnection) void {
            // If currently disconnected, do nothing.
            // This function will be called again on reconnect.
            if (self.fd == -1) return;
            const envelope = self.send_queue.out orelse return;
            self.message_bus.io.send(
                *ReplicaConnection,
                self,
                on_send,
                &self.completion,
                self.fd,
                envelope.message.buffer[self.send_progress..],
                os.MSG_NOSIGNAL,
            );
        }

        fn on_send(self: *ReplicaConnection, completion: *IO.Completion, result: IO.SendError!usize) void {
            self.send_progress += result catch |err| {
                // TODO: maybe don't need to close on *every* error
                log.err("error sending message to replica at {}: {}", .{ self.address, err });
                self.close();
                return;
            };
            assert(self.send_progress <= self.send_queue.out.?.message.buffer.len);
            // If the message has been fully sent, move on to the next one.
            if (self.send_progress == self.send_queue.out.?.message.buffer.len) {
                self.send_progress = 0;
                const envelope = self.send_queue.pop().?;
                envelope.message.references -= 1;
                self.message_bus.gc(envelope.message);
                self.message_bus.allocator.destroy(envelope);
            }
            self.send();
        }
    };

    /// Connection used to recive data from clients and replicas as well as
    /// send data to clients. This connection is not re-created if it drops,
    /// The other replica or client is responsible for re-connecting.
    const Connection = struct {
        message_bus: *MessageBus,
        /// A value of -1 indicates that the connection is currently being
        /// closed or is inactive.
        fd: os.socket_t = -1,
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
        } = .none,

        /// This completion is used for all recv operations.
        recv_completion: IO.Completion = undefined,
        /// Number of bytes of the current header/message that have already been recieved.
        recv_progress: usize = 0,
        incoming_header: Header = undefined,
        incoming_message: *Message = undefined,

        /// This completion is used for all send operations.
        send_completion: IO.Completion = undefined,
        /// Number of bytes of the current message that have already been sent.
        send_progress: usize = 0,
        /// The queue of messages to send to the client.
        /// Empty unless peer == .client
        send_queue: FIFO(Envelope) = .{},

        /// TODO: this can be gotten rid of once tick() is implemented and
        /// maybe_accept() can use blocking syscalls to disconnect clients
        close_completion: IO.Completion = undefined,

        /// Given a newly accepted fd, start receiving messages on it.
        /// Callbacks will be continously re-registered until close() is
        /// called and the connection is terminated.
        fn on_accept(self: *Connection, fd: os.socket_t) void {
            assert(self.fd == -1);
            assert(self.peer == .none);
            self.fd = fd;
            self.peer = .unknown;
            self.recv_header();
        }

        fn recv_header(self: *Connection) void {
            // The fd may have been closed due to an error while sending.
            if (self.fd == -1) return;
            self.message_bus.io.recv(
                *Connection,
                self,
                on_recv_header,
                &self.recv_completion,
                self.fd,
                mem.asBytes(&self.incoming_header)[self.recv_progress..],
                os.MSG_NOSIGNAL,
            );
        }

        fn on_recv_header(self: *Connection, completion: *IO.Completion, result: IO.RecvError!usize) void {
            self.recv_progress += result catch |err| {
                // TODO: maybe don't need to close on *every* error
                log.err("error receiving body from {}: {}", .{ self.peer, err });
                self.close();
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
                self.close();
                return;
            }

            assert(self.peer != .none);
            if (self.peer == .unknown) {
                if (self.incoming_header.command == .request) {
                    self.peer = .{ .client = self.incoming_header.client };
                } else {
                    self.message_bus.replica_connections += 1;
                    self.peer = .{ .replica = self.incoming_header.replica };
                }
            }

            self.incoming_message = self.message_bus.create_message(self.incoming_header.size) catch unreachable;
            self.incoming_message.references += 1;
            self.incoming_message.header.* = self.incoming_header;
            self.recv_body();
        }

        fn recv_body(self: *Connection) void {
            // The fd may have been closed due to an error while sending.
            if (self.fd == -1) return;
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

        fn on_recv_body(self: *Connection, completion: *IO.Completion, result: IO.RecvError!usize) void {
            self.recv_progress += result catch |err| {
                // TODO: maybe don't need to close on *every* error
                log.err("error receiving body from {}: {}", .{ self.peer, err });
                self.close();
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
                self.close();
            }

            self.incoming_message.references -= 1;
            self.message_bus.gc(self.incoming_message);

            // Reset state and try to receive the next message.
            self.incoming_header = undefined;
            self.incoming_message = undefined;
            self.recv_progress = 0;
            self.recv_header();
        }

        fn send_message(self: *Connection, envelope: *Envelope) void {
            assert(self.peer == .client);
            const queue_was_empty = self.send_queue.out == null;
            self.send_queue.push(envelope);
            // If the queue was not empty, the message will be sent after the
            // messages currently being sent.
            if (queue_was_empty) self.send();
        }

        fn send(self: *Connection) void {
            // If currently disconnected, do nothing.
            // This function will be called again on reconnect.
            if (self.fd == -1) return;
            const envelope = self.send_queue.out orelse return;
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
            self.send_progress += result catch |err| {
                // TODO: maybe don't need to close on *every* error
                log.err("error sending message to replica at {}: {}", .{ self.peer, err });
                self.close();
                return;
            };
            assert(self.send_progress <= self.send_queue.out.?.message.buffer.len);
            // If the message has been fully sent, move on to the next one.
            if (self.send_progress == self.send_queue.out.?.message.buffer.len) {
                self.send_progress = 0;
                const envelope = self.send_queue.pop().?;
                envelope.message.references -= 1;
                self.message_bus.gc(envelope.message);
                self.message_bus.allocator.destroy(envelope);
            }
            self.send();
        }

        fn close(self: *Connection) void {
            // If an error occurs in both sending and receving at roughly,
            // the same time, this function might be called twice.
            if (self.fd == -1) return;
            defer self.fd = -1;
            self.message_bus.io.close(
                *Connection,
                self,
                on_close,
                &self.close_completion,
                self.fd,
            );
        }

        fn on_close(self: *Connection, completion: *IO.Completion, result: IO.CloseError!void) void {
            defer {
                if (self.peer == .replica) self.message_bus.replica_connections -= 1;
                self.message_bus.connections_used -= 1;
                while (self.send_queue.pop()) |envelope| {
                    envelope.message.references -= 1;
                    self.message_bus.gc(envelope.message);
                    self.message_bus.allocator.destroy(envelope);
                }
                self.* = .{ .message_bus = self.message_bus };
                self.message_bus.maybe_accept();
            }
            result catch |err| {
                log.err("error closing connection to {}: {}", .{ self.peer, err });
                return;
            };
            log.debug("closed connection to {}", .{self.peer});
        }
    };
};
