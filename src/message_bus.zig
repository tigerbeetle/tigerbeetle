const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

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
    /// The slot for the server's replica id is undefined
    replicas: []ReplicaConnection,
    connections: []Connection,

    /// Initialize the MessageBus for the given server replica and configuration
    pub fn init(
        self: *MessageBus,
        allocator: *mem.Allocator,
        io: *IO,
        server: *Replica,
        server_id: u16,
        configuration: []std.net.Address,
    ) !void {
        const replicas = try allocator.alloc(ReplicaConnection, configuration.len);
        errdefer allocator.free(replicas);

        for (configuration) |address, i| {
            if (i == server_id) continue;
            replicas[i] = .{ .connection = .{
                .message_bus = self,
                .address = address,
            } };
        }

        const connections = try allocator.alloc(Connection, num_connections);
        errdefer allocator.free(connections);
        mem.set(Connection, connections, .{ .message_bus = self });

        self.* = .{
            .allocator = allocator,
            .io = io,
            .server = server,
            .server_fd = try init_tcp(configuration[server_id]),
            .replicas = replicas,
            .connections = connections,
        };
    }

    fn init_tcp(address: std.net.Address) !os.socket_t {
        const fd = try os.socket(address.any.family, os.SOCK_STREAM | os.SOCK_CLOEXEC, 0);
        errdefer os.close(fd);
        // TODO: configure RCVBUF, SNDBUF, KEEPALIVE, TIMEOUT, NODELAY
        try os.setsockopt(server, os.SOL_SOCKET, os.SO_REUSEADDR, &mem.toBytes(@as(c_int, 1)));
        // TODO: port hopping
        try os.bind(server, &address.any, address.getOsSockLen());
        try os.listen(server, tcp_backlog);
    }

    /// Teardown, using blocking syscalls to close all sockets
    pub fn deinit(self: *MessageBus) void {
        for (self.replicas) |connection| {
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
        envelope.* = .{
            .address = self.configuration[replica],
            .message = message,
        };
        self.replicas[replica].send_queue.push(envelope);
    }

    pub fn send_header_to_client(self: *MessageBus, client_id: u128, header: Header) void {
        assert(header.size == @sizeOf(Header));

        // TODO Pre-allocate messages at startup.
        var message = self.create_message(@sizeOf(Header)) catch unreachable;
        message.header.* = header;

        const data = message.buffer[@sizeOf(Header)..message.header.size];
        // The order matters here because checksum depends on checksum_data:
        message.header.set_checksum_data(data);
        message.header.set_checksum();

        assert(message.references == 0);
        self.send_message_to_client(replica, message);
    }

    pub fn send_message_to_client(self: *MessageBus, client_id: u128, message: *Message) void {
        message.references += 1;

        // TODO Pre-allocate envelopes at startup.
        const envelope = self.allocator.create(Envelope) catch unreachable;
        envelope.* = .{
            .address = self.configuration[replica],
            .message = message,
        };
        // TODO
    }

    // TODO: get rid of this and just send right away?
    pub fn send_queued_messages(self: *MessageBus) void {
        for (self.replicas) |connection| {
            if (connection.fd == -1) {
                connection.connect();
            } else {
                connection.flush_queue();
            }
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
        send_queue: FIFO(Envelope) = .{},
        /// Number of bytes of the current message that have already been sent.
        bytes_sent: usize = 0,

        fn connect(self: *ReplicaConnection) void {
            assert(self.fd == -1);
            self.fd = try os.socket(self.address.any.family, os.SOCK_STREAM | os.SOCK_CLOEXEC, 0);
            self.message_bus.io.connect(
                *ReplicaConnection,
                self,
                complete_connect,
                &self.completion,
                self.fd,
                &self.address.any,
                self.address.getOsSockLen(),
            );
        }

        fn complete_connect(self: *ReplicaConnection, completion: *IO.Completion, result: ConnectError!void) void {
            result catch |err| {
                // TODO: maybe don't need to close on *every* error
                log.err("error connecting to {}: {}", .{ self.address, err });
                self.close();
                return;
            };
            log.debug("connected to {}", .{self.address});
        }

        fn close(self: *ReplicaConnection) void {
            self.message_bus.io.close(
                *ReplicaConnection,
                self,
                complete_close,
                &self.completion,
                self.fd,
            );
        }

        fn complete_close(self: *ReplicaConnection, completion: *IO.Completion, result: ConnectError!void) void {
            result catch |err| {
                log.err("error closing connection to replica at {}: {}", .{ self.address, err });
                return;
            };
            log.debug("closed connection to replica at {}", .{self.address});
        }

        fn flush_queue(self: *ReplicaConnection) void {
            const envelope = self.send_queue.out orelse return;
            self.message_bus.io.send(
                *ReplicaConnection,
                self,
                complete_send,
                &self.completion,
                self.fd,
                envelope.message.buffer[self.bytes_sent..],
                os.MSG_NOSIGNAL,
            );
        }

        fn complete_send(self: *ReplicaConnection, completion: *IO.Completion, result: SendError!usize) void {
            self.bytes_sent += result catch |err| {
                // TODO: maybe don't need to close on *every* error
                log.err("error sending message to replica at {}: {}", .{ self.address, err });
                self.close();
                return;
            };
            assert(self.bytes_sent <= self.send_queue.out.?.message.buffer.len);
            // If the message has been fully sent, move on to the next one.
            if (self.bytes_sent == self.send_queue.out.?.message.buffer.len) {
                self.bytes_sent = 0;
                const envelope = self.send_queue.pop().?;
                envelope.message.references -= 1;
                self.message_bus.gc(envelope.message);
                self.message_bus.allocator.destroy(envelope);
            }
            self.flush_queue();
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
        bytes_received: usize = 0,
        incoming_header: Header = undefined,
        incoming_message: *Message = undefined,

        /// This completion is used for all send operations.
        send_completion: IO.Completion = undefined,
        /// The queue of messages to send to the client.
        /// Empty unless peer == .client
        send_queue: FIFO(Envelope) = .{},
        bytes_sent: usize = 0,

        /// Given a newly accepted fd, start receiving messages on it.
        /// Callbacks will be continously re-registered until close() is
        /// called and the connection is terminated.
        fn receive_messages(self: *Connection, fd: os.socket_t) void {
            assert(self.fd == -1);
            assert(self.peer == .none);
            self.fd = fd;
            self.peer = unknown;
            self.recv_header();
        }

        fn recv_header(self: *Connection) void {
            // The fd may have been closed due to an error while sending.
            if (self.fd == -1) return;
            self.message_bus.io.recv(
                *ReplicaConnection,
                self,
                complete_recv_header,
                &self.completion,
                mem.asBytes(&self.incoming_header)[self.bytes_received..],
                os.MSG_NOSIGNAL,
            );
        }

        fn complete_recv_header(self: *Connection, completion: *IO.Completion, result: RecvError!usize) void {
            self.bytes_received += result catch |err| {
                // TODO: maybe don't need to close on *every* error
                log.err("error receiving data from {}: {}", .{ self.peer, err });
                self.close();
                return;
            };

            if (self.bytes_received < @sizeOf(Header)) {
                // The header has not yet been fully received.
                self.recv_header();
                return;
            }
            assert(self.bytes_received == @sizeOf(Header));

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
                *ReplicaConnection,
                self,
                complete_recv_body,
                &self.completion,
                self.incoming_message.buffer[self.bytes_received..],
                os.MSG_NOSIGNAL,
            );
        }

        fn complete_recv_body(self: *Connection, completion: *IO.Completion, result: RecvError!usize) void {
            self.bytes_received += result catch |err| {
                // TODO: maybe don't need to close on *every* error
                log.err("error receiving data from: {}", .{ peer, err });
                self.close();
                return;
            };

            if (self.bytes_received < self.incoming_message.buffer.len) {
                // The body has not yet been fully received.
                self.recv_body();
                return;
            }
            assert(self.bytes_received == self.incoming_message.buffer.len);

            const data = self.incoming_message.buffer[@sizeOf(Header)..self.incoming_header.size];
            if (self.incoming_header.valid_checksum_data(data)) {
                self.message_bus.server.on_message(self.incoming_message);
            } else {
                log.err("invalid checksum on data received from {}", .{self.peer});
                self.close();
            }

            self.incoming_message.references -= 1;
            self.message_bus.gc(self.incoming_message);

            // Reset state and try to receive the next message.
            self.incoming_header = undefined;
            self.incoming_message = undefined;
            self.bytes_received = 0;
            self.recv_header();
        }

        fn close(self: *Connection) void {
            // If an error occurs in both sending and receving at roughly,
            // the same time, this function might be called twice.
            if (self.fd == -1) return;
            self.message_bus.io.close(
                *Connection,
                self,
                complete_close,
                &self.completion,
                self.fd,
            );
        }

        fn complete_close(self: *Connection, completion: *IO.Completion, result: CloseError!void) void {
            result catch |err| {
                log.err("error closing connection to {}: {}", .{ self.peer, err });
                return;
            };
            log.debug("closed connection to {}", .{self.peer});
            self.* = .{ .message_bus = self.message_bus };
        }
    };
};
