const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const constants = @import("constants.zig");
const log = std.log.scoped(.message_bus);

const vsr = @import("vsr.zig");

const stdx = @import("stdx");
const maybe = stdx.maybe;
const RingBufferType = stdx.RingBufferType;
const IO = @import("io.zig").IO;
const MessagePool = @import("message_pool.zig").MessagePool;
const Message = MessagePool.Message;
const MessageBuffer = @import("./message_buffer.zig").MessageBuffer;
const QueueType = @import("./queue.zig").QueueType;

pub const MessageBusReplica = MessageBusType(.replica);
pub const MessageBusClient = MessageBusType(.client);

fn MessageBusType(comptime process_type: vsr.ProcessType) type {
    const ProcessID = union(vsr.ProcessType) {
        replica: u8,
        client: u128,
    };

    return struct {
        const MessageBus = @This();

        pool: *MessagePool,
        io: *IO,

        configuration: []const std.net.Address,

        process: switch (process_type) {
            .replica => struct {
                replica: u8,
                /// The file descriptor for the process on which to accept connections.
                accept_fd: IO.socket_t,
                /// Address the accept_fd is bound to, as reported by `getsockname`.
                ///
                /// This allows passing port 0 as an address for the OS to pick an open port for us
                /// in a TOCTOU immune way and logging the resulting port number.
                accept_address: std.net.Address,
                accept_completion: IO.Completion = undefined,
            },
            .client => void,
        },
        /// Prefix for log messages.
        id: u128,

        /// The callback to be called when a message is received.
        on_messages_callback: *const fn (message_bus: *MessageBus, buffer: *MessageBuffer) void,

        connections: ConnectionSet,

        resume_receive_completion: IO.Completion = undefined,
        resume_receive_submitted: bool = false,

        /// The number of outgoing `connect()` attempts for a given replica:
        /// Reset to zero after a successful `on_connect()`.
        replicas_connect_attempts: []u64,

        /// Used to apply jitter when calculating exponential backoff:
        /// Seeded with the process' replica index or client ID.
        prng: stdx.PRNG,

        pub const Options = struct {
            configuration: []const std.net.Address,
            io: *IO,
            clients_limit: ?usize = null,
        };

        /// Initialize the MessageBus for the given configuration and replica/client process.
        pub fn init(
            allocator: mem.Allocator,
            process_id: ProcessID,
            message_pool: *MessagePool,
            on_messages_callback: *const fn (message_bus: *MessageBus, buffer: *MessageBuffer) void,
            options: Options,
        ) !MessageBus {
            assert(@as(vsr.ProcessType, process_id) == process_type);

            switch (process_type) {
                .replica => assert(options.clients_limit.? > 0),
                .client => assert(options.clients_limit == null),
            }

            const connections = ConnectionSet.init(allocator, .{
                .replicas = options.configuration.len,
                .connections_max = switch (process_type) {
                    // The maximum number of connections that can be held open by the server at any
                    // time. -1 since we don't need a connection to ourself.
                    .replica => @intCast(options.configuration.len - 1 + options.clients_limit.?),
                    .client => @intCast(options.configuration.len),
                },
            });
            errdefer connections.deinit(connections);

            const replicas_connect_attempts = try allocator.alloc(u64, options.configuration.len);
            errdefer allocator.free(replicas_connect_attempts);
            @memset(replicas_connect_attempts, 0);

            const prng_seed = switch (process_type) {
                .replica => process_id.replica,
                .client => @as(u64, @truncate(process_id.client)),
            };

            const process: @FieldType(MessageBus, "process") = switch (process_type) {
                .replica => blk: {
                    const address = options.configuration[process_id.replica];
                    const fd = try init_tcp(options.io, address.any.family);
                    errdefer options.io.close_socket(fd);

                    const accept_address = try options.io.listen(fd, address, .{
                        .backlog = constants.tcp_backlog,
                    });

                    break :blk .{
                        .replica = process_id.replica,
                        .accept_fd = fd,
                        .accept_address = accept_address,
                    };
                },
                .client => {},
            };

            return .{
                .pool = message_pool,
                .io = options.io,
                .configuration = options.configuration,
                .process = process,
                .id = switch (process_id) {
                    .replica => |index| @as(u128, index),
                    .client => |id| id,
                },
                .on_messages_callback = on_messages_callback,
                .connections = connections,
                .replicas_connect_attempts = replicas_connect_attempts,
                .prng = stdx.PRNG.from_seed(prng_seed),
            };
        }

        pub fn deinit(bus: *MessageBus, allocator: std.mem.Allocator) void {
            if (process_type == .replica) {
                bus.io.close_socket(bus.process.accept_fd);
            }

            for (bus.connections.connections) |*connection| {
                if (connection.fd != IO.INVALID_SOCKET) {
                    bus.io.close_socket(connection.fd);
                }

                if (connection.recv_buffer) |*buffer| buffer.deinit(bus.pool);
                connection.recv_buffer = null;
                while (connection.send_queue.pop()) |message| bus.unref(message);
            }
            bus.connections.deinit(allocator);
            allocator.free(bus.replicas_connect_attempts);
        }

        fn init_tcp(io: *IO, family: u32) !IO.socket_t {
            return try io.open_socket_tcp(family, .{
                .rcvbuf = constants.tcp_rcvbuf,
                .sndbuf = switch (process_type) {
                    .replica => constants.tcp_sndbuf_replica,
                    .client => constants.tcp_sndbuf_client,
                },
                .keepalive = if (constants.tcp_keepalive) .{
                    .keepidle = constants.tcp_keepidle,
                    .keepintvl = constants.tcp_keepintvl,
                    .keepcnt = constants.tcp_keepcnt,
                } else null,
                .user_timeout_ms = constants.tcp_user_timeout_ms,
                .nodelay = constants.tcp_nodelay,
            });
        }

        pub fn tick(bus: *MessageBus) void {
            switch (process_type) {
                .replica => {
                    // Each replica is responsible for connecting to replicas that come
                    // after it in the configuration. This ensures that replicas never try
                    // to connect to each other at the same time.
                    const replica_next = bus.process.replica + 1;
                    for (
                        bus.connections.replicas[replica_next..],
                        replica_next..,
                    ) |*connection, replica| {
                        if (connection == null) bus.connect_to_replica(@intCast(replica));
                    }
                    assert(bus.connections_used >= bus.replicas.len - replica_next);

                    // Only replicas accept connections from other replicas and clients:
                    bus.accept();
                },
                .client => {
                    // The client connects to all replicas.
                    for (bus.connections.replicas, 0..) |connection, replica| {
                        if (connection == null) bus.connect_to_replica(@intCast(replica));
                    }
                    assert(bus.connections_used >= bus.replicas.len);
                },
            }
        }

        fn connect_to_replica(bus: *MessageBus, replica: u8) void {
            assert(bus.connections.replicas[replica] == null);
            if (process_type == .replica) assert(replica != bus.process.replica);

            // Obtain a connection struct for our new replica connection.
            // If there is a free connection, use that. Otherwise drop
            // a client or unknown connection to make space. Prefer dropping
            // a client connection to an unknown one as the unknown peer may
            // be a replica. Since shutting a connection down does not happen
            // instantly, simply return after starting the shutdown and try again
            // on the next tick().
            const connection = bus.connections.next_free() orelse {
                bus.connect_to_replica_free_up_connection();
                return;
            };
            assert(connection.status == .free);
            assert(connection.fd == IO.INVALID_SOCKET);
            bus.connections.transition(connection, .free, .{ .replica = replica });
            assert(bus.connections.replicas[replica] == connection);
            connection.peer_certain = true;

            const family = bus.configuration[replica].any.family;
            connection.fd = init_tcp(bus.io, family) catch return;

            assert(bus.replicas[replica] == null);

            const ms = vsr.exponential_backoff_with_jitter(
                &bus.prng,
                constants.connection_delay_min_ms,
                constants.connection_delay_max_ms,
                bus.replicas_connect_attempts[replica],
            );
            bus.replicas_connect_attempts[replica] += 1;

            log.debug("{}: connect_to_replica: connecting to={} after={}ms", .{
                bus.id,
                connection.status.replica,
                ms,
            });

            assert(!connection.recv_submitted);
            connection.recv_submitted = true;

            assert(!connection.terminating);

            bus.io.timeout(
                *MessageBus,
                bus,
                connect_to_replica_timeout_callback,
                // We use `recv_completion` for the connection `timeout()` and `connect()` calls
                &connection.recv_completion,
                @as(u63, @intCast(ms * std.time.ns_per_ms)),
            );
        }

        fn connect_to_replica_free_up_connection(bus: *MessageBus) void {
            assert(bus.connections.next_free() == null);
            for (bus.connections.all) |*connection| {
                // If there is already a connection being shut down, no need to kill another.
                if (connection.terminating) return;
            }

            for (bus.connections.all) |*connection| {
                if (connection.status == .client) {
                    log.info(
                        "{}: connect_to_replica: no free connection, disconnecting a client",
                        .{bus.id},
                    );
                    connection.terminate(bus, .shutdown);
                    return;
                }
            }
            for (bus.connections.all) |*connection| {
                if (connection.status == .unknown) {
                    log.info(
                        "{}: connect_to_replica: no free connection, disconnecting unknown peer",
                        .{bus.id},
                    );
                    connection.terminate(bus, .shutdown);
                    return;
                }
            }

            // We assert that the max number of connections is greater
            // than the number of replicas in init().
            unreachable;
        }

        fn connect_to_replica_timeout_callback(
            bus: *MessageBus,
            completion: *IO.Completion,
            result: IO.TimeoutError!void,
        ) void {
            const connection: *Connection = @alignCast(
                @fieldParentPtr("recv_completion", completion),
            );
            assert(connection.status == .replica);
            assert(connection.peer_certain);
            assert(connection.recv_submitted);
            connection.recv_submitted = false;
            if (connection.terminating) { // FIXME
                connection.maybe_close(bus);
                return;
            }
            result catch unreachable;

            log.debug("{}: on_connect_with_exponential_backoff: to={}", .{
                bus.id,
                connection.status.replica,
            });

            assert(!connection.recv_submitted);
            connection.recv_submitted = true;

            bus.io.connect(
                *MessageBus,
                bus,
                connect_to_replica_connect_callback,
                // We use `recv_completion` for the connection `timeout()` and `connect()` calls
                &connection.recv_completion,
                connection.fd,
                bus.configuration[connection.status.replica],
            );
        }

        fn connect_to_replica_connect_callback(
            bus: *MessageBus,
            completion: *IO.Completion,
            result: IO.ConnectError!void,
        ) void {
            const connection: *Connection = @alignCast(
                @fieldParentPtr("recv_completion", completion),
            );
            assert(connection.status == .replica);
            assert(connection.peer_certain);
            assert(connection.recv_submitted);
            connection.recv_submitted = false;

            if (connection.terminating) { // FIXME
                connection.maybe_close(bus);
                return;
            }

            result catch |err| {
                log.warn("{}: on_connect: error to={} {}", .{
                    bus.id,
                    connection.status.replica,
                    err,
                });
                connection.terminate(bus, .close);
                return;
            };

            log.info("{}: on_connect: connected to={}", .{ bus.id, connection.status.replica });
            bus.replicas_connect_attempts[connection.status.replica] = 0;

            // A message may have been queued for sending while we were connecting:
            // TODO Should we relax recv() and send() to return if `state != .connected`?
            if (connection.state == .connected) connection.send(bus);
            bus.recv_loop_start(connection);
        }

        fn accept(bus: *MessageBus) void {
            comptime assert(process_type == .replica);

            if (bus.connections.accept != null) return;
            // All connections are currently in use, do nothing.
            const connection = bus.connections.next_free() orelse return;
            assert(connection.status == .free);

            bus.connections.transition(connection, .free, .accept);
            assert(connection.status == .accept);
            assert(!connection.peer_certain);

            bus.io.accept(
                *MessageBus,
                bus,
                accept_callback,
                connection,
                bus.process.accept_fd,
            );
        }

        fn accept_callback(
            bus: *MessageBus,
            _: *IO.Completion,
            result: IO.AcceptError!IO.socket_t,
        ) void {
            comptime assert(process_type == .replica);
            assert(bus.connections.accept != null);
            defer assert(bus.connections.accept == null);
            const connection = bus.connections.accept.?;
            assert(connection.status == .accept);
            assert(!connection.peer_certain);
            assert(connection.fd == IO.INVALID_SOCKET);

            const fd = result catch |err| {
                log.warn("{}: on_accept: {}", .{ bus.id, err });
                bus.connections.transition(connection, .accept, .free);
                bus.process.accept_connection.?.state = .free;
                return; // TODO: Some errors should probably be fatal.
            };

            bus.connections.transition(connection, .accept, .unknown);
            assert(connection.status == .accept);
            assert(!connection.peer_certain);
            connection.fd = fd;

            assert(connection.send_queue.empty());
            bus.recv_loop_start(connection);
        }

        fn recv_loop_start(bus: *MessageBus, connection: *Connection) void {
            assert(connection.status == .unknown or
                connection.status == .replica or
                connection.status == .client);
            assert(!connection.terminating);
            assert(connection.fd != IO.INVALID_SOCKET);

            assert(connection.recv_submitted == false);
            assert(connection.recv_buffer == null);

            assert(connection.send_submitted == false);
            assert(connection.send_progress == 0);

            assert(connection.recv_buffer == null);
            connection.recv_buffer = MessageBuffer.init(bus.pool);
            bus.recv(connection);
        }

        fn recv(bus: *MessageBus, connection: *Connection) void {
            assert(connection.status == .unknown or
                connection.status == .replica or
                connection.status == .client);
            assert(!connection.terminating);
            assert(connection.fd != IO.INVALID_SOCKET);

            assert(!connection.recv_submitted);
            connection.recv_submitted = true;

            bus.io.recv(
                *MessageBus,
                bus,
                recv_callback,
                &connection.recv_completion,
                connection.fd,
                connection.recv_buffer.?.recv_slice(),
            );
        }

        fn recv_callback(
            bus: *MessageBus,
            completion: *IO.Completion,
            result: IO.RecvError!usize,
        ) void {
            const connection: *Connection = @alignCast(
                @fieldParentPtr("recv_completion", completion),
            );
            assert(connection.recv_submitted);
            connection.recv_submitted = false;
            if (connection.terminating) {
                connection.maybe_close(bus);
                return;
            }
            const bytes_received = result catch |err| {
                // TODO: maybe don't need to close on *every* error
                log.warn("{}: on_recv: from={} {}", .{ bus.id, connection.peer, err });
                connection.terminate(bus, .shutdown);
                return;
            };
            // No bytes received means that the peer closed its side of the connection.
            if (bytes_received == 0) {
                log.info("{}: on_recv: from={} orderly shutdown", .{ bus.id, connection.peer });
                connection.terminate(bus, .close);
                return;
            }
            assert(bytes_received <= constants.message_size_max);
            assert(connection.recv_buffer != null);
            connection.recv_buffer.?.recv_advance(@intCast(bytes_received));

            switch (process_type) {
                // Replicas may forward messages from clients or from other replicas so we
                // may receive messages from a peer before we know who they are:
                // This has the same effect as an asymmetric network where, for a short time
                // bounded by the time it takes to ping, we can hear from a peer before we
                // can send back to them.
                .replica => {
                    while (connection.recv_buffer.?.next_header()) |header| {
                        connection.recv_buffer.?.suspend_message(&header);
                        connection.set_peer(bus, header.peer_type());
                    }
                },
                // The client connects only to replicas and should set peer when connecting:
                .client => assert(connection.peer == .replica),
            }
            connection.call_on_messages(bus);
        }

        fn set_peer(bus: *MessageBus, connection: *Connection, peer: vsr.Peer) void {
            comptime assert(process_type == .replica);

            assert(connection.status == .unknown or
                connection.status == .replica or
                connection.status == .client);
            assert(connection.fd != IO.INVALID_SOCKET);
            assert(connection.recv_buffer != null);

            switch (vsr.Peer.transition(connection.peer, peer)) {
                .retain => return true,
                .reject => return false,
                .update => {},
            }

            defer connection.peer = peer;

            switch (peer) {
                .unknown => return,
                .replica => |replica_index| {
                    if (connection.status == .unknown) {
                        if (bus.connections.replicas[replica_index]) |*old| {}

                        bus.connections.transition(
                            connection,
                            .unknown,
                            .{ .replica = replica_index },
                        );
                    }
                    if (connection.status == .replica) {
                        if (connection.status.replica != replica_index) {
                            bus.terminate(connectoin, .shutdown);
                        }
                        return;
                    }
                    if (conne)
                        if (replica_index >= bus.configuration.len) return false;

                    // Allowed transitions:
                    // * unknown        → replica
                    // * client_likely  → replica
                    assert(connection.peer == .unknown or connection.peer == .client_likely);

                    // If there is a connection to this replica, terminate and replace it.
                    if (bus.replicas[replica_index]) |old| {
                        assert(old != connection);
                        assert(old.peer == .replica);
                        assert(old.peer.replica == replica_index);
                        assert(old.state != .free);
                        if (old.state != .terminating) old.terminate(bus, .shutdown);
                    }

                    switch (connection.peer) {
                        .unknown => {},
                        // If this connection was misclassified to a client due to a forwarded
                        // request message (see `peer_type` in message_header.zig), it may
                        // reside in the clients map. If so, it must be popped and mapped to the
                        // correct replica.
                        .client_likely => |existing| {
                            if (bus.process.clients.get(existing)) |existing_connection| {
                                if (existing_connection == connection) {
                                    assert(bus.process.clients.remove(existing));
                                }
                            }
                        },
                        .replica, .client => unreachable,
                    }

                    bus.replicas[replica_index] = connection;
                    log.info("{}: set_and_verify_peer: connection from replica={}", .{
                        bus.id,
                        replica_index,
                    });
                },
                .client => |client_id| {
                    assert(client_id != 0);

                    // Allowed transitions:
                    // * unknown        → client
                    // * client_likely  → client
                    assert(connection.peer == .unknown or connection.peer == .client_likely);

                    // If there is a connection to this client, terminate and replace it.
                    const result = bus.process.clients.getOrPutAssumeCapacity(client_id);
                    if (result.found_existing) {
                        const old = result.value_ptr.*;
                        assert(old.state == .connected or old.state == .terminating);
                        if (connection.peer == .unknown) assert(old != connection);

                        switch (old.peer) {
                            .client, .client_likely => |client| {
                                assert(client == client_id);
                            },
                            .unknown, .replica => unreachable,
                        }

                        if (old != connection and old.state != .terminating) {
                            old.terminate(bus, .shutdown);
                        }
                    }

                    result.value_ptr.* = connection;
                    log.info("{}: set_and_verify_peer connection from client={}", .{
                        bus.id,
                        client_id,
                    });
                },

                .client_likely => |client_id| {
                    assert(client_id != 0);
                    switch (connection.peer) {
                        .unknown => {
                            // If the peer transitions from unknown -> client_likely, either
                            // a replica or a client may be sending a request message. Instead
                            // of terminating an existing connection and replacing it, if one
                            // exists in the client map, we wait for it to get resolved to
                            // either a replica or a client.
                            const result =
                                bus.process.clients.getOrPutAssumeCapacity(client_id);
                            if (!result.found_existing) {
                                result.value_ptr.* = connection;
                                log.info("{}: set_and_verify_peer connection from " ++
                                    "client_likely={}", .{ bus.id, client_id });
                            }
                        },
                        .replica, .client, .client_likely => unreachable,
                    }
                },
                .unknown => {},
            }
        }

        pub fn terminate(bus: *MessageBus, connection: *Connection) void {}

        pub fn get_message(
            bus: *MessageBus,
            comptime command: ?vsr.Command,
        ) MessagePool.GetMessageType(command) {
            return bus.pool.get_message(command);
        }

        /// `@TypeOf(message)` is one of:
        /// - `*Message`
        /// - `MessageType(command)` for any `command`.
        pub fn unref(bus: *MessageBus, message: anytype) void {
            bus.pool.unref(message);
        }

        pub fn resume_needed(bus: *MessageBus) bool {
            if (bus.connections_suspended.empty()) return false;
            if (bus.resume_receive_submitted) return false;
            return true;
        }

        pub fn resume_receive(bus: *MessageBus) void {
            if (!bus.resume_needed()) return;

            bus.resume_receive_submitted = true;
            bus.io.timeout(
                *MessageBus,
                bus,
                ready_to_receive_callback,
                &bus.resume_receive_completion,
                0, // Zero timeout means next tick.
            );
        }

        fn ready_to_receive_callback(
            bus: *MessageBus,
            completion: *IO.Completion,
            result: IO.TimeoutError!void,
        ) void {
            assert(completion == &bus.resume_receive_completion);
            _ = result catch |e| switch (e) {
                error.Canceled => unreachable,
                error.Unexpected => unreachable,
            };
            assert(bus.resume_receive_submitted);
            bus.resume_receive_submitted = false;
            maybe(bus.connections_suspended.empty());

            // Steal the queue to avoid an infinite loop.
            var connections_suspended = bus.connections_suspended;
            bus.connections_suspended.reset();

            while (connections_suspended.pop()) |connection| {
                assert(connection.recv_buffer != null);
                assert(connection.recv_buffer.?.advance_size >= @sizeOf(vsr.Header));
                assert(connection.recv_buffer.?.has_message());
                connection.call_on_messages(bus);
            }
        }

        pub fn send_message_to_replica(bus: *MessageBus, replica: u8, message: *Message) void {
            // Messages sent by a replica to itself should never be passed to the message bus.
            if (process_type == .replica) assert(replica != bus.process.replica);

            if (bus.replicas[replica]) |connection| {
                connection.send_message(bus, message);
            } else {
                log.debug("{}: send_message_to_replica: no connection to={} header={}", .{
                    bus.id,
                    replica,
                    message.header,
                });
            }
        }

        /// Try to send the message to the client with the given id.
        /// If the client is not currently connected, the message is silently dropped.
        pub fn send_message_to_client(bus: *MessageBus, client_id: u128, message: *Message) void {
            comptime assert(process_type == .replica);

            if (bus.process.clients.get(client_id)) |connection| {
                connection.send_message(bus, message);
            } else {
                log.debug(
                    "{}: send_message_to_client: no connection to={}",
                    .{ bus.id, client_id },
                );
            }
        }
    };
}

pub const ConnectionSet = struct {
    all: []Connection,

    free: std.DynamicBitSetUnmanaged,
    accept: ?*Connection,
    unknown: std.DynamicBitSetUnmanaged,
    replicas: []?*Connection,
    clients: std.AutoHashMapUnmanaged(u128, *Connection),

    connections_suspended: QueueType(Connection) = .init(.{ .name = null }),

    pub fn init(gpa: mem.Allocator, options: struct {
        replicas: u8,
        connections_max: u32,
    }) !ConnectionSet {
        const all = gpa.alloc(Connection, options.connections_max);
        @memset(all, .{});
        errdefer gpa.free(all);

        const free = std.DynamicBitSetUnmanaged.initFull(gpa, options.connections_max);
        errdefer gpa.free(free);

        const unknown = std.DynamicBitSetUnmanaged.initEmpty(gpa, options.connections_max);
        errdefer gpa.free(unknown);

        const replicas = gpa.alloc(?*Connection, options.replica_count);
        @memset(replicas, null);
        errdefer gpa.free(replicas);

        const clients: std.AutoArrayHashMapUnmanaged(u128, *Connection) = .{};
        errdefer clients.deinit(gpa);

        clients.ensureTotalCapacity(options.connections_max);

        const connections: ConnectionSet = .{
            .all = all,

            .free = free,
            .accept = null,
            .unknown = unknown,
            .replicas = replicas,
            .clients = clients,
        };
        connections.assert_invariants();
        return connections;
    }

    pub fn deinit(connections: *ConnectionSet, gpa: mem.Allocator) void {
        connections.clients.deinit(gpa);
        gpa.free(connections.replicas);
        connections.unknown.deinit(gpa);
        connections.free.deinit(gpa);
        gpa.free(connections.connections);
        connections.* = undefined;
    }

    pub fn assert_invariants(connections: *ConnectionSet) void {
        assert(connections.connectons.len == connections.free.capacity());
        assert(connections.connectons.len == connections.unknown.capacity());
        assert(connections.connectons.len == connections.clients.capacity());

        for (connections.all, 0..) |*connection, index| {
            assert(connections.free.isSet(index) == (connection.status == .free));
            assert(connections.unknown.isSet(index) == (connection.status == .unknown));
            assert((connections.accept == connection) == (connection.status == .accept));
            switch (connection.status) {
                .free, .accept, .unknown => {},
                .replica => |replica| assert(connections.replicas[replica] == connection),
                .client => |client| assert(connections.clients.get(client) == connection),
            }
        }

        var bit_set_iterator = connections.free.iterator(.{});
        while (bit_set_iterator.next()) |index| {
            assert(connections.connections[index].status == .free);
        }

        if (connections.accept) |connection| assert(connection.status == .accept);

        bit_set_iterator = connections.unknown.iterator(.{});
        while (bit_set_iterator.next()) |index| {
            assert(connections.connections[index].status == .unknown);
        }

        for (connections.replicas, 0) |connection_maybe, index| {
            if (connection_maybe) |connection| {
                assert(connection.status == .replica);
                assert(connection.status.replica == index);
            }
        }

        var map_iterator = connections.clients.iterator();
        while (map_iterator.next()) |entry| {
            assert(entry.value_ptr.status == .client);
            assert(entry.value_ptr.status.client == entry.key_ptr.*);
        }
        assert(
            connections.all.len ==
                (connections.free.count() +
                    @intFromBool(connections.accept != null) +
                    connections.unknown.count() +
                    b: {
                        var sum: usize = 0;
                        for (connections.replicas) |connection| {
                            sum += @intFromBool(connection != null);
                        }
                        break :b sum;
                    } +
                    connections.clients.count()),
        );
    }

    pub fn transition(
        connections: *ConnectionSet,
        connection: *Connection,
        from: Connection.Status,
        to: Connection.Status,
    ) void {
        assert(std.meta.eql(connection.status, from));
        connection.status = to;

        const index = connections.index_of(connection);
        if (from == .free and to == .replica) {
            assert(connections.free.isSet(index));
            assert(!connections.unknown.isSet(index));
            assert(connections.replcias[to.replica] == null);

            connections.free.unset(index);
            connections.replicas[to.replica] = connection;
        } else if (from == .free and to == .accept) {
            assert(connections.free.isSet(index));
            assert(!connections.unknown.isSet(index));
            assert(connections.accept == null);

            connections.free.unset(index);
            connections.accept = connection;
        } else if (from == .accept and to == .free) {
            assert(!connections.free.isSet(index));
            assert(!connections.unknown.isSet(index));
            assert(connections.accept.? == connection);

            connections.free.set(index);
            connections.accept = null;
            connection.* = .{};
        } else if (from == .accept and to == .unknown) {
            assert(!connections.free.isSet(index));
            assert(!connections.unknown.isSet(index));
            assert(connections.accept.? == connection);

            assert(connections.unknown.set(index));
            connections.accept = null;
        }
        @panic("invalid transition");
    }

    pub fn next_free(connections: *const ConnectionSet) ?*Connection {
        const index = connections.free.findFirstSet() orelse return null;
        assert(connections.all[index].status == .free);
        return &connections.all[index];
    }

    fn index_of(connections: *const ConnectionSet, connection: *Connection) usize {
        return @divExact(
            @intFromPtr(connection.ptr - connections.all.ptr),
            @sizeOf(Connection),
        );
    }
};

/// Used to send/receive messages to/from a client or fellow replica.
const Connection = struct {
    status: Status = .free,

    /// This is guaranteed to be valid only while state is connected.
    /// It will be reset to IO.INVALID_SOCKET during the shutdown process and is always
    /// IO.INVALID_SOCKET if the connection is unused (i.e. peer == .none). We use
    /// IO.INVALID_SOCKET instead of undefined here for safety to ensure an error if the
    /// invalid value is ever used, instead of potentially performing an action on an
    /// active fd.
    fd: IO.socket_t = IO.INVALID_SOCKET,

    peer_certain: bool = false,

    /// This completion is used for all recv operations.
    /// It is also used for the initial connect when establishing a replica connection.
    recv_completion: IO.Completion = undefined,
    /// True exactly when the recv_completion has been submitted to the IO abstraction
    /// but the callback has not yet been run.
    recv_submitted: bool = false,
    recv_buffer: ?MessageBuffer = null,

    /// For connections_suspended.
    link: QueueType(Connection).Link = .{},

    /// This completion is used for all send operations.
    send_completion: IO.Completion = undefined,
    /// True exactly when the send_completion has been submitted to the IO abstraction
    /// but the callback has not yet been run.
    send_submitted: bool = false,
    /// Number of bytes of the current message that have already been sent.
    send_progress: usize = 0,
    /// The queue of messages to send to the client or replica peer.
    send_queue: SendQueue = SendQueue.init(),

    terminating: bool = false,

    const Status = union(enum) {
        free,
        accept,
        unknown,
        replica: u8,
        client: u128,
        terminting,
    };

    const SendQueue = RingBufferType(*Message, .{
        .array = @max(
            constants.connection_send_queue_max_replica,
            constants.connection_send_queue_max_client,
        ),
    });

    /// Attempt to connect to a replica.
    /// The slot in the Message.replicas slices is immediately reserved.
    /// Failure is silent and returns the connection to an unused state.
    pub fn connect_to_replica(connection: *Connection, bus: *MessageBus, replica: u8) void {}

    /// Given a newly accepted fd, start receiving messages on it.
    /// Callbacks will be continuously re-registered until terminate() is called.
    pub fn on_accept(connection: *Connection, bus: *MessageBus, fd: IO.socket_t) void {}

    fn assert_recv_send_initial_state(connection: *Connection, bus: *MessageBus) void {}

    /// Add a message to the connection's send queue, starting a send operation
    /// if the queue was previously empty.
    pub fn send_message(connection: *Connection, bus: *MessageBus, message: *Message) void {
        assert(connection.peer != .unknown);

        switch (connection.state) {
            .connected, .connecting => {},
            .terminating => return,
            .free, .accepting => unreachable,
        }
        if (connection.send_queue.full()) {
            log.info("{}: send_message: to={} queue full, dropping command={s}", .{
                bus.id,
                connection.peer,
                @tagName(message.header.command),
            });
            return;
        }
        connection.send_queue.push_assume_capacity(message.ref());
        // If the connection has not yet been established we can't send yet.
        // Instead on_connect() will call send().
        if (connection.state == .connecting) {
            assert(connection.peer == .replica);
            return;
        }
        // If there is no send operation currently in progress, start one.
        if (!connection.send_submitted) connection.send(bus);
    }

    /// Clean up an active connection and reset it to its initial, unused, state.
    /// This reset does not happen instantly as currently in progress operations
    /// must first be stopped. The `how` arg allows the caller to specify if a
    /// shutdown syscall should be made or not before proceeding to wait for
    /// currently in progress operations to complete and close the socket.
    /// I'll be back! (when the Connection is reused after being fully closed)
    pub fn terminate(
        connection: *Connection,
        bus: *MessageBus,
        how: enum { shutdown, close },
    ) void {
        assert(connection.state != .free);
        assert(connection.fd != IO.INVALID_SOCKET);
        switch (how) {
            .shutdown => {
                // The shutdown syscall will cause currently in progress send/recv
                // operations to be gracefully closed while keeping the fd open.
                //
                // TODO: Investigate differences between shutdown() on Linux vs Darwin.
                // Especially how this interacts with our assumptions around pending I/O.
                bus.io.shutdown(connection.fd, .both) catch |err| switch (err) {
                    error.SocketNotConnected => {
                        // This should only happen if we for some reason decide to terminate
                        // a connection while a connect operation is in progress.
                        // This is fine though, we simply continue with the logic below and
                        // wait for the connect operation to finish.

                        // TODO: This currently happens in other cases if the
                        // connection was closed due to an error. We need to intelligently
                        // decide whether to shutdown or close directly based on the error
                        // before these assertions may be re-enabled.

                        //assert(connection.state == .connecting);
                        //assert(connection.recv_submitted);
                        //assert(!connection.send_submitted);
                    },
                    // Ignore all the remaining errors for now
                    error.ConnectionAborted,
                    error.ConnectionResetByPeer,
                    error.BlockingOperationInProgress,
                    error.NetworkSubsystemFailed,
                    error.SystemResources,
                    error.Unexpected,
                    => {},
                };
            },
            .close => {},
        }
        assert(connection.state != .terminating);
        connection.state = .terminating;
        connection.maybe_close(bus);
    }

    fn set_and_verify_peer(connection: *Connection, bus: *MessageBus, peer: vsr.Peer) bool {
        return true;
    }

    fn recv(connection: *Connection, bus: *MessageBus) void {}

    fn call_on_messages(connection: *Connection, bus: *MessageBus) void {
        if (connection.recv_buffer.?.has_message()) {
            bus.on_messages_callback(bus, &connection.recv_buffer.?);
        }

        if (connection.recv_buffer.?.invalid) |reason| {
            log.warn("{}: on_recv: from={} terminating connection: invalid {s}", .{
                bus.id,
                connection.peer,
                @tagName(reason),
            });
            connection.terminate(bus, .close);
            return;
        }

        if (connection.recv_buffer.?.has_message()) {
            maybe(connection.state == .terminating);
            bus.connections_suspended.push(connection);
        } else {
            if (connection.state == .terminating) {
                connection.maybe_close(bus);
            } else {
                connection.recv(bus);
            }
        }
    }

    fn send(connection: *Connection, bus: *MessageBus) void {
        assert(connection.peer != .unknown);
        assert(connection.state == .connected);
        assert(connection.fd != IO.INVALID_SOCKET);
        assert(!connection.send_submitted);

        connection.send_now(bus);

        const message = connection.send_queue.head() orelse return;
        connection.send_submitted = true;
        bus.io.send(
            *MessageBus,
            bus,
            on_send,
            &connection.send_completion,
            connection.fd,
            message.buffer[connection.send_progress..message.header.size],
        );
    }

    // Optimization/fast path: try to immediately copy the send queue over to the in-kernel
    // send buffer, falling back to asynchronous send if that's not possible.
    fn send_now(connection: *Connection, bus: *MessageBus) void {
        assert(connection.state == .connected);
        assert(connection.fd != IO.INVALID_SOCKET);
        assert(!connection.send_submitted);

        for (0..SendQueue.count_max) |_| {
            const message = connection.send_queue.head() orelse return;
            assert(connection.send_progress < message.header.size);
            const write_size = bus.io.send_now(
                connection.fd,
                message.buffer[connection.send_progress..message.header.size],
            ) orelse return;
            connection.send_progress += write_size;
            assert(connection.send_progress <= message.header.size);
            if (connection.send_progress == message.header.size) {
                _ = connection.send_queue.pop();
                bus.unref(message);
                connection.send_progress = 0;
            } else {
                assert(connection.send_progress < message.header.size);
                return;
            }
        }
    }

    fn on_send(
        bus: *MessageBus,
        completion: *IO.Completion,
        result: IO.SendError!usize,
    ) void {
        const connection: *Connection = @alignCast(
            @fieldParentPtr("send_completion", completion),
        );
        assert(connection.send_submitted);
        connection.send_submitted = false;
        assert(connection.peer != .unknown);
        if (connection.state == .terminating) {
            connection.maybe_close(bus);
            return;
        }
        assert(connection.state == .connected);
        connection.send_progress += result catch |err| {
            // TODO: maybe don't need to close on *every* error
            log.warn("{}: on_send: to={} {}", .{
                bus.id,
                connection.peer,
                err,
            });
            connection.terminate(bus, .shutdown);
            return;
        };
        assert(connection.send_progress <= connection.send_queue.head().?.header.size);
        // If the message has been fully sent, move on to the next one.
        if (connection.send_progress == connection.send_queue.head().?.header.size) {
            connection.send_progress = 0;
            const message = connection.send_queue.pop().?;
            bus.unref(message);
        }
        connection.send(bus);
    }

    fn maybe_close(connection: *Connection, bus: *MessageBus) void {
        assert(connection.state == .terminating);
        // If a recv or send operation is currently submitted to the kernel,
        // submitting a close would cause a race. Therefore we must wait for
        // any currently submitted operation to complete.
        if (connection.recv_submitted or connection.send_submitted) return;
        // Even if there's no active physical IO in progress, we want to wait until all
        // messages already received are consumed, to prevent graceful termination of
        // connection from dropping messages.
        if (connection.recv_buffer) |*receive_buffer| {
            if (receive_buffer.has_message()) return;
        }
        connection.send_submitted = true;
        connection.recv_submitted = true;
        // We can free resources now that there is no longer any I/O in progress.
        while (connection.send_queue.pop()) |message| {
            bus.unref(message);
        }
        if (connection.recv_buffer) |*buffer| buffer.deinit(bus.pool);
        connection.recv_buffer = null;
        assert(connection.fd != IO.INVALID_SOCKET);
        defer connection.fd = IO.INVALID_SOCKET;
        // It's OK to use the send completion here as we know that no send
        // operation is currently in progress.
        bus.io.close(
            *MessageBus,
            bus,
            on_close,
            &connection.send_completion,
            connection.fd,
        );
    }

    fn on_close(
        bus: *MessageBus,
        completion: *IO.Completion,
        result: IO.CloseError!void,
    ) void {
        const connection: *Connection = @alignCast(
            @fieldParentPtr("send_completion", completion),
        );
        assert(connection.send_submitted);
        assert(connection.recv_submitted);

        assert(connection.state == .terminating);

        // Reset the connection to its initial state.
        defer {
            assert(connection.recv_buffer == null);
            assert(connection.send_queue.empty());

            switch (connection.peer) {
                .unknown => {},
                .client, .client_likely => |client_id| switch (process_type) {
                    .replica => {
                        // A newer client connection may have replaced this one:
                        if (bus.process.clients.get(client_id)) |existing_connection| {
                            if (existing_connection == connection) {
                                assert(bus.process.clients.remove(client_id));
                            }
                        } else {
                            // A newer client connection may even leapfrog this connection
                            // and then be terminated and set to null before we can get
                            // here.
                        }
                    },
                    .client => unreachable,
                },
                .replica => |replica| {
                    // A newer replica connection may have replaced this one:
                    if (bus.replicas[replica] == connection) {
                        bus.replicas[replica] = null;
                    } else {
                        // A newer replica connection may even leapfrog this connection and
                        // then be terminated and set to null before we can get here:
                        stdx.maybe(bus.replicas[replica] == null);
                    }
                },
            }
            bus.connections_used -= 1;
            connection.* = .{};
        }

        result catch |err| {
            log.warn("{}: on_closing: to={} {}", .{ bus.id, connection.peer, err });
            return;
        };
    }
};
