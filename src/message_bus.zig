const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const constants = @import("constants.zig");
const log = std.log.scoped(.message_bus);

const vsr = @import("vsr.zig");

const stdx = @import("stdx");
const maybe = stdx.maybe;
const RingBufferType = stdx.RingBufferType;
const MessagePool = @import("message_pool.zig").MessagePool;
const Message = MessagePool.Message;
const MessageBuffer = @import("./message_buffer.zig").MessageBuffer;
const QueueType = @import("./queue.zig").QueueType;

pub fn MessageBusType(comptime IO: type) type {
    // Slice points to a subslice of send_queue_buffer.
    const SendQueue = RingBufferType(*Message, .slice);

    const ProcessID = union(vsr.ProcessType) {
        replica: u8,
        client: u128,
    };

    return struct {
        pool: *MessagePool,
        io: *IO,

        process: ProcessID,
        /// Prefix for log messages.
        id: u128,

        /// The file descriptor for the process on which to accept connections.
        accept_fd: ?IO.socket_t = null,
        /// Address the accept_fd is bound to, as reported by `getsockname`.
        ///
        /// This allows passing port 0 as an address for the OS to pick an open port for us
        /// in a TOCTOU immune way and logging the resulting port number.
        accept_address: ?Address = null,
        accept_completion: IO.Completion = undefined,
        /// The connection reserved for the currently in progress accept operation.
        /// This is non-null exactly when an accept operation is submitted.
        accept_connection: ?*Connection = null,

        /// The callback to be called when a message is received.
        on_messages_callback: *const fn (message_bus: *MessageBus, buffer: *MessageBuffer) void,

        /// SendQueue storage shared by all connections.
        send_queue_buffer: []*Message,
        /// This slice is allocated with a fixed size in the init function and never reallocated.
        connections: []Connection,
        /// Number of connections currently in use (i.e. connection.state != .free).
        connections_used: u32 = 0,
        connections_suspended: QueueType(Connection) = QueueType(Connection).init(.{
            .name = null,
        }),
        resume_receive_completion: IO.Completion = undefined,
        resume_receive_submitted: bool = false,

        /// Map from replica index to the currently active connection for that replica, if any.
        /// The connection for the process replica if any will always be null.
        replicas: []?*Connection,
        replicas_addresses: []Address,
        /// The number of outgoing `connect()` attempts for a given replica:
        /// Reset to zero after a successful `on_connect()`.
        replicas_connect_attempts: []u64,

        /// Map from client id to the currently active connection for that client.
        /// This is used to make lookup of client connections when sending messages
        /// efficient and to ensure old client connections are dropped if a new one
        /// is established.
        clients: std.AutoHashMapUnmanaged(u128, *Connection) = .{},

        /// Used to apply jitter when calculating exponential backoff:
        /// Seeded with the process' replica index or client ID.
        prng: stdx.PRNG,

        comptime {
            // Assert it is correct to use u32 to track sizes.
            assert(constants.message_size_max < std.math.maxInt(u32));
        }

        pub const Options = struct {
            configuration: []const Address,
            io: *IO,
            clients_limit: ?u32 = null,
        };
        const Address = std.net.Address;
        const MessageBus = @This();

        /// Initialize the MessageBus for the given configuration and replica/client process.
        pub fn init(
            allocator: mem.Allocator,
            process_id: ProcessID,
            message_pool: *MessagePool,
            on_messages_callback: *const fn (message_bus: *MessageBus, buffer: *MessageBuffer) void,
            options: Options,
        ) !MessageBus {
            switch (process_id) {
                .replica => assert(options.clients_limit.? > 0),
                .client => assert(options.clients_limit == null),
            }

            const connections_max: u32 = switch (process_id) {
                // The maximum number of connections that can be held open by the server at any
                // time. -1 since we don't need a connection to ourself.
                .replica => @intCast(options.configuration.len - 1 + options.clients_limit.?),
                .client => @intCast(options.configuration.len),
            };

            const send_queue_max = switch (process_id) {
                .replica => constants.connection_send_queue_max_replica,
                .client => constants.connection_send_queue_max_client,
            };

            const send_queue_buffer = try allocator.alloc(
                *Message,
                connections_max * send_queue_max,
            );
            @memset(send_queue_buffer, undefined);
            errdefer allocator.free(send_queue_buffer);

            const connections = try allocator.alloc(Connection, connections_max);
            errdefer allocator.free(connections);
            for (connections, 0..) |*connection, index| {
                connection.* = .{
                    .send_queue = .{
                        .buffer = send_queue_buffer[index * send_queue_max ..][0..send_queue_max],
                    },
                };
            }

            const replicas = try allocator.alloc(?*Connection, options.configuration.len);
            errdefer allocator.free(replicas);
            @memset(replicas, null);

            const replicas_addresses = try allocator.alloc(Address, options.configuration.len);
            errdefer allocator.free(replicas_addresses);
            stdx.copy_disjoint(.exact, Address, replicas_addresses, options.configuration);

            const replicas_connect_attempts = try allocator.alloc(u64, options.configuration.len);
            errdefer allocator.free(replicas_connect_attempts);
            @memset(replicas_connect_attempts, 0);

            const prng_seed = switch (process_id) {
                .replica => |replica| replica,
                .client => |client| @as(u64, @truncate(client)),
            };

            var bus: MessageBus = .{
                .pool = message_pool,
                .io = options.io,
                .process = process_id,
                .id = switch (process_id) {
                    .replica => |index| @as(u128, index),
                    .client => |id| id,
                },
                .on_messages_callback = on_messages_callback,
                .send_queue_buffer = send_queue_buffer,
                .connections = connections,
                .replicas = replicas,
                .replicas_addresses = replicas_addresses,
                .replicas_connect_attempts = replicas_connect_attempts,
                .prng = stdx.PRNG.from_seed(prng_seed),
            };

            switch (process_id) {
                .replica => {
                    // Pre-allocate enough memory to hold all possible connections
                    // in the client map.
                    try bus.clients.ensureTotalCapacity(allocator, connections_max);
                    errdefer bus.clients.deinit(allocator);

                    return bus;
                },
                .client => return bus,
            }
        }

        pub fn deinit(bus: *MessageBus, allocator: std.mem.Allocator) void {
            bus.clients.deinit(allocator);

            if (bus.accept_fd) |fd| {
                assert(bus.process == .replica);
                assert(bus.accept_address != null);
                bus.io.close_socket(fd);
            }

            const send_queue_max = switch (bus.process) {
                .replica => constants.connection_send_queue_max_replica,
                .client => constants.connection_send_queue_max_client,
            };
            var send_queue_buffer_previous: ?[]*Message = null;
            for (bus.connections) |*connection| {
                if (connection.fd) |fd| {
                    bus.io.close_socket(fd);
                }

                if (connection.recv_buffer) |*buffer| buffer.deinit(bus.pool);
                connection.recv_buffer = null;
                while (connection.send_queue.pop()) |message| bus.unref(message);

                assert(connection.send_queue.buffer.len == send_queue_max);
                if (send_queue_buffer_previous) |previous| {
                    assert(connection.send_queue.buffer.ptr == previous.ptr + previous.len);
                } else {
                    assert(connection.send_queue.buffer.ptr == bus.send_queue_buffer.ptr);
                }
                send_queue_buffer_previous = connection.send_queue.buffer;
            }
            assert(bus.send_queue_buffer.ptr + bus.send_queue_buffer.len ==
                send_queue_buffer_previous.?.ptr + send_queue_buffer_previous.?.len);

            allocator.free(bus.replicas_connect_attempts);
            allocator.free(bus.replicas_addresses);
            allocator.free(bus.replicas);
            allocator.free(bus.connections);
            allocator.free(bus.send_queue_buffer);
            bus.* = undefined;
        }

        fn init_tcp(io: *IO, process: vsr.ProcessType, family: u32) !IO.socket_t {
            return try io.open_socket_tcp(family, .{
                .rcvbuf = constants.tcp_rcvbuf,
                .sndbuf = switch (process) {
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

        pub fn listen(bus: *MessageBus) !void {
            assert(bus.process == .replica);
            assert(bus.accept_fd == null);
            assert(bus.accept_address == null);

            const address = bus.replicas_addresses[bus.process.replica];
            const fd = try init_tcp(bus.io, .replica, address.any.family);
            errdefer bus.io.close_socket(fd);

            const accept_address = try bus.io.listen(fd, address, .{
                .backlog = constants.tcp_backlog,
            });

            bus.accept_fd = fd;
            bus.accept_address = accept_address;
        }

        pub fn tick(bus: *MessageBus) void {
            assert(bus.process == .replica);
            bus.tick_connect();
            bus.tick_accept(); // Only replicas accept connections from other replicas and clients.
        }

        // The same as tick, but asserts a client and avoids accept, allowing Zig's lazy semantics
        // to not add dead accept code to client libraries.
        pub fn tick_client(bus: *MessageBus) void {
            assert(bus.process == .client);
            bus.tick_connect();
        }

        fn tick_connect(bus: *MessageBus) void {
            const replica_next = switch (bus.process) {
                // Each replica is responsible for connecting to replicas that come
                // after it in the configuration. This ensures that replicas never try
                // to connect to each other at the same time.
                .replica => |replica| replica + 1,
                // The client connects to all replicas.
                .client => 0,
            };
            for (bus.replicas[replica_next..], replica_next..) |*connection, replica| {
                if (connection.* == null) bus.connect(@intCast(replica));
            }
            assert(bus.connections_used >= bus.replicas.len - replica_next);
        }

        fn tick_accept(bus: *MessageBus) void {
            assert(bus.process == .replica);
            assert(bus.accept_fd != null); // Must listen before tick.
            bus.accept();
        }

        fn accept(bus: *MessageBus) void {
            assert(bus.process == .replica);
            assert(bus.accept_fd != null);

            if (bus.accept_connection != null) return;
            // All connections are currently in use, do nothing.
            if (bus.connections_used == bus.connections.len) return;
            assert(bus.connections_used < bus.connections.len);
            bus.accept_connection = for (bus.connections) |*connection| {
                if (connection.state == .free) {
                    connection.state = .accepting;
                    break connection;
                }
            } else unreachable;
            bus.io.accept(
                *MessageBus,
                bus,
                accept_callback,
                &bus.accept_completion,
                bus.accept_fd.?,
            );
        }

        fn accept_callback(
            bus: *MessageBus,
            _: *IO.Completion,
            result: IO.AcceptError!IO.socket_t,
        ) void {
            assert(bus.process == .replica);

            assert(bus.accept_connection != null);
            const connection: *Connection = bus.accept_connection.?;
            bus.accept_connection = null;

            assert(connection.peer == .unknown);
            assert(connection.fd == null);
            assert(connection.state == .accepting);
            defer assert(connection.state == .connected or connection.state == .free);

            if (result) |fd| {
                connection.state = .connected;
                connection.fd = fd;
                bus.connections_used += 1;

                bus.assert_connection_initial_state(connection);
                assert(connection.recv_buffer == null);
                connection.recv_buffer = MessageBuffer.init(bus.pool);
                bus.recv(connection);
                // Don't start send loop yet --- on accept, we don't know which peer this is.
                assert(connection.send_queue.empty());
                assert(connection.state == .connected);
            } else |err| {
                connection.state = .free;
                // TODO: some errors should probably be fatal
                log.warn("{}: on_accept: {}", .{ bus.id, err });
            }
        }

        fn connect(bus: *MessageBus, replica: u8) void {
            assert(bus.replicas[replica] == null);

            // Obtain a connection struct for our new replica connection.
            // If there is a free connection, use that. Otherwise drop
            // a client or unknown connection to make space. Prefer dropping
            // a client connection to an unknown one as the unknown peer may
            // be a replica. Since shutting a connection down does not happen
            // instantly, simply return after starting the shutdown and try again
            // on the next tick().
            const connection_free: *Connection = for (bus.connections) |*connection| {
                if (connection.state == .free) break connection;
            } else {
                bus.connect_reclaim_connection();
                return;
            };

            assert(connection_free.state == .free);
            // This will immediately add the connection to bus.replicas,
            // or else will return early if a socket file descriptor cannot be obtained:
            bus.connect_connection(connection_free, replica);
            switch (connection_free.state) {
                .connecting => assert(bus.replicas[replica] != null),
                .free => assert(bus.replicas[replica] == null),
                else => unreachable,
            }
        }

        fn connect_reclaim_connection(bus: *MessageBus) void {
            for (bus.connections) |*connection| assert(connection.state != .free);

            // If there is already a connection being shut down, no need to kill another.
            for (bus.connections) |*connection| {
                if (connection.state == .terminating) return;
            }

            log.info("{}: connect_to_replica: no free connection, disconnecting a client", .{
                bus.id,
            });
            for (bus.connections) |*connection| {
                if (connection.peer == .client) {
                    bus.terminate(connection, .shutdown);
                    return;
                }
            }

            log.info("{}: connect_to_replica: no free connection, disconnecting unknown peer", .{
                bus.id,
            });
            for (bus.connections) |*connection| {
                if (connection.peer == .unknown) {
                    bus.terminate(connection, .shutdown);
                    return;
                }
            }

            // We assert that the max number of connections is greater
            // than the number of replicas in init().
            unreachable;
        }

        /// Attempt to connect to a replica.
        /// The slot in the Message.replicas slices is immediately reserved.
        /// Failure is silent and returns the connection to an unused state.
        fn connect_connection(bus: *MessageBus, connection: *Connection, replica: u8) void {
            if (bus.process == .replica) assert(replica > bus.process.replica);

            assert(connection.state == .free);
            assert(connection.fd == null);

            const family = bus.replicas_addresses[replica].any.family;
            connection.fd = init_tcp(bus.io, bus.process, family) catch |err| {
                log.err("{}: connect_to_replica: init_tcp error={s}", .{
                    bus.id,
                    @errorName(err),
                });
                return;
            };
            connection.peer = .{ .replica = replica };
            connection.state = .connecting;
            bus.connections_used += 1;

            assert(bus.replicas[replica] == null);
            bus.replicas[replica] = connection;

            const attempts = &bus.replicas_connect_attempts[replica];
            const ms = vsr.exponential_backoff_with_jitter(
                &bus.prng,
                constants.connection_delay_min_ms,
                constants.connection_delay_max_ms,
                attempts.*,
            );
            attempts.* += 1;

            log.debug("{}: connect_to_replica: connecting to={} after={}ms", .{
                bus.id,
                connection.peer.replica,
                ms,
            });

            assert(!connection.recv_submitted);
            connection.recv_submitted = true;

            bus.io.timeout(
                *MessageBus,
                bus,
                connect_timeout_callback,
                // We use `recv_completion` for the connection `timeout()` and `connect()` calls
                &connection.recv_completion,
                @as(u63, @intCast(ms * std.time.ns_per_ms)),
            );
        }

        fn connect_timeout_callback(
            bus: *MessageBus,
            completion: *IO.Completion,
            result: IO.TimeoutError!void,
        ) void {
            const connection: *Connection = @alignCast(
                @fieldParentPtr("recv_completion", completion),
            );
            assert(connection.recv_submitted);
            connection.recv_submitted = false;
            if (connection.state == .terminating) {
                bus.terminate_join(connection);
                return;
            }
            assert(connection.state == .connecting);
            result catch unreachable;

            log.debug("{}: on_connect_with_exponential_backoff: to={}", .{
                bus.id,
                connection.peer.replica,
            });

            assert(!connection.recv_submitted);
            connection.recv_submitted = true;

            bus.io.connect(
                *MessageBus,
                bus,
                connect_callback,
                // We use `recv_completion` for the connection `timeout()` and `connect()` calls
                &connection.recv_completion,
                connection.fd.?,
                bus.replicas_addresses[connection.peer.replica],
            );
        }

        fn connect_callback(
            bus: *MessageBus,
            completion: *IO.Completion,
            result: IO.ConnectError!void,
        ) void {
            const connection: *Connection = @alignCast(
                @fieldParentPtr("recv_completion", completion),
            );
            assert(connection.recv_submitted);
            connection.recv_submitted = false;

            if (connection.state == .terminating) {
                bus.terminate_join(connection);
                return;
            }
            assert(connection.state == .connecting);
            connection.state = .connected;

            result catch |err| {
                log.warn("{}: on_connect: error to={} {}", .{
                    bus.id,
                    connection.peer.replica,
                    err,
                });
                bus.terminate(connection, .no_shutdown);
                return;
            };

            log.info("{}: on_connect: connected to={}", .{ bus.id, connection.peer.replica });
            bus.replicas_connect_attempts[connection.peer.replica] = 0;

            bus.assert_connection_initial_state(connection);
            assert(connection.recv_buffer == null);
            connection.recv_buffer = MessageBuffer.init(bus.pool);
            bus.recv(connection);
            bus.send(connection);
            assert(connection.state == .connected);
        }

        fn assert_connection_initial_state(bus: *MessageBus, connection: *Connection) void {
            assert(bus.connections_used > 0);

            assert(connection.peer == .unknown or connection.peer == .replica);
            assert(connection.state == .connected);
            assert(connection.fd != null);

            assert(connection.recv_submitted == false);
            assert(connection.recv_buffer == null);

            assert(connection.send_submitted == false);
            assert(connection.send_progress == 0);
        }

        /// The recv loop.
        ///
        /// Kickstarted by `accept` and `connect`, and loops onto itself via `recv_buffer_drain`.
        fn recv(bus: *MessageBus, connection: *Connection) void {
            assert(connection.state == .connected);
            assert(connection.fd != null);
            assert(connection.recv_buffer != null);

            assert(!connection.recv_submitted);
            connection.recv_submitted = true;

            bus.io.recv(
                *MessageBus,
                bus,
                recv_callback,
                &connection.recv_completion,
                connection.fd.?,
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
            if (connection.state == .terminating) {
                bus.terminate_join(connection);
                return;
            }
            assert(connection.state == .connected);
            const bytes_received = result catch |err| {
                // TODO: maybe don't need to close on *every* error
                log.warn("{}: on_recv: from={} {}", .{ bus.id, connection.peer, err });
                bus.terminate(connection, .shutdown);
                return;
            };
            // No bytes received means that the peer closed its side of the connection.
            if (bytes_received == 0) {
                log.info("{}: on_recv: from={} orderly shutdown", .{ bus.id, connection.peer });
                bus.terminate(connection, .no_shutdown);
                return;
            }
            assert(bytes_received <= constants.message_size_max);
            assert(connection.recv_buffer != null);
            connection.recv_buffer.?.recv_advance(@intCast(bytes_received));

            switch (bus.process) {
                // Replicas may forward messages from clients or from other replicas so we
                // may receive messages from a peer before we know who they are:
                // This has the same effect as an asymmetric network where, for a short time
                // bounded by the time it takes to ping, we can hear from a peer before we
                // can send back to them.
                .replica => {
                    while (connection.recv_buffer.?.next_header()) |header| {
                        if (bus.recv_update_peer(connection, header.peer_type())) {
                            connection.recv_buffer.?.suspend_message(&header);
                        } else {
                            log.warn("{}: on_recv: invalid peer transition {any} -> {any}", .{
                                bus.id,
                                connection.peer,
                                header.peer_type(),
                            });
                            connection.recv_buffer.?.invalidate(.misdirected);
                        }
                    }
                },
                // The client connects only to replicas and should set peer when connecting:
                .client => assert(connection.peer == .replica),
            }
            bus.recv_buffer_drain(connection);
        }

        fn recv_update_peer(bus: *MessageBus, connection: *Connection, peer: vsr.Peer) bool {
            assert(bus.process == .replica);
            assert(bus.clients.capacity() > 0);

            assert(bus.connections_used > 0);

            assert(connection.state == .connected);
            assert(connection.fd != null);
            assert(connection.recv_buffer != null);

            switch (vsr.Peer.transition(connection.peer, peer)) {
                .retain => return true,
                .reject => return false,
                .update => {},
            }

            switch (peer) {
                .replica => |replica_index| {
                    if (replica_index >= bus.replicas_addresses.len) return false;

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
                        if (old.state != .terminating) bus.terminate(old, .shutdown);
                    }

                    switch (connection.peer) {
                        .unknown => {},
                        // If this connection was misclassified to a client due to a forwarded
                        // request message (see `peer_type` in message_header.zig), it may
                        // reside in the clients map. If so, it must be popped and mapped to the
                        // correct replica.
                        .client_likely => |existing| {
                            if (bus.clients.get(existing)) |existing_connection| {
                                if (existing_connection == connection) {
                                    assert(bus.clients.remove(existing));
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
                    const result = bus.clients.getOrPutAssumeCapacity(client_id);
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
                            bus.terminate(old, .shutdown);
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
                                bus.clients.getOrPutAssumeCapacity(client_id);
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

            connection.peer = peer;

            return true;
        }

        /// Attempt moving messages from recv buffer into replcia for processing. Called when recv
        /// syscall completes, or when a replica signals readiness to consume previously suspended
        /// messages.
        fn recv_buffer_drain(bus: *MessageBus, connection: *Connection) void {
            assert(connection.recv_buffer != null);

            if (connection.recv_buffer.?.has_message()) {
                bus.on_messages_callback(bus, &connection.recv_buffer.?);
            }

            if (connection.recv_buffer.?.invalid) |reason| {
                log.warn("{}: on_recv: from={} terminating connection: invalid {s}", .{
                    bus.id,
                    connection.peer,
                    @tagName(reason),
                });
                bus.terminate(connection, .no_shutdown);
                return;
            }

            if (connection.recv_buffer.?.has_message()) {
                maybe(connection.state == .terminating);
                bus.connections_suspended.push(connection);
            } else {
                if (connection.state == .terminating) {
                    bus.terminate_join(connection);
                } else {
                    bus.recv(connection);
                }
            }
        }

        pub fn send_message_to_replica(bus: *MessageBus, replica: u8, message: *Message) void {
            // Messages sent by a replica to itself should never be passed to the message bus.
            if (bus.process == .replica) assert(replica != bus.process.replica);

            if (bus.replicas[replica]) |connection| {
                bus.send_message(connection, message);
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
            assert(bus.process == .replica);
            assert(bus.clients.capacity() > 0);

            if (bus.clients.get(client_id)) |connection| {
                bus.send_message(connection, message);
            } else {
                log.debug(
                    "{}: send_message_to_client: no connection to={}",
                    .{ bus.id, client_id },
                );
            }
        }

        /// Add a message to the connection's send queue, starting a send operation
        /// if the queue was previously empty.
        fn send_message(bus: *MessageBus, connection: *Connection, message: *Message) void {
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
            if (!connection.send_submitted) bus.send(connection);
        }

        /// Send loop.
        ///
        /// Kickstarted by `connect` and loops onto itself until all enqueue messages are sent.
        /// `accept` doesn't start the send loop because it doesn't know the identity of the peer.
        fn send(bus: *MessageBus, connection: *Connection) void {
            assert(connection.peer != .unknown);
            assert(connection.state == .connected);
            assert(connection.fd != null);
            assert(!connection.send_submitted);

            bus.send_now(connection);

            const message = connection.send_queue.head() orelse
                return; // Nothing more to send, break out of the send loop.
            connection.send_submitted = true;
            bus.io.send(
                *MessageBus,
                bus,
                send_callback,
                &connection.send_completion,
                connection.fd.?,
                message.buffer[connection.send_progress..message.header.size],
            );
        }

        // Optimization/fast path: try to immediately copy the send queue over to the in-kernel
        // send buffer, falling back to asynchronous send if that's not possible.
        fn send_now(bus: *MessageBus, connection: *Connection) void {
            assert(connection.state == .connected);
            assert(connection.fd != null);
            assert(!connection.send_submitted);

            for (0..connection.send_queue.count) |_| {
                const message = connection.send_queue.head().?;
                assert(connection.send_progress < message.header.size);
                const write_size = bus.io.send_now(
                    connection.fd.?,
                    message.buffer[connection.send_progress..message.header.size],
                ) orelse return;
                assert(write_size <= constants.message_size_max);
                connection.send_progress += @intCast(write_size);
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

        fn send_callback(
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
                bus.terminate_join(connection);
                return;
            }
            assert(connection.state == .connected);
            const write_size = result catch |err| {
                // TODO: maybe don't need to close on *every* error
                log.warn("{}: on_send: to={} {}", .{
                    bus.id,
                    connection.peer,
                    err,
                });
                bus.terminate(connection, .shutdown);
                return;
            };
            assert(write_size <= constants.message_size_max);
            connection.send_progress += @intCast(write_size);
            assert(connection.send_progress <= connection.send_queue.head().?.header.size);
            // If the message has been fully sent, move on to the next one.
            if (connection.send_progress == connection.send_queue.head().?.header.size) {
                connection.send_progress = 0;
                const message = connection.send_queue.pop().?;
                bus.unref(message);
            }
            bus.send(connection);
        }

        /// Clean up an active connection and reset it to its initial, unused, state.
        /// This reset does not happen instantly as currently in progress operations
        /// must first be stopped. The `how` arg allows the caller to specify if a
        /// shutdown syscall should be made or not before proceeding to wait for
        /// currently in progress operations to complete and close the socket.
        /// I'll be back! (when the Connection is reused after being fully closed)
        fn terminate(
            bus: *MessageBus,
            connection: *Connection,
            how: enum { shutdown, no_shutdown },
        ) void {
            assert(connection.state != .free);
            assert(connection.fd != null);
            switch (how) {
                .shutdown => {
                    // The shutdown syscall will cause currently in progress send/recv
                    // operations to be gracefully closed while keeping the fd open.
                    //
                    // TODO: Investigate differences between shutdown() on Linux vs Darwin.
                    // Especially how this interacts with our assumptions around pending I/O.
                    bus.io.shutdown(connection.fd.?, .both) catch |err| switch (err) {
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
                .no_shutdown => {},
            }
            assert(connection.state != .terminating);
            connection.state = .terminating;
            bus.terminate_join(connection);
        }

        fn terminate_join(bus: *MessageBus, connection: *Connection) void {
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

            bus.terminate_close(connection);
        }

        fn terminate_close(bus: *MessageBus, connection: *Connection) void {
            assert(connection.state == .terminating);
            assert(!connection.recv_submitted);
            assert(!connection.send_submitted);
            if (connection.recv_buffer) |receive_buffer| assert(!receive_buffer.has_message());
            assert(connection.fd != null);

            connection.send_submitted = true;
            connection.recv_submitted = true;
            // We can free resources now that there is no longer any I/O in progress.
            while (connection.send_queue.pop()) |message| {
                bus.unref(message);
            }
            if (connection.recv_buffer) |*buffer| buffer.deinit(bus.pool);
            connection.recv_buffer = null;
            const fd = connection.fd.?;
            connection.fd = null;
            // It's OK to use the send completion here as we know that no send
            // operation is currently in progress.
            bus.io.close(
                *MessageBus,
                bus,
                terminate_close_callback,
                &connection.send_completion,
                fd,
            );
        }

        fn terminate_close_callback(
            bus: *MessageBus,
            completion: *IO.Completion,
            result: IO.CloseError!void,
        ) void {
            const connection: *Connection = @alignCast(
                @fieldParentPtr("send_completion", completion),
            );
            assert(connection.state == .terminating);
            assert(connection.recv_submitted);
            assert(connection.send_submitted);
            assert(connection.recv_buffer == null);
            assert(connection.send_queue.empty());
            assert(connection.fd == null);

            result catch |err| {
                log.warn("{}: on_close: to={} {}", .{ bus.id, connection.peer, err });
            };

            // Reset the connection to its initial state.
            switch (connection.peer) {
                .unknown => {},
                .client, .client_likely => |client_id| {
                    assert(bus.process == .replica);
                    // A newer client connection may have replaced this one:
                    if (bus.clients.get(client_id)) |existing_connection| {
                        if (existing_connection == connection) {
                            assert(bus.clients.remove(client_id));
                        }
                    } else {
                        // A newer client connection may even leapfrog this connection
                        // and then be terminated and set to null before we can get
                        // here.
                    }
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
            connection.* = .{
                .send_queue = .{
                    .buffer = connection.send_queue.buffer,
                },
            };
        }

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
                bus.recv_buffer_drain(connection);
            }
        }

        /// Used to send/receive messages to/from a client or fellow replica.
        const Connection = struct {
            /// The peer is determined by inspecting all message headers received on this
            /// connection. If the peer changes unexpectedly (for example, due to a misdirected
            /// message), we terminate the connection.
            peer: vsr.Peer = .unknown,

            state: enum {
                /// The connection is not in use, with peer set to `.none`.
                free,
                /// The connection has been reserved for an in progress accept operation,
                /// with peer set to `.none`.
                accepting,
                /// The peer is a replica and a connect operation has been started
                /// but not yet completed.
                connecting,
                /// The peer is fully connected and may be a client, replica, or unknown.
                connected,
                /// The connection is being terminated but cleanup has not yet finished.
                terminating,
            } = .free,
            /// This is guaranteed to be valid only while state is connected.
            /// It will be reset to null during the shutdown process and is always null if the
            /// connection is unused (i.e. peer == .none).
            fd: ?IO.socket_t = null,

            /// This completion is used for all recv operations.
            /// It is also used for the initial connect when establishing a replica connection.
            recv_completion: IO.Completion = undefined,
            /// True exactly when the recv_completion has been submitted to the IO abstraction
            /// but the callback has not yet been run.
            recv_submitted: bool = false,
            recv_buffer: ?MessageBuffer = null,

            /// This completion is used for all send operations.
            send_completion: IO.Completion = undefined,
            /// True exactly when the send_completion has been submitted to the IO abstraction
            /// but the callback has not yet been run.
            send_submitted: bool = false,
            /// Number of bytes of the current message that have already been sent.
            send_progress: u32 = 0,
            /// The queue of messages to send to the client or replica peer.
            send_queue: SendQueue,
            /// For connections_suspended.
            link: QueueType(Connection).Link = .{},
        };
    };
}
