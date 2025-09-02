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
    const SendQueue = RingBufferType(*Message, .{
        .array = switch (process_type) {
            .replica => constants.connection_send_queue_max_replica,
            // A client has at most 1 in-flight request, plus pings.
            .client => constants.connection_send_queue_max_client,
        },
    });

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
                /// The connection reserved for the currently in progress accept operation.
                /// This is non-null exactly when an accept operation is submitted.
                accept_connection: ?*Connection = null,
                /// Map from client id to the currently active connection for that client.
                /// This is used to make lookup of client connections when sending messages
                /// efficient and to ensure old client connections are dropped if a new one
                /// is established.
                clients: std.AutoHashMapUnmanaged(u128, *Connection) = .{},
            },
            .client => void,
        },
        /// Prefix for log messages.
        id: u128,

        /// The callback to be called when a message is received.
        on_messages_callback: *const fn (message_bus: *MessageBus, buffer: *MessageBuffer) void,

        /// This slice is allocated with a fixed size in the init function and never reallocated.
        connections: []Connection,
        /// Number of connections currently in use (i.e. connection.peer != .none).
        connections_used: usize = 0,
        connections_suspended: QueueType(Connection) = QueueType(Connection).init(.{
            .name = null,
        }),
        resume_receive_completion: IO.Completion = undefined,
        resume_receive_submitted: bool = false,

        /// Map from replica index to the currently active connection for that replica, if any.
        /// The connection for the process replica if any will always be null.
        replicas: []?*Connection,
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

            const connections_max: u32 = switch (process_type) {
                // The maximum number of connections that can be held open by the server at any
                // time. -1 since we don't need a connection to ourself.
                .replica => @intCast(options.configuration.len - 1 + options.clients_limit.?),
                .client => @intCast(options.configuration.len),
            };

            const connections = try allocator.alloc(Connection, connections_max);
            errdefer allocator.free(connections);
            @memset(connections, .{});

            const replicas = try allocator.alloc(?*Connection, options.configuration.len);
            errdefer allocator.free(replicas);
            @memset(replicas, null);

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

            var bus: MessageBus = .{
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
                .replicas = replicas,
                .replicas_connect_attempts = replicas_connect_attempts,
                .prng = stdx.PRNG.from_seed(prng_seed),
            };

            // Pre-allocate enough memory to hold all possible connections in the client map.
            if (process_type == .replica) {
                try bus.process.clients.ensureTotalCapacity(allocator, connections_max);
            }

            return bus;
        }

        pub fn deinit(bus: *MessageBus, allocator: std.mem.Allocator) void {
            if (process_type == .replica) {
                bus.process.clients.deinit(allocator);
                bus.io.close_socket(bus.process.accept_fd);
            }

            for (bus.connections) |*connection| {
                if (connection.fd != IO.INVALID_SOCKET) {
                    bus.io.close_socket(connection.fd);
                }

                if (connection.recv_buffer) |*buffer| buffer.deinit(bus.pool);
                connection.recv_buffer = null;
                while (connection.send_queue.pop()) |message| bus.unref(message);
            }
            allocator.free(bus.connections);
            allocator.free(bus.replicas);
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
                    for (bus.replicas[replica_next..], replica_next..) |connection, replica| {
                        if (connection == null) bus.connect_to_replica(@intCast(replica));
                    }
                    assert(bus.connections_used >= bus.replicas.len - replica_next);

                    // Only replicas accept connections from other replicas and clients:
                    bus.maybe_accept();
                },
                .client => {
                    // The client connects to all replicas.
                    for (bus.replicas, 0..) |connection, replica| {
                        if (connection == null) bus.connect_to_replica(@intCast(replica));
                    }
                    assert(bus.connections_used >= bus.replicas.len);
                },
            }
        }

        fn connect_to_replica(bus: *MessageBus, replica: u8) void {
            assert(bus.replicas[replica] == null);

            // Obtain a connection struct for our new replica connection.
            // If there is a free connection, use that. Otherwise drop
            // a client or unknown connection to make space. Prefer dropping
            // a client connection to an unknown one as the unknown peer may
            // be a replica. Since shutting a connection down does not happen
            // instantly, simply return after starting the shutdown and try again
            // on the next tick().
            for (bus.connections) |*connection| {
                if (connection.state == .free) {
                    assert(connection.peer == .none);
                    // This will immediately add the connection to bus.replicas,
                    // or else will return early if a socket file descriptor cannot be obtained:
                    // TODO See if we can clean this up to remove/expose the early return branch.
                    connection.connect_to_replica(bus, replica);
                    return;
                }
            }

            // If there is already a connection being shut down, no need to kill another.
            for (bus.connections) |*connection| {
                if (connection.state == .terminating) return;
            }

            log.info("{}: connect_to_replica: no free connection, disconnecting a client", .{
                bus.id,
            });
            for (bus.connections) |*connection| {
                if (connection.peer == .client) {
                    connection.terminate(bus, .shutdown);
                    return;
                }
            }

            log.info("{}: connect_to_replica: no free connection, disconnecting unknown peer", .{
                bus.id,
            });
            for (bus.connections) |*connection| {
                if (connection.peer == .unknown) {
                    connection.terminate(bus, .shutdown);
                    return;
                }
            }

            // We assert that the max number of connections is greater
            // than the number of replicas in init().
            unreachable;
        }

        fn maybe_accept(bus: *MessageBus) void {
            comptime assert(process_type == .replica);

            if (bus.process.accept_connection != null) return;
            // All connections are currently in use, do nothing.
            if (bus.connections_used == bus.connections.len) return;
            assert(bus.connections_used < bus.connections.len);
            bus.process.accept_connection = for (bus.connections) |*connection| {
                if (connection.state == .free) {
                    assert(connection.peer == .none);
                    connection.state = .accepting;
                    break connection;
                }
            } else unreachable;
            bus.io.accept(
                *MessageBus,
                bus,
                on_accept,
                &bus.process.accept_completion,
                bus.process.accept_fd,
            );
        }

        fn on_accept(
            bus: *MessageBus,
            completion: *IO.Completion,
            result: IO.AcceptError!IO.socket_t,
        ) void {
            _ = completion;

            comptime assert(process_type == .replica);
            assert(bus.process.accept_connection != null);
            defer bus.process.accept_connection = null;
            const fd = result catch |err| {
                bus.process.accept_connection.?.state = .free;
                // TODO: some errors should probably be fatal
                log.warn("{}: on_accept: {}", .{ bus.id, err });
                return;
            };
            bus.process.accept_connection.?.on_accept(bus, fd);
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

        /// Used to send/receive messages to/from a client or fellow replica.
        const Connection = struct {
            const Peer = union(enum) {
                /// No peer is currently connected.
                none: void,
                /// A connection is established but an unambiguous header has not yet been received.
                unknown: void,
                /// The peer is a client with the given id.
                client: u128,
                /// The peer is a replica with the given id.
                replica: u8,
            };

            /// The peer is determined by inspecting the first message header
            /// received.
            peer: Peer = .none,
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
            /// It will be reset to IO.INVALID_SOCKET during the shutdown process and is always
            /// IO.INVALID_SOCKET if the connection is unused (i.e. peer == .none). We use
            /// IO.INVALID_SOCKET instead of undefined here for safety to ensure an error if the
            /// invalid value is ever used, instead of potentially performing an action on an
            /// active fd.
            fd: IO.socket_t = IO.INVALID_SOCKET,

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
            send_progress: usize = 0,
            /// The queue of messages to send to the client or replica peer.
            send_queue: SendQueue = SendQueue.init(),
            /// For connections_suspended.
            link: QueueType(Connection).Link = .{},

            /// Attempt to connect to a replica.
            /// The slot in the Message.replicas slices is immediately reserved.
            /// Failure is silent and returns the connection to an unused state.
            pub fn connect_to_replica(connection: *Connection, bus: *MessageBus, replica: u8) void {
                if (process_type == .replica) assert(replica != bus.process.replica);

                assert(connection.peer == .none);
                assert(connection.state == .free);
                assert(connection.fd == IO.INVALID_SOCKET);

                const family = bus.configuration[replica].any.family;
                connection.fd = init_tcp(bus.io, family) catch return;
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
                    on_connect_with_exponential_backoff,
                    // We use `recv_completion` for the connection `timeout()` and `connect()` calls
                    &connection.recv_completion,
                    @as(u63, @intCast(ms * std.time.ns_per_ms)),
                );
            }

            fn on_connect_with_exponential_backoff(
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
                    connection.maybe_close(bus);
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
                    on_connect,
                    // We use `recv_completion` for the connection `timeout()` and `connect()` calls
                    &connection.recv_completion,
                    connection.fd,
                    bus.configuration[connection.peer.replica],
                );
            }

            fn on_connect(
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
                    connection.maybe_close(bus);
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
                    connection.terminate(bus, .close);
                    return;
                };

                log.info("{}: on_connect: connected to={}", .{ bus.id, connection.peer.replica });
                bus.replicas_connect_attempts[connection.peer.replica] = 0;

                connection.assert_recv_send_initial_state(bus);
                assert(connection.recv_buffer == null);
                connection.recv_buffer = MessageBuffer.init(bus.pool);
                connection.recv(bus);
                // A message may have been queued for sending while we were connecting:
                // TODO Should we relax recv() and send() to return if `state != .connected`?
                if (connection.state == .connected) connection.send(bus);
            }

            /// Given a newly accepted fd, start receiving messages on it.
            /// Callbacks will be continuously re-registered until terminate() is called.
            pub fn on_accept(connection: *Connection, bus: *MessageBus, fd: IO.socket_t) void {
                assert(connection.peer == .none);
                assert(connection.state == .accepting);
                assert(connection.fd == IO.INVALID_SOCKET);

                connection.peer = .unknown;
                connection.state = .connected;
                connection.fd = fd;
                bus.connections_used += 1;

                connection.assert_recv_send_initial_state(bus);
                assert(connection.send_queue.empty());

                assert(connection.recv_buffer == null);
                connection.recv_buffer = MessageBuffer.init(bus.pool);
                connection.recv(bus);
            }

            fn assert_recv_send_initial_state(connection: *Connection, bus: *MessageBus) void {
                assert(bus.connections_used > 0);

                assert(connection.peer == .unknown or connection.peer == .replica);
                assert(connection.state == .connected);
                assert(connection.fd != IO.INVALID_SOCKET);

                assert(connection.recv_submitted == false);
                assert(connection.recv_buffer == null);

                assert(connection.send_submitted == false);
                assert(connection.send_progress == 0);
            }

            /// Add a message to the connection's send queue, starting a send operation
            /// if the queue was previously empty.
            pub fn send_message(connection: *Connection, bus: *MessageBus, message: *Message) void {
                assert(connection.peer == .client or connection.peer == .replica);
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
                assert(connection.peer != .none);
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

            fn set_and_verify_peer(connection: *Connection, bus: *MessageBus) void {
                comptime assert(process_type == .replica);

                assert(bus.connections_used > 0);

                assert(connection.peer != .none);
                assert(connection.state == .connected);
                assert(connection.fd != IO.INVALID_SOCKET);
                assert(connection.recv_buffer != null);

                const header_peer: Connection.Peer = switch (connection.recv_buffer.?.peer) {
                    .unknown => return,
                    .replica => |replica| .{ .replica = replica },
                    .client, .client_likely => |client| .{ .client = client },
                };

                if (std.meta.eql(connection.peer, header_peer)) return;

                defer connection.peer = header_peer;

                switch (header_peer) {
                    .replica => |replica_index| {
                        assert(replica_index < bus.configuration.len);
                        // If there is a connection to this replica, terminate and replace it.
                        // Otherwise, this connection was misclassified to a client due to a
                        // forwarded request message (see `peer_type` in message_header.zig), map it
                        // to the correct replica. Allowed transitions:
                        // * unknown → replica
                        // * client  → replica
                        if (bus.replicas[replica_index]) |old| {
                            assert(old != connection);
                            assert(old.peer == .replica);
                            assert(old.peer.replica == replica_index);
                            assert(old.state != .free);
                            if (old.state != .terminating) old.terminate(bus, .shutdown);
                        }

                        switch (connection.peer) {
                            .unknown => {},
                            .client => |existing| assert(bus.process.clients.remove(existing)),
                            .replica => assert(connection.recv_buffer.?.invalid.? == .misdirected),
                            .none => unreachable,
                        }

                        bus.replicas[replica_index] = connection;
                        log.info("{}: set_and_verify_peer: connection from replica={}", .{
                            bus.id,
                            replica_index,
                        });
                    },
                    .client => |client_id| {
                        assert(client_id != 0);
                        const result = bus.process.clients.getOrPutAssumeCapacity(client_id);

                        // If there is a connection to this client, terminate and replace it.
                        // Allowed transitions:
                        // * unknown → client
                        if (result.found_existing) {
                            const old = result.value_ptr.*;

                            assert(old != connection);
                            assert(old.peer == .client);
                            assert(old.peer.client == client_id);
                            assert(old.state == .connected or old.state == .terminating);
                            if (old.state != .terminating) old.terminate(bus, .shutdown);
                        } else {
                            switch (connection.peer) {
                                .unknown => {},
                                .client, .replica => assert(
                                    connection.recv_buffer.?.invalid.? == .misdirected,
                                ),
                                .none => unreachable,
                            }
                        }

                        result.value_ptr.* = connection;
                        log.info("{}: set_and_verify_peer connection from client={}", .{
                            bus.id,
                            client_id,
                        });
                    },
                    .none, .unknown => unreachable,
                }
            }

            fn recv(connection: *Connection, bus: *MessageBus) void {
                assert(connection.peer != .none);
                assert(connection.state == .connected);
                assert(connection.fd != IO.INVALID_SOCKET);
                assert(connection.recv_buffer != null);

                assert(!connection.recv_submitted);
                connection.recv_submitted = true;

                bus.io.recv(
                    *MessageBus,
                    bus,
                    on_recv,
                    &connection.recv_completion,
                    connection.fd,
                    connection.recv_buffer.?.recv_slice(),
                );
            }

            fn on_recv(
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
                    connection.maybe_close(bus);
                    return;
                }
                assert(connection.state == .connected);
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
                connection.recv_buffer.?.recv_advance(@intCast(bytes_received));

                switch (process_type) {
                    // Replicas may forward messages from clients or from other replicas so we
                    // may receive messages from a peer before we know who they are:
                    // This has the same effect as an asymmetric network where, for a short time
                    // bounded by the time it takes to ping, we can hear from a peer before we
                    // can send back to them.
                    .replica => connection.set_and_verify_peer(bus),
                    // The client connects only to replicas and should set peer when connecting:
                    .client => assert(connection.peer == .replica),
                }
                connection.call_on_messages(bus);
            }

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
                assert(connection.peer == .client or connection.peer == .replica);
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
                assert(connection.peer == .client or connection.peer == .replica);
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
                assert(connection.peer != .none);
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

                assert(connection.peer != .none);
                assert(connection.state == .terminating);

                // Reset the connection to its initial state.
                defer {
                    assert(connection.recv_buffer == null);
                    assert(connection.send_queue.empty());

                    switch (connection.peer) {
                        .none => unreachable,
                        .unknown => {},
                        .client => switch (process_type) {
                            .replica => {
                                // A newer client connection may have replaced this one:
                                if (bus.process.clients.get(
                                    connection.peer.client,
                                )) |existing_connection| {
                                    if (existing_connection == connection) {
                                        assert(bus.process.clients.remove(connection.peer.client));
                                    }
                                } else {
                                    // A newer client connection may even leapfrog this connection
                                    // and then be terminated and set to null before we can get
                                    // here.
                                }
                            },
                            .client => unreachable,
                        },
                        .replica => {
                            // A newer replica connection may have replaced this one:
                            if (bus.replicas[connection.peer.replica] == connection) {
                                bus.replicas[connection.peer.replica] = null;
                            } else {
                                // A newer replica connection may even leapfrog this connection and
                                // then be terminated and set to null before we can get here:
                                stdx.maybe(bus.replicas[connection.peer.replica] == null);
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
    };
}
