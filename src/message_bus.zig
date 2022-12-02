const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const mem = std.mem;
const os = std.os;

const is_linux = builtin.target.os.tag == .linux;

const constants = @import("constants.zig");
const log = std.log.scoped(.message_bus);

const vsr = @import("vsr.zig");
const Header = vsr.Header;

const util = @import("util.zig");
const RingBuffer = @import("ring_buffer.zig").RingBuffer;
const IO = @import("io.zig").IO;
const MessagePool = @import("message_pool.zig").MessagePool;
const Message = MessagePool.Message;

pub const MessageBusReplica = MessageBusType(.replica);
pub const MessageBusClient = MessageBusType(.client);

fn MessageBusType(comptime process_type: vsr.ProcessType) type {
    const SendQueue = RingBuffer(*Message, switch (process_type) {
        .replica => constants.connection_send_queue_max_replica,
        // A client has at most 1 in-flight request, plus pings.
        .client => constants.connection_send_queue_max_client,
    }, .array);

    const tcp_sndbuf = switch (process_type) {
        .replica => constants.tcp_sndbuf_replica,
        .client => constants.tcp_sndbuf_client,
    };

    const Process = union(vsr.ProcessType) {
        replica: u8,
        client: u128,
    };

    return struct {
        const Self = @This();

        pool: *MessagePool,
        io: *IO,

        cluster: u32,
        configuration: []const std.net.Address,

        process: switch (process_type) {
            .replica => struct {
                replica: u8,
                /// The file descriptor for the process on which to accept connections.
                accept_fd: os.socket_t,
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

        /// The callback to be called when a message is received.
        on_message_callback: fn (message_bus: *Self, message: *Message) void,

        /// This slice is allocated with a fixed size in the init function and never reallocated.
        connections: []Connection,
        /// Number of connections currently in use (i.e. connection.peer != .none).
        connections_used: usize = 0,

        /// Map from replica index to the currently active connection for that replica, if any.
        /// The connection for the process replica if any will always be null.
        replicas: []?*Connection,
        /// The number of outgoing `connect()` attempts for a given replica:
        /// Reset to zero after a successful `on_connect()`.
        replicas_connect_attempts: []u64,

        /// Used to apply jitter when calculating exponential backoff:
        /// Seeded with the process' replica index or client ID.
        prng: std.rand.DefaultPrng,

        pub const Options = struct {
            configuration: []const std.net.Address,
            io: *IO,
        };

        /// Initialize the MessageBus for the given cluster, configuration and replica/client process.
        pub fn init(
            allocator: mem.Allocator,
            cluster: u32,
            process: Process,
            message_pool: *MessagePool,
            on_message_callback: fn (message_bus: *Self, message: *Message) void,
            options: Options,
        ) !Self {
            // There must be enough connections for all replicas and at least one client.
            assert(constants.connections_max > options.configuration.len);
            assert(@as(vsr.ProcessType, process) == process_type);

            const connections = try allocator.alloc(Connection, constants.connections_max);
            errdefer allocator.free(connections);
            mem.set(Connection, connections, .{});

            const replicas = try allocator.alloc(?*Connection, options.configuration.len);
            errdefer allocator.free(replicas);
            mem.set(?*Connection, replicas, null);

            const replicas_connect_attempts = try allocator.alloc(u64, options.configuration.len);
            errdefer allocator.free(replicas_connect_attempts);
            mem.set(u64, replicas_connect_attempts, 0);

            const prng_seed = switch (process_type) {
                .replica => process.replica,
                .client => @truncate(u64, process.client),
            };

            var bus: Self = .{
                .pool = message_pool,
                .io = options.io,
                .cluster = cluster,
                .configuration = options.configuration,
                .process = switch (process_type) {
                    .replica => .{
                        .replica = process.replica,
                        .accept_fd = try init_tcp(options.io, options.configuration[process.replica]),
                    },
                    .client => {},
                },
                .on_message_callback = on_message_callback,
                .connections = connections,
                .replicas = replicas,
                .replicas_connect_attempts = replicas_connect_attempts,
                .prng = std.rand.DefaultPrng.init(prng_seed),
            };

            // Pre-allocate enough memory to hold all possible connections in the client map.
            if (process_type == .replica) {
                try bus.process.clients.ensureTotalCapacity(allocator, constants.connections_max);
            }

            return bus;
        }

        pub fn deinit(bus: *Self, allocator: std.mem.Allocator) void {
            if (process_type == .replica) {
                bus.process.clients.deinit(allocator);
            }

            for (bus.connections) |*connection| {
                if (connection.recv_message) |message| bus.unref(message);
                while (connection.send_queue.pop()) |message| bus.unref(message);
            }
            allocator.free(bus.connections);
            allocator.free(bus.replicas);
            allocator.free(bus.replicas_connect_attempts);
        }

        fn init_tcp(io: *IO, address: std.net.Address) !os.socket_t {
            const fd = try io.open_socket(
                address.any.family,
                os.SOCK.STREAM,
                os.IPPROTO.TCP,
            );
            errdefer os.closeSocket(fd);

            const set = struct {
                fn set(_fd: os.socket_t, level: u32, option: u32, value: c_int) !void {
                    try os.setsockopt(_fd, level, option, &mem.toBytes(value));
                }
            }.set;

            if (constants.tcp_rcvbuf > 0) rcvbuf: {
                if (is_linux) {
                    // Requires CAP_NET_ADMIN privilege (settle for SO_RCVBUF in case of an EPERM):
                    if (set(fd, os.SOL.SOCKET, os.SO.RCVBUFFORCE, constants.tcp_rcvbuf)) |_| {
                        break :rcvbuf;
                    } else |err| switch (err) {
                        error.PermissionDenied => {},
                        else => |e| return e,
                    }
                }
                try set(fd, os.SOL.SOCKET, os.SO.RCVBUF, constants.tcp_rcvbuf);
            }

            if (tcp_sndbuf > 0) sndbuf: {
                if (is_linux) {
                    // Requires CAP_NET_ADMIN privilege (settle for SO_SNDBUF in case of an EPERM):
                    if (set(fd, os.SOL.SOCKET, os.SO.SNDBUFFORCE, tcp_sndbuf)) |_| {
                        break :sndbuf;
                    } else |err| switch (err) {
                        error.PermissionDenied => {},
                        else => |e| return e,
                    }
                }
                try set(fd, os.SOL.SOCKET, os.SO.SNDBUF, tcp_sndbuf);
            }

            if (constants.tcp_keepalive) {
                try set(fd, os.SOL.SOCKET, os.SO.KEEPALIVE, 1);
                if (is_linux) {
                    try set(fd, os.IPPROTO.TCP, os.TCP.KEEPIDLE, constants.tcp_keepidle);
                    try set(fd, os.IPPROTO.TCP, os.TCP.KEEPINTVL, constants.tcp_keepintvl);
                    try set(fd, os.IPPROTO.TCP, os.TCP.KEEPCNT, constants.tcp_keepcnt);
                }
            }

            if (constants.tcp_user_timeout_ms > 0) {
                if (is_linux) {
                    try set(fd, os.IPPROTO.TCP, os.TCP.USER_TIMEOUT, constants.tcp_user_timeout_ms);
                }
            }

            // Set tcp no-delay
            if (constants.tcp_nodelay) {
                if (is_linux) {
                    try set(fd, os.IPPROTO.TCP, os.TCP.NODELAY, 1);
                }
            }

            try set(fd, os.SOL.SOCKET, os.SO.REUSEADDR, 1);
            try os.bind(fd, &address.any, address.getOsSockLen());
            try os.listen(fd, constants.tcp_backlog);

            return fd;
        }

        pub fn tick(bus: *Self) void {
            switch (process_type) {
                .replica => {
                    // Each replica is responsible for connecting to replicas that come
                    // after it in the configuration. This ensures that replicas never try
                    // to connect to each other at the same time.
                    var replica: u8 = bus.process.replica + 1;
                    while (replica < bus.replicas.len) : (replica += 1) {
                        bus.maybe_connect_to_replica(replica);
                    }

                    // Only replicas accept connections from other replicas and clients:
                    bus.maybe_accept();
                },
                .client => {
                    // The client connects to all replicas.
                    var replica: u8 = 0;
                    while (replica < bus.replicas.len) : (replica += 1) {
                        bus.maybe_connect_to_replica(replica);
                    }
                },
            }
        }

        fn maybe_connect_to_replica(bus: *Self, replica: u8) void {
            // We already have a connection to the given replica.
            if (bus.replicas[replica] != null) {
                assert(bus.connections_used > 0);
                return;
            }

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

            log.info("all connections in use but not all replicas are connected, " ++
                "attempting to disconnect a client", .{});
            for (bus.connections) |*connection| {
                if (connection.peer == .client) {
                    connection.terminate(bus, .shutdown);
                    return;
                }
            }

            log.info("failed to disconnect a client as no peer was a known client, " ++
                "attempting to disconnect an unknown peer.", .{});
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

        fn maybe_accept(bus: *Self) void {
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
                *Self,
                bus,
                on_accept,
                &bus.process.accept_completion,
                bus.process.accept_fd,
            );
        }

        fn on_accept(
            bus: *Self,
            completion: *IO.Completion,
            result: IO.AcceptError!os.socket_t,
        ) void {
            _ = completion;

            comptime assert(process_type == .replica);
            assert(bus.process.accept_connection != null);
            defer bus.process.accept_connection = null;
            const fd = result catch |err| {
                bus.process.accept_connection.?.state = .free;
                // TODO: some errors should probably be fatal
                log.err("accept failed: {}", .{err});
                return;
            };
            bus.process.accept_connection.?.on_accept(bus, fd);
        }

        pub fn get_message(bus: *Self) *Message {
            return bus.pool.get_message();
        }

        pub fn unref(bus: *Self, message: *Message) void {
            bus.pool.unref(message);
        }

        pub fn send_message_to_replica(bus: *Self, replica: u8, message: *Message) void {
            // Messages sent by a replica to itself should never be passed to the message bus.
            if (process_type == .replica) assert(replica != bus.process.replica);

            if (bus.replicas[replica]) |connection| {
                connection.send_message(bus, message);
            } else {
                log.debug("no active connection to replica {}, " ++
                    "dropping message with header {}", .{ replica, message.header });
            }
        }

        /// Try to send the message to the client with the given id.
        /// If the client is not currently connected, the message is silently dropped.
        pub fn send_message_to_client(bus: *Self, client_id: u128, message: *Message) void {
            comptime assert(process_type == .replica);

            if (bus.process.clients.get(client_id)) |connection| {
                connection.send_message(bus, message);
            } else {
                log.debug("no connection to client {x}", .{client_id});
            }
        }

        /// Used to send/receive messages to/from a client or fellow replica.
        const Connection = struct {
            /// The peer is determined by inspecting the first message header
            /// received.
            peer: union(enum) {
                /// No peer is currently connected.
                none: void,
                /// A connection is established but an unambiguous header has not yet been received.
                unknown: void,
                /// The peer is a client with the given id.
                client: u128,
                /// The peer is a replica with the given id.
                replica: u8,
            } = .none,
            state: enum {
                /// The connection is not in use, with peer set to `.none`.
                free,
                /// The connection has been reserved for an in progress accept operation,
                /// with peer set to `.none`.
                accepting,
                /// The peer is a replica and a connect operation has been started
                /// but not yet competed.
                connecting,
                /// The peer is fully connected and may be a client, replica, or unknown.
                connected,
                /// The connection is being terminated but cleanup has not yet finished.
                terminating,
            } = .free,
            /// This is guaranteed to be valid only while state is connected.
            /// It will be reset to IO.INVALID_SOCKET during the shutdown process and is always IO.INVALID_SOCKET if the
            /// connection is unused (i.e. peer == .none). We use IO.INVALID_SOCKET instead of undefined here
            /// for safety to ensure an error if the invalid value is ever used, instead of
            /// potentially performing an action on an active fd.
            fd: os.socket_t = IO.INVALID_SOCKET,

            /// This completion is used for all recv operations.
            /// It is also used for the initial connect when establishing a replica connection.
            recv_completion: IO.Completion = undefined,
            /// True exactly when the recv_completion has been submitted to the IO abstraction
            /// but the callback has not yet been run.
            recv_submitted: bool = false,
            /// The Message with the buffer passed to the kernel for recv operations.
            recv_message: ?*Message = null,
            /// The number of bytes in `recv_message` that have been received and need parsing.
            recv_progress: usize = 0,
            /// The number of bytes in `recv_message` that have been parsed.
            recv_parsed: usize = 0,
            /// True if we have already checked the header checksum of the message we
            /// are currently receiving/parsing.
            recv_checked_header: bool = false,

            /// This completion is used for all send operations.
            send_completion: IO.Completion = undefined,
            /// True exactly when the send_completion has been submitted to the IO abstraction
            /// but the callback has not yet been run.
            send_submitted: bool = false,
            /// Number of bytes of the current message that have already been sent.
            send_progress: usize = 0,
            /// The queue of messages to send to the client or replica peer.
            send_queue: SendQueue = .{},

            /// Attempt to connect to a replica.
            /// The slot in the Message.replicas slices is immediately reserved.
            /// Failure is silent and returns the connection to an unused state.
            pub fn connect_to_replica(connection: *Connection, bus: *Self, replica: u8) void {
                if (process_type == .replica) assert(replica != bus.process.replica);

                assert(connection.peer == .none);
                assert(connection.state == .free);
                assert(connection.fd == IO.INVALID_SOCKET);

                // The first replica's network address family determines the
                // family for all other replicas:
                const family = bus.configuration[0].any.family;
                connection.fd = bus.io.open_socket(family, os.SOCK.STREAM, os.IPPROTO.TCP) catch return;
                connection.peer = .{ .replica = replica };
                connection.state = .connecting;
                bus.connections_used += 1;

                assert(bus.replicas[replica] == null);
                bus.replicas[replica] = connection;

                var attempts = &bus.replicas_connect_attempts[replica];
                const ms = vsr.exponential_backoff_with_jitter(
                    bus.prng.random(),
                    constants.connection_delay_min_ms,
                    constants.connection_delay_max_ms,
                    attempts.*,
                );
                attempts.* += 1;

                log.debug("connecting to replica {} in {}ms...", .{ connection.peer.replica, ms });

                assert(!connection.recv_submitted);
                connection.recv_submitted = true;

                bus.io.timeout(
                    *Self,
                    bus,
                    on_connect_with_exponential_backoff,
                    // We use `recv_completion` for the connection `timeout()` and `connect()` calls
                    &connection.recv_completion,
                    @intCast(u63, ms * std.time.ns_per_ms),
                );
            }

            fn on_connect_with_exponential_backoff(
                bus: *Self,
                completion: *IO.Completion,
                result: IO.TimeoutError!void,
            ) void {
                const connection = @fieldParentPtr(Connection, "recv_completion", completion);
                assert(connection.recv_submitted);
                connection.recv_submitted = false;
                if (connection.state == .terminating) {
                    connection.maybe_close(bus);
                    return;
                }
                assert(connection.state == .connecting);
                result catch unreachable;

                log.debug("connecting to replica {}...", .{connection.peer.replica});

                assert(!connection.recv_submitted);
                connection.recv_submitted = true;

                bus.io.connect(
                    *Self,
                    bus,
                    on_connect,
                    // We use `recv_completion` for the connection `timeout()` and `connect()` calls
                    &connection.recv_completion,
                    connection.fd,
                    bus.configuration[connection.peer.replica],
                );
            }

            fn on_connect(
                bus: *Self,
                completion: *IO.Completion,
                result: IO.ConnectError!void,
            ) void {
                const connection = @fieldParentPtr(Connection, "recv_completion", completion);
                assert(connection.recv_submitted);
                connection.recv_submitted = false;

                if (connection.state == .terminating) {
                    connection.maybe_close(bus);
                    return;
                }
                assert(connection.state == .connecting);
                connection.state = .connected;

                result catch |err| {
                    log.err("error connecting to replica {}: {}", .{ connection.peer.replica, err });
                    connection.terminate(bus, .close);
                    return;
                };

                log.info("connected to replica {}", .{connection.peer.replica});
                bus.replicas_connect_attempts[connection.peer.replica] = 0;

                connection.assert_recv_send_initial_state(bus);
                // This will terminate the connection if there are no messages available:
                connection.get_recv_message_and_recv(bus);
                // A message may have been queued for sending while we were connecting:
                // TODO Should we relax recv() and send() to return if `connection.state != .connected`?
                if (connection.state == .connected) connection.send(bus);
            }

            /// Given a newly accepted fd, start receiving messages on it.
            /// Callbacks will be continuously re-registered until terminate() is called.
            pub fn on_accept(connection: *Connection, bus: *Self, fd: os.socket_t) void {
                assert(connection.peer == .none);
                assert(connection.state == .accepting);
                assert(connection.fd == IO.INVALID_SOCKET);

                connection.peer = .unknown;
                connection.state = .connected;
                connection.fd = fd;
                bus.connections_used += 1;

                connection.assert_recv_send_initial_state(bus);
                connection.get_recv_message_and_recv(bus);
                assert(connection.send_queue.empty());
            }

            fn assert_recv_send_initial_state(connection: *Connection, bus: *Self) void {
                assert(bus.connections_used > 0);

                assert(connection.peer == .unknown or connection.peer == .replica);
                assert(connection.state == .connected);
                assert(connection.fd != IO.INVALID_SOCKET);

                assert(connection.recv_submitted == false);
                assert(connection.recv_message == null);
                assert(connection.recv_progress == 0);
                assert(connection.recv_parsed == 0);

                assert(connection.send_submitted == false);
                assert(connection.send_progress == 0);
            }

            /// Add a message to the connection's send queue, starting a send operation
            /// if the queue was previously empty.
            pub fn send_message(connection: *Connection, bus: *Self, message: *Message) void {
                assert(connection.peer == .client or connection.peer == .replica);
                switch (connection.state) {
                    .connected, .connecting => {},
                    .terminating => return,
                    .free, .accepting => unreachable,
                }
                if (connection.send_queue.full()) {
                    log.info("message queue for peer {} full, dropping {s} message", .{
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
            pub fn terminate(connection: *Connection, bus: *Self, how: enum { shutdown, close }) void {
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
                        os.shutdown(connection.fd, .both) catch |err| switch (err) {
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

            fn parse_messages(connection: *Connection, bus: *Self) void {
                assert(connection.peer != .none);
                assert(connection.state == .connected);
                assert(connection.fd != IO.INVALID_SOCKET);

                while (connection.parse_message(bus)) |message| {
                    defer bus.unref(message);

                    connection.on_message(bus, message);
                }
            }

            fn parse_message(connection: *Connection, bus: *Self) ?*Message {
                const data = connection.recv_message.?.buffer[connection.recv_parsed..connection.recv_progress];
                if (data.len < @sizeOf(Header)) {
                    connection.get_recv_message_and_recv(bus);
                    return null;
                }

                const header = mem.bytesAsValue(
                    Header,
                    @alignCast(@alignOf(Header), data[0..@sizeOf(Header)]),
                );

                if (!connection.recv_checked_header) {
                    if (!header.valid_checksum()) {
                        log.err("invalid header checksum received from {}", .{connection.peer});
                        connection.terminate(bus, .shutdown);
                        return null;
                    }

                    if (header.size < @sizeOf(Header) or header.size > constants.message_size_max) {
                        log.err("header with invalid size {d} received from peer {}", .{
                            header.size,
                            connection.peer,
                        });
                        connection.terminate(bus, .shutdown);
                        return null;
                    }

                    if (header.cluster != bus.cluster) {
                        log.err("message addressed to the wrong cluster: {}", .{header.cluster});
                        connection.terminate(bus, .shutdown);
                        return null;
                    }

                    switch (process_type) {
                        // Replicas may forward messages from clients or from other replicas so we
                        // may receive messages from a peer before we know who they are:
                        // This has the same effect as an asymmetric network where, for a short time
                        // bounded by the time it takes to ping, we can hear from a peer before we
                        // can send back to them.
                        .replica => connection.maybe_set_peer(bus, header),
                        // The client connects only to replicas and should set peer when connecting:
                        .client => assert(connection.peer == .replica),
                    }

                    connection.recv_checked_header = true;
                }

                if (data.len < header.size) {
                    connection.get_recv_message_and_recv(bus);
                    return null;
                }

                // At this point we know that we have the full message in our buffer.
                // We will now either deliver this message or terminate the connection
                // due to an error, so reset recv_checked_header for the next message.
                assert(connection.recv_checked_header);
                connection.recv_checked_header = false;

                const body = data[@sizeOf(Header)..header.size];
                if (!header.valid_checksum_body(body)) {
                    log.err("invalid body checksum received from {}", .{connection.peer});
                    connection.terminate(bus, .shutdown);
                    return null;
                }

                connection.recv_parsed += header.size;

                // Return the parsed message using zero-copy if we can, or copy if the client is
                // pipelining:
                // If this is the first message but there are messages in the pipeline then we
                // copy the message so that its sector padding (if any) will not overwrite the
                // front of the pipeline.  If this is not the first message then we must copy
                // the message to a new message as each message needs to have its own unique
                // `references` and `header` metadata.
                if (connection.recv_progress == header.size) return connection.recv_message.?.ref();

                const message = bus.get_message();
                util.copy_disjoint(.inexact, u8, message.buffer, data[0..header.size]);
                return message;
            }

            /// Forward a received message to `Process.on_message()`.
            /// Zero any `.prepare` sector padding up to the nearest sector multiple after the body.
            fn on_message(connection: *Connection, bus: *Self, message: *Message) void {
                if (message == connection.recv_message.?) {
                    assert(connection.recv_parsed == message.header.size);
                    assert(connection.recv_parsed == connection.recv_progress);
                } else if (connection.recv_parsed == message.header.size) {
                    assert(connection.recv_parsed < connection.recv_progress);
                } else {
                    assert(connection.recv_parsed > message.header.size);
                    assert(connection.recv_parsed <= connection.recv_progress);
                }

                if (message.header.command == .request or message.header.command == .prepare) {
                    const sector_ceil = vsr.sector_ceil(message.header.size);
                    if (message.header.size != sector_ceil) {
                        assert(message.header.size < sector_ceil);
                        assert(message.buffer.len == constants.message_size_max + constants.sector_size);
                        mem.set(u8, message.buffer[message.header.size..sector_ceil], 0);
                    }
                }

                bus.on_message_callback(bus, message);
            }

            fn maybe_set_peer(connection: *Connection, bus: *Self, header: *const Header) void {
                comptime assert(process_type == .replica);

                assert(bus.cluster == header.cluster);
                assert(bus.connections_used > 0);

                assert(connection.peer != .none);
                assert(connection.state == .connected);
                assert(connection.fd != IO.INVALID_SOCKET);

                if (connection.peer != .unknown) return;

                switch (header.peer_type()) {
                    .unknown => return,
                    .replica => {
                        connection.peer = .{ .replica = header.replica };
                        // If there is a connection to this replica, terminate and replace it:
                        if (bus.replicas[connection.peer.replica]) |old| {
                            assert(old.peer == .replica);
                            assert(old.peer.replica == connection.peer.replica);
                            assert(old.state != .free);
                            if (old.state != .terminating) old.terminate(bus, .shutdown);
                        }
                        bus.replicas[connection.peer.replica] = connection;
                        log.info("connection from replica {}", .{connection.peer.replica});
                    },
                    .client => {
                        connection.peer = .{ .client = header.client };
                        const result = bus.process.clients.getOrPutAssumeCapacity(header.client);
                        // If there is a connection to this client, terminate and replace it:
                        if (result.found_existing) {
                            const old = result.value_ptr.*;
                            assert(old.peer == .client);
                            assert(old.peer.client == connection.peer.client);
                            assert(old.state == .connected or old.state == .terminating);
                            if (old.state != .terminating) old.terminate(bus, .shutdown);
                        }
                        result.value_ptr.* = connection;
                        log.info("connection from client {}", .{connection.peer.client});
                    },
                }
            }

            /// Acquires a free message if necessary and then calls `recv()`.
            /// If the connection has a `recv_message` and the message being parsed is
            /// at pole position then calls `recv()` immediately, otherwise copies any
            /// partially received message into a new Message and sets `recv_message`,
            /// releasing the old one.
            fn get_recv_message_and_recv(connection: *Connection, bus: *Self) void {
                if (connection.recv_message != null and connection.recv_parsed == 0) {
                    connection.recv(bus);
                    return;
                }

                const new_message = bus.get_message();
                defer bus.unref(new_message);

                if (connection.recv_message) |recv_message| {
                    defer bus.unref(recv_message);

                    assert(connection.recv_progress > 0);
                    assert(connection.recv_parsed > 0);
                    const data = recv_message.buffer[connection.recv_parsed..connection.recv_progress];
                    util.copy_disjoint(.inexact, u8, new_message.buffer, data);
                    connection.recv_progress = data.len;
                    connection.recv_parsed = 0;
                } else {
                    assert(connection.recv_progress == 0);
                    assert(connection.recv_parsed == 0);
                }

                connection.recv_message = new_message.ref();
                connection.recv(bus);
            }

            fn recv(connection: *Connection, bus: *Self) void {
                assert(connection.peer != .none);
                assert(connection.state == .connected);
                assert(connection.fd != IO.INVALID_SOCKET);

                assert(!connection.recv_submitted);
                connection.recv_submitted = true;

                assert(connection.recv_progress < constants.message_size_max);

                bus.io.recv(
                    *Self,
                    bus,
                    on_recv,
                    &connection.recv_completion,
                    connection.fd,
                    connection.recv_message.?.buffer[connection.recv_progress..constants.message_size_max],
                );
            }

            fn on_recv(bus: *Self, completion: *IO.Completion, result: IO.RecvError!usize) void {
                const connection = @fieldParentPtr(Connection, "recv_completion", completion);
                assert(connection.recv_submitted);
                connection.recv_submitted = false;
                if (connection.state == .terminating) {
                    connection.maybe_close(bus);
                    return;
                }
                assert(connection.state == .connected);
                const bytes_received = result catch |err| {
                    // TODO: maybe don't need to close on *every* error
                    log.err("error receiving from {}: {}", .{ connection.peer, err });
                    connection.terminate(bus, .shutdown);
                    return;
                };
                // No bytes received means that the peer closed its side of the connection.
                if (bytes_received == 0) {
                    log.info("peer performed an orderly shutdown: {}", .{connection.peer});
                    connection.terminate(bus, .close);
                    return;
                }
                connection.recv_progress += bytes_received;
                connection.parse_messages(bus);
            }

            fn send(connection: *Connection, bus: *Self) void {
                assert(connection.peer == .client or connection.peer == .replica);
                assert(connection.state == .connected);
                assert(connection.fd != IO.INVALID_SOCKET);
                const message = connection.send_queue.head() orelse return;
                assert(!connection.send_submitted);
                connection.send_submitted = true;
                bus.io.send(
                    *Self,
                    bus,
                    on_send,
                    &connection.send_completion,
                    connection.fd,
                    message.buffer[connection.send_progress..message.header.size],
                );
            }

            fn on_send(bus: *Self, completion: *IO.Completion, result: IO.SendError!usize) void {
                const connection = @fieldParentPtr(Connection, "send_completion", completion);
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
                    log.err("error sending message to replica at {}: {}", .{ connection.peer, err });
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

            fn maybe_close(connection: *Connection, bus: *Self) void {
                assert(connection.peer != .none);
                assert(connection.state == .terminating);
                // If a recv or send operation is currently submitted to the kernel,
                // submitting a close would cause a race. Therefore we must wait for
                // any currently submitted operation to complete.
                if (connection.recv_submitted or connection.send_submitted) return;
                connection.send_submitted = true;
                connection.recv_submitted = true;
                // We can free resources now that there is no longer any I/O in progress.
                while (connection.send_queue.pop()) |message| {
                    bus.unref(message);
                }
                if (connection.recv_message) |message| {
                    bus.unref(message);
                    connection.recv_message = null;
                }
                assert(connection.fd != IO.INVALID_SOCKET);
                defer connection.fd = IO.INVALID_SOCKET;
                // It's OK to use the send completion here as we know that no send
                // operation is currently in progress.
                bus.io.close(*Self, bus, on_close, &connection.send_completion, connection.fd);
            }

            fn on_close(bus: *Self, completion: *IO.Completion, result: IO.CloseError!void) void {
                const connection = @fieldParentPtr(Connection, "send_completion", completion);
                assert(connection.send_submitted);
                assert(connection.recv_submitted);

                assert(connection.peer != .none);
                assert(connection.state == .terminating);

                // Reset the connection to its initial state.
                defer {
                    assert(connection.recv_message == null);
                    assert(connection.send_queue.empty());

                    switch (connection.peer) {
                        .none => unreachable,
                        .unknown => {},
                        .client => switch (process_type) {
                            .replica => assert(bus.process.clients.remove(connection.peer.client)),
                            .client => unreachable,
                        },
                        .replica => {
                            // A newer replica connection may have replaced this one:
                            if (bus.replicas[connection.peer.replica] == connection) {
                                bus.replicas[connection.peer.replica] = null;
                            } else {
                                // A newer replica connection may even leapfrog this connection and
                                // then be terminated and set to null before we can get here:
                                assert(bus.replicas[connection.peer.replica] != null or
                                    bus.replicas[connection.peer.replica] == null);
                            }
                        },
                    }
                    bus.connections_used -= 1;
                    connection.* = .{};
                }

                result catch |err| {
                    log.err("error closing connection to {}: {}", .{ connection.peer, err });
                    return;
                };
            }
        };
    };
}
