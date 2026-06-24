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
const Header = vsr.Header;
const HeaderEncrypted = vsr.HeaderEncrypted;
const QueueType = @import("./queue.zig").QueueType;
const Tracer = vsr.trace.Tracer;
const encryption = @import("encryption.zig");
const KeyExchange = encryption.KeyExchangeInsecure;
const Peer = vsr.Peer;

const RecvState = struct {
    /// This is the buffer that contains data received from the network.
    buffer: []u8 = &.{},
    advanced_size: u32 = 0,
    consumed_size: u32 = 0,
    message_size: ?u32 = null,
    outstanding: bool = false,

    fn slice(recv: *RecvState) []u8 {
        assert(recv.outstanding == false);
        // Can we actually assert this?
        assert(recv.advanced_size < recv.buffer.len);
        recv.outstanding = true;
        return recv.buffer[recv.advanced_size..];
    }

    fn advance(recv: *RecvState, size: u32) void {
        assert(recv.outstanding == true);
        assert(recv.advanced_size + size <= recv.buffer.len);
        assert(size > 0);

        recv.outstanding = false;
        recv.advanced_size += size;
    }

    fn peek_header(recv: *RecvState) ?HeaderEncrypted {
        assert(recv.advanced_size >= recv.consumed_size);
        assert(recv.buffer.len >= recv.advanced_size);
        assert(!recv.outstanding);
        assert(recv.message_size == null);

        if (recv.advanced_size - recv.consumed_size < @sizeOf(HeaderEncrypted)) {
            return null;
        }

        const header_bytes = recv.buffer[recv.consumed_size..][0..@sizeOf(HeaderEncrypted)];
        var header_encrypted: HeaderEncrypted = undefined;
        stdx.copy_disjoint(.exact, u8, std.mem.asBytes(&header_encrypted), header_bytes);

        return header_encrypted;
    }

    fn pop_message(recv: *RecvState) ?[]const u8 {
        assert(recv.advanced_size >= recv.consumed_size);
        assert(recv.buffer.len >= recv.advanced_size);
        assert(!recv.outstanding);

        if (recv.message_size == null) {
            return null;
        }

        const size = recv.message_size.?;

        if (recv.advanced_size - recv.consumed_size < size) {
            return null;
        }

        assert(recv.consumed_size + size <= recv.advanced_size);

        const message = recv.buffer[recv.consumed_size..][0..size];

        recv.consumed_size += size;
        return message;
    }

    fn copy_left(recv: *RecvState) void {
        assert(recv.advanced_size >= recv.consumed_size);
        assert(recv.message_size != null);
        assert(!recv.outstanding);

        const consumed = recv.consumed_size;
        assert(consumed > 0);

        stdx.copy_left(
            .inexact,
            u8,
            recv.buffer[0..],
            recv.buffer[consumed..recv.advanced_size],
        );

        recv.* = .{
            .buffer = recv.buffer,
            .advanced_size = recv.advanced_size - consumed,
            .consumed_size = 0,
            .message_size = null,
            .outstanding = false,
        };
    }
};

const SendState = struct {
    /// This is the buffer that contains data to be written to the network.
    buffer: []u8 = &.{},
    /// Number of bytes of the current message that have already been sent.
    progress: u32 = 0,
    /// Number of bytes requested to send via this connection.
    requested: u32 = 0,
};

pub const MessageNetwork = struct {
    context: ?*anyopaque,
    buffer: []u8,
};

pub const HeaderCallbackResult = struct {
    message_size: u32,
};

// TODO: revisit and think about if this should indicate if a handshake is completed.
pub const MessageCallbackResult = struct {
    peer: Peer,
};

pub fn MessageBusType(comptime IO: type) type {
    const ProcessID = union(vsr.ProcessType) {
        replica: u8,
        client: u128,
    };

    const Connection = struct {
        state: enum {
            /// The connection is not in use, with peer set to `.unknown`.
            free,
            /// The connection has been reserved for an in progress accept operation,
            /// with peer set to `.unknown`.
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
        /// connection is unused
        fd: ?IO.socket_t = null,

        /// This completion is used for all recv operations.
        /// It is also used for the initial connect when establishing a replica connection.
        recv_completion: IO.Completion = undefined,

        recv: RecvState = .{},

        /// True exactly when the recv_completion has been submitted to the IO abstraction
        /// but the callback has not yet been run.
        recv_submitted: bool = false,

        /// This completion is used for all send operations.
        send_completion: IO.Completion = undefined,
        /// True exactly when the send_completion has been submitted to the IO abstraction
        /// but the callback has not yet been run.
        send_submitted: bool = false,

        send: SendState = .{},

        peer: Peer = .unknown,
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

        /// SendQueue storage shared by all connections.
        send_buffers: []u8,
        recv_buffers: []u8,
        /// This slice is allocated with a fixed size in the init function and never reallocated.
        connections: []Connection,
        /// Number of connections currently in use (i.e. connection.state != .free).
        connections_used: u32 = 0,

        header_callback: *const fn (
            context: *anyopaque,
            header: HeaderEncrypted,
        ) anyerror!u32,

        message_callback: *const fn (
            context: *anyopaque,
            message: []const u8,
        ) anyerror!Peer,

        /// Map from replica index to the currently active connection for that replica, if any.
        /// The connection for the process replica if any will always be null.
        replicas: []?*Connection,
        replicas_addresses: []Address,
        /// The number of outgoing `connect()` attempts for a given replica:
        /// Reset to zero after a successful `connect_callback()`.
        replicas_connect_attempts: []u64,

        /// Map from client id to the currently active connection for that client.
        /// This is used to make lookup of client connections when sending messages
        /// efficient and to ensure old client connections are dropped if a new one
        /// is established.
        clients: std.AutoHashMapUnmanaged(u128, *Connection) = .{},

        handshakes: std.AutoHashMapUnmanaged(u128, *Connection) = .{},

        /// Used to apply jitter when calculating exponential backoff:
        /// Seeded with the process' replica index or client ID.
        prng: stdx.PRNG,

        trace: ?*Tracer,

        comptime {
            // Assert it is correct to use u32 to track sizes.
            assert(constants.message_size_max < std.math.maxInt(u32));
        }

        pub const Options = struct {
            configuration: []const Address,
            io: *IO,
            trace: ?*Tracer,
            clients_limit: ?u32 = null,
        };
        const Address = std.net.Address;
        const MessageBus = @This();

        /// Initialize the MessageBus for the given configuration and replica/client process.
        pub fn init(
            allocator: mem.Allocator,
            process_id: ProcessID,
            message_pool: *MessagePool,
            header_callback: *const fn (
                context: *anyopaque,
                header: HeaderEncrypted,
            ) anyerror!u32,
            message_callback: *const fn (
                context: *anyopaque,
                message: []const u8,
            ) anyerror!Peer,
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

            const send_buffer_size = 2 * constants.message_size_max;

            const send_buffers = try allocator.alloc(
                u8,
                connections_max * send_buffer_size,
            );
            errdefer allocator.free(send_buffers);

            const recv_buffers = try allocator.alloc(
                u8,
                connections_max * constants.message_size_max,
            );
            errdefer allocator.free(send_buffers);

            const connections = try allocator.alloc(Connection, connections_max);
            errdefer allocator.free(connections);

            for (connections, 0..) |*connection, index| {
                const send_buffer = send_buffers[index * send_buffer_size ..];
                const recv_buffer = recv_buffers[index * constants.message_size_max ..];
                connection.* = .{};
                connection.send.buffer = send_buffer[0..send_buffer_size];
                connection.recv.buffer = recv_buffer[0..constants.message_size_max];
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
                .send_buffers = send_buffers,
                .recv_buffers = recv_buffers,
                .connections = connections,
                .header_callback = header_callback,
                .message_callback = message_callback,
                .replicas = replicas,
                .replicas_addresses = replicas_addresses,
                .replicas_connect_attempts = replicas_connect_attempts,
                .prng = stdx.PRNG.from_seed(prng_seed),
                .trace = options.trace,
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

        /// Begin a graceful shutdown of every active connection.
        ///
        /// After this is called, the caller must continue to drive `io.run_for_ns()` until
        /// `shutdown_complete()` returns true. Only then is it safe to call `deinit()` or
        /// to tear down the underlying IO.
        ///
        /// Idempotent: connections already terminating are left alone.
        pub fn shutdown(bus: *MessageBus) void {
            for (bus.connections) |*connection| {
                switch (connection.state) {
                    .free, .terminating => {},
                    // `.accepting` would need different handling (no fd yet); only client-side
                    // bus shutdown is supported today, and clients never accept.
                    .accepting => assert(bus.process == .replica),
                    .connecting,
                    .connected,
                    => {
                        bus.terminate(connection, .shutdown);
                    },
                }
            }
        }

        /// True when every connection initiated by `shutdown()` has fully closed, so no
        /// outstanding kernel operation references this bus's memory.
        pub fn shutdown_complete(bus: *const MessageBus) bool {
            return bus.connections_used == 0;
        }

        pub fn deinit(bus: *MessageBus, allocator: std.mem.Allocator) void {
            bus.clients.deinit(allocator);

            if (bus.accept_fd) |fd| {
                assert(bus.process == .replica);
                assert(bus.accept_address != null);
                bus.io.close_socket(fd);
            }

            for (bus.connections) |*connection| {
                if (connection.fd) |fd| {
                    bus.io.close_socket(fd);
                }
            }

            allocator.free(bus.replicas_connect_attempts);
            allocator.free(bus.replicas_addresses);
            allocator.free(bus.replicas);
            allocator.free(bus.connections);
            allocator.free(bus.send_buffers);
            allocator.free(bus.recv_buffers);

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
            std.log.info("accept_callback: on replica: {d}", .{bus.process.replica});

            assert(bus.accept_connection != null);
            const connection: *Connection = bus.accept_connection.?;
            bus.accept_connection = null;

            assert(connection.fd == null);
            assert(connection.state == .accepting);
            defer assert(connection.state == .connected or
                connection.state == .free or
                connection.state == .terminating);

            if (result) |fd| {
                connection.state = .connected;
                connection.fd = fd;
                bus.connections_used += 1;

                bus.assert_connection_initial_state(connection);
                bus.recv(connection);
                // Don't start send loop yet --- on accept, we don't know which peer this is.
                assert(connection.state == .connected);
            } else |err| {
                connection.state = .free;
                // TODO: some errors should probably be fatal
                log.warn("{}: accept_callback: {}", .{ bus.id, err });
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
            // TODO: revisit
            // for (bus.connections) |*connection| {
            //     if (connection.peer == .known and connection.peer.known.peer == .client) {
            //         bus.terminate(connection, .shutdown);
            //         return;
            //     }
            // }

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

            comptime assert(constants.connection_delay_min.to_ms() > 0);

            const attempts = &bus.replicas_connect_attempts[replica];
            const ms = vsr.exponential_backoff_with_jitter(
                &bus.prng,
                constants.connection_delay_min.to_ms(),
                constants.connection_delay_max.to_ms(),
                attempts.*,
            );
            attempts.* += 1;

            log.debug("{}: connect_to_replica: connecting after={}ms", .{
                bus.id,
                ms,
            });

            assert(!connection.recv_submitted);
            connection.recv_submitted = true;

            assert(ms > 0);
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

            log.debug("{}: connect_callback_with_exponential_backoff: ", .{
                bus.id,
            });

            assert(!connection.recv_submitted);
            connection.recv_submitted = true;

            assert(connection.peer == .replica);
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
                log.warn("{}: connect_callback: error {}", .{
                    bus.id,
                    err,
                });
                bus.terminate(connection, .no_shutdown);
                return;
            };

            log.info("{}: connect_callback: connected", .{bus.id});
            assert(connection.peer == .replica);
            bus.replicas_connect_attempts[connection.peer.replica] = 0;

            bus.assert_connection_initial_state(connection);

            assert(connection.state == .connected);
        }

        fn assert_connection_initial_state(bus: *MessageBus, connection: *Connection) void {
            assert(bus.connections_used > 0);

            assert(connection.state == .connected);
            assert(connection.fd != null);

            assert(connection.recv_submitted == false);

            assert(connection.send_submitted == false);
            assert(connection.send.progress == 0);
        }

        /// The recv loop.
        ///
        /// Kickstarted by `accept` and `connect`, and loops onto itself via `recv_buffer_drain`.
        fn recv(bus: *MessageBus, connection: *Connection) void {
            assert(connection.state == .connected);
            assert(connection.fd != null);

            assert(!connection.recv_submitted);
            connection.recv_submitted = true;

            bus.io.recv(
                *MessageBus,
                bus,
                recv_callback,
                &connection.recv_completion,
                connection.fd.?,
                connection.recv.slice(),
            );
        }

        /// Attempt moving messages from recv buffer into replica for processing. Called when recv
        /// syscall completes, or when a replica signals readiness to consume previously suspended
        /// messages.
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
                // On any error the framing becomes unknown.
                // The only winning move is not to play.
                log.warn("{}: recv_callback: read error {}", .{ bus.id, err });
                bus.terminate(connection, .shutdown);
                return;
            };

            std.log.info("recv_callback: received {d} bytes", .{bytes_received});

            // No bytes received means that the peer closed its side of the connection.
            if (bytes_received == 0) {
                log.info("{}: recv_callback: orderly shutdown", .{bus.id});
                bus.terminate(connection, .no_shutdown);
                return;
            }

            assert(bytes_received <= connection.recv.buffer.len);

            connection.recv.advance(@intCast(bytes_received));

            if (connection.recv.message_size == null) {
                if (connection.recv.peek_header()) |header_encrypted| {
                    const message_size = bus.header_callback(bus, header_encrypted) catch |err| {
                        log.warn("{}: recv_callback: err {}", .{ bus.id, err });
                        bus.terminate(connection, .shutdown);
                        return;
                    };
                    connection.recv.message_size = message_size;
                }
            }

            if (connection.recv.pop_message()) |message| {
                const peer = bus.message_callback(bus, message) catch |err| {
                    log.warn("{}: recv_callback: {}", .{ bus.id, err });
                    bus.terminate(connection, .shutdown);
                    return;
                };

                switch (peer) {
                    .replica => |replica_idx| {
                        if (replica_idx >= constants.replicas_max) {
                            log.warn("{}: recv_callback: illegal replica id: {d}", .{
                                bus.id,
                                replica_idx,
                            });
                            bus.terminate(connection, .shutdown);
                            return;
                        }
                    },
                    else => {},
                }

                switch (connection.peer.transition(peer)) {
                    .update => {
                        connection.peer = peer;
                        switch (connection.peer) {
                            .client => |client_id| {
                                const client_gop = bus.clients.getOrPutAssumeCapacity(client_id);
                                client_gop.value_ptr.* = connection;
                            },
                            .replica => |replica_idx| {
                                assert(replica_idx < constants.replicas_max);
                                bus.replicas[replica_idx] = connection;
                            },
                            else => {},
                        }
                    },
                    .retain => {},
                    .reject => {
                        log.warn("{}: recv_callback: bad peer transition", .{bus.id});
                        bus.terminate(connection, .shutdown);
                    },
                }

                connection.recv.copy_left();
            }

            bus.recv(connection);
        }

        pub fn send_message_to_client(
            bus: *MessageBus,
            client_id: u128,
            size: u32,
        ) ?[]u8 {
            assert(bus.process == .replica);
            assert(bus.clients.capacity() > 0);

            if (bus.clients.get(client_id)) |connection| {
                return bus.send_message(connection, size);
            } else {
                log.warn(
                    "{}: send_message_to_client: no connection to={}",
                    .{ bus.id, client_id },
                );
                return null;
            }
        }

        pub fn send_message_handshake(
            bus: *MessageBus,
            handshake_id: u128,
            size: u32,
        ) ?[]u8 {
            assert(bus.process == .replica);
            assert(bus.clients.capacity() > 0);

            if (bus.clients.get(handshake_id)) |connection| {
                return bus.send_message(connection, size);
            } else {
                log.warn(
                    "{}: send_message_to_handshake: no connection to={}",
                    .{ bus.id, handshake_id },
                );
                return null;
            }
        }

        pub fn send_message_to_replica(
            bus: *MessageBus,
            replica: u8,
            size: u32,
        ) ?[]u8 {
            // Messages sent by a replica to itself should never be passed to the message bus.
            if (bus.process == .replica) assert(replica != bus.process.replica);

            if (bus.replicas[replica]) |connection| {
                return bus.send_message(connection, size);
            } else {
                log.warn("{}: send_message_to_replica: no connection to={}", .{
                    bus.id,
                    replica,
                });
                return null;
            }
        }

        fn send_message(
            bus: *MessageBus,
            connection: *Connection,
            size: u32,
        ) ?[]u8 {
            if (size > connection.send.buffer.len - connection.send.requested) {
                log.debug("{}: acquire: out of memory for connection to={}", .{
                    bus.id,
                    connection.peer,
                });
                return null;
            }

            const buffer = connection.send.buffer[connection.send.requested..];
            connection.send.requested += size;

            if (!connection.send_submitted) {
                bus.issue_send(connection);
            }

            return buffer[0..size];
        }

        /// Send loop.
        ///
        /// Kickstarted by `connect` and loops onto itself until all enqueue messages are sent.
        /// `accept` doesn't start the send loop because it doesn't know the identity of the peer.
        fn issue_send(bus: *MessageBus, connection: *Connection) void {
            assert(connection.state == .connected);
            assert(connection.fd != null);
            assert(!connection.send_submitted);

            if (connection.send.requested == connection.send.progress) {
                connection.send.requested = 0;
                connection.send.progress = 0;
                return;
            }

            connection.send_submitted = true;

            bus.io.send(
                *MessageBus,
                bus,
                send_callback,
                &connection.send_completion,
                connection.fd.?,
                connection.send.buffer[connection.send.progress..connection.send.requested],
            );
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

            if (connection.state == .terminating) {
                bus.terminate_join(connection);
                return;
            }
            assert(connection.state == .connected);

            const write_size = result catch |err| {
                // TODO: maybe don't need to close on *every* error
                log.warn("{}: send_callback: {}", .{
                    bus.id,
                    err,
                });
                bus.terminate(connection, .shutdown);
                return;
            };

            std.log.info("send_callback: sent {d} bytes", .{write_size});

            assert(write_size <= constants.message_size_max);
            connection.send.progress += @intCast(write_size);
            assert(connection.send.progress <= connection.send.requested);

            // If all bytes have been sent, reset the state.
            if (connection.send.progress == connection.send.requested) {
                connection.send.progress = 0;
                connection.send.requested = 0;
                return;
            }

            // Otherwise, left align the send buffer and submit another send.
            assert(connection.send.requested > connection.send.progress);
            const remaining_size = connection.send.requested - connection.send.progress;

            stdx.copy_left(
                .exact,
                u8,
                connection.send.buffer[0..remaining_size],
                connection.send.buffer[connection.send.progress..][0..remaining_size],
            );

            connection.send.requested -= connection.send.progress;
            connection.send.progress = 0;
            bus.issue_send(connection);
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

            bus.terminate_close(connection);
        }

        fn terminate_close(bus: *MessageBus, connection: *Connection) void {
            assert(connection.state == .terminating);
            assert(!connection.recv_submitted);
            assert(!connection.send_submitted);
            assert(connection.fd != null);

            connection.send_submitted = true;
            connection.recv_submitted = true;

            // We can free resources now that there is no longer any I/O in progress.
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
            // TODO add assertions
            assert(connection.fd == null);

            result catch |err| {
                log.warn("{}: on_close:  {}", .{ bus.id, err });
            };

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
                .handshaking => |handshake_id| {
                    _ = handshake_id;
                    // TODO: same as client
                },
            }

            // Reset the connection to its initial state.
            bus.connections_used -= 1;
            const send_buffer = connection.send.buffer;
            const recv_buffer = connection.recv.buffer;
            connection.* = .{};
            connection.send.buffer = send_buffer;
            connection.recv.buffer = recv_buffer;
        }

        pub fn get_message(
            bus: *MessageBus,
            comptime command: ?vsr.Command,
        ) MessagePool.GetMessageType(command) {
            return bus.pool.get_message(command);
        }

        /// `@TypeOf(message)` is one of:
        /// - `*MessageNetwork`
        /// - `MessageType(command)` for any `command`.
        pub fn unref(bus: *MessageBus, message: anytype) void {
            bus.pool.unref(message);
        }
    };
}

fn create_message_from_command(message_pool: *MessagePool, prng: *stdx.PRNG, comptime command: vsr.Command) *Message {
    const message = message_pool.get_message(command).base();

    const message_header: *vsr.Header =
        @alignCast(std.mem.bytesAsValue(vsr.Header, message.buffer[0..@sizeOf(vsr.Header)]));

    message_header.* = random_header(prng, command);

    message.header.set_checksum_body(&.{});
    message.header.set_checksum();
    return message;
}

fn create_message_with_body(message_pool: *MessagePool, body: []const u8) *Message {
    assert(body.len <= constants.message_message_size_max);
    const message = message_pool.get_message(.reserved).base();

    const message_header: *vsr.Header =
        @alignCast(std.mem.bytesAsValue(vsr.Header, message.buffer[0..@sizeOf(vsr.Header)]));

    const message_body = message.buffer[@sizeOf(vsr.Header)..][0..body.len];

    stdx.copy_disjoint(.exact, u8, message_body, body);

    message_header.* =
        .{
            .header_tag = 0,
            .header_key_id = 0,
            .header_nonce = 0,
            .body_tag = 0,
            .body_nonce = 0,
            .cluster = 123, // MessageBus doesn't check cluster.
            .size = @sizeOf(vsr.Header) + @as(u32, @intCast(body.len)),
            .epoch = 0,
            .view = 10,
            .release = vsr.Release.zero,
            .protocol = vsr.Version,
            .command = .reserved,
            .replica = 0,
            .reserved_frame = @splat(0),
            .reserved_command = @splat(0),
        };

    const message_network: *Message = @ptrCast(message);
    message_network.metadata = .{
        .size_value = message.header.size,
        .command_value = message.header.command,
    };

    message_network.header.set_checksum_body(message_network.body_used());
    message_network.header.set_checksum();
    message_network.header = undefined;
    return message_network;
}

fn random_header(prng: *stdx.PRNG, command: vsr.Command) vsr.Header {
    return .{
        .header_tag = 0,
        .header_key_id = 0,
        .header_nonce = 0,
        .body_tag = 0,
        .body_nonce = 0,
        .cluster = prng.int(u128), // MessageBus doesn't check cluster.
        .size = @sizeOf(vsr.Header),
        .epoch = 0,
        .view = prng.int(u32),
        .release = vsr.Release.zero,
        .protocol = vsr.Version,
        .command = command,
        .replica = 0,
        .reserved_frame = @splat(0),
        .reserved_command = @splat(0),
    };
}

test "MessageBus unit test" {
    std.testing.log_level = .debug;
    const IO = @import("message_bus_fuzz.zig").IO;
    const MessageBus = MessageBusType(IO);
    const gpa = std.testing.allocator;

    const Parent = struct {
        message_pool: *MessagePool,
        session: *encryption.EncryptionTransit,
        header: ?Header,
        bus: MessageBus,
    };

    var message_pool = try MessagePool.init_capacity(gpa, 128);
    defer message_pool.deinit(gpa);

    var prng = stdx.PRNG.from_seed_testing();

    var io = try IO.init(gpa, .{
        .seed = prng.int(u64),
        .recv_partial_probability = .zero(),
        .recv_error_probability = .zero(),
        .recv_after_shutdown_probability = .zero(),
        .send_partial_probability = .zero(),
        .send_corrupt_probability = .zero(),
        .send_error_probability = .zero(),
        .send_now_probability = .zero(),
        .send_after_shutdown_probability = .zero(),
        .close_error_probability = .zero(),
        .shutdown_error_probability = .zero(),
        .accept_error_probability = .zero(),
        .connect_error_probability = .zero(),
    });
    defer io.deinit();

    const clients_limit = 2;
    const configuration = &.{
        try std.net.Address.parseIp4("127.0.0.1", 3000),
        try std.net.Address.parseIp4("127.0.0.1", 3001),
    };

    const header_callback = struct {
        fn header_callback(context: *anyopaque, header: HeaderEncrypted) anyerror!HeaderCallbackResult {
            const message_bus: *MessageBus = @ptrCast(@alignCast(context));
            const parent: *Parent = @fieldParentPtr("bus", message_bus);
            std.log.info("received header {} for context {*}", .{ header, context });
            const session = parent.session;
            assert(session.established());

            const header_decrypted = try session.decrypt_header(&header);
            parent.header = header_decrypted;

            return .{ .peer = session.other(), .message_size = @sizeOf(HeaderEncrypted) };
        }
    }.header_callback;

    const message_callback = struct {
        pub fn message_callback(
            context: *anyopaque,
            body_encrypted: []const u8,
        ) anyerror!MessageCallbackResult {
            const message_bus: *MessageBus = @ptrCast(@alignCast(context));
            const parent: *Parent = @fieldParentPtr("bus", message_bus);
            const session = parent.session;
            assert(session.established());
            assert(parent.header != null);

            const message = parent.message_pool.get_message(null);
            defer parent.message_pool.unref(message);

            try session.decrypt_body(message, &parent.header.?, body_encrypted);

            std.log.info("Decrypted body", .{});
            parent.header = null;
        }
    }.message_callback;

    var session1 = encryption.EncryptionTransit.connect(.{ .self_id = 0, .self_peer = .replica, .other_id = 1, .other_peer = .replica });
    var session2 = encryption.EncryptionTransit.accept(session1.routing_id, 1, .replica);

    var bus1 = try MessageBus.init(
        gpa,
        .{ .replica = 0 },
        &message_pool,
        header_callback,
        message_callback,
        .{
            .configuration = configuration,
            .io = &io,
            .clients_limit = clients_limit,
            .trace = null,
        },
    );
    defer bus1.deinit(gpa);

    var bus2 = try MessageBus.init(
        gpa,
        .{ .replica = 1 },
        &message_pool,
        header_callback,
        message_callback,
        .{
            .configuration = configuration,
            .io = &io,
            .clients_limit = clients_limit,
            .trace = null,
        },
    );
    defer bus2.deinit(gpa);

    const parent1: Parent = .{
        .message_pool = &message_pool,
        .session = &session1,
        .header = null,
        .bus = bus1,
    };

    const parent2: Parent = .{
        .message_pool = &message_pool,
        .session = &session2,
        .header = null,
        .bus = bus2,
    };

    _ = parent1;
    _ = parent2;

    var step = session1.handshake(null);
    var is_one: bool = false;
    while (!session1.established() or !session2.established()) {
        switch (step.status) {
            .handshaking, .established => {},
            .failed => unreachable,
        }

        if (is_one) {
            step = session1.handshake(step.send);
        } else {
            step = session2.handshake(step.send);
        }
        is_one = !is_one;
    }

    std.log.info("session1 key id: {x}", .{session1.state.established.key_id});
    std.log.info("session1 key_send_header : {any}", .{session1.state.established.key_send_header});
    std.log.info("session1 key_recv_header : {any}", .{session1.state.established.key_recv_header});
    std.log.info("session2 key id: {x}", .{session2.state.established.key_id});
    std.log.info("session2 key_send_header : {any}", .{session2.state.established.key_send_header});
    std.log.info("session2 key_recv_header : {any}", .{session2.state.established.key_recv_header});

    try bus1.listen();
    try bus2.listen();

    for (0..1000) |_| {
        try io.run();
        bus1.tick();
        bus2.tick();
    }

    const message = create_message_from_command(&message_pool, &prng, .ping);
    defer message_pool.unref(message);

    const message_network = bus1.send_message_to_replica(1, message.header.size) orelse unreachable;

    session1.encrypt_message(message_network.buffer, message);

    const header_encrypted: *HeaderEncrypted = @alignCast(std.mem.bytesAsValue(HeaderEncrypted, message_network.buffer[0..@sizeOf(HeaderEncrypted)]));
    std.log.info("header after encryption: {}", .{header_encrypted});

    bus1.send(message_network);

    for (0..1000) |_| {
        try io.run();
        bus1.tick();
        bus2.tick();
    }
}

test "RecvState fuzz" {
    // Generate a byte buffer with a bunch of prepares side-by-side.
    // Optionally corrupt a single bit in the buffer.
    // Feed the buffer in chunks of varying length to the MessageBuffer, verify that all messages
    // are received unless a fault is detected.
    const messages_max = 100;

    var prng = stdx.PRNG.from_seed_testing();
    const gpa = std.testing.allocator;

    var buffer: []u8 = try gpa.alloc(u8, 5 * constants.message_size_max);
    defer gpa.free(buffer);

    blk: for (0..100) |_| {
        var fault = prng.boolean();
        var total_size: u32 = 0;
        var headers: stdx.BoundedArrayType(Header.Prepare, messages_max) = .{};
        for (0..messages_max) |idx| {
            const message_size: u32 = switch (prng.chances(.{
                .min = 10,
                .max = 10,
                .random = 80,
            })) {
                .min => @sizeOf(Header),
                .max => constants.message_size_max,
                .random => prng.range_inclusive(u32, @sizeOf(Header), constants.message_size_max),
            };

            if (total_size + message_size > buffer.len) {
                break;
            }

            var header: vsr.Header.Prepare = .{
                .cluster = 1,
                .view = 1,
                .command = .prepare,
                .parent = prng.int(u128),
                .request_checksum = prng.int(u128),
                .checkpoint_id = prng.int(u128),
                .client = 1,
                .commit = 10,
                .timestamp = 999,
                .request = 1,
                .operation = .register,
                .release = vsr.Release.minimum,
                .op = idx,
                .size = message_size,
            };
            const body = buffer[total_size..][@sizeOf(Header)..header.size];
            prng.fill(body);
            header.set_checksum_body(body);
            header.set_checksum();
            stdx.copy_disjoint(
                .exact,
                u8,
                buffer[total_size..][0..@sizeOf(Header)],
                std.mem.asBytes(&header),
            );
            total_size += header.size;
            headers.push(header);
        }

        if (fault) {
            // const valid_header = std.mem.bytesAsValue(Header, buffer[0..@sizeOf(Header)]).*;
            const byte_index = prng.index(buffer[0..@sizeOf(Header)]);
            const bit_index = prng.int_inclusive(u3, 7);
            buffer[byte_index] ^= @as(u8, 1) << bit_index;
            const invalid_header: Header = std.mem.bytesAsValue(
                Header,
                buffer[0..@sizeOf(Header)],
            ).*;
            if (invalid_header.valid_checksum_zeros()) {
                fault = false;
            }
        }

        var recv_buffer: [constants.message_size_max]u8 = undefined;
        var recv_state = RecvState{ .buffer = &recv_buffer };

        var header_last: ?vsr.Header.Prepare = null;
        var recv_size: u32 = 0;

        while (headers.count() > 0) {
            if (recv_size < total_size) {
                const recv_slice = recv_state.slice();
                const chunk_size = @min(
                    prng.range_inclusive(u32, 1, @intCast(recv_slice.len)),
                    total_size - recv_size,
                );
                stdx.copy_disjoint(
                    .exact,
                    u8,
                    recv_slice[0..chunk_size],
                    buffer[recv_size..][0..chunk_size],
                );
                recv_state.advance(chunk_size);
                recv_size += chunk_size;
            }

            if (recv_state.message_size == null) {
                if (recv_state.peek_header()) |header| {
                    const header_decrypted: vsr.Header.Prepare = std.mem.bytesAsValue(
                        vsr.Header.Prepare,
                        std.mem.asBytes(&header),
                    ).*;
                    if (header_decrypted.header_key_id != 0 or
                        header_decrypted.header_nonce != 0 or
                        header_decrypted.body_nonce != 0 or
                        !header_decrypted.valid_checksum())
                    {
                        assert(fault);
                        continue :blk;
                    }
                    recv_state.message_size = header_decrypted.size;
                }
            }

            if (recv_state.pop_message()) |message| {
                const header_decrypted: vsr.Header.Prepare = std.mem.bytesAsValue(
                    vsr.Header.Prepare,
                    message[0..@sizeOf(HeaderEncrypted)],
                ).*;

                if (!header_decrypted.valid_checksum_body(message[@sizeOf(HeaderEncrypted)..])) {
                    assert(fault);
                    continue :blk;
                }

                const header_expected = headers.ordered_remove(0);

                try std.testing.expectEqual(header_expected, header_decrypted);

                recv_state.copy_left();
                header_last = null;
            }
        }
        assert(!fault);
    }
}
