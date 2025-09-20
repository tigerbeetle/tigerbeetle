//! Fuzz message bus.
//!
//! Here's how it works:
//! 1. Generate ping messages, ping_client messages, and then a bunch of random non-ping messages.
//! 2. Each of the non-ping messages has an intended source and destination.
//! 3. Until all of those messages are delivered, nodes try to deliver their undelivered messages.
const std = @import("std");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const log = std.log.scoped(.message_bus_fuzz);

const vsr = @import("vsr.zig");
const constants = vsr.constants;
const stdx = vsr.stdx;
const MessageBus = vsr.message_bus.MessageBusType(IO, .replica);
const MessagePool = vsr.message_pool.MessagePool;
const MessageBuffer = @import("./message_buffer.zig").MessageBuffer;
const fuzz = @import("testing/fuzz.zig");
const ratio = stdx.PRNG.ratio;
const Ratio = stdx.PRNG.Ratio;

/// TODO Fuzz client MessageBus too.
/// TODO Test suspend/resume.
/// TODO If we remove type-level distinction between MessageBusReplica and MessageBusClient, then we
/// can simplify erase_types()/Context handling in the fuzzer's IO implementation to avoid some
/// `*any opaque`s.
pub fn main(gpa: std.mem.Allocator, args: fuzz.FuzzArgs) !void {
    const messages_max = args.events_max orelse 100;
    // Usually we don't need nearly this many ticks, but certain combinations of errors can make
    // delivering messages quite time consuming.
    const ticks_max = messages_max * 32_000;

    var prng = stdx.PRNG.from_seed(args.seed);

    const replica_count = 3;
    const clients_limit = 2;
    const message_bus_send_probability = ratio(prng.range_inclusive(u64, 1, 10), 10);
    const message_bus_tick_probability = ratio(prng.range_inclusive(u64, 2, 10), 10);
    // Ping often so that listener→connector connections are eventually identified correctly.
    // (Otherwise those messages could stall forever with "no connection to..." errors).
    // Note that this probability is conditional on sending a message.
    const message_bus_ping_replica_probability = ratio(prng.range_inclusive(u64, 2, 5), 10);
    // Occasionally inject a ping_client, causing the connection peer type to be misidentified.
    const message_bus_ping_client_probability = ratio(prng.range_inclusive(u64, 0, 3), 10);

    const configuration = &.{
        try std.net.Address.parseIp4("127.0.0.1", 3000),
        try std.net.Address.parseIp4("127.0.0.1", 3001),
        try std.net.Address.parseIp4("127.0.0.1", 3002),
    };

    const command_weights = weights: {
        var command_weights = fuzz.random_enum_weights(&prng, vsr.Command);
        // The message bus asserts that no message has command=reserved.
        // Move the weight (rather than just zeroing it) so that we can't end up with total=0.
        command_weights.prepare += command_weights.reserved;
        // ping/ping_client are handled separately since they are used to identify/misidentify the
        // connection.
        command_weights.prepare += command_weights.ping;
        command_weights.prepare += command_weights.ping_client;

        command_weights.reserved = 0;
        command_weights.ping = 0;
        command_weights.ping_client = 0;
        // requests are interesting since they may originate from both clients and replicas.
        command_weights.request *= 20;
        break :weights command_weights;
    };

    inline for (std.meta.fields(@TypeOf(command_weights))) |field| {
        log.info("command weight: {s} = {}", .{ field.name, @field(command_weights, field.name) });
    }

    var message_pool = try MessagePool.init_capacity(gpa, 128);
    defer message_pool.deinit(gpa);

    var io = try IO.init(gpa, .{
        .seed = prng.int(u64),
        .recv_partial_probability = ratio(prng.int_inclusive(u64, 10), 10),
        .recv_error_probability = ratio(prng.int_inclusive(u64, 1), 10),
        .send_partial_probability = ratio(prng.int_inclusive(u64, 10), 10),
        .send_corrupt_probability = ratio(prng.int_inclusive(u64, 10), 100),
        .send_error_probability = ratio(prng.int_inclusive(u64, 1), 10),
        .send_now_probability = ratio(prng.int_inclusive(u64, 10), 10),
        .close_error_probability = ratio(prng.int_inclusive(u64, 2), 10),
        .shutdown_error_probability = ratio(prng.int_inclusive(u64, 2), 10),
        .accept_error_probability = ratio(prng.int_inclusive(u64, 2), 10),
        .connect_error_probability = ratio(prng.int_inclusive(u64, 2), 10),
    });
    defer io.deinit();

    // Mapping from message checksum → intended message destination.
    var messages_pending = std.AutoArrayHashMapUnmanaged(u128, MessagePending).empty;
    defer messages_pending.deinit(gpa);

    var nodes = try gpa.alloc(Node, replica_count);
    defer gpa.free(nodes);

    for (nodes, 0..) |*node, i| {
        errdefer for (nodes[0..i]) |*n| n.message_bus.deinit(gpa);

        node.* = .{
            .messages_pending = &messages_pending,
            .message_bus = try MessageBus.init(
                gpa,
                .{ .replica = @intCast(i) },
                &message_pool,
                on_messages_callback,
                .{ .configuration = configuration, .io = &io, .clients_limit = clients_limit },
            ),
            .replica = @intCast(i),
        };
    }
    defer for (nodes) |*node| node.message_bus.deinit(gpa);

    const messages = try gpa.alignedAlloc(
        [constants.message_size_max]u8,
        constants.sector_size,
        // Allocate extra for the pings and ping_clients, which are not part of messages_max.
        messages_max + replica_count * 2,
    );
    defer gpa.free(messages);

    for (messages, 0..) |*message, i| {
        const message_header = std.mem.bytesAsValue(vsr.Header, message[0..@sizeOf(vsr.Header)]);

        if (i < replica_count) {
            message_header.* = random_header(&prng, .ping, @intCast(i));
        } else if (i < 2 * replica_count) {
            message_header.* = random_header(&prng, .ping_client, @intCast(i - replica_count));
        } else {
            const replica_send: u8 = @intCast(prng.index(nodes));

            const message_command = prng.enum_weighted(vsr.Command, command_weights);
            const message_body_size: u32 =
                @min(fuzz.random_int_exponential(&prng, u32, 256), constants.message_body_size_max);
            const message_body = message[@sizeOf(vsr.Header)..][0..message_body_size];

            message_header.* = random_header(&prng, message_command, replica_send);
            message_header.size += message_body_size;
            prng.fill(message_body);
        }
        message_header.set_checksum_body(message[@sizeOf(vsr.Header)..message_header.size]);
        message_header.set_checksum();

        if (message_header.command != .ping and
            message_header.command != .ping_client)
        {
            const replica_receive: u8 =
                (message_header.replica + prng.range_inclusive(u8, 1, replica_count - 1)) %
                replica_count;
            assert(replica_receive != message_header.replica);
            assert(replica_receive < replica_count);
            try messages_pending.putNoClobber(gpa, message_header.checksum, .{
                .buffer = message[0..message_header.size],
                .target = replica_receive,
            });
        }
    }

    for (0..ticks_max) |_| {
        if (messages_pending.count() == 0) break;

        if (prng.chance(message_bus_send_probability)) {
            const message = message_pool.get_message(.reserved).base();
            defer message_pool.unref(message);

            var target: ?u8 = null;
            if (prng.chance(message_bus_ping_replica_probability)) {
                const pings = messages[0..replica_count];
                const ping_index = prng.index(pings);
                stdx.copy_disjoint(.inexact, u8, message.buffer, &pings[ping_index]);
                assert(message.header.command == .ping);

                target = (message.header.replica + prng.range_inclusive(u8, 1, replica_count - 1)) %
                    replica_count;
            } else if (prng.chance(message_bus_ping_client_probability)) {
                const ping_clients = messages[replica_count..][0..replica_count];
                const ping_index = prng.index(ping_clients);
                stdx.copy_disjoint(.inexact, u8, message.buffer, &ping_clients[ping_index]);
                assert(message.header.command == .ping_client);

                target = (message.header.replica + prng.range_inclusive(u8, 1, replica_count - 1)) %
                    replica_count;
            } else {
                const message_pending_index = prng.index(messages_pending.keys());
                const message_pending = &messages_pending.values()[message_pending_index];
                stdx.copy_disjoint(.inexact, u8, message.buffer, message_pending.buffer);

                target = message_pending.target;
            }
            nodes[message.header.replica].message_bus.send_message_to_replica(target.?, message);
        }

        for (nodes) |*node| {
            if (prng.chance(message_bus_tick_probability)) {
                node.message_bus.tick();
            }
        }
        try io.run();
    } else {
        std.debug.panic("only {}/{} messages delivered", .{
            messages_max - messages_pending.count(),
            messages_max,
        });
    }
    assert(messages_pending.count() == 0);

    log.info("Passed!", .{});
}

fn random_header(prng: *stdx.PRNG, command: vsr.Command, replica: u8) vsr.Header {
    var header = vsr.Header{
        .checksum = 0,
        .checksum_padding = 0,
        .checksum_body = 0,
        .checksum_body_padding = 0,
        .nonce_reserved = 0,
        .cluster = prng.int(u128), // MessageBus doesn't check cluster.
        .size = @sizeOf(vsr.Header),
        .epoch = 0,
        .view = prng.int(u32),
        .release = vsr.Release.zero,
        .protocol = vsr.Version,
        .command = command,
        .replica = replica,
        .reserved_frame = @splat(0),
        .reserved_command = @splat(0),
    };
    // invalid() is only checked by replica, not message bus, so we can mostly ignore the
    // command-specific header data. However, we need to keep it valid enough for peer_type() to
    // be useful, otherwise the "replicas" will never actually connect.
    switch (command) {
        inline else => |command_comptime| {
            const command_header = header.into(command_comptime).?;
            if (@hasField(@TypeOf(command_header.*), "client")) {
                // set_and_verify_peer asserts a nonzero client id.
                command_header.client = @max(1, fuzz.random_int_exponential(prng, u128, 3));
            }
        },
    }
    return header;
}

const Node = struct {
    messages_pending: *std.AutoArrayHashMapUnmanaged(u128, MessagePending),
    message_bus: MessageBus,
    replica: u8,
};

const MessagePending = struct {
    buffer: []const u8,
    target: u8,
};

fn on_messages_callback(message_bus: *MessageBus, buffer: *MessageBuffer) void {
    const node: *Node = @fieldParentPtr("message_bus", message_bus);
    while (buffer.next_header()) |header| {
        const message = buffer.consume_message(message_bus.pool, &header);
        defer message_bus.unref(message);

        assert(message.header.valid_checksum());
        assert(message.header.valid_checksum_body(message.body_used()));

        if (node.messages_pending.get(message.header.checksum)) |message_pending| {
            const message_checksum =
                std.mem.bytesAsValue(u128, message_pending.buffer[0..@sizeOf(vsr.Header)]).*;
            assert(message_checksum == message.header.checksum);
            assert(message_pending.target == node.replica);

            const message_removed = node.messages_pending.swapRemove(message.header.checksum);
            assert(message_removed);
        } else {
            // Ignore duplicate messages.
        }

        log.debug("{}: received {s}", .{ message_bus.process.replica, @tagName(header.command) });
    }
}

const IO = struct {
    gpa: std.mem.Allocator,
    prng: stdx.PRNG,
    options: Options,

    servers: std.AutoArrayHashMapUnmanaged(socket_t, SocketServer) = .{},
    connections: std.AutoArrayHashMapUnmanaged(socket_t, SocketConnection) = .{},
    /// Current number of events with event.completion.operation == .close
    /// (We cache this separately since it is used in deinit(), so the Completion references are
    /// invalid.)
    connections_closing: u32 = 0,
    events: EventQueue,
    ticks: u64 = 0,
    fd: socket_t = 1,

    const posix = std.posix;
    const RealIO = @import("io.zig").IO;
    pub const AcceptError = RealIO.AcceptError;
    pub const CloseError = RealIO.CloseError;
    pub const ConnectError = RealIO.ConnectError;
    pub const RecvError = RealIO.RecvError;
    pub const SendError = RealIO.SendError;
    pub const TimeoutError = RealIO.TimeoutError;
    pub const socket_t = RealIO.socket_t;
    pub const fd_t = RealIO.fd_t;
    pub const INVALID_SOCKET = RealIO.INVALID_SOCKET;
    const EventQueue = std.PriorityQueue(Event, void, Event.less_than);

    pub const Options = struct {
        seed: u64 = 0,
        recv_partial_probability: Ratio,
        recv_error_probability: Ratio,
        send_partial_probability: Ratio,
        send_corrupt_probability: Ratio,
        send_error_probability: Ratio,
        send_now_probability: Ratio,
        close_error_probability: Ratio,
        shutdown_error_probability: Ratio,
        accept_error_probability: Ratio,
        connect_error_probability: Ratio,
    };

    const SocketServer = struct {
        address: std.net.Address,
        /// Invariant: completion.operation == .connect
        backlog: std.ArrayListUnmanaged(*Completion) = .empty,
    };

    const SocketConnection = struct {
        shutdown_recv: bool = false,
        shutdown_send: bool = false,
        /// There is at most one send() and at most one recv() pending per socket.
        pending_send: bool = false,
        pending_recv: bool = false,

        closed: bool = false,
        remote: ?socket_t,
        sending: std.ArrayListUnmanaged(u8) = .empty,
        sending_offset: u32 = 0,
    };

    const Event = struct {
        ready_at: stdx.Instant,
        completion: *Completion,

        fn less_than(_: void, a: Event, b: Event) std.math.Order {
            return std.math.order(a.ready_at.ns, b.ready_at.ns);
        }
    };

    const Operation = union(enum) {
        accept: struct { socket: socket_t },
        close: struct { fd: fd_t },
        connect: struct { socket: socket_t, address: std.net.Address },
        recv: struct { socket: socket_t, buffer: []u8 },
        send: struct { socket: socket_t, buffer: []const u8 },
        timeout,
    };

    pub const Completion = struct {
        context: ?*anyopaque,
        callback: *const fn (
            context: ?*anyopaque,
            completion: *Completion,
            result: *const anyopaque,
        ) void,
        operation: Operation,
    };

    pub fn init(gpa: std.mem.Allocator, options: Options) !IO {
        var events = EventQueue.init(gpa, {});
        errdefer events.deinit();

        return .{
            .gpa = gpa,
            .prng = stdx.PRNG.from_seed(options.seed),
            .options = options,
            .events = events,
        };
    }

    pub fn deinit(io: *IO) void {
        // Servers were already cleaned up by io.close_socket().
        assert(io.servers.count() == 0);

        var connections_open: u32 = 0;
        for (io.connections.values()) |*connection| {
            connections_open += @intFromBool(!connection.closed);
            connection.sending.deinit(io.gpa);
        }
        // "<=" instead of "==" since MessageBus may try to close() a connection which never
        // successfully connected due to a connect() error.
        assert(connections_open <= io.connections_closing);

        io.events.deinit();
        io.connections.deinit(io.gpa);
        io.servers.deinit(io.gpa);
    }

    pub fn run(io: *IO) !void {
        while (try io.step()) {}
        io.ticks += 1;
    }

    fn step(io: *IO) !bool {
        const event_peek = io.events.peek() orelse return false;
        if (event_peek.ready_at.ns <= io.tick_instant().ns) {
            const event = io.events.remove();
            switch (try io.complete(event.completion)) {
                .retry => io.enqueue(event.completion),
                .done => {},
            }
            return true;
        } else {
            return false;
        }
    }

    fn complete(io: *IO, completion: *Completion) !enum { done, retry } {
        const gpa = io.gpa;
        switch (completion.operation) {
            .accept => |operation| {
                if (io.prng.chance(io.options.accept_error_probability)) {
                    const result: AcceptError!socket_t = io.prng.error_uniform(AcceptError);
                    completion.callback(completion.context, completion, &result);
                    return .done;
                }

                const server = io.servers.getPtr(operation.socket).?;
                if (server.backlog.items.len == 0) return .retry;

                const local_fd = io.fd;
                io.fd += 1;

                const remote_completion =
                    server.backlog.swapRemove(io.prng.index(server.backlog.items));
                const remote_fd = remote_completion.operation.connect.socket;

                try io.connections.putNoClobber(io.gpa, local_fd, .{ .remote = remote_fd });
                try io.connections.putNoClobber(io.gpa, remote_fd, .{ .remote = local_fd });

                const result_accept: AcceptError!socket_t = local_fd;
                const result_connect: ConnectError!void = {};

                completion.callback(
                    completion.context,
                    completion,
                    &result_accept,
                );
                remote_completion.callback(
                    remote_completion.context,
                    remote_completion,
                    &result_connect,
                );
            },
            .close => |operation| {
                io.connections_closing -= 1;

                const local = io.connections.getPtr(operation.fd) orelse {
                    // close() was called but we didn't ever connect.
                    // (This happens when we injected an error into connect()).
                    const result: CloseError!void = {};
                    completion.callback(completion.context, completion, &result);
                    return .done;
                };

                local.closed = true;
                if (io.prng.chance(io.options.close_error_probability)) {
                    const result: CloseError!void = io.prng.error_uniform(CloseError);
                    completion.callback(completion.context, completion, &result);
                } else {
                    const result: CloseError!void = {};
                    completion.callback(completion.context, completion, &result);
                }
            },
            .connect => |operation| {
                if (io.prng.chance(io.options.connect_error_probability)) {
                    const result: ConnectError!void = io.prng.error_uniform(ConnectError);
                    completion.callback(completion.context, completion, &result);
                    return .done;
                }

                for (io.servers.values()) |*server| {
                    if (server.address.eql(operation.address)) {
                        try server.backlog.append(gpa, completion);
                        break;
                    }
                } else {
                    // No one listening at that address.
                    const result: ConnectError!void = error.ConnectionRefused;
                    completion.callback(completion.context, completion, &result);
                    return .done;
                }
            },
            .send => |operation| {
                const sender = io.connections.getPtr(operation.socket).?;
                assert(!sender.closed);
                assert(sender.pending_send);

                if (io.prng.chance(io.options.send_error_probability)) {
                    const result: SendError!usize = io.prng.error_uniform(SendError);
                    completion.callback(completion.context, completion, &result);

                    // If there is a pending recv(), we need to make sure that it fails (rather than
                    // potentially stalling and preventing MessageBus from calling shutdown/close).
                    sender.closed = true;
                    return .done;
                }

                const send_size = if (io.prng.chance(io.options.send_partial_probability))
                    io.prng.range_inclusive(usize, 0, operation.buffer.len)
                else
                    operation.buffer.len;

                sender.pending_send = false;
                if (sender.shutdown_send) {
                    const result: SendError!usize = error.BrokenPipe;
                    completion.callback(completion.context, completion, &result);
                } else {
                    const send_buffer = operation.buffer[0..send_size];
                    try sender.sending.appendSlice(gpa, send_buffer);

                    if (io.prng.chance(io.options.send_corrupt_probability)) {
                        if (send_buffer.len == 0) {
                            try sender.sending.append(gpa, io.prng.int(u8));
                        } else {
                            const corrupt_byte = io.prng.index(send_buffer);
                            const corrupt_bit = io.prng.int_inclusive(u3, @bitSizeOf(u8) - 1);
                            sender.sending.items[sender.sending.items.len - corrupt_byte - 1] ^=
                                @as(u8, 1) << corrupt_bit;
                        }
                    }

                    const result: SendError!usize = send_size;
                    completion.callback(completion.context, completion, &result);
                }
            },
            .recv => |operation| {
                const receiver = io.connections.getPtr(operation.socket).?;
                assert(receiver.pending_recv);
                // If we had a send() fail, then we must avoid requeueing this operation.
                maybe(receiver.closed);

                if (receiver.closed or io.prng.chance(io.options.recv_error_probability)) {
                    const result: RecvError!usize = io.prng.error_uniform(RecvError);
                    completion.callback(completion.context, completion, &result);
                    return .done;
                }

                if (receiver.shutdown_recv) {
                    const result: RecvError!usize = 0;
                    completion.callback(completion.context, completion, &result);
                    return .done;
                }

                const sender_fd = receiver.remote orelse return .retry;
                const sender = io.connections.getPtr(sender_fd).?;
                if (sender.closed) {
                    const result: RecvError!usize = error.ConnectionResetByPeer;
                    completion.callback(completion.context, completion, &result);
                    return .done;
                }

                assert(sender.sending_offset <= sender.sending.items.len);
                if (sender.sending_offset == sender.sending.items.len) {
                    if (sender.shutdown_send) {
                        receiver.shutdown_recv = true;
                        receiver.pending_recv = false;
                        // Connection was half-closed, and we have received all the buffered data,
                        // so now it can close.
                        const result: RecvError!usize = 0;
                        completion.callback(completion.context, completion, &result);
                    } else {
                        // Sender is still open, but has nothing to deliver right now.
                        return .retry;
                    }
                } else {
                    const recv_size_max = @min(
                        operation.buffer.len,
                        sender.sending.items.len - sender.sending_offset,
                    );
                    const recv_size = if (io.prng.chance(io.options.recv_partial_probability))
                        io.prng.range_inclusive(u64, 1, recv_size_max)
                    else
                        recv_size_max;
                    assert(recv_size > 0);
                    assert(recv_size <= recv_size_max);

                    stdx.copy_disjoint(
                        .inexact,
                        u8,
                        operation.buffer[0..recv_size],
                        sender.sending.items[sender.sending_offset..][0..recv_size],
                    );
                    sender.sending_offset += @intCast(recv_size);
                    receiver.pending_recv = false;

                    const result: RecvError!usize = recv_size;
                    completion.callback(completion.context, completion, &result);
                }
            },
            .timeout => {
                const result: TimeoutError!void = {};
                completion.callback(completion.context, completion, &result);
            },
        }
        return .done;
    }

    pub fn open_socket_tcp(io: *IO, _: u32, _: RealIO.TCPOptions) !socket_t {
        const socket = io.fd;
        io.fd += 1;
        return @intCast(socket);
    }

    pub fn listen(
        io: *IO,
        fd: socket_t,
        address: std.net.Address,
        _: RealIO.ListenOptions,
    ) !std.net.Address {
        io.servers.putNoClobber(io.gpa, fd, .{ .address = address }) catch unreachable;
        return address;
    }

    pub fn close_socket(io: *IO, socket: socket_t) void {
        if (io.servers.getPtr(socket)) |server| {
            assert(!io.connections.contains(socket));
            server.backlog.deinit(io.gpa);

            const server_removed = io.servers.swapRemove(socket);
            assert(server_removed);
        } else if (io.connections.getPtr(socket)) |connection| {
            assert(!io.servers.contains(socket));

            connection.closed = true;
        }
    }

    pub fn shutdown(io: *IO, socket: socket_t, how: posix.ShutdownHow) posix.ShutdownError!void {
        if (io.prng.chance(io.options.shutdown_error_probability)) {
            return io.prng.error_uniform(posix.ShutdownError);
        } else {
            if (how == .both or how == .recv) io.connections.getPtr(socket).?.shutdown_recv = false;
            if (how == .both or how == .send) io.connections.getPtr(socket).?.shutdown_send = false;
        }
    }

    pub fn accept(
        io: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (Context, *Completion, AcceptError!socket_t) void,
        completion: *Completion,
        socket: socket_t,
    ) void {
        completion.* = .{
            .context = context,
            .callback = erase_types(Context, AcceptError!socket_t, callback),
            .operation = .{ .accept = .{ .socket = socket } },
        };
        io.enqueue(completion);
    }

    pub fn close(
        io: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (Context, *Completion, CloseError!void) void,
        completion: *Completion,
        fd: fd_t,
    ) void {
        completion.* = .{
            .context = context,
            .callback = erase_types(Context, CloseError!void, callback),
            .operation = .{ .close = .{ .fd = fd } },
        };
        io.enqueue(completion);
        io.connections_closing += 1;
    }

    pub fn connect(
        io: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (Context, *Completion, ConnectError!void) void,
        completion: *Completion,
        socket: socket_t,
        address: std.net.Address,
    ) void {
        completion.* = .{
            .context = context,
            .callback = erase_types(Context, ConnectError!void, callback),
            .operation = .{ .connect = .{ .socket = socket, .address = address } },
        };
        io.enqueue(completion);
    }

    pub fn recv(
        io: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (Context, *Completion, RecvError!usize) void,
        completion: *Completion,
        socket: socket_t,
        buffer: []u8,
    ) void {
        const connection = io.connections.getPtr(socket).?;
        assert(!connection.closed);
        assert(!connection.pending_recv);
        connection.pending_recv = true;

        completion.* = .{
            .context = context,
            .callback = erase_types(Context, RecvError!usize, callback),
            .operation = .{ .recv = .{ .socket = socket, .buffer = buffer } },
        };
        io.enqueue(completion);
    }

    pub fn send(
        io: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (Context, *Completion, SendError!usize) void,
        completion: *Completion,
        socket: socket_t,
        buffer: []const u8,
    ) void {
        const connection = io.connections.getPtr(socket).?;
        assert(!connection.closed);
        assert(!connection.pending_send);
        connection.pending_send = true;

        completion.* = .{
            .context = context,
            .callback = erase_types(Context, SendError!usize, callback),
            .operation = .{ .send = .{ .socket = socket, .buffer = buffer } },
        };
        io.enqueue(completion);
    }

    pub fn send_now(io: *IO, socket: socket_t, buffer: []const u8) ?usize {
        const sender = io.connections.getPtr(socket).?;
        assert(!sender.closed);
        assert(!sender.shutdown_send);
        assert(!sender.pending_send);

        if (!io.prng.chance(io.options.send_now_probability)) return null;

        const send_size = if (io.prng.chance(io.options.send_partial_probability))
            io.prng.index(buffer)
        else
            buffer.len;

        sender.sending.appendSlice(io.gpa, buffer[0..send_size]) catch unreachable;
        return send_size;
    }

    pub fn timeout(
        io: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (Context, *Completion, TimeoutError!void) void,
        completion: *Completion,
        nanoseconds: u63,
    ) void {
        completion.* = .{
            .context = context,
            .callback = erase_types(Context, TimeoutError!void, callback),
            .operation = .timeout,
        };

        const jitter_mean = 1_000;
        const jitter = fuzz.random_int_exponential(&io.prng, u64, jitter_mean);
        io.events.add(.{
            .completion = completion,
            .ready_at = io.tick_instant().add(.{ .ns = (nanoseconds -| jitter_mean) + jitter }),
        }) catch unreachable;
    }

    fn tick_instant(io: *const IO) stdx.Instant {
        return .{ .ns = io.ticks * constants.tick_ms * std.time.ns_per_ms };
    }

    fn enqueue(io: *IO, completion: *Completion) void {
        const tick_ns = constants.tick_ms * std.time.ns_per_ms;
        const delay_ns = fuzz.random_int_exponential(&io.prng, u64, 10 * tick_ns);
        io.events.add(.{
            .completion = completion,
            .ready_at = io.tick_instant().add(.{ .ns = delay_ns }),
        }) catch unreachable;
    }

    fn erase_types(
        comptime Context: type,
        comptime Result: type,
        comptime callback: fn (context: Context, completion: *Completion, result: Result) void,
    ) *const fn (?*anyopaque, *Completion, *const anyopaque) void {
        return &struct {
            fn erased(
                ctx_any: ?*anyopaque,
                completion: *Completion,
                result_any: *const anyopaque,
            ) void {
                const ctx: Context = @ptrCast(@alignCast(ctx_any));
                const result: *const Result = @ptrCast(@alignCast(result_any));
                callback(ctx, completion, result.*);
            }
        }.erased;
    }
};
