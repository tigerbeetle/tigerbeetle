//! Fuzz message bus.
//!
//! Here's how it works:
//! 1. Generate ping messages, ping_client messages, and many random non-ping messages.
//! 2. Each of the non-ping messages has an intended source and destination.
//! 3. Until all of those non-ping messages are successfully delivered to their intended
//!    destinations:
//!    - nodes try to deliver their undelivered messages,
//!    - replicas and clients send pings and ping_clients (respectively), to ensure peer
//!      identification,
//!    - replica nodes occasionally send ping_clients, to test peer misidentification handling.
const std = @import("std");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const log = std.log.scoped(.message_bus_fuzz);

const vsr = @import("vsr.zig");
const constants = vsr.constants;
const stdx = vsr.stdx;
const MessageBus = vsr.message_bus.MessageBusType(IO);
const MessagePool = vsr.message_pool.MessagePool;
const Message = MessagePool.Message;
const MessageBuffer = @import("./message_buffer.zig").MessageBuffer;
const fuzz = @import("testing/fuzz.zig");
const ratio = stdx.PRNG.ratio;
const Ratio = stdx.PRNG.Ratio;

pub fn main(gpa: std.mem.Allocator, args: fuzz.FuzzArgs) !void {
    const messages_max = args.events_max orelse 200;
    // Usually we don't need nearly this many ticks, but certain combinations of errors can make
    // delivering messages quite time consuming.
    const ticks_max = messages_max * 10_000;

    var prng = stdx.PRNG.from_seed(args.seed);

    const replica_count = 3;
    const clients_limit = 2;
    const node_count = replica_count + clients_limit;
    const message_bus_send_probability = ratio(prng.range_inclusive(u64, 1, 10), 10);
    const message_bus_tick_probability = ratio(prng.range_inclusive(u64, 3, 10), 10);
    // Ping often so that listener→connector connections are eventually identified correctly.
    // (Otherwise those messages could stall forever with "no connection to..." errors).
    // Note that this probability is conditional on sending a message.
    const message_bus_ping_probability = ratio(prng.range_inclusive(u64, 2, 5), 10);
    // Occasionally inject an incorrect ping, causing the connection peer type to be misidentified.
    // Note that this probability is conditional on sending a ping.
    const message_bus_ping_misdirect_probability = ratio(prng.range_inclusive(u64, 0, 3), 10);

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
        .recv_after_shutdown_probability = ratio(prng.int_inclusive(u64, 10), 10),
        .send_partial_probability = ratio(prng.int_inclusive(u64, 5), 10),
        .send_corrupt_probability = ratio(prng.int_inclusive(u64, 10), 100),
        .send_error_probability = ratio(prng.int_inclusive(u64, 1), 10),
        .send_now_probability = ratio(prng.int_inclusive(u64, 10), 10),
        .send_after_shutdown_probability = ratio(prng.int_inclusive(u64, 10), 10),
        .close_error_probability = ratio(prng.int_inclusive(u64, 2), 10),
        .shutdown_error_probability = ratio(prng.int_inclusive(u64, 2), 10),
        .accept_error_probability = ratio(prng.int_inclusive(u64, 2), 10),
        .connect_error_probability = ratio(prng.int_inclusive(u64, 2), 10),
    });
    defer io.deinit();

    // Mapping from message checksum → intended message destination.
    var messages_pending = std.AutoArrayHashMapUnmanaged(u128, MessagePending).empty;
    defer messages_pending.deinit(gpa);

    var nodes = try gpa.alloc(Node, replica_count + clients_limit);
    defer gpa.free(nodes);

    for (nodes[0..replica_count], 0..) |*node, i| {
        errdefer for (nodes[0..i]) |*n| n.message_bus.deinit(gpa);

        node.* = .{
            .prng = &prng,
            .options = .swarm(&prng),
            .messages_pending = &messages_pending,
            .message_bus = try MessageBus.init(
                gpa,
                .{ .replica = @intCast(i) },
                &message_pool,
                Node.on_messages_callback,
                .{ .configuration = configuration, .io = &io, .clients_limit = clients_limit },
            ),
            .id = @intCast(i),
        };
        try node.message_bus.listen();
    }
    defer for (nodes[0..replica_count]) |*node| node.message_bus.deinit(gpa);

    for (nodes[replica_count..], 0..) |*node, i| {
        errdefer for (nodes[replica_count..][0..i]) |*n| n.message_bus.deinit(gpa);

        node.* = .{
            .prng = &prng,
            .options = .swarm(&prng),
            .messages_pending = &messages_pending,
            .message_bus = try MessageBus.init(
                gpa,
                .{ .client = replica_count + i },
                &message_pool,
                Node.on_messages_callback,
                .{ .configuration = configuration, .io = &io, .clients_limit = null },
            ),
            .id = @intCast(replica_count + i),
        };
    }
    defer for (nodes[replica_count..]) |*node| node.message_bus.deinit(gpa);

    // Allocate extra for the pings and ping_clients, which are not counted by messages_max since we
    // don't track their delivery. (Pings/ping_clients are used to identify (or misidentify!) the
    // peer type of nodes).
    var messages = try std.ArrayListAlignedUnmanaged(
        [constants.message_size_max]u8,
        constants.sector_size,
    ).initCapacity(gpa, messages_max + node_count);
    defer messages.deinit(gpa);

    for (0..replica_count) |replica| {
        const message = messages.addOneAssumeCapacity();
        const message_header: *vsr.Header =
            @alignCast(std.mem.bytesAsValue(vsr.Header, message[0..@sizeOf(vsr.Header)]));
        message_header.* = random_header(&prng, .ping);
        message_header.replica = @intCast(replica);
        message_header.set_checksum_body(message[@sizeOf(vsr.Header)..message_header.size]);
        message_header.set_checksum();

        assert(std.meta.eql(message_header.peer_type(), .{ .replica = @intCast(replica) }));
    }

    for (0..clients_limit) |i| {
        const client = replica_count + i;
        const message = messages.addOneAssumeCapacity();
        const message_header: *vsr.Header =
            @alignCast(std.mem.bytesAsValue(vsr.Header, message[0..@sizeOf(vsr.Header)]));
        message_header.* = random_header(&prng, .ping_client);
        message_header.into(.ping_client).?.client = client;
        message_header.set_checksum_body(message[@sizeOf(vsr.Header)..message_header.size]);
        message_header.set_checksum();

        assert(std.meta.eql(message_header.peer_type(), .{ .client = client }));
    }

    for (0..messages_max) |_| {
        const message = messages.addOneAssumeCapacity();
        const message_header: *vsr.Header =
            @alignCast(std.mem.bytesAsValue(vsr.Header, message[0..@sizeOf(vsr.Header)]));
        const node_source: u8 = @intCast(prng.index(nodes));

        const message_body_size: u32 =
            @min(fuzz.random_int_exponential(&prng, u32, 256), constants.message_body_size_max);
        const message_body = message[@sizeOf(vsr.Header)..][0..message_body_size];

        message_header.* = random_header(&prng, .reserved);
        message_header.size += message_body_size;
        prng.fill(message_body);

        // invalid() is only checked by replica, not message bus, so we can mostly ignore the
        // command-specific header data. However, we need to keep it valid enough for peer_type() to
        // be useful, otherwise the "replicas" will never actually connect.
        if (nodes[node_source].message_bus.process == .replica) {
            message_header.replica = node_source;
            message_header.command = prng.enum_weighted(vsr.Command, command_weights);
            if (message_header.into(.request)) |request_header| {
                request_header.client = @max(1, fuzz.random_int_exponential(&prng, u128, 3));
            }
        } else {
            message_header.replica = @intCast(prng.index(nodes[0..replica_count]));
            message_header.command = .request;
            message_header.into(.request).?.client = node_source;
        }
        message_header.set_checksum_body(message[@sizeOf(vsr.Header)..message_header.size]);
        message_header.set_checksum();

        const node_target: u8 = if (nodes[node_source].message_bus.process == .replica)
            random_node(&prng, message_header.replica, node_count)
        else
            @intCast(prng.index(nodes[0..replica_count]));
        try messages_pending.putNoClobber(gpa, message_header.checksum, .{
            .buffer = message[0..message_header.size],
            .source = node_source,
            .target = node_target,
        });
    }

    for (0..ticks_max) |_| {
        if (messages_pending.count() == 0) break;

        if (prng.chance(message_bus_send_probability)) {
            const message = message_pool.get_message(.reserved).base();
            defer message_pool.unref(message);

            if (prng.chance(message_bus_ping_probability)) {
                const node_index = prng.index(nodes);
                stdx.copy_disjoint(.inexact, u8, message.buffer, &messages.items[node_index]);
                assert(message.header.command == .ping or message.header.command == .ping_client);

                const target: u8 = if (message.header.command == .ping)
                    random_node(&prng, message.header.replica, node_count)
                else
                    @intCast(prng.index(nodes[0..replica_count]));

                if (message.header.command == .ping_client and
                    prng.chance(message_bus_ping_misdirect_probability))
                {
                    nodes[random_node(&prng, target, node_count)].send_message(target, message);
                } else {
                    nodes[node_index].send_message(target, message);
                }
            } else {
                const message_pending_index = prng.index(messages_pending.keys());
                const message_pending = &messages_pending.values()[message_pending_index];
                stdx.copy_disjoint(.inexact, u8, message.buffer, message_pending.buffer);

                nodes[message_pending.source].send_message(message_pending.target, message);
            }
        }

        for (nodes) |*node| {
            if (prng.chance(message_bus_tick_probability)) {
                if (node.message_bus.process == .replica) {
                    node.message_bus.tick();
                } else {
                    node.message_bus.tick_client();
                }
            }
            if (prng.chance(node.options.message_resume_probability)) {
                node.message_bus.resume_receive();
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

fn random_header(prng: *stdx.PRNG, command: vsr.Command) vsr.Header {
    return .{
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
        .replica = 0,
        .reserved_frame = @splat(0),
        .reserved_command = @splat(0),
    };
}

fn random_node(prng: *stdx.PRNG, node_exclude: u8, node_count: u8) u8 {
    return (node_exclude + prng.range_inclusive(u8, 1, node_count - 1)) % node_count;
}

const NodeOptions = struct {
    message_suspend_probability: Ratio,
    message_resume_probability: Ratio,

    pub fn swarm(prng: *stdx.PRNG) NodeOptions {
        var result: NodeOptions = .{
            .message_suspend_probability = ratio(prng.int_inclusive(u64, 5), 10),
            .message_resume_probability = ratio(prng.range_inclusive(u64, 0, 10), 10),
        };

        if (result.message_suspend_probability.numerator > 0) {
            result.message_resume_probability = ratio(prng.range_inclusive(u64, 2, 10), 10);
            assert(result.message_resume_probability.numerator > 0);
        }

        return result;
    }
};

const Node = struct {
    prng: *stdx.PRNG,
    options: NodeOptions,
    messages_pending: *std.AutoArrayHashMapUnmanaged(u128, MessagePending),
    message_bus: MessageBus,
    id: u8,

    fn send_message(node: *Node, target: u8, message: *Message) void {
        if (target < node.message_bus.replicas_addresses.len) {
            node.message_bus.send_message_to_replica(target, message);
        } else {
            assert(node.message_bus.process == .replica);
            node.message_bus.send_message_to_client(target, message);
        }
    }

    fn on_messages_callback(message_bus: *MessageBus, buffer: *MessageBuffer) void {
        const node: *Node = @fieldParentPtr("message_bus", message_bus);
        while (buffer.next_header()) |header| {
            assert(header.valid_checksum());

            if (node.prng.chance(node.options.message_suspend_probability)) {
                buffer.suspend_message(&header);
                continue;
            }

            const message = buffer.consume_message(message_bus.pool, &header);
            defer message_bus.unref(message);

            assert(stdx.equal_bytes(vsr.Header, &header, message.header));
            assert(message.header.valid_checksum_body(message.body_used()));

            if (node.messages_pending.get(message.header.checksum)) |message_pending| {
                const message_pending_checksum =
                    std.mem.bytesAsValue(u128, message_pending.buffer[0..@sizeOf(u128)]).*;
                assert(message_pending_checksum == message.header.checksum);

                if (message_pending.target == node.id) {
                    const message_removed =
                        node.messages_pending.swapRemove(message.header.checksum);
                    assert(message_removed);
                } else {
                    // We aren't the intended recipient of this message.
                    //
                    // One of the following is true:
                    // - We are a replica that was misidentified as a client, either due to:
                    //   - accidental misidentification due to a request we sent, or
                    //   - deliberate misidentification a ping_client we sent.
                    // - We are a client that was misidentified as a *different* client, due to
                    //   a "misdirected" ping_client we sent.
                }
            } else {
                // Ignore duplicate messages.
            }
        }
    }
};

const MessagePending = struct {
    buffer: []const u8,
    source: u8,
    target: u8,
};

const IO = struct {
    gpa: std.mem.Allocator,
    prng: stdx.PRNG,
    options: Options,

    servers: std.AutoArrayHashMapUnmanaged(socket_t, SocketServer) = .{},
    connections: std.AutoArrayHashMapUnmanaged(socket_t, SocketConnection) = .{},
    events: EventQueue,
    ticks: u64 = 0,
    fd_next: socket_t = 1,
    /// Count the number of fds/sockets which have been created but not closed.
    /// Unlike `connection.closed`, this is tracking whether sockets were explicitly closed by the
    /// message bus.
    fds_open: u32 = 0,

    const posix = std.posix;
    // We can't specify io/linux.zig since it won't compile on windows, which means that we are
    // potentially using different error sets for different OS's, which means that fuzzer seeds are
    // only reproducible on the same OS.
    const RealIO = @import("io.zig").IO;
    pub const AcceptError = RealIO.AcceptError;
    pub const CloseError = RealIO.CloseError;
    pub const ConnectError = RealIO.ConnectError;
    pub const RecvError = RealIO.RecvError;
    pub const SendError = RealIO.SendError;
    pub const TimeoutError = RealIO.TimeoutError;
    pub const socket_t = i32;
    pub const fd_t = i32;
    const EventQueue = std.PriorityQueue(Event, void, Event.less_than);

    pub const Options = struct {
        seed: u64 = 0,
        recv_partial_probability: Ratio,
        recv_error_probability: Ratio,
        recv_after_shutdown_probability: Ratio,
        send_partial_probability: Ratio,
        send_corrupt_probability: Ratio,
        send_error_probability: Ratio,
        send_now_probability: Ratio,
        send_after_shutdown_probability: Ratio,
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
        context: *MessageBus,
        callback: union(enum) {
            accept: *const fn (*MessageBus, *Completion, AcceptError!socket_t) void,
            close: *const fn (*MessageBus, *Completion, CloseError!void) void,
            connect: *const fn (*MessageBus, *Completion, ConnectError!void) void,
            recv: *const fn (*MessageBus, *Completion, RecvError!usize) void,
            send: *const fn (*MessageBus, *Completion, SendError!usize) void,
            timeout: *const fn (*MessageBus, *Completion, TimeoutError!void) void,
        },
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
        assert(io.fds_open == 0);

        for (io.connections.values()) |*connection| {
            assert(connection.closed);
            connection.sending.deinit(io.gpa);
        }

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
                    completion.callback.accept(completion.context, completion, result);
                    return .done;
                }

                const server = io.servers.getPtr(operation.socket).?;
                if (server.backlog.items.len == 0) return .retry;

                const local_fd = io.fd_next;
                io.fd_next += 1;
                io.fds_open += 1;

                const remote_completion =
                    server.backlog.swapRemove(io.prng.index(server.backlog.items));
                const remote_fd = remote_completion.operation.connect.socket;

                try io.connections.putNoClobber(io.gpa, local_fd, .{ .remote = remote_fd });
                try io.connections.putNoClobber(io.gpa, remote_fd, .{ .remote = local_fd });

                const result_accept: AcceptError!socket_t = local_fd;
                const result_connect: ConnectError!void = {};

                completion.callback.accept(
                    completion.context,
                    completion,
                    result_accept,
                );
                remote_completion.callback.connect(
                    remote_completion.context,
                    remote_completion,
                    result_connect,
                );
            },
            .close => |operation| {
                const local = io.connections.getPtr(operation.fd) orelse {
                    // close() was called but we didn't ever connect.
                    // (This happens when we injected an error into connect()).
                    const result: CloseError!void = {};
                    completion.callback.close(completion.context, completion, result);
                    return .done;
                };
                assert(local.closed);

                if (io.prng.chance(io.options.close_error_probability)) {
                    const result: CloseError!void = io.prng.error_uniform(CloseError);
                    completion.callback.close(completion.context, completion, result);
                } else {
                    const result: CloseError!void = {};
                    completion.callback.close(completion.context, completion, result);
                }
            },
            .connect => |operation| {
                if (io.prng.chance(io.options.connect_error_probability)) {
                    const result: ConnectError!void = io.prng.error_uniform(ConnectError);
                    completion.callback.connect(completion.context, completion, result);
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
                    completion.callback.connect(completion.context, completion, result);
                    return .done;
                }
            },
            .send => |operation| {
                const sender = io.connections.getPtr(operation.socket).?;
                assert(!sender.closed);
                assert(sender.pending_send);
                sender.pending_send = false;

                if (io.prng.chance(io.options.send_error_probability)) {
                    const result: SendError!usize = io.prng.error_uniform(SendError);
                    completion.callback.send(completion.context, completion, result);

                    // If there is a pending recv(), we need to make sure that it fails (rather than
                    // potentially stalling and preventing MessageBus from calling shutdown/close).
                    sender.closed = true;
                    return .done;
                }

                if (sender.shutdown_send) {
                    if (io.prng.chance(io.options.send_after_shutdown_probability)) {
                        // Sometimes allow the send() to complete even after shutdown().
                    } else {
                        const result: SendError!usize = error.BrokenPipe;
                        completion.callback.send(completion.context, completion, result);
                        return .done;
                    }
                }

                const send_size = if (io.prng.chance(io.options.send_partial_probability))
                    io.prng.range_inclusive(usize, 0, operation.buffer.len)
                else
                    operation.buffer.len;

                const send_buffer = operation.buffer[0..send_size];
                try sender.sending.appendSlice(gpa, send_buffer);

                if (io.prng.chance(io.options.send_corrupt_probability)) {
                    if (send_buffer.len == 0) {
                        try sender.sending.append(gpa, io.prng.int(u8));
                    } else {
                        const corrupt_byte = io.prng.index(send_buffer);
                        sender.sending.items[sender.sending.items.len - corrupt_byte - 1] ^=
                            io.prng.bit(u8);
                    }
                }

                const result: SendError!usize = send_size;
                completion.callback.send(completion.context, completion, result);
            },
            .recv => |operation| {
                const receiver = io.connections.getPtr(operation.socket).?;
                assert(receiver.pending_recv);
                // If we had a send() fail, then we must avoid requeueing this operation.
                maybe(receiver.closed);

                if (receiver.closed or io.prng.chance(io.options.recv_error_probability)) {
                    const result: RecvError!usize = io.prng.error_uniform(RecvError);
                    completion.callback.recv(completion.context, completion, result);
                    return .done;
                }

                if (receiver.shutdown_recv) {
                    if (io.prng.chance(io.options.recv_after_shutdown_probability)) {
                        // Sometimes allow the recv() to complete even after shutdown().
                    } else {
                        const result: RecvError!usize = 0;
                        completion.callback.recv(completion.context, completion, result);
                        return .done;
                    }
                }

                const sender_fd = receiver.remote orelse return .retry;
                const sender = io.connections.getPtr(sender_fd).?;
                if (sender.closed) {
                    const result: RecvError!usize = error.ConnectionResetByPeer;
                    completion.callback.recv(completion.context, completion, result);
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
                        completion.callback.recv(completion.context, completion, result);
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
                    completion.callback.recv(completion.context, completion, result);
                }
            },
            .timeout => {
                const result: TimeoutError!void = {};
                completion.callback.timeout(completion.context, completion, result);
            },
        }
        return .done;
    }

    pub fn open_socket_tcp(io: *IO, _: u32, _: RealIO.TCPOptions) !socket_t {
        const socket = io.fd_next;
        io.fd_next += 1;
        io.fds_open += 1;
        return @intCast(socket);
    }

    pub fn listen(
        io: *IO,
        fd: socket_t,
        address: std.net.Address,
        _: RealIO.ListenOptions,
    ) !std.net.Address {
        io.servers.putNoClobber(io.gpa, fd, .{ .address = address }) catch @panic("OOM");
        return address;
    }

    pub fn close_socket(io: *IO, socket: socket_t) void {
        io.fds_open -= 1;

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
            .callback = .{ .accept = callback },
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
        // There are no pending operations on this socket.
        var events = io.events.iterator();
        while (events.next()) |event| {
            switch (event.completion.operation) {
                inline .accept, .connect, .recv, .send => |data| assert(data.socket != fd),
                .close => |data| assert(data.fd != fd),
                .timeout => {},
            }
        }

        io.fds_open -= 1;
        if (io.connections.getPtr(fd)) |connection| {
            connection.closed = true;
        } else {
            // We might be closing a socket which didn't ever connect().
        }

        completion.* = .{
            .context = context,
            .callback = .{ .close = callback },
            .operation = .{ .close = .{ .fd = fd } },
        };
        io.enqueue(completion);
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
            .callback = .{ .connect = callback },
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
            .callback = .{ .recv = callback },
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
            .callback = .{ .send = callback },
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

        sender.sending.appendSlice(io.gpa, buffer[0..send_size]) catch @panic("OOM");
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
            .callback = .{ .timeout = callback },
            .operation = .timeout,
        };

        const jitter_mean = 1_000;
        const jitter = fuzz.random_int_exponential(&io.prng, u64, jitter_mean);
        io.events.add(.{
            .completion = completion,
            .ready_at = io.tick_instant().add(.{ .ns = (nanoseconds -| jitter_mean) + jitter }),
        }) catch @panic("OOM");
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
        }) catch @panic("OOM");
    }
};
