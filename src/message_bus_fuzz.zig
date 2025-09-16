const builtin = @import("builtin");
const std = @import("std");
const assert = std.debug.assert;
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

/// TODO Fuzz client MessageBus too, or remove type-level distinction.
/// TODO Test suspend/resume.
pub fn main(gpa: std.mem.Allocator, args: fuzz.FuzzArgs) !void {
    const events_max = args.events_max orelse 10_000;
    var prng = stdx.PRNG.from_seed(args.seed);

    const replica_count = 3;
    const clients_limit = 2;
    const message_bus_send_probability = ratio(@max(1, prng.int_inclusive(u64, 10)), 10);
    const message_bus_tick_probability = ratio(@max(1, prng.int_inclusive(u64, 10)), 10);

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
        command_weights.reserved = 0;
        // requests and ping_client are interesting since they are client messages.
        command_weights.request *= 20;
        command_weights.ping_client *= 30;
        break :weights command_weights;
    };

    inline for (std.meta.fields(@TypeOf(command_weights))) |field| {
        log.info("command weight: {s} = {}", .{ field.name, @field(command_weights, field.name) });
    }

    var message_pool = try MessagePool.init_capacity(gpa, 128);
    defer message_pool.deinit(gpa);

    var io = try IO.init(gpa, .{
        .seed = prng.int(u64),
        .recv_partial_probability = ratio(prng.int_inclusive(u64, 100), 100),
        .recv_error_probability = ratio(prng.int_inclusive(u64, 20), 100),
        .send_partial_probability = ratio(prng.int_inclusive(u64, 100), 100),
        .send_corrupt_probability = ratio(prng.int_inclusive(u64, 10), 100),
        .send_error_probability = ratio(prng.int_inclusive(u64, 20), 100),
        .send_now_probability = ratio(prng.int_inclusive(u64, 100), 100),
        .close_error_probability = ratio(prng.int_inclusive(u64, 20), 100),
    });
    defer io.deinit();

    var message_buses = try gpa.alloc(MessageBus, replica_count);
    defer gpa.free(message_buses);

    for (message_buses, 0..) |*message_bus, i| {
        errdefer for (message_buses[0..i]) |*b| b.deinit(gpa);

        message_bus.* = try MessageBus.init(
            gpa,
            .{ .replica = @intCast(i) },
            &message_pool,
            on_messages_callback,
            .{ .configuration = configuration, .io = &io, .clients_limit = clients_limit },
        );
    }
    defer for (message_buses) |*message_bus| message_bus.deinit(gpa);

    for (0..events_max) |_| {
        const replica_send: u8 = @intCast(prng.index(message_buses));
        const replica_receive: u8 =
            (replica_send + 1 + prng.int_inclusive(u8, replica_count - 2)) % replica_count;
        assert(replica_receive != replica_send);
        assert(replica_receive < replica_count);

        const message_command = prng.enum_weighted(vsr.Command, command_weights);
        const message_body_size: u32 =
            @min(fuzz.random_int_exponential(&prng, u32, 256), constants.message_body_size_max);

        var message = message_pool.get_message(.reserved).base();
        defer message_pool.unref(message);

        message.header.* = .{
            .checksum = 0,
            .checksum_padding = 0,
            .checksum_body = 0,
            .checksum_body_padding = 0,
            .nonce_reserved = 0,
            .cluster = 0,
            .size = @sizeOf(vsr.Header) + message_body_size,
            .epoch = 0,
            .view = prng.int(u32),
            .release = vsr.Release.zero,
            .protocol = vsr.Version,
            .command = message_command,
            .replica = replica_send,
            .reserved_frame = @splat(0),
            .reserved_command = @splat(0),
        };
        prng.fill(message.body_used());

        // invalid() is only checked by replica, not message bus, so we can mostly ignore the
        // command-specific header data. However, we need to keep it valid enough for peer_type() to
        // be useful, otherwise the "replicas" will never actually connect.
        switch (message_command) {
            inline else => |command| {
                const header = message.header.into(command).?;
                if (@hasField(@TypeOf(header.*), "client")) {
                    // set_and_verify_peer asserts a nonzero client id.
                    header.client = @max(1, fuzz.random_int_exponential(&prng, u128, 3));
                }
            },
        }
        message.header.set_checksum_body(message.body_used());
        message.header.set_checksum();

        if (prng.chance(message_bus_send_probability)) {
            message_buses[replica_send].send_message_to_replica(replica_receive, message);
        }

        for (message_buses) |*message_bus| {
            if (prng.chance(message_bus_tick_probability)) {
                message_bus.tick();
            }
        }
        try io.run();
    }

    log.info("Passed!", .{});
}

// TODO Check that this receives the messages we expect.
fn on_messages_callback(message_bus: *MessageBus, buffer: *MessageBuffer) void {
    while (buffer.next_header()) |header| {
        const message = buffer.consume_message(message_bus.pool, &header);
        defer message_bus.unref(message);

        log.debug("{}: received {s}", .{
            message_bus.process.replica,
            @tagName(header.command),
        });
    }
}

// TODO Inject connection failures.
const IO = struct {
    gpa: std.mem.Allocator,
    prng: stdx.PRNG,
    options: Options,

    servers: std.AutoArrayHashMapUnmanaged(socket_t, SocketServer) = .{},
    connections: std.AutoArrayHashMapUnmanaged(socket_t, SocketConnection) = .{},
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
    };

    const SocketServer = struct {
        address: std.net.Address,
        /// Invariant: completion.operation == .connect
        backlog: std.ArrayListUnmanaged(*Completion) = .empty,
    };

    const SocketConnection = struct {
        shutdown_recv: bool = false,
        shutdown_send: bool = false,
        closed: bool = false,
        remote: ?socket_t,
        sending: std.ArrayListUnmanaged(u8) = .empty,
        sending_offset: u32 = 0,
        /// There is at most one send() and at most one recv() pending per socket.
        pending_send: bool = false,
        pending_recv: bool = false,
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
        timeout: stdx.Duration,
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
                const local = io.connections.getPtr(operation.fd).?;
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
                if (io.prng.chance(io.options.send_error_probability)) {
                    const result: SendError!usize = io.prng.error_uniform(SendError);
                    completion.callback(completion.context, completion, &result);
                    return .done;
                }

                const send_size = if (io.prng.chance(io.options.send_partial_probability))
                    io.prng.index(operation.buffer)
                else
                    operation.buffer.len;

                const sender = io.connections.getPtr(operation.socket).?;
                assert(!sender.closed);
                assert(sender.pending_send);

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
                if (io.prng.chance(io.options.recv_error_probability)) {
                    const result: RecvError!usize = io.prng.error_uniform(RecvError);
                    completion.callback(completion.context, completion, &result);
                    return .done;
                }

                const receiver = io.connections.getPtr(operation.socket).?;
                assert(!receiver.closed);
                assert(receiver.pending_recv);

                if (receiver.shutdown_recv) {
                    const result: RecvError!usize = 0;
                    completion.callback(completion.context, completion, &result);
                    return .done;
                }

                const sender_fd = receiver.remote orelse return .retry;
                const sender = io.connections.getPtr(sender_fd).?;

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
                        @max(1, io.prng.int_inclusive(u64, recv_size_max))
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
        if (how == .both or how == .recv) io.connections.getPtr(socket).?.shutdown_recv = false;
        if (how == .both or how == .send) io.connections.getPtr(socket).?.shutdown_send = false;
    }

    pub fn accept(
        io: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: AcceptError!socket_t,
        ) void,
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
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: CloseError!void,
        ) void,
        completion: *Completion,
        fd: fd_t,
    ) void {
        completion.* = .{
            .context = context,
            .callback = erase_types(Context, CloseError!void, callback),
            .operation = .{ .close = .{ .fd = fd } },
        };
        io.enqueue(completion);
    }

    pub fn connect(
        io: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: ConnectError!void,
        ) void,
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
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: RecvError!usize,
        ) void,
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
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: SendError!usize,
        ) void,
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
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: TimeoutError!void,
        ) void,
        completion: *Completion,
        nanoseconds: u63,
    ) void {
        completion.* = .{
            .context = context,
            .callback = erase_types(Context, TimeoutError!void, callback),
            .operation = .{ .timeout = .{ .ns = nanoseconds } },
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

    // FIXME randomize distribution per operation type
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
