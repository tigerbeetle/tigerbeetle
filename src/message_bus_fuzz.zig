const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.message_bus_fuzz);

const vsr = @import("vsr.zig");
const constants = vsr.constants;
const stdx = vsr.stdx;
const MessageBus = vsr.message_bus.MessageBusReplica;
const MessagePool = vsr.message_pool.MessagePool;
const MessageBuffer = @import("./message_buffer.zig").MessageBuffer;
const IO = @import("io.zig").IO;
const fuzz = @import("testing/fuzz.zig");
const ratio = stdx.PRNG.ratio;

/// NOTE: In practice this fuzzer should be *mostly* deterministic, but it is using real network IO.
pub fn main(gpa: std.mem.Allocator, args: fuzz.FuzzArgs) !void {
    const events_max = args.events_max orelse 1_000;
    var prng = stdx.PRNG.from_seed(args.seed);

    const replica_count = 3;
    const clients_limit = 2;
    const tick_message_bus_probability = ratio(1, 4);
    const restart_probability = ratio(1, 50);

    const configuration = &.{
        // TODO avoid port collisions in CFO. Maybe as a CLI arg?
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

    var ios = try gpa.alloc(IO, replica_count);
    defer gpa.free(ios);

    for (ios, 0..) |*io, i| {
        errdefer for (ios[0..i]) |*o| o.deinit();
        io.* = try IO.init(32, 0);
    }
    defer for (ios) |*io| io.deinit();

    var message_buses = try gpa.alloc(MessageBus, replica_count);
    defer gpa.free(message_buses);

    for (message_buses, ios, 0..) |*message_bus, *io, i| {
        errdefer for (message_buses[0..i]) |*b| b.deinit(gpa);

        message_bus.* = try MessageBus.init(
            gpa,
            .{ .replica = @intCast(i) },
            &message_pool,
            on_messages_callback,
            .{ .configuration = configuration, .io = io, .clients_limit = clients_limit },
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
        const message_body_size =
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

        message_buses[replica_send].send_message_to_replica(replica_receive, message);

        for (message_buses, ios, 0..) |*message_bus, *io, i| {
            if (prng.chance(tick_message_bus_probability)) {
                message_bus.tick();
            }

            if (prng.chance(restart_probability)) {
                log.debug("{}: restart", .{i});

                io.cancel_all();
                message_bus.deinit(gpa);
                io.deinit();

                io.* = IO.init(32, 0) catch unreachable;
                message_bus.* = MessageBus.init(
                    gpa,
                    .{ .replica = @intCast(i) },
                    &message_pool,
                    on_messages_callback,
                    .{ .configuration = configuration, .io = io, .clients_limit = clients_limit },
                ) catch unreachable;
            }

            try io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
        }
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
