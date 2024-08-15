const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.fuzz_storage);
const Allocator = std.mem.Allocator;
const Address = std.net.Address;

const stdx = @import("stdx.zig");
const vsr = @import("vsr.zig");
const constants = @import("constants.zig");
const IO = @import("testing/io.zig").IO;
const MessageBusType = @import("message_bus.zig").MessageBusType;
const MessagePool = @import("message_pool.zig").MessagePool;
const Message = MessagePool.Message;
const Header = vsr.Header;
const fuzz = @import("testing/fuzz.zig");

fn random_message(
    random: std.Random,
    buffer: *align(constants.sector_size) [constants.message_size_max]u8,
) void {
    random.bytes(buffer[0..@sizeOf(Header)]);
    const header = std.mem.bytesAsValue(Header, buffer[0..@sizeOf(Header)]);

    header.cluster = 0;
    header.command = random.enumValue(vsr.Command);
    header.size = random.intRangeLessThan(u32, @sizeOf(Header), @intCast(buffer.len));

    const body = buffer[@sizeOf(Header)..header.size];
    random.bytes(body);

    header.set_checksum_body(body);
    header.set_checksum();
}

pub const Fuzzer = struct {
    const Options = struct {
        seed: u64,
        replicas: []const Address,
        clients: usize,
    };

    allocator: Allocator,
    io: *IO,
    prng: std.rand.DefaultPrng,

    replicas: std.ArrayListUnmanaged(*NodeReplica),
    clients: std.ArrayListUnmanaged(*NodeClient),

    pub fn create(
        allocator: Allocator,
        io: *IO,
        options: Options,
    ) !*Fuzzer {
        const fuzzer = try allocator.create(Fuzzer);

        fuzzer.* = .{
            .allocator = allocator,
            .io = io,
            .prng = std.rand.DefaultPrng.init(options.seed),

            .replicas = try std.ArrayListUnmanaged(*NodeReplica).initCapacity(
                allocator,
                options.replicas.len,
            ),
            .clients = try std.ArrayListUnmanaged(*NodeClient).initCapacity(
                allocator,
                options.clients,
            ),
        };

        for (0..options.replicas.len) |id| {
            const replica = fuzzer.replicas.addOneAssumeCapacity();
            replica.* = try NodeReplica.create(fuzzer, @intCast(id), options.replicas);
        }

        for (0..options.clients) |id_minus_one| {
            const client = fuzzer.clients.addOneAssumeCapacity();
            client.* = try NodeClient.create(fuzzer, id_minus_one + 1, options.replicas);
        }

        return fuzzer;
    }

    pub fn destroy(fuzzer: *Fuzzer) void {
        for (fuzzer.replicas.items) |replica| {
            replica.destroy(fuzzer.allocator);
        }
        fuzzer.replicas.deinit(fuzzer.allocator);

        for (fuzzer.clients.items) |client| {
            client.destroy(fuzzer.allocator);
        }
        fuzzer.clients.deinit(fuzzer.allocator);

        fuzzer.allocator.destroy(fuzzer);
    }

    pub fn tick(fuzzer: *Fuzzer) !void {
        for (fuzzer.replicas.items) |replica| {
            replica.message_bus.tick();
        }

        for (fuzzer.clients.items) |client| {
            client.message_bus.tick();
        }

        try fuzzer.io.tick();
    }
};

pub const NodeReplica = Node(.replica);
pub const NodeClient = Node(.client);

pub fn Node(comptime process_type: vsr.ProcessType) type {
    return struct {
        const Context = @This();
        const MessageBus = MessageBusType(process_type, IO);

        const Id = switch (process_type) {
            .replica => u8,
            .client => u128,
        };

        fuzzer: *Fuzzer,
        message_pool: MessagePool,
        message_bus: MessageBus,

        fn create(
            fuzzer: *Fuzzer,
            id: Id,
            configuration: []const Address,
        ) !*@This() {
            const allocator = fuzzer.allocator;
            const io = fuzzer.io;

            const node = try allocator.create(@This());

            node.* = .{
                .fuzzer = fuzzer,
                .message_pool = try MessagePool.init(allocator, switch (process_type) {
                    .replica => .{
                        .replica = .{
                            .members_count = @intCast(configuration.len),
                            .pipeline_requests_limit = 0,
                        },
                    },
                    .client => .client,
                }),
                .message_bus = try MessageBus.init(
                    allocator,
                    0,
                    switch (process_type) {
                        .replica => .{ .replica = id },
                        .client => .{ .client = id },
                    },
                    &node.message_pool,
                    &on_message,
                    .{
                        .io = io,
                        .configuration = configuration,
                        .clients_limit = switch (process_type) {
                            .replica => 16,
                            .client => null,
                        },
                    },
                ),
            };

            return node;
        }

        fn destroy(node: *@This(), allocator: std.mem.Allocator) void {
            node.message_bus.deinit(allocator);
            node.message_pool.deinit(allocator);
            node.fuzzer.allocator.destroy(node);
        }

        fn on_message(message_bus: *MessageBus, message: *Message) void {
            _ = message; // autofix
            var context: *Context = @fieldParentPtr("message_bus", message_bus);
            _ = &context;
        }
    };
}

pub fn main(args: fuzz.FuzzArgs) !void {
    // var prng = std.Random.DefaultPrng.init(args.seed);
    const allocator = fuzz.allocator;

    var io = IO.init(allocator, &.{}, .{});
    var fuzzer = try Fuzzer.create(allocator, &io, .{
        .seed = args.seed,
        .replicas = &.{
            try Address.parseIp4("127.0.0.1", 3000),
            try Address.parseIp4("127.0.0.1", 3001),
            try Address.parseIp4("127.0.0.1", 3002),
            try Address.parseIp4("127.0.0.1", 3003),
        },
        .clients = 16,
    });
    defer fuzzer.destroy();

    for (0..10) |_| {
        try fuzzer.tick();
    }

    // const configuration: []const Address = &.{
    //     try Address.parseIp4("127.0.0.1", 3000),
    // };

    // var replica = try FuzzContext(.replica).create(allocator, &io, .{
    //     .id = 0,
    //     .configuration = configuration,
    // });
    // defer replica.destroy(allocator);

    // var client = try FuzzContext(.client).create(allocator, &io, .{
    //     .id = 1,
    //     .configuration = configuration,
    // });
    // defer client.destroy(allocator);

    // while (client.message_bus.connections_used < configuration.len) {
    //     try client.tick();
    //     try replica.tick();
    // }

    // const message = client.message_bus.get_message(null);
    // defer client.message_bus.unref(message);

    // random_message(prng.random(), message.buffer);

    // std.log.info("{any}", .{message.into_any()});

    // message.header.* = (Header.PingClient{
    //     .command = .ping_client,
    //     .cluster = 0,
    //     .release = release,
    //     .client = client.id,
    // }).frame_const().*;

    // message.header.set_checksum_body(message.body());
    // message.header.set_checksum();

    // client.message_bus.send_message_to_replica(0, message.ref());

    // for (0..20) |_| {
    //     try client.tick();
    //     try replica.tick();
    // }
}
