const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.fuzz_storage);

const stdx = @import("stdx.zig");
const vsr = @import("vsr.zig");
const constants = @import("constants.zig");
const IO = @import("testing/io.zig").IO;
const MessageBusType = @import("message_bus.zig").MessageBusType;
const MessagePool = @import("message_pool.zig").MessagePool;
const Message = MessagePool.Message;
const Header = vsr.Header;
const fuzz = @import("testing/fuzz.zig");

pub fn FuzzContext(comptime process_type: vsr.ProcessType) type {
    return struct {
        const Context = @This();
        const MessageBus = MessageBusType(process_type, IO);

        const Id = switch (process_type) {
            .replica => u8,
            .client => u128,
        };

        id: Id,
        io: *IO,
        message_pool: MessagePool,
        message_bus: MessageBus,

        fn create(
            allocator: std.mem.Allocator,
            id: Id,
            io: *IO,
            configuration: []const std.net.Address,
        ) !*Context {
            const context = try allocator.create(Context);

            context.* = .{
                .id = id,
                .io = io,
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
                    &context.message_pool,
                    &on_message,
                    .{
                        .io = context.io,
                        .configuration = configuration,
                        .clients_limit = switch (process_type) {
                            .replica => 16,
                            .client => null,
                        },
                    },
                ),
            };

            return context;
        }

        fn destroy(context: *Context, allocator: std.mem.Allocator) void {
            allocator.destroy(context);
        }

        fn tick(context: *Context) !void {
            context.message_bus.tick();
            try context.io.tick();
        }

        fn on_message(message_bus: *MessageBus, message: *Message) void {
            _ = message_bus; // autofix
            // var context: *Context = @fieldParentPtr("message_bus", message_bus);
            // _ = context; // autofix

            std.log.info("{any}", .{message});
        }
    };
}

pub fn main(args: fuzz.FuzzArgs) !void {
    _ = args; // autofix
    const allocator = fuzz.allocator;

    var io = IO.init(allocator, &.{}, .{});

    const configuration: []const std.net.Address = &.{
        try std.net.Address.parseIp4("127.0.0.1", 3000),
    };

    var replica = try FuzzContext(.replica).create(allocator, 0, &io, configuration);
    defer replica.destroy(allocator);

    var client = try FuzzContext(.client).create(allocator, 1, &io, configuration);
    defer client.destroy(allocator);

    for (0..20) |_| {
        try client.tick();
        try replica.tick();
    }

    const message = client.message_bus.get_message(null);
    defer client.message_bus.unref(message);

    message.header.* = (Header.PingClient{
        .command = .ping_client,
        .cluster = 0,
        .release = .{ .value = 0 },
        .client = client.id,
    }).frame_const().*;

    message.header.set_checksum_body(message.body());
    message.header.set_checksum();

    client.message_bus.send_message_to_replica(0, message.ref());

    for (0..20) |_| {
        try client.tick();
        try replica.tick();
    }
}
