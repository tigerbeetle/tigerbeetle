const std = @import("std");

const config = @import("config.zig");
pub const log_level: std.log.Level = @intToEnum(std.log.Level, config.log_level);

const cli = @import("cli.zig");
const IO = @import("io.zig").IO;
const Client = @import("client.zig").Client;
const MessageBus = @import("message_bus.zig").MessageBusClient;

pub fn main() !void {
    var arena_allocator = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena_allocator.deinit();
    const allocator = &arena_allocator.allocator;

    const args = cli.parse_args();

    const id = 123;

    var io = try IO.init(32, 0);
    // TODO Use a comptime generator or have MessageBus.init() return self:
    var message_bus: MessageBus = undefined;
    try message_bus.init(allocator, args.cluster, args.configuration, .{ .client = id }, &io);
    var client = try Client.init(
        allocator,
        id,
        args.cluster,
        args.configuration,
        &message_bus,
    );
    message_bus.process = .{ .client = &client };

    while (true) {
        client.tick();
        // We tick IO outside of client so that an IO instance can be shared by multiple clients:
        // Otherwise we will hit io_uring memory restrictions too quickly.
        // TODO client.ts must setInterval(10ms) to call io.tick() and clients.forEach.tick().
        try io.tick();
        std.time.sleep(1000 * std.time.ns_per_ms);
    }
}
