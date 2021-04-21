const std = @import("std");

const config = @import("config.zig");
pub const log_level: std.log.Level = @intToEnum(std.log.Level, config.log_level);

const cli = @import("cli.zig");
const IO = @import("io.zig").IO;
const Client = @import("client.zig").Client;
const MessageBus = @import("message_bus.zig").MessageBus;

pub fn main() !void {
    var arena_allocator = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena_allocator.deinit();
    const allocator = &arena_allocator.allocator;

    const args = cli.parse_args();

    const id = 123;

    var io = try IO.init(32, 0);
    var message_bus: MessageBus = undefined;
    var client = try Client.init(
        allocator,
        id,
        args.cluster,
        args.configuration,
        &message_bus,
    );
    try message_bus.init(allocator, &io, args.cluster, args.configuration, .{ .client = &client });

    while (true) {
        client.tick();
        try io.run_for_ns(config.tick_ms * std.time.ns_per_ms);
    }
}
