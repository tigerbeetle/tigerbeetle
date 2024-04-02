const std = @import("std");
const builtin = @import("builtin");
const zap = @import("zap");
const assert = std.debug.assert;
const vsr = @import("vsr");
const constants = vsr.constants;
const IO = vsr.io.IO;
const Storage = vsr.storage.Storage;
const StateMachine = vsr.state_machine.StateMachineType(
    Storage,
    constants.state_machine_config,
);
const MessagePool = vsr.message_pool.MessagePool;

const tb = vsr.tigerbeetle;

pub fn WebType(comptime MessageBus: type) type {
    const Client = vsr.Client(StateMachine, MessageBus);

    return struct {
        debug_logs: bool,

        client: *Client,
        printer: vsr.repl.Printer,

        const Web = @This();

        fn debug(web: *const Web, comptime format: []const u8, arguments: anytype) !void {
            if (web.debug_logs) {
                try web.printer.print("[Debug] " ++ format, arguments);
            }
        }

        pub fn run(
            arena: *std.heap.ArenaAllocator,
            addresses: []const std.net.Address,
            cluster_id: u128,
            verbose: bool,
        ) !void {
            const allocator = arena.allocator();

            var web = Web{
                .client = undefined,
                .debug_logs = verbose,
                .printer = .{
                    .stderr = std.io.getStdErr().writer(),
                    .stdout = std.io.getStdOut().writer(),
                },
            };

            try web.debug("Connecting to '{any}'.\n", .{addresses});

            const client_id = std.crypto.random.int(u128);

            var io = try IO.init(32, 0);

            var message_pool = try MessagePool.init(allocator, .client);

            var client = try Client.init(
                allocator,
                client_id,
                cluster_id,
                @intCast(addresses.len),
                &message_pool,
                .{
                    .configuration = addresses,
                    .io = &io,
                },
            );
            web.client = &client;

            try run_zap();
        }

        pub fn run_zap() !void {
            var listener = zap.HttpListener.init(.{
                .port = 3008,
                .on_request = on_request,
                .log = true,
                .max_clients = 100000,
            });
            try listener.listen();

            // web.client.batch_submit();

            std.debug.print("Listening on 0.0.0.0:3008\n", .{});

            zap.start(.{
                .threads = 2,
                .workers = 1, // 1 worker enables sharing state between threads
            });
        }
    };
}

fn on_request(r: zap.Request) void {
    if (r.path) |the_path| {
        std.debug.print("PATH: {s}\n", .{the_path});
    }

    if (r.query) |the_query| {
        std.debug.print("QUERY: {s}\n", .{the_query});
    }

    r.sendBody("<html><body><h1>Hello from ZAP!!!</h1></body></html>") catch return;
}
