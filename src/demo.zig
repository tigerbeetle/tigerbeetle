const std = @import("std");
const assert = std.debug.assert;

usingnamespace @import("tigerbeetle.zig");

const IO = @import("io.zig").IO;
const MessageBus = @import("message_bus.zig").MessageBusClient;
const StateMachine = @import("state_machine.zig").StateMachine;

const vr = @import("vr.zig");
const Header = vr.Header;
const Client = vr.Client(StateMachine, MessageBus);

pub const Demo = struct {
    pub fn request(
        operation: StateMachine.Operation,
        batch: anytype,
        Results: anytype,
    ) !void {
        const allocator = std.heap.page_allocator;
        const client_id = std.crypto.random.int(u128);
        const cluster_id: u32 = 0;
        var addresses = [_]std.net.Address{try std.net.Address.parseIp4("127.0.0.1", config.port)};

        var io = try IO.init(32, 0);
        defer io.deinit();

        var message_bus = try MessageBus.init(allocator, cluster_id, &addresses, client_id, &io);
        defer message_bus.deinit();

        var client = try Client.init(
            allocator,
            client_id,
            cluster_id,
            @intCast(u8, addresses.len),
            &message_bus,
        );
        defer client.deinit();

        message_bus.set_on_message(*Client, &client, Client.on_message);

        var message = client.get_message() orelse unreachable;
        defer client.unref(message);

        const body = std.mem.asBytes(&batch);
        std.mem.copy(u8, message.buffer[@sizeOf(Header)..], body);
        std.debug.print("ready to send request...\n", .{});

        client.request(
            0,
            on_reply,
            operation,
            message,
            body.len,
        );

        while (client.request_queue.count > 0) {
            client.tick();
            try io.run_for_ns(config.tick_ms * std.time.ns_per_ms);
        }
    }

    pub fn on_reply(user_data: u128, operation: StateMachine.Operation, results: Client.Error![]const u8) void {
        std.debug.print("received reply\n", .{});
        //const reply_body = recv[@sizeOf(Header)..reply_header.size];
        //assert(reply_header.valid_checksum_body(reply_body));

        //for (std.mem.bytesAsSlice(Results, reply_body)) |result| {
        // TODO
        //try result.jsonStringify(.{}, stdout);
        //try stdout.writeAll("\n");
        //    std.debug.print("{}\n", .{result});
        //}
    }
};
