const std = @import("std");
const assert = std.debug.assert;

const config = @import("config.zig");

const tb = @import("tigerbeetle.zig");
const Account = tb.Account;
const Transfer = tb.Transfer;

const CreateAccountsResult = tb.CreateAccountsResult;
const CreateTransfersResult = tb.CreateTransfersResult;

const util = @import("util.zig");
const IO = @import("io.zig").IO;
const Storage = @import("storage.zig").Storage;
const MessagePool = @import("message_pool.zig").MessagePool;
const MessageBus = @import("message_bus.zig").MessageBusClient;
const StateMachine = @import("state_machine.zig").StateMachineType(Storage, .{
    .message_body_size_max = config.message_body_size_max,
});

const vsr = @import("vsr.zig");
const Header = vsr.Header;
const Client = vsr.Client(StateMachine, MessageBus);

pub const log_level: std.log.Level = .alert;

pub fn request(
    operation: StateMachine.Operation,
    batch: anytype,
    on_reply: fn (
        user_data: u128,
        operation: StateMachine.Operation,
        results: Client.Error![]const u8,
    ) void,
) !void {
    const allocator = std.heap.page_allocator;
    const client_id = std.crypto.random.int(u128);
    const cluster_id: u32 = 0;
    var addresses = [_]std.net.Address{try std.net.Address.parseIp4("127.0.0.1", config.port)};

    var io = try IO.init(32, 0);
    defer io.deinit();

    var message_pool = try MessagePool.init(allocator, .client);
    defer message_pool.deinit(allocator);

    var client = try Client.init(
        allocator,
        client_id,
        cluster_id,
        @intCast(u8, addresses.len),
        &message_pool,
        .{
            .configuration = &addresses,
            .io = &io,
        },
    );
    defer client.deinit(allocator);

    const message = client.get_message();
    defer client.unref(message);

    const body = std.mem.asBytes(&batch);
    util.copy_disjoint(.inexact, u8, message.buffer[@sizeOf(Header)..], body);

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

pub fn on_create_accounts(
    user_data: u128,
    operation: StateMachine.Operation,
    results: Client.Error![]const u8,
) void {
    _ = user_data;
    _ = operation;

    print_results(CreateAccountsResult, results);
}

pub fn on_lookup_accounts(
    user_data: u128,
    operation: StateMachine.Operation,
    results: Client.Error![]const u8,
) void {
    _ = user_data;
    _ = operation;

    print_results(Account, results);
}

pub fn on_lookup_transfers(
    user_data: u128,
    operation: StateMachine.Operation,
    results: Client.Error![]const u8,
) void {
    _ = user_data;
    _ = operation;

    print_results(Transfer, results);
}

pub fn on_create_transfers(
    user_data: u128,
    operation: StateMachine.Operation,
    results: Client.Error![]const u8,
) void {
    _ = user_data;
    _ = operation;

    print_results(CreateTransfersResult, results);
}

fn print_results(comptime Results: type, results: Client.Error![]const u8) void {
    const body = results catch unreachable;
    const slice = std.mem.bytesAsSlice(Results, body);
    for (slice) |result| {
        std.debug.print("{}\n", .{result});
    }
    if (slice.len == 0) std.debug.print("OK\n", .{});
}
