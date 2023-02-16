const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const panic = std.debug.panic;
const log = std.log;
pub const log_level: std.log.Level = .err;

const constants = @import("constants.zig");
const stdx = @import("stdx.zig");
const random_int_exponential = @import("testing/fuzz.zig").random_int_exponential;
const IO = @import("io.zig").IO;
const Storage = @import("storage.zig").Storage;
const MessagePool = @import("message_pool.zig").MessagePool;
const MessageBus = @import("message_bus.zig").MessageBusClient;
const StateMachine = @import("state_machine.zig").StateMachineType(Storage, .{
    .message_body_size_max = constants.message_body_size_max,
});
const RingBuffer = @import("ring_buffer.zig").RingBuffer;
const vsr = @import("vsr.zig");
const Client = vsr.Client(StateMachine, MessageBus);
const tb = @import("tigerbeetle.zig");

const transfer_count_per_batch = @divExact(
    constants.message_size_max - @sizeOf(vsr.Header),
    @sizeOf(tb.Transfer),
);
comptime {
    assert(transfer_count_per_batch >= 2041);
}

var accounts = [_]tb.Account{
    .{
        .id = 1,
        .user_data = 0,
        .reserved = [_]u8{0} ** 48,
        .ledger = 2,
        .code = 1,
        .flags = .{},
        .debits_pending = 0,
        .debits_posted = 0,
        .credits_pending = 0,
        .credits_posted = 0,
    },
    .{
        .id = 2,
        .user_data = 0,
        .reserved = [_]u8{0} ** 48,
        .ledger = 2,
        .code = 1,
        .flags = .{},
        .debits_pending = 0,
        .debits_posted = 0,
        .credits_pending = 0,
        .credits_posted = 0,
    },
};

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();
    const stderr = std.io.getStdErr().writer();

    if (builtin.mode != .ReleaseSafe and builtin.mode != .ReleaseFast) {
        try stderr.print("Benchmark must be built as ReleaseSafe for reasonable results.\n", .{});
    }

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    var transfer_count: usize = 10_000_000;
    var transfer_count_per_second: usize = 1_000_000;

    var args = std.process.args();

    // Discard executable name.
    _ = try args.next(allocator).?;

    // Parse arguments.
    while (args.next(allocator)) |arg_or_err| {
        const arg = try arg_or_err;
        if (std.mem.eql(u8, arg, "--transfer-count")) {
            const int_string_or_err = args.next(allocator) orelse
                panic("Expected an argument to --transfer-count", .{});
            const int_string = try int_string_or_err;

            transfer_count = std.fmt.parseInt(usize, int_string, 10) catch |err|
                panic(
                "Could not parse \"{}\" as an integer: {}",
                .{ std.zig.fmtEscapes(int_string), err },
            );
        } else if (std.mem.eql(u8, arg, "--transfer-count-per-second")) {
            const int_string_or_err = args.next(allocator) orelse
                panic("Expected an argument to --transfer-count-per-second", .{});
            const int_string = try int_string_or_err;

            transfer_count_per_second = std.fmt.parseInt(usize, int_string, 10) catch |err|
                panic(
                "Could not parse \"{}\" as an integer: {}",
                .{ std.zig.fmtEscapes(int_string), err },
            );
        } else {
            std.debug.panic("Unrecognized argument: \"{}\"", .{std.zig.fmtEscapes(arg)});
        }
    }

    const transfer_arrival_rate_ns = @divTrunc(
        std.time.ns_per_s,
        transfer_count_per_second,
    );

    const client_id = std.crypto.random.int(u128);
    const cluster_id: u32 = 0;
    var address = [_]std.net.Address{try std.net.Address.parseIp4("127.0.0.1", constants.port)};

    var io = try IO.init(32, 0);
    defer io.deinit();

    var message_pool = try MessagePool.init(allocator, .client);
    defer message_pool.deinit(allocator);

    var client = try Client.init(
        allocator,
        client_id,
        cluster_id,
        @intCast(u8, address.len),
        &message_pool,
        .{
            .configuration = address[0..],
            .io = &io,
        },
    );
    defer client.deinit(allocator);

    try send(
        &io,
        &client,
        .create_accounts,
        std.mem.sliceAsBytes(accounts[0..]),
    );

    var rng = std.rand.DefaultPrng.init(42);
    const random = rng.random();

    var timer = try std.time.Timer.start();

    var batch_latency_ns = try std.ArrayList(u64).initCapacity(allocator, transfer_count);
    var transfer_latency_ns = try std.ArrayList(u64).initCapacity(allocator, transfer_count);

    // Submit all batches.
    var batch_transfers = try std.ArrayList(tb.Transfer).initCapacity(allocator, transfer_count_per_batch);
    var transfer_start_ns = try std.ArrayList(u64).initCapacity(allocator, transfer_count_per_batch);
    var batch_index: usize = 0;
    var transfer_index: usize = 0;
    var transfer_next_arrival_ns = timer.read();
    while (transfer_index < transfer_count) {
        const batch_start_ns = timer.read();

        // Fill batch.
        while (transfer_index < transfer_count and
            batch_transfers.items.len < transfer_count_per_batch and
            transfer_next_arrival_ns < batch_start_ns)
        {
            batch_transfers.appendAssumeCapacity(.{
                .id = transfer_index + 1,
                .debit_account_id = accounts[0].id,
                .credit_account_id = accounts[1].id,
                .user_data = 0,
                .reserved = 0,
                .pending_id = 0,
                .timeout = 0,
                .ledger = 2,
                .code = 1,
                .flags = .{},
                .amount = 1,
                .timestamp = 0,
            });
            transfer_start_ns.appendAssumeCapacity(transfer_next_arrival_ns);

            transfer_index += 1;
            transfer_next_arrival_ns += random_int_exponential(random, u64, transfer_arrival_rate_ns);
        }

        if (batch_transfers.items.len == 0) continue;

        // Submit batch.
        try send(
            &io,
            &client,
            .create_transfers,
            std.mem.sliceAsBytes(batch_transfers.items),
        );

        // Record latencies.
        const batch_end_ns = timer.read();
        log.debug("batch {}: {} tx in {} ms\n", .{
            batch_index,
            batch_transfers.items.len,
            @divTrunc(batch_end_ns - batch_start_ns, std.time.ns_per_ms),
        });
        batch_latency_ns.appendAssumeCapacity(batch_end_ns - batch_start_ns);
        for (transfer_start_ns.items) |start_ns| {
            transfer_latency_ns.appendAssumeCapacity(batch_end_ns - start_ns);
        }

        // Reset.
        batch_index += 1;
        try batch_transfers.resize(0);
        try transfer_start_ns.resize(0);
    }

    const total_ns = timer.read();

    const less_than_ns = (struct {
        fn lessThan(_: void, a: u64, b: u64) bool {
            return a < b;
        }
    }).lessThan;
    std.sort.sort(u64, batch_latency_ns.items, {}, less_than_ns);
    std.sort.sort(u64, transfer_latency_ns.items, {}, less_than_ns);

    try stdout.print("{} batches in {d:2} s\n", .{
        batch_index,
        @intToFloat(f64, total_ns) / std.time.ns_per_s,
    });
    try stdout.print("offered load = {} tx/s\n", .{
        transfer_count_per_second,
    });
    try stdout.print("accepted load = {} tx/s\n", .{
        @divTrunc(
            transfer_count * std.time.ns_per_s,
            total_ns,
        ),
    });
    try stdout.print("batch latency p100 = {} ms\n", .{
        @divTrunc(
            batch_latency_ns.items[batch_latency_ns.items.len - 1],
            std.time.ns_per_ms,
        ),
    });
    try stdout.print("transfer latency p100 = {} ms\n", .{
        @divTrunc(
            transfer_latency_ns.items[transfer_latency_ns.items.len - 1],
            std.time.ns_per_ms,
        ),
    });
}

fn send(
    io: *IO,
    client: *Client,
    operation: StateMachine.Operation,
    payload: []u8,
) !void {
    const message = client.get_message();
    defer client.unref(message);

    stdx.copy_disjoint(
        .inexact,
        u8,
        message.buffer[@sizeOf(vsr.Header)..],
        payload,
    );

    var result: ?(Client.Error![]const u8) = null;

    client.request(
        @intCast(u128, @ptrToInt(&result)),
        send_complete,
        operation,
        message,
        payload.len,
    );

    while (result == null) {
        client.tick();
        try io.run_for_ns(5 * std.time.ns_per_ms);
    }

    const result_payload = result.? catch |err|
        panic("Client returned error: {}", .{err});

    switch (operation) {
        .create_accounts => {
            const create_accounts_results = std.mem.bytesAsSlice(
                tb.CreateAccountsResult,
                result_payload,
            );
            if (create_accounts_results.len > 0) {
                panic("CreateAccountsResults: {any}", .{create_accounts_results});
            }
        },
        .create_transfers => {
            const create_transfers_results = std.mem.bytesAsSlice(
                tb.CreateTransfersResult,
                result_payload,
            );
            if (create_transfers_results.len > 0) {
                panic("CreateTransfersResults: {any}", .{create_transfers_results});
            }
        },
        else => unreachable,
    }
}

fn send_complete(
    user_data: u128,
    operation: StateMachine.Operation,
    result: Client.Error![]const u8,
) void {
    _ = operation;

    const result_ptr = @intToPtr(*?@TypeOf(result), @intCast(u64, user_data));
    result_ptr.* = result;
}
