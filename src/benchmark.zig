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
    .lsm_batch_multiple = constants.lsm_batch_multiple,
});
const RingBuffer = @import("ring_buffer.zig").RingBuffer;
const vsr = @import("vsr.zig");
const Client = vsr.Client(StateMachine, MessageBus);
const tb = @import("tigerbeetle.zig");

const account_count_per_batch = @divExact(
    constants.message_size_max - @sizeOf(vsr.Header),
    @sizeOf(tb.Account),
);
const transfer_count_per_batch = @divExact(
    constants.message_size_max - @sizeOf(vsr.Header),
    @sizeOf(tb.Transfer),
);

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();
    const stderr = std.io.getStdErr().writer();

    if (builtin.mode != .ReleaseSafe and builtin.mode != .ReleaseFast) {
        try stderr.print("Benchmark must be built as ReleaseSafe for reasonable results.\n", .{});
    }

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    var account_count: usize = 10_000;
    var transfer_count: usize = 10_000_000;
    var transfer_count_per_second: usize = 1_000_000;

    var args = std.process.args();

    // Discard executable name.
    _ = try args.next(allocator).?;

    // Parse arguments.
    while (args.next(allocator)) |arg_or_err| {
        const arg = try arg_or_err;
        _ = (try parse_arg(allocator, &args, arg, "--account-count", &account_count)) or
            (try parse_arg(allocator, &args, arg, "--transfer-count", &transfer_count)) or
            (try parse_arg(allocator, &args, arg, "--transfer-count-per-second", &transfer_count_per_second)) or
            panic("Unrecognized argument: \"{}\"", .{std.zig.fmtEscapes(arg)});
    }

    if (account_count < 2) panic("Need at least two acconts, got {}", .{account_count});

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

    // Init accounts.
    var batch_accounts = try std.ArrayList(tb.Account).initCapacity(allocator, account_count_per_batch);
    var account_index: usize = 0;
    while (account_index < account_count) {
        // Fill batch.
        while (account_index < account_count and
            batch_accounts.items.len < account_count_per_batch)
        {
            batch_accounts.appendAssumeCapacity(.{
                .id = @bitReverse(u128, account_index + 1),
                .user_data = 0,
                .reserved = [_]u8{0} ** 48,
                .ledger = 2,
                .code = 1,
                .flags = .{},
                .debits_pending = 0,
                .debits_posted = 0,
                .credits_pending = 0,
                .credits_posted = 0,
            });
            account_index += 1;
        }

        // Submit batch.
        try send(
            &io,
            &client,
            .create_accounts,
            std.mem.sliceAsBytes(batch_accounts.items),
        );

        // Reset.
        try batch_accounts.resize(0);
    }

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
            const debit_account_index = random.uintLessThan(u64, account_count);
            var credit_account_index = random.uintLessThan(u64, account_count);
            if (debit_account_index == credit_account_index) {
                credit_account_index = (credit_account_index + 1) % account_count;
            }
            assert(debit_account_index != credit_account_index);
            batch_transfers.appendAssumeCapacity(.{
                // Reverse the bits to stress non-append-only index for `id`.
                .id = @bitReverse(u128, transfer_index + 1),
                .debit_account_id = @bitReverse(u128, debit_account_index + 1),
                .credit_account_id = @bitReverse(u128, credit_account_index + 1),
                .user_data = random.int(u128),
                .reserved = 0,
                // TODO Benchmark posting/voiding pending transfers.
                .pending_id = 0,
                .timeout = 0,
                .ledger = 2,
                .code = random.int(u16) +| 1,
                .flags = .{},
                .amount = random_int_exponential(random, u64, 10_000) +| 1,
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

    try stdout.print("{} batches in {d:.2} s\n", .{
        batch_index,
        @intToFloat(f64, total_ns) / std.time.ns_per_s,
    });
    try stdout.print("load offered = {} tx/s\n", .{
        transfer_count_per_second,
    });
    try stdout.print("load accepted = {} tx/s\n", .{
        @divTrunc(
            transfer_count * std.time.ns_per_s,
            total_ns,
        ),
    });
    try print_deciles(stdout, "batch", batch_latency_ns.items);
    try print_deciles(stdout, "transfer", transfer_latency_ns.items);
}

fn parse_arg(
    allocator: std.mem.Allocator,
    args: *std.process.ArgIterator,
    arg: []const u8,
    arg_name: []const u8,
    arg_value: *usize,
) !bool {
    if (!std.mem.eql(u8, arg, arg_name)) return false;

    const int_string_or_err = args.next(allocator) orelse
        panic("Expected an argument to {s}", .{arg_name});
    const int_string = try int_string_or_err;
    arg_value.* = std.fmt.parseInt(usize, int_string, 10) catch |err|
        panic(
        "Could not parse \"{}\" as an integer: {}",
        .{ std.zig.fmtEscapes(int_string), err },
    );
    return true;
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

    var done = false;

    client.request(
        @intCast(u128, @ptrToInt(&done)),
        send_complete,
        operation,
        message,
        payload.len,
    );

    while (!done) {
        client.tick();
        try io.run_for_ns(5 * std.time.ns_per_ms);
    }
}

fn send_complete(
    user_data: u128,
    operation: StateMachine.Operation,
    result: Client.Error![]const u8,
) void {
    _ = operation;

    const result_payload = result catch |err|
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

    const done = @intToPtr(*bool, @intCast(u64, user_data));
    done.* = true;
}

fn print_deciles(
    stdout: anytype,
    label: []const u8,
    latencies: []const u64,
) !void {
    var decile: usize = 0;
    while (decile <= 10) : (decile += 1) {
        const index = @divTrunc(latencies.len * decile, 10) -| 1;
        try stdout.print("{s} latency p{}0 = {} ms\n", .{
            label,
            decile,
            @divTrunc(
                latencies[index],
                std.time.ns_per_ms,
            ),
        });
    }
}
