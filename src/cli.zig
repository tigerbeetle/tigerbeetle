const std = @import("std");
const builtin = @import("builtin");

const constants = @import("constants.zig");
const stdx = @import("stdx.zig");
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

const Command = enum {
    none,
    create_accounts,
    lookup_accounts,
    create_transfers,
    lookup_transfers,
};

const Config = struct {
    cluster_id: []const u8 = "",
    addresses: []const u8 = "",
    command: Command = .none,
    args: []const u8 = "",
};

fn next_arg(args: std.process.ArgsIterator, flag_name: []const u8) []const u8 {
    if (args.nextPosix()) |arg| {
        return arg;
    }
    
    std.debug.print(
        std.fmt.printAlloc(
            "Expected argument to {}\n",
            .{flag_name},) catch unreachable
    );
    std.os.exit(1);
    unreachable;
}

fn get_config() Config {
    var config = Config{};
    var args = std.process.args();
    while (args.nextPosix()) |arg| {
        if (std.mem.eql(u8, arg, "--cluster_id")) {
            config.cluster_id = next_arg(args, "--cluster_id");
        }

        if (std.mem.eql(u8, arg, "--addresses")) {
            config.addresses = next_arg(args, "--addresses");
        }

        if (std.mem.eql(u8, arg, "--command") or
                std.mem.eql(u8, arg, "-c")) {
            var cmd_string = next_arg(args, "--command/-c");
            if (std.mem.eql(u8, cmd_string, "create_accounts")) {
                config.command = .create_accounts;
            } else if (std.mem.eql(u8, cmd_string, "lookup_accounts")) {
                config.command = .lookup_accounts;
            } else if (std.mem.eql(u8, cmd_string, "create_transfers")) {
                config.command = .create_transfers;
            } else if (std.mem.eql(u8, cmd_string, "lookup_transfers")) {
                config.command = .lookup_transfers;
            } else {
                std.debug.print(
                    std.fmt.printAlloc(
                        allocator,
                        "Expected valid --command. Got '{s}'",
                        .{cmd_string},
                    ) catch unreachable
                );
            }
        }

        if (std.mem.eql(u8, arg, "--args") or
                std.mem.eql(u8, arg, "-a")) {
            config.command = next_arg(args, "--args/-a");
        }
    }

    return config;
}

const Cli = struct {
    io: IO,
    msg_pool: MessagePool,
    client: Client,
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator, conf: Config) !Cli {
        var client = try Client.init(
            allocator,
            std.crypto.random.int(u128), // client_id
            conf.cluster_id,
            @intCast(u8, conf.address.len),
            &message_pool,
            .{
                .configuration = conf.addresses,
                .io = &io,
            },
        );

        return .{
            .allocator = allocator,
            .client = client,
            .msg_pool = try MessagePool.init(allocator, .client),
            .io = try IO.init(32, 0),
        }
    }

    fn deinit(self: Cli) void {
        client.deinit(self.allocator);
        message_pool.deinit(self.allocator);
        io.deinit();
    }

    fn create_accounts(self: Cli, args: []const u8) {
        var accounts = Record.split_records(args);
        var batch_accounts = try std.ArrayList(tb.Account).initCapacity(allocator, accounts.len);
        defer batch_accounts.deinit();

        while (accounts) |account| {
            batch_accounts.appendAssumeCapacity(.{
                .id = account.as_uint128("id"),
                .user_data = account.as_uint128("user_data"),
                .reserved = [_]u8{0} ** 48,
                .ledger = account.as_uint32("ledger"),
                .code = account.as_uint16("code"),
                .flags = account.as_uint16("flags"),
                .debits_pending = 0,
                .debits_posted = 0,
                .credits_pending = 0,
                .credits_posted = 0,
            });
        }
        
        // Submit batch.
        try send(
            &io,
            &client,
            .create_accounts,
            std.mem.sliceAsBytes(batch_accounts.items),
        );
    }

    fn create_transfers(self: Cli, args: []const u8) {
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
    }
}

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();
    const stderr = std.io.getStdErr().writer();

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    const conf = get_config(allocator);
    var cli = cli.init(allocator, conf);
    defer cli.deinit();

    switch (conf.command) {
        .create_accounts => cli.create_accounts(conf.args),
        .lookup_accounts => cli.lookup_accounts(conf.args),
        .create_transfers => cli.create_transfers(conf.args),
        .lookup_transfers => cli.lookup_transfers(conf.args),
    }
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
