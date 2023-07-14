const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const panic = std.debug.panic;
const log = std.log;
pub const log_level: std.log.Level = .info;

const constants = @import("constants.zig");
const stdx = @import("stdx.zig");
const random_int_exponential = @import("testing/fuzz.zig").random_int_exponential;
const IO = @import("io.zig").IO;
const Storage = @import("storage.zig").Storage;
const MessagePool = @import("message_pool.zig").MessagePool;
const MessageBus = @import("message_bus.zig").MessageBusClient;
const StateMachine = @import("state_machine.zig").StateMachineType(Storage, constants.state_machine_config);
const RingBuffer = @import("ring_buffer.zig").RingBuffer;
const vsr = @import("vsr.zig");
const Client = vsr.Client(StateMachine, MessageBus);
const tb = @import("tigerbeetle.zig");
const StatsD = @import("statsd.zig").StatsD;

const account_count_per_batch = @divExact(
    constants.message_size_max - @sizeOf(vsr.Header),
    @sizeOf(tb.Account),
);
const transfer_count_per_batch = @divExact(
    constants.message_size_max - @sizeOf(vsr.Header),
    @sizeOf(tb.Transfer),
);

pub fn main() !void {
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
    var print_batch_timings = false;
    var enable_statsd = false;

    var addresses = try allocator.alloc(std.net.Address, 1);
    addresses[0] = try std.net.Address.parseIp4("127.0.0.1", constants.port);

    // This will either free the above address alloc, or parse_arg_addresses will
    // free and re-alloc internally and this will free that.
    defer allocator.free(addresses);

    var args = std.process.args();

    // Discard executable name.
    _ = try args.next(allocator).?;

    // Parse arguments.
    while (args.next(allocator)) |arg_or_err| {
        const arg = try arg_or_err;
        _ = (try parse_arg_usize(allocator, &args, arg, "--account-count", &account_count)) or
            (try parse_arg_usize(allocator, &args, arg, "--transfer-count", &transfer_count)) or
            (try parse_arg_usize(allocator, &args, arg, "--transfer-count-per-second", &transfer_count_per_second)) or
            (try parse_arg_addresses(allocator, &args, arg, "--addresses", &addresses)) or
            (try parse_arg_bool(allocator, &args, arg, "--print-batch-timings", &print_batch_timings)) or
            (try parse_arg_bool(allocator, &args, arg, "--statsd", &enable_statsd)) or
            panic("Unrecognized argument: \"{}\"", .{std.zig.fmtEscapes(arg)});
    }

    if (account_count < 2) panic("Need at least two acconts, got {}", .{account_count});

    const transfer_arrival_rate_ns = @divTrunc(
        std.time.ns_per_s,
        transfer_count_per_second,
    );

    const client_id = std.crypto.random.int(u128);
    const cluster_id: u32 = 0;

    var io = try IO.init(32, 0);

    var message_pool = try MessagePool.init(allocator, .client);

    std.log.info("Benchmark running against {any}", .{addresses});

    var client = try Client.init(
        allocator,
        client_id,
        cluster_id,
        @intCast(u8, addresses.len),
        &message_pool,
        .{
            .configuration = addresses,
            .io = &io,
        },
    );

    var benchmark = Benchmark{
        .io = &io,
        .message_pool = &message_pool,
        .client = &client,
        .batch_accounts = try std.ArrayList(tb.Account).initCapacity(allocator, account_count_per_batch),
        .account_count = account_count,
        .account_index = 0,
        .rng = std.rand.DefaultPrng.init(42),
        .timer = try std.time.Timer.start(),
        .batch_latency_ns = try std.ArrayList(u64).initCapacity(allocator, transfer_count),
        .transfer_latency_ns = try std.ArrayList(u64).initCapacity(allocator, transfer_count),
        .batch_transfers = try std.ArrayList(tb.Transfer).initCapacity(allocator, transfer_count_per_batch),
        .batch_start_ns = 0,
        .tranfer_index = 0,
        .transfer_count = transfer_count,
        .transfer_count_per_second = transfer_count_per_second,
        .transfer_arrival_rate_ns = transfer_arrival_rate_ns,
        .transfer_start_ns = try std.ArrayList(u64).initCapacity(allocator, transfer_count_per_batch),
        .batch_index = 0,
        .transfers_sent = 0,
        .transfer_index = 0,
        .transfer_next_arrival_ns = 0,
        .message = null,
        .callback = null,
        .done = false,
        .statsd = if (enable_statsd) &try StatsD.init(
            allocator,
            &io,
            std.net.Address.parseIp4("127.0.0.1", 8125) catch unreachable,
        ) else null,
        .print_batch_timings = print_batch_timings,
    };

    defer if (enable_statsd) benchmark.statsd.?.deinit(allocator);

    benchmark.create_accounts();

    while (!benchmark.done) {
        benchmark.client.tick();
        try benchmark.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
    }
}

fn parse_arg_addresses(
    allocator: std.mem.Allocator,
    args: *std.process.ArgIterator,
    arg: []const u8,
    arg_name: []const u8,
    arg_value: *[]std.net.Address,
) !bool {
    if (!std.mem.eql(u8, arg, arg_name)) return false;

    allocator.free(arg_value.*);

    const address_string_or_err = args.next(allocator) orelse
        panic("Expected an argument to {s}", .{arg_name});
    const address_string = try address_string_or_err;
    arg_value.* = try vsr.parse_addresses(allocator, address_string, constants.members_max);
    return true;
}

fn parse_arg_usize(
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

fn parse_arg_bool(
    allocator: std.mem.Allocator,
    args: *std.process.ArgIterator,
    arg: []const u8,
    arg_name: []const u8,
    arg_value: *bool,
) !bool {
    if (!std.mem.eql(u8, arg, arg_name)) return false;

    const bool_string_or_err = args.next(allocator) orelse
        panic("Expected an argument to {s}", .{arg_name});
    const bool_string = try bool_string_or_err;
    arg_value.* = std.mem.eql(u8, bool_string, "true");

    return true;
}

const Benchmark = struct {
    io: *IO,
    message_pool: *MessagePool,
    client: *Client,
    batch_accounts: std.ArrayList(tb.Account),
    account_count: usize,
    account_index: usize,
    rng: std.rand.DefaultPrng,
    timer: std.time.Timer,
    batch_latency_ns: std.ArrayList(u64),
    transfer_latency_ns: std.ArrayList(u64),
    batch_transfers: std.ArrayList(tb.Transfer),
    batch_start_ns: usize,
    transfers_sent: usize,
    tranfer_index: usize,
    transfer_count: usize,
    transfer_count_per_second: usize,
    transfer_arrival_rate_ns: usize,
    transfer_start_ns: std.ArrayList(u64),
    batch_index: usize,
    transfer_index: usize,
    transfer_next_arrival_ns: usize,
    message: ?*MessagePool.Message,
    callback: ?fn (*Benchmark) void,
    done: bool,
    statsd: ?*StatsD,
    print_batch_timings: bool,

    fn create_accounts(b: *Benchmark) void {
        if (b.account_index >= b.account_count) {
            b.create_transfers();
            return;
        }

        // Reset batch.
        b.batch_accounts.resize(0) catch unreachable;

        // Fill batch.
        while (b.account_index < b.account_count and
            b.batch_accounts.items.len < account_count_per_batch)
        {
            b.batch_accounts.appendAssumeCapacity(.{
                .id = @bitReverse(u128, b.account_index + 1),
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
            b.account_index += 1;
        }

        // Submit batch.
        b.send(
            create_accounts,
            .create_accounts,
            std.mem.sliceAsBytes(b.batch_accounts.items),
        );
    }

    fn create_transfers(b: *Benchmark) void {
        if (b.transfer_index >= b.transfer_count) {
            b.finish();
            return;
        }

        if (b.transfer_index == 0) {
            // Init timer.
            b.timer.reset();
            b.transfer_next_arrival_ns = b.timer.read();
        }

        const random = b.rng.random();

        b.batch_transfers.resize(0) catch unreachable;
        b.transfer_start_ns.resize(0) catch unreachable;

        // Busy-wait for at least one transfer to be available.
        while (b.transfer_next_arrival_ns >= b.timer.read()) {}
        b.batch_start_ns = b.timer.read();

        // Fill batch.
        while (b.transfer_index < b.transfer_count and
            b.batch_transfers.items.len < transfer_count_per_batch and
            b.transfer_next_arrival_ns < b.batch_start_ns)
        {
            const debit_account_index = random.uintLessThan(u64, b.account_count);
            var credit_account_index = random.uintLessThan(u64, b.account_count);
            if (debit_account_index == credit_account_index) {
                credit_account_index = (credit_account_index + 1) % b.account_count;
            }
            assert(debit_account_index != credit_account_index);
            b.batch_transfers.appendAssumeCapacity(.{
                // Reverse the bits to stress non-append-only index for `id`.
                .id = @bitReverse(u128, b.transfer_index + 1),
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
            b.transfer_start_ns.appendAssumeCapacity(b.transfer_next_arrival_ns);

            b.transfer_index += 1;
            b.transfer_next_arrival_ns += random_int_exponential(random, u64, b.transfer_arrival_rate_ns);
        }

        assert(b.batch_transfers.items.len > 0);

        // Submit batch.
        b.send(
            create_transfers_finish,
            .create_transfers,
            std.mem.sliceAsBytes(b.batch_transfers.items),
        );
    }

    fn create_transfers_finish(b: *Benchmark) void {
        // Record latencies.
        const batch_end_ns = b.timer.read();
        const ms_time = @divTrunc(batch_end_ns - b.batch_start_ns, std.time.ns_per_ms);

        if (b.print_batch_timings) {
            log.info("batch {}: {} tx in {} ms\n", .{
                b.batch_index,
                b.batch_transfers.items.len,
                ms_time,
            });
        }

        b.batch_latency_ns.appendAssumeCapacity(batch_end_ns - b.batch_start_ns);
        for (b.transfer_start_ns.items) |start_ns| {
            b.transfer_latency_ns.appendAssumeCapacity(batch_end_ns - start_ns);
        }

        b.batch_index += 1;
        b.transfers_sent += b.batch_transfers.items.len;

        if (b.statsd) |statsd| {
            statsd.gauge("benchmark.txns", b.batch_transfers.items.len) catch {};
            statsd.timing("benchmark.timings", ms_time) catch {};
            statsd.gauge("benchmark.batch", b.batch_index) catch {};
            statsd.gauge("benchmark.completed", b.transfers_sent) catch {};
        }

        b.create_transfers();
    }

    fn finish(b: *Benchmark) void {
        const total_ns = b.timer.read();

        const less_than_ns = (struct {
            fn lessThan(_: void, ns1: u64, ns2: u64) bool {
                return ns1 < ns2;
            }
        }).lessThan;
        std.sort.sort(u64, b.batch_latency_ns.items, {}, less_than_ns);
        std.sort.sort(u64, b.transfer_latency_ns.items, {}, less_than_ns);

        const stdout = std.io.getStdOut().writer();

        stdout.print("{} batches in {d:.2} s\n", .{
            b.batch_index,
            @intToFloat(f64, total_ns) / std.time.ns_per_s,
        }) catch unreachable;
        stdout.print("load offered = {} tx/s\n", .{
            b.transfer_count_per_second,
        }) catch unreachable;
        stdout.print("load accepted = {} tx/s\n", .{
            @divTrunc(
                b.transfer_count * std.time.ns_per_s,
                total_ns,
            ),
        }) catch unreachable;
        print_deciles(stdout, "batch", b.batch_latency_ns.items);
        print_deciles(stdout, "transfer", b.transfer_latency_ns.items);

        b.done = true;
    }

    fn send(
        b: *Benchmark,
        callback: fn (*Benchmark) void,
        operation: StateMachine.Operation,
        payload: []u8,
    ) void {
        b.callback = callback;
        b.message = b.client.get_message();

        stdx.copy_disjoint(
            .inexact,
            u8,
            b.message.?.buffer[@sizeOf(vsr.Header)..],
            payload,
        );

        b.client.request(
            @intCast(u128, @ptrToInt(b)),
            send_complete,
            operation,
            b.message.?,
            payload.len,
        );
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

        const b = @intToPtr(*Benchmark, @intCast(u64, user_data));

        b.client.unref(b.message.?);
        b.message = null;

        const callback = b.callback.?;
        b.callback = null;
        callback(b);
    }
};

fn print_deciles(
    stdout: anytype,
    label: []const u8,
    latencies: []const u64,
) void {
    var decile: usize = 0;
    while (decile <= 10) : (decile += 1) {
        const index = @divTrunc(latencies.len * decile, 10) -| 1;
        stdout.print("{s} latency p{}0 = {} ms\n", .{
            label,
            decile,
            @divTrunc(
                latencies[index],
                std.time.ns_per_ms,
            ),
        }) catch unreachable;
    }
}
