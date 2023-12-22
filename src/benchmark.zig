//! This script is a Zig TigerBeetle client that connects to a TigerBeetle
//! cluster and runs a workload on it (described below) while measuring
//! (and at the end, printing) observed latencies and throughput.
//!
//! It uses a single client to 1) create `account_count` accounts then 2)
//! generate `transfer_count` transfers between random different
//! accounts. It does not attempt to create more than
//! `transfer_count_per_second` transfers per second, however it may reach
//! less than this rate since it will wait at least as long as it takes
//! for the cluster to respond before creating more transfers. It does not
//! validate that the transfers succeed.
//!
//! `./scripts/benchmark.sh` (and `.\scripts\benchmark.bat` on Windows)
//! are helpers that spin up a single TigerBeetle replica on a free port
//! and run the benchmark `./zig/zig build benchmark` (and
//! `.\zig\zig build benchmark` on Windows) against the replica. To
//! run against a cluster of TigerBeetle replicas, use `./zig/zig build
//! benchmark --addresses=X` where `X` is the list of replica
//! addresses. It is the same format for the `--addresses=X` flag on the
//! `tigerbeetle start` command.

const account_count_default: usize = 10_000;
const transfer_count_default: usize = 10_000_000;
const query_count_default: usize = 100;
const transfer_count_per_second_default: usize = 1_000_000;

const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const panic = std.debug.panic;
const log = std.log;
pub const std_options = struct {
    pub const log_level: std.log.Level = .info;
};

const constants = @import("constants.zig");
const stdx = @import("stdx.zig");
const flags = @import("./flags.zig");
const random_int_exponential = @import("testing/fuzz.zig").random_int_exponential;
const vsr = @import("vsr.zig");
const IO = vsr.io.IO;
const Storage = vsr.storage.Storage;
const MessagePool = vsr.message_pool.MessagePool;
const MessageBus = vsr.message_bus.MessageBusClient;
const StateMachine = vsr.state_machine.StateMachineType(Storage, constants.state_machine_config);
const Client = vsr.Client(StateMachine, MessageBus);
const tb = @import("tigerbeetle.zig");
const StatsD = @import("statsd.zig").StatsD;
const IdPermutation = @import("testing/id.zig").IdPermutation;

const account_count_per_batch = @divExact(
    constants.message_size_max - @sizeOf(vsr.Header),
    @sizeOf(tb.Account),
);
const transfer_count_per_batch = @divExact(
    constants.message_size_max - @sizeOf(vsr.Header),
    @sizeOf(tb.Transfer),
);

/// The ID order can affect the results of a benchmark significantly. Specifically, sequential is
/// expected to be the best (since it can take advantage of various optimizations such as avoiding
/// negative prefetch) while random / reversed can't.
const IdOrder = enum { sequential, random, reversed };

const CliArgs = struct {
    account_count: usize = account_count_default,
    transfer_count: usize = transfer_count_default,
    query_count: usize = query_count_default,
    transfer_count_per_second: usize = transfer_count_per_second_default,
    print_batch_timings: bool = false,
    id_order: IdOrder = .reversed,
    statsd: bool = false,
    addresses: []const u8 = "127.0.0.1:" ++ std.fmt.comptimePrint("{}", .{constants.port}),
};

pub fn main() !void {
    const stderr = std.io.getStdErr().writer();

    if (builtin.mode != .ReleaseSafe and builtin.mode != .ReleaseFast) {
        try stderr.print("Benchmark must be built as ReleaseSafe for reasonable results.\n", .{});
    }

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    // Discard executable name.
    _ = args.next().?;

    const cli_args = flags.parse(&args, CliArgs);

    const addresses = try vsr.parse_addresses(allocator, cli_args.addresses, constants.members_max);
    defer allocator.free(addresses);

    if (cli_args.account_count < 2) flags.fatal(
        "--account-count: need at least two accounts, got {}",
        .{cli_args.account_count},
    );

    const transfer_arrival_rate_ns = @divTrunc(
        std.time.ns_per_s,
        cli_args.transfer_count_per_second,
    );

    const client_id = std.crypto.random.int(u128);
    const cluster_id: u128 = 0;

    var io = try IO.init(32, 0);

    var message_pool = try MessagePool.init(allocator, .client);

    std.log.info("Benchmark running against {any}", .{addresses});

    var client = try Client.init(
        allocator,
        client_id,
        cluster_id,
        @as(u8, @intCast(addresses.len)),
        &message_pool,
        .{
            .configuration = addresses,
            .io = &io,
        },
    );

    var batch_accounts =
        try std.ArrayListUnmanaged(tb.Account).initCapacity(allocator, account_count_per_batch);
    defer batch_accounts.deinit(allocator);

    // Each array position corresponds to a histogram bucket of 1ms. The last bucket is 10000ms+.
    const batch_latency_histogram = try allocator.alloc(u64, 10001);
    @memset(batch_latency_histogram, 0);
    defer allocator.free(batch_latency_histogram);

    var query_latency_ns =
        try std.ArrayListUnmanaged(u64).initCapacity(allocator, cli_args.query_count);
    defer query_latency_ns.deinit(allocator);

    var batch_transfers =
        try std.ArrayListUnmanaged(tb.Transfer).initCapacity(allocator, transfer_count_per_batch);
    defer batch_transfers.deinit(allocator);

    var statsd_opt: ?StatsD = null;
    defer if (statsd_opt) |*statsd| statsd.deinit(allocator);

    if (cli_args.statsd) {
        statsd_opt = try StatsD.init(
            allocator,
            &io,
            std.net.Address.parseIp4("127.0.0.1", 8125) catch unreachable,
        );
    }

    var rng = std.rand.DefaultPrng.init(42);
    const random = rng.random();
    const account_id_permutation: IdPermutation = switch (cli_args.id_order) {
        .sequential => .{ .identity = {} },
        .random => .{ .random = random.int(u64) },
        .reversed => .{ .inversion = {} },
    };

    var benchmark = Benchmark{
        .io = &io,
        .message_pool = &message_pool,
        .client = &client,
        .batch_accounts = batch_accounts,
        .account_count = cli_args.account_count,
        .account_index = 0,
        .query_count = cli_args.query_count,
        .query_index = 0,
        .account_id_permutation = account_id_permutation,
        .rng = rng,
        .timer = try std.time.Timer.start(),
        .batch_latency_histogram = batch_latency_histogram,
        .query_latency_ns = query_latency_ns,
        .batch_transfers = batch_transfers,
        .batch_start_ns = 0,
        .transfer_count = cli_args.transfer_count,
        .transfer_count_per_second = cli_args.transfer_count_per_second,
        .transfer_arrival_rate_ns = transfer_arrival_rate_ns,
        .batch_index = 0,
        .transfers_sent = 0,
        .transfer_index = 0,
        .transfer_next_arrival_ns = 0,
        .callback = null,
        .done = false,
        .statsd = if (statsd_opt) |*statsd| statsd else null,
        .print_batch_timings = cli_args.print_batch_timings,
        .id_order = cli_args.id_order,
    };

    benchmark.create_accounts();

    while (!benchmark.done) {
        benchmark.client.tick();
        try benchmark.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
    }
}

const Benchmark = struct {
    io: *IO,
    message_pool: *MessagePool,
    client: *Client,
    batch_accounts: std.ArrayListUnmanaged(tb.Account),
    account_count: usize,
    account_index: usize,
    query_count: usize,
    query_index: usize,
    account_id_permutation: IdPermutation,
    rng: std.rand.DefaultPrng,
    timer: std.time.Timer,
    batch_latency_histogram: []u64,
    query_latency_ns: std.ArrayListUnmanaged(u64),
    batch_transfers: std.ArrayListUnmanaged(tb.Transfer),
    batch_start_ns: usize,
    transfers_sent: usize,
    transfer_count: usize,
    transfer_count_per_second: usize,
    transfer_arrival_rate_ns: usize,
    batch_index: usize,
    transfer_index: usize,
    transfer_next_arrival_ns: usize,
    callback: ?*const fn (*Benchmark, StateMachine.Operation, []const u8) void,
    done: bool,
    statsd: ?*StatsD,
    print_batch_timings: bool,
    id_order: IdOrder,

    fn create_accounts(b: *Benchmark) void {
        if (b.account_index >= b.account_count) {
            b.create_transfers();
            return;
        }

        // Reset batch.
        b.batch_accounts.clearRetainingCapacity();

        // Fill batch.
        while (b.account_index < b.account_count and
            b.batch_accounts.items.len < account_count_per_batch)
        {
            b.batch_accounts.appendAssumeCapacity(.{
                .id = b.account_id_permutation.encode(b.account_index + 1),
                .user_data_128 = 0,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .reserved = 0,
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
            create_accounts_finish,
            .create_accounts,
            std.mem.sliceAsBytes(b.batch_accounts.items),
        );
    }

    fn create_accounts_finish(
        b: *Benchmark,
        operation: StateMachine.Operation,
        result: []const u8,
    ) void {
        assert(operation == .create_accounts);
        const create_accounts_results = std.mem.bytesAsSlice(
            tb.CreateAccountsResult,
            result,
        );
        if (create_accounts_results.len > 0) {
            panic("CreateAccountsResults: {any}", .{create_accounts_results});
        }

        b.create_accounts();
    }

    fn create_transfers(b: *Benchmark) void {
        if (b.transfer_index >= b.transfer_count) {
            b.summary_transfers();
            return;
        }

        if (b.transfer_index == 0) {
            // Init timer.
            b.timer.reset();
            b.transfer_next_arrival_ns = b.timer.read();
        }

        const random = b.rng.random();

        b.batch_transfers.clearRetainingCapacity();

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
            const debit_account_id = b.account_id_permutation.encode(debit_account_index + 1);
            const credit_account_id = b.account_id_permutation.encode(credit_account_index + 1);
            assert(debit_account_index != credit_account_index);

            b.batch_transfers.appendAssumeCapacity(.{
                .id = b.account_id_permutation.encode(b.transfer_index + 1),
                .debit_account_id = debit_account_id,
                .credit_account_id = credit_account_id,
                .user_data_128 = random.int(u128),
                .user_data_64 = random.int(u64),
                .user_data_32 = random.int(u32),
                // TODO Benchmark posting/voiding pending transfers.
                .pending_id = 0,
                .timeout = 0,
                .ledger = 2,
                .code = random.int(u16) +| 1,
                .flags = .{},
                .amount = random_int_exponential(random, u64, 10_000) +| 1,
                .timestamp = 0,
            });

            b.transfer_index += 1;
            b.transfer_next_arrival_ns +=
                random_int_exponential(random, u64, b.transfer_arrival_rate_ns);
        }

        assert(b.batch_transfers.items.len > 0);

        // Submit batch.
        b.send(
            create_transfers_finish,
            .create_transfers,
            std.mem.sliceAsBytes(b.batch_transfers.items),
        );
    }

    fn create_transfers_finish(
        b: *Benchmark,
        operation: StateMachine.Operation,
        result: []const u8,
    ) void {
        assert(operation == .create_transfers);
        const create_transfers_results = std.mem.bytesAsSlice(
            tb.CreateTransfersResult,
            result,
        );
        if (create_transfers_results.len > 0) {
            panic("CreateTransfersResults: {any}", .{create_transfers_results});
        }

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

        b.batch_latency_histogram[@min(ms_time, b.batch_latency_histogram.len - 1)] += 1;

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

    fn summary_transfers(b: *Benchmark) void {
        const total_ns = b.timer.read();
        const stdout = std.io.getStdOut().writer();

        stdout.print("{} batches in {d:.2} s\n", .{
            b.batch_index,
            @as(f64, @floatFromInt(total_ns)) / std.time.ns_per_s,
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
        print_percentiles_histogram(stdout, "batch", b.batch_latency_histogram, b.batch_index);

        if (b.query_count > 0) {
            b.timer.reset();
            b.account_index = 0;
            b.query_account_transfers();
        } else {
            b.done = true;
        }
    }

    fn query_account_transfers(b: *Benchmark) void {
        if (b.query_index >= b.query_count) {
            b.summary_query();
            return;
        }

        b.account_index = b.rng.random().intRangeLessThan(usize, 0, b.account_count);
        var filter = tb.GetAccountTransfers{
            .account_id = b.account_id_permutation.encode(b.account_index + 1),
            .timestamp = 0,
            .limit = transfer_count_per_batch,
            .flags = .{
                .credits = true,
                .debits = true,
                .reversed = false,
            },
        };

        b.batch_start_ns = b.timer.read();
        b.send(
            query_account_transfers_finish,
            .get_account_transfers,
            std.mem.asBytes(&filter),
        );
    }

    fn query_account_transfers_finish(
        b: *Benchmark,
        operation: StateMachine.Operation,
        result: []const u8,
    ) void {
        assert(operation == .get_account_transfers);

        const batch_end_ns = b.timer.read();
        const transfers = std.mem.bytesAsSlice(tb.Transfer, result);
        const account_id = b.account_id_permutation.encode(b.account_index + 1);
        for (transfers) |*transfer| {
            assert(transfer.debit_account_id == account_id or
                transfer.credit_account_id == account_id);
        }

        b.query_latency_ns.appendAssumeCapacity(batch_end_ns - b.batch_start_ns);
        b.query_index += 1;
        b.query_account_transfers();
    }

    fn summary_query(b: *Benchmark) void {
        const total_ns = b.timer.read();

        const less_than_ns = (struct {
            fn lessThan(_: void, ns1: u64, ns2: u64) bool {
                return ns1 < ns2;
            }
        }).lessThan;
        std.mem.sort(u64, b.query_latency_ns.items, {}, less_than_ns);

        const stdout = std.io.getStdOut().writer();

        stdout.print("\n{} queries in {d:.2} s\n", .{
            b.query_count,
            @as(f64, @floatFromInt(total_ns)) / std.time.ns_per_s,
        }) catch unreachable;
        print_deciles(stdout, "query", b.query_latency_ns.items);

        b.done = true;
    }

    fn send(
        b: *Benchmark,
        callback: *const fn (*Benchmark, StateMachine.Operation, []const u8) void,
        operation: StateMachine.Operation,
        payload: []u8,
    ) void {
        b.callback = callback;

        const event_count = switch (operation) {
            inline else => |op| @divExact(payload.len, @sizeOf(StateMachine.Event(op))),
        };

        const batch = b.client.batch_get(operation, event_count) catch unreachable;
        stdx.copy_disjoint(.exact, u8, batch.slice(), payload);

        b.client.batch_submit(
            @intCast(@intFromPtr(b)),
            send_complete,
            batch,
        );
    }

    fn send_complete(
        user_data: u128,
        operation: StateMachine.Operation,
        result: []const u8,
    ) void {
        const b: *Benchmark = @ptrFromInt(@as(u64, @intCast(user_data)));
        const callback = b.callback.?;
        b.callback = null;

        callback(b, operation, result);
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

fn print_percentiles_histogram(
    stdout: anytype,
    label: []const u8,
    latencies: []const u64,
    count: u64,
) void {
    const percentiles = [_]u64{ 1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 99, 100 };
    for (percentiles) |percentile| {
        var target_count: usize = @divTrunc(count * percentile, 100);
        var current_count: usize = 0;
        var index: usize = 0;

        // Since our histogram is 1ms buckets, index == ms.
        while (index < latencies.len) {
            current_count += latencies[index];

            if (current_count >= target_count) {
                break;
            }

            index += 1;
        }

        const warning = if (index == latencies.len) "+ (exceeds histogram resolution)" else "";

        stdout.print("{s} latency p{} = {} ms{s}\n", .{
            label,
            percentile,
            index,
            warning,
        }) catch unreachable;
    }
}
