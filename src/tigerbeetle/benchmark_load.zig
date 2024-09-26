//! This script is a Zig TigerBeetle client that connects to a TigerBeetle
//! cluster and runs a workload on it (described below) while measuring
//! (and at the end, printing) observed latencies and throughput.
//!
//! It uses a single client to 1) create `account_count` accounts then 2)
//! generate `transfer_count` transfers between random different
//! accounts. It does not validate that the transfers succeed.
const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const panic = std.debug.panic;
const log = std.log;
pub const std_options = .{
    .log_level = .info,
};

const vsr = @import("vsr");
const constants = vsr.constants;
const stdx = vsr.stdx;
const flags = vsr.flags;
const random_int_exponential = vsr.testing.random_int_exponential;
const IO = vsr.io.IO;
const Storage = vsr.storage.Storage(IO);
const MessagePool = vsr.message_pool.MessagePool;
const MessageBus = vsr.message_bus.MessageBusClient;
const StateMachine = vsr.state_machine.StateMachineType(Storage, constants.state_machine_config);
const Client = vsr.Client(StateMachine, MessageBus);
const tb = vsr.tigerbeetle;
const StatsD = vsr.statsd.StatsD;
const IdPermutation = vsr.testing.IdPermutation;
const ZipfianGenerator = stdx.ZipfianGenerator;
const ZipfianShuffled = stdx.ZipfianShuffled;
const Distribution = cli.Command.Benchmark.Distribution;

const cli = @import("./cli.zig");

pub fn main(
    allocator: std.mem.Allocator,
    addresses: []const std.net.Address,
    cli_args: *const cli.Command.Benchmark,
) !void {
    const stderr = std.io.getStdErr().writer();

    if (builtin.mode != .ReleaseSafe and builtin.mode != .ReleaseFast) {
        try stderr.print("Benchmark must be built with '-Drelease' for reasonable results.\n", .{});
    }
    if (!vsr.constants.config.process.direct_io) {
        log.warn("direct io is disabled", .{});
    }
    if (vsr.constants.config.process.verify) {
        log.warn("extra assertions are enabled", .{});
    }

    if (cli_args.account_count < 2) vsr.fatal(
        .cli,
        "--account-count: need at least two accounts, got {}",
        .{cli_args.account_count},
    );

    // The first account_count_hot accounts are "hot" -- they will be the debit side of
    // transfer_hot_percent of the transfers.
    if (cli_args.account_count_hot > cli_args.account_count) vsr.fatal(
        .cli,
        "--account-count-hot: must be less-than-or-equal-to --account-count, got {}",
        .{cli_args.account_count_hot},
    );

    if (cli_args.transfer_hot_percent > 100) vsr.fatal(
        .cli,
        "--transfer-hot-percent: must be less-than-or-equal-to 100, got {}",
        .{cli_args.transfer_hot_percent},
    );

    const client_id = std.crypto.random.int(u128);
    const cluster_id: u128 = 0;

    var io = try IO.init(32, 0);

    var message_pool = try MessagePool.init(allocator, .client);

    std.log.info("Benchmark running against {any}", .{addresses});

    var client = try Client.init(
        allocator,
        .{
            .id = client_id,
            .cluster = cluster_id,
            .replica_count = @intCast(addresses.len),
            .message_pool = &message_pool,
            .message_bus_options = .{
                .configuration = addresses,
                .io = &io,
            },
        },
    );

    var batch_accounts =
        try std.ArrayListUnmanaged(tb.Account).initCapacity(allocator, cli_args.account_batch_size);
    defer batch_accounts.deinit(allocator);

    // Each array position corresponds to a histogram bucket of 1ms. The last bucket is 10_000ms+.
    const batch_latency_histogram = try allocator.alloc(u64, 10_001);
    @memset(batch_latency_histogram, 0);
    defer allocator.free(batch_latency_histogram);

    const query_latency_histogram = try allocator.alloc(u64, 10_001);
    @memset(query_latency_histogram, 0);
    defer allocator.free(query_latency_histogram);

    var batch_transfers =
        try std.ArrayListUnmanaged(tb.Transfer).initCapacity(
        allocator,
        cli_args.transfer_batch_size,
    );
    defer batch_transfers.deinit(allocator);

    var batch_account_ids =
        try std.ArrayListUnmanaged(u128).initCapacity(allocator, cli_args.account_batch_size);
    defer batch_account_ids.deinit(allocator);

    var batch_transfer_ids =
        try std.ArrayListUnmanaged(u128).initCapacity(allocator, cli_args.transfer_batch_size);
    defer batch_transfer_ids.deinit(allocator);

    var statsd_opt: ?StatsD = null;
    defer if (statsd_opt) |*statsd| statsd.deinit(allocator);

    if (cli_args.statsd) {
        statsd_opt = try StatsD.init(
            allocator,
            &io,
            std.net.Address.parseIp4("127.0.0.1", 8125) catch unreachable,
        );
    }

    // If no seed was given, use a default seed for reproducibility.
    const seed = seed_from_arg: {
        const seed_argument = cli_args.seed orelse break :seed_from_arg 42;
        break :seed_from_arg vsr.testing.parse_seed(seed_argument);
    };

    log.info("Benchmark seed = {}", .{seed});

    var rng = std.rand.DefaultPrng.init(seed);
    const random = rng.random();
    const account_id_permutation: IdPermutation = switch (cli_args.id_order) {
        .sequential => .{ .identity = {} },
        .random => .{ .random = random.int(u64) },
        .reversed => .{ .inversion = {} },
    };

    assert(cli_args.account_count >= cli_args.account_count_hot);
    const account_generator = Generator.from_distribution(
        cli_args.account_distribution,
        cli_args.account_count - cli_args.account_count_hot,
        random,
    );
    const account_generator_hot = Generator.from_distribution(
        cli_args.account_distribution,
        cli_args.account_count_hot,
        random,
    );

    log.info("Account distribution: {s}", .{
        @tagName(cli_args.account_distribution),
    });

    var benchmark = Benchmark{
        .io = &io,
        .message_pool = &message_pool,
        .client = &client,
        .batch_accounts = batch_accounts,
        .account_count = cli_args.account_count,
        .account_count_hot = cli_args.account_count_hot,
        .account_generator = account_generator,
        .account_generator_hot = account_generator_hot,
        .flag_history = cli_args.flag_history,
        .flag_imported = cli_args.flag_imported,
        .account_index = 0,
        .query_count = cli_args.query_count,
        .query_index = 0,
        .account_id_permutation = account_id_permutation,
        .rng = rng,
        .timer = try std.time.Timer.start(),
        .batch_latency_histogram = batch_latency_histogram,
        .query_latency_histogram = query_latency_histogram,
        .batch_transfers = batch_transfers,
        .batch_start_ns = 0,
        .transfer_count = cli_args.transfer_count,
        .transfer_hot_percent = cli_args.transfer_hot_percent,
        .transfer_pending = cli_args.transfer_pending,
        .transfer_batch_size = cli_args.transfer_batch_size,
        .transfer_batch_delay_us = cli_args.transfer_batch_delay_us,
        .validate = cli_args.validate,
        .batch_index = 0,
        .transfers_sent = 0,
        .transfer_index = 0,
        .transfer_next_arrival_ns = 0,
        .callback = null,
        .done = false,
        .statsd = if (statsd_opt) |*statsd| statsd else null,
        .print_batch_timings = cli_args.print_batch_timings,
        .id_order = cli_args.id_order,
        .batch_account_ids = batch_account_ids,
        .batch_transfer_ids = batch_transfer_ids,
    };

    benchmark.client.register(Benchmark.register_callback, @intCast(@intFromPtr(&benchmark)));
    while (!benchmark.done) {
        benchmark.client.tick();
        try benchmark.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
    }
    benchmark.done = false;

    // benchmark.create_accounts();

    // while (!benchmark.done) {
    //     benchmark.client.tick();
    //     try benchmark.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
    // }
    // benchmark.done = false;

    // if (cli_args.checksum_performance) {
    //     const stdout = std.io.getStdOut().writer();
    //     stdout.print("\nmessage size max = {} bytes\n", .{
    //         constants.message_size_max,
    //     }) catch unreachable;

    //     const buffer = try allocator.alloc(u8, constants.message_size_max);
    //     defer allocator.free(buffer);
    //     benchmark.rng.fill(buffer);

    //     benchmark.timer.reset();
    //     _ = vsr.checksum(buffer);
    //     const checksum_duration_ns = benchmark.timer.read();

    //     stdout.print("checksum message size max = {} us\n", .{
    //         @divTrunc(checksum_duration_ns, std.time.ns_per_us),
    //     }) catch unreachable;
    // }

    // if (!benchmark.validate) return;

    // // Reset our state so we can check our work.
    // benchmark.rng = rng;
    // benchmark.account_index = 0;
    // benchmark.transfer_index = 0;
    // benchmark.validate_accounts();

    // while (!benchmark.done) {
    //     benchmark.client.tick();
    //     try benchmark.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
    // }

    var workloads =
        try std.ArrayListUnmanaged(Workload).initCapacity(allocator, 0);
    defer workloads.deinit(allocator);

    try workloads.appendSlice(allocator, &[_]Workload{
        // The "setup" workload that generates the starting state of the database,
        // currently just `account_count` accounts.
        Workload {
            .operation_count = cli_args.account_count,

            .create_accounts_weight = 100,
            .create_transfers_weight = 0,
            .get_account_balances_weight = 0,
            .get_account_transfers_weight = 0,
            .lookup_accounts_weight = 0,
            .lookup_transfers_weight = 0,
            .query_accounts_weight = 0,
            .query_transfers_weight = 0,

            .create_accounts_batch_size = cli_args.account_batch_size,
            .create_transfers_batch_size = cli_args.transfer_batch_size,

            .account_distribution_debit = Distribution.uniform,
            .account_distribution_credit = Distribution.uniform,
            .account_distribution_query = Distribution.uniform,

            .flag_history = cli_args.flag_history,
            .flag_imported = cli_args.flag_imported,
            .transfer_pending = cli_args.transfer_pending,
        },
        // The main workload, currently just generating `transfer_count` transfers.
        Workload {
            .operation_count = cli_args.transfer_count,

            .create_accounts_weight = 0,
            .create_transfers_weight = 100,
            .get_account_balances_weight = 0,
            .get_account_transfers_weight = 0,
            .lookup_accounts_weight = 0,
            .lookup_transfers_weight = 0,
            .query_accounts_weight = 0,
            .query_transfers_weight = 0,

            .create_accounts_batch_size = cli_args.account_batch_size,
            .create_transfers_batch_size = cli_args.transfer_batch_size,

            .account_distribution_debit = cli_args.account_distribution,
            .account_distribution_credit = cli_args.account_distribution,
            .account_distribution_query = cli_args.account_distribution,

            .flag_history = cli_args.flag_history,
            .flag_imported = cli_args.flag_imported,
            .transfer_pending = cli_args.transfer_pending,
        },
        // The default query workload.
        Workload {
            .operation_count = cli_args.query_count,

            .create_accounts_weight = 0,
            .create_transfers_weight = 0,
            .get_account_balances_weight = 0,
            .get_account_transfers_weight = 100,
            .lookup_accounts_weight = 0,
            .lookup_transfers_weight = 0,
            .query_accounts_weight = 0,
            .query_transfers_weight = 0,

            .create_accounts_batch_size = cli_args.account_batch_size,
            .create_transfers_batch_size = cli_args.transfer_batch_size,

            .account_distribution_debit = cli_args.account_distribution,
            .account_distribution_credit = cli_args.account_distribution,
            .account_distribution_query = cli_args.account_distribution,

            .flag_history = cli_args.flag_history,
            .flag_imported = cli_args.flag_imported,
            .transfer_pending = cli_args.transfer_pending,
        },
        // An example workload of multiple interleaved operations.
        Workload {
            .operation_count = cli_args.account_count + cli_args.transfer_count + cli_args.query_count,

            .create_accounts_weight = 33,
            .create_transfers_weight = 33,
            .get_account_balances_weight = 0,
            .get_account_transfers_weight = 33,
            .lookup_accounts_weight = 0,
            .lookup_transfers_weight = 0,
            .query_accounts_weight = 0,
            .query_transfers_weight = 0,

            .create_accounts_batch_size = cli_args.account_batch_size,
            .create_transfers_batch_size = cli_args.transfer_batch_size,

            .account_distribution_debit = cli_args.account_distribution,
            .account_distribution_credit = cli_args.account_distribution,
            .account_distribution_query = cli_args.account_distribution,

            .flag_history = cli_args.flag_history,
            .flag_imported = cli_args.flag_imported,
            .transfer_pending = cli_args.transfer_pending,
        }
    });

    try run_workloads(
        allocator,
        benchmark.io,
        benchmark.client,
        workloads.items,
        account_id_permutation,
        .{
            .rng_seed = seed,
            .validate = cli_args.validate,
        },
    );
}

const Generator = union(enum) {
    zipfian: ZipfianShuffled,
    latest: ZipfianGenerator,
    uniform,

    fn from_distribution(
        distribution: Distribution,
        count: u64,
        random: std.Random,
    ) Generator {
        return switch (distribution) {
            .zipfian => .{
                .zipfian = ZipfianShuffled.init(count, random),
            },
            .latest => .{
                .latest = ZipfianGenerator.init(count),
            },
            .uniform => .uniform,
        };
    }

    fn grow(self: *Generator, count: u64, random: std.Random) void {
        switch (self.*) {
            .zipfian => |*d| d.grow(count, random),
            .latest => |*d| d.grow(count),
            .uniform => {},
        }
    }
};

const Benchmark = struct {
    io: *IO,
    message_pool: *MessagePool,
    client: *Client,
    batch_accounts: std.ArrayListUnmanaged(tb.Account),
    account_count: usize,
    account_count_hot: usize,
    account_generator: Generator,
    account_generator_hot: Generator,
    flag_history: bool,
    flag_imported: bool,
    account_index: usize,
    query_count: usize,
    query_index: usize,
    account_id_permutation: IdPermutation,
    rng: std.rand.DefaultPrng,
    timer: std.time.Timer,
    batch_latency_histogram: []u64,
    query_latency_histogram: []u64,
    batch_transfers: std.ArrayListUnmanaged(tb.Transfer),
    batch_start_ns: usize,
    transfers_sent: usize,
    transfer_count: usize,
    transfer_hot_percent: usize,
    transfer_pending: bool,
    transfer_batch_size: usize,
    transfer_batch_delay_us: usize,
    validate: bool,
    batch_index: usize,
    transfer_index: usize,
    transfer_next_arrival_ns: usize,
    callback: ?*const fn (*Benchmark, StateMachine.Operation, []const u8) void,
    done: bool,
    statsd: ?*StatsD,
    print_batch_timings: bool,
    id_order: cli.Command.Benchmark.IdOrder,
    batch_account_ids: std.ArrayListUnmanaged(u128),
    batch_transfer_ids: std.ArrayListUnmanaged(u128),

    fn create_account(b: *Benchmark) tb.Account {
        const random = b.rng.random();

        defer b.account_index += 1;
        return .{
            .id = b.account_id_permutation.encode(b.account_index + 1),
            .user_data_128 = random.int(u128),
            .user_data_64 = random.int(u64),
            .user_data_32 = random.int(u32),
            .reserved = 0,
            .ledger = 2,
            .code = 1,
            .flags = .{
                .history = b.flag_history,
                .imported = b.flag_imported,
            },
            .debits_pending = 0,
            .debits_posted = 0,
            .credits_pending = 0,
            .credits_posted = 0,
            .timestamp = if (b.flag_imported) b.account_index + 1 else 0,
        };
    }

    fn create_accounts(b: *Benchmark) void {
        if (b.account_index >= b.account_count) {
            b.create_transfers();
            return;
        }

        // Reset batch.
        b.batch_accounts.clearRetainingCapacity();

        // Fill batch.
        while (b.account_index < b.account_count and
            b.batch_accounts.items.len < b.batch_accounts.capacity)
        {
            b.batch_accounts.appendAssumeCapacity(b.create_account());
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

    fn gen_account_index(b: *Benchmark, generator: *Generator) u64 {
        const random = b.rng.random();
        switch (generator.*) {
            .zipfian => |gen| {
                // zipfian set size must be same as account set size
                assert(b.account_count == gen.gen.n);
                const index = gen.next(random);
                assert(index < b.account_count);
                return index;
            },
            .latest => |gen| {
                assert(b.account_count == gen.n);
                const index_rev = gen.next(random);
                assert(index_rev < b.account_count);
                return b.account_count - index_rev - 1;
            },
            .uniform => {
                return random.uintLessThan(u64, b.account_count);
            },
        }
    }

    fn create_transfer(b: *Benchmark) tb.Transfer {
        const random = b.rng.random();

        // The set of accounts is divided into two different "worlds" by
        // `account_count_hot`. Sometimes the debit account will be selected
        // from the first `account_count_hot` accounts; otherwise both
        // debit and credit will be selected from an account >= `account_count_hot`.

        const debit_account_hot = b.account_count_hot > 0 and
            random.uintLessThan(u64, 100) < b.transfer_hot_percent;

        const debit_account_index = if (debit_account_hot)
            b.gen_account_index(&b.account_generator_hot)
        else
            b.gen_account_index(&b.account_generator) + b.account_count_hot;
        const credit_account_index = index: {
            var index = b.gen_account_index(&b.account_generator) + b.account_count_hot;
            if (index == debit_account_index) {
                index = (index + 1) % b.account_count + b.account_count_hot;
            }
            break :index index;
        };

        const debit_account_id = b.account_id_permutation.encode(debit_account_index + 1);
        const credit_account_id = b.account_id_permutation.encode(credit_account_index + 1);
        assert(debit_account_index != credit_account_index);

        // 30% of pending transfers.
        const pending = b.transfer_pending and random.intRangeAtMost(u8, 0, 9) < 3;

        defer b.transfer_index += 1;
        return .{
            .id = b.account_id_permutation.encode(b.transfer_index + 1),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .user_data_128 = random.int(u128),
            .user_data_64 = random.int(u64),
            .user_data_32 = random.int(u32),
            // TODO Benchmark posting/voiding pending transfers.
            .pending_id = 0,
            .ledger = 2,
            .code = random.int(u16) +| 1,
            .flags = .{
                .pending = pending,
                .imported = b.flag_imported,
            },
            .timeout = if (pending) random.intRangeAtMost(u32, 1, 60) else 0,
            .amount = random_int_exponential(random, u64, 10_000) +| 1,
            .timestamp = if (b.flag_imported) b.account_index + b.transfer_index + 1 else 0,
        };
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

        b.batch_transfers.clearRetainingCapacity();

        b.batch_start_ns = b.timer.read();

        // Fill batch.
        while (b.transfer_index < b.transfer_count and
            b.batch_transfers.items.len < b.batch_transfers.capacity)
        {
            b.batch_transfers.appendAssumeCapacity(b.create_transfer());
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
            log.info("batch {}: {} tx in {} ms", .{
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

        std.time.sleep(b.transfer_batch_delay_us * std.time.ns_per_us);
        b.create_transfers();
    }

    fn summary_transfers(b: *Benchmark) void {
        const total_ns = b.timer.read();
        const stdout = std.io.getStdOut().writer();

        stdout.print("{} batches in {d:.2} s\n", .{
            b.batch_index,
            @as(f64, @floatFromInt(total_ns)) / std.time.ns_per_s,
        }) catch unreachable;
        stdout.print("transfer batch size = {} txs\n", .{
            b.transfer_batch_size,
        }) catch unreachable;
        stdout.print("transfer batch delay = {} us\n", .{
            b.transfer_batch_delay_us,
        }) catch unreachable;
        stdout.print("load accepted = {} tx/s\n", .{
            @divTrunc(
                b.transfer_count * std.time.ns_per_s,
                total_ns,
            ),
        }) catch unreachable;
        print_percentiles_histogram(stdout, "batch", b.batch_latency_histogram);

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

        b.account_index = b.gen_account_index(&b.account_generator);
        var filter = tb.AccountFilter{
            .account_id = b.account_id_permutation.encode(b.account_index + 1),
            .user_data_128 = 0,
            .user_data_64 = 0,
            .user_data_32 = 0,
            .code = 0,
            .timestamp_min = 0,
            .timestamp_max = 0,
            .limit = @divExact(
                constants.message_size_max - @sizeOf(vsr.Header),
                @sizeOf(tb.Transfer),
            ),
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

        const ms_time = @divTrunc(batch_end_ns - b.batch_start_ns, std.time.ns_per_ms);
        b.query_latency_histogram[@min(ms_time, b.query_latency_histogram.len - 1)] += 1;

        b.query_index += 1;
        b.query_account_transfers();
    }

    fn summary_query(b: *Benchmark) void {
        const total_ns = b.timer.read();

        const stdout = std.io.getStdOut().writer();

        stdout.print("\n{} queries in {d:.2} s\n", .{
            b.query_count,
            @as(f64, @floatFromInt(total_ns)) / std.time.ns_per_s,
        }) catch unreachable;
        print_percentiles_histogram(stdout, "query", b.query_latency_histogram);

        b.done = true;
    }

    fn validate_accounts(b: *Benchmark) void {
        if (b.account_index >= b.account_count) {
            b.validate_transfers();
            return;
        }

        // Reset batch.
        b.batch_accounts.clearRetainingCapacity();
        b.batch_account_ids.clearRetainingCapacity();

        // Fill batch.
        while (b.account_index < b.account_count and
            b.batch_accounts.items.len < b.batch_accounts.capacity)
        {
            const account = b.create_account();
            b.batch_accounts.appendAssumeCapacity(account);
            b.batch_account_ids.appendAssumeCapacity(account.id);
        }

        b.send(
            validate_accounts_finish,
            .lookup_accounts,
            std.mem.sliceAsBytes(b.batch_account_ids.items),
        );
    }

    fn validate_accounts_finish(
        b: *Benchmark,
        operation: StateMachine.Operation,
        result: []const u8,
    ) void {
        assert(operation == .lookup_accounts);

        const accounts = std.mem.bytesAsSlice(tb.Account, result);

        for (b.batch_accounts.items, accounts) |expected, actual| {
            assert(expected.id == actual.id);
            assert(expected.user_data_128 == actual.user_data_128);
            assert(expected.user_data_64 == actual.user_data_64);
            assert(expected.user_data_32 == actual.user_data_32);
            assert(expected.code == actual.code);
            assert(@as(u16, @bitCast(expected.flags)) == @as(u16, @bitCast(actual.flags)));
        }

        b.validate_accounts();
    }

    fn validate_transfers(b: *Benchmark) void {
        if (b.transfer_index >= b.transfer_count) {
            b.summary_validate();
            return;
        }

        b.batch_transfers.clearRetainingCapacity();
        b.batch_transfer_ids.clearRetainingCapacity();

        b.batch_start_ns = b.timer.read();

        // Fill batch.
        while (b.transfer_index < b.transfer_count and
            b.batch_transfers.items.len < b.batch_transfers.capacity)
        {
            const transfer = b.create_transfer();
            b.batch_transfers.appendAssumeCapacity(transfer);
            b.batch_transfer_ids.appendAssumeCapacity(transfer.id);
        }

        assert(b.batch_transfer_ids.items.len > 0);

        // Submit batch.
        b.send(
            validate_transfers_finish,
            .lookup_transfers,
            std.mem.sliceAsBytes(b.batch_transfer_ids.items),
        );
    }

    fn validate_transfers_finish(
        b: *Benchmark,
        operation: StateMachine.Operation,
        result: []const u8,
    ) void {
        assert(operation == .lookup_transfers);

        const transfers = std.mem.bytesAsSlice(tb.Transfer, result);

        for (b.batch_transfers.items, transfers) |expected, actual| {
            assert(expected.id == actual.id);
            assert(expected.debit_account_id == actual.debit_account_id);
            assert(expected.credit_account_id == actual.credit_account_id);
            assert(expected.amount == actual.amount);
            assert(expected.pending_id == actual.pending_id);
            assert(expected.user_data_128 == actual.user_data_128);
            assert(expected.user_data_64 == actual.user_data_64);
            assert(expected.user_data_32 == actual.user_data_32);
            assert(expected.timeout == actual.timeout);
            assert(expected.ledger == actual.ledger);
            assert(expected.code == actual.code);
            assert(@as(u16, @bitCast(expected.flags)) == @as(u16, @bitCast(actual.flags)));
        }

        b.validate_transfers();
    }

    fn summary_validate(b: *Benchmark) void {
        const stdout = std.io.getStdOut().writer();

        stdout.print("validated {d} accounts, {d} transfers\n", .{
            b.account_count,
            b.transfer_count,
        }) catch unreachable;

        b.done = true;
    }

    fn send(
        b: *Benchmark,
        callback: *const fn (*Benchmark, StateMachine.Operation, []const u8) void,
        operation: StateMachine.Operation,
        payload: []u8,
    ) void {
        b.callback = callback;
        b.client.request(
            send_complete,
            @intCast(@intFromPtr(b)),
            operation,
            payload,
        );
    }

    fn send_complete(
        user_data: u128,
        operation: StateMachine.Operation,
        result: []u8,
    ) void {
        const b: *Benchmark = @ptrFromInt(@as(usize, @intCast(user_data)));
        const callback = b.callback.?;
        b.callback = null;

        callback(b, operation, result);
    }

    fn register_callback(
        user_data: u128,
        result: *const vsr.RegisterResult,
    ) void {
        _ = result;

        const b: *Benchmark = @ptrFromInt(@as(usize, @intCast(user_data)));
        assert(!b.done);
        b.done = true;
    }
};

fn print_percentiles_histogram(
    stdout: anytype,
    label: []const u8,
    histogram_buckets: []const u64,
) void {
    var histogram_total: u64 = 0;
    for (histogram_buckets) |bucket| histogram_total += bucket;

    const percentiles = [_]u64{ 1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 99, 100 };
    for (percentiles) |percentile| {
        const histogram_percentile: usize = @divTrunc(histogram_total * percentile, 100);

        // Since each bucket in our histogram represents 1ms, the bucket we're in is the ms value.
        var sum: usize = 0;
        const latency = for (histogram_buckets, 0..) |bucket, bucket_index| {
            sum += bucket;
            if (sum >= histogram_percentile) break bucket_index;
        } else histogram_buckets.len;

        stdout.print("{s} latency p{} = {} ms{s}\n", .{
            label,
            percentile,
            latency,
            if (latency == histogram_buckets.len) "+ (exceeds histogram resolution)" else "",
        }) catch unreachable;
    }
}

/// Run a series of deterministic workloads against the database.
///
/// After each workload print a summary.
/// Optionally run a second validation pass that checks the state
/// of the database against the transactions generated by each workload.
fn run_workloads(
    allocator: std.mem.Allocator,
    io: *IO,
    client: *Client,
    workloads: []const Workload,
    id_permutation: IdPermutation,
    options: struct {
        rng_seed: u64,
        validate: bool,
    },
) !void {
    const stdout = std.io.getStdOut().writer().any();

    const modes = if (!options.validate)
        &[_]WorkloadRunMode{.transact}
    else
        &[_]WorkloadRunMode{.transact, .validate}
    ;

    for (modes) |mode| {
        var account_index: u64 = 0;
        var transfer_index: u64 = 0;

        for (workloads, 0..) |*workload, i| {

            switch (mode) {
                .transact => {
                    try stdout.print("Running transactional workload {}\n", .{i});
                },
                .validate => {
                    try stdout.print("Running validation workload {}\n", .{i});
                },
            }

            var workload_runner = try WorkloadRunner.init(.{
                .allocator = allocator,
                .workload = workload,
                .workload_index = i,
                .mode = mode,
                .client = client,
                .rng_seed = options.rng_seed,
                .id_permutation = id_permutation,
                .account_index_committed = account_index,
                .transfer_index_committed = transfer_index,
            });
            defer workload_runner.deinit();

            workload_runner.run();

            while (!workload_runner.done) {
                client.tick();
                try io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
            }

            try workload_runner.print_summary(stdout);

            const next_world_state = workload_runner.world_state();
            account_index = next_world_state.account_index;
            transfer_index = next_world_state.transfer_index;
        }
    }

}

/// A description of a workload.
///
/// Each workload is defined by a total number of operations to perform,
/// and a probabilistic combination of all types of operation
/// supported by TigerBeetle, as defined by the various `weight` fields.
/// If e.g. only one `weight` field is non-zero, then the workload will consist
/// of a single type of operation.
///
/// Operations that can be batched have a defined batch size, and will only
/// submit batches of exactly that size, except for the final batch,
/// which will be whatever size is necessary to fulfill the required number
/// of operations.
///
/// In various transactions and queries, ids are selected by a specific
/// random distribution, as defined by the `Distribution` type.
const Workload = struct {
    operation_count: usize,

    create_accounts_weight: u64,
    create_transfers_weight: u64,
    get_account_balances_weight: u64,
    get_account_transfers_weight: u64,
    lookup_accounts_weight: u64,
    lookup_transfers_weight: u64,
    query_accounts_weight: u64,
    query_transfers_weight: u64,

    create_accounts_batch_size: u64,
    create_transfers_batch_size: u64,

    account_distribution_debit: Distribution,
    account_distribution_credit: Distribution,
    account_distribution_query: Distribution,

    flag_history: bool,
    flag_imported: bool,
    transfer_pending: bool,

    fn total_operation_weight(self: *const Workload) u64 {
        return self.create_accounts_weight +
            self.create_transfers_weight +
            self.get_account_balances_weight +
            self.get_account_transfers_weight +
            self.lookup_accounts_weight +
            self.lookup_transfers_weight +
            self.query_accounts_weight +
            self.query_transfers_weight;
    }

    fn next_operation_type(
        self: *const Workload,
        random: std.Random,
    ) StateMachine.Operation {
        const OptionTuple = struct { u64, StateMachine.Operation };
        const options = [_]OptionTuple{
            .{ self.create_accounts_weight, StateMachine.Operation.create_accounts },
            .{ self.create_transfers_weight, StateMachine.Operation.create_transfers },
            .{ self.get_account_balances_weight, StateMachine.Operation.get_account_balances },
            .{ self.get_account_transfers_weight, StateMachine.Operation.get_account_transfers },
            .{ self.lookup_accounts_weight, StateMachine.Operation.lookup_accounts },
            .{ self.lookup_transfers_weight, StateMachine.Operation.lookup_transfers },
            .{ self.query_accounts_weight, StateMachine.Operation.query_accounts },
            .{ self.query_transfers_weight, StateMachine.Operation.query_transfers },
        };

        const rand_max = self.total_operation_weight();
        assert(rand_max > 0);
        var num = random.uintLessThan(u64, rand_max);

        for (options) |option| {
            const weight = option[0];
            const operation = option[1];
            if (num < weight) {
                return operation;
            } else {
                num -= weight;
            }
        }

        unreachable;
    }
};

const WorkloadRunMode = enum { transact, validate };

/// Runs a single `Workload`.
///
/// The workload runs is one of two modes: `transact` or `validate`.
/// Validation mode regenerates the same random operations, but does not submit
/// transactions, instead just querying the database and checking that the state
/// is as expected after a previously run transactional workload.
///
/// Execution is initiated by the `run` method, and driven by an external
/// `IO` event loop, which should check the `done` variable to determine
/// when the workload is complete. A summary can the be printed with `print_summary`.
///
/// The random generation of requests is delegated to `WorkloadOperations`.
const WorkloadRunner = struct {
    workload: *const Workload,
    workload_index: u64,
    mode: WorkloadRunMode,

    allocator: std.mem.Allocator,
    client: *Client,
    rng: std.rand.DefaultPrng,
    timer: std.time.Timer,

    callback: ?*const fn (*WorkloadRunner, StateMachine.Operation, []const u8) void,

    /// Generates pending operations and requests to send to the database.
    operations: WorkloadOperations,
    inflight_request_info: ?struct {
        request: WorkloadInflightRequest,
        start_ns: u64,
    },

    done: bool,

    metrics: WorkloadMetrics,

    /// Buffers for storing the validation payloads.
    validate_buffers: struct {
        create_accounts: std.ArrayListUnmanaged(u128),
        create_transfers: std.ArrayListUnmanaged(u128),
    },

    fn init(options: struct {
        allocator: std.mem.Allocator,
        workload: *const Workload,
        workload_index: u64,
        mode: WorkloadRunMode,
        client: *Client,
        rng_seed: u64,
        id_permutation: IdPermutation,
        account_index_committed: u64,
        transfer_index_committed: u64,
    }) !WorkloadRunner {
        var rng = std.rand.DefaultPrng.init(options.rng_seed +% options.workload_index);
        const random = rng.random();

        const generator = WorkloadGenerator.init(.{
            .workload = options.workload,
            .id_permutation = options.id_permutation,
            .account_index_committed = options.account_index_committed,
            .transfer_index_committed = options.transfer_index_committed,
            .random = random,
        });

        var operations = try WorkloadOperations.init(options.allocator, options.workload, generator);
        errdefer operations.deinit(options.allocator);

        var metrics = try WorkloadMetrics.init(options.allocator);
        errdefer metrics.deinit(options.allocator);

        var validate_buffer_create_accounts = try std.ArrayListUnmanaged(u128).initCapacity(
            options.allocator, options.workload.create_accounts_batch_size,
        );
        errdefer validate_buffer_create_accounts.deinit(options.allocator);
        var validate_buffer_create_transfers = try std.ArrayListUnmanaged(u128).initCapacity(
            options.allocator, options.workload.create_transfers_batch_size,
        );
        errdefer validate_buffer_create_transfers.deinit(options.allocator);

        return WorkloadRunner {
            .workload = options.workload,
            .workload_index = options.workload_index,
            .mode = options.mode,

            .allocator = options.allocator,
            .client = options.client,
            .rng = rng,
            .timer = try std.time.Timer.start(),

            .callback = null,

            .operations = operations,
            .inflight_request_info = null,

            .done = false,

            .metrics = metrics,

            .validate_buffers = .{
                .create_accounts = validate_buffer_create_accounts,
                .create_transfers = validate_buffer_create_transfers,
            },
        };
    }

    fn deinit(self: *WorkloadRunner) void {
        self.operations.deinit(self.allocator);
        self.metrics.deinit(self.allocator);
        self.validate_buffers.create_accounts.deinit(self.allocator);
        self.validate_buffers.create_transfers.deinit(self.allocator);
    }

    fn run(self: *WorkloadRunner) void {
        assert(!self.done);
        self.metrics.workload_start_ns = self.timer.read();
        self.exec_step();
    }

    fn stop(self: *WorkloadRunner) void {
        assert(!self.done);
        self.metrics.workload_end_ns = self.timer.read();
        self.done = true;
    }

    /// Execute a single request and wait for completion.
    ///
    /// It generates a new random request from the `WorkloadOperations` then:
    ///
    /// - In `transact` mode sends that request to the server.
    /// - In `validate` mode, if the generated request is a transaction
    ///   instead sends a query to the server asking about the ids in that transaction.
    fn exec_step(self: *WorkloadRunner) void {
        assert(self.operations.operations_completed < self.workload.operation_count);
        assert(!self.done);

        const random = self.rng.random();
        const request = self.operations.next_request(random);
        const request_start_ns = self.timer.read();

        switch (self.mode) {
            .transact => {
                self.send_request_for_transact_mode(&request);
            },
            .validate => {
                self.send_request_for_validate_mode(&request);
            },
        }

        assert(self.inflight_request_info == null);
        self.inflight_request_info = .{
            .request = request,
            .start_ns = request_start_ns,
        };

        // NB: could do async preparation of next request
        // here for a minor (1%?) throughput improvement.
    }

    /// Completion callback for the request sent by `exec_step`.
    ///
    /// Validates the response, records metrics about the request,
    /// and returns allocated resources to the `WorkloadOperations`.
    fn exec_step_finish(
        self: *WorkloadRunner,
        operation: StateMachine.Operation,
        result: []const u8,
    ) void {
        const request_start_ns = self.inflight_request_info.?.start_ns;
        const request_end_ns = self.timer.read();
        const request_time_ns = request_end_ns - request_start_ns;
        const request_time_ms = @divTrunc(request_time_ns, std.time.ns_per_ms);

        self.metrics.request_total_ns += request_time_ns;
        add_time_ms_to_histogram(self.metrics.all_requests_latency_histogram, request_time_ms);

        switch (self.mode) {
            .transact => {
                handle_response_for_transact_mode(
                    operation,
                    result,
                    &self.inflight_request_info.?.request,
                    request_time_ns,
                    request_time_ms,
                    &self.metrics,
                );
            },
            .validate => {
                handle_response_for_validate_mode(
                    operation,
                    result,
                    &self.inflight_request_info.?.request,
                    request_time_ns,
                    request_time_ms,
                    &self.metrics,
                );
            },
        }

        const random = self.rng.random();
        self.operations.complete_request(
            random,
            self.inflight_request_info.?.request,
        );
        self.inflight_request_info = null;

        assert(self.operations.operations_completed <= self.workload.operation_count);

        if (self.operations.operations_completed == self.workload.operation_count) {
            self.stop();
            return;
        }

        self.exec_step();
    }

    fn send_request_for_transact_mode(
        self: *WorkloadRunner,
        request: *const WorkloadInflightRequest,
    ) void {
        self.send(
            exec_step_finish,
            request.operation,
            request.payload,
        );
    }

    fn handle_response_for_transact_mode(
        operation: StateMachine.Operation,
        result: []const u8,
        inflight_request: *const WorkloadInflightRequest,
        request_time_ns: u64,
        request_time_ms: u64,
        metrics: *WorkloadMetrics,
    ) void {
        switch (inflight_request.data) {
            .create_accounts => |request| {
                assert(operation == StateMachine.Operation.create_accounts);

                const results = std.mem.bytesAsSlice(
                    tb.CreateAccountsResult,
                    result,
                );
                if (results.len > 0) {
                    panic("CreateAccountsResults: {any}", .{results});
                }

                metrics.create_accounts_operations += request.count;
                metrics.create_accounts_requests += 1;
                metrics.create_accounts_total_ns += request_time_ns;
                add_time_ms_to_histogram(metrics.create_accounts_latency_histogram, request_time_ms);

            },
            .create_transfers => |request| {
                assert(operation == StateMachine.Operation.create_transfers);

                const results = std.mem.bytesAsSlice(
                    tb.CreateTransfersResult,
                    result,
                );
                if (results.len > 0) {
                    panic("CreateTransfersResults: {any}", .{results});
                }

                metrics.create_transfers_operations += request.count;
                metrics.create_transfers_requests += 1;
                metrics.create_transfers_total_ns += request_time_ns;
                add_time_ms_to_histogram(metrics.create_transfers_latency_histogram, request_time_ms);

            },
            .get_account_transfers => |request| {
                assert(operation == StateMachine.Operation.get_account_transfers);

                const transfers = std.mem.bytesAsSlice(tb.Transfer, result);
                for (transfers) |*transfer| {
                    assert(transfer.debit_account_id == request.account_id or
                               transfer.credit_account_id == request.account_id);
                }

                metrics.get_account_transfers_operations += 1;
                metrics.get_account_transfers_total_ns += request_time_ns;
                add_time_ms_to_histogram(metrics.get_account_transfers_latency_histogram, request_time_ms);
            }
        }
    }

    fn send_request_for_validate_mode(
        self: *WorkloadRunner,
        inflight_request: *const WorkloadInflightRequest,
    ) void {
        const send_noop_request = struct {
            fn send_noop_request(
                self_: *WorkloadRunner,
            ) void {
                self_.send(
                    exec_step_finish,
                    StateMachine.Operation.lookup_accounts,
                    &[_]u8{},
                );
            }
        }.send_noop_request;

        switch (inflight_request.data) {
            .create_accounts => |request| {
                assert(self.validate_buffers.create_accounts.items.len == 0);
                assert(self.validate_buffers.create_accounts.capacity ==
                           request.accounts.capacity);
                for (request.accounts.items) |*account| {
                    self.validate_buffers.create_accounts.appendAssumeCapacity(account.*.id);
                }
                self.send(
                    exec_step_finish,
                    StateMachine.Operation.lookup_accounts,
                    std.mem.sliceAsBytes(self.validate_buffers.create_accounts.items),
                );
                self.validate_buffers.create_accounts.clearRetainingCapacity();
            },
            .create_transfers => |request| {
                assert(self.validate_buffers.create_transfers.items.len == 0);
                assert(self.validate_buffers.create_transfers.capacity ==
                           request.transfers.capacity);
                for (request.transfers.items) |*transfer| {
                    self.validate_buffers.create_transfers.appendAssumeCapacity(transfer.*.id);
                }
                self.send(
                    exec_step_finish,
                    StateMachine.Operation.lookup_transfers,
                    std.mem.sliceAsBytes(self.validate_buffers.create_transfers.items),
                );
                self.validate_buffers.create_transfers.clearRetainingCapacity();
            },
            .get_account_transfers => |request| {
                _ = request;
                send_noop_request(self);
            },
        }
    }

    fn handle_response_for_validate_mode(
        operation: StateMachine.Operation,
        result: []const u8,
        inflight_request: *const WorkloadInflightRequest,
        request_time_ns: u64,
        request_time_ms: u64,
        metrics: *WorkloadMetrics,
    ) void {
        const handle_noop_request = struct {
            fn handle_noop_request(
                operation_: StateMachine.Operation,
                result_: []const u8,
                request_time_ns_: u64,
                request_time_ms_: u64,
                metrics_: *WorkloadMetrics,
            ) void {
                assert(operation_ == StateMachine.Operation.lookup_accounts);
                assert(result_.len == 0);

                metrics_.validate_noop_operations += 1;
                metrics_.validate_noop_total_ns += request_time_ns_;
                add_time_ms_to_histogram(metrics_.validate_noop_latency_histogram, request_time_ms_);
            }
        }.handle_noop_request;

        switch (inflight_request.data) {
            .create_accounts => |request| {
                assert(operation == StateMachine.Operation.lookup_accounts);

                const accounts = std.mem.bytesAsSlice(tb.Account, result);

                for (request.accounts.items, accounts) |expected, actual| {
                    assert(expected.id == actual.id);
                    assert(expected.user_data_128 == actual.user_data_128);
                    assert(expected.user_data_64 == actual.user_data_64);
                    assert(expected.user_data_32 == actual.user_data_32);
                    assert(expected.code == actual.code);
                    assert(@as(u16, @bitCast(expected.flags)) == @as(u16, @bitCast(actual.flags)));
                }

                metrics.validate_lookup_accounts_operations += request.count;
                metrics.validate_lookup_accounts_requests += 1;
                metrics.validate_lookup_accounts_total_ns += request_time_ns;
                add_time_ms_to_histogram(metrics.validate_lookup_accounts_latency_histogram, request_time_ms);
            },
            .create_transfers => |request| {
                assert(operation == StateMachine.Operation.lookup_transfers);

                const transfers = std.mem.bytesAsSlice(tb.Transfer, result);

                for (request.transfers.items, transfers) |expected, actual| {
                    assert(expected.id == actual.id);
                    assert(expected.debit_account_id == actual.debit_account_id);
                    assert(expected.credit_account_id == actual.credit_account_id);
                    assert(expected.amount == actual.amount);
                    assert(expected.pending_id == actual.pending_id);
                    assert(expected.user_data_128 == actual.user_data_128);
                    assert(expected.user_data_64 == actual.user_data_64);
                    assert(expected.user_data_32 == actual.user_data_32);
                    assert(expected.timeout == actual.timeout);
                    assert(expected.ledger == actual.ledger);
                    assert(expected.code == actual.code);
                    assert(@as(u16, @bitCast(expected.flags)) == @as(u16, @bitCast(actual.flags)));
                }

                metrics.validate_lookup_transfers_operations += request.count;
                metrics.validate_lookup_transfers_requests += 1;
                metrics.validate_lookup_transfers_total_ns += request_time_ns;
                add_time_ms_to_histogram(metrics.validate_lookup_transfers_latency_histogram, request_time_ms);
            },
            .get_account_transfers => |request| {
                _ = request;
                handle_noop_request(
                    operation,
                    result,
                    request_time_ns,
                    request_time_ms,
                    metrics,
                );
            }
        }
    }

    fn send(
        self: *WorkloadRunner,
        callback: *const fn (*WorkloadRunner, StateMachine.Operation, []const u8) void,
        operation: StateMachine.Operation,
        payload: []const u8,
    ) void {
        self.callback = callback;
        self.client.request(
            send_complete,
            @intCast(@intFromPtr(self)),
            operation,
            payload,
        );
    }

    fn send_complete(
        user_data: u128,
        operation: StateMachine.Operation,
        result: []u8,
    ) void {
        const self: *WorkloadRunner = @ptrFromInt(@as(usize, @intCast(user_data)));
        const callback = self.callback.?;
        self.callback = null;

        callback(self, operation, result);
    }

    fn add_time_ms_to_histogram(histogram: []u64, time_ms: u64) void {
        histogram[@min(time_ms, histogram.len - 1)] += 1;
    }

    fn print_summary(
        self: *const WorkloadRunner,
        stdout: std.io.AnyWriter,
    ) !void {
        const summary = WorkloadSummary.init(self);
        try summary.print(stdout);
    }

    fn world_state(self: *const WorkloadRunner) struct {
        account_index: u64,
        transfer_index: u64,
    } {
        return .{
            .account_index = self.operations.generator.account_index_committed,
            .transfer_index = self.operations.generator.transfer_index_committed,
        };
    }
};

/// Generates new data for transactions according to specified distributions,
/// transforms raw generated id indexes into database ids per `IdPermutation`,
/// and maintains information about generated ids.
const WorkloadGenerator = struct {
    workload: *const Workload,

    id_permutation: IdPermutation,

    account_index_uncommitted: usize,
    account_index_committed: usize,
    transfer_index_uncommitted: usize,
    transfer_index_committed: usize,

    account_generator_debit: Generator,
    account_generator_credit: Generator,
    account_generator_query: Generator,

    fn init(options: struct {
        workload: *const Workload,
        id_permutation: IdPermutation,
        account_index_committed: u64,
        transfer_index_committed: u64,
        random: std.Random,
    }) WorkloadGenerator {
        const account_generator_debit = Generator.from_distribution(
            options.workload.account_distribution_debit,
            options.account_index_committed,
            options.random,
        );
        const account_generator_credit = Generator.from_distribution(
            options.workload.account_distribution_credit,
            options.account_index_committed,
            options.random,
        );
        const account_generator_query = Generator.from_distribution(
            options.workload.account_distribution_query,
            options.account_index_committed,
            options.random,
        );

        return WorkloadGenerator {
            .workload = options.workload,

            .id_permutation = options.id_permutation,

            .account_index_uncommitted = options.account_index_committed,
            .account_index_committed = options.account_index_committed,
            .transfer_index_uncommitted = options.transfer_index_committed,
            .transfer_index_committed = options.transfer_index_committed,

            .account_generator_debit = account_generator_debit,
            .account_generator_credit = account_generator_credit,
            .account_generator_query = account_generator_query,
        };
    }

    fn gen_account_index(
        self: *WorkloadGenerator,
        generator: *Generator,
        random: std.Random,
    ) u64 {
        switch (generator.*) {
            .zipfian => |gen| {
                // zipfian set size must be same as account set size
                assert(self.account_index_committed == gen.gen.n);
                const index = gen.next(random);
                assert(index < self.account_index_committed);
                return index;
            },
            .latest => |gen| {
                assert(self.account_index_committed == gen.n);
                const index_rev = gen.next(random);
                assert(index_rev < self.account_index_committed);
                return self.account_index_committed - index_rev - 1;
            },
            .uniform => {
                return random.uintLessThan(u64, self.account_index_committed);
            },
        }
    }

    fn gen_account(self: *WorkloadGenerator, random: std.Random) tb.Account {
        const account = tb.Account {
            .id = self.id_permutation.encode(self.account_index_uncommitted + 1),
            .user_data_128 = random.int(u128),
            .user_data_64 = random.int(u64),
            .user_data_32 = random.int(u32),
            .reserved = 0,
            .ledger = 2,
            .code = 1,
            .flags = .{
                .history = self.workload.flag_history,
                .imported = self.workload.flag_imported,
            },
            .debits_pending = 0,
            .debits_posted = 0,
            .credits_pending = 0,
            .credits_posted = 0,
            .timestamp = if (self.workload.flag_imported) self.account_index_uncommitted + 1 else 0,
        };
        self.account_index_uncommitted += 1;
        return account;
    }

    fn gen_transfer(self: *WorkloadGenerator, random: std.Random) ?tb.Transfer {
        if (self.account_index_committed < 2) {
            assert(self.workload.create_accounts_weight > 0);
            return null;
        }

        var debit_account_index: u64 = 0;
        var credit_account_index: u64 = 0;
        while (debit_account_index == credit_account_index) {
            debit_account_index = self.gen_account_index(&self.account_generator_debit, random);
            credit_account_index = self.gen_account_index(&self.account_generator_credit, random);
        }

        const debit_account_id = self.id_permutation.encode(debit_account_index + 1);
        const credit_account_id = self.id_permutation.encode(credit_account_index + 1);
        assert(debit_account_id != credit_account_id);

        // 30% of pending transfers.
        const pending = self.workload.transfer_pending and random.intRangeAtMost(u8, 0, 9) < 3;

        const transfer = tb.Transfer {
            .id = self.id_permutation.encode(self.transfer_index_uncommitted + 1),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .user_data_128 = random.int(u128),
            .user_data_64 = random.int(u64),
            .user_data_32 = random.int(u32),
            // TODO Benchmark posting/voiding pending transfers.
            .pending_id = 0,
            .ledger = 2,
            .code = random.int(u16) +| 1,
            .flags = .{
                .pending = pending,
                .imported = self.workload.flag_imported,
            },
            .timeout = if (pending) random.intRangeAtMost(u32, 1, 60) else 0,
            .amount = random_int_exponential(random, u64, 10_000) +| 1,
            // FIXME will this clash with account timestamps?
            .timestamp = if (self.workload.flag_imported) self.account_index_uncommitted + self.transfer_index_uncommitted + 1 else 0,
        };
        self.transfer_index_uncommitted += 1;
        return transfer;
    }

    fn gen_account_filter(self: *WorkloadGenerator, random: std.Random) ?tb.AccountFilter {
        if (self.account_index_committed < 1) {
            assert(self.workload.create_accounts_weight > 0);
            return null;
        }

        const account_index = self.gen_account_index(&self.account_generator_query, random);
        const filter = tb.AccountFilter{
            .account_id = self.id_permutation.encode(account_index + 1),
            .user_data_128 = 0,
            .user_data_64 = 0,
            .user_data_32 = 0,
            .code = 0,
            .timestamp_min = 0,
            .timestamp_max = 0,
            .limit = @divExact(
                constants.message_size_max - @sizeOf(vsr.Header),
                @sizeOf(tb.Transfer),
            ),
            .flags = .{
                .credits = true,
                .debits = true,
                .reversed = false,
            },
        };
        return filter;
    }
};

/// Maintains pending database operations and turns them into requests as needed.
const WorkloadOperations = struct {
    workload: *const Workload,
    generator: WorkloadGenerator,

    create_accounts_operations: ?std.ArrayListUnmanaged(tb.Account),
    create_transfers_operations: ?std.ArrayListUnmanaged(tb.Transfer),
    unbatched_operation: ?UnbatchedOperation,

    unbatched_buffers: struct {
        account_filter: ?*?tb.AccountFilter,
    },

    request_in_flight: bool,
    operations_completed: u64,

    fn init(
        allocator: std.mem.Allocator,
        workload: *const Workload,
        generator: WorkloadGenerator,
    ) !WorkloadOperations {
        var create_accounts_operations =
            try std.ArrayListUnmanaged(tb.Account).initCapacity(
                allocator,
                workload.create_accounts_batch_size,
        );
        errdefer create_accounts_operations.deinit(allocator);

        var create_transfers_operations =
            try std.ArrayListUnmanaged(tb.Transfer).initCapacity(
                allocator,
                workload.create_transfers_batch_size,
        );
        errdefer create_transfers_operations.deinit(allocator);

        const buffer_account_filter = try allocator.create(?tb.AccountFilter);
        errdefer allocator.destroy(buffer_account_filter);
        buffer_account_filter.* = null;

        return WorkloadOperations {
            .workload = workload,
            .generator = generator,

            .create_accounts_operations = create_accounts_operations,
            .create_transfers_operations = create_transfers_operations,
            .unbatched_operation = null,

            .unbatched_buffers = .{
                .account_filter = buffer_account_filter,
            },

            .request_in_flight = false,
            .operations_completed = 0,
        };
    }

    fn deinit(self: *WorkloadOperations, allocator: std.mem.Allocator) void {
        assert(!self.request_in_flight);
        self.create_accounts_operations.?.deinit(allocator);
        self.create_transfers_operations.?.deinit(allocator);
        allocator.destroy(self.unbatched_buffers.account_filter.?);
    }

    fn count(self: *const WorkloadOperations) u64 {
        assert(!self.request_in_flight);
        return self.create_accounts_operations.?.items.len
            + self.create_transfers_operations.?.items.len
            + if (self.unbatched_operation == null) @as(u64, 0) else @as(u64, 1);
    }

    fn all_operations_generated(self: *const WorkloadOperations) bool {
        return self.operations_completed + self.count() == self.workload.operation_count;
    }

    fn next_request(
        self: *WorkloadOperations,
        random: std.Random,
    ) WorkloadInflightRequest {
        assert(self.operations_completed + self.count() <= self.workload.operation_count);

        assert(!self.request_in_flight);
        defer self.request_in_flight = true;

        assert(self.create_accounts_operations != null);
        assert(self.create_transfers_operations != null);
        assert(self.unbatched_buffers.account_filter != null);
        assert(self.unbatched_buffers.account_filter.?.* == null);

        while(true) {
            assert(self.create_accounts_operations.?.items.len <= self.workload.create_accounts_batch_size);
            assert(self.create_transfers_operations.?.items.len <= self.workload.create_transfers_batch_size);

            const have_full_account_batch = self.create_accounts_operations.?.items.len == self.workload.create_accounts_batch_size;
            const have_partial_account_batch = self.create_accounts_operations.?.items.len > 0 and !have_full_account_batch;
            const have_full_transfer_batch = self.create_transfers_operations.?.items.len == self.workload.create_transfers_batch_size;
            const have_partial_transfer_batch = self.create_transfers_operations.?.items.len > 0 and !have_full_transfer_batch;

            if (have_full_account_batch) {
                const new_account_count = self.workload.create_accounts_batch_size;
                const accounts = self.create_accounts_operations.?;
                self.create_accounts_operations = null;
                return WorkloadInflightRequest {
                    .operation = StateMachine.Operation.create_accounts,
                    .payload = std.mem.sliceAsBytes(
                        accounts.items[0..new_account_count],
                    ),
                    .data = WorkloadInflightRequestData {
                        .create_accounts = .{
                            .count = new_account_count,
                            .accounts = accounts,
                        },
                    },
                };
            }
            if (have_partial_account_batch and self.all_operations_generated()) {
                const new_account_count = self.create_accounts_operations.?.items.len;
                const accounts = self.create_accounts_operations.?;
                self.create_accounts_operations = null;
                return WorkloadInflightRequest {
                    .operation = StateMachine.Operation.create_accounts,
                    .payload = std.mem.sliceAsBytes(
                        accounts.items[0..new_account_count],
                    ),
                    .data = WorkloadInflightRequestData {
                        .create_accounts = .{
                            .count = new_account_count,
                            .accounts = accounts,
                        },
                    },
                };
            }
            if (have_full_transfer_batch) {
                const new_transfer_count = self.workload.create_transfers_batch_size;
                const transfers = self.create_transfers_operations.?;
                self.create_transfers_operations = null;
                return WorkloadInflightRequest {
                    .operation = StateMachine.Operation.create_transfers,
                    .payload = std.mem.sliceAsBytes(
                        transfers.items[0..new_transfer_count],
                    ),
                    .data = WorkloadInflightRequestData {
                        .create_transfers = .{
                            .count = new_transfer_count,
                            .transfers = transfers,
                        },
                    },
                };
            }
            if (have_partial_transfer_batch and self.all_operations_generated()) {
                const new_transfer_count = self.create_transfers_operations.?.items.len;
                const transfers = self.create_transfers_operations.?;
                self.create_transfers_operations = null;
                return WorkloadInflightRequest {
                    .operation = StateMachine.Operation.create_transfers,
                    .payload = std.mem.sliceAsBytes(
                        transfers.items[0..new_transfer_count],
                    ),
                    .data = WorkloadInflightRequestData {
                        .create_transfers = .{
                            .count = new_transfer_count,
                            .transfers = transfers,
                        },
                    },
                };
            }
            if (self.unbatched_operation) |*op| {
                defer self.unbatched_operation = null;
                switch (op.*) {
                    .get_account_transfers => |*filter| {
                        const filter_buffer = self.unbatched_buffers.account_filter.?;
                        self.unbatched_buffers.account_filter = null;
                        filter_buffer.* = filter.*;
                        return WorkloadInflightRequest {
                            .operation = StateMachine.Operation.get_account_transfers,
                            .payload = std.mem.asBytes(&(filter_buffer.*.?)),
                            .data = WorkloadInflightRequestData {
                                .get_account_transfers = .{
                                    .account_id = filter.account_id,
                                    .filter = filter_buffer,
                                },
                            },
                        };
                    },
                }
            }

            assert(!self.all_operations_generated());

            // fixme it may improve throughput to short circuit the above checks when generating large batches
            self.prepare_one_operation(random);
        }
    }

    fn complete_request(
        self: *WorkloadOperations,
        random: std.Random,
        completed_request: WorkloadInflightRequest,
    ) void {
        assert(self.request_in_flight);
        defer self.request_in_flight = false;

        switch (completed_request.data) {
            .create_accounts => |request| {
                assert(self.create_accounts_operations == null);
                self.create_accounts_operations = request.accounts;
                self.create_accounts_operations.?.clearRetainingCapacity();
                
                self.operations_completed += request.count;

                self.generator.account_index_committed += request.count;

                self.generator.account_generator_debit.grow(request.count, random);
                self.generator.account_generator_credit.grow(request.count, random);
                self.generator.account_generator_query.grow(request.count, random);
            },
            .create_transfers => |request| {
                assert(self.create_transfers_operations == null);
                self.create_transfers_operations = request.transfers;
                self.create_transfers_operations.?.clearRetainingCapacity();

                self.operations_completed += request.count;

                self.generator.transfer_index_committed += request.count;
            },
            .get_account_transfers => |request| {
                assert(self.unbatched_buffers.account_filter == null);
                self.unbatched_buffers.account_filter = request.filter;
                self.unbatched_buffers.account_filter.?.* = null;

                self.operations_completed += 1;
            },
        }
    }

    fn prepare_one_operation(
        self: *WorkloadOperations,
        random: std.Random,
    ) void {
        assert(!self.request_in_flight);
        assert(!self.all_operations_generated());
        assert(self.create_accounts_operations.?.items.len < self.create_accounts_operations.?.capacity);
        assert(self.create_transfers_operations.?.items.len < self.create_transfers_operations.?.capacity);
        assert(self.unbatched_operation == null);

        switch (self.workload.next_operation_type(random)) {
            StateMachine.Operation.create_accounts => {
                const account = self.generator.gen_account(random);
                self.create_accounts_operations.?.appendAssumeCapacity(account);
            },
            StateMachine.Operation.create_transfers => {
                if (self.generator.gen_transfer(random)) |transfer| {
                    self.create_transfers_operations.?.appendAssumeCapacity(transfer);
                }
            },
            StateMachine.Operation.get_account_transfers => {
                if (self.generator.gen_account_filter(random)) |filter| {
                    self.unbatched_operation = UnbatchedOperation {
                        .get_account_transfers = filter,
                    };
                }
            },
            else => @panic("todo"),
        }
    }
};

const UnbatchedOperation = union(enum) {
    get_account_transfers: tb.AccountFilter,
};

const WorkloadInflightRequest = struct {
    operation: StateMachine.Operation,
    payload: []const u8,
    data: WorkloadInflightRequestData,
};

const WorkloadInflightRequestData = union(enum) {
    create_accounts: struct {
        count: u64,
        accounts: std.ArrayListUnmanaged(tb.Account),
    },
    create_transfers: struct {
        count: u64,
        transfers: std.ArrayListUnmanaged(tb.Transfer),
    },
    get_account_transfers: struct {
        account_id: u128,
        filter: *?tb.AccountFilter,
    },
};

const WorkloadMetrics = struct {
    workload_start_ns: u64 = 0,
    workload_end_ns: u64 = 0,
    request_total_ns: u64 = 0,

    all_requests_latency_histogram: []u64,

    create_accounts_operations: u64 = 0,
    create_accounts_requests: u64 = 0,
    create_accounts_total_ns: u64 = 0,
    create_accounts_latency_histogram: []u64,

    create_transfers_operations: u64 = 0,
    create_transfers_requests: u64 = 0,
    create_transfers_total_ns: u64 = 0,
    create_transfers_latency_histogram: []u64,

    get_account_transfers_operations: u64 = 0,
    get_account_transfers_total_ns: u64 = 0,
    get_account_transfers_latency_histogram: []u64,

    validate_lookup_accounts_operations: u64 = 0,
    validate_lookup_accounts_requests: u64 = 0,
    validate_lookup_accounts_total_ns: u64 = 0,
    validate_lookup_accounts_latency_histogram: []u64,

    validate_lookup_transfers_operations: u64 = 0,
    validate_lookup_transfers_requests: u64 = 0,
    validate_lookup_transfers_total_ns: u64 = 0,
    validate_lookup_transfers_latency_histogram: []u64,

    validate_noop_operations: u64 = 0,
    validate_noop_total_ns: u64 = 0,
    validate_noop_latency_histogram: []u64,

    fn init(allocator: std.mem.Allocator) !WorkloadMetrics {
        const all_requests_latency_histogram =
            try allocator.alloc(u64, 10_001);
        @memset(all_requests_latency_histogram, 0);
        errdefer allocator.free(all_requests_latency_histogram);
        const create_accounts_latency_histogram =
            try allocator.alloc(u64, 10_001);
        @memset(create_accounts_latency_histogram, 0);
        errdefer allocator.free(create_accounts_latency_histogram);
        const create_transfers_latency_histogram =
            try allocator.alloc(u64, 10_001);
        @memset(create_transfers_latency_histogram, 0);
        errdefer allocator.free(create_transfers_latency_histogram);
        const get_account_transfers_latency_histogram =
            try allocator.alloc(u64, 10_001);
        @memset(get_account_transfers_latency_histogram, 0);
        errdefer allocator.free(get_account_transfers_latency_histogram);
        const validate_lookup_accounts_latency_histogram =
            try allocator.alloc(u64, 10_001);
        @memset(validate_lookup_accounts_latency_histogram, 0);
        errdefer allocator.free(validate_lookup_accounts_latency_histogram);
        const validate_lookup_transfers_latency_histogram =
            try allocator.alloc(u64, 10_001);
        @memset(validate_lookup_transfers_latency_histogram, 0);
        errdefer allocator.free(validate_lookup_transfers_latency_histogram);
        const validate_noop_latency_histogram =
            try allocator.alloc(u64, 10_001);
        @memset(validate_noop_latency_histogram, 0);
        errdefer allocator.free(validate_noop_latency_histogram);

        return WorkloadMetrics {
            .all_requests_latency_histogram = all_requests_latency_histogram,
            .create_accounts_latency_histogram = create_accounts_latency_histogram,
            .create_transfers_latency_histogram = create_transfers_latency_histogram,
            .get_account_transfers_latency_histogram = get_account_transfers_latency_histogram,
            .validate_lookup_accounts_latency_histogram = validate_lookup_accounts_latency_histogram,
            .validate_lookup_transfers_latency_histogram = validate_lookup_transfers_latency_histogram,
            .validate_noop_latency_histogram = validate_noop_latency_histogram,
        };
    }

    fn deinit(self: *WorkloadMetrics, allocator: std.mem.Allocator) void {
        allocator.free(self.all_requests_latency_histogram);
        allocator.free(self.create_accounts_latency_histogram);
        allocator.free(self.create_transfers_latency_histogram);
        allocator.free(self.get_account_transfers_latency_histogram);
        allocator.free(self.validate_lookup_accounts_latency_histogram);
        allocator.free(self.validate_lookup_transfers_latency_histogram);
        allocator.free(self.validate_noop_latency_histogram);
    }
};

const WorkloadSummary = struct {
    workload_index: u64,

    total_workload_time_s: f64,
    total_request_time_s: f64,
    total_non_request_time_s: f64,
    total_operations: u64,
    total_requests: u64,
    total_request_latencies: LatencyPercentiles,

    create_accounts_request_time_s: f64,
    create_accounts_operations: u64,
    create_accounts_requests: u64,
    create_accounts_batch_size: u64,
    create_accounts_batch_size_rem: u64,
    create_accounts_latencies: LatencyPercentiles,

    create_transfers_request_time_s: f64,
    create_transfers_operations: u64,
    create_transfers_requests: u64,
    create_transfers_batch_size: u64,
    create_transfers_batch_size_rem: u64,
    create_transfers_latencies: LatencyPercentiles,

    get_account_transfers_request_time_s: f64,
    get_account_transfers_operations: u64,
    get_account_transfers_latencies: LatencyPercentiles,

    validate_lookup_accounts_request_time_s: f64,
    validate_lookup_accounts_operations: u64,
    validate_lookup_accounts_requests: u64,
    validate_lookup_accounts_batch_size: u64,
    validate_lookup_accounts_batch_size_rem: u64,
    validate_lookup_accounts_latencies: LatencyPercentiles,

    validate_lookup_transfers_request_time_s: f64,
    validate_lookup_transfers_operations: u64,
    validate_lookup_transfers_requests: u64,
    validate_lookup_transfers_batch_size: u64,
    validate_lookup_transfers_batch_size_rem: u64,
    validate_lookup_transfers_latencies: LatencyPercentiles,

    validate_noop_request_time_s: f64,
    validate_noop_operations: u64,
    validate_noop_latencies: LatencyPercentiles,

    fn init(runner: *const WorkloadRunner) WorkloadSummary {
        const total_workload_time_ns = runner.metrics.workload_end_ns - runner.metrics.workload_start_ns;
        const total_workload_time_s = @as(f64, @floatFromInt(total_workload_time_ns)) / std.time.ns_per_s;
        const total_request_time_s = @as(f64, @floatFromInt(runner.metrics.request_total_ns)) / std.time.ns_per_s;
        const total_non_request_time_s = total_workload_time_s - total_request_time_s;

        const create_accounts_request_time_s = @as(f64, @floatFromInt(runner.metrics.create_accounts_total_ns)) / std.time.ns_per_s;
        const create_transfers_request_time_s = @as(f64, @floatFromInt(runner.metrics.create_transfers_total_ns)) / std.time.ns_per_s;
        const get_account_transfers_request_time_s = @as(f64, @floatFromInt(runner.metrics.get_account_transfers_total_ns)) / std.time.ns_per_s;
        const validate_lookup_accounts_request_time_s = @as(f64, @floatFromInt(runner.metrics.validate_lookup_accounts_total_ns)) / std.time.ns_per_s;
        const validate_lookup_transfers_request_time_s = @as(f64, @floatFromInt(runner.metrics.validate_lookup_transfers_total_ns)) / std.time.ns_per_s;
        const validate_noop_request_time_s = @as(f64, @floatFromInt(runner.metrics.validate_noop_total_ns)) / std.time.ns_per_s;

        const total_operations = runner.metrics.create_accounts_operations +
            runner.metrics.create_transfers_operations +
            runner.metrics.get_account_transfers_operations +
            runner.metrics.validate_lookup_accounts_operations +
            runner.metrics.validate_lookup_transfers_operations +
            runner.metrics.validate_noop_operations;
        const total_requests = runner.metrics.create_accounts_requests +
            runner.metrics.create_transfers_requests +
            runner.metrics.get_account_transfers_operations +
            runner.metrics.validate_lookup_accounts_requests +
            runner.metrics.validate_lookup_transfers_requests +
            runner.metrics.validate_noop_operations;

        const create_accounts_batch_size_rem =
            std.math.rem(
                u64,
                runner.metrics.create_accounts_operations,
                runner.workload.create_accounts_batch_size,
        ) catch 0;
        const create_transfers_batch_size_rem =
            std.math.rem(
                u64,
                runner.metrics.create_transfers_operations,
                runner.workload.create_transfers_batch_size,
        ) catch 0;
        const validate_lookup_accounts_batch_size_rem =
            std.math.rem(
                u64,
                runner.metrics.validate_lookup_accounts_operations,
                // nb - same batch size as create_accounts
                runner.workload.create_accounts_batch_size,
        ) catch 0;
        const validate_lookup_transfers_batch_size_rem =
            std.math.rem(
                u64,
                runner.metrics.validate_lookup_transfers_operations,
                // nb - same batch size as create_transfers
                runner.workload.create_transfers_batch_size,
        ) catch 0;

        return WorkloadSummary {
            .workload_index = runner.workload_index,

            .total_workload_time_s = total_workload_time_s,
            .total_request_time_s = total_request_time_s,
            .total_non_request_time_s = total_non_request_time_s,
            .total_operations = total_operations,
            .total_requests = total_requests,
            .total_request_latencies = LatencyPercentiles.from_histogram(
                runner.metrics.all_requests_latency_histogram,
            ),

            .create_accounts_request_time_s = create_accounts_request_time_s,
            .create_accounts_operations = runner.metrics.create_accounts_operations,
            .create_accounts_requests = runner.metrics.create_accounts_requests,
            .create_accounts_batch_size = runner.workload.create_accounts_batch_size,
            .create_accounts_batch_size_rem = create_accounts_batch_size_rem,
            .create_accounts_latencies = LatencyPercentiles.from_histogram(
                runner.metrics.create_accounts_latency_histogram,
            ),

            .create_transfers_request_time_s = create_transfers_request_time_s,
            .create_transfers_operations = runner.metrics.create_transfers_operations,
            .create_transfers_requests = runner.metrics.create_transfers_requests,
            .create_transfers_batch_size = runner.workload.create_transfers_batch_size,
            .create_transfers_batch_size_rem = create_transfers_batch_size_rem,
            .create_transfers_latencies = LatencyPercentiles.from_histogram(
                runner.metrics.create_transfers_latency_histogram,
            ),

            .get_account_transfers_request_time_s = get_account_transfers_request_time_s,
            .get_account_transfers_operations = runner.metrics.get_account_transfers_operations,
            .get_account_transfers_latencies = LatencyPercentiles.from_histogram(
                runner.metrics.get_account_transfers_latency_histogram,
            ),

            .validate_lookup_accounts_request_time_s = validate_lookup_accounts_request_time_s,
            .validate_lookup_accounts_operations = runner.metrics.validate_lookup_accounts_operations,
            .validate_lookup_accounts_requests = runner.metrics.validate_lookup_accounts_requests,
            // nb - same batch size as create_accounts
            .validate_lookup_accounts_batch_size = runner.workload.create_accounts_batch_size,
            .validate_lookup_accounts_batch_size_rem = validate_lookup_accounts_batch_size_rem,
            .validate_lookup_accounts_latencies = LatencyPercentiles.from_histogram(
                runner.metrics.validate_lookup_accounts_latency_histogram,
            ),

            .validate_lookup_transfers_request_time_s = validate_lookup_transfers_request_time_s,
            .validate_lookup_transfers_operations = runner.metrics.validate_lookup_transfers_operations,
            .validate_lookup_transfers_requests = runner.metrics.validate_lookup_transfers_requests,
            // nb - same batch size as create_transfers
            .validate_lookup_transfers_batch_size = runner.workload.create_transfers_batch_size,
            .validate_lookup_transfers_batch_size_rem = validate_lookup_transfers_batch_size_rem,
            .validate_lookup_transfers_latencies = LatencyPercentiles.from_histogram(
                runner.metrics.validate_lookup_transfers_latency_histogram,
            ),

            .validate_noop_request_time_s = validate_noop_request_time_s,
            .validate_noop_operations = runner.metrics.validate_noop_operations,
            .validate_noop_latencies = LatencyPercentiles.from_histogram(
                runner.metrics.validate_noop_latency_histogram,
            ),
        };
    }

    fn print(
        self: *const WorkloadSummary,
        stdout: std.io.AnyWriter,
    ) !void {
        try stdout.print(
            "-----\n", .{},
        );
        try stdout.print(
            "# Workload {}\n",
            .{self.workload_index},
        );
        try stdout.print(
            "total time: {d:.2}\n",
            .{self.total_workload_time_s},
        );
        try stdout.print(
            "total request time: {d:.2}s\n",
            .{self.total_request_time_s},
        );
        try stdout.print(
            "total non-request time: {d:.2}s\n",
            .{self.total_non_request_time_s},
        );
        try stdout.print(
            "total operations: {}\n",
            .{self.total_operations},
        );
        try stdout.print(
            "total requests: {}\n",
            .{self.total_requests},
        );
        try self.total_request_latencies.print(stdout, "total");
        if (self.create_accounts_operations > 0) {
            try stdout.print(
                "create_accounts request time: {d:.2}s\n",
                .{self.create_accounts_request_time_s},
            );
            try stdout.print(
                "create_accounts operations: {}\n",
                .{self.create_accounts_operations},
            );
            try stdout.print(
                "create_accounts requests: {}\n",
                .{self.create_accounts_requests},
            );
            try stdout.print(
                "create_accounts batch size: {}\n",
                .{self.create_accounts_batch_size},
            );
            try stdout.print(
                "create_accounts final batch size: {}\n",
                .{self.create_accounts_batch_size_rem},
            );
            try self.create_accounts_latencies.print(stdout, "create_accounts");
        }
        if (self.create_transfers_operations > 0) {
            try stdout.print(
                "create_transfers request time: {d:.2}s\n",
                .{self.create_transfers_request_time_s},
            );
            try stdout.print(
                "create_transfers operations: {}\n",
                .{self.create_transfers_operations},
            );
            try stdout.print(
                "create_transfers requests: {}\n",
                .{self.create_transfers_requests},
            );
            try stdout.print(
                "create_transfers batch size: {}\n",
                .{self.create_transfers_batch_size},
            );
            try stdout.print(
                "create_transfers final batch size: {}\n",
                .{self.create_transfers_batch_size_rem},
            );
            try self.create_transfers_latencies.print(stdout, "create_transfers");
        }
        if (self.get_account_transfers_operations > 0) {
            try stdout.print(
                "get_account_transfers request time: {d:.2}s\n",
                .{self.get_account_transfers_request_time_s},
            );
            try stdout.print(
                "get_account_transfers operations: {}\n",
                .{self.get_account_transfers_operations},
            );
            try self.get_account_transfers_latencies.print(stdout, "get_account_transfers");
        }
        if (self.validate_lookup_accounts_operations > 0) {
            try stdout.print(
                "validate_lookup_accounts request time: {d:.2}s\n",
                .{self.validate_lookup_accounts_request_time_s},
            );
            try stdout.print(
                "validate_lookup_accounts operations: {}\n",
                .{self.validate_lookup_accounts_operations},
            );
            try stdout.print(
                "validate_lookup_accounts requests: {}\n",
                .{self.validate_lookup_accounts_requests},
            );
            try stdout.print(
                "validate_lookup_accounts batch size: {}\n",
                .{self.validate_lookup_accounts_batch_size},
            );
            try stdout.print(
                "validate_lookup_accounts final batch size: {}\n",
                .{self.validate_lookup_accounts_batch_size_rem},
            );
            try self.validate_lookup_accounts_latencies.print(stdout, "validate_lookup_accounts");
        }
        if (self.validate_lookup_transfers_operations > 0) {
            try stdout.print(
                "validate_lookup_transfers request time: {d:.2}s\n",
                .{self.validate_lookup_transfers_request_time_s},
            );
            try stdout.print(
                "validate_lookup_transfers operations: {}\n",
                .{self.validate_lookup_transfers_operations},
            );
            try stdout.print(
                "validate_lookup_transfers requests: {}\n",
                .{self.validate_lookup_transfers_requests},
            );
            try stdout.print(
                "validate_lookup_transfers batch size: {}\n",
                .{self.validate_lookup_transfers_batch_size},
            );
            try stdout.print(
                "validate_lookup_transfers final batch size: {}\n",
                .{self.validate_lookup_transfers_batch_size_rem},
            );
            try self.validate_lookup_transfers_latencies.print(stdout, "validate_lookup_transfers");
        }
        if (self.validate_noop_operations > 0) {
            try stdout.print(
                "validate_noop request time: {d:.2}s\n",
                .{self.validate_noop_request_time_s},
            );
            try stdout.print(
                "validate_noop operations: {}\n",
                .{self.validate_noop_operations},
            );
            try self.validate_noop_latencies.print(stdout, "validate_noop");
        }
        try stdout.print(
            "-----\n", .{},
        );
    }
};

const LatencyPercentiles = struct {
    percentiles: [7]u64,
    latencies: [7]u64,
    exceptions: [7]bool,

    fn from_histogram(histogram_buckets: []const u64) LatencyPercentiles {
        var histogram_total: u64 = 0;
        for (histogram_buckets) |bucket| histogram_total += bucket;

        const percentiles = [_]u64{ 1, 10, 50, 90, 95, 99, 100 };
        var latencies = [_]u64{ 0 } ** percentiles.len;
        var exceptions = [_]bool{ false } ** percentiles.len;
        for (percentiles, 0..) |percentile, percentile_index| {
            const histogram_percentile: usize = @divTrunc(histogram_total * percentile, 100);

            // Since each bucket in our histogram represents 1ms, the bucket we're in is the ms value.
            var sum: usize = 0;
            const latency = for (histogram_buckets, 0..) |bucket, bucket_index| {
                sum += bucket;
                if (sum >= histogram_percentile) break bucket_index;
            } else histogram_buckets.len;
            latencies[percentile_index] = latency;

            if (latency == histogram_buckets.len) {
                exceptions[percentile_index] = true;
            }
        }

        return LatencyPercentiles {
            .percentiles = percentiles,
            .latencies = latencies,
            .exceptions = exceptions,
        };
    }

    fn print(
        self: *const LatencyPercentiles,
        stdout: std.io.AnyWriter,
        label: []const u8,
    ) !void {
        var have_exception = false;
        var exception_chars = [_]u8{ ' ' } ** 7;
        assert(self.exceptions.len == exception_chars.len);
        for (self.exceptions, 0..) |exception, i| {
            if (exception) {
                have_exception = true;
                exception_chars[i] = '*';
            }
        }

        try stdout.print("{s} latency histogram:\n", .{label});
        try stdout.print("|p |p{: <3}  |p{: <3}  |p{: <3}  |p{: <3}  |p{: <3}  |p{: <3}  |p{: <3}\n", .{
            self.percentiles[0], self.percentiles[1],
            self.percentiles[2], self.percentiles[3],
            self.percentiles[4], self.percentiles[5],
            self.percentiles[6],
        });
        try stdout.print("|ms|{: >5}{c}|{: >5}{c}|{: >5}{c}|{: >5}{c}|{: >5}{c}|{: >5}{c}|{: >5}{c}\n", .{
            self.latencies[0], exception_chars[0],
            self.latencies[1], exception_chars[1],
            self.latencies[2], exception_chars[2],
            self.latencies[3], exception_chars[3],
            self.latencies[4], exception_chars[4],
            self.latencies[5], exception_chars[5],
            self.latencies[6], exception_chars[6],
        });

        if (have_exception) {
            try stdout.print(" *: exceeds histogram resolution", .{});
        }
    }
};
