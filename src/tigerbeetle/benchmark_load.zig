//! Start TigerBeetle clients to run a workload (described below) against a cluster while measuring
//! observed latencies and throughput.
//!
//! The benchmark only generates valid data -- i.e., all accounts/transfers succeed.
//!
//! Workload steps:
//! 1. Create accounts.
//! 2. Create transfers.
//! 3. Query account transfers (`get_account_transfers`).
//! 4. Lookup accounts (when verification is enabled).
//! 5. Lookup transfers (when verification is enabled).
const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const panic = std.debug.panic;
const log = std.log.scoped(.benchmark);

const vsr = @import("vsr");
const tb = vsr.tigerbeetle;
const constants = vsr.constants;
const stdx = vsr.stdx;
const ratio = stdx.PRNG.ratio;
const flags = vsr.flags;
const random_int_exponential = vsr.testing.random_int_exponential;
const IO = vsr.io.IO;
const Time = vsr.time.Time;
const Duration = stdx.Duration;
const MessagePool = vsr.message_pool.MessagePool;
const MessageBus = vsr.message_bus.MessageBusType(IO);
const Client = vsr.ClientType(tb.Operation, MessageBus);
const IdPermutation = vsr.testing.IdPermutation;
const ZipfianGenerator = stdx.ZipfianGenerator;
const ZipfianShuffled = stdx.ZipfianShuffled;

const cli = @import("./cli.zig");

pub fn main(
    allocator: std.mem.Allocator,
    io: *IO,
    time: Time,
    addresses: []const std.net.Address,
    cli_args: *const cli.Command.Benchmark,
) !void {
    if (builtin.mode != .ReleaseSafe and builtin.mode != .ReleaseFast) {
        log.warn("Benchmark must be built with '-Drelease' for reasonable results.", .{});
    }
    if (!vsr.constants.config.process.direct_io) {
        log.warn("Direct IO is disabled.", .{});
    }
    if (vsr.constants.config.process.verify) {
        log.warn("Extra assertions are enabled.", .{});
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

    if (cli_args.clients == 0 or cli_args.clients > constants.clients_max) vsr.fatal(
        .cli,
        "--clients: must be between 1 and {}, got {}",
        .{ constants.clients_max, cli_args.clients },
    );

    const cluster_id: u128 = 0;

    var message_pools = stdx.BoundedArrayType(MessagePool, constants.clients_max){};
    defer for (message_pools.slice()) |*message_pool| message_pool.deinit(allocator);
    for (0..cli_args.clients) |_| {
        message_pools.push(try MessagePool.init(allocator, .client));
    }

    std.log.info("Benchmark running against {any}", .{addresses});

    var clients = stdx.BoundedArrayType(Client, constants.clients_max){};
    defer for (clients.slice()) |*client| client.deinit(allocator);

    for (0..cli_args.clients) |i| {
        clients.push(try Client.init(
            allocator,
            time,
            &message_pools.slice()[i],
            .{
                .id = stdx.unique_u128(),
                .cluster = cluster_id,
                .replica_count = @intCast(addresses.len),
                .message_bus_options = .{ .configuration = addresses, .io = io },
            },
        ));
    }

    // Each array position corresponds to a histogram bucket of 1ms. The last bucket is 10_000ms+.
    const request_latency_histogram = try allocator.alloc(u64, 10_001);
    @memset(request_latency_histogram, 0);
    defer allocator.free(request_latency_histogram);

    const client_timeouts = try allocator.alloc(Benchmark.Timeout, clients.count());
    defer allocator.free(client_timeouts);

    const client_requests = try allocator.alignedAlloc(
        [constants.message_body_size_max]u8,
        constants.sector_size,
        clients.count(),
    );
    defer allocator.free(client_requests);

    const client_replies = try allocator.alignedAlloc(
        [constants.message_body_size_max]u8,
        constants.sector_size,
        clients.count(),
    );
    defer allocator.free(client_replies);

    // If no seed was given, use a default seed for reproducibility.
    const seed = seed_from_arg: {
        const seed_argument = cli_args.seed orelse break :seed_from_arg 42;
        break :seed_from_arg vsr.testing.parse_seed(seed_argument);
    };

    log.info("Benchmark seed = {}", .{seed});

    var prng = stdx.PRNG.from_seed(seed);
    const account_id_permutation: IdPermutation = switch (cli_args.id_order) {
        .sequential => .{ .identity = {} },
        .random => .{ .random = prng.int(u64) },
        .reversed => .{ .inversion = {} },
    };

    assert(cli_args.account_count >= cli_args.account_count_hot);
    const account_generator = Generator.from_distribution(
        cli_args.account_distribution,
        cli_args.account_count - cli_args.account_count_hot,
        &prng,
    );
    const account_generator_hot = Generator.from_distribution(
        cli_args.account_distribution,
        cli_args.account_count_hot,
        &prng,
    );

    log.info("Account distribution: {s}", .{
        @tagName(cli_args.account_distribution),
    });

    var benchmark = Benchmark{
        .io = io,
        .prng = &prng,
        .timer = try std.time.Timer.start(),
        .output = std.io.getStdOut().writer().any(),
        .clients = clients.slice(),
        .client_timeouts = client_timeouts,
        .client_requests = client_requests,
        .client_replies = client_replies,
        .request_latency_histogram = request_latency_histogram,
        .account_id_permutation = account_id_permutation,
        .account_batch_size = cli_args.account_batch_size,
        .account_count = cli_args.account_count,
        .account_count_hot = cli_args.account_count_hot,
        .account_generator = account_generator,
        .account_generator_hot = account_generator_hot,
        .transfer_id_permutation = account_id_permutation,
        .transfer_batch_size = cli_args.transfer_batch_size,
        .transfer_batch_delay = cli_args.transfer_batch_delay,
        .transfer_count = cli_args.transfer_count,
        .transfer_hot_percent = cli_args.transfer_hot_percent,
        .transfer_pending = cli_args.transfer_pending,
        .query_count = cli_args.query_count,
        .flag_history = cli_args.flag_history,
        .flag_imported = cli_args.flag_imported,
        .validate = cli_args.validate,
        .print_batch_timings = cli_args.print_batch_timings,
    };

    try benchmark.run(.register);

    var prng_init = prng;
    {
        try benchmark.run(.create_accounts);
        try benchmark.run(.create_transfers);
        if (benchmark.query_count > 0) {
            try benchmark.run(.get_account_transfers);
        }
    }

    if (benchmark.validate) {
        // Reset our state so we can check our work.
        benchmark.prng = &prng_init;
        try benchmark.run(.validate_accounts);
        try benchmark.run(.validate_transfers);
    }

    if (cli_args.checksum_performance) {
        const buffer = try allocator.alloc(u8, constants.message_size_max);
        defer allocator.free(buffer);

        benchmark.prng.fill(buffer);

        benchmark.timer.reset();
        _ = vsr.checksum(buffer);
        const checksum_duration_ns = benchmark.timer.read();

        benchmark.output.print(
            \\message size max = {} bytes
            \\checksum message size max = {} us
            \\
        , .{
            constants.message_size_max,
            @divTrunc(checksum_duration_ns, std.time.ns_per_us),
        }) catch unreachable;
    }
}

const Generator = union(enum) {
    zipfian: ZipfianShuffled,
    latest: ZipfianGenerator,
    uniform: u64,

    fn from_distribution(
        distribution: cli.Command.Benchmark.Distribution,
        count: u64,
        prng: *stdx.PRNG,
    ) Generator {
        return switch (distribution) {
            .zipfian => .{ .zipfian = ZipfianShuffled.init(count, prng) },
            .latest => .{ .latest = ZipfianGenerator.init(count) },
            .uniform => .{ .uniform = count },
        };
    }
};

const Benchmark = struct {
    io: *IO,
    prng: *stdx.PRNG,
    timer: std.time.Timer,
    output: std.io.AnyWriter,
    clients: []Client,

    // Configuration:
    account_id_permutation: IdPermutation,
    account_batch_size: u32,
    account_count: u32,
    account_count_hot: u32,
    account_generator: Generator,
    account_generator_hot: Generator,
    transfer_id_permutation: IdPermutation,
    transfer_batch_size: u32,
    transfer_batch_delay: Duration,
    transfer_count: u64,
    transfer_hot_percent: u32,
    transfer_pending: bool,
    query_count: u32,
    flag_history: bool,
    flag_imported: bool,
    validate: bool,
    print_batch_timings: bool,

    // State:
    clients_busy: stdx.BitSetType(constants.clients_max) = .{},
    clients_request_ns: [constants.clients_max]u64 = @splat(undefined),
    client_requests: []align(constants.sector_size) [constants.message_body_size_max]u8,
    client_replies: []align(constants.sector_size) [constants.message_body_size_max]u8,
    client_timeouts: []Timeout,
    request_latency_histogram: []u64,
    request_index: usize = 0,
    account_index: usize = 0,
    transfer_index: usize = 0,
    transfers_created: usize = 0,
    query_index: usize = 0,
    stage: Stage = .idle,

    const Timeout = struct {
        benchmark: *Benchmark,
        client_index: u32,
        completion: IO.Completion = undefined,
    };

    const Stage = enum {
        idle,
        register,
        create_accounts,
        create_transfers,
        get_account_transfers,
        validate_accounts,
        validate_transfers,
    };

    pub fn run(b: *Benchmark, stage: Stage) !void {
        assert(b.stage == .idle);
        assert(b.clients.len > 0);
        assert(b.clients_busy.empty());
        assert(stdx.zeroed(std.mem.sliceAsBytes(b.request_latency_histogram)));
        assert(b.request_index == 0);
        assert(b.account_index == 0);
        assert(b.transfer_index == 0);
        assert(b.query_index == 0);
        assert(stage != .idle);

        b.stage = stage;
        b.timer.reset();

        for (0..b.clients.len) |client_usize| {
            const client = @as(u32, @intCast(client_usize));
            switch (b.stage) {
                .register => b.register(client),
                .create_accounts => b.create_accounts(client),
                .create_transfers => b.create_transfers(client),
                .get_account_transfers => b.get_account_transfers(client),
                .validate_accounts => b.validate_accounts(client),
                .validate_transfers => b.validate_transfers(client),
                .idle => break, // i-1 decided not to start any work.
            }
        }

        while (b.stage != .idle) {
            for (b.clients) |*client| client.tick();
            try b.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
        }
    }

    fn run_finish(b: *Benchmark) void {
        assert(b.stage != .idle);
        assert(b.clients_busy.empty());

        b.stage = .idle;
        b.request_index = 0;
        b.account_index = 0;
        b.transfer_index = 0;
        b.query_index = 0;
        @memset(b.request_latency_histogram, 0);
    }

    fn register(b: *Benchmark, client_index: usize) void {
        assert(b.stage == .register);
        assert(!b.clients_busy.is_set(client_index));

        b.clients_busy.set(client_index);
        b.clients[client_index].register(register_callback, @bitCast(RequestContext{
            .benchmark = b,
            .client_index = @intCast(client_index),
            .request_index = undefined,
        }));
        b.request_index += 1;
    }

    fn register_callback(user_data: u128, _: *const vsr.RegisterResult) void {
        const context: RequestContext = @bitCast(user_data);
        const b: *Benchmark = context.benchmark;
        assert(b.stage == .register);
        assert(b.clients_busy.is_set(context.client_index));

        b.clients_busy.unset(context.client_index);
        if (b.clients_busy.empty()) b.run_finish();
    }

    fn create_accounts(b: *Benchmark, client_index: u32) void {
        assert(b.stage == .create_accounts);
        assert(!b.clients_busy.is_set(client_index));
        assert(b.account_batch_size > 0);
        assert(b.account_index <= b.account_count);
        if (b.account_index == b.account_count) {
            if (b.clients_busy.empty()) b.run_finish();
            return;
        }

        const account_count: u32 = @intCast(@min(
            b.account_count - b.account_index,
            b.account_batch_size,
        ));
        const accounts = stdx.bytes_as_slice(
            .exact,
            tb.Account,
            &b.client_requests[client_index],
        )[0..account_count];
        b.build_accounts(accounts);
        b.request(client_index, .create_accounts, .{
            .batch_count = account_count,
            .event_size = @sizeOf(tb.Account),
        });
    }

    fn create_accounts_callback(b: *Benchmark, client_index: u32, result: []const u8) void {
        assert(b.stage == .create_accounts);

        const create_accounts_results = stdx.bytes_as_slice(
            .exact,
            tb.CreateAccountsResult,
            result,
        );
        if (create_accounts_results.len > 0) {
            panic("CreateAccountsResults: {any}", .{create_accounts_results});
        }
        b.create_accounts(client_index);
    }

    fn create_transfers(b: *Benchmark, client_index: u32) void {
        assert(b.stage == .create_transfers);
        assert(!b.clients_busy.is_set(client_index));
        assert(b.transfer_batch_size > 0);
        assert(b.transfer_index <= b.transfer_count);
        if (b.transfer_index == b.transfer_count) {
            if (b.clients_busy.empty()) b.create_transfers_finish();
            return;
        }

        const transfer_count: u32 = @intCast(@min(
            b.transfer_count - b.transfer_index,
            b.transfer_batch_size,
        ));
        const transfers = stdx.bytes_as_slice(
            .exact,
            tb.Transfer,
            &b.client_requests[client_index],
        )[0..transfer_count];
        b.build_transfers(transfers);
        b.request(client_index, .create_transfers, .{
            .batch_count = transfer_count,
            .event_size = @sizeOf(tb.Transfer),
        });
    }

    fn create_transfers_callback(b: *Benchmark, client_index: u32, result: []const u8) void {
        assert(!b.clients_busy.is_set(client_index));
        const create_transfers_results = stdx.bytes_as_slice(
            .exact,
            tb.CreateTransfersResult,
            result,
        );
        if (create_transfers_results.len > 0) {
            panic("CreateTransfersResults: {any}", .{create_transfers_results});
        }

        const requests_complete = b.request_index - b.clients_busy.count();
        const request_duration_ns = b.timer.read() - b.clients_request_ns[client_index];
        const request_duration_ms = @divTrunc(request_duration_ns, std.time.ns_per_ms);
        const transfers_created = @min(b.transfer_count, b.transfer_batch_size);
        b.transfers_created += transfers_created;

        if (b.print_batch_timings) {
            log.info("batch {}: {} tx in {} ms", .{
                requests_complete,
                b.transfer_batch_size,
                request_duration_ms,
            });
        }

        b.client_timeouts[client_index] = .{ .benchmark = b, .client_index = client_index };
        b.clients_busy.set(client_index);
        b.io.timeout(
            *Timeout,
            &b.client_timeouts[client_index],
            create_transfers_next,
            &b.client_timeouts[client_index].completion,
            @intCast(b.transfer_batch_delay.ns),
        );
    }

    fn create_transfers_next(
        timeout: *Timeout,
        completion: *IO.Completion,
        result: IO.TimeoutError!void,
    ) void {
        assert(completion == &timeout.completion);
        _ = result catch |e| switch (e) {
            error.Canceled => unreachable,
            error.Unexpected => unreachable,
        };

        const b = timeout.benchmark;
        assert(b.clients_busy.is_set(timeout.client_index));

        b.clients_busy.unset(timeout.client_index);
        b.create_transfers(timeout.client_index);
    }

    fn create_transfers_finish(b: *Benchmark) void {
        assert(b.stage == .create_transfers);

        b.output.print(
            \\{[batch_count]} batches in {[batch_duration_s]d:.2} s
            \\transfer batch size = {[batch_size]} txs
            \\transfer batch delay = {[batch_delay]}
            \\load accepted = {[transfer_rate]} tx/s
            \\
        , .{
            .batch_count = b.request_index,
            .batch_duration_s = @as(f64, @floatFromInt(b.timer.read())) / std.time.ns_per_s,
            .batch_size = b.transfer_batch_size,
            .batch_delay = b.transfer_batch_delay,
            .transfer_rate = @divTrunc(
                @as(u64, b.transfer_count) * std.time.ns_per_s,
                b.timer.read(),
            ),
        }) catch unreachable;
        print_percentiles_histogram(b.output, "batch", b.request_latency_histogram);

        b.run_finish();
    }

    fn get_account_transfers(b: *Benchmark, client_index: u32) void {
        assert(b.stage == .get_account_transfers);
        assert(!b.clients_busy.is_set(client_index));

        if (b.query_index >= b.query_count) {
            if (b.clients_busy.empty()) b.get_account_transfers_finish();
            return;
        }
        b.query_index += 1;

        const request_body = b.client_requests[client_index][0..@sizeOf(tb.AccountFilter)];
        // Use hot accounts for queries to equalize the number of results
        // returned on each execution.
        const account_index = b.choose_account_index(.hot);
        const filter: *tb.AccountFilter = @alignCast(std.mem.bytesAsValue(
            tb.AccountFilter,
            request_body,
        ));
        filter.* = .{
            .account_id = b.account_id_permutation.encode(account_index + 1),
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
        b.request(client_index, .get_account_transfers, .{
            .batch_count = 1,
            .event_size = @sizeOf(tb.AccountFilter),
        });
    }

    fn get_account_transfers_callback(b: *Benchmark, client_index: u32, result: []const u8) void {
        assert(b.stage == .get_account_transfers);

        const filter: tb.AccountFilter = std.mem.bytesToValue(
            tb.AccountFilter,
            b.client_requests[client_index][0..@sizeOf(tb.AccountFilter)],
        );
        const results = stdx.bytes_as_slice(.exact, tb.Transfer, result);
        for (results) |*transfer| {
            assert((transfer.debit_account_id == filter.account_id) !=
                (transfer.credit_account_id == filter.account_id));
        }
        b.get_account_transfers(client_index);
    }

    fn get_account_transfers_finish(b: *Benchmark) void {
        assert(b.stage == .get_account_transfers);

        b.output.print("\n{[query_count]} queries in {[query_duration_s]d:.1} s\n", .{
            .query_count = b.request_index,
            .query_duration_s = @as(f64, @floatFromInt(b.timer.read())) / std.time.ns_per_s,
        }) catch unreachable;
        print_percentiles_histogram(b.output, "query", b.request_latency_histogram);

        b.run_finish();
    }

    fn validate_accounts(b: *Benchmark, client_index: u32) void {
        assert(b.stage == .validate_accounts);
        assert(!b.clients_busy.is_set(client_index));
        assert(b.account_index <= b.account_count);
        if (b.account_index == b.account_count) {
            if (b.clients_busy.empty()) b.validate_accounts_finish();
            return;
        }

        const account_count: u32 = @intCast(@min(
            b.account_count - b.account_index,
            b.account_batch_size,
        ));
        const account_ids = stdx.bytes_as_slice(
            .exact,
            u128,
            &b.client_requests[client_index],
        )[0..account_count];
        const accounts = stdx.bytes_as_slice(
            .exact,
            tb.Account,
            &b.client_replies[client_index],
        )[0..account_count];
        b.build_accounts(accounts);
        for (account_ids, accounts) |*account_id, account| account_id.* = account.id;
        b.request(client_index, .lookup_accounts, .{
            .batch_count = account_count,
            .event_size = @sizeOf(u128),
        });
    }

    fn validate_accounts_callback(
        b: *Benchmark,
        client_index: u32,
        result: []const u8,
    ) void {
        assert(b.stage == .validate_accounts);

        const accounts_count = accounts_count: {
            if (b.account_index == b.account_count) {
                // The last batch might not be full.
                const remaining = @rem(b.account_count, b.account_batch_size);
                if (remaining > 0) break :accounts_count remaining;
            }

            break :accounts_count b.account_batch_size;
        };
        const accounts_expected_body = &b.client_replies[client_index];
        const accounts_expected = stdx.bytes_as_slice(
            .exact,
            tb.Account,
            accounts_expected_body,
        )[0..accounts_count];
        const accounts_actual = stdx.bytes_as_slice(
            .exact,
            tb.Account,
            result,
        );
        assert(accounts_actual.len == accounts_count);
        for (accounts_expected, accounts_actual) |expected, actual| {
            assert(expected.id == actual.id);
            assert(expected.user_data_128 == actual.user_data_128);
            assert(expected.user_data_64 == actual.user_data_64);
            assert(expected.user_data_32 == actual.user_data_32);
            assert(expected.code == actual.code);
            assert(@as(u16, @bitCast(expected.flags)) == @as(u16, @bitCast(actual.flags)));
        }
        b.validate_accounts(client_index);
    }

    fn validate_accounts_finish(b: *Benchmark) void {
        assert(b.stage == .validate_accounts);

        b.output.print(
            "validated {d} accounts\n",
            .{b.account_count},
        ) catch unreachable;
        b.run_finish();
    }

    fn validate_transfers(b: *Benchmark, client_index: u32) void {
        assert(b.stage == .validate_transfers);
        assert(!b.clients_busy.is_set(client_index));
        assert(b.transfer_index <= b.transfer_count);
        if (b.transfer_index == b.transfer_count) {
            if (b.clients_busy.empty()) b.validate_transfers_finish();
            return;
        }

        const transfer_count: u32 = @intCast(@min(
            b.transfer_count - b.transfer_index,
            b.transfer_batch_size,
        ));
        const transfer_ids = stdx.bytes_as_slice(
            .exact,
            u128,
            &b.client_requests[client_index],
        )[0..transfer_count];
        const transfers = stdx.bytes_as_slice(
            .exact,
            tb.Transfer,
            &b.client_replies[client_index],
        )[0..transfer_count];
        b.build_transfers(transfers);
        for (transfer_ids, transfers) |*transfer_id, transfer| transfer_id.* = transfer.id;
        b.request(client_index, .lookup_transfers, .{
            .batch_count = transfer_count,
            .event_size = @sizeOf(u128),
        });
    }

    fn validate_transfers_callback(
        b: *Benchmark,
        client_index: u32,
        result: []const u8,
    ) void {
        assert(b.stage == .validate_transfers);

        const transfers_count = transfers_count: {
            if (b.transfer_index == b.transfer_count) {
                // The last batch might not be full.
                const remaining = @rem(b.transfer_count, b.transfer_batch_size);
                if (remaining > 0) break :transfers_count remaining;
            }

            break :transfers_count b.transfer_batch_size;
        };
        const transfers_expected = stdx.bytes_as_slice(
            .exact,
            tb.Transfer,
            &b.client_replies[client_index],
        )[0..transfers_count];
        const transfers_actual = stdx.bytes_as_slice(
            .exact,
            tb.Transfer,
            result,
        );
        assert(transfers_actual.len == transfers_count);
        for (transfers_expected, transfers_actual) |expected, actual| {
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
        b.validate_transfers(client_index);
    }

    fn validate_transfers_finish(b: *Benchmark) void {
        assert(b.stage == .validate_transfers);

        b.output.print(
            "validated {d} transfers\n",
            .{b.transfer_count},
        ) catch unreachable;

        b.run_finish();
    }

    const RequestContext = extern struct {
        benchmark: *Benchmark,
        client_index: u32,
        request_index: u32,

        comptime {
            assert(@sizeOf(RequestContext) == @sizeOf(u128));
        }
    };

    fn request(
        b: *Benchmark,
        client_index: usize,
        operation: tb.Operation,
        options: struct {
            batch_count: u32,
            event_size: u32,
        },
    ) void {
        assert(b.stage != .idle);
        assert(b.clients_busy.count() < b.clients.len);
        assert(!b.clients_busy.is_set(client_index));

        b.clients_busy.set(client_index);
        b.clients_request_ns[client_index] = b.timer.read();
        b.request_index += 1;

        var encoder = vsr.multi_batch.MultiBatchEncoder.init(
            &b.client_requests[client_index],
            .{ .element_size = options.event_size },
        );
        encoder.add(options.batch_count * options.event_size);
        const bytes_written = encoder.finish();

        b.clients[client_index].request(
            request_complete,
            @bitCast(RequestContext{
                .benchmark = b,
                .client_index = @intCast(client_index),
                .request_index = @intCast(b.request_index - 1),
            }),
            operation,
            b.client_requests[client_index][0..bytes_written],
        );
    }

    fn request_complete(
        user_data: u128,
        operation_vsr: vsr.Operation,
        timestamp: u64,
        result: []u8,
    ) void {
        const operation = operation_vsr.cast(tb.Operation);
        const context: RequestContext = @bitCast(user_data);
        const client = context.client_index;
        const b: *Benchmark = context.benchmark;
        assert(b.clients_busy.is_set(client));
        assert(b.stage != .idle);
        assert(timestamp > 0);

        b.clients_busy.unset(client);

        const duration_ns = b.timer.read() - b.clients_request_ns[client];
        const duration_ms = @divTrunc(duration_ns, std.time.ns_per_ms);
        b.request_latency_histogram[@min(duration_ms, b.request_latency_histogram.len - 1)] += 1;

        const input: []const u8 = input: {
            assert(operation.is_multi_batch());
            var reply_decoder = vsr.multi_batch.MultiBatchDecoder.init(
                result,
                .{ .element_size = operation.result_size() },
            ) catch unreachable;
            assert(reply_decoder.batch_count() == 1);
            break :input reply_decoder.peek();
        };

        switch (operation) {
            .create_accounts => b.create_accounts_callback(client, input),
            .create_transfers => b.create_transfers_callback(client, input),
            .lookup_accounts => b.validate_accounts_callback(client, input),
            .lookup_transfers => b.validate_transfers_callback(client, input),
            .get_account_transfers => b.get_account_transfers_callback(client, input),
            else => unreachable,
        }
    }

    fn build_accounts(b: *Benchmark, accounts: []tb.Account) void {
        for (accounts) |*account| {
            account.* = .{
                .id = b.account_id_permutation.encode(b.account_index + 1),
                .user_data_128 = b.prng.int(u128),
                .user_data_64 = b.prng.int(u64),
                .user_data_32 = b.prng.int(u32),
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
            b.account_index += 1;
        }
    }

    fn build_transfers(b: *Benchmark, transfers: []tb.Transfer) void {
        for (transfers) |*transfer| {
            // The set of accounts is divided into two different "worlds" by
            // `account_count_hot`. Sometimes the debit account will be selected
            // from the first `account_count_hot` accounts; otherwise both
            // debit and credit will be selected from an account >= `account_count_hot`.

            const debit_account_index = b.choose_account_index(
                if (b.prng.range_inclusive(usize, 1, 100) <= b.transfer_hot_percent)
                    .hot
                else
                    .cold,
            );

            const credit_account_index = index: {
                var index = b.choose_account_index(.cold);
                if (index == debit_account_index) {
                    index = (index + 1) % b.account_count;
                }
                break :index index;
            };
            assert(debit_account_index < b.account_count);
            assert(credit_account_index < b.account_count);
            assert(debit_account_index != credit_account_index);

            const debit_account_id = b.transfer_id_permutation.encode(debit_account_index + 1);
            const credit_account_id = b.transfer_id_permutation.encode(credit_account_index + 1);
            assert(debit_account_id != credit_account_id);

            // 30% of pending transfers.
            const pending = b.transfer_pending and b.prng.chance(ratio(3, 10));

            transfer.* = .{
                .id = b.transfer_id_permutation.encode(b.transfer_index + 1),
                .debit_account_id = debit_account_id,
                .credit_account_id = credit_account_id,
                .user_data_128 = b.prng.int(u128),
                .user_data_64 = b.prng.int(u64),
                .user_data_32 = b.prng.int(u32),
                // TODO Benchmark posting/voiding pending transfers.
                .pending_id = 0,
                .ledger = 2,
                .code = b.prng.int(u16) +| 1,
                .flags = .{
                    .pending = pending,
                    .imported = b.flag_imported,
                },
                .timeout = if (pending)
                    // Timeouts must be short enough to ensure they are likely to expire
                    // during the benchmark, allowing the performance impact to be measured.
                    b.prng.range_inclusive(u32, 1, 5)
                else
                    0,
                .amount = random_int_exponential(b.prng, u64, 10_000) +| 1,
                .timestamp = if (b.flag_imported) b.account_index + b.transfer_index + 1 else 0,
            };
            b.transfer_index += 1;
        }
    }

    fn choose_account_index(b: *Benchmark, hint: enum { hot, cold }) u64 {
        assert(b.account_count > 0);
        stdx.maybe(b.account_count_hot == 0);
        assert(b.account_count >= b.account_count_hot);

        // The hint may be ignored if:
        // Always use hot accounts if `account_count == account_count_hot`.
        // Always use cold accounts if `account_count_hot == 0`.
        const source: @TypeOf(hint) = switch (hint) {
            .hot => if (b.account_count_hot > 0) .hot else .cold,
            .cold => if (b.account_count > b.account_count_hot) .cold else .hot,
        };

        // Select the generator and the count from each source.
        const generator: *Generator, const account_count: u64 = switch (source) {
            .hot => .{ &b.account_generator_hot, b.account_count_hot },
            .cold => .{ &b.account_generator, b.account_count - b.account_count_hot },
        };
        assert(account_count > 0);

        const index = switch (generator.*) {
            .zipfian => |gen| index: {
                // zipfian set size must be same as account set size
                assert(account_count == gen.gen.n);
                const index = gen.next(b.prng);
                assert(index < account_count);
                break :index index;
            },
            .latest => |gen| index: {
                assert(account_count == gen.n);
                const index_rev = gen.next(b.prng);
                assert(index_rev < account_count);
                break :index account_count - index_rev - 1;
            },
            .uniform => |count| index: {
                const index = b.prng.int_inclusive(u64, count - 1);
                assert(index < account_count);
                break :index index;
            },
        };

        return switch (source) {
            .hot => index,
            .cold => index + b.account_count_hot,
        };
    }
};

fn print_percentiles_histogram(
    stdout: std.io.AnyWriter,
    label: []const u8,
    histogram_buckets: []const u64,
) void {
    var histogram_total: u64 = 0;
    for (histogram_buckets) |bucket| histogram_total += bucket;

    const percentiles = [_]u64{ 1, 50, 99, 100 };
    for (percentiles) |percentile| {
        const histogram_percentile: usize = @divTrunc(histogram_total * percentile, 100);

        // Since each bucket in our histogram represents 1ms, the bucket we're in is the ms value.
        var sum: usize = 0;
        const latency = for (histogram_buckets, 0..) |bucket, bucket_index| {
            sum += bucket;
            if (sum >= histogram_percentile) break bucket_index;
        } else histogram_buckets.len;

        stdout.print("{s} latency p{: <3} = {} ms{s}\n", .{
            label,
            percentile,
            latency,
            if (latency == histogram_buckets.len) "+ (exceeds histogram resolution)" else "",
        }) catch unreachable;
    }
}
