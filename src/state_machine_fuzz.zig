//! Very simple state machine fuzzer. It looks for poison pill style ops that are otherwise valid
//! which cause a crash, then be replayed after said crash, resulting in a crash loop.
const std = @import("std");
const assert = std.debug.assert;

const vsr = @import("vsr.zig");
const constants = vsr.constants;
const stdx = @import("stdx");

const tb = @import("tigerbeetle.zig");
const fuzz = @import("./testing/fuzz.zig");

const StateMachineType = @import("./state_machine.zig").StateMachineType;
const MultiBatchDecoder = @import("./vsr/multi_batch.zig").MultiBatchDecoder;
const MultiBatchEncoder = @import("./vsr/multi_batch.zig").MultiBatchEncoder;

const TimeSim = stdx.TimeSim;
const Storage = @import("testing/storage.zig").Storage;
const Tracer = Storage.Tracer;
const data_file_size_min = @import("vsr/superblock.zig").data_file_size_min;
const SuperBlock = @import("vsr/superblock.zig").SuperBlockType(Storage);
const Grid = @import("vsr/grid.zig").GridType(Storage);
const fixtures = @import("testing/fixtures.zig");
const StateMachineReferenceType = @import("./state_machine/reference_model.zig").StateMachineReferenceType;
const MiB = stdx.MiB;

const Ratio = stdx.PRNG.Ratio;
const ratio = stdx.PRNG.ratio;

const log = std.log.scoped(.state_machine_fuzz);

pub fn main(gpa: std.mem.Allocator, args: fuzz.FuzzArgs) !void {
    var world: World = undefined;
    try world.init(gpa);
    defer world.deinit(gpa);

    const request_buffer = try gpa.alignedAlloc(
        u8,
        .fromByteUnits(constants.cache_line_size),
        vsr.constants.message_body_size_max,
    );
    defer gpa.free(request_buffer);
    const request_body = request_buffer[0..world.state_machine.batch_size_limit];

    const reply_buffer = try gpa.alignedAlloc(
        u8,
        .fromByteUnits(constants.cache_line_size),
        vsr.constants.message_body_size_max,
    );
    defer gpa.free(reply_buffer);

    var prng = stdx.PRNG.from_seed(args.seed);
    const options = Options.swarm(&prng);

    // TODO: extend the options swarm to time and queries.
    // - think about which constallations would provoke some of the findings
    // - max u64 as filter.
    // - too strict assertion: failed transfer (orphaned id) + posting/voiding the failed one.
    // - state machine missing CDC, how can we detect this ... only with minimal state?
    //   - but we should make it so that the case can happen:
    //    \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ _   _ _  0 created
    //    \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ _   _ _  0 created
    //    \\ commit create_accounts
    //    \\
    //    // T1 will expire in 1 second.
    //    \\ transfer T1 A1 A2 10  _ _ _ _ 1 L1 C1 _ PEN _   _   _ _ _ _ _ _ _ created
    //    \\ commit create_transfers
    //    \\
    //    \\ tick 900 milliseconds
    //    \\
    //    // T1 hasn't expired yet.
    //    \\ transfer T2 A1 A2  20  _ _ _ _ 0 L1 C1 _   _ _   _   _ _ IMP _ _ _ 10 created
    //    \\ commit create_transfers
    //    \\
    //    \\ tick 100 milliseconds
    //    \\
    //    // T1's expiry timestamp is later than the imported timestamp.
    //    \\ transfer T3 A1 A2  30  _ _ _ _ 0 L1 C1 _   _ _   _   _ _ IMP _ _ _ 20 imported_event_timestamp_must_not_regress
    //    \\ commit create_transfers
    //    \\
    //    // T1's expiry timestamp is earlier than the imported timestamp.
    //    \\ transfer T4 A1 A2  40  _ _ _ _ 0 L1 C1 _   _ _   _   _ _ IMP _ _ _ 1000000035 created
    //    \\ commit create_transfers
    //    - how to advance time?
    //    - tick is just:
    //     context.state_machine.prepare_timestamp += if (ticks.value > 0)
    //         interval_ns
    //     else
    //         TimestampRange.timestamp_max - interval_ns;
    //     context.commit_timestamp_expected = context.state_machine.prepare_timestamp;
    //     // Pulse is executed when the cluster is idle.
    //     context.pulse();

    for (0..args.events_max orelse 100) |_| {
        const operation = prng.enum_weighted(World.StateMachine.Operation, options.operation_weights);
        const size: usize = size: {
            if (!operation.is_multi_batch()) {
                break :size build_batch(&prng, &options, operation, request_body);
            }
            assert(operation.is_multi_batch());

            var body_encoder: MultiBatchEncoder = .init(request_body, .{
                .element_size = operation.event_size(),
            });

            const batch_count = prng.enum_uniform(enum { one, random, max });
            while (body_encoder.writable()) |writable| {
                if (writable.len == 0) break;
                const bytes_written: u32 = build_batch(&prng, &options, operation, writable);
                body_encoder.add(bytes_written);
                switch (batch_count) {
                    .one => {
                        if (body_encoder.batch_count == 1) break;
                    },
                    .random => if (prng.chance(.{ .numerator = 30, .denominator = 100 })) {
                        break;
                    },
                    .max => {},
                }
            }

            break :size body_encoder.finish();
        };

        if (world.state_machine.input_valid(operation, request_buffer[0..size])) {
            world.prepare(operation, request_buffer[0..size]);
            const reply_size = world.execute(
                world.op,
                operation,
                request_buffer[0..size],
                @ptrCast(reply_buffer),
            );
            stdx.maybe(reply_size == 0);
            if (operation.is_multi_batch()) {
                assert(reply_size > 0);
                _ = MultiBatchDecoder.init(reply_buffer[0..reply_size], .{
                    .element_size = operation.result_size(),
                }) catch |err| switch (err) {
                    error.MultiBatchInvalid => unreachable,
                };
            }
            // TODO: add tracking how many return codes we got back.
        }
        // Match Replica's commit order, including the delay before released blocks can be reused.
        world.checkpoint_durable();
        world.state_machine_compact();
        const checkpoint_next = vsr.Checkpoint.checkpoint_after(world.checkpoint_op);
        if (world.op == vsr.Checkpoint.trigger_for_checkpoint(checkpoint_next).?) {
            world.state_machine_checkpoint();
            world.grid_checkpoint();
            world.checkpoint_op = checkpoint_next;
            world.grid.mark_checkpoint_not_durable();
        }
        world.op += 1;
    }
}

const World = struct {
    storage: Storage,
    time_sim: TimeSim,
    trace: Tracer,
    superblock: SuperBlock,
    grid: Grid,
    state_machine: StateMachine,
    reference: StateMachineReference = .{},
    // This fuzzer never reopens storage, so checkpoint progress is only tracked in memory.
    checkpoint_op: u64,
    op: u64,
    busy: bool,

    const StateMachineReference = StateMachineReferenceType(1000, 1000);
    const StateMachine = vsr.state_machine.StateMachineType(Storage);

    fn init(world: *World, gpa: std.mem.Allocator) !void {
        world.storage = try fixtures.init_storage(gpa, .{ .size = 512 * MiB });
        errdefer world.storage.deinit(gpa);

        try fixtures.storage_format(gpa, &world.storage, .{
            .replica_count = 1,
        });

        world.time_sim = fixtures.init_time(.{});

        world.trace = try fixtures.init_tracer(gpa, world.time_sim.interface(), .{});
        errdefer world.trace.deinit(gpa);

        world.superblock = try fixtures.init_superblock(gpa, &world.storage, .{
            .storage_size_limit = 256 * MiB,
        });
        errdefer world.superblock.deinit(gpa);

        fixtures.open_superblock(&world.superblock);

        world.grid = try fixtures.init_grid(gpa, &world.trace, &world.superblock, .{
            .blocks_released_prior_checkpoint_durability_max = StateMachine.Forest
                .compaction_blocks_released_per_pipeline_max(),
        });
        errdefer world.grid.deinit(gpa);

        fixtures.open_grid(&world.grid);

        const batch_size_limit = 30 * @max(@sizeOf(tb.Account), @sizeOf(tb.Transfer));
        assert(batch_size_limit <= constants.message_body_size_max);
        try world.state_machine.init(
            gpa,
            world.time_sim.interface(),
            &world.grid,
            .{
                .batch_size_limit = batch_size_limit,
                .lsm_forest_compaction_block_count = StateMachine.Forest.Options
                    .compaction_block_count_min,
                .lsm_forest_node_count = 128,
                .cache_entries_accounts = 0,
                .cache_entries_transfers = 0,
                .cache_entries_transfers_pending = 0,
                .log_trace = true,
                .aof_recovery = false,
            },
        );
        errdefer world.state_machine.deinit(gpa);

        world.state_machine_open();
    }

    pub fn deinit(ctx: *World, allocator: std.mem.Allocator) void {
        ctx.state_machine.deinit(allocator);
        ctx.grid.deinit(allocator);
        ctx.superblock.deinit(allocator);
        ctx.trace.deinit(allocator);
        ctx.storage.deinit(allocator);
        ctx.* = undefined;
    }

    fn state_machine_open(world: *World) void {
        world.busy = true;
        world.op = 1;
        world.checkpoint_op = 0;
        world.state_machine.open(state_machine_open_callback);

        while (world.busy) world.storage.run();
    }

    fn state_machine_open_callback(state_machine: *StateMachine) void {
        const world: *World = @fieldParentPtr("state_machine", state_machine);
        assert(world.busy);
        world.busy = false;
    }

    fn state_machine_compact(world: *World) void {
        world.busy = true;
        world.state_machine.compact(state_machine_compact_callback, world.op);
        while (world.busy) world.storage.run();
    }

    fn state_machine_compact_callback(state_machine: *StateMachine) void {
        const world: *World = @fieldParentPtr("state_machine", state_machine);
        assert(world.busy);
        world.busy = false;
    }

    fn state_machine_checkpoint(world: *World) void {
        world.busy = true;
        world.state_machine.checkpoint(state_machine_checkpoint_callback);
        while (world.busy) world.storage.run();
    }

    fn checkpoint_durable(world: *World) void {
        if (world.grid.free_set.checkpoint_durable) return;
        const checkpoint_op = world.checkpoint_op;
        if (!vsr.Checkpoint.durable(checkpoint_op, world.op)) return;

        if (vsr.Checkpoint.trigger_for_checkpoint(checkpoint_op)) |trigger| {
            assert(world.op == trigger + constants.pipeline_prepare_queue_max + 1);
        }

        // No repairs run in this fuzzer, so there are no repair writes to await before freeing blocks.
        world.grid.free_set.mark_checkpoint_durable();
    }

    fn state_machine_checkpoint_callback(state_machine: *StateMachine) void {
        const world: *World = @fieldParentPtr("state_machine", state_machine);
        assert(world.busy);
        world.busy = false;
    }

    fn grid_checkpoint(world: *World) void {
        world.busy = true;
        world.grid.checkpoint(grid_checkpoint_callback);
        while (world.busy) world.storage.run();
    }

    fn grid_checkpoint_callback(grid: *Grid) void {
        const world: *World = @alignCast(@fieldParentPtr("grid", grid));
        assert(world.busy);
        world.busy = false;
    }

    fn prepare(
        world: *World,
        operation: StateMachine.Operation,
        message_body_used: []align(constants.cache_line_size) const u8,
    ) void {
        world.state_machine.commit_timestamp = world.state_machine.prepare_timestamp;
        world.state_machine.prepare_timestamp += 1;
        world.state_machine.prepare(
            operation,
            message_body_used,
        );
    }

    fn execute(
        world: *World,
        op: u64,
        operation: StateMachine.Operation,
        message_body_used: []align(constants.cache_line_size) const u8,
        output_buffer: *align(constants.cache_line_size) [constants.message_body_size_max]u8,
    ) usize {
        const timestamp = world.state_machine.prepare_timestamp;
        world.busy = true;
        world.state_machine.prefetch_timestamp = timestamp;
        world.state_machine.prefetch(
            struct {
                fn callback(state_machine: *StateMachine) void {
                    const ctx: *World = @fieldParentPtr("state_machine", state_machine);
                    assert(ctx.busy);
                    ctx.busy = false;
                }
            }.callback,
            op,
            op,
            operation,
            message_body_used,
        );
        while (world.busy) world.storage.run();

        return world.state_machine.commit(
            1,
            op,
            timestamp,
            operation,
            message_body_used,
            output_buffer,
        );
    }
};

/// Generate a random number, biased towards all bit 'edges' of T. That is, given a u64, it's very
/// likely to not only get 0 or maxInt(u64), but also values around maxInt(u63), maxInt(u62), ...,
/// maxInt(u1).
pub fn int_edge_biased(prng: *stdx.PRNG, T: anytype) T {
    const bits = @typeInfo(T).int.bits;
    comptime assert(@typeInfo(T).int.signedness == .unsigned);

    // With bits * 2, there's a ~50% chance of generating a uniform integer within the full range,
    // and a ~50% chance of generating an integer biased towards an edge.
    const bias_to = prng.range_inclusive(T, 0, bits * 2);

    if (bias_to > bits) {
        return prng.int(T);
    } else {
        const bias_center: T = if (bias_to == bits)
            std.math.maxInt(T)
        else
            std.math.pow(T, 2, bias_to);
        const bias_min = if (bias_to == 0) 0 else bias_center - @min(bias_center, 8);
        const bias_max = if (bias_to == bits) bias_center else bias_center + 8;

        return prng.range_inclusive(T, bias_min, bias_max);
    }
}

const Options = struct {
    operation_weights: stdx.PRNG.EnumWeightsType(World.StateMachine.Operation),
    account_id_max: u128,
    transfer_id_max: u128,
    account_mutation_probability: MutationProbability(tb.Account),
    transfer_mutation_probability: MutationProbability(tb.Transfer),
    repeat_probability: Ratio,
    repeats_seed: u64,

    fn swarm(prng: *stdx.PRNG) Options {
        var operation_weights = fuzz.random_enum_weights(prng, World.StateMachine.Operation);
        // Keep both creates enabled while swarming the surrounding operations.
        operation_weights.create_accounts = prng.range_inclusive(u64, 100, 1000);
        operation_weights.create_transfers = prng.range_inclusive(u64, 100, 1000);
        return .{
            .operation_weights = operation_weights,
            .account_id_max = prng.range_inclusive(u128, 1, 256),
            .transfer_id_max = prng.range_inclusive(u128, 1, 256),
            .account_mutation_probability = mutation_probability_swarm(prng, tb.Account),
            .transfer_mutation_probability = mutation_probability_swarm(prng, tb.Transfer),
            .repeats_seed = prng.int(u64),
            .repeat_probability = ratio(
                prng.int_inclusive(u8, 80),
                100,
            ),
        };
    }
};

fn MutationProbability(comptime Event: type) type {
    return std.enums.EnumFieldStruct(std.meta.FieldEnum(Event), stdx.PRNG.Ratio, null);
}

fn mutation_probability_swarm(prng: *stdx.PRNG, comptime Event: type) MutationProbability(Event) {
    var probability: MutationProbability(Event) = undefined;
    inline for (comptime std.meta.fieldNames(Event)) |field| {
        @field(probability, field) = ratio(
            prng.int_inclusive(u8, 30),
            100,
        );
    }
    return probability;
}

fn mutate_event(
    prng: *stdx.PRNG,
    comptime Event: type,
    probability: MutationProbability(Event),
    event: *Event,
) void {
    inline for (std.meta.fields(Event)) |field| {
        if (comptime std.mem.eql(u8, field.name, "flags")) {
            inline for (std.meta.fields(field.type)) |flag| {
                // if (comptime std.mem.eql(u8, flag.name, "imported")) continue;
                if (prng.chance(probability.flags)) {
                    @field(event.flags, flag.name) = if (flag.type == bool)
                        prng.boolean()
                    else
                        int_edge_biased(prng, flag.type);
                }
            }
        } else if (prng.chance(@field(probability, field.name))) {
            @field(event, field.name) = if (prng.boolean())
                (if (prng.boolean()) 0 else std.math.maxInt(field.type))
            else
                int_edge_biased(prng, field.type);
        }
    }
}

fn build_create_accounts(prng: *stdx.PRNG, options: *const Options, buffer: []u8) u32 {
    const accounts = stdx.bytes_as_slice(.inexact, tb.Account, buffer);
    const count = prng.int_inclusive(u32, @intCast(accounts.len));
    for (accounts[0..count]) |*account| {
        // Common defaults and a small ID pool allow independent events to collide and retry.
        // Field mutations also introduce IDs outside the pool. No account is guaranteed valid.
        account.* = std.mem.zeroes(tb.Account);
        account.id = prng.range_inclusive(u128, 1, options.account_id_max);
        account.ledger = 1;
        account.code = 1;
        mutate_event(prng, tb.Account, options.account_mutation_probability, account);
    }
    return count * @sizeOf(tb.Account);
}

fn build_create_transfers(prng: *stdx.PRNG, options: *const Options, buffer: []u8) u32 {
    const transfers = stdx.bytes_as_slice(.inexact, tb.Transfer, buffer);
    const count = prng.int_inclusive(u32, @intCast(transfers.len));
    for (transfers[0..count]) |*transfer| {
        if (prng.chance(options.repeat_probability)) {
            var prng_low_cardinality = stdx.PRNG.from_seed(
                prng.int_inclusive(u64, 10) ^ options.repeats_seed,
            );
            build_transfer(&prng_low_cardinality, options, transfer);
            if (prng.chance(ratio(1, 10))) {
                const transfer_bytes = std.mem.asBytes(transfer);
                transfer_bytes[prng.index(transfer_bytes)] ^= prng.bit(u8);
            }
        } else {
            build_transfer(prng, options, transfer);
        }
    }
    return count * @sizeOf(tb.Transfer);
}

fn build_transfer(prng: *stdx.PRNG, options: *const Options, transfer: *tb.Transfer) void {
    transfer.* = std.mem.zeroes(tb.Transfer);
    transfer.id = prng.range_inclusive(u128, 1, options.transfer_id_max);
    transfer.debit_account_id = prng.range_inclusive(u128, 1, options.account_id_max);
    transfer.credit_account_id = prng.range_inclusive(u128, 1, options.account_id_max);
    // References are independent of flags and of whether the referenced event exists.
    transfer.pending_id = if (prng.boolean())
        0
    else
        prng.range_inclusive(u128, 1, options.transfer_id_max);
    transfer.amount = 1;
    transfer.ledger = 1;
    transfer.code = 1;
    mutate_event(prng, tb.Transfer, options.transfer_mutation_probability, transfer);
}

fn build_batch(
    prng: *stdx.PRNG,
    options: *const Options,
    operation: World.StateMachine.Operation,
    buffer: []u8,
) u32 {
    return switch (operation) {
        // No payload, so not very interesting yet.
        .pulse => 0,

        .create_transfers => build_create_transfers(prng, options, buffer),
        .create_accounts => build_create_accounts(prng, options, buffer),

        .deprecated_create_accounts_sparse,
        .deprecated_create_transfers_sparse,
        => 0,
        .deprecated_create_accounts_unbatched,
        .deprecated_create_transfers_unbatched,
        => 0,

        .lookup_accounts, .lookup_transfers => build_lookup(prng, buffer),
        .get_account_transfers, .get_account_balances => build_account_filter(prng, buffer),
        .query_accounts, .query_transfers => build_query_filter(prng, buffer),
        .get_change_events => build_get_change_events_filter(prng, buffer),

        .deprecated_lookup_accounts_unbatched,
        .deprecated_lookup_transfers_unbatched,
        => build_lookup(prng, buffer),
        .deprecated_get_account_transfers_unbatched,
        .deprecated_get_account_balances_unbatched,
        => build_account_filter(prng, buffer),
        .deprecated_query_accounts_unbatched,
        .deprecated_query_transfers_unbatched,
        => build_query_filter(prng, buffer),
    };
}

fn build_lookup(prng: *stdx.PRNG, buffer: []u8) u32 {
    const ids: []u128 = stdx.bytes_as_slice(.inexact, u128, buffer);
    const size: u32 = prng.int_inclusive(u32, @intCast(ids.len));
    for (ids[0..size]) |*id| {
        id.* = int_edge_biased(prng, u128);
    }
    return size * @sizeOf(u128);
}

fn build_account_filter(prng: *stdx.PRNG, buffer: []u8) u32 {
    const filter: *tb.AccountFilter = filter: {
        const slice = stdx.bytes_as_slice(
            .inexact,
            tb.AccountFilter,
            buffer,
        );
        if (slice.len == 0) return 0;
        break :filter &slice[0];
    };
    var reserved: [58]u8 = @splat(0);
    if (prng.chance(.{ .numerator = 1, .denominator = 1000 })) {
        prng.fill(&reserved);
    }

    filter.* = .{
        .account_id = int_edge_biased(prng, u128),
        .user_data_128 = int_edge_biased(prng, u128),
        .user_data_64 = int_edge_biased(prng, u64),
        .user_data_32 = int_edge_biased(prng, u32),
        .code = int_edge_biased(prng, u16),
        .timestamp_min = int_edge_biased(prng, u64),
        .timestamp_max = int_edge_biased(prng, u64),
        .limit = int_edge_biased(prng, u32),
        .reserved = reserved,
        .flags = .{
            .reversed = prng.boolean(),
            .debits = prng.boolean(),
            .credits = prng.boolean(),
            .padding = if (prng.chance(.{ .numerator = 1, .denominator = 1000 }))
                int_edge_biased(prng, u29)
            else
                0,
        },
    };

    return @sizeOf(tb.AccountFilter);
}

fn build_query_filter(prng: *stdx.PRNG, buffer: []u8) u32 {
    const filter: *tb.QueryFilter = filter: {
        const slice = stdx.bytes_as_slice(
            .inexact,
            tb.QueryFilter,
            buffer,
        );
        if (slice.len == 0) return 0;
        break :filter &slice[0];
    };
    var reserved: [6]u8 = @splat(0);
    if (prng.chance(.{ .numerator = 1, .denominator = 1000 })) {
        prng.fill(&reserved);
    }

    filter.* = .{
        .user_data_128 = int_edge_biased(prng, u128),
        .user_data_64 = int_edge_biased(prng, u64),
        .user_data_32 = int_edge_biased(prng, u32),
        .ledger = int_edge_biased(prng, u32),
        .code = int_edge_biased(prng, u16),
        .timestamp_min = int_edge_biased(prng, u64),
        .timestamp_max = int_edge_biased(prng, u64),
        .limit = int_edge_biased(prng, u32),
        .reserved = reserved,
        .flags = .{
            .reversed = prng.boolean(),
            .padding = if (prng.chance(.{ .numerator = 1, .denominator = 1000 }))
                int_edge_biased(prng, u31)
            else
                0,
        },
    };

    return @sizeOf(tb.QueryFilter);
}

fn build_get_change_events_filter(prng: *stdx.PRNG, buffer: []u8) u32 {
    const filter: *tb.ChangeEventsFilter = filter: {
        const slice = stdx.bytes_as_slice(
            .inexact,
            tb.ChangeEventsFilter,
            buffer,
        );
        if (slice.len == 0) return 0;
        break :filter &slice[0];
    };
    var reserved: [44]u8 = @splat(0);
    if (prng.chance(.{ .numerator = 1, .denominator = 1000 })) {
        prng.fill(&reserved);
    }

    filter.* = .{
        .timestamp_min = int_edge_biased(prng, u64),
        .timestamp_max = int_edge_biased(prng, u64),
        .limit = int_edge_biased(prng, u32),
        .reserved = reserved,
    };

    return @sizeOf(tb.ChangeEventsFilter);
}

test "int_edge_biased" {
    const seed = 42;

    var prng = stdx.PRNG.from_seed(seed);
    var found_max_int: [129]bool = std.mem.zeroes([129]bool);

    // Currently takes ~20 000 random values to hit all maxInts (eg, 0, maxInt(u1), maxInt(u2), etc,
    // for a u128 with a seed of 42. Even if the seed changes, we expect this to find them all
    // within a relatively short space of time.
    for (0..20_000) |_| {
        const int = int_edge_biased(&prng, u128);

        if (int == 0) {
            found_max_int[0] = true;
            continue;
        }

        inline for (1..129) |bits| {
            const IntType = @Int(.unsigned, bits);
            const max = std.math.maxInt(IntType);
            if (int == max) {
                found_max_int[bits] = true;
            }
        }
    }

    assert(std.mem.allEqual(bool, &found_max_int, true));
}

test "create_accounts options swarm coverage" {
    var covered = std.EnumArray(tb.CreateAccountStatus, bool).initFill(false);
    for (0..100) |seed| {
        var prng = stdx.PRNG.from_seed(seed);
        const options = Options.swarm(&prng);
        var model: StateMachineReferenceType(800, 1) = .{};
        for (0..100) |_| {
            var accounts: [8]tb.Account = undefined;
            const size = build_create_accounts(&prng, &options, std.mem.asBytes(&accounts));
            const count = @divExact(size, @sizeOf(tb.Account));
            var results: [8]tb.CreateAccountResult = undefined;
            try model.create_accounts(accounts[0..count], results[0..count]);
            for (results[0..count]) |result| covered.set(result.status, true);
        }
    }

    inline for (@typeInfo(tb.CreateAccountStatus).@"enum".fields) |field| {
        const status: tb.CreateAccountStatus = @enumFromInt(field.value);
        // Those are the ones we ignore for now.
        switch (status) {
            .deprecated_ok => continue,
            .imported_event_not_expected => continue,
            .imported_event_timestamp_must_not_advance => continue,
            .imported_event_timestamp_must_not_regress => continue,
            else => {},
        }
        try std.testing.expect(covered.get(status));
    }
}

// Let account and transfer history accumulate without constructing prerequisites for events.
test "create_transfers options swarm" {
    @setEvalBranchQuota(100_000);
    var covered = std.EnumArray(tb.CreateTransferStatus.Ordered, bool).initFill(false);

    for (0..1_000) |seed| {
        var prng = stdx.PRNG.from_seed(seed);
        const options = Options.swarm(&prng);
        var model: StateMachineReferenceType(400, 400) = .{};
        for (0..100) |_| {
            var accounts: [4]tb.Account = undefined;
            const accounts_size = build_create_accounts(&prng, &options, std.mem.asBytes(&accounts));
            const accounts_count = @divExact(accounts_size, @sizeOf(tb.Account));
            var account_results: [4]tb.CreateAccountResult = undefined;
            try model.create_accounts(accounts[0..accounts_count], account_results[0..accounts_count]);

            var transfers: [4]tb.Transfer = undefined;
            const transfers_size = build_create_transfers(&prng, &options, std.mem.asBytes(&transfers));
            const transfers_count = @divExact(transfers_size, @sizeOf(tb.Transfer));
            var transfer_results: [4]tb.CreateTransferResult = undefined;
            try model.create_transfers(transfers[0..transfers_count], transfer_results[0..transfers_count]);
            for (transfer_results[0..transfers_count]) |result| {
                covered.set(result.status.to_ordered(), true);
            }
        }
    }

    for (std.enums.values(tb.CreateTransferStatus.Ordered)) |status| {
        // const status: tb.CreateTransferStatus.Ordered = @enumFromInt(field.value);
        // Those are the ones we don't hit currently.
        // TODO: need to tweak the swarm.
        switch (status) {
            .deprecated_18 => {
                assert(covered.get(status) == false);
                continue;
            },
            .deprecated_ok => {
                assert(covered.get(status) == false);
                continue;
            },
            .exceeds_pending_transfer_amount => {
                assert(covered.get(status) == false);
                continue;
            },
            .exists_with_different_amount => {
                assert(covered.get(status) == false);
                continue;
            },
            .exists_with_different_code => {
                assert(covered.get(status) == false);
                continue;
            },
            .exists_with_different_ledger => {
                assert(covered.get(status) == false);
                continue;
            },
            .exists_with_different_user_data_64 => {
                assert(covered.get(status) == false);
                continue;
            },
            .imported_event_timeout_must_be_zero => {
                assert(covered.get(status) == false);
                continue;
            },
            .imported_event_timestamp_must_not_regress => {
                assert(covered.get(status) == false);
                continue;
            },
            .imported_event_timestamp_must_postdate_credit_account => {
                assert(covered.get(status) == false);
                continue;
            },
            .imported_event_timestamp_must_postdate_debit_account => {
                assert(covered.get(status) == false);
                continue;
            },
            .overflows_credits => {
                assert(covered.get(status) == false);
                continue;
            },
            .overflows_credits_pending => {
                assert(covered.get(status) == false);
                continue;
            },
            .overflows_debits => {
                assert(covered.get(status) == false);
                continue;
            },
            .overflows_debits_pending => {
                assert(covered.get(status) == false);
                continue;
            },
            .overflows_timeout => {
                assert(covered.get(status) == false);
                continue;
            },
            .pending_transfer_already_posted => {
                assert(covered.get(status) == false);
                continue;
            },
            .pending_transfer_already_voided => {
                assert(covered.get(status) == false);
                continue;
            },
            .pending_transfer_expired => {
                assert(covered.get(status) == false);
                continue;
            },
            .pending_transfer_has_different_amount => {
                assert(covered.get(status) == false);
                continue;
            },
            .pending_transfer_has_different_code => {
                assert(covered.get(status) == false);
                continue;
            },
            // .pending_transfer_has_different_credit_account_id => {
            //     assert(covered.get(status) == false);
            //     continue;
            // },
            //  .pending_transfer_has_different_debit_account_id => {
            //     assert(covered.get(status) == false);
            //     continue;
            // },
            .pending_transfer_has_different_ledger => {
                assert(covered.get(status) == false);
                continue;
            },
            else => {},
        }
        if (!covered.get(status)) {
            log.err("uncovered: {}", .{status});
            return error.TestFailed;
        }
    }
}
