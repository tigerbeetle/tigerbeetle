//! Very simple state machine fuzzer. It looks for poison pill style ops that are otherwise valid
//! which cause a crash, then be replayed after said crash, resulting in a crash loop.
const std = @import("std");
const assert = std.debug.assert;

const vsr = @import("vsr.zig");
const constants = vsr.constants;
const stdx = @import("stdx");

const tb = @import("tigerbeetle.zig");
const fuzz = @import("./testing/fuzz.zig");

const MultiBatchDecoder = @import("./vsr/multi_batch.zig").MultiBatchDecoder;
const MultiBatchEncoder = @import("./vsr/multi_batch.zig").MultiBatchEncoder;

const TimeSim = stdx.TimeSim;
const Storage = @import("testing/storage.zig").Storage;
const Tracer = Storage.Tracer;
const SuperBlock = @import("vsr/superblock.zig").SuperBlockType(Storage);
const Grid = @import("vsr/grid.zig").GridType(Storage);
const fixtures = @import("testing/fixtures.zig");
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

    for (0..args.events_max orelse 100) |_| {
        const operation = prng.enum_weighted(
            World.StateMachine.Operation,
            options.operation_weights,
        );
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
            if (operation == .create_transfers) {
                var decoder = MultiBatchDecoder.init(request_buffer[0..size], .{
                    .element_size = @sizeOf(tb.Transfer),
                }) catch unreachable;
                const transfer_max = if (world.state_machine.forest.grooves.transfers.objects
                    .key_range) |range| range.key_max else 0;
                const cdc_max = world.cdc_timestamp_max();
                while (decoder.pop()) |batch| {
                    for (stdx.bytes_as_slice(.exact, tb.Transfer, @constCast(batch))) |*transfer| {
                        // Keep IDs, accounts, and validation errors from ordinary generation.
                        // Bias imports to the gap exposed by expiry events, and its boundaries.
                        if (prng.chance(ratio(1, 10))) transfer.flags.imported = true;
                        if (transfer.flags.imported and prng.boolean()) {
                            transfer.timestamp = switch (prng.enum_uniform(enum {
                                after_transfer,
                                at_cdc,
                                after_cdc,
                            })) {
                                .after_transfer => transfer_max + 1,
                                .at_cdc => cdc_max,
                                .after_cdc => cdc_max + 1,
                            };
                        }
                    }
                }
            }
            const elapsed_ns: u64 = switch (prng.enum_weighted(TimeStep, options.time_weights)) {
                .zero => 0,
                .milliseconds_100 => 100 * std.time.ns_per_ms,
                .milliseconds_900 => 900 * std.time.ns_per_ms,
                .second => std.time.ns_per_s,
                .seconds_3 => 3 * std.time.ns_per_s,
            };
            world.prepare(operation, request_buffer[0..size], elapsed_ns);
            const cdc_timestamp_before = world.cdc_timestamp_max();
            const reply_size = world.execute(
                world.op,
                operation,
                request_buffer[0..size],
                @ptrCast(reply_buffer),
            );
            stdx.maybe(reply_size == 0);
            if (operation == .create_transfers) {
                var decoder = MultiBatchDecoder.init(reply_buffer[0..reply_size], .{
                    .element_size = @sizeOf(tb.CreateTransferResult),
                }) catch unreachable;
                var watermark = cdc_timestamp_before;
                while (decoder.pop()) |batch| {
                    for (stdx.bytes_as_slice(.exact, tb.CreateTransferResult, batch)) |result| {
                        // Failed/exists events do not insert CDC. Linked rollbacks are
                        // reflected in their final statuses, so only created events advance it.
                        if (result.status == .created) {
                            if (result.timestamp <= watermark) {
                                log.err(
                                    "CDC timestamp regression: op={} timestamp={} watermark={}",
                                    .{ world.op, result.timestamp, watermark },
                                );
                                @panic("created transfer regressed CDC timestamp");
                            }
                            watermark = result.timestamp;
                        }
                    }
                }
            }
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
        world.tick();
    }
    log.info("expiration coverage: pulses={} transfers={}", .{
        world.expiration_pulses,
        world.expiration_transfers,
    });
}

const World = struct {
    storage: Storage,
    time_sim: TimeSim,
    trace: Tracer,
    superblock: SuperBlock,
    grid: Grid,
    state_machine: StateMachine,
    // This fuzzer never reopens storage, so checkpoint progress is only tracked in memory.
    checkpoint_op: u64,
    op: u64,
    busy: bool,
    expiration_pulses: u64,
    expiration_transfers: u64,

    const StateMachine = vsr.state_machine.StateMachineType(Storage);

    fn tick(world: *World) void {
        // Match Replica's commit order, including delayed reuse of released blocks.
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

    fn request(
        world: *World,
        comptime operation: StateMachine.Operation,
        events: []const operation.EventType(),
        output: *align(constants.cache_line_size) [constants.message_body_size_max]u8,
    ) []const operation.ResultType() {
        var input: [constants.message_body_size_max]u8 align(constants.cache_line_size) = undefined;
        const bytes = std.mem.sliceAsBytes(events);
        const size = if (operation.is_multi_batch()) size: {
            var encoder = MultiBatchEncoder.init(
                &input,
                .{ .element_size = operation.event_size() },
            );
            stdx.copy_disjoint(.inexact, u8, encoder.writable().?, bytes);
            encoder.add(@intCast(bytes.len));
            break :size encoder.finish();
        } else size: {
            stdx.copy_disjoint(.inexact, u8, &input, bytes);
            break :size bytes.len;
        };
        assert(world.state_machine.input_valid(operation, input[0..size]));
        world.prepare(operation, input[0..size], 0);
        const reply_size = world.execute(world.op, operation, input[0..size], output);
        world.tick();
        const reply = if (operation.is_multi_batch()) reply: {
            var decoder = MultiBatchDecoder.init(output[0..reply_size], .{
                .element_size = operation.result_size(),
            }) catch unreachable;
            const batch = decoder.pop().?;
            assert(decoder.pop() == null);
            break :reply batch;
        } else output[0..reply_size];
        return stdx.bytes_as_slice(.exact, operation.ResultType(), reply);
    }

    fn cdc_timestamp_max(world: *const World) u64 {
        return if (world.state_machine.forest.grooves.account_events.objects.key_range) |range|
            range.key_max
        else
            0;
    }

    fn init(world: *World, gpa: std.mem.Allocator) !void {
        world.expiration_pulses = 0;
        world.expiration_transfers = 0;
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

        // No repairs run in this fuzzer, so there are no repair writes to await before
        // freeing blocks.
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
        elapsed_ns: u64,
    ) void {
        // Preserve the previous commit boundary before advancing logical time.
        world.state_machine.commit_timestamp = world.state_machine.prepare_timestamp;
        world.state_machine.prepare_timestamp += elapsed_ns + 1;
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

        // Commit clears the prefetch results. Retain only the number of expirations,
        // independently of whether the worker inserts their CDC records.
        const expiration_count: u32 = if (operation == .pulse and
            world.state_machine.pulse.action == .expire_pending_transfers)
            world.state_machine.scan_lookup_results.items[0]
        else
            0;

        const reply_size = world.state_machine.commit(
            1,
            op,
            timestamp,
            operation,
            message_body_used,
            output_buffer,
        );
        if (expiration_count > 0) {
            // The last expiry event has the pulse timestamp, including partial pulses.
            // CDC must be recorded regardless of Account.flags.history.
            const key_range = world.state_machine.forest.grooves.account_events.objects
                .key_range orelse @panic("missing expiration CDC");
            assert(key_range.key_max == timestamp);
            world.expiration_pulses += 1;
            world.expiration_transfers += expiration_count;
        }
        return reply_size;
    }
};

/// Generate a random number, biased towards all bit 'edges' of T. That is, given a u64, it's very
/// likely to not only get 0 or maxInt(u64), but also values around maxInt(u63), maxInt(u62), ...,
/// maxInt(u1).
pub fn int_edge_biased(prng: *stdx.PRNG, comptime T: type) T {
    const bits = @typeInfo(T).int.bits;
    comptime assert(@typeInfo(T).int.signedness == .unsigned);

    // With bits * 2, there's a ~50% chance of generating a uniform integer within the full range,
    // and a ~50% chance of generating an integer biased towards an edge.
    const bias_to = prng.range_inclusive(T, 0, bits * 2);

    if (bias_to > bits) return prng.int(T);

    const bias_center: T = if (bias_to == bits)
        std.math.maxInt(T)
    else
        @as(T, 1) << @intCast(bias_to);
    const bias_min = if (bias_to == 0) 0 else bias_center - @min(bias_center, 8);
    const bias_max = if (bias_to == bits) bias_center else bias_center + 8;

    return prng.range_inclusive(T, bias_min, bias_max);
}

const TimeStep = enum { zero, milliseconds_100, milliseconds_900, second, seconds_3 };

const Options = struct {
    operation_weights: stdx.PRNG.EnumWeightsType(World.StateMachine.Operation),
    transfers_weights: stdx.PRNG.EnumWeightsType(TransferKind),
    account_id_max: u128,
    transfer_id_max: u128,
    account_mutation_probability: MutationProbabilityType(tb.Account),
    transfer_mutation_probability: MutationProbabilityType(tb.Transfer),
    repeat_probability: Ratio,
    repeats_seed: u64,
    reads: ReadOptions,
    time_weights: stdx.PRNG.EnumWeightsType(TimeStep),
    small_timeout_probability: Ratio,
    timestamp_start: u64,

    fn swarm(prng: *stdx.PRNG) Options {
        // Keep the existing create sequence stable while adding read-operation swarms.
        var prng_reads = prng.*;
        var operation_weights = prng.enum_weights(World.StateMachine.Operation);
        // Keep both creates enabled while swarming the surrounding operations.
        operation_weights.create_accounts = prng.range_inclusive(u64, 100, 1000);
        operation_weights.create_transfers = prng.range_inclusive(u64, 100, 1000);
        operation_weights.pulse = prng_reads.range_inclusive(u64, 100, 1000);
        const timeout_max_ns: u64 = std.math.maxInt(u32) * std.time.ns_per_s;
        return .{
            .operation_weights = operation_weights,
            .transfers_weights = prng.enum_weights(TransferKind),
            .account_id_max = prng.range_inclusive(u128, 1, 256),
            .transfer_id_max = prng.range_inclusive(u128, 1, 256),
            .account_mutation_probability = mutation_probability_swarm(prng, tb.Account),
            .transfer_mutation_probability = mutation_probability_swarm(prng, tb.Transfer),
            .repeats_seed = prng.int(u64),
            .repeat_probability = ratio(
                prng.int_inclusive(u8, 80),
                100,
            ),
            .reads = ReadOptions.swarm(&prng_reads),
            .time_weights = prng_reads.enum_weights(TimeStep),
            .small_timeout_probability = ratio(prng_reads.int_inclusive(u8, 100), 100),
            // Large timeouts can overflow u63, with decades of headroom for the run itself.
            .timestamp_start = if (prng_reads.boolean())
                std.math.maxInt(u63) - timeout_max_ns / 2
            else
                0,
        };
    }
};

const LookupKind = enum { pool, edge };
const FilterKind = enum { broad, selective, mutated };

const ReadOptions = struct {
    lookup_weights: stdx.PRNG.EnumWeightsType(LookupKind),
    filter_weights: stdx.PRNG.EnumWeightsType(FilterKind),
    account_filter_mutation_probability: MutationProbabilityType(tb.AccountFilter),
    query_filter_mutation_probability: MutationProbabilityType(tb.QueryFilter),
    change_events_filter_mutation_probability: MutationProbabilityType(tb.ChangeEventsFilter),

    fn swarm(prng: *stdx.PRNG) ReadOptions {
        return .{
            .lookup_weights = prng.enum_weights(LookupKind),
            .filter_weights = prng.enum_weights(FilterKind),
            .account_filter_mutation_probability = mutation_probability_swarm(
                prng,
                tb.AccountFilter,
            ),
            .query_filter_mutation_probability = mutation_probability_swarm(prng, tb.QueryFilter),
            .change_events_filter_mutation_probability = mutation_probability_swarm(
                prng,
                tb.ChangeEventsFilter,
            ),
        };
    }
};

fn MutationProbabilityType(comptime Event: type) type {
    return std.enums.EnumFieldStruct(std.meta.FieldEnum(Event), stdx.PRNG.Ratio, null);
}

fn mutation_probability_swarm(
    prng: *stdx.PRNG,
    comptime Event: type,
) MutationProbabilityType(Event) {
    var probability: MutationProbabilityType(Event) = undefined;
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
    probability: MutationProbabilityType(Event),
    event: *Event,
) void {
    inline for (std.meta.fields(Event)) |field| {
        if (comptime std.mem.eql(u8, field.name, "flags")) {
            inline for (std.meta.fields(field.type)) |flag| {
                if (prng.chance(probability.flags)) {
                    @field(event.flags, flag.name) = if (flag.type == bool)
                        prng.boolean()
                    else
                        int_edge_biased(prng, flag.type);
                }
            }
        } else if (prng.chance(@field(probability, field.name))) {
            if (comptime @typeInfo(field.type) == .array) {
                prng.fill(&@field(event, field.name));
            } else {
                // Mix small values with values near integer boundaries.
                @field(event, field.name) = if (prng.boolean())
                    prng.int_inclusive(field.type, 30)
                else
                    int_edge_biased(prng, field.type);
            }
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

const TransferKind = enum {
    one_phase,
    two_phase,
};

fn build_create_transfers(prng: *stdx.PRNG, options: *const Options, buffer: []u8) u32 {
    const transfers = stdx.bytes_as_slice(.inexact, tb.Transfer, buffer);
    const count = prng.int_inclusive(u32, @intCast(transfers.len));
    for (transfers[0..count]) |*transfer| {
        const transfer_kind = prng.enum_weighted(TransferKind, options.transfers_weights);
        switch (transfer_kind) {
            .one_phase => {
                if (prng.chance(options.repeat_probability)) {
                    var prng_low_cardinality = stdx.PRNG.from_seed(
                        prng.int_inclusive(u64, 10) ^ options.repeats_seed,
                    );
                    build_transfer(&prng_low_cardinality, options, transfer);
                    if (prng.chance(ratio(2, 10))) {
                        // Give small fields such as code and ledger the same chance as IDs.
                        switch (prng.enum_uniform(std.meta.FieldEnum(tb.Transfer))) {
                            .flags => transfer.flags = @bitCast(
                                @as(u16, @bitCast(transfer.flags)) ^ prng.bit(u16),
                            ),
                            inline else => |field| {
                                const value = &@field(transfer, @tagName(field));
                                value.* ^= prng.bit(@TypeOf(value.*));
                            },
                        }
                    }
                } else {
                    build_transfer(prng, options, transfer);
                }
            },
            .two_phase => {
                // Revisit a small pool of pending candidates across batches. The same candidate
                // supplies the pending ID and accounts for a resolution, without consulting the
                // model or guaranteeing that the pending transfer was successfully created.
                var prng_low_cardinality = stdx.PRNG.from_seed(
                    prng.int_inclusive(u64, 10) ^ options.repeats_seed,
                );
                const related = prng.chance(options.repeat_probability);
                const prng_candidate = if (related) &prng_low_cardinality else prng;
                transfer.* = std.mem.zeroes(tb.Transfer);
                transfer.id = prng_candidate.range_inclusive(u128, 1, options.transfer_id_max);
                transfer.debit_account_id = prng_candidate.range_inclusive(
                    u128,
                    1,
                    options.account_id_max,
                );
                transfer.credit_account_id = prng_candidate.range_inclusive(
                    u128,
                    1,
                    options.account_id_max,
                );
                const pending = prng.boolean();
                if (pending) {
                    transfer.flags = tb.TransferFlags{ .pending = true };
                } else {
                    transfer.flags = if (prng.boolean())
                        tb.TransferFlags{ .post_pending_transfer = true }
                    else
                        tb.TransferFlags{ .void_pending_transfer = true };
                    transfer.pending_id = if (related)
                        transfer.id
                    else
                        prng.range_inclusive(u128, 1, options.transfer_id_max);
                    transfer.id = prng.range_inclusive(u128, 1, options.transfer_id_max);
                }
                // Exercise partial posts, exact resolutions, excessive amounts, and voids with
                // a different amount, even when amount mutation is disabled for this seed.
                transfer.amount = if (pending)
                    (if (prng.chance(ratio(1, 10))) std.math.maxInt(u128) else 2)
                else
                    prng.int_inclusive(u128, 3);
                transfer.ledger = 1;
                transfer.code = 1;
                mutate_event(prng, tb.Transfer, options.transfer_mutation_probability, transfer);
            },
        }
        // Allow short expiry deadlines without constructing successful prerequisites.
        if (transfer.flags.pending and !transfer.flags.imported and
            prng.chance(options.small_timeout_probability))
        {
            transfer.timeout = prng.range_inclusive(u32, 1, 3);
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
        // Logical time advances before prepare; pulses can expire generated transfers.
        .pulse => 0,

        .create_transfers => build_create_transfers(prng, options, buffer),
        .create_accounts => build_create_accounts(prng, options, buffer),

        .deprecated_create_accounts_sparse,
        .deprecated_create_transfers_sparse,
        => 0,
        .deprecated_create_accounts_unbatched,
        .deprecated_create_transfers_unbatched,
        => 0,

        .lookup_accounts => build_lookup(prng, options, options.account_id_max, buffer),
        .lookup_transfers => build_lookup(prng, options, options.transfer_id_max, buffer),
        .get_account_transfers,
        .get_account_balances,
        => build_filter(prng, options, tb.AccountFilter, buffer),
        .query_accounts, .query_transfers => build_filter(prng, options, tb.QueryFilter, buffer),
        .get_change_events => build_filter(prng, options, tb.ChangeEventsFilter, buffer),

        .deprecated_lookup_accounts_unbatched => build_lookup(
            prng,
            options,
            options.account_id_max,
            buffer,
        ),
        .deprecated_lookup_transfers_unbatched => build_lookup(
            prng,
            options,
            options.transfer_id_max,
            buffer,
        ),
        .deprecated_get_account_transfers_unbatched,
        .deprecated_get_account_balances_unbatched,
        => build_filter(prng, options, tb.AccountFilter, buffer),
        .deprecated_query_accounts_unbatched,
        .deprecated_query_transfers_unbatched,
        => build_filter(prng, options, tb.QueryFilter, buffer),
    };
}

fn build_lookup(prng: *stdx.PRNG, options: *const Options, id_max: u128, buffer: []u8) u32 {
    const ids = stdx.bytes_as_slice(.inexact, u128, buffer);
    const count = prng.int_inclusive(u32, @intCast(ids.len));
    for (ids[0..count]) |*id| {
        var prng_low_cardinality = stdx.PRNG.from_seed(
            prng.int_inclusive(u64, 10) ^ options.repeats_seed,
        );
        const prng_candidate = if (prng.chance(options.repeat_probability))
            &prng_low_cardinality
        else
            prng;
        id.* = switch (prng_candidate.enum_weighted(LookupKind, options.reads.lookup_weights)) {
            .pool => prng_candidate.range_inclusive(u128, 1, id_max),
            .edge => int_edge_biased(prng_candidate, u128),
        };
    }
    return count * @sizeOf(u128);
}

fn build_filter(
    prng: *stdx.PRNG,
    options: *const Options,
    comptime Filter: type,
    buffer: []u8,
) u32 {
    const filters = stdx.bytes_as_slice(.inexact, Filter, buffer);
    if (filters.len == 0) return 0;
    const filter = &filters[0];
    var prng_low_cardinality = stdx.PRNG.from_seed(
        prng.int_inclusive(u64, 10) ^ options.repeats_seed,
    );
    const prng_candidate = if (prng.chance(options.repeat_probability))
        &prng_low_cardinality
    else
        prng;
    const kind = prng_candidate.enum_weighted(FilterKind, options.reads.filter_weights);

    // Unset optional constraints allow accumulated history to match. IDs come from the same
    // pool as creates, without consulting the model or guaranteeing a matching record.
    filter.* = std.mem.zeroes(Filter);
    filter.limit = prng_candidate.range_inclusive(u32, 1, 256);
    if (Filter == tb.AccountFilter) {
        filter.account_id = prng_candidate.range_inclusive(u128, 1, options.account_id_max);
        filter.flags.debits = true;
        filter.flags.credits = true;
    }
    if (@hasField(Filter, "flags")) filter.flags.reversed = prng_candidate.boolean();

    switch (kind) {
        .broad => {},
        .selective => {
            inline for (.{ "user_data_128", "user_data_64", "user_data_32" }) |field| {
                if (@hasField(Filter, field) and prng_candidate.boolean()) {
                    @field(filter, field) = prng_candidate.int_inclusive(
                        @FieldType(Filter, field),
                        30,
                    );
                }
            }
            if (@hasField(Filter, "ledger")) filter.ledger = if (prng_candidate.boolean()) 1 else 0;
            if (@hasField(Filter, "code")) filter.code = if (prng_candidate.boolean()) 1 else 0;
            const timestamp_a = prng_candidate.int_inclusive(u64, 1000);
            const timestamp_b = prng_candidate.int_inclusive(u64, 1000);
            filter.timestamp_min = @min(timestamp_a, timestamp_b);
            filter.timestamp_max = @max(timestamp_a, timestamp_b);
            if (Filter == tb.AccountFilter) {
                filter.flags.debits = prng_candidate.boolean();
                filter.flags.credits = !filter.flags.debits or prng_candidate.boolean();
            }
        },
        .mutated => {
            const probability = switch (Filter) {
                tb.AccountFilter => options.reads.account_filter_mutation_probability,
                tb.QueryFilter => options.reads.query_filter_mutation_probability,
                tb.ChangeEventsFilter => options.reads.change_events_filter_mutation_probability,
                else => unreachable,
            };
            mutate_event(prng_candidate, Filter, probability, filter);
        },
    }
    return @sizeOf(Filter);
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
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var covered = std.EnumArray(tb.CreateAccountStatus, bool).initFill(false);
    for (0..100) |seed| {
        defer assert(arena.reset(.retain_capacity));

        var prng = stdx.PRNG.from_seed(seed);
        const options = Options.swarm(&prng);
        var world: World = undefined;
        try world.init(arena.allocator());
        defer world.deinit(arena.allocator());

        var output: [constants.message_body_size_max]u8 align(constants.cache_line_size) =
            undefined;
        for (0..100) |_| {
            var accounts: [8]tb.Account = undefined;
            const size = build_create_accounts(&prng, &options, std.mem.asBytes(&accounts));
            const count = @divExact(size, @sizeOf(tb.Account));
            const results = world.request(.create_accounts, accounts[0..count], &output);
            for (results) |result| covered.set(result.status, true);
        }
    }

    for (std.enums.values(tb.CreateAccountStatus)) |status| {
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
    // Reuse the fixture's storage allocation across seeds.
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var covered = std.EnumArray(tb.CreateTransferStatus.Ordered, bool).initFill(false);
    var required = std.EnumArray(tb.CreateTransferStatus.Ordered, bool).initFill(true);
    // Deprecated results are unreachable; expiry and imported timeouts need dedicated swarms.
    for ([_]tb.CreateTransferStatus.Ordered{
        .deprecated_ok,
        .deprecated_18,
        .imported_event_timeout_must_be_zero,
        .pending_transfer_expired,
    }) |status| required.set(status, false);

    // Try seeds that previously advanced coverage before discovering additional statuses.
    const seeds = [_]u64{
        0,   1,    2,    3,    4,    6,    9,    16,   30,   31,
        50,  71,   80,   92,   95,   102,  112,  113,  115,  144,
        153, 212,  238,  303,  304,  307,  343,  346,  458,  591,
        703, 1412, 1669, 1842, 2746, 3597, 3603, 3895, 4263, 12374,
    };

    var prng_seed = stdx.PRNG.from_seed_testing();

    for (0..seeds.len + 50_000) |seed_index| {
        const seed: u64 = if (seed_index < seeds.len)
            seeds[seed_index]
        else
            prng_seed.int(u64);
        defer assert(arena.reset(.retain_capacity));

        var prng = stdx.PRNG.from_seed(seed);
        const options = Options.swarm(&prng);
        var world: World = undefined;
        try world.init(arena.allocator());
        defer world.deinit(arena.allocator());
        world.state_machine.prepare_timestamp = options.timestamp_start;

        var output: [constants.message_body_size_max]u8 align(constants.cache_line_size) =
            undefined;
        for (0..100) |_| {
            var accounts: [4]tb.Account = undefined;
            const accounts_size = build_create_accounts(
                &prng,
                &options,
                std.mem.asBytes(&accounts),
            );
            const accounts_count = @divExact(accounts_size, @sizeOf(tb.Account));
            _ = world.request(.create_accounts, accounts[0..accounts_count], &output);

            var transfers: [4]tb.Transfer = undefined;
            const transfers_size = build_create_transfers(
                &prng,
                &options,
                std.mem.asBytes(&transfers),
            );
            const transfers_count = @divExact(transfers_size, @sizeOf(tb.Transfer));
            const transfer_results = world.request(
                .create_transfers,
                transfers[0..transfers_count],
                &output,
            );
            for (transfer_results) |result| {
                covered.set(result.status.to_ordered(), true);
            }
        }
        var iterator = covered.iterator();
        var complete = true;
        while (iterator.next()) |entry| {
            if (required.get(entry.key) and !entry.value.*) complete = false;
        }
        if (complete) break;
    }

    for (std.enums.values(tb.CreateTransferStatus.Ordered)) |status| {
        if (required.get(status) and !covered.get(status)) {
            std.debug.print("uncovered: {}\n", .{status});
            return error.TestFailed;
        }
    }
}

// Exercise reads against existing history, so returning records and returning nothing both
// remain represented as the generator changes.
test "read options swarm" {
    var world: World = undefined;
    try world.init(std.testing.allocator);
    defer world.deinit(std.testing.allocator);

    var output: [constants.message_body_size_max]u8 align(constants.cache_line_size) = undefined;
    var accounts: [2]tb.Account = @splat(std.mem.zeroes(tb.Account));
    for (&accounts, 1..) |*account, id| {
        account.id = id;
        account.ledger = 1;
        account.code = 1;
    }
    _ = world.request(.create_accounts, &accounts, &output);
    var transfer = std.mem.zeroes(tb.Transfer);
    transfer.id = 1;
    transfer.debit_account_id = 1;
    transfer.credit_account_id = 2;
    transfer.ledger = 1;
    transfer.code = 1;
    transfer.amount = 1;
    const transfer_results = world.request(.create_transfers, &.{transfer}, &output);
    try std.testing.expectEqual(tb.CreateTransferStatus.created, transfer_results[0].status);

    var lookup_hits: usize = 0;
    var lookup_misses: usize = 0;
    var query_hits: usize = 0;
    var query_misses: usize = 0;
    var query_invalid: usize = 0;
    for (0..100) |seed| {
        var prng = stdx.PRNG.from_seed(seed);
        const options = Options.swarm(&prng);
        for (0..20) |_| {
            var ids: [8]u128 = undefined;
            const size = build_batch(&prng, &options, .lookup_accounts, std.mem.asBytes(&ids));
            const ids_count = @divExact(size, @sizeOf(u128));
            const lookup_results = world.request(.lookup_accounts, ids[0..ids_count], &output);
            lookup_hits += lookup_results.len;
            lookup_misses += ids_count - lookup_results.len;
            var filter: tb.QueryFilter = undefined;
            _ = build_batch(&prng, &options, .query_transfers, std.mem.asBytes(&filter));
            const results = world.request(.query_transfers, &.{filter}, &output);
            if (filter.limit == 0 or filter.flags.padding != 0 or !stdx.zeroed(&filter.reserved)) {
                try std.testing.expectEqual(@as(usize, 0), results.len);
                query_invalid += 1;
            } else {
                if (results.len != 0) query_hits += 1 else query_misses += 1;
            }
            // Also instantiate the account and change-event filter builders and their mutations.
            var account_filter: tb.AccountFilter = undefined;
            try std.testing.expectEqual(@as(u32, @sizeOf(tb.AccountFilter)), build_batch(
                &prng,
                &options,
                .get_account_transfers,
                std.mem.asBytes(&account_filter),
            ));
            var change_filter: tb.ChangeEventsFilter = undefined;
            try std.testing.expectEqual(@as(u32, @sizeOf(tb.ChangeEventsFilter)), build_batch(
                &prng,
                &options,
                .get_change_events,
                std.mem.asBytes(&change_filter),
            ));
        }
    }
    try std.testing.expect(lookup_hits > 0 and lookup_misses > 0);
    try std.testing.expect(query_hits > 0 and query_misses > 0 and query_invalid > 0);
}
