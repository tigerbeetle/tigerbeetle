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

pub fn main(gpa: std.mem.Allocator, args: fuzz.FuzzArgs) !void {
    var context: TestContext = undefined;
    try context.init(gpa);
    defer context.deinit(gpa);

    const request_buffer = try gpa.alignedAlloc(
        u8,
        .fromByteUnits(constants.cache_line_size),
        vsr.constants.message_body_size_max,
    );
    defer gpa.free(request_buffer);

    const reply_buffer = try gpa.alignedAlloc(
        u8,
        .fromByteUnits(constants.cache_line_size),
        vsr.constants.message_body_size_max,
    );
    defer gpa.free(reply_buffer);

    var prng = stdx.PRNG.from_seed(args.seed);

    for (0..args.events_max orelse 100) |_| {
        var operation = prng.enum_uniform(TestContext.StateMachine.Operation);
        operation = .create_accounts;
        const size: usize = size: {
            if (!operation.is_multi_batch()) {
                break :size build_batch(&prng, operation, request_buffer);
            }
            assert(operation.is_multi_batch());

            var body_encoder: MultiBatchEncoder = .init(request_buffer, .{
                .element_size = operation.event_size(),
            });

            const batch_count = prng.enum_uniform(enum { one, random, max });
            while (body_encoder.writable()) |writable| {
                if (writable.len == 0) break;
                const bytes_written: u32 = build_batch(&prng, operation, writable);
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

        if (context.state_machine.input_valid(operation, request_buffer[0..size])) {
            context.prepare(operation, request_buffer[0..size]);
            const reply_size = context.execute(
                context.op,
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
        }
        // Match Replica's commit order, including the delay before released blocks can be reused.
        context.checkpoint_durable();
        context.state_machine_compact();
        const checkpoint_next = vsr.Checkpoint.checkpoint_after(context.checkpoint_op);
        if (context.op == vsr.Checkpoint.trigger_for_checkpoint(checkpoint_next).?) {
            context.state_machine_checkpoint();
            context.grid_checkpoint();
            context.checkpoint_op = checkpoint_next;
            context.grid.mark_checkpoint_not_durable();
        }
        context.op += 1;
    }
}

const TestContext = struct { // TODO: rename to WORLD
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

    fn init(ctx: *TestContext, gpa: std.mem.Allocator) !void {
        ctx.storage = try fixtures.init_storage(gpa, .{ .size = 512 * MiB });
        errdefer ctx.storage.deinit(gpa);

        try fixtures.storage_format(gpa, &ctx.storage, .{
            .replica_count = 1,
        });

        ctx.time_sim = fixtures.init_time(.{});

        ctx.trace = try fixtures.init_tracer(gpa, ctx.time_sim.interface(), .{});
        errdefer ctx.trace.deinit(gpa);

        ctx.superblock = try fixtures.init_superblock(gpa, &ctx.storage, .{
            .storage_size_limit = 256 * MiB,
        });
        errdefer ctx.superblock.deinit(gpa);

        fixtures.open_superblock(&ctx.superblock);

        ctx.grid = try fixtures.init_grid(gpa, &ctx.trace, &ctx.superblock, .{
            .blocks_released_prior_checkpoint_durability_max = StateMachine.Forest
                .compaction_blocks_released_per_pipeline_max(),
        });
        errdefer ctx.grid.deinit(gpa);

        fixtures.open_grid(&ctx.grid);

        const batch_size_limit = 30 * @max(@sizeOf(tb.Account), @sizeOf(tb.Transfer));
        assert(batch_size_limit <= constants.message_body_size_max);
        try ctx.state_machine.init(
            gpa,
            ctx.time_sim.interface(),
            &ctx.grid,
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
        errdefer ctx.state_machine.deinit(gpa);

        ctx.state_machine_open();
    }

    pub fn deinit(ctx: *TestContext, allocator: std.mem.Allocator) void {
        ctx.state_machine.deinit(allocator);
        ctx.grid.deinit(allocator);
        ctx.superblock.deinit(allocator);
        ctx.trace.deinit(allocator);
        ctx.storage.deinit(allocator);
        ctx.* = undefined;
    }

    fn state_machine_open(context: *TestContext) void {
        context.busy = true;
        context.op = 1;
        context.checkpoint_op = 0;
        context.state_machine.open(state_machine_open_callback);

        while (context.busy) context.storage.run();
    }

    fn state_machine_open_callback(state_machine: *StateMachine) void {
        const context: *TestContext = @fieldParentPtr("state_machine", state_machine);
        assert(context.busy);
        context.busy = false;
    }

    fn state_machine_compact(context: *TestContext) void {
        context.busy = true;
        context.state_machine.compact(state_machine_compact_callback, context.op);
        while (context.busy) context.storage.run();
    }

    fn state_machine_compact_callback(state_machine: *StateMachine) void {
        const context: *TestContext = @fieldParentPtr("state_machine", state_machine);
        assert(context.busy);
        context.busy = false;
    }

    fn state_machine_checkpoint(context: *TestContext) void {
        context.busy = true;
        context.state_machine.checkpoint(state_machine_checkpoint_callback);
        while (context.busy) context.storage.run();
    }

    fn checkpoint_durable(context: *TestContext) void {
        if (context.grid.free_set.checkpoint_durable) return;
        const checkpoint_op = context.checkpoint_op;
        if (!vsr.Checkpoint.durable(checkpoint_op, context.op)) return;

        if (vsr.Checkpoint.trigger_for_checkpoint(checkpoint_op)) |trigger| {
            assert(context.op == trigger + constants.pipeline_prepare_queue_max + 1);
        }

        // No repairs run in this fuzzer, so there are no repair writes to await before freeing blocks.
        context.grid.free_set.mark_checkpoint_durable();
    }

    fn state_machine_checkpoint_callback(state_machine: *StateMachine) void {
        const context: *TestContext = @fieldParentPtr("state_machine", state_machine);
        assert(context.busy);
        context.busy = false;
    }

    fn grid_checkpoint(context: *TestContext) void {
        context.busy = true;
        context.grid.checkpoint(grid_checkpoint_callback);
        while (context.busy) context.storage.run();
    }

    fn grid_checkpoint_callback(grid: *Grid) void {
        const context: *TestContext = @alignCast(@fieldParentPtr("grid", grid));
        assert(context.busy);
        context.busy = false;
    }

    fn prepare(
        context: *TestContext,
        operation: StateMachine.Operation,
        message_body_used: []align(constants.cache_line_size) const u8,
    ) void {
        context.state_machine.commit_timestamp = context.state_machine.prepare_timestamp;
        context.state_machine.prepare_timestamp += 1;
        context.state_machine.prepare(
            operation,
            message_body_used,
        );
    }

    fn execute(
        context: *TestContext,
        op: u64,
        operation: StateMachine.Operation,
        message_body_used: []align(constants.cache_line_size) const u8,
        output_buffer: *align(constants.cache_line_size) [constants.message_body_size_max]u8,
    ) usize {
        const timestamp = context.state_machine.prepare_timestamp;
        context.busy = true;
        context.state_machine.prefetch_timestamp = timestamp;
        context.state_machine.prefetch(
            struct {
                fn callback(state_machine: *StateMachine) void {
                    const ctx: *TestContext = @fieldParentPtr("state_machine", state_machine);
                    assert(ctx.busy);
                    ctx.busy = false;
                }
            }.callback,
            op,
            op,
            operation,
            message_body_used,
        );
        while (context.busy) context.storage.run();

        return context.state_machine.commit(
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

fn build_batch(
    prng: *stdx.PRNG,
    operation: TestContext.StateMachine.Operation,
    buffer: []u8,
) u32 {
    return switch (operation) {
        // No payload, so not very interesting yet.
        .pulse => 0,

        // No payload, `create_*` require compaction to be hooked up.
        .create_transfers,
        => 0,
        .create_accounts => {
            const account: tb.Account = .{
                .id = prng.int(u128),
                .debits_pending = 0,
                .debits_posted = 0,
                .credits_pending = 0,
                .credits_posted = 0,
                .user_data_128 = 0,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .reserved = 0,
                .ledger = 1,
                .code = 1,
                .flags = .{},
                .timestamp = 0,
            };
            stdx.copy_disjoint(.inexact, u8, buffer, std.mem.asBytes(&account));
            return @sizeOf(tb.Account);
        },

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
