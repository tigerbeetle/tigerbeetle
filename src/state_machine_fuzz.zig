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

const TimeSim = @import("testing/time.zig").TimeSim;
const Storage = @import("testing/storage.zig").Storage;
const Tracer = Storage.Tracer;
const data_file_size_min = @import("vsr/superblock.zig").data_file_size_min;
const SuperBlock = @import("vsr/superblock.zig").SuperBlockType(Storage);
const Grid = @import("vsr/grid.zig").GridType(Storage);
const fixtures = @import("testing/fixtures.zig");

const TestContext = struct {
    storage: Storage,
    time_sim: TimeSim,
    trace: Tracer,
    superblock: SuperBlock,
    grid: Grid,
    state_machine: StateMachine,
    op: u64,
    busy: bool,

    const StateMachine = vsr.state_machine.StateMachineType(Storage);

    fn init(ctx: *TestContext, allocator: std.mem.Allocator) !void {
        ctx.storage = try fixtures.init_storage(allocator, .{ .size = 4096 });
        errdefer ctx.storage.deinit(allocator);

        ctx.time_sim = fixtures.init_time(.{});

        ctx.trace = try fixtures.init_tracer(allocator, ctx.time_sim.time(), .{});
        errdefer ctx.trace.deinit(allocator);

        ctx.superblock = try fixtures.init_superblock(allocator, &ctx.storage, .{
            .storage_size_limit = data_file_size_min,
        });
        errdefer ctx.superblock.deinit(allocator);

        // Pretend that the superblock is open so that the Forest can initialize.
        ctx.superblock.opened = true;
        ctx.superblock.working.vsr_state.checkpoint.header.op = 0;

        ctx.grid = try fixtures.init_grid(allocator, &ctx.trace, &ctx.superblock, .{});
        errdefer ctx.grid.deinit(allocator);

        const batch_size_limit = 30 * @max(@sizeOf(tb.Account), @sizeOf(tb.Transfer));
        assert(batch_size_limit <= constants.message_body_size_max);
        try ctx.state_machine.init(
            allocator,
            ctx.time_sim.time(),
            &ctx.grid,
            .{
                .batch_size_limit = batch_size_limit,
                .lsm_forest_compaction_block_count = StateMachine.Forest.Options
                    .compaction_block_count_min,
                .lsm_forest_node_count = 1,
                .cache_entries_accounts = 0,
                .cache_entries_transfers = 0,
                .cache_entries_transfers_pending = 0,
                .log_trace = true,
                .aof_recovery = false,
            },
        );
        errdefer ctx.state_machine.deinit(allocator);

        ctx.op = 1;
        ctx.busy = false;
    }

    pub fn deinit(ctx: *TestContext, allocator: std.mem.Allocator) void {
        ctx.state_machine.deinit(allocator);
        ctx.grid.deinit(allocator);
        ctx.superblock.deinit(allocator);
        ctx.trace.deinit(allocator);
        ctx.storage.deinit(allocator);
        ctx.* = undefined;
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

pub fn main(allocator: std.mem.Allocator, args: fuzz.FuzzArgs) !void {
    var context: TestContext = undefined;
    try context.init(allocator);
    defer context.deinit(allocator);

    const request_buffer = try allocator.alignedAlloc(
        u8,
        constants.cache_line_size,
        vsr.constants.message_body_size_max,
    );
    defer allocator.free(request_buffer);

    const reply_buffer = try allocator.alignedAlloc(
        u8,
        constants.cache_line_size,
        vsr.constants.message_body_size_max,
    );
    defer allocator.free(reply_buffer);

    var prng = stdx.PRNG.from_seed(args.seed);

    var op: u64 = 1;

    for (0..args.events_max orelse 50_000) |_| {
        const operation = prng.enum_uniform(TestContext.StateMachine.Operation);
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
                op,
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
        op += 1;
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
        .create_accounts,
        .create_transfers,
        => 0,
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
            const IntType = @Type(.{ .int = .{
                .signedness = .unsigned,
                .bits = bits,
            } });
            const max = std.math.maxInt(IntType);
            if (int == max) {
                found_max_int[bits] = true;
            }
        }
    }

    assert(std.mem.allEqual(bool, &found_max_int, true));
}
