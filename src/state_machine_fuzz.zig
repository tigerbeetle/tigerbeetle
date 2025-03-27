//! Very simple state machine fuzzer. It looks for poison pill style ops that are otherwise valid
//! which cause a crash, then be replayed after said crash, resulting in a crash loop.
const std = @import("std");
const assert = std.debug.assert;

const stdx = @import("stdx.zig");

const tb = @import("tigerbeetle.zig");
const TestContext = @import("state_machine.zig").TestContext;
const fuzz = @import("./testing/fuzz.zig");

/// Generate a random number, biased towards all bit 'edges' of T. That is, given a u64, it's very
/// likely to not only get 0 or maxInt(u64), but also values around maxInt(u63), maxInt(u62), ...,
/// maxInt(u1).
pub fn int_edge_biased(prng: *stdx.PRNG, T: anytype) T {
    const bits = @typeInfo(T).Int.bits;
    comptime assert(@typeInfo(T).Int.signedness == .unsigned);

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

    const request_buffer_full = try allocator.alignedAlloc(
        u8,
        16,
        TestContext.message_body_size_max,
    );
    defer allocator.free(request_buffer_full);

    const reply_buffer = try allocator.alignedAlloc(
        u8,
        16,
        TestContext.message_body_size_max,
    );
    defer allocator.free(reply_buffer);

    var prng = stdx.PRNG.from_seed(args.seed);

    var op: u64 = 1;

    for (0..args.events_max orelse 10_000_000) |_| {
        const operation = prng.enum_uniform(TestContext.StateMachine.Operation);
        const operation_size: u64 = switch (operation) {
            inline else => |tag| @sizeOf(TestContext.StateMachine.EventType(tag)),
        };
        const request_buffer_operation = request_buffer_full[0..operation_size];

        switch (operation) {
            .query_accounts, .query_transfers => {
                const query_filter: *tb.QueryFilter = @ptrCast(request_buffer_operation);
                var reserved = std.mem.zeroes([6]u8);
                if (prng.chance(.{ .numerator = 1, .denominator = 1000 })) {
                    prng.fill(&reserved);
                }

                query_filter.* = .{
                    .user_data_128 = int_edge_biased(&prng, u128),
                    .user_data_64 = int_edge_biased(&prng, u64),
                    .user_data_32 = int_edge_biased(&prng, u32),
                    .ledger = int_edge_biased(&prng, u32),
                    .code = int_edge_biased(&prng, u16),
                    .timestamp_min = int_edge_biased(&prng, u64),
                    .timestamp_max = int_edge_biased(&prng, u64),
                    .limit = int_edge_biased(&prng, u32),
                    .reserved = reserved,
                    .flags = .{
                        .reversed = prng.boolean(),
                        .padding = if (prng.chance(.{ .numerator = 1, .denominator = 1000 }))
                            int_edge_biased(&prng, u31)
                        else
                            0,
                    },
                };
            },
            .get_account_balances, .get_account_transfers => {
                const account_filter: *tb.AccountFilter = @ptrCast(request_buffer_operation);
                var reserved = std.mem.zeroes([58]u8);
                if (prng.chance(.{ .numerator = 1, .denominator = 1000 })) {
                    prng.fill(&reserved);
                }

                account_filter.* = .{
                    .account_id = int_edge_biased(&prng, u128),
                    .user_data_128 = int_edge_biased(&prng, u128),
                    .user_data_64 = int_edge_biased(&prng, u64),
                    .user_data_32 = int_edge_biased(&prng, u32),
                    .code = int_edge_biased(&prng, u16),
                    .timestamp_min = int_edge_biased(&prng, u64),
                    .timestamp_max = int_edge_biased(&prng, u64),
                    .limit = int_edge_biased(&prng, u32),
                    .reserved = reserved,
                    .flags = .{
                        .reversed = prng.boolean(),
                        .debits = prng.boolean(),
                        .credits = prng.boolean(),
                        .padding = if (prng.chance(.{ .numerator = 1, .denominator = 1000 }))
                            int_edge_biased(&prng, u29)
                        else
                            0,
                    },
                };
            },
            .lookup_accounts, .lookup_transfers => {
                const account_or_transfer_id: *u128 = @ptrCast(request_buffer_operation);
                account_or_transfer_id.* = int_edge_biased(&prng, u128);
            },

            // TODO: Only reads are supported now, due to how create_* require compaction to be
            // hooked up.
            .create_accounts, .create_transfers => continue,

            // No payload, so not very interesting yet.
            .get_events, .pulse => continue,
        }

        // This is checked in the state machine, but as of writing the fuzz, all that it cares about
        // is the length. Maybe that changes in the future and you hit this assert.
        assert(context.state_machine.input_valid(operation, request_buffer_operation));

        context.prepare(operation, request_buffer_operation);

        const reply_size = context.execute(
            op,
            operation,
            request_buffer_operation,
            @ptrCast(reply_buffer),
        );
        stdx.maybe(reply_size == 0);

        op += 1;
    }
}
