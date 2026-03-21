//! This workload generates requests, and reconciles replies with a model, tracking account
//! balances.
//!
//! After every operation, all accounts are queried, and basic invariants are checked.
//!
//! The workload and drivers communicate with a binary protocol over stdio. The protocol is based
//! on the extern structs in `src/tigerbeetle.zig` and `src/state_machine.zig`, and it works like
//! this:
//!
//! 1. Workload sends a request, which is:
//!    * the _operation_ (1 byte),
//!    * the _event count_ (4 bytes), and
//!    * the events (event count * size of event).
//! 2. The driver uses its client to submit those events. When receiving results, it sends them
//!    back on its stdout as:
//!    * the _operation_ (1 byte)
//!    * the _result count_ (4 bytes), and
//!    * the results (result count * size of result pair), where each pair holds an index and a
//!      result enum value (see `src/tigerbeetle.zig`)
//! 3. The workload receives the results, and expects them to be of the same operation type as
//!    originally requested.

const std = @import("std");
const stdx = @import("stdx");
const tb = @import("../../tigerbeetle.zig");
const Operation = tb.Operation;
const ratio = stdx.PRNG.ratio;

const log = std.log.scoped(.workload);
const assert = std.debug.assert;
const testing = std.testing;

const events_count_max = 8189;
const accounts_count_max = 128;

pub const Command = enum {
    create_accounts,
    create_transfers,
    lookup_accounts,

    pub fn operation(command: Command) Operation {
        return switch (command) {
            .create_accounts => .create_accounts,
            .create_transfers => .create_transfers,
            .lookup_accounts => .lookup_accounts,
        };
    }
};

// TODO Add padding to the protocol to avoid the misalignment.
const Request = union(Command) {
    create_accounts: []align(1) const tb.Account,
    create_transfers: []align(1) const tb.Transfer,
    lookup_accounts: []align(1) const u128,
};

const Result = union(Command) {
    create_accounts: []align(1) const tb.CreateAccountResult,
    create_transfers: []align(1) const tb.CreateTransferResult,
    lookup_accounts: []align(1) const tb.Account,
};

fn reconcile_(command: Command, request: Request, result: Result, model: *Model) !void {
    assert(command == request);
    assert(command == result);
    switch (result) {
        .create_accounts => |account_results| {
            const accounts = request.create_accounts;
            assert(account_results.len == accounts.len);

            for (
                accounts,
                account_results,
                0..,
            ) |account, account_result, index| {
                if (account_result.status == .created) {
                    model.accounts.putAssumeCapacityNoClobber(account.id, account);
                } else {
                    log.err("got status {s} for event {d}: {any}", .{
                        @tagName(account_result.status),
                        index,
                        account,
                    });
                    return error.TestFailed;
                }
            }
        },
        .create_transfers => |transfer_results| {
            const transfers = request.create_transfers;
            assert(transfer_results.len == transfers.len);

            for (transfers, transfer_results, 0..) |transfer, transfer_result, index| {
                // Check that linked transfers fail together.
                if (index > 0) {
                    const preceding_transfer = transfers[index - 1];
                    if (preceding_transfer.flags.linked) {
                        const preceding_entry = transfer_results[index - 1];
                        try testing.expect(preceding_entry.status != .created);
                    }
                }

                // No further validation needed for failed transfers.
                if (transfer_result.status != .created) {
                    continue;
                }

                const debit_account = model.accounts.getPtr(transfer.debit_account_id).?;
                const credit_account = model.accounts.getPtr(transfer.credit_account_id).?;
                debit_account.debits_posted += transfer.amount;
                credit_account.credits_posted += transfer.amount;
            }

            model.transfers_created += transfers.len;
        },
        .lookup_accounts => |accounts_found| {
            const account_ids = request.lookup_accounts;
            for (account_ids, accounts_found) |account_id, account| {
                const account_expect = model.accounts.getPtr(account_id).?;
                try testing.expectEqual(account.id, account_id);
                try testing.expectEqual(account.debits_pending, account_expect.debits_pending);
                try testing.expectEqual(account.debits_posted, account_expect.debits_posted);
                try testing.expectEqual(account.credits_pending, account_expect.credits_pending);
                try testing.expectEqual(account.credits_posted, account_expect.credits_posted);
            }
        },
    }
}

/// Tracks information about the accounts and transfers created by the workload.
pub const Model = struct {
    accounts: std.AutoArrayHashMapUnmanaged(u128, tb.Account),
    transfers_created: u64 = 0,

    pub fn init(allocator: std.mem.Allocator) !Model {
        var accounts = std.AutoArrayHashMapUnmanaged(u128, tb.Account).empty;
        errdefer accounts.deinit(allocator);

        try accounts.ensureTotalCapacity(allocator, accounts_count_max);
        return .{ .accounts = accounts };
    }

    pub fn deinit(model: *Model, allocator: std.mem.Allocator) void {
        model.accounts.deinit(allocator);
    }

    pub fn reconcile(
        model: *Model,
        command: Command,
        request: []const u8,
        result: []const u8,
    ) !void {
        try reconcile_(
            command,
            switch (command) {
                .create_accounts => .{
                    .create_accounts = std.mem.bytesAsSlice(tb.Account, request),
                },
                .create_transfers => .{
                    .create_transfers = std.mem.bytesAsSlice(tb.Transfer, request),
                },
                .lookup_accounts => .{
                    .lookup_accounts = std.mem.bytesAsSlice(u128, request),
                },
            },
            switch (command) {
                .create_accounts => .{
                    .create_accounts = std.mem.bytesAsSlice(tb.CreateAccountResult, result),
                },
                .create_transfers => .{
                    .create_transfers = std.mem.bytesAsSlice(tb.CreateTransferResult, result),
                },
                .lookup_accounts => .{
                    .lookup_accounts = std.mem.bytesAsSlice(tb.Account, result),
                },
            },
            model,
        );
    }
};

pub const Generator = struct {
    prng: stdx.PRNG,

    pub const buffer_size = events_count_max * @max(@sizeOf(tb.Account), @sizeOf(tb.Account));

    pub fn init(seed: u64) Generator {
        return .{ .prng = stdx.PRNG.from_seed(seed) };
    }

    pub fn random_command(generator: *Generator, model: *const Model) Command {
        // Mostly send create_transfers, to fill up the LSM.
        return generator.prng.enum_weighted(Command, .{
            .create_accounts = if (model.accounts.count() < accounts_count_max) 1 else 0,
            .create_transfers = if (model.accounts.count() > 2) 20 else 0,
            .lookup_accounts = if (model.accounts.count() > 0) 1 else 0,
        });
    }

    pub fn random_request(
        generator: *Generator,
        model: *const Model,
        command: Command,
        buffer: []u8,
    ) u32 {
        assert(buffer.len == buffer_size);

        return switch (command) {
            .create_accounts => generator.random_create_accounts(model, buffer),
            .create_transfers => generator.random_create_transfers(model, buffer),
            .lookup_accounts => generator.random_lookup_accounts(model, buffer),
        };
    }

    fn random_create_accounts(generator: *Generator, model: *const Model, buffer: []u8) u32 {
        const events_count =
            generator.prng.range_inclusive(usize, 1, accounts_count_max - model.accounts.count());
        assert(events_count <= events_count_max);

        const events_buffer = buffer[0..(events_count * @sizeOf(tb.Account))];
        const events = std.mem.bytesAsSlice(tb.Account, events_buffer);
        for (events) |*event| {
            event.* = std.mem.zeroInit(tb.Account, .{
                .id = generator.prng.range_inclusive(u128, 1, std.math.maxInt(u128)),
                .ledger = 1,
                .code = generator.prng.range_inclusive(u16, 1, 100),
                .flags = .{ .history = generator.prng.chance(ratio(1, 10)) },
            });
        }
        return @intCast(events_buffer.len);
    }

    fn random_create_transfers(generator: *Generator, model: *const Model, buffer: []u8) u32 {
        const events_count = generator.prng.range_inclusive(usize, 1, events_count_max);
        assert(events_count <= events_count_max);

        const events_buffer = buffer[0..(events_count * @sizeOf(tb.Transfer))];
        const events = std.mem.bytesAsSlice(tb.Transfer, events_buffer);
        for (events) |*event| {
            const debit_account_id =
                model.accounts.values()[generator.prng.index(model.accounts.values())].id;
            var credit_account_id: u128 = 0;
            while (credit_account_id == 0 or credit_account_id == debit_account_id) {
                credit_account_id =
                    model.accounts.values()[generator.prng.index(model.accounts.values())].id;
            }

            event.* = std.mem.zeroInit(tb.Transfer, .{
                .id = generator.prng.int(u128) +| 1,
                .ledger = 1,
                .debit_account_id = debit_account_id,
                .credit_account_id = credit_account_id,
                .amount = generator.prng.int_inclusive(u128, 1 << 32),
                .code = generator.prng.range_inclusive(u16, 1, 100),
            });
        }
        return @intCast(events_buffer.len);
    }

    fn random_lookup_accounts(generator: *Generator, model: *const Model, buffer: []u8) u32 {
        const events_count = @min(events_count_max, model.accounts.count());
        const events_buffer = buffer[0..(events_count * @sizeOf(u128))];
        const events = std.mem.bytesAsSlice(u128, events_buffer);
        for (events) |*event| {
            event.* = model.accounts.values()[generator.prng.index(model.accounts.values())].id;
        }
        return @intCast(events_buffer.len);
    }
};
