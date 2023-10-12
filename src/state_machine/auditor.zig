//! The Auditor constructs the expected state of its corresponding StateMachine from requests and
//! replies. It validates replies against its local state.
//!
//! The Auditor expects replies in ascending commit order.
const std = @import("std");
const stdx = @import("../stdx.zig");
const assert = std.debug.assert;
const log = std.log.scoped(.test_auditor);

const constants = @import("../constants.zig");
const tb = @import("../tigerbeetle.zig");
const vsr = @import("../vsr.zig");
const IdPermutation = @import("../testing/id.zig").IdPermutation;

const PriorityQueue = std.PriorityQueue;
const Storage = @import("../testing/storage.zig").Storage;
const StateMachine = @import("../state_machine.zig").StateMachineType(Storage, constants.state_machine_config);

pub const CreateAccountResultSet = std.enums.EnumSet(tb.CreateAccountResult);
pub const CreateTransferResultSet = std.enums.EnumSet(tb.CreateTransferResult);

/// Batch sizes apply to both `create` and `lookup` operations.
/// (More ids would fit in the `lookup` request, but then the response wouldn't fit.)
const accounts_batch_size_max = StateMachine.constants.batch_max.create_accounts;
const transfers_batch_size_max = StateMachine.constants.batch_max.create_transfers;

/// Store expected possible results for an in-flight request.
/// This reply validation takes advantage of the Workload's additional context about the request.
const InFlight = union(enum) {
    create_accounts: [accounts_batch_size_max]CreateAccountResultSet,
    create_transfers: [transfers_batch_size_max]CreateTransferResultSet,
};

const InFlightQueue = std.AutoHashMapUnmanaged(struct {
    client_index: usize,
    /// This index corresponds to Auditor.creates_sent/Auditor.creates_delivered.
    client_request: usize,
}, InFlight);

const PendingTransfer = struct {
    amount: u128,
    debit_account_index: usize,
    credit_account_index: usize,
};

const PendingExpiry = struct {
    transfer: u128,
    timestamp: u64,
};

const PendingExpiryQueue = PriorityQueue(PendingExpiry, void, struct {
    /// Order by ascending timestamp.
    fn compare(_: void, a: PendingExpiry, b: PendingExpiry) std.math.Order {
        return std.math.order(a.timestamp, b.timestamp);
    }
}.compare);

pub const AccountingAuditor = struct {
    const Self = @This();

    pub const Options = struct {
        accounts_max: usize,
        account_id_permutation: IdPermutation,
        client_count: usize,

        /// This is the maximum number of pending transfers, not counting those that have timed
        /// out.
        ///
        /// NOTE: Transfers that have posted/voided successfully (or not) that have _not_ yet
        /// reached their expiry are still included in this count — see `pending_expiries`.
        transfers_pending_max: usize,

        /// From the Auditor's point-of-view, all stalled requests are still in-flight, even if
        /// their reply has actually arrived at the ReplySequence.
        ///
        /// A request stops being "in-flight" when `on_reply` is called.
        ///
        /// This should equal the ReplySequence's `stalled_queue_capacity`.
        in_flight_max: usize,
    };

    random: std.rand.Random,
    options: Options,

    /// The timestamp of the last processed reply.
    timestamp: u64 = 0,

    /// The account configuration. Balances are in sync with the remote StateMachine for a
    /// given commit (double-double entry accounting).
    accounts: []tb.Account,

    /// Set to true when `create_accounts` returns `.ok` for an account.
    accounts_created: []bool,

    /// Map pending transfers to the (pending) amount and accounts.
    ///
    /// * Added in `on_create_transfers` for pending transfers.
    /// * Removed after a transfer is posted, voided, or timed out.
    ///
    /// All entries in `pending_transfers` have a corresponding entry in `pending_expiries`.
    pending_transfers: std.AutoHashMapUnmanaged(u128, PendingTransfer),

    /// After a transfer is posted/voided, the entry in `pending_expiries` is untouched.
    /// The timeout will not impact account balances (because the `pending_transfers` entry is
    /// removed), but until timeout the transfer still counts against `transfers_pending_max`.
    pending_expiries: PendingExpiryQueue,

    /// Track the expected result of the in-flight request for each client.
    /// Each member queue corresponds to entries of the client's request queue, but omits
    /// `register` messages.
    in_flight: InFlightQueue,

    /// The number of `create_accounts`/`create_transfers` sent, per client. Keyed by client index.
    creates_sent: []usize,

    /// The number of `create_accounts`/`create_transfers` delivered (i.e. replies received),
    /// per client. Keyed by client index.
    creates_delivered: []usize,

    pub fn init(allocator: std.mem.Allocator, random: std.rand.Random, options: Options) !Self {
        assert(options.accounts_max >= 2);
        assert(options.client_count > 0);

        const accounts = try allocator.alloc(tb.Account, options.accounts_max);
        errdefer allocator.free(accounts);
        @memset(accounts, undefined);

        const accounts_created = try allocator.alloc(bool, options.accounts_max);
        errdefer allocator.free(accounts_created);
        @memset(accounts_created, false);

        var pending_transfers = std.AutoHashMapUnmanaged(u128, PendingTransfer){};
        errdefer pending_transfers.deinit(allocator);
        try pending_transfers.ensureTotalCapacity(allocator, @as(u32, @intCast(options.transfers_pending_max)));

        var pending_expiries = PendingExpiryQueue.init(allocator, {});
        errdefer pending_expiries.deinit();
        try pending_expiries.ensureTotalCapacity(options.transfers_pending_max);

        var in_flight = InFlightQueue{};
        errdefer in_flight.deinit(allocator);
        try in_flight.ensureTotalCapacity(allocator, @as(u32, @intCast(options.in_flight_max)));

        var creates_sent = try allocator.alloc(usize, options.client_count);
        errdefer allocator.free(creates_sent);
        @memset(creates_sent, 0);

        var creates_delivered = try allocator.alloc(usize, options.client_count);
        errdefer allocator.free(creates_delivered);
        @memset(creates_delivered, 0);

        return Self{
            .random = random,
            .options = options,
            .accounts = accounts,
            .accounts_created = accounts_created,
            .pending_transfers = pending_transfers,
            .pending_expiries = pending_expiries,
            .in_flight = in_flight,
            .creates_sent = creates_sent,
            .creates_delivered = creates_delivered,
        };
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        allocator.free(self.accounts);
        allocator.free(self.accounts_created);
        self.pending_transfers.deinit(allocator);
        self.pending_expiries.deinit();
        self.in_flight.deinit(allocator);
        allocator.free(self.creates_sent);
        allocator.free(self.creates_delivered);
    }

    pub fn done(self: *const Self) bool {
        if (self.in_flight.count() != 0) return false;

        for (self.creates_sent, 0..) |sent, client_index| {
            if (sent != self.creates_delivered[client_index]) return false;
        }
        // Don't check pending_transfers; the workload might not have posted/voided every transfer.

        return true;
    }

    pub fn expect_create_accounts(self: *Self, client_index: usize) []CreateAccountResultSet {
        const result = self.in_flight.getOrPutAssumeCapacity(.{
            .client_index = client_index,
            .client_request = self.creates_sent[client_index],
        });
        assert(!result.found_existing);

        self.creates_sent[client_index] += 1;
        result.value_ptr.* = .{ .create_accounts = undefined };
        return result.value_ptr.*.create_accounts[0..];
    }

    pub fn expect_create_transfers(self: *Self, client_index: usize) []CreateTransferResultSet {
        const result = self.in_flight.getOrPutAssumeCapacity(.{
            .client_index = client_index,
            .client_request = self.creates_sent[client_index],
        });
        assert(!result.found_existing);

        self.creates_sent[client_index] += 1;
        result.value_ptr.* = .{ .create_transfers = undefined };
        return result.value_ptr.*.create_transfers[0..];
    }

    /// Expire pending transfers that have not been posted or voided.
    fn tick_to_timestamp(self: *Self, timestamp: u64) void {
        assert(self.timestamp < timestamp);

        while (self.pending_expiries.peek()) |expiration| {
            if (timestamp < expiration.timestamp) break;
            defer _ = self.pending_expiries.remove();

            // Ignore the transfer if it was already posted/voided.
            const pending_transfer = self.pending_transfers.get(expiration.transfer) orelse continue;
            assert(self.pending_transfers.remove(expiration.transfer));
            assert(self.accounts_created[pending_transfer.debit_account_index]);
            assert(self.accounts_created[pending_transfer.credit_account_index]);

            const dr = &self.accounts[pending_transfer.debit_account_index];
            const cr = &self.accounts[pending_transfer.credit_account_index];
            dr.debits_pending -= pending_transfer.amount;
            cr.credits_pending -= pending_transfer.amount;

            assert(!dr.debits_exceed_credits(0));
            assert(!dr.credits_exceed_debits(0));
            assert(!cr.debits_exceed_credits(0));
            assert(!cr.credits_exceed_debits(0));

            // TODO(Timeouts): When timeouts are implemented in the StateMachine, remove this unimplemented.
            stdx.unimplemented("timeouts");
        }

        self.timestamp = timestamp;
    }

    pub fn on_create_accounts(
        self: *Self,
        client_index: usize,
        timestamp: u64,
        accounts: []const tb.Account,
        results: []const tb.CreateAccountsResult,
    ) void {
        assert(accounts.len >= results.len);
        assert(self.timestamp < timestamp);
        defer assert(self.timestamp == timestamp);

        const results_expect = self.take_in_flight(client_index).create_accounts;
        var results_iterator = IteratorForCreate(tb.CreateAccountsResult).init(results);
        defer assert(results_iterator.results.len == 0);

        for (accounts, 0..) |*account, i| {
            const account_timestamp = timestamp - accounts.len + i + 1;
            // TODO Should this be at the end of the loop? (If a timeout & post land on the same
            // timestamp, which wins?)
            self.tick_to_timestamp(account_timestamp);

            const result_actual = results_iterator.take(i) orelse .ok;
            if (!results_expect[i].contains(result_actual)) {
                log.err("on_create_accounts: account={} expect={} result={}", .{
                    account.*,
                    results_expect[i],
                    result_actual,
                });
                @panic("on_create_accounts: unexpected result");
            }

            const account_index = self.account_id_to_index(account.id);
            if (result_actual == .ok) {
                assert(!self.accounts_created[account_index]);
                self.accounts_created[account_index] = true;
                self.accounts[account_index] = account.*;
                self.accounts[account_index].timestamp = account_timestamp;
            }

            if (account_index >= self.accounts.len) {
                assert(result_actual != .ok);
            }
        }

        if (accounts.len == 0) {
            self.tick_to_timestamp(timestamp);
        }
    }

    pub fn on_create_transfers(
        self: *Self,
        client_index: usize,
        timestamp: u64,
        transfers: []const tb.Transfer,
        results: []const tb.CreateTransfersResult,
    ) void {
        assert(transfers.len >= results.len);
        assert(self.timestamp < timestamp);
        defer assert(self.timestamp == timestamp);

        const results_expect = self.take_in_flight(client_index).create_transfers;
        var results_iterator = IteratorForCreate(tb.CreateTransfersResult).init(results);
        defer assert(results_iterator.results.len == 0);

        for (transfers, 0..) |*transfer, i| {
            const transfer_timestamp = timestamp - transfers.len + i + 1;
            // TODO Should this be deferrred to the end of the loop? (If a timeout & post land on
            // the same timestamp, which wins?)
            self.tick_to_timestamp(transfer_timestamp);

            const result_actual = results_iterator.take(i) orelse .ok;
            if (!results_expect[i].contains(result_actual)) {
                log.err("on_create_transfers: transfer={} expect={} result={}", .{
                    transfer.*,
                    results_expect[i],
                    result_actual,
                });
                @panic("on_create_transfers: unexpected result");
            }

            if (result_actual != .ok) continue;

            if (transfer.flags.post_pending_transfer or transfer.flags.void_pending_transfer) {
                if (self.pending_transfers.get(transfer.pending_id)) |p| {
                    const dr = &self.accounts[p.debit_account_index];
                    const cr = &self.accounts[p.credit_account_index];
                    assert(self.accounts_created[p.debit_account_index]);
                    assert(self.accounts_created[p.credit_account_index]);

                    assert(self.pending_transfers.remove(transfer.pending_id));
                    // The transfer may still be in `pending_expiries` — removal would be O(n),
                    // so don't bother.

                    dr.debits_pending -= p.amount;
                    cr.credits_pending -= p.amount;
                    if (transfer.flags.post_pending_transfer) {
                        const amount = if (transfer.amount > 0) transfer.amount else p.amount;
                        dr.debits_posted += amount;
                        cr.credits_posted += amount;
                    }

                    assert(!dr.debits_exceed_credits(0));
                    assert(!dr.credits_exceed_debits(0));
                    assert(!cr.debits_exceed_credits(0));
                    assert(!cr.credits_exceed_debits(0));
                } else {
                    // The transfer was already completed by another post/void or timeout.
                }
            } else {
                const dr_index = self.account_id_to_index(transfer.debit_account_id);
                const cr_index = self.account_id_to_index(transfer.credit_account_id);
                const dr = &self.accounts[dr_index];
                const cr = &self.accounts[cr_index];
                assert(self.accounts_created[dr_index]);
                assert(self.accounts_created[cr_index]);

                if (transfer.flags.pending) {
                    self.pending_transfers.putAssumeCapacity(transfer.id, .{
                        .amount = transfer.amount,
                        .debit_account_index = dr_index,
                        .credit_account_index = cr_index,
                    });
                    self.pending_expiries.add(.{
                        .transfer = transfer.id,
                        .timestamp = transfer_timestamp +
                            @as(u64, transfer.timeout) * std.time.ns_per_s,
                    }) catch unreachable;
                    // PriorityQueue lacks an "unmanaged" API, so verify that the workload hasn't
                    // created more pending transfers than permitted.
                    assert(self.pending_expiries.len <= self.options.transfers_pending_max);

                    dr.debits_pending += transfer.amount;
                    cr.credits_pending += transfer.amount;
                } else {
                    dr.debits_posted += transfer.amount;
                    cr.credits_posted += transfer.amount;
                }

                assert(!dr.debits_exceed_credits(0));
                assert(!dr.credits_exceed_debits(0));
                assert(!cr.debits_exceed_credits(0));
                assert(!cr.credits_exceed_debits(0));
            }
        }

        if (transfers.len == 0) {
            self.tick_to_timestamp(timestamp);
        }
    }

    pub fn on_lookup_accounts(
        self: *Self,
        client_index: usize,
        timestamp: u64,
        ids: []const u128,
        results: []const tb.Account,
    ) void {
        _ = client_index;
        assert(ids.len >= results.len);
        assert(self.timestamp < timestamp);
        defer assert(self.timestamp == timestamp);

        var results_iterator = IteratorForLookup(tb.Account).init(results);
        defer assert(results_iterator.results.len == 0);

        for (ids) |account_id| {
            const account_index = self.account_id_to_index(account_id);
            const account_lookup = results_iterator.take(account_id);

            if (account_index < self.accounts.len and self.accounts_created[account_index]) {
                // If this assertion fails, `lookup_accounts` didn't return an account when it
                // should have.
                assert(account_lookup != null);
                assert(!account_lookup.?.debits_exceed_credits(0));
                assert(!account_lookup.?.credits_exceed_debits(0));

                const account_expect = &self.accounts[account_index];
                if (!std.mem.eql(
                    u8,
                    std.mem.asBytes(account_lookup.?),
                    std.mem.asBytes(account_expect),
                )) {
                    log.err("on_lookup_accounts: account data mismatch " ++
                        "account_id={} expect={} lookup={}", .{
                        account_id,
                        account_expect,
                        account_lookup.?,
                    });
                    @panic("on_lookup_accounts: account data mismatch");
                }
            } else {
                // If this assertion fails, `lookup_accounts` returned an account when it shouldn't.
                assert(account_lookup == null);
            }
        }
        self.tick_to_timestamp(timestamp);
    }

    /// Most `lookup_transfers` validation is handled by Workload.
    /// (Workload has more context around transfers, so it can be much stricter.)
    pub fn on_lookup_transfers(
        self: *Self,
        client_index: usize,
        timestamp: u64,
        ids: []const u128,
        results: []const tb.Transfer,
    ) void {
        _ = client_index;
        assert(ids.len >= results.len);
        assert(self.timestamp < timestamp);
        defer assert(self.timestamp == timestamp);

        var results_iterator = IteratorForLookup(tb.Transfer).init(results);
        defer assert(results_iterator.results.len == 0);

        for (ids) |id| {
            const result = results_iterator.take(id);
            assert(result == null or result.?.id == id);
        }
        self.tick_to_timestamp(timestamp);
    }

    /// Returns a random account matching the given criteria.
    /// Returns null when no account matches the given criteria.
    pub fn pick_account(
        self: *const Self,
        match: struct {
            /// Whether the account is known to be created
            /// (we have received an `ok` for the respective `create_accounts`).
            created: ?bool,
            debits_must_not_exceed_credits: ?bool,
            credits_must_not_exceed_debits: ?bool,
            /// Don't match this account.
            exclude: ?u128 = null,
        },
    ) ?*const tb.Account {
        const offset = self.random.uintLessThanBiased(usize, self.accounts.len);
        var i: usize = 0;
        // Iterate `accounts`, starting from a random offset.
        while (i < self.accounts.len) : (i += 1) {
            const account_index = (offset + i) % self.accounts.len;
            if (match.created) |expect_created| {
                if (self.accounts_created[account_index]) {
                    if (!expect_created) continue;
                } else {
                    if (expect_created) continue;
                }
            }

            const account = &self.accounts[account_index];
            if (match.debits_must_not_exceed_credits) |b| {
                if (account.flags.debits_must_not_exceed_credits != b) continue;
            }

            if (match.credits_must_not_exceed_debits) |b| {
                if (account.flags.credits_must_not_exceed_debits != b) continue;
            }

            if (match.exclude) |exclude_id| {
                if (account.id == exclude_id) continue;
            }
            return account;
        }
        return null;
    }

    pub fn account_id_to_index(self: *const Self, id: u128) usize {
        // -1 because id=0 is not valid, so index=0→id=1.
        return @as(usize, @intCast(self.options.account_id_permutation.decode(id))) - 1;
    }

    pub fn account_index_to_id(self: *const Self, index: usize) u128 {
        // +1 so that index=0 is encoded as a valid id.
        return self.options.account_id_permutation.encode(index + 1);
    }

    fn take_in_flight(self: *Self, client_index: usize) InFlight {
        const key = .{
            .client_index = client_index,
            .client_request = self.creates_delivered[client_index],
        };
        self.creates_delivered[client_index] += 1;

        const in_flight = self.in_flight.get(key).?;
        assert(self.in_flight.remove(key));
        return in_flight;
    }
};

pub fn IteratorForCreate(comptime Result: type) type {
    assert(Result == tb.CreateAccountsResult or Result == tb.CreateTransfersResult);

    return struct {
        const Self = @This();

        results: []const Result,

        pub fn init(results: []const Result) Self {
            return .{ .results = results };
        }

        pub fn take(self: *Self, event_index: usize) ?std.meta.fieldInfo(Result, .result).type {
            if (self.results.len > 0 and self.results[0].index == event_index) {
                defer self.results = self.results[1..];
                return self.results[0].result;
            } else {
                return null;
            }
        }
    };
}

pub fn IteratorForLookup(comptime Result: type) type {
    assert(Result == tb.Account or Result == tb.Transfer);

    return struct {
        const Self = @This();

        results: []const Result,

        pub fn init(results: []const Result) Self {
            return .{ .results = results };
        }

        pub fn take(self: *Self, id: u128) ?*const Result {
            if (self.results.len > 0 and self.results[0].id == id) {
                defer self.results = self.results[1..];
                return &self.results[0];
            } else {
                return null;
            }
        }
    };
}
