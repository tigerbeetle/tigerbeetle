//! The Auditor constructs the expected state of its corresponding StateMachine from requests and
//! replies. It validates replies against its local state.
//!
//! The Auditor expects replies in ascending commit order.
const std = @import("std");
const stdx = @import("stdx");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const log = std.log.scoped(.test_auditor);

const tb = @import("../tigerbeetle.zig");
const IdPermutation = @import("../testing/id.zig").IdPermutation;
const TimestampRange = @import("../lsm/timestamp_range.zig").TimestampRange;

const PriorityQueue = std.PriorityQueue;
const Storage = @import("../testing/storage.zig").Storage;
const StateMachine = @import("../state_machine.zig").StateMachineType(Storage);

pub const CreateAccountResultSet = std.enums.EnumSet(tb.CreateAccountResult);
// TODO(zig): See `Ordered` comments.
pub const CreateTransferResultSet = std.enums.EnumSet(tb.CreateTransferResult.Ordered);

/// Batch sizes apply to both `create` and `lookup` operations.
/// (More ids would fit in the `lookup` request, but then the response wouldn't fit.)
const accounts_batch_size_max = StateMachine.batch_max.create_accounts;
const transfers_batch_size_max = StateMachine.batch_max.create_transfers;

const InFlightKey = struct {
    client_index: usize,
    /// This index corresponds to Auditor.creates_sent/Auditor.creates_delivered.
    client_request: usize,
};

/// Store expected possible results for an in-flight request.
/// This reply validation takes advantage of the Workload's additional context about the request.
const InFlight = union(enum) {
    create_accounts: [accounts_batch_size_max]CreateAccountResultSet,
    create_transfers: [transfers_batch_size_max]CreateTransferResultSet,
};

const InFlightQueue = std.AutoHashMapUnmanaged(InFlightKey, InFlight);

const PendingTransfer = struct {
    amount: u128,
    debit_account_index: usize,
    credit_account_index: usize,
    query_intersection_index: usize,
};

const PendingExpiry = struct {
    transfer_id: u128,
    transfer_timestamp: u64,
    expires_at: u64,
};

const PendingExpiryQueue = PriorityQueue(PendingExpiry, void, struct {
    /// Order by ascending expiration date and then by transfer's timestamp.
    fn compare(_: void, a: PendingExpiry, b: PendingExpiry) std.math.Order {
        const order = switch (std.math.order(a.expires_at, b.expires_at)) {
            .eq => std.math.order(a.transfer_timestamp, b.transfer_timestamp),
            else => |order| order,
        };
        assert(order != .eq);
        return order;
    }
}.compare);

pub const AccountingAuditor = struct {
    pub const AccountState = struct {
        /// Set to true when `create_accounts` returns `.ok` for an account.
        created: bool = false,
        /// The number of transfers created on the debit side.
        dr_transfer_count: u32 = 0,
        /// The number of transfers created on the credit side.
        cr_transfer_count: u32 = 0,
        /// Timestamp of the first transfer recorded.
        transfer_timestamp_min: u64 = 0,
        /// Timestamp of the last transfer recorded.
        transfer_timestamp_max: u64 = 0,

        fn update(
            state: *AccountState,
            comptime entry: enum { dr, cr },
            transfer_timestamp: u64,
        ) void {
            assert(state.created);
            switch (entry) {
                .dr => state.dr_transfer_count += 1,
                .cr => state.cr_transfer_count += 1,
            }

            if (state.transfer_timestamp_min == 0) {
                assert(state.transfer_timestamp_max == 0);
                state.transfer_timestamp_min = transfer_timestamp;
            }
            state.transfer_timestamp_max = transfer_timestamp;
        }

        pub fn transfers_count(self: *const AccountState, flags: tb.AccountFilterFlags) u32 {
            var transfer_count: u32 = 0;
            if (flags.debits) {
                transfer_count += self.dr_transfer_count;
            }
            if (flags.credits) {
                transfer_count += self.cr_transfer_count;
            }
            return transfer_count;
        }
    };

    pub const Options = struct {
        accounts_max: usize,
        account_id_permutation: IdPermutation,
        client_count: usize,

        /// The maximum number of pending transfers that can be expired per pulse.
        pulse_expiries_max: u32,

        /// This is the maximum number of pending transfers, not counting those that have timed
        /// out.
        ///
        /// NOTE: Transfers that have posted/voided successfully (or not) that have _not_ yet
        /// reached their expiry are still included in this count — see `pending_expiries`.
        transfers_pending_max: usize,

        /// This is the maximum number of changes events needs to be tracked.
        changes_events_max: u32,

        /// From the Auditor's point-of-view, all stalled requests are still in-flight, even if
        /// their reply has actually arrived at the ReplySequence.
        ///
        /// A request stops being "in-flight" when `on_reply` is called.
        ///
        /// This should equal the ReplySequence's `stalled_queue_capacity`.
        in_flight_max: usize,
    };

    pub const QueryIntersection = struct {
        user_data_64: u64,
        user_data_32: u32,
        code: u16,

        accounts: QueryIntersectionState = .{},
        transfers: QueryIntersectionState = .{},
    };

    pub const QueryIntersectionState = struct {
        /// The number of objects recorded.
        count: u32 = 0,
        /// Timestamp of the first object recorded.
        timestamp_min: u64 = 0,
        /// Timestamp of the last object recorded.
        timestamp_max: u64 = 0,
    };

    pub const ChangesTracker = struct {
        const EnumArray = std.EnumArray(tb.ChangeEventType, u32);
        const Counter = struct {
            /// The number of events recorded.
            count: EnumArray,
            /// Timestamp of the first event recorded.
            timestamp_min: u64,
            /// Timestamp of the last event recorded.
            timestamp_max: u64,

            fn init() Counter {
                return .{
                    .count = EnumArray.initFill(0),
                    .timestamp_min = 0,
                    .timestamp_max = 0,
                };
            }

            pub fn count_total(self: *const Counter) u32 {
                const timestamp_valid: bool =
                    TimestampRange.valid(self.timestamp_min) and
                    TimestampRange.valid(self.timestamp_max);
                maybe(timestamp_valid);

                var total: u32 = 0;
                for (self.count.values) |value| total += value;
                assert((total > 0) == timestamp_valid);
                return total;
            }
        };

        current: Counter,
        snapshot: ?Counter,
        changes_events_max: u32,

        fn init(changes_events_max: u32) ChangesTracker {
            return .{
                .current = Counter.init(),
                .snapshot = null,
                .changes_events_max = changes_events_max,
            };
        }

        fn update(self: *ChangesTracker, change: union(enum) {
            transfer: struct {
                timestamp: u64,
                flags: tb.TransferFlags,
            },
            expiry: struct {
                timestamp: u64,
                expired_count: u32,
            },
        }) void {
            defer assert(self.current.count_total() <= self.changes_events_max);
            const count: u32 = switch (change) {
                .transfer => 1,
                .expiry => |expiry| expiry.expired_count,
            };
            assert(count > 0);
            if (self.current.count_total() + count > self.changes_events_max) {
                // Reset the counters if we reach the maximum size.
                self.current = Counter.init();
                // Too many events to keep track of.
                if (count > self.changes_events_max) return;
            }

            switch (change) {
                .transfer => |transfer| {
                    assert(TimestampRange.valid(transfer.timestamp));
                    if (self.current.timestamp_min == 0 and
                        self.current.timestamp_max == 0)
                    {
                        self.current.timestamp_min = transfer.timestamp;
                        self.current.timestamp_max = transfer.timestamp;
                    } else {
                        assert(TimestampRange.valid(self.current.timestamp_min));
                        assert(TimestampRange.valid(self.current.timestamp_max));
                        assert(self.current.timestamp_min <= self.current.timestamp_max);
                        assert(transfer.timestamp > self.current.timestamp_max);
                        self.current.timestamp_max = transfer.timestamp;
                    }

                    if (transfer.flags.pending) {
                        self.current.count.getPtr(.two_phase_pending).* += 1;
                    } else if (transfer.flags.post_pending_transfer) {
                        self.current.count.getPtr(.two_phase_posted).* += 1;
                    } else if (transfer.flags.void_pending_transfer) {
                        self.current.count.getPtr(.two_phase_voided).* += 1;
                    } else {
                        self.current.count.getPtr(.single_phase).* += 1;
                    }
                },
                .expiry => |expiry| {
                    assert(TimestampRange.valid(expiry.timestamp));
                    if (self.current.timestamp_min == 0 and
                        self.current.timestamp_max == 0)
                    {
                        const timestamp_first: u64 = expiry.timestamp - expiry.expired_count;
                        assert(TimestampRange.valid(timestamp_first));
                        self.current.timestamp_min = timestamp_first;
                        self.current.timestamp_max = expiry.timestamp;
                    } else {
                        assert(TimestampRange.valid(self.current.timestamp_min));
                        assert(TimestampRange.valid(self.current.timestamp_max));
                        assert(self.current.timestamp_min <= self.current.timestamp_max);
                        assert(expiry.timestamp > self.current.timestamp_max);
                        self.current.timestamp_max = expiry.timestamp;
                    }
                    self.current.count.getPtr(.two_phase_expired).* += expiry.expired_count;
                },
            }
        }

        pub fn acquire_snapshot(self: *ChangesTracker) ?Counter {
            // Snapshot already in use for another query.
            if (self.snapshot != null) return null;
            // No events.
            if (self.current.count_total() == 0) return null;

            assert(self.snapshot == null);
            self.snapshot = self.current;
            return self.snapshot.?;
        }

        fn release_snapshot(self: *ChangesTracker) Counter {
            assert(self.snapshot != null);
            defer self.snapshot = null;
            return self.snapshot.?;
        }
    };

    prng: *stdx.PRNG,
    options: Options,

    /// The timestamp of the last processed reply.
    timestamp: u64 = 0,

    /// The account configuration. Balances are in sync with the remote StateMachine for a
    /// given commit (double-double entry accounting).
    accounts: []tb.Account,

    /// Additional account state. Keyed by account index.
    accounts_state: []AccountState,

    /// Known intersection values for a particular combination of secondary indexes.
    /// Counters are in sync with the remote StateMachine tracking the number of objects
    /// with such fields.
    query_intersections: []QueryIntersection,

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

    /// Records the number of events in a given timestamp span.
    /// Used to validate the `get_change_events` results.
    changes_tracker: ChangesTracker,

    /// Track the expected result of the in-flight request for each client.
    /// Each member queue corresponds to entries of the client's request queue, but omits
    /// `register` messages.
    in_flight: InFlightQueue,

    /// The number of `create_accounts`/`create_transfers` sent, per client. Keyed by client index.
    creates_sent: []usize,

    /// The number of `create_accounts`/`create_transfers` delivered (i.e. replies received),
    /// per client. Keyed by client index.
    creates_delivered: []usize,

    pub fn init(
        gpa: std.mem.Allocator,
        prng: *stdx.PRNG,
        options: Options,
    ) !AccountingAuditor {
        assert(options.accounts_max >= 2);
        assert(options.client_count > 0);

        const accounts = try gpa.alloc(tb.Account, options.accounts_max);
        errdefer gpa.free(accounts);
        @memset(accounts, undefined);

        const accounts_state = try gpa.alloc(AccountState, options.accounts_max);
        errdefer gpa.free(accounts_state);
        @memset(accounts_state, AccountState{});

        // The number of known intersection values for the secondary indices is kept low enough to
        // explore different cardinalities.
        const query_intersections = try gpa.alloc(
            QueryIntersection,
            options.accounts_max / 2,
        );
        errdefer gpa.free(query_intersections);
        for (query_intersections, 1..) |*query_intersection, index| {
            query_intersection.* = .{
                .user_data_64 = @intCast(index * 1_000_000),
                .user_data_32 = @intCast(index * 1_000),
                .code = @intCast(index), // It will be used to recover the index.
            };
        }

        var pending_transfers = std.AutoHashMapUnmanaged(u128, PendingTransfer){};
        errdefer pending_transfers.deinit(gpa);
        try pending_transfers.ensureTotalCapacity(
            gpa,
            @intCast(options.transfers_pending_max),
        );

        var pending_expiries = PendingExpiryQueue.init(gpa, {});
        errdefer pending_expiries.deinit();
        try pending_expiries.ensureTotalCapacity(options.transfers_pending_max);

        var in_flight = InFlightQueue{};
        errdefer in_flight.deinit(gpa);
        try in_flight.ensureTotalCapacity(gpa, @intCast(options.in_flight_max));

        const creates_sent = try gpa.alloc(usize, options.client_count);
        errdefer gpa.free(creates_sent);
        @memset(creates_sent, 0);

        const creates_delivered = try gpa.alloc(usize, options.client_count);
        errdefer gpa.free(creates_delivered);
        @memset(creates_delivered, 0);

        return .{
            .prng = prng,
            .options = options,
            .accounts = accounts,
            .accounts_state = accounts_state,
            .query_intersections = query_intersections,
            .pending_transfers = pending_transfers,
            .pending_expiries = pending_expiries,
            .changes_tracker = ChangesTracker.init(options.changes_events_max),
            .in_flight = in_flight,
            .creates_sent = creates_sent,
            .creates_delivered = creates_delivered,
        };
    }

    pub fn deinit(self: *AccountingAuditor, gpa: std.mem.Allocator) void {
        gpa.free(self.creates_delivered);
        gpa.free(self.creates_sent);
        self.in_flight.deinit(gpa);
        self.pending_expiries.deinit();
        self.pending_transfers.deinit(gpa);
        gpa.free(self.query_intersections);
        gpa.free(self.accounts_state);
        gpa.free(self.accounts);
    }

    pub fn done(self: *const AccountingAuditor) bool {
        if (self.in_flight.count() != 0) return false;

        for (self.creates_sent, 0..) |sent, client_index| {
            if (sent != self.creates_delivered[client_index]) return false;
        }
        // Don't check pending_transfers; the workload might not have posted/voided every transfer.

        return true;
    }

    pub fn expect_create_accounts(
        self: *AccountingAuditor,
        client_index: usize,
    ) []CreateAccountResultSet {
        const result = self.in_flight.getOrPutAssumeCapacity(.{
            .client_index = client_index,
            .client_request = self.creates_sent[client_index],
        });
        assert(!result.found_existing);

        self.creates_sent[client_index] += 1;
        result.value_ptr.* = .{ .create_accounts = undefined };
        return result.value_ptr.*.create_accounts[0..];
    }

    pub fn expect_create_transfers(
        self: *AccountingAuditor,
        client_index: usize,
    ) []CreateTransferResultSet {
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
    pub fn expire_pending_transfers(self: *AccountingAuditor, timestamp: u64) void {
        assert(self.timestamp < timestamp);
        defer self.timestamp = timestamp;

        var expired_count: u32 = 0;
        while (self.pending_expiries.peek()) |expiration| {
            if (timestamp < expiration.expires_at) break;
            defer _ = self.pending_expiries.remove();

            // Ignore the transfer if it was already posted/voided.
            const pending_transfer =
                self.pending_transfers.get(expiration.transfer_id) orelse continue;
            assert(self.pending_transfers.remove(expiration.transfer_id));
            assert(self.accounts_state[pending_transfer.debit_account_index].created);
            assert(self.accounts_state[pending_transfer.credit_account_index].created);

            const dr = &self.accounts[pending_transfer.debit_account_index];
            const cr = &self.accounts[pending_transfer.credit_account_index];
            dr.debits_pending -= pending_transfer.amount;
            cr.credits_pending -= pending_transfer.amount;
            assert(!dr.debits_exceed_credits(0));
            assert(!dr.credits_exceed_debits(0));
            assert(!cr.debits_exceed_credits(0));
            assert(!cr.credits_exceed_debits(0));

            // Each expiration round can expire at most one batch of transfers.
            expired_count += 1;
            if (expired_count == self.options.pulse_expiries_max) break;
        }

        if (expired_count > 0) {
            self.changes_tracker.update(.{ .expiry = .{
                .timestamp = timestamp,
                .expired_count = expired_count,
            } });
        }
    }

    pub fn on_create_accounts(
        self: *AccountingAuditor,
        client_index: usize,
        timestamp: u64,
        accounts: []const tb.Account,
        results: []const tb.CreateAccountsResult,
    ) void {
        assert(accounts.len >= results.len);
        assert(self.timestamp < timestamp or
            // Zero-sized batches packed in a multi-batch message:
            (accounts.len == 0 and self.timestamp == timestamp));
        defer self.timestamp = timestamp;

        const results_expect = self.take_in_flight(client_index).create_accounts;
        var results_iterator = IteratorForCreateType(tb.CreateAccountsResult).init(results);
        defer assert(results_iterator.results.len == 0);

        for (accounts, 0..) |*account, i| {
            const account_timestamp = timestamp - accounts.len + i + 1;

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
                assert(!self.accounts_state[account_index].created);
                self.accounts_state[account_index].created = true;
                self.accounts[account_index] = account.*;
                self.accounts[account_index].timestamp = account_timestamp;

                const query_intersection_index = account.code - 1;
                const query_intersection = &self.query_intersections[query_intersection_index];
                assert(account.user_data_64 == query_intersection.user_data_64);
                assert(account.user_data_32 == query_intersection.user_data_32);
                assert(account.code == query_intersection.code);
                query_intersection.accounts.count += 1;
                if (query_intersection.accounts.timestamp_min == 0) {
                    query_intersection.accounts.timestamp_min = account_timestamp;
                }
                query_intersection.accounts.timestamp_max = account_timestamp;
            }

            if (account_index >= self.accounts.len) {
                assert(result_actual != .ok);
            }
        }
    }

    pub fn on_create_transfers(
        self: *AccountingAuditor,
        client_index: usize,
        timestamp: u64,
        transfers: []const tb.Transfer,
        results: []const tb.CreateTransfersResult,
    ) void {
        assert(transfers.len >= results.len);
        assert(self.timestamp < timestamp or
            // Zero-sized batches packed in a multi-batch message:
            (transfers.len == 0 and self.timestamp == timestamp));
        defer self.timestamp = timestamp;

        const results_expect = self.take_in_flight(client_index).create_transfers;
        var results_iterator = IteratorForCreateType(tb.CreateTransfersResult).init(results);
        defer assert(results_iterator.results.len == 0);

        for (transfers, 0..) |*transfer, i| {
            const transfer_timestamp = timestamp - transfers.len + i + 1;

            const result_actual = results_iterator.take(i) orelse .ok;
            if (!results_expect[i].contains(result_actual.to_ordered())) {
                log.err("on_create_transfers: transfer={} expect={} result={}", .{
                    transfer.*,
                    results_expect[i],
                    result_actual,
                });
                @panic("on_create_transfers: unexpected result");
            }

            if (result_actual != .ok) continue;
            self.changes_tracker.update(.{ .transfer = .{
                .timestamp = transfer_timestamp,
                .flags = transfer.flags,
            } });

            const query_intersection_index = transfer.code - 1;
            const query_intersection = &self.query_intersections[query_intersection_index];
            assert(transfer.user_data_64 == query_intersection.user_data_64);
            assert(transfer.user_data_32 == query_intersection.user_data_32);
            assert(transfer.code == query_intersection.code);
            query_intersection.transfers.count += 1;
            if (query_intersection.transfers.timestamp_min == 0) {
                query_intersection.transfers.timestamp_min = transfer_timestamp;
            }
            query_intersection.transfers.timestamp_max = transfer_timestamp;

            if (transfer.flags.post_pending_transfer or transfer.flags.void_pending_transfer) {
                const p = self.pending_transfers.get(transfer.pending_id).?;
                const dr_state = &self.accounts_state[p.debit_account_index];
                const cr_state = &self.accounts_state[p.credit_account_index];
                dr_state.update(.dr, transfer_timestamp);
                cr_state.update(.cr, transfer_timestamp);

                const dr = &self.accounts[p.debit_account_index];
                const cr = &self.accounts[p.credit_account_index];

                assert(self.pending_transfers.remove(transfer.pending_id));
                // The transfer may still be in `pending_expiries` — removal would be O(n),
                // so don't bother.

                dr.debits_pending -= p.amount;
                cr.credits_pending -= p.amount;
                if (transfer.flags.post_pending_transfer) {
                    const amount = @min(transfer.amount, p.amount);
                    dr.debits_posted += amount;
                    cr.credits_posted += amount;
                }

                assert(!dr.debits_exceed_credits(0));
                assert(!dr.credits_exceed_debits(0));
                assert(!cr.debits_exceed_credits(0));
                assert(!cr.credits_exceed_debits(0));
            } else {
                const dr_index = self.account_id_to_index(transfer.debit_account_id);
                const cr_index = self.account_id_to_index(transfer.credit_account_id);
                const dr_state = &self.accounts_state[dr_index];
                const cr_state = &self.accounts_state[cr_index];
                dr_state.update(.dr, transfer_timestamp);
                cr_state.update(.cr, transfer_timestamp);

                const dr = &self.accounts[dr_index];
                const cr = &self.accounts[cr_index];

                if (transfer.flags.pending) {
                    if (transfer.timeout > 0) {
                        self.pending_transfers.putAssumeCapacity(transfer.id, .{
                            .amount = transfer.amount,
                            .debit_account_index = dr_index,
                            .credit_account_index = cr_index,
                            .query_intersection_index = transfer.code - 1,
                        });
                        self.pending_expiries.add(.{
                            .transfer_id = transfer.id,
                            .transfer_timestamp = transfer_timestamp,
                            .expires_at = transfer_timestamp + transfer.timeout_ns(),
                        }) catch unreachable;
                        // PriorityQueue lacks an "unmanaged" API, so verify that the workload
                        // hasn't created more pending transfers than permitted.
                        assert(self.pending_expiries.count() <= self.options.transfers_pending_max);
                    }
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
    }

    pub fn on_lookup_accounts(
        self: *AccountingAuditor,
        client_index: usize,
        timestamp: u64,
        ids: []const u128,
        results: []const tb.Account,
    ) void {
        _ = client_index;
        assert(ids.len >= results.len);
        assert(self.timestamp <= timestamp);
        defer self.timestamp = timestamp;

        var results_iterator = IteratorForLookupType(tb.Account).init(results);
        defer assert(results_iterator.results.len == 0);

        for (ids) |account_id| {
            const account_index = self.account_id_to_index(account_id);
            const account_lookup = results_iterator.take(account_id);

            if (account_index < self.accounts.len and
                self.accounts_state[account_index].created)
            {
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
    }

    /// Most `lookup_transfers` validation is handled by Workload.
    /// (Workload has more context around transfers, so it can be much stricter.)
    pub fn on_lookup_transfers(
        self: *AccountingAuditor,
        client_index: usize,
        timestamp: u64,
        ids: []const u128,
        results: []const tb.Transfer,
    ) void {
        _ = client_index;
        assert(ids.len >= results.len);
        assert(self.timestamp <= timestamp);
        defer self.timestamp = timestamp;

        var results_iterator = IteratorForLookupType(tb.Transfer).init(results);
        defer assert(results_iterator.results.len == 0);

        for (ids) |id| {
            const result = results_iterator.take(id);
            assert(result == null or result.?.id == id);
        }
    }

    /// Returns a random account matching the given criteria.
    /// Returns null when no account matches the given criteria.
    pub fn pick_account(
        self: *const AccountingAuditor,
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
        const offset = self.prng.int_inclusive(usize, self.accounts.len - 1);
        var i: usize = 0;
        // Iterate `accounts`, starting from a random offset.
        while (i < self.accounts.len) : (i += 1) {
            const account_index = (offset + i) % self.accounts.len;
            if (match.created) |expect_created| {
                if (self.accounts_state[account_index].created) {
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

    pub fn on_get_change_events(
        self: *AccountingAuditor,
        timestamp: u64,
        filter: tb.ChangeEventsFilter,
        results: []const tb.ChangeEvent,
    ) void {
        _ = timestamp;
        const filter_valid = filter.limit != 0 and
            TimestampRange.valid(filter.timestamp_min) and
            TimestampRange.valid(filter.timestamp_max) and
            stdx.zeroed(&filter.reserved) and
            (filter.timestamp_max == 0 or filter.timestamp_min <= filter.timestamp_max);
        if (!filter_valid) {
            assert(results.len == 0);
            return;
        }

        const snapshot = self.changes_tracker.release_snapshot();
        assert(filter.limit >= snapshot.count_total());
        assert(filter.timestamp_min == snapshot.timestamp_min);
        assert(filter.timestamp_max == snapshot.timestamp_max);
        assert(results.len == snapshot.count_total());

        var timestamp_previous: u64 = 0;
        var count = ChangesTracker.EnumArray.initFill(0);
        for (results) |*result| {
            assert(result.timestamp > timestamp_previous);
            timestamp_previous = result.timestamp;

            if (filter.timestamp_min > 0) {
                assert(result.timestamp >= filter.timestamp_min);
            }
            if (filter.timestamp_max > 0) {
                assert(result.timestamp <= filter.timestamp_max);
            }

            count.getPtr(result.type).* += 1;
        }

        var iterator = count.iterator();
        while (iterator.next()) |kv| {
            const expected = snapshot.count.getPtrConst(kv.key).*;
            assert(kv.value.* == expected);
        }
    }

    pub fn account_id_to_index(self: *const AccountingAuditor, id: u128) usize {
        // -1 because id=0 is not valid, so index=0→id=1.
        return @as(usize, @intCast(self.options.account_id_permutation.decode(id))) - 1;
    }

    pub fn account_index_to_id(self: *const AccountingAuditor, index: usize) u128 {
        // +1 so that index=0 is encoded as a valid id.
        return self.options.account_id_permutation.encode(index + 1);
    }

    pub fn get_account(self: *const AccountingAuditor, id: u128) ?*const tb.Account {
        const index = self.account_id_to_index(id);
        return if (index < self.accounts.len) &self.accounts[index] else null;
    }

    pub fn get_account_state(self: *const AccountingAuditor, id: u128) ?*const AccountState {
        const index = self.account_id_to_index(id);
        return if (index < self.accounts_state.len) &self.accounts_state[index] else null;
    }

    fn take_in_flight(self: *AccountingAuditor, client_index: usize) InFlight {
        const key: InFlightKey = .{
            .client_index = client_index,
            .client_request = self.creates_delivered[client_index],
        };
        self.creates_delivered[client_index] += 1;

        const in_flight = self.in_flight.get(key).?;
        assert(self.in_flight.remove(key));
        return in_flight;
    }
};

pub fn IteratorForCreateType(comptime Result: type) type {
    assert(Result == tb.CreateAccountsResult or Result == tb.CreateTransfersResult);

    return struct {
        const IteratorForCreate = @This();

        results: []const Result,

        pub fn init(results: []const Result) IteratorForCreate {
            return .{ .results = results };
        }

        pub fn take(
            self: *IteratorForCreate,
            event_index: usize,
        ) ?@FieldType(Result, "result") {
            if (self.results.len > 0 and self.results[0].index == event_index) {
                defer self.results = self.results[1..];
                return self.results[0].result;
            } else {
                return null;
            }
        }
    };
}

pub fn IteratorForLookupType(comptime Result: type) type {
    assert(Result == tb.Account or Result == tb.Transfer);

    return struct {
        const IteratorForLookup = @This();

        results: []const Result,

        pub fn init(results: []const Result) IteratorForLookup {
            return .{ .results = results };
        }

        pub fn take(self: *IteratorForLookup, id: u128) ?*const Result {
            if (self.results.len > 0 and self.results[0].id == id) {
                defer self.results = self.results[1..];
                return &self.results[0];
            } else {
                return null;
            }
        }
    };
}
