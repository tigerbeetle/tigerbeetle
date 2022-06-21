const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;
const log = std.log.scoped(.state_machine);

const tb = @import("tigerbeetle.zig");

const Account = tb.Account;
const AccountFlags = tb.AccountFlags;

const Transfer = tb.Transfer;
const TransferFlags = tb.TransferFlags;

const CreateAccountsResult = tb.CreateAccountsResult;
const CreateTransfersResult = tb.CreateTransfersResult;

const CreateAccountResult = tb.CreateAccountResult;
const CreateTransferResult = tb.CreateTransferResult;
const LookupAccountResult = tb.LookupAccountResult;

const HashMapAccounts = std.AutoHashMap(u128, Account);
const HashMapTransfers = std.AutoHashMap(u128, Transfer);
const HashMapPosted = std.AutoHashMap(u128, bool);

pub const StateMachine = struct {
    pub const Operation = enum(u8) {
        /// Operations reserved by VR protocol (for all state machines):
        reserved,
        root,
        register,

        /// Operations exported by TigerBeetle:
        create_accounts,
        create_transfers,
        lookup_accounts,
        lookup_transfers,
    };

    allocator: mem.Allocator,
    prepare_timestamp: u64,
    commit_timestamp: u64,
    accounts: HashMapAccounts,
    transfers: HashMapTransfers,
    posted: HashMapPosted,

    pub fn init(
        allocator: mem.Allocator,
        accounts_max: usize,
        transfers_max: usize,
        transfers_pending_max: usize,
    ) !StateMachine {
        var accounts = HashMapAccounts.init(allocator);
        errdefer accounts.deinit();
        try accounts.ensureTotalCapacity(@intCast(u32, accounts_max));

        var transfers = HashMapTransfers.init(allocator);
        errdefer transfers.deinit();
        try transfers.ensureTotalCapacity(@intCast(u32, transfers_max));

        var posted = HashMapPosted.init(allocator);
        errdefer posted.deinit();
        try posted.ensureTotalCapacity(@intCast(u32, transfers_pending_max));

        return StateMachine{
            .allocator = allocator,
            .prepare_timestamp = 0,
            .commit_timestamp = 0,
            .accounts = accounts,
            .transfers = transfers,
            .posted = posted,
        };
    }

    pub fn deinit(self: *StateMachine) void {
        self.accounts.deinit();
        self.transfers.deinit();
        self.posted.deinit();
    }

    pub fn Event(comptime operation: Operation) type {
        return switch (operation) {
            .create_accounts => Account,
            .create_transfers => Transfer,
            .lookup_accounts => u128,
            .lookup_transfers => u128,
            else => unreachable,
        };
    }

    pub fn Result(comptime operation: Operation) type {
        return switch (operation) {
            .create_accounts => CreateAccountsResult,
            .create_transfers => CreateTransfersResult,
            .lookup_accounts => Account,
            .lookup_transfers => Transfer,
            else => unreachable,
        };
    }

    /// Returns the header's timestamp.
    pub fn prepare(self: *StateMachine, operation: Operation, input: []u8) u64 {
        switch (operation) {
            .root => unreachable,
            .register => {},
            .create_accounts => self.prepare_timestamps(.create_accounts, input),
            .create_transfers => self.prepare_timestamps(.create_transfers, input),
            .lookup_accounts => {},
            .lookup_transfers => {},
            else => unreachable,
        }
        return self.prepare_timestamp;
    }

    fn prepare_timestamps(
        self: *StateMachine,
        comptime operation: Operation,
        input: []u8,
    ) void {
        var sum_reserved_timestamps: usize = 0;
        var events = mem.bytesAsSlice(Event(operation), input);
        for (events) |*event| {
            sum_reserved_timestamps += event.timestamp;
            self.prepare_timestamp += 1;
            event.timestamp = self.prepare_timestamp;
        }
        // The client is responsible for ensuring that timestamps are reserved:
        // Use a single branch condition to detect non-zero reserved timestamps.
        // Summing then branching once is faster than branching every iteration of the loop.
        assert(sum_reserved_timestamps == 0);
    }

    pub fn commit(
        self: *StateMachine,
        client: u128,
        operation: Operation,
        input: []const u8,
        output: []u8,
    ) usize {
        _ = client;
        return switch (operation) {
            .root => unreachable,
            .register => 0,
            .create_accounts => self.execute(.create_accounts, input, output),
            .create_transfers => self.execute(.create_transfers, input, output),
            .lookup_accounts => self.execute_lookup_accounts(input, output),
            .lookup_transfers => self.execute_lookup_transfers(input, output),
            else => unreachable,
        };
    }

    fn execute(
        self: *StateMachine,
        comptime operation: Operation,
        input: []const u8,
        output: []u8,
    ) usize {
        comptime assert(operation != .lookup_accounts and operation != .lookup_transfers);

        const events = mem.bytesAsSlice(Event(operation), input);
        var results = mem.bytesAsSlice(Result(operation), output);
        var count: usize = 0;

        var chain: ?usize = null;
        var chain_broken = false;

        for (events) |*event, index| {
            if (event.flags.linked and chain == null) {
                chain = index;
                assert(chain_broken == false);
            }
            const result = if (chain_broken) .linked_event_failed else switch (operation) {
                .create_accounts => self.create_account(event),
                .create_transfers => self.create_transfer(event),
                else => unreachable,
            };
            log.debug("{s} {}/{}: {}: {}", .{
                @tagName(operation),
                index + 1,
                events.len,
                result,
                event,
            });
            if (result != .ok) {
                if (chain) |chain_start_index| {
                    if (!chain_broken) {
                        chain_broken = true;
                        // Rollback events in LIFO order, excluding this event that broke the chain:
                        self.rollback(operation, input, chain_start_index, index);
                        // Add errors for rolled back events in FIFO order:
                        var chain_index = chain_start_index;
                        while (chain_index < index) : (chain_index += 1) {
                            results[count] = .{
                                .index = @intCast(u32, chain_index),
                                .result = .linked_event_failed,
                            };
                            count += 1;
                        }
                    } else {
                        assert(result == .linked_event_failed);
                    }
                }
                results[count] = .{ .index = @intCast(u32, index), .result = result };
                count += 1;
            }
            if (!event.flags.linked and chain != null) {
                chain = null;
                chain_broken = false;
            }
        }
        // TODO client.zig: Validate that batch chains are always well-formed and closed.
        // This is programming error and we should raise an exception for this in the client ASAP.
        assert(chain == null);
        assert(chain_broken == false);

        return @sizeOf(Result(operation)) * count;
    }

    fn rollback(
        self: *StateMachine,
        comptime operation: Operation,
        input: []const u8,
        chain_start_index: usize,
        chain_error_index: usize,
    ) void {
        const events = mem.bytesAsSlice(Event(operation), input);

        // We commit events in FIFO order.
        // We must therefore rollback events in LIFO order with a reverse loop.
        // We do not rollback `self.commit_timestamp` to ensure that subsequent events are
        // timestamped correctly.
        var index = chain_error_index;
        while (index > chain_start_index) {
            index -= 1;

            assert(index >= chain_start_index);
            assert(index < chain_error_index);
            const event = events[index];
            assert(event.timestamp <= self.commit_timestamp);

            switch (operation) {
                .create_accounts => self.create_account_rollback(&event),
                .create_transfers => self.create_transfer_rollback(&event),
                else => unreachable,
            }
            log.debug("{s} {}/{}: rollback(): {}", .{
                @tagName(operation),
                index + 1,
                events.len,
                event,
            });
        }
        assert(index == chain_start_index);
    }

    fn execute_lookup_accounts(self: *StateMachine, input: []const u8, output: []u8) usize {
        const batch = mem.bytesAsSlice(u128, input);
        const output_len = @divFloor(output.len, @sizeOf(Account)) * @sizeOf(Account);
        const results = mem.bytesAsSlice(Account, output[0..output_len]);
        var results_count: usize = 0;
        for (batch) |id| {
            if (self.get_account(id)) |result| {
                results[results_count] = result.*;
                results_count += 1;
            }
        }
        return results_count * @sizeOf(Account);
    }

    fn execute_lookup_transfers(self: *StateMachine, input: []const u8, output: []u8) usize {
        const batch = mem.bytesAsSlice(u128, input);
        const output_len = @divFloor(output.len, @sizeOf(Transfer)) * @sizeOf(Transfer);
        const results = mem.bytesAsSlice(Transfer, output[0..output_len]);
        var results_count: usize = 0;
        for (batch) |id| {
            if (self.get_transfer(id)) |result| {
                results[results_count] = result.*;
                results_count += 1;
            }
        }
        return results_count * @sizeOf(Transfer);
    }

    fn create_account(self: *StateMachine, a: *const Account) CreateAccountResult {
        assert(a.timestamp > self.commit_timestamp);

        if (a.flags.padding != 0) return .reserved_flag;
        if (!zeroed_48_bytes(a.reserved)) return .reserved_field;

        if (a.id == 0) return .id_must_not_be_zero;
        if (a.ledger == 0) return .ledger_must_not_be_zero;
        if (a.code == 0) return .code_must_not_be_zero;

        if (a.flags.debits_must_not_exceed_credits and a.flags.credits_must_not_exceed_debits) {
            return .mutually_exclusive_flags;
        }

        if (sum_overflows(a.debits_pending, a.debits_posted)) return .overflows_debits;
        if (sum_overflows(a.credits_pending, a.credits_posted)) return .overflows_credits;

        // Opening balances may never exceed limits:
        if (a.debits_exceed_credits(0)) return .exceeds_credits;
        if (a.credits_exceed_debits(0)) return .exceeds_debits;

        if (self.get_account(a.id)) |e| return create_account_exists(a, e);

        self.accounts.putAssumeCapacityNoClobber(a.id, a.*);

        self.commit_timestamp = a.timestamp;
        return .ok;
    }

    fn create_account_rollback(self: *StateMachine, a: *const Account) void {
        assert(self.accounts.remove(a.id));
    }

    fn create_account_exists(a: *const Account, e: *const Account) CreateAccountResult {
        assert(a.id == e.id);
        if (@bitCast(u16, a.flags) != @bitCast(u16, e.flags)) return .exists_with_different_flags;
        if (a.user_data != e.user_data) return .exists_with_different_user_data;
        assert(zeroed_48_bytes(a.reserved) and zeroed_48_bytes(e.reserved));
        if (a.ledger != e.ledger) return .exists_with_different_ledger;
        if (a.code != e.code) return .exists_with_different_code;
        if (a.debits_pending != e.debits_pending) return .exists_with_different_debits_pending;
        if (a.debits_posted != e.debits_posted) return .exists_with_different_debits_posted;
        if (a.credits_pending != e.credits_pending) return .exists_with_different_credits_pending;
        if (a.credits_posted != e.credits_posted) return .exists_with_different_credits_posted;
        return .exists;
    }

    fn create_transfer(self: *StateMachine, t: *const Transfer) CreateTransferResult {
        assert(t.timestamp > self.commit_timestamp);

        if (t.flags.padding != 0) return .reserved_flag;
        if (t.reserved != 0) return .reserved_field;

        if (t.id == 0) return .id_must_not_be_zero;

        if (t.flags.post_pending_transfer or t.flags.void_pending_transfer) {
            return self.post_or_void_pending_transfer(t);
        }

        if (t.debit_account_id == 0) return .debit_account_id_must_not_be_zero;
        if (t.credit_account_id == 0) return .credit_account_id_must_not_be_zero;
        if (t.credit_account_id == t.debit_account_id) return .accounts_must_be_different;

        if (t.pending_id != 0) return .pending_id_must_be_zero;
        if (t.flags.pending) {
            // Otherwise, reserved amounts may never be released.
            if (t.timeout == 0) return .pending_transfer_must_timeout;
        } else {
            if (t.timeout != 0) return .timeout_reserved_for_pending_transfer;
        }

        if (t.ledger == 0) return .ledger_must_not_be_zero;
        if (t.code == 0) return .code_must_not_be_zero;
        if (t.amount == 0) return .amount_must_not_be_zero;

        // The etymology of the DR and CR abbreviations for debit/credit is interesting, either:
        // 1. derived from the Latin past participles of debitum/creditum, i.e. debere/credere,
        // 2. standing for debit record and credit record, or
        // 3. relating to debtor and creditor.
        // We use them to distinguish between `cr` (credit account), and `c` (commit).
        const dr = self.get_account(t.debit_account_id) orelse return .debit_account_not_found;
        const cr = self.get_account(t.credit_account_id) orelse return .credit_account_not_found;
        assert(dr.id == t.debit_account_id);
        assert(cr.id == t.credit_account_id);
        assert(t.timestamp > dr.timestamp);
        assert(t.timestamp > cr.timestamp);

        if (dr.ledger != cr.ledger) return .accounts_must_have_the_same_ledger;
        if (t.ledger != dr.ledger) return .transfer_must_have_the_same_ledger_as_accounts;

        // If the transfer already exists, then it must not influence the overflow or limit checks.
        if (self.get_transfer(t.id)) |e| return create_transfer_exists(t, e);

        if (t.flags.pending) {
            if (sum_overflows(t.amount, dr.debits_pending)) return .overflows_debits_pending;
            if (sum_overflows(t.amount, cr.credits_pending)) return .overflows_credits_pending;
        }
        if (sum_overflows(t.amount, dr.debits_posted)) return .overflows_debits_posted;
        if (sum_overflows(t.amount, cr.credits_posted)) return .overflows_credits_posted;
        // We assert that the sum of the pending and posted balances can never overflow:
        if (sum_overflows(t.amount, dr.debits_pending + dr.debits_posted)) {
            return .overflows_debits;
        }
        if (sum_overflows(t.amount, cr.credits_pending + cr.credits_posted)) {
            return .overflows_credits;
        }

        if (dr.debits_exceed_credits(t.amount)) return .exceeds_credits;
        if (cr.credits_exceed_debits(t.amount)) return .exceeds_debits;

        self.transfers.putAssumeCapacityNoClobber(t.id, t.*);

        if (t.flags.pending) {
            dr.debits_pending += t.amount;
            cr.credits_pending += t.amount;
        } else {
            dr.debits_posted += t.amount;
            cr.credits_posted += t.amount;
        }

        self.commit_timestamp = t.timestamp;
        return .ok;
    }

    fn create_transfer_rollback(self: *StateMachine, t: *const Transfer) void {
        if (t.flags.post_pending_transfer or t.flags.void_pending_transfer) {
            return self.post_or_void_pending_transfer_rollback(t);
        }

        const dr = self.get_account(t.debit_account_id).?;
        const cr = self.get_account(t.credit_account_id).?;
        assert(dr.id == t.debit_account_id);
        assert(cr.id == t.credit_account_id);

        if (t.flags.pending) {
            dr.debits_pending -= t.amount;
            cr.credits_pending -= t.amount;
        } else {
            dr.debits_posted -= t.amount;
            cr.credits_posted -= t.amount;
        }
        assert(self.transfers.remove(t.id));
    }

    fn create_transfer_exists(t: *const Transfer, e: *const Transfer) CreateTransferResult {
        assert(t.id == e.id);
        // The flags change the behavior of the remaining comparisons, so compare the flags first.
        if (@bitCast(u16, t.flags) != @bitCast(u16, e.flags)) return .exists_with_different_flags;
        if (t.debit_account_id != e.debit_account_id) {
            return .exists_with_different_debit_account_id;
        }
        if (t.credit_account_id != e.credit_account_id) {
            return .exists_with_different_credit_account_id;
        }
        if (t.user_data != e.user_data) return .exists_with_different_user_data;
        assert(t.reserved == 0 and e.reserved == 0);
        assert(t.pending_id == 0 and e.pending_id == 0); // We know that the flags are the same.
        if (t.timeout != e.timeout) return .exists_with_different_timeout;
        assert(t.ledger == e.ledger); // If the accounts are the same, the ledger must be the same.
        if (t.code != e.code) return .exists_with_different_code;
        if (t.amount != e.amount) return .exists_with_different_amount;
        return .exists;
    }

    fn post_or_void_pending_transfer(self: *StateMachine, t: *const Transfer) CreateTransferResult {
        assert(t.id != 0);
        assert(t.flags.padding == 0);
        assert(t.reserved == 0);
        assert(t.timestamp > self.commit_timestamp);
        assert(t.flags.post_pending_transfer or t.flags.void_pending_transfer);

        if (t.flags.post_pending_transfer and t.flags.void_pending_transfer) {
            return .cannot_post_and_void_pending_transfer;
        }
        if (t.flags.pending) return .pending_transfer_cannot_post_or_void_another;
        if (t.timeout != 0) return .timeout_reserved_for_pending_transfer;

        if (t.pending_id == 0) return .pending_id_must_not_be_zero;
        if (t.pending_id == t.id) return .pending_id_must_be_different;

        const p = self.get_transfer(t.pending_id) orelse return .pending_transfer_not_found;
        assert(p.id == t.pending_id);
        if (!p.flags.pending) return .pending_transfer_not_pending;

        const dr = self.get_account(p.debit_account_id).?;
        const cr = self.get_account(p.credit_account_id).?;
        assert(dr.id == p.debit_account_id);
        assert(cr.id == p.credit_account_id);
        assert(p.timestamp > dr.timestamp);
        assert(p.timestamp > cr.timestamp);
        assert(p.amount > 0);

        if (t.debit_account_id > 0 and t.debit_account_id != p.debit_account_id) {
            return .pending_transfer_has_different_debit_account_id;
        }
        if (t.credit_account_id > 0 and t.credit_account_id != p.credit_account_id) {
            return .pending_transfer_has_different_credit_account_id;
        }
        // The user_data field is allowed to differ across pending and posting/voiding transfers.
        if (t.ledger > 0 and t.ledger != p.ledger) return .pending_transfer_has_different_ledger;
        if (t.code > 0 and t.code != p.code) return .pending_transfer_has_different_code;

        const amount = if (t.amount > 0) t.amount else p.amount;
        if (amount > p.amount) return .exceeds_pending_transfer_amount;

        if (t.flags.void_pending_transfer and amount < p.amount) {
            return .pending_transfer_has_different_amount;
        }

        if (self.get_transfer(t.id)) |e| return post_or_void_pending_transfer_exists(t, e, p);

        if (self.get_posted(t.pending_id)) |posted| {
            if (posted) return .pending_transfer_already_posted;
            return .pending_transfer_already_voided;
        }

        assert(p.timestamp < t.timestamp);
        assert(p.timeout > 0);
        if (p.timestamp + p.timeout <= t.timestamp) return .pending_transfer_expired;

        self.transfers.putAssumeCapacityNoClobber(t.id, .{
            .id = t.id,
            .debit_account_id = p.debit_account_id,
            .credit_account_id = p.credit_account_id,
            .user_data = if (t.user_data > 0) t.user_data else p.user_data,
            .reserved = p.reserved,
            .ledger = p.ledger,
            .code = p.code,
            .pending_id = t.pending_id,
            .timeout = t.timeout,
            .timestamp = t.timestamp,
            .flags = t.flags,
            .amount = amount,
        });

        self.posted.putAssumeCapacityNoClobber(t.pending_id, t.flags.post_pending_transfer);

        dr.debits_pending -= p.amount;
        cr.credits_pending -= p.amount;

        if (t.flags.post_pending_transfer) {
            assert(amount > 0);
            assert(amount <= p.amount);
            dr.debits_posted += amount;
            cr.credits_posted += amount;
        }

        self.commit_timestamp = t.timestamp;
        return .ok;
    }

    fn post_or_void_pending_transfer_rollback(self: *StateMachine, t: *const Transfer) void {
        assert(t.id > 0);
        assert(t.flags.post_pending_transfer or t.flags.void_pending_transfer);

        assert(t.pending_id > 0);
        const p = self.get_transfer(t.pending_id).?;
        assert(p.id == t.pending_id);
        assert(p.debit_account_id > 0);
        assert(p.credit_account_id > 0);

        const dr = self.get_account(p.debit_account_id).?;
        const cr = self.get_account(p.credit_account_id).?;
        assert(dr.id == p.debit_account_id);
        assert(cr.id == p.credit_account_id);

        if (t.flags.post_pending_transfer) {
            const amount = if (t.amount > 0) t.amount else p.amount;
            assert(amount > 0);
            assert(amount <= p.amount);
            dr.debits_posted -= amount;
            cr.credits_posted -= amount;
        }
        dr.debits_pending += p.amount;
        cr.credits_pending += p.amount;

        assert(self.posted.remove(t.pending_id));
        assert(self.transfers.remove(t.id));
    }

    fn post_or_void_pending_transfer_exists(
        t: *const Transfer,
        e: *const Transfer,
        p: *const Transfer,
    ) CreateTransferResult {
        assert(p.flags.pending);
        assert(t.pending_id == p.id);
        assert(t.id != p.id);
        assert(t.id == e.id);

        // Do not assume that `e` is necessarily a posting or voiding transfer.
        if (@bitCast(u16, t.flags) != @bitCast(u16, e.flags)) {
            return .exists_with_different_flags;
        }

        // If `e` posted or voided a different pending transfer, then the accounts will differ.
        if (t.pending_id != e.pending_id) return .exists_with_different_pending_id;

        assert(e.flags.post_pending_transfer or e.flags.void_pending_transfer);
        assert(e.debit_account_id == p.debit_account_id);
        assert(e.credit_account_id == p.credit_account_id);
        assert(e.reserved == 0);
        assert(e.pending_id == p.id);
        assert(e.timeout == 0);
        assert(e.ledger == p.ledger);
        assert(e.code == p.code);
        assert(e.timestamp > p.timestamp);

        assert(t.flags.post_pending_transfer == e.flags.post_pending_transfer);
        assert(t.flags.void_pending_transfer == e.flags.void_pending_transfer);
        assert(t.debit_account_id == 0 or t.debit_account_id == e.debit_account_id);
        assert(t.credit_account_id == 0 or t.credit_account_id == e.credit_account_id);
        assert(t.reserved == 0);
        assert(t.timeout == 0);
        assert(t.ledger == 0 or t.ledger == e.ledger);
        assert(t.code == 0 or t.code == e.code);
        assert(t.timestamp > e.timestamp);

        if (t.user_data == 0) {
            if (e.user_data != p.user_data) return .exists_with_different_user_data;
        } else {
            if (t.user_data != e.user_data) return .exists_with_different_user_data;
        }

        if (t.amount == 0) {
            if (e.amount != p.amount) return .exists_with_different_amount;
        } else {
            if (t.amount != e.amount) return .exists_with_different_amount;
        }

        return .exists;
    }

    /// This is our core private method for changing balances.
    /// Returns a live pointer to an Account in the accounts hash map.
    /// This is intended to lookup an Account and modify balances directly by reference.
    /// This pointer is invalidated if the hash map is resized by another insert, e.g. if we get a
    /// pointer, insert another account without capacity, and then modify this pointer... BOOM!
    /// This is a sharp tool but replaces a lookup, copy and update with a single lookup.
    fn get_account(self: *const StateMachine, id: u128) ?*Account {
        return self.accounts.getPtr(id);
    }

    /// See the comment for get_account().
    fn get_transfer(self: *const StateMachine, id: u128) ?*Transfer {
        return self.transfers.getPtr(id);
    }

    /// Returns whether a pending transfer, if it exists, has already been posted or voided.
    fn get_posted(self: *const StateMachine, pending_id: u128) ?bool {
        return self.posted.get(pending_id);
    }
};

fn sum_overflows(a: u64, b: u64) bool {
    var c: u64 = undefined;
    return @addWithOverflow(u64, a, b, &c);
}

/// Optimizes for the common case, where the array is zeroed. Completely branchless.
fn zeroed_32_bytes(a: [32]u8) bool {
    const x = @bitCast([4]u64, a);
    return (x[0] | x[1] | x[2] | x[3]) == 0;
}

fn zeroed_48_bytes(a: [48]u8) bool {
    const x = @bitCast([6]u64, a);
    return (x[0] | x[1] | x[2] | x[3] | x[4] | x[5]) == 0;
}

/// Optimizes for the common case, where the arrays are equal. Completely branchless.
fn equal_32_bytes(a: [32]u8, b: [32]u8) bool {
    const x = @bitCast([4]u64, a);
    const y = @bitCast([4]u64, b);

    const c = (x[0] ^ y[0]) | (x[1] ^ y[1]);
    const d = (x[2] ^ y[2]) | (x[3] ^ y[3]);

    return (c | d) == 0;
}

fn equal_48_bytes(a: [48]u8, b: [48]u8) bool {
    const x = @bitCast([6]u64, a);
    const y = @bitCast([6]u64, b);

    const c = (x[0] ^ y[0]) | (x[1] ^ y[1]);
    const d = (x[2] ^ y[2]) | (x[3] ^ y[3]);
    const e = (x[4] ^ y[4]) | (x[5] ^ y[5]);

    return (c | d | e) == 0;
}

const testing = std.testing;
const expect = testing.expect;
const expectEqual = testing.expectEqual;
const expectEqualSlices = testing.expectEqualSlices;

test "sum_overflows" {
    try expectEqual(false, sum_overflows(math.maxInt(u64), 0));
    try expectEqual(false, sum_overflows(math.maxInt(u64) - 1, 1));
    try expectEqual(false, sum_overflows(1, math.maxInt(u64) - 1));

    try expectEqual(true, sum_overflows(math.maxInt(u64), 1));
    try expectEqual(true, sum_overflows(1, math.maxInt(u64)));

    try expectEqual(true, sum_overflows(math.maxInt(u64), math.maxInt(u64)));
    try expectEqual(true, sum_overflows(math.maxInt(u64), math.maxInt(u64)));
}

test "create/lookup/rollback accounts" {
    const Vector = struct { result: CreateAccountResult, object: Account };

    const vectors = [_]Vector{
        .{
            .result = .ok,
            .object = mem.zeroInit(Account, .{
                .id = 1,
                .user_data = 2,
                .ledger = 3,
                .code = 4,
                .debits_pending = 5,
                .debits_posted = 6,
                .credits_pending = 7,
                .credits_posted = 8,
                .timestamp = 1,
            }),
        },
        .{
            .result = .reserved_flag,
            .object = mem.zeroInit(Account, .{
                .id = 0,
                .user_data = 0,
                .reserved = [_]u8{1} ** 48,
                .ledger = 0,
                .code = 0,
                .flags = .{
                    .padding = 1,
                    .debits_must_not_exceed_credits = true,
                    .credits_must_not_exceed_debits = true,
                },
                .debits_pending = math.maxInt(u64),
                .debits_posted = math.maxInt(u64),
                .credits_pending = math.maxInt(u64),
                .credits_posted = math.maxInt(u64),
                .timestamp = 2,
            }),
        },
        .{
            .result = .reserved_field,
            .object = mem.zeroInit(Account, .{
                .id = 0,
                .user_data = 0,
                .reserved = [_]u8{1} ** 48,
                .ledger = 0,
                .code = 0,
                .flags = .{
                    .padding = 0,
                    .debits_must_not_exceed_credits = true,
                    .credits_must_not_exceed_debits = true,
                },
                .debits_pending = math.maxInt(u64),
                .debits_posted = math.maxInt(u64),
                .credits_pending = math.maxInt(u64),
                .credits_posted = math.maxInt(u64),
                .timestamp = 2,
            }),
        },
        .{
            .result = .id_must_not_be_zero,
            .object = mem.zeroInit(Account, .{
                .id = 0,
                .user_data = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{
                    .padding = 0,
                    .debits_must_not_exceed_credits = true,
                    .credits_must_not_exceed_debits = true,
                },
                .debits_pending = math.maxInt(u64),
                .debits_posted = math.maxInt(u64),
                .credits_pending = math.maxInt(u64),
                .credits_posted = math.maxInt(u64),
                .timestamp = 2,
            }),
        },
        .{
            .result = .ledger_must_not_be_zero,
            .object = mem.zeroInit(Account, .{
                .id = 1,
                .user_data = 20,
                .ledger = 0,
                .code = 0,
                .flags = .{
                    .padding = 0,
                    .debits_must_not_exceed_credits = true,
                    .credits_must_not_exceed_debits = true,
                },
                .debits_pending = math.maxInt(u64),
                .debits_posted = math.maxInt(u64),
                .credits_pending = math.maxInt(u64),
                .credits_posted = math.maxInt(u64),
                .timestamp = 2,
            }),
        },
        .{
            .result = .code_must_not_be_zero,
            .object = mem.zeroInit(Account, .{
                .id = 1,
                .user_data = 20,
                .ledger = 30,
                .code = 0,
                .flags = .{
                    .debits_must_not_exceed_credits = true,
                    .credits_must_not_exceed_debits = true,
                },
                .debits_pending = math.maxInt(u64),
                .debits_posted = math.maxInt(u64),
                .credits_pending = math.maxInt(u64),
                .credits_posted = math.maxInt(u64),
                .timestamp = 2,
            }),
        },
        .{
            .result = .mutually_exclusive_flags,
            .object = mem.zeroInit(Account, .{
                .id = 1,
                .user_data = 20,
                .ledger = 30,
                .code = 40,
                .flags = .{
                    .debits_must_not_exceed_credits = true,
                    .credits_must_not_exceed_debits = true,
                },
                .debits_pending = math.maxInt(u64),
                .debits_posted = math.maxInt(u64),
                .credits_pending = math.maxInt(u64),
                .credits_posted = math.maxInt(u64),
                .timestamp = 2,
            }),
        },
        .{
            .result = .overflows_debits,
            .object = mem.zeroInit(Account, .{
                .id = 1,
                .user_data = 20,
                .ledger = 30,
                .code = 40,
                .flags = .{
                    .debits_must_not_exceed_credits = true,
                },
                .debits_pending = math.maxInt(u64),
                .debits_posted = 60,
                .credits_pending = math.maxInt(u64),
                .credits_posted = 80,
                .timestamp = 2,
            }),
        },
        .{
            .result = .overflows_credits,
            .object = mem.zeroInit(Account, .{
                .id = 1,
                .user_data = 20,
                .ledger = 30,
                .code = 40,
                .flags = .{
                    .credits_must_not_exceed_debits = true,
                },
                .debits_pending = 50,
                .debits_posted = 60,
                .credits_pending = math.maxInt(u64),
                .credits_posted = 80,
                .timestamp = 2,
            }),
        },
        .{
            .result = .exceeds_credits,
            .object = mem.zeroInit(Account, .{
                .id = 1,
                .user_data = 20,
                .ledger = 30,
                .code = 40,
                .flags = .{
                    .debits_must_not_exceed_credits = true,
                },
                .debits_pending = 50,
                .debits_posted = 60,
                .credits_pending = 1,
                .credits_posted = 109,
                .timestamp = 2,
            }),
        },
        .{
            .result = .exceeds_debits,
            .object = mem.zeroInit(Account, .{
                .id = 1,
                .user_data = 20,
                .ledger = 30,
                .code = 40,
                .flags = .{
                    .credits_must_not_exceed_debits = true,
                },
                .debits_pending = 50,
                .debits_posted = 60,
                .credits_pending = 1,
                .credits_posted = 109,
                .timestamp = 2,
            }),
        },
        .{
            .result = .exists_with_different_flags,
            .object = mem.zeroInit(Account, .{
                .id = 1,
                .user_data = 20,
                .ledger = 30,
                .code = 40,
                .flags = .{
                    .credits_must_not_exceed_debits = true,
                },
                .debits_pending = 50,
                .debits_posted = 60,
                .credits_pending = 0,
                .credits_posted = 0,
                .timestamp = 2,
            }),
        },
        .{
            .result = .exists_with_different_user_data,
            .object = mem.zeroInit(Account, .{
                .id = 1,
                .user_data = 20,
                .ledger = 30,
                .code = 40,
                .debits_pending = 50,
                .debits_posted = 60,
                .credits_pending = 70,
                .credits_posted = 80,
                .timestamp = 2,
            }),
        },
        .{
            .result = .exists_with_different_ledger,
            .object = mem.zeroInit(Account, .{
                .id = 1,
                .user_data = 2,
                .ledger = 30,
                .code = 40,
                .debits_pending = 50,
                .debits_posted = 60,
                .credits_pending = 70,
                .credits_posted = 80,
                .timestamp = 2,
            }),
        },
        .{
            .result = .exists_with_different_code,
            .object = mem.zeroInit(Account, .{
                .id = 1,
                .user_data = 2,
                .ledger = 3,
                .code = 40,
                .debits_pending = 50,
                .debits_posted = 60,
                .credits_pending = 70,
                .credits_posted = 80,
                .timestamp = 2,
            }),
        },
        .{
            .result = .exists_with_different_debits_pending,
            .object = mem.zeroInit(Account, .{
                .id = 1,
                .user_data = 2,
                .ledger = 3,
                .code = 4,
                .debits_pending = 50,
                .debits_posted = 60,
                .credits_pending = 70,
                .credits_posted = 80,
                .timestamp = 2,
            }),
        },
        .{
            .result = .exists_with_different_debits_posted,
            .object = mem.zeroInit(Account, .{
                .id = 1,
                .user_data = 2,
                .ledger = 3,
                .code = 4,
                .debits_pending = 5,
                .debits_posted = 60,
                .credits_pending = 70,
                .credits_posted = 80,
                .timestamp = 2,
            }),
        },
        .{
            .result = .exists_with_different_credits_pending,
            .object = mem.zeroInit(Account, .{
                .id = 1,
                .user_data = 2,
                .ledger = 3,
                .code = 4,
                .debits_pending = 5,
                .debits_posted = 6,
                .credits_pending = 70,
                .credits_posted = 80,
                .timestamp = 2,
            }),
        },
        .{
            .result = .exists_with_different_credits_posted,
            .object = mem.zeroInit(Account, .{
                .id = 1,
                .user_data = 2,
                .ledger = 3,
                .code = 4,
                .debits_pending = 5,
                .debits_posted = 6,
                .credits_pending = 7,
                .credits_posted = 80,
                .timestamp = 2,
            }),
        },
        .{
            .result = .exists,
            .object = mem.zeroInit(Account, .{
                .id = 1,
                .user_data = 2,
                .ledger = 3,
                .code = 4,
                .debits_pending = 5,
                .debits_posted = 6,
                .credits_pending = 7,
                .credits_posted = 8,
                .timestamp = 2,
            }),
        },
    };

    var state_machine = try StateMachine.init(testing.allocator, vectors.len, 0, 0);
    defer state_machine.deinit();

    for (vectors) |vector, i| {
        const result = state_machine.create_account(&vector.object);
        expectEqual(vector.result, result) catch |err| {
            print_test_vector(i, vector.result, result, vector.object, err);
            return err;
        };

        if (vector.result == .ok) {
            try expectEqual(vector.object, state_machine.get_account(vector.object.id).?.*);
        }
    }

    state_machine.create_account_rollback(&vectors[0].object);
    try expect(state_machine.get_account(vectors[0].object.id) == null);
}

test "linked accounts" {
    const accounts_max = 5;
    const transfers_max = 0;
    const transfers_pending_max = 0;

    var accounts = [_]Account{
        // An individual event (successful):
        mem.zeroInit(Account, .{ .id = 7, .code = 1, .ledger = 1 }),

        // A chain of 4 events (the last event in the chain closes the chain with linked=false):
        // Commit/rollback.
        mem.zeroInit(Account, .{ .id = 1, .code = 1, .ledger = 1, .flags = .{ .linked = true } }),
        // Commit/rollback.
        mem.zeroInit(Account, .{ .id = 2, .code = 1, .ledger = 1, .flags = .{ .linked = true } }),
        // Fail with .exists.
        mem.zeroInit(Account, .{ .id = 1, .code = 1, .ledger = 1, .flags = .{ .linked = true } }),
        // Fail without committing.
        mem.zeroInit(Account, .{ .id = 3, .code = 1, .ledger = 1 }),

        // An individual event (successful):
        // This should not see any effect from the failed chain above.
        mem.zeroInit(Account, .{ .id = 1, .code = 1, .ledger = 1 }),

        // A chain of 2 events (the first event fails the chain):
        mem.zeroInit(Account, .{ .id = 1, .code = 2, .ledger = 1, .flags = .{ .linked = true } }),
        mem.zeroInit(Account, .{ .id = 2, .code = 1, .ledger = 1 }),

        // An individual event (successful):
        mem.zeroInit(Account, .{ .id = 2, .code = 1, .ledger = 1 }),

        // A chain of 2 events (the last event fails the chain):
        mem.zeroInit(Account, .{ .id = 3, .code = 1, .ledger = 1, .flags = .{ .linked = true } }),
        mem.zeroInit(Account, .{ .id = 1, .code = 1, .ledger = 2 }),

        // A chain of 2 events (successful):
        mem.zeroInit(Account, .{ .id = 3, .code = 1, .ledger = 1, .flags = .{ .linked = true } }),
        mem.zeroInit(Account, .{ .id = 4, .code = 1, .ledger = 1 }),
    };

    var state_machine = try StateMachine.init(
        testing.allocator,
        accounts_max,
        transfers_max,
        transfers_pending_max,
    );
    defer state_machine.deinit();

    const input = mem.asBytes(&accounts);

    const output = try testing.allocator.alloc(u8, 4096);
    defer testing.allocator.free(output);

    _ = state_machine.prepare(.create_accounts, input);
    const size = state_machine.commit(0, .create_accounts, input, output);
    const results = mem.bytesAsSlice(CreateAccountsResult, output[0..size]);

    try expectEqualSlices(
        CreateAccountsResult,
        &[_]CreateAccountsResult{
            .{ .index = 1, .result = .linked_event_failed },
            .{ .index = 2, .result = .linked_event_failed },
            .{ .index = 3, .result = .exists },
            .{ .index = 4, .result = .linked_event_failed },
            .{ .index = 6, .result = .exists_with_different_flags },
            .{ .index = 7, .result = .linked_event_failed },
            .{ .index = 9, .result = .linked_event_failed },
            .{ .index = 10, .result = .exists_with_different_ledger },
        },
        results,
    );

    try expectEqual(accounts[0], state_machine.get_account(accounts[0].id).?.*);
    try expectEqual(accounts[5], state_machine.get_account(accounts[5].id).?.*);
    try expectEqual(accounts[8], state_machine.get_account(accounts[8].id).?.*);
    try expectEqual(accounts[11], state_machine.get_account(accounts[11].id).?.*);
    try expectEqual(accounts[12], state_machine.get_account(accounts[12].id).?.*);
    try expectEqual(@as(u32, 5), state_machine.accounts.count());

    // TODO How can we test that events were in fact rolled back in LIFO order?
    // All our rollback handlers appear to be commutative.
}

// The goal is to ensure that:
// 1. all CreateTransferResult enums are covered, with
// 2. enums tested in the order that they are defined, for easier auditing of coverage, and that
// 3. state machine logic cannot be reordered in any way, breaking determinism.
test "create/lookup/rollback transfers" {
    var accounts = [_]Account{
        mem.zeroInit(Account, .{
            .id = 1,
            .ledger = 1,
            .code = 1,
            .debits_pending = 100,
            .debits_posted = 200,
        }),
        mem.zeroInit(Account, .{ .id = 2, .ledger = 2, .code = 2 }),
        mem.zeroInit(Account, .{
            .id = 3,
            .ledger = 1,
            .code = 1,
            .credits_pending = 110,
            .credits_posted = 210,
        }),
        mem.zeroInit(Account, .{
            .id = 4,
            .ledger = 1,
            .code = 1,
            .flags = .{ .debits_must_not_exceed_credits = true },
            .debits_pending = 20,
            .debits_posted = math.maxInt(u64) - 500 - 200,
            .credits_pending = 0,
            .credits_posted = math.maxInt(u64) - 500,
        }),
        mem.zeroInit(Account, .{
            .id = 5,
            .ledger = 1,
            .code = 1,
            .flags = .{ .credits_must_not_exceed_debits = true },
            .debits_pending = 0,
            .debits_posted = math.maxInt(u64) - 1000,
            .credits_pending = 10,
            .credits_posted = math.maxInt(u64) - 1000 - 100,
        }),
    };

    var state_machine = try StateMachine.init(testing.allocator, accounts.len, 1, 0);
    defer state_machine.deinit();

    const input = mem.asBytes(&accounts);

    const output = try testing.allocator.alloc(u8, 4096);
    defer testing.allocator.free(output);

    _ = state_machine.prepare(.create_accounts, input);
    const size = state_machine.commit(0, .create_accounts, input, output);

    const errors = mem.bytesAsSlice(CreateAccountsResult, output[0..size]);
    try expect(errors.len == 0);

    for (accounts) |account| {
        try expectEqual(account, state_machine.get_account(account.id).?.*);
    }

    const Vector = struct { result: CreateTransferResult, object: Transfer };

    const timestamp: u64 = (state_machine.commit_timestamp + 1);
    const vectors = [_]Vector{
        .{
            .result = .reserved_flag,
            .object = mem.zeroInit(Transfer, .{
                .id = 0,
                .debit_account_id = 0,
                .credit_account_id = 0,
                .reserved = 1,
                .pending_id = 1,
                .timeout = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{ .pending = true, .padding = 1 },
                .amount = 0,
                .timestamp = timestamp,
            }),
        },
        .{
            .result = .reserved_field,
            .object = mem.zeroInit(Transfer, .{
                .id = 0,
                .debit_account_id = 0,
                .credit_account_id = 0,
                .reserved = 1,
                .pending_id = 1,
                .timeout = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{ .pending = true },
                .amount = 0,
                .timestamp = timestamp,
            }),
        },
        .{
            .result = .id_must_not_be_zero,
            .object = mem.zeroInit(Transfer, .{
                .id = 0,
                .debit_account_id = 0,
                .credit_account_id = 0,
                .pending_id = 1,
                .timeout = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{ .pending = true },
                .amount = 0,
                .timestamp = timestamp,
            }),
        },
        .{
            .result = .debit_account_id_must_not_be_zero,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 0,
                .credit_account_id = 0,
                .pending_id = 1,
                .timeout = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{ .pending = true },
                .amount = 0,
                .timestamp = timestamp,
            }),
        },
        .{
            .result = .credit_account_id_must_not_be_zero,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 100,
                .credit_account_id = 0,
                .pending_id = 1,
                .timeout = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{ .pending = true },
                .amount = 0,
                .timestamp = timestamp,
            }),
        },
        .{
            .result = .accounts_must_be_different,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 100,
                .credit_account_id = 100,
                .pending_id = 1,
                .timeout = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{ .pending = true },
                .amount = 0,
                .timestamp = timestamp,
            }),
        },
        .{
            .result = .pending_id_must_be_zero,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 100,
                .credit_account_id = 200,
                .pending_id = 1,
                .timeout = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{ .pending = true },
                .amount = 0,
                .timestamp = timestamp,
            }),
        },
        .{
            .result = .pending_transfer_must_timeout,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 100,
                .credit_account_id = 200,
                .timeout = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{ .pending = true },
                .amount = 0,
                .timestamp = timestamp,
            }),
        },
        .{
            .result = .timeout_reserved_for_pending_transfer,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 100,
                .credit_account_id = 200,
                .timeout = 1,
                .ledger = 0,
                .code = 0,
                .amount = 0,
                .timestamp = timestamp,
            }),
        },
        .{
            .result = .ledger_must_not_be_zero,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 100,
                .credit_account_id = 200,
                .timeout = 1,
                .ledger = 0,
                .code = 0,
                .flags = .{ .pending = true },
                .amount = 0,
                .timestamp = timestamp,
            }),
        },
        .{
            .result = .code_must_not_be_zero,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 100,
                .credit_account_id = 200,
                .timeout = 1,
                .ledger = 100,
                .code = 0,
                .flags = .{ .pending = true },
                .amount = 0,
                .timestamp = timestamp,
            }),
        },
        .{
            .result = .amount_must_not_be_zero,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 100,
                .credit_account_id = 200,
                .timeout = 1,
                .ledger = 100,
                .code = 1,
                .flags = .{ .pending = true },
                .amount = 0,
                .timestamp = timestamp,
            }),
        },
        .{
            .result = .debit_account_not_found,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 100,
                .credit_account_id = 200,
                .timeout = 1,
                .ledger = 100,
                .code = 1,
                .flags = .{ .pending = true },
                .amount = 100,
                .timestamp = timestamp,
            }),
        },
        .{
            .result = .credit_account_not_found,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 1,
                .credit_account_id = 200,
                .timeout = 1,
                .ledger = 100,
                .code = 1,
                .flags = .{ .pending = true },
                .amount = 100,
                .timestamp = timestamp,
            }),
        },
        .{
            .result = .accounts_must_have_the_same_ledger,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 1,
                .credit_account_id = 2,
                .ledger = 100,
                .code = 1,
                .amount = 1,
                .timestamp = timestamp,
            }),
        },
        .{
            .result = .transfer_must_have_the_same_ledger_as_accounts,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 1,
                .credit_account_id = 3,
                .ledger = 100,
                .code = 1,
                .amount = 1,
                .timestamp = timestamp,
            }),
        },
        .{
            .result = .overflows_debits_pending,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 1,
                .credit_account_id = 3,
                .timeout = 30000,
                .ledger = 1,
                .code = 1,
                .flags = .{ .pending = true },
                .amount = math.maxInt(u64) - accounts[1 - 1].debits_pending + 1,
                .timestamp = timestamp,
            }),
        },
        .{
            .result = .overflows_credits_pending,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 1,
                .credit_account_id = 3,
                .timeout = 30000,
                .ledger = 1,
                .code = 1,
                .flags = .{ .pending = true },
                .amount = math.maxInt(u64) - accounts[3 - 1].credits_pending + 1,
                .timestamp = timestamp,
            }),
        },
        .{
            .result = .overflows_debits_posted,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 1,
                .credit_account_id = 3,
                .ledger = 1,
                .code = 1,
                .amount = math.maxInt(u64) - accounts[1 - 1].debits_posted + 1,
                .timestamp = timestamp,
            }),
        },
        .{
            .result = .overflows_credits_posted,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 1,
                .credit_account_id = 3,
                .ledger = 1,
                .code = 1,
                .amount = math.maxInt(u64) - accounts[3 - 1].credits_posted + 1,
                .timestamp = timestamp,
            }),
        },
        .{
            .result = .overflows_debits,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 1,
                .credit_account_id = 3,
                .ledger = 1,
                .code = 1,
                .amount = math.maxInt(u64) -
                    accounts[1 - 1].debits_pending -
                    accounts[1 - 1].debits_posted + 1,
                .timestamp = timestamp,
            }),
        },
        .{
            .result = .overflows_credits,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 1,
                .credit_account_id = 3,
                .ledger = 1,
                .code = 1,
                .amount = math.maxInt(u64) -
                    accounts[3 - 1].credits_pending -
                    accounts[3 - 1].credits_posted + 1,
                .timestamp = timestamp,
            }),
        },
        .{
            .result = .exceeds_credits,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 4,
                .credit_account_id = 5,
                .ledger = 1,
                .code = 1,
                .amount = accounts[4 - 1].credits_posted -
                    accounts[4 - 1].debits_pending -
                    accounts[4 - 1].debits_posted + 1,
                .timestamp = timestamp,
            }),
        },
        .{
            .result = .exceeds_debits,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 4,
                .credit_account_id = 5,
                .ledger = 1,
                .code = 1,
                .amount = accounts[5 - 1].debits_posted -
                    accounts[5 - 1].credits_pending -
                    accounts[5 - 1].credits_posted + 1,
                .timestamp = timestamp,
            }),
        },
        .{
            .result = .ok,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 1,
                .credit_account_id = 3,
                .timeout = 10000,
                .ledger = 1,
                .code = 1,
                .flags = .{ .pending = true },
                .amount = 123,
                .timestamp = timestamp + 1,
            }),
        },
        .{
            // Ensure that idempotence is only checked after validation.
            .result = .transfer_must_have_the_same_ledger_as_accounts,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 1,
                .credit_account_id = 3,
                .timeout = 10000,
                .ledger = 2,
                .code = 1,
                .flags = .{ .pending = true },
                .amount = 123,
                .timestamp = timestamp + 2,
            }),
        },
        .{
            .result = .exists_with_different_flags,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 1,
                .credit_account_id = 3,
                .user_data = 1,
                .ledger = 1,
                .code = 2,
                .flags = .{ .pending = false },
                .amount = math.maxInt(u64),
                .timestamp = timestamp + 2,
            }),
        },
        .{
            .result = .exists_with_different_debit_account_id,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 3,
                .credit_account_id = 1,
                .user_data = 1,
                .timeout = 10000,
                .ledger = 1,
                .code = 2,
                .flags = .{ .pending = true },
                .amount = math.maxInt(u64),
                .timestamp = timestamp + 2,
            }),
        },
        .{
            .result = .exists_with_different_credit_account_id,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 1,
                .credit_account_id = 4,
                .user_data = 1,
                .timeout = 10000,
                .ledger = 1,
                .code = 2,
                .flags = .{ .pending = true },
                .amount = math.maxInt(u64),
                .timestamp = timestamp + 2,
            }),
        },
        .{
            .result = .exists_with_different_user_data,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 1,
                .credit_account_id = 3,
                .user_data = 1,
                .timeout = 10000,
                .ledger = 1,
                .code = 2,
                .flags = .{ .pending = true },
                .amount = math.maxInt(u64),
                .timestamp = timestamp + 2,
            }),
        },
        .{
            .result = .exists_with_different_timeout,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 1,
                .credit_account_id = 3,
                .timeout = 10001,
                .ledger = 1,
                .code = 2,
                .flags = .{ .pending = true },
                .amount = math.maxInt(u64),
                .timestamp = timestamp + 2,
            }),
        },
        .{
            .result = .exists_with_different_code,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 1,
                .credit_account_id = 3,
                .timeout = 10000,
                .ledger = 1,
                .code = 2,
                .flags = .{ .pending = true },
                .amount = math.maxInt(u64),
                .timestamp = timestamp + 2,
            }),
        },
        .{
            .result = .exists_with_different_amount,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 1,
                .credit_account_id = 3,
                .timeout = 10000,
                .ledger = 1,
                .code = 1,
                .flags = .{ .pending = true },
                .amount = math.maxInt(u64),
                .timestamp = timestamp + 2,
            }),
        },
        .{
            .result = .exists,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .debit_account_id = 1,
                .credit_account_id = 3,
                .timeout = 10000,
                .ledger = 1,
                .code = 1,
                .flags = .{ .pending = true },
                .amount = 123,
                .timestamp = timestamp + 2,
            }),
        },
        .{
            .result = .ok,
            .object = mem.zeroInit(Transfer, .{
                .id = 2,
                .debit_account_id = 3,
                .credit_account_id = 1,
                .ledger = 1,
                .code = 2,
                .amount = 7,
                .timestamp = timestamp + 2,
            }),
        },
        .{
            .result = .ok,
            .object = mem.zeroInit(Transfer, .{
                .id = 3,
                .debit_account_id = 1,
                .credit_account_id = 3,
                .ledger = 1,
                .code = 2,
                .amount = 3,
                .timestamp = timestamp + 3,
            }),
        },
    };

    for (vectors) |vector, i| {
        const result = state_machine.create_transfer(&vector.object);
        expectEqual(vector.result, result) catch |err| {
            print_test_vector(i, vector.result, result, vector.object, err);
            return err;
        };
        if (vector.result == .ok) {
            try expectEqual(vector.object, state_machine.get_transfer(vector.object.id).?.*);
        }
    }

    // Transfer 3:
    try test_account_balances(&state_machine, 1, 100 + 123, 200 + 3, 0, 7);
    try test_account_balances(&state_machine, 3, 0, 7, 110 + 123, 210 + 3);
    state_machine.create_transfer_rollback(state_machine.get_transfer(3).?);
    try test_account_balances(&state_machine, 1, 100 + 123, 200, 0, 7);
    try test_account_balances(&state_machine, 3, 0, 7, 110 + 123, 210);
    try expect(state_machine.get_transfer(3) == null);

    // Transfer 2:
    try test_account_balances(&state_machine, 1, 100 + 123, 200, 0, 7);
    try test_account_balances(&state_machine, 3, 0, 7, 110 + 123, 210);
    state_machine.create_transfer_rollback(state_machine.get_transfer(2).?);
    try test_account_balances(&state_machine, 1, 100 + 123, 200, 0, 0);
    try test_account_balances(&state_machine, 3, 0, 0, 110 + 123, 210);
    try expect(state_machine.get_transfer(2) == null);

    // Transfer 1:
    try test_account_balances(&state_machine, 1, 100 + 123, 200, 0, 0);
    try test_account_balances(&state_machine, 3, 0, 0, 110 + 123, 210);
    state_machine.create_transfer_rollback(state_machine.get_transfer(1).?);
    try test_account_balances(&state_machine, 1, 100, 200, 0, 0);
    try test_account_balances(&state_machine, 3, 0, 0, 110, 210);
    try expect(state_machine.get_transfer(1) == null);

    for (accounts) |account| {
        state_machine.create_account_rollback(&account);
        try expect(state_machine.get_account(account.id) == null);
    }
}

test "create/lookup/rollback 2-phase transfers" {
    var accounts = [_]Account{
        mem.zeroInit(Account, .{ .id = 1, .ledger = 1, .code = 1 }),
        mem.zeroInit(Account, .{ .id = 2, .ledger = 1, .code = 1 }),
    };

    var transfers = [_]Transfer{
        mem.zeroInit(Transfer, .{
            .id = 1,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .ledger = 1,
            .code = 1,
            .amount = 15,
        }),
        mem.zeroInit(Transfer, .{
            .id = 2,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .timeout = 1000,
            .ledger = 1,
            .code = 1,
            .flags = .{ .pending = true },
            .amount = 15,
        }),
        mem.zeroInit(Transfer, .{
            .id = 3,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .timeout = 5,
            .ledger = 1,
            .code = 1,
            .flags = .{ .pending = true },
            .amount = 15,
        }),
        mem.zeroInit(Transfer, .{
            .id = 4,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .timeout = 1,
            .ledger = 1,
            .code = 1,
            .flags = .{ .pending = true },
            .amount = 15,
        }),
        mem.zeroInit(Transfer, .{
            .id = 5,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .user_data = 73,
            .timeout = 6,
            .ledger = 1,
            .code = 1,
            .flags = .{ .pending = true },
            .amount = 7,
        }),
    };

    var state_machine = try StateMachine.init(testing.allocator, accounts.len, 100, 1);
    defer state_machine.deinit();

    // Create accounts:
    const accounts_input = mem.asBytes(&accounts);

    const accounts_output = try testing.allocator.alloc(u8, 4096);
    defer testing.allocator.free(accounts_output);

    const accounts_timestamp = state_machine.prepare(.create_accounts, accounts_input);
    {
        const size = state_machine.commit(0, .create_accounts, accounts_input, accounts_output);
        const errors = mem.bytesAsSlice(CreateAccountsResult, accounts_output[0..size]);
        try expectEqual(@as(usize, 0), errors.len);
    }
    for (accounts) |account| {
        try expectEqual(account, state_machine.get_account(account.id).?.*);
    }

    // Create pending transfers:
    const transfers_input = mem.asBytes(&transfers);

    const transfers_output = try testing.allocator.alloc(u8, 4096);
    defer testing.allocator.free(transfers_output);

    const transfers_timestamp = state_machine.prepare(.create_transfers, transfers_input);
    try testing.expect(transfers_timestamp > accounts_timestamp);
    {
        const size = state_machine.commit(0, .create_transfers, transfers_input, transfers_output);
        const errors = mem.bytesAsSlice(CreateTransfersResult, transfers_output[0..size]);
        try expectEqual(@as(usize, 0), errors.len);
    }
    for (transfers) |transfer| {
        try expectEqual(transfer, state_machine.get_transfer(transfer.id).?.*);
    }

    // Test balances before posting:
    try test_account_balances(&state_machine, 1, 52, 15, 0, 0);
    try test_account_balances(&state_machine, 2, 0, 0, 52, 15);

    // Post pending transfers:
    const Vector = struct { result: CreateTransferResult, object: Transfer };
    const timestamp: u64 = (state_machine.commit_timestamp + 1);

    const vectors = [_]Vector{
        .{
            .result = .ok,
            .object = mem.zeroInit(Transfer, .{
                .id = 101,
                .debit_account_id = 1,
                .credit_account_id = 2,
                .user_data = 1000,
                .pending_id = 2,
                .ledger = 1,
                .code = 1,
                .flags = .{ .post_pending_transfer = true },
                .amount = 13,
                .timestamp = timestamp,
            }),
        },
        .{
            .result = .cannot_post_and_void_pending_transfer,
            .object = mem.zeroInit(Transfer, .{
                .id = 101,
                .debit_account_id = 10,
                .credit_account_id = 20,
                .user_data = 30,
                .pending_id = 0,
                .timeout = 50,
                .ledger = 60,
                .code = 70,
                .flags = .{
                    .pending = true,
                    .post_pending_transfer = true,
                    .void_pending_transfer = true,
                },
                .amount = 80,
                .timestamp = timestamp + 1,
            }),
        },
        .{
            .result = .pending_transfer_cannot_post_or_void_another,
            .object = mem.zeroInit(Transfer, .{
                .id = 101,
                .debit_account_id = 10,
                .credit_account_id = 20,
                .user_data = 30,
                .pending_id = 0,
                .timeout = 50,
                .ledger = 60,
                .code = 70,
                .flags = .{
                    .pending = true,
                    .void_pending_transfer = true,
                },
                .amount = 80,
                .timestamp = timestamp + 1,
            }),
        },
        .{
            .result = .timeout_reserved_for_pending_transfer,
            .object = mem.zeroInit(Transfer, .{
                .id = 101,
                .debit_account_id = 10,
                .credit_account_id = 20,
                .user_data = 30,
                .timeout = 50,
                .ledger = 60,
                .code = 70,
                .flags = .{
                    .void_pending_transfer = true,
                },
                .amount = 80,
                .timestamp = timestamp + 1,
            }),
        },
        .{
            .result = .pending_id_must_not_be_zero,
            .object = mem.zeroInit(Transfer, .{
                .id = 101,
                .debit_account_id = 10,
                .credit_account_id = 20,
                .user_data = 30,
                .pending_id = 0,
                .ledger = 60,
                .code = 70,
                .flags = .{
                    .void_pending_transfer = true,
                },
                .amount = 80,
                .timestamp = timestamp + 1,
            }),
        },
        .{
            .result = .pending_id_must_be_different,
            .object = mem.zeroInit(Transfer, .{
                .id = 101,
                .debit_account_id = 10,
                .credit_account_id = 20,
                .user_data = 30,
                .pending_id = 101,
                .ledger = 60,
                .code = 70,
                .flags = .{
                    .void_pending_transfer = true,
                },
                .amount = 80,
                .timestamp = timestamp + 1,
            }),
        },
        .{
            .result = .pending_transfer_not_found,
            .object = mem.zeroInit(Transfer, .{
                .id = 101,
                .debit_account_id = 10,
                .credit_account_id = 20,
                .user_data = 30,
                .pending_id = 102,
                .ledger = 60,
                .code = 70,
                .flags = .{
                    .void_pending_transfer = true,
                },
                .amount = 80,
                .timestamp = timestamp + 1,
            }),
        },
        .{
            .result = .pending_transfer_not_pending,
            .object = mem.zeroInit(Transfer, .{
                .id = 101,
                .debit_account_id = 10,
                .credit_account_id = 20,
                .user_data = 30,
                .pending_id = 1,
                .ledger = 60,
                .code = 70,
                .flags = .{
                    .void_pending_transfer = true,
                },
                .amount = 80,
                .timestamp = timestamp + 1,
            }),
        },
        .{
            .result = .pending_transfer_has_different_debit_account_id,
            .object = mem.zeroInit(Transfer, .{
                .id = 101,
                .debit_account_id = 10,
                .credit_account_id = 20,
                .user_data = 30,
                .pending_id = 2,
                .ledger = 60,
                .code = 70,
                .flags = .{
                    .void_pending_transfer = true,
                },
                .amount = 80,
                .timestamp = timestamp + 1,
            }),
        },
        .{
            .result = .pending_transfer_has_different_credit_account_id,
            .object = mem.zeroInit(Transfer, .{
                .id = 101,
                .debit_account_id = 1,
                .credit_account_id = 20,
                .user_data = 30,
                .pending_id = 2,
                .ledger = 60,
                .code = 70,
                .flags = .{
                    .void_pending_transfer = true,
                },
                .amount = 80,
                .timestamp = timestamp + 1,
            }),
        },
        .{
            .result = .pending_transfer_has_different_ledger,
            .object = mem.zeroInit(Transfer, .{
                .id = 101,
                .debit_account_id = 1,
                .credit_account_id = 2,
                .user_data = 30,
                .pending_id = 2,
                .ledger = 60,
                .code = 70,
                .flags = .{
                    .void_pending_transfer = true,
                },
                .amount = 80,
                .timestamp = timestamp + 1,
            }),
        },
        .{
            .result = .pending_transfer_has_different_code,
            .object = mem.zeroInit(Transfer, .{
                .id = 101,
                .debit_account_id = 1,
                .credit_account_id = 2,
                .user_data = 30,
                .pending_id = 2,
                .ledger = 1,
                .code = 70,
                .flags = .{
                    .void_pending_transfer = true,
                },
                .amount = 80,
                .timestamp = timestamp + 1,
            }),
        },
        .{
            .result = .exceeds_pending_transfer_amount,
            .object = mem.zeroInit(Transfer, .{
                .id = 101,
                .debit_account_id = 1,
                .credit_account_id = 2,
                .user_data = 7000,
                .pending_id = 2,
                .ledger = 1,
                .code = 1,
                .flags = .{
                    .void_pending_transfer = true,
                },
                .amount = 80,
                .timestamp = timestamp + 1,
            }),
        },
        .{
            .result = .pending_transfer_has_different_amount,
            .object = mem.zeroInit(Transfer, .{
                .id = 101,
                .debit_account_id = 1,
                .credit_account_id = 2,
                .user_data = 7000,
                .pending_id = 2,
                .ledger = 1,
                .code = 1,
                .flags = .{
                    .void_pending_transfer = true,
                },
                .amount = 1,
                .timestamp = timestamp + 1,
            }),
        },
        .{
            .result = .exists_with_different_flags,
            .object = mem.zeroInit(Transfer, .{
                .id = 101,
                .debit_account_id = 0,
                .credit_account_id = 0,
                .user_data = 7000,
                .pending_id = 3,
                .ledger = 0,
                .code = 0,
                .flags = .{
                    .void_pending_transfer = true,
                },
                .amount = 15,
                .timestamp = timestamp + 1,
            }),
        },
        .{
            .result = .exists_with_different_pending_id,
            .object = mem.zeroInit(Transfer, .{
                .id = 101,
                .debit_account_id = 1,
                .credit_account_id = 2,
                .user_data = 7000,
                .pending_id = 3,
                .ledger = 1,
                .code = 1,
                .flags = .{
                    .post_pending_transfer = true,
                },
                .amount = 14,
                .timestamp = timestamp + 1,
            }),
        },
        .{
            .result = .exists_with_different_user_data,
            .object = mem.zeroInit(Transfer, .{
                .id = 101,
                .debit_account_id = 1,
                .credit_account_id = 2,
                .user_data = 7000,
                .pending_id = 2,
                .ledger = 1,
                .code = 1,
                .flags = .{
                    .post_pending_transfer = true,
                },
                .amount = 14,
                .timestamp = timestamp + 1,
            }),
        },
        .{
            .result = .exists_with_different_user_data,
            .object = mem.zeroInit(Transfer, .{
                .id = 101,
                .debit_account_id = 1,
                .credit_account_id = 2,
                .user_data = 0,
                .pending_id = 2,
                .ledger = 1,
                .code = 1,
                .flags = .{
                    .post_pending_transfer = true,
                },
                .amount = 14,
                .timestamp = timestamp + 1,
            }),
        },
        .{
            .result = .exists_with_different_amount,
            .object = mem.zeroInit(Transfer, .{
                .id = 101,
                .debit_account_id = 1,
                .credit_account_id = 2,
                .user_data = 1000,
                .pending_id = 2,
                .ledger = 1,
                .code = 1,
                .flags = .{
                    .post_pending_transfer = true,
                },
                .amount = 14,
                .timestamp = timestamp + 1,
            }),
        },
        .{
            .result = .exists_with_different_amount,
            .object = mem.zeroInit(Transfer, .{
                .id = 101,
                .debit_account_id = 1,
                .credit_account_id = 2,
                .user_data = 1000,
                .pending_id = 2,
                .ledger = 1,
                .code = 1,
                .flags = .{
                    .post_pending_transfer = true,
                },
                .amount = 0,
                .timestamp = timestamp + 1,
            }),
        },
        .{
            .result = .exists,
            .object = mem.zeroInit(Transfer, .{
                .id = 101,
                .debit_account_id = 1,
                .credit_account_id = 2,
                .user_data = 1000,
                .pending_id = 2,
                .ledger = 1,
                .code = 1,
                .flags = .{
                    .post_pending_transfer = true,
                },
                .amount = 13,
                .timestamp = timestamp + 1,
            }),
        },
        .{
            .result = .pending_transfer_already_posted,
            .object = mem.zeroInit(Transfer, .{
                .id = 102,
                .debit_account_id = 1,
                .credit_account_id = 2,
                .user_data = 1000,
                .pending_id = 2,
                .ledger = 1,
                .code = 1,
                .flags = .{
                    .post_pending_transfer = true,
                },
                .amount = 13,
                .timestamp = timestamp + 1,
            }),
        },
        .{
            .result = .ok,
            .object = mem.zeroInit(Transfer, .{
                .id = 103,
                .debit_account_id = 0,
                .credit_account_id = 0,
                .user_data = 0,
                .pending_id = 3,
                .ledger = 0,
                .code = 0,
                .flags = .{ .void_pending_transfer = true },
                .amount = 15,
                .timestamp = timestamp + 1,
            }),
        },
        .{
            .result = .pending_transfer_already_voided,
            .object = mem.zeroInit(Transfer, .{
                .id = 102,
                .debit_account_id = 1,
                .credit_account_id = 2,
                .user_data = 1000,
                .pending_id = 3,
                .ledger = 1,
                .code = 1,
                .flags = .{
                    .post_pending_transfer = true,
                },
                .amount = 13,
                .timestamp = timestamp + 2,
            }),
        },
        .{
            .result = .pending_transfer_expired,
            .object = mem.zeroInit(Transfer, .{
                .id = 102,
                .debit_account_id = 1,
                .credit_account_id = 2,
                .user_data = 4000,
                .pending_id = 4,
                .ledger = 1,
                .code = 1,
                .flags = .{
                    .void_pending_transfer = true,
                },
                .amount = 15,
                .timestamp = timestamp + 2,
            }),
        },
        .{
            .result = .ok,
            .object = mem.zeroInit(Transfer, .{
                .id = 105,
                .debit_account_id = 0,
                .credit_account_id = 0,
                .user_data = 0,
                .pending_id = 5,
                .ledger = 0,
                .code = 0,
                .flags = .{ .post_pending_transfer = true },
                .amount = 0,
                .timestamp = timestamp + 2,
            }),
        },
    };

    for (vectors) |vector, i| {
        const result = state_machine.create_transfer(&vector.object);
        expectEqual(vector.result, result) catch |err| {
            print_test_vector(i, vector.result, result, vector.object, err);
            return err;
        };

        if (vector.result == .ok) {
            // Test that posted values inherit from the pending or posting transfer:
            const pending = state_machine.get_transfer(vector.object.pending_id).?.*;
            const posted = state_machine.get_transfer(vector.object.id).?.*;

            const user_data = if (vector.object.user_data == 0)
                pending.user_data
            else
                vector.object.user_data;

            const amount = if (vector.object.amount == 0)
                pending.amount
            else
                vector.object.amount;

            const expected = Transfer{
                .id = vector.object.id,
                .debit_account_id = pending.debit_account_id,
                .credit_account_id = pending.credit_account_id,
                .user_data = user_data,
                .reserved = 0,
                .pending_id = pending.id,
                .timeout = 0,
                .ledger = pending.ledger,
                .code = pending.code,
                .flags = vector.object.flags,
                .amount = amount,
                .timestamp = vector.object.timestamp,
            };
            expectEqual(expected, posted) catch |err| {
                print_test_vector(
                    i,
                    expected,
                    posted,
                    vector.object,
                    err,
                );
                return err;
            };
        }
    }

    // Balances after posting:
    try test_account_balances(&state_machine, 1, 15, 35, 0, 0);
    try test_account_balances(&state_machine, 2, 0, 0, 15, 35);

    // Rollback posting transfer (different amount):
    assert(vectors[0].result == .ok);
    try test_transfer_rollback(&state_machine, &vectors[0].object);
    try test_account_balances(&state_machine, 1, 30, 22, 0, 0);
    try test_account_balances(&state_machine, 2, 0, 0, 30, 22);

    // Rollback voiding transfer:
    assert(vectors[22].result == .ok);
    try test_transfer_rollback(&state_machine, &vectors[22].object);
    try test_account_balances(&state_machine, 1, 45, 22, 0, 0);
    try test_account_balances(&state_machine, 2, 0, 0, 45, 22);

    // Rollback posting transfer (zero amount):
    assert(vectors[25].result == .ok);
    try test_transfer_rollback(&state_machine, &vectors[25].object);
    try test_account_balances(&state_machine, 1, 52, 15, 0, 0);
    try test_account_balances(&state_machine, 2, 0, 0, 52, 15);

    // Rollback all pending transfers:
    try test_transfer_rollback(&state_machine, &transfers[1]);
    try test_account_balances(&state_machine, 1, 37, 15, 0, 0);
    try test_account_balances(&state_machine, 2, 0, 0, 37, 15);

    try test_transfer_rollback(&state_machine, &transfers[2]);
    try test_account_balances(&state_machine, 1, 22, 15, 0, 0);
    try test_account_balances(&state_machine, 2, 0, 0, 22, 15);

    try test_transfer_rollback(&state_machine, &transfers[3]);
    try test_account_balances(&state_machine, 1, 7, 15, 0, 0);
    try test_account_balances(&state_machine, 2, 0, 0, 7, 15);

    try test_transfer_rollback(&state_machine, &transfers[4]);
    try test_account_balances(&state_machine, 1, 0, 15, 0, 0);
    try test_account_balances(&state_machine, 2, 0, 0, 0, 15);

    // Rollback transfer:
    try test_transfer_rollback(&state_machine, &transfers[0]);
    try test_account_balances(&state_machine, 1, 0, 0, 0, 0);
    try test_account_balances(&state_machine, 2, 0, 0, 0, 0);
}

fn print_test_vector(
    i: usize,
    result_expect: anytype,
    result_actual: anytype,
    vector_object: anytype,
    err: anyerror,
) void {
    std.debug.print("\nindex={}\n\nexpect: {}\n\nactual: {}\n\nobject: {}\n\nerr={}", .{
        i,
        result_expect,
        result_actual,
        vector_object,
        err,
    });
}

fn test_account_balances(
    state_machine: *const StateMachine,
    account_id: u128,
    debits_pending: u64,
    debits_posted: u64,
    credits_pending: u64,
    credits_posted: u64,
) !void {
    const account = state_machine.get_account(account_id).?.*;
    try expectEqual(debits_pending, account.debits_pending);
    try expectEqual(debits_posted, account.debits_posted);
    try expectEqual(credits_pending, account.credits_pending);
    try expectEqual(credits_posted, account.credits_posted);
}

fn test_transfer_rollback(state_machine: *StateMachine, transfer: *const Transfer) !void {
    assert(state_machine.get_transfer(transfer.id) != null);

    state_machine.create_transfer_rollback(transfer);

    try expect(state_machine.get_transfer(transfer.id) == null);
    if (transfer.flags.post_pending_transfer or transfer.flags.void_pending_transfer) {
        try expect(state_machine.get_posted(transfer.pending_id) == null);
    }
}

test "zeroed_32_bytes" {
    try test_zeroed_n_bytes(32);
}

test "zeroed_48_bytes" {
    try test_zeroed_n_bytes(48);
}

fn test_zeroed_n_bytes(comptime n: usize) !void {
    const routine = switch (n) {
        32 => zeroed_32_bytes,
        48 => zeroed_48_bytes,
        else => unreachable,
    };
    var a = [_]u8{0} ** n;
    var i: usize = 0;
    while (i < a.len) : (i += 1) {
        a[i] = 1;
        try expectEqual(false, routine(a));
        a[i] = 0;
    }
    try expectEqual(true, routine(a));
}

test "equal_32_bytes" {
    try test_equal_n_bytes(32);
}

test "equal_48_bytes" {
    try test_equal_n_bytes(48);
}

fn test_equal_n_bytes(comptime n: usize) !void {
    const routine = switch (n) {
        32 => equal_32_bytes,
        48 => equal_48_bytes,
        else => unreachable,
    };
    var a = [_]u8{0} ** n;
    var b = [_]u8{0} ** n;
    var i: usize = 0;
    while (i < a.len) : (i += 1) {
        a[i] = 1;
        try expectEqual(false, routine(a, b));
        a[i] = 0;

        b[i] = 1;
        try expectEqual(false, routine(a, b));
        b[i] = 0;
    }
    try expectEqual(true, routine(a, b));
}
