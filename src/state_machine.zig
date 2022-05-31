const std = @import("std");
const assert = std.debug.assert;
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
        init,
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

        // TODO After recovery, set prepare_timestamp max(wall clock, op timestamp).
        // TODO After recovery, set commit_timestamp max(wall clock, commit timestamp).

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

    pub fn prepare(self: *StateMachine, realtime: i64, operation: Operation, input: []u8) void {
        switch (operation) {
            .init => unreachable,
            .register => {},
            .create_accounts => self.prepare_timestamps(realtime, .create_accounts, input),
            .create_transfers => self.prepare_timestamps(realtime, .create_transfers, input),
            .lookup_accounts => {},
            .lookup_transfers => {},
            else => unreachable,
        }
    }

    fn prepare_timestamps(
        self: *StateMachine,
        realtime: i64,
        comptime operation: Operation,
        input: []u8,
    ) void {
        // Guard against the wall clock going backwards by taking the max with timestamps issued:
        self.prepare_timestamp = std.math.max(
            // The cluster `commit_timestamp` may be ahead of our `prepare_timestamp` because this
            // may be our first prepare as a recently elected leader:
            std.math.max(self.prepare_timestamp, self.commit_timestamp) + 1,
            @intCast(u64, realtime),
        );
        assert(self.prepare_timestamp > self.commit_timestamp);
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
            .init => unreachable,
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

        // Opening balances may never exceed limits:
        if (a.debits_exceed_credits(0)) return .exceeds_credits;
        if (a.credits_exceed_debits(0)) return .exceeds_debits;

        var insert = self.accounts.getOrPutAssumeCapacity(a.id);
        if (insert.found_existing) {
            const existing = insert.value_ptr.*;
            if (a.user_data != existing.user_data) return .exists_with_different_user_data;
            if (a.ledger != existing.ledger) return .exists_with_different_ledger;
            if (a.code != existing.code) return .exists_with_different_code;
            if (@bitCast(u16, a.flags) != @bitCast(u16, existing.flags)) {
                return .exists_with_different_flags;
            }
            return .exists;
        } else {
            insert.value_ptr.* = a.*;
            self.commit_timestamp = a.timestamp;
            return .ok;
        }
    }

    fn create_account_rollback(self: *StateMachine, a: *const Account) void {
        assert(self.accounts.remove(a.id));
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

        if (t.flags.pending) {
            if (overflow(t.amount, dr.debits_pending)) return .overflows_debits_pending;
            if (overflow(t.amount, cr.credits_pending)) return .overflows_credits_pending;
        }
        if (overflow(t.amount, dr.debits_posted)) return .overflows_debits_posted;
        if (overflow(t.amount, cr.credits_posted)) return .overflows_credits_posted;
        if (overflow(t.amount, dr.debits_pending + dr.debits_posted)) return .overflows_debits;
        if (overflow(t.amount, cr.credits_pending + cr.credits_posted)) return .overflows_credits;

        if (dr.debits_exceed_credits(t.amount)) return .exceeds_credits;
        if (cr.credits_exceed_debits(t.amount)) return .exceeds_debits;

        const insert = self.transfers.getOrPutAssumeCapacity(t.id);
        if (insert.found_existing) return transfer_exists(t, insert.value_ptr);

        insert.value_ptr.* = t.*;
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

    fn post_or_void_pending_transfer(self: *StateMachine, t: *const Transfer) CreateTransferResult {
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

        const dr = self.get_account(p.debit_account_id) orelse return .debit_account_not_found;
        const cr = self.get_account(p.credit_account_id) orelse return .credit_account_not_found;
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

        if (self.get_transfer(t.id)) |exists| return transfer_exists(t, exists);

        if (self.get_posted(t.pending_id)) |posted| {
            if (posted) return .pending_transfer_already_posted;
            return .pending_transfer_already_voided;
        }

        assert(p.timestamp < t.timestamp);
        assert(p.timeout > 0);
        if (p.timestamp + p.timeout <= t.timestamp) return .pending_transfer_expired;

        const insert = self.transfers.getOrPutAssumeCapacity(t.id);
        assert(!insert.found_existing);

        insert.value_ptr.* = Transfer{
            .id = t.id,
            .debit_account_id = p.debit_account_id,
            .credit_account_id = p.credit_account_id,
            .user_data = p.user_data,
            .reserved = p.reserved,
            .ledger = p.ledger,
            .code = p.code,
            .pending_id = t.pending_id,
            .timeout = t.timeout,
            .timestamp = t.timestamp,
            .flags = t.flags,
            .amount = amount,
        };

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

    fn transfer_exists(t: *const Transfer, e: *const Transfer) CreateTransferResult {
        if (t.debit_account_id != e.debit_account_id) {
            return .exists_with_different_debit_account_id;
        }
        if (t.credit_account_id != e.credit_account_id) {
            return .exists_with_different_credit_account_id;
        }
        if (t.user_data != e.user_data) return .exists_with_different_user_data;
        if (t.pending_id != e.pending_id) return .exists_with_different_pending_id;
        if (t.timeout != e.timeout) return .exists_with_different_timeout;
        if (t.ledger != e.ledger) return .exists_with_different_ledger;
        if (t.code != e.code) return .exists_with_different_code;
        if (@bitCast(u16, t.flags) != @bitCast(u16, e.flags)) return .exists_with_different_flags;
        if (t.amount != e.amount) return .exists_with_different_amount;

        return .exists;
    }

    /// This is our core private method for changing balances.
    /// Returns a live pointer to an Account in the accounts hash map.
    /// This is intended to lookup an Account and modify balances directly by reference.
    /// This pointer is invalidated if the hash map is resized by another insert, e.g. if we get a
    /// pointer, insert another account without capacity, and then modify this pointer... BOOM!
    /// This is a sharp tool but replaces a lookup, copy and update with a single lookup.
    fn get_account(self: *StateMachine, id: u128) ?*Account {
        return self.accounts.getPtr(id);
    }

    /// See the comment for get_account().
    fn get_transfer(self: *StateMachine, id: u128) ?*Transfer {
        return self.transfers.getPtr(id);
    }

    /// Returns whether a pending transfer, if it exists, has already been posted or voided.
    fn get_posted(self: *StateMachine, pending_id: u128) ?bool {
        return self.posted.get(pending_id);
    }
};

fn overflow(a: u64, b: u64) bool {
    var c: u64 = undefined;
    return @addWithOverflow(u64, a, b, &c);
}

test "overflow" {
    const expectEqual = std.testing.expectEqual;

    try expectEqual(false, overflow(std.math.maxInt(u64), 0));
    try expectEqual(false, overflow(std.math.maxInt(u64) - 1, 1));
    try expectEqual(false, overflow(1, std.math.maxInt(u64) - 1));

    try expectEqual(true, overflow(std.math.maxInt(u64), 1));
    try expectEqual(true, overflow(1, std.math.maxInt(u64)));

    try expectEqual(true, overflow(std.math.maxInt(u64), std.math.maxInt(u64)));
    try expectEqual(true, overflow(std.math.maxInt(u64), std.math.maxInt(u64)));
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

test "create/lookup accounts" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    const Vector = struct { result: CreateAccountResult, object: Account };

    const vectors = [_]Vector{
        Vector{
            .result = .reserved_flag,
            .object = mem.zeroInit(Account, .{
                .id = 1,
                .timestamp = 1,
                .flags = .{ .padding = 1 },
            }),
        },
        Vector{
            .result = .reserved_field,
            .object = mem.zeroInit(Account, .{
                .id = 2,
                .timestamp = 1,
                .reserved = [_]u8{1} ** 48,
            }),
        },
        Vector{
            .result = .exceeds_credits,
            .object = mem.zeroInit(Account, .{
                .id = 3,
                .timestamp = 1,
                .debits_pending = 10,
                .flags = .{ .debits_must_not_exceed_credits = true },
            }),
        },
        Vector{
            .result = .exceeds_credits,
            .object = mem.zeroInit(Account, .{
                .id = 4,
                .timestamp = 1,
                .debits_posted = 10,
                .flags = .{ .debits_must_not_exceed_credits = true },
            }),
        },
        Vector{
            .result = .exceeds_debits,
            .object = mem.zeroInit(Account, .{
                .id = 5,
                .timestamp = 1,
                .credits_pending = 10,
                .flags = .{ .credits_must_not_exceed_debits = true },
            }),
        },
        Vector{
            .result = .exceeds_debits,
            .object = mem.zeroInit(Account, .{
                .id = 6,
                .timestamp = 1,
                .credits_posted = 10,
                .flags = .{ .credits_must_not_exceed_debits = true },
            }),
        },
        Vector{
            .result = .ok,
            .object = mem.zeroInit(Account, .{
                .id = 7,
                .timestamp = 1,
            }),
        },
        Vector{
            .result = .exists,
            .object = mem.zeroInit(Account, .{
                .id = 7,
                .timestamp = 2,
            }),
        },
        Vector{
            .result = .ok,
            .object = mem.zeroInit(Account, .{
                .id = 8,
                .timestamp = 2,
                .user_data = 'U',
                .ledger = 9,
            }),
        },
        Vector{
            .result = .exists_with_different_ledger,
            .object = mem.zeroInit(Account, .{
                .id = 8,
                .timestamp = 3,
                .user_data = 'U',
                .ledger = 10,
            }),
        },
        Vector{
            .result = .ok,
            .object = mem.zeroInit(Account, .{
                .id = 9,
                .timestamp = 3,
                .code = 9,
                .user_data = 'U',
            }),
        },
        Vector{
            .result = .exists_with_different_code,
            .object = mem.zeroInit(Account, .{
                .id = 9,
                .timestamp = 4,
                .code = 10,
                .user_data = 'D',
            }),
        },
        Vector{
            .result = .ok,
            .object = mem.zeroInit(Account, .{
                .id = 10,
                .timestamp = 4,
                .flags = .{ .credits_must_not_exceed_debits = true },
            }),
        },
        Vector{
            .result = .exists_with_different_flags,
            .object = mem.zeroInit(Account, .{
                .id = 10,
                .timestamp = 5,
                .flags = .{ .debits_must_not_exceed_credits = true },
            }),
        },
        Vector{
            .result = .ok,
            .object = mem.zeroInit(Account, .{
                .id = 11,
                .timestamp = 5,
                .user_data = 'U',
            }),
        },
        Vector{
            .result = .exists_with_different_user_data,
            .object = mem.zeroInit(Account, .{
                .id = 11,
                .timestamp = 6,
                .user_data = 'D',
            }),
        },
    };

    var state_machine = try StateMachine.init(allocator, vectors.len, 0, 0);
    defer state_machine.deinit();

    for (vectors) |vector| {
        try testing.expectEqual(vector.result, state_machine.create_account(&vector.object));
        if (vector.result == .ok) {
            try testing.expectEqual(vector.object, state_machine.get_account(vector.object.id).?.*);
        }
    }
}

test "linked accounts" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    const accounts_max = 5;
    const transfers_max = 0;
    const transfers_pending_max = 0;

    var accounts = [_]Account{
        // An individual event (successful):
        mem.zeroInit(Account, .{ .id = 7, .code = 1, .ledger = 1 }),

        // A chain of 4 events (the last event in the chain closes the chain with linked=false):
        // Commit/rollback:
        mem.zeroInit(Account, .{ .id = 1, .code = 1, .ledger = 1, .flags = .{ .linked = true } }),
        // Commit/rollback:
        mem.zeroInit(Account, .{ .id = 2, .code = 1, .ledger = 1, .flags = .{ .linked = true } }),
        // Fail with .exists:
        mem.zeroInit(Account, .{ .id = 1, .code = 1, .ledger = 1, .flags = .{ .linked = true } }),
        // Fail without committing.
        mem.zeroInit(Account, .{ .id = 3, .code = 1, .ledger = 1 }),

        // An individual event (successful):
        // This should not see any effect from the failed chain above:
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
        allocator,
        accounts_max,
        transfers_max,
        transfers_pending_max,
    );
    defer state_machine.deinit();

    const input = mem.asBytes(&accounts);
    const output = try allocator.alloc(u8, 4096);

    state_machine.prepare(0, .create_accounts, input);
    const size = state_machine.commit(0, .create_accounts, input, output);
    const results = mem.bytesAsSlice(CreateAccountsResult, output[0..size]);

    try testing.expectEqualSlices(
        CreateAccountsResult,
        &[_]CreateAccountsResult{
            CreateAccountsResult{ .index = 1, .result = .linked_event_failed },
            CreateAccountsResult{ .index = 2, .result = .linked_event_failed },
            CreateAccountsResult{ .index = 3, .result = .exists },
            CreateAccountsResult{ .index = 4, .result = .linked_event_failed },
            CreateAccountsResult{ .index = 6, .result = .exists_with_different_code },
            CreateAccountsResult{ .index = 7, .result = .linked_event_failed },
            CreateAccountsResult{ .index = 9, .result = .linked_event_failed },
            CreateAccountsResult{ .index = 10, .result = .exists_with_different_ledger },
        },
        results,
    );

    try testing.expectEqual(accounts[0], state_machine.get_account(accounts[0].id).?.*);
    try testing.expectEqual(accounts[5], state_machine.get_account(accounts[5].id).?.*);
    try testing.expectEqual(accounts[8], state_machine.get_account(accounts[8].id).?.*);
    try testing.expectEqual(accounts[11], state_machine.get_account(accounts[11].id).?.*);
    try testing.expectEqual(accounts[12], state_machine.get_account(accounts[12].id).?.*);
    try testing.expectEqual(@as(u32, 5), state_machine.accounts.count());

    // TODO How can we test that events were in fact rolled back in LIFO order?
    // All our rollback handlers appear to be commutative.
}

test "create/lookup/rollback transfers" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    var accounts = [_]Account{
        mem.zeroInit(Account, .{ .id = 1 }),
        mem.zeroInit(Account, .{ .id = 2 }),
        mem.zeroInit(Account, .{ .id = 3, .ledger = 1 }),
        mem.zeroInit(Account, .{ .id = 4, .ledger = 2 }),
        mem.zeroInit(Account, .{ .id = 5, .credits_posted = 1000, .debits_posted = 500, .flags = .{ .debits_must_not_exceed_credits = true } }),
        mem.zeroInit(Account, .{ .id = 6, .credits_posted = 500, .debits_posted = 1000, .flags = .{ .credits_must_not_exceed_debits = true } }),
        mem.zeroInit(Account, .{ .id = 7 }),
        mem.zeroInit(Account, .{ .id = 8 }),
    };

    var state_machine = try StateMachine.init(allocator, accounts.len, 1, 0);
    defer state_machine.deinit();

    const input = mem.asBytes(&accounts);
    const output = try allocator.alloc(u8, 4096);

    state_machine.prepare(0, .create_accounts, input);
    const size = state_machine.commit(0, .create_accounts, input, output);

    const errors = mem.bytesAsSlice(CreateAccountsResult, output[0..size]);
    try testing.expectEqual(@as(usize, 0), errors.len);

    for (accounts) |account| {
        try testing.expectEqual(account, state_machine.get_account(account.id).?.*);
    }

    const Vector = struct { result: CreateTransferResult, object: Transfer };

    const timestamp: u64 = (state_machine.commit_timestamp + 1);
    const vectors = [_]Vector{
        Vector{
            .result = .amount_must_not_be_zero,
            .object = mem.zeroInit(Transfer, .{
                .id = 1,
                .timestamp = timestamp,
            }),
        },
        Vector{
            .result = .reserved_flag,
            .object = mem.zeroInit(Transfer, .{
                .id = 2,
                .timestamp = timestamp,
                .flags = .{ .padding = 1 },
            }),
        },
        Vector{
            .result = .pending_transfer_must_timeout,
            .object = mem.zeroInit(Transfer, .{
                .id = 3,
                .timestamp = timestamp,
                .flags = .{ .pending = true },
            }),
        },
        Vector{
            .result = .timeout_reserved_for_pending_transfer,
            .object = mem.zeroInit(Transfer, .{
                .id = 4,
                .timestamp = timestamp,
                .timeout = 1,
            }),
        },
        Vector{
            .result = .accounts_must_be_different,
            .object = mem.zeroInit(Transfer, .{
                .id = 6,
                .timestamp = timestamp,
                .amount = 10,
                .debit_account_id = 1,
                .credit_account_id = 1,
            }),
        },
        Vector{
            .result = .debit_account_not_found,
            .object = mem.zeroInit(Transfer, .{
                .id = 7,
                .timestamp = timestamp,
                .amount = 10,
                .debit_account_id = 100,
                .credit_account_id = 1,
            }),
        },
        Vector{
            .result = .credit_account_not_found,
            .object = mem.zeroInit(Transfer, .{
                .id = 8,
                .timestamp = timestamp,
                .amount = 10,
                .debit_account_id = 1,
                .credit_account_id = 100,
            }),
        },
        Vector{
            .result = .accounts_must_have_the_same_ledger,
            .object = mem.zeroInit(Transfer, .{
                .id = 9,
                .timestamp = timestamp,
                .amount = 10,
                .debit_account_id = 3,
                .credit_account_id = 4,
            }),
        },
        Vector{
            .result = .overflows_credits,
            .object = mem.zeroInit(Transfer, .{
                .id = 10,
                .timestamp = timestamp,
                .amount = std.math.maxInt(u64),
                .debit_account_id = 1,
                .credit_account_id = 6,
            }),
        },
        Vector{
            .result = .overflows_debits_posted,
            .object = mem.zeroInit(Transfer, .{
                .id = 10,
                .timestamp = timestamp,
                .amount = std.math.maxInt(u64),
                .debit_account_id = 5,
                .credit_account_id = 1,
            }),
        },
        Vector{
            .result = .exceeds_credits,
            .object = mem.zeroInit(Transfer, .{
                .id = 10,
                .timestamp = timestamp,
                .amount = 1000,
                .debit_account_id = 5,
                .credit_account_id = 1,
            }),
        },
        Vector{
            .result = .exceeds_debits,
            .object = mem.zeroInit(Transfer, .{
                .id = 11,
                .timestamp = timestamp,
                .amount = 1000,
                .debit_account_id = 1,
                .credit_account_id = 6,
            }),
        },
        Vector{
            .result = .ok,
            .object = mem.zeroInit(Transfer, .{
                .id = 12,
                .timestamp = timestamp,
                .amount = 10,
                .debit_account_id = 7,
                .credit_account_id = 8,
            }),
        },
        Vector{
            .result = .exists,
            .object = mem.zeroInit(Transfer, .{
                .id = 12,
                .timestamp = timestamp + 1,
                .amount = 10,
                .debit_account_id = 7,
                .credit_account_id = 8,
            }),
        },
        Vector{
            .result = .exists_with_different_debit_account_id,
            .object = mem.zeroInit(Transfer, .{
                .id = 12,
                .timestamp = timestamp + 1,
                .amount = 10,
                .debit_account_id = 8,
                .credit_account_id = 7,
            }),
        },
        Vector{
            .result = .exists_with_different_credit_account_id,
            .object = mem.zeroInit(Transfer, .{
                .id = 12,
                .timestamp = timestamp + 1,
                .amount = 10,
                .debit_account_id = 7,
                .credit_account_id = 1,
            }),
        },
        Vector{
            .result = .exists_with_different_amount,
            .object = mem.zeroInit(Transfer, .{
                .id = 12,
                .timestamp = timestamp + 1,
                .amount = 11,
                .debit_account_id = 7,
                .credit_account_id = 8,
            }),
        },
        Vector{
            .result = .exists_with_different_flags,
            .object = mem.zeroInit(Transfer, .{
                .id = 12,
                .timestamp = timestamp + 1,
                .timeout = 1234567,
                .amount = 10,
                .debit_account_id = 7,
                .credit_account_id = 8,
                .flags = .{
                    .pending = true,
                },
            }),
        },
        Vector{
            .result = .exists_with_different_user_data,
            .object = mem.zeroInit(Transfer, .{
                .id = 12,
                .timestamp = timestamp + 1,
                .amount = 10,
                .debit_account_id = 7,
                .credit_account_id = 8,
                .user_data = 'A',
            }),
        },
        Vector{
            .result = .ok,
            .object = mem.zeroInit(Transfer, .{
                .id = 13,
                .timestamp = timestamp + 1,
                .amount = 10,
                .debit_account_id = 7,
                .credit_account_id = 8,
                .reserved = 0,
            }),
        },
        Vector{
            .result = .timeout_reserved_for_pending_transfer,
            .object = mem.zeroInit(Transfer, .{
                .id = 13,
                .timestamp = timestamp + 2,
                .amount = 10,
                .debit_account_id = 7,
                .credit_account_id = 8,
                .reserved = 0,
                .timeout = 10,
            }),
        },
        Vector{
            .result = .pending_transfer_must_timeout,
            .object = mem.zeroInit(Transfer, .{
                .id = 14,
                .user_data = 14,
                .timestamp = timestamp + 2,
                .amount = 10,
                .debit_account_id = 7,
                .credit_account_id = 8,
                .flags = .{ .pending = true },
                .timeout = 0,
            }),
        },
        Vector{
            .result = .ok,
            .object = mem.zeroInit(Transfer, .{
                .id = 15,
                .timestamp = timestamp + 2,
                .amount = 10,
                .debit_account_id = 7,
                .credit_account_id = 8,
                .flags = .{ .pending = true },
                .timeout = 20,
            }),
        },
        Vector{
            .result = .exists_with_different_timeout,
            .object = mem.zeroInit(Transfer, .{
                .id = 15,
                .timestamp = timestamp + 3,
                .amount = 10,
                .debit_account_id = 7,
                .credit_account_id = 8,
                .flags = .{ .pending = true },
                .timeout = 25,
            }),
        },
    };

    for (vectors) |vector, i| {
        const result = state_machine.create_transfer(&vector.object);
        testing.expectEqual(vector.result, result) catch |err| {
            test_debug_vector_create(Vector, CreateTransferResult, i, vector, result, err);
            return err;
        };
        // Fetch the successfully created transfer to ensure it is persisted correctly:
        if (vector.result == .ok) {
            try testing.expectEqual(vector.object, state_machine.get_transfer(vector.object.id).?.*);
        }
    }

    // 2 phase commit [pending]:
    try testing.expectEqual(@as(u64, 10), state_machine.get_account(7).?.*.debits_pending);
    try testing.expectEqual(@as(u64, 0), state_machine.get_account(7).?.*.credits_pending);
    try testing.expectEqual(@as(u64, 10), state_machine.get_account(8).?.*.credits_pending);
    try testing.expectEqual(@as(u64, 0), state_machine.get_account(8).?.*.debits_pending);
    // 1 phase commit [posted]:
    try testing.expectEqual(@as(u64, 20), state_machine.get_account(7).?.*.debits_posted);
    try testing.expectEqual(@as(u64, 0), state_machine.get_account(7).?.*.credits_posted);
    try testing.expectEqual(@as(u64, 20), state_machine.get_account(8).?.*.credits_posted);
    try testing.expectEqual(@as(u64, 0), state_machine.get_account(8).?.*.debits_posted);

    // Rollback transfer with id [12], amount of 10:
    state_machine.create_transfer_rollback(state_machine.get_transfer(vectors[12].object.id).?);
    try testing.expectEqual(@as(u64, 10), state_machine.get_account(7).?.*.debits_posted);
    try testing.expectEqual(@as(u64, 0), state_machine.get_account(7).?.*.credits_posted);
    try testing.expectEqual(@as(u64, 10), state_machine.get_account(8).?.*.credits_posted);
    try testing.expectEqual(@as(u64, 0), state_machine.get_account(8).?.*.debits_posted);
    try testing.expect(state_machine.get_transfer(vectors[11].object.id) == null);

    // Rollback transfer with id [15], amount of 10:
    state_machine.create_transfer_rollback(state_machine.get_transfer(vectors[23].object.id).?);
    try testing.expectEqual(@as(u64, 0), state_machine.get_account(7).?.*.debits_pending);
    try testing.expectEqual(@as(u64, 0), state_machine.get_account(8).?.*.credits_pending);
    try testing.expect(state_machine.get_transfer(vectors[22].object.id) == null);
}

test "create/lookup/rollback 2-phase transfers" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    var accounts = [_]Account{
        mem.zeroInit(Account, .{ .id = 1 }),
        mem.zeroInit(Account, .{ .id = 2 }),
        mem.zeroInit(Account, .{ .id = 3 }),
        mem.zeroInit(Account, .{ .id = 4 }),
        mem.zeroInit(Account, .{ .id = 11 }),
        mem.zeroInit(Account, .{ .id = 12 }),
    };

    var transfers = [_]Transfer{
        mem.zeroInit(Transfer, .{
            .id = 1,
            .amount = 15,
            .debit_account_id = 1,
            .credit_account_id = 2,
        }),
        mem.zeroInit(Transfer, .{
            .id = 2,
            .amount = 15,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .flags = .{ .pending = true },
            .timeout = 25,
        }),
        mem.zeroInit(Transfer, .{
            .id = 3,
            .amount = 15,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .flags = .{ .pending = true },
            .timeout = 25,
        }),
        mem.zeroInit(Transfer, .{
            .id = 4,
            .amount = 15,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .flags = .{ .pending = true },
            .timeout = 1,
        }),
        mem.zeroInit(Transfer, .{
            .id = 5,
            .amount = 15,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .flags = .{
                .pending = true,
            },
            .timeout = 25,
        }),
        mem.zeroInit(Transfer, .{
            .id = 6,
            .amount = 15,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .flags = .{
                .pending = true,
            },
            .timeout = 25,
        }),
        mem.zeroInit(Transfer, .{
            .id = 7,
            .amount = 15,
            .debit_account_id = 3,
            .credit_account_id = 4,
            .flags = .{ .pending = true },
            .timeout = 25,
        }),
        mem.zeroInit(Transfer, .{
            .id = 8,
            .amount = 15,
            .debit_account_id = 11,
            .credit_account_id = 12,
            .flags = .{ .pending = true },
            .timeout = 25,
        }),
    };

    var state_machine = try StateMachine.init(allocator, accounts.len, transfers.len, 1);
    defer state_machine.deinit();

    const input = mem.asBytes(&accounts);
    const output = try allocator.alloc(u8, 4096);

    // Accounts:
    state_machine.prepare(0, .create_accounts, input);
    const size = state_machine.commit(0, .create_accounts, input, output);
    {
        const errors = mem.bytesAsSlice(CreateAccountsResult, output[0..size]);
        try testing.expectEqual(@as(usize, 0), errors.len);
    }

    for (accounts) |account| {
        try testing.expectEqual(account, state_machine.get_account(account.id).?.*);
    }

    // Pending Transfers:
    const object_transfers = mem.asBytes(&transfers);
    const output_transfers = try allocator.alloc(u8, 4096);

    state_machine.prepare(0, .create_transfers, object_transfers);
    const size_transfers = state_machine.commit(
        0,
        .create_transfers,
        object_transfers,
        output_transfers,
    );
    const errors = mem.bytesAsSlice(CreateTransfersResult, output_transfers[0..size_transfers]);
    try testing.expectEqual(@as(usize, 0), errors.len);

    for (transfers) |transfer| {
        try testing.expectEqual(transfer, state_machine.get_transfer(transfer.id).?.*);
    }

    // Post the [pending] Transfer:
    const Vector = struct { result: CreateTransferResult, object: Transfer };
    const timestamp: u64 = (state_machine.commit_timestamp + 1);
    const vectors = [_]Vector{
        .{
            .result = .pending_transfer_not_pending,
            .object = mem.zeroInit(Transfer, .{
                .id = 9,
                .pending_id = 1,
                .timestamp = timestamp,
                .reserved = 0,
                .flags = .{ .post_pending_transfer = true },
            }),
        },
        .{
            .result = .reserved_flag,
            .object = mem.zeroInit(Transfer, .{
                .id = 10,
                .pending_id = 1,
                .timestamp = timestamp,
                .flags = .{ .padding = 1, .post_pending_transfer = true },
            }),
        },
        .{
            .result = .pending_transfer_not_found,
            .object = mem.zeroInit(Transfer, .{
                .id = 11,
                .pending_id = 777,
                .timestamp = timestamp,
                .flags = .{ .post_pending_transfer = true },
            }),
        },
        .{
            .result = .pending_transfer_not_pending,
            .object = mem.zeroInit(Transfer, .{
                .id = 12,
                .pending_id = 1,
                .timestamp = timestamp,
                .flags = .{ .post_pending_transfer = true },
            }),
        },
        .{
            .result = .ok,
            .object = mem.zeroInit(Transfer, .{
                .id = 13,
                .pending_id = 2,
                .debit_account_id = 1,
                .credit_account_id = 2,
                .amount = 15,
                .timestamp = timestamp,
                .flags = .{ .post_pending_transfer = true },
            }),
        },
        .{
            .result = .pending_transfer_already_posted,
            .object = mem.zeroInit(Transfer, .{
                .id = 14,
                .pending_id = 2,
                .timestamp = timestamp + 1,
                .flags = .{ .void_pending_transfer = true },
            }),
        },
        .{
            .result = .pending_transfer_already_posted,
            .object = mem.zeroInit(Transfer, .{
                .id = 15,
                .pending_id = 2,
                .timestamp = timestamp + 1,
                .flags = .{ .post_pending_transfer = true },
            }),
        },
        .{
            .result = .ok,
            .object = mem.zeroInit(Transfer, .{
                .id = 16,
                .pending_id = 3,
                .amount = 15,
                .debit_account_id = 1,
                .credit_account_id = 2,
                .timestamp = timestamp + 1,
                .flags = .{ .void_pending_transfer = true },
            }),
        },
        .{
            .result = .pending_transfer_already_voided,
            .object = mem.zeroInit(Transfer, .{
                .id = 17,
                .pending_id = 3,
                .timestamp = timestamp + 2,
                .flags = .{ .post_pending_transfer = true },
            }),
        },
        .{
            .result = .pending_transfer_expired,
            .object = mem.zeroInit(Transfer, .{
                .id = 18,
                .pending_id = 4,
                .timestamp = timestamp + 2,
                .flags = .{ .post_pending_transfer = true },
            }),
        },
        .{
            .result = .cannot_post_and_void_pending_transfer,
            .object = mem.zeroInit(Transfer, .{
                .id = 22,
                .pending_id = 6,
                .timestamp = timestamp + 2,
                .flags = .{ .void_pending_transfer = true, .post_pending_transfer = true },
            }),
        },
        .{
            .result = .pending_id_must_not_be_zero,
            .object = mem.zeroInit(Transfer, .{
                .id = 22,
                .pending_id = 0,
                .timestamp = timestamp + 2,
                .flags = .{ .void_pending_transfer = true },
            }),
        },
        .{
            .result = .exceeds_pending_transfer_amount,
            .object = mem.zeroInit(Transfer, .{
                .id = 23,
                .pending_id = 8,
                .amount = 16, //original amount is 15.
                .debit_account_id = 11,
                .credit_account_id = 12,
                .timestamp = timestamp + 3,
                .flags = .{ .void_pending_transfer = true },
            }),
        },
    };

    // Test balances BEFORE posting
    // Account 1:
    const account_1_before = state_machine.get_account(1).?.*;
    try testing.expectEqual(@as(u64, 15), account_1_before.debits_posted);
    try testing.expectEqual(@as(u64, 75), account_1_before.debits_pending);
    try testing.expectEqual(@as(u64, 0), account_1_before.credits_posted);
    try testing.expectEqual(@as(u64, 0), account_1_before.credits_pending);
    // Account 2:
    const account_2_before = state_machine.get_account(2).?.*;
    try testing.expectEqual(@as(u64, 0), account_2_before.debits_posted);
    try testing.expectEqual(@as(u64, 0), account_2_before.debits_pending);
    try testing.expectEqual(@as(u64, 15), account_2_before.credits_posted);
    try testing.expectEqual(@as(u64, 75), account_2_before.credits_pending);

    for (vectors) |vector, i| {
        const result = state_machine.create_transfer(&vector.object);
        testing.expectEqual(vector.result, result) catch |err| {
            test_debug_vector_create(Vector, CreateTransferResult, i, vector, result, err);
            return err;
        };

        if (vector.result == .ok) {
            const fetched_posted = state_machine.get_transfer(vector.object.id).?.*;
            testing.expectEqual(vector.object, fetched_posted) catch |err| {
                test_debug_vector_create(Vector, Transfer, i, vector, fetched_posted, err);
                return err;
            };
        }
    }

    // Test balances AFTER posting
    // Account 1:
    const account_1_after = state_machine.get_account(1).?.*;
    try testing.expectEqual(@as(u64, 30), account_1_after.debits_posted);
    // +15 (acceptance applied):
    try testing.expectEqual(@as(u64, 45), account_1_after.debits_pending);
    // -15 (reserved moved):
    try testing.expectEqual(@as(u64, 0), account_1_after.credits_posted);
    try testing.expectEqual(@as(u64, 0), account_1_after.credits_pending);
    // Account 2:
    const account_2_after = state_machine.get_account(2).?.*;
    try testing.expectEqual(@as(u64, 0), account_2_after.debits_posted);
    try testing.expectEqual(@as(u64, 0), account_2_after.debits_pending);
    // +15 (acceptance applied):
    try testing.expectEqual(@as(u64, 30), account_2_after.credits_posted);
    // -15 (reserved moved):
    try testing.expectEqual(@as(u64, 45), account_2_after.credits_pending);

    // Test posted Transfer with invalid debit/credit accounts
    state_machine.create_account_rollback(&accounts[3]);
    try testing.expect(state_machine.get_account(accounts[3].id) == null);
    try testing.expectEqual(
        state_machine.create_transfer(&mem.zeroInit(Transfer, .{
            .id = 23,
            .pending_id = 7,
            .timestamp = timestamp + 2,
            .flags = .{ .post_pending_transfer = true },
        })),
        .credit_account_not_found,
    );
    state_machine.create_account_rollback(&accounts[2]);
    try testing.expect(state_machine.get_account(accounts[2].id) == null);
    try testing.expectEqual(
        state_machine.create_transfer(&mem.zeroInit(Transfer, .{
            .id = 24,
            .pending_id = 7,
            .timestamp = timestamp + 2,
            .flags = .{ .post_pending_transfer = true },
        })),
        .debit_account_not_found,
    );

    // Post with pending Transfer amount 15 by setting the amount as [0]
    try testing.expectEqual(
        state_machine.create_transfer(&mem.zeroInit(Transfer, .{
            .id = 25,
            .pending_id = 8,
            .amount = 0,
            .debit_account_id = 11,
            .credit_account_id = 12,
            .timestamp = timestamp + 2,
            .flags = .{ .post_pending_transfer = true },
        })),
        .ok,
    );

    // Rollback [id=13,pending_id=2] not rejected:
    state_machine.create_transfer_rollback(&vectors[4].object);

    // Account 1:
    const account_1_rollback = state_machine.get_account(1).?.*;
    // -15 (rollback):
    try testing.expectEqual(@as(u64, 15), account_1_rollback.debits_posted);
    try testing.expectEqual(@as(u64, 60), account_1_rollback.debits_pending);
    try testing.expectEqual(@as(u64, 0), account_1_rollback.credits_posted);
    try testing.expectEqual(@as(u64, 0), account_1_rollback.credits_pending);
    // Account 2:
    const account_2_rollback = state_machine.get_account(2).?.*;
    try testing.expectEqual(@as(u64, 0), account_2_rollback.debits_posted);
    try testing.expectEqual(@as(u64, 0), account_2_rollback.debits_pending);
    // -15 (rollback):
    try testing.expectEqual(@as(u64, 15), account_2_rollback.credits_posted);
    try testing.expectEqual(@as(u64, 60), account_2_rollback.credits_pending);

    // Rollback [id=3] rejected:
    state_machine.create_transfer_rollback(&vectors[7].object);
    // Account 1:
    const account_1_rollback_reject = state_machine.get_account(1).?.*;
    try testing.expectEqual(@as(u64, 15), account_1_rollback_reject.debits_posted);
    // Remains unchanged:
    try testing.expectEqual(@as(u64, 75), account_1_rollback_reject.debits_pending);
    // +15 rolled back:
    try testing.expectEqual(@as(u64, 0), account_1_rollback_reject.credits_posted);
    try testing.expectEqual(@as(u64, 0), account_1_rollback_reject.credits_pending);
    // Account 2:
    const account_2_rollback_reject = state_machine.get_account(2).?.*;
    try testing.expectEqual(@as(u64, 0), account_2_rollback_reject.debits_posted);
    try testing.expectEqual(@as(u64, 0), account_2_rollback_reject.debits_pending);
    try testing.expectEqual(@as(u64, 15), account_2_rollback_reject.credits_posted);
    // +15 rolled back"
    try testing.expectEqual(@as(u64, 75), account_2_rollback_reject.credits_pending);
}

fn test_debug_vector_create(comptime vector_type: type, comptime create_result_type: type, i: usize, vector: vector_type, create_result: create_result_type, err: anyerror) void {
    std.debug.print("VECTOR-FAILURE-> Index[{d}] Expected[{s}:{s}] -> \n\n-----\n{any}\n----- \n ERR({d}:{d}): -> \n\n\t{any}.", .{
        i,
        vector.result,
        create_result,
        vector.object,
        i,
        vector.object.id,
        err,
    });
}

fn test_routine_zeroed(comptime len: usize) !void {
    const routine = switch (len) {
        32 => zeroed_32_bytes,
        48 => zeroed_48_bytes,
        else => unreachable,
    };
    var a = [_]u8{0} ** len;
    var i: usize = 0;
    while (i < a.len) : (i += 1) {
        a[i] = 1;
        try testing.expectEqual(false, routine(a));
        a[i] = 0;
    }
    try testing.expectEqual(true, routine(a));
}

fn test_routine_equal(comptime len: usize) !void {
    const routine = switch (len) {
        32 => equal_32_bytes,
        48 => equal_48_bytes,
        else => unreachable,
    };
    var a = [_]u8{0} ** len;
    var b = [_]u8{0} ** len;
    var i: usize = 0;
    while (i < a.len) : (i += 1) {
        a[i] = 1;
        try testing.expectEqual(false, routine(a, b));
        a[i] = 0;

        b[i] = 1;
        try testing.expectEqual(false, routine(a, b));
        b[i] = 0;
    }
    try testing.expectEqual(true, routine(a, b));
}

test "zeroed_32_bytes" {
    try test_routine_zeroed(32);
}

test "zeroed_48_bytes" {
    try test_routine_zeroed(48);
}

test "equal_32_bytes" {
    try test_routine_equal(32);
}

test "equal_48_bytes" {
    try test_routine_equal(48);
}
