const std = @import("std");
const assert = std.debug.assert;
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
const HashMapPosted = std.AutoHashMap(u128, TransferFlags);
const HashMapUserDataToId = std.AutoHashMap(u128, u128);

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

    allocator: std.mem.Allocator,
    prepare_timestamp: u64,
    commit_timestamp: u64,
    accounts: HashMapAccounts,
    transfers: HashMapTransfers,
    posted: HashMapPosted,

    pub fn init(
        allocator: std.mem.Allocator,
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
        var events = std.mem.bytesAsSlice(Event(operation), input);
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

        const events = std.mem.bytesAsSlice(Event(operation), input);
        var results = std.mem.bytesAsSlice(Result(operation), output);
        var count: usize = 0;

        var chain: ?usize = null;
        var chain_broken = false;

        for (events) |event, index| {
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
        const events = std.mem.bytesAsSlice(Event(operation), input);

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
                .create_accounts => self.create_account_rollback(event),
                .create_transfers => self.transfer_rollback(event),
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
        const batch = std.mem.bytesAsSlice(u128, input);
        const output_len = @divFloor(output.len, @sizeOf(Account)) * @sizeOf(Account);
        const results = std.mem.bytesAsSlice(Account, output[0..output_len]);
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
        const batch = std.mem.bytesAsSlice(u128, input);
        const output_len = @divFloor(output.len, @sizeOf(Transfer)) * @sizeOf(Transfer);
        const results = std.mem.bytesAsSlice(Transfer, output[0..output_len]);
        var results_count: usize = 0;
        for (batch) |id| {
            if (self.get_transfer(id)) |result| {
                results[results_count] = result.*;
                results_count += 1;
            }
        }
        return results_count * @sizeOf(Transfer);
    }

    fn create_account(self: *StateMachine, a: Account) CreateAccountResult {
        assert(a.timestamp > self.commit_timestamp);

        if (!zeroed_48_bytes(a.reserved)) return .reserved_field;
        if (a.flags.padding != 0) return .reserved_flag_padding;

        // Opening balances may never exceed limits:
        if (a.debits_exceed_credits(0)) return .exceeds_credits;
        if (a.credits_exceed_debits(0)) return .exceeds_debits;

        var insert = self.accounts.getOrPutAssumeCapacity(a.id);
        if (insert.found_existing) {
            const exists = insert.value_ptr.*;
            if (exists.ledger != a.ledger) return .exists_with_different_ledger;
            if (exists.code != a.code) return .exists_with_different_code;
            if (@bitCast(u16, exists.flags) != @bitCast(u16, a.flags)) {
                return .exists_with_different_flags;
            }
            if (exists.user_data != a.user_data) return .exists_with_different_user_data;
            if (!equal_48_bytes(exists.reserved, a.reserved)) {
                return .exists_with_different_reserved_field;
            }
            return .exists;
        } else {
            insert.value_ptr.* = a;
            self.commit_timestamp = a.timestamp;
            return .ok;
        }
    }

    fn create_account_rollback(self: *StateMachine, a: Account) void {
        assert(self.accounts.remove(a.id));
    }

    fn create_transfer(self: *StateMachine, t: Transfer) CreateTransferResult {
        assert(t.timestamp > self.commit_timestamp);

        if (t.flags.post_pending_transfer and t.flags.void_pending_transfer) {
            return .cannot_post_and_void_pending_transfer;
        } else if (t.flags.post_pending_transfer or t.flags.void_pending_transfer) {
            return self.post_or_void_pending_transfer(t);
        } else {
            if (t.flags.padding != 0) return .reserved_flag_padding;
            if (t.flags.pending) {
                // Otherwise reserved amounts may never be released:
                if (t.timeout == 0) return .pending_transfer_must_timeout;
                if (t.pending_id != 0) return .pending_id_must_be_zero;
            } else if (t.timeout != 0) return .timeout_reserved_for_pending_transfer;

            if (t.amount == 0) return .amount_is_zero;

            if (t.debit_account_id == t.credit_account_id) return .accounts_are_the_same;

            // The etymology of the DR and CR abbreviations for debit/credit is interesting, either:
            // 1. derived from the Latin past participles of debitum/creditum, i.e. debere/credere,
            // 2. standing for debit record and credit record, or
            // 3. relating to debtor and creditor.
            // We use them to distinguish between `cr` (credit account), and `c` (commit).
            const dr = self.get_account(t.debit_account_id) orelse return .debit_account_not_found;
            const cr = self.get_account(t.credit_account_id) orelse return .credit_account_not_found;
            assert(t.timestamp > dr.timestamp);
            assert(t.timestamp > cr.timestamp);

            if (dr.ledger != cr.ledger) return .accounts_have_different_ledgers;

            // Verify that the debit/credit amounts do not exceed or overflow:
            // Debit:
            if (dr.debits_pending_overflow(t.amount)) return .debits_pending_would_overflow;
            if (dr.debits_posted_overflow(t.amount)) return .debits_posted_would_overflow;
            if (dr.debits_overflow(t.amount)) return .debits_would_overflow;

            // Credit:
            if (dr.credits_pending_overflow(t.amount)) return .credits_pending_would_overflow;
            if (dr.credits_posted_overflow(t.amount)) return .credits_posted_would_overflow;
            if (cr.credits_overflow(t.amount)) return .credits_would_overflow;

            // Exceed Limits Verification:
            if (dr.debits_exceed_credits(t.amount)) return .exceeds_credits;
            if (cr.credits_exceed_debits(t.amount)) return .exceeds_debits;

            const insert = self.transfers.getOrPutAssumeCapacity(t.id);
            if (insert.found_existing) return existing_transfer_error(t, insert.value_ptr.*);

            // Success, update balances on the C/D accounts:
            insert.value_ptr.* = t;
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
    }

    fn post_or_void_pending_transfer(self: *StateMachine, t: Transfer) CreateTransferResult {
        if (t.flags.padding != 0) return .reserved_flag_padding;
        if (t.pending_id == 0) return .pending_id_is_zero;

        if (self.get_posted(t.pending_id)) |exists| {
            if (!exists.void_pending_transfer and t.flags.void_pending_transfer) return .transfer_already_posted;
            if (exists.void_pending_transfer and !t.flags.void_pending_transfer) return .transfer_already_voided;
            return .transfer_already_posted;
        }

        const pending = self.get_transfer(t.pending_id) orelse return .pending_transfer_not_found;
        if (!pending.flags.pending) return .transfer_not_pending;

        assert(t.timestamp > pending.timestamp);

        if (pending.timeout > 0 and pending.timestamp + pending.timeout <= t.timestamp) return .transfer_expired;

        const dr = self.get_account(pending.debit_account_id) orelse return .debit_account_not_found;
        const cr = self.get_account(pending.credit_account_id) orelse return .credit_account_not_found;
        assert(pending.timestamp > dr.timestamp);
        assert(pending.timestamp > cr.timestamp);

        if (dr.debits_pending < pending.amount) return .debit_amount_not_pending;
        if (cr.credits_pending < pending.amount) return .credit_amount_not_pending;

        if (t.debit_account_id > 0 and pending.debit_account_id != t.debit_account_id) {
            return .exists_with_different_debit_account_id;
        }
        if (t.credit_account_id > 0 and pending.credit_account_id != t.credit_account_id) {
            return .exists_with_different_credit_account_id;
        }

        const user_data = if (t.user_data == 0) pending.user_data else t.user_data;
        if (t.user_data != user_data) return .exists_with_different_user_data;

        const ledger = if (t.ledger == 0) pending.ledger else t.ledger;
        if (t.ledger != ledger) return .exists_with_different_ledger;

        const code = if (t.code == 0) pending.code else t.code;
        if (t.code != code) return .exists_with_different_code;

        const amount = if (t.amount == 0) pending.amount else t.amount;
        if (amount > pending.amount) return .amount_exceeds_pending_amount;

        // Store the [posted]/[voided] transfer:
        const insert = self.transfers.getOrPutAssumeCapacity(t.id);
        if (insert.found_existing) return existing_transfer_error(t, insert.value_ptr.*);
        insert.value_ptr.* = Transfer{
            .id = t.id,
            .debit_account_id = pending.debit_account_id,
            .credit_account_id = pending.credit_account_id,
            .user_data = user_data,
            .reserved = t.reserved,
            .ledger = ledger,
            .code = code,
            .pending_id = t.pending_id,
            .timeout = t.timeout,
            .timestamp = t.timestamp,
            .flags = t.flags,
            .amount = amount,
        };

        const p_insert = self.posted.getOrPutAssumeCapacity(t.pending_id);
        assert(!p_insert.found_existing);
        p_insert.value_ptr.* = t.flags;

        // Success, update balances on the C/D accounts:
        dr.debits_pending -= pending.amount;
        cr.credits_pending -= pending.amount;
        if (t.flags.post_pending_transfer) {
            dr.debits_posted += amount;
            cr.credits_posted += amount;
        }
        self.commit_timestamp = t.timestamp;
        return .ok;
    }

    fn transfer_rollback(self: *StateMachine, t: Transfer) void {
        const dr = self.get_account(t.debit_account_id).?;
        const cr = self.get_account(t.credit_account_id).?;

        if (t.pending_id > 0) {
            dr.debits_pending += t.amount;
            cr.credits_pending += t.amount;
            if (!t.flags.void_pending_transfer) {
                dr.debits_posted -= t.amount;
                cr.credits_posted -= t.amount;
            }
            assert(self.posted.remove(t.pending_id));
        } else {
            if (t.flags.pending) {
                dr.debits_pending -= t.amount;
                cr.credits_pending -= t.amount;
            } else {
                dr.debits_posted -= t.amount;
                cr.credits_posted -= t.amount;
            }
        }
        // ID will be unique for a [posted] and [pending] Transfer:
        assert(self.transfers.remove(t.id));
    }

    fn existing_transfer_error(t: Transfer, existing: Transfer) CreateTransferResult {
        if (existing.debit_account_id != t.debit_account_id) {
            return .exists_with_different_debit_account_id;
        } else if (existing.credit_account_id != t.credit_account_id) {
            return .exists_with_different_credit_account_id;
        }

        if (existing.amount != t.amount) return .exists_with_different_amount;

        if (@bitCast(u16, existing.flags) != @bitCast(u16, t.flags)) {
            return .exists_with_different_flags;
        }

        if (existing.user_data != t.user_data) return .exists_with_different_user_data;
        if (existing.ledger != t.ledger) return .exists_with_different_ledger;
        if (existing.reserved != t.reserved) return .exists_with_different_reserved_field;
        if (existing.timeout != t.timeout) return .exists_with_different_timeout;

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

    /// See the comment for get_account().
    fn get_posted(self: *StateMachine, pending_id: u128) ?*TransferFlags {
        return self.posted.getPtr(pending_id);
    }
};

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
            .result = .reserved_flag_padding,
            .object = std.mem.zeroInit(Account, .{
                .id = 1,
                .timestamp = 1,
                .flags = .{ .padding = 1 },
            }),
        },
        Vector{
            .result = .reserved_field,
            .object = std.mem.zeroInit(Account, .{
                .id = 2,
                .timestamp = 1,
                .reserved = [_]u8{1} ** 48,
            }),
        },
        Vector{
            .result = .exceeds_credits,
            .object = std.mem.zeroInit(Account, .{
                .id = 3,
                .timestamp = 1,
                .debits_pending = 10,
                .flags = .{ .debits_must_not_exceed_credits = true },
            }),
        },
        Vector{
            .result = .exceeds_credits,
            .object = std.mem.zeroInit(Account, .{
                .id = 4,
                .timestamp = 1,
                .debits_posted = 10,
                .flags = .{ .debits_must_not_exceed_credits = true },
            }),
        },
        Vector{
            .result = .exceeds_debits,
            .object = std.mem.zeroInit(Account, .{
                .id = 5,
                .timestamp = 1,
                .credits_pending = 10,
                .flags = .{ .credits_must_not_exceed_debits = true },
            }),
        },
        Vector{
            .result = .exceeds_debits,
            .object = std.mem.zeroInit(Account, .{
                .id = 6,
                .timestamp = 1,
                .credits_posted = 10,
                .flags = .{ .credits_must_not_exceed_debits = true },
            }),
        },
        Vector{
            .result = .ok,
            .object = std.mem.zeroInit(Account, .{
                .id = 7,
                .timestamp = 1,
            }),
        },
        Vector{
            .result = .exists,
            .object = std.mem.zeroInit(Account, .{
                .id = 7,
                .timestamp = 2,
            }),
        },
        Vector{
            .result = .ok,
            .object = std.mem.zeroInit(Account, .{
                .id = 8,
                .timestamp = 2,
                .user_data = 'U',
                .ledger = 9,
            }),
        },
        Vector{
            .result = .exists_with_different_ledger,
            .object = std.mem.zeroInit(Account, .{
                .id = 8,
                .timestamp = 3,
                .user_data = 'U',
                .ledger = 10,
            }),
        },
        Vector{
            .result = .ok,
            .object = std.mem.zeroInit(Account, .{
                .id = 9,
                .timestamp = 3,
                .code = 9,
                .user_data = 'U',
            }),
        },
        Vector{
            .result = .exists_with_different_code,
            .object = std.mem.zeroInit(Account, .{
                .id = 9,
                .timestamp = 4,
                .code = 10,
                .user_data = 'D',
            }),
        },
        Vector{
            .result = .ok,
            .object = std.mem.zeroInit(Account, .{
                .id = 10,
                .timestamp = 4,
                .flags = .{ .credits_must_not_exceed_debits = true },
            }),
        },
        Vector{
            .result = .exists_with_different_flags,
            .object = std.mem.zeroInit(Account, .{
                .id = 10,
                .timestamp = 5,
                .flags = .{ .debits_must_not_exceed_credits = true },
            }),
        },
        Vector{
            .result = .ok,
            .object = std.mem.zeroInit(Account, .{
                .id = 11,
                .timestamp = 5,
                .user_data = 'U',
            }),
        },
        Vector{
            .result = .exists_with_different_user_data,
            .object = std.mem.zeroInit(Account, .{
                .id = 11,
                .timestamp = 6,
                .user_data = 'D',
            }),
        },
    };

    var state_machine = try StateMachine.init(allocator, vectors.len, 0, 0);
    defer state_machine.deinit();

    for (vectors) |vector| {
        try testing.expectEqual(vector.result, state_machine.create_account(vector.object));
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
        std.mem.zeroInit(Account, .{ .id = 7, .code = 200 }),

        // A chain of 4 events (the last event in the chain closes the chain with linked=false):
        // Commit/rollback:
        std.mem.zeroInit(Account, .{ .id = 0, .flags = .{ .linked = true } }),
        // Commit/rollback:
        std.mem.zeroInit(Account, .{ .id = 1, .flags = .{ .linked = true } }),
        // Fail with .exists:
        std.mem.zeroInit(Account, .{ .id = 0, .flags = .{ .linked = true } }),
        // Fail without committing.
        std.mem.zeroInit(Account, .{ .id = 2 }),

        // An individual event (successful):
        // This should not see any effect from the failed chain above:
        std.mem.zeroInit(Account, .{ .id = 0, .code = 200 }),

        // A chain of 2 events (the first event fails the chain):
        std.mem.zeroInit(Account, .{ .id = 0, .flags = .{ .linked = true } }),
        std.mem.zeroInit(Account, .{ .id = 1 }),

        // An individual event (successful):
        std.mem.zeroInit(Account, .{ .id = 1, .code = 200 }),

        // A chain of 2 events (the last event fails the chain):
        std.mem.zeroInit(Account, .{ .id = 2, .flags = .{ .linked = true } }),
        std.mem.zeroInit(Account, .{ .id = 0 }),

        // A chain of 2 events (successful):
        std.mem.zeroInit(Account, .{ .id = 2, .flags = .{ .linked = true } }),
        std.mem.zeroInit(Account, .{ .id = 3 }),
    };

    var state_machine = try StateMachine.init(allocator, accounts_max, transfers_max, transfers_pending_max);
    defer state_machine.deinit();

    const input = std.mem.asBytes(&accounts);
    const output = try allocator.alloc(u8, 4096);

    state_machine.prepare(0, .create_accounts, input);
    const size = state_machine.commit(0, .create_accounts, input, output);
    const results = std.mem.bytesAsSlice(CreateAccountsResult, output[0..size]);

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
            CreateAccountsResult{ .index = 10, .result = .exists_with_different_code },
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
        std.mem.zeroInit(Account, .{ .id = 1 }),
        std.mem.zeroInit(Account, .{ .id = 2 }),
        std.mem.zeroInit(Account, .{ .id = 3, .ledger = 1 }),
        std.mem.zeroInit(Account, .{ .id = 4, .ledger = 2 }),
        std.mem.zeroInit(Account, .{ .id = 5, .credits_posted = 1000, .debits_posted = 500, .flags = .{ .debits_must_not_exceed_credits = true } }),
        std.mem.zeroInit(Account, .{ .id = 6, .credits_posted = 500, .debits_posted = 1000, .flags = .{ .credits_must_not_exceed_debits = true } }),
        std.mem.zeroInit(Account, .{ .id = 7 }),
        std.mem.zeroInit(Account, .{ .id = 8 }),
    };

    var state_machine = try StateMachine.init(allocator, accounts.len, 1, 0);
    defer state_machine.deinit();

    const input = std.mem.asBytes(&accounts);
    const output = try allocator.alloc(u8, 4096);

    state_machine.prepare(0, .create_accounts, input);
    const size = state_machine.commit(0, .create_accounts, input, output);

    const errors = std.mem.bytesAsSlice(CreateAccountsResult, output[0..size]);
    try testing.expectEqual(@as(usize, 0), errors.len);

    for (accounts) |account| {
        try testing.expectEqual(account, state_machine.get_account(account.id).?.*);
    }

    const Vector = struct { result: CreateTransferResult, object: Transfer };

    const timestamp: u64 = (state_machine.commit_timestamp + 1);
    const vectors = [_]Vector{
        Vector{
            .result = .amount_is_zero,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 1,
                .timestamp = timestamp,
            }),
        },
        Vector{
            .result = .reserved_flag_padding,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 2,
                .timestamp = timestamp,
                .flags = .{ .padding = 1 },
            }),
        },
        Vector{
            .result = .pending_transfer_must_timeout,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 3,
                .timestamp = timestamp,
                .flags = .{ .pending = true },
            }),
        },
        Vector{
            .result = .timeout_reserved_for_pending_transfer,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 4,
                .timestamp = timestamp,
                .timeout = 1,
            }),
        },
        Vector{
            .result = .accounts_are_the_same,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 6,
                .timestamp = timestamp,
                .amount = 10,
                .debit_account_id = 1,
                .credit_account_id = 1,
            }),
        },
        Vector{
            .result = .debit_account_not_found,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 7,
                .timestamp = timestamp,
                .amount = 10,
                .debit_account_id = 100,
                .credit_account_id = 1,
            }),
        },
        Vector{
            .result = .credit_account_not_found,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 8,
                .timestamp = timestamp,
                .amount = 10,
                .debit_account_id = 1,
                .credit_account_id = 100,
            }),
        },
        Vector{
            .result = .accounts_have_different_ledgers,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 9,
                .timestamp = timestamp,
                .amount = 10,
                .debit_account_id = 3,
                .credit_account_id = 4,
            }),
        },
        Vector{
            .result = .credits_would_overflow,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 10,
                .timestamp = timestamp,
                .amount = std.math.maxInt(u64),
                .debit_account_id = 1,
                .credit_account_id = 6,
            }),
        },
        Vector{
            .result = .debits_posted_would_overflow,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 10,
                .timestamp = timestamp,
                .amount = std.math.maxInt(u64),
                .debit_account_id = 5,
                .credit_account_id = 1,
            }),
        },
        Vector{
            .result = .exceeds_credits,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 10,
                .timestamp = timestamp,
                .amount = 1000,
                .debit_account_id = 5,
                .credit_account_id = 1,
            }),
        },
        Vector{
            .result = .exceeds_debits,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 11,
                .timestamp = timestamp,
                .amount = 1000,
                .debit_account_id = 1,
                .credit_account_id = 6,
            }),
        },
        Vector{
            .result = .ok,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 12,
                .timestamp = timestamp,
                .amount = 10,
                .debit_account_id = 7,
                .credit_account_id = 8,
            }),
        },
        Vector{
            .result = .exists,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 12,
                .timestamp = timestamp + 1,
                .amount = 10,
                .debit_account_id = 7,
                .credit_account_id = 8,
            }),
        },
        Vector{
            .result = .exists_with_different_debit_account_id,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 12,
                .timestamp = timestamp + 1,
                .amount = 10,
                .debit_account_id = 8,
                .credit_account_id = 7,
            }),
        },
        Vector{
            .result = .exists_with_different_credit_account_id,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 12,
                .timestamp = timestamp + 1,
                .amount = 10,
                .debit_account_id = 7,
                .credit_account_id = 1,
            }),
        },
        Vector{
            .result = .exists_with_different_amount,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 12,
                .timestamp = timestamp + 1,
                .amount = 11,
                .debit_account_id = 7,
                .credit_account_id = 8,
            }),
        },
        Vector{
            .result = .exists_with_different_flags,
            .object = std.mem.zeroInit(Transfer, .{
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
            .object = std.mem.zeroInit(Transfer, .{
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
            .object = std.mem.zeroInit(Transfer, .{
                .id = 13,
                .timestamp = timestamp + 1,
                .amount = 10,
                .debit_account_id = 7,
                .credit_account_id = 8,
                .reserved = 0,
            }),
        },
        Vector{
            .result = .exists_with_different_reserved_field,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 13,
                .timestamp = timestamp + 2,
                .amount = 10,
                .debit_account_id = 7,
                .credit_account_id = 8,
                .reserved = 1,
            }),
        },
        Vector{
            .result = .timeout_reserved_for_pending_transfer,
            .object = std.mem.zeroInit(Transfer, .{
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
            .object = std.mem.zeroInit(Transfer, .{
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
            .object = std.mem.zeroInit(Transfer, .{
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
            .object = std.mem.zeroInit(Transfer, .{
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
        const create_result = state_machine.create_transfer(vector.object);
        testing.expectEqual(vector.result, create_result) catch |err| {
            test_debug_vector_create(Vector, CreateTransferResult, i, vector, create_result, err);
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
    state_machine.transfer_rollback(state_machine.get_transfer(vectors[12].object.id).?.*);
    try testing.expectEqual(@as(u64, 10), state_machine.get_account(7).?.*.debits_posted);
    try testing.expectEqual(@as(u64, 0), state_machine.get_account(7).?.*.credits_posted);
    try testing.expectEqual(@as(u64, 10), state_machine.get_account(8).?.*.credits_posted);
    try testing.expectEqual(@as(u64, 0), state_machine.get_account(8).?.*.debits_posted);
    try testing.expect(state_machine.get_transfer(vectors[11].object.id) == null);

    // Rollback transfer with id [15], amount of 10:
    state_machine.transfer_rollback(state_machine.get_transfer(vectors[23].object.id).?.*);
    try testing.expectEqual(@as(u64, 0), state_machine.get_account(7).?.*.debits_pending);
    try testing.expectEqual(@as(u64, 0), state_machine.get_account(8).?.*.credits_pending);
    try testing.expect(state_machine.get_transfer(vectors[22].object.id) == null);
}

test "create/lookup/rollback 2-phase transfers" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    var accounts = [_]Account{
        std.mem.zeroInit(Account, .{ .id = 1 }),
        std.mem.zeroInit(Account, .{ .id = 2 }),
        std.mem.zeroInit(Account, .{ .id = 3 }),
        std.mem.zeroInit(Account, .{ .id = 4 }),
        std.mem.zeroInit(Account, .{ .id = 11 }),
        std.mem.zeroInit(Account, .{ .id = 12 }),
    };

    var transfers = [_]Transfer{
        std.mem.zeroInit(Transfer, .{
            .id = 1,
            .amount = 15,
            .debit_account_id = 1,
            .credit_account_id = 2,
        }),
        std.mem.zeroInit(Transfer, .{
            .id = 2,
            .amount = 15,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .flags = .{ .pending = true },
            .timeout = 25,
        }),
        std.mem.zeroInit(Transfer, .{
            .id = 3,
            .amount = 15,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .flags = .{ .pending = true },
            .timeout = 25,
        }),
        std.mem.zeroInit(Transfer, .{
            .id = 4,
            .amount = 15,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .flags = .{ .pending = true },
            .timeout = 1,
        }),
        std.mem.zeroInit(Transfer, .{
            .id = 5,
            .amount = 15,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .flags = .{
                .pending = true,
            },
            .timeout = 25,
        }),
        std.mem.zeroInit(Transfer, .{
            .id = 6,
            .amount = 15,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .flags = .{
                .pending = true,
            },
            .timeout = 25,
        }),
        std.mem.zeroInit(Transfer, .{
            .id = 7,
            .amount = 15,
            .debit_account_id = 3,
            .credit_account_id = 4,
            .flags = .{ .pending = true },
            .timeout = 25,
        }),
        std.mem.zeroInit(Transfer, .{
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

    const input = std.mem.asBytes(&accounts);
    const output = try allocator.alloc(u8, 4096);

    // Accounts:
    state_machine.prepare(0, .create_accounts, input);
    const size = state_machine.commit(0, .create_accounts, input, output);
    {
        const errors = std.mem.bytesAsSlice(CreateAccountsResult, output[0..size]);
        try testing.expectEqual(@as(usize, 0), errors.len);
    }

    for (accounts) |account| {
        try testing.expectEqual(account, state_machine.get_account(account.id).?.*);
    }

    // Pending Transfers:
    const object_transfers = std.mem.asBytes(&transfers);
    const output_transfers = try allocator.alloc(u8, 4096);

    state_machine.prepare(0, .create_transfers, object_transfers);
    const size_transfers = state_machine.commit(
        0,
        .create_transfers,
        object_transfers,
        output_transfers,
    );
    const errors = std.mem.bytesAsSlice(CreateTransfersResult, output_transfers[0..size_transfers]);
    try testing.expectEqual(@as(usize, 0), errors.len);

    for (transfers) |transfer| {
        try testing.expectEqual(transfer, state_machine.get_transfer(transfer.id).?.*);
    }

    // Post the [pending] Transfer:
    const Vector = struct { result: CreateTransferResult, object: Transfer };
    const timestamp: u64 = (state_machine.commit_timestamp + 1);
    const vectors = [_]Vector{
        Vector{
            .result = .transfer_not_pending,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 9,
                .pending_id = 1,
                .timestamp = timestamp,
                .reserved = 0,
                .flags = .{ .post_pending_transfer = true },
            }),
        },
        Vector{
            .result = .reserved_flag_padding,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 10,
                .pending_id = 1,
                .timestamp = timestamp,
                .flags = .{ .padding = 1, .post_pending_transfer = true },
            }),
        },
        Vector{
            .result = .pending_transfer_not_found,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 11,
                .pending_id = 777,
                .timestamp = timestamp,
                .flags = .{ .post_pending_transfer = true },
            }),
        },
        Vector{
            .result = .transfer_not_pending,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 12,
                .pending_id = 1,
                .timestamp = timestamp,
                .flags = .{ .post_pending_transfer = true },
            }),
        },
        Vector{
            .result = .ok,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 13,
                .pending_id = 2,
                .debit_account_id = 1,
                .credit_account_id = 2,
                .amount = 15,
                .timestamp = timestamp,
                .flags = .{ .post_pending_transfer = true },
            }),
        },
        Vector{
            .result = .transfer_already_posted,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 14,
                .pending_id = 2,
                .timestamp = timestamp + 1,
                .flags = .{ .void_pending_transfer = true },
            }),
        },
        Vector{
            .result = .transfer_already_posted,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 15,
                .pending_id = 2,
                .timestamp = timestamp + 1,
                .flags = .{ .post_pending_transfer = true },
            }),
        },
        Vector{
            .result = .ok,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 16,
                .pending_id = 3,
                .amount = 15,
                .debit_account_id = 1,
                .credit_account_id = 2,
                .timestamp = timestamp + 1,
                .flags = .{ .void_pending_transfer = true },
            }),
        },
        Vector{
            .result = .transfer_already_voided,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 17,
                .pending_id = 3,
                .timestamp = timestamp + 2,
                .flags = .{ .post_pending_transfer = true },
            }),
        },
        Vector{
            .result = .transfer_expired,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 18,
                .pending_id = 4,
                .timestamp = timestamp + 2,
                .flags = .{ .post_pending_transfer = true },
            }),
        },
        Vector{
            .result = .cannot_post_and_void_pending_transfer,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 22,
                .pending_id = 6,
                .timestamp = timestamp + 2,
                .flags = .{ .void_pending_transfer = true, .post_pending_transfer = true },
            }),
        },
        Vector{
            .result = .pending_id_is_zero,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 22,
                .pending_id = 0,
                .timestamp = timestamp + 2,
                .flags = .{ .void_pending_transfer = true },
            }),
        },
        Vector{
            .result = .amount_exceeds_pending_amount,
            .object = std.mem.zeroInit(Transfer, .{
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
        const create_result = state_machine.create_transfer(vector.object); //2-phase post(commit)
        testing.expectEqual(vector.result, create_result) catch |err| {
            test_debug_vector_create(Vector, CreateTransferResult, i, vector, create_result, err);
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
    state_machine.create_account_rollback(accounts[3]);
    try testing.expect(state_machine.get_account(accounts[3].id) == null);
    try testing.expectEqual(
        state_machine.create_transfer(std.mem.zeroInit(Transfer, .{ //2-phase commit
            .id = 23,
            .pending_id = 7,
            .timestamp = timestamp + 2,
            .flags = .{ .post_pending_transfer = true },
        })),
        .credit_account_not_found,
    );
    state_machine.create_account_rollback(accounts[2]);
    try testing.expect(state_machine.get_account(accounts[2].id) == null);
    try testing.expectEqual(
        state_machine.create_transfer(std.mem.zeroInit(Transfer, .{ //2-phase commit
            .id = 24,
            .pending_id = 7,
            .timestamp = timestamp + 2,
            .flags = .{ .post_pending_transfer = true },
        })),
        .debit_account_not_found,
    );

    // Post with pending Transfer amount 15 by setting the amount as [0]
    try testing.expectEqual(
        state_machine.create_transfer(std.mem.zeroInit(Transfer, .{ //2-phase commit
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
    state_machine.transfer_rollback(vectors[4].object);

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
    state_machine.transfer_rollback(vectors[7].object);
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

test "credit/debit limit overflows " {
    const acc_debit_not_exceed_credit_pending = Account{
        .id = 1,
        .user_data = 1,
        .reserved = [_]u8{0} ** 48,
        .ledger = 710,
        .code = 1000,
        .flags = .{ .debits_must_not_exceed_credits = true },
        .debits_pending = std.math.maxInt(u64),
        .debits_posted = 0,
        .credits_pending = 0,
        .credits_posted = 0,
    };
    const acc_debit_not_exceed_credit_posted = Account{
        .id = 1,
        .user_data = 1,
        .reserved = [_]u8{0} ** 48,
        .ledger = 710,
        .code = 1000,
        .flags = .{ .debits_must_not_exceed_credits = true },
        .debits_pending = 0,
        .debits_posted = std.math.maxInt(u64),
        .credits_pending = 0,
        .credits_posted = 0,
    };
    const acc_credit_not_exceed_debit_pending = Account{
        .id = 1,
        .user_data = 1,
        .reserved = [_]u8{0} ** 48,
        .ledger = 710,
        .code = 1000,
        .flags = .{ .credits_must_not_exceed_debits = true },
        .debits_pending = 0,
        .debits_posted = 0,
        .credits_pending = std.math.maxInt(u64),
        .credits_posted = 0,
    };
    const acc_credit_not_exceed_debit_posted = Account{
        .id = 1,
        .user_data = 1,
        .reserved = [_]u8{0} ** 48,
        .ledger = 710,
        .code = 1000,
        .flags = .{ .credits_must_not_exceed_debits = true },
        .debits_pending = 0,
        .debits_posted = 0,
        .credits_pending = 0,
        .credits_posted = std.math.maxInt(u64),
    };

    // Exceeds the overflow limit by 1:
    try testing.expect(acc_debit_not_exceed_credit_pending.debits_overflow(1));
    try testing.expect(acc_debit_not_exceed_credit_pending.debits_pending_overflow(1));
    try testing.expect(acc_debit_not_exceed_credit_posted.debits_posted_overflow(1));
    try testing.expect(acc_credit_not_exceed_debit_pending.credits_overflow(1));
    try testing.expect(acc_credit_not_exceed_debit_pending.credits_pending_overflow(1));
    try testing.expect(acc_credit_not_exceed_debit_posted.credits_posted_overflow(1));

    const acc_debit_not_exceed_credit_no_overflow = Account{
        .id = 1,
        .user_data = 1,
        .reserved = [_]u8{0} ** 48,
        .ledger = 710,
        .code = 1000,
        .flags = .{ .debits_must_not_exceed_credits = true },
        .debits_pending = std.math.maxInt(u64) - 1,
        .debits_posted = 0,
        .credits_pending = 0,
        .credits_posted = 0,
    };
    const acc_credit_not_exceed_debit_no_overflow = Account{
        .id = 1,
        .user_data = 1,
        .reserved = [_]u8{0} ** 48,
        .ledger = 710,
        .code = 1000,
        .flags = .{ .credits_must_not_exceed_debits = true },
        .debits_pending = 0,
        .debits_posted = 0,
        .credits_pending = std.math.maxInt(u64) - 1,
        .credits_posted = 0,
    };

    // Exceed limits, but still no overflow:
    try testing.expect(acc_debit_not_exceed_credit_no_overflow.debits_exceed_credits(1));
    try testing.expect(acc_credit_not_exceed_debit_no_overflow.credits_exceed_debits(1));

    const acc_debit_not_exceed_credit_ok = Account{
        .id = 1,
        .user_data = 1,
        .reserved = [_]u8{0} ** 48,
        .ledger = 710,
        .code = 1000,
        .flags = .{ .debits_must_not_exceed_credits = true },
        .debits_pending = 450,
        .debits_posted = 450,
        .credits_pending = 0,
        .credits_posted = 1000,
    };
    const acc_credit_not_exceed_debit_ok = Account{
        .id = 1,
        .user_data = 1,
        .reserved = [_]u8{0} ** 48,
        .ledger = 710,
        .code = 1000,
        .flags = .{ .credits_must_not_exceed_debits = true },
        .debits_pending = 0,
        .debits_posted = 1000,
        .credits_pending = 450,
        .credits_posted = 450,
    };

    // No limit or overflow (.ok):
    try testing.expect(!acc_debit_not_exceed_credit_ok.debits_exceed_credits(100));
    try testing.expect(!acc_credit_not_exceed_debit_ok.credits_exceed_debits(100));
    // Ensure we were at the limit:
    try testing.expect(acc_debit_not_exceed_credit_ok.debits_exceed_credits(101));
    try testing.expect(acc_credit_not_exceed_debit_ok.credits_exceed_debits(101));
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
