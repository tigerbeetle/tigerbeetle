const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.state);
const mem = std.mem;
const Allocator = mem.Allocator;

usingnamespace @import("types.zig");

const HashMapAccounts = std.AutoHashMap(u128, Account);
const HashMapTransfers = std.AutoHashMap(u128, Transfer);

pub const State = struct {
    allocator: *Allocator,
    timestamp: u64,
    accounts: HashMapAccounts,
    transfers: HashMapTransfers,

    pub fn init(allocator: *Allocator, accounts_max: usize, transfers_max: usize) !State {
        var accounts = HashMapAccounts.init(allocator);
        errdefer accounts.deinit();
        try accounts.ensureCapacity(@intCast(u32, accounts_max));

        var transfers = HashMapTransfers.init(allocator);
        errdefer transfers.deinit();
        try transfers.ensureCapacity(@intCast(u32, transfers_max));

        return State {
            .allocator = allocator,
            .timestamp = 0,
            .accounts = accounts,
            .transfers = transfers
        };
    }

    pub fn deinit(self: *State) void {
        self.accounts.deinit();
        self.transfers.deinit();
    }

    pub fn apply(self: *State, command: Command, input: []const u8, output: []u8) usize {
        return switch (command) {
            .reserved => unreachable,
            .ack => unreachable,
            .create_accounts => self.apply_create_accounts(input, output),
            .create_transfers => self.apply_create_transfers(input, output),
            .commit_transfers => self.apply_commit_transfers(input, output),
        };
    }

    pub fn apply_create_accounts(self: *State, input: []const u8, output: []u8) usize {
        const batch = mem.bytesAsSlice(Account, input);
        var results = mem.bytesAsSlice(CreateAccountResults, output);
        var results_count: usize = 0;
        for (batch) |account, index| {
            log.debug("create_accounts {}/{}: {}", .{ index + 1, batch.len, account });
            const result = self.create_account(account);
            log.debug("{}", .{ result });
            if (result != .ok) {
                results[results_count] = .{ .index = @intCast(u32, index), .result = result };
                results_count += 1;
            }
        }
        return results_count * @sizeOf(CreateAccountResults);
    }

    pub fn apply_create_transfers(self: *State, input: []const u8, output: []u8) usize {
        const batch = mem.bytesAsSlice(Transfer, input);
        var results = mem.bytesAsSlice(CreateTransferResults, output);
        var results_count: usize = 0;
        for (batch) |transfer, index| {
            log.debug("create_transfers {}/{}: {}", .{ index + 1, batch.len, transfer });
            const result = self.create_transfer(transfer);
            log.debug("{}", .{ result });
            if (result != .ok) {
                results[results_count] = .{ .index = @intCast(u32, index), .result = result };
                results_count += 1;
            }
        }
        return results_count * @sizeOf(CreateTransferResults);
    }

    pub fn apply_commit_transfers(self: *State, input: []const u8, output: []u8) usize {
        const batch = mem.bytesAsSlice(Commit, input);
        var results = mem.bytesAsSlice(CommitTransferResults, output);
        var results_count: usize = 0;
        for (batch) |commit, index| {
            log.debug("commit_transfers {}/{}: {}", .{ index + 1, batch.len, commit });
            const result = self.commit_transfer(commit);
            log.debug("{}", .{ result });
            if (result != .ok) {
                results[results_count] = .{ .index = @intCast(u32, index), .result = result };
                results_count += 1;
            }
        }
        return results_count * @sizeOf(CommitTransferResults);
    }

    pub fn create_account(self: *State, a: Account) CreateAccountResult {
        assert(a.timestamp > self.timestamp);

        if (a.custom != 0) return .reserved_field_custom;
        if (a.padding != 0) return .reserved_field_padding;

        if (a.flags.reserved != 0) return .reserved_flag;

        // Opening balances may never exceed limits:
        if (a.exceeds_debit_reserved_limit(0)) return .exceeds_debit_reserved_limit;
        if (a.exceeds_debit_accepted_limit(0)) return .exceeds_debit_accepted_limit;
        if (a.exceeds_credit_reserved_limit(0)) return .exceeds_credit_reserved_limit;
        if (a.exceeds_credit_accepted_limit(0)) return .exceeds_credit_accepted_limit;

        // Accounts may never reserve more than they could possibly accept:
        if (a.debit_accepted_limit > 0 and a.debit_reserved_limit > a.debit_accepted_limit) {
            return .debit_reserved_limit_exceeds_debit_accepted_limit;
        }
        if (a.credit_accepted_limit > 0 and a.credit_reserved_limit > a.credit_accepted_limit) {
            return .credit_reserved_limit_exceeds_credit_accepted_limit;
        }

        var hash_map_result = self.accounts.getOrPutAssumeCapacity(a.id);
        if (hash_map_result.found_existing) {
            const exists = hash_map_result.entry.value;
            if (exists.unit != a.unit) return .exists_with_different_unit;
            if (
                exists.debit_reserved_limit != a.debit_reserved_limit or
                exists.debit_accepted_limit != a.debit_accepted_limit or
                exists.credit_reserved_limit != a.credit_reserved_limit or
                exists.credit_accepted_limit != a.credit_accepted_limit
            ) {
                return .exists_with_different_limits;
            }
            if (exists.custom != a.custom) return .exists_with_different_custom_field;
            if (@bitCast(u64, exists.flags) != @bitCast(u64, a.flags)) {
                return .exists_with_different_flags;
            }
            return .exists;
        } else {
            hash_map_result.entry.value = a;
            self.timestamp = a.timestamp;
            return .ok;
        }
    }

    pub fn create_transfer(self: *State, t: Transfer) CreateTransferResult {
        assert(t.timestamp > self.timestamp);

        if (t.custom_1 != 0) return .reserved_field_custom;
        if (t.custom_2 != 0) return .reserved_field_custom;
        if (t.custom_3 != 0) return .reserved_field_custom;

        if (t.flags.reserved != 0) return .reserved_flag;
        if (t.flags.accept and !t.flags.auto_commit) return .reserved_flag_accept;
        if (t.flags.reject) return .reserved_flag_reject;
        if (t.flags.auto_commit) {
            if (!t.flags.accept) return .auto_commit_must_accept;
            if (t.timeout != 0) return .auto_commit_cannot_timeout;
        }

        if (t.amount == 0) return .amount_is_zero;

        if (t.debit_account_id == t.credit_account_id) return .accounts_are_the_same;

        // The etymology of the DR and CR abbreviations for debit and credit is interesting, either:
        // 1. derived from the Latin past participles of debitum and creditum, debere and credere,
        // 2. standing for debit record and credit record, or
        // 3. relating to debtor and creditor.
        // We use them to distinguish between `cr` (credit account), and `c` (commit).
        var dr = self.get_account(t.debit_account_id) orelse return .debit_account_not_found;
        var cr = self.get_account(t.credit_account_id) orelse return .credit_account_not_found;
        
        if (dr.unit != cr.unit) return .accounts_have_different_units;

        if (!t.flags.auto_commit) {
            if (dr.exceeds_debit_reserved_limit(t.amount)) return .exceeds_debit_reserved_limit;
            if (cr.exceeds_credit_reserved_limit(t.amount)) return .exceeds_credit_reserved_limit;
        }
        if (dr.exceeds_debit_accepted_limit(t.amount)) return .exceeds_debit_accepted_limit;
        if (cr.exceeds_credit_accepted_limit(t.amount)) return .exceeds_credit_accepted_limit;
        
        var hash_map_result = self.transfers.getOrPutAssumeCapacity(t.id);
        if (hash_map_result.found_existing) {
            const exists = hash_map_result.entry.value;
            if (exists.debit_account_id != t.debit_account_id) {
                return .exists_with_different_debit_account_id;
            }
            if (exists.credit_account_id != t.credit_account_id) {
                return .exists_with_different_credit_account_id;
            }
            if (
                exists.custom_1 != t.custom_1 or
                exists.custom_2 != t.custom_2 or
                exists.custom_3 != t.custom_3
            ) {
                return .exists_with_different_custom_fields;
            }
            if (exists.amount != t.amount) return .exists_with_different_amount;
            if (exists.timeout != t.timeout) return .exists_with_different_timeout;
            if (@bitCast(u64, exists.flags) != @bitCast(u64, t.flags)) {
                if (!exists.flags.auto_commit and !t.flags.auto_commit) {
                    if (exists.flags.accept) return .exists_and_already_committed_and_accepted;
                    if (exists.flags.reject) return .exists_and_already_committed_and_rejected;
                }
                return .exists_with_different_flags;
            }
            return .exists;
        } else {
            hash_map_result.entry.value = t;
            if (t.flags.auto_commit) {
                dr.debit_accepted += t.amount;
                cr.credit_accepted += t.amount;
            } else {
                dr.debit_reserved += t.amount;
                cr.credit_reserved += t.amount;
            }
            self.timestamp = t.timestamp;
            return .ok;
        }
    }

    pub fn commit_transfer(self: *State, c: Commit) CommitTransferResult {
        assert(c.timestamp > self.timestamp);

        if (c.custom_1 != 0) return .reserved_field_custom;
        if (c.custom_2 != 0) return .reserved_field_custom;
        if (c.custom_3 != 0) return .reserved_field_custom;

        if (c.flags.reserved != 0) return .reserved_flag;
        if (!c.flags.accept and !c.flags.reject) return .commit_must_accept_or_reject;
        if (c.flags.accept and c.flags.reject) return .commit_cannot_accept_and_reject;

        var t = self.get_transfer(c.id) orelse return .transfer_not_found;

        if (t.flags.accept or t.flags.reject) {
            if (t.flags.auto_commit) return .already_auto_committed;
            if (t.flags.accept and c.flags.reject) return .already_committed_but_accepted;
            if (t.flags.reject and c.flags.accept) return .already_committed_but_rejected;
            return .already_committed;
        }

        assert(c.timestamp > t.timestamp);
        if (t.timeout > 0 and t.timestamp + t.timeout <= c.timestamp) return .transfer_expired;
        
        var dr = self.get_account(t.debit_account_id) orelse return .debit_account_not_found;
        var cr = self.get_account(t.credit_account_id) orelse return .credit_account_not_found;

        assert(!t.flags.auto_commit);
        if (dr.debit_reserved < t.amount) return .debit_amount_was_not_reserved;
        if (cr.credit_reserved < t.amount) return .credit_amount_was_not_reserved;

        if (dr.exceeds_debit_accepted_limit(t.amount)) return .exceeds_debit_accepted_limit;
        if (dr.exceeds_credit_accepted_limit(t.amount)) return .exceeds_credit_accepted_limit;

        dr.debit_reserved -= t.amount;
        cr.credit_reserved -= t.amount;
        if (c.flags.accept) {
            t.flags.accept = true;
            dr.debit_accepted += t.amount;
            cr.credit_accepted += t.amount;
        } else {
            assert(c.flags.reject);
            t.flags.reject = true;
        }
        assert(t.flags.accept or t.flags.reject);
        self.timestamp = c.timestamp;
        return .ok;
    }

    /// This is our core private method for changing balances.
    /// Returns a live pointer to an Account entry in the accounts hash map.
    /// This is intended to lookup an Account and modify balances directly by reference.
    /// This pointer is invalidated if the hash map is resized by another insert, e.g. if we get a
    /// pointer, insert another account without capacity, and then modify this pointer... BOOM!
    /// This is a sharp tool but replaces a lookup, copy and update with a single lookup.
    fn get_account(self: *State, id: u128) ?*Account {
        if (self.accounts.getEntry(id)) |entry| {
            return &entry.value;
        } else {
            return null;
        }
    }

    /// See the above comment for get_account().
    fn get_transfer(self: *State, id: u128) ?*Transfer {
        if (self.transfers.getEntry(id)) |entry| {
            return &entry.value;
        } else {
            return null;
        }
    }
};
