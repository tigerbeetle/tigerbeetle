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
            .commit_transfers => unreachable, // TODO
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

        // Insert:
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
        }
        hash_map_result.entry.value = a;
        self.timestamp = a.timestamp;

        return .ok;
    }

    pub fn create_transfer(self: *State, t: Transfer) CreateTransferResult {
        assert(t.timestamp > self.timestamp);

        if (t.custom_1 != 0) return .reserved_field_custom;
        if (t.custom_2 != 0) return .reserved_field_custom;
        if (t.custom_2 != 0) return .reserved_field_custom;

        if (t.flags.reserved != 0) return .reserved_flag;
        if (t.flags.accept and !t.flags.auto_commit) return .reserved_flag_accept;
        if (t.flags.reject) return .reserved_flag_reject;
        if (t.flags.auto_commit) {
            if (!t.flags.accept) return .auto_commit_must_accept;
            if (t.timeout != 0) return .auto_commit_cannot_timeout;
        }

        if (t.amount == 0) return .amount_is_zero;

        if (t.debit_account_id == t.credit_account_id) return .accounts_are_the_same;

        var d = self.get_account(t.debit_account_id) orelse return .debit_account_does_not_exist;
        var c = self.get_account(t.credit_account_id) orelse return .credit_account_does_not_exist;
        
        if (d.unit != c.unit) return .accounts_have_different_units;

        if (!t.flags.auto_commit) {
            if (d.exceeds_debit_reserved_limit(t.amount)) return .exceeds_debit_reserved_limit;
            if (c.exceeds_credit_reserved_limit(t.amount)) return .exceeds_credit_reserved_limit;
        }
        if (d.exceeds_debit_accepted_limit(t.amount)) return .exceeds_debit_accepted_limit;
        if (c.exceeds_credit_accepted_limit(t.amount)) return .exceeds_credit_accepted_limit;
        
        // Insert:
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
                return .exists_with_different_flags;
            }
            return .exists;
        }
        hash_map_result.entry.value = t;
        if (t.flags.auto_commit) {
            d.debit_accepted += t.amount;
            c.credit_accepted += t.amount;
        } else {
            d.debit_reserved += t.amount;
            c.credit_reserved += t.amount;
        }
        self.timestamp = t.timestamp;

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
};
