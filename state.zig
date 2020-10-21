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
            .create_transfers => unreachable, // TODO
            .commit_transfers => unreachable, // TODO
        };
    }

    pub fn apply_create_accounts(self: *State, input: []const u8, output: []u8) usize {
        const batch = mem.bytesAsSlice(Account, input);
        var results = mem.bytesAsSlice(AccountResults, output);
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
        return results_count * @sizeOf(AccountResults);
    }

    pub fn create_account(self: *State, a: Account) AccountResult {
        assert(a.timestamp > self.timestamp);

        if (a.custom != 0) return .reserved_field_custom;
        if (a.flags != 0) return .reserved_field_flags;
        if (a.padding != 0) return .reserved_field_padding;

        // Amounts may never exceed limits:
        if (a.debit_reserved_limit > 0 and a.debit_reserved > a.debit_reserved_limit) {
            return .debit_reserved_exceeds_debit_reserved_limit;
        }
        if (a.debit_accepted_limit > 0 and a.debit_accepted > a.debit_accepted_limit) {
            return .debit_accepted_exceeds_debit_accepted_limit;
        }
        if (a.credit_reserved_limit > 0 and a.credit_reserved > a.credit_reserved_limit) {
            return .credit_reserved_exceeds_credit_reserved_limit;
        }
        if (a.credit_accepted_limit > 0 and a.credit_accepted > a.credit_accepted_limit) {
            return .credit_accepted_exceeds_credit_accepted_limit;
        }

        // Accounts may never reserve more than they could possibly accept:
        if (a.debit_accepted_limit > 0 and a.debit_reserved_limit > a.debit_accepted_limit) {
            return .debit_reserved_limit_exceeds_debit_accepted_limit;
        }
        if (a.credit_accepted_limit > 0 and a.credit_reserved_limit > a.credit_accepted_limit) {
            return .credit_reserved_limit_exceeds_credit_accepted_limit;
        }

        var hash_map_result = self.accounts.getOrPutAssumeCapacity(a.id);
        if (hash_map_result.found_existing) return .already_exists;

        // Insert:
        hash_map_result.entry.value = a;
        self.timestamp = a.timestamp;

        return .ok;
    }
};
