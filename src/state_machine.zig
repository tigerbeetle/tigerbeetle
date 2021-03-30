const std = @import("std");
const assert = std.debug.assert;
const crypto = std.crypto;
const log = std.log.scoped(.state_machine);
const mem = std.mem;
const Allocator = mem.Allocator;

usingnamespace @import("tigerbeetle.zig");

const HashMapAccounts = std.AutoHashMap(u128, Account);
const HashMapTransfers = std.AutoHashMap(u128, Transfer);

pub const Operation = packed enum(u8) {
    // We reserve command "0" to detect an accidental zero byte being interpreted as an operation:
    reserved,
    init,

    create_accounts,
    create_transfers,
    commit_transfers,
    lookup_accounts,

    pub fn jsonStringify(self: Command, options: StringifyOptions, writer: anytype) !void {
        try std.fmt.format(writer, "\"{}\"", .{@tagName(self)});
    }
};

pub const StateMachine = struct {
    allocator: *Allocator,
    prepare_timestamp: u64,
    commit_timestamp: u64,
    accounts: HashMapAccounts,
    transfers: HashMapTransfers,

    pub fn init(allocator: *Allocator, accounts_max: usize, transfers_max: usize) !StateMachine {
        var accounts = HashMapAccounts.init(allocator);
        errdefer accounts.deinit();
        try accounts.ensureCapacity(@intCast(u32, accounts_max));

        var transfers = HashMapTransfers.init(allocator);
        errdefer transfers.deinit();
        try transfers.ensureCapacity(@intCast(u32, transfers_max));

        // TODO After recovery, set prepare_timestamp max(wall clock, op timestamp).
        // TODO After recovery, set commit_timestamp max(wall clock, commit timestamp).

        return StateMachine{
            .allocator = allocator,
            .prepare_timestamp = 0,
            .commit_timestamp = 0,
            .accounts = accounts,
            .transfers = transfers,
        };
    }

    pub fn deinit(self: *StateMachine) void {
        self.accounts.deinit();
        self.transfers.deinit();
    }

    pub fn prepare(self: *StateMachine, operation: Operation, batch: []u8) void {
        switch (operation) {
            .create_accounts => self.prepare_timestamps(Account, batch),
            .create_transfers => self.prepare_timestamps(Transfer, batch),
            .commit_transfers => self.prepare_timestamps(Commit, batch),
            .lookup_accounts => {},
            else => unreachable,
        }
    }

    pub fn prepare_timestamps(self: *StateMachine, comptime T: type, batch: []u8) void {
        // Guard against the wall clock going backwards by taking the max with timestamps issued:
        self.prepare_timestamp = std.math.max(self.prepare_timestamp, @intCast(u64, std.time.nanoTimestamp()));
        assert(self.prepare_timestamp > self.commit_timestamp);
        var sum_reserved_timestamps: usize = 0;
        for (mem.bytesAsSlice(T, batch)) |*event| {
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
        operation: Operation,
        batch: []const u8,
        results: []u8,
    ) usize {
        return switch (operation) {
            .init => 0,
            .create_accounts => self.apply_create_accounts(batch, results),
            .create_transfers => self.apply_create_transfers(batch, results),
            .commit_transfers => self.apply_commit_transfers(batch, results),
            .lookup_accounts => self.apply_lookup_accounts(batch, results),
            else => unreachable,
        };
    }

    pub fn apply_create_accounts(self: *StateMachine, input: []const u8, output: []u8) usize {
        const batch = mem.bytesAsSlice(Account, input);
        var results = mem.bytesAsSlice(CreateAccountResults, output);
        var results_count: usize = 0;
        for (batch) |event, index| {
            log.debug("create_accounts {}/{}: {}", .{ index + 1, batch.len, event });
            const result = self.create_account(event);
            log.debug("{}", .{result});
            if (result != .ok) {
                results[results_count] = .{ .index = @intCast(u32, index), .result = result };
                results_count += 1;
            }
        }
        return results_count * @sizeOf(CreateAccountResults);
    }

    pub fn apply_create_transfers(self: *StateMachine, input: []const u8, output: []u8) usize {
        const batch = mem.bytesAsSlice(Transfer, input);
        var results = mem.bytesAsSlice(CreateTransferResults, output);
        var results_count: usize = 0;
        for (batch) |event, index| {
            log.debug("create_transfers {}/{}: {}", .{ index + 1, batch.len, event });
            const result = self.create_transfer(event);
            log.debug("{}", .{result});
            if (result != .ok) {
                results[results_count] = .{ .index = @intCast(u32, index), .result = result };
                results_count += 1;
            }
        }
        return results_count * @sizeOf(CreateTransferResults);
    }

    pub fn apply_commit_transfers(self: *StateMachine, input: []const u8, output: []u8) usize {
        const batch = mem.bytesAsSlice(Commit, input);
        var results = mem.bytesAsSlice(CommitTransferResults, output);
        var results_count: usize = 0;
        for (batch) |event, index| {
            log.debug("commit_transfers {}/{}: {}", .{ index + 1, batch.len, event });
            const result = self.commit_transfer(event);
            log.debug("{}", .{result});
            if (result != .ok) {
                results[results_count] = .{ .index = @intCast(u32, index), .result = result };
                results_count += 1;
            }
        }
        return results_count * @sizeOf(CommitTransferResults);
    }

    pub fn apply_lookup_accounts(self: *StateMachine, input: []const u8, output: []u8) usize {
        const batch = mem.bytesAsSlice(u128, input);
        // TODO Do the same for other apply methods:
        var output_len = @divFloor(output.len, @sizeOf(Account)) * @sizeOf(Account);
        var results = mem.bytesAsSlice(Account, output[0..output_len]);
        var results_count: usize = 0;
        for (batch) |id, index| {
            if (self.get_account(id)) |result| {
                results[results_count] = result.*;
                results_count += 1;
            }
        }
        return results_count * @sizeOf(Account);
    }

    pub fn create_account(self: *StateMachine, a: Account) CreateAccountResult {
        assert(a.timestamp > self.commit_timestamp);

        if (a.custom != 0) return .reserved_field_custom;
        if (a.padding != 0) return .reserved_field_padding;

        if (a.flags.padding != 0) return .reserved_flag_padding;

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
            if (exists.debit_reserved_limit != a.debit_reserved_limit or
                exists.debit_accepted_limit != a.debit_accepted_limit or
                exists.credit_reserved_limit != a.credit_reserved_limit or
                exists.credit_accepted_limit != a.credit_accepted_limit)
            {
                return .exists_with_different_limits;
            }
            if (exists.custom != a.custom) return .exists_with_different_custom_field;
            if (@bitCast(u64, exists.flags) != @bitCast(u64, a.flags)) {
                return .exists_with_different_flags;
            }
            return .exists;
        } else {
            hash_map_result.entry.value = a;
            self.commit_timestamp = a.timestamp;
            return .ok;
        }
    }

    pub fn create_transfer(self: *StateMachine, t: Transfer) CreateTransferResult {
        assert(t.timestamp > self.commit_timestamp);

        if (t.flags.padding != 0) return .reserved_flag_padding;
        if (t.flags.accept and !t.flags.auto_commit) return .reserved_flag_accept;
        if (t.flags.reject) return .reserved_flag_reject;
        if (t.flags.auto_commit) {
            if (!t.flags.accept) return .auto_commit_must_accept;
            if (t.timeout != 0) return .auto_commit_cannot_timeout;
        }
        if (t.custom_1 != 0 or t.custom_2 != 0) {
            if (!t.flags.condition) return .reserved_field_custom;
        }
        if (t.custom_3 != 0) return .reserved_field_custom;

        if (t.amount == 0) return .amount_is_zero;

        if (t.debit_account_id == t.credit_account_id) return .accounts_are_the_same;

        // The etymology of the DR and CR abbreviations for debit and credit is interesting, either:
        // 1. derived from the Latin past participles of debitum and creditum, debere and credere,
        // 2. standing for debit record and credit record, or
        // 3. relating to debtor and creditor.
        // We use them to distinguish between `cr` (credit account), and `c` (commit).
        var dr = self.get_account(t.debit_account_id) orelse return .debit_account_not_found;
        var cr = self.get_account(t.credit_account_id) orelse return .credit_account_not_found;
        assert(t.timestamp > dr.timestamp);
        assert(t.timestamp > cr.timestamp);

        if (dr.unit != cr.unit) return .accounts_have_different_units;

        // TODO We need a lookup before inserting in case transfer exists and would overflow limits.
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
            if (exists.custom_1 != t.custom_1 or
                exists.custom_2 != t.custom_2 or
                exists.custom_3 != t.custom_3)
            {
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
            self.commit_timestamp = t.timestamp;
            return .ok;
        }
    }

    pub fn commit_transfer(self: *StateMachine, c: Commit) CommitTransferResult {
        assert(c.timestamp > self.commit_timestamp);

        if (c.flags.padding != 0) return .reserved_flag_padding;
        if (!c.flags.accept and !c.flags.reject) return .commit_must_accept_or_reject;
        if (c.flags.accept and c.flags.reject) return .commit_cannot_accept_and_reject;

        if (c.custom_1 != 0 or c.custom_2 != 0) {
            if (!c.flags.preimage) return .reserved_field_custom;
        }
        if (c.custom_3 != 0) return .reserved_field_custom;

        var t = self.get_transfer(c.id) orelse return .transfer_not_found;
        assert(c.timestamp > t.timestamp);

        if (t.flags.accept or t.flags.reject) {
            if (t.flags.auto_commit) return .already_auto_committed;
            if (t.flags.accept and c.flags.reject) return .already_committed_but_accepted;
            if (t.flags.reject and c.flags.accept) return .already_committed_but_rejected;
            return .already_committed;
        }

        if (t.timeout > 0 and t.timestamp + t.timeout <= c.timestamp) return .transfer_expired;

        if (t.flags.condition) {
            if (!c.flags.preimage) return .condition_requires_preimage;
        } else if (c.flags.preimage) {
            return .preimage_requires_condition;
        }

        var dr = self.get_account(t.debit_account_id) orelse return .debit_account_not_found;
        var cr = self.get_account(t.credit_account_id) orelse return .credit_account_not_found;
        assert(t.timestamp > dr.timestamp);
        assert(t.timestamp > cr.timestamp);

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
        self.commit_timestamp = c.timestamp;
        return .ok;
    }

    pub fn valid_preimage(condition: u256, preimage: u256) bool {
        var target: [32]u8 = undefined;
        crypto.hash.sha2.Sha256.hash(@bitCast([32]u8, preimage)[0..], target[0..], .{});
        return mem.eql(u8, target[0..], @bitCast([32]u8, condition)[0..]);
    }

    /// This is our core private method for changing balances.
    /// Returns a live pointer to an Account entry in the accounts hash map.
    /// This is intended to lookup an Account and modify balances directly by reference.
    /// This pointer is invalidated if the hash map is resized by another insert, e.g. if we get a
    /// pointer, insert another account without capacity, and then modify this pointer... BOOM!
    /// This is a sharp tool but replaces a lookup, copy and update with a single lookup.
    fn get_account(self: *StateMachine, id: u128) ?*Account {
        if (self.accounts.getEntry(id)) |entry| {
            return &entry.value;
        } else {
            return null;
        }
    }

    /// See the above comment for get_account().
    fn get_transfer(self: *StateMachine, id: u128) ?*Transfer {
        if (self.transfers.getEntry(id)) |entry| {
            return &entry.value;
        } else {
            return null;
        }
    }
};
