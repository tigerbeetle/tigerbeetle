const std = @import("std");
const assert = std.debug.assert;
const crypto = std.crypto;
const log = std.log.scoped(.state_machine);
const mem = std.mem;
const Allocator = mem.Allocator;

usingnamespace @import("tigerbeetle.zig");

const HashMapAccounts = std.AutoHashMap(u128, Account);
const HashMapTransfers = std.AutoHashMap(u128, Transfer);
const HashMapCommits = std.AutoHashMap(u128, Commit);

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
    commits: HashMapCommits,

    pub fn init(allocator: *Allocator, accounts_max: usize, transfers_max: usize) !StateMachine {
        var accounts = HashMapAccounts.init(allocator);
        errdefer accounts.deinit();
        try accounts.ensureCapacity(@intCast(u32, accounts_max));

        var transfers = HashMapTransfers.init(allocator);
        errdefer transfers.deinit();
        try transfers.ensureCapacity(@intCast(u32, transfers_max));

        // TODO Add config for commits_max to enable non-two-phase-commit systems to save memory.
        const commits_max = transfers_max;
        var commits = HashMapCommits.init(allocator);
        errdefer commits.deinit();
        try commits.ensureCapacity(@intCast(u32, commits_max));

        // TODO After recovery, set prepare_timestamp max(wall clock, op timestamp).
        // TODO After recovery, set commit_timestamp max(wall clock, commit timestamp).

        return StateMachine{
            .allocator = allocator,
            .prepare_timestamp = 0,
            .commit_timestamp = 0,
            .accounts = accounts,
            .transfers = transfers,
            .commits = commits,
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

    fn prepare_timestamps(self: *StateMachine, comptime T: type, batch: []u8) void {
        // Guard against the wall clock going backwards by taking the max with timestamps issued:
        self.prepare_timestamp = std.math.max(
            self.prepare_timestamp,
            @intCast(u64, std.time.nanoTimestamp()),
        );
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

    fn apply_create_accounts(self: *StateMachine, input: []const u8, output: []u8) usize {
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

    fn apply_create_transfers(self: *StateMachine, input: []const u8, output: []u8) usize {
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

    fn apply_commit_transfers(self: *StateMachine, input: []const u8, output: []u8) usize {
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

    fn apply_lookup_accounts(self: *StateMachine, input: []const u8, output: []u8) usize {
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

    fn create_account(self: *StateMachine, a: Account) CreateAccountResult {
        assert(a.timestamp > self.commit_timestamp);

        if (!zeroed(&a.reserved)) return .reserved_field;
        if (a.flags.padding != 0) return .reserved_flag_padding;

        // Opening balances may never exceed limits:
        if (a.debits_exceed_credits(0)) return .exceeds_credits;
        if (a.credits_exceed_debits(0)) return .exceeds_debits;

        var insert = self.accounts.getOrPutAssumeCapacity(a.id);
        if (insert.found_existing) {
            const exists = insert.entry.value;
            if (exists.unit != a.unit) return .exists_with_different_unit;
            if (exists.code != a.code) return .exists_with_different_code;
            if (@bitCast(u32, exists.flags) != @bitCast(u32, a.flags)) {
                return .exists_with_different_flags;
            }
            if (exists.user_data != a.user_data) return .exists_with_different_user_data;
            return .exists;
        } else {
            insert.entry.value = a;
            self.commit_timestamp = a.timestamp;
            return .ok;
        }
    }

    fn create_transfer(self: *StateMachine, t: Transfer) CreateTransferResult {
        assert(t.timestamp > self.commit_timestamp);

        if (t.flags.padding != 0) return .reserved_flag_padding;
        if (t.timeout != 0 and !t.flags.two_phase_commit) {
            return .timeout_reserved_for_two_phase_commit;
        }
        if (!t.flags.condition and !zeroed(&t.reserved)) return .reserved_field;

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
        // If the transfer exists, then we should rather return .exists as an error.
        if (dr.debits_exceed_credits(t.amount)) return .exceeds_credits;
        if (cr.credits_exceed_debits(t.amount)) return .exceeds_debits;

        var insert = self.transfers.getOrPutAssumeCapacity(t.id);
        if (insert.found_existing) {
            const exists = insert.entry.value;
            if (exists.debit_account_id != t.debit_account_id) {
                return .exists_with_different_debit_account_id;
            }
            if (exists.credit_account_id != t.credit_account_id) {
                return .exists_with_different_credit_account_id;
            }
            if (exists.amount != t.amount) return .exists_with_different_amount;
            if (@bitCast(u32, exists.flags) != @bitCast(u32, t.flags)) {
                return .exists_with_different_flags;
            }
            if (exists.user_data != t.user_data) return .exists_with_different_user_data;
            if (@bitCast(u256, exists.reserved) != @bitCast(u256, t.reserved)) {
                return .exists_with_different_reserved_field;
            }
            if (exists.timeout != t.timeout) return .exists_with_different_timeout;
            return .exists;
        } else {
            insert.entry.value = t;
            if (t.flags.two_phase_commit) {
                dr.debits_reserved += t.amount;
                cr.credits_reserved += t.amount;
            } else {
                dr.debits_accepted += t.amount;
                cr.credits_accepted += t.amount;
            }
            self.commit_timestamp = t.timestamp;
            return .ok;
        }
    }

    fn commit_transfer(self: *StateMachine, c: Commit) CommitTransferResult {
        assert(c.timestamp > self.commit_timestamp);

        if (!c.flags.preimage and !zeroed(&c.reserved)) return .reserved_field;
        if (c.flags.padding != 0) return .reserved_flag_padding;
        if (!c.flags.accept and !c.flags.reject) return .commit_must_accept_or_reject;
        if (c.flags.accept and c.flags.reject) return .commit_cannot_accept_and_reject;

        var t = self.get_transfer(c.id) orelse return .transfer_not_found;
        assert(c.timestamp > t.timestamp);

        if (!t.flags.two_phase_commit) return .transfer_not_two_phase_commit;

        if (self.get_commit(c.id)) |exists| {
            if (exists.flags.accept and c.flags.reject) return .already_committed_but_accepted;
            if (exists.flags.reject and c.flags.accept) return .already_committed_but_rejected;
            return .already_committed;
        }

        if (t.timeout > 0 and t.timestamp + t.timeout <= c.timestamp) return .transfer_expired;

        if (t.flags.condition) {
            if (!c.flags.preimage) return .condition_requires_preimage;
            // TODO Verify condition.
        } else if (c.flags.preimage) {
            return .preimage_requires_condition;
        }

        var dr = self.get_account(t.debit_account_id) orelse return .debit_account_not_found;
        var cr = self.get_account(t.credit_account_id) orelse return .credit_account_not_found;
        assert(t.timestamp > dr.timestamp);
        assert(t.timestamp > cr.timestamp);

        assert(t.flags.two_phase_commit);
        if (dr.debits_reserved < t.amount) return .debit_amount_was_not_reserved;
        if (cr.credits_reserved < t.amount) return .credit_amount_was_not_reserved;

        // TODO Should we check limits again here?
        // On the one hand, it's possible for a subsequent transfer to change balances.
        // On the other hand, the spirit of two-phase commit is that we reserve resources upfront.

        // TODO We can combine this lookup with the previous lookup if we return `error!void`:
        var insert = self.commits.getOrPutAssumeCapacity(c.id);
        if (insert.found_existing) {
            unreachable;
        } else {
            insert.entry.value = c;
            dr.debits_reserved -= t.amount;
            cr.credits_reserved -= t.amount;
            if (c.flags.accept) {
                dr.debits_accepted += t.amount;
                cr.credits_accepted += t.amount;
            } else {
                assert(c.flags.reject);
            }
            self.commit_timestamp = c.timestamp;
            return .ok;
        }
    }

    fn valid_preimage(condition: u256, preimage: u256) bool {
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

    /// See the comment for get_account().
    fn get_transfer(self: *StateMachine, id: u128) ?*Transfer {
        if (self.transfers.getEntry(id)) |entry| {
            return &entry.value;
        } else {
            return null;
        }
    }

    /// See the comment for get_account().
    fn get_commit(self: *StateMachine, id: u128) ?*Commit {
        if (self.commits.getEntry(id)) |entry| {
            return &entry.value;
        } else {
            return null;
        }
    }
};

fn zeroed(slice: []const u8) bool {
    // TODO Remove all loop branching for comptime known types: 48 bytes, 32 bytes.
    // e.g. Unroll to 3x16 or 2x16 comparisons.
    return std.mem.allEqual(u8, slice, 0);
}
