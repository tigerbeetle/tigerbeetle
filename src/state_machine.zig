const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.state_machine);

const tb = @import("tigerbeetle.zig");

const Account = tb.Account;
const AccountFlags = tb.AccountFlags;

const Transfer = tb.Transfer;
const TransferFlags = tb.TransferFlags;

const Commit = tb.Commit;
const CommitFlags = tb.CommitFlags;

const CreateAccountsResult = tb.CreateAccountsResult;
const CreateTransfersResult = tb.CreateTransfersResult;
const CommitTransfersResult = tb.CommitTransfersResult;

const CreateAccountResult = tb.CreateAccountResult;
const CreateTransferResult = tb.CreateTransferResult;
const CommitTransferResult = tb.CommitTransferResult;
const LookupAccountResult = tb.LookupAccountResult;

const HashMapAccounts = std.AutoHashMap(u128, Account);
const HashMapTransfers = std.AutoHashMap(u128, Transfer);
const HashMapCommits = std.AutoHashMap(u128, Commit);

pub const StateMachine = struct {
    pub const Operation = enum(u8) {
        /// Operations reserved by VR protocol (for all state machines):
        reserved,
        init,
        register,

        /// Operations exported by TigerBeetle:
        create_accounts,
        create_transfers,
        commit_transfers,
        lookup_accounts,
        lookup_transfers,
    };

    allocator: std.mem.Allocator,
    prepare_timestamp: u64,
    commit_timestamp: u64,
    accounts: HashMapAccounts,
    transfers: HashMapTransfers,
    commits: HashMapCommits,

    pub fn init(
        allocator: std.mem.Allocator,
        accounts_max: usize,
        transfers_max: usize,
        commits_max: usize,
    ) !StateMachine {
        var accounts = HashMapAccounts.init(allocator);
        errdefer accounts.deinit();
        try accounts.ensureTotalCapacity(@intCast(u32, accounts_max));

        var transfers = HashMapTransfers.init(allocator);
        errdefer transfers.deinit();
        try transfers.ensureTotalCapacity(@intCast(u32, transfers_max));

        var commits = HashMapCommits.init(allocator);
        errdefer commits.deinit();
        try commits.ensureTotalCapacity(@intCast(u32, commits_max));

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
        self.commits.deinit();
    }

    pub fn Event(comptime operation: Operation) type {
        return switch (operation) {
            .create_accounts => Account,
            .create_transfers => Transfer,
            .commit_transfers => Commit,
            .lookup_accounts => u128,
            .lookup_transfers => u128,
            else => unreachable,
        };
    }

    pub fn Result(comptime operation: Operation) type {
        return switch (operation) {
            .create_accounts => CreateAccountsResult,
            .create_transfers => CreateTransfersResult,
            .commit_transfers => CommitTransfersResult,
            .lookup_accounts => Account,
            .lookup_transfers => Transfer,
            else => unreachable,
        };
    }

    /// Returns the header's timestamp.
    pub fn prepare(self: *StateMachine, realtime: i64, operation: Operation, input: []u8) u64 {
        // Guard against the wall clock going backwards by taking the max with timestamps issued:
        self.prepare_timestamp = std.math.max(
            // The cluster `commit_timestamp` may be ahead of our `prepare_timestamp` because this
            // may be our first prepare as a recently elected leader:
            std.math.max(self.prepare_timestamp, self.commit_timestamp) + 1,
            @intCast(u64, realtime),
        );
        assert(self.prepare_timestamp > self.commit_timestamp);

        switch (operation) {
            .init => unreachable,
            .register => {},
            .create_accounts => self.prepare_timestamps(.create_accounts, input),
            .create_transfers => self.prepare_timestamps(.create_transfers, input),
            .commit_transfers => self.prepare_timestamps(.commit_transfers, input),
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
        timestamp: u64,
        operation: Operation,
        input: []const u8,
        output: []u8,
    ) usize {
        _ = client;
        assert(self.commit_timestamp < timestamp);

        defer {
            // If this condition is modified, modify the same condition in `test/state_machine.zig`.
            assert(self.commit_timestamp <= timestamp);
            self.commit_timestamp = timestamp;
        }
        return switch (operation) {
            .init => unreachable,
            .register => 0,
            .create_accounts => self.execute(.create_accounts, input, output),
            .create_transfers => self.execute(.create_transfers, input, output),
            .commit_transfers => self.execute(.commit_transfers, input, output),
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
                .commit_transfers => self.commit_transfer(event),
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
                .create_transfers => self.create_transfer_rollback(event),
                .commit_transfers => self.commit_transfer_rollback(event),
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
            if (exists.unit != a.unit) return .exists_with_different_unit;
            if (exists.code != a.code) return .exists_with_different_code;
            if (@bitCast(u32, exists.flags) != @bitCast(u32, a.flags)) {
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

        if (t.flags.padding != 0) return .reserved_flag_padding;
        if (t.flags.two_phase_commit) {
            // Otherwise reserved amounts may never be released:
            if (t.timeout == 0) return .two_phase_commit_must_timeout;
        } else if (t.timeout != 0) {
            return .timeout_reserved_for_two_phase_commit;
        }
        if (!t.flags.condition and !zeroed_32_bytes(t.reserved)) return .reserved_field;

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
            const exists = insert.value_ptr.*;
            if (exists.debit_account_id != t.debit_account_id) {
                return .exists_with_different_debit_account_id;
            } else if (exists.credit_account_id != t.credit_account_id) {
                return .exists_with_different_credit_account_id;
            }
            if (exists.amount != t.amount) return .exists_with_different_amount;
            if (@bitCast(u32, exists.flags) != @bitCast(u32, t.flags)) {
                return .exists_with_different_flags;
            }
            if (exists.user_data != t.user_data) return .exists_with_different_user_data;
            if (!equal_32_bytes(exists.reserved, t.reserved)) {
                return .exists_with_different_reserved_field;
            }
            if (exists.timeout != t.timeout) return .exists_with_different_timeout;
            return .exists;
        } else {
            insert.value_ptr.* = t;
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

    fn create_transfer_rollback(self: *StateMachine, t: Transfer) void {
        var dr = self.get_account(t.debit_account_id).?;
        var cr = self.get_account(t.credit_account_id).?;
        if (t.flags.two_phase_commit) {
            dr.debits_reserved -= t.amount;
            cr.credits_reserved -= t.amount;
        } else {
            dr.debits_accepted -= t.amount;
            cr.credits_accepted -= t.amount;
        }
        assert(self.transfers.remove(t.id));
    }

    fn commit_transfer(self: *StateMachine, c: Commit) CommitTransferResult {
        assert(c.timestamp > self.commit_timestamp);

        if (!c.flags.preimage and !zeroed_32_bytes(c.reserved)) return .reserved_field;
        if (c.flags.padding != 0) return .reserved_flag_padding;

        var t = self.get_transfer(c.id) orelse return .transfer_not_found;
        assert(c.timestamp > t.timestamp);

        if (!t.flags.two_phase_commit) return .transfer_not_two_phase_commit;

        if (self.get_commit(c.id)) |exists| {
            if (!exists.flags.reject and c.flags.reject) return .already_committed_but_accepted;
            if (exists.flags.reject and !c.flags.reject) return .already_committed_but_rejected;
            return .already_committed;
        }

        if (t.timeout > 0 and t.timestamp + t.timeout <= c.timestamp) return .transfer_expired;

        if (t.flags.condition) {
            if (!c.flags.preimage) return .condition_requires_preimage;
            if (!valid_preimage(t.reserved, c.reserved)) return .preimage_invalid;
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

        // Once reserved, the amount can be moved from reserved to accepted without breaking limits:
        assert(!dr.debits_exceed_credits(0));
        assert(!cr.credits_exceed_debits(0));

        // TODO We can combine this lookup with the previous lookup if we return `error!void`:
        var insert = self.commits.getOrPutAssumeCapacity(c.id);
        if (insert.found_existing) {
            unreachable;
        } else {
            insert.value_ptr.* = c;
            dr.debits_reserved -= t.amount;
            cr.credits_reserved -= t.amount;
            if (!c.flags.reject) {
                dr.debits_accepted += t.amount;
                cr.credits_accepted += t.amount;
            }
            self.commit_timestamp = c.timestamp;
            return .ok;
        }
    }

    fn commit_transfer_rollback(self: *StateMachine, c: Commit) void {
        assert(self.get_commit(c.id) != null);
        var t = self.get_transfer(c.id).?;
        var dr = self.get_account(t.debit_account_id).?;
        var cr = self.get_account(t.credit_account_id).?;
        dr.debits_reserved += t.amount;
        cr.credits_reserved += t.amount;
        if (!c.flags.reject) {
            dr.debits_accepted -= t.amount;
            cr.credits_accepted -= t.amount;
        }
        assert(self.commits.remove(c.id));
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
    fn get_commit(self: *StateMachine, id: u128) ?*Commit {
        return self.commits.getPtr(id);
    }
};

// TODO Optimize this by precomputing hashes outside and before committing to the state machine.
// If we see that a batch of commits contains commits with preimages, then we will:
// Divide the batch into subsets, dispatch these to multiple threads, store the result in a bitset.
// Then we can simply provide the result bitset to the state machine when committing.
// This will improve crypto performance significantly by a factor of 8x.
fn valid_preimage(condition: [32]u8, preimage: [32]u8) bool {
    var target: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(&preimage, &target, .{});
    return std.crypto.utils.timingSafeEql([32]u8, target, condition);
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
                .debits_reserved = 10,
                .flags = .{ .debits_must_not_exceed_credits = true },
            }),
        },
        Vector{
            .result = .exceeds_credits,
            .object = std.mem.zeroInit(Account, .{
                .id = 4,
                .timestamp = 1,
                .debits_accepted = 10,
                .flags = .{ .debits_must_not_exceed_credits = true },
            }),
        },
        Vector{
            .result = .exceeds_debits,
            .object = std.mem.zeroInit(Account, .{
                .id = 5,
                .timestamp = 1,
                .credits_reserved = 10,
                .flags = .{ .credits_must_not_exceed_debits = true },
            }),
        },
        Vector{
            .result = .exceeds_debits,
            .object = std.mem.zeroInit(Account, .{
                .id = 6,
                .timestamp = 1,
                .credits_accepted = 10,
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
                .unit = 9,
            }),
        },
        Vector{
            .result = .exists_with_different_unit,
            .object = std.mem.zeroInit(Account, .{
                .id = 8,
                .timestamp = 3,
                .user_data = 'U',
                .unit = 10,
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
    const commits_max = 0;

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

    var state_machine = try StateMachine.init(allocator, accounts_max, transfers_max, commits_max);
    defer state_machine.deinit();

    const input = std.mem.asBytes(&accounts);
    const output = try allocator.alloc(u8, 4096);

    const prepare_timestamp = state_machine.prepare(0, .create_accounts, input);
    const size = state_machine.commit(0, prepare_timestamp, .create_accounts, input, output);
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
        std.mem.zeroInit(Account, .{ .id = 3, .unit = 1 }),
        std.mem.zeroInit(Account, .{ .id = 4, .unit = 2 }),
        std.mem.zeroInit(Account, .{ .id = 5, .flags = .{ .debits_must_not_exceed_credits = true } }),
        std.mem.zeroInit(Account, .{ .id = 6, .flags = .{ .credits_must_not_exceed_debits = true } }),
        std.mem.zeroInit(Account, .{ .id = 7 }),
        std.mem.zeroInit(Account, .{ .id = 8 }),
    };

    var state_machine = try StateMachine.init(allocator, accounts.len, 1, 0);
    defer state_machine.deinit();

    const input = std.mem.asBytes(&accounts);
    const output = try allocator.alloc(u8, 4096);

    const prepare_timestamp = state_machine.prepare(0, .create_accounts, input);
    const size = state_machine.commit(0, prepare_timestamp, .create_accounts, input, output);

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
            .result = .two_phase_commit_must_timeout,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 3,
                .timestamp = timestamp,
                .flags = .{ .two_phase_commit = true },
            }),
        },
        Vector{
            .result = .timeout_reserved_for_two_phase_commit,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 4,
                .timestamp = timestamp,
                .timeout = 1,
            }),
        },
        Vector{
            .result = .reserved_field,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 5,
                .timestamp = timestamp,
                .flags = .{ .condition = false },
                .reserved = [_]u8{1} ** 32,
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
            .result = .accounts_have_different_units,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 9,
                .timestamp = timestamp,
                .amount = 10,
                .debit_account_id = 3,
                .credit_account_id = 4,
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
                .amount = 10,
                .debit_account_id = 7,
                .credit_account_id = 8,
                .flags = .{ .condition = true },
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
                .flags = .{ .condition = true },
                .reserved = [_]u8{1} ** 32,
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
                .flags = .{ .condition = true },
                .reserved = [_]u8{2} ** 32,
            }),
        },
        Vector{
            .result = .timeout_reserved_for_two_phase_commit,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 13,
                .timestamp = timestamp + 2,
                .amount = 10,
                .debit_account_id = 7,
                .credit_account_id = 8,
                .flags = .{ .condition = true },
                .reserved = [_]u8{1} ** 32,
                .timeout = 10,
            }),
        },
        Vector{
            .result = .two_phase_commit_must_timeout,
            .object = std.mem.zeroInit(Transfer, .{
                .id = 14,
                .timestamp = timestamp + 2,
                .amount = 10,
                .debit_account_id = 7,
                .credit_account_id = 8,
                .flags = .{ .two_phase_commit = true },
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
                .flags = .{ .two_phase_commit = true },
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
                .flags = .{ .two_phase_commit = true },
                .timeout = 25,
            }),
        },
    };

    for (vectors) |vector| {
        try testing.expectEqual(vector.result, state_machine.create_transfer(vector.object));
        if (vector.result == .ok) {
            try testing.expectEqual(vector.object, state_machine.get_transfer(vector.object.id).?.*);
        }
    }

    // 2 phase commit [reserved]:
    try testing.expectEqual(@as(u64, 10), state_machine.get_account(7).?.*.debits_reserved);
    try testing.expectEqual(@as(u64, 0), state_machine.get_account(7).?.*.credits_reserved);
    try testing.expectEqual(@as(u64, 10), state_machine.get_account(8).?.*.credits_reserved);
    try testing.expectEqual(@as(u64, 0), state_machine.get_account(8).?.*.debits_reserved);
    // 1 phase commit [accepted]:
    try testing.expectEqual(@as(u64, 20), state_machine.get_account(7).?.*.debits_accepted);
    try testing.expectEqual(@as(u64, 0), state_machine.get_account(7).?.*.credits_accepted);
    try testing.expectEqual(@as(u64, 20), state_machine.get_account(8).?.*.credits_accepted);
    try testing.expectEqual(@as(u64, 0), state_machine.get_account(8).?.*.debits_accepted);

    // Rollback transfer with id [12], amount of 10:
    state_machine.create_transfer_rollback(state_machine.get_transfer(vectors[11].object.id).?.*);
    try testing.expectEqual(@as(u64, 10), state_machine.get_account(7).?.*.debits_accepted);
    try testing.expectEqual(@as(u64, 0), state_machine.get_account(7).?.*.credits_accepted);
    try testing.expectEqual(@as(u64, 10), state_machine.get_account(8).?.*.credits_accepted);
    try testing.expectEqual(@as(u64, 0), state_machine.get_account(8).?.*.debits_accepted);
    try testing.expect(state_machine.get_transfer(vectors[11].object.id) == null);

    // Rollback transfer with id [15], amount of 10:
    state_machine.create_transfer_rollback(state_machine.get_transfer(vectors[22].object.id).?.*);
    try testing.expectEqual(@as(u64, 0), state_machine.get_account(7).?.*.debits_reserved);
    try testing.expectEqual(@as(u64, 0), state_machine.get_account(8).?.*.credits_reserved);
    try testing.expect(state_machine.get_transfer(vectors[22].object.id) == null);
}

test "create/lookup/rollback commits" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    const Vector = struct { result: CommitTransferResult, object: Commit };

    var accounts = [_]Account{
        std.mem.zeroInit(Account, .{ .id = 1 }),
        std.mem.zeroInit(Account, .{ .id = 2 }),
        std.mem.zeroInit(Account, .{ .id = 3 }),
        std.mem.zeroInit(Account, .{ .id = 4 }),
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
            .flags = .{ .two_phase_commit = true },
            .timeout = 25,
        }),
        std.mem.zeroInit(Transfer, .{
            .id = 3,
            .amount = 15,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .flags = .{ .two_phase_commit = true },
            .timeout = 25,
        }),
        std.mem.zeroInit(Transfer, .{
            .id = 4,
            .amount = 15,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .flags = .{ .two_phase_commit = true },
            .timeout = 1,
        }),
        std.mem.zeroInit(Transfer, .{
            .id = 5,
            .amount = 15,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .flags = .{
                .two_phase_commit = true,
                .condition = true,
            },
            .timeout = 25,
        }),
        std.mem.zeroInit(Transfer, .{
            .id = 6,
            .amount = 15,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .flags = .{
                .two_phase_commit = true,
                .condition = false,
            },
            .timeout = 25,
        }),
        std.mem.zeroInit(Transfer, .{
            .id = 7,
            .amount = 15,
            .debit_account_id = 3,
            .credit_account_id = 4,
            .flags = .{ .two_phase_commit = true },
            .timeout = 25,
        }),
    };

    var state_machine = try StateMachine.init(allocator, accounts.len, transfers.len, 1);
    defer state_machine.deinit();

    const input = std.mem.asBytes(&accounts);
    const output = try allocator.alloc(u8, 4096);

    // Accounts:
    const accounts_timestamp = state_machine.prepare(0, .create_accounts, input);
    const size = state_machine.commit(0, accounts_timestamp, .create_accounts, input, output);
    {
        const errors = std.mem.bytesAsSlice(CreateAccountsResult, output[0..size]);
        try testing.expectEqual(@as(usize, 0), errors.len);
    }

    for (accounts) |account| {
        try testing.expectEqual(account, state_machine.get_account(account.id).?.*);
    }

    // Transfers:
    const object_transfers = std.mem.asBytes(&transfers);
    const output_transfers = try allocator.alloc(u8, 4096);

    const transfers_timestamp = state_machine.prepare(0, .create_transfers, object_transfers);
    try testing.expect(transfers_timestamp > accounts_timestamp);
    const size_transfers = state_machine.commit(
        0,
        transfers_timestamp,
        .create_transfers,
        object_transfers,
        output_transfers,
    );
    const errors = std.mem.bytesAsSlice(CreateTransfersResult, output_transfers[0..size_transfers]);
    try testing.expectEqual(@as(usize, 0), errors.len);

    for (transfers) |transfer| {
        try testing.expectEqual(transfer, state_machine.get_transfer(transfer.id).?.*);
    }

    // Commits:
    const timestamp: u64 = (state_machine.commit_timestamp + 1);
    const vectors = [_]Vector{
        Vector{
            .result = .reserved_field,
            .object = std.mem.zeroInit(Commit, .{
                .id = 1,
                .timestamp = timestamp,
                .reserved = [_]u8{1} ** 32,
            }),
        },
        Vector{
            .result = .reserved_flag_padding,
            .object = std.mem.zeroInit(Commit, .{
                .id = 1,
                .timestamp = timestamp,
                .flags = .{ .padding = 1 },
            }),
        },
        Vector{
            .result = .transfer_not_found,
            .object = std.mem.zeroInit(Commit, .{
                .id = 777,
                .timestamp = timestamp,
            }),
        },
        Vector{
            .result = .transfer_not_two_phase_commit,
            .object = std.mem.zeroInit(Commit, .{
                .id = 1,
                .timestamp = timestamp,
            }),
        },
        Vector{
            .result = .ok,
            .object = std.mem.zeroInit(Commit, .{
                .id = 2,
                .timestamp = timestamp,
            }),
        },
        Vector{
            .result = .already_committed_but_accepted,
            .object = std.mem.zeroInit(Commit, .{
                .id = 2,
                .timestamp = timestamp + 1,
                .flags = .{ .reject = true },
            }),
        },
        Vector{
            .result = .already_committed,
            .object = std.mem.zeroInit(Commit, .{
                .id = 2,
                .timestamp = timestamp + 1,
            }),
        },
        Vector{
            .result = .ok,
            .object = std.mem.zeroInit(Commit, .{
                .id = 3,
                .timestamp = timestamp + 1,
                .flags = .{ .reject = true },
            }),
        },
        Vector{
            .result = .already_committed_but_rejected,
            .object = std.mem.zeroInit(Commit, .{
                .id = 3,
                .timestamp = timestamp + 2,
            }),
        },
        Vector{
            .result = .transfer_expired,
            .object = std.mem.zeroInit(Commit, .{
                .id = 4,
                .timestamp = timestamp + 2,
            }),
        },
        Vector{
            .result = .condition_requires_preimage,
            .object = std.mem.zeroInit(Commit, .{
                .id = 5,
                .timestamp = timestamp + 2,
            }),
        },
        Vector{
            .result = .preimage_invalid,
            .object = std.mem.zeroInit(Commit, .{
                .id = 5,
                .timestamp = timestamp + 2,
                .flags = .{ .preimage = true },
                .reserved = [_]u8{1} ** 32,
            }),
        },
        Vector{
            .result = .preimage_requires_condition,
            .object = std.mem.zeroInit(Commit, .{
                .id = 6,
                .timestamp = timestamp + 2,
                .flags = .{ .preimage = true },
            }),
        },
    };

    // Test balances BEFORE commit
    // Account 1:
    const account_1_before = state_machine.get_account(1).?.*;
    try testing.expectEqual(@as(u64, 15), account_1_before.debits_accepted);
    try testing.expectEqual(@as(u64, 75), account_1_before.debits_reserved);
    try testing.expectEqual(@as(u64, 0), account_1_before.credits_accepted);
    try testing.expectEqual(@as(u64, 0), account_1_before.credits_reserved);
    // Account 2:
    const account_2_before = state_machine.get_account(2).?.*;
    try testing.expectEqual(@as(u64, 0), account_2_before.debits_accepted);
    try testing.expectEqual(@as(u64, 0), account_2_before.debits_reserved);
    try testing.expectEqual(@as(u64, 15), account_2_before.credits_accepted);
    try testing.expectEqual(@as(u64, 75), account_2_before.credits_reserved);

    for (vectors) |vector| {
        try testing.expectEqual(vector.result, state_machine.commit_transfer(vector.object));
        if (vector.result == .ok) {
            try testing.expectEqual(vector.object, state_machine.get_commit(vector.object.id).?.*);
        }
    }

    // Test balances AFTER commit
    // Account 1:
    const account_1_after = state_machine.get_account(1).?.*;
    try testing.expectEqual(@as(u64, 30), account_1_after.debits_accepted);
    // +15 (acceptance applied):
    try testing.expectEqual(@as(u64, 45), account_1_after.debits_reserved);
    // -15 (reserved moved):
    try testing.expectEqual(@as(u64, 0), account_1_after.credits_accepted);
    try testing.expectEqual(@as(u64, 0), account_1_after.credits_reserved);
    // Account 2:
    const account_2_after = state_machine.get_account(2).?.*;
    try testing.expectEqual(@as(u64, 0), account_2_after.debits_accepted);
    try testing.expectEqual(@as(u64, 0), account_2_after.debits_reserved);
    // +15 (acceptance applied):
    try testing.expectEqual(@as(u64, 30), account_2_after.credits_accepted);
    // -15 (reserved moved):
    try testing.expectEqual(@as(u64, 45), account_2_after.credits_reserved);

    // Test COMMIT with invalid debit/credit accounts
    state_machine.create_account_rollback(accounts[3]);
    try testing.expect(state_machine.get_account(accounts[3].id) == null);
    try testing.expectEqual(
        state_machine.commit_transfer(std.mem.zeroInit(Commit, .{
            .id = 7,
            .timestamp = timestamp + 2,
        })),
        .credit_account_not_found,
    );
    state_machine.create_account_rollback(accounts[2]);
    try testing.expect(state_machine.get_account(accounts[2].id) == null);
    try testing.expectEqual(
        state_machine.commit_transfer(std.mem.zeroInit(Commit, .{
            .id = 7,
            .timestamp = timestamp + 2,
        })),
        .debit_account_not_found,
    );

    // Rollback [id=2] not rejected:
    state_machine.commit_transfer_rollback(vectors[4].object);

    // Account 1:
    const account_1_rollback = state_machine.get_account(1).?.*;
    // -15 (rollback):
    try testing.expectEqual(@as(u64, 15), account_1_rollback.debits_accepted);
    try testing.expectEqual(@as(u64, 60), account_1_rollback.debits_reserved);
    try testing.expectEqual(@as(u64, 0), account_1_rollback.credits_accepted);
    try testing.expectEqual(@as(u64, 0), account_1_rollback.credits_reserved);
    // Account 2:
    const account_2_rollback = state_machine.get_account(2).?.*;
    try testing.expectEqual(@as(u64, 0), account_2_rollback.debits_accepted);
    try testing.expectEqual(@as(u64, 0), account_2_rollback.debits_reserved);
    // -15 (rollback):
    try testing.expectEqual(@as(u64, 15), account_2_rollback.credits_accepted);
    try testing.expectEqual(@as(u64, 60), account_2_rollback.credits_reserved);

    // Rollback [id=3] rejected:
    state_machine.commit_transfer_rollback(vectors[7].object);
    // Account 1:
    const account_1_rollback_reject = state_machine.get_account(1).?.*;
    try testing.expectEqual(@as(u64, 15), account_1_rollback_reject.debits_accepted);
    // Remains unchanged:
    try testing.expectEqual(@as(u64, 75), account_1_rollback_reject.debits_reserved);
    // +15 rolled back:
    try testing.expectEqual(@as(u64, 0), account_1_rollback_reject.credits_accepted);
    try testing.expectEqual(@as(u64, 0), account_1_rollback_reject.credits_reserved);
    // Account 2:
    const account_2_rollback_reject = state_machine.get_account(2).?.*;
    try testing.expectEqual(@as(u64, 0), account_2_rollback_reject.debits_accepted);
    try testing.expectEqual(@as(u64, 0), account_2_rollback_reject.debits_reserved);
    try testing.expectEqual(@as(u64, 15), account_2_rollback_reject.credits_accepted);
    // +15 rolled back"
    try testing.expectEqual(@as(u64, 75), account_2_rollback_reject.credits_reserved);
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
