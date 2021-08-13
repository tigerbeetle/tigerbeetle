const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.state_machine);

usingnamespace @import("tigerbeetle.zig");

const vr = @import("vr.zig");
const ObjectStore = vr.ObjectStore;

const Accounts = ObjectStore(Account, .copy_on_write);
const Transfers = ObjectStore(Transfer, .append_only);
const Commits = ObjectStore(Commit, .append_only);

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

        pub fn jsonStringify(self: Command, options: StringifyOptions, writer: anytype) !void {
            try std.fmt.format(writer, "\"{}\"", .{@tagName(self)});
        }
    };

    allocator: *std.mem.Allocator,
    prepare_timestamp: u64,
    commit_timestamp: u64,
    accounts: Accounts,
    transfers: Transfers,
    commits: Commits,

    pub fn init(
        allocator: *std.mem.Allocator,
        accounts_max: usize,
        transfers_max: usize,
        commits_max: usize,
    ) !StateMachine {
        var accounts = try Accounts.init(allocator, accounts_max);
        errdefer accounts.deinit(allocator);

        var transfers = try Transfers.init(allocator, transfers_max);
        errdefer transfers.deinit(allocator);

        var commits = try Commits.init(allocator, commits_max);
        errdefer commits.deinit(allocator);

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
        self.accounts.deinit(self.allocator);
        self.transfers.deinit(self.allocator);
        self.commits.deinit(self.allocator);
    }

    pub fn Event(comptime operation: Operation) type {
        return switch (operation) {
            .create_accounts => Account,
            .create_transfers => Transfer,
            .commit_transfers => Commit,
            .lookup_accounts => u128,
            else => unreachable,
        };
    }

    pub fn Result(comptime operation: Operation) type {
        return switch (operation) {
            .create_accounts => CreateAccountsResult,
            .create_transfers => CreateTransfersResult,
            .commit_transfers => CommitTransfersResult,
            .lookup_accounts => Account,
            else => unreachable,
        };
    }

    pub fn prepare(self: *StateMachine, realtime: i64, operation: Operation, input: []u8) void {
        switch (operation) {
            .init => unreachable,
            .register => {},
            .create_accounts => self.prepare_timestamps(realtime, .create_accounts, input),
            .create_transfers => self.prepare_timestamps(realtime, .create_transfers, input),
            .commit_transfers => self.prepare_timestamps(realtime, .commit_transfers, input),
            .lookup_accounts => {},
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
        operation: Operation,
        input: []const u8,
        output: []u8,
    ) usize {
        return switch (operation) {
            .init => unreachable,
            .register => 0,
            .create_accounts => self.execute(.create_accounts, input, output),
            .create_transfers => self.execute(.create_transfers, input, output),
            .commit_transfers => self.execute(.commit_transfers, input, output),
            .lookup_accounts => self.execute_lookup_accounts(input, output),
            else => unreachable,
        };
    }

    fn execute(
        self: *StateMachine,
        comptime operation: Operation,
        input: []const u8,
        output: []u8,
    ) usize {
        comptime assert(operation != .lookup_accounts);

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
        var output_len = @divFloor(output.len, @sizeOf(Account)) * @sizeOf(Account);
        var results = std.mem.bytesAsSlice(Account, output[0..output_len]);
        var results_count: usize = 0;
        for (batch) |id, index| {
            if (self.accounts.get_const(id)) |result| {
                results[results_count] = result.*;
                results_count += 1;
            }
        }
        return results_count * @sizeOf(Account);
    }

    fn create_account(self: *StateMachine, a: Account) CreateAccountResult {
        assert(a.timestamp > self.commit_timestamp);

        if (!zeroed_48_bytes(a.reserved)) return .reserved_field;
        if (a.flags.padding != 0) return .reserved_flag_padding;

        // Opening balances may never exceed limits:
        if (a.debits_exceed_credits(0)) return .exceeds_credits;
        if (a.credits_exceed_debits(0)) return .exceeds_debits;

        const account = self.accounts.get_or_append(a.id);
        if (account.exists) {
            const existing = account.object;
            if (existing.unit != a.unit) return .exists_with_different_unit;
            if (existing.code != a.code) return .exists_with_different_code;
            if (@bitCast(u32, existing.flags) != @bitCast(u32, a.flags)) {
                return .exists_with_different_flags;
            }
            if (existing.user_data != a.user_data) return .exists_with_different_user_data;
            if (!equal_48_bytes(existing.reserved, a.reserved)) {
                return .exists_with_different_reserved_field;
            }
            return .exists;
        } else {
            account.object.* = a;
            self.commit_timestamp = a.timestamp;
            return .ok;
        }
    }

    fn create_account_rollback(self: *StateMachine, a: Account) void {
        self.accounts.rollback(a.id);
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
        const dr = self.accounts.get(t.debit_account_id) orelse return .debit_account_not_found;
        const cr = self.accounts.get(t.credit_account_id) orelse return .credit_account_not_found;
        assert(t.timestamp > dr.timestamp);
        assert(t.timestamp > cr.timestamp);

        if (dr.unit != cr.unit) return .accounts_have_different_units;

        // TODO We need a lookup before inserting in case transfer exists and would overflow limits.
        // If the transfer exists, then we should rather return .exists as an error.
        if (dr.debits_exceed_credits(t.amount)) return .exceeds_credits;
        if (cr.credits_exceed_debits(t.amount)) return .exceeds_debits;

        const transfer = self.transfers.get_or_append(t.id);
        if (transfer.exists) {
            const existing = transfer.object;
            if (existing.debit_account_id != t.debit_account_id) {
                return .exists_with_different_debit_account_id;
            }
            if (existing.credit_account_id != t.credit_account_id) {
                return .exists_with_different_credit_account_id;
            }
            if (existing.amount != t.amount) return .exists_with_different_amount;
            if (@bitCast(u32, existing.flags) != @bitCast(u32, t.flags)) {
                return .exists_with_different_flags;
            }
            if (existing.user_data != t.user_data) return .exists_with_different_user_data;
            if (!equal_32_bytes(existing.reserved, t.reserved)) {
                return .exists_with_different_reserved_field;
            }
            if (existing.timeout != t.timeout) return .exists_with_different_timeout;
            return .exists;
        } else {
            transfer.object.* = t;
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
        const dr = self.accounts.get(t.debit_account_id).?;
        const cr = self.accounts.get(t.credit_account_id).?;
        if (t.flags.two_phase_commit) {
            dr.debits_reserved -= t.amount;
            cr.credits_reserved -= t.amount;
        } else {
            dr.debits_accepted -= t.amount;
            cr.credits_accepted -= t.amount;
        }
        self.transfers.rollback(t.id);
    }

    fn commit_transfer(self: *StateMachine, c: Commit) CommitTransferResult {
        assert(c.timestamp > self.commit_timestamp);

        if (!c.flags.preimage and !zeroed_32_bytes(c.reserved)) return .reserved_field;
        if (c.flags.padding != 0) return .reserved_flag_padding;

        const t = self.transfers.get_const(c.id) orelse return .transfer_not_found;
        assert(c.timestamp > t.timestamp);

        if (!t.flags.two_phase_commit) return .transfer_not_two_phase_commit;

        if (self.commits.get_const(c.id)) |existing| {
            if (!existing.flags.reject and c.flags.reject) return .already_committed_but_accepted;
            if (existing.flags.reject and !c.flags.reject) return .already_committed_but_rejected;
            return .already_committed;
        }

        if (t.timeout > 0 and t.timestamp + t.timeout <= c.timestamp) return .transfer_expired;

        if (t.flags.condition) {
            if (!c.flags.preimage) return .condition_requires_preimage;
            if (!valid_preimage(t.reserved, c.reserved)) return .preimage_invalid;
        } else if (c.flags.preimage) {
            return .preimage_requires_condition;
        }

        const dr = self.accounts.get(t.debit_account_id) orelse return .debit_account_not_found;
        const cr = self.accounts.get(t.credit_account_id) orelse return .credit_account_not_found;
        assert(t.timestamp > dr.timestamp);
        assert(t.timestamp > cr.timestamp);

        assert(t.flags.two_phase_commit);
        if (dr.debits_reserved < t.amount) return .debit_amount_was_not_reserved;
        if (cr.credits_reserved < t.amount) return .credit_amount_was_not_reserved;

        // Once reserved, the amount can be moved from reserved to accepted without breaking limits:
        assert(!dr.debits_exceed_credits(0));
        assert(!cr.credits_exceed_debits(0));

        // TODO We can combine this lookup with the previous lookup if we return `error!void`:
        const object = self.commits.append(c.id);
        object.* = c;
        dr.debits_reserved -= t.amount;
        cr.credits_reserved -= t.amount;
        if (!c.flags.reject) {
            dr.debits_accepted += t.amount;
            cr.credits_accepted += t.amount;
        }
        self.commit_timestamp = c.timestamp;
        return .ok;
    }

    fn commit_transfer_rollback(self: *StateMachine, c: Commit) void {
        const t = self.transfers.get_const(c.id).?;
        const dr = self.accounts.get(t.debit_account_id).?;
        const cr = self.accounts.get(t.credit_account_id).?;
        dr.debits_reserved += t.amount;
        cr.credits_reserved += t.amount;
        if (!c.flags.reject) {
            dr.debits_accepted -= t.amount;
            cr.credits_accepted -= t.amount;
        }
        self.commits.rollback(c.id);
    }

    pub const Snapshot = struct {
        pub const Page = struct {
            bytes: *align(config.sector_size) const [config.snapshot_page_size]u8,
            user_data: u64,
        };

        self: *StateMachine,

        accounts: Accounts.Snapshot,
        transfers: Transfers.Snapshot,
        commits: Commits.Snapshot,

        state: enum {
            accounts,
            transfers,
            commits,
            done,
        } = .accounts,

        pub fn next_page(snapshot: *Snapshot) ?Page {
            assert(snapshot.state != .done);
            while (true) {
                switch (snapshot.state) {
                    .accounts => return snapshot.accounts.next_page() orelse {
                        snapshot.state = .transfers;
                        continue;
                    },
                    .transfers => return snapshot.transfers.next_page() orelse {
                        snapshot.state = .commits;
                        continue;
                    },
                    .commits => return snapshot.commits.next_page() orelse {
                        snapshot.state = .done;
                        continue;
                    },
                    .done => return null,
                }
            }
        }
    };

    pub fn snapshot(self: *StateMachine) Snapshot {
        return .{
            .self = self,
            .accounts = self.accounts.snapshot(),
            .transfers = self.transfers.snapshot(),
            .commits = self.commits.snapshot(),
        };
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

test "linked accounts" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = &arena.allocator;

    const accounts_max = 5;
    const transfers_max = 0;
    const commits_max = 0;

    var state_machine = try StateMachine.init(allocator, accounts_max, transfers_max, commits_max);
    defer state_machine.deinit();

    var accounts = [_]Account{
        // An individual event (successful):
        std.mem.zeroInit(Account, .{ .id = 7, .code = 200 }),

        // A chain of 4 events (the last event in the chain closes the chain with linked=false):
        std.mem.zeroInit(Account, .{ .id = 0, .flags = .{ .linked = true } }), // Commit/rollback.
        std.mem.zeroInit(Account, .{ .id = 1, .flags = .{ .linked = true } }), // Commit/rollback.
        std.mem.zeroInit(Account, .{ .id = 0, .flags = .{ .linked = true } }), // Fail with .exists.
        std.mem.zeroInit(Account, .{ .id = 2 }), // Fail without committing.

        // An individual event (successful):
        // This should not see any effect from the failed chain above.
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

    const input = std.mem.asBytes(&accounts);
    const output = try allocator.alloc(u8, 4096);

    state_machine.prepare(0, .create_accounts, input);
    const size = state_machine.commit(.create_accounts, input, output);
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

    try testing.expectEqual(accounts[0], state_machine.accounts.get_const(accounts[0].id).?.*);
    try testing.expectEqual(accounts[5], state_machine.accounts.get_const(accounts[5].id).?.*);
    try testing.expectEqual(accounts[8], state_machine.accounts.get_const(accounts[8].id).?.*);
    try testing.expectEqual(accounts[11], state_machine.accounts.get_const(accounts[11].id).?.*);
    try testing.expectEqual(accounts[12], state_machine.accounts.get_const(accounts[12].id).?.*);
    try testing.expectEqual(@as(u64, 5), state_machine.accounts.count());

    // TODO How can we test that events were in fact rolled back in LIFO order?
    // All our rollback handlers appear to be commutative.
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
