const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.state_machine);

usingnamespace @import("tigerbeetle.zig");

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

        pub fn jsonStringify(self: Command, options: StringifyOptions, writer: anytype) !void {
            try std.fmt.format(writer, "\"{}\"", .{@tagName(self)});
        }
    };

    allocator: *std.mem.Allocator,
    prepare_timestamp: u64,
    commit_timestamp: u64,
    accounts: HashMapAccounts,
    transfers: HashMapTransfers,
    commits: HashMapCommits,

    pub fn init(
        allocator: *std.mem.Allocator,
        accounts_max: usize,
        transfers_max: usize,
        commits_max: usize,
    ) !StateMachine {
        var accounts = HashMapAccounts.init(allocator);
        errdefer accounts.deinit();
        try accounts.ensureCapacity(@intCast(u32, accounts_max));

        var transfers = HashMapTransfers.init(allocator);
        errdefer transfers.deinit();
        try transfers.ensureCapacity(@intCast(u32, transfers_max));

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
        self.commits.deinit();
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
        client: u128,
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
            if (self.get_account(id)) |result| {
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

test "create/lookup accounts [REWORK-NEW]" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = &arena.allocator;

    const AccTestVector = struct { outcome: CreateAccountResult, input: Account };

    const test_timestamp: u64 = 1;
    const acc_test_vector = [_]AccTestVector{
        AccTestVector{
            .outcome = CreateAccountResult.reserved_flag_padding,
            .input = std.mem.zeroInit(Account, .{
                .id = 1,
                .timestamp = test_timestamp,
                .flags = .{ .padding = 1 },
            }),
        },
        AccTestVector{
            .outcome = CreateAccountResult.reserved_field,
            .input = std.mem.zeroInit(Account, .{
                .id = 2,
                .timestamp = test_timestamp,
                .reserved = [_]u8{1} ** 48,
            }),
        },
        AccTestVector{
            .outcome = CreateAccountResult.exceeds_credits,
            .input = std.mem.zeroInit(Account, .{
                .id = 3,
                .timestamp = test_timestamp,
                .debits_reserved = 10,
                .flags = .{ .debits_must_not_exceed_credits = true },
            }),
        },
        AccTestVector{
            .outcome = CreateAccountResult.exceeds_credits,
            .input = std.mem.zeroInit(Account, .{
                .id = 4,
                .timestamp = test_timestamp,
                .debits_accepted = 10,
                .flags = .{ .debits_must_not_exceed_credits = true },
            }),
        },
        AccTestVector{
            .outcome = CreateAccountResult.exceeds_debits,
            .input = std.mem.zeroInit(Account, .{
                .id = 5,
                .timestamp = test_timestamp,
                .credits_reserved = 10,
                .flags = .{ .credits_must_not_exceed_debits = true },
            }),
        },
        AccTestVector{
            .outcome = CreateAccountResult.exceeds_debits,
            .input = std.mem.zeroInit(Account, .{
                .id = 6,
                .timestamp = test_timestamp,
                .credits_accepted = 10,
                .flags = .{ .credits_must_not_exceed_debits = true },
            }),
        },
        AccTestVector{
            .outcome = CreateAccountResult.ok,
            .input = std.mem.zeroInit(Account, .{
                .id = 7,
                .timestamp = test_timestamp,
            }),
        },
        AccTestVector{
            .outcome = CreateAccountResult.exists,
            .input = std.mem.zeroInit(Account, .{
                .id = 7,
                .timestamp = (test_timestamp + 1),
            }),
        },
        AccTestVector{
            .outcome = CreateAccountResult.ok,
            .input = std.mem.zeroInit(Account, .{
                .id = 8,
                .timestamp = (test_timestamp + 1),
                .unit = 9,
            }),
        },
        AccTestVector{
            .outcome = CreateAccountResult.exists_with_different_unit,
            .input = std.mem.zeroInit(Account, .{
                .id = 8,
                .timestamp = (test_timestamp + 2),
                .unit = 10,
            }),
        },
        AccTestVector{
            .outcome = CreateAccountResult.ok,
            .input = std.mem.zeroInit(Account, .{
                .id = 9,
                .timestamp = (test_timestamp + 2),
                .code = 9,
            }),
        },
        AccTestVector{
            .outcome = CreateAccountResult.exists_with_different_code,
            .input = std.mem.zeroInit(Account, .{
                .id = 9,
                .timestamp = (test_timestamp + 3),
                .code = 10,
            }),
        },
        AccTestVector{
            .outcome = CreateAccountResult.ok,
            .input = std.mem.zeroInit(Account, .{
                .id = 10,
                .timestamp = (test_timestamp + 3),
                .flags = .{ .credits_must_not_exceed_debits = true },
            }),
        },
        AccTestVector{
            .outcome = CreateAccountResult.exists_with_different_flags,
            .input = std.mem.zeroInit(Account, .{
                .id = 10,
                .timestamp = (test_timestamp + 4),
                .flags = .{ .debits_must_not_exceed_credits = true },
            }),
        },
        AccTestVector{
            .outcome = CreateAccountResult.ok,
            .input = std.mem.zeroInit(Account, .{
                .id = 11,
                .timestamp = (test_timestamp + 4),
                .user_data = 'U',
            }),
        },
        AccTestVector{
            .outcome = CreateAccountResult.exists_with_different_user_data,
            .input = std.mem.zeroInit(Account, .{
                .id = 11,
                .timestamp = (test_timestamp + 5),
                .user_data = 'D',
            }),
        },
    };

    //var account_list = try std.ArrayListUnmanaged(Account).initCapacity(allocator, acc_test_vector.len);
    //errdefer account_list.deinit(allocator);
    //var accounts = account_list.items;

    var state_machine = try StateMachine.init(allocator, acc_test_vector.len, 0, 0);
    defer state_machine.deinit();

    for (acc_test_vector) |itm, i| {
        const create_result = state_machine.create_account(itm.input);
        switch (itm.outcome) {
            .ok => try testing.expectEqual(itm.input, state_machine.get_account(itm.input.id).?.*),
            else => try testing.expectEqual(itm.outcome, create_result),
        }
    }

    //.ok => std.debug.print("Out-> SUCCESS [{d} - {any}] \n", .{ i, result.outcome }), //TODO try testing.expectEqual(result, state_machine.get_account(accounts[i].id).?.*);
    //else => std.debug.print("Out-> [{d} - {any}] \n", .{ i, result.outcome }), //TODO try testing.expectEqual(result, results[i]);
}

test "create/lookup accounts" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = &arena.allocator;

    var accounts = [_]Account{
        std.mem.zeroInit(Account, .{ .id = 1, .flags = .{ .padding = 1 } }), //.reserved_flag_padding
        std.mem.zeroInit(Account, .{ .id = 2, .reserved = [_]u8{1} ** 48 }), //.reserved_field
        std.mem.zeroInit(Account, .{
            .id = 3,
            .debits_reserved = 10,
            .flags = .{ .debits_must_not_exceed_credits = true },
        }), //.debits_must_not_exceed_credits
        std.mem.zeroInit(Account, .{
            .id = 4,
            .debits_accepted = 10,
            .flags = .{ .debits_must_not_exceed_credits = true },
        }), //.debits_must_not_exceed_credits
        std.mem.zeroInit(Account, .{
            .id = 5,
            .credits_reserved = 10,
            .flags = .{ .credits_must_not_exceed_debits = true },
        }), //.credits_must_not_exceed_debits
        std.mem.zeroInit(Account, .{
            .id = 6,
            .credits_accepted = 10,
            .flags = .{ .credits_must_not_exceed_debits = true },
        }), //.credits_must_not_exceed_debits
        std.mem.zeroInit(Account, .{ .id = 7 }),
        std.mem.zeroInit(Account, .{ .id = 7 }), //.exists
        std.mem.zeroInit(Account, .{ .id = 8, .unit = 9 }),
        std.mem.zeroInit(Account, .{ .id = 8, .unit = 10 }), //.exists_with_different_unit
        std.mem.zeroInit(Account, .{ .id = 9, .code = 9 }),
        std.mem.zeroInit(Account, .{ .id = 9, .code = 10 }), //.exists_with_different_code
        std.mem.zeroInit(Account, .{
            .id = 10,
            .flags = .{ .credits_must_not_exceed_debits = true },
        }),
        std.mem.zeroInit(Account, .{
            .id = 10,
            .flags = .{ .debits_must_not_exceed_credits = true },
        }), //.exists_with_different_flags
        std.mem.zeroInit(Account, .{ .id = 11, .user_data = 'U' }),
        std.mem.zeroInit(Account, .{ .id = 11, .user_data = 'D' }), //.exists_with_different_user_data
        //.exists_with_different_reserved_field - We do not currently have any reserved fields.
    };

    var state_machine = try StateMachine.init(allocator, accounts.len, 0, 0);
    defer state_machine.deinit();

    const input = std.mem.asBytes(&accounts);
    const output = try allocator.alloc(u8, 4096);

    // Use a timestamp of 0 since this is just a test
    state_machine.prepare(0, .create_accounts, input);
    const size = state_machine.commit(0, .create_accounts, input, output);
    const results = std.mem.bytesAsSlice(CreateAccountsResult, output[0..size]);

    try testing.expectEqualSlices(
        CreateAccountsResult,
        &[_]CreateAccountsResult{
            CreateAccountsResult{ .index = 0, .result = .reserved_flag_padding },
            CreateAccountsResult{ .index = 1, .result = .reserved_field },
            CreateAccountsResult{ .index = 2, .result = .exceeds_credits },
            CreateAccountsResult{ .index = 3, .result = .exceeds_credits },
            CreateAccountsResult{ .index = 4, .result = .exceeds_debits },
            CreateAccountsResult{ .index = 5, .result = .exceeds_debits },
            //CreateAccountsResult{ .index = 6, .result = .ok },
            CreateAccountsResult{ .index = 7, .result = .exists },
            //CreateAccountsResult{ .index = 8, .result = .ok },
            CreateAccountsResult{ .index = 9, .result = .exists_with_different_unit },
            //CreateAccountsResult{ .index = 10, .result = .ok },
            CreateAccountsResult{ .index = 11, .result = .exists_with_different_code },
            //CreateAccountsResult{ .index = 12, .result = .ok },
            CreateAccountsResult{ .index = 13, .result = .exists_with_different_flags },
            //CreateAccountsResult{ .index = 14, .result = .ok },
            CreateAccountsResult{ .index = 15, .result = .exists_with_different_user_data },
        },
        results,
    );

    try testing.expectEqual(accounts[6], state_machine.get_account(accounts[6].id).?.*);
    try testing.expectEqual(accounts[8], state_machine.get_account(accounts[8].id).?.*);
    try testing.expectEqual(accounts[10], state_machine.get_account(accounts[10].id).?.*);
    try testing.expectEqual(accounts[12], state_machine.get_account(accounts[12].id).?.*);
    try testing.expectEqual(accounts[14], state_machine.get_account(accounts[14].id).?.*);
}

test "linked accounts" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = &arena.allocator;

    const transfers_max = 0;
    const commits_max = 0;

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

    var state_machine = try StateMachine.init(allocator, accounts.len, transfers_max, commits_max);
    defer state_machine.deinit();

    const input = std.mem.asBytes(&accounts);
    const output = try allocator.alloc(u8, 4096);

    // Use a timestamp of 0 since this is just a test
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
    const allocator = &arena.allocator;

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

    var transfers = [_]Transfer{
        std.mem.zeroInit(Transfer, .{ .id = 1 }), //.amount_is_zero
        std.mem.zeroInit(Transfer, .{ .id = 2, .flags = .{ .padding = 1 } }), //.reserved_flag_padding
        std.mem.zeroInit(Transfer, .{ .id = 3, .flags = .{ .two_phase_commit = true } }), //.reserved_flag_padding
        std.mem.zeroInit(Transfer, .{ .id = 4, .timeout = 1 }), //.timeout_reserved_for_two_phase_commit
        std.mem.zeroInit(Transfer, .{
            .id = 5,
            .flags = .{ .condition = false },
            .reserved = [_]u8{1} ** 32,
        }), //.reserved_field
        std.mem.zeroInit(Transfer, .{
            .id = 6,
            .amount = 10,
            .debit_account_id = 1,
            .credit_account_id = 1,
        }), //.accounts_are_the_same
        std.mem.zeroInit(Transfer, .{
            .id = 7,
            .amount = 10,
            .debit_account_id = 100,
            .credit_account_id = 1,
        }), //.debit_account_not_found
        std.mem.zeroInit(Transfer, .{
            .id = 8,
            .amount = 10,
            .debit_account_id = 1,
            .credit_account_id = 100,
        }), //.credit_account_not_found
        std.mem.zeroInit(Transfer, .{
            .id = 9,
            .amount = 10,
            .debit_account_id = 3,
            .credit_account_id = 4,
        }), //.accounts_have_different_units
        std.mem.zeroInit(Transfer, .{
            .id = 10,
            .amount = 1000,
            .debit_account_id = 5,
            .credit_account_id = 1,
        }), //.exceeds_credits
        std.mem.zeroInit(Transfer, .{
            .id = 11,
            .amount = 1000,
            .debit_account_id = 1,
            .credit_account_id = 6,
        }), //.exceeds_debits
        std.mem.zeroInit(
            Transfer,
            .{ .id = 12, .amount = 10, .debit_account_id = 7, .credit_account_id = 8 },
        ), //.ok
        std.mem.zeroInit(
            Transfer,
            .{ .id = 12, .amount = 10, .debit_account_id = 7, .credit_account_id = 8 },
        ), //.exists
        std.mem.zeroInit(
            Transfer,
            .{ .id = 12, .amount = 10, .debit_account_id = 8, .credit_account_id = 7 },
        ), //.exists_with_different_debit_account_id
        std.mem.zeroInit(
            Transfer,
            .{ .id = 12, .amount = 10, .debit_account_id = 7, .credit_account_id = 1 },
        ), //.exists_with_different_credit_account_id
        std.mem.zeroInit(
            Transfer,
            .{ .id = 12, .amount = 11, .debit_account_id = 7, .credit_account_id = 8 },
        ), //.exists_with_different_amount
        std.mem.zeroInit(
            Transfer,
            .{
                .id = 12,
                .amount = 10,
                .debit_account_id = 7,
                .credit_account_id = 8,
                .flags = .{ .condition = true },
            },
        ), //.exists_with_different_flags
        std.mem.zeroInit(
            Transfer,
            .{
                .id = 12,
                .amount = 10,
                .debit_account_id = 7,
                .credit_account_id = 8,
                .user_data = 'A',
            },
        ), //.exists_with_different_user_data
        //.exists_with_different_reserved_field - We do not currently have any reserved fields.
        std.mem.zeroInit(
            Transfer,
            .{
                .id = 13,
                .amount = 10,
                .debit_account_id = 7,
                .credit_account_id = 8,
                .flags = .{ .condition = true },
                .reserved = [_]u8{1} ** 32,
            },
        ), //.ok
        std.mem.zeroInit(
            Transfer,
            .{
                .id = 13,
                .amount = 10,
                .debit_account_id = 7,
                .credit_account_id = 8,
                .flags = .{ .condition = true },
                .reserved = [_]u8{2} ** 32,
            },
        ), //.exists_with_different_reserved_field
        std.mem.zeroInit(
            Transfer,
            .{
                .id = 13,
                .amount = 10,
                .debit_account_id = 7,
                .credit_account_id = 8,
                .flags = .{ .condition = true },
                .reserved = [_]u8{1} ** 32,
                .timeout = 10,
            },
        ), //.timeout_reserved_for_two_phase_commit
        std.mem.zeroInit(
            Transfer,
            .{
                .id = 14,
                .amount = 10,
                .debit_account_id = 7,
                .credit_account_id = 8,
                .flags = .{ .two_phase_commit = true },
                .timeout = 0,
            },
        ), //.two_phase_commit_must_timeout
        std.mem.zeroInit(
            Transfer,
            .{
                .id = 15,
                .amount = 10,
                .debit_account_id = 7,
                .credit_account_id = 8,
                .flags = .{ .two_phase_commit = true },
                .timeout = 20,
            },
        ), //.ok
        std.mem.zeroInit(
            Transfer,
            .{
                .id = 15,
                .amount = 10,
                .debit_account_id = 7,
                .credit_account_id = 8,
                .flags = .{ .two_phase_commit = true },
                .timeout = 25,
            },
        ), //.exists_with_different_timeout
    };

    var state_machine = try StateMachine.init(allocator, accounts.len, 1, 0);
    defer state_machine.deinit();

    const input = std.mem.asBytes(&accounts);
    const output = try allocator.alloc(u8, 4096);

    // Use a timestamp of 0 since this is just a test
    comptime const timestamp: u8 = 0;
    state_machine.prepare(timestamp, .create_accounts, input);
    const size = state_machine.commit(timestamp, .create_accounts, input, output);
    const results = std.mem.bytesAsSlice(CreateAccountsResult, output[0..size]);

    //Ensure the accounts were created successfully...
    try testing.expectEqual(accounts[0], state_machine.get_account(accounts[0].id).?.*);
    try testing.expectEqual(accounts[1], state_machine.get_account(accounts[1].id).?.*);
    try testing.expectEqual(accounts[2], state_machine.get_account(accounts[2].id).?.*);
    try testing.expectEqual(accounts[3], state_machine.get_account(accounts[3].id).?.*);
    try testing.expectEqual(accounts[4], state_machine.get_account(accounts[4].id).?.*);
    try testing.expectEqual(accounts[5], state_machine.get_account(accounts[5].id).?.*);
    try testing.expectEqual(accounts[6], state_machine.get_account(accounts[6].id).?.*);
    try testing.expectEqual(accounts[7], state_machine.get_account(accounts[7].id).?.*);

    const input_transfers = std.mem.asBytes(&transfers);
    const output_transfers = try allocator.alloc(u8, 4096);

    state_machine.prepare(timestamp, .create_transfers, input_transfers);
    const size_transfers = state_machine.commit(timestamp, .create_transfers, input_transfers, output_transfers);
    const results_transfers = std.mem.bytesAsSlice(CreateTransfersResult, output_transfers[0..size_transfers]);

    //Ensure the transfers were created successfully...
    try testing.expectEqual(transfers[11], state_machine.get_transfer(transfers[11].id).?.*); //id=12
    try testing.expectEqual(transfers[18], state_machine.get_transfer(transfers[18].id).?.*); //id=13
    try testing.expectEqual(transfers[22], state_machine.get_transfer(transfers[22].id).?.*); //id=15

    //2 phase commit [reserved]
    try testing.expectEqual(@as(u64, 10), state_machine.get_account(7).?.*.debits_reserved);
    try testing.expectEqual(@as(u64, 0), state_machine.get_account(7).?.*.credits_reserved);
    try testing.expectEqual(@as(u64, 10), state_machine.get_account(8).?.*.credits_reserved);
    try testing.expectEqual(@as(u64, 0), state_machine.get_account(8).?.*.debits_reserved);
    //1 phase commit [accepted]
    try testing.expectEqual(@as(u64, 20), state_machine.get_account(7).?.*.debits_accepted);
    try testing.expectEqual(@as(u64, 0), state_machine.get_account(7).?.*.credits_accepted);
    try testing.expectEqual(@as(u64, 20), state_machine.get_account(8).?.*.credits_accepted);
    try testing.expectEqual(@as(u64, 0), state_machine.get_account(8).?.*.debits_accepted);

    //rollback transfer with id [12], amount of 10...
    state_machine.create_transfer_rollback(state_machine.get_transfer(transfers[11].id).?.*);
    try testing.expectEqual(@as(u64, 10), state_machine.get_account(7).?.*.debits_accepted);
    try testing.expectEqual(@as(u64, 0), state_machine.get_account(7).?.*.credits_accepted);
    try testing.expectEqual(@as(u64, 10), state_machine.get_account(8).?.*.credits_accepted);
    try testing.expectEqual(@as(u64, 0), state_machine.get_account(8).?.*.debits_accepted);
    try testing.expect(state_machine.get_transfer(transfers[11].id) == null);

    //rollback transfer with id [15], amount of 10...
    state_machine.create_transfer_rollback(state_machine.get_transfer(transfers[22].id).?.*);
    try testing.expectEqual(@as(u64, 0), state_machine.get_account(7).?.*.debits_reserved);
    try testing.expectEqual(@as(u64, 0), state_machine.get_account(8).?.*.credits_reserved);
    try testing.expect(state_machine.get_transfer(transfers[22].id) == null);

    try testing.expectEqualSlices(
        CreateTransfersResult,
        &[_]CreateTransfersResult{
            CreateTransfersResult{ .index = 0, .result = .amount_is_zero },
            CreateTransfersResult{ .index = 1, .result = .reserved_flag_padding },
            CreateTransfersResult{ .index = 2, .result = .two_phase_commit_must_timeout },
            CreateTransfersResult{ .index = 3, .result = .timeout_reserved_for_two_phase_commit },
            CreateTransfersResult{ .index = 4, .result = .reserved_field },
            CreateTransfersResult{ .index = 5, .result = .accounts_are_the_same },
            CreateTransfersResult{ .index = 6, .result = .debit_account_not_found },
            CreateTransfersResult{ .index = 7, .result = .credit_account_not_found },
            CreateTransfersResult{ .index = 8, .result = .accounts_have_different_units },
            CreateTransfersResult{ .index = 9, .result = .exceeds_credits },
            CreateTransfersResult{ .index = 10, .result = .exceeds_debits },
            //CreateTransfersResult{ .index = 11, .result = .ok },
            CreateTransfersResult{ .index = 12, .result = .exists },
            CreateTransfersResult{ .index = 13, .result = .exists_with_different_debit_account_id },
            CreateTransfersResult{ .index = 14, .result = .exists_with_different_credit_account_id },
            CreateTransfersResult{ .index = 15, .result = .exists_with_different_amount },
            CreateTransfersResult{ .index = 16, .result = .exists_with_different_flags },
            CreateTransfersResult{ .index = 17, .result = .exists_with_different_user_data },
            //CreateTransfersResult{ .index = 18, .result = .ok },
            CreateTransfersResult{ .index = 19, .result = .exists_with_different_reserved_field },
            CreateTransfersResult{ .index = 20, .result = .timeout_reserved_for_two_phase_commit },
            CreateTransfersResult{ .index = 21, .result = .two_phase_commit_must_timeout },
            //CreateTransfersResult{ .index = 22, .result = .ok },
            CreateTransfersResult{ .index = 23, .result = .exists_with_different_timeout },
        },
        results_transfers,
    );
}

test "create/lookup/rollback commits" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = &arena.allocator;

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
        }), //.transfer_not_two_phase_commit
        std.mem.zeroInit(Transfer, .{
            .id = 2,
            .amount = 15,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .flags = .{ .two_phase_commit = true },
            .timeout = 25,
        }), //.ok
        std.mem.zeroInit(Transfer, .{
            .id = 3,
            .amount = 15,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .flags = .{ .two_phase_commit = true },
            .timeout = 25,
        }), //.ok
        std.mem.zeroInit(Transfer, .{
            .id = 4,
            .amount = 15,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .flags = .{ .two_phase_commit = true },
            .timeout = 1,
        }), //.ok
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
        }), //.condition_requires_preimage
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
        }), //.condition_requires_preimage
        std.mem.zeroInit(Transfer, .{
            .id = 7,
            .amount = 15,
            .debit_account_id = 3,
            .credit_account_id = 4,
            .flags = .{ .two_phase_commit = true },
            .timeout = 25,
        }), //.credit_account_not_found / .debit_account_not_found
    };

    var commits = [_]Commit{
        std.mem.zeroInit(Commit, .{ .id = 1, .reserved = [_]u8{1} ** 32 }), //.reserved_field
        std.mem.zeroInit(Commit, .{ .id = 1, .flags = .{ .padding = 1 } }), //.reserved_flag_padding
        std.mem.zeroInit(Commit, .{ .id = 777 }), //.transfer_not_found
        std.mem.zeroInit(Commit, .{ .id = 1 }), //.transfer_not_two_phase_commit
        std.mem.zeroInit(Commit, .{ .id = 2 }), //.ok
        std.mem.zeroInit(Commit, .{ .id = 2, .flags = .{ .reject = true } }), //.already_committed_but_accepted
        std.mem.zeroInit(Commit, .{ .id = 2 }), //.already_committed
        std.mem.zeroInit(Commit, .{ .id = 3, .flags = .{ .reject = true } }), //.ok
        std.mem.zeroInit(Commit, .{ .id = 3 }), //.already_committed_but_rejected
        std.mem.zeroInit(Commit, .{ .id = 4 }), //.transfer_expired
        std.mem.zeroInit(Commit, .{ .id = 5 }), //.condition_requires_preimage
        std.mem.zeroInit(Commit, .{ .id = 5, .reserved = [_]u8{1} ** 32, .flags = .{
            .preimage = true,
        } }), //.preimage_invalid
        std.mem.zeroInit(Commit, .{ .id = 6, .flags = .{
            .preimage = true,
        } }), //.preimage_requires_condition
        std.mem.zeroInit(Commit, .{ .id = 7 }), //.ok
    };

    var state_machine = try StateMachine.init(allocator, accounts.len, 10, 10);
    defer state_machine.deinit();

    const input_accounts = std.mem.asBytes(&accounts);
    const output_accounts = try allocator.alloc(u8, 4096);

    // Use a timestamp of 0 since this is just a test
    comptime const timestamp: u8 = 0;

    // ACCOUNTS
    state_machine.prepare(timestamp, .create_accounts, input_accounts);
    const size = state_machine.commit(timestamp, .create_accounts, input_accounts, output_accounts);
    const results = std.mem.bytesAsSlice(CreateAccountsResult, output_accounts[0..size]);

    //Ensure the accounts were created successfully...
    try testing.expectEqual(accounts[0], state_machine.get_account(accounts[0].id).?.*); //id=1
    try testing.expectEqual(accounts[1], state_machine.get_account(accounts[1].id).?.*); //id=2

    // TRANSFERS
    const input_transfers = std.mem.asBytes(&transfers);
    const output_transfers = try allocator.alloc(u8, 4096);

    state_machine.prepare(timestamp, .create_transfers, input_transfers);
    const size_transfers = state_machine.commit(timestamp, .create_transfers, input_transfers, output_transfers);
    const results_transfers = std.mem.bytesAsSlice(CreateTransfersResult, output_transfers[0..size_transfers]);

    //Ensure the transfers were created successfully...
    try testing.expectEqual(transfers[0], state_machine.get_transfer(transfers[0].id).?.*); //id=1
    try testing.expectEqual(transfers[1], state_machine.get_transfer(transfers[1].id).?.*); //id=2
    try testing.expectEqual(transfers[2], state_machine.get_transfer(transfers[2].id).?.*); //id=3
    try testing.expectEqual(transfers[6], state_machine.get_transfer(transfers[6].id).?.*); //id=3

    // COMMITS
    const input_commits = std.mem.asBytes(&commits);
    const output_commits = try allocator.alloc(u8, 4096);

    state_machine.prepare(timestamp, .commit_transfers, input_commits);

    try testing.expectEqual(state_machine.commit_transfer(commits[0]), .reserved_field);
    try testing.expectEqual(state_machine.commit_transfer(commits[1]), .reserved_flag_padding);
    try testing.expectEqual(state_machine.commit_transfer(commits[2]), .transfer_not_found);
    try testing.expectEqual(state_machine.commit_transfer(commits[3]), .transfer_not_two_phase_commit);

    // Test Balance BEFORE commit
    // Account 1
    const acc_1_before = state_machine.get_account(1).?.*;
    try testing.expectEqual(@as(u64, 15), acc_1_before.debits_accepted);
    try testing.expectEqual(@as(u64, 75), acc_1_before.debits_reserved);
    try testing.expectEqual(@as(u64, 0), acc_1_before.credits_accepted);
    try testing.expectEqual(@as(u64, 0), acc_1_before.credits_reserved);
    // Account 2
    const acc_2_before = state_machine.get_account(2).?.*;
    try testing.expectEqual(@as(u64, 0), acc_2_before.debits_accepted);
    try testing.expectEqual(@as(u64, 0), acc_2_before.debits_reserved);
    try testing.expectEqual(@as(u64, 15), acc_2_before.credits_accepted);
    try testing.expectEqual(@as(u64, 75), acc_2_before.credits_reserved);

    try testing.expectEqual(state_machine.commit_transfer(commits[4]), .ok); //commit [id=2]

    // Test Balance AFTER commit
    // Account 1
    const acc_1_after = state_machine.get_account(1).?.*;
    try testing.expectEqual(@as(u64, 30), acc_1_after.debits_accepted); //+15 (acceptance applied)
    try testing.expectEqual(@as(u64, 60), acc_1_after.debits_reserved); //-15 (reserved moved)
    try testing.expectEqual(@as(u64, 0), acc_1_after.credits_accepted);
    try testing.expectEqual(@as(u64, 0), acc_1_after.credits_reserved);
    // Account 2
    const acc_2_after = state_machine.get_account(2).?.*;
    try testing.expectEqual(@as(u64, 0), acc_2_after.debits_accepted);
    try testing.expectEqual(@as(u64, 0), acc_2_after.debits_reserved);
    try testing.expectEqual(@as(u64, 30), acc_2_after.credits_accepted); //+15 (acceptance applied)
    try testing.expectEqual(@as(u64, 60), acc_2_after.credits_reserved); //-15 (reserved moved)

    try testing.expectEqual(state_machine.commit_transfer(commits[5]), .already_committed_but_accepted);
    try testing.expectEqual(state_machine.commit_transfer(commits[6]), .already_committed);
    try testing.expectEqual(state_machine.commit_transfer(commits[7]), .ok);
    try testing.expectEqual(state_machine.commit_transfer(commits[8]), .already_committed_but_rejected);
    try testing.expectEqual(state_machine.commit_transfer(commits[9]), .transfer_expired);
    try testing.expectEqual(state_machine.commit_transfer(commits[10]), .condition_requires_preimage);
    try testing.expectEqual(state_machine.commit_transfer(commits[11]), .preimage_invalid);
    try testing.expectEqual(state_machine.commit_transfer(commits[12]), .preimage_requires_condition);

    // Test COMMIT with invalid debit/credit accounts
    state_machine.create_account_rollback(accounts[3]);
    try testing.expect(state_machine.get_account(accounts[3].id) == null);
    try testing.expectEqual(state_machine.commit_transfer(commits[13]), .credit_account_not_found);
    state_machine.create_account_rollback(accounts[2]);
    try testing.expect(state_machine.get_account(accounts[2].id) == null);
    try testing.expectEqual(state_machine.commit_transfer(commits[13]), .debit_account_not_found);

    state_machine.commit_transfer_rollback(commits[4]); //rollback [id=2] not rejected

    // Account 1
    const acc_1_rollback = state_machine.get_account(1).?.*;
    try testing.expectEqual(@as(u64, 15), acc_1_rollback.debits_accepted); //-15 (rollback)
    try testing.expectEqual(@as(u64, 60), acc_1_rollback.debits_reserved);
    try testing.expectEqual(@as(u64, 0), acc_1_rollback.credits_accepted);
    try testing.expectEqual(@as(u64, 0), acc_1_rollback.credits_reserved);
    // Account 2
    const acc_2_rollback = state_machine.get_account(2).?.*;
    try testing.expectEqual(@as(u64, 0), acc_2_rollback.debits_accepted);
    try testing.expectEqual(@as(u64, 0), acc_2_rollback.debits_reserved);
    try testing.expectEqual(@as(u64, 15), acc_2_rollback.credits_accepted); //-15 (rollback)
    try testing.expectEqual(@as(u64, 60), acc_2_rollback.credits_reserved);

    state_machine.commit_transfer_rollback(commits[7]); //rollback [id=3] rejected
    // Account 1
    const acc_1_rollback_reject = state_machine.get_account(1).?.*;
    try testing.expectEqual(@as(u64, 15), acc_1_rollback_reject.debits_accepted); //remains unchanged
    try testing.expectEqual(@as(u64, 75), acc_1_rollback_reject.debits_reserved); //+15 rolled back
    try testing.expectEqual(@as(u64, 0), acc_1_rollback_reject.credits_accepted);
    try testing.expectEqual(@as(u64, 0), acc_1_rollback_reject.credits_reserved);
    // Account 2
    const acc_2_rollback_reject = state_machine.get_account(2).?.*;
    try testing.expectEqual(@as(u64, 0), acc_2_rollback_reject.debits_accepted);
    try testing.expectEqual(@as(u64, 0), acc_2_rollback_reject.debits_reserved);
    try testing.expectEqual(@as(u64, 15), acc_2_rollback_reject.credits_accepted);
    try testing.expectEqual(@as(u64, 75), acc_2_rollback_reject.credits_reserved); //+15 rolled back
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
