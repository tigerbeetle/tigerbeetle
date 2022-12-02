const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const log = std.log.scoped(.state_machine);
const tracer = @import("tracer.zig");

const tb = @import("tigerbeetle.zig");
const snapshot_latest = @import("lsm/tree.zig").snapshot_latest;

const Account = tb.Account;
const AccountFlags = tb.AccountFlags;

const Transfer = tb.Transfer;
const TransferFlags = tb.TransferFlags;

const CreateAccountsResult = tb.CreateAccountsResult;
const CreateTransfersResult = tb.CreateTransfersResult;

const CreateAccountResult = tb.CreateAccountResult;
const CreateTransferResult = tb.CreateTransferResult;

pub fn StateMachineType(comptime Storage: type, comptime constants_: struct {
    message_body_size_max: usize,
}) type {
    return struct {
        const StateMachine = @This();

        const Grid = @import("lsm/grid.zig").GridType(Storage);
        const GrooveType = @import("lsm/groove.zig").GrooveType;
        const ForestType = @import("lsm/forest.zig").ForestType;

        const AccountsGroove = GrooveType(
            Storage,
            Account,
            .{
                .ignored = &[_][]const u8{ "reserved", "flags" },
                .derived = .{},
            },
        );
        const TransfersGroove = GrooveType(
            Storage,
            Transfer,
            .{
                .ignored = &[_][]const u8{ "reserved", "flags" },
                .derived = .{},
            },
        );
        const PostedGroove = @import("lsm/posted_groove.zig").PostedGrooveType(Storage);

        pub const Forest = ForestType(Storage, .{
            .accounts = AccountsGroove,
            .transfers = TransfersGroove,
            .posted = PostedGroove,
        });

        pub const Operation = enum(u8) {
            /// Operations reserved by VR protocol (for all state machines):
            reserved,
            root,
            register,

            /// Operations exported by TigerBeetle:
            create_accounts,
            create_transfers,
            lookup_accounts,
            lookup_transfers,
        };

        pub const constants = struct {
            /// The maximum number of objects within a batch, by operation.
            pub const batch_max = struct {
                pub const create_accounts = operation_batch_max(.create_accounts);
                pub const create_transfers = operation_batch_max(.create_transfers);
                pub const lookup_accounts = operation_batch_max(.lookup_accounts);
                pub const lookup_transfers = operation_batch_max(.lookup_transfers);

                comptime {
                    assert(create_accounts > 0);
                    assert(create_transfers > 0);
                    assert(lookup_accounts > 0);
                    assert(lookup_transfers > 0);
                }

                fn operation_batch_max(comptime operation: Operation) usize {
                    return @divFloor(constants_.message_body_size_max, std.math.max(
                        @sizeOf(Event(operation)),
                        @sizeOf(Result(operation)),
                    ));
                }
            };
        };

        pub const Options = struct {
            lsm_forest_node_count: u32,
            cache_entries_accounts: u32,
            cache_entries_transfers: u32,
            cache_entries_posted: u32,
        };

        prepare_timestamp: u64,
        commit_timestamp: u64,
        forest: Forest,

        prefetch_input: ?[]align(16) const u8 = null,
        prefetch_callback: ?fn (*StateMachine) void = null,
        // TODO(ifreund): use a union for these to save memory, likely an extern union
        // so that we can safetly @ptrCast() until @fieldParentPtr() is implemented
        // for unions. See: https://github.com/ziglang/zig/issues/6611
        prefetch_accounts_context: AccountsGroove.PrefetchContext = undefined,
        prefetch_transfers_context: TransfersGroove.PrefetchContext = undefined,
        prefetch_posted_context: PostedGroove.PrefetchContext = undefined,

        open_callback: ?fn (*StateMachine) void = null,
        compact_callback: ?fn (*StateMachine) void = null,
        checkpoint_callback: ?fn (*StateMachine) void = null,

        tracer_slot: ?tracer.SpanStart,

        pub fn init(allocator: mem.Allocator, grid: *Grid, options: Options) !StateMachine {
            var forest = try Forest.init(
                allocator,
                grid,
                options.lsm_forest_node_count,
                forest_options(options),
            );
            errdefer forest.deinit(allocator);

            return StateMachine{
                .prepare_timestamp = 0,
                .commit_timestamp = 0,
                .forest = forest,
                .tracer_slot = null,
            };
        }

        pub fn deinit(self: *StateMachine, allocator: mem.Allocator) void {
            assert(self.tracer_slot == null);

            self.forest.deinit(allocator);
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

        pub fn open(self: *StateMachine, callback: fn (*StateMachine) void) void {
            assert(self.open_callback == null);
            self.open_callback = callback;

            self.forest.open(forest_open_callback);
        }

        fn forest_open_callback(forest: *Forest) void {
            const self = @fieldParentPtr(StateMachine, "forest", forest);
            assert(self.open_callback != null);

            const callback = self.open_callback.?;
            self.open_callback = null;
            callback(self);
        }

        /// Returns the header's timestamp.
        pub fn prepare(self: *StateMachine, operation: Operation, input: []u8) u64 {
            switch (operation) {
                .root => unreachable,
                .register => {},
                .create_accounts => self.prepare_timestamps(.create_accounts, input),
                .create_transfers => self.prepare_timestamps(.create_transfers, input),
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

        pub fn prefetch(
            self: *StateMachine,
            callback: fn (*StateMachine) void,
            op: u64,
            operation: Operation,
            input: []align(16) const u8,
        ) void {
            _ = op;
            assert(self.prefetch_input == null);
            assert(self.prefetch_callback == null);

            if (operation == .register) {
                callback(self);
                return;
            }

            tracer.start(
                &self.tracer_slot,
                .main,
                .state_machine_prefetch,

                @src(),
            );

            self.prefetch_input = input;
            self.prefetch_callback = callback;

            // TODO(Snapshots) Pass in the target snapshot.
            self.forest.grooves.accounts.prefetch_setup(null);
            self.forest.grooves.transfers.prefetch_setup(null);
            self.forest.grooves.posted.prefetch_setup(null);

            return switch (operation) {
                .reserved, .root, .register => unreachable,
                .create_accounts => {
                    self.prefetch_create_accounts(mem.bytesAsSlice(Account, input));
                },
                .create_transfers => {
                    self.prefetch_create_transfers(mem.bytesAsSlice(Transfer, input));
                },
                .lookup_accounts => {
                    self.prefetch_lookup_accounts(mem.bytesAsSlice(u128, input));
                },
                .lookup_transfers => {
                    self.prefetch_lookup_transfers(mem.bytesAsSlice(u128, input));
                },
            };
        }

        fn prefetch_finish(self: *StateMachine) void {
            assert(self.prefetch_input != null);
            const callback = self.prefetch_callback.?;
            self.prefetch_input = null;
            self.prefetch_callback = null;

            tracer.end(
                &self.tracer_slot,
                .main,
                .state_machine_prefetch,
            );

            callback(self);
        }

        fn prefetch_create_accounts(self: *StateMachine, accounts: []const Account) void {
            for (accounts) |*a| {
                self.forest.grooves.accounts.prefetch_enqueue(a.id);
            }
            self.forest.grooves.accounts.prefetch(
                prefetch_create_accounts_callback,
                &self.prefetch_accounts_context,
            );
        }

        fn prefetch_create_accounts_callback(completion: *AccountsGroove.PrefetchContext) void {
            const self = @fieldParentPtr(StateMachine, "prefetch_accounts_context", completion);

            self.prefetch_finish();
        }

        fn prefetch_create_transfers(self: *StateMachine, transfers: []const Transfer) void {
            for (transfers) |*t| {
                self.forest.grooves.transfers.prefetch_enqueue(t.id);

                if (t.flags.post_pending_transfer or t.flags.void_pending_transfer) {
                    self.forest.grooves.transfers.prefetch_enqueue(t.pending_id);
                    // This prefetch isn't run yet, but enqueue it here as well to save an extra
                    // iteration over transfers.
                    self.forest.grooves.posted.prefetch_enqueue(t.pending_id);
                }
            }

            self.forest.grooves.transfers.prefetch(
                prefetch_create_transfers_callback_transfers,
                &self.prefetch_transfers_context,
            );
        }

        fn prefetch_create_transfers_callback_transfers(completion: *TransfersGroove.PrefetchContext) void {
            const self = @fieldParentPtr(StateMachine, "prefetch_transfers_context", completion);

            const transfers = mem.bytesAsSlice(Event(.create_transfers), self.prefetch_input.?);
            for (transfers) |*t| {
                if (t.flags.post_pending_transfer or t.flags.void_pending_transfer) {
                    if (self.forest.grooves.transfers.get(t.pending_id)) |p| {
                        self.forest.grooves.accounts.prefetch_enqueue(p.debit_account_id);
                        self.forest.grooves.accounts.prefetch_enqueue(p.credit_account_id);
                    }
                } else {
                    self.forest.grooves.accounts.prefetch_enqueue(t.debit_account_id);
                    self.forest.grooves.accounts.prefetch_enqueue(t.credit_account_id);
                }
            }

            self.forest.grooves.accounts.prefetch(
                prefetch_create_transfers_callback_accounts,
                &self.prefetch_accounts_context,
            );
        }

        fn prefetch_create_transfers_callback_accounts(completion: *AccountsGroove.PrefetchContext) void {
            const self = @fieldParentPtr(StateMachine, "prefetch_accounts_context", completion);

            self.forest.grooves.posted.prefetch(
                prefetch_create_transfers_callback_posted,
                &self.prefetch_posted_context,
            );
        }

        fn prefetch_create_transfers_callback_posted(completion: *PostedGroove.PrefetchContext) void {
            const self = @fieldParentPtr(StateMachine, "prefetch_posted_context", completion);

            self.prefetch_finish();
        }

        fn prefetch_lookup_accounts(self: *StateMachine, ids: []const u128) void {
            for (ids) |id| {
                self.forest.grooves.accounts.prefetch_enqueue(id);
            }

            self.forest.grooves.accounts.prefetch(
                prefetch_lookup_accounts_callback,
                &self.prefetch_accounts_context,
            );
        }

        fn prefetch_lookup_accounts_callback(completion: *AccountsGroove.PrefetchContext) void {
            const self = @fieldParentPtr(StateMachine, "prefetch_accounts_context", completion);

            self.prefetch_finish();
        }

        fn prefetch_lookup_transfers(self: *StateMachine, ids: []const u128) void {
            for (ids) |id| {
                self.forest.grooves.transfers.prefetch_enqueue(id);
            }

            self.forest.grooves.transfers.prefetch(
                prefetch_lookup_transfers_callback,
                &self.prefetch_transfers_context,
            );
        }

        fn prefetch_lookup_transfers_callback(completion: *TransfersGroove.PrefetchContext) void {
            const self = @fieldParentPtr(StateMachine, "prefetch_transfers_context", completion);

            self.prefetch_finish();
        }

        pub fn commit(
            self: *StateMachine,
            client: u128,
            op: u64,
            operation: Operation,
            input: []align(16) const u8,
            output: []align(16) u8,
        ) usize {
            _ = client;
            assert(op != 0);

            tracer.start(
                &self.tracer_slot,
                .main,
                .state_machine_commit,
                @src(),
            );

            const result = switch (operation) {
                .root => unreachable,
                .register => 0,
                .create_accounts => self.execute(.create_accounts, input, output),
                .create_transfers => self.execute(.create_transfers, input, output),
                .lookup_accounts => self.execute_lookup_accounts(input, output),
                .lookup_transfers => self.execute_lookup_transfers(input, output),
                else => unreachable,
            };

            tracer.end(
                &self.tracer_slot,
                .main,
                .state_machine_commit,
            );

            return result;
        }

        pub fn compact(self: *StateMachine, callback: fn (*StateMachine) void, op: u64) void {
            assert(self.compact_callback == null);
            assert(self.checkpoint_callback == null);

            tracer.start(
                &self.tracer_slot,
                .main,
                .state_machine_compact,
                @src(),
            );

            self.compact_callback = callback;
            self.forest.compact(compact_finish, op);
        }

        fn compact_finish(forest: *Forest) void {
            const self = @fieldParentPtr(StateMachine, "forest", forest);
            const callback = self.compact_callback.?;
            self.compact_callback = null;

            tracer.end(
                &self.tracer_slot,
                .main,
                .state_machine_compact,
            );

            callback(self);
        }

        pub fn checkpoint(self: *StateMachine, callback: fn (*StateMachine) void) void {
            assert(self.compact_callback == null);
            assert(self.checkpoint_callback == null);

            self.checkpoint_callback = callback;
            self.forest.checkpoint(checkpoint_finish);
        }

        fn checkpoint_finish(forest: *Forest) void {
            const self = @fieldParentPtr(StateMachine, "forest", forest);
            const callback = self.checkpoint_callback.?;
            self.checkpoint_callback = null;
            callback(self);
        }

        fn execute(
            self: *StateMachine,
            comptime operation: Operation,
            input: []align(16) const u8,
            output: []align(16) u8,
        ) usize {
            comptime assert(operation != .lookup_accounts and operation != .lookup_transfers);

            const events = mem.bytesAsSlice(Event(operation), input);
            var results = mem.bytesAsSlice(Result(operation), output);
            var count: usize = 0;

            var chain: ?usize = null;
            var chain_broken = false;

            for (events) |*event, index| {
                const result = blk: {
                    if (event.flags.linked) {
                        if (chain == null) {
                            chain = index;
                            assert(chain_broken == false);
                        }

                        if (index == events.len - 1) break :blk .linked_event_chain_open;
                    }

                    break :blk if (chain_broken) .linked_event_failed else switch (operation) {
                        .create_accounts => self.create_account(event),
                        .create_transfers => self.create_transfer(event),
                        else => unreachable,
                    };
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
                            assert(result == .linked_event_failed or result == .linked_event_chain_open);
                        }
                    }
                    results[count] = .{ .index = @intCast(u32, index), .result = result };
                    count += 1;
                }
                if (chain != null and (!event.flags.linked or result == .linked_event_chain_open)) {
                    chain = null;
                    chain_broken = false;
                }
            }
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

        // Accounts that do not fit in the response are omitted.
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

        // Transfers that do not fit in the response are omitted.
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
            if (a.id == math.maxInt(u128)) return .id_must_not_be_int_max;
            if (a.ledger == 0) return .ledger_must_not_be_zero;
            if (a.code == 0) return .code_must_not_be_zero;

            if (a.flags.debits_must_not_exceed_credits and a.flags.credits_must_not_exceed_debits) {
                return .mutually_exclusive_flags;
            }

            if (a.debits_pending != 0) return .debits_pending_must_be_zero;
            if (a.debits_posted != 0) return .debits_posted_must_be_zero;
            if (a.credits_pending != 0) return .credits_pending_must_be_zero;
            if (a.credits_posted != 0) return .credits_posted_must_be_zero;

            if (self.get_account(a.id)) |e| return create_account_exists(a, e);

            self.forest.grooves.accounts.put_no_clobber(a);

            self.commit_timestamp = a.timestamp;
            return .ok;
        }

        fn create_account_rollback(self: *StateMachine, a: *const Account) void {
            self.forest.grooves.accounts.remove(a.id);
        }

        fn create_account_exists(a: *const Account, e: *const Account) CreateAccountResult {
            assert(a.id == e.id);
            if (@bitCast(u16, a.flags) != @bitCast(u16, e.flags)) return .exists_with_different_flags;
            if (a.user_data != e.user_data) return .exists_with_different_user_data;
            assert(zeroed_48_bytes(a.reserved) and zeroed_48_bytes(e.reserved));
            if (a.ledger != e.ledger) return .exists_with_different_ledger;
            if (a.code != e.code) return .exists_with_different_code;
            return .exists;
        }

        fn create_transfer(self: *StateMachine, t: *const Transfer) CreateTransferResult {
            assert(t.timestamp > self.commit_timestamp);

            if (t.flags.padding != 0) return .reserved_flag;
            if (t.reserved != 0) return .reserved_field;

            if (t.id == 0) return .id_must_not_be_zero;
            if (t.id == math.maxInt(u128)) return .id_must_not_be_int_max;

            if (t.flags.post_pending_transfer or t.flags.void_pending_transfer) {
                return self.post_or_void_pending_transfer(t);
            }

            if (t.debit_account_id == 0) return .debit_account_id_must_not_be_zero;
            if (t.debit_account_id == math.maxInt(u128)) return .debit_account_id_must_not_be_int_max;
            if (t.credit_account_id == 0) return .credit_account_id_must_not_be_zero;
            if (t.credit_account_id == math.maxInt(u128)) return .credit_account_id_must_not_be_int_max;
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

            // If the transfer already exists, then it must not influence the overflow or limit checks.
            if (self.get_transfer(t.id)) |e| return create_transfer_exists(t, e);

            if (t.flags.pending) {
                if (sum_overflows(t.amount, dr.debits_pending)) return .overflows_debits_pending;
                if (sum_overflows(t.amount, cr.credits_pending)) return .overflows_credits_pending;
            }
            if (sum_overflows(t.amount, dr.debits_posted)) return .overflows_debits_posted;
            if (sum_overflows(t.amount, cr.credits_posted)) return .overflows_credits_posted;
            // We assert that the sum of the pending and posted balances can never overflow:
            if (sum_overflows(t.amount, dr.debits_pending + dr.debits_posted)) {
                return .overflows_debits;
            }
            if (sum_overflows(t.amount, cr.credits_pending + cr.credits_posted)) {
                return .overflows_credits;
            }
            if (sum_overflows(t.timestamp, t.timeout)) return .overflows_timeout;

            if (dr.debits_exceed_credits(t.amount)) return .exceeds_credits;
            if (cr.credits_exceed_debits(t.amount)) return .exceeds_debits;

            self.forest.grooves.transfers.put_no_clobber(t);

            var dr_new = dr.*;
            var cr_new = cr.*;
            if (t.flags.pending) {
                dr_new.debits_pending += t.amount;
                cr_new.credits_pending += t.amount;
            } else {
                dr_new.debits_posted += t.amount;
                cr_new.credits_posted += t.amount;
            }
            self.forest.grooves.accounts.put(&dr_new);
            self.forest.grooves.accounts.put(&cr_new);

            self.commit_timestamp = t.timestamp;
            return .ok;
        }

        fn create_transfer_rollback(self: *StateMachine, t: *const Transfer) void {
            if (t.flags.post_pending_transfer or t.flags.void_pending_transfer) {
                return self.post_or_void_pending_transfer_rollback(t);
            }

            var dr = self.get_account(t.debit_account_id).?.*;
            var cr = self.get_account(t.credit_account_id).?.*;
            assert(dr.id == t.debit_account_id);
            assert(cr.id == t.credit_account_id);

            if (t.flags.pending) {
                dr.debits_pending -= t.amount;
                cr.credits_pending -= t.amount;
            } else {
                dr.debits_posted -= t.amount;
                cr.credits_posted -= t.amount;
            }
            self.forest.grooves.accounts.put(&dr);
            self.forest.grooves.accounts.put(&cr);

            self.forest.grooves.transfers.remove(t.id);
        }

        fn create_transfer_exists(t: *const Transfer, e: *const Transfer) CreateTransferResult {
            assert(t.id == e.id);
            // The flags change the behavior of the remaining comparisons, so compare the flags first.
            if (@bitCast(u16, t.flags) != @bitCast(u16, e.flags)) return .exists_with_different_flags;
            if (t.debit_account_id != e.debit_account_id) {
                return .exists_with_different_debit_account_id;
            }
            if (t.credit_account_id != e.credit_account_id) {
                return .exists_with_different_credit_account_id;
            }
            if (t.user_data != e.user_data) return .exists_with_different_user_data;
            assert(t.reserved == 0 and e.reserved == 0);
            assert(t.pending_id == 0 and e.pending_id == 0); // We know that the flags are the same.
            if (t.timeout != e.timeout) return .exists_with_different_timeout;
            assert(t.ledger == e.ledger); // If the accounts are the same, the ledger must be the same.
            if (t.code != e.code) return .exists_with_different_code;
            if (t.amount != e.amount) return .exists_with_different_amount;
            return .exists;
        }

        fn post_or_void_pending_transfer(self: *StateMachine, t: *const Transfer) CreateTransferResult {
            assert(t.id != 0);
            assert(t.flags.padding == 0);
            assert(t.reserved == 0);
            assert(t.timestamp > self.commit_timestamp);
            assert(t.flags.post_pending_transfer or t.flags.void_pending_transfer);

            if (t.flags.post_pending_transfer and t.flags.void_pending_transfer) {
                return .cannot_post_and_void_pending_transfer;
            }
            if (t.flags.pending) return .pending_transfer_cannot_post_or_void_another;
            if (t.timeout != 0) return .timeout_reserved_for_pending_transfer;

            if (t.pending_id == 0) return .pending_id_must_not_be_zero;
            if (t.pending_id == math.maxInt(u128)) return .pending_id_must_not_be_int_max;
            if (t.pending_id == t.id) return .pending_id_must_be_different;

            const p = self.get_transfer(t.pending_id) orelse return .pending_transfer_not_found;
            assert(p.id == t.pending_id);
            if (!p.flags.pending) return .pending_transfer_not_pending;

            const dr = self.get_account(p.debit_account_id).?;
            const cr = self.get_account(p.credit_account_id).?;
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

            if (t.flags.void_pending_transfer and amount < p.amount) {
                return .pending_transfer_has_different_amount;
            }

            if (self.get_transfer(t.id)) |e| return post_or_void_pending_transfer_exists(t, e, p);

            if (self.get_posted(t.pending_id)) |posted| {
                if (posted) return .pending_transfer_already_posted;
                return .pending_transfer_already_voided;
            }

            assert(p.timestamp < t.timestamp);
            assert(p.timeout > 0);
            if (p.timestamp + p.timeout <= t.timestamp) return .pending_transfer_expired;

            self.forest.grooves.transfers.put_no_clobber(&Transfer{
                .id = t.id,
                .debit_account_id = p.debit_account_id,
                .credit_account_id = p.credit_account_id,
                .user_data = if (t.user_data > 0) t.user_data else p.user_data,
                .reserved = p.reserved,
                .ledger = p.ledger,
                .code = p.code,
                .pending_id = t.pending_id,
                .timeout = 0,
                .timestamp = t.timestamp,
                .flags = t.flags,
                .amount = amount,
            });

            self.forest.grooves.posted.put_no_clobber(t.pending_id, t.flags.post_pending_transfer);

            var dr_new = dr.*;
            var cr_new = cr.*;

            dr_new.debits_pending -= p.amount;
            cr_new.credits_pending -= p.amount;

            if (t.flags.post_pending_transfer) {
                assert(amount > 0);
                assert(amount <= p.amount);
                dr_new.debits_posted += amount;
                cr_new.credits_posted += amount;
            }

            self.forest.grooves.accounts.put(&dr_new);
            self.forest.grooves.accounts.put(&cr_new);

            self.commit_timestamp = t.timestamp;
            return .ok;
        }

        fn post_or_void_pending_transfer_rollback(self: *StateMachine, t: *const Transfer) void {
            assert(t.id > 0);
            assert(t.flags.post_pending_transfer or t.flags.void_pending_transfer);

            assert(t.pending_id > 0);
            const p = self.get_transfer(t.pending_id).?;
            assert(p.id == t.pending_id);
            assert(p.debit_account_id > 0);
            assert(p.credit_account_id > 0);

            var dr = self.get_account(p.debit_account_id).?.*;
            var cr = self.get_account(p.credit_account_id).?.*;
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

            self.forest.grooves.accounts.put(&dr);
            self.forest.grooves.accounts.put(&cr);

            self.forest.grooves.posted.remove(t.pending_id);
            self.forest.grooves.transfers.remove(t.id);
        }

        fn post_or_void_pending_transfer_exists(
            t: *const Transfer,
            e: *const Transfer,
            p: *const Transfer,
        ) CreateTransferResult {
            assert(p.flags.pending);
            assert(t.pending_id == p.id);
            assert(t.id != p.id);
            assert(t.id == e.id);

            // Do not assume that `e` is necessarily a posting or voiding transfer.
            if (@bitCast(u16, t.flags) != @bitCast(u16, e.flags)) {
                return .exists_with_different_flags;
            }

            // If `e` posted or voided a different pending transfer, then the accounts will differ.
            if (t.pending_id != e.pending_id) return .exists_with_different_pending_id;

            assert(e.flags.post_pending_transfer or e.flags.void_pending_transfer);
            assert(e.debit_account_id == p.debit_account_id);
            assert(e.credit_account_id == p.credit_account_id);
            assert(e.reserved == 0);
            assert(e.pending_id == p.id);
            assert(e.timeout == 0);
            assert(e.ledger == p.ledger);
            assert(e.code == p.code);
            assert(e.timestamp > p.timestamp);

            assert(t.flags.post_pending_transfer == e.flags.post_pending_transfer);
            assert(t.flags.void_pending_transfer == e.flags.void_pending_transfer);
            assert(t.debit_account_id == 0 or t.debit_account_id == e.debit_account_id);
            assert(t.credit_account_id == 0 or t.credit_account_id == e.credit_account_id);
            assert(t.reserved == 0);
            assert(t.timeout == 0);
            assert(t.ledger == 0 or t.ledger == e.ledger);
            assert(t.code == 0 or t.code == e.code);
            assert(t.timestamp > e.timestamp);

            if (t.user_data == 0) {
                if (e.user_data != p.user_data) return .exists_with_different_user_data;
            } else {
                if (t.user_data != e.user_data) return .exists_with_different_user_data;
            }

            if (t.amount == 0) {
                if (e.amount != p.amount) return .exists_with_different_amount;
            } else {
                if (t.amount != e.amount) return .exists_with_different_amount;
            }

            return .exists;
        }

        fn get_account(self: *const StateMachine, id: u128) ?*const Account {
            return self.forest.grooves.accounts.get(id);
        }

        fn get_transfer(self: *const StateMachine, id: u128) ?*const Transfer {
            return self.forest.grooves.transfers.get(id);
        }

        /// Returns whether a pending transfer, if it exists, has already been posted or voided.
        fn get_posted(self: *const StateMachine, pending_id: u128) ?bool {
            return self.forest.grooves.posted.get(pending_id);
        }

        pub fn forest_options(options: Options) Forest.GroovesOptions {
            const batch_accounts_max = @intCast(u32, constants.batch_max.create_accounts);
            const batch_transfers_max = @intCast(u32, constants.batch_max.create_transfers);
            assert(batch_accounts_max == constants.batch_max.lookup_accounts);
            assert(batch_transfers_max == constants.batch_max.lookup_transfers);

            return .{
                .accounts = .{
                    .cache_entries_max = options.cache_entries_accounts,
                    .prefetch_entries_max = std.math.max(
                        // create_account()/lookup_account() looks up 1 account per item.
                        batch_accounts_max,
                        // create_transfer()/post_or_void_pending_transfer() looks up 2
                        // accounts for every transfer.
                        2 * batch_transfers_max,
                    ),
                    .tree_options_object = .{
                        .commit_entries_max = math.max(
                            batch_accounts_max,
                            // ×2 because creating a transfer will update 2 accounts.
                            2 * batch_transfers_max,
                        ),
                    },
                    .tree_options_id = .{ .commit_entries_max = batch_accounts_max },
                    .tree_options_index = .{
                        .user_data = .{ .commit_entries_max = batch_accounts_max },
                        .ledger = .{ .commit_entries_max = batch_accounts_max },
                        .code = .{ .commit_entries_max = batch_accounts_max },
                        // Transfers mutate the secondary indices for debits/credits pending/posted.
                        //
                        // * Each mutation results in a remove and an insert: the ×2 multiplier.
                        // * Each transfer modifies two accounts. However, this does not
                        //   necessitate an additional ×2 multiplier — the credits of the debit
                        //   account and the debits of the credit account are not modified.
                        .debits_pending = .{
                            .commit_entries_max = math.max(
                                batch_accounts_max,
                                2 * batch_transfers_max,
                            ),
                        },
                        .debits_posted = .{
                            .commit_entries_max = math.max(
                                batch_accounts_max,
                                2 * batch_transfers_max,
                            ),
                        },
                        .credits_pending = .{
                            .commit_entries_max = math.max(
                                batch_accounts_max,
                                2 * batch_transfers_max,
                            ),
                        },
                        .credits_posted = .{
                            .commit_entries_max = math.max(
                                batch_accounts_max,
                                2 * batch_transfers_max,
                            ),
                        },
                    },
                },
                .transfers = .{
                    .cache_entries_max = options.cache_entries_transfers,
                    // *2 to fetch pending and post/void transfer.
                    .prefetch_entries_max = 2 * batch_transfers_max,
                    .tree_options_object = .{ .commit_entries_max = batch_transfers_max },
                    .tree_options_id = .{ .commit_entries_max = batch_transfers_max },
                    .tree_options_index = .{
                        .debit_account_id = .{ .commit_entries_max = batch_transfers_max },
                        .credit_account_id = .{ .commit_entries_max = batch_transfers_max },
                        .user_data = .{ .commit_entries_max = batch_transfers_max },
                        .pending_id = .{ .commit_entries_max = batch_transfers_max },
                        .timeout = .{ .commit_entries_max = batch_transfers_max },
                        .ledger = .{ .commit_entries_max = batch_transfers_max },
                        .code = .{ .commit_entries_max = batch_transfers_max },
                        .amount = .{ .commit_entries_max = batch_transfers_max },
                    },
                },
                .posted = .{
                    .cache_entries_max = options.cache_entries_posted,
                    .prefetch_entries_max = batch_transfers_max,
                    .commit_entries_max = batch_transfers_max,
                },
            };
        }
    };
}

fn sum_overflows(a: u64, b: u64) bool {
    var c: u64 = undefined;
    return @addWithOverflow(u64, a, b, &c);
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
const expect = testing.expect;
const expectEqual = testing.expectEqual;
const expectEqualSlices = testing.expectEqualSlices;

test "sum_overflows" {
    try expectEqual(false, sum_overflows(math.maxInt(u64), 0));
    try expectEqual(false, sum_overflows(math.maxInt(u64) - 1, 1));
    try expectEqual(false, sum_overflows(1, math.maxInt(u64) - 1));

    try expectEqual(true, sum_overflows(math.maxInt(u64), 1));
    try expectEqual(true, sum_overflows(1, math.maxInt(u64)));

    try expectEqual(true, sum_overflows(math.maxInt(u64), math.maxInt(u64)));
    try expectEqual(true, sum_overflows(math.maxInt(u64), math.maxInt(u64)));
}

const TestContext = struct {
    const Storage = @import("test/storage.zig").Storage;
    const MessagePool = @import("message_pool.zig").MessagePool;
    const SuperBlock = @import("vsr/superblock.zig").SuperBlockType(Storage);
    const Grid = @import("lsm/grid.zig").GridType(Storage);
    const StateMachine = StateMachineType(Storage, .{
        // Overestimate the batch size (in order to overprovision commit_entries_max)
        // because the test never compacts.
        .message_body_size_max = 1000 * @sizeOf(Account),
    });

    storage: Storage,
    message_pool: MessagePool,
    superblock: SuperBlock,
    grid: Grid,
    state_machine: StateMachine,

    fn init(ctx: *TestContext, allocator: mem.Allocator) !void {
        ctx.storage = try Storage.init(
            allocator,
            4096,
            .{
                .read_latency_min = 0,
                .read_latency_mean = 0,
                .write_latency_min = 0,
                .write_latency_mean = 0,
            },
        );
        errdefer ctx.storage.deinit(allocator);

        ctx.message_pool = .{
            .free_list = null,
            .messages_max = 0,
        };
        errdefer ctx.message_pool.deinit(allocator);

        ctx.superblock = try SuperBlock.init(allocator, &ctx.storage, &ctx.message_pool);
        errdefer ctx.superblock.deinit(allocator);

        // Pretend that the superblock is open so that the Forest can initialize.
        ctx.superblock.opened = true;
        ctx.superblock.working.vsr_state.commit_min = 0;

        ctx.grid = try Grid.init(allocator, &ctx.superblock);
        errdefer ctx.grid.deinit(allocator);

        ctx.state_machine = try StateMachine.init(allocator, &ctx.grid, .{
            .lsm_forest_node_count = 1,
            .cache_entries_accounts = 2048,
            .cache_entries_transfers = 2048,
            .cache_entries_posted = 2048,
        });
        errdefer ctx.state_machine.deinit(allocator);
    }

    fn deinit(ctx: *TestContext, allocator: mem.Allocator) void {
        ctx.storage.deinit(allocator);
        ctx.superblock.deinit(allocator);
        ctx.grid.deinit(allocator);
        ctx.state_machine.deinit(allocator);
        ctx.message_pool.deinit(allocator);
        ctx.* = undefined;
    }
};

const TestAction = union(enum) {
    // Set the account's balance.
    setup: struct {
        account: u128,
        debits_pending: u64,
        debits_posted: u64,
        credits_pending: u64,
        credits_posted: u64,
    },

    commit: TestContext.StateMachine.Operation,
    account: TestCreateAccount,
    transfer: TestCreateTransfer,

    lookup_account: struct {
        id: u128,
        balance: ?struct {
            debits_pending: u64,
            debits_posted: u64,
            credits_pending: u64,
            credits_posted: u64,
        } = null,
    },
    lookup_transfer: struct { id: u128, exists: bool },
};

const TestCreateAccount = struct {
    id: u128,
    user_data: u128 = 0,
    reserved: enum { @"0", @"1" } = .@"0",
    ledger: u32,
    code: u16,
    flags_linked: ?enum { LNK } = null,
    flags_debits_must_not_exceed_credits: ?enum { @"D<C" } = null,
    flags_credits_must_not_exceed_debits: ?enum { @"C<D" } = null,
    flags_padding: u13 = 0,
    debits_pending: u64 = 0,
    debits_posted: u64 = 0,
    credits_pending: u64 = 0,
    credits_posted: u64 = 0,
    result: CreateAccountResult,

    fn event(a: TestCreateAccount) Account {
        return .{
            .id = a.id,
            .user_data = a.user_data,
            .reserved = switch (a.reserved) {
                .@"0" => [_]u8{0} ** 48,
                .@"1" => [_]u8{1} ** 48,
            },
            .ledger = a.ledger,
            .code = a.code,
            .flags = .{
                .linked = a.flags_linked != null,
                .debits_must_not_exceed_credits = a.flags_debits_must_not_exceed_credits != null,
                .credits_must_not_exceed_debits = a.flags_credits_must_not_exceed_debits != null,
                .padding = a.flags_padding,
            },
            .debits_pending = a.debits_pending,
            .debits_posted = a.debits_posted,
            .credits_pending = a.credits_pending,
            .credits_posted = a.credits_posted,
            .timestamp = 0,
        };
    }
};

const TestCreateTransfer = struct {
    id: u128,
    debit_account_id: u128,
    credit_account_id: u128,
    user_data: u128 = 0,
    reserved: u128 = 0,
    pending_id: u128 = 0,
    timeout: u64 = 0,
    ledger: u32,
    code: u16,
    flags_linked: ?enum { LNK } = null,
    flags_pending: ?enum { PEN } = null,
    flags_post_pending_transfer: ?enum { POS } = null,
    flags_void_pending_transfer: ?enum { VOI } = null,
    flags_padding: u12 = 0,
    amount: u64 = 0,
    result: CreateTransferResult,

    fn event(t: TestCreateTransfer) Transfer {
        return .{
            .id = t.id,
            .debit_account_id = t.debit_account_id,
            .credit_account_id = t.credit_account_id,
            .user_data = t.user_data,
            .reserved = t.reserved,
            .pending_id = t.pending_id,
            .timeout = t.timeout,
            .ledger = t.ledger,
            .code = t.code,
            .flags = .{
                .linked = t.flags_linked != null,
                .pending = t.flags_pending != null,
                .post_pending_transfer = t.flags_post_pending_transfer != null,
                .void_pending_transfer = t.flags_void_pending_transfer != null,
                .padding = t.flags_padding,
            },
            .amount = t.amount,
            .timestamp = 0,
        };
    }
};

fn check(comptime test_table: []const u8) !void {
    const parse_table = @import("test/table.zig").parse;
    const allocator = std.testing.allocator;

    var context: TestContext = undefined;
    try context.init(allocator);
    defer context.deinit(allocator);

    const test_actions = try parse_table(allocator, TestAction, test_table);
    defer test_actions.deinit();

    var accounts = std.AutoHashMap(u128, Account).init(allocator);
    defer accounts.deinit();

    var transfers = std.AutoHashMap(u128, Transfer).init(allocator);
    defer transfers.deinit();

    var request = std.ArrayListAligned(u8, 16).init(allocator);
    defer request.deinit();

    var reply = std.ArrayListAligned(u8, 16).init(allocator);
    defer reply.deinit();

    var operation: ?TestContext.StateMachine.Operation = null;
    for (test_actions.items) |test_action| {
        switch (test_action) {
            .setup => |b| {
                assert(operation == null);

                var account = context.state_machine.get_account(b.account).?.*;
                account.debits_pending = b.debits_pending;
                account.debits_posted = b.debits_posted;
                account.credits_pending = b.credits_pending;
                account.credits_posted = b.credits_posted;
                context.state_machine.forest.grooves.accounts.put(&account);
            },

            .account => |a| {
                assert(operation == null or operation.? == .create_accounts);
                operation = .create_accounts;

                const event = a.event();
                try request.appendSlice(std.mem.asBytes(&event));
                if (a.result == .ok) {
                    try accounts.put(a.id, event);
                } else {
                    const result = CreateAccountsResult{
                        .index = @intCast(u32, @divExact(request.items.len, @sizeOf(Account)) - 1),
                        .result = a.result,
                    };
                    try reply.appendSlice(std.mem.asBytes(&result));
                }
            },
            .transfer => |t| {
                assert(operation == null or operation.? == .create_transfers);
                operation = .create_transfers;

                const event = t.event();
                try request.appendSlice(std.mem.asBytes(&event));
                if (t.result == .ok) {
                    try transfers.put(t.id, event);
                } else {
                    const result = CreateTransfersResult{
                        .index = @intCast(u32, @divExact(request.items.len, @sizeOf(Transfer)) - 1),
                        .result = t.result,
                    };
                    try reply.appendSlice(std.mem.asBytes(&result));
                }
            },
            .lookup_account => |a| {
                assert(operation == null or operation.? == .lookup_accounts);
                operation = .lookup_accounts;

                try request.appendSlice(std.mem.asBytes(&a.id));
                if (a.balance) |b| {
                    var account = accounts.get(a.id).?;
                    account.debits_pending = b.debits_pending;
                    account.debits_posted = b.debits_posted;
                    account.credits_pending = b.credits_pending;
                    account.credits_posted = b.credits_posted;
                    try reply.appendSlice(std.mem.asBytes(&account));
                }
            },
            .lookup_transfer => |t| {
                assert(operation == null or operation.? == .lookup_transfers);
                operation = .lookup_transfers;

                try request.appendSlice(std.mem.asBytes(&t.id));
                if (t.exists) {
                    var transfer = transfers.get(t.id).?;
                    try reply.appendSlice(std.mem.asBytes(&transfer));
                }
            },

            .commit => |commit_operation| {
                assert(operation == null or operation.? == commit_operation);
                _ = context.state_machine.prepare(commit_operation, request.items);

                const reply_actual_buffer = try allocator.alignedAlloc(u8, 16, 4096);
                defer allocator.free(reply_actual_buffer);

                const reply_actual_size = context.state_machine.commit(
                    0,
                    1,
                    commit_operation,
                    request.items,
                    reply_actual_buffer,
                );
                var reply_actual = reply_actual_buffer[0..reply_actual_size];

                if (commit_operation == .lookup_accounts) {
                    for (std.mem.bytesAsSlice(Account, reply_actual)) |*a| a.timestamp = 0;
                }

                if (commit_operation == .lookup_transfers) {
                    for (std.mem.bytesAsSlice(Transfer, reply_actual)) |*t| t.timestamp = 0;
                }

                // TODO(Zig): Use inline-switch to cast the replies to []Reply(operation), then
                // change this to a simple "try".
                testing.expectEqualSlices(u8, reply.items, reply_actual) catch |err| {
                    print_results("expect", commit_operation, reply.items);
                    print_results("actual", commit_operation, reply_actual);
                    return err;
                };

                request.clearRetainingCapacity();
                reply.clearRetainingCapacity();
                operation = null;
            },
        }
    }

    assert(operation == null);
    assert(request.items.len == 0);
    assert(reply.items.len == 0);
}

fn print_results(
    label: []const u8,
    operation: TestContext.StateMachine.Operation,
    reply: []const u8,
) void {
    inline for (.{
        .create_accounts,
        .create_transfers,
        .lookup_accounts,
        .lookup_transfers,
    }) |o| {
        if (o == operation) {
            const Result = TestContext.StateMachine.Result(o);
            const results = std.mem.bytesAsSlice(Result, reply);
            std.debug.print("{s}={any}\n", .{ label, results });
            return;
        }
    }
    unreachable;
}

test "create_accounts" {
    try check(
        \\ account A1 U2 _ L3 C4 _   _   _ _  0  0  0  0 ok
        \\ account A0  _ 1 L0 C0 _ D<C C<D 1  1  1  1  1 reserved_flag
        \\ account A0  _ 1 L0 C0 _ D<C C<D _  1  1  1  1 reserved_field
        \\ account A0  _ _ L0 C0 _ D<C C<D _  1  1  1  1 id_must_not_be_zero
        \\ account -0  _ _ L0 C0 _ D<C C<D _  1  1  1  1 id_must_not_be_int_max
        \\ account A1 U1 _ L0 C0 _ D<C C<D _  1  1  1  1 ledger_must_not_be_zero
        \\ account A1 U1 _ L9 C0 _ D<C C<D _  1  1  1  1 code_must_not_be_zero
        \\ account A1 U1 _ L9 C9 _ D<C C<D _ -0 -0 -0 -0 mutually_exclusive_flags
        \\ account A1 U1 _ L9 C9 _ D<C   _ _  1  1  1  1 debits_pending_must_be_zero
        \\ account A1 U1 _ L9 C9 _ D<C   _ _  0  1  1  1 debits_posted_must_be_zero
        \\ account A1 U1 _ L9 C9 _ D<C   _ _  0  0  1  1 credits_pending_must_be_zero
        \\ account A1 U1 _ L9 C9 _ D<C   _ _  0  0  0  1 credits_posted_must_be_zero
        \\ account A1 U1 _ L9 C9 _ D<C   _ _  0  0  0  0 exists_with_different_flags
        \\ account A1 U1 _ L9 C9 _   _ C<D _  0  0  0  0 exists_with_different_flags
        \\ account A1 U1 _ L9 C9 _   _   _ _  0  0  0  0 exists_with_different_user_data
        \\ account A1 U2 _ L9 C9 _   _   _ _  0  0  0  0 exists_with_different_ledger
        \\ account A1 U2 _ L3 C9 _   _   _ _  0  0  0  0 exists_with_different_code
        \\ account A1 U2 _ L3 C4 _   _   _ _  0  0  0  0 exists
        \\ commit create_accounts
        \\
        \\ lookup_account -0 _
        \\ lookup_account A0 _
        \\ lookup_account A1 0 0 0 0
        \\ lookup_account A2 _
        \\ commit lookup_accounts
    );
}

test "create_accounts: empty" {
    try check(
        \\ commit create_transfers
    );
}

test "linked accounts" {
    try check(
        \\ account A7 _ _ L1 C1   _ _ _ _ 0 0 0 0 ok // An individual event (successful):

        // A chain of 4 events (the last event in the chain closes the chain with linked=false):
        \\ account A1 _ _ L1 C1 LNK _ _ _ 0 0 0 0 linked_event_failed // Commit/rollback.
        \\ account A2 _ _ L1 C1 LNK _ _ _ 0 0 0 0 linked_event_failed // Commit/rollback.
        \\ account A1 _ _ L1 C1 LNK _ _ _ 0 0 0 0 exists              // Fail with .exists.
        \\ account A3 _ _ L1 C1   _ _ _ _ 0 0 0 0 linked_event_failed // Fail without committing.

        // An individual event (successful):
        // This does not see any effect from the failed chain above.
        \\ account A1 _ _ L1 C1   _ _ _ _ 0 0 0 0 ok

        // A chain of 2 events (the first event fails the chain):
        \\ account A1 _ _ L1 C2 LNK _ _ _ 0 0 0 0 exists_with_different_flags
        \\ account A2 _ _ L1 C1   _ _ _ _ 0 0 0 0 linked_event_failed

        // An individual event (successful):
        \\ account A2 _ _ L1 C1   _ _ _ _ 0 0 0 0 ok

        // A chain of 2 events (the last event fails the chain):
        \\ account A3 _ _ L1 C1 LNK _ _ _ 0 0 0 0 linked_event_failed
        \\ account A1 _ _ L2 C1   _ _ _ _ 0 0 0 0 exists_with_different_ledger

        // A chain of 2 events (successful):
        \\ account A3 _ _ L1 C1 LNK _ _ _ 0 0 0 0 ok
        \\ account A4 _ _ L1 C1   _ _ _ _ 0 0 0 0 ok
        \\ commit create_accounts
        \\
        \\ lookup_account A7 0 0 0 0
        \\ lookup_account A1 0 0 0 0
        \\ lookup_account A2 0 0 0 0
        \\ lookup_account A3 0 0 0 0
        \\ lookup_account A4 0 0 0 0
        \\ commit lookup_accounts
    );

    try check(
        \\ account A7 _ _ L1 C1   _ _ _ _ 0 0 0 0 ok // An individual event (successful):

        // A chain of 4 events:
        \\ account A1 _ _ L1 C1 LNK _ _ _ 0 0 0 0 linked_event_failed // Commit/rollback.
        \\ account A2 _ _ L1 C1 LNK _ _ _ 0 0 0 0 linked_event_failed // Commit/rollback.
        \\ account A1 _ _ L1 C1 LNK _ _ _ 0 0 0 0 exists              // Fail with .exists.
        \\ account A3 _ _ L1 C1   _ _ _ _ 0 0 0 0 linked_event_failed // Fail without committing.
        \\ commit create_accounts
        \\
        \\ lookup_account A7 0 0 0 0
        \\ lookup_account A1 _
        \\ lookup_account A2 _
        \\ lookup_account A3 _
        \\ commit lookup_accounts
    );

    // TODO How can we test that events were in fact rolled back in LIFO order?
    // All our rollback handlers appear to be commutative.
}

test "linked_event_chain_open" {
    try check(
    // A chain of 3 events (the last event in the chain closes the chain with linked=false):
        \\ account A1 _ _ L1 C1 LNK _ _ _ 0 0 0 0 ok
        \\ account A2 _ _ L1 C1 LNK _ _ _ 0 0 0 0 ok
        \\ account A3 _ _ L1 C1   _ _ _ _ 0 0 0 0 ok

        // An open chain of 2 events:
        \\ account A4 _ _ L1 C1 LNK _ _ _ 0 0 0 0 linked_event_failed
        \\ account A5 _ _ L1 C1 LNK _ _ _ 0 0 0 0 linked_event_chain_open
        \\ commit create_accounts
        \\
        \\ lookup_account A1 0 0 0 0
        \\ lookup_account A2 0 0 0 0
        \\ lookup_account A3 0 0 0 0
        \\ lookup_account A4 _
        \\ lookup_account A5 _
        \\ commit lookup_accounts
    );
}

test "linked_event_chain_open for an already failed batch" {
    try check(
    // An individual event (successful):
        \\ account A1 _ _ L1 C1   _ _ _ _ 0 0 0 0 ok

        // An open chain of 3 events (the second one fails):
        \\ account A2 _ _ L1 C1 LNK _ _ _ 0 0 0 0 linked_event_failed
        \\ account A1 _ _ L1 C1 LNK _ _ _ 0 0 0 0 exists_with_different_flags
        \\ account A3 _ _ L1 C1 LNK _ _ _ 0 0 0 0 linked_event_chain_open
        \\ commit create_accounts
        \\
        \\ lookup_account A1 0 0 0 0
        \\ lookup_account A2 _
        \\ lookup_account A3 _
        \\ commit lookup_accounts
    );
}

test "linked_event_chain_open for a batch of 1" {
    try check(
        \\ account A1 _ _ L1 C1 LNK _ _ _ 0 0 0 0 linked_event_chain_open
        \\ commit create_accounts
        \\
        \\ lookup_account A1 _
        \\ commit lookup_accounts
    );
}

// The goal is to ensure that:
// 1. all CreateTransferResult enums are covered, with
// 2. enums tested in the order that they are defined, for easier auditing of coverage, and that
// 3. state machine logic cannot be reordered in any way, breaking determinism.
test "create_transfers/lookup_transfers" {
    try check(
        \\ account A1 _ _ L1 C1 _   _   _ _ 0 0 0 0 ok
        \\ account A2 _ _ L2 C2 _   _   _ _ 0 0 0 0 ok
        \\ account A3 _ _ L1 C1 _   _   _ _ 0 0 0 0 ok
        \\ account A4 _ _ L1 C1 _ D<C   _ _ 0 0 0 0 ok
        \\ account A5 _ _ L1 C1 _   _ C<D _ 0 0 0 0 ok
        \\ commit create_accounts

        // Set up initial balances.
        \\ setup A1  100   200    0     0
        \\ setup A2    0     0    0     0
        \\ setup A3    0     0  110   210
        \\ setup A4   20  -700    0  -500
        \\ setup A5    0 -1000   10 -1100

        // Test errors by descending precedence.
        \\ transfer T0 A0 A0  _ R1 T1   _ L0 C0 _ PEN _ _ P1    0 reserved_flag
        \\ transfer T0 A0 A0  _ R1 T1   _ L0 C0 _ PEN _ _  _    0 reserved_field
        \\ transfer T0 A0 A0  _  _ T1   _ L0 C0 _ PEN _ _  _    0 id_must_not_be_zero
        \\ transfer -0 A0 A0  _  _ T1   _ L0 C0 _ PEN _ _  _    0 id_must_not_be_int_max
        \\ transfer T1 A0 A0  _  _ T1   _ L0 C0 _ PEN _ _  _    0 debit_account_id_must_not_be_zero
        \\ transfer T1 -0 A0  _  _ T1   _ L0 C0 _ PEN _ _  _    0 debit_account_id_must_not_be_int_max
        \\ transfer T1 A8 A0  _  _ T1   _ L0 C0 _ PEN _ _  _    0 credit_account_id_must_not_be_zero
        \\ transfer T1 A8 -0  _  _ T1   _ L0 C0 _ PEN _ _  _    0 credit_account_id_must_not_be_int_max
        \\ transfer T1 A8 A8  _  _ T1   _ L0 C0 _ PEN _ _  _    0 accounts_must_be_different
        \\ transfer T1 A8 A9  _  _ T1   _ L0 C0 _ PEN _ _  _    0 pending_id_must_be_zero
        \\ transfer T1 A8 A9  _  _  _   _ L0 C0 _ PEN _ _  _    0 pending_transfer_must_timeout
        \\ transfer T1 A8 A9  _  _  _  -0 L0 C0 _   _ _ _  _    0 timeout_reserved_for_pending_transfer
        \\ transfer T1 A8 A9  _  _  _  -0 L0 C0 _ PEN _ _  _    0 ledger_must_not_be_zero
        \\ transfer T1 A8 A9  _  _  _  -0 L9 C0 _ PEN _ _  _    0 code_must_not_be_zero
        \\ transfer T1 A8 A9  _  _  _  -0 L9 C1 _ PEN _ _  _    0 amount_must_not_be_zero
        \\ transfer T1 A8 A9  _  _  _  -0 L9 C1 _ PEN _ _  _    9 debit_account_not_found
        \\ transfer T1 A1 A9  _  _  _  -0 L9 C1 _ PEN _ _  _    9 credit_account_not_found
        \\ transfer T1 A1 A2  _  _  _  -0 L9 C1 _ PEN _ _  _    1 accounts_must_have_the_same_ledger
        \\ transfer T1 A1 A3  _  _  _  -0 L9 C1 _ PEN _ _  _    1 transfer_must_have_the_same_ledger_as_accounts
        \\ transfer T1 A1 A3  _  _  _  -0 L1 C1 _ PEN _ _  _  -99 overflows_debits_pending  // amount = max - A1.debits_pending + 1
        \\ transfer T1 A1 A3  _  _  _  -0 L1 C1 _ PEN _ _  _ -109 overflows_credits_pending // amount = max - A3.credits_pending + 1
        \\ transfer T1 A1 A3  _  _  _  -0 L1 C1 _ PEN _ _  _ -199 overflows_debits_posted   // amount = max - A1.debits_posted + 1
        \\ transfer T1 A1 A3  _  _  _  -0 L1 C1 _ PEN _ _  _ -209 overflows_credits_posted  // amount = max - A3.credits_posted + 1
        \\ transfer T1 A1 A3  _  _  _  -0 L1 C1 _ PEN _ _  _ -299 overflows_debits          // amount = max - A1.debits_pending - A1.debits_posted + 1
        \\ transfer T1 A1 A3  _  _  _  -0 L1 C1 _ PEN _ _  _ -319 overflows_credits         // amount = max - A3.credits_pending - A3.credits_posted + 1
        \\ transfer T1 A4 A5  _  _  _  -0 L1 C1 _ PEN _ _  _  199 overflows_timeout         // amount = A4.credits_posted - A4.debits_pending - A4.debits_posted + 1
        \\ transfer T1 A4 A5  _  _  _   _ L1 C1 _   _ _ _  _  199 exceeds_credits           // amount = A4.credits_posted - A4.debits_pending - A4.debits_posted + 1
        \\ transfer T1 A4 A5  _  _  _   _ L1 C1 _   _ _ _  _   91 exceeds_debits            // amount = A5.debits_posted - A5.credits_pending - A5.credits_posted + 1
        \\ transfer T1 A1 A3  _  _  _ 999 L1 C1 _ PEN _ _  _  123 ok

        // Ensure that idempotence is only checked after validation.
        \\ transfer T1 A1 A3  _  _  _ 999 L2 C1 _ PEN _ _  _  123 transfer_must_have_the_same_ledger_as_accounts
        \\ transfer T1 A1 A3 U1  _  _   _ L1 C2 _   _ _ _  _   -0 exists_with_different_flags
        \\ transfer T1 A3 A1 U1  _  _ 999 L1 C2 _ PEN _ _  _   -0 exists_with_different_debit_account_id
        \\ transfer T1 A1 A4 U1  _  _ 999 L1 C2 _ PEN _ _  _   -0 exists_with_different_credit_account_id
        \\ transfer T1 A1 A3 U1  _  _ 999 L1 C2 _ PEN _ _  _   -0 exists_with_different_user_data
        \\ transfer T1 A1 A3  _  _  _ 998 L1 C2 _ PEN _ _  _   -0 exists_with_different_timeout
        \\ transfer T1 A1 A3  _  _  _ 999 L1 C2 _ PEN _ _  _   -0 exists_with_different_code
        \\ transfer T1 A1 A3  _  _  _ 999 L1 C1 _ PEN _ _  _   -0 exists_with_different_amount
        \\ transfer T1 A1 A3  _  _  _ 999 L1 C1 _ PEN _ _  _  123 exists
        \\ transfer T2 A3 A1  _  _  _   _ L1 C2 _   _ _ _  _    7 ok
        \\ transfer T3 A1 A3  _  _  _   _ L1 C2 _   _ _ _  _    3 ok
        \\ commit create_transfers
        \\
        \\ lookup_account A1 223 203   0   7
        \\ lookup_account A3   0   7 233 213
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T1 true
        \\ lookup_transfer T2 true
        \\ lookup_transfer T3 true
        \\ lookup_transfer -0 false
        \\ commit lookup_transfers
    );
}

test "create/lookup 2-phase transfers" {
    try check(
        \\ account A1 _ _ L1 C1   _  _  _  _ 0 0 0 0 ok
        \\ account A2 _ _ L1 C1   _  _  _  _ 0 0 0 0 ok
        \\ commit create_accounts

        // First phase.
        \\ transfer   T1 A1 A2  _ _   _    _ L1 C1 _   _   _   _ _ 15 ok // Not pending!
        \\ transfer   T2 A1 A2  _ _   _ 1000 L1 C1 _ PEN   _   _ _ 15 ok
        \\ transfer   T3 A1 A2  _ _   _   50 L1 C1 _ PEN   _   _ _ 15 ok
        \\ transfer   T4 A1 A2  _ _   _    1 L1 C1 _ PEN   _   _ _ 15 ok
        \\ transfer   T5 A1 A2 U9 _   _   50 L1 C1 _ PEN   _   _ _  7 ok
        \\ commit create_transfers

        // Check balances before resolving.
        \\ lookup_account A1 52 15  0  0
        \\ lookup_account A2  0  0 52 15
        \\ commit lookup_accounts

        // Second phase.
        \\ transfer T101 A1 A2 U1 _  T2    _ L1 C1 _   _ POS   _ _ 13 ok
        \\ transfer T101 A8 A9 U2 _  T0   50 L6 C7 _ PEN POS VOI _ 16 cannot_post_and_void_pending_transfer
        \\ transfer T101 A8 A9 U2 _  T0   50 L6 C7 _ PEN   _ VOI _ 16 pending_transfer_cannot_post_or_void_another
        \\ transfer T101 A8 A9 U2 _  T0   50 L6 C7 _   _   _ VOI _ 16 timeout_reserved_for_pending_transfer
        \\ transfer T101 A8 A9 U2 _  T0    _ L6 C7 _   _   _ VOI _ 16 pending_id_must_not_be_zero
        \\ transfer T101 A8 A9 U2 _  -0    _ L6 C7 _   _   _ VOI _ 16 pending_id_must_not_be_int_max
        \\ transfer T101 A8 A9 U2 _ 101    _ L6 C7 _   _   _ VOI _ 16 pending_id_must_be_different
        \\ transfer T101 A8 A9 U2 _ 102    _ L6 C7 _   _   _ VOI _ 16 pending_transfer_not_found
        \\ transfer T101 A8 A9 U2 _  T1    _ L6 C7 _   _   _ VOI _ 16 pending_transfer_not_pending
        \\ transfer T101 A8 A9 U2 _  T2    _ L6 C7 _   _   _ VOI _ 16 pending_transfer_has_different_debit_account_id
        \\ transfer T101 A1 A9 U2 _  T2    _ L6 C7 _   _   _ VOI _ 16 pending_transfer_has_different_credit_account_id
        \\ transfer T101 A1 A2 U2 _  T2    _ L6 C7 _   _   _ VOI _ 16 pending_transfer_has_different_ledger
        \\ transfer T101 A1 A2 U2 _  T2    _ L1 C7 _   _   _ VOI _ 16 pending_transfer_has_different_code
        \\ transfer T101 A1 A2 U2 _  T2    _ L1 C1 _   _   _ VOI _ 16 exceeds_pending_transfer_amount
        \\ transfer T101 A1 A2 U2 _  T2    _ L1 C1 _   _   _ VOI _ 14 pending_transfer_has_different_amount
        \\ transfer T101 A1 A2 U2 _  T2    _ L1 C1 _   _   _ VOI _ 15 exists_with_different_flags
        \\ transfer T101 A1 A2 U2 _  T3    _ L1 C1 _   _ POS   _ _ 14 exists_with_different_pending_id
        \\ transfer T101 A1 A2 U2 _  T2    _ L1 C1 _   _ POS   _ _ 14 exists_with_different_user_data
        \\ transfer T101 A1 A2 U0 _  T2    _ L1 C1 _   _ POS   _ _ 14 exists_with_different_user_data
        \\ transfer T101 A1 A2 U1 _  T2    _ L1 C1 _   _ POS   _ _ 14 exists_with_different_amount
        \\ transfer T101 A1 A2 U1 _  T2    _ L1 C1 _   _ POS   _ _  _ exists_with_different_amount
        \\ transfer T101 A1 A2 U1 _  T2    _ L1 C1 _   _ POS   _ _ 13 exists
        \\ transfer T102 A1 A2 U1 _  T2    _ L1 C1 _   _ POS   _ _ 13 pending_transfer_already_posted
        \\ transfer T103 A1 A2 U1 _  T3    _ L1 C1 _   _   _ VOI _ 15 ok
        \\ transfer T102 A1 A2 U1 _  T3    _ L1 C1 _   _ POS   _ _ 13 pending_transfer_already_voided
        \\ transfer T102 A1 A2 U1 _  T4    _ L1 C1 _   _   _ VOI _ 15 pending_transfer_expired
        \\ transfer T105 A0 A0 U0 _  T5    _ L0 C0 _   _ POS   _ _  _ ok
        \\ commit create_transfers

        // Check balances after resolving.
        \\ lookup_account A1 15 35  0  0
        \\ lookup_account A2  0  0 15 35
        \\ commit lookup_accounts
    );
}

test "create_transfers: empty" {
    try check(
        \\ commit create_transfers
    );
}

test "create_transfers/lookup_transfers: failed transfer does not exist" {
    try check(
        \\ account A1 _ _ L1 C1   _  _  _  _ 0 0 0 0 ok
        \\ account A2 _ _ L1 C1   _  _  _  _ 0 0 0 0 ok
        \\ commit create_accounts
        \\
        \\ transfer T1 A1 A2  _ _   _ _ L1 C1 _ _ _ _ _ 15 ok
        \\ transfer T2 A1 A2  _ _   _ _ L0 C1 _ _ _ _ _ 15 ledger_must_not_be_zero
        \\ commit create_transfers
        \\
        \\ lookup_account A1 0 15 0  0
        \\ lookup_account A2 0  0 0 15
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T1 true
        \\ lookup_transfer T2 false
        \\ commit lookup_transfers
    );
}

test "create_transfers/lookup_transfers: failed linked-chains are undone" {
    try check(
        \\ account A1 _ _ L1 C1   _  _  _  _ 0 0 0 0 ok
        \\ account A2 _ _ L1 C1   _  _  _  _ 0 0 0 0 ok
        \\ commit create_accounts
        \\
        \\ transfer T1 A1 A2  _ _   _ _ L1 C1 LNK   _ _ _ _ 15 linked_event_failed
        \\ transfer T2 A1 A2  _ _   _ _ L0 C1   _   _ _ _ _ 15 ledger_must_not_be_zero
        \\ commit create_transfers
        \\
        \\ transfer T3 A1 A2  _ _   _ 1 L1 C1 LNK PEN _ _ _ 15 linked_event_failed
        \\ transfer T4 A1 A2  _ _   _ _ L0 C1   _   _ _ _ _ 15 ledger_must_not_be_zero
        \\ commit create_transfers
        \\
        \\ lookup_account A1 0 0 0 0
        \\ lookup_account A2 0 0 0 0
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T1 false
        \\ lookup_transfer T2 false
        \\ lookup_transfer T3 false
        \\ lookup_transfer T4 false
        \\ commit lookup_transfers
    );
}

test "zeroed_32_bytes" {
    try test_zeroed_n_bytes(32);
}

test "zeroed_48_bytes" {
    try test_zeroed_n_bytes(48);
}

fn test_zeroed_n_bytes(comptime n: usize) !void {
    const routine = switch (n) {
        32 => zeroed_32_bytes,
        48 => zeroed_48_bytes,
        else => unreachable,
    };
    var a = [_]u8{0} ** n;
    var i: usize = 0;
    while (i < a.len) : (i += 1) {
        a[i] = 1;
        try expectEqual(false, routine(a));
        a[i] = 0;
    }
    try expectEqual(true, routine(a));
}

test "equal_32_bytes" {
    try test_equal_n_bytes(32);
}

test "equal_48_bytes" {
    try test_equal_n_bytes(48);
}

fn test_equal_n_bytes(comptime n: usize) !void {
    const routine = switch (n) {
        32 => equal_32_bytes,
        48 => equal_48_bytes,
        else => unreachable,
    };
    var a = [_]u8{0} ** n;
    var b = [_]u8{0} ** n;
    var i: usize = 0;
    while (i < a.len) : (i += 1) {
        a[i] = 1;
        try expectEqual(false, routine(a, b));
        a[i] = 0;

        b[i] = 1;
        try expectEqual(false, routine(a, b));
        b[i] = 0;
    }
    try expectEqual(true, routine(a, b));
}

test "StateMachine: ref all decls" {
    const Storage = @import("storage.zig").Storage;
    const StateMachine = StateMachineType(Storage, .{
        .message_body_size_max = 1000 * @sizeOf(Account),
    });

    std.testing.refAllDecls(StateMachine);
}
