const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const log = std.log.scoped(.state_machine);
const tracer = @import("tracer.zig");

const tb = @import("tigerbeetle.zig");
const snapshot_latest = @import("lsm/tree.zig").snapshot_latest;
const WorkloadType = @import("state_machine/workload.zig").WorkloadType;

const Account = tb.Account;
const AccountFlags = tb.AccountFlags;

const Transfer = tb.Transfer;
const TransferFlags = tb.TransferFlags;

const CreateAccountsResult = tb.CreateAccountsResult;
const CreateTransfersResult = tb.CreateTransfersResult;

const CreateAccountResult = tb.CreateAccountResult;
const CreateTransferResult = tb.CreateTransferResult;

pub fn StateMachineType(
    comptime Storage: type,
    comptime config: @import("constants.zig").StateMachineConfig,
) type {
    assert(config.message_body_size_max > 0);
    assert(config.lsm_batch_multiple > 0);
    assert(config.vsr_operations_reserved > 0);

    return struct {
        const StateMachine = @This();
        const Grid = @import("lsm/grid.zig").GridType(Storage);
        const GrooveType = @import("lsm/groove.zig").GrooveType;
        const ForestType = @import("lsm/forest.zig").ForestType;

        pub const constants = struct {
            pub const message_body_size_max = config.message_body_size_max;

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
                    return @divFloor(message_body_size_max, std.math.max(
                        @sizeOf(Event(operation)),
                        @sizeOf(Result(operation)),
                    ));
                }
            };
        };

        pub const AccountImmutable = extern struct {
            id: u128,
            user_data: u128,
            timestamp: u64,
            ledger: u32,
            code: u16,
            flags: AccountFlags,
            padding: [16]u8,

            comptime {
                assert(@sizeOf(AccountImmutable) == 64);
                assert(@bitSizeOf(AccountImmutable) == @sizeOf(AccountImmutable) * 8);
            }

            pub fn from_account(a: *const Account) AccountImmutable {
                return .{
                    .id = a.id,
                    .user_data = a.user_data,
                    .timestamp = a.timestamp,
                    .ledger = a.ledger,
                    .code = a.code,
                    .flags = a.flags,
                    .padding = mem.zeroes([16]u8),
                };
            }
        };

        pub const AccountMutable = extern struct {
            debits_pending: u64,
            debits_posted: u64,
            credits_pending: u64,
            credits_posted: u64,
            timestamp: u64,
            padding: [24]u8,

            comptime {
                assert(@sizeOf(AccountMutable) == 64);
                assert(@bitSizeOf(AccountMutable) == @sizeOf(AccountMutable) * 8);
            }

            pub fn from_account(a: *const Account) AccountMutable {
                return .{
                    .debits_pending = a.debits_pending,
                    .debits_posted = a.debits_posted,
                    .credits_pending = a.credits_pending,
                    .credits_posted = a.credits_posted,
                    .timestamp = a.timestamp,
                    .padding = mem.zeroes([24]u8),
                };
            }
        };

        pub fn into_account(immut: *const AccountImmutable, mut: *const AccountMutable) Account {
            assert(immut.timestamp == mut.timestamp);
            return Account{
                .id = immut.id,
                .user_data = immut.user_data,
                .reserved = mem.zeroes([48]u8),
                .ledger = immut.ledger,
                .code = immut.code,
                .flags = immut.flags,
                .debits_pending = mut.debits_pending,
                .debits_posted = mut.debits_posted,
                .credits_pending = mut.credits_pending,
                .credits_posted = mut.credits_posted,
                .timestamp = mut.timestamp,
            };
        }

        const AccountsImmutableGroove = GrooveType(
            Storage,
            AccountImmutable,
            .{
                .value_count_max = .{
                    .timestamp = config.lsm_batch_multiple * math.max(
                        constants.batch_max.create_accounts,
                        // ×2 because creating a transfer will update 2 accounts.
                        2 * constants.batch_max.create_transfers,
                    ),
                    .id = config.lsm_batch_multiple * constants.batch_max.create_accounts,
                    .user_data = config.lsm_batch_multiple * constants.batch_max.create_accounts,
                    .ledger = config.lsm_batch_multiple * constants.batch_max.create_accounts,
                    .code = config.lsm_batch_multiple * constants.batch_max.create_accounts,
                },
                .ignored = &[_][]const u8{ "flags", "padding" },
                .derived = .{},
            },
        );

        const AccountsMutableGroove = GrooveType(
            Storage,
            AccountMutable,
            .{
                .value_count_max = .{
                    .timestamp = config.lsm_batch_multiple * math.max(
                        constants.batch_max.create_accounts,
                        // ×2 because creating a transfer will update 2 accounts.
                        2 * constants.batch_max.create_transfers,
                    ),
                    // Transfers mutate the secondary indices for debits/credits pending/posted.
                    //
                    // * Each mutation results in a remove and an insert: the ×2 multiplier.
                    // * Each transfer modifies two accounts. However, this does not
                    //   necessitate an additional ×2 multiplier — the credits of the debit
                    //   account and the debits of the credit account are not modified.
                    .debits_pending = config.lsm_batch_multiple * math.max(
                        constants.batch_max.create_accounts,
                        2 * constants.batch_max.create_transfers,
                    ),
                    .debits_posted = config.lsm_batch_multiple * math.max(
                        constants.batch_max.create_accounts,
                        2 * constants.batch_max.create_transfers,
                    ),
                    .credits_pending = config.lsm_batch_multiple * math.max(
                        constants.batch_max.create_accounts,
                        2 * constants.batch_max.create_transfers,
                    ),
                    .credits_posted = config.lsm_batch_multiple * math.max(
                        constants.batch_max.create_accounts,
                        2 * constants.batch_max.create_transfers,
                    ),
                },
                .ignored = &[_][]const u8{"padding"},
                .derived = .{},
            },
        );

        const TransfersGroove = GrooveType(
            Storage,
            Transfer,
            .{
                .value_count_max = .{
                    .timestamp = config.lsm_batch_multiple * constants.batch_max.create_transfers,
                    .id = config.lsm_batch_multiple * constants.batch_max.create_transfers,
                    .debit_account_id = config.lsm_batch_multiple * constants.batch_max.create_transfers,
                    .credit_account_id = config.lsm_batch_multiple * constants.batch_max.create_transfers,
                    .user_data = config.lsm_batch_multiple * constants.batch_max.create_transfers,
                    .pending_id = config.lsm_batch_multiple * constants.batch_max.create_transfers,
                    .timeout = config.lsm_batch_multiple * constants.batch_max.create_transfers,
                    .ledger = config.lsm_batch_multiple * constants.batch_max.create_transfers,
                    .code = config.lsm_batch_multiple * constants.batch_max.create_transfers,
                    .amount = config.lsm_batch_multiple * constants.batch_max.create_transfers,
                },
                .ignored = &[_][]const u8{ "reserved", "flags" },
                .derived = .{},
            },
        );

        const PostedGroove = @import("lsm/posted_groove.zig").PostedGrooveType(
            Storage,
            config.lsm_batch_multiple * constants.batch_max.create_transfers,
        );

        pub const Workload = WorkloadType(StateMachine);

        pub const Forest = ForestType(Storage, .{
            .accounts_immutable = AccountsImmutableGroove,
            .accounts_mutable = AccountsMutableGroove,
            .transfers = TransfersGroove,
            .posted = PostedGroove,
        });

        pub const Operation = enum(u8) {
            /// Operations reserved by VR protocol (for all state machines):
            reserved = 0,
            root = 1,
            register = 2,

            /// Operations exported by TigerBeetle:
            create_accounts = config.vsr_operations_reserved + 0,
            create_transfers = config.vsr_operations_reserved + 1,
            lookup_accounts = config.vsr_operations_reserved + 2,
            lookup_transfers = config.vsr_operations_reserved + 3,
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
        prefetch_accounts_immutable_context: AccountsImmutableGroove.PrefetchContext = undefined,
        prefetch_accounts_mutable_context: AccountsMutableGroove.PrefetchContext = undefined,
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
                .reserved => unreachable,
                .root => unreachable,
                .register => {},
                .create_accounts => self.prepare_timestamp += mem.bytesAsSlice(Account, input).len,
                .create_transfers => self.prepare_timestamp += mem.bytesAsSlice(Transfer, input).len,
                .lookup_accounts => {},
                .lookup_transfers => {},
            }
            return self.prepare_timestamp;
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

            // NOTE: prefetch(.register)'s callback should end up calling commit()
            // (which is always async) instead of recursing, so this inline callback is fine.
            if (operation == .register) {
                callback(self);
                return;
            }

            tracer.start(
                &self.tracer_slot,
                .state_machine_prefetch,
                @src(),
            );

            self.prefetch_input = input;
            self.prefetch_callback = callback;

            // TODO(Snapshots) Pass in the target snapshot.
            self.forest.grooves.accounts_immutable.prefetch_setup(null);
            self.forest.grooves.accounts_mutable.prefetch_setup(null);
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
                .state_machine_prefetch,
            );

            callback(self);
        }

        fn prefetch_create_accounts(self: *StateMachine, accounts: []const Account) void {
            for (accounts) |*a| {
                self.forest.grooves.accounts_immutable.prefetch_enqueue(a.id);
            }
            self.forest.grooves.accounts_immutable.prefetch(
                prefetch_create_accounts_immutable_callback,
                &self.prefetch_accounts_immutable_context,
            );
        }

        fn prefetch_create_accounts_immutable_callback(
            completion: *AccountsImmutableGroove.PrefetchContext,
        ) void {
            const self = @fieldParentPtr(
                StateMachine,
                "prefetch_accounts_immutable_context",
                completion,
            );

            // Nothing to prefetch_enqueue() from accounts_mutable as accounts_immutable
            // is all that is needed to check for pre-existing accounts before creating one.
            // We still call prefetch() anyway to keep a valid/expected Groove state for commit().

            self.forest.grooves.accounts_mutable.prefetch(
                prefetch_create_accounts_mutable_callback,
                &self.prefetch_accounts_mutable_context,
            );
        }

        fn prefetch_create_accounts_mutable_callback(
            completion: *AccountsMutableGroove.PrefetchContext,
        ) void {
            const self = @fieldParentPtr(
                StateMachine,
                "prefetch_accounts_mutable_context",
                completion,
            );

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
                        self.forest.grooves.accounts_immutable.prefetch_enqueue(p.debit_account_id);
                        self.forest.grooves.accounts_immutable.prefetch_enqueue(p.credit_account_id);
                    }
                } else {
                    self.forest.grooves.accounts_immutable.prefetch_enqueue(t.debit_account_id);
                    self.forest.grooves.accounts_immutable.prefetch_enqueue(t.credit_account_id);
                }
            }

            self.forest.grooves.accounts_immutable.prefetch(
                prefetch_create_transfers_callback_accounts_immutable,
                &self.prefetch_accounts_immutable_context,
            );
        }

        fn prefetch_create_transfers_callback_accounts_immutable(completion: *AccountsImmutableGroove.PrefetchContext) void {
            const self = @fieldParentPtr(StateMachine, "prefetch_accounts_immutable_context", completion);

            const transfers = mem.bytesAsSlice(Event(.create_transfers), self.prefetch_input.?);
            for (transfers) |*t| {
                if (t.flags.post_pending_transfer or t.flags.void_pending_transfer) {
                    if (self.forest.grooves.transfers.get(t.pending_id)) |p| {
                        if (self.forest.grooves.accounts_immutable.get(p.debit_account_id)) |dr_immut| {
                            self.forest.grooves.accounts_mutable.prefetch_enqueue(dr_immut.timestamp);
                        }
                        if (self.forest.grooves.accounts_immutable.get(p.credit_account_id)) |cr_immut| {
                            self.forest.grooves.accounts_mutable.prefetch_enqueue(cr_immut.timestamp);
                        }
                    }
                } else {
                    if (self.forest.grooves.accounts_immutable.get(t.debit_account_id)) |dr_immut| {
                        self.forest.grooves.accounts_mutable.prefetch_enqueue(dr_immut.timestamp);
                    }
                    if (self.forest.grooves.accounts_immutable.get(t.credit_account_id)) |cr_immut| {
                        self.forest.grooves.accounts_mutable.prefetch_enqueue(cr_immut.timestamp);
                    }
                }
            }

            self.forest.grooves.accounts_mutable.prefetch(
                prefetch_create_transfers_callback_accounts_mutable,
                &self.prefetch_accounts_mutable_context,
            );
        }

        fn prefetch_create_transfers_callback_accounts_mutable(completion: *AccountsMutableGroove.PrefetchContext) void {
            const self = @fieldParentPtr(StateMachine, "prefetch_accounts_mutable_context", completion);

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
                self.forest.grooves.accounts_immutable.prefetch_enqueue(id);
            }

            self.forest.grooves.accounts_immutable.prefetch(
                prefetch_lookup_accounts_immutable_callback,
                &self.prefetch_accounts_immutable_context,
            );
        }

        fn prefetch_lookup_accounts_immutable_callback(completion: *AccountsImmutableGroove.PrefetchContext) void {
            const self = @fieldParentPtr(StateMachine, "prefetch_accounts_immutable_context", completion);

            const ids = mem.bytesAsSlice(Event(.lookup_accounts), self.prefetch_input.?);
            for (ids) |id| {
                if (self.forest.grooves.accounts_immutable.get(id)) |immut| {
                    self.forest.grooves.accounts_mutable.prefetch_enqueue(immut.timestamp);
                }
            }

            self.forest.grooves.accounts_mutable.prefetch(
                prefetch_lookup_accounts_mutable_callback,
                &self.prefetch_accounts_mutable_context,
            );
        }

        fn prefetch_lookup_accounts_mutable_callback(completion: *AccountsMutableGroove.PrefetchContext) void {
            const self = @fieldParentPtr(StateMachine, "prefetch_accounts_mutable_context", completion);

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
            timestamp: u64,
            operation: Operation,
            input: []align(16) const u8,
            output: *align(16) [constants.message_body_size_max]u8,
        ) usize {
            _ = client;
            assert(op != 0);
            assert(timestamp > self.commit_timestamp);

            tracer.start(
                &self.tracer_slot,
                .state_machine_commit,
                @src(),
            );

            const result = switch (operation) {
                .root => unreachable,
                .register => 0,
                .create_accounts => self.execute(.create_accounts, timestamp, input, output),
                .create_transfers => self.execute(.create_transfers, timestamp, input, output),
                .lookup_accounts => self.execute_lookup_accounts(input, output),
                .lookup_transfers => self.execute_lookup_transfers(input, output),
                else => unreachable,
            };

            tracer.end(
                &self.tracer_slot,
                .state_machine_commit,
            );

            return result;
        }

        pub fn compact(self: *StateMachine, callback: fn (*StateMachine) void, op: u64) void {
            assert(self.compact_callback == null);
            assert(self.checkpoint_callback == null);

            tracer.start(
                &self.tracer_slot,
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
                .state_machine_compact,
            );

            callback(self);
        }

        pub fn checkpoint(self: *StateMachine, callback: fn (*StateMachine) void, op: u64) void {
            assert(self.compact_callback == null);
            assert(self.checkpoint_callback == null);

            self.checkpoint_callback = callback;
            self.forest.checkpoint(checkpoint_finish, op);
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
            timestamp: u64,
            input: []align(16) const u8,
            output: *align(16) [constants.message_body_size_max]u8,
        ) usize {
            comptime assert(operation != .lookup_accounts and operation != .lookup_transfers);

            const events = mem.bytesAsSlice(Event(operation), input);
            var results = mem.bytesAsSlice(Result(operation), output);
            var count: usize = 0;

            var chain: ?usize = null;
            var chain_broken = false;

            for (events) |*event_, index| {
                var event = event_.*;

                const result = blk: {
                    if (event.flags.linked) {
                        if (chain == null) {
                            chain = index;
                            assert(chain_broken == false);
                        }

                        if (index == events.len - 1) break :blk .linked_event_chain_open;
                    }

                    if (chain_broken) break :blk .linked_event_failed;
                    if (event.timestamp != 0) break :blk .timestamp_must_be_zero;

                    event.timestamp = timestamp - events.len + index + 1;

                    break :blk switch (operation) {
                        .create_accounts => self.create_account(&event),
                        .create_transfers => self.create_transfer(&event),
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
        fn execute_lookup_accounts(
            self: *StateMachine,
            input: []const u8,
            output: *align(16) [constants.message_body_size_max]u8,
        ) usize {
            const batch = mem.bytesAsSlice(u128, input);
            const output_len = @divFloor(output.len, @sizeOf(Account)) * @sizeOf(Account);
            const results = mem.bytesAsSlice(Account, output[0..output_len]);
            var results_count: usize = 0;
            for (batch) |id| {
                if (self.forest.grooves.accounts_immutable.get(id)) |immut| {
                    const mut = self.forest.grooves.accounts_mutable.get(immut.timestamp).?;
                    results[results_count] = into_account(immut, mut);
                    results_count += 1;
                }
            }
            return results_count * @sizeOf(Account);
        }

        // Transfers that do not fit in the response are omitted.
        fn execute_lookup_transfers(
            self: *StateMachine,
            input: []const u8,
            output: *align(16) [constants.message_body_size_max]u8,
        ) usize {
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

            if (a.flags.debits_must_not_exceed_credits and a.flags.credits_must_not_exceed_debits) {
                return .flags_are_mutually_exclusive;
            }

            if (a.ledger == 0) return .ledger_must_not_be_zero;
            if (a.code == 0) return .code_must_not_be_zero;
            if (a.debits_pending != 0) return .debits_pending_must_be_zero;
            if (a.debits_posted != 0) return .debits_posted_must_be_zero;
            if (a.credits_pending != 0) return .credits_pending_must_be_zero;
            if (a.credits_posted != 0) return .credits_posted_must_be_zero;

            if (self.forest.grooves.accounts_immutable.get(a.id)) |e| {
                return create_account_exists(a, e);
            }

            self.forest.grooves.accounts_immutable.put_no_clobber(&AccountImmutable.from_account(a));
            self.forest.grooves.accounts_mutable.put_no_clobber(&AccountMutable.from_account(a));

            self.commit_timestamp = a.timestamp;
            return .ok;
        }

        fn create_account_rollback(self: *StateMachine, a: *const Account) void {
            // Need to get the timestamp from the inserted account rather than the one passed in.
            const timestamp = self.forest.grooves.accounts_immutable.get(a.id).?.timestamp;

            self.forest.grooves.accounts_immutable.remove(a.id);
            self.forest.grooves.accounts_mutable.remove(timestamp);
        }

        fn create_account_exists(a: *const Account, e: *const AccountImmutable) CreateAccountResult {
            assert(a.id == e.id);
            if (@bitCast(u16, a.flags) != @bitCast(u16, e.flags)) return .exists_with_different_flags;
            if (a.user_data != e.user_data) return .exists_with_different_user_data;
            assert(zeroed_48_bytes(a.reserved) and zeroed_16_bytes(e.padding));
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
            if (!t.flags.pending) {
                if (t.timeout != 0) return .timeout_reserved_for_pending_transfer;
            }

            if (t.ledger == 0) return .ledger_must_not_be_zero;
            if (t.code == 0) return .code_must_not_be_zero;
            if (!t.flags.balancing_debit and !t.flags.balancing_credit) {
                if (t.amount == 0) return .amount_must_not_be_zero;
            }

            // The etymology of the DR and CR abbreviations for debit/credit is interesting, either:
            // 1. derived from the Latin past participles of debitum/creditum, i.e. debere/credere,
            // 2. standing for debit record and credit record, or
            // 3. relating to debtor and creditor.
            // We use them to distinguish between `cr` (credit account), and `c` (commit).
            const dr_immut = self.forest.grooves.accounts_immutable.get(t.debit_account_id) orelse return .debit_account_not_found;
            const cr_immut = self.forest.grooves.accounts_immutable.get(t.credit_account_id) orelse return .credit_account_not_found;
            assert(dr_immut.id == t.debit_account_id);
            assert(cr_immut.id == t.credit_account_id);
            assert(t.timestamp > dr_immut.timestamp);
            assert(t.timestamp > cr_immut.timestamp);

            if (dr_immut.ledger != cr_immut.ledger) return .accounts_must_have_the_same_ledger;
            if (t.ledger != dr_immut.ledger) return .transfer_must_have_the_same_ledger_as_accounts;

            // If the transfer already exists, then it must not influence the overflow or limit checks.
            if (self.get_transfer(t.id)) |e| return create_transfer_exists(t, e);

            const dr_mut = self.forest.grooves.accounts_mutable.get(dr_immut.timestamp).?;
            const cr_mut = self.forest.grooves.accounts_mutable.get(cr_immut.timestamp).?;
            assert(dr_mut.timestamp == dr_immut.timestamp);
            assert(cr_mut.timestamp == cr_immut.timestamp);

            const amount = amount: {
                var amount = t.amount;
                if (t.flags.balancing_debit or t.flags.balancing_credit) {
                    if (amount == 0) amount = std.math.maxInt(u64);
                } else {
                    assert(amount != 0);
                }

                if (t.flags.balancing_debit) {
                    const dr_balance = dr_mut.debits_posted + dr_mut.debits_pending;
                    amount = std.math.min(amount, dr_mut.credits_posted -| dr_balance);
                    if (amount == 0) return .exceeds_credits;
                }

                if (t.flags.balancing_credit) {
                    const cr_balance = cr_mut.credits_posted + cr_mut.credits_pending;
                    amount = std.math.min(amount, cr_mut.debits_posted -| cr_balance);
                    if (amount == 0) return .exceeds_debits;
                }
                break :amount amount;
            };

            if (t.flags.pending) {
                if (sum_overflows(amount, dr_mut.debits_pending)) return .overflows_debits_pending;
                if (sum_overflows(amount, cr_mut.credits_pending)) return .overflows_credits_pending;
            }
            if (sum_overflows(amount, dr_mut.debits_posted)) return .overflows_debits_posted;
            if (sum_overflows(amount, cr_mut.credits_posted)) return .overflows_credits_posted;
            // We assert that the sum of the pending and posted balances can never overflow:
            if (sum_overflows(amount, dr_mut.debits_pending + dr_mut.debits_posted)) {
                return .overflows_debits;
            }
            if (sum_overflows(amount, cr_mut.credits_pending + cr_mut.credits_posted)) {
                return .overflows_credits;
            }
            if (sum_overflows(t.timestamp, t.timeout)) return .overflows_timeout;

            if (into_account(dr_immut, dr_mut).debits_exceed_credits(amount)) return .exceeds_credits;
            if (into_account(cr_immut, cr_mut).credits_exceed_debits(amount)) return .exceeds_debits;

            var t2 = t.*;
            t2.amount = amount;
            self.forest.grooves.transfers.put_no_clobber(&t2);

            var dr_mut_new = dr_mut.*;
            var cr_mut_new = cr_mut.*;
            if (t.flags.pending) {
                dr_mut_new.debits_pending += amount;
                cr_mut_new.credits_pending += amount;
            } else {
                dr_mut_new.debits_posted += amount;
                cr_mut_new.credits_posted += amount;
            }
            self.forest.grooves.accounts_mutable.put(&dr_mut_new);
            self.forest.grooves.accounts_mutable.put(&cr_mut_new);

            self.commit_timestamp = t.timestamp;
            return .ok;
        }

        fn create_transfer_rollback(self: *StateMachine, t: *const Transfer) void {
            if (t.flags.post_pending_transfer or t.flags.void_pending_transfer) {
                return self.post_or_void_pending_transfer_rollback(t);
            }

            const dr_immut = self.forest.grooves.accounts_immutable.get(t.debit_account_id).?;
            const cr_immut = self.forest.grooves.accounts_immutable.get(t.credit_account_id).?;
            assert(dr_immut.id == t.debit_account_id);
            assert(cr_immut.id == t.credit_account_id);

            var dr_mut = self.forest.grooves.accounts_mutable.get(dr_immut.timestamp).?.*;
            var cr_mut = self.forest.grooves.accounts_mutable.get(cr_immut.timestamp).?.*;
            assert(dr_mut.timestamp == dr_immut.timestamp);
            assert(cr_mut.timestamp == cr_immut.timestamp);

            if (t.flags.pending) {
                dr_mut.debits_pending -= t.amount;
                cr_mut.credits_pending -= t.amount;
            } else {
                dr_mut.debits_posted -= t.amount;
                cr_mut.credits_posted -= t.amount;
            }
            self.forest.grooves.accounts_mutable.put(&dr_mut);
            self.forest.grooves.accounts_mutable.put(&cr_mut);

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
                return .flags_are_mutually_exclusive;
            }
            if (t.flags.pending) return .flags_are_mutually_exclusive;
            if (t.flags.balancing_debit) return .flags_are_mutually_exclusive;
            if (t.flags.balancing_credit) return .flags_are_mutually_exclusive;

            if (t.pending_id == 0) return .pending_id_must_not_be_zero;
            if (t.pending_id == math.maxInt(u128)) return .pending_id_must_not_be_int_max;
            if (t.pending_id == t.id) return .pending_id_must_be_different;
            if (t.timeout != 0) return .timeout_reserved_for_pending_transfer;

            const p = self.get_transfer(t.pending_id) orelse return .pending_transfer_not_found;
            assert(p.id == t.pending_id);
            if (!p.flags.pending) return .pending_transfer_not_pending;

            const dr_immut = self.forest.grooves.accounts_immutable.get(p.debit_account_id).?;
            const cr_immut = self.forest.grooves.accounts_immutable.get(p.credit_account_id).?;
            assert(dr_immut.id == p.debit_account_id);
            assert(cr_immut.id == p.credit_account_id);
            assert(p.timestamp > dr_immut.timestamp);
            assert(p.timestamp > cr_immut.timestamp);
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
            if (p.timeout > 0) {
                if (p.timestamp + p.timeout <= t.timestamp) return .pending_transfer_expired;
            }

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

            var dr_mut_new = self.forest.grooves.accounts_mutable.get(dr_immut.timestamp).?.*;
            var cr_mut_new = self.forest.grooves.accounts_mutable.get(cr_immut.timestamp).?.*;
            assert(dr_mut_new.timestamp == dr_immut.timestamp);
            assert(cr_mut_new.timestamp == cr_immut.timestamp);

            dr_mut_new.debits_pending -= p.amount;
            cr_mut_new.credits_pending -= p.amount;

            if (t.flags.post_pending_transfer) {
                assert(amount > 0);
                assert(amount <= p.amount);
                dr_mut_new.debits_posted += amount;
                cr_mut_new.credits_posted += amount;
            }

            self.forest.grooves.accounts_mutable.put(&dr_mut_new);
            self.forest.grooves.accounts_mutable.put(&cr_mut_new);

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

            const dr_immut = self.forest.grooves.accounts_immutable.get(p.debit_account_id).?;
            const cr_immut = self.forest.grooves.accounts_immutable.get(p.credit_account_id).?;
            assert(dr_immut.id == p.debit_account_id);
            assert(cr_immut.id == p.credit_account_id);

            var dr_mut = self.forest.grooves.accounts_mutable.get(dr_immut.timestamp).?.*;
            var cr_mut = self.forest.grooves.accounts_mutable.get(cr_immut.timestamp).?.*;
            assert(dr_mut.timestamp == dr_immut.timestamp);
            assert(cr_mut.timestamp == cr_immut.timestamp);

            if (t.flags.post_pending_transfer) {
                const amount = if (t.amount > 0) t.amount else p.amount;
                assert(amount > 0);
                assert(amount <= p.amount);
                dr_mut.debits_posted -= amount;
                cr_mut.credits_posted -= amount;
            }
            dr_mut.debits_pending += p.amount;
            cr_mut.credits_pending += p.amount;

            self.forest.grooves.accounts_mutable.put(&dr_mut);
            self.forest.grooves.accounts_mutable.put(&cr_mut);

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
                .accounts_immutable = .{
                    .prefetch_entries_max = std.math.max(
                        // create_account()/lookup_account() looks up 1 AccountImmutable per item.
                        batch_accounts_max,
                        // create_transfer()/post_or_void_pending_transfer() looks up 2
                        // AccountImmutables for every transfer.
                        2 * batch_transfers_max,
                    ),
                    .tree_options_object = .{
                        .cache_entries_max = options.cache_entries_accounts,
                    },
                    .tree_options_id = .{
                        .cache_entries_max = options.cache_entries_accounts,
                    },
                    .tree_options_index = .{
                        .user_data = .{},
                        .ledger = .{},
                        .code = .{},
                    },
                },
                .accounts_mutable = .{
                    .prefetch_entries_max = std.math.max(
                        // create_account()/lookup_account() looks up 1 AccountMutable per item.
                        batch_accounts_max,
                        // create_transfer()/post_or_void_pending_transfer() looks up 2
                        // AccountMutables for every transfer.
                        2 * batch_transfers_max,
                    ),
                    .tree_options_object = .{
                        .cache_entries_max = options.cache_entries_accounts,
                    },
                    .tree_options_id = {}, // No ID tree at there's one already for AccountsMutable.
                    .tree_options_index = .{
                        .debits_pending = .{},
                        .debits_posted = .{},
                        .credits_pending = .{},
                        .credits_posted = .{},
                    },
                },
                .transfers = .{
                    // *2 to fetch pending and post/void transfer.
                    .prefetch_entries_max = 2 * batch_transfers_max,
                    .tree_options_object = .{
                        .cache_entries_max = options.cache_entries_transfers,
                    },
                    .tree_options_id = .{
                        .cache_entries_max = options.cache_entries_transfers,
                    },
                    .tree_options_index = .{
                        .debit_account_id = .{},
                        .credit_account_id = .{},
                        .user_data = .{},
                        .pending_id = .{},
                        .timeout = .{},
                        .ledger = .{},
                        .code = .{},
                        .amount = .{},
                    },
                },
                .posted = .{
                    .cache_entries_max = options.cache_entries_posted,
                    .prefetch_entries_max = batch_transfers_max,
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
fn zeroed_16_bytes(a: [16]u8) bool {
    const x = @bitCast([2]u64, a);
    return (x[0] | x[1]) == 0;
}

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
    const Storage = @import("testing/storage.zig").Storage;
    const MessagePool = @import("message_pool.zig").MessagePool;
    const data_file_size_min = @import("vsr/superblock.zig").data_file_size_min;
    const SuperBlock = @import("vsr/superblock.zig").SuperBlockType(Storage);
    const Grid = @import("lsm/grid.zig").GridType(Storage);
    const StateMachine = StateMachineType(Storage, .{
        // Overestimate the batch size because the test never compacts.
        .message_body_size_max = TestContext.message_body_size_max,
        .lsm_batch_multiple = 1,
        .vsr_operations_reserved = 128,
    });
    const message_body_size_max = 32 * @sizeOf(Account);

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

        ctx.superblock = try SuperBlock.init(allocator, .{
            .storage = &ctx.storage,
            .storage_size_limit = data_file_size_min,
            .message_pool = &ctx.message_pool,
        });
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
    lookup_transfer: struct {
        id: u128,
        data: union(enum) {
            exists: bool,
            amount: u64,
        },
    },
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
    timestamp: u64 = 0,
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
            .timestamp = a.timestamp,
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
    flags_balancing_debit: ?enum { BDR } = null,
    flags_balancing_credit: ?enum { BCR } = null,
    flags_padding: u10 = 0,
    amount: u64 = 0,
    timestamp: u64 = 0,
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
                .balancing_debit = t.flags_balancing_debit != null,
                .balancing_credit = t.flags_balancing_credit != null,
                .padding = t.flags_padding,
            },
            .amount = t.amount,
            .timestamp = t.timestamp,
        };
    }
};

fn check(comptime test_table: []const u8) !void {
    // TODO(Zig): Disabled because of spurious failure (segfault) on MacOS at the
    // `test_actions.constSlice()`. Most likely a comptime bug that will be resolved by 0.10.
    if (@import("builtin").os.tag == .macos) return;

    const parse_table = @import("testing/table.zig").parse;
    const allocator = std.testing.allocator;

    var context: TestContext = undefined;
    try context.init(allocator);
    defer context.deinit(allocator);

    var accounts = std.AutoHashMap(u128, Account).init(allocator);
    defer accounts.deinit();

    var transfers = std.AutoHashMap(u128, Transfer).init(allocator);
    defer transfers.deinit();

    var request = std.ArrayListAligned(u8, 16).init(allocator);
    defer request.deinit();

    var reply = std.ArrayListAligned(u8, 16).init(allocator);
    defer reply.deinit();

    var operation: ?TestContext.StateMachine.Operation = null;

    const test_actions = parse_table(TestAction, test_table);
    for (test_actions.constSlice()) |test_action| {
        switch (test_action) {
            .setup => |b| {
                assert(operation == null);

                const immut = context.state_machine.forest.grooves.accounts_immutable.get(b.account).?;
                var mut = context.state_machine.forest.grooves.accounts_mutable.get(immut.timestamp).?.*;
                mut.debits_pending = b.debits_pending;
                mut.debits_posted = b.debits_posted;
                mut.credits_pending = b.credits_pending;
                mut.credits_posted = b.credits_posted;
                context.state_machine.forest.grooves.accounts_mutable.put(&mut);
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
                switch (t.data) {
                    .exists => |exists| {
                        if (exists) {
                            var transfer = transfers.get(t.id).?;
                            try reply.appendSlice(std.mem.asBytes(&transfer));
                        }
                    },
                    .amount => |amount| {
                        var transfer = transfers.get(t.id).?;
                        transfer.amount = amount;
                        try reply.appendSlice(std.mem.asBytes(&transfer));
                    },
                }
            },

            .commit => |commit_operation| {
                assert(operation == null or operation.? == commit_operation);

                context.state_machine.prepare_timestamp += 1;
                const timestamp = context.state_machine.prepare(commit_operation, request.items);

                const reply_actual_buffer = try allocator.alignedAlloc(u8, 16, 4096);
                defer allocator.free(reply_actual_buffer);

                const reply_actual_size = context.state_machine.commit(
                    0,
                    1,
                    timestamp,
                    commit_operation,
                    request.items,
                    reply_actual_buffer[0..TestContext.message_body_size_max],
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
            for (results) |result, i| {
                std.debug.print("{s}[{}]={}\n", .{ label, i, result });
            }
            return;
        }
    }
    unreachable;
}

test "create_accounts" {
    try check(
        \\ account A1 U2 _ L3 C4 _   _   _ _  0  0  0  0 _ ok
        \\ account A0  _ 1 L0 C0 _ D<C C<D 1  1  1  1  1 1 timestamp_must_be_zero
        \\ account A0  _ 1 L0 C0 _ D<C C<D 1  1  1  1  1 _ reserved_flag
        \\ account A0  _ 1 L0 C0 _ D<C C<D _  1  1  1  1 _ reserved_field
        \\ account A0  _ _ L0 C0 _ D<C C<D _  1  1  1  1 _ id_must_not_be_zero
        \\ account -0  _ _ L0 C0 _ D<C C<D _  1  1  1  1 _ id_must_not_be_int_max
        \\ account A1 U1 _ L0 C0 _ D<C C<D _  1  1  1  1 _ flags_are_mutually_exclusive
        \\ account A1 U1 _ L0 C0 _ D<C   _ _  1  1  1  1 _ ledger_must_not_be_zero
        \\ account A1 U1 _ L9 C0 _ D<C   _ _  1  1  1  1 _ code_must_not_be_zero
        \\ account A1 U1 _ L9 C9 _ D<C   _ _  1  1  1  1 _ debits_pending_must_be_zero
        \\ account A1 U1 _ L9 C9 _ D<C   _ _  0  1  1  1 _ debits_posted_must_be_zero
        \\ account A1 U1 _ L9 C9 _ D<C   _ _  0  0  1  1 _ credits_pending_must_be_zero
        \\ account A1 U1 _ L9 C9 _ D<C   _ _  0  0  0  1 _ credits_posted_must_be_zero
        \\ account A1 U1 _ L9 C9 _ D<C   _ _  0  0  0  0 _ exists_with_different_flags
        \\ account A1 U1 _ L9 C9 _   _ C<D _  0  0  0  0 _ exists_with_different_flags
        \\ account A1 U1 _ L9 C9 _   _   _ _  0  0  0  0 _ exists_with_different_user_data
        \\ account A1 U2 _ L9 C9 _   _   _ _  0  0  0  0 _ exists_with_different_ledger
        \\ account A1 U2 _ L3 C9 _   _   _ _  0  0  0  0 _ exists_with_different_code
        \\ account A1 U2 _ L3 C4 _   _   _ _  0  0  0  0 _ exists
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
        \\ account A7 _ _ L1 C1   _ _ _ _ 0 0 0 0 _ ok // An individual event (successful):

        // A chain of 4 events (the last event in the chain closes the chain with linked=false):
        \\ account A1 _ _ L1 C1 LNK _ _ _ 0 0 0 0 _ linked_event_failed // Commit/rollback.
        \\ account A2 _ _ L1 C1 LNK _ _ _ 0 0 0 0 _ linked_event_failed // Commit/rollback.
        \\ account A1 _ _ L1 C1 LNK _ _ _ 0 0 0 0 _ exists              // Fail with .exists.
        \\ account A3 _ _ L1 C1   _ _ _ _ 0 0 0 0 _ linked_event_failed // Fail without committing.

        // An individual event (successful):
        // This does not see any effect from the failed chain above.
        \\ account A1 _ _ L1 C1   _ _ _ _ 0 0 0 0 _ ok

        // A chain of 2 events (the first event fails the chain):
        \\ account A1 _ _ L1 C2 LNK _ _ _ 0 0 0 0 _ exists_with_different_flags
        \\ account A2 _ _ L1 C1   _ _ _ _ 0 0 0 0 _ linked_event_failed

        // An individual event (successful):
        \\ account A2 _ _ L1 C1   _ _ _ _ 0 0 0 0 _ ok

        // A chain of 2 events (the last event fails the chain):
        \\ account A3 _ _ L1 C1 LNK _ _ _ 0 0 0 0 _ linked_event_failed
        \\ account A1 _ _ L2 C1   _ _ _ _ 0 0 0 0 _ exists_with_different_ledger

        // A chain of 2 events (successful):
        \\ account A3 _ _ L1 C1 LNK _ _ _ 0 0 0 0 _ ok
        \\ account A4 _ _ L1 C1   _ _ _ _ 0 0 0 0 _ ok
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
        \\ account A7 _ _ L1 C1   _ _ _ _ 0 0 0 0 _ ok // An individual event (successful):

        // A chain of 4 events:
        \\ account A1 _ _ L1 C1 LNK _ _ _ 0 0 0 0 _ linked_event_failed // Commit/rollback.
        \\ account A2 _ _ L1 C1 LNK _ _ _ 0 0 0 0 _ linked_event_failed // Commit/rollback.
        \\ account A1 _ _ L1 C1 LNK _ _ _ 0 0 0 0 _ exists              // Fail with .exists.
        \\ account A3 _ _ L1 C1   _ _ _ _ 0 0 0 0 _ linked_event_failed // Fail without committing.
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
        \\ account A1 _ _ L1 C1 LNK _ _ _ 0 0 0 0 _ ok
        \\ account A2 _ _ L1 C1 LNK _ _ _ 0 0 0 0 _ ok
        \\ account A3 _ _ L1 C1   _ _ _ _ 0 0 0 0 _ ok

        // An open chain of 2 events:
        \\ account A4 _ _ L1 C1 LNK _ _ _ 0 0 0 0 _ linked_event_failed
        \\ account A5 _ _ L1 C1 LNK _ _ _ 0 0 0 0 _ linked_event_chain_open
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
        \\ account A1 _ _ L1 C1   _ _ _ _ 0 0 0 0 _ ok

        // An open chain of 3 events (the second one fails):
        \\ account A2 _ _ L1 C1 LNK _ _ _ 0 0 0 0 _ linked_event_failed
        \\ account A1 _ _ L1 C1 LNK _ _ _ 0 0 0 0 _ exists_with_different_flags
        \\ account A3 _ _ L1 C1 LNK _ _ _ 0 0 0 0 _ linked_event_chain_open
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
        \\ account A1 _ _ L1 C1 LNK _ _ _ 0 0 0 0 _ linked_event_chain_open
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
        \\ account A1 _ _ L1 C1 _   _   _ _ 0 0 0 0 _ ok
        \\ account A2 _ _ L2 C2 _   _   _ _ 0 0 0 0 _ ok
        \\ account A3 _ _ L1 C1 _   _   _ _ 0 0 0 0 _ ok
        \\ account A4 _ _ L1 C1 _ D<C   _ _ 0 0 0 0 _ ok
        \\ account A5 _ _ L1 C1 _   _ C<D _ 0 0 0 0 _ ok
        \\ commit create_accounts

        // Set up initial balances.
        \\ setup A1  100   200    0     0
        \\ setup A2    0     0    0     0
        \\ setup A3    0     0  110   210
        \\ setup A4   20  -700    0  -500
        \\ setup A5    0 -1000   10 -1100

        // Test errors by descending precedence.
        \\ transfer T0 A0 A0  _ R1 T1   _ L0 C0 _ PEN _ _ _ _ P1    0 1 timestamp_must_be_zero
        \\ transfer T0 A0 A0  _ R1 T1   _ L0 C0 _ PEN _ _ _ _ P1    0 _ reserved_flag
        \\ transfer T0 A0 A0  _ R1 T1   _ L0 C0 _ PEN _ _ _ _  _    0 _ reserved_field
        \\ transfer T0 A0 A0  _  _ T1   _ L0 C0 _ PEN _ _ _ _  _    0 _ id_must_not_be_zero
        \\ transfer -0 A0 A0  _  _ T1   _ L0 C0 _ PEN _ _ _ _  _    0 _ id_must_not_be_int_max
        \\ transfer T1 A0 A0  _  _ T1   _ L0 C0 _ PEN _ _ _ _  _    0 _ debit_account_id_must_not_be_zero
        \\ transfer T1 -0 A0  _  _ T1   _ L0 C0 _ PEN _ _ _ _  _    0 _ debit_account_id_must_not_be_int_max
        \\ transfer T1 A8 A0  _  _ T1   _ L0 C0 _ PEN _ _ _ _  _    0 _ credit_account_id_must_not_be_zero
        \\ transfer T1 A8 -0  _  _ T1   _ L0 C0 _ PEN _ _ _ _  _    0 _ credit_account_id_must_not_be_int_max
        \\ transfer T1 A8 A8  _  _ T1   _ L0 C0 _ PEN _ _ _ _  _    0 _ accounts_must_be_different
        \\ transfer T1 A8 A9  _  _ T1   _ L0 C0 _ PEN _ _ _ _  _    0 _ pending_id_must_be_zero
        \\ transfer T1 A8 A9  _  _  _  -0 L0 C0 _   _ _ _ _ _  _    0 _ timeout_reserved_for_pending_transfer
        \\ transfer T1 A8 A9  _  _  _  -0 L0 C0 _ PEN _ _ _ _  _    0 _ ledger_must_not_be_zero
        \\ transfer T1 A8 A9  _  _  _  -0 L9 C0 _ PEN _ _ _ _  _    0 _ code_must_not_be_zero
        \\ transfer T1 A8 A9  _  _  _  -0 L9 C1 _ PEN _ _ _ _  _    0 _ amount_must_not_be_zero
        \\ transfer T1 A8 A9  _  _  _  -0 L9 C1 _ PEN _ _ _ _  _    9 _ debit_account_not_found
        \\ transfer T1 A1 A9  _  _  _  -0 L9 C1 _ PEN _ _ _ _  _    9 _ credit_account_not_found
        \\ transfer T1 A1 A2  _  _  _  -0 L9 C1 _ PEN _ _ _ _  _    1 _ accounts_must_have_the_same_ledger
        \\ transfer T1 A1 A3  _  _  _  -0 L9 C1 _ PEN _ _ _ _  _    1 _ transfer_must_have_the_same_ledger_as_accounts
        \\ transfer T1 A1 A3  _  _  _  -0 L1 C1 _ PEN _ _ _ _  _  -99 _ overflows_debits_pending  // amount = max - A1.debits_pending + 1
        \\ transfer T1 A1 A3  _  _  _  -0 L1 C1 _ PEN _ _ _ _  _ -109 _ overflows_credits_pending // amount = max - A3.credits_pending + 1
        \\ transfer T1 A1 A3  _  _  _  -0 L1 C1 _ PEN _ _ _ _  _ -199 _ overflows_debits_posted   // amount = max - A1.debits_posted + 1
        \\ transfer T1 A1 A3  _  _  _  -0 L1 C1 _ PEN _ _ _ _  _ -209 _ overflows_credits_posted  // amount = max - A3.credits_posted + 1
        \\ transfer T1 A1 A3  _  _  _  -0 L1 C1 _ PEN _ _ _ _  _ -299 _ overflows_debits          // amount = max - A1.debits_pending - A1.debits_posted + 1
        \\ transfer T1 A1 A3  _  _  _  -0 L1 C1 _ PEN _ _ _ _  _ -319 _ overflows_credits         // amount = max - A3.credits_pending - A3.credits_posted + 1
        \\ transfer T1 A4 A5  _  _  _  -0 L1 C1 _ PEN _ _ _ _  _  199 _ overflows_timeout         // amount = A4.credits_posted - A4.debits_pending - A4.debits_posted + 1
        \\ transfer T1 A4 A5  _  _  _   _ L1 C1 _   _ _ _ _ _  _  199 _ exceeds_credits           // amount = A4.credits_posted - A4.debits_pending - A4.debits_posted + 1
        \\ transfer T1 A4 A5  _  _  _   _ L1 C1 _   _ _ _ _ _  _   91 _ exceeds_debits            // amount = A5.debits_posted - A5.credits_pending - A5.credits_posted + 1
        \\ transfer T1 A1 A3  _  _  _ 999 L1 C1 _ PEN _ _ _ _  _  123 _ ok

        // Ensure that idempotence is only checked after validation.
        \\ transfer T1 A1 A3  _  _  _ 999 L2 C1 _ PEN _ _ _ _  _  123 _ transfer_must_have_the_same_ledger_as_accounts
        \\ transfer T1 A1 A3 U1  _  _   _ L1 C2 _   _ _ _ _ _  _   -0 _ exists_with_different_flags
        \\ transfer T1 A3 A1 U1  _  _ 999 L1 C2 _ PEN _ _ _ _  _   -0 _ exists_with_different_debit_account_id
        \\ transfer T1 A1 A4 U1  _  _ 999 L1 C2 _ PEN _ _ _ _  _   -0 _ exists_with_different_credit_account_id
        \\ transfer T1 A1 A3 U1  _  _ 999 L1 C2 _ PEN _ _ _ _  _   -0 _ exists_with_different_user_data
        \\ transfer T1 A1 A3  _  _  _ 998 L1 C2 _ PEN _ _ _ _  _   -0 _ exists_with_different_timeout
        \\ transfer T1 A1 A3  _  _  _ 999 L1 C2 _ PEN _ _ _ _  _   -0 _ exists_with_different_code
        \\ transfer T1 A1 A3  _  _  _ 999 L1 C1 _ PEN _ _ _ _  _   -0 _ exists_with_different_amount
        \\ transfer T1 A1 A3  _  _  _ 999 L1 C1 _ PEN _ _ _ _  _  123 _ exists
        \\ transfer T2 A3 A1  _  _  _   _ L1 C2 _   _ _ _ _ _  _    7 _ ok
        \\ transfer T3 A1 A3  _  _  _   _ L1 C2 _   _ _ _ _ _  _    3 _ ok
        \\ commit create_transfers
        \\
        \\ lookup_account A1 223 203   0   7
        \\ lookup_account A3   0   7 233 213
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T1 exists true
        \\ lookup_transfer T2 exists true
        \\ lookup_transfer T3 exists true
        \\ lookup_transfer -0 exists false
        \\ commit lookup_transfers
    );
}

test "create/lookup 2-phase transfers" {
    try check(
        \\ account A1 _ _ L1 C1   _  _  _  _ 0 0 0 0 _ ok
        \\ account A2 _ _ L1 C1   _  _  _  _ 0 0 0 0 _ ok
        \\ commit create_accounts

        // First phase.
        \\ transfer   T1 A1 A2  _ _   _    _ L1 C1 _   _   _   _  _  _ _ 15 _ ok // Not pending!
        \\ transfer   T2 A1 A2  _ _   _ 1000 L1 C1 _ PEN   _   _  _  _ _ 15 _ ok
        \\ transfer   T3 A1 A2  _ _   _   50 L1 C1 _ PEN   _   _  _  _ _ 15 _ ok
        \\ transfer   T4 A1 A2  _ _   _    1 L1 C1 _ PEN   _   _  _  _ _ 15 _ ok
        \\ transfer   T5 A1 A2 U9 _   _   50 L1 C1 _ PEN   _   _  _  _ _  7 _ ok
        \\ transfer   T6 A1 A2  _ _   _    0 L1 C1 _ PEN   _   _  _  _ _  1 _ ok
        \\ commit create_transfers

        // Check balances before resolving.
        \\ lookup_account A1 53 15  0  0
        \\ lookup_account A2  0  0 53 15
        \\ commit lookup_accounts

        // Second phase.
        \\ transfer T101 A1 A2 U1 _  T2    _ L1 C1 _   _ POS   _   _   _ _ 13 _ ok
        \\ transfer   T0 A8 A9 U2 _  T0   50 L6 C7 _ PEN POS VOI   _   _ _ 16 1 timestamp_must_be_zero
        \\ transfer   T0 A8 A9 U2 _  T0   50 L6 C7 _ PEN POS VOI   _   _ _ 16 _ id_must_not_be_zero
        \\ transfer   -0 A8 A9 U2 _  T0   50 L6 C7 _ PEN POS VOI   _   _ _ 16 _ id_must_not_be_int_max
        \\ transfer T101 A8 A9 U2 _  T0   50 L6 C7 _ PEN POS VOI   _   _ _ 16 _ flags_are_mutually_exclusive
        \\ transfer T101 A8 A9 U2 _  T0   50 L6 C7 _ PEN POS VOI BDR   _ _ 16 _ flags_are_mutually_exclusive
        \\ transfer T101 A8 A9 U2 _  T0   50 L6 C7 _ PEN POS VOI BDR BCR _ 16 _ flags_are_mutually_exclusive
        \\ transfer T101 A8 A9 U2 _  T0   50 L6 C7 _ PEN POS VOI   _ BCR _ 16 _ flags_are_mutually_exclusive
        \\ transfer T101 A8 A9 U2 _  T0   50 L6 C7 _ PEN   _ VOI   _   _ _ 16 _ flags_are_mutually_exclusive
        \\ transfer T101 A8 A9 U2 _  T0   50 L6 C7 _   _   _ VOI BDR   _ _ 16 _ flags_are_mutually_exclusive
        \\ transfer T101 A8 A9 U2 _  T0   50 L6 C7 _   _   _ VOI BDR BCR _ 16 _ flags_are_mutually_exclusive
        \\ transfer T101 A8 A9 U2 _  T0   50 L6 C7 _   _   _ VOI   _ BCR _ 16 _ flags_are_mutually_exclusive
        \\ transfer T101 A8 A9 U2 _  T0   50 L6 C7 _   _ POS   _ BDR   _ _ 16 _ flags_are_mutually_exclusive
        \\ transfer T101 A8 A9 U2 _  T0   50 L6 C7 _   _ POS   _ BDR BCR _ 16 _ flags_are_mutually_exclusive
        \\ transfer T101 A8 A9 U2 _  T0   50 L6 C7 _   _ POS   _   _ BCR _ 16 _ flags_are_mutually_exclusive
        \\ transfer T101 A8 A9 U2 _  T0   50 L6 C7 _   _   _ VOI   _   _ _ 16 _ pending_id_must_not_be_zero
        \\ transfer T101 A8 A9 U2 _  -0   50 L6 C7 _   _   _ VOI   _   _ _ 16 _ pending_id_must_not_be_int_max
        \\ transfer T101 A8 A9 U2 _ 101   50 L6 C7 _   _   _ VOI   _   _ _ 16 _ pending_id_must_be_different
        \\ transfer T101 A8 A9 U2 _ 102   50 L6 C7 _   _   _ VOI   _   _ _ 16 _ timeout_reserved_for_pending_transfer
        \\ transfer T101 A8 A9 U2 _ 102    _ L6 C7 _   _   _ VOI   _   _ _ 16 _ pending_transfer_not_found
        \\ transfer T101 A8 A9 U2 _  T1    _ L6 C7 _   _   _ VOI   _   _ _ 16 _ pending_transfer_not_pending
        \\ transfer T101 A8 A9 U2 _  T3    _ L6 C7 _   _   _ VOI   _   _ _ 16 _ pending_transfer_has_different_debit_account_id
        \\ transfer T101 A1 A9 U2 _  T3    _ L6 C7 _   _   _ VOI   _   _ _ 16 _ pending_transfer_has_different_credit_account_id
        \\ transfer T101 A1 A2 U2 _  T3    _ L6 C7 _   _   _ VOI   _   _ _ 16 _ pending_transfer_has_different_ledger
        \\ transfer T101 A1 A2 U2 _  T3    _ L1 C7 _   _   _ VOI   _   _ _ 16 _ pending_transfer_has_different_code
        \\ transfer T101 A1 A2 U2 _  T3    _ L1 C1 _   _   _ VOI   _   _ _ 16 _ exceeds_pending_transfer_amount
        \\ transfer T101 A1 A2 U2 _  T3    _ L1 C1 _   _   _ VOI   _   _ _ 14 _ pending_transfer_has_different_amount
        \\ transfer T101 A1 A2 U2 _  T3    _ L1 C1 _   _   _ VOI   _   _ _ 15 _ exists_with_different_flags
        \\ transfer T101 A1 A2 U2 _  T3    _ L1 C1 _   _ POS   _   _   _ _ 14 _ exists_with_different_pending_id
        \\ transfer T101 A1 A2 U2 _  T2    _ L1 C1 _   _ POS   _   _   _ _ 14 _ exists_with_different_user_data
        \\ transfer T101 A1 A2 U0 _  T2    _ L1 C1 _   _ POS   _   _   _ _ 14 _ exists_with_different_user_data
        \\ transfer T101 A1 A2 U1 _  T2    _ L1 C1 _   _ POS   _   _   _ _ 14 _ exists_with_different_amount
        \\ transfer T101 A1 A2 U1 _  T2    _ L1 C1 _   _ POS   _   _   _ _  _ _ exists_with_different_amount
        \\ transfer T101 A1 A2 U1 _  T2    _ L1 C1 _   _ POS   _   _   _ _ 13 _ exists
        \\ transfer T102 A1 A2 U1 _  T2    _ L1 C1 _   _ POS   _   _   _ _ 13 _ pending_transfer_already_posted
        \\ transfer T103 A1 A2 U1 _  T3    _ L1 C1 _   _   _ VOI   _   _ _ 15 _ ok
        \\ transfer T102 A1 A2 U1 _  T3    _ L1 C1 _   _ POS   _   _   _ _ 13 _ pending_transfer_already_voided
        \\ transfer T102 A1 A2 U1 _  T4    _ L1 C1 _   _   _ VOI   _   _ _ 15 _ pending_transfer_expired
        \\ transfer T105 A0 A0 U0 _  T5    _ L0 C0 _   _ POS   _   _   _ _  _ _ ok
        \\ transfer T106 A0 A0 U0 _  T6    _ L1 C1 _   _ POS   _   _   _ _  0 _ ok
        \\ commit create_transfers

        // Check balances after resolving.
        \\ lookup_account A1 15 36  0  0
        \\ lookup_account A2  0  0 15 36
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
        \\ account A1 _ _ L1 C1   _  _  _  _ 0 0 0 0 _ ok
        \\ account A2 _ _ L1 C1   _  _  _  _ 0 0 0 0 _ ok
        \\ commit create_accounts
        \\
        \\ transfer T1 A1 A2  _ _   _ _ L1 C1 _ _ _ _ _ _ _ 15 _ ok
        \\ transfer T2 A1 A2  _ _   _ _ L0 C1 _ _ _ _ _ _ _ 15 _ ledger_must_not_be_zero
        \\ commit create_transfers
        \\
        \\ lookup_account A1 0 15 0  0
        \\ lookup_account A2 0  0 0 15
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T1 exists true
        \\ lookup_transfer T2 exists false
        \\ commit lookup_transfers
    );
}

test "create_transfers: failed linked-chains are undone" {
    try check(
        \\ account A1 _ _ L1 C1   _  _  _  _ 0 0 0 0 _ ok
        \\ account A2 _ _ L1 C1   _  _  _  _ 0 0 0 0 _ ok
        \\ commit create_accounts
        \\
        \\ transfer T1 A1 A2  _ _   _ _ L1 C1 LNK   _ _ _ _ _ _ 15 _ linked_event_failed
        \\ transfer T2 A1 A2  _ _   _ _ L0 C1   _   _ _ _ _ _ _ 15 _ ledger_must_not_be_zero
        \\ commit create_transfers
        \\
        \\ transfer T3 A1 A2  _ _   _ 1 L1 C1 LNK PEN _ _ _ _ _ 15 _ linked_event_failed
        \\ transfer T4 A1 A2  _ _   _ _ L0 C1   _   _ _ _ _ _ _ 15 _ ledger_must_not_be_zero
        \\ commit create_transfers
        \\
        \\ lookup_account A1 0 0 0 0
        \\ lookup_account A2 0 0 0 0
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T1 exists false
        \\ lookup_transfer T2 exists false
        \\ lookup_transfer T3 exists false
        \\ lookup_transfer T4 exists false
        \\ commit lookup_transfers
    );
}

test "create_transfers: failed linked-chains are undone within a commit" {
    try check(
        \\ account A1 _ _ L1 C1 _ D<C _ _ 0 0 0 0 _ ok
        \\ account A2 _ _ L1 C1 _   _ _ _ 0 0 0 0 _ ok
        \\ commit create_accounts
        \\
        \\ setup A1 0 0 0 20
        \\
        \\ transfer T1 A1 A2  _ _   _ _ L1 C1 LNK _ _ _ _ _ _ 15 _ linked_event_failed
        \\ transfer T2 A1 A2  _ _   _ _ L0 C1   _ _ _ _ _ _ _  5 _ ledger_must_not_be_zero
        \\ transfer T3 A1 A2  _ _   _ _ L1 C1   _ _ _ _ _ _ _ 15 _ ok
        \\ commit create_transfers
        \\
        \\ lookup_account A1 0 15 0 20
        \\ lookup_account A2 0  0 0 15
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T1 exists false
        \\ lookup_transfer T2 exists false
        \\ lookup_transfer T3 exists true
        \\ commit lookup_transfers
    );
}

test "create_transfers: balancing_debit | balancing_credit (*_must_not_exceed_*)" {
    try check(
        \\ account A1 _ _ L1 C1 _ D<C   _ _ 0 0 0 0 _ ok
        \\ account A2 _ _ L1 C1 _   _ C<D _ 0 0 0 0 _ ok
        \\ account A3 _ _ L1 C1 _   _   _ _ 0 0 0 0 _ ok
        \\ commit create_accounts
        \\
        \\ setup A1 1  0 0 10
        \\ setup A2 0 10 2  0
        \\
        \\ transfer T1 A1 A3  _ _   _ _ L2 C1 _ _ _ _ BDR   _ _  3 _ transfer_must_have_the_same_ledger_as_accounts
        \\ transfer T1 A3 A2  _ _   _ _ L2 C1 _ _ _ _   _ BCR _  3 _ transfer_must_have_the_same_ledger_as_accounts
        \\ transfer T1 A1 A3  _ _   _ _ L1 C1 _ _ _ _ BDR   _ _  3 _ ok
        \\ transfer T2 A1 A3  _ _   _ _ L1 C1 _ _ _ _ BDR   _ _ 13 _ ok
        \\ transfer T3 A3 A2  _ _   _ _ L1 C1 _ _ _ _   _ BCR _  3 _ ok
        \\ transfer T4 A3 A2  _ _   _ _ L1 C1 _ _ _ _   _ BCR _ 13 _ ok
        \\ transfer T5 A1 A3  _ _   _ _ L1 C1 _ _ _ _ BDR   _ _  1 _ exceeds_credits
        \\ transfer T5 A1 A3  _ _   _ _ L1 C1 _ _ _ _ BDR BCR _  1 _ exceeds_credits
        \\ transfer T5 A3 A2  _ _   _ _ L1 C1 _ _ _ _   _ BCR _  1 _ exceeds_debits
        \\ transfer T5 A1 A2  _ _   _ _ L1 C1 _ _ _ _ BDR BCR _  1 _ exceeds_credits

        // "exists" requires that the amount matches exactly, even when BDR/BCR is set.
        \\ transfer T1 A1 A3  _ _   _ _ L1 C1 _ _ _ _ BDR   _ _  2 _ exists_with_different_amount
        \\ transfer T1 A1 A3  _ _   _ _ L1 C1 _ _ _ _ BDR   _ _  4 _ exists_with_different_amount
        \\ transfer T1 A1 A3  _ _   _ _ L1 C1 _ _ _ _ BDR   _ _  3 _ exists
        \\ transfer T2 A1 A3  _ _   _ _ L1 C1 _ _ _ _ BDR   _ _  6 _ exists
        \\ transfer T3 A3 A2  _ _   _ _ L1 C1 _ _ _ _   _ BCR _  3 _ exists
        \\ transfer T4 A3 A2  _ _   _ _ L1 C1 _ _ _ _   _ BCR _  5 _ exists
        \\ commit create_transfers
        \\
        \\ lookup_account A1 1  9 0 10
        \\ lookup_account A2 0 10 2  8
        \\ lookup_account A3 0  8 0  9
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T1 amount 3
        \\ lookup_transfer T2 amount 6
        \\ lookup_transfer T3 amount 3
        \\ lookup_transfer T4 amount 5
        \\ lookup_transfer T5 exists false
        \\ commit lookup_transfers
    );
}

test "create_transfers: balancing_debit | balancing_credit (¬*_must_not_exceed_*)" {
    try check(
        \\ account A1 _ _ L1 C1 _   _   _ _ 0 0 0 0 _ ok
        \\ account A2 _ _ L1 C1 _   _   _ _ 0 0 0 0 _ ok
        \\ account A3 _ _ L1 C1 _   _   _ _ 0 0 0 0 _ ok
        \\ commit create_accounts
        \\
        \\ setup A1 1  0 0 10
        \\ setup A2 0 10 2  0
        \\
        \\ transfer T1 A3 A1  _ _   _ _ L1 C1 _ _ _ _ BDR BCR _ 99 _ exceeds_credits
        \\ transfer T1 A3 A1  _ _   _ _ L1 C1 _ _ _ _ BDR   _ _ 99 _ exceeds_credits
        \\ transfer T1 A2 A3  _ _   _ _ L1 C1 _ _ _ _   _ BCR _ 99 _ exceeds_debits
        \\ transfer T1 A1 A3  _ _   _ _ L1 C1 _ _ _ _ BDR   _ _ 99 _ ok
        \\ transfer T2 A1 A3  _ _   _ _ L1 C1 _ _ _ _ BDR   _ _ 99 _ exceeds_credits
        \\ transfer T3 A3 A2  _ _   _ _ L1 C1 _ _ _ _   _ BCR _ 99 _ ok
        \\ transfer T4 A3 A2  _ _   _ _ L1 C1 _ _ _ _   _ BCR _ 99 _ exceeds_debits
        \\ commit create_transfers
        \\
        \\ lookup_account A1 1  9 0 10
        \\ lookup_account A2 0 10 2  8
        \\ lookup_account A3 0  8 0  9
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T1 amount 9
        \\ lookup_transfer T2 exists false
        \\ lookup_transfer T3 amount 8
        \\ lookup_transfer T4 exists false
        \\ commit lookup_transfers
    );
}

test "create_transfers: balancing_debit | balancing_credit (amount=0)" {
    try check(
        \\ account A1 _ _ L1 C1 _ D<C   _ _ 0 0 0 0 _ ok
        \\ account A2 _ _ L1 C1 _   _ C<D _ 0 0 0 0 _ ok
        \\ account A3 _ _ L1 C1 _   _ C<D _ 0 0 0 0 _ ok
        \\ account A4 _ _ L1 C1 _   _   _ _ 0 0 0 0 _ ok
        \\ commit create_accounts
        \\
        \\ setup A1 1  0 0 10
        \\ setup A2 0 10 2  0
        \\ setup A3 0 10 2  0
        \\
        \\ transfer T1 A1 A4  _ _   _ _ L1 C1 _   _ _ _ BDR   _ _  0 _ ok
        \\ transfer T2 A4 A2  _ _   _ _ L1 C1 _   _ _ _   _ BCR _  0 _ ok
        \\ transfer T3 A4 A3  _ _   _ _ L1 C1 _ PEN _ _   _ BCR _  0 _ ok
        \\ commit create_transfers
        \\
        \\ lookup_account A1 1  9  0 10
        \\ lookup_account A2 0 10  2  8
        \\ lookup_account A3 0 10 10  0
        \\ lookup_account A4 8  8  0  9
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T1 amount 9
        \\ lookup_transfer T2 amount 8
        \\ lookup_transfer T3 amount 8
        \\ commit lookup_transfers
    );
}

test "create_transfers: balancing_debit & balancing_credit" {
    try check(
        \\ account A1 _ _ L1 C1 _ D<C   _ _ 0 0 0 0 _ ok
        \\ account A2 _ _ L1 C1 _   _ C<D _ 0 0 0 0 _ ok
        \\ account A3 _ _ L1 C1 _   _   _ _ 0 0 0 0 _ ok
        \\ commit create_accounts
        \\
        \\ setup A1 0  0 0 20
        \\ setup A2 0 10 0  0
        \\ setup A3 0 99 0  0
        \\
        \\ transfer T1 A1 A2  _ _   _ _ L1 C1 _ _ _ _ BDR BCR _  1 _ ok
        \\ transfer T2 A1 A2  _ _   _ _ L1 C1 _ _ _ _ BDR BCR _ 12 _ ok
        \\ transfer T3 A1 A2  _ _   _ _ L1 C1 _ _ _ _ BDR BCR _  1 _ exceeds_debits
        \\ transfer T3 A1 A3  _ _   _ _ L1 C1 _ _ _ _ BDR BCR _ 12 _ ok
        \\ transfer T4 A1 A3  _ _   _ _ L1 C1 _ _ _ _ BDR BCR _  1 _ exceeds_credits
        \\ commit create_transfers
        \\
        \\ lookup_account A1 0 20 0 20
        \\ lookup_account A2 0 10 0 10
        \\ lookup_account A3 0 99 0 10
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T1 amount  1
        \\ lookup_transfer T2 amount  9
        \\ lookup_transfer T3 amount 10
        \\ lookup_transfer T4 exists false
        \\ commit lookup_transfers
    );
}

test "create_transfers: balancing_debit/balancing_credit + pending" {
    try check(
        \\ account A1 _ _ L1 C1 _ D<C   _ _ 0 0 0 0 _ ok
        \\ account A2 _ _ L1 C1 _   _ C<D _ 0 0 0 0 _ ok
        \\ commit create_accounts
        \\
        \\ setup A1 0  0 0 10
        \\ setup A2 0 10 0  0
        \\
        \\ transfer T1 A1 A2  _ _   _ _ L1 C1 _ PEN   _   _ BDR   _ _  3 _ ok
        \\ transfer T2 A1 A2  _ _   _ _ L1 C1 _ PEN   _   _ BDR   _ _ 13 _ ok
        \\ transfer T3 A1 A2  _ _   _ _ L1 C1 _ PEN   _   _ BDR   _ _  1 _ exceeds_credits
        \\ commit create_transfers
        \\
        \\ lookup_account A1 10  0  0 10
        \\ lookup_account A2  0 10 10  0
        \\ commit lookup_accounts
        \\
        \\ transfer T3 A1 A2  _ _  T1 _ L1 C1 _   _ POS   _   _   _ _  0 _ ok
        \\ transfer T4 A1 A2  _ _  T2 _ L1 C1 _   _ POS   _   _   _ _  5 _ ok
        \\ commit create_transfers
        \\
        \\ lookup_transfer T1 amount  3
        \\ lookup_transfer T2 amount  7
        \\ lookup_transfer T3 amount  3
        \\ lookup_transfer T4 amount  5
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
        .lsm_batch_multiple = 1,
        .vsr_operations_reserved = 128,
    });

    std.testing.refAllDecls(StateMachine);
}
