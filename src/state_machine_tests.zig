const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const stdx = @import("stdx");
const maybe = stdx.maybe;

const tb = @import("tigerbeetle.zig");
const vsr = @import("vsr.zig");
const constants = vsr.constants;

const Account = tb.Account;
const AccountBalance = tb.AccountBalance;
const Transfer = tb.Transfer;
const CreateAccountResult = tb.CreateAccountResult;
const CreateTransferResult = tb.CreateTransferResult;

const CreateAccountStatus = tb.CreateAccountStatus;
const CreateTransferStatus = tb.CreateTransferStatus;

const AccountFilter = tb.AccountFilter;
const QueryFilter = tb.QueryFilter;
const ChangeEventsFilter = tb.ChangeEventsFilter;
const ChangeEvent = tb.ChangeEvent;
const ChangeEventType = tb.ChangeEventType;

const StateMachineType = @import("state_machine.zig").StateMachineType;
const MessagePool = @import("message_pool.zig").MessagePool;
const MultiBatchDecoder = @import("./vsr/multi_batch.zig").MultiBatchDecoder;
const MultiBatchEncoder = @import("./vsr/multi_batch.zig").MultiBatchEncoder;
const Packet = @import("./clients/c/tb_client/packet.zig").Packet;
const TimestampRange = @import("lsm/timestamp_range.zig").TimestampRange;

const TimeSim = @import("testing/time.zig").TimeSim;
const Storage = @import("testing/storage.zig").Storage;
const Tracer = Storage.Tracer;
const SuperBlock = @import("vsr/superblock.zig").SuperBlockType(Storage);
const Grid = @import("vsr/grid.zig").GridType(Storage);
const fixtures = @import("testing/fixtures.zig");
const data_file_size_min = @import("vsr/superblock.zig").data_file_size_min;

const parse_table = @import("testing/table.zig").parse;
const testing = std.testing;

const StateMachine = StateMachineType(Storage);

/// Variations of operations supported by the state machine,
/// including deprecated ones used by old clients.
const TestOperation = enum {
    create_accounts,
    create_transfers,
    lookup_accounts,
    lookup_transfers,
    get_account_transfers,
    get_account_balances,
    query_accounts,
    query_transfers,
    get_change_events,

    const VersionMap = std.EnumArray(TestOperation, StateMachine.Operation);
    const versions: []const VersionMap = &.{
        .init(.{
            .create_accounts = .create_accounts,
            .create_transfers = .create_transfers,
            .lookup_accounts = .lookup_accounts,
            .lookup_transfers = .lookup_transfers,
            .get_account_transfers = .get_account_transfers,
            .get_account_balances = .get_account_balances,
            .query_accounts = .query_accounts,
            .query_transfers = .query_transfers,
            .get_change_events = .get_change_events,
        }),
        .init(.{
            .create_accounts = .deprecated_create_accounts_sparse,
            .create_transfers = .deprecated_create_transfers_sparse,
            .lookup_accounts = .lookup_accounts,
            .lookup_transfers = .lookup_transfers,
            .get_account_transfers = .get_account_transfers,
            .get_account_balances = .get_account_balances,
            .query_accounts = .query_accounts,
            .query_transfers = .query_transfers,
            .get_change_events = .get_change_events,
        }),
        .init(.{
            .create_accounts = .deprecated_create_accounts_unbatched,
            .create_transfers = .deprecated_create_transfers_unbatched,
            .lookup_accounts = .deprecated_lookup_accounts_unbatched,
            .lookup_transfers = .deprecated_lookup_transfers_unbatched,
            .get_account_transfers = .deprecated_get_account_transfers_unbatched,
            .get_account_balances = .deprecated_get_account_balances_unbatched,
            .query_accounts = .deprecated_query_accounts_unbatched,
            .query_transfers = .deprecated_query_transfers_unbatched,
            .get_change_events = .get_change_events,
        }),
    };
};

const TestAction = union(enum) {
    /// Set the account's balance.
    setup: Setup,

    tick: Tick,

    commit: TestOperation,
    account: CreateAccount,
    transfer: CreateTransfer,

    lookup_account: LookupAccount,
    lookup_transfer: LookupTransfer,

    get_account_balances: GetAccountBalances,
    get_account_balances_result: GetAccountBalancesResult,

    get_account_transfers: GetAccountTransfers,
    get_account_transfers_result: u128,

    query_accounts: QueryAccounts,
    query_accounts_result: QueryAccountsResult,

    query_transfers: QueryTransfers,
    query_transfers_result: u128,

    get_change_events: GetChangeEvents,
    get_change_events_result: GetChangeEventsResult,

    const Setup = struct {
        account: u128,
        debits_pending: u128,
        debits_posted: u128,
        credits_pending: u128,
        credits_posted: u128,
    };

    const Tick = struct {
        value: i64,
        unit: enum { nanoseconds, seconds },
    };

    const CreateAccount = struct {
        id: u128,
        debits_pending: u128 = 0,
        debits_posted: u128 = 0,
        credits_pending: u128 = 0,
        credits_posted: u128 = 0,
        user_data_128: u128 = 0,
        user_data_64: u64 = 0,
        user_data_32: u32 = 0,
        reserved: u1 = 0,
        ledger: u32,
        code: u16,
        flags_linked: ?enum { LNK } = null,
        flags_debits_must_not_exceed_credits: ?enum { @"D<C" } = null,
        flags_credits_must_not_exceed_debits: ?enum { @"C<D" } = null,
        flags_history: ?enum { HIST } = null,
        flags_imported: ?enum { IMP } = null,
        flags_closed: ?enum { CLSD } = null,
        flags_padding: u10 = 0,
        timestamp: u64 = 0,
        status: CreateAccountStatus,

        fn event(a: CreateAccount) Account {
            return .{
                .id = a.id,
                .debits_pending = a.debits_pending,
                .debits_posted = a.debits_posted,
                .credits_pending = a.credits_pending,
                .credits_posted = a.credits_posted,
                .user_data_128 = a.user_data_128,
                .user_data_64 = a.user_data_64,
                .user_data_32 = a.user_data_32,
                .reserved = a.reserved,
                .ledger = a.ledger,
                .code = a.code,
                .flags = .{
                    .linked = a.flags_linked != null,
                    .debits_must_not_exceed_credits = a
                        .flags_debits_must_not_exceed_credits != null,
                    .credits_must_not_exceed_debits = a
                        .flags_credits_must_not_exceed_debits != null,
                    .history = a.flags_history != null,
                    .imported = a.flags_imported != null,
                    .closed = a.flags_closed != null,
                    .padding = a.flags_padding,
                },
                .timestamp = a.timestamp,
            };
        }
    };

    const CreateTransfer = struct {
        id: u128,
        debit_account_id: u128,
        credit_account_id: u128,
        amount: u128 = 0,
        pending_id: u128 = 0,
        user_data_128: u128 = 0,
        user_data_64: u64 = 0,
        user_data_32: u32 = 0,
        timeout: u32 = 0,
        ledger: u32,
        code: u16,
        flags_linked: ?enum { LNK } = null,
        flags_pending: ?enum { PEN } = null,
        flags_post_pending_transfer: ?enum { POS } = null,
        flags_void_pending_transfer: ?enum { VOI } = null,
        flags_balancing_debit: ?enum { BDR } = null,
        flags_balancing_credit: ?enum { BCR } = null,
        flags_imported: ?enum { IMP } = null,
        flags_closing_debit: ?enum { CDR } = null,
        flags_closing_credit: ?enum { CCR } = null,
        flags_padding: u5 = 0,
        timestamp: u64 = 0,
        status: CreateTransferStatus,

        fn event(t: CreateTransfer) Transfer {
            return .{
                .id = t.id,
                .debit_account_id = t.debit_account_id,
                .credit_account_id = t.credit_account_id,
                .amount = t.amount,
                .pending_id = t.pending_id,
                .user_data_128 = t.user_data_128,
                .user_data_64 = t.user_data_64,
                .user_data_32 = t.user_data_32,
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
                    .imported = t.flags_imported != null,
                    .closing_debit = t.flags_closing_debit != null,
                    .closing_credit = t.flags_closing_credit != null,
                    .padding = t.flags_padding,
                },
                .timestamp = t.timestamp,
            };
        }
    };

    const LookupAccount = struct {
        id: u128,
        data: ?struct {
            debits_pending: u128,
            debits_posted: u128,
            credits_pending: u128,
            credits_posted: u128,
            flag_closed: ?enum { CLSD } = null,
        } = null,
    };

    const LookupTransfer = struct {
        id: u128,
        data: union(enum) {
            exists: bool,
            amount: u128,
            timestamp: u64,
        },
    };

    const GetAccountBalances = struct {
        account_id: u128,
        user_data_128: ?u128 = null,
        user_data_64: ?u64 = null,
        user_data_32: ?u32 = null,
        code: ?u16 = null,
        // When non-null, the filter is set to the timestamp
        //at which the specified transfer (by id) was created.
        timestamp_min_transfer_id: ?u128 = null,
        timestamp_max_transfer_id: ?u128 = null,
        limit: u32,
        flags_debits: ?enum { DR } = null,
        flags_credits: ?enum { CR } = null,
        flags_reversed: ?enum { REV } = null,
    };

    const GetAccountBalancesResult = struct {
        transfer_id: u128,
        debits_pending: u128,
        debits_posted: u128,
        credits_pending: u128,
        credits_posted: u128,
    };

    const GetAccountTransfers = struct {
        account_id: u128,
        user_data_128: ?u128 = null,
        user_data_64: ?u64 = null,
        user_data_32: ?u32 = null,
        code: ?u16 = null,
        // When non-null, the filter is set to the timestamp at which
        // the specified transfer (by id) was created.
        timestamp_min_transfer_id: ?u128 = null,
        timestamp_max_transfer_id: ?u128 = null,
        limit: u32,
        flags_debits: ?enum { DR } = null,
        flags_credits: ?enum { CR } = null,
        flags_reversed: ?enum { REV } = null,
    };

    const QueryAccounts = struct {
        user_data_128: u128,
        user_data_64: u64,
        user_data_32: u32,
        ledger: u32,
        code: u16,
        timestamp_min_transfer_id: ?u128 = null,
        timestamp_max_transfer_id: ?u128 = null,
        limit: u32,
        flags_reversed: ?enum { REV } = null,
    };

    const QueryAccountsResult = struct {
        id: u128,
        data: ?struct {
            debits_pending: u128,
            debits_posted: u128,
            credits_pending: u128,
            credits_posted: u128,
            flag_closed: ?enum { CLSD } = null,
        } = null,
    };

    const QueryTransfers = struct {
        user_data_128: u128,
        user_data_64: u64,
        user_data_32: u32,
        ledger: u32,
        code: u16,
        timestamp_min_transfer_id: ?u128 = null,
        timestamp_max_transfer_id: ?u128 = null,
        limit: u32,
        flags_reversed: ?enum { REV } = null,
    };

    const GetChangeEvents = struct {
        timestamp_min_transfer_id: ?u128 = null,
        timestamp_max_transfer_id: ?u128 = null,
        limit: u32,
    };

    const GetChangeEventsResult = struct {
        const Balance = struct {
            account_id: u128,
            debits_pending: u128,
            debits_posted: u128,
            credits_pending: u128,
            credits_posted: u128,
            closed: ?enum { CLSD } = null,
        };

        event_type: ?enum { PEN, POS, VOI, EXP } = null,
        timestamp_transfer: ?u128 = null,
        amount: u128,
        transfer_pending_id: ?u128 = null,
        dr_account: Balance,
        cr_account: Balance,

        fn match(
            self: *const GetChangeEventsResult,
            accounts: *std.AutoHashMap(u128, Account),
            transfers: *std.AutoHashMap(u128, Transfer),
            event: *const ChangeEvent,
        ) bool {
            if (self.timestamp_transfer) |id| {
                const transfer = transfers.get(id).?;
                if (event.type == .two_phase_expired) return false;
                if (event.timestamp != transfer.timestamp) return false;
                if (!match_transfer(event, &transfer)) return false;
            }
            if (self.event_type) |event_type| {
                const expected: ChangeEventType = switch (event_type) {
                    .PEN => .two_phase_pending,
                    .POS => .two_phase_posted,
                    .VOI => .two_phase_voided,
                    .EXP => .two_phase_expired,
                };
                if (event.type != expected) return false;
            } else {
                if (event.type != .single_phase) return false;
            }
            if (event.transfer_amount != self.amount) return false;
            if (self.transfer_pending_id) |transfer_pending_id| {
                switch (event.type) {
                    .two_phase_pending, .single_phase => return false,
                    .two_phase_posted, .two_phase_voided => {
                        if (event.transfer_pending_id != transfer_pending_id) return false;
                    },
                    .two_phase_expired => {
                        const transfer = transfers.get(transfer_pending_id).?;
                        if (transfer.timeout == 0) return false;
                        if (event.timestamp <
                            transfer.timestamp + transfer.timeout_ns()) return false;
                        if (!match_transfer(event, &transfer)) return false;
                    },
                }
            }

            const dr_account = accounts.get(self.dr_account.account_id).?;
            if (dr_account.ledger != event.ledger) return false;
            if (self.dr_account.account_id != event.debit_account_id) return false;
            if (dr_account.timestamp != event.debit_account_timestamp) return false;
            if (self.dr_account.debits_pending != event.debit_account_debits_pending) return false;
            if (self.dr_account.debits_posted != event.debit_account_debits_posted) return false;
            if (self.dr_account.credits_pending != event.debit_account_credits_pending)
                return false;
            if (self.dr_account.credits_posted != event.debit_account_credits_posted) return false;
            if ((self.dr_account.closed == .CLSD) != event.debit_account_flags.closed)
                return false;

            const cr_account = accounts.get(self.cr_account.account_id).?;
            if (cr_account.ledger != event.ledger) return false;
            if (self.cr_account.account_id != event.credit_account_id) return false;
            if (cr_account.timestamp != event.credit_account_timestamp) return false;
            if (self.cr_account.debits_pending != event.credit_account_debits_pending)
                return false;
            if (self.cr_account.debits_posted != event.credit_account_debits_posted)
                return false;
            if (self.cr_account.credits_pending != event.credit_account_credits_pending)
                return false;
            if (self.cr_account.credits_posted != event.credit_account_credits_posted)
                return false;
            if ((self.cr_account.closed == .CLSD) != event.credit_account_flags.closed)
                return false;

            return true;
        }

        fn match_transfer(event: *const ChangeEvent, transfer: *const tb.Transfer) bool {
            if (event.transfer_timestamp != transfer.timestamp) return false;
            if (event.transfer_id != transfer.id) return false;
            if (event.transfer_amount != transfer.amount and
                // The in-memory model keeps the `AMOUNT_MAX`.
                transfer.amount != std.math.maxInt(u128)) return false;
            if (event.transfer_pending_id != transfer.pending_id) return false;
            if (event.transfer_user_data_128 != transfer.user_data_128) return false;
            if (event.transfer_user_data_64 != transfer.user_data_64) return false;
            if (event.transfer_user_data_32 != transfer.user_data_32) return false;
            if (event.transfer_code != transfer.code) return false;
            if (event.ledger != transfer.ledger) return false;
            if (event.ledger != transfer.ledger) return false;
            if (@as(u16, @bitCast(transfer.flags)) !=
                @as(u16, @bitCast(event.transfer_flags))) return false;
            return true;
        }
    };

    fn operation(action: TestAction) ?TestOperation {
        return switch (action) {
            .setup => null,
            .tick => null,

            .commit => |tag| tag,

            .account => .create_accounts,
            .transfer => .create_transfers,
            .lookup_account => .lookup_accounts,
            .lookup_transfer => .lookup_transfers,

            .get_account_balances,
            .get_account_balances_result,
            => .get_account_balances,

            .get_account_transfers,
            .get_account_transfers_result,
            => .get_account_transfers,

            .query_accounts,
            .query_accounts_result,
            => .query_accounts,

            .query_transfers,
            .query_transfers_result,
            => .query_transfers,

            .get_change_events,
            .get_change_events_result,
            => .get_change_events,
        };
    }
};

const ArrayList = std.ArrayListAligned(u8, constants.cache_line_size);

fn check(test_table: []const u8) !void {
    const test_actions = parse_table(TestAction, test_table);

    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();

    // Runs the same test for each variation of supported operations,
    // simulating different client versions.
    inline for (TestOperation.versions) |*version_map| {
        // When multibatching is enabled, operations of the same type
        // can be submitted together to the state machine.
        inline for (.{ true, false }) |multibatching_enabled| {
            defer _ = arena.reset(.free_all);

            const Runner = RunnerType(.{
                .multibatching_enabled = multibatching_enabled,
                .version_map = version_map,
            });

            try Runner.run(arena.allocator(), test_actions.const_slice());
        }
    }
}

const Options = struct {
    version_map: *const TestOperation.VersionMap,
    multibatching_enabled: bool,
};

const Request = struct {
    packet: Packet,
    reply_expected: []const u8,
};

fn RunnerType(comptime options: Options) type {
    return struct {
        const Runner = @This();

        pub fn run(
            arena: mem.Allocator,
            test_actions: []const TestAction,
        ) !void {
            var context: TestContext = undefined;
            try context.init(arena, &assert_results);

            var input_buffer: ArrayList = .init(arena);
            var output_buffer: ArrayList = .init(arena);

            var operation: ?TestOperation = null;
            for (test_actions) |test_action| {
                switch (test_action) {
                    inline else => |action, tag| {
                        if (operation != null and operation != test_action.operation()) {
                            try context.flush();
                        }

                        operation = test_action.operation();

                        const function = @field(Runner, @tagName(tag));
                        try function(&context, action, &input_buffer, &output_buffer);

                        if (test_action == .commit and !options.multibatching_enabled) {
                            try context.flush();
                            operation = null;
                        }
                    },
                }
            }

            if (options.multibatching_enabled) {
                try context.flush();
                operation = null;
            }
            assert(operation == null);
            assert(input_buffer.items.len == 0);
            assert(output_buffer.items.len == 0);
        }

        fn setup(
            context: *TestContext,
            action: TestAction.Setup,
            input_buffer: *ArrayList,
            output_buffer: *ArrayList,
        ) !void {
            assert(input_buffer.items.len == 0);
            assert(output_buffer.items.len == 0);

            assert(context.state == .idle);
            assert(context.pending.empty());

            const account_old = context.get_account_from_cache(action.account).?;
            var account_new: Account = account_old;

            account_new.debits_pending = action.debits_pending;
            account_new.debits_posted = action.debits_posted;
            account_new.credits_pending = action.credits_pending;
            account_new.credits_posted = action.credits_posted;
            assert(!account_new.debits_exceed_credits(0));
            assert(!account_new.credits_exceed_debits(0));

            if (!stdx.equal_bytes(Account, &account_new, &account_old)) {
                context.state_machine.forest.grooves.accounts.update(.{
                    .old = &account_old,
                    .new = &account_new,
                });
            }
        }

        fn tick(
            context: *TestContext,
            ticks: TestAction.Tick,
            input_buffer: *ArrayList,
            output_buffer: *ArrayList,
        ) !void {
            assert(input_buffer.items.len == 0);
            assert(output_buffer.items.len == 0);

            assert(context.state == .idle);
            assert(context.pending.empty());
            assert(ticks.value != 0);

            // The `parse` logic already computes `maxInt - value` when a unsigned int is
            // represented as a negative number. However, we need to use a signed int and
            // perform our own calculation to account for the unit.
            const interval_ns: u64 = @abs(ticks.value) *
                @as(u64, switch (ticks.unit) {
                    .nanoseconds => 1,
                    .seconds => std.time.ns_per_s,
                });

            context.state_machine.prepare_timestamp += if (ticks.value > 0)
                interval_ns
            else
                TimestampRange.timestamp_max - interval_ns;
            context.commit_timestamp_expected = context.state_machine.prepare_timestamp;

            // Pulse is executed when the cluster is idle.
            context.pulse();
        }

        fn account(
            context: *TestContext,
            action: TestAction.CreateAccount,
            input_buffer: *ArrayList,
            output_buffer: *ArrayList,
        ) !void {
            var event = action.event();
            try input_buffer.appendSlice(std.mem.asBytes(&event));

            context.commit_timestamp_expected += 1;

            if (event.timestamp == 0) event.timestamp = context.commit_timestamp_expected;
            if (action.status == .created) {
                try context.accounts.put(action.id, event);
            }

            switch (options.version_map.get(.create_accounts)) {
                .create_accounts => {
                    const result: CreateAccountResult = .{
                        .timestamp = timestamp: {
                            if (action.status == .created or
                                action.status == .linked_event_failed)
                            {
                                break :timestamp event.timestamp;
                            }
                            if (action.status == .exists) {
                                break :timestamp if (context.accounts.get(action.id)) |exists|
                                    exists.timestamp
                                else
                                    context.linked_events_failed.get(action.id).?;
                            }
                            break :timestamp context.commit_timestamp_expected;
                        },
                        .status = action.status,
                    };
                    try output_buffer.appendSlice(std.mem.asBytes(&result));

                    if (event.flags.linked) {
                        if (action.status == .linked_event_failed) {
                            try context.linked_events_failed.putNoClobber(
                                event.id,
                                event.timestamp,
                            );
                        }
                    } else {
                        context.linked_events_failed.clearRetainingCapacity();
                    }
                },
                .deprecated_create_accounts_sparse,
                .deprecated_create_accounts_unbatched,
                => if (action.status != .created) {
                    const result: tb.CreateAccountErrorResult = .{
                        .index = @intCast(@divExact(input_buffer.items.len, @sizeOf(Account)) - 1),
                        .result = action.status,
                    };
                    try output_buffer.appendSlice(std.mem.asBytes(&result));
                },
                else => unreachable,
            }
        }

        fn transfer(
            context: *TestContext,
            action: TestAction.CreateTransfer,
            input_buffer: *ArrayList,
            output_buffer: *ArrayList,
        ) !void {
            var event = action.event();
            try input_buffer.appendSlice(std.mem.asBytes(&event));

            context.commit_timestamp_expected += 1;

            if (action.timestamp == 0) event.timestamp = context.commit_timestamp_expected;
            if (action.status == .created) {
                if (event.pending_id != 0) {
                    // Fill in default values.
                    const t_pending = context.transfers.get(event.pending_id).?;
                    inline for (.{
                        "debit_account_id",
                        "credit_account_id",
                        "ledger",
                        "code",
                        "user_data_128",
                        "user_data_64",
                        "user_data_32",
                    }) |field| {
                        if (@field(event, field) == 0) {
                            @field(event, field) = @field(t_pending, field);
                        }
                    }

                    if (event.flags.void_pending_transfer) {
                        if (event.amount == 0) event.amount = t_pending.amount;
                    }
                }
                try context.transfers.put(action.id, event);
            }

            switch (options.version_map.get(.create_transfers)) {
                .create_transfers => {
                    const result: CreateTransferResult = .{
                        .timestamp = timestamp: {
                            if (action.status == .created or
                                action.status == .linked_event_failed)
                            {
                                break :timestamp event.timestamp;
                            }
                            if (action.status == .exists) {
                                break :timestamp if (context.transfers.get(action.id)) |exists|
                                    exists.timestamp
                                else
                                    context.linked_events_failed.get(action.id).?;
                            }
                            break :timestamp context.commit_timestamp_expected;
                        },
                        .status = action.status,
                    };
                    try output_buffer.appendSlice(std.mem.asBytes(&result));

                    if (event.flags.linked) {
                        if (action.status == .linked_event_failed) {
                            try context.linked_events_failed.putNoClobber(
                                event.id,
                                event.timestamp,
                            );
                        }
                    } else {
                        context.linked_events_failed.clearRetainingCapacity();
                    }
                },
                .deprecated_create_transfers_sparse,
                .deprecated_create_transfers_unbatched,
                => if (action.status != .created) {
                    const result: tb.CreateTransferErrorResult = .{
                        .index = @intCast(@divExact(input_buffer.items.len, @sizeOf(Transfer)) - 1),
                        .result = action.status,
                    };
                    try output_buffer.appendSlice(std.mem.asBytes(&result));
                },
                else => unreachable,
            }
        }

        fn lookup_account(
            context: *TestContext,
            action: TestAction.LookupAccount,
            input_buffer: *ArrayList,
            output_buffer: *ArrayList,
        ) !void {
            try input_buffer.appendSlice(std.mem.asBytes(&action.id));
            if (action.data) |data| {
                var a: Account = context.accounts.get(action.id).?;
                a.debits_pending = data.debits_pending;
                a.debits_posted = data.debits_posted;
                a.credits_pending = data.credits_pending;
                a.credits_posted = data.credits_posted;
                a.flags.closed = data.flag_closed != null;
                try output_buffer.appendSlice(std.mem.asBytes(&a));
            }
        }

        fn lookup_transfer(
            context: *TestContext,
            action: TestAction.LookupTransfer,
            input_buffer: *ArrayList,
            output_buffer: *ArrayList,
        ) !void {
            try input_buffer.appendSlice(std.mem.asBytes(&action.id));
            switch (action.data) {
                .exists => |exists| {
                    if (exists) {
                        var t: Transfer = context.transfers.get(action.id).?;
                        try output_buffer.appendSlice(std.mem.asBytes(&t));
                    }
                },
                .amount => |amount| {
                    var t: Transfer = context.transfers.get(action.id).?;
                    t.amount = amount;
                    try output_buffer.appendSlice(std.mem.asBytes(&t));
                },
                .timestamp => |timestamp| {
                    var t: Transfer = context.transfers.get(action.id).?;
                    t.timestamp = timestamp;
                    try output_buffer.appendSlice(std.mem.asBytes(&t));
                },
            }
        }

        fn get_account_balances(
            context: *TestContext,
            action: TestAction.GetAccountBalances,
            input_buffer: *ArrayList,
            output_buffer: *ArrayList,
        ) !void {
            _ = output_buffer;

            const timestamp_min =
                if (action.timestamp_min_transfer_id) |id|
                    context.transfers.get(id).?.timestamp
                else
                    0;
            const timestamp_max =
                if (action.timestamp_max_transfer_id) |id|
                    context.transfers.get(id).?.timestamp
                else
                    0;

            const limit: u32 = if (action.limit == std.math.maxInt(u32))
                options.version_map.get(.get_account_balances)
                    .result_max(context.state_machine.batch_size_limit)
            else
                action.limit;

            const event: AccountFilter = .{
                .account_id = action.account_id,
                .user_data_128 = action.user_data_128 orelse 0,
                .user_data_64 = action.user_data_64 orelse 0,
                .user_data_32 = action.user_data_32 orelse 0,
                .code = action.code orelse 0,
                .timestamp_min = timestamp_min,
                .timestamp_max = timestamp_max,
                .limit = limit,
                .flags = .{
                    .debits = action.flags_debits != null,
                    .credits = action.flags_credits != null,
                    .reversed = action.flags_reversed != null,
                },
            };
            try input_buffer.appendSlice(std.mem.asBytes(&event));
        }

        fn get_account_balances_result(
            context: *TestContext,
            action: TestAction.GetAccountBalancesResult,
            input_buffer: *ArrayList,
            output_buffer: *ArrayList,
        ) !void {
            _ = input_buffer;

            const result: AccountBalance = .{
                .debits_pending = action.debits_pending,
                .debits_posted = action.debits_posted,
                .credits_pending = action.credits_pending,
                .credits_posted = action.credits_posted,
                .timestamp = context.transfers.get(action.transfer_id).?.timestamp,
            };
            try output_buffer.appendSlice(std.mem.asBytes(&result));
        }

        fn get_account_transfers(
            context: *TestContext,
            action: TestAction.GetAccountTransfers,
            input_buffer: *ArrayList,
            output_buffer: *ArrayList,
        ) !void {
            _ = output_buffer;

            const timestamp_min =
                if (action.timestamp_min_transfer_id) |id|
                    context.transfers.get(id).?.timestamp
                else
                    0;
            const timestamp_max =
                if (action.timestamp_max_transfer_id) |id|
                    context.transfers.get(id).?.timestamp
                else
                    0;

            const limit: u32 = if (action.limit == std.math.maxInt(u32))
                options.version_map.get(.get_account_transfers)
                    .result_max(context.state_machine.batch_size_limit)
            else
                action.limit;

            const event: AccountFilter = .{
                .account_id = action.account_id,
                .user_data_128 = action.user_data_128 orelse 0,
                .user_data_64 = action.user_data_64 orelse 0,
                .user_data_32 = action.user_data_32 orelse 0,
                .code = action.code orelse 0,
                .timestamp_min = timestamp_min,
                .timestamp_max = timestamp_max,
                .limit = limit,
                .flags = .{
                    .debits = action.flags_debits != null,
                    .credits = action.flags_credits != null,
                    .reversed = action.flags_reversed != null,
                },
            };
            try input_buffer.appendSlice(std.mem.asBytes(&event));
        }

        fn get_account_transfers_result(
            context: *TestContext,
            id: u128,
            input_buffer: *ArrayList,
            output_buffer: *ArrayList,
        ) !void {
            _ = input_buffer;
            try output_buffer.appendSlice(std.mem.asBytes(&context.transfers.get(id).?));
        }

        fn query_accounts(
            context: *TestContext,
            action: TestAction.QueryAccounts,
            input_buffer: *ArrayList,
            output_buffer: *ArrayList,
        ) !void {
            _ = output_buffer;

            const timestamp_min = if (action.timestamp_min_transfer_id) |id|
                context.accounts.get(id).?.timestamp
            else
                0;
            const timestamp_max = if (action.timestamp_max_transfer_id) |id|
                context.accounts.get(id).?.timestamp
            else
                0;

            const limit: u32 = if (action.limit == std.math.maxInt(u32))
                options.version_map.get(.query_accounts)
                    .result_max(context.state_machine.batch_size_limit)
            else
                action.limit;

            const event: QueryFilter = .{
                .user_data_128 = action.user_data_128,
                .user_data_64 = action.user_data_64,
                .user_data_32 = action.user_data_32,
                .ledger = action.ledger,
                .code = action.code,
                .timestamp_min = timestamp_min,
                .timestamp_max = timestamp_max,
                .limit = limit,
                .flags = .{
                    .reversed = action.flags_reversed != null,
                },
            };
            try input_buffer.appendSlice(std.mem.asBytes(&event));
        }

        fn query_accounts_result(
            context: *TestContext,
            result: TestAction.QueryAccountsResult,
            input_buffer: *ArrayList,
            output_buffer: *ArrayList,
        ) !void {
            _ = input_buffer;
            var a: Account = context.accounts.get(result.id).?;
            if (result.data) |data| {
                a.debits_pending = data.debits_pending;
                a.debits_posted = data.debits_posted;
                a.credits_pending = data.credits_pending;
                a.credits_posted = data.credits_posted;
                a.flags.closed = data.flag_closed != null;
            }
            try output_buffer.appendSlice(std.mem.asBytes(&a));
        }

        fn query_transfers(
            context: *TestContext,
            action: TestAction.QueryTransfers,
            input_buffer: *ArrayList,
            output_buffer: *ArrayList,
        ) !void {
            _ = output_buffer;
            const timestamp_min = if (action.timestamp_min_transfer_id) |id|
                context.transfers.get(id).?.timestamp
            else
                0;
            const timestamp_max = if (action.timestamp_max_transfer_id) |id|
                context.transfers.get(id).?.timestamp
            else
                0;

            const limit: u32 = if (action.limit == std.math.maxInt(u32))
                options.version_map.get(.query_accounts)
                    .result_max(context.state_machine.batch_size_limit)
            else
                action.limit;

            const event: QueryFilter = .{
                .user_data_128 = action.user_data_128,
                .user_data_64 = action.user_data_64,
                .user_data_32 = action.user_data_32,
                .ledger = action.ledger,
                .code = action.code,
                .timestamp_min = timestamp_min,
                .timestamp_max = timestamp_max,
                .limit = limit,
                .flags = .{
                    .reversed = action.flags_reversed != null,
                },
            };
            try input_buffer.appendSlice(std.mem.asBytes(&event));
        }

        fn query_transfers_result(
            context: *TestContext,
            id: u128,
            input_buffer: *ArrayList,
            output_buffer: *ArrayList,
        ) !void {
            _ = input_buffer;

            try output_buffer.appendSlice(std.mem.asBytes(&context.transfers.get(id).?));
        }

        fn get_change_events(
            context: *TestContext,
            action: TestAction.GetChangeEvents,
            input_buffer: *ArrayList,
            output_buffer: *ArrayList,
        ) !void {
            _ = output_buffer;

            const timestamp_min = if (action.timestamp_min_transfer_id) |id|
                context.transfers.get(id).?.timestamp
            else
                0;
            const timestamp_max = if (action.timestamp_max_transfer_id) |id|
                context.transfers.get(id).?.timestamp
            else
                0;

            const limit: u32 = if (action.limit == std.math.maxInt(u32))
                options.version_map.get(.get_change_events)
                    .result_max(context.state_machine.batch_size_limit)
            else
                action.limit;

            const event = ChangeEventsFilter{
                .timestamp_min = timestamp_min,
                .timestamp_max = timestamp_max,
                .limit = limit,
            };
            try input_buffer.appendSlice(std.mem.asBytes(&event));
        }

        fn get_change_events_result(
            context: *TestContext,
            result: TestAction.GetChangeEventsResult,
            input_buffer: *ArrayList,
            output_buffer: *ArrayList,
        ) !void {
            _ = context;
            _ = input_buffer;
            try output_buffer.appendSlice(std.mem.asBytes(&result));
        }

        fn commit(
            context: *TestContext,
            operation: TestOperation,
            input_buffer: *ArrayList,
            output_buffer: *ArrayList,
        ) !void {
            // Enqueues a `packet` to be submitted to the state machine when `flush()` is called.
            // Multibatching can be achieved by calling `submit()` multiple times.
            assert(context.state == .idle);

            const data: []const u8 = try input_buffer.toOwnedSlice();
            const reply_expected: []const u8 = try output_buffer.toOwnedSlice();
            const request: *Request = try context.arena.create(Request);

            request.* = .{
                .reply_expected = reply_expected,
                .packet = .init(&.{
                    .data = data.ptr,
                    .data_size = @intCast(data.len),
                    .user_data = request,
                    .operation = @intFromEnum(options.version_map.get(operation)),
                    .user_tag = 0,
                    .status = .ok,
                }),
            };

            try request.packet.batch_enqueue(
                StateMachine.Operation,
                &options.version_map.values,
                .{
                    .target = &context.pending,
                    .batch_size_limit = context.state_machine.batch_size_limit,
                    .time = context.time_sim.time(),
                },
            );
        }

        fn assert_results(
            client: *TestContext,
            operation: StateMachine.Operation,
            packet: *Packet,
            timestamp: u64,
            reply_actual: []const u8,
        ) !void {
            assert(packet.operation == @intFromEnum(operation));
            assert(packet.status == .ok);
            assert(packet.data_size == 0 or packet.data != null);
            assert(TimestampRange.valid(timestamp));

            const request: *Request = @alignCast(@fieldParentPtr("packet", packet));

            switch (operation) {
                inline else => |operation_actual_comptime| {
                    const Result = operation_actual_comptime.ResultType();
                    try testing.expectEqualSlices(
                        Result,
                        stdx.bytes_as_slice(.exact, Result, request.reply_expected),
                        stdx.bytes_as_slice(.exact, Result, reply_actual),
                    );
                },
                .get_change_events => {
                    const results_actual = stdx.bytes_as_slice(
                        .exact,
                        ChangeEvent,
                        reply_actual,
                    );
                    const results_expected = stdx.bytes_as_slice(
                        .exact,
                        TestAction.GetChangeEventsResult,
                        request.reply_expected,
                    );
                    try testing.expectEqual(results_expected.len, results_actual.len);
                    for (results_actual, results_expected) |*actual, *expected| {
                        try testing.expect(expected.match(
                            &client.accounts,
                            &client.transfers,
                            actual,
                        ));
                    }
                },
                .pulse => unreachable,
            }
        }
    };
}

/// Implements a tb_client-like interface that calls the state machine directly.
/// Batching and error-handling logic are shared with the real client implementation.
const TestContext = struct {
    arena: mem.Allocator,

    accounts: std.AutoHashMap(u128, Account),
    transfers: std.AutoHashMap(u128, Transfer),
    // The result code `.exists` always returns the timestamp of the original event.
    // Even if the existing event was created within a linked chain and rolled back.
    // For example, the linked chain:
    //  events:  { id=1, flags=linked; id=1 }
    //  results: { result=linked_event_failed, timestamp=100; result=exists, timestamp=100 }
    //                                                   ^^^                           ^^^
    linked_events_failed: std.AutoHashMap(u128, u64),

    message_pool: MessagePool,
    storage: Storage,
    time_sim: TimeSim,
    trace: Tracer,
    superblock: SuperBlock,
    grid: Grid,
    state_machine: StateMachine,

    pending: Packet.Queue,
    op: u64,
    commit_timestamp_expected: u64,
    state: enum {
        idle,
        prefetch,
    },

    callback: Callback,

    const Callback = *const fn (
        context: *TestContext,
        operation: StateMachine.Operation,
        packet: *Packet,
        timestamp: u64,
        reply_actual: []const u8,
    ) anyerror!void;

    fn init(
        context: *TestContext,
        arena: std.mem.Allocator,
        callback: Callback,
    ) !void {
        context.* = .{
            .arena = arena,

            .accounts = .init(arena),
            .transfers = .init(arena),
            .linked_events_failed = .init(arena),

            .message_pool = undefined,
            .time_sim = undefined,
            .trace = undefined,
            .superblock = undefined,
            .grid = undefined,
            .state_machine = undefined,
            .storage = undefined,

            .pending = Packet.Queue.init(.{
                .name = null,
                .verify_push = true,
            }),

            .op = 1,
            .commit_timestamp_expected = 0,
            .state = .idle,

            .callback = callback,
        };

        context.message_pool = try MessagePool.init(arena, .client);
        context.storage = try fixtures.init_storage(arena, .{ .size = 4096 });

        context.time_sim = fixtures.init_time(.{});
        context.time_sim.ticks = 1;

        context.trace = try fixtures.init_tracer(arena, context.time_sim.time(), .{});

        context.superblock = try fixtures.init_superblock(arena, &context.storage, .{
            .storage_size_limit = data_file_size_min,
        });

        // Pretend that the superblock is open so that the Forest can initialize.
        context.superblock.opened = true;
        context.superblock.working.vsr_state.checkpoint.header.op = 0;

        context.grid = try fixtures.init_grid(arena, &context.trace, &context.superblock, .{});

        try context.state_machine.init(
            arena,
            context.time_sim.time(),
            &context.grid,
            .{
                .batch_size_limit = constants.message_body_size_max,
                .lsm_forest_compaction_block_count = StateMachine.Forest.Options
                    .compaction_block_count_min,
                .lsm_forest_node_count = 1,
                .cache_entries_accounts = 0,
                .cache_entries_transfers = 0,
                .cache_entries_transfers_pending = 0,
                .log_trace = true,
                .aof_recovery = false,
            },
        );

        // Usually, `pulse_next_timestamp` starts in an unknown state,
        // signaling that the state machine needs a `pulse` to scan for
        // pending transfers and correctly determine when to process the
        // next expiry. However, this initial `pulse` unnecessarily bumps
        // time, making unit tests that depend on the `timestamp` harder
        // to reason about.
        //
        // Since this is a newly created state machine, we can bypass the
        // initial check, ensuring that there will be no `timestamp` bumps
        // between operations unless actual pending transfers get expired.
        context.state_machine.expire_pending_transfers
            .pulse_next_timestamp = TimestampRange.timestamp_max;
    }

    fn submit(context: *TestContext, packet_list: *Packet) !void {
        packet_list.assert_phase(.pending);

        const request = context.message_pool.get_message(.request);
        defer context.message_pool.unref(request);

        const batch = packet_list.batch_write(
            StateMachine.Operation,
            std.enums.values(StateMachine.Operation),
            .{
                .output_buffer = request.buffer[@sizeOf(vsr.Header)..],
                .batch_size_limit = context.state_machine.batch_size_limit,
            },
        );

        const reply = context.message_pool.get_message(.request);
        defer context.message_pool.unref(reply);

        const reply_body = context.raw_request(
            batch.operation,
            request.buffer[@sizeOf(vsr.Header)..][0..batch.request_size],
            reply.buffer[@sizeOf(vsr.Header)..],
        );

        try context.raw_reply(packet_list, reply_body);
    }

    fn flush(context: *TestContext) !void {
        assert(context.state == .idle);
        maybe(context.pending.empty());
        defer assert(context.pending.empty());

        const pulse_needed = context.state_machine.pulse_needed(
            context.state_machine.prepare_timestamp,
        );
        maybe(pulse_needed);

        // Pulse is executed in a best-effort manner
        // after committing the current pipelined operation.
        defer if (pulse_needed) context.pulse();

        var operation: ?u8 = null;
        while (context.pending.pop()) |packet_list| {
            // The real client can enqueue batches of different operations,
            // although they are never sent together in the same request.
            // For testing, we always flush between operations, so
            // the pending queue must contain only batches of the same kind.
            if (operation) |current| assert(packet_list.operation == current);
            operation = packet_list.operation;

            try context.submit(packet_list);
        }

        context.commit_timestamp_expected = context.state_machine.prefetch_timestamp;
    }

    fn pulse(context: *TestContext) void {
        assert(context.state == .idle);

        if (context.state_machine.pulse_needed(context.state_machine.prepare_timestamp)) {
            const operation = vsr.Operation.pulse.cast(StateMachine.Operation);
            const reply = context.raw_request(
                operation,
                &.{},
                undefined, // Not used by pulses.
            );
            assert(reply.len == 0);
            context.commit_timestamp_expected = context.state_machine.prepare_timestamp;
        }
    }

    fn raw_request(
        context: *TestContext,
        operation: StateMachine.Operation,
        message_body: []align(constants.cache_line_size) const u8,
        output_buffer: *align(constants.cache_line_size) [constants.message_body_size_max]u8,
    ) []align(constants.cache_line_size) const u8 {
        assert(context.op > 0);
        defer context.op += 1;

        assert(context.state == .idle);
        defer assert(context.state == .idle);

        context.state_machine.prepare(
            operation,
            message_body,
        );
        if (context.state_machine.prepare_timestamp == context.state_machine.commit_timestamp) {
            context.state_machine.prepare_timestamp += 1;
        }
        const timestamp = context.state_machine.prepare_timestamp;
        assert(timestamp > context.state_machine.commit_timestamp);

        context.state = .prefetch;
        context.state_machine.prefetch_timestamp = timestamp;
        context.state_machine.prefetch(
            struct {
                fn callback(state_machine: *StateMachine) void {
                    const ctx: *TestContext = @fieldParentPtr(
                        "state_machine",
                        state_machine,
                    );
                    assert(ctx.state == .prefetch);
                    ctx.state = .idle;
                }
            }.callback,
            context.op,
            context.op,
            operation,
            message_body,
        );
        while (context.state == .prefetch) context.storage.run();

        const client_id: u128 = 1;
        const size = context.state_machine.commit(
            client_id,
            context.op,
            timestamp,
            operation,
            message_body,
            output_buffer,
        );
        return output_buffer[0..size];
    }

    fn raw_reply(
        context: *TestContext,
        packet_list: *Packet,
        reply: []align(constants.cache_line_size) const u8,
    ) !void {
        const batch = try packet_list.batch_validate(
            StateMachine.Operation,
            std.enums.values(StateMachine.Operation),
            .{
                .batch_size_limit = context.state_machine.batch_size_limit,
            },
        );
        assert(batch.result_size > 0);

        if (!batch.operation.is_multi_batch()) {
            assert(packet_list.multi_batch_next == null);
            assert(reply.len % batch.result_size == 0);
            try context.callback(
                context,
                batch.operation,
                packet_list,
                context.state_machine.prepare_timestamp,
                reply,
            );
            return;
        }
        assert(batch.operation.is_multi_batch());

        var reply_decoder = try MultiBatchDecoder.init(reply, .{
            .element_size = batch.result_size,
        });
        assert(packet_list.multi_batch_count == reply_decoder.batch_count());

        // Copying it because `packet` is no longer valid after the callback.
        const multi_batch_result_count_expected: u32 =
            packet_list.multi_batch_result_count_expected;

        var multi_batch_results_actual: u16 = 0;
        var it: ?*Packet = packet_list;
        while (it) |packet_next| {
            if (packet_next != packet_list) packet_next.assert_phase(.batched);
            assert(packet_next.operation == @intFromEnum(batch.operation));

            it = packet_next.multi_batch_next;

            const batched_reply: []const u8 = reply_decoder.pop().?;
            multi_batch_results_actual += @intCast(@divExact(
                batched_reply.len,
                batch.result_size,
            ));
            try context.callback(
                context,
                batch.operation,
                packet_next,
                context.state_machine.prepare_timestamp,
                batched_reply,
            );
        }
        assert(reply_decoder.pop() == null);
        assert(multi_batch_results_actual <= multi_batch_result_count_expected);
    }

    fn get_account_from_cache(context: *TestContext, id: u128) ?Account {
        return switch (context.state_machine.forest.grooves.accounts.get(id)) {
            .found_object => |object| object,
            .not_found => null,
        };
    }
};

test "create_accounts" {
    try check(
        \\ account A1  0  0  0  0 U2 U2 U2 _ L3 C4 _   _   _ _ _ _ _ _ created
        \\ account A0  1  1  1  1  _  _  _ 1 L0 C0 _ D<C C<D _ _ _ 1 1 timestamp_must_be_zero
        \\ account A0  1  1  1  1  _  _  _ 1 L0 C0 _ D<C C<D _ _ _ 1 _ reserved_field
        \\ account A0  1  1  1  1  _  _  _ _ L0 C0 _ D<C C<D _ _ _ 1 _ reserved_flag
        \\ account A0  1  1  1  1  _  _  _ _ L0 C0 _ D<C C<D _ _ _ _ _ id_must_not_be_zero
        \\ account -0  1  1  1  1  _  _  _ _ L0 C0 _ D<C C<D _ _ _ _ _ id_must_not_be_int_max
        \\ account A1  0  0  0  0 U1 U1 U1 _ L9 C9 _ D<C   _ _ _ _ _ _ exists_with_different_flags
        \\ account A1  0  0  0  0 U1 U1 U1 _ L9 C9 _   _ C<D _ _ _ _ _ exists_with_different_flags
        \\ account A1  0  0  0  0 U1 U1 U1 _ L9 C9 _   _   _ _ _ _ _ _ exists_with_different_user_data_128
        \\ account A1  0  0  0  0 U2 U1 U1 _ L9 C9 _   _   _ _ _ _ _ _ exists_with_different_user_data_64
        \\ account A1  0  0  0  0 U2 U2 U1 _ L9 C9 _   _   _ _ _ _ _ _ exists_with_different_user_data_32
        \\ account A1  0  0  0  0 U2 U2 U2 _ L9 C9 _   _   _ _ _ _ _ _ exists_with_different_ledger
        \\ account A1  0  0  0  0 U2 U2 U2 _ L3 C9 _   _   _ _ _ _ _ _ exists_with_different_code
        \\ account A1  0  0  0  0 U2 U2 U2 _ L3 C4 _   _   _ _ _ _ _ _ exists
        \\ account A2  1  1  1  1 U1 U1 U1 _ L0 C0 _ D<C C<D _ _ _ _ _ flags_are_mutually_exclusive
        \\ account A2  1  1  1  1 U1 U1 U1 _ L9 C9 _ D<C   _ _ _ _ _ _ debits_pending_must_be_zero
        \\ account A2  0  1  1  1 U1 U1 U1 _ L9 C9 _ D<C   _ _ _ _ _ _ debits_posted_must_be_zero
        \\ account A2  0  0  1  1 U1 U1 U1 _ L9 C9 _ D<C   _ _ _ _ _ _ credits_pending_must_be_zero
        \\ account A2  0  0  0  1 U1 U1 U1 _ L9 C9 _ D<C   _ _ _ _ _ _ credits_posted_must_be_zero
        \\ account A2  0  0  0  0 U1 U1 U1 _ L0 C0 _ D<C   _ _ _ _ _ _ ledger_must_not_be_zero
        \\ account A2  0  0  0  0 U1 U1 U1 _ L9 C0 _ D<C   _ _ _ _ _ _ code_must_not_be_zero
        \\ commit create_accounts
        \\
        \\ lookup_account -0 _
        \\ lookup_account A0 _
        \\ lookup_account A1 0 0 0 0 _
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
        \\ account A7  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created // An individual event (successful):

        // A chain of 4 events (the last event in the chain closes the chain with linked=false):
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ _ _ _ linked_event_failed // Commit/rollback.
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ _ _ _ linked_event_failed // Commit/rollback.
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ _ _ _ exists              // Fail with .exists.
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ linked_event_failed // Fail without committing.

        // An individual event (successful):
        // This does not see any effect from the failed chain above.
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created

        // A chain of 2 events (the first event fails the chain):
        \\ account A1  0  0  0  0  _  _  _ _ L1 C2 LNK   _   _ _ _ _ _ _ exists_with_different_flags
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ linked_event_failed

        // An individual event (successful):
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created

        // A chain of 2 events (the last event fails the chain):
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ _ _ _ linked_event_failed
        \\ account A1  0  0  0  0  _  _  _ _ L2 C1   _   _   _ _ _ _ _ _ exists_with_different_ledger

        // A chain of 2 events (successful):
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ _ _ _ created
        \\ account A4  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ commit create_accounts
        \\
        \\ lookup_account A7 0 0 0 0 _
        \\ lookup_account A1 0 0 0 0 _
        \\ lookup_account A2 0 0 0 0 _
        \\ lookup_account A3 0 0 0 0 _
        \\ lookup_account A4 0 0 0 0 _
        \\ commit lookup_accounts
    );

    try check(
        \\ account A7  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created // An individual event (successful):

        // A chain of 4 events:
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ _ _ _ linked_event_failed // Commit/rollback.
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ _ _ _ linked_event_failed // Commit/rollback.
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ _ _ _ exists              // Fail with .exists.
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ linked_event_failed // Fail without committing.
        \\ commit create_accounts
        \\
        \\ lookup_account A7 0 0 0 0 _
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ _ _ _ created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ _ _ _ created
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created

        // An open chain of 2 events:
        \\ account A4  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ _ _ _ linked_event_failed
        \\ account A5  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ _ _ _ linked_event_chain_open
        \\ commit create_accounts
        \\
        \\ lookup_account A1 0 0 0 0 _
        \\ lookup_account A2 0 0 0 0 _
        \\ lookup_account A3 0 0 0 0 _
        \\ lookup_account A4 _
        \\ lookup_account A5 _
        \\ commit lookup_accounts
    );
}

test "linked_event_chain_open for an already failed batch" {
    try check(
        // An individual event (successful):
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created

        // An open chain of 3 events (the second one fails):
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ _ _ _ linked_event_failed
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ _ _ _ exists_with_different_flags
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ _ _ _ linked_event_chain_open
        \\ commit create_accounts
        \\
        \\ lookup_account A1 0 0 0 0 _
        \\ lookup_account A2 _
        \\ lookup_account A3 _
        \\ commit lookup_accounts
    );
}

test "linked_event_chain_open for a batch of 1" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ _ _ _ linked_event_chain_open
        \\ commit create_accounts
        \\
        \\ lookup_account A1 _
        \\ commit lookup_accounts
    );
}

// The goal is to ensure that:
// 1. all CreateTransferStatus enums are covered, with
// 2. enums tested in the order that they are defined, for easier auditing of coverage, and that
// 3. state machine logic cannot be reordered in any way, breaking determinism.
test "create_transfers/lookup_transfers" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ account A2  0  0  0  0  _  _  _ _ L2 C2   _   _   _ _ _ _ _ _ created
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ account A4  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ created
        \\ account A5  0  0  0  0  _  _  _ _ L1 C1   _   _ C<D _ _ _ _ _ created
        \\ commit create_accounts

        // Set up initial balances.
        \\ setup A1  100   200    0     0
        \\ setup A2    0     0    0     0
        \\ setup A3    0     0  110   210
        \\ setup A4   20  -700    0  -500
        \\ setup A5    0 -1000   10 -1100

        // Bump the state machine time to `maxInt - 3s` for testing timeout overflow.
        \\ tick -3 seconds

        // Test errors by descending precedence.
        \\ transfer   T0 A0 A0    9  T1  _  _  _    _ L0 C0   _ PEN   _   _   _   _ _ _    _   P1 1 timestamp_must_be_zero
        \\ transfer   T0 A0 A0    9  T1  _  _  _    _ L0 C0   _ PEN   _   _   _   _ _ _    _   P1 _ reserved_flag
        \\ transfer   T0 A0 A0    9  T1  _  _  _    _ L0 C0   _ PEN   _   _   _   _ _  _   _   _ _ id_must_not_be_zero
        \\ transfer   -0 A0 A0    9  T1  _  _  _    _ L0 C0   _ PEN   _   _   _   _ _  _   _   _ _ id_must_not_be_int_max
        \\ transfer   T1 A0 A0    9  T1  _  _  _    _ L0 C0   _ PEN   _   _   _   _ _  _   _   _ _ debit_account_id_must_not_be_zero
        \\ transfer   T1 -0 A0    9  T1  _  _  _    _ L0 C0   _ PEN   _   _   _   _ _  _   _   _ _ debit_account_id_must_not_be_int_max
        \\ transfer   T1 A8 A0    9  T1  _  _  _    _ L0 C0   _ PEN   _   _   _   _ _  _   _   _ _ credit_account_id_must_not_be_zero
        \\ transfer   T1 A8 -0    9  T1  _  _  _    _ L0 C0   _ PEN   _   _   _   _ _  _   _   _ _ credit_account_id_must_not_be_int_max
        \\ transfer   T1 A8 A8    9  T1  _  _  _    _ L0 C0   _ PEN   _   _   _   _ _  _   _   _ _ accounts_must_be_different
        \\ transfer   T1 A8 A9    9  T1  _  _  _    _ L0 C0   _ PEN   _   _   _   _ _  _   _   _ _ pending_id_must_be_zero
        \\ transfer   T1 A8 A9    9   _  _  _  _    1 L0 C0   _   _   _   _   _   _ _  _   _   _ _ timeout_reserved_for_pending_transfer
        \\ transfer   T1 A8 A9    9   _  _  _  _    _ L0 C0   _   _   _   _   _   _ _  CDR _   _ _ closing_transfer_must_be_pending
        \\ transfer   T1 A8 A9    9   _  _  _  _    _ L0 C0   _   _   _   _   _   _ _  _   CCR _ _ closing_transfer_must_be_pending
        \\ transfer   T1 A8 A9    9   _  _  _  _    _ L0 C0   _ PEN   _   _   _   _ _  _   _   _ _ ledger_must_not_be_zero
        \\ transfer   T1 A8 A9    9   _  _  _  _    _ L9 C0   _ PEN   _   _   _   _ _  _   _   _ _ code_must_not_be_zero
        // `debit_account_not_found` is a transient error, T1 cannot be reused:
        \\ transfer   T1 A8 A9    9   _  _  _  _    _ L9 C1   _ PEN   _   _   _   _ _  _   _   _ _ debit_account_not_found
        \\ transfer   T1 A1 A3  123   _  _  _  _    _ L1 C1   _ _     _   _   _   _ _  _   _   _ _ id_already_failed
        // `credit_account_not_found` is a transient error, T2 cannot be reused:
        \\ transfer   T2 A1 A9    9   _  _  _  _    _ L9 C1   _ PEN   _   _   _   _ _  _   _   _ _ credit_account_not_found
        \\ transfer   T2 A1 A3  123   _  _  _  _    _ L1 C1   _ _     _   _   _   _ _  _   _   _ _ id_already_failed
        \\ commit create_transfers
        \\
        \\ transfer   T3 A1 A2    1   _  _  _  _    _ L9 C1   _ PEN   _   _   _   _ _  _   _   _ _ accounts_must_have_the_same_ledger
        \\ transfer   T3 A1 A3    1   _  _  _  _    _ L9 C1   _ PEN   _   _   _   _ _  _   _   _ _ transfer_must_have_the_same_ledger_as_accounts
        \\ transfer   T3 A1 A3  -99   _  _  _  _    _ L1 C1   _ PEN   _   _   _   _ _  _   _   _ _ overflows_debits_pending  // amount = max - A1.debits_pending + 1
        \\ transfer   T3 A1 A3 -109   _  _  _  _    _ L1 C1   _ PEN   _   _   _   _ _  _   _   _ _ overflows_credits_pending // amount = max - A3.credits_pending + 1
        \\ transfer   T3 A1 A3 -199   _  _  _  _    _ L1 C1   _ PEN   _   _   _   _ _  _   _   _ _ overflows_debits_posted   // amount = max - A1.debits_posted + 1
        \\ transfer   T3 A1 A3 -209   _  _  _  _    _ L1 C1   _ PEN   _   _   _   _ _  _   _   _ _ overflows_credits_posted  // amount = max - A3.credits_posted + 1
        \\ transfer   T3 A1 A3 -299   _  _  _  _    _ L1 C1   _ PEN   _   _   _   _ _  _   _   _ _ overflows_debits          // amount = max - A1.debits_pending - A1.debits_posted + 1
        \\ transfer   T3 A1 A3 -319   _  _  _  _    _ L1 C1   _ PEN   _   _   _   _ _  _   _   _ _ overflows_credits         // amount = max - A3.credits_pending - A3.credits_posted + 1
        \\ transfer   T3 A4 A5  199   _  _  _  _  999 L1 C1   _ PEN   _   _   _   _ _  _   _   _ _ overflows_timeout
        // `exceeds_credits` is a transient error, T3 cannot be reused:
        \\ transfer   T3 A4 A5  199   _  _  _  _    _ L1 C1   _   _   _   _   _   _ _  _   _   _ _ exceeds_credits           // amount = A4.credits_posted - A4.debits_pending - A4.debits_posted + 1
        \\ transfer   T3 A1 A3  123   _  _  _  _    _ L1 C1   _ _     _   _   _   _ _  _   _   _ _ id_already_failed
        // `exceeds_debits` is a transient error, T4 cannot be reused:
        \\ transfer   T4 A4 A5   91   _  _  _  _    _ L1 C1   _   _   _   _   _   _ _  _   _   _ _ exceeds_debits            // amount = A5.debits_posted - A5.credits_pending - A5.credits_posted + 1
        \\ transfer   T4 A1 A3  123   _  _  _  _    _ L1 C1   _ _     _   _   _   _ _  _   _   _ _ id_already_failed
        \\
        \\ transfer   T5 A1 A3  123   _  _  _  _    1 L1 C1   _ PEN   _   _   _   _ _  _   _   _ _ created
        \\ commit create_transfers

        // Ensure that idempotence is checked first:
        \\ transfer   T5 A1 A3  123   _  _  _  _    1 L2 C1   _ PEN   _   _   _   _ _  _ _ _ _ exists_with_different_ledger
        \\ transfer   T5 A1 A3   -0   _ U1 U1 U1    _ L1 C2   _   _   _   _   _   _ _  _ _ _ _ exists_with_different_flags
        \\ transfer   T5 A3 A1   -0   _ U1 U1 U1    1 L1 C2   _ PEN   _   _   _   _ _  _ _ _ _ exists_with_different_debit_account_id
        \\ transfer   T5 A1 A4   -0   _ U1 U1 U1    1 L1 C2   _ PEN   _   _   _   _ _  _ _ _ _ exists_with_different_credit_account_id
        \\ transfer   T5 A1 A3   -0   _ U1 U1 U1    1 L1 C1   _ PEN   _   _   _   _ _  _ _ _ _ exists_with_different_amount
        \\ transfer   T5 A1 A3  123   _ U1 U1 U1    1 L1 C2   _ PEN   _   _   _   _ _  _ _ _ _ exists_with_different_user_data_128
        \\ transfer   T5 A1 A3  123   _  _ U1 U1    1 L1 C2   _ PEN   _   _   _   _ _  _ _ _ _ exists_with_different_user_data_64
        \\ transfer   T5 A1 A3  123   _  _  _ U1    1 L1 C2   _ PEN   _   _   _   _ _  _ _ _ _ exists_with_different_user_data_32
        \\ transfer   T5 A1 A3  123   _  _  _  _    2 L1 C2   _ PEN   _   _   _   _ _  _ _ _ _ exists_with_different_timeout
        \\ transfer   T5 A1 A3  123   _  _  _  _    1 L1 C2   _ PEN   _   _   _   _ _  _ _ _ _ exists_with_different_code
        \\ transfer   T5 A1 A3  123   _  _  _  _    1 L1 C1   _ PEN   _   _   _   _ _  _ _ _ _ exists
        \\
        \\ transfer   T6 A3 A1    7   _  _  _  _    _ L1 C2   _   _   _   _   _   _ _  _ _ _ _ created
        \\ transfer   T7 A1 A3    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _ _  _ _ _ _ created
        \\ transfer   T8 A1 A3    0   _  _  _  _    _ L1 C2   _   _   _   _   _   _ _  _ _ _ _ created
        \\ commit create_transfers
        \\
        \\ lookup_account A1 223 203   0   7  _
        \\ lookup_account A3   0   7 233 213  _
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T5 exists true
        \\ lookup_transfer T6 exists true
        \\ lookup_transfer T7 exists true
        \\ lookup_transfer T8 exists true
        \\ lookup_transfer -0 exists false
        \\ commit lookup_transfers
    );
}

test "create/lookup 2-phase transfers" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ commit create_accounts

        // First phase.
        \\ transfer   T1 A1 A2   15   _  _  _  _    _ L1 C1   _   _   _   _   _   _  _ _ _ _ _ created // Not pending!
        \\ transfer   T2 A1 A2   15   _  _  _  _ 1000 L1 C1   _ PEN   _   _   _   _  _ _ _ _ _ created
        \\ transfer   T3 A1 A2   15   _  _  _  _   50 L1 C1   _ PEN   _   _   _   _  _ _ _ _ _ created
        \\ transfer   T4 A1 A2   15   _  _  _  _    1 L1 C1   _ PEN   _   _   _   _  _ _ _ _ _ created
        \\ transfer   T5 A1 A2    7   _ U9 U9 U9   50 L1 C1   _ PEN   _   _   _   _  _ _ _ _ _ created
        \\ transfer   T6 A1 A2    1   _  _  _  _    0 L1 C1   _ PEN   _   _   _   _  _ _ _ _ _ created
        \\ transfer   T7 A1 A2    1   _  _  _  _    0 L1 C1   _ PEN   _   _   _   _  _ _ _ _ _ created
        \\ commit create_transfers

        // Check balances before resolving.
        \\ lookup_account A1 54 15  0  0  _
        \\ lookup_account A2  0  0 54 15  _
        \\ commit lookup_accounts

        // Bump the state machine time in +1s for testing the timeout expiration.
        \\ tick 1 seconds

        // Second phase.
        \\ transfer T101 A1 A2   13  T2 U1 U1 U1    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ created
        \\ transfer   T0 A8 A9   16  T0 U2 U2 U2   50 L6 C7   _ PEN POS VOI   _   _  _ _ _ _ 1 timestamp_must_be_zero
        \\ transfer   T0 A8 A9   16  T0 U2 U2 U2   50 L6 C7   _ PEN POS VOI   _   _  _ _ _ _ _ id_must_not_be_zero
        \\ transfer   -0 A8 A9   16  T0 U2 U2 U2   50 L6 C7   _ PEN POS VOI   _   _  _ _ _ _ _ id_must_not_be_int_max
        \\ transfer T101 A1 A2   15  T3 U2 U2 U2    _ L1 C1   _   _   _ VOI   _   _  _ _ _ _ _ exists_with_different_flags
        \\ transfer T101 A1 A2   14  T2 U1 U1 U1    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ exists_with_different_amount
        \\ transfer T101 A1 A2    _  T2 U1 U1 U1    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ exists_with_different_amount
        \\ transfer T101 A1 A2   13  T3 U2 U2 U2    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ exists_with_different_pending_id
        \\ transfer T101 A1 A2   13  T2 U2 U2 U2    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ exists_with_different_user_data_128
        \\ transfer T101 A1 A2   13  T2 U1 U2 U2    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ exists_with_different_user_data_64
        \\ transfer T101 A1 A2   13  T2 U1 U1 U2    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ exists_with_different_user_data_32
        \\ transfer T101 A1 A2   13  T2 U1 U1 U1    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ exists
        \\ transfer T102 A8 A9   16  T0 U2 U2 U2   50 L6 C7   _ PEN POS VOI   _   _  _ _ _ _ _ flags_are_mutually_exclusive
        \\ transfer T102 A8 A9   16  T0 U2 U2 U2   50 L6 C7   _ PEN POS VOI BDR   _  _ _ _ _ _ flags_are_mutually_exclusive
        \\ transfer T102 A8 A9   16  T0 U2 U2 U2   50 L6 C7   _ PEN POS VOI BDR BCR  _ _ _ _ _ flags_are_mutually_exclusive
        \\ transfer T102 A8 A9   16  T0 U2 U2 U2   50 L6 C7   _ PEN POS VOI   _ BCR  _ _ _ _ _ flags_are_mutually_exclusive
        \\ transfer T102 A8 A9   16  T0 U2 U2 U2   50 L6 C7   _ PEN   _ VOI   _   _  _ _ _ _ _ flags_are_mutually_exclusive
        \\ transfer T102 A8 A9   16  T0 U2 U2 U2   50 L6 C7   _   _   _ VOI BDR   _  _ _ _ _ _ flags_are_mutually_exclusive
        \\ transfer T102 A8 A9   16  T0 U2 U2 U2   50 L6 C7   _   _   _ VOI BDR BCR  _ _ _ _ _ flags_are_mutually_exclusive
        \\ transfer T102 A8 A9   16  T0 U2 U2 U2   50 L6 C7   _   _   _ VOI   _ BCR  _ _ _ _ _ flags_are_mutually_exclusive
        \\ transfer T102 A8 A9   16  T0 U2 U2 U2   50 L6 C7   _   _ POS   _ BDR   _  _ _ _ _ _ flags_are_mutually_exclusive
        \\ transfer T102 A8 A9   16  T0 U2 U2 U2   50 L6 C7   _   _ POS   _ BDR BCR  _ _ _ _ _ flags_are_mutually_exclusive
        \\ transfer T102 A8 A9   16  T0 U2 U2 U2   50 L6 C7   _   _ POS   _   _ BCR  _ _ _ _ _ flags_are_mutually_exclusive
        \\ transfer T102 A8 A9   16  T0 U2 U2 U2   50 L6 C7   _   _   _ VOI   _   _  _ _ _ _ _ pending_id_must_not_be_zero
        \\ transfer T102 A8 A9   16  -0 U2 U2 U2   50 L6 C7   _   _   _ VOI   _   _  _ _ _ _ _ pending_id_must_not_be_int_max
        \\ transfer T102 A8 A9   16 102 U2 U2 U2   50 L6 C7   _   _   _ VOI   _   _  _ _ _ _ _ pending_id_must_be_different
        \\ transfer T102 A8 A9   16 103 U2 U2 U2   50 L6 C7   _   _   _ VOI   _   _  _ _ _ _ _ timeout_reserved_for_pending_transfer
        \\ commit create_transfers

        // `pending_transfer_not_found` is a transient error, T102 cannot be reused:
        \\ transfer T102 A8 A9   16 103 U2 U2 U2    _ L6 C7   _   _   _ VOI   _   _  _ _ _ _ _ pending_transfer_not_found
        \\ transfer T102 A1 A2   13   _ U1 U1 U1    _ L1 C1   _   _   _   _   _   _  _ _ _ _ _ id_already_failed
        \\
        \\ transfer T103 A8 A9   16  T1 U2 U2 U2    _ L6 C7   _   _   _ VOI   _   _  _ _ _ _ _ pending_transfer_not_pending
        \\ transfer T103 A8 A9   16  T3 U2 U2 U2    _ L6 C7   _   _   _ VOI   _   _  _ _ _ _ _ pending_transfer_has_different_debit_account_id
        \\ transfer T103 A1 A9   16  T3 U2 U2 U2    _ L6 C7   _   _   _ VOI   _   _  _ _ _ _ _ pending_transfer_has_different_credit_account_id
        \\ transfer T103 A1 A2   16  T3 U2 U2 U2    _ L6 C7   _   _   _ VOI   _   _  _ _ _ _ _ pending_transfer_has_different_ledger
        \\ transfer T103 A1 A2   16  T3 U2 U2 U2    _ L1 C7   _   _   _ VOI   _   _  _ _ _ _ _ pending_transfer_has_different_code
        \\ transfer T103 A1 A2   16  T3 U2 U2 U2    _ L1 C1   _   _   _ VOI   _   _  _ _ _ _ _ exceeds_pending_transfer_amount
        \\ transfer T103 A1 A2   14  T3 U2 U2 U2    _ L1 C1   _   _   _ VOI   _   _  _ _ _ _ _ pending_transfer_has_different_amount
        \\ transfer T103 A1 A2   13  T2 U1 U1 U1    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ pending_transfer_already_posted
        \\ transfer T103 A1 A2   15  T3 U1 U1 U1    _ L1 C1   _   _   _ VOI   _   _  _ _ _ _ _ created
        \\ transfer T104 A1 A2   13  T3 U1 U1 U1    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ pending_transfer_already_voided
        \\ transfer T104 A1 A2   15  T4 U1 U1 U1    _ L1 C1   _   _   _ VOI   _   _  _ _ _ _ _ pending_transfer_expired
        \\ commit create_transfers

        // Transfers posted/voided with optional fields must not raise `exists_with_different_*`.
        // But transfers posted with posted.amount≠pending.amount may return
        // exists_with_different_amount.
        \\ transfer T101 A0 A0   14  T2 U0 U0 U0    _ L0 C0   _   _ POS   _   _   _  _ _ _ _ _ exists_with_different_amount // t.amount > e.amount
        \\ transfer T101 A0 A0   14  T2 U0 U0 U0    _ L0 C0   _   _ POS   _   _   _  _ _ _ _ _ exists_with_different_amount
        \\ transfer T101 A0 A0   12  T2 U0 U0 U0    _ L0 C0   _   _ POS   _   _   _  _ _ _ _ _ exists_with_different_amount // t.amount < e.amount
        \\
        \\ transfer T105 A0 A0    8  T5 U0 U0 U0    _ L0 C0   _   _ POS   _   _   _  _ _ _ _ _ exceeds_pending_transfer_amount // t.amount > p.amount
        \\ transfer T105 A0 A0   -0  T5 U0 U0 U0    _ L0 C0   _   _ POS   _   _   _  _ _ _ _ _ created
        \\ transfer T105 A0 A0    7  T5 U0 U0 U0    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ exists
        \\ transfer T105 A0 A0    7  T5 U0 U0 U0    _ L0 C0   _   _ POS   _   _   _  _ _ _ _ _ exists // ledger/code = 0
        \\ transfer T105 A0 A0   -0  T5 U0 U0 U0    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ exists // amount = max
        \\ transfer T105 A0 A0    8  T5 U0 U0 U0    _ L0 C0   _   _ POS   _   _   _  _ _ _ _ _ exists_with_different_amount // t.amount > p.amount
        \\ transfer T105 A0 A0    6  T5 U0 U0 U0    _ L0 C0   _   _ POS   _   _   _  _ _ _ _ _ exists_with_different_amount // t.amount < e.amount
        \\ transfer T105 A0 A0    0  T5 U0 U0 U0    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ exists_with_different_amount
        \\
        \\ transfer T106 A0 A0   -1  T6 U0 U0 U0    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ exceeds_pending_transfer_amount
        \\ transfer T106 A0 A0   -0  T6 U0 U0 U0    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ created
        \\ transfer T106 A0 A0   -0  T6 U0 U0 U0    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ exists
        \\ transfer T106 A0 A0    1  T6 U0 U0 U0    _ L0 C0   _   _ POS   _   _   _  _ _ _ _ _ exists
        \\ transfer T106 A0 A0    2  T6 U0 U0 U0    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ exists_with_different_amount // t.amount > p.amount
        \\ transfer T106 A0 A0    0  T6 U0 U0 U0    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ exists_with_different_amount // t.amount < p.amount
        \\
        \\ transfer T107 A0 A0    0  T7 U0 U0 U0    _ L0 C0   _   _ POS   _   _   _  _ _ _ _ _ created
        \\ transfer T107 A0 A0    0  T7 U0 U0 U0    _ L0 C0   _   _ POS   _   _   _  _ _ _ _ _ exists
        \\ transfer T107 A0 A0    1  T7 U0 U0 U0    _ L0 C0   _   _ POS   _   _   _  _ _ _ _ _ exists_with_different_amount // t.amount > e.amount
        \\ commit create_transfers

        // Check balances after resolving.
        \\ lookup_account A1  0 36  0  0  _
        \\ lookup_account A2  0  0  0 36  _
        \\ commit lookup_accounts

        // The posted transfer amounts are set to the actual amount posted (which may be less than
        // the "client" set as the amount).
        \\ lookup_transfer T101 amount 13
        \\ lookup_transfer T105 amount 7
        \\ lookup_transfer T106 amount 1
        \\ lookup_transfer T107 amount 0
        \\ commit lookup_transfers
    );
}

test "create/lookup 2-phase transfers (amount=maxInt)" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ commit create_accounts

        // Posting maxInt(u128) is a pun – it is interpreted as "send full pending amount", which in
        // this case is exactly maxInt(u127).
        \\ transfer T1 A1 A2   -0   _  _  _  _ _ L1 C1   _ PEN   _   _   _   _  _ _ _ _ _ created
        \\ transfer T2 A1 A2   -0  T1  _  _  _ _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ created
        \\ transfer T2 A1 A2   -0  T1  _  _  _ _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ exists
        \\ commit create_transfers

        // Check balances after resolving.
        \\ lookup_account A1  0 -0  0  0 _
        \\ lookup_account A2  0  0  0 -0 _
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T1 amount -0
        \\ lookup_transfer T2 amount -0
        \\ commit lookup_transfers
    );
}

test "create/lookup expired transfers" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ commit create_accounts

        // First phase.
        \\ transfer   T1 A1 A2   10   _  _  _  _    0 L1 C1   _ PEN   _   _   _   _  _ _ _ _ _ created // Timeout zero will never expire.
        \\ transfer   T2 A1 A2   11   _  _  _  _    1 L1 C1   _ PEN   _   _   _   _  _ _ _ _ _ created
        \\ transfer   T3 A1 A2   12   _  _  _  _    2 L1 C1   _ PEN   _   _   _   _  _ _ _ _ _ created
        \\ transfer   T4 A1 A2   13   _  _  _  _    3 L1 C1   _ PEN   _   _   _   _  _ _ _ _ _ created
        \\ commit create_transfers

        // Check balances before expiration.
        \\ lookup_account A1 46  0  0  0  _
        \\ lookup_account A2  0  0 46  0  _
        \\ commit lookup_accounts

        // Check balances after 1s.
        \\ tick 1 seconds
        \\ lookup_account A1 35  0  0  0  _
        \\ lookup_account A2  0  0 35  0  _
        \\ commit lookup_accounts

        // Check balances after 1s.
        \\ tick 1 seconds
        \\ lookup_account A1 23  0  0  0  _
        \\ lookup_account A2  0  0 23  0  _
        \\ commit lookup_accounts

        // Check balances after 1s.
        \\ tick 1 seconds
        \\ lookup_account A1 10  0  0  0  _
        \\ lookup_account A2  0  0 10  0  _
        \\ commit lookup_accounts

        // Second phase.
        \\ transfer T101 A1 A2   10  T1 U1 U1 U1    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ created
        \\ transfer T102 A1 A2   11  T2 U1 U1 U1    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ pending_transfer_expired
        \\ transfer T103 A1 A2   12  T3 U1 U1 U1    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ pending_transfer_expired
        \\ transfer T104 A1 A2   13  T4 U1 U1 U1    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ pending_transfer_expired
        \\ commit create_transfers

        // Check final balances.
        \\ lookup_account A1  0 10  0  0  _
        \\ lookup_account A2  0  0  0 10  _
        \\ commit lookup_accounts

        // Check transfers.
        \\ lookup_transfer T101 exists true
        \\ lookup_transfer T102 exists false
        \\ lookup_transfer T103 exists false
        \\ lookup_transfer T104 exists false
        \\ commit lookup_transfers
    );
}

test "create_transfers: empty" {
    try check(
        \\ commit create_transfers
    );
}

test "create_transfers/lookup_transfers: failed transfer does not exist" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ commit create_accounts
        \\
        \\ transfer   T1 A1 A2   15   _  _  _  _    _ L1 C1   _   _   _   _   _   _  _ _ _ _ _ created
        \\ transfer   T2 A1 A2   15   _  _  _  _    _ L0 C1   _   _   _   _   _   _  _ _ _ _ _ ledger_must_not_be_zero
        \\ commit create_transfers
        \\
        \\ lookup_account A1 0 15 0  0  _
        \\ lookup_account A2 0  0 0 15  _
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T1 exists true
        \\ lookup_transfer T2 exists false
        \\ commit lookup_transfers
    );
}

test "create_transfers: failed linked-chains are undone" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ commit create_accounts
        \\
        \\ transfer   T1 A1 A2   15   _  _  _  _    _ L1 C1 LNK   _   _   _   _   _  _ _ _ _ _ linked_event_failed
        \\ transfer   T2 A1 A2   15   _  _  _  _    _ L0 C1   _   _   _   _   _   _  _ _ _ _ _ ledger_must_not_be_zero
        \\ commit create_transfers
        \\
        \\ transfer   T3 A1 A2   15   _  _  _  _    1 L1 C1 LNK PEN   _   _   _   _  _ _ _ _ _ linked_event_failed
        \\ transfer   T4 A1 A2   15   _  _  _  _    _ L0 C1   _   _   _   _   _   _  _ _ _ _ _ ledger_must_not_be_zero
        \\ commit create_transfers
        \\
        \\ lookup_account A1 0 0 0 0 _
        \\ lookup_account A2 0 0 0 0 _
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ commit create_accounts
        \\
        \\ setup A1 0 0 0 20
        \\
        \\ transfer   T1 A1 A2   15   _ _   _  _    _ L1 C1 LNK   _   _   _   _   _  _ _ _ _ _ linked_event_failed
        \\ transfer   T2 A1 A2    5   _ _   _  _    _ L0 C1   _   _   _   _   _   _  _ _ _ _ _ ledger_must_not_be_zero
        \\ transfer   T3 A1 A2   15   _ _   _  _    _ L1 C1   _   _   _   _   _   _  _ _ _ _ _ created
        \\ commit create_transfers
        \\
        \\ lookup_account A1 0 15 0 20  _
        \\ lookup_account A2 0  0 0 15  _
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _ C<D _ _ _ _ _ created
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ commit create_accounts
        \\
        \\ setup A1 1  0 0 10
        \\ setup A2 0 10 2  0
        \\
        \\ transfer   T1 A1 A3  3     _  _  _  _    _ L2 C1   _   _   _   _ BDR   _  _ _ _ _ _ transfer_must_have_the_same_ledger_as_accounts
        \\ transfer   T1 A3 A2  3     _  _  _  _    _ L2 C1   _   _   _   _   _ BCR  _ _ _ _ _ transfer_must_have_the_same_ledger_as_accounts
        \\ transfer   T1 A1 A3  3     _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ _ _ _ created
        \\ transfer   T2 A1 A3 13     _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ _ _ _ created
        \\ transfer   T3 A3 A2  3     _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ _ _ _ created
        \\ transfer   T4 A3 A2 13     _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ _ _ _ created
        \\ transfer   T5 A1 A3  1     _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ _ _ _ created // Amount reduced to 0.
        \\ transfer   T6 A1 A3  1     _  _  _  _    _ L1 C1   _   _   _   _ BDR BCR  _ _ _ _ _ created // ↑
        \\ transfer   T7 A3 A2  1     _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ _ _ _ created // ↑
        \\ transfer   T8 A1 A2  1     _  _  _  _    _ L1 C1   _   _   _   _ BDR BCR  _ _ _ _ _ created // ↑
        \\ transfer   T1 A1 A3    2   _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ _ _ _ exists_with_different_amount // Less than the transfer amount.
        \\ transfer   T1 A1 A3    0   _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ _ _ _ exists_with_different_amount // ↑
        \\ transfer   T1 A1 A3    3   _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ _ _ _ exists // Greater-than-or-equal-to the transfer amount.
        \\ transfer   T1 A1 A3    4   _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ _ _ _ exists // ↑
        \\ transfer   T2 A1 A3    6   _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ _ _ _ exists // Equal to the transfer amount.
        \\ transfer   T2 A1 A3    0   _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ _ _ _ exists_with_different_amount // Less than the transfer amount.
        \\ transfer   T3 A3 A2    2   _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ _ _ _ exists_with_different_amount // Less than the transfer amount.
        \\ transfer   T3 A3 A2    0   _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ _ _ _ exists_with_different_amount // ↑
        \\ transfer   T3 A3 A2    3   _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ _ _ _ exists
        \\ transfer   T3 A3 A2    4   _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ _ _ _ exists
        \\ transfer   T4 A3 A2    5   _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ _ _ _ exists // Greater-than-or-equal-to the transfer amount.
        \\ transfer   T4 A3 A2    6   _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ _ _ _ exists // ↑
        \\ transfer   T4 A3 A2    0   _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ _ _ _ exists_with_different_amount // Less than the transfer amount.
        \\ commit create_transfers
        \\
        \\ lookup_account A1 1  9 0 10  _
        \\ lookup_account A2 0 10 2  8  _
        \\ lookup_account A3 0  8 0  9  _
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T1 amount 3
        \\ lookup_transfer T2 amount 6
        \\ lookup_transfer T3 amount 3
        \\ lookup_transfer T4 amount 5
        \\ lookup_transfer T5 amount 0
        \\ lookup_transfer T6 amount 0
        \\ lookup_transfer T7 amount 0
        \\ lookup_transfer T8 amount 0
        \\ commit lookup_transfers
    );
}

test "create_transfers: balancing_debit | balancing_credit (*_must_not_exceed_*, exceeds_*)" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _ C<D _ _ _ _ _ created
        \\ account A4  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ commit create_accounts
        \\
        \\ setup A1 0 0 0 4
        \\ setup A2 0 5 0 0
        \\ setup A3 0 4 0 0
        \\ setup A4 0 0 0 5
        \\
        \\ transfer   T1 A1 A2   10   _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ _ _ _ exceeds_credits
        \\ transfer   T2 A1 A2   10   _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ _ _ _ created
        \\ transfer   T3 A4 A3   10   _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ _ _ _ exceeds_debits
        \\ transfer   T4 A4 A3   10   _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ _ _ _ created
        \\ commit create_transfers
        \\
        \\ lookup_account A1 0 4 0 4 _
        \\ lookup_account A2 0 5 0 4 _
        \\ lookup_account A3 0 4 0 4 _
        \\ lookup_account A4 0 4 0 5 _
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T1 exists false
        \\ lookup_transfer T2 amount 4
        \\ lookup_transfer T3 exists false
        \\ lookup_transfer T4 amount 4
        \\ commit lookup_transfers
    );
}

test "create_transfers: balancing_debit | balancing_credit (¬*_must_not_exceed_*)" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ commit create_accounts
        \\
        \\ setup A1 1  0 0 10
        \\ setup A2 0 10 2  0
        \\
        \\ transfer   T1 A3 A1   99   _  _  _  _    _ L1 C1   _   _   _   _ BDR BCR  _ _ _ _ _ created // Amount reduced to 0.
        \\ transfer   T2 A3 A1   99   _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ _ _ _ created // ↑
        \\ transfer   T3 A2 A3   99   _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ _ _ _ created // ↑
        \\ transfer   T4 A1 A3   99   _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ _ _ _ created
        \\ transfer   T5 A1 A3   99   _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ _ _ _ created // Amount reduced to 0.
        \\ transfer   T6 A3 A2   99   _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ _ _ _ created
        \\ transfer   T7 A3 A2   99   _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ _ _ _ created // Amount reduced to 0.
        \\ commit create_transfers
        \\
        \\ lookup_account A1 1  9 0 10 _
        \\ lookup_account A2 0 10 2  8 _
        \\ lookup_account A3 0  8 0  9 _
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T1 amount 0
        \\ lookup_transfer T2 amount 0
        \\ lookup_transfer T3 amount 0
        \\ lookup_transfer T4 amount 9
        \\ lookup_transfer T5 amount 0
        \\ lookup_transfer T6 amount 8
        \\ lookup_transfer T7 amount 0
        \\ commit lookup_transfers
    );
}

test "create_transfers: balancing_debit | balancing_credit (amount=0)" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _ C<D _ _ _ _ _ created
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _ C<D _ _ _ _ _ created
        \\ account A4  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ commit create_accounts
        \\
        \\ setup A1 1  0 0 10
        \\ setup A2 0 10 2  0
        \\ setup A3 0 10 2  0
        \\
        // Test amount=0 transfers:
        \\ transfer   T1 A1 A4    0   _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ _ _ _ created
        \\ transfer   T2 A4 A2    0   _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ _ _ _ created
        \\ transfer   T3 A1 A4    0   _  _  _  _    _ L1 C1   _   _   _   _ BDR BCR  _ _ _ _ _ created
        \\ transfer   T4 A4 A3    0   _  _  _  _    _ L1 C1   _ PEN   _   _   _ BCR  _ _ _ _ _ created
        // The respective balancing flag reduces nonzero amounts to zero even though A4 lacks
        // must_not_exceed (since its net balance is zero):
        \\ transfer   T5 A4 A1    1   _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ _ _ _ created
        \\ transfer   T6 A2 A4    1   _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ _ _ _ created
        \\ commit create_transfers
        \\
        // None of the accounts' balances have changed -- none of the transfers moved any money.
        \\ lookup_account A1 1  0 0 10 _
        \\ lookup_account A2 0 10 2  0 _
        \\ lookup_account A3 0 10 2  0 _
        \\ lookup_account A4 0  0 0  0 _
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T1 amount 0
        \\ lookup_transfer T2 amount 0
        \\ lookup_transfer T3 amount 0
        \\ lookup_transfer T4 amount 0
        \\ lookup_transfer T5 amount 0
        \\ lookup_transfer T6 amount 0
        \\ commit lookup_transfers
    );
}

test "create_transfers: balancing_debit | balancing_credit (amount=maxInt, balance≈maxInt)" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ created
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _ C<D _ _ _ _ _ created
        \\ account A4  0  0  0  0  _  _  _ _ L1 C1   _   _ C<D _ _ _ _ _ created
        \\ commit create_accounts
        \\
        \\ setup A1 0  0 0 -1
        \\ setup A4 0 -1 0  0
        \\
        \\ transfer   T1 A1 A2   -0   _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ _ _ _ created
        \\ transfer   T2 A3 A4   -0   _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ _ _ _ created
        \\ commit create_transfers
        \\
        \\ lookup_account A1 0 -1  0 -1 _
        \\ lookup_account A2 0  0  0 -1 _
        \\ lookup_account A3 0 -1  0  0 _
        \\ lookup_account A4 0 -1  0 -1 _
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T1 amount -1
        \\ lookup_transfer T2 amount -1
        \\ commit lookup_transfers
    );
}

test "create_transfers: balancing_debit & balancing_credit" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _ C<D _ _ _ _ _ created
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ commit create_accounts
        \\
        \\ setup A1 0  0 0 20
        \\ setup A2 0 10 0  0
        \\ setup A3 0 99 0  0
        \\
        \\ transfer   T1 A1 A2    1   _  _  _  _    _ L1 C1   _   _   _   _ BDR BCR  _ _ _ _ _ created
        \\ transfer   T2 A1 A2   12   _  _  _  _    _ L1 C1   _   _   _   _ BDR BCR  _ _ _ _ _ created
        \\ transfer   T3 A1 A2    1   _  _  _  _    _ L1 C1   _   _   _   _ BDR BCR  _ _ _ _ _ created // Amount reduced to 0.
        \\ transfer   T4 A1 A3   12   _  _  _  _    _ L1 C1   _   _   _   _ BDR BCR  _ _ _ _ _ created
        \\ transfer   T5 A1 A3    1   _  _  _  _    _ L1 C1   _   _   _   _ BDR BCR  _ _ _ _ _ created // Amount reduced to 0.
        \\ commit create_transfers
        \\
        \\ lookup_account A1 0 20 0 20  _
        \\ lookup_account A2 0 10 0 10  _
        \\ lookup_account A3 0 99 0 10  _
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T1 amount  1
        \\ lookup_transfer T2 amount  9
        \\ lookup_transfer T3 amount  0
        \\ lookup_transfer T4 amount 10
        \\ lookup_transfer T5 amount  0
        \\ commit lookup_transfers
    );
}

test "create_transfers: balancing_debit/balancing_credit + pending" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _ C<D _ _ _ _ _ created
        \\ commit create_accounts
        \\
        \\ setup A1 0  0 0 10
        \\ setup A2 0 10 0  0
        \\
        \\ transfer   T1 A1 A2    3   _  _  _  _    _ L1 C1   _ PEN   _   _ BDR   _  _ _ _ _ _ created
        \\ transfer   T2 A1 A2   13   _  _  _  _    _ L1 C1   _ PEN   _   _ BDR   _  _ _ _ _ _ created
        \\ transfer   T3 A1 A2    1   _  _  _  _    _ L1 C1   _ PEN   _   _ BDR   _  _ _ _ _ _ created // Amount reduced to 0.
        \\ commit create_transfers
        \\
        \\ lookup_account A1 10  0  0 10  _
        \\ lookup_account A2  0 10 10  0  _
        \\ commit lookup_accounts
        \\
        \\ transfer   T4 A1 A2    3  T1  _  _  _    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ created
        \\ transfer   T5 A1 A2    5  T2  _  _  _    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ created
        \\ commit create_transfers
        \\
        \\ lookup_transfer T1 amount  3
        \\ lookup_transfer T2 amount  7
        \\ lookup_transfer T3 amount  0
        \\ lookup_transfer T4 amount  3
        \\ lookup_transfer T5 amount  5
        \\ commit lookup_transfers
    );
}

test "create_transfers: multiple debits, single credit, balancing debits" {
    // See `recipes/multi-debit-credit-transfers.md`.
    // Source accounts:     A1, A2, A3
    // Control account:     A4
    // Limit account:       A5
    // Destination account: A6

    // Sufficient funds:
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ created
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ created
        \\ account A4  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ account A5  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ created
        \\ account A6  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ commit create_accounts
        \\
        \\ setup A1 0 0 0 40
        \\ setup A2 0 0 0 40
        \\ setup A3 0 0 0 21
        \\
        \\ transfer T1 A4 A5  100   _  _  _  _    0 L1 C1 LNK _ _ _   _   _ _ _ _ _ _ created
        \\ transfer T2 A1 A4  100   _  _  _  _    0 L1 C1 LNK _ _ _ BDR BCR _ _ _ _ _ created
        \\ transfer T3 A2 A4  100   _  _  _  _    0 L1 C1 LNK _ _ _ BDR BCR _ _ _ _ _ created
        \\ transfer T4 A3 A4  100   _  _  _  _    0 L1 C1 LNK _ _ _ BDR BCR _ _ _ _ _ created
        \\ transfer T5 A4 A6  100   _  _  _  _    0 L1 C1 LNK _ _ _   _   _ _ _ _ _ _ created
        \\ transfer T6 A5 A4   -0   _  _  _  _    0 L1 C1   _ _ _ _   _ BCR _ _ _ _ _ created
        \\ commit create_transfers
        \\
        \\ lookup_account A1 0  40 0  40 _
        \\ lookup_account A2 0  40 0  40 _
        \\ lookup_account A3 0  20 0  21 _
        \\ lookup_account A4 0 200 0 200 _
        \\ lookup_account A5 0 100 0 100 _
        \\ lookup_account A6 0   0 0 100 _
        \\ commit lookup_accounts
    );

    // Insufficient funds.
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ created
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ created
        \\ account A4  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ account A5  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ created
        \\ account A6  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ commit create_accounts
        \\
        \\ setup A1 0 0 0 40
        \\ setup A2 0 0 0 40
        \\ setup A3 0 0 0 19
        \\
        \\ transfer T1 A4 A5  100   _  _  _  _    0 L1 C1 LNK _ _ _   _   _ _ _ _ _ _ linked_event_failed
        \\ transfer T2 A1 A4  100   _  _  _  _    0 L1 C1 LNK _ _ _ BDR BCR _ _ _ _ _ linked_event_failed
        \\ transfer T3 A2 A4  100   _  _  _  _    0 L1 C1 LNK _ _ _ BDR BCR _ _ _ _ _ linked_event_failed
        \\ transfer T4 A3 A4  100   _  _  _  _    0 L1 C1 LNK _ _ _ BDR BCR _ _ _ _ _ linked_event_failed
        \\ transfer T5 A4 A6  100   _  _  _  _    0 L1 C1 LNK _ _ _   _   _ _ _ _ _ _ linked_event_failed
        \\ transfer T6 A5 A4   -0   _  _  _  _    0 L1 C1   _ _ _ _   _ BCR _ _ _ _ _ exceeds_credits
        \\ commit create_transfers
    );
}

test "create_transfers: per-transfer balance invariant" {
    // Temporarily enforce `credits_must_not_exceed_debits` on account `A2`.
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _ C<D _ _ _ _ _ created
        \\ commit create_accounts
        \\
        \\ setup A2 0 40 0 0
        \\
        \\ transfer T1 A1 A2 41   _  _  _  _    0 L1 C1 LNK   _ _   _   _   _ _ _ _ _ _ linked_event_failed
        \\ transfer T2 A2 A3  1   _  _  _  _    0 L1 C1 LNK PEN _   _ BDR   _ _ _ _ _ _ exceeds_debits
        \\ transfer T3 A2 A3  0  T2  _  _  _    0 L1 C1   _   _ _ VOI   _   _ _ _ _ _ _ linked_event_failed
        \\ commit create_transfers
        \\
        // Ids failed in a linked chain can be reused, but
        // `exceeds_debits` is a transient error, T2 cannot be reused:
        \\ transfer T1 A1 A2  0   _  _  _  _    0 L1 C1   _   _ _   _   _   _ _ _ _ _ _ created
        \\ transfer T2 A2 A3  0   _  _  _  _    0 L1 C1   _   _ _   _   _   _ _ _ _ _ _ id_already_failed
        \\ transfer T3 A2 A3  0   _  _  _  _    0 L1 C1   _   _ _   _   _   _ _ _ _ _ _ created
        \\ commit create_transfers
        \\
        \\ transfer T4 A1 A2 40   _  _  _  _    0 L1 C1 LNK   _ _   _   _   _ _ _ _ _ _ created
        \\ transfer T5 A2 A3  1   _  _  _  _    0 L1 C1 LNK PEN _   _ BDR   _ _ _ _ _ _ created
        \\ transfer T6 A2 A3  0  T5  _  _  _    0 L1 C1   _   _ _ VOI   _   _ _ _ _ _ _ created
        \\ commit create_transfers
        \\
        \\ lookup_account A1 0 40 0  0 _
        \\ lookup_account A2 0 40 0 40 _
        \\ lookup_account A3 0  0 0  0 _
        \\ commit lookup_accounts
    );

    // Temporarily enforce `debits_must_not_exceed_credits` on account `A1`.
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   D<C _ _ _ _ _ _ created
        \\ commit create_accounts
        \\
        \\ setup A1 0 0 0 40
        \\
        \\ transfer T1 A1 A2 41   _  _  _  _    0 L1 C1 LNK   _ _   _   _   _ _ _ _ _ _ linked_event_failed
        \\ transfer T2 A3 A1  1   _  _  _  _    0 L1 C1 LNK PEN _   _   _ BCR _ _ _ _ _ exceeds_credits
        \\ transfer T3 A3 A1  0  T2  _  _  _    0 L1 C1   _   _ _ VOI   _   _ _ _ _ _ _ linked_event_failed
        \\ commit create_transfers
        \\
        // Ids failed in a linked chain can be reused, but
        // `exceeds_credits` is a transient error, T2 cannot be reused:
        \\ transfer T1 A1 A2  0   _  _  _  _    0 L1 C1   _   _ _   _   _   _ _ _ _ _ _ created
        \\ transfer T2 A3 A1  0   _  _  _  _    0 L1 C1   _   _ _   _   _   _ _ _ _ _ _ id_already_failed
        \\ transfer T3 A3 A1  0   _  _  _  _    0 L1 C1   _   _ _   _   _   _ _ _ _ _ _ created
        \\ commit create_transfers
        \\
        \\ transfer T4 A1 A2 40   _  _  _  _    0 L1 C1 LNK   _ _   _   _   _ _ _ _ _ _ created
        \\ transfer T5 A3 A1  1   _  _  _  _    0 L1 C1 LNK PEN _   _   _ BCR _ _ _ _ _ created
        \\ transfer T6 A3 A1  0  T5  _  _  _    0 L1 C1   _   _ _ VOI   _   _ _ _ _ _ _ created
        \\ commit create_transfers
        \\
        \\ lookup_account A1 0 40 0 40 _
        \\ lookup_account A2 0  0 0 40 _
        \\ lookup_account A3 0  0 0  0 _
        \\ commit lookup_accounts
    );
}

test "imported events: imported batch" {
    try check(
        \\ tick 10 nanoseconds
        // The first event determines if the batch is either imported or not.
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _ 1 created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ _   _ _ 0 imported_event_expected
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _ 2 created
        \\ commit create_accounts
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ _   _ _ 0 created
        \\ account A4  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _ 3 imported_event_not_expected
        \\ commit create_accounts
        \\
        \\ transfer   T1 A1 A2    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  IMP _ _ _ 10 created
        \\ transfer   T2 A1 A2    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  _   _ _ _  0 imported_event_expected
        \\ commit create_transfers
        \\ transfer   T3 A1 A2    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  _   _  _ _ 0 created
        \\ transfer   T4 A1 A2    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  IMP _  _ _ 0 imported_event_not_expected
        \\ commit create_transfers
    );
}

test "imported events: timestamp" {
    try check(
        \\ tick 10 nanoseconds
        \\
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _  0 imported_event_timestamp_out_of_range
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _ -1 imported_event_timestamp_out_of_range
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _ 99 imported_event_timestamp_must_not_advance
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _  2 created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _  1 imported_event_timestamp_must_not_regress
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _  3 created
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _  9 created
        \\ commit create_accounts
        \\
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _ 99 imported_event_timestamp_must_not_advance
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _  1 exists
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _  3 exists
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _  4 exists
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _  9 exists
        \\ commit create_accounts
        \\
        \\ transfer   T1 A1 A2    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  IMP _ _ _  0 imported_event_timestamp_out_of_range
        \\ transfer   T1 A1 A2    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  IMP _ _ _ -1 imported_event_timestamp_out_of_range
        \\ transfer   T1 A1 A2    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  IMP _ _ _ 99 imported_event_timestamp_must_not_advance
        \\ transfer   T1 A1 A2    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  IMP _ _ _  2 imported_event_timestamp_must_not_regress // The same timestamp as the dr account.
        \\ transfer   T1 A1 A2    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  IMP _ _ _  3 imported_event_timestamp_must_not_regress // The same timestamp as the cr account.
        \\ transfer   T1 A3 A2    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  IMP _ _ _  4 imported_event_timestamp_must_postdate_debit_account
        \\ transfer   T1 A1 A3    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  IMP _ _ _  4 imported_event_timestamp_must_postdate_credit_account
        \\ transfer   T1 A1 A2    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  IMP _ _ _  4 created
        \\ transfer   T2 A1 A2    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  IMP _ _ _  3 imported_event_timestamp_must_not_regress
        \\ transfer   T2 A1 A2    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  IMP _ _ _  5 created
        \\ commit create_transfers
        \\
        \\ transfer   T2 A1 A2    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  IMP _ _ _ 99 imported_event_timestamp_must_not_advance
        \\ transfer   T2 A1 A2    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  IMP _ _ _  4 exists // T2 `exists` regardless different timestamps.
        \\ transfer   T2 A1 A2    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  IMP _ _ _  5 exists
        \\ transfer   T2 A1 A2    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  IMP _ _ _  6 exists
        \\ commit create_transfers
        \\
        \\ transfer   T3 A1 A2    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  IMP _ _ _ 10 created
        \\ commit create_transfers
        \\
        \\ account A4  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _ 10 imported_event_timestamp_must_not_regress // The same timestamp as a transfer.
        \\ commit create_accounts
    );
}

test "imported events: resolve timed pending transfers" {
    try check(
        \\ tick 10 nanoseconds
        \\
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ IMP _ _ 1 created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ IMP _ _ 2 created
        \\ commit create_accounts
        \\
        \\ transfer T1 A1 A2 10  _ _ _ _ 60 L1 C1 _ PEN _   _   _ _ _ _ _ _ _ created
        \\ transfer T2 A1 A2 20  _ _ _ _ 60 L1 C1 _ PEN _   _   _ _ _ _ _ _ _ created
        \\ commit create_transfers
        \\
        \\ tick 10 nanoseconds
        \\ transfer T3 A1 A2 10 T1 _ _ _  0 L1 C1 _   _ POS _   _ _ IMP _ _ _ 17 created
        \\ transfer T4 A1 A2  0 T2 _ _ _  0 L1 C1 _   _ _   VOI _ _ IMP _ _ _ 18 created
        \\ commit create_transfers
        \\
        \\ lookup_account A1 0 10 0  0 _
        \\ lookup_account A2 0  0 0 10 _
        \\ commit lookup_accounts
        \\
        // Crossing the original timeout must not find a stale expiry index entry.
        \\ tick 60 seconds
        \\ lookup_account A1 0 10 0  0 _
        \\ lookup_account A2 0  0 0 10 _
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T1 timestamp 13
        \\ lookup_transfer T2 timestamp 14
        \\ lookup_transfer T3 timestamp 17
        \\ lookup_transfer T4 timestamp 18
        \\ commit lookup_transfers
    );
}

test "imported events: pending transfers" {
    try check(
        \\ tick 10 nanoseconds
        \\
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _ 1 created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _ 2 created
        \\ commit create_accounts
        \\
        \\ transfer   T1 A1 A2    3   _  _  _  _    _ L1 C2   _     _   _   _   _   _  IMP _ _ _ 3 created
        \\ transfer   T2 A1 A2    4   _  _  _  _    1 L1 C2   _     PEN _   _   _   _  IMP _ _ _ 4 imported_event_timeout_must_be_zero
        \\ transfer   T2 A1 A2    4   _  _  _  _    0 L1 C2   _     PEN _   _   _   _  IMP _ _ _ 4 created
        \\ commit create_transfers
        \\
        \\ lookup_account A1 4  3  0  0  _
        \\ lookup_account A2 0  0  4  3  _
        \\ commit lookup_accounts
        \\
        \\ transfer   T3 A1 A2    4  T2 _  _   _    _ L1 C2   _     _   POS _   _   _  IMP _ _ _ 5 created
        \\ commit create_transfers
        \\
        \\ lookup_account A1 0  7  0  0  _
        \\ lookup_account A2 0  0  0  7  _
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T1 timestamp 3
        \\ lookup_transfer T2 timestamp 4
        \\ lookup_transfer T3 timestamp 5
        \\ commit lookup_transfers
    );
}

test "imported events: linked chain" {
    try check(
        \\ tick 10 nanoseconds
        \\
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   LNK  _  _  _ IMP _ _ 1 linked_event_failed
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   LNK  _  _  _ IMP _ _ 2 linked_event_failed
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _ 0 imported_event_timestamp_out_of_range
        \\ commit create_accounts
        \\
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   LNK  _  _  _ IMP _ _ 1 created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   LNK  _  _  _ IMP _ _ 2 created
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _ 3 created
        \\ commit create_accounts
        \\
        \\ transfer   T1 A1 A2    3   _  _  _  _    _ L1 C2   LNK   _   _   _   _   _  IMP _ _ _ 4 linked_event_failed
        \\ transfer   T2 A1 A2    3   _  _  _  _    _ L1 C2   LNK   _   _   _   _   _  IMP _ _ _ 5 linked_event_failed
        \\ transfer   T3 A1 A2    3   _  _  _  _    _ L1 C2   _     _   _   _   _   _  IMP _ _ _ 0 imported_event_timestamp_out_of_range
        \\ commit create_transfers
        \\
        \\ transfer   T1 A1 A2    3   _  _  _  _    _ L1 C2   LNK   _   _   _   _   _  IMP _ _ _ 4 created
        \\ transfer   T2 A1 A2    3   _  _  _  _    _ L1 C2   LNK   _   _   _   _   _  IMP _ _ _ 5 created
        \\ transfer   T3 A1 A2    3   _  _  _  _    _ L1 C2   _     _   _   _   _   _  IMP _ _ _ 6 created
        \\ commit create_transfers
        \\
        \\ lookup_account A1 0  9  0  0  _
        \\ lookup_account A2 0  0  0  9  _
        \\ lookup_account A3 0  0  0  0  _
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T1 timestamp 4
        \\ lookup_transfer T2 timestamp 5
        \\ lookup_transfer T3 timestamp 6
        \\ commit lookup_transfers
    );
}

test "create_accounts: closed accounts" {
    try check(
        // Accounts can be created already closed.
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _  _  _ _ _  CLSD _ _ created
        \\ commit create_accounts
        \\
        \\ lookup_account A1  0  0  0   0  CLSD
        \\ commit lookup_accounts
    );
}

test "create_transfers: closing accounts" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _  _  _ _ _ _ _ _ created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _  _  _ _ _ _ _ _ created
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _  _  _ _ _ _ _ _ created
        \\ commit create_accounts
        \\
        // Closing the debit account.
        \\ transfer   T1  A1 A2   15   _  _   _  _    _ L1 C1   _   _   _   _   _   _  _  _   _   _ _ created
        \\ transfer   T2  A1 A2    0   _  _   _  _    _ L1 C1   _   _   _   _   _   _  _  CDR _   _ _ closing_transfer_must_be_pending
        \\ transfer   T2  A1 A2    0   _  _   _  _    0 L1 C1   _   PEN _   _   _   _  _  CDR _   _ _ created
        \\ transfer   T2  A1 A2    0   _  _   _  _    0 L1 C1   _   PEN _   _   _   _  _  CDR _   _ _ exists
        // `debit_account_already_closed` is a transient error, T3 cannot be reused:
        \\ transfer   T3  A1 A2    5   _  _   _  _    _ L1 C1   _   _   _   _   _   _  _  _   _   _ _ debit_account_already_closed
        \\ transfer   T3  A1 A2    5   _  _   _  _    _ L1 C1   _   _   _   _   _   _  _  _   _   _ _ id_already_failed
        // `credit_account_already_closed` is a transient error, T4 cannot be reused:
        \\ transfer   T4  A2 A1    5   _  _   _  _    _ L1 C1   _   _   _   _   _   _  _  _   _   _ _ credit_account_already_closed
        \\ transfer   T4  A1 A2    5   _  _   _  _    _ L1 C1   _   _   _   _   _   _  _  _   _   _ _ id_already_failed
        \\ commit create_transfers
        \\
        \\ lookup_account A1  0 15  0   0  CLSD
        \\ lookup_account A2  0  0  0  15  _
        \\ commit lookup_accounts
        // `debit_account_already_closed` is a transient error, T5 cannot be reused:
        \\ transfer   T5  A1 A2    0   T2 _   _  _    _ L1 C1   _   _   POS _   _   _  _  _   _   _ _ debit_account_already_closed
        \\ transfer   T5  A1 A2    0   T2 _   _  _    _ L1 C1   _   _   _   VOI _   _  _  _   _   _ _ id_already_failed
        \\
        \\ transfer   T6  A1 A2    0   T2 _   _  _    _ L1 C1   _   _   _   VOI _   _  _  _   _   _ _ created // Re-opening the account.
        \\ transfer   T6  A1 A2    0   T2 _   _  _    _ L1 C1   _   _   _   VOI _   _  _  _   _   _ _ exists
        \\ transfer   T7  A1 A2    5   _  _   _  _    _ L1 C1   _   _   _   _   _   _  _  _   _   _ _ created
        \\ commit create_transfers
        \\
        \\ lookup_account A1  0 20  0   0  _
        \\ lookup_account A2  0  0  0  20  _
        \\ commit lookup_accounts
        \\
        // Closing the credit account with a timeout.
        // Pending transfer can be voided, but not posted in a closed account.
        \\ transfer   T8  A1 A2   10   _  _   _  _    1 L1 C1   _   PEN _   _   _   _  _  _   _   _ _ created
        \\ transfer   T9  A1 A2   10   _  _   _  _    0 L1 C1   _   PEN _   _   _   _  _  _   _   _ _ created
        \\ transfer   T10 A1 A2    0   _  _   _  _    2 L1 C1   _   PEN _   _   _   _  _  _   CCR _ _ created
        \\ transfer   T10 A1 A2    0   _  _   _  _    2 L1 C1   _   PEN _   _   _   _  _  _   CCR _ _ exists
        // `credit_account_already_closed` is a transient error, T11 cannot be reused:
        \\ transfer   T11 A1 A2   10   T9 _   _  _    _ L1 C1   _   _   POS _   _   _  _  _   _   _ _ credit_account_already_closed
        \\ transfer   T11 A1 A2   10   T9 _   _  _    _ L1 C1   _   _   _   VOI _   _  _  _   _   _ _ id_already_failed
        \\
        \\ transfer   T12 A1 A2   10   T9 _   _  _    _ L1 C1   _   _   _   VOI _   _  _  _   _   _ _ created
        \\ transfer   T12 A1 A2   10   T9 _   _  _    _ L1 C1   _   _   _   VOI _   _  _  _   _   _ _ exists
        \\ commit create_transfers
        \\
        \\ lookup_account A1 10 20  0   0  _
        \\ lookup_account A2  0  0 10  20  CLSD
        \\ commit lookup_accounts
        \\
        // Pending balance can expire for closed accounts.
        \\ tick 1 seconds
        \\ lookup_account A1  0 20  0   0  _
        \\ lookup_account A2  0  0  0  20  CLSD
        \\ commit lookup_accounts
        \\
        // Pending closing accounts can expire after the timeout.
        \\ tick 1 seconds
        \\ lookup_account A1  0 20  0   0  _
        \\ lookup_account A2  0  0  0  20  _
        \\ commit lookup_accounts
        \\
        // Closing both accounts.
        \\ transfer   T13  A1 A2    0   _  _   _  _    _ L1 C1   _   _   _   _   _   _  _  CDR CCR _ _ closing_transfer_must_be_pending
        \\ transfer   T13  A1 A2    0   _  _   _  _    0 L1 C1   _   PEN _   _   _   _  _  CDR CCR _ _ created
        \\ transfer   T13  A1 A2    0   _  _   _  _    0 L1 C1   _   PEN _   _   _   _  _  CDR CCR _ _ exists
        // `debit_account_already_closed` is a transient error, T14 cannot be reused:
        \\ transfer   T14  A1 A3    5   _  _   _  _    _ L1 C1   _   _   _   _   _   _  _  _   _   _ _ debit_account_already_closed
        \\ transfer   T14  A1 A3    5   _  _   _  _    _ L1 C1   _   _   _   _   _   _  _  _   _   _ _ id_already_failed
        // `credit_account_already_closed` is a transient error, T15 cannot be reused:
        \\ transfer   T15  A3 A2    5   _  _   _  _    _ L1 C1   _   _   _   _   _   _  _  _   _   _ _ credit_account_already_closed
        \\ transfer   T15  A3 A2    5   _  _   _  _    _ L1 C1   _   _   _   _   _   _  _  _   _   _ _ id_already_failed
        \\ commit create_transfers
        \\
        \\ lookup_account A1  0 20  0   0  CLSD
        \\ lookup_account A2  0  0  0  20  CLSD
        \\ commit lookup_accounts
        \\
        // Cannot close an already closed account.
        // `debit_account_already_closed` is a transient error, T16 cannot be reused:
        \\ transfer   T16 A1 A3    0   _  _   _  _    0 L1 C1   _   PEN   _   _   _   _  _  CDR _   _ _ debit_account_already_closed
        \\ transfer   T16 A1 A3    0   _  _   _  _    0 L1 C1   _   PEN   _   _   _   _  _  CDR _   _ _ id_already_failed
        // `credit_account_already_closed` is a transient error, T17 cannot be reused:
        \\ transfer   T17 A3 A2    0   _  _   _  _    0 L1 C1   _   PEN   _   _   _   _  _  _   CCR _ _ credit_account_already_closed
        \\ transfer   T17 A3 A2    0   _  _   _  _    0 L1 C1   _   PEN   _   _   _   _  _  _   CCR _ _ id_already_failed
        \\ commit create_transfers
    );
}

test "get_account_transfers: single-phase" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ _ _ _ _ _ _ _ created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _ _ _ _ _ _ _ _ created
        \\ commit create_accounts
        \\
        \\ transfer T1 A1 A2   10   _  U1000  U10  U1 _ L1 C1   _   _   _   _   _   _  _ _ _ _ _ created
        \\ transfer T2 A2 A1   11   _  U1001  U10  U2 _ L1 C2   _   _   _   _   _   _  _ _ _ _ _ created
        \\ transfer T3 A1 A2   12   _  U1000  U20  U2 _ L1 C1   _   _   _   _   _   _  _ _ _ _ _ created
        \\ transfer T4 A2 A1   13   _  U1001  U20  U1 _ L1 C2   _   _   _   _   _   _  _ _ _ _ _ created
        \\ commit create_transfers
        \\
        // Debits + credits, chronological.
        \\ get_account_transfers A1 _ _ _ _ _  _ 10 DR CR  _
        \\ get_account_transfers_result T1
        \\ get_account_transfers_result T2
        \\ get_account_transfers_result T3
        \\ get_account_transfers_result T4
        \\ commit get_account_transfers
        \\
        // Debits + credits, limit=2.
        \\ get_account_transfers A1 _ _ _ _  _  _  2 DR CR  _
        \\ get_account_transfers_result T1
        \\ get_account_transfers_result T2
        \\ commit get_account_transfers
        \\
        // Debits + credits, timestamp_min>0.
        \\ get_account_transfers A1 _ _ _ _  T3 _ 10 DR CR  _
        \\ get_account_transfers_result T3
        \\ get_account_transfers_result T4
        \\ commit get_account_transfers
        \\
        // Debits + credits, timestamp_max>0.
        \\ get_account_transfers A1 _ _ _ _  _ T2 10 DR CR  _
        \\ get_account_transfers_result T1
        \\ get_account_transfers_result T2
        \\ commit get_account_transfers
        \\
        // Debits + credits, 0 < timestamp_min ≤ timestamp_max.
        \\ get_account_transfers A1 _ _ _ _ T2 T3 10 DR CR  _
        \\ get_account_transfers_result T2
        \\ get_account_transfers_result T3
        \\ commit get_account_transfers
        \\
        // Debits + credits, reverse-chronological.
        \\ get_account_transfers A1 _ _ _ _  _  _ 10 DR CR REV
        \\ get_account_transfers_result T4
        \\ get_account_transfers_result T3
        \\ get_account_transfers_result T2
        \\ get_account_transfers_result T1
        \\ commit get_account_transfers
        \\
        // Debits only.
        \\ get_account_transfers A1 _ _ _ _  _  _ 10 DR  _  _
        \\ get_account_transfers_result T1
        \\ get_account_transfers_result T3
        \\ commit get_account_transfers
        \\
        // Credits only.
        \\ get_account_transfers A1 _ _ _ _  _  _ 10  _ CR  _
        \\ get_account_transfers_result T2
        \\ get_account_transfers_result T4
        \\ commit get_account_transfers
        \\
        // Debits + credits + user_data_128, chronological.
        \\ get_account_transfers A1 U1001 _ _ _ _  _ 10 DR CR  _
        \\ get_account_transfers_result T2
        \\ get_account_transfers_result T4
        \\ commit get_account_transfers
        \\
        // Debits + credits + user_data_64, chronological.
        \\ get_account_transfers A1 _ U10 _ _ _  _ 10 DR CR  _
        \\ get_account_transfers_result T1
        \\ get_account_transfers_result T2
        \\ commit get_account_transfers
        \\
        // Debits + credits + user_data_32, chronological.
        \\ get_account_transfers A1 _ _ U1 _ _  _ 10 DR CR  _
        \\ get_account_transfers_result T1
        \\ get_account_transfers_result T4
        \\ commit get_account_transfers
        \\
        // Debits + credits + code, chronological.
        \\ get_account_transfers A1 _ _ _ C1 _  _ 10 DR CR  _
        \\ get_account_transfers_result T1
        \\ get_account_transfers_result T3
        \\ commit get_account_transfers
        \\
        // Debits + credits + all filters, 0 < timestamp_min ≤ timestamp_max, chronological.
        \\ get_account_transfers A1 U1000 U10 U1 C1 T1 T3 10 DR CR  _
        \\ get_account_transfers_result T1
        \\ commit get_account_transfers
        \\
        // Debits only + all filters, 0 < timestamp_min ≤ timestamp_max, chronological.
        \\ get_account_transfers A1 U1000 U10 U1 C1 T1 T3 10 DR _  _
        \\ get_account_transfers_result T1
        \\ commit get_account_transfers
        \\
        // Credits only + all filters, 0 < timestamp_min ≤ timestamp_max, chronological.
        \\ get_account_transfers A2 U1000 U10 U1 C1 T1 T3 10 _ CR  _
        \\ get_account_transfers_result T1
        \\ commit get_account_transfers
        \\
        // Not found.
        \\ get_account_transfers A1 U1000 U20 U2 C2 _ _ 10 DR CR  _
        \\ commit get_account_transfers
    );
}

test "get_account_transfers: two-phase" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ _ _ HIST _ _ _ _ created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _ _ _ HIST _ _ _ _ created
        \\ commit create_accounts
        \\
        \\ transfer T1 A1 A2    2   _  _  _  _    0 L1 C1   _ PEN   _   _   _   _  _ _ _ _ _ created
        \\ transfer T2 A1 A2    1  T1  _  _  _    0 L1 C1   _   _ POS   _   _   _  _ _ _ _ _ created
        \\ commit create_transfers
        \\
        \\ get_account_transfers A1 _ _ _ _ _ _ 10 DR CR  _
        \\ get_account_transfers_result T1
        \\ get_account_transfers_result T2
        \\ commit get_account_transfers
    );
}

test "get_account_transfers: invalid filter" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ _ _ _ _ _ _ _ created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _ _ _ _ _ _ _ _ created
        \\ commit create_accounts
        \\
        \\ transfer T1 A1 A2    2   _  _  _  _    0 L1 C1   _ PEN   _   _   _   _  _ _ _ _ _ created
        \\ transfer T2 A1 A2    1  T1  _  _  _    0 L1 C1   _   _ POS   _   _   _  _ _ _ _ _ created
        \\ commit create_transfers
        \\
        // Invalid account.
        \\ get_account_transfers A3 _ _ _ _  _  _  10 DR CR _
        \\ commit get_account_transfers // Empty result.
        \\
        // Invalid filter flags.
        \\ get_account_transfers A1 _ _ _ _  _  _  10 _  _  _
        \\ commit get_account_transfers // Empty result.
        \\
        // Invalid timestamp_min > timestamp_max.
        \\ get_account_transfers A1 _ _ _ _  T2 T1 10 DR CR _
        \\ commit get_account_transfers // Empty result.
        \\
        // Invalid limit.
        \\ get_account_transfers A1 _ _ _ _  _   _  0 DR CR _
        \\ commit get_account_transfers // Empty result.
        \\
        // Success.
        \\ get_account_transfers A1 _ _ _ C1 _   _ 10 DR CR _
        \\ get_account_transfers_result T1
        \\ get_account_transfers_result T2
        \\ commit get_account_transfers
    );
}

test "get_account_balances: single-phase" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ _ _ HIST _ _ _ _ created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _ _ _ HIST _ _ _ _ created
        \\ commit create_accounts
        \\
        \\ transfer T1 A1 A2   10   _  U1000  U10  U1 _ L1 C1   _   _   _   _   _   _  _ _ _ _ _ created
        \\ transfer T2 A2 A1   11   _  U1001  U10  U2 _ L1 C2   _   _   _   _   _   _  _ _ _ _ _ created
        \\ transfer T3 A1 A2   12   _  U1000  U20  U2 _ L1 C1   _   _   _   _   _   _  _ _ _ _ _ created
        \\ transfer T4 A2 A1   13   _  U1001  U20  U1 _ L1 C2   _   _   _   _   _   _  _ _ _ _ _ created
        \\ commit create_transfers
        \\
        // Debits + credits, chronological.
        \\ get_account_balances A1 _ _ _ _  _  _ 10 DR CR  _
        \\ get_account_balances_result T1 0 10 0  0
        \\ get_account_balances_result T2 0 10 0 11
        \\ get_account_balances_result T3 0 22 0 11
        \\ get_account_balances_result T4 0 22 0 24
        \\ commit get_account_balances
        \\
        // Debits + credits, limit=2.
        \\ get_account_balances A1 _ _ _ _  _  _ 2 DR CR  _
        \\ get_account_balances_result T1 0 10 0  0
        \\ get_account_balances_result T2 0 10 0 11
        \\ commit get_account_balances
        \\
        // Debits + credits, timestamp_min>0.
        \\ get_account_balances A1 _ _ _ _  T3 _ 10 DR CR  _
        \\ get_account_balances_result T3 0 22 0 11
        \\ get_account_balances_result T4 0 22 0 24
        \\ commit get_account_balances
        \\
        // Debits + credits, timestamp_max>0.
        \\ get_account_balances A1 _ _ _ _  _ T2 10 DR CR  _
        \\ get_account_balances_result T1 0 10 0  0
        \\ get_account_balances_result T2 0 10 0 11
        \\ commit get_account_balances
        \\
        // Debits + credits, 0 < timestamp_min ≤ timestamp_max.
        \\ get_account_balances A1 _ _ _ _ T2 T3 10 DR CR  _
        \\ get_account_balances_result T2 0 10 0 11
        \\ get_account_balances_result T3 0 22 0 11
        \\ commit get_account_balances
        \\
        // Debits + credits, reverse-chronological.
        \\ get_account_balances A1 _ _ _ _  _  _ 10 DR CR REV
        \\ get_account_balances_result T4 0 22 0 24
        \\ get_account_balances_result T3 0 22 0 11
        \\ get_account_balances_result T2 0 10 0 11
        \\ get_account_balances_result T1 0 10 0  0
        \\ commit get_account_balances
        \\
        // Debits only.
        \\ get_account_balances A1 _ _ _ _  _  _ 10 DR  _  _
        \\ get_account_balances_result T1 0 10 0  0
        \\ get_account_balances_result T3 0 22 0 11
        \\ commit get_account_balances
        \\
        // Credits only.
        \\ get_account_balances A1 _ _ _ _  _  _ 10  _ CR  _
        \\ get_account_balances_result T2 0 10 0 11
        \\ get_account_balances_result T4 0 22 0 24
        \\ commit get_account_balances
        \\
        // Debits + credits + user_data_128, chronological.
        \\ get_account_balances A1 U1001 _ _ _ _  _ 10 DR CR  _
        \\ get_account_balances_result T2 0 10 0 11
        \\ get_account_balances_result T4 0 22 0 24
        \\ commit get_account_balances
        \\
        // Debits + credits + user_data_64, chronological.
        \\ get_account_balances A1 _ U10 _ _ _  _ 10 DR CR  _
        \\ get_account_balances_result T1 0 10 0  0
        \\ get_account_balances_result T2 0 10 0 11
        \\ commit get_account_balances
        \\
        // Debits + credits + user_data_32, chronological.
        \\ get_account_balances A1 _ _ U1 _ _  _ 10 DR CR  _
        \\ get_account_balances_result T1 0 10 0  0
        \\ get_account_balances_result T4 0 22 0 24
        \\ commit get_account_balances
        \\
        // Debits + credits + code, chronological.
        \\ get_account_balances A1 _ _ _ C1 _  _ 10 DR CR  _
        \\ get_account_balances_result T1 0 10 0  0
        \\ get_account_balances_result T3 0 22 0 11
        \\ commit get_account_balances
        \\
        // Debits + credits + all filters, 0 < timestamp_min ≤ timestamp_max, chronological.
        \\ get_account_balances A1 U1000 U10 U1 C1 T1 T3 10 DR CR  _
        \\ get_account_balances_result T1 0 10 0  0
        \\ commit get_account_balances
        \\
        // Debits only + all filters, 0 < timestamp_min ≤ timestamp_max, chronological.
        \\ get_account_balances A1 U1000 U10 U1 C1 T1 T3 10 DR _  _
        \\ get_account_balances_result T1 0 10 0  0
        \\ commit get_account_balances
        \\
        // Credits only + all filters, 0 < timestamp_min ≤ timestamp_max, chronological.
        \\ get_account_balances A2 U1000 U10 U1 C1 T1 T3 10 _ CR  _
        \\ get_account_balances_result T1 0  0 0  10
        \\ commit get_account_balances
        \\
        // Not found.
        \\ get_account_balances A1 U1000 U20 U2 C2 _ _ 10 DR CR  _
        \\ commit get_account_balances
    );
}

test "get_account_balances: two-phase" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ _ _ HIST _ _ _ _ created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _ _ _ HIST _ _ _ _ created
        \\ commit create_accounts
        \\
        \\ transfer T1 A1 A2    1   _  _  _  _    0 L1 C1   _ PEN   _   _   _   _  _ _ _ _ _ created
        \\ transfer T2 A1 A2    1  T1  _  _  _    0 L1 C1   _   _ POS   _   _   _  _ _ _ _ _ created
        \\ commit create_transfers
        \\
        \\ get_account_balances A1 _ _ _ _ _ _ 10 DR CR  _
        \\ get_account_balances_result T1 1 0 0 0
        \\ get_account_balances_result T2 0 1 0 0
        \\ commit get_account_balances
    );
}

test "get_account_balances: invalid filter" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ _ _ HIST _ _ _ _ created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _ _ _    _ _ _ _ _ created
        \\ commit create_accounts
        \\
        \\ transfer T1 A1 A2    2   _  _  _  _    0 L1 C1   _   _   _   _   _   _  _ _ _ _ _ created
        \\ transfer T2 A1 A2    1   _  _  _  _    0 L1 C1   _   _   _   _   _   _  _ _ _ _ _ created
        \\ commit create_transfers
        \\
        // Invalid account.
        \\ get_account_balances A3 _ _ _  _ _  _  10 DR CR _
        \\ commit get_account_balances // Empty result.
        \\
        // Account without flags.history.
        \\ get_account_balances A2 _ _ _  _ _  _  10 DR CR _
        \\ commit get_account_balances // Empty result.
        \\
        // Invalid filter flags.
        \\ get_account_balances A1 _ _ _  _ _  _  10 _  _  _
        \\ commit get_account_balances // Empty result.
        \\
        // Invalid timestamp_min > timestamp_max.
        \\ get_account_balances A1 _ _ _  _ T2 T1 10 DR CR _
        \\ commit get_account_balances // Empty result.
        \\
        // Invalid limit.
        \\ get_account_balances A1 _ _ _  _ _   _  0 DR CR _
        \\ commit get_account_balances // Empty result.
        \\
        // Success.
        \\ get_account_balances A1 _ _ _ C1 _  _ 10 DR CR  _
        \\ get_account_balances_result T1 0 2 0 0
        \\ get_account_balances_result T2 0 3 0 0
        \\ commit get_account_balances
    );
}

test "query_accounts" {
    try check(
        \\ account A1  0  0  0  0 U1000 U10 U1 _ L1 C1 _   _   _ _ _ _ _ _ created
        \\ account A2  0  0  0  0 U1000 U11 U2 _ L2 C2 _   _   _ _ _ _ _ _ created
        \\ account A3  0  0  0  0 U1000 U10 U3 _ L3 C3 _   _   _ _ _ _ _ _ created
        \\ account A4  0  0  0  0 U1000 U11 U4 _ L4 C4 _   _   _ _ _ _ _ _ created
        \\ account A5  0  0  0  0 U2000 U10 U1 _ L3 C5 _   _   _ _ _ _ _ _ created
        \\ account A6  0  0  0  0 U2000 U11 U2 _ L2 C6 _   _   _ _ _ _ _ _ created
        \\ account A7  0  0  0  0 U2000 U10 U3 _ L1 C7 _   _   _ _ _ _ _ _ created
        \\ account A8  0  0  0  0 U1000 U10 U1 _ L1 C1 _   _   _ _ _ _ _ _ created
        \\ commit create_accounts

        // WHERE user_data_128=1000:
        \\ query_accounts U1000 U0 U0 L0 C0 _ _ L-0 _
        \\ query_accounts_result A1 _
        \\ query_accounts_result A2 _
        \\ query_accounts_result A3 _
        \\ query_accounts_result A4 _
        \\ query_accounts_result A8 _
        \\ commit query_accounts

        // WHERE user_data_128=1000 ORDER BY DESC:
        \\ query_accounts U1000 U0 U0 L0 C0 _ _ L-0 REV
        \\ query_accounts_result A8 _
        \\ query_accounts_result A4 _
        \\ query_accounts_result A3 _
        \\ query_accounts_result A2 _
        \\ query_accounts_result A1 _
        \\ commit query_accounts

        // WHERE user_data_64=10 AND user_data_32=3
        \\ query_accounts U0 U10 U3 L0 C0 _ _ L-0 _
        \\ query_accounts_result A3 _
        \\ query_accounts_result A7 _
        \\ commit query_accounts

        // WHERE user_data_64=10 AND user_data_32=3 ORDER BY DESC:
        \\ query_accounts U0 U10 U3 L0 C0 _ _ L-0 REV
        \\ query_accounts_result A7 _
        \\ query_accounts_result A3 _
        \\ commit query_accounts

        // WHERE user_data_64=11 AND user_data_32=2 AND code=2:
        \\ query_accounts U0 U11 U2 L2 C0 _ _ L-0 _
        \\ query_accounts_result A2 _
        \\ query_accounts_result A6 _
        \\ commit query_accounts

        // WHERE user_data_64=11 AND user_data_32=2 AND code=2 ORDER BY DESC:
        \\ query_accounts U0 U11 U2 L2 C0 _ _ L-0 REV
        \\ query_accounts_result A6 _
        \\ query_accounts_result A2 _
        \\ commit query_accounts

        // WHERE user_data_128=1000 AND user_data_64=10
        // AND user_data_32=1 AND ledger=1 AND code=1:
        \\ query_accounts U1000 U10 U1 L1 C1 _ _ L-0 _
        \\ query_accounts_result A1 _
        \\ query_accounts_result A8 _
        \\ commit query_accounts

        // WHERE user_data_128=1000 AND user_data_64=10
        // AND user_data_32=1 AND ledger=1 AND code=1 ORDER BY DESC:
        \\ query_accounts U1000 U10 U1 L1 C1 _ _ L-0 REV
        \\ query_accounts_result A8 _
        \\ query_accounts_result A1 _
        \\ commit query_accounts

        // WHERE user_data_128=1000 AND timestamp >= A3.timestamp:
        \\ query_accounts U1000 U0 U0 L0 C0 A3 _ L-0 _
        \\ query_accounts_result A3 _
        \\ query_accounts_result A4 _
        \\ query_accounts_result A8 _
        \\ commit query_accounts

        // WHERE user_data_128=1000 AND timestamp <= A3.timestamp:
        \\ query_accounts U1000 U0 U0 L0 C0 _ A3 L-0 _
        \\ query_accounts_result A1 _
        \\ query_accounts_result A2 _
        \\ query_accounts_result A3 _
        \\ commit query_accounts

        // WHERE user_data_128=1000 AND timestamp BETWEEN A2.timestamp AND A4.timestamp:
        \\ query_accounts U1000 U0 U0 L0 C0 A2 A4 L-0 _
        \\ query_accounts_result A2 _
        \\ query_accounts_result A3 _
        \\ query_accounts_result A4 _
        \\ commit query_accounts

        // SELECT * :
        \\ query_accounts U0 U0 U0 L0 C0 _ _ L-0 _
        \\ query_accounts_result A1 _
        \\ query_accounts_result A2 _
        \\ query_accounts_result A3 _
        \\ query_accounts_result A4 _
        \\ query_accounts_result A5 _
        \\ query_accounts_result A6 _
        \\ query_accounts_result A7 _
        \\ query_accounts_result A8 _
        \\ commit query_accounts

        // SELECT * ORDER BY DESC:
        \\ query_accounts U0 U0 U0 L0 C0 _ _ L-0 REV
        \\ query_accounts_result A8 _
        \\ query_accounts_result A7 _
        \\ query_accounts_result A6 _
        \\ query_accounts_result A5 _
        \\ query_accounts_result A4 _
        \\ query_accounts_result A3 _
        \\ query_accounts_result A2 _
        \\ query_accounts_result A1 _
        \\ commit query_accounts

        // SELECT * WHERE timestamp >= A2.timestamp LIMIT 3:
        \\ query_accounts U0 U0 U0 L0 C0 A2 _ L3 _
        \\ query_accounts_result A2 _
        \\ query_accounts_result A3 _
        \\ query_accounts_result A4 _
        \\ commit query_accounts

        // SELECT * LIMIT 1:
        \\ query_accounts U0 U0 U0 L0 C0 _ _ L1 _
        \\ query_accounts_result A1 _
        \\ commit query_accounts

        // SELECT * ORDER BY DESC LIMIT 1:
        \\ query_accounts U0 U0 U0 L0 C0 _ _ L1 REV
        \\ query_accounts_result A8 _
        \\ commit query_accounts

        // NOT FOUND:

        // SELECT * LIMIT 0:
        \\ query_accounts U0 U0 U0 L0 C0 _ _ L0 _
        \\ commit query_accounts

        // WHERE user_data_128=3000
        \\ query_accounts U3000 U0 U0 L0 C0 _ _ L-0 _
        \\ commit query_accounts

        // WHERE user_data_128=1000 AND code=5
        \\ query_accounts U1000 U0 U0 L0 C5 _ _ L-0 _
        \\ commit query_accounts

        // WHERE user_data_128=1000 AND user_data_64=10
        // AND user_data_32=1 AND ledger=1 AND code=2:
        \\ query_accounts U1000 U10 U1 L1 C2 _ _ L-0 _
        \\ commit query_accounts

        // WHERE user_data_128=1000 AND timestamp BETWEEN A5.timestamp AND A7.timestamp:
        \\ query_accounts U1000 U0 U0 L0 C0 A5 A7 L-0 _
        \\ commit query_accounts
    );
}

test "query_transfers" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ account A3  0  0  0  0  _  _  _ _ L2 C1   _   _   _ _ _ _ _ _ created
        \\ account A4  0  0  0  0  _  _  _ _ L2 C1   _   _   _ _ _ _ _ _ created
        \\ commit create_accounts

        // Creating transfers:
        \\ transfer T1 A1 A2    0  _ U1000 U10 U1 _ L1 C1 _ _ _ _ _ _ _ _ _ _ _ created
        \\ transfer T2 A3 A4   11  _ U1000 U11 U2 _ L2 C2 _ _ _ _ _ _ _ _ _ _ _ created
        \\ transfer T3 A2 A1   12  _ U1000 U10 U3 _ L1 C3 _ _ _ _ _ _ _ _ _ _ _ created
        \\ transfer T4 A4 A3   13  _ U1000 U11 U4 _ L2 C4 _ _ _ _ _ _ _ _ _ _ _ created
        \\ transfer T5 A2 A1   14  _ U2000 U10 U1 _ L1 C5 _ _ _ _ _ _ _ _ _ _ _ created
        \\ transfer T6 A4 A3   15  _ U2000 U11 U2 _ L2 C6 _ _ _ _ _ _ _ _ _ _ _ created
        \\ transfer T7 A1 A2   16  _ U2000 U10 U3 _ L1 C7 _ _ _ _ _ _ _ _ _ _ _ created
        \\ transfer T8 A2 A1   17  _ U1000 U10 U1 _ L1 C1 _ _ _ _ _ _ _ _ _ _ _ created
        \\ commit create_transfers

        // WHERE user_data_128=1000:
        \\ query_transfers U1000 U0 U0 L0 C0 _ _ L-0 _
        \\ query_transfers_result T1
        \\ query_transfers_result T2
        \\ query_transfers_result T3
        \\ query_transfers_result T4
        \\ query_transfers_result T8
        \\ commit query_transfers

        // WHERE user_data_128=1000 ORDER BY DESC:
        \\ query_transfers U1000 U0 U0 L0 C0 _ _ L-0 REV
        \\ query_transfers_result T8
        \\ query_transfers_result T4
        \\ query_transfers_result T3
        \\ query_transfers_result T2
        \\ query_transfers_result T1
        \\ commit query_transfers

        // WHERE user_data_64=10 AND user_data_32=3
        \\ query_transfers U0 U10 U3 L0 C0 _ _ L-0 _
        \\ query_transfers_result T3
        \\ query_transfers_result T7
        \\ commit query_transfers

        // WHERE user_data_64=10 AND user_data_32=3 ORDER BY DESC:
        \\ query_transfers U0 U10 U3 L0 C0 _ _ L-0 REV
        \\ query_transfers_result T7
        \\ query_transfers_result T3
        \\ commit query_transfers

        // WHERE user_data_64=11 AND user_data_32=2 AND code=2:
        \\ query_transfers U0 U11 U2 L2 C0 _ _ L-0 _
        \\ query_transfers_result T2
        \\ query_transfers_result T6
        \\ commit query_transfers

        // WHERE user_data_64=11 AND user_data_32=2 AND code=2 ORDER BY DESC:
        \\ query_transfers U0 U11 U2 L2 C0 _ _ L-0 REV
        \\ query_transfers_result T6
        \\ query_transfers_result T2
        \\ commit query_transfers

        // WHERE user_data_128=1000 AND user_data_64=10
        // AND user_data_32=1 AND ledger=1 AND code=1:
        \\ query_transfers U1000 U10 U1 L1 C1 _ _ L-0 _
        \\ query_transfers_result T1
        \\ query_transfers_result T8
        \\ commit query_transfers

        // WHERE user_data_128=1000 AND user_data_64=10
        // AND user_data_32=1 AND ledger=1 AND code=1 ORDER BY DESC:
        \\ query_transfers U1000 U10 U1 L1 C1 _ _ L-0 REV
        \\ query_transfers_result T8
        \\ query_transfers_result T1
        \\ commit query_transfers

        // WHERE user_data_128=1000 AND timestamp >= T3.timestamp:
        \\ query_transfers U1000 U0 U0 L0 C0 A3 _ L-0 _
        \\ query_transfers_result T3
        \\ query_transfers_result T4
        \\ query_transfers_result T8
        \\ commit query_transfers

        // WHERE user_data_128=1000 AND timestamp <= T3.timestamp:
        \\ query_transfers U1000 U0 U0 L0 C0 _ A3 L-0 _
        \\ query_transfers_result T1
        \\ query_transfers_result T2
        \\ query_transfers_result T3
        \\ commit query_transfers

        // WHERE user_data_128=1000 AND timestamp BETWEEN T2.timestamp AND T4.timestamp:
        \\ query_transfers U1000 U0 U0 L0 C0 A2 A4 L-0 _
        \\ query_transfers_result T2
        \\ query_transfers_result T3
        \\ query_transfers_result T4
        \\ commit query_transfers

        // SELECT * :
        \\ query_transfers U0 U0 U0 L0 C0 _ _ L-0 _
        \\ query_transfers_result T1
        \\ query_transfers_result T2
        \\ query_transfers_result T3
        \\ query_transfers_result T4
        \\ query_transfers_result T5
        \\ query_transfers_result T6
        \\ query_transfers_result T7
        \\ query_transfers_result T8
        \\ commit query_transfers

        // SELECT * ORDER BY DESC:
        \\ query_transfers U0 U0 U0 L0 C0 _ _ L-0 REV
        \\ query_transfers_result T8
        \\ query_transfers_result T7
        \\ query_transfers_result T6
        \\ query_transfers_result T5
        \\ query_transfers_result T4
        \\ query_transfers_result T3
        \\ query_transfers_result T2
        \\ query_transfers_result T1
        \\ commit query_transfers

        // SELECT * WHERE timestamp >= A2.timestamp LIMIT 3:
        \\ query_transfers U0 U0 U0 L0 C0 A2 _ L3 _
        \\ query_transfers_result T2
        \\ query_transfers_result T3
        \\ query_transfers_result T4
        \\ commit query_transfers

        // SELECT * LIMIT 1:
        \\ query_transfers U0 U0 U0 L0 C0 _ _ L1 _
        \\ query_transfers_result T1
        \\ commit query_transfers

        // SELECT * ORDER BY DESC LIMIT 1:
        \\ query_transfers U0 U0 U0 L0 C0 _ _ L1 REV
        \\ query_transfers_result T8
        \\ commit query_transfers

        // NOT FOUND:

        // SELECT * LIMIT 0:
        \\ query_transfers U0 U0 U0 L0 C0 _ _ L0 _
        \\ commit query_transfers

        // WHERE user_data_128=3000
        \\ query_transfers U3000 U0 U0 L0 C0 _ _ L-0 _
        \\ commit query_transfers

        // WHERE user_data_128=1000 AND code=5
        \\ query_transfers U1000 U0 U0 L0 C5 _ _ L-0 _
        \\ commit query_transfers

        // WHERE user_data_128=1000 AND user_data_64=10
        // AND user_data_32=1 AND ledger=1 AND code=2:
        \\ query_transfers U1000 U10 U1 L1 C2 _ _ L-0 _
        \\ commit query_transfers

        // WHERE user_data_128=1000 AND timestamp BETWEEN T5.timestamp AND T7.timestamp:
        \\ query_transfers U1000 U0 U0 L0 C0 A5 A7 L-0 _
        \\ commit query_transfers
    );
}

test "get_change_events" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ account A4  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ created
        \\ commit create_accounts

        // First phase.
        \\ transfer   T1 A1 A2   10   _  _  _  _    _ L1 C1   _   _   _   _   _   _  _   _   _ _ _ created // Not pending.
        \\ transfer   T2 A1 A2   11   _  _  _  _    0 L1 C1   _ PEN   _   _   _   _  _   _   _ _ _ created // Timeout zero will never expire.
        \\ transfer   T3 A1 A2   12   _  _  _  _    1 L1 C1   _ PEN   _   _   _   _  _   _   _ _ _ created // Will expire.
        \\ transfer   T4 A1 A2   13   _  _  _  _    2 L1 C1   _ PEN   _   _   _   _  _   _   _ _ _ created // Will be posted.
        \\ transfer   T5 A1 A2   14   _  _  _  _    2 L1 C1   _ PEN   _   _   _   _  _   _   _ _ _ created // Will be voided.
        // Closes the debit and credit accounts.
        \\ transfer   T6 A3 A1    0   _  _  _  _    0 L1 C1   _ PEN   _   _   _   _  _ CDR   _ _ _ created
        \\ transfer   T7 A1 A4    0   _  _  _  _    0 L1 C1   _ PEN   _   _   _   _  _   _ CCR _ _ created
        \\ commit create_transfers

        // Bump the state machine time in +1s for testing the timeout expiration.
        \\ tick 1 seconds

        // Second phase.
        \\ transfer   T14 A0 A0   -0  T4  _  _  _    _ L0 C0   _   _ POS   _   _   _  _ _ _ _ _ created // Posts T4.
        \\ transfer   T15 A0 A0    0  T5  _  _  _    _ L0 C0   _   _   _ VOI   _   _  _ _ _ _ _ created // Voids T5.
        // Reopens the debit and credit accounts.
        \\ transfer   T16 A0 A0    0  T6  _  _  _    _ L0 C0   _   _   _ VOI   _   _  _ _ _ _ _ created
        \\ transfer   T17 A0 A0    0  T7  _  _  _    _ L0 C0   _   _   _ VOI   _   _  _ _ _ _ _ created
        \\ commit create_transfers

        // Check the events.
        \\ get_change_events _ T6 5
        \\ get_change_events_result   _  T1 10  _ D1  0 10  0  0 _ C2  0  0  0 10 _
        \\ get_change_events_result PEN  T2 11  _ D1 11 10  0  0 _ C2  0  0 11 10 _
        \\ get_change_events_result PEN  T3 12  _ D1 23 10  0  0 _ C2  0  0 23 10 _
        \\ get_change_events_result PEN  T4 13  _ D1 36 10  0  0 _ C2  0  0 36 10 _
        \\ get_change_events_result PEN  T5 14  _ D1 50 10  0  0 _ C2  0  0 50 10 _
        \\ commit get_change_events
        \\
        \\ get_change_events T6 _ -0
        \\ get_change_events_result PEN  T6  0  _ D3  0  0  0  0 CLSD A1 50 10  0  0 _
        \\ get_change_events_result PEN  T7  0  _ D1 50 10  0  0    _ C4  0  0  0  0 CLSD
        \\ get_change_events_result EXP   _ 12 T3 D1 38 10  0  0    _ C2  0  0 38 10 _
        \\ get_change_events_result POS T14 13 T4 D1 25 23  0  0    _ C2  0  0 25 23 _
        \\ get_change_events_result VOI T15 14 T5 D1 11 23  0  0    _ C2  0  0 11 23 _
        \\ get_change_events_result VOI T16  0 T6 D3  0  0  0  0    _ C1 11 23  0  0 _
        \\ get_change_events_result VOI T17  0 T7 D1 11 23  0  0    _ C4  0  0  0  0 _
        \\ commit get_change_events
    );
}

// Sanity test to check the maximum batch size.
// For a comprehensive test of all operations, see the `input_valid` test.
test "StateMachine: batch_elements_max" {
    const Operation = vsr.tigerbeetle.Operation;

    const events_max: u32 = @divExact(
        constants.message_body_size_max,
        @max(@sizeOf(Account), @sizeOf(Transfer)),
    );

    // No multi-batch encode.
    try testing.expectEqual(events_max, Operation.deprecated_create_accounts_unbatched.event_max(
        constants.message_body_size_max,
    ));
    try testing.expectEqual(events_max, Operation.deprecated_lookup_accounts_unbatched.event_max(
        constants.message_body_size_max,
    ));
    try testing.expectEqual(events_max, Operation.deprecated_create_transfers_unbatched.event_max(
        constants.message_body_size_max,
    ));
    try testing.expectEqual(events_max, Operation.deprecated_lookup_transfers_unbatched.event_max(
        constants.message_body_size_max,
    ));

    // Multi-batch encoded (the size corresponding to one element is occupied by the trailer).
    try testing.expectEqual(events_max - 1, Operation.create_accounts.event_max(
        constants.message_body_size_max,
    ));
    try testing.expectEqual(events_max - 1, Operation.create_transfers.event_max(
        constants.message_body_size_max,
    ));
    try testing.expectEqual(events_max - 1, Operation.lookup_accounts.event_max(
        constants.message_body_size_max,
    ));
    try testing.expectEqual(events_max - 1, Operation.lookup_transfers.event_max(
        constants.message_body_size_max,
    ));
}

// Tests the input validation logic for both multi-batch encoded messages and
// the former single-batch format.
test "StateMachine: input_valid" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();

    const input = try arena.allocator().alignedAlloc(
        u8,
        constants.cache_line_size,
        2 * constants.message_body_size_max,
    );

    const build_input = struct {
        fn build_input(buffer: []align(constants.cache_line_size) u8, options: struct {
            operation: StateMachine.Operation,
            event_count: u32,
        }) []align(constants.cache_line_size) const u8 {
            const event_size = options.operation.event_size();
            const payload_size: u32 = options.event_count * event_size;
            if (options.operation.is_multi_batch()) {
                var body_encoder: MultiBatchEncoder = .init(buffer, .{
                    .element_size = event_size,
                });
                assert(payload_size <= body_encoder.writable().?.len);
                body_encoder.add(payload_size);
                const bytes_written = body_encoder.finish();
                assert(bytes_written > 0);
                return buffer[0..bytes_written];
            }

            return buffer[0..payload_size];
        }
    }.build_input;

    var context: TestContext = undefined;
    try context.init(arena.allocator(), struct {
        fn callback(
            _: *TestContext,
            _: StateMachine.Operation,
            _: *Packet,
            _: u64,
            _: []const u8,
        ) !void {
            unreachable;
        }
    }.callback);

    const operations = std.enums.values(StateMachine.Operation);
    for (operations) |operation| {
        if (operation == .pulse) continue;
        const event_size = operation.event_size();
        maybe(event_size == 0);

        const event_min: u32, const event_max: u32 = limits: {
            if (event_size == 0) {
                break :limits .{ 0, 0 };
            }
            if (!operation.is_batchable()) {
                break :limits .{ 1, 1 };
            }
            break :limits .{
                0,
                operation.event_max(context.state_machine.batch_size_limit),
            };
        };
        assert(event_min <= event_max);

        try std.testing.expect(context.state_machine.input_valid(
            operation,
            build_input(input, .{
                .event_count = 0,
                .operation = operation,
            }),
        ) == (event_min == 0));
        if (event_size == 0) {
            assert(event_min == 0);
            assert(event_max == 0);
            continue;
        }

        try std.testing.expect(context.state_machine.input_valid(
            operation,
            build_input(input, .{
                .event_count = 1,
                .operation = operation,
            }),
        ));
        try std.testing.expect(context.state_machine.input_valid(
            operation,
            build_input(input, .{
                .event_count = event_max,
                .operation = operation,
            }),
        ));
        const too_much_data = build_input(input, .{
            .event_count = event_max + 1,
            .operation = operation,
        });
        if (too_much_data.len < constants.message_body_size_max) {
            try std.testing.expect(!context.state_machine.input_valid(
                operation,
                too_much_data,
            ));
        } else {
            // Don't test input larger than the message body limit, since input_valid()
            // would panic on an assert.
        }
    }
}

// Tests multi-batched query filters.
// Multi-batch filters are valid as long as the sum of `filter.limit` stays within the maximum
// number of results that can fit in the reply message.
test "StateMachine: query multi-batch input_valid" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();

    const input = try arena.allocator().alignedAlloc(
        u8,
        constants.cache_line_size,
        2 * constants.message_body_size_max,
    );

    var context: TestContext = undefined;
    try context.init(arena.allocator(), struct {
        fn callback(
            _: *TestContext,
            _: StateMachine.Operation,
            _: *Packet,
            _: u64,
            _: []const u8,
        ) !void {
            unreachable;
        }
    }.callback);

    const build_input = struct {
        fn build_input(
            operation: StateMachine.Operation,
            limits: []const u32,
            buffer: []align(constants.cache_line_size) u8,
        ) []align(constants.cache_line_size) const u8 {
            switch (operation) {
                .get_account_transfers,
                .get_account_balances,
                => {
                    var body_encoder: MultiBatchEncoder = .init(buffer, .{
                        .element_size = @sizeOf(AccountFilter),
                    });
                    if (limits.len == 0) body_encoder.add(0) else for (limits) |limit| {
                        const batch: []u8 = body_encoder.writable().?;
                        const filter: *AccountFilter = @alignCast(std.mem.bytesAsValue(
                            AccountFilter,
                            batch[0..@sizeOf(AccountFilter)],
                        ));
                        filter.* = .{
                            .account_id = 0,
                            .user_data_128 = 0,
                            .user_data_64 = 0,
                            .user_data_32 = 0,
                            .code = 0,
                            .timestamp_min = 0,
                            .timestamp_max = 0,
                            .limit = limit,
                            .flags = .{
                                .debits = false,
                                .credits = false,
                                .reversed = false,
                            },
                        };
                        body_encoder.add(@sizeOf(AccountFilter));
                    }
                    return buffer[0..body_encoder.finish()];
                },
                .query_accounts,
                .query_transfers,
                => {
                    var body_encoder: MultiBatchEncoder = .init(buffer, .{
                        .element_size = @sizeOf(QueryFilter),
                    });
                    if (limits.len == 0) body_encoder.add(0) else for (limits) |limit| {
                        const batch: []u8 = body_encoder.writable().?;
                        const filter: *QueryFilter = @alignCast(std.mem.bytesAsValue(
                            QueryFilter,
                            batch[0..@sizeOf(QueryFilter)],
                        ));
                        filter.* = .{
                            .user_data_128 = 0,
                            .user_data_64 = 0,
                            .user_data_32 = 0,
                            .code = 0,
                            .ledger = 0,
                            .timestamp_min = 0,
                            .timestamp_max = 0,
                            .limit = limit,
                            .flags = .{
                                .reversed = false,
                            },
                        };
                        body_encoder.add(@sizeOf(QueryFilter));
                    }
                    return buffer[0..body_encoder.finish()];
                },
                else => unreachable,
            }
        }
    }.build_input;

    const operations = &[_]StateMachine.Operation{
        .get_account_transfers,
        .get_account_balances,
        .query_accounts,
        .query_transfers,
    };

    for (operations) |operation| {
        const batch_max = operation.result_max(context.state_machine.batch_size_limit);

        // Valid inputs:
        try std.testing.expect(context.state_machine.input_valid(
            operation,
            build_input(operation, &.{0}, input),
        ));
        try std.testing.expect(context.state_machine.input_valid(
            operation,
            build_input(operation, &.{ 0, 0 }, input),
        ));
        try std.testing.expect(context.state_machine.input_valid(
            operation,
            build_input(operation, &.{1}, input),
        ));
        try std.testing.expect(context.state_machine.input_valid(
            operation,
            build_input(operation, &.{ 1, 1, 1 }, input),
        ));
        try std.testing.expect(context.state_machine.input_valid(
            operation,
            build_input(operation, &.{batch_max}, input),
        ));
        try std.testing.expect(context.state_machine.input_valid(
            operation,
            build_input(operation, &.{ 0, batch_max }, input),
        ));
        try std.testing.expect(context.state_machine.input_valid(
            operation,
            build_input(operation, &.{ 0, 1, batch_max - 1 }, input),
        ));
        try std.testing.expect(context.state_machine.input_valid(
            operation,
            build_input(operation, &.{ 1, 1, batch_max - 2 }, input),
        ));
        try std.testing.expect(context.state_machine.input_valid(
            operation,
            build_input(operation, &.{
                @divFloor(batch_max, 2),
                stdx.div_ceil(batch_max, 2),
            }, input),
        ));
        try std.testing.expect(context.state_machine.input_valid(
            operation,
            build_input(operation, &.{std.math.maxInt(u32)}, input),
        ));

        // Invalid inputs:
        try std.testing.expect(!context.state_machine.input_valid(
            operation,
            build_input(operation, &.{}, input),
        ));
        try std.testing.expect(!context.state_machine.input_valid(
            operation,
            build_input(operation, &.{ 1, batch_max }, input),
        ));
        try std.testing.expect(!context.state_machine.input_valid(
            operation,
            build_input(operation, &.{ 1, std.math.maxInt(u32) }, input),
        ));
        try std.testing.expect(!context.state_machine.input_valid(
            operation,
            build_input(operation, &.{ batch_max, batch_max }, input),
        ));
        try std.testing.expect(!context.state_machine.input_valid(
            operation,
            build_input(operation, &.{
                @divFloor(batch_max, 2),
                stdx.div_ceil(batch_max, 2),
                1,
            }, input),
        ));
    }
}
