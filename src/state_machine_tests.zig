const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const stdx = @import("stdx");
const maybe = stdx.maybe;

const tb = @import("tigerbeetle.zig");
const vsr = @import("vsr.zig");
const constants = vsr.constants;

const MultiBatchEncoder = vsr.multi_batch.MultiBatchEncoder;
const MultiBatchDecoder = vsr.multi_batch.MultiBatchDecoder;

const TimestampRange = @import("lsm/timestamp_range.zig").TimestampRange;

const Account = tb.Account;
const AccountBalance = tb.AccountBalance;
const Transfer = tb.Transfer;
const CreateAccountsResult = tb.CreateAccountsResult;
const CreateTransfersResult = tb.CreateTransfersResult;

const CreateAccountResult = tb.CreateAccountResult;
const CreateTransferResult = tb.CreateTransferResult;

const AccountFilter = tb.AccountFilter;
const QueryFilter = tb.QueryFilter;
const ChangeEventsFilter = tb.ChangeEventsFilter;
const ChangeEvent = tb.ChangeEvent;
const ChangeEventType = tb.ChangeEventType;

const StateMachineType = @import("state_machine.zig").StateMachineType;

const testing = std.testing;

pub const TestContext = struct {
    const TimeSim = @import("testing/time.zig").TimeSim;
    const Storage = @import("testing/storage.zig").Storage;
    const Tracer = Storage.Tracer;
    const data_file_size_min = @import("vsr/superblock.zig").data_file_size_min;
    const SuperBlock = @import("vsr/superblock.zig").SuperBlockType(Storage);
    const Grid = @import("vsr/grid.zig").GridType(Storage);
    const fixtures = @import("testing/fixtures.zig");

    pub const StateMachine = StateMachineType(Storage);

    pub const Operation = enum {
        create_accounts,
        create_transfers,
        lookup_accounts,
        lookup_transfers,
        get_account_transfers,
        get_account_balances,
        query_accounts,
        query_transfers,
        get_change_events,

        const VersionMap = std.EnumArray(Operation, StateMachine.Operation);
        /// Variations of operations supported by the state machine,
        /// including deprecated ones used by old clients.
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

    storage: Storage,
    time_sim: TimeSim,
    trace: Tracer,
    superblock: SuperBlock,
    grid: Grid,
    state_machine: StateMachine,
    op: u64,
    busy: bool,

    pub fn init(ctx: *TestContext, allocator: mem.Allocator) !void {
        ctx.storage = try fixtures.init_storage(allocator, .{ .size = 4096 });
        errdefer ctx.storage.deinit(allocator);

        ctx.time_sim = fixtures.init_time(.{});

        ctx.trace = try fixtures.init_tracer(allocator, ctx.time_sim.time(), .{});
        errdefer ctx.trace.deinit(allocator);

        ctx.superblock = try fixtures.init_superblock(allocator, &ctx.storage, .{
            .storage_size_limit = data_file_size_min,
        });
        errdefer ctx.superblock.deinit(allocator);

        // Pretend that the superblock is open so that the Forest can initialize.
        ctx.superblock.opened = true;
        ctx.superblock.working.vsr_state.checkpoint.header.op = 0;

        ctx.grid = try fixtures.init_grid(allocator, &ctx.trace, &ctx.superblock, .{});
        errdefer ctx.grid.deinit(allocator);

        const batch_size_limit = 30 * @max(@sizeOf(Account), @sizeOf(Transfer));
        assert(batch_size_limit <= constants.message_body_size_max);
        try ctx.state_machine.init(
            allocator,
            ctx.time_sim.time(),
            &ctx.grid,
            .{
                .batch_size_limit = batch_size_limit,
                .lsm_forest_compaction_block_count = StateMachine.Forest.Options
                    .compaction_block_count_min,
                .lsm_forest_node_count = 1,
                .cache_entries_accounts = 0,
                .cache_entries_transfers = 0,
                .cache_entries_transfers_pending = 0,
                .log_trace = true,
            },
        );
        errdefer ctx.state_machine.deinit(allocator);
        // Usually, `pulse_next_timestamp` starts in an unknown state, signaling that the state
        // machine needs a `pulse` to scan for pending transfers and correctly determine when to
        // process the next expiry. However, this initial `pulse` unnecessarily bumps time, making
        // unit tests that depend on the `timestamp` harder to reason about.
        //
        // Since this is a newly created state machine, we can bypass the initial check, ensuring
        // that there will be no `timestamp` bumps between operations unless actual pending
        // transfers get expired.
        ctx.state_machine.expire_pending_transfers
            .pulse_next_timestamp = TimestampRange.timestamp_max;

        ctx.op = 1;
        ctx.busy = false;
    }

    pub fn deinit(ctx: *TestContext, allocator: mem.Allocator) void {
        ctx.state_machine.deinit(allocator);
        ctx.grid.deinit(allocator);
        ctx.superblock.deinit(allocator);
        ctx.trace.deinit(allocator);
        ctx.storage.deinit(allocator);
        ctx.* = undefined;
    }

    fn callback(state_machine: *StateMachine) void {
        const ctx: *TestContext = @fieldParentPtr("state_machine", state_machine);
        assert(ctx.busy);
        ctx.busy = false;
    }

    fn submit(
        context: *TestContext,
        operation: TestContext.StateMachine.Operation,
        input_buffer: []align(16) u8,
        input_size: u32,
        output_buffer: *align(16) [constants.message_body_size_max]u8,
    ) []const u8 {
        const message_body: []align(16) const u8 = message_body: {
            if (!operation.is_multi_batch()) {
                break :message_body input_buffer[0..input_size];
            }
            assert(operation.is_multi_batch());
            const event_size = operation.event_size();
            var body_encoder = MultiBatchEncoder.init(input_buffer, .{
                .element_size = event_size,
            });
            body_encoder.add(input_size);
            const bytes_written = body_encoder.finish();
            assert(bytes_written > 0);
            break :message_body input_buffer[0..bytes_written];
        };
        context.prepare(operation, message_body);

        const pulse_needed = context.state_machine.pulse_needed(
            context.state_machine.prepare_timestamp,
        );
        maybe(pulse_needed);
        // Pulse is executed in a best-effort manner
        // after committing the current pipelined operation.
        defer if (pulse_needed) context.pulse();

        const reply_actual_size = context.execute(
            context.op,
            operation,
            message_body,
            output_buffer,
        );

        if (!operation.is_multi_batch()) {
            return output_buffer[0..reply_actual_size];
        }
        assert(operation.is_multi_batch());

        const result_size = operation.result_size();
        var reply_decoder = MultiBatchDecoder.init(
            output_buffer[0..reply_actual_size],
            .{ .element_size = result_size },
        ) catch unreachable;
        assert(reply_decoder.batch_count() == 1);
        return reply_decoder.peek();
    }

    pub fn prepare(
        context: *TestContext,
        operation: TestContext.StateMachine.Operation,
        message_body_used: []align(16) const u8,
    ) void {
        context.state_machine.commit_timestamp = context.state_machine.prepare_timestamp;
        context.state_machine.prepare_timestamp += 1;
        context.state_machine.prepare(
            operation,
            message_body_used,
        );
    }

    fn pulse(context: *TestContext) void {
        if (context.state_machine.pulse_needed(context.state_machine.prepare_timestamp)) {
            const operation = vsr.Operation.pulse.cast(TestContext.StateMachine.Operation);
            context.prepare(operation, &.{});
            const pulse_size = context.execute(
                context.op,
                operation,
                &.{},
                undefined, // Output is never used for pulse.
            );
            assert(pulse_size == 0);
            context.op += 1;
        }
    }

    pub fn execute(
        context: *TestContext,
        op: u64,
        operation: TestContext.StateMachine.Operation,
        message_body_used: []align(16) const u8,
        output_buffer: *align(16) [constants.message_body_size_max]u8,
    ) usize {
        const timestamp = context.state_machine.prepare_timestamp;
        context.busy = true;
        context.state_machine.prefetch_timestamp = timestamp;
        context.state_machine.prefetch(
            TestContext.callback,
            op,
            operation,
            message_body_used,
        );
        while (context.busy) context.storage.run();

        return context.state_machine.commit(
            1,
            op,
            timestamp,
            operation,
            message_body_used,
            output_buffer,
        );
    }

    fn get_account_from_cache(context: *TestContext, id: u128) ?Account {
        return switch (context.state_machine.forest.grooves.accounts.get(id)) {
            .found_object => |a| a,
            .found_orphaned_id => unreachable,
            .not_found => null,
        };
    }
};

const TestAction = union(enum) {
    // Set the account's balance.
    setup: struct {
        account: u128,
        debits_pending: u128,
        debits_posted: u128,
        credits_pending: u128,
        credits_posted: u128,
    },

    tick: struct {
        value: i64,
        unit: enum { nanoseconds, seconds },
    },

    commit: TestContext.Operation,
    account: TestCreateAccount,
    transfer: TestCreateTransfer,

    lookup_account: struct {
        id: u128,
        data: ?struct {
            debits_pending: u128,
            debits_posted: u128,
            credits_pending: u128,
            credits_posted: u128,
            flag_closed: ?enum { CLSD } = null,
        } = null,
    },
    lookup_transfer: struct {
        id: u128,
        data: union(enum) {
            exists: bool,
            amount: u128,
            timestamp: u64,
        },
    },

    get_account_balances: TestGetAccountBalances,
    get_account_balances_result: struct {
        transfer_id: u128,
        debits_pending: u128,
        debits_posted: u128,
        credits_pending: u128,
        credits_posted: u128,
    },

    get_account_transfers: TestGetAccountTransfers,
    get_account_transfers_result: u128,

    query_accounts: TestQueryAccounts,
    query_accounts_result: struct {
        id: u128,
        data: ?struct {
            debits_pending: u128,
            debits_posted: u128,
            credits_pending: u128,
            credits_posted: u128,
            flag_closed: ?enum { CLSD } = null,
        } = null,
    },

    query_transfers: TestQueryTransfers,
    query_transfers_result: u128,

    get_change_events: TestGetChangeEventsFilter,
    get_change_events_result: TestGetChangeEventsResult,
};

const TestCreateAccount = struct {
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
    result: CreateAccountResult,

    fn event(a: TestCreateAccount) Account {
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
                .debits_must_not_exceed_credits = a.flags_debits_must_not_exceed_credits != null,
                .credits_must_not_exceed_debits = a.flags_credits_must_not_exceed_debits != null,
                .history = a.flags_history != null,
                .imported = a.flags_imported != null,
                .closed = a.flags_closed != null,
                .padding = a.flags_padding,
            },
            .timestamp = a.timestamp,
        };
    }
};

const TestCreateTransfer = struct {
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
    result: CreateTransferResult,

    fn event(t: TestCreateTransfer) Transfer {
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

const TestAccountFilter = struct {
    account_id: u128,
    user_data_128: ?u128 = null,
    user_data_64: ?u64 = null,
    user_data_32: ?u32 = null,
    code: ?u16 = null,
    // When non-null, the filter is set to the timestamp at which the specified transfer (by id) was
    // created.
    timestamp_min_transfer_id: ?u128 = null,
    timestamp_max_transfer_id: ?u128 = null,
    limit: u32,
    flags_debits: ?enum { DR } = null,
    flags_credits: ?enum { CR } = null,
    flags_reversed: ?enum { REV } = null,
};

const TestQueryFilter = struct {
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

const TestGetChangeEventsFilter = struct {
    timestamp_min_transfer_id: ?u128 = null,
    timestamp_max_transfer_id: ?u128 = null,
    limit: u32,
};

const TestGetChangeEventsResult = struct {
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
        self: *const TestGetChangeEventsResult,
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
        if (self.dr_account.credits_pending != event.debit_account_credits_pending) return false;
        if (self.dr_account.credits_posted != event.debit_account_credits_posted) return false;
        if ((self.dr_account.closed == .CLSD) != event.debit_account_flags.closed) return false;

        const cr_account = accounts.get(self.cr_account.account_id).?;
        if (cr_account.ledger != event.ledger) return false;
        if (self.cr_account.account_id != event.credit_account_id) return false;
        if (cr_account.timestamp != event.credit_account_timestamp) return false;
        if (self.cr_account.debits_pending != event.credit_account_debits_pending) return false;
        if (self.cr_account.debits_posted != event.credit_account_debits_posted) return false;
        if (self.cr_account.credits_pending != event.credit_account_credits_pending) return false;
        if (self.cr_account.credits_posted != event.credit_account_credits_posted) return false;
        if ((self.cr_account.closed == .CLSD) != event.credit_account_flags.closed) return false;

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

// Operations that share the same input.
const TestGetAccountBalances = TestAccountFilter;
const TestGetAccountTransfers = TestAccountFilter;
const TestQueryAccounts = TestQueryFilter;
const TestQueryTransfers = TestQueryFilter;

fn check(test_table: []const u8) !void {
    const parse_table = @import("testing/table.zig").parse;
    const test_actions = parse_table(TestAction, test_table);

    // Runs the same test for each variation of supported operations,
    // simulating different client versions.
    for (TestContext.Operation.versions) |*version_map| {
        try check_version(
            test_actions.const_slice(),
            version_map,
        );
    }
}

fn check_version(
    test_actions: []const TestAction,
    version_map: *const TestContext.Operation.VersionMap,
) !void {
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
    try request.ensureTotalCapacity(constants.message_body_size_max);

    var reply = std.ArrayListAligned(u8, 16).init(allocator);
    defer reply.deinit();

    var operation: ?TestContext.Operation = null;
    for (test_actions) |test_action| {
        switch (test_action) {
            .setup => |b| {
                assert(operation == null);

                const account = context.get_account_from_cache(b.account).?;
                var account_new = account;

                account_new.debits_pending = b.debits_pending;
                account_new.debits_posted = b.debits_posted;
                account_new.credits_pending = b.credits_pending;
                account_new.credits_posted = b.credits_posted;
                assert(!account_new.debits_exceed_credits(0));
                assert(!account_new.credits_exceed_debits(0));

                if (!stdx.equal_bytes(Account, &account_new, &account)) {
                    context.state_machine.forest.grooves.accounts.update(.{
                        .old = &account,
                        .new = &account_new,
                    });
                }
            },
            .tick => |ticks| {
                assert(ticks.value != 0);

                const interval_ns: u64 = @abs(ticks.value) *
                    @as(u64, switch (ticks.unit) {
                        .nanoseconds => 1,
                        .seconds => std.time.ns_per_s,
                    });

                // The `parse` logic already computes `maxInt - value` when a unsigned int is
                // represented as a negative number. However, we need to use a signed int and
                // perform our own calculation to account for the unit.
                context.state_machine.prepare_timestamp += if (ticks.value > 0)
                    interval_ns
                else
                    TimestampRange.timestamp_max - interval_ns;

                // Pulse is executed when the cluster is idle.
                context.pulse();
            },
            .account => |a| {
                assert(operation == null or operation.? == .create_accounts);
                operation = .create_accounts;

                var event = a.event();
                try request.appendSlice(std.mem.asBytes(&event));
                if (a.result == .ok) {
                    if (a.timestamp == 0) {
                        event.timestamp = context.state_machine.prepare_timestamp + 1 +
                            @divExact(request.items.len, @sizeOf(Account));
                    }

                    try accounts.put(a.id, event);
                } else {
                    const result = CreateAccountsResult{
                        .index = @intCast(@divExact(request.items.len, @sizeOf(Account)) - 1),
                        .result = a.result,
                    };
                    try reply.appendSlice(std.mem.asBytes(&result));
                }
            },
            .transfer => |t| {
                assert(operation == null or operation.? == .create_transfers);
                operation = .create_transfers;

                var event = t.event();
                try request.appendSlice(std.mem.asBytes(&event));
                if (t.result == .ok) {
                    if (t.timestamp == 0) {
                        event.timestamp = context.state_machine.prepare_timestamp + 1 +
                            @divExact(request.items.len, @sizeOf(Transfer));
                    }

                    if (event.pending_id != 0) {
                        // Fill in default values.
                        const t_pending = transfers.get(event.pending_id).?;
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
                    try transfers.put(t.id, event);
                } else {
                    const result = CreateTransfersResult{
                        .index = @intCast(@divExact(request.items.len, @sizeOf(Transfer)) - 1),
                        .result = t.result,
                    };
                    try reply.appendSlice(std.mem.asBytes(&result));
                }
            },
            .lookup_account => |a| {
                assert(operation == null or operation.? == .lookup_accounts);
                operation = .lookup_accounts;

                try request.appendSlice(std.mem.asBytes(&a.id));
                if (a.data) |data| {
                    var account = accounts.get(a.id).?;
                    account.debits_pending = data.debits_pending;
                    account.debits_posted = data.debits_posted;
                    account.credits_pending = data.credits_pending;
                    account.credits_posted = data.credits_posted;
                    account.flags.closed = data.flag_closed != null;
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
                    .timestamp => |timestamp| {
                        var transfer = transfers.get(t.id).?;
                        transfer.timestamp = timestamp;
                        try reply.appendSlice(std.mem.asBytes(&transfer));
                    },
                }
            },
            .get_account_balances => |f| {
                assert(operation == null or operation.? == .get_account_balances);
                operation = .get_account_balances;

                const timestamp_min =
                    if (f.timestamp_min_transfer_id) |id| transfers.get(id).?.timestamp else 0;
                const timestamp_max =
                    if (f.timestamp_max_transfer_id) |id| transfers.get(id).?.timestamp else 0;

                const event = AccountFilter{
                    .account_id = f.account_id,
                    .user_data_128 = f.user_data_128 orelse 0,
                    .user_data_64 = f.user_data_64 orelse 0,
                    .user_data_32 = f.user_data_32 orelse 0,
                    .code = f.code orelse 0,
                    .timestamp_min = timestamp_min,
                    .timestamp_max = timestamp_max,
                    .limit = f.limit,
                    .flags = .{
                        .debits = f.flags_debits != null,
                        .credits = f.flags_credits != null,
                        .reversed = f.flags_reversed != null,
                    },
                };
                try request.appendSlice(std.mem.asBytes(&event));
            },
            .get_account_balances_result => |r| {
                assert(operation.? == .get_account_balances);

                const balance = AccountBalance{
                    .debits_pending = r.debits_pending,
                    .debits_posted = r.debits_posted,
                    .credits_pending = r.credits_pending,
                    .credits_posted = r.credits_posted,
                    .timestamp = transfers.get(r.transfer_id).?.timestamp,
                };
                try reply.appendSlice(std.mem.asBytes(&balance));
            },
            .get_account_transfers => |f| {
                assert(operation == null or operation.? == .get_account_transfers);
                operation = .get_account_transfers;

                const timestamp_min =
                    if (f.timestamp_min_transfer_id) |id| transfers.get(id).?.timestamp else 0;
                const timestamp_max =
                    if (f.timestamp_max_transfer_id) |id| transfers.get(id).?.timestamp else 0;

                const event = AccountFilter{
                    .account_id = f.account_id,
                    .user_data_128 = f.user_data_128 orelse 0,
                    .user_data_64 = f.user_data_64 orelse 0,
                    .user_data_32 = f.user_data_32 orelse 0,
                    .code = f.code orelse 0,
                    .timestamp_min = timestamp_min,
                    .timestamp_max = timestamp_max,
                    .limit = f.limit,
                    .flags = .{
                        .debits = f.flags_debits != null,
                        .credits = f.flags_credits != null,
                        .reversed = f.flags_reversed != null,
                    },
                };
                try request.appendSlice(std.mem.asBytes(&event));
            },
            .get_account_transfers_result => |id| {
                assert(operation.? == .get_account_transfers);
                try reply.appendSlice(std.mem.asBytes(&transfers.get(id).?));
            },
            .query_accounts => |f| {
                assert(operation == null or operation.? == .query_accounts);
                operation = .query_accounts;

                const timestamp_min = if (f.timestamp_min_transfer_id) |id|
                    accounts.get(id).?.timestamp
                else
                    0;
                const timestamp_max = if (f.timestamp_max_transfer_id) |id|
                    accounts.get(id).?.timestamp
                else
                    0;

                const event = QueryFilter{
                    .user_data_128 = f.user_data_128,
                    .user_data_64 = f.user_data_64,
                    .user_data_32 = f.user_data_32,
                    .ledger = f.ledger,
                    .code = f.code,
                    .timestamp_min = timestamp_min,
                    .timestamp_max = timestamp_max,
                    .limit = f.limit,
                    .flags = .{
                        .reversed = f.flags_reversed != null,
                    },
                };
                try request.appendSlice(std.mem.asBytes(&event));
            },
            .query_accounts_result => |a| {
                assert(operation.? == .query_accounts);
                var account = accounts.get(a.id).?;
                if (a.data) |data| {
                    account.debits_pending = data.debits_pending;
                    account.debits_posted = data.debits_posted;
                    account.credits_pending = data.credits_pending;
                    account.credits_posted = data.credits_posted;
                    account.flags.closed = data.flag_closed != null;
                }
                try reply.appendSlice(std.mem.asBytes(&account));
            },
            .query_transfers => |f| {
                assert(operation == null or operation.? == .query_transfers);
                operation = .query_transfers;

                const timestamp_min = if (f.timestamp_min_transfer_id) |id|
                    transfers.get(id).?.timestamp
                else
                    0;
                const timestamp_max = if (f.timestamp_max_transfer_id) |id|
                    transfers.get(id).?.timestamp
                else
                    0;

                const event = QueryFilter{
                    .user_data_128 = f.user_data_128,
                    .user_data_64 = f.user_data_64,
                    .user_data_32 = f.user_data_32,
                    .ledger = f.ledger,
                    .code = f.code,
                    .timestamp_min = timestamp_min,
                    .timestamp_max = timestamp_max,
                    .limit = f.limit,
                    .flags = .{
                        .reversed = f.flags_reversed != null,
                    },
                };
                try request.appendSlice(std.mem.asBytes(&event));
            },
            .query_transfers_result => |id| {
                assert(operation.? == .query_transfers);
                try reply.appendSlice(std.mem.asBytes(&transfers.get(id).?));
            },
            .get_change_events => |f| {
                assert(operation == null or operation.? == .get_change_events);
                operation = .get_change_events;
                const timestamp_min = if (f.timestamp_min_transfer_id) |id|
                    transfers.get(id).?.timestamp
                else
                    0;
                const timestamp_max = if (f.timestamp_max_transfer_id) |id|
                    transfers.get(id).?.timestamp
                else
                    0;

                const event = ChangeEventsFilter{
                    .timestamp_min = timestamp_min,
                    .timestamp_max = timestamp_max,
                    .limit = f.limit,
                };
                try request.appendSlice(std.mem.asBytes(&event));
            },
            .get_change_events_result => |*t| {
                assert(operation.? == .get_change_events);
                try reply.appendSlice(std.mem.asBytes(t));
            },
            .commit => |commit_operation| {
                assert(operation == null or operation.? == commit_operation);
                assert(!context.busy);

                const reply_actual_buffer = try allocator.alignedAlloc(
                    u8,
                    16,
                    constants.message_body_size_max,
                );
                defer allocator.free(reply_actual_buffer);
                const payload_size: u32 = @intCast(request.items.len);
                request.expandToCapacity();

                const operation_actual = version_map.get(commit_operation);
                const reply_actual = context.submit(
                    operation_actual,
                    request.items,
                    payload_size,
                    reply_actual_buffer[0..constants.message_body_size_max],
                );

                switch (operation_actual) {
                    inline else => |operation_actual_comptime| {
                        const Result = operation_actual_comptime.ResultType();
                        try testing.expectEqualSlices(
                            Result,
                            stdx.bytes_as_slice(.exact, Result, reply.items),
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
                            TestGetChangeEventsResult,
                            reply.items,
                        );
                        try testing.expectEqual(results_actual.len, results_expected.len);
                        for (results_actual, results_expected) |*actual, *expected| {
                            try testing.expect(expected.match(&accounts, &transfers, actual));
                        }
                    },
                    .pulse => unreachable,
                }

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

test "create_accounts" {
    try check(
        \\ account A1  0  0  0  0 U2 U2 U2 _ L3 C4 _   _   _ _ _ _ _ _ ok
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
        \\ account A7  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok // An individual event (successful):

        // A chain of 4 events (the last event in the chain closes the chain with linked=false):
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ _ _ _ linked_event_failed // Commit/rollback.
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ _ _ _ linked_event_failed // Commit/rollback.
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ _ _ _ exists              // Fail with .exists.
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ linked_event_failed // Fail without committing.

        // An individual event (successful):
        // This does not see any effect from the failed chain above.
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok

        // A chain of 2 events (the first event fails the chain):
        \\ account A1  0  0  0  0  _  _  _ _ L1 C2 LNK   _   _ _ _ _ _ _ exists_with_different_flags
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ linked_event_failed

        // An individual event (successful):
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok

        // A chain of 2 events (the last event fails the chain):
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ _ _ _ linked_event_failed
        \\ account A1  0  0  0  0  _  _  _ _ L2 C1   _   _   _ _ _ _ _ _ exists_with_different_ledger

        // A chain of 2 events (successful):
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ _ _ _ ok
        \\ account A4  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
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
        \\ account A7  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok // An individual event (successful):

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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ _ _ _ ok
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok

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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok

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
// 1. all CreateTransferResult enums are covered, with
// 2. enums tested in the order that they are defined, for easier auditing of coverage, and that
// 3. state machine logic cannot be reordered in any way, breaking determinism.
test "create_transfers/lookup_transfers" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L2 C2   _   _   _ _ _ _ _ _ ok
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ account A4  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ ok
        \\ account A5  0  0  0  0  _  _  _ _ L1 C1   _   _ C<D _ _ _ _ _ ok
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
        \\ transfer   T5 A1 A3  123   _  _  _  _    1 L1 C1   _ PEN   _   _   _   _ _  _   _   _ _ ok
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
        \\ transfer   T6 A3 A1    7   _  _  _  _    _ L1 C2   _   _   _   _   _   _ _  _ _ _ _ ok
        \\ transfer   T7 A1 A3    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _ _  _ _ _ _ ok
        \\ transfer   T8 A1 A3    0   _  _  _  _    _ L1 C2   _   _   _   _   _   _ _  _ _ _ _ ok
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ commit create_accounts

        // First phase.
        \\ transfer   T1 A1 A2   15   _  _  _  _    _ L1 C1   _   _   _   _   _   _  _ _ _ _ _ ok // Not pending!
        \\ transfer   T2 A1 A2   15   _  _  _  _ 1000 L1 C1   _ PEN   _   _   _   _  _ _ _ _ _ ok
        \\ transfer   T3 A1 A2   15   _  _  _  _   50 L1 C1   _ PEN   _   _   _   _  _ _ _ _ _ ok
        \\ transfer   T4 A1 A2   15   _  _  _  _    1 L1 C1   _ PEN   _   _   _   _  _ _ _ _ _ ok
        \\ transfer   T5 A1 A2    7   _ U9 U9 U9   50 L1 C1   _ PEN   _   _   _   _  _ _ _ _ _ ok
        \\ transfer   T6 A1 A2    1   _  _  _  _    0 L1 C1   _ PEN   _   _   _   _  _ _ _ _ _ ok
        \\ transfer   T7 A1 A2    1   _  _  _  _    0 L1 C1   _ PEN   _   _   _   _  _ _ _ _ _ ok
        \\ commit create_transfers

        // Check balances before resolving.
        \\ lookup_account A1 54 15  0  0  _
        \\ lookup_account A2  0  0 54 15  _
        \\ commit lookup_accounts

        // Bump the state machine time in +1s for testing the timeout expiration.
        \\ tick 1 seconds

        // Second phase.
        \\ transfer T101 A1 A2   13  T2 U1 U1 U1    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ ok
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
        \\ transfer T103 A1 A2   15  T3 U1 U1 U1    _ L1 C1   _   _   _ VOI   _   _  _ _ _ _ _ ok
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
        \\ transfer T105 A0 A0   -0  T5 U0 U0 U0    _ L0 C0   _   _ POS   _   _   _  _ _ _ _ _ ok
        \\ transfer T105 A0 A0    7  T5 U0 U0 U0    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ exists
        \\ transfer T105 A0 A0    7  T5 U0 U0 U0    _ L0 C0   _   _ POS   _   _   _  _ _ _ _ _ exists // ledger/code = 0
        \\ transfer T105 A0 A0   -0  T5 U0 U0 U0    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ exists // amount = max
        \\ transfer T105 A0 A0    8  T5 U0 U0 U0    _ L0 C0   _   _ POS   _   _   _  _ _ _ _ _ exists_with_different_amount // t.amount > p.amount
        \\ transfer T105 A0 A0    6  T5 U0 U0 U0    _ L0 C0   _   _ POS   _   _   _  _ _ _ _ _ exists_with_different_amount // t.amount < e.amount
        \\ transfer T105 A0 A0    0  T5 U0 U0 U0    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ exists_with_different_amount
        \\
        \\ transfer T106 A0 A0   -1  T6 U0 U0 U0    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ exceeds_pending_transfer_amount
        \\ transfer T106 A0 A0   -0  T6 U0 U0 U0    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ ok
        \\ transfer T106 A0 A0   -0  T6 U0 U0 U0    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ exists
        \\ transfer T106 A0 A0    1  T6 U0 U0 U0    _ L0 C0   _   _ POS   _   _   _  _ _ _ _ _ exists
        \\ transfer T106 A0 A0    2  T6 U0 U0 U0    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ exists_with_different_amount // t.amount > p.amount
        \\ transfer T106 A0 A0    0  T6 U0 U0 U0    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ exists_with_different_amount // t.amount < p.amount
        \\
        \\ transfer T107 A0 A0    0  T7 U0 U0 U0    _ L0 C0   _   _ POS   _   _   _  _ _ _ _ _ ok
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ commit create_accounts

        // Posting maxInt(u128) is a pun – it is interpreted as "send full pending amount", which in
        // this case is exactly maxInt(u127).
        \\ transfer T1 A1 A2   -0   _  _  _  _ _ L1 C1   _ PEN   _   _   _   _  _ _ _ _ _ ok
        \\ transfer T2 A1 A2   -0  T1  _  _  _ _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ ok
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ commit create_accounts

        // First phase.
        \\ transfer   T1 A1 A2   10   _  _  _  _    0 L1 C1   _ PEN   _   _   _   _  _ _ _ _ _ ok // Timeout zero will never expire.
        \\ transfer   T2 A1 A2   11   _  _  _  _    1 L1 C1   _ PEN   _   _   _   _  _ _ _ _ _ ok
        \\ transfer   T3 A1 A2   12   _  _  _  _    2 L1 C1   _ PEN   _   _   _   _  _ _ _ _ _ ok
        \\ transfer   T4 A1 A2   13   _  _  _  _    3 L1 C1   _ PEN   _   _   _   _  _ _ _ _ _ ok
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
        \\ transfer T101 A1 A2   10  T1 U1 U1 U1    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ ok
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ commit create_accounts
        \\
        \\ transfer   T1 A1 A2   15   _  _  _  _    _ L1 C1   _   _   _   _   _   _  _ _ _ _ _ ok
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ commit create_accounts
        \\
        \\ setup A1 0 0 0 20
        \\
        \\ transfer   T1 A1 A2   15   _ _   _  _    _ L1 C1 LNK   _   _   _   _   _  _ _ _ _ _ linked_event_failed
        \\ transfer   T2 A1 A2    5   _ _   _  _    _ L0 C1   _   _   _   _   _   _  _ _ _ _ _ ledger_must_not_be_zero
        \\ transfer   T3 A1 A2   15   _ _   _  _    _ L1 C1   _   _   _   _   _   _  _ _ _ _ _ ok
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _ C<D _ _ _ _ _ ok
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ commit create_accounts
        \\
        \\ setup A1 1  0 0 10
        \\ setup A2 0 10 2  0
        \\
        \\ transfer   T1 A1 A3  3     _  _  _  _    _ L2 C1   _   _   _   _ BDR   _  _ _ _ _ _ transfer_must_have_the_same_ledger_as_accounts
        \\ transfer   T1 A3 A2  3     _  _  _  _    _ L2 C1   _   _   _   _   _ BCR  _ _ _ _ _ transfer_must_have_the_same_ledger_as_accounts
        \\ transfer   T1 A1 A3  3     _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ _ _ _ ok
        \\ transfer   T2 A1 A3 13     _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ _ _ _ ok
        \\ transfer   T3 A3 A2  3     _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ _ _ _ ok
        \\ transfer   T4 A3 A2 13     _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ _ _ _ ok
        \\ transfer   T5 A1 A3  1     _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ _ _ _ ok // Amount reduced to 0.
        \\ transfer   T6 A1 A3  1     _  _  _  _    _ L1 C1   _   _   _   _ BDR BCR  _ _ _ _ _ ok // ↑
        \\ transfer   T7 A3 A2  1     _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ _ _ _ ok // ↑
        \\ transfer   T8 A1 A2  1     _  _  _  _    _ L1 C1   _   _   _   _ BDR BCR  _ _ _ _ _ ok // ↑
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _ C<D _ _ _ _ _ ok
        \\ account A4  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ commit create_accounts
        \\
        \\ setup A1 0 0 0 4
        \\ setup A2 0 5 0 0
        \\ setup A3 0 4 0 0
        \\ setup A4 0 0 0 5
        \\
        \\ transfer   T1 A1 A2   10   _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ _ _ _ exceeds_credits
        \\ transfer   T2 A1 A2   10   _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ _ _ _ ok
        \\ transfer   T3 A4 A3   10   _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ _ _ _ exceeds_debits
        \\ transfer   T4 A4 A3   10   _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ _ _ _ ok
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ commit create_accounts
        \\
        \\ setup A1 1  0 0 10
        \\ setup A2 0 10 2  0
        \\
        \\ transfer   T1 A3 A1   99   _  _  _  _    _ L1 C1   _   _   _   _ BDR BCR  _ _ _ _ _ ok // Amount reduced to 0.
        \\ transfer   T2 A3 A1   99   _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ _ _ _ ok // ↑
        \\ transfer   T3 A2 A3   99   _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ _ _ _ ok // ↑
        \\ transfer   T4 A1 A3   99   _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ _ _ _ ok
        \\ transfer   T5 A1 A3   99   _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ _ _ _ ok // Amount reduced to 0.
        \\ transfer   T6 A3 A2   99   _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ _ _ _ ok
        \\ transfer   T7 A3 A2   99   _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ _ _ _ ok // Amount reduced to 0.
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _ C<D _ _ _ _ _ ok
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _ C<D _ _ _ _ _ ok
        \\ account A4  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ commit create_accounts
        \\
        \\ setup A1 1  0 0 10
        \\ setup A2 0 10 2  0
        \\ setup A3 0 10 2  0
        \\
        // Test amount=0 transfers:
        \\ transfer   T1 A1 A4    0   _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ _ _ _ ok
        \\ transfer   T2 A4 A2    0   _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ _ _ _ ok
        \\ transfer   T3 A1 A4    0   _  _  _  _    _ L1 C1   _   _   _   _ BDR BCR  _ _ _ _ _ ok
        \\ transfer   T4 A4 A3    0   _  _  _  _    _ L1 C1   _ PEN   _   _   _ BCR  _ _ _ _ _ ok
        // The respective balancing flag reduces nonzero amounts to zero even though A4 lacks
        // must_not_exceed (since its net balance is zero):
        \\ transfer   T5 A4 A1    1   _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ _ _ _ ok
        \\ transfer   T6 A2 A4    1   _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ _ _ _ ok
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ ok
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _ C<D _ _ _ _ _ ok
        \\ account A4  0  0  0  0  _  _  _ _ L1 C1   _   _ C<D _ _ _ _ _ ok
        \\ commit create_accounts
        \\
        \\ setup A1 0  0 0 -1
        \\ setup A4 0 -1 0  0
        \\
        \\ transfer   T1 A1 A2   -0   _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ _ _ _ ok
        \\ transfer   T2 A3 A4   -0   _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ _ _ _ ok
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _ C<D _ _ _ _ _ ok
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ commit create_accounts
        \\
        \\ setup A1 0  0 0 20
        \\ setup A2 0 10 0  0
        \\ setup A3 0 99 0  0
        \\
        \\ transfer   T1 A1 A2    1   _  _  _  _    _ L1 C1   _   _   _   _ BDR BCR  _ _ _ _ _ ok
        \\ transfer   T2 A1 A2   12   _  _  _  _    _ L1 C1   _   _   _   _ BDR BCR  _ _ _ _ _ ok
        \\ transfer   T3 A1 A2    1   _  _  _  _    _ L1 C1   _   _   _   _ BDR BCR  _ _ _ _ _ ok // Amount reduced to 0.
        \\ transfer   T4 A1 A3   12   _  _  _  _    _ L1 C1   _   _   _   _ BDR BCR  _ _ _ _ _ ok
        \\ transfer   T5 A1 A3    1   _  _  _  _    _ L1 C1   _   _   _   _ BDR BCR  _ _ _ _ _ ok // Amount reduced to 0.
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _ C<D _ _ _ _ _ ok
        \\ commit create_accounts
        \\
        \\ setup A1 0  0 0 10
        \\ setup A2 0 10 0  0
        \\
        \\ transfer   T1 A1 A2    3   _  _  _  _    _ L1 C1   _ PEN   _   _ BDR   _  _ _ _ _ _ ok
        \\ transfer   T2 A1 A2   13   _  _  _  _    _ L1 C1   _ PEN   _   _ BDR   _  _ _ _ _ _ ok
        \\ transfer   T3 A1 A2    1   _  _  _  _    _ L1 C1   _ PEN   _   _ BDR   _  _ _ _ _ _ ok // Amount reduced to 0.
        \\ commit create_transfers
        \\
        \\ lookup_account A1 10  0  0 10  _
        \\ lookup_account A2  0 10 10  0  _
        \\ commit lookup_accounts
        \\
        \\ transfer   T4 A1 A2    3  T1  _  _  _    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ ok
        \\ transfer   T5 A1 A2    5  T2  _  _  _    _ L1 C1   _   _ POS   _   _   _  _ _ _ _ _ ok
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ ok
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ ok
        \\ account A4  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ account A5  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ ok
        \\ account A6  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ commit create_accounts
        \\
        \\ setup A1 0 0 0 40
        \\ setup A2 0 0 0 40
        \\ setup A3 0 0 0 21
        \\
        \\ transfer T1 A4 A5  100   _  _  _  _    0 L1 C1 LNK _ _ _   _   _ _ _ _ _ _ ok
        \\ transfer T2 A1 A4  100   _  _  _  _    0 L1 C1 LNK _ _ _ BDR BCR _ _ _ _ _ ok
        \\ transfer T3 A2 A4  100   _  _  _  _    0 L1 C1 LNK _ _ _ BDR BCR _ _ _ _ _ ok
        \\ transfer T4 A3 A4  100   _  _  _  _    0 L1 C1 LNK _ _ _ BDR BCR _ _ _ _ _ ok
        \\ transfer T5 A4 A6  100   _  _  _  _    0 L1 C1 LNK _ _ _   _   _ _ _ _ _ _ ok
        \\ transfer T6 A5 A4   -0   _  _  _  _    0 L1 C1   _ _ _ _   _ BCR _ _ _ _ _ ok
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ ok
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ ok
        \\ account A4  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ account A5  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ _ _ _ ok
        \\ account A6  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _ C<D _ _ _ _ _ ok
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
        \\ transfer T1 A1 A2  0   _  _  _  _    0 L1 C1   _   _ _   _   _   _ _ _ _ _ _ ok
        \\ transfer T2 A2 A3  0   _  _  _  _    0 L1 C1   _   _ _   _   _   _ _ _ _ _ _ id_already_failed
        \\ transfer T3 A2 A3  0   _  _  _  _    0 L1 C1   _   _ _   _   _   _ _ _ _ _ _ ok
        \\ commit create_transfers
        \\
        \\ transfer T4 A1 A2 40   _  _  _  _    0 L1 C1 LNK   _ _   _   _   _ _ _ _ _ _ ok
        \\ transfer T5 A2 A3  1   _  _  _  _    0 L1 C1 LNK PEN _   _ BDR   _ _ _ _ _ _ ok
        \\ transfer T6 A2 A3  0  T5  _  _  _    0 L1 C1   _   _ _ VOI   _   _ _ _ _ _ _ ok
        \\ commit create_transfers
        \\
        \\ lookup_account A1 0 40 0  0 _
        \\ lookup_account A2 0 40 0 40 _
        \\ lookup_account A3 0  0 0  0 _
        \\ commit lookup_accounts
    );

    // Temporarily enforce `debits_must_not_exceed_credits` on account `A1`.
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   D<C _ _ _ _ _ _ ok
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
        \\ transfer T1 A1 A2  0   _  _  _  _    0 L1 C1   _   _ _   _   _   _ _ _ _ _ _ ok
        \\ transfer T2 A3 A1  0   _  _  _  _    0 L1 C1   _   _ _   _   _   _ _ _ _ _ _ id_already_failed
        \\ transfer T3 A3 A1  0   _  _  _  _    0 L1 C1   _   _ _   _   _   _ _ _ _ _ _ ok
        \\ commit create_transfers
        \\
        \\ transfer T4 A1 A2 40   _  _  _  _    0 L1 C1 LNK   _ _   _   _   _ _ _ _ _ _ ok
        \\ transfer T5 A3 A1  1   _  _  _  _    0 L1 C1 LNK PEN _   _   _ BCR _ _ _ _ _ ok
        \\ transfer T6 A3 A1  0  T5  _  _  _    0 L1 C1   _   _ _ VOI   _   _ _ _ _ _ _ ok
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _ 1 ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ _   _ _ 0 imported_event_expected
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _ 2 ok
        \\ commit create_accounts
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ _   _ _ 0 ok
        \\ account A4  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _ 3 imported_event_not_expected
        \\ commit create_accounts
        \\
        \\ transfer   T1 A1 A2    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  IMP _ _ _ 10 ok
        \\ transfer   T2 A1 A2    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  _   _ _ _  0 imported_event_expected
        \\ commit create_transfers
        \\ transfer   T3 A1 A2    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  _   _  _ _ 0 ok
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _  2 ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _  1 imported_event_timestamp_must_not_regress
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _  3 ok
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _  9 ok
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
        \\ transfer   T1 A1 A2    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  IMP _ _ _  4 ok
        \\ transfer   T2 A1 A2    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  IMP _ _ _  3 imported_event_timestamp_must_not_regress
        \\ transfer   T2 A1 A2    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  IMP _ _ _  5 ok
        \\ commit create_transfers
        \\
        \\ transfer   T2 A1 A2    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  IMP _ _ _ 99 imported_event_timestamp_must_not_advance
        \\ transfer   T2 A1 A2    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  IMP _ _ _  4 exists // T2 `exists` regardless different timestamps.
        \\ transfer   T2 A1 A2    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  IMP _ _ _  5 exists
        \\ transfer   T2 A1 A2    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  IMP _ _ _  6 exists
        \\ commit create_transfers
        \\
        \\ transfer   T3 A1 A2    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  IMP _ _ _ 10 ok
        \\ commit create_transfers
        \\
        \\ account A4  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _ 10 imported_event_timestamp_must_not_regress // The same timestamp as a transfer.
        \\ commit create_accounts
    );
}

test "imported events: pending transfers" {
    try check(
        \\ tick 10 nanoseconds
        \\
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _ 1 ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _ 2 ok
        \\ commit create_accounts
        \\
        \\ transfer   T1 A1 A2    3   _  _  _  _    _ L1 C2   _     _   _   _   _   _  IMP _ _ _ 3 ok
        \\ transfer   T2 A1 A2    4   _  _  _  _    1 L1 C2   _     PEN _   _   _   _  IMP _ _ _ 4 imported_event_timeout_must_be_zero
        \\ transfer   T2 A1 A2    4   _  _  _  _    0 L1 C2   _     PEN _   _   _   _  IMP _ _ _ 4 ok
        \\ commit create_transfers
        \\
        \\ lookup_account A1 4  3  0  0  _
        \\ lookup_account A2 0  0  4  3  _
        \\ commit lookup_accounts
        \\
        \\ transfer   T3 A1 A2    4  T2 _  _   _    _ L1 C2   _     _   POS _   _   _  IMP _ _ _ 5 ok
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   LNK  _  _  _ IMP _ _ 1 ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   LNK  _  _  _ IMP _ _ 2 ok
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _    _  _  _ IMP _ _ 3 ok
        \\ commit create_accounts
        \\
        \\ transfer   T1 A1 A2    3   _  _  _  _    _ L1 C2   LNK   _   _   _   _   _  IMP _ _ _ 4 linked_event_failed
        \\ transfer   T2 A1 A2    3   _  _  _  _    _ L1 C2   LNK   _   _   _   _   _  IMP _ _ _ 5 linked_event_failed
        \\ transfer   T3 A1 A2    3   _  _  _  _    _ L1 C2   _     _   _   _   _   _  IMP _ _ _ 0 imported_event_timestamp_out_of_range
        \\ commit create_transfers
        \\
        \\ transfer   T1 A1 A2    3   _  _  _  _    _ L1 C2   LNK   _   _   _   _   _  IMP _ _ _ 4 ok
        \\ transfer   T2 A1 A2    3   _  _  _  _    _ L1 C2   LNK   _   _   _   _   _  IMP _ _ _ 5 ok
        \\ transfer   T3 A1 A2    3   _  _  _  _    _ L1 C2   _     _   _   _   _   _  IMP _ _ _ 6 ok
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _  _  _ _ _  CLSD _ _ ok
        \\ commit create_accounts
        \\
        \\ lookup_account A1  0  0  0   0  CLSD
        \\ commit lookup_accounts
    );
}

test "create_transfers: closing accounts" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _  _  _ _ _ _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _  _  _ _ _ _ _ _ ok
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _  _  _ _ _ _ _ _ ok
        \\ commit create_accounts
        \\
        // Closing the debit account.
        \\ transfer   T1  A1 A2   15   _  _   _  _    _ L1 C1   _   _   _   _   _   _  _  _   _   _ _ ok
        \\ transfer   T2  A1 A2    0   _  _   _  _    _ L1 C1   _   _   _   _   _   _  _  CDR _   _ _ closing_transfer_must_be_pending
        \\ transfer   T2  A1 A2    0   _  _   _  _    0 L1 C1   _   PEN _   _   _   _  _  CDR _   _ _ ok
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
        \\ transfer   T6  A1 A2    0   T2 _   _  _    _ L1 C1   _   _   _   VOI _   _  _  _   _   _ _ ok // Re-opening the account.
        \\ transfer   T6  A1 A2    0   T2 _   _  _    _ L1 C1   _   _   _   VOI _   _  _  _   _   _ _ exists
        \\ transfer   T7  A1 A2    5   _  _   _  _    _ L1 C1   _   _   _   _   _   _  _  _   _   _ _ ok
        \\ commit create_transfers
        \\
        \\ lookup_account A1  0 20  0   0  _
        \\ lookup_account A2  0  0  0  20  _
        \\ commit lookup_accounts
        \\
        // Closing the credit account with a timeout.
        // Pending transfer can be voided, but not posted in a closed account.
        \\ transfer   T8  A1 A2   10   _  _   _  _    1 L1 C1   _   PEN _   _   _   _  _  _   _   _ _ ok
        \\ transfer   T9  A1 A2   10   _  _   _  _    0 L1 C1   _   PEN _   _   _   _  _  _   _   _ _ ok
        \\ transfer   T10 A1 A2    0   _  _   _  _    2 L1 C1   _   PEN _   _   _   _  _  _   CCR _ _ ok
        \\ transfer   T10 A1 A2    0   _  _   _  _    2 L1 C1   _   PEN _   _   _   _  _  _   CCR _ _ exists
        // `credit_account_already_closed` is a transient error, T11 cannot be reused:
        \\ transfer   T11 A1 A2   10   T9 _   _  _    _ L1 C1   _   _   POS _   _   _  _  _   _   _ _ credit_account_already_closed
        \\ transfer   T11 A1 A2   10   T9 _   _  _    _ L1 C1   _   _   _   VOI _   _  _  _   _   _ _ id_already_failed
        \\
        \\ transfer   T12 A1 A2   10   T9 _   _  _    _ L1 C1   _   _   _   VOI _   _  _  _   _   _ _ ok
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
        \\ transfer   T13  A1 A2    0   _  _   _  _    0 L1 C1   _   PEN _   _   _   _  _  CDR CCR _ _ ok
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ _ _ _ _ _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _ _ _ _ _ _ _ _ ok
        \\ commit create_accounts
        \\
        \\ transfer T1 A1 A2   10   _  U1000  U10  U1 _ L1 C1   _   _   _   _   _   _  _ _ _ _ _ ok
        \\ transfer T2 A2 A1   11   _  U1001  U10  U2 _ L1 C2   _   _   _   _   _   _  _ _ _ _ _ ok
        \\ transfer T3 A1 A2   12   _  U1000  U20  U2 _ L1 C1   _   _   _   _   _   _  _ _ _ _ _ ok
        \\ transfer T4 A2 A1   13   _  U1001  U20  U1 _ L1 C2   _   _   _   _   _   _  _ _ _ _ _ ok
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ _ _ HIST _ _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _ _ _ HIST _ _ _ _ ok
        \\ commit create_accounts
        \\
        \\ transfer T1 A1 A2    2   _  _  _  _    0 L1 C1   _ PEN   _   _   _   _  _ _ _ _ _ ok
        \\ transfer T2 A1 A2    1  T1  _  _  _    0 L1 C1   _   _ POS   _   _   _  _ _ _ _ _ ok
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ _ _ _ _ _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _ _ _ _ _ _ _ _ ok
        \\ commit create_accounts
        \\
        \\ transfer T1 A1 A2    2   _  _  _  _    0 L1 C1   _ PEN   _   _   _   _  _ _ _ _ _ ok
        \\ transfer T2 A1 A2    1  T1  _  _  _    0 L1 C1   _   _ POS   _   _   _  _ _ _ _ _ ok
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ _ _ HIST _ _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _ _ _ HIST _ _ _ _ ok
        \\ commit create_accounts
        \\
        \\ transfer T1 A1 A2   10   _  U1000  U10  U1 _ L1 C1   _   _   _   _   _   _  _ _ _ _ _ ok
        \\ transfer T2 A2 A1   11   _  U1001  U10  U2 _ L1 C2   _   _   _   _   _   _  _ _ _ _ _ ok
        \\ transfer T3 A1 A2   12   _  U1000  U20  U2 _ L1 C1   _   _   _   _   _   _  _ _ _ _ _ ok
        \\ transfer T4 A2 A1   13   _  U1001  U20  U1 _ L1 C2   _   _   _   _   _   _  _ _ _ _ _ ok
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ _ _ HIST _ _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _ _ _ HIST _ _ _ _ ok
        \\ commit create_accounts
        \\
        \\ transfer T1 A1 A2    1   _  _  _  _    0 L1 C1   _ PEN   _   _   _   _  _ _ _ _ _ ok
        \\ transfer T2 A1 A2    1  T1  _  _  _    0 L1 C1   _   _ POS   _   _   _  _ _ _ _ _ ok
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ _ _ HIST _ _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _ _ _    _ _ _ _ _ ok
        \\ commit create_accounts
        \\
        \\ transfer T1 A1 A2    2   _  _  _  _    0 L1 C1   _   _   _   _   _   _  _ _ _ _ _ ok
        \\ transfer T2 A1 A2    1   _  _  _  _    0 L1 C1   _   _   _   _   _   _  _ _ _ _ _ ok
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
        \\ account A1  0  0  0  0 U1000 U10 U1 _ L1 C1 _   _   _ _ _ _ _ _ ok
        \\ account A2  0  0  0  0 U1000 U11 U2 _ L2 C2 _   _   _ _ _ _ _ _ ok
        \\ account A3  0  0  0  0 U1000 U10 U3 _ L3 C3 _   _   _ _ _ _ _ _ ok
        \\ account A4  0  0  0  0 U1000 U11 U4 _ L4 C4 _   _   _ _ _ _ _ _ ok
        \\ account A5  0  0  0  0 U2000 U10 U1 _ L3 C5 _   _   _ _ _ _ _ _ ok
        \\ account A6  0  0  0  0 U2000 U11 U2 _ L2 C6 _   _   _ _ _ _ _ _ ok
        \\ account A7  0  0  0  0 U2000 U10 U3 _ L1 C7 _   _   _ _ _ _ _ _ ok
        \\ account A8  0  0  0  0 U1000 U10 U1 _ L1 C1 _   _   _ _ _ _ _ _ ok
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ account A3  0  0  0  0  _  _  _ _ L2 C1   _   _   _ _ _ _ _ _ ok
        \\ account A4  0  0  0  0  _  _  _ _ L2 C1   _   _   _ _ _ _ _ _ ok
        \\ commit create_accounts

        // Creating transfers:
        \\ transfer T1 A1 A2    0  _ U1000 U10 U1 _ L1 C1 _ _ _ _ _ _ _ _ _ _ _ ok
        \\ transfer T2 A3 A4   11  _ U1000 U11 U2 _ L2 C2 _ _ _ _ _ _ _ _ _ _ _ ok
        \\ transfer T3 A2 A1   12  _ U1000 U10 U3 _ L1 C3 _ _ _ _ _ _ _ _ _ _ _ ok
        \\ transfer T4 A4 A3   13  _ U1000 U11 U4 _ L2 C4 _ _ _ _ _ _ _ _ _ _ _ ok
        \\ transfer T5 A2 A1   14  _ U2000 U10 U1 _ L1 C5 _ _ _ _ _ _ _ _ _ _ _ ok
        \\ transfer T6 A4 A3   15  _ U2000 U11 U2 _ L2 C6 _ _ _ _ _ _ _ _ _ _ _ ok
        \\ transfer T7 A1 A2   16  _ U2000 U10 U3 _ L1 C7 _ _ _ _ _ _ _ _ _ _ _ ok
        \\ transfer T8 A2 A1   17  _ U1000 U10 U1 _ L1 C1 _ _ _ _ _ _ _ _ _ _ _ ok
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
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ account A4  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ _ _ _ ok
        \\ commit create_accounts

        // First phase.
        \\ transfer   T1 A1 A2   10   _  _  _  _    _ L1 C1   _   _   _   _   _   _  _   _   _ _ _ ok // Not pending.
        \\ transfer   T2 A1 A2   11   _  _  _  _    0 L1 C1   _ PEN   _   _   _   _  _   _   _ _ _ ok // Timeout zero will never expire.
        \\ transfer   T3 A1 A2   12   _  _  _  _    1 L1 C1   _ PEN   _   _   _   _  _   _   _ _ _ ok // Will expire.
        \\ transfer   T4 A1 A2   13   _  _  _  _    2 L1 C1   _ PEN   _   _   _   _  _   _   _ _ _ ok // Will be posted.
        \\ transfer   T5 A1 A2   14   _  _  _  _    2 L1 C1   _ PEN   _   _   _   _  _   _   _ _ _ ok // Will be voided.
        // Closes the debit and credit accounts.
        \\ transfer   T6 A3 A1    0   _  _  _  _    0 L1 C1   _ PEN   _   _   _   _  _ CDR   _ _ _ ok
        \\ transfer   T7 A1 A4    0   _  _  _  _    0 L1 C1   _ PEN   _   _   _   _  _   _ CCR _ _ ok
        \\ commit create_transfers

        // Bump the state machine time in +1s for testing the timeout expiration.
        \\ tick 1 seconds

        // Second phase.
        \\ transfer  T14 A0 A0   -0  T4  _  _  _    _ L0 C0   _   _ POS   _   _   _  _ _ _ _ _ ok // Posts T4.
        \\ transfer  T15 A0 A0    0  T5  _  _  _    _ L0 C0   _   _   _ VOI   _   _  _ _ _ _ _ ok // Voids T5.
        // Reopens the debit and credit accounts.
        \\ transfer  T16 A0 A0    0  T6  _  _  _    _ L0 C0   _   _   _ VOI   _   _  _ _ _ _ _ ok
        \\ transfer  T17 A0 A0    0  T7  _  _  _    _ L0 C0   _   _   _ VOI   _   _  _ _ _ _ _ ok
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
    const allocator = std.testing.allocator;
    const input = try allocator.alignedAlloc(u8, 16, 2 * constants.message_body_size_max);
    defer allocator.free(input);

    const build_input = struct {
        fn build_input(buffer: []align(16) u8, options: struct {
            operation: TestContext.StateMachine.Operation,
            event_count: u32,
        }) []align(16) const u8 {
            const event_size = options.operation.event_size();
            const payload_size: u32 = options.event_count * event_size;
            if (options.operation.is_multi_batch()) {
                var body_encoder = vsr.multi_batch.MultiBatchEncoder.init(buffer, .{
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
    try context.init(std.testing.allocator);
    defer context.deinit(std.testing.allocator);

    const operations = std.enums.values(TestContext.StateMachine.Operation);
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
    const allocator = std.testing.allocator;
    const input = try allocator.alignedAlloc(u8, 16, 2 * constants.message_body_size_max);
    defer allocator.free(input);

    var context: TestContext = undefined;
    try context.init(std.testing.allocator);
    defer context.deinit(std.testing.allocator);

    const build_input = struct {
        fn build_input(
            operation: TestContext.StateMachine.Operation,
            limits: []const u32,
            buffer: []align(16) u8,
        ) []align(16) const u8 {
            switch (operation) {
                .get_account_transfers,
                .get_account_balances,
                => {
                    var body_encoder = vsr.multi_batch.MultiBatchEncoder.init(buffer, .{
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
                    var body_encoder = vsr.multi_batch.MultiBatchEncoder.init(buffer, .{
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

    const operations = &[_]TestContext.StateMachine.Operation{
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
