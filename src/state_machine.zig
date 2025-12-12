const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const log = std.log.scoped(.state_machine);

const stdx = @import("stdx");
const maybe = stdx.maybe;

const constants = @import("constants.zig");
const tb = @import("tigerbeetle.zig");
const vsr = @import("vsr.zig");
const snapshot_latest = @import("lsm/tree.zig").snapshot_latest;
const ScopeCloseMode = @import("lsm/tree.zig").ScopeCloseMode;
const WorkloadType = @import("state_machine/workload.zig").WorkloadType;
const GrooveType = @import("lsm/groove.zig").GrooveType;
const ForestType = @import("lsm/forest.zig").ForestType;
const ScanBuffer = @import("lsm/scan_buffer.zig").ScanBuffer;
const ScanLookupType = @import("lsm/scan_lookup.zig").ScanLookupType;

const MultiBatchEncoder = vsr.multi_batch.MultiBatchEncoder;
const MultiBatchDecoder = vsr.multi_batch.MultiBatchDecoder;

const Direction = @import("direction.zig").Direction;
const TimestampRange = @import("lsm/timestamp_range.zig").TimestampRange;

const Account = tb.Account;
const AccountFlags = tb.AccountFlags;
const AccountBalance = tb.AccountBalance;

const Transfer = tb.Transfer;
const TransferFlags = tb.TransferFlags;
const TransferPendingStatus = tb.TransferPendingStatus;

const CreateAccountResult = tb.CreateAccountResult;
const CreateTransferResult = tb.CreateTransferResult;

const AccountFilter = tb.AccountFilter;
const QueryFilter = tb.QueryFilter;
const ChangeEventsFilter = tb.ChangeEventsFilter;
const ChangeEvent = tb.ChangeEvent;
const ChangeEventType = tb.ChangeEventType;

pub const tree_ids = struct {
    pub const Account = .{
        .id = 1,
        .user_data_128 = 2,
        .user_data_64 = 3,
        .user_data_32 = 4,
        .ledger = 5,
        .code = 6,
        .timestamp = 7,
        .imported = 23,
        .closed = 25,
    };

    pub const Transfer = .{
        .id = 8,
        .debit_account_id = 9,
        .credit_account_id = 10,
        .amount = 11,
        .pending_id = 12,
        .user_data_128 = 13,
        .user_data_64 = 14,
        .user_data_32 = 15,
        .ledger = 16,
        .code = 17,
        .timestamp = 18,
        .expires_at = 19,
        .imported = 24,
        .closing = 26,
    };

    pub const TransferPending = .{
        .timestamp = 20,
        .status = 21,
    };

    pub const AccountEvents = .{
        .timestamp = 22,
        .account_timestamp = 27,
        .transfer_pending_status = 28,
        .dr_account_id_expired = 29,
        .cr_account_id_expired = 30,
        .transfer_pending_id_expired = 31,
        .ledger_expired = 32,
        .prunable = 33,
    };
};

pub const TransferPending = extern struct {
    timestamp: u64,
    status: TransferPendingStatus,
    padding: [7]u8 = @splat(0),

    comptime {
        // Assert that there is no implicit padding.
        assert(@sizeOf(TransferPending) == 16);
        assert(stdx.no_padding(TransferPending));
    }
};

pub const AccountEvent = extern struct {
    dr_account_id: u128,
    dr_debits_pending: u128,
    dr_debits_posted: u128,
    dr_credits_pending: u128,
    dr_credits_posted: u128,
    cr_account_id: u128,
    cr_debits_pending: u128,
    cr_debits_posted: u128,
    cr_credits_pending: u128,
    cr_credits_posted: u128,
    timestamp: u64,
    dr_account_timestamp: u64,
    cr_account_timestamp: u64,
    dr_account_flags: AccountFlags,
    cr_account_flags: AccountFlags,
    transfer_flags: TransferFlags,
    transfer_pending_flags: TransferFlags,
    transfer_pending_id: u128,
    amount_requested: u128,
    amount: u128,
    ledger: u32,

    /// Although similar to `TransferPending.status`, this index tracks the event,
    /// not the original pending transfer.
    /// Examples: (No such index exists in `Transfers.flags`)
    ///   "All voided or expired events today."
    ///   "All single-phase or posted events today."
    ///
    ///  Value   | Description
    /// ---------|-----------------------------------------------------
    /// `none`   | This event is a regular transfer.
    /// `pending`| This event is a pending transfer.
    /// `posted` | This event posted a pending transfer.
    /// `voided` | This event voided a pending transfer.
    /// `expired`| This event expired a pending transfer,
    ///            the `timestamp` does not relate to a transfer.
    ///
    /// See `transfer_pending_id` for tracking the pending transfer.
    /// It will be `zero` for `none` and `pending`.
    transfer_pending_status: TransferPendingStatus,
    reserved: [11]u8 = @splat(0),

    /// Previous schema before the changes introduced by PR #2507.
    const Former = extern struct {
        dr_account_id: u128,
        dr_debits_pending: u128,
        dr_debits_posted: u128,
        dr_credits_pending: u128,
        dr_credits_posted: u128,
        cr_account_id: u128,
        cr_debits_pending: u128,
        cr_debits_posted: u128,
        cr_credits_pending: u128,
        cr_credits_posted: u128,
        timestamp: u64,
        reserved: [88]u8 = @splat(0),

        comptime {
            assert(stdx.no_padding(Former));
            assert(@sizeOf(Former) == @sizeOf(AccountEvent));
            assert(@alignOf(Former) == @alignOf(AccountEvent));

            // Asserting the fields are identical.
            for (std.meta.fields(Former)) |field_former| {
                if (std.mem.eql(u8, field_former.name, "reserved")) continue;
                const field = std.meta.fields(AccountEvent)[
                    std.meta.fieldIndex(
                        AccountEvent,
                        field_former.name,
                    ).?
                ];
                assert(field_former.type == field.type);
                assert(field_former.alignment == field.alignment);
                assert(@offsetOf(AccountEvent, field_former.name) ==
                    @offsetOf(Former, field_former.name));
            }
        }
    };

    /// Checks the object and returns whether it was created
    /// using the current or the former schema.
    fn schema(self: *const AccountEvent) union(enum) {
        current,
        former: *const AccountEvent.Former,
    } {
        assert(self.timestamp != 0);

        const former: *const AccountEvent.Former = @ptrCast(self);
        if (stdx.zeroed(&former.reserved)) {
            // In the former schema:
            // Balances for accounts without the `history` flag weren’t stored.
            // If neither side had the `history` flag, the entire object wasn’t stored.
            assert(former.dr_account_id != 0 or former.cr_account_id != 0);

            return .{ .former = former };
        }

        assert(self.dr_account_timestamp != 0);
        assert(self.dr_account_id != 0);
        assert(self.cr_account_timestamp != 0);
        assert(self.cr_account_id != 0);
        assert(self.ledger != 0);
        switch (self.transfer_pending_status) {
            .none, .pending => assert(self.transfer_pending_id == 0),
            .posted, .voided, .expired => assert(self.transfer_pending_id != 0),
        }
        assert(stdx.zeroed(&self.reserved));
        return .current;
    }

    comptime {
        assert(stdx.no_padding(AccountEvent));
        assert(@sizeOf(AccountEvent) == 256);
        assert(@alignOf(AccountEvent) == 16);
    }
};

pub fn StateMachineType(comptime Storage: type) type {
    assert(constants.message_body_size_max > 0);
    assert(constants.lsm_compaction_ops > 0);
    assert(constants.vsr_operations_reserved > 0);

    return struct {
        batch_size_limit: u32,
        prefetch_timestamp: u64,
        prepare_timestamp: u64,
        commit_timestamp: u64,
        forest: Forest,

        prefetch_operation: ?Operation = null,
        prefetch_input: ?[]const u8 = null,
        prefetch_callback: ?*const fn (*StateMachine) void = null,
        prefetch_context: PrefetchContext = .null,

        scan_lookup: ScanLookup = .null,
        scan_lookup_buffer: []align(16) u8,
        scan_lookup_buffer_index: u32 = 0,
        scan_lookup_results: std.ArrayListUnmanaged(u32),
        scan_lookup_next_tick: Grid.NextTick = undefined,

        expire_pending_transfers: ExpirePendingTransfers = .{},

        open_callback: ?*const fn (*StateMachine) void = null,
        compact_callback: ?*const fn (*StateMachine) void = null,
        checkpoint_callback: ?*const fn (*StateMachine) void = null,

        /// Temporary metrics, until proper ones are merged.
        metrics: Metrics,
        log_trace: bool,

        const StateMachine = @This();
        const Grid = @import("vsr/grid.zig").GridType(Storage);

        /// Re-exports the `Contract` declarations, so it can be interchangeable
        /// with a concrete state machine type.
        pub const Operation = tb.Operation;

        pub const Options = struct {
            batch_size_limit: u32,
            lsm_forest_compaction_block_count: u32,
            lsm_forest_node_count: u32,
            cache_entries_accounts: u32,
            cache_entries_transfers: u32,
            cache_entries_transfers_pending: u32,
            log_trace: bool,
        };

        pub const Workload = WorkloadType(StateMachine);

        pub const Forest = ForestType(Storage, .{
            .accounts = AccountsGroove,
            .transfers = TransfersGroove,
            .transfers_pending = TransfersPendingGroove,
            .account_events = AccountEventsGroove,
        });

        pub const batch_max = struct {
            pub const create_accounts: u32 = @max(
                Operation.create_accounts.event_max(
                    constants.message_body_size_max,
                ),
                Operation.deprecated_create_accounts_unbatched.event_max(
                    constants.message_body_size_max,
                ),
            );
            pub const create_transfers: u32 = @max(
                Operation.create_transfers.event_max(
                    constants.message_body_size_max,
                ),
                Operation.deprecated_create_transfers_unbatched.event_max(
                    constants.message_body_size_max,
                ),
            );
            pub const lookup_accounts: u32 = @max(
                Operation.lookup_accounts.event_max(
                    constants.message_body_size_max,
                ),
                Operation.deprecated_lookup_accounts_unbatched.event_max(
                    constants.message_body_size_max,
                ),
            );
            pub const lookup_transfers: u32 = @max(
                Operation.lookup_transfers.event_max(
                    constants.message_body_size_max,
                ),
                Operation.deprecated_lookup_transfers_unbatched.event_max(
                    constants.message_body_size_max,
                ),
            );

            comptime {
                assert(create_accounts > 0);
                assert(create_transfers > 0);
                assert(lookup_accounts > 0);
                assert(lookup_transfers > 0);
            }
        };

        const tree_values_count_max = tree_values_count(constants.message_body_size_max);

        const AccountsGroove = GrooveType(
            Storage,
            Account,
            .{
                .ids = tree_ids.Account,
                .batch_value_count_max = tree_values_count_max.accounts,
                .ignored = &[_][]const u8{
                    "debits_posted",
                    "debits_pending",
                    "credits_posted",
                    "credits_pending",
                    "flags",
                    "reserved",
                },
                .optional = &[_][]const u8{
                    "user_data_128",
                    "user_data_64",
                    "user_data_32",
                },
                .derived = .{
                    .imported = struct {
                        fn imported(object: *const Account) ?void {
                            return if (object.flags.imported) {} else null;
                        }
                    }.imported,
                    .closed = struct {
                        fn closed(object: *const Account) ?void {
                            return if (object.flags.closed) {} else null;
                        }
                    }.closed,
                },
                .orphaned_ids = false,
                .objects_cache = true,
            },
        );

        const TransfersGroove = GrooveType(
            Storage,
            Transfer,
            .{
                .ids = tree_ids.Transfer,
                .batch_value_count_max = tree_values_count_max.transfers,
                .ignored = &[_][]const u8{ "timeout", "flags" },
                .optional = &[_][]const u8{
                    "pending_id",
                    "user_data_128",
                    "user_data_64",
                    "user_data_32",
                },
                .derived = .{
                    .expires_at = struct {
                        fn expires_at(object: *const Transfer) ?u64 {
                            if (object.flags.pending and object.timeout > 0) {
                                return object.timestamp + object.timeout_ns();
                            }
                            return null;
                        }
                    }.expires_at,
                    .imported = struct {
                        fn imported(object: *const Transfer) ?void {
                            return if (object.flags.imported) {} else null;
                        }
                    }.imported,
                    .closing = struct {
                        fn closing(object: *const Transfer) ?void {
                            if (object.flags.closing_debit or object.flags.closing_credit) {
                                return {};
                            } else {
                                return null;
                            }
                        }
                    }.closing,
                },
                .orphaned_ids = true,
                .objects_cache = true,
            },
        );

        const TransfersPendingGroove = GrooveType(
            Storage,
            TransferPending,
            .{
                .ids = tree_ids.TransferPending,
                .batch_value_count_max = tree_values_count_max.transfers_pending,
                .ignored = &[_][]const u8{"padding"},
                .optional = &[_][]const u8{
                    // Index the current status of a pending transfer.
                    // Examples:
                    //   "Pending transfers that are still pending."
                    //   "Pending transfers that were voided or expired."
                    "status",
                },
                .derived = .{},
                .orphaned_ids = false,
                .objects_cache = true,
            },
        );

        const AccountEventsGroove = GrooveType(
            Storage,
            AccountEvent,
            .{
                .ids = tree_ids.AccountEvents,
                .batch_value_count_max = tree_values_count_max.account_events,
                .ignored = &[_][]const u8{
                    "dr_account_id",
                    "dr_debits_pending",
                    "dr_debits_posted",
                    "dr_credits_pending",
                    "dr_credits_posted",
                    "cr_account_id",
                    "cr_debits_pending",
                    "cr_debits_posted",
                    "cr_credits_pending",
                    "cr_credits_posted",
                    "dr_account_timestamp",
                    "cr_account_timestamp",
                    "dr_account_flags",
                    "cr_account_flags",
                    "transfer_flags",
                    "transfer_pending_flags",
                    "transfer_pending_id",
                    "amount_requested",
                    "amount",
                    "ledger",
                    "reserved",
                },
                .optional = &[_][]const u8{},
                .derived = .{
                    // Placeholder derived index (will be inserted during `account_event`).
                    //
                    // This index stores two values per object (the credit and debit accounts).
                    // It is used for balance as-of queries and to search events related to a
                    // particular account.
                    // Examples:
                    //   "Balance where account=X and timestamp=Y".
                    //   "Last time account=X was updated".
                    .account_timestamp = struct {
                        fn account_timestamp(_: *const AccountEvent) ?u64 {
                            return null;
                        }
                    }.account_timestamp,

                    // Events related to transfers can be searched using `Transfers.dr_account_id`
                    // and `Transfers.cr_account_id`.
                    // However, expired events require a specific index to be searchable by both
                    // debit and credit accounts.
                    // Example: "All expired debits where account=X".
                    .dr_account_id_expired = struct {
                        fn dr_account_id_expired(object: *const AccountEvent) ?u128 {
                            return if (object.transfer_pending_status == .expired)
                                object.dr_account_id
                            else
                                null;
                        }
                    }.dr_account_id_expired,
                    .cr_account_id_expired = struct {
                        fn cr_account_id_expired(object: *const AccountEvent) ?u128 {
                            return if (object.transfer_pending_status == .expired)
                                object.cr_account_id
                            else
                                null;
                        }
                    }.cr_account_id_expired,

                    // Events related to voiding or posting pending transfers can be searched using
                    // `Transfers.pending_id`.
                    // However, expired events require a specific index to be searchable by the
                    // transfer.
                    // Example: "When transfer=X has expired".
                    .transfer_pending_id_expired = struct {
                        fn transfer_pending_id_expired(object: *const AccountEvent) ?u128 {
                            return if (object.transfer_pending_status == .expired)
                                object.transfer_pending_id
                            else
                                null;
                        }
                    }.transfer_pending_id_expired,

                    // Events related to transfers can be searched using `Transfers.ledger`.
                    // However, expired events require a specific index to be searchable
                    // by ledger.
                    // Example: "All expiry events where ledger=X".
                    .ledger_expired = struct {
                        fn transfer_expired_ledger(object: *const AccountEvent) ?u128 {
                            return if (object.transfer_pending_status == .expired)
                                object.ledger
                            else
                                null;
                        }
                    }.transfer_expired_ledger,

                    // Tracks events for accounts without the history flag,
                    // enabling a cleanup job to delete them after CDC.
                    .prunable = struct {
                        fn prunable(object: *const AccountEvent) ?void {
                            if (object.dr_account_flags.history or
                                object.cr_account_flags.history)
                            {
                                return null;
                            } else {
                                return {};
                            }
                        }
                    }.prunable,
                },
                .orphaned_ids = false,
                .objects_cache = false,
            },
        );

        const AccountsScanLookup = ScanLookupType(
            AccountsGroove,
            AccountsGroove.ScanBuilder.Scan,
            Storage,
        );

        const TransfersScanLookup = ScanLookupType(
            TransfersGroove,
            TransfersGroove.ScanBuilder.Scan,
            Storage,
        );

        const AccountBalancesScanLookup = ScanLookupType(
            AccountEventsGroove,
            // Both Objects use the same timestamp, so we can use the TransfersGroove's indexes.
            TransfersGroove.ScanBuilder.Scan,
            Storage,
        );

        const ChangeEventsScanLookup = ChangeEventsScanLookupType(AccountEventsGroove, Storage);

        /// Since prefetch contexts are used one at a time, it's safe to access
        /// the union's fields and reuse the same memory for all context instances.
        const PrefetchContext = union(enum) {
            null,
            accounts: AccountsGroove.PrefetchContext,
            transfers: TransfersGroove.PrefetchContext,
            transfers_pending: TransfersPendingGroove.PrefetchContext,

            pub const Field = std.meta.FieldEnum(PrefetchContext);
            pub fn FieldType(comptime field: Field) type {
                return @FieldType(PrefetchContext, @tagName(field));
            }

            pub fn parent(
                comptime field: Field,
                completion: *FieldType(field),
            ) *StateMachine {
                comptime assert(field != .null);

                const context: *PrefetchContext = @fieldParentPtr(@tagName(field), completion);
                return @fieldParentPtr("prefetch_context", context);
            }

            pub fn get(self: *PrefetchContext, comptime field: Field) *FieldType(field) {
                comptime assert(field != .null);
                assert(self.* == .null);

                self.* = @unionInit(PrefetchContext, @tagName(field), undefined);
                return &@field(self, @tagName(field));
            }
        };

        const ExpirePendingTransfers = ExpirePendingTransfersType(TransfersGroove, Storage);

        /// Since scan lookups are used one at a time, it's safe to access
        /// the union's fields and reuse the same memory for all ScanLookup instances.
        const ScanLookup = union(enum) {
            null,
            transfers: TransfersScanLookup,
            accounts: AccountsScanLookup,
            account_balances: AccountBalancesScanLookup,
            expire_pending_transfers: ExpirePendingTransfers.ScanLookup,
            change_events: ChangeEventsScanLookup,

            pub const Field = std.meta.FieldEnum(ScanLookup);
            pub fn FieldType(comptime field: Field) type {
                return @FieldType(ScanLookup, @tagName(field));
            }

            pub fn parent(
                comptime field: Field,
                completion: *FieldType(field),
            ) *StateMachine {
                comptime assert(field != .null);

                const context: *ScanLookup = @fieldParentPtr(@tagName(field), completion);
                return @fieldParentPtr("scan_lookup", context);
            }

            pub fn get(self: *ScanLookup, comptime field: Field) *FieldType(field) {
                comptime assert(field != .null);
                assert(self.* == .null);

                self.* = @unionInit(ScanLookup, @tagName(field), undefined);
                return &@field(self, @tagName(field));
            }
        };

        const Metrics = struct {
            create_accounts: TimingSummary = .{},
            create_transfers: TimingSummary = .{},
            lookup_accounts: TimingSummary = .{},
            lookup_transfers: TimingSummary = .{},
            get_account_transfers: TimingSummary = .{},
            get_account_balances: TimingSummary = .{},
            query_accounts: TimingSummary = .{},
            query_transfers: TimingSummary = .{},
            get_change_events: TimingSummary = .{},

            compact: TimingSummary = .{},
            checkpoint: TimingSummary = .{},

            timer: vsr.time.Timer,

            const TimingSummary = struct {
                duration_min_us: ?u64 = null,
                duration_max_us: ?u64 = null,
                duration_sum_us: u64 = 0,

                count: u64 = 0,

                /// If an operation supports batching (eg, lookup_accounts) this is a count of the
                /// number of internal operations.
                count_batch: u64 = 0,
            };

            /// Technically 'timer' can't be used, but that'll error out at comptime.
            const MetricEnum = std.meta.FieldEnum(Metrics);

            pub fn log_and_reset(metrics: *Metrics) void {
                inline for (comptime std.meta.fieldNames(Metrics)) |field_name| {
                    if (comptime !std.mem.eql(u8, field_name, "timer")) {
                        const timing: *TimingSummary = &@field(metrics, field_name);
                        if (timing.count > 0) {
                            log.info("{s}: p0={?}us mean={}us p100={?}us " ++
                                "sum={}us count={} count_batch={}", .{
                                field_name,
                                timing.duration_min_us,
                                @divFloor(timing.duration_sum_us, timing.count),
                                timing.duration_max_us,
                                timing.duration_sum_us,
                                timing.count,
                                timing.count_batch,
                            });
                        }
                    }
                }

                metrics.* = .{
                    .timer = metrics.timer,
                };
            }

            pub fn record(
                metrics: *Metrics,
                comptime metric: MetricEnum,
                duration_us: u64,
                count_batch: u64,
            ) void {
                const timing: *Metrics.TimingSummary = &@field(
                    metrics,
                    @tagName(metric),
                );

                timing.duration_min_us = if (timing.duration_min_us) |duration_min_us|
                    @min(duration_min_us, duration_us)
                else
                    duration_us;
                timing.duration_max_us = if (timing.duration_max_us) |duration_max_us|
                    @max(duration_max_us, duration_us)
                else
                    duration_us;
                timing.duration_sum_us += duration_us;
                timing.count += 1;
                timing.count_batch += count_batch;
            }

            fn from_operation(comptime operation: Operation) MetricEnum {
                return switch (operation) {
                    .create_accounts,
                    .deprecated_create_accounts_unbatched,
                    => .create_accounts,

                    .create_transfers,
                    .deprecated_create_transfers_unbatched,
                    => .create_transfers,

                    .lookup_accounts,
                    .deprecated_lookup_accounts_unbatched,
                    => .lookup_accounts,

                    .lookup_transfers,
                    .deprecated_lookup_transfers_unbatched,
                    => .lookup_transfers,

                    .get_account_transfers,
                    .deprecated_get_account_transfers_unbatched,
                    => .get_account_transfers,

                    .get_account_balances,
                    .deprecated_get_account_balances_unbatched,
                    => .get_account_balances,

                    .query_accounts,
                    .deprecated_query_accounts_unbatched,
                    => .query_accounts,

                    .query_transfers,
                    .deprecated_query_transfers_unbatched,
                    => .query_transfers,

                    .get_change_events,
                    => .get_change_events,

                    .pulse => comptime unreachable,
                };
            }
        };

        pub fn init(
            self: *StateMachine,
            allocator: mem.Allocator,
            time: vsr.time.Time,
            grid: *Grid,
            options: Options,
        ) !void {
            assert(options.batch_size_limit <= constants.message_body_size_max);
            inline for (comptime std.enums.values(Operation)) |operation| {
                assert(options.batch_size_limit >= operation.event_size());
            }

            self.* = .{
                .batch_size_limit = options.batch_size_limit,
                .prefetch_timestamp = 0,
                .prepare_timestamp = 0,
                .commit_timestamp = 0,

                .forest = undefined,
                .scan_lookup_buffer = undefined,
                .scan_lookup_results = undefined,

                .metrics = .{
                    .timer = .init(time),
                },

                .log_trace = options.log_trace,
            };

            try self.forest.init(
                allocator,
                grid,
                .{
                    .compaction_block_count = options.lsm_forest_compaction_block_count,
                    .node_count = options.lsm_forest_node_count,
                },
                forest_options(options),
            );
            errdefer self.forest.deinit(allocator);

            // The scan lookup buffer and the list that holds the result counts are shared between
            // all operations, so they need to be large enough for the worst case.
            const scan_lookup_buffer_size: usize, const scan_lookup_result_max: u16 =
                max: {
                    const operations: []const Operation = &.{
                        .get_account_transfers,
                        .get_account_balances,
                        .query_accounts,
                        .query_transfers,
                        .get_change_events,

                        .deprecated_get_account_transfers_unbatched,
                        .deprecated_get_account_balances_unbatched,
                        .deprecated_query_accounts_unbatched,
                        .deprecated_query_transfers_unbatched,
                    };
                    var batch_count_max: u16 = 0;
                    var buffer_size_max: usize = 0;
                    inline for (operations) |operation| {
                        // The `Groove` object is stored in the buffer, not necessarily
                        // the same as `ResultType(operation)`.
                        const object_size: usize = switch (operation) {
                            .get_account_transfers,
                            .deprecated_get_account_transfers_unbatched,
                            => @sizeOf(Transfer),
                            .get_account_balances,
                            .deprecated_get_account_balances_unbatched,
                            => @sizeOf(AccountEvent),
                            .query_accounts,
                            .deprecated_query_accounts_unbatched,
                            => @sizeOf(Account),
                            .query_transfers,
                            .deprecated_query_transfers_unbatched,
                            => @sizeOf(Transfer),
                            .get_change_events => @sizeOf(AccountEvent),
                            else => comptime unreachable,
                        };
                        buffer_size_max = @max(
                            buffer_size_max,
                            operation.result_max(
                                self.batch_size_limit,
                            ) * object_size,
                        );

                        // For multi-batched queries, the result count of each individual query
                        // is stored in a list and used as the offset into `scan_lookup_buffer`.
                        batch_count_max = @max(
                            batch_count_max,
                            if (operation.is_multi_batch())
                                vsr.multi_batch.multi_batch_count_max(.{
                                    .batch_size_min = operation.event_size(),
                                    .batch_size_limit = options.batch_size_limit,
                                })
                            else
                                1,
                        );
                    }
                    break :max .{ buffer_size_max, batch_count_max };
                };
            self.scan_lookup_buffer = try allocator.alignedAlloc(u8, 16, scan_lookup_buffer_size);
            errdefer allocator.free(self.scan_lookup_buffer);

            self.scan_lookup_results = try std.ArrayListUnmanaged(u32).initCapacity(
                allocator,
                scan_lookup_result_max,
            );
            errdefer self.scan_lookup_results.deinit(allocator);
        }

        pub fn deinit(self: *StateMachine, allocator: mem.Allocator) void {
            allocator.free(self.scan_lookup_buffer);
            self.scan_lookup_results.deinit(allocator);
            self.forest.deinit(allocator);
        }

        pub fn reset(self: *StateMachine) void {
            self.forest.reset();
            self.scan_lookup_results.clearRetainingCapacity();

            self.* = .{
                .batch_size_limit = self.batch_size_limit,
                .prefetch_timestamp = 0,
                .prepare_timestamp = 0,
                .commit_timestamp = 0,
                .forest = self.forest,
                .scan_lookup_buffer = self.scan_lookup_buffer,
                .scan_lookup_results = self.scan_lookup_results,

                .metrics = .{
                    .timer = self.metrics.timer,
                },

                .log_trace = self.log_trace,
            };
        }

        pub fn open(self: *StateMachine, callback: *const fn (*StateMachine) void) void {
            assert(self.open_callback == null);
            self.open_callback = callback;

            self.forest.open(forest_open_callback);
        }

        fn forest_open_callback(forest: *Forest) void {
            const self: *StateMachine = @fieldParentPtr("forest", forest);
            assert(self.open_callback != null);

            const callback = self.open_callback.?;
            self.open_callback = null;
            callback(self);
        }

        pub fn input_valid(
            self: *const StateMachine,
            operation: Operation,
            message_body_used: []align(16) const u8,
        ) bool {
            // NB: This function should never accept `client_release` as an argument.
            // Any public API changes must be introduced explicitly as a new `operation` number.
            assert(message_body_used.len <= self.batch_size_limit);

            if (!operation.is_multi_batch()) {
                return self.batch_valid(operation, message_body_used);
            }
            assert(operation.is_multi_batch());

            const event_size: u32 = operation.event_size();
            maybe(event_size == 0);
            const result_size: u32 = operation.result_size();
            assert(result_size > 0);

            // Verifying whether the multi-batch message is properly encoded.
            var body_decoder = MultiBatchDecoder.init(message_body_used, .{
                .element_size = event_size,
            }) catch |err| switch (err) {
                error.MultiBatchInvalid => return false,
            };

            var result_count_expected: u32 = 0;
            while (body_decoder.pop()) |batch| {
                if (!self.batch_valid(operation, batch)) return false;
                result_count_expected += operation.result_count_expected(batch);
            }
            const reply_trailer_size: u32 = vsr.multi_batch.trailer_total_size(.{
                .element_size = result_size,
                .batch_count = body_decoder.batch_count(),
            });
            // Checking if the expected number of results will fit the reply.
            if (constants.message_body_size_max <
                (result_count_expected * result_size) +
                    reply_trailer_size)
            {
                return false;
            }

            return true;
        }

        /// Validates a batch.
        /// For multi-batch requests, this function expects a single, already decoded batch.
        fn batch_valid(
            self: *const StateMachine,
            operation: Operation,
            batch: []const u8,
        ) bool {
            assert(batch.len <= self.batch_size_limit);
            maybe(batch.len == 0);
            switch (operation) {
                .pulse => return batch.len == 0,
                inline else => |operation_comptime| {
                    const event_size = operation_comptime.event_size();
                    assert(event_size > 0);

                    if (comptime !operation_comptime.is_batchable()) {
                        return batch.len == event_size;
                    }
                    comptime assert(operation_comptime.is_batchable());

                    // Clients do not validate batch size == 0,
                    // and even the simulator can generate requests with no events.
                    maybe(batch.len == 0);
                    if (batch.len % event_size != 0) return false;

                    const event_max: u32 = operation_comptime.event_max(self.batch_size_limit);
                    assert(event_max > 0);

                    const event_count: u32 = @intCast(@divExact(batch.len, event_size));
                    if (event_count > event_max) return false;
                    return true;
                },
            }
        }

        /// Updates `prepare_timestamp` to the highest timestamp of the response.
        pub fn prepare(
            self: *StateMachine,
            operation: Operation,
            message_body_used: []align(16) const u8,
        ) void {
            assert(message_body_used.len <= self.batch_size_limit);
            const delta: u64 = delta: {
                if (!operation.is_multi_batch()) {
                    break :delta self.prepare_delta_nanoseconds(
                        operation,
                        message_body_used,
                    );
                }
                assert(operation.is_multi_batch());

                var body_decoder = MultiBatchDecoder.init(message_body_used, .{
                    .element_size = operation.event_size(),
                }) catch unreachable; // Already validated by `input_valid()`.

                var delta: u64 = 0;
                while (body_decoder.pop()) |batch| {
                    delta += self.prepare_delta_nanoseconds(
                        operation,
                        batch,
                    );
                }
                break :delta delta;
            };

            maybe(delta == 0);
            self.prepare_timestamp += delta;
        }

        /// Returns the logical time increment (in nanoseconds) for the highest
        /// timestamp of the batch.
        /// For multi-batch requests, this function expects a single, already decoded batch.
        fn prepare_delta_nanoseconds(
            self: *StateMachine,
            operation: Operation,
            batch: []const u8,
        ) u64 {
            assert(batch.len <= self.batch_size_limit);
            return switch (operation) {
                .pulse => batch_max.create_transfers, // Max transfers to expire.
                .create_accounts => @divExact(batch.len, @sizeOf(Account)),
                .create_transfers => @divExact(batch.len, @sizeOf(Transfer)),
                .lookup_accounts => 0,
                .lookup_transfers => 0,
                .get_account_transfers => 0,
                .get_account_balances => 0,
                .query_accounts => 0,
                .query_transfers => 0,
                .get_change_events => 0,

                .deprecated_create_accounts_unbatched => @divExact(batch.len, @sizeOf(Account)),
                .deprecated_create_transfers_unbatched => @divExact(batch.len, @sizeOf(Transfer)),
                .deprecated_lookup_accounts_unbatched => 0,
                .deprecated_lookup_transfers_unbatched => 0,
                .deprecated_get_account_transfers_unbatched => 0,
                .deprecated_get_account_balances_unbatched => 0,
                .deprecated_query_accounts_unbatched => 0,
                .deprecated_query_transfers_unbatched => 0,
            };
        }

        pub fn pulse_needed(self: *const StateMachine, timestamp: u64) bool {
            assert(!constants.aof_recovery);
            assert(self.expire_pending_transfers.pulse_next_timestamp >=
                TimestampRange.timestamp_min);

            return self.expire_pending_transfers.pulse_next_timestamp <= timestamp;
        }

        pub fn prefetch(
            self: *StateMachine,
            callback: *const fn (*StateMachine) void,
            op: u64,
            operation: Operation,
            message_body_used: []align(16) const u8,
        ) void {
            // NB: This function should never accept `client_release` as an argument.
            // Any public API changes must be introduced explicitly as a new `operation` number.
            assert(op > 0);
            assert(self.prefetch_operation == null);
            assert(self.prefetch_input == null);
            assert(self.prefetch_callback == null);
            assert(message_body_used.len <= self.batch_size_limit);

            assert(self.scan_lookup_buffer_index == 0);
            assert(self.scan_lookup_results.items.len == 0);

            const prefetch_input: []const u8 = input: {
                if (!operation.is_multi_batch()) {
                    assert(self.batch_valid(operation, message_body_used));
                    break :input message_body_used;
                }
                assert(operation.is_multi_batch());

                var body_decoder = MultiBatchDecoder.init(
                    message_body_used,
                    .{
                        .element_size = operation.event_size(),
                    },
                ) catch unreachable; // Already validated by `input_valid()`.
                while (body_decoder.pop()) |input| {
                    assert(self.batch_valid(operation, input));
                }
                break :input body_decoder.payload;
            };

            self.prefetch_operation = operation;
            self.prefetch_input = prefetch_input;
            self.prefetch_callback = callback;

            // TODO(Snapshots) Pass in the target snapshot.
            self.forest.grooves.accounts.prefetch_setup(null);
            self.forest.grooves.transfers.prefetch_setup(null);
            self.forest.grooves.transfers_pending.prefetch_setup(null);

            // Prefetch starts timing for an operation.
            self.metrics.timer.reset();

            switch (operation) {
                .pulse => self.prefetch_expire_pending_transfers(),
                .create_accounts => self.prefetch_create_accounts(),
                .create_transfers => self.prefetch_create_transfers(),
                .lookup_accounts => self.prefetch_lookup_accounts(),
                .lookup_transfers => self.prefetch_lookup_transfers(),
                .get_account_transfers => self.prefetch_get_account_transfers(),
                .get_account_balances => self.prefetch_get_account_balances(),
                .query_accounts => self.prefetch_query_accounts(),
                .query_transfers => self.prefetch_query_transfers(),
                .get_change_events => self.prefetch_get_change_events(),

                .deprecated_create_accounts_unbatched => {
                    self.prefetch_create_accounts();
                },
                .deprecated_create_transfers_unbatched => {
                    self.prefetch_create_transfers();
                },
                .deprecated_lookup_accounts_unbatched => {
                    self.prefetch_lookup_accounts();
                },
                .deprecated_lookup_transfers_unbatched => {
                    self.prefetch_lookup_transfers();
                },
                .deprecated_get_account_transfers_unbatched => {
                    self.prefetch_get_account_transfers();
                },
                .deprecated_get_account_balances_unbatched => {
                    self.prefetch_get_account_balances();
                },
                .deprecated_query_accounts_unbatched => {
                    self.prefetch_query_accounts();
                },
                .deprecated_query_transfers_unbatched => {
                    self.prefetch_query_transfers();
                },
            }
        }

        fn prefetch_finish(self: *StateMachine) void {
            assert(self.prefetch_operation != null);
            assert(self.prefetch_input != null);
            assert(self.prefetch_context == .null);
            assert(self.scan_lookup == .null);

            const callback = self.prefetch_callback.?;
            self.prefetch_operation = null;
            self.prefetch_input = null;
            self.prefetch_callback = null;

            callback(self);
        }

        fn prefetch_create_accounts(self: *StateMachine) void {
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation == .create_accounts or
                self.prefetch_operation == .deprecated_create_accounts_unbatched);

            const accounts = stdx.bytes_as_slice(
                .exact,
                Account,
                self.prefetch_input.?,
            );
            for (accounts) |*a| {
                self.forest.grooves.accounts.prefetch_enqueue(a.id);
            }

            self.forest.grooves.accounts.prefetch(
                prefetch_create_accounts_callback,
                self.prefetch_context.get(.accounts),
            );
        }

        fn prefetch_create_accounts_callback(
            completion: *AccountsGroove.PrefetchContext,
        ) void {
            const self: *StateMachine = PrefetchContext.parent(.accounts, completion);
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation == .create_accounts or
                self.prefetch_operation == .deprecated_create_accounts_unbatched);

            self.prefetch_context = .null;
            const accounts = stdx.bytes_as_slice(
                .exact,
                Account,
                self.prefetch_input.?,
            );
            if (accounts.len > 0 and
                accounts[0].flags.imported)
            {
                // Looking for transfers with the same timestamp.
                for (accounts) |*a| {
                    self.forest.grooves.transfers.prefetch_enqueue_by_timestamp(a.timestamp);
                }

                self.forest.grooves.transfers.prefetch(
                    prefetch_create_accounts_transfers_callback,
                    self.prefetch_context.get(.transfers),
                );
            } else {
                self.prefetch_finish();
            }
        }

        fn prefetch_create_accounts_transfers_callback(
            completion: *TransfersGroove.PrefetchContext,
        ) void {
            const self: *StateMachine = PrefetchContext.parent(.transfers, completion);
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation == .create_accounts or
                self.prefetch_operation == .deprecated_create_accounts_unbatched);

            self.prefetch_context = .null;
            self.prefetch_finish();
        }

        fn prefetch_create_transfers(self: *StateMachine) void {
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation == .create_transfers or
                self.prefetch_operation == .deprecated_create_transfers_unbatched);

            const transfers = stdx.bytes_as_slice(
                .exact,
                Transfer,
                self.prefetch_input.?,
            );
            for (transfers) |*t| {
                self.forest.grooves.transfers.prefetch_enqueue(t.id);

                if (t.flags.post_pending_transfer or t.flags.void_pending_transfer) {
                    self.forest.grooves.transfers.prefetch_enqueue(t.pending_id);
                }
            }

            self.forest.grooves.transfers.prefetch(
                prefetch_create_transfers_callback_transfers,
                self.prefetch_context.get(.transfers),
            );
        }

        fn prefetch_create_transfers_callback_transfers(
            completion: *TransfersGroove.PrefetchContext,
        ) void {
            const self: *StateMachine = PrefetchContext.parent(.transfers, completion);
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation == .create_transfers or
                self.prefetch_operation == .deprecated_create_transfers_unbatched);

            self.prefetch_context = .null;
            const transfers = stdx.bytes_as_slice(
                .exact,
                Transfer,
                self.prefetch_input.?,
            );
            for (transfers) |*t| {
                if (t.flags.post_pending_transfer or t.flags.void_pending_transfer) {
                    if (self.get_transfer(t.pending_id)) |p| {
                        // This prefetch isn't run yet, but enqueue it here as well to save an extra
                        // iteration over transfers.
                        self.forest.grooves.transfers_pending.prefetch_enqueue(p.timestamp);

                        self.forest.grooves.accounts.prefetch_enqueue(p.debit_account_id);
                        self.forest.grooves.accounts.prefetch_enqueue(p.credit_account_id);
                    }
                } else {
                    self.forest.grooves.accounts.prefetch_enqueue(t.debit_account_id);
                    self.forest.grooves.accounts.prefetch_enqueue(t.credit_account_id);
                }
            }

            if (transfers.len > 0 and
                transfers[0].flags.imported)
            {
                // Looking for accounts with the same timestamp.
                // This logic could be in the loop above, but we choose to iterate again,
                // avoiding an extra comparison in the more common case of non-imported batches.
                for (transfers) |*t| {
                    self.forest.grooves.accounts.prefetch_enqueue_by_timestamp(t.timestamp);
                }
            }

            self.forest.grooves.accounts.prefetch(
                prefetch_create_transfers_callback_accounts,
                self.prefetch_context.get(.accounts),
            );
        }

        fn prefetch_create_transfers_callback_accounts(
            completion: *AccountsGroove.PrefetchContext,
        ) void {
            const self: *StateMachine = PrefetchContext.parent(.accounts, completion);
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation == .create_transfers or
                self.prefetch_operation == .deprecated_create_transfers_unbatched);

            self.prefetch_context = .null;
            self.forest.grooves.transfers_pending.prefetch(
                prefetch_create_transfers_callback_transfers_pending,
                self.prefetch_context.get(.transfers_pending),
            );
        }

        fn prefetch_create_transfers_callback_transfers_pending(
            completion: *TransfersPendingGroove.PrefetchContext,
        ) void {
            const self: *StateMachine = PrefetchContext.parent(.transfers_pending, completion);
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation == .create_transfers or
                self.prefetch_operation == .deprecated_create_transfers_unbatched);

            self.prefetch_context = .null;
            self.prefetch_finish();
        }

        fn prefetch_lookup_accounts(self: *StateMachine) void {
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation == .lookup_accounts or
                self.prefetch_operation == .deprecated_lookup_accounts_unbatched);

            const ids = stdx.bytes_as_slice(
                .exact,
                u128,
                self.prefetch_input.?,
            );
            for (ids) |id| {
                self.forest.grooves.accounts.prefetch_enqueue(id);
            }

            self.forest.grooves.accounts.prefetch(
                prefetch_lookup_accounts_callback,
                self.prefetch_context.get(.accounts),
            );
        }

        fn prefetch_lookup_accounts_callback(completion: *AccountsGroove.PrefetchContext) void {
            const self: *StateMachine = PrefetchContext.parent(.accounts, completion);
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation == .lookup_accounts or
                self.prefetch_operation == .deprecated_lookup_accounts_unbatched);

            self.prefetch_context = .null;
            self.prefetch_finish();
        }

        fn prefetch_lookup_transfers(self: *StateMachine) void {
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation == .lookup_transfers or
                self.prefetch_operation == .deprecated_lookup_transfers_unbatched);

            const ids = stdx.bytes_as_slice(
                .exact,
                u128,
                self.prefetch_input.?,
            );
            for (ids) |id| {
                self.forest.grooves.transfers.prefetch_enqueue(id);
            }

            self.forest.grooves.transfers.prefetch(
                prefetch_lookup_transfers_callback,
                self.prefetch_context.get(.transfers),
            );
        }

        fn prefetch_lookup_transfers_callback(completion: *TransfersGroove.PrefetchContext) void {
            const self: *StateMachine = PrefetchContext.parent(.transfers, completion);
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation == .lookup_transfers or
                self.prefetch_operation == .deprecated_lookup_transfers_unbatched);

            self.prefetch_context = .null;
            self.prefetch_finish();
        }

        fn prefetch_get_account_transfers(self: *StateMachine) void {
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation.? == .get_account_transfers or
                self.prefetch_operation.? == .deprecated_get_account_transfers_unbatched);
            assert(self.scan_lookup == .null);
            assert(self.scan_lookup_buffer_index == 0);
            assert(self.scan_lookup_results.items.len == 0);

            const filter: *const AccountFilter = self.get_prefetch_account_filter().?;
            self.prefetch_get_account_transfers_scan(filter);
        }

        fn prefetch_get_account_transfers_scan(
            self: *StateMachine,
            filter: *const AccountFilter,
        ) void {
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation.? == .get_account_transfers or
                self.prefetch_operation.? == .deprecated_get_account_transfers_unbatched);
            assert(self.scan_lookup_buffer_index < self.scan_lookup_buffer.len);
            maybe(self.scan_lookup_results.items.len > 0);

            log.debug("{?}: get_account_transfers: {}", .{
                self.forest.grid.superblock.replica_index,
                filter,
            });

            assert(self.forest.scan_buffer_pool.scan_buffer_used == 0);
            if (self.get_scan_from_account_filter(filter)) |scan| {
                assert(self.forest.scan_buffer_pool.scan_buffer_used > 0);

                const scan_buffer = stdx.bytes_as_slice(
                    .inexact,
                    Transfer,
                    self.scan_lookup_buffer[self.scan_lookup_buffer_index..],
                );

                const scan_lookup = self.scan_lookup.get(.transfers);
                scan_lookup.* = TransfersScanLookup.init(
                    &self.forest.grooves.transfers,
                    scan,
                );

                // Limiting the buffer size according to the query limit.
                // TODO: Prevent clients from setting the limit larger than the buffer size.
                const limit = @min(
                    filter.limit,
                    self.prefetch_operation.?.result_max(self.batch_size_limit),
                );
                assert(limit > 0);
                assert(scan_buffer.len >= limit);
                scan_lookup.read(
                    scan_buffer[0..limit],
                    &prefetch_get_account_transfers_scan_callback,
                );
                return;
            }

            // TODO(batiati): Improve the way we do validations on the state machine.
            log.info("invalid filter for get_account_transfers: {any}", .{filter});
            self.forest.grid.on_next_tick(
                &prefetch_scan_next_tick_callback,
                &self.scan_lookup_next_tick,
            );
        }

        fn prefetch_get_account_transfers_scan_callback(
            scan_lookup: *TransfersScanLookup,
            results: []const Transfer,
        ) void {
            const self: *StateMachine = ScanLookup.parent(.transfers, scan_lookup);
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation.? == .get_account_transfers or
                self.prefetch_operation.? == .deprecated_get_account_transfers_unbatched);
            assert(self.scan_lookup_buffer_index < self.scan_lookup_buffer.len);
            maybe(self.scan_lookup_results.items.len > 0);

            self.scan_lookup_buffer_index += @intCast(results.len * @sizeOf(Transfer));
            self.scan_lookup_results.appendAssumeCapacity(@intCast(results.len));

            self.scan_lookup = .null;
            self.forest.scan_buffer_pool.reset();
            self.forest.grooves.transfers.scan_builder.reset();

            return self.prefetch_scan_resume();
        }

        fn prefetch_get_account_balances(self: *StateMachine) void {
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation.? == .get_account_balances or
                self.prefetch_operation.? == .deprecated_get_account_balances_unbatched);
            assert(self.scan_lookup == .null);
            assert(self.scan_lookup_buffer_index == 0);
            assert(self.scan_lookup_results.items.len == 0);

            const filters = stdx.bytes_as_slice(
                .exact,
                AccountFilter,
                self.prefetch_input.?,
            );
            assert(filters.len > 0);
            assert(filters.len == 1 or self.prefetch_operation.?.is_multi_batch());
            for (filters) |*filter| {
                self.forest.grooves.accounts.prefetch_enqueue(filter.account_id);
            }
            self.forest.grooves.accounts.prefetch(
                prefetch_get_account_balances_lookup_account_callback,
                self.prefetch_context.get(.accounts),
            );
        }

        fn prefetch_get_account_balances_lookup_account_callback(
            completion: *AccountsGroove.PrefetchContext,
        ) void {
            const self: *StateMachine = PrefetchContext.parent(.accounts, completion);
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation.? == .get_account_balances or
                self.prefetch_operation.? == .deprecated_get_account_balances_unbatched);
            assert(self.scan_lookup == .null);
            assert(self.scan_lookup_buffer_index == 0);
            assert(self.scan_lookup_results.items.len == 0);

            self.prefetch_context = .null;
            const filter: *const AccountFilter = self.get_prefetch_account_filter().?;
            self.prefetch_get_account_balances_scan(filter);
        }

        fn prefetch_get_account_balances_scan(
            self: *StateMachine,
            filter: *const AccountFilter,
        ) void {
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation.? == .get_account_balances or
                self.prefetch_operation.? == .deprecated_get_account_balances_unbatched);
            assert(self.scan_lookup_buffer_index < self.scan_lookup_buffer.len);
            maybe(self.scan_lookup_results.items.len > 0);

            log.debug("{?}: get_account_balances: {}", .{
                self.forest.grid.superblock.replica_index,
                filter,
            });

            assert(self.forest.scan_buffer_pool.scan_buffer_used == 0);
            if (self.get_account(filter.account_id)) |account| {
                if (account.flags.history) {
                    if (self.get_scan_from_account_filter(filter)) |scan| {
                        assert(self.forest.scan_buffer_pool.scan_buffer_used > 0);

                        const scan_buffer = stdx.bytes_as_slice(
                            .inexact,
                            AccountEvent,
                            self.scan_lookup_buffer[self.scan_lookup_buffer_index..],
                        );

                        const scan_lookup = self.scan_lookup.get(.account_balances);
                        scan_lookup.* = AccountBalancesScanLookup.init(
                            &self.forest.grooves.account_events,
                            scan,
                        );

                        // Limiting the buffer size according to the query limit.
                        // TODO: Prevent clients from setting the limit larger than the buffer size.
                        const limit = @min(
                            filter.limit,
                            self.prefetch_operation.?.result_max(self.batch_size_limit),
                        );
                        assert(limit > 0);
                        assert(scan_buffer.len >= limit);
                        scan_lookup.read(
                            scan_buffer[0..limit],
                            &prefetch_get_account_balances_scan_callback,
                        );
                        return;
                    } else {
                        // TODO(batiati): Improve the way we do validations on the state machine.
                        log.info("get_account_balances: invalid filter: {any}", .{filter});
                    }
                } else {
                    log.info(
                        "get_account_balances: cannot query account.id={}; flags.history=false",
                        .{filter.account_id},
                    );
                }
            } else {
                log.info(
                    "get_account_balances: cannot query account.id={}; account does not exist",
                    .{filter.account_id},
                );
            }

            // Returning an empty array on the next tick.
            self.forest.grid.on_next_tick(
                &prefetch_scan_next_tick_callback,
                &self.scan_lookup_next_tick,
            );
        }

        fn prefetch_get_account_balances_scan_callback(
            scan_lookup: *AccountBalancesScanLookup,
            results: []const AccountEvent,
        ) void {
            const self: *StateMachine = ScanLookup.parent(.account_balances, scan_lookup);
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation.? == .get_account_balances or
                self.prefetch_operation.? == .deprecated_get_account_balances_unbatched);
            assert(self.scan_lookup_buffer_index < self.scan_lookup_buffer.len);
            maybe(self.scan_lookup_results.items.len > 0);

            self.scan_lookup_buffer_index += @intCast(results.len * @sizeOf(AccountEvent));
            self.scan_lookup_results.appendAssumeCapacity(@intCast(results.len));

            self.forest.scan_buffer_pool.reset();
            self.forest.grooves.transfers.scan_builder.reset();
            self.scan_lookup = .null;

            return self.prefetch_scan_resume();
        }

        /// Returns the `AccountFilter` from the prefetch input buffer.
        /// In the case of multi-batch inputs, returns the current filter
        /// or `null` if all filters have been executed.
        fn get_prefetch_account_filter(self: *StateMachine) ?*const AccountFilter {
            assert(self.prefetch_input != null);
            assert(self.scan_lookup_buffer_index <= self.scan_lookup_buffer.len);

            switch (self.prefetch_operation.?) {
                .get_account_transfers, .get_account_balances => {
                    const filter_index = self.scan_lookup_results.items.len;
                    maybe(filter_index > 0);

                    const filters = stdx.bytes_as_slice(
                        .exact,
                        AccountFilter,
                        self.prefetch_input.?,
                    );
                    assert(filters.len > 0);
                    assert(filter_index <= filters.len);

                    // Returns null if all filters were processed.
                    if (filter_index == filters.len) return null;
                    return &filters[filter_index];
                },
                .deprecated_get_account_transfers_unbatched,
                .deprecated_get_account_balances_unbatched,
                => {
                    // Operations not encoded as multi-batch must have only a single filter.
                    assert(self.scan_lookup_results.items.len == 0);
                    const filter: *const AccountFilter = @alignCast(std.mem.bytesAsValue(
                        AccountFilter,
                        self.prefetch_input.?,
                    ));
                    return filter;
                },
                else => unreachable,
            }
        }

        fn get_scan_from_account_filter(
            self: *StateMachine,
            filter: *const AccountFilter,
        ) ?*TransfersGroove.ScanBuilder.Scan {
            assert(self.forest.scan_buffer_pool.scan_buffer_used == 0);

            const filter_valid =
                filter.account_id != 0 and filter.account_id != std.math.maxInt(u128) and
                (filter.timestamp_min == 0 or TimestampRange.valid(filter.timestamp_min)) and
                (filter.timestamp_max == 0 or TimestampRange.valid(filter.timestamp_max)) and
                (filter.timestamp_max == 0 or filter.timestamp_min <= filter.timestamp_max) and
                filter.limit != 0 and
                (filter.flags.credits or filter.flags.debits) and
                filter.flags.padding == 0 and
                stdx.zeroed(&filter.reserved);

            if (!filter_valid) return null;

            const transfers_groove: *TransfersGroove = &self.forest.grooves.transfers;
            const scan_builder: *TransfersGroove.ScanBuilder = &transfers_groove.scan_builder;

            const timestamp_range: TimestampRange = .{
                .min = if (filter.timestamp_min == 0)
                    TimestampRange.timestamp_min
                else
                    filter.timestamp_min,

                .max = if (filter.timestamp_max == 0)
                    TimestampRange.timestamp_max
                else
                    filter.timestamp_max,
            };
            assert(timestamp_range.min <= timestamp_range.max);

            // This expression may have at most 5 scans, the `debit OR credit`
            // counts as just one:
            // ```
            //   WHERE
            //     (debit_account_id=? OR credit_account_id=?) AND
            //     user_data_128=? AND
            //     user_data_64=? AND
            //     user_data_32=? AND
            //     code=?
            // ```
            var scan_conditions: stdx.BoundedArrayType(*TransfersGroove.ScanBuilder.Scan, 5) = .{};
            const direction: Direction = if (filter.flags.reversed) .descending else .ascending;

            // Adding the condition for `debit_account_id = $account_id`.
            if (filter.flags.debits) {
                scan_conditions.push(scan_builder.scan_prefix(
                    .debit_account_id,
                    self.forest.scan_buffer_pool.acquire_assume_capacity(),
                    snapshot_latest,
                    filter.account_id,
                    timestamp_range,
                    direction,
                ));
            }

            // Adding the condition for `credit_account_id = $account_id`.
            if (filter.flags.credits) {
                scan_conditions.push(scan_builder.scan_prefix(
                    .credit_account_id,
                    self.forest.scan_buffer_pool.acquire_assume_capacity(),
                    snapshot_latest,
                    filter.account_id,
                    timestamp_range,
                    direction,
                ));
            }

            switch (scan_conditions.count()) {
                1 => {},
                2 => {
                    // Creating an union `OR` with the `debit_account_id` and `credit_account_id`.
                    const accounts_merge = scan_builder.merge_union(scan_conditions.const_slice());
                    scan_conditions.clear();
                    scan_conditions.push(accounts_merge);
                },
                else => unreachable,
            }

            // Additional filters with an intersection `AND`.
            inline for ([_]std.meta.FieldEnum(TransfersGroove.IndexTrees){
                .user_data_128, .user_data_64, .user_data_32, .code,
            }) |filter_field| {
                const filter_value = @field(filter, @tagName(filter_field));
                if (filter_value != 0) {
                    scan_conditions.push(scan_builder.scan_prefix(
                        filter_field,
                        self.forest.scan_buffer_pool.acquire_assume_capacity(),
                        snapshot_latest,
                        filter_value,
                        timestamp_range,
                        direction,
                    ));
                }
            }

            return switch (scan_conditions.count()) {
                1 => scan_conditions.get(0),
                2...5 => scan_builder.merge_intersection(scan_conditions.const_slice()),
                else => unreachable,
            };
        }

        fn prefetch_query_accounts(self: *StateMachine) void {
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation.? == .query_accounts or
                self.prefetch_operation.? == .deprecated_query_accounts_unbatched);
            assert(self.scan_lookup == .null);
            assert(self.scan_lookup_buffer_index == 0);
            assert(self.scan_lookup_results.items.len == 0);

            const filter: *const QueryFilter = self.get_prefetch_query_filter().?;
            self.prefetch_query_accounts_scan(filter);
        }

        fn prefetch_query_accounts_scan(self: *StateMachine, filter: *const QueryFilter) void {
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation.? == .query_accounts or
                self.prefetch_operation.? == .deprecated_query_accounts_unbatched);
            assert(self.scan_lookup_buffer_index < self.scan_lookup_buffer.len);
            maybe(self.scan_lookup_results.items.len > 0);

            log.debug("{?}: prefetch_query_accounts_scan: {}", .{
                self.forest.grid.superblock.replica_index,
                filter,
            });

            assert(self.forest.scan_buffer_pool.scan_buffer_used == 0);
            if (self.get_scan_from_query_filter(
                AccountsGroove,
                &self.forest.grooves.accounts,
                filter,
            )) |scan| {
                assert(self.forest.scan_buffer_pool.scan_buffer_used > 0);

                const scan_buffer = stdx.bytes_as_slice(
                    .inexact,
                    Account,
                    self.scan_lookup_buffer[self.scan_lookup_buffer_index..],
                );

                const scan_lookup = self.scan_lookup.get(.accounts);
                scan_lookup.* = AccountsScanLookup.init(
                    &self.forest.grooves.accounts,
                    scan,
                );

                // Limiting the buffer size according to the query limit.
                // TODO: Prevent clients from setting the limit larger than the reply size by
                // failing with `TooMuchData`.
                const limit = @min(
                    filter.limit,
                    self.prefetch_operation.?.result_max(self.batch_size_limit),
                );
                assert(limit > 0);
                assert(scan_buffer.len >= limit);
                scan_lookup.read(
                    scan_buffer[0..limit],
                    &prefetch_query_accounts_scan_callback,
                );
                return;
            }

            // TODO(batiati): Improve the way we do validations on the state machine.
            log.info("invalid filter for query_accounts: {any}", .{filter});
            self.forest.grid.on_next_tick(
                &prefetch_scan_next_tick_callback,
                &self.scan_lookup_next_tick,
            );
        }

        fn prefetch_query_accounts_scan_callback(
            scan_lookup: *AccountsScanLookup,
            results: []const Account,
        ) void {
            const self: *StateMachine = ScanLookup.parent(.accounts, scan_lookup);
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation.? == .query_accounts or
                self.prefetch_operation.? == .deprecated_query_accounts_unbatched);
            assert(self.scan_lookup_buffer_index < self.scan_lookup_buffer.len);
            maybe(self.scan_lookup_results.items.len > 0);

            self.scan_lookup_buffer_index += @intCast(results.len * @sizeOf(Account));
            self.scan_lookup_results.appendAssumeCapacity(@intCast(results.len));

            self.scan_lookup = .null;
            self.forest.scan_buffer_pool.reset();
            self.forest.grooves.accounts.scan_builder.reset();

            return self.prefetch_scan_resume();
        }

        fn prefetch_query_transfers(self: *StateMachine) void {
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation.? == .query_transfers or
                self.prefetch_operation.? == .deprecated_query_transfers_unbatched);
            assert(self.scan_lookup == .null);
            assert(self.scan_lookup_buffer_index == 0);
            assert(self.scan_lookup_results.items.len == 0);

            const filter: *const QueryFilter = self.get_prefetch_query_filter().?;
            self.prefetch_query_transfers_scan(filter);
        }

        fn prefetch_query_transfers_scan(self: *StateMachine, filter: *const QueryFilter) void {
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation.? == .query_transfers or
                self.prefetch_operation.? == .deprecated_query_transfers_unbatched);
            assert(self.scan_lookup_buffer_index < self.scan_lookup_buffer.len);
            maybe(self.scan_lookup_results.items.len > 0);

            log.debug("{?}: prefetch_query_transfers_scan: {}", .{
                self.forest.grid.superblock.replica_index,
                filter,
            });

            assert(self.forest.scan_buffer_pool.scan_buffer_used == 0);
            if (self.get_scan_from_query_filter(
                TransfersGroove,
                &self.forest.grooves.transfers,
                filter,
            )) |scan| {
                assert(self.forest.scan_buffer_pool.scan_buffer_used > 0);

                const scan_buffer = stdx.bytes_as_slice(
                    .inexact,
                    Transfer,
                    self.scan_lookup_buffer[self.scan_lookup_buffer_index..],
                );

                const scan_lookup = self.scan_lookup.get(.transfers);
                scan_lookup.* = TransfersScanLookup.init(
                    &self.forest.grooves.transfers,
                    scan,
                );

                // Limiting the buffer size according to the query limit.
                // TODO: Prevent clients from setting the limit larger than the buffer size.
                const limit = @min(
                    filter.limit,
                    self.prefetch_operation.?.result_max(self.batch_size_limit),
                );
                assert(limit > 0);
                assert(scan_buffer.len >= limit);
                scan_lookup.read(
                    scan_buffer[0..limit],
                    &prefetch_query_transfers_scan_callback,
                );
                return;
            }

            // TODO(batiati): Improve the way we do validations on the state machine.
            log.info("invalid filter for query_transfers: {any}", .{filter});
            self.forest.grid.on_next_tick(
                &prefetch_scan_next_tick_callback,
                &self.scan_lookup_next_tick,
            );
        }

        fn prefetch_query_transfers_scan_callback(
            scan_lookup: *TransfersScanLookup,
            results: []const Transfer,
        ) void {
            const self: *StateMachine = ScanLookup.parent(.transfers, scan_lookup);
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation.? == .query_transfers or
                self.prefetch_operation.? == .deprecated_query_transfers_unbatched);
            assert(self.scan_lookup_buffer_index < self.scan_lookup_buffer.len);
            maybe(self.scan_lookup_results.items.len > 0);

            self.scan_lookup_buffer_index += @intCast(results.len * @sizeOf(Transfer));
            self.scan_lookup_results.appendAssumeCapacity(@intCast(results.len));

            self.scan_lookup = .null;
            self.forest.scan_buffer_pool.reset();
            self.forest.grooves.transfers.scan_builder.reset();

            return self.prefetch_scan_resume();
        }

        /// Returns the `QueryFilter` from the prefetch input buffer.
        /// In the case of multi-batch inputs, returns the current filter
        /// or `null` if all filters have been executed.
        fn get_prefetch_query_filter(self: *StateMachine) ?*const QueryFilter {
            assert(self.prefetch_input != null);
            assert(self.scan_lookup_buffer_index <= self.scan_lookup_buffer.len);

            switch (self.prefetch_operation.?) {
                .query_accounts, .query_transfers => {
                    const filter_index = self.scan_lookup_results.items.len;
                    maybe(filter_index > 0);

                    const filters = stdx.bytes_as_slice(
                        .exact,
                        QueryFilter,
                        self.prefetch_input.?,
                    );
                    assert(filters.len > 0);
                    assert(filter_index <= filters.len);

                    // Returns null if all filters were processed.
                    if (filter_index == filters.len) return null;
                    return &filters[filter_index];
                },
                .deprecated_query_accounts_unbatched, .deprecated_query_transfers_unbatched => {
                    // Operations not encoded as multi-batch must have only a single filter.
                    assert(self.scan_lookup_results.items.len == 0);
                    const filter: *const QueryFilter = @alignCast(std.mem.bytesAsValue(
                        QueryFilter,
                        self.prefetch_input.?,
                    ));
                    return filter;
                },
                else => unreachable,
            }
        }

        fn get_scan_from_query_filter(
            self: *StateMachine,
            comptime Groove: type,
            groove: *Groove,
            filter: *const QueryFilter,
        ) ?*Groove.ScanBuilder.Scan {
            assert(self.forest.scan_buffer_pool.scan_buffer_used == 0);

            const filter_valid =
                (filter.timestamp_min == 0 or TimestampRange.valid(filter.timestamp_min)) and
                (filter.timestamp_max == 0 or TimestampRange.valid(filter.timestamp_max)) and
                (filter.timestamp_max == 0 or filter.timestamp_min <= filter.timestamp_max) and
                filter.limit != 0 and
                filter.flags.padding == 0 and
                stdx.zeroed(&filter.reserved);

            if (!filter_valid) return null;

            const direction: Direction = if (filter.flags.reversed) .descending else .ascending;
            const timestamp_range: TimestampRange = .{
                .min = if (filter.timestamp_min == 0)
                    TimestampRange.timestamp_min
                else
                    filter.timestamp_min,

                .max = if (filter.timestamp_max == 0)
                    TimestampRange.timestamp_max
                else
                    filter.timestamp_max,
            };
            assert(timestamp_range.min <= timestamp_range.max);

            const indexes = [_]std.meta.FieldEnum(QueryFilter){
                .user_data_128,
                .user_data_64,
                .user_data_32,
                .ledger,
                .code,
            };
            comptime assert(indexes.len <= constants.lsm_scans_max);

            var scan_conditions: stdx.BoundedArrayType(*Groove.ScanBuilder.Scan, indexes.len) = .{};
            inline for (indexes) |index| {
                if (@field(filter, @tagName(index)) != 0) {
                    scan_conditions.push(groove.scan_builder.scan_prefix(
                        std.enums.nameCast(std.meta.FieldEnum(Groove.IndexTrees), index),
                        self.forest.scan_buffer_pool.acquire_assume_capacity(),
                        snapshot_latest,
                        @field(filter, @tagName(index)),
                        timestamp_range,
                        direction,
                    ));
                }
            }

            return switch (scan_conditions.count()) {
                0 =>
                // TODO(batiati): Querying only by timestamp uses the Object groove,
                // we could skip the lookup step entirely then.
                // It will be implemented as part of the query executor.
                groove.scan_builder.scan_timestamp(
                    self.forest.scan_buffer_pool.acquire_assume_capacity(),
                    snapshot_latest,
                    timestamp_range,
                    direction,
                ),
                1 => scan_conditions.get(0),
                else => groove.scan_builder.merge_intersection(scan_conditions.const_slice()),
            };
        }

        /// Common `next_tick` callback used by all `prefetch_scan_*` functions to complete
        /// the operation when the filter is invalid.
        fn prefetch_scan_next_tick_callback(completion: *Grid.NextTick) void {
            const self: *StateMachine = @alignCast(@fieldParentPtr(
                "scan_lookup_next_tick",
                completion,
            ));

            // Invalid filter, no results found.
            self.scan_lookup_results.appendAssumeCapacity(0);

            self.prefetch_scan_resume();
        }

        fn prefetch_scan_resume(self: *StateMachine) void {
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation != null);
            assert(self.scan_lookup == .null);
            maybe(self.scan_lookup_buffer_index > 0);
            maybe(self.scan_lookup_results.items.len > 0);
            assert(self.forest.scan_buffer_pool.scan_buffer_used == 0);

            // Processes the next filter in the case of multi-batch messages.
            switch (self.prefetch_operation.?) {
                .get_account_transfers => {
                    if (self.get_prefetch_account_filter()) |filter_next| {
                        return self.prefetch_get_account_transfers_scan(filter_next);
                    }
                },
                .get_account_balances => {
                    if (self.get_prefetch_account_filter()) |filter_next| {
                        return self.prefetch_get_account_balances_scan(filter_next);
                    }
                },
                .query_accounts => {
                    if (self.get_prefetch_query_filter()) |filter_next| {
                        return self.prefetch_query_accounts_scan(filter_next);
                    }
                },
                .query_transfers => {
                    if (self.get_prefetch_query_filter()) |filter_next| {
                        return self.prefetch_query_transfers_scan(filter_next);
                    }
                },
                .get_change_events => {},

                .deprecated_get_account_transfers_unbatched,
                .deprecated_get_account_balances_unbatched,
                .deprecated_query_accounts_unbatched,
                .deprecated_query_transfers_unbatched,
                => {},

                else => unreachable, // Not query operations.
            }

            self.prefetch_finish();
        }

        fn prefetch_get_change_events(self: *StateMachine) void {
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation.? == .get_change_events);
            assert(self.scan_lookup == .null);
            assert(self.scan_lookup_buffer_index == 0);
            assert(self.scan_lookup_results.items.len == 0);

            const filter: *const ChangeEventsFilter = self.get_prefetch_event_filter();
            self.prefetch_get_change_events_scan(filter);
        }

        fn prefetch_get_change_events_scan(
            self: *StateMachine,
            filter: *const ChangeEventsFilter,
        ) void {
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation.? == .get_change_events);
            assert(self.scan_lookup_buffer_index < self.scan_lookup_buffer.len);
            maybe(self.scan_lookup_results.items.len > 0);

            log.debug("{?}: prefetch_get_change_events_scan: {}", .{
                self.forest.grid.superblock.replica_index,
                filter,
            });

            assert(self.forest.scan_buffer_pool.scan_buffer_used == 0);
            if (self.get_scan_from_change_events_filter(filter)) |scan_lookup| {
                assert(self.forest.scan_buffer_pool.scan_buffer_used > 0);

                const scan_buffer = stdx.bytes_as_slice(
                    .inexact,
                    AccountEvent,
                    self.scan_lookup_buffer[self.scan_lookup_buffer_index..],
                );

                // TODO: For queries, the number of available prefetches may need to be considered:
                // - In cases like `query_accounts` and `query_transfers`, no prefetching is
                //   required, so the limit is simply `message_body_size_max / result_size`.
                // - In `get_account_balances`, one object is prefetched per each event (the query
                //   filter).
                // - In `get_change_events` and `expire_pending_transfers`, objects are prefetched
                //   per scanned result.
                // We could either:
                // - Calculate the number of prefetches based on the event/reply size, as is done
                //   for `create_*` and `lookup_*` operations;
                // - Or, make the `operation_{event,result}_max(...)` functions aware of the number
                //   of prefetches.
                const limit_max: u32 = limit_max: {
                    const result_max = self.prefetch_operation.?.result_max(self.batch_size_limit);
                    // Also constrained by the maximum number of available prefetches.
                    const prefetch_transfers = @max(
                        Operation.lookup_transfers.event_max(self.batch_size_limit),
                        Operation.deprecated_lookup_transfers_unbatched.event_max(
                            self.batch_size_limit,
                        ),
                    );
                    const prefetch_accounts = @max(
                        Operation.lookup_accounts.event_max(self.batch_size_limit),
                        Operation.deprecated_lookup_accounts_unbatched.event_max(
                            self.batch_size_limit,
                        ),
                    );

                    break :limit_max @min(
                        result_max,
                        prefetch_transfers,
                        // Each event == 2 accounts.
                        @divFloor(prefetch_accounts, 2),
                    );
                };

                // Limiting the buffer size according to the query limit.
                // TODO: Prevent clients from setting the limit larger than the buffer size.
                const limit = @min(filter.limit, limit_max);
                assert(limit > 0);
                assert(scan_buffer.len >= limit);
                scan_lookup.read(
                    scan_buffer[0..limit],
                    &prefetch_get_change_events_scan_callback,
                );
                return;
            }

            // TODO(batiati): Improve the way we do validations on the state machine.
            log.info("invalid filter for prefetch_get_change_events_scan: {any}", .{filter});
            self.forest.grid.on_next_tick(
                &prefetch_scan_next_tick_callback,
                &self.scan_lookup_next_tick,
            );
        }

        fn prefetch_get_change_events_scan_callback(
            scan_lookup: *ChangeEventsScanLookup,
            results: []const AccountEvent,
        ) void {
            const self: *StateMachine = ScanLookup.parent(.change_events, scan_lookup);
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation.? == .get_change_events);
            assert(self.scan_lookup_buffer_index < self.scan_lookup_buffer.len);
            assert(self.scan_lookup_results.items.len == 0);

            self.scan_lookup_buffer_index += @intCast(results.len * @sizeOf(AccountEvent));
            self.scan_lookup_results.appendAssumeCapacity(@intCast(results.len));

            self.forest.scan_buffer_pool.reset();
            self.forest.grooves.account_events.scan_builder.reset();
            self.scan_lookup = .null;

            if (results.len == 0) return self.prefetch_finish();

            const accounts: *AccountsGroove = &self.forest.grooves.accounts;
            const transfers: *TransfersGroove = &self.forest.grooves.transfers;
            for (results) |result| {
                switch (result.schema()) {
                    .current => {
                        assert(result.dr_account_timestamp != 0);
                        assert(result.cr_account_timestamp != 0);

                        accounts.prefetch_enqueue_by_timestamp(result.dr_account_timestamp);
                        accounts.prefetch_enqueue_by_timestamp(result.cr_account_timestamp);
                        if (result.transfer_pending_status == .expired) {
                            // For expiry events, the timestamp isn't associated with any transfer.
                            // Instead, the original pending transfer is prefetched.
                            assert(result.transfer_pending_id != 0);
                            transfers.prefetch_enqueue(result.transfer_pending_id);
                        } else {
                            transfers.prefetch_enqueue_by_timestamp(result.timestamp);
                        }
                    },
                    .former => |former| {
                        // In the former schema:
                        // If either the debit or credit account ID is zero (one side without
                        // the history flag), the lookup would have already omitted the event
                        // from the results.
                        assert(former.dr_account_id != 0);
                        assert(former.cr_account_id != 0);

                        accounts.prefetch_enqueue(former.dr_account_id);
                        accounts.prefetch_enqueue(former.cr_account_id);
                        transfers.prefetch_enqueue_by_timestamp(former.timestamp);
                    },
                }
            }

            accounts.prefetch(
                prefetch_get_change_events_callback_accounts,
                self.prefetch_context.get(.accounts),
            );
        }

        fn prefetch_get_change_events_callback_accounts(
            completion: *AccountsGroove.PrefetchContext,
        ) void {
            const self: *StateMachine = PrefetchContext.parent(.accounts, completion);
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation.? == .get_change_events);
            self.prefetch_context = .null;

            self.forest.grooves.transfers.prefetch(
                prefetch_get_change_events_callback_transfers,
                self.prefetch_context.get(.transfers),
            );
        }

        fn prefetch_get_change_events_callback_transfers(
            completion: *TransfersGroove.PrefetchContext,
        ) void {
            const self: *StateMachine = PrefetchContext.parent(.transfers, completion);
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation.? == .get_change_events);

            self.prefetch_context = .null;
            self.prefetch_finish();
        }

        /// Returns the `EventFilter` from the prefetch input buffer.
        fn get_prefetch_event_filter(self: *StateMachine) *const ChangeEventsFilter {
            assert(self.prefetch_input != null);
            assert(self.scan_lookup_buffer_index <= self.scan_lookup_buffer.len);

            // Operations not encoded as multi-batch must have only a single filter.
            assert(self.scan_lookup_results.items.len == 0);
            const filter: *const ChangeEventsFilter = @alignCast(std.mem.bytesAsValue(
                ChangeEventsFilter,
                self.prefetch_input.?,
            ));
            return filter;
        }

        fn get_scan_from_change_events_filter(
            self: *StateMachine,
            filter: *const ChangeEventsFilter,
        ) ?*ChangeEventsScanLookup {
            assert(self.forest.scan_buffer_pool.scan_buffer_used == 0);

            const filter_valid =
                (filter.timestamp_min == 0 or TimestampRange.valid(filter.timestamp_min)) and
                (filter.timestamp_max == 0 or TimestampRange.valid(filter.timestamp_max)) and
                (filter.timestamp_max == 0 or filter.timestamp_min <= filter.timestamp_max) and
                filter.limit != 0 and
                stdx.zeroed(&filter.reserved);

            if (!filter_valid) return null;

            // CDC is always in ascending order.
            const timestamp_range: TimestampRange = .{
                .min = if (filter.timestamp_min == 0)
                    TimestampRange.timestamp_min
                else
                    filter.timestamp_min,

                .max = if (filter.timestamp_max == 0)
                    TimestampRange.timestamp_max
                else
                    filter.timestamp_max,
            };
            assert(timestamp_range.min <= timestamp_range.max);

            const scan_lookup: *ChangeEventsScanLookup = self.scan_lookup.get(.change_events);
            scan_lookup.init(
                &self.forest.grooves.account_events.objects,
                self.forest.scan_buffer_pool.acquire_assume_capacity(),
                snapshot_latest,
                timestamp_range,
            );

            return scan_lookup;
        }

        fn prefetch_expire_pending_transfers(self: *StateMachine) void {
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation.? == .pulse);
            assert(self.scan_lookup_buffer_index == 0);
            assert(self.scan_lookup_results.items.len == 0);
            assert(self.forest.scan_buffer_pool.scan_buffer_used == 0);
            assert(TimestampRange.valid(self.prefetch_timestamp));

            // We must be constrained to the same limit as `create_transfers`.
            const scan_buffer_size = @max(
                Operation.create_transfers.event_max(self.batch_size_limit),
                Operation.deprecated_create_transfers_unbatched.event_max(self.batch_size_limit),
            ) * @sizeOf(Transfer);

            const scan_lookup_buffer = stdx.bytes_as_slice(
                .inexact,
                Transfer,
                self.scan_lookup_buffer[0..scan_buffer_size],
            );

            const transfers_groove: *TransfersGroove = &self.forest.grooves.transfers;
            const scan = self.expire_pending_transfers.scan(
                &transfers_groove.indexes.expires_at,
                self.forest.scan_buffer_pool.acquire_assume_capacity(),
                .{
                    .snapshot = transfers_groove.prefetch_snapshot.?,
                    .expires_at_max = self.prefetch_timestamp,
                },
            );

            const scan_lookup = self.scan_lookup.get(.expire_pending_transfers);
            scan_lookup.* = ExpirePendingTransfers.ScanLookup.init(
                transfers_groove,
                scan,
            );
            scan_lookup.read(
                scan_lookup_buffer,
                &prefetch_expire_pending_transfers_scan_callback,
            );
        }

        fn prefetch_expire_pending_transfers_scan_callback(
            scan_lookup: *ExpirePendingTransfers.ScanLookup,
            results: []const Transfer,
        ) void {
            const self: *StateMachine = ScanLookup.parent(.expire_pending_transfers, scan_lookup);
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation.? == .pulse);
            assert(self.scan_lookup_buffer_index < self.scan_lookup_buffer.len);
            assert(self.scan_lookup_results.items.len == 0);

            self.expire_pending_transfers.finish(scan_lookup.state, results);
            self.scan_lookup_buffer_index = @intCast(results.len * @sizeOf(Transfer));
            self.scan_lookup_results.appendAssumeCapacity(@intCast(results.len));

            self.scan_lookup = .null;
            self.forest.scan_buffer_pool.reset();
            self.forest.grooves.transfers.scan_builder.reset();

            self.prefetch_expire_pending_transfers_accounts();
        }

        fn prefetch_expire_pending_transfers_accounts(self: *StateMachine) void {
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation.? == .pulse);
            assert(self.scan_lookup_results.items.len == 1);
            maybe(self.scan_lookup_buffer_index == 0);

            const result_count: u32 = self.scan_lookup_results.items[0];
            if (result_count == 0) return self.prefetch_finish();

            const result_max: u32 = @max(
                Operation.create_transfers.event_max(self.batch_size_limit),
                Operation.deprecated_create_transfers_unbatched.event_max(self.batch_size_limit),
            );
            assert(result_count <= result_max);
            assert(self.scan_lookup_buffer_index == result_count * @sizeOf(Transfer));
            const transfers = stdx.bytes_as_slice(
                .exact,
                Transfer,
                self.scan_lookup_buffer[0..self.scan_lookup_buffer_index],
            );

            const grooves = &self.forest.grooves;
            for (transfers) |expired| {
                assert(expired.flags.pending == true);
                const expires_at = expired.timestamp + expired.timeout_ns();

                assert(expires_at <= self.prefetch_timestamp);

                grooves.accounts.prefetch_enqueue(expired.debit_account_id);
                grooves.accounts.prefetch_enqueue(expired.credit_account_id);
                grooves.transfers_pending.prefetch_enqueue(expired.timestamp);
            }

            self.forest.grooves.accounts.prefetch(
                prefetch_expire_pending_transfers_callback_accounts,
                self.prefetch_context.get(.accounts),
            );
        }

        fn prefetch_expire_pending_transfers_callback_accounts(
            completion: *AccountsGroove.PrefetchContext,
        ) void {
            const self: *StateMachine = PrefetchContext.parent(.accounts, completion);
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation.? == .pulse);
            self.prefetch_context = .null;

            self.forest.grooves.transfers_pending.prefetch(
                prefetch_expire_pending_transfers_callback_transfers_pending,
                self.prefetch_context.get(.transfers_pending),
            );
        }

        fn prefetch_expire_pending_transfers_callback_transfers_pending(
            completion: *TransfersPendingGroove.PrefetchContext,
        ) void {
            const self: *StateMachine = PrefetchContext.parent(.transfers_pending, completion);
            assert(self.prefetch_input != null);
            assert(self.prefetch_operation.? == .pulse);

            self.prefetch_context = .null;
            self.prefetch_finish();
        }

        pub fn commit(
            self: *StateMachine,
            client: u128,
            op: u64,
            timestamp: u64,
            operation: Operation,
            message_body_used: []align(16) const u8,
            output_buffer: *align(16) [constants.message_body_size_max]u8,
        ) usize {
            // NB: This function should never accept `client_release` as an argument.
            // Any public API changes must be introduced explicitly as a new `operation` number.
            assert(op != 0);
            assert(timestamp > self.commit_timestamp or constants.aof_recovery);
            assert(message_body_used.len <= self.batch_size_limit);
            if (client == 0) assert(operation == .pulse);

            maybe(self.scan_lookup_buffer_index > 0);
            maybe(self.scan_lookup_results.items.len > 0);
            defer {
                assert(self.scan_lookup_buffer_index == 0);
                assert(self.scan_lookup_results.items.len == 0);
            }

            const result: usize = switch (operation) {
                .pulse => self.execute_expire_pending_transfers(timestamp),
                inline .create_accounts,
                .create_transfers,
                .lookup_accounts,
                .lookup_transfers,
                => |operation_comptime| self.execute_multi_batch(
                    timestamp,
                    operation_comptime,
                    message_body_used,
                    output_buffer,
                ),
                inline .get_account_transfers,
                .get_account_balances,
                .query_accounts,
                .query_transfers,
                => |operation_comptime| self.execute_query_multi_batch(
                    operation_comptime,
                    message_body_used,
                    output_buffer,
                ),
                .get_change_events => self.execute_query(
                    .get_change_events,
                    message_body_used,
                    output_buffer,
                ),

                inline .deprecated_create_accounts_unbatched,
                .deprecated_create_transfers_unbatched,
                .deprecated_lookup_accounts_unbatched,
                .deprecated_lookup_transfers_unbatched,
                => |operation_comptime| self.execute(
                    timestamp,
                    operation_comptime,
                    message_body_used,
                    output_buffer,
                ),
                inline .deprecated_get_account_transfers_unbatched,
                .deprecated_get_account_balances_unbatched,
                .deprecated_query_accounts_unbatched,
                .deprecated_query_transfers_unbatched,
                => |operation_comptime| self.execute_query(
                    operation_comptime,
                    message_body_used,
                    output_buffer,
                ),
            };

            @setEvalBranchQuota(10_000);
            switch (operation) {
                .pulse => {},
                inline else => |operation_comptime| {
                    const event_size: u32 = operation_comptime.event_size();
                    const batch_count: u32 = batch_count: {
                        if (!operation_comptime.is_multi_batch()) {
                            break :batch_count @intCast(@divExact(
                                message_body_used.len,
                                event_size,
                            ));
                        }
                        comptime assert(operation_comptime.is_multi_batch());

                        const body_decoder = MultiBatchDecoder.init(message_body_used, .{
                            .element_size = event_size,
                        }) catch unreachable; // Already validated by `input_valid()`.
                        break :batch_count @intCast(@divExact(
                            body_decoder.payload.len,
                            event_size,
                        ));
                    };
                    const duration = self.metrics.timer.read();

                    self.metrics.record(
                        Metrics.from_operation(operation_comptime),
                        duration.to_us(),
                        batch_count,
                    );
                },
            }
            return result;
        }

        fn execute(
            self: *StateMachine,
            timestamp: u64,
            comptime operation: Operation,
            message_body_used: []align(16) const u8,
            output_buffer: *align(16) [constants.message_body_size_max]u8,
        ) usize {
            comptime assert(!operation.is_multi_batch());
            comptime assert(operation.is_batchable());

            switch (operation) {
                .deprecated_create_accounts_unbatched,
                .deprecated_create_transfers_unbatched,
                => return self.execute_create(
                    operation,
                    timestamp,
                    message_body_used,
                    output_buffer,
                ),
                .deprecated_lookup_accounts_unbatched => return self.execute_lookup_accounts(
                    message_body_used,
                    output_buffer,
                ),
                .deprecated_lookup_transfers_unbatched => return self.execute_lookup_transfers(
                    message_body_used,
                    output_buffer,
                ),
                else => comptime unreachable,
            }
        }

        fn execute_multi_batch(
            self: *StateMachine,
            timestamp: u64,
            comptime operation: Operation,
            message_body_used: []align(16) const u8,
            output_buffer: *align(16) [constants.message_body_size_max]u8,
        ) usize {
            comptime assert(operation.is_multi_batch());
            comptime assert(operation.is_batchable());

            var body_decoder = MultiBatchDecoder.init(message_body_used, .{
                .element_size = operation.event_size(),
            }) catch unreachable; // Already validated by `input_valid()`.
            assert(body_decoder.batch_count() >= 1);
            var reply_encoder = MultiBatchEncoder.init(output_buffer, .{
                .element_size = operation.result_size(),
            });

            var execute_timestamp: u64 = timestamp -
                self.prepare_delta_nanoseconds(
                    operation,
                    body_decoder.payload, // The entire message's body without the trailer.
                );
            while (body_decoder.pop()) |batch| {
                assert(self.batch_valid(operation, batch));
                // Commit each batched set of events
                // using the timestamp of the highest result of the response.
                execute_timestamp += self.prepare_delta_nanoseconds(
                    operation,
                    batch, // The batch's body.
                );
                const bytes_written: usize = switch (operation) {
                    .create_accounts,
                    .create_transfers,
                    => self.execute_create(
                        operation,
                        execute_timestamp,
                        batch,
                        reply_encoder.writable().?,
                    ),
                    .lookup_accounts => self.execute_lookup_accounts(
                        batch,
                        reply_encoder.writable().?,
                    ),
                    .lookup_transfers => self.execute_lookup_transfers(
                        batch,
                        reply_encoder.writable().?,
                    ),
                    else => comptime unreachable,
                };
                reply_encoder.add(@intCast(bytes_written));
            }
            assert(execute_timestamp == timestamp);
            assert(body_decoder.batch_count() == reply_encoder.batch_count);

            const encoded_bytes: usize = reply_encoder.finish();
            assert(encoded_bytes > 0);
            return encoded_bytes;
        }

        fn execute_query(
            self: *StateMachine,
            comptime operation: Operation,
            message_body_used: []align(16) const u8,
            output_buffer: *align(16) [constants.message_body_size_max]u8,
        ) usize {
            comptime assert(!operation.is_multi_batch());
            comptime assert(!operation.is_batchable());
            assert(self.scan_lookup_results.items.len > 0);
            maybe(self.scan_lookup_buffer_index == 0);
            defer {
                self.scan_lookup_buffer_index = 0;
                self.scan_lookup_results.clearRetainingCapacity();
            }

            assert(self.scan_lookup_results.items.len == 1);
            const result_count: u32 = self.scan_lookup_results.items[0];
            const result_size: u32 = self.scan_lookup_buffer_index;
            // Invalid filter or no results found.
            if (result_size == 0) {
                assert(result_count == 0);
                return 0;
            }

            assert(result_count > 0);
            assert(result_size % result_count == 0);
            assert(result_size <= self.scan_lookup_buffer.len);
            const bytes_written: usize = switch (operation) {
                .get_change_events => self.execute_get_change_events(
                    message_body_used,
                    self.scan_lookup_buffer[0..result_size],
                    output_buffer,
                ),
                .deprecated_get_account_transfers_unbatched => self.execute_get_account_transfers(
                    message_body_used,
                    self.scan_lookup_buffer[0..result_size],
                    output_buffer,
                ),
                .deprecated_get_account_balances_unbatched => self.execute_get_account_balances(
                    message_body_used,
                    self.scan_lookup_buffer[0..result_size],
                    output_buffer,
                ),
                .deprecated_query_transfers_unbatched => self.execute_query_transfers(
                    message_body_used,
                    self.scan_lookup_buffer[0..result_size],
                    output_buffer,
                ),
                .deprecated_query_accounts_unbatched => self.execute_query_accounts(
                    message_body_used,
                    self.scan_lookup_buffer[0..result_size],
                    output_buffer,
                ),
                else => comptime unreachable,
            };
            maybe(bytes_written == 0);
            return bytes_written;
        }

        fn execute_query_multi_batch(
            self: *StateMachine,
            comptime operation: Operation,
            message_body_used: []align(16) const u8,
            output_buffer: *align(16) [constants.message_body_size_max]u8,
        ) usize {
            comptime assert(operation.is_multi_batch());
            comptime assert(!operation.is_batchable());
            assert(self.scan_lookup_results.items.len > 0);
            maybe(self.scan_lookup_buffer_index == 0);
            defer {
                self.scan_lookup_buffer_index = 0;
                self.scan_lookup_results.clearRetainingCapacity();
            }

            var offset: u32 = 0;
            var body_decoder = MultiBatchDecoder.init(message_body_used, .{
                .element_size = operation.event_size(),
            }) catch unreachable; // Already validated by `input_valid()`.
            assert(body_decoder.batch_count() == self.scan_lookup_results.items.len);
            var reply_encoder = MultiBatchEncoder.init(output_buffer, .{
                .element_size = operation.result_size(),
            });
            for (self.scan_lookup_results.items) |result_count| {
                const batch: []const u8 = body_decoder.pop().?;
                const encoder_output_buffer: []u8 = reply_encoder.writable().?;
                const bytes_written: usize = switch (operation) {
                    .get_account_transfers => size: {
                        const scan_size: u32 = result_count * @sizeOf(Transfer);
                        assert(self.scan_lookup_buffer_index <= self.scan_lookup_buffer.len);
                        assert(self.scan_lookup_buffer_index >= scan_size + offset);
                        defer offset += scan_size;
                        break :size self.execute_get_account_transfers(
                            batch,
                            self.scan_lookup_buffer[offset..][0..scan_size],
                            encoder_output_buffer,
                        );
                    },
                    .get_account_balances => size: {
                        const scan_size: u32 = result_count * @sizeOf(AccountEvent);
                        assert(self.scan_lookup_buffer_index <= self.scan_lookup_buffer.len);
                        assert(self.scan_lookup_buffer_index >= scan_size + offset);
                        defer offset += scan_size;
                        break :size self.execute_get_account_balances(
                            batch,
                            self.scan_lookup_buffer[offset..][0..scan_size],
                            encoder_output_buffer,
                        );
                    },
                    .query_transfers => size: {
                        const scan_size: u32 = result_count * @sizeOf(Transfer);
                        assert(self.scan_lookup_buffer_index <= self.scan_lookup_buffer.len);
                        assert(self.scan_lookup_buffer_index >= scan_size + offset);
                        defer offset += scan_size;
                        break :size self.execute_query_transfers(
                            batch,
                            self.scan_lookup_buffer[offset..][0..scan_size],
                            encoder_output_buffer,
                        );
                    },
                    .query_accounts => size: {
                        const scan_size: u32 = result_count * @sizeOf(Account);
                        assert(self.scan_lookup_buffer_index <= self.scan_lookup_buffer.len);
                        assert(self.scan_lookup_buffer_index >= scan_size + offset);
                        defer offset += scan_size;
                        break :size self.execute_query_accounts(
                            batch,
                            self.scan_lookup_buffer[offset..][0..scan_size],
                            encoder_output_buffer,
                        );
                    },
                    else => comptime unreachable,
                };
                maybe(bytes_written == 0);
                reply_encoder.add(@intCast(bytes_written));
            }
            assert(body_decoder.pop() == null);
            assert(reply_encoder.batch_count == self.scan_lookup_results.items.len);
            assert(offset == self.scan_lookup_buffer_index);

            const encoded_bytes_written: usize = reply_encoder.finish();
            assert(encoded_bytes_written > 0);
            return encoded_bytes_written;
        }

        pub fn compact(
            self: *StateMachine,
            callback: *const fn (*StateMachine) void,
            op: u64,
        ) void {
            assert(self.compact_callback == null);
            assert(self.checkpoint_callback == null);

            self.metrics.timer.reset();

            self.compact_callback = callback;
            self.forest.compact(compact_finish, op);
        }

        fn compact_finish(forest: *Forest) void {
            const self: *StateMachine = @fieldParentPtr("forest", forest);
            const callback = self.compact_callback.?;
            self.compact_callback = null;

            const duration = self.metrics.timer.read();
            self.metrics.record(.compact, duration.to_us(), 1);

            callback(self);
        }

        pub fn checkpoint(self: *StateMachine, callback: *const fn (*StateMachine) void) void {
            assert(self.compact_callback == null);
            assert(self.checkpoint_callback == null);

            self.metrics.timer.reset();

            self.checkpoint_callback = callback;
            self.forest.checkpoint(checkpoint_finish);
        }

        fn checkpoint_finish(forest: *Forest) void {
            const self: *StateMachine = @fieldParentPtr("forest", forest);
            const callback = self.checkpoint_callback.?;
            self.checkpoint_callback = null;

            const duration = self.metrics.timer.read();
            self.metrics.record(.checkpoint, duration.to_us(), 1);

            self.metrics.log_and_reset();

            callback(self);
        }

        fn scope_open(self: *StateMachine, operation: Operation) void {
            switch (operation) {
                .create_accounts,
                .deprecated_create_accounts_unbatched,
                => {
                    self.forest.grooves.accounts.scope_open();
                },
                .create_transfers,
                .deprecated_create_transfers_unbatched,
                => {
                    self.forest.grooves.accounts.scope_open();
                    self.forest.grooves.transfers.scope_open();
                    self.forest.grooves.transfers_pending.scope_open();
                    self.forest.grooves.account_events.scope_open();
                },
                else => unreachable,
            }
        }

        fn scope_close(self: *StateMachine, operation: Operation, mode: ScopeCloseMode) void {
            switch (operation) {
                .create_accounts,
                .deprecated_create_accounts_unbatched,
                => {
                    self.forest.grooves.accounts.scope_close(mode);
                },
                .create_transfers,
                .deprecated_create_transfers_unbatched,
                => {
                    self.forest.grooves.accounts.scope_close(mode);
                    self.forest.grooves.transfers.scope_close(mode);
                    self.forest.grooves.transfers_pending.scope_close(mode);
                    self.forest.grooves.account_events.scope_close(mode);
                },
                else => unreachable,
            }
        }

        fn execute_create(
            self: *StateMachine,
            comptime operation: Operation,
            timestamp: u64,
            batch: []const u8,
            output_buffer: []u8,
        ) usize {
            comptime assert(operation == .create_accounts or
                operation == .create_transfers or
                operation == .deprecated_create_accounts_unbatched or
                operation == .deprecated_create_transfers_unbatched);

            const Event = operation.EventType();
            const Result = operation.ResultType();
            const events = stdx.bytes_as_slice(.exact, Event, batch);
            const results = stdx.bytes_as_slice(.inexact, Result, output_buffer);
            assert(events.len <= results.len);

            var count: usize = 0;
            var chain: ?usize = null;
            var chain_broken = false;

            // The first event determines the batch behavior for
            // importing events with past timestamp.
            const batch_imported = events.len > 0 and events[0].flags.imported;
            for (events, 0..) |*event, index| {
                const result = result: {
                    if (event.flags.linked) {
                        if (chain == null) {
                            chain = index;
                            assert(chain_broken == false);
                            self.scope_open(operation);
                        }

                        if (index == events.len - 1) break :result .linked_event_chain_open;
                    }

                    if (chain_broken) break :result .linked_event_failed;

                    if (batch_imported != event.flags.imported) {
                        if (event.flags.imported) {
                            break :result .imported_event_not_expected;
                        } else {
                            break :result .imported_event_expected;
                        }
                    }

                    const timestamp_event = timestamp: {
                        if (event.flags.imported) {
                            if (!TimestampRange.valid(event.timestamp)) {
                                break :result .imported_event_timestamp_out_of_range;
                            }
                            if (event.timestamp >= timestamp) {
                                break :result .imported_event_timestamp_must_not_advance;
                            }
                            break :timestamp event.timestamp;
                        }
                        if (event.timestamp != 0) break :result .timestamp_must_be_zero;
                        break :timestamp timestamp - events.len + index + 1;
                    };
                    assert(TimestampRange.valid(timestamp_event));

                    break :result switch (operation) {
                        .deprecated_create_accounts_unbatched,
                        .create_accounts,
                        => self.create_account(timestamp_event, event),
                        .deprecated_create_transfers_unbatched,
                        .create_transfers,
                        => self.create_transfer(timestamp_event, event),
                        else => comptime unreachable,
                    };
                };
                if (self.log_trace) {
                    log.debug("{?}: {s} {}/{}: {}: {}", .{
                        self.forest.grid.superblock.replica_index,
                        @tagName(operation),
                        index + 1,
                        events.len,
                        result,
                        event,
                    });
                }
                if (result != .ok) {
                    if (chain) |chain_start_index| {
                        if (!chain_broken) {
                            chain_broken = true;
                            // Our chain has just been broken, discard the scope we started above.
                            self.scope_close(operation, .discard);

                            // Add errors for rolled back events in FIFO order:
                            var chain_index = chain_start_index;
                            while (chain_index < index) : (chain_index += 1) {
                                results[count] = .{
                                    .index = @intCast(chain_index),
                                    .result = .linked_event_failed,
                                };
                                count += 1;
                            }
                        } else {
                            assert(result == .linked_event_failed or
                                result == .linked_event_chain_open);
                        }
                    }
                    results[count] = .{ .index = @intCast(index), .result = result };
                    count += 1;

                    self.transient_error(operation, event.id, result);
                }
                if (chain != null and (!event.flags.linked or result == .linked_event_chain_open)) {
                    if (!chain_broken) {
                        // We've finished this linked chain, and all events have applied
                        // successfully.
                        self.scope_close(operation, .persist);
                    }

                    chain = null;
                    chain_broken = false;
                }
            }
            assert(chain == null);
            assert(chain_broken == false);

            return @sizeOf(Result) * count;
        }

        fn transient_error(
            self: *StateMachine,
            comptime operation: Operation,
            id: u128,
            result: anytype,
        ) void {
            assert(result != .ok);

            switch (operation) {
                .create_accounts,
                .deprecated_create_accounts_unbatched,
                => {
                    comptime assert(@TypeOf(result) == CreateAccountResult);
                    // The `create_accounts` error codes do not depend on transient system status.
                    return;
                },
                .create_transfers,
                .deprecated_create_transfers_unbatched,
                => {
                    comptime assert(@TypeOf(result) == CreateTransferResult);

                    // Transfers that fail with transient codes cannot reuse the same `id`,
                    // ensuring strong idempotency guarantees.
                    // Once a transfer fails with a transient error, it must be retried
                    // with a different `id`.
                    if (result.transient()) {
                        self.forest.grooves.transfers.insert_orphaned_id(id);
                    }
                },
                else => comptime unreachable,
            }
        }

        // Accounts that do not fit in the response are omitted.
        fn execute_lookup_accounts(
            self: *StateMachine,
            batch: []const u8,
            output_buffer: []u8,
        ) usize {
            const events = stdx.bytes_as_slice(.exact, u128, batch);
            const results = stdx.bytes_as_slice(.inexact, Account, output_buffer);
            assert(events.len <= results.len);

            var results_count: usize = 0;
            for (events) |id| {
                if (self.get_account(id)) |account| {
                    results[results_count] = account;
                    results_count += 1;
                }
            }
            return results_count * @sizeOf(Account);
        }

        // Transfers that do not fit in the response are omitted.
        fn execute_lookup_transfers(
            self: *StateMachine,
            batch: []const u8,
            output_buffer: []u8,
        ) usize {
            const events = stdx.bytes_as_slice(.exact, u128, batch);
            const results = stdx.bytes_as_slice(.inexact, Transfer, output_buffer);
            assert(events.len <= results.len);

            var results_count: usize = 0;
            for (events) |id| {
                if (self.get_transfer(id)) |result| {
                    results[results_count] = result;
                    results_count += 1;
                }
            }
            return results_count * @sizeOf(Transfer);
        }

        fn execute_get_account_transfers(
            self: *StateMachine,
            batch: []const u8,
            scan_buffer: []const u8,
            output_buffer: []u8,
        ) usize {
            _ = self;
            _ = batch;
            assert(scan_buffer.len <= output_buffer.len);
            stdx.copy_disjoint(
                .inexact,
                u8,
                output_buffer,
                scan_buffer,
            );
            return scan_buffer.len;
        }

        fn execute_get_account_balances(
            self: *StateMachine,
            batch: []const u8,
            scan_buffer: []const u8,
            output_buffer: []u8,
        ) usize {
            _ = self;
            const scan_count = @divExact(scan_buffer.len, @sizeOf(AccountEvent));
            const output_count_max = @divFloor(output_buffer.len, @sizeOf(AccountBalance));
            assert(scan_count <= output_count_max);

            const filter: *const AccountFilter = @alignCast(std.mem.bytesAsValue(
                AccountFilter,
                batch,
            ));

            const scan_results = stdx.bytes_as_slice(.exact, AccountEvent, scan_buffer);
            const output_slice = stdx.bytes_as_slice(.inexact, AccountBalance, output_buffer);
            var output_count: u32 = 0;

            for (scan_results) |*result| {
                assert(result.dr_account_id != result.cr_account_id);

                output_slice[output_count] = if (filter.account_id == result.dr_account_id) .{
                    .timestamp = result.timestamp,
                    .debits_pending = result.dr_debits_pending,
                    .debits_posted = result.dr_debits_posted,
                    .credits_pending = result.dr_credits_pending,
                    .credits_posted = result.dr_credits_posted,
                } else if (filter.account_id == result.cr_account_id) .{
                    .timestamp = result.timestamp,
                    .debits_pending = result.cr_debits_pending,
                    .debits_posted = result.cr_debits_posted,
                    .credits_pending = result.cr_credits_pending,
                    .credits_posted = result.cr_credits_posted,
                } else {
                    // We have checked that this account has `flags.history == true`.
                    unreachable;
                };

                output_count += 1;
            }

            assert(output_count == scan_results.len);
            return output_count * @sizeOf(AccountBalance);
        }

        fn execute_query_accounts(
            self: *StateMachine,
            batch: []const u8,
            scan_buffer: []const u8,
            output_buffer: []u8,
        ) usize {
            _ = self;
            _ = batch;
            assert(scan_buffer.len <= output_buffer.len);
            stdx.copy_disjoint(
                .inexact,
                u8,
                output_buffer,
                scan_buffer,
            );
            return scan_buffer.len;
        }

        fn execute_query_transfers(
            self: *StateMachine,
            batch: []const u8,
            scan_buffer: []const u8,
            output_buffer: []u8,
        ) usize {
            _ = self;
            _ = batch;
            assert(scan_buffer.len <= output_buffer.len);
            stdx.copy_disjoint(
                .inexact,
                u8,
                output_buffer,
                scan_buffer,
            );
            return scan_buffer.len;
        }

        fn execute_get_change_events(
            self: *StateMachine,
            batch: []const u8,
            scan_buffer: []const u8,
            output_buffer: []u8,
        ) usize {
            _ = batch;

            const scan_results: []const AccountEvent = stdx.bytes_as_slice(
                .exact,
                AccountEvent,
                scan_buffer,
            );
            const output_slice = stdx.bytes_as_slice(.inexact, ChangeEvent, output_buffer);
            var output_count: u32 = 0;

            for (scan_results) |*result| {
                assert(TimestampRange.valid(result.timestamp));
                assert(result.dr_account_id != result.cr_account_id);
                output_slice[output_count] = switch (result.schema()) {
                    .current => self.get_change_event(result),
                    .former => |former| self.get_change_event_former(former),
                };
                output_count += 1;
            }

            return output_count * @sizeOf(ChangeEvent);
        }

        fn get_change_event(
            self: *StateMachine,
            result: *const AccountEvent,
        ) ChangeEvent {
            // Getting the transfer by `timestamp`,
            // except for expiries where there is no transfer associated with the timestamp.
            const transfer: Transfer = switch (result.transfer_pending_status) {
                .none,
                .pending,
                .posted,
                .voided,
                => switch (self.forest.grooves.transfers.get_by_timestamp(result.timestamp)) {
                    .found_object => |transfer| transfer,
                    .found_orphaned_id, .not_found => unreachable,
                },
                .expired => self.get_transfer(result.transfer_pending_id).?,
            };
            const dr_account = self.get_account(result.dr_account_id).?;
            const cr_account = self.get_account(result.cr_account_id).?;
            assert(transfer.debit_account_id == dr_account.id);
            assert(transfer.credit_account_id == cr_account.id);
            assert(transfer.ledger == result.ledger);
            assert(dr_account.ledger == result.ledger);
            assert(cr_account.ledger == result.ledger);

            const event_type: ChangeEventType = event_type: {
                switch (result.transfer_pending_status) {
                    .none => {
                        assert(transfer.timestamp == result.timestamp);
                        assert(!transfer.flags.pending);
                        assert(!transfer.flags.post_pending_transfer);
                        assert(!transfer.flags.void_pending_transfer);
                        assert(transfer.pending_id == 0);
                        break :event_type .single_phase;
                    },
                    .pending => {
                        assert(transfer.timestamp == result.timestamp);
                        assert(transfer.flags.pending);
                        assert(transfer.pending_id == 0);
                        break :event_type .two_phase_pending;
                    },
                    .posted => {
                        assert(transfer.timestamp == result.timestamp);
                        assert(transfer.flags.post_pending_transfer);
                        assert(transfer.pending_id == result.transfer_pending_id);
                        break :event_type .two_phase_posted;
                    },
                    .voided => {
                        assert(transfer.timestamp == result.timestamp);
                        assert(transfer.flags.void_pending_transfer);
                        assert(transfer.pending_id == result.transfer_pending_id);
                        break :event_type .two_phase_voided;
                    },
                    .expired => {
                        assert(transfer.flags.pending);
                        assert(transfer.id == result.transfer_pending_id);
                        assert(transfer.timeout > 0);
                        assert(transfer.timestamp < result.timestamp);
                        break :event_type .two_phase_expired;
                    },
                }
            };

            return .{
                .transfer_id = transfer.id,
                .transfer_amount = result.amount,
                .transfer_pending_id = transfer.pending_id,
                .transfer_user_data_128 = transfer.user_data_128,
                .transfer_user_data_64 = transfer.user_data_64,
                .transfer_user_data_32 = transfer.user_data_32,
                .transfer_timeout = transfer.timeout,

                .ledger = result.ledger,
                .transfer_code = transfer.code,
                .transfer_flags = transfer.flags,
                .type = event_type,

                .debit_account_id = dr_account.id,
                .debit_account_debits_pending = result.dr_debits_pending,
                .debit_account_debits_posted = result.dr_debits_posted,
                .debit_account_credits_pending = result.dr_credits_pending,
                .debit_account_credits_posted = result.dr_credits_posted,
                .debit_account_user_data_128 = dr_account.user_data_128,
                .debit_account_user_data_64 = dr_account.user_data_64,
                .debit_account_user_data_32 = dr_account.user_data_32,
                .debit_account_code = dr_account.code,
                .debit_account_flags = result.dr_account_flags,

                .credit_account_id = cr_account.id,
                .credit_account_debits_pending = result.cr_debits_pending,
                .credit_account_debits_posted = result.cr_debits_posted,
                .credit_account_credits_pending = result.cr_credits_pending,
                .credit_account_credits_posted = result.cr_credits_posted,
                .credit_account_user_data_128 = cr_account.user_data_128,
                .credit_account_user_data_64 = cr_account.user_data_64,
                .credit_account_user_data_32 = cr_account.user_data_32,
                .credit_account_code = cr_account.code,
                .credit_account_flags = result.cr_account_flags,

                .timestamp = result.timestamp,
                .transfer_timestamp = transfer.timestamp,
                .debit_account_timestamp = dr_account.timestamp,
                .credit_account_timestamp = cr_account.timestamp,
            };
        }

        fn get_change_event_former(
            self: *StateMachine,
            result: *const AccountEvent.Former,
        ) ChangeEvent {
            assert(result.dr_account_id != 0);
            assert(result.cr_account_id != 0);
            const transfer: Transfer =
                switch (self.forest.grooves.transfers.get_by_timestamp(result.timestamp)) {
                    .found_object => |transfer| transfer,
                    .found_orphaned_id, .not_found => unreachable,
                };
            const dr_account = self.get_account(result.dr_account_id).?;
            const cr_account = self.get_account(result.cr_account_id).?;
            assert(transfer.debit_account_id == dr_account.id);
            assert(transfer.credit_account_id == cr_account.id);
            assert(transfer.ledger == dr_account.ledger);
            assert(transfer.ledger == cr_account.ledger);

            const event_type: ChangeEventType = event_type: {
                if (transfer.flags.pending) break :event_type .two_phase_pending;
                if (transfer.flags.post_pending_transfer) break :event_type .two_phase_posted;
                if (transfer.flags.void_pending_transfer) break :event_type .two_phase_voided;
                break :event_type .single_phase;
            };

            return .{
                .transfer_id = transfer.id,
                .transfer_amount = transfer.amount,
                .transfer_pending_id = transfer.pending_id,
                .transfer_user_data_128 = transfer.user_data_128,
                .transfer_user_data_64 = transfer.user_data_64,
                .transfer_user_data_32 = transfer.user_data_32,
                .transfer_timeout = transfer.timeout,

                .ledger = transfer.ledger,
                .transfer_code = transfer.code,

                .type = event_type,

                .debit_account_id = dr_account.id,
                .debit_account_debits_pending = result.dr_debits_pending,
                .debit_account_debits_posted = result.dr_debits_posted,
                .debit_account_credits_pending = result.dr_credits_pending,
                .debit_account_credits_posted = result.dr_credits_posted,
                .debit_account_user_data_128 = dr_account.user_data_128,
                .debit_account_user_data_64 = dr_account.user_data_64,
                .debit_account_user_data_32 = dr_account.user_data_32,
                .debit_account_code = dr_account.code,

                .credit_account_id = cr_account.id,
                .credit_account_debits_pending = result.cr_debits_pending,
                .credit_account_debits_posted = result.cr_debits_posted,
                .credit_account_credits_pending = result.cr_credits_pending,
                .credit_account_credits_posted = result.cr_credits_posted,
                .credit_account_user_data_128 = cr_account.user_data_128,
                .credit_account_user_data_64 = cr_account.user_data_64,
                .credit_account_user_data_32 = cr_account.user_data_32,
                .credit_account_code = cr_account.code,

                // Not present in the former schema, returning the most current flags.
                .transfer_flags = transfer.flags,
                .debit_account_flags = dr_account.flags,
                .credit_account_flags = cr_account.flags,

                .timestamp = result.timestamp,
                .transfer_timestamp = transfer.timestamp,
                .debit_account_timestamp = dr_account.timestamp,
                .credit_account_timestamp = cr_account.timestamp,
            };
        }

        fn create_account(
            self: *StateMachine,
            timestamp: u64,
            a: *const Account,
        ) CreateAccountResult {
            assert(timestamp > self.commit_timestamp or
                a.flags.imported or
                constants.aof_recovery);
            if (a.flags.imported) {
                assert(a.timestamp == timestamp);
            } else {
                assert(a.timestamp == 0);
            }

            if (a.reserved != 0) return .reserved_field;
            if (a.flags.padding != 0) return .reserved_flag;

            if (a.id == 0) return .id_must_not_be_zero;
            if (a.id == math.maxInt(u128)) return .id_must_not_be_int_max;

            switch (self.forest.grooves.accounts.get(a.id)) {
                .found_object => |e| return create_account_exists(a, &e),
                .found_orphaned_id => unreachable,
                .not_found => {},
            }

            if (a.flags.debits_must_not_exceed_credits and a.flags.credits_must_not_exceed_debits) {
                return .flags_are_mutually_exclusive;
            }

            if (a.debits_pending != 0) return .debits_pending_must_be_zero;
            if (a.debits_posted != 0) return .debits_posted_must_be_zero;
            if (a.credits_pending != 0) return .credits_pending_must_be_zero;
            if (a.credits_posted != 0) return .credits_posted_must_be_zero;
            if (a.ledger == 0) return .ledger_must_not_be_zero;
            if (a.code == 0) return .code_must_not_be_zero;

            if (a.flags.imported) {
                // Allows past timestamp, but validates whether it regressed from the last
                // inserted account.
                // This validation must be called _after_ the idempotency checks so the user
                // can still handle `exists` results when importing.
                if (self.forest.grooves.accounts.objects.key_range) |*key_range| {
                    if (timestamp <= key_range.key_max) {
                        return .imported_event_timestamp_must_not_regress;
                    }
                }
                if (self.forest.grooves.transfers.exists(timestamp)) {
                    return .imported_event_timestamp_must_not_regress;
                }
            }

            self.forest.grooves.accounts.insert(&.{
                .id = a.id,
                .debits_pending = 0,
                .debits_posted = 0,
                .credits_pending = 0,
                .credits_posted = 0,
                .user_data_128 = a.user_data_128,
                .user_data_64 = a.user_data_64,
                .user_data_32 = a.user_data_32,
                .reserved = 0,
                .ledger = a.ledger,
                .code = a.code,
                .flags = a.flags,
                .timestamp = timestamp,
            });
            self.commit_timestamp = timestamp;
            return .ok;
        }

        fn create_account_exists(a: *const Account, e: *const Account) CreateAccountResult {
            assert(a.id == e.id);
            if (@as(u16, @bitCast(a.flags)) != @as(u16, @bitCast(e.flags))) {
                return .exists_with_different_flags;
            }
            if (a.user_data_128 != e.user_data_128) return .exists_with_different_user_data_128;
            if (a.user_data_64 != e.user_data_64) return .exists_with_different_user_data_64;
            if (a.user_data_32 != e.user_data_32) return .exists_with_different_user_data_32;
            assert(a.reserved == 0 and e.reserved == 0);
            if (a.ledger != e.ledger) return .exists_with_different_ledger;
            if (a.code != e.code) return .exists_with_different_code;
            return .exists;
        }

        fn create_transfer(
            self: *StateMachine,
            timestamp: u64,
            t: *const Transfer,
        ) CreateTransferResult {
            assert(timestamp > self.commit_timestamp or
                t.flags.imported or
                constants.aof_recovery);
            if (t.flags.imported) {
                assert(t.timestamp == timestamp);
            } else {
                assert(t.timestamp == 0);
            }

            if (t.flags.padding != 0) return .reserved_flag;

            if (t.id == 0) return .id_must_not_be_zero;
            if (t.id == math.maxInt(u128)) return .id_must_not_be_int_max;

            switch (self.forest.grooves.transfers.get(t.id)) {
                .found_object => |*e| return self.create_transfer_exists(t, e),
                .found_orphaned_id => return .id_already_failed,
                .not_found => {},
            }

            if (t.flags.post_pending_transfer or t.flags.void_pending_transfer) {
                return self.post_or_void_pending_transfer(timestamp, t);
            }

            if (t.debit_account_id == 0) return .debit_account_id_must_not_be_zero;
            if (t.debit_account_id == math.maxInt(u128)) {
                return .debit_account_id_must_not_be_int_max;
            }
            if (t.credit_account_id == 0) return .credit_account_id_must_not_be_zero;
            if (t.credit_account_id == math.maxInt(u128)) {
                return .credit_account_id_must_not_be_int_max;
            }
            if (t.credit_account_id == t.debit_account_id) return .accounts_must_be_different;

            if (t.pending_id != 0) return .pending_id_must_be_zero;
            if (!t.flags.pending) {
                if (t.timeout != 0) return .timeout_reserved_for_pending_transfer;
                if (t.flags.closing_debit or t.flags.closing_credit) {
                    return .closing_transfer_must_be_pending;
                }
            }

            if (t.ledger == 0) return .ledger_must_not_be_zero;
            if (t.code == 0) return .code_must_not_be_zero;

            // The etymology of the DR and CR abbreviations for debit/credit is interesting, either:
            // 1. derived from the Latin past participles of debitum/creditum, i.e. debere/credere,
            // 2. standing for debit record and credit record, or
            // 3. relating to debtor and creditor.
            // We use them to distinguish between `cr` (credit account), and `c` (commit).
            const dr_account = self.get_account(t.debit_account_id) orelse
                return .debit_account_not_found;
            const cr_account = self.get_account(t.credit_account_id) orelse
                return .credit_account_not_found;
            assert(dr_account.id == t.debit_account_id);
            assert(cr_account.id == t.credit_account_id);

            if (dr_account.ledger != cr_account.ledger) return .accounts_must_have_the_same_ledger;
            if (t.ledger != dr_account.ledger) {
                return .transfer_must_have_the_same_ledger_as_accounts;
            }

            if (t.flags.imported) {
                // Allows past timestamp, but validates whether it regressed from the last
                // inserted event.
                // This validation must be called _after_ the idempotency checks so the user
                // can still handle `exists` results when importing.
                if (self.forest.grooves.transfers.objects.key_range) |*key_range| {
                    if (timestamp <= key_range.key_max) {
                        return .imported_event_timestamp_must_not_regress;
                    }
                }
                if (self.forest.grooves.accounts.exists(timestamp)) {
                    return .imported_event_timestamp_must_not_regress;
                }

                if (timestamp <= dr_account.timestamp) {
                    return .imported_event_timestamp_must_postdate_debit_account;
                }
                if (timestamp <= cr_account.timestamp) {
                    return .imported_event_timestamp_must_postdate_credit_account;
                }
                if (t.timeout != 0) {
                    assert(t.flags.pending);
                    return .imported_event_timeout_must_be_zero;
                }
            }
            assert(timestamp > dr_account.timestamp);
            assert(timestamp > cr_account.timestamp);

            if (dr_account.flags.closed) return .debit_account_already_closed;
            if (cr_account.flags.closed) return .credit_account_already_closed;

            maybe(t.amount == 0);
            const amount_actual = amount: {
                var amount = t.amount;
                if (t.flags.balancing_debit) {
                    const dr_balance = dr_account.debits_posted + dr_account.debits_pending;
                    amount = @min(amount, dr_account.credits_posted -| dr_balance);
                }

                if (t.flags.balancing_credit) {
                    const cr_balance = cr_account.credits_posted + cr_account.credits_pending;
                    amount = @min(amount, cr_account.debits_posted -| cr_balance);
                }
                break :amount amount;
            };
            maybe(amount_actual == 0);

            if (t.flags.pending) {
                if (sum_overflows(u128, amount_actual, dr_account.debits_pending)) {
                    return .overflows_debits_pending;
                }
                if (sum_overflows(u128, amount_actual, cr_account.credits_pending)) {
                    return .overflows_credits_pending;
                }
            }
            if (sum_overflows(u128, amount_actual, dr_account.debits_posted)) {
                return .overflows_debits_posted;
            }
            if (sum_overflows(u128, amount_actual, cr_account.credits_posted)) {
                return .overflows_credits_posted;
            }
            // We assert that the sum of the pending and posted balances can never overflow:
            if (sum_overflows(
                u128,
                amount_actual,
                dr_account.debits_pending + dr_account.debits_posted,
            )) {
                return .overflows_debits;
            }
            if (sum_overflows(
                u128,
                amount_actual,
                cr_account.credits_pending + cr_account.credits_posted,
            )) {
                return .overflows_credits;
            }

            // Comptime asserts that the max value of the timeout expressed in seconds cannot
            // overflow a `u63` when converted to nanoseconds.
            // It is `u63` because the most significant bit of the `u64` timestamp
            // is used as the tombstone flag.
            comptime assert(!std.meta.isError(std.math.mul(
                u63,
                @as(u63, std.math.maxInt(@TypeOf(t.timeout))),
                std.time.ns_per_s,
            )));
            if (sum_overflows(
                u63,
                @intCast(timestamp),
                @as(u63, t.timeout) * std.time.ns_per_s,
            )) {
                return .overflows_timeout;
            }

            if (dr_account.debits_exceed_credits(amount_actual)) return .exceeds_credits;
            if (cr_account.credits_exceed_debits(amount_actual)) return .exceeds_debits;

            // After this point, the transfer must succeed.
            defer assert(self.commit_timestamp == timestamp);

            self.forest.grooves.transfers.insert(&.{
                .id = t.id,
                .debit_account_id = t.debit_account_id,
                .credit_account_id = t.credit_account_id,
                .amount = amount_actual,
                .pending_id = t.pending_id,
                .user_data_128 = t.user_data_128,
                .user_data_64 = t.user_data_64,
                .user_data_32 = t.user_data_32,
                .timeout = t.timeout,
                .ledger = t.ledger,
                .code = t.code,
                .flags = t.flags,
                .timestamp = timestamp,
            });

            var dr_account_new = dr_account;
            var cr_account_new = cr_account;
            if (t.flags.pending) {
                dr_account_new.debits_pending += amount_actual;
                cr_account_new.credits_pending += amount_actual;

                self.forest.grooves.transfers_pending.insert(&.{
                    .timestamp = timestamp,
                    .status = .pending,
                });
            } else {
                dr_account_new.debits_posted += amount_actual;
                cr_account_new.credits_posted += amount_actual;
            }

            // Closing accounts:
            assert(!dr_account_new.flags.closed);
            assert(!cr_account_new.flags.closed);
            if (t.flags.closing_debit) dr_account_new.flags.closed = true;
            if (t.flags.closing_credit) cr_account_new.flags.closed = true;

            const dr_updated = amount_actual > 0 or dr_account_new.flags.closed;
            assert(dr_updated == !stdx.equal_bytes(Account, &dr_account, &dr_account_new));
            if (dr_updated) {
                self.forest.grooves.accounts.update(.{
                    .old = &dr_account,
                    .new = &dr_account_new,
                });
            }

            const cr_updated = amount_actual > 0 or cr_account_new.flags.closed;
            assert(cr_updated == !stdx.equal_bytes(Account, &cr_account, &cr_account_new));
            if (cr_updated) {
                self.forest.grooves.accounts.update(.{
                    .old = &cr_account,
                    .new = &cr_account_new,
                });
            }

            self.account_event(.{
                .event_timestamp = timestamp,
                .dr_account = &dr_account_new,
                .cr_account = &cr_account_new,
                .transfer_flags = t.flags,
                .transfer_pending_status = if (t.flags.pending) .pending else .none,
                .transfer_pending = null,
                .amount_requested = t.amount,
                .amount = amount_actual,
            });

            if (t.timeout > 0) {
                assert(t.flags.pending);
                assert(!t.flags.imported);
                const expires_at = timestamp + t.timeout_ns();
                if (expires_at < self.expire_pending_transfers.pulse_next_timestamp) {
                    self.expire_pending_transfers.pulse_next_timestamp = expires_at;
                }
            }

            self.commit_timestamp = timestamp;
            return .ok;
        }

        fn create_transfer_exists(
            self: *const StateMachine,
            t: *const Transfer,
            e: *const Transfer,
        ) CreateTransferResult {
            assert(t.id == e.id);
            // The flags change the behavior of the remaining comparisons,
            // so compare the flags first.
            if (@as(u16, @bitCast(t.flags)) != @as(u16, @bitCast(e.flags))) {
                return .exists_with_different_flags;
            }

            if (t.pending_id != e.pending_id) return .exists_with_different_pending_id;
            if (t.timeout != e.timeout) return .exists_with_different_timeout;

            if (t.flags.post_pending_transfer or t.flags.void_pending_transfer) {
                // Since both `t` and `e` have the same `pending_id`,
                // it must be a valid transfer.
                const p = self.get_transfer(t.pending_id).?;
                return post_or_void_pending_transfer_exists(t, e, &p);
            } else {
                if (t.debit_account_id != e.debit_account_id) {
                    return .exists_with_different_debit_account_id;
                }
                if (t.credit_account_id != e.credit_account_id) {
                    return .exists_with_different_credit_account_id;
                }

                // In transfers with `flags.balancing_debit` or `flags.balancing_credit` set,
                // the field `amount` means the _upper limit_ (or zero for `maxInt`) that can be
                // moved in order to balance debits and credits.
                // The actual amount moved depends on the account's balance at the time the
                // transfer was executed.
                //
                // This is a special case in the idempotency check:
                // When _resubmitting_ the same balancing transfer, the amount will likely be
                // different from what was previously committed, but as long as it is within the
                // range of possible values it should fail with `exists` rather than
                // `exists_with_different_amount`.
                if (t.flags.balancing_debit or t.flags.balancing_credit) {
                    if (t.amount < e.amount) return .exists_with_different_amount;
                } else {
                    if (t.amount != e.amount) return .exists_with_different_amount;
                }

                if (t.user_data_128 != e.user_data_128) {
                    return .exists_with_different_user_data_128;
                }
                if (t.user_data_64 != e.user_data_64) {
                    return .exists_with_different_user_data_64;
                }
                if (t.user_data_32 != e.user_data_32) {
                    return .exists_with_different_user_data_32;
                }
                if (t.ledger != e.ledger) {
                    return .exists_with_different_ledger;
                }
                if (t.code != e.code) {
                    return .exists_with_different_code;
                }

                return .exists;
            }
        }

        fn post_or_void_pending_transfer(
            self: *StateMachine,
            timestamp: u64,
            t: *const Transfer,
        ) CreateTransferResult {
            assert(t.id != 0);
            assert(t.id != std.math.maxInt(u128));
            assert(self.forest.grooves.transfers.get(t.id) == .not_found);
            assert(t.flags.padding == 0);
            assert(timestamp > self.commit_timestamp or t.flags.imported);
            if (t.flags.imported) {
                assert(t.timestamp == timestamp);
            } else {
                assert(t.timestamp == 0);
            }
            assert(t.flags.post_pending_transfer or t.flags.void_pending_transfer);

            if (t.flags.post_pending_transfer and t.flags.void_pending_transfer) {
                return .flags_are_mutually_exclusive;
            }
            if (t.flags.pending) return .flags_are_mutually_exclusive;
            if (t.flags.balancing_debit) return .flags_are_mutually_exclusive;
            if (t.flags.balancing_credit) return .flags_are_mutually_exclusive;
            if (t.flags.closing_debit) return .flags_are_mutually_exclusive;
            if (t.flags.closing_credit) return .flags_are_mutually_exclusive;

            if (t.pending_id == 0) return .pending_id_must_not_be_zero;
            if (t.pending_id == math.maxInt(u128)) return .pending_id_must_not_be_int_max;
            if (t.pending_id == t.id) return .pending_id_must_be_different;
            if (t.timeout != 0) return .timeout_reserved_for_pending_transfer;

            const p = self.get_transfer(t.pending_id) orelse return .pending_transfer_not_found;
            assert(p.id == t.pending_id);
            assert(p.timestamp < timestamp);
            if (!p.flags.pending) return .pending_transfer_not_pending;

            const dr_account = self.get_account(p.debit_account_id).?;
            const cr_account = self.get_account(p.credit_account_id).?;
            assert(dr_account.id == p.debit_account_id);
            assert(cr_account.id == p.credit_account_id);
            assert(p.timestamp > dr_account.timestamp);
            assert(p.timestamp > cr_account.timestamp);

            if (t.debit_account_id > 0 and t.debit_account_id != p.debit_account_id) {
                return .pending_transfer_has_different_debit_account_id;
            }
            if (t.credit_account_id > 0 and t.credit_account_id != p.credit_account_id) {
                return .pending_transfer_has_different_credit_account_id;
            }
            // The user_data field is allowed to differ across pending and posting/voiding
            // transfers.
            if (t.ledger > 0 and t.ledger != p.ledger) {
                return .pending_transfer_has_different_ledger;
            }
            if (t.code > 0 and t.code != p.code) return .pending_transfer_has_different_code;

            maybe(t.amount == 0);
            maybe(p.amount == 0);
            const amount_actual = amount: {
                if (t.flags.void_pending_transfer) {
                    break :amount if (t.amount == 0) p.amount else t.amount;
                } else {
                    break :amount if (t.amount == std.math.maxInt(u128)) p.amount else t.amount;
                }
            };
            maybe(amount_actual == 0);

            if (amount_actual > p.amount) return .exceeds_pending_transfer_amount;

            if (t.flags.void_pending_transfer and amount_actual < p.amount) {
                return .pending_transfer_has_different_amount;
            }

            const transfer_pending = self.get_transfer_pending(p.timestamp).?;
            assert(p.timestamp == transfer_pending.timestamp);
            switch (transfer_pending.status) {
                .none => unreachable,
                .pending => {},
                .posted => return .pending_transfer_already_posted,
                .voided => return .pending_transfer_already_voided,
                .expired => {
                    assert(p.timeout > 0);
                    assert(!p.flags.imported);
                    assert(timestamp >= p.timestamp + p.timeout_ns());
                    return .pending_transfer_expired;
                },
            }

            const expires_at: ?u64 = if (p.timeout == 0) null else expires_at: {
                assert(!p.flags.imported);
                const expires_at: u64 = p.timestamp + p.timeout_ns();
                if (expires_at <= timestamp) {
                    // TODO: It's still possible for an operation to see an expired transfer
                    // if there's more than one batch of transfers to expire in a single `pulse`
                    // and the current operation was pipelined before the expiration commits.
                    return .pending_transfer_expired;
                }

                break :expires_at expires_at;
            };

            if (t.flags.imported) {
                // Allows past timestamp, but validates whether it regressed from the last
                // inserted transfer.
                // This validation must be called _after_ the idempotency checks so the user
                // can still handle `exists` results when importing.
                if (self.forest.grooves.transfers.objects.key_range) |*key_range| {
                    if (timestamp <= key_range.key_max) {
                        return .imported_event_timestamp_must_not_regress;
                    }
                }
                if (self.forest.grooves.accounts.exists(timestamp)) {
                    return .imported_event_timestamp_must_not_regress;
                }
            }
            assert(timestamp > dr_account.timestamp);
            assert(timestamp > cr_account.timestamp);

            // The only movement allowed in a closed account is voiding a pending transfer.
            if (dr_account.flags.closed and !t.flags.void_pending_transfer) {
                return .debit_account_already_closed;
            }
            if (cr_account.flags.closed and !t.flags.void_pending_transfer) {
                return .credit_account_already_closed;
            }

            // After this point, the transfer must succeed.
            defer assert(self.commit_timestamp == timestamp);

            self.forest.grooves.transfers.insert(&.{
                .id = t.id,
                .debit_account_id = p.debit_account_id,
                .credit_account_id = p.credit_account_id,
                .user_data_128 = if (t.user_data_128 > 0) t.user_data_128 else p.user_data_128,
                .user_data_64 = if (t.user_data_64 > 0) t.user_data_64 else p.user_data_64,
                .user_data_32 = if (t.user_data_32 > 0) t.user_data_32 else p.user_data_32,
                .ledger = p.ledger,
                .code = p.code,
                .pending_id = t.pending_id,
                .timeout = 0,
                .timestamp = timestamp,
                .flags = t.flags,
                .amount = amount_actual,
            });

            if (expires_at) |expiry| {
                assert(expiry > timestamp);
                // Removing the pending `expires_at` index.
                self.forest.grooves.transfers.indexes.expires_at.remove(&.{
                    .field = expiry,
                    .timestamp = p.timestamp,
                });

                // In case the pending transfer's timeout is exactly the one we are using
                // as flag, we need to zero the value to run the next `pulse`.
                if (self.expire_pending_transfers.pulse_next_timestamp == expiry) {
                    self.expire_pending_transfers.pulse_next_timestamp =
                        TimestampRange.timestamp_min;
                }
            }

            const transfer_pending_status: TransferPendingStatus = status: {
                if (t.flags.post_pending_transfer) break :status .posted;
                if (t.flags.void_pending_transfer) break :status .voided;
                unreachable;
            };
            self.transfer_update_pending_status(&transfer_pending, transfer_pending_status);

            var dr_account_new = dr_account;
            var cr_account_new = cr_account;
            dr_account_new.debits_pending -= p.amount;
            cr_account_new.credits_pending -= p.amount;

            if (t.flags.post_pending_transfer) {
                assert(!p.flags.closing_debit);
                assert(!p.flags.closing_credit);
                assert(amount_actual <= p.amount);
                dr_account_new.debits_posted += amount_actual;
                cr_account_new.credits_posted += amount_actual;
            }
            if (t.flags.void_pending_transfer) {
                assert(amount_actual == p.amount);
                // Reverts the closing account operation:
                if (p.flags.closing_debit) {
                    assert(dr_account.flags.closed);
                    dr_account_new.flags.closed = false;
                }
                if (p.flags.closing_credit) {
                    assert(cr_account.flags.closed);
                    cr_account_new.flags.closed = false;
                }
            }

            const dr_updated = amount_actual > 0 or p.amount > 0 or
                dr_account_new.flags.closed != dr_account.flags.closed;
            assert(dr_updated == !stdx.equal_bytes(Account, &dr_account, &dr_account_new));
            if (dr_updated) {
                self.forest.grooves.accounts.update(.{
                    .old = &dr_account,
                    .new = &dr_account_new,
                });
            }

            const cr_updated = amount_actual > 0 or p.amount > 0 or
                cr_account_new.flags.closed != cr_account.flags.closed;
            assert(cr_updated == !stdx.equal_bytes(Account, &cr_account, &cr_account_new));
            if (cr_updated) {
                self.forest.grooves.accounts.update(.{
                    .old = &cr_account,
                    .new = &cr_account_new,
                });
            }

            self.account_event(.{
                .event_timestamp = timestamp,
                .dr_account = &dr_account_new,
                .cr_account = &cr_account_new,
                .transfer_flags = t.flags,
                .transfer_pending_status = transfer_pending_status,
                .transfer_pending = &p,
                .amount_requested = t.amount,
                .amount = amount_actual,
            });

            self.commit_timestamp = timestamp;

            return .ok;
        }

        fn post_or_void_pending_transfer_exists(
            t: *const Transfer,
            e: *const Transfer,
            p: *const Transfer,
        ) CreateTransferResult {
            assert(t.id == e.id);
            assert(t.id != p.id);
            assert(t.flags.post_pending_transfer or t.flags.void_pending_transfer);
            assert(@as(u16, @bitCast(t.flags)) == @as(u16, @bitCast(e.flags)));
            assert(t.pending_id == e.pending_id);
            assert(t.pending_id == p.id);
            assert(p.flags.pending);
            assert(t.timeout == e.timeout);
            assert(t.timeout == 0);
            assert(e.debit_account_id == p.debit_account_id);
            assert(e.credit_account_id == p.credit_account_id);
            assert(e.ledger == p.ledger);
            assert(e.code == p.code);
            assert(e.timestamp > p.timestamp);

            if (t.debit_account_id != 0 and t.debit_account_id != e.debit_account_id) {
                return .exists_with_different_debit_account_id;
            }
            if (t.credit_account_id != 0 and t.credit_account_id != e.credit_account_id) {
                return .exists_with_different_credit_account_id;
            }

            if (t.flags.void_pending_transfer) {
                if (t.amount == 0) {
                    if (e.amount != p.amount) return .exists_with_different_amount;
                } else {
                    if (t.amount != e.amount) return .exists_with_different_amount;
                }
            }
            if (t.flags.post_pending_transfer) {
                assert(e.amount <= p.amount);
                if (t.amount == std.math.maxInt(u128)) {
                    if (e.amount != p.amount) return .exists_with_different_amount;
                } else {
                    if (t.amount != e.amount) return .exists_with_different_amount;
                }
            }

            if (t.user_data_128 == 0) {
                if (e.user_data_128 != p.user_data_128) {
                    return .exists_with_different_user_data_128;
                }
            } else {
                if (t.user_data_128 != e.user_data_128) {
                    return .exists_with_different_user_data_128;
                }
            }

            if (t.user_data_64 == 0) {
                if (e.user_data_64 != p.user_data_64) {
                    return .exists_with_different_user_data_64;
                }
            } else {
                if (t.user_data_64 != e.user_data_64) {
                    return .exists_with_different_user_data_64;
                }
            }

            if (t.user_data_32 == 0) {
                if (e.user_data_32 != p.user_data_32) {
                    return .exists_with_different_user_data_32;
                }
            } else {
                if (t.user_data_32 != e.user_data_32) {
                    return .exists_with_different_user_data_32;
                }
            }

            if (t.ledger != 0 and t.ledger != e.ledger) {
                return .exists_with_different_ledger;
            }
            if (t.code != 0 and t.code != e.code) {
                return .exists_with_different_code;
            }

            return .exists;
        }

        fn account_event(
            self: *StateMachine,
            args: struct {
                event_timestamp: u64,
                dr_account: *const Account,
                cr_account: *const Account,
                transfer_flags: ?TransferFlags,
                transfer_pending_status: TransferPendingStatus,
                transfer_pending: ?*const Transfer,
                /// The amount from the user request.
                /// It may differ from the recorded `amount` when posting transfers and balancing
                /// accounts. Always zero for expiry events, as no user request is involved.
                amount_requested: u128,
                /// The actual amount recorded in the transfer.
                amount: u128,
            },
        ) void {
            assert(args.event_timestamp > 0);
            switch (args.transfer_pending_status) {
                .none, .pending => {
                    assert(args.transfer_flags != null);
                    assert(args.transfer_pending == null);
                },
                .posted, .voided => {
                    assert(args.transfer_flags != null);
                    assert(args.transfer_pending != null);
                },
                .expired => {
                    assert(args.transfer_flags == null);
                    assert(args.transfer_pending != null);
                },
            }

            // For CDC we always insert the history regardless `Account.flags.history`.
            self.forest.grooves.account_events.insert(&.{
                .timestamp = args.event_timestamp,

                .dr_account_id = args.dr_account.id,
                .dr_account_timestamp = args.dr_account.timestamp,
                .dr_debits_pending = args.dr_account.debits_pending,
                .dr_debits_posted = args.dr_account.debits_posted,
                .dr_credits_pending = args.dr_account.credits_pending,
                .dr_credits_posted = args.dr_account.credits_posted,
                .dr_account_flags = args.dr_account.flags,

                .cr_account_id = args.cr_account.id,
                .cr_account_timestamp = args.cr_account.timestamp,
                .cr_debits_pending = args.cr_account.debits_pending,
                .cr_debits_posted = args.cr_account.debits_posted,
                .cr_credits_pending = args.cr_account.credits_pending,
                .cr_credits_posted = args.cr_account.credits_posted,
                .cr_account_flags = args.cr_account.flags,

                .amount_requested = args.amount_requested,
                .amount = args.amount,
                .ledger = ledger: {
                    assert(args.dr_account.ledger == args.cr_account.ledger);
                    break :ledger args.dr_account.ledger;
                },

                .transfer_flags = if (args.transfer_flags) |flags| flags else .{},

                .transfer_pending_id = if (args.transfer_pending) |p| p.id else 0,
                .transfer_pending_status = args.transfer_pending_status,
                .transfer_pending_flags = if (args.transfer_pending) |p| p.flags else .{},
            });

            if (args.dr_account.flags.history) {
                // Indexing the debit account.
                self.forest.grooves.account_events.indexes.account_timestamp.put(&.{
                    .timestamp = args.event_timestamp,
                    .field = args.dr_account.timestamp,
                });
            }
            if (args.cr_account.flags.history) {
                // Indexing the credit account.
                self.forest.grooves.account_events.indexes.account_timestamp.put(&.{
                    .timestamp = args.event_timestamp,
                    .field = args.cr_account.timestamp,
                });
            }
        }

        fn get_transfer(self: *const StateMachine, id: u128) ?Transfer {
            return switch (self.forest.grooves.transfers.get(id)) {
                .found_object => |t| t,
                .found_orphaned_id, .not_found => null,
            };
        }

        fn get_account(self: *const StateMachine, id: u128) ?Account {
            return switch (self.forest.grooves.accounts.get(id)) {
                .found_object => |a| a,
                .found_orphaned_id => unreachable,
                .not_found => null,
            };
        }

        /// Returns whether a pending transfer, if it exists, has already been
        /// posted,voided, or expired.
        fn get_transfer_pending(
            self: *const StateMachine,
            pending_timestamp: u64,
        ) ?TransferPending {
            return switch (self.forest.grooves.transfers_pending.get(pending_timestamp)) {
                .found_object => |a| a,
                .found_orphaned_id => unreachable,
                .not_found => null,
            };
        }

        fn transfer_update_pending_status(
            self: *StateMachine,
            transfer_pending: *const TransferPending,
            status: TransferPendingStatus,
        ) void {
            assert(transfer_pending.timestamp != 0);
            assert(transfer_pending.status == .pending);
            assert(status != .none and status != .pending);

            self.forest.grooves.transfers_pending.update(.{
                .old = transfer_pending,
                .new = &.{
                    .timestamp = transfer_pending.timestamp,
                    .status = status,
                },
            });
        }

        fn execute_expire_pending_transfers(self: *StateMachine, timestamp: u64) usize {
            assert(self.scan_lookup_results.items.len == 1); // No multi-batch.
            assert(timestamp > self.commit_timestamp or constants.aof_recovery);

            defer {
                self.scan_lookup_buffer_index = 0;
                self.scan_lookup_results.clearRetainingCapacity();
            }

            const result_count: u32 = self.scan_lookup_results.items[0];
            if (result_count == 0) return 0;

            const result_max: u32 = @max(
                Operation.create_transfers.event_max(self.batch_size_limit),
                Operation.deprecated_create_transfers_unbatched.event_max(self.batch_size_limit),
            );
            assert(result_count <= result_max);

            assert(self.scan_lookup_buffer_index > 0);
            assert(self.scan_lookup_buffer_index == result_count * @sizeOf(Transfer));

            const transfers_pending = stdx.bytes_as_slice(
                .exact,
                Transfer,
                self.scan_lookup_buffer[0..self.scan_lookup_buffer_index],
            );

            log.debug("expire_pending_transfers: len={}", .{transfers_pending.len});

            for (transfers_pending, 0..) |*p, index| {
                assert(p.flags.pending);
                assert(p.timeout > 0);

                const event_timestamp = timestamp - transfers_pending.len + index + 1;
                assert(TimestampRange.valid(event_timestamp));
                assert(self.commit_timestamp < event_timestamp);
                defer self.commit_timestamp = event_timestamp;

                const expires_at = p.timestamp + p.timeout_ns();
                assert(expires_at <= event_timestamp);

                const dr_account = self.get_account(
                    p.debit_account_id,
                ).?;
                assert(dr_account.debits_pending >= p.amount);

                const cr_account = self.get_account(
                    p.credit_account_id,
                ).?;
                assert(cr_account.credits_pending >= p.amount);

                var dr_account_new = dr_account;
                var cr_account_new = cr_account;
                dr_account_new.debits_pending -= p.amount;
                cr_account_new.credits_pending -= p.amount;

                if (p.flags.closing_debit) {
                    assert(dr_account_new.flags.closed);
                    dr_account_new.flags.closed = false;
                }
                if (p.flags.closing_credit) {
                    assert(cr_account_new.flags.closed);
                    cr_account_new.flags.closed = false;
                }

                // Pending transfers can expire in closed accounts.
                maybe(dr_account_new.flags.closed);
                maybe(cr_account_new.flags.closed);

                const dr_updated = p.amount > 0 or
                    dr_account_new.flags.closed != dr_account.flags.closed;
                if (dr_updated) {
                    self.forest.grooves.accounts.update(.{
                        .old = &dr_account,
                        .new = &dr_account_new,
                    });
                }

                const cr_updated = p.amount > 0 or
                    cr_account_new.flags.closed != cr_account.flags.closed;
                if (cr_updated) {
                    self.forest.grooves.accounts.update(.{
                        .old = &cr_account,
                        .new = &cr_account_new,
                    });
                }

                const transfer_pending = self.get_transfer_pending(p.timestamp).?;
                assert(p.timestamp == transfer_pending.timestamp);
                assert(transfer_pending.status == .pending);
                self.transfer_update_pending_status(&transfer_pending, .expired);

                // Removing the `expires_at` index.
                self.forest.grooves.transfers.indexes.expires_at.remove(&.{
                    .timestamp = p.timestamp,
                    .field = expires_at,
                });

                self.account_event(.{
                    .event_timestamp = event_timestamp,
                    .dr_account = &dr_account_new,
                    .cr_account = &cr_account_new,
                    .transfer_flags = null,
                    .transfer_pending_status = .expired,
                    .transfer_pending = p,
                    .amount_requested = 0,
                    .amount = p.amount,
                });
            }

            // This operation has no output.
            return 0;
        }

        pub fn forest_options(options: Options) Forest.GroovesOptions {
            const prefetch_create_accounts_limit: u32 = @max(
                Operation.create_accounts.event_max(options.batch_size_limit),
                Operation.deprecated_create_accounts_unbatched.event_max(options.batch_size_limit),
            );
            assert(prefetch_create_accounts_limit > 0);
            assert(prefetch_create_accounts_limit <= batch_max.create_accounts);

            const prefetch_lookup_accounts_limit: u32 = @max(
                Operation.lookup_accounts.event_max(options.batch_size_limit),
                Operation.deprecated_lookup_accounts_unbatched.event_max(options.batch_size_limit),
            );
            assert(prefetch_lookup_accounts_limit > 0);
            assert(prefetch_lookup_accounts_limit <= batch_max.lookup_accounts);
            assert(prefetch_create_accounts_limit <= batch_max.lookup_accounts);

            const prefetch_create_transfers_limit: u32 = @max(
                Operation.create_transfers.event_max(options.batch_size_limit),
                Operation.deprecated_create_transfers_unbatched.event_max(options.batch_size_limit),
            );
            assert(prefetch_create_transfers_limit > 0);
            assert(prefetch_create_transfers_limit <= batch_max.create_transfers);

            const prefetch_lookup_transfers_limit: u32 = @max(
                Operation.lookup_transfers.event_max(options.batch_size_limit),
                Operation.deprecated_lookup_transfers_unbatched.event_max(options.batch_size_limit),
            );
            assert(prefetch_lookup_transfers_limit > 0);
            assert(prefetch_lookup_transfers_limit <= batch_max.lookup_transfers);
            assert(prefetch_create_accounts_limit <= batch_max.lookup_transfers);
            assert(prefetch_create_transfers_limit <= batch_max.lookup_transfers);

            // Inputs are bounded by the runtime-known `batch_size_limit`,
            // while replies are only limited by the constant `message_body_size_max`.
            // This allows more read objects (lookups and queries) than writes (creates).
            maybe(prefetch_lookup_accounts_limit > prefetch_create_accounts_limit);
            maybe(prefetch_lookup_transfers_limit > prefetch_create_transfers_limit);

            const tree_values_count_limit = tree_values_count(options.batch_size_limit);
            return .{
                .accounts = .{
                    // lookup_account() looks up 1 Account per item.
                    .prefetch_entries_for_read_max = @max(
                        prefetch_lookup_accounts_limit,
                        prefetch_create_accounts_limit,
                    ),
                    .prefetch_entries_for_update_max = @max(
                        prefetch_create_accounts_limit, // create_account
                        2 * prefetch_create_transfers_limit, // create_transfer dr and cr accounts.
                    ),
                    .cache_entries_max = options.cache_entries_accounts,
                    .tree_options_object = .{
                        .batch_value_count_limit = tree_values_count_limit.accounts.timestamp,
                    },
                    .tree_options_id = .{
                        .batch_value_count_limit = tree_values_count_limit.accounts.id,
                    },
                    .tree_options_index = index_tree_options(
                        AccountsGroove.IndexTreeOptions,
                        tree_values_count_limit.accounts,
                    ),
                },
                .transfers = .{
                    // lookup_transfer() looks up 1 Transfer.
                    // create_transfer() looks up at most 1 Transfer for posting/voiding.
                    .prefetch_entries_for_read_max = @max(
                        prefetch_lookup_transfers_limit,
                        prefetch_create_transfers_limit,
                    ),
                    // create_transfer() updates a single Transfer.
                    .prefetch_entries_for_update_max = prefetch_create_transfers_limit,
                    .cache_entries_max = options.cache_entries_transfers,
                    .tree_options_object = .{
                        .batch_value_count_limit = tree_values_count_limit.transfers.timestamp,
                    },
                    .tree_options_id = .{
                        .batch_value_count_limit = tree_values_count_limit.transfers.id,
                    },
                    .tree_options_index = index_tree_options(
                        TransfersGroove.IndexTreeOptions,
                        tree_values_count_limit.transfers,
                    ),
                },
                .transfers_pending = .{
                    .prefetch_entries_for_read_max = @max(
                        prefetch_lookup_transfers_limit,
                        prefetch_create_transfers_limit,
                    ),
                    // create_transfer() posts/voids at most one transfer.
                    .prefetch_entries_for_update_max = prefetch_create_transfers_limit,
                    .cache_entries_max = options.cache_entries_transfers_pending,
                    .tree_options_object = .{
                        .batch_value_count_limit = tree_values_count_limit
                            .transfers_pending.timestamp,
                    },
                    .tree_options_id = {},
                    .tree_options_index = index_tree_options(
                        TransfersPendingGroove.IndexTreeOptions,
                        tree_values_count_limit.transfers_pending,
                    ),
                },
                .account_events = .{
                    .prefetch_entries_for_read_max = 0,
                    // We don't need to update the history, it's append only.
                    .prefetch_entries_for_update_max = 0,
                    .cache_entries_max = 0,
                    .tree_options_object = .{
                        .batch_value_count_limit = tree_values_count_limit
                            .account_events.timestamp,
                    },
                    .tree_options_id = {},
                    .tree_options_index = index_tree_options(
                        AccountEventsGroove.IndexTreeOptions,
                        tree_values_count_limit.account_events,
                    ),
                },
            };
        }

        fn index_tree_options(
            comptime IndexTreeOptions: type,
            batch_limits: anytype,
        ) IndexTreeOptions {
            var result: IndexTreeOptions = undefined;
            inline for (comptime std.meta.fieldNames(IndexTreeOptions)) |field| {
                @field(result, field) = .{ .batch_value_count_limit = @field(batch_limits, field) };
            }
            return result;
        }

        fn tree_values_count(batch_size_limit: u32) struct {
            accounts: struct {
                id: u32,
                user_data_128: u32,
                user_data_64: u32,
                user_data_32: u32,
                ledger: u32,
                code: u32,
                timestamp: u32,
                imported: u32,
                closed: u32,
            },
            transfers: struct {
                timestamp: u32,
                id: u32,
                debit_account_id: u32,
                credit_account_id: u32,
                amount: u32,
                pending_id: u32,
                user_data_128: u32,
                user_data_64: u32,
                user_data_32: u32,
                ledger: u32,
                code: u32,
                expires_at: u32,
                imported: u32,
                closing: u32,
            },
            transfers_pending: struct {
                timestamp: u32,
                status: u32,
            },
            account_events: struct {
                timestamp: u32,
                account_timestamp: u32,
                transfer_pending_status: u32,
                dr_account_id_expired: u32,
                cr_account_id_expired: u32,
                transfer_pending_id_expired: u32,
                ledger_expired: u32,
                prunable: u32,
            },
        } {
            assert(batch_size_limit <= constants.message_body_size_max);

            const batch_create_accounts: u32 = @max(
                Operation.create_accounts.event_max(batch_size_limit),
                Operation.deprecated_create_accounts_unbatched.event_max(batch_size_limit),
            );
            assert(batch_create_accounts > 0);
            assert(batch_create_accounts <= batch_max.create_accounts);

            const batch_create_transfers: u32 = @max(
                Operation.create_transfers.event_max(batch_size_limit),
                Operation.deprecated_create_transfers_unbatched.event_max(batch_size_limit),
            );
            assert(batch_create_transfers > 0);
            assert(batch_create_transfers <= batch_max.create_transfers);

            return .{
                .accounts = .{
                    .id = batch_create_accounts,
                    .user_data_128 = batch_create_accounts,
                    .user_data_64 = batch_create_accounts,
                    .user_data_32 = batch_create_accounts,
                    .ledger = batch_create_accounts,
                    .code = batch_create_accounts,
                    .imported = batch_create_accounts,

                    // Transfers mutate the account balance and the closed flag.
                    // Each transfer modifies two accounts.
                    .timestamp = @max(batch_create_accounts, 2 * batch_create_transfers),
                    .closed = @max(batch_create_accounts, 2 * batch_create_transfers),
                },
                .transfers = .{
                    .timestamp = batch_create_transfers,
                    .id = batch_create_transfers,
                    .debit_account_id = batch_create_transfers,
                    .credit_account_id = batch_create_transfers,
                    .amount = batch_create_transfers,
                    .pending_id = batch_create_transfers,
                    .user_data_128 = batch_create_transfers,
                    .user_data_64 = batch_create_transfers,
                    .user_data_32 = batch_create_transfers,
                    .ledger = batch_create_transfers,
                    .code = batch_create_transfers,
                    .expires_at = batch_create_transfers,
                    .imported = batch_create_transfers,
                    .closing = batch_create_transfers,
                },
                .transfers_pending = .{
                    // Objects are mutated when the pending transfer is posted/voided/expired.
                    .timestamp = 2 * batch_create_transfers,
                    .status = 2 * batch_create_transfers,
                },
                .account_events = .{
                    .timestamp = batch_create_transfers,
                    .account_timestamp = 2 * batch_create_transfers, // dr and cr accounts.
                    .transfer_pending_status = batch_create_transfers,
                    .dr_account_id_expired = batch_create_transfers,
                    .cr_account_id_expired = batch_create_transfers,
                    .transfer_pending_id_expired = batch_create_transfers,
                    .ledger_expired = batch_create_transfers,
                    .prunable = batch_create_transfers,
                },
            };
        }
    };
}

/// Scans all `Transfers` that already expired at any timestamp.
/// A custom evaluator is used to stop at the first result where
/// `expires_at > prefetch_timestamp` while updating the next pulse timestamp.
/// This way we can achieve the same effect of two conditions with a single scan:
/// ```
/// WHERE expires_at <= prefetch_timestamp
/// UNION
/// WHERE expires_at > prefetch_timestamp LIMIT 1
/// ```
fn ExpirePendingTransfersType(
    comptime TransfersGroove: type,
    comptime Storage: type,
) type {
    return struct {
        const ExpirePendingTransfers = @This();
        const ScanRangeType = @import("lsm/scan_range.zig").ScanRangeType;
        const EvaluateNext = @import("lsm/scan_range.zig").EvaluateNext;
        const ScanLookupStatus = @import("lsm/scan_lookup.zig").ScanLookupStatus;

        const Tree = @FieldType(TransfersGroove.IndexTrees, "expires_at");
        const Value = Tree.Table.Value;

        // TODO(zig) Context should be `*ExpirePendingTransfers`,
        // but its a dependency loop.
        const Context = struct {};
        const ScanRange = ScanRangeType(
            Tree,
            Storage,
            *Context,
            value_next,
            timestamp_from_value,
        );

        pub const ScanLookup = ScanLookupType(
            TransfersGroove,
            ScanRange,
            Storage,
        );

        context: Context = undefined,
        phase: union(enum) {
            idle,
            running: struct {
                scan: ScanRange,
                expires_at_max: u64,
            },
        } = .idle,

        /// Used by the state machine to determine "when" it needs to execute the expiration logic:
        /// - When `== timestamp_min`, there may be pending transfers to expire,
        ///   but we need to scan to check.
        /// - When `== timestamp_max`, there are no pending transfers to expire.
        /// - Otherwise, this is the timestamp of the next pending transfer expiry.
        pulse_next_timestamp: u64 = TimestampRange.timestamp_min,
        value_next_expired_at: ?u64 = null,

        fn reset(self: *ExpirePendingTransfers) void {
            assert(self.phase == .idle);
            self.* = .{};
        }

        fn scan(
            self: *ExpirePendingTransfers,
            tree: *Tree,
            buffer: *const ScanBuffer,
            filter: struct {
                snapshot: u64,
                /// Will fetch transfers expired before this timestamp (inclusive).
                expires_at_max: u64,
            },
        ) *ScanRange {
            assert(self.phase == .idle);
            assert(TimestampRange.valid(filter.expires_at_max));
            maybe(filter.expires_at_max != TimestampRange.timestamp_min and
                filter.expires_at_max != TimestampRange.timestamp_max and
                self.pulse_next_timestamp > filter.expires_at_max);

            self.* = .{
                .pulse_next_timestamp = self.pulse_next_timestamp,
                .phase = .{ .running = .{
                    .expires_at_max = filter.expires_at_max,
                    .scan = ScanRange.init(
                        &self.context,
                        tree,
                        buffer,
                        filter.snapshot,
                        Tree.Table.key_from_value(&.{
                            .field = TimestampRange.timestamp_min,
                            .timestamp = TimestampRange.timestamp_min,
                        }),
                        Tree.Table.key_from_value(&.{
                            .field = TimestampRange.timestamp_max,
                            .timestamp = TimestampRange.timestamp_max,
                        }),
                        .ascending,
                    ),
                } },
            };
            return &self.phase.running.scan;
        }

        fn finish(
            self: *ExpirePendingTransfers,
            status: ScanLookupStatus,
            results: []const Transfer,
        ) void {
            assert(self.phase == .running);
            if (self.phase.running.expires_at_max != TimestampRange.timestamp_min and
                self.phase.running.expires_at_max != TimestampRange.timestamp_max and
                self.pulse_next_timestamp > self.phase.running.expires_at_max)
            {
                assert(results.len == 0);
            }

            switch (status) {
                .scan_finished => {
                    if (self.value_next_expired_at == null or
                        self.value_next_expired_at.? <= self.phase.running.expires_at_max)
                    {
                        // There are no more unexpired transfers left to expire in the next pulse.
                        self.pulse_next_timestamp = TimestampRange.timestamp_max;
                    } else {
                        self.pulse_next_timestamp = self.value_next_expired_at.?;
                    }
                },
                .buffer_finished => {
                    // There are more transfers to expire than a single batch.
                    assert(self.value_next_expired_at != null);
                    self.pulse_next_timestamp = self.value_next_expired_at.?;
                },
                else => unreachable,
            }
            self.phase = .idle;
            self.value_next_expired_at = null;
        }

        inline fn value_next(context: *Context, value: *const Value) EvaluateNext {
            const self: *ExpirePendingTransfers = @alignCast(@fieldParentPtr(
                "context",
                context,
            ));
            assert(self.phase == .running);

            const expires_at: u64 = value.field;

            assert(self.value_next_expired_at == null or
                self.value_next_expired_at.? <= expires_at);

            self.value_next_expired_at = expires_at;

            return if (expires_at <= self.phase.running.expires_at_max)
                .include_and_continue
            else
                .exclude_and_stop;
        }

        inline fn timestamp_from_value(context: *Context, value: *const Value) u64 {
            _ = context;
            return value.timestamp;
        }
    };
}

/// Iterates directly over the `ScanTree` since this query doesn't use secondary indexes.
/// No additional lookups are necessary, as the `ScanTree` iteration already yields the
/// object values.
fn ChangeEventsScanLookupType(
    comptime AccountEventsGroove: type,
    comptime Storage: type,
) type {
    const ScanTreeType = @import("lsm/scan_tree.zig").ScanTreeType;

    return struct {
        const AccountEventsLookup = @This();
        const ScanTree = ScanTreeType(
            void,
            AccountEventsGroove.ObjectTree,
            Storage,
        );

        pub const Callback = *const fn (*AccountEventsLookup, []const AccountEvent) void;

        scan_tree: ScanTree,
        state: union(enum) {
            idle,
            scan: struct {
                buffer: []AccountEvent,
                callback: Callback,
                buffer_produced_len: usize,
            },
            finished,
        },

        fn init(
            self: *AccountEventsLookup,
            tree: *AccountEventsGroove.ObjectTree,
            scan_buffer: *const ScanBuffer,
            snapshot: u64,
            timestamp_range: TimestampRange,
        ) void {
            self.* = .{
                .scan_tree = ScanTree.init(
                    tree,
                    scan_buffer,
                    snapshot,
                    timestamp_range.min,
                    timestamp_range.max,
                    .ascending,
                ),
                .state = .idle,
            };
        }

        fn read(
            self: *AccountEventsLookup,
            buffer: []AccountEvent,
            callback: Callback,
        ) void {
            assert(self.state == .idle);
            assert(self.scan_tree.state == .idle);
            assert(buffer.len > 0);

            self.state = .{
                .scan = .{
                    .buffer = buffer,
                    .callback = callback,
                    .buffer_produced_len = 0,
                },
            };
            self.scan_tree.read({}, &scan_read_callback);
        }

        fn scan_read_callback(_: void, scan_tree: *ScanTree) void {
            const self: *AccountEventsLookup = @fieldParentPtr("scan_tree", scan_tree);
            assert(self.state == .scan);

            while (scan_tree.next() catch |err| switch (err) {
                error.Pending => {
                    self.scan_tree.read({}, &scan_read_callback);
                    return;
                },
            }) |object| {
                assert(self.state.scan.buffer_produced_len < self.state.scan.buffer.len);
                assert(object.timestamp != 0);

                switch (object.schema()) {
                    .current => {},
                    .former => |former| {
                        // In the former schema:
                        // Only accounts with the `history` flag enabled had their balance stored.
                        assert(former.dr_account_id != 0 or former.cr_account_id != 0);
                        if (former.dr_account_id == 0 or former.cr_account_id == 0) {
                            // Skipping events with the balance of just one side,
                            // as they are not useful for `get_change_events`.
                            continue;
                        }
                    },
                }
                assert(object.dr_account_id != 0);
                assert(object.cr_account_id != 0);

                self.state.scan.buffer[self.state.scan.buffer_produced_len] = object;
                self.state.scan.buffer_produced_len += 1;
                if (self.state.scan.buffer_produced_len == self.state.scan.buffer.len) break;
            }

            const callback = self.state.scan.callback;
            const results = self.state.scan.buffer[0..self.state.scan.buffer_produced_len];
            self.state = .finished;
            callback(self, results);
        }
    };
}

fn sum_overflows(comptime Int: type, a: Int, b: Int) bool {
    comptime assert(Int != comptime_int);
    comptime assert(Int != comptime_float);
    _ = std.math.add(Int, a, b) catch return true;
    return false;
}

fn sum_overflows_test(comptime Int: type) !void {
    try std.testing.expectEqual(false, sum_overflows(Int, math.maxInt(Int), 0));
    try std.testing.expectEqual(false, sum_overflows(Int, math.maxInt(Int) - 1, 1));
    try std.testing.expectEqual(false, sum_overflows(Int, 1, math.maxInt(Int) - 1));

    try std.testing.expectEqual(true, sum_overflows(Int, math.maxInt(Int), 1));
    try std.testing.expectEqual(true, sum_overflows(Int, 1, math.maxInt(Int)));

    try std.testing.expectEqual(true, sum_overflows(Int, math.maxInt(Int), math.maxInt(Int)));
    try std.testing.expectEqual(true, sum_overflows(Int, math.maxInt(Int), math.maxInt(Int)));
}

test "sum_overflows" {
    try sum_overflows_test(u64);
    try sum_overflows_test(u128);
}
