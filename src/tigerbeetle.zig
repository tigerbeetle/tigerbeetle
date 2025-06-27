const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;

const stdx = @import("stdx.zig");
const constants = @import("constants.zig");

pub const Account = extern struct {
    id: u128,
    debits_pending: u128,
    debits_posted: u128,
    credits_pending: u128,
    credits_posted: u128,
    /// Opaque third-party identifiers to link this account (many-to-one) to external entities.
    user_data_128: u128,
    user_data_64: u64,
    user_data_32: u32,
    /// Reserved for accounting policy primitives.
    reserved: u32,
    ledger: u32,
    /// A chart of accounts code describing the type of account (e.g. clearing, settlement).
    code: u16,
    flags: AccountFlags,
    timestamp: u64,

    comptime {
        assert(stdx.no_padding(Account));
        assert(@sizeOf(Account) == 128);
        assert(@alignOf(Account) == 16);
    }

    pub fn debits_exceed_credits(self: *const Account, amount: u128) bool {
        return (self.flags.debits_must_not_exceed_credits and
            self.debits_pending + self.debits_posted + amount > self.credits_posted);
    }

    pub fn credits_exceed_debits(self: *const Account, amount: u128) bool {
        return (self.flags.credits_must_not_exceed_debits and
            self.credits_pending + self.credits_posted + amount > self.debits_posted);
    }
};

pub const AccountFlags = packed struct(u16) {
    /// When the .linked flag is specified, it links an event with the next event in the batch, to
    /// create a chain of events, of arbitrary length, which all succeed or fail together. The tail
    /// of a chain is denoted by the first event without this flag. The last event in a batch may
    /// therefore never have the .linked flag set as this would leave a chain open-ended. Multiple
    /// chains or individual events may coexist within a batch to succeed or fail independently.
    /// Events within a chain are executed within order, or are rolled back on error, so that the
    /// effect of each event in the chain is visible to the next, and so that the chain is either
    /// visible or invisible as a unit to subsequent events after the chain. The event that was the
    /// first to break the chain will have a unique error result. Other events in the chain will
    /// have their error result set to .linked_event_failed.
    linked: bool = false,
    debits_must_not_exceed_credits: bool = false,
    credits_must_not_exceed_debits: bool = false,
    history: bool = false,
    imported: bool = false,
    closed: bool = false,
    padding: u10 = 0,

    comptime {
        assert(@sizeOf(AccountFlags) == @sizeOf(u16));
        assert(@bitSizeOf(AccountFlags) == @sizeOf(AccountFlags) * 8);
    }
};

pub const AccountBalance = extern struct {
    debits_pending: u128,
    debits_posted: u128,
    credits_pending: u128,
    credits_posted: u128,
    timestamp: u64,
    reserved: [56]u8 = [_]u8{0} ** 56,

    comptime {
        assert(stdx.no_padding(AccountBalance));
        assert(@sizeOf(AccountBalance) == 128);
        assert(@alignOf(AccountBalance) == 16);
    }
};

pub const Transfer = extern struct {
    id: u128,
    debit_account_id: u128,
    credit_account_id: u128,
    amount: u128,
    /// If this transfer will post or void a pending transfer, the id of that pending transfer.
    pending_id: u128,
    /// Opaque third-party identifiers to link this transfer (many-to-one) to an external entities.
    user_data_128: u128,
    user_data_64: u64,
    user_data_32: u32,
    /// Timeout in seconds for pending transfers to expire automatically
    /// if not manually posted or voided.
    timeout: u32,
    ledger: u32,
    /// A chart of accounts code describing the reason for the transfer (e.g. deposit, settlement).
    code: u16,
    flags: TransferFlags,
    timestamp: u64,

    // Converts the timeout from seconds to ns.
    pub fn timeout_ns(self: *const Transfer) u64 {
        // Casting to u64 to avoid integer overflow:
        return @as(u64, self.timeout) * std.time.ns_per_s;
    }

    comptime {
        assert(stdx.no_padding(Transfer));
        assert(@sizeOf(Transfer) == 128);
        assert(@alignOf(Transfer) == 16);
    }
};

pub const TransferPendingStatus = enum(u8) {
    none = 0,
    pending = 1,
    posted = 2,
    voided = 3,
    expired = 4,

    comptime {
        for (std.enums.values(TransferPendingStatus), 0..) |result, index| {
            assert(@intFromEnum(result) == index);
        }
    }
};

pub const TransferFlags = packed struct(u16) {
    linked: bool = false,
    pending: bool = false,
    post_pending_transfer: bool = false,
    void_pending_transfer: bool = false,
    balancing_debit: bool = false,
    balancing_credit: bool = false,
    closing_debit: bool = false,
    closing_credit: bool = false,
    imported: bool = false,
    balancing_debit_with_pending: bool = false,
    balancing_credit_with_pending: bool = false,
    padding: u5 = 0,

    comptime {
        assert(@sizeOf(TransferFlags) == @sizeOf(u16));
        assert(@bitSizeOf(TransferFlags) == @sizeOf(TransferFlags) * 8);
    }
};

/// Error codes are ordered by descending precedence.
/// When errors do not have an obvious/natural precedence (e.g. "*_must_be_zero"),
/// the ordering matches struct field order.
pub const CreateAccountResult = enum(u32) {
    ok = 0,
    linked_event_failed = 1,
    linked_event_chain_open = 2,

    imported_event_expected = 22,
    imported_event_not_expected = 23,

    timestamp_must_be_zero = 3,

    imported_event_timestamp_out_of_range = 24,
    imported_event_timestamp_must_not_advance = 25,

    reserved_field = 4,
    reserved_flag = 5,

    id_must_not_be_zero = 6,
    id_must_not_be_int_max = 7,

    exists_with_different_flags = 15,
    exists_with_different_user_data_128 = 16,
    exists_with_different_user_data_64 = 17,
    exists_with_different_user_data_32 = 18,
    exists_with_different_ledger = 19,
    exists_with_different_code = 20,
    exists = 21,

    flags_are_mutually_exclusive = 8,

    debits_pending_must_be_zero = 9,
    debits_posted_must_be_zero = 10,
    credits_pending_must_be_zero = 11,
    credits_posted_must_be_zero = 12,
    ledger_must_not_be_zero = 13,
    code_must_not_be_zero = 14,

    imported_event_timestamp_must_not_regress = 26,

    comptime {
        const values = std.enums.values(CreateAccountResult);
        const BitSet = stdx.BitSetType(values.len);
        var set: BitSet = .{};
        for (0..values.len) |index| {
            const result: CreateAccountResult = @enumFromInt(index);
            stdx.maybe(result == values[index]);

            assert(!set.is_set(index));
            set.set(index);
        }

        // It's a non-ordered enum, we need to ensure
        // there are no gaps in the numbering of the values.
        assert(set.full());
    }
};

/// Error codes are ordered by descending precedence.
/// When errors do not have an obvious/natural precedence (e.g. "*_must_not_be_zero"),
/// the ordering matches struct field order.
pub const CreateTransferResult = enum(u32) {
    ok = 0,
    linked_event_failed = 1,
    linked_event_chain_open = 2,

    imported_event_expected = 56,
    imported_event_not_expected = 57,

    timestamp_must_be_zero = 3,

    imported_event_timestamp_out_of_range = 58,
    imported_event_timestamp_must_not_advance = 59,

    reserved_flag = 4,

    id_must_not_be_zero = 5,
    id_must_not_be_int_max = 6,

    exists_with_different_flags = 36,
    exists_with_different_pending_id = 40,
    exists_with_different_timeout = 44,
    exists_with_different_debit_account_id = 37,
    exists_with_different_credit_account_id = 38,
    exists_with_different_amount = 39,
    exists_with_different_user_data_128 = 41,
    exists_with_different_user_data_64 = 42,
    exists_with_different_user_data_32 = 43,
    exists_with_different_ledger = 67,
    exists_with_different_code = 45,
    exists = 46,

    id_already_failed = 68,

    flags_are_mutually_exclusive = 7,

    debit_account_id_must_not_be_zero = 8,
    debit_account_id_must_not_be_int_max = 9,
    credit_account_id_must_not_be_zero = 10,
    credit_account_id_must_not_be_int_max = 11,
    accounts_must_be_different = 12,

    pending_id_must_be_zero = 13,
    pending_id_must_not_be_zero = 14,
    pending_id_must_not_be_int_max = 15,
    pending_id_must_be_different = 16,
    timeout_reserved_for_pending_transfer = 17,

    closing_transfer_must_be_pending = 64,

    ledger_must_not_be_zero = 19,
    code_must_not_be_zero = 20,

    debit_account_not_found = 21,
    credit_account_not_found = 22,

    accounts_must_have_the_same_ledger = 23,
    transfer_must_have_the_same_ledger_as_accounts = 24,

    pending_transfer_not_found = 25,
    pending_transfer_not_pending = 26,

    pending_transfer_has_different_debit_account_id = 27,
    pending_transfer_has_different_credit_account_id = 28,
    pending_transfer_has_different_ledger = 29,
    pending_transfer_has_different_code = 30,

    exceeds_pending_transfer_amount = 31,
    pending_transfer_has_different_amount = 32,

    pending_transfer_already_posted = 33,
    pending_transfer_already_voided = 34,

    pending_transfer_expired = 35,

    imported_event_timestamp_must_not_regress = 60,
    imported_event_timestamp_must_postdate_debit_account = 61,
    imported_event_timestamp_must_postdate_credit_account = 62,
    imported_event_timeout_must_be_zero = 63,

    debit_account_already_closed = 65,
    credit_account_already_closed = 66,

    overflows_debits_pending = 47,
    overflows_credits_pending = 48,
    overflows_debits_posted = 49,
    overflows_credits_posted = 50,
    overflows_debits = 51,
    overflows_credits = 52,
    overflows_timeout = 53,

    exceeds_credits = 54,
    exceeds_debits = 55,

    deprecated_18 = 18, // amount_must_not_be_zero.

    // Update this comment when adding a new value:
    // Last item: id_already_failed = 68.

    /// Returns `true` if the error code depends on transient system status and retrying
    /// the same transfer with identical request data can produce different outcomes.
    pub fn transient(result: CreateTransferResult) bool {
        return switch (result) {
            .ok => unreachable,

            .debit_account_not_found,
            .credit_account_not_found,
            .pending_transfer_not_found,
            .exceeds_credits,
            .exceeds_debits,
            .debit_account_already_closed,
            .credit_account_already_closed,
            => true,

            .linked_event_failed,
            .linked_event_chain_open,
            .imported_event_expected,
            .imported_event_not_expected,
            .timestamp_must_be_zero,
            .imported_event_timestamp_out_of_range,
            .imported_event_timestamp_must_not_advance,
            .reserved_flag,
            .id_must_not_be_zero,
            .id_must_not_be_int_max,
            .id_already_failed,
            .exists_with_different_flags,
            .exists_with_different_pending_id,
            .exists_with_different_timeout,
            .exists_with_different_debit_account_id,
            .exists_with_different_credit_account_id,
            .exists_with_different_amount,
            .exists_with_different_user_data_128,
            .exists_with_different_user_data_64,
            .exists_with_different_user_data_32,
            .exists_with_different_ledger,
            .exists_with_different_code,
            .exists,
            .imported_event_timestamp_must_not_regress,
            .imported_event_timestamp_must_postdate_debit_account,
            .imported_event_timestamp_must_postdate_credit_account,
            .imported_event_timeout_must_be_zero,
            .flags_are_mutually_exclusive,
            .debit_account_id_must_not_be_zero,
            .debit_account_id_must_not_be_int_max,
            .credit_account_id_must_not_be_zero,
            .credit_account_id_must_not_be_int_max,
            .accounts_must_be_different,
            .pending_id_must_be_zero,
            .pending_id_must_not_be_zero,
            .pending_id_must_not_be_int_max,
            .pending_id_must_be_different,
            .timeout_reserved_for_pending_transfer,
            .closing_transfer_must_be_pending,
            .ledger_must_not_be_zero,
            .code_must_not_be_zero,
            .accounts_must_have_the_same_ledger,
            .transfer_must_have_the_same_ledger_as_accounts,
            .pending_transfer_not_pending,
            .pending_transfer_has_different_debit_account_id,
            .pending_transfer_has_different_credit_account_id,
            .pending_transfer_has_different_ledger,
            .pending_transfer_has_different_code,
            .exceeds_pending_transfer_amount,
            .pending_transfer_has_different_amount,
            .pending_transfer_already_posted,
            .pending_transfer_already_voided,
            .pending_transfer_expired,
            .overflows_debits_pending,
            .overflows_credits_pending,
            .overflows_debits_posted,
            .overflows_credits_posted,
            .overflows_debits,
            .overflows_credits,
            .overflows_timeout,
            => false,

            .deprecated_18 => unreachable,
        };
    }

    comptime {
        const values = std.enums.values(CreateTransferResult);
        const BitSet = stdx.BitSetType(values.len);
        var set: BitSet = .{};
        for (0..values.len) |index| {
            const result: CreateTransferResult = @enumFromInt(index);
            stdx.maybe(result == values[index]);

            assert(!set.is_set(index));
            set.set(index);
        }

        // It's a non-ordered enum, we need to ensure
        // there are no gaps in the numbering of the values.
        assert(set.full());
    }

    /// TODO(zig): CreateTransferResult is ordered by precedence, but it crashes
    /// `EnumSet`, and `@setEvalBranchQuota()` isn't propagating correctly:
    /// https://godbolt.org/z/6a45bx6xs
    /// error: evaluation exceeded 1000 backwards branches
    /// note: use @setEvalBranchQuota() to raise the branch limit from 1000.
    ///
    /// As a workaround we generate a new Ordered enum to be used in this case.
    pub const Ordered = type: {
        const values = std.enums.values(CreateTransferResult);
        var fields: [values.len]std.builtin.Type.EnumField = undefined;
        for (0..values.len) |index| {
            const result: CreateTransferResult = @enumFromInt(index);
            fields[index] = .{
                .name = @tagName(result),
                .value = index,
            };
        }

        var type_info = @typeInfo(enum {});
        type_info.Enum.tag_type = std.meta.Tag(CreateTransferResult);
        type_info.Enum.fields = &fields;
        break :type @Type(type_info);
    };

    pub fn to_ordered(value: CreateTransferResult) Ordered {
        return @enumFromInt(@intFromEnum(value));
    }

    comptime {
        const values = std.enums.values(Ordered);
        assert(values.len == std.enums.values(CreateTransferResult).len);
        for (0..values.len) |index| {
            const value: Ordered = @enumFromInt(index);
            assert(value == values[index]);

            const value_source: CreateTransferResult = @enumFromInt(index);
            assert(std.mem.eql(u8, @tagName(value_source), @tagName(value)));
        }
    }
};

pub const CreateAccountsResult = extern struct {
    index: u32,
    result: CreateAccountResult,

    comptime {
        assert(@sizeOf(CreateAccountsResult) == 8);
        assert(stdx.no_padding(CreateAccountsResult));
    }
};

pub const CreateTransfersResult = extern struct {
    index: u32,
    result: CreateTransferResult,

    comptime {
        assert(@sizeOf(CreateTransfersResult) == 8);
        assert(stdx.no_padding(CreateTransfersResult));
    }
};

pub const QueryFilter = extern struct {
    /// Query by the `user_data_128` index.
    /// Use zero for no filter.
    user_data_128: u128,
    /// Query by the `user_data_64` index.
    /// Use zero for no filter.
    user_data_64: u64,
    /// Query by the `user_data_32` index.
    /// Use zero for no filter.
    user_data_32: u32,
    /// Query by the `ledger` index.
    /// Use zero for no filter.
    ledger: u32,
    /// Query by the `code` index.
    /// Use zero for no filter.
    code: u16,
    reserved: [6]u8 = [_]u8{0} ** 6,
    /// The initial timestamp (inclusive).
    /// Use zero for no filter.
    timestamp_min: u64,
    /// The final timestamp (inclusive).
    /// Use zero for no filter.
    timestamp_max: u64,
    /// Maximum number of results that can be returned by this query.
    /// Must be greater than zero.
    limit: u32,
    /// Query flags.
    flags: QueryFilterFlags,

    comptime {
        assert(@sizeOf(QueryFilter) == 64);
        assert(stdx.no_padding(QueryFilter));
    }
};

pub const QueryFilterFlags = packed struct(u32) {
    /// Whether the results are sorted by timestamp in chronological or reverse-chronological order.
    reversed: bool,
    padding: u31 = 0,

    comptime {
        assert(@sizeOf(QueryFilterFlags) == @sizeOf(u32));
        assert(@bitSizeOf(QueryFilterFlags) == @sizeOf(QueryFilterFlags) * 8);
    }
};

/// Filter used in both `get_account_transfers` and `get_account_balances`.
pub const AccountFilter = extern struct {
    /// The account id.
    account_id: u128,
    /// Filter by the `user_data_128` index.
    /// Use zero for no filter.
    user_data_128: u128,
    /// Filter by the `user_data_64` index.
    /// Use zero for no filter.
    user_data_64: u64,
    /// Filter by the `user_data_32` index.
    /// Use zero for no filter.
    user_data_32: u32,
    /// Query by the `code` index.
    /// Use zero for no filter.
    code: u16,

    reserved: [58]u8 = [_]u8{0} ** 58,
    /// The initial timestamp (inclusive).
    /// Use zero for no filter.
    timestamp_min: u64,
    /// The final timestamp (inclusive).
    /// Use zero for no filter.
    timestamp_max: u64,
    /// Maximum number of results that can be returned by this query.
    /// Must be greater than zero.
    limit: u32,
    /// Query flags.
    flags: AccountFilterFlags,

    comptime {
        assert(@sizeOf(AccountFilter) == 128);
        assert(stdx.no_padding(AccountFilter));
    }
};

pub const AccountFilterFlags = packed struct(u32) {
    /// Whether to include results where `debit_account_id` matches.
    debits: bool,
    /// Whether to include results where `credit_account_id` matches.
    credits: bool,
    /// Whether the results are sorted by timestamp in chronological or reverse-chronological order.
    reversed: bool,
    padding: u29 = 0,

    comptime {
        assert(@sizeOf(AccountFilterFlags) == @sizeOf(u32));
        assert(@bitSizeOf(AccountFilterFlags) == @sizeOf(AccountFilterFlags) * 8);
    }
};

pub const ChangeEventType = enum(u8) {
    single_phase = 0,
    two_phase_pending = 1,
    two_phase_posted = 2,
    two_phase_voided = 3,
    two_phase_expired = 4,
};

pub const ChangeEvent = extern struct {
    transfer_id: u128,
    transfer_amount: u128,
    transfer_pending_id: u128,
    transfer_user_data_128: u128,
    transfer_user_data_64: u64,
    transfer_user_data_32: u32,
    transfer_timeout: u32,
    transfer_code: u16,
    transfer_flags: TransferFlags,

    ledger: u32,
    type: ChangeEventType,
    reserved: [39]u8 = [_]u8{0} ** 39,

    debit_account_id: u128,
    debit_account_debits_pending: u128,
    debit_account_debits_posted: u128,
    debit_account_credits_pending: u128,
    debit_account_credits_posted: u128,
    debit_account_user_data_128: u128,
    debit_account_user_data_64: u64,
    debit_account_user_data_32: u32,
    debit_account_code: u16,
    debit_account_flags: AccountFlags,

    credit_account_id: u128,
    credit_account_debits_pending: u128,
    credit_account_debits_posted: u128,
    credit_account_credits_pending: u128,
    credit_account_credits_posted: u128,
    credit_account_user_data_128: u128,
    credit_account_user_data_64: u64,
    credit_account_user_data_32: u32,
    credit_account_code: u16,
    credit_account_flags: AccountFlags,

    timestamp: u64,
    transfer_timestamp: u64,
    debit_account_timestamp: u64,
    credit_account_timestamp: u64,

    comptime {
        assert(stdx.no_padding(ChangeEvent));
        // Each event has the size of one transfer + 2 accounts.
        assert(@sizeOf(ChangeEvent) == @sizeOf(Transfer) + (2 * @sizeOf(Account)));
        assert(@alignOf(ChangeEvent) == 16);
    }
};

pub const ChangeEventsFilter = extern struct {
    timestamp_min: u64,
    timestamp_max: u64,
    limit: u32,
    reserved: [44]u8 = [_]u8{0} ** 44,

    comptime {
        assert(stdx.no_padding(ChangeEventsFilter));
        assert(@sizeOf(ChangeEventsFilter) == 64);
    }
};

// Looking to make backwards incompatible changes here? Make sure to check release.zig for
// `release_triple_client_min`.
pub const Operation = enum(u8) {
    /// Operations exported by TigerBeetle:
    pulse = constants.vsr_operations_reserved + 0,

    // Deprecated operations not encoded as multi-batch:
    deprecated_create_accounts = constants.vsr_operations_reserved + 1,
    deprecated_create_transfers = constants.vsr_operations_reserved + 2,
    deprecated_lookup_accounts = constants.vsr_operations_reserved + 3,
    deprecated_lookup_transfers = constants.vsr_operations_reserved + 4,
    deprecated_get_account_transfers = constants.vsr_operations_reserved + 5,
    deprecated_get_account_balances = constants.vsr_operations_reserved + 6,
    deprecated_query_accounts = constants.vsr_operations_reserved + 7,
    deprecated_query_transfers = constants.vsr_operations_reserved + 8,

    get_change_events = constants.vsr_operations_reserved + 9,

    create_accounts = constants.vsr_operations_reserved + 10,
    create_transfers = constants.vsr_operations_reserved + 11,
    lookup_accounts = constants.vsr_operations_reserved + 12,
    lookup_transfers = constants.vsr_operations_reserved + 13,
    get_account_transfers = constants.vsr_operations_reserved + 14,
    get_account_balances = constants.vsr_operations_reserved + 15,
    query_accounts = constants.vsr_operations_reserved + 16,
    query_transfers = constants.vsr_operations_reserved + 17,
};

comptime {
    const target = builtin.target;

    if (target.os.tag != .linux and !target.isDarwin() and target.os.tag != .windows) {
        @compileError("linux, windows or macos is required for io");
    }

    // We require little-endian architectures everywhere for efficient network deserialization:
    if (target.cpu.arch.endian() != .little) {
        @compileError("big-endian systems not supported");
    }

    switch (builtin.mode) {
        .Debug, .ReleaseSafe => {},
        .ReleaseFast, .ReleaseSmall => @compileError("safety checks are required for correctness"),
    }
}
