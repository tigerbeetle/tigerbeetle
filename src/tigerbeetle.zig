const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;

const stdx = @import("stdx.zig");

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
    timestamp: u64 = 0,

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
    padding: u13 = 0,

    comptime {
        assert(@sizeOf(AccountFlags) == @sizeOf(u16));
        assert(@bitSizeOf(AccountFlags) == @sizeOf(AccountFlags) * 8);
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
    timestamp: u64 = 0,

    comptime {
        assert(stdx.no_padding(Transfer));
        assert(@sizeOf(Transfer) == 128);
        assert(@alignOf(Transfer) == 16);
    }
};

pub const TransferFlags = packed struct(u16) {
    linked: bool = false,
    pending: bool = false,
    post_pending_transfer: bool = false,
    void_pending_transfer: bool = false,
    balancing_debit: bool = false,
    balancing_credit: bool = false,
    padding: u10 = 0,

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
    timestamp_must_be_zero = 3,

    reserved_field = 4,
    reserved_flag = 5,

    id_must_not_be_zero = 6,
    id_must_not_be_int_max = 7,

    flags_are_mutually_exclusive = 8,

    debits_pending_must_be_zero = 9,
    debits_posted_must_be_zero = 10,
    credits_pending_must_be_zero = 11,
    credits_posted_must_be_zero = 12,
    ledger_must_not_be_zero = 13,
    code_must_not_be_zero = 14,

    exists_with_different_flags = 15,

    exists_with_different_user_data_128 = 16,
    exists_with_different_user_data_64 = 17,
    exists_with_different_user_data_32 = 18,
    exists_with_different_ledger = 19,
    exists_with_different_code = 20,
    exists = 21,

    comptime {
        for (std.enums.values(CreateAccountResult), 0..) |result, index| {
            assert(@intFromEnum(result) == index);
        }
    }
};

/// Error codes are ordered by descending precedence.
/// When errors do not have an obvious/natural precedence (e.g. "*_must_not_be_zero"),
/// the ordering matches struct field order.
pub const CreateTransferResult = enum(u32) {
    ok = 0,
    linked_event_failed = 1,
    linked_event_chain_open = 2,
    timestamp_must_be_zero = 3,

    reserved_flag = 4,

    id_must_not_be_zero = 5,
    id_must_not_be_int_max = 6,

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

    amount_must_not_be_zero = 18,
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

    exists_with_different_flags = 36,

    exists_with_different_debit_account_id = 37,
    exists_with_different_credit_account_id = 38,
    exists_with_different_amount = 39,
    exists_with_different_pending_id = 40,
    exists_with_different_user_data_128 = 41,
    exists_with_different_user_data_64 = 42,
    exists_with_different_user_data_32 = 43,
    exists_with_different_timeout = 44,
    exists_with_different_code = 45,
    exists = 46,

    overflows_debits_pending = 47,
    overflows_credits_pending = 48,
    overflows_debits_posted = 49,
    overflows_credits_posted = 50,
    overflows_debits = 51,
    overflows_credits = 52,
    overflows_timeout = 53,

    exceeds_credits = 54,
    exceeds_debits = 55,

    comptime {
        for (std.enums.values(CreateTransferResult), 0..) |result, index| {
            assert(@intFromEnum(result) == index);
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

pub const GetAccountTransfers = extern struct {
    /// The account id.
    account_id: u128,

    /// Use this field for pagination, transfers will be returned from this timestamp
    /// depending on the sort order.
    timestamp: u64,

    /// Maximum number of transfers that can be returned by this query.
    limit: u32,

    /// Query flags.
    flags: GetAccountTransfersFlags,

    comptime {
        assert(@sizeOf(GetAccountTransfers) == 32);
        assert(stdx.no_padding(GetAccountTransfers));
    }
};

pub const GetAccountTransfersFlags = packed struct(u32) {
    /// Whether to include debit transfers where `debit_account_id` matches.
    debits: bool,
    /// Whether to include credit transfers where `credit_account_id` matches.
    credits: bool,
    /// Whether the results are sorted by timestamp in chronological or reverse-chronological order.
    reversed: bool,
    padding: u29 = 0,

    comptime {
        assert(@sizeOf(GetAccountTransfersFlags) == @sizeOf(u32));
        assert(@bitSizeOf(GetAccountTransfersFlags) == @sizeOf(GetAccountTransfersFlags) * 8);
    }
};

comptime {
    const target = builtin.target;

    if (target.os.tag != .linux and !target.isDarwin() and target.os.tag != .windows) {
        @compileError("linux, windows or macos is required for io");
    }

    // We require little-endian architectures everywhere for efficient network deserialization:
    if (target.cpu.arch.endian() != .Little) {
        @compileError("big-endian systems not supported");
    }

    switch (builtin.mode) {
        .Debug, .ReleaseSafe => {},
        .ReleaseFast, .ReleaseSmall => @compileError("safety checks are required for correctness"),
    }
}
