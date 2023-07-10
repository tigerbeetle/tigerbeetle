const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;

pub const Account = extern struct {
    id: u128,
    /// Opaque third-party identifier to link this account (many-to-one) to an external entity.
    user_data: u128,
    /// Reserved for accounting policy primitives.
    reserved: [48]u8,
    ledger: u32,
    /// A chart of accounts code describing the type of account (e.g. clearing, settlement).
    code: u16,
    flags: AccountFlags,
    debits_pending: u64,
    debits_posted: u64,
    credits_pending: u64,
    credits_posted: u64,
    timestamp: u64 = 0,

    comptime {
        assert(@sizeOf(Account) == 128);
        assert(@bitSizeOf(Account) == @sizeOf(Account) * 8);
        assert(@alignOf(Account) == 16);
    }

    pub fn debits_exceed_credits(self: *const Account, amount: u64) bool {
        return (self.flags.debits_must_not_exceed_credits and
            self.debits_pending + self.debits_posted + amount > self.credits_posted);
    }

    pub fn credits_exceed_debits(self: *const Account, amount: u64) bool {
        return (self.flags.credits_must_not_exceed_debits and
            self.credits_pending + self.credits_posted + amount > self.debits_posted);
    }
};

pub const AccountFlags = packed struct {
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
    }
};

pub const Transfer = extern struct {
    id: u128,
    debit_account_id: u128,
    credit_account_id: u128,
    /// Opaque third-party identifier to link this transfer (many-to-one) to an external entity.
    user_data: u128,
    /// Reserved for accounting policy primitives.
    reserved: u128,
    /// If this transfer will post or void a pending transfer, the id of that pending transfer.
    pending_id: u128,
    timeout: u64,
    ledger: u32,
    /// A chart of accounts code describing the reason for the transfer (e.g. deposit, settlement).
    code: u16,
    flags: TransferFlags,
    amount: u64,
    timestamp: u64 = 0,

    comptime {
        assert(@sizeOf(Transfer) == 128);
        assert(@bitSizeOf(Transfer) == @sizeOf(Transfer) * 8);
        assert(@alignOf(Transfer) == 16);
    }
};

pub const TransferFlags = packed struct {
    linked: bool = false,
    pending: bool = false,
    post_pending_transfer: bool = false,
    void_pending_transfer: bool = false,
    balancing_debit: bool = false,
    balancing_credit: bool = false,
    padding: u10 = 0,

    comptime {
        assert(@sizeOf(TransferFlags) == @sizeOf(u16));
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

    reserved_flag = 4,
    reserved_field = 5,

    id_must_not_be_zero = 6,
    id_must_not_be_int_max = 7,

    flags_are_mutually_exclusive = 8,

    ledger_must_not_be_zero = 9,
    code_must_not_be_zero = 10,
    debits_pending_must_be_zero = 11,
    debits_posted_must_be_zero = 12,
    credits_pending_must_be_zero = 13,
    credits_posted_must_be_zero = 14,

    exists_with_different_flags = 15,
    exists_with_different_user_data = 16,
    exists_with_different_ledger = 17,
    exists_with_different_code = 18,
    exists = 19,

    comptime {
        for (std.enums.values(CreateAccountResult)) |result, index| {
            assert(@enumToInt(result) == index);
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
    reserved_field = 5,

    id_must_not_be_zero = 6,
    id_must_not_be_int_max = 7,

    flags_are_mutually_exclusive = 8,

    debit_account_id_must_not_be_zero = 9,
    debit_account_id_must_not_be_int_max = 10,
    credit_account_id_must_not_be_zero = 11,
    credit_account_id_must_not_be_int_max = 12,
    accounts_must_be_different = 13,

    pending_id_must_be_zero = 14,
    pending_id_must_not_be_zero = 15,
    pending_id_must_not_be_int_max = 16,
    pending_id_must_be_different = 17,
    timeout_reserved_for_pending_transfer = 18,

    ledger_must_not_be_zero = 19,
    code_must_not_be_zero = 20,
    amount_must_not_be_zero = 21,

    debit_account_not_found = 22,
    credit_account_not_found = 23,

    accounts_must_have_the_same_ledger = 24,
    transfer_must_have_the_same_ledger_as_accounts = 25,

    pending_transfer_not_found = 26,
    pending_transfer_not_pending = 27,

    pending_transfer_has_different_debit_account_id = 28,
    pending_transfer_has_different_credit_account_id = 29,
    pending_transfer_has_different_ledger = 30,
    pending_transfer_has_different_code = 31,

    exceeds_pending_transfer_amount = 32,
    pending_transfer_has_different_amount = 33,

    pending_transfer_already_posted = 34,
    pending_transfer_already_voided = 35,

    pending_transfer_expired = 36,

    exists_with_different_flags = 37,
    exists_with_different_debit_account_id = 38,
    exists_with_different_credit_account_id = 39,
    exists_with_different_pending_id = 40,
    exists_with_different_user_data = 41,
    exists_with_different_timeout = 42,
    exists_with_different_code = 43,
    exists_with_different_amount = 44,
    exists = 45,

    overflows_debits_pending = 46,
    overflows_credits_pending = 47,
    overflows_debits_posted = 48,
    overflows_credits_posted = 49,
    overflows_debits = 50,
    overflows_credits = 51,
    overflows_timeout = 52,

    exceeds_credits = 53,
    exceeds_debits = 54,

    comptime {
        for (std.enums.values(CreateTransferResult)) |result, index| {
            assert(@enumToInt(result) == index);
        }
    }
};

pub const CreateAccountsResult = extern struct {
    index: u32,
    result: CreateAccountResult,

    comptime {
        assert(@sizeOf(CreateAccountsResult) == 8);
        assert(@bitSizeOf(CreateAccountsResult) == @sizeOf(CreateAccountsResult) * 8);
    }
};

pub const CreateTransfersResult = extern struct {
    index: u32,
    result: CreateTransferResult,

    comptime {
        assert(@sizeOf(CreateTransfersResult) == 8);
        assert(@bitSizeOf(CreateTransfersResult) == @sizeOf(CreateTransfersResult) * 8);
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
