const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;

pub const config = @import("config.zig");

pub const Account = packed struct {
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

pub const Transfer = packed struct {
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
    }
};

pub const TransferFlags = packed struct {
    linked: bool = false,
    pending: bool = false,
    post_pending_transfer: bool = false,
    void_pending_transfer: bool = false,
    padding: u12 = 0,

    comptime {
        assert(@sizeOf(TransferFlags) == @sizeOf(u16));
    }
};

pub const CreateAccountResult = enum(u32) {
    ok,
    linked_event_failed,

    reserved_flag,
    reserved_field,

    id_must_not_be_zero,
    ledger_must_not_be_zero,
    code_must_not_be_zero,

    mutually_exclusive_flags,

    overflows_debits,
    overflows_credits,

    exceeds_credits,
    exceeds_debits,

    exists_with_different_flags,
    exists_with_different_user_data,
    exists_with_different_ledger,
    exists_with_different_code,
    exists_with_different_debits_pending,
    exists_with_different_debits_posted,
    exists_with_different_credits_pending,
    exists_with_different_credits_posted,
    exists,
};

pub const CreateTransferResult = enum(u32) {
    ok,
    linked_event_failed,

    reserved_flag,
    reserved_field,

    id_must_not_be_zero,
    debit_account_id_must_not_be_zero,
    credit_account_id_must_not_be_zero,
    accounts_must_be_different,

    pending_id_must_be_zero,
    pending_transfer_must_timeout,

    ledger_must_not_be_zero,
    code_must_not_be_zero,
    amount_must_not_be_zero,

    debit_account_not_found,
    credit_account_not_found,

    accounts_must_have_the_same_ledger,
    transfer_must_have_the_same_ledger_as_accounts,

    exists_with_different_flags,
    exists_with_different_debit_account_id,
    exists_with_different_credit_account_id,
    exists_with_different_user_data,
    exists_with_different_pending_id,
    exists_with_different_timeout,
    exists_with_different_code,
    exists_with_different_amount,
    exists,

    overflows_debits_pending,
    overflows_credits_pending,
    overflows_debits_posted,
    overflows_credits_posted,
    overflows_debits,
    overflows_credits,

    exceeds_credits,
    exceeds_debits,

    cannot_post_and_void_pending_transfer,
    pending_transfer_cannot_post_or_void_another,
    timeout_reserved_for_pending_transfer,

    pending_id_must_not_be_zero,
    pending_id_must_be_different,

    pending_transfer_not_found,
    pending_transfer_not_pending,

    pending_transfer_has_different_debit_account_id,
    pending_transfer_has_different_credit_account_id,
    pending_transfer_has_different_ledger,
    pending_transfer_has_different_code,

    exceeds_pending_transfer_amount,
    pending_transfer_has_different_amount,

    pending_transfer_already_posted,
    pending_transfer_already_voided,

    pending_transfer_expired,
};

pub const CreateAccountsResult = packed struct {
    index: u32,
    result: CreateAccountResult,

    comptime {
        assert(@sizeOf(CreateAccountsResult) == 8);
    }
};

pub const CreateTransfersResult = packed struct {
    index: u32,
    result: CreateTransferResult,

    comptime {
        assert(@sizeOf(CreateTransfersResult) == 8);
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
}
