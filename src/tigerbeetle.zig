const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;

pub const config = @import("config.zig");

pub const Account = packed struct {
    id: u128,
    /// Opaque third-party identifier to link this account (many-to-one) to an external entity:
    user_data: u128,
    /// Reserved for accounting policy primitives:
    reserved: [48]u8,
    unit: u16,
    /// A chart of accounts code describing the type of account (e.g. clearing, settlement):
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
    padding: u29 = 0,

    comptime {
        assert(@sizeOf(AccountFlags) == @sizeOf(u32));
    }
};

pub const Transfer = packed struct {
    id: u128,
    debit_account_id: u128,
    credit_account_id: u128,
    /// Opaque third-party identifier to link this transfer (many-to-one) to an external entity:
    user_data: u128,
    /// Reserved for accounting policy primitives:
    reserved: [32]u8,
    timeout: u64,
    /// A chart of accounts code describing the reason for the transfer (e.g. deposit, settlement):
    code: u32,
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
    hashlock: bool = false,
    padding: u27 = 0,

    comptime {
        assert(@sizeOf(TransferFlags) == @sizeOf(u32));
    }
};

pub const CreateAccountResult = enum(u32) {
    ok,
    linked_event_failed,
    exists,
    exists_with_different_user_data,
    exists_with_different_reserved_field,
    exists_with_different_unit,
    exists_with_different_code,
    exists_with_different_flags,
    exceeds_credits,
    exceeds_debits,
    reserved_field,
    reserved_flag_padding,
};

pub const CreateTransferResult = enum(u32) {
    ok,
    linked_event_failed,
    exists,
    exists_with_different_debit_account_id,
    exists_with_different_credit_account_id,
    exists_with_different_user_data,
    exists_with_different_reserved_field,
    exists_with_different_code,
    exists_with_different_amount,
    exists_with_different_timeout,
    exists_with_different_flags,
    exists_and_already_committed_and_accepted,
    exists_and_already_committed_and_rejected,
    reserved_field,
    reserved_flag_padding,
    debit_account_not_found,
    credit_account_not_found,
    accounts_are_the_same,
    accounts_have_different_units,
    amount_is_zero,
    exceeds_credits,
    exceeds_debits,
    two_phase_commit_must_timeout,
    timeout_reserved_for_two_phase_commit,
    //TODO @jason Fields from the Commit
    cannot_void_and_post_two_phase_commit,
    transfer_not_found,
    transfer_not_two_phase_commit,
    already_committed_but_accepted,
    already_committed_but_rejected,
    already_committed,
    transfer_expired,
    condition_requires_preimage,
    preimage_invalid,
    preimage_requires_condition,
    debit_amount_was_not_reserved,
    credit_amount_was_not_reserved,
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
