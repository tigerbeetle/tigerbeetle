const std = @import("std");
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
    debits_reserved: u64,
    debits_accepted: u64,
    credits_reserved: u64,
    credits_accepted: u64,
    timestamp: u64 = 0,

    comptime {
        assert(@sizeOf(Account) == 128);
    }

    pub fn debits_exceed_credits(self: *const Account, amount: u64) bool {
        return (self.flags.debits_must_not_exceed_credits and
            self.debits_reserved + self.debits_accepted + amount > self.credits_accepted);
    }

    pub fn credits_exceed_debits(self: *const Account, amount: u64) bool {
        return (self.flags.credits_must_not_exceed_debits and
            self.credits_reserved + self.credits_accepted + amount > self.debits_accepted);
    }

    pub fn jsonStringify(self: Account, options: std.json.StringifyOptions, writer: anytype) !void {
        try writer.writeAll("{");
        try std.fmt.format(writer, "\"id\":{},", .{self.id});
        try std.fmt.format(writer, "\"user_data\":\"{x:0>32}\",", .{self.user_data});
        try std.fmt.format(writer, "\"reserved\":\"{x:0>48}\",", .{self.reserved});
        try std.fmt.format(writer, "\"unit\":{},", .{self.unit});
        try std.fmt.format(writer, "\"code\":{},", .{self.code});
        try writer.writeAll("\"flags\":");
        try std.json.stringify(self.flags, .{}, writer);
        try writer.writeAll(",");
        try std.fmt.format(writer, "\"debits_reserved\":{},", .{self.debits_reserved});
        try std.fmt.format(writer, "\"debits_accepted\":{},", .{self.debits_accepted});
        try std.fmt.format(writer, "\"credits_reserved\":{},", .{self.credits_reserved});
        try std.fmt.format(writer, "\"credits_accepted\":{},", .{self.credits_accepted});
        try std.fmt.format(writer, "\"timestamp\":\"{}\"", .{self.timestamp});
        try writer.writeAll("}");
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

    pub fn jsonStringify(
        self: AccountFlags,
        options: std.json.StringifyOptions,
        writer: anytype,
    ) !void {
        try writer.writeAll("{}");
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

    pub fn jsonStringify(
        self: Transfer,
        options: std.json.StringifyOptions,
        writer: anytype,
    ) !void {
        try writer.writeAll("{");
        try std.fmt.format(writer, "\"id\":{},", .{self.id});
        try std.fmt.format(writer, "\"debit_account_id\":{},", .{self.debit_account_id});
        try std.fmt.format(writer, "\"credit_account_id\":{},", .{self.credit_account_id});
        try std.fmt.format(writer, "\"user_data\":\"{x:0>32}\",", .{self.user_data});
        try std.fmt.format(writer, "\"reserved\":\"{x:0>64}\",", .{self.reserved});
        try std.fmt.format(writer, "\"code\":{},", .{self.code});
        try writer.writeAll("\"flags\":");
        try std.json.stringify(self.flags, .{}, writer);
        try writer.writeAll(",");
        try std.fmt.format(writer, "\"amount\":{},", .{self.amount});
        try std.fmt.format(writer, "\"timeout\":{},", .{self.timeout});
        try std.fmt.format(writer, "\"timestamp\":{}", .{self.timestamp});
        try writer.writeAll("}");
    }
};

pub const TransferFlags = packed struct {
    linked: bool = false,
    two_phase_commit: bool = false,
    condition: bool = false,
    padding: u29 = 0,

    comptime {
        assert(@sizeOf(TransferFlags) == @sizeOf(u32));
    }

    pub fn jsonStringify(
        self: TransferFlags,
        options: std.json.StringifyOptions,
        writer: anytype,
    ) !void {
        try writer.writeAll("{");
        try std.fmt.format(writer, "\"accept\":{},", .{self.accept});
        try std.fmt.format(writer, "\"reject\":{},", .{self.reject});
        try std.fmt.format(writer, "\"auto_commit\":{},", .{self.auto_commit});
        try std.fmt.format(writer, "\"condition\":{}", .{self.condition});
        try writer.writeAll("}");
    }
};

pub const Commit = packed struct {
    id: u128,
    /// Reserved for accounting policy primitives:
    reserved: [32]u8,
    /// A chart of accounts code describing the reason for the accept/reject:
    code: u32,
    flags: CommitFlags,
    timestamp: u64 = 0,

    comptime {
        assert(@sizeOf(Commit) == 64);
    }

    pub fn jsonStringify(
        self: Commit,
        options: std.json.StringifyOptions,
        writer: anytype,
    ) !void {
        try writer.writeAll("{");
        try std.fmt.format(writer, "\"id\":{},", .{self.id});
        try std.fmt.format(writer, "\"reserved\":\"{x:0>64}\",", .{self.reserved});
        try std.fmt.format(writer, "\"code\":{},", .{self.code});
        try writer.writeAll("\"flags\":");
        try std.json.stringify(self.flags, .{}, writer);
        try writer.writeAll(",");
        try std.fmt.format(writer, "\"timestamp\":{}", .{self.timestamp});
        try writer.writeAll("}");
    }
};

pub const CommitFlags = packed struct {
    linked: bool = false,
    reject: bool = false,
    preimage: bool = false,
    padding: u29 = 0,

    comptime {
        assert(@sizeOf(CommitFlags) == @sizeOf(u32));
    }

    pub fn jsonStringify(
        self: CommitFlags,
        options: std.json.StringifyOptions,
        writer: anytype,
    ) !void {
        try writer.writeAll("{");
        try std.fmt.format(writer, "\"accept\":{},", .{self.accept});
        try std.fmt.format(writer, "\"reject\":{},", .{self.reject});
        try std.fmt.format(writer, "\"preimage\":{}", .{self.preimage});
        try writer.writeAll("}");
    }
};

pub const CreateAccountResult = packed enum(u32) {
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

pub const CreateTransferResult = packed enum(u32) {
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
};

pub const CommitTransferResult = packed enum(u32) {
    ok,
    linked_event_failed,
    reserved_field,
    reserved_flag_padding,
    transfer_not_found,
    transfer_not_two_phase_commit,
    transfer_expired,
    already_committed,
    already_committed_but_accepted,
    already_committed_but_rejected,
    debit_account_not_found,
    credit_account_not_found,
    debit_amount_was_not_reserved,
    credit_amount_was_not_reserved,
    exceeds_credits,
    exceeds_debits,
    condition_requires_preimage,
    preimage_requires_condition,
    preimage_invalid,
};

pub const CreateAccountsResult = packed struct {
    index: u32,
    result: CreateAccountResult,

    comptime {
        assert(@sizeOf(CreateAccountsResult) == 8);
    }

    pub fn jsonStringify(
        self: CreateAccountResults,
        options: std.json.StringifyOptions,
        writer: anytype,
    ) !void {
        try writer.writeAll("{");
        try std.fmt.format(writer, "\"index\":{},", .{self.index});
        try std.fmt.format(writer, "\"result\":\"{}\"", .{@tagName(self.result)});
        try writer.writeAll("}");
    }
};

pub const CreateTransfersResult = packed struct {
    index: u32,
    result: CreateTransferResult,

    comptime {
        assert(@sizeOf(CreateTransfersResult) == 8);
    }

    pub fn jsonStringify(
        self: CreateTransferResults,
        options: std.json.StringifyOptions,
        writer: anytype,
    ) !void {
        try writer.writeAll("{");
        try std.fmt.format(writer, "\"index\":{},", .{self.index});
        try std.fmt.format(writer, "\"result\":\"{}\"", .{@tagName(self.result)});
        try writer.writeAll("}");
    }
};

pub const CommitTransfersResult = packed struct {
    index: u32,
    result: CommitTransferResult,

    comptime {
        assert(@sizeOf(CommitTransfersResult) == 8);
    }

    pub fn jsonStringify(
        self: CommitTransferResults,
        options: std.json.StringifyOptions,
        writer: anytype,
    ) !void {
        try writer.writeAll("{");
        try std.fmt.format(writer, "\"index\":{},", .{self.index});
        try std.fmt.format(writer, "\"result\":\"{}\"", .{@tagName(self.result)});
        try writer.writeAll("}");
    }
};

comptime {
    const target = std.Target.current;

    if (target.os.tag != .linux and !target.isDarwin()) {
        @compileError("linux or macos required for io");
    }

    // We require little-endian architectures everywhere for efficient network deserialization:
    if (target.cpu.arch.endian() != std.builtin.Endian.Little) {
        @compileError("big-endian systems not supported");
    }
}
