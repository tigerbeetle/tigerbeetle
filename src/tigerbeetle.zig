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
    /// A chart of accounts code describing the reason for the transfer (e.g. deposit, settlement):
    code: u32,
    flags: TransferFlags,
    amount: u64,
    timeout: u64,
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
    accept: bool = false,
    reject: bool = false,
    preimage: bool = false,
    padding: u28 = 0,

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
    exists,
    exists_with_different_user_data,
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
    reserved_flag_accept,
    reserved_flag_reject,
    debit_account_not_found,
    credit_account_not_found,
    accounts_are_the_same,
    accounts_have_different_units,
    amount_is_zero,
    exceeds_credits,
    exceeds_debits,
    timeout_reserved_for_two_phase_commit,
};

pub const CommitTransferResult = packed enum(u32) {
    ok,
    reserved_field,
    reserved_flag_padding,
    commit_must_accept_or_reject,
    commit_cannot_accept_and_reject,
    transfer_not_found,
    transfer_not_two_phase_commit,
    transfer_expired,
    already_auto_committed,
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

pub const CreateAccountResults = packed struct {
    index: u32,
    result: CreateAccountResult,

    comptime {
        assert(@sizeOf(CreateAccountResults) == 8);
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

pub const CreateTransferResults = packed struct {
    index: u32,
    result: CreateTransferResult,

    comptime {
        assert(@sizeOf(CreateTransferResults) == 8);
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

pub const CommitTransferResults = packed struct {
    index: u32,
    result: CommitTransferResult,

    comptime {
        assert(@sizeOf(CommitTransferResults) == 8);
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
    if (std.Target.current.os.tag != .linux) @compileError("linux required for io_uring");

    // We require little-endian architectures everywhere for efficient network deserialization:
    if (std.Target.current.cpu.arch.endian() != std.builtin.Endian.Little) {
        @compileError("big-endian systems not supported");
    }
}
