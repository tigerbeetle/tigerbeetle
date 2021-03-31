const std = @import("std");
const assert = std.debug.assert;
const builtin = std.builtin;
const crypto = std.crypto;
const mem = std.mem;
const StringifyOptions = std.json.StringifyOptions;

pub const config = @import("tigerbeetle.conf");

pub const Account = packed struct {
    id: u128,
    custom: u128,
    flags: AccountFlags,
    unit: u64,
    debit_reserved: u64,
    debit_accepted: u64,
    credit_reserved: u64,
    credit_accepted: u64,
    debit_reserved_limit: u64,
    debit_accepted_limit: u64,
    credit_reserved_limit: u64,
    credit_accepted_limit: u64,
    padding: u64 = 0,
    timestamp: u64 = 0,

    pub fn exceeds(balance: u64, amount: u64, limit: u64) bool {
        return limit > 0 and balance + amount > limit;
    }

    pub fn exceeds_debit_reserved_limit(self: *const Account, amount: u64) bool {
        return Account.exceeds(self.debit_reserved, amount, self.debit_reserved_limit);
    }

    pub fn exceeds_debit_accepted_limit(self: *const Account, amount: u64) bool {
        return Account.exceeds(self.debit_accepted, amount, self.debit_accepted_limit);
    }

    pub fn exceeds_credit_reserved_limit(self: *const Account, amount: u64) bool {
        return Account.exceeds(self.credit_reserved, amount, self.credit_reserved_limit);
    }

    pub fn exceeds_credit_accepted_limit(self: *const Account, amount: u64) bool {
        return Account.exceeds(self.credit_accepted, amount, self.credit_accepted_limit);
    }

    pub fn jsonStringify(self: Account, options: StringifyOptions, writer: anytype) !void {
        try writer.writeAll("{");
        try std.fmt.format(writer, "\"id\":{},", .{self.id});
        try std.fmt.format(writer, "\"custom\":\"{x:0>32}\",", .{self.custom});
        try writer.writeAll("\"flags\":");
        try std.json.stringify(self.flags, .{}, writer);
        try writer.writeAll(",");
        try std.fmt.format(writer, "\"unit\":{},", .{self.unit});
        try std.fmt.format(writer, "\"debit_reserved\":{},", .{self.debit_reserved});
        try std.fmt.format(writer, "\"debit_accepted\":{},", .{self.debit_accepted});
        try std.fmt.format(writer, "\"credit_reserved\":{},", .{self.credit_reserved});
        try std.fmt.format(writer, "\"credit_accepted\":{},", .{self.credit_accepted});
        try std.fmt.format(writer, "\"debit_reserved_limit\":{},", .{self.debit_reserved_limit});
        try std.fmt.format(writer, "\"debit_accepted_limit\":{},", .{self.debit_accepted_limit});
        try std.fmt.format(writer, "\"credit_reserved_limit\":{},", .{self.credit_reserved_limit});
        try std.fmt.format(writer, "\"credit_accepted_limit\":{},", .{self.credit_accepted_limit});
        try std.fmt.format(writer, "\"timestamp\":\"{}\"", .{self.timestamp});
        try writer.writeAll("}");
    }
};

pub const AccountFlags = packed struct {
    padding: u64 = 0,

    pub fn jsonStringify(self: AccountFlags, options: StringifyOptions, writer: anytype) !void {
        try writer.writeAll("{}");
    }
};

pub const Transfer = packed struct {
    id: u128,
    debit_account_id: u128,
    credit_account_id: u128,
    custom_1: u128,
    custom_2: u128,
    custom_3: u128,
    flags: TransferFlags,
    amount: u64,
    timeout: u64,
    timestamp: u64 = 0,

    pub fn jsonStringify(self: Transfer, options: StringifyOptions, writer: anytype) !void {
        try writer.writeAll("{");
        try std.fmt.format(writer, "\"id\":{},", .{self.id});
        try std.fmt.format(writer, "\"debit_account_id\":{},", .{self.debit_account_id});
        try std.fmt.format(writer, "\"credit_account_id\":{},", .{self.credit_account_id});
        try std.fmt.format(writer, "\"custom_1\":\"{x:0>32}\",", .{self.custom_1});
        try std.fmt.format(writer, "\"custom_2\":\"{x:0>32}\",", .{self.custom_2});
        try std.fmt.format(writer, "\"custom_3\":\"{x:0>32}\",", .{self.custom_3});
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
    accept: bool = false,
    reject: bool = false,
    auto_commit: bool = false,
    condition: bool = false,
    padding: u60 = 0,

    pub fn jsonStringify(self: TransferFlags, options: StringifyOptions, writer: anytype) !void {
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
    custom_1: u128,
    custom_2: u128,
    custom_3: u128,
    flags: CommitFlags,
    timestamp: u64 = 0,

    pub fn jsonStringify(self: Commit, options: StringifyOptions, writer: anytype) !void {
        try writer.writeAll("{");
        try std.fmt.format(writer, "\"id\":{},", .{self.id});
        try std.fmt.format(writer, "\"custom_1\":{},", .{self.custom_1});
        try std.fmt.format(writer, "\"custom_2\":{},", .{self.custom_2});
        try std.fmt.format(writer, "\"custom_3\":{},", .{self.custom_3});
        try writer.writeAll("\"flags\":");
        try std.json.stringify(self.flags, .{}, writer);
        try writer.writeAll(",");
        try std.fmt.format(writer, "\"timestamp\":{}", .{self.timestamp});
        try writer.writeAll("}");
    }
};

pub const CommitFlags = packed struct {
    accept: bool = false,
    reject: bool = false,
    preimage: bool = false,
    padding: u61 = 0,

    pub fn jsonStringify(self: CommitFlags, options: StringifyOptions, writer: anytype) !void {
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
    exists_with_different_unit,
    exists_with_different_limits,
    exists_with_different_custom_field,
    exists_with_different_flags,
    reserved_field_custom,
    reserved_field_padding,
    reserved_field_timestamp,
    reserved_flag_padding,
    exceeds_debit_reserved_limit,
    exceeds_debit_accepted_limit,
    exceeds_credit_reserved_limit,
    exceeds_credit_accepted_limit,
    debit_reserved_limit_exceeds_debit_accepted_limit,
    credit_reserved_limit_exceeds_credit_accepted_limit,
};

pub const CreateTransferResult = packed enum(u32) {
    ok,
    exists,
    exists_with_different_debit_account_id,
    exists_with_different_credit_account_id,
    exists_with_different_custom_fields,
    exists_with_different_amount,
    exists_with_different_timeout,
    exists_with_different_flags,
    exists_and_already_committed_and_accepted,
    exists_and_already_committed_and_rejected,
    reserved_field_custom,
    reserved_field_timestamp,
    reserved_flag_padding,
    reserved_flag_accept,
    reserved_flag_reject,
    debit_account_not_found,
    credit_account_not_found,
    accounts_are_the_same,
    accounts_have_different_units,
    amount_is_zero,
    exceeds_debit_reserved_limit,
    exceeds_debit_accepted_limit,
    exceeds_credit_reserved_limit,
    exceeds_credit_accepted_limit,
    auto_commit_must_accept,
    auto_commit_cannot_timeout,
};

pub const CommitTransferResult = packed enum(u32) {
    ok,
    reserved_field_custom,
    reserved_field_timestamp,
    reserved_flag_padding,
    commit_must_accept_or_reject,
    commit_cannot_accept_and_reject,
    transfer_not_found,
    transfer_expired,
    already_auto_committed,
    already_committed,
    already_committed_but_accepted,
    already_committed_but_rejected,
    debit_account_not_found,
    credit_account_not_found,
    debit_amount_was_not_reserved,
    credit_amount_was_not_reserved,
    exceeds_debit_accepted_limit,
    exceeds_credit_accepted_limit,
    condition_requires_preimage,
    preimage_requires_condition,
    preimage_invalid,
};

pub const CreateAccountResults = packed struct {
    index: u32,
    result: CreateAccountResult,

    pub fn jsonStringify(self: CreateAccountResults, options: StringifyOptions, writer: anytype) !void {
        try writer.writeAll("{");
        try std.fmt.format(writer, "\"index\":{},", .{ self.index });
        try std.fmt.format(writer, "\"result\":\"{}\"", .{ @tagName(self.result) });
        try writer.writeAll("}");
    }
};

pub const CreateTransferResults = packed struct {
    index: u32,
    result: CreateTransferResult,

    pub fn jsonStringify(self: CreateTransferResults, options: StringifyOptions, writer: anytype) !void {
        try writer.writeAll("{");
        try std.fmt.format(writer, "\"index\":{},", .{ self.index });
        try std.fmt.format(writer, "\"result\":\"{}\"", .{ @tagName(self.result) });
        try writer.writeAll("}");
    }
};

pub const CommitTransferResults = packed struct {
    index: u32,
    result: CommitTransferResult,

    pub fn jsonStringify(self: CommitTransferResults, options: StringifyOptions, writer: anytype) !void {
        try writer.writeAll("{");
        try std.fmt.format(writer, "\"index\":{},", .{ self.index });
        try std.fmt.format(writer, "\"result\":\"{}\"", .{ @tagName(self.result) });
        try writer.writeAll("}");
    }
};

comptime {
    if (builtin.os.tag != .linux) @compileError("linux required for io_uring");

    // We require little-endian architectures everywhere for efficient network deserialization:
    if (builtin.endian != builtin.Endian.Little) @compileError("big-endian systems not supported");
}

const testing = std.testing;

test "data structure sizes" {
    testing.expectEqual(@as(usize, 8), @sizeOf(AccountFlags));
    testing.expectEqual(@as(usize, 128), @sizeOf(Account));
    testing.expectEqual(@as(usize, 8), @sizeOf(TransferFlags));
    testing.expectEqual(@as(usize, 128), @sizeOf(Transfer));
    testing.expectEqual(@as(usize, 8), @sizeOf(CommitFlags));
    testing.expectEqual(@as(usize, 80), @sizeOf(Commit));
    testing.expectEqual(@as(usize, 8), @sizeOf(CreateAccountResults));
    testing.expectEqual(@as(usize, 8), @sizeOf(CreateTransferResults));
    testing.expectEqual(@as(usize, 8), @sizeOf(CommitTransferResults));
}
