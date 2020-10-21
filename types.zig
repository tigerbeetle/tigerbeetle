const std = @import("std");
const assert = std.debug.assert;
const builtin = std.builtin;
const crypto = std.crypto;
const mem = std.mem;
const testing = std.testing;

comptime {
    if (builtin.os.tag != .linux) @compileError("linux required for io_uring");
    // We require little-endian architectures everywhere for efficient network deserialization:
    if (builtin.endian != builtin.Endian.Little) @compileError("big-endian systems not supported");
}

pub const Command = packed enum(u32) {
    reserved,
    ack,
    create_accounts,
    create_transfers,
    commit_transfers,
};

pub const Account = packed struct {
                       id: u128,
                   custom: u128,
                    flags: u64,
                     unit: u64,
           debit_reserved: u64,
           debit_accepted: u64,
          credit_reserved: u64,
          credit_accepted: u64,
     debit_reserved_limit: u64,
     debit_accepted_limit: u64,
    credit_reserved_limit: u64,
    credit_accepted_limit: u64,
                  padding: u64,
                timestamp: u64,
};

pub const AccountResult = packed enum(u32) {
    ok,
    already_exists,
    reserved_field_custom,
    reserved_field_flags,
    reserved_field_padding,
    reserved_field_timestamp,
    debit_reserved_exceeds_debit_reserved_limit,
    debit_accepted_exceeds_debit_accepted_limit,
    credit_reserved_exceeds_credit_reserved_limit,
    credit_accepted_exceeds_credit_accepted_limit,
    debit_reserved_limit_exceeds_debit_accepted_limit,
    credit_reserved_limit_exceeds_credit_accepted_limit,
};

pub const Transfer = packed struct {
                   id: u128,
    source_account_id: u128,
    target_account_id: u128,
             custom_1: u128,
             custom_2: u128,
             custom_3: u128,
                flags: u64,
               amount: u64,
              timeout: u64,
            timestamp: u64,
};

pub const TransferFlag = enum(u64) {
    accept,
    reject,
    auto_commit,
};

pub const Commit = packed struct {
           id: u128,
     custom_1: u128,
     custom_2: u128,
     custom_3: u128,
        flags:  u64,
    timestamp:  u64,
};

pub const CommitFlag = enum(u64) {
    accept,
    reject,
};

pub const AccountResults = packed struct {
     index: u32,
    result: AccountResult,
};

pub const Magic: u64 = @byteSwap(u64, 0x0a_5ca1ab1e_bee11e); // "A scalable beetle..."

pub const Header = packed struct {
    checksum_meta: u128 = undefined,
    checksum_data: u128 = undefined,
               id: u128,
            magic: u64 = Magic,
          command: Command,
             size: u32,

    pub fn calculate_checksum_meta(self: *Header) u128 {
        const meta = @bitCast([@sizeOf(Header)]u8, self.*);
        var target: [32]u8 = undefined;
        const checksum_size = @sizeOf(@TypeOf(self.checksum_meta));
        assert(checksum_size == 16);
        crypto.hash.Blake3.hash(meta[checksum_size..], target[0..], .{});
        return @bitCast(u128, target[0..checksum_size].*);
    }

    pub fn calculate_checksum_data(self: *Header, data: []const u8) u128 {
        assert(@sizeOf(Header) + data.len == self.size);
        var target: [32]u8 = undefined;
        const checksum_size = @sizeOf(@TypeOf(self.checksum_data));
        assert(checksum_size == 16);
        crypto.hash.Blake3.hash(data[0..], target[0..], .{});
        return @bitCast(u128, target[0..checksum_size].*);
    }

    pub fn set_checksum_meta(self: *Header) void {
        self.checksum_meta = self.calculate_checksum_meta();
    }

    pub fn set_checksum_data(self: *Header, data: []const u8) void {
        self.checksum_data = self.calculate_checksum_data(data);
    }

    pub fn valid_checksum_meta(self: *Header) bool {
        return self.checksum_meta == self.calculate_checksum_meta();
    }

    pub fn valid_checksum_data(self: *Header, data: []const u8) bool {
        return self.checksum_data == self.calculate_checksum_data(data);
    }

    pub fn valid_size(self: *Header) bool {
        if (self.size < @sizeOf(Header)) return false;
        const data_size = self.size - @sizeOf(Header);
        const type_size: usize = switch (self.command) {
            .reserved => unreachable,
            .ack => @sizeOf(AccountResult),
            .create_accounts => @sizeOf(Account),
            .create_transfers => @sizeOf(Transfer),
            .commit_transfers => @sizeOf(Commit)
        };
        const min_count: usize = switch (self.command) {
            .reserved => 0,
            .ack => 0,
            .create_accounts => 1,
            .create_transfers => 1,
            .commit_transfers => 1
        };
        return (
            @mod(data_size, type_size) == 0 and
            @divExact(data_size, type_size) >= min_count
        );
    }
};

test "Magic" {
    testing.expectEqualSlices(
        u8,
        ([_]u8{ 0x0a, 0x5c, 0xa1, 0xab, 0x1e, 0xbe, 0xe1, 0x1e })[0..],
        mem.toBytes(Magic)[0..]
    );
}

test "Header" {
    testing.expectEqual(@as(usize, 64), @sizeOf(Header));
}
