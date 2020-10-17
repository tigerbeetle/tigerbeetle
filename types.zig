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

pub const TransferFlag = packed enum(u64) {
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

pub const CommitFlag = packed enum(u64) {
    accept,
    reject,
};

pub const NetworkMagic: u64 = @byteSwap(u64, 0x0a_5ca1ab1e_bee11e); // "A scalable beetle..."

pub const NetworkHeader = packed struct {
    checksum_meta: u128 = undefined,
    checksum_data: u128 = undefined,
               id: u128,
            magic: u64 = NetworkMagic,
          command: Command,
        data_size: u32,

    pub fn calculate_checksum_meta(self: *NetworkHeader) u128 {
        const meta = @bitCast([@sizeOf(NetworkHeader)]u8, self.*);
        var target: [32]u8 = undefined;
        const checksum_size = @sizeOf(@TypeOf(self.checksum_meta));
        assert(checksum_size == 16);
        crypto.hash.Blake3.hash(meta[checksum_size..], target[0..], .{});
        return @bitCast(u128, target[0..checksum_size].*);
    }

    pub fn calculate_checksum_data(self: *NetworkHeader, data: []const u8) u128 {
        assert(data.len == self.data_size);
        var target: [32]u8 = undefined;
        const checksum_size = @sizeOf(@TypeOf(self.checksum_data));
        assert(checksum_size == 16);
        crypto.hash.Blake3.hash(data[0..], target[0..], .{});
        return @bitCast(u128, target[0..checksum_size].*);
    }

    pub fn set_checksum_meta(self: *NetworkHeader) void {
        self.checksum_meta = self.calculate_checksum_meta();
    }

    pub fn set_checksum_data(self: *NetworkHeader, data: []const u8) void {
        self.checksum_data = self.calculate_checksum_data(data);
    }

    pub fn valid_checksum_meta(self: *NetworkHeader) bool {
        return self.checksum_meta == self.calculate_checksum_meta();
    }

    pub fn valid_checksum_data(self: *NetworkHeader, data: []const u8) bool {
        return self.checksum_data == self.calculate_checksum_data(data);
    }
};

test "NetworkMagic" {
    testing.expectEqualSlices(
        u8,
        ([_]u8{ 0x0a, 0x5c, 0xa1, 0xab, 0x1e, 0xbe, 0xe1, 0x1e })[0..],
        mem.toBytes(NetworkMagic)[0..]
    );
}

test "NetworkHeader" {
    testing.expectEqual(@as(usize, 64), @sizeOf(NetworkHeader));
}
