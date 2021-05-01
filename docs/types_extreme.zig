/// 64 bytes:
pub const Account = extern struct {
    id: u128,
    flags: u32,
    unit_of_value: u32,
    debits_reserved: u64,
    debits_accepted: u64,
    credits_reserved: u64,
    credits_accepted: u64,
    timestamp: u64 = 0,
};

/// 64 bytes:
/// We lose almost all accounting policy: closing transfers, timeouts, conditions, descriptions.
pub const Transfer = extern struct {
    id: u128,
    debit_account_id: u128,
    credit_account_id: u128,
    flags: u32,
    amount: u32,
    timestamp: u64 = 0,
};

/// 28 bytes (which the compiler would pad to 32 bytes):
pub const Commit = extern struct {
    id: u128,
    flags: u32,
    timestamp: u64 = 0,
};

test "sizeOf" {
    const std = @import("std");
    std.debug.print("\n", .{});
    std.debug.print("sizeOf(Account)={}\n", .{@sizeOf(Account)});
    std.debug.print("sizeOf(Transfer)={}\n", .{@sizeOf(Transfer)});
    std.debug.print("sizeOf(Commit)={}\n", .{@sizeOf(Commit)});
}
