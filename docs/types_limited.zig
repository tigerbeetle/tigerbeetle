/// 64 bytes:
/// Saves 64 bytes compared to the 128 byte struct we had before.
/// Reduces number of fields from 15 fields to 8 fields.
/// We drop support for arbritrary net debit/credit balance limits.
/// Net debit/credit balance limits are still supported and activated by flag.
/// However, these now simply enforce a limit of 0 on the net balance.
/// This guides operators towards prepaid accounts where they need NDC > 0, i.e. some risk.
/// The advantage of prepaid accounts is auditability, every limit change becomes a transfer.
/// Cons: No way to give an account a name, or link an account with an external system.
/// Cons: No way to easily do non-zero NDCs where these are essential.
/// Cons: No way to capture the IDs for a composite account: A payable to B.
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

/// 128 bytes
/// Three custom slots are replaced with two user_data slots.
/// There's no longer a need for a helper to splice 256-bit data across two 128-bit slots.
///
/// Adds a `description`, essential for the classic journal entry tuple (date, description, amount):
/// We would have done this with a flag and a `user_data` slot, but this way we avoid polymorphism.
///
/// We swap transfers to being auto-commit by default, so two-phase commit transfers are explicit.
pub const Transfer = extern struct {
    id: u128,
    debit_account_id: u128,
    credit_account_id: u128,
    user_data_128: [16]u8, // 128-bit: A third-party UUID to connect into another system.
    user_data_256: [32]u8, // 256-bit: An ILPv4 condition.
    flags: u32,
    description: u32, // A system inventory code describing the reason for the transfer.
    amount: u64,
    timeout: u64,
    timestamp: u64 = 0,
};

/// 64 bytes
/// Saves 16 bytes compared to 80 byte struct we had before.
/// Reduces number of fields from 6 fields to 5 fields.
pub const Commit = extern struct {
    id: u128,
    user_data_256: [32]u8, // 256-bit: An ILPv4 preimage, or third-party UUIDs.
    flags: u32,
    description: u32, // A system inventory code describing the reason for the accept or reject.
    timestamp: u64 = 0,
};

test "sizeOf" {
    const std = @import("std");
    std.debug.print("\n", .{});
    std.debug.print("sizeOf(Account)={}\n", .{@sizeOf(Account)});
    std.debug.print("sizeOf(Transfer)={}\n", .{@sizeOf(Transfer)});
    std.debug.print("sizeOf(Commit)={}\n", .{@sizeOf(Commit)});
}
