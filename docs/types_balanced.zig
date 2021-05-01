/// Accounting = Accounting Books + Accounting Policy (designed and enforced by the accountant)
/// TigerBeetle = Accounting books + Accounting policy (allows operator to declare invariants)

/// Goals:
///
/// 1. Improve the product experience, get away from polymorphism.
/// At the same time, improve the way TigerBeetle integrates with the greater accounting system.
///
/// 2. Simplify and reduce the size of data structures for performance, and reduced memory use.
/// At the same time, balance this with the accounting policy features we want to support.
///
/// 3. Guide the operator towards the right way of doing things.
///
/// What is accounting policy? (double-entry, immutability, closing transfers, net debit balance limit).
/// Have an eye on the future (micropayments, distributed systems, Interledger).
/// TigerBeetle fills the void for high-volume low-value payments.
/// TigerBeetle is also digital infrastructure, which means we're not just pen and paper.
/// We also want to make distributed accounting policy easy: two-phase commit transfers.
/// We can't imagine TigerBeetle without first-class support for Interledger.

/// 64 bytes:
/// Saves 64 bytes compared to the 128 byte struct we had before.
/// Reduces number of fields from 15 fields to 8 fields.
/// We drop support for arbritrary net debit/credit balance limits.
/// Net debit/credit balance limits are still supported and activated by flag.
/// However, these now simply enforce a limit of 0 on the net balance.
/// This guides operators towards prepaid accounts where they need NDC > 0, i.e. some risk.
/// The advantage of prepaid accounts is auditability, every limit change becomes a transfer.
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

/// 96 bytes (64 + 32)
/// Saves 32 bytes compared to the 128 byte struct we had before.
/// Reduces number of fields from 10 fields to 9 fields.
/// No loss of any accounting policy features.
///
/// Adds a `description`, essential for the classic journal entry tuple (date, description, amount):
/// We would have done this with a flag and a `user_data` slot, but this way we avoid polymorphism.
///
/// We swap transfers to being auto-commit by default, so two-phase commit transfers are explicit.
pub const Transfer = extern struct {
    id: u128,
    debit_account_id: u128,
    credit_account_id: u128,
    user_data: [16]u8, // 128-bit: An ILPv4 condition (truncated), or third-party UUID.
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
    user_data: [32]u8, // 256-bit: An ILPv4 preimage, or third-party UUIDs.
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
