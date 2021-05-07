/// Accounting = accounting books + accounting policy (designed and enforced by the accountant)
/// TigerBeetle = accounting books + accounting policy (allows the operator to declare invariants)
///
/// This is the key reason that TigerBeetle's Account struct is more than only credits and debits.
///
/// Goals:
///
/// 1. Improve the product experience, get away from polymorphism.
/// At the same time, improve the way TigerBeetle integrates with third party systems.
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

/// 128 bytes:
/// A 64-byte Account will not improve performance significantly, and drops too much functionality.
/// Reduces cognitive complexity by reducing the number of fields from 15 fields to 11 fields.
/// Enables referencing at least two third-party UUIDs, e.g. for tuple accounts: A Payable To B.
/// Enables referencing external entities, where multiple accounts reference the same entity.
pub const Account = extern struct {
    id: u128,
    user_data: u128, // Opaque, e.g. a third-party identifier to link this account (many-to-one) to an external entity.
    reserved: [48]u8,
    unit: u16, // This used to be 32-bits which was overkill, we've borrowed 16-bits from this to make space for a chart of accounts `code`.
    code: u16, // A chart of accounts code describing the type of the account (e.g. liquidity account, settlement account, payable account etc.).
    flags: u32,
    debits_reserved: u64,
    debits_accepted: u64,
    credits_reserved: u64,
    credits_accepted: u64,
    timestamp: u64 = 0,
};

/// 128 bytes:
/// Reduces the number of fields from 10 fields to 9 fields.
/// No loss of any accounting policy features.
///
///
/// Adds a `code`, essential for the classic journal entry tuple (date, description, amount):
/// We would have done this with a flag and a `user_data` slot, but this way we avoid polymorphism.
///
/// We swap transfers to being auto-commit by default, so two-phase commit transfers are explicit.
pub const Transfer = extern struct {
    id: u128,
    debit_account_id: u128,
    credit_account_id: u128,
    user_data: u128, // Opaque, e.g. a third-party identifier to link this transfer (many-to-one) to an external entity.
    reserved: [32]u8, // Reserved for TigerBeetle accounting primitives.
    code: u32, // A chart of accounts code describing the type of the transfer (e.g. deposit, settlement etc.).
    flags: u32,
    amount: u64,
    timeout: u64,
    timestamp: u64 = 0,
};

/// 64 bytes
/// Saves 16 bytes compared to 80 byte struct we had before.
/// Reduces the number of fields from 6 fields to 5 fields.
pub const Commit = extern struct {
    id: u128,
    reserved: [32]u8, // Reserved for TigerBeetle accounting primitives.
    code: u32, // A chart of accounts code describing the reason for the commit accept/reject.
    flags: u32,
    timestamp: u64 = 0,
};

test "sizeOf" {
    const std = @import("std");
    const assert = std.debug.assert;

    std.debug.print("\n", .{});
    std.debug.print("sizeOf(Account)={}\n", .{@sizeOf(Account)});
    std.debug.print("sizeOf(Transfer)={}\n", .{@sizeOf(Transfer)});
    std.debug.print("sizeOf(Commit)={}\n", .{@sizeOf(Commit)});

    assert(@sizeOf(Account) == 128);
    assert(@sizeOf(Transfer) == 128);
    assert(@sizeOf(Commit) == 64);
}
