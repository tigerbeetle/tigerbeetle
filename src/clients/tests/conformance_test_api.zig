//! This file specifies the DSL allowed in conformance suite files (`suites/*.zig`).
//!
//! It serves both as documentation, and as a way to silence LSP warnings regarding calls to unknown
//! functions.
//!
//! Only `U128`, `Account`, and `Transfer` can be bound with `const`, we had no need for others yet.

const tb = @import("vsr").tigerbeetle;

pub const AccountBalance = tb.AccountBalance;
pub const AccountFilterFlags = tb.AccountFilterFlags;
pub const AccountFilter = tb.AccountFilter;
pub const AccountFlags = tb.AccountFlags;
pub const Account = tb.Account;
pub const CreateAccountResult = tb.CreateAccountResult;
pub const CreateTransferResult = tb.CreateTransferResult;
pub const QueryFilterFlags = tb.QueryFilterFlags;
pub const QueryFilter = tb.QueryFilter;
pub const Transfer = tb.Transfer;
pub const TransferFlags = tb.TransferFlags;

/// Transpiles to language-specific u128 implementations where applicable.
pub const U128 = struct { value: u128 };

const ct_compile_error = "conformance test suites are parsed, not compiled";

/// A runtime-generated TigerBeetle id.
pub fn generate_id() u128 {
    @panic(ct_compile_error);
}

/// `count` runtime-generated ids.
pub fn generate_ids(count: u32) []const u128 {
    _ = count;
    @panic(ct_compile_error);
}

pub fn increment(reference: u64, by: u64) u64 {
    _ = reference;
    _ = by;
    @panic(ct_compile_error);
}

pub fn decrement(reference: u64, by: u64) u64 {
    _ = reference;
    _ = by;
    @panic(ct_compile_error);
}

pub fn create_accounts(accounts: anytype) []const CreateAccountResult {
    _ = accounts;
    @panic(ct_compile_error);
}

pub fn create_transfers(transfers: anytype) []const CreateTransferResult {
    _ = transfers;
    @panic(ct_compile_error);
}

pub fn lookup_accounts(ids: anytype) []const Account {
    _ = ids;
    @panic(ct_compile_error);
}

pub fn lookup_transfers(ids: anytype) []const Transfer {
    _ = ids;
    @panic(ct_compile_error);
}

pub fn get_account_transfers(filter: AccountFilter) []const Transfer {
    _ = filter;
    @panic(ct_compile_error);
}

pub fn get_account_balances(filter: AccountFilter) []const AccountBalance {
    _ = filter;
    @panic(ct_compile_error);
}

pub fn query_accounts(filter: QueryFilter) []const Account {
    _ = filter;
    @panic(ct_compile_error);
}

pub fn query_transfers(filter: QueryFilter) []const Transfer {
    _ = filter;
    @panic(ct_compile_error);
}

pub fn close_client() void {
    @panic(ct_compile_error);
}

/// Runs `call` `n` times, one thread each, on the shared client.
pub fn concurrently(n: u32, call: anytype) void {
    _ = n;
    _ = call;
    @panic(ct_compile_error);
}

pub fn sleep_ms(ms: u32) void {
    _ = ms;
    @panic(ct_compile_error);
}

/// Restricts the case to clients whose integers are unbounded so we can test for things like u128
/// overflow.
pub fn requires_unbounded_integers() void {
    @panic(ct_compile_error);
}

/// Restricts the case to clients that can pass a fractional value where an integer is expected, so
/// we can test that it is rejected.
pub fn requires_fractional_amounts() void {
    @panic(ct_compile_error);
}

/// Compares only the fields listed in `expected`.
pub fn assert_equal(actual: anytype, expected: anytype) void {
    _ = actual;
    _ = expected;
    @panic(ct_compile_error);
}

pub fn assert_empty(actual: anytype) void {
    _ = actual;
    @panic(ct_compile_error);
}

pub fn assert_unique(ids: []const u128) void {
    _ = ids;
    @panic(ct_compile_error);
}

pub fn assert_ascending(ids: []const u128) void {
    _ = ids;
    @panic(ct_compile_error);
}

pub fn assert_greater_than(actual: anytype, value: anytype) void {
    _ = actual;
    _ = value;
    @panic(ct_compile_error);
}

/// The wrapped call must fail with any client error.
pub fn assert_fail(call: anytype) void {
    _ = call;
    @panic(ct_compile_error);
}
