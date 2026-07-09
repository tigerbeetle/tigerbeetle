//! This file specifies the DSL allowed in conformance suite files (`suites/*.zig`).
//!
//! Suite files import this module as `ct` and are parsed, never compiled or run. These stubs
//! keep the corpus `zig ast-check`-clean and give go-to-definition and hover documentation
//! in the editor. The generator decides what the calls mean.

const tb = @import("../../tigerbeetle.zig");

pub const Account = tb.Account;
pub const Transfer = tb.Transfer;
pub const AccountBalance = tb.AccountBalance;
pub const AccountFilter = tb.AccountFilter;
pub const QueryFilter = tb.QueryFilter;
pub const CreateAccountResult = tb.CreateAccountResult;
pub const CreateTransferResult = tb.CreateTransferResult;

const ct_run_error = "attempted to run conformance test as Zig code";

/// A runtime-generated TigerBeetle id.
pub fn generate_id() u128 {
    @panic(ct_run_error);
}

/// `count` runtime-generated ids.
pub fn generate_ids(count: u32) []const u128 {
    _ = count;
    @panic(ct_run_error);
}

pub fn create_accounts(accounts: anytype) []const CreateAccountResult {
    _ = accounts;
    @panic(ct_run_error);
}

pub fn create_transfers(transfers: anytype) []const CreateTransferResult {
    _ = transfers;
    @panic(ct_run_error);
}

pub fn lookup_accounts(ids: anytype) []const Account {
    _ = ids;
    @panic(ct_run_error);
}

pub fn lookup_transfers(ids: anytype) []const Transfer {
    _ = ids;
    @panic(ct_run_error);
}

pub fn get_account_transfers(filter: AccountFilter) []const Transfer {
    _ = filter;
    @panic(ct_run_error);
}

pub fn get_account_balances(filter: AccountFilter) []const AccountBalance {
    _ = filter;
    @panic(ct_run_error);
}

pub fn query_accounts(filter: QueryFilter) []const Account {
    _ = filter;
    @panic(ct_run_error);
}

pub fn query_transfers(filter: QueryFilter) []const Transfer {
    _ = filter;
    @panic(ct_run_error);
}

pub fn close_client() void {
    @panic(ct_run_error);
}

/// Runs `call` `n` times, one thread each, on the shared client.
pub fn concurrently(n: u32, call: anytype) void {
    _ = n;
    _ = call;
    @panic(ct_run_error);
}

/// Compares only the fields listed in `expected`.
pub fn assert_equal(actual: anytype, expected: anytype) void {
    _ = actual;
    _ = expected;
    @panic(ct_run_error);
}

pub fn assert_empty(actual: anytype) void {
    _ = actual;
    @panic(ct_run_error);
}

pub fn assert_unique(ids: []const u128) void {
    _ = ids;
    @panic(ct_run_error);
}

pub fn assert_ascending(ids: []const u128) void {
    _ = ids;
    @panic(ct_run_error);
}

/// The wrapped call must fail with any client error.
pub fn assert_fail(call: anytype) void {
    _ = call;
    @panic(ct_run_error);
}
