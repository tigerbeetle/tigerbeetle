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
pub const uint128_max = 340282366920938463463374607431768211455;

/// Client errors for `assert_fail_with`. Emitters map them for the target language.
pub const ClientError = enum {
    too_much_data,
    client_closed,
    address_invalid,
    address_limit_exceeded,
    cluster_id_invalid,
};

const ct_compile_error = "conformance test suites are parsed, not compiled";

/// Restricts the case to clients whose integers are unbounded so we can test for things like u128
/// overflow.
pub fn requires_unbounded_integers() void {
    @compileError(ct_compile_error);
}

/// Restricts the case to clients that can pass a fractional value where an integer is expected, so
/// we can test that it is rejected.
pub fn requires_fractional_amounts() void {
    @compileError(ct_compile_error);
}

/// The client the test harness creates for each case.
/// NOTE: This doesn't do anything in the emitters. I just tried to avoid having an implicit client
/// magically available in all test.
pub const default_client: Client = .{};

/// Used for client construction tests. Creates a client and closes it again.
pub fn create_client(arguments: struct {
    cluster_id: u128,
    replica_addresses: []const u8,
}) void {
    _ = arguments;
    @compileError(ct_compile_error);
}

/// The test server's address, which only exists at runtime.
pub fn replica_addresses() []const u8 {
    @compileError(ct_compile_error);
}

/// A runtime-generated TigerBeetle id.
pub fn generate_id() u128 {
    @compileError(ct_compile_error);
}

/// `count` runtime-generated ids, generated across several clock ticks. See
/// `conformance_test_ast.ids_between_sleeps`.
pub fn generate_ids(count: u32) []const u128 {
    _ = count;
    @compileError(ct_compile_error);
}

pub fn increment(reference: u64, by: u64) u64 {
    _ = reference;
    _ = by;
    @compileError(ct_compile_error);
}

pub fn decrement(reference: u64, by: u64) u64 {
    _ = reference;
    _ = by;
    @compileError(ct_compile_error);
}

/// Runs `call` `n` times, one thread each, on the shared client.
pub fn concurrently(n: u32, call: anytype) void {
    _ = n;
    _ = call;
    @compileError(ct_compile_error);
}

pub fn sleep_ms(ms: u32) void {
    _ = ms;
    @compileError(ct_compile_error);
}

/// Compares only the fields listed in `expected`.
pub fn assert_equal(actual: anytype, expected: anytype) void {
    _ = actual;
    _ = expected;
    @compileError(ct_compile_error);
}

pub fn assert_empty(actual: anytype) void {
    _ = actual;
    @compileError(ct_compile_error);
}

pub fn assert_ascending(ids: []const u128) void {
    _ = ids;
    @compileError(ct_compile_error);
}

pub fn assert_greater_than(actual: anytype, value: anytype) void {
    _ = actual;
    _ = value;
    @compileError(ct_compile_error);
}

/// The wrapped call must fail with any client error.
pub fn assert_fail(call: anytype) void {
    _ = call;
    @compileError(ct_compile_error);
}

/// The wrapped call must fail with a specific client error.
pub fn assert_fail_with(call: anytype, client_error: ClientError) void {
    _ = call;
    _ = client_error;
    @compileError(ct_compile_error);
}

pub const Client = struct {
    pub fn create_accounts(client: Client, accounts: anytype) []const CreateAccountResult {
        _ = client;
        _ = accounts;
        @compileError(ct_compile_error);
    }

    pub fn create_transfers(client: Client, transfers: anytype) []const CreateTransferResult {
        _ = client;
        _ = transfers;
        @compileError(ct_compile_error);
    }

    pub fn lookup_accounts(client: Client, ids: anytype) []const Account {
        _ = client;
        _ = ids;
        @compileError(ct_compile_error);
    }

    pub fn lookup_transfers(client: Client, ids: anytype) []const Transfer {
        _ = client;
        _ = ids;
        @compileError(ct_compile_error);
    }

    pub fn get_account_transfers(client: Client, filter: AccountFilter) []const Transfer {
        _ = client;
        _ = filter;
        @compileError(ct_compile_error);
    }

    pub fn get_account_balances(client: Client, filter: AccountFilter) []const AccountBalance {
        _ = client;
        _ = filter;
        @compileError(ct_compile_error);
    }

    pub fn query_accounts(client: Client, filter: QueryFilter) []const Account {
        _ = client;
        _ = filter;
        @compileError(ct_compile_error);
    }

    pub fn query_transfers(client: Client, filter: QueryFilter) []const Transfer {
        _ = client;
        _ = filter;
        @compileError(ct_compile_error);
    }

    pub fn close_client(client: Client) void {
        _ = client;
        @compileError(ct_compile_error);
    }
};
