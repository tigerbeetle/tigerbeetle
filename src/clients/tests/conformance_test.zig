const std = @import("std");

pub const Printer = @import("conformance_test_printer.zig").Printer;
pub const ast = @import("conformance_test_ast.zig");
const parser = @import("conformance_test_parser.zig");

// In the generated test files, suites appear in the order defined here. We are trying to structure
// this like a walkthrough for readers of the conformance test suite. The second tuple item is used
// in the generated test identifiers e.g. test_get_account_balances_*.
pub const suites: []const struct { []const u8, []const u8 } = &.{
    .{ "ids_generate", "generate_ids" },
    .{ "accounts_create", "create_accounts" },
    .{ "accounts_lookup", "lookup_accounts" },
    .{ "transfers_create", "create_transfers" },
    .{ "transfers_lookup", "lookup_transfers" },
    .{ "account_transfers_get", "get_account_transfers" },
    .{ "account_balances_get", "get_account_balances" },
    .{ "accounts_query", "query_accounts" },
    .{ "transfers_query", "query_transfers" },
    .{ "transfers_two_phase", "two_phase_transfer" },
    .{ "transfers_closing", "closing_transfer" },
    .{ "accounts_import", "import_accounts" },
    .{ "transfers_import", "import_transfers" },
    .{ "uint128_range", "uint128_range" },
    .{ "transfers_create_concurrent", "create_transfers_concurrent" },
    .{ "client_close", "close_client" },
};

pub fn parse(arena: std.mem.Allocator) !ast.ConformanceTests {
    return parser.parse(arena, suites);
}
