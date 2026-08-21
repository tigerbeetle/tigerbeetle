const std = @import("std");
const stdx = @import("stdx");

const MiB = stdx.MiB;

pub const Formatter = @import("conformance_test_formatter.zig").Formatter;
pub const ast = @import("conformance_test_ast.zig");
const parser = @import("conformance_test_parser.zig");
const debug = @import("conformance_test_debug.zig");

const log = std.log;

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
    .{ "uint128_range", "uint128_range" },
    .{ "transfers_create_concurrent", "create_transfers_concurrent" },
    .{ "client_close", "close_client" },
};

var memory: [1 * MiB]u8 = undefined;
var fba = std.heap.FixedBufferAllocator.init(&memory);

pub fn main(init: std.process.Init) !void {
    const tests = try parse(allocator());

    const args = try init.minimal.args.toSlice(allocator());
    if (args.len > 1) {
        if (args.len > 2 or !std.mem.eql(u8, args[1], "--debug")) {
            log.err("usage: conformance_test [--debug]", .{});
            return error.InvalidArguments;
        }
        debug.dump(init.io, tests);
    }
}

/// Returns a fixed sized (1 MiB) buffer allocator for all emitters to use.
pub fn allocator() std.mem.Allocator {
    return fba.allocator();
}

pub fn parse(arena: std.mem.Allocator) !ast.ConformanceTests {
    return parser.parse(arena, suites);
}
