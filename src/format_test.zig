const std = @import("std");
usingnamespace @import("tigerbeetle.zig");

pub fn main() !void {
    // TODO We need to move to hex for struct ids as well, since these will usually be random.
    // Otherwise, we risk overflowing the JSON integer size limit.

    // TODO Move this file into tigerbeetle.zig as literal tests:
    // See std.json for examples of how to test jsonStringify methods.
    // That way, running "zig test tigerbeetle.zig" will test these automatically.

    // TODO Update demo.zig's send() method to use std.json.stringify.

    // TODO Update demo.zig to write to std out.
    // e.g.
    // var writer = std.io.getStdOut().writer();
    // try std.json.stringify(transfer, .{}, writer);
    var command = Command.ack;

    var network = NetworkHeader{
        .checksum_meta = 76567523,
        .checksum_data = 2345456373567,
        .id = 87236482354976,
        .command = Command.ack,
        .size = 76253,
    };

    var account = Account{
        .id = 1,
        .custom = 10,
        .flags = .{},
        .unit = 710, // Let's use the ISO-4217 Code Number for ZAR
        .debit_reserved = 0,
        .debit_accepted = 0,
        .credit_reserved = 0,
        .credit_accepted = 0,
        .debit_reserved_limit = 100_000,
        .debit_accepted_limit = 1_000_000,
        .credit_reserved_limit = 0,
        .credit_accepted_limit = 0,
    };

    var transfer = Transfer{
        .id = 1000,
        .debit_account_id = 1,
        .credit_account_id = 2,
        .custom_1 = 0,
        .custom_2 = 0,
        .custom_3 = 0,
        .flags = .{
            .accept = true,
            .auto_commit = true,
        },
        .amount = 1000,
        .timeout = 0,
    };

    try std.json.stringify(command, .{}, std.io.getStdErr().writer());
    std.debug.print("\n\n", .{});

    try std.json.stringify(network, .{}, std.io.getStdErr().writer());
    std.debug.print("\n\n", .{});

    try std.json.stringify(account, .{}, std.io.getStdErr().writer());
    std.debug.print("\n\n", .{});

    try std.json.stringify(transfer, .{}, std.io.getStdErr().writer());
    std.debug.print("\n\n", .{});
}
