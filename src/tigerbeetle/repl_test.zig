const std = @import("std");

const alloc = std.testing.allocator;

const vsr = @import("vsr");
const constants = vsr.constants;
const tb = vsr.tigerbeetle;
const MessageBus = vsr.message_bus.MessageBusReplica;

const repl = @import("./repl.zig");
const Parser = repl.Parser;

const null_printer = repl.Printer{
    .stderr = null,
    .stdout = null,
};

test "repl.zig: Parser single transfer successfully" {
    var tests = [_]struct {
        in: []const u8 = "",
        want: tb.Transfer,
    }{
        .{
            .in = "create_transfers id=1",
            .want = tb.Transfer{
                .id = 1,
                .debit_account_id = 0,
                .credit_account_id = 0,
                .amount = 0,
                .pending_id = 0,
                .user_data_128 = 0,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .timeout = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{},
                .timestamp = 0,
            },
        },
        .{
            .in =
            \\create_transfers id=32 amount=65 ledger=12 code=9999 pending_id=7
            \\ credit_account_id=2121 debit_account_id=77 user_data_128=2
            \\ user_data_64=3 user_data_32=4 flags=linked
            ,
            .want = tb.Transfer{
                .id = 32,
                .debit_account_id = 77,
                .credit_account_id = 2121,
                .amount = 65,
                .pending_id = 7,
                .user_data_128 = 2,
                .user_data_64 = 3,
                .user_data_32 = 4,
                .timeout = 0,
                .ledger = 12,
                .code = 9999,
                .flags = .{ .linked = true },
                .timestamp = 0,
            },
        },
        .{
            .in =
            \\create_transfers flags=
            \\ post_pending_transfer |
            \\ balancing_credit |
            \\ balancing_debit |
            \\ void_pending_transfer |
            \\ pending |
            \\ linked
            ,
            .want = tb.Transfer{
                .id = 0,
                .debit_account_id = 0,
                .credit_account_id = 0,
                .amount = 0,
                .pending_id = 0,
                .user_data_128 = 0,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .timeout = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{
                    .post_pending_transfer = true,
                    .balancing_credit = true,
                    .balancing_debit = true,
                    .void_pending_transfer = true,
                    .pending = true,
                    .linked = true,
                },
                .timestamp = 0,
            },
        },
    };

    for (tests) |t| {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();

        var statement = try Parser.parse_statement(
            &arena,
            t.in,
            null_printer,
        );

        try std.testing.expectEqual(statement.operation, .create_transfers);
        try std.testing.expectEqualSlices(u8, statement.arguments, std.mem.asBytes(&t.want));
    }
}

test "repl.zig: Parser multiple transfers successfully" {
    var tests = [_]struct {
        in: []const u8 = "",
        want: [2]tb.Transfer,
    }{
        .{
            .in = "create_transfers id=1 debit_account_id=2, id=2 credit_account_id = 1;",
            .want = [2]tb.Transfer{
                tb.Transfer{
                    .id = 1,
                    .debit_account_id = 2,
                    .credit_account_id = 0,
                    .amount = 0,
                    .pending_id = 0,
                    .user_data_128 = 0,
                    .user_data_64 = 0,
                    .user_data_32 = 0,
                    .timeout = 0,
                    .ledger = 0,
                    .code = 0,
                    .flags = .{},
                    .timestamp = 0,
                },
                tb.Transfer{
                    .id = 2,
                    .debit_account_id = 0,
                    .credit_account_id = 1,
                    .amount = 0,
                    .pending_id = 0,
                    .user_data_128 = 0,
                    .user_data_64 = 0,
                    .user_data_32 = 0,
                    .timeout = 0,
                    .ledger = 0,
                    .code = 0,
                    .flags = .{},
                    .timestamp = 0,
                },
            },
        },
    };

    for (tests) |t| {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();

        var statement = try Parser.parse_statement(
            &arena,
            t.in,
            null_printer,
        );

        try std.testing.expectEqual(statement.operation, .create_transfers);
        try std.testing.expectEqualSlices(u8, statement.arguments, std.mem.sliceAsBytes(&t.want));
    }
}

test "repl.zig: Parser single account successfully" {
    var tests = [_]struct {
        in: []const u8,
        want: tb.Account,
    }{
        .{
            .in = "create_accounts id=1",
            .want = tb.Account{
                .id = 1,
                .debits_pending = 0,
                .debits_posted = 0,
                .credits_pending = 0,
                .credits_posted = 0,
                .user_data_128 = 0,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .reserved = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{},
            },
        },
        .{
            .in =
            \\create_accounts id=32 credits_posted=344 ledger=12 credits_pending=18
            \\ code=9999 flags=linked | debits_must_not_exceed_credits debits_posted=3390
            \\ debits_pending=3212 user_data_128=2 user_data_64=3 user_data_32=4
            ,
            .want = tb.Account{
                .id = 32,
                .debits_pending = 3212,
                .debits_posted = 3390,
                .credits_pending = 18,
                .credits_posted = 344,
                .user_data_128 = 2,
                .user_data_64 = 3,
                .user_data_32 = 4,
                .reserved = 0,
                .ledger = 12,
                .code = 9999,
                .flags = .{ .linked = true, .debits_must_not_exceed_credits = true },
            },
        },
        .{
            .in =
            \\create_accounts flags=credits_must_not_exceed_debits|
            \\ linked|debits_must_not_exceed_credits id =1
            ,
            .want = tb.Account{
                .id = 1,
                .debits_pending = 0,
                .debits_posted = 0,
                .credits_pending = 0,
                .credits_posted = 0,
                .user_data_128 = 0,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .reserved = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{
                    .credits_must_not_exceed_debits = true,
                    .linked = true,
                    .debits_must_not_exceed_credits = true,
                },
            },
        },
    };

    for (tests) |t| {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();

        var statement = try Parser.parse_statement(
            &arena,
            t.in,
            null_printer,
        );

        try std.testing.expectEqual(statement.operation, .create_accounts);
        try std.testing.expectEqualSlices(u8, statement.arguments, std.mem.asBytes(&t.want));
    }
}

test "repl.zig: Parser multiple accounts successfully" {
    var tests = [_]struct {
        in: []const u8,
        want: [2]tb.Account,
    }{
        .{
            .in = "create_accounts id=1, id=2",
            .want = [2]tb.Account{
                tb.Account{
                    .id = 1,
                    .debits_pending = 0,
                    .debits_posted = 0,
                    .credits_pending = 0,
                    .credits_posted = 0,
                    .user_data_128 = 0,
                    .user_data_64 = 0,
                    .user_data_32 = 0,
                    .reserved = 0,
                    .ledger = 0,
                    .code = 0,
                    .flags = .{},
                },
                tb.Account{
                    .id = 2,
                    .debits_pending = 0,
                    .debits_posted = 0,
                    .credits_pending = 0,
                    .credits_posted = 0,
                    .user_data_128 = 0,
                    .user_data_64 = 0,
                    .user_data_32 = 0,
                    .reserved = 0,
                    .ledger = 0,
                    .code = 0,
                    .flags = .{},
                },
            },
        },
    };

    for (tests) |t| {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();

        var statement = try Parser.parse_statement(
            &arena,
            t.in,
            null_printer,
        );

        try std.testing.expectEqual(statement.operation, .create_accounts);
        try std.testing.expectEqualSlices(u8, statement.arguments, std.mem.sliceAsBytes(&t.want));
    }
}

test "repl.zig: Parser odd but correct formatting" {
    var tests = [_]struct {
        in: []const u8 = "",
        want: tb.Transfer,
    }{
        // Space between key-value pair and equality
        .{
            .in = "create_transfers id = 1",
            .want = tb.Transfer{
                .id = 1,
                .debit_account_id = 0,
                .credit_account_id = 0,
                .amount = 0,
                .pending_id = 0,
                .user_data_128 = 0,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .timeout = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{},
                .timestamp = 0,
            },
        },
        // Space only before equals sign
        .{
            .in = "create_transfers id =1",
            .want = tb.Transfer{
                .id = 1,
                .debit_account_id = 0,
                .credit_account_id = 0,
                .amount = 0,
                .pending_id = 0,
                .user_data_128 = 0,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .timeout = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{},
                .timestamp = 0,
            },
        },
        // Whitespace before command
        .{
            .in = "  \t  \n  create_transfers id=1",
            .want = tb.Transfer{
                .id = 1,
                .debit_account_id = 0,
                .credit_account_id = 0,
                .amount = 0,
                .pending_id = 0,
                .user_data_128 = 0,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .timeout = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{},
                .timestamp = 0,
            },
        },
        // Trailing semicolon
        .{
            .in = "create_transfers id=1;",
            .want = tb.Transfer{
                .id = 1,
                .debit_account_id = 0,
                .credit_account_id = 0,
                .amount = 0,
                .pending_id = 0,
                .user_data_128 = 0,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .timeout = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{},
                .timestamp = 0,
            },
        },
        // Spaces everywhere
        .{
            .in =
            \\
            \\
            \\      create_transfers
            \\            id =    1
            \\       user_data_128 = 12
            \\ debit_account_id=1 credit_account_id        = 10
            \\    ;
            \\
            \\
            ,
            .want = tb.Transfer{
                .id = 1,
                .debit_account_id = 1,
                .credit_account_id = 10,
                .amount = 0,
                .pending_id = 0,
                .user_data_128 = 12,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .timeout = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{},
                .timestamp = 0,
            },
        },
    };

    for (tests) |t| {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();

        var statement = try Parser.parse_statement(
            &arena,
            t.in,
            null_printer,
        );

        try std.testing.expectEqual(statement.operation, .create_transfers);
        try std.testing.expectEqualSlices(u8, statement.arguments, std.mem.asBytes(&t.want));
    }
}

test "repl.zig: Handle parsing errors" {
    var tests = [_]struct {
        in: []const u8 = "",
        err: anyerror,
    }{
        .{
            .in = "create_trans",
            .err = error.BadOperation,
        },
        .{
            .in =
            \\
            \\
            \\ create
            ,
            .err = error.BadOperation,
        },
        .{
            .in = "create_transfers 12",
            .err = error.BadIdentifier,
        },
        .{
            .in = "create_transfers =12",
            .err = error.BadIdentifier,
        },
        .{
            .in = "create_transfers x",
            .err = error.MissingEqualBetweenKeyValuePair,
        },
        .{
            .in = "create_transfers x=",
            .err = error.BadValue,
        },
        .{
            .in = "create_transfers x=    ",
            .err = error.BadValue,
        },
        .{
            .in = "create_transfers x=    ;",
            .err = error.BadValue,
        },
        .{
            .in = "create_transfers x=[]",
            .err = error.BadValue,
        },
        .{
            .in = "create_transfers id=abcd",
            .err = error.BadKeyValuePair,
        },
    };

    for (tests) |t| {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();

        var result = Parser.parse_statement(
            &arena,
            t.in,
            null_printer,
        );
        try std.testing.expectError(t.err, result);
    }
}
