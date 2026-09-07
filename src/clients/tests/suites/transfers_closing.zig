const ct = @import("../conformance_test_api.zig");

test "closes both accounts with a pending closing transfer" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 1, .flags = .{ .history = true } },
        .{ .id = account_2_id, .ledger = 1, .code = 1, .flags = .{ .history = true } },
    });

    const results = ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 0,
            .ledger = 1,
            .code = 1,
            .flags = .{ .pending = true, .closing_debit = true, .closing_credit = true },
        },
    });

    ct.assert_equal(results, .{.{ .status = .created }});

    const accounts = ct.lookup_accounts(.{ account_1_id, account_2_id });
    ct.assert_equal(accounts, .{
        .{ .id = account_1_id, .flags = .{ .history = true, .closed = true } },
        .{ .id = account_2_id, .flags = .{ .history = true, .closed = true } },
    });
}

test "reopens both accounts when the closing transfer is voided" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    const closing_transfer_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 1, .flags = .{ .history = true } },
        .{ .id = account_2_id, .ledger = 1, .code = 1, .flags = .{ .history = true } },
    });
    ct.create_transfers(.{
        .{
            .id = closing_transfer_id,
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 0,
            .ledger = 1,
            .code = 1,
            .flags = .{ .pending = true, .closing_debit = true, .closing_credit = true },
        },
    });

    const results = ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 0,
            .pending_id = closing_transfer_id,
            .ledger = 1,
            .code = 1,
            .flags = .{ .void_pending_transfer = true },
        },
    });

    ct.assert_equal(results, .{.{ .status = .created }});

    const accounts = ct.lookup_accounts(.{ account_1_id, account_2_id });
    ct.assert_equal(accounts, .{
        .{ .id = account_1_id, .flags = .{ .history = true } },
        .{ .id = account_2_id, .flags = .{ .history = true } },
    });
}
