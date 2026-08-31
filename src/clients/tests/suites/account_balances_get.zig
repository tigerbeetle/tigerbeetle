const ct = @import("../conformance_test_api.zig");

test "returns a balance per transfer for a history account" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 1, .flags = .{ .history = true } },
        .{ .id = account_2_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_2_id,
            .credit_account_id = account_1_id,
            .amount = 20,
            .ledger = 1,
            .code = 1,
        },
    });

    const balances = ct.get_account_balances(.{
        .account_id = account_1_id,
        .limit = 10,
        .flags = .{ .debits = true, .credits = true },
    });

    ct.assert_equal(balances, .{
        .{ .debits_posted = 10, .credits_posted = 0 },
        .{ .debits_posted = 10, .credits_posted = 20 },
    });
}

test "pairs each balance with the transfer that produced it" {
    const account_id = ct.generate_id();
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_id, .ledger = 1, .code = 1, .flags = .{ .history = true } },
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    const transfer_1_id = ct.generate_id();
    const transfer_2_id = ct.generate_id();
    const transfer_3_id = ct.generate_id();
    const transfer_4_id = ct.generate_id();
    ct.create_transfers(.{
        .{
            .id = transfer_1_id,
            .debit_account_id = account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_2_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = account_id,
            .amount = 20,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_3_id,
            .debit_account_id = account_id,
            .credit_account_id = credit_account_id,
            .amount = 30,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_4_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = account_id,
            .amount = 40,
            .ledger = 1,
            .code = 1,
        },
    });

    const transfers = ct.get_account_transfers(.{
        .account_id = account_id,
        .limit = 10,
        .flags = .{ .debits = true, .credits = true },
    });
    ct.assert_equal(transfers, .{
        .{ .id = transfer_1_id },
        .{ .id = transfer_2_id },
        .{ .id = transfer_3_id },
        .{ .id = transfer_4_id },
    });

    const balances = ct.get_account_balances(.{
        .account_id = account_id,
        .limit = 10,
        .flags = .{ .debits = true, .credits = true },
    });
    ct.assert_equal(balances, .{
        .{ .debits_posted = 10, .credits_posted = 0 },
        .{ .debits_posted = 10, .credits_posted = 20 },
        .{ .debits_posted = 40, .credits_posted = 20 },
        .{ .debits_posted = 40, .credits_posted = 60 },
    });

    const transfer_1 = transfers[0];
    const transfer_2 = transfers[1];
    const transfer_3 = transfers[2];
    const transfer_4 = transfers[3];
    const balance_1 = balances[0];
    const balance_2 = balances[1];
    const balance_3 = balances[2];
    const balance_4 = balances[3];
    ct.assert_equal(balance_1.timestamp, transfer_1.timestamp);
    ct.assert_equal(balance_2.timestamp, transfer_2.timestamp);
    ct.assert_equal(balance_3.timestamp, transfer_3.timestamp);
    ct.assert_equal(balance_4.timestamp, transfer_4.timestamp);
}

test "returns no balances without the history flag" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 1 },
        .{ .id = account_2_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    });

    const balances = ct.get_account_balances(.{
        .account_id = account_1_id,
        .limit = 10,
        .flags = .{ .debits = true, .credits = true },
    });

    ct.assert_empty(balances);
}
