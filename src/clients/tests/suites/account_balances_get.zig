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
