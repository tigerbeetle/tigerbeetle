const ct = @import("../conformance_test_api.zig");

test "returns transfers matching user data" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    const transfer_1_id = ct.generate_id();
    const transfer_2_id = ct.generate_id();
    const user_data = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = transfer_1_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .user_data_128 = user_data,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_2_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 20,
            .user_data_128 = user_data,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 30,
            .user_data_128 = ct.generate_id(),
            .ledger = 1,
            .code = 1,
        },
    });

    const transfers = ct.query_transfers(.{ .user_data_128 = user_data, .limit = 10 });

    ct.assert_equal(transfers, .{
        .{ .id = transfer_1_id, .amount = 10, .user_data_128 = user_data },
        .{ .id = transfer_2_id, .amount = 20, .user_data_128 = user_data },
    });
}

test "returns transfers in reverse order with the reversed flag" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    const transfer_1_id = ct.generate_id();
    const transfer_2_id = ct.generate_id();
    const user_data = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = transfer_1_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .user_data_128 = user_data,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_2_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 20,
            .user_data_128 = user_data,
            .ledger = 1,
            .code = 1,
        },
    });

    const transfers = ct.query_transfers(.{
        .user_data_128 = user_data,
        .limit = 10,
        .flags = .{ .reversed = true },
    });

    ct.assert_equal(transfers, .{
        .{ .id = transfer_2_id, .amount = 20 },
        .{ .id = transfer_1_id, .amount = 10 },
    });
}

test "returns transfers matching ledger and code" {
    const transfer_id = ct.generate_id();
    const user_data = ct.generate_id();
    const ledger_7_debit_id = ct.generate_id();
    const ledger_7_credit_id = ct.generate_id();
    const ledger_8_debit_id = ct.generate_id();
    const ledger_8_credit_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = ledger_7_debit_id, .ledger = 7, .code = 1 },
        .{ .id = ledger_7_credit_id, .ledger = 7, .code = 1 },
        .{ .id = ledger_8_debit_id, .ledger = 8, .code = 1 },
        .{ .id = ledger_8_credit_id, .ledger = 8, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = transfer_id,
            .debit_account_id = ledger_7_debit_id,
            .credit_account_id = ledger_7_credit_id,
            .amount = 10,
            .user_data_128 = user_data,
            .ledger = 7,
            .code = 42,
        },
        .{
            .id = ct.generate_id(),
            .debit_account_id = ledger_7_debit_id,
            .credit_account_id = ledger_7_credit_id,
            .amount = 20,
            .user_data_128 = user_data,
            .ledger = 7,
            .code = 43,
        },
        .{
            .id = ct.generate_id(),
            .debit_account_id = ledger_8_debit_id,
            .credit_account_id = ledger_8_credit_id,
            .amount = 30,
            .user_data_128 = user_data,
            .ledger = 8,
            .code = 42,
        },
    });

    const transfers = ct.query_transfers(.{
        .user_data_128 = user_data,
        .ledger = 7,
        .code = 42,
        .limit = 10,
    });

    ct.assert_equal(transfers, .{.{ .id = transfer_id, .ledger = 7, .code = 42 }});
}

test "fails when the limit is too large" {
    ct.assert_fail(ct.query_transfers(.{ .limit = 10000 }));
}

test "returns no transfers for unused user data" {
    const transfers = ct.query_transfers(.{ .user_data_128 = ct.generate_id(), .limit = 10 });

    ct.assert_empty(transfers);
}
