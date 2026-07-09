const ct = @import("../conformance_test_api.zig");

test "returns debit and credit transfers for an account" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    const account_3_id = ct.generate_id();
    const debit_transfer_id = ct.generate_id();
    const credit_transfer_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 1 },
        .{ .id = account_2_id, .ledger = 1, .code = 1 },
        .{ .id = account_3_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = debit_transfer_id,
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = credit_transfer_id,
            .debit_account_id = account_3_id,
            .credit_account_id = account_1_id,
            .amount = 20,
            .ledger = 1,
            .code = 1,
        },
    });

    const transfers = ct.get_account_transfers(.{
        .account_id = account_1_id,
        .limit = 10,
        .flags = .{ .debits = true, .credits = true },
    });

    ct.assert_equal(transfers, .{
        .{ .id = debit_transfer_id, .amount = 10 },
        .{ .id = credit_transfer_id, .amount = 20 },
    });
}

test "returns only debit transfers with the debits flag" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    const account_3_id = ct.generate_id();
    const debit_transfer_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 1 },
        .{ .id = account_2_id, .ledger = 1, .code = 1 },
        .{ .id = account_3_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = debit_transfer_id,
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_3_id,
            .credit_account_id = account_1_id,
            .amount = 20,
            .ledger = 1,
            .code = 1,
        },
    });

    const transfers = ct.get_account_transfers(.{
        .account_id = account_1_id,
        .limit = 10,
        .flags = .{ .debits = true },
    });

    ct.assert_equal(transfers, .{.{ .id = debit_transfer_id, .amount = 10 }});
}

test "returns only credit transfers with the credits flag" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    const account_3_id = ct.generate_id();
    const credit_transfer_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 1 },
        .{ .id = account_2_id, .ledger = 1, .code = 1 },
        .{ .id = account_3_id, .ledger = 1, .code = 1 },
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
            .id = credit_transfer_id,
            .debit_account_id = account_3_id,
            .credit_account_id = account_1_id,
            .amount = 20,
            .ledger = 1,
            .code = 1,
        },
    });

    const transfers = ct.get_account_transfers(.{
        .account_id = account_1_id,
        .limit = 10,
        .flags = .{ .credits = true },
    });

    ct.assert_equal(transfers, .{.{ .id = credit_transfer_id, .amount = 20 }});
}

test "returns no transfers for an unused account" {
    const transfers = ct.get_account_transfers(.{
        .account_id = ct.generate_id(),
        .limit = 10,
        .flags = .{ .debits = true, .credits = true },
    });

    ct.assert_empty(transfers);
}
