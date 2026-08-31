const ct = @import("../conformance_test_api.zig");

test "creates a transfer" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    const results = ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 100,
            .ledger = 1,
            .code = 1,
        },
    });

    ct.assert_equal(results, .{.{ .status = .created }});
}

test "returns exists for a duplicate transfer" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });
    const transfer = ct.Transfer{
        .id = ct.generate_id(),
        .debit_account_id = debit_account_id,
        .credit_account_id = credit_account_id,
        .amount = 100,
        .ledger = 1,
        .code = 1,
    };
    ct.create_transfers(.{transfer});

    const results = ct.create_transfers(.{transfer});

    ct.assert_equal(results, .{.{ .status = .exists }});
}

test "rejects a zero id" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    const results = ct.create_transfers(.{
        .{
            .id = 0,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    });

    ct.assert_equal(results, .{.{ .status = .id_must_not_be_zero }});
}

test "rejects a zero debit account id" {
    const credit_account_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    const results = ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = 0,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    });

    ct.assert_equal(results, .{.{ .status = .debit_account_id_must_not_be_zero }});
}

test "rejects a zero credit account id" {
    const debit_account_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
    });

    const results = ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = 0,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    });

    ct.assert_equal(results, .{.{ .status = .credit_account_id_must_not_be_zero }});
}

test "rejects identical debit and credit accounts" {
    const account_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_id, .ledger = 1, .code = 1 },
    });

    const results = ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_id,
            .credit_account_id = account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    });

    ct.assert_equal(results, .{.{ .status = .accounts_must_be_different }});
}

test "rejects a zero ledger" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    const results = ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 0,
            .code = 1,
        },
    });

    ct.assert_equal(results, .{.{ .status = .ledger_must_not_be_zero }});
}

test "rejects a zero code" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    const results = ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 0,
        },
    });

    ct.assert_equal(results, .{.{ .status = .code_must_not_be_zero }});
}

test "rejects a fractional amount" {
    ct.requires_fractional_amounts();

    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    ct.assert_fail(ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 1.5,
            .ledger = 1,
            .code = 1,
        },
    }));
}
