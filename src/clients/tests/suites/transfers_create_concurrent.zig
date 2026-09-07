const ct = @import("../conformance_test_api.zig");

test "applies transfers submitted concurrently" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });
    ct.concurrently(100, ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    }));

    const accounts = ct.lookup_accounts(.{ debit_account_id, credit_account_id });

    ct.assert_equal(accounts, .{
        .{ .id = debit_account_id, .debits_posted = 1000, .credits_posted = 0 },
        .{ .id = credit_account_id, .debits_posted = 0, .credits_posted = 1000 },
    });
}

test "applies no transfer when an open linked chain is submitted concurrently" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });
    ct.concurrently(10, ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
            .flags = .{ .linked = true },
        },
    }));

    const accounts = ct.lookup_accounts(.{ debit_account_id, credit_account_id });

    ct.assert_equal(accounts, .{
        .{ .id = debit_account_id, .debits_posted = 0, .credits_posted = 0 },
        .{ .id = credit_account_id, .debits_posted = 0, .credits_posted = 0 },
    });
}

test "applies a transfer once when its id is submitted concurrently" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    const transfer_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });
    ct.concurrently(10, ct.create_transfers(.{
        .{
            .id = transfer_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    }));

    const accounts = ct.lookup_accounts(.{ debit_account_id, credit_account_id });

    ct.assert_equal(accounts, .{
        .{ .id = debit_account_id, .debits_posted = 10, .credits_posted = 0 },
        .{ .id = credit_account_id, .debits_posted = 0, .credits_posted = 10 },
    });
}
