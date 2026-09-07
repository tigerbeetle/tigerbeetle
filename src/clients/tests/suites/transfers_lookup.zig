const ct = @import("../conformance_test_api.zig");

test "returns an existing transfer" {
    const transfer_id = ct.generate_id();
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = transfer_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 42,
            .ledger = 1,
            .code = 1,
        },
    });

    const transfers = ct.lookup_transfers(.{transfer_id});

    ct.assert_equal(transfers, .{.{
        .id = transfer_id,
        .debit_account_id = debit_account_id,
        .credit_account_id = credit_account_id,
        .amount = 42,
    }});
}

test "returns no transfers for a missing id" {
    const transfers = ct.lookup_transfers(.{ct.generate_id()});

    ct.assert_empty(transfers);
}

test "returns transfers in the order they were requested" {
    const transfer_1_id = ct.generate_id();
    const transfer_2_id = ct.generate_id();
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
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
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_2_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 20,
            .ledger = 1,
            .code = 1,
        },
    });

    const transfers = ct.lookup_transfers(.{ transfer_2_id, transfer_1_id });

    ct.assert_equal(transfers, .{
        .{ .id = transfer_2_id, .amount = 20 },
        .{ .id = transfer_1_id, .amount = 10 },
    });
}

test "returns a transfer once per requested id" {
    const transfer_id = ct.generate_id();
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = transfer_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    });

    const transfers = ct.lookup_transfers(.{ transfer_id, transfer_id });

    ct.assert_equal(transfers, .{
        .{ .id = transfer_id, .amount = 10 },
        .{ .id = transfer_id, .amount = 10 },
    });
}

test "returns no transfer for an id that failed" {
    const transfer_id = ct.generate_id();
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    ct.create_accounts(.{
        .{
            .id = debit_account_id,
            .ledger = 1,
            .code = 1,
            .flags = .{ .debits_must_not_exceed_credits = true },
        },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = transfer_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    });

    const transfers = ct.lookup_transfers(.{transfer_id});

    ct.assert_empty(transfers);
}

test "returns only the existing transfer for a partial match" {
    const existing_id = ct.generate_id();
    const missing_id = ct.generate_id();
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = existing_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 5,
            .ledger = 1,
            .code = 1,
        },
    });

    const transfers = ct.lookup_transfers(.{ existing_id, missing_id });

    ct.assert_equal(transfers, .{.{ .id = existing_id }});
}

test "returns no transfers for an empty batch" {
    const transfers = ct.lookup_transfers(.{});

    ct.assert_empty(transfers);
}

test "round-trips all fields" {
    const transfer_id = ct.generate_id();
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    const user_data_128 = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = transfer_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 99,
            .ledger = 1,
            .code = 7,
            .user_data_128 = user_data_128,
            .user_data_64 = 8888888888,
            .user_data_32 = 54321,
        },
    });

    const transfers = ct.lookup_transfers(.{transfer_id});

    ct.assert_equal(transfers, .{.{
        .id = transfer_id,
        .debit_account_id = debit_account_id,
        .credit_account_id = credit_account_id,
        .amount = 99,
        .ledger = 1,
        .code = 7,
        .user_data_128 = user_data_128,
        .user_data_64 = 8888888888,
        .user_data_32 = 54321,
    }});
    const transfer = transfers[0];
    ct.assert_greater_than(transfer.timestamp, 0);
}
