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

test "returns transfers with the timestamps of their create results" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    const transfer_1_id = ct.generate_id();
    const transfer_2_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 1 },
        .{ .id = account_2_id, .ledger = 1, .code = 1 },
    });
    const transfer_results = ct.create_transfers(.{
        .{
            .id = transfer_1_id,
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_2_id,
            .debit_account_id = account_2_id,
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
        .{ .id = transfer_1_id },
        .{ .id = transfer_2_id },
    });
    const transfer_result_1 = transfer_results[0];
    const transfer_result_2 = transfer_results[1];
    const transfer_1 = transfers[0];
    const transfer_2 = transfers[1];
    ct.assert_equal(transfer_1.timestamp, transfer_result_1.timestamp);
    ct.assert_equal(transfer_2.timestamp, transfer_result_2.timestamp);
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

test "returns transfers in reverse order with the reversed flag" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    const transfer_1_id = ct.generate_id();
    const transfer_2_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 1 },
        .{ .id = account_2_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = transfer_1_id,
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_2_id,
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 20,
            .ledger = 1,
            .code = 1,
        },
    });

    const transfers = ct.get_account_transfers(.{
        .account_id = account_1_id,
        .limit = 10,
        .flags = .{ .debits = true, .credits = true, .reversed = true },
    });

    ct.assert_equal(transfers, .{
        .{ .id = transfer_2_id, .amount = 20 },
        .{ .id = transfer_1_id, .amount = 10 },
    });
}

test "returns only debit transfers in reverse order" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    const account_3_id = ct.generate_id();
    const debit_transfer_1_id = ct.generate_id();
    const debit_transfer_2_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 1 },
        .{ .id = account_2_id, .ledger = 1, .code = 1 },
        .{ .id = account_3_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = debit_transfer_1_id,
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
        .{
            .id = debit_transfer_2_id,
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 30,
            .ledger = 1,
            .code = 1,
        },
    });

    const transfers = ct.get_account_transfers(.{
        .account_id = account_1_id,
        .limit = 10,
        .flags = .{ .debits = true, .reversed = true },
    });

    ct.assert_equal(transfers, .{
        .{ .id = debit_transfer_2_id, .amount = 30 },
        .{ .id = debit_transfer_1_id, .amount = 10 },
    });
}

test "returns only credit transfers in reverse order" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    const account_3_id = ct.generate_id();
    const credit_transfer_1_id = ct.generate_id();
    const credit_transfer_2_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 1 },
        .{ .id = account_2_id, .ledger = 1, .code = 1 },
        .{ .id = account_3_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = credit_transfer_1_id,
            .debit_account_id = account_3_id,
            .credit_account_id = account_1_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 20,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = credit_transfer_2_id,
            .debit_account_id = account_3_id,
            .credit_account_id = account_1_id,
            .amount = 30,
            .ledger = 1,
            .code = 1,
        },
    });

    const transfers = ct.get_account_transfers(.{
        .account_id = account_1_id,
        .limit = 10,
        .flags = .{ .credits = true, .reversed = true },
    });

    ct.assert_equal(transfers, .{
        .{ .id = credit_transfer_2_id, .amount = 30 },
        .{ .id = credit_transfer_1_id, .amount = 10 },
    });
}

test "filters transfers by code" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    const transfer_1_id = ct.generate_id();
    const transfer_2_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 1 },
        .{ .id = account_2_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = transfer_1_id,
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_2_id,
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 20,
            .ledger = 1,
            .code = 2,
        },
    });

    const transfers = ct.get_account_transfers(.{
        .account_id = account_1_id,
        .code = 2,
        .limit = 10,
        .flags = .{ .debits = true, .credits = true },
    });

    ct.assert_equal(transfers, .{.{ .id = transfer_2_id, .amount = 20, .code = 2 }});
}

test "filters transfers by user data" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    const transfer_1_id = ct.generate_id();
    const transfer_2_id = ct.generate_id();
    const user_data_128 = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 1 },
        .{ .id = account_2_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = transfer_1_id,
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
            .user_data_128 = user_data_128,
            .user_data_64 = 64,
            .user_data_32 = 32,
        },
        .{
            .id = transfer_2_id,
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 20,
            .ledger = 1,
            .code = 1,
            .user_data_128 = ct.generate_id(),
            .user_data_64 = 65,
            .user_data_32 = 33,
        },
    });

    const transfers = ct.get_account_transfers(.{
        .account_id = account_1_id,
        .user_data_128 = user_data_128,
        .user_data_64 = 64,
        .user_data_32 = 32,
        .limit = 10,
        .flags = .{ .debits = true, .credits = true },
    });

    ct.assert_equal(transfers, .{.{ .id = transfer_1_id, .amount = 10 }});
}

test "pages through transfers with a timestamp cursor" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    const transfer_1_id = ct.generate_id();
    const transfer_2_id = ct.generate_id();
    const transfer_3_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 1 },
        .{ .id = account_2_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = transfer_1_id,
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_2_id,
            .debit_account_id = account_2_id,
            .credit_account_id = account_1_id,
            .amount = 20,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_3_id,
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 30,
            .ledger = 1,
            .code = 1,
        },
    });

    const page_1 = ct.get_account_transfers(.{
        .account_id = account_1_id,
        .limit = 2,
        .flags = .{ .debits = true, .credits = true },
    });

    ct.assert_equal(page_1, .{
        .{ .id = transfer_1_id, .amount = 10 },
        .{ .id = transfer_2_id, .amount = 20 },
    });

    const page_1_last = page_1[1];
    const cursor_1 = page_1_last.timestamp;
    const page_2 = ct.get_account_transfers(.{
        .account_id = account_1_id,
        .timestamp_min = ct.increment(cursor_1, 1),
        .limit = 2,
        .flags = .{ .debits = true, .credits = true },
    });

    ct.assert_equal(page_2, .{.{ .id = transfer_3_id, .amount = 30 }});

    const page_2_last = page_2[0];
    const cursor_2 = page_2_last.timestamp;
    const page_3 = ct.get_account_transfers(.{
        .account_id = account_1_id,
        .timestamp_min = ct.increment(cursor_2, 1),
        .limit = 2,
        .flags = .{ .debits = true, .credits = true },
    });

    ct.assert_empty(page_3);
}

test "pages through reversed transfers with a timestamp cursor" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    const transfer_1_id = ct.generate_id();
    const transfer_2_id = ct.generate_id();
    const transfer_3_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 1 },
        .{ .id = account_2_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = transfer_1_id,
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_2_id,
            .debit_account_id = account_2_id,
            .credit_account_id = account_1_id,
            .amount = 20,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_3_id,
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 30,
            .ledger = 1,
            .code = 1,
        },
    });

    const page_1 = ct.get_account_transfers(.{
        .account_id = account_1_id,
        .limit = 2,
        .flags = .{ .debits = true, .credits = true, .reversed = true },
    });

    ct.assert_equal(page_1, .{
        .{ .id = transfer_3_id, .amount = 30 },
        .{ .id = transfer_2_id, .amount = 20 },
    });

    const page_1_last = page_1[1];
    const cursor_1 = page_1_last.timestamp;
    const page_2 = ct.get_account_transfers(.{
        .account_id = account_1_id,
        .timestamp_max = ct.decrement(cursor_1, 1),
        .limit = 2,
        .flags = .{ .debits = true, .credits = true, .reversed = true },
    });

    ct.assert_equal(page_2, .{.{ .id = transfer_1_id, .amount = 10 }});

    const page_2_last = page_2[0];
    const cursor_2 = page_2_last.timestamp;
    const page_3 = ct.get_account_transfers(.{
        .account_id = account_1_id,
        .timestamp_max = ct.decrement(cursor_2, 1),
        .limit = 2,
        .flags = .{ .debits = true, .credits = true, .reversed = true },
    });

    ct.assert_empty(page_3);
}

test "returns no transfers for an unused account" {
    const transfers = ct.get_account_transfers(.{
        .account_id = ct.generate_id(),
        .limit = 10,
        .flags = .{ .debits = true, .credits = true },
    });

    ct.assert_empty(transfers);
}

test "returns no transfers for a zero account id" {
    const transfers = ct.get_account_transfers(.{
        .account_id = 0,
        .limit = 10,
        .flags = .{ .debits = true, .credits = true },
    });

    ct.assert_empty(transfers);
}

test "returns no transfers for a default filter" {
    const transfers = ct.get_account_transfers(.{});

    ct.assert_empty(transfers);
}

test "returns no transfers for a zero limit" {
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

    const transfers = ct.get_account_transfers(.{
        .account_id = account_1_id,
        .limit = 0,
        .flags = .{ .debits = true, .credits = true },
    });

    ct.assert_empty(transfers);
}

test "returns no transfers when the timestamp range is inverted" {
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
    const matched = ct.get_account_transfers(.{
        .account_id = account_1_id,
        .limit = 10,
        .flags = .{ .debits = true, .credits = true },
    });
    const transfer = matched[0];
    const transfer_timestamp = transfer.timestamp;

    const transfers = ct.get_account_transfers(.{
        .account_id = account_1_id,
        .timestamp_min = ct.increment(transfer_timestamp, 1),
        .timestamp_max = ct.decrement(transfer_timestamp, 1),
        .limit = 10,
        .flags = .{ .debits = true, .credits = true },
    });

    ct.assert_empty(transfers);
}

test "returns no transfers for a timestamp minimum of u64 max" {
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
    const transfers = ct.get_account_transfers(.{
        .account_id = account_1_id,
        .timestamp_min = 18446744073709551615,
        .limit = 10,
        .flags = .{ .debits = true, .credits = true },
    });

    ct.assert_empty(transfers);
}

test "returns no transfers for a timestamp maximum of u64 max" {
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
    const transfers = ct.get_account_transfers(.{
        .account_id = account_1_id,
        .timestamp_max = 18446744073709551615,
        .limit = 10,
        .flags = .{ .debits = true, .credits = true },
    });

    ct.assert_empty(transfers);
}

test "returns no transfers for an inverted timestamp range at u64 max" {
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
    const transfers = ct.get_account_transfers(.{
        .account_id = account_1_id,
        .timestamp_min = 18446744073709551614,
        .timestamp_max = 1,
        .limit = 10,
        .flags = .{ .debits = true, .credits = true },
    });

    ct.assert_empty(transfers);
}

test "returns no transfers without the debits or credits flag" {
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

    const transfers = ct.get_account_transfers(.{
        .account_id = account_1_id,
        .limit = 10,
    });

    ct.assert_empty(transfers);
}

test "fails when the limit is too large" {
    ct.assert_fail_with(ct.get_account_transfers(.{
        .account_id = ct.generate_id(),
        .limit = 10000,
        .flags = .{ .debits = true, .credits = true },
    }), .too_much_data);
}
