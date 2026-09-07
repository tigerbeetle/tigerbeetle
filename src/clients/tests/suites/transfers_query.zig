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

test "returns transfers matching every filter field" {
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
            .user_data_64 = 100,
            .user_data_32 = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 20,
            .user_data_128 = user_data,
            .user_data_64 = 100,
            .user_data_32 = 20,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 30,
            .user_data_128 = user_data,
            .user_data_64 = 200,
            .user_data_32 = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_2_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 40,
            .user_data_128 = user_data,
            .user_data_64 = 100,
            .user_data_32 = 10,
            .ledger = 1,
            .code = 1,
        },
    });

    const transfers = ct.query_transfers(.{
        .user_data_128 = user_data,
        .user_data_64 = 100,
        .user_data_32 = 10,
        .ledger = 1,
        .code = 1,
        .limit = 10,
    });

    ct.assert_equal(transfers, .{
        .{ .id = transfer_1_id, .amount = 10 },
        .{ .id = transfer_2_id, .amount = 40 },
    });
}

test "returns transfers matching every filter field in reverse order" {
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
            .user_data_64 = 100,
            .user_data_32 = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 20,
            .user_data_128 = user_data,
            .user_data_64 = 100,
            .user_data_32 = 20,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 30,
            .user_data_128 = user_data,
            .user_data_64 = 200,
            .user_data_32 = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_2_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 40,
            .user_data_128 = user_data,
            .user_data_64 = 100,
            .user_data_32 = 10,
            .ledger = 1,
            .code = 1,
        },
    });

    const transfers = ct.query_transfers(.{
        .user_data_128 = user_data,
        .user_data_64 = 100,
        .user_data_32 = 10,
        .ledger = 1,
        .code = 1,
        .limit = 10,
        .flags = .{ .reversed = true },
    });

    ct.assert_equal(transfers, .{
        .{ .id = transfer_2_id, .amount = 40 },
        .{ .id = transfer_1_id, .amount = 10 },
    });
}

test "returns transfers matching code" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    const transfer_1_id = ct.generate_id();
    const transfer_2_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });
    const results = ct.create_transfers(.{
        .{
            .id = transfer_1_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 999,
        },
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 20,
            .ledger = 1,
            .code = 998,
        },
        .{
            .id = transfer_2_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 30,
            .ledger = 1,
            .code = 999,
        },
    });
    const result_first = results[0];
    const result_last = results[2];
    const timestamp_first = result_first.timestamp;
    const timestamp_last = result_last.timestamp;

    const transfers = ct.query_transfers(.{
        .code = 999,
        .timestamp_min = timestamp_first,
        .timestamp_max = timestamp_last,
        .limit = 10,
    });

    ct.assert_equal(transfers, .{
        .{ .id = transfer_1_id, .amount = 10, .code = 999 },
        .{ .id = transfer_2_id, .amount = 30, .code = 999 },
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

test "pages through reversed transfers with a timestamp cursor" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    const transfer_1_id = ct.generate_id();
    const transfer_2_id = ct.generate_id();
    const transfer_3_id = ct.generate_id();
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
            .id = transfer_3_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 30,
            .user_data_128 = user_data,
            .ledger = 1,
            .code = 1,
        },
    });

    const page_1 = ct.query_transfers(.{
        .user_data_128 = user_data,
        .limit = 2,
        .flags = .{ .reversed = true },
    });

    ct.assert_equal(page_1, .{
        .{ .id = transfer_3_id, .amount = 30 },
        .{ .id = transfer_2_id, .amount = 20 },
    });

    const page_1_last = page_1[1];
    const cursor_1 = page_1_last.timestamp;
    const page_2 = ct.query_transfers(.{
        .user_data_128 = user_data,
        .timestamp_max = ct.decrement(cursor_1, 1),
        .limit = 2,
        .flags = .{ .reversed = true },
    });

    ct.assert_equal(page_2, .{.{ .id = transfer_1_id, .amount = 10 }});

    const page_2_last = page_2[0];
    const cursor_2 = page_2_last.timestamp;
    const page_3 = ct.query_transfers(.{
        .user_data_128 = user_data,
        .timestamp_max = ct.decrement(cursor_2, 1),
        .limit = 2,
        .flags = .{ .reversed = true },
    });

    ct.assert_empty(page_3);
}

test "returns no transfers for unused user data" {
    const transfers = ct.query_transfers(.{ .user_data_128 = ct.generate_id(), .limit = 10 });

    ct.assert_empty(transfers);
}

test "returns no transfers when no transfer matches every user data field" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    const user_data = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .user_data_128 = user_data,
            .user_data_64 = 100,
            .user_data_32 = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 20,
            .user_data_128 = user_data,
            .user_data_64 = 200,
            .user_data_32 = 20,
            .ledger = 1,
            .code = 1,
        },
    });

    const transfers = ct.query_transfers(.{
        .user_data_128 = user_data,
        .user_data_64 = 200,
        .user_data_32 = 10,
        .limit = 10,
    });

    ct.assert_empty(transfers);
}

test "returns no transfers for a default filter" {
    const transfers = ct.query_transfers(.{});

    ct.assert_empty(transfers);
}

test "returns no transfers for a zero limit" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    const user_data = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .user_data_128 = user_data,
            .ledger = 1,
            .code = 1,
        },
    });

    const transfers = ct.query_transfers(.{ .user_data_128 = user_data, .limit = 0 });

    ct.assert_empty(transfers);
}

test "returns no transfers when the timestamp range is inverted" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    const user_data = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .user_data_128 = user_data,
            .ledger = 1,
            .code = 1,
        },
    });

    const matched = ct.query_transfers(.{ .user_data_128 = user_data, .limit = 10 });
    const transfer = matched[0];
    const transfer_timestamp = transfer.timestamp;

    const transfers = ct.query_transfers(.{
        .user_data_128 = user_data,
        .timestamp_min = ct.increment(transfer_timestamp, 1),
        .timestamp_max = ct.decrement(transfer_timestamp, 1),
        .limit = 10,
    });

    ct.assert_empty(transfers);
}

test "returns no transfers for a timestamp minimum of u64 max" {
    const user_data = ct.generate_id();

    const transfers = ct.query_transfers(.{
        .user_data_128 = user_data,
        .timestamp_min = 18446744073709551615,
        .limit = 10,
    });

    ct.assert_empty(transfers);
}

test "returns no transfers for a timestamp maximum of u64 max" {
    const user_data = ct.generate_id();

    const transfers = ct.query_transfers(.{
        .user_data_128 = user_data,
        .timestamp_max = 18446744073709551615,
        .limit = 10,
    });

    ct.assert_empty(transfers);
}

test "returns no transfers for an inverted timestamp range at u64 max" {
    const user_data = ct.generate_id();

    const transfers = ct.query_transfers(.{
        .user_data_128 = user_data,
        .timestamp_min = 18446744073709551614,
        .timestamp_max = 1,
        .limit = 10,
    });

    ct.assert_empty(transfers);
}

test "fails when the limit is too large" {
    ct.assert_fail(ct.query_transfers(.{ .limit = 10000 }));
}
