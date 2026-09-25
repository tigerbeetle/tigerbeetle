const ct = @import("../conformance_test_api.zig");

test "returns accounts matching user data" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    const user_data = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .user_data_128 = user_data, .ledger = 1, .code = 1 },
        .{ .id = account_2_id, .user_data_128 = user_data, .ledger = 1, .code = 1 },
        .{ .id = ct.generate_id(), .user_data_128 = ct.generate_id(), .ledger = 1, .code = 1 },
    });

    const accounts = ct.query_accounts(.{ .user_data_128 = user_data, .limit = 10 });

    ct.assert_equal(accounts, .{
        .{ .id = account_1_id, .user_data_128 = user_data },
        .{ .id = account_2_id, .user_data_128 = user_data },
    });
}

test "returns accounts matching ledger and code" {
    const account_id = ct.generate_id();
    const user_data = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_id, .user_data_128 = user_data, .ledger = 7, .code = 42 },
        .{ .id = ct.generate_id(), .user_data_128 = user_data, .ledger = 7, .code = 43 },
        .{ .id = ct.generate_id(), .user_data_128 = user_data, .ledger = 8, .code = 42 },
    });

    const accounts = ct.query_accounts(.{
        .user_data_128 = user_data,
        .ledger = 7,
        .code = 42,
        .limit = 10,
    });

    ct.assert_equal(accounts, .{.{ .id = account_id, .ledger = 7, .code = 42 }});
}

test "returns accounts matching every filter field" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    const user_data = ct.generate_id();
    ct.create_accounts(.{
        .{
            .id = account_1_id,
            .user_data_128 = user_data,
            .user_data_64 = 100,
            .user_data_32 = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = ct.generate_id(),
            .user_data_128 = user_data,
            .user_data_64 = 100,
            .user_data_32 = 20,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = ct.generate_id(),
            .user_data_128 = user_data,
            .user_data_64 = 200,
            .user_data_32 = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = account_2_id,
            .user_data_128 = user_data,
            .user_data_64 = 100,
            .user_data_32 = 10,
            .ledger = 1,
            .code = 1,
        },
    });

    const accounts = ct.query_accounts(.{
        .user_data_128 = user_data,
        .user_data_64 = 100,
        .user_data_32 = 10,
        .ledger = 1,
        .code = 1,
        .limit = 10,
    });

    ct.assert_equal(accounts, .{
        .{ .id = account_1_id, .user_data_64 = 100, .user_data_32 = 10 },
        .{ .id = account_2_id, .user_data_64 = 100, .user_data_32 = 10 },
    });
}

test "returns accounts matching every filter field in reverse order" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    const user_data = ct.generate_id();
    ct.create_accounts(.{
        .{
            .id = account_1_id,
            .user_data_128 = user_data,
            .user_data_64 = 100,
            .user_data_32 = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = ct.generate_id(),
            .user_data_128 = user_data,
            .user_data_64 = 100,
            .user_data_32 = 20,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = ct.generate_id(),
            .user_data_128 = user_data,
            .user_data_64 = 200,
            .user_data_32 = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = account_2_id,
            .user_data_128 = user_data,
            .user_data_64 = 100,
            .user_data_32 = 10,
            .ledger = 1,
            .code = 1,
        },
    });

    const accounts = ct.query_accounts(.{
        .user_data_128 = user_data,
        .user_data_64 = 100,
        .user_data_32 = 10,
        .ledger = 1,
        .code = 1,
        .limit = 10,
        .flags = .{ .reversed = true },
    });

    ct.assert_equal(accounts, .{
        .{ .id = account_2_id, .user_data_64 = 100, .user_data_32 = 10 },
        .{ .id = account_1_id, .user_data_64 = 100, .user_data_32 = 10 },
    });
}

test "returns accounts matching code" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    const results = ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 999 },
        .{ .id = ct.generate_id(), .ledger = 1, .code = 998 },
        .{ .id = account_2_id, .ledger = 1, .code = 999 },
    });
    const result_first = results[0];
    const result_last = results[2];
    const timestamp_first = result_first.timestamp;
    const timestamp_last = result_last.timestamp;

    const accounts = ct.query_accounts(.{
        .code = 999,
        .timestamp_min = timestamp_first,
        .timestamp_max = timestamp_last,
        .limit = 10,
    });

    ct.assert_equal(accounts, .{
        .{ .id = account_1_id, .code = 999 },
        .{ .id = account_2_id, .code = 999 },
    });
}

test "returns accounts in reverse order with the reversed flag" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    const user_data = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .user_data_128 = user_data, .ledger = 1, .code = 1 },
        .{ .id = account_2_id, .user_data_128 = user_data, .ledger = 1, .code = 1 },
    });

    const accounts = ct.query_accounts(.{
        .user_data_128 = user_data,
        .limit = 10,
        .flags = .{ .reversed = true },
    });

    ct.assert_equal(accounts, .{
        .{ .id = account_2_id },
        .{ .id = account_1_id },
    });
}

test "pages through reversed accounts with a timestamp cursor" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    const account_3_id = ct.generate_id();
    const user_data = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .user_data_128 = user_data, .ledger = 1, .code = 1 },
        .{ .id = account_2_id, .user_data_128 = user_data, .ledger = 1, .code = 1 },
        .{ .id = account_3_id, .user_data_128 = user_data, .ledger = 1, .code = 1 },
    });

    const page_1 = ct.query_accounts(.{
        .user_data_128 = user_data,
        .limit = 2,
        .flags = .{ .reversed = true },
    });

    ct.assert_equal(page_1, .{
        .{ .id = account_3_id },
        .{ .id = account_2_id },
    });

    const page_1_last = page_1[1];
    const cursor_1 = page_1_last.timestamp;
    const page_2 = ct.query_accounts(.{
        .user_data_128 = user_data,
        .timestamp_max = ct.decrement(cursor_1, 1),
        .limit = 2,
        .flags = .{ .reversed = true },
    });

    ct.assert_equal(page_2, .{.{ .id = account_1_id }});

    const page_2_last = page_2[0];
    const cursor_2 = page_2_last.timestamp;
    const page_3 = ct.query_accounts(.{
        .user_data_128 = user_data,
        .timestamp_max = ct.decrement(cursor_2, 1),
        .limit = 2,
        .flags = .{ .reversed = true },
    });

    ct.assert_empty(page_3);
}

test "returns no accounts for unused user data" {
    const accounts = ct.query_accounts(.{ .user_data_128 = ct.generate_id(), .limit = 10 });

    ct.assert_empty(accounts);
}

test "returns no accounts when no account matches every user data field" {
    const user_data = ct.generate_id();
    ct.create_accounts(.{
        .{
            .id = ct.generate_id(),
            .user_data_128 = user_data,
            .user_data_64 = 100,
            .user_data_32 = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = ct.generate_id(),
            .user_data_128 = user_data,
            .user_data_64 = 200,
            .user_data_32 = 20,
            .ledger = 1,
            .code = 1,
        },
    });

    const accounts = ct.query_accounts(.{
        .user_data_128 = user_data,
        .user_data_64 = 200,
        .user_data_32 = 10,
        .limit = 10,
    });

    ct.assert_empty(accounts);
}

test "returns no accounts for a default filter" {
    const accounts = ct.query_accounts(.{});

    ct.assert_empty(accounts);
}

test "returns no accounts for a zero limit" {
    const account_id = ct.generate_id();
    const user_data = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_id, .user_data_128 = user_data, .ledger = 1, .code = 1 },
    });

    const accounts = ct.query_accounts(.{ .user_data_128 = user_data, .limit = 0 });

    ct.assert_empty(accounts);
}

test "returns no accounts when the timestamp range is inverted" {
    const account_id = ct.generate_id();
    const user_data = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_id, .user_data_128 = user_data, .ledger = 1, .code = 1 },
    });
    const matched = ct.query_accounts(.{ .user_data_128 = user_data, .limit = 10 });
    const account = matched[0];
    const account_timestamp = account.timestamp;

    const accounts = ct.query_accounts(.{
        .user_data_128 = user_data,
        .timestamp_min = ct.increment(account_timestamp, 1),
        .timestamp_max = ct.decrement(account_timestamp, 1),
        .limit = 10,
    });

    ct.assert_empty(accounts);
}

test "returns no accounts for a timestamp minimum of u64 max" {
    const user_data = ct.generate_id();

    const accounts = ct.query_accounts(.{
        .user_data_128 = user_data,
        .timestamp_min = 18446744073709551615,
        .limit = 10,
    });

    ct.assert_empty(accounts);
}

test "returns no accounts for a timestamp maximum of u64 max" {
    const user_data = ct.generate_id();

    const accounts = ct.query_accounts(.{
        .user_data_128 = user_data,
        .timestamp_max = 18446744073709551615,
        .limit = 10,
    });

    ct.assert_empty(accounts);
}

test "returns no accounts for an inverted timestamp range at u64 max" {
    const user_data = ct.generate_id();

    const accounts = ct.query_accounts(.{
        .user_data_128 = user_data,
        .timestamp_min = 18446744073709551614,
        .timestamp_max = 1,
        .limit = 10,
    });

    ct.assert_empty(accounts);
}

test "fails when the limit is too large" {
    ct.assert_fail_with(ct.query_accounts(.{
        .user_data_128 = ct.generate_id(),
        .limit = 10000,
    }), .too_much_data);
}
