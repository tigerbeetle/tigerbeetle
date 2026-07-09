const ct = @import("../conformance_test_api.zig");

test "returns an existing account" {
    const account_id = ct.generate_id();
    ct.create_accounts(.{.{ .id = account_id, .ledger = 1, .code = 1 }});

    const accounts = ct.lookup_accounts(.{account_id});

    ct.assert_equal(accounts, .{.{ .id = account_id, .ledger = 1, .code = 1 }});
}

test "returns no accounts for a missing id" {
    const accounts = ct.lookup_accounts(.{ct.generate_id()});

    ct.assert_empty(accounts);
}

test "returns multiple existing accounts in one batch" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 1 },
        .{ .id = account_2_id, .ledger = 2, .code = 2 },
    });

    const accounts = ct.lookup_accounts(.{ account_1_id, account_2_id });

    ct.assert_equal(accounts, .{
        .{ .id = account_1_id, .ledger = 1 },
        .{ .id = account_2_id, .ledger = 2 },
    });
}

test "returns only the existing account for a partial match" {
    const existing_id = ct.generate_id();
    const missing_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = existing_id, .ledger = 1, .code = 1 },
    });

    const accounts = ct.lookup_accounts(.{ existing_id, missing_id });

    ct.assert_equal(accounts, .{.{ .id = existing_id }});
}

test "returns no accounts for an empty batch" {
    const accounts = ct.lookup_accounts(.{});

    ct.assert_empty(accounts);
}

test "round-trips all fields" {
    const account_id = ct.generate_id();
    const user_data_128 = ct.generate_id();
    ct.create_accounts(.{
        .{
            .id = account_id,
            .ledger = 7,
            .code = 42,
            .user_data_128 = user_data_128,
            .user_data_64 = 9999999999,
            .user_data_32 = 12345,
            .flags = .{ .history = true },
        },
    });

    const accounts = ct.lookup_accounts(.{account_id});

    ct.assert_equal(accounts, .{.{
        .id = account_id,
        .ledger = 7,
        .code = 42,
        .user_data_128 = user_data_128,
        .user_data_64 = 9999999999,
        .user_data_32 = 12345,
        .flags = .{ .history = true },
    }});
    const account = accounts[0];
    ct.assert_greater_than(account.timestamp, 0);
}
