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

test "returns no accounts for unused user data" {
    const accounts = ct.query_accounts(.{ .user_data_128 = ct.generate_id(), .limit = 10 });

    ct.assert_empty(accounts);
}
