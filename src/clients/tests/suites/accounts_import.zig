const ct = @import("../conformance/api.zig");
const client = ct.default_client;

test "imports accounts with explicit timestamps" {
    const reference_results = client.create_accounts(.{
        .{ .id = ct.generate_id(), .ledger = 1, .code = 1 },
    });
    const reference_result = reference_results[0];
    const reference = reference_result.timestamp;

    ct.sleep_ms(10);

    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    const results = client.create_accounts(.{
        .{
            .id = account_1_id,
            .ledger = 1,
            .code = 1,
            .flags = .{ .imported = true },
            .timestamp = ct.increment(reference, 1),
        },
        .{
            .id = account_2_id,
            .ledger = 1,
            .code = 1,
            .flags = .{ .imported = true },
            .timestamp = ct.increment(reference, 2),
        },
    });

    ct.assert_equal(results, .{
        .{ .status = .created },
        .{ .status = .created },
    });

    const accounts = client.lookup_accounts(.{ account_1_id, account_2_id });
    ct.assert_equal(accounts, .{
        .{ .id = account_1_id, .flags = .{ .imported = true } },
        .{ .id = account_2_id, .flags = .{ .imported = true } },
    });

    const result_1 = results[0];
    const result_2 = results[1];
    const account_1 = accounts[0];
    const account_2 = accounts[1];
    ct.assert_equal(result_1.timestamp, ct.increment(reference, 1));
    ct.assert_equal(result_2.timestamp, ct.increment(reference, 2));
    ct.assert_equal(account_1.timestamp, result_1.timestamp);
    ct.assert_equal(account_2.timestamp, result_2.timestamp);
}
