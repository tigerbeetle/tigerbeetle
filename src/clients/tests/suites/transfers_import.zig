const ct = @import("../conformance/api.zig");
const client = ct.default_client;

test "imports a transfer with an explicit timestamp" {
    const reference_results = client.create_accounts(.{
        .{ .id = ct.generate_id(), .ledger = 1, .code = 1 },
    });
    const reference_result = reference_results[0];
    const reference = reference_result.timestamp;

    ct.sleep_ms(10);

    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    client.create_accounts(.{
        .{
            .id = debit_account_id,
            .ledger = 1,
            .code = 1,
            .flags = .{ .imported = true },
            .timestamp = ct.increment(reference, 1),
        },
        .{
            .id = credit_account_id,
            .ledger = 1,
            .code = 1,
            .flags = .{ .imported = true },
            .timestamp = ct.increment(reference, 2),
        },
    });

    const transfer_id = ct.generate_id();
    const results = client.create_transfers(.{
        .{
            .id = transfer_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 100,
            .ledger = 1,
            .code = 1,
            .flags = .{ .imported = true },
            .timestamp = ct.increment(reference, 3),
        },
    });

    ct.assert_equal(results, .{.{ .status = .created }});

    const transfers = client.lookup_transfers(.{transfer_id});
    ct.assert_equal(transfers, .{
        .{ .id = transfer_id, .amount = 100, .flags = .{ .imported = true } },
    });

    const result = results[0];
    const transfer = transfers[0];
    ct.assert_equal(result.timestamp, ct.increment(reference, 3));
    ct.assert_equal(transfer.timestamp, result.timestamp);
}
