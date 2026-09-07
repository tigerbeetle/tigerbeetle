const ct = @import("../conformance_test_api.zig");

test "accepts an empty batch" {
    const results = ct.create_accounts(.{});

    ct.assert_empty(results);
}

test "creates an account" {
    const results = ct.create_accounts(.{
        .{ .id = ct.generate_id(), .ledger = 1, .code = 1 },
    });

    ct.assert_equal(results, .{.{ .status = .created }});
}

test "returns a result per account in a batch" {
    const results = ct.create_accounts(.{
        .{ .id = ct.generate_id(), .ledger = 1, .code = 1 },
        .{ .id = 0, .ledger = 1, .code = 1 },
        .{ .id = ct.generate_id(), .ledger = 1, .code = 1 },
    });

    ct.assert_equal(results, .{
        .{ .status = .created },
        .{ .status = .id_must_not_be_zero },
        .{ .status = .created },
    });
}

test "returns exists for a duplicate account" {
    const account = ct.Account{ .id = ct.generate_id(), .ledger = 1, .code = 1 };
    ct.create_accounts(.{account});

    const results = ct.create_accounts(.{account});

    ct.assert_equal(results, .{.{ .status = .exists }});
}

test "returns exists with a different ledger" {
    const account_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_id, .ledger = 1, .code = 1 },
    });

    const results = ct.create_accounts(.{
        .{ .id = account_id, .ledger = 2, .code = 1 },
    });

    ct.assert_equal(results, .{.{ .status = .exists_with_different_ledger }});
}

test "rejects a zero id" {
    const results = ct.create_accounts(.{
        .{ .id = 0, .ledger = 1, .code = 1 },
    });

    ct.assert_equal(results, .{.{ .status = .id_must_not_be_zero }});
}

test "rejects an id of the maximum u128" {
    const results = ct.create_accounts(.{
        .{ .id = ct.uint128_max, .ledger = 1, .code = 1 },
    });

    ct.assert_equal(results, .{.{ .status = .id_must_not_be_int_max }});
}

test "rejects a zero ledger" {
    const results = ct.create_accounts(.{
        .{ .id = ct.generate_id(), .ledger = 0, .code = 1 },
    });

    ct.assert_equal(results, .{.{ .status = .ledger_must_not_be_zero }});
}

test "rejects a zero code" {
    const results = ct.create_accounts(.{
        .{ .id = ct.generate_id(), .ledger = 1, .code = 0 },
    });

    ct.assert_equal(results, .{.{ .status = .code_must_not_be_zero }});
}

test "rejects a non-zero debits pending" {
    const results = ct.create_accounts(.{
        .{ .id = ct.generate_id(), .ledger = 1, .code = 1, .debits_pending = 1 },
    });

    ct.assert_equal(results, .{.{ .status = .debits_pending_must_be_zero }});
}

test "rejects a non-zero debits posted" {
    const results = ct.create_accounts(.{
        .{ .id = ct.generate_id(), .ledger = 1, .code = 1, .debits_posted = 1 },
    });

    ct.assert_equal(results, .{.{ .status = .debits_posted_must_be_zero }});
}

test "rejects a non-zero credits pending" {
    const results = ct.create_accounts(.{
        .{ .id = ct.generate_id(), .ledger = 1, .code = 1, .credits_pending = 1 },
    });

    ct.assert_equal(results, .{.{ .status = .credits_pending_must_be_zero }});
}

test "rejects a non-zero credits posted" {
    const results = ct.create_accounts(.{
        .{ .id = ct.generate_id(), .ledger = 1, .code = 1, .credits_posted = 1 },
    });

    ct.assert_equal(results, .{.{ .status = .credits_posted_must_be_zero }});
}

test "rejects mutually exclusive flags" {
    const results = ct.create_accounts(.{
        .{
            .id = ct.generate_id(),
            .ledger = 1,
            .code = 1,
            .flags = .{
                .debits_must_not_exceed_credits = true,
                .credits_must_not_exceed_debits = true,
            },
        },
    });

    ct.assert_equal(results, .{.{ .status = .flags_are_mutually_exclusive }});
}

test "rejects a non-zero timestamp" {
    const results = ct.create_accounts(.{
        .{ .id = ct.generate_id(), .ledger = 1, .code = 1, .timestamp = 2 },
    });

    ct.assert_equal(results, .{.{ .status = .timestamp_must_be_zero }});
}
