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

test "returns exists for a duplicate account" {
    const account = ct.Account{ .id = ct.generate_id(), .ledger = 1, .code = 1 };
    ct.create_accounts(.{account});

    const results = ct.create_accounts(.{account});

    ct.assert_equal(results, .{.{ .status = .exists }});
}

test "rejects a zero id" {
    const results = ct.create_accounts(.{
        .{ .id = 0, .ledger = 1, .code = 1 },
    });

    ct.assert_equal(results, .{.{ .status = .id_must_not_be_zero }});
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
