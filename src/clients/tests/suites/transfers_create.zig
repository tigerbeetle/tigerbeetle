const ct = @import("../conformance/api.zig");
const client = ct.default_client;

test "accepts an empty batch" {
    const results = client.create_transfers(.{});

    ct.assert_empty(results);
}

test "creates a transfer" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    client.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    const results = client.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 100,
            .ledger = 1,
            .code = 1,
        },
    });

    ct.assert_equal(results, .{.{ .status = .created }});
}

test "returns a result per transfer in a batch" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    client.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    const results = client.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = 0,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    });

    ct.assert_equal(results, .{
        .{ .status = .created },
        .{ .status = .id_must_not_be_zero },
        .{ .status = .created },
    });
}

test "returns exists for a duplicate transfer" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    const transfer_id = ct.generate_id();
    client.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });
    const created_results = client.create_transfers(.{
        .{
            .id = transfer_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 100,
            .ledger = 1,
            .code = 1,
        },
    });

    const results = client.create_transfers(.{
        .{
            .id = transfer_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 100,
            .ledger = 1,
            .code = 1,
        },
    });

    ct.assert_equal(results, .{.{ .status = .exists }});

    const created_result = created_results[0];
    const result = results[0];
    ct.assert_equal(result.timestamp, created_result.timestamp);
}

test "returns exists with a different amount" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    client.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });
    const transfer_id = ct.generate_id();
    client.create_transfers(.{
        .{
            .id = transfer_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 100,
            .ledger = 1,
            .code = 1,
        },
    });

    const results = client.create_transfers(.{
        .{
            .id = transfer_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 200,
            .ledger = 1,
            .code = 1,
        },
    });

    ct.assert_equal(results, .{.{ .status = .exists_with_different_amount }});
}

test "rejects a zero id" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    client.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    const results = client.create_transfers(.{
        .{
            .id = 0,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    });

    ct.assert_equal(results, .{.{ .status = .id_must_not_be_zero }});
}

test "rejects an id of the maximum u128" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    client.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    const results = client.create_transfers(.{
        .{
            .id = ct.uint128_max,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    });

    ct.assert_equal(results, .{.{ .status = .id_must_not_be_int_max }});
}

test "rejects a zero debit account id" {
    const credit_account_id = ct.generate_id();
    client.create_accounts(.{
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    const results = client.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = 0,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    });

    ct.assert_equal(results, .{.{ .status = .debit_account_id_must_not_be_zero }});
}

test "rejects a debit account id of the maximum u128" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    client.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    const results = client.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = ct.uint128_max,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    });

    ct.assert_equal(results, .{.{ .status = .debit_account_id_must_not_be_int_max }});
}

test "rejects a zero credit account id" {
    const debit_account_id = ct.generate_id();
    client.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
    });

    const results = client.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = 0,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    });

    ct.assert_equal(results, .{.{ .status = .credit_account_id_must_not_be_zero }});
}

test "rejects a credit account id of the maximum u128" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    client.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    const results = client.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = ct.uint128_max,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    });

    ct.assert_equal(results, .{.{ .status = .credit_account_id_must_not_be_int_max }});
}

test "rejects a zero ledger" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    client.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    const results = client.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 0,
            .code = 1,
        },
    });

    ct.assert_equal(results, .{.{ .status = .ledger_must_not_be_zero }});
}

test "rejects a zero code" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    client.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    const results = client.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 0,
        },
    });

    ct.assert_equal(results, .{.{ .status = .code_must_not_be_zero }});
}

test "rejects identical debit and credit accounts" {
    const account_id = ct.generate_id();
    client.create_accounts(.{
        .{ .id = account_id, .ledger = 1, .code = 1 },
    });

    const results = client.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_id,
            .credit_account_id = account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    });

    ct.assert_equal(results, .{.{ .status = .accounts_must_be_different }});
}

test "rejects an unknown debit account" {
    const credit_account_id = ct.generate_id();
    client.create_accounts(.{
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    const results = client.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = ct.generate_id(),
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    });

    ct.assert_equal(results, .{.{ .status = .debit_account_not_found }});
}

test "rejects an unknown credit account" {
    const debit_account_id = ct.generate_id();
    client.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
    });

    const results = client.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = ct.generate_id(),
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    });

    ct.assert_equal(results, .{.{ .status = .credit_account_not_found }});
}

test "rejects accounts on different ledgers" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    client.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 2, .code = 1 },
    });

    const results = client.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    });

    ct.assert_equal(results, .{.{ .status = .accounts_must_have_the_same_ledger }});
}

test "rejects a transfer on a different ledger to its accounts" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    client.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    const results = client.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 2,
            .code = 1,
        },
    });

    ct.assert_equal(results, .{.{ .status = .transfer_must_have_the_same_ledger_as_accounts }});
}

test "rejects mutually exclusive flags" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    client.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    const results = client.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
            .flags = .{ .post_pending_transfer = true, .void_pending_transfer = true },
        },
    });

    ct.assert_equal(results, .{.{ .status = .flags_are_mutually_exclusive }});
}

test "rejects a non-zero timestamp" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    client.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    const results = client.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
            .timestamp = 2,
        },
    });

    ct.assert_equal(results, .{.{ .status = .timestamp_must_be_zero }});
}

test "rejects a transfer exceeding credits" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    client.create_accounts(.{
        .{
            .id = debit_account_id,
            .ledger = 1,
            .code = 1,
            .flags = .{ .debits_must_not_exceed_credits = true },
        },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    const results = client.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    });

    ct.assert_equal(results, .{.{ .status = .exceeds_credits }});
}

test "rejects a transfer exceeding debits" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    client.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{
            .id = credit_account_id,
            .ledger = 1,
            .code = 1,
            .flags = .{ .credits_must_not_exceed_debits = true },
        },
    });

    const results = client.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    });

    ct.assert_equal(results, .{.{ .status = .exceeds_debits }});
}

test "accepts a transfer once credits allow it" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    client.create_accounts(.{
        .{
            .id = debit_account_id,
            .ledger = 1,
            .code = 1,
            .flags = .{ .debits_must_not_exceed_credits = true },
        },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });
    client.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = credit_account_id,
            .credit_account_id = debit_account_id,
            .amount = 100,
            .ledger = 1,
            .code = 1,
        },
    });

    const results = client.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    });

    ct.assert_equal(results, .{.{ .status = .created }});
}

test "rejects a transfer that leaves a linked chain open" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    client.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    const results = client.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 100,
            .ledger = 1,
            .code = 1,
            .flags = .{ .linked = true },
        },
    });

    ct.assert_equal(results, .{.{ .status = .linked_event_chain_open }});

    const accounts = client.lookup_accounts(.{ debit_account_id, credit_account_id });
    ct.assert_equal(accounts, .{
        .{ .id = debit_account_id, .debits_posted = 0, .credits_posted = 0 },
        .{ .id = credit_account_id, .debits_posted = 0, .credits_posted = 0 },
    });
}

test "rejects a linked chain when a transfer in it fails" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    const transfer_id = ct.generate_id();
    client.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    const results = client.create_transfers(.{
        .{
            .id = transfer_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 100,
            .ledger = 1,
            .code = 1,
            .flags = .{ .linked = true },
        },
        .{
            .id = transfer_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 100,
            .ledger = 1,
            .code = 1,
        },
    });

    ct.assert_equal(results, .{
        .{ .status = .linked_event_failed },
        .{ .status = .exists_with_different_flags },
    });

    const accounts = client.lookup_accounts(.{ debit_account_id, credit_account_id });
    ct.assert_equal(accounts, .{
        .{ .id = debit_account_id, .debits_posted = 0, .credits_posted = 0 },
        .{ .id = credit_account_id, .debits_posted = 0, .credits_posted = 0 },
    });
}

test "rejects an id reused after a transient failure" {
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    client.create_accounts(.{
        .{
            .id = debit_account_id,
            .ledger = 1,
            .code = 1,
            .flags = .{ .debits_must_not_exceed_credits = true },
        },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });
    const transfer_id = ct.generate_id();
    client.create_transfers(.{
        .{
            .id = transfer_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    });
    client.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = credit_account_id,
            .credit_account_id = debit_account_id,
            .amount = 100,
            .ledger = 1,
            .code = 1,
        },
    });

    const results = client.create_transfers(.{
        .{
            .id = transfer_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    });

    ct.assert_equal(results, .{.{ .status = .id_already_failed }});
}

test "rejects a fractional amount" {
    ct.requires_fractional_amounts();

    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    client.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    ct.assert_fail(client.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 1.5,
            .ledger = 1,
            .code = 1,
        },
    }));
}
