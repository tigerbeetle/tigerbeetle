const ct = @import("../conformance_test_api.zig");

test "creates, posts, voids, and expires two-phase transfers" {
    const account_a_id = ct.generate_id();
    const account_b_id = ct.generate_id();

    const account_results_1 = ct.create_accounts(.{
        .{ .id = account_a_id, .ledger = 1, .code = 718 },
    });
    const account_result_1 = account_results_1[0];
    ct.assert_equal(account_result_1.status, .created);
    ct.assert_greater_than(account_result_1.timestamp, 0);

    const account_results_2 = ct.create_accounts(.{
        .{ .id = account_a_id, .ledger = 1, .code = 718 },
        .{ .id = account_b_id, .ledger = 1, .code = 719 },
    });
    const account_result_2 = account_results_2[0];
    const account_result_3 = account_results_2[1];
    ct.assert_equal(account_result_2.status, .exists);
    ct.assert_greater_than(account_result_2.timestamp, 0);
    ct.assert_equal(account_result_3.status, .created);
    ct.assert_greater_than(account_result_3.timestamp, 0);

    const transfer_results_1 = ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_b_id,
            .credit_account_id = account_a_id,
            .amount = 100,
            .ledger = 1,
            .code = 1,
        },
    });
    const transfer_result_1 = transfer_results_1[0];
    ct.assert_equal(transfer_result_1.status, .created);
    ct.assert_greater_than(transfer_result_1.timestamp, 0);

    const accounts_1 = ct.lookup_accounts(.{ account_a_id, account_b_id });
    ct.assert_equal(accounts_1, .{
        .{
            .id = account_a_id,
            .credits_posted = 100,
            .credits_pending = 0,
            .debits_posted = 0,
            .debits_pending = 0,
        },
        .{
            .id = account_b_id,
            .credits_posted = 0,
            .credits_pending = 0,
            .debits_posted = 100,
            .debits_pending = 0,
        },
    });

    const transfer_2_id = ct.generate_id();
    const transfer_results_2 = ct.create_transfers(.{
        .{
            .id = transfer_2_id,
            .debit_account_id = account_b_id,
            .credit_account_id = account_a_id,
            .amount = 50,
            .timeout = 2_000_000_000,
            .ledger = 1,
            .code = 1,
            .flags = .{ .pending = true },
        },
    });
    const transfer_result_2 = transfer_results_2[0];
    ct.assert_equal(transfer_result_2.status, .created);
    ct.assert_greater_than(transfer_result_2.timestamp, 0);

    const accounts_2 = ct.lookup_accounts(.{ account_a_id, account_b_id });
    ct.assert_equal(accounts_2, .{
        .{
            .id = account_a_id,
            .credits_posted = 100,
            .credits_pending = 50,
            .debits_posted = 0,
            .debits_pending = 0,
        },
        .{
            .id = account_b_id,
            .credits_posted = 0,
            .credits_pending = 0,
            .debits_posted = 100,
            .debits_pending = 50,
        },
    });

    const transfers_1 = ct.lookup_transfers(.{transfer_2_id});
    const transfer_lookup_1 = transfers_1[0];
    ct.assert_equal(transfers_1, .{.{
        .id = transfer_2_id,
        .debit_account_id = account_b_id,
        .credit_account_id = account_a_id,
        .amount = 50,
        .user_data_128 = 0,
        .user_data_64 = 0,
        .user_data_32 = 0,
        .code = 1,
        .flags = .{ .pending = true },
    }});
    ct.assert_greater_than(transfer_lookup_1.timeout, 0);

    const commit_results = ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .amount = 340282366920938463463374607431768211455,
            .pending_id = transfer_2_id,
            .ledger = 1,
            .code = 1,
            .flags = .{ .post_pending_transfer = true },
        },
    });
    const commit_result = commit_results[0];
    ct.assert_equal(commit_result.status, .created);
    ct.assert_greater_than(commit_result.timestamp, 0);

    const accounts_3 = ct.lookup_accounts(.{ account_a_id, account_b_id });
    ct.assert_equal(accounts_3, .{
        .{
            .id = account_a_id,
            .credits_posted = 150,
            .credits_pending = 0,
            .debits_posted = 0,
            .debits_pending = 0,
        },
        .{
            .id = account_b_id,
            .credits_posted = 0,
            .credits_pending = 0,
            .debits_posted = 150,
            .debits_pending = 0,
        },
    });

    const transfer_3_id = ct.generate_id();
    const transfer_results_3 = ct.create_transfers(.{
        .{
            .id = transfer_3_id,
            .debit_account_id = account_b_id,
            .credit_account_id = account_a_id,
            .amount = 50,
            .timeout = 1_000_000_000,
            .ledger = 1,
            .code = 1,
            .flags = .{ .pending = true },
        },
    });
    const transfer_result_3 = transfer_results_3[0];
    ct.assert_equal(transfer_result_3.status, .created);
    ct.assert_greater_than(transfer_result_3.timestamp, 0);

    const reject_results = ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .pending_id = transfer_3_id,
            .ledger = 1,
            .code = 1,
            .flags = .{ .void_pending_transfer = true },
        },
    });
    const reject_result = reject_results[0];
    ct.assert_equal(reject_result.status, .created);
    ct.assert_greater_than(reject_result.timestamp, 0);

    const accounts_4 = ct.lookup_accounts(.{ account_a_id, account_b_id });
    ct.assert_equal(accounts_4, .{
        .{
            .id = account_a_id,
            .credits_posted = 150,
            .credits_pending = 0,
            .debits_posted = 0,
            .debits_pending = 0,
        },
        .{
            .id = account_b_id,
            .credits_posted = 0,
            .credits_pending = 0,
            .debits_posted = 150,
            .debits_pending = 0,
        },
    });

    const transfer_4_id = ct.generate_id();
    const transfer_results_4 = ct.create_transfers(.{
        .{
            .id = transfer_4_id,
            .debit_account_id = account_b_id,
            .credit_account_id = account_a_id,
            .amount = 50,
            .timeout = 1,
            .ledger = 1,
            .code = 1,
            .flags = .{ .pending = true },
        },
    });
    const transfer_result_4 = transfer_results_4[0];
    ct.assert_equal(transfer_result_4.status, .created);
    ct.assert_greater_than(transfer_result_4.timestamp, 0);

    const accounts_5 = ct.lookup_accounts(.{ account_a_id, account_b_id });
    ct.assert_equal(accounts_5, .{
        .{
            .id = account_a_id,
            .credits_posted = 150,
            .credits_pending = 50,
            .debits_posted = 0,
            .debits_pending = 0,
        },
        .{
            .id = account_b_id,
            .credits_posted = 0,
            .credits_pending = 0,
            .debits_posted = 150,
            .debits_pending = 50,
        },
    });

    ct.sleep_ms(1500);

    const accounts_6 = ct.lookup_accounts(.{ account_a_id, account_b_id });
    ct.assert_equal(accounts_6, .{
        .{
            .id = account_a_id,
            .credits_posted = 150,
            .credits_pending = 0,
            .debits_posted = 0,
            .debits_pending = 0,
        },
        .{
            .id = account_b_id,
            .credits_posted = 0,
            .credits_pending = 0,
            .debits_posted = 150,
            .debits_pending = 0,
        },
    });

    const expired_results = ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .pending_id = transfer_4_id,
            .ledger = 1,
            .code = 1,
            .flags = .{ .void_pending_transfer = true },
        },
    });
    const expired_result = expired_results[0];
    ct.assert_equal(expired_result.status, .pending_transfer_expired);
}
