const ct = @import("../conformance_test_api.zig");

test "returns a balance per transfer for a history account" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 1, .flags = .{ .history = true } },
        .{ .id = account_2_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_2_id,
            .credit_account_id = account_1_id,
            .amount = 20,
            .ledger = 1,
            .code = 1,
        },
    });

    const balances = ct.get_account_balances(.{
        .account_id = account_1_id,
        .limit = 10,
        .flags = .{ .debits = true, .credits = true },
    });

    ct.assert_equal(balances, .{
        .{ .debits_posted = 10, .credits_posted = 0 },
        .{ .debits_posted = 10, .credits_posted = 20 },
    });
}

test "returns pending balances for a history account" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 1, .flags = .{ .history = true } },
        .{ .id = account_2_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 30,
            .ledger = 1,
            .code = 1,
            .flags = .{ .pending = true },
        },
    });

    const balances = ct.get_account_balances(.{
        .account_id = account_1_id,
        .limit = 10,
        .flags = .{ .debits = true, .credits = true },
    });

    ct.assert_equal(balances, .{
        .{
            .debits_pending = 30,
            .debits_posted = 0,
            .credits_pending = 0,
            .credits_posted = 0,
        },
    });
}

test "pairs each balance with the transfer that produced it" {
    const account_id = ct.generate_id();
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_id, .ledger = 1, .code = 1, .flags = .{ .history = true } },
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    const transfer_1_id = ct.generate_id();
    const transfer_2_id = ct.generate_id();
    const transfer_3_id = ct.generate_id();
    const transfer_4_id = ct.generate_id();
    ct.create_transfers(.{
        .{
            .id = transfer_1_id,
            .debit_account_id = account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_2_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = account_id,
            .amount = 20,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_3_id,
            .debit_account_id = account_id,
            .credit_account_id = credit_account_id,
            .amount = 30,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_4_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = account_id,
            .amount = 40,
            .ledger = 1,
            .code = 1,
        },
    });

    const transfers = ct.get_account_transfers(.{
        .account_id = account_id,
        .limit = 10,
        .flags = .{ .debits = true, .credits = true },
    });
    ct.assert_equal(transfers, .{
        .{ .id = transfer_1_id },
        .{ .id = transfer_2_id },
        .{ .id = transfer_3_id },
        .{ .id = transfer_4_id },
    });

    const balances = ct.get_account_balances(.{
        .account_id = account_id,
        .limit = 10,
        .flags = .{ .debits = true, .credits = true },
    });
    ct.assert_equal(balances, .{
        .{ .debits_posted = 10, .credits_posted = 0 },
        .{ .debits_posted = 10, .credits_posted = 20 },
        .{ .debits_posted = 40, .credits_posted = 20 },
        .{ .debits_posted = 40, .credits_posted = 60 },
    });

    const transfer_1 = transfers[0];
    const transfer_2 = transfers[1];
    const transfer_3 = transfers[2];
    const transfer_4 = transfers[3];
    const balance_1 = balances[0];
    const balance_2 = balances[1];
    const balance_3 = balances[2];
    const balance_4 = balances[3];
    ct.assert_equal(balance_1.timestamp, transfer_1.timestamp);
    ct.assert_equal(balance_2.timestamp, transfer_2.timestamp);
    ct.assert_equal(balance_3.timestamp, transfer_3.timestamp);
    ct.assert_equal(balance_4.timestamp, transfer_4.timestamp);
}

test "pairs debit balances with debit transfers" {
    const account_id = ct.generate_id();
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_id, .ledger = 1, .code = 1, .flags = .{ .history = true } },
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    const transfer_1_id = ct.generate_id();
    const transfer_3_id = ct.generate_id();
    ct.create_transfers(.{
        .{
            .id = transfer_1_id,
            .debit_account_id = account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = account_id,
            .amount = 20,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_3_id,
            .debit_account_id = account_id,
            .credit_account_id = credit_account_id,
            .amount = 30,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = account_id,
            .amount = 40,
            .ledger = 1,
            .code = 1,
        },
    });

    const transfers = ct.get_account_transfers(.{
        .account_id = account_id,
        .limit = 10,
        .flags = .{ .debits = true },
    });
    ct.assert_equal(transfers, .{
        .{ .id = transfer_1_id },
        .{ .id = transfer_3_id },
    });

    const balances = ct.get_account_balances(.{
        .account_id = account_id,
        .limit = 10,
        .flags = .{ .debits = true },
    });
    ct.assert_equal(balances, .{
        .{ .debits_posted = 10, .credits_posted = 0 },
        .{ .debits_posted = 40, .credits_posted = 20 },
    });

    const transfer_1 = transfers[0];
    const transfer_3 = transfers[1];
    const balance_1 = balances[0];
    const balance_3 = balances[1];
    ct.assert_equal(balance_1.timestamp, transfer_1.timestamp);
    ct.assert_equal(balance_3.timestamp, transfer_3.timestamp);
}

test "pairs credit balances with credit transfers" {
    const account_id = ct.generate_id();
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_id, .ledger = 1, .code = 1, .flags = .{ .history = true } },
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    const transfer_2_id = ct.generate_id();
    const transfer_4_id = ct.generate_id();
    ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_2_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = account_id,
            .amount = 20,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_id,
            .credit_account_id = credit_account_id,
            .amount = 30,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_4_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = account_id,
            .amount = 40,
            .ledger = 1,
            .code = 1,
        },
    });

    const transfers = ct.get_account_transfers(.{
        .account_id = account_id,
        .limit = 10,
        .flags = .{ .credits = true },
    });
    ct.assert_equal(transfers, .{
        .{ .id = transfer_2_id },
        .{ .id = transfer_4_id },
    });

    const balances = ct.get_account_balances(.{
        .account_id = account_id,
        .limit = 10,
        .flags = .{ .credits = true },
    });
    ct.assert_equal(balances, .{
        .{ .debits_posted = 10, .credits_posted = 20 },
        .{ .debits_posted = 40, .credits_posted = 60 },
    });

    const transfer_2 = transfers[0];
    const transfer_4 = transfers[1];
    const balance_2 = balances[0];
    const balance_4 = balances[1];
    ct.assert_equal(balance_2.timestamp, transfer_2.timestamp);
    ct.assert_equal(balance_4.timestamp, transfer_4.timestamp);
}

test "pairs debit balances with debit transfers in reverse order" {
    const account_id = ct.generate_id();
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_id, .ledger = 1, .code = 1, .flags = .{ .history = true } },
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    const transfer_1_id = ct.generate_id();
    const transfer_3_id = ct.generate_id();
    ct.create_transfers(.{
        .{
            .id = transfer_1_id,
            .debit_account_id = account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = account_id,
            .amount = 20,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_3_id,
            .debit_account_id = account_id,
            .credit_account_id = credit_account_id,
            .amount = 30,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = account_id,
            .amount = 40,
            .ledger = 1,
            .code = 1,
        },
    });

    const transfers = ct.get_account_transfers(.{
        .account_id = account_id,
        .limit = 10,
        .flags = .{ .debits = true, .reversed = true },
    });
    ct.assert_equal(transfers, .{
        .{ .id = transfer_3_id },
        .{ .id = transfer_1_id },
    });

    const balances = ct.get_account_balances(.{
        .account_id = account_id,
        .limit = 10,
        .flags = .{ .debits = true, .reversed = true },
    });
    ct.assert_equal(balances, .{
        .{ .debits_posted = 40, .credits_posted = 20 },
        .{ .debits_posted = 10, .credits_posted = 0 },
    });

    const transfer_3 = transfers[0];
    const transfer_1 = transfers[1];
    const balance_3 = balances[0];
    const balance_1 = balances[1];
    ct.assert_equal(balance_3.timestamp, transfer_3.timestamp);
    ct.assert_equal(balance_1.timestamp, transfer_1.timestamp);
}

test "pairs credit balances with credit transfers in reverse order" {
    const account_id = ct.generate_id();
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_id, .ledger = 1, .code = 1, .flags = .{ .history = true } },
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    const transfer_2_id = ct.generate_id();
    const transfer_4_id = ct.generate_id();
    ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_2_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = account_id,
            .amount = 20,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_id,
            .credit_account_id = credit_account_id,
            .amount = 30,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_4_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = account_id,
            .amount = 40,
            .ledger = 1,
            .code = 1,
        },
    });

    const transfers = ct.get_account_transfers(.{
        .account_id = account_id,
        .limit = 10,
        .flags = .{ .credits = true, .reversed = true },
    });
    ct.assert_equal(transfers, .{
        .{ .id = transfer_4_id },
        .{ .id = transfer_2_id },
    });

    const balances = ct.get_account_balances(.{
        .account_id = account_id,
        .limit = 10,
        .flags = .{ .credits = true, .reversed = true },
    });
    ct.assert_equal(balances, .{
        .{ .debits_posted = 40, .credits_posted = 60 },
        .{ .debits_posted = 10, .credits_posted = 20 },
    });

    const transfer_4 = transfers[0];
    const transfer_2 = transfers[1];
    const balance_4 = balances[0];
    const balance_2 = balances[1];
    ct.assert_equal(balance_4.timestamp, transfer_4.timestamp);
    ct.assert_equal(balance_2.timestamp, transfer_2.timestamp);
}

test "returns balances in reverse order with the reversed flag" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 1, .flags = .{ .history = true } },
        .{ .id = account_2_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_2_id,
            .credit_account_id = account_1_id,
            .amount = 20,
            .ledger = 1,
            .code = 1,
        },
    });

    const balances = ct.get_account_balances(.{
        .account_id = account_1_id,
        .limit = 10,
        .flags = .{ .debits = true, .credits = true, .reversed = true },
    });

    ct.assert_equal(balances, .{
        .{ .debits_posted = 10, .credits_posted = 20 },
        .{ .debits_posted = 10, .credits_posted = 0 },
    });
}

test "pairs each balance with its transfer in reverse order" {
    const account_id = ct.generate_id();
    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_id, .ledger = 1, .code = 1, .flags = .{ .history = true } },
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    const transfer_1_id = ct.generate_id();
    const transfer_2_id = ct.generate_id();
    const transfer_3_id = ct.generate_id();
    const transfer_4_id = ct.generate_id();
    ct.create_transfers(.{
        .{
            .id = transfer_1_id,
            .debit_account_id = account_id,
            .credit_account_id = credit_account_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_2_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = account_id,
            .amount = 20,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_3_id,
            .debit_account_id = account_id,
            .credit_account_id = credit_account_id,
            .amount = 30,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_4_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = account_id,
            .amount = 40,
            .ledger = 1,
            .code = 1,
        },
    });

    const transfers = ct.get_account_transfers(.{
        .account_id = account_id,
        .limit = 10,
        .flags = .{ .debits = true, .credits = true, .reversed = true },
    });
    ct.assert_equal(transfers, .{
        .{ .id = transfer_4_id },
        .{ .id = transfer_3_id },
        .{ .id = transfer_2_id },
        .{ .id = transfer_1_id },
    });

    const balances = ct.get_account_balances(.{
        .account_id = account_id,
        .limit = 10,
        .flags = .{ .debits = true, .credits = true, .reversed = true },
    });
    ct.assert_equal(balances, .{
        .{ .debits_posted = 40, .credits_posted = 60 },
        .{ .debits_posted = 40, .credits_posted = 20 },
        .{ .debits_posted = 10, .credits_posted = 20 },
        .{ .debits_posted = 10, .credits_posted = 0 },
    });

    const transfer_4 = transfers[0];
    const transfer_3 = transfers[1];
    const transfer_2 = transfers[2];
    const transfer_1 = transfers[3];
    const balance_4 = balances[0];
    const balance_3 = balances[1];
    const balance_2 = balances[2];
    const balance_1 = balances[3];
    ct.assert_equal(balance_4.timestamp, transfer_4.timestamp);
    ct.assert_equal(balance_3.timestamp, transfer_3.timestamp);
    ct.assert_equal(balance_2.timestamp, transfer_2.timestamp);
    ct.assert_equal(balance_1.timestamp, transfer_1.timestamp);
}

test "pages through balances with a timestamp cursor" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 1, .flags = .{ .history = true } },
        .{ .id = account_2_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_2_id,
            .credit_account_id = account_1_id,
            .amount = 20,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 30,
            .ledger = 1,
            .code = 1,
        },
    });

    const page_1 = ct.get_account_balances(.{
        .account_id = account_1_id,
        .limit = 2,
        .flags = .{ .debits = true, .credits = true },
    });

    ct.assert_equal(page_1, .{
        .{ .debits_posted = 10, .credits_posted = 0 },
        .{ .debits_posted = 10, .credits_posted = 20 },
    });

    const page_1_last = page_1[1];
    const cursor_1 = page_1_last.timestamp;
    const page_2 = ct.get_account_balances(.{
        .account_id = account_1_id,
        .timestamp_min = ct.increment(cursor_1, 1),
        .limit = 2,
        .flags = .{ .debits = true, .credits = true },
    });

    ct.assert_equal(page_2, .{.{ .debits_posted = 40, .credits_posted = 20 }});

    const page_2_last = page_2[0];
    const cursor_2 = page_2_last.timestamp;
    const page_3 = ct.get_account_balances(.{
        .account_id = account_1_id,
        .timestamp_min = ct.increment(cursor_2, 1),
        .limit = 2,
        .flags = .{ .debits = true, .credits = true },
    });

    ct.assert_empty(page_3);
}

test "pairs each balance with its transfer across pages" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    const transfer_1_id = ct.generate_id();
    const transfer_2_id = ct.generate_id();
    const transfer_3_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 1, .flags = .{ .history = true } },
        .{ .id = account_2_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = transfer_1_id,
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_2_id,
            .debit_account_id = account_2_id,
            .credit_account_id = account_1_id,
            .amount = 20,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_3_id,
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 30,
            .ledger = 1,
            .code = 1,
        },
    });

    const transfers_page_1 = ct.get_account_transfers(.{
        .account_id = account_1_id,
        .limit = 2,
        .flags = .{ .debits = true, .credits = true },
    });
    ct.assert_equal(transfers_page_1, .{
        .{ .id = transfer_1_id },
        .{ .id = transfer_2_id },
    });
    const balances_page_1 = ct.get_account_balances(.{
        .account_id = account_1_id,
        .limit = 2,
        .flags = .{ .debits = true, .credits = true },
    });
    ct.assert_equal(balances_page_1, .{
        .{ .debits_posted = 10, .credits_posted = 0 },
        .{ .debits_posted = 10, .credits_posted = 20 },
    });
    const transfer_1 = transfers_page_1[0];
    const transfer_2 = transfers_page_1[1];
    const balance_1 = balances_page_1[0];
    const balance_2 = balances_page_1[1];
    ct.assert_equal(balance_1.timestamp, transfer_1.timestamp);
    ct.assert_equal(balance_2.timestamp, transfer_2.timestamp);

    const cursor_1 = transfer_2.timestamp;
    const transfers_page_2 = ct.get_account_transfers(.{
        .account_id = account_1_id,
        .timestamp_min = ct.increment(cursor_1, 1),
        .limit = 2,
        .flags = .{ .debits = true, .credits = true },
    });
    ct.assert_equal(transfers_page_2, .{.{ .id = transfer_3_id }});
    const balances_page_2 = ct.get_account_balances(.{
        .account_id = account_1_id,
        .timestamp_min = ct.increment(cursor_1, 1),
        .limit = 2,
        .flags = .{ .debits = true, .credits = true },
    });
    ct.assert_equal(balances_page_2, .{.{ .debits_posted = 40, .credits_posted = 20 }});
    const transfer_3 = transfers_page_2[0];
    const balance_3 = balances_page_2[0];
    ct.assert_equal(balance_3.timestamp, transfer_3.timestamp);

    const cursor_2 = transfer_3.timestamp;
    const transfers_page_3 = ct.get_account_transfers(.{
        .account_id = account_1_id,
        .timestamp_min = ct.increment(cursor_2, 1),
        .limit = 2,
        .flags = .{ .debits = true, .credits = true },
    });
    ct.assert_empty(transfers_page_3);
    const balances_page_3 = ct.get_account_balances(.{
        .account_id = account_1_id,
        .timestamp_min = ct.increment(cursor_2, 1),
        .limit = 2,
        .flags = .{ .debits = true, .credits = true },
    });
    ct.assert_empty(balances_page_3);
}

test "pages through reversed balances with a timestamp cursor" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 1, .flags = .{ .history = true } },
        .{ .id = account_2_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_2_id,
            .credit_account_id = account_1_id,
            .amount = 20,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 30,
            .ledger = 1,
            .code = 1,
        },
    });

    const page_1 = ct.get_account_balances(.{
        .account_id = account_1_id,
        .limit = 2,
        .flags = .{ .debits = true, .credits = true, .reversed = true },
    });

    ct.assert_equal(page_1, .{
        .{ .debits_posted = 40, .credits_posted = 20 },
        .{ .debits_posted = 10, .credits_posted = 20 },
    });

    const page_1_last = page_1[1];
    const cursor_1 = page_1_last.timestamp;
    const page_2 = ct.get_account_balances(.{
        .account_id = account_1_id,
        .timestamp_max = ct.decrement(cursor_1, 1),
        .limit = 2,
        .flags = .{ .debits = true, .credits = true, .reversed = true },
    });

    ct.assert_equal(page_2, .{.{ .debits_posted = 10, .credits_posted = 0 }});

    const page_2_last = page_2[0];
    const cursor_2 = page_2_last.timestamp;
    const page_3 = ct.get_account_balances(.{
        .account_id = account_1_id,
        .timestamp_max = ct.decrement(cursor_2, 1),
        .limit = 2,
        .flags = .{ .debits = true, .credits = true, .reversed = true },
    });

    ct.assert_empty(page_3);
}

test "pairs each balance with its transfer across reversed pages" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    const transfer_1_id = ct.generate_id();
    const transfer_2_id = ct.generate_id();
    const transfer_3_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 1, .flags = .{ .history = true } },
        .{ .id = account_2_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = transfer_1_id,
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_2_id,
            .debit_account_id = account_2_id,
            .credit_account_id = account_1_id,
            .amount = 20,
            .ledger = 1,
            .code = 1,
        },
        .{
            .id = transfer_3_id,
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 30,
            .ledger = 1,
            .code = 1,
        },
    });

    const transfers_page_1 = ct.get_account_transfers(.{
        .account_id = account_1_id,
        .limit = 2,
        .flags = .{ .debits = true, .credits = true, .reversed = true },
    });
    ct.assert_equal(transfers_page_1, .{
        .{ .id = transfer_3_id },
        .{ .id = transfer_2_id },
    });
    const balances_page_1 = ct.get_account_balances(.{
        .account_id = account_1_id,
        .limit = 2,
        .flags = .{ .debits = true, .credits = true, .reversed = true },
    });
    ct.assert_equal(balances_page_1, .{
        .{ .debits_posted = 40, .credits_posted = 20 },
        .{ .debits_posted = 10, .credits_posted = 20 },
    });
    const transfer_3 = transfers_page_1[0];
    const transfer_2 = transfers_page_1[1];
    const balance_3 = balances_page_1[0];
    const balance_2 = balances_page_1[1];
    ct.assert_equal(balance_3.timestamp, transfer_3.timestamp);
    ct.assert_equal(balance_2.timestamp, transfer_2.timestamp);

    const cursor_1 = transfer_2.timestamp;
    const transfers_page_2 = ct.get_account_transfers(.{
        .account_id = account_1_id,
        .timestamp_max = ct.decrement(cursor_1, 1),
        .limit = 2,
        .flags = .{ .debits = true, .credits = true, .reversed = true },
    });
    ct.assert_equal(transfers_page_2, .{.{ .id = transfer_1_id }});
    const balances_page_2 = ct.get_account_balances(.{
        .account_id = account_1_id,
        .timestamp_max = ct.decrement(cursor_1, 1),
        .limit = 2,
        .flags = .{ .debits = true, .credits = true, .reversed = true },
    });
    ct.assert_equal(balances_page_2, .{.{ .debits_posted = 10, .credits_posted = 0 }});
    const transfer_1 = transfers_page_2[0];
    const balance_1 = balances_page_2[0];
    ct.assert_equal(balance_1.timestamp, transfer_1.timestamp);

    const cursor_2 = transfer_1.timestamp;
    const transfers_page_3 = ct.get_account_transfers(.{
        .account_id = account_1_id,
        .timestamp_max = ct.decrement(cursor_2, 1),
        .limit = 2,
        .flags = .{ .debits = true, .credits = true, .reversed = true },
    });
    ct.assert_empty(transfers_page_3);
    const balances_page_3 = ct.get_account_balances(.{
        .account_id = account_1_id,
        .timestamp_max = ct.decrement(cursor_2, 1),
        .limit = 2,
        .flags = .{ .debits = true, .credits = true, .reversed = true },
    });
    ct.assert_empty(balances_page_3);
}

test "returns no balances without the history flag" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 1 },
        .{ .id = account_2_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    });

    const balances = ct.get_account_balances(.{
        .account_id = account_1_id,
        .limit = 10,
        .flags = .{ .debits = true, .credits = true },
    });

    ct.assert_empty(balances);
}

test "returns no balances for an account with no transfers" {
    const account_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_id, .ledger = 1, .code = 1, .flags = .{ .history = true } },
    });

    const balances = ct.get_account_balances(.{
        .account_id = account_id,
        .limit = 10,
        .flags = .{ .debits = true, .credits = true },
    });

    ct.assert_empty(balances);
}

test "returns no balances for a zero account id" {
    const balances = ct.get_account_balances(.{
        .account_id = 0,
        .limit = 10,
        .flags = .{ .debits = true, .credits = true },
    });

    ct.assert_empty(balances);
}

test "returns no balances for a default filter" {
    const balances = ct.get_account_balances(.{});

    ct.assert_empty(balances);
}

test "returns no balances for a zero limit" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 1, .flags = .{ .history = true } },
        .{ .id = account_2_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    });

    const balances = ct.get_account_balances(.{
        .account_id = account_1_id,
        .limit = 0,
        .flags = .{ .debits = true, .credits = true },
    });

    ct.assert_empty(balances);
}

test "returns no balances when the timestamp range is inverted" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 1, .flags = .{ .history = true } },
        .{ .id = account_2_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    });
    const matched = ct.get_account_balances(.{
        .account_id = account_1_id,
        .limit = 10,
        .flags = .{ .debits = true, .credits = true },
    });
    const balance = matched[0];
    const balance_timestamp = balance.timestamp;

    const balances = ct.get_account_balances(.{
        .account_id = account_1_id,
        .timestamp_min = ct.increment(balance_timestamp, 1),
        .timestamp_max = ct.decrement(balance_timestamp, 1),
        .limit = 10,
        .flags = .{ .debits = true, .credits = true },
    });

    ct.assert_empty(balances);
}

test "returns no balances for a timestamp minimum of u64 max" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 1, .flags = .{ .history = true } },
        .{ .id = account_2_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    });
    const balances = ct.get_account_balances(.{
        .account_id = account_1_id,
        .timestamp_min = 18446744073709551615,
        .limit = 10,
        .flags = .{ .debits = true, .credits = true },
    });

    ct.assert_empty(balances);
}

test "returns no balances for a timestamp maximum of u64 max" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 1, .flags = .{ .history = true } },
        .{ .id = account_2_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    });
    const balances = ct.get_account_balances(.{
        .account_id = account_1_id,
        .timestamp_max = 18446744073709551615,
        .limit = 10,
        .flags = .{ .debits = true, .credits = true },
    });

    ct.assert_empty(balances);
}

test "returns no balances for an inverted timestamp range at u64 max" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 1, .flags = .{ .history = true } },
        .{ .id = account_2_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    });
    const balances = ct.get_account_balances(.{
        .account_id = account_1_id,
        .timestamp_min = 18446744073709551614,
        .timestamp_max = 1,
        .limit = 10,
        .flags = .{ .debits = true, .credits = true },
    });

    ct.assert_empty(balances);
}

test "returns no balances without the debits or credits flag" {
    const account_1_id = ct.generate_id();
    const account_2_id = ct.generate_id();
    ct.create_accounts(.{
        .{ .id = account_1_id, .ledger = 1, .code = 1, .flags = .{ .history = true } },
        .{ .id = account_2_id, .ledger = 1, .code = 1 },
    });
    ct.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = account_1_id,
            .credit_account_id = account_2_id,
            .amount = 10,
            .ledger = 1,
            .code = 1,
        },
    });

    const balances = ct.get_account_balances(.{
        .account_id = account_1_id,
        .limit = 10,
    });

    ct.assert_empty(balances);
}

test "fails when the limit is too large" {
    ct.assert_fail(ct.get_account_balances(.{
        .account_id = ct.generate_id(),
        .limit = 10000,
        .flags = .{ .debits = true, .credits = true },
    }));
}
