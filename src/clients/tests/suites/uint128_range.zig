const ct = @import("../conformance_test_api.zig");

test "accepts the maximum u128" {
    const uint128_max = ct.U128{ .value = 340282366920938463463374607431768211455 };

    const accounts = ct.lookup_accounts(.{uint128_max});

    ct.assert_empty(accounts);
}

test "rejects a u128 above the maximum" {
    ct.requires_unbounded_integers();

    ct.assert_fail(ct.lookup_accounts(.{340282366920938463463374607431768211456}));
}

test "rejects a negative u128" {
    ct.requires_unbounded_integers();

    ct.assert_fail(ct.lookup_accounts(.{-1}));
}

test "rejects a u128 above the maximum on a struct field" {
    ct.requires_unbounded_integers();

    ct.assert_fail(ct.create_accounts(.{
        .{ .id = 340282366920938463463374607431768211456, .ledger = 1, .code = 1 },
    }));
}

test "rejects a negative u128 on a struct field" {
    ct.requires_unbounded_integers();

    ct.assert_fail(ct.create_accounts(.{
        .{ .id = -1, .ledger = 1, .code = 1 },
    }));
}
