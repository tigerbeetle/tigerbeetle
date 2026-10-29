const ct = @import("../conformance/api.zig");
const client = ct.default_client;

test "accepts the maximum u128" {
    const accounts = client.lookup_accounts(.{ct.uint128_max});

    ct.assert_empty(accounts);
}

test "rejects a u128 above the maximum" {
    ct.requires_unbounded_integers();

    ct.assert_fail(client.lookup_accounts(.{340282366920938463463374607431768211456}));
}

test "rejects a negative u128" {
    ct.requires_unbounded_integers();

    ct.assert_fail(client.lookup_accounts(.{-1}));
}

test "rejects a u128 above the maximum on a struct field" {
    ct.requires_unbounded_integers();

    ct.assert_fail(client.create_accounts(.{
        .{ .id = 340282366920938463463374607431768211456, .ledger = 1, .code = 1 },
    }));
}

test "rejects a negative u128 on a struct field" {
    ct.requires_unbounded_integers();

    ct.assert_fail(client.create_accounts(.{
        .{ .id = -1, .ledger = 1, .code = 1 },
    }));
}
