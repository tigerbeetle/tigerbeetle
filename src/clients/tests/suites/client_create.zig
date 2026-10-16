const ct = @import("../conformance_test_api.zig");

test "creates a client from valid arguments" {
    ct.create_client(.{ .cluster_id = 0, .replica_addresses = ct.replica_addresses() });
}

test "rejects an invalid address" {
    ct.assert_fail_with(
        ct.create_client(.{ .cluster_id = 0, .replica_addresses = "not-an-address" }),
        .address_invalid,
    );
}

test "rejects an address with a port above the maximum" {
    ct.assert_fail_with(
        ct.create_client(.{ .cluster_id = 0, .replica_addresses = "127.0.0.1:99999" }),
        .address_invalid,
    );
}

test "rejects more addresses than replicas" {
    ct.assert_fail_with(ct.create_client(.{
        .cluster_id = 0,
        .replica_addresses = "3001,3002,3003,3004,3005,3006,3007,3008,3009,3010",
    }), .address_limit_exceeded);
}

test "rejects an empty address" {
    ct.assert_fail_with(
        ct.create_client(.{ .cluster_id = 0, .replica_addresses = "" }),
        .address_invalid,
    );
}

test "rejects empty addresses" {
    ct.assert_fail_with(
        ct.create_client(.{ .cluster_id = 0, .replica_addresses = ",," }),
        .address_invalid,
    );
}

test "rejects a cluster_id above the maximum" {
    ct.requires_unbounded_integers();

    ct.assert_fail_with(ct.create_client(.{
        .cluster_id = 340282366920938463463374607431768211456,
        .replica_addresses = ct.replica_addresses(),
    }), .cluster_id_invalid);
}
