const ct = @import("../conformance_test_api.zig");

test "generates unique ids" {
    const ids = ct.generate_ids(1000);

    ct.assert_unique(ids);
}

test "generates monotonically increasing ids" {
    const ids = ct.generate_ids(100);

    ct.assert_ascending(ids);
}
