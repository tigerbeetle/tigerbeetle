const ct = @import("../conformance_test_api.zig");

test "generates monotonically increasing ids" {
    const ids = ct.generate_ids(100000);

    ct.assert_ascending(ids);
}
