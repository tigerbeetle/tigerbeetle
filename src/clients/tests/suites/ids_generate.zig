const ct = @import("../conformance_test_api.zig");
const client = ct.default_client;

test "generates monotonically increasing ids" {
    const ids = ct.generate_ids(100000);

    ct.assert_ascending(ids);
}
