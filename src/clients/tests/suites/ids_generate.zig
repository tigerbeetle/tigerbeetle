const ct = @import("../conformance/api.zig");
const client = ct.default_client;

test "generates monotonically increasing ids" {
    const ids = ct.generate_ids(100000);

    ct.assert_id_ascending(ids);
}
