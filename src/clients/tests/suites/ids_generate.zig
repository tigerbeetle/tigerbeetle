const ct = @import("../conformance/api.zig");

test "generates monotonically increasing ids" {
    const ids = ct.generate_ids(100000);

    ct.assert_id_ascending(ids);
}
