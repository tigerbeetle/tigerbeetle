const ct = @import("../conformance_test_api.zig");

test "fails operations after close" {
    ct.close_client();

    ct.assert_fail(ct.lookup_accounts(.{ct.generate_id()}));
}
