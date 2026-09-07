const ct = @import("../conformance_test_api.zig");

test "fails operations after close" {
    ct.close_client();

    ct.assert_fail(ct.lookup_accounts(.{ct.generate_id()}));
}

test "fails a second close" {
    ct.requires_raise_on_double_close();

    ct.close_client();

    ct.assert_fail(ct.close_client());
}
