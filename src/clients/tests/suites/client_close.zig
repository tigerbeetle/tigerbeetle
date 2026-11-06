const ct = @import("../conformance_test_api.zig");

test "fails operations after close" {
    ct.close_client();

    ct.assert_fail_with(ct.lookup_accounts(.{ct.generate_id()}), .client_closed);
}

test "fails a second close" {
    ct.requires_raise_on_double_close();

    ct.close_client();

    ct.assert_fail_with(ct.close_client(), .client_closed);
}
