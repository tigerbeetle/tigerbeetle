const ct = @import("../conformance_test_api.zig");

test "fails operations after close" {
    ct.close_client();

    ct.assert_fail_with(ct.lookup_accounts(.{ct.generate_id()}), .client_closed);
}

// Closing the client also doubles as cancellation. To avoid any manual synchronization in
// concurrent code, we are forgiving when closing an already closed client.
test "ignores a second close" {
    ct.close_client();

    ct.close_client();
}
