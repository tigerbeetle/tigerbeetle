const ct = @import("../conformance/api.zig");
const client = ct.default_client;

test "fails operations after close" {
    client.close_client();

    ct.assert_fail_with(client.lookup_accounts(.{ct.generate_id()}), .client_closed);
}

// Closing the client also doubles as cancellation. To avoid any manual synchronization in
// concurrent code, we are forgiving when closing an already closed client.
test "ignores a second close" {
    client.close_client();

    client.close_client();
}
