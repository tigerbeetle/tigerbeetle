const std = @import("std");
const builtin = @import("builtin");

const assert = std.debug.assert;
const Header = @import("vsr/message_header.zig").Header;

pub const Encryption = struct {
    pub fn encrypt_test(header: *Header) void {
        // assert(builtin.is_test);
        header.body_nonce = 1;
        header.header_nonce = 1;
        header.header_key_id = 1;
        header.set_checksum_body(&[0]u8{});
        header.set_checksum();
    }
};
