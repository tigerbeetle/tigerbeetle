const std = @import("std");
const builtin = @import("builtin");
const stdx = @import("stdx");

const assert = std.debug.assert;
const Header = @import("vsr/message_header.zig").Header;
const aegis = std.crypto.aead.aegis;
const aegis_auth = std.crypto.auth.aegis;

pub const Encryption = struct {
    pub fn encrypt_test(header: *Header) void {
        // assert(builtin.is_test);
        header.body_nonce = 1;
        header.header_nonce = 1;
        header.header_key_id = 1;
        header.set_checksum_body(&[0]u8{});
        header.set_checksum();
    }

    pub const TransitState = struct {
        pub fn encrypt() void {}
    };

    pub const StorageState = struct {
        pub fn encrypt() void {}
    };
};

pub const EncryptionTransit = struct {
    pub fn init() EncryptionTransit {}
};

// NOTE: try X variants

var seed_once = std.once(seed_init);
var seed_state: aegis_auth.Aegis256Mac = undefined;

comptime {
    // As described above, TigerBeetle uses Aegis (and thus AES Blocks), for its checksumming.
    // While there is a software implementation, it's much slower and we don't expect to ever be
    // using it considering we target platforms with AES hardware acceleration.
    //
    // If you're trying to compile TigerBeetle for an older CPU without AES hardware acceleration,
    // you'll need to disable the following assert.
    assert(std.crypto.core.aes.has_hardware_support);
}

fn seed_init() void {
    const key: [32]u8 = @splat(0);
    seed_state = aegis_auth.Aegis256Mac.init(&key);
}

pub const ChecksumStream = struct {
    state: aegis_auth.Aegis256Mac,

    pub fn init() ChecksumStream {
        seed_once.call();
        return ChecksumStream{ .state = seed_state };
    }

    pub fn add(stream: *ChecksumStream, bytes: []const u8) void {
        stream.state.update(bytes);
    }

    pub fn checksum(stream: *ChecksumStream) u256 {
        var result: u256 = undefined;
        stream.state.final(std.mem.asBytes(&result));
        stream.* = undefined;
        return result;
    }
};

fn checksum(bytes: []const u8) u256 {
    var stream: ChecksumStream = .init();
    stream.add(bytes);
    return stream.checksum();
}

pub const EncryptionStorage = struct {
    storage_key: [32]u8,

    pub fn init(storage_key: [32]u8) EncryptionStorage {
        return .{
            .storage_key = storage_key,
        };
    }

    pub fn encrypt_header(header: *Header, key: [32]u8, nonce: [16]u8) void {
        assert(!stdx.zeroed(&key));
        assert(!stdx.zeroed(&nonce));

        var original = header.*;
        const bytes_cleartext = std.mem.asBytes(&original);
        // TODO: move into header
        const ad = original.slice_associated_data();
        const nonce_checksum = checksum(&nonce);

        const bytes_ciphertext = header.slice_encrypted();
        const tag = std.mem.asBytes(&header.header_tag);

        aegis.Aegis256.encrypt(
            bytes_ciphertext,
            tag,
            bytes_cleartext,
            ad,
            std.mem.asBytes(&nonce_checksum)[0..32].*,
            key,
        );
    }
};

test "EncryptStorage encrypt_header" {
    var prng = stdx.PRNG.from_seed_testing();
    var test_key: [32]u8 = undefined;
    var test_nonce: [16]u8 = undefined;
    prng.fill(&test_key);
    prng.fill(&test_nonce);

    var prepare = Header.Prepare.root(0);
    EncryptionStorage.encrypt_header(prepare.frame(), test_key, test_nonce);
}
