const std = @import("std");
const builtin = @import("builtin");
const stdx = @import("stdx");

const assert = std.debug.assert;
const Header = @import("vsr/message_header.zig").Header;
const aegis = std.crypto.aead.aegis;
const aegis_auth = std.crypto.auth.aegis;
const hkdf = std.crypto.kdf.hkdf;

pub const encryption_version: u8 = 1;

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

const Payload = enum(u8) { header = 1, body = 2 };
const PeerType = enum(u8) { replica = 1, client = 2 };

const Peer = extern struct {
    peer: PeerType,
    padding: [15]u8 = @splat(0),

    // The ID of the peer needs to be communicated during the handshake.
    id: u128,

    comptime {
        assert(stdx.no_padding(Peer));
        assert(@sizeOf(Peer) == 32);
    }

    pub fn client(id: u128) Peer {
        return .{ .peer = .client, .id = id };
    }

    pub fn replica(id: u128) Peer {
        return .{ .peer = .replica, .id = id };
    }

    pub fn less_than(self: Peer, other: Peer) bool {
        const self_int = std.mem.bytesAsValue(u256, std.mem.asBytes(&self)).*;
        const other_int = std.mem.bytesAsValue(u256, std.mem.asBytes(&other)).*;
        return self_int < other_int;
    }
};

const Intent = extern struct {
    from: Peer,
    to: Peer,

    payload: Payload,
    identifier: [6]u8 = "intent".*,
    padding: [9]u8 = @splat(0),

    // Padding and asserts around it omitted for clarity.
};

const KeyId = extern struct {
    /// The version is also bound into the ephemeral_secret from the key exchange.
    version: u8,
    identifier: [5]u8 = "keyid".*,
    padding: [10]u8 = @splat(0),

    // FIXME
    /// This is still undefined, as it will relate to the key exchange protocol, but the core idea
    /// is to ensure we tie some part of the authority of the key that allowed the key exchange
    /// (eg, the signed certificate and the CA) into the key_id. Otherwise, imagine a situation
    /// where CAs were rotated, but existing in-memory keys stayed valid!
    origin: u128 = 0,

    peer_1: Peer,
    peer_2: Peer,

    pub fn init(version: u8, peer_self: Peer, peer_other: Peer) KeyId {
        // TOOD: add tests for stability of this.
        const peer_1, const peer_2 = if (peer_self.less_than(peer_other))
            .{ peer_self, peer_other }
        else
            .{ peer_other, peer_self };

        assert(peer_1.less_than(peer_2));

        return .{
            .version = version,

            .peer_1 = peer_1,
            .peer_2 = peer_2,
        };
    }

    pub fn id(self: *const KeyId, ephemeral_secret: [32]u8) u128 {
        // HKDF is truncated
        const result = hkdf.HkdfSha256.extract(std.mem.asBytes(self), &ephemeral_secret);
        return std.mem.bytesAsValue(u128, result[0..16]).*;
    }
};

comptime {
    // Ensure that derived secrets for Intent and KeyId can never be the same.
    const peer1 = Peer.replica(1);
    const peer2 = Peer.replica(2);

    const intent: Intent = .{
        .from = peer1,
        .to = peer2,
        .payload = .body,
    };

    const key_id = KeyId.init(encryption_version, peer1, peer2);

    assert(!std.mem.eql(u8, &key_id.identifier, &intent.identifier));
    assert(@sizeOf(KeyId) != @sizeOf(Intent));
}

const EncryptionTransit = struct {
    key_id: u128,

    key_send_header: [32]u8,
    // send_header_counter: NonceCounter = .{},
    key_send_body: [32]u8,

    key_recv_header: [32]u8,
    // recv_header_window: NonceWindow = .{},
    key_recv_body: [32]u8,

    pub fn init(ephemeral_secret: [32]u8, peer_self: Peer, peer_other: Peer) EncryptionTransit {
        const key_id = KeyId.init(encryption_version, peer_self, peer_other);

        const intent_send_header = Intent{
            .from = peer_self,
            .to = peer_other,
            .payload = .header,
        };

        const intent_send_body = Intent{
            .from = peer_self,
            .to = peer_other,
            .payload = .body,
        };

        const intent_recv_header = Intent{
            .from = peer_other,
            .to = peer_self,
            .payload = .header,
        };

        const intent_recv_body = Intent{
            .from = peer_other,
            .to = peer_self,
            .payload = .body,
        };

        return .{
            .key_id = key_id.id(ephemeral_secret),

            .key_send_header = hkdf.HkdfSha256.extract(
                std.mem.asBytes(&intent_send_header),
                &ephemeral_secret,
            ),
            .key_send_body = hkdf.HkdfSha256.extract(
                std.mem.asBytes(&intent_send_body),
                &ephemeral_secret,
            ),

            .key_recv_header = hkdf.HkdfSha256.extract(
                std.mem.asBytes(&intent_recv_header),
                &ephemeral_secret,
            ),
            .key_recv_body = hkdf.HkdfSha256.extract(
                std.mem.asBytes(&intent_recv_body),
                &ephemeral_secret,
            ),
        };
    }

    pub fn deinit(enc: *EncryptionTransit) void {
        std.crypto.utils.secureZero(u8, std.mem.asBytes(enc));
        enc.* = undefined;
    }

    pub fn encrypt_header(
        enc: *EncryptionTransit,
        header: *Header,
    ) void {
        const key = enc.key_send_header;
        const nonce = std.crypto.random.int(u128);

        assert(header.body_tag != 0);
        assert(!stdx.zeroed(&key));
        assert(nonce != 0);

        var original = header.*;
        const bytes_cleartext = original.slice_encrypted();

        const bytes_ciphertext = header.slice_encrypted();
        const tag = std.mem.asBytes(&header.header_tag);
        header.header_nonce = nonce;
        const ad = header.slice_associated_data();

        aegis.Aegis256.encrypt(
            bytes_ciphertext,
            tag,
            bytes_cleartext,
            ad,
            extend_nonce(nonce),
            key,
        );
    }

    pub fn decrypt_header(
        enc: *EncryptionTransit,
        header: *Header,
    ) !void {
        const key = enc.key_recv_header;
        assert(!stdx.zeroed(&key));

        var original = header.*;
        const bytes_ciphertext = original.slice_encrypted();
        const tag = std.mem.asBytes(&original.header_tag);
        const ad = original.slice_associated_data();

        const bytes_cleartext = header.slice_encrypted();

        aegis.Aegis256.decrypt(
            bytes_cleartext,
            bytes_ciphertext,
            tag.*,
            ad,
            extend_nonce(original.header_nonce),
            key,
        ) catch |err| {
            header.* = original;
            return err;
        };
    }

    pub fn encrypt_body(
        enc: *EncryptionTransit,
        header: *Header,
        target: []u8,
        source: []const u8,
    ) []const u8 {
        const key = enc.key_send_body;
        const nonce = std.crypto.random.int(u128);

        assert(!stdx.zeroed(&key));
        assert(nonce != 0);

        const tag = std.mem.asBytes(&header.body_tag);
        header.body_nonce = nonce;

        aegis.Aegis256.encrypt(
            target,
            tag,
            source,
            &[0]u8{},
            extend_nonce(nonce),
            key,
        );
        return target;
    }

    pub fn decrypt_body(
        enc: *EncryptionTransit,
        header: *Header,
        target: []u8,
        source: []const u8,
    ) ![]const u8 {
        const key = enc.key_recv_body;

        assert(!stdx.zeroed(&key));
        assert(header.body_nonce != 0);

        const tag = std.mem.asBytes(&header.body_tag).*;

        try aegis.Aegis256.decrypt(
            target,
            source,
            tag,
            &[0]u8{},
            extend_nonce(header.body_nonce),
            key,
        );
        return target;
    }
};

fn extend_nonce(short_nonce: u128) [32]u8 {
    return std.mem.asBytes(&short_nonce)[0..16].* ++ @as([16]u8, @splat(0));
}

pub const EncryptionStorage = struct {
    pub fn encrypt_header(
        header: *Header,
        key: [32]u8,
        nonce: u128,
    ) void {
        assert(header.body_tag != 0);
        assert(!stdx.zeroed(&key));
        assert(nonce != 0);

        var original = header.*;
        const bytes_cleartext = original.slice_encrypted();

        const bytes_ciphertext = header.slice_encrypted();
        const tag = std.mem.asBytes(&header.header_tag);
        header.header_nonce = nonce;
        const ad = header.slice_associated_data();

        aegis.Aegis256.encrypt(
            bytes_ciphertext,
            tag,
            bytes_cleartext,
            ad,
            extend_nonce(nonce),
            key,
        );
    }

    pub fn calculate_checksum_header(header: *Header, key: [32]u8, nonce: u128) u128 {
        var mac: [16]u8 = undefined;
        aegis_auth.Aegis256Mac.createWithNonce(
            &mac,
            header.slice_without_header_tag(),
            &key,
            &nonce,
        );
        return std.mem.bytesAsValue(u128, &mac).*;
    }

    pub fn set_checksum_header(
        header: *Header,
        key: [32]u8,
        nonce: u128,
    ) void {
        header.header_tag = calculate_checksum_header(header, key, nonce);
    }
    pub fn calculate_checksum_body(
        body: []const u8,
        key: [32]u8,
        nonce: u128,
    ) u128 {
        var mac: [16]u8 = undefined;
        aegis_auth.Aegis256Mac.createWithNonce(
            &mac,
            body,
            &key,
            &nonce,
        );
        return std.mem.bytesAsValue(u128, &mac).*;
    }

    pub fn set_checksum_body(
        header: *Header,
        body: []const u8,
        key: [32]u8,
        nonce: u128,
    ) void {
        header.body_tag = calculate_checksum_body(body, key, nonce);
    }

    pub fn decrypt_header(
        header: *Header,
        key: [32]u8,
    ) !void {
        assert(!stdx.zeroed(&key));

        var original = header.*;
        const bytes_ciphertext = original.slice_encrypted();
        const tag = std.mem.asBytes(&original.header_tag);
        const ad = original.slice_associated_data();

        const bytes_cleartext = header.slice_encrypted();

        aegis.Aegis256.decrypt(
            bytes_cleartext,
            bytes_ciphertext,
            tag.*,
            ad,
            extend_nonce(original.header_nonce),
            key,
        ) catch |err| {
            header.* = original;
            return err;
        };
    }

    pub fn encrypt_body(
        header: *Header,
        target: []u8,
        source: []const u8,
        key: [32]u8,
        nonce: u128,
    ) []const u8 {
        assert(target.len == source.len);
        assert(header.size == @sizeOf(Header) + source.len);
        assert(!stdx.zeroed(&key));
        assert(nonce != 0);

        header.body_nonce = nonce;

        aegis.Aegis256.encrypt(
            target,
            std.mem.asBytes(&header.body_tag),
            source,
            &[0]u8{},
            extend_nonce(nonce),
            key,
        );
        return target;
    }

    pub fn decrypt_body(
        header: *Header,
        target: []u8,
        source: []const u8,
        key: [32]u8,
    ) ![]const u8 {
        assert(target.len == source.len);
        assert(header.size == @sizeOf(Header) + source.len);
        assert(!stdx.zeroed(&key));

        try aegis.Aegis256.decrypt(
            target,
            source,
            std.mem.asBytes(&header.body_tag).*,
            &[0]u8{},
            extend_nonce(header.body_nonce),
            key,
        );
        return target;
    }
};

test "EncryptStorage" {
    var prng = stdx.PRNG.from_seed_testing();
    const body_test_key: [32]u8 = blk: {
        var body_test_key: [32]u8 = undefined;
        prng.fill(&body_test_key);
        break :blk body_test_key;
    };
    const body_test_nonce: u128 = prng.int(u128);

    const header_test_nonce: u128 = prng.int(u128);
    const header_test_key: [32]u8 = blk: {
        var header_test_key: [32]u8 = undefined;
        prng.fill(&header_test_key);
        break :blk header_test_key;
    };

    var body: [1024]u8 = undefined;
    prng.fill(&body);

    var encrypt_buffer: [1024]u8 = undefined;

    var prepare = Header.Prepare.root(0);
    prepare.size = @intCast(@sizeOf(Header) + body.len);

    const body_encrypted = EncryptionStorage.encrypt_body(
        prepare.frame(),
        &encrypt_buffer,
        &body,
        body_test_key,
        body_test_nonce,
    );

    var unencrypted = prepare.frame().*;

    EncryptionStorage.encrypt_header(prepare.frame(), header_test_key, header_test_nonce);
    try EncryptionStorage.decrypt_header(prepare.frame(), header_test_key);

    try std.testing.expectEqualSlices(
        u8,
        unencrypted.slice_encrypted(),
        prepare.frame().slice_encrypted(),
    );

    var decrypt_buffer: [1024]u8 = undefined;
    const body_decrypted = try EncryptionStorage.decrypt_body(
        prepare.frame(),
        &decrypt_buffer,
        body_encrypted,
        body_test_key,
    );

    try std.testing.expectEqualSlices(u8, &body, body_decrypted);
}

test "EncryptTransit" {
    var prng = stdx.PRNG.from_seed_testing();
    const ephemeral_secret: [32]u8 = blk: {
        var ephemeral_secret: [32]u8 = undefined;
        prng.fill(&ephemeral_secret);
        break :blk ephemeral_secret;
    };

    const peer_a = Peer.replica(1);
    const peer_b = Peer.replica(2);
    var enc_a = EncryptionTransit.init(ephemeral_secret, peer_a, peer_b);
    defer enc_a.deinit();

    var enc_b = EncryptionTransit.init(ephemeral_secret, peer_b, peer_a);
    defer enc_b.deinit();

    try std.testing.expectEqual(enc_a.key_id, enc_b.key_id);
    try std.testing.expectEqual(enc_a.key_send_header, enc_b.key_recv_header);
    try std.testing.expectEqual(enc_a.key_send_body, enc_b.key_recv_body);
    try std.testing.expectEqual(enc_a.key_recv_header, enc_b.key_send_header);
    try std.testing.expectEqual(enc_a.key_recv_body, enc_b.key_send_body);

    try std.testing.expect(!std.mem.eql(u8, &enc_a.key_send_header, &enc_a.key_send_body));
    try std.testing.expect(!std.mem.eql(u8, &enc_a.key_send_header, &enc_a.key_recv_header));
    try std.testing.expect(!std.mem.eql(u8, &enc_a.key_send_header, &enc_a.key_recv_body));

    try std.testing.expect(!std.mem.eql(u8, &enc_a.key_send_body, &enc_a.key_recv_header));
    try std.testing.expect(!std.mem.eql(u8, &enc_a.key_send_body, &enc_a.key_recv_body));

    try std.testing.expect(!std.mem.eql(u8, &enc_a.key_recv_header, &enc_a.key_recv_body));

    var body: [1024]u8 = undefined;
    var encrypt_buffer: [1024]u8 = undefined;
    var decrypt_buffer: [1024]u8 = undefined;
    prng.fill(&body);

    var prepare = Header.Prepare.root(0);
    prepare.size = @intCast(@sizeOf(Header) + body.len);

    const encrypted = enc_a.encrypt_body(prepare.frame(), &encrypt_buffer, &body);

    var unencrypted = prepare.frame().*;

    enc_a.encrypt_header(prepare.frame());

    try std.testing.expect(!stdx.equal_bytes(Header, &unencrypted, prepare.frame()));

    try std.testing.expectError(error.AuthenticationFailed, enc_a.decrypt_body(
        prepare.frame(),
        &decrypt_buffer,
        encrypted,
    ));

    try std.testing.expectError(error.AuthenticationFailed, enc_a.decrypt_header(prepare.frame()));

    try enc_b.decrypt_header(prepare.frame());

    const decrypted = try enc_b.decrypt_body(prepare.frame(), &decrypt_buffer, encrypted);

    try std.testing.expectEqualSlices(
        u8,
        &body,
        decrypted,
    );

    try std.testing.expectEqualSlices(
        u8,
        unencrypted.slice_encrypted(),
        prepare.frame().slice_encrypted(),
    );
}

test "EncryptStorage Bit Fuzzer" {
    var prng = stdx.PRNG.from_seed_testing();

    const header_test_nonce: u128 = prng.int(u128);
    const header_test_key: [32]u8 = blk: {
        var header_test_key: [32]u8 = undefined;
        prng.fill(&header_test_key);
        break :blk header_test_key;
    };

    var prepare = Header.Prepare.root(0);
    prepare.size = @intCast(@sizeOf(Header));

    for (0..@bitSizeOf(Header)) |bit| {
        var header = prepare.frame().*;

        EncryptionStorage.encrypt_header(&header, header_test_key, header_test_nonce);

        var header_int: u2048 = @bitCast(header);
        header_int ^= @as(u2048, 1) << @intCast(bit);
        header = @bitCast(header_int);

        try std.testing.expectError(error.AuthenticationFailed, EncryptionStorage.decrypt_header(
            prepare.frame(),
            header_test_key,
        ));
    }

    const body_test_key: [32]u8 = blk: {
        var body_test_key: [32]u8 = undefined;
        prng.fill(&body_test_key);
        break :blk body_test_key;
    };
    const body_test_nonce: u128 = prng.int(u128);

    var body: [1024]u8 = undefined;
    prng.fill(&body);

    var encrypt_buffer: [1024]u8 = undefined;
    var decrypt_buffer: [1024]u8 = undefined;

    assert(body.len == encrypt_buffer.len);
    assert(encrypt_buffer.len == decrypt_buffer.len);

    prepare.size = @intCast(@sizeOf(Header) + body.len);

    for (0..encrypt_buffer.len) |pos| {
        for (0..@bitSizeOf(u8)) |bit| {
            const body_encrypted = EncryptionStorage.encrypt_body(
                prepare.frame(),
                &encrypt_buffer,
                &body,
                body_test_key,
                body_test_nonce,
            );
            assert(body_encrypted.len == encrypt_buffer.len);
            encrypt_buffer[pos] = body_encrypted[pos] ^ @as(u8, 1) << @intCast(bit);

            try std.testing.expectError(error.AuthenticationFailed, EncryptionStorage.decrypt_body(
                prepare.frame(),
                &decrypt_buffer,
                body_encrypted,
                body_test_key,
            ));
        }
    }
}

test "EncryptTransit Bit Fuzzer" {
    var prng = stdx.PRNG.from_seed_testing();

    const ephemeral_secret: [32]u8 = blk: {
        var ephemeral_secret: [32]u8 = undefined;
        prng.fill(&ephemeral_secret);
        break :blk ephemeral_secret;
    };

    const peer_a = Peer.replica(1);
    const peer_b = Peer.replica(2);
    var enc_a = EncryptionTransit.init(ephemeral_secret, peer_a, peer_b);
    defer enc_a.deinit();

    var enc_b = EncryptionTransit.init(ephemeral_secret, peer_b, peer_a);
    defer enc_b.deinit();

    var prepare = Header.Prepare.root(0);
    prepare.size = @intCast(@sizeOf(Header));

    for (0..@bitSizeOf(Header)) |bit| {
        var header = prepare.frame().*;

        enc_a.encrypt_header(&header);

        var header_int: u2048 = @bitCast(header);
        header_int ^= @as(u2048, 1) << @intCast(bit);
        header = @bitCast(header_int);

        try std.testing.expectError(error.AuthenticationFailed, enc_b.decrypt_header(
            prepare.frame(),
        ));
    }

    var body: [1024]u8 = undefined;
    prng.fill(&body);

    var encrypt_buffer: [1024]u8 = undefined;
    var decrypt_buffer: [1024]u8 = undefined;

    assert(body.len == encrypt_buffer.len);
    assert(encrypt_buffer.len == decrypt_buffer.len);

    prepare.size = @intCast(@sizeOf(Header) + body.len);

    for (0..encrypt_buffer.len) |pos| {
        for (0..@bitSizeOf(u8)) |bit| {
            const body_encrypted = enc_a.encrypt_body(
                prepare.frame(),
                &encrypt_buffer,
                &body,
            );
            assert(body_encrypted.len == encrypt_buffer.len);
            encrypt_buffer[pos] = body_encrypted[pos] ^ @as(u8, 1) << @intCast(bit);

            try std.testing.expectError(error.AuthenticationFailed, enc_b.decrypt_body(
                prepare.frame(),
                &decrypt_buffer,
                body_encrypted,
            ));
        }
    }
}
