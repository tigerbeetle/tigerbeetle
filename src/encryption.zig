const std = @import("std");
const builtin = @import("builtin");
const stdx = @import("stdx");
const vsr = @import("vsr.zig");

const assert = std.debug.assert;
const Header = @import("vsr/message_header.zig").Header;
const MessagePool = vsr.message_pool.MessagePool;
const Message = MessagePool.Message;
const MessageNetwork = Message.Network;
const MessageStorage = Message.Storage;
const aegis = std.crypto.aead.aegis;
const aegis_auth = std.crypto.auth.aegis;
const hkdf = std.crypto.kdf.hkdf;

pub const encryption_version: u8 = 1;

var seed_once = std.once(seed_init);
// NOTE: try X variants
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

pub const Peer = extern struct {
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

    // TODO
    /// This is still undefined, as it will relate to the key exchange protocol, but the core idea
    /// is to ensure we tie some part of the authority of the key that allowed the key exchange
    /// (eg, the signed certificate and the CA) into the key_id. Otherwise, imagine a situation
    /// where CAs were rotated, but existing in-memory keys stayed valid!
    origin: u128 = 0,

    peer_1: Peer,
    peer_2: Peer,

    pub fn init(version: u8, peer_self: Peer, peer_other: Peer) KeyId {
        // TODO: add tests for stability of this.
        const peer_1, const peer_2 = if (peer_self.less_than(peer_other))
            .{ peer_self, peer_other }
        else
            .{ peer_other, peer_self };

        // TODO: properly setup encryption between clients and replicas.
        // Currently we use peer_1 = peer_2
        // assert(peer_1.less_than(peer_2));

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

const X25519 = std.crypto.dh.X25519;

pub const KeyExchangeInsecure = struct {
    state: State,
    self_id: u128,
    self_type: PeerType,
    key_pair: X25519.KeyPair,
    result: ?KeyExchangeResult = null,

    const State = union(Role) {
        initiator: DHState,
        responder: DHState,
    };

    const DHState = enum {
        recv_dh,
        send_dh,
        derive_secret,
    };

    const Role = enum {
        initiator,
        responder,
    };

    const SharedSecret = [32]u8;

    pub const KeyExchangeResult = struct {
        shared_secret: SharedSecret,
        peer_id: u128,
        peer_type: PeerType,
    };

    pub const KeyExchangeMessage = extern struct {
        peer_type: PeerType,
        peer_id: u128,
        public_key: [32]u8,
    };

    const Action = union(enum) {
        send: KeyExchangeMessage,
        recv,
        done: KeyExchangeResult,
        terminate,
    };

    pub fn init(role: Role, self_id: u128, self_type: PeerType) KeyExchangeInsecure {
        return .{
            .state = switch (role) {
                .responder => .{ .responder = .recv_dh },
                .initiator => .{ .initiator = .send_dh },
            },
            .self_id = self_id,
            .self_type = self_type,
            .key_pair = X25519.KeyPair.generate(),
        };
    }

    pub fn feed(
        kex: *KeyExchangeInsecure,
        maybe_msg: ?KeyExchangeMessage,
    ) Action {
        switch (kex.state) {
            .initiator => |state| {
                switch (state) {
                    .send_dh => {
                        if (maybe_msg != null) return .terminate;
                        kex.state.initiator = .recv_dh;
                        return .{ .send = .{
                            .public_key = kex.key_pair.public_key,
                            .peer_id = kex.self_id,
                            .peer_type = kex.self_type,
                        } };
                    },
                    .recv_dh => {
                        if (maybe_msg) |msg| {
                            const shared_secret = X25519.scalarmult(
                                kex.key_pair.secret_key,
                                msg.public_key,
                            ) catch {
                                return .terminate;
                            };

                            kex.result = .{
                                .shared_secret = shared_secret,
                                .peer_type = msg.peer_type,
                                .peer_id = msg.peer_id,
                            };
                            return .{ .done = kex.result.? };
                        } else {
                            return .{ .send = .{
                                .public_key = kex.key_pair.public_key,
                                .peer_id = kex.self_id,
                                .peer_type = kex.self_type,
                            } };
                        }
                    },
                    .derive_secret => {
                        if (maybe_msg != null) return .terminate;
                        return .{ .done = kex.result.? };
                    },
                }
            },
            .responder => |state| {
                switch (state) {
                    .recv_dh => {
                        if (maybe_msg) |msg| {
                            const shared_secret = X25519.scalarmult(
                                kex.key_pair.secret_key,
                                msg.public_key,
                            ) catch {
                                return .terminate;
                            };

                            kex.result = .{
                                .shared_secret = shared_secret,
                                .peer_type = msg.peer_type,
                                .peer_id = msg.peer_id,
                            };
                            kex.state.responder = .send_dh;
                            return .{ .send = .{
                                .public_key = kex.key_pair.public_key,
                                .peer_id = kex.self_id,
                                .peer_type = kex.self_type,
                            } };
                        } else {
                            return .recv;
                        }
                    },
                    .send_dh => {
                        if (maybe_msg != null) return .terminate;
                        kex.state.responder = .derive_secret;
                        return .{ .done = kex.result.? };
                    },
                    .derive_secret => {
                        if (maybe_msg != null) return .terminate;
                        return .{ .done = kex.result.? };
                    },
                }
            },
        }
    }
};

test "KeyExchangeInsecure" {
    var initiator_kex: KeyExchangeInsecure = .init(.initiator, 1, .replica);
    var responder_kex: KeyExchangeInsecure = .init(.responder, 2, .replica);

    var initiator_result: ?KeyExchangeInsecure.KeyExchangeResult = null;
    var responder_result: ?KeyExchangeInsecure.KeyExchangeResult = null;

    var maybe_initiator_action: ?KeyExchangeInsecure.Action = null;
    var maybe_responder_action: ?KeyExchangeInsecure.Action = null;
    while (initiator_result == null or responder_result == null) {
        const initiator_action = maybe_initiator_action orelse initiator_kex.feed(null);
        maybe_initiator_action = null;
        switch (initiator_action) {
            .done => |shared_secret| {
                initiator_result = shared_secret;
            },
            .send => |msg| {
                maybe_responder_action = responder_kex.feed(msg);
            },
            else => unreachable,
        }

        const responder_action = maybe_responder_action orelse responder_kex.feed(null);
        maybe_responder_action = null;
        switch (responder_action) {
            .done => |shared_secret| {
                responder_result = shared_secret;
            },
            .send => |msg| {
                maybe_initiator_action = initiator_kex.feed(msg);
            },
            else => unreachable,
        }
    }

    try std.testing.expectEqualSlices(
        u8,
        &initiator_result.?.shared_secret,
        &responder_result.?.shared_secret,
    );

    try std.testing.expectEqual(
        initiator_result.?.peer_type,
        responder_kex.self_type,
    );

    try std.testing.expectEqual(
        initiator_result.?.peer_id,
        responder_kex.self_id,
    );

    try std.testing.expectEqual(
        responder_result.?.peer_type,
        initiator_kex.self_type,
    );

    try std.testing.expectEqual(
        responder_result.?.peer_id,
        initiator_kex.self_id,
    );
}

pub const EncryptionTransit = struct {
    const BodyTagNonce = struct { body_nonce: u128, body_tag: u128 };

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

    pub fn encrypt_message(
        enc: *EncryptionTransit,
        target: *Message,
        source: *const Message,
    ) *MessageNetwork {
        const body_tag_nonce = enc.encrypt_body(
            target.buffer[@sizeOf(Header)..source.header.size],
            source.body_used(),
        );

        var header = source.header.*;
        header.body_tag = body_tag_nonce.body_tag;
        header.body_nonce = body_tag_nonce.body_nonce;

        // TODO: When sending a message, assert the last 16 bytes are not zero.
        @memset(target.buffer[source.header.size..], 0);
        target.header.* = enc.encrypt_header(&header);
        target.header = undefined;
        target.metadata = .{
            .size_value = header.size,
            .command_value = header.command,
        };
        return @ptrCast(target);
    }

    pub fn decrypt_message(
        enc: *EncryptionTransit,
        target: *Message,
        source: *const MessageNetwork,
    ) !void {
        const header_encrypted = source.get_header_encrypted();
        if (header_encrypted.header_key_id != enc.key_id) {
            return error.AuthenticationFailed;
        }

        // We are decrypting the header, despite already having decrypted it in message_buffer.zig
        const header_decrypted = try enc.decrypt_header(header_encrypted);

        target.header.* = header_decrypted;

        try enc.decrypt_body(
            .{ .body_tag = header_decrypted.body_tag, .body_nonce = header_decrypted.body_nonce },
            target.body_used(),
            source.body_used(),
        );

        target.header.set_checksum_body(target.body_used());
        target.header.set_zeroes();
        target.header.set_checksum();
    }

    pub fn encrypt_header(
        enc: *EncryptionTransit,
        header: *const Header,
    ) Header {
        const key = enc.key_send_header;
        const nonce = std.crypto.random.int(u128);

        assert(header.body_tag != 0);
        assert(!stdx.zeroed(&key));
        assert(nonce != 0);
        assert(!std.mem.eql(u8, &key, &@as([32]u8, @splat(undefined_u8))));
        assert(nonce != undefined_u128);

        var encrypted = header.*;
        const bytes_cleartext = header.slice_encrypted_const();

        const bytes_ciphertext = encrypted.slice_encrypted();
        const tag = std.mem.asBytes(&encrypted.header_tag);
        encrypted.header_nonce = nonce;
        encrypted.header_key_id = enc.key_id;
        const ad = encrypted.slice_associated_data();

        aegis.Aegis256.encrypt(
            bytes_ciphertext,
            tag,
            bytes_cleartext,
            ad,
            extend_nonce(nonce),
            key,
        );

        return encrypted;
    }

    pub fn decrypt_header(
        enc: *EncryptionTransit,
        header: *const Header,
    ) !Header {
        const key = enc.key_recv_header;
        assert(!stdx.zeroed(&key));
        assert(!std.mem.eql(u8, &key, &@as([32]u8, @splat(undefined_u8))));

        if (header.header_key_id != enc.key_id) {
            return error.AuthenticationFailed;
        }

        if (header.header_nonce == 0 or header.header_nonce == undefined_u128) {
            return error.InvalidHeaderNonce;
        }

        var decrypted = header.*;
        const bytes_ciphertext = header.slice_encrypted_const();
        const tag = std.mem.asBytes(&header.header_tag);
        const ad = header.slice_associated_data_const();

        const bytes_cleartext = decrypted.slice_encrypted();

        try aegis.Aegis256.decrypt(
            bytes_cleartext,
            bytes_ciphertext,
            tag.*,
            ad,
            extend_nonce(decrypted.header_nonce),
            key,
        );

        // Check that command is valid.
        const command_raw = @intFromEnum(decrypted.command);
        _ = std.meta.intToEnum(vsr.Command, command_raw) catch {
            // TODO: revisit this and do not crash
            vsr.fatal(
                .unknown_vsr_command,
                "unknown VSR command, crashing for safety " ++
                    "(command={d} protocol={d} replica={d} release={})",
                .{
                    command_raw,
                    decrypted.protocol,
                    decrypted.replica,
                    decrypted.release,
                },
            );
        };

        return decrypted;
    }

    fn encrypt_body(
        enc: *EncryptionTransit,
        target: []u8,
        source: []const u8,
    ) BodyTagNonce {
        const key = enc.key_send_body;
        const nonce = std.crypto.random.int(u128);

        assert(target.len == source.len);
        assert(!stdx.zeroed(&key));
        assert(nonce != 0);
        assert(!std.mem.eql(u8, &key, &@as([32]u8, @splat(undefined_u8))));
        assert(nonce != undefined_u128);

        var body_tag: u128 = 0;
        const tag = std.mem.asBytes(&body_tag);

        aegis.Aegis256.encrypt(
            target,
            tag,
            source,
            &[0]u8{},
            extend_nonce(nonce),
            key,
        );
        return .{ .body_nonce = nonce, .body_tag = body_tag };
    }

    fn decrypt_body(
        enc: *EncryptionTransit,
        body_tag_nonce: BodyTagNonce,
        target: []u8,
        source: []const u8,
    ) !void {
        const key = enc.key_recv_body;

        assert(target.len == source.len);
        assert(!stdx.zeroed(&key));
        assert(!std.mem.eql(u8, &key, &@as([32]u8, @splat(undefined_u8))));

        if (body_tag_nonce.body_nonce == 0 or body_tag_nonce.body_nonce == undefined_u128) {
            return error.InvalidBodyNonce;
        }

        const tag = std.mem.asBytes(&body_tag_nonce.body_tag).*;

        try aegis.Aegis256.decrypt(
            target,
            source,
            tag,
            &[0]u8{},
            extend_nonce(body_tag_nonce.body_nonce),
            key,
        );
    }
};

fn extend_nonce(short_nonce: u128) [32]u8 {
    return std.mem.asBytes(&short_nonce)[0..16].* ++ @as([16]u8, @splat(0));
}

const undefined_u128: u128 = 0xaaaaaaaaaaaaaaaa;
const undefined_u8: u8 = 0xaa;

// NOTE: double check value for undefined
// comptime {
// const value: u128 = undefined;
// assert(undefined_u128 == value);
// }

pub const EncryptionStorage = struct {
    pub const Keys = struct {
        header_key: [32]u8,
        header_nonce: u128,
        body_key: [32]u8,
        body_nonce: u128,

        pub fn generate() Keys {
            var keys: Keys = undefined;
            std.crypto.random.bytes(std.mem.asBytes(&keys));
            return keys;
        }

        pub fn generate_deterministic(prng: *stdx.PRNG) Keys {
            assert(builtin.is_test);
            var keys: Keys = undefined;
            prng.fill(std.mem.asBytes(&keys));
            return keys;
        }
    };

    pub fn encrypt_message(
        target: *Message,
        source: *const Message,
        keys: Keys,
    ) *MessageStorage {
        encrypt_body(
            source.header,
            target.buffer[@sizeOf(Header)..source.header.size],
            source.body_used(),
            keys.header_key,
            keys.header_nonce,
        );
        // TODO: When storing a message, assert the last 16 bytes are not zero.
        @memset(target.buffer[source.header.size..], 0);
        target.header.* = encrypt_header(
            source.header,
            keys.body_key,
            keys.body_nonce,
        );
        return @ptrCast(target);
    }

    pub fn decrypt_message(
        target: *Message,
        source: *const MessageStorage,
        keys: Keys,
    ) void {
        target.header.* = decrypt_header(
            source.header,
            keys.header_key,
            keys.header_nonce,
        );
        decrypt_body(
            target.header,
            target.body_used(),
            source.buffer[@sizeOf(Header)..target.header.size],
            keys.body_key,
            keys.body_nonce,
        );
    }

    pub fn calculate_checksum_header(header: *Header, keys: Keys) u128 {
        var mac: [16]u8 = undefined;
        aegis_auth.Aegis256Mac_128.createWithNonce(
            &mac,
            header.slice_without_header_tag(),
            &keys.header_key,
            &extend_nonce(keys.header_nonce),
        );
        return std.mem.bytesAsValue(u128, &mac).*;
    }

    pub fn set_checksum_header(
        header: *Header,
        keys: Keys,
    ) void {
        header.header_tag = calculate_checksum_header(header, keys);
    }

    pub fn calculate_checksum_body(
        body: []const u8,
        keys: Keys,
    ) u128 {
        var mac: [16]u8 = undefined;
        aegis_auth.Aegis256Mac_128.createWithNonce(
            &mac,
            body,
            &keys.body_key,
            &extend_nonce(keys.body_nonce),
        );
        return std.mem.bytesAsValue(u128, &mac).*;
    }

    pub fn set_checksum_body(
        header: *Header,
        body: []const u8,
        keys: Keys,
    ) void {
        header.body_tag = calculate_checksum_body(body, keys);
    }

    pub fn set_checksum_message(
        message: *Message,
        keys: Keys,
    ) void {
        set_checksum_body(message.header, message.body_used(), keys);
        set_checksum_header(message.header, keys);
    }

    pub fn encrypt_header(
        header: *const Header,
        key: [32]u8,
        nonce: u128,
    ) Header {
        assert(header.body_tag != 0);
        assert(!stdx.zeroed(&key));
        assert(nonce != 0);
        assert(!std.mem.eql(u8, &key, &@as([32]u8, @splat(undefined_u8))));
        assert(nonce != undefined_u128);

        var encrypted = header.*;
        const bytes_cleartext = header.slice_encrypted_const();

        const bytes_ciphertext = encrypted.slice_encrypted();
        const tag = std.mem.asBytes(&encrypted.header_tag);
        encrypted.header_nonce = nonce;
        const ad = encrypted.slice_associated_data();

        aegis.Aegis256.encrypt(
            bytes_ciphertext,
            tag,
            bytes_cleartext,
            ad,
            extend_nonce(nonce),
            key,
        );
        return encrypted;
    }

    pub fn decrypt_header(
        header: *const Header,
        key: [32]u8,
    ) !Header {
        assert(!stdx.zeroed(&key));
        assert(!std.mem.eql(u8, &key, &@as([32]u8, @splat(undefined_u8))));

        if (header.header_nonce == 0 or header.header_nonce == undefined_u128) {
            return error.InvalidHeaderNonce;
        }

        var decrypted = header.*;
        const bytes_ciphertext = header.slice_encrypted_const();
        const tag = std.mem.asBytes(&header.header_tag).*;
        const ad = header.slice_associated_data_const();

        const bytes_cleartext = decrypted.slice_encrypted();

        try aegis.Aegis256.decrypt(
            bytes_cleartext,
            bytes_ciphertext,
            tag,
            ad,
            extend_nonce(decrypted.header_nonce),
            key,
        );
        return decrypted;
    }

    pub fn encrypt_body(
        header: *Header,
        target: []u8,
        source: []const u8,
        key: [32]u8,
        nonce: u128,
    ) void {
        assert(target.len == source.len);
        assert(header.size == @sizeOf(Header) + source.len);
        assert(!stdx.zeroed(&key));
        assert(nonce != 0);
        assert(!std.mem.eql(u8, &key, &@as([32]u8, @splat(undefined_u8))));
        assert(nonce != undefined_u128);

        header.body_nonce = nonce;

        aegis.Aegis256.encrypt(
            target,
            std.mem.asBytes(&header.body_tag),
            source,
            &[0]u8{},
            extend_nonce(nonce),
            key,
        );
    }

    pub fn decrypt_body(
        header: *const Header,
        target: []u8,
        source: []const u8,
        key: [32]u8,
    ) !void {
        assert(target.len == source.len);
        assert(header.size == @sizeOf(Header) + source.len);
        assert(!stdx.zeroed(&key));
        assert(!std.mem.eql(u8, &key, &@as([32]u8, @splat(undefined_u8))));

        if (header.body_nonce == 0 or header.body_nonce == undefined_u128) {
            return error.InvalidBodyNonce;
        }

        try aegis.Aegis256.decrypt(
            target,
            source,
            std.mem.asBytes(&header.body_tag).*,
            &[0]u8{},
            extend_nonce(header.body_nonce),
            key,
        );
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

    EncryptionStorage.encrypt_body(
        prepare.frame(),
        &encrypt_buffer,
        &body,
        body_test_key,
        body_test_nonce,
    );

    const encrypted = EncryptionStorage.encrypt_header(
        prepare.frame(),
        header_test_key,
        header_test_nonce,
    );
    const unencrypted = try EncryptionStorage.decrypt_header(&encrypted, header_test_key);

    try std.testing.expectEqualSlices(
        u8,
        prepare.frame().slice_encrypted_const(),
        unencrypted.slice_encrypted_const(),
    );

    var decrypt_buffer: [1024]u8 = undefined;
    try EncryptionStorage.decrypt_body(
        &unencrypted,
        &decrypt_buffer,
        &encrypt_buffer,
        body_test_key,
    );

    try std.testing.expectEqualSlices(u8, &body, &decrypt_buffer);
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
    prepare.frame().set_zeroes();

    const body_tag_nonce = enc_a.encrypt_body(&encrypt_buffer, &body);
    prepare.frame().body_tag = body_tag_nonce.body_tag;
    prepare.frame().body_nonce = body_tag_nonce.body_nonce;

    const header_unencrypted = prepare.frame().*;

    const header_encrypted = enc_a.encrypt_header(&header_unencrypted);

    try std.testing.expect(!stdx.equal_bytes(Header, &header_unencrypted, &header_encrypted));

    try std.testing.expectError(
        error.AuthenticationFailed,
        enc_a.decrypt_header(&header_encrypted),
    );

    var header_decrypted = try enc_b.decrypt_header(&header_encrypted);

    try std.testing.expectEqualSlices(
        u8,
        header_unencrypted.slice_encrypted_const(),
        header_decrypted.slice_encrypted_const(),
    );

    try std.testing.expectError(error.AuthenticationFailed, enc_a.decrypt_body(
        body_tag_nonce,
        &decrypt_buffer,
        &encrypt_buffer,
    ));

    try enc_b.decrypt_body(
        body_tag_nonce,
        &decrypt_buffer,
        &encrypt_buffer,
    );

    try std.testing.expectEqualSlices(
        u8,
        &body,
        &decrypt_buffer,
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
        const header_unencrypted = prepare.frame().*;

        var header_encrypted = EncryptionStorage.encrypt_header(
            &header_unencrypted,
            header_test_key,
            header_test_nonce,
        );

        var header_int: u2048 = @bitCast(header_encrypted);
        header_int ^= @as(u2048, 1) << @intCast(bit);
        header_encrypted = @bitCast(header_int);

        try std.testing.expectError(error.AuthenticationFailed, EncryptionStorage.decrypt_header(
            &header_encrypted,
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
            EncryptionStorage.encrypt_body(
                prepare.frame(),
                &encrypt_buffer,
                &body,
                body_test_key,
                body_test_nonce,
            );
            encrypt_buffer[pos] = encrypt_buffer[pos] ^ @as(u8, 1) << @intCast(bit);

            try std.testing.expectError(error.AuthenticationFailed, EncryptionStorage.decrypt_body(
                prepare.frame(),
                &decrypt_buffer,
                &encrypt_buffer,
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

        var header_encrypted = enc_a.encrypt_header(&header);

        var header_int: u2048 = @bitCast(header_encrypted);
        header_int ^= @as(u2048, 1) << @intCast(bit);
        header_encrypted = @bitCast(header_int);

        try std.testing.expectError(error.AuthenticationFailed, enc_b.decrypt_header(
            &header_encrypted,
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
            const body_tag_nonce = enc_a.encrypt_body(
                &encrypt_buffer,
                &body,
            );
            encrypt_buffer[pos] = encrypt_buffer[pos] ^ @as(u8, 1) << @intCast(bit);

            try std.testing.expectError(error.AuthenticationFailed, enc_b.decrypt_body(
                body_tag_nonce,
                &decrypt_buffer,
                &encrypt_buffer,
            ));
        }
    }
}
