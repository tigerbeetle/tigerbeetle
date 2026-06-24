const std = @import("std");
const builtin = @import("builtin");
const stdx = @import("stdx");
const vsr = @import("vsr.zig");

const assert = std.debug.assert;
const Header = vsr.Header;
const HeaderEncrypted = vsr.HeaderEncrypted;
const MessagePool = vsr.message_pool.MessagePool;
const Message = MessagePool.Message;
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

    pub fn equal(self: Peer, other: Peer) bool {
        const self_int = std.mem.bytesAsValue(u256, std.mem.asBytes(&self)).*;
        const other_int = std.mem.bytesAsValue(u256, std.mem.asBytes(&other)).*;
        return self_int == other_int;
    }

    pub fn to_vsr_peer(self: Peer) vsr.Peer {
        switch (self.peer) {
            .client => {
                return .{ .client = self.id };
            },
            .replica => {
                return .{ .replica = @intCast(self.id) };
            },
        }
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

pub const EncryptionTransitContext = struct {
    self_id: u128,
    self_peer: PeerType,

    // Receiving a message requires looking up a session by key id.
    session_id_mapping: std.AutoHashMapUnmanaged(u128, *SessionTypes),

    // Sending a message requires looking up a session by logical identifier.
    replica_sessions: []?SessionTypes,
    client_sessions: std.AutoHashMapUnmanaged(u128, SessionTypes),

    const SessionTypes = union(enum) {
        encrypted: EncryptionTransit,
        unencrypted: EncryptionTransit2,
    };

    pub fn init(
        gpa: std.mem.Allocator,
        options: struct {
            replicas_max: u32,
            clients_max: u32,
            self_id: u128,
            self_peer: PeerType,
        },
    ) !EncryptionTransitContext {
        const capacity = options.clients_max + options.replicas_max;

        const replica_sessions: []?SessionTypes = try gpa.alloc(?SessionTypes, capacity);
        errdefer gpa.free(replica_sessions);
        for (replica_sessions) |*session| {
            session.* = null;
        }

        var client_sessions: std.AutoHashMapUnmanaged(u128, SessionTypes) = .{};
        try client_sessions.ensureTotalCapacity(gpa, options.clients_max);
        errdefer client_sessions.deinit(gpa);

        var session_id_mapping: std.AutoHashMapUnmanaged(u128, *SessionTypes) = .{};
        try session_id_mapping.ensureTotalCapacity(gpa, options.replicas_max + options.clients_max);
        errdefer session_id_mapping.deinit(gpa);

        return .{
            .self_id = options.self_id,
            .self_peer = options.self_peer,
            .replica_sessions = replica_sessions,
            .client_sessions = client_sessions,
            .session_id_mapping = session_id_mapping,
        };
    }

    pub fn deinit(context: *EncryptionTransitContext, gpa: std.mem.Allocator) void {
        assert(context.session_id_mapping.count() == 0);
        context.session_id_mapping.deinit(gpa);

        assert(context.client_sessions.count() == 0);
        context.client_sessions.deinit(gpa);

        for (context.replica_sessions) |*session| {
            assert(session.* == null);
        }
        gpa.free(context.replica_sessions);
        context.* = undefined;
    }

    pub fn is_handshake(header: *const HeaderEncrypted) bool {
        return header.header_tag == 1;
    }

    pub fn handshake_message_size(header: *const HeaderEncrypted) ?u32 {
        return if (is_handshake(header)) @sizeOf(Header.Handshake) +
            @sizeOf(KeyExchangeInsecure.KeyExchangeMessage) else null;
    }

    pub fn decrypt_header(
        context: *const EncryptionTransitContext,
        header: *const HeaderEncrypted,
    ) !Header {
        const session = context.session_id_mapping.get(header.header_key_id) orelse
            return error.InvalidKeyId;
        switch (session.*) {
            inline else => |*session_implementation| {
                return session_implementation.decrypt_header(header);
            },
        }
    }

    pub fn session_established(context: *EncryptionTransitContext, peer: vsr.Peer) bool {
        const session = switch (peer) {
            .replica => |replica| &(context.replica_sessions[replica] orelse return false),
            .client => |client_id| context.client_sessions.getPtr(client_id) orelse return false,
            else => unreachable,
        };

        switch (session.*) {
            inline else => |*session_implementation| {
                return session_implementation.established();
            },
        }
    }

    pub fn initiator_handshake(
        context: *EncryptionTransitContext,
        replica: u8,
    ) KeyExchangeInsecure.KeyExchangeMessage {
        if (context.replica_sessions[replica] == null) {
            context.replica_sessions[replica] = .{ .encrypted = EncryptionTransit.initiator(context.self_id, context.self_peer) };
        }
        switch (context.replica_sessions[replica].?) {
            inline else => |*session_implementation| {
                const handshake_result = session_implementation.handshake(null);
                assert(handshake_result == .send);
                return handshake_result.send;
            },
        }
    }

    pub fn consume_handshake(context: *EncryptionTransitContext, source: []const u8) union(enum) {
        peer: vsr.Peer,
        send: KeyExchangeInsecure.KeyExchangeMessage,
        err: anyerror,
    } {
        var header_encrypted: HeaderEncrypted = undefined;
        stdx.copy_disjoint(
            .exact,
            u8,
            std.mem.asBytes(&header_encrypted),
            source[0..@sizeOf(HeaderEncrypted)],
        );
        assert(handshake_message_size(&header_encrypted) != null);

        var key_exchange_message: KeyExchangeInsecure.KeyExchangeMessage = undefined;
        stdx.copy_disjoint(
            .exact,
            u8,
            std.mem.asBytes(&key_exchange_message),
            source[@sizeOf(HeaderEncrypted)..],
        );

        const session = context.session_id_mapping.get(header_encrypted.header_key_id).?;
        switch (session.*) {
            inline else => |*session_implementation| {
                switch (session_implementation.handshake(key_exchange_message)) {
                    .send => |response| {
                        return .{ .send = response };
                    },
                    .recv => unreachable,
                    .done => {
                        assert(session_implementation.established());
                        return .{ .peer = session_implementation.other() };
                    },
                    .failed => {
                        return .{ .err = error.KeyExchangeFailed };
                    },
                }
            },
        }
    }

    pub fn encrypt_message(
        context: *EncryptionTransitContext,
        peer: vsr.Peer,
        target: []u8,
        source: *const Message,
    ) void {
        const session = switch (peer) {
            .replica => |replica| &(context.replica_sessions[replica].?),
            .client => |client_id| context.client_sessions.getPtr(client_id).?,
            else => unreachable,
        };
        switch (session.*) {
            inline else => |*session_implementation| {
                session_implementation.encrypt_message(target, source);
            },
        }
    }

    pub fn decrypt_message(
        context: *EncryptionTransitContext,
        target: *Message,
        source: []const u8,
    ) !vsr.Peer {
        var header_encrypted: HeaderEncrypted = undefined;
        stdx.copy_disjoint(
            .exact,
            u8,
            std.mem.asBytes(&header_encrypted),
            source[0..@sizeOf(HeaderEncrypted)],
        );
        const session = context.session_id_mapping.get(header_encrypted.header_key_id) orelse
            return error.InvalidKeyId;

        switch (session.*) {
            inline else => |*session_implementation| {
                try session_implementation.decrypt_message(target, source);
                return session_implementation.other();
            },
        }
    }

    pub fn session_exists(context: *EncryptionTransitContext, session_id: u128) bool {
        return context.session_id_mapping.contains(session_id);
    }
};

pub const EncryptionTransit2 = struct {
    pub fn initiator(
        _: u128,
        _: PeerType,
    ) EncryptionTransit2 {
        unreachable;
    }
    pub fn decrypt_header(_: *EncryptionTransit2, _: *const HeaderEncrypted) !Header {
        unreachable;
    }

    pub fn decrypt_message(_: *EncryptionTransit2, _: *Message, _: []const u8) !void {
        unreachable;
    }

    pub fn encrypt_message(_: *EncryptionTransit2, _: []u8, _: *const Message) void {}

    pub fn handshake(
        _: *EncryptionTransit2,
        _: ?KeyExchangeInsecure.KeyExchangeMessage,
    ) HandshakeResult {
        unreachable;
    }
    pub fn other(_: *EncryptionTransit2) vsr.Peer {
        unreachable;
    }

    pub fn established(_: *EncryptionTransit2) bool {
        unreachable;
    }
};

const HandshakeResult = union(enum) {
    send: KeyExchangeInsecure.KeyExchangeMessage,
    recv,
    done: u128,
    failed,
};

pub const EncryptionTransit = struct {
    session_id: u128,
    key_exchange: ?KeyExchangeInsecure,
    cipher: ?SymmetricCipher,
    self: Peer,

    pub const KeyExchangeMessage = KeyExchangeInsecure.KeyExchangeMessage;
    pub const handshake_header_tag: u128 = 0;

    pub fn initiator(
        self_id: u128,
        self_peer: PeerType,
    ) EncryptionTransit {
        // The initiator decides on a random handshake id.
        const self: Peer = .{ .id = self_id, .peer = self_peer };
        return .{
            .session_id = std.crypto.random.int(u128),
            .key_exchange = null,
            .cipher = null,
            .self = self,
        };
    }

    pub fn responder(session_id: u128, self_id: u128, self_peer: PeerType) EncryptionTransit {
        // The initiator decides on a random handshake id.
        return .{
            .session_id = session_id,
            .key_exchange = null,
            .cipher = null,
            .self = .{ .id = self_id, .peer = self_peer },
        };
    }

    pub fn handshake(
        encryption: *EncryptionTransit,
        message: ?KeyExchangeInsecure.KeyExchangeMessage,
    ) HandshakeResult {
        if (encryption.key_exchange == null) {
            if (message == null) {
                encryption.key_exchange = .init(.responder, encryption.self);
            } else {
                encryption.key_exchange = .init(.initiator, encryption.self);
            }
        }

        assert(encryption.key_exchange != null);

        switch (encryption.key_exchange.?.feed(message)) {
            .send => |msg| {
                return .{ .send = msg };
            },
            .recv => {
                return .recv;
            },
            .done => |result| {
                const other_peer: Peer = .{ .id = result.peer_id, .peer = result.peer_type };
                encryption.key_exchange = null;
                encryption.cipher = SymmetricCipher.init(result.shared_secret, encryption.self, other_peer);
                return .{ .done = encryption.cipher.?.key_id };
            },
            .terminate => {
                encryption.key_exchange = null;
                return .failed;
            },
        }
    }

    pub fn other(encryption: *EncryptionTransit) vsr.Peer {
        assert(encryption.established());
        return encryption.cipher.?.other.to_vsr_peer();
    }

    pub fn established(encryption: *EncryptionTransit) bool {
        return encryption.cipher != null;
    }

    pub fn encrypt_message(encryption: *EncryptionTransit, target: []u8, source: *const Message) void {
        assert(encryption.established());
        encryption.cipher.?.encrypt_message(encryption.session_id, target, source);
    }

    pub fn decrypt_message(encryption: *EncryptionTransit, target: *Message, source: []const u8) !void {
        assert(encryption.established());
        return encryption.cipher.?.decrypt_message(target, source, encryption.session_id);
    }

    pub fn decrypt_header(encryption: *EncryptionTransit, header_encrypted: *const HeaderEncrypted) !Header {
        assert(encryption.established());
        return encryption.cipher.?.decrypt_header(header_encrypted, encryption.session_id);
    }

    pub fn decrypt_body(
        encryption: *EncryptionTransit,
        target: *Message,
        header: *const Header,
        body_encrypted: []const u8,
    ) !void {
        assert(encryption.established());

        const enc = &encryption.cipher.?;
        target.header.* = header.*;

        try enc.decrypt_body(
            .{ .body_tag = header.body_tag, .body_nonce = header.body_nonce },
            target.body_used(),
            body_encrypted,
        );

        target.header.set_checksum_body(target.body_used());
        target.header.set_zeroes();
        target.header.set_checksum();
    }
};

// TODO(georg): How would be put that into messages?
// Use magic value of HeaderEncrypted
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

    pub fn init(role: Role, self: Peer) KeyExchangeInsecure {
        return .{
            .state = switch (role) {
                .responder => .{ .responder = .recv_dh },
                .initiator => .{ .initiator = .send_dh },
            },
            .self_id = self.id,
            .self_type = self.peer,
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
    var initiator_kex: KeyExchangeInsecure = .init(.initiator, .{ .id = 1, .peer = .replica });
    var responder_kex: KeyExchangeInsecure = .init(.responder, .{ .id = 2, .peer = .replica });

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

pub const SymmetricCipher = struct {
    const BodyTagNonce = struct { body_nonce: u128, body_tag: u128 };
    key_id: u128,

    other: Peer,

    key_send_header: [32]u8,
    // send_header_counter: NonceCounter = .{},
    key_send_body: [32]u8,

    key_recv_header: [32]u8,
    // recv_header_window: NonceWindow = .{},
    key_recv_body: [32]u8,

    pub fn init(ephemeral_secret: [32]u8, peer_self: Peer, peer_other: Peer) SymmetricCipher {
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

            .other = peer_other,

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

    pub fn deinit(enc: *SymmetricCipher) void {
        std.crypto.utils.secureZero(u8, std.mem.asBytes(enc));
        enc.* = undefined;
    }

    pub fn encrypt_message(
        cipher: *SymmetricCipher,
        key_id: u128,
        target: []u8,
        source: *const Message,
    ) void {
        const body_tag_nonce = cipher.encrypt_body(
            target[@sizeOf(Header)..source.header.size],
            source.body_used(),
        );

        var header = source.header.*;
        header.body_tag = body_tag_nonce.body_tag;
        header.body_nonce = body_tag_nonce.body_nonce;

        // TODO: When sending a message, assert the last 16 bytes are not zero.
        @memset(target[source.header.size..], 0);
        const header_encrypted = cipher.encrypt_header(&header, key_id);

        stdx.copy_disjoint(
            .exact,
            u8,
            target[0..@sizeOf(HeaderEncrypted)],
            std.mem.asBytes(&header_encrypted),
        );
    }

    pub fn decrypt_message(
        cipher: *SymmetricCipher,
        target: *Message,
        source: []const u8,
        key_id: u128,
    ) !void {
        var header_encrypted: HeaderEncrypted = undefined;
        stdx.copy_disjoint(.exact, u8, std.mem.asBytes(&header_encrypted), source[0..@sizeOf(HeaderEncrypted)]);

        target.header.* = try cipher.decrypt_header(&header_encrypted, key_id);

        try cipher.decrypt_body(
            .{
                .body_tag = target.header.body_tag,
                .body_nonce = target.header.body_nonce,
            },
            target.body_used(),
            source[@sizeOf(HeaderEncrypted)..],
        );

        target.header.set_checksum_body(target.body_used());
        target.header.set_zeroes();
        target.header.set_checksum();
    }

    pub fn encrypt_header(
        cipher: *SymmetricCipher,
        header: *const Header,
        key_id: u128,
    ) HeaderEncrypted {
        const key = cipher.key_send_header;
        const nonce = std.crypto.random.int(u128);

        assert(header.body_tag != 0);
        assert(!stdx.zeroed(&key));
        assert(nonce != 0);
        assert(!std.mem.eql(u8, &key, &@as([32]u8, @splat(undefined_u8))));
        assert(nonce != undefined_u128);

        const bytes_cleartext = header.slice_encrypted_const();

        var encrypted: HeaderEncrypted = .{
            .header_tag = header.header_tag,
            .header_key_id = header.header_key_id,
            .header_nonce = header.header_nonce,
            .encrypted_data = undefined,
        };

        const bytes_ciphertext = encrypted.slice_encrypted();

        const tag = std.mem.asBytes(&encrypted.header_tag);
        encrypted.header_nonce = nonce;
        encrypted.header_key_id = key_id;
        const ad = encrypted.slice_associated_data();

        std.log.info("header encryption key: {any}", .{key});
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
        cipher: *SymmetricCipher,
        header: *const HeaderEncrypted,
        key_id: u128,
    ) !Header {
        const key = cipher.key_recv_header;
        assert(!stdx.zeroed(&key));
        assert(!std.mem.eql(u8, &key, &@as([32]u8, @splat(undefined_u8))));

        if (header.header_key_id != key_id) {
            return error.InvalidKeyId;
        }

        if (header.header_nonce == 0 or header.header_nonce == undefined_u128) {
            return error.InvalidHeaderNonce;
        }

        var decrypted: Header = std.mem.bytesAsValue(Header, std.mem.asBytes(header)).*;
        const bytes_ciphertext = header.slice_encrypted_const();
        const tag = std.mem.asBytes(&header.header_tag);
        const ad = header.slice_associated_data_const();

        const bytes_cleartext = decrypted.slice_encrypted();

        std.log.info("header encrypted: {}", .{header});
        std.log.info("header decryption key: {any}", .{key});

        try aegis.Aegis256.decrypt(
            bytes_cleartext,
            bytes_ciphertext,
            tag.*,
            ad,
            extend_nonce(header.header_nonce),
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
        cipher: *SymmetricCipher,
        target: []u8,
        source: []const u8,
    ) BodyTagNonce {
        const key = cipher.key_send_body;
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

    pub fn decrypt_body(
        cipher: *SymmetricCipher,
        body_tag_nonce: BodyTagNonce,
        target: []u8,
        source: []const u8,
    ) !void {
        const key = cipher.key_recv_body;

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
        _ = target;
        _ = source;
        _ = keys;
        // target.header.* = decrypt_header(
        //     source.header,
        //     keys.header_key,
        //     keys.header_nonce,
        // );
        // decrypt_body(
        //     target.header,
        //     target.body_used(),
        //     source.buffer[@sizeOf(Header)..target.header.size],
        //     keys.body_key,
        //     keys.body_nonce,
        // );
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
    ) HeaderEncrypted {
        assert(header.body_tag != 0);
        assert(!stdx.zeroed(&key));
        assert(nonce != 0);
        assert(!std.mem.eql(u8, &key, &@as([32]u8, @splat(undefined_u8))));
        assert(nonce != undefined_u128);

        const bytes_cleartext = header.slice_encrypted_const();

        var encrypted: HeaderEncrypted = .{
            .header_tag = header.header_tag,
            .header_key_id = header.header_key_id,
            .header_nonce = header.header_nonce,
            .encrypted_data = undefined,
        };

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
        header: *const HeaderEncrypted,
        key: [32]u8,
    ) !Header {
        assert(!stdx.zeroed(&key));
        assert(!std.mem.eql(u8, &key, &@as([32]u8, @splat(undefined_u8))));

        if (header.header_nonce == 0 or header.header_nonce == undefined_u128) {
            return error.InvalidHeaderNonce;
        }

        var decrypted = std.mem.bytesAsValue(Header, std.mem.asBytes(header)).*;
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
    var enc_a = SymmetricCipher.init(ephemeral_secret, peer_a, peer_b);
    defer enc_a.deinit();

    var enc_b = SymmetricCipher.init(ephemeral_secret, peer_b, peer_a);
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

    try std.testing.expect(
        !stdx.equal_bytes(HeaderEncrypted, std.mem.bytesAsValue(HeaderEncrypted, &header_unencrypted), &header_encrypted),
    );

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
    var enc_a = SymmetricCipher.init(ephemeral_secret, peer_a, peer_b);
    defer enc_a.deinit();

    var enc_b = SymmetricCipher.init(ephemeral_secret, peer_b, peer_a);
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

test "SessionManager Unit Test" {
    const gpa = std.testing.allocator;
    const capacity: u32 = 64;

    var session_manager = try EncryptionTransitContext.init(
        gpa,
        capacity,
    );
    defer session_manager.deinit(gpa);
}
