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
const log = std.log.scoped(.encryption);

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

// TODO:
// Add metrics for e.g. # handshakes, # handshakes pending, # ciphers, etc..
pub const EncryptionNetwork = struct {
    self_id: u128,
    self_peer: PeerType,

    // Receiving a message requires looking up a cipher by physical identifier (key id).
    key_id_cipher: std.AutoHashMapUnmanaged(u128, *CipherTypes),

    // Sending a message requires looking up a cipher by logical identifier.
    replica_cipher: []?CipherTypes,
    client_cipher: std.AutoHashMapUnmanaged(u128, CipherTypes),

    handshakes_pending: std.AutoArrayHashMapUnmanaged(u128, HandshakeTypes),

    const HandshakeTypes = union(enum) {
        insecure: HandshakeInsecure,
    };

    const CipherTypes = union(enum) {
        aegis_256_nonce_128: CipherAegis256Nonce128,
    };

    pub fn init(
        gpa: std.mem.Allocator,
        options: struct {
            replicas_max: u32,
            clients_max: u32,
            self_id: u128,
            self_peer: PeerType,
        },
    ) !EncryptionNetwork {
        const capacity = options.clients_max + options.replicas_max;

        const replica_cipher: []?CipherTypes = try gpa.alloc(?CipherTypes, capacity);
        errdefer gpa.free(replica_cipher);
        for (replica_cipher) |*encryption_network| {
            encryption_network.* = null;
        }

        var client_cipher: std.AutoHashMapUnmanaged(u128, CipherTypes) = .{};
        try client_cipher.ensureTotalCapacity(gpa, options.clients_max);
        errdefer client_cipher.deinit(gpa);

        var key_id_cipher: std.AutoHashMapUnmanaged(u128, *CipherTypes) = .{};
        try key_id_cipher.ensureTotalCapacity(gpa, options.replicas_max + options.clients_max);
        errdefer key_id_cipher.deinit(gpa);

        var handshakes_pending: std.AutoArrayHashMapUnmanaged(u128, HandshakeTypes) = .{};
        try handshakes_pending.ensureTotalCapacity(gpa, capacity);
        errdefer handshakes_pending.deinit(gpa);

        return .{
            .self_id = options.self_id,
            .self_peer = options.self_peer,
            .replica_cipher = replica_cipher,
            .client_cipher = client_cipher,
            .key_id_cipher = key_id_cipher,
            .handshakes_pending = handshakes_pending,
        };
    }

    pub fn deinit(encryption: *EncryptionNetwork, gpa: std.mem.Allocator) void {
        var handshake_iterator = encryption.handshakes_pending.iterator();
        while (handshake_iterator.next()) |entry| {
            switch (entry.value_ptr.*) {
                inline else => |*handshake_impl| {
                    handshake_impl.deinit();
                },
            }
        }
        encryption.handshakes_pending.deinit(gpa);

        var client_cipher_iterator = encryption.client_cipher.iterator();
        while (client_cipher_iterator.next()) |entry| {
            switch (entry.value_ptr.*) {
                inline else => |*cipher_impl| {
                    cipher_impl.deinit();
                },
            }
        }
        encryption.client_cipher.deinit(gpa);

        for (encryption.replica_cipher) |*maybe_cipher| {
            if (maybe_cipher.*) |*cipher| {
                switch (cipher.*) {
                    inline else => |*cipher_impl| {
                        cipher_impl.deinit();
                    },
                }
            }
        }
        gpa.free(encryption.replica_cipher);

        encryption.key_id_cipher.deinit(gpa);

        encryption.* = undefined;
    }
    
    pub fn insert_existing(
        encryption: *EncryptionNetwork,
        other: *EncryptionNetwork,
    ) void {
        if (encryption == other) {
            encryption.client_cipher.clearRetainingCapacity();
            encryption.handshakes_pending.clearRetainingCapacity();
            encryption.key_id_cipher.clearRetainingCapacity();
            for (encryption.replica_cipher) |*replica_cipher| {
                replica_cipher.* = null;
            }
            return;
        }

        assert(encryption.self_id == other.self_id);
        assert(encryption.self_peer == other.self_peer);
        assert(encryption.replica_cipher.len == other.replica_cipher.len);

        stdx.copy_disjoint(.exact, ?CipherTypes, encryption.replica_cipher, other.replica_cipher);

        encryption.client_cipher.clearRetainingCapacity();
        var client_cipher_iterator = other.client_cipher.iterator();
        while (client_cipher_iterator.next()) |cipher| {
            encryption.client_cipher.putAssumeCapacity(cipher.key_ptr.*, cipher.value_ptr.*);
        }

        encryption.handshakes_pending.clearRetainingCapacity();
        var handshakes_iterator = other.handshakes_pending.iterator();
        while (handshakes_iterator.next()) |handshake| {
            encryption.handshakes_pending.putAssumeCapacity(
                handshake.key_ptr.*,
                handshake.value_ptr.*,
            );
        }

        encryption.key_id_cipher.clearRetainingCapacity();
        var key_id_ciphers_iterator = other.key_id_cipher.iterator();

        const replica_base = @intFromPtr(other.replica_cipher.ptr);
        const replica_end = replica_base + other.replica_cipher.len * @sizeOf(?CipherTypes);

        while (key_id_ciphers_iterator.next()) |key_id_cipher| {
            const key_id = key_id_cipher.key_ptr.*;
            const cihper_ptr_other = key_id_cipher.value_ptr.*;

            const ptr_addr = @intFromPtr(cihper_ptr_other);

            if (ptr_addr >= replica_base and ptr_addr < replica_end) {
                // Cipher for communication with replica.
                const index = (ptr_addr - replica_base) / @sizeOf(?CipherTypes);
                encryption.key_id_cipher.putAssumeCapacity(
                    key_id,
                    &(encryption.replica_cipher[index].?),
                );
            } else {
                // Cipher for communication with client.
                encryption.key_id_cipher.putAssumeCapacity(
                    key_id,
                    encryption.client_cipher.getPtr(key_id).?,
                );
            }
        }
    }

    pub fn message_type(header: *const HeaderEncrypted) union(enum) { encrypted, handshake: u32 } {
        if (header.header_tag == 1) {
            return .{ .handshake = @sizeOf(HandshakeMessage) };
        }
        return .encrypted;
    }

    pub fn handshake_initiate(
        encryption: *EncryptionNetwork,
    ) HandshakeMessage {
        const handshake = HandshakeInsecure.initiator(.{
            .id = encryption.self_id,
            .peer = encryption.self_peer,
        });

        log.debug("initiator_handshake: creating handshake id  ({d})", .{handshake.handshake_id});

        if (encryption.handshakes_pending.count() == encryption.handshakes_pending.capacity()) {
            const handshake_id = encryption.handshakes_pending.entries.get(0).key;
            log.warn("initiator_handshake: dropping handshake id ({d})", .{handshake_id});
            // TODO: maybe optimize this to avoid shifting on every remove.
            encryption.handshakes_pending.orderedRemoveAt(0);
        }
        const gop = encryption.handshakes_pending.getOrPutAssumeCapacity(handshake.handshake_id);
        assert(!gop.found_existing);
        gop.value_ptr.* = .{ .insecure = handshake };

        switch (gop.value_ptr.*) {
            inline else => |*handshake_impl| {
                const handshake_result = handshake_impl.feed(null);
                assert(handshake_result == .operation);
                assert(handshake_result.operation.message != null);
                return handshake_result.operation.message.?;
            },
        }
    }

    pub fn handshake_consume(
        encryption: *EncryptionNetwork,
        source: []const u8,
    ) union(enum) {
        operation: struct { message: ?HandshakeMessage, peer: ?vsr.Peer },
        err: anyerror,
    } {
        defer encryption.verify_state();

        var handshake_message: HandshakeMessage = undefined;
        stdx.copy_disjoint(
            .exact,
            u8,
            std.mem.asBytes(&handshake_message),
            source,
        );

        assert(message_type(&handshake_message.header) == .handshake);

        if (encryption.handshakes_pending.capacity() == encryption.handshakes_pending.count()) {
            const handshake_old = encryption.handshakes_pending.entries.get(0);
            log.warn("handshake_consume: abort handshake: {d}", .{handshake_old.key});
            encryption.handshakes_pending.orderedRemoveAt(0);
        }
        assert(encryption.handshakes_pending.capacity() > encryption.handshakes_pending.count());

        const gop_handshake = encryption.handshakes_pending.getOrPutAssumeCapacity(
            handshake_message.header.header_key_id,
        );

        if (!gop_handshake.found_existing) {
            switch (encryption.self_peer) {
                .client => {
                    log.warn(
                        "consume_handshake: dropping message for unknown handshake id  ({d})",
                        .{handshake_message.header.header_key_id},
                    );
                    const remove_result = encryption.handshakes_pending.orderedRemove(
                        handshake_message.header.header_key_id,
                    );
                    assert(remove_result);
                    return .{ .err = error.HandshakeFailed };
                },
                .replica => {
                    const handshake = HandshakeInsecure.responder(
                        .{ .id = encryption.self_id, .peer = encryption.self_peer },
                        handshake_message.header.header_key_id,
                    );
                    gop_handshake.value_ptr.* = .{ .insecure = handshake };
                },
            }
        }

        switch (gop_handshake.value_ptr.*) {
            inline else => |*handshake_impl| {
                switch (handshake_impl.feed(handshake_message)) {
                    .operation => |operation| {
                        assert(operation.result != null or operation.message != null);
                        var peer: ?vsr.Peer = null;
                        if (operation.result) |result| {
                            const other_peer: Peer = .{
                                .id = result.peer_id,
                                .peer = result.peer_type,
                            };
                            peer = other_peer.to_vsr_peer();

                            // TODO: maybe fix this to avoid shifting.
                            const remove_result = encryption.handshakes_pending.orderedRemove(
                                handshake_message.header.header_key_id,
                            );
                            assert(remove_result);
                            const cipher = CipherAegis256Nonce128.init(
                                result.shared_secret,
                                .{ .peer = encryption.self_peer, .id = encryption.self_id },
                                other_peer,
                            );

                            switch (result.peer_type) {
                                .client => {
                                    const gop = encryption.client_cipher.getOrPutAssumeCapacity(
                                        result.peer_id,
                                    );
                                    if (gop.found_existing) {
                                        const old_key_id = switch (gop.value_ptr.*) {
                                            inline else => |old_cipher| old_cipher.key_id,
                                        };
                                        const removed = encryption.key_id_cipher.remove(old_key_id);
                                        assert(removed);
                                    }

                                    gop.value_ptr.* =
                                        .{ .aegis_256_nonce_128 = cipher };
                                    encryption.key_id_cipher.putAssumeCapacity(
                                        cipher.key_id,
                                        gop.value_ptr,
                                    );
                                },
                                .replica => {
                                    assert(result.peer_id < encryption.replica_cipher.len);
                                    const index: usize = @intCast(result.peer_id);
                                    if (encryption.replica_cipher[index]) |old_cipher| {
                                        const old_key_id = switch (old_cipher) {
                                            inline else => |c| c.key_id,
                                        };
                                        const removed = encryption.key_id_cipher.remove(old_key_id);
                                        assert(removed);
                                    }
                                    encryption.replica_cipher[index] = .{
                                        .aegis_256_nonce_128 = cipher,
                                    };
                                    encryption.key_id_cipher.putAssumeCapacity(
                                        cipher.key_id,
                                        &encryption.replica_cipher[index].?,
                                    );
                                },
                            }

                            log.debug("handshake_consume: cipher key id: ({d}) ", .{
                                cipher.key_id,
                            });
                            log.debug("handshake_consume: completed handshake id ({d})", .{
                                handshake_message.header.header_key_id,
                            });
                        }
                        return .{ .operation = .{ .message = operation.message, .peer = peer } };
                    },
                    .terminate => {
                        log.warn(
                            "handshake_consume: terminating handshake handshake id  ({d})",
                            .{handshake_message.header.header_key_id},
                        );
                        const remove_result = encryption.handshakes_pending.orderedRemove(
                            handshake_message.header.header_key_id,
                        );
                        assert(remove_result);
                        return .{ .err = error.HandshakeFailed };
                    },
                }
            },
        }
    }

    pub fn handshake_completed(encryption: *EncryptionNetwork, peer: vsr.Peer) bool {
        switch (peer) {
            .replica => |replica| return encryption.replica_cipher[replica] != null,
            .client => |client_id| return encryption.client_cipher.contains(client_id),
            else => unreachable,
        }
    }

    pub fn decrypt_header(
        encryption: *const EncryptionNetwork,
        header: *const HeaderEncrypted,
    ) !Header {
        const cipher = encryption.key_id_cipher.get(header.header_key_id) orelse
            return error.InvalidKeyId;

        assert(message_type(header) == .encrypted);

        switch (cipher.*) {
            inline else => |*cipher_impl| {
                return cipher_impl.decrypt_header(header);
            },
        }
    }

    fn verify_state(encryption: *EncryptionNetwork) void {
        var cipher_count: u64 = 0;

        // Every non-null replica cipher must have a matching entry in key_id_cipher,
        // keyed by its own key_id, pointing back at its actual storage slot.
        for (encryption.replica_cipher, 0..) |*maybe_cipher, index| {
            if (maybe_cipher.*) |cipher| {
                const key_id = switch (cipher) {
                    inline else => |cipher_impl| cipher_impl.key_id,
                };
                cipher_count += 1;

                const ptr = encryption.key_id_cipher.get(key_id);
                assert(ptr != null);
                assert(ptr.? == &(encryption.replica_cipher[index].?));
            }
        }

        // Every client cipher must have a matching entry in key_id_cipher,
        // keyed by its own key_id, pointing back at its actual storage slot.
        var client_cipher_iterator = encryption.client_cipher.iterator();
        while (client_cipher_iterator.next()) |entry| {
            const client_id = entry.key_ptr.*;
            const cipher = entry.value_ptr.*;
            const key_id = switch (cipher) {
                inline else => |cipher_impl| cipher_impl.key_id,
            };
            cipher_count += 1;

            const ptr = encryption.key_id_cipher.get(key_id);
            assert(ptr != null);
            assert(ptr.? == encryption.client_cipher.getPtr(client_id).?);
        }

        // No stale/orphaned entries: key_id_cipher must contain exactly the
        // live ciphers found above, nothing more.
        assert(encryption.key_id_cipher.count() == cipher_count);
    }

    pub fn encrypt_message(
        encryption: *EncryptionNetwork,
        peer: vsr.Peer,
        target: []u8,
        source: *const Message,
    ) void {
        const encryption_network = switch (peer) {
            .replica => |replica| &(encryption.replica_cipher[replica].?),
            .client => |client_id| encryption.client_cipher.getPtr(client_id).?,
            else => unreachable,
        };
        switch (encryption_network.*) {
            inline else => |*encryption_network_implementation| {
                encryption_network_implementation.encrypt_message(target, source);
            },
        }
    }

    pub fn decrypt_message(
        encryption: *EncryptionNetwork,
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
        const cipher = encryption.key_id_cipher.get(header_encrypted.header_key_id) orelse
            return error.InvalidKeyId;

        switch (cipher.*) {
            inline else => |*cipher_impl| {
                try cipher_impl.decrypt_message(target, source);
                return cipher_impl.other.to_vsr_peer();
            },
        }
    }
};

const SharedSecret = [32]u8;

pub const HandshakeResult = struct {
    shared_secret: SharedSecret,
    peer_id: u128,
    peer_type: PeerType,
};

pub const HandshakeMessage = extern struct {
    header: vsr.HeaderEncrypted,
    peer_type: PeerType,
    peer_id: u128,
    public_key: [32]u8,
    message_type: MessageType,

    const MessageType = enum(u8) { diffie_hellman, identity, unknown };

    pub fn init(
        handshake_id: u128,
        peer_type: PeerType,
        peer_id: u128,
        public_key: [32]u8,
        message_type: MessageType,
    ) HandshakeMessage {
        const header_encrypted: HeaderEncrypted = .{
            .header_tag = 1,
            .header_key_id = handshake_id,
            .header_nonce = 0,
            .encrypted_data = @splat(0),
        };

        assert(EncryptionNetwork.message_type(&header_encrypted) == .handshake);

        return .{
            .header = header_encrypted,
            .peer_type = peer_type,
            .peer_id = peer_id,
            .public_key = public_key,
            .message_type = message_type,
        };
    }
};

// TODO(georg): How would be put that into messages?
// Use magic value of HeaderEncrypted
pub const HandshakeInsecure = struct {
    handshake_id: u128,
    state: State,
    self_id: u128,
    self_type: PeerType,
    key_pair: X25519.KeyPair,
    result: ?HandshakeResult = null,

    const State = union(Role) {
        initiator: InitiatorState,
        responder: ResponderState,
    };

    const InitiatorState = enum {
        send_dh,
        send_identity,
    };

    const ResponderState = enum {
        recv_dh,
        recv_identity,
    };

    const Role = enum {
        initiator,
        responder,
    };

    pub const Operation = struct {
        message: ?HandshakeMessage,
        result: ?HandshakeResult,
    };

    pub fn initiator_deterministic(
        self: Peer,
        seed: [X25519.seed_length]u8,
        handshake_id: u128,
    ) !HandshakeInsecure {
        const key_pair = try X25519.KeyPair.generateDeterministic(seed);
        return .{
            .handshake_id = handshake_id,
            .state = .{ .initiator = .send_dh },
            .self_id = self.id,
            .self_type = self.peer,
            .key_pair = key_pair,
        };
    }

    pub fn initiator(self: Peer) HandshakeInsecure {
        const handshake_id = stdx.unique_u128();

        var random_seed: [X25519.seed_length]u8 = undefined;
        while (true) {
            std.crypto.random.bytes(&random_seed);
            return initiator_deterministic(self, random_seed, handshake_id) catch {
                @branchHint(.unlikely);
                continue;
            };
        }
    }

    pub fn responder_deterministic(
        self: Peer,
        seed: [X25519.seed_length]u8,
        handshake_id: u128,
    ) !HandshakeInsecure {
        const key_pair = try X25519.KeyPair.generateDeterministic(seed);
        return .{
            .handshake_id = handshake_id,
            .state = .{ .responder = .recv_dh },
            .self_id = self.id,
            .self_type = self.peer,
            .key_pair = key_pair,
        };
    }

    pub fn responder(self: Peer, handshake_id: u128) HandshakeInsecure {
        var random_seed: [X25519.seed_length]u8 = undefined;
        while (true) {
            std.crypto.random.bytes(&random_seed);
            return responder_deterministic(self, random_seed, handshake_id) catch {
                @branchHint(.unlikely);
                continue;
            };
        }
    }

    pub fn deinit(handshake: *HandshakeInsecure) void {
        std.crypto.utils.secureZero(u8, std.mem.asBytes(handshake));
        handshake.* = undefined;
    }

    pub fn feed(
        handshake: *HandshakeInsecure,
        maybe_msg: ?HandshakeMessage,
    ) union(enum) {
        operation: Operation,
        terminate,
    } {
        switch (handshake.state) {
            .initiator => |state| {
                switch (state) {
                    .send_dh => {
                        if (maybe_msg != null) return .terminate;
                        log.info("feed: (initator) send dh", .{});
                        handshake.state.initiator = .send_identity;
                        return .{
                            .operation = .{ .message = .init(
                                handshake.handshake_id,
                                handshake.self_type,
                                handshake.self_id,
                                handshake.key_pair.public_key,
                                .diffie_hellman,
                            ), .result = null },
                        };
                    },
                    .send_identity => {
                        if (maybe_msg == null) {
                            return .terminate;
                        }
                        const msg = maybe_msg.?;
                        if (msg.message_type != .diffie_hellman) {
                            return .terminate;
                        }
                        log.info("feed: (initator) received dh", .{});

                        const shared_secret = X25519.scalarmult(
                            handshake.key_pair.secret_key,
                            msg.public_key,
                        ) catch {
                            return .terminate;
                        };

                        handshake.result = .{
                            .shared_secret = shared_secret,
                            .peer_type = msg.peer_type,
                            .peer_id = msg.peer_id,
                        };
                        handshake.state.initiator = .send_identity;
                        log.info("feed: (initator) send identity", .{});

                        return .{
                            .operation = .{ .message = .init(
                                handshake.handshake_id,
                                handshake.self_type,
                                handshake.self_id,
                                handshake.key_pair.public_key,
                                .identity,
                            ), .result = handshake.result.? },
                        };
                    },
                }
            },
            .responder => |state| {
                switch (state) {
                    .recv_dh => {
                        if (maybe_msg == null) {
                            return .terminate;
                        }
                        const msg = maybe_msg.?;
                        if (msg.message_type != .diffie_hellman) {
                            return .terminate;
                        }
                        log.info("feed: (responder) received dh", .{});

                        const shared_secret = X25519.scalarmult(
                            handshake.key_pair.secret_key,
                            msg.public_key,
                        ) catch {
                            return .terminate;
                        };

                        handshake.result = .{
                            .shared_secret = shared_secret,
                            .peer_type = msg.peer_type,
                            .peer_id = msg.peer_id,
                        };
                        handshake.state.responder = .recv_identity;
                        log.info("feed: (responder) send dh", .{});

                        return .{
                            .operation = .{ .message = .init(
                                handshake.handshake_id,
                                handshake.self_type,
                                handshake.self_id,
                                handshake.key_pair.public_key,
                                .diffie_hellman,
                            ), .result = null },
                        };
                    },
                    .recv_identity => {
                        if (maybe_msg == null) {
                            return .terminate;
                        }
                        const msg = maybe_msg.?;
                        if (msg.message_type != .identity) {
                            return .terminate;
                        }

                        log.info("feed: (responder) received identity", .{});

                        return .{ .operation = .{
                            .message = null,
                            .result = handshake.result.?,
                        } };
                    },
                }
            },
        }
    }
};

test "HandshakeInsecure" {
    var prng = stdx.PRNG.from_seed_testing();

    const handshake_id = prng.int(u128);
    var seed_initiator: [X25519.seed_length]u8 = undefined;
    prng.fill(&seed_initiator);
    var seed_responder: [X25519.seed_length]u8 = undefined;
    prng.fill(&seed_responder);
    var handshake_initiator = HandshakeInsecure.initiator_deterministic(
        .{ .id = 1, .peer = .replica },
        seed_initiator,
        handshake_id,
    ) catch unreachable;
    var handshake_responder = HandshakeInsecure.responder_deterministic(
        .{ .id = 2, .peer = .replica },
        seed_responder,
        handshake_id,
    ) catch unreachable;

    var result_initiator: ?HandshakeResult = null;
    var result_responder: ?HandshakeResult = null;

    var in_flight: struct {
        message: ?HandshakeMessage,
        source: enum { initiator, responder },
    } = .{ .message = null, .source = .responder };

    while (result_initiator == null or result_responder == null) {
        switch (in_flight.source) {
            .responder => {
                if (result_initiator != null) {
                    continue;
                }
                const operation = handshake_initiator.feed(in_flight.message).operation;
                result_initiator = operation.result;
                in_flight.message = operation.message;
                in_flight.source = .initiator;
            },
            .initiator => {
                if (result_responder != null) {
                    continue;
                }
                const operation = handshake_responder.feed(in_flight.message).operation;
                result_responder = operation.result;
                in_flight.message = operation.message;
                in_flight.source = .responder;
            },
        }
    }

    try std.testing.expectEqualSlices(
        u8,
        &result_initiator.?.shared_secret,
        &result_responder.?.shared_secret,
    );

    try std.testing.expectEqual(
        result_initiator.?.peer_type,
        handshake_responder.self_type,
    );

    try std.testing.expectEqual(
        result_initiator.?.peer_id,
        handshake_responder.self_id,
    );

    try std.testing.expectEqual(
        result_responder.?.peer_type,
        handshake_initiator.self_type,
    );

    try std.testing.expectEqual(
        result_responder.?.peer_id,
        handshake_initiator.self_id,
    );
}

// TODO: implement NonceCounter, otherwise vulnerable to
// replay attacks.
pub const CipherAegis256Nonce128 = struct {
    const BodyTagNonce = struct { body_nonce: u128, body_tag: u128 };
    key_id: u128,

    other: Peer,

    key_send_header: [32]u8,
    // send_header_counter: NonceCounter = .{},
    key_send_body: [32]u8,

    key_recv_header: [32]u8,
    // recv_header_window: NonceWindow = .{},
    key_recv_body: [32]u8,

    pub fn init(
        ephemeral_secret: [32]u8,
        peer_self: Peer,
        peer_other: Peer,
    ) CipherAegis256Nonce128 {
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

    pub fn deinit(enc: *CipherAegis256Nonce128) void {
        std.crypto.utils.secureZero(u8, std.mem.asBytes(enc));
        enc.* = undefined;
    }

    pub fn encrypt_message(
        cipher: *CipherAegis256Nonce128,
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
        const header_encrypted = cipher.encrypt_header(&header);

        stdx.copy_disjoint(
            .exact,
            u8,
            target[0..@sizeOf(HeaderEncrypted)],
            std.mem.asBytes(&header_encrypted),
        );
    }

    pub fn decrypt_message(
        cipher: *CipherAegis256Nonce128,
        target: *Message,
        source: []const u8,
    ) !void {
        var header_encrypted: HeaderEncrypted = undefined;
        stdx.copy_disjoint(
            .exact,
            u8,
            std.mem.asBytes(&header_encrypted),
            source[0..@sizeOf(HeaderEncrypted)],
        );

        target.header.* = try cipher.decrypt_header(&header_encrypted);

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
        cipher: *CipherAegis256Nonce128,
        header: *const Header,
    ) HeaderEncrypted {
        const key = cipher.key_send_header;
        const nonce = stdx.unique_u128();

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
        encrypted.header_key_id = cipher.key_id;
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
        cipher: *CipherAegis256Nonce128,
        header: *const HeaderEncrypted,
    ) !Header {
        const key = cipher.key_recv_header;
        assert(!stdx.zeroed(&key));
        assert(!std.mem.eql(u8, &key, &@as([32]u8, @splat(undefined_u8))));

        if (header.header_key_id != cipher.key_id) {
            return error.AuthenticationFailed;
        }

        assert(header.header_key_id == cipher.key_id);

        if (header.header_nonce == 0 or header.header_nonce == undefined_u128) {
            return error.InvalidHeaderNonce;
        }

        assert(header.header_nonce != 0 and header.header_nonce != undefined_u128);

        var decrypted: Header = std.mem.bytesAsValue(Header, std.mem.asBytes(header)).*;
        const bytes_ciphertext = header.slice_encrypted_const();
        const tag = std.mem.asBytes(&header.header_tag);
        const ad = header.slice_associated_data_const();

        const bytes_cleartext = decrypted.slice_encrypted();

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
        cipher: *CipherAegis256Nonce128,
        target: []u8,
        source: []const u8,
    ) BodyTagNonce {
        const key = cipher.key_send_body;
        const nonce = stdx.unique_u128();

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
        cipher: *CipherAegis256Nonce128,
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
    var enc_a = CipherAegis256Nonce128.init(ephemeral_secret, peer_a, peer_b);
    defer enc_a.deinit();

    var enc_b = CipherAegis256Nonce128.init(ephemeral_secret, peer_b, peer_a);
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
        !stdx.equal_bytes(
            HeaderEncrypted,
            std.mem.bytesAsValue(HeaderEncrypted, &header_unencrypted),
            &header_encrypted,
        ),
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

test "EncryptionNetwork Bit Fuzzer" {
    var prng = stdx.PRNG.from_seed_testing();

    const ephemeral_secret: [32]u8 = blk: {
        var ephemeral_secret: [32]u8 = undefined;
        prng.fill(&ephemeral_secret);
        break :blk ephemeral_secret;
    };

    const peer_a = Peer.replica(1);
    const peer_b = Peer.replica(2);
    var enc_a = CipherAegis256Nonce128.init(ephemeral_secret, peer_a, peer_b);
    defer enc_a.deinit();

    var enc_b = CipherAegis256Nonce128.init(ephemeral_secret, peer_b, peer_a);
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

test "EncryptionNetwork Unit Test" {
    const gpa = std.testing.allocator;

    var encryption_network = try EncryptionNetwork.init(
        gpa,
        .{
            .replicas_max = 6,
            .clients_max = 4,
            .self_id = 0,
            .self_peer = .replica,
        },
    );
    defer encryption_network.deinit(gpa);
}
