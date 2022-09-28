const std = @import("std");
const math = std.math;
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const log = std.log.scoped(.vsr);

const config = @import("config.zig");

/// The version of our Viewstamped Replication protocol in use, including customizations.
/// For backwards compatibility through breaking changes (e.g. upgrading checksums/ciphers).
pub const Version: u8 = 0;

pub const ReplicaType = @import("vsr/replica.zig").ReplicaType;
pub const format = @import("vsr/replica.zig").format;
pub const Status = @import("vsr/replica.zig").Status;
pub const Client = @import("vsr/client.zig").Client;
pub const Clock = @import("vsr/clock.zig").Clock;
pub const Journal = @import("vsr/journal.zig").Journal;
pub const SlotRange = @import("vsr/journal.zig").SlotRange;
pub const SuperBlockType = @import("vsr/superblock.zig").SuperBlockType;
pub const VSRState = @import("vsr/superblock.zig").SuperBlockSector.VSRState;

pub const ProcessType = enum { replica, client };

pub const Zone = enum {
    superblock,
    wal_headers,
    wal_prepares,
    grid,

    const size_superblock = @import("vsr/superblock.zig").superblock_zone_size;
    const size_wal_headers = config.journal_size_headers;
    const size_wal_prepares = config.journal_size_prepares;

    comptime {
        for (.{
            size_superblock,
            size_wal_headers,
            size_wal_prepares,
        }) |zone_size| {
            assert(zone_size % config.sector_size == 0);
        }
    }

    pub fn offset(zone: Zone, offset_logical: u64) u64 {
        if (zone.size()) |zone_size| {
            assert(offset_logical < zone_size);
        }

        return offset_logical + switch (zone) {
            .superblock => 0,
            .wal_headers => size_superblock,
            .wal_prepares => size_superblock + size_wal_headers,
            .grid => size_superblock + size_wal_headers + size_wal_prepares,
        };
    }

    pub fn size(zone: Zone) ?u64 {
        return switch (zone) {
            .superblock => size_superblock,
            .wal_headers => size_wal_headers,
            .wal_prepares => size_wal_prepares,
            .grid => null,
        };
    }
};

/// Viewstamped Replication protocol commands:
pub const Command = enum(u8) {
    reserved,

    ping,
    pong,

    request,
    prepare,
    prepare_ok,
    reply,
    commit,

    start_view_change,
    do_view_change,
    start_view,

    recovery,
    recovery_response,

    request_start_view,
    request_headers,
    request_prepare,
    headers,
    nack_prepare,

    eviction,

    request_block,
    block,
};

/// This type exists to avoid making the Header type dependant on the state
/// machine used, which would cause awkward circular type dependencies.
pub const Operation = enum(u8) {
    /// Operations reserved by VR protocol (for all state machines):
    /// The value 0 is reserved to prevent a spurious zero from being interpreted as an operation.
    reserved = 0,
    /// The value 1 is reserved to initialize the cluster.
    root = 1,
    /// The value 2 is reserved to register a client session with the cluster.
    register = 2,

    /// Operations exported by the state machine (all other values are free):
    _,

    pub fn from(comptime StateMachine: type, op: StateMachine.Operation) Operation {
        check_state_machine_operations(StateMachine.Operation);
        return @intToEnum(Operation, @enumToInt(op));
    }

    pub fn cast(self: Operation, comptime StateMachine: type) StateMachine.Operation {
        check_state_machine_operations(StateMachine.Operation);
        return @intToEnum(StateMachine.Operation, @enumToInt(self));
    }

    fn check_state_machine_operations(comptime Op: type) void {
        if (!@hasField(Op, "reserved") or std.meta.fieldInfo(Op, .reserved).value != 0) {
            @compileError("StateMachine.Operation must have a 'reserved' field with value 0");
        }
        if (!@hasField(Op, "root") or std.meta.fieldInfo(Op, .root).value != 1) {
            @compileError("StateMachine.Operation must have a 'root' field with value 1");
        }
        if (!@hasField(Op, "register") or std.meta.fieldInfo(Op, .register).value != 2) {
            @compileError("StateMachine.Operation must have a 'register' field with value 2");
        }
    }
};

/// Network message and journal entry header:
/// We reuse the same header for both so that prepare messages from the leader can simply be
/// journalled as is by the followers without requiring any further modification.
pub const Header = extern struct {
    comptime {
        assert(@sizeOf(Header) == 128);
        // Assert that there is no implicit padding in the struct.
        assert(@bitSizeOf(Header) == @sizeOf(Header) * 8);
    }
    /// A checksum covering only the remainder of this header.
    /// This allows the header to be trusted without having to recv() or read() the associated body.
    /// This checksum is enough to uniquely identify a network message or journal entry.
    checksum: u128 = 0,

    /// A checksum covering only the associated body after this header.
    checksum_body: u128 = 0,

    /// A backpointer to the previous request or prepare checksum for hash chain verification.
    /// This provides a cryptographic guarantee for linearizability:
    /// 1. across our distributed log of prepares, and
    /// 2. across a client's requests and our replies.
    /// This may also be used as the initialization vector for AEAD encryption at rest, provided
    /// that the leader ratchets the encryption key every view change to ensure that prepares
    /// reordered through a view change never repeat the same IV for the same encryption key.
    parent: u128 = 0,

    /// Each client process generates a unique, random and ephemeral client ID at initialization.
    /// The client ID identifies connections made by the client to the cluster for the sake of
    /// routing messages back to the client.
    ///
    /// With the client ID in hand, the client then registers a monotonically increasing session
    /// number (committed through the cluster) to allow the client's session to be evicted safely
    /// from the client table if too many concurrent clients cause the client table to overflow.
    /// The monotonically increasing session number prevents duplicate client requests from being
    /// replayed.
    ///
    /// The problem of routing is therefore solved by the 128-bit client ID, and the problem of
    /// detecting whether a session has been evicted is solved by the session number.
    client: u128 = 0,

    /// The checksum of the message to which this message refers, or a unique recovery nonce.
    ///
    /// We use this cryptographic context in various ways, for example:
    ///
    /// * A `request` sets this to the client's session number.
    /// * A `prepare` sets this to the checksum of the client's request.
    /// * A `prepare_ok` sets this to the checksum of the prepare being acked.
    /// * A `commit` sets this to the checksum of the latest committed prepare.
    /// * A `request_prepare` sets this to the checksum of the prepare being requested.
    /// * A `nack_prepare` sets this to the checksum of the prepare being nacked.
    /// * A `recovery` and `recovery_response` sets this to the nonce.
    ///
    /// This allows for cryptographic guarantees beyond request, op, and commit numbers, which have
    /// low entropy and may otherwise collide in the event of any correctness bugs.
    context: u128 = 0,

    /// Each request is given a number by the client and later requests must have larger numbers
    /// than earlier ones. The request number is used by the replicas to avoid running requests more
    /// than once; it is also used by the client to discard duplicate responses to its requests.
    /// A client is allowed to have at most one request inflight at a time.
    request: u32 = 0,

    /// The cluster number binds intention into the header, so that a client or replica can indicate
    /// the cluster it believes it is speaking to, instead of accidentally talking to the wrong
    /// cluster (for example, staging vs production).
    cluster: u32,

    /// The cluster reconfiguration epoch number (for future use).
    epoch: u32 = 0,

    /// Every message sent from one replica to another contains the sending replica's current view.
    /// A `u32` allows for a minimum lifetime of 136 years at a rate of one view change per second.
    view: u32 = 0,

    /// The op number of the latest prepare that may or may not yet be committed. Uncommitted ops
    /// may be replaced by different ops if they do not survive through a view change.
    op: u64 = 0,

    /// The commit number of the latest committed prepare. Committed ops are immutable.
    ///
    /// * A `do_view_change` sets this to `commit_min`, to indicate the sending replica's progress.
    ///   The sending replica may continue to commit after sending the DVC.
    /// * A `start_view` sets this to `commit_max`.
    commit: u64 = 0,

    /// This field is used in various ways:
    ///
    /// * A `prepare` sets this to the leader's state machine `prepare_timestamp`.
    ///   For `create_accounts` and `create_transfers` this is the batch's highest timestamp.
    /// * A `reply` sets this to the corresponding `prepare`'s timestamp.
    ///   This allows the test workload to verify transfer timeouts.
    /// * A `do_view_change` sets this to the latest normal view number.
    /// * A `pong` sets this to the sender's wall clock value.
    /// * A `request_prepare` sets this to `1` when `context` is set to a checksum, and `0`
    ///   otherwise.
    timestamp: u64 = 0,

    /// The size of the Header structure (always), plus any associated body.
    size: u32 = @sizeOf(Header),

    /// The index of the replica in the cluster configuration array that authored this message.
    /// This identifies only the ultimate author because messages may be forwarded amongst replicas.
    replica: u8 = 0,

    /// The Viewstamped Replication protocol command for this message.
    command: Command,

    /// The state machine operation to apply.
    operation: Operation = .reserved,

    /// The version of the protocol implementation that originated this message.
    version: u8 = Version,

    pub fn calculate_checksum(self: *const Header) u128 {
        const checksum_size = @sizeOf(@TypeOf(self.checksum));
        assert(checksum_size == 16);
        const checksum_value = checksum(std.mem.asBytes(self)[checksum_size..]);
        assert(@TypeOf(checksum_value) == @TypeOf(self.checksum));
        return checksum_value;
    }

    pub fn calculate_checksum_body(self: *const Header, body: []const u8) u128 {
        assert(self.size == @sizeOf(Header) + body.len);
        const checksum_value = checksum(body);
        assert(@TypeOf(checksum_value) == @TypeOf(self.checksum_body));
        return checksum_value;
    }

    /// This must be called only after set_checksum_body() so that checksum_body is also covered:
    pub fn set_checksum(self: *Header) void {
        self.checksum = self.calculate_checksum();
    }

    pub fn set_checksum_body(self: *Header, body: []const u8) void {
        self.checksum_body = self.calculate_checksum_body(body);
    }

    pub fn valid_checksum(self: *const Header) bool {
        return self.checksum == self.calculate_checksum();
    }

    pub fn valid_checksum_body(self: *const Header, body: []const u8) bool {
        return self.checksum_body == self.calculate_checksum_body(body);
    }

    /// Returns null if all fields are set correctly according to the command, or else a warning.
    /// This does not verify that checksum is valid, and expects that this has already been done.
    pub fn invalid(self: *const Header) ?[]const u8 {
        if (self.version != Version) return "version != Version";
        if (self.size < @sizeOf(Header)) return "size < @sizeOf(Header)";
        if (self.epoch != 0) return "epoch != 0";
        return switch (self.command) {
            .reserved => self.invalid_reserved(),
            .ping => self.invalid_ping(),
            .pong => self.invalid_pong(),
            .request => self.invalid_request(),
            .prepare => self.invalid_prepare(),
            .prepare_ok => self.invalid_prepare_ok(),
            .reply => self.invalid_reply(),
            .commit => self.invalid_commit(),
            .start_view_change => self.invalid_start_view_change(),
            .do_view_change => self.invalid_do_view_change(),
            .start_view => self.invalid_start_view(),
            .recovery => self.invalid_recovery(),
            .recovery_response => self.invalid_recovery_response(),
            .request_start_view => self.invalid_request_start_view(),
            .request_headers => self.invalid_request_headers(),
            .request_prepare => self.invalid_request_prepare(),
            .request_block => null, // TODO
            .headers => self.invalid_headers(),
            .nack_prepare => self.invalid_nack_prepare(),
            .eviction => self.invalid_eviction(),
            .block => null, // TODO
        };
    }

    fn invalid_reserved(self: *const Header) ?[]const u8 {
        assert(self.command == .reserved);
        if (self.parent != 0) return "parent != 0";
        if (self.client != 0) return "client != 0";
        if (self.context != 0) return "context != 0";
        if (self.request != 0) return "request != 0";
        if (self.view != 0) return "view != 0";
        if (self.commit != 0) return "commit != 0";
        if (self.timestamp != 0) return "timestamp != 0";
        if (self.replica != 0) return "replica != 0";
        if (self.operation != .reserved) return "operation != .reserved";
        return null;
    }

    fn invalid_ping(self: *const Header) ?[]const u8 {
        assert(self.command == .ping);
        if (self.parent != 0) return "parent != 0";
        if (self.context != 0) return "context != 0";
        if (self.request != 0) return "request != 0";
        if (self.commit != 0) return "commit != 0";
        if (self.timestamp != 0) return "timestamp != 0";
        if (self.operation != .reserved) return "operation != .reserved";
        return null;
    }

    fn invalid_pong(self: *const Header) ?[]const u8 {
        assert(self.command == .pong);
        if (self.parent != 0) return "parent != 0";
        if (self.client != 0) return "client != 0";
        if (self.context != 0) return "context != 0";
        if (self.request != 0) return "request != 0";
        if (self.commit != 0) return "commit != 0";
        if (self.operation != .reserved) return "operation != .reserved";
        return null;
    }

    fn invalid_request(self: *const Header) ?[]const u8 {
        assert(self.command == .request);
        if (self.client == 0) return "client == 0";
        if (self.op != 0) return "op != 0";
        if (self.commit != 0) return "commit != 0";
        if (self.timestamp != 0) return "timestamp != 0";
        if (self.replica != 0) return "replica != 0";
        switch (self.operation) {
            .reserved => return "operation == .reserved",
            .root => return "operation == .root",
            .register => {
                // The first request a client makes must be to register with the cluster:
                if (self.parent != 0) return "parent != 0";
                if (self.context != 0) return "context != 0";
                if (self.request != 0) return "request != 0";
                // The .register operation carries no payload:
                if (self.size != @sizeOf(Header)) return "size != @sizeOf(Header)";
            },
            else => {
                // Thereafter, the client must provide the session number in the context:
                // These requests should set `parent` to the `checksum` of the previous reply.
                if (self.context == 0) return "context == 0";
                if (self.request == 0) return "request == 0";
            },
        }
        return null;
    }

    fn invalid_prepare(self: *const Header) ?[]const u8 {
        assert(self.command == .prepare);
        switch (self.operation) {
            .reserved => return "operation == .reserved",
            .root => {
                if (self.parent != 0) return "root: parent != 0";
                if (self.client != 0) return "root: client != 0";
                if (self.context != 0) return "root: context != 0";
                if (self.request != 0) return "root: request != 0";
                if (self.view != 0) return "root: view != 0";
                if (self.op != 0) return "root: op != 0";
                if (self.commit != 0) return "root: commit != 0";
                if (self.timestamp != 0) return "root: timestamp != 0";
                if (self.size != @sizeOf(Header)) return "root: size != @sizeOf(Header)";
                if (self.replica != 0) return "root: replica != 0";
            },
            else => {
                if (self.client == 0) return "client == 0";
                if (self.op == 0) return "op == 0";
                if (self.op <= self.commit) return "op <= commit";
                if (self.timestamp == 0) return "timestamp == 0";
                if (self.operation == .register) {
                    // Client session numbers are replaced by the reference to the previous prepare.
                    if (self.request != 0) return "request != 0";
                } else {
                    // Client session numbers are replaced by the reference to the previous prepare.
                    if (self.request == 0) return "request == 0";
                }
            },
        }
        return null;
    }

    fn invalid_prepare_ok(self: *const Header) ?[]const u8 {
        assert(self.command == .prepare_ok);
        if (self.size != @sizeOf(Header)) return "size != @sizeOf(Header)";
        switch (self.operation) {
            .reserved => return "operation == .reserved",
            .root => {
                if (self.parent != 0) return "root: parent != 0";
                if (self.client != 0) return "root: client != 0";
                if (self.context != 0) return "root: context != 0";
                if (self.request != 0) return "root: request != 0";
                if (self.view != 0) return "root: view != 0";
                if (self.op != 0) return "root: op != 0";
                if (self.commit != 0) return "root: commit != 0";
                if (self.timestamp != 0) return "root: timestamp != 0";
                if (self.replica != 0) return "root: replica != 0";
            },
            else => {
                if (self.client == 0) return "client == 0";
                if (self.op == 0) return "op == 0";
                if (self.op <= self.commit) return "op <= commit";
                if (self.operation == .register) {
                    if (self.request != 0) return "request != 0";
                } else {
                    if (self.request == 0) return "request == 0";
                }
            },
        }
        return null;
    }

    fn invalid_reply(self: *const Header) ?[]const u8 {
        assert(self.command == .reply);
        // Initialization within `client.zig` asserts that client `id` is greater than zero:
        if (self.client == 0) return "client == 0";
        if (self.context != 0) return "context != 0";
        if (self.op != self.commit) return "op != commit";
        if (self.timestamp == 0) return "timestamp == 0";
        if (self.operation == .register) {
            // In this context, the commit number is the newly registered session number.
            // The `0` commit number is reserved for cluster initialization.
            if (self.commit == 0) return "commit == 0";
            if (self.request != 0) return "request != 0";
        } else {
            if (self.commit == 0) return "commit == 0";
            if (self.request == 0) return "request == 0";
        }
        return null;
    }

    fn invalid_commit(self: *const Header) ?[]const u8 {
        assert(self.command == .commit);
        if (self.parent != 0) return "parent != 0";
        if (self.client != 0) return "client != 0";
        if (self.request != 0) return "request != 0";
        if (self.op != 0) return "op != 0";
        if (self.timestamp != 0) return "timestamp != 0";
        if (self.operation != .reserved) return "operation != .reserved";
        return null;
    }

    fn invalid_start_view_change(self: *const Header) ?[]const u8 {
        assert(self.command == .start_view_change);
        if (self.parent != 0) return "parent != 0";
        if (self.client != 0) return "client != 0";
        if (self.context != 0) return "context != 0";
        if (self.request != 0) return "request != 0";
        if (self.op != 0) return "op != 0";
        if (self.commit != 0) return "commit != 0";
        if (self.timestamp != 0) return "timestamp != 0";
        if (self.operation != .reserved) return "operation != .reserved";
        return null;
    }

    fn invalid_do_view_change(self: *const Header) ?[]const u8 {
        assert(self.command == .do_view_change);
        if (self.parent != 0) return "parent != 0";
        if (self.client != 0) return "client != 0";
        if (self.context != 0) return "context != 0";
        if (self.request != 0) return "request != 0";
        if (self.operation != .reserved) return "operation != .reserved";
        return null;
    }

    fn invalid_start_view(self: *const Header) ?[]const u8 {
        assert(self.command == .start_view);
        if (self.parent != 0) return "parent != 0";
        if (self.client != 0) return "client != 0";
        if (self.context != 0) return "context != 0";
        if (self.request != 0) return "request != 0";
        if (self.timestamp != 0) return "timestamp != 0";
        if (self.operation != .reserved) return "operation != .reserved";
        return null;
    }

    fn invalid_recovery(self: *const Header) ?[]const u8 {
        assert(self.command == .recovery);
        if (self.parent != 0) return "parent != 0";
        if (self.client != 0) return "client != 0";
        if (self.request != 0) return "request != 0";
        if (self.view != 0) return "view != 0";
        if (self.op != 0) return "op != 0";
        if (self.commit != 0) return "commit != 0";
        if (self.timestamp != 0) return "timestamp != 0";
        if (self.operation != .reserved) return "operation != .reserved";
        return null;
    }

    fn invalid_recovery_response(self: *const Header) ?[]const u8 {
        assert(self.command == .recovery_response);
        if (self.parent != 0) return "parent != 0";
        if (self.client != 0) return "client != 0";
        if (self.request != 0) return "request != 0";
        if (self.timestamp != 0) return "timestamp != 0";
        if (self.operation != .reserved) return "operation != .reserved";
        return null;
    }

    fn invalid_request_start_view(self: *const Header) ?[]const u8 {
        assert(self.command == .request_start_view);
        if (self.parent != 0) return "parent != 0";
        if (self.client != 0) return "client != 0";
        if (self.context != 0) return "context != 0";
        if (self.request != 0) return "request != 0";
        if (self.op != 0) return "op != 0";
        if (self.commit != 0) return "commit != 0";
        if (self.timestamp != 0) return "timestamp != 0";
        if (self.operation != .reserved) return "operation != .reserved";
        return null;
    }

    fn invalid_request_headers(self: *const Header) ?[]const u8 {
        assert(self.command == .request_headers);
        if (self.parent != 0) return "parent != 0";
        if (self.client != 0) return "client != 0";
        if (self.context != 0) return "context != 0";
        if (self.request != 0) return "request != 0";
        if (self.timestamp != 0) return "timestamp != 0";
        if (self.commit > self.op) return "op_min > op_max";
        if (self.operation != .reserved) return "operation != .reserved";
        return null;
    }

    fn invalid_request_prepare(self: *const Header) ?[]const u8 {
        assert(self.command == .request_prepare);
        if (self.parent != 0) return "parent != 0";
        if (self.client != 0) return "client != 0";
        if (self.request != 0) return "request != 0";
        if (self.commit != 0) return "commit != 0";
        switch (self.timestamp) {
            0 => if (self.context != 0) return "context != 0",
            1 => {}, // context is a checksum, which may be 0.
            else => return "timestamp > 1",
        }
        if (self.operation != .reserved) return "operation != .reserved";
        return null;
    }

    fn invalid_headers(self: *const Header) ?[]const u8 {
        assert(self.command == .headers);
        if (self.parent != 0) return "parent != 0";
        if (self.client != 0) return "client != 0";
        if (self.request != 0) return "request != 0";
        if (self.op != 0) return "op != 0";
        if (self.commit != 0) return "commit != 0";
        if (self.timestamp != 0) return "timestamp != 0";
        if (self.operation != .reserved) return "operation != .reserved";
        return null;
    }

    fn invalid_nack_prepare(self: *const Header) ?[]const u8 {
        assert(self.command == .nack_prepare);
        if (self.parent != 0) return "parent != 0";
        if (self.client != 0) return "client != 0";
        if (self.request != 0) return "request != 0";
        if (self.commit != 0) return "commit != 0";
        if (self.timestamp != 0) return "timestamp != 0";
        if (self.operation != .reserved) return "operation != .reserved";
        return null;
    }

    fn invalid_eviction(self: *const Header) ?[]const u8 {
        assert(self.command == .eviction);
        if (self.parent != 0) return "parent != 0";
        if (self.context != 0) return "context != 0";
        if (self.request != 0) return "request != 0";
        if (self.op != 0) return "op != 0";
        if (self.commit != 0) return "commit != 0";
        if (self.timestamp != 0) return "timestamp != 0";
        if (self.operation != .reserved) return "operation != .reserved";
        return null;
    }

    /// Returns whether the immediate sender is a replica or client (if this can be determined).
    /// Some commands such as .request or .prepare may be forwarded on to other replicas so that
    /// Header.replica or Header.client only identifies the ultimate origin, not the latest peer.
    pub fn peer_type(self: *const Header) enum { unknown, replica, client } {
        switch (self.command) {
            .reserved => unreachable,
            // These messages cannot always identify the peer as they may be forwarded:
            .request => switch (self.operation) {
                // However, we do not forward the first .register request sent by a client:
                .register => return .client,
                else => return .unknown,
            },
            .prepare => return .unknown,
            // These messages identify the peer as either a replica or a client:
            // TODO Assert that pong responses from a replica do not echo the pinging client's ID.
            .ping, .pong => {
                if (self.client > 0) {
                    assert(self.replica == 0);
                    return .client;
                } else {
                    return .replica;
                }
            },
            // All other messages identify the peer as a replica:
            else => return .replica,
        }
    }

    pub fn reserved(cluster: u32, slot: u64) Header {
        assert(slot < config.journal_slot_count);

        var header = Header{
            .command = .reserved,
            .cluster = cluster,
            .op = slot,
        };
        header.set_checksum_body(&[0]u8{});
        header.set_checksum();
        assert(header.invalid() == null);
        return header;
    }

    pub fn root_prepare(cluster: u32) Header {
        var header = Header{
            .cluster = cluster,
            .size = @sizeOf(Header),
            .command = .prepare,
            .operation = .root,
        };
        header.set_checksum_body(&[0]u8{});
        header.set_checksum();
        assert(header.invalid() == null);
        return header;
    }
};

pub const Timeout = struct {
    name: []const u8,
    id: u128,
    after: u64,
    attempts: u8 = 0,
    rtt: u64 = config.rtt_ticks,
    rtt_multiple: u8 = config.rtt_multiple,
    ticks: u64 = 0,
    ticking: bool = false,

    /// Increments the attempts counter and resets the timeout with exponential backoff and jitter.
    /// Allows the attempts counter to wrap from time to time.
    /// The overflow period is kept short to surface any related bugs sooner rather than later.
    /// We do not saturate the counter as this would cause round-robin retries to get stuck.
    pub fn backoff(self: *Timeout, random: std.rand.Random) void {
        assert(self.ticking);

        self.ticks = 0;
        self.attempts +%= 1;

        log.debug("{}: {s} backing off", .{ self.id, self.name });
        self.set_after_for_rtt_and_attempts(random);
    }

    /// It's important to check that when fired() is acted on that the timeout is stopped/started,
    /// otherwise further ticks around the event loop may trigger a thundering herd of messages.
    pub fn fired(self: *Timeout) bool {
        if (self.ticking and self.ticks >= self.after) {
            log.debug("{}: {s} fired", .{ self.id, self.name });
            if (self.ticks > self.after) {
                log.err("{}: {s} is firing every tick", .{ self.id, self.name });
                @panic("timeout was not reset correctly");
            }
            return true;
        } else {
            return false;
        }
    }

    pub fn reset(self: *Timeout) void {
        self.attempts = 0;
        self.ticks = 0;
        assert(self.ticking);
        // TODO Use self.prng to adjust for rtt and attempts.
        log.debug("{}: {s} reset", .{ self.id, self.name });
    }

    /// Sets the value of `after` as a function of `rtt` and `attempts`.
    /// Adds exponential backoff and jitter.
    /// May be called only after a timeout has been stopped or reset, to prevent backward jumps.
    pub fn set_after_for_rtt_and_attempts(self: *Timeout, random: std.rand.Random) void {
        // If `after` is reduced by this function to less than `ticks`, then `fired()` will panic:
        assert(self.ticks == 0);
        assert(self.rtt > 0);

        const after = (self.rtt * self.rtt_multiple) + exponential_backoff_with_jitter(
            random,
            config.backoff_min_ticks,
            config.backoff_max_ticks,
            self.attempts,
        );

        // TODO Clamp `after` to min/max tick bounds for timeout.

        log.debug("{}: {s} after={}..{} (rtt={} min={} max={} attempts={})", .{
            self.id,
            self.name,
            self.after,
            after,
            self.rtt,
            config.backoff_min_ticks,
            config.backoff_max_ticks,
            self.attempts,
        });

        self.after = after;
        assert(self.after > 0);
    }

    pub fn set_rtt(self: *Timeout, rtt_ticks: u64) void {
        assert(self.rtt > 0);
        assert(rtt_ticks > 0);

        log.debug("{}: {s} rtt={}..{}", .{
            self.id,
            self.name,
            self.rtt,
            rtt_ticks,
        });

        self.rtt = rtt_ticks;
    }

    pub fn start(self: *Timeout) void {
        self.attempts = 0;
        self.ticks = 0;
        self.ticking = true;
        // TODO Use self.prng to adjust for rtt and attempts.
        log.debug("{}: {s} started", .{ self.id, self.name });
    }

    pub fn stop(self: *Timeout) void {
        self.attempts = 0;
        self.ticks = 0;
        self.ticking = false;
        log.debug("{}: {s} stopped", .{ self.id, self.name });
    }

    pub fn tick(self: *Timeout) void {
        if (self.ticking) self.ticks += 1;
    }
};

/// Calculates exponential backoff with jitter to prevent cascading failure due to thundering herds.
pub fn exponential_backoff_with_jitter(
    random: std.rand.Random,
    min: u64,
    max: u64,
    attempt: u64,
) u64 {
    const range = max - min;
    assert(range > 0);

    // Do not use `@truncate(u6, attempt)` since that only discards the high bits:
    // We want a saturating exponent here instead.
    const exponent = @intCast(u6, std.math.min(std.math.maxInt(u6), attempt));

    // A "1" shifted left gives any power of two:
    // 1<<0 = 1, 1<<1 = 2, 1<<2 = 4, 1<<3 = 8
    const power = std.math.shlExact(u128, 1, exponent) catch unreachable; // Do not truncate.

    // Ensure that `backoff` is calculated correctly when min is 0, taking `std.math.max(1, min)`.
    // Otherwise, the final result will always be 0. This was an actual bug we encountered.
    const min_non_zero = std.math.max(1, min);
    assert(min_non_zero > 0);
    assert(power > 0);

    // Calculate the capped exponential backoff component, `min(range, min * 2 ^ attempt)`:
    const backoff = std.math.min(range, min_non_zero * power);
    const jitter = random.uintAtMostBiased(u64, backoff);

    const result = @intCast(u64, min + jitter);
    assert(result >= min);
    assert(result <= max);

    return result;
}

test "exponential_backoff_with_jitter" {
    const testing = std.testing;

    var prng = std.rand.DefaultPrng.init(0);
    const random = prng.random();

    const attempts = 1000;
    const max: u64 = std.math.maxInt(u64);
    const min = max - attempts;

    var attempt = max - attempts;
    while (attempt < max) : (attempt += 1) {
        const ebwj = exponential_backoff_with_jitter(random, min, max, attempt);
        try testing.expect(ebwj >= min);
        try testing.expect(ebwj <= max);
    }
}

/// Returns An array containing the remote or local addresses of each of the 2f + 1 replicas:
/// Unlike the VRR paper, we do not sort the array but leave the order explicitly to the user.
/// There are several advantages to this:
/// * The operator may deploy a cluster with proximity in mind since replication follows order.
/// * A replica's IP address may be changed without reconfiguration.
/// This does require that the user specify the same order to all replicas.
/// The caller owns the memory of the returned slice of addresses.
pub fn parse_addresses(allocator: std.mem.Allocator, raw: []const u8, address_limit: usize) ![]std.net.Address {
    // TODO After parsing addresses resize the memory allocation.
    var addresses = try allocator.alloc(std.net.Address, address_limit);
    errdefer allocator.free(addresses);

    var index: usize = 0;
    var comma_iterator = std.mem.split(u8, raw, ",");
    while (comma_iterator.next()) |raw_address| : (index += 1) {
        if (raw_address.len == 0) return error.AddressHasTrailingComma;
        if (index == address_limit) return error.AddressLimitExceeded;
        addresses[index] = try parse_address(raw_address);
    }
    return addresses[0..index];
}

pub fn parse_address(raw: []const u8) !std.net.Address {
    var colon_iterator = std.mem.split(u8, raw, ":");
    // The split iterator will always return non-null once, even if the delimiter is not found:
    const raw_ipv4 = colon_iterator.next().?;

    if (colon_iterator.next()) |raw_port| {
        if (colon_iterator.next() != null) return error.AddressHasMoreThanOneColon;

        const port = std.fmt.parseUnsigned(u16, raw_port, 10) catch |err| switch (err) {
            error.Overflow => return error.PortOverflow,
            error.InvalidCharacter => return error.PortInvalid,
        };
        return std.net.Address.parseIp4(raw_ipv4, port) catch {
            return error.AddressInvalid;
        };
    } else {
        // There was no colon in the address so there are now two cases:
        // 1. an IPv4 address with the default port, or
        // 2. a port with the default IPv4 address.

        // Let's try parsing as a port first:
        if (std.fmt.parseUnsigned(u16, raw, 10)) |port| {
            return std.net.Address.parseIp4(config.address, port) catch unreachable;
        } else |err| switch (err) {
            error.Overflow => return error.PortOverflow,
            error.InvalidCharacter => {
                // Something was not a digit, let's try parsing as an IPv4 instead:
                return std.net.Address.parseIp4(raw, config.port) catch {
                    return error.AddressInvalid;
                };
            },
        }
    }
}

test "parse_addresses" {
    const vectors_positive = &[_]struct {
        raw: []const u8,
        addresses: [3]std.net.Address,
    }{
        .{
            // Test the minimum/maximum address/port.
            .raw = "1.2.3.4:567,0.0.0.0:0,255.255.255.255:65535",
            .addresses = [3]std.net.Address{
                std.net.Address.initIp4([_]u8{ 1, 2, 3, 4 }, 567),
                std.net.Address.initIp4([_]u8{ 0, 0, 0, 0 }, 0),
                std.net.Address.initIp4([_]u8{ 255, 255, 255, 255 }, 65535),
            },
        },
        .{
            // Addresses are not reordered.
            .raw = "3.4.5.6:7777,200.3.4.5:6666,1.2.3.4:5555",
            .addresses = [3]std.net.Address{
                std.net.Address.initIp4([_]u8{ 3, 4, 5, 6 }, 7777),
                std.net.Address.initIp4([_]u8{ 200, 3, 4, 5 }, 6666),
                std.net.Address.initIp4([_]u8{ 1, 2, 3, 4 }, 5555),
            },
        },
        .{
            // Test default address and port.
            .raw = "1.2.3.4:5,4321,2.3.4.5",
            .addresses = [3]std.net.Address{
                std.net.Address.initIp4([_]u8{ 1, 2, 3, 4 }, 5),
                try std.net.Address.parseIp4(config.address, 4321),
                std.net.Address.initIp4([_]u8{ 2, 3, 4, 5 }, config.port),
            },
        },
    };

    const vectors_negative = &[_]struct {
        raw: []const u8,
        err: anyerror![]std.net.Address,
    }{
        .{ .raw = "", .err = error.AddressHasTrailingComma },
        .{ .raw = "1.2.3.4:5,2.3.4.5:6,4.5.6.7:8", .err = error.AddressLimitExceeded },
        .{ .raw = "1.2.3.4:7777,2.3.4.5:8888,", .err = error.AddressHasTrailingComma },
        .{ .raw = "1.2.3.4:7777,2.3.4.5::8888", .err = error.AddressHasMoreThanOneColon },
        .{ .raw = "1.2.3.4:5,A", .err = error.AddressInvalid }, // default port
        .{ .raw = "1.2.3.4:5,2.a.4.5", .err = error.AddressInvalid }, // default port
        .{ .raw = "1.2.3.4:5,2.a.4.5:6", .err = error.AddressInvalid }, // specified port
        .{ .raw = "1.2.3.4:5,2.3.4.5:", .err = error.PortInvalid },
        .{ .raw = "1.2.3.4:5,2.3.4.5:A", .err = error.PortInvalid },
        .{ .raw = "1.2.3.4:5,65536", .err = error.PortOverflow }, // default address
        .{ .raw = "1.2.3.4:5,2.3.4.5:65536", .err = error.PortOverflow },
    };

    for (vectors_positive) |vector| {
        const addresses_actual = try parse_addresses(std.testing.allocator, vector.raw, 3);
        defer std.testing.allocator.free(addresses_actual);

        try std.testing.expectEqual(addresses_actual.len, 3);
        for (vector.addresses) |address_expect, i| {
            const address_actual = addresses_actual[i];
            try std.testing.expectEqual(address_expect.in.sa.family, address_actual.in.sa.family);
            try std.testing.expectEqual(address_expect.in.sa.port, address_actual.in.sa.port);
            try std.testing.expectEqual(address_expect.in.sa.addr, address_actual.in.sa.addr);
            try std.testing.expectEqual(address_expect.in.sa.zero, address_actual.in.sa.zero);
        }
    }

    for (vectors_negative) |vector| {
        try std.testing.expectEqual(vector.err, parse_addresses(std.testing.allocator, vector.raw, 2));
    }
}

pub fn sector_floor(offset: u64) u64 {
    const sectors = math.divFloor(u64, offset, config.sector_size) catch unreachable;
    return sectors * config.sector_size;
}

pub fn sector_ceil(offset: u64) u64 {
    const sectors = math.divCeil(u64, offset, config.sector_size) catch unreachable;
    return sectors * config.sector_size;
}

pub fn checksum(source: []const u8) u128 {
    var target: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(source, target[0..], .{});
    return @bitCast(u128, target[0..@sizeOf(u128)].*);
}
