const std = @import("std");
const math = std.math;
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const log = std.log.scoped(.vsr);

// vsr.zig is the root of a zig package, reexport all public APIs.
//
// Note that we don't promise any stability of these interfaces yet.
pub const constants = @import("constants.zig");
pub const io = @import("io.zig");
pub const message_bus = @import("message_bus.zig");
pub const message_pool = @import("message_pool.zig");
pub const state_machine = @import("state_machine.zig");
pub const storage = @import("storage.zig");
pub const tigerbeetle = @import("tigerbeetle.zig");
pub const time = @import("time.zig");
pub const tracer = @import("tracer.zig");
pub const config = @import("config.zig");
pub const stdx = @import("stdx.zig");
pub const superblock = @import("vsr/superblock.zig");
pub const lsm = .{
    .tree = @import("lsm/tree.zig"),
    .grid = @import("lsm/grid.zig"),
    .groove = @import("lsm/groove.zig"),
    .forest = @import("lsm/forest.zig"),
    .posted_groove = @import("lsm/posted_groove.zig"),
};
pub const testing = .{
    .cluster = @import("testing/cluster.zig"),
};

pub const ReplicaType = @import("vsr/replica.zig").ReplicaType;
pub const format = @import("vsr/replica_format.zig").format;
pub const Status = @import("vsr/replica.zig").Status;
pub const Client = @import("vsr/client.zig").Client;
pub const Clock = @import("vsr/clock.zig").Clock;
pub const JournalType = @import("vsr/journal.zig").JournalType;
pub const SlotRange = @import("vsr/journal.zig").SlotRange;
pub const SuperBlockType = superblock.SuperBlockType;
pub const VSRState = superblock.SuperBlockHeader.VSRState;

/// The version of our Viewstamped Replication protocol in use, including customizations.
/// For backwards compatibility through breaking changes (e.g. upgrading checksums/ciphers).
pub const Version: u8 = 0;

pub const ProcessType = enum { replica, client };

pub const Zone = enum {
    superblock,
    wal_headers,
    wal_prepares,
    grid,

    const size_superblock = superblock.superblock_zone_size;
    const size_wal_headers = constants.journal_size_headers;
    const size_wal_prepares = constants.journal_size_prepares;

    comptime {
        for (.{
            size_superblock,
            size_wal_headers,
            size_wal_prepares,
        }) |zone_size| {
            assert(zone_size % constants.sector_size == 0);
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

    ping_client,
    pong_client,

    request,
    prepare,
    prepare_ok,
    reply,
    commit,

    start_view_change,
    do_view_change,
    start_view,

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
/// We reuse the same header for both so that prepare messages from the primary can simply be
/// journalled as is by the backups without requiring any further modification.
pub const Header = extern struct {
    const checksum_body_empty = checksum(&.{});

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
    /// that the primary ratchets the encryption key every view change to ensure that prepares
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
    ///
    /// * A `request_headers` sets this to the maximum op requested (inclusive).
    op: u64 = 0,

    /// The commit number of the latest committed prepare. Committed ops are immutable.
    ///
    /// * A `do_view_change` sets this to `commit_min`, to indicate the sending replica's progress.
    ///   The sending replica may continue to commit after sending the DVC.
    /// * A `start_view` sets this to `commit_max`.
    /// * A `request_headers` sets this to the minimum op requested (inclusive).
    commit: u64 = 0,

    /// This field is used in various ways:
    ///
    /// * A `prepare` sets this to the primary's state machine `prepare_timestamp`.
    ///   For `create_accounts` and `create_transfers` this is the batch's highest timestamp.
    /// * A `reply` sets this to the corresponding `prepare`'s timestamp.
    ///   This allows the test workload to verify transfer timeouts.
    /// * A `do_view_change` sets this to the latest normal view number.
    /// * A `pong` sets this to the sender's wall clock value.
    /// * A `commit` message sets this to the replica's monotonic timestamp.
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
        const checksum_size = @sizeOf(@TypeOf(self.checksum_body));
        assert(checksum_size == 16);
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
            .ping_client => self.invalid_ping_client(),
            .pong_client => self.invalid_pong_client(),
            .request => self.invalid_request(),
            .prepare => self.invalid_prepare(),
            .prepare_ok => self.invalid_prepare_ok(),
            .reply => self.invalid_reply(),
            .commit => self.invalid_commit(),
            .start_view_change => self.invalid_start_view_change(),
            .do_view_change => self.invalid_do_view_change(),
            .start_view => self.invalid_start_view(),
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
        if (self.client != 0) return "client != 0";
        if (self.context != 0) return "context != 0";
        if (self.request != 0) return "request != 0";
        if (self.view != 0) return "view != 0";
        if (self.commit != 0) return "commit != 0";
        if (self.timestamp != 0) return "timestamp != 0";
        if (self.checksum_body != checksum_body_empty) return "checksum_body != expected";
        if (self.size != @sizeOf(Header)) return "size != @sizeOf(Header)";
        if (self.operation != .reserved) return "operation != .reserved";
        return null;
    }

    fn invalid_pong(self: *const Header) ?[]const u8 {
        assert(self.command == .pong);
        if (self.parent != 0) return "parent != 0";
        if (self.client != 0) return "client != 0";
        if (self.context != 0) return "context != 0";
        if (self.request != 0) return "request != 0";
        if (self.view != 0) return "view != 0";
        if (self.commit != 0) return "commit != 0";
        if (self.timestamp == 0) return "timestamp == 0";
        if (self.checksum_body != checksum_body_empty) return "checksum_body != expected";
        if (self.size != @sizeOf(Header)) return "size != @sizeOf(Header)";
        if (self.operation != .reserved) return "operation != .reserved";
        return null;
    }

    fn invalid_ping_client(self: *const Header) ?[]const u8 {
        assert(self.command == .ping_client);
        if (self.parent != 0) return "parent != 0";
        if (self.client == 0) return "client == 0";
        if (self.context != 0) return "context != 0";
        if (self.request != 0) return "request != 0";
        if (self.view != 0) return "view != 0";
        if (self.op != 0) return "op != 0";
        if (self.commit != 0) return "commit != 0";
        if (self.timestamp != 0) return "timestamp != 0";
        if (self.checksum_body != checksum_body_empty) return "checksum_body != expected";
        if (self.size != @sizeOf(Header)) return "size != @sizeOf(Header)";
        if (self.replica != 0) return "replica != 0";
        if (self.operation != .reserved) return "operation != .reserved";
        return null;
    }

    fn invalid_pong_client(self: *const Header) ?[]const u8 {
        assert(self.command == .pong_client);
        if (self.parent != 0) return "parent != 0";
        if (self.client != 0) return "client != 0";
        if (self.context != 0) return "context != 0";
        if (self.request != 0) return "request != 0";
        if (self.op != 0) return "op != 0";
        if (self.commit != 0) return "commit != 0";
        if (self.timestamp != 0) return "timestamp != 0";
        if (self.checksum_body != checksum_body_empty) return "checksum_body != expected";
        if (self.size != @sizeOf(Header)) return "size != @sizeOf(Header)";
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
                if (self.parent != 0) return "register: parent != 0";
                if (self.context != 0) return "register: context != 0";
                if (self.request != 0) return "register: request != 0";
                // The .register operation carries no payload:
                if (self.checksum_body != checksum_body_empty) return "register: checksum_body != expected";
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
                if (self.checksum_body != checksum_body_empty) return "root: checksum_body != expected";
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
        if (self.checksum_body != checksum_body_empty) return "checksum_body != expected";
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
                if (self.timestamp == 0) return "timestamp == 0";
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
        if (self.timestamp == 0) return "timestamp == 0";
        if (self.checksum_body != checksum_body_empty) return "checksum_body != expected";
        if (self.size != @sizeOf(Header)) return "size != @sizeOf(Header)";
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
        if (self.checksum_body != checksum_body_empty) return "checksum_body != expected";
        if (self.size != @sizeOf(Header)) return "size != @sizeOf(Header)";
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

    fn invalid_request_start_view(self: *const Header) ?[]const u8 {
        assert(self.command == .request_start_view);
        if (self.parent != 0) return "parent != 0";
        if (self.client != 0) return "client != 0";
        if (self.context != 0) return "context != 0";
        if (self.request != 0) return "request != 0";
        if (self.op != 0) return "op != 0";
        if (self.commit != 0) return "commit != 0";
        if (self.timestamp != 0) return "timestamp != 0";
        if (self.checksum_body != checksum_body_empty) return "checksum_body != expected";
        if (self.size != @sizeOf(Header)) return "size != @sizeOf(Header)";
        if (self.operation != .reserved) return "operation != .reserved";
        return null;
    }

    fn invalid_request_headers(self: *const Header) ?[]const u8 {
        assert(self.command == .request_headers);
        if (self.parent != 0) return "parent != 0";
        if (self.client != 0) return "client != 0";
        if (self.context != 0) return "context != 0";
        if (self.request != 0) return "request != 0";
        if (self.commit > self.op) return "op_min > op_max";
        if (self.timestamp != 0) return "timestamp != 0";
        if (self.checksum_body != checksum_body_empty) return "checksum_body != expected";
        if (self.size != @sizeOf(Header)) return "size != @sizeOf(Header)";
        if (self.operation != .reserved) return "operation != .reserved";
        return null;
    }

    fn invalid_request_prepare(self: *const Header) ?[]const u8 {
        assert(self.command == .request_prepare);
        if (self.parent != 0) return "parent != 0";
        if (self.client != 0) return "client != 0";
        if (self.request != 0) return "request != 0";
        if (self.commit != 0) return "commit != 0";
        if (self.timestamp != 0) return "timestamp != 0";
        if (self.checksum_body != checksum_body_empty) return "checksum_body != expected";
        if (self.size != @sizeOf(Header)) return "size != @sizeOf(Header)";
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
        if (self.checksum_body != checksum_body_empty) return "checksum_body != expected";
        if (self.size != @sizeOf(Header)) return "size != @sizeOf(Header)";
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
        if (self.checksum_body != checksum_body_empty) return "checksum_body != expected";
        if (self.size != @sizeOf(Header)) return "size != @sizeOf(Header)";
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
            .ping_client => return .client,
            // All other messages identify the peer as a replica:
            else => return .replica,
        }
    }

    pub fn reserved(cluster: u32, slot: u64) Header {
        assert(slot < constants.journal_slot_count);

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
    rtt: u64 = constants.rtt_ticks,
    rtt_multiple: u8 = constants.rtt_multiple,
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
    pub fn fired(self: *const Timeout) bool {
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
            constants.backoff_min_ticks,
            constants.backoff_max_ticks,
            self.attempts,
        );

        // TODO Clamp `after` to min/max tick bounds for timeout.

        log.debug("{}: {s} after={}..{} (rtt={} min={} max={} attempts={})", .{
            self.id,
            self.name,
            self.after,
            after,
            self.rtt,
            constants.backoff_min_ticks,
            constants.backoff_max_ticks,
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
    var prng = std.rand.DefaultPrng.init(0);
    const random = prng.random();

    const attempts = 1000;
    const max: u64 = std.math.maxInt(u64);
    const min = max - attempts;

    var attempt = max - attempts;
    while (attempt < max) : (attempt += 1) {
        const ebwj = exponential_backoff_with_jitter(random, min, max, attempt);
        try std.testing.expect(ebwj >= min);
        try std.testing.expect(ebwj <= max);
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
    const address_count = std.mem.count(u8, raw, ",") + 1;
    if (address_count > address_limit) return error.AddressLimitExceeded;

    const addresses = try allocator.alloc(std.net.Address, address_count);
    errdefer allocator.free(addresses);

    var index: usize = 0;
    var comma_iterator = std.mem.split(u8, raw, ",");
    while (comma_iterator.next()) |raw_address| : (index += 1) {
        assert(index < address_limit);
        if (raw_address.len == 0) return error.AddressHasTrailingComma;
        addresses[index] = try parse_address(raw_address);
    }
    assert(index == address_count);

    return addresses;
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
            return std.net.Address.parseIp4(constants.address, port) catch unreachable;
        } else |err| switch (err) {
            error.Overflow => return error.PortOverflow,
            error.InvalidCharacter => {
                // Something was not a digit, let's try parsing as an IPv4 instead:
                return std.net.Address.parseIp4(raw, constants.port) catch {
                    return error.AddressInvalid;
                };
            },
        }
    }
}

test "parse_addresses" {
    const vectors_positive = &[_]struct {
        raw: []const u8,
        addresses: []const std.net.Address,
    }{
        .{
            // Test the minimum/maximum address/port.
            .raw = "1.2.3.4:567,0.0.0.0:0,255.255.255.255:65535",
            .addresses = &[3]std.net.Address{
                std.net.Address.initIp4([_]u8{ 1, 2, 3, 4 }, 567),
                std.net.Address.initIp4([_]u8{ 0, 0, 0, 0 }, 0),
                std.net.Address.initIp4([_]u8{ 255, 255, 255, 255 }, 65535),
            },
        },
        .{
            // Addresses are not reordered.
            .raw = "3.4.5.6:7777,200.3.4.5:6666,1.2.3.4:5555",
            .addresses = &[3]std.net.Address{
                std.net.Address.initIp4([_]u8{ 3, 4, 5, 6 }, 7777),
                std.net.Address.initIp4([_]u8{ 200, 3, 4, 5 }, 6666),
                std.net.Address.initIp4([_]u8{ 1, 2, 3, 4 }, 5555),
            },
        },
        .{
            // Test default address and port.
            .raw = "1.2.3.4:5,4321,2.3.4.5",
            .addresses = &[3]std.net.Address{
                std.net.Address.initIp4([_]u8{ 1, 2, 3, 4 }, 5),
                try std.net.Address.parseIp4(constants.address, 4321),
                std.net.Address.initIp4([_]u8{ 2, 3, 4, 5 }, constants.port),
            },
        },
        .{
            // Test addresses less than address_limit.
            .raw = "1.2.3.4:5,4321",
            .addresses = &[2]std.net.Address{
                std.net.Address.initIp4([_]u8{ 1, 2, 3, 4 }, 5),
                try std.net.Address.parseIp4(constants.address, 4321),
            },
        },
    };

    const vectors_negative = &[_]struct {
        raw: []const u8,
        err: anyerror![]std.net.Address,
    }{
        .{ .raw = "", .err = error.AddressHasTrailingComma },
        .{ .raw = "1.2.3.4:5,2.3.4.5:6,4.5.6.7:8", .err = error.AddressLimitExceeded },
        .{ .raw = "1.2.3.4:7777,", .err = error.AddressHasTrailingComma },
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

        try std.testing.expectEqual(addresses_actual.len, vector.addresses.len);
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
    const sectors = math.divFloor(u64, offset, constants.sector_size) catch unreachable;
    return sectors * constants.sector_size;
}

pub fn sector_ceil(offset: u64) u64 {
    const sectors = math.divCeil(u64, offset, constants.sector_size) catch unreachable;
    return sectors * constants.sector_size;
}

pub fn checksum(source: []const u8) u128 {
    @setEvalBranchQuota(4000);

    var target: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(source, target[0..], .{});
    return @bitCast(u128, target[0..@sizeOf(u128)].*);
}

pub fn quorums(replica_count: u8) struct {
    replication: u8,
    view_change: u8,
} {
    assert(replica_count > 0);

    assert(constants.quorum_replication_max >= 2);
    // For replica_count=2, set quorum_replication=2 even though =1 would intersect.
    // This improves durability of small clusters.
    const quorum_replication = if (replica_count == 2) 2 else std.math.min(
        constants.quorum_replication_max,
        stdx.div_ceil(replica_count, 2),
    );
    assert(quorum_replication <= replica_count);
    assert(quorum_replication >= 2 or quorum_replication == replica_count);

    // For replica_count=2, set quorum_view_change=2 even though =1 would intersect.
    // This avoids special cases for a single-replica view-change in Replica.
    const quorum_view_change =
        if (replica_count == 2) 2 else replica_count - quorum_replication + 1;
    // The view change quorum may be more expensive to make the replication quorum cheaper.
    // The insight is that the replication phase is by far more common than the view change.
    // This trade-off allows us to optimize for the common case.
    // See the comments in `constants.zig` for further explanation.
    assert(quorum_view_change <= replica_count);
    assert(quorum_view_change >= 2 or quorum_view_change == replica_count);
    assert(quorum_view_change >= @divFloor(replica_count, 2) + 1);
    assert(quorum_view_change + quorum_replication > replica_count);

    return .{
        .replication = quorum_replication,
        .view_change = quorum_view_change,
    };
}

test "quorums" {
    if (constants.quorum_replication_max != 3) return error.SkipZigTest;

    const expect_replication = [_]u8{ 1, 2, 2, 2, 3, 3, 3, 3 };
    const expect_view_change = [_]u8{ 1, 2, 2, 3, 3, 4, 5, 6 };

    for (expect_replication[0..]) |_, i| {
        const actual = quorums(@intCast(u8, i) + 1);
        try std.testing.expectEqual(actual.replication, expect_replication[i]);
        try std.testing.expectEqual(actual.view_change, expect_view_change[i]);
    }
}

pub const Headers = struct {
    pub const Array = std.BoundedArray(Header, constants.view_change_headers_max);
    /// The SuperBlock's persisted VSR headers.
    /// One of the following:
    ///
    /// - SV headers (consecutive chain)
    /// - DVC headers (disjoint chain)
    pub const ViewChangeSlice = ViewChangeHeadersSlice;
    pub const ViewChangeArray = ViewChangeHeadersArray;
};

const ViewChangeHeadersSlice = struct {
    /// Headers are ordered from high-to-low op.
    slice: []const Header,

    pub fn init(slice: []const Header) ViewChangeHeadersSlice {
        ViewChangeHeadersSlice.verify(slice);

        return .{ .slice = slice };
    }

    pub fn verify(slice: []const Header) void {
        assert(slice.len > 0);
        assert(slice.len <= constants.view_change_headers_max);

        var child: ?*const Header = null;
        for (slice) |*header| {
            assert(header.valid_checksum());
            assert(header.command == .prepare);

            if (child) |child_header| {
                assert(header.op < child_header.op);
                assert(header.view <= child_header.view);
                assert((header.op + 1 == child_header.op) ==
                    (header.checksum == child_header.parent));
                assert(header.timestamp < child_header.timestamp);
            }
            child = header;
        }
    }

    const ViewRange = struct {
        min: u32, // inclusive
        max: u32, // inclusive

        pub fn contains(range: ViewRange, view: u32) bool {
            return range.min <= view and view <= range.max;
        }
    };

    /// Returns the range of possible views (of prepare, not commit) for a message that is part of
    /// the same log_view as these headers.
    ///
    /// - When these are DVC headers for a log_view=V, we must be in view_change status working to
    ///   transition to a view beyond V. So we will never prepare anything else as part of view V.
    /// - When these are SV headers for a log_view=V, we can continue to add to them (by preparing
    ///   more ops), but those ops will laways be part of the log_view. If they were prepared during
    ///   a view prior to the log_view, they would already be part of the headers.
    pub fn view_for_op(headers: ViewChangeHeadersSlice, op: u64, log_view: u32) ViewRange {
        const header_newest = &headers.slice[0];
        const header_oldest = &headers.slice[headers.slice.len - 1];

        if (op < header_oldest.op) return .{ .min = 0, .max = header_oldest.view };
        if (op > header_newest.op) return .{ .min = log_view, .max = log_view };

        for (headers.slice) |*header| {
            if (header.op == op) return .{ .min = header.view, .max = header.view };
        }

        for (headers.slice[0 .. headers.slice.len - 1]) |*header_next, header_next_index| {
            const header_prev = headers.slice[header_next_index + 1];
            if (header_prev.op < op and op < header_next.op) {
                return .{ .min = header_prev.view, .max = header_next.view };
            }
        }
        unreachable;
    }
};

test "Headers.ViewChangeSlice.view_for_op" {
    var headers_array = [_]Header{
        std.mem.zeroInit(Header, .{ .op = 9, .view = 10 }),
        std.mem.zeroInit(Header, .{ .op = 6, .view = 7 }),
    };

    const headers = Headers.ViewChangeSlice{ .slice = &headers_array };
    try std.testing.expect(std.meta.eql(headers.view_for_op(11, 12), .{ .min = 12, .max = 12 }));
    try std.testing.expect(std.meta.eql(headers.view_for_op(10, 12), .{ .min = 12, .max = 12 }));
    try std.testing.expect(std.meta.eql(headers.view_for_op(9, 12), .{ .min = 10, .max = 10 }));
    try std.testing.expect(std.meta.eql(headers.view_for_op(8, 12), .{ .min = 7, .max = 10 }));
    try std.testing.expect(std.meta.eql(headers.view_for_op(7, 12), .{ .min = 7, .max = 10 }));
    try std.testing.expect(std.meta.eql(headers.view_for_op(6, 12), .{ .min = 7, .max = 7 }));
    try std.testing.expect(std.meta.eql(headers.view_for_op(5, 12), .{ .min = 0, .max = 7 }));
    try std.testing.expect(std.meta.eql(headers.view_for_op(0, 12), .{ .min = 0, .max = 7 }));
}

/// The headers of a SV or DVC message.
const ViewChangeHeadersArray = struct {
    array: Headers.Array,

    pub fn root(cluster: u32) ViewChangeHeadersArray {
        var array = Headers.Array{ .buffer = undefined };
        array.appendAssumeCapacity(Header.root_prepare(cluster));
        return ViewChangeHeadersArray.init(array);
    }

    pub fn from_slice(slice: []const Header) ViewChangeHeadersArray {
        Headers.ViewChangeSlice.verify(slice);
        return .{ .array = Headers.Array.fromSlice(slice) catch unreachable };
    }

    fn init(array: Headers.Array) ViewChangeHeadersArray {
        Headers.ViewChangeSlice.verify(array.constSlice());
        return .{ .array = array };
    }
};
