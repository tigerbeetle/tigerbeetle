const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const log = std.log.scoped(.vr);
pub const log_level: std.log.Level = .debug;
/// Viewstamped Replication protocol commands:
// TODO Command for client to fetch its latest request_number from the cluster.
pub const Command = packed enum(u8) {
    reserved,

    request,
    prepare,
    prepare_ok,
    reply,
    commit,

    start_view_change,
    do_view_change,
    start_view,
};

/// State machine operations:
pub const Operation = packed enum(u8) {
    reserved,

    noop,
};

/// Network message and Journal entry header:
/// We reuse the same header for both so that prepare messages from the leader can simply be
/// journalled as is by the followers without requiring any further modification.
pub const Header = packed struct {
    /// A checksum covering only the rest of this header (but including checksum_data):
    /// This enables the header to be trusted without having to recv() or read() associated data.
    /// This checksum is enough to uniquely identify a network message or journal entry.
    checksum_meta: u128 = 0,

    /// A checksum covering only associated data.
    checksum_data: u128 = 0,

    /// The checksum_meta of the message to which this message refers, or a unique recovery nonce:
    /// We use this nonce in various ways, for example:
    /// * A prepare sets nonce to the checksum_meta of the prior prepare to hash-chain the journal.
    /// * A prepare_ok sets nonce to the checksum_meta of the prepare it wants to ack.
    /// * A commit sets nonce to the checksum_meta of the latest committed op.
    /// This adds an additional cryptographic safety control beyond VR's op and commit numbers.
    nonce: u128 = 0,

    /// Each client records its own client_id and a current request_number. A client is allowed to
    /// have just one outstanding request at a time. Each request is given a number by the client
    /// and later requests must have larger numbers than earlier ones. The request_number is used
    /// by the replicas to avoid running requests more than once; it is also used by the client to
    /// discard duplicate responses to its requests.
    client_id: u128 = 0,
    request_number: u64 = 0,

    /// Every message sent from one replica to another contains the sending replica's current view:
    view: u64 = 0,

    /// The latest view for which the replica's status was normal:
    latest_normal_view: u64 = 0,

    /// The op number:
    op: u64 = 0,

    /// The commit number:
    commit: u64 = 0,

    /// The journal offset to which this message relates:
    /// This enables direct access to an entry, without requiring previous variable-length entries.
    offset: u64 = 0,

    /// The journal index to which this message relates:
    /// Similarly, this enables direct access to an entry's header in the journal's header array.
    index: u32 = 0,

    /// The size of this message header and any associated data:
    size: u32 = @sizeOf(Header),

    /// The cluster reconfiguration epoch number (for future use):
    epoch: u32 = 0,

    /// The index of the replica that sent this message:
    replica: u16 = 0,

    /// The VR protocol command for this message:
    command: Command = .reserved,

    /// The state machine operation to apply:
    operation: Operation = .reserved,

    pub fn calculate_checksum_meta(self: *const Header) u128 {
        const meta = @bitCast([@sizeOf(Header)]u8, self.*);
        const checksum_size = @sizeOf(@TypeOf(self.checksum_meta));
        assert(checksum_size == 16);
        var target: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(meta[checksum_size..], target[0..], .{});
        return @bitCast(u128, target[0..checksum_size].*);
    }

    pub fn calculate_checksum_data(self: *const Header, data: []const u8) u128 {
        assert(@sizeOf(Header) + data.len == self.size);
        const checksum_size = @sizeOf(@TypeOf(self.checksum_data));
        assert(checksum_size == 16);
        var target: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(data[0..], target[0..], .{});
        return @bitCast(u128, target[0..checksum_size].*);
    }

    pub fn reset(self: *Header) void {
        self.* = .{};
        assert(self.checksum_meta == 0);
        assert(self.checksum_data == 0);
        assert(self.size == @sizeOf(Header));
        assert(self.command == .reserved);
        assert(self.operation == .reserved);
    }

    /// This must be called only after set_checksum_data() so that checksum_data is also covered:
    pub fn set_checksum_meta(self: *Header) void {
        self.checksum_meta = self.calculate_checksum_meta();
    }

    pub fn set_checksum_data(self: *Header, data: []const u8) void {
        self.checksum_data = self.calculate_checksum_data(data);
    }

    pub fn valid_checksum_meta(self: *const Header) bool {
        return self.checksum_meta == self.calculate_checksum_meta();
    }

    pub fn valid_checksum_data(self: *const Header, data: []const u8) bool {
        return self.checksum_data == self.calculate_checksum_data(data);
    }

    /// Returns null if all fields are set correctly according to the command, or else a warning.
    /// This does not verify that checksum_meta is valid, and expects that has already been done.
    pub fn bad(self: *const Header) ?[]const u8 {
        switch (self.command) {
            .request => {
                if (self.nonce != 0) return "nonce != 0";
                if (self.client_id == 0) return "client_id == 0";
                if (self.request_number == 0) return "request_number == 0";
                if (self.view != 0) return "view != 0";
                if (self.latest_normal_view != 0) return "latest_normal_view != 0";
                if (self.op != 0) return "op != 0";
                if (self.commit != 0) return "commit != 0";
                if (self.offset != 0) return "offset != 0";
                if (self.index != 0) return "index != 0";
                if (self.epoch != 0) return "epoch != 0";
                if (self.replica != 0) return "replica != 0";
                if (self.operation == .reserved) return "operation == .reserved";
            },
            else => {}, // TODO Add validators for all commands.
        }
        return null;
    }
};

const Client = struct {};

const ConfigurationAddress = union(enum) {
    address: *std.net.Address,
    replica: *Replica,
};

pub const Message = struct {
    header: *Header,
    buffer: []u8,
    references: usize = 0,
};

pub const MessageBus = struct {
    allocator: *Allocator,
    allocated: usize = 0,
    configuration: []ConfigurationAddress,

    /// A linked list of messages that are ready to send (FIFO):
    head: ?*Envelope = null,
    tail: ?*Envelope = null,

    const Address = union(enum) {
        replica: *Replica,
    };

    const Envelope = struct {
        address: Address,
        message: *Message,
        next: ?*Envelope,
    };

    pub fn init(allocator: *Allocator, configuration: []ConfigurationAddress) !MessageBus {
        var self = MessageBus{
            .allocator = allocator,
            .configuration = configuration,
        };
        return self;
    }

    pub fn deinit(self: *MessageBus) void {}

    pub fn gc(self: *MessageBus, message: *Message) void {
        if (message.references == 0) {
            log.debug("message_bus: freeing {}", .{message.header});
            self.destroy_message(message);
        }
    }

    pub fn send_header_to_replica(self: *MessageBus, replica: u16, header: Header) void {
        assert(header.size == @sizeOf(Header));

        // TODO Pre-allocate messages at startup.
        var message = self.create_message(@sizeOf(Header)) catch unreachable;
        message.header.* = header;

        const data = message.buffer[@sizeOf(Header)..message.header.size];
        // The order matters here because checksum_meta depends on checksum_data:
        message.header.set_checksum_data(data);
        message.header.set_checksum_meta();
        assert(message.header.valid_checksum_data(data));
        assert(message.header.valid_checksum_meta());

        assert(message.references == 0);
        self.send_message_to_replica(replica, message);
    }

    pub fn send_message_to_replica(self: *MessageBus, replica: u16, message: *Message) void {
        message.references += 1;

        // TODO Pre-allocate envelopes at startup.
        var envelope = self.allocator.create(Envelope) catch unreachable;
        envelope.* = .{
            .address = .{ .replica = self.configuration[replica].replica },
            .message = message,
            .next = null,
        };
        self.enqueue_message(envelope);
    }

    pub fn send_queued_messages(self: *MessageBus) void {
        while (self.head != null) {
            // Loop on a copy of the linked list, having reset the linked list first, so that any
            // synchronous append in on_message() is executed the next iteration of the outer loop.
            // This is not critical for safety here, but see run() in src/io.zig where it is.
            var head = self.head;
            self.head = null;
            self.tail = null;
            while (head) |envelope| {
                head = envelope.next;
                envelope.next = null;
                assert(envelope.message.references > 0);
                switch (envelope.address) {
                    .replica => |r| r.on_message(envelope.message),
                }
                envelope.message.references -= 1;
                self.gc(envelope.message);
                // The lifetime of an envelope will often be shorter than that of the message.
                self.allocator.destroy(envelope);
            }
        }
    }

    fn create_message(self: *MessageBus, size: u32) !*Message {
        assert(size >= @sizeOf(Header));

        var buffer = try self.allocator.alloc(u8, size);
        errdefer self.allocator.free(buffer);
        std.mem.set(u8, buffer, 0);

        var message = try self.allocator.create(Message);
        errdefer self.allocator.destroy(message);

        self.allocated += 1;

        message.* = .{
            .header = std.mem.bytesAsValue(Header, buffer[0..@sizeOf(Header)]),
            .buffer = buffer,
            .references = 0,
        };

        return message;
    }

    fn destroy_message(self: *MessageBus, message: *Message) void {
        assert(message.references == 0);
        self.allocator.free(message.buffer);
        self.allocator.destroy(message);
        self.allocated -= 1;
    }

    fn enqueue_message(self: *MessageBus, envelope: *Envelope) void {
        assert(envelope.message.references > 0);
        assert(envelope.next == null);
        if (self.head == null) {
            assert(self.tail == null);
            self.head = envelope;
            self.tail = envelope;
        } else {
            self.tail.?.next = envelope;
            self.tail = envelope;
        }
    }
};

const sector_size = 4096;

pub const Journal = struct {
    allocator: *Allocator,
    hash_chain: u128,
    headers: []Header align(sector_size),
    dirty: []bool,
    offset: u64,
    index: u32,

    pub fn init(allocator: *Allocator, entries_max: u32) !Journal {
        assert(entries_max > 0);
        var headers = try allocator.allocAdvanced(
            Header,
            sector_size,
            entries_max,
            .exact,
        );
        errdefer allocator.free(headers);
        std.mem.set(u8, std.mem.sliceAsBytes(headers), 0);

        var dirty = try allocator.alloc(bool, entries_max);
        errdefer allocator.free(dirty);
        std.mem.set(bool, dirty, false);

        var self = Journal{
            .allocator = allocator,
            .hash_chain = 0,
            .headers = headers,
            .dirty = dirty,
            .offset = 0,
            .index = 0,
        };

        assert(@mod(@ptrToInt(&headers[0]), sector_size) == 0);
        assert(dirty.len == headers.len);
        assert(@mod(self.offset, sector_size) == 0);

        return self;
    }

    pub fn deinit(self: *Journal) void {}

    /// Advances the index and offset to after a header.
    /// The header's index and offset must match the Journal's current position (as a safety check).
    /// The header does not need to have been written yet.
    pub fn advance_index_and_offset_to_after(self: *Journal, header: *const Header) void {
        assert(header.command != .reserved);
        assert(header.index == self.index);
        assert(header.offset == self.offset);
        const index = @mod(header.index + 1, @intCast(u32, self.headers.len));
        const offset = header.offset + header.size;
        assert(index > 0); // TODO Snapshotting.
        // TODO Assert against offset overflow.
        self.index = index;
        self.offset = offset;
    }

    /// Sets the index and offset to after a header.
    pub fn set_index_and_offset_to_after(self: *Journal, header: *const Header) void {
        assert(header.command != .reserved);
        self.index = header.index;
        self.offset = header.offset;
        self.advance_index_and_offset_to_after(header);
    }

    pub fn set_index_and_offset_to_empty(self: *Journal) void {
        self.assert_all_headers_are_reserved_from_index(0);
        // TODO With snapshots, we need to set this to the snapshot index and offset.
        self.index = 0;
        self.offset = 0;
    }

    pub fn assert_all_headers_are_reserved_from_index(self: *Journal, index: u32) void {
        // TODO With snapshots, adjust slices to stop before starting index.
        for (self.headers[index..]) |*header| assert(header.command == .reserved);
        for (self.dirty[index..]) |dirty| assert(dirty == false);
    }

    /// Returns a pointer to the header with the matching op number, or null if not found.
    /// Asserts that at most one such header exists.
    pub fn find_header_for_op(self: *Journal, op: u64) ?*Header {
        var result: ?*Header = null;
        for (self.headers) |*header, index| {
            if (header.op == op and header.command != .reserved) {
                assert(result == null);
                assert(header.index == index);
                result = header;
            }
        }
        return result;
    }

    pub fn flush(self: *Journal) void {
        // TODO Flush headers to disk to both slots.
        // Used for rolling back state, where we want to be sure we don't leak old state.
        // Especially before we need to append a new entry and we want a good starting point.
    }

    /// Removes entries after op number (exclusive), i.e. with a higher op number.
    /// This is used after a view change to prune uncommitted entries discarded by the new leader.
    pub fn remove_entries_after_op(self: *Journal, op: u64) void {
        assert(op > 0);
        for (self.headers) |*header, index| {
            if (header.op > op and header.command != .reserved) {
                self.remove_entry_at_index(index);
            }
        }
    }

    pub fn remove_entry_at_index(self: *Journal, index: u64) void {
        self.headers[index].reset();
        self.dirty[index] = false;
    }

    pub fn write(self: *Journal, header: *const Header, buffer: []const u8) void {
        assert(header.command == .prepare);
        assert(header.operation != .reserved);
        assert(header.size >= @sizeOf(Header));
        assert(header.size == buffer.len);
        // TODO Assert against offset overflow.

        var existing = self.headers[header.index];
        assert(existing.command == .reserved or existing.checksum_meta == header.checksum_meta);

        log.debug("journal: write: index={} offset={} len={}", .{
            header.index,
            header.offset,
            buffer.len,
        });

        self.headers[header.index] = header.*;
        self.dirty[header.index] = false;
        // TODO Write to disk.
        // TODO Write headers to disk to both slots.
    }
};

pub const StateMachine = struct {
    allocator: *Allocator,

    pub fn init(allocator: *Allocator) !StateMachine {
        var self = StateMachine{
            .allocator = allocator,
        };

        return self;
    }

    pub fn deinit(self: *StateMachine) void {}
};

const Status = enum {
    normal,
    view_change,
    recovering,
};

const Timeout = struct {
    name: []const u8,
    replica: u16,
    after: u64,
    ticks: u64 = 0,
    ticking: bool = false,

    /// It's important to check that when fired() is acted on that the timeout is stopped/started,
    /// otherwise further ticks around the event loop may trigger a thundering herd of messages.
    pub fn fired(self: *Timeout) bool {
        if (self.ticking and self.ticks >= self.after) {
            log.debug("{}: {} fired", .{ self.replica, self.name });
            return true;
        } else {
            return false;
        }
    }

    pub fn reset(self: *Timeout) void {
        assert(self.ticking);
        self.ticks = 0;
        log.debug("{}: {} reset", .{ self.replica, self.name });
    }

    pub fn start(self: *Timeout) void {
        self.ticks = 0;
        self.ticking = true;
        log.debug("{}: {} started", .{ self.replica, self.name });
    }

    pub fn stop(self: *Timeout) void {
        self.ticks = 0;
        self.ticking = false;
        log.debug("{}: {} stopped", .{ self.replica, self.name });
    }

    pub fn tick(self: *Timeout) void {
        if (self.ticking) self.ticks += 1;
    }
};

pub const Replica = struct {
    allocator: *Allocator,

    /// The maximum number of replicas that may be faulty:
    f: u32,

    /// An array containing the remote or local addresses of each of the 2f + 1 replicas:
    /// Unlike the VRR paper, we do not sort the array but leave the order explicitly to the user.
    /// There are several advantages to this:
    /// * The operator may deploy a cluster with proximity in mind since replication follows order.
    /// * A replica's IP address may be changed without reconfiguration.
    /// This does require that the user specify the same order to all replicas.
    configuration: []ConfigurationAddress,

    /// An abstraction to send messages from the replica to itself or another replica or client.
    /// The recipient replica or client may be a local in-memory pointer or network-addressable.
    /// The message bus will also deliver messages to this replica by calling Replica.on_message().
    message_bus: *MessageBus,

    /// The persistent log of hash-chained journal entries:
    journal: *Journal,

    /// For executing service up-calls after an operation has been committed:
    state_machine: *StateMachine,

    /// The index into the configuration where this replica's IP address is stored:
    replica: u16,

    /// The current view, initially 0:
    view: u64 = 0,

    /// The current status, either normal, view_change, or recovering:
    /// TODO Don't default to normal, set the starting status according to the Journal's health.
    status: Status = .normal,

    /// The op number assigned to the most recently received request, initially 0:
    op: u64 = 0,

    /// The op number of the most recently committed operation:
    /// TODO Review that all usages handle the special starting case (where nothing is committed).
    commit: u64 = 0,

    /// The current request's checksum (used for now to enforce one-at-a-time request processing):
    request_checksum_meta: ?u128 = null,

    /// The current prepare message (used to cross-check prepare_ok messages, and for resending):
    prepare_message: ?*Message = null,
    prepare_attempt: u64 = 0,

    /// Unique prepare_ok messages for the same view, op number and checksum from ALL replicas:
    prepare_ok_from_all_replicas: []?*Message,

    /// Unique start_view_change messages for the same view from OTHER replicas (excluding ourself):
    start_view_change_from_other_replicas: []?*Message,

    /// Unique do_view_change messages for the same view from ALL replicas (including ourself):
    do_view_change_from_all_replicas: []?*Message,

    /// The number of ticks without enough prepare_ok's before the leader resends a prepare:
    /// TODO Adjust this dynamically to match sliding window EWMA of recent network latencies.
    prepare_timeout: Timeout,

    /// The number of ticks before the leader sends a commit heartbeat:
    /// The leader always sends a commit heartbeat irrespective of when it last sent a prepare.
    /// This improves liveness when prepare messages cannot be replicated fully due to partitions.
    commit_timeout: Timeout,

    /// The number of ticks without hearing from the leader before a follower starts a view change:
    /// This is also the number of ticks before an existing view change is timed out.
    view_timeout: Timeout,

    // TODO Limit integer types for `f` and `replica` to match their upper bounds in practice.
    pub fn init(
        allocator: *Allocator,
        f: u32,
        configuration: []ConfigurationAddress,
        message_bus: *MessageBus,
        journal: *Journal,
        state_machine: *StateMachine,
        replica: u16,
    ) !Replica {
        assert(configuration.len > 0);
        assert(configuration.len > f);
        assert(replica < configuration.len);

        var prepare_ok = try allocator.alloc(?*Message, configuration.len);
        for (prepare_ok) |*received| received.* = null;
        errdefer allocator.free(prepare_ok);

        var start_view_change = try allocator.alloc(?*Message, configuration.len);
        for (start_view_change) |*received| received.* = null;
        errdefer allocator.free(start_view_change);

        var do_view_change = try allocator.alloc(?*Message, configuration.len);
        for (do_view_change) |*received| received.* = null;
        errdefer allocator.free(do_view_change);

        var self = Replica{
            .allocator = allocator,
            .f = f,
            .configuration = configuration,
            .replica = replica,
            .message_bus = message_bus,
            .journal = journal,
            .state_machine = state_machine,
            .prepare_ok_from_all_replicas = prepare_ok,
            .start_view_change_from_other_replicas = start_view_change,
            .do_view_change_from_all_replicas = do_view_change,
            .prepare_timeout = Timeout{
                .name = "prepare_timeout",
                .replica = replica,
                .after = 10,
            },
            .commit_timeout = Timeout{
                .name = "commit_timeout",
                .replica = replica,
                .after = 30,
            },
            .view_timeout = Timeout{
                .name = "view_timeout",
                .replica = replica,
                .after = 500,
            },
        };

        // We must initialize timeouts here, not in tick() on the first tick, because on_message()
        // can race with tick()... before timeouts have been initialized:
        assert(self.status == .normal);
        if (self.leader()) {
            log.debug("{}: init: leader", .{self.replica});
            self.commit_timeout.start();
        } else {
            log.debug("{}: init: follower", .{self.replica});
            self.view_timeout.start();
        }

        return self;
    }

    pub fn deinit(self: *Replica) void {
        self.allocator.free(self.prepare_ok_from_all_replicas);
        self.allocator.free(self.start_view_change_from_other_replicas);
        self.allocator.free(self.do_view_change_from_all_replicas);
    }

    /// Returns whether the replica is a follower for the current view.
    /// This may be used only when the replica status is normal.
    pub fn follower(self: *Replica) bool {
        return !self.leader();
    }

    /// Returns whether the replica is the leader for the current view.
    /// This may be used only when the replica status is normal.
    pub fn leader(self: *Replica) bool {
        assert(self.status == .normal);
        return self.leader_index(self.view) == self.replica;
    }

    /// Returns the index into the configuration of the leader for a given view.
    pub fn leader_index(self: *Replica, view: u64) u16 {
        return @intCast(u16, @mod(view, self.configuration.len));
    }

    /// Returns the index into the configuration of the next replica for a given view,
    /// or null if the next replica is the view's leader.
    /// Replication starts and ends with the leader, we never forward back to the leader.
    pub fn next_replica(self: *Replica, view: u64) ?u16 {
        const next = @mod(self.replica + 1, @intCast(u16, self.configuration.len));
        if (next == self.leader_index(view)) return null;
        return next;
    }

    /// Time is measured in logical ticks that are incremented on every call to tick().
    /// This eliminates a dependency on the system time and enables deterministic testing.
    pub fn tick(self: *Replica) void {
        self.prepare_timeout.tick();
        self.commit_timeout.tick();
        self.view_timeout.tick();

        if (self.prepare_timeout.fired()) self.on_prepare_timeout();
        if (self.commit_timeout.fired()) self.on_commit_timeout();
        if (self.view_timeout.fired()) self.on_view_timeout();
    }

    /// Called by the MessageBus to deliver a message to the replica.
    pub fn on_message(self: *Replica, message: *Message) void {
        log.debug("{}: on_message: view={} status={} {}", .{ self.replica, self.view, @tagName(self.status), message.header });
        if (message.header.bad()) |reason| {
            log.debug("{}: on_message: bad ({})", .{ self.replica, reason });
            return;
        }
        assert(message.header.replica < self.configuration.len);
        switch (message.header.command) {
            .request => self.on_request(message),
            .prepare => self.on_prepare(message),
            .prepare_ok => self.on_prepare_ok(message),
            .commit => self.on_commit(message),
            .start_view_change => self.on_start_view_change(message),
            .do_view_change => self.on_do_view_change(message),
            .start_view => self.on_start_view(message),
            else => unreachable,
        }
    }

    fn on_request(self: *Replica, message: *Message) void {
        if (self.status != .normal) {
            log.debug("{}: on_request: ignoring ({})", .{ self.replica, self.status });
            return;
        }

        if (self.follower()) {
            // TODO Add an optimization to tell the client the view (and leader) ahead of a timeout.
            // This would prevent thundering herds where clients suddenly broadcast the cluster.
            log.debug("{}: on_request: ignoring (follower)", .{self.replica});
            return;
        }

        assert(self.status == .normal);
        assert(self.leader());

        // TODO Check the client table to see if this is a duplicate request and reply if so.
        // TODO If request is pending then this will also reflect in client table and we can ignore.
        // TODO Add client information to client table.

        if (self.request_checksum_meta) |request_checksum_meta| {
            assert(message.header.command == .request);
            if (message.header.checksum_meta == request_checksum_meta) {
                log.debug("{}: on_request: ignoring (already preparing)", .{self.replica});
                return;
            }
        }

        // TODO Queue (or drop client requests after a limit) to handle one request at a time:
        // TODO Clear this queue if we lose our leadership (critical for correctness).
        assert(self.request_checksum_meta == null);
        self.request_checksum_meta = message.header.checksum_meta;

        log.debug("{}: on_request: request {}", .{ self.replica, message.header.checksum_meta });

        // The primary advances op-number, adds the request to the end of the log, and updates the
        // information for this client in the client-table to contain the new request number, s.
        // Then it sends a ⟨PREPARE v, m, n, k⟩ message to the other replicas, where v is the
        // current view-number, m is the message it received from the client, n is the op-number it
        // assigned to the request, and k is the commit-number.

        var data = message.buffer[@sizeOf(Header)..message.header.size];
        // TODO Assign timestamps to structs in associated data.

        message.header.nonce = self.journal.hash_chain;
        message.header.view = self.view;
        message.header.op = self.op + 1;
        message.header.commit = self.commit;
        message.header.offset = self.journal.offset;
        message.header.index = self.journal.index;
        message.header.replica = self.replica;
        message.header.command = .prepare;

        message.header.set_checksum_data(data);
        message.header.set_checksum_meta();
        assert(message.header.valid_checksum_data(data));
        assert(message.header.valid_checksum_meta());
        assert(message.header.checksum_meta != self.request_checksum_meta.?);

        log.debug("{}: on_request: prepare {}", .{ self.replica, message.header.checksum_meta });

        assert(self.prepare_message == null);
        assert(self.prepare_attempt == 0);
        for (self.prepare_ok_from_all_replicas) |received| assert(received == null);
        assert(self.prepare_timeout.ticking == false);

        message.references += 1;
        self.prepare_message = message;
        self.prepare_attempt = 0;
        self.prepare_timeout.start();

        // Use the same replication code path for the leader and followers:
        self.send_message_to_replica(self.replica, message);
    }

    /// Replication is simple, with a single code path for the leader and followers:
    ///
    /// The leader starts by sending a prepare message to itself.
    ///
    /// Each replica (including the leader) then forwards this prepare message to the next replica
    /// in the configuration, in parallel to writing to its own journal, closing the circle until
    /// the next replica is back to the leader, in which case the replica does not forward.
    ///
    /// This keeps the leader's outgoing bandwidth limited (one-for-one) to incoming bandwidth,
    /// since the leader need only replicate to the next replica. Otherwise, the leader would need
    /// to replicate to multiple followers, dividing available bandwidth.
    ///
    /// This does not impact latency, since with Flexible Paxos we need only one remote prepare_ok.
    /// It is ideal if this synchronous replication to one remote replica is to the next replica,
    /// since that is the replica next in line to be leader, which will need to be up-to-date before
    /// it can start the next view.
    ///
    /// At the same time, asynchronous replication keeps going, so that if our local disk is slow
    /// then any latency spike will be masked by more remote prepare_ok messages as they come in.
    /// This gives automatic tail latency tolerance for storage latency spikes.
    ///
    /// The remaining problem then is tail latency tolerance for network latency spikes.
    /// If the next replica is down or partitioned, then the leader's prepare timeout will fire,
    /// and the leader will resend but to another replica, until it receives enough prepare_ok's.
    fn on_prepare(self: *Replica, message: *Message) void {
        if (self.status != .normal) {
            log.debug("{}: on_prepare: ignoring ({})", .{ self.replica, self.status });
            self.on_repair(message);
            return;
        }

        if (message.header.view < self.view) {
            log.debug("{}: on_prepare: ignoring (older view)", .{self.replica});
            self.on_repair(message);
            return;
        }

        // TODO Add assertions for repair edge-cases where we are the leader.

        if (message.header.view == self.view and message.header.op <= self.op) {
            // Since we allow gaps in the journal, our op number now means only the highest op we
            // have seen, and no longer the highest op we have prepared (as with basic VRR).
            log.debug("{}: on_prepare: older op (duplicate, reordered or repair)", .{self.replica});
            self.on_repair(message);
            self.send_prepare_ok(message);
            return;
        }

        if (message.header.view > self.view) {
            log.debug("{}: on_prepare: newer view", .{self.replica});
            self.jump_to_newer_view(message.header.view);
        }

        assert(self.status == .normal);
        assert(message.header.view == self.view);
        assert(self.leader() or self.follower());
        assert(message.header.replica == self.leader_index(message.header.view));
        assert(message.header.op >= self.op + 1);

        if (message.header.op > self.op + 1) {
            // This can happen where we have an op jump for the same view (no view jump).
            log.debug("{}: on_prepare: op jump from {} to {}", .{
                self.replica,
                self.op + 1,
                message.header.op,
            });
            assert(message.header.op > 0);
            assert(message.header.op > self.commit);
            self.op = message.header.op - 1;
            assert(self.op >= self.commit);
            assert(self.op + 1 == message.header.op);
        }

        if (self.follower()) {
            self.view_timeout.reset();
            log.debug("{}: on_prepare: view_timeout reset", .{self.replica});
        }

        // We must advance our op number before replicating or journalling as per the paper,
        // so that the leader has the correct op number when it receives the prepare_ok quorum:
        log.debug("{}: on_prepare: advancing op number", .{self.replica});
        assert(message.header.op == self.op + 1);
        self.op = message.header.op;

        // TODO Update client's information in the client table.

        // Replicate to the next replica in the configuration (until we get back to the leader):
        // TODO All replicas should send commit heartbeats.
        // TODO Skip past the next replica if we know it's probably crashed, frozen or partitioned.
        // This optimization will improve latency in the event of faults or partitions.
        if (self.next_replica(self.view)) |next| {
            log.debug("{}: on_prepare: replicating to replica {}", .{ self.replica, next });
            self.send_message_to_replica(next, message);
        }

        log.debug("{}: on_prepare: appending to journal", .{self.replica});

        if (self.leader()) {
            assert(message.header.index == self.journal.index);
            assert(message.header.offset == self.journal.offset);
            // TODO Handling view jumps and op jumps above must also correct journal index, offset.
            // TODO As well as ensure that the destination journal entry is free (reserved).
            // We can then do the above index and offset assertions for followers as well.
        }
        // TODO Assert that append will not overflow the journal.
        // The index and offset must be updated before writing, as the quorum may outrun our writes:
        self.journal.index = @mod(
            message.header.index + 1,
            @intCast(u32, self.journal.headers.len),
        );
        self.journal.offset = message.header.offset + message.header.size;

        // TODO Assign an async frame (self.appending) and allow what follows to then be async:
        // We will then have two async frames (self.appending and self.repairing).
        // We will be able to repair and append concurrently, and so catch up to the leader.
        self.journal.write(message.header, message.buffer);

        self.send_prepare_ok(message);

        if (self.follower()) {
            self.commit_ops_through(message.header.commit);
        }
    }

    fn send_prepare_ok(self: *Replica, message: *Message) void {
        if (self.status != .normal) {
            log.debug("{}: send_prepare_ok: not sending ({})", .{ self.replica, self.status });
            return;
        }

        if (message.header.view < self.view) {
            log.debug("{}: send_prepare_ok: not sending (older view)", .{self.replica});
            return;
        }

        if (message.header.view > self.view) {
            log.warn("{}: send_prepare_ok: not sending (newer view)", .{self.replica});
            return;
        }

        if (message.header.op > self.op) {
            log.warn("{}: send_prepare_ok: not sending (newer op)", .{self.replica});
            return;
        }

        if (message.header.op <= self.commit) {
            log.debug("{}: send_prepare_ok: not sending (committed)", .{self.replica});
            return;
        }

        var entry = self.journal.headers[message.header.index];

        if (entry.command == .reserved) {
            log.debug("{}: send_prepare_ok: not sending (journal entry reserved)", .{self.replica});
            return;
        }

        if (entry.checksum_meta != message.header.checksum_meta) {
            log.debug("{}: send_prepare_ok: not sending (journal entry checksum)", .{self.replica});
            return;
        }

        if (self.journal.dirty[message.header.index]) {
            log.debug("{}: send_prepare_ok: not sending (journal entry dirty)", .{self.replica});
            return;
        }

        self.send_header_to_replica(message.header.replica, .{
            .command = .prepare_ok,
            .nonce = message.header.checksum_meta,
            .replica = self.replica,
            .view = message.header.view,
            .op = message.header.op,
        });
    }

    fn jump_to_newer_view(self: *Replica, new_view: u64) void {
        log.debug("{}: jump_to_newer_view: view {}", .{ self.replica, new_view });
        assert(self.status == .normal);
        assert(new_view > self.view);
        assert(self.leader() or self.follower());
        assert(self.leader_index(new_view) != self.replica);

        // 5.2 State Transfer
        // There are two cases, depending on whether the slow node has learned that it is missing
        // requests in its current view, or has heard about a later view. In the former case it only
        // needs to learn about requests after its op-number. In the latter it needs to learn about
        // requests after the latest committed request in its log, since requests after that might
        // have been reordered in the view change, so in this case it sets its op-number to its
        // commit-number and removes all entries after this from its log.

        // It is critical for correctness that we discard any ops here that were not involved in the
        // view change(s), since they may have been replaced in the view change(s) by other ops.
        // If we fail to do this immediately then we may commit the wrong op and diverge state when
        // we process a commit message or when we process a commit number in a prepare message.
        if (self.op > self.commit) {
            log.debug("{}: jump_to_newer_view: setting op number {} to commit number {}", .{
                self.replica,
                self.op,
                self.commit,
            });
            self.op = self.commit;
            log.debug("{}: jump_to_newer_view: removing journal entries after op number {}", .{
                self.replica,
                self.op,
            });
            self.journal.remove_entries_after_op(self.op);
            if (self.journal.find_header_for_op(self.op)) |header| {
                self.journal.set_index_and_offset_to_after(header);
            } else {
                // TODO When we implement snapshots we must update this assertion since the last
                // committed op may have been included in the snapshot and removed from the journal.
                // Our op number may then be greater than zero.
                assert(self.op == 0);
                self.journal.set_index_and_offset_to_empty();
            }
            self.journal.assert_all_headers_are_reserved_from_index(self.journal.index);
            self.journal.flush();
        } else {
            log.debug("{}: jump_to_newer_view: no journal entries to remove", .{self.replica});
        }

        assert(self.op == self.commit);
        self.transition_to_normal_status(new_view);
        assert(self.view == new_view);
        assert(self.follower());
    }

    fn on_prepare_timeout(self: *Replica) void {
        log.debug("{}: on_prepare_timeout: fired", .{self.replica});

        assert(self.status == .normal);
        assert(self.leader());
        assert(self.request_checksum_meta != null);
        assert(self.prepare_message != null);
        assert(self.prepare_message.?.header.view == self.view);

        // TODO Exponential backoff.
        // TODO Prevent flooding the network due to multiple concurrent rounds of replication.
        self.prepare_attempt += 1;
        self.prepare_timeout.reset();

        // The list of remote replicas yet to send a prepare_ok:
        var waiting: [32]u16 = undefined;
        var waiting_len: usize = 0;
        for (self.prepare_ok_from_all_replicas) |received, replica| {
            if (received == null and replica != self.replica) {
                waiting[waiting_len] = @intCast(u16, replica);
                waiting_len += 1;
                if (waiting_len == waiting.len) break;
            }
        }

        if (waiting_len == 0) {
            // TODO Add an assertion that we are indeed waiting on the journal as we expect.
            log.debug("{}: on_prepare_timeout: waiting for journal", .{self.replica});
            return;
        }

        for (waiting[0..waiting_len]) |replica| {
            log.debug("{}: on_prepare_timeout: waiting for replica {}", .{ self.replica, replica });
        }

        // Cycle through the list for each attempt to reach live replicas and get around partitions:
        // If only the first replica in the list was chosen... liveness would suffer if it was down!
        var replica = waiting[@mod(self.prepare_attempt, waiting_len)];
        assert(replica != self.replica);

        log.debug("{}: on_prepare_timeout: replicating to replica {}", .{ self.replica, replica });
        self.send_message_to_replica(replica, self.prepare_message.?);
    }

    fn on_prepare_ok(self: *Replica, message: *Message) void {
        if (self.status != .normal) {
            log.debug("{}: on_prepare_ok: ignoring ({})", .{ self.replica, self.status });
            return;
        }

        if (message.header.view < self.view) {
            log.debug("{}: on_prepare_ok: ignoring (older view)", .{self.replica});
            return;
        }

        if (message.header.view > self.view) {
            // Another replica is treating us as the leader for a view we do not know about.
            // This may be caused by a fault in the network topology.
            log.warn("{}: on_prepare_ok: ignoring (newer view)", .{self.replica});
            return;
        }

        if (self.follower()) {
            // This may be caused by a fault in the network topology.
            log.warn("{}: on_prepare_ok: ignoring (follower)", .{self.replica});
            return;
        }

        if (self.prepare_message) |prepare_message| {
            if (message.header.nonce != prepare_message.header.checksum_meta) {
                log.debug("{}: on_prepare_ok: ignoring (different nonce)", .{self.replica});
                return;
            }
        } else {
            log.debug("{}: on_prepare_ok: ignoring (not preparing)", .{self.replica});
            return;
        }

        assert(self.status == .normal);
        assert(message.header.view == self.view);
        assert(self.leader());
        assert(message.header.nonce == self.prepare_message.?.header.checksum_meta);
        assert(message.header.op == self.op);
        assert(message.header.op == self.commit + 1);

        // Wait until we have `f + 1` messages (including ourself) for quorum:
        const count = self.add_message_and_receive_quorum_exactly_once(
            self.prepare_ok_from_all_replicas,
            message,
            self.f + 1,
        ) orelse return;

        assert(count == self.f + 1);
        log.debug("{}: on_prepare_ok: quorum received", .{self.replica});

        self.commit_ops_through(message.header.op);

        self.reset_prepare();
    }

    fn reset_prepare(self: *Replica) void {
        if (self.prepare_message) |message| {
            self.request_checksum_meta = null;
            message.references -= 1;
            self.message_bus.gc(message);
            self.prepare_message = null;
            self.prepare_attempt = 0;
            self.prepare_timeout.stop();
            self.reset_quorum_counter(self.prepare_ok_from_all_replicas, .prepare_ok);
        }
        assert(self.request_checksum_meta == null);
        assert(self.prepare_message == null);
        assert(self.prepare_attempt == 0);
        assert(self.prepare_timeout.ticking == false);
        for (self.prepare_ok_from_all_replicas) |received| assert(received == null);
    }

    fn on_commit_timeout(self: *Replica) void {
        assert(self.status == .normal);
        assert(self.leader());

        self.commit_timeout.reset();

        // TODO Add checksum_meta (as nonce) of latest committed entry so that followers can verify.
        self.send_header_to_other_replicas(.{
            .command = .commit,
            .replica = self.replica,
            .view = self.view,
            .commit = self.commit,
        });
    }

    fn on_commit(self: *Replica, message: *const Message) void {
        if (self.status != .normal) {
            log.debug("{}: on_commit: ignoring ({})", .{ self.replica, self.status });
            return;
        }

        if (message.header.view < self.view) {
            log.debug("{}: on_commit: ignoring (older view)", .{self.replica});
            return;
        }

        if (message.header.view > self.view) {
            log.debug("{}: on_commit: newer view", .{self.replica});
            self.jump_to_newer_view(message.header.view);
        }

        if (self.leader()) {
            log.warn("{}: on_commit: ignoring (leader)", .{self.replica});
            return;
        }

        assert(self.status == .normal);
        assert(message.header.view == self.view);
        assert(self.follower());
        assert(message.header.replica == self.leader_index(message.header.view));
        assert(self.commit <= self.op);

        self.view_timeout.reset();

        self.commit_ops_through(message.header.commit);
    }

    fn on_repair(self: *Replica, message: *Message) void {
        @panic("TODO");
    }

    fn on_view_timeout(self: *Replica) void {
        assert(self.status == .view_change or self.status == .normal);
        // During a view change, all replicas may have view_timeout running, even the new leader:
        assert(self.status == .view_change or self.follower());
        // The transition will reset the view timeout:
        self.transition_to_view_change_status(self.view + 1);
    }

    /// A replica i that notices the need for a view change advances its view, sets its status to
    /// view_change, and sends a ⟨start_view_change v, i⟩ message to all the other replicas,
    /// where v identifies the new view. A replica notices the need for a view change either based
    /// on its own timer, or because it receives a start_view_change or do_view_change message for
    /// a view with a larger number than its own view.
    fn transition_to_view_change_status(self: *Replica, new_view: u64) void {
        log.debug("{}: transition_to_view_change_status: view {}", .{ self.replica, new_view });
        assert(new_view > self.view);
        self.view = new_view;
        self.status = .view_change;

        self.commit_timeout.stop();
        self.view_timeout.start();

        self.reset_prepare();

        // Some VR implementations reset their counters only on entering a view, perhaps assuming
        // the view will be followed only by a single subsequent view change to the next view.
        // However, multiple successive view changes can fail, e.g. after a view change timeout.
        // We must therefore reset our counters here to avoid counting messages from an older view,
        // which would violate the quorum intersection property essential for correctness.
        self.reset_quorum_counter(self.start_view_change_from_other_replicas, .start_view_change);
        self.reset_quorum_counter(self.do_view_change_from_all_replicas, .do_view_change);

        // Send only to other replicas (and not to ourself) to avoid a quorum off-by-one error:
        // This could happen if the replica mistakenly counts its own message in the quorum.
        self.send_header_to_other_replicas(.{
            .command = .start_view_change,
            .replica = self.replica,
            .view = new_view,
        });
    }

    fn on_start_view_change(self: *Replica, message: *Message) void {
        // Any view change must be greater than zero since the initial view is already zero:
        assert(message.header.view > 0);

        if (message.header.view < self.view) {
            log.debug("{}: on_start_view_change: ignoring (older view)", .{self.replica});
            return;
        }

        if (message.header.view == self.view and self.status != .view_change) {
            log.debug("{}: on_start_view_change: ignoring ({})", .{ self.replica, self.status });
            return;
        }

        if (message.header.view > self.view) {
            log.debug("{}: on_start_view_change: changing to newer view", .{self.replica});
            self.transition_to_view_change_status(message.header.view);
            // Continue below...
        }

        if (message.header.replica == self.replica) {
            // This may be caused by a fault in the network topology.
            log.warn("{}: on_start_view_change: ignoring (self)", .{self.replica});
            return;
        }

        assert(self.status == .view_change);
        assert(message.header.view == self.view);

        // Wait until we have `f` messages (excluding ourself) for quorum:
        const count = self.add_message_and_receive_quorum_exactly_once(
            self.start_view_change_from_other_replicas,
            message,
            self.f,
        ) orelse return;

        assert(count == self.f);
        log.debug("{}: on_start_view_change: quorum received", .{self.replica});

        // When replica i receives start_view_change messages for its view from f other replicas,
        // it sends a ⟨do_view_change v, l, v’, n, k, i⟩ message to the node that will be the
        // primary in the new view. Here v is its view, l is its log, v′ is the view number of the
        // latest view in which its status was normal, n is the op number, and k is the commit
        // number.
        const new_leader = self.leader_index(self.view);
        self.send_header_to_replica(new_leader, .{
            .command = .do_view_change,
            .replica = self.replica,
            .view = self.view,
            .op = self.op,
            .commit = self.commit,
        });

        // TODO Add latest normal view.
        // TODO Add N most recent journal entries.
    }

    fn on_do_view_change(self: *Replica, message: *Message) void {
        // Any view change must be great than zero since the initial view is already zero:
        assert(message.header.view > 0);

        if (message.header.view < self.view) {
            log.debug("{}: on_do_view_change: ignoring (older view)", .{self.replica});
            return;
        }

        if (message.header.view == self.view and self.status != .view_change) {
            log.debug("{}: on_do_view_change: ignoring ({})", .{ self.replica, self.status });
            return;
        }

        if (message.header.view > self.view) {
            log.debug("{}: on_do_view_change: changing to newer view", .{self.replica});
            // At first glance, it might seem that sending start_view_change messages out now after
            // receiving a do_view_change here would be superfluous. However, this is essential,
            // especially in the presence of asymmetric network faults. At this point, we may not
            // yet have received a do_view_change quorum, and another replica might need our
            // start_view_change message in order to get its do_view_change message through to us.
            // We therefore do not special case the start_view_change function, and we always send
            // start_view_change messages, regardless of the message that initiated the view change:
            self.transition_to_view_change_status(message.header.view);
            // Continue below...
        }

        if (self.leader_index(self.view) != self.replica) {
            // This may be caused by a fault in the network topology.
            log.warn("{}: on_do_view_change: ignoring (follower)", .{self.replica});
            return;
        }

        assert(self.status == .view_change);
        assert(self.leader_index(self.view) == self.replica);
        assert(message.header.view == self.view);

        // Wait until we have `f + 1` messages (including ourself) for quorum:
        const count = self.add_message_and_receive_quorum_exactly_once(
            self.do_view_change_from_all_replicas,
            message,
            self.f + 1,
        ) orelse return;

        assert(count == self.f + 1);
        log.debug("{}: on_do_view_change: quorum received", .{self.replica});

        // When the new primary receives f + 1 do_view_change messages from different replicas
        // (including itself), it sets its view number to that in the messages and selects as the
        // new log the one contained in the message with the largest v′; if several messages have
        // the same v′ it selects the one among them with the largest n. It sets its op number to
        // that of the topmost entry in the new log, sets its commit number to the largest such
        // number it received in the do_view_change messages, changes its status to normal, and
        // informs the other replicas of the completion of the view change by sending
        // ⟨start_view v, l, n, k⟩ messages to the other replicas, where l is the new log, n is the
        // op number, and k is the commit number.

        // It's possible for the first view change to not have anything newer than these initial values:
        // Use real values here, rather than introducing anything artificial.
        var r: u16 = self.replica;
        var v: u64 = 0;
        var n: u64 = 0;
        var k: u64 = 0;
        for (self.do_view_change_from_all_replicas) |received, replica| {
            if (received) |m| {
                if ((m.header.latest_normal_view > v) or
                    (m.header.latest_normal_view == v and m.header.op > n))
                {
                    r = @intCast(u16, replica);
                    v = m.header.latest_normal_view;
                    n = m.header.op;
                    k = m.header.commit;
                }
            }
        }

        // We do not assert that v must be non-zero, because v represents the latest normal view,
        // which may be zero if this is our first view change.

        log.debug("{}: replica={} has the latest log: op={} commit={}", .{ self.replica, r, n, k });

        // TODO Update Journal
        // TODO Calculate how much of the Journal to send in start_view message.
        self.transition_to_normal_status(self.view);

        assert(self.status == .normal);
        assert(self.leader());

        self.op = n;
        self.commit_ops_through(k);
        assert(self.commit == k);

        // TODO Add Journal entries to start_view message:
        self.send_header_to_other_replicas(.{
            .command = .start_view,
            .replica = self.replica,
            .view = self.view,
            .op = self.op,
            .commit = self.commit,
        });
    }

    fn on_start_view(self: *Replica, message: *const Message) void {
        assert(message.header.view > 0);

        if (message.header.view < self.view) {
            log.debug("{}: on_start_view: ignoring (older view)", .{self.replica});
            return;
        }

        if (message.header.view == self.view and self.status != .view_change) {
            log.debug("{}: on_start_view: ignoring ({})", .{ self.replica, self.status });
            return;
        }

        if (message.header.replica == self.replica) {
            // This may be caused by a fault in the network topology.
            log.warn("{}: on_start_view: ignoring (self)", .{self.replica});
            return;
        }

        assert(message.header.replica == self.leader_index(message.header.view));

        // TODO Assert that start_view message matches what we expect if our journal is empty.
        // TODO Assert that start_view message's oldest op overlaps with our last commit number.
        // TODO Update the journal.

        self.transition_to_normal_status(message.header.view);

        assert(self.status == .normal);
        assert(self.follower());

        // TODO Update our last op number according to message.

        // TODO self.commit(msg.lastcommitted());
        // TODO self.send_prepare_oks(oldLastOp);
    }

    fn transition_to_normal_status(self: *Replica, new_view: u64) void {
        log.debug("{}: transition_to_normal_status: view {}", .{ self.replica, new_view });
        // In the VRR paper it's possible to transition from .normal to .normal for the same view.
        // For example, this could happen after a state transfer triggered by an op jump.
        assert(new_view >= self.view);
        self.view = new_view;
        self.status = .normal;

        if (self.leader()) {
            log.debug("{}: transition_to_normal_status: leader", .{self.replica});

            self.commit_timeout.start();
            self.view_timeout.stop();
        } else {
            log.debug("{}: transition_to_normal_status: follower", .{self.replica});

            self.commit_timeout.stop();
            self.view_timeout.start();
        }

        // TODO Wait for any async journal write to finish (before transitioning out of .normal).
        // This is essential for correctness:
        self.reset_prepare();

        // Reset and garbage collect all view change messages (if any):
        // This is not essential for correctness, only efficiency.
        // We just don't want to tie them up until the next view change (when they must be reset):
        self.reset_quorum_counter(self.start_view_change_from_other_replicas, .start_view_change);
        self.reset_quorum_counter(self.do_view_change_from_all_replicas, .do_view_change);
    }

    fn add_message_and_receive_quorum_exactly_once(
        self: *Replica,
        messages: []?*Message,
        message: *Message,
        threshold: u32,
    ) ?usize {
        assert(messages.len == self.configuration.len);

        assert(message.header.view == self.view);
        switch (message.header.command) {
            .prepare_ok => {
                assert(self.status == .normal);
                assert(self.leader());
                assert(message.header.nonce == self.prepare_message.?.header.checksum_meta);
            },
            .start_view_change => assert(self.status == .view_change),
            .do_view_change => {
                assert(self.status == .view_change);
                assert(self.leader_index(self.view) == self.replica);
            },
            else => unreachable,
        }

        // TODO Improve this to work for "a cluster of one":
        assert(threshold >= 1);
        assert(threshold <= self.configuration.len);

        // Do not allow duplicate messages to trigger multiple passes through a state transition:
        if (messages[message.header.replica]) |m| {
            // Assert that this truly is a duplicate message and not a different message:
            // TODO Review that all message fields are compared for equality:
            assert(m.header.command == message.header.command);
            assert(m.header.replica == message.header.replica);
            assert(m.header.view == message.header.view);
            assert(m.header.op == message.header.op);
            assert(m.header.commit == message.header.commit);
            assert(m.header.latest_normal_view == message.header.latest_normal_view);
            log.debug(
                "{}: on_{}: ignoring (duplicate message)",
                .{ self.replica, @tagName(message.header.command) },
            );
            return null;
        }

        // Record the first receipt of this message:
        assert(messages[message.header.replica] == null);
        messages[message.header.replica] = message;
        assert(message.references == 1);
        message.references += 1;

        // Count the number of unique messages now received:
        var count: usize = 0;
        for (messages) |received, replica| {
            if (received) |m| {
                assert(m.header.command == message.header.command);
                assert(m.header.replica == replica);
                assert(m.header.view == self.view);
                switch (message.header.command) {
                    .prepare_ok => {
                        assert(m.header.latest_normal_view <= self.view);
                        assert(m.header.nonce == message.header.nonce);
                    },
                    .start_view_change => {
                        assert(m.header.latest_normal_view < self.view);
                        assert(m.header.replica != self.replica);
                    },
                    .do_view_change => {
                        assert(m.header.latest_normal_view < self.view);
                    },
                    else => unreachable,
                }
                count += 1;
            }
        }
        log.debug("{}: on_{}: {} message(s)", .{ self.replica, @tagName(message.header.command), count });

        // Wait until we have exactly `threshold` messages for quorum:
        if (count < threshold) {
            log.debug("{}: on_{}: waiting for quorum", .{ self.replica, @tagName(message.header.command) });
            return null;
        }

        // This is not the first time we have had quorum, the state transition has already happened:
        if (count > threshold) {
            log.debug("{}: on_{}: ignoring (quorum received already)", .{ self.replica, @tagName(message.header.command) });
            return null;
        }

        assert(count == threshold);
        return count;
    }

    fn reset_quorum_counter(self: *Replica, messages: []?*Message, command: Command) void {
        var count: usize = 0;
        for (messages) |*received, replica| {
            if (received.*) |message| {
                assert(message.header.command == command);
                assert(message.header.replica == replica);
                assert(message.header.view <= self.view);
                message.references -= 1;
                self.message_bus.gc(message);
                count += 1;
            }
            received.* = null;
        }
        log.debug("{}: reset {} {} message(s)", .{ self.replica, count, @tagName(command) });
    }

    fn commit_ops_through(self: *Replica, commit: u64) void {
        // TODO Wait until our journal chain from self.commit to self.op is completely connected.
        // This will serve as another defense against not removing ops after a view jump.

        // We may receive commit messages for ops we don't yet have:
        // Even a naive state transfer may fail to correct for this.
        while (self.commit < commit and self.commit < self.op) {
            self.commit += 1;
            assert(self.commit <= self.op);
            // Find operation in Journal:
            // TODO Journal should have a fast path where current operation is already in memory.
            // var entry = self.journal.find(self.commit) orelse @panic("operation not found in log");

            // TODO Apply to State Machine:
            log.debug("{}: executing op {}", .{ self.replica, self.commit });

            // TODO Add reply to the client table to answer future duplicate requests idempotently.
            // Lookup client table entry using client id.
            // If client's last request id is <= this request id, then update client table entry.
            // Otherwise the client is already ahead of us, and we don't need to update the entry.

            // TODO Now that the reply is in the client table, trigger this message to be sent.
            // TODO Do not reply to client if we are a follower.
        }
    }

    fn send_header_to_other_replicas(self: *Replica, header: Header) void {
        for (self.configuration) |_, replica| {
            if (replica != self.replica) {
                self.send_header_to_replica(@intCast(u16, replica), header);
            }
        }
    }

    // TODO Work out the maximum number of messages a replica may output per tick() or on_message().
    fn send_header_to_replica(self: *Replica, replica: u16, header: Header) void {
        log.debug("{}: sending {} to replica {}: {}", .{
            self.replica,
            @tagName(header.command),
            replica,
            header,
        });
        assert(header.replica == self.replica);
        assert(header.view == self.view);

        self.message_bus.send_header_to_replica(replica, header);
    }

    fn send_message_to_replica(self: *Replica, replica: u16, message: *Message) void {
        log.debug("{}: sending {} to replica {}: {}", .{
            self.replica,
            @tagName(message.header.command),
            replica,
            message.header,
        });
        // We may forward messages sent by another replica (e.g. prepares from the leader).
        // We therefore do not assert message.header.replica as we do for send_header_to_replica().
        assert(self.status == .normal);
        assert(message.header.view == self.view);
        assert(message.header.command == .prepare);

        self.message_bus.send_message_to_replica(replica, message);
    }
};

pub fn main() !void {
    assert(@sizeOf(Header) == 128);

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    var allocator = &arena.allocator;

    const f = 1;
    const entries_max = 10000;

    var configuration: [3]ConfigurationAddress = undefined;
    var message_bus = try MessageBus.init(allocator, &configuration);

    var journals: [2 * f + 1]Journal = undefined;
    for (journals) |*journal| {
        journal.* = try Journal.init(allocator, entries_max);
    }

    var state_machines: [2 * f + 1]StateMachine = undefined;
    for (state_machines) |*state_machine| {
        state_machine.* = try StateMachine.init(allocator);
    }

    var replicas: [2 * f + 1]Replica = undefined;
    for (replicas) |*replica, index| {
        replica.* = try Replica.init(
            allocator,
            f,
            &configuration,
            &message_bus,
            &journals[index],
            &state_machines[index],
            @intCast(u16, index),
        );
        configuration[index] = .{ .replica = replica };
        std.debug.print("{}\n", .{replica});
    }

    var ticks: usize = 0;
    while (true) : (ticks += 1) {
        message_bus.send_queued_messages();

        std.debug.print("\n", .{});
        log.debug("ticking (message_bus has {} messages allocated)", .{message_bus.allocated});
        std.debug.print("\n", .{});

        var leader: ?*Replica = null;
        for (replicas) |*replica| {
            if (replica.status == .normal and replica.leader()) leader = replica;
            replica.tick();
        }

        if (leader) |replica| {
            if (ticks == 1) {
                var request = message_bus.create_message(@sizeOf(Header)) catch unreachable;
                request.header.* = .{
                    .client_id = 1,
                    .request_number = ticks,
                    .command = .request,
                    .operation = .noop,
                };
                request.header.set_checksum_data(request.buffer[0..0]);
                request.header.set_checksum_meta();

                message_bus.send_message_to_replica(replica.replica, request);
            }
        }

        message_bus.send_queued_messages();
        std.time.sleep(std.time.ns_per_s);
    }
}
