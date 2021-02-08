const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const log = std.log.scoped(.vr);
pub const log_level: std.log.Level = .debug;

// TODO Command to enable client to fetch its latest request_number from the cluster.
pub const Command = packed enum(u16) {
    request,
    prepare,
    prepare_ok,
    reply,

    commit,

    request_state_transfer,
    state_transfer,

    start_view_change,
    do_view_change,
    start_view,
};

const ConfigurationAddress = union(enum) {
    address: *std.net.Address,
    replica: *Replica,
};

pub const Message = packed struct {
    checksum_meta: u128 = undefined,
    checksum_data: u128 = undefined,

    /// The checksum_meta of the message to which this message refers, or a recovery nonce:
    nonce: u128 = 0,

    client_id: u128 = 0,
    request_number: u64 = 0,

    /// The cluster reconfiguration epoch number (for future use):
    epoch: u64 = 0,

    /// Every message sent from one replica to another contains the sender's current view:
    view: u64,

    /// The latest view for which the replica's status was .normal:
    latest_normal_view: u64 = 0,

    /// The op number:
    op: u64 = 0,

    /// The commit number:
    commit: u64 = 0,

    /// The journal offset to which this message relates:
    offset: u64 = 0,

    /// The size of this message header plus additional data if any:
    size: u32 = @sizeOf(Message),

    /// The index of the replica that sent this message (0 for clients):
    replica: u16,

    command: Command,
};

const Client = struct {};

pub const MessageBus = struct {
    allocator: *Allocator,
    configuration: []ConfigurationAddress,

    // TODO Add support for remote connections.

    pub const AddressTag = enum { client, replica };
    pub const Address = union(AddressTag) {
        client: *Client,
        replica: *Replica,
    };

    pub fn init(allocator: *Allocator, configuration: []ConfigurationAddress) !MessageBus {
        return MessageBus{
            .allocator = allocator,
            .configuration = configuration,
        };
    }

    pub fn send_message(self: *MessageBus, address: *Address, message: Message) void {
        switch (address) {
            .replica => |*replica| replica.on_message(buffer),
            .client => |*client| client.on_message(buffer),
        }
    }

    pub fn send_data(
        self: *MessageBus,
        address: *Address,
        message: Message,
        data: []const u8,
    ) void {}

    pub fn send_buffer(self: *MessageBus, address: *Address, buffer: []const u8) void {
        switch (address) {
            .replica => |*replica| replica.on_message(buffer),
            .client => |*client| client.on_message(buffer),
        }
    }
};

const Status = enum {
    normal,
    view_change,
    recovering,
};

const Timeout = struct {
    after: u64,
    ticks: u64 = 0,
    ticking: bool = false,

    pub fn fired(self: *Timeout) bool {
        return self.ticking and self.ticks >= self.after;
    }

    pub fn start(self: *Timeout) void {
        self.ticks = 0;
        self.ticking = true;
    }

    pub fn stop(self: *Timeout) void {
        self.ticks = 0;
        self.ticking = false;
    }

    pub fn tick(self: *Timeout) void {
        if (self.ticking) self.ticks += 1;
    }
};

pub const Replica = struct {
    allocator: *Allocator,

    /// The maximum number of replicas that may be faulty:
    f: u32,

    /// A sorted array containing the remote or local addresses of each of the 2f + 1 replicas:
    configuration: []ConfigurationAddress,

    /// An array of outgoing messages, one for each of the 2f + 1 replicas:
    /// TODO Replace this with a MessageBus instance.
    message: [3]?Message,
    message_bus: *MessageBus,

    /// The index into the configuration where this replica's IP address is stored:
    index: u16,

    /// The current view, initially 0:
    view: u64 = 0,

    /// The current status, either normal, view_change, or recovering:
    /// TODO Do not default to normal but set the starting status according to the Journal's health.
    status: Status = .normal,

    /// The op number assigned to the most recently received request, initially 0:
    op: u64 = 0,

    /// The op number of the most recently committed operation:
    /// TODO Review that all usages handle the special starting case (where nothing is committed).
    commit: u64 = 0,

    /// Unique prepare_ok messages for the same view, op number and checksum from OTHER replicas:
    prepare_ok_from_other_replicas: [3]?Message,

    /// Unique start_view_change messages for the same view from OTHER replicas:
    start_view_change_from_other_replicas: [3]?Message,

    /// Unique do_view_change messages for the same view from ALL replicas (including ourself):
    do_view_change_from_all_replicas: [3]?Message,

    /// The number of ticks without sending a prepare before the leader sends a commit heartbeat:
    timeout_commit: Timeout = Timeout{ .after = 100 },

    /// The number of ticks without hearing from the leader before a follower starts a view change:
    timeout_view: Timeout = Timeout{ .after = 1000 },

    /// A hash representing the state of the journal:
    /// For now we're hashing fixed message values into the journal to prototype faster.
    /// TODO Replace this with a Journal instance.
    journal: [32]u8 = [_]u8{0} ** 32,

    /// For executing service up calls after an operation has been committed:
    /// TODO Replace this with a StateMachine instance.
    state_machine: bool = true,

    // TODO Limit integer types for f and index to match their upper bounds in practice.
    pub fn init(
        allocator: *Allocator,
        f: u32,
        configuration: []ConfigurationAddress,
        message_bus: *MessageBus,
        index: u16,
    ) !Replica {
        assert(configuration.len > 0);
        assert(index < configuration.len);

        var self = Replica{
            .allocator = allocator,
            .f = f,
            .configuration = configuration,
            .index = index,
            .message = undefined,
            .message_bus = message_bus,
            .prepare_ok_from_other_replicas = undefined,
            .start_view_change_from_other_replicas = undefined,
            .do_view_change_from_all_replicas = undefined,
        };
        for (self.prepare_ok_from_other_replicas) |*m| m.* = null;
        for (self.start_view_change_from_other_replicas) |*m| m.* = null;
        for (self.do_view_change_from_all_replicas) |*m| m.* = null;

        // It's important to initialize timeouts here and not in tick() for the very first tick,
        // since on_message() may race with tick() before timeouts have been initialized:
        if (self.leader()) {
            log.debug("{}: init: leader", .{self.index});

            self.timeout_commit.start();
            log.debug("{}: init: timeout_commit started", .{self.index});
        } else {
            log.debug("{}: init: follower", .{self.index});

            self.timeout_view.start();
            log.debug("{}: init: timeout_view started", .{self.index});
        }

        return self;
    }

    pub fn deinit() void {
        // TODO
    }

    pub fn follower(self: *Replica) bool {
        return !self.leader();
    }

    pub fn leader(self: *Replica) bool {
        return self.leader_index(self.view) == self.index;
    }

    // TODO Move this into a Configuration struct:
    pub fn leader_index(self: *Replica, view: u64) u32 {
        return @intCast(u32, @mod(view, self.configuration.len));
    }

    pub fn on_message(self: *Replica, message: Message) void {
        log.debug("", .{});
        log.debug("{}: on_message: {}", .{ self.index, message });
        assert(message.replica < self.configuration.len);
        switch (message.command) {
            .request => self.on_request(message),
            .prepare => self.on_prepare(message),
            .prepare_ok => self.on_prepare_ok(message),
            .start_view_change => self.on_start_view_change(message),
            .do_view_change => self.on_do_view_change(message),
            .start_view => self.on_start_view(message),
            else => unreachable,
        }
    }

    /// Time is measured in logical ticks that are incremented on every call to tick():
    /// This eliminates a dependency on the system time and enables deterministic testing.
    pub fn tick(self: *Replica) void {
        self.timeout_commit.tick();
        self.timeout_view.tick();

        if (self.timeout_commit.fired()) {
            log.debug("{}: tick: timeout_commit fired", .{self.index});
            assert(self.leader());
            assert(self.status == .normal);
            self.timeout_commit.start();
            // TODO Add more commit fields:
            self.send_message_to_other_replicas(.{
                .command = .commit,
                .replica = self.index,
                .view = self.view,
            });
        }

        if (self.timeout_view.fired()) {
            log.debug("{}: tick: timeout_view fired", .{self.index});
            assert(self.follower());
            self.transition_to_view_change_status(self.view + 1);
        }
    }

    fn on_request(self: *Replica, message: Message) void {
        if (self.status != .normal) {
            log.debug("{}: on_request: ignoring ({})", .{ self.index, self.status });
            return;
        }

        if (self.follower()) {
            // TODO Add an optimization to tell the client the view (and leader) ahead of a timeout.
            // This would also avoid thundering herds where clients suddenly broadcast the cluster.
            log.debug("{}: on_request: ignoring (follower)", .{self.index});
            return;
        }

        // TODO Check the client table to see if this is a duplicate request.
    }

    fn on_prepare(self: *Replica, message: Message) void {
        assert(message.replica == self.leader_index(message.view));
        assert(message.replica != self.index);

        if (self.status != .normal) {
            log.debug("{}: on_prepare: ignoring ({})", .{ self.index, self.status });
            return;
        }

        if (message.view < self.view) {
            log.debug("{}: on_prepare: ignoring (older view)", .{self.index});
            return;
        }

        if (message.view > self.view) {
            log.debug("{}: on_prepare: ignoring (newer view)", .{self.index});
            self.request_state_transfer();
            // TODO Queue this prepare message?
            return;
        }

        assert(message.view == self.view);

        self.timeout_view.start();
        log.debug("{}: on_prepare: timeout_view started", .{self.index});

        if (message.op > self.op + 1) {
            log.debug("{}: on_prepare: ignoring (newer op)", .{self.index});
            self.request_state_transfer();
            // TODO Queue this prepare message?
            return;
        }

        if (message.op <= self.op) {
            // TODO Sending a prepare_ok means making an important guarantee that we have the data.
            // We should add more assertions here to check that we really do have exactly what the
            // leader wants us to ack.
            log.debug("{}: on_prepare: already journalled", .{self.index});
            self.send_message_to_replica(message.replica, Message{
                .command = .prepare_ok,
                .replica = self.index,
                .view = message.view,
                .op = message.op,
            });
            return;
        }

        // Fold message value into the journal:
        // TODO Pass to a generic Journal instance.
        // TODO Handle Journal errors.
        log.debug("{}: on_prepare: journal was {}", .{ self.index, self.journal });
        var hasher = std.crypto.hash.Blake3.init(.{});
        hasher.update(self.journal[0..]);
        hasher.final(self.journal[0..]);
        log.debug("{}: on_prepare: journal now {}", .{ self.index, self.journal });

        self.op += 1;
        assert(self.op == message.op);

        // TODO Update client table.

        self.send_message_to_replica(message.replica, Message{
            .command = .prepare_ok,
            .replica = self.index,
            .view = message.view,
            .op = message.op,
        });
    }

    fn on_prepare_ok(self: *Replica, message: Message) void {
        if (self.status != .normal) {
            log.debug("{}: on_prepare_ok: ignoring ({})", .{ self.index, self.status });
            return;
        }

        if (message.view < self.view) {
            log.debug("{}: on_prepare_ok: ignoring (older view)", .{self.index});
            return;
        }

        if (message.view > self.view) {
            log.debug("{}: on_prepare_ok: ignoring (newer view)", .{self.index});
            self.request_state_transfer();
            return;
        }

        // TODO Assert that op is not newer.

        assert(message.view == self.view);
        assert(self.status == .normal);
        assert(self.leader());

        // Wait until we have `f` messages (excluding ourself) for quorum:
        // TODO Assert that we only count acks for the same {view,op}.
        const count = self.add_message_and_receive_quorum_exactly_once(
            self.prepare_ok_from_other_replicas[0..],
            message,
            self.f,
        ) orelse return;

        assert(count == self.f);
        log.debug("{}: on_prepare_ok: quorum received", .{self.index});

        self.commit_ops_through(message.op);
    }

    fn on_commit(self: *Replica, message: Message) void {
        if (self.status != .normal) {
            log.debug("{}: on_commit: ignoring ({})", .{ self.index, self.status });
            return;
        }

        if (message.view < self.view) {
            log.debug("{}: on_commit: ignoring (older view)", .{self.index});
            return;
        }

        if (message.view > self.view) {
            log.debug("{}: on_commit: ignoring (newer view)", .{self.index});
            self.request_state_transfer();
            return;
        }

        assert(message.view == self.view);
        assert(self.status == .normal);
        assert(self.leader());
        assert(self.commit <= self.op); // TODO Tie this down further if possible.

        self.timeout_view.start();

        if (message.op <= self.commit) {
            log.debug("{}: on_commit: ignoring (already committed)", .{self.index});
            return;
        }

        if (message.op > self.op) {
            log.debug("{}: on_commit: ignoring (newer op)", .{self.index});
            self.request_state_transfer();
            return;
        }

        self.commit_ops_through(message.op);
    }

    fn on_start_view_change(self: *Replica, message: Message) void {
        // A start_view_change message can only be sent to other replicas:
        assert(message.replica != self.index);

        // Any view change must be greater than zero since the initial view is already zero:
        assert(message.view > 0);

        if (message.view < self.view) {
            log.debug("{}: on_start_view_change: ignoring (older view)", .{self.index});
            return;
        }

        if (message.view == self.view and self.status != .view_change) {
            log.debug("{}: on_start_view_change: ignoring ({})", .{ self.index, self.status });
            return;
        }

        if (message.view > self.view) {
            log.debug("{}: on_start_view_change: changing to newer view", .{self.index});
            self.transition_to_view_change_status(message.view);
            // Continue below...
        }

        assert(self.view == message.view);
        assert(self.status == .view_change);

        // Wait until we have `f` messages (excluding ourself) for quorum:
        const count = self.add_message_and_receive_quorum_exactly_once(
            self.start_view_change_from_other_replicas[0..],
            message,
            self.f,
        ) orelse return;

        assert(count == self.f);
        log.debug("{}: on_start_view_change: quorum received", .{self.index});

        // When replica i receives start_view_change messages for its view from f other replicas,
        // it sends a ⟨do_view_change v, l, v’, n, k, i⟩ message to the node that will be the
        // primary in the new view. Here v is its view, l is its log, v′ is the view number of the
        // latest view in which its status was normal, n is the op number, and k is the commit
        // number.
        const new_leader = self.leader_index(self.view);
        self.send_message_to_replica(new_leader, Message{
            .command = .do_view_change,
            .replica = self.index,
            .view = self.view,
        });

        // TODO Add log, last normal view, op number, and commit number.
        // These will be used by the new leader to decide on the longest log.
    }

    fn on_do_view_change(self: *Replica, message: Message) void {
        // Any view change must be great than zero since the initial view is already zero:
        assert(message.view > 0);

        if (message.view < self.view) {
            log.debug("{}: on_do_view_change: ignoring (older view)", .{self.index});
            return;
        }

        if (message.view == self.view and self.status != .view_change) {
            log.debug("{}: on_do_view_change: ignoring ({})", .{ self.index, self.status });
            return;
        }

        if (message.view > self.view) {
            log.debug("{}: on_do_view_change: changing to newer view", .{self.index});
            // At first glance, it might seem that sending start_view_change messages out now after
            // receiving a do_view_change here would be superfluous. However, this is essential,
            // especially in the presence of asymmetric network faults. At this point, we may not
            // yet have received a do_view_change quorum, and another replica might need our
            // start_view_change message in order to get its do_view_change message through to us.
            // We therefore do not special case the start_view_change function, and we always send
            // start_view_change messages, regardless of the message that initiated the view change:
            self.transition_to_view_change_status(message.view);
            // Continue below...
        }

        assert(self.view == message.view);
        assert(self.status == .view_change);
        assert(self.leader());

        // Wait until we have `f + 1` messages (including ourself) for quorum:
        const count = self.add_message_and_receive_quorum_exactly_once(
            self.do_view_change_from_all_replicas[0..],
            message,
            self.f + 1,
        ) orelse return;

        assert(count == self.f + 1);
        log.debug("{}: on_do_view_change: quorum received", .{self.index});

        // When the new primary receives f + 1 do_view_change messages from different replicas
        // (including itself), it sets its view number to that in the messages and selects as the
        // new log the one contained in the message with the largest v′; if several messages have
        // the same v′ it selects the one among them with the largest n. It sets its op number to
        // that of the topmost entry in the new log, sets its commit number to the largest such
        // number it received in the do_view_change messages, changes its status to normal, and
        // informs the other replicas of the completion of the view change by sending
        // ⟨start_view v, l, n, k⟩ messages to the other replicas, where l is the new log, n is the
        // op number, and k is the commit number.
        var r: usize = undefined;
        var v: u64 = 0;
        var n: u64 = 0;
        var k: u64 = 0;
        for (self.do_view_change_from_all_replicas) |received, replica| {
            if (received) |m| {
                if (m.latest_normal_view > v or (m.latest_normal_view == v and m.op > n)) {
                    r = replica;
                    v = m.latest_normal_view;
                    n = m.op;
                    k = m.commit;
                }
            }
        }

        // We do not assert that v must be non-zero, because v represents the latest normal view,
        // which may be zero if this is our first view change.

        log.debug("{}: replica={} has the latest log: op={} commit={}", .{ self.index, r, n, k });

        // TODO Update Journal
        // TODO Calculate how much of the Journal to send in start_view message.

        self.transition_to_normal_status(self.view);
        assert(self.leader());

        self.op = n;
        self.commit_ops_through(k);
        assert(self.commit == k);

        // TODO Add Journal entries to start_view message:
        self.send_message_to_other_replicas(.{
            .command = .start_view,
            .replica = self.index,
            .view = self.view,
            .op = self.op,
            .commit = self.commit,
        });
    }

    fn on_start_view(self: *Replica, message: Message) void {
        assert(message.replica != self.index);
        assert(message.view > 0);

        if (message.view < self.view) {
            log.debug("{}: on_start_view: ignoring (older view)", .{self.index});
            return;
        }

        if (message.view == self.view and self.status != .view_change) {
            log.debug("{}: on_start_view: ignoring ({})", .{ self.index, self.status });
            return;
        }

        assert(self.leader_index(message.view) != self.index);

        // TODO Assert that start_view message matches what we expect if our journal is empty.
        // TODO Assert that start_view message's oldest op overlaps with our last commit number.
        // TODO Update the journal.

        self.transition_to_normal_status(message.view);

        // TODO Update our last op number according to message.

        assert(!self.leader());

        // TODO self.commit(msg.lastcommitted());
        // TODO self.send_prepare_oks(oldLastOp);
    }

    fn add_message_and_receive_quorum_exactly_once(
        self: *Replica,
        messages: []?Message,
        message: Message,
        threshold: u32,
    ) ?usize {
        assert(messages.len == self.configuration.len);

        switch (message.command) {
            .prepare_ok => assert(self.status == .normal),
            .start_view_change, .do_view_change => assert(self.status == .view_change),
            else => unreachable,
        }
        assert(message.view == self.view);
        if (message.command == .do_view_change) assert(self.leader());

        // TODO Improve this to work for "a cluster of one":
        assert(threshold >= 1);
        assert(threshold <= self.configuration.len);

        // Do not allow duplicate messages to trigger multiple passes through a state transition:
        if (messages[message.replica]) |m| {
            // Assert that this truly is a duplicate message and not a different message:
            // TODO Review that all message fields are compared for equality:
            assert(m.command == message.command);
            assert(m.replica == message.replica);
            assert(m.view == message.view);
            assert(m.op == message.op);
            assert(m.commit == message.commit);
            assert(m.latest_normal_view == message.latest_normal_view);
            log.debug(
                "{}: on_{}: ignoring (duplicate message)",
                .{ self.index, @tagName(message.command) },
            );
            return null;
        }

        // Record the first receipt of this message:
        assert(messages[message.replica] == null);
        messages[message.replica] = message;

        // Count the number of unique messages now received:
        var count: usize = 0;
        for (messages) |received, replica| {
            if (received) |m| {
                // A replica must send a do_view_change to itself if it will become the new leader:
                assert(replica != self.index or message.command == .do_view_change);
                assert(m.command == message.command);
                assert(m.replica == replica);
                assert(m.view == self.view);
                switch (message.command) {
                    .prepare_ok => assert(m.latest_normal_view <= self.view),
                    .start_view_change, .do_view_change => assert(m.latest_normal_view < self.view),
                    else => unreachable,
                }
                count += 1;
            }
        }
        log.debug("{}: on_{}: {} message(s)", .{ self.index, @tagName(message.command), count });

        // Wait until we have exactly `threshold` messages for quorum:
        if (count < threshold) {
            log.debug("{}: on_{}: waiting for quorum", .{ self.index, @tagName(message.command) });
            return null;
        }

        // This is not the first time we have had quorum, the state transition has already happened:
        if (count > threshold) {
            log.debug("{}: on_{}: ignoring (quorum received already)", .{ self.index, @tagName(message.command) });
            return null;
        }

        assert(count == threshold);
        return count;
    }

    fn commit_ops_through(self: *Replica, op: u64) void {
        while (self.commit < op) {
            self.commit += 1;

            // Find operation in Journal:
            // TODO Journal should have a fast path where current operation is already in memory.
            // var entry = self.journal.find(self.commit) orelse @panic("operation not found in log");

            // TODO Apply to State Machine:
            log.debug("{}: executing op {}", .{ self.index, self.commit });

            // var reply = Message{
            //    .replica = self.index,
            //    .view = entry.view,
            //    .op = entry.op,
            // };

            // TODO Add reply to the client table to answer future duplicate requests idempotently.
            // Lookup client table entry using client id.
            // If client's last request id is <= this request id, then update client table entry.
            // Otherwise the client is already ahead of us, and we don't need to update the entry.

            // TODO Now that the reply is in the client table, trigger this message to be sent.
            // TODO Replica.init() should accept a ClientTable instance with a send() method.

            // TODO Do not reply to client if we are a follower.
            // TODO Do we add to client table if we are a follower?
        }
    }

    fn request_state_transfer(self: *Replica) void {
        // TODO
    }

    fn send_message_to_other_replicas(self: *Replica, message: Message) void {
        for (self.message) |_, replica| {
            if (replica != self.index) {
                self.send_message_to_replica(replica, message);
            }
        }
    }

    // TODO Work out the maximum number of messages a replica may output per tick() or on_message().
    // This can then be used to allocate a fixed message buffer containing [{node_id, message}].
    // The application can then drain this message buffer after tick() or on_message() without overflow.
    // However, this introduces a copy.
    fn send_message_to_replica(self: *Replica, replica: usize, message: Message) void {
        log.debug("{}: sending {} to replica {}: {}", .{
            self.index,
            @tagName(message.command),
            replica,
            message,
        });
        assert(message.replica == self.index);
        assert(message.view == self.view);
        if (replica == self.index) {
            // Bypass the external network queue:
            // A replica should not need to keep a TCP connection open to itself.
            self.on_message(message);
        } else {
            self.message[replica] = message;
        }
    }

    fn transition_to_normal_status(self: *Replica, new_view: u64) void {
        std.debug.print("{}: transition_to_normal_status: view {}\n", .{ self.index, new_view });
        // TODO Is it possible to transition from .normal to .normal for the same view?
        assert(new_view >= self.view);
        self.view = new_view;
        self.status = .normal;

        if (self.leader()) {
            log.debug("{}: transition_to_normal_status: leader", .{self.index});

            self.timeout_commit.start();
            log.debug("{}: transition_to_normal_status: timeout_commit started", .{self.index});

            self.timeout_view.stop();
            log.debug("{}: transition_to_normal_status: timeout_view stopped", .{self.index});
        } else {
            log.debug("{}: transition_to_normal_status: follower", .{self.index});

            self.timeout_commit.stop();
            log.debug("{}: transition_to_normal_status: timeout_commit stopped", .{self.index});

            self.timeout_view.start();
            log.debug("{}: transition_to_normal_status: timeout_view started", .{self.index});

            // TODO Stop "resend prepare" timeout.
        }

        // TODO Reset prepare_ok message counters.
    }

    /// A replica i that notices the need for a view change advances its view, sets its status to
    /// view_change, and sends a ⟨start_view_change v, i⟩ message to all the other replicas,
    /// where v identifies the new view. A replica notices the need for a view change either based
    /// on its own timer, or because it receives a start_view_change or do_view_change message for
    /// a view with a larger number than its own view.
    fn transition_to_view_change_status(self: *Replica, new_view: u64) void {
        log.debug("{}: transition_to_view_change_status: view {}", .{ self.index, new_view });
        assert(new_view > self.view);
        self.view = new_view;
        self.status = .view_change;

        // Some VR implementations reset their counters only on entering a view, perhaps assuming
        // the view will be followed only by a single subsequent view change to the next view.
        // However, multiple successive view changes can fail, e.g. after a view change timeout.
        // We must therefore reset our counters here to avoid counting messages from an older view,
        // which would violate the quorum intersection property required for correctness.
        // At the same time, we want to assert that we don't clear messages relating to this view,
        // in case a message callback mistakenly adds a message before calling start_view_change().
        for (self.start_view_change_from_other_replicas) |*message| {
            if (message.*) |m| assert(m.view < new_view);
            message.* = null;
        }
        for (self.do_view_change_from_all_replicas) |*message| {
            if (message.*) |m| assert(m.view < new_view);
            message.* = null;
        }

        self.timeout_commit.stop();
        log.debug("{}: transition_to_view_change_status: timeout_commit stopped", .{self.index});

        self.timeout_view.start();
        log.debug("{}: transition_to_view_change_status: timeout_view started", .{self.index});

        // TODO Stop "resend prepare" timeout.

        // Send only to other replicas (and not to ourself) to avoid a quorum off-by-one error:
        // This could happen if the replica mistakenly counts its own message in the quorum.
        self.send_message_to_other_replicas(.{
            .command = .start_view_change,
            .replica = self.index,
            .view = new_view,
        });
    }
};

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    var allocator = &arena.allocator;

    const f = 1;
    var configuration: [3]ConfigurationAddress = undefined;
    var message_bus = try MessageBus.init(allocator, &configuration);
    var replicas: [2 * f + 1]Replica = undefined;

    for (replicas) |*replica, index| {
        replica.* = try Replica.init(
            allocator,
            f,
            &configuration,
            &message_bus,
            @intCast(u16, index),
        );
        configuration[index] = .{ .replica = replica };
        std.debug.print("{}\n", .{replica});
    }

    // Send for newer view:
    replicas[0].on_message(.{
        .replica = 1,
        .view = 7,
        .command = .start_view_change,
    });

    // Send a duplicate message:
    replicas[0].on_message(.{
        .replica = 1,
        .view = 7,
        .command = .start_view_change,
    });

    // Increase quorum:
    replicas[0].on_message(.{
        .replica = 2,
        .view = 7,
        .command = .start_view_change,
    });

    // Send an older view:
    replicas[0].on_message(.{
        .replica = 1,
        .view = 6,
        .command = .start_view_change,
    });
}
