const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;

const config = @import("../config.zig");

const Message = @import("../message_pool.zig").MessagePool.Message;
const RingBuffer = @import("../ring_buffer.zig").RingBuffer;

const vsr = @import("../vsr.zig");
const Header = vsr.Header;
const Timeout = vsr.Timeout;
const Command = vsr.Command;
const Version = vsr.Version;

const log = std.log.scoped(.replica);

pub const Status = enum {
    normal,
    view_change,
    recovering,
};

const ClientTable = std.AutoHashMapUnmanaged(u128, ClientTableEntry);

/// We found two bugs in the VRR paper relating to the client table:
///
/// 1. a correctness bug, where successive client crashes may cause request numbers to collide for
/// different request payloads, resulting in requests receiving the wrong reply, and
///
/// 2. a liveness bug, where if the client table is updated for request and prepare messages with
/// the client's latest request number, then the client may be locked out from the cluster if the
/// request is ever reordered through a view change.
///
/// We therefore take a different approach with the implementation of our client table, to:
///
/// 1. register client sessions explicitly through the state machine to ensure that client session
/// numbers always increase, and
///
/// 2. make a more careful distinction between uncommitted and committed request numbers,
/// considering that uncommitted requests may not survive a view change.
const ClientTableEntry = struct {
    /// The client's session number as committed to the cluster by a register request.
    session: u64,

    /// The reply sent to the client's latest committed request.
    reply: *Message,
};

const Prepare = struct {
    /// The current prepare message (used to cross-check prepare_ok messages, and for resending).
    message: *Message,

    /// Unique prepare_ok messages for the same view, op number and checksum from ALL replicas.
    ok_from_all_replicas: QuorumMessages = QuorumMessagesNull,

    /// Whether a quorum of prepare_ok messages has been received for this prepare.
    ok_quorum_received: bool = false,
};

const QuorumMessages = [config.replicas_max]?*Message;
const QuorumMessagesNull = [_]?*Message{null} ** config.replicas_max;

pub fn Replica(
    comptime StateMachine: type,
    comptime MessageBus: type,
    comptime Storage: type,
    comptime Time: type,
) type {
    return struct {
        const Self = @This();

        const Journal = vsr.Journal(Self, Storage);
        const Clock = vsr.Clock(Time);

        /// The number of the cluster to which this replica belongs:
        cluster: u32,

        /// The number of replicas in the cluster:
        replica_count: u8,

        /// The index of this replica's address in the configuration array held by the MessageBus:
        replica: u8,

        /// The minimum number of replicas required to form a replication quorum:
        quorum_replication: u8,

        /// The minimum number of replicas required to form a view change quorum:
        quorum_view_change: u8,

        /// A distributed fault-tolerant clock for lower and upper bounds on the leader's wall clock:
        clock: Clock,

        /// The persistent log of hash-chained journal entries:
        journal: Journal,

        /// An abstraction to send messages from the replica to another replica or client.
        /// The message bus will also deliver messages to this replica by calling `on_message()`.
        message_bus: *MessageBus,

        /// For executing service up-calls after an operation has been committed:
        state_machine: *StateMachine,

        /// The client table records for each client the latest session and the latest committed reply.
        client_table: ClientTable,

        /// The current view, initially 0:
        view: u32,

        /// The latest view, in which the replica's status was normal.
        view_normal: u32,

        /// The current status, either normal, view_change, or recovering:
        /// TODO Don't default to normal, set the starting status according to the journal's health.
        status: Status = .normal,

        /// The op number assigned to the most recently prepared operation:
        op: u64,

        /// The op number of the latest committed and executed operation (according to the replica):
        /// The replica may have to wait for repairs to complete before commit_min reaches commit_max.
        commit_min: u64,

        /// The op number of the latest committed operation (according to the cluster):
        /// This is the commit number in terms of the VRR paper.
        commit_max: u64,

        /// Whether we are reading a prepare from storage in order to commit.
        committing: bool = false,

        /// Whether we are reading a prepare from storage in order to push to the pipeline.
        repairing_pipeline: bool = false,

        /// The leader's pipeline of inflight prepares waiting to commit in FIFO order.
        /// This allows us to pipeline without the complexity of out-of-order commits.
        pipeline: RingBuffer(Prepare, config.pipelining_max) = .{},

        /// In some cases, a replica may send a message to itself. We do not submit these messages
        /// to the message bus but rather queue them here for guaranteed immediate delivery, which
        /// we require and assert in our protocol implementation.
        loopback_queue: ?*Message = null,

        /// Unique start_view_change messages for the same view from OTHER replicas (excluding ourself).
        start_view_change_from_other_replicas: QuorumMessages = QuorumMessagesNull,

        /// Unique do_view_change messages for the same view from ALL replicas (including ourself).
        do_view_change_from_all_replicas: QuorumMessages = QuorumMessagesNull,

        /// Unique nack_prepare messages for the same view from OTHER replicas (excluding ourself).
        nack_prepare_from_other_replicas: QuorumMessages = QuorumMessagesNull,

        /// Whether a replica has received a quorum of start_view_change messages for the view change:
        start_view_change_quorum: bool = false,

        /// Whether the leader has received a quorum of do_view_change messages for the view change:
        /// Determines whether the leader may effect repairs according to the CTRL protocol.
        do_view_change_quorum: bool = false,

        /// Whether the leader is expecting to receive a nack_prepare and for which op:
        nack_prepare_op: ?u64 = null,

        /// The number of ticks before a leader or follower broadcasts a ping to other replicas.
        /// TODO Explain why we need this (MessageBus handshaking, leapfrogging faulty replicas,
        /// deciding whether starting a view change would be detrimental under some network partitions).
        ping_timeout: Timeout,

        /// The number of ticks without enough prepare_ok's before the leader resends a prepare.
        prepare_timeout: Timeout,

        /// The number of ticks before the leader sends a commit heartbeat:
        /// The leader always sends a commit heartbeat irrespective of when it last sent a prepare.
        /// This improves liveness when prepare messages cannot be replicated fully due to partitions.
        commit_timeout: Timeout,

        /// The number of ticks without hearing from the leader before starting a view change.
        /// This transitions from .normal status to .view_change status.
        normal_status_timeout: Timeout,

        /// The number of ticks before a view change is timed out:
        /// This transitions from `view_change` status to `view_change` status but for a newer view.
        view_change_status_timeout: Timeout,

        /// The number of ticks before resending a `start_view_change` or `do_view_change` message:
        view_change_message_timeout: Timeout,

        /// The number of ticks before repairing missing/disconnected headers and/or dirty entries:
        repair_timeout: Timeout,

        /// Used to provide deterministic entropy to `choose_any_other_replica()`.
        /// Incremented whenever `choose_any_other_replica()` is called.
        choose_any_other_replica_ticks: u64 = 0,

        /// Used to calculate exponential backoff with random jitter.
        /// Seeded with the replica's index number.
        prng: std.rand.DefaultPrng,

        on_change_state: ?fn (replica: *Self) void = null,

        pub fn init(
            allocator: *Allocator,
            cluster: u32,
            replica_count: u8,
            replica: u8,
            time: *Time,
            storage: *Storage,
            message_bus: *MessageBus,
            state_machine: *StateMachine,
        ) !Self {
            assert(replica_count > 0);
            assert(replica < replica_count);

            const majority = (replica_count / 2) + 1;
            assert(majority <= replica_count);

            assert(config.quorum_replication_max >= 2);
            const quorum_replication = std.math.min(config.quorum_replication_max, majority);
            assert(quorum_replication >= 2 or quorum_replication == replica_count);

            const quorum_view_change = std.math.max(
                replica_count - quorum_replication + 1,
                majority,
            );
            // The view change quorum may be more expensive to make the replication quorum cheaper.
            // The insight is that the replication phase is by far more common than the view change.
            // This trade-off allows us to optimize for the common case.
            // See the comments in `config.zig` for further explanation.
            assert(quorum_view_change >= majority);

            if (replica_count <= 2) {
                assert(quorum_replication == replica_count);
                assert(quorum_view_change == replica_count);
            } else {
                assert(quorum_replication < replica_count);
                assert(quorum_view_change < replica_count);
            }

            // Flexible quorums are safe if these two quorums intersect so that this relation holds:
            assert(quorum_replication + quorum_view_change > replica_count);

            var client_table: ClientTable = .{};
            errdefer client_table.deinit(allocator);
            try client_table.ensureCapacity(allocator, @intCast(u32, config.clients_max));
            assert(client_table.capacity() >= config.clients_max);

            var init_prepare = Header{
                .parent = 0,
                .client = 0,
                .context = 0,
                .request = 0,
                .cluster = cluster,
                .epoch = 0,
                .view = 0,
                .op = 0,
                .commit = 0,
                .offset = 0,
                .size = @sizeOf(Header),
                .replica = 0,
                .command = .prepare,
                .operation = .init,
                .version = Version,
            };
            init_prepare.set_checksum_body(&[0]u8{});
            init_prepare.set_checksum();

            var self = Self{
                .cluster = cluster,
                .replica_count = replica_count,
                .replica = replica,
                .quorum_replication = quorum_replication,
                .quorum_view_change = quorum_view_change,
                .clock = try Clock.init(
                    allocator,
                    replica_count,
                    replica,
                    time,
                ),
                .journal = try Journal.init(
                    allocator,
                    storage,
                    replica,
                    config.journal_size_max,
                    config.journal_headers_max,
                    &init_prepare,
                ),
                .message_bus = message_bus,
                .state_machine = state_machine,
                .client_table = client_table,
                .view = init_prepare.view,
                .view_normal = init_prepare.view,
                .op = init_prepare.op,
                .commit_min = init_prepare.commit,
                .commit_max = init_prepare.commit,
                .ping_timeout = Timeout{
                    .name = "ping_timeout",
                    .id = replica,
                    .after = 100,
                },
                .prepare_timeout = Timeout{
                    .name = "prepare_timeout",
                    .id = replica,
                    .after = 50,
                },
                .commit_timeout = Timeout{
                    .name = "commit_timeout",
                    .id = replica,
                    .after = 100,
                },
                .normal_status_timeout = Timeout{
                    .name = "normal_status_timeout",
                    .id = replica,
                    .after = 500,
                },
                .view_change_status_timeout = Timeout{
                    .name = "view_change_status_timeout",
                    .id = replica,
                    .after = 500,
                },
                .view_change_message_timeout = Timeout{
                    .name = "view_change_message_timeout",
                    .id = replica,
                    .after = 50,
                },
                .repair_timeout = Timeout{
                    .name = "repair_timeout",
                    .id = replica,
                    .after = 50,
                },
                .prng = std.rand.DefaultPrng.init(replica),
            };

            log.debug("{}: init: replica_count={} quorum_view_change={} quorum_replication={}", .{
                self.replica,
                self.replica_count,
                self.quorum_view_change,
                self.quorum_replication,
            });

            // To reduce the probability of clustering, for efficient linear probing, the hash map will
            // always overallocate capacity by a factor of two.
            log.debug("{}: init: client_table.capacity()={} for config.clients_max={} entries", .{
                self.replica,
                self.client_table.capacity(),
                config.clients_max,
            });

            // We must initialize timeouts here, not in tick() on the first tick, because on_message()
            // can race with tick()... before timeouts have been initialized:
            assert(self.status == .normal);
            if (self.leader()) {
                log.debug("{}: init: leader", .{self.replica});
                self.ping_timeout.start();
                self.commit_timeout.start();
                self.repair_timeout.start();
            } else {
                log.debug("{}: init: follower", .{self.replica});
                self.ping_timeout.start();
                self.normal_status_timeout.start();
                self.repair_timeout.start();
            }

            return self;
        }

        /// Free all memory and unref all messages held by the replica
        /// This does not deinitialize the StateMachine, MessageBus, Storage, or Time
        pub fn deinit(self: *Self, allocator: *Allocator) void {
            self.journal.deinit(allocator);
            self.clock.deinit(allocator);

            {
                var it = self.client_table.iterator();
                while (it.next()) |entry| {
                    self.message_bus.unref(entry.value_ptr.reply);
                }
                self.client_table.deinit(allocator);
            }

            {
                var it = self.pipeline.iterator();
                while (it.next()) |prepare| {
                    self.message_bus.unref(prepare.message);
                    for (prepare.ok_from_all_replicas) |message| {
                        if (message) |m| self.message_bus.unref(m);
                    }
                }
            }

            if (self.loopback_queue) |loopback_message| {
                assert(loopback_message.next == null);
                self.message_bus.unref(loopback_message);
                self.loopback_queue = null;
            }

            for (self.start_view_change_from_other_replicas) |message| {
                if (message) |m| self.message_bus.unref(m);
            }
            for (self.do_view_change_from_all_replicas) |message| {
                if (message) |m| self.message_bus.unref(m);
            }
            for (self.nack_prepare_from_other_replicas) |message| {
                if (message) |m| self.message_bus.unref(m);
            }
        }

        /// Time is measured in logical ticks that are incremented on every call to tick().
        /// This eliminates a dependency on the system time and enables deterministic testing.
        pub fn tick(self: *Self) void {
            // Ensure that all asynchronous IO callbacks flushed the loopback queue as needed.
            // If an IO callback queues a loopback message without flushing the queue then this will
            // delay the delivery of messages (e.g. a prepare_ok from the leader to itself) and
            // decrease throughput significantly.
            assert(self.loopback_queue == null);

            self.clock.tick();

            if (!self.journal.recovered) {
                self.journal.recover();
                return;
            } else {
                assert(!self.journal.recovering);
            }

            self.ping_timeout.tick();
            self.prepare_timeout.tick();
            self.commit_timeout.tick();
            self.normal_status_timeout.tick();
            self.view_change_status_timeout.tick();
            self.view_change_message_timeout.tick();
            self.repair_timeout.tick();

            if (self.ping_timeout.fired()) self.on_ping_timeout();
            if (self.prepare_timeout.fired()) self.on_prepare_timeout();
            if (self.commit_timeout.fired()) self.on_commit_timeout();
            if (self.normal_status_timeout.fired()) self.on_normal_status_timeout();
            if (self.view_change_status_timeout.fired()) self.on_view_change_status_timeout();
            if (self.view_change_message_timeout.fired()) self.on_view_change_message_timeout();
            if (self.repair_timeout.fired()) self.on_repair_timeout();

            // None of the on_timeout() functions above should send a message to this replica.
            assert(self.loopback_queue == null);
        }

        /// Called by the MessageBus to deliver a message to the replica.
        pub fn on_message(self: *Self, message: *Message) void {
            assert(self.loopback_queue == null);

            log.debug("{}: on_message: view={} status={s} {}", .{
                self.replica,
                self.view,
                @tagName(self.status),
                message.header,
            });

            if (message.header.invalid()) |reason| {
                log.alert("{}: on_message: invalid ({s})", .{ self.replica, reason });
                return;
            }

            // No client or replica should ever send a .reserved message.
            assert(message.header.command != .reserved);

            if (message.header.cluster != self.cluster) {
                log.warn("{}: on_message: wrong cluster (cluster must be {} not {})", .{
                    self.replica,
                    self.cluster,
                    message.header.cluster,
                });
                return;
            }

            if (!self.journal.recovered) {
                self.journal.recover();
                log.debug("{}: on_message: waiting for journal to recover", .{self.replica});
                return;
            } else {
                assert(!self.journal.recovering);
            }

            assert(message.header.replica < self.replica_count);
            switch (message.header.command) {
                .ping => self.on_ping(message),
                .pong => self.on_pong(message),
                .request => self.on_request(message),
                .prepare => self.on_prepare(message),
                .prepare_ok => self.on_prepare_ok(message),
                .commit => self.on_commit(message),
                .start_view_change => self.on_start_view_change(message),
                .do_view_change => self.on_do_view_change(message),
                .start_view => self.on_start_view(message),
                .recovery => self.on_recovery(message),
                .recovery_response => return, // TODO
                .request_start_view => self.on_request_start_view(message),
                .request_prepare => self.on_request_prepare(message),
                .request_headers => self.on_request_headers(message),
                .headers => self.on_headers(message),
                .nack_prepare => self.on_nack_prepare(message),
                // A replica should never handle misdirected messages intended for a client:
                .eviction, .reply => {
                    log.warn("{}: on_message: ignoring misdirected {s} message", .{
                        self.replica,
                        @tagName(message.header.command),
                    });
                    return;
                },
                .reserved => unreachable,
            }

            if (self.loopback_queue) |loopback_message| {
                log.emerg("{}: on_message: on_{s}() queued a {s} loopback message with no flush", .{
                    self.replica,
                    @tagName(message.header.command),
                    @tagName(loopback_message.header.command),
                });
            }

            // Any message handlers that loopback must take responsibility for the flush.
            assert(self.loopback_queue == null);
        }

        fn on_ping(self: *Self, message: *const Message) void {
            if (self.status != .normal and self.status != .view_change) return;

            assert(self.status == .normal or self.status == .view_change);

            // TODO Drop pings that were not addressed to us.

            var pong = Header{
                .command = .pong,
                .cluster = self.cluster,
                .replica = self.replica,
                .view = self.view,
            };

            if (message.header.client > 0) {
                assert(message.header.replica == 0);

                // We must only ever send our view number to a client via a pong message if we are
                // in normal status. Otherwise, we may be partitioned from the cluster with a newer
                // view number, leak this to the client, which would then pass this to the cluster
                // in subsequent client requests, which would then ignore these client requests with
                // a newer view number, locking out the client. The principle here is that we must
                // never send view numbers for views that have not yet started.
                if (self.status == .normal) {
                    self.send_header_to_client(message.header.client, pong);
                }
            } else if (message.header.replica == self.replica) {
                log.warn("{}: on_ping: ignoring (self)", .{self.replica});
            } else {
                // Copy the ping's monotonic timestamp to our pong and add our wall clock sample:
                pong.op = message.header.op;
                pong.offset = @bitCast(u64, self.clock.realtime());
                self.send_header_to_replica(message.header.replica, pong);
            }
        }

        fn on_pong(self: *Self, message: *const Message) void {
            if (message.header.client > 0) return;
            if (message.header.replica == self.replica) return;

            const m0 = message.header.op;
            const t1 = @bitCast(i64, message.header.offset);
            const m2 = self.clock.monotonic();

            self.clock.learn(message.header.replica, m0, t1, m2);
        }

        /// The primary advances op-number, adds the request to the end of the log, and updates the
        /// information for this client in the client-table to contain the new request number, s.
        /// Then it sends a ⟨PREPARE v, m, n, k⟩ message to the other replicas, where v is the current
        /// view-number, m is the message it received from the client, n is the op-number it assigned to
        /// the request, and k is the commit-number.
        fn on_request(self: *Self, message: *Message) void {
            if (self.ignore_request_message(message)) return;

            assert(self.status == .normal);
            assert(self.leader());
            assert(self.commit_min == self.commit_max);
            assert(self.commit_max + self.pipeline.count == self.op);

            assert(message.header.command == .request);
            assert(message.header.view <= self.view); // The client's view may be behind ours.

            const realtime = self.clock.realtime_synchronized() orelse {
                log.alert("{}: on_request: dropping (clock not synchronized)", .{self.replica});
                return;
            };

            log.debug("{}: on_request: request {}", .{ self.replica, message.header.checksum });

            self.state_machine.prepare(
                realtime,
                message.header.operation.cast(StateMachine),
                message.body(),
            );

            var latest_entry = self.journal.entry_for_op_exact(self.op).?;
            message.header.parent = latest_entry.checksum;
            message.header.context = message.header.checksum;
            message.header.view = self.view;
            message.header.op = self.op + 1;
            message.header.commit = self.commit_max;
            message.header.offset = self.journal.next_offset(latest_entry);
            message.header.replica = self.replica;
            message.header.command = .prepare;

            message.header.set_checksum_body(message.body());
            message.header.set_checksum();

            log.debug("{}: on_request: prepare {}", .{ self.replica, message.header.checksum });

            self.pipeline.push(.{ .message = message.ref() }) catch unreachable;
            assert(self.pipeline.count >= 1);

            if (self.pipeline.count == 1) {
                // This is the only prepare in the pipeline, start the timeout:
                assert(!self.prepare_timeout.ticking);
                self.prepare_timeout.start();
            } else {
                // Do not restart the prepare timeout as it is already ticking for another prepare.
                assert(self.prepare_timeout.ticking);
            }

            self.on_prepare(message);

            // We expect `on_prepare()` to increment `self.op` to match the leader's latest prepare:
            // This is critical to ensure that pipelined prepares do not receive the same op number.
            assert(self.op == message.header.op);
        }

        /// Replication is simple, with a single code path for the leader and followers.
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
        /// At the same time, asynchronous replication keeps going, so that if our local disk is slow,
        /// then any latency spike will be masked by more remote prepare_ok messages as they come in.
        /// This gives automatic tail latency tolerance for storage latency spikes.
        ///
        /// The remaining problem then is tail latency tolerance for network latency spikes.
        /// If the next replica is down or partitioned, then the leader's prepare timeout will fire,
        /// and the leader will resend but to another replica, until it receives enough prepare_ok's.
        fn on_prepare(self: *Self, message: *Message) void {
            self.view_jump(message.header);

            if (self.is_repair(message)) {
                log.debug("{}: on_prepare: ignoring (repair)", .{self.replica});
                self.on_repair(message);
                return;
            }

            if (self.status != .normal) {
                log.debug("{}: on_prepare: ignoring ({})", .{ self.replica, self.status });
                return;
            }

            if (message.header.view < self.view) {
                log.debug("{}: on_prepare: ignoring (older view)", .{self.replica});
                return;
            }

            if (message.header.view > self.view) {
                log.debug("{}: on_prepare: ignoring (newer view)", .{self.replica});
                return;
            }

            assert(self.status == .normal);
            assert(message.header.view == self.view);
            assert(self.leader() or self.follower());
            assert(message.header.replica == self.leader_index(message.header.view));
            assert(message.header.op > self.op);
            assert(message.header.op > self.commit_min);

            if (self.follower()) self.normal_status_timeout.reset();

            if (message.header.op > self.op + 1) {
                log.debug("{}: on_prepare: newer op", .{self.replica});
                self.jump_to_newer_op_in_normal_status(message.header);
            }

            if (self.journal.previous_entry(message.header)) |previous| {
                // Any previous entry may be a whole journal's worth of ops behind due to wrapping.
                // We therefore do not do any further op, offset or checksum assertions beyond this:
                self.panic_if_hash_chain_would_break_in_the_same_view(previous, message.header);
            }

            // We must advance our op and set the header as dirty before replicating and journalling.
            // The leader needs this before its journal is outrun by any prepare_ok quorum:
            log.debug("{}: on_prepare: advancing: op={}..{} checksum={}..{}", .{
                self.replica,
                self.op,
                message.header.op,
                message.header.parent,
                message.header.checksum,
            });
            assert(message.header.op == self.op + 1);
            self.op = message.header.op;
            self.journal.set_entry_as_dirty(message.header);

            self.replicate(message);
            self.append(message);

            if (self.follower()) {
                // A prepare may already be committed if requested by repair() so take the max:
                self.commit_ops(std.math.max(message.header.commit, self.commit_max));
                assert(self.commit_max >= message.header.commit);
            }
        }

        fn on_prepare_ok(self: *Self, message: *Message) void {
            if (self.ignore_prepare_ok(message)) return;

            assert(self.status == .normal);
            assert(message.header.view == self.view);
            assert(self.leader());

            const prepare = self.pipeline_prepare_for_prepare_ok(message) orelse return;

            assert(prepare.message.header.checksum == message.header.context);
            assert(prepare.message.header.op >= self.commit_max + 1);
            assert(prepare.message.header.op <= self.commit_max + self.pipeline.count);
            assert(prepare.message.header.op <= self.op);

            // Wait until we have `f + 1` prepare_ok messages (including ourself) for quorum:
            const threshold = self.quorum_replication;

            const count = self.add_message_and_receive_quorum_exactly_once(
                &prepare.ok_from_all_replicas,
                message,
                threshold,
            ) orelse return;

            assert(count == threshold);
            assert(!prepare.ok_quorum_received);
            prepare.ok_quorum_received = true;

            log.debug("{}: on_prepare_ok: quorum received, context={}", .{
                self.replica,
                prepare.message.header.checksum,
            });

            self.commit_pipeline();
        }

        /// Known issue:
        /// TODO The leader should stand down if it sees too many retries in on_prepare_timeout().
        /// It's possible for the network to be one-way partitioned so that followers don't see the
        /// leader as down, but neither can the leader hear from the followers.
        fn on_commit(self: *Self, message: *const Message) void {
            self.view_jump(message.header);

            if (self.status != .normal) {
                log.debug("{}: on_commit: ignoring ({})", .{ self.replica, self.status });
                return;
            }

            if (message.header.view < self.view) {
                log.debug("{}: on_commit: ignoring (older view)", .{self.replica});
                return;
            }

            if (message.header.view > self.view) {
                log.debug("{}: on_commit: ignoring (newer view)", .{self.replica});
                return;
            }

            if (self.leader()) {
                log.warn("{}: on_commit: ignoring (leader)", .{self.replica});
                return;
            }

            assert(self.status == .normal);
            assert(self.follower());
            assert(message.header.view == self.view);
            assert(message.header.replica == self.leader_index(message.header.view));

            // We may not always have the latest commit entry but if we do our checksum must match:
            if (self.journal.entry_for_op_exact(message.header.commit)) |commit_entry| {
                if (commit_entry.checksum == message.header.context) {
                    log.debug("{}: on_commit: checksum verified", .{self.replica});
                } else if (self.valid_hash_chain("on_commit")) {
                    @panic("commit checksum verification failed");
                } else {
                    // We may still be repairing after receiving the start_view message.
                    log.debug("{}: on_commit: skipping checksum verification", .{self.replica});
                }
            }

            self.normal_status_timeout.reset();

            self.commit_ops(message.header.commit);
        }

        fn on_repair(self: *Self, message: *Message) void {
            assert(message.header.command == .prepare);

            if (self.status != .normal and self.status != .view_change) {
                log.debug("{}: on_repair: ignoring ({})", .{ self.replica, self.status });
                return;
            }

            if (message.header.view > self.view) {
                log.debug("{}: on_repair: ignoring (newer view)", .{self.replica});
                return;
            }

            if (self.status == .view_change and message.header.view == self.view) {
                log.debug("{}: on_repair: ignoring (view started)", .{self.replica});
                return;
            }

            if (self.status == .view_change and self.leader_index(self.view) != self.replica) {
                log.debug("{}: on_repair: ignoring (view change, follower)", .{self.replica});
                return;
            }

            if (self.status == .view_change and !self.do_view_change_quorum) {
                log.debug("{}: on_repair: ignoring (view change, waiting for quorum)", .{self.replica});
                return;
            }

            if (message.header.op > self.op) {
                assert(message.header.view < self.view);
                log.debug("{}: on_repair: ignoring (would advance self.op)", .{self.replica});
                return;
            }

            assert(self.status == .normal or self.status == .view_change);
            assert(self.repairs_allowed());
            assert(message.header.view <= self.view);
            assert(message.header.op <= self.op); // Repairs may never advance `self.op`.

            if (self.journal.has_clean(message.header)) {
                log.debug("{}: on_repair: ignoring (duplicate)", .{self.replica});
                self.send_prepare_ok(message.header);
                defer self.flush_loopback_queue();
                return;
            }

            if (self.repair_header(message.header)) {
                assert(self.journal.has_dirty(message.header));

                if (self.nack_prepare_op) |nack_prepare_op| {
                    if (nack_prepare_op == message.header.op) {
                        log.debug("{}: on_repair: repairing uncommitted op={}", .{
                            self.replica,
                            message.header.op,
                        });
                        self.reset_quorum_nack_prepare();
                    }
                }

                log.debug("{}: on_repair: repairing journal", .{self.replica});
                self.write_prepare(message, .repair);
            }
        }

        fn on_start_view_change(self: *Self, message: *Message) void {
            if (self.ignore_view_change_message(message)) return;

            assert(self.status == .normal or self.status == .view_change);
            assert(message.header.view >= self.view);
            assert(message.header.replica != self.replica);

            self.view_jump(message.header);

            assert(self.status == .view_change);
            assert(message.header.view == self.view);

            if (self.leader_index(self.view) == self.replica) {
                // If we are the leader of the new view, then wait until we have a message to send a
                // do_view_change message to ourself. The on_do_view_change() handler will panic if
                // we received a start_view_change quorum without a do_view_change to ourself.
                if (self.message_bus.get_message()) |available| {
                    self.message_bus.unref(available);
                } else {
                    log.alert("{}: on_start_view_change: waiting for message for do_view_change", .{
                        self.replica,
                    });
                    return;
                }
            }

            // Wait until we have `f` messages (excluding ourself) for quorum:
            assert(self.replica_count > 1);
            const threshold = self.quorum_view_change - 1;

            const count = self.add_message_and_receive_quorum_exactly_once(
                &self.start_view_change_from_other_replicas,
                message,
                threshold,
            ) orelse return;

            assert(count == threshold);
            assert(self.start_view_change_from_other_replicas[self.replica] == null);
            log.debug("{}: on_start_view_change: view={} quorum received", .{
                self.replica,
                self.view,
            });

            assert(!self.start_view_change_quorum);
            assert(!self.do_view_change_quorum);
            self.start_view_change_quorum = true;

            // When replica i receives start_view_change messages for its view from f other replicas,
            // it sends a ⟨do_view_change v, l, v’, n, k, i⟩ message to the node that will be the
            // primary in the new view. Here v is its view, l is its log, v′ is the view number of the
            // latest view in which its status was normal, n is the op number, and k is the commit
            // number.
            self.send_do_view_change();
            defer self.flush_loopback_queue();
        }

        /// When the new primary receives f + 1 do_view_change messages from different replicas
        /// (including itself), it sets its view number to that in the messages and selects as the
        /// new log the one contained in the message with the largest v′; if several messages have
        /// the same v′ it selects the one among them with the largest n. It sets its op number to
        /// that of the topmost entry in the new log, sets its commit number to the largest such
        /// number it received in the do_view_change messages, changes its status to normal, and
        /// informs the other replicas of the completion of the view change by sending
        /// ⟨start_view v, l, n, k⟩ messages to the other replicas, where l is the new log, n is the
        /// op number, and k is the commit number.
        fn on_do_view_change(self: *Self, message: *Message) void {
            if (self.ignore_view_change_message(message)) return;

            assert(self.status == .normal or self.status == .view_change);
            assert(message.header.view >= self.view);
            assert(self.leader_index(message.header.view) == self.replica);

            self.view_jump(message.header);

            assert(self.status == .view_change);
            assert(message.header.view == self.view);

            // We may receive a `do_view_change` quorum from other replicas, which already have a
            // `start_view_change_quorum`, before we receive a `start_view_change_quorum`:
            if (!self.start_view_change_quorum) {
                log.debug("{}: on_do_view_change: waiting for start_view_change quorum", .{
                    self.replica,
                });
                return;
            }

            // Wait until we have `f + 1` messages (including ourself) for quorum:
            assert(self.replica_count > 1);
            const threshold = self.quorum_view_change;

            const count = self.add_message_and_receive_quorum_exactly_once(
                &self.do_view_change_from_all_replicas,
                message,
                threshold,
            ) orelse return;

            assert(count == threshold);
            assert(self.do_view_change_from_all_replicas[self.replica] != null);
            log.debug("{}: on_do_view_change: view={} quorum received", .{
                self.replica,
                self.view,
            });

            var v: ?u32 = null;
            var k: ?u64 = null;
            var latest = Header.reserved();

            for (self.do_view_change_from_all_replicas) |received, replica| {
                if (received) |m| {
                    assert(m.header.command == .do_view_change);
                    assert(m.header.cluster == self.cluster);
                    assert(m.header.replica == replica);
                    assert(m.header.view == self.view);

                    // The latest normal view experienced by this replica:
                    // This may be higher than the view in any of the prepare headers.
                    var replica_view_normal = @intCast(u32, m.header.offset);
                    assert(replica_view_normal < m.header.view);

                    var replica_latest = Header.reserved();
                    self.set_latest_op(self.message_body_as_headers(m), &replica_latest);
                    assert(replica_latest.op == m.header.op);

                    log.debug(
                        "{}: on_do_view_change: replica={} v'={} op={} commit={} latest={}",
                        .{
                            self.replica,
                            m.header.replica,
                            replica_view_normal,
                            m.header.op,
                            m.header.commit,
                            replica_latest,
                        },
                    );

                    if (v == null or replica_view_normal > v.?) {
                        v = replica_view_normal;
                        latest = replica_latest;
                    } else if (replica_view_normal == v.? and replica_latest.op > latest.op) {
                        v = replica_view_normal;
                        latest = replica_latest;
                    }

                    if (k == null or m.header.commit > k.?) k = m.header.commit;
                }
            }

            self.set_latest_op_and_k(&latest, k.?, "on_do_view_change");

            // Now that we have the latest op in place, repair any other headers:
            for (self.do_view_change_from_all_replicas) |received| {
                if (received) |m| {
                    for (self.message_body_as_headers(m)) |*h| {
                        _ = self.repair_header(h);
                    }
                }
            }

            // Verify that the repairs above have not replaced or advanced the latest op:
            assert(self.journal.entry_for_op_exact(self.op).?.checksum == latest.checksum);

            assert(self.start_view_change_quorum);
            assert(!self.do_view_change_quorum);
            self.do_view_change_quorum = true;

            self.discard_uncommitted_headers();
            assert(self.op >= self.commit_max);
            assert(self.journal.entry_for_op_exact(self.op) != null);

            // Start repairs according to the CTRL protocol:
            assert(!self.repair_timeout.ticking);
            self.repair_timeout.start();
            self.repair();
        }

        /// When other replicas receive the start_view message, they replace their log with the one
        /// in the message, set their op number to that of the latest entry in the log, set their
        /// view number to the view number in the message, change their status to normal, and update
        /// the information in their client table. If there are non-committed operations in the log,
        /// they send a ⟨prepare_ok v, n, i⟩ message to the primary; here n is the op-number. Then
        /// they execute all operations known to be committed that they haven’t executed previously,
        /// advance their commit number, and update the information in their client table.
        fn on_start_view(self: *Self, message: *const Message) void {
            if (self.ignore_view_change_message(message)) return;

            assert(self.status == .view_change or self.status == .normal);
            assert(message.header.view >= self.view);
            assert(message.header.replica != self.replica);
            assert(message.header.replica == self.leader_index(message.header.view));

            self.view_jump(message.header);

            assert(self.status == .view_change);
            assert(message.header.view == self.view);

            var latest = Header.reserved();
            self.set_latest_op(self.message_body_as_headers(message), &latest);
            assert(latest.op == message.header.op);

            self.set_latest_op_and_k(&latest, message.header.commit, "on_start_view");

            // Now that we have the latest op in place, repair any other headers:
            for (self.message_body_as_headers(message)) |*h| {
                _ = self.repair_header(h);
            }

            // Verify that the repairs above have not replaced or advanced the latest op:
            assert(self.journal.entry_for_op_exact(self.op).?.checksum == latest.checksum);

            if (self.status == .view_change) {
                self.transition_to_normal_status(message.header.view);
                self.send_prepare_oks_after_view_change();
            }

            assert(self.status == .normal);
            assert(message.header.view == self.view);
            assert(self.follower());

            self.commit_ops(self.commit_max);

            self.repair();
        }

        fn on_request_start_view(self: *Self, message: *const Message) void {
            if (self.ignore_repair_message(message)) return;

            assert(self.status == .normal);
            assert(message.header.view == self.view);
            assert(message.header.replica != self.replica);
            assert(self.leader());

            const start_view = self.create_view_change_message(.start_view) orelse {
                log.alert("{}: on_request_start_view: dropping start_view, no message available", .{
                    self.replica,
                });
                return;
            };
            defer self.message_bus.unref(start_view);

            assert(start_view.references == 1);
            assert(start_view.header.command == .start_view);
            assert(start_view.header.view == self.view);
            assert(start_view.header.op == self.op);
            assert(start_view.header.commit == self.commit_max);

            self.send_message_to_replica(message.header.replica, start_view);
        }

        /// TODO This is a work in progress (out of scope for the bounty)
        fn on_recovery(self: *Self, message: *const Message) void {
            if (self.status != .normal) {
                log.debug("{}: on_recovery: ignoring ({})", .{ self.replica, self.status });
                return;
            }

            if (message.header.replica == self.replica) {
                log.warn("{}: on_recovery: ignoring (self)", .{self.replica});
                return;
            }

            const response = self.message_bus.get_message() orelse {
                log.alert("{}: on_recovery: ignoring (waiting for message)", .{self.replica});
                return;
            };
            defer self.message_bus.unref(response);

            response.header.* = .{
                .command = .recovery_response,
                .cluster = self.cluster,
                .context = message.header.context,
                .replica = self.replica,
                .view = self.view,
                .op = self.op,
                .commit = self.commit_max,
            };

            const count_max = 8; // The number of prepare headers to include in the body.

            const size_max = @sizeOf(Header) * std.math.min(
                std.math.max(@divFloor(response.buffer.len, @sizeOf(Header)), 2),
                1 + count_max,
            );
            assert(size_max > @sizeOf(Header));

            const count = self.journal.copy_latest_headers_between(
                0,
                self.op,
                std.mem.bytesAsSlice(Header, response.buffer[@sizeOf(Header)..size_max]),
            );

            // We expect that self.op always exists.
            assert(count > 0);

            response.header.size = @intCast(u32, @sizeOf(Header) + @sizeOf(Header) * count);

            response.header.set_checksum_body(response.body());
            response.header.set_checksum();

            assert(self.status == .normal);
            // The checksum for a recovery message is deterministic, and cannot be used as a nonce:
            assert(response.header.context != message.header.checksum);

            self.send_message_to_replica(message.header.replica, response);
        }

        /// TODO This is a work in progress (out of scope for the bounty)
        fn on_recovery_response(self: *Self, message: *Message) void {}

        fn on_request_prepare(self: *Self, message: *const Message) void {
            if (self.ignore_repair_message(message)) return;

            assert(self.status == .normal or self.status == .view_change);
            assert(message.header.view == self.view);
            assert(message.header.replica != self.replica);

            const op = message.header.op;
            var checksum: ?u128 = message.header.context;
            if (self.leader_index(self.view) == self.replica and checksum.? == 0) checksum = null;

            if (self.journal.entry_for_op_exact_with_checksum(op, checksum)) |entry| {
                assert(entry.op == op);
                assert(checksum == null or entry.checksum == checksum.?);

                if (!self.journal.dirty.bit(op)) {
                    assert(!self.journal.faulty.bit(op));

                    log.debug("{}: on_request_prepare: op={} checksum={} reading", .{
                        self.replica,
                        op,
                        checksum,
                    });

                    // TODO Do not reissue the read if we are already reading in order to send to
                    // this particular destination replica.

                    self.journal.read_prepare(
                        on_request_prepare_read,
                        op,
                        entry.checksum,
                        message.header.replica,
                    );

                    // We have guaranteed the prepare and our copy is clean (not safe to nack).
                    return;
                } else if (self.journal.faulty.bit(op)) {
                    log.debug("{}: on_request_prepare: op={} checksum={} faulty", .{
                        self.replica,
                        op,
                        checksum,
                    });

                    // We have gauranteed the prepare but our copy is faulty (not safe to nack).
                    return;
                }

                // We know of the prepare but we have yet to write or guarantee it (safe to nack).
                // Continue through below...
            }

            if (self.status == .view_change) {
                assert(message.header.replica == self.leader_index(self.view));
                assert(checksum != null);
                if (self.journal.entry_for_op_exact_with_checksum(op, checksum) != null) {
                    assert(self.journal.dirty.bit(op) and !self.journal.faulty.bit(op));
                }

                log.debug("{}: on_request_prepare: op={} checksum={} nacking", .{
                    self.replica,
                    op,
                    checksum,
                });

                self.send_header_to_replica(message.header.replica, .{
                    .command = .nack_prepare,
                    .context = checksum.?,
                    .cluster = self.cluster,
                    .replica = self.replica,
                    .view = self.view,
                    .op = op,
                });
            }
        }

        fn on_request_prepare_read(self: *Self, prepare: ?*Message, destination_replica: ?u8) void {
            const message = prepare orelse {
                log.debug("{}: on_request_prepare_read: prepare=null", .{self.replica});
                return;
            };

            log.debug("{}: on_request_prepare_read: op={} checksum={} sending to replica={}", .{
                self.replica,
                message.header.op,
                message.header.checksum,
                destination_replica.?,
            });

            assert(destination_replica.? != self.replica);
            self.send_message_to_replica(destination_replica.?, message);
        }

        fn on_request_headers(self: *Self, message: *const Message) void {
            if (self.ignore_repair_message(message)) return;

            assert(self.status == .normal or self.status == .view_change);
            assert(message.header.view == self.view);
            assert(message.header.replica != self.replica);

            const response = self.message_bus.get_message() orelse {
                log.alert("{}: on_request_headers: ignoring (op={}..{}, no message available)", .{
                    self.replica,
                    message.header.commit,
                    message.header.op,
                });
                return;
            };
            defer self.message_bus.unref(response);

            response.header.* = .{
                .command = .headers,
                // We echo the context back to the replica so that they can match up our response:
                .context = message.header.context,
                .cluster = self.cluster,
                .replica = self.replica,
                .view = self.view,
            };

            const op_min = message.header.commit;
            const op_max = message.header.op;
            assert(op_max >= op_min);

            // We must add 1 because op_max and op_min are both inclusive:
            const count_max = @intCast(u32, std.math.min(64, op_max - op_min + 1));
            assert(count_max > 0);

            const size_max = @sizeOf(Header) * std.math.min(
                std.math.max(@divFloor(message.buffer.len, @sizeOf(Header)), 2),
                1 + count_max,
            );
            assert(size_max > @sizeOf(Header));

            const count = self.journal.copy_latest_headers_between(
                op_min,
                op_max,
                std.mem.bytesAsSlice(Header, response.buffer[@sizeOf(Header)..size_max]),
            );

            if (count == 0) {
                log.debug("{}: on_request_headers: ignoring (op={}..{}, no headers)", .{
                    self.replica,
                    op_min,
                    op_max,
                });
                return;
            }

            response.header.size = @intCast(u32, @sizeOf(Header) + @sizeOf(Header) * count);

            response.header.set_checksum_body(response.body());
            response.header.set_checksum();

            self.send_message_to_replica(message.header.replica, response);
        }

        fn on_nack_prepare(self: *Self, message: *Message) void {
            if (self.ignore_repair_message(message)) return;

            assert(self.status == .view_change);
            assert(message.header.view == self.view);
            assert(message.header.replica != self.replica);
            assert(self.leader_index(self.view) == self.replica);
            assert(self.do_view_change_quorum);
            assert(self.repairs_allowed());

            if (self.nack_prepare_op == null) {
                log.debug("{}: on_nack_prepare: ignoring (no longer expected)", .{self.replica});
                return;
            }

            const op = self.nack_prepare_op.?;
            const checksum = self.journal.entry_for_op_exact(op).?.checksum;

            if (message.header.op != op) {
                log.debug("{}: on_nack_prepare: ignoring (repairing another op)", .{self.replica});
                return;
            }

            if (message.header.context != checksum) {
                log.debug("{}: on_nack_prepare: ignoring (repairing another checksum)", .{
                    self.replica,
                });
                return;
            }

            // Followers may not send a `nack_prepare` for a different checksum:
            // However our op may change in between sending the request and getting the nack.
            assert(message.header.op == op);
            assert(message.header.context == checksum);

            // Here are what our nack quorums look like, if we know our op is faulty:
            // These are for various replication quorums under Flexible Paxos.
            // We need to have enough nacks to guarantee that `quorum_replication` was not reached,
            // because if the replication quorum was reached, then it may have been committed.
            // We add `1` in each case because our op is faulty and may have been counted.
            //
            // replica_count=2 - quorum_replication=2 + 1 = 0 + 1 = 1 nacks required
            // replica_count=3 - quorum_replication=2 + 1 = 1 + 1 = 2 nacks required
            // replica_count=4 - quorum_replication=2 + 1 = 2 + 1 = 3 nacks required
            // replica_count=4 - quorum_replication=3 + 1 = 1 + 1 = 2 nacks required
            // replica_count=5 - quorum_replication=2 + 1 = 3 + 1 = 4 nacks required
            // replica_count=5 - quorum_replication=3 + 1 = 2 + 1 = 3 nacks required
            //
            // Otherwise, if we know we do not have the op, then we can exclude ourselves.
            assert(self.replica_count > 1);

            const threshold = if (self.journal.faulty.bit(op))
                self.replica_count - self.quorum_replication + 1
            else
                self.replica_count - self.quorum_replication;

            if (threshold == 0) {
                assert(self.replica_count == 2);
                assert(!self.journal.faulty.bit(op));

                // This is a special case for a cluster-of-two, handled in `repair_prepare()`.
                log.debug("{}: on_nack_prepare: ignoring (cluster-of-two, not faulty)", .{
                    self.replica,
                });
                return;
            }

            log.debug("{}: on_nack_prepare: quorum_replication={} threshold={}", .{
                self.replica,
                self.quorum_replication,
                threshold,
            });

            // We should never expect to receive a nack from ourselves:
            // Detect if we ever set `threshold` to `quorum_view_change` for a cluster-of-two again.
            assert(threshold < self.replica_count);

            // Wait until we have `threshold` messages for quorum:
            const count = self.add_message_and_receive_quorum_exactly_once(
                &self.nack_prepare_from_other_replicas,
                message,
                threshold,
            ) orelse return;

            assert(count == threshold);
            assert(self.nack_prepare_from_other_replicas[self.replica] == null);
            log.debug("{}: on_nack_prepare: quorum received", .{self.replica});

            self.discard_uncommitted_ops_from(op, checksum);
            self.reset_quorum_nack_prepare();
            self.repair();
        }

        fn on_headers(self: *Self, message: *const Message) void {
            if (self.ignore_repair_message(message)) return;

            assert(self.status == .normal or self.status == .view_change);
            assert(message.header.view == self.view);
            assert(message.header.replica != self.replica);

            // We expect at least one header in the body, or otherwise no response to our request.
            assert(message.header.size > @sizeOf(Header));

            var op_min: ?u64 = null;
            var op_max: ?u64 = null;
            for (self.message_body_as_headers(message)) |*h| {
                if (op_min == null or h.op < op_min.?) op_min = h.op;
                if (op_max == null or h.op > op_max.?) op_max = h.op;
                _ = self.repair_header(h);
            }
            assert(op_max.? >= op_min.?);

            self.repair();
        }

        fn on_ping_timeout(self: *Self) void {
            self.ping_timeout.reset();

            // TODO We may want to ping for connectivity during a view change.
            assert(self.status == .normal);
            assert(self.leader() or self.follower());

            var ping = Header{
                .command = .ping,
                .cluster = self.cluster,
                .replica = self.replica,
                .view = self.view,
                .op = self.clock.monotonic(),
            };

            self.send_header_to_other_replicas(ping);
        }

        fn on_prepare_timeout(self: *Self) void {
            // We will decide below whether to reset or backoff the timeout.
            assert(self.status == .normal);
            assert(self.leader());

            const prepare = self.pipeline.peek_ptr().?;
            assert(prepare.message.header.command == .prepare);

            if (prepare.ok_quorum_received) {
                self.prepare_timeout.reset();

                // We were unable to commit at the time because we were waiting for a message.
                log.debug("{}: on_prepare_timeout: quorum already received, retrying commit", .{
                    self.replica,
                });
                self.commit_pipeline();
                return;
            }

            // The list of remote replicas yet to send a prepare_ok:
            var waiting: [config.replicas_max]u8 = undefined;
            var waiting_len: usize = 0;
            for (prepare.ok_from_all_replicas[0..self.replica_count]) |received, replica| {
                if (received == null and replica != self.replica) {
                    waiting[waiting_len] = @intCast(u8, replica);
                    waiting_len += 1;
                }
            }

            if (waiting_len == 0) {
                self.prepare_timeout.reset();

                log.debug("{}: on_prepare_timeout: waiting for journal", .{self.replica});
                assert(prepare.ok_from_all_replicas[self.replica] == null);

                // We may be slow and waiting for the write to complete.
                //
                // We may even have maxed out our IO depth and been unable to initiate the write,
                // which can happen if `config.pipelining_max` exceeds `config.io_depth_write`.
                // This can lead to deadlock for a cluster of one or two (if we do not retry here),
                // since there is no other way for the leader to repair the dirty op because no
                // other replica has it.
                //
                // Retry the write through `on_repair()` which will work out which is which.
                // We do expect that the op would have been run through `on_prepare()` already.
                assert(prepare.message.header.op <= self.op);
                self.on_repair(prepare.message);

                return;
            }

            self.prepare_timeout.backoff(&self.prng);

            assert(waiting_len <= self.replica_count);
            for (waiting[0..waiting_len]) |replica| {
                assert(replica < self.replica_count);

                log.debug("{}: on_prepare_timeout: waiting for replica {}", .{
                    self.replica,
                    replica,
                });
            }

            // Cycle through the list to reach live replicas and get around partitions:
            // We do not assert `prepare_timeout.attempts > 0` since the counter may wrap back to 0.
            const replica = waiting[self.prepare_timeout.attempts % waiting_len];
            assert(replica != self.replica);

            log.debug("{}: on_prepare_timeout: replicating to replica {}", .{ self.replica, replica });
            self.send_message_to_replica(replica, prepare.message);
        }

        fn on_commit_timeout(self: *Self) void {
            self.commit_timeout.reset();

            assert(self.status == .normal);
            assert(self.leader());
            assert(self.commit_min == self.commit_max);

            // TODO Snapshots: Use snapshot checksum if commit is no longer in journal.
            const latest_committed_entry = self.journal.entry_for_op_exact(self.commit_max).?;

            self.send_header_to_other_replicas(.{
                .command = .commit,
                .context = latest_committed_entry.checksum,
                .cluster = self.cluster,
                .replica = self.replica,
                .view = self.view,
                .commit = self.commit_max,
            });
        }

        fn on_normal_status_timeout(self: *Self) void {
            assert(self.status == .normal);
            assert(self.follower());
            self.transition_to_view_change_status(self.view + 1);
        }

        fn on_view_change_status_timeout(self: *Self) void {
            assert(self.status == .view_change);
            self.transition_to_view_change_status(self.view + 1);
        }

        fn on_view_change_message_timeout(self: *Self) void {
            self.view_change_message_timeout.reset();
            assert(self.status == .view_change);

            // Keep sending `start_view_change` messages:
            // We may have a `start_view_change_quorum` but other replicas may not.
            // However, the leader may stop sending once it has a `do_view_change_quorum`.
            if (!self.do_view_change_quorum) self.send_start_view_change();

            // It is critical that a `do_view_change` message implies a `start_view_change_quorum`:
            if (self.start_view_change_quorum) {
                // The leader need not retry to send a `do_view_change` message to itself:
                // We assume the MessageBus will not drop messages sent by a replica to itself.
                if (self.leader_index(self.view) != self.replica) self.send_do_view_change();
            }
        }

        fn on_repair_timeout(self: *Self) void {
            assert(self.status == .normal or self.status == .view_change);
            self.repair();
        }

        fn add_message_and_receive_quorum_exactly_once(
            self: *Self,
            messages: *QuorumMessages,
            message: *Message,
            threshold: u32,
        ) ?usize {
            assert(threshold >= 1);
            assert(threshold <= self.replica_count);

            assert(messages.len == config.replicas_max);
            assert(message.header.cluster == self.cluster);
            assert(message.header.replica < self.replica_count);
            assert(message.header.view == self.view);
            switch (message.header.command) {
                .prepare_ok => {
                    if (self.replica_count <= 2) assert(threshold == self.replica_count);

                    assert(self.status == .normal);
                    assert(self.leader());
                },
                .start_view_change => {
                    assert(self.replica_count > 1);
                    if (self.replica_count == 2) assert(threshold == 1);

                    assert(self.status == .view_change);
                },
                .do_view_change => {
                    assert(self.replica_count > 1);
                    if (self.replica_count == 2) assert(threshold == 2);

                    assert(self.status == .view_change);
                    assert(self.leader_index(self.view) == self.replica);
                },
                .nack_prepare => {
                    assert(self.replica_count > 1);
                    if (self.replica_count == 2) assert(threshold >= 1);

                    assert(self.status == .view_change);
                    assert(self.leader_index(self.view) == self.replica);
                },
                else => unreachable,
            }

            const command: []const u8 = @tagName(message.header.command);

            // Do not allow duplicate messages to trigger multiple passes through a state transition:
            if (messages[message.header.replica]) |m| {
                // Assert that this is a duplicate message and not a different message:
                assert(m.header.command == message.header.command);
                assert(m.header.replica == message.header.replica);
                assert(m.header.view == message.header.view);
                assert(m.header.op == message.header.op);
                assert(m.header.commit == message.header.commit);
                assert(m.header.checksum_body == message.header.checksum_body);
                assert(m.header.checksum == message.header.checksum);
                log.debug("{}: on_{s}: ignoring (duplicate message)", .{ self.replica, command });
                return null;
            }

            // Record the first receipt of this message:
            assert(messages[message.header.replica] == null);
            messages[message.header.replica] = message.ref();

            // Count the number of unique messages now received:
            const count = self.count_quorum(messages, message.header.command, message.header.context);
            log.debug("{}: on_{s}: {} message(s)", .{ self.replica, command, count });

            // Wait until we have exactly `threshold` messages for quorum:
            if (count < threshold) {
                log.debug("{}: on_{s}: waiting for quorum", .{ self.replica, command });
                return null;
            }

            // This is not the first time we have had quorum, the state transition has already happened:
            if (count > threshold) {
                log.debug("{}: on_{s}: ignoring (quorum received already)", .{ self.replica, command });
                return null;
            }

            assert(count == threshold);
            return count;
        }

        fn append(self: *Self, message: *Message) void {
            assert(self.status == .normal);
            assert(message.header.command == .prepare);
            assert(message.header.view == self.view);
            assert(message.header.op == self.op);

            log.debug("{}: append: appending to journal", .{self.replica});
            self.write_prepare(message, .append);
        }

        /// Returns whether `b` succeeds `a` by having a newer view or same view and newer op.
        fn ascending_viewstamps(
            self: *Self,
            a: *const Header,
            b: *const Header,
        ) bool {
            assert(a.command == .prepare);
            assert(b.command == .prepare);

            if (a.view < b.view) {
                // We do not assert b.op >= a.op, ops may be reordered during a view change.
                return true;
            } else if (a.view > b.view) {
                // We do not assert b.op <= a.op, ops may be reordered during a view change.
                return false;
            } else if (a.op < b.op) {
                assert(a.view == b.view);
                return true;
            } else if (a.op > b.op) {
                assert(a.view == b.view);
                return false;
            } else {
                unreachable;
            }
        }

        /// Choose a different replica each time if possible (excluding ourself).
        fn choose_any_other_replica(self: *Self) ?u8 {
            if (self.replica_count == 1) return null;

            var count: usize = 0;
            while (count < self.replica_count) : (count += 1) {
                self.choose_any_other_replica_ticks += 1;
                const replica = @mod(
                    self.replica + self.choose_any_other_replica_ticks,
                    self.replica_count,
                );
                if (replica == self.replica) continue;
                return @intCast(u8, replica);
            }
            unreachable;
        }

        /// Commit ops up to commit number `commit` (inclusive).
        /// A function which calls `commit_ops()` to set `commit_max` must first call `view_jump()`.
        /// Otherwise, we may fork the log.
        fn commit_ops(self: *Self, commit: u64) void {
            // TODO Restrict `view_change` status only to the leader purely as defense-in-depth.
            // Be careful of concurrency when doing this, as successive view changes can happen quickly.
            assert(self.status == .normal or self.status == .view_change);
            assert(self.commit_min <= self.commit_max);
            assert(self.commit_min <= self.op);
            assert(self.commit_max <= self.op or self.commit_max > self.op);
            assert(commit <= self.op or commit > self.op);

            // We have already committed this far:
            if (commit <= self.commit_min) return;

            // We must update `commit_max` even if we are already committing, otherwise we will lose
            // information that we should know, and `set_latest_op_and_k()` will catch us out:
            if (commit > self.commit_max) {
                log.debug("{}: commit_ops: advancing commit_max={}..{}", .{
                    self.replica,
                    self.commit_max,
                    commit,
                });
                self.commit_max = commit;
            }

            // Guard against multiple concurrent invocations of commit_ops():
            if (self.committing) {
                log.debug("{}: commit_ops: already committing...", .{self.replica});
                return;
            }

            // We check the hash chain before we read each op, rather than once upfront, because
            // it's possible for `commit_max` to change while we read asynchronously, after we
            // validate the hash chain.
            //
            // We therefore cannot keep committing until we reach `commit_max`. We need to verify
            // the hash chain before each read. Once verified (before the read) we can commit in the
            // callback after the read, but if we see a change we need to stop committing any
            // further ops, because `commit_max` may have been bumped and may refer to a different
            // op.

            assert(!self.committing);
            self.committing = true;

            self.commit_ops_read();
        }

        fn commit_ops_read(self: *Self) void {
            assert(self.committing);
            assert(self.status == .normal or self.status == .view_change);
            assert(self.commit_min <= self.commit_max);
            assert(self.commit_min <= self.op);

            if (!self.valid_hash_chain("commit_ops_read")) {
                self.committing = false;
                return;
            }
            assert(self.op >= self.commit_max);

            // We may receive commit numbers for ops we do not yet have (`commit_max > self.op`):
            // Even a naive state transfer may fail to correct for this.
            if (self.commit_min < self.commit_max and self.commit_min < self.op) {
                const op = self.commit_min + 1;
                const checksum = self.journal.entry_for_op_exact(op).?.checksum;
                self.journal.read_prepare(commit_ops_commit, op, checksum, null);
            } else {
                self.committing = false;
                // This is an optimization to expedite the view change before the `repair_timeout`:
                if (self.status == .view_change and self.repairs_allowed()) self.repair();
            }
        }

        fn commit_ops_commit(self: *Self, prepare: ?*Message, destination_replica: ?u8) void {
            assert(destination_replica == null);

            assert(self.committing);
            self.committing = false;

            if (prepare == null) {
                log.debug("{}: commit_ops_commit: prepare == null", .{self.replica});
                return;
            }

            if (self.status == .view_change) {
                if (self.leader_index(self.view) != self.replica) {
                    log.debug("{}: commit_ops_commit: no longer leader", .{self.replica});
                    return;
                }

                // Only the leader may commit during a view change before starting the new view.
                // Fall through if this is indeed the case.
            } else if (self.status != .normal) {
                log.debug("{}: commit_ops_commit: no longer in normal status", .{self.replica});
                return;
            }

            const op = self.commit_min + 1;

            if (prepare.?.header.op != op) {
                log.debug("{}: commit_ops_commit: op changed", .{self.replica});
                return;
            }

            if (prepare.?.header.checksum != self.journal.entry_for_op_exact(op).?.checksum) {
                log.debug("{}: commit_ops_commit: checksum changed", .{self.replica});
                return;
            }

            // TODO We can optimize this to commit into the client table reply if it exists.
            const reply = self.message_bus.get_message() orelse {
                log.alert("{}: commit_ops_commit: waiting for message", .{self.replica});
                return;
            };
            defer self.message_bus.unref(reply);

            self.commit_op(prepare.?, reply);

            assert(self.commit_min == op);
            assert(self.commit_min <= self.commit_max);
            assert(self.commit_min <= self.op);

            self.committing = true;
            self.commit_ops_read();
        }

        fn commit_op(self: *Self, prepare: *const Message, reply: *Message) void {
            // TODO Can we add more checks around allowing commit_op() during a view change?
            assert(self.status == .normal or self.status == .view_change);
            assert(prepare.header.command == .prepare);
            assert(prepare.header.operation != .init);
            assert(prepare.header.op == self.commit_min + 1);
            assert(prepare.header.op <= self.op);

            // If we are a follower committing through `commit_ops()` then a view change may have
            // happened since we last checked in `commit_ops_read()`. However, this would relate to
            // subsequent ops, since by now we have already verified the hash chain for this commit.

            assert(self.journal.entry_for_op_exact(self.commit_min).?.checksum ==
                prepare.header.parent);

            log.debug("{}: commit_op: executing view={} {} op={} checksum={} ({s})", .{
                self.replica,
                self.view,
                self.leader_index(self.view) == self.replica,
                prepare.header.op,
                prepare.header.checksum,
                @tagName(prepare.header.operation.cast(StateMachine)),
            });

            const reply_body_size = @intCast(u32, self.state_machine.commit(
                prepare.header.client,
                prepare.header.operation.cast(StateMachine),
                prepare.buffer[@sizeOf(Header)..prepare.header.size],
                reply.buffer[@sizeOf(Header)..],
            ));

            self.commit_min += 1;
            assert(self.commit_min == prepare.header.op);
            if (self.commit_min > self.commit_max) self.commit_max = self.commit_min;

            if (self.on_change_state) |hook| hook(self);

            reply.header.* = .{
                .command = .reply,
                .operation = prepare.header.operation,
                .parent = prepare.header.context, // The prepare's context has `request.checksum`.
                .client = prepare.header.client,
                .request = prepare.header.request,
                .cluster = prepare.header.cluster,
                .replica = prepare.header.replica,
                .view = prepare.header.view,
                .op = prepare.header.op,
                .commit = prepare.header.op,
                .size = @sizeOf(Header) + reply_body_size,
            };
            assert(reply.header.offset == 0);
            assert(reply.header.epoch == 0);

            reply.header.set_checksum_body(reply.buffer[@sizeOf(Header)..reply.header.size]);
            reply.header.set_checksum();

            if (reply.header.operation == .register) {
                self.create_client_table_entry(reply);
            } else {
                self.update_client_table_entry(reply);
            }

            if (self.leader_index(self.view) == self.replica) {
                log.debug("{}: commit_op: replying to client: {}", .{ self.replica, reply.header });
                self.message_bus.send_message_to_client(reply.header.client, reply);
            }
        }

        /// Commits, frees and pops as many prepares at the head of the pipeline as have quorum.
        /// Can be called only when the pipeline has at least one prepare.
        /// Stops the prepare timeout and resets the timeouts counter if the pipeline becomes empty.
        fn commit_pipeline(self: *Self) void {
            assert(self.status == .normal);
            assert(self.leader());
            assert(self.pipeline.count > 0);

            while (self.pipeline.peek_ptr()) |prepare| {
                assert(self.pipeline.count > 0);
                assert(self.commit_min == self.commit_max);
                assert(self.commit_max + self.pipeline.count == self.op);
                assert(self.commit_max + 1 == prepare.message.header.op);

                if (!prepare.ok_quorum_received) {
                    // Eventually handled by on_prepare_timeout().
                    log.debug("{}: commit_pipeline: waiting for quorum", .{self.replica});
                    return;
                }

                const count = self.count_quorum(
                    &prepare.ok_from_all_replicas,
                    .prepare_ok,
                    prepare.message.header.checksum,
                );
                assert(count >= self.quorum_replication);

                // TODO We can optimize this to commit into the client table reply if it exists.
                const reply = self.message_bus.get_message() orelse {
                    // Eventually handled by on_prepare_timeout().
                    log.alert("{}: commit_pipeline: waiting for message", .{self.replica});
                    return;
                };
                defer self.message_bus.unref(reply);

                self.commit_op(prepare.message, reply);

                assert(self.commit_min == self.commit_max);
                assert(self.commit_max == prepare.message.header.op);

                self.unref_prepare_message_and_quorum_messages(prepare);
                assert(self.pipeline.pop() != null);
            }

            assert(self.prepare_timeout.ticking);

            if (self.pipeline.count == 0) self.prepare_timeout.stop();
        }

        fn count_quorum(
            self: *Self,
            messages: *QuorumMessages,
            command: Command,
            context: u128,
        ) usize {
            assert(messages.len == config.replicas_max);
            var count: usize = 0;
            for (messages) |received, replica| {
                if (received) |m| {
                    assert(replica < self.replica_count);
                    assert(m.header.cluster == self.cluster);
                    assert(m.header.command == command);
                    assert(m.header.context == context);
                    assert(m.header.replica == replica);
                    switch (command) {
                        .prepare_ok => {
                            if (self.status == .normal) {
                                assert(self.leader());
                                assert(m.header.view == self.view);
                            } else {
                                assert(self.status == .view_change);
                                assert(m.header.view < self.view);
                            }
                        },
                        .start_view_change => {
                            assert(m.header.replica != self.replica);
                            assert(m.header.view == self.view);
                        },
                        .do_view_change => assert(m.header.view == self.view),
                        .nack_prepare => {
                            // TODO See if we can restrict this branch further.
                            assert(m.header.replica != self.replica);
                            assert(m.header.op == self.nack_prepare_op.?);
                        },
                        else => unreachable,
                    }
                    count += 1;
                }
            }
            assert(count <= self.replica_count);
            return count;
        }

        /// Creates an entry in the client table when registering a new client session.
        /// Asserts that the new session does not yet exist.
        /// Evicts another entry deterministically, if necessary, to make space for the insert.
        fn create_client_table_entry(self: *Self, reply: *Message) void {
            assert(reply.header.command == .reply);
            assert(reply.header.operation == .register);
            assert(reply.header.client > 0);
            assert(reply.header.context == 0);
            assert(reply.header.op == reply.header.commit);
            assert(reply.header.size == @sizeOf(Header));

            const session = reply.header.commit; // The commit number becomes the session number.
            const request = reply.header.request;

            assert(session > 0); // We reserved the `0` commit number for the cluster `.init` operation.
            assert(request == 0);

            // For correctness, it's critical that all replicas evict deterministically:
            // We cannot depend on `HashMap.capacity()` since `HashMap.ensureCapacity()` may change
            // across different versions of the Zig std lib. We therefore rely on `config.clients_max`,
            // which must be the same across all replicas, and must not change after initing a cluster.
            // We also do not depend on `HashMap.valueIterator()` being deterministic here. However, we
            // do require that all entries have different commit numbers and are at least iterated.
            // This ensures that we will always pick the entry with the oldest commit number.
            // We also double-check that a client has only one entry in the hash map (or it's buggy).
            const clients = self.client_table.count();
            assert(clients <= config.clients_max);
            if (clients == config.clients_max) {
                var evictee: ?*Message = null;
                var iterated: usize = 0;
                var iterator = self.client_table.valueIterator();
                while (iterator.next()) |entry| : (iterated += 1) {
                    assert(entry.reply.header.command == .reply);
                    assert(entry.reply.header.context == 0);
                    assert(entry.reply.header.op == entry.reply.header.commit);
                    assert(entry.reply.header.commit >= entry.session);

                    if (evictee) |evictee_reply| {
                        assert(entry.reply.header.client != evictee_reply.header.client);
                        assert(entry.reply.header.commit != evictee_reply.header.commit);

                        if (entry.reply.header.commit < evictee_reply.header.commit) {
                            evictee = entry.reply;
                        }
                    } else {
                        evictee = entry.reply;
                    }
                }
                assert(iterated == clients);
                log.alert("{}: create_client_table_entry: clients={}/{} evicting client={}", .{
                    self.replica,
                    clients,
                    config.clients_max,
                    evictee.?.header.client,
                });
                assert(self.client_table.remove(evictee.?.header.client));
                assert(!self.client_table.contains(evictee.?.header.client));
                self.message_bus.unref(evictee.?);
            }

            log.debug("{}: create_client_table_entry: client={} session={} request={}", .{
                self.replica,
                reply.header.client,
                session,
                request,
            });

            // Any duplicate .register requests should have received the same session number if the
            // client table entry already existed, or been dropped if a session was being committed:
            self.client_table.putAssumeCapacityNoClobber(reply.header.client, .{
                .session = session,
                .reply = reply.ref(),
            });
            assert(self.client_table.count() <= config.clients_max);
        }

        /// The caller owns the returned message, if any, which has exactly 1 reference.
        fn create_view_change_message(self: *Self, command: Command) ?*Message {
            assert(command == .do_view_change or command == .start_view);

            // We may send a start_view message in normal status to resolve a follower's view jump:
            assert(self.status == .normal or self.status == .view_change);

            const message = self.message_bus.get_message() orelse return null;
            defer self.message_bus.unref(message);

            message.header.* = .{
                .command = command,
                .cluster = self.cluster,
                .replica = self.replica,
                .view = self.view,
                // The latest normal view (as specified in the 2012 paper) is different to the view
                // number contained in the prepare headers we include in the body. The former shows
                // how recent a view change the replica participated in, which may be much higher.
                // We use the `offset` field to send this in addition to the current view number:
                .offset = if (command == .do_view_change) self.view_normal else 0,
                .op = self.op,
                .commit = self.commit_max,
            };

            // CRITICAL: The number of prepare headers to include in the body:
            // We must provide enough headers to cover all uncommitted headers so that the new
            // leader (if we are in a view change) can decide whether to discard uncommitted headers
            // that cannot be repaired because they are gaps, and this must be relative to the
            // cluster as a whole (not relative to the difference between our op and commit number)
            // as otherwise we would break correctness.
            const count_max = config.pipelining_max;
            assert(count_max > 0);

            const size_max = @sizeOf(Header) * std.math.min(
                std.math.max(@divFloor(message.buffer.len, @sizeOf(Header)), 2),
                1 + count_max,
            );
            assert(size_max > @sizeOf(Header));

            const count = self.journal.copy_latest_headers_between(
                0,
                self.op,
                std.mem.bytesAsSlice(Header, message.buffer[@sizeOf(Header)..size_max]),
            );

            // We expect that self.op always exists.
            assert(count > 0);

            message.header.size = @intCast(u32, @sizeOf(Header) + @sizeOf(Header) * count);

            message.header.set_checksum_body(message.body());
            message.header.set_checksum();

            return message.ref();
        }

        /// The caller owns the returned message, if any, which has exactly 1 reference.
        fn create_message_from_header(self: *Self, header: Header) ?*Message {
            assert(header.replica == self.replica);
            assert(header.view == self.view or header.command == .request_start_view);
            assert(header.size == @sizeOf(Header));

            const message = self.message_bus.pool.get_header_only_message() orelse return null;
            defer self.message_bus.unref(message);

            message.header.* = header;
            message.header.set_checksum_body(message.body());
            message.header.set_checksum();

            return message.ref();
        }

        /// Discards uncommitted headers during a view change before the new leader starts the view.
        /// This is required to maximize availability in the presence of storage faults.
        /// Refer to the CTRL protocol from Protocol-Aware Recovery for Consensus-Based Storage.
        ///
        /// It's possible for the new leader to have done an op jump in a previous view, and so
        /// introduced a header gap for an op, which was then discarded by another leader during a
        /// newer view change, before surviving into this view as a gap because our latest op was
        /// set as the latest op for the quorum.
        ///
        /// In this case, it may be impossible for the new leader to repair the missing header since
        /// the rest of the cluster may have already discarded it. We therefore iterate over our
        /// uncommitted header gaps and compare them with the quorum of do_view_change messages
        /// received from other replicas, before starting the new view, to discard any that may be
        /// impossible to repair.
        fn discard_uncommitted_headers(self: *Self) void {
            assert(self.status == .view_change);
            assert(self.leader_index(self.view) == self.replica);
            assert(self.do_view_change_quorum);
            assert(!self.repair_timeout.ticking);
            assert(self.op >= self.commit_max);
            assert(self.replica_count > 1);

            const threshold = self.replica_count - self.quorum_replication;
            if (threshold == 0) {
                assert(self.replica_count == 2);
                return;
            }

            var op = self.op;
            while (op > self.commit_max) : (op -= 1) {
                if (self.journal.entry_for_op_exact(op) != null) continue;

                log.debug("{}: discard_uncommitted_headers: op={} gap", .{ self.replica, op });

                var nacks: usize = 0;
                for (self.do_view_change_from_all_replicas) |received, replica| {
                    if (received) |m| {
                        assert(m.header.command == .do_view_change);
                        assert(m.header.cluster == self.cluster);
                        assert(m.header.replica == replica);
                        assert(m.header.view == self.view);

                        if (replica != self.replica) {
                            if (m.header.op < op) nacks += 1;

                            log.debug("{}: discard_uncommitted_headers: replica={} op={}", .{
                                self.replica,
                                m.header.replica,
                                m.header.op,
                            });
                        }
                    }
                }

                log.debug("{}: discard_uncommitted_headers: op={} nacks={} threshold={}", .{
                    self.replica,
                    op,
                    nacks,
                    threshold,
                });

                if (nacks >= threshold) {
                    self.journal.remove_entries_from(op);
                    self.op = op - 1;

                    assert(self.journal.entry_for_op(op) == null);
                    assert(!self.journal.dirty.bit(op));
                    assert(!self.journal.faulty.bit(op));
                }
            }
        }

        /// Discards uncommitted ops during a view change from after and including `op`.
        /// This is required to maximize availability in the presence of storage faults.
        /// Refer to the CTRL protocol from Protocol-Aware Recovery for Consensus-Based Storage.
        fn discard_uncommitted_ops_from(self: *Self, op: u64, checksum: u128) void {
            assert(self.status == .view_change);
            assert(self.leader_index(self.view) == self.replica);
            assert(self.repairs_allowed());

            assert(self.valid_hash_chain("discard_uncommitted_ops_from"));

            assert(op > self.commit_max);
            assert(op <= self.op);
            assert(self.journal.entry_for_op_exact_with_checksum(op, checksum) != null);
            assert(self.journal.dirty.bit(op));

            log.debug("{}: discard_uncommitted_ops_from: ops={}..{} view={}", .{
                self.replica,
                op,
                self.op,
                self.view,
            });

            self.journal.remove_entries_from(op);
            self.op = op - 1;

            assert(self.journal.entry_for_op(op) == null);
            assert(!self.journal.dirty.bit(op));
            assert(!self.journal.faulty.bit(op));

            // We require that `self.op` always exists. Rewinding `self.op` could change that.
            // However, we do this only as the leader within a view change, with all headers intact.
            assert(self.journal.entry_for_op_exact(self.op) != null);
        }

        /// Returns whether the replica is a follower for the current view.
        /// This may be used only when the replica status is normal.
        fn follower(self: *Self) bool {
            return !self.leader();
        }

        fn flush_loopback_queue(self: *Self) void {
            // There are three cases where a replica will send a message to itself:
            // However, of these three cases, only two cases will call send_message_to_replica().
            //
            // 1. In on_request(), the leader sends a synchronous prepare to itself, but this is
            //    done by calling on_prepare() directly, and subsequent prepare timeout retries will
            //    never resend to self.
            // 2. In on_prepare(), after writing to storage, the leader sends a (typically)
            //    asynchronous prepare_ok to itself.
            // 3. In on_start_view_change(), after receiving a quorum of start_view_change
            //    messages, the new leader sends a synchronous do_view_change to itself.
            if (self.loopback_queue) |message| {
                defer self.message_bus.unref(message);

                assert(message.next == null);
                self.loopback_queue = null;
                assert(message.header.replica == self.replica);
                self.on_message(message);
                // We do not call flush_loopback_queue() within on_message() to avoid recursion.
            }
            // We expect that delivering a prepare_ok or do_view_change message to ourselves will
            // not result in any further messages being added synchronously to the loopback queue.
            assert(self.loopback_queue == null);
        }

        fn ignore_prepare_ok(self: *Self, message: *const Message) bool {
            if (self.status != .normal) {
                log.debug("{}: on_prepare_ok: ignoring ({})", .{ self.replica, self.status });
                return true;
            }

            if (message.header.view < self.view) {
                log.debug("{}: on_prepare_ok: ignoring (older view)", .{self.replica});
                return true;
            }

            if (message.header.view > self.view) {
                // Another replica is treating us as the leader for a view we do not know about.
                // This may be caused by a fault in the network topology.
                log.warn("{}: on_prepare_ok: ignoring (newer view)", .{self.replica});
                return true;
            }

            if (self.follower()) {
                // This may be caused by a fault in the network topology.
                log.warn("{}: on_prepare_ok: ignoring (follower)", .{self.replica});
                return true;
            }

            return false;
        }

        fn ignore_repair_message(self: *Self, message: *const Message) bool {
            assert(message.header.command == .request_start_view or
                message.header.command == .request_headers or
                message.header.command == .request_prepare or
                message.header.command == .headers or
                message.header.command == .nack_prepare);

            const command: []const u8 = @tagName(message.header.command);

            if (self.status != .normal and self.status != .view_change) {
                log.debug("{}: on_{s}: ignoring ({})", .{ self.replica, command, self.status });
                return true;
            }

            if (message.header.view < self.view) {
                log.debug("{}: on_{s}: ignoring (older view)", .{ self.replica, command });
                return true;
            }

            if (message.header.view > self.view) {
                log.debug("{}: on_{s}: ignoring (newer view)", .{ self.replica, command });
                return true;
            }

            if (self.ignore_repair_message_during_view_change(message)) return true;

            if (message.header.replica == self.replica) {
                log.warn("{}: on_{s}: ignoring (self)", .{ self.replica, command });
                return true;
            }

            if (self.leader_index(self.view) != self.replica) {
                switch (message.header.command) {
                    // Only the leader may receive these messages:
                    .request_start_view, .nack_prepare => {
                        log.warn("{}: on_{s}: ignoring (follower)", .{ self.replica, command });
                        return true;
                    },
                    // Only the leader may answer a request for a prepare without a context:
                    .request_prepare => if (message.header.context == 0) {
                        log.warn("{}: on_{s}: ignoring (no context)", .{ self.replica, command });
                        return true;
                    },
                    else => {},
                }
            }

            if (message.header.command == .nack_prepare and self.status == .normal) {
                log.debug("{}: on_{s}: ignoring (view started)", .{ self.replica, command });
                return true;
            }

            // Only allow repairs for same view as defense-in-depth:
            assert(message.header.view == self.view);
            return false;
        }

        fn ignore_repair_message_during_view_change(self: *Self, message: *const Message) bool {
            if (self.status != .view_change) return false;

            const command: []const u8 = @tagName(message.header.command);

            switch (message.header.command) {
                .request_start_view => {
                    log.debug("{}: on_{s}: ignoring (view change)", .{ self.replica, command });
                    return true;
                },
                .request_headers, .request_prepare => {
                    if (self.leader_index(self.view) != message.header.replica) {
                        log.debug("{}: on_{s}: ignoring (view change, requested by follower)", .{
                            self.replica,
                            command,
                        });
                        return true;
                    }
                },
                .headers, .nack_prepare => {
                    if (self.leader_index(self.view) != self.replica) {
                        log.debug("{}: on_{s}: ignoring (view change, received by follower)", .{
                            self.replica,
                            command,
                        });
                        return true;
                    } else if (!self.do_view_change_quorum) {
                        log.debug("{}: on_{s}: ignoring (view change, waiting for quorum)", .{
                            self.replica,
                            command,
                        });
                        return true;
                    }
                },
                else => unreachable,
            }

            return false;
        }

        fn ignore_request_message(self: *Self, message: *Message) bool {
            assert(message.header.command == .request);

            if (self.status != .normal) {
                log.debug("{}: on_request: ignoring ({s})", .{ self.replica, self.status });
                return true;
            }

            if (self.ignore_request_message_follower(message)) return true;
            if (self.ignore_request_message_duplicate(message)) return true;
            if (self.ignore_request_message_preparing(message)) return true;
            return false;
        }

        /// Returns whether the request is stale, or a duplicate of the latest committed request.
        /// Resends the reply to the latest request if the request has been committed.
        fn ignore_request_message_duplicate(self: *Self, message: *const Message) bool {
            assert(self.status == .normal);
            assert(self.leader());

            assert(message.header.command == .request);
            assert(message.header.client > 0);
            assert(message.header.view <= self.view); // See ignore_request_message_follower().
            assert(message.header.context == 0 or message.header.operation != .register);
            assert(message.header.request == 0 or message.header.operation != .register);

            if (self.client_table.getPtr(message.header.client)) |entry| {
                assert(entry.reply.header.command == .reply);
                assert(entry.reply.header.client == message.header.client);

                if (message.header.operation == .register) {
                    // Fall through below to check if we should resend the .register session reply.
                } else if (entry.session > message.header.context) {
                    // The client must not reuse the ephemeral client ID when registering a new session.
                    log.alert("{}: on_request: ignoring older session (client bug)", .{self.replica});
                    return true;
                } else if (entry.session < message.header.context) {
                    // This cannot be because of a partition since we check the client's view number.
                    log.alert("{}: on_request: ignoring newer session (client bug)", .{self.replica});
                    return true;
                }

                if (entry.reply.header.request > message.header.request) {
                    log.debug("{}: on_request: ignoring older request", .{self.replica});
                    return true;
                } else if (entry.reply.header.request == message.header.request) {
                    if (message.header.checksum == entry.reply.header.parent) {
                        assert(entry.reply.header.operation == message.header.operation);

                        log.debug("{}: on_request: replying to duplicate request", .{self.replica});
                        self.message_bus.send_message_to_client(message.header.client, entry.reply);
                        return true;
                    } else {
                        log.alert("{}: on_request: request collision (client bug)", .{self.replica});
                        return true;
                    }
                } else if (entry.reply.header.request + 1 == message.header.request) {
                    if (message.header.parent == entry.reply.header.checksum) {
                        // The client has proved that they received our last reply.
                        log.debug("{}: on_request: new request", .{self.replica});
                        return false;
                    } else {
                        // The client may have only one request inflight at a time.
                        log.alert("{}: on_request: ignoring new request (client bug)", .{self.replica});
                        return true;
                    }
                } else {
                    log.alert("{}: on_request: ignoring newer request (client bug)", .{self.replica});
                    return true;
                }
            } else if (message.header.operation == .register) {
                log.debug("{}: on_request: new session", .{self.replica});
                return false;
            } else if (self.pipeline_prepare_for_client(message.header.client)) |_| {
                // The client registered with the previous leader, which committed and replied back
                // to the client before the view change, after which the register operation was
                // reloaded into the pipeline to be driven to completion by the new leader, which
                // now receives a request from the client that appears to have no session.
                // However, the session is about to be registered, so we must wait for it to commit.
                log.debug("{}: on_request: waiting for session to commit", .{self.replica});
                return true;
            } else {
                // We must have all commits to know whether a session has been evicted. For example,
                // there is the risk of sending an eviction message (even as the leader) if we are
                // partitioned and don't yet know about a session. We solve this by having clients
                // include the view number and rejecting messages from clients with newer views.
                log.err("{}: on_request: no session", .{self.replica});
                self.send_eviction_message_to_client(message.header.client);
                return true;
            }
        }

        /// Returns whether the replica is eligible to process this request as the leader.
        /// Takes the client's perspective into account if the client is aware of a newer view.
        /// Forwards requests to the leader if the client has an older view.
        fn ignore_request_message_follower(self: *Self, message: *Message) bool {
            assert(self.status == .normal);
            assert(message.header.command == .request);

            // The client is aware of a newer view:
            // Even if we think we are the leader, we may be partitioned from the rest of the cluster.
            // We therefore drop the message rather than flood our partition with traffic.
            if (message.header.view > self.view) {
                log.debug("{}: on_request: ignoring (newer view)", .{self.replica});
                return true;
            } else if (self.leader()) {
                return false;
            }

            if (message.header.operation == .register) {
                // We do not forward `.register` requests for the sake of `Header.peer_type()`.
                // This enables the MessageBus to identify client connections on the first message.
                log.debug("{}: on_request: ignoring (follower, register)", .{self.replica});
            } else if (message.header.view < self.view) {
                // The client may not know who the leader is, or may be retrying after a leader failure.
                // We forward to the new leader ahead of any client retry timeout to reduce latency.
                // Since the client is already connected to all replicas, the client may yet receive the
                // reply from the new leader directly.
                log.debug("{}: on_request: forwarding (follower)", .{self.replica});
                self.send_message_to_replica(self.leader_index(self.view), message);
            } else {
                assert(message.header.view == self.view);
                // The client has the correct view, but has retried against a follower.
                // This may mean that the leader is down and that we are about to do a view change.
                // There is also not much we can do as the client already knows who the leader is.
                // We do not forward as this would amplify traffic on the network.

                // TODO This may also indicate a client-leader partition. If we see enough of these,
                // should we trigger a view change to select a leader that clients can reach?
                // This is a question of weighing the probability of a partition vs routing error.
                log.debug("{}: on_request: ignoring (follower, same view)", .{self.replica});
            }

            assert(self.follower());
            return true;
        }

        fn ignore_request_message_preparing(self: *Self, message: *const Message) bool {
            assert(self.status == .normal);
            assert(self.leader());

            assert(message.header.command == .request);
            assert(message.header.client > 0);
            assert(message.header.view <= self.view); // See ignore_request_message_follower().

            if (self.pipeline_prepare_for_client(message.header.client)) |prepare| {
                assert(prepare.message.header.command == .prepare);
                assert(prepare.message.header.client == message.header.client);
                assert(prepare.message.header.op > self.commit_max);

                if (message.header.checksum == prepare.message.header.context) {
                    log.debug("{}: on_request: ignoring (already preparing)", .{self.replica});
                    return true;
                } else {
                    log.alert("{}: on_request: ignoring (client forked)", .{self.replica});
                    return true;
                }
            }

            if (self.pipeline.full()) {
                log.debug("{}: on_request: ignoring (pipeline full)", .{self.replica});
                return true;
            }

            return false;
        }

        fn ignore_view_change_message(self: *Self, message: *const Message) bool {
            assert(message.header.command == .start_view_change or
                message.header.command == .do_view_change or
                message.header.command == .start_view);
            assert(message.header.view > 0); // The initial view is already zero.

            const command: []const u8 = @tagName(message.header.command);

            // 4.3 Recovery
            // While a replica's status is recovering it does not participate in either the request
            // processing protocol or the view change protocol.
            // This is critical for correctness (to avoid data loss):
            if (self.status == .recovering) {
                log.debug("{}: on_{s}: ignoring (recovering)", .{ self.replica, command });
                return true;
            }

            if (message.header.view < self.view) {
                log.debug("{}: on_{s}: ignoring (older view)", .{ self.replica, command });
                return true;
            }

            if (message.header.view == self.view and self.status == .normal) {
                log.debug("{}: on_{s}: ignoring (view started)", .{ self.replica, command });
                return true;
            }

            // These may be caused by faults in the network topology.
            switch (message.header.command) {
                .start_view_change, .start_view => {
                    if (message.header.replica == self.replica) {
                        log.warn("{}: on_{s}: ignoring (self)", .{ self.replica, command });
                        return true;
                    }
                },
                .do_view_change => {
                    if (self.leader_index(message.header.view) != self.replica) {
                        log.warn("{}: on_{s}: ignoring (follower)", .{ self.replica, command });
                        return true;
                    }
                },
                else => unreachable,
            }

            return false;
        }

        fn is_repair(self: *Self, message: *const Message) bool {
            assert(message.header.command == .prepare);

            if (self.status == .normal) {
                if (message.header.view < self.view) return true;
                if (message.header.view == self.view and message.header.op <= self.op) return true;
            } else if (self.status == .view_change) {
                if (message.header.view < self.view) return true;
                // The view has already started or is newer.
            }

            return false;
        }

        /// Returns whether the replica is the leader for the current view.
        /// This may be used only when the replica status is normal.
        fn leader(self: *Self) bool {
            assert(self.status == .normal);
            return self.leader_index(self.view) == self.replica;
        }

        /// Returns the index into the configuration of the leader for a given view.
        fn leader_index(self: *Self, view: u32) u8 {
            return @intCast(u8, @mod(view, self.replica_count));
        }

        /// Advances `op` to where we need to be before `header` can be processed as a prepare:
        fn jump_to_newer_op_in_normal_status(self: *Self, header: *const Header) void {
            assert(self.status == .normal);
            assert(self.follower());
            assert(header.view == self.view);
            assert(header.op > self.op + 1);
            // We may have learned of a higher `commit_max` through a commit message before jumping to a
            // newer op that is less than `commit_max` but greater than `commit_min`:
            assert(header.op > self.commit_min);

            log.debug("{}: jump_to_newer_op: advancing: op={}..{} checksum={}..{}", .{
                self.replica,
                self.op,
                header.op - 1,
                self.journal.entry_for_op_exact(self.op).?.checksum,
                header.parent,
            });

            self.op = header.op - 1;
            assert(self.op >= self.commit_min);
            assert(self.op + 1 == header.op);
        }

        fn message_body_as_headers(self: *Self, message: *const Message) []Header {
            // TODO Assert message commands that we expect this to be called for.
            assert(message.header.size > @sizeOf(Header)); // Body must contain at least one header.
            return std.mem.bytesAsSlice(Header, message.buffer[@sizeOf(Header)..message.header.size]);
        }

        /// Panics if immediate neighbors in the same view would have a broken hash chain.
        /// Assumes gaps and does not require that a preceeds b.
        fn panic_if_hash_chain_would_break_in_the_same_view(
            self: *Self,
            a: *const Header,
            b: *const Header,
        ) void {
            assert(a.command == .prepare);
            assert(b.command == .prepare);
            assert(a.cluster == b.cluster);
            if (a.view == b.view and a.op + 1 == b.op and a.checksum != b.parent) {
                assert(a.valid_checksum());
                assert(b.valid_checksum());
                log.emerg("{}: panic_if_hash_chain_would_break: a: {}", .{ self.replica, a });
                log.emerg("{}: panic_if_hash_chain_would_break: b: {}", .{ self.replica, b });
                @panic("hash chain would break");
            }
        }

        /// Searches the pipeline for a prepare for a given client.
        fn pipeline_prepare_for_client(self: *Self, client: u128) ?*Prepare {
            assert(self.status == .normal);
            assert(self.leader());
            assert(self.commit_min == self.commit_max);

            var op = self.commit_max + 1;
            var parent = self.journal.entry_for_op_exact(self.commit_max).?.checksum;
            var iterator = self.pipeline.iterator();
            while (iterator.next_ptr()) |prepare| {
                assert(prepare.message.header.command == .prepare);
                assert(prepare.message.header.op == op);
                assert(prepare.message.header.parent == parent);

                // A client may have multiple requests in the pipeline if these were committed by
                // the previous leader and were reloaded into the pipeline after a view change.
                if (prepare.message.header.client == client) return prepare;

                parent = prepare.message.header.checksum;
                op += 1;
            }

            assert(self.pipeline.count <= config.pipelining_max);
            assert(self.commit_max + self.pipeline.count == op - 1);
            assert(self.commit_max + self.pipeline.count == self.op);

            return null;
        }

        /// Searches the pipeline for a prepare for a given client and checksum.
        /// Passing the prepare_ok message prevents these u128s from being accidentally swapped.
        /// Asserts that the returned prepare, if any, exactly matches the prepare_ok.
        fn pipeline_prepare_for_prepare_ok(self: *Self, ok: *const Message) ?*Prepare {
            assert(ok.header.command == .prepare_ok);

            assert(self.status == .normal);
            assert(self.leader());

            const prepare = self.pipeline_prepare_for_client(ok.header.client) orelse {
                log.debug("{}: pipeline_prepare_for_prepare_ok: not preparing", .{self.replica});
                return null;
            };

            if (ok.header.context != prepare.message.header.checksum) {
                // This can be normal, for example, if an old prepare_ok is replayed.
                log.debug("{}: pipeline_prepare_for_prepare_ok: preparing a different client op", .{
                    self.replica,
                });
                return null;
            }

            assert(prepare.message.header.parent == ok.header.parent);
            assert(prepare.message.header.client == ok.header.client);
            assert(prepare.message.header.request == ok.header.request);
            assert(prepare.message.header.cluster == ok.header.cluster);
            assert(prepare.message.header.epoch == ok.header.epoch);
            // A prepare may be committed in the same view or in a newer view:
            assert(prepare.message.header.view <= ok.header.view);
            assert(prepare.message.header.op == ok.header.op);
            assert(prepare.message.header.commit == ok.header.commit);
            assert(prepare.message.header.offset == ok.header.offset);
            assert(prepare.message.header.operation == ok.header.operation);

            return prepare;
        }

        /// Starting from the latest journal entry, backfill any missing or disconnected headers.
        /// A header is disconnected if it breaks the hash chain with its newer neighbor to the right.
        /// Since we work backwards from the latest entry, we should always be able to fix the chain.
        /// Once headers are connected, backfill any dirty or faulty prepares.
        fn repair(self: *Self) void {
            if (!self.repair_timeout.ticking) {
                log.debug("{}: repair: ignoring (optimistic, not ticking)", .{self.replica});
                return;
            }

            self.repair_timeout.reset();

            assert(self.status == .normal or self.status == .view_change);
            assert(self.repairs_allowed());
            assert(self.commit_min <= self.op);
            assert(self.commit_min <= self.commit_max);

            // We expect these always to exist:
            assert(self.journal.entry_for_op_exact(self.commit_min) != null);
            assert(self.journal.entry_for_op_exact(self.op) != null);

            // Request outstanding committed prepares to advance our op number:
            // This handles the case of an idle cluster, where a follower will not otherwise advance.
            // This is not required for correctness, but for durability.
            if (self.op < self.commit_max) {
                // If the leader repairs during a view change, it will have already advanced
                // `self.op` to the latest op according to the quorum of `do_view_change` messages
                // received, so we must therefore be a follower in normal status:
                assert(self.status == .normal);
                assert(self.follower());
                log.debug("{}: repair: op={} < commit_max={}", .{
                    self.replica,
                    self.op,
                    self.commit_max,
                });
                // We need to advance our op number and therefore have to `request_prepare`,
                // since only `on_prepare()` can do this, not `repair_header()` in `on_headers()`.
                self.send_header_to_replica(self.leader_index(self.view), .{
                    .command = .request_prepare,
                    // We cannot yet know the checksum of the prepare so we set the context to 0:
                    // Context is optional when requesting from the leader but required otherwise.
                    .context = 0,
                    .cluster = self.cluster,
                    .replica = self.replica,
                    .view = self.view,
                    .op = self.commit_max,
                });
                return;
            }

            // Request any missing or disconnected headers:
            // TODO Snapshots: Ensure that self.commit_min op always exists in the journal.
            var broken = self.journal.find_latest_headers_break_between(self.commit_min, self.op);
            if (broken) |range| {
                log.debug("{}: repair: break: view={} op_min={} op_max={} (commit={}..{} op={})", .{
                    self.replica,
                    self.view,
                    range.op_min,
                    range.op_max,
                    self.commit_min,
                    self.commit_max,
                    self.op,
                });
                assert(range.op_min > self.commit_min);
                assert(range.op_max < self.op);
                // A range of `op_min=0` or `op_max=0` should be impossible as a header break:
                // This is the init op that is prepared when the cluster is initialized.
                assert(range.op_min > 0);
                assert(range.op_max > 0);
                if (self.choose_any_other_replica()) |replica| {
                    self.send_header_to_replica(replica, .{
                        .command = .request_headers,
                        .cluster = self.cluster,
                        .replica = self.replica,
                        .view = self.view,
                        .commit = range.op_min,
                        .op = range.op_max,
                    });
                }
                return;
            }

            // Assert that all headers are now present and connected with a perfect hash chain:
            assert(self.op >= self.commit_max);
            assert(self.valid_hash_chain_between(self.commit_min, self.op));

            // Request and repair any dirty or faulty prepares:
            if (self.journal.dirty.len > 0) return self.repair_prepares();

            // Commit ops, which may in turn discover faulty prepares and drive more repairs:
            if (self.commit_min < self.commit_max) return self.commit_ops(self.commit_max);

            if (self.status == .view_change and self.leader_index(self.view) == self.replica) {
                if (self.repair_pipeline_op() != null) return self.repair_pipeline();

                // Start the view as the new leader:
                self.start_view_as_the_new_leader();
            }
        }

        /// Decide whether or not to insert or update a header:
        ///
        /// A repair may never advance or replace `self.op` (critical for correctness):
        ///
        /// Repairs must always backfill in behind `self.op` but may never advance `self.op`.
        /// Otherwise, a split-brain leader may reapply an op that was removed through a view
        /// change, which could be committed by a higher `commit_max` number in a commit message.
        ///
        /// See this commit message for an example:
        /// https://github.com/coilhq/tigerbeetle/commit/6119c7f759f924d09c088422d5c60ac6334d03de
        ///
        /// Our guiding principles around repairs in general:
        ///
        /// * The latest op makes sense of everything else and must not be replaced with a different
        /// op or advanced except by the leader in the current view.
        ///
        /// * Do not jump to a view in normal status without receiving a start_view message.
        ///
        /// * Do not commit until the hash chain between `self.commit_min` and `self.op` is fully
        /// connected, to ensure that all the ops in this range are correct.
        ///
        /// * Ensure that `self.commit_max` is never advanced for a newer view without first
        /// receiving a start_view message, otherwise `self.commit_max` may refer to different ops.
        ///
        /// * Ensure that `self.op` is never advanced by a repair since repairs may occur in a view
        /// change where the view has not yet started.
        ///
        /// * Do not assume that an existing op with a older viewstamp can be replaced by an op with
        /// a newer viewstamp, but only compare ops in the same view or with reference to the chain.
        /// See Figure 3.7 on page 41 in Diego Ongaro's Raft thesis for an example of where an op
        /// with an older view number may be committed instead of an op with a newer view number:
        /// http://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf.
        ///
        fn repair_header(self: *Self, header: *const Header) bool {
            assert(header.valid_checksum());
            assert(header.invalid() == null);
            assert(header.command == .prepare);

            switch (self.status) {
                .normal => assert(header.view <= self.view),
                .view_change => assert(header.view <= self.view),
                else => unreachable,
            }

            if (header.op > self.op) {
                log.debug("{}: repair_header: false (advances self.op)", .{self.replica});
                return false;
            } else if (header.op == self.op) {
                if (self.journal.entry_for_op_exact_with_checksum(self.op, header.checksum)) |_| {
                    // Fall through below to check if self.op is uncommitted AND reordered,
                    // which we would see by the presence of an earlier op with higher view number,
                    // that breaks the chain with self.op. In this case, we must skip the repair to
                    // avoid overwriting any overlapping op.
                } else {
                    log.debug("{}: repair_header: false (changes self.op={})", .{
                        self.replica,
                        self.op,
                    });
                    return false;
                }
            }

            if (self.journal.entry(header)) |existing| {
                // Do not replace any existing op lightly as doing so may impair durability and even
                // violate correctness by undoing a prepare already acknowledged to the leader:
                if (existing.checksum == header.checksum) {
                    if (!self.journal.dirty.bit(header.op)) {
                        log.debug("{}: repair_header: false (checksum clean)", .{self.replica});
                        return false;
                    }

                    log.debug("{}: repair_header: exists, checksum dirty", .{self.replica});
                } else if (existing.view == header.view) {
                    // The journal must have wrapped:
                    // We expect that the same view and op will have the same checksum.
                    assert(existing.op != header.op);

                    if (existing.op > header.op) {
                        log.debug("{}: repair_header: false (view has newer op)", .{self.replica});
                        return false;
                    }

                    log.debug("{}: repair_header: exists, view has older op", .{self.replica});
                } else {
                    assert(existing.view != header.view);
                    assert(existing.op == header.op or existing.op != header.op);

                    if (!self.repair_header_would_connect_hash_chain(header)) {
                        // We cannot replace this op until we are sure that doing so would not
                        // violate any prior commitments made to the leader.
                        log.debug("{}: repair_header: false (exists)", .{self.replica});
                        return false;
                    }

                    log.debug("{}: repair_header: exists, connects hash chain", .{self.replica});
                }
            } else {
                log.debug("{}: repair_header: gap", .{self.replica});
            }

            // Caveat: Do not repair an existing op or gap if doing so would break the hash chain:
            if (self.repair_header_would_break_hash_chain_with_next_entry(header)) {
                log.debug("{}: repair_header: false (breaks hash chain)", .{self.replica});
                return false;
            }

            // Caveat: Do not repair an existing op or gap if doing so would overlap another:
            if (self.repair_header_would_overlap_another(header)) {
                if (!self.repair_header_would_connect_hash_chain(header)) {
                    log.debug("{}: repair_header: false (overlap)", .{self.replica});
                    return false;
                }
                // We may have to overlap previous entries in order to connect the hash chain:
                log.debug("{}: repair_header: overlap, connects hash chain", .{self.replica});
            }

            // TODO Snapshots: Skip if this header is already snapshotted.

            assert(header.op < self.op or
                self.journal.entry_for_op_exact(self.op).?.checksum == header.checksum);

            self.journal.set_entry_as_dirty(header);
            return true;
        }

        /// If we repair this header, then would this break the hash chain only to our immediate right?
        /// This offers a weak guarantee compared to `repair_header_would_connect_hash_chain()` below.
        /// However, this is useful for allowing repairs when the hash chain is sparse.
        fn repair_header_would_break_hash_chain_with_next_entry(
            self: *Self,
            header: *const Header,
        ) bool {
            if (self.journal.previous_entry(header)) |previous| {
                self.panic_if_hash_chain_would_break_in_the_same_view(previous, header);
            }

            if (self.journal.next_entry(header)) |next| {
                self.panic_if_hash_chain_would_break_in_the_same_view(header, next);

                if (header.checksum == next.parent) {
                    assert(header.view <= next.view);
                    assert(header.op + 1 == next.op);
                    // We don't break with `next` but this is no guarantee that `next` does not break.
                    return false;
                } else {
                    // If the journal has wrapped, then err in favor of a break regardless of op order:
                    return true;
                }
            }

            // We are not completely sure since there is no entry to the immediate right:
            return false;
        }

        /// If we repair this header, then would this connect the hash chain through to the latest op?
        /// This offers a strong guarantee that may be used to replace or overlap an existing op.
        ///
        /// Here is an example of what could go wrong if we did not check for complete connection:
        ///
        /// 1. We do a prepare that's going to be committed.
        /// 2. We do a stale prepare to the right of this, ignoring the hash chain break to the left.
        /// 3. We do another stale prepare that replaces the first op because it connects to the second.
        ///
        /// This would violate our quorum replication commitment to the leader.
        /// The mistake in this example was not that we ignored the break to the left, which we must
        /// do to repair reordered ops, but that we did not check for connection to the right.
        fn repair_header_would_connect_hash_chain(self: *Self, header: *const Header) bool {
            var entry = header;

            while (entry.op < self.op) {
                if (self.journal.next_entry(entry)) |next| {
                    if (entry.checksum == next.parent) {
                        assert(entry.view <= next.view);
                        assert(entry.op + 1 == next.op);
                        entry = next;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            }

            assert(entry.op == self.op);
            assert(entry.checksum == self.journal.entry_for_op_exact(self.op).?.checksum);
            return true;
        }

        /// If we repair this header, then would this overlap and overwrite part of another batch?
        /// Journal entries have variable-sized batches that may overlap if entries are disconnected.
        fn repair_header_would_overlap_another(self: *Self, header: *const Header) bool {
            // TODO Snapshots: Handle journal wrap around.
            {
                // Look behind this entry for any preceeding entry that this would overlap:
                var op: u64 = header.op;
                while (op > 0) {
                    op -= 1;
                    if (self.journal.entry_for_op(op)) |neighbor| {
                        if (self.journal.next_offset(neighbor) > header.offset) return true;
                        break;
                    }
                }
            }
            {
                // Look beyond this entry for any succeeding entry that this would overlap:
                var op: u64 = header.op + 1;
                while (op <= self.op) : (op += 1) {
                    if (self.journal.entry_for_op(op)) |neighbor| {
                        if (self.journal.next_offset(header) > neighbor.offset) return true;
                        break;
                    }
                }
            }
            return false;
        }

        /// Reads prepares into the pipeline (before we start the view as the new leader).
        fn repair_pipeline(self: *Self) void {
            assert(self.status == .view_change);
            assert(self.leader_index(self.view) == self.replica);
            assert(self.commit_max < self.op);

            if (self.repairing_pipeline) {
                log.debug("{}: repair_pipeline: already repairing...", .{self.replica});
                return;
            }

            log.debug("{}: repair_pipeline: repairing", .{self.replica});

            assert(!self.repairing_pipeline);
            self.repairing_pipeline = true;

            self.repair_pipeline_read();
        }

        /// Returns the next `op` number that needs to be read into the pipeline.
        fn repair_pipeline_op(self: *Self) ?u64 {
            assert(self.status == .view_change);
            assert(self.leader_index(self.view) == self.replica);

            const op = self.commit_max + self.pipeline.count + 1;
            if (op <= self.op) return op;

            assert(self.commit_max + self.pipeline.count == self.op);
            return null;
        }

        fn repair_pipeline_read(self: *Self) void {
            assert(self.repairing_pipeline);
            assert(self.status == .view_change);
            assert(self.leader_index(self.view) == self.replica);

            if (self.repair_pipeline_op()) |op| {
                assert(op > self.commit_max);
                assert(op <= self.op);
                assert(self.commit_max + self.pipeline.count + 1 == op);

                const checksum = self.journal.entry_for_op_exact(op).?.checksum;

                log.debug("{}: repair_pipeline_read: op={} checksum={}", .{
                    self.replica,
                    op,
                    checksum,
                });

                self.journal.read_prepare(repair_pipeline_push, op, checksum, null);
            } else {
                log.debug("{}: repair_pipeline_read: repaired", .{self.replica});
                self.repairing_pipeline = false;
                self.repair();
            }
        }

        fn repair_pipeline_push(
            self: *Self,
            prepare: ?*Message,
            destination_replica: ?u8,
        ) void {
            assert(destination_replica == null);

            assert(self.repairing_pipeline);
            self.repairing_pipeline = false;

            if (prepare == null) {
                log.debug("{}: repair_pipeline_push: prepare == null", .{self.replica});
                return;
            }

            // Our state may have advanced significantly while we were reading from disk.
            if (self.status != .view_change) {
                log.debug("{}: repair_pipeline_push: no longer in view change status", .{
                    self.replica,
                });
                return;
            }

            if (self.leader_index(self.view) != self.replica) {
                log.debug("{}: repair_pipeline_push: no longer leader", .{self.replica});
                return;
            }

            // We may even be several views ahead and may now have a completely different pipeline.
            const op = self.repair_pipeline_op() orelse {
                log.debug("{}: repair_pipeline_push: pipeline changed", .{self.replica});
                return;
            };

            assert(op > self.commit_max);
            assert(op <= self.op);
            assert(self.commit_max + self.pipeline.count + 1 == op);

            if (prepare.?.header.op != op) {
                log.debug("{}: repair_pipeline_push: op changed", .{self.replica});
                return;
            }

            if (prepare.?.header.checksum != self.journal.entry_for_op_exact(op).?.checksum) {
                log.debug("{}: repair_pipeline_push: checksum changed", .{self.replica});
                return;
            }

            assert(self.status == .view_change);
            assert(self.leader_index(self.view) == self.replica);

            log.debug("{}: repair_pipeline_push: op={} checksum={}", .{
                self.replica,
                prepare.?.header.op,
                prepare.?.header.checksum,
            });

            self.pipeline.push(.{ .message = prepare.?.ref() }) catch unreachable;
            assert(self.pipeline.count >= 1);

            self.repairing_pipeline = true;
            self.repair_pipeline_read();
        }

        fn repair_prepares(self: *Self) void {
            assert(self.status == .normal or self.status == .view_change);
            assert(self.repairs_allowed());
            assert(self.journal.dirty.len > 0);

            // Request enough prepares to utilize our max IO depth:
            var budget = self.journal.writes.available();
            if (budget == 0) {
                log.debug("{}: repair_prepares: waiting for IOP", .{self.replica});
                return;
            }

            var op = self.op + 1;
            while (op > 0) {
                op -= 1;

                if (self.journal.dirty.bit(op)) {
                    // If this is an uncommitted op, and we are the leader in `view_change` status,
                    // then we will `request_prepare` from the cluster, set `nack_prepare_op`,
                    // and stop repairing any further prepares:
                    // This will also rebroadcast any `request_prepare` every `repair_timeout` tick.
                    if (self.repair_prepare(op)) {
                        if (self.nack_prepare_op) |nack_prepare_op| {
                            assert(nack_prepare_op == op);
                            assert(self.status == .view_change);
                            assert(self.leader_index(self.view) == self.replica);
                            assert(op > self.commit_max);
                            return;
                        }

                        // Otherwise, we continue to request prepares until our budget is used:
                        budget -= 1;
                        if (budget == 0) {
                            log.debug("{}: repair_prepares: request budget used", .{self.replica});
                            return;
                        }
                    }
                } else {
                    assert(!self.journal.faulty.bit(op));
                }
            }
        }

        /// During a view change, for uncommitted ops, which are few, we optimize for latency:
        ///
        /// * request a `prepare` or `nack_prepare` from all followers in parallel,
        /// * repair as soon as we get a `prepare`, or
        /// * discard as soon as we get a majority of `nack_prepare` messages for the same checksum.
        ///
        /// For committed ops, which represent the bulk of ops, we optimize for throughput:
        ///
        /// * have multiple requests in flight to prime the repair queue,
        /// * rotate these requests across the cluster round-robin,
        /// * to spread the load across connected peers,
        /// * to take advantage of each peer's outgoing bandwidth, and
        /// * to parallelize disk seeks and disk read bandwidth.
        ///
        /// This is effectively "many-to-one" repair, where a single replica recovers using the
        /// resources of many replicas, for faster recovery.
        fn repair_prepare(self: *Self, op: u64) bool {
            assert(self.status == .normal or self.status == .view_change);
            assert(self.repairs_allowed());
            assert(self.journal.dirty.bit(op));

            const checksum = self.journal.entry_for_op_exact(op).?.checksum;

            // We may be appending to or repairing the journal concurrently.
            // We do not want to re-request any of these prepares unnecessarily.
            if (self.journal.writing(op, checksum)) {
                log.debug("{}: repair_prepare: already writing op={} checksum={}", .{
                    self.replica,
                    op,
                    checksum,
                });
                return false;
            }

            const request_prepare = Header{
                .command = .request_prepare,
                // If we request a prepare from a follower, as below, it is critical to pass a checksum:
                // Otherwise we could receive different prepares for the same op number.
                .context = checksum,
                .cluster = self.cluster,
                .replica = self.replica,
                .view = self.view,
                .op = op,
            };

            if (self.status == .view_change and op > self.commit_max) {
                // Only the leader is allowed to do repairs in a view change:
                assert(self.leader_index(self.view) == self.replica);

                const reason = if (self.journal.faulty.bit(op)) "faulty" else "dirty";
                log.debug(
                    "{}: repair_prepare: op={} checksum={} (uncommitted, {s}, view_change)",
                    .{
                        self.replica,
                        op,
                        checksum,
                        reason,
                    },
                );

                if (self.replica_count == 2 and !self.journal.faulty.bit(op)) {
                    // This is required to avoid a liveness issue for a cluster-of-two where a new
                    // leader learns of an op during a view change but where the op is faulty on
                    // the old leader. We must immediately roll back the op since it could not have
                    // been committed by the old leader if we know we do not have it, and because
                    // the old leader cannot send a nack_prepare for its faulty copy.
                    // For this to be correct, the recovery protocol must set all headers as faulty,
                    // not only as dirty.
                    self.discard_uncommitted_ops_from(op, checksum);
                    return false;
                }

                // Initialize the `nack_prepare` quorum counter for this uncommitted op:
                // It is also possible that we may start repairing a lower uncommitted op, having
                // initialized `nack_prepare_op` before we learn of a higher uncommitted dirty op,
                // in which case we also want to reset the quorum counter.
                if (self.nack_prepare_op) |nack_prepare_op| {
                    assert(nack_prepare_op <= op);
                    if (nack_prepare_op != op) {
                        self.nack_prepare_op = op;
                        self.reset_quorum_messages(
                            &self.nack_prepare_from_other_replicas,
                            .nack_prepare,
                        );
                    }
                } else {
                    self.nack_prepare_op = op;
                    self.reset_quorum_messages(
                        &self.nack_prepare_from_other_replicas,
                        .nack_prepare,
                    );
                }

                assert(self.nack_prepare_op.? == op);
                assert(request_prepare.context == checksum);
                self.send_header_to_other_replicas(request_prepare);
            } else {
                const nature = if (op > self.commit_max) "uncommitted" else "committed";
                const reason = if (self.journal.faulty.bit(op)) "faulty" else "dirty";
                log.debug("{}: repair_prepare: op={} checksum={} ({s}, {s})", .{
                    self.replica,
                    op,
                    checksum,
                    nature,
                    reason,
                });

                // We expect that `repair_prepare()` is called in reverse chronological order:
                // Any uncommitted ops should have already been dealt with.
                // We never roll back committed ops, and thus never regard `nack_prepare` responses.
                // Alternatively, we may not be the leader, in which case we do distinguish anyway.
                assert(self.nack_prepare_op == null);
                assert(request_prepare.context == checksum);
                if (self.choose_any_other_replica()) |replica| {
                    self.send_header_to_replica(replica, request_prepare);
                }
            }

            return true;
        }

        fn repairs_allowed(self: *Self) bool {
            switch (self.status) {
                .view_change => {
                    if (self.do_view_change_quorum) {
                        assert(self.leader_index(self.view) == self.replica);
                        return true;
                    } else {
                        return false;
                    }
                },
                .normal => return true,
                else => return false,
            }
        }

        /// Replicates to the next replica in the configuration (until we get back to the leader):
        /// Replication starts and ends with the leader, we never forward back to the leader.
        /// Does not flood the network with prepares that have already committed.
        /// TODO Use recent heartbeat data for next replica to leapfrog if faulty (optimization).
        fn replicate(self: *Self, message: *Message) void {
            assert(self.status == .normal);
            assert(message.header.command == .prepare);
            assert(message.header.view == self.view);
            assert(message.header.op == self.op);

            if (message.header.op <= self.commit_max) {
                log.debug("{}: replicate: not replicating (committed)", .{self.replica});
                return;
            }

            const next = @mod(self.replica + 1, @intCast(u8, self.replica_count));
            if (next == self.leader_index(message.header.view)) {
                log.debug("{}: replicate: not replicating (completed)", .{self.replica});
                return;
            }

            log.debug("{}: replicate: replicating to replica {}", .{ self.replica, next });
            self.send_message_to_replica(next, message);
        }

        /// Empties the prepare pipeline, unreffing all prepare and prepare_ok messages.
        /// Stops the prepare timeout and resets the timeouts counter.
        fn reset_pipeline(self: *Self) void {
            while (self.pipeline.pop()) |prepare| {
                self.unref_prepare_message_and_quorum_messages(&prepare);
            }

            self.prepare_timeout.stop();

            assert(self.pipeline.count == 0);
            assert(self.prepare_timeout.ticking == false);

            // Do not reset `repairing_pipeline` here as this must be reset by the read callback.
            // Otherwise, we would be making `repair_pipeline()` reentrant.
        }

        fn reset_quorum_messages(self: *Self, messages: *QuorumMessages, command: Command) void {
            assert(messages.len == config.replicas_max);
            var view: ?u32 = null;
            var count: usize = 0;
            for (messages) |*received, replica| {
                if (received.*) |message| {
                    assert(replica < self.replica_count);
                    assert(message.header.command == command);
                    assert(message.header.replica == replica);
                    // We may have transitioned into a newer view:
                    // However, all messages in the quorum should have the same view.
                    assert(message.header.view <= self.view);
                    if (view) |v| {
                        assert(message.header.view == v);
                    } else {
                        view = message.header.view;
                    }

                    self.message_bus.unref(message);
                    count += 1;
                }
                received.* = null;
            }
            assert(count <= self.replica_count);
            log.debug("{}: reset {} {s} message(s)", .{ self.replica, count, @tagName(command) });
        }

        fn reset_quorum_do_view_change(self: *Self) void {
            self.reset_quorum_messages(&self.do_view_change_from_all_replicas, .do_view_change);
            self.do_view_change_quorum = false;
        }

        fn reset_quorum_nack_prepare(self: *Self) void {
            self.reset_quorum_messages(&self.nack_prepare_from_other_replicas, .nack_prepare);
            self.nack_prepare_op = null;
        }

        fn reset_quorum_start_view_change(self: *Self) void {
            self.reset_quorum_messages(&self.start_view_change_from_other_replicas, .start_view_change);
            self.start_view_change_quorum = false;
        }

        fn send_prepare_ok(self: *Self, header: *const Header) void {
            assert(header.command == .prepare);
            assert(header.cluster == self.cluster);
            assert(header.replica == self.leader_index(header.view));
            assert(header.view <= self.view);
            assert(header.op <= self.op or header.view < self.view);

            if (self.status != .normal) {
                log.debug("{}: send_prepare_ok: not sending ({})", .{ self.replica, self.status });
                return;
            }

            if (header.op > self.op) {
                assert(header.view < self.view);
                // An op may be reordered concurrently through a view change while being journalled:
                log.debug("{}: send_prepare_ok: not sending (reordered)", .{self.replica});
                return;
            }

            assert(self.status == .normal);
            // After a view change, replicas send prepare_oks for uncommitted ops with older views:
            // However, we only send to the leader of the current view (see below where we send).
            assert(header.view <= self.view);
            assert(header.op <= self.op);

            if (header.op <= self.commit_max) {
                log.debug("{}: send_prepare_ok: not sending (committed)", .{self.replica});
                return;
            }

            if (self.journal.has_clean(header)) {
                log.debug("{}: send_prepare_ok: op={} checksum={}", .{
                    self.replica,
                    header.op,
                    header.checksum,
                });

                // It is crucial that replicas stop accepting prepare messages from earlier views
                // once they start the view change protocol. Without this constraint, the system
                // could get into a state in which there are two active primaries: the old one,
                // which hasn't failed but is merely slow or not well connected to the network, and
                // the new one. If a replica sent a prepare_ok message to the old primary after
                // sending its log to the new one, the old primary might commit an operation that
                // the new primary doesn't learn about in the do_view_change messages.

                // We therefore only ever send to the leader of the current view, never to the
                // leader of the prepare header's view:
                self.send_header_to_replica(self.leader_index(self.view), .{
                    .command = .prepare_ok,
                    .parent = header.parent,
                    .client = header.client,
                    .context = header.checksum,
                    .request = header.request,
                    .cluster = self.cluster,
                    .replica = self.replica,
                    .epoch = header.epoch,
                    .view = self.view,
                    .op = header.op,
                    .commit = header.commit,
                    .offset = header.offset,
                    .operation = header.operation,
                });
            } else {
                log.debug("{}: send_prepare_ok: not sending (dirty)", .{self.replica});
                return;
            }
        }

        fn send_prepare_oks_after_view_change(self: *Self) void {
            assert(self.status == .normal);

            var op = self.commit_max + 1;
            while (op <= self.op) : (op += 1) {
                // We may have breaks or stale headers in our uncommitted chain here. However:
                // * being able to send what we have will allow the pipeline to commit earlier, and
                // * the leader will drop any prepare_ok for a prepare not in the pipeline.
                // This is safe only because the leader can verify against the prepare checksum.
                if (self.journal.entry_for_op_exact(op)) |header| {
                    self.send_prepare_ok(header);
                    defer self.flush_loopback_queue();
                }
            }
        }

        fn send_start_view_change(self: *Self) void {
            assert(self.status == .view_change);
            assert(!self.do_view_change_quorum);
            // Send only to other replicas (and not to ourself) to avoid a quorum off-by-one error:
            // This could happen if the replica mistakenly counts its own message in the quorum.
            self.send_header_to_other_replicas(.{
                .command = .start_view_change,
                .cluster = self.cluster,
                .replica = self.replica,
                .view = self.view,
            });
        }

        fn send_do_view_change(self: *Self) void {
            assert(self.status == .view_change);
            assert(self.start_view_change_quorum);
            assert(!self.do_view_change_quorum);
            const count_start_view_change = self.count_quorum(
                &self.start_view_change_from_other_replicas,
                .start_view_change,
                0,
            );
            assert(count_start_view_change >= self.quorum_view_change - 1);

            const message = self.create_view_change_message(.do_view_change) orelse {
                log.alert("{}: send_do_view_change: waiting for message", .{self.replica});
                return;
            };
            defer self.message_bus.unref(message);

            assert(message.references == 1);
            assert(message.header.command == .do_view_change);
            assert(message.header.view == self.view);
            assert(message.header.op == self.op);
            assert(message.header.commit == self.commit_max);
            // TODO Assert that latest header in message body matches self.op.

            self.send_message_to_replica(self.leader_index(self.view), message);
        }

        fn send_eviction_message_to_client(self: *Self, client: u128) void {
            assert(self.status == .normal);
            assert(self.leader());

            log.alert("{}: too many sessions, sending eviction message to client={}", .{
                self.replica,
                client,
            });

            self.send_header_to_client(client, .{
                .command = .eviction,
                .cluster = self.cluster,
                .replica = self.replica,
                .view = self.view,
                .client = client,
            });
        }

        fn send_header_to_client(self: *Self, client: u128, header: Header) void {
            const message = self.create_message_from_header(header) orelse {
                log.alert("{}: no header-only message available, dropping message to client {}", .{
                    self.replica,
                    client,
                });
                return;
            };
            defer self.message_bus.unref(message);

            self.message_bus.send_message_to_client(client, message);
        }

        fn send_header_to_other_replicas(self: *Self, header: Header) void {
            const message = self.create_message_from_header(header) orelse {
                log.alert("{}: no header-only message available, dropping message to replicas", .{
                    self.replica,
                });
                return;
            };
            defer self.message_bus.unref(message);

            var replica: u8 = 0;
            while (replica < self.replica_count) : (replica += 1) {
                if (replica != self.replica) {
                    self.send_message_to_replica(replica, message);
                }
            }
        }

        fn send_header_to_replica(self: *Self, replica: u8, header: Header) void {
            const message = self.create_message_from_header(header) orelse {
                log.alert("{}: no header-only message available, dropping message to replica {}", .{
                    self.replica,
                    replica,
                });
                return;
            };
            defer self.message_bus.unref(message);

            self.send_message_to_replica(replica, message);
        }

        fn send_message_to_other_replicas(self: *Self, message: *Message) void {
            var replica: u8 = 0;
            while (replica < self.replica_count) : (replica += 1) {
                if (replica != self.replica) {
                    self.send_message_to_replica(replica, message);
                }
            }
        }

        fn send_message_to_replica(self: *Self, replica: u8, message: *Message) void {
            log.debug("{}: sending {s} to replica {}: {}", .{
                self.replica,
                @tagName(message.header.command),
                replica,
                message.header,
            });

            if (message.header.invalid()) |reason| {
                log.emerg("{}: send_message_to_replica: invalid ({s})", .{ self.replica, reason });
                @panic("send_message_to_replica: invalid message");
            }

            assert(message.header.cluster == self.cluster);

            // TODO According to message.header.command, assert on the destination replica.
            switch (message.header.command) {
                .request => {
                    // Do not assert message.header.replica because we forward .request messages.
                    assert(self.status == .normal);
                    assert(message.header.view <= self.view);
                },
                .prepare => {
                    // Do not assert message.header.replica because we forward .prepare messages.
                    switch (self.status) {
                        .normal => assert(message.header.view <= self.view),
                        .view_change => assert(message.header.view < self.view),
                        else => unreachable,
                    }
                },
                .prepare_ok => {
                    assert(self.status == .normal);
                    assert(message.header.view == self.view);
                    // We must only ever send a prepare_ok to the latest leader of the active view:
                    // We must never straddle views by sending to a leader in an older view.
                    // Otherwise, we would be enabling a partitioned leader to commit.
                    assert(replica == self.leader_index(self.view));
                    assert(message.header.replica == self.replica);
                },
                .start_view_change => {
                    assert(self.status == .view_change);
                    assert(message.header.view == self.view);
                    assert(message.header.replica == self.replica);
                },
                .do_view_change => {
                    assert(self.status == .view_change);
                    assert(self.start_view_change_quorum);
                    assert(!self.do_view_change_quorum);
                    assert(message.header.view == self.view);
                    assert(message.header.replica == self.replica);
                    assert(message.header.op == self.op);
                    assert(replica == self.leader_index(self.view));
                },
                .start_view => switch (self.status) {
                    .normal => {
                        // A follower may ask the leader to resend the start_view message.
                        assert(!self.start_view_change_quorum);
                        assert(!self.do_view_change_quorum);
                        assert(message.header.view == self.view);
                        assert(message.header.replica == self.replica);
                    },
                    .view_change => {
                        assert(self.start_view_change_quorum);
                        assert(self.do_view_change_quorum);
                        assert(message.header.view == self.view);
                        assert(message.header.replica == self.replica);
                    },
                    else => unreachable,
                },
                .headers => {
                    assert(self.status == .normal or self.status == .view_change);
                    assert(message.header.view == self.view);
                    assert(message.header.replica == self.replica);
                },
                .ping, .pong => {
                    assert(message.header.view == self.view);
                    assert(message.header.replica == self.replica);
                },
                .commit => {
                    assert(self.status == .normal);
                    assert(self.leader());
                    assert(message.header.view == self.view);
                    assert(message.header.replica == self.replica);
                },
                .request_headers => {
                    assert(message.header.view == self.view);
                    assert(message.header.replica == self.replica);
                },
                .request_prepare => {
                    assert(message.header.view == self.view);
                    assert(message.header.replica == self.replica);
                },
                .nack_prepare => {
                    assert(message.header.view == self.view);
                    assert(message.header.replica == self.replica);
                    assert(replica == self.leader_index(self.view));
                },
                else => {
                    log.notice("{}: send_message_to_replica: TODO {s}", .{
                        self.replica,
                        @tagName(message.header.command),
                    });
                },
            }

            if (replica == self.replica) {
                assert(self.loopback_queue == null);
                self.loopback_queue = message.ref();
            } else {
                self.message_bus.send_message_to_replica(replica, message);
            }
        }

        /// Finds the header with the highest op number in a slice of headers from a replica.
        /// Searches only by op number to find the highest `self.op for the replica.
        fn set_latest_op(self: *Self, headers: []Header, latest: *Header) void {
            switch (latest.command) {
                .reserved, .prepare => assert(latest.valid_checksum()),
                else => unreachable,
            }

            for (headers) |header| {
                assert(header.valid_checksum());
                assert(header.invalid() == null);
                assert(header.command == .prepare);

                if (latest.command == .reserved or header.op > latest.op) {
                    // We are simply trying to find the latest `self.op` in the replica's log.
                    // We therefore do not compare views here.
                    latest.* = header;
                }
            }
        }

        fn set_latest_op_and_k(
            self: *Self,
            latest: *const Header,
            k: u64,
            method: []const u8,
        ) void {
            assert(self.status == .view_change);

            assert(latest.valid_checksum());
            assert(latest.invalid() == null);
            assert(latest.command == .prepare);
            assert(latest.cluster == self.cluster);

            // The view may have started already, so we can have a prepare in the same view:
            assert(latest.view <= self.view);

            log.debug("{}: {s}: view={} op={}..{} commit={}..{} checksum={} offset={}", .{
                self.replica,
                method,
                self.view,
                self.op,
                latest.op,
                self.commit_max,
                k,
                latest.checksum,
                latest.offset,
            });

            // Uncommitted ops may not survive a view change so we must assert `latest.op` against
            // `commit_max` and not `self.op`. However, committed ops (`commit_max`) must survive:
            assert(latest.op >= self.commit_max);
            assert(latest.op >= latest.commit);
            assert(latest.op >= k);
            // We expect that `commit_max` (and `commit_min`) may be greater than `latest.commit`
            // because `latest.commit` is the commit number at the time the `latest.op` prepared.
            // We expect that `commit_max` (and `commit_min`) may also be greater even than `k`
            // because we may be the old leader joining towards the end of the view change and we
            // may have committed the `latest.op` already. However, this is bounded by pipelining.
            // The intersection property only requires that all "possibly" committed operations must
            // survive into the new view so that they can then be committed by the new leader. This
            // guarantees that if the old leader "possibly" committed the operation, then the new
            // leader will also commit the operation.
            if (k < self.commit_max and self.commit_min == self.commit_max) {
                log.debug("{}: {s}: k={} < commit_max={} and commit_min == commit_max", .{
                    self.replica,
                    method,
                    k,
                    self.commit_max,
                });
            }
            assert(k >= latest.commit);
            assert(k >= self.commit_max - std.math.min(config.pipelining_max, self.commit_max));

            assert(self.commit_min <= self.commit_max);
            assert(self.op >= self.commit_max or self.op < self.commit_max);

            self.op = latest.op;
            // Crucially, we must never rewind `commit_max` (and then `commit_min`) because
            // `commit_min` represents what we have already applied to our state machine:
            self.commit_max = std.math.max(self.commit_max, k);

            assert(self.commit_min <= self.commit_max);
            assert(self.op >= self.commit_max);

            // Do not set the latest op as dirty if we already have it exactly:
            // Otherwise, this would trigger a repair and delay the view change, or worse, it would
            // prevent us from assisting another replica to recover when we do in fact have the op.
            if (self.journal.entry_for_op_exact_with_checksum(latest.op, latest.checksum)) |_| {
                log.debug("{}: {s}: latest op exists exactly", .{ self.replica, method });
            } else {
                self.journal.set_entry_as_dirty(latest);
            }

            assert(self.op == latest.op);
            self.journal.remove_entries_from(self.op + 1);
            assert(self.journal.entry_for_op_exact(self.op).?.checksum == latest.checksum);
        }

        fn start_view_as_the_new_leader(self: *Self) void {
            assert(self.status == .view_change);
            assert(self.leader_index(self.view) == self.replica);
            assert(self.do_view_change_quorum);

            assert(!self.committing);
            assert(!self.repairing_pipeline);

            assert(self.commit_min == self.commit_max);
            assert(self.repair_pipeline_op() == null);
            assert(self.commit_max + self.pipeline.count == self.op);
            assert(self.valid_hash_chain_between(self.commit_min, self.op));

            var pipeline_op = self.commit_max + 1;
            var pipeline_parent = self.journal.entry_for_op_exact(self.commit_max).?.checksum;
            var iterator = self.pipeline.iterator();
            while (iterator.next_ptr()) |prepare| {
                assert(prepare.message.header.command == .prepare);
                assert(prepare.message.header.op == pipeline_op);
                assert(prepare.message.header.parent == pipeline_parent);

                pipeline_parent = prepare.message.header.checksum;
                pipeline_op += 1;
            }
            assert(self.pipeline.count <= config.pipelining_max);
            assert(self.commit_max + self.pipeline.count == pipeline_op - 1);

            assert(self.journal.dirty.len == 0);
            assert(self.journal.faulty.len == 0);
            assert(self.nack_prepare_op == null);

            const start_view = self.create_view_change_message(.start_view) orelse {
                log.alert("{}: start_view_as_the_new_leader: waiting for message", .{self.replica});
                return;
            };
            defer self.message_bus.unref(start_view);

            self.transition_to_normal_status(self.view);
            // Detect if the transition to normal status above accidentally resets the pipeline:
            assert(self.commit_max + self.pipeline.count == self.op);

            assert(self.status == .normal);
            assert(self.leader());

            assert(start_view.references == 1);
            assert(start_view.header.command == .start_view);
            assert(start_view.header.view == self.view);
            assert(start_view.header.op == self.op);
            assert(start_view.header.commit == self.commit_max);

            // Send prepare_ok messages to ourself to contribute to the pipeline.
            self.send_prepare_oks_after_view_change();

            self.send_message_to_other_replicas(start_view);
        }

        fn transition_to_normal_status(self: *Self, new_view: u32) void {
            log.debug("{}: transition_to_normal_status: view={}", .{ self.replica, new_view });
            // In the VRR paper it's possible to transition from normal to normal for the same view.
            // For example, this could happen after a state transfer triggered by an op jump.
            assert(new_view >= self.view);
            self.view = new_view;
            self.view_normal = new_view;
            self.status = .normal;

            if (self.leader()) {
                log.debug("{}: transition_to_normal_status: leader", .{self.replica});

                self.ping_timeout.start();
                self.commit_timeout.start();
                self.normal_status_timeout.stop();
                self.view_change_status_timeout.stop();
                self.view_change_message_timeout.stop();
                self.repair_timeout.start();

                // Do not reset the pipeline as there may be uncommitted ops to drive to completion.
                if (self.pipeline.count > 0) {
                    assert(!self.prepare_timeout.ticking);
                    self.prepare_timeout.start();
                }
            } else {
                log.debug("{}: transition_to_normal_status: follower", .{self.replica});

                self.ping_timeout.start();
                self.commit_timeout.stop();
                self.normal_status_timeout.start();
                self.view_change_status_timeout.stop();
                self.view_change_message_timeout.stop();
                self.repair_timeout.start();

                self.reset_pipeline();
            }

            self.reset_quorum_start_view_change();
            self.reset_quorum_do_view_change();
            self.reset_quorum_nack_prepare();

            assert(self.start_view_change_quorum == false);
            assert(self.do_view_change_quorum == false);
            assert(self.nack_prepare_op == null);
        }

        /// A replica i that notices the need for a view change advances its view, sets its status to
        /// view_change, and sends a ⟨start_view_change v, i⟩ message to all the other replicas,
        /// where v identifies the new view. A replica notices the need for a view change either based
        /// on its own timer, or because it receives a start_view_change or do_view_change message for
        /// a view with a larger number than its own view.
        fn transition_to_view_change_status(self: *Self, new_view: u32) void {
            log.debug("{}: transition_to_view_change_status: view={}..{}", .{
                self.replica,
                self.view,
                new_view,
            });
            assert(new_view > self.view);
            self.view = new_view;
            self.status = .view_change;

            self.ping_timeout.stop();
            self.commit_timeout.stop();
            self.normal_status_timeout.stop();
            self.view_change_status_timeout.start();
            self.view_change_message_timeout.start();
            self.repair_timeout.stop();

            // Do not reset quorum counters only on entering a view, assuming that the view will be
            // followed only by a single subsequent view change to the next view, because multiple
            // successive view changes can fail, e.g. after a view change timeout.
            // We must therefore reset our counters here to avoid counting messages from an older view,
            // which would violate the quorum intersection property essential for correctness.
            self.reset_pipeline();
            self.reset_quorum_start_view_change();
            self.reset_quorum_do_view_change();
            self.reset_quorum_nack_prepare();

            assert(self.start_view_change_quorum == false);
            assert(self.do_view_change_quorum == false);
            assert(self.nack_prepare_op == null);

            self.send_start_view_change();
        }

        fn unref_prepare_message_and_quorum_messages(
            self: *Self,
            prepare: *const Prepare,
        ) void {
            self.message_bus.unref(prepare.message);
            for (prepare.ok_from_all_replicas) |received, replica| {
                if (received) |prepare_ok| {
                    assert(replica < self.replica_count);
                    self.message_bus.unref(prepare_ok);
                }
            }
        }

        fn update_client_table_entry(self: *Self, reply: *Message) void {
            assert(reply.header.command == .reply);
            assert(reply.header.operation != .register);
            assert(reply.header.client > 0);
            assert(reply.header.context == 0);
            assert(reply.header.op == reply.header.commit);
            assert(reply.header.commit > 0);
            assert(reply.header.request > 0);

            if (self.client_table.getPtr(reply.header.client)) |entry| {
                assert(entry.reply.header.command == .reply);
                assert(entry.reply.header.context == 0);
                assert(entry.reply.header.op == entry.reply.header.commit);
                assert(entry.reply.header.commit >= entry.session);

                assert(entry.reply.header.client == reply.header.client);
                assert(entry.reply.header.request + 1 == reply.header.request);
                assert(entry.reply.header.op < reply.header.op);
                assert(entry.reply.header.commit < reply.header.commit);

                // TODO Use this reply's prepare to cross-check against the entry's prepare, if we
                // still have access to the prepare in the journal (it may have been snapshotted).

                log.debug("{}: update_client_table_entry: client={} session={} request={}", .{
                    self.replica,
                    reply.header.client,
                    entry.session,
                    reply.header.request,
                });

                self.message_bus.unref(entry.reply);
                entry.reply = reply.ref();
            } else {
                // If no entry exists, then the session must have been evicted while being prepared.
                // We can still send the reply, the next request will receive an eviction message.
            }
        }

        /// Whether it is safe to commit or send prepare_ok messages.
        /// Returns true if the hash chain is valid and up to date for the current view.
        /// This is a stronger guarantee than `valid_hash_chain_between()` below.
        fn valid_hash_chain(self: *Self, method: []const u8) bool {
            // If we know we could validate the hash chain even further, then wait until we can:
            // This is partial defense-in-depth in case `self.op` is ever advanced by a reordered op.
            if (self.op < self.commit_max) {
                log.debug("{}: {s}: waiting for repair (op={} < commit={})", .{
                    self.replica,
                    method,
                    self.op,
                    self.commit_max,
                });
                return false;
            }

            // We must validate the hash chain as far as possible, since `self.op` may disclose a fork:
            if (!self.valid_hash_chain_between(self.commit_min, self.op)) {
                log.debug("{}: {s}: waiting for repair (hash chain)", .{ self.replica, method });
                return false;
            }

            return true;
        }

        /// Returns true if all operations are present, correctly ordered and connected by hash chain,
        /// between `op_min` and `op_max` (both inclusive).
        fn valid_hash_chain_between(self: *Self, op_min: u64, op_max: u64) bool {
            assert(op_min <= op_max);

            // If we use anything less than self.op then we may commit ops for a forked hash chain that
            // have since been reordered by a new leader.
            assert(op_max == self.op);
            var b = self.journal.entry_for_op_exact(op_max).?;

            var op = op_max;
            while (op > op_min) {
                op -= 1;

                if (self.journal.entry_for_op_exact(op)) |a| {
                    assert(a.op + 1 == b.op);
                    if (a.checksum == b.parent) {
                        assert(self.ascending_viewstamps(a, b));
                        b = a;
                    } else {
                        log.debug("{}: valid_hash_chain_between: break: A: {}", .{ self.replica, a });
                        log.debug("{}: valid_hash_chain_between: break: B: {}", .{ self.replica, b });
                        return false;
                    }
                } else {
                    log.debug("{}: valid_hash_chain_between: missing op={}", .{ self.replica, op });
                    return false;
                }
            }
            assert(b.op == op_min);
            return true;
        }

        fn view_jump(self: *Self, header: *const Header) void {
            const to: Status = switch (header.command) {
                .prepare, .commit => .normal,
                .start_view_change, .do_view_change, .start_view => .view_change,
                else => unreachable,
            };

            if (self.status != .normal and self.status != .view_change) return;

            if (header.view < self.view) return;

            // Compare status transitions and decide whether to view jump or ignore:
            switch (self.status) {
                .normal => switch (to) {
                    // If the transition is to `.normal`, then ignore if for the same view:
                    .normal => if (header.view == self.view) return,
                    // If the transition is to `.view_change`, then ignore if the view has started:
                    .view_change => if (header.view == self.view) return,
                    else => unreachable,
                },
                .view_change => switch (to) {
                    // This is an interesting special case:
                    // If the transition is to `.normal` in the same view, then we missed the
                    // `start_view` message and we must also consider this a view jump:
                    // If we don't handle this below then our `view_change_status_timeout` will fire
                    // and we will disrupt the cluster with another view change for a newer view.
                    .normal => {},
                    // If the transition is to `.view_change`, then ignore if for the same view:
                    .view_change => if (header.view == self.view) return,
                    else => unreachable,
                },
                else => unreachable,
            }

            switch (to) {
                .normal => {
                    if (header.view == self.view) {
                        assert(self.status == .view_change);

                        log.debug("{}: view_jump: waiting to exit view change", .{self.replica});
                    } else {
                        assert(header.view > self.view);
                        assert(self.status == .view_change or self.status == .normal);

                        log.debug("{}: view_jump: waiting to jump to newer view", .{self.replica});
                    }

                    // TODO Debounce and decouple this from `on_message()` by moving into `tick()`:
                    log.debug("{}: view_jump: requesting start_view message", .{self.replica});
                    self.send_header_to_replica(self.leader_index(header.view), .{
                        .command = .request_start_view,
                        .cluster = self.cluster,
                        .replica = self.replica,
                        .view = header.view,
                    });
                },
                .view_change => {
                    assert(header.view > self.view);
                    assert(self.status == .view_change or self.status == .normal);

                    if (header.view == self.view + 1) {
                        log.debug("{}: view_jump: jumping to view change", .{self.replica});
                    } else {
                        log.debug("{}: view_jump: jumping to next view change", .{self.replica});
                    }
                    self.transition_to_view_change_status(header.view);
                },
                else => unreachable,
            }
        }

        fn write_prepare(self: *Self, message: *Message, trigger: Journal.Write.Trigger) void {
            assert(message.references > 0);
            assert(message.header.command == .prepare);
            assert(message.header.view <= self.view);
            assert(message.header.op <= self.op);

            if (!self.journal.has(message.header)) {
                log.debug("{}: write_prepare: ignoring op={} checksum={} (header changed)", .{
                    self.replica,
                    message.header.op,
                    message.header.checksum,
                });
                return;
            }

            if (self.journal.writing(message.header.op, message.header.checksum)) {
                log.debug("{}: write_prepare: ignoring op={} checksum={} (already writing)", .{
                    self.replica,
                    message.header.op,
                    message.header.checksum,
                });
                return;
            }

            self.journal.write_prepare(write_prepare_on_write, message, trigger);
        }

        fn write_prepare_on_write(
            self: *Self,
            wrote: ?*Message,
            trigger: Journal.Write.Trigger,
        ) void {
            // `null` indicates that we did not complete the write for some reason.
            const message = wrote orelse return;

            self.send_prepare_ok(message.header);
            defer self.flush_loopback_queue();

            switch (trigger) {
                .append => {},
                // If this was a repair, continue immediately to repair the next prepare:
                // This is an optimization to eliminate waiting until the next repair timeout.
                .repair => self.repair(),
            }
        }
    };
}
