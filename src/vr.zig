const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.vr);
pub const log_level: std.log.Level = .debug;

const Command = enum {
    prepare,
    prepare_ok,
    commit,
    request_state_transfer,
    state_transfer,
    start_view_change,
    do_view_change,
    start_view,
};

const Message = struct {
    replica: u32,

    // Every message sent from one replica to another contains the sender's current view.
    view: u64,

    command: Command,

    // TODO Remove default values:
    op: u64 = 0,
    commit: u64 = 0,
    latest_normal_view: u64 = 0,
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
    /// Time is measured in logical ticks that are incremented on every call to tick():
    /// This eliminates a dependency on the system time and enables deterministic testing.
    ticks: u64 = 0,

    /// The maximum number of replicas that may be faulty:
    f: u32,

    /// A sorted array containing the IP addresses of each of the 2f + 1 replicas:
    configuration: [3]u32,

    /// An array of outgoing messages, one for each of the 2f + 1 replicas:
    message: [3]?Message,

    /// The index into the configuration where this replica's IP address is stored:
    index: u32,

    /// The current view, initially 0:
    view: u64 = 0,

    /// The current status, either normal, view_change, or recovering:
    status: Status = .normal,

    /// The op number assigned to the most recently received request, initially 0:
    op: u64 = 0,

    /// The op number of the most recently committed operation:
    commit: ?u64 = null,

    /// Unique start_view_change messages for the same view from OTHER replicas:
    start_view_change_from_other_replicas: [3]?Message,

    /// Unique do_view_change messages for the same view from ALL replicas (including ourself):
    do_view_change_from_all_replicas: [3]?Message,

    /// The number of ticks without sending a prepare before the leader sends a commit heartbeat:
    timeout_commit: Timeout = Timeout{ .after = 100 },

    /// The number of ticks without hearing from the leader before a follower starts a view change:
    timeout_view: Timeout = Timeout{ .after = 1000 },

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
            .start_view_change => self.on_start_view_change(message),
            .do_view_change => self.on_do_view_change(message),
            .start_view => self.on_start_view(message),
            else => unreachable,
        }
    }

    pub fn tick(self: *Replica) void {
        if (self.ticks == 0) {
            if (self.leader()) {
                self.timeout_commit.start();
            } else {
                self.timeout_view.start();
            }
        }

        self.ticks += 1;

        self.timeout_commit.tick();
        self.timeout_view.tick();

        if (self.timeout_commit.fired()) {
            assert(self.leader());
            // TODO Send a commit message to other replicas.
        }

        if (self.timeout_view.fired()) {
            assert(self.follower());
            log.debug("{}: view timed out", .{self.index});
            self.start_view_change(self.view + 1);
        }
    }

    fn on_start_view_change(self: *Replica, message: Message) void {
        // A start_view_change message can only be sent to other replicas:
        assert(message.replica != self.index);

        // Any view change must be greater than zero since the initial view is already zero:
        assert(message.view > 0);

        if (message.view < self.view) {
            log.debug("{}: on_start_view_change: older view", .{self.index});
            return;
        }

        if (message.view == self.view and self.status != .view_change) {
            switch (self.status) {
                .normal => log.debug("{}: on_start_view_change: already started", .{self.index}),
                .recovering => log.debug("{}: on_start_view_change: recovering", .{self.index}),
                .view_change => unreachable,
            }
            return;
        }

        if (message.view > self.view) {
            log.debug("{}: on_start_view_change: newer view", .{self.index});
            self.start_view_change(message.view);
            // Continue below...
        }

        assert(self.view == message.view);
        assert(self.status == .view_change);

        // Do not allow duplicate messages to trigger multiple passes through the state transition:
        if (self.start_view_change_from_other_replicas[message.replica]) |m| {
            // Assert that this truly is a duplicate message and not a different message:
            assert(m.command == .start_view_change);
            assert(m.replica == message.replica);
            assert(m.view == message.view);
            log.debug("{}: on_start_view_change: duplicate message", .{self.index});
            return;
        }

        // Record the first receipt of this start_view_change message:
        assert(self.start_view_change_from_other_replicas[message.replica] == null);
        self.start_view_change_from_other_replicas[message.replica] = message;

        // Count the number of unique messages received from other replicas:
        var count: usize = 0;
        assert(self.start_view_change_from_other_replicas.len == self.configuration.len);
        for (self.start_view_change_from_other_replicas) |received, replica| {
            if (received) |m| {
                assert(replica != self.index);
                assert(m.command == .start_view_change);
                assert(m.replica == replica);
                assert(m.view == self.view);
                count += 1;
            }
        }
        log.debug("{}: on_start_view_change: {} message(s)", .{ self.index, count });

        // Wait until we have `f` messages (excluding ourself) for quorum:
        if (count < self.f) {
            log.debug("{}: on_start_view_change: waiting for quorum", .{self.index});
            return;
        }

        // This is not the first time we have had quorum, the state transition has already happened:
        if (count > self.f) {
            log.debug("{}: on_start_view_change: quorum received already", .{self.index});
            return;
        }

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
            log.debug("{}: on_do_view_change: older view", .{self.index});
            return;
        }

        if (message.view == self.view and self.status != .view_change) {
            switch (self.status) {
                .normal => log.debug("{}: on_do_view_change: already started", .{self.index}),
                .recovering => log.debug("{}: on_do_view_change: recovering", .{self.index}),
                .view_change => unreachable,
            }
            return;
        }

        if (message.view > self.view) {
            log.debug("{}: on_do_view_change: newer view", .{self.index});
            // At first glance, it might seem that sending start_view_change messages out now after
            // receiving a do_view_change here would be superfluous. However, this is essential,
            // especially in the presence of asymmetric network faults. At this point, we may not
            // yet have received a do_view_change quorum, and another replica might need our
            // start_view_change message in order to get its do_view_change message through to us.
            // We therefore do not special case the start_view_change function, and we always send
            // start_view_change messages, regardless of the message that initiated the view change:
            self.start_view_change(message.view);
            // Continue below...
        }

        assert(self.view == message.view);
        assert(self.status == .view_change);
        assert(self.leader_index(self.view) == self.index);

        // Do not allow duplicate messages to trigger multiple passes through the state transition:
        if (self.do_view_change_from_all_replicas[message.replica]) |m| {
            // Assert that this truly is a duplicate message and not a different message:
            // TODO Check that these assertions cover all fields of a do_view_change message:
            assert(m.command == .do_view_change);
            assert(m.replica == message.replica);
            assert(m.view == message.view);
            assert(m.latest_normal_view == message.latest_normal_view);
            assert(m.op == message.op);
            assert(m.commit == message.commit);
            log.debug("{}: on_do_view_change: duplicate message", .{self.index});
            return;
        }

        // Record the first receipt of this do_view_change message:
        assert(self.do_view_change_from_all_replicas[message.replica] == null);
        self.do_view_change_from_all_replicas[message.replica] = message;

        // Count the number of unique messages received from all replicas:
        var count: usize = 0;
        assert(self.do_view_change_from_all_replicas.len == self.configuration.len);
        for (self.do_view_change_from_all_replicas) |received, replica| {
            if (received) |m| {
                // A replica must send a do_view_change to itself if it will be the new leader.
                // We therefore do not assert(replica != self.index) as we do for start_view_change.
                assert(m.command == .do_view_change);
                assert(m.replica == replica);
                assert(m.view == self.view);
                assert(m.latest_normal_view < self.view);
                count += 1;
            }
        }
        log.debug("{}: on_do_view_change: {} message(s)", .{ self.index, count });

        // Wait until we have `f + 1` messages (including ourself) for quorum:
        if (count < self.f + 1) {
            log.debug("{}: on_do_view_change: waiting for quorum", .{self.index});
            return;
        }

        // This is not the first time we have had quorum, the state transition has already happened:
        if (count > self.f + 1) {
            log.debug("{}: on_do_view_change: quorum received already", .{self.index});
            return;
        }

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
    }

    fn on_start_view(self: *Replica, message: Message) void {
        assert(message.replica != self.index);
        assert(message.view > 0);

        if (message.view < self.view) {
            log.debug("{}: on_start_view: older view", .{self.index});
            return;
        }

        if (message.view == self.view and self.status != .view_change) {
            switch (self.status) {
                .normal => log.debug("{}: on_start_view: already started", .{self.index}),
                .recovering => log.debug("{}: on_start_view: recovering", .{self.index}),
                .view_change => unreachable,
            }
            return;
        }

        assert(self.leader_index(message.view) != self.index);

        // TODO Assert that start_view message matches what we expect if our journal is empty.
        // TODO Assert that start_view message's oldest op overlaps with our last commit number.
        // TODO Update the journal.

        self.enter_view(message.view);

        // TODO Update our last op number according to message.

        assert(!self.leader());

        // TODO self.commit(msg.lastcommitted());
        // TODO self.send_prepare_oks(oldLastOp);
    }

    fn enter_view(self: *Replica, new_view: u64) void {
        // TODO: Refine assertion (the new view may be the same as the current view if ...):
        assert(new_view >= self.view);
        // TODO Assert which states are allowed to call enter_view().
        std.debug.print("{}: entering new view {}\n", .{ self.index, new_view });
        self.view = new_view;
        self.status = .normal;

        if (self.leader()) {
            self.timeout_commit.start();
            self.timeout_view.stop();
        } else {
            self.timeout_commit.stop();
            self.timeout_view.start();
            // TODO Stop "resend prepare" timeout.
        }
        
        // TODO Reset prepare_ok message counters.
    }

    /// A replica i that notices the need for a view change advances its view, sets its status to
    /// view_change, and sends a ⟨start_view_change v, i⟩ message to all the other replicas,
    /// where v identifies the new view. A replica notices the need for a view change either based
    /// on its own timer, or because it receives a start_view_change or do_view_change message for
    /// a view with a larger number than its own view.
    fn start_view_change(self: *Replica, new_view: u64) void {
        log.debug("{}: starting view change", .{self.index});
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

        self.timeout_view.start();
        assert(self.timeout_view.ticks == 0);
        assert(self.timeout_view.ticking == true);

        self.timeout_commit.stop();
        assert(self.timeout_commit.ticks == 0);
        assert(self.timeout_commit.ticking == false);

        // TODO Stop "resend prepare" timeout.

        // Send only to other replicas (and not to ourself) to avoid a quorum off-by-one error:
        // This could happen if the replica mistakenly counts its own message in the quorum.
        log.debug("{}: sending start_view_change to other replicas:", .{self.index});
        self.send_message_to_other_replicas(.{
            .command = .start_view_change,
            .replica = self.index,
            .view = new_view,
        });
    }

    fn send_message_to_other_replicas(self: *Replica, message: Message) void {
        for (self.message) |_, replica| {
            if (replica != self.index) {
                self.send_message_to_replica(replica, message);
            }
        }
    }

    fn send_message_to_replica(self: *Replica, replica: usize, message: Message) void {
        log.debug("{}: sending {} to replica {}: {}", .{
            self.index,
            @tagName(message.command),
            replica,
            message,
        });
        if (replica == self.index) {
            // Bypass the external network queue:
            // A replica should not need to keep a TCP connection open to itself.
            self.on_message(message);
        } else {
            self.message[replica] = message;
        }
    }
};

pub fn main() void {
    std.debug.print("\n", .{});
    const f = 1;

    var replicas: [2 * f + 1]Replica = undefined;
    for (replicas) |*replica, index| {
        // TODO Assign nulls to message, start_view_change... and do_view_change...:
        replica.* = .{
            .f = f,
            .configuration = .{ 0, 1, 2 },
            .index = @intCast(u32, index),
            .message = undefined,
            .start_view_change_from_other_replicas = undefined,
            .do_view_change_from_all_replicas = undefined,
        };
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
