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

    /// A bitmap of start_view_change messages from other replicas (for current view):
    /// We use a bitmap instead of a simple counter to tolerate message duplicates.
    start_view_change_from_other_replicas: u64 = 0,

    /// Here, we also want to keep the actual do_view_change messages received from all replicas:
    do_view_change_from_all_replicas: [3]?Message,

    fn enter_view(self: *Replica, view: u64) void {
        std.debug.print("replica {}: entering new view {}\n", .{ self.index, view });
        assert(view >= self.view);
        self.view = view;
        self.status = .normal;

        // TODO Stop timeouts
        // TODO Start commit_timeout if leader
        // TODO Start view_change_timeout if follower
        // TODO Reset prepare_ok, start_view_change and do_view_change message counters.
    }

    pub fn on_message(self: *Replica, message: *const Message) void {
        log.debug("", .{});
        log.debug("{}: on_message: {}", .{ self.index, message });
        assert(message.replica != self.index);
        assert(message.replica < self.configuration.len);
        switch (message.command) {
            .start_view_change => self.on_start_view_change(message),
            .do_view_change => self.on_do_view_change(message),
            .start_view => self.on_start_view(message),
            else => unreachable,
        }
    }

    fn on_start_view_change(self: *Replica, message: *const Message) void {
        assert(message.replica != self.index);

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

        // Switch on a bit to track that a replica has sent us a start_view_change (idempotent):
        // A 64-bit integer can be shifted by at most 6 bits (2 ^ 6 = 64 bits) without overflow.
        const bit = @as(u64, 1) << @intCast(u6, message.replica);

        // Do not allow duplicate messages to trigger sending a redundant do_view_change message:
        // We want to prevent multiple passes through the same state transition.
        // We want to be precise and exact and handle this state transition exactly once.
        if ((self.start_view_change_from_other_replicas & bit) != 0) {
            log.debug("{}: on_start_view_change: duplicate message", .{self.index});
            return;
        }

        // Record the first receipt of this start_view_change message from the other replica:
        assert(bit != self.index);
        self.start_view_change_from_other_replicas |= bit;

        // Count the number of unique messages received from other replicas:
        const count = @popCount(u64, self.start_view_change_from_other_replicas);
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
        const new_leader = self.leader(self.view);
        self.send_message_to_replica(new_leader, Message{
            .replica = self.index,
            .view = self.view,
            .command = .do_view_change,
        });
        // TODO Add log, last normal view, op number, and commit number.
        // These will be used by the new leader to decide on the longest log.
    }

    fn on_do_view_change(self: *Replica, message: *const Message) void {
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
        assert(self.leader(self.view) == self.index);

        // Do not allow duplicate messages to trigger multiple passes through the state transition:
        if (self.do_view_change_from_all_replicas[message.replica]) |duplicate| {
            assert(duplicate.view == message.view);
            assert(duplicate.replica == message.replica);
            assert(duplicate.command == .do_view_change);
            log.debug("{}: on_do_view_change: duplicate message", .{self.index});
            return;
        }

        // Record the first receipt of this do_view_change message:
        assert(self.do_view_change_from_all_replicas[message.replica] == null);
        self.do_view_change_from_all_replicas[message.replica] = message.*;

        // Count the number of unique messages received from all replicas:
        var count: usize = 0;
        assert(self.do_view_change_from_all_replicas.len == self.configuration.len);
        for (self.do_view_change_from_all_replicas) |received, replica| {
            if (received) |m| {
                assert(m.view == self.view);
                assert(m.replica == replica);
                assert(m.command == .do_view_change);
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
        var wv: u64 = 0;
        var wo: u64 = 0;
        var wc: u64 = 0;
        // TODO Remove latest_message in favor of latest_log.
        // We are still within the message handler and it's simply too easy to use `message`.
        // Even better, move this to another function where we pass a slice of non-null messages.
        var wm: Message = undefined;
        for (self.do_view_change_from_all_replicas) |received, replica| {
            if (received) |m| {
                if (m.latest_normal_view > wv or (m.latest_normal_view == wv and m.op > wo)) {
                    wv = m.latest_normal_view;
                    wo = m.op;
                    wc = m.commit;
                    wm = m;
                }
            }
        }

        log.debug("{}: winner is replica {} with op number {}", .{ self.index, wm.replica, wm.op });
    }

    fn on_start_view(self: *Replica, message: *const Message) void {}

    /// A replica i that notices the need for a view change advances its view,
    /// sets its status to view_change, and sends a ⟨start_view_change v, i⟩ message to all the
    /// other replicas, where v identifies the new view.
    /// A replica notices the need for a view change either based on its own timer, or because it
    /// receives a start_view_change or do_view_change message for a view with a larger number than
    /// its own view.
    fn start_view_change(self: *Replica, newer_view: u64) void {
        log.debug("{}: starting view change", .{self.index});
        assert(newer_view > self.view);
        self.view = newer_view;
        self.status = .view_change;

        // TODO Reset start_view_change quorum counter (see where we should do this).

        // TODO Reset view change timeout.
        // TODO Stop null commit timeout.
        // TODO Stop resend prepare timeout.

        // Send only to other replicas (not to self) to avoid any quorum off-by-one error:
        // This could happen if the replica sends/receives and counts its own message in the quorum.
        log.debug("{}: sending start_view_change to other replicas:", .{self.index});
        self.send_message_to_other_replicas(.{
            .replica = self.index,
            .view = newer_view,
            .command = .start_view_change,
        });
    }

    fn send_message_to_other_replicas(self: *Replica, message: Message) void {
        for (self.message) |_, replica| {
            if (replica == self.index) continue;
            self.send_message_to_replica(replica, message);
        }
    }

    fn send_message_to_replica(self: *Replica, replica: usize, message: Message) void {
        // TODO Add assertions to prevent some messages being sent to self.
        log.debug("{}: sending {} to replica {}: {}", .{
            self.index,
            @tagName(message.command),
            replica,
            message,
        });
        self.message[replica] = message;
    }

    fn tick(self: *Replica) void {
        self.ticks += 1;
    }

    // TODO Move this into a Configuration struct:
    fn leader(self: *Replica, view: u64) u32 {
        return @intCast(u32, @mod(view, self.configuration.len));
    }
};

pub fn main() void {
    std.debug.print("\n", .{});
    const f = 1;

    var replicas: [2 * f + 1]Replica = undefined;
    for (replicas) |*replica, index| {
        // TODO Assign nulls to message and do_view_change...:
        replica.* = .{
            .f = f,
            .configuration = .{ 0, 1, 2 },
            .index = @intCast(u32, index),
            .message = undefined,
            .do_view_change_from_all_replicas = undefined,
        };
        std.debug.print("{}\n", .{replica});
    }

    // Send for current view:
    replicas[0].on_message(&(Message{
        .replica = 1,
        .view = 0,
        .command = .start_view_change,
    }));

    // Send for newer view:
    replicas[0].on_message(&(Message{
        .replica = 1,
        .view = 7,
        .command = .start_view_change,
    }));

    // Send a duplicate message:
    replicas[0].on_message(&(Message{
        .replica = 1,
        .view = 7,
        .command = .start_view_change,
    }));

    // Increase quorum:
    replicas[0].on_message(&(Message{
        .replica = 2,
        .view = 7,
        .command = .start_view_change,
    }));

    // Send an older view:
    replicas[0].on_message(&(Message{
        .replica = 1,
        .view = 6,
        .command = .start_view_change,
    }));
}
