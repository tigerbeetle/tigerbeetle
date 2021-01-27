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
    outbox: [3]?Message,

    /// The index into the configuration where this replica's IP address is stored:
    replica: u32,

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

    do_view_change_from_all_replicas: u64 = 0,

    fn enter_view(self: *Replica, view: u64) void {
        std.debug.print("replica {}: entering new view {}\n", .{ self.replica, view });
        assert(view >= self.view);
        self.view = view;
        self.status = .normal;

        // TODO Stop timeouts
        // TODO Start commit_timeout if leader
        // TODO Start view_change_timeout if follower
        // TODO Reset prepare_ok, start_view_change and do_view_change message counters.
    }

    pub fn on_message(self: *Replica, message: *const Message) void {
        log.debug("replica {}: on_message: {}", .{ self.replica, message });
        assert(message.replica != self.replica);
        assert(message.replica < self.configuration.len);
        switch (message.command) {
            .start_view_change => self.on_start_view_change(message),
            .do_view_change => self.on_do_view_change(message),
            .start_view => self.on_start_view(message),
            else => unreachable,
        }
    }

    fn on_start_view_change(self: *Replica, message: *const Message) void {
        assert(message.replica != self.replica);

        if (message.view < self.view) {
            log.debug("on_start_view_change: older view", .{});
            return;
        }

        if (message.view == self.view and self.status != .view_change) {
            switch (self.status) {
                .normal => log.debug("on_start_view_change: view already started", .{}),
                .recovering => log.debug("on_start_view_change: same view, but recovering", .{}),
                .view_change => unreachable,
            }
            return;
        }

        if (message.view > self.view) {
            log.debug("on_start_view_change: newer view, starting view change", .{});
            self.start_view_change(message.view);
            // Continue below...
        }

        assert(self.view == message.view);
        assert(self.status == .view_change);

        // Switch on a bit to track that a replica has sent us a start_view_change (idempotent):
        // A 64-bit integer can be shifted by at most 6 bits (2 ^ 6 = 64 bits) without overflow.
        const bit = @as(u64, 1) << @intCast(u6, message.replica);

        // Do not allow duplicate messages to trigger sending a redundant do_view_change message:
        // The only reason for this is to prevent multiple passes through the same state transition.
        // We want to be precise and exact and handle this state transition exactly once.
        if ((self.start_view_change_from_other_replicas & bit) != 0) {
            log.debug("on_start_view_change: duplicate message", .{});
            return;
        }

        // Record the first receipt of this start_view_change message from the other replica:
        assert(bit != self.replica);
        self.start_view_change_from_other_replicas |= bit;

        // Count the number of unique messages received from other replicas:
        const count = @popCount(u64, self.start_view_change_from_other_replicas);

        // Wait until we have `f` messages (in addition to ourself) for quorum:
        if (count < self.f) {
            log.debug("on_start_view_change: {} message(s), waiting for quorum", .{count});
            return;
        }

        // This is not the first time we have had quorum, the state transition has already happened:
        if (count > self.f) {
            log.debug("on_start_view_change: {} message(s), quorum already received", .{count});
            return;
        }

        assert(count == self.f);
        log.debug("on_start_view_change: {} message(s), quorum received", .{count});

        const new_leader = self.leader(self.view);
        if (new_leader == self.replica) {
            log.debug("on_start_view_change: not sending do_view_change to self as leader", .{});
            return;
        }

        // When replica i receives start_view_change messages for its view from f other
        // replicas, it sends a ⟨DOVIEWCHANGE v, l, v’, n, k, i⟩ message to the node that will
        // be the primary in the new view. Here v is its view, l is its log, v′ is the
        // view number of the latest view in which its status was normal, n is the op number,
        // and k is the commit number.
        log.debug("on_start_view_change: sending do_view_change to new leader {}", .{new_leader});
        // TODO Add log, last normal view, op number, and commit number.
        // These will be used by the new leader to decide on the longest log.
        self.outbox[new_leader] = Message{
            .replica = self.replica,
            .view = self.view,
            .command = .do_view_change,
        };
    }

    fn on_do_view_change(self: *Replica, message: *const Message) void {
        if (message.view > self.view) {
            self.start_view_change(message.view);
        } else if (message.view == self.view) {
            if (self.leader(self.view) == self.replica) {} else {
                // TODO Log warning that replica should not have sent do_view_change to us.
            }
        } else {
            unreachable;
        }
    }

    fn on_start_view(self: *Replica, message: *const Message) void {}

    /// A replica i that notices the need for a view change advances its view,
    /// sets its status to view_change, and sends a ⟨start_view_change v, i⟩ message to all the
    /// other replicas, where v identifies the new view.
    /// A replica notices the need for a view change either based on its own timer, or because it
    /// receives a start_view_change or do_view_change message for a view with a larger number than
    /// its own view.
    fn start_view_change(self: *Replica, newer_view: u64) void {
        log.debug("replica {}: starting change to newer view {}", .{ self.replica, newer_view });
        assert(newer_view > self.view);
        self.view = newer_view;
        self.status = .view_change;

        // TODO Reset view change timeout.
        // TODO Stop null commit timeout.
        // TODO Stop resend prepare timeout.

        // TODO Do we send to ourself?
        // TODO If we arrive here by timeout, do we increase quorum for ourself?
        log.debug("sending start_view_change to all other replicas", .{});
        for (self.outbox) |*message| {
            message.* = Message{
                .replica = self.replica,
                .view = newer_view,
                .command = .do_view_change,
            };
        }
    }

    fn tick(self: *Replica) void {
        self.ticks += 1;
        self.send();
    }

    fn send(self: *Replica) void {
        self.send_start_view_change_to_other_replicas();
        self.send_do_view_change_to_new_leader();
    }

    fn send_start_view_change_to_other_replicas(self: *Replica) void {
        if (self.status != .view_change) return;
        // TODO
        std.debug.print("{}: send start_view_change message to other replicas\n", .{self.replica});
        std.debug.print("{}: leader is going to be {}\n", .{ self.replica, self.leader(self.view) });
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
        replica.* = .{
            .f = f,
            .configuration = .{ 0, 1, 2 },
            .replica = @intCast(u32, index),
            .outbox = undefined,
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
