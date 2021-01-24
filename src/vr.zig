const std = @import("std");
const assert = std.debug.assert;

const Command = enum {
    start_view_change,
    do_view_change,
    start_view,
};

const Message = struct {
    sender_replica_number: u32,

    // Every message sent from one replica to another contains the sender's current view_number.
    sender_view_number: u64,

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

    /// The index into the configuration where this replica's IP address is stored:
    replica_number: u32,

    /// The current view_number, initially 0:
    view_number: u64 = 0,

    /// The current status, either normal, view_change, or recovering:
    status: Status = .normal,

    /// The op_number assigned to the most recently received request, initially 0:
    op_number: u64 = 0,

    /// The op_number of the most recently committed operation:
    commit_number: ?u64 = null,

    /// A bitmap of start_view_change messages from other replicas (for current view_number):
    /// We use a bitmap instead of a simple counter to tolerate message duplicates.
    start_view_change_from_other_replicas: u64 = 0,

    do_view_change_from_all_replicas: u64 = 0,

    pub fn on_message(self: *Replica, message: *Message) void {
        std.debug.print("replica {}: on_message: {}\n", .{ self.replica_number, message });
        assert(message.sender_replica_number != self.replica_number);
        assert(message.sender_replica_number < self.configuration.len);
        if (message.sender_view_number < self.view_number) return;
        switch (message.command) {
            .start_view_change => self.on_message_start_view_change(message),
            .do_view_change => self.on_message_do_view_change(message),
            .start_view => self.on_message_start_view(message),
        }
    }

    fn on_message_start_view_change(self: *Replica, message: *Message) void {
        if (message.sender_view_number > self.view_number) {
            self.start_view_change(message.sender_view_number);
        } else if (message.sender_view_number == self.view_number) {
            // TODO Are we in the right state to process this?

            // Switch a bit on idempotently to track that a replica has sent us a start_view_change:
            assert(message.sender_replica_number != self.replica_number);
            const bit = @as(u64, 1) << @intCast(u6, message.sender_replica_number);
            self.start_view_change_from_other_replicas |= bit;
        } else {
            unreachable;
        }
    }

    fn on_message_do_view_change(self: *Replica, message: *Message) void {
        if (message.sender_view_number > self.view_number) {
            self.start_view_change(message.sender_view_number);
        } else if (message.sender_view_number == self.view_number) {
            if (self.primary() == self.replica_number) {

            } else {
                // TODO Log warning that replica should not have sent do_view_change to us.
            }
        } else {
            unreachable;
        }
    }

    fn on_message_start_view(self: *Replica, message: *Message) void {
        
    }

    /// A replica i that notices the need for a view change advances its view_number,
    /// sets its status to view_change, and sends a ⟨start_view_change v, i⟩ message to all the
    /// other replicas, where v identifies the new view.
    /// A replica notices the need for a view change either based on its own timer, or because it
    /// receives a start_view_change or do_view_change message for a view with a larger number than
    /// its own view_number.
    fn start_view_change(self: *Replica, view_number: u64) void {
        assert(view_number > self.view_number);
        self.view_number = view_number;
        self.status = .view_change;
        self.start_view_change_from_other_replicas = 0;
    }

    fn tick(self: *Replica) void {
        self.ticks += 1;
        self.send();
    }

    fn send(self: *Replica) void {
        self.send_start_view_change_to_other_replicas();
        self.send_do_view_change_to_new_primary();
    }

    fn send_start_view_change_to_other_replicas(self: *Replica) void {
        if (self.status != .view_change) return;
        // TODO
        std.debug.print("{}: send start_view_change message to other replicas\n", .{ self.replica_number });
        std.debug.print("{}: primary is going to be {}\n", .{ self.replica_number, self.primary() });
    }

    // When replica i receives start_view_change messages for its view_number from f other
    // replicas, it sends a ⟨DOVIEWCHANGE v, l, v’, n, k, i⟩ message to the node that will
    // be the primary in the new view. Here v is its view_number, l is its log, v′ is the
    // view number of the latest view in which its status was normal, n is the op-number,
    // and k is the commit_number.
    fn send_do_view_change_to_new_primary(self: *Replica) void {
        if (self.status != .view_change) return;
        const bit_self = @as(u64, 1) << @intCast(u6, self.replica_number);
        assert((self.start_view_change_from_other_replicas & bit_self) == 0);
        // Count the number of bits set, which represents the idempotent message count:
        const bit_count = @popCount(u64, self.start_view_change_from_other_replicas);
        if (bit_count < self.f) return;
        // TODO
        std.debug.print("{}: send do_view_change to new primary {}\n", .{ self.replica_number, self.primary() });
    }

    fn primary(self: *Replica) u32 {
        return @intCast(u32, @mod(self.view_number, self.configuration.len));
    }
};

test "init" {
    std.debug.print("\n", .{});
    const f = 1;

    var replicas: [2 * f + 1]Replica = undefined;
    for (replicas) |*replica, index| {
        replica.* = .{
            .f = f,
            .configuration = .{ 0, 1, 2 },
            .replica_number = @intCast(u32, index),
        };
        std.debug.print("{}\n", .{ replica });
    }

    var message = Message{
        .sender_replica_number = 1,
        .sender_view_number = 7,
        .command = .start_view_change,
    };
    replicas[0].on_message(&message);
    replicas[0].send();
}
