const std = @import("std");
const assert = std.debug.assert;
const Atomic = std.atomic.Value;

pub const Packet = extern struct {
    next: ?*Packet,
    user_data: ?*anyopaque,
    operation: u8,
    status: Status,
    data_size: u32,
    data: ?*anyopaque,
    batch_next: ?*Packet,
    batch_tail: ?*Packet,
    batch_count_packets: u16,
    batch_count_events: u16,
    batch_count_results: u16,
    reserved: [6]u8,

    comptime {
        assert(@sizeOf(Packet) == 64);
        assert(@alignOf(Packet) == 8);
    }

    pub const Status = enum(u8) {
        ok,
        too_much_data,
        client_shutdown,
        invalid_operation,
        invalid_data_size,
    };

    /// Thread-safe stack optimized for 1 consumer (io thread) and N producers (client threads),
    /// `push` uses a spin lock, and `pop` is not thread-safe.
    pub const SubmissionStack = struct {
        pushed: Atomic(?*Packet) = Atomic(?*Packet).init(null),
        popped: ?*Packet = null,

        pub fn push(self: *SubmissionStack, packet: *Packet) void {
            var pushed = self.pushed.load(.monotonic);
            while (true) {
                packet.next = pushed;
                pushed = self.pushed.cmpxchgWeak(
                    pushed,
                    packet,
                    .release,
                    .monotonic,
                ) orelse break;
            }
        }

        pub fn pop(self: *SubmissionStack) ?*Packet {
            if (self.popped == null) self.popped = self.pushed.swap(null, .acquire);
            const packet = self.popped orelse return null;
            self.popped = packet.next;
            packet.next = null;
            return packet;
        }
    };
};
