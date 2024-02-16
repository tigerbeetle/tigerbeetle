const std = @import("std");
const assert = std.debug.assert;
const Atomic = std.atomic.Atomic;
const vsr = @import("../../../vsr.zig");

pub const Packet = extern struct {
    status: Status,
    context: ?*anyopaque,
    user_data: ?*anyopaque,
    request: vsr.ClientRequest,

    fn next_ptr(packet: *Packet) *?*Packet {
        return @ptrCast(&packet.request.next);
    }

    pub const Status = enum(u8) {
        ok,
        too_much_data,
        invalid_operation,
        invalid_data_size,
    };

    /// Thread-safe stack optimized for 1 consumer (io thread) and N producers (client threads),
    /// `push` uses a spin lock, and `pop` is not thread-safe.
    pub const SubmissionStack = struct {
        pushed: Atomic(?*Packet) = Atomic(?*Packet).init(null),
        popped: ?*Packet = null,

        pub fn push(self: *SubmissionStack, packet: *Packet) void {
            var pushed = self.pushed.load(.Monotonic);
            while (true) {
                packet.next_ptr().* = pushed;
                pushed = self.pushed.tryCompareAndSwap(
                    pushed,
                    packet,
                    .Release,
                    .Monotonic,
                ) orelse break;
            }
        }

        pub fn pop(self: *SubmissionStack) ?*Packet {
            if (self.popped == null) self.popped = self.pushed.swap(null, .Acquire);
            const packet = self.popped orelse return null;
            self.popped = packet.next_ptr().*;
            return packet;
        }
    };
};
