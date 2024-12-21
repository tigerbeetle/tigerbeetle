const std = @import("std");
const assert = std.debug.assert;
const Atomic = std.atomic.Value;

// When referenced from unit_test.zig, there is no vsr import module so use path.
const vsr = if (@import("root") == @This()) @import("vsr") else @import("../../../vsr.zig");
const stdx = vsr.stdx;

pub const Packet = extern struct {
    next: ?*Packet,
    user_data: ?*anyopaque,
    data: ?*anyopaque,
    data_size: u32,
    operation: u8,
    status: Status,
    reserved: [2]u8 = [_]u8{0} ** 2,

    comptime {
        assert(@sizeOf(Packet) == 32);
        assert(@alignOf(Packet) == 8);
    }

    pub const Status = enum(u8) {
        ok,
        too_much_data,
        client_evicted,
        client_release_too_low,
        client_release_too_high,
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

    pub fn events(packet: *const Packet) []const u8 {
        if (packet.data_size == 0) {
            // It may be an empty array (null pointer)
            // or a buffer with no elements (valid pointer and size == 0).
            stdx.maybe(packet.data == null);
            return &[0]u8{};
        }

        const data: [*]const u8 = @ptrCast(packet.data.?);
        return data[0..packet.data_size];
    }
};
