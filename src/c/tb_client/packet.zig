const std = @import("std");
const assert = std.debug.assert;
const Atomic = std.atomic.Atomic;

pub const Packet = extern struct {
    next: ?*Packet,
    user_data: usize,
    operation: u8,
    status: Status,
    data_size: u32,
    data: [*]const u8,

    pub const Status = enum(u8) {
        ok,
        too_much_data,
        invalid_operation,
        invalid_data_size,
    };

    pub const List = extern struct {
        head: ?*Packet = null,
        tail: ?*Packet = null,

        pub fn from(packet: *Packet) List {
            packet.next = null;
            return List{
                .head = packet,
                .tail = packet,
            };
        }

        pub fn push(self: *List, list: List) void {
            const prev = if (self.tail) |tail| &tail.next else &self.head;
            prev.* = list.head orelse return;
            self.tail = list.tail orelse unreachable;
        }

        pub fn peek(self: List) ?*Packet {
            return self.head orelse {
                assert(self.tail == null);
                return null;
            };
        }

        pub fn pop(self: *List) ?*Packet {
            const packet = self.head orelse return null;
            self.head = packet.next;
            if (self.head == null) self.tail = null;
            return packet;
        }
    };

    pub const Stack = struct {
        pushed: Atomic(?*Packet) = Atomic(?*Packet).init(null),
        popped: ?*Packet = null,

        pub fn push(self: *Stack, list: List) void {
            const head = list.head orelse return;
            const tail = list.tail orelse unreachable;

            var pushed = self.pushed.load(.Monotonic);
            while (true) {
                tail.next = pushed;
                pushed = self.pushed.tryCompareAndSwap(
                    pushed,
                    head,
                    .Release,
                    .Monotonic,
                ) orelse break;
            }
        }

        pub fn pop(self: *Stack) ?*Packet {
            if (self.popped == null) self.popped = self.pushed.swap(null, .Acquire);
            const packet = self.popped orelse return null;
            self.popped = packet.next;
            return packet;
        }
    };
};