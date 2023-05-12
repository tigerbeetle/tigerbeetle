const std = @import("std");
const assert = std.debug.assert;
const Atomic = std.atomic.Atomic;

pub const Packet = extern struct {
    next: ?*Packet,
    user_data: ?*anyopaque,
    operation: u8,
    status: Status,
    data_size: u32,
    data: ?*anyopaque,

    pub const Status = enum(u8) {
        ok,
        too_much_data,
        invalid_operation,
        invalid_data_size,
    };

    /// Non thread-safe linked list.
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

    /// Thread-safe stack optimized for 1 consumer (io thread) and N producers (client threads),
    /// `push` uses a spin lock, and `pop` is not thread-safe.
    pub const SubmissionStack = struct {
        pushed: Atomic(?*Packet) = Atomic(?*Packet).init(null),
        popped: ?*Packet = null,

        pub fn push(self: *SubmissionStack, packet: *Packet) void {
            var pushed = self.pushed.load(.Monotonic);
            while (true) {
                packet.next = pushed;
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
            self.popped = packet.next;
            return packet;
        }
    };

    /// Thread-safe stack, `push` and `pop` can be called concurrently from the client threads.
    pub const ConcurrentStack = struct {
        head: Atomic(?*Packet) = Atomic(?*Packet).init(null),
        count: Atomic(u32) = Atomic(u32).init(0),

        pub inline fn get_count(self: *ConcurrentStack) u32 {
            return self.count.load(.Monotonic);
        } 

        pub fn push(self: *ConcurrentStack, packet: *Packet) void {
            var head = self.head.load(.Monotonic);
            while (true) {
                packet.next = head;
                head = self.head.tryCompareAndSwap(
                    head,
                    packet,
                    .Release,
                    .Monotonic,
                ) orelse break;
            }

            _ = self.count.fetchAdd(1, .Monotonic);
        }

        pub fn pop(self: *ConcurrentStack) ?*Packet {
            var head = self.head.load(.Monotonic);
            while (true) {
                var next = (head orelse return null).next;
                head = self.head.tryCompareAndSwap(
                    head,
                    next,
                    .Release,
                    .Monotonic,
                ) orelse {
                    head.?.next = null;
                    _ = self.count.fetchSub(1, .Monotonic);
                    return head;
                };
            }
        }
    };
};
