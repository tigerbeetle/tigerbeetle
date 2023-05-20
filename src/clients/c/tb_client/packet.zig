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

    /// Thread-safe stack, guarded by a Mutex,
    /// `push` and `pop` can be called concurrently from the client threads.
    pub const ConcurrentStack = struct {
        mutex: std.Thread.Mutex = .{},
        head: ?*Packet = null,

        pub fn push(self: *ConcurrentStack, packet: *Packet) void {
            self.mutex.lock();
            defer self.mutex.unlock();
            packet.next = self.head;
            self.head = packet;
        }

        pub fn pop(self: *ConcurrentStack) ?*Packet {
            self.mutex.lock();
            defer self.mutex.unlock();
            var head = self.head orelse return null;
            self.head = head.next;

            head.next = null;
            return head;
        }
    };
};
