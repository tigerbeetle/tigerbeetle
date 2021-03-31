const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const vr = @import("vr.zig");
const Header = vr.Header;
const Replica = vr.Replica;
const RingBuffer = @import("ring_buffer.zig").RingBuffer;

const log = std.log.scoped(.message_bus);

const queue_size = 3;

pub const MessageBus = struct {
    pub const Address = *Replica;

    pub const Message = struct {
        header: *Header,
        buffer: []u8 align(vr.sector_size),
        references: usize = 0,
        next: ?*Message = null,
    };

    const SendQueue = RingBuffer(*Message, queue_size);

    allocator: *mem.Allocator,
    allocated: usize = 0,

    configuration: []Address,
    send_queues: []RingBuffer(*Message, queue_size),

    pub fn init(allocator: *mem.Allocator, configuration: []Address) !MessageBus {
        const send_queues = try allocator.alloc(SendQueue, configuration.len);
        errdefer allocator.free(send_queues);
        mem.set(SendQueue, send_queues, .{});

        return MessageBus{
            .allocator = allocator,
            .configuration = configuration,
            .send_queues = send_queues,
        };
    }

    pub fn tick(self: *MessageBus) void {
        // Do nothing

    }

    pub fn deinit(self: *MessageBus) void {
        self.allocator.free(self.send_queues);
    }

    /// Increment the reference count of the message and return the same pointer passed.
    pub fn ref(self: *MessageBus, message: *Message) *Message {
        message.references += 1;
        return message;
    }

    /// Decrement the reference count of the message, possibly freeing it.
    pub fn unref(self: *MessageBus, message: *Message) void {
        message.references -= 1;
        if (message.references == 0) {
            log.debug("freeing {}", .{message.header});
            self.allocator.free(message.buffer);
            self.allocator.destroy(message);
            self.allocated -= 1;
        }
    }

    /// Returns true if the target replica is connected and has space in its send queue.
    pub fn can_send_to_replica(self: *MessageBus, replica: u16) bool {
        return !self.send_queues[replica].full();
    }

    pub fn send_header_to_replica(self: *MessageBus, replica: u16, header: Header) void {
        assert(header.size == @sizeOf(Header));

        // TODO Pre-allocate messages at startup.
        var message = self.create_message(@sizeOf(Header)) catch unreachable;
        message.header.* = header;

        const body = message.buffer[@sizeOf(Header)..message.header.size];
        // The order matters here because checksum depends on checksum_body:
        message.header.set_checksum_body(body);
        message.header.set_checksum();

        assert(message.references == 0);
        self.send_message_to_replica(replica, message);
    }

    pub fn send_message_to_replica(self: *MessageBus, replica: u16, message: *Message) void {
        self.send_queues[replica].push(self.ref(message)) catch |err| switch (err) {
            error.NoSpaceLeft => {
                self.unref(message);
                log.notice("message queue for replica {} full, dropping message", .{replica});
            },
        };
    }

    pub fn send_header_to_client(self: *MessageBus, client_id: u128, header: Header) void {
        assert(header.size == @sizeOf(Header));

        // TODO Pre-allocate messages at startup.
        var message = self.create_message(@sizeOf(Header)) catch unreachable;
        message.header.* = header;

        const body = message.buffer[@sizeOf(Header)..message.header.size];
        // The order matters here because checksum depends on checksum_body:
        message.header.set_checksum_body(body);
        message.header.set_checksum();

        assert(message.references == 0);
        self.send_message_to_client(client_id, message);
    }

    /// Try to send the message to the client with the given id.
    /// If the client is not currently connected, the message is silently dropped.
    pub fn send_message_to_client(self: *MessageBus, client_id: u128, message: *Message) void {
        // Do nothing
    }

    /// Deliver messages to all replicas. Always iterate on a copy to
    /// avoid potential infinite loops.
    pub fn flush(self: *MessageBus) void {
        for (self.send_queues) |*queue, replica| {
            var copy = queue.*;
            queue.* = .{};
            while (copy.pop()) |message| {
                self.configuration[replica].on_message(message);
                self.unref(message);
            }
        }
    }

    pub fn create_message(self: *MessageBus, size: u32) !*Message {
        assert(size >= @sizeOf(Header));

        var buffer = try self.allocator.allocAdvanced(u8, vr.sector_size, size, .exact);
        errdefer self.allocator.free(buffer);
        mem.set(u8, buffer, 0);

        var message = try self.allocator.create(Message);
        errdefer self.allocator.destroy(message);

        self.allocated += 1;

        message.* = .{
            .header = mem.bytesAsValue(Header, buffer[0..@sizeOf(Header)]),
            .buffer = buffer,
        };

        return message;
    }
};

test "" {
    std.testing.refAllDecls(MessageBus);
}
