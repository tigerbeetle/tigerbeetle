const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

const vr = @import("vr.zig");
const ConfigurationAddress = vr.ConfigurationAddress;
const Header = vr.Header;
const Replica = vr.Replica;

const log = std.log.scoped(.message_bus);

pub const Message = struct {
    header: *Header,
    buffer: []u8 align(vr.sector_size),
    references: usize = 0,
    next: ?*Message = null,
};

pub const MessageBus = struct {
    allocator: *Allocator,
    allocated: usize = 0,
    configuration: []ConfigurationAddress,

    /// A linked list of messages that are ready to send (FIFO):
    head: ?*Envelope = null,
    tail: ?*Envelope = null,

    const Address = union(enum) {
        replica: *Replica,
    };

    const Envelope = struct {
        address: Address,
        message: *Message,
        next: ?*Envelope,
    };

    pub fn init(allocator: *Allocator, configuration: []ConfigurationAddress) !MessageBus {
        var self = MessageBus{
            .allocator = allocator,
            .configuration = configuration,
        };
        return self;
    }

    pub fn deinit(self: *MessageBus) void {}

    /// TODO Detect if gc() called multiple times for message.references == 0.
    pub fn gc(self: *MessageBus, message: *Message) void {
        if (message.references == 0) {
            log.debug("message_bus: freeing {}", .{message.header});
            self.destroy_message(message);
        }
    }

    pub fn send_header_to_replica(self: *MessageBus, replica: u16, header: Header) void {
        assert(header.size == @sizeOf(Header));

        // TODO Pre-allocate messages at startup.
        var message = self.create_message(@sizeOf(Header)) catch unreachable;
        message.header.* = header;

        const data = message.buffer[@sizeOf(Header)..message.header.size];
        // The order matters here because checksum depends on checksum_data:
        message.header.set_checksum_data(data);
        message.header.set_checksum();

        assert(message.references == 0);
        self.send_message_to_replica(replica, message);
    }

    pub fn send_message_to_replica(self: *MessageBus, replica: u16, message: *Message) void {
        message.references += 1;

        // TODO Pre-allocate envelopes at startup.
        var envelope = self.allocator.create(Envelope) catch unreachable;
        envelope.* = .{
            .address = .{ .replica = self.configuration[replica].replica },
            .message = message,
            .next = null,
        };
        self.enqueue_message(envelope);
    }

    pub fn send_queued_messages(self: *MessageBus) void {
        while (self.head != null) {
            // Loop on a copy of the linked list, having reset the linked list first, so that any
            // synchronous append in on_message() is executed the next iteration of the outer loop.
            // This is not critical for safety here, but see run() in src/io.zig where it is.
            var head = self.head;
            self.head = null;
            self.tail = null;
            while (head) |envelope| {
                head = envelope.next;
                envelope.next = null;
                assert(envelope.message.references > 0);
                switch (envelope.address) {
                    .replica => |r| r.on_message(envelope.message),
                }
                envelope.message.references -= 1;
                self.gc(envelope.message);
                // The lifetime of an envelope will often be shorter than that of the message.
                self.allocator.destroy(envelope);
            }
        }
    }

    pub fn create_message(self: *MessageBus, size: u32) !*Message {
        assert(size >= @sizeOf(Header));

        var buffer = try self.allocator.allocAdvanced(u8, vr.sector_size, size, .exact);
        errdefer self.allocator.free(buffer);
        std.mem.set(u8, buffer, 0);

        var message = try self.allocator.create(Message);
        errdefer self.allocator.destroy(message);

        self.allocated += 1;

        message.* = .{
            .header = std.mem.bytesAsValue(Header, buffer[0..@sizeOf(Header)]),
            .buffer = buffer,
            .references = 0,
        };

        return message;
    }

    fn destroy_message(self: *MessageBus, message: *Message) void {
        assert(message.references == 0);
        self.allocator.free(message.buffer);
        self.allocator.destroy(message);
        self.allocated -= 1;
    }

    fn enqueue_message(self: *MessageBus, envelope: *Envelope) void {
        assert(envelope.message.references > 0);
        assert(envelope.next == null);
        if (self.head == null) {
            assert(self.tail == null);
            self.head = envelope;
            self.tail = envelope;
        } else {
            self.tail.?.next = envelope;
            self.tail = envelope;
        }
    }
};
