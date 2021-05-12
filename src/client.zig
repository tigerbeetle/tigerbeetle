const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const config = @import("config.zig");
const vr = @import("vr.zig");
const Header = vr.Header;

const MessageBus = @import("message_bus.zig").MessageBusClient;
const Message = @import("message_bus.zig").Message;
const Operation = @import("state_machine.zig").Operation;
const RingBuffer = @import("ring_buffer.zig").RingBuffer;

const tb = @import("tigerbeetle.zig");
const Account = tb.Account;
const Transfer = tb.Transfer;
const Commit = tb.Commit;
const CreateAccountResults = tb.CreateAccountResults;
const CreateTransferResults = tb.CreateTransferResults;
const CommitTransferResults = tb.CommitTransferResults;

const log = std.log;

pub const Client = struct {
    const Request = struct {
        const Callback = fn (user_data: u128, operation: Operation, results: []const u8) void;
        user_data: u128,
        callback: Callback,
        operation: Operation,
        message: *Message,
    };

    allocator: *mem.Allocator,
    id: u128,
    cluster: u128,
    replica_count: u16,
    message_bus: *MessageBus,

    // TODO Track the latest view number received in .pong and .reply messages.
    // TODO Ask the cluster for our last request number.
    request_number: u32 = 0,

    /// Leave one Message free to receive with
    request_queue: RingBuffer(Request, config.message_bus_messages_max - 1) = .{},
    request_timeout: vr.Timeout,

    ping_timeout: vr.Timeout,

    pub fn init(
        allocator: *mem.Allocator,
        id: u128,
        cluster: u128,
        replica_count: u16,
        message_bus: *MessageBus,
    ) !Client {
        assert(id > 0);
        assert(cluster > 0);

        var self = Client{
            .allocator = allocator,
            .id = id,
            .cluster = cluster,
            .replica_count = replica_count,
            .message_bus = message_bus,
            .request_timeout = .{
                .name = "request_timeout",
                .replica = std.math.maxInt(u16),
                .after = 10,
            },
            .ping_timeout = .{
                .name = "ping_timeout",
                .replica = std.math.maxInt(u16),
                .after = 10,
            },
        };

        self.ping_timeout.start();

        return self;
    }

    pub fn deinit(self: *Client) void {}

    pub fn tick(self: *Client) void {
        self.message_bus.tick();

        self.request_timeout.tick();
        if (self.request_timeout.fired()) self.on_request_timeout();

        self.ping_timeout.tick();
        if (self.ping_timeout.fired()) self.on_ping_timeout();

        // TODO Resend the request to the leader when the request_timeout fires.
        // This covers for dropped packets, when the leader is still the leader.

        // TODO Resend the request to the next replica and so on each time the reply_timeout fires.
        // This anticipates the next view change, without the cost of broadcast against the cluster.

        // TODO Tick ping_timeout and send ping if necessary to all replicas.
        // We need to keep doing this until we discover our latest request_number.
        // Thereafter, we can extend our ping_timeout considerably.
        // The cluster can use this ping information to do LRU eviction from the client table when
        // it is overflowed by the number of unique client IDs.

        // TODO Resend the request to the leader when the request_timeout fires.
        // This covers for dropped packets, when the leader is still the leader.

        // TODO Resend the request to the next replica and so on each time the reply_timeout fires.
        // This anticipates the next view change, without the cost of broadcast against the cluster.
    }

    /// A client is allowed at most one inflight request at a time, concurrent requests are queued.
    pub fn request(
        self: *Client,
        user_data: u128,
        callback: Request.Callback,
        operation: Operation,
        data: []const u8,
    ) void {
        const message = self.message_bus.get_message() orelse
            @panic("TODO: bubble up an error/drop the request");
        self.init_message(message, operation, data);

        const was_empty = self.request_queue.empty();
        self.request_queue.push(.{
            .user_data = user_data,
            .callback = callback,
            .operation = operation,
            .message = message.ref(),
        }) catch {
            @panic("TODO: bubble up an error/drop the request");
        };

        // If the queue was empty, there is no currently inflight message, so send this one.
        if (was_empty) self.send_request(message);
    }

    fn on_request_timeout(self: *Client) void {
        const current_request = self.request_queue.peek() orelse return;
        self.send_request(current_request.message);
    }

    fn send_request(self: *Client, request_message: *Message) void {
        self.send_message_to_replicas(request_message);
        self.request_timeout.start();
    }

    fn on_reply(self: *Client, reply: *Message) void {
        const done = self.request_queue.pop().?;
        done.callback(done.user_data, done.operation, reply.body());
        self.message_bus.unref(done.message);
        self.request_timeout.stop();

        if (self.request_queue.peek()) |next_request| {
            self.send_request(next_request.message);
        }
    }

    pub fn on_message(self: *Client, message: *Message) void {
        log.debug("{}: on_message: {}", .{ self.id, message.header });
        if (message.header.invalid()) |reason| {
            log.debug("{}: on_message: invalid ({s})", .{ self.id, reason });
            return;
        }
        if (message.header.cluster != self.cluster) {
            log.warn("{}: on_message: wrong cluster (message.header.cluster={} instead of {})", .{
                self.id,
                message.header.cluster,
                self.cluster,
            });
            return;
        }
        switch (message.header.command) {
            .reply => {
                if (message.header.request < self.request_number) {
                    log.debug("{}: on_message: duplicate reply {}", .{ self.id, message.header.request });
                    return;
                }

                self.on_reply(message);
            },
            .ping => self.on_ping(message),
            .pong => {
                // TODO: when we implement proper request number usage, we will
                // need to get the request number from a pong message on startup.
            },
            else => {
                log.warn("{}: on_message: unexpected command {}", .{ self.id, message.header.command });
            },
        }
    }

    fn on_ping_timeout(self: *Client) void {
        self.ping_timeout.reset();

        const ping = Header{
            .command = .ping,
            .cluster = self.cluster,
            .client = self.id,
        };

        self.send_header_to_replicas(ping);
    }

    fn on_ping(self: Client, ping: *const Message) void {
        const pong: Header = .{
            .command = .pong,
            .cluster = self.cluster,
            .client = self.id,
        };
        self.message_bus.send_header_to_replica(ping.header.replica, pong);
    }

    /// Initialize header fields and set the checksums
    fn init_message(self: Client, message: *Message, operation: Operation, data: []const u8) void {
        message.header.* = .{
            .client = self.id,
            .cluster = self.cluster,
            .request = 1, // TODO: use request numbers properly
            .command = .request,
            .operation = operation,
            .size = @intCast(u32, @sizeOf(Header) + data.len),
        };
        const body = message.buffer[@sizeOf(Header)..][0..data.len];
        std.mem.copy(u8, body, data);
        message.header.set_checksum_body(body);
        message.header.set_checksum();
    }

    fn send_message_to_leader(self: *Client, message: *Message) void {
        // TODO For this to work, we need to send pings to the cluster every N ticks.
        // Otherwise, the latest leader will have our connection.peer set to .unknown.

        // TODO Use the latest view number modulo the configuration length to find the leader.
        // For now, replica 0 will forward onto the latest leader.
        self.message_bus.send_message_to_replica(0, message);
    }

    fn send_message_to_replicas(self: *Client, message: *Message) void {
        var replica: u16 = 0;
        while (replica < self.replica_count) : (replica += 1) {
            self.message_bus.send_message_to_replica(replica, message);
        }
    }

    fn send_header_to_replicas(self: *Client, header: Header) void {
        var replica: u16 = 0;
        while (replica < self.replica_count) : (replica += 1) {
            self.message_bus.send_header_to_replica(replica, header);
        }
    }
};
