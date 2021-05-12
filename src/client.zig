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

    request_queue: RingBuffer(Request, config.message_bus_messages_max) = .{},

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
        };

        return self;
    }

    pub fn deinit(self: *Client) void {}

    pub fn tick(self: *Client) void {
        self.message_bus.tick();

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
        if (was_empty) self.send_message_to_replicas(message);
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
        if (message.header.command != .reply) {
            log.warn("{}: on_message: unexpected command {}", .{ self.id, message.header.command });
            return;
        }
        if (message.header.request < self.request_number) {
            log.debug("{}: on_message: duplicate reply {}", .{ self.id, message.header.request });
            return;
        }

        const completed = self.request_queue.pop().?;
        completed.callback(completed.user_data, completed.operation, message.body());
        self.message_bus.unref(completed.message);

        if (self.request_queue.peek()) |next_request| {
            self.send_message_to_replicas(next_request.message);
        }
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
};
