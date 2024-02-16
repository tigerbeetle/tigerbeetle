const std = @import("std");
const stdx = @import("../../../stdx.zig");
const assert = std.debug.assert;
const mem = std.mem;

const constants = @import("../../../constants.zig");
const vsr = @import("../../../vsr.zig");
const Header = vsr.Header;

const FIFO = @import("../../../fifo.zig").FIFO;
const IOPS = @import("../../../iops.zig").IOPS;
const RingBuffer = @import("../../../ring_buffer.zig").RingBuffer;
const MessagePool = @import("../../../message_pool.zig").MessagePool;
const Message = @import("../../../message_pool.zig").MessagePool.Message;

pub fn EchoClient(comptime StateMachine_: type, comptime MessageBus: type) type {
    return struct {
        const Self = @This();

        // Exposing the same types the real client does:
        const Client = @import("../../../vsr/client.zig").Client(StateMachine_, MessageBus);
        pub const StateMachine = Client.StateMachine;
        pub const Request = Client.Request;

        id: u128,
        cluster: u128,
        request_queue: FIFO(Request) = .{ .name = null },
        request_echo_body: *[constants.message_body_size_max]u8,
        messages_available: u32 = constants.client_request_queue_max,
        message_pool: *MessagePool,

        pub fn init(
            allocator: mem.Allocator,
            id: u128,
            cluster: u128,
            replica_count: u8,
            message_pool: *MessagePool,
            message_bus_options: MessageBus.Options,
        ) !Self {
            _ = replica_count;
            _ = message_bus_options;

            return Self{
                .id = id,
                .cluster = cluster,
                .request_echo_body = try allocator.create([constants.message_body_size_max]u8),
                .message_pool = message_pool,
            };
        }

        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            self.reply(); // Drains all pending requests before deiniting.
            allocator.destroy(self.request_echo_body);
        }

        pub fn tick(self: *Self) void {
            self.reply();
        }

        pub fn submit(
            self: *Self,
            callback: Request.Callback,
            request: *Request,
            operation: StateMachine.Operation,
            event_data: []const u8,
        ) void {
            const event_size: usize = switch (operation) {
                inline else => |operation_comptime| @sizeOf(StateMachine.Event(operation_comptime)),
            };
            assert(event_data.len <= constants.message_body_size_max);
            assert(@divExact(event_data.len, event_size) > 0);

            request.* = .{
                .next = null,
                .operation = vsr.Operation.from(StateMachine, operation),
                .callback = callback,
                .body_ptr = event_data.ptr,
                .body_len = @intCast(event_data.len),
                .queue_total = @intCast(event_data.len),
                .queue = .{ .name = null },
            };

            self.request_queue.push(request);
        }

        pub fn get_message(self: *Self) *Message {
            assert(self.messages_available > 0);
            self.messages_available -= 1;
            return self.message_pool.get_message(null);
        }

        pub fn release(self: *Self, message: *Message) void {
            assert(self.messages_available < constants.client_request_queue_max);
            self.messages_available += 1;
            self.message_pool.unref(message);
        }

        fn reply(self: *Self) void {
            while (self.request_queue.pop()) |request| {
                const echo_body = self.request_echo_body[0..request.body_len];
                stdx.copy_disjoint(.exact, u8, echo_body, request.body[0..request.body_len]);
                request.callback(request, echo_body);
            }
        }
    };
}
