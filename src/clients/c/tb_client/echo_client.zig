const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const constants = @import("../../../constants.zig");
const vsr = @import("../../../vsr.zig");
const Header = vsr.Header;

const RingBuffer = @import("../../../ring_buffer.zig").RingBuffer;
const MessagePool = @import("../../../message_pool.zig").MessagePool;
const Message = @import("../../../message_pool.zig").MessagePool.Message;

pub fn EchoClient(comptime StateMachine_: type, comptime MessageBus: type) type {
    return struct {
        const Self = @This();

        // Exposing the same types the real client does:
        pub usingnamespace blk: {
            const Client = @import("../../../vsr/client.zig").Client(StateMachine_, MessageBus);
            break :blk struct {
                pub const StateMachine = Client.StateMachine;
                pub const Error = Client.Error;
                pub const Request = Client.Request;
            };
        };

        request_queue: RingBuffer(Self.Request, constants.client_request_queue_max, .array) = .{},
        message_pool: *MessagePool,

        pub fn init(
            allocator: mem.Allocator,
            id: u128,
            cluster: u32,
            replica_count: u8,
            message_pool: *MessagePool,
            message_bus_options: MessageBus.Options,
        ) !Self {
            _ = allocator;
            _ = id;
            _ = cluster;
            _ = replica_count;
            _ = message_bus_options;

            return Self{
                .message_pool = message_pool,
            };
        }

        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            _ = allocator;
            // Drains all pending requests before deiniting.
            self.reply();
        }

        pub fn tick(self: *Self) void {
            self.reply();
        }

        pub fn request(
            self: *Self,
            user_data: u128,
            callback: Self.Request.Callback,
            operation: Self.StateMachine.Operation,
            message: *Message,
            message_body_size: usize,
        ) void {
            message.header.* = .{
                .client = 0,
                .request = 0,
                .cluster = 0,
                .command = .request,
                .operation = vsr.Operation.from(Self.StateMachine, operation),
                .size = @intCast(u32, @sizeOf(Header) + message_body_size),
            };

            if (self.request_queue.full()) {
                callback(user_data, operation, error.TooManyOutstandingRequests);
                return;
            }

            self.request_queue.push_assume_capacity(.{
                .user_data = user_data,
                .callback = callback,
                .message = message.ref(),
            });
        }

        pub fn get_message(self: *Self) *Message {
            return self.message_pool.get_message();
        }

        pub fn unref(self: *Self, message: *Message) void {
            self.message_pool.unref(message);
        }

        fn reply(self: *Self) void {
            while (self.request_queue.pop()) |inflight| {
                defer self.unref(inflight.message);

                inflight.callback(
                    inflight.user_data,
                    inflight.message.header.operation.cast(Self.StateMachine),
                    inflight.message.body(),
                );
            }
        }
    };
}
