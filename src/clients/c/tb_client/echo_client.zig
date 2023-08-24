const std = @import("std");
const stdx = @import("../../../stdx.zig");
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

        const RequestQueue = RingBuffer(Self.Request, .{
            .array = constants.client_request_queue_max,
        });

        messages_available: u32 = constants.client_request_queue_max,
        request_queue: RequestQueue = RequestQueue.init(),
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
                .size = @as(u32, @intCast(@sizeOf(Header) + message_body_size)),
            };

            assert(!self.request_queue.full());

            self.request_queue.push_assume_capacity(.{
                .user_data = user_data,
                .callback = callback,
                .message = message.ref(),
            });
        }

        pub fn get_message(self: *Self) *Message {
            assert(self.messages_available > 0);
            self.messages_available -= 1;
            return self.message_pool.get_message();
        }

        pub fn unref(self: *Self, message: *Message) void {
            if (message.references == 1) {
                self.messages_available += 1;
            }
            self.message_pool.unref(message);
        }

        fn reply(self: *Self) void {
            while (self.request_queue.pop()) |inflight| {
                const reply_message = self.message_pool.get_message();
                defer self.message_pool.unref(reply_message);

                stdx.copy_disjoint(.exact, u8, reply_message.buffer, inflight.message.buffer);
                // Similarly to the real client, release the request message before invoking the
                // callback. This necessitates a `copy_disjoint` above.
                self.unref(inflight.message);

                inflight.callback.?(
                    inflight.user_data,
                    reply_message.header.operation.cast(Self.StateMachine),
                    reply_message.body(),
                );
            }
        }
    };
}
