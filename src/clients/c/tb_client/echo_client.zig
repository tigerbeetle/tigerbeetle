const std = @import("std");
const stdx = @import("../../../stdx.zig");
const assert = std.debug.assert;
const mem = std.mem;

const constants = @import("../../../constants.zig");
const vsr = @import("../../../vsr.zig");
const Header = vsr.Header;

const IOPS = @import("../../../iops.zig").IOPS;
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
                pub const BatchError = Client.BatchError;
            };
        };

        const EchoRequest = struct {
            message: *Message.Request,
            user_data: u128,
            callback: Self.Request.Callback,
        };

        const RequestQueue = RingBuffer(EchoRequest, .{
            .array = constants.client_request_queue_max,
        });

        id: u128,
        cluster: u128,
        messages_available: u32 = constants.client_request_queue_max,
        request_queue: RequestQueue = RequestQueue.init(),
        message_pool: *MessagePool,

        pub fn init(
            allocator: mem.Allocator,
            id: u128,
            cluster: u128,
            replica_count: u8,
            message_pool: *MessagePool,
            message_bus_options: MessageBus.Options,
        ) !Self {
            _ = allocator;
            _ = replica_count;
            _ = message_bus_options;

            return Self{
                .id = id,
                .cluster = cluster,
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

        pub fn batch_get(
            self: *Self,
            operation: Self.StateMachine.Operation,
            event_count: usize,
        ) Self.BatchError!Batch {
            const body_size = switch (operation) {
                inline else => |batch_operation| blk: {
                    break :blk @sizeOf(Self.StateMachine.Event(batch_operation)) * event_count;
                },
            };
            assert(body_size <= constants.message_body_size_max);

            if (self.messages_available == 0) return error.TooManyOutstanding;
            const message = self.get_message();
            errdefer self.release(message);

            const message_request = message.build(.request);
            message_request.header.* = .{
                .client = self.id,
                .request = 0,
                .cluster = self.cluster,
                .command = .request,
                .operation = vsr.Operation.from(Self.StateMachine, operation),
                .size = @intCast(@sizeOf(Header) + body_size),
            };

            return Batch{ .message = message_request };
        }

        pub const Batch = struct {
            message: *Message.Request,

            pub fn slice(batch: Batch) []u8 {
                return batch.message.body();
            }
        };

        pub fn batch_submit(
            self: *Self,
            user_data: u128,
            callback: Self.Request.Callback,
            batch: Batch,
        ) void {
            assert(!self.request_queue.full());
            self.request_queue.push_assume_capacity(.{
                .message = batch.message,
                .user_data = user_data,
                .callback = callback,
            });
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
                const reply_message = self.message_pool.get_message(.request);
                defer self.message_pool.unref(reply_message.base());

                const operation = request.message.header.operation.cast(Self.StateMachine);
                stdx.copy_disjoint(
                    .exact,
                    u8,
                    reply_message.buffer,
                    request.message.buffer,
                );

                // Similarly to the real client, release the request message before invoking the
                // callback. This necessitates a `copy_disjoint` above.
                self.release(request.message.base());

                request.callback(
                    request.user_data,
                    operation,
                    reply_message.body(),
                );
            }
        }
    };
}
