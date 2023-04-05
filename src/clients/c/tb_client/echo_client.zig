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
                pub const Callback = Client.Callback;
                pub const Error = Client.Error;
            };
        };

        pub const Batch = struct {
            message: *Message,

            pub fn slice(self: *const @This()) []align(@alignOf(vsr.Header)) u8 {
                return self.message.body();
            }
        };

        const Request = struct {
            message: *Message,
            user_data: u128,
            callback: Self.Callback,

            pub fn slice(self: *const @This()) []align(@alignOf(vsr.Header)) u8 {
                return self.message.body();
            }
        };

        request_queue: RingBuffer(Request, constants.client_request_queue_max, .array) = .{},
        message_pool: *MessagePool,

        pub fn init(
            allocator: mem.Allocator,
            id: u128,
            cluster: u32,
            replica_count: u8,
            batch_logical_max: u32,
            message_pool: *MessagePool,
            message_bus_options: MessageBus.Options,
        ) !Self {
            _ = allocator;
            _ = id;
            _ = cluster;
            _ = replica_count;
            _ = message_bus_options;
            _ = batch_logical_max;

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

        pub fn get_batch(
            self: *Self,
            operation: Self.StateMachine.Operation,
            size: u32,
        ) Self.Error!Batch {
            assert(@enumToInt(operation) >= constants.vsr_operations_reserved);

            // We must validate the message size before trying to acquire a batch/message.
            try Self.StateMachine.constants.operation_batch_body_valid(operation, size);

            // Checking if there is room for a new request.
            if (self.request_queue.full()) return error.BatchTooManyOutstanding;

            const message = self.message_pool.get_message();
            message.header.* = .{
                .client = 0,
                .request = 0,
                .cluster = 0,
                .command = .request,
                .operation = vsr.Operation.from(Self.StateMachine, operation),
                .size = @intCast(u32, @sizeOf(Header) + size),
            };

            return Batch{
                .message = message,
            };
        }

        pub fn submit_batch(
            self: *Self,
            user_data: u128,
            callback: Self.Callback,
            batch: Batch,
        ) void {
            self.request_queue.push_assume_capacity(.{
                .user_data = user_data,
                .callback = callback,
                .message = batch.message,
            });
        }

        fn reply(self: *Self) void {
            while (self.request_queue.pop()) |inflight| {
                defer self.message_pool.unref(inflight.message);

                inflight.callback(
                    inflight.user_data,
                    inflight.message.header.operation.cast(Self.StateMachine),
                    inflight.message.body(),
                );
            }
        }
    };
}
