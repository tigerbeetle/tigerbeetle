const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const vsr = @import("../tb_client.zig").vsr;
const Header = vsr.Header;
const stdx = vsr.stdx;
const constants = vsr.constants;
const MessagePool = vsr.message_pool.MessagePool;
const Message = MessagePool.Message;
const Time = vsr.time.Time;

pub fn EchoClientType(comptime MessageBus: type) type {
    return struct {
        const EchoClient = @This();

        // Exposing the same types the real client does:
        const VSRClient = vsr.ClientType(EchoOperation, MessageBus);
        pub const Operation = VSRClient.Operation;
        pub const Request = VSRClient.Request;

        id: u128,
        cluster: u128,
        release: vsr.Release = vsr.Release.minimum,
        request_number: u32 = 0,
        reply_timestamp: u64 = 0, // Fake timestamp, just a counter.
        request_inflight: ?Request = null,
        message_pool: *MessagePool,
        time: Time,

        pub fn init(
            allocator: mem.Allocator,
            time: Time,
            message_pool: *MessagePool,
            options: struct {
                id: u128,
                cluster: u128,
                replica_count: u8,
                message_bus_options: MessageBus.Options,
                eviction_callback: ?*const fn (
                    client: *EchoClient,
                    eviction: *const Message.Eviction,
                ) void = null,
            },
        ) !EchoClient {
            _ = allocator;
            _ = options.replica_count;
            _ = options.message_bus_options;

            return EchoClient{
                .id = options.id,
                .cluster = options.cluster,
                .message_pool = message_pool,
                .time = time,
            };
        }

        pub fn deinit(self: *EchoClient, allocator: std.mem.Allocator) void {
            _ = allocator;
            if (self.request_inflight) |inflight| self.release_message(inflight.message.base());
        }

        pub fn tick(self: *EchoClient) void {
            const inflight = self.request_inflight orelse return;
            self.request_inflight = null;

            self.reply_timestamp += 1;
            const timestamp = self.reply_timestamp;

            // Allocate a reply message.
            const reply = self.get_message().build(.request);
            defer self.release_message(reply.base());

            // Copy the request message's entire content including header into the reply.
            const operation = inflight.message.header.operation;
            stdx.copy_disjoint(
                .exact,
                u8,
                reply.buffer,
                inflight.message.buffer,
            );

            // Similarly to the real client, release the request message before invoking the
            // callback. This necessitates a `copy_disjoint` above.
            self.release_message(inflight.message.base());

            switch (inflight.callback) {
                .request => |callback| {
                    callback(inflight.user_data, operation, timestamp, reply.body_used());
                },
                .register => |callback| {
                    const result = vsr.RegisterResult{
                        .batch_size_limit = constants.message_body_size_max,
                    };
                    callback(inflight.user_data, &result);
                },
            }
        }

        pub fn register(
            self: *EchoClient,
            callback: Request.RegisterCallback,
            user_data: u128,
        ) void {
            assert(self.request_inflight == null);
            assert(self.request_number == 0);

            const message = self.get_message().build(.request);
            errdefer self.release_message(message.base());

            // We will set parent, session, view and checksums only when sending for the first time:
            message.header.* = .{
                .client = self.id,
                .request = self.request_number,
                .cluster = self.cluster,
                .command = .request,
                .operation = .register,
                .release = vsr.Release.minimum,
                .previous_request_latency = 0,
            };

            assert(self.request_number == 0);
            self.request_number += 1;

            self.request_inflight = .{
                .message = message,
                .user_data = user_data,
                .callback = .{ .register = callback },
            };
        }

        pub fn request(
            self: *EchoClient,
            callback: Request.Callback,
            user_data: u128,
            operation: Operation,
            events: []const u8,
        ) void {
            const event_size = operation.event_size();
            assert(events.len <= constants.message_body_size_max);
            assert(events.len % event_size == 0);

            const message = self.get_message().build(.request);
            errdefer self.release_message(message.base());

            message.header.* = .{
                .client = self.id,
                .request = 0, // Set by raw_request() below.
                .cluster = self.cluster,
                .command = .request,
                .release = vsr.Release.minimum,
                .operation = operation.to_vsr(),
                .size = @intCast(@sizeOf(Header) + events.len),
                .previous_request_latency = 0,
            };

            stdx.copy_disjoint(.exact, u8, message.body_used(), events);
            self.raw_request(callback, user_data, message);
        }

        pub fn raw_request(
            self: *EchoClient,
            callback: Request.Callback,
            user_data: u128,
            message: *Message.Request,
        ) void {
            assert(message.header.client == self.id);
            assert(message.header.cluster == self.cluster);
            assert(message.header.release.value == self.release.value);
            assert(!message.header.operation.vsr_reserved());
            assert(message.header.size >= @sizeOf(Header));
            assert(message.header.size <= constants.message_size_max);

            message.header.request = self.request_number;
            self.request_number += 1;

            assert(self.request_inflight == null);
            self.request_inflight = .{
                .message = message,
                .user_data = user_data,
                .callback = .{ .request = callback },
            };
        }

        pub fn get_message(self: *EchoClient) *Message {
            return self.message_pool.get_message(null);
        }

        pub fn release_message(self: *EchoClient, message: *Message) void {
            self.message_pool.unref(message);
        }
    };
}

/// Mocks the Accounting StateMachine operation, but replaces
/// all `Result`s with `Event`s, since the echo client replies
/// with the same content of the input.
pub const EchoOperation = enum(u8) {
    const Operation = vsr.tigerbeetle.Operation;

    pulse = @intFromEnum(Operation.pulse),

    get_change_events = @intFromEnum(Operation.get_change_events),

    create_accounts = @intFromEnum(Operation.create_accounts),
    create_transfers = @intFromEnum(Operation.create_transfers),
    lookup_accounts = @intFromEnum(Operation.lookup_accounts),
    lookup_transfers = @intFromEnum(Operation.lookup_transfers),
    get_account_transfers = @intFromEnum(Operation.get_account_transfers),
    get_account_balances = @intFromEnum(Operation.get_account_balances),
    query_accounts = @intFromEnum(Operation.query_accounts),
    query_transfers = @intFromEnum(Operation.query_transfers),

    comptime {
        const operation_type_info = @typeInfo(Operation).@"enum";
        const echo_type_info = @typeInfo(EchoOperation).@"enum";
        assert(echo_type_info.tag_type == operation_type_info.tag_type);
        assert(echo_type_info.is_exhaustive);
        assert(echo_type_info.fields.len <= operation_type_info.fields.len);
        for (echo_type_info.fields) |field| {
            assert(@hasField(Operation, field.name));

            const a = @field(Operation, field.name);
            const b = @field(EchoOperation, field.name);
            assert(@intFromEnum(a) == @intFromEnum(b));
        }
    }

    inline fn cast(operation: EchoOperation) Operation {
        return @enumFromInt(@intFromEnum(operation));
    }

    pub fn EventType(comptime operation: EchoOperation) type {
        return operation.cast().EventType();
    }

    pub inline fn event_size(operation: EchoOperation) u32 {
        return operation.cast().event_size();
    }

    pub inline fn is_batchable(operation: EchoOperation) bool {
        return operation.cast().is_batchable();
    }

    pub inline fn is_multi_batch(operation: EchoOperation) bool {
        return operation.cast().is_multi_batch();
    }

    pub inline fn event_max(operation: EchoOperation, batch_size_limit: u32) u32 {
        return operation.cast().event_max(batch_size_limit);
    }

    pub inline fn result_count_expected(
        operation: EchoOperation,
        batch: []const u8,
    ) u32 {
        return operation.cast().result_count_expected(batch);
    }

    pub fn from_vsr(operation: vsr.Operation) ?EchoOperation {
        if (operation == .pulse) return .pulse;
        if (operation.vsr_reserved()) return null;

        return vsr.Operation.to(EchoOperation, operation);
    }

    pub fn to_vsr(operation: EchoOperation) vsr.Operation {
        return vsr.Operation.from(EchoOperation, operation);
    }

    // Re-exporting functions where results are equal to events.

    pub fn ResultType(comptime operation: EchoOperation) type {
        return operation.EventType();
    }

    pub inline fn result_size(operation: EchoOperation) u32 {
        return operation.event_size();
    }

    pub inline fn result_max(operation: EchoOperation, batch_size_limit: u32) u32 {
        return operation.event_max(batch_size_limit);
    }
};
