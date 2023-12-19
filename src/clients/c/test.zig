const std = @import("std");
const assert = std.debug.assert;

const testing = std.testing;

const c = @cImport({
    _ = @import("tb_client_exports.zig"); // Needed for the @export()'ed C ffi functions.
    @cInclude("tb_client.h");
});

const stdx = @import("../../stdx.zig");
const constants = @import("../../constants.zig");
const Packet = @import("tb_client/packet.zig").Packet;

const Mutex = std.Thread.Mutex;
const Condition = std.Thread.Condition;

fn RequestContextType(comptime request_size_max: comptime_int) type {
    return struct {
        const Self = @This();

        completion: *Completion,
        sent_data: [request_size_max]u8 = undefined,
        sent_data_size: u32,
        packet: *c.tb_packet_t = undefined,
        reply: ?struct {
            tb_context: usize,
            tb_client: c.tb_client_t,
            tb_packet: *c.tb_packet_t,
            result: ?[request_size_max]u8,
            result_len: u32,
        } = null,

        pub fn on_complete(
            tb_context: usize,
            tb_client: c.tb_client_t,
            tb_packet: [*c]c.tb_packet_t,
            result_ptr: [*c]const u8,
            result_len: u32,
        ) callconv(.C) void {
            var self: *Self = @ptrCast(@alignCast(tb_packet.*.user_data.?));
            defer self.completion.complete();

            self.reply = .{
                .tb_context = tb_context,
                .tb_client = tb_client,
                .tb_packet = tb_packet,
                .result = if (result_ptr != null and result_len > 0) blk: {
                    // Copy the message's body to the context buffer:
                    assert(result_len <= request_size_max);
                    var writable: [request_size_max]u8 = undefined;
                    const readable = @as([*]const u8, @ptrCast(result_ptr.?));
                    stdx.copy_disjoint(.inexact, u8, &writable, readable[0..result_len]);
                    break :blk writable;
                } else null,
                .result_len = result_len,
            };
        }
    };
}

// Notifies the main thread when all pending requests are completed.
const Completion = struct {
    const Self = @This();

    pending: usize,
    mutex: Mutex = .{},
    cond: Condition = .{},

    pub fn complete(self: *Self) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        assert(self.pending > 0);
        self.pending -= 1;
        self.cond.signal();
    }

    pub fn wait_pending(self: *Self) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        while (self.pending > 0)
            self.cond.wait(&self.mutex);
    }
};

// When initialized with tb_client_init_echo, the c_client uses a test context that echoes
// the data back without creating an actual client or connecting to a cluster.
//
// This same test should be implemented by all the target programming languages, asserting that:
// 1. the c_client api was initialized correctly.
// 2. the application can submit messages and receive replies through the completion callback.
// 3. the data marshaling is correct, and exactly the same data sent was received back.
test "c_client echo" {
    // Using the create_accounts operation for this test.
    const RequestContext = RequestContextType(constants.message_body_size_max);
    const create_accounts_operation: u8 = c.TB_OPERATION_CREATE_ACCOUNTS;
    const event_size = @sizeOf(c.tb_account_t);
    const event_request_max = @divFloor(constants.message_body_size_max, event_size);

    // Initializing an echo client for testing purposes.
    // We ensure that the retry mechanism is being tested
    // by allowing more simultaneous packets than "client_request_queue_max".
    var tb_client: c.tb_client_t = undefined;
    const cluster_id = 0;
    const address = "3000";
    const concurrency_max: u32 = constants.client_request_queue_max * 2;
    const tb_context: usize = 42;
    const result = c.tb_client_init_echo(
        &tb_client,
        cluster_id,
        address,
        @as(u32, @intCast(address.len)),
        concurrency_max,
        tb_context,
        RequestContext.on_complete,
    );

    try testing.expectEqual(@as(c_uint, c.TB_STATUS_SUCCESS), result);
    defer c.tb_client_deinit(tb_client);

    var prng = std.rand.DefaultPrng.init(tb_context);

    var requests: []RequestContext = try testing.allocator.alloc(RequestContext, concurrency_max);
    defer testing.allocator.free(requests);

    // Repeating the same test multiple times to stress the
    // cycle of message exhaustion followed by completions.
    const repetitions_max = 100;
    var repetition: u32 = 0;
    while (repetition < repetitions_max) : (repetition += 1) {
        var completion = Completion{ .pending = concurrency_max };

        // Submitting some random data to be echoed back:
        for (requests) |*request| {
            request.* = .{
                .completion = &completion,
                .sent_data_size = prng.random().intRangeAtMost(u32, 1, event_request_max) * event_size,
            };
            prng.random().bytes(request.sent_data[0..request.sent_data_size]);

            request.packet = blk: {
                var out_packet: ?*c.tb_packet_t = null;
                const packet_acquire_status = c.tb_client_acquire_packet(tb_client, &out_packet);

                if (out_packet) |packet| {
                    try testing.expectEqual(@as(c_uint, c.TB_PACKET_ACQUIRE_OK), packet_acquire_status);

                    packet.operation = create_accounts_operation;
                    packet.user_data = request;
                    packet.data = &request.sent_data;
                    packet.data_size = request.sent_data_size;
                    packet.next = null;
                    packet.status = c.TB_PACKET_OK;
                    break :blk packet;
                } else unreachable;
            };

            c.tb_client_submit(tb_client, request.packet);
        }

        // Waiting until the c_client thread has processed all submitted requests:
        completion.wait_pending();

        // Checking if the received echo matches the data we sent:
        for (requests) |*request| {
            defer c.tb_client_release_packet(tb_client, request.packet);

            try testing.expect(request.reply != null);
            try testing.expectEqual(tb_context, request.reply.?.tb_context);
            try testing.expectEqual(tb_client, request.reply.?.tb_client);
            try testing.expectEqual(c.TB_PACKET_OK, request.packet.status);
            try testing.expectEqual(@intFromPtr(request.packet), @intFromPtr(request.reply.?.tb_packet));
            try testing.expect(request.reply.?.result != null);
            try testing.expectEqual(request.sent_data_size, request.reply.?.result_len);

            const sent_data = request.sent_data[0..request.sent_data_size];
            const reply = request.reply.?.result.?[0..request.reply.?.result_len];
            try testing.expectEqualSlices(u8, sent_data, reply);
        }
    }
}

// Asserts the validation rules associated with the "TB_STATUS" enum.
test "c_client tb_status" {
    const assert_status = struct {
        pub fn action(
            concurrency_max: u32,
            addresses: []const u8,
            expected_status: c_uint,
        ) !void {
            var tb_client: c.tb_client_t = undefined;
            const cluster_id = 0;
            const tb_context: usize = 0;
            const result = c.tb_client_init_echo(
                &tb_client,
                cluster_id,
                addresses.ptr,
                @as(u32, @intCast(addresses.len)),
                concurrency_max,
                tb_context,
                RequestContextType(0).on_complete,
            );
            defer if (result == c.TB_STATUS_SUCCESS) c.tb_client_deinit(tb_client);

            try testing.expectEqual(expected_status, result);
        }
    }.action;

    // Valid addresses and concurrency max should return TB_STATUS_SUCCESS:
    try assert_status(1, "3000", c.TB_STATUS_SUCCESS);
    try assert_status(32, "127.0.0.1", c.TB_STATUS_SUCCESS);
    try assert_status(128, "127.0.0.1:3000", c.TB_STATUS_SUCCESS);
    try assert_status(512, "3000,3001,3002", c.TB_STATUS_SUCCESS);
    try assert_status(1024, "127.0.0.1,127.0.0.2,172.0.0.3", c.TB_STATUS_SUCCESS);
    try assert_status(8192, "127.0.0.1:3000,127.0.0.1:3002,127.0.0.1:3003", c.TB_STATUS_SUCCESS);

    // Invalid or empty address should return "TB_STATUS_ADDRESS_INVALID":
    try assert_status(1, "invalid", c.TB_STATUS_ADDRESS_INVALID);
    try assert_status(1, "", c.TB_STATUS_ADDRESS_INVALID);

    // More addresses thant "replicas_max" should return "TB_STATUS_ADDRESS_LIMIT_EXCEEDED":
    try assert_status(
        1,
        ("3000," ** constants.replicas_max) ++ "3001",
        c.TB_STATUS_ADDRESS_LIMIT_EXCEEDED,
    );

    // ConcurrencyMax Zero or greater than 4096 should return "TB_STATUS_CONCURRENCY_MAX_INVALID":
    try assert_status(0, "3000", c.TB_STATUS_CONCURRENCY_MAX_INVALID);
    try assert_status(8193, "3000", c.TB_STATUS_CONCURRENCY_MAX_INVALID);
    try assert_status(std.math.maxInt(u32), "3000", c.TB_STATUS_CONCURRENCY_MAX_INVALID);

    // All other status are not testable.
}

// Asserts the validation rules associated with the "TB_PACKET_STATUS" enum.
test "c_client tb_packet_status" {
    const RequestContext = RequestContextType(constants.message_body_size_max);

    var tb_client: c.tb_client_t = undefined;
    const cluster_id = 0;
    const address = "3000";
    const concurrency_max = 1;
    const tb_context: usize = 42;
    const result = c.tb_client_init_echo(
        &tb_client,
        cluster_id,
        address,
        @as(u32, @intCast(address.len)),
        concurrency_max,
        tb_context,
        RequestContext.on_complete,
    );

    try testing.expectEqual(@as(c_uint, c.TB_STATUS_SUCCESS), result);
    defer c.tb_client_deinit(tb_client);

    const assert_result = struct {
        // Asserts if the packet's status matches the expected status
        // for a given operation and request_size.
        pub fn action(
            client: c.tb_client_t,
            operation: u8,
            request_size: u32,
            tb_packet_status_expected: c_int,
        ) !void {
            var completion = Completion{ .pending = 1 };
            var request = RequestContext{
                .completion = &completion,
                .sent_data_size = request_size,
            };

            request.packet = blk: {
                var out_packet: ?*c.tb_packet_t = null;
                const packet_acquire_status = c.tb_client_acquire_packet(client, &out_packet);

                if (out_packet) |packet| {
                    try testing.expectEqual(@as(c_uint, c.TB_PACKET_ACQUIRE_OK), packet_acquire_status);

                    packet.operation = operation;
                    packet.user_data = &request;
                    packet.data = &request.sent_data;
                    packet.data_size = request_size;
                    packet.next = null;
                    packet.status = c.TB_PACKET_OK;
                    break :blk packet;
                } else unreachable;
            };
            defer c.tb_client_release_packet(client, request.packet);

            c.tb_client_submit(client, request.packet);

            completion.wait_pending();

            try testing.expect(request.reply != null);
            try testing.expectEqual(tb_context, request.reply.?.tb_context);
            try testing.expectEqual(client, request.reply.?.tb_client);
            try testing.expectEqual(@intFromPtr(request.packet), @intFromPtr(request.reply.?.tb_packet));
            try testing.expectEqual(tb_packet_status_expected, request.packet.status);
        }
    }.action;

    // Messages larger than constants.message_body_size_max should return "too_much_data":
    try assert_result(
        tb_client,
        c.TB_OPERATION_CREATE_TRANSFERS,
        constants.message_body_size_max + @sizeOf(c.tb_transfer_t),
        c.TB_PACKET_TOO_MUCH_DATA,
    );

    // All reserved and unknown operations should return "invalid_operation":
    try assert_result(
        tb_client,
        0,
        @sizeOf(u128),
        c.TB_PACKET_INVALID_OPERATION,
    );
    try assert_result(
        tb_client,
        1,
        @sizeOf(u128),
        c.TB_PACKET_INVALID_OPERATION,
    );
    try assert_result(
        tb_client,
        2,
        @sizeOf(u128),
        c.TB_PACKET_INVALID_OPERATION,
    );
    try assert_result(
        tb_client,
        99,
        @sizeOf(u128),
        c.TB_PACKET_INVALID_OPERATION,
    );

    // Messages length 0 or not a multiple of the event size
    // should return "invalid_data_size":
    try assert_result(
        tb_client,
        c.TB_OPERATION_CREATE_ACCOUNTS,
        0,
        c.TB_PACKET_INVALID_DATA_SIZE,
    );
    try assert_result(
        tb_client,
        c.TB_OPERATION_CREATE_TRANSFERS,
        @sizeOf(c.tb_transfer_t) - 1,
        c.TB_PACKET_INVALID_DATA_SIZE,
    );
    try assert_result(
        tb_client,
        c.TB_OPERATION_LOOKUP_TRANSFERS,
        @sizeOf(u128) + 1,
        c.TB_PACKET_INVALID_DATA_SIZE,
    );
    try assert_result(
        tb_client,
        c.TB_OPERATION_LOOKUP_ACCOUNTS,
        @sizeOf(u128) * 2.5,
        c.TB_PACKET_INVALID_DATA_SIZE,
    );
}
