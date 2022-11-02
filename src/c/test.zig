const std = @import("std");
const assert = std.debug.assert;

const testing = std.testing;

const message_pool = @import("../message_pool.zig");

const config = @import("../config.zig");
const Packet = @import("tb_client/packet.zig").Packet;

const api = @import("tb_client.zig");
const tb_status_t = api.tb_status_t;
const tb_client_t = api.tb_client_t;
const tb_completion_t = api.tb_completion_t;
const tb_packet_t = api.tb_packet_t;
const tb_packet_list_t = api.tb_packet_list_t;

const Mutex = std.Thread.Mutex;
const Condition = std.Thread.Condition;

fn RequestContextType(comptime request_size: comptime_int) type {
    return struct {
        completion: *Completion,
        sent_data: [request_size]u8 = undefined,
        packet: *tb_packet_t = undefined,
        result: ?struct {
            context: usize,
            client: tb_client_t,
            packet: *tb_packet_t,
            reply: ?[request_size]u8 = null,
        } = null,

        pub fn on_complete(
            context: usize,
            client: tb_client_t,
            packet: *tb_packet_t,
            result_ptr: ?[*]const u8,
            result_len: u32,
        ) callconv(.C) void {
            var self = @intToPtr(*@This(), packet.user_data);
            defer self.completion.complete();
            self.result = .{
                .context = context,
                .client = client,
                .packet = packet,
            };

            // Copy the message's body to the context buffer:
            if (result_ptr != null and result_len > 0) {
                assert(result_len == request_size);
                self.result.?.reply = [_]u8{0} ** request_size;
                std.mem.copy(u8, &self.result.?.reply.?, result_ptr.?[0..result_len]);
            }
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

// When initialized with tb_client_echo_init, the c_client uses a test context that echoes the data back
// without creating an actual client or connecting to a cluster.
//
// This same test should be implemented by all the target programming languages, asserting that:
// 1. the c_client threading/signaling mechanism was initialized correctly.
// 2. the client usage is correct, and they can submit and receive messages through the completion callback.
// 3. the data marshaling is correct, and exactly the same data sent was received back, no matter the message length.
test "c_client echo" {
    const tb_completion_ctx: usize = 42;
    var client: api.tb_client_t = undefined;
    var packet_list: api.tb_packet_list_t = undefined;

    // We ensure that the retry mechanism is being tested by allowing more simultaneous packets than "client_request_queue_max".
    const num_packets = config.client_request_queue_max * 2;

    // Using the create_accounts operation for this test.
    const RequestContext = RequestContextType(@sizeOf(@import("../tigerbeetle.zig").Account));
    const create_accounts_operation: u8 = 3;

    // Initializing an echo client for testing purposes.
    const cluster_id = 0;
    const address = "3000";
    const result = api.tb_client_echo_init(
        &client,
        &packet_list,
        cluster_id,
        address,
        address.len,
        num_packets,
        tb_completion_ctx,
        RequestContext.on_complete,
    );

    try testing.expectEqual(api.tb_status_t.success, result);
    defer api.tb_client_deinit(client);

    var prng = std.rand.DefaultPrng.init(tb_completion_ctx);

    // Repeating the same test multiple times to stress the
    // cycle of message exhaustion followed by completions.
    const repetitions_max = 250;
    var repetition: u32 = 0;
    while (repetition < repetitions_max) : (repetition += 1) {
        var completion = Completion{ .pending = num_packets };
        var requests: [num_packets]RequestContext = undefined;

        // Submitting some random data to be echoed back:
        for (requests) |*request| {
            request.* = .{ .completion = &completion };
            prng.random().bytes(&request.sent_data);

            request.packet = blk: {
                var packet = packet_list.pop().?;
                packet.operation = create_accounts_operation;
                packet.user_data = @ptrToInt(request);
                packet.data = &request.sent_data;
                packet.data_size = @intCast(u32, request.sent_data.len);
                packet.next = null;
                packet.status = .ok;
                break :blk packet;
            };

            var list = Packet.List.from(request.packet);
            api.tb_client_submit(client, &list);
        }

        // Waiting until the c_client thread has processed all submitted requests:
        completion.wait_pending();

        // Checking if the received echo matches the data we sent:
        for (requests) |*request| {
            defer packet_list.push(Packet.List.from(request.packet));

            try testing.expect(request.result != null);
            try testing.expectEqual(tb_completion_ctx, request.result.?.context);
            try testing.expectEqual(client, request.result.?.client);
            try testing.expectEqual(request.packet, request.result.?.packet);
            try testing.expect(request.result.?.reply != null);
            try testing.expectEqualSlices(u8, &request.sent_data, &request.result.?.reply.?);
        }
    }
}
