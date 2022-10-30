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

const RequestContext = struct {
    completion: *Completion,
    sent_data: [128]u8 = undefined,
    packet: *tb_packet_t = undefined,
    result: ?struct {
        context: usize,
        client: tb_client_t,
        packet: *tb_packet_t,
        result_ptr: ?[*]const u8,
        result_len: u32,
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
            .result_ptr = result_ptr,
            .result_len = result_len,
        };
    }
};

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

    // We ensure that the retry mechanism is being tested by allowing more simultaneous packets than "messages_max_client".
    const num_packets = message_pool.messages_max_client * 2;

    const result = api.tb_client_init(
        &client,
        &packet_list,
        0,
        "",
        0,
        num_packets,
        tb_completion_ctx,
        RequestContext.on_complete,
    );

    try testing.expectEqual(api.tb_status_t.success, result);
    defer api.tb_client_deinit(client);

    var max_repetitions: u32 = 1_000;
    while (max_repetitions > 0) : (max_repetitions -= 1) {
        var completion = Completion{ .pending = num_packets };
        var requests: [num_packets]RequestContext = undefined;

        // Submitting some random data to be echoed back:
        for (requests) |*request| {
            request.* = .{ .completion = &completion };
            std.crypto.random.bytes(&request.sent_data);

            request.packet = blk: {
                var packet = packet_list.pop() orelse unreachable;
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
            try testing.expect(request.result.?.result_ptr != null);
            try testing.expect(request.result.?.result_len == request.sent_data.len);

            const echo = request.result.?.result_ptr.?[0..request.result.?.result_len];
            try testing.expectEqualSlices(u8, &request.sent_data, echo);
        }
    }
}
