const std = @import("std");
const assert = std.debug.assert;

const testing = std.testing;
const constants = @import("../constants.zig");

const tb_client = @import("../../src/clients/c/tb_client.zig");
const Context = tb_client.Context;
const Packet = tb_client.Packet;
const PacketStatus = tb_client.PacketStatus;
const InitError = tb_client.InitError;
const ClientInterface = tb_client.ClientInterface;
const Operation = tb_client.Operation;

const TmpTigerBeetle = @import("../testing/tmp_tigerbeetle.zig");

pub const CLIArgs = struct {};

const TestingContext = struct {
    mutex: std.Thread.Mutex = .{},
    cond: std.Thread.Condition = .{},
    reply: ?struct {
        tb_context: usize,
        tb_packet: *Packet,
        timestamp: u64,
        result_size: u32,
    } = null,

    pub fn wait_pending(self: *TestingContext) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        while (self.reply == null) {
            self.cond.wait(&self.mutex);
        }
    }

    pub fn on_complete(
        tb_context: usize,
        tb_packet: *Packet,
        timestamp: u64,
        result: ?[*]const u8,
        result_size: u32,
    ) callconv(.c) void {
        _ = result;
        var self: *TestingContext = @ptrCast(@alignCast(tb_packet.*.user_data.?));

        self.mutex.lock();
        defer self.mutex.unlock();

        assert(self.reply == null);
        self.reply = .{
            .tb_context = tb_context,
            .tb_packet = tb_packet,
            .timestamp = timestamp,
            .result_size = result_size,
        };
        self.cond.signal();
    }
};

pub fn main(gpa: std.mem.Allocator, cli_args: CLIArgs) !void {
    _ = cli_args;

    var tmp_beetle = try TmpTigerBeetle.init(gpa, .{
        .development = false,
    });
    defer tmp_beetle.deinit(gpa);

    try test_init(gpa, tmp_beetle.port_str);
    try test_client_status(gpa, tmp_beetle.port_str);
    try test_packet_status(gpa, tmp_beetle.port_str);
}

// Asserts the validation rules associated with the `init*` functions.
fn test_init(gpa: std.mem.Allocator, _: []const u8) !void {
    const init = struct {
        fn init(allocator: std.mem.Allocator, addresses: []const u8) !void {
            var client: ClientInterface = undefined;
            const cluster_id: u128 = 0;
            try Context.init(
                allocator,
                &client,
                cluster_id,
                addresses,
                0,
                TestingContext.on_complete,
            );
            client.deinit() catch unreachable;
        }
    }.init;

    // Valid addresses should return TB_STATUS_SUCCESS:
    try init(gpa, "3000");
    try init(gpa, "127.0.0.1");
    try init(gpa, "127.0.0.1:3000");
    try init(gpa, "3000,3001,3002");
    try init(gpa, "127.0.0.1,127.0.0.2,172.0.0.3");
    try init(gpa, "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002");

    // Invalid or empty address should return "TB_STATUS_ADDRESS_INVALID":
    try testing.expectError(InitError.AddressInvalid, init(gpa, "invalid"));
    try testing.expectError(InitError.AddressInvalid, init(gpa, "127.0.0.256"));
    try testing.expectError(InitError.AddressInvalid, init(gpa, "127.0.0.1.2"));
    try testing.expectError(InitError.AddressInvalid, init(gpa, "127.0.0.1:99000"));
    try testing.expectError(InitError.AddressInvalid, init(gpa, "99000"));
    try testing.expectError(InitError.AddressInvalid, init(gpa, ""));

    // More addresses than "replicas_max" should return "TB_STATUS_ADDRESS_LIMIT_EXCEEDED":
    try testing.expectError(
        InitError.AddressLimitExceeded,
        init(gpa, ("3000," ** constants.replicas_max) ++ "3001"),
    );

    // All other status are not testable.
}

// Asserts the validation rules associated with the client status.
fn test_client_status(gpa: std.mem.Allocator, addresses: []const u8) !void {
    var request: TestingContext = .{};
    var packet: Packet = .{
        .operation = @intFromEnum(Operation.create_accounts),
        .user_data = &request,
        .data = null,
        .data_size = 0,
        .user_tag = 0,
        .status = .ok,
    };

    // An uninitialized client must return `ClientInvalid`.
    var client: ClientInterface = undefined;
    try testing.expectError(ClientInterface.Error.ClientInvalid, client.submit(&packet));

    // Initializing the client.
    const cluster_id: u128 = 0;
    try Context.init(
        gpa,
        &client,
        cluster_id,
        addresses,
        0,
        TestingContext.on_complete,
    );
    errdefer client.deinit() catch |err| switch (err) {
        error.ClientInvalid => {},
    };

    // Sanity test to verify that the client is working.
    try client.submit(&packet);
    request.wait_pending();

    // Deinit the client.
    try client.deinit();

    // Cannot submit after deinit.
    try testing.expectError(ClientInterface.Error.ClientInvalid, client.submit(&packet));

    // Multiple deinit calls are safe.
    try testing.expectError(ClientInterface.Error.ClientInvalid, client.deinit());
}

// Asserts the validation rules associated with the "PacketStatus" enum.
fn test_packet_status(gpa: std.mem.Allocator, addresses: []const u8) !void {
    var client: ClientInterface = undefined;
    const cluster_id: u128 = 0;
    const tb_context: usize = 42;
    try Context.init(
        gpa,
        &client,
        cluster_id,
        addresses,
        tb_context,
        TestingContext.on_complete,
    );
    defer client.deinit() catch unreachable;

    const submit = struct {
        fn submit(
            client_interface: *ClientInterface,
            operation: u8,
            request_size: u32,
        ) !tb_client.PacketStatus {
            var request: TestingContext = .{};
            var packet: Packet = .{
                .operation = operation,
                .user_data = &request,
                .data = &[0]u8{}, // It won't be dereferenced during the tests.
                .data_size = request_size,
                .user_tag = 0,
                .status = .ok,
            };

            try client_interface.submit(&packet);
            request.wait_pending();

            try testing.expect(request.reply != null);
            try testing.expectEqual(tb_context, request.reply.?.tb_context);
            try testing.expectEqual(
                @intFromPtr(&packet),
                @intFromPtr(request.reply.?.tb_packet),
            );

            return packet.status;
        }
    }.submit;

    // Messages larger than constants.message_body_size_max should return "too_much_data":
    try std.testing.expectEqual(PacketStatus.too_much_data, try submit(
        &client,
        @intFromEnum(tb_client.Operation.create_transfers),
        constants.message_body_size_max + @sizeOf(tb_client.exports.tb_transfer_t),
    ));

    // All reserved and unknown operations should return "invalid_operation":
    try std.testing.expectEqual(
        PacketStatus.invalid_operation,
        try submit(&client, 0, @sizeOf(u128)),
    );
    try std.testing.expectEqual(
        PacketStatus.invalid_operation,
        try submit(&client, 1, @sizeOf(u128)),
    );
    try std.testing.expectEqual(
        PacketStatus.invalid_operation,
        try submit(&client, std.math.maxInt(u8), @sizeOf(u128)),
    );

    // Messages not a multiple of the event size
    // should return "invalid_data_size":
    try std.testing.expectEqual(
        PacketStatus.invalid_data_size,
        try submit(
            &client,
            @intFromEnum(Operation.create_transfers),
            @sizeOf(tb_client.exports.tb_transfer_t) - 1,
        ),
    );
    try std.testing.expectEqual(
        PacketStatus.invalid_data_size,
        try submit(
            &client,
            @intFromEnum(Operation.lookup_transfers),
            @sizeOf(u128) + 1,
        ),
    );
    try std.testing.expectEqual(
        PacketStatus.invalid_data_size,
        try submit(
            &client,
            @intFromEnum(Operation.lookup_accounts),
            @sizeOf(u128) * 2.5,
        ),
    );

    // Batches with zero length are valid.
    try std.testing.expectEqual(
        PacketStatus.ok,
        try submit(
            &client,
            @intFromEnum(Operation.create_accounts),
            0,
        ),
    );

    // Non-batched operations require exactly one event.
    try std.testing.expectEqual(
        PacketStatus.invalid_data_size,
        try submit(
            &client,
            @intFromEnum(Operation.query_accounts),
            0,
        ),
    );
    try std.testing.expectEqual(
        PacketStatus.invalid_data_size,
        try submit(
            &client,
            @intFromEnum(Operation.query_transfers),
            @sizeOf(tb_client.exports.tb_query_filter_t) * 2,
        ),
    );
}
