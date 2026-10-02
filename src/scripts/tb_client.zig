const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;

const testing = std.testing;
const constants = @import("../constants.zig");

const tb_client = @import("../../src/clients/c/tb_client.zig");
const Context = tb_client.Context;
const Packet = tb_client.Packet;
const PacketStatus = tb_client.PacketStatus;
const InitError = tb_client.InitError;
const ClientInterface = tb_client.ClientInterface;
const ClientError = tb_client.ClientError;
const Operation = tb_client.Operation;

const TmpTigerBeetle = @import("../testing/tmp_tigerbeetle.zig");
const Shell = @import("stdx").Shell;

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
        .prebuilt = null,
    });
    defer tmp_beetle.deinit(gpa);

    try test_init(gpa, tmp_beetle.port_str);
    try test_client_status(gpa, tmp_beetle.port_str);
    try test_packet_status(gpa, tmp_beetle.port_str);
    try test_deinit(gpa);
}

// Asserts the validation rules associated with the `init` function.
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

    // An uninitialized client must return `NotInitialized`.
    var client: ClientInterface = undefined;
    try testing.expectError(ClientError.NotInitialized, client.submit(&packet));

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
        ClientError.Closed => {},
        ClientError.NotInitialized => unreachable,
    };

    // Sanity test to verify that the client is working.
    try client.submit(&packet);
    request.wait_pending();

    // Deinit the client.
    try client.deinit();

    // Cannot submit after deinit.
    try testing.expectError(ClientError.Closed, client.submit(&packet));

    // Multiple deinit calls are safe.
    try testing.expectError(ClientError.Closed, client.deinit());
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

// Closing a client (`deinit`) first tries to end its session, within `deregister_timeout`
// (see `Context.deregister()`). These tests read how that attempt ended from
// `tb_client.deregister_last`, which is recorded only in builds with `config_verify`
// (`constants.verify`, on by default outside `-Drelease`), so `zig build -Drelease ci` skips them.
// They close one client at a time, since the record is per process.
fn test_deinit(gpa: std.mem.Allocator) !void {
    if (!constants.verify) {
        std.log.warn("tb_client deinit tests skipped: they need config_verify", .{});
        return;
    }

    try test_deinit_healthy(gpa);
    try test_deinit_never_registered(gpa);
    try test_deinit_server_gone(gpa);
    try test_deinit_server_stopped(gpa);
    try test_deinit_evicted(gpa);
}

const DeinitClient = struct {
    client: ClientInterface,
    request: TestingContext,

    fn init(deinit_client: *DeinitClient, gpa: std.mem.Allocator, addresses: []const u8) !void {
        deinit_client.* = .{ .client = undefined, .request = .{} };
        const cluster_id: u128 = 0;
        try Context.init(
            gpa,
            &deinit_client.client,
            cluster_id,
            addresses,
            0,
            TestingContext.on_complete,
        );
    }

    fn lookup_account(deinit_client: *DeinitClient) !PacketStatus {
        const id: u128 = 1;
        var packet: Packet = .{
            .operation = @intFromEnum(Operation.lookup_accounts),
            .user_data = &deinit_client.request,
            .data = &id,
            .data_size = @sizeOf(u128),
            .user_tag = 0,
            .status = .ok,
        };
        deinit_client.request.reply = null;
        try deinit_client.client.submit(&packet);
        deinit_client.request.wait_pending();
        return packet.status;
    }

    /// Closes the client, and returns how its deregistration attempt ended.
    fn deinit(deinit_client: *DeinitClient) !tb_client.DeregisterRecord {
        tb_client.deregister_last.* = null;
        try deinit_client.client.deinit();
        return tb_client.deregister_last.* orelse error.DeregisterNotRecorded;
    }
};

fn tmp_beetle_development(gpa: std.mem.Allocator) !TmpTigerBeetle {
    return try TmpTigerBeetle.init(gpa, .{ .development = true, .prebuilt = null });
}

fn test_deinit_healthy(gpa: std.mem.Allocator) !void {
    var tmp_beetle = try tmp_beetle_development(gpa);
    defer tmp_beetle.deinit(gpa);

    var client: DeinitClient = undefined;
    try client.init(gpa, tmp_beetle.port_str);
    try testing.expectEqual(PacketStatus.ok, try client.lookup_account());

    const deregister = try client.deinit();
    try testing.expectEqual(tb_client.DeregisterOutcome.done, deregister.outcome);
    try testing.expect(deregister.duration.ns < tb_client.deregister_timeout.ns);
}

fn test_deinit_never_registered(gpa: std.mem.Allocator) !void {
    // This assumes that nothing listens on TCP port 1, so that the register stays in flight. If
    // something there answers the register, the outcome differs and the test fails loudly; it
    // cannot pass falsely.
    var client: DeinitClient = undefined;
    try client.init(gpa, "1");

    const deregister = try client.deinit();
    try testing.expectEqual(
        tb_client.DeregisterOutcome.skipped_never_registered,
        deregister.outcome,
    );
    // A skip spends none of the budget. (Generous, for slow machines.)
    try testing.expect(deregister.duration.ns < tb_client.deregister_timeout.ns / 2);
}

fn test_deinit_server_gone(gpa: std.mem.Allocator) !void {
    var tmp_beetle = try tmp_beetle_development(gpa);
    var tmp_beetle_stopped = false;
    defer if (!tmp_beetle_stopped) tmp_beetle.deinit(gpa);

    var client: DeinitClient = undefined;
    try client.init(gpa, tmp_beetle.port_str);
    try testing.expectEqual(PacketStatus.ok, try client.lookup_account());

    tmp_beetle.deinit(gpa); // Waits until the process has exited.
    tmp_beetle_stopped = true;
    // The client's IO thread notices the closed connection within a tick or two. The client
    // interface has no way to observe that, so wait generously.
    std.time.sleep(std.time.ns_per_s);

    const deregister = try client.deinit();
    try testing.expectEqual(
        tb_client.DeregisterOutcome.skipped_no_primary_connection,
        deregister.outcome,
    );
    try testing.expect(deregister.duration.ns < tb_client.deregister_timeout.ns / 2);
}

fn test_deinit_server_stopped(gpa: std.mem.Allocator) !void {
    if (builtin.os.tag == .windows) return;

    var tmp_beetle = try tmp_beetle_development(gpa);
    defer tmp_beetle.deinit(gpa);

    var client: DeinitClient = undefined;
    try client.init(gpa, tmp_beetle.port_str);
    try testing.expectEqual(PacketStatus.ok, try client.lookup_account());

    // The connection stays established, but the server never replies.
    try std.posix.kill(tmp_beetle.process.id, std.posix.SIG.STOP);
    defer std.posix.kill(tmp_beetle.process.id, std.posix.SIG.CONT) catch {};

    // Only the deregistration attempt is bounded by the budget: closing the connections
    // afterwards is not. So `deinit` as a whole only gets a generous deadline, to detect hangs.
    const Deinit = struct {
        client: *DeinitClient,
        done: std.Thread.ResetEvent = .{},
        result: anyerror!tb_client.DeregisterRecord = error.Pending,

        fn run(deinit: *@This()) void {
            deinit.result = deinit.client.deinit();
            deinit.done.set();
        }
    };
    var deinit: Deinit = .{ .client = &client };
    const thread = try std.Thread.spawn(.{}, Deinit.run, .{&deinit});
    deinit.done.timedWait(10 * std.time.ns_per_s) catch |err| {
        try std.posix.kill(tmp_beetle.process.id, std.posix.SIG.CONT);
        thread.join();
        return err;
    };
    thread.join();

    const deregister = try deinit.result;
    try testing.expectEqual(tb_client.DeregisterOutcome.timed_out, deregister.outcome);
    // The whole budget is spent, but not much more (generous, for slow machines).
    try testing.expect(deregister.duration.ns >= tb_client.deregister_timeout.ns);
    try testing.expect(deregister.duration.ns <= 2 * tb_client.deregister_timeout.ns);
}

fn test_deinit_evicted(gpa: std.mem.Allocator) !void {
    const shell = try Shell.create(gpa);
    defer shell.destroy();

    var tmp_beetle = try tmp_beetle_development(gpa);
    defer tmp_beetle.deinit(gpa);

    var client: DeinitClient = undefined;
    try client.init(gpa, tmp_beetle.port_str);
    try testing.expectEqual(PacketStatus.ok, try client.lookup_account());

    // Each `repl` process registers a session and exits without ending it (it uses `vsr.Client`
    // directly, which never sends a deregister). The client's session is the oldest, so the
    // last register evicts it.
    for (0..constants.clients_max) |index| {
        try shell.exec_options(
            .{ .timeout = .seconds(30) },
            "{tigerbeetle} repl --cluster=0 --addresses={addresses} --command={command}",
            .{
                .tigerbeetle = tmp_beetle.tigerbeetle_exe,
                .addresses = tmp_beetle.port_str,
                .command = try shell.fmt("create_accounts id={d} ledger=1 code=1", .{index + 1}),
            },
        );
    }
    try testing.expectEqual(PacketStatus.client_evicted, try client.lookup_account());

    const deregister = try client.deinit();
    try testing.expectEqual(tb_client.DeregisterOutcome.skipped_evicted, deregister.outcome);
    try testing.expect(deregister.duration.ns < tb_client.deregister_timeout.ns / 2);
}
