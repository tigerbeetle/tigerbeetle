const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const config = @import("config.zig");

const log = std.log;
pub const log_level: std.log.Level = .err;

const cli = @import("cli.zig");
const IO = @import("io.zig").IO;

const MessageBus = @import("message_bus.zig").MessageBusClient;
const StateMachine = @import("state_machine.zig").StateMachine;
const RingBuffer = @import("ring_buffer.zig").RingBuffer;

const vsr = @import("vsr.zig");
const Client = vsr.Client(StateMachine, MessageBus);

const tb = @import("tigerbeetle.zig");

const batches_count = 100;

const transfers_per_batch: u32 = @divExact(
    config.message_size_max - @sizeOf(vsr.Header),
    @sizeOf(tb.Transfer),
);
comptime {
    assert(transfers_per_batch >= 2041);
}

const transfers_max: u32 = batches_count * transfers_per_batch;

var accounts = [_]tb.Account{
    .{
        .id = 1,
        .user_data = 0,
        .reserved = [_]u8{0} ** 48,
        .ledger = 2,
        .code = 0,
        .flags = .{},
        .debits_pending = 0,
        .debits_posted = 0,
        .credits_pending = 0,
        .credits_posted = 0,
    },
    .{
        .id = 2,
        .user_data = 0,
        .reserved = [_]u8{0} ** 48,
        .ledger = 2,
        .code = 0,
        .flags = .{},
        .debits_pending = 0,
        .debits_posted = 0,
        .credits_pending = 0,
        .credits_posted = 0,
    },
};
var create_transfers_latency_max: i64 = 0;

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();
    const stderr = std.io.getStdErr().writer();

    if (builtin.mode != .ReleaseSafe and builtin.mode != .ReleaseFast) {
        try stderr.print("Benchmark must be built as ReleaseSafe for reasonable results.\n", .{});
    }

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    const client_id = std.crypto.random.int(u128);
    const cluster_id: u32 = 0;
    var address = [_]std.net.Address{try std.net.Address.parseIp4("127.0.0.1", config.port)};

    var io = try IO.init(32, 0);
    var message_bus = try MessageBus.init(allocator, cluster_id, address[0..], client_id, &io);
    defer message_bus.deinit();

    var client = try Client.init(
        allocator,
        client_id,
        cluster_id,
        @intCast(u8, address.len),
        &message_bus,
    );
    defer client.deinit();

    message_bus.set_on_message(*Client, &client, Client.on_message);

    // Pre-allocate a million transfers:
    const transfers = try arena.allocator().alloc(tb.Transfer, transfers_max);
    for (transfers) |*transfer, index| {
        transfer.* = .{
            .id = index,
            .debit_account_id = accounts[0].id,
            .credit_account_id = accounts[1].id,
            .pending_id = 0,
            .user_data = 0,
            .reserved = 0,
            .code = 0,
            .ledger = 2,
            .flags = .{},
            .amount = 1,
            .timeout = 0,
        };
    }

    try wait_for_connect(&client, &io);

    var queue = TimedQueue.init(&client, &io);
    try queue.push(.{
        .operation = StateMachine.Operation.create_accounts,
        .data = std.mem.sliceAsBytes(accounts[0..]),
    });

    try queue.execute();
    assert(queue.end != null);
    assert(queue.batches.empty());

    var count: u64 = 0;
    queue.reset();
    while (count < transfers.len) {
        try queue.push(.{
            .operation = .create_transfers,
            .data = std.mem.sliceAsBytes(transfers[count..][0..transfers_per_batch]),
        });
        count += transfers_per_batch;
    }
    assert(count == transfers_max);

    try queue.execute();
    assert(queue.end != null);
    assert(queue.batches.empty());

    var ms = queue.end.? - queue.start.?;

    const result: i64 = @divFloor(@intCast(i64, transfers.len * 1000), ms);
    try stdout.print("============================================\n", .{});
    try stdout.print("{} transfers per second\n\n", .{result});
    try stdout.print("max p100 latency per {} transfers = {}ms\n", .{
        transfers_per_batch,
        queue.transfers_latency_max,
    });
}

const Batch = struct {
    operation: StateMachine.Operation,
    data: []u8,
};

const TimedQueue = struct {
    batch_start: ?i64,
    start: ?i64,
    end: ?i64,
    transfers_latency_max: i64,
    client: *Client,
    io: *IO,
    batches: RingBuffer(Batch, batches_count),

    pub fn init(client: *Client, io: *IO) TimedQueue {
        var self = TimedQueue{
            .batch_start = null,
            .start = null,
            .end = null,
            .transfers_latency_max = 0,
            .client = client,
            .io = io,
            .batches = .{},
        };

        return self;
    }

    pub fn reset(self: *TimedQueue) void {
        self.batch_start = null;
        self.start = null;
        self.end = null;
        self.transfers_latency_max = 0;
    }

    pub fn push(self: *TimedQueue, batch: Batch) !void {
        try self.batches.push(batch);
    }

    pub fn execute(self: *TimedQueue) !void {
        assert(self.start == null);
        assert(!self.batches.empty());
        self.reset();
        log.debug("executing batches...", .{});

        const now = std.time.milliTimestamp();
        self.start = now;
        if (self.batches.head_ptr()) |starting_batch| {
            log.debug("sending first batch...", .{});
            self.batch_start = now;
            const message = self.client.get_message();
            defer self.client.unref(message);

            std.mem.copy(
                u8,
                message.buffer[@sizeOf(vsr.Header)..],
                std.mem.sliceAsBytes(starting_batch.data),
            );
            self.client.request(
                @intCast(u128, @ptrToInt(self)),
                TimedQueue.lap,
                starting_batch.operation,
                message,
                starting_batch.data.len,
            );
        }

        while (!self.batches.empty()) {
            self.client.tick();
            try self.io.run_for_ns(5 * std.time.ns_per_ms);
        }
    }

    pub fn lap(
        user_data: u128,
        operation: StateMachine.Operation,
        results: Client.Error![]const u8,
    ) void {
        const now = std.time.milliTimestamp();
        const value = results catch |err| {
            log.err("Client returned error={o}", .{@errorName(err)});
            @panic("Client returned error during benchmarking.");
        };

        log.debug("response={s}", .{std.mem.bytesAsSlice(tb.CreateAccountsResult, value)});

        const self: *TimedQueue = @intToPtr(*TimedQueue, @intCast(usize, user_data));
        const completed_batch: ?Batch = self.batches.pop();
        assert(completed_batch != null);
        assert(completed_batch.?.operation == operation);

        log.debug("completed batch operation={} start={}", .{
            completed_batch.?.operation,
            self.batch_start,
        });
        const latency = now - self.batch_start.?;
        switch (operation) {
            .create_accounts => {},
            .create_transfers => {
                if (latency > self.transfers_latency_max) {
                    self.transfers_latency_max = latency;
                }
            },
            else => unreachable,
        }

        if (self.batches.head_ptr()) |next_batch| {
            const message = self.client.get_message();
            defer self.client.unref(message);

            std.mem.copy(
                u8,
                message.buffer[@sizeOf(vsr.Header)..],
                std.mem.sliceAsBytes(next_batch.data),
            );

            self.batch_start = std.time.milliTimestamp();
            self.client.request(
                @intCast(u128, @ptrToInt(self)),
                TimedQueue.lap,
                next_batch.operation,
                message,
                next_batch.data.len,
            );
        } else {
            log.debug("stopping timer...", .{});
            self.end = now;
        }
    }
};

fn wait_for_connect(client: *Client, io: *IO) !void {
    var ticks: u32 = 0;
    while (ticks < 20) : (ticks += 1) {
        client.tick();
        // We tick IO outside of client so that an IO instance can be shared by multiple clients:
        // Otherwise we will hit io_uring memory restrictions too quickly.
        try io.tick();

        std.time.sleep(10 * std.time.ns_per_ms);
    }
}
