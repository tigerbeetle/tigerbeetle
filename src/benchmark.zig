const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const config = @import("config.zig");

const cli = @import("cli.zig");
const IO = @import("io.zig").IO;

const MessageBus = @import("message_bus.zig").MessageBusClient;
const StateMachine = @import("state_machine.zig").StateMachine;
const Operation = StateMachine.Operation;
const RingBuffer = @import("ring_buffer.zig").RingBuffer;

const vsr = @import("vsr.zig");
const Header = vsr.Header;
const Client = vsr.Client(StateMachine, MessageBus);

const tb = @import("tigerbeetle.zig");
const Transfer = tb.Transfer;
const Commit = tb.Commit;
const Account = tb.Account;
const CreateAccountsResult = tb.CreateAccountsResult;
const CreateTransfersResult = tb.CreateTransfersResult;

const MAX_TRANSFERS: u32 = 1_000_000;
const BATCH_SIZE: u32 = 5_000;
const IS_TWO_PHASE_COMMIT = false;
const BENCHMARK = if (IS_TWO_PHASE_COMMIT) 500_000 else 1_000_000;
const RESULT_TOLERANCE = 10; // percent
const BATCHES: f32 = MAX_TRANSFERS / BATCH_SIZE;
const TOTAL_BATCHES = @ceil(BATCHES);

const log = std.log;
pub const log_level: std.log.Level = .info;

var accounts = [_]Account{
    Account{
        .id = 1,
        .user_data = 0,
        .reserved = [_]u8{0} ** 48,
        .unit = 2,
        .code = 0,
        .flags = .{},
        .debits_reserved = 0,
        .debits_accepted = 0,
        .credits_reserved = 0,
        .credits_accepted = 0,
    },
    Account{
        .id = 2,
        .user_data = 0,
        .reserved = [_]u8{0} ** 48,
        .unit = 2,
        .code = 0,
        .flags = .{},
        .debits_reserved = 0,
        .debits_accepted = 0,
        .credits_reserved = 0,
        .credits_accepted = 0,
    },
};
var max_create_transfers_latency: i64 = 0;
var max_commit_transfers_latency: i64 = 0;

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();
    const stderr = std.io.getStdErr().writer();

    if (builtin.mode != .ReleaseSafe and builtin.mode != .ReleaseFast) {
        try stderr.print("Benchmark must be built as ReleaseSafe for minimum performance.\n", .{});
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
    var transfers = try arena.allocator().alloc(Transfer, MAX_TRANSFERS);
    for (transfers) |*transfer, index| {
        transfer.* = .{
            .id = index,
            .debit_account_id = accounts[0].id,
            .credit_account_id = accounts[1].id,
            .user_data = 0,
            .reserved = [_]u8{0} ** 32,
            .code = 0,
            .flags = if (IS_TWO_PHASE_COMMIT) .{ .two_phase_commit = true } else .{},
            .amount = 1,
            .timeout = if (IS_TWO_PHASE_COMMIT) std.time.ns_per_hour else 0,
        };
    }

    // Pre-allocate a million commits:
    var commits: ?[]Commit = if (IS_TWO_PHASE_COMMIT) try arena.allocator().alloc(Commit, MAX_TRANSFERS) else null;
    if (commits) |all_commits| {
        for (all_commits) |*commit, index| {
            commit.* = .{
                .id = index,
                .reserved = [_]u8{0} ** 32,
                .code = 0,
                .flags = .{},
            };
        }
    }

    try wait_for_connect(&client, &io);

    try stdout.print("creating accounts...\n", .{});
    var queue = TimedQueue.init(&client, &io);
    try queue.push(.{
        .operation = Operation.create_accounts,
        .data = std.mem.sliceAsBytes(accounts[0..]),
    });
    try queue.execute();
    assert(queue.end != null);
    assert(queue.batches.empty());

    try stdout.print("batching transfers...\n", .{});
    var count: u64 = 0;
    queue.reset();
    while (count < transfers.len) {
        try queue.push(.{
            .operation = .create_transfers,
            .data = std.mem.sliceAsBytes(transfers[count..][0..BATCH_SIZE]),
        });

        if (IS_TWO_PHASE_COMMIT) {
            try queue.push(.{
                .operation = .commit_transfers,
                .data = std.mem.sliceAsBytes(commits.?[count..][0..BATCH_SIZE]),
            });
        }

        count += BATCH_SIZE;
    }
    assert(count == MAX_TRANSFERS);

    try stdout.print("starting benchmark...\n", .{});
    try queue.execute();
    assert(queue.end != null);
    assert(queue.batches.empty());

    var ms = queue.end.? - queue.start.?;
    const transfer_type = if (IS_TWO_PHASE_COMMIT) "two-phase commit " else "";
    const result: i64 = @divFloor(@intCast(i64, transfers.len * 1000), ms);
    try stdout.print("============================================\n", .{});
    try stdout.print("{} {s}transfers per second\n\n", .{
        result,
        transfer_type,
    });
    try stdout.print("create_transfers max p100 latency per {} transfers = {}ms\n", .{
        BATCH_SIZE,
        queue.max_transfers_latency,
    });
    try stdout.print("commit_transfers max p100 latency per {} transfers = {}ms\n", .{
        BATCH_SIZE,
        queue.max_commits_latency,
    });
}

const Batch = struct {
    operation: Operation,
    data: []u8,
};

const TimedQueue = struct {
    batch_start: ?i64,
    start: ?i64,
    end: ?i64,
    max_transfers_latency: i64,
    max_commits_latency: i64,
    client: *Client,
    io: *IO,
    batches: if (IS_TWO_PHASE_COMMIT)
        RingBuffer(Batch, 2 * TOTAL_BATCHES, .array)
    else
        RingBuffer(Batch, TOTAL_BATCHES, .array),

    pub fn init(client: *Client, io: *IO) TimedQueue {
        var self = TimedQueue{
            .batch_start = null,
            .start = null,
            .end = null,
            .max_transfers_latency = 0,
            .max_commits_latency = 0,
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
        self.max_transfers_latency = 0;
        self.max_commits_latency = 0;
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
            var message = self.client.get_message() orelse {
                @panic("Client message pool has been exhausted. Cannot execute batch.");
            };
            defer self.client.unref(message);

            std.mem.copy(
                u8,
                message.buffer[@sizeOf(Header)..],
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

    pub fn lap(user_data: u128, operation: Operation, results: Client.Error![]const u8) void {
        const now = std.time.milliTimestamp();
        const value = results catch |err| {
            log.err("Client returned error={o}", .{@errorName(err)});
            @panic("Client returned error during benchmarking.");
        };

        log.debug("response={s}", .{std.mem.bytesAsSlice(CreateAccountsResult, value)});

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
                if (latency > self.max_transfers_latency) {
                    self.max_transfers_latency = latency;
                }
            },
            .commit_transfers => {
                if (latency > self.max_commits_latency) {
                    self.max_commits_latency = latency;
                }
            },
            else => unreachable,
        }

        if (self.batches.head_ptr()) |next_batch| {
            var message = self.client.get_message() orelse {
                @panic("Client message pool has been exhausted.");
            };
            defer self.client.unref(message);

            std.mem.copy(
                u8,
                message.buffer[@sizeOf(Header)..],
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
