const std = @import("std");
const log = std.log.scoped(.cdc);
const assert = std.debug.assert;

const vsr = @import("../vsr.zig");
const stdx = vsr.stdx;
const IO = vsr.io.IO;
const Time = vsr.time.Time;
const Storage = vsr.storage.StorageType(IO);
const MessagePool = vsr.message_pool.MessagePool;
const MessageBus = vsr.message_bus.MessageBusClient;
const StateMachine = vsr.state_machine.StateMachineType(
    Storage,
    vsr.constants.state_machine_config,
);
const Client = vsr.ClientType(StateMachine, MessageBus);
const TimestampRange = vsr.lsm.TimestampRange;
const tb = vsr.tigerbeetle;

const Transport = @import("transport.zig").Transport;

/// Generic CDC processor using pluggable transport.
///
/// Producer: TigerBeetle `get_change_events` operation.
/// Consumer: Publishes to transport (AMQP, NATS, Kafka, etc.)
/// Both consumer and producer run concurrently using `io_uring`.
/// See `DualBuffer` for more details.
pub const Runner = struct {
    const StateRecoveryMode = union(enum) {
        recover,
        override: u64,
    };

    const constants = struct {
        const tick_ms = vsr.constants.tick_ms;
        const idle_interval_ns: u63 = 1 * std.time.ns_per_s;
        const reply_timeout_ticks = @divExact(
            30 * std.time.ms_per_s,
            constants.tick_ms,
        );
        const app_id = "tigerbeetle";
        const event_count_max: u32 = Client.StateMachine.operation_result_max(
            .get_change_events,
            vsr.constants.message_body_size_max,
        );
    };

    allocator: std.mem.Allocator,

    io: IO,
    idle_completion: IO.Completion = undefined,
    idle_interval_ns: u63,
    event_count_max: u32,

    message_pool: MessagePool,
    vsr_client: Client,
    buffer: DualBuffer,

    transport: Transport,
    routing_key: []const u8,

    connected: struct {
        vsr: bool = false,
        transport: bool = false,
    },

    producer: enum {
        idle,
        rate_limit,
        request,
        waiting,
    },

    consumer: enum {
        idle,
        publish,
        progress_update,
    },

    rate_limit: RateLimit,
    metrics: Metrics,

    state: union(enum) {
        unknown: StateRecoveryMode,
        recovering: struct {
            timestamp_last: ?u64,
            phase: enum {
                acquire_lock,
                get_progress,
            },
        },
        last: struct {
            producer_timestamp: u64,
            consumer_timestamp: u64,
        },
    },

    pub const Options = struct {
        /// TigerBeetle cluster ID.
        cluster_id: u128,
        /// TigerBeetle cluster addresses.
        addresses: []const std.net.Address,
        /// Routing key / subject prefix for publishing.
        routing_key: []const u8,
        /// Max events per batch.
        event_count_max: ?u32,
        /// Idle interval in milliseconds.
        idle_interval_ms: ?u32,
        /// Rate limit (requests per second).
        requests_per_second_limit: ?u32,
        /// Recovery mode.
        recovery_mode: StateRecoveryMode,
    };

    pub fn init(
        self: *Runner,
        allocator: std.mem.Allocator,
        time: Time,
        transport: Transport,
        options: Options,
    ) !void {
        assert(options.addresses.len > 0);

        const idle_interval_ns: u63 = if (options.idle_interval_ms) |value|
            @intCast(@as(u64, value) * std.time.ns_per_ms)
        else
            constants.idle_interval_ns;
        assert(idle_interval_ns > 0);

        const event_count_max: u32 = if (options.event_count_max) |ecm|
            @min(ecm, constants.event_count_max)
        else
            constants.event_count_max;
        assert(event_count_max > 0);

        var dual_buffer = try DualBuffer.init(allocator, event_count_max);
        errdefer dual_buffer.deinit(allocator);

        self.* = .{
            .allocator = allocator,
            .idle_interval_ns = idle_interval_ns,
            .event_count_max = event_count_max,
            .routing_key = options.routing_key,
            .transport = transport,
            .connected = .{},
            .io = try IO.init(32, 0),
            .producer = .idle,
            .consumer = .idle,
            .rate_limit = undefined,
            .metrics = undefined,
            .state = .{ .unknown = options.recovery_mode },
            .buffer = dual_buffer,
            .message_pool = undefined,
            .vsr_client = undefined,
        };
        errdefer self.io.deinit();

        self.rate_limit = RateLimit.init(time, .{
            .limit = options.requests_per_second_limit orelse std.math.maxInt(u32),
            .period = stdx.Duration.seconds(1),
        });

        self.metrics = .{
            .producer = .{ .timer = .init(time) },
            .consumer = .{ .timer = .init(time) },
            .flush_ticks = 0,
            .flush_timeout_ticks = @divExact(30 * std.time.ms_per_s, constants.tick_ms),
        };

        self.message_pool = try MessagePool.init(allocator, .client);
        errdefer self.message_pool.deinit(allocator);

        self.vsr_client = try Client.init(
            allocator,
            time,
            &self.message_pool,
            .{
                .id = stdx.unique_u128(),
                .cluster = options.cluster_id,
                .replica_count = @intCast(options.addresses.len),
                .message_bus_options = .{ .configuration = options.addresses, .io = &self.io },
            },
        );
        errdefer self.vsr_client.deinit(allocator);

        // Connect transport
        self.transport.connect(&onTransportConnected, self);
    }

    fn onTransportConnected(ctx: *anyopaque, success: bool) void {
        const self: *Runner = @ptrCast(@alignCast(ctx));
        if (success) {
            log.info("Transport connected.", .{});
            self.connected.transport = true;
            self.recover();
        } else {
            log.err("Transport connection failed.", .{});
        }
    }

    pub fn deinit(self: *Runner, allocator: std.mem.Allocator) void {
        self.transport.deinit(allocator);
        self.vsr_client.deinit(allocator);
        self.message_pool.deinit(allocator);
        self.io.deinit();
        self.buffer.deinit(allocator);
    }

    fn recover(self: *Runner) void {
        assert(self.connected.transport);
        assert(self.state == .unknown);

        const recovery_mode = self.state.unknown;
        const timestamp_override: ?u64 = switch (recovery_mode) {
            .recover => null,
            .override => |timestamp| timestamp,
        };

        self.state = .{
            .recovering = .{
                .timestamp_last = timestamp_override,
                .phase = .acquire_lock,
            },
        };

        self.recoverDispatch();
    }

    fn recoverDispatch(self: *Runner) void {
        assert(self.connected.transport);
        assert(self.state == .recovering);

        switch (self.state.recovering.phase) {
            .acquire_lock => {
                self.transport.acquireLock(&onLockAcquired, self);
            },
            .get_progress => {
                if (self.state.recovering.timestamp_last != null) {
                    // Timestamp override - skip progress check
                    self.transitionToRunning();
                } else {
                    self.transport.getProgress(&onProgressReceived, self);
                }
            },
        }
    }

    fn onLockAcquired(ctx: *anyopaque, acquired: bool) void {
        const self: *Runner = @ptrCast(@alignCast(ctx));
        assert(self.state == .recovering);

        if (!acquired) {
            log.err("Failed to acquire lock - another CDC instance may be running", .{});
            // Could retry or exit here
            return;
        }

        log.info("Lock acquired.", .{});
        self.state.recovering.phase = .get_progress;
        self.recoverDispatch();
    }

    fn onProgressReceived(ctx: *anyopaque, progress: ?Transport.ProgressState) void {
        const self: *Runner = @ptrCast(@alignCast(ctx));
        assert(self.state == .recovering);

        if (progress) |p| {
            self.state.recovering.timestamp_last = p.timestamp;
            log.info("Recovered progress: timestamp={}", .{p.timestamp});
        }

        self.transitionToRunning();
    }

    fn transitionToRunning(self: *Runner) void {
        assert(self.state == .recovering);

        const timestamp_last = self.state.recovering.timestamp_last orelse 0;
        self.state = .{
            .last = .{
                .producer_timestamp = timestamp_last,
                .consumer_timestamp = timestamp_last,
            },
        };

        log.info("CDC started from timestamp={}", .{timestamp_last});

        // Register VSR client
        self.vsr_client.register(&onVsrRegistered, @intCast(@intFromPtr(self)));
    }

    fn onVsrRegistered(context: u128, _: *const vsr.RegisterResult) void {
        const self: *Runner = @ptrFromInt(@as(usize, @intCast(context)));
        assert(!self.connected.vsr);
        log.info("VSR client registered.", .{});
        self.connected.vsr = true;
        self.produce();
    }

    pub fn tick(self: *Runner) void {
        self.vsr_client.tick();
        self.transport.tick();
        self.metrics.tick();
    }

    fn produce(self: *Runner) void {
        assert(self.connected.vsr);
        assert(self.connected.transport);
        assert(self.state == .last);

        switch (self.producer) {
            .idle => {
                if (!self.buffer.producer_begin()) {
                    return;
                }
                self.producer = .rate_limit;
                self.produceDispatch();
            },
            else => {},
        }
    }

    fn produceDispatch(self: *Runner) void {
        assert(self.connected.vsr);
        assert(self.connected.transport);
        assert(self.state == .last);

        switch (self.producer) {
            .idle => unreachable,
            .rate_limit => {
                switch (self.rate_limit.attempt()) {
                    .ok => {
                        self.producer = .request;
                        self.metrics.producer.timer.reset();

                        const timestamp_min = self.state.last.producer_timestamp;
                        const filter: tb.ChangeEventsFilter = .{
                            .timestamp_min = timestamp_min,
                            .timestamp_max = 0,
                            .limit = self.event_count_max,
                        };

                        self.vsr_client.request(
                            &onProduceResult,
                            @intFromPtr(self),
                            .get_change_events,
                            std.mem.asBytes(&filter),
                        );
                    },
                    .wait => |duration| {
                        self.io.timeout(
                            *Runner,
                            self,
                            struct {
                                fn callback(
                                    runner: *Runner,
                                    _: *IO.Completion,
                                    result: IO.TimeoutError!void,
                                ) void {
                                    result catch unreachable;
                                    assert(runner.producer == .rate_limit);
                                    runner.produceDispatch();
                                }
                            }.callback,
                            &self.idle_completion,
                            @intCast(duration.ns),
                        );
                    },
                }
            },
            .request => {},
            .waiting => {
                self.io.timeout(
                    *Runner,
                    self,
                    struct {
                        fn callback(
                            runner: *Runner,
                            _: *IO.Completion,
                            result: IO.TimeoutError!void,
                        ) void {
                            result catch unreachable;
                            assert(runner.buffer.all_free());
                            assert(runner.consumer == .idle);
                            assert(runner.producer == .waiting);

                            const producer_begin = runner.buffer.producer_begin();
                            assert(producer_begin);
                            runner.producer = .rate_limit;
                            runner.produceDispatch();
                        }
                    }.callback,
                    &self.idle_completion,
                    self.idle_interval_ns,
                );
            },
        }
    }

    fn onProduceResult(
        context: u128,
        operation_vsr: vsr.Operation,
        timestamp: u64,
        result: []u8,
    ) void {
        const operation = operation_vsr.cast(Client.StateMachine);
        assert(operation == .get_change_events);
        assert(timestamp != 0);
        const self: *Runner = @ptrFromInt(@as(usize, @intCast(context)));
        assert(self.producer == .request);

        const source: []const tb.ChangeEvent = stdx.bytes_as_slice(
            .exact,
            tb.ChangeEvent,
            result,
        );
        const target: []tb.ChangeEvent = self.buffer.get_producer_buffer();
        assert(source.len <= target.len);

        stdx.copy_disjoint(.inexact, tb.ChangeEvent, target, source);
        self.buffer.producer_finish(@intCast(source.len));

        if (self.buffer.all_free()) {
            assert(source.len == 0);
            assert(self.consumer == .idle);
            self.producer = .waiting;
            return self.produceDispatch();
        }

        self.producer = .idle;
        self.metrics.producer.record(source.len);
        assert(source.len > 0 or self.consumer != .idle);

        if (source.len > 0) {
            const timestamp_next = source[source.len - 1].timestamp + 1;
            assert(TimestampRange.valid(timestamp_next));
            self.state.last.producer_timestamp = timestamp_next;

            if (self.consumer == .idle) self.consume();
            self.produce();
        }
    }

    fn consume(self: *Runner) void {
        assert(self.connected.vsr);
        assert(self.connected.transport);
        assert(self.state == .last);

        switch (self.consumer) {
            .idle => {
                if (!self.buffer.consumer_begin()) {
                    return;
                }
                self.consumer = .publish;
                self.metrics.consumer.timer.reset();
                self.consumeDispatch();
            },
            else => {},
        }
    }

    fn consumeDispatch(self: *Runner) void {
        assert(self.connected.vsr);
        assert(self.connected.transport);
        assert(self.state == .last);

        switch (self.consumer) {
            .idle => unreachable,
            .publish => {
                const events: []const tb.ChangeEvent = self.buffer.get_consumer_buffer();
                assert(events.len > 0);

                // Enqueue all events for publishing
                for (events) |*event| {
                    const message = Message.init(event);
                    var json_buffer: [Message.json_string_size_max]u8 = undefined;
                    const json_len = message.writeJson(&json_buffer);

                    self.transport.publishEnqueue(.{
                        .routing_key = self.routing_key,
                        .headers = .{
                            .event_type = @tagName(event.type),
                            .ledger = event.ledger,
                            .transfer_code = event.transfer_code,
                            .debit_account_code = event.debit_account_code,
                            .credit_account_code = event.credit_account_code,
                        },
                        .body = json_buffer[0..json_len],
                        .timestamp = event.timestamp,
                    });
                }

                // Send the batch
                self.transport.publishSend(&onPublishComplete, self);
            },
            .progress_update => {
                const events = self.buffer.get_consumer_buffer();
                assert(events.len > 0);

                const timestamp_last = events[events.len - 1].timestamp;

                self.transport.updateProgress(
                    .{
                        .timestamp = timestamp_last,
                        .release = vsr.constants.state_machine_config.release,
                    },
                    &onProgressUpdated,
                    self,
                );
            },
        }
    }

    fn onPublishComplete(ctx: *anyopaque, success: bool) void {
        const self: *Runner = @ptrCast(@alignCast(ctx));
        assert(self.consumer == .publish);

        if (!success) {
            log.err("Publish failed, will retry", .{});
            // Could add retry logic here
        }

        self.consumer = .progress_update;
        self.consumeDispatch();
    }

    fn onProgressUpdated(ctx: *anyopaque, success: bool) void {
        const self: *Runner = @ptrCast(@alignCast(ctx));
        assert(self.consumer == .progress_update);

        if (!success) {
            log.err("Progress update failed", .{});
        }

        const events = self.buffer.get_consumer_buffer();
        const event_count = events.len;
        const timestamp_last = events[events.len - 1].timestamp;

        self.buffer.consumer_finish();
        self.state.last.consumer_timestamp = timestamp_last;

        self.consumer = .idle;
        self.metrics.consumer.record(event_count);
        self.consume();
    }
};

// Rate limiter
const RateLimit = struct {
    timer: vsr.time.Timer,
    count: u32,
    options: struct {
        limit: u32,
        period: stdx.Duration,
    },

    fn init(time: vsr.time.Time, options: anytype) RateLimit {
        return .{
            .timer = .init(time),
            .count = 0,
            .options = .{
                .limit = options.limit,
                .period = options.period,
            },
        };
    }

    fn attempt(self: *RateLimit) union(enum) {
        ok,
        wait: stdx.Duration,
    } {
        assert(self.options.limit > 0);
        assert(self.options.period.ns > 0);
        assert(self.count <= self.options.limit);

        if (self.count == 0) {
            self.timer.reset();
            self.count = 1;
            return .ok;
        }

        const duration = self.timer.read();

        if (duration.ns >= self.options.period.ns) {
            self.timer.reset();
            self.count = 0;
        } else if (self.count == self.options.limit) {
            return .{ .wait = .{ .ns = self.options.period.ns - duration.ns } };
        }

        self.count += 1;
        return .ok;
    }
};

// Metrics
const Metrics = struct {
    const TimingSummary = struct {
        timer: vsr.time.Timer,
        duration_min: ?stdx.Duration = null,
        duration_max: ?stdx.Duration = null,
        duration_sum: stdx.Duration = .{ .ns = 0 },
        event_count: u64 = 0,
        count: u64 = 0,

        fn record(metrics: *TimingSummary, event_count: u64) void {
            metrics.timing(event_count, metrics.timer.read());
        }

        fn timing(metrics: *TimingSummary, event_count: u64, duration: stdx.Duration) void {
            metrics.count += 1;
            metrics.event_count += event_count;
            metrics.duration_min = if (metrics.duration_min) |min|
                duration.min(min)
            else
                duration;
            metrics.duration_max = if (metrics.duration_max) |max|
                duration.max(max)
            else
                duration;
            metrics.duration_sum.ns += duration.ns;
        }
    };

    producer: TimingSummary,
    consumer: TimingSummary,
    flush_ticks: u64,
    flush_timeout_ticks: u64,

    fn tick(self: *Metrics) void {
        assert(self.flush_ticks < self.flush_timeout_ticks);
        self.flush_ticks += 1;
        if (self.flush_ticks == self.flush_timeout_ticks) {
            self.flush_ticks = 0;
            self.log_and_reset();
        }
    }

    fn log_and_reset(metrics: *Metrics) void {
        const Fields = enum { producer, consumer };
        inline for (comptime std.enums.values(Fields)) |field| {
            const summary: *TimingSummary = &@field(metrics, @tagName(field));
            if (summary.count > 0 and summary.duration_sum.ns > 0) {
                const event_rate = @divTrunc(
                    summary.event_count * std.time.ns_per_s,
                    summary.duration_sum.ns,
                );
                log.info("{s}: p0={}ms mean={}ms p100={}ms events={} throughput={} op/s", .{
                    @tagName(field),
                    summary.duration_min.?.to_ms(),
                    @divFloor(summary.duration_sum.to_ms(), summary.count),
                    summary.duration_max.?.to_ms(),
                    summary.event_count,
                    event_rate,
                });
            }
            summary.* = .{ .timer = summary.timer };
        }
    }
};

// Dual buffer for concurrent producer/consumer
const DualBuffer = struct {
    const State = enum { free, producing, ready, consuming };

    const Buffer = struct {
        buffer: []tb.ChangeEvent,
        state: union(State) {
            free,
            producing,
            ready: u32,
            consuming: u32,
        } = .free,
    };

    buffer_1: Buffer,
    buffer_2: Buffer,

    pub fn init(allocator: std.mem.Allocator, event_count: u32) !DualBuffer {
        const buffer_1 = try allocator.alloc(tb.ChangeEvent, event_count);
        errdefer allocator.free(buffer_1);
        const buffer_2 = try allocator.alloc(tb.ChangeEvent, event_count);
        return .{
            .buffer_1 = .{ .buffer = buffer_1, .state = .free },
            .buffer_2 = .{ .buffer = buffer_2, .state = .free },
        };
    }

    pub fn deinit(self: *const DualBuffer, allocator: std.mem.Allocator) void {
        allocator.free(self.buffer_2.buffer);
        allocator.free(self.buffer_1.buffer);
    }

    pub fn producer_begin(self: *DualBuffer) bool {
        assert(self.find(.producing) == null);
        const buffer = self.find(.free) orelse return false;
        buffer.state = .producing;
        return true;
    }

    pub fn get_producer_buffer(self: *DualBuffer) []tb.ChangeEvent {
        return self.find(.producing).?.buffer;
    }

    pub fn producer_finish(self: *DualBuffer, count: u32) void {
        const buffer = self.find(.producing).?;
        buffer.state = if (count == 0) .free else .{ .ready = count };
    }

    pub fn consumer_begin(self: *DualBuffer) bool {
        assert(self.find(.consuming) == null);
        const buffer = self.find(.ready) orelse return false;
        const count = buffer.state.ready;
        buffer.state = .{ .consuming = count };
        return true;
    }

    pub fn get_consumer_buffer(self: *DualBuffer) []const tb.ChangeEvent {
        const buffer = self.find(.consuming).?;
        return buffer.buffer[0..buffer.state.consuming];
    }

    pub fn consumer_finish(self: *DualBuffer) void {
        self.find(.consuming).?.state = .free;
    }

    pub fn all_free(self: *const DualBuffer) bool {
        return self.buffer_1.state == .free and self.buffer_2.state == .free;
    }

    fn find(self: *DualBuffer, state: State) ?*Buffer {
        if (self.buffer_1.state == state) return &self.buffer_1;
        if (self.buffer_2.state == state) return &self.buffer_2;
        return null;
    }
};

// Message formatting
pub const Message = struct {
    pub const json_string_size_max: usize = 1500;

    timestamp: u64,
    type: tb.ChangeEventType,
    ledger: u32,
    transfer: struct {
        id: u128,
        amount: u128,
        pending_id: u128,
        user_data_128: u128,
        user_data_64: u64,
        user_data_32: u32,
        timeout: u32,
        code: u16,
        flags: u16,
        timestamp: u64,
    },
    debit_account: struct {
        id: u128,
        debits_pending: u128,
        debits_posted: u128,
        credits_pending: u128,
        credits_posted: u128,
        user_data_128: u128,
        user_data_64: u64,
        user_data_32: u32,
        code: u16,
        flags: u16,
        timestamp: u64,
    },
    credit_account: struct {
        id: u128,
        debits_pending: u128,
        debits_posted: u128,
        credits_pending: u128,
        credits_posted: u128,
        user_data_128: u128,
        user_data_64: u64,
        user_data_32: u32,
        code: u16,
        flags: u16,
        timestamp: u64,
    },

    pub fn init(event: *const tb.ChangeEvent) Message {
        return .{
            .timestamp = event.timestamp,
            .type = event.type,
            .ledger = event.ledger,
            .transfer = .{
                .id = event.transfer_id,
                .amount = event.transfer_amount,
                .pending_id = event.transfer_pending_id,
                .user_data_128 = event.transfer_user_data_128,
                .user_data_64 = event.transfer_user_data_64,
                .user_data_32 = event.transfer_user_data_32,
                .timeout = event.transfer_timeout,
                .code = event.transfer_code,
                .flags = @bitCast(event.transfer_flags),
                .timestamp = event.transfer_timestamp,
            },
            .debit_account = .{
                .id = event.debit_account_id,
                .debits_pending = event.debit_account_debits_pending,
                .debits_posted = event.debit_account_debits_posted,
                .credits_pending = event.debit_account_credits_pending,
                .credits_posted = event.debit_account_credits_posted,
                .user_data_128 = event.debit_account_user_data_128,
                .user_data_64 = event.debit_account_user_data_64,
                .user_data_32 = event.debit_account_user_data_32,
                .code = event.debit_account_code,
                .flags = @bitCast(event.debit_account_flags),
                .timestamp = event.debit_account_timestamp,
            },
            .credit_account = .{
                .id = event.credit_account_id,
                .debits_pending = event.credit_account_debits_pending,
                .debits_posted = event.credit_account_debits_posted,
                .credits_pending = event.credit_account_credits_pending,
                .credits_posted = event.credit_account_credits_posted,
                .user_data_128 = event.credit_account_user_data_128,
                .user_data_64 = event.credit_account_user_data_64,
                .user_data_32 = event.credit_account_user_data_32,
                .code = event.credit_account_code,
                .flags = @bitCast(event.credit_account_flags),
                .timestamp = event.credit_account_timestamp,
            },
        };
    }

    pub fn writeJson(self: *const Message, buffer: []u8) usize {
        var fbs = std.io.fixedBufferStream(buffer);
        std.json.stringify(self, .{
            .whitespace = .minified,
            .emit_nonportable_numbers_as_strings = true,
        }, fbs.writer()) catch return 0;
        return fbs.pos;
    }
};
