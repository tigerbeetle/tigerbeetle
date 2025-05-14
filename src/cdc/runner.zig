const std = @import("std");
const log = std.log.scoped(.amqp);

const vsr = @import("../vsr.zig");
const assert = std.debug.assert;
const maybe = vsr.stdx.maybe;
const fatal = @import("amqp/protocol.zig").fatal;

const stdx = vsr.stdx;
const IO = vsr.io.IO;
const Tracer = vsr.trace.TracerType(vsr.time.Time);
const Storage = vsr.storage.StorageType(IO, Tracer);
const MessagePool = vsr.message_pool.MessagePool;
const MessageBus = vsr.message_bus.MessageBusClient;
const StateMachine = vsr.state_machine.StateMachineType(
    Storage,
    vsr.constants.state_machine_config,
);
const Client = vsr.ClientType(StateMachine, MessageBus, vsr.time.Time);
const TimestampRange = vsr.lsm.TimestampRange;
const tb = vsr.tigerbeetle;

pub const amqp = @import("amqp.zig");

/// CDC processor targeting an AMQP 0.9.1 compliant server (e.g., RabbitMQ).
/// Producer: TigerBeetle `get_events` operation.
/// Consumer: AMQP publisher.
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
        const progress_tracker_queue = "tigerbeetle.internal.progress";
        const locker_queue = "tigerbeetle.internal.locker";
        const event_count_max: u32 = Client.StateMachine.operation_result_max(
            .get_events,
            vsr.constants.message_body_size_max,
        );
    };

    io: IO,
    idle_completion: IO.Completion = undefined,
    idle_interval_ns: u63,
    event_count_max: u32,

    message_pool: MessagePool,
    vsr_client: Client,
    buffer: DualBuffer,

    amqp_client: amqp.Client,
    publish_exchange: []const u8,
    publish_routing_key: []const u8,
    progress_tracker_queue: []const u8,
    locker_queue: []const u8,

    connected: struct {
        /// VSR client registered.
        vsr: bool = false,
        /// AMQP client connected and ready to publish.
        amqp: bool = false,
    },
    /// The producer is responsible for reading events from TigerBeetle.
    producer: enum {
        idle,
        request,
        /// No events to publish,
        /// waiting for the timeout to check for new events.
        waiting,
    },

    /// The consumer is responsible to publish events on the AMQP server.
    consumer: enum {
        idle,
        begin_transaction,
        publish,
        commit_transaction,
        progress_update,
    },

    metrics: Metrics,

    state: union(enum) {
        unknown: StateRecoveryMode,
        recovering: struct {
            timestamp_last: ?u64,
            phase: union(enum) {
                validate_exchange,
                declare_locker_queue,
                declare_progress_queue,
                get_progress_message,
                nack_progress_message: struct {
                    delivery_tag: u64,
                },
            },
        },
        last: struct {
            /// Last event read from TigerBeetle.
            producer_timestamp: u64,
            /// Last event published.
            consumer_timestamp: u64,
        },
    },

    pub fn init(
        self: *Runner,
        allocator: std.mem.Allocator,
        options: struct {
            /// TigerBeetle cluster ID.
            cluster_id: u128,
            /// TigerBeetle cluster addresses.
            addresses: []const std.net.Address,
            /// AMQP host address.
            host: std.net.Address,
            /// AMQP User name for PLAIN authentication.
            user: []const u8,
            /// AMQP Password for PLAIN authentication.
            password: []const u8,
            /// AMQP vhost.
            vhost: []const u8,
            /// AMQP exchange name for publishing messages.
            publish_exchange: ?[]const u8,
            /// AMQP routing key for publishing messages.
            publish_routing_key: ?[]const u8,
            /// Overrides the number max of events produced/consumed each time.
            event_count_max: ?u32,
            /// Overrides the number of milliseconds to query again if there's no new events to
            /// process.
            /// Must be greater than zero.
            idle_interval_ms: ?u32,
            /// Indicates whether to recover the last timestamp published on the state
            /// tracker queue, or override it with a user-defined value.
            recovery_mode: StateRecoveryMode,
        },
    ) !void {
        assert(options.addresses.len > 0);

        const idle_interval_ns: u63 = if (options.idle_interval_ms) |value|
            @intCast(@as(u64, value) * std.time.ns_per_ms)
        else
            constants.idle_interval_ns;
        assert(idle_interval_ns > 0);

        const event_count_max: u32 = if (options.event_count_max) |event_count_max|
            @min(event_count_max, constants.event_count_max)
        else
            constants.event_count_max;
        assert(event_count_max > 0);

        const publish_exchange: []const u8 = options.publish_exchange orelse "";
        const publish_routing_key: []const u8 = options.publish_routing_key orelse "";
        assert(publish_exchange.len > 0 or publish_routing_key.len > 0);

        const progress_tracker_queue_owned: []const u8 = try std.fmt.allocPrint(
            allocator,
            "{s}.{}",
            .{
                constants.progress_tracker_queue,
                options.cluster_id,
            },
        );
        errdefer allocator.free(progress_tracker_queue_owned);
        assert(progress_tracker_queue_owned.len <= 255);

        const locker_queue_owned: []const u8 = try std.fmt.allocPrint(
            allocator,
            "{s}.{}",
            .{
                constants.locker_queue,
                options.cluster_id,
            },
        );
        errdefer allocator.free(locker_queue_owned);
        assert(locker_queue_owned.len <= 255);

        const dual_buffer = try DualBuffer.init(allocator, event_count_max);
        errdefer self.buffer.deinit(allocator);

        self.* = .{
            .idle_interval_ns = idle_interval_ns,
            .event_count_max = event_count_max,
            .publish_exchange = publish_exchange,
            .publish_routing_key = publish_routing_key,
            .progress_tracker_queue = progress_tracker_queue_owned,
            .locker_queue = locker_queue_owned,
            .connected = .{},
            .io = undefined,
            .producer = .idle,
            .consumer = .idle,
            .metrics = undefined,
            .state = .{ .unknown = options.recovery_mode },
            .buffer = dual_buffer,
            .message_pool = undefined,
            .vsr_client = undefined,
            .amqp_client = undefined,
        };

        self.metrics = .{
            .producer = .{
                .timer = try std.time.Timer.start(),
            },
            .consumer = .{
                .timer = try std.time.Timer.start(),
            },
            .flush_ticks = 0,
            .flush_timeout_ticks = @divExact(30 * std.time.ms_per_s, constants.tick_ms),
        };

        self.io = try IO.init(32, 0);
        errdefer self.io.deinit();

        self.message_pool = try MessagePool.init(allocator, .client);
        errdefer self.message_pool.deinit(allocator);

        self.vsr_client = try Client.init(allocator, .{
            .id = stdx.unique_u128(),
            .cluster = options.cluster_id,
            .replica_count = @intCast(options.addresses.len),
            .time = .{},
            .message_pool = &self.message_pool,
            .message_bus_options = .{ .configuration = options.addresses, .io = &self.io },
        });
        errdefer self.vsr_client.deinit(allocator);

        self.amqp_client = try amqp.Client.init(allocator, .{
            .io = &self.io,
            .message_count_max = self.event_count_max,
            .message_body_size_max = Message.json_string_size_max,
            .reply_timeout_ticks = constants.reply_timeout_ticks,
        });
        errdefer self.amqp_client.deinit(allocator);

        // Starting both the VSR and the AMQP clients:

        try self.amqp_client.connect(
            &struct {
                fn callback(context: *amqp.Client) void {
                    const runner: *Runner = @alignCast(@fieldParentPtr("amqp_client", context));
                    assert(!runner.connected.amqp);
                    maybe(runner.connected.vsr);
                    log.info("AMQP connected.", .{});
                    runner.connected.amqp = true;
                    runner.recover();
                }
            }.callback,
            .{
                .host = options.host,
                .user_name = options.user,
                .password = options.password,
                .vhost = options.vhost,
            },
        );

        self.vsr_client.register(
            &struct {
                fn callback(
                    user_data: u128,
                    result: *const vsr.RegisterResult,
                ) void {
                    const runner: *Runner = @ptrFromInt(@as(usize, @intCast(user_data)));
                    assert(!runner.connected.vsr);
                    maybe(runner.connected.amqp);
                    log.info("VSR client registered.", .{});
                    runner.vsr_client.batch_size_limit = result.batch_size_limit;
                    runner.connected.vsr = true;
                    runner.start();
                }
            }.callback,
            @as(u128, @intCast(@intFromPtr(self))),
        );
    }

    pub fn deinit(self: *Runner, allocator: std.mem.Allocator) void {
        self.vsr_client.deinit(allocator);
        self.message_pool.deinit(allocator);
        self.amqp_client.deinit(allocator);
        self.buffer.deinit(allocator);
        allocator.free(self.progress_tracker_queue);
        allocator.free(self.locker_queue);
    }

    fn start(self: *Runner) void {
        assert(self.connected.vsr or self.connected.amqp);
        assert(self.producer == .idle);
        assert(self.consumer == .idle);

        if (self.connected.vsr and
            self.connected.amqp and
            self.state == .last)
        {
            log.info("Starting CDC.", .{});
            self.produce();
        }
    }

    /// To make the CDC stateless, internal queues are used to store the state:
    ///
    /// - Progress tracking queue:
    ///   A persistent queue with a maximum size of 1 message and "drop head" behavior on overflow.
    ///   During publishing, a message containing the last timestamp is pushed into this queue at
    ///   the end of each published batch.
    ///   On restart, the presence of a message indicates the `timestamp_min` from which to resume
    ///   processing events. Otherwise, processing starts from the beginning.
    ///   The queue name is generated to be unique based on the `cluster_id`.
    ///   The initial timestamp can be overridden via the command line.
    ///
    /// - Locker queue:
    ///   A temporary, exclusive queue used to ensure that only a single CDC process is publishing
    ///   at any given time. This queue is not used for publishing or consuming messages.
    ///   The queue name is generated to be unique based on the `cluster_id`.
    fn recover(self: *Runner) void {
        assert(self.connected.amqp);
        assert(self.state == .unknown);
        const recovery_mode = self.state.unknown;
        const timestamp_override: ?u64 = switch (recovery_mode) {
            .recover => null,
            .override => |timestamp| timestamp,
        };

        const is_default_exchange = self.publish_exchange.len == 0;
        self.state = .{
            .recovering = .{
                .timestamp_last = timestamp_override,
                .phase = if (is_default_exchange)
                    // No need to validate the default exchange, skipping `validate_exchange`.
                    .declare_locker_queue
                else
                    .validate_exchange,
            },
        };
        self.recover_dispatch();
    }

    fn recover_dispatch(self: *Runner) void {
        assert(self.connected.amqp);
        assert(self.state == .recovering);
        switch (self.state.recovering.phase) {
            // Check whether the exchange exists.
            // Declaring the exchange with `passive==true` only asserts if it already exists.
            .validate_exchange => {
                assert(self.publish_exchange.len > 0);
                maybe(self.state.recovering.timestamp_last == null);

                self.amqp_client.exchange_declare(
                    &struct {
                        fn callback(context: *amqp.Client) void {
                            const runner: *Runner = @alignCast(@fieldParentPtr(
                                "amqp_client",
                                context,
                            ));
                            assert(runner.state == .recovering);
                            const recovering = &runner.state.recovering;
                            assert(recovering.phase == .validate_exchange);
                            maybe(recovering.timestamp_last == null);

                            recovering.phase = .declare_locker_queue;
                            runner.recover_dispatch();
                        }
                    }.callback,
                    .{
                        .exchange = self.publish_exchange,
                        .type = "",
                        .passive = true,
                        .durable = false,
                        .internal = false,
                        .auto_delete = false,
                    },
                );
            },
            // Declaring the locker queue.
            // With `durable=false`, a temporary queue is created that exists only for the
            // duration of the current connection, and with `exclusive=true`, no other connection
            // can declare the same queue while this one is active.
            // This effectively acts as a distributed lock to prevent multiple CDC
            // instances from running simultaneously.
            .declare_locker_queue => {
                maybe(self.state.recovering.timestamp_last == null);

                self.amqp_client.queue_declare(
                    &struct {
                        fn callback(context: *amqp.Client) void {
                            const runner: *Runner = @alignCast(@fieldParentPtr(
                                "amqp_client",
                                context,
                            ));
                            switch (runner.state) {
                                .recovering => |*recovering| {
                                    assert(recovering.phase == .declare_locker_queue);
                                    maybe(recovering.timestamp_last == null);

                                    recovering.phase = .declare_progress_queue;
                                    runner.recover_dispatch();
                                },
                                else => unreachable,
                            }
                        }
                    }.callback,
                    .{
                        .queue = self.locker_queue,
                        .passive = false,
                        .durable = false,
                        .exclusive = true,
                        .auto_delete = true,
                        .arguments = .{
                            .overflow = .drop_head,
                            .max_length = 0,
                            .max_length_bytes = 0,
                            .single_active_consumer = true,
                        },
                    },
                );
            },
            // Declaring the progress tracking queue.
            // It's a no-op if the queue already exists.
            .declare_progress_queue => {
                maybe(self.state.recovering.timestamp_last == null);

                self.amqp_client.queue_declare(
                    &struct {
                        fn callback(context: *amqp.Client) void {
                            const runner: *Runner = @alignCast(@fieldParentPtr(
                                "amqp_client",
                                context,
                            ));
                            switch (runner.state) {
                                .recovering => |*recovering| {
                                    assert(recovering.phase == .declare_progress_queue);

                                    // Overriding the progress-tracking timestamp.
                                    if (recovering.timestamp_last) |timestamp_override| {
                                        runner.state = .{
                                            .last = .{
                                                .consumer_timestamp = timestamp_override,
                                                .producer_timestamp = timestamp_override + 1,
                                            },
                                        };
                                        return runner.start();
                                    }
                                    assert(recovering.timestamp_last == null);

                                    recovering.phase = .get_progress_message;
                                    runner.recover_dispatch();
                                },
                                else => unreachable,
                            }
                        }
                    }.callback,
                    .{
                        .queue = self.progress_tracker_queue,
                        .passive = false,
                        .durable = true,
                        .exclusive = false,
                        .auto_delete = false,
                        .arguments = .{
                            .overflow = .drop_head,
                            .max_length = 1,
                            .max_length_bytes = 0,
                            .single_active_consumer = true,
                        },
                    },
                );
            },
            // Getting the message header from the progress tracking queue.
            .get_progress_message => {
                assert(self.state.recovering.timestamp_last == null);
                self.amqp_client.get_message(
                    &struct {
                        fn callback(
                            context: *amqp.Client,
                            found: ?amqp.GetMessagePropertiesResult,
                        ) amqp.Decoder.Error!void {
                            const runner: *Runner = @alignCast(@fieldParentPtr(
                                "amqp_client",
                                context,
                            ));
                            switch (runner.state) {
                                .recovering => |*recovering| {
                                    assert(recovering.phase == .get_progress_message);
                                    assert(recovering.timestamp_last == null);

                                    if (found) |result| {
                                        // Since this queue is declared with `limit == 1`,
                                        // we don't expect more than one message.
                                        if (result.message_count > 0) fatal(
                                            "Unexpected message_count={} in the progress queue.",
                                            .{result.message_count},
                                        );
                                        assert(result.delivery_tag > 0);

                                        // Recovering from a valid timestamp is crucial,
                                        // otherwise `get_events` may return empty results
                                        // due to invalid filters.
                                        const progress_tracker = try ProgressTrackerMessage.parse(
                                            result.properties.headers,
                                        );
                                        assert(TimestampRange.valid(progress_tracker.timestamp));

                                        // Downgrading the CDC job is not allowed.
                                        if (vsr.constants.state_machine_config.release.value <
                                            progress_tracker.release.value)
                                        {
                                            fatal("The last event was published using a newer " ++
                                                "release (event={} current={}).", .{
                                                progress_tracker.release,
                                                vsr.constants.state_machine_config.release,
                                            });
                                        }

                                        recovering.timestamp_last = progress_tracker.timestamp;
                                        recovering.phase = .{
                                            .nack_progress_message = .{
                                                .delivery_tag = result.delivery_tag,
                                            },
                                        };

                                        return runner.recover_dispatch();
                                    }

                                    // No previous progress record found,
                                    // starting from the beginning.
                                    assert(found == null);
                                    runner.state = .{ .last = .{
                                        .consumer_timestamp = 0,
                                        .producer_timestamp = TimestampRange.timestamp_min,
                                    } };
                                    runner.start();
                                },
                                else => unreachable,
                            }
                        }
                    }.callback,
                    .{
                        .queue = self.progress_tracker_queue,
                        .no_ack = false,
                    },
                );
            },
            // Sending a `nack` with `requeue=true`, so the message remains in the progress
            // tracking queue in case we restart and need to recover again.
            .nack_progress_message => |message| {
                assert(self.state.recovering.timestamp_last != null);
                assert(TimestampRange.valid(self.state.recovering.timestamp_last.?));
                assert(message.delivery_tag > 0);

                self.amqp_client.nack(&struct {
                    fn callback(context: *amqp.Client) void {
                        const runner: *Runner = @alignCast(@fieldParentPtr(
                            "amqp_client",
                            context,
                        ));
                        switch (runner.state) {
                            .recovering => |*recovering| {
                                assert(recovering.phase == .nack_progress_message);
                                assert(recovering.timestamp_last != null);
                                assert(TimestampRange.valid(recovering.timestamp_last.?));

                                const nack = recovering.phase.nack_progress_message;
                                assert(nack.delivery_tag > 0);

                                runner.state = .{ .last = .{
                                    .consumer_timestamp = recovering.timestamp_last.?,
                                    .producer_timestamp = recovering.timestamp_last.? + 1,
                                } };
                                runner.start();
                            },
                            else => unreachable,
                        }
                    }
                }.callback, .{
                    .delivery_tag = message.delivery_tag,
                    .requeue = true,
                    .multiple = false,
                });
            },
        }
    }

    /// The "Producer" fetches events from TigerBeetle (`get_events` operation) into a buffer
    /// to be consumed by the "Consumer".
    fn produce(self: *Runner) void {
        assert(self.connected.vsr);
        assert(self.connected.amqp);
        assert(self.state == .last);
        assert(TimestampRange.valid(self.state.last.producer_timestamp));
        assert(self.state.last.consumer_timestamp == 0 or
            TimestampRange.valid(self.state.last.consumer_timestamp));
        assert(self.state.last.producer_timestamp > self.state.last.consumer_timestamp);
        switch (self.producer) {
            .idle => {
                if (!self.buffer.producer_begin()) {
                    // No free buffers.
                    // The running consumer will resume the producer once it finishes.
                    assert(self.consumer != .idle);
                    return;
                }
                self.producer = .request;
                self.metrics.producer.timer.reset();
                self.produce_dispatch();
            },
            else => unreachable, // Already running.
        }
    }

    fn produce_dispatch(self: *Runner) void {
        assert(self.connected.vsr);
        assert(self.connected.amqp);
        assert(self.state == .last);
        assert(TimestampRange.valid(self.state.last.producer_timestamp));
        assert(self.state.last.consumer_timestamp == 0 or
            TimestampRange.valid(self.state.last.consumer_timestamp));
        assert(self.state.last.producer_timestamp > self.state.last.consumer_timestamp);
        switch (self.producer) {
            .idle => unreachable,
            // Submitting the request through the VSR client.
            .request => {
                const filter: tb.EventFilter = .{
                    .limit = self.event_count_max,
                    .timestamp_min = self.state.last.producer_timestamp,
                    .timestamp_max = 0,
                };

                self.vsr_client.request(
                    &produce_request_callback,
                    @intFromPtr(self),
                    .get_events,
                    std.mem.asBytes(&filter),
                );
            },
            // No running consumer and no events returned from the last query,
            // waiting for the timeout to resume the producer.
            .waiting => {
                self.io.timeout(
                    *Runner,
                    self,
                    struct {
                        fn callback(
                            runner: *Runner,
                            completion: *IO.Completion,
                            result: IO.TimeoutError!void,
                        ) void {
                            result catch unreachable;
                            _ = completion;
                            assert(runner.buffer.all_free());
                            assert(runner.consumer == .idle);
                            assert(runner.producer == .waiting);

                            const producer_begin = runner.buffer.producer_begin();
                            assert(producer_begin);
                            runner.producer = .request;
                            runner.produce_dispatch();
                        }
                    }.callback,
                    &self.idle_completion,
                    self.idle_interval_ns,
                );
            },
        }
    }

    fn produce_request_callback(
        context: u128,
        operation: StateMachine.Operation,
        timestamp: u64,
        result: []u8,
    ) void {
        assert(operation == .get_events);
        assert(timestamp != 0);
        const runner: *Runner = @ptrFromInt(@as(usize, @intCast(context)));
        assert(runner.producer == .request);

        const source: []const tb.Event = stdx.bytes_as_slice(.exact, tb.Event, result);
        const target: []tb.Event = runner.buffer.get_producer_buffer();
        assert(source.len <= target.len);

        stdx.copy_disjoint(
            .inexact,
            tb.Event,
            target,
            source,
        );
        runner.buffer.producer_finish(@intCast(source.len));

        if (runner.buffer.all_free()) {
            // No events to publish.
            // Going idle and will check again for new events.
            assert(source.len == 0);
            assert(runner.consumer == .idle);
            runner.producer = .waiting;
            return runner.produce_dispatch();
        }

        runner.producer = .idle;
        runner.metrics.producer.record(source.len);
        assert(source.len > 0 or runner.consumer != .idle);
        if (source.len > 0) {
            const timestamp_next = source[source.len - 1].timestamp + 1;
            assert(TimestampRange.valid(timestamp_next));
            runner.state.last.producer_timestamp = timestamp_next;

            // Since the buffer was populated,
            // resume consuming (if not already running).
            if (runner.consumer == .idle) runner.consume();

            // Resume producing (if there's a free buffer).
            runner.produce();
        }
    }

    /// The "Consumer" reads from the buffer populated by the "Producer"
    /// and publishes the events to the AMQP server.
    fn consume(self: *Runner) void {
        assert(self.connected.vsr);
        assert(self.connected.amqp);
        assert(self.state == .last);
        assert(TimestampRange.valid(self.state.last.producer_timestamp));
        assert(self.state.last.consumer_timestamp == 0 or
            TimestampRange.valid(self.state.last.consumer_timestamp));
        assert(self.state.last.producer_timestamp > self.state.last.consumer_timestamp);
        switch (self.consumer) {
            .idle => {
                if (!self.buffer.consumer_begin()) {
                    // No buffers ready.
                    // The running producer will resume the consumer once it finishes.
                    assert(self.buffer.all_free() or self.producer != .idle);
                    return;
                }
                self.consumer = .begin_transaction;
                self.metrics.consumer.timer.reset();
                self.consume_dispatch();
            },
            else => unreachable, // Already running.
        }
    }

    fn consume_dispatch(self: *Runner) void {
        assert(self.connected.vsr);
        assert(self.connected.amqp);
        assert(self.state == .last);
        assert(TimestampRange.valid(self.state.last.producer_timestamp));
        assert(self.state.last.consumer_timestamp == 0 or
            TimestampRange.valid(self.state.last.consumer_timestamp));
        assert(self.state.last.producer_timestamp > self.state.last.consumer_timestamp);
        switch (self.consumer) {
            .idle => unreachable,
            // Starting a transaction with the AMQP server.
            // The entire batch of published messages must succeed atomically.
            .begin_transaction => {
                self.amqp_client.tx_select(&struct {
                    fn callback(context: *amqp.Client) void {
                        const runner: *Runner = @alignCast(@fieldParentPtr("amqp_client", context));
                        assert(runner.consumer == .begin_transaction);
                        runner.consumer = .publish;
                        runner.consume_dispatch();
                    }
                }.callback);
            },
            // Publishing the events.
            .publish => {
                const events: []const tb.Event = self.buffer.get_consumer_buffer();
                assert(events.len > 0);
                for (events) |*event| {
                    const message = Message.init(event);
                    self.amqp_client.publish_enqueue(.{
                        .exchange = self.publish_exchange,
                        .routing_key = self.publish_routing_key,
                        .mandatory = true,
                        .immediate = false,
                        .properties = .{
                            .content_type = Message.content_type,
                            .delivery_mode = .persistent,
                            .app_id = constants.app_id,
                            // AMQP timestamp in seconds.
                            .timestamp = @divTrunc(event.timestamp, std.time.ns_per_s),
                            .headers = message.header(),
                        },
                        .body = message.body(),
                    });
                }
                self.amqp_client.publish_send(&struct {
                    fn callback(context: *amqp.Client) void {
                        const runner: *Runner = @alignCast(@fieldParentPtr(
                            "amqp_client",
                            context,
                        ));
                        assert(runner.consumer == .publish);
                        runner.consumer = .commit_transaction;
                        runner.consume_dispatch();
                    }
                }.callback);
            },
            // Committing the transaction with the AMQP server.
            .commit_transaction => {
                self.amqp_client.tx_commit(&struct {
                    fn callback(context: *amqp.Client) void {
                        const runner: *Runner = @alignCast(@fieldParentPtr(
                            "amqp_client",
                            context,
                        ));
                        assert(runner.consumer == .commit_transaction);
                        runner.consumer = .progress_update;
                        runner.consume_dispatch();
                    }
                }.callback);
            },
            // Publishing the progress-tracking message containing the last timestamp.
            // N.B.: Message brokers cannot guarantee transaction atomicity across queues,
            // and may allow partially committed transactions.
            // Updating the status *after* confirming `tx_commit` leaves room for duplicated
            // messages, but guarantees *at least once* semantics.
            .progress_update => {
                const progress_tracker: ProgressTrackerMessage = progress: {
                    const events = self.buffer.get_consumer_buffer();
                    assert(events.len > 0);
                    break :progress .{
                        .timestamp = events[events.len - 1].timestamp,
                        .release = vsr.constants.state_machine_config.release,
                    };
                };
                self.amqp_client.publish_enqueue(.{
                    .exchange = "", // No exchange sends directly to this queue.
                    .routing_key = self.progress_tracker_queue,
                    .mandatory = true,
                    .immediate = false,
                    .properties = .{
                        .delivery_mode = .persistent,
                        .timestamp = @intCast(std.time.milliTimestamp()),
                        .headers = progress_tracker.header(),
                    },
                    .body = null,
                });
                self.amqp_client.publish_send(&struct {
                    fn callback(context: *amqp.Client) void {
                        const runner: *Runner = @alignCast(@fieldParentPtr(
                            "amqp_client",
                            context,
                        ));
                        assert(runner.consumer == .progress_update);

                        const event_count: usize, const timestamp_last: u64 = events: {
                            const events = runner.buffer.get_consumer_buffer();
                            assert(events.len > 0);
                            break :events .{ events.len, events[events.len - 1].timestamp };
                        };
                        runner.buffer.consumer_finish();
                        runner.state.last.consumer_timestamp = timestamp_last;

                        // Resume consuming (if there's a buffer ready).
                        runner.consumer = .idle;
                        runner.metrics.consumer.record(event_count);
                        runner.consume();

                        // Since the buffer was released,
                        // resume producing (if not already running).
                        if (runner.producer == .idle) runner.produce();
                    }
                }.callback);
            },
        }
    }

    pub fn tick(self: *Runner) void {
        assert(!self.vsr_client.evicted);
        self.vsr_client.tick();
        self.amqp_client.tick();
        self.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms) catch unreachable;

        self.metrics.tick();
    }
};

/// Inspired by the StateMachine metrics,
/// though the current method of shipping the metrics is a temporary solution.
const Metrics = struct {
    const TimingSummary = struct {
        timer: std.time.Timer,

        duration_min_ms: ?u64 = null,
        duration_max_ms: ?u64 = null,
        duration_sum_ms: u64 = 0,
        event_count: u64 = 0,
        count: u64 = 0,

        fn record(
            metrics: *TimingSummary,
            event_count: u64,
        ) void {
            const duration_ms: u64 = @divFloor(metrics.timer.read(), std.time.ns_per_ms);

            metrics.duration_min_ms =
                @min(duration_ms, metrics.duration_min_ms orelse std.math.maxInt(u64));
            metrics.duration_max_ms = @max(duration_ms, metrics.duration_max_ms orelse 0);
            metrics.duration_sum_ms += duration_ms;
            metrics.count += 1;
            metrics.event_count += event_count;
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
        const runner: *const Runner = @alignCast(@fieldParentPtr("metrics", metrics));
        inline for (comptime std.enums.values(Fields)) |field| {
            const summary: *TimingSummary = &@field(metrics, @tagName(field));
            if (summary.count > 0) {
                assert(runner.state == .last);
                const timestamp_last = switch (field) {
                    .consumer => runner.state.last.consumer_timestamp,
                    .producer => runner.state.last.producer_timestamp,
                };
                const event_rate = @divTrunc(
                    summary.event_count * std.time.ms_per_s,
                    summary.duration_sum_ms,
                );
                log.info("{s}: p0={?}ms mean={}ms p100={?}ms " ++
                    "event_count={} throughput={} op/s " ++
                    "last timestamp={} ({})", .{
                    @tagName(field),
                    summary.duration_min_ms,
                    @divFloor(summary.duration_sum_ms, summary.count),
                    summary.duration_max_ms,
                    summary.event_count,
                    event_rate,
                    timestamp_last,
                    stdx.DateTimeUTC.from_timestamp_ms(
                        timestamp_last / std.time.ns_per_ms,
                    ),
                });
            }
            summary.* = .{
                .timer = summary.timer,
            };
        }
    }
};

/// Buffers swapped between producer and consumer, allowing reading from TigerBeetle
/// and publishing to AMQP to happen concurrently.
const DualBuffer = struct {
    const State = enum {
        free,
        producing,
        ready,
        consuming,
    };

    const Buffer = struct {
        buffer: []tb.Event,
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
        assert(event_count > 0);
        assert(event_count <= Runner.constants.event_count_max);

        const buffer_1 = try allocator.alloc(tb.Event, event_count);
        errdefer allocator.free(buffer_1);

        const buffer_2 = try allocator.alloc(tb.Event, event_count);
        errdefer allocator.free(buffer_2);

        return .{
            .buffer_1 = .{
                .buffer = buffer_1,
                .state = .free,
            },
            .buffer_2 = .{
                .buffer = buffer_2,
                .state = .free,
            },
        };
    }

    pub fn deinit(self: *DualBuffer, allocator: std.mem.Allocator) void {
        allocator.free(self.buffer_2.buffer);
        allocator.free(self.buffer_1.buffer);
    }

    pub fn producer_begin(self: *DualBuffer) bool {
        self.assert_state();
        // Already producing.
        if (self.find(.producing) != null) return false;
        const buffer = self.find(.free) orelse
            // No free buffers.
            return false;
        buffer.state = .producing;
        return true;
    }

    pub fn get_producer_buffer(self: *DualBuffer) []tb.Event {
        self.assert_state();
        const buffer = self.find(.producing).?;
        return buffer.buffer;
    }

    pub fn producer_finish(self: *DualBuffer, count: u32) void {
        self.assert_state();
        const buffer = self.find(.producing).?;
        buffer.state = if (count == 0) .free else .{ .ready = count };
    }

    pub fn consumer_begin(self: *DualBuffer) bool {
        self.assert_state();
        // Already consuming.
        if (self.find(.consuming) != null) return false;
        const buffer = self.find(.ready) orelse
            // No buffers ready.
            return false;
        const count = buffer.state.ready;
        buffer.state = .{ .consuming = count };
        return true;
    }

    pub fn get_consumer_buffer(self: *DualBuffer) []const tb.Event {
        self.assert_state();
        const buffer = self.find(.consuming).?;
        return buffer.buffer[0..buffer.state.consuming];
    }

    pub fn consumer_finish(self: *DualBuffer) void {
        self.assert_state();
        const buffer = self.find(.consuming).?;
        buffer.state = .free;
    }

    pub fn all_free(self: *const DualBuffer) bool {
        return self.buffer_1.state == .free and
            self.buffer_2.state == .free;
    }

    fn find(self: *DualBuffer, state: State) ?*Buffer {
        self.assert_state();
        if (self.buffer_1.state == state) return &self.buffer_1;
        if (self.buffer_2.state == state) return &self.buffer_2;
        return null;
    }

    fn assert_state(self: *const DualBuffer) void {
        // Two buffers: one can be producing while the other is consuming,
        // but never two consumers or producers.
        assert(!(self.buffer_1.state == .producing and self.buffer_2.state == .producing));
        assert(!(self.buffer_1.state == .consuming and self.buffer_2.state == .consuming));
        assert(!(self.buffer_1.state == .ready and self.buffer_2.state == .ready));
        maybe(self.buffer_1.state == .free and self.buffer_2.state == .free);
    }
};

/// Progress tracker message with no body, containing the timestamp
/// and the release version of the last acknowledged publish.
const ProgressTrackerMessage = struct {
    release: vsr.Release,
    timestamp: u64,

    fn header(self: *const ProgressTrackerMessage) amqp.Encoder.Table {
        const vtable: amqp.Encoder.Table.VTable = comptime .{
            .write = &struct {
                fn write(context: *const anyopaque, encoder: *amqp.Encoder.TableEncoder) void {
                    const message: *const ProgressTrackerMessage = @ptrCast(@alignCast(context));
                    var release_buffer: [
                        std.fmt.count("{}", vsr.Release.from(.{
                            .major = std.math.maxInt(u16),
                            .minor = std.math.maxInt(u8),
                            .patch = std.math.maxInt(u8),
                        }))
                    ]u8 = undefined;
                    encoder.put("release", .{ .string = std.fmt.bufPrint(
                        &release_buffer,
                        "{}",
                        .{message.release},
                    ) catch unreachable });
                    encoder.put("timestamp", .{ .long_long_uint = message.timestamp });
                }
            }.write,
        };
        return .{ .context = self, .vtable = &vtable };
    }

    fn parse(table: ?amqp.Decoder.Table) amqp.Decoder.Error!ProgressTrackerMessage {
        if (table) |headers| {
            var timestamp: ?u64 = null;
            var release: ?vsr.Release = null;

            // Intentionally allows the presence of header fields other than `timestamp`,
            // since some plugin may insert additional headers into messages.
            var iterator = headers.iterator();
            while (try iterator.next()) |entry| {
                if (std.mem.eql(u8, entry.key, "timestamp")) {
                    switch (entry.value) {
                        .long_long_uint => |value| {
                            if (!TimestampRange.valid(value)) break;
                            timestamp = value;
                        },
                        else => break,
                    }
                }
                if (std.mem.eql(u8, entry.key, "release")) {
                    switch (entry.value) {
                        .string => |value| {
                            release = vsr.Release.parse(value) catch break;
                        },
                        else => break,
                    }
                }

                if (timestamp != null and release != null) return .{
                    .timestamp = timestamp.?,
                    .release = release.?,
                };
            }
        }
        fatal(
            \\Invalid progress tracker message.
            \\Use `--timestamp-last` to restore a valid initial timestamp.
        , .{});
    }
};

/// Message with the body in the JSON schema.
const Message = struct {
    pub const content_type = "application/json";

    pub const json_string_size_max = size: {
        var counting_writer = std.io.countingWriter(std.io.null_writer);
        std.json.stringify(
            worse_case(Message),
            stringify_options,
            counting_writer.writer(),
        ) catch unreachable;
        break :size counting_writer.bytes_written;
    };

    const stringify_options = std.json.StringifyOptions{
        .whitespace = .minified,
        .emit_nonportable_numbers_as_strings = true,
    };

    timestamp: u64,
    type: tb.EventType,
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

    fn init(event: *const tb.Event) Message {
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

    fn header(self: *const Message) amqp.Encoder.Table {
        const vtable: amqp.Encoder.Table.VTable = comptime .{
            .write = &struct {
                fn write(context: *const anyopaque, encoder: *amqp.Encoder.TableEncoder) void {
                    const message: *const Message = @ptrCast(@alignCast(context));
                    encoder.put("event_type", .{ .string = @tagName(message.type) });
                    encoder.put("ledger", .{ .long_uint = message.ledger });
                    encoder.put("transfer_code", .{ .short_uint = message.transfer.code });
                    encoder.put("debit_account_code", .{
                        .short_uint = message.debit_account.code,
                    });
                    encoder.put("credit_account_code", .{
                        .short_uint = message.credit_account.code,
                    });
                }
            }.write,
        };
        return .{ .context = self, .vtable = &vtable };
    }

    fn body(self: *const Message) amqp.Encoder.Body {
        const vtable: amqp.Encoder.Body.VTable = comptime .{
            .write = &struct {
                fn write(context: *const anyopaque, buffer: []u8) usize {
                    const message: *const Message = @ptrCast(@alignCast(context));
                    var fbs = std.io.fixedBufferStream(buffer);
                    std.json.stringify(message, .{
                        .whitespace = .minified,
                        .emit_nonportable_numbers_as_strings = true,
                    }, fbs.writer()) catch unreachable;
                    return fbs.pos;
                }
            }.write,
        };
        return .{ .context = self, .vtable = &vtable };
    }

    /// Fill all fields for the largest string representation.
    fn worse_case(comptime T: type) T {
        var value: T = undefined;
        for (std.meta.fields(T)) |field| {
            @field(value, field.name) = switch (@typeInfo(field.type)) {
                .Int => std.math.maxInt(field.type),
                .Enum => max: {
                    var name: []const u8 = "";
                    for (std.enums.values(tb.EventType)) |tag| {
                        if (@tagName(tag).len > name.len) {
                            name = @tagName(tag);
                        }
                    }
                    break :max @field(field.type, name);
                },
                .Struct => worse_case(field.type),
                else => unreachable,
            };
        }
        return value;
    }
};

const testing = std.testing;

test "amqp: DualBuffer" {
    const event_count_max = Runner.constants.event_count_max;

    var prng = stdx.PRNG.from_seed(42);
    var dual_buffer = try DualBuffer.init(testing.allocator, event_count_max);
    defer dual_buffer.deinit(testing.allocator);

    for (0..4096) |_| {
        try testing.expect(dual_buffer.all_free());

        // Starts a producer:
        const producer_begin = dual_buffer.producer_begin();
        try testing.expect(producer_begin);
        try testing.expect(!dual_buffer.all_free());
        // We can't consume yet.
        try testing.expect(!dual_buffer.consumer_begin());

        const producer1_buffer = dual_buffer.get_producer_buffer();
        try testing.expectEqual(@as(usize, event_count_max), producer1_buffer.len);

        const producer1_count = prng.range_inclusive(u32, 1, event_count_max);
        prng.fill(std.mem.asBytes(producer1_buffer[0..producer1_count]));
        dual_buffer.producer_finish(producer1_count);

        // Starts a consumer after the producer has finished:
        const consumer_begin = dual_buffer.consumer_begin();
        try testing.expect(consumer_begin);
        try testing.expect(!dual_buffer.all_free());

        // Concurrently starts another producer:
        const producer_begin_concurrently = dual_buffer.producer_begin();
        try testing.expect(producer_begin_concurrently);
        try testing.expect(!dual_buffer.all_free());

        const producer2_buffer = dual_buffer.get_producer_buffer();
        try testing.expectEqual(@as(usize, event_count_max), producer2_buffer.len);

        const producer2_count = prng.range_inclusive(u32, 0, event_count_max);
        maybe(producer2_count == 0); // Testing zeroed producers.
        prng.fill(std.mem.asBytes(producer2_buffer[0..producer2_count]));
        dual_buffer.producer_finish(producer2_count);

        // Consuming the first producer:
        const consumer_buffer = dual_buffer.get_consumer_buffer();
        try testing.expectEqual(producer1_buffer.ptr, consumer_buffer.ptr);
        try testing.expectEqual(@as(usize, producer1_count), consumer_buffer.len);
        try testing.expectEqualSlices(
            u8,
            std.mem.sliceAsBytes(producer1_buffer[0..producer1_count]),
            std.mem.sliceAsBytes(consumer_buffer),
        );
        dual_buffer.consumer_finish();

        // Consuming the second producer.
        // It might not have produced anything, so the buffer cannot be consumed:
        const consumer_begin_again = dual_buffer.consumer_begin();
        if (producer2_count == 0) {
            try testing.expect(!consumer_begin_again);
            try testing.expect(dual_buffer.all_free());
            continue;
        }

        try testing.expect(consumer_begin_again);
        try testing.expect(!dual_buffer.all_free());

        const consumer2_buffer = dual_buffer.get_consumer_buffer();
        try testing.expectEqual(producer2_buffer.ptr, consumer2_buffer.ptr);
        try testing.expectEqual(@as(usize, producer2_count), consumer2_buffer.len);
        try testing.expectEqualSlices(
            u8,
            std.mem.sliceAsBytes(producer2_buffer[0..producer2_count]),
            std.mem.sliceAsBytes(consumer2_buffer),
        );

        dual_buffer.consumer_finish();
        try testing.expect(dual_buffer.all_free());
    }
}

test "amqp: ProgressTrackerMessage" {
    const buffer = try testing.allocator.alloc(u8, amqp.frame_min_size);
    defer testing.allocator.free(buffer);

    const values: []const u64 = &.{
        TimestampRange.timestamp_min,
        1745055501942402250,
        TimestampRange.timestamp_max,
    };
    for (values) |value| {
        const message: ProgressTrackerMessage = .{
            .release = vsr.Release.minimum,
            .timestamp = value,
        };
        var encoder = amqp.Encoder.init(buffer);
        encoder.write_table(message.header());

        var decoder = amqp.Decoder.init(buffer[0..encoder.index]);
        const decoded_message = try ProgressTrackerMessage.parse(try decoder.read_table());
        try testing.expectEqual(message.release.value, decoded_message.release.value);
        try testing.expectEqual(message.timestamp, decoded_message.timestamp);
    }
}

test "amqp: JSON message" {
    const Snap = @import("../testing/snaptest.zig").Snap;
    const snap = Snap.snap;

    const buffer = try testing.allocator.alloc(u8, Message.json_string_size_max);
    defer testing.allocator.free(buffer);

    {
        const message: Message = std.mem.zeroInit(Message, .{});
        const size = message.body().write(buffer);
        try testing.expectEqual(@as(usize, 564), size);

        try snap(@src(),
            \\{"timestamp":0,"type":"single_phase","ledger":0,"transfer":{"id":0,"amount":0,"pending_id":0,"user_data_128":0,"user_data_64":0,"user_data_32":0,"timeout":0,"code":0,"flags":0,"timestamp":0},"debit_account":{"id":0,"debits_pending":0,"debits_posted":0,"credits_pending":0,"credits_posted":0,"user_data_128":0,"user_data_64":0,"user_data_32":0,"code":0,"flags":0,"timestamp":0},"credit_account":{"id":0,"debits_pending":0,"debits_posted":0,"credits_pending":0,"credits_posted":0,"user_data_128":0,"user_data_64":0,"user_data_32":0,"code":0,"flags":0,"timestamp":0}}
        ).diff(buffer[0..size]);
    }

    {
        const message = comptime Message.worse_case(Message);
        const size = message.body().write(buffer);
        try testing.expectEqual(@as(usize, 1425), size);
        try testing.expectEqual(size, buffer.len);

        try snap(@src(),
            \\{"timestamp":"18446744073709551615","type":"two_phase_pending","ledger":4294967295,"transfer":{"id":"340282366920938463463374607431768211455","amount":"340282366920938463463374607431768211455","pending_id":"340282366920938463463374607431768211455","user_data_128":"340282366920938463463374607431768211455","user_data_64":"18446744073709551615","user_data_32":4294967295,"timeout":4294967295,"code":65535,"flags":65535,"timestamp":"18446744073709551615"},"debit_account":{"id":"340282366920938463463374607431768211455","debits_pending":"340282366920938463463374607431768211455","debits_posted":"340282366920938463463374607431768211455","credits_pending":"340282366920938463463374607431768211455","credits_posted":"340282366920938463463374607431768211455","user_data_128":"340282366920938463463374607431768211455","user_data_64":"18446744073709551615","user_data_32":4294967295,"code":65535,"flags":65535,"timestamp":"18446744073709551615"},"credit_account":{"id":"340282366920938463463374607431768211455","debits_pending":"340282366920938463463374607431768211455","debits_posted":"340282366920938463463374607431768211455","credits_pending":"340282366920938463463374607431768211455","credits_posted":"340282366920938463463374607431768211455","user_data_128":"340282366920938463463374607431768211455","user_data_64":"18446744073709551615","user_data_32":4294967295,"code":65535,"flags":65535,"timestamp":"18446744073709551615"}}
        ).diff(buffer);
    }
}
