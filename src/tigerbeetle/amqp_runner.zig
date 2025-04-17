const std = @import("std");
const log = std.log.scoped(.cdc_amqp);

const vsr = @import("vsr");
const assert = std.debug.assert;
const maybe = vsr.stdx.maybe;

const constants = vsr.constants;
const stdx = vsr.stdx;
const IO = vsr.io.IO;
const Tracer = vsr.trace.TracerType(vsr.time.Time);
const Storage = vsr.storage.StorageType(IO, Tracer);
const MessagePool = vsr.message_pool.MessagePool;
const MessageBus = vsr.message_bus.MessageBusClient;
const StateMachine = vsr.state_machine.StateMachineType(Storage, constants.state_machine_config);
const Client = vsr.ClientType(StateMachine, MessageBus, vsr.time.Time);
const TimestampRange = vsr.lsm.TimestampRange;
const tb = vsr.tigerbeetle;

const amqp = @import("../cdc/amqp.zig");

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

    const defaults = struct {
        const idle_check_ns: u63 = 5 * std.time.ns_per_s;
        const amqp_app_id = "tigerbeetle";
        const amqp_progress_tracker_queue = "tigerbeetle.internal.progress";
        const amqp_locker_queue = "tigerbeetle.internal.locker";
        const event_count_max: u32 = Client.StateMachine.operation_result_max(
            .get_events,
            constants.message_body_size_max,
        );
    };

    io: *IO,
    idle_completion: IO.Completion = undefined,
    idle_check_ns: u63,
    event_count_max: u32,

    message_pool: MessagePool,
    vsr_client: Client,
    buffer: DualBuffer,

    amqp_client: amqp.Client,
    amqp_publish_exchange: []const u8,
    amqp_publish_routing_key: []const u8,
    amqp_progress_tracker_queue: []const u8,
    amqp_locker_queue: []const u8,

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
                    amqp_delivery_tag: u64,
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
        io: *IO,
        options: struct {
            tb_cluster_id: u128,
            tb_addresses: []const std.net.Address,
            amqp_address: std.net.Address,
            /// AMQP User name for PLAIN authentication.
            /// This reference should be valid until the callback is invoked.
            amqp_user: []const u8,
            /// AMQP Password for PLAIN authentication.
            /// This reference should be valid until the callback is invoked.
            amqp_password: []const u8,
            /// AMQP vhost.
            /// This reference should be valid until the callback is invoked.
            amqp_vhost: []const u8,
            /// AMQP exchange name for publishing messages.
            /// This reference should be valid for the life-time of the `Runner`.
            amqp_publish_exchange: ?[]const u8,
            /// AMQP routing key for publishing messages.
            /// This reference should be valid for the life-time of the `Runner`.
            amqp_publish_routing_key: ?[]const u8,
            /// Overrides the AMQP queue name declared by the client as progress tracker.
            /// This reference should be valid for the life-time of the `Runner`.
            amqp_progress_tracker_queue: ?[]const u8,
            /// Overrides the number max of events produced/consumed each time.
            event_count_max: ?u32,
            /// Overrides the number of seconds to query again if there's no new events to process.
            /// Must be greater than zero.
            idle_check_seconds: ?u32,
            /// Indicates whether to recover the last timestamp published on the state
            /// tracker queue, or override it with a user-defined value.
            recovery_mode: StateRecoveryMode,
        },
    ) !void {
        const idle_check_ns: u63 = interval: {
            if (options.idle_check_seconds) |value| {
                assert(value > 0);
                break :interval @intCast(@as(u64, value) * std.time.ns_per_s);
            }

            break :interval defaults.idle_check_ns;
        };

        const event_count_max: u32 = count: {
            if (options.event_count_max) |event_count_max| {
                assert(event_count_max > 0);
                break :count @min(event_count_max, defaults.event_count_max);
            }

            break :count defaults.event_count_max;
        };

        const amqp_publish_exchange: []const u8 = name: {
            if (options.amqp_publish_exchange) |exchange_name| {
                assert(exchange_name.len > 0);
                break :name try allocator.dupe(u8, exchange_name);
            }
            break :name try allocator.dupe(u8, "");
        };
        errdefer allocator.free(amqp_publish_exchange);

        const amqp_publish_routing_key: []const u8 = key: {
            if (options.amqp_publish_routing_key) |routing_key| {
                assert(routing_key.len > 0);
                break :key try allocator.dupe(u8, routing_key);
            }
            break :key try allocator.dupe(u8, "");
        };
        errdefer allocator.free(amqp_publish_routing_key);

        const amqp_progress_tracker_queue: []const u8 = name: {
            if (options.amqp_progress_tracker_queue) |queue_name| {
                assert(queue_name.len > 0);
                break :name try allocator.dupe(u8, queue_name);
            }

            break :name try allocator.dupe(u8, defaults.amqp_progress_tracker_queue);
        };
        errdefer allocator.free(amqp_progress_tracker_queue);

        const amqp_locker_queue: []const u8 = try std.fmt.allocPrint(
            allocator,
            "{s}.{s}.{s}",
            .{
                defaults.amqp_locker_queue,
                options.amqp_publish_exchange orelse "default",
                options.amqp_publish_routing_key orelse "default",
            },
        );
        errdefer allocator.free(amqp_locker_queue);

        const dual_buffer = try DualBuffer.init(allocator, event_count_max);
        errdefer self.buffer.deinit(allocator);

        self.* = .{
            .io = io,
            .idle_check_ns = idle_check_ns,
            .event_count_max = event_count_max,
            .amqp_publish_exchange = amqp_publish_exchange,
            .amqp_publish_routing_key = amqp_publish_routing_key,
            .amqp_progress_tracker_queue = amqp_progress_tracker_queue,
            .amqp_locker_queue = amqp_locker_queue,
            .connected = .{},
            .producer = .idle,
            .consumer = .idle,
            .state = .{ .unknown = options.recovery_mode },
            .buffer = dual_buffer,
            .message_pool = undefined,
            .vsr_client = undefined,
            .amqp_client = undefined,
        };

        self.message_pool = try MessagePool.init(allocator, .client);
        errdefer self.message_pool.deinit(allocator);

        self.vsr_client = try Client.init(allocator, .{
            .id = stdx.unique_u128(),
            .cluster = options.tb_cluster_id,
            .replica_count = @intCast(options.tb_addresses.len),
            .time = .{},
            .message_pool = &self.message_pool,
            .message_bus_options = .{ .configuration = options.tb_addresses, .io = io },
        });
        errdefer self.vsr_client.deinit(allocator);

        self.amqp_client = try amqp.Client.init(allocator, .{
            .io = io,
            .message_count_max = self.event_count_max,
            .message_body_size_max = Message.json_string_size_max,
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
                .address = options.amqp_address,
                .user_name = options.amqp_user,
                .password = options.amqp_password,
                .vhost = options.amqp_vhost,
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
        allocator.free(self.amqp_publish_exchange);
        allocator.free(self.amqp_publish_routing_key);
        allocator.free(self.amqp_progress_tracker_queue);
        allocator.free(self.amqp_locker_queue);
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
    ///   The queue name can be overridden via the command line.
    ///   The initial timestamp can also be overridden via the command line.
    ///
    /// - Locker queue:
    ///   A temporary, exclusive queue used to ensure that only a single CDC process is publishing
    ///   at any given time. This queue is not used for publishing or consuming messages.
    ///   The queue name is generated to be unique based on the `exchange` and `routing_key`.
    fn recover(self: *Runner) void {
        assert(self.connected.amqp);
        switch (self.state) {
            .unknown => |recovery_mode| {
                const timestamp_override: ?u64 = switch (recovery_mode) {
                    .recover => null,
                    .override => |timestamp| timestamp,
                };

                const is_default_exchange = self.amqp_publish_exchange.len == 0;
                if (is_default_exchange) {
                    // No need to validate the default exchange, skipping `validate_exchange`.
                    self.state = .{
                        .recovering = .{
                            .timestamp_last = timestamp_override,
                            .phase = .declare_locker_queue,
                        },
                    };
                } else {
                    self.state = .{
                        .recovering = .{
                            .timestamp_last = timestamp_override,
                            .phase = .validate_exchange,
                        },
                    };
                }
                self.recover_dispatch();
            },
            else => unreachable,
        }
    }

    fn recover_dispatch(self: *Runner) void {
        assert(self.connected.amqp);
        assert(self.state == .recovering);
        switch (self.state.recovering.phase) {
            // Check whether the exchange exists.
            // Declaring the exchange with `passive==true` only asserts if it already exists.
            .validate_exchange => {
                assert(self.amqp_publish_exchange.len > 0);
                maybe(self.state.recovering.timestamp_last == null);

                self.amqp_client.exchange_declare(
                    &struct {
                        fn callback(context: *amqp.Client) void {
                            const runner: *Runner = @alignCast(@fieldParentPtr(
                                "amqp_client",
                                context,
                            ));
                            switch (runner.state) {
                                .recovering => |*recovering| {
                                    assert(recovering.phase == .validate_exchange);
                                    maybe(recovering.timestamp_last == null);

                                    recovering.phase = .declare_locker_queue;
                                    runner.recover_dispatch();
                                },
                                else => unreachable,
                            }
                        }
                    }.callback,
                    .{
                        .exchange = self.amqp_publish_exchange,
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
                        .queue = self.amqp_locker_queue,
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
                        .queue = self.amqp_progress_tracker_queue,
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
                            delivery_tag: u64,
                            properties: ?amqp.Decoder.BasicProperties,
                        ) amqp.Decoder.Error!void {
                            const runner: *Runner = @alignCast(@fieldParentPtr(
                                "amqp_client",
                                context,
                            ));
                            switch (runner.state) {
                                .recovering => |*recovering| {
                                    assert(recovering.phase == .get_progress_message);
                                    assert(recovering.timestamp_last == null);

                                    if (properties == null) {
                                        // No previous progress record found,
                                        // starting from the beginning.
                                        assert(delivery_tag == 0);
                                        runner.state = .{ .last = .{
                                            .consumer_timestamp = 0,
                                            .producer_timestamp = TimestampRange.timestamp_min,
                                        } };
                                        return runner.start();
                                    }
                                    assert(delivery_tag > 0);

                                    // Recovering from a valid timestamp is crucial,
                                    // otherwise `get_events` may return empty results
                                    // due to invalid filters.
                                    const progress_tracker = try ProgressTrackerMessage.parse(
                                        properties.?,
                                    );
                                    assert(TimestampRange.valid(progress_tracker.timestamp));

                                    recovering.timestamp_last = progress_tracker.timestamp;
                                    recovering.phase = .{
                                        .nack_progress_message = .{
                                            .amqp_delivery_tag = delivery_tag,
                                        },
                                    };
                                    runner.recover_dispatch();
                                },
                                else => unreachable,
                            }
                        }
                    }.callback,
                    .{
                        .queue = self.amqp_progress_tracker_queue,
                        .no_ack = false,
                    },
                );
            },
            // Sending a `nack` with `requeue=true`, so the message remains in the progress
            // tracking queue in case we restart and need to recover again.
            .nack_progress_message => |message| {
                assert(self.state.recovering.timestamp_last != null);
                assert(TimestampRange.valid(self.state.recovering.timestamp_last.?));
                assert(message.amqp_delivery_tag > 0);

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
                                assert(nack.amqp_delivery_tag > 0);

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
                    .delivery_tag = message.amqp_delivery_tag,
                    .requeue = true,
                    .multiple = false,
                });
            },
        }
    }

    /// The "Producer" fetches events from TigerBeetle (`get_events` operation) into a buffer
    /// to be consumed by the "Consumer".
    fn produce(self: *Runner) void {
        assert(self.connected.vsr and self.connected.amqp);
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
                self.produce_dispatch();
            },
            else => unreachable, // Already running.
        }
    }

    fn produce_dispatch(self: *Runner) void {
        assert(self.connected.vsr and self.connected.amqp);
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
                    @as(u128, @intFromPtr(self)),
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

                            const producer_begin = runner.buffer.producer_begin();
                            assert(producer_begin);
                            runner.producer = .request;
                            runner.produce_dispatch();
                        }
                    }.callback,
                    &self.idle_completion,
                    self.idle_check_ns,
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
        const source: []const tb.Event = stdx.bytes_as_slice(.exact, tb.Event, result);
        const target: []tb.Event = runner.buffer.get_producer_buffer();
        assert(source.len <= target.len);

        stdx.copy_left(
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
        assert(self.connected.vsr and self.connected.amqp);
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
                    assert(self.producer != .idle);
                    return;
                }
                self.consumer = .begin_transaction;
                self.consume_dispatch();
            },
            else => unreachable, // Already running.
        }
    }

    fn consume_dispatch(self: *Runner) void {
        assert(self.connected.vsr and self.connected.amqp);
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
                        .exchange = self.amqp_publish_exchange,
                        .routing_key = self.amqp_publish_routing_key,
                        .mandatory = true,
                        .immediate = false,
                        .properties = .{
                            .content_type = Message.content_type,
                            .delivery_mode = .persistent,
                            .app_id = defaults.amqp_app_id,
                            // AMQP timestamp in seconds.
                            .timestamp = event.timestamp / std.time.ns_per_s,
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
                const progress_tracker: ProgressTrackerMessage = .{
                    .timestamp = self.buffer.get_consumer_timestamp_last(),
                };
                self.amqp_client.publish_enqueue(.{
                    .exchange = "", // No exchange sends directly to this queue.
                    .routing_key = self.amqp_progress_tracker_queue,
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

                        const timestamp_last = runner.buffer.get_consumer_timestamp_last();
                        runner.buffer.consumer_finish();
                        runner.state.last.consumer_timestamp = timestamp_last;

                        // Since the buffer was released,
                        // resume producing (if not already running).
                        if (runner.producer == .idle) runner.produce();

                        // Resume consuming (if there's a buffer ready).
                        runner.consumer = .idle;
                        runner.consume();
                    }
                }.callback);
            },
        }
    }

    pub fn tick(self: *Runner) void {
        if (!self.vsr_client.evicted) {
            self.vsr_client.tick();
        }
    }
};

/// Buffers swapped between producer and consumer, allowing reading from TigerBeetle
/// and publishing to AMQP to happen concurrently.
const DualBuffer = struct {
    const Buffer = struct {
        buffer: []tb.Event,
        state: union(enum) {
            free,
            producing,
            ready: u32,
            consuming: u32,
        } = .free,
    };

    buffer_1: Buffer,
    buffer_2: Buffer,

    pub fn init(allocator: std.mem.Allocator, event_count: u32) !DualBuffer {
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
        allocator.free(self.buffer_1.buffer);
        allocator.free(self.buffer_2.buffer);
    }

    pub fn producer_begin(self: *DualBuffer) bool {
        self.assert_state();
        // Already producing.
        if (self.buffer_1.state == .producing or
            self.buffer_2.state == .producing) return false;
        for (self.buffers()) |buffer| {
            if (buffer.state == .free) {
                buffer.state = .producing;
                return true;
            }
        }
        // No free buffers.
        return false;
    }

    pub fn get_producer_buffer(self: *DualBuffer) []tb.Event {
        self.assert_state();
        for (self.buffers()) |buffer| {
            if (buffer.state == .producing) {
                return buffer.buffer;
            }
        } else unreachable;
    }

    pub fn producer_finish(self: *DualBuffer, count: u32) void {
        self.assert_state();
        for (self.buffers()) |buffer| {
            if (buffer.state == .producing) {
                buffer.state = if (count == 0) .free else .{ .ready = count };
                return;
            }
        } else unreachable;
    }

    pub fn consumer_begin(self: *DualBuffer) bool {
        self.assert_state();
        // Already consuming.
        if (self.buffer_1.state == .consuming or
            self.buffer_2.state == .consuming) return false;
        for (self.buffers()) |buffer| {
            if (buffer.state == .ready) {
                const count = buffer.state.ready;
                buffer.state = .{ .consuming = count };
                return true;
            }
        }
        // No buffers ready.
        return false;
    }

    pub fn get_consumer_buffer(self: *DualBuffer) []const tb.Event {
        self.assert_state();
        for (self.buffers()) |buffer| {
            if (buffer.state == .consuming) {
                return buffer.buffer[0..buffer.state.consuming];
            }
        } else unreachable;
    }

    pub fn get_consumer_timestamp_last(self: *DualBuffer) u64 {
        const events: []const tb.Event = self.get_consumer_buffer();
        assert(events.len > 0);
        return events[events.len - 1].timestamp;
    }

    pub fn consumer_finish(self: *DualBuffer) void {
        self.assert_state();
        for (self.buffers()) |buffer| {
            if (buffer.state == .consuming) {
                buffer.state = .free;
                return;
            }
        } else unreachable;
    }

    pub fn all_free(self: *const DualBuffer) bool {
        return self.buffer_1.state == .free and
            self.buffer_2.state == .free;
    }

    inline fn buffers(self: *DualBuffer) *const [2]*Buffer {
        return &.{ &self.buffer_1, &self.buffer_2 };
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

/// Minimal state tracker message with no body,
/// containing only the last timestamp in the header.
const ProgressTrackerMessage = struct {
    timestamp: u64,

    fn header(self: *const ProgressTrackerMessage) amqp.Encoder.Table {
        const vtable: amqp.Encoder.Table.VTable = comptime .{
            .write = &struct {
                fn write(context: *const anyopaque, encoder: *amqp.Encoder.TableEncoder) void {
                    const message: *const ProgressTrackerMessage = @ptrCast(@alignCast(context));
                    encoder.put("timestamp", .{ .long_long_uint = message.timestamp });
                }
            }.write,
        };
        return .{ .context = self, .vtable = &vtable };
    }

    fn parse(properties: amqp.Decoder.BasicProperties) amqp.Decoder.Error!ProgressTrackerMessage {
        if (properties.headers) |headers| {
            // Intentionally allows the presence of header fields other than `timestamp`,
            // since some middleware may insert additional headers into messages.
            var iterator = headers.iterator();
            while (try iterator.next()) |entry| {
                if (std.mem.eql(u8, entry.key, "timestamp")) {
                    switch (entry.value) {
                        .long_long_uint => |value| return .{
                            .timestamp = value,
                        },
                        else => break,
                    }
                }
            }
        }
        @panic(
            \\ Invalid progress tracker message.
            \\Use `--timestamp-last` to restore a valid initial timestamp.
        );
    }
};

/// Message with the body in the JSON schema.
const Message = struct {
    pub const content_type = "application/json";

    pub const json_string_size_max = size: {
        var counting_writer = std.io.countingWriter(std.io.null_writer);
        std.json.stringify(
            wrost_case(Message),
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
    fn wrost_case(comptime T: type) T {
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
                .Struct => wrost_case(field.type),
                else => unreachable,
            };
        }
        return value;
    }
};
