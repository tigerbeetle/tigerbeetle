const std = @import("std");
const builtin = @import("builtin");
const log = std.log.scoped(.cdc_amqp);

const vsr = @import("vsr");
const maybe = vsr.stdx.maybe;
const assert = std.debug.assert;
const panic = std.debug.panic;

const constants = vsr.constants;
const stdx = vsr.stdx;
const IO = vsr.io.IO;
const Tracer = vsr.trace.TracerType(vsr.time.Time);
const Storage = vsr.storage.StorageType(IO, Tracer);
const MessagePool = vsr.message_pool.MessagePool;
const MessageBus = vsr.message_bus.MessageBusClient;
const StateMachine = vsr.state_machine.StateMachineType(Storage, constants.state_machine_config);
const Client = vsr.ClientType(StateMachine, MessageBus, vsr.time.Time);
const tb = vsr.tigerbeetle;

const amqp = @import("../cdc/amqp/amqp.zig");

/// CDC processor targeting an AMQP 0.9.1 compliant server (e.g., RabbitMQ).
/// Producer: TigerBeetle `get_event` operation.
/// Consumer: AMQP publisher.
/// Both consumer and producer run concurrently using `io_uring`.
/// See `DualBuffer` for more details.
pub const Runner = struct {
    io: *IO,
    idle_completion: IO.Completion = undefined,
    idle_check_seconds: u32,

    message_pool: MessagePool,
    vsr_client: Client,
    amqp_client: amqp.Client,
    amqp_publish_exchange_name: []const u8,
    amqp_queue_state_tracker_name: []const u8,

    buffer: DualBuffer,
    state: struct {
        /// VSR client registered.
        vsr_connected: bool = false,
        /// AMQP client connected and ready to publish.
        amqp_connected: bool = false,
        /// Recovery is responsible for retrieving the last published event
        /// timestamp from the AMQP server, allowing to resume from that point.
        /// See `state_recover()` for more details.
        recovering: union(enum) {
            idle,
            declare_queue,
            get_header,
            nack: u64,
            done,
        } = .idle,

        /// The producer is responsible for reading events from TigerBeetle.
        producing: enum {
            idle,
            requesting,
            /// No events to publish,
            /// waiting for the timeout to check for new events.
            waiting,
        } = .idle,

        /// The consumer is responsible to publish events on the AMQP server.
        consuming: enum {
            idle,
            begin_transaction,
            publish,
            commit,
        } = .idle,

        /// Last timestamps:
        /// - producer = read from TigerBeetle.
        /// - consumer = published on AMQP.
        timestamp_last: ?struct {
            producer: u64,
            consumer: u64,
        } = null,
    } = .{},

    pub const event_count_max: u32 = Client.StateMachine.operation_result_max(
        .get_events,
        constants.message_body_size_max,
    );

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
            amqp_publish_exchange_name: []const u8,
            /// AMQP queue name declared by the client as state tracker.
            /// This reference should be valid for the life-time of the `Runner`.
            amqp_queue_state_tracker_name: []const u8,
            /// Number max of events produced/consumed each time.
            events_count_limit: u32 = event_count_max,
            /// Number of seconds to query again if there's no new events to process.
            /// Must be greater than zero.
            idle_check_seconds: u32 = 5,
        },
    ) !void {
        self.* = undefined;
        self.io = io;

        assert(options.idle_check_seconds > 0);
        self.idle_check_seconds = options.idle_check_seconds;

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

        assert(options.events_count_limit <= event_count_max);
        self.buffer = try DualBuffer.init(allocator, options.events_count_limit);
        errdefer self.buffer.deinit(allocator);

        self.amqp_publish_exchange_name = options.amqp_publish_exchange_name;
        self.amqp_queue_state_tracker_name = options.amqp_queue_state_tracker_name;
        self.state = .{};

        self.amqp_client = try amqp.Client.init(allocator, .{
            .io = io,
            .messages_count_max = options.events_count_limit,
            .message_body_size_max = Message.json_string_size_max,
        });
        errdefer self.amqp_client.deinit(allocator);
        try self.amqp_client.connect(
            &struct {
                fn callback(context: *vsr.amqp.Client) void {
                    const runner: *Runner = @alignCast(@fieldParentPtr("amqp_client", context));
                    assert(!runner.state.amqp_connected);
                    maybe(runner.state.vsr_connected);
                    log.info("AMQP connected.", .{});
                    runner.state.amqp_connected = true;
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
                    assert(!runner.state.vsr_connected);
                    maybe(runner.state.amqp_connected);
                    log.info("VSR client registered.", .{});
                    runner.vsr_client.batch_size_limit = result.batch_size_limit;
                    runner.state.vsr_connected = true;
                    runner.start();
                }
            }.callback,
            @as(u128, @intCast(@intFromPtr(self))),
        );
    }

    pub fn deinit(self: *Runner, allocator: std.mem.Allocator) void {
        self.message_pool.deinit(allocator);
        self.vsr_client.deinit(allocator);
        self.buffer.deinit(allocator);
        self.amqp_client.deinit(allocator);
    }

    fn start(self: *Runner) void {
        assert(self.state.vsr_connected or self.state.amqp_connected);
        assert(self.state.producing == .idle);
        assert(self.state.consuming == .idle);

        if (self.state.vsr_connected and
            self.state.amqp_connected and
            self.state.recovering == .done)
        {
            assert(self.state.timestamp_last != null);
            log.info("Starting CDC.", .{});
            self.produce();
        }
    }

    /// In order to make the CDC stateless, a queue is used to store the state:
    /// - A persistent, single-consumer queue is declared with a max size of 1 message
    ///   and "drop head" behavior on overflow.
    /// - During publishing, a message containing just the last timestamp is pushed into
    ///   this queue at the end of the processed batch, within the same transaction scope.
    /// - On restart, the presence of a message indicates the `timestamp_min` from which to
    ///   resume processing events. Otherwise, starts processing from the beginning.
    /// - The initial timestamp can be overridden via the command line.
    fn recover(self: *Runner) void {
        assert(self.state.amqp_connected);
        assert(self.state.timestamp_last == null);
        switch (self.state.recovering) {
            .idle => {
                self.state.recovering = .declare_queue;
                self.recover_dispatch();
            },
            else => unreachable,
        }
    }

    fn recover_dispatch(self: *Runner) void {
        assert(self.state.amqp_connected);
        switch (self.state.recovering) {
            .idle => unreachable,
            // Declaring the state tracker queue.
            .declare_queue => {
                // Declaring the queue is a no-op is it already exists.
                self.amqp_client.declare(
                    &struct {
                        fn callback(context: *vsr.amqp.Client) void {
                            const runner: *Runner = @alignCast(@fieldParentPtr("amqp_client", context));
                            assert(runner.state.recovering == .declare_queue);
                            runner.state.recovering = .get_header;
                            runner.recover_dispatch();
                        }
                    }.callback,
                    .{
                        .queue = self.amqp_queue_state_tracker_name,
                        .passive = false,
                        .durable = true,
                        .exclusive = false,
                        .auto_delete = false,
                        .overflow = .@"drop-head",
                        .max_length = 1,
                        .max_length_bytes = 0,
                        .single_active_consumer = true,
                    },
                );
            },
            // Getting the message header from the state tracker queue.
            .get_header => {
                self.amqp_client.get_header(
                    &struct {
                        fn callback(context: *vsr.amqp.Client, delivery_tag: u64, properties: ?amqp.BasicProperties) void {
                            const runner: *Runner = @alignCast(@fieldParentPtr("amqp_client", context));
                            assert(runner.state.recovering == .get_header);
                            if (properties == null) {
                                runner.state.timestamp_last = .{
                                    .consumer = 0,
                                    .producer = 0,
                                };
                                runner.state.recovering = .done;
                                return runner.start();
                            }

                            var iterator = properties.?.headers.?.iterator();
                            const kv = (iterator.next() catch unreachable).?;
                            assert(std.mem.eql(u8, kv.key, "timestamp_last"));
                            assert(kv.value == .long_long_uint);

                            const timestamp: u64 = kv.value.long_long_uint;
                            runner.state.timestamp_last = .{
                                .consumer = timestamp,
                                .producer = timestamp + 1,
                            };

                            runner.state.recovering = .{ .nack = delivery_tag };
                            runner.recover_dispatch();
                        }
                    }.callback,
                    .{
                        .queue = self.amqp_queue_state_tracker_name,
                    },
                );
            },
            // Sending a `nack`, so the message remains in the queue
            // in case we restart before publishing any new message.
            .nack => |delivery_tag| {
                assert(delivery_tag > 0);
                self.amqp_client.nack(&struct {
                    fn callback(context: *vsr.amqp.Client) void {
                        const runner: *Runner = @alignCast(@fieldParentPtr("amqp_client", context));
                        assert(runner.state.recovering == .nack);
                        runner.state.recovering = .done;
                        return runner.start();
                    }
                }.callback, .{
                    .delivery_tag = delivery_tag,
                });
            },
            .done => unreachable,
        }
    }

    /// The "Producer" fetches events from TigerBeetle (`get_events` operation) into a buffer
    /// to be consumed by the "Consumer".
    fn produce(self: *Runner) void {
        assert(self.state.vsr_connected and self.state.amqp_connected);
        assert(self.state.recovering == .done);
        assert(self.state.timestamp_last != null);
        switch (self.state.producing) {
            .idle => {
                if (!self.buffer.producer_begin()) {
                    // No free buffers.
                    // The running consumer will resume the producer once it finishes.
                    assert(self.state.consuming != .idle);
                    return;
                }
                self.state.producing = .requesting;
                self.produce_dispatch();
            },
            else => unreachable, // Already running.
        }
    }

    fn produce_dispatch(self: *Runner) void {
        assert(self.state.vsr_connected and self.state.amqp_connected);
        assert(self.state.recovering == .done);
        assert(self.state.timestamp_last != null);
        switch (self.state.producing) {
            .idle => unreachable,
            // Submitting the request through the VSR client.
            .requesting => {
                const filter: tb.EventFilter = .{
                    .limit = self.buffer.event_count_limit(),
                    .timestamp_min = self.state.timestamp_last.?.producer,
                    .timestamp_max = 0,
                };
                self.vsr_client.request(
                    &struct {
                        fn callback(
                            context: u128,
                            operation: StateMachine.Operation,
                            timestamp: u64,
                            result: []u8,
                        ) void {
                            assert(operation == .get_events);
                            assert(timestamp != 0);
                            const runner: *Runner = @ptrFromInt(@as(usize, @intCast(context)));
                            const events: []const tb.Event = stdx.bytes_as_slice(.exact, tb.Event, result);

                            stdx.copy_left(
                                .inexact,
                                tb.Event,
                                runner.buffer.get_producer_buffer(),
                                events,
                            );
                            runner.buffer.producer_finish(@intCast(events.len));

                            if (runner.buffer.all_free()) {
                                // No events to publish.
                                // Going idle and will check again for new events.
                                assert(events.len == 0);
                                assert(runner.state.consuming == .idle);
                                runner.state.producing = .waiting;
                                return runner.produce_dispatch();
                            }

                            runner.state.producing = .idle;
                            assert(events.len > 0 or runner.state.consuming != .idle);
                            if (events.len > 0) {
                                const timestamp_next = events[events.len - 1].timestamp + 1;
                                runner.state.timestamp_last.?.producer = timestamp_next;

                                // Since the buffer was populated,
                                // resume consuming (if not already running).
                                if (runner.state.consuming == .idle) runner.consume();

                                // Resume producing (if there's a buffer free).
                                runner.produce();
                            }
                        }
                    }.callback,
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
                            assert(runner.state.consuming == .idle);

                            const producer_begin = runner.buffer.producer_begin();
                            assert(producer_begin);
                            runner.state.producing = .requesting;
                            runner.produce_dispatch();
                        }
                    }.callback,
                    &self.idle_completion,
                    self.idle_check_seconds * std.time.ns_per_s,
                );
            },
        }
    }

    /// The "Consumer" reads from the buffer populated by the "Producer"
    /// and publishes the events to the AMQP server.
    fn consume(self: *Runner) void {
        assert(self.state.vsr_connected and self.state.amqp_connected);
        assert(self.state.recovering == .done);
        assert(self.state.timestamp_last != null);
        switch (self.state.consuming) {
            .idle => {
                if (!self.buffer.consumer_begin()) {
                    // No buffers ready.
                    // The running producer will resume the consumer once it finishes.
                    assert(self.state.producing != .idle);
                    return;
                }
                self.state.consuming = .begin_transaction;
                self.consume_dispatch();
            },
            else => unreachable, // Already running.
        }
    }

    fn consume_dispatch(self: *Runner) void {
        assert(self.state.vsr_connected and self.state.amqp_connected);
        assert(self.state.recovering == .done);
        assert(self.state.timestamp_last != null);
        switch (self.state.consuming) {
            .idle => unreachable,
            // Starting a transaction with the AMQP server.
            // The entire batch of published messages must succeed atomically.
            .begin_transaction => {
                self.amqp_client.tx_select(&struct {
                    fn callback(context: *vsr.amqp.Client) void {
                        const runner: *Runner = @alignCast(@fieldParentPtr("amqp_client", context));
                        assert(runner.state.consuming == .begin_transaction);
                        runner.state.consuming = .publish;
                        runner.consume_dispatch();
                    }
                }.callback);
            },
            // Publishing all the events plus the state-tracking message containing
            // the last timestamp.
            .publish => {
                const events: []const tb.Event = self.buffer.get_consumer_buffer();
                assert(events.len > 0);
                for (events) |*event| {
                    self.amqp_client.publish_enqueue(
                        *const tb.Event,
                        Message.Header,
                        event,
                        Message.header(event),
                        Message.render,
                        .{
                            .content_type = Message.content_type,
                            .exchange = self.amqp_publish_exchange_name,
                            .routing_key = "",
                            .mandatory = true,
                            .immediate = false,
                            .delivery_mode = .persistent,
                            // AMQP timestamp in seconds.
                            .timestamp = event.timestamp / std.time.ns_per_s,
                        },
                    );
                }

                self.amqp_client.publish_enqueue(
                    StateTrackerMessage,
                    StateTrackerMessage.Header,
                    .{},
                    .{ .timestamp_last = events[events.len - 1].timestamp },
                    StateTrackerMessage.render,
                    .{
                        .exchange = "",
                        .routing_key = self.amqp_queue_state_tracker_name,
                        .mandatory = true,
                        .immediate = false,
                        .delivery_mode = .persistent,
                        .timestamp = @intCast(std.time.milliTimestamp()),
                    },
                );

                self.amqp_client.publish_flush(&struct {
                    fn callback(context: *vsr.amqp.Client) void {
                        const runner: *Runner = @alignCast(@fieldParentPtr(
                            "amqp_client",
                            context,
                        ));
                        assert(runner.state.consuming == .publish);
                        runner.state.consuming = .commit;
                        runner.consume_dispatch();
                    }
                }.callback);
            },
            // Committing the transaction with the AMQP server.
            .commit => {
                self.amqp_client.tx_commit(&struct {
                    fn callback(context: *vsr.amqp.Client) void {
                        const runner: *Runner = @alignCast(@fieldParentPtr(
                            "amqp_client",
                            context,
                        ));
                        assert(runner.state.consuming == .commit);
                        const events = runner.buffer.get_consumer_buffer();
                        const timestamp_last = events[events.len - 1].timestamp;

                        runner.buffer.consumer_finish();
                        runner.state.timestamp_last.?.consumer = timestamp_last;

                        // Since the buffer was released,
                        // resume producing (if not already running).
                        if (runner.state.producing == .idle) runner.produce();

                        // Resume consuming (if there's a buffer ready).
                        runner.state.consuming = .idle;
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

    pub fn event_count_limit(self: *const DualBuffer) u32 {
        assert(self.buffer_1.buffer.len == self.buffer_2.buffer.len);
        return @intCast(self.buffer_1.buffer.len);
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
const StateTrackerMessage = struct {
    pub const Header = struct {
        timestamp_last: u64,
    };

    pub fn render(_: StateTrackerMessage, _: []u8) usize {
        return 0;
    }
};

/// Message with the body in the JSON schema.
const Message = struct {
    /// Custom metadata published as part of the messaage header.
    pub const Header = struct {
        event_type: []const u8,
        ledger: u32,
        transfer_code: u16,
        debit_account_code: u16,
        credit_account_code: u16,
    };

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

    pub fn header(event: *const tb.Event) Header {
        return .{
            .event_type = @tagName(event.type),
            .ledger = event.ledger,
            .transfer_code = event.transfer_code,
            .debit_account_code = event.debit_account_code,
            .credit_account_code = event.credit_account_code,
        };
    }

    pub fn render(event: *const tb.Event, output_buffer: []u8) usize {
        var array_list = std.ArrayListUnmanaged(u8).initBuffer(output_buffer);
        std.json.stringify(from_event(event), .{
            .whitespace = .minified,
            .emit_nonportable_numbers_as_strings = true,
        }, array_list.fixedWriter()) catch unreachable;
        return array_list.items.len;
    }

    fn from_event(event: *const tb.Event) Message {
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
