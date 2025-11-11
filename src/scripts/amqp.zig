//! Integration tests for the AMQP client.
//! Uses a real RabbitMQ instance running on docker.

const std = @import("std");
const builtin = @import("builtin");
const log = std.log;
const testing = std.testing;
const stdx = @import("stdx");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const ratio = stdx.PRNG.ratio;
const KiB = stdx.KiB;

const tb = @import("../tigerbeetle.zig");
const vsr = @import("../vsr.zig");
const amqp = @import("../cdc/amqp.zig");

const JSONMessage = @import("../cdc/runner.zig").Message;
const Shell = @import("../shell.zig");
const TmpTigerBeetle = @import("../testing/tmp_tigerbeetle.zig");

pub const CLIArgs = struct {
    transfer_count: u32,
    image: ?[]const u8 = null,
};

pub fn main(_: *Shell, gpa: std.mem.Allocator, cli_args: CLIArgs) !void {
    if (builtin.os.tag != .linux and !builtin.cpu.arch.isX86()) {
        log.warn("skip AMQP integration tests for platforms other than Linux X64", .{});
        return;
    }

    const images: []const []const u8 = if (cli_args.image) |image|
        &.{image}
    else
        &.{ TmpRabbitMQ.rabbitmq3, TmpRabbitMQ.rabbitmq4 };

    for (images) |image| {
        log.info("image: {s}", .{image});

        var rabbit_mq = try TmpRabbitMQ.init(gpa, .{
            .image = image,
        });
        defer rabbit_mq.stop(gpa) catch unreachable;

        try run_protocol_test(gpa, .{
            .host = rabbit_mq.host,
        });
        try run_serialization_test(gpa, .{
            .host = rabbit_mq.host,
        });
        try run_cdc_test(gpa, .{
            .host = rabbit_mq.host,
            .transfer_count = cli_args.transfer_count,
        });
    }
}

fn run_protocol_test(gpa: std.mem.Allocator, options: struct { host: std.net.Address }) !void {
    var context: AmqpContext = undefined;
    try context.init(gpa);
    defer context.deinit(gpa);

    try context.connect(options.host);

    context.exchange_declare(.{
        .exchange = "amq.direct", // One of the pre-existing exchanges.
        .type = "",
        .passive = true, // Validate if the exchange exists.
        .durable = false,
        .auto_delete = false,
        .internal = false,
    });

    const default_exchange = "";
    const testing_queue = try std.fmt.allocPrint(gpa, "queue_{}", .{
        stdx.unique_u128(),
    });
    defer gpa.free(testing_queue);
    context.queue_declare(.{
        .queue = testing_queue, // Creating the queue.
        .passive = false,
        .durable = false,
        .exclusive = false,
        .auto_delete = true,
        .arguments = .{},
    });
    context.queue_declare(.{
        .queue = testing_queue,
        .passive = true, // Validate if the queue was created.
        .durable = true,
        .exclusive = false,
        .auto_delete = true,
        .arguments = .{},
    });

    context.publish(&.{
        .{
            .exchange = default_exchange,
            .routing_key = testing_queue,
            .mandatory = true,
            .immediate = false,
            .properties = .{
                .message_id = "1",
                .delivery_mode = .persistent,
            },
            .body = null,
        },
    });
    const message_1 = context.get_message(.{ .queue = testing_queue, .no_ack = false });
    try testing.expect(message_1 != null);
    try testing.expect(!message_1.?.header.has_body);
    try testing.expectEqual(@as(u32, 0), message_1.?.header.message_count);
    try testing.expect(message_1.?.header.properties.message_id != null);
    try testing.expectEqualStrings("1", message_1.?.header.properties.message_id.?);

    // Nack: message "1" is returned to the queue.
    // Since "nack" is asynchronous, we can't assert `get_message() == "1"` immediately afterward.
    // Instead, we publish one more message and then read them again.
    context.nack(.{
        .delivery_tag = message_1.?.header.delivery_tag,
        .multiple = false,
        .requeue = true,
    });
    context.publish(&.{
        .{
            .exchange = default_exchange,
            .routing_key = testing_queue,
            .mandatory = true,
            .immediate = false,
            .properties = .{
                .message_id = "2",
                .delivery_mode = .persistent,
            },
            .body = null,
        },
    });
    const message_1_again = context.get_message(.{ .queue = testing_queue, .no_ack = true });
    try testing.expect(message_1_again != null);
    try testing.expect(!message_1_again.?.header.has_body);
    try testing.expectEqual(
        @as(u32, 1), // There's one more message.
        message_1_again.?.header.message_count,
    );
    try testing.expect(message_1_again.?.header.properties.message_id != null);
    try testing.expectEqualStrings("1", message_1_again.?.header.properties.message_id.?);
    const message_2 = context.get_message(.{ .queue = testing_queue, .no_ack = true });
    try testing.expect(message_2 != null);
    try testing.expect(!message_2.?.header.has_body);
    try testing.expectEqual(@as(u32, 0), message_2.?.header.message_count);
    try testing.expect(message_2.?.header.properties.message_id != null);
    try testing.expectEqualStrings("2", message_2.?.header.properties.message_id.?);
    try testing.expectEqual(null, context.get_message(
        .{ .queue = testing_queue, .no_ack = false },
    ));

    context.publish(&.{
        .{
            .exchange = default_exchange,
            .routing_key = testing_queue,
            .mandatory = true,
            .immediate = false,
            .properties = .{
                .message_id = "3",
                .delivery_mode = .persistent,
            },
            .body = null,
        },
    });
    const message_3 = context.get_message(.{ .queue = testing_queue, .no_ack = false });
    try testing.expect(message_3 != null);
    try testing.expect(!message_3.?.header.has_body);
    try testing.expectEqual(@as(u32, 0), message_3.?.header.message_count);
    try testing.expect(message_3.?.header.properties.message_id != null);
    try testing.expectEqualStrings("3", message_3.?.header.properties.message_id.?);

    // Closing the connection without a ack/nack:
    try context.disconnect(gpa);
    try context.connect(options.host);

    // The message must not be consumed:
    const message_3_again = context.get_message(.{ .queue = testing_queue, .no_ack = false });
    try testing.expect(message_3_again != null);
    try testing.expect(!message_3_again.?.header.has_body);
    try testing.expectEqual(@as(u32, 0), message_3.?.header.message_count);
    try testing.expect(message_3_again.?.header.properties.message_id != null);
    try testing.expectEqualStrings("3", message_3_again.?.header.properties.message_id.?);

    // Asserting the progress queue "drop head" behavior,
    // where only the last published message must remain.
    const progress_queue = try std.fmt.allocPrint(gpa, "queue_{}", .{
        stdx.unique_u128(),
    });
    defer gpa.free(progress_queue);
    context.queue_declare(.{
        .queue = progress_queue,
        .passive = false,
        .durable = true,
        .exclusive = false,
        .auto_delete = true,
        .arguments = .{
            .overflow = .drop_head,
            .max_length = 1,
        },
    });
    context.publish(&.{
        .{
            .exchange = default_exchange,
            .routing_key = progress_queue,
            .mandatory = true,
            .immediate = false,
            .properties = .{
                .message_id = "4",
                .delivery_mode = .persistent,
            },
            .body = null,
        },
        .{
            .exchange = default_exchange,
            .routing_key = progress_queue,
            .mandatory = true,
            .immediate = false,
            .properties = .{
                .message_id = "5",
                .delivery_mode = .persistent,
            },
            .body = null,
        },
    });
    // Message "5" must drop the previous "4".
    const message_5 = context.get_message(.{ .queue = progress_queue, .no_ack = false });
    try testing.expect(message_5 != null);
    try testing.expect(!message_5.?.header.has_body);
    try testing.expectEqual(@as(u32, 0), message_5.?.header.message_count);
    try testing.expect(message_5.?.header.properties.message_id != null);
    try testing.expectEqualStrings("5", message_5.?.header.properties.message_id.?);
    // Nack: message "5" is returned to the queue.
    context.nack(.{
        .delivery_tag = message_5.?.header.delivery_tag,
        .multiple = false,
        .requeue = true,
    });
    // Message "6" must drop the returned "5".
    context.publish(&.{
        .{
            .exchange = default_exchange,
            .routing_key = progress_queue,
            .mandatory = true,
            .immediate = false,
            .properties = .{
                .message_id = "6",
                .delivery_mode = .persistent,
            },
            .body = null,
        },
    });
    const message_6 = context.get_message(.{ .queue = progress_queue, .no_ack = false });
    try testing.expect(message_6 != null);
    try testing.expect(!message_6.?.header.has_body);
    try testing.expectEqual(@as(u32, 0), message_6.?.header.message_count);
    try testing.expect(message_6.?.header.properties.message_id != null);
    try testing.expectEqualStrings("6", message_6.?.header.properties.message_id.?);
}

fn run_serialization_test(
    gpa: std.mem.Allocator,
    options: struct { host: std.net.Address },
) !void {
    var context: AmqpContext = undefined;
    try context.init(gpa);
    defer context.deinit(gpa);

    try context.connect(options.host);
    const default_exchange = "";
    const queue = try std.fmt.allocPrint(gpa, "queue_{}", .{
        stdx.unique_u128(),
    });
    defer gpa.free(queue);
    context.queue_declare(.{
        .queue = queue,
        .passive = false,
        .durable = true,
        .exclusive = false,
        .auto_delete = true,
        .arguments = .{},
    });

    var messages = try std.ArrayListUnmanaged(amqp.BasicPublishOptions).initCapacity(
        gpa,
        AmqpContext.message_count_max,
    );
    defer messages.deinit(gpa);

    var prng = stdx.PRNG.from_seed(42);
    for (0..64) |_| {
        var arena = std.heap.ArenaAllocator.init(gpa);
        defer arena.deinit();

        const message_count = prng.range_inclusive(u32, 1, AmqpContext.message_count_max);
        assert(messages.capacity >= message_count);
        assert(messages.items.len == 0);
        for (0..message_count) |_| {
            const properties = try TestingBasicProperties.random(.{
                .arena = arena.allocator(),
                .prng = &prng,
                .default = .{
                    // Always send persistent messages
                    .delivery_mode = .persistent,
                    // The fields `user_id` and `expiration` cannot be random values,
                    // as they are parsed by the server.
                    .user_id = "guest",
                    .expiration = try std.fmt.allocPrint(arena.allocator(), "{}", .{
                        // Random TTL between 5 minutes and 24 hours.
                        prng.range_inclusive(u32, 5 * 60, 24 * 60 * 60),
                    }),
                },
            });
            const content: ?*TestingContent = content: {
                if (prng.chance(ratio(20, 80))) {
                    break :content null;
                }
                break :content try TestingContent.init(arena.allocator(), &prng);
            };
            messages.appendAssumeCapacity(.{
                .exchange = default_exchange,
                .routing_key = queue,
                .mandatory = true,
                .immediate = false,
                .properties = properties,
                .body = if (content) |message| message.body() else null,
            });
        }
        assert(messages.items.len == message_count);
        context.publish(messages.items);
        // Maybe disconnect the client between publishes:
        if (prng.chance(ratio(20, 100))) {
            try context.disconnect(gpa);
            try context.connect(options.host);
        }

        for (messages.items, 0..) |sent, index| {
            const received = context.get_message(.{ .queue = queue, .no_ack = true });
            const remaining: u32 = @intCast(message_count - index - 1);
            try testing.expect(received != null);
            try testing.expectEqual(remaining, received.?.header.message_count);
            try testing.expectEqual(true, try TestingBasicProperties.eql(
                arena.allocator(),
                sent.properties,
                received.?.header.properties,
            ));
            try testing.expectEqual(sent.body != null, received.?.header.has_body);
            try testing.expectEqual(sent.body != null, received.?.body != null);
            if (received.?.body) |body_received| {
                const content: *const TestingContent = @ptrCast(@alignCast(sent.body.?.context));
                try testing.expectEqualSlices(u8, content.bytes, body_received);
            }
        }
        try testing.expectEqual(null, context.get_message(
            .{ .queue = queue, .no_ack = false },
        ));
        messages.clearRetainingCapacity();
    }
}

fn run_cdc_test(
    gpa: std.mem.Allocator,
    options: struct {
        transfer_count: u32,
        host: std.net.Address,
    },
) !void {
    var amqp_context: AmqpContext = undefined;
    try amqp_context.init(gpa);
    defer amqp_context.deinit(gpa);
    try amqp_context.connect(options.host);

    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();

    var time_os: vsr.time.TimeOS = .{};

    const queue = try std.fmt.allocPrint(arena.allocator(), "queue_{}", .{
        stdx.unique_u128(),
    });
    amqp_context.queue_declare(.{
        .queue = queue,
        .passive = false,
        .durable = true,
        .exclusive = false,
        .auto_delete = false,
        .arguments = .{},
    });

    var tmp_beetle = try TmpTigerBeetle.init(gpa, .{
        .development = false,
    });
    defer tmp_beetle.deinit(gpa);

    const shell = try Shell.create(gpa);
    defer shell.destroy();

    // Starting the CDC job:
    var cdc_job = try shell.spawn(
        .{},
        "{tigerbeetle} amqp " ++
            "--cluster=0 --addresses={addresses} " ++
            "--host=127.0.0.1:{port} --vhost=/ --user=guest --password=guest " ++
            "--publish-routing-key={queue} " ++
            "--idle-interval-ms={idle_interval_ms}",
        .{
            .tigerbeetle = tmp_beetle.tigerbeetle_exe,
            .addresses = tmp_beetle.port_str,
            .port = options.host.getPort(),
            .queue = queue,
            .idle_interval_ms = 1,
        },
    );
    defer _ = cdc_job.kill() catch undefined;

    // Use the `benchmark` command to generate data.
    assert(options.transfer_count > 0);
    var benchmark = try shell.spawn(
        .{},
        "{tigerbeetle} benchmark " ++
            "--addresses={addresses} " ++
            "--transfer-count={transfer_count} " ++
            "--transfer-pending",
        .{
            .tigerbeetle = tmp_beetle.tigerbeetle_exe,
            .addresses = tmp_beetle.port_str,
            .transfer_count = options.transfer_count,
        },
    );
    defer {
        const term = benchmark.wait() catch unreachable;
        assert(term == .Exited);
        assert(term.Exited == 0);
    }

    // TODO: Improvements:
    // - Use benchmark `--transfer-batch-size` and `--transfer-batch-delay-us`
    //   to throttle data generation, stressing the CDC idle path.
    // - Eventually kill the CDC job to stress the recovery path, and assert that
    //   at most one batch is duplicated.
    // - Start multiple CDC jobs to stress the lock queue.
    var vsr_context: VSRContext = undefined;
    try vsr_context.init(gpa, time_os.time(), tmp_beetle.port);
    defer vsr_context.deinit(gpa);

    var count: u32 = 0;
    var expiry_count: u32 = 0;
    var expiry_pending_count: u32 = 0;
    var timestamp_previous: u64 = 0;
    while (count < options.transfer_count + expiry_count) {
        const events: []tb.ChangeEvent = events: {
            for (0..10) |attempt| {
                if (attempt > 0) {
                    // Waiting for events:
                    std.time.sleep(500 * std.time.ns_per_ms);
                }
                const events = try vsr_context.get_change_events(timestamp_previous + 1);
                if (events.len > 0) break :events events;
            }
            try testing.expect(false);
            unreachable;
        };
        assert(events.len > 0);
        defer timestamp_previous = events[events.len - 1].timestamp;
        for (events) |*event| {
            // Keep track of how many transfers will expire:
            switch (event.type) {
                .single_phase => {},
                .two_phase_pending => if (event.transfer_timeout > 0) {
                    assert(expiry_count >= expiry_pending_count);
                    expiry_count += 1;
                    expiry_pending_count += 1;
                },
                .two_phase_expired => {
                    assert(event.transfer_timeout > 0);
                    assert(expiry_count > 0);
                    assert(expiry_pending_count > 0);
                    assert(expiry_count >= expiry_pending_count);
                    expiry_pending_count -= 1;
                },
                .two_phase_posted, .two_phase_voided => {
                    // The benchmark doesn't post/void transfers with timeout.
                    assert(event.transfer_timeout == 0);
                },
            }
            assert(count < options.transfer_count + expiry_count);
            count += 1;

            const message = message: {
                for (0..10) |attempt| {
                    if (attempt > 0) {
                        // Give the CDC job some time to finish publishing the messages.
                        std.time.sleep(500 * std.time.ns_per_ms);
                    }
                    if (amqp_context.get_message(.{
                        .queue = queue,
                        .no_ack = true,
                    })) |message| break :message message;
                }
                try testing.expect(false);
                unreachable;
            };

            const json = try std.json.parseFromSliceLeaky(
                JSONMessage,
                arena.allocator(),
                message.body.?,
                .{},
            );
            try testing.expectEqualDeep(JSONMessage.init(event), json);
        }
    }
    // No more events.
    assert(expiry_pending_count == 0);
    try testing.expectEqualSlices(
        tb.ChangeEvent,
        &.{},
        try vsr_context.get_change_events(timestamp_previous + 1),
    );
    try testing.expectEqual(@as(?AmqpContext.Message, null), amqp_context.get_message(.{
        .queue = queue,
        .no_ack = true,
    }));
}

const AmqpContext = struct {
    const Message = struct {
        header: amqp.GetMessagePropertiesResult,
        body: ?[]const u8,
    };

    io: vsr.io.IO,
    client: amqp.Client,
    busy: bool,
    message: ?Message,

    /// Faster ticks, since `wait()` blocks on `io.run_for_ns()`.
    const tick_ms = 1;
    const message_count_max = 64;
    const reply_timeout_ticks = @divExact(
        30 * std.time.ms_per_s,
        tick_ms,
    );

    pub fn init(self: *AmqpContext, gpa: std.mem.Allocator) !void {
        self.* = .{
            .busy = false,
            .message = null,
            .io = undefined,
            .client = undefined,
        };

        self.io = try vsr.io.IO.init(32, 0);
        errdefer self.io.deinit();

        self.client = try amqp.Client.init(gpa, .{
            .io = &self.io,
            .message_count_max = message_count_max,
            .message_body_size_max = amqp.frame_min_size,
            .reply_timeout_ticks = reply_timeout_ticks,
        });
    }

    pub fn deinit(self: *AmqpContext, gpa: std.mem.Allocator) void {
        assert(!self.busy);
        self.client.deinit(gpa);
        self.io.deinit();
    }

    pub fn connect(self: *AmqpContext, host: std.net.Address) !void {
        assert(!self.busy);
        self.busy = true;
        try self.client.connect(&callback, .{
            .host = host,
            .user_name = "guest",
            .password = "guest",
            .vhost = "/",
        });
        self.wait();
    }

    // Simulates a crash by disconnecting the client and creating a new one.
    // Uses `deinit() + init()` since graceful disconnection/reconnection is not handled.
    pub fn disconnect(self: *AmqpContext, gpa: std.mem.Allocator) !void {
        assert(!self.busy);
        assert(self.client.fd != null);
        assert(self.client.awaiter == .none);
        assert(self.client.action == .none);
        assert(self.client.send_buffer.state == .idle);
        assert(self.client.receive_buffer.state == .receiving);
        maybe(self.client.heartbeat != .idle); // It may be processing a heartbeat.

        self.deinit(gpa);
        try self.init(gpa);
    }

    pub fn queue_declare(self: *AmqpContext, options: amqp.QueueDeclareOptions) void {
        assert(!self.busy);
        self.busy = true;
        self.client.queue_declare(&callback, options);
        self.wait();
    }

    pub fn exchange_declare(self: *AmqpContext, options: amqp.ExchangeDeclareOptions) void {
        assert(!self.busy);
        self.busy = true;
        self.client.exchange_declare(&callback, options);
        self.wait();
    }

    pub fn publish(self: *AmqpContext, options: []const amqp.BasicPublishOptions) void {
        assert(!self.busy);
        self.busy = true;
        for (options) |message| self.client.publish_enqueue(message);
        self.client.publish_send(&callback);
        self.wait();
    }

    pub fn get_message(self: *AmqpContext, options: amqp.GetMessageOptions) ?Message {
        assert(!self.busy);
        assert(self.message == null);
        defer self.message = null;
        self.busy = true;
        self.client.get_message(&get_message_header_callback, options);
        self.wait();
        return self.message;
    }

    pub fn nack(self: *AmqpContext, options: amqp.BasicNackOptions) void {
        assert(!self.busy);
        self.busy = true;
        self.client.nack(&callback, options);
        self.wait();
    }

    fn wait(self: *AmqpContext) void {
        while (self.busy) {
            self.io.run_for_ns(tick_ms * std.time.ns_per_ms) catch unreachable;
            self.client.tick();
        }
        assert(!self.busy);
    }

    fn callback(client: *amqp.Client) void {
        const context: *AmqpContext = @alignCast(@fieldParentPtr("client", client));
        assert(context.busy);
        context.busy = false;
    }

    fn get_message_header_callback(
        client: *amqp.Client,
        result: ?amqp.GetMessagePropertiesResult,
    ) amqp.Decoder.Error!void {
        const context: *AmqpContext = @alignCast(@fieldParentPtr("client", client));
        assert(context.busy);
        assert(context.message == null);
        if (result) |header| {
            // N.B.: The `GetMessagePropertiesResult` contains references to the `recv` buffer,
            // such as `properties.headers`, which are only valid for the duration of this callback
            // and would normally need to be copied if they are to be stored!
            // However, for simplicity, this *test* keeps the reference and reads the message body,
            // as the header and body are small enough to be received together in the same buffer.
            context.message = .{
                .header = header,
                .body = null,
            };
            if (header.has_body) {
                return context.client.get_message_body(&get_message_body_callback);
            }
        }
        context.busy = false;
    }

    fn get_message_body_callback(
        client: *amqp.Client,
        result: []const u8,
    ) amqp.Decoder.Error!void {
        const context: *AmqpContext = @alignCast(@fieldParentPtr("client", client));
        assert(context.busy);
        assert(context.message != null);
        assert(context.message.?.body == null);
        context.message.?.body = result;
        context.busy = false;
    }
};

const VSRContext = struct {
    const MessagePool = vsr.message_pool.MessagePool;
    const Message = MessagePool.Message;
    const Client = vsr.ClientType(
        tb.Operation,
        vsr.message_bus.MessageBusType(vsr.io.IO),
    );

    client: Client,
    io: vsr.io.IO,
    message_pool: MessagePool,
    busy: bool,
    event_buffer: []tb.ChangeEvent,
    event_count: ?u32,

    pub fn init(self: *VSRContext, gpa: std.mem.Allocator, time: vsr.time.Time, port: u16) !void {
        self.io = try vsr.io.IO.init(32, 0);
        errdefer self.io.deinit();

        self.message_pool = try MessagePool.init(gpa, .client);
        errdefer self.message_pool.deinit(gpa);

        const address = try std.net.Address.parseIp4("127.0.0.1", port);
        self.client = try Client.init(
            gpa,
            time,
            &self.message_pool,
            .{
                .id = stdx.unique_u128(),
                .cluster = 0,
                .replica_count = 1,
                .message_bus_options = .{
                    .configuration = &.{address},
                    .io = &self.io,
                },
            },
        );
        errdefer self.client.deinit(gpa);

        self.event_buffer = undefined;
        self.event_count = null;
        self.busy = true;
        self.client.register(register_callback, @intFromPtr(self));
        self.wait();

        self.event_buffer = try gpa.alloc(tb.ChangeEvent, @divFloor(
            tb.Operation.get_change_events.result_max(vsr.constants.message_body_size_max),
            @sizeOf(tb.ChangeEvent),
        ));
        errdefer gpa.free(self.event_buffer);
        assert(!self.busy);
    }

    pub fn deinit(self: *VSRContext, gpa: std.mem.Allocator) void {
        assert(!self.busy);
        gpa.free(self.event_buffer);
        self.client.deinit(gpa);
        self.message_pool.deinit(gpa);
        self.io.deinit();
        self.* = undefined;
    }

    pub fn get_change_events(self: *VSRContext, timestamp_min: u64) ![]tb.ChangeEvent {
        assert(!self.busy);
        assert(self.event_count == null);
        defer self.event_count = null;

        const filter: tb.ChangeEventsFilter = .{
            .limit = std.math.maxInt(u32),
            .timestamp_min = timestamp_min,
            .timestamp_max = 0,
        };
        self.busy = true;
        self.client.request(
            &request_callback,
            @intFromPtr(self),
            .get_change_events,
            std.mem.asBytes(&filter),
        );
        self.wait();
        assert(!self.busy);
        assert(self.event_count != null);
        assert(self.event_count.? <= self.event_buffer.len);

        return self.event_buffer[0..self.event_count.?];
    }

    fn wait(self: *VSRContext) void {
        while (self.busy) {
            self.client.tick();
            self.io.run_for_ns(vsr.constants.tick_ms * std.time.ns_per_ms) catch unreachable;
        }
    }

    fn register_callback(
        user_data: u128,
        result: *const vsr.RegisterResult,
    ) void {
        // Running with compatible configs.
        assert(result.batch_size_limit <= vsr.constants.message_body_size_max);
        const self: *VSRContext = @ptrFromInt(@as(usize, @intCast(user_data)));
        assert(self.busy);
        self.busy = false;
    }

    fn request_callback(
        user_data: u128,
        operation_vsr: vsr.Operation,
        timestamp: u64,
        result: []u8,
    ) void {
        _ = timestamp;
        const operation = operation_vsr.cast(tb.Operation);
        assert(operation == .get_change_events);

        const self: *VSRContext = @ptrFromInt(@as(usize, @intCast(user_data)));
        assert(self.busy);
        assert(self.event_count == null);

        const events = stdx.bytes_as_slice(
            .exact,
            tb.ChangeEvent,
            result,
        );
        assert(events.len <= self.event_buffer.len);
        self.event_count = @intCast(events.len);
        stdx.copy_disjoint(
            .inexact,
            tb.ChangeEvent,
            self.event_buffer,
            events,
        );
        self.busy = false;
    }
};

const TmpRabbitMQ = struct {
    const rabbitmq3 = "rabbitmq:3";
    const rabbitmq4 = "rabbitmq:4";

    id: u128,
    host: std.net.Address,
    process: std.process.Child,

    pub fn init(
        gpa: std.mem.Allocator,
        options: struct {
            image: []const u8,
        },
    ) !TmpRabbitMQ {
        const shell = try Shell.create(gpa);
        defer shell.destroy();

        const id = stdx.unique_u128();

        // Spawning a RabbitMQ server as a Docker container.
        _ = try try_execute(shell, "docker image pull {image}", .{ .image = options.image });
        var process = try shell.spawn(
            .{},
            "docker run --rm --name {id} --publish {port} {image}",
            .{
                .id = id,
                .port = amqp.tcp_port_default,
                .image = options.image,
            },
        );
        errdefer _ = process.kill() catch unreachable;

        const host: std.net.Address = host: {
            const stdout = try try_execute(shell, "docker port {id}", .{ .id = id });
            // The command `docker port` outputs multiple lines:
            // 5672/tcp -> 0.0.0.0:32773
            // 5672/tcp -> [::]:32773
            var lines = std.mem.splitScalar(u8, stdout, '\n');
            while (lines.next()) |line| {
                _, const host = stdx.cut(line, " -> ") orelse continue;
                // Last index of `:`, because ipv6 can be `[::]:port`.
                const index = std.mem.lastIndexOfScalar(u8, host, ':') orelse continue;
                const port = try std.fmt.parseUnsigned(u16, host[index + 1 ..], 10);
                break :host try std.net.Address.parseIp4("127.0.0.1", port);
            }
            try testing.expect(false);
            unreachable;
        };

        // Waiting for RabbitMQ to become available.
        _ = try try_execute(
            shell,
            "docker exec {id} rabbitmq-diagnostics check_port_connectivity",
            .{ .id = id },
        );

        return TmpRabbitMQ{
            .id = id,
            .host = host,
            .process = process,
        };
    }

    pub fn stop(self: *TmpRabbitMQ, gpa: std.mem.Allocator) !void {
        const shell = try Shell.create(gpa);
        defer shell.destroy();

        try shell.exec(
            "docker stop {id}",
            .{ .id = self.id },
        );
        const term = self.process.wait() catch unreachable;
        assert(term == .Exited);
    }
};

const TestingBasicProperties = @import("../cdc/amqp/protocol.zig").TestingBasicProperties;
const TestingContent = struct {
    const size_max = 1 * KiB;
    bytes: []const u8,

    fn init(arena: std.mem.Allocator, prng: *stdx.PRNG) !*TestingContent {
        const size = prng.range_inclusive(u32, 1, size_max);
        const bytes = try arena.alloc(u8, size);
        prng.fill(bytes);

        const self: *TestingContent = try arena.create(TestingContent);
        self.* = .{
            .bytes = bytes,
        };
        return self;
    }

    fn body(self: *const TestingContent) amqp.Encoder.Body {
        const vtable: amqp.Encoder.Body.VTable = comptime .{
            .write = &struct {
                fn write(context: *const anyopaque, buffer: []u8) usize {
                    const content: *const TestingContent = @ptrCast(@alignCast(context));
                    stdx.copy_disjoint(.inexact, u8, buffer, content.bytes);
                    return content.bytes.len;
                }
            }.write,
        };
        return .{ .context = self, .vtable = &vtable };
    }
};

/// Attempts to execute a shell command.
/// On success, returns the command's stdout output.
/// On failure, waits and retries until the maximum number of attempts is reached.
/// If all attempts fail, prints the command output and returns an error.
fn try_execute(
    shell: *Shell,
    comptime cmd: []const u8,
    cmd_args: anytype,
) ![]const u8 {
    var exec_result: ?std.process.Child.RunResult = null;
    const attempt_max = 15;
    for (0..attempt_max) |attempt| {
        if (attempt > 0) std.time.sleep(1 * std.time.ns_per_s);
        exec_result = try shell.exec_raw(cmd, cmd_args);
        switch (exec_result.?.term) {
            .Exited => |code| if (code == 0) return exec_result.?.stdout,
            else => {},
        }
    }
    assert(exec_result != null);
    std.log.err(
        \\cmd={s}
        \\{s}
        \\{s}
    , .{
        cmd,
        exec_result.?.stdout,
        exec_result.?.stderr,
    });
    try std.testing.expect(false);
    unreachable;
}
