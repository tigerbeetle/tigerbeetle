//! Integration tests for the AMQP client.
//! Uses a real RabbitMQ instance running on docker.

const std = @import("std");
const builtin = @import("builtin");
const log = std.log;
const testing = std.testing;
const stdx = @import("../stdx.zig");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const ratio = stdx.PRNG.ratio;

const vsr = @import("../vsr.zig");
const amqp = @import("amqp.zig");
const Shell = @import("../shell.zig");

test "amqp: integration: RabbitMQ v3" {
    try run_test(TmpRabbitMQ.rabbitmq3);
}

test "amqp: integration: RabbitMQ v4" {
    try run_test(TmpRabbitMQ.rabbitmq4);
}

fn run_test(image: []const u8) !void {
    if (builtin.os.tag != .linux or !builtin.cpu.arch.isX86()) {
        log.info("skipping amqp on unsupported OS/CPU: {s}/{s}", .{
            @tagName(builtin.os.tag),
            @tagName(builtin.cpu.arch),
        });
        return;
    }

    var rabbit_mq = try TmpRabbitMQ.init(std.testing.allocator, .{ .image = image });
    errdefer rabbit_mq.deinit();

    try run_publish_test(rabbit_mq.host);
    try run_serialization_test(rabbit_mq.host);
}

fn run_publish_test(host: std.net.Address) !void {
    var context: Context = undefined;
    try context.init(std.testing.allocator);
    defer context.deinit(std.testing.allocator);

    try context.connect(host);
    const default_exchange = "";
    const testing_queue = "testing";

    context.exchange_declare(.{
        .exchange = "amq.direct", // One of the pre-existing exchanges.
        .type = "",
        .passive = true, // Validate if the exchange exists.
        .durable = false,
        .auto_delete = false,
        .internal = false,
    });
    context.queue_declare(.{
        .queue = testing_queue, // Creating the queue.
        .passive = false,
        .durable = true,
        .exclusive = false,
        .auto_delete = false,
        .arguments = .{},
    });
    context.queue_declare(.{
        .queue = testing_queue,
        .passive = true, // Validate if the queue was created.
        .durable = false,
        .exclusive = false,
        .auto_delete = false,
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
    try context.disconnect(testing.allocator);
    try context.connect(host);
    // The message must not be consumed:
    const message_3_again = context.get_message(.{ .queue = testing_queue, .no_ack = false });
    try testing.expect(message_3_again != null);
    try testing.expect(!message_3_again.?.header.has_body);
    try testing.expectEqual(@as(u32, 0), message_3.?.header.message_count);
    try testing.expect(message_3_again.?.header.properties.message_id != null);
    try testing.expectEqualStrings("3", message_3_again.?.header.properties.message_id.?);

    // Asserting the progress queue "drop head" behavior,
    // where only the last published message must remain.
    const progress_queue = "progress_queue";
    context.queue_declare(.{
        .queue = progress_queue,
        .passive = false,
        .durable = true,
        .exclusive = false,
        .auto_delete = false,
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

fn run_serialization_test(host: std.net.Address) !void {
    var context: Context = undefined;
    try context.init(std.testing.allocator);
    defer context.deinit(std.testing.allocator);

    try context.connect(host);
    const default_exchange = "";
    const queue = "serialization_queue";
    context.queue_declare(.{
        .queue = queue,
        .passive = false,
        .durable = true,
        .exclusive = false,
        .auto_delete = false,
        .arguments = .{},
    });

    var messages = try std.ArrayListUnmanaged(amqp.BasicPublishOptions).initCapacity(
        testing.allocator,
        Context.message_count_max,
    );
    defer messages.deinit(testing.allocator);

    var prng = stdx.PRNG.from_seed(42);
    for (0..64) |_| {
        var arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena.deinit();

        const message_count = prng.range_inclusive(u32, 1, Context.message_count_max);
        assert(messages.capacity >= message_count);
        assert(messages.items.len == 0);
        for (0..message_count) |_| {
            var properties = try TestingBasicProperties.random(.{
                .arena = arena.allocator(),
                .prng = &prng,
            });
            // The fields `user_id` and `expiration` cannot be random values,
            // as they are parsed by the server, so we leave them empty.
            properties.user_id = null;
            properties.expiration = null;

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
            try context.disconnect(testing.allocator);
            try context.connect(host);
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

const Context = struct {
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

    pub fn init(self: *Context, gpa: std.mem.Allocator) !void {
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

    pub fn deinit(self: *Context, gpa: std.mem.Allocator) void {
        assert(!self.busy);
        self.client.deinit(gpa);
        self.io.deinit();
    }

    pub fn connect(self: *Context, host: std.net.Address) !void {
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
    pub fn disconnect(self: *Context, gpa: std.mem.Allocator) !void {
        assert(!self.busy);
        assert(self.client.fd != vsr.io.IO.INVALID_SOCKET);
        assert(self.client.awaiter == .none);
        assert(self.client.action == .none);
        assert(self.client.send_buffer.state == .idle);
        assert(self.client.receive_buffer.state == .receiving);
        maybe(self.client.heartbeat != .idle); // It may be processing a heartbeat.

        self.deinit(gpa);
        try self.init(gpa);
    }

    pub fn queue_declare(self: *Context, options: amqp.QueueDeclareOptions) void {
        assert(!self.busy);
        self.busy = true;
        self.client.queue_declare(&callback, options);
        self.wait();
    }

    pub fn exchange_declare(self: *Context, options: amqp.ExchangeDeclareOptions) void {
        assert(!self.busy);
        self.busy = true;
        self.client.exchange_declare(&callback, options);
        self.wait();
    }

    pub fn publish(self: *Context, options: []const amqp.BasicPublishOptions) void {
        assert(!self.busy);
        self.busy = true;
        for (options) |message| self.client.publish_enqueue(message);
        self.client.publish_send(&callback);
        self.wait();
    }

    pub fn get_message(self: *Context, options: amqp.GetMessageOptions) ?Message {
        assert(!self.busy);
        assert(self.message == null);
        defer self.message = null;
        self.busy = true;
        self.client.get_message(&get_message_header_callback, options);
        self.wait();
        return self.message;
    }

    pub fn nack(self: *Context, options: amqp.BasicNackOptions) void {
        assert(!self.busy);
        self.busy = true;
        self.client.nack(&callback, options);
        self.wait();
    }

    fn wait(self: *Context) void {
        while (self.busy) {
            self.io.run_for_ns(tick_ms * std.time.ns_per_ms) catch unreachable;
            self.client.tick();
        }
        assert(!self.busy);
    }

    fn callback(client: *amqp.Client) void {
        const context: *Context = @alignCast(@fieldParentPtr("client", client));
        assert(context.busy);
        context.busy = false;
    }

    fn get_message_header_callback(
        client: *amqp.Client,
        result: ?amqp.GetMessagePropertiesResult,
    ) amqp.Decoder.Error!void {
        const context: *Context = @alignCast(@fieldParentPtr("client", client));
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
        const context: *Context = @alignCast(@fieldParentPtr("client", client));
        assert(context.busy);
        assert(context.message != null);
        assert(context.message.?.body == null);
        context.message.?.body = result;
        context.busy = false;
    }
};

const TestingBasicProperties = @import("./amqp/protocol.zig").TestingBasicProperties;
const TestingContent = struct {
    const size_max = 1024;
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

const TmpRabbitMQ = struct {
    const rabbitmq3 = "rabbitmq:3";
    const rabbitmq4 = "rabbitmq:4";

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

        const name: []const u8 = try std.fmt.allocPrint(gpa, "{}", .{stdx.unique_u128()});
        defer gpa.free(name);

        // Spawning a RabbitMQ server as a Docker container.
        _ = try try_execute(shell, "docker image pull {image}", .{ .image = options.image });
        var process = try shell.spawn(
            .{},
            "docker run --rm --name {name} --publish {port} {image}",
            .{
                .name = name,
                .port = amqp.tcp_port_default,
                .image = options.image,
            },
        );
        errdefer _ = process.kill() catch unreachable;

        const host: std.net.Address = host: {
            const stdout = try try_execute(shell, "docker port {name}", .{ .name = name });
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
            "docker exec {name} rabbitmq-diagnostics check_port_connectivity",
            .{ .name = name },
        );

        return TmpRabbitMQ{
            .host = host,
            .process = process,
        };
    }

    pub fn deinit(self: *TmpRabbitMQ) void {
        assert(self.process.term == null);
        _ = self.process.kill() catch unreachable;
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
    const attempt_max = 5;
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
