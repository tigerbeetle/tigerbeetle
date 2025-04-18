const std = @import("std");
const builtin = @import("builtin");

const stdx = @import("../stdx.zig");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const log = std.log.scoped(.amqp);

const vsr = @import("../vsr.zig");
const IO = vsr.io.IO;

const spec = @import("./amqp/spec.zig");
const protocol = @import("./amqp/protocol.zig");
const types = @import("./amqp/types.zig");

const ErrorCodes = protocol.ErrorCodes;

pub const Decoder = protocol.Decoder;
pub const Encoder = protocol.Encoder;

pub const ConnectOptions = types.ConnectOptions;
pub const ConnectionProperties = types.ConnectionProperties;
pub const ExchangeDeclareOptions = types.ExchangeDeclareOptions;
pub const QueueDeclareOptions = types.QueueDeclareOptions;
pub const QueueDeclareArguments = types.QueueDeclareArguments;
pub const BasicPublishOptions = types.BasicPublishOptions;
pub const GetMessageOptions = types.GetMessageOptions;
pub const BasicNackOptions = types.BasicNackOptions;

/// AMQP (Advanced Message Queuing Protocol) 0.9.1 client.
/// - Uses TigerBeetle's IO interface.
/// - Single channel only.
/// - Batched publishing with fixed buffers.
/// - Limited consumer capabilities.
/// - Implements only the methods required by TigerBeetle.
/// - No error handling: **CAN PANIC**.
pub const Client = struct {
    pub const tcp_port_default = 5672;

    /// The channel number is 0 for all frames which are global to the connection.
    const channel_global = 0;

    // Id of the current channel.
    // For TigerBeetle, supporting multiple channels is unnecessary,
    // as messages are submitted in batches through io_uring without concurrency.
    const channel_current = 1;

    pub const Callback = *const fn (self: *Client) void;
    pub const GetMessagePropertiesCallback = *const fn (
        self: *Client,
        delivery_tag: u64,
        properties: ?Decoder.BasicProperties,
    ) Decoder.Error!void;

    io: *IO,
    fd: IO.socket_t = IO.INVALID_SOCKET,
    connection_options: ?ConnectOptions,
    frame_size_max: u32,

    receive_buffer: ReceiveBuffer,
    receive_completion: IO.Completion = undefined,

    send_buffer: SendBuffer,
    send_completion: IO.Completion = undefined,

    heartbeat: union(enum) {
        idle,
        sending: IO.Completion,
    } = .idle,

    callback: union(enum) {
        none,
        connect: Callback,
        close: Callback,
        tx_select: Callback,
        tx_commit: Callback,
        queue_declare: Callback,
        exchange_declare: Callback,
        get_message: GetMessagePropertiesCallback,
        nack: Callback,
        publish: Callback,
    } = .none,

    awaiter: union(enum) {
        none,
        /// Flushes the current send buffer and invokes the callback upon completion.
        /// Invariant: the send buffer must be ready to be flushed.
        send_and_forget: *const fn (self: *Client) void,
        /// Flushes the current send buffer and invokes the callback when a synchronous
        /// AMQP method is received.
        /// Invariant: the send buffer must be ready to be flushed.
        send_and_await_reply: struct {
            channel: u16,
            state: enum { sending, awaiting },
            callback: *const fn (self: *Client, reply: spec.ClientMethod) Decoder.Error!void,
        },
        /// Invokes the callback when an AMQP header frame is received, containing information
        /// about the incoming message.
        /// Invariant: the send buffer must be empty.
        await_message_header: struct {
            channel: u16,
            delivery_tag: u64,
            callback: *const fn (
                self: *Client,
                delivery_tag: u64,
                header: Decoder.Header,
            ) Decoder.Error!void,
        },
    } = .none,

    pub fn init(
        allocator: std.mem.Allocator,
        options: struct {
            io: *IO,
            message_count_max: u32,
            message_body_size_max: u32,
        },
    ) !Client {
        assert(options.message_count_max > 0);
        assert(options.message_body_size_max > 0);

        // The worst-case size required to write a frame containing the message body.
        const body_frame_size = Encoder.FrameHeader.SIZE +
            options.message_body_size_max +
            @sizeOf(protocol.FrameEnd);

        const frame_size = @max(spec.FRAME_MIN_SIZE, body_frame_size);

        // Large messages are not expected, but we must be able to receive at least
        // the same frame size we send.
        const receive_buffer = try allocator.alloc(u8, frame_size);
        errdefer allocator.free(receive_buffer);

        // When publishing messages, three frames are sent:
        // Method (including arguments) + Header (including metadata) + Body.
        // Rounded up to multiples of the frame size.
        // TODO: The size could be more effiently calculated if we were aware of
        // the actual variable data we will send.
        const send_buffer_size = stdx.div_ceil(
            ((2 * spec.FRAME_MIN_SIZE) + body_frame_size) * options.message_count_max,
            frame_size,
        ) * frame_size;
        const send_buffer = try allocator.alloc(u8, send_buffer_size);
        errdefer allocator.free(send_buffer);

        return .{
            .io = options.io,
            .connection_options = null,
            .receive_buffer = .{ .buffer = receive_buffer },
            .send_buffer = .{ .buffer = send_buffer },
            .frame_size_max = frame_size,
        };
    }

    pub fn deinit(
        self: *Client,
        allocator: std.mem.Allocator,
    ) void {
        allocator.free(self.receive_buffer.buffer);
        allocator.free(self.send_buffer.buffer);
    }

    pub fn connect(self: *Client, callback: Callback, options: ConnectOptions) !void {
        assert(self.fd == IO.INVALID_SOCKET);
        assert(self.awaiter == .none);
        assert(self.send_buffer.state == .idle);
        assert(self.callback == .none);
        self.callback = .{ .connect = callback };
        self.connection_options = options;

        self.fd = try self.io.open_socket(
            options.address.any.family,
            std.posix.SOCK.STREAM,
            std.posix.IPPROTO.TCP,
        );
        errdefer self.io.close_socket(self.fd);
        if (builtin.os.tag == .linux) {
            try std.posix.setsockopt(
                self.fd,
                std.posix.IPPROTO.TCP,
                std.posix.TCP.NODELAY,
                std.mem.asBytes(&@as(c_int, 1)),
            );
        }
        self.io.connect(
            *Client,
            self,
            struct {
                fn continuation(
                    context: *Client,
                    completion: *IO.Completion,
                    result: IO.ConnectError!void,
                ) void {
                    _ = completion;
                    _ = result catch vsr.fatal(
                        .amqp,
                        "Connection refused.",
                        .{},
                    );

                    // Start receiving and send the AMQP protocol header:
                    context.receive();

                    const encoder = context.send_buffer.encoder();
                    encoder.write_bytes(&protocol.protocol_header);
                    context.send_and_await_reply(channel_global, &connect_dispatch);
                }
            }.continuation,
            &self.receive_completion,
            self.fd,
            options.address,
        );
    }

    fn connect_dispatch(self: *Client, reply: spec.ClientMethod) Decoder.Error!void {
        assert(self.awaiter == .none);
        assert(self.send_buffer.state == .idle);
        assert(self.callback == .connect);
        switch (reply) {
            .connection_start => |args| {
                log.info("Connection start received:", .{});
                log.info("version {}.{}", .{ args.version_major, args.version_minor });
                log.info("locales {s}", .{args.locales});
                log.info("mechanisms {s}", .{args.mechanisms});
                try log_table("server_properties", args.server_properties);

                const plain_auth: types.SASLPlainAuth = .{
                    .user_name = self.connection_options.?.user_name,
                    .password = self.connection_options.?.password,
                };
                const start_ok: spec.ServerMethod = .{ .connection_start_ok = .{
                    .client_properties = self.connection_options.?.properties.table(),
                    .mechanism = types.SASLPlainAuth.mechanism,
                    .response = plain_auth.response(),
                    .locale = self.connection_options.?.locale orelse first: {
                        var iterator = std.mem.splitScalar(u8, args.locales, ' ');
                        break :first iterator.next().?;
                    },
                } };

                const encoder = self.send_buffer.encoder();
                start_ok.write(channel_global, encoder);
                self.send_and_await_reply(channel_global, &connect_dispatch);
            },
            .connection_secure => unreachable, // Not implemented,
            .connection_tune => |args| {
                log.info("Connection tune received:", .{});
                log.info("channel_max {}", .{args.channel_max});
                log.info("frame_max {}", .{args.frame_max});
                log.info("heartbeat {}", .{args.heartbeat});

                const encoder = self.send_buffer.encoder();

                const tune_ok: spec.ServerMethod = .{ .connection_tune_ok = .{
                    .channel_max = 1,
                    .frame_max = @max(spec.FRAME_MIN_SIZE, self.frame_size_max),
                    .heartbeat = self.connection_options.?.heartbeat orelse args.heartbeat,
                } };
                tune_ok.write(channel_global, encoder);

                const open: spec.ServerMethod = .{ .connection_open = .{
                    .virtual_host = self.connection_options.?.vhost,
                } };
                open.write(channel_global, encoder);
                self.send_and_await_reply(channel_global, &connect_dispatch);
            },
            .connection_open_ok => {
                const channel_open: spec.ServerMethod = .{ .channel_open = .{} };
                channel_open.write(channel_current, self.send_buffer.encoder());
                self.send_and_await_reply(channel_current, &connect_dispatch);
            },
            .channel_open_ok => {
                const callback = self.callback.connect;
                self.callback = .none;
                callback(self);
            },
            else => unreachable,
        }
    }

    pub fn tx_select(self: *Client, callback: Callback) void {
        assert(self.callback == .none);
        self.callback = .{ .tx_select = callback };

        const method: spec.ServerMethod = .{ .tx_select = .{} };
        method.write(channel_current, self.send_buffer.encoder());
        self.send_and_await_reply(channel_current, &struct {
            fn dispatch(context: *Client, reply: spec.ClientMethod) Decoder.Error!void {
                assert(reply == .tx_select_ok);
                assert(context.callback == .tx_select);
                const tx_select_callback = context.callback.tx_select;
                context.callback = .none;
                tx_select_callback(context);
            }
        }.dispatch);
    }

    pub fn tx_commit(self: *Client, callback: Callback) void {
        assert(self.callback == .none);
        self.callback = .{ .tx_commit = callback };

        const method: spec.ServerMethod = .{ .tx_commit = .{} };
        method.write(channel_current, self.send_buffer.encoder());
        self.send_and_await_reply(channel_current, &struct {
            fn dispatch(context: *Client, reply: spec.ClientMethod) Decoder.Error!void {
                assert(reply == .tx_commit_ok);
                assert(context.callback == .tx_commit);

                const tx_commit_callback = context.callback.tx_commit;
                context.callback = .none;
                tx_commit_callback(context);
            }
        }.dispatch);
    }

    pub fn exchange_declare(
        self: *Client,
        callback: Callback,
        options: ExchangeDeclareOptions,
    ) void {
        assert(self.callback == .none);
        self.callback = .{ .exchange_declare = callback };

        const method: spec.ServerMethod = .{
            .exchange_declare = .{
                .exchange = options.exchange,
                .internal = options.internal,
                .passive = options.passive,
                .durable = options.durable,
                .type = options.type,
                .auto_delete = options.auto_delete,
                .no_wait = false, // Always await the reply.
                .arguments = null,
            },
        };
        method.write(channel_current, self.send_buffer.encoder());
        self.send_and_await_reply(
            channel_current,
            &struct {
                fn dispatch(context: *Client, reply: spec.ClientMethod) Decoder.Error!void {
                    assert(reply == .exchange_declare_ok);
                    assert(context.callback == .exchange_declare);
                    const exchange_declare_callback = context.callback.exchange_declare;
                    context.callback = .none;
                    exchange_declare_callback(context);
                }
            }.dispatch,
        );
    }

    pub fn queue_declare(self: *Client, callback: Callback, options: QueueDeclareOptions) void {
        assert(self.callback == .none);
        self.callback = .{ .queue_declare = callback };

        const method: spec.ServerMethod = .{
            .queue_declare = .{
                .queue = options.queue,
                .passive = options.passive,
                .durable = options.durable,
                .exclusive = options.exclusive,
                .auto_delete = options.auto_delete,
                .no_wait = false, // Always await the reply.
                .arguments = options.arguments.table(),
            },
        };
        method.write(channel_current, self.send_buffer.encoder());
        self.send_and_await_reply(
            channel_current,
            &struct {
                fn dispatch(context: *Client, reply: spec.ClientMethod) Decoder.Error!void {
                    assert(reply == .queue_declare_ok);
                    assert(context.callback == .queue_declare);

                    const queue_declare_callback = context.callback.queue_declare;
                    context.callback = .none;
                    queue_declare_callback(context);
                }
            }.dispatch,
        );
    }

    /// Enqueue a message to be sent by `publish_flush()`.
    pub fn publish_enqueue(self: *Client, options: BasicPublishOptions) void {
        assert(self.callback == .none);

        // To send a message with metadata and payload, the following `Frames` must be written:
        const encoder = self.send_buffer.encoder();

        // 1. Method frame — contains the `Basic.Publish` method arguments.
        const method: spec.ServerMethod = .{ .basic_publish = .{
            .exchange = options.exchange,
            .routing_key = options.routing_key,
            .mandatory = options.mandatory,
            .immediate = options.immediate,
        } };
        method.write(channel_current, encoder);

        // 2. Header frame — contains the `Basic` properties and custom headers.
        const frame_header_ref = encoder.begin_frame(.{
            .type = .header,
            .channel = channel_current,
        });
        const header_ref = encoder.begin_header(.{
            .class = method.method_header().class,
            .weight = 0,
        });
        options.properties.write(encoder);
        encoder.finish_frame(frame_header_ref);

        // 3. Body frame (optional) — contains the message payload.
        //    This could be split into N frames, but we only support single-frame bodies.
        const frame_body_ref = encoder.begin_frame(.{
            .type = .body,
            .channel = channel_current,
        });
        const body_size = if (options.body) |body|
            body.write(encoder.buffer[encoder.index..])
        else
            0;
        if (body_size == 0) {
            // Body size zero, remove the body frame.
            encoder.index = frame_body_ref.index;
            encoder.finish_header(header_ref, 0);
            return;
        }
        encoder.index += body_size;
        encoder.finish_header(header_ref, body_size);
        encoder.finish_frame(frame_body_ref);
    }

    /// Sends all messages enqueued so far by `publish_enqueue()`.
    pub fn publish_send(
        self: *Client,
        callback: Callback,
    ) void {
        assert(self.awaiter == .none);
        assert(self.callback == .none);
        assert(self.send_buffer.state == .writting);
        self.callback = .{ .publish = callback };
        self.send_and_forget(&struct {
            fn dispatch(context: *Client) void {
                assert(context.callback == .publish);
                const publish_callback = context.callback.publish;
                context.callback = .none;
                publish_callback(context);
            }
        }.dispatch);
    }

    /// Uses a polling model to retrieve a message (`Basic.Get`).
    /// The callback is invoked with either `null` properties if the queue is empty,
    /// or with the properties of the first available message.
    /// N.B.: The message body is not retrieved.
    pub fn get_message(
        self: *Client,
        callback: GetMessagePropertiesCallback,
        options: GetMessageOptions,
    ) void {
        assert(self.callback == .none);
        self.callback = .{ .get_message = callback };

        const method: spec.ServerMethod = .{ .basic_get = .{
            .queue = options.queue,
            .no_ack = options.no_ack,
        } };
        method.write(channel_current, self.send_buffer.encoder());
        self.send_and_await_reply(channel_current, &get_message_dispatch);
    }

    fn get_message_dispatch(self: *Client, reply: spec.ClientMethod) Decoder.Error!void {
        assert(self.callback == .get_message);
        switch (reply) {
            .basic_get_empty => {
                const get_header_callback = self.callback.get_message;
                self.callback = .none;
                try get_header_callback(self, 0, null);
            },
            .basic_get_ok => |get_ok| self.awaiter = .{ .await_message_header = .{
                .delivery_tag = get_ok.delivery_tag,
                .channel = channel_current,
                .callback = &struct {
                    fn dispatch(
                        context: *Client,
                        delivery_tag: u64,
                        header: Decoder.Header,
                    ) Decoder.Error!void {
                        assert(context.callback == .get_message);
                        const properties = try Decoder.BasicProperties.read(
                            header.property_flags,
                            header.properties,
                        );
                        const get_message_callback = context.callback.get_message;
                        context.callback = .none;
                        try get_message_callback(context, delivery_tag, properties);
                    }
                }.dispatch,
            } },
            else => unreachable,
        }
    }

    /// Rejects a message.
    pub fn nack(self: *Client, callback: Callback, options: BasicNackOptions) void {
        assert(self.awaiter == .none);
        assert(self.callback == .none);
        self.callback = .{ .nack = callback };

        const method: spec.ServerMethod = .{ .basic_nack = .{
            .delivery_tag = options.delivery_tag,
            .requeue = options.requeue,
            .multiple = options.multiple,
        } };
        method.write(channel_current, self.send_buffer.encoder());
        self.send_and_forget(&struct {
            fn dispatch(context: *Client) void {
                assert(context.callback == .nack);
                const nack_callback = context.callback.nack;
                context.callback = .none;
                nack_callback(context);
            }
        }.dispatch);
    }

    fn send_and_await_reply(
        self: *Client,
        channel: u16,
        callback: *const fn (self: *Client, reply: spec.ClientMethod) Decoder.Error!void,
    ) void {
        assert(self.awaiter == .none);
        assert(self.send_buffer.state == .writting);
        self.awaiter = .{ .send_and_await_reply = .{
            .channel = channel,
            .state = .sending,
            .callback = callback,
        } };
        self.send();
    }

    fn send_and_forget(
        self: *Client,
        callback: *const fn (self: *Client) void,
    ) void {
        assert(self.awaiter == .none);
        assert(self.send_buffer.state == .writting);
        self.awaiter = .{ .send_and_forget = callback };
        self.send();
    }

    fn send(self: *Client) void {
        switch (self.awaiter) {
            .send_and_forget,
            .send_and_await_reply,
            => {
                self.io.send(
                    *Client,
                    self,
                    send_callback,
                    &self.send_completion,
                    self.fd,
                    self.send_buffer.flush(),
                );
            },
            .none, .await_message_header => unreachable,
        }
    }

    fn send_callback(
        self: *Client,
        completion: *IO.Completion,
        result: IO.SendError!usize,
    ) void {
        _ = completion;
        assert(self.awaiter == .send_and_forget or self.awaiter == .send_and_await_reply);
        const size = result catch |err| vsr.fatal(
            .amqp,
            "Network error: {s}",
            .{@errorName(err)},
        );
        if (self.send_buffer.remaining(size)) |remaining| {
            return self.io.send(
                *Client,
                self,
                send_callback,
                &self.send_completion,
                self.fd,
                remaining,
            );
        }
        assert(self.send_buffer.state == .idle);

        switch (self.awaiter) {
            .send_and_forget => |callback| {
                self.awaiter = .none;
                callback(self);
            },
            .send_and_await_reply => |*awaiter| {
                assert(awaiter.state == .sending);
                awaiter.state = .awaiting;
            },
            .none, .await_message_header => unreachable,
        }
    }

    fn receive(self: *Client) void {
        assert(self.fd != IO.INVALID_SOCKET);
        assert(self.receive_buffer.state == .idle);
        self.io.recv(
            *Client,
            self,
            receive_callback,
            &self.receive_completion,
            self.fd,
            self.receive_buffer.begin_receive(),
        );
    }

    fn receive_callback(
        self: *Client,
        completion: *IO.Completion,
        result: IO.RecvError!usize,
    ) void {
        _ = completion;
        assert(self.receive_buffer.state == .receiving);

        const size: usize = result catch |err| vsr.fatal(.amqp, "Network error: {}.", .{err});
        // No bytes received means that the AMQP server closed the connection.
        if (size == 0) vsr.fatal(
            .amqp,
            "The server closed the connection unexpectedly.",
            .{},
        );

        var decoder = self.receive_buffer.end_receive(size);
        assert(decoder.buffer.len > 0);
        var processed_index_last: usize = 0;
        while (!decoder.empty()) {
            self.process(&decoder) catch |err| switch (err) {
                error.BufferExhausted => {
                    // The buffer ended before the entire frame could be parsed.
                    break;
                },
                error.Unexpected => vsr.fatal(
                    .amqp,
                    "Invalid command received.",
                    .{},
                ),
            };
            processed_index_last = decoder.index;
        }
        assert(processed_index_last <= decoder.buffer.len);
        const receive_buffer = self.receive_buffer.end_decode(processed_index_last);

        self.io.recv(
            *Client,
            self,
            receive_callback,
            &self.receive_completion,
            self.fd,
            receive_buffer,
        );
    }

    fn process(self: *Client, decoder: *Decoder) Decoder.Error!void {
        const frame_header = try decoder.read_frame_header();
        switch (frame_header.type) {
            .method => {
                const method_header = try decoder.read_method_header();
                try self.process_method(frame_header, method_header, decoder);
            },
            .header => {
                const header = try decoder.read_header(frame_header.size);
                try self.process_header(frame_header, header);
            },
            .body => {
                const body = try decoder.read_body(frame_header.size);
                try self.process_body(frame_header, body);
            },
            .heartbeat => {
                try decoder.read_frame_end();
                self.send_heartbeat();
            },
        }
    }

    fn process_method(
        self: *Client,
        frame_header: Decoder.FrameHeader,
        method_header: protocol.MethodHeader,
        decoder: *Decoder,
    ) Decoder.Error!void {
        assert(frame_header.type == .method);
        const client_method = try spec.ClientMethod.read(method_header, decoder);
        switch (client_method) {
            inline .connection_close, .channel_close => |close_reason, tag| {
                const error_code: ErrorCodes = @enumFromInt(close_reason.reply_code);
                if (std.meta.intToEnum(
                    spec.ServerMethod.Tag,
                    @as(u32, @bitCast(protocol.MethodHeader{
                        .class = close_reason.class_id,
                        .method = close_reason.method_id,
                    })),
                ) catch null) |server_method| {
                    vsr.fatal(
                        .amqp,
                        "Operation cannot be completed: method={s} {s}={s}",
                        .{
                            @tagName(server_method),
                            @tagName(error_code),
                            close_reason.reply_text,
                        },
                    );
                } else {
                    vsr.fatal(
                        .amqp,
                        switch (tag) {
                            .connection_close => "Connection closed: {s}={s}",
                            .channel_close => "Channel closed: {s}={s}",
                            else => comptime unreachable,
                        },
                        .{
                            @tagName(error_code),
                            close_reason.reply_text,
                        },
                    );
                }
            },
            .basic_return => |basic_return| {
                const soft_error: ErrorCodes = @enumFromInt(basic_return.reply_code);
                vsr.fatal(
                    .amqp,
                    "Message cannot be delivered: exchange=\"{s}\" routing_key=\"{s}\" {s}={s}",
                    .{
                        basic_return.exchange,
                        basic_return.routing_key,
                        @tagName(soft_error),
                        basic_return.reply_text,
                    },
                );
            },
            // Channel flow is not supported, but the command can be ignored.
            // It's up to the server to evict clients that do not respect
            // the flow control directive.
            .channel_flow => |channel_flow| return log.warn(
                "Channel flow ignored: active={}",
                .{channel_flow.active},
            ),
            .connection_blocked,
            .connection_unblocked,
            .basic_deliver,
            .basic_ack,
            .basic_nack,
            => vsr.fatal(
                .amqp,
                "AMQP operation not supported: {s} channel={}",
                .{ @tagName(client_method), frame_header.channel },
            ),
            else => {},
        }

        switch (self.awaiter) {
            .send_and_await_reply => |awaiter| {
                if (awaiter.channel == frame_header.channel) {
                    assert(awaiter.state == .awaiting);
                    self.awaiter = .none;
                    return try awaiter.callback(self, client_method);
                }
            },
            else => {},
        }

        vsr.fatal(
            .amqp,
            "Unexpected AMQP method: {s} channel={}",
            .{ @tagName(client_method), frame_header.channel },
        );
    }

    fn process_header(
        self: *Client,
        frame_header: Decoder.FrameHeader,
        header: Decoder.Header,
    ) Decoder.Error!void {
        assert(frame_header.type == .header);
        switch (self.awaiter) {
            .await_message_header => |awaiter| {
                // We don't support reading the message body.
                if (frame_header.channel == awaiter.channel and header.body_size == 0) {
                    self.awaiter = .none;
                    return try awaiter.callback(
                        self,
                        awaiter.delivery_tag,
                        header,
                    );
                }
            },
            else => {},
        }

        vsr.fatal(
            .amqp,
            "Unexpected message header: channel={} class={} body_size={}",
            .{
                frame_header.channel,
                header.class,
                header.body_size,
            },
        );
    }

    fn process_body(
        self: *Client,
        frame_header: Decoder.FrameHeader,
        body: []const u8,
    ) Decoder.Error!void {
        _ = self;
        assert(frame_header.type == .body);
        vsr.fatal(
            .amqp,
            "Unexpected message body: channel={} body_size={}",
            .{
                frame_header.channel,
                body.len,
            },
        );
    }

    fn send_heartbeat(self: *Client) void {
        assert(self.fd != IO.INVALID_SOCKET);
        if (self.heartbeat == .sending) return;

        log.info("Heartbeat", .{});

        const heartbeat_message: [8]u8 = comptime heartbeat: {
            var buffer: [8]u8 = undefined;
            var encoder = Encoder.init(&buffer);
            const frame_reference = encoder.begin_frame(.{
                .type = .heartbeat,
                .channel = channel_global,
            });
            encoder.finish_frame(frame_reference);
            assert(encoder.index == buffer.len);
            break :heartbeat buffer;
        };

        assert(self.heartbeat == .idle);
        self.heartbeat = .{ .sending = undefined };
        self.io.send(
            *Client,
            self,
            on_heartbeat_callback,
            &self.heartbeat.sending,
            self.fd,
            &heartbeat_message,
        );
    }

    fn on_heartbeat_callback(
        self: *Client,
        completion: *IO.Completion,
        result: IO.SendError!usize,
    ) void {
        assert(self.heartbeat == .sending);
        self.heartbeat = .idle;
        _ = completion;
        _ = result catch |err| vsr.fatal(.amqp, "Network error: {}", .{err});
    }

    pub fn tick(self: *Client) void {
        _ = self;
    }
};

const ReceiveBuffer = struct {
    buffer: []u8,
    state: union(enum) {
        idle,
        receiving: struct {
            non_consumed: usize,
        },
        decoding: struct {
            size: usize,
        },
    } = .idle,

    fn begin_receive(self: *ReceiveBuffer) []u8 {
        switch (self.state) {
            .idle => {
                self.state = .{
                    .receiving = .{ .non_consumed = 0 },
                };
                return self.buffer;
            },
            .decoding, .receiving => unreachable,
        }
    }

    fn end_receive(self: *ReceiveBuffer, size: usize) Decoder {
        assert(size > 0);
        switch (self.state) {
            .idle, .decoding => unreachable,
            .receiving => |receive_state| {
                const total_size = size + receive_state.non_consumed;
                assert(total_size <= self.buffer.len);
                maybe(receive_state.non_consumed == 0);
                self.state = .{ .decoding = .{ .size = total_size } };
                return Decoder.init(self.buffer[0..total_size]);
            },
        }
    }

    fn end_decode(self: *ReceiveBuffer, processed_last_index: usize) []u8 {
        maybe(processed_last_index == 0);
        switch (self.state) {
            .idle, .receiving => unreachable,
            .decoding => |decoding_state| {
                if (processed_last_index == decoding_state.size) {
                    self.state = .{
                        .receiving = .{ .non_consumed = 0 },
                    };
                    return self.buffer;
                }

                assert(processed_last_index < decoding_state.size);
                const remaining = self.buffer[processed_last_index..decoding_state.size];
                assert(remaining.len < self.buffer.len);
                stdx.copy_left(.inexact, u8, self.buffer, remaining);
                self.state = .{
                    .receiving = .{ .non_consumed = remaining.len },
                };
                return self.buffer[remaining.len..];
            },
        }
    }
};

const SendBuffer = struct {
    buffer: []u8,
    state: union(enum) {
        idle,
        writting: Encoder,
        sending: struct {
            size: usize,
            progress: usize,
        },
    } = .idle,

    fn encoder(self: *SendBuffer) *Encoder {
        switch (self.state) {
            .idle => {
                self.state = .{ .writting = Encoder.init(self.buffer) };
                return &self.state.writting;
            },
            .writting => |*current| return current,
            .sending => unreachable,
        }
    }

    fn flush(self: *SendBuffer) []const u8 {
        switch (self.state) {
            .idle, .sending => unreachable,
            .writting => |*current| {
                assert(current.index > 0);
                const size = current.index;
                self.state = .{ .sending = .{
                    .size = size,
                    .progress = 0,
                } };

                return self.buffer[0..size];
            },
        }
    }

    fn remaining(self: *SendBuffer, progress: usize) ?[]const u8 {
        switch (self.state) {
            .idle, .writting => unreachable,
            .sending => |*send_state| {
                send_state.progress += progress;
                if (send_state.progress == send_state.size) {
                    self.state = .idle;
                    return null;
                }

                assert(send_state.progress < send_state.size);
                return self.buffer[send_state.progress..send_state.size];
            },
        }
    }
};

fn log_table(name: []const u8, table: Decoder.Table) Decoder.Error!void {
    var iterator = table.iterator();
    while (try iterator.next()) |entry| {
        switch (entry.value) {
            .string,
            => |str| log.info("{s} {s}:{s}", .{
                name,
                entry.key,
                str,
            }),
            .field_table => |field_table| try log_table(entry.key, field_table),
            inline else => |any| log.info("{s} {s}:{any}", .{
                name,
                entry.key,
                any,
            }),
        }
    }
}
