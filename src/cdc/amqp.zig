const std = @import("std");

const stdx = @import("../stdx.zig");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const log = std.log.scoped(.amqp);

const vsr = @import("../vsr.zig");
const constants = vsr.constants;
const IO = vsr.io.IO;

const spec = @import("./amqp/spec.zig");
const protocol = @import("./amqp/protocol.zig");
const types = @import("./amqp/types.zig");
const fatal = protocol.fatal;

const ErrorCodes = protocol.ErrorCodes;

pub const Decoder = protocol.Decoder;
pub const Encoder = protocol.Encoder;

pub const ConnectOptions = types.ConnectOptions;
pub const ConnectionProperties = types.ConnectionProperties;
pub const ExchangeDeclareOptions = types.ExchangeDeclareOptions;
pub const QueueDeclareOptions = types.QueueDeclareOptions;
pub const QueueDeclareArguments = types.QueueDeclareArguments;
pub const BasicPublishOptions = types.BasicPublishOptions;
pub const GetMessagePropertiesResult = types.GetMessagePropertiesResult;
pub const GetMessageOptions = types.GetMessageOptions;
pub const BasicNackOptions = types.BasicNackOptions;

pub const tcp_port_default = 5672;
pub const frame_min_size = spec.FRAME_MIN_SIZE;

/// AMQP (Advanced Message Queuing Protocol) 0.9.1 client.
/// - Uses TigerBeetle's IO interface.
/// - Single channel only.
/// - Batched publishing with fixed buffers.
/// - Limited consumer capabilities.
/// - Implements only the methods required by TigerBeetle.
/// - No error handling: **CAN PANIC**.
pub const Client = struct {
    /// The channel number is 0 for all frames which are global to the connection.
    const channel_global = 0;

    // Id of the current channel.
    // For TigerBeetle, supporting multiple channels is unnecessary,
    // as messages are submitted in batches through io_uring without concurrency.
    const channel_current = 1;

    pub const Callback = *const fn (self: *Client) void;
    pub const GetMessagePropertiesCallback = *const fn (
        self: *Client,
        result: ?GetMessagePropertiesResult,
    ) Decoder.Error!void;

    io: *IO,
    fd: IO.socket_t = IO.INVALID_SOCKET,
    frame_size_max: u32,
    reply_timeout_ticks: u64,

    receive_buffer: ReceiveBuffer,
    receive_completion: IO.Completion = undefined,

    send_buffer: SendBuffer,
    send_completion: IO.Completion = undefined,

    heartbeat: union(enum) {
        idle,
        sending: IO.Completion,
    } = .idle,

    action: union(enum) {
        none,
        connect: struct {
            options: ConnectOptions,
            callback: Callback,
        },
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
            state: union(enum) {
                sending,
                awaiting: struct {
                    duration_ticks: u64 = 0,
                },
            },
            callback: *const fn (self: *Client, reply: spec.ClientMethod) Decoder.Error!void,
        },
        /// Invokes the callback when an AMQP header frame is received, containing information
        /// about the incoming message.
        /// Invariant: the send buffer must be empty.
        await_content_header: struct {
            channel: u16,
            duration_ticks: u64 = 0,
            delivery_tag: u64,
            message_count: u32,
            callback: *const fn (
                self: *Client,
                delivery_tag: u64,
                message_count: u32,
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
            reply_timeout_ticks: u64,
        },
    ) !Client {
        assert(options.message_count_max > 0);
        assert(options.message_body_size_max > 0);

        // The worst-case size required to write a frame containing the message body.
        const body_frame_size = Encoder.FrameHeader.SIZE +
            options.message_body_size_max +
            @sizeOf(protocol.FrameEnd);

        const frame_size = @max(frame_min_size, body_frame_size);

        // Large messages are not expected, but we must be able to receive at least
        // the same frame size we send.
        const receive_buffer = try allocator.alloc(u8, frame_size);
        errdefer allocator.free(receive_buffer);

        // When publishing messages, the method and header frames (including metadata)
        // are sent in addition to the body frame.
        // TODO: The size could be calculated more efficiently if the variable data had
        // a known size, otherwise, it’s constrained only by the maximum frame size.
        const send_buffer_size = 3 * frame_size * options.message_count_max;
        const send_buffer = try allocator.alloc(u8, send_buffer_size);
        errdefer allocator.free(send_buffer);

        return .{
            .io = options.io,
            .receive_buffer = ReceiveBuffer.init(receive_buffer),
            .send_buffer = SendBuffer.init(send_buffer),
            .frame_size_max = frame_size,
            .reply_timeout_ticks = options.reply_timeout_ticks,
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
        assert(self.action == .none);
        self.action = .{ .connect = .{
            .options = options,
            .callback = callback,
        } };

        self.fd = try self.io.open_socket_tcp(
            options.address.any.family,
            .{
                // Keeping the default value.
                // Large buffers can cause latency issues.
                .sndbuf = 0,
                .rcvbuf = 0,
                .keepalive = if (constants.tcp_keepalive) .{
                    .keepidle = constants.tcp_keepidle,
                    .keepintvl = constants.tcp_keepintvl,
                    .keepcnt = constants.tcp_keepcnt,
                } else null,
                .user_timeout_ms = constants.tcp_user_timeout_ms,
                .nodelay = constants.tcp_nodelay,
            },
        );
        errdefer self.io.close_socket(self.fd);

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
                    _ = result catch fatal("Connection refused.", .{});

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
        assert(self.action == .connect);
        const connection_options = self.action.connect.options;

        switch (reply) {
            .connection_start => |args| {
                log.info("Connection start received:", .{});
                log.info("version {}.{}", .{ args.version_major, args.version_minor });
                log.info("locales {s}", .{args.locales});
                log.info("mechanisms {s}", .{args.mechanisms});
                try log_table("server_properties", args.server_properties);

                const plain_auth: types.SASLPlainAuth = .{
                    .user_name = connection_options.user_name,
                    .password = connection_options.password,
                };
                const method: spec.ServerMethod = .{ .connection_start_ok = .{
                    .client_properties = connection_options.properties.table(),
                    .mechanism = types.SASLPlainAuth.mechanism,
                    .response = plain_auth.response(),
                    .locale = connection_options.locale orelse first: {
                        var iterator = std.mem.splitScalar(u8, args.locales, ' ');
                        break :first iterator.next().?;
                    },
                } };

                const encoder = self.send_buffer.encoder();
                method.encode(channel_global, encoder);
                self.send_and_await_reply(channel_global, &connect_dispatch);
            },
            .connection_secure => fatal(
                "Connection secure not supported.",
                .{},
            ),
            .connection_tune => |args| {
                log.info("Connection tune received:", .{});
                log.info("channel_max {}", .{args.channel_max});
                log.info("frame_max {}", .{args.frame_max});
                log.info("heartbeat {}", .{args.heartbeat});

                const encoder = self.send_buffer.encoder();

                // Since `tune-ok` has no reply (send-and-forget),
                // we can flush it together with `open`.
                const method_tune_ok: spec.ServerMethod = .{ .connection_tune_ok = .{
                    .channel_max = 1,
                    .frame_max = @max(frame_min_size, self.frame_size_max),
                    .heartbeat = connection_options.heartbeat orelse args.heartbeat,
                } };
                method_tune_ok.encode(channel_global, encoder);

                const method_open: spec.ServerMethod = .{ .connection_open = .{
                    .virtual_host = connection_options.vhost,
                } };
                method_open.encode(channel_global, encoder);
                self.send_and_await_reply(channel_global, &connect_dispatch);
            },
            .connection_open_ok => {
                const method: spec.ServerMethod = .{ .channel_open = .{} };
                method.encode(channel_current, self.send_buffer.encoder());
                self.send_and_await_reply(channel_current, &connect_dispatch);
            },
            .channel_open_ok => {
                const callback = self.action.connect.callback;
                self.action = .none;
                callback(self);
            },
            else => unreachable,
        }
    }

    pub fn tx_select(self: *Client, callback: Callback) void {
        assert(self.action == .none);
        self.action = .{ .tx_select = callback };

        const method: spec.ServerMethod = .{ .tx_select = .{} };
        method.encode(channel_current, self.send_buffer.encoder());
        self.send_and_await_reply(channel_current, &struct {
            fn dispatch(context: *Client, reply: spec.ClientMethod) Decoder.Error!void {
                assert(reply == .tx_select_ok);
                assert(context.action == .tx_select);
                const tx_select_callback = context.action.tx_select;
                context.action = .none;
                tx_select_callback(context);
            }
        }.dispatch);
    }

    pub fn tx_commit(self: *Client, callback: Callback) void {
        assert(self.action == .none);
        self.action = .{ .tx_commit = callback };

        const method: spec.ServerMethod = .{ .tx_commit = .{} };
        method.encode(channel_current, self.send_buffer.encoder());
        self.send_and_await_reply(channel_current, &struct {
            fn dispatch(context: *Client, reply: spec.ClientMethod) Decoder.Error!void {
                assert(reply == .tx_commit_ok);
                assert(context.action == .tx_commit);

                const tx_commit_callback = context.action.tx_commit;
                context.action = .none;
                tx_commit_callback(context);
            }
        }.dispatch);
    }

    pub fn exchange_declare(
        self: *Client,
        callback: Callback,
        options: ExchangeDeclareOptions,
    ) void {
        assert(self.action == .none);
        self.action = .{ .exchange_declare = callback };

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
        method.encode(channel_current, self.send_buffer.encoder());
        self.send_and_await_reply(
            channel_current,
            &struct {
                fn dispatch(context: *Client, reply: spec.ClientMethod) Decoder.Error!void {
                    assert(reply == .exchange_declare_ok);
                    assert(context.action == .exchange_declare);
                    const exchange_declare_callback = context.action.exchange_declare;
                    context.action = .none;
                    exchange_declare_callback(context);
                }
            }.dispatch,
        );
    }

    pub fn queue_declare(self: *Client, callback: Callback, options: QueueDeclareOptions) void {
        assert(self.action == .none);
        self.action = .{ .queue_declare = callback };

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
        method.encode(channel_current, self.send_buffer.encoder());
        self.send_and_await_reply(
            channel_current,
            &struct {
                fn dispatch(context: *Client, reply: spec.ClientMethod) Decoder.Error!void {
                    assert(reply == .queue_declare_ok);
                    assert(context.action == .queue_declare);

                    const queue_declare_callback = context.action.queue_declare;
                    context.action = .none;
                    queue_declare_callback(context);
                }
            }.dispatch,
        );
    }

    /// Enqueue a message to be sent by `publish_flush()`.
    pub fn publish_enqueue(self: *Client, options: BasicPublishOptions) void {
        assert(self.action == .none);

        // To send a message with metadata and payload, the following `Frames` must be written:
        const encoder = self.send_buffer.encoder();

        // 1. Method frame — contains the `Basic.Publish` method arguments.
        const method: spec.ServerMethod = .{ .basic_publish = .{
            .exchange = options.exchange,
            .routing_key = options.routing_key,
            .mandatory = options.mandatory,
            .immediate = options.immediate,
        } };
        method.encode(channel_current, encoder);

        // 2. Header frame — contains the `Basic` properties and custom headers.
        const frame_header_ref = encoder.begin_frame(.{
            .type = .header,
            .channel = channel_current,
        });
        const header_ref = encoder.begin_header(.{
            .class = method.method_header().class,
            .weight = 0,
        });
        options.properties.encode(encoder);
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
        assert(self.action == .none);
        assert(self.send_buffer.state == .writting);
        self.action = .{ .publish = callback };
        self.send_and_forget(&struct {
            fn dispatch(context: *Client) void {
                assert(context.action == .publish);
                const publish_callback = context.action.publish;
                context.action = .none;
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
        assert(self.action == .none);
        self.action = .{ .get_message = callback };

        const method: spec.ServerMethod = .{ .basic_get = .{
            .queue = options.queue,
            .no_ack = options.no_ack,
        } };
        method.encode(channel_current, self.send_buffer.encoder());
        self.send_and_await_reply(channel_current, &get_message_dispatch);
    }

    fn get_message_dispatch(self: *Client, reply: spec.ClientMethod) Decoder.Error!void {
        assert(self.action == .get_message);
        switch (reply) {
            .basic_get_empty => {
                const get_header_callback = self.action.get_message;
                self.action = .none;
                try get_header_callback(self, null);
            },
            .basic_get_ok => |get_ok| self.awaiter = .{ .await_content_header = .{
                .channel = channel_current,
                .delivery_tag = get_ok.delivery_tag,
                .message_count = get_ok.message_count,
                .callback = &struct {
                    fn dispatch(
                        context: *Client,
                        delivery_tag: u64,
                        message_count: u32,
                        header: Decoder.Header,
                    ) Decoder.Error!void {
                        assert(context.action == .get_message);
                        const properties = try Decoder.BasicProperties.decode(
                            header.property_flags,
                            header.properties,
                        );
                        const get_message_callback = context.action.get_message;
                        context.action = .none;
                        try get_message_callback(context, .{
                            .delivery_tag = delivery_tag,
                            .message_count = message_count,
                            .properties = properties,
                        });
                    }
                }.dispatch,
            } },
            else => unreachable,
        }
    }

    /// Rejects a message.
    pub fn nack(self: *Client, callback: Callback, options: BasicNackOptions) void {
        assert(self.awaiter == .none);
        assert(self.action == .none);
        self.action = .{ .nack = callback };

        const method: spec.ServerMethod = .{ .basic_nack = .{
            .delivery_tag = options.delivery_tag,
            .requeue = options.requeue,
            .multiple = options.multiple,
        } };
        method.encode(channel_current, self.send_buffer.encoder());
        self.send_and_forget(&struct {
            fn dispatch(context: *Client) void {
                assert(context.action == .nack);
                const nack_callback = context.action.nack;
                context.action = .none;
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
            .none, .await_content_header => unreachable,
        }
    }

    fn send_callback(
        self: *Client,
        completion: *IO.Completion,
        result: IO.SendError!usize,
    ) void {
        _ = completion;
        assert(self.awaiter == .send_and_forget or self.awaiter == .send_and_await_reply);
        const size = result catch |err| fatal(
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
                awaiter.state = .{ .awaiting = .{} };
            },
            .none, .await_content_header => unreachable,
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

        const size: usize = result catch |err| fatal("Network error: {}.", .{err});
        // No bytes received means that the AMQP server closed the connection.
        if (size == 0) fatal(
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
                error.Unexpected => fatal(
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
        const client_method = try spec.ClientMethod.decode(method_header, decoder);
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
                    fatal(
                        "Operation cannot be completed: method={s} {s}={s}",
                        .{
                            @tagName(server_method),
                            @tagName(error_code),
                            close_reason.reply_text,
                        },
                    );
                } else {
                    fatal(
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
                fatal(
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
            => fatal(
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

        fatal(
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
            .await_content_header => |awaiter| {
                // We don't support reading the message body.
                if (frame_header.channel == awaiter.channel and header.body_size == 0) {
                    self.awaiter = .none;
                    return try awaiter.callback(
                        self,
                        awaiter.delivery_tag,
                        awaiter.message_count,
                        header,
                    );
                }
            },
            else => {},
        }

        fatal(
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
        fatal(
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
        _ = result catch |err| fatal("Network error: {}", .{err});
    }

    pub fn tick(self: *Client) void {
        const duration_ticks: u64 = switch (self.awaiter) {
            .none, .send_and_forget => return,
            .send_and_await_reply => |*awaiter| ticks: {
                if (awaiter.state == .sending) return;
                assert(awaiter.state == .awaiting);
                awaiter.state.awaiting.duration_ticks += 1;
                break :ticks awaiter.state.awaiting.duration_ticks;
            },
            .await_content_header => |*awaiter| ticks: {
                awaiter.duration_ticks += 1;
                break :ticks awaiter.duration_ticks;
            },
        };
        assert(self.action != .none);
        if (duration_ticks > self.reply_timeout_ticks) {
            fatal(
                "Operation {s} timed out. No reply received from the AMQP server.",
                .{@tagName(self.action)},
            );
        }
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
    },

    fn init(buffer: []u8) ReceiveBuffer {
        assert(buffer.len >= frame_min_size);
        return .{
            .buffer = buffer,
            .state = .idle,
        };
    }

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
    },

    fn init(buffer: []u8) SendBuffer {
        assert(buffer.len >= frame_min_size);
        return .{
            .buffer = buffer,
            .state = .idle,
        };
    }

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

    fn remaining(self: *SendBuffer, written_bytes: usize) ?[]const u8 {
        switch (self.state) {
            .idle, .writting => unreachable,
            .sending => |*send_state| {
                send_state.progress += written_bytes;
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

const testing = std.testing;

test "amqp: SendBuffer" {
    const buffer = try testing.allocator.alloc(u8, frame_min_size);
    defer testing.allocator.free(buffer);

    var prng: stdx.PRNG = stdx.PRNG.from_seed(42);
    var send_buffer = SendBuffer.init(buffer);
    for (0..4096) |_| {
        const Element = u64;
        const element_count = prng.range_inclusive(
            usize,
            1,
            @divExact(buffer.len, @sizeOf(Element)),
        );
        // Zero the unused memory so we can assert it wasn't modified by the encoder.
        @memset(buffer[element_count * @sizeOf(Element) ..], 0);

        try testing.expect(send_buffer.state == .idle);
        for (0..element_count) |index| {
            var encoder = send_buffer.encoder();
            try testing.expect(send_buffer.state == .writting);
            try testing.expectEqual(index * @sizeOf(Element), encoder.index);

            var element: Element = undefined;
            prng.fill(std.mem.asBytes(&element));
            encoder.write_int(Element, element);
        }

        const flush_slice = send_buffer.flush();
        try testing.expect(send_buffer.state == .sending);
        try testing.expectEqual(element_count * @sizeOf(Element), flush_slice.len);
        try testing.expectEqualSlices(
            u8,
            buffer[0 .. element_count * @sizeOf(Element)],
            flush_slice,
        );
        try testing.expect(stdx.zeroed(buffer[element_count * @sizeOf(Element) ..]));

        var progress: usize = 0;
        while (progress < flush_slice.len) {
            const remaining_count = flush_slice.len - progress;
            const written = prng.range_inclusive(usize, 1, remaining_count);
            progress += written;

            if (send_buffer.remaining(written)) |remaining_slice| {
                try testing.expectEqual(flush_slice.len - progress, remaining_slice.len);
                try testing.expectEqualSlices(
                    u8,
                    flush_slice[progress..],
                    remaining_slice,
                );
            } else {
                try testing.expect(send_buffer.state == .idle);
                try testing.expectEqual(flush_slice.len, progress);
            }
        }
    }
}

test "amqp: ReceiveBuffer" {
    const ratio = stdx.PRNG.ratio;

    const buffer = try testing.allocator.alloc(u8, frame_min_size);
    defer testing.allocator.free(buffer);

    var receive_buffer = ReceiveBuffer.init(buffer);
    try testing.expect(receive_buffer.state == .idle);

    const receive_slice = receive_buffer.begin_receive();
    try testing.expect(receive_buffer.state == .receiving);
    try testing.expectEqual(buffer.len, receive_slice.len);

    var prng = stdx.PRNG.from_seed(42);
    prng.fill(receive_slice);

    var decoded_remain: usize = 0;
    for (0..4096) |_| {
        const receive_size: usize = prng.range_inclusive(usize, 1, buffer.len - decoded_remain);
        const size = receive_size + decoded_remain;

        const decoder = receive_buffer.end_receive(receive_size);
        try testing.expect(receive_buffer.state == .decoding);
        try testing.expectEqual(size, decoder.buffer.len);
        try testing.expectEqualSlices(u8, buffer[0..size], decoder.buffer);

        const decoded_count = if (prng.chance(ratio(10, 100))) size else prng.range_inclusive(
            usize,
            1,
            size,
        );
        decoded_remain = size - decoded_count;
        const receive_slice_next = receive_buffer.end_decode(decoded_count);
        try testing.expect(receive_buffer.state == .receiving);
        try testing.expectEqual(buffer.len - decoded_remain, receive_slice_next.len);
        try testing.expectEqualSlices(u8, buffer[decoded_remain..], receive_slice_next);
    }
}
