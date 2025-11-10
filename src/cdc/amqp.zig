const std = @import("std");

const stdx = @import("stdx");
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
const Channel = protocol.Channel;

pub const Decoder = protocol.Decoder;
pub const Encoder = protocol.Encoder;

pub const ConnectOptions = types.ConnectOptions;
pub const ExchangeDeclareOptions = types.ExchangeDeclareOptions;
pub const QueueDeclareOptions = types.QueueDeclareOptions;
pub const BasicPublishOptions = types.BasicPublishOptions;
pub const GetMessagePropertiesResult = types.GetMessagePropertiesResult;
pub const GetMessageOptions = types.GetMessageOptions;
pub const BasicNackOptions = types.BasicNackOptions;

pub const tcp_port_default = protocol.tcp_port_default;
pub const frame_min_size = protocol.frame_min_size;

/// AMQP (Advanced Message Queuing Protocol) 0.9.1 client.
/// - Uses TigerBeetle's IO interface.
/// - Single channel only.
/// - Batched publishing with fixed buffers.
/// - Limited consumer capabilities.
/// - Implements only the methods required by TigerBeetle.
/// - No error handling: **CAN PANIC**.
pub const Client = struct {
    pub const Callback = *const fn (self: *Client) void;
    pub const GetMessagePropertiesCallback = *const fn (
        self: *Client,
        result: ?GetMessagePropertiesResult,
    ) Decoder.Error!void;
    pub const GetMessageBodyCallback = *const fn (
        self: *Client,
        body: []const u8,
    ) Decoder.Error!void;

    io: *IO,
    fd: ?IO.socket_t = null,
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
            phase: enum {
                dial,
                handshake,
                auth,
                connection_open,
                channel_open,
                confirm_select,
            },
            callback: Callback,
        },
        close: Callback,
        queue_declare: Callback,
        exchange_declare: Callback,
        get_message: GetMessagePropertiesCallback,
        message_body_pending: struct {
            body_size: u64,
        },
        get_message_body: struct {
            body_size: u64,
            callback: GetMessageBodyCallback,
        },
        nack: Callback,
        publish_enqueue: struct {
            count: u32 = 0,
        },
        publish: struct {
            callback: Callback,
            phase: enum { sending, awaiting_confirmation } = .sending,
        },
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
            channel: Channel,
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
            channel: Channel,
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
        /// Invokes the callback when an AMQP body frame is received.
        /// Invariant: the send buffer must be empty.
        await_body: struct {
            channel: Channel,
            duration_ticks: u64 = 0,
            callback: *const fn (
                self: *Client,
                body: []const u8,
            ) Decoder.Error!void,
        },
    } = .none,

    publish_confirms: Confirms,

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
        assert(options.reply_timeout_ticks > 0);

        // The worst-case size required to write a frame containing the message body.
        const body_frame_size = Encoder.FrameHeader.size_total +
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

        var publish_confirms: Confirms = try Confirms.init(allocator, options.message_count_max);
        errdefer publish_confirms.deinit(allocator);

        return .{
            .io = options.io,
            .receive_buffer = ReceiveBuffer.init(receive_buffer),
            .send_buffer = SendBuffer.init(send_buffer),
            .reply_timeout_ticks = options.reply_timeout_ticks,
            .publish_confirms = publish_confirms,
        };
    }

    pub fn deinit(
        self: *Client,
        allocator: std.mem.Allocator,
    ) void {
        if (self.fd) |fd| {
            self.io.close_socket(fd);
            self.fd = null;
        }
        self.publish_confirms.deinit(allocator);
        allocator.free(self.send_buffer.buffer);
        allocator.free(self.receive_buffer.buffer);
    }

    pub fn connect(self: *Client, callback: Callback, options: ConnectOptions) !void {
        assert(self.fd == null);
        assert(self.awaiter == .none);
        assert(self.send_buffer.state == .idle);
        assert(self.action == .none);
        self.action = .{ .connect = .{
            .options = options,
            .phase = .dial,
            .callback = callback,
        } };

        self.fd = try self.io.open_socket_tcp(
            options.host.any.family,
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
                    assert(context.action == .connect);
                    assert(context.action.connect.phase == .dial);

                    // Start receiving and send the AMQP protocol header:
                    context.receive();

                    const encoder = context.send_buffer.encoder();
                    encoder.write_bytes(protocol.protocol_header);
                    context.action.connect.phase = .handshake;
                    context.send_and_await_reply(.global, &connect_dispatch);
                }
            }.continuation,
            &self.receive_completion,
            self.fd.?,
            options.host,
        );
    }

    fn connect_dispatch(self: *Client, reply: spec.ClientMethod) Decoder.Error!void {
        assert(self.awaiter == .none);
        assert(self.send_buffer.state == .idle);
        assert(self.action == .connect);
        const connection_options = self.action.connect.options;

        switch (reply) {
            .connection_start => |args| {
                assert(self.action.connect.phase == .handshake);

                log.info("Connection start received:", .{});
                log.info("version {}.{}", .{ args.version_major, args.version_minor });
                log.info("locales {s}", .{args.locales});
                log.info("mechanisms {s}", .{args.mechanisms});
                try log_table("server_properties", args.server_properties);

                if (args.version_major != protocol.version.major or
                    args.version_minor != protocol.version.minor)
                {
                    fatal("Unsuported AMQP server version {}.{}", .{
                        args.version_major,
                        args.version_minor,
                    });
                }

                if (std.mem.indexOfPosLinear(
                    u8,
                    args.mechanisms,
                    0,
                    types.SASLPlainAuth.mechanism,
                ) == null) {
                    fatal(
                        \\AMQP server does not support {s} authentication.
                        \\Supported methods: {s}.
                    , .{
                        types.SASLPlainAuth.mechanism,
                        args.mechanisms,
                    });
                }

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
                method.encode(.global, encoder);
                self.action.connect.phase = .auth;
                self.send_and_await_reply(.global, &connect_dispatch);
            },
            .connection_secure => fatal(
                "Connection secure not supported.",
                .{},
            ),
            .connection_tune => |args| {
                assert(self.action.connect.phase == .auth);

                log.info("Connection tune received:", .{});
                log.info("channel_max {}", .{args.channel_max});
                log.info("frame_max {}", .{args.frame_max});
                log.info("heartbeat {}", .{args.heartbeat});
                // Zero indicates no specified limit.
                assert(args.frame_max == 0 or args.frame_max >= frame_min_size);
                maybe(args.channel_max == 0);
                maybe(args.heartbeat == 0);

                const encoder = self.send_buffer.encoder();

                // Since `tune-ok` has no reply (send-and-forget),
                // we can flush it together with `open`.
                const method_tune_ok: spec.ServerMethod = .{
                    .connection_tune_ok = .{
                        .channel_max = 1,
                        // Don't override `frame_max`. RabbitMQ 4.1 requires frame sizes larger
                        // than those specified in the AMQP spec.
                        // https://www.rabbitmq.com/blog/2025/04/15/rabbitmq-4.1.0-is-released#initial-amqp-0-9-1-maximum-frame-size
                        .frame_max = args.frame_max,
                        .heartbeat = if (args.heartbeat == 0)
                            // Zero means the server does not want a heartbeat.
                            0
                        else
                            connection_options.heartbeat_seconds orelse args.heartbeat,
                    },
                };
                method_tune_ok.encode(.global, encoder);

                const method_open: spec.ServerMethod = .{ .connection_open = .{
                    .virtual_host = connection_options.vhost,
                } };
                method_open.encode(.global, encoder);
                self.action.connect.phase = .connection_open;
                self.send_and_await_reply(.global, &connect_dispatch);
            },
            .connection_open_ok => {
                assert(self.action.connect.phase == .connection_open);

                const method: spec.ServerMethod = .{ .channel_open = .{} };
                method.encode(.current, self.send_buffer.encoder());
                self.action.connect.phase = .channel_open;
                self.send_and_await_reply(.current, &connect_dispatch);
            },
            .channel_open_ok => {
                assert(self.action.connect.phase == .channel_open);

                // Enabling the `confirm` mode on the channel.
                // https://www.rabbitmq.com/docs/confirms#publisher-confirms
                const method: spec.ServerMethod = .{ .confirm_select = .{ .nowait = false } };
                method.encode(.current, self.send_buffer.encoder());
                self.action.connect.phase = .confirm_select;
                self.send_and_await_reply(.current, &connect_dispatch);
            },
            .confirm_select_ok => {
                assert(self.action.connect.phase == .confirm_select);

                const callback = self.action.connect.callback;
                self.action = .none;
                callback(self);
            },
            else => fatal(
                "Unexpected AMQP method received during connection: {s}",
                .{@tagName(reply)},
            ),
        }
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
        method.encode(.current, self.send_buffer.encoder());
        self.send_and_await_reply(
            .current,
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
        method.encode(.current, self.send_buffer.encoder());
        self.send_and_await_reply(
            .current,
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

    /// Enqueue a message to be sent by `publish_send()`.
    pub fn publish_enqueue(self: *Client, options: BasicPublishOptions) void {
        assert(self.awaiter == .none);
        if (self.action == .none) self.action = .{ .publish_enqueue = .{} };

        assert(self.action == .publish_enqueue);
        self.action.publish_enqueue.count += 1;

        // To send a message with metadata and payload, the following `Frames` must be written:
        const encoder = self.send_buffer.encoder();

        // 1. Method frame — contains the `Basic.Publish` method arguments.
        const method: spec.ServerMethod = .{ .basic_publish = .{
            .exchange = options.exchange,
            .routing_key = options.routing_key,
            .mandatory = options.mandatory,
            .immediate = options.immediate,
        } };
        method.encode(.current, encoder);

        // 2. Header frame — contains the `Basic` properties and custom headers.
        encoder.begin_frame(.{
            .type = .header,
            .channel = .current,
        });
        encoder.begin_header(.{
            .class = method.method_header().class,
            .weight = 0,
        });
        options.properties.encode(encoder);
        encoder.finish_frame(.header);

        if (options.body) |body| {
            // 3. Body frame (optional) — contains the message payload.
            //    This could be split into N frames, but we only support single-frame bodies.
            encoder.begin_frame(.{
                .type = .body,
                .channel = .current,
            });
            const body_size = body.write(encoder.buffer[encoder.index..]);
            encoder.index += body_size;
            encoder.finish_header(body_size);
            encoder.finish_frame(.body);
        } else {
            // No body frame.
            encoder.finish_header(0);
        }
    }

    /// Sends all messages enqueued so far by `publish_enqueue()`.
    pub fn publish_send(
        self: *Client,
        callback: Callback,
    ) void {
        assert(self.awaiter == .none);
        assert(self.send_buffer.state == .writing);
        assert(self.action == .publish_enqueue);
        assert(self.action.publish_enqueue.count > 0);

        self.publish_confirms.wait(self.action.publish_enqueue.count);
        self.action = .{ .publish = .{ .callback = callback } };
        self.send_and_forget(&struct {
            fn dispatch(context: *Client) void {
                assert(context.action == .publish);
                assert(context.action.publish.phase == .sending);
                context.action.publish.phase = .awaiting_confirmation;
            }
        }.dispatch);
    }

    /// Uses a polling model to retrieve a message (`Basic.Get`).
    /// The callback is invoked with either `null` properties if the queue is empty,
    /// or with the properties of the first available message.
    /// N.B.: The message body is not retrieved.
    /// The method `get_message_body` **MUST** be called if `has_body == true`.
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
        method.encode(.current, self.send_buffer.encoder());
        self.send_and_await_reply(.current, &get_message_dispatch);
    }

    fn get_message_dispatch(self: *Client, reply: spec.ClientMethod) Decoder.Error!void {
        assert(self.action == .get_message);
        assert(self.awaiter == .none);
        switch (reply) {
            .basic_get_empty => {
                const get_header_callback = self.action.get_message;
                self.action = .none;
                try get_header_callback(self, null);
            },
            .basic_get_ok => |get_ok| self.awaiter = .{ .await_content_header = .{
                .channel = .current,
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
                        assert(header.body_size <= protocol.frame_min_size);
                        const properties = try Decoder.BasicProperties.decode(
                            header.property_flags,
                            header.properties,
                        );
                        const get_message_callback = context.action.get_message;
                        const has_body = header.body_size > 0;
                        context.action = if (has_body) .{
                            .message_body_pending = .{
                                .body_size = header.body_size,
                            },
                        } else .none;
                        try get_message_callback(context, .{
                            .delivery_tag = delivery_tag,
                            .message_count = message_count,
                            .properties = properties,
                            .has_body = has_body,
                        });
                    }
                }.dispatch,
            } },
            else => fatal(
                "Unexpected AMQP method received during get_message: {s}",
                .{@tagName(reply)},
            ),
        }
    }

    pub fn get_message_body(
        self: *Client,
        callback: GetMessageBodyCallback,
    ) void {
        assert(self.action == .message_body_pending);
        assert(self.action.message_body_pending.body_size <= protocol.frame_min_size);
        const body_size = self.action.message_body_pending.body_size;
        self.action = .{ .get_message_body = .{
            .body_size = body_size,
            .callback = callback,
        } };
        self.awaiter = .{ .await_body = .{
            .channel = .current,
            .callback = &struct {
                fn dispatch(
                    context: *Client,
                    body: []const u8,
                ) Decoder.Error!void {
                    assert(context.action == .get_message_body);
                    assert(context.action.get_message_body.body_size == body.len);
                    const get_message_body_callback = context.action.get_message_body.callback;
                    context.action = .none;
                    try get_message_body_callback(context, body);
                }
            }.dispatch,
        } };
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
        method.encode(.current, self.send_buffer.encoder());
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
        channel: Channel,
        callback: *const fn (self: *Client, reply: spec.ClientMethod) Decoder.Error!void,
    ) void {
        assert(self.awaiter == .none);
        assert(self.send_buffer.state == .writing);
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
        assert(self.send_buffer.state == .writing);
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
                    self.fd.?,
                    self.send_buffer.flush(),
                );
            },
            .none, .await_content_header, .await_body => unreachable,
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
                self.fd.?,
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
            .none, .await_content_header, .await_body => unreachable,
        }
    }

    fn receive(self: *Client) void {
        assert(self.fd != null);
        assert(self.receive_buffer.state == .idle);
        self.io.recv(
            *Client,
            self,
            receive_callback,
            &self.receive_completion,
            self.fd.?,
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
            self.fd.?,
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
            => fatal(
                "AMQP operation not supported: {s} channel={}",
                .{ @tagName(client_method), frame_header.channel },
            ),
            .basic_ack => |basic_ack| {
                // Processing acks in "publish confirms" mode.
                if (self.action == .publish) {
                    const publish = &self.action.publish;
                    // Confirmations can be received while sending a batch of messages.
                    if (publish.phase == .sending) assert(self.awaiter == .send_and_forget);
                    if (self.publish_confirms.confirm(basic_ack)) {
                        assert(self.awaiter == .none);
                        assert(publish.phase == .awaiting_confirmation);
                        const publish_callback = publish.callback;
                        self.action = .none;
                        publish_callback(self);
                    }
                    return;
                }
            },
            .basic_nack => fatal(
                "Message was rejected by the AMQP server: {s} channel={}",
                .{ @tagName(client_method), frame_header.channel },
            ),
            else => {},
        }

        switch (self.awaiter) {
            .send_and_await_reply => |awaiter| {
                if (awaiter.state == .awaiting and
                    awaiter.channel == frame_header.channel)
                {
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
        maybe(header.body_size == 0);
        if (self.awaiter == .await_content_header) {
            const awaiter = self.awaiter.await_content_header;
            if (frame_header.channel == awaiter.channel) {
                self.awaiter = .none;
                return try awaiter.callback(
                    self,
                    awaiter.delivery_tag,
                    awaiter.message_count,
                    header,
                );
            }
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
        assert(frame_header.type == .body);
        assert(body.len > 0);
        if (self.awaiter == .await_body) {
            const awaiter = self.awaiter.await_body;
            if (frame_header.channel == awaiter.channel) {
                self.awaiter = .none;
                return try awaiter.callback(
                    self,
                    body,
                );
            }
        }
        fatal(
            "Unexpected message body: channel={} body_size={}",
            .{
                frame_header.channel,
                body.len,
            },
        );
    }

    fn send_heartbeat(self: *Client) void {
        assert(self.fd != null);
        if (self.heartbeat == .sending) return;

        log.info("Heartbeat", .{});

        const heartbeat_message: [8]u8 = comptime heartbeat: {
            var buffer: [8]u8 = undefined;
            var encoder = Encoder.init(&buffer);
            encoder.begin_frame(.{
                .type = .heartbeat,
                .channel = .global,
            });
            encoder.finish_frame(.heartbeat);
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
            self.fd.?,
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
            inline .await_content_header, .await_body => |*awaiter| ticks: {
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
        assert(self.state == .decoding);
        const decoding_state = self.state.decoding;
        if (processed_last_index == decoding_state.size) {
            self.state = .{
                .receiving = .{ .non_consumed = 0 },
            };
            return self.buffer;
        }

        assert(processed_last_index < decoding_state.size);
        const remaining = self.buffer[processed_last_index..decoding_state.size];
        assert(remaining.len < self.buffer.len);
        if (processed_last_index > 0) {
            stdx.copy_left(.inexact, u8, self.buffer, remaining);
        }

        self.state = .{
            .receiving = .{ .non_consumed = remaining.len },
        };
        return self.buffer[remaining.len..];
    }
};

const SendBuffer = struct {
    buffer: []u8,
    state: union(enum) {
        idle,
        writing: Encoder,
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
                self.state = .{ .writing = Encoder.init(self.buffer) };
                return &self.state.writing;
            },
            .writing => |*current| return current,
            .sending => unreachable,
        }
    }

    fn flush(self: *SendBuffer) []const u8 {
        switch (self.state) {
            .idle, .sending => unreachable,
            .writing => |*current| {
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
            .idle, .writing => unreachable,
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
            .string => |str| log.info("{s} {s}:{s}", .{
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

/// Implements the RabbitMQ Publisher Confirms acknowledgment logic.
/// Both the broker and the client count messages.
/// Counting starts at 1 on the first `confirm_select`.
/// https://www.rabbitmq.com/docs/confirms#publisher-confirms
/// https://www.rabbitmq.com/blog/2011/02/10/introducing-publisher-confirms
const Confirms = struct {
    processed: std.DynamicBitSetUnmanaged,
    state: union(enum) {
        idle: struct {
            sequence: u64,
        },
        waiting: struct {
            count: u32,
            sequence_initial: u64,
        },
    },

    fn init(allocator: std.mem.Allocator, capacity: u32) !Confirms {
        assert(capacity > 0);
        const processed = try std.DynamicBitSetUnmanaged.initEmpty(allocator, capacity);
        return .{
            .state = .{ .idle = .{ .sequence = 1 } },
            .processed = processed,
        };
    }

    fn deinit(self: *Confirms, allocator: std.mem.Allocator) void {
        assert(self.state == .idle);
        assert(self.processed.count() == 0);
        self.processed.deinit(allocator);
    }

    /// Waits until `count` published messages have been acknowledged by the server.
    fn wait(self: *Confirms, count: u32) void {
        assert(count > 0);
        assert(count <= self.processed.capacity());
        assert(self.processed.count() == 0);
        assert(self.state == .idle);

        const sequence = self.state.idle.sequence;
        assert(sequence > 0);
        self.state = .{ .waiting = .{
            .count = count,
            .sequence_initial = sequence,
        } };
    }

    /// Confirms that the server has received and fsync'ed a batch of published messages.
    /// Returns `true` if there are no more messages pending acknowledgment.
    fn confirm(self: *Confirms, ack: std.meta.TagPayload(spec.ClientMethod, .basic_ack)) bool {
        assert(self.state == .waiting);

        const state = self.state.waiting;
        assert(state.count > 0);
        assert(state.sequence_initial > 0);

        // The server must not use a zero value for delivery tags.
        // Zero is reserved for client use, meaning "all messages so far received".
        // https://www.rabbitmq.com/docs/specification#rules
        assert(ack.delivery_tag > 0);
        assert(ack.delivery_tag >= state.sequence_initial);
        assert(ack.delivery_tag < state.sequence_initial + state.count);

        const range: std.bit_set.Range = range: {
            const index = ack.delivery_tag - state.sequence_initial;
            // Published messages will be confirmed only once.
            assert(!self.processed.isSet(index));
            const start: usize = start: {
                if (!ack.multiple) break :start index; // Single message.

                // Finds the first unconfirmed delivery tag to acknowledge
                // all pending messages up to `ack.delivery_tag`.
                var iterator = self.processed.iterator(.{
                    .direction = .forward,
                    .kind = .unset,
                });
                const unconfirmed_index = iterator.next().?;
                assert(unconfirmed_index <= index);
                break :start unconfirmed_index;
            };
            break :range .{
                .start = start,
                .end = index + 1, // +1 to be inclusive.
            };
        };
        self.processed.setRangeValue(range, true);

        log.debug("basic_ack: delivery_tag={} multiple={} count={} confirmed={}", .{
            ack.delivery_tag,
            ack.multiple,
            state.count,
            self.processed.count(),
        });

        if (self.processed.count() == state.count) {
            self.processed.unsetAll();
            self.state = .{ .idle = .{
                .sequence = state.sequence_initial + state.count,
            } };
            return true;
        }
        return false;
    }
};

const testing = std.testing;

test "amqp: SendBuffer" {
    const buffer = try testing.allocator.alloc(u8, frame_min_size);
    defer testing.allocator.free(buffer);

    var prng: stdx.PRNG = stdx.PRNG.from_seed_testing();
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
            try testing.expect(send_buffer.state == .writing);
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

    var prng = stdx.PRNG.from_seed_testing();
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

test "amqp: Confirms" {
    // Confirmations can be out of order, for example:
    // Pending tags              Ack
    // [1,2,3,4,5,6,7,8,9,10] -> tag=1  multiple=true
    // [2,3,4,5,6,7,8,9,10]   -> tag=3  multiple=false
    // [2,4,5,6,7,8,9,10]     -> tag=2  multiple=false
    // [4,5,6,7,8,9,10]       -> tag=5  multiple=true
    // [6,7,8,9,10]           -> tag=7  multiple=false
    // [6,8,9,10]             -> tag=10 multiple=true
    // []                     -> finished
    var confirms = try Confirms.init(testing.allocator, 10);
    defer confirms.deinit(testing.allocator);

    try testing.expect(confirms.state == .idle);
    try testing.expect(confirms.state.idle.sequence == 1);

    confirms.wait(10);
    try testing.expect(confirms.state == .waiting);
    try testing.expectEqual(@as(usize, 0), confirms.processed.count());

    try testing.expectEqual(false, confirms.confirm(.{ .delivery_tag = 1, .multiple = true }));
    try testing.expect(confirms.state == .waiting);
    try testing.expectEqual(@as(usize, 1), confirms.processed.count());

    try testing.expectEqual(false, confirms.confirm(.{ .delivery_tag = 3, .multiple = false }));
    try testing.expect(confirms.state == .waiting);
    try testing.expectEqual(@as(usize, 2), confirms.processed.count());

    try testing.expectEqual(false, confirms.confirm(.{ .delivery_tag = 2, .multiple = false }));
    try testing.expect(confirms.state == .waiting);
    try testing.expectEqual(@as(usize, 3), confirms.processed.count());

    try testing.expectEqual(false, confirms.confirm(.{ .delivery_tag = 5, .multiple = true }));
    try testing.expect(confirms.state == .waiting);
    try testing.expectEqual(@as(usize, 5), confirms.processed.count());

    try testing.expectEqual(false, confirms.confirm(.{ .delivery_tag = 7, .multiple = false }));
    try testing.expect(confirms.state == .waiting);
    try testing.expectEqual(@as(usize, 6), confirms.processed.count());

    try testing.expectEqual(true, confirms.confirm(.{ .delivery_tag = 10, .multiple = true }));
    try testing.expect(confirms.state == .idle);
    try testing.expectEqual(@as(usize, 0), confirms.processed.count());
    try testing.expect(confirms.state.idle.sequence == 11);
}

test "amqp: spec" {
    // Sanity check to ensure the spec hasn't been manually modified.
    // Checking the hash to avoid downloading the XML from external sources during CI.
    try testing.expectEqual(
        269315715514333185404011239500341468006,
        vsr.checksum(@embedFile("amqp/spec.zig")),
    );
}
