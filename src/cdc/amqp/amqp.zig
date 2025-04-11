const std = @import("std");
const builtin = @import("builtin");

const stdx = @import("../../stdx.zig");
const assert = std.debug.assert;

const vsr = @import("../../vsr.zig");
const IO = vsr.io.IO;

const protocol = @import("./protocol/protocol.zig");
const spec = @import("./protocol/spec.zig");
const Reader = protocol.Reader;
const Writer = protocol.Writer;
const Table = protocol.Table;
const Error = protocol.Error;

const log = std.log.scoped(.amqp);

pub const BasicProperties = protocol.BasicProperties;

/// AMQP (Advanced Message Queuing Protocol) 0.9.1 client.
/// A very opinionated and minimalistic implementation:
///
/// - Uses TigerBeetle's IO interface.
/// - Batched publishing with zero-copy and fixed buffers.
/// - Single channel only.
/// - No error handling, may panic.
pub const Client = struct {
    /// The channel number is 0 for all frames which are global to the connection.
    const channel_global = 0;

    // Id of the current channel.
    // For TigerBeetle, supporting multiple channels is unnecessary,
    // as messages are submitted in batches through io_uring without concurrency.
    const channel_current = 1;

    /// The maximum size in bytes of the custom headers carried with the AMQP message,
    /// including both keys and values.
    const amqp_custom_headers_size_max = 128;

    const Callback = *const fn (self: *Client) void;

    pub const ConnectOptions = struct {
        address: std.net.Address,
        user_name: []const u8,
        password: []const u8,
        vhost: []const u8,
        locale: ?[]const u8 = null,
        heartbeat: ?u16 = null,
    };

    pub const DeliveryMode = protocol.DeliveryMode;

    pub const PublishOptions = struct {
        exchange: []const u8,
        routing_key: []const u8,
        mandatory: bool,
        immediate: bool,
        content_type: ?[]const u8 = null,
        type: ?[]const u8 = null,
        timestamp: ?u64 = null,
        delivery_mode: ?DeliveryMode,
    };

    pub const DeclareOptions = struct {
        queue: []const u8,
        passive: bool,
        durable: bool,
        exclusive: bool,
        auto_delete: bool,

        /// How long a queue can be unused for before it is automatically deleted (milliseconds).
        /// (Sets the "x-expires" argument.)
        expires: ?u32 = null,

        /// How long a message published to a queue can live before it is discarded (milliseconds).
        /// (Sets the "x-message-ttl" argument.)
        message_ttl: ?u32 = null,

        /// Sets the queue overflow behaviour.
        /// This determines what happens to messages when the maximum length of a queue is reached.
        overflow: ?enum {
            @"drop-head",
            @"reject-publish",
            @"reject-publish-dlx",
        },

        /// If set, makes sure only one consumer at a time consumes from the queue and fails over
        /// to another registered consumer in case the active one is cancelled or dies.
        /// (Sets the "x-single-active-consumer" argument.)
        single_active_consumer: ?bool = false,

        /// Optional name of an exchange to which messages will be republished
        /// if they are rejected or expire.
        /// (Sets the "x-dead-letter-exchange" argument.)
        dead_letter_exchange: ?[]const u8 = null,

        /// Optional replacement routing key to use when a message is dead-lettered.
        /// If this is not set, the message's original routing key will be used.
        /// (Sets the "x-dead-letter-routing-key" argument.)
        dead_letter_routing_key: ?[]const u8 = null,

        /// How many (ready) messages a queue can contain before it starts to drop them
        /// from its head.
        /// (Sets the "x-max-length" argument.)
        max_length: ?u32 = null,

        /// Total body size for ready messages a queue can contain before it starts to drop
        /// them from its head.
        /// (Sets the "x-max-length-bytes" argument.)
        max_length_bytes: ?u32 = null,
    };

    const SyncResponse = struct {
        channel: u16,
        state: enum { sending, awaiting } = .sending,
        callback: *const fn (self: *Client, channel: u16, response: spec.ReceiveMethod) Error!void,
    };

    const AsyncResponse = struct {
        channel: u16,
        callback: *const fn (self: *Client, channel: u16) void,
    };

    const ContentResponse = struct {
        channel: u16,
        delivery_tag: u64,
        callback: *const fn (self: *Client, delivery_tag: u64, header: protocol.Header) void,
    };

    pub const GetHeaderCallback = *const fn (self: *Client, delivery_tag: u64, properties: ?BasicProperties) void;

    io: *IO,
    fd: IO.socket_t = IO.INVALID_SOCKET,
    connection_options: ?ConnectOptions,
    frame_size_max: u32,

    recv_progress: usize = 0,
    recv_completion: IO.Completion = undefined,
    recv_buffer: []u8,

    send_buffer: []u8,

    heartbeat: union(enum) {
        idle,
        sending: IO.Completion,
    } = .idle,

    send_context: union(enum) {
        idle,
        writting: u32,
        sending: struct {
            size: u32,
            progress: u32 = 0,
            completion: IO.Completion = undefined,
        },
    } = .idle,

    method: union(enum) {
        none,
        send_and_forget: AsyncResponse,
        send_and_await: SyncResponse,
        await_header: ContentResponse,
    } = .none,

    action: union(enum) {
        idle,
        connect: Callback,
        close: Callback,
        tx_select: Callback,
        tx_commit: Callback,
        tx_rollback: Callback,
        queue_declare: Callback,
        get: GetHeaderCallback,
        nack: Callback,
        publish: Callback,
    } = .idle,

    pub fn init(
        allocator: std.mem.Allocator,
        options: struct {
            io: *IO,
            messages_count_max: u32,
            message_body_size_max: u32,
        },
    ) !Client {
        assert(options.messages_count_max > 0);
        assert(options.message_body_size_max > 0);

        // Large messages are not expected.
        const recv_buffer = try allocator.alloc(u8, spec.FRAME_MIN_SIZE);
        errdefer allocator.free(recv_buffer);

        // The worst-case size required to write a message containing the JSON payload plus
        // the custom key/value headers.
        const frame_size_max =
            protocol.FrameHeader.SIZE + protocol.MethodHeader.SIZE +
            protocol.Header.SIZE + protocol.BasicProperties.SIZE_MAX +
            amqp_custom_headers_size_max + options.message_body_size_max +
            @sizeOf(protocol.FrameEnd);

        // Rounded up to a multiple of 1 KiB.
        const send_buffer_size = stdx.div_ceil(
            frame_size_max * options.messages_count_max,
            1024,
        ) * 1024;

        const send_buffer = try allocator.alloc(u8, send_buffer_size);
        errdefer allocator.free(send_buffer);

        return .{
            .io = options.io,
            .connection_options = null,
            .recv_buffer = recv_buffer,
            .send_buffer = send_buffer,
            .frame_size_max = frame_size_max,
        };
    }

    pub fn deinit(
        self: *Client,
        allocator: std.mem.Allocator,
    ) void {
        allocator.free(self.recv_buffer);
        allocator.free(self.send_buffer);
    }

    pub fn connect(self: *Client, callback: Callback, options: ConnectOptions) !void {
        assert(self.fd == IO.INVALID_SOCKET);
        assert(self.method == .none);
        assert(self.send_context == .idle);
        assert(self.action == .idle);
        self.action = .{ .connect = callback };
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
                    _ = result catch {
                        // TODO: error handling.
                        return context.terminate();
                    };
                    // Start receiving and send the AMQP protocol header:
                    context.recv();
                    const protocol_header: spec.SendMethod = .protocol_header;
                    context.write_method(channel_global, protocol_header);
                    context.send_sync(.{
                        .channel = channel_global,
                        .callback = &connect_dispatch,
                    });
                }
            }.continuation,
            &self.recv_completion,
            self.fd,
            options.address,
        );
    }

    fn connect_dispatch(self: *Client, channel: u16, method: spec.ReceiveMethod) Error!void {
        assert(self.method == .none);
        assert(self.send_context == .idle);
        assert(self.action == .connect);
        switch (method) {
            .connection_start => |args| {
                assert(channel == channel_global);

                log.info("Connection start received:", .{});
                log.info("version {}.{}", .{ args.version_major, args.version_minor });
                log.info("locales {s}", .{args.locales});
                log.info("mechanisms {s}", .{args.mechanisms});
                try log_table("server_properties", args.server_properties);

                var properties_buffer: [128]u8 = undefined;
                var properties = protocol.TableWriter.init(&properties_buffer);
                properties.put("product", .{ .string = "TigerBeetle AMQP 0.9.1 Client" });
                properties.put("version", .{ .string = std.fmt.comptimePrint(
                    "{}",
                    .{vsr.constants.state_machine_config.release},
                ) });

                // By AMQP convention, "platform" refers to the technology the client was built on,
                // e.g., Erlang, Java, Go, etc.
                properties.put("platform", .{ .string = "Zig " ++ builtin.zig_version_string });

                var response_buffer: [255]u8 = undefined;
                const response: []const u8 = std.fmt.bufPrint(&response_buffer, "\x00{s}\x00{s}", .{
                    self.connection_options.?.user_name,
                    self.connection_options.?.password,
                }) catch {
                    log.err("Username and password are too long.", .{});
                    return error.Unexpected;
                };

                const start_ok: spec.SendMethod = .{ .connection_start_ok = .{
                    .client_properties = properties.table(),
                    .mechanism = "PLAIN",
                    .response = response,
                    .locale = self.connection_options.?.locale orelse first: {
                        var iterator = std.mem.splitScalar(u8, args.locales, ' ');
                        break :first iterator.next().?;
                    },
                } };
                self.write_method(channel_global, start_ok);
                self.send_sync(.{
                    .channel = channel_global,
                    .callback = &connect_dispatch,
                });
            },
            .connection_secure => unreachable, // Not implemented,
            .connection_tune => |args| {
                assert(channel == channel_global);

                log.info("Connection tune received:", .{});
                log.info("channel_max {}", .{args.channel_max});
                log.info("frame_max {}", .{args.frame_max});
                log.info("heartbeat {}", .{args.heartbeat});

                const tune_ok: spec.SendMethod = .{ .connection_tune_ok = .{
                    .channel_max = 1,
                    .frame_max = @max(spec.FRAME_MIN_SIZE, self.frame_size_max),
                    .heartbeat = self.connection_options.?.heartbeat orelse args.heartbeat,
                } };
                assert(tune_ok.response() == null);
                self.write_method(channel_global, tune_ok);

                const open: spec.SendMethod = .{ .connection_open = .{
                    .virtual_host = self.connection_options.?.vhost,
                } };
                self.write_method(channel_global, open);
                self.send_sync(.{
                    .channel = channel_global,
                    .callback = &connect_dispatch,
                });
            },
            .connection_open_ok => {
                assert(channel == channel_global);

                const channel_open: spec.SendMethod = .{ .channel_open = .{} };
                self.write_method(channel_current, channel_open);
                self.send_sync(.{
                    .channel = channel_current,
                    .callback = &connect_dispatch,
                });
            },
            .channel_open_ok => {
                assert(channel == channel_current);

                const callback = self.action.connect;
                self.action = .idle;
                callback(self);
            },
            else => unreachable,
        }
    }

    pub fn close(self: *Client, callback: Callback) void {
        assert(self.action == .idle);
        self.action = .{ .close = callback };

        const connection_close: spec.SendMethod = .{ .connection_close = .{
            .reply_code = 0,
            .reply_text = "",
            .class_id = 0,
            .method_id = 0,
        } };
        self.write_method(channel_global, connection_close);
        self.send_sync(.{
            .channel = channel_global,
            .response_tag = connection_close.response(),
            .callback = &struct {
                fn continuation(context: *Client, channel: u16, method: spec.ReceiveMethod) Error!void {
                    assert(channel == 0);
                    assert(method == .connection_close_ok);
                    assert(context.action == .close);

                    log.info("Connection close_ok received:", .{});
                    const close_callback = context.action.close;
                    context.action = .idle;

                    close_callback(context);
                }
            }.continuation,
        });
    }

    pub fn tx_select(self: *Client, callback: Callback) void {
        assert(self.action == .idle);
        self.action = .{ .tx_select = callback };

        const method: spec.SendMethod = .{ .tx_select = .{} };
        self.write_method(channel_current, method);
        self.send_sync(.{
            .channel = channel_current,
            .callback = struct {
                fn continuation(
                    context: *Client,
                    response_channel: u16,
                    response: spec.ReceiveMethod,
                ) Error!void {
                    assert(response == .tx_select_ok);
                    assert(context.action == .tx_select);
                    assert(response_channel == channel_current);

                    const tx_select_callback = context.action.tx_select;
                    context.action = .idle;
                    tx_select_callback(context);
                }
            }.continuation,
        });
    }

    pub fn tx_commit(self: *Client, callback: Callback) void {
        assert(self.action == .idle);
        self.action = .{ .tx_commit = callback };

        const method: spec.SendMethod = .{ .tx_commit = .{} };
        self.write_method(channel_current, method);
        self.send_sync(.{
            .channel = channel_current,
            .callback = struct {
                fn continuation(
                    context: *Client,
                    response_channel: u16,
                    response: spec.ReceiveMethod,
                ) Error!void {
                    assert(response == .tx_commit_ok);
                    assert(context.action == .tx_commit);
                    assert(response_channel == channel_current);

                    const tx_commit_callback = context.action.tx_commit;
                    context.action = .idle;
                    tx_commit_callback(context);
                }
            }.continuation,
        });
    }

    pub fn tx_rollback(self: *Client, callback: Callback) void {
        assert(self.action == .idle);
        self.action = .{ .tx_rollback = callback };

        const method: spec.SendMethod = .{ .tx_rollback = .{} };
        self.write_method(channel_current, method);
        self.send_sync(.{
            .channel = channel_current,
            .callback = struct {
                fn continuation(
                    context: *Client,
                    response_channel: u16,
                    response: spec.ReceiveMethod,
                ) Error!void {
                    assert(response == .tx_rollback_ok);
                    assert(context.action == .tx_rollback);
                    assert(response_channel == channel_current);

                    const tx_rollback_callback = context.action.tx_rollback;
                    context.action = .idle;
                    tx_rollback_callback(context);
                }
            }.continuation,
        });
    }

    pub fn declare(self: *Client, callback: Callback, options: DeclareOptions) void {
        assert(self.action == .idle);
        self.action = .{ .queue_declare = callback };

        var arguments_buffer: [amqp_custom_headers_size_max]u8 = undefined;
        var arguments = protocol.TableWriter.init(&arguments_buffer);
        if (options.expires) |expires| {
            arguments.put("x-expires", .{ .long_uint = expires });
        }
        if (options.message_ttl) |message_ttl| arguments.put(
            "x-message-ttl",
            .{ .long_uint = message_ttl },
        );
        if (options.overflow) |overflow| arguments.put(
            "x-overflow",
            .{ .string = @tagName(overflow) },
        );
        if (options.single_active_consumer) |single_active_consumer| arguments.put(
            "x-single-active-consumer",
            .{ .boolean = single_active_consumer },
        );
        if (options.dead_letter_exchange) |dead_letter_exchange| arguments.put(
            "x-dead-letter-exchange",
            .{ .string = dead_letter_exchange },
        );
        if (options.dead_letter_routing_key) |dead_letter_routing_key| arguments.put(
            "x-dead-letter-routing-key",
            .{ .string = dead_letter_routing_key },
        );
        if (options.max_length) |max_length| arguments.put(
            "x-max-length",
            .{ .long_uint = max_length },
        );
        if (options.max_length_bytes) |max_length_bytes| arguments.put(
            "x-max-length-bytes",
            .{ .long_uint = max_length_bytes },
        );

        const method: spec.SendMethod = .{ .queue_declare = .{
            .queue = options.queue,
            .passive = options.passive,
            .durable = options.durable,
            .exclusive = options.exclusive,
            .auto_delete = options.auto_delete,
            .no_wait = false,
            .arguments = arguments.table(),
        } };
        self.write_method(channel_current, method);
        self.send_sync(.{
            .channel = channel_current,
            .callback = struct {
                fn continuation(
                    context: *Client,
                    response_channel: u16,
                    response: spec.ReceiveMethod,
                ) Error!void {
                    assert(response == .queue_declare_ok);
                    assert(context.action == .queue_declare);
                    assert(response_channel == channel_current);

                    const queue_declare_callback = context.action.queue_declare;
                    context.action = .idle;
                    queue_declare_callback(context);
                }
            }.continuation,
        });
    }

    /// Enqueue a message to be sent by `publish_flush()`.
    pub fn publish_enqueue(
        self: *Client,
        comptime Message: type,
        comptime CustomHeaders: type,
        message: Message,
        custom_headers: CustomHeaders,
        comptime render_body: fn (
            message: Message,
            output_buffer: []u8,
        ) usize,
        options: PublishOptions,
    ) void {
        assert(self.action == .idle);
        const method: spec.SendMethod = .{ .basic_publish = .{
            .exchange = options.exchange,
            .routing_key = options.routing_key,
            .mandatory = options.mandatory,
            .immediate = options.immediate,
        } };
        self.write_method(channel_current, method);

        const index: u32 = switch (self.send_context) {
            .writting => |index| index,
            .idle, .sending => unreachable,
        };
        assert(index < self.send_buffer.len);

        var writer = Writer.init(self.send_buffer[index..]);
        defer self.send_context = .{ .writting = @intCast(index + writer.index) };

        const header_frame_ref = writer.begin_frame(.{
            .type = .header,
            .channel = channel_current,
            .size = 0,
        });

        var headers_buffer: [amqp_custom_headers_size_max]u8 = undefined;
        var headers = protocol.TableWriter.init(&headers_buffer);
        inline for (std.meta.fields(CustomHeaders)) |field| {
            headers.put(field.name, switch (field.type) {
                []const u8 => .{ .string = @field(custom_headers, field.name) },
                u64 => .{ .long_long_uint = @field(custom_headers, field.name) },
                u32 => .{ .long_uint = @field(custom_headers, field.name) },
                u16 => .{ .short_uint = @field(custom_headers, field.name) },
                else => comptime unreachable,
            });
        }

        const header_ref = writer.begin_header(.{
            .class = spec.Basic.class,
            .weight = 0,
            .properties = .{
                .type = options.type,
                .content_type = options.content_type,
                .headers = headers.table(),
                .delivery_mode = options.delivery_mode,
                .timestamp = options.timestamp,
                .app_id = "tigerbeetle",
            },
        });
        writer.finish_frame(header_frame_ref);
        const body_frame_ref = writer.begin_frame(.{
            .type = .body,
            .channel = channel_current,
            .size = 0,
        });
        const body_size = render_body(
            message,
            writer.buffer[writer.index..],
        );
        if (body_size == 0) {
            // Remove the body frame.
            writer.index = body_frame_ref.index;
            writer.finish_header(header_ref, 0);
            return;
        }

        writer.index += body_size;
        writer.finish_header(header_ref, body_size);
        writer.finish_frame(body_frame_ref);
    }

    /// Sends all messages enqueued so far by `publish_enqueue()`.
    pub fn publish_flush(
        self: *Client,
        callback: Callback,
    ) void {
        assert(self.action == .idle);
        self.action = .{ .publish = callback };
        self.send_async(.{
            .channel = channel_current,
            .callback = struct {
                fn continuation(
                    context: *Client,
                    response_channel: u16,
                ) void {
                    assert(context.action == .publish);
                    assert(response_channel == channel_current);

                    const publish_callback = context.action.publish;
                    context.action = .idle;
                    publish_callback(context);
                }
            }.continuation,
        });
    }

    pub fn nack(self: *Client, callback: Callback, options: struct {
        delivery_tag: u64,
    }) void {
        assert(self.action == .idle);
        self.action = .{ .nack = callback };

        const method: spec.SendMethod = .{ .basic_nack = .{
            .delivery_tag = options.delivery_tag,
            .multiple = false,
            .requeue = true,
        } };
        self.write_method(channel_current, method);
        self.send_async(.{
            .channel = channel_current,
            .callback = struct {
                fn continuation(
                    context: *Client,
                    response_channel: u16,
                ) void {
                    assert(context.action == .nack);
                    assert(response_channel == channel_current);

                    const nack_callback = context.action.nack;
                    context.action = .idle;
                    nack_callback(context);
                }
            }.continuation,
        });
    }

    pub fn get_header(
        self: *Client,
        callback: GetHeaderCallback,
        options: struct {
            queue: []const u8,
        },
    ) void {
        assert(self.action == .idle);
        self.action = .{ .get = callback };

        const method: spec.SendMethod = .{ .basic_get = .{
            .queue = options.queue,
            .no_ack = false,
        } };
        self.write_method(channel_current, method);
        self.send_sync(.{
            .channel = channel_current,
            .callback = struct {
                fn continuation(
                    context: *Client,
                    response_channel: u16,
                    response: spec.ReceiveMethod,
                ) Error!void {
                    assert(context.action == .get);
                    assert(response_channel == channel_current);
                    switch (response) {
                        .basic_get_empty => {
                            const get_callback = context.action.get;
                            context.action = .idle;
                            get_callback(context, 0, null);
                        },
                        .basic_get_ok => |get_ok| context.method = .{ .await_header = .{
                            .delivery_tag = get_ok.delivery_tag,
                            .channel = response_channel,
                            .callback = &get_header_callback,
                        } },
                        else => unreachable,
                    }
                }
            }.continuation,
        });
    }

    fn get_header_callback(self: *Client, delivery_tag: u64, header: protocol.Header) void {
        assert(self.action == .get);

        const get_callback = self.action.get;
        self.action = .idle;
        get_callback(self, delivery_tag, header.basic_properties());
    }

    fn write_method(
        self: *Client,
        channel: u16,
        method: spec.SendMethod,
    ) void {
        const index: u32 = switch (self.send_context) {
            .idle => 0,
            .writting => |index| index,
            .sending => unreachable,
        };
        assert(index < self.send_buffer.len);

        var writer = Writer.init(self.send_buffer[index..]);
        defer self.send_context = .{ .writting = @intCast(index + writer.index) };

        method.write(channel, &writer);
    }

    fn send_sync(
        self: *Client,
        response: SyncResponse,
    ) void {
        assert(self.method == .none);
        assert(self.send_context == .writting);

        self.method = .{ .send_and_await = response };
        self.send();
    }

    fn send_async(
        self: *Client,
        response: AsyncResponse,
    ) void {
        assert(self.method == .none);
        assert(self.send_context == .writting);

        self.method = .{ .send_and_forget = response };
        self.send();
    }

    fn send(self: *Client) void {
        const slice: []const u8 = switch (self.send_context) {
            .idle => unreachable,
            .writting => |index| slice: {
                self.send_context = .{ .sending = .{ .size = index } };
                break :slice self.send_buffer[0..index];
            },
            .sending => |*sending| self.send_buffer[sending.progress..sending.size],
        };
        self.io.send(
            *Client,
            self,
            send_callback,
            &self.send_context.sending.completion,
            self.fd,
            slice,
        );
    }

    fn send_callback(
        self: *Client,
        completion: *IO.Completion,
        result: IO.SendError!usize,
    ) void {
        switch (self.send_context) {
            .sending => |*context| {
                _ = completion;
                context.progress += @intCast(result catch {
                    // TODO: error handling.
                    return self.terminate();
                });
                if (context.progress < context.size) return self.send();
                assert(context.progress == context.size);
                self.send_context = .idle;

                switch (self.method) {
                    .none => unreachable,
                    .send_and_await => |*response| response.state = .awaiting,
                    .send_and_forget => |response| {
                        self.method = .none;
                        response.callback(self, response.channel);
                    },
                    .await_header => unreachable,
                }
            },
            .idle, .writting => unreachable,
        }
    }

    fn recv(self: *Client) void {
        assert(self.fd != IO.INVALID_SOCKET);
        assert(self.recv_progress == 0);
        self.io.recv(
            *Client,
            self,
            recv_callback,
            &self.recv_completion,
            self.fd,
            self.recv_buffer,
        );
    }

    fn recv_callback(
        self: *Client,
        completion: *IO.Completion,
        result: IO.RecvError!usize,
    ) void {
        _ = completion;

        const buffer_available: usize = buffer_available: {
            const read: usize = result catch return self.terminate();
            // No bytes received means that the AMQP server closed the connection.
            if (read == 0) return self.terminate();
            break :buffer_available self.recv_progress + read;
        };

        var reader = Reader.init(self.recv_buffer[0..buffer_available]);
        var processed_index: usize = 0;

        while (reader.remaining_size() > 0) {
            self.process(&reader) catch |err| switch (err) {
                error.BufferExhausted => break,
                error.Unexpected => unreachable,
            };
            processed_index = reader.index;
        }

        const remaining = buffer_available - processed_index;
        if (processed_index < buffer_available) {
            stdx.copy_left(.inexact, u8, self.recv_buffer[0..], self.recv_buffer[remaining..buffer_available]);
        }
        self.recv_progress = remaining;
        self.io.recv(
            *Client,
            self,
            recv_callback,
            &self.recv_completion,
            self.fd,
            self.recv_buffer[self.recv_progress..],
        );
    }

    fn process(self: *Client, reader: *Reader) Error!void {
        const frame_header = try reader.read_frame_header();
        switch (frame_header.type) {
            .method => {
                const method_header = try reader.read_method_header();
                try self.process_method(frame_header, method_header, reader);
            },
            .header => {
                const header = try reader.read_header(frame_header.size);
                try self.process_header(frame_header, header);
            },
            .body => {
                const body = try reader.read_body(frame_header.size);
                try self.process_body(frame_header, body);
            },
            .heartbeat => {
                try reader.read_frame_end();
                self.send_heartbeat();
            },
        }
    }

    fn terminate(self: *Client) void {
        assert(self.fd != IO.INVALID_SOCKET);
        self.io.close_socket(self.fd);
        self.fd = IO.INVALID_SOCKET;
    }

    pub fn tick(self: *Client) void {
        _ = self;
    }

    fn process_method(
        self: *Client,
        frame_header: protocol.FrameHeader,
        method_header: protocol.MethodHeader,
        reader: *Reader,
    ) Error!void {
        const method = try spec.ReceiveMethod.read(method_header, reader);

        switch (method) {
            .channel_flow => |flow| {
                log.err(
                    "FLOW RECEIVED: Start/Stop publishing {}",
                    .{flow.active},
                );
            },
            .connection_close, .channel_close => {
                log.err(
                    "TODO: Handle server side close",
                    .{},
                );
            },
            else => switch (self.method) {
                .send_and_await => |response| {
                    assert(response.state == .awaiting);
                    assert(response.channel == frame_header.channel);
                    self.method = .none;
                    try response.callback(self, response.channel, method);
                },
                .none,
                .send_and_forget,
                => unreachable,
                .await_header,
                => unreachable,
            },
        }
    }

    fn process_header(
        self: *Client,
        frame_header: protocol.FrameHeader,
        header: protocol.Header,
    ) Error!void {
        assert(self.method == .await_header);
        const await_header = self.method.await_header;
        self.method = .none;

        assert(frame_header.channel == await_header.channel);
        assert(header.body_size == 0); // We don't support reading the message body.
        await_header.callback(self, await_header.delivery_tag, header);
    }

    fn process_body(
        self: *Client,
        frame_header: protocol.FrameHeader,
        body: []const u8,
    ) Error!void {
        _ = self;
        _ = frame_header;
        _ = body;
        // Reading the message body is not required.
        unreachable;
    }

    fn send_heartbeat(self: *Client) void {
        assert(self.fd != IO.INVALID_SOCKET);
        if (self.heartbeat == .sending) return;

        log.info("Heartbeat", .{});

        const heartbeat_message: [8]u8 = comptime heartbeat: {
            var buffer: [8]u8 = undefined;
            var writer = Writer.init(&buffer);
            const frame_reference = writer.begin_frame(.{
                .type = .heartbeat,
                .channel = 0,
                .size = 0,
            });
            writer.finish_frame(frame_reference);
            assert(writer.index == buffer.len);
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
        _ = result catch {
            self.terminate();
        };
    }
};

fn log_table(name: []const u8, table: Table) Error!void {
    var it = table.iterator();
    while (try it.next()) |kv| {
        switch (kv.value) {
            .string,
            => |str| log.info("{s} {s}:{s}", .{
                name,
                kv.key,
                str,
            }),
            .field_table => |field_table| try log_table(kv.key, field_table),
            inline else => |any| log.info("{s} {s}:{any}", .{
                name,
                kv.key,
                any,
            }),
        }
    }
}
