const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.amqp_transport);

const vsr = @import("../vsr.zig");
const transport_mod = @import("transport.zig");
const Transport = transport_mod.Transport;
const CommonConfig = transport_mod.CommonConfig;
const amqp = @import("amqp.zig");

/// AMQP transport implementation for CDC.
///
/// Uses RabbitMQ-compatible AMQP 0.9.1 protocol.
/// - Distributed lock: Exclusive queue (auto-deleted on disconnect)
/// - Progress tracking: Durable queue with max-length=1
/// - Publishing: Exchange with routing key, publisher confirms
pub const AmqpTransport = struct {
    allocator: std.mem.Allocator,
    client: amqp.Client,
    config: Config,

    // State
    connected: bool = false,
    lock_acquired: bool = false,

    // Pending callbacks
    connect_callback: ?*const fn (*anyopaque, bool) void = null,
    connect_callback_ctx: ?*anyopaque = null,
    lock_callback: ?*const fn (*anyopaque, bool) void = null,
    lock_callback_ctx: ?*anyopaque = null,
    progress_callback: ?*const fn (*anyopaque, ?Transport.ProgressState) void = null,
    progress_callback_ctx: ?*anyopaque = null,
    publish_callback: ?*const fn (*anyopaque, bool) void = null,
    publish_callback_ctx: ?*anyopaque = null,
    update_progress_callback: ?*const fn (*anyopaque, bool) void = null,
    update_progress_callback_ctx: ?*anyopaque = null,

    // Queue names (generated from cluster_id)
    progress_queue: []const u8,
    locker_queue: []const u8,

    pub const Config = struct {
        /// AMQP host address.
        host: std.net.Address,
        /// AMQP user name.
        user: []const u8,
        /// AMQP password.
        password: []const u8,
        /// AMQP vhost.
        vhost: []const u8,
        /// Exchange for publishing.
        publish_exchange: []const u8,
        /// Routing key for publishing.
        publish_routing_key: []const u8,
        /// Common transport config.
        common: CommonConfig,
    };

    const vtable: Transport.VTable = .{
        .connect = connect,
        .acquireLock = acquireLock,
        .getProgress = getProgress,
        .publishEnqueue = publishEnqueue,
        .publishSend = publishSend,
        .updateProgress = updateProgress,
        .tick = tick,
        .isConnected = isConnected,
        .deinit = deinit,
    };

    pub fn init(allocator: std.mem.Allocator, io: *vsr.io.IO, config: Config) !AmqpTransport {
        const progress_queue = try std.fmt.allocPrint(
            allocator,
            "tigerbeetle.internal.progress.{}",
            .{config.common.cluster_id},
        );
        errdefer allocator.free(progress_queue);

        const locker_queue = try std.fmt.allocPrint(
            allocator,
            "tigerbeetle.internal.locker.{}",
            .{config.common.cluster_id},
        );
        errdefer allocator.free(locker_queue);

        const client = try amqp.Client.init(allocator, .{
            .io = io,
            .message_count_max = config.common.message_count_max,
            .message_body_size_max = config.common.message_body_size_max,
            .reply_timeout_ticks = config.common.reply_timeout_ticks,
        });

        return .{
            .allocator = allocator,
            .client = client,
            .config = config,
            .progress_queue = progress_queue,
            .locker_queue = locker_queue,
        };
    }

    pub fn transport(self: *AmqpTransport) Transport {
        return .{
            .ptr = self,
            .vtable = &vtable,
        };
    }

    fn connect(ptr: *anyopaque, callback: *const fn (*anyopaque, bool) void, ctx: *anyopaque) void {
        const self: *AmqpTransport = @ptrCast(@alignCast(ptr));
        self.connect_callback = callback;
        self.connect_callback_ctx = ctx;

        self.client.connect(
            &struct {
                fn cb(client: *amqp.Client) void {
                    const transport_ptr: *AmqpTransport = @alignCast(
                        @fieldParentPtr("client", client),
                    );
                    transport_ptr.connected = true;
                    if (transport_ptr.connect_callback) |cb_fn| {
                        cb_fn(transport_ptr.connect_callback_ctx.?, true);
                    }
                }
            }.cb,
            .{
                .host = self.config.host,
                .user_name = self.config.user,
                .password = self.config.password,
                .vhost = self.config.vhost,
            },
        ) catch {
            if (self.connect_callback) |cb_fn| {
                cb_fn(self.connect_callback_ctx.?, false);
            }
        };
    }

    fn acquireLock(
        ptr: *anyopaque,
        callback: *const fn (*anyopaque, bool) void,
        ctx: *anyopaque,
    ) void {
        const self: *AmqpTransport = @ptrCast(@alignCast(ptr));
        self.lock_callback = callback;
        self.lock_callback_ctx = ctx;

        // Declare exclusive queue - fails if another instance has it
        self.client.queue_declare(
            &struct {
                fn cb(client: *amqp.Client) void {
                    const transport_ptr: *AmqpTransport = @alignCast(
                        @fieldParentPtr("client", client),
                    );
                    transport_ptr.lock_acquired = true;
                    if (transport_ptr.lock_callback) |cb_fn| {
                        cb_fn(transport_ptr.lock_callback_ctx.?, true);
                    }
                }
            }.cb,
            .{
                .queue = self.locker_queue,
                .passive = false,
                .durable = false,
                .exclusive = true,
                .auto_delete = true,
                .arguments = .{},
            },
        );
    }

    fn getProgress(
        ptr: *anyopaque,
        callback: *const fn (*anyopaque, ?Transport.ProgressState) void,
        ctx: *anyopaque,
    ) void {
        const self: *AmqpTransport = @ptrCast(@alignCast(ptr));
        self.progress_callback = callback;
        self.progress_callback_ctx = ctx;

        // First declare the progress queue
        self.client.queue_declare(
            &struct {
                fn cb(client: *amqp.Client) void {
                    const transport_ptr: *AmqpTransport = @alignCast(
                        @fieldParentPtr("client", client),
                    );
                    // Then try to get the message
                    transport_ptr.client.get_message(
                        &struct {
                            fn msg_cb(
                                c: *amqp.Client,
                                result: ?amqp.GetMessagePropertiesResult,
                            ) amqp.Decoder.Error!void {
                                const tp: *AmqpTransport = @alignCast(
                                    @fieldParentPtr("client", c),
                                );
                                if (result) |props| {
                                    // Parse headers for timestamp and release
                                    const progress = parseProgressFromHeaders(
                                        props.properties.headers,
                                    );
                                    if (tp.progress_callback) |cb_fn| {
                                        cb_fn(tp.progress_callback_ctx.?, progress);
                                    }
                                } else {
                                    // No message - start from beginning
                                    if (tp.progress_callback) |cb_fn| {
                                        cb_fn(tp.progress_callback_ctx.?, null);
                                    }
                                }
                            }
                        }.msg_cb,
                        .{
                            .queue = transport_ptr.progress_queue,
                            .no_ack = false,
                        },
                    );
                }
            }.cb,
            .{
                .queue = self.progress_queue,
                .passive = false,
                .durable = true,
                .exclusive = false,
                .auto_delete = false,
                .arguments = .{
                    .overflow = .drop_head,
                    .max_length = 1,
                },
            },
        );
    }

    fn parseProgressFromHeaders(headers: ?amqp.Decoder.Table) ?Transport.ProgressState {
        if (headers) |table| {
            var timestamp: ?u64 = null;
            var release: ?vsr.Release = null;

            var iterator = table.iterator();
            while (iterator.next() catch null) |entry| {
                if (std.mem.eql(u8, entry.key, "timestamp")) {
                    switch (entry.value) {
                        .int64 => |v| timestamp = @intCast(v),
                        else => {},
                    }
                }
                if (std.mem.eql(u8, entry.key, "release")) {
                    switch (entry.value) {
                        .string => |v| release = vsr.Release.parse(v) catch null,
                        else => {},
                    }
                }
            }

            if (timestamp) |ts| {
                return .{
                    .timestamp = ts,
                    .release = release orelse vsr.Release.minimum,
                };
            }
        }
        return null;
    }

    fn publishEnqueue(ptr: *anyopaque, options: Transport.PublishOptions) void {
        const self: *AmqpTransport = @ptrCast(@alignCast(ptr));

        // Create header table
        const header_table = HeaderTable{
            .headers = options.headers,
        };

        // Create body wrapper for slice
        const body_wrapper = BodyWrapper{ .data = options.body };

        self.client.publish_enqueue(.{
            .exchange = self.config.publish_exchange,
            .routing_key = if (self.config.publish_routing_key.len > 0)
                self.config.publish_routing_key
            else
                options.routing_key,
            .mandatory = true,
            .immediate = false,
            .properties = .{
                .content_type = "application/json",
                .delivery_mode = .persistent,
                .app_id = "tigerbeetle",
                .timestamp = @divTrunc(options.timestamp, std.time.ns_per_s),
                .headers = header_table.table(),
            },
            .body = body_wrapper.body(),
        });
    }

    const BodyWrapper = struct {
        data: []const u8,

        fn body(self: *const BodyWrapper) amqp.Encoder.Body {
            const vtable_impl: amqp.Encoder.Body.VTable = .{
                .write = &struct {
                    fn write(context: *const anyopaque, buffer: []u8) usize {
                        const wrapper: *const BodyWrapper = @ptrCast(@alignCast(context));
                        const len = @min(wrapper.data.len, buffer.len);
                        @memcpy(buffer[0..len], wrapper.data[0..len]);
                        return len;
                    }
                }.write,
            };
            return .{ .context = self, .vtable = &vtable_impl };
        }
    };

    const HeaderTable = struct {
        headers: Transport.Headers,

        fn table(self: *const HeaderTable) amqp.Encoder.Table {
            const vtable_impl: amqp.Encoder.Table.VTable = .{
                .write = &struct {
                    fn write(context: *const anyopaque, encoder: *amqp.Encoder.TableEncoder) void {
                        const h: *const HeaderTable = @ptrCast(@alignCast(context));
                        encoder.put("event_type", .{ .string = h.headers.event_type });
                        encoder.put("ledger", .{ .int64 = h.headers.ledger });
                        encoder.put("transfer_code", .{ .int32 = h.headers.transfer_code });
                        encoder.put(
                            "debit_account_code",
                            .{ .int32 = h.headers.debit_account_code },
                        );
                        encoder.put(
                            "credit_account_code",
                            .{ .int32 = h.headers.credit_account_code },
                        );
                    }
                }.write,
            };
            return .{ .context = self, .vtable = &vtable_impl };
        }
    };

    fn publishSend(
        ptr: *anyopaque,
        callback: *const fn (*anyopaque, bool) void,
        ctx: *anyopaque,
    ) void {
        const self: *AmqpTransport = @ptrCast(@alignCast(ptr));
        self.publish_callback = callback;
        self.publish_callback_ctx = ctx;

        self.client.publish_send(&struct {
            fn cb(client: *amqp.Client) void {
                const transport_ptr: *AmqpTransport = @alignCast(
                    @fieldParentPtr("client", client),
                );
                if (transport_ptr.publish_callback) |cb_fn| {
                    cb_fn(transport_ptr.publish_callback_ctx.?, true);
                }
            }
        }.cb);
    }

    fn updateProgress(
        ptr: *anyopaque,
        progress: Transport.ProgressState,
        callback: *const fn (*anyopaque, bool) void,
        ctx: *anyopaque,
    ) void {
        const self: *AmqpTransport = @ptrCast(@alignCast(ptr));
        self.update_progress_callback = callback;
        self.update_progress_callback_ctx = ctx;

        const progress_header = ProgressHeader{ .progress = progress };

        self.client.publish_enqueue(.{
            .exchange = "",
            .routing_key = self.progress_queue,
            .mandatory = true,
            .immediate = false,
            .properties = .{
                .delivery_mode = .persistent,
                .timestamp = @intCast(std.time.milliTimestamp()),
                .headers = progress_header.table(),
            },
            .body = null,
        });

        self.client.publish_send(&struct {
            fn cb(client: *amqp.Client) void {
                const transport_ptr: *AmqpTransport = @alignCast(
                    @fieldParentPtr("client", client),
                );
                if (transport_ptr.update_progress_callback) |cb_fn| {
                    cb_fn(transport_ptr.update_progress_callback_ctx.?, true);
                }
            }
        }.cb);
    }

    const ProgressHeader = struct {
        progress: Transport.ProgressState,

        fn table(self: *const ProgressHeader) amqp.Encoder.Table {
            const vtable_impl: amqp.Encoder.Table.VTable = .{
                .write = &struct {
                    fn write(context: *const anyopaque, encoder: *amqp.Encoder.TableEncoder) void {
                        const h: *const ProgressHeader = @ptrCast(@alignCast(context));
                        var release_buf: [32]u8 = undefined;
                        const release_str = std.fmt.bufPrint(
                            &release_buf,
                            "{}",
                            .{h.progress.release},
                        ) catch "0.0.0";
                        encoder.put("release", .{ .string = release_str });
                        encoder.put("timestamp", .{ .int64 = @intCast(h.progress.timestamp) });
                    }
                }.write,
            };
            return .{ .context = self, .vtable = &vtable_impl };
        }
    };

    fn tick(ptr: *anyopaque) void {
        const self: *AmqpTransport = @ptrCast(@alignCast(ptr));
        self.client.tick();
    }

    fn isConnected(ptr: *anyopaque) bool {
        const self: *AmqpTransport = @ptrCast(@alignCast(ptr));
        return self.connected;
    }

    fn deinit(ptr: *anyopaque, allocator: std.mem.Allocator) void {
        const self: *AmqpTransport = @ptrCast(@alignCast(ptr));
        self.client.deinit(allocator);
        allocator.free(self.progress_queue);
        allocator.free(self.locker_queue);
    }
};
