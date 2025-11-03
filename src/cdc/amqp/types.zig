const std = @import("std");
const builtin = @import("builtin");
const vsr = @import("../../vsr.zig");
const protocol = @import("protocol.zig");
const Encoder = protocol.Encoder;
const Decoder = protocol.Decoder;

pub const ConnectOptions = struct {
    host: std.net.Address,
    user_name: []const u8,
    password: []const u8,
    vhost: []const u8,
    locale: ?[]const u8 = null,
    heartbeat_seconds: ?u16 = null,
    properties: ConnectionProperties = ConnectionProperties.default,
};

pub const ConnectionProperties = struct {
    product: []const u8,
    version: []const u8,
    platform: []const u8,
    capabilities: *const ClientCapabilities,

    pub const default: ConnectionProperties = .{
        .product = "TigerBeetle",
        .version = std.fmt.comptimePrint(
            "{}",
            .{vsr.constants.config.process.release},
        ),
        // By convention, "platform" refers to the programming language.
        // e.g., Erlang, Java, Go, etc.
        .platform = "Zig " ++ builtin.zig_version_string,
        .capabilities = &ClientCapabilities.default,
    };

    pub fn table(self: *const ConnectionProperties) Encoder.Table {
        const vtable: Encoder.Table.VTable = comptime .{
            .write = &struct {
                fn write(context: *const anyopaque, encoder: *Encoder.TableEncoder) void {
                    const properties: *const ConnectionProperties = @ptrCast(@alignCast(context));
                    inline for (std.meta.fields(ConnectionProperties)) |field| {
                        const value = @field(properties, field.name);
                        encoder.put(field.name, switch (field.type) {
                            []const u8 => .{ .string = value },
                            *const ClientCapabilities => .{ .field_table = value.table() },
                            else => comptime unreachable,
                        });
                    }
                }
            }.write,
        };
        return .{ .context = self, .vtable = &vtable };
    }
};

pub const ClientCapabilities = struct {
    publisher_confirms: bool,
    exchange_exchange_bindings: bool,
    basic_nack: bool,
    consumer_cancel_notify: bool,
    connection_blocked: bool,
    consumer_priorities: bool,
    authentication_failure_close: bool,
    per_consumer_qos: bool,
    direct_reply_to: bool,

    pub const default: ClientCapabilities = .{
        .publisher_confirms = true,
        .exchange_exchange_bindings = false,
        .basic_nack = true,
        .consumer_cancel_notify = false,
        .connection_blocked = false,
        .consumer_priorities = false,
        .authentication_failure_close = true,
        .per_consumer_qos = false,
        .direct_reply_to = false,
    };

    pub fn table(self: *const ClientCapabilities) Encoder.Table {
        const vtable: Encoder.Table.VTable = comptime .{
            .write = &struct {
                fn write(context: *const anyopaque, encoder: *Encoder.TableEncoder) void {
                    const capabilities: *const ClientCapabilities = @ptrCast(@alignCast(context));
                    encoder.put("publisher_confirms", .{
                        .boolean = capabilities.publisher_confirms,
                    });
                    encoder.put("exchange_exchange_bindings", .{
                        .boolean = capabilities.exchange_exchange_bindings,
                    });
                    encoder.put("basic.nack", .{
                        .boolean = capabilities.basic_nack,
                    });
                    encoder.put("consumer_cancel_notify", .{
                        .boolean = capabilities.consumer_cancel_notify,
                    });
                    encoder.put("connection.blocked", .{
                        .boolean = capabilities.connection_blocked,
                    });
                    encoder.put("consumer_priorities", .{
                        .boolean = capabilities.consumer_priorities,
                    });
                    encoder.put("authentication_failure_close", .{
                        .boolean = capabilities.authentication_failure_close,
                    });
                    encoder.put("per_consumer_qos", .{
                        .boolean = capabilities.per_consumer_qos,
                    });
                    encoder.put("direct_reply_to", .{
                        .boolean = capabilities.direct_reply_to,
                    });
                }
            }.write,
        };
        return .{ .context = self, .vtable = &vtable };
    }
};

/// "SASL" means "Simple Authentication and Security Layer"
pub const SASLPlainAuth = struct {
    pub const mechanism = "PLAIN";

    user_name: []const u8,
    password: []const u8,

    /// Response returns the SASL PLAIN mechanism encoding, delimited by null characters.
    pub fn response(self: *const SASLPlainAuth) Encoder.Body {
        const vtable: Encoder.Body.VTable = comptime .{
            .write = &struct {
                fn write(context: *const anyopaque, buffer: []u8) usize {
                    const auth: *const SASLPlainAuth = @ptrCast(@alignCast(context));
                    var fbs = std.io.fixedBufferStream(buffer);
                    fbs.writer().print("\x00{s}\x00{s}", .{
                        auth.user_name,
                        auth.password,
                    }) catch unreachable;
                    return fbs.pos;
                }
            }.write,
        };
        return .{ .context = self, .vtable = &vtable };
    }
};

pub const QueueOverflow = enum {
    drop_head,
    reject_publish,
    reject_publish_dlx,
};

/// A set of implementation-defined arguments for the declaration.
/// The syntax and semantics of these arguments are specific to RabbitMQ.
pub const QueueDeclareArguments = struct {
    /// How long a queue can be unused for before it is automatically deleted (milliseconds).
    /// (Sets the "x-expires" argument.)
    expires: ?u32 = null,
    /// How long a message published to a queue can live before it is discarded (milliseconds).
    /// (Sets the "x-message-ttl" argument.)
    message_ttl: ?u32 = null,
    /// Sets the queue overflow behaviour.
    /// This determines what happens to messages when the maximum length of a queue is reached.
    overflow: ?QueueOverflow = null,
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

    pub fn table(self: *const QueueDeclareArguments) Encoder.Table {
        const vtable: Encoder.Table.VTable = comptime .{
            .write = &struct {
                fn write(context: *const anyopaque, encoder: *Encoder.TableEncoder) void {
                    const arguments: *const QueueDeclareArguments = @ptrCast(@alignCast(context));
                    inline for (std.meta.fields(QueueDeclareArguments)) |field| {
                        if (@field(arguments, field.name)) |value| {
                            const FieldType = @TypeOf(value);
                            // Keys are follow the pattern "x-max-length":
                            const key = comptime "x-" ++ kebab_case(field.name);
                            switch (FieldType) {
                                []const u8 => encoder.put(key, .{ .string = value }),
                                QueueOverflow => encoder.put(key, .{ .string = switch (value) {
                                    inline else => |tag| comptime "" ++ kebab_case(@tagName(tag)),
                                } }),
                                bool => encoder.put(key, .{ .boolean = value }),
                                u32 => encoder.put(key, .{ .uint32 = value }),
                                else => comptime unreachable,
                            }
                        }
                    }
                }
                fn kebab_case(comptime key: []const u8) []const u8 {
                    comptime {
                        var buffer: [key.len]u8 = @as(*const [key.len]u8, @ptrCast(key)).*;
                        std.mem.replaceScalar(u8, &buffer, '_', '-');
                        return &buffer;
                    }
                }
            }.write,
        };
        return .{ .context = self, .vtable = &vtable };
    }
};

pub const BasicPublishOptions = struct {
    /// Specifies the name of the exchange to publish to. The exchange name can be
    /// empty, meaning the default exchange. If the exchange name is specified, and that
    /// exchange does not exist, the server will raise a channel exception.
    exchange: []const u8,
    /// Message routing key.
    /// Specifies the routing key for the message. The routing key is used for routing
    /// messages depending on the exchange configuration.
    routing_key: []const u8,
    /// Indicate mandatory routing.
    /// This flag tells the server how to react if the message cannot be routed to a
    /// queue. If this flag is set, the server will return an unroutable message with a
    /// Return method. If this flag is zero, the server silently drops the message.
    mandatory: bool,
    /// Request immediate delivery.
    /// This flag tells the server how to react if the message cannot be routed to a
    /// queue consumer immediately. If this flag is set, the server will return an
    /// undeliverable message with a Return method. If this flag is zero, the server
    /// will queue the message, but with no guarantee that it will ever be consumed.
    immediate: bool,
    /// Metadata associated with the message.
    properties: Encoder.BasicProperties,
    /// The message payload.
    body: ?Encoder.Body,
};

pub const QueueDeclareOptions = struct {
    queue: []const u8,
    /// Do not create queue.
    /// If set, the server will reply with Declare-Ok if the queue already
    /// exists with the same name, and raise an error if not.  The client can
    /// use this to check whether a queue exists without modifying the
    /// server state.  When set, all other method fields except name and no-wait
    /// are ignored.  A declare with both passive and no-wait has no effect.
    /// Arguments are compared for semantic equivalence.
    passive: bool,
    /// Request a durable queue.
    /// If set when creating a new queue, the queue will be marked as durable. Durable
    /// queues remain active when a server restarts. Non-durable queues (transient
    /// queues) are purged if/when a server restarts. Note that durable queues do not
    /// necessarily hold persistent messages, although it does not make sense to send
    /// persistent messages to a transient queue.
    durable: bool,
    /// Request an exclusive queue.
    /// Exclusive queues may only be accessed by the current connection, and are
    /// deleted when that connection closes.  Passive declaration of an exclusive
    /// queue by other connections are not allowed.
    exclusive: bool,
    /// Auto-delete queue when unused.
    /// If set, the queue is deleted when all consumers have finished using it.  The last
    /// consumer can be cancelled either explicitly or because its channel is closed. If
    /// there was no consumer ever on the queue, it won't be deleted.  Applications can
    /// explicitly delete auto-delete queues using the Delete method as normal.
    auto_delete: bool,
    /// A set of implementation-defined arguments for the declaration.
    /// The syntax and semantics of these arguments are specific to RabbitMQ.
    arguments: QueueDeclareArguments,
};

pub const ExchangeDeclareOptions = struct {
    exchange: []const u8,
    /// Exchange type.
    /// Each exchange belongs to one of a set of exchange types implemented by the
    /// server. The exchange types define the functionality of the exchange - i.e. how
    /// messages are routed through it. It is not valid or meaningful to attempt to
    /// change the type of an existing exchange.
    type: []const u8,
    /// Do not create exchange.
    /// If set, the server will reply with Declare-Ok if the exchange already
    /// exists with the same name, and raise an error if not.  The client can
    /// use this to check whether an exchange exists without modifying the
    /// server state. When set, all other method fields except name and no-wait
    /// are ignored.  A declare with both passive and no-wait has no effect.
    /// Arguments are compared for semantic equivalence.
    passive: bool,
    /// Request a durable exchange.
    /// If set when creating a new exchange, the exchange will be marked as durable.
    /// Durable exchanges remain active when a server restarts. Non-durable exchanges
    /// (transient exchanges) are purged if/when a server restarts.
    durable: bool,
    /// Auto-delete when unused.
    /// If set, the exchange is deleted when all queues have
    /// finished using it.
    auto_delete: bool,
    /// Create internal exchange.
    /// If set, the exchange may not be used directly by publishers,
    /// but only when bound to other exchanges. Internal exchanges
    /// are used to construct wiring that is not visible to
    /// applications.
    internal: bool,
};

pub const GetMessageOptions = struct {
    /// Specifies the name of the queue to get a message from.
    queue: []const u8,
    /// When `true`, indicates that the message does not require an acknowledgment
    /// and will be instantly acknowledged upon delivery.
    no_ack: bool,
};

pub const GetMessagePropertiesResult = struct {
    /// Delivery tag used to acknowledge or reject the message.
    delivery_tag: u64,
    /// The number of messages still available in the queue.
    message_count: u32,
    /// Basic properties including custom headers.
    properties: Decoder.BasicProperties,
    /// Indicates whether the message includes a body frame.
    has_body: bool,
};

pub const BasicNackOptions = struct {
    delivery_tag: u64,
    /// Requeue the message.
    /// If requeue is true, the server will attempt to requeue the message.
    requeue: bool,
    /// Reject multiple messages.
    /// If set to `true`, the delivery tag is treated as "up to and including",
    /// so that multiple messages can be rejected with a single method.
    /// If set to zero, the delivery tag refers to a single message.
    /// If the multiple field is `true`, and the delivery tag is `false`,
    /// this indicates rejection of all outstanding messages.
    multiple: bool,
};
