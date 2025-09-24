///! Implements the AMQP (Advanced Message Queuing Protocol) 0.9.1 wire protocol.
///! https://www.amqp.org/sites/amqp.org/files/amqp0-9-1.zip
///!
///! The `Frame` is the basic unit of the AMQP protocol. Its minimum size is
///! 8 bytes, and the maximum size can be negotiated between the client and server.
///!
///! # Frame layout:
///! ┌────────┬──────────┬────────┐ ┌───────────────┐ ┌───────┐
///! │  type  │ channel  │  size  │ │    payload    │ │ 0xCE  │
///! │   u8   │   u16    │  u32   │ │ variable size │ │  u8   │
///! └────────┴──────────┴────────┘ └───────────────┘ └───────┘
///! There are four frame types: "method", "header", "body", and "heartbeat".
///! Each frame type (except by heartbeat) has different types of payloads.
///!
///! # Method payload:
///! ┌──────────┬───────────┬─────────────────┐
///! │ class_id │ method_id │     arguments   │
///! │   u16    │    u16    │  variable size  │
///! └──────────┴───────────┴─────────────────┘
///! The `spec.zig` file contains declarations for all methods defined by the
///! specification and their expected arguments.
///!
///! # Header payload:
///! ┌──────────┬────────┬────────────┬────────────────┬─────────────────┐
///! │ class_id │ weight │ body_size  │ property_flags │   properties    │
///! │   u16    │  u16   │   u64      │     u16        │  variable size  │
///! └──────────┴────────┴────────────┴────────────────┴─────────────────┘
///! Certain "method" frames are followed by a "header" frame. For example, in the `basic-publish`
///! method, the content header contains metadata about the message being published. The frame with
///! `type == header` always follows its corresponding `type == method` frame.
///! See `BasicProperties` for parsing the `property_flags` and `properties`.
///!
///! # Body payload:
///! ┌───────────────┐
///! │    content    │
///! │ variable size │
///! └───────────────┘
///! The body frame contains the application-specific content of the message.
///! The body can be split across multiple frames if `body_size` exceeds the frame size, however we
///! only support single-frame bodies.
///!
///! # Endianness:
///! Integers are encoded in network byte order (big endian).
///!
const std = @import("std");
const stdx = @import("stdx");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const KiB = stdx.KiB;

const spec = @import("spec.zig");

pub const frame_min_size = spec.FRAME_MIN_SIZE;
pub const tcp_port_default = 5672;

/// The major, minor, and revision numbers can take any value from 0 to 99 for official
/// specifications.
/// Major, minor, and revision numbers of 100 and above are reserved for internal testing
/// and development purposes.
pub const version = .{
    .major = 0,
    .minor = 9,
    .revision = 1,
};

/// The protocol header consists of the upper case letters "AMQP"
/// followed by the constant 0 and the AMQP version number.
pub const protocol_header: *const [8]u8 = "AMQP" ++ [_]u8{
    0,
    version.major,
    version.minor,
    version.revision,
};

pub const DeliveryMode = enum(u8) {
    transient = 1,
    persistent = 2,
};

pub const FrameEnd = enum(u8) {
    value = spec.FRAME_END,
};

pub const FrameType = enum(u8) {
    method = spec.FRAME_METHOD,
    header = spec.FRAME_HEADER,
    body = spec.FRAME_BODY,
    heartbeat = spec.FRAME_HEARTBEAT,
};

pub const MethodHeader = packed struct(u32) {
    class: u16,
    method: u16,
};

pub const Channel = enum(u16) {
    /// The channel number is 0 for all frames which are global to the connection.
    global = 0,
    /// Id of the current channel.
    /// Supporting multiple channels is unnecessary, as messages are submitted in batches
    /// through io_uring without concurrency.
    current = 1,
};

pub const ErrorCodes = enum(u16) {
    /// The client attempted to transfer content larger than the server could accept
    /// at the present time. The client may retry at a later time.
    ContentTooLarge = spec.SOFT_ERROR_CONTENT_TOO_LARGE,
    /// Returned when RabbitMQ sends back with 'basic.return' when a
    /// 'mandatory' message cannot be delivered to any queue.
    NoRoute = spec.SOFT_ERROR_NO_ROUTE,
    /// When the exchange cannot deliver to a consumer when the immediate flag is
    /// set. As a result of pending data on the queue or the absence of any
    /// consumers of the queue.
    NoConsumers = spec.SOFT_ERROR_NO_CONSUMERS,
    /// The client attempted to work with a server entity to which it has no
    /// access due to security settings.
    AccessRefused = spec.SOFT_ERROR_ACCESS_REFUSED,
    /// The client attempted to work with a server entity that does not exist.
    NotFound = spec.SOFT_ERROR_NOT_FOUND,
    /// The client attempted to work with a server entity to which it has no
    /// access because another client is working with it.
    ResourceLocked = spec.SOFT_ERROR_RESOURCE_LOCKED,
    /// The client requested a method that was not allowed because some precondition
    /// failed.
    PreconditionFailed = spec.SOFT_ERROR_PRECONDITION_FAILED,
    /// An operator intervened to close the connection for some reason. The client
    /// may retry at some later date.
    ConnectionForced = spec.HARD_ERROR_CONNECTION_FORCED,
    /// The client tried to work with an unknown virtual host.
    InvalidPath = spec.HARD_ERROR_INVALID_PATH,
    /// The sender sent a malformed frame that the recipient could not decode.
    /// This strongly implies a programming error in the sending peer.
    FrameError = spec.HARD_ERROR_FRAME_ERROR,
    /// The sender sent a frame that contained illegal values for one or more
    /// fields. This strongly implies a programming error in the sending peer.
    SyntaxError = spec.HARD_ERROR_SYNTAX_ERROR,
    /// The client sent an invalid sequence of frames, attempting to perform an
    /// operation that was considered invalid by the server. This usually implies
    /// a programming error in the client.
    CommandInvalid = spec.HARD_ERROR_COMMAND_INVALID,
    /// The client attempted to work with a channel that had not been correctly
    /// opened. This most likely indicates a fault in the client layer.
    ChannelError = spec.HARD_ERROR_CHANNEL_ERROR,
    /// The peer sent a frame that was not expected, usually in the context of
    /// a content header and body.  This strongly indicates a fault in the peer's
    /// content processing.
    UnexpectedFrame = spec.HARD_ERROR_UNEXPECTED_FRAME,
    /// The server could not complete the method because it lacked sufficient
    /// resources. This may be due to the client creating too many of some type
    /// of entity.
    ResourceError = spec.HARD_ERROR_RESOURCE_ERROR,
    /// The client tried to work with some entity in a manner that is prohibited
    /// by the server, due to security settings or by some other criteria.
    NotAllowed = spec.HARD_ERROR_NOT_ALLOWED,
    /// The client tried to use functionality that is not implemented in the
    /// server.
    NotImplemented = spec.HARD_ERROR_NOT_IMPLEMENTED,
    /// The server could not complete the method because of an internal error.
    /// The server may require intervention by an operator in order to resume
    /// normal operations.
    InternalError = spec.HARD_ERROR_INTERNAL_ERROR,

    _,
};

pub const FieldValueTag = enum(u8) {
    boolean = 't',
    uint8 = 'B',
    int8 = 'b',
    uint16 = 'u',
    int16 = 's',
    uint32 = 'i',
    int32 = 'I',
    // Both `l` and `L` are decoded as signed integers by RabbitMQ:
    // https://www.rabbitmq.com/amqp-0-9-1-errata#section_3
    // https://github.com/rabbitmq/rabbitmq-server/issues/1093#issuecomment-276351183
    int64 = 'l',
    string = 'S',
    timestamp = 'T',
    field_table = 'F',
    void = 'V',

    // We don't send or expect to receive these types from the AMQP server.
    // Only user-defined tables would use them.
    not_implemented_uint64 = 'L',
    not_implemented_field_array = 'A',
    not_implemented_float = 'f',
    not_implemented_double = 'd',
    not_implemented_decimal = 'D',
    not_implemented_byte_array = 'x',
};

pub const Decoder = struct {
    pub const Error = error{
        BufferExhausted,
        Unexpected,
    };

    pub const FrameHeader = extern struct {
        type: FrameType,
        channel: Channel,
        size: u32,
    };

    pub const Header = struct {
        class: u16,
        weight: u16,
        body_size: u64,
        property_flags: u16,
        properties: []const u8,
    };

    pub const BasicProperties = BasicPropertiesType(.decode);

    /// `FieldValue` represents a `tag` + `value` pair as specified by the AMQP spec.
    pub const FieldValue = FieldValueType(.decode);

    /// Allows iteration over the contents of an AMQP table read from the receive buffer.
    pub const Table = struct {
        pub const Iterator = struct {
            decoder: Decoder,

            pub fn reset(self: *Iterator) void {
                self.decoder.reset();
            }

            pub fn next(self: *Iterator) Decoder.Error!?struct {
                key: []const u8,
                value: FieldValue,
            } {
                if (self.decoder.empty()) return null;
                return .{
                    .key = try self.decoder.read_short_string(),
                    .value = try self.decoder.read_field(),
                };
            }
        };

        length: u32,
        pointer: [*]const u8,

        pub fn init(value: []const u8) Table {
            assert(value.len <= std.math.maxInt(u32));
            return .{
                .length = @intCast(value.len),
                .pointer = value.ptr,
            };
        }

        pub fn slice(self: Table) []const u8 {
            return self.pointer[0..self.length];
        }

        pub fn iterator(self: Table) Iterator {
            return .{
                .decoder = Decoder.init(self.slice()),
            };
        }
    };

    buffer: []const u8,
    /// Invariants: index <= buffer.len
    index: usize,

    pub fn init(buffer: []const u8) Decoder {
        return .{
            .buffer = buffer,
            .index = 0,
        };
    }

    pub fn empty(self: *const Decoder) bool {
        return self.index == self.buffer.len;
    }

    pub fn read_int(self: *Decoder, comptime T: type) Error!T {
        comptime assert(@typeInfo(T) == .int);
        comptime assert(@typeInfo(T).int.signedness == .unsigned);
        comptime assert(@sizeOf(T) == 1 or @sizeOf(T) == 2 or @sizeOf(T) == 4 or @sizeOf(T) == 8);
        if (self.index + @sizeOf(T) > self.buffer.len) return error.BufferExhausted;
        defer {
            self.index += @sizeOf(T);
            assert(self.index <= self.buffer.len);
        }

        return std.mem.readInt(T, self.buffer[self.index..][0..@sizeOf(T)], .big);
    }

    pub fn read_enum(self: *Decoder, comptime Enum: type) Error!Enum {
        comptime assert(@typeInfo(Enum) == .@"enum");
        const Int = std.meta.Tag(Enum);
        const value = try self.read_int(Int);
        return std.meta.intToEnum(
            Enum,
            value,
        ) catch |err| switch (err) {
            error.InvalidEnumTag => return error.Unexpected,
        };
    }

    pub fn read_bool(self: *Decoder) Error!bool {
        const value = try self.read_int(u8);
        return value != 0;
    }

    pub fn read_short_string(self: *Decoder) Error![]const u8 {
        const length: u8 = try self.read_int(u8);
        return try self.read_bytes(length);
    }

    pub fn read_long_string(self: *Decoder) Error![]const u8 {
        const length: u32 = try self.read_int(u32);
        return try self.read_bytes(length);
    }

    pub fn read_table(self: *Decoder) Error!Table {
        const length: u32 = try self.read_int(u32);
        const bytes = try self.read_bytes(length);
        return Table.init(bytes);
    }

    fn read_bytes(self: *Decoder, length: u32) Error![]const u8 {
        assert(self.index <= self.buffer.len);
        if (self.index + length > self.buffer.len) return error.BufferExhausted;
        defer {
            self.index += length;
            assert(self.index <= self.buffer.len);
        }

        return self.buffer[self.index..][0..length];
    }

    pub fn read_field(self: *Decoder) Error!FieldValue {
        const tag = try self.read_enum(FieldValueTag);
        const value: FieldValue = switch (tag) {
            .boolean => .{ .boolean = try self.read_bool() },
            .uint8 => .{ .uint8 = try self.read_int(u8) },
            .int8 => .{ .int8 = @bitCast(try self.read_int(u8)) },
            .uint16 => .{ .uint16 = try self.read_int(u16) },
            .int16 => .{ .int16 = @bitCast(try self.read_int(u16)) },
            .uint32 => .{ .uint32 = try self.read_int(u32) },
            .int32 => .{ .int32 = @bitCast(try self.read_int(u32)) },
            .int64 => .{ .int64 = @bitCast(try self.read_int(u64)) },
            .string => .{ .string = try self.read_long_string() },
            .timestamp => .{ .timestamp = try self.read_int(u64) },
            .field_table => .{ .field_table = try self.read_table() },
            .void => .void,

            .not_implemented_uint64,
            .not_implemented_field_array,
            .not_implemented_float,
            .not_implemented_double,
            .not_implemented_decimal,
            .not_implemented_byte_array,
            => fatal("AMQP type '{c}' not supported.", .{@intFromEnum(tag)}),
        };
        assert(value == tag);
        return value;
    }

    pub fn read_frame_header(self: *Decoder) Error!FrameHeader {
        return .{
            .type = try self.read_enum(FrameType),
            .channel = try self.read_enum(Channel),
            .size = try self.read_int(u32),
        };
    }

    pub fn read_frame_end(self: *Decoder) Error!void {
        _ = try self.read_enum(FrameEnd);
    }

    pub fn read_method_header(self: *Decoder) Error!MethodHeader {
        return .{
            .class = try self.read_int(u16),
            .method = try self.read_int(u16),
        };
    }

    pub fn read_header(self: *Decoder, frame_size: usize) Error!Header {
        const initial_index = self.index;

        const class = try self.read_int(u16);
        const weight = try self.read_int(u16);
        const body_size = try self.read_int(u64);
        const property_flags = try self.read_int(u16);

        if (initial_index + frame_size > self.buffer.len) return error.BufferExhausted;
        const properties = self.buffer[self.index .. initial_index + frame_size];
        self.index += properties.len;

        try self.read_frame_end();

        return .{
            .class = class,
            .weight = weight,
            .body_size = body_size,
            .property_flags = @bitCast(property_flags),
            .properties = properties,
        };
    }

    pub fn read_body(self: *Decoder, frame_size: usize) Error![]const u8 {
        if (self.index + frame_size > self.buffer.len) return error.BufferExhausted;
        const body = self.buffer[self.index..][0..frame_size];
        self.index += frame_size;
        assert(self.index <= self.buffer.len);
        try self.read_frame_end();
        return body;
    }
};

pub const Encoder = struct {
    pub const FrameHeader = struct {
        /// Total size in bytes including the `size` field.
        pub const size_total = @sizeOf(@FieldType(Decoder.FrameHeader, "type")) +
            @sizeOf(@FieldType(Decoder.FrameHeader, "channel")) +
            @sizeOf(@FieldType(Decoder.FrameHeader, "size"));

        type: FrameType,
        channel: Channel,
    };

    pub const Header = struct {
        /// Total size in bytes including the `body_size` field.
        pub const size_total = @sizeOf(@FieldType(Decoder.Header, "class")) +
            @sizeOf(@FieldType(Decoder.Header, "weight")) +
            @sizeOf(@FieldType(Decoder.Header, "body_size"));

        class: u16,
        weight: u16,
    };

    pub const BasicProperties = BasicPropertiesType(.encode);

    /// `FieldValue` represents a `tag` + `value` pair as specified by the AMQP spec.
    pub const FieldValue = FieldValueType(.encode);

    /// Interface for a user-defined set of values to be encoded as an AMQP table
    /// directly into the send buffer without copying.
    pub const Table = struct {
        pub const VTable = struct {
            write: *const fn (*const anyopaque, *TableEncoder) void,
        };

        context: *const anyopaque,
        vtable: *const VTable,

        pub fn write(self: Table, encoder: *TableEncoder) void {
            self.vtable.write(self.context, encoder);
        }
    };

    /// Interface for user-defined content to be written directly
    /// into the send buffer without copying.
    pub const Body = struct {
        pub const VTable = struct {
            write: *const fn (*const anyopaque, []u8) usize,
        };

        context: *const anyopaque,
        vtable: *const VTable,

        pub fn write(self: Body, buffer: []u8) usize {
            return self.vtable.write(self.context, buffer);
        }
    };

    pub const TableEncoder = struct {
        encoder: *Encoder,

        pub fn put(self: *TableEncoder, key: []const u8, value: FieldValue) void {
            self.encoder.write_short_string(key);
            self.encoder.write_field(value);
        }
    };

    buffer: []u8,
    index: usize,

    frame_reference: ?struct {
        index: usize,
        frame_header: FrameHeader,
    },
    header_reference: ?struct {
        index: usize,
        header: Header,
    },

    pub fn init(buffer: []u8) Encoder {
        return .{
            .buffer = buffer,
            .index = 0,
            .frame_reference = null,
            .header_reference = null,
        };
    }

    pub fn write_int(self: *Encoder, comptime T: type, value: T) void {
        comptime assert(@typeInfo(T) == .int);
        comptime assert(@sizeOf(T) == 1 or @sizeOf(T) == 2 or @sizeOf(T) == 4 or @sizeOf(T) == 8);
        assert(self.index + @sizeOf(T) <= self.buffer.len);
        std.mem.writeInt(T, self.buffer[self.index..][0..@sizeOf(T)], value, .big);
        self.index += @sizeOf(T);
        assert(self.index <= self.buffer.len);
    }

    pub fn write_bool(self: *Encoder, value: bool) void {
        self.write_int(u8, @intFromBool(value));
    }

    pub fn write_short_string(self: *Encoder, value: []const u8) void {
        assert(value.len <= std.math.maxInt(u8));
        self.write_int(u8, @intCast(value.len));
        assert(self.index + value.len <= self.buffer.len);
        stdx.copy_left(.inexact, u8, self.buffer[self.index..], value);
        self.index += value.len;
    }

    pub fn write_long_string(self: *Encoder, value: []const u8) void {
        assert(value.len <= std.math.maxInt(u32));
        self.write_int(u32, @intCast(value.len));
        assert(self.index + value.len <= self.buffer.len);
        stdx.copy_left(.inexact, u8, self.buffer[self.index..], value);
        self.index += value.len;
    }

    pub fn write_long_string_body(self: *Encoder, body: ?Body) void {
        if (body == null) {
            self.write_int(u32, 0); // Zero sized string.
            return;
        }

        const start_index = self.index;
        self.index += @sizeOf(u32);
        assert(self.index <= self.buffer.len);

        self.index += body.?.write(self.buffer[self.index..]);
        assert(self.index <= self.buffer.len);
        const end_index = self.index;

        const size: u32 = @intCast(end_index - start_index - @sizeOf(u32));
        self.index = start_index;
        self.write_int(u32, size);
        self.index = end_index;
    }

    pub fn write_table(self: *Encoder, table: ?Table) void {
        if (table == null) {
            self.write_int(u32, 0); // Zero sized table.
            return;
        }

        const start_index = self.index;
        self.index += @sizeOf(u32);
        assert(self.index <= self.buffer.len);

        var table_encoder: TableEncoder = .{ .encoder = self };
        table.?.write(&table_encoder);
        const end_index = self.index;

        const size: u32 = @intCast(end_index - start_index - @sizeOf(u32));
        self.index = start_index;
        self.write_int(u32, size);
        self.index = end_index;
    }

    pub fn write_field(self: *Encoder, field: FieldValue) void {
        const tag: FieldValueTag = field;
        self.write_int(u8, @intFromEnum(tag));
        switch (field) {
            .boolean => |value| self.write_bool(value),
            .uint8 => |value| self.write_int(u8, value),
            .int8 => |value| self.write_int(u8, @bitCast(value)),
            .uint16 => |value| self.write_int(u16, value),
            .int16 => |value| self.write_int(u16, @bitCast(value)),
            .uint32 => |value| self.write_int(u32, value),
            .int32 => |value| self.write_int(u32, @bitCast(value)),
            .int64 => |value| self.write_int(u64, @bitCast(value)),
            .string => |value| self.write_long_string(value),
            .timestamp => |value| self.write_int(u64, value),
            .field_table => |value| self.write_table(value),
            .void => {},

            .not_implemented_uint64,
            .not_implemented_field_array,
            .not_implemented_float,
            .not_implemented_double,
            .not_implemented_decimal,
            .not_implemented_byte_array,
            => fatal("AMQP type '{c}' not supported.", .{@intFromEnum(tag)}),
        }
    }

    pub fn write_bytes(self: *Encoder, bytes: []const u8) void {
        assert(bytes.len > 0);
        assert(self.index + bytes.len <= self.buffer.len);
        stdx.copy_disjoint(.inexact, u8, self.buffer[self.index..], bytes);
        self.index += bytes.len;
        assert(self.index <= self.buffer.len);
    }

    pub fn begin_frame(self: *Encoder, frame_header: FrameHeader) void {
        assert(self.frame_reference == null);
        assert(self.header_reference == null or frame_header.type == .body);
        // Reserve the frame header bytes to be updated by `finish_frame()`.
        assert(self.index + FrameHeader.size_total <= self.buffer.len);
        const frame_header_index = self.index;
        self.index += FrameHeader.size_total;
        self.frame_reference = .{
            .index = frame_header_index,
            .frame_header = frame_header,
        };
    }

    pub fn finish_frame(self: *Encoder, frame_type: FrameType) void {
        assert(self.frame_reference != null);
        assert(self.frame_reference.?.frame_header.type == frame_type);
        maybe(self.header_reference == null);

        const reference = self.frame_reference.?;
        self.frame_reference = null;
        assert(reference.index + FrameHeader.size_total <= self.index);
        const restore_index = self.index;
        // The frame size field in the FrameHeader must be updated.
        // It represents the payload size, excluding the FrameHeader
        // and the frame end byte.
        const size: u32 = @intCast(restore_index - reference.index - FrameHeader.size_total);
        self.index = reference.index;
        self.write_int(u8, @intFromEnum(reference.frame_header.type));
        self.write_int(u16, @intFromEnum(reference.frame_header.channel));
        self.write_int(u32, size);

        self.index = restore_index;
        self.write_int(u8, spec.FRAME_END);
    }

    pub fn begin_header(self: *Encoder, header: Header) void {
        // Reserve the frame header bytes to be updated by `finish_header()`.
        assert(self.frame_reference != null);
        assert(self.frame_reference.?.frame_header.type == .header);
        assert(self.header_reference == null);
        const header_index = self.index;
        self.index += Header.size_total;
        self.header_reference = .{
            .header = header,
            .index = header_index,
        };
    }

    pub fn finish_header(self: *Encoder, body_size: u64) void {
        assert((body_size == 0) == (self.frame_reference == null));
        assert(body_size == 0 or self.frame_reference.?.frame_header.type == .body);
        assert(self.header_reference != null);

        const reference = self.header_reference.?;
        self.header_reference = null;
        assert(reference.index + Header.size_total <= self.index);
        const restore_index = self.index;
        self.index = reference.index;
        self.write_int(u16, reference.header.class);
        self.write_int(u16, reference.header.weight);
        self.write_int(u64, body_size);
        self.index = restore_index;
    }

    pub fn write_method_header(self: *Encoder, method_header: MethodHeader) void {
        assert(self.frame_reference != null);
        assert(self.frame_reference.?.frame_header.type == .method);
        assert(self.header_reference == null);
        self.write_int(u16, method_header.class);
        self.write_int(u16, method_header.method);
    }
};

fn FieldValueType(comptime target: enum { encode, decode }) type {
    return union(FieldValueTag) {
        boolean: bool,
        uint8: u8,
        int8: i8,
        uint16: u16,
        int16: i16,
        uint32: u32,
        int32: i32,
        int64: i64,
        string: []const u8,
        timestamp: u64,
        field_table: switch (target) {
            .encode => Encoder.Table,
            .decode => Decoder.Table,
        },
        void,

        not_implemented_uint64,
        not_implemented_field_array,
        not_implemented_float,
        not_implemented_double,
        not_implemented_decimal,
        not_implemented_byte_array,
    };
}

fn BasicPropertiesType(comptime target: enum { encode, decode }) type {
    return struct {
        const BasicProperties = @This();

        /// MIME content type of the message payload.
        content_type: ?[]const u8 = null,
        /// MIME content encoding of the message payload.
        content_encoding: ?[]const u8 = null,
        /// Application-defined custom headers.
        headers: ?switch (target) {
            .encode => Encoder.Table,
            .decode => Decoder.Table,
        } = null,
        /// For queues that implement persistence,
        /// whether the message will be logged to disk and survive a broker restart.
        delivery_mode: ?DeliveryMode = null,
        /// Message priority, 0 to 9.
        priority: ?u8 = null,
        /// Application-defined correlation identifier.
        correlation_id: ?[]const u8 = null,
        /// Address to reply to.
        reply_to: ?[]const u8 = null,
        /// Message expiration specification.
        expiration: ?[]const u8 = null,
        /// Application-defined message identifier.
        message_id: ?[]const u8 = null,
        /// Message timestamp (UNIX epoch in seconds).
        timestamp: ?u64 = null,
        /// Application-defined message type name.
        type: ?[]const u8 = null,
        /// Application-defined creating user id
        user_id: ?[]const u8 = null,
        /// Application-defined creating application id.
        app_id: ?[]const u8 = null,
        cluster_id: ?[]const u8 = null,

        fn property_flags(self: *const BasicProperties) u16 {
            var bitset: stdx.BitSetType(16) = .{};
            inline for (std.meta.fields(BasicProperties), 0..) |field, index| {
                bitset.set_value(index, @field(self, field.name) != null);
            }
            return @bitReverse(bitset.bits);
        }

        pub fn decode(flags: u16, content: []const u8) Decoder.Error!BasicProperties {
            comptime assert(target == .decode);

            var reader = Decoder.init(content);
            var bitset: stdx.BitSetType(16) = .{ .bits = @bitReverse(flags) };
            var properties: BasicProperties = .{};
            inline for (std.meta.fields(BasicProperties), 0..) |field, index| {
                if (bitset.is_set(index)) {
                    const FieldType = std.meta.Child(field.type);
                    @field(properties, field.name) = try switch (FieldType) {
                        []const u8 => reader.read_short_string(),
                        Decoder.Table => reader.read_table(),
                        DeliveryMode => reader.read_enum(DeliveryMode),
                        u64 => reader.read_int(u64),
                        u8 => reader.read_int(u8),
                        else => comptime unreachable,
                    };
                }
            }
            assert(reader.index == content.len);
            return properties;
        }

        pub fn encode(self: *const BasicProperties, encoder: *Encoder) void {
            comptime assert(target == .encode);

            encoder.write_int(u16, self.property_flags());
            inline for (std.meta.fields(BasicProperties)) |field| {
                if (@field(self, field.name)) |value| {
                    switch (@TypeOf(value)) {
                        []const u8 => encoder.write_short_string(value),
                        Encoder.Table => encoder.write_table(value),
                        DeliveryMode => encoder.write_int(u8, @intFromEnum(value)),
                        u64 => encoder.write_int(u64, value),
                        u8 => encoder.write_int(u8, value),
                        else => unreachable,
                    }
                }
            }
        }
    };
}

/// Terminates the process with non-zero exit code.
/// Use fatal when encountering an environmental error.
/// Similar to `vsr.fatal`, but not logged in the `vsr` scope.
pub fn fatal(comptime format: []const u8, args: anytype) noreturn {
    const log = std.log.scoped(.amqp);
    log.err(format, args);

    const vsr = @import("../../vsr.zig");
    const status = vsr.FatalReason.cli.exit_status();
    assert(status != 0);
    std.process.exit(status);
}

const testing = std.testing;

test "amqp: Encoder/Decoder primitives" {
    var buffer = try testing.allocator.alloc(u8, frame_min_size);
    defer testing.allocator.free(buffer);

    const Primitives = enum {
        bool,
        uint64,
        uint32,
        uint16,
        uint8,
        short_string,
        long_string,
    };

    var prng = stdx.PRNG.from_seed_testing();
    for (0..4096) |_| {
        var encoder = Encoder.init(buffer);

        switch (prng.enum_uniform(Primitives)) {
            .bool => {
                const value = prng.boolean();
                encoder.write_bool(value);

                var decoder = Decoder.init(buffer[0..encoder.index]);
                try testing.expectEqual(value, try decoder.read_bool());
            },
            inline .uint64, .uint32, .uint16, .uint8 => |tag| {
                const Int = switch (tag) {
                    .uint64 => u64,
                    .uint32 => u32,
                    .uint16 => u16,
                    .uint8 => u8,
                    else => comptime unreachable,
                };
                const value = prng.int(Int);
                encoder.write_int(Int, value);

                var decoder = Decoder.init(buffer[0..encoder.index]);
                try testing.expectEqual(value, try decoder.read_int(Int));
            },
            .short_string => {
                const size = prng.range_inclusive(u32, 0, 255);
                const value = try testing.allocator.alloc(u8, size);
                defer testing.allocator.free(value);

                prng.fill(value);
                encoder.write_short_string(value);

                var decoder = Decoder.init(buffer[0..encoder.index]);
                try testing.expectEqualStrings(value, try decoder.read_short_string());
            },
            .long_string => {
                const size = prng.range_inclusive(u32, 256, frame_min_size - @sizeOf(u32));
                const value = try testing.allocator.alloc(u8, size);
                defer testing.allocator.free(value);

                prng.fill(value);
                encoder.write_long_string(value);

                var decoder = Decoder.init(buffer[0..encoder.index]);
                try testing.expectEqualStrings(value, try decoder.read_long_string());
            },
        }
    }
}

test "amqp: Encoder/Decoder enums" {
    var buffer = try testing.allocator.alloc(u8, frame_min_size);
    defer testing.allocator.free(buffer);

    const Enum = enum(u8) {
        a = 1,
        b = 2,
        c = 3,
    };

    for (std.enums.values(Enum)) |value| {
        var encoder: Encoder = Encoder.init(buffer);
        encoder.write_int(u8, @intFromEnum(value));

        var decoder: Decoder = Decoder.init(buffer[0..buffer.len]);
        try testing.expectEqual(value, try decoder.read_enum(Enum));
    }

    // Invalid enum:
    var encoder: Encoder = Encoder.init(buffer);
    encoder.write_int(u8, 0);

    var decoder: Decoder = Decoder.init(buffer[0..buffer.len]);
    try testing.expectError(error.Unexpected, decoder.read_enum(Enum));
}

test "amqp: BasicProperties property_flags" {
    // Sets the field with any value, just to compute the `property_flags`.
    const BasicProperties = BasicPropertiesType(.decode);
    const set_flag = struct {
        fn set_flag(set_field: std.meta.FieldEnum(BasicProperties)) u16 {
            var properties: BasicProperties = .{};
            switch (set_field) {
                inline else => |field| {
                    const Field = std.meta.Child(@FieldType(BasicProperties, @tagName(field)));
                    @field(properties, @tagName(field)) = switch (Field) {
                        []const u8 => "",
                        DeliveryMode => .persistent,
                        u8, u64 => 0,
                        Decoder.Table => Decoder.Table.init(&.{}),
                        else => comptime unreachable,
                    };
                },
            }
            return properties.property_flags();
        }
    }.set_flag;

    const empty: BasicProperties = .{};
    try testing.expectEqual(@as(u16, 0x0000), empty.property_flags());

    // The last bit corresponding to the first property (it's big endian).
    try testing.expectEqual(@as(u16, 0x8000), set_flag(.content_type));
    try testing.expectEqual(@as(u16, 0x4000), set_flag(.content_encoding));
    try testing.expectEqual(@as(u16, 0x2000), set_flag(.headers));
    try testing.expectEqual(@as(u16, 0x1000), set_flag(.delivery_mode));
    try testing.expectEqual(@as(u16, 0x0800), set_flag(.priority));
    try testing.expectEqual(@as(u16, 0x0400), set_flag(.correlation_id));
    try testing.expectEqual(@as(u16, 0x0200), set_flag(.reply_to));
    try testing.expectEqual(@as(u16, 0x0100), set_flag(.expiration));
    try testing.expectEqual(@as(u16, 0x0080), set_flag(.message_id));
    try testing.expectEqual(@as(u16, 0x0040), set_flag(.timestamp));
    try testing.expectEqual(@as(u16, 0x0020), set_flag(.type));
    try testing.expectEqual(@as(u16, 0x0010), set_flag(.user_id));
    try testing.expectEqual(@as(u16, 0x0008), set_flag(.app_id));
    try testing.expectEqual(@as(u16, 0x0004), set_flag(.cluster_id));
}

test "amqp: BasicProperties encode/decode" {
    var buffer = try testing.allocator.alloc(u8, frame_min_size);
    defer testing.allocator.free(buffer);

    var prng = stdx.PRNG.from_seed_testing();
    for (0..4096) |_| {
        var arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena.deinit();

        const properties = try TestingBasicProperties.random(.{
            .arena = arena.allocator(),
            .prng = &prng,
        });

        var encoder = Encoder.init(buffer);
        properties.encode(&encoder);

        // Decoding:
        var decoder = Decoder.init(buffer[0..encoder.index]);
        const flags = try decoder.read_int(u16);
        const properties_decoded = try Decoder.BasicProperties.decode(
            flags,
            decoder.buffer[decoder.index..],
        );
        try testing.expect(try TestingBasicProperties.eql(
            arena.allocator(),
            properties,
            properties_decoded,
        ));
    }
}

test "amqp: Table encode/decode" {
    // 64k ought to be enough for any random!
    var buffer = try testing.allocator.alloc(u8, 64 * KiB);
    defer testing.allocator.free(buffer);

    var prng = stdx.PRNG.from_seed_testing();
    for (0..4096) |_| {
        var arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena.deinit();

        const object = try TestingTable.random(.{
            .arena = arena.allocator(),
            .prng = &prng,
            .recursive = true,
        });

        // Encoding the complex object:
        var encoder = Encoder.init(buffer);
        encoder.write_table(object.table());

        // Decoding:
        var decoder = Decoder.init(buffer[0..encoder.index]);
        const object_decoded = try TestingTable.from_table(
            arena.allocator(),
            try decoder.read_table(),
        );
        try testing.expect(TestingTable.eql(object, object_decoded));
    }
}

test "amqp: frame and header" {
    const Snap = stdx.Snap;
    const snap = Snap.snap_fn("src");

    var buffer = try testing.allocator.alloc(u8, frame_min_size);
    defer testing.allocator.free(buffer);

    {
        // Method frame.
        var encoder = Encoder.init(buffer);
        encoder.begin_frame(.{ .type = .method, .channel = .global });
        encoder.write_method_header(.{ .class = 1, .method = 10 });
        encoder.finish_frame(.method);
        try snap(@src(),
            \\01 00 00 00 00 00 04 00  01 00 0a ce
        ).diff_hex(buffer[0..encoder.index]);
    }

    {
        // Method + header.
        var encoder = Encoder.init(buffer);
        encoder.begin_frame(.{ .type = .method, .channel = .global });
        encoder.write_method_header(.{ .class = 10, .method = 100 });
        encoder.finish_frame(.method);

        encoder.begin_frame(.{ .type = .header, .channel = .current });
        encoder.begin_header(.{ .class = 10, .weight = 0 });
        encoder.finish_frame(.header);
        encoder.finish_header(0);

        try snap(@src(),
            \\01 00 00 00 00 00 04 00  0a 00 64 ce 02 00 01 00
            \\00 00 0c 00 0a 00 00 00  00 00 00 00 00 00 00 ce
        ).diff_hex(buffer[0..encoder.index]);
    }

    {
        // Method + header + body.
        var encoder = Encoder.init(buffer);
        encoder.begin_frame(.{ .type = .method, .channel = .global });
        encoder.write_method_header(.{ .class = 100, .method = 1000 });
        encoder.finish_frame(.method);

        encoder.begin_frame(.{ .type = .header, .channel = .current });
        encoder.begin_header(.{ .class = 100, .weight = 0 });
        encoder.finish_frame(.header);

        encoder.begin_frame(.{ .type = .body, .channel = .current });
        encoder.write_bytes("body");
        encoder.finish_header("body".len);
        encoder.finish_frame(.body);

        try snap(@src(),
            \\01 00 00 00 00 00 04 00  64 03 e8 ce 02 00 01 00
            \\00 00 0c 00 64 00 00 00  00 00 00 00 00 00 04 ce
            \\03 00 01 00 00 00 04 62  6f 64 79 ce
        ).diff_hex(buffer[0..encoder.index]);
    }
}

const TestingTable = struct {
    const Timestamp = u63;

    boolean: ?bool = null,
    string: ?[]const u8 = null,
    int64: ?i64 = null,
    uint32: ?u32 = null,
    int32: ?i32 = null,
    uint16: ?u16 = null,
    int16: ?i16 = null,
    uint8: ?u8 = null,
    int8: ?i8 = null,
    field_table: ?*const TestingTable = null,
    timestamp: ?Timestamp = null,

    const empty: TestingTable = .{};

    fn table(self: *const TestingTable) Encoder.Table {
        const vtable: Encoder.Table.VTable = comptime .{
            .write = &struct {
                fn write(context: *const anyopaque, encoder: *Encoder.TableEncoder) void {
                    const object: *const TestingTable = @ptrCast(@alignCast(context));
                    inline for (std.meta.fields(TestingTable)) |field| {
                        if (@field(object, field.name)) |value| {
                            encoder.put(field.name, switch (std.meta.Child(field.type)) {
                                bool => .{ .boolean = value },
                                []const u8 => .{ .string = value },
                                i64 => .{ .int64 = value },
                                u32 => .{ .uint32 = value },
                                i32 => .{ .int32 = value },
                                u16 => .{ .uint16 = value },
                                i16 => .{ .int16 = value },
                                u8 => .{ .uint8 = value },
                                i8 => .{ .int8 = value },
                                *const TestingTable => .{ .field_table = value.table() },
                                Timestamp => .{ .timestamp = value },
                                else => comptime unreachable,
                            });
                        }
                    }
                }
            }.write,
        };
        return .{ .context = self, .vtable = &vtable };
    }

    fn from_table(arena: std.mem.Allocator, decoder: Decoder.Table) !*const TestingTable {
        var object = try arena.create(TestingTable);
        object.* = TestingTable.empty;

        var iterator = decoder.iterator();
        while (try iterator.next()) |entry| {
            const FieldEnum = std.meta.FieldEnum(TestingTable);
            const entry_field = std.meta.stringToEnum(FieldEnum, entry.key).?;
            switch (entry_field) {
                inline else => |field| {
                    const Field = @FieldType(TestingTable, @tagName(field));
                    @field(object, @tagName(field)) = switch (std.meta.Child(Field)) {
                        bool => entry.value.boolean,
                        []const u8 => entry.value.string,
                        i64 => entry.value.int64,
                        u32 => entry.value.uint32,
                        i32 => entry.value.int32,
                        u16 => entry.value.uint16,
                        i16 => entry.value.int16,
                        u8 => entry.value.uint8,
                        i8 => entry.value.int8,
                        *const TestingTable => try from_table(arena, entry.value.field_table),
                        Timestamp => @intCast(entry.value.timestamp),
                        else => comptime unreachable,
                    };
                },
            }
        }
        return object;
    }

    fn eql(table1: *const TestingTable, table2: *const TestingTable) bool {
        inline for (std.meta.fields(TestingTable)) |field| {
            const both_null = @field(table1, field.name) == null and
                @field(table2, field.name) == null;
            if (!both_null) {
                const value1 = @field(table1, field.name) orelse return false;
                const value2 = @field(table2, field.name) orelse return false;

                const equals = switch (std.meta.Child(field.type)) {
                    bool => value1 == value2,
                    []const u8 => std.mem.eql(u8, value1, value2),
                    i64, u32, i32, u16, i16, u8, i8 => value1 == value2,
                    *const TestingTable => eql(value1, value2),
                    Timestamp => value1 == value2,
                    else => comptime unreachable,
                };
                if (!equals) return false;
            }
        }

        return true;
    }

    fn random(options: struct {
        arena: std.mem.Allocator,
        prng: *stdx.PRNG,
        recursive: bool,
    }) !*const TestingTable {
        const ratio = stdx.PRNG.ratio;

        const is_empty = options.prng.chance(ratio(5, 100));
        if (is_empty) return &TestingTable.empty;

        var object = try options.arena.create(TestingTable);
        inline for (std.meta.fields(TestingTable)) |field| {
            const is_null = options.prng.chance(ratio(5, 100));
            if (is_null) {
                @field(object, field.name) = null;
            } else switch (std.meta.Child(field.type)) {
                bool => {
                    @field(object, field.name) = options.prng.boolean();
                },
                []const u8 => {
                    const size = options.prng.range_inclusive(u32, 0, 255);
                    const str = try options.arena.alloc(u8, size);
                    options.prng.fill(str);
                    @field(object, field.name) = str;
                },
                u32, u16, u8 => |Int| {
                    @field(object, field.name) = options.prng.int(Int);
                },
                i64, i32, i16, i8 => |Int| {
                    const Unsigned = std.meta.Int(.unsigned, @bitSizeOf(Int));
                    @field(object, field.name) = @bitCast(options.prng.int(Unsigned));
                },
                *const TestingTable => {
                    @field(object, field.name) = if (options.recursive)
                        try random(options)
                    else
                        null;
                },
                Timestamp => {
                    @field(object, field.name) = options.prng.int(Timestamp);
                },
                else => comptime unreachable,
            }
        }
        return object;
    }
};

pub const TestingBasicProperties = struct {
    pub fn random(options: struct {
        arena: std.mem.Allocator,
        prng: *stdx.PRNG,
        default: Encoder.BasicProperties = .{},
    }) !Encoder.BasicProperties {
        const is_null = stdx.PRNG.ratio(5, 100);
        var properties: Encoder.BasicProperties = .{};
        inline for (std.meta.fields(Encoder.BasicProperties)) |field| {
            if (@field(options.default, field.name)) |default| {
                @field(properties, field.name) = default;
            } else if (options.prng.chance(is_null)) {
                @field(properties, field.name) = null;
            } else switch (std.meta.Child(field.type)) {
                []const u8 => {
                    const size = options.prng.range_inclusive(u32, 0, 255);
                    const str = try options.arena.alloc(u8, size);
                    options.prng.fill(str);
                    @field(properties, field.name) = str;
                },
                u64, u8 => |Int| {
                    @field(properties, field.name) = options.prng.int(Int);
                },
                DeliveryMode => {
                    @field(properties, field.name) = options.prng.enum_uniform(DeliveryMode);
                },
                Encoder.Table => {
                    const object = try TestingTable.random(.{
                        .arena = options.arena,
                        .prng = options.prng,
                        .recursive = false,
                    });
                    @field(properties, field.name) = object.table();
                },
                else => comptime unreachable,
            }
        }
        return properties;
    }

    pub fn eql(
        arena: std.mem.Allocator,
        properties1: Encoder.BasicProperties,
        properties2: Decoder.BasicProperties,
    ) !bool {
        inline for (std.meta.fields(Encoder.BasicProperties)) |field| {
            const both_null = @field(properties1, field.name) == null and
                @field(properties2, field.name) == null;
            if (!both_null) {
                const value1 = @field(properties1, field.name) orelse return false;
                const value2 = @field(properties2, field.name) orelse return false;

                const equals = switch (std.meta.Child(field.type)) {
                    []const u8 => std.mem.eql(u8, value1, value2),
                    u64, u8 => value1 == value2,
                    DeliveryMode => value1 == value2,
                    Encoder.Table => eql: {
                        const encoded_object: *const TestingTable = @ptrCast(@alignCast(
                            value1.context,
                        ));
                        const decoded_object: *const TestingTable = try TestingTable.from_table(
                            arena,
                            value2,
                        );
                        break :eql TestingTable.eql(encoded_object, decoded_object);
                    },
                    else => comptime unreachable,
                };
                if (!equals) return false;
            }
        }
        return true;
    }
};
