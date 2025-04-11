const std = @import("std");
const stdx = @import("../../../stdx.zig");
const assert = std.debug.assert;

const spec = @import("spec.zig");

const FrameType = enum(u8) {
    method = spec.FRAME_METHOD,
    header = spec.FRAME_HEADER,
    body = spec.FRAME_BODY,
    heartbeat = spec.FRAME_HEARTBEAT,
};

pub const FrameEnd = enum(u8) {
    value = spec.FRAME_END,
};

pub const FrameHeader = extern struct {
    pub const SIZE = @sizeOf(std.meta.FieldType(FrameHeader, .type)) +
        @sizeOf(std.meta.FieldType(FrameHeader, .channel)) +
        @sizeOf(std.meta.FieldType(FrameHeader, .size));

    type: FrameType,
    channel: u16,
    size: u32,
};

pub const FrameHeaderReference = struct {
    frame_header: FrameHeader,
    index: usize,
};

pub const MethodHeader = packed struct(u32) {
    pub const SIZE = @sizeOf(std.meta.FieldType(MethodHeader, .class)) +
        @sizeOf(std.meta.FieldType(MethodHeader, .method));

    class: u16,
    method: u16,
};

pub const Header = struct {
    pub const SIZE = @sizeOf(std.meta.FieldType(Header, .class)) +
        @sizeOf(std.meta.FieldType(Header, .weight)) +
        @sizeOf(std.meta.FieldType(Header, .body_size));

    class: u16,
    weight: u16,
    body_size: u64,
    property_flags: u16,
    properties: []const u8,

    pub fn basic_properties(self: *const Header) BasicProperties {
        return BasicProperties.read(
            self.property_flags,
            self.properties,
        ) catch unreachable; // Already validated during `read_header()`.
    }
};

pub const HeaderReference = struct {
    class: u16,
    weight: u16,
    index: usize,
};

pub const DeliveryMode = enum(u8) { transient = 1, persistent = 2 };

pub const BasicProperties = struct {
    pub const SIZE_MAX = 256;

    content_type: ?[]const u8 = null,
    content_encoding: ?[]const u8 = null,
    headers: ?Table = null,
    delivery_mode: ?DeliveryMode = null,
    priority: ?u8 = null,
    correlation_id: ?[]const u8 = null,
    reply_to: ?[]const u8 = null,
    expiration: ?[]const u8 = null,
    message_id: ?[]const u8 = null,
    timestamp: ?u64 = null,
    type: ?[]const u8 = null,
    user_id: ?[]const u8 = null,
    app_id: ?[]const u8 = null,
    cluster_id: ?[]const u8 = null,

    fn read(flags: u16, content: []const u8) Error!BasicProperties {
        var reader = Reader.init(content);
        var bitset: stdx.BitSetType(16) = .{ .bits = @bitReverse(flags) };
        var properties: BasicProperties = .{};
        inline for (std.meta.fields(BasicProperties), 0..) |field, index| {
            if (bitset.is_set(index)) {
                @field(properties, field.name) = switch (std.meta.Child(field.type)) {
                    []const u8 => try reader.read_short_string(),
                    Table => try reader.read_table(),
                    DeliveryMode => try reader.read_enum(DeliveryMode),
                    u64 => try reader.read_int(u64),
                    u8 => try reader.read_int(u8),
                    else => comptime unreachable,
                };
            }
        }
        assert(reader.index == content.len);
        return properties;
    }

    fn property_flags(self: *const BasicProperties) u16 {
        var bitset: stdx.BitSetType(16) = .{};
        inline for (std.meta.fields(BasicProperties), 0..) |field, index| {
            bitset.set_value(index, @field(self, field.name) != null);
        }
        return @bitReverse(bitset.bits);
    }

    fn write(self: *const BasicProperties, writer: *Writer) void {
        const initial_index = writer.index;
        inline for (std.meta.fields(BasicProperties)) |field| {
            if (@field(self, field.name)) |value| {
                switch (@TypeOf(value)) {
                    []const u8 => writer.write_short_string(value),
                    Table => writer.write_table(value),
                    DeliveryMode => writer.write_int(u8, @intFromEnum(value)),
                    u64 => writer.write_int(u64, value),
                    u8 => writer.write_int(u8, value),
                    else => unreachable,
                }
            }
        }
        assert(writer.index < initial_index + SIZE_MAX);
    }

    comptime {
        @setEvalBranchQuota(10_000);

        // Sets the field with an `undefined` value, just to compute the `property_flags`.
        const set_flag = struct {
            fn set_flag(comptime field: std.meta.FieldEnum(BasicProperties)) u16 {
                var properties: BasicProperties = .{};
                const NotNull = std.meta.Child(@TypeOf(
                    @field(properties, @tagName(field)),
                ));
                const not_null: NotNull = undefined;
                @field(properties, @tagName(field)) = not_null;

                return properties.property_flags();
            }
        }.set_flag;

        // The last bit corresponding to the first property.
        assert(set_flag(.content_type) == 0x8000);
        assert(set_flag(.content_encoding) == 0x4000);
        assert(set_flag(.headers) == 0x2000);
        assert(set_flag(.delivery_mode) == 0x1000);
        assert(set_flag(.priority) == 0x0800);
        assert(set_flag(.correlation_id) == 0x0400);
        assert(set_flag(.reply_to) == 0x0200);
        assert(set_flag(.expiration) == 0x0100);
        assert(set_flag(.message_id) == 0x0080);
        assert(set_flag(.timestamp) == 0x0040);
        assert(set_flag(.type) == 0x0020);
        assert(set_flag(.user_id) == 0x0010);
        assert(set_flag(.app_id) == 0x0008);
    }
};

/// AMQP decimal type.
pub const Decimal = struct {
    scale: u8,
    value: i32,
};

/// AMQP Table content.
pub const Table = struct {
    pub const Iterator = struct {
        reader: Reader,

        pub fn reset(self: *Iterator) void {
            self.reader.reset();
        }

        pub fn next(self: *Iterator) Error!?struct {
            key: []const u8,
            value: FieldValue,
        } {
            if (self.reader.remaining_size() == 0) return null;
            return .{
                .key = try self.reader.read_short_string(),
                .value = try self.reader.read_field(),
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
            .reader = Reader.init(self.slice()),
        };
    }
};

pub const TableWriter = struct {
    writer: Writer,

    pub fn init(buffer: []u8) TableWriter {
        return .{
            .writer = Writer.init(buffer),
        };
    }

    pub fn put(self: *TableWriter, key: []const u8, value: FieldValue) void {
        self.writer.write_short_string(key);
        self.writer.write_field(value);
    }

    pub fn table(self: *const TableWriter) Table {
        return .{
            .length = @intCast(self.writer.index),
            .pointer = self.writer.buffer.ptr,
        };
    }
};

/// AMQP Array content.
pub const Array = struct {
    pub const Iterator = struct {
        reader: Reader,

        pub fn reset(self: *Iterator) void {
            self.reader.reset();
        }

        pub fn next(self: *Iterator) Error!?FieldValue {
            if (self.reader.remaining_size() == 0) return null;
            return try self.reader.read_field();
        }
    };

    length: u32,
    pointer: [*]const u8,

    pub fn init(value: []const u8) Array {
        assert(value.len <= std.math.maxInt(u32));
        return .{
            .length = @intCast(value.len),
            .pointer = value.ptr,
        };
    }

    pub fn slice(self: Array) []const u8 {
        return self.pointer[0..self.length];
    }

    pub fn iterator(self: Array) Iterator {
        return .{
            .reader = Reader.init(self.slice()),
        };
    }
};

pub const FieldType = enum(u8) {
    boolean = 't',
    short_short_int = 'b',
    short_short_uint = 'B',
    short_int = 's',
    short_uint = 'u',
    long_int = 'I',
    long_uint = 'i',
    long_long_int = 'L',
    long_long_uint = 'l',
    float = 'f',
    double = 'd',
    decimal_value = 'D',
    string = 'S',
    field_array = 'A',
    timestamp = 'T',
    field_table = 'F',
    void = 'V',
};

pub const FieldValue = union(FieldType) {
    boolean: bool,
    short_short_int: i8,
    short_short_uint: u8,
    short_int: i16,
    short_uint: u16,
    long_int: i32,
    long_uint: u32,
    long_long_int: i64,
    long_long_uint: u64,
    float: f32,
    double: f64,
    decimal_value: Decimal,
    string: []const u8,
    field_array: Array,
    timestamp: u64,
    field_table: Table,
    void,
};

pub const Error = error{
    BufferExhausted,
    Unexpected,
};

pub const Reader = struct {
    buffer: []const u8,
    index: usize,

    pub fn init(buffer: []const u8) Reader {
        return .{
            .buffer = buffer,
            .index = 0,
        };
    }

    pub fn reset(self: *Reader) void {
        self.* = .{
            .buffer = self.buffer,
            .index = 0,
        };
    }

    pub fn remaining_size(self: *const Reader) usize {
        return self.buffer.len - self.index;
    }

    pub fn read_int(self: *Reader, comptime T: type) Error!T {
        comptime assert(@typeInfo(T) == .Int);
        comptime assert(@sizeOf(T) == 1 or @sizeOf(T) == 2 or @sizeOf(T) == 4 or @sizeOf(T) == 8);
        if (self.index + @sizeOf(T) > self.buffer.len) return error.BufferExhausted;
        const value: T = std.mem.readInt(T, self.buffer[self.index..][0..@sizeOf(T)], .big);
        self.index += @sizeOf(T);
        assert(self.index <= self.buffer.len);
        return value;
    }

    pub fn read_enum(self: *Reader, comptime Enum: type) Error!Enum {
        comptime assert(@typeInfo(Enum) == .Enum);
        const Int = std.meta.Tag(Enum);
        const value = try self.read_int(Int);
        return std.meta.intToEnum(
            Enum,
            value,
        ) catch |err| switch (err) {
            error.InvalidEnumTag => return error.Unexpected,
        };
    }

    pub fn read_bool(self: *Reader) Error!bool {
        const value = try self.read_int(u8);
        return value != 0;
    }

    pub fn read_bitset(self: *Reader) Error!stdx.BitSetType(8) {
        const value = try self.read_int(u8);
        return .{
            .bits = value,
        };
    }

    pub fn read_short_string(self: *Reader) Error![]const u8 {
        const length: u8 = try self.read_int(u8);
        if (self.index + length > self.buffer.len) return error.BufferExhausted;
        const value = self.buffer[self.index..][0..length];
        self.index += length;
        assert(self.index <= self.buffer.len);
        return value;
    }

    pub fn read_long_string(self: *Reader) Error![]const u8 {
        const length: u32 = try self.read_int(u32);
        if (self.index + length > self.buffer.len) return error.BufferExhausted;
        const value = self.buffer[self.index..][0..length];
        self.index += length;
        assert(self.index <= self.buffer.len);
        return value;
    }

    pub fn read_table(self: *Reader) Error!Table {
        const length: u32 = try self.read_int(u32);
        if (self.index + length > self.buffer.len) return error.BufferExhausted;
        const value = self.buffer[self.index..][0..length];
        self.index += length;
        assert(self.index <= self.buffer.len);
        return Table.init(value);
    }

    pub fn read_array(self: *Reader) Error!Array {
        const length: u32 = try self.read_int(u32);
        if (self.index + length > self.buffer.len) return error.BufferExhausted;
        const value = self.buffer[self.index..][0..length];
        self.index += length;
        assert(self.index <= self.buffer.len);
        return Array.init(value);
    }

    pub fn read_float(self: *Reader) Error!f32 {
        _ = self;
        // IEEE-754 big endian.
        @panic("read_float intentionally not implemented");
    }

    pub fn read_double(self: *Reader) Error!f64 {
        _ = self;
        // rfc1832 XDR double big endian.
        @panic("read_double intentionally not implemented");
    }

    pub fn read_decimal(self: *Reader) Error!Decimal {
        return .{
            .scale = try self.read_int(u8),
            .value = try self.read_int(i32),
        };
    }

    pub fn read_field(self: *Reader) Error!FieldValue {
        const field_type = try self.read_enum(FieldType);
        const value: FieldValue = switch (field_type) {
            .boolean => .{ .boolean = try self.read_bool() },
            .short_short_int => .{ .short_short_int = try self.read_int(i8) },
            .short_short_uint => .{ .short_short_uint = try self.read_int(u8) },
            .short_int => .{ .short_int = try self.read_int(i16) },
            .short_uint => .{ .short_uint = try self.read_int(u16) },
            .long_int => .{ .long_int = try self.read_int(i32) },
            .long_uint => .{ .long_uint = try self.read_int(u32) },
            .long_long_int => .{ .long_long_int = try self.read_int(i64) },
            .long_long_uint => .{ .long_long_uint = try self.read_int(u64) },
            .float => .{ .float = try self.read_float() },
            .double => .{ .double = try self.read_double() },
            .decimal_value => .{ .decimal_value = try self.read_decimal() },
            .string => .{ .string = try self.read_long_string() },
            .field_array => .{ .field_array = try self.read_array() },
            .timestamp => .{ .timestamp = try self.read_int(u64) },
            .field_table => .{ .field_table = try self.read_table() },
            .void => .void,
        };
        assert(value == field_type);
        return value;
    }

    pub fn read_frame_header(self: *Reader) Error!FrameHeader {
        return .{
            .type = try self.read_enum(FrameType),
            .channel = try self.read_int(u16),
            .size = try self.read_int(u32),
        };
    }

    pub fn read_frame_end(self: *Reader) Error!void {
        _ = try self.read_enum(FrameEnd);
    }

    pub fn read_method_header(self: *Reader) Error!MethodHeader {
        return .{
            .class = try self.read_int(u16),
            .method = try self.read_int(u16),
        };
    }

    pub fn read_header(self: *Reader, frame_size: usize) Error!Header {
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

    pub fn read_body(self: *Reader, frame_size: usize) Error![]const u8 {
        if (self.index + frame_size > self.buffer.len) return error.BufferExhausted;
        const body = self.buffer[self.index..][0..frame_size];
        self.index += frame_size;
        assert(self.index <= self.buffer.len);
        try self.read_frame_end();
        return body;
    }
};

pub const Writer = struct {
    buffer: []u8,
    index: usize,

    pub fn init(buffer: []u8) Writer {
        return .{
            .buffer = buffer,
            .index = 0,
        };
    }

    pub fn reset(self: *Writer) void {
        self.* = .{
            .buffer = self.buffer,
            .index = 0,
        };
    }

    pub fn slice(self: *const Writer) []const u8 {
        assert(self.index <= self.buffer.len);
        return self.buffer[0..self.index];
    }

    pub fn write_int(self: *Writer, comptime T: type, value: T) void {
        comptime assert(@typeInfo(T) == .Int);
        comptime assert(@sizeOf(T) == 1 or @sizeOf(T) == 2 or @sizeOf(T) == 4 or @sizeOf(T) == 8);
        assert(self.index + @sizeOf(T) <= self.buffer.len);
        std.mem.writeInt(T, self.buffer[self.index..][0..@sizeOf(T)], value, .big);
        self.index += @sizeOf(T);
        assert(self.index <= self.buffer.len);
    }

    pub fn write_bool(self: *Writer, value: bool) void {
        self.write_int(u8, @intFromBool(value));
    }

    pub fn write_short_string(self: *Writer, value: []const u8) void {
        assert(value.len <= std.math.maxInt(u8));
        self.write_int(u8, @intCast(value.len));
        assert(self.index + value.len <= self.buffer.len);
        stdx.copy_left(.inexact, u8, self.buffer[self.index..], value);
        self.index += value.len;
    }

    pub fn write_long_string(self: *Writer, value: []const u8) void {
        assert(value.len <= std.math.maxInt(u32));
        self.write_int(u32, @intCast(value.len));
        assert(self.index + value.len <= self.buffer.len);
        stdx.copy_left(.inexact, u8, self.buffer[self.index..], value);
        self.index += value.len;
    }

    pub fn write_table(self: *Writer, table: Table) void {
        self.write_int(u32, table.length);
        assert(self.index + table.length <= self.buffer.len);
        stdx.copy_left(.inexact, u8, self.buffer[self.index..], table.slice());
        self.index += table.length;
    }

    pub fn write_array(self: *Writer, array: Array) void {
        self.write_int(u32, array.length);
        assert(self.index + array.length <= self.buffer.len);
        stdx.copy_left(.inexact, u8, self.buffer[self.index..], array.slice());
        self.index += array.length;
    }

    pub fn write_float(self: *Writer, value: f32) void {
        _ = self;
        _ = value;
        // IEEE-754 big endian.
        @panic("read_float intentionally not implemented");
    }

    pub fn write_double(self: *Writer, value: f64) void {
        _ = self;
        _ = value;
        // rfc1832 XDR double big endian.
        @panic("read_double intentionally not implemented");
    }

    pub fn write_decimal(self: *Writer, value: Decimal) void {
        self.write_int(u8, value.scale);
        self.write_int(i32, value.value);
    }

    pub fn write_field(self: *Writer, field: FieldValue) void {
        const field_type: FieldType = field;
        self.write_int(u8, @intFromEnum(field_type));
        switch (field) {
            .boolean => |value| self.write_bool(value),
            .short_short_int => |value| self.write_int(i8, value),
            .short_short_uint => |value| self.write_int(u8, value),
            .short_int => |value| self.write_int(i16, value),
            .short_uint => |value| self.write_int(u16, value),
            .long_int => |value| self.write_int(i32, value),
            .long_uint => |value| self.write_int(u32, value),
            .long_long_int => |value| self.write_int(i64, value),
            .long_long_uint => |value| self.write_int(u64, value),
            .float => |value| self.write_float(value),
            .double => |value| self.write_double(value),
            .decimal_value => |value| self.write_decimal(value),
            .string => |value| self.write_long_string(value),
            .field_array => |value| self.write_array(value),
            .timestamp => |value| self.write_int(u64, value),
            .field_table => |value| self.write_table(value),
            .void => {},
        }
    }

    pub fn write_frame_end(self: *Writer) void {
        self.write_int(u8, @intFromEnum(FrameEnd.value));
    }

    pub fn begin_frame(self: *Writer, frame_header: FrameHeader) FrameHeaderReference {
        assert(self.index + FrameHeader.SIZE <= self.buffer.len);
        assert(frame_header.size == 0);
        // Reserve the frame header bytes to be updated by `finish_frame()`.
        const frame_header_index = self.index;
        self.index += FrameHeader.SIZE;
        return .{
            .frame_header = frame_header,
            .index = frame_header_index,
        };
    }

    pub fn finish_frame(self: *Writer, reference: FrameHeaderReference) void {
        assert(reference.index < self.index);
        {
            const restore_index = self.index;
            defer self.index = restore_index;
            self.index = reference.index;

            self.write_int(u8, @intFromEnum(reference.frame_header.type));
            self.write_int(u16, reference.frame_header.channel);
            // The frame size field in the FrameHeader must be updated.
            // It represents the payload size, excluding the FrameHeader
            // and the frame end byte.
            self.write_int(u32, @intCast(restore_index - reference.index - FrameHeader.SIZE));
        }
        self.write_int(u8, spec.FRAME_END);
    }

    pub fn write_method_header(self: *Writer, method_header: MethodHeader) void {
        self.write_int(u16, method_header.class);
        self.write_int(u16, method_header.method);
    }

    pub fn begin_header(self: *Writer, header: struct {
        class: u16,
        weight: u16,
        properties: BasicProperties,
    }) HeaderReference {
        // Reserve the frame header bytes to be updated by `finish_frame()`.
        const header_index = self.index;
        self.index += Header.SIZE;
        self.write_int(u16, header.properties.property_flags());
        header.properties.write(self);

        return .{
            .class = header.class,
            .weight = header.weight,
            .index = header_index,
        };
    }

    pub fn finish_header(self: *Writer, reference: HeaderReference, body_size: u64) void {
        assert(reference.index < self.index);
        const restore_index = self.index;
        defer self.index = restore_index;
        self.index = reference.index;

        self.write_int(u16, reference.class);
        self.write_int(u16, reference.weight);
        // The body size field in the Header must be updated.
        self.write_int(u64, body_size);
    }

    pub fn write_body(self: *Writer, body: []const u8) void {
        stdx.copy_left(.inexact, u8, self.buffer[self.index..], body);
        self.index += body.len;
    }
};
