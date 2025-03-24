const std = @import("std");

const vsr = @import("vsr");
const stdx = vsr.stdx;
const tb = vsr.tigerbeetle;
const tb_client = vsr.tb_client;
const exports = tb_client.exports;
const assert = std.debug.assert;

const TypeMapping = struct {
    name: []const u8,
    private_fields: []const []const u8 = &.{},
    readonly_fields: []const []const u8 = &.{},
    docs_link: ?[]const u8 = null,
    visibility: enum { public, internal } = .public,
    constants: []const u8 = "",

    pub fn is_private(comptime self: @This(), name: []const u8) bool {
        inline for (self.private_fields) |field| {
            if (std.mem.eql(u8, field, name)) {
                return true;
            }
        } else return false;
    }

    pub fn is_read_only(comptime self: @This(), name: []const u8) bool {
        inline for (self.readonly_fields) |field| {
            if (std.mem.eql(u8, field, name)) {
                return true;
            }
        } else return false;
    }
};

/// Some 128-bit fields are better represented as `java.math.BigInteger`,
/// otherwise they are considered IDs and exposed as an array of bytes.
const big_integer = struct {
    const fields = .{
        "credits_posted",
        "credits_pending",
        "debits_posted",
        "debits_pending",
        "amount",
    };

    fn contains(comptime field: []const u8) bool {
        return comptime blk: for (fields) |value| {
            if (std.mem.eql(u8, field, value)) break :blk true;
        } else false;
    }

    fn contains_any(comptime type_info: anytype) bool {
        return comptime blk: for (type_info.fields) |field| {
            if (contains(field.name)) break :blk true;
        } else false;
    }
};

const type_mappings = .{
    .{ tb.AccountFlags, TypeMapping{
        .name = "AccountFlags",
        .private_fields = &.{"padding"},
        .docs_link = "reference/account#flags",
    } },
    .{ tb.TransferFlags, TypeMapping{
        .name = "TransferFlags",
        .private_fields = &.{"padding"},
        .docs_link = "reference/transfer#flags",
    } },
    .{ tb.AccountFilterFlags, TypeMapping{
        .name = "AccountFilterFlags",
        .private_fields = &.{"padding"},
        .visibility = .internal,
    } },
    .{ tb.QueryFilterFlags, TypeMapping{
        .name = "QueryFilterFlags",
        .private_fields = &.{"padding"},
        .visibility = .internal,
    } },
    .{ tb.Account, TypeMapping{
        .name = "AccountBatch",
        .private_fields = &.{"reserved"},
        .readonly_fields = &.{
            "debits_pending",
            "credits_pending",
            "debits_posted",
            "credits_posted",
        },
        .docs_link = "reference/account#",
    } },
    .{ tb.AccountBalance, TypeMapping{
        .name = "AccountBalanceBatch",
        .private_fields = &.{"reserved"},
        .readonly_fields = &.{
            "debits_pending",
            "credits_pending",
            "debits_posted",
            "credits_posted",
            "timestamp",
        },
        .docs_link = "reference/account-balances#",
    } },
    .{
        tb.Transfer, TypeMapping{
            .name = "TransferBatch",
            .private_fields = &.{"reserved"},
            .readonly_fields = &.{},
            .docs_link = "reference/transfer#",
            .constants =
            \\    public static final BigInteger AMOUNT_MAX = UInt128.asBigInteger(-1L, -1L);
            \\
            ,
        },
    },
    .{ tb.CreateAccountResult, TypeMapping{
        .name = "CreateAccountResult",
        .docs_link = "reference/requests/create_accounts#",
    } },
    .{ tb.CreateTransferResult, TypeMapping{
        .name = "CreateTransferResult",
        .docs_link = "reference/requests/create_transfers#",
    } },
    .{ tb.CreateAccountsResult, TypeMapping{
        .name = "CreateAccountResultBatch",
        .readonly_fields = &.{ "index", "result" },
    } },
    .{ tb.CreateTransfersResult, TypeMapping{
        .name = "CreateTransferResultBatch",
        .readonly_fields = &.{ "index", "result" },
    } },
    .{ tb.AccountFilter, TypeMapping{
        .name = "AccountFilterBatch",
        .visibility = .internal,
        .private_fields = &.{"reserved"},
    } },
    .{ tb.QueryFilter, TypeMapping{
        .name = "QueryFilterBatch",
        .visibility = .internal,
        .private_fields = &.{"reserved"},
    } },
    .{ exports.tb_init_status, TypeMapping{
        .name = "InitializationStatus",
    } },
    .{ exports.tb_client_status, TypeMapping{
        .name = "ClientStatus",
        .visibility = .internal,
    } },
    .{ exports.tb_packet_status, TypeMapping{
        .name = "PacketStatus",
    } },
};

const auto_generated_code_notice =
    \\//////////////////////////////////////////////////////////
    \\// This file was auto-generated by java_bindings.zig
    \\// Do not manually modify.
    \\//////////////////////////////////////////////////////////
    \\
;

fn java_type(
    comptime Type: type,
) []const u8 {
    switch (@typeInfo(Type)) {
        .Enum => return comptime get_mapped_type_name(Type) orelse @compileError(
            "Type " ++ @typeName(Type) ++ " not mapped.",
        ),
        .Struct => |info| switch (info.layout) {
            .@"packed" => return comptime java_type(std.meta.Int(.unsigned, @bitSizeOf(Type))),
            else => return comptime get_mapped_type_name(Type) orelse @compileError(
                "Type " ++ @typeName(Type) ++ " not mapped.",
            ),
        },
        .Int => |info| {
            // For better API ergonomy,
            // we expose 16-bit unsigned integers in Java as "int" instead of "short".
            // Even though, the backing fields are always stored as "short".
            assert(info.signedness == .unsigned);
            return switch (info.bits) {
                1 => "byte",
                8 => "byte",
                16, 32 => "int",
                64 => "long",
                else => @compileError("invalid int type"),
            };
        },
        else => @compileError("Unhandled type: " ++ @typeName(Type)),
    }
}

fn get_mapped_type_name(comptime Type: type) ?[]const u8 {
    inline for (type_mappings) |type_mapping| {
        if (Type == type_mapping[0]) {
            return type_mapping[1].name;
        }
    } else return null;
}

fn to_case(
    comptime input: []const u8,
    comptime case: enum { camel, pascal, upper },
) []const u8 {
    // TODO(Zig): Cleanup when this is fixed after Zig 0.11.
    // Without comptime blk, the compiler thinks slicing the output on return happens at runtime.
    return comptime blk: {
        var output: [input.len]u8 = undefined;
        if (case == .upper) {
            const len = std.ascii.upperString(output[0..], input).len;
            break :blk stdx.comptime_slice(&output, len);
        } else {
            var len: usize = 0;
            var iterator = std.mem.tokenizeScalar(u8, input, '_');
            while (iterator.next()) |word| {
                _ = std.ascii.lowerString(output[len..], word);
                output[len] = std.ascii.toUpper(output[len]);
                len += word.len;
            }

            output[0] = switch (case) {
                .camel => std.ascii.toLower(output[0]),
                .pascal => std.ascii.toUpper(output[0]),
                .upper => unreachable,
            };

            break :blk stdx.comptime_slice(&output, len);
        }
    };
}

fn emit_enum(
    buffer: *std.ArrayList(u8),
    comptime Type: type,
    comptime mapping: TypeMapping,
    comptime int_type: []const u8,
) !void {
    try buffer.writer().print(
        \\{[notice]s}
        \\package com.tigerbeetle;
        \\
        \\{[visibility]s}enum {[name]s} {{
        \\
    , .{
        .visibility = if (mapping.visibility == .internal) "" else "public ",
        .notice = auto_generated_code_notice,
        .name = mapping.name,
    });

    const fields = comptime fields: {
        const EnumField = std.builtin.Type.EnumField;
        const type_info = @typeInfo(Type).Enum;
        var fields: []const EnumField = &[_]EnumField{};
        for (type_info.fields) |field| {
            if (mapping.is_private(field.name)) continue;
            if (std.mem.startsWith(u8, field.name, "deprecated_")) continue;
            fields = fields ++ [_]EnumField{field};
        }
        break :fields fields;
    };

    inline for (fields, 0..) |field, i| {
        if (mapping.docs_link) |docs_link| {
            try buffer.writer().print(
                \\
                \\    /**
                \\     * @see <a href="https://docs.tigerbeetle.com/{[docs_link]s}{[field_name]s}">{[field_name]s}</a>
                \\     */
                \\
            , .{
                .docs_link = docs_link,
                .field_name = field.name,
            });
        }

        try buffer.writer().print(
            \\    {[enum_name]s}(({[int_type]s}) {[value]d}){[separator]c}
            \\
        , .{
            .enum_name = to_case(field.name, .pascal),
            .int_type = int_type,
            .value = @intFromEnum(@field(Type, field.name)),
            .separator = if (i == fields.len - 1) ';' else ',',
        });
    }

    try buffer.writer().print(
        \\
        \\    public final {[int_type]s} value;
        \\
        \\    {[name]s}({[int_type]s} value) {{
        \\        this.value = value;
        \\    }}
        \\
        \\    public static {[name]s} fromValue({[int_type]s} value) {{
        \\        switch (value) {{
        \\
    , .{
        .int_type = int_type,
        .name = mapping.name,
    });

    inline for (fields) |field| {
        try buffer.writer().print(
            \\            case {[value]d}: return {[enum_name]s};
            \\
        , .{
            .enum_name = to_case(field.name, .pascal),
            .value = @intFromEnum(@field(Type, field.name)),
        });
    }

    try buffer.writer().print(
        \\            default: throw new IllegalArgumentException(
        \\                String.format("Invalid {[name]s} value=%d", value));
        \\        }}
        \\    }}
        \\}}
        \\
        \\
    , .{
        .name = mapping.name,
    });
}

fn emit_packed_enum(
    buffer: *std.ArrayList(u8),
    comptime type_info: anytype,
    comptime mapping: TypeMapping,
    comptime int_type: []const u8,
) !void {
    try buffer.writer().print(
        \\{[notice]s}
        \\package com.tigerbeetle;
        \\
        \\{[visibility]s}interface {[name]s} {{
        \\    {[int_type]s} NONE = ({[int_type]s}) 0;
        \\
    , .{
        .visibility = if (mapping.visibility == .internal) "" else "public ",
        .notice = auto_generated_code_notice,
        .name = mapping.name,
        .int_type = int_type,
    });

    inline for (type_info.fields, 0..) |field, i| {
        if (comptime mapping.is_private(field.name)) continue;

        if (mapping.docs_link) |docs_link| {
            try buffer.writer().print(
                \\
                \\    /**
                \\     * @see <a href="https://docs.tigerbeetle.com/{[docs_link]s}{[field_name]s}">{[field_name]s}</a>
                \\     */
                \\
            , .{
                .docs_link = docs_link,
                .field_name = field.name,
            });
        }

        try buffer.writer().print(
            \\    {[int_type]s} {[enum_name]s} = ({[int_type]s}) (1 << {[value]d});
            \\
        , .{
            .int_type = int_type,
            .enum_name = to_case(field.name, .upper),
            .value = i,
        });
    }

    try buffer.writer().print("\n", .{});

    inline for (type_info.fields) |field| {
        if (comptime mapping.is_private(field.name)) continue;

        try buffer.writer().print(
            \\    static boolean has{[flag_name]s}(final {[int_type]s} flags) {{
            \\        return (flags & {[enum_name]s}) == {[enum_name]s};
            \\    }}
            \\
            \\
        , .{
            .flag_name = to_case(field.name, .pascal),
            .int_type = int_type,
            .enum_name = to_case(field.name, .upper),
        });
    }

    try buffer.writer().print(
        \\}}
        \\
    , .{});
}

fn batch_type(comptime Type: type) []const u8 {
    switch (@typeInfo(Type)) {
        .Int => |info| {
            assert(info.signedness == .unsigned);
            switch (info.bits) {
                16 => return "UInt16",
                32 => return "UInt32",
                64 => return "UInt64",
                else => {},
            }
        },
        .Struct => |info| switch (info.layout) {
            .@"packed" => return batch_type(std.meta.Int(.unsigned, @bitSizeOf(Type))),
            else => {},
        },
        .Enum => return batch_type(std.meta.Int(.unsigned, @bitSizeOf(Type))),
        else => {},
    }

    @compileError("Unhandled type: " ++ @typeName(Type));
}

fn emit_batch(
    buffer: *std.ArrayList(u8),
    comptime type_info: anytype,
    comptime mapping: TypeMapping,
    comptime size: usize,
) !void {
    try buffer.writer().print(
        \\{[notice]s}
        \\package com.tigerbeetle;
        \\
        \\import java.nio.ByteBuffer;
        \\{[big_integer_import]s}
        \\
        \\{[visibility]s}final class {[name]s} extends Batch {{
        \\
        \\{[constants]s}
        \\    interface Struct {{
        \\        int SIZE = {[size]d};
        \\
        \\
    , .{
        .visibility = if (mapping.visibility == .internal) "" else "public ",
        .notice = auto_generated_code_notice,
        .name = mapping.name,
        .size = size,
        .big_integer_import = if (big_integer.contains_any(type_info))
            "import java.math.BigInteger;"
        else
            "",
        .constants = mapping.constants,
    });

    // Fields offset:
    var offset: usize = 0;
    inline for (type_info.fields) |field| {
        try buffer.writer().print(
            \\        int {[field_name]s} = {[offset]d};
            \\
        , .{
            .field_name = to_case(field.name, .pascal),
            .offset = offset,
        });

        offset += @sizeOf(field.type);
    }

    // Constructors:
    try buffer.writer().print(
        \\    }}
        \\
        \\    /**
        \\     * Creates an empty batch with the desired maximum capacity.
        \\     * <p>
        \\     * Once created, an instance cannot be resized, however it may contain any number of elements
        \\     * between zero and its {{@link #getCapacity capacity}}.
        \\     *
        \\     * @param capacity the maximum capacity.
        \\     * @throws IllegalArgumentException if capacity is negative.
        \\     */
        \\    public {[name]s}(final int capacity) {{
        \\        super(capacity, Struct.SIZE);
        \\    }}
        \\
        \\    {[name]s}(final ByteBuffer buffer) {{
        \\        super(buffer, Struct.SIZE);
        \\    }}
        \\
        \\
    , .{
        .name = mapping.name,
    });

    // Properties:
    inline for (type_info.fields) |field| {
        if (field.type == u128) {
            try emit_u128_batch_accessors(buffer, mapping, field);
        } else {
            try emit_batch_accessors(buffer, mapping, field);
        }
    }

    try buffer.writer().print(
        \\}}
        \\
        \\
    , .{});
}

fn emit_batch_accessors(
    buffer: *std.ArrayList(u8),
    comptime mapping: TypeMapping,
    comptime field: anytype,
) !void {
    comptime assert(field.type != u128);
    const is_private = comptime mapping.is_private(field.name);
    const is_read_only = comptime mapping.is_read_only(field.name);

    // Get:
    try buffer.writer().print(
        \\    /**
        \\     * @throws IllegalStateException if not at a {{@link #isValidPosition valid position}}.
        \\
    , .{});

    if (mapping.docs_link) |docs_link| {
        try buffer.writer().print(
            \\     * @see <a href="https://docs.tigerbeetle.com/{[docs_link]s}{[field_name]s}">{[field_name]s}</a>
            \\     */
            \\
        , .{
            .docs_link = docs_link,
            .field_name = field.name,
        });
    } else {
        try buffer.writer().print(
            \\     */
            \\
        , .{});
    }

    if (@typeInfo(field.type) == .Array) {
        try buffer.writer().print(
            \\    {[visibility]s}byte[] get{[property]s}() {{
            \\        return getArray(at(Struct.{[property]s}), {[array_len]d});
            \\    }}
            \\
            \\
        , .{
            .visibility = if (is_private) "" else "public ",
            .property = to_case(field.name, .pascal),
            .array_len = @typeInfo(field.type).Array.len,
        });
    } else {
        try buffer.writer().print(
            \\    {[visibility]s}{[java_type]s} get{[property]s}() {{
            \\        final var value = get{[batch_type]s}(at(Struct.{[property]s}));
            \\        return {[return_expression]s};
            \\    }}
            \\
            \\
        , .{
            .visibility = if (is_private) "" else "public ",
            .java_type = java_type(field.type),
            .property = to_case(field.name, .pascal),
            .batch_type = batch_type(field.type),
            .return_expression = comptime if (@typeInfo(field.type) == .Enum)
                get_mapped_type_name(field.type).? ++ ".fromValue(value)"
            else
                "value",
        });
    }

    // Set:
    try buffer.writer().print(
        \\    /**
        \\     * @param {[param_name]s}
        \\     * @throws IllegalStateException if not at a {{@link #isValidPosition valid position}}.
        \\     * @throws IllegalStateException if a {{@link #isReadOnly() read-only}} batch.
        \\
    , .{
        .param_name = to_case(field.name, .camel),
    });

    if (mapping.docs_link) |docs_link| {
        try buffer.writer().print(
            \\     * @see <a href="https://docs.tigerbeetle.com/{[docs_link]s}{[field_name]s}">{[field_name]s}</a>
            \\     */
            \\
        , .{
            .docs_link = docs_link,
            .field_name = field.name,
        });
    } else {
        try buffer.writer().print(
            \\     */
            \\
        , .{});
    }

    if (@typeInfo(field.type) == .Array) {
        try buffer.writer().print(
            \\    {[visibility]s}void set{[property]s}(byte[] {[param_name]s}) {{
            \\        if ({[param_name]s} == null)
            \\            {[param_name]s} = new byte[{[array_len]d}];
            \\        if ({[param_name]s}.length != {[array_len]d})
            \\            throw new IllegalArgumentException("Reserved must be {[array_len]d} bytes long");
            \\        putArray(at(Struct.{[property]s}), {[param_name]s});
            \\    }}
            \\
            \\
        , .{
            .property = to_case(field.name, .pascal),
            .param_name = to_case(field.name, .camel),
            .visibility = if (is_private or is_read_only) "" else "public ",
            .array_len = @typeInfo(field.type).Array.len,
        });
    } else {
        try buffer.writer().print(
            \\    {[visibility]s}void set{[property]s}(final {[java_type]s} {[param_name]s}) {{
            \\        put{[batch_type]s}(at(Struct.{[property]s}), {[param_name]s}{[value_expression]s});
            \\    }}
            \\
            \\
        , .{
            .property = to_case(field.name, .pascal),
            .param_name = to_case(field.name, .camel),
            .visibility = if (is_private or is_read_only) "" else "public ",
            .batch_type = batch_type(field.type),
            .java_type = java_type(field.type),
            .value_expression = if (comptime @typeInfo(field.type) == .Enum)
                ".value"
            else
                "",
        });
    }
}

// We offer multiple APIs for dealing with UInt128 in Java:
// - A byte array, heap-allocated, for ids and user_data;
// - A BigInteger, heap-allocated, for balances and amounts;
// - Two 64-bit integers (long), stack-allocated, for both cases;
fn emit_u128_batch_accessors(
    buffer: *std.ArrayList(u8),
    comptime mapping: TypeMapping,
    comptime field: anytype,
) !void {
    comptime assert(field.type == u128);
    const is_private = comptime mapping.is_private(field.name);
    const is_read_only = comptime mapping.is_read_only(field.name);

    if (big_integer.contains(field.name)) {
        // Get BigInteger:
        try buffer.writer().print(
            \\    /**
            \\     * @return a {{@link java.math.BigInteger}} representing the 128-bit value.
            \\     * @throws IllegalStateException if not at a {{@link #isValidPosition valid position}}.
            \\
        , .{});

        if (mapping.docs_link) |docs_link| {
            try buffer.writer().print(
                \\     * @see <a href="https://docs.tigerbeetle.com/{[docs_link]s}{[field_name]s}">{[field_name]s}</a>
                \\     */
                \\
            , .{
                .docs_link = docs_link,
                .field_name = field.name,
            });
        } else {
            try buffer.writer().print(
                \\     */
                \\
            , .{});
        }

        try buffer.writer().print(
            \\    {[visibility]s}BigInteger get{[property]s}() {{
            \\        final var index = at(Struct.{[property]s});
            \\        return UInt128.asBigInteger(
            \\            getUInt128(index, UInt128.LeastSignificant),
            \\            getUInt128(index, UInt128.MostSignificant));
            \\    }}
            \\
            \\
        , .{
            .visibility = if (is_private) "" else "public ",
            .property = to_case(field.name, .pascal),
        });
    } else {
        // Get array:
        try buffer.writer().print(
            \\    /**
            \\     * @return an array of 16 bytes representing the 128-bit value.
            \\     * @throws IllegalStateException if not at a {{@link #isValidPosition valid position}}.
            \\
        , .{});

        if (mapping.docs_link) |docs_link| {
            try buffer.writer().print(
                \\     * @see <a href="https://docs.tigerbeetle.com/{[docs_link]s}{[field_name]s}">{[field_name]s}</a>
                \\     */
                \\
            , .{
                .docs_link = docs_link,
                .field_name = field.name,
            });
        } else {
            try buffer.writer().print(
                \\     */
                \\
            , .{});
        }

        try buffer.writer().print(
            \\    {[visibility]s}byte[] get{[property]s}() {{
            \\        return getUInt128(at(Struct.{[property]s}));
            \\    }}
            \\
            \\
        , .{
            .visibility = if (is_private) "" else "public ",
            .property = to_case(field.name, .pascal),
        });
    }

    // Get long:
    try buffer.writer().print(
        \\    /**
        \\     * @param part a {{@link UInt128}} enum indicating which part of the 128-bit value
        \\              is to be retrieved.
        \\     * @return a {{@code long}} representing the first 8 bytes of the 128-bit value if
        \\     *         {{@link UInt128#LeastSignificant}} is informed, or the last 8 bytes if
        \\     *         {{@link UInt128#MostSignificant}}.
        \\     * @throws IllegalStateException if not at a {{@link #isValidPosition valid position}}.
        \\
    , .{});

    if (mapping.docs_link) |docs_link| {
        try buffer.writer().print(
            \\     * @see <a href="https://docs.tigerbeetle.com/{[docs_link]s}{[field_name]s}">{[field_name]s}</a>
            \\     */
            \\
        , .{
            .docs_link = docs_link,
            .field_name = field.name,
        });
    } else {
        try buffer.writer().print(
            \\     */
            \\
        , .{});
    }

    try buffer.writer().print(
        \\    {[visibility]s}long get{[property]s}(final UInt128 part) {{
        \\        return getUInt128(at(Struct.{[property]s}), part);
        \\    }}
        \\
        \\
    , .{
        .visibility = if (is_private) "" else "public ",
        .property = to_case(field.name, .pascal),
    });

    if (big_integer.contains(field.name)) {
        // Set BigInteger:
        try buffer.writer().print(
            \\    /**
            \\     * @param {[param_name]s} a {{@link java.math.BigInteger}} representing the 128-bit value.
            \\     * @throws IllegalStateException if not at a {{@link #isValidPosition valid position}}.
            \\     * @throws IllegalStateException if a {{@link #isReadOnly() read-only}} batch.
            \\
        , .{
            .param_name = to_case(field.name, .camel),
        });

        if (mapping.docs_link) |docs_link| {
            try buffer.writer().print(
                \\     * @see <a href="https://docs.tigerbeetle.com/{[docs_link]s}{[field_name]s}">{[field_name]s}</a>
                \\     */
                \\
            , .{
                .docs_link = docs_link,
                .field_name = field.name,
            });
        } else {
            try buffer.writer().print(
                \\     */
                \\
            , .{});
        }

        try buffer.writer().print(
            \\    {[visibility]s}void set{[property]s}(final BigInteger {[param_name]s}) {{
            \\        putUInt128(at(Struct.{[property]s}), UInt128.asBytes({[param_name]s}));
            \\    }}
            \\
            \\
        , .{
            .visibility = if (is_private or is_read_only) "" else "public ",
            .property = to_case(field.name, .pascal),
            .param_name = to_case(field.name, .camel),
        });
    } else {
        // Set array:
        try buffer.writer().print(
            \\    /**
            \\     * @param {[param_name]s} an array of 16 bytes representing the 128-bit value.
            \\     * @throws IllegalArgumentException if {{@code {[param_name]s}}} is not 16 bytes long.
            \\     * @throws IllegalStateException if not at a {{@link #isValidPosition valid position}}.
            \\     * @throws IllegalStateException if a {{@link #isReadOnly() read-only}} batch.
            \\
        , .{
            .param_name = to_case(field.name, .camel),
        });

        if (mapping.docs_link) |docs_link| {
            try buffer.writer().print(
                \\     * @see <a href="https://docs.tigerbeetle.com/{[docs_link]s}{[field_name]s}">{[field_name]s}</a>
                \\     */
                \\
            , .{
                .docs_link = docs_link,
                .field_name = field.name,
            });
        } else {
            try buffer.writer().print(
                \\     */
                \\
            , .{});
        }

        try buffer.writer().print(
            \\    {[visibility]s}void set{[property]s}(final byte[] {[param_name]s}) {{
            \\        putUInt128(at(Struct.{[property]s}), {[param_name]s});
            \\    }}
            \\
            \\
        , .{
            .visibility = if (is_private or is_read_only) "" else "public ",
            .property = to_case(field.name, .pascal),
            .param_name = to_case(field.name, .camel),
        });
    }

    // Set long:
    try buffer.writer().print(
        \\    /**
        \\     * @param leastSignificant a {{@code long}} representing the first 8 bytes of the 128-bit value.
        \\     * @param mostSignificant a {{@code long}} representing the last 8 bytes of the 128-bit value.
        \\     * @throws IllegalStateException if not at a {{@link #isValidPosition valid position}}.
        \\     * @throws IllegalStateException if a {{@link #isReadOnly() read-only}} batch.
        \\
    , .{});

    if (mapping.docs_link) |docs_link| {
        try buffer.writer().print(
            \\     * @see <a href="https://docs.tigerbeetle.com/{[docs_link]s}{[field_name]s}">{[field_name]s}</a>
            \\     */
            \\
        , .{
            .docs_link = docs_link,
            .field_name = field.name,
        });
    } else {
        try buffer.writer().print(
            \\     */
            \\
        , .{});
    }

    try buffer.writer().print(
        \\    {[visibility]s}void set{[property]s}(final long leastSignificant, final long mostSignificant) {{
        \\        putUInt128(at(Struct.{[property]s}), leastSignificant, mostSignificant);
        \\    }}
        \\
        \\
    , .{
        .visibility = if (is_private or is_read_only) "" else "public ",
        .property = to_case(field.name, .pascal),
    });

    // Set long without most significant bits
    try buffer.writer().print(
        \\    /**
        \\     * @param leastSignificant a {{@code long}} representing the first 8 bytes of the 128-bit value.
        \\     * @throws IllegalStateException if not at a {{@link #isValidPosition valid position}}.
        \\     * @throws IllegalStateException if a {{@link #isReadOnly() read-only}} batch.
        \\
    , .{});

    if (mapping.docs_link) |docs_link| {
        try buffer.writer().print(
            \\     * @see <a href="https://docs.tigerbeetle.com/{[docs_link]s}{[field_name]s}">{[field_name]s}</a>
            \\     */
            \\
        , .{
            .docs_link = docs_link,
            .field_name = field.name,
        });
    } else {
        try buffer.writer().print(
            \\     */
            \\
        , .{});
    }

    try buffer.writer().print(
        \\    {[visibility]s}void set{[property]s}(final long leastSignificant) {{
        \\        putUInt128(at(Struct.{[property]s}), leastSignificant, 0);
        \\    }}
        \\
        \\
    , .{
        .visibility = if (is_private or is_read_only) "" else "public ",
        .property = to_case(field.name, .pascal),
    });
}

pub fn generate_bindings(
    comptime ZigType: type,
    comptime mapping: TypeMapping,
    buffer: *std.ArrayList(u8),
) !void {
    @setEvalBranchQuota(100_000);

    switch (@typeInfo(ZigType)) {
        .Struct => |info| switch (info.layout) {
            .auto => @compileError(
                "Only packed or extern structs are supported: " ++ @typeName(ZigType),
            ),
            .@"packed" => try emit_packed_enum(
                buffer,
                info,
                mapping,
                comptime java_type(std.meta.Int(.unsigned, @bitSizeOf(ZigType))),
            ),
            .@"extern" => try emit_batch(
                buffer,
                info,
                mapping,
                @sizeOf(ZigType),
            ),
        },
        .Enum => try emit_enum(
            buffer,
            ZigType,
            mapping,
            comptime java_type(std.meta.Int(.unsigned, @bitSizeOf(ZigType))),
        ),
        else => @compileError("Type cannot be represented: " ++ @typeName(ZigType)),
    }
}

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();
    assert(args.skip());
    const target_dir_path = args.next().?;
    assert(args.next() == null);

    var target_dir = try std.fs.cwd().openDir(target_dir_path, .{});
    defer target_dir.close();

    // Emit Java declarations.
    inline for (type_mappings) |type_mapping| {
        const ZigType = type_mapping[0];
        const mapping = type_mapping[1];

        var buffer = std.ArrayList(u8).init(allocator);
        try generate_bindings(ZigType, mapping, &buffer);

        try target_dir.writeFile(.{
            .sub_path = mapping.name ++ ".java",
            .data = buffer.items,
        });
    }

    {
        var buffer = std.ArrayList(u8).init(allocator);
        try buffer.writer().print(
            \\package com.tigerbeetle;
            \\
            \\interface TBClient {{
            \\    int SIZE = {};
            \\    int ALIGNMENT = {};
            \\}}
            \\
        , .{
            @sizeOf(exports.tb_client_t),
            @alignOf(exports.tb_client_t),
        });
        try target_dir.writeFile(.{
            .sub_path = "TBClient.java",
            .data = buffer.items,
        });
    }
}
