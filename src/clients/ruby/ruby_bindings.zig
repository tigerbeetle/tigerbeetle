const std = @import("std");
const assert = std.debug.assert;

const generator_options = @import("ruby_bindings_options");
const vsr = @import("vsr");
const exports = vsr.tb_client.exports;
const tb = vsr.tigerbeetle;
const stdx = vsr.stdx;

const flag_mappings = .{
    .{ tb.AccountFlags, "AccountFlags" },
    .{ tb.TransferFlags, "TransferFlags" },
    .{ tb.AccountFilterFlags, "AccountFilterFlags" },
    .{ tb.QueryFilterFlags, "QueryFilterFlags" },
};

const Buffer = struct {
    inner: std.ArrayList(u8),

    pub fn init(allocator: std.mem.Allocator) Buffer {
        return .{ .inner = std.ArrayList(u8).init(allocator) };
    }

    pub fn print(self: *Buffer, comptime format: []const u8, args: anytype) void {
        self.inner.writer().print(format, args) catch unreachable;
    }

    pub fn write(self: *Buffer, bytes: []const u8) void {
        self.inner.writer().writeAll(bytes) catch unreachable;
    }
};

fn ruby_flags_name_from_type(comptime Type: type) ?[]const u8 {
    comptime for (flag_mappings) |mapping| {
        const ZigType, const ruby_name = mapping;
        if (Type == ZigType) return ruby_name;
    };
    return null;
}

fn c_operation_name(comptime operation: tb.Operation) []const u8 {
    const upper_snake = stdx.to_case(@tagName(operation), .UPPER_CASE);
    return "TB_OPERATION_" ++ upper_snake;
}

fn c_init_status_name(comptime status: exports.tb_init_status) []const u8 {
    const upper_snake = stdx.to_case(@tagName(status), .UPPER_CASE);
    return "TB_INIT_" ++ upper_snake;
}

fn operation_supported(comptime operation: tb.Operation) bool {
    const name = @tagName(operation);
    if (comptime std.mem.startsWith(u8, name, "deprecated_")) return false;
    return switch (operation) {
        .pulse,
        .get_change_events,
        => false,
        else => true,
    };
}

fn type_basename(comptime Type: type) []const u8 {
    const name = @typeName(Type);
    if (comptime std.mem.lastIndexOfScalar(u8, name, '.')) |index| {
        return name[index + 1 ..];
    }
    return name;
}

fn to_snake_case(comptime input: []const u8) []const u8 {
    comptime var output: [input.len * 2]u8 = undefined;
    comptime var len: usize = 0;
    inline for (input, 0..) |c, i| {
        if (c >= 'A' and c <= 'Z') {
            if (i > 0) {
                output[len] = '_';
                len += 1;
            }
            output[len] = c + 32;
        } else {
            output[len] = c;
        }
        len += 1;
    }
    return output[0..len];
}

fn c_type_name(comptime Type: type) []const u8 {
    return "tb_" ++ to_snake_case(type_basename(Type)) ++ "_t";
}

fn ruby_type_name(comptime Type: type) []const u8 {
    return type_basename(Type);
}

fn operation_function_name(comptime operation: tb.Operation) []const u8 {
    return @tagName(operation);
}

fn int_bits(comptime Type: type) comptime_int {
    return switch (@typeInfo(Type)) {
        .int => |info| info.bits,
        .@"enum" => |info| @bitSizeOf(info.tag_type),
        .@"struct" => |info| switch (info.layout) {
            .@"packed" => @bitSizeOf(Type),
            else => @compileError("unsupported struct integer field: " ++ @typeName(Type)),
        },
        else => @compileError("unsupported integer field: " ++ @typeName(Type)),
    };
}

fn c_uint_type(comptime bits: comptime_int) []const u8 {
    return switch (bits) {
        8 => "uint8_t",
        16 => "uint16_t",
        32 => "uint32_t",
        64 => "uint64_t",
        128 => "tb_uint128_t",
        else => @compileError("unsupported integer width"),
    };
}

fn field_reserved(comptime field_name: []const u8) bool {
    if (comptime std.mem.eql(u8, field_name, "reserved")) return true;
    if (comptime std.mem.eql(u8, field_name, "padding")) return true;
    return false;
}

fn emit_flags_module(
    buffer: *Buffer,
    comptime Type: type,
    comptime ruby_name: []const u8,
) void {
    assert(@typeInfo(Type) == .@"struct");
    assert(@typeInfo(Type).@"struct".layout == .@"packed");

    buffer.print("  module {s}\n", .{ruby_name});
    buffer.print("    NONE = 0\n", .{});
    inline for (@typeInfo(Type).@"struct".fields, 0..) |field, i| {
        if (comptime std.mem.startsWith(u8, field.name, "deprecated_")) continue;
        if (comptime std.mem.eql(u8, field.name, "padding")) continue;
        const upper_snake = stdx.to_case(field.name, .UPPER_CASE);
        buffer.print("    {s} = 1 << {d}\n", .{ upper_snake, i });
    }
    buffer.print("  end\n\n", .{});
}

fn emit_enum_module(
    buffer: *Buffer,
    comptime Type: type,
    comptime ruby_name: []const u8,
    comptime skip_fields: []const []const u8,
) void {
    assert(@typeInfo(Type) == .@"enum");

    buffer.print("  module {s}\n", .{ruby_name});
    inline for (@typeInfo(Type).@"enum".fields) |field| {
        if (comptime std.mem.startsWith(u8, field.name, "deprecated_")) continue;
        comptime var skip = false;
        inline for (skip_fields) |sf| {
            skip = skip or comptime std.mem.eql(u8, sf, field.name);
        }
        if (skip) continue;
        const upper_snake = stdx.to_case(field.name, .UPPER_CASE);
        const value: u64 = @intCast(@intFromEnum(@field(Type, field.name)));
        buffer.print("    {s} = {d}\n", .{ upper_snake, value });
    }
    buffer.print("  end\n\n", .{});
}

fn emit_field_default(buffer: *Buffer, comptime FieldType: type) void {
    const type_info = @typeInfo(FieldType);
    const is_flags = type_info == .@"struct" and type_info.@"struct".layout == .@"packed";
    if (is_flags) {
        buffer.print("{s}::NONE", .{comptime ruby_flags_name_from_type(FieldType).?});
    } else {
        buffer.print("0", .{});
    }
}

fn emit_struct_class(
    buffer: *Buffer,
    comptime Type: type,
    comptime ruby_name: []const u8,
    comptime read_only: bool,
) void {
    assert(@typeInfo(Type) == .@"struct");
    assert(@typeInfo(Type).@"struct".layout == .@"extern");

    const fields = @typeInfo(Type).@"struct".fields;

    buffer.print("  class {s}\n", .{ruby_name});

    inline for (fields) |field| {
        if (comptime std.mem.eql(u8, field.name, "reserved")) continue;
        buffer.print(
            "    attr_{s} :{s}\n",
            .{ if (read_only) "reader" else "accessor", field.name },
        );
    }
    buffer.print("\n", .{});

    if (!read_only) {
        buffer.print("    def initialize(\n", .{});
        comptime var sep: []const u8 = "";
        inline for (fields) |field| {
            if (comptime std.mem.eql(u8, field.name, "reserved")) continue;
            if (sep.len > 0) buffer.print("{s}", .{sep});
            buffer.print("      {s}: ", .{field.name});
            emit_field_default(buffer, field.type);
            sep = ",\n";
        }
        buffer.print("\n    )\n", .{});
        inline for (fields) |field| {
            if (comptime std.mem.eql(u8, field.name, "reserved")) continue;
            buffer.print("      @{s} = {s}\n", .{ field.name, field.name });
        }
        buffer.print("    end\n", .{});
    } else {
        buffer.print("    def initialize\n", .{});
        inline for (fields) |field| {
            if (comptime std.mem.eql(u8, field.name, "reserved")) continue;
            buffer.print("      @{s} = 0\n", .{field.name});
        }
        buffer.print("    end\n", .{});
    }
    buffer.print("  end\n\n", .{});
}

fn emit_rbs_constants_module(
    buffer: *Buffer,
    comptime Type: type,
    comptime ruby_name: []const u8,
    comptime skip_fields: []const []const u8,
) void {
    buffer.print("  module {s}\n", .{ruby_name});

    switch (@typeInfo(Type)) {
        .@"struct" => |info| {
            assert(info.layout == .@"packed");
            buffer.print("    NONE: Integer\n", .{});
            inline for (info.fields) |field| {
                if (comptime std.mem.startsWith(u8, field.name, "deprecated_")) continue;
                if (comptime std.mem.eql(u8, field.name, "padding")) continue;
                const upper_snake = stdx.to_case(field.name, .UPPER_CASE);
                buffer.print("    {s}: Integer\n", .{upper_snake});
            }
        },
        .@"enum" => |info| {
            inline for (info.fields) |field| {
                if (comptime std.mem.startsWith(u8, field.name, "deprecated_")) continue;
                comptime var skip = false;
                inline for (skip_fields) |sf| {
                    skip = skip or comptime std.mem.eql(u8, sf, field.name);
                }
                if (skip) continue;
                const upper_snake = stdx.to_case(field.name, .UPPER_CASE);
                buffer.print("    {s}: Integer\n", .{upper_snake});
            }
        },
        else => @compileError("unsupported RBS constants module type: " ++ @typeName(Type)),
    }

    buffer.print("  end\n\n", .{});
}

fn emit_rbs_struct_class(
    buffer: *Buffer,
    comptime Type: type,
    comptime ruby_name: []const u8,
    comptime read_only: bool,
) void {
    assert(@typeInfo(Type) == .@"struct");
    assert(@typeInfo(Type).@"struct".layout == .@"extern");

    const fields = @typeInfo(Type).@"struct".fields;

    buffer.print("  class {s}\n", .{ruby_name});

    inline for (fields) |field| {
        if (comptime std.mem.eql(u8, field.name, "reserved")) continue;
        buffer.print(
            "    attr_{s} {s}: Integer\n",
            .{ if (read_only) "reader" else "accessor", field.name },
        );
    }
    buffer.print("\n", .{});

    if (read_only) {
        buffer.print("    def initialize: () -> void\n", .{});
    } else {
        buffer.print("    def initialize: (", .{});
        comptime var sep: []const u8 = "";
        inline for (fields) |field| {
            if (comptime std.mem.eql(u8, field.name, "reserved")) continue;
            buffer.print("{s}?{s}: Integer", .{ sep, field.name });
            sep = ", ";
        }
        buffer.print(") -> void\n", .{});
    }

    buffer.print("  end\n\n", .{});
}

fn emit_rbs_bindings(buffer: *Buffer) void {
    @setEvalBranchQuota(100_000);

    buffer.write(
        \\########################################################
        \\## This file was auto-generated by ruby_bindings.zig ##
        \\##              Do not manually modify.              ##
        \\########################################################
        \\
        \\module TigerBeetle
        \\  VERSION: String
        \\
        \\  def self.id: () -> Integer
        \\
        \\  class InitError < StandardError
        \\  end
        \\
        \\  class ClientClosedError < StandardError
        \\  end
        \\
        \\  class PacketError < StandardError
        \\  end
        \\
        \\  class Client
    );
    buffer.write("\n    def self.open: (cluster_id: Integer, replica_addresses: String)");
    buffer.write(" { (Client) -> untyped } -> untyped\n\n");
    buffer.write(
        \\    def initialize: (cluster_id: Integer, replica_addresses: String) -> void
        \\    def close: () -> nil
        \\    def closed?: () -> bool
        \\
        \\    def create_accounts: (Array[Account]) -> Array[CreateAccountResult]
        \\    def create_transfers: (Array[Transfer]) -> Array[CreateTransferResult]
        \\    def lookup_accounts: (Array[Integer]) -> Array[Account]
        \\    def lookup_transfers: (Array[Integer]) -> Array[Transfer]
        \\    def get_account_transfers: (AccountFilter) -> Array[Transfer]
        \\    def get_account_balances: (AccountFilter) -> Array[AccountBalance]
        \\    def query_accounts: (QueryFilter) -> Array[Account]
        \\    def query_transfers: (QueryFilter) -> Array[Transfer]
        \\  end
        \\
        \\
    );

    inline for (flag_mappings) |mapping| {
        const ZigType, const ruby_name = mapping;
        emit_rbs_constants_module(buffer, ZigType, ruby_name, &.{});
    }

    emit_rbs_constants_module(buffer, exports.tb_operation, "Operation", &.{
        "reserved", "root", "register", "pulse", "get_change_events",
    });
    emit_rbs_constants_module(buffer, tb.CreateAccountStatus, "CreateAccountStatus", &.{});
    emit_rbs_constants_module(buffer, tb.CreateTransferStatus, "CreateTransferStatus", &.{});

    emit_rbs_struct_class(buffer, tb.Account, "Account", false);
    emit_rbs_struct_class(buffer, tb.Transfer, "Transfer", false);
    emit_rbs_struct_class(buffer, tb.AccountFilter, "AccountFilter", false);
    emit_rbs_struct_class(buffer, tb.QueryFilter, "QueryFilter", false);

    emit_rbs_struct_class(buffer, tb.AccountBalance, "AccountBalance", true);
    emit_rbs_struct_class(buffer, tb.CreateAccountResult, "CreateAccountResult", true);
    emit_rbs_struct_class(buffer, tb.CreateTransferResult, "CreateTransferResult", true);

    buffer.print("end\n", .{});
}

fn emit_ruby_bindings(buffer: *Buffer) void {
    @setEvalBranchQuota(100_000);

    buffer.print(
        \\########################################################
        \\## This file was auto-generated by ruby_bindings.zig ##
        \\##              Do not manually modify.              ##
        \\########################################################
        \\
        \\module TigerBeetle
        \\
    , .{});

    // Flag modules (packed structs).
    inline for (flag_mappings) |mapping| {
        const ZigType, const ruby_name = mapping;
        emit_flags_module(buffer, ZigType, ruby_name);
    }

    // Operation constants.
    emit_enum_module(buffer, exports.tb_operation, "Operation", &.{
        "reserved", "root", "register", "pulse", "get_change_events",
    });

    // Status constants.
    emit_enum_module(buffer, tb.CreateAccountStatus, "CreateAccountStatus", &.{});
    emit_enum_module(buffer, tb.CreateTransferStatus, "CreateTransferStatus", &.{});

    // Struct classes — input types (read-write, yield self).
    emit_struct_class(buffer, tb.Account, "Account", false);
    emit_struct_class(buffer, tb.Transfer, "Transfer", false);
    emit_struct_class(buffer, tb.AccountFilter, "AccountFilter", false);
    emit_struct_class(buffer, tb.QueryFilter, "QueryFilter", false);

    // Struct classes — response-only types (read-only, no yield).
    emit_struct_class(buffer, tb.AccountBalance, "AccountBalance", true);
    emit_struct_class(buffer, tb.CreateAccountResult, "CreateAccountResult", true);
    emit_struct_class(buffer, tb.CreateTransferResult, "CreateTransferResult", true);

    buffer.print("end\n", .{});
}

fn emit_c_header_preamble(buffer: *Buffer) void {
    buffer.write(
        \\////////////////////////////////////////////////////////
        \\// This file was auto-generated by ruby_bindings.zig //
        \\//              Do not manually modify.              //
        \\////////////////////////////////////////////////////////
        \\
        \\#ifndef RB_TB_GEN_H
        \\#define RB_TB_GEN_H
        \\
        \\#include "ruby.h"
        \\#include "tb_client.h"
        \\#include <stdint.h>
        \\#include <stdio.h>
        \\#include <stdlib.h>
        \\#include <string.h>
        \\
        \\static inline void rb_tb_pack_u128(VALUE v, void *dst) {
        \\    rb_integer_pack(v, dst, 16, 1, 0, INTEGER_PACK_LITTLE_ENDIAN);
        \\}
        \\
        \\static inline VALUE rb_tb_unpack_u128(const void *src) {
        \\    return rb_integer_unpack(src, 16, 1, 0, INTEGER_PACK_LITTLE_ENDIAN);
        \\}
        \\
        \\static inline void tb_assert_fail(
        \\    const char *condition,
        \\    int line,
        \\    const char *function
        \\) {
        \\    fprintf(stderr, "tb_assert failed: %s at line %d in %s\n", condition, line, function);
        \\    abort();
        \\}
        \\
        \\// A version of `assert` macro that's always on regardless of NDEBUG macro.
        \\#define tb_assert(condition) \
        \\    do { \
        \\        if (!(condition)) { \
        \\            tb_assert_fail(#condition, __LINE__, __func__); \
        \\        } \
        \\    } while (0)
        \\
    );
}

fn emit_c_init_error_message(buffer: *Buffer) void {
    buffer.print("static const char *rb_tb_init_error_message(TB_INIT_STATUS status) {{\n", .{});
    buffer.print("    switch (status) {{\n", .{});
    inline for (@typeInfo(exports.tb_init_status).@"enum".fields) |field| {
        const status: exports.tb_init_status = @enumFromInt(field.value);
        buffer.print("    case {s}:\n", .{comptime c_init_status_name(status)});
        buffer.print("        return \"{s}\";\n", .{field.name});
    }
    buffer.write(
        \\    default:
        \\        return "unknown";
        \\    }
        \\}
        \\
        \\
    );
}

fn emit_c_num_from_ruby(
    buffer: *Buffer,
    comptime FieldType: type,
    comptime ruby_value: []const u8,
) void {
    const bits = comptime int_bits(FieldType);
    switch (bits) {
        8, 16, 32 => buffer.print("({s})NUM2UINT({s})", .{ c_uint_type(bits), ruby_value }),
        64 => buffer.print("NUM2ULL({s})", .{ruby_value}),
        else => @compileError("unsupported Ruby numeric field"),
    }
}

fn emit_c_value_from_field(
    buffer: *Buffer,
    comptime FieldType: type,
    comptime field_expr: []const u8,
) void {
    const bits = comptime int_bits(FieldType);
    switch (bits) {
        8, 16, 32 => buffer.print("UINT2NUM({s})", .{field_expr}),
        64 => buffer.print("ULL2NUM({s})", .{field_expr}),
        128 => buffer.print("rb_tb_unpack_u128(&{s})", .{field_expr}),
        else => @compileError("unsupported C numeric field"),
    }
}

fn emit_c_serialize_struct(buffer: *Buffer, comptime operation: tb.Operation) void {
    const Type = operation.EventType();
    const operation_name = comptime operation_function_name(operation);
    const c_name = comptime c_type_name(Type);
    buffer.print(
        "static void rb_tb_serialize_{s}(VALUE items_rb, uint8_t *buf, long count) {{\n",
        .{
            operation_name,
        },
    );
    buffer.print("    {s} *items = ({s} *)buf;\n", .{ c_name, c_name });
    buffer.print("    for (long i = 0; i < count; i++) {{\n", .{});
    buffer.print("        VALUE item_rb = RARRAY_AREF(items_rb, i);\n", .{});
    buffer.print("        {s} *item = &items[i];\n", .{c_name});

    inline for (@typeInfo(Type).@"struct".fields) |field| {
        if (comptime field_reserved(field.name)) {
            switch (@typeInfo(field.type)) {
                .array => buffer.print(
                    "        memset(item->{s}, 0, sizeof(item->{s}));\n",
                    .{ field.name, field.name },
                ),
                else => buffer.print("        item->{s} = 0;\n", .{field.name}),
            }
            continue;
        }
        const value_expr = "rb_ivar_get(item_rb, rb_intern(\"@" ++ field.name ++ "\"))";
        switch (@typeInfo(field.type)) {
            .int => |info| if (info.bits == 128) {
                buffer.print(
                    "        rb_tb_pack_u128({s}, &item->{s});\n",
                    .{ value_expr, field.name },
                );
            } else {
                buffer.print("        item->{s} = ", .{field.name});
                emit_c_num_from_ruby(buffer, field.type, value_expr);
                buffer.print(";\n", .{});
            },
            .@"enum", .@"struct" => {
                buffer.print("        item->{s} = ", .{field.name});
                emit_c_num_from_ruby(buffer, field.type, value_expr);
                buffer.print(";\n", .{});
            },
            else => @compileError("unsupported serializer field: " ++ @typeName(field.type)),
        }
    }

    buffer.print("    }}\n", .{});
    buffer.print("}}\n\n", .{});
}

fn emit_c_deserialize_struct(buffer: *Buffer, comptime operation: tb.Operation) void {
    const Type = operation.ResultType();
    const operation_name = comptime operation_function_name(operation);
    const c_name = comptime c_type_name(Type);
    buffer.print(
        "static VALUE rb_tb_deserialize_{s}(const uint8_t *buf, uint32_t buf_size) {{\n",
        .{operation_name},
    );
    buffer.print("    VALUE klass = rb_path2class(\"TigerBeetle::{s}\");\n", .{
        comptime ruby_type_name(Type),
    });
    buffer.print("    tb_assert(buf_size % sizeof({s}) == 0);\n", .{c_name});
    buffer.print("    long count = (long)(buf_size / sizeof({s}));\n", .{c_name});
    buffer.print("    VALUE results = rb_ary_new_capa(count);\n", .{});
    buffer.print("    const {s} *items = (const {s} *)buf;\n", .{ c_name, c_name });
    buffer.print("    for (long i = 0; i < count; i++) {{\n", .{});
    buffer.print("        const {s} *item = &items[i];\n", .{c_name});
    buffer.print("        VALUE obj = rb_obj_alloc(klass);\n", .{});

    inline for (@typeInfo(Type).@"struct".fields) |field| {
        if (comptime field_reserved(field.name)) {
            switch (@typeInfo(field.type)) {
                .array => buffer.print(
                    \\        uint8_t zero[sizeof(item->{s})] = {{0}};
                    \\        tb_assert(memcmp(item->{s}, zero, sizeof(item->{s})) == 0);
                    \\
                ,
                    .{ field.name, field.name, field.name },
                ),
                else => buffer.print("        tb_assert(item->{s} == 0);\n", .{field.name}),
            }
            continue;
        }
        const field_expr = "item->" ++ field.name;
        buffer.print("        rb_ivar_set(obj, rb_intern(\"@{s}\"), ", .{field.name});
        emit_c_value_from_field(buffer, field.type, field_expr);
        buffer.print(");\n", .{});
    }

    buffer.print("        rb_ary_push(results, obj);\n", .{});
    buffer.print("    }}\n", .{});
    buffer.print("    return results;\n", .{});
    buffer.print("}}\n\n", .{});
}

fn emit_c_lookup_serializer(buffer: *Buffer) void {
    buffer.write(
        \\static void rb_tb_serialize_u128(VALUE items_rb, uint8_t *buf, long count) {
        \\    tb_uint128_t *ids = (tb_uint128_t *)buf;
        \\    for (long i = 0; i < count; i++) {
        \\        rb_tb_pack_u128(RARRAY_AREF(items_rb, i), &ids[i]);
        \\    }
        \\}
        \\
        \\
    );
}

fn emit_c_event_size(buffer: *Buffer) void {
    buffer.print("static size_t rb_tb_event_size(TB_OPERATION operation) {{\n", .{});
    buffer.print("    switch (operation) {{\n", .{});
    inline for (@typeInfo(tb.Operation).@"enum".fields) |operation_field| {
        const operation: tb.Operation = @enumFromInt(operation_field.value);
        if (comptime !operation_supported(operation)) continue;
        const Event = operation.EventType();
        buffer.print("    case {s}:\n", .{comptime c_operation_name(operation)});
        if (Event == u128) {
            buffer.print("        return sizeof(tb_uint128_t);\n", .{});
        } else {
            buffer.print("        return sizeof({s});\n", .{comptime c_type_name(Event)});
        }
    }
    buffer.write(
        \\    default:
        \\        rb_raise(rb_eRuntimeError, "unsupported operation: %d", (int)operation);
        \\        return 0;
        \\    }
        \\}
        \\
        \\
    );
}

fn emit_c_serialize_dispatch(buffer: *Buffer) void {
    buffer.write(
        \\static void rb_tb_serialize(
        \\    TB_OPERATION operation,
        \\    VALUE items_rb,
        \\    uint8_t *buf,
        \\    long count
        \\) {
        \\
    );
    buffer.print("    switch (operation) {{\n", .{});
    inline for (@typeInfo(tb.Operation).@"enum".fields) |operation_field| {
        const operation: tb.Operation = @enumFromInt(operation_field.value);
        if (comptime !operation_supported(operation)) continue;
        const Event = operation.EventType();
        buffer.print("    case {s}:\n", .{comptime c_operation_name(operation)});
        if (Event == u128) {
            buffer.print("        rb_tb_serialize_u128(items_rb, buf, count);\n", .{});
        } else {
            buffer.print("        rb_tb_serialize_{s}(items_rb, buf, count);\n", .{
                comptime operation_function_name(operation),
            });
        }
        buffer.print("        break;\n", .{});
    }
    buffer.write(
        \\    default:
        \\        rb_raise(rb_eRuntimeError, "unsupported operation: %d", (int)operation);
        \\    }
        \\}
        \\
        \\
    );
}

fn emit_c_deserialize_dispatch(buffer: *Buffer) void {
    buffer.write(
        \\static VALUE rb_tb_deserialize(
        \\    TB_OPERATION operation,
        \\    const uint8_t *buf,
        \\    uint32_t buf_size
        \\) {
        \\
    );
    buffer.print("    switch (operation) {{\n", .{});
    inline for (@typeInfo(tb.Operation).@"enum".fields) |operation_field| {
        const operation: tb.Operation = @enumFromInt(operation_field.value);
        if (comptime !operation_supported(operation)) continue;
        buffer.print("    case {s}:\n", .{comptime c_operation_name(operation)});
        buffer.print("        return rb_tb_deserialize_{s}(buf, buf_size);\n", .{
            comptime operation_function_name(operation),
        });
    }
    buffer.write(
        \\    default:
        \\        rb_raise(rb_eRuntimeError, "unsupported operation: %d", (int)operation);
        \\    }
        \\}
        \\
    );
}

fn emit_c_header(buffer: *Buffer) void {
    @setEvalBranchQuota(100_000);

    emit_c_header_preamble(buffer);
    emit_c_init_error_message(buffer);

    inline for (@typeInfo(tb.Operation).@"enum".fields) |operation_field| {
        const operation: tb.Operation = @enumFromInt(operation_field.value);
        if (comptime !operation_supported(operation)) continue;
        if (operation.EventType() != u128) emit_c_serialize_struct(buffer, operation);
        emit_c_deserialize_struct(buffer, operation);
    }

    emit_c_lookup_serializer(buffer);
    emit_c_event_size(buffer);
    emit_c_serialize_dispatch(buffer);
    emit_c_deserialize_dispatch(buffer);

    buffer.print("#endif\n", .{});
}

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var buffer = Buffer.init(allocator);

    if (comptime std.mem.eql(u8, generator_options.output, "ruby")) {
        emit_ruby_bindings(&buffer);
    } else if (comptime std.mem.eql(u8, generator_options.output, "rbs")) {
        emit_rbs_bindings(&buffer);
    } else if (comptime std.mem.eql(u8, generator_options.output, "c_header")) {
        emit_c_header(&buffer);
    } else {
        @compileError("unsupported ruby bindings output mode");
    }

    try std.io.getStdOut().writeAll(buffer.inner.items);
}
