const std = @import("std");
const assert = std.debug.assert;

const generator_options = @import("ruby_bindings_options");
const vsr = @import("vsr");
const exports = vsr.tb_client.exports;
const tb = vsr.tigerbeetle;
const stdx = vsr.stdx;
const SourcePrinter = stdx.SourcePrinter;

const flag_mappings = .{
    .{ tb.AccountFlags, "AccountFlags" },
    .{ tb.TransferFlags, "TransferFlags" },
    .{ tb.AccountFilterFlags, "AccountFilterFlags" },
    .{ tb.QueryFilterFlags, "QueryFilterFlags" },
};

fn ruby_flags_name_from_type(comptime Type: type) ?[]const u8 {
    comptime for (flag_mappings) |mapping| {
        const ZigType, const ruby_name = mapping;
        if (Type == ZigType) return ruby_name;
    };
    return null;
}

fn c_operation_name(comptime operation: tb.Operation) []const u8 {
    const upper_snake = SourcePrinter.to_case(@tagName(operation), .upper_case);
    return "TB_OPERATION_" ++ upper_snake;
}

fn c_init_status_name(comptime status: exports.tb_init_status) []const u8 {
    const upper_snake = SourcePrinter.to_case(@tagName(status), .upper_case);
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

fn c_type_name(comptime Type: type) []const u8 {
    return "tb_" ++ SourcePrinter.to_case(type_basename(Type), .snake_case) ++ "_t";
}

fn ruby_type_name(comptime Type: type) []const u8 {
    return type_basename(Type);
}

fn result_status_type(comptime Type: type) ?type {
    if (Type == tb.CreateAccountResult) return tb.CreateAccountStatus;
    if (Type == tb.CreateTransferResult) return tb.CreateTransferStatus;
    return null;
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

fn field_reserved(comptime field_name: []const u8) bool {
    if (comptime std.mem.eql(u8, field_name, "reserved")) return true;
    if (comptime std.mem.eql(u8, field_name, "padding")) return true;
    return false;
}

fn emit_flags_module(
    printer: *SourcePrinter,
    comptime Type: type,
    comptime ruby_name: []const u8,
) !void {
    assert(@typeInfo(Type) == .@"struct");
    assert(@typeInfo(Type).@"struct".layout == .@"packed");

    try printer.print_indented("module {s}", .{ruby_name});
    printer.indent();
    try printer.write_indented("NONE = 0");
    inline for (@typeInfo(Type).@"struct".fields, 0..) |field, i| {
        if (comptime std.mem.startsWith(u8, field.name, "deprecated_")) continue;
        if (comptime std.mem.eql(u8, field.name, "padding")) continue;
        const upper_snake = SourcePrinter.to_case(field.name, .upper_case);
        try printer.print_indented("{s} = 1 << {d}", .{ upper_snake, i });
    }
    printer.dedent();
    try printer.write_indented("end");
    try printer.write_empty_line();
}

fn emit_enum_module(
    printer: *SourcePrinter,
    comptime Type: type,
    comptime ruby_name: []const u8,
    comptime skip_fields: []const []const u8,
) !void {
    assert(@typeInfo(Type) == .@"enum");

    try printer.print_indented("module {s}", .{ruby_name});
    printer.indent();
    inline for (@typeInfo(Type).@"enum".fields) |field| {
        if (comptime std.mem.startsWith(u8, field.name, "deprecated_")) continue;
        comptime var skip = false;
        inline for (skip_fields) |sf| {
            skip = skip or comptime std.mem.eql(u8, sf, field.name);
        }
        if (skip) continue;
        const upper_snake = SourcePrinter.to_case(field.name, .upper_case);
        const value: u64 = @intCast(@intFromEnum(@field(Type, field.name)));
        try printer.print_indented("{s} = {d}", .{ upper_snake, value });
    }
    printer.dedent();
    try printer.write_indented("end");
    try printer.write_empty_line();
}

fn emit_field_default(printer: *SourcePrinter, comptime FieldType: type) !void {
    const type_info = @typeInfo(FieldType);
    const is_flags = type_info == .@"struct" and type_info.@"struct".layout == .@"packed";
    if (is_flags) {
        try printer.print("{s}::NONE", .{comptime ruby_flags_name_from_type(FieldType).?});
    } else {
        try printer.write("0");
    }
}

fn emit_struct_class(
    printer: *SourcePrinter,
    comptime Type: type,
    comptime ruby_name: []const u8,
    comptime read_only: bool,
) !void {
    assert(@typeInfo(Type) == .@"struct");
    assert(@typeInfo(Type).@"struct".layout == .@"extern");

    const fields = @typeInfo(Type).@"struct".fields;

    try printer.print_indented("class {s}", .{ruby_name});
    printer.indent();

    inline for (fields) |field| {
        if (comptime std.mem.eql(u8, field.name, "reserved")) continue;
        try printer.print_indented(
            "attr_{s} :{s}",
            .{ if (read_only) "reader" else "accessor", field.name },
        );
        if (comptime read_only and
            std.mem.eql(u8, field.name, "status") and
            result_status_type(Type) != null)
        {
            try printer.write_indented("attr_reader :status_name");
        }
    }
    try printer.write_empty_line();

    if (!read_only) {
        try printer.write_indented("def initialize(");
        printer.indent();
        comptime var sep: []const u8 = "";
        inline for (fields) |field| {
            if (comptime std.mem.eql(u8, field.name, "reserved")) continue;
            if (sep.len > 0) try printer.write(sep);
            try printer.write_indent();
            try printer.print("{s}: ", .{field.name});
            try emit_field_default(printer, field.type);
            sep = ",\n";
        }
        try printer.write("\n");
        printer.dedent();
        try printer.write_indented(")");
        printer.indent();
        inline for (fields) |field| {
            if (comptime std.mem.eql(u8, field.name, "reserved")) continue;
            try printer.print_indented("@{s} = {s}", .{ field.name, field.name });
        }
        printer.dedent();
        try printer.write_indented("end");
    } else {
        try printer.write_indented("def initialize");
        printer.indent();
        inline for (fields) |field| {
            if (comptime std.mem.eql(u8, field.name, "reserved")) continue;
            try printer.print_indented("@{s} = 0", .{field.name});
        }
        printer.dedent();
        try printer.write_indented("end");

        if (comptime result_status_type(Type) != null) {
            try printer.write_empty_line();
            try printer.write_indented("def to_s =");
            printer.indent();
            try printer.write_indent();
            try printer.write("\"#<#{self.class} timestamp=#{@timestamp} ");
            try printer.write("status_name=#{@status_name}>\"\n");
            printer.dedent();
        }
    }
    printer.dedent();
    try printer.write_indented("end");
    try printer.write_empty_line();
}

fn emit_rbs_constants_module(
    printer: *SourcePrinter,
    comptime Type: type,
    comptime ruby_name: []const u8,
    comptime skip_fields: []const []const u8,
) !void {
    try printer.print_indented("module {s}", .{ruby_name});
    printer.indent();

    switch (@typeInfo(Type)) {
        .@"struct" => |info| {
            assert(info.layout == .@"packed");
            try printer.write_indented("NONE: Integer");
            inline for (info.fields) |field| {
                if (comptime std.mem.startsWith(u8, field.name, "deprecated_")) continue;
                if (comptime std.mem.eql(u8, field.name, "padding")) continue;
                const upper_snake = SourcePrinter.to_case(field.name, .upper_case);
                try printer.print_indented("{s}: Integer", .{upper_snake});
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
                const upper_snake = SourcePrinter.to_case(field.name, .upper_case);
                try printer.print_indented("{s}: Integer", .{upper_snake});
            }
        },
        else => @compileError("unsupported RBS constants module type: " ++ @typeName(Type)),
    }

    printer.dedent();
    try printer.write_indented("end");
    try printer.write_empty_line();
}

fn emit_rbs_status_name_alias(
    printer: *SourcePrinter,
    comptime StatusType: type,
    comptime alias_name: []const u8,
) !void {
    assert(@typeInfo(StatusType) == .@"enum");

    try printer.print_indented("type {s} =", .{alias_name});
    printer.indent();
    comptime var sep: []const u8 = "";
    inline for (@typeInfo(StatusType).@"enum".fields) |field| {
        if (comptime std.mem.startsWith(u8, field.name, "deprecated_")) continue;
        try printer.print_indented("{s}:{s}", .{ sep, field.name });
        sep = "| ";
    }
    printer.dedent();
    try printer.write_empty_line();
}

fn emit_rbs_struct_class(
    printer: *SourcePrinter,
    comptime Type: type,
    comptime ruby_name: []const u8,
    comptime read_only: bool,
) !void {
    assert(@typeInfo(Type) == .@"struct");
    assert(@typeInfo(Type).@"struct".layout == .@"extern");

    const fields = @typeInfo(Type).@"struct".fields;

    try printer.print_indented("class {s}", .{ruby_name});
    printer.indent();

    inline for (fields) |field| {
        if (comptime std.mem.eql(u8, field.name, "reserved")) continue;
        try printer.print_indented(
            "attr_{s} {s}: Integer",
            .{ if (read_only) "reader" else "accessor", field.name },
        );
        if (comptime read_only and std.mem.eql(u8, field.name, "status")) {
            if (comptime Type == tb.CreateAccountResult) {
                try printer.write_indented("attr_reader status_name: create_account_status_name");
            } else if (comptime Type == tb.CreateTransferResult) {
                try printer.write_indented("attr_reader status_name: create_transfer_status_name");
            }
        }
    }
    try printer.write_empty_line();

    if (read_only) {
        try printer.write_indented("def initialize: () -> void");
    } else {
        try printer.write_indent();
        try printer.write("def initialize: (");
        comptime var sep: []const u8 = "";
        inline for (fields) |field| {
            if (comptime std.mem.eql(u8, field.name, "reserved")) continue;
            try printer.print("{s}?{s}: Integer", .{ sep, field.name });
            sep = ", ";
        }
        try printer.write(") -> void\n");
    }

    printer.dedent();
    try printer.write_indented("end");
    try printer.write_empty_line();
}

fn emit_rbs_bindings(printer: *SourcePrinter) !void {
    @setEvalBranchQuota(100_000);

    try printer.write(
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
        \\  class BatchError < StandardError
        \\  end
        \\
        \\  class Client
    );
    try printer.write("\n    def self.open: (cluster_id: Integer, replica_addresses: String)");
    try printer.write(" { (Client) -> untyped } -> untyped\n\n");
    try printer.write(
        \\    def initialize: (cluster_id: Integer, replica_addresses: String) -> void
        \\    def close: () -> nil
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

    printer.indent();
    inline for (flag_mappings) |mapping| {
        const ZigType, const ruby_name = mapping;
        try emit_rbs_constants_module(printer, ZigType, ruby_name, &.{});
    }

    try emit_rbs_constants_module(printer, exports.tb_operation, "Operation", &.{
        "reserved", "root", "register", "pulse", "get_change_events",
    });
    try emit_rbs_constants_module(printer, tb.CreateAccountStatus, "CreateAccountStatus", &.{});
    try emit_rbs_constants_module(printer, tb.CreateTransferStatus, "CreateTransferStatus", &.{});

    try emit_rbs_struct_class(printer, tb.Account, "Account", false);
    try emit_rbs_struct_class(printer, tb.Transfer, "Transfer", false);
    try emit_rbs_struct_class(printer, tb.AccountFilter, "AccountFilter", false);
    try emit_rbs_struct_class(printer, tb.QueryFilter, "QueryFilter", false);

    try emit_rbs_struct_class(printer, tb.AccountBalance, "AccountBalance", true);
    try emit_rbs_status_name_alias(
        printer,
        tb.CreateAccountStatus,
        "create_account_status_name",
    );
    try emit_rbs_status_name_alias(
        printer,
        tb.CreateTransferStatus,
        "create_transfer_status_name",
    );
    try emit_rbs_struct_class(printer, tb.CreateAccountResult, "CreateAccountResult", true);
    try emit_rbs_struct_class(printer, tb.CreateTransferResult, "CreateTransferResult", true);

    printer.dedent();
    try printer.write_indented("end");
}

fn emit_ruby_bindings(printer: *SourcePrinter) !void {
    @setEvalBranchQuota(100_000);

    try printer.write(
        \\########################################################
        \\## This file was auto-generated by ruby_bindings.zig ##
        \\##              Do not manually modify.              ##
        \\########################################################
        \\
        \\module TigerBeetle
        \\
    );

    printer.indent();

    // Flag modules (packed structs).
    inline for (flag_mappings) |mapping| {
        const ZigType, const ruby_name = mapping;
        try emit_flags_module(printer, ZigType, ruby_name);
    }

    // Operation constants.
    try emit_enum_module(printer, exports.tb_operation, "Operation", &.{
        "reserved", "root", "register", "pulse", "get_change_events",
    });

    // Status constants.
    try emit_enum_module(printer, tb.CreateAccountStatus, "CreateAccountStatus", &.{});
    try emit_enum_module(printer, tb.CreateTransferStatus, "CreateTransferStatus", &.{});

    // Struct classes — input types (read-write, yield self).
    try emit_struct_class(printer, tb.Account, "Account", false);
    try emit_struct_class(printer, tb.Transfer, "Transfer", false);
    try emit_struct_class(printer, tb.AccountFilter, "AccountFilter", false);
    try emit_struct_class(printer, tb.QueryFilter, "QueryFilter", false);

    // Struct classes — response-only types (read-only, no yield).
    try emit_struct_class(printer, tb.AccountBalance, "AccountBalance", true);
    try emit_struct_class(printer, tb.CreateAccountResult, "CreateAccountResult", true);
    try emit_struct_class(printer, tb.CreateTransferResult, "CreateTransferResult", true);

    printer.dedent();
    try printer.write_indented("end");
}

fn emit_c_header_preamble(printer: *SourcePrinter) !void {
    try printer.write(
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
        \\static inline VALUE rb_tb_check_integer(VALUE v, const char *field) {
        \\    if (!RB_INTEGER_TYPE_P(v)) {
        \\        rb_raise(rb_eTypeError, "%s must be an Integer, got %s", field, rb_obj_classname(v));
        \\    }
        \\    return v;
        \\}
        \\
        \\static inline void rb_tb_pack_u128(VALUE v, void *dst, const char *field) {
        \\    rb_tb_check_integer(v, field);
        \\    int status = rb_integer_pack(v, dst, 16, 1, 0, INTEGER_PACK_LITTLE_ENDIAN);
        \\    if (status != 0 && status != 1) {
        \\        rb_raise(rb_eRangeError, "integer must be between 0 and 2**128 - 1");
        \\    }
        \\}
        \\
        \\#define rb_tb_ivar_get_checked(item_rb, field) \
        \\    rb_tb_check_integer(rb_ivar_get(item_rb, rb_intern("@" #field)), #field)
        \\
        \\#define rb_tb_pack_u128_checked(item_rb, item, field) \
        \\    rb_tb_pack_u128(rb_ivar_get(item_rb, rb_intern("@" #field)), &item->field, #field)
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

fn emit_c_init_error_message(printer: *SourcePrinter) !void {
    try printer.write_indented(
        "static const char *rb_tb_init_error_message(TB_INIT_STATUS status) {",
    );
    printer.indent();
    try printer.write_indented("switch (status) {");
    inline for (@typeInfo(exports.tb_init_status).@"enum".fields) |field| {
        const status: exports.tb_init_status = @enumFromInt(field.value);
        try printer.print_indented("case {s}:", .{comptime c_init_status_name(status)});
        printer.indent();
        try printer.print_indented("return \"{s}\";", .{field.name});
        printer.dedent();
    }
    try printer.write_indented("default:");
    printer.indent();
    try printer.write_indented("return \"unknown\";");
    printer.dedent();
    try printer.write_indented("}");
    printer.dedent();
    try printer.write_indented("}");
    try printer.write_empty_line();
}

fn emit_c_status_name_function(
    printer: *SourcePrinter,
    comptime StatusType: type,
    comptime function_name: []const u8,
) !void {
    assert(@typeInfo(StatusType) == .@"enum");

    try printer.print_indented("static VALUE {s}(uint32_t status) {{", .{function_name});
    printer.indent();
    try printer.write_indented("switch (status) {");
    inline for (@typeInfo(StatusType).@"enum".fields) |field| {
        if (comptime std.mem.startsWith(u8, field.name, "deprecated_")) continue;
        const value: u64 = @intCast(@intFromEnum(@field(StatusType, field.name)));
        try printer.print_indented("case {d}:", .{value});
        printer.indent();
        try printer.print_indented("return ID2SYM(rb_intern(\"{s}\"));", .{field.name});
        printer.dedent();
    }
    try printer.write_indented("default:");
    printer.indent();
    try printer.write_indented("tb_assert(false);");
    try printer.write_indented("return Qnil;");
    printer.dedent();
    try printer.write_indented("}");
    printer.dedent();
    try printer.write_indented("}");
    try printer.write_empty_line();
}

fn emit_c_num_from_ruby(
    printer: *SourcePrinter,
    comptime FieldType: type,
    comptime field_name: []const u8,
) !void {
    const bits = comptime int_bits(FieldType);
    const macro: []const u8 = switch (bits) {
        8 => "RB_NUM2CHR",
        16 => "RB_NUM2USHORT",
        32 => "RB_NUM2UINT",
        64 => "RB_NUM2ULL",
        else => @compileError("unsupported Ruby numeric field"),
    };
    try printer.print("{s}(rb_tb_ivar_get_checked(item_rb, {s}))", .{ macro, field_name });
}

fn emit_c_value_from_field(
    printer: *SourcePrinter,
    comptime FieldType: type,
    comptime field_expr: []const u8,
) !void {
    const bits = comptime int_bits(FieldType);
    switch (bits) {
        8, 16, 32 => try printer.print("RB_UINT2NUM({s})", .{field_expr}),
        64 => try printer.print("RB_ULL2NUM({s})", .{field_expr}),
        128 => try printer.print("rb_tb_unpack_u128(&{s})", .{field_expr}),
        else => @compileError("unsupported C numeric field"),
    }
}

fn emit_c_serialize_struct(printer: *SourcePrinter, comptime operation: tb.Operation) !void {
    const Type = operation.EventType();
    const operation_name = comptime operation_function_name(operation);
    const c_name = comptime c_type_name(Type);
    try printer.print_indented(
        "static void rb_tb_serialize_{s}(VALUE items_rb, uint8_t *buf, long count) {{",
        .{
            operation_name,
        },
    );
    printer.indent();
    try printer.print_indented("{s} *items = ({s} *)buf;", .{ c_name, c_name });
    try printer.write_indented("for (long i = 0; i < count; i++) {");
    printer.indent();
    try printer.write_indented("VALUE item_rb = RARRAY_AREF(items_rb, i);");
    try printer.print_indented("{s} *item = &items[i];", .{c_name});

    inline for (@typeInfo(Type).@"struct".fields) |field| {
        if (comptime field_reserved(field.name)) {
            switch (@typeInfo(field.type)) {
                .array => try printer.print_indented(
                    "memset(item->{s}, 0, sizeof(item->{s}));",
                    .{ field.name, field.name },
                ),
                else => try printer.print_indented("item->{s} = 0;", .{field.name}),
            }
            continue;
        }
        switch (@typeInfo(field.type)) {
            .int => |info| if (info.bits == 128) {
                try printer.print_indented(
                    "rb_tb_pack_u128_checked(item_rb, item, {s});",
                    .{field.name},
                );
            } else {
                try printer.write_indent();
                try printer.print("item->{s} = ", .{field.name});
                try emit_c_num_from_ruby(printer, field.type, field.name);
                try printer.write(";\n");
            },
            .@"enum", .@"struct" => {
                try printer.write_indent();
                try printer.print("item->{s} = ", .{field.name});
                try emit_c_num_from_ruby(printer, field.type, field.name);
                try printer.write(";\n");
            },
            else => @compileError("unsupported serializer field: " ++ @typeName(field.type)),
        }
    }

    printer.dedent();
    try printer.write_indented("}");
    printer.dedent();
    try printer.write_indented("}");
    try printer.write_empty_line();
}

fn emit_c_deserialize_struct(printer: *SourcePrinter, comptime operation: tb.Operation) !void {
    const Type = operation.ResultType();
    const operation_name = comptime operation_function_name(operation);
    const c_name = comptime c_type_name(Type);
    try printer.print_indented(
        "static VALUE rb_tb_deserialize_{s}(const uint8_t *buf, uint32_t buf_size) {{",
        .{operation_name},
    );
    printer.indent();
    try printer.print_indented("VALUE klass = rb_path2class(\"TigerBeetle::{s}\");", .{
        comptime ruby_type_name(Type),
    });
    try printer.print_indented("tb_assert(buf_size % sizeof({s}) == 0);", .{c_name});
    try printer.print_indented("long count = (long)(buf_size / sizeof({s}));", .{c_name});
    try printer.write_indented("VALUE results = rb_ary_new_capa(count);");
    try printer.print_indented("const {s} *items = (const {s} *)buf;", .{ c_name, c_name });
    try printer.write_indented("for (long i = 0; i < count; i++) {");
    printer.indent();
    try printer.print_indented("const {s} *item = &items[i];", .{c_name});
    try printer.write_indented("VALUE obj = rb_obj_alloc(klass);");

    inline for (@typeInfo(Type).@"struct".fields) |field| {
        if (comptime field_reserved(field.name)) {
            switch (@typeInfo(field.type)) {
                .array => {
                    try printer.print_indented(
                        "uint8_t zero[sizeof(item->{s})] = {{0}};",
                        .{field.name},
                    );
                    try printer.print_indented(
                        "tb_assert(memcmp(item->{s}, zero, sizeof(item->{s})) == 0);",
                        .{ field.name, field.name },
                    );
                },
                else => try printer.print_indented("tb_assert(item->{s} == 0);", .{field.name}),
            }
            continue;
        }
        const field_expr = "item->" ++ field.name;
        try printer.write_indent();
        try printer.print("rb_ivar_set(obj, rb_intern(\"@{s}\"), ", .{field.name});
        try emit_c_value_from_field(printer, field.type, field_expr);
        try printer.write(");\n");
        if (comptime std.mem.eql(u8, field.name, "status")) {
            if (comptime result_status_type(Type) != null) {
                try printer.write_indented("rb_ivar_set(");
                printer.indent();
                try printer.write_indented("obj,");
                try printer.write_indented("rb_intern(\"@status_name\"),");
                try printer.print_indented("rb_tb_{s}_status_name(item->status)", .{
                    operation_name,
                });
                printer.dedent();
                try printer.write_indented(");");
            }
        }
    }

    try printer.write_indented("rb_ary_push(results, obj);");
    printer.dedent();
    try printer.write_indented("}");
    try printer.write_indented("return results;");
    printer.dedent();
    try printer.write_indented("}");
    try printer.write_empty_line();
}

fn emit_c_lookup_serializer(printer: *SourcePrinter) !void {
    try printer.write(
        \\static void rb_tb_serialize_u128(VALUE items_rb, uint8_t *buf, long count) {
        \\    tb_uint128_t *ids = (tb_uint128_t *)buf;
        \\    for (long i = 0; i < count; i++) {
        \\        rb_tb_pack_u128(RARRAY_AREF(items_rb, i), &ids[i], "id");
        \\    }
        \\}
        \\
        \\
    );
}

fn emit_c_event_size(printer: *SourcePrinter) !void {
    @setEvalBranchQuota(5_000);

    try printer.write_indented("static size_t rb_tb_event_size(TB_OPERATION operation) {");
    printer.indent();
    try printer.write_indented("switch (operation) {");
    inline for (@typeInfo(tb.Operation).@"enum".fields) |operation_field| {
        const operation: tb.Operation = @enumFromInt(operation_field.value);
        if (comptime !operation_supported(operation)) continue;
        const Event = operation.EventType();
        try printer.print_indented("case {s}:", .{comptime c_operation_name(operation)});
        printer.indent();
        if (Event == u128) {
            try printer.write_indented("return sizeof(tb_uint128_t);");
        } else {
            try printer.print_indented("return sizeof({s});", .{comptime c_type_name(Event)});
        }
        printer.dedent();
    }
    try printer.write_indented("default:");
    printer.indent();
    try printer.write_indented(
        "rb_raise(rb_eRuntimeError, \"unsupported operation: %d\", (int)operation);",
    );
    try printer.write_indented("return 0;");
    printer.dedent();
    try printer.write_indented("}");
    printer.dedent();
    try printer.write_indented("}");
    try printer.write_empty_line();
}

fn emit_c_serialize_dispatch(printer: *SourcePrinter) !void {
    @setEvalBranchQuota(5_000);

    try printer.write(
        \\static void rb_tb_serialize(
        \\    TB_OPERATION operation,
        \\    VALUE items_rb,
        \\    uint8_t *buf,
        \\    long count
        \\) {
        \\
    );
    printer.indent();
    try printer.write_indented("switch (operation) {");
    inline for (@typeInfo(tb.Operation).@"enum".fields) |operation_field| {
        const operation: tb.Operation = @enumFromInt(operation_field.value);
        if (comptime !operation_supported(operation)) continue;
        const Event = operation.EventType();
        try printer.print_indented("case {s}:", .{comptime c_operation_name(operation)});
        printer.indent();
        if (Event == u128) {
            try printer.write_indented("rb_tb_serialize_u128(items_rb, buf, count);");
        } else {
            try printer.print_indented("rb_tb_serialize_{s}(items_rb, buf, count);", .{
                comptime operation_function_name(operation),
            });
        }
        try printer.write_indented("break;");
        printer.dedent();
    }
    try printer.write_indented("default:");
    printer.indent();
    try printer.write_indented(
        "rb_raise(rb_eRuntimeError, \"unsupported operation: %d\", (int)operation);",
    );
    printer.dedent();
    try printer.write_indented("}");
    printer.dedent();
    try printer.write_indented("}");
    try printer.write_empty_line();
}

fn emit_c_deserialize_dispatch(printer: *SourcePrinter) !void {
    @setEvalBranchQuota(5_000);

    try printer.write(
        \\static VALUE rb_tb_deserialize(
        \\    TB_OPERATION operation,
        \\    const uint8_t *buf,
        \\    uint32_t buf_size
        \\) {
        \\
    );
    printer.indent();
    try printer.write_indented("switch (operation) {");
    inline for (@typeInfo(tb.Operation).@"enum".fields) |operation_field| {
        const operation: tb.Operation = @enumFromInt(operation_field.value);
        if (comptime !operation_supported(operation)) continue;
        try printer.print_indented("case {s}:", .{comptime c_operation_name(operation)});
        printer.indent();
        try printer.print_indented("return rb_tb_deserialize_{s}(buf, buf_size);", .{
            comptime operation_function_name(operation),
        });
        printer.dedent();
    }
    try printer.write_indented("default:");
    printer.indent();
    try printer.write_indented(
        "rb_raise(rb_eRuntimeError, \"unsupported operation: %d\", (int)operation);",
    );
    printer.dedent();
    try printer.write_indented("}");
    printer.dedent();
    try printer.write_indented("}");
}

fn emit_c_header(printer: *SourcePrinter) !void {
    @setEvalBranchQuota(100_000);

    try emit_c_header_preamble(printer);
    try emit_c_init_error_message(printer);
    try emit_c_status_name_function(
        printer,
        tb.CreateAccountStatus,
        "rb_tb_create_accounts_status_name",
    );
    try emit_c_status_name_function(
        printer,
        tb.CreateTransferStatus,
        "rb_tb_create_transfers_status_name",
    );

    inline for (@typeInfo(tb.Operation).@"enum".fields) |operation_field| {
        const operation: tb.Operation = @enumFromInt(operation_field.value);
        if (comptime !operation_supported(operation)) continue;
        if (operation.EventType() != u128) try emit_c_serialize_struct(printer, operation);
        try emit_c_deserialize_struct(printer, operation);
    }

    try emit_c_lookup_serializer(printer);
    try emit_c_event_size(printer);
    try emit_c_serialize_dispatch(printer);
    try emit_c_deserialize_dispatch(printer);

    try printer.write_indented("#endif");
}

pub fn main(init: std.process.Init) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var buffer: std.Io.Writer.Allocating = .init(allocator);
    const is_c = comptime std.mem.eql(u8, generator_options.output, "c_header");
    var printer = SourcePrinter.init(allocator, &buffer.writer, .{
        .indent_width = if (is_c) 4 else 2,
        .indent_char = ' ',
        .comment_marker = if (is_c) "//" else "#",
    });

    if (comptime std.mem.eql(u8, generator_options.output, "ruby")) {
        try emit_ruby_bindings(&printer);
    } else if (comptime std.mem.eql(u8, generator_options.output, "rbs")) {
        try emit_rbs_bindings(&printer);
    } else if (comptime std.mem.eql(u8, generator_options.output, "c_header")) {
        try emit_c_header(&printer);
    } else {
        @compileError("unsupported ruby bindings output mode");
    }

    try std.Io.File.stdout().writeStreamingAll(init.io, buffer.written());
}
