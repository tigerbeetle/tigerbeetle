const std = @import("std");
const vsr = @import("vsr");
const assert = std.debug.assert;

const stdx = vsr.stdx;
const tb = vsr.tigerbeetle;

const SourcePrinter = stdx.SourcePrinter;

// See: https://go.dev/wiki/CodeReviewComments#initialisms
const go_initialisms: []const []const u8 = &.{ "id", "ok" };

const type_mappings = .{
    .{ tb.AccountFlags, "AccountFlags" },
    .{ tb.TransferFlags, "TransferFlags" },
    .{ tb.AccountFilterFlags, "AccountFilterFlags" },
    .{ tb.QueryFilterFlags, "QueryFilterFlags" },
    .{ tb.Account, "Account" },
    .{ tb.Transfer, "Transfer" },
    .{ tb.CreateAccountStatus, "CreateAccountStatus", "Account" },
    .{ tb.CreateTransferStatus, "CreateTransferStatus", "Transfer" },
    .{ tb.CreateAccountResult, "CreateAccountResult" },
    .{ tb.CreateTransferResult, "CreateTransferResult" },
    .{ tb.AccountFilter, "AccountFilter" },
    .{ tb.AccountBalance, "AccountBalance" },
    .{ tb.QueryFilter, "QueryFilter" },
    .{ tb.ChangeEvent, "ChangeEvent" },
    .{ tb.ChangeEventType, "ChangeEventType", "ChangeEvent" },
    .{ tb.ChangeEventsFilter, "ChangeEventsFilter" },
};

fn go_type(comptime Type: type) []const u8 {
    switch (@typeInfo(Type)) {
        .bool => return "bool",
        .@"enum" => return comptime get_mapped_type_name(Type) orelse
            @compileError("Type " ++ @typeName(Type) ++ " not mapped."),
        .@"struct" => |info| switch (info.layout) {
            .@"packed" => return comptime go_type(std.meta.Int(.unsigned, @bitSizeOf(Type))),
            else => return comptime get_mapped_type_name(Type) orelse
                @compileError("Type " ++ @typeName(Type) ++ " not mapped."),
        },
        .int => |info| {
            assert(info.signedness == .unsigned);
            return switch (info.bits) {
                1 => "bool",
                8 => "uint8",
                16 => "uint16",
                32 => "uint32",
                64 => "uint64",
                128 => "Uint128",
                else => @compileError("invalid int type"),
            };
        },
        else => @compileError("Unhandled type: " ++ @typeName(Type)),
    }
}

fn get_mapped_type_name(comptime Type: type) ?[]const u8 {
    inline for (type_mappings) |type_mapping| {
        if (Type == type_mapping[0]) {
            return type_mapping[1];
        }
    } else return null;
}

fn calculate_min_len(comptime type_info: anytype) comptime_int {
    comptime {
        var min_len: comptime_int = 0;
        for (type_info.fields) |field| {
            const field_len = SourcePrinter.to_case_initialisms(
                field.name,
                .pascal_case,
                go_initialisms,
            ).len;
            if (field_len > min_len) {
                min_len = field_len;
            }
        }
        return min_len;
    }
}

fn emit_enum(
    printer: *SourcePrinter,
    comptime Type: type,
    comptime name: []const u8,
    comptime prefix: []const u8,
    comptime tag_type: []const u8,
) !void {
    try printer.print_indented("type {s} {s}", .{ name, tag_type });
    try printer.write_empty_line();
    try printer.write_indented("const (");
    printer.indent();

    const type_info = @typeInfo(Type).@"enum";
    const min_len = calculate_min_len(type_info);
    inline for (type_info.fields) |field| {
        if (comptime std.mem.startsWith(u8, field.name, "deprecated_")) continue;
        const enum_name = prefix ++ comptime SourcePrinter.to_case_initialisms(
            field.name,
            .pascal_case,
            go_initialisms,
        );
        const enum_width = prefix.len + min_len;
        if (type_info.tag_type == u1) {
            try printer.print_indented("{s:<[3]} {s} = {s}", .{
                enum_name,
                name,
                if (@intFromEnum(@field(Type, field.name)) == 1) "true" else "false",
                enum_width,
            });
        } else {
            const int_value = @intFromEnum(@field(Type, field.name));
            try printer.print_indented("{s:<[3]} {s} = {s}", .{
                enum_name,
                name,
                if (int_value == std.math.maxInt(@TypeOf(int_value)))
                    std.fmt.comptimePrint("0x{X}", .{int_value})
                else
                    std.fmt.comptimePrint("{}", .{int_value}),
                enum_width,
            });
        }
    }

    printer.dedent();
    try printer.write_indented(")");
    try printer.write_empty_line();
    try printer.print_indented("func (i {s}) String() string {{", .{name});
    printer.indent();

    if (type_info.tag_type == u1) {
        const enum_zero_name = prefix ++ comptime SourcePrinter.to_case_initialisms(
            @tagName(@as(Type, @enumFromInt(0))),
            .pascal_case,
            go_initialisms,
        );
        const enum_one_name = prefix ++ comptime SourcePrinter.to_case_initialisms(
            @tagName(@as(Type, @enumFromInt(1))),
            .pascal_case,
            go_initialisms,
        );

        try printer.print_indented("if (i == {s}) {{", .{enum_one_name});
        printer.indent();
        try printer.print_indented("return \"{s}\"", .{enum_one_name});
        printer.dedent();
        try printer.write_indented("} else {");
        printer.indent();
        try printer.print_indented("return \"{s}\"", .{enum_zero_name});
        printer.dedent();
        try printer.write_indented("}");
    } else {
        try printer.write_indented("switch i {");

        inline for (type_info.fields) |field| {
            if (comptime std.mem.startsWith(u8, field.name, "deprecated_")) continue;
            const enum_name = prefix ++ comptime SourcePrinter.to_case_initialisms(
                field.name,
                .pascal_case,
                go_initialisms,
            );
            try printer.print_indented("case {s}:", .{enum_name});
            printer.indent();
            try printer.print_indented("return \"{s}\"", .{enum_name});
            printer.dedent();
        }

        try printer.write_indented("}");
        try printer.print_indented(
            "return \"{s}(\" + strconv.FormatInt(int64(i+1), 10) + \")\"",
            .{name},
        );
    }

    printer.dedent();
    try printer.write_indented("}");
    try printer.write_empty_line();
}

fn emit_packed_struct(
    printer: *SourcePrinter,
    comptime type_info: anytype,
    comptime name: []const u8,
    comptime int_type: []const u8,
) !void {
    try printer.print_indented("type {s} struct {{", .{name});
    printer.indent();

    const min_len = calculate_min_len(type_info);
    inline for (type_info.fields) |field| {
        if (comptime std.mem.eql(u8, "padding", field.name)) continue;
        try printer.print_indented("{s:<[2]} {s}", .{
            SourcePrinter.to_case_initialisms(field.name, .pascal_case, go_initialisms),
            go_type(field.type),
            min_len,
        });
    }

    printer.dedent();
    try printer.write_indented("}");
    try printer.write_empty_line();

    // Conversion from struct to packed (e.g. AccountFlags.ToUint16())
    try printer.print_indented("func (f {s}) To{s}() {s} {{", .{
        name,
        SourcePrinter.to_case_initialisms(int_type, .pascal_case, go_initialisms),
        int_type,
    });
    printer.indent();
    try printer.print_indented("var ret {s} = 0", .{int_type});
    try printer.write_empty_line();

    inline for (type_info.fields, 0..) |field, i| {
        if (comptime std.mem.eql(u8, "padding", field.name)) continue;

        try printer.print_indented("if f.{s} {{", .{
            SourcePrinter.to_case_initialisms(field.name, .pascal_case, go_initialisms),
        });
        printer.indent();
        try printer.print_indented("ret |= (1 << {d})", .{i});
        printer.dedent();
        try printer.write_indented("}");
        try printer.write_empty_line();
    }

    try printer.write_indented("return ret");
    printer.dedent();
    try printer.write_indented("}");
    try printer.write_empty_line();
}

fn emit_struct(
    printer: *SourcePrinter,
    comptime type_info: anytype,
    comptime name: []const u8,
) !void {
    try printer.print_indented("type {s} struct {{", .{name});
    printer.indent();

    const min_len = calculate_min_len(type_info);
    comptime var flagsField = false;
    inline for (type_info.fields) |field| {
        switch (@typeInfo(field.type)) {
            .array => |array| {
                try printer.print_indented("{s:<[3]} [{d}]{s}", .{
                    SourcePrinter.to_case_initialisms(field.name, .pascal_case, go_initialisms),
                    array.len,
                    go_type(array.child),
                    min_len,
                });
            },
            else => {
                if (comptime std.mem.eql(u8, field.name, "flags")) {
                    flagsField = true;
                }

                try printer.print_indented(
                    "{s:<[2]} {s}",
                    .{
                        SourcePrinter.to_case_initialisms(
                            field.name,
                            .pascal_case,
                            go_initialisms,
                        ),
                        go_type(field.type),
                        min_len,
                    },
                );
            },
        }
    }

    printer.dedent();
    try printer.write_indented("}");
    try printer.write_empty_line();

    if (flagsField) {
        const flagType = if (comptime std.mem.eql(u8, name, "Account"))
            tb.AccountFlags
        else if (comptime std.mem.eql(u8, name, "Transfer"))
            tb.TransferFlags
        else if (comptime std.mem.eql(u8, name, "AccountFilter"))
            tb.AccountFilterFlags
        else if (comptime std.mem.eql(u8, name, "QueryFilter"))
            tb.QueryFilterFlags
        else
            unreachable;
        // Conversion from packed to struct (e.g. Account.AccountFlags())
        try printer.print_indented("func (o {s}) {s}Flags() {s}Flags {{", .{ name, name, name });
        printer.indent();
        try printer.print_indented("var f {s}Flags", .{name});

        switch (@typeInfo(flagType)) {
            .@"struct" => |info| switch (info.layout) {
                .@"packed" => inline for (info.fields, 0..) |field, i| {
                    if (comptime std.mem.eql(u8, "padding", field.name)) continue;

                    try printer.print_indented("f.{s} = ((o.Flags >> {}) & 0x1) == 1", .{
                        SourcePrinter.to_case_initialisms(field.name, .pascal_case, go_initialisms),
                        i,
                    });
                },
                else => unreachable,
            },
            else => unreachable,
        }

        try printer.write_indented("return f");
        printer.dedent();
        try printer.write_indented("}");
        try printer.write_empty_line();
    }
}

pub fn generate_bindings(printer: *SourcePrinter) !void {
    @setEvalBranchQuota(100_000);

    try printer.write(
        \\///////////////////////////////////////////////////////
        \\// This file was auto-generated by go_bindings.zig   //
        \\//              Do not manually modify.              //
        \\///////////////////////////////////////////////////////
        \\
        \\package tigerbeetle_go
        \\
        \\/*
        \\#include "./native/tb_client.h"
        \\*/
        \\import "C"
        \\import "strconv"
        \\
        \\
    );

    // Emit Go declarations.
    inline for (type_mappings) |type_mapping| {
        const ZigType = type_mapping[0];
        const name = type_mapping[1];

        switch (@typeInfo(ZigType)) {
            .@"struct" => |info| switch (info.layout) {
                .auto => @compileError(
                    "Only packed or extern structs are supported: " ++ @typeName(ZigType),
                ),
                .@"packed" => try emit_packed_struct(
                    printer,
                    info,
                    name,
                    comptime go_type(std.meta.Int(.unsigned, @bitSizeOf(ZigType))),
                ),
                .@"extern" => try emit_struct(printer, info, name),
            },
            .@"enum" => try emit_enum(
                printer,
                ZigType,
                name,
                type_mapping[2],
                comptime go_type(std.meta.Int(.unsigned, @bitSizeOf(ZigType))),
            ),
            else => @compileError("Type cannot be represented: " ++ @typeName(ZigType)),
        }
    }
    const buffer = printer.writer;
    assert(buffer.buffered()[buffer.end - 1] == '\n');
    buffer.undo(1);
    assert(std.mem.endsWith(u8, buffer.buffered(), "\n"));
    assert(!std.mem.endsWith(u8, buffer.buffered(), "\n\n"));
}

pub fn main(init: std.process.Init) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var buffer: std.Io.Writer.Allocating = .init(allocator);
    var printer = SourcePrinter.init(allocator, &buffer.writer, .{
        .indent_width = 1,
        .indent_char = '\t',
        .comment_marker = "//",
    });
    try generate_bindings(&printer);
    try std.Io.File.stdout().writeStreamingAll(init.io, buffer.written());
}
