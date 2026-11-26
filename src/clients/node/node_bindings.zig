const std = @import("std");
const vsr = @import("vsr");
const stdx = vsr.stdx;
const SourcePrinter = stdx.SourcePrinter;

const assert = std.debug.assert;
const tb = vsr.tigerbeetle;
const tb_client = vsr.tb_client;

const TypeMapping = struct {
    name: []const u8,
    hidden_fields: []const []const u8 = &.{},
    docs_link: ?[]const u8 = null,

    pub fn hidden(comptime self: @This(), name: []const u8) bool {
        inline for (self.hidden_fields) |field| {
            if (std.mem.eql(u8, field, name)) {
                return true;
            }
        } else return false;
    }
};

const type_mappings = .{
    .{ tb.AccountFlags, TypeMapping{
        .name = "AccountFlags",
        .hidden_fields = &.{"padding"},
        .docs_link = "reference/account#flags",
    } },
    .{ tb.TransferFlags, TypeMapping{
        .name = "TransferFlags",
        .hidden_fields = &.{"padding"},
        .docs_link = "reference/transfer#flags",
    } },
    .{ tb.AccountFilterFlags, TypeMapping{
        .name = "AccountFilterFlags",
        .hidden_fields = &.{"padding"},
        .docs_link = "reference/account-filter#flags",
    } },
    .{ tb.QueryFilterFlags, TypeMapping{
        .name = "QueryFilterFlags",
        .hidden_fields = &.{"padding"},
        .docs_link = "reference/query-filter#flags",
    } },
    .{ tb.Account, TypeMapping{
        .name = "Account",
        .docs_link = "reference/account/#",
    } },
    .{ tb.Transfer, TypeMapping{
        .name = "Transfer",
        .docs_link = "reference/transfer/#",
    } },
    .{ tb.CreateAccountStatus, TypeMapping{
        .name = "CreateAccountStatus",
        .docs_link = "reference/requests/create_accounts#",
    } },
    .{ tb.CreateTransferStatus, TypeMapping{
        .name = "CreateTransferStatus",
        .docs_link = "reference/requests/create_transfers#",
    } },
    .{ tb.CreateAccountResult, TypeMapping{
        .name = "CreateAccountResult",
        .hidden_fields = &.{"reserved"},
    } },
    .{ tb.CreateTransferResult, TypeMapping{
        .name = "CreateTransferResult",
        .hidden_fields = &.{"reserved"},
    } },
    .{ tb.AccountFilter, TypeMapping{
        .name = "AccountFilter",
        .hidden_fields = &.{"reserved"},
        .docs_link = "reference/account-filter#",
    } },
    .{ tb.QueryFilter, TypeMapping{
        .name = "QueryFilter",
        .hidden_fields = &.{"reserved"},
        .docs_link = "reference/query-filter#",
    } },
    .{ tb.AccountBalance, TypeMapping{
        .name = "AccountBalance",
        .hidden_fields = &.{"reserved"},
        .docs_link = "reference/account-balances#",
    } },
    .{ tb_client.Operation, TypeMapping{
        .name = "Operation",
        .hidden_fields = &.{ "reserved", "root", "register" },
    } },
};

fn typescript_type(comptime Type: type) []const u8 {
    switch (@typeInfo(Type)) {
        .@"enum" => return comptime get_mapped_type_name(Type) orelse @compileError(
            "Type " ++ @typeName(Type) ++ " not mapped.",
        ),
        .@"struct" => |info| switch (info.layout) {
            .@"packed" => return comptime typescript_type(
                std.meta.Int(.unsigned, @bitSizeOf(Type)),
            ),
            else => return comptime get_mapped_type_name(Type) orelse @compileError(
                "Type " ++ @typeName(Type) ++ " not mapped.",
            ),
        },
        .int => |info| {
            assert(info.signedness == .unsigned);
            return switch (info.bits) {
                16 => "number",
                32 => "number",
                64 => "bigint",
                128 => "bigint",
                else => @compileError("invalid int type: " ++ @typeName(Type)),
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

fn emit_enum(
    printer: *SourcePrinter,
    comptime Type: type,
    comptime mapping: TypeMapping,
) !void {
    try emit_docs(printer, mapping, null);

    try printer.print_indented("export enum {s} {{", .{mapping.name});
    printer.indent();

    inline for (@typeInfo(Type).@"enum".fields) |field| {
        if (comptime std.mem.startsWith(u8, field.name, "deprecated_")) continue;
        if (comptime mapping.hidden(field.name)) continue;

        try emit_docs(printer, mapping, field.name);

        const int_value = @intFromEnum(@field(Type, field.name));
        try printer.print_indented("{s} = {s},", .{
            field.name,
            if (int_value == std.math.maxInt(@TypeOf(int_value)))
                std.fmt.comptimePrint("0x{X}", .{int_value})
            else
                std.fmt.comptimePrint("{}", .{int_value}),
        });
    }

    printer.dedent();
    try printer.write_indented("}");
    try printer.write_empty_line();
}

fn emit_packed_struct(
    printer: *SourcePrinter,
    comptime type_info: anytype,
    comptime mapping: TypeMapping,
) !void {
    assert(type_info.layout == .@"packed");
    try emit_docs(printer, mapping, null);

    try printer.print_indented("export enum {s} {{", .{mapping.name});
    printer.indent();
    try printer.write_indented("none = 0,");

    inline for (type_info.fields, 0..) |field, i| {
        if (comptime mapping.hidden(field.name)) continue;

        try emit_docs(printer, mapping, field.name);

        try printer.print_indented("{s} = (1 << {d}),", .{
            field.name,
            i,
        });
    }

    printer.dedent();
    try printer.write_indented("}");
    try printer.write_empty_line();
}

fn emit_struct(
    printer: *SourcePrinter,
    comptime type_info: anytype,
    comptime mapping: TypeMapping,
) !void {
    try emit_docs(printer, mapping, null);

    try printer.print_indented("export type {s} = {{", .{mapping.name});
    printer.indent();

    inline for (type_info.fields) |field| {
        if (comptime mapping.hidden(field.name)) continue;

        try emit_docs(printer, mapping, field.name);

        switch (@typeInfo(field.type)) {
            .array => try printer.print_indented("{s}: Buffer", .{field.name}),
            else => try printer.print_indented("{s}: {s}", .{
                field.name,
                typescript_type(field.type),
            }),
        }
    }

    printer.dedent();
    try printer.write_indented("}");
    try printer.write_empty_line();
}

fn emit_docs(
    printer: *SourcePrinter,
    comptime mapping: TypeMapping,
    comptime field: ?[]const u8,
) !void {
    if (mapping.docs_link) |docs_link| {
        try printer.write_empty_line();
        try printer.write_indented("/**");
        try printer.print_indented(" * See [{s}](https://docs.tigerbeetle.com/{s}{s})", .{
            field orelse mapping.name,
            docs_link,
            field orelse "",
        });
        try printer.write_indented(" */");
    }
}

pub fn generate_bindings(printer: *SourcePrinter) !void {
    @setEvalBranchQuota(100_000);

    try printer.write(
        \\///////////////////////////////////////////////////////
        \\// This file was auto-generated by node_bindings.zig //
        \\//              Do not manually modify.              //
        \\///////////////////////////////////////////////////////
        \\
        \\
    );

    // Emit JS declarations.
    inline for (type_mappings) |type_mapping| {
        const ZigType = type_mapping[0];
        const mapping = type_mapping[1];

        switch (@typeInfo(ZigType)) {
            .@"struct" => |info| switch (info.layout) {
                .auto => @compileError(
                    "Only packed or extern structs are supported: " ++ @typeName(ZigType),
                ),
                .@"packed" => try emit_packed_struct(printer, info, mapping),
                .@"extern" => try emit_struct(printer, info, mapping),
            },
            .@"enum" => try emit_enum(printer, ZigType, mapping),
            else => @compileError("Type cannot be represented: " ++ @typeName(ZigType)),
        }
    }
}

pub fn main(init: std.process.Init) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var buffer: std.Io.Writer.Allocating = .init(allocator);
    var printer = SourcePrinter.init(allocator, &buffer.writer, .{
        .indent_width = 2,
        .indent_char = ' ',
        .comment_marker = "//",
    });
    try generate_bindings(&printer);
    try std.Io.File.stdout().writeStreamingAll(init.io, buffer.written());
}
