const std = @import("std");
const assert = std.debug.assert;

const tb = @import("../../tigerbeetle.zig");
const tb_client = @import("../c/tb_client.zig");

const output_file = "src/clients/node/src/bindings.ts";

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
        .docs_link = "reference/accounts#flags",
    } },
    .{ tb.TransferFlags, TypeMapping{
        .name = "TransferFlags",
        .hidden_fields = &.{"padding"},
        .docs_link = "reference/transfers#flags",
    } },
    .{ tb.GetAccountTransfersFlags, TypeMapping{
        .name = "GetAccountTransfersFlags",
        .hidden_fields = &.{"padding"},
        .docs_link = "reference/operations/get_account_transfers#flags",
    } },
    .{ tb.Account, TypeMapping{
        .name = "Account",
        .docs_link = "reference/accounts/#",
    } },
    .{ tb.Transfer, TypeMapping{
        .name = "Transfer",
        .docs_link = "reference/transfers/#",
    } },
    .{ tb.CreateAccountResult, TypeMapping{
        .name = "CreateAccountError",
        .docs_link = "reference/operations/create_accounts#",
    } },
    .{ tb.CreateTransferResult, TypeMapping{
        .name = "CreateTransferError",
        .docs_link = "reference/operations/create_transfers#",
    } },
    .{ tb.CreateAccountsResult, TypeMapping{
        .name = "CreateAccountsError",
    } },
    .{ tb.CreateTransfersResult, TypeMapping{
        .name = "CreateTransfersError",
    } },
    .{ tb.GetAccountTransfers, TypeMapping{
        .name = "GetAccountTransfers",
        .docs_link = "reference/operations/get_account_transfers#",
    } },
    .{ tb_client.tb_operation_t, TypeMapping{
        .name = "Operation",
        .hidden_fields = &.{ "reserved", "root", "register" },
    } },
};

fn typescript_type(comptime Type: type) []const u8 {
    switch (@typeInfo(Type)) {
        .Enum => return comptime get_mapped_type_name(Type) orelse @compileError("Type " ++ @typeName(Type) ++ " not mapped."),
        .Struct => |info| switch (info.layout) {
            .Packed => return comptime typescript_type(std.meta.Int(.unsigned, @bitSizeOf(Type))),
            else => return comptime get_mapped_type_name(Type) orelse @compileError("Type " ++ @typeName(Type) ++ " not mapped."),
        },
        .Int => |info| {
            std.debug.assert(info.signedness == .unsigned);
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
    buffer: *std.ArrayList(u8),
    comptime Type: type,
    comptime mapping: TypeMapping,
) !void {
    try emit_docs(buffer, mapping, 0, null);

    try buffer.writer().print("export enum {s} {{\n", .{mapping.name});

    inline for (@typeInfo(Type).Enum.fields) |field| {
        if (comptime mapping.hidden(field.name)) continue;

        try emit_docs(buffer, mapping, 1, field.name);

        try buffer.writer().print("  {s} = {d},\n", .{
            field.name,
            @intFromEnum(@field(Type, field.name)),
        });
    }

    try buffer.writer().print("}}\n\n", .{});
}

fn emit_packed_struct(
    buffer: *std.ArrayList(u8),
    comptime type_info: anytype,
    comptime mapping: TypeMapping,
) !void {
    assert(type_info.layout == .Packed);
    try emit_docs(buffer, mapping, 0, null);

    try buffer.writer().print(
        \\export enum {s} {{
        \\  none = 0,
        \\
    , .{mapping.name});

    inline for (type_info.fields, 0..) |field, i| {
        if (comptime mapping.hidden(field.name)) continue;

        try emit_docs(buffer, mapping, 1, field.name);

        try buffer.writer().print("  {s} = (1 << {d}),\n", .{
            field.name,
            i,
        });
    }

    try buffer.writer().print("}}\n\n", .{});
}

fn emit_struct(
    buffer: *std.ArrayList(u8),
    comptime type_info: anytype,
    comptime mapping: TypeMapping,
) !void {
    try emit_docs(buffer, mapping, 0, null);

    try buffer.writer().print("export type {s} = {{\n", .{
        mapping.name,
    });

    inline for (type_info.fields) |field| {
        if (comptime mapping.hidden(field.name)) continue;

        try emit_docs(buffer, mapping, 1, field.name);

        switch (@typeInfo(field.type)) {
            .Array => try buffer.writer().print("  {s}: Buffer\n", .{
                field.name,
            }),
            else => try buffer.writer().print(
                "  {s}: {s}\n",
                .{
                    field.name,
                    typescript_type(field.type),
                },
            ),
        }
    }

    try buffer.writer().print("}}\n\n", .{});
}

fn emit_docs(buffer: anytype, comptime mapping: TypeMapping, comptime indent: comptime_int, comptime field: ?[]const u8) !void {
    if (mapping.docs_link) |docs_link| {
        try buffer.writer().print(
            \\
            \\{[indent]s}/**
            \\{[indent]s}* See [{[name]s}](https://docs.tigerbeetle.com/{[docs_link]s}{[field]s})
            \\{[indent]s}*/
            \\
        , .{
            .indent = "  " ** indent,
            .name = field orelse mapping.name,
            .docs_link = docs_link,
            .field = field orelse "",
        });
    }
}

pub fn generate_bindings(buffer: *std.ArrayList(u8)) !void {
    @setEvalBranchQuota(100_000);

    try buffer.writer().print(
        \\///////////////////////////////////////////////////////
        \\// This file was auto-generated by node_bindings.zig //
        \\//              Do not manually modify.              //
        \\///////////////////////////////////////////////////////
        \\
        \\
    , .{});

    // Emit JS declarations.
    inline for (type_mappings) |type_mapping| {
        const ZigType = type_mapping[0];
        const mapping = type_mapping[1];

        switch (@typeInfo(ZigType)) {
            .Struct => |info| switch (info.layout) {
                .Auto => @compileError("Only packed or extern structs are supported: " ++ @typeName(ZigType)),
                .Packed => try emit_packed_struct(buffer, info, mapping),
                .Extern => try emit_struct(buffer, info, mapping),
            },
            .Enum => try emit_enum(buffer, ZigType, mapping),
            else => @compileError("Type cannot be represented: " ++ @typeName(ZigType)),
        }
    }
}

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var buffer = std.ArrayList(u8).init(allocator);
    try generate_bindings(&buffer);
    try std.fs.cwd().writeFile(output_file, buffer.items);
}

const testing = std.testing;

test "bindings node" {
    var buffer = std.ArrayList(u8).init(testing.allocator);
    defer buffer.deinit();

    try generate_bindings(&buffer);

    const current = try std.fs.cwd().readFileAlloc(testing.allocator, output_file, std.math.maxInt(usize));
    defer testing.allocator.free(current);

    try testing.expectEqualStrings(current, buffer.items);
}
