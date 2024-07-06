const std = @import("std");
const assert = std.debug.assert;

// TODO: Move this back to src/clients/node when there's a better solution for main_pkg_path=src/
const vsr = @import("vsr.zig");
const tb = vsr.tigerbeetle;
const tb_client = vsr.tb_client;

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
    .{ tb.CreateAccountResult, TypeMapping{
        .name = "CreateAccountError",
        .docs_link = "reference/requests/create_accounts#",
    } },
    .{ tb.CreateTransferResult, TypeMapping{
        .name = "CreateTransferError",
        .docs_link = "reference/requests/create_transfers#",
    } },
    .{ tb.CreateAccountsResult, TypeMapping{
        .name = "CreateAccountsError",
    } },
    .{ tb.CreateTransfersResult, TypeMapping{
        .name = "CreateTransfersError",
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
    .{ tb_client.tb_operation_t, TypeMapping{
        .name = "Operation",
        .hidden_fields = &.{ "reserved", "root", "register" },
    } },
};

fn typescript_type(comptime Type: type) []const u8 {
    switch (@typeInfo(Type)) {
        .Enum => return comptime get_mapped_type_name(Type) orelse @compileError(
            "Type " ++ @typeName(Type) ++ " not mapped.",
        ),
        .Struct => |info| switch (info.layout) {
            .@"packed" => return comptime typescript_type(
                std.meta.Int(.unsigned, @bitSizeOf(Type)),
            ),
            else => return comptime get_mapped_type_name(Type) orelse @compileError(
                "Type " ++ @typeName(Type) ++ " not mapped.",
            ),
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
    assert(type_info.layout == .@"packed");
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

fn emit_docs(
    buffer: anytype,
    comptime mapping: TypeMapping,
    comptime indent: comptime_int,
    comptime field: ?[]const u8,
) !void {
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
                .auto => @compileError(
                    "Only packed or extern structs are supported: " ++ @typeName(ZigType),
                ),
                .@"packed" => try emit_packed_struct(buffer, info, mapping),
                .@"extern" => try emit_struct(buffer, info, mapping),
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
    try std.fs.cwd().writeFile(.{ .sub_path = output_file, .data = buffer.items });
}

const testing = std.testing;

test "bindings node" {
    var buffer = std.ArrayList(u8).init(testing.allocator);
    defer buffer.deinit();

    try generate_bindings(&buffer);

    const current = try std.fs.cwd().readFileAlloc(
        testing.allocator,
        output_file,
        std.math.maxInt(usize),
    );
    defer testing.allocator.free(current);

    try testing.expectEqualStrings(current, buffer.items);
}
