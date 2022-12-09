const std = @import("std");
const tb = @import("../../../tigerbeetle.zig");
const tb_client = @import("../../c/tb_client.zig");

const type_mappings = .{
    .{ tb.AccountFlags, "AccountFlags", .public },
    .{ tb.Account, "Account", .public },
    .{ tb.TransferFlags, "TransferFlags", .public },
    .{ tb.Transfer, "Transfer", .public },
    .{ tb.CreateAccountResult, "CreateAccountResult", .public },
    .{ tb.CreateTransferResult, "CreateTransferResult", .public },
    .{ tb.CreateAccountsResult, "CreateAccountsResult", .public },
    .{ tb.CreateTransfersResult, "CreateTransfersResult", .public },
    .{ tb_client.tb_operation_t, "TBOperation", .internal },
    .{ tb_client.tb_packet_status_t, "PacketStatus", .public },
    .{ tb_client.tb_packet_t, "TBPacket", .internal },
    .{ tb_client.tb_packet_list_t, "TBPacketList", .internal },
    .{ tb_client.tb_status_t, "InitializationStatus", .public },
};

fn dotnet_type(comptime Type: type) []const u8 {
    switch (@typeInfo(Type)) {
        .Enum, .Struct => return comptime get_mapped_type_name(Type) orelse @compileError("Type " ++ @typeName(Type) ++ " not mapped."),
        .Int => |info| {
            std.debug.assert(info.signedness == .unsigned);
            return switch (info.bits) {
                8 => "byte",
                16 => "short",
                32 => "int",
                64 => "ulong",
                128 => "UInt128",
                else => @compileError("invalid int type"),
            };
        },
        .Optional => |info| switch (@typeInfo(info.child)) {
            .Pointer => return dotnet_type(info.child),
            else => @compileError("Unsupported optional type: " ++ @typeName(Type)),
        },
        .Pointer => |info| {
            std.debug.assert(info.size != .Slice);
            std.debug.assert(!info.is_allowzero);

            return if (comptime get_mapped_type_name(info.child)) |name| name ++ "*" else dotnet_type(info.child);
        },
        .Void, .Opaque => return "IntPtr",
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

fn to_case(comptime input: []const u8, comptime case: enum { camel, pascal }) []const u8 {
    comptime {
        var len: usize = 0;
        var word_start: bool = false;
        var output: [input.len]u8 = undefined;
        inline for (input) |char| {
            if (char == '_') {
                word_start = true;
                continue;
            }

            output[len] = if (word_start) std.ascii.toUpper(char) else char;
            word_start = false;
            len += 1;
        }

        switch (case) {
            .camel => output[0] = std.ascii.toLower(output[0]),
            .pascal => output[0] = std.ascii.toUpper(output[0]),
        }

        return output[0..len];
    }
}

fn emit_enum(
    buffer: *std.ArrayList(u8),
    comptime visibility: anytype,
    comptime type_info: anytype,
    comptime int_type: []const u8,
    comptime name: []const u8,
    comptime value_fmt: []const u8,
    comptime skip_fields: []const []const u8,
) !void {
    try buffer.writer().print(
        \\    {s} enum {s} : {s}
        \\    {{
        \\
    , .{
        @tagName(visibility),
        name,
        int_type,
    });

    if (@TypeOf(type_info) == std.builtin.TypeInfo.Struct) {
        try buffer.writer().print(
            \\        None = 0,
            \\
        , .{});
    }

    inline for (type_info.fields) |field, i| {
        comptime var skip = false;
        inline for (skip_fields) |sf| {
            skip = skip or comptime std.mem.eql(u8, sf, field.name);
        }

        if (!skip) {
            try buffer.writer().print(
                \\        {s} = 
            ++ value_fmt ++ ",\n", .{
                to_case(field.name, .pascal),
                i,
            });
        }
    }

    try buffer.writer().print(
        \\    }}
        \\
        \\
    , .{});
}

fn emit_struct(
    buffer: *std.ArrayList(u8),
    comptime visibility: anytype,
    comptime type_info: anytype,
    comptime name: []const u8,
    comptime size: usize,
) !void {
    try buffer.writer().print(
        \\    [StructLayout(LayoutKind.Sequential, Size = SIZE)]
        \\    {s} {s}struct {s}
        \\    {{
        \\        public const int SIZE = {};    
        \\
        \\
    , .{
        @tagName(visibility),
        if (visibility == .internal) "unsafe " else " ",
        name,
        size,
    });

    // Fixed len array are exposed as internal structs with stackalloc fields
    // It's more efficient than exposing heap-allocated arrays using
    // [MarshalAs(UnmanagedType.ByValArray)] attribute.
    inline for (type_info.fields) |field| {
        switch (@typeInfo(field.field_type)) {
            .Array => |array| {
                try buffer.writer().print(
                    \\        [StructLayout(LayoutKind.Sequential, Size = SIZE)]
                    \\        private unsafe struct {s}Data
                    \\        {{
                    \\            public const int SIZE = {};
                    \\
                    \\            private fixed byte raw[SIZE];
                    \\
                    \\            public byte[] GetData()
                    \\            {{
                    \\                fixed (void* ptr = raw)
                    \\                {{
                    \\                    return new ReadOnlySpan<byte>(ptr, SIZE).ToArray();
                    \\                }}
                    \\            }}
                    \\
                    \\            public void SetData(byte[] value)
                    \\            {{
                    \\                if (value == null) throw new ArgumentNullException(nameof(value));
                    \\                if (value.Length != SIZE) throw new ArgumentOutOfRangeException(nameof(value));
                    \\
                    \\                fixed (void* ptr = raw)
                    \\                {{
                    \\                    value.CopyTo(new Span<byte>(ptr, SIZE));
                    \\                }}
                    \\            }}
                    \\        }}
                    \\
                    \\
                , .{
                    to_case(field.name, .pascal),
                    array.len,
                });
            },
            else => {},
        }
    }

    // Fields
    inline for (type_info.fields) |field| {
        switch (@typeInfo(field.field_type)) {
            .Array => try buffer.writer().print(
                \\        {s} {s}Data {s};
                \\
                \\
            ,
                .{
                    if (visibility == .internal) "public" else "private",
                    to_case(field.name, .pascal),
                    to_case(field.name, .camel),
                },
            ),
            else => try buffer.writer().print(
                \\        {s} {s} {s};
                \\
                \\
            ,
                .{
                    if (visibility == .internal) "public" else "private",
                    dotnet_type(field.field_type),
                    to_case(field.name, .camel),
                },
            ),
        }
    }

    if (visibility == .public) {
        try buffer.writer().print("\n", .{});

        // Properties
        inline for (type_info.fields) |field| {
            switch (@typeInfo(field.field_type)) {
                .Array => try buffer.writer().print(
                    \\        public byte[] {s} {{ get => {s}.GetData(); set => {s}.SetData(value); }}
                    \\
                    \\
                , .{
                    to_case(field.name, .pascal),
                    to_case(field.name, .camel),
                    to_case(field.name, .camel),
                }),
                else => try buffer.writer().print(
                    \\        public {s} {s} {{ get => {s}; set => {s} = value; }}
                    \\
                    \\
                , .{
                    dotnet_type(field.field_type),
                    to_case(field.name, .pascal),
                    to_case(field.name, .camel),
                    to_case(field.name, .camel),
                }),
            }
        }
    }

    try buffer.writer().print(
        \\  }}
        \\
        \\
    , .{});
}

pub fn main() !void {
    @setEvalBranchQuota(100_000);

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var buffer = std.ArrayList(u8).init(allocator);
    try buffer.writer().print(
        \\//////////////////////////////////////////////////////////
        \\// This file was auto-generated by dotnet_interop.zig //
        \\//              Do not manually modify.                 //
        \\//////////////////////////////////////////////////////////
        \\
        \\
        \\using System;
        \\using System.Runtime.InteropServices;
        \\
        \\namespace TigerBeetle
        \\{{
        \\
    , .{});

    // Emit C type declarations.
    inline for (type_mappings) |type_mapping| {
        const ZigType = type_mapping[0];
        const name = type_mapping[1];
        const visibility = type_mapping[2];

        switch (@typeInfo(ZigType)) {
            .Struct => |info| switch (info.layout) {
                .Auto => @compileError("Invalid C struct type: " ++ @typeName(ZigType)),
                .Packed => try emit_enum(
                    &buffer,
                    visibility,
                    info,
                    comptime dotnet_type(std.meta.Int(.unsigned, @bitSizeOf(ZigType))),
                    name,
                    "1 << {d}",
                    &.{"padding"},
                ),
                .Extern => try emit_struct(
                    &buffer,
                    visibility,
                    info,
                    name,
                    @sizeOf(ZigType),
                ),
            },
            .Enum => |info| {
                comptime var skip: []const []const u8 = &.{};
                if (ZigType == tb_client.tb_operation_t) {
                    skip = &.{ "reserved", "root", "register" };
                }

                try emit_enum(
                    &buffer,
                    visibility,
                    info,
                    comptime dotnet_type(std.meta.Int(.unsigned, @bitSizeOf(ZigType))),
                    name,
                    "{d}",
                    skip,
                );
            },
            else => try buffer.writer().print("typedef {s} {s}; \n\n", .{
                dotnet_type(ZigType),
                name,
            }),
        }
    }

    // Emit C function declarations.
    // TODO: use `std.meta.declaractions` and generate with pub + export functions.
    // Zig 0.9.1 has `decl.data.Fn.arg_names` but it's currently/incorrectly a zero-sized slice.
    try buffer.writer().print(
        \\}}
        \\
        \\
    , .{});

    try std.fs.cwd().writeFile("src/clients/dotnet/src/TigerBeetle/Interop.cs", buffer.items);
}
