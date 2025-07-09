const std = @import("std");
const vsr = @import("vsr");

const assert = std.debug.assert;
const stdx = vsr.stdx;
const tb = vsr.tigerbeetle;
const exports = vsr.tb_client.exports;

const TypeMapping = struct {
    name: []const u8,
    visibility: enum { public, internal },
    private_fields: []const []const u8 = &.{},
    readonly_fields: []const []const u8 = &.{},
    docs_link: ?[]const u8 = null,
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

const type_mappings = .{
    .{ tb.AccountFlags, TypeMapping{
        .name = "AccountFlags",
        .visibility = .public,
        .private_fields = &.{"padding"},
        .docs_link = "reference/account#flags",
    } },
    .{ tb.TransferFlags, TypeMapping{
        .name = "TransferFlags",
        .visibility = .public,
        .private_fields = &.{"padding"},
        .docs_link = "reference/transfer#flags",
    } },
    .{ tb.AccountFilterFlags, TypeMapping{
        .name = "AccountFilterFlags",
        .visibility = .public,
        .private_fields = &.{"padding"},
        .docs_link = "reference/account-filter#flags",
    } },
    .{ tb.QueryFilterFlags, TypeMapping{
        .name = "QueryFilterFlags",
        .visibility = .public,
        .private_fields = &.{"padding"},
        .docs_link = "reference/query-filter#flags",
    } },
    .{ tb.Account, TypeMapping{
        .name = "Account",
        .visibility = .public,
        .private_fields = &.{"reserved"},
        .readonly_fields = &.{
            "debits_pending",
            "credits_pending",
            "debits_posted",
            "credits_posted",
        },
        .docs_link = "reference/account#",
    } },
    .{
        tb.Transfer, TypeMapping{
            .name = "Transfer",
            .visibility = .public,
            .private_fields = &.{"reserved"},
            .readonly_fields = &.{},
            .docs_link = "reference/transfer#",
            .constants =
            \\    public static UInt128 AmountMax => UInt128.MaxValue;
            \\
            ,
        },
    },
    .{ tb.CreateAccountResult, TypeMapping{
        .name = "CreateAccountResult",
        .visibility = .public,
        .docs_link = "reference/requests/create_accounts#",
    } },
    .{ tb.CreateTransferResult, TypeMapping{
        .name = "CreateTransferResult",
        .visibility = .public,
        .docs_link = "reference/requests/create_transfers#",
    } },
    .{ tb.CreateAccountsResult, TypeMapping{
        .name = "CreateAccountsResult",
        .visibility = .public,
    } },
    .{ tb.CreateTransfersResult, TypeMapping{
        .name = "CreateTransfersResult",
        .visibility = .public,
    } },
    .{ tb.AccountFilter, TypeMapping{
        .name = "AccountFilter",
        .visibility = .public,
        .private_fields = &.{"reserved"},
        .docs_link = "reference/account-filter#",
    } },
    .{ tb.AccountBalance, TypeMapping{
        .name = "AccountBalance",
        .visibility = .public,
        .private_fields = &.{"reserved"},
        .docs_link = "reference/account-balances#",
    } },
    .{ tb.QueryFilter, TypeMapping{
        .name = "QueryFilter",
        .visibility = .public,
        .private_fields = &.{"reserved"},
        .docs_link = "reference/query-filter#",
    } },
    .{ exports.tb_init_status, TypeMapping{
        .name = "InitializationStatus",
        .visibility = .public,
    } },
    .{ exports.tb_client_status, TypeMapping{
        .name = "ClientStatus",
        .visibility = .internal,
    } },
    .{ exports.tb_packet_status, TypeMapping{
        .name = "PacketStatus",
        .visibility = .public,
    } },
    .{ exports.tb_operation, TypeMapping{
        .name = "TBOperation",
        .visibility = .internal,
        .private_fields = &.{ "reserved", "root", "register" },
    } },
    .{ exports.tb_client_t, TypeMapping{
        .name = "TBClient",
        .visibility = .internal,
        .private_fields = &.{"opaque"},
    } },
    .{ exports.tb_packet_t, TypeMapping{
        .name = "TBPacket",
        .visibility = .internal,
        .private_fields = &.{"opaque"},
    } },
};

fn dotnet_type(comptime Type: type) []const u8 {
    switch (@typeInfo(Type)) {
        .@"enum", .@"struct" => return comptime get_mapped_type_name(Type) orelse
            @compileError("Type " ++ @typeName(Type) ++ " not mapped."),
        .bool => return "byte",
        .int => |info| {
            assert(info.signedness == .unsigned);
            return switch (info.bits) {
                8 => "byte",
                16 => "ushort",
                32 => "uint",
                64 => "ulong",
                128 => "UInt128",
                else => @compileError("invalid int type"),
            };
        },
        .optional => |info| switch (@typeInfo(info.child)) {
            .pointer => return dotnet_type(info.child),
            else => @compileError("Unsupported optional type: " ++ @typeName(Type)),
        },
        .pointer => |info| {
            assert(info.size != .slice);
            assert(!info.is_allowzero);

            return if (comptime get_mapped_type_name(info.child)) |name|
                name ++ "*"
            else
                dotnet_type(info.child);
        },
        .void, .@"opaque" => return "IntPtr",
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

fn to_case(comptime input: []const u8, comptime case: enum { camel, pascal }) []const u8 {
    return comptime blk: {
        var len: usize = 0;
        var output: [input.len]u8 = undefined;
        var iterator = std.mem.tokenizeScalar(u8, input, '_');
        while (iterator.next()) |word| {
            _ = std.ascii.lowerString(output[len..], word);
            output[len] = std.ascii.toUpper(output[len]);
            len += word.len;
        }

        output[0] = switch (case) {
            .camel => std.ascii.toLower(output[0]),
            .pascal => std.ascii.toUpper(output[0]),
        };

        break :blk stdx.comptime_slice(&output, len);
    };
}

fn emit_enum(
    buffer: *std.ArrayList(u8),
    comptime Type: type,
    comptime type_info: anytype,
    comptime mapping: TypeMapping,
    comptime int_type: []const u8,
) !void {
    const is_packed_struct = @TypeOf(type_info) == std.builtin.Type.Struct;
    if (is_packed_struct) {
        assert(type_info.layout == .@"packed");
        // Packed structs represented as Enum needs a Flags attribute:
        try buffer.writer().print("[Flags]\n", .{});
    }

    try buffer.writer().print(
        \\{s} enum {s} : {s}
        \\{{
        \\
    , .{
        @tagName(mapping.visibility),
        mapping.name,
        int_type,
    });

    if (is_packed_struct) {
        // Packed structs represented as Enum needs a ZERO value:
        try buffer.writer().print(
            \\    None = 0,
            \\
            \\
        , .{});
    }

    inline for (type_info.fields, 0..) |field, i| {
        if (comptime mapping.is_private(field.name)) continue;
        if (comptime std.mem.startsWith(u8, field.name, "deprecated_")) continue;

        try emit_docs(buffer, mapping, field.name);
        if (is_packed_struct) {
            try buffer.writer().print("    {s} = 1 << {},\n\n", .{
                to_case(field.name, .pascal),
                i,
            });
        } else {
            try buffer.writer().print("    {s} = {},\n\n", .{
                to_case(field.name, .pascal),
                @intFromEnum(@field(Type, field.name)),
            });
        }
    }

    try buffer.writer().print(
        \\}}
        \\
        \\
    , .{});
}

fn emit_struct(
    buffer: *std.ArrayList(u8),
    comptime type_info: anytype,
    comptime mapping: TypeMapping,
    comptime size: usize,
) !void {
    try buffer.writer().print(
        \\[StructLayout(LayoutKind.Sequential, Size = SIZE)]
        \\{s} {s}struct {s}
        \\{{
        \\    public const int SIZE = {};
        \\
        \\{s}
        \\
    , .{
        @tagName(mapping.visibility),
        if (mapping.visibility == .internal) "unsafe " else "",
        mapping.name,
        size,
        mapping.constants,
    });

    // Fixed len array are exposed as internal structs with stackalloc fields
    // It's more efficient than exposing heap-allocated arrays using
    // [MarshalAs(UnmanagedType.ByValArray)] attribute.
    inline for (type_info.fields) |field| {
        switch (@typeInfo(field.type)) {
            .array => |array| {
                try buffer.writer().print(
                    \\    [StructLayout(LayoutKind.Sequential, Size = {[name]s}Data.SIZE)]
                    \\    private unsafe struct {[name]s}Data
                    \\    {{
                    \\        public const int SIZE = {[size]};
                    \\        private const int LENGTH = {[len]};
                    \\
                    \\        private fixed {[child_type]s} raw[LENGTH];
                    \\
                    \\        public {[child_type]s}[] GetData()
                    \\        {{
                    \\            fixed (void* ptr = raw)
                    \\            {{
                    \\                return new ReadOnlySpan<{[child_type]s}>(ptr, LENGTH).ToArray();
                    \\            }}
                    \\        }}
                    \\
                    \\        public void SetData({[child_type]s}[] value)
                    \\        {{
                    \\            if (value == null) throw new ArgumentNullException(nameof(value));
                    \\            if (value.Length != LENGTH)
                    \\            {{
                    \\                throw new ArgumentException(
                    \\                    "Expected a {[child_type]s}[" + LENGTH + "] array",
                    \\                    nameof(value));
                    \\            }}
                    \\
                    \\            fixed (void* ptr = raw)
                    \\            {{
                    \\                value.CopyTo(new Span<{[child_type]s}>(ptr, LENGTH));
                    \\            }}
                    \\        }}
                    \\    }}
                    \\
                    \\
                , .{
                    .name = to_case(field.name, .pascal),
                    .size = array.len * @sizeOf(array.child),
                    .len = array.len,
                    .child_type = dotnet_type(array.child),
                });
            },
            else => {},
        }
    }

    // Fields
    inline for (type_info.fields) |field| {
        const is_private = comptime mapping.is_private(field.name);

        switch (@typeInfo(field.type)) {
            .array => try buffer.writer().print(
                \\    {s} {s}Data {s};
                \\
                \\
            ,
                .{
                    if (mapping.visibility == .internal and !is_private) "public" else "private",
                    to_case(field.name, .pascal),
                    to_case(field.name, .camel),
                },
            ),
            else => try buffer.writer().print(
                \\    {s} {s} {s};
                \\
                \\
            ,
                .{
                    if (mapping.visibility == .internal and !is_private) "public" else "private",
                    dotnet_type(field.type),
                    to_case(field.name, .camel),
                },
            ),
        }
    }

    if (mapping.visibility == .public) {

        // Properties
        inline for (type_info.fields) |field| {
            try emit_docs(buffer, mapping, field.name);

            const is_private = comptime mapping.is_private(field.name);
            const is_read_only = comptime mapping.is_read_only(field.name);

            switch (@typeInfo(field.type)) {
                .array => try buffer.writer().print(
                    \\    {s} byte[] {s} {{ get => {s}.GetData(); {s}set => {s}.SetData(value); }}
                    \\
                    \\
                , .{
                    if (is_private) "internal" else "public",
                    to_case(field.name, .pascal),
                    to_case(field.name, .camel),
                    if (is_read_only and !is_private) "internal " else "",
                    to_case(field.name, .camel),
                }),
                else => try buffer.writer().print(
                    \\    {s} {s} {s} {{ get => {s}; {s}set => {s} = value; }}
                    \\
                    \\
                , .{
                    if (is_private) "internal" else "public",
                    dotnet_type(field.type),
                    to_case(field.name, .pascal),
                    to_case(field.name, .camel),
                    if (is_read_only and !is_private) "internal " else "",
                    to_case(field.name, .camel),
                }),
            }
        }
    }

    try buffer.writer().print(
        \\}}
        \\
        \\
    , .{});
}

fn emit_docs(buffer: anytype, comptime mapping: TypeMapping, comptime field: ?[]const u8) !void {
    if (mapping.docs_link) |docs_link| {
        try buffer.writer().print(
            \\    /// <summary>
            \\    /// https://docs.tigerbeetle.com/{s}{s}
            \\    /// </summary>
            \\
        , .{
            docs_link,
            field orelse "",
        });
    }
}

pub fn generate_bindings(buffer: *std.ArrayList(u8)) !void {
    @setEvalBranchQuota(100_000);

    try buffer.writer().print(
        \\//////////////////////////////////////////////////////////
        \\// This file was auto-generated by dotnet_bindings.zig  //
        \\//              Do not manually modify.                 //
        \\//////////////////////////////////////////////////////////
        \\
        \\using System;
        \\using System.Runtime.InteropServices;
        \\
        \\namespace TigerBeetle;
        \\
        \\
    , .{});

    // Emit C# declarations.
    inline for (type_mappings) |type_mapping| {
        const ZigType = type_mapping[0];
        const mapping = type_mapping[1];

        switch (@typeInfo(ZigType)) {
            .@"struct" => |info| switch (info.layout) {
                .auto => @compileError(
                    "Only packed or extern structs are supported: " ++ @typeName(ZigType),
                ),
                .@"packed" => try emit_enum(
                    buffer,
                    ZigType,
                    info,
                    mapping,
                    comptime dotnet_type(
                        std.meta.Int(.unsigned, @bitSizeOf(ZigType)),
                    ),
                ),
                .@"extern" => try emit_struct(
                    buffer,
                    info,
                    mapping,
                    @sizeOf(ZigType),
                ),
            },
            .@"enum" => |info| try emit_enum(
                buffer,
                ZigType,
                info,
                mapping,
                comptime dotnet_type(std.meta.Int(.unsigned, @bitSizeOf(ZigType))),
            ),
            else => @compileError("Type cannot be represented: " ++ @typeName(ZigType)),
        }
    }

    // Emit function declarations.
    // TODO: use `std.meta.declaractions` and generate with pub + export functions.
    // Zig 0.9.1 has `decl.data.Fn.arg_names` but it's currently/incorrectly a zero-sized slice.
    try buffer.writer().print(
        \\internal static class Native
        \\{{
        \\    private const string LIB_NAME = "tb_client";
        \\
        \\    [DllImport(LIB_NAME, CallingConvention = CallingConvention.Cdecl)]
        \\    public static unsafe extern InitializationStatus tb_client_init(
        \\        TBClient* client_out,
        \\        UInt128Extensions.UnsafeU128* cluster_id,
        \\        byte* address_ptr,
        \\        uint address_len,
        \\        IntPtr completion_ctx,
        \\        delegate* unmanaged[Cdecl]<IntPtr,
        \\                                   TBPacket*, ulong,
        \\                                   byte*, uint, void> completion_callback
        \\    );
        \\
        \\    [DllImport(LIB_NAME, CallingConvention = CallingConvention.Cdecl)]
        \\    public static unsafe extern InitializationStatus tb_client_init_echo(
        \\        TBClient* out_client,
        \\        UInt128Extensions.UnsafeU128* cluster_id,
        \\        byte* address_ptr,
        \\        uint address_len,
        \\        IntPtr completion_ctx,
        \\        delegate* unmanaged[Cdecl]<IntPtr,
        \\                                   TBPacket*, ulong,
        \\                                   byte*, uint, void> completion_callback
        \\    );
        \\
        \\    [DllImport(LIB_NAME, CallingConvention = CallingConvention.Cdecl)]
        \\    public static unsafe extern ClientStatus tb_client_submit(
        \\        TBClient* client,
        \\        TBPacket* packet
        \\    );
        \\
        \\    [DllImport(LIB_NAME, CallingConvention = CallingConvention.Cdecl)]
        \\    public static unsafe extern ClientStatus tb_client_deinit(
        \\        TBClient* client
        \\    );
        \\}}
        \\
        \\
    , .{});
}

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var buffer = std.ArrayList(u8).init(allocator);
    try generate_bindings(&buffer);

    try std.io.getStdOut().writeAll(buffer.items);
}
