const std = @import("std");
const tb = @import("../../tigerbeetle.zig");
const tb_client = @import("../c/tb_client.zig");

const TypeMapping = struct {
    name: []const u8,
    visibility: enum { public, internal },
    private_fields: []const []const u8 = &.{},
    readonly_fields: []const []const u8 = &.{},
    docs_link: ?[]const u8 = null,

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
        .docs_link = "reference/accounts#flags",
    } },
    .{ tb.TransferFlags, TypeMapping{
        .name = "TransferFlags",
        .visibility = .public,
        .private_fields = &.{"padding"},
        .docs_link = "reference/transfers#flags",
    } },
    .{ tb.Account, TypeMapping{
        .name = "Account",
        .visibility = .public,
        .private_fields = &.{"reserved"},
        .readonly_fields = &.{ "debits_pending", "credits_pending", "debits_posted", "credits_posted", "timestamp" },
        .docs_link = "reference/accounts/#",
    } },
    .{ tb.Transfer, TypeMapping{
        .name = "Transfer",
        .visibility = .public,
        .private_fields = &.{"reserved"},
        .readonly_fields = &.{"timestamp"},
        .docs_link = "reference/transfers/#",
    } },
    .{ tb.CreateAccountResult, TypeMapping{
        .name = "CreateAccountResult",
        .visibility = .public,
        .docs_link = "reference/operations/create_accounts#",
    } },
    .{ tb.CreateTransferResult, TypeMapping{
        .name = "CreateTransferResult",
        .visibility = .public,
        .docs_link = "reference/operations/create_transfers#",
    } },
    .{ tb.CreateAccountsResult, TypeMapping{
        .name = "CreateAccountsResult",
        .visibility = .public,
    } },
    .{ tb.CreateTransfersResult, TypeMapping{
        .name = "CreateTransfersResult",
        .visibility = .public,
    } },
    .{ tb_client.tb_status_t, TypeMapping{
        .name = "InitializationStatus",
        .visibility = .public,
    } },
    .{ tb_client.tb_packet_status_t, TypeMapping{
        .name = "PacketStatus",
        .visibility = .public,
    } },
    .{ tb_client.tb_operation_t, TypeMapping{
        .name = "TBOperation",
        .visibility = .internal,
        .private_fields = &.{ "reserved", "root", "register" },
    } },
    .{ tb_client.tb_packet_t, TypeMapping{
        .name = "TBPacket",
        .visibility = .internal,
    } },
    .{ tb_client.tb_packet_list_t, TypeMapping{
        .name = "TBPacketList",
        .visibility = .internal,
    } },
};

fn dotnet_type(comptime Type: type) []const u8 {
    switch (@typeInfo(Type)) {
        .Enum, .Struct => return comptime get_mapped_type_name(Type) orelse @compileError("Type " ++ @typeName(Type) ++ " not mapped."),
        .Int => |info| {
            std.debug.assert(info.signedness == .unsigned);
            return switch (info.bits) {
                8 => "byte",
                16 => "ushort",
                32 => "uint",
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
            return type_mapping[1].name;
        }
    } else return null;
}

fn to_case(comptime input: []const u8, comptime case: enum { camel, pascal }) []const u8 {
    comptime {
        var len: usize = 0;
        var output: [input.len]u8 = undefined;
        var iterator = std.mem.tokenize(u8, input, "_");
        while (iterator.next()) |word| {
            _ = std.ascii.lowerString(output[len..], word);
            output[len] = std.ascii.toUpper(output[len]);
            len += word.len;
        }

        output[0] = switch (case) {
            .camel => std.ascii.toLower(output[0]),
            .pascal => std.ascii.toUpper(output[0]),
        };

        return output[0..len];
    }
}

fn emit_enum(
    buffer: *std.ArrayList(u8),
    comptime type_info: anytype,
    comptime mapping: TypeMapping,
    comptime int_type: []const u8,
    comptime value_fmt: []const u8,
) !void {
    const is_packed_struct = @TypeOf(type_info) == std.builtin.TypeInfo.Struct;
    if (is_packed_struct) {
        // Packed structs represented as Enum needs a Flags attribute:
        try buffer.writer().print("    [Flags]\n", .{});
    }

    try buffer.writer().print(
        \\    {s} enum {s} : {s}
        \\    {{
        \\
    , .{
        @tagName(mapping.visibility),
        mapping.name,
        int_type,
    });

    if (is_packed_struct) {
        // Packed structs represented as Enum needs a ZERO value:
        try buffer.writer().print(
            \\        None = 0,
            \\
            \\
        , .{});
    }

    inline for (type_info.fields) |field, i| {
        if (comptime mapping.is_private(field.name)) continue;

        try emit_docs(buffer, mapping, field.name);

        try buffer.writer().print(
            \\        {s} = 
        ++ value_fmt ++ ",\n\n", .{
            to_case(field.name, .pascal),
            i,
        });
    }

    try buffer.writer().print(
        \\    }}
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
        \\    [StructLayout(LayoutKind.Sequential, Size = SIZE)]
        \\    {s} {s}struct {s}
        \\    {{
        \\        public const int SIZE = {};
        \\
        \\
    , .{
        @tagName(mapping.visibility),
        if (mapping.visibility == .internal) "unsafe " else "",
        mapping.name,
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
                    \\                if (value.Length != SIZE) throw new ArgumentException("Expected a byte[" + SIZE + "] array", nameof(value));
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
                    if (mapping.visibility == .internal) "public" else "private",
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
                    if (mapping.visibility == .internal) "public" else "private",
                    dotnet_type(field.field_type),
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

            switch (@typeInfo(field.field_type)) {
                .Array => try buffer.writer().print(
                    \\        {s} byte[] {s} {{ get => {s}.GetData(); {s}set => {s}.SetData(value); }}
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
                    \\        {s} {s} {s} {{ get => {s}; {s}set => {s} = value; }}
                    \\
                    \\
                , .{
                    if (is_private) "internal" else "public",
                    dotnet_type(field.field_type),
                    to_case(field.name, .pascal),
                    to_case(field.name, .camel),
                    if (is_read_only and !is_private) "internal " else "",
                    to_case(field.name, .camel),
                }),
            }
        }
    }

    try buffer.writer().print(
        \\    }}
        \\
        \\
    , .{});
}

fn emit_docs(buffer: anytype, comptime mapping: TypeMapping, comptime field: ?[]const u8) !void {
    if (mapping.docs_link) |docs_link| {
        try buffer.writer().print(
            \\        /// <summary>
            \\        /// https://docs.tigerbeetle.com/{s}{s}
            \\        /// </summary>
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
        \\namespace TigerBeetle
        \\{{
        \\
    , .{});

    // Emit C# declarations.
    inline for (type_mappings) |type_mapping| {
        const ZigType = type_mapping[0];
        const mapping = type_mapping[1];

        switch (@typeInfo(ZigType)) {
            .Struct => |info| switch (info.layout) {
                .Auto => @compileError("Only packed or extern structs are supported: " ++ @typeName(ZigType)),
                .Packed => try emit_enum(
                    buffer,
                    info,
                    mapping,
                    comptime dotnet_type(std.meta.Int(.unsigned, @bitSizeOf(ZigType))),
                    "1 << {d}",
                ),
                .Extern => try emit_struct(
                    buffer,
                    info,
                    mapping,
                    @sizeOf(ZigType),
                ),
            },
            .Enum => |info| try emit_enum(
                buffer,
                info,
                mapping,
                comptime dotnet_type(std.meta.Int(.unsigned, @bitSizeOf(ZigType))),
                "{d}",
            ),
            else => @compileError("Type cannot be represented: " ++ @typeName(ZigType)),
        }
    }

    // Emit function declarations.
    // TODO: use `std.meta.declaractions` and generate with pub + export functions.
    // Zig 0.9.1 has `decl.data.Fn.arg_names` but it's currently/incorrectly a zero-sized slice.
    try buffer.writer().print(
        \\    internal static class TBClient
        \\    {{
        \\        private const string LIB_NAME = "tb_client";
        \\
        \\        // Uses either the new function pointer by value, or the old managed delegate in .Net standard
        \\        // https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/proposals/csharp-9.0/function-pointers
        \\
        \\#if NETSTANDARD
        \\        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        \\        public unsafe delegate void OnCompletionFn(IntPtr ctx, IntPtr client, TBPacket* packet, byte* result, uint result_len);
        \\#endif
        \\
        \\        [DllImport(LIB_NAME, CallingConvention = CallingConvention.Cdecl)]
        \\        public static unsafe extern InitializationStatus tb_client_init(
        \\            IntPtr* out_client,
        \\            TBPacketList* out_packets,
        \\            uint cluster_id,
        \\            byte* address_ptr,
        \\            uint address_len,
        \\            uint num_packets,
        \\            IntPtr on_completion_ctx,
        \\
        \\#if NETSTANDARD
        \\            [MarshalAs(UnmanagedType.FunctionPtr)]
        \\            OnCompletionFn on_completion_fn
        \\#else
        \\            delegate* unmanaged[Cdecl]<IntPtr, IntPtr, TBPacket*, byte*, uint, void> on_completion_fn
        \\#endif
        \\        );
        \\
        \\        [DllImport(LIB_NAME, CallingConvention = CallingConvention.Cdecl)]
        \\        public static unsafe extern InitializationStatus tb_client_init_echo(
        \\            IntPtr* out_client,
        \\            TBPacketList* out_packets,
        \\            uint cluster_id,
        \\            byte* address_ptr,
        \\            uint address_len,
        \\            uint num_packets,
        \\            IntPtr on_completion_ctx,
        \\
        \\#if NETSTANDARD
        \\            [MarshalAs(UnmanagedType.FunctionPtr)]
        \\            OnCompletionFn on_completion_fn
        \\#else
        \\            delegate* unmanaged[Cdecl]<IntPtr, IntPtr, TBPacket*, byte*, uint, void> on_completion_fn
        \\#endif
        \\        );
        \\
        \\        [DllImport(LIB_NAME, CallingConvention = CallingConvention.Cdecl)]
        \\        public static unsafe extern void tb_client_submit(
        \\            IntPtr client,
        \\            TBPacketList* packets
        \\        );
        \\
        \\        [DllImport(LIB_NAME, CallingConvention = CallingConvention.Cdecl)]
        \\        public static unsafe extern void tb_client_deinit(
        \\            IntPtr client
        \\        );
        \\    }}
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

    try std.fs.cwd().writeFile("src/clients/dotnet/TigerBeetle/Bindings.cs", buffer.items);
}
