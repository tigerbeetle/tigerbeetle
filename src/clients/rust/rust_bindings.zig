const std = @import("std");
const assert = std.debug.assert;

const vsr = @import("vsr");
const exports = vsr.tb_client.exports;
const stdx = vsr.stdx;

const TypeMapping = struct {
    source: type,
    target: enum {
        auto, // auto-detect based on zig type
        enum_manual,
        struct_with_default,
    } = .auto,
    name: []const u8,
    comment: ?[]const u8 = null,
};

const type_mappings = [_]TypeMapping{
    .{ .source = exports.tb_account_flags, .name = "AccountFlags" },
    .{ .source = exports.tb_account_t, .name = "tb_account_t" },
    .{ .source = exports.tb_transfer_flags, .name = "TransferFlags" },
    .{ .source = exports.tb_transfer_t, .name = "tb_transfer_t" },
    .{ .source = exports.tb_create_account_status, .name = "CreateAccountStatus" },
    .{ .source = exports.tb_create_transfer_status, .name = "CreateTransferStatus" },
    .{ .source = exports.tb_create_account_result_t, .name = "CreateAccountResult" },
    .{ .source = exports.tb_create_transfer_result_t, .name = "CreateTransferResult" },
    .{
        .source = exports.tb_account_filter_t,
        .name = "AccountFilter",
        .target = .struct_with_default,
    },
    .{ .source = exports.tb_account_filter_flags, .name = "AccountFilterFlags" },
    .{ .source = exports.tb_account_balance_t, .name = "tb_account_balance_t" },
    .{ .source = exports.tb_query_filter_t, .name = "QueryFilter", .target = .struct_with_default },
    .{ .source = exports.tb_query_filter_flags, .name = "QueryFilterFlags" },
    .{
        .source = exports.tb_client_t,
        .name = "tb_client_t",
        .comment =
        \\// Opaque struct serving as a handle for the client instance.
        \\// This struct must be "pinned" (not copyable or movable), as its address 
        \\// must remain stable throughout the lifetime of the client instance.
        ,
    },
    .{
        .source = exports.tb_packet_t,
        .name = "tb_packet_t",
        .comment =
        \\// Struct containing the state of a request submitted through the client.
        \\// This struct must be "pinned" (not copyable or movable), as its address 
        \\// must remain stable throughout the lifetime of the request.
        ,
    },
    .{ .source = exports.tb_operation, .name = "TB_OPERATION", .target = .enum_manual },
    .{ .source = exports.tb_packet_status, .name = "TB_PACKET_STATUS", .target = .enum_manual },
    .{ .source = exports.tb_init_status, .name = "TB_INIT_STATUS", .target = .enum_manual },
    .{ .source = exports.tb_client_status, .name = "TB_CLIENT_STATUS", .target = .enum_manual },
    .{
        .source = exports.tb_register_log_callback_status,
        .name = "TB_REGISTER_LOG_CALLBACK_STATUS",
        .target = .enum_manual,
    },
    .{ .source = exports.tb_log_level, .name = "TB_LOG_LEVEL", .target = .enum_manual },
};

fn resolve_rust_type(comptime Type: type) []const u8 {
    switch (@typeInfo(Type)) {
        .array => |info| return resolve_rust_type(info.child),
        .@"enum" => |info| {
            inline for (type_mappings) |type_mapping| {
                if (Type == type_mapping.source) {
                    return type_mapping.name;
                }
            }

            return resolve_rust_type(info.tag_type);
        },
        .@"struct" => {
            inline for (type_mappings) |type_mapping| {
                if (Type == type_mapping.source) {
                    return type_mapping.name;
                }
            }

            return resolve_rust_type(std.meta.Int(.unsigned, @bitSizeOf(Type)));
        },
        .bool => return "u8", // todo "bool"
        .int => |info| {
            assert(info.signedness == .unsigned);
            return switch (info.bits) {
                8 => "u8",
                16 => "u16",
                32 => "u32",
                64 => "u64",
                128 => "u128",
                else => @compileError("invalid int type"),
            };
        },
        .optional => |info| switch (@typeInfo(info.child)) {
            .pointer => return resolve_rust_type(info.child),
            else => @compileError("Unsupported optional type: " ++ @typeName(Type)),
        },
        .pointer => |info| {
            assert(info.size != .slice);
            assert(!info.is_allowzero);

            inline for (type_mappings) |type_mapping| {
                const ZigType = type_mapping.source;
                const c_name = type_mapping.name;

                if (info.child == ZigType) {
                    return "*mut " ++ c_name;
                }
            }

            return comptime "*mut " ++ resolve_rust_type(info.child);
        },
        .void, .@"opaque" => return "::std::os::raw::c_void",
        else => @compileError("Unhandled type: " ++ @typeName(Type)),
    }
}

fn resolve_rust_backing_integer(comptime integer_type: type) []const u8 {
    return switch (@typeInfo(integer_type)) {
        .int => |i| brk: {
            break :brk switch (i.bits) {
                32 => switch (i.signedness) {
                    .unsigned => "u32",
                    .signed => "i32",
                },
                16 => "u16",
                8 => "u8",
                else => @panic("unexpected"),
            };
        },
        else => @panic("unexpected"),
    };
}

fn emit_bitflags(
    writer: anytype,
    comptime type_info: std.builtin.Type.Struct,
    comptime rust_name: []const u8,
    comptime skip_fields: []const []const u8,
) !void {
    assert(type_info.layout == .@"packed");
    assert(std.mem.count(u8, rust_name, "_") == 0);
    assert(rust_name[0] >= 'A' and rust_name[0] <= 'Z');

    const backing_type_text = resolve_rust_backing_integer(type_info.backing_integer.?);

    try writer.print(
        \\#[derive(Copy, Clone, Debug, Default)]
        \\#[derive(Eq, PartialEq, Ord, PartialOrd, Hash)]
        \\#[repr(transparent)]
        \\pub struct {[rust_name]s}(pub {[backing_type_text]s});
        \\
    , .{ .rust_name = rust_name, .backing_type_text = backing_type_text });

    try writer.print("impl {s} {{\n", .{rust_name});
    {
        inline for (type_info.fields, 0..) |field, bit_index| {
            if (comptime std.mem.startsWith(u8, field.name, "deprecated_")) continue;
            comptime var skip = false;
            inline for (skip_fields) |sf| {
                skip = skip or comptime std.mem.eql(u8, sf, field.name);
            }
            if (skip) continue;

            assert(field.type == bool);
            const field_name = stdx.to_case(field.name, .PascalCase);
            try writer.print("    pub const {s}: {s} = {s}(1 << {});\n", .{
                field_name,
                rust_name,
                rust_name,
                bit_index,
            });
        }
        try writer.print("\n", .{});
        try writer.print("    pub fn empty() -> Self {{ {s}(0) }}\n", .{rust_name});
    }
    try writer.print("}}\n\n", .{});

    try writer.print(
        \\impl std::ops::BitOr for {[rust_name]s} {{
        \\    type Output = {[rust_name]s};
        \\    fn bitor(self, rhs: Self) -> Self::Output {{
        \\         Self(self.0 | rhs.0)
        \\    }}
        \\}}
        \\
        \\
    , .{ .rust_name = rust_name });
}

fn emit_enum_direct(
    writer: anytype,
    comptime Type: type,
    comptime type_info: std.builtin.Type.Enum,
    comptime rust_name: []const u8,
) !void {
    @setEvalBranchQuota(2000);
    const backing_type_text = resolve_rust_backing_integer(type_info.tag_type);

    try writer.print("#[repr({s})]\n", .{backing_type_text});
    try writer.print("#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]\n", .{});
    try writer.print("#[non_exhaustive]\n", .{});
    try writer.print("pub enum {s} {{\n", .{rust_name});
    inline for (type_info.fields) |field| {
        if (comptime std.mem.startsWith(u8, field.name, "deprecated_")) continue;
        const field_name = stdx.to_case(field.name, .PascalCase);
        const int_value = @intFromEnum(@field(Type, field.name));
        const int_fmt = if (int_value == std.math.maxInt(@TypeOf(int_value))) "0x{X}" else "{}";
        try writer.print("    {s} = " ++ int_fmt ++ ",\n", .{ field_name, int_value });
    }
    try writer.print("}}\n\n", .{}); // enum close

    { // convert from integer to enum
        try writer.print("impl From<{s}> for {s} {{\n", .{ backing_type_text, rust_name });
        try writer.print(
            \\    fn from(value: {s}) -> {s} {{
            \\        match value {{
            \\
        , .{ backing_type_text, rust_name });
        inline for (type_info.fields) |field| {
            if (comptime std.mem.startsWith(u8, field.name, "deprecated_")) continue;
            const field_name = stdx.to_case(field.name, .PascalCase);
            const int_value = @intFromEnum(@field(Type, field.name));
            const int_fmt = if (int_value == std.math.maxInt(@TypeOf(int_value))) "0x{X}" else "{}";
            try writer.print("            " ++ int_fmt ++ " => {s}::{s},\n", .{
                int_value,
                rust_name,
                field_name,
            });
        }
        try writer.print(
            \\            other_value => panic!("cannot convert {s} {{other_value}} to {s}")
            \\        }}
            \\    }}
            \\
        , .{ backing_type_text, rust_name });
        try writer.print("}}\n\n", .{}); // impl From<int> close
    }
    { // convert from enum to integer
        try writer.print(
            \\impl From<{[enum_type]s}> for {[int_type]s} {{
            \\    fn from(value: {[enum_type]s}) -> {[int_type]s} {{
            \\        value as {[int_type]s}
            \\    }}
            \\}}
            \\
            \\
        , .{ .enum_type = rust_name, .int_type = backing_type_text });
    }
    { // implement display
        try writer.print(
            \\ impl core::fmt::Display for {[enum_name]s} {{
            \\     fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {{
            \\         match self {{
            \\
        , .{ .enum_name = rust_name });
        inline for (type_info.fields) |field| {
            if (comptime std.mem.startsWith(u8, field.name, "deprecated_")) continue;
            const field_name = stdx.to_case(field.name, .PascalCase);
            try writer.print(
                "             Self::{[field_name]s} => f.write_str(\"{[field_name]s}\"),\n",
                .{ .field_name = field_name },
            );
        }
        try writer.print(
            \\        }}
            \\    }}
            \\}}
            \\
            \\
        , .{});
    }
}

fn emit_enum_manual(
    writer: anytype,
    comptime Type: type,
    comptime type_info: std.builtin.Type.Enum,
    comptime rust_name: []const u8,
) !void {
    var suffix_pos = std.mem.lastIndexOf(u8, rust_name, "_").?;
    if (std.mem.count(u8, rust_name, "_") == 1) suffix_pos = rust_name.len;

    const backing_type_text = resolve_rust_backing_integer(type_info.tag_type);

    try writer.print("pub type {s} = {s};\n", .{ rust_name, backing_type_text });

    inline for (type_info.fields) |field| {
        if (comptime std.mem.startsWith(u8, field.name, "deprecated_")) continue;

        const field_name = stdx.to_case(field.name, .UPPER_CASE);
        const int_value = @intFromEnum(@field(Type, field.name));
        try writer.print("pub const {s}_{s}_{s}: {s} = {s};\n", .{
            rust_name,
            rust_name[0..suffix_pos],
            field_name,
            rust_name,
            if (int_value == std.math.maxInt(@TypeOf(int_value)))
                std.fmt.comptimePrint("0x{X}", .{int_value})
            else
                std.fmt.comptimePrint("{}", .{int_value}),
        });
    }

    try writer.print("\n", .{});
}

fn emit_struct(
    writer: anytype,
    comptime type_info: std.builtin.Type.Struct,
    comptime rust_name: []const u8,
    options: struct {
        derive: []const u8,
    },
) !void {
    assert(type_info.layout == .@"extern");

    try writer.print("#[repr(C)]\n", .{});
    if (options.derive.len > 0) {
        try writer.print("#[derive({s})]\n", .{options.derive});
    }
    try writer.print("pub struct {s} {{\n", .{rust_name});

    inline for (type_info.fields) |field| {
        switch (@typeInfo(field.type)) {
            .array => |array| {
                if (std.mem.eql(u8, field.name, "reserved")) {
                    assert(array.child == u8);
                    try writer.print("    pub reserved: Reserved<{d}>", .{array.len});
                } else {
                    try writer.print("    pub {s}: [{s}; {}]", .{
                        field.name,
                        resolve_rust_type(field.type),
                        array.len,
                    });
                }
            },
            else => {
                try writer.print("    pub {s}: {s}", .{
                    field.name,
                    resolve_rust_type(field.type),
                });
            },
        }

        try writer.print(",\n", .{});
    }

    try writer.print("}}\n\n", .{});
}

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var buffer = std.ArrayList(u8).init(allocator);
    var writer = buffer.writer();
    try writer.print(
        \\ ///////////////////////////////////////////////////////
        \\ // This file was auto-generated by rust_bindings.zig //
        \\ //              Do not manually modify.              //
        \\ ///////////////////////////////////////////////////////
        \\
        \\
    , .{});

    inline for (type_mappings) |type_mapping| {
        if (type_mapping.comment) |comment| {
            try writer.print(comment, .{});
            try writer.print("\n", .{});
        }

        const rust_name = type_mapping.name;
        const type_info = @typeInfo(type_mapping.source);

        switch (type_mapping.target) {
            .enum_manual => {
                try emit_enum_manual(writer, type_mapping.source, type_info.@"enum", rust_name);
            },
            .struct_with_default => {
                try emit_struct(writer, type_info.@"struct", rust_name, .{
                    .derive = "Debug, Copy, Clone, Default",
                });
            },
            .auto => switch (type_info) {
                .@"struct" => |info| switch (info.layout) {
                    .auto => @compileError("Invalid C struct layout: " ++ info.layout),
                    .@"packed" => try emit_bitflags(writer, info, rust_name, &.{"padding"}),
                    .@"extern" => try emit_struct(writer, info, rust_name, .{
                        .derive = "Debug, Copy, Clone",
                    }),
                },
                .@"enum" => |info| {
                    try emit_enum_direct(writer, type_mapping.source, info, rust_name);
                },
                else => try writer.print("pub type {s} = {s};\n\n", .{
                    rust_name,
                    resolve_rust_type(type_mapping.source),
                }),
            },
        }
    }

    try writer.print(
        \\#[repr(transparent)]
        \\#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
        \\pub struct Reserved<const N: usize>([u8; N]);
        \\
        \\impl<const N: usize> Default for Reserved<N> {{
        \\    fn default() -> Reserved<N> {{
        \\        Reserved([0; N])
        \\    }}
        \\}}
        \\
        \\extern "C" {{
        \\    // Initialize a new TigerBeetle client which connects to the addresses provided and
        \\    // completes submitted packets by invoking the callback with the given context.
        \\    pub fn tb_client_init(
        \\        client_out: *mut tb_client_t,
        \\        // 128-bit unsigned integer represented as a 16-byte little-endian array.
        \\        cluster_id: *const [u8; 16],
        \\        address_ptr: *const ::std::os::raw::c_char,
        \\        address_len: u32,
        \\        completion_ctx: usize,
        \\        completion_callback: ::std::option::Option<
        \\            unsafe extern "C" fn(
        \\                arg1: usize,
        \\                arg3: *mut tb_packet_t,
        \\                arg4: u64,
        \\                arg5: *const u8,
        \\                arg6: u32,
        \\            ),
        \\        >,
        \\    ) -> TB_INIT_STATUS;
        \\
        \\    // Retrieve the callback context initially passed into `tb_client_init`.
        \\    pub fn tb_client_completion_context(
        \\        client: *mut tb_client_t,
        \\        completion_ctx_out: *mut usize,
        \\    ) -> TB_CLIENT_STATUS;
        \\
        \\    // Submit a packet with its operation, data, and data_size fields set.
        \\    // Once completed, `on_completion` will be invoked with `on_completion_ctx` and the given
        \\    // packet on the `tb_client` thread (separate from caller's thread).
        \\    pub fn tb_client_submit(
        \\        client: *mut tb_client_t,
        \\        packet: *mut tb_packet_t,
        \\    ) -> TB_CLIENT_STATUS;
        \\
        \\    // Closes the client, causing any previously submitted packets to be completed with
        \\    // `TB_PACKET_CLIENT_SHUTDOWN` before freeing any allocated client resources from init.
        \\    // It is undefined behavior to use any functions on the client once deinit is called.
        \\    pub fn tb_client_deinit(
        \\        client: *mut tb_client_t,
        \\    ) -> TB_CLIENT_STATUS;
        \\
        \\    // Registers or unregisters the application log callback.
        \\    pub fn register_log_callback(
        \\        callback: ::std::option::Option<
        \\            unsafe extern "C" fn(
        \\                TB_LOG_LEVEL,
        \\                *const u8,
        \\                u32,
        \\            ),
        \\        >,
        \\        debug: bool,
        \\    ) -> TB_REGISTER_LOG_CALLBACK_STATUS;
        \\}}
    , .{});

    try std.io.getStdOut().writeAll(buffer.items);
}
