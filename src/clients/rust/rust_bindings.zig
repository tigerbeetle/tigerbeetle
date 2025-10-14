const std = @import("std");
const vsr = @import("vsr");
const exports = vsr.tb_client.exports;
const assert = std.debug.assert;

const type_mappings = .{
    .{ exports.tb_account_flags, "TB_ACCOUNT_FLAGS" },
    .{ exports.tb_account_t, "tb_account_t" },
    .{ exports.tb_transfer_flags, "TB_TRANSFER_FLAGS" },
    .{ exports.tb_transfer_t, "tb_transfer_t" },
    .{ exports.tb_create_account_result, "TB_CREATE_ACCOUNT_RESULT" },
    .{ exports.tb_create_transfer_result, "TB_CREATE_TRANSFER_RESULT" },
    .{ exports.tb_create_accounts_result_t, "tb_create_accounts_result_t" },
    .{ exports.tb_create_transfers_result_t, "tb_create_transfers_result_t" },
    .{ exports.tb_account_filter_t, "tb_account_filter_t" },
    .{ exports.tb_account_filter_flags, "TB_ACCOUNT_FILTER_FLAGS" },
    .{ exports.tb_account_balance_t, "tb_account_balance_t" },
    .{ exports.tb_query_filter_t, "tb_query_filter_t" },
    .{ exports.tb_query_filter_flags, "TB_QUERY_FILTER_FLAGS" },
    .{
        exports.tb_client_t, "tb_client_t",
        \\// Opaque struct serving as a handle for the client instance.
        \\// This struct must be "pinned" (not copyable or movable), as its address must remain stable
        \\// throughout the lifetime of the client instance.
    },
    .{
        exports.tb_packet_t, "tb_packet_t",
        \\// Struct containing the state of a request submitted through the client.
        \\// This struct must be "pinned" (not copyable or movable), as its address must remain stable
        \\// throughout the lifetime of the request.
    },
    .{ exports.tb_operation, "TB_OPERATION" },
    .{ exports.tb_packet_status, "TB_PACKET_STATUS" },
    .{ exports.tb_init_status, "TB_INIT_STATUS" },
    .{ exports.tb_client_status, "TB_CLIENT_STATUS" },
    .{ exports.tb_register_log_callback_status, "TB_REGISTER_LOG_CALLBACK_STATUS" },
    .{ exports.tb_log_level, "TB_LOG_LEVEL" },
};

fn resolve_rust_type(comptime Type: type) []const u8 {
    switch (@typeInfo(Type)) {
        .array => |info| return resolve_rust_type(info.child),
        .@"enum" => |info| return resolve_rust_type(info.tag_type),
        .@"struct" => return resolve_rust_type(std.meta.Int(.unsigned, @bitSizeOf(Type))),
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
                const ZigType = type_mapping[0];
                const c_name = type_mapping[1];

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

fn to_uppercase(comptime input: []const u8) [input.len]u8 {
    comptime var output: [input.len]u8 = undefined;
    inline for (&output, 0..) |*char, i| {
        char.* = input[i];
        char.* -= 32 * @as(u8, @intFromBool(char.* >= 'a' and char.* <= 'z'));
    }
    return output;
}

fn emit_enum(
    buffer: *std.ArrayList(u8),
    comptime Type: type,
    comptime type_info: anytype,
    comptime rust_name: []const u8,
    comptime skip_fields: []const []const u8,
) !void {
    var suffix_pos = std.mem.lastIndexOf(u8, rust_name, "_").?;
    if (std.mem.count(u8, rust_name, "_") == 1) suffix_pos = rust_name.len;

    const backing_type = switch (@typeInfo(Type)) {
        .@"struct" => |s| s.backing_integer.?,
        .@"enum" => |e| e.tag_type,
        else => @panic("unexpected"),
    };
    const rust_backing_type_str = switch (@typeInfo(backing_type)) {
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

    try buffer.writer().print("pub type {s} = {s};\n", .{ rust_name, rust_backing_type_str });

    inline for (type_info.fields, 0..) |field, i| {
        if (comptime std.mem.startsWith(u8, field.name, "deprecated_")) continue;
        comptime var skip = false;
        inline for (skip_fields) |sf| {
            skip = skip or comptime std.mem.eql(u8, sf, field.name);
        }

        if (!skip) {
            const field_name = to_uppercase(field.name);
            if (@typeInfo(Type) == .@"enum") {
                try buffer.writer().print("pub const {s}_{s}_{s}: {s} = {};\n", .{
                    rust_name,
                    rust_name[0..suffix_pos],
                    @as([]const u8, &field_name),
                    rust_name,
                    @intFromEnum(@field(Type, field.name)),
                });
            } else {
                // Packed structs.
                try buffer.writer().print("pub const {s}_{s}_{s}: {s} = 1 << {};\n", .{
                    rust_name,
                    rust_name[0..suffix_pos],
                    @as([]const u8, &field_name),
                    rust_name,
                    i,
                });
            }
        }
    }

    try buffer.writer().print("\n", .{});
}

fn emit_struct(
    buffer: *std.ArrayList(u8),
    comptime type_info: anytype,
    comptime rust_name: []const u8,
) !void {
    try buffer.writer().print("#[repr(C)]\n", .{});
    try buffer.writer().print("#[derive(Debug, Copy, Clone)]\n", .{});
    try buffer.writer().print("pub struct {s} {{\n", .{rust_name});

    inline for (type_info.fields) |field| {
        switch (@typeInfo(field.type)) {
            .array => |array| {
                try buffer.writer().print("    pub {s}: [{s}; {}]", .{
                    field.name,
                    resolve_rust_type(field.type),
                    array.len,
                });
            },
            else => {
                try buffer.writer().print("    pub {s}: {s}", .{
                    field.name,
                    resolve_rust_type(field.type),
                });
            },
        }

        try buffer.writer().print(",\n", .{});
    }

    try buffer.writer().print("}}\n\n", .{});
}

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var buffer = std.ArrayList(u8).init(allocator);
    try buffer.writer().print(
        \\ //////////////////////////////////////////////////////////
        \\ // This file was auto-generated by tb_client_header.zig //
        \\ //              Do not manually modify.                 //
        \\ //////////////////////////////////////////////////////////
        \\
        \\
    , .{});

    inline for (type_mappings) |type_mapping| {
        const ZigType = type_mapping[0];
        const rust_name = type_mapping[1];
        if (type_mapping.len == 3) {
            const comments: []const u8 = type_mapping[2];
            try buffer.writer().print(comments, .{});
            try buffer.writer().print("\n", .{});
        }

        switch (@typeInfo(ZigType)) {
            .@"struct" => |info| switch (info.layout) {
                .auto => @compileError("Invalid C struct type: " ++ @typeName(ZigType)),
                .@"packed" => try emit_enum(&buffer, ZigType, info, rust_name, &.{"padding"}),
                .@"extern" => try emit_struct(&buffer, info, rust_name),
            },
            .@"enum" => |info| {
                try emit_enum(&buffer, ZigType, info, rust_name, &.{});
            },
            else => try buffer.writer().print("pub type {s} = {s};\n\n", .{
                rust_name,
                resolve_rust_type(ZigType),
            }),
        }
    }

    try buffer.writer().print(
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
        \\    // Initialize a new TigerBeetle client which echos back any data submitted.
        \\    pub fn tb_client_init_echo(
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
        \\    // Retrieve the callback context initially passed into `tb_client_init` or
        \\    // `tb_client_init_echo`.
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
