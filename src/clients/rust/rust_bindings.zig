// todo: don't double-prefix enum/packed struct variant names
// todo: dead code? `skip = &.{ "reserved", "root", "register" };`
// todo: investigate build script rebuilding on commit (.git directory changes?)
// todo: packed struct typedefs are not the right width in C generator, etc
// todo: use zig names for structs?

const std = @import("std");
const vsr = @import("vsr");
const tb = vsr.tigerbeetle;
const tb_client = vsr.tb_client;

const type_mappings = .{
    .{ tb.AccountFlags, "TB_ACCOUNT_FLAGS" },
    .{ tb.Account, "tb_account_t" },
    .{ tb.TransferFlags, "TB_TRANSFER_FLAGS" },
    .{ tb.Transfer, "tb_transfer_t" },
    .{ tb.CreateAccountResult, "TB_CREATE_ACCOUNT_RESULT" },
    .{ tb.CreateTransferResult, "TB_CREATE_TRANSFER_RESULT" },
    .{ tb.CreateAccountsResult, "tb_create_accounts_result_t" },
    .{ tb.CreateTransfersResult, "tb_create_transfers_result_t" },
    .{ tb.AccountFilter, "tb_account_filter_t" },
    .{ tb.AccountFilterFlags, "TB_ACCOUNT_FILTER_FLAGS" },
    .{ tb.AccountBalance, "tb_account_balance_t" },
    .{ tb.QueryFilter, "tb_query_filter_t" },
    .{ tb.QueryFilterFlags, "TB_QUERY_FILTER_FLAGS" },

    .{ tb_client.tb_operation_t, "TB_OPERATION" },
    .{ tb_client.tb_packet_status_t, "TB_PACKET_STATUS" },
    .{ tb_client.tb_packet_t, "tb_packet_t" },
    .{ tb_client.tb_client_t, "tb_client_t" },
    .{ tb_client.tb_status_t, "TB_STATUS" },
};

fn resolve_rust_type(comptime Type: type) []const u8 {
    switch (@typeInfo(Type)) {
        .Array => |info| return resolve_rust_type(info.child),
        .Enum => |info| return resolve_rust_type(info.tag_type),
        .Struct => return resolve_rust_type(std.meta.Int(.unsigned, @bitSizeOf(Type))),
        .Bool => return "u8", // todo "bool"
        .Int => |info | {
            std.debug.assert(info.signedness == .unsigned);
            return switch (info.bits) {
                8 => "u8",
                16 => "u16",
                32 => "u32",
                64 => "u64",
                128 => "u128",
                else => @compileError("invalid int type"),
            };
        },
        .Optional => |info| switch (@typeInfo(info.child)) {
            .Pointer => return resolve_rust_type(info.child),
            else => @compileError("Unsupported optional type: " ++ @typeName(Type)),
        },
        .Pointer => |info| {
            std.debug.assert(info.size != .Slice);
            std.debug.assert(!info.is_allowzero);

            inline for (type_mappings) |type_mapping| {
                const ZigType = type_mapping[0];
                const c_name = type_mapping[1];

                if (info.child == ZigType) {
                    return "*mut " ++ c_name;
                }
            }

            return comptime "*mut " ++ resolve_rust_type(info.child);
        },
        .Void, .Opaque => return "::std::os::raw::c_void",
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

    const backing_type = switch(@typeInfo(Type)) {
        .Struct => |s| s.backing_integer.?,
        .Enum => |e| e.tag_type,
        else => @panic("unexpected"),
    };
    const rust_backing_type_str = switch (@typeInfo(backing_type)) {
        .Int => |i| brk: {
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

    try buffer.writer().print("pub type {s} = {s};\n", .{rust_name, rust_backing_type_str});

    inline for (type_info.fields, 0..) |field, i| {
        comptime var skip = false;
        inline for (skip_fields) |sf| {
            skip = skip or comptime std.mem.eql(u8, sf, field.name);
        }

        if (!skip) {
            const field_name = to_uppercase(field.name);
            if (@typeInfo(Type) == .Enum) {
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
            .Array => |array| {
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

        switch (@typeInfo(ZigType)) {
            .Struct => |info| switch (info.layout) {
                .auto => @compileError("Invalid C struct type: " ++ @typeName(ZigType)),
                .@"packed" => try emit_enum(&buffer, ZigType, info, rust_name, &.{"padding"}),
                .@"extern" => try emit_struct(&buffer, info, rust_name),
            },
            .Enum => |info| {
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
        \\        out_client: *mut tb_client_t,
        \\        cluster_id: *const u8,
        \\        address_ptr: *const ::std::os::raw::c_char,
        \\        address_len: u32,
        \\        on_completion_ctx: usize,
        \\        on_completion: ::std::option::Option<
        \\            unsafe extern "C" fn(
        \\                arg1: usize,
        \\                arg2: tb_client_t,
        \\                arg3: *mut tb_packet_t,
        \\                arg4: u64,
        \\                arg5: *const u8,
        \\                arg6: u32,
        \\            ),
        \\        >,
        \\    ) -> TB_STATUS;
        \\
        \\    // Initialize a new TigerBeetle client which echos back any data submitted.
        \\    pub fn tb_client_init_echo(
        \\        out_client: *mut tb_client_t,
        \\        cluster_id: *const u8,
        \\        address_ptr: *const ::std::os::raw::c_char,
        \\        address_len: u32,
        \\        on_completion_ctx: usize,
        \\        on_completion: ::std::option::Option<
        \\            unsafe extern "C" fn(
        \\                arg1: usize,
        \\                arg2: tb_client_t,
        \\                arg3: *mut tb_packet_t,
        \\                arg4: u64,
        \\                arg5: *const u8,
        \\                arg6: u32,
        \\            ),
        \\        >,
        \\    ) -> TB_STATUS;
        \\
        \\    // Retrieve the callback context initially passed into `tb_client_init` or
        \\    // `tb_client_init_echo`.
        \\    pub fn tb_client_completion_context(
        \\        client: tb_client_t,
        \\    ) -> usize;
        \\
        \\    // Submit a packet with its operation, data, and data_size fields set.
        \\    // Once completed, `on_completion` will be invoked with `on_completion_ctx` and the given
        \\    // packet on the `tb_client` thread (separate from caller's thread).
        \\    pub fn tb_client_submit(
        \\        client: tb_client_t,
        \\        packet: *mut tb_packet_t,
        \\    );
        \\
        \\    // Closes the client, causing any previously submitted packets to be completed with
        \\    // `TB_PACKET_CLIENT_SHUTDOWN` before freeing any allocated client resources from init.
        \\    // It is undefined behavior to use any functions on the client once deinit is called.
        \\    pub fn tb_client_deinit(
        \\        client: tb_client_t,
        \\    );
        \\}}
    , .{});

    try std.io.getStdOut().writeAll(buffer.items);
}

