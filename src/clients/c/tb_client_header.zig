const std = @import("std");
const assert = std.debug.assert;

const vsr = @import("vsr");
const exports = vsr.tb_client.exports;

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
    .{ exports.tb_init_parameters, "tb_init_parameters_t" },
};

fn resolve_c_type(comptime Type: type) []const u8 {
    switch (@typeInfo(Type)) {
        .array => |info| return resolve_c_type(info.child),
        .@"enum" => |info| return resolve_c_type(info.tag_type),
        .@"struct" => return resolve_c_type(std.meta.Int(.unsigned, @bitSizeOf(Type))),
        .bool => return "uint8_t",
        .int => |info| {
            assert(info.signedness == .unsigned);
            return switch (info.bits) {
                8 => "uint8_t",
                16 => "uint16_t",
                32 => "uint32_t",
                64 => "uint64_t",
                128 => "tb_uint128_t",
                else => @compileError("invalid int type"),
            };
        },
        .optional => |info| switch (@typeInfo(info.child)) {
            .pointer => return resolve_c_type(info.child),
            else => @compileError("Unsupported optional type: " ++ @typeName(Type)),
        },
        .pointer => |info| {
            assert(info.size != .slice);
            assert(!info.is_allowzero);

            inline for (type_mappings) |type_mapping| {
                const ZigType = type_mapping[0];
                const c_name = type_mapping[1];

                if (info.child == ZigType) {
                    const prefix = if (@typeInfo(ZigType) == .@"struct") "struct " else "";
                    return prefix ++ c_name ++ "*";
                }
            }

            return comptime resolve_c_type(info.child) ++ "*";
        },
        .void, .@"opaque" => return "void",
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
    comptime c_name: []const u8,
    comptime skip_fields: []const []const u8,
) !void {
    var suffix_pos = std.mem.lastIndexOfScalar(u8, c_name, '_').?;
    if (std.mem.count(u8, c_name, "_") == 1) suffix_pos = c_name.len;

    try buffer.writer().print("typedef enum {s} {{\n", .{c_name});

    inline for (type_info.fields, 0..) |field, i| {
        if (comptime std.mem.startsWith(u8, field.name, "deprecated_")) continue;
        comptime var skip = false;
        inline for (skip_fields) |sf| {
            skip = skip or comptime std.mem.eql(u8, sf, field.name);
        }

        if (!skip) {
            const field_name = to_uppercase(field.name);
            if (@typeInfo(Type) == .@"enum") {
                try buffer.writer().print("    {s}_{s} = {},\n", .{
                    c_name[0..suffix_pos],
                    @as([]const u8, &field_name),
                    @intFromEnum(@field(Type, field.name)),
                });
            } else {
                // Packed structs.
                try buffer.writer().print("    {s}_{s} = 1 << {},\n", .{
                    c_name[0..suffix_pos],
                    @as([]const u8, &field_name),
                    i,
                });
            }
        }
    }

    try buffer.writer().print("}} {s};\n\n", .{c_name});
}

fn emit_struct(
    buffer: *std.ArrayList(u8),
    comptime type_info: anytype,
    comptime c_name: []const u8,
) !void {
    try buffer.writer().print("typedef struct {s} {{\n", .{c_name});

    inline for (type_info.fields) |field| {
        try buffer.writer().print("    {s} {s}", .{
            resolve_c_type(field.type),
            field.name,
        });

        switch (@typeInfo(field.type)) {
            .array => |array| try buffer.writer().print("[{d}]", .{array.len}),
            else => {},
        }

        try buffer.writer().print(";\n", .{});
    }

    try buffer.writer().print("}} {s};\n\n", .{c_name});
}

pub fn main() !void {
    @setEvalBranchQuota(100_000);

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
        \\#ifndef TB_CLIENT_H
        \\#define TB_CLIENT_H
        \\
        \\#ifdef __cplusplus
        \\extern "C" {{
        \\#endif
        \\
        \\#include <stddef.h>
        \\#include <stdint.h>
        \\#include <stdbool.h>
        \\
        \\typedef __uint128_t tb_uint128_t;
        \\
        \\
    , .{});

    // Emit C type declarations.
    inline for (type_mappings) |type_mapping| {
        const ZigType = type_mapping[0];
        const c_name = type_mapping[1];
        if (type_mapping.len == 3) {
            const comments: []const u8 = type_mapping[2];
            try buffer.writer().print(comments, .{});
            try buffer.writer().print("\n", .{});
        }

        switch (@typeInfo(ZigType)) {
            .@"struct" => |info| switch (info.layout) {
                .auto => @compileError("Invalid C struct type: " ++ @typeName(ZigType)),
                .@"packed" => try emit_enum(&buffer, ZigType, info, c_name, &.{"padding"}),
                .@"extern" => try emit_struct(&buffer, info, c_name),
            },
            .@"enum" => |info| {
                comptime var skip: []const []const u8 = &.{};
                if (ZigType == exports.tb_operation) {
                    skip = &.{ "reserved", "root", "register" };
                }

                try emit_enum(&buffer, ZigType, info, c_name, skip);
            },
            else => try buffer.writer().print("typedef {s} {s}; \n\n", .{
                resolve_c_type(ZigType),
                c_name,
            }),
        }
    }

    // Emit C function declarations.
    // TODO: use `std.meta.declaractions` and generate with pub + export functions.
    // Zig 0.9.1 has `decl.data.Fn.arg_names` but it's currently/incorrectly a zero-sized slice.
    try buffer.writer().print(
        \\// Initialize a new TigerBeetle client which connects to the addresses provided and
        \\// completes submitted packets by invoking the callback with the given context.
        \\TB_INIT_STATUS tb_client_init(
        \\    tb_client_t *client_out,
        \\    // 128-bit unsigned integer represented as a 16-byte little-endian array.
        \\    const uint8_t cluster_id[16],
        \\    const char *address_ptr,
        \\    uint32_t address_len,
        \\    uintptr_t completion_ctx,
        \\    void (*completion_callback)(uintptr_t, tb_packet_t*, uint64_t, const uint8_t*, uint32_t)
        \\);
        \\
        \\// Initialize a new TigerBeetle client that echoes back any submitted data.
        \\TB_INIT_STATUS tb_client_init_echo(
        \\    tb_client_t *client_out,
        \\    // 128-bit unsigned integer represented as a 16-byte little-endian array.
        \\    const uint8_t cluster_id[16],
        \\    const char *address_ptr,
        \\    uint32_t address_len,
        \\    uintptr_t completion_ctx,
        \\    void (*completion_callback)(uintptr_t, tb_packet_t*, uint64_t, const uint8_t*, uint32_t)
        \\);
        \\
        \\// Retrieve the parameters initially passed to `tb_client_init` or `tb_client_init_echo`.
        \\// Return value: `TB_CLIENT_OK` on success, or `TB_CLIENT_INVALID` if the client handle was
        \\// not initialized or has already been closed.
        \\TB_CLIENT_STATUS tb_client_init_parameters(
        \\    tb_client_t* client,
        \\    tb_init_parameters_t* init_parameters_out
        \\);
        \\
        \\// Retrieve the callback context initially passed to `tb_client_init` or `tb_client_init_echo`.
        \\// Return value: `TB_CLIENT_OK` on success, or `TB_CLIENT_INVALID` if the client handle was
        \\// not initialized or has already been closed.
        \\TB_CLIENT_STATUS tb_client_completion_context(
        \\    tb_client_t* client,
        \\    uintptr_t* completion_ctx_out
        \\);
        \\
        \\// Submit a packet with its `operation`, `data`, and `data_size` fields set.
        \\// Once completed, `completion_callback` will be invoked with `completion_ctx`
        \\// and the given packet on the `tb_client` thread (separate from the caller's thread).
        \\// Return value: `TB_CLIENT_OK` on success, or `TB_CLIENT_INVALID` if the client handle was
        \\// not initialized or has already been closed.
        \\TB_CLIENT_STATUS tb_client_submit(
        \\    tb_client_t *client,
        \\    tb_packet_t *packet
        \\);
        \\
        \\// Closes the client, causing any previously submitted packets to be completed with
        \\// `TB_PACKET_CLIENT_SHUTDOWN` before freeing any allocated client resources from init.
        \\// Return value: `TB_CLIENT_OK` on success, or `TB_CLIENT_INVALID` if the client handle was
        \\// not initialized or has already been closed.
        \\TB_CLIENT_STATUS tb_client_deinit(
        \\    tb_client_t *client
        \\);
        \\
        \\// Registers or unregisters the application log callback.
        \\TB_REGISTER_LOG_CALLBACK_STATUS tb_client_register_log_callback(
        \\    void (*callback)(TB_LOG_LEVEL, const uint8_t*, uint32_t),
        \\    bool debug
        \\);
        \\
        \\
    , .{});

    try buffer.writer().print(
        \\#ifdef __cplusplus
        \\}} // extern "C"
        \\#endif
        \\
        \\#endif // TB_CLIENT_H
        \\
    , .{});

    try std.io.getStdOut().writeAll(buffer.items);
}
