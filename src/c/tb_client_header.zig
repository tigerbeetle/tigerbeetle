const std = @import("std");
const tb = @import("../tigerbeetle.zig");
const build_options = @import("build_options");

fn c_type_name(comptime ty: type) []const u8 {
    switch (@typeInfo(ty)) {
        .Array => |info| return c_type_name(info.child),
        .Enum => |info| return c_type_name(info.tag_type),
        .Struct => return c_type_name(std.meta.Int(.unsigned, @sizeOf(ty) * 8)),
        .Int => |info| {
            std.debug.assert(info.signedness == .unsigned);
            return switch (info.bits) {
                8 => "uint8_t",
                16 => "uint16_t",
                32 => "uint32_t",
                64 => "uint64_t",
                128 => "tb_uint128_t",
                else => @compileError("invalid int type"),
            };
        },
        else => |t| @compileLog(t),
    }
}

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const allocator = arena.allocator();

    var buffer = std.ArrayList(u8).init(allocator);
    try buffer.writer().print(
        \\#ifndef TB_CLIENT_C
        \\#define TB_CLIENT_C
        \\ 
        \\#include <stddef.h>
        \\#include <stdint.h>
        \\#include <stdbool.h>
        \\
        \\typedef __uint128_t tb_uint128_t;
        \\
        \\typedef enum TB_OPERATION {{
        \\    TB_OP_CREATE_ACCOUNTS = 3,
        \\    TB_OP_CREATE_TRANSFERS = 4,
        \\    TB_OP_LOOKUP_ACCOUNTS = 5,
        \\    TB_OP_LOOKUP_TRANSFERS = 6
        \\}} TB_OPERATION;
        \\
        \\typedef enum TB_PACKET_STATUS {{
        \\    TB_PACKET_OK,
        \\    TB_PACKET_TOO_MUCH_DATA,
        \\    TB_PACKET_INVALID_OPERATION,
        \\    TB_PACKET_INVALID_DATA_SIZE
        \\}} TB_PACKET_STATUS;
        \\
        \\typedef struct tb_packet_t {{
        \\    struct tb_packet_t* next;
        \\    void* user_data;
        \\    uint8_t operation;
        \\    uint8_t status;
        \\    uint32_t data_size;
        \\    void* data;
        \\}} tb_packet_t;
        \\
        \\typedef struct tb_packet_list_t {{
        \\    struct tb_packet_t* head;
        \\    struct tb_packet_t* tail;
        \\}} tb_packet_list_t;
        \\
        \\typedef void* tb_client_t;
        \\
        \\typedef enum TB_STATUS {{
        \\    TB_STATUS_SUCCESS = 0,
        \\    TB_STATUS_UNEXPECTED = 1,
        \\    TB_STATUS_OUT_OF_MEMORY = 2,
        \\    TB_STATUS_INVALID_ADDRESS = 3,
        \\    TB_STATUS_SYSTEM_RESOURCES = 4,
        \\    TB_STATUS_NETWORK_SUBSYSTEM = 5,
        \\}} TB_STATUS;
        \\
        \\TB_STATUS tb_client_init(
        \\    tb_client_t* out_client,
        \\    tb_packet_list_t* out_packets,
        \\    uint32_t cluster_id,
        \\    const char* address_ptr,
        \\    uint32_t address_len,
        \\    uint32_t num_packets,
        \\    uintptr_t on_completion_ctx,
        \\    void (*on_completion_fn)(uintptr_t, tb_client_t, tb_packet_t*, const uint8_t*, uint32_t)
        \\);
        \\
        \\void tb_client_submit(
        \\    tb_client_t client,
        \\    tb_packet_list_t* packets
        \\);
        \\
        \\void tb_client_deinit(
        \\    tb_client_t client
        \\);
        \\
        \\
    , .{});

    inline for (.{
        tb.Account,
        tb.Transfer,
        tb.AccountFlags,
        tb.TransferFlags,
        tb.CreateAccountsResult,
        tb.CreateTransfersResult,
        tb.CreateAccountResult,
        tb.CreateTransferResult,
    }) |ty| {
        var c_name_buf = std.ArrayList(u8).init(allocator);
        defer c_name_buf.deinit();

        try c_name_buf.writer().print("tb", .{});
        inline for (@typeName(ty)) |char| {
            const uppercase = char >= 'A' and char <= 'Z';
            if (uppercase) try c_name_buf.append('_');
            try c_name_buf.append(char + (@as(u8, @boolToInt(uppercase)) * 32));
        }

        try c_name_buf.writer().print("_t", .{});
        var c_name = c_name_buf.items;

        switch (@typeInfo(ty)) {
            .Struct => |info| switch (info.layout) {
                .Auto => @compileError("invalid struct type " ++ @typeName(ty)),
                .Packed => {
                    const suffix = "Flags";
                    comptime std.debug.assert(std.mem.endsWith(u8, @typeName(ty), suffix));

                    c_name = c_name[0..c_name.len - 2];
                    for (c_name) |*char| {
                        char.* -= 32 * @as(u8, @boolToInt(char.* != '_'));
                    }

                    try buffer.writer().print("typedef enum {s} {{\n", .{c_name});

                    inline for (info.fields) |field, i| {
                        if (comptime !std.mem.eql(u8, field.name, "padding")) {
                            var f_name_buf = std.ArrayList(u8).init(allocator);
                            defer f_name_buf.deinit();

                            try f_name_buf.appendSlice(c_name[0..c_name.len - suffix.len]);
                            try f_name_buf.appendSlice(field.name);

                            const f_name = f_name_buf.items;
                            for (f_name[c_name.len - suffix.len..]) |*char| {
                                char.* -= 32 * @as(u8, @boolToInt(char.* != '_'));
                            }

                            try buffer.writer().print("    {s} = 1 << {d},\n", .{f_name, i});
                        }
                    }

                    try buffer.writer().print("}} {s};\n\n", .{c_name});
                },
                .Extern => {
                    try buffer.writer().print("typedef struct {s} {{\n", .{c_name});

                    inline for (info.fields) |field| {
                        try buffer.writer().print("    {s} {s}", .{
                            c_type_name(field.field_type),
                            field.name,
                        });

                        switch (@typeInfo(field.field_type)) {
                            .Array => |array| try buffer.writer().print("[{d}]", .{array.len}),
                            else => {},
                        }

                        try buffer.writer().print(";\n", .{});
                    }

                    try buffer.writer().print("}} {s};\n\n", .{c_name});
                },
            },
            .Enum => |info| {
                const suffix = "Result";
                comptime std.debug.assert(std.mem.endsWith(u8, @typeName(ty), suffix));

                c_name = c_name[0..c_name.len - 2];
                for (c_name) |*char| {
                    char.* -= 32 * @as(u8, @boolToInt(char.* != '_'));
                }

                try buffer.writer().print("typedef enum {s} {{\n", .{c_name});

                inline for (info.fields) |field, i| {
                    if (comptime !std.mem.eql(u8, field.name, "ok")) {
                        var f_name_buf = std.ArrayList(u8).init(allocator);
                        defer f_name_buf.deinit();

                        try f_name_buf.appendSlice(c_name[0..c_name.len - suffix.len]);
                        try f_name_buf.appendSlice(field.name);

                        const f_name = f_name_buf.items;
                        for (f_name[c_name.len - suffix.len..]) |*char| {
                            char.* -= 32 * @as(u8, @boolToInt(char.* != '_'));
                        }

                        try buffer.writer().print("    {s} = {d},\n", .{f_name, i});
                    }
                }

                try buffer.writer().print("}} {s};\n\n", .{c_name});
            },
            else => @compileError("unsupported type"),
        }
    }

    try buffer.writer().print("#endif // TB_CLIENT_C\n", .{});
    const header_contents = buffer.items;

    const output_dir = build_options.output_dir;
    try std.fs.cwd().writeFile(output_dir ++ "/tb_client.h", header_contents);
}
