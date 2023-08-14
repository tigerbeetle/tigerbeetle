const std = @import("std");
const assert = std.debug.assert;

const tb = @import("../../tigerbeetle.zig");
const tb_client = @import("tb_client.zig");
const c = @cImport(@cInclude("tb_client.h"));

fn to_lowercase(comptime input: []const u8) []const u8 {
    comptime var lowercase: [input.len]u8 = undefined;
    inline for (input, 0..) |char, i| {
        const is_uppercase = (char >= 'A') and (char <= 'Z');
        lowercase[i] = char + (@as(u8, @intFromBool(is_uppercase)) * 32);
    }
    return &lowercase;
}

fn to_uppercase(comptime input: []const u8) []const u8 {
    comptime var uppercase: [input.len]u8 = undefined;
    inline for (input, 0..) |char, i| {
        const is_lowercase = (char >= 'a') and (char <= 'z');
        uppercase[i] = char - (@as(u8, @intFromBool(is_lowercase)) * 32);
    }
    return &uppercase;
}

fn to_snakecase(comptime input: []const u8) []const u8 {
    comptime var output: []const u8 = &.{};
    inline for (input, 0..) |char, i| {
        const is_uppercase = (char >= 'A') and (char <= 'Z');
        if (is_uppercase and i > 0) output = "_" ++ output;
        output = output ++ &[_]u8{char};
    }
    return output;
}

test "valid tb_client.h" {
    @setEvalBranchQuota(20_000);

    inline for (.{
        .{ tb.Account, "tb_account_t" },
        .{ tb.Transfer, "tb_transfer_t" },
        .{ tb.AccountFlags, "TB_ACCOUNT_FLAGS" },
        .{ tb.TransferFlags, "TB_TRANSFER_FLAGS" },
        .{ tb.CreateAccountResult, "TB_CREATE_ACCOUNT_RESULT" },
        .{ tb.CreateTransferResult, "TB_CREATE_TRANSFER_RESULT" },
        .{ tb.CreateAccountsResult, "tb_create_accounts_result_t" },
        .{ tb.CreateTransfersResult, "tb_create_transfers_result_t" },

        .{ u128, "tb_uint128_t" },
        .{ tb_client.tb_status_t, "TB_STATUS" },
        .{ tb_client.tb_client_t, "tb_client_t" },
        .{ tb_client.tb_packet_t, "tb_packet_t" },
        .{ tb_client.tb_packet_status_t, "TB_PACKET_STATUS" },
    }) |c_export| {
        const ty: type = c_export[0];
        const c_type_name = @as([]const u8, c_export[1]);
        const c_type: type = @field(c, c_type_name);

        switch (@typeInfo(ty)) {
            .Int => comptime assert(ty == c_type),
            .Pointer => comptime assert(@sizeOf(ty) == @sizeOf(c_type)),
            .Enum => {
                const prefix_offset = comptime std.mem.lastIndexOf(u8, c_type_name, "_").?;
                comptime var c_enum_prefix: []const u8 = c_type_name[0 .. prefix_offset + 1];
                comptime assert(c_type == c_uint);

                // TB_STATUS is a special case in naming
                if (comptime std.mem.eql(u8, c_type_name, "TB_STATUS")) {
                    c_enum_prefix = c_type_name ++ "_";
                }

                // Compare the enum int values in C to the enum int values in Zig.
                inline for (std.meta.fields(ty)) |field| {
                    const c_enum_field = comptime to_uppercase(to_snakecase(field.name));
                    const c_value = @field(c, c_enum_prefix ++ c_enum_field);

                    const zig_value = @intFromEnum(@field(ty, field.name));
                    comptime assert(zig_value == c_value);
                }
            },
            .Struct => |type_info| switch (type_info.layout) {
                .Auto => @compileError("struct must be extern or packed to be used in C"),
                .Packed => {
                    const prefix_offset = comptime std.mem.lastIndexOf(u8, c_type_name, "_").?;
                    const c_enum_prefix = c_type_name[0 .. prefix_offset + 1];
                    comptime assert(c_type == c_uint);

                    inline for (std.meta.fields(ty)) |field| {
                        if (comptime !std.mem.eql(u8, field.name, "padding")) {
                            // Get the bit value in the C enum.
                            const c_enum_field = comptime to_uppercase(to_snakecase(field.name));
                            const c_value = @field(c, c_enum_prefix ++ c_enum_field);

                            // Compare the bit value to the packed struct's field.
                            comptime var instance = std.mem.zeroes(ty);
                            @field(instance, field.name) = true;
                            comptime assert(@as(u16, @bitCast(instance)) == c_value);
                        }
                    }
                },
                .Extern => {
                    // Ensure structs are effectively the same.
                    comptime assert(@sizeOf(ty) == @sizeOf(c_type));
                    comptime assert(@alignOf(ty) == @alignOf(c_type));

                    inline for (std.meta.fields(ty)) |field| {
                        // In C, packed structs and enums are replaced with integers.
                        comptime var field_type = field.type;
                        switch (@typeInfo(field_type)) {
                            .Struct => |info| {
                                comptime assert(info.layout == .Packed);
                                comptime assert(@sizeOf(field_type) <= @sizeOf(u128));
                                field_type = std.meta.Int(.unsigned, @bitSizeOf(field_type));
                            },
                            .Enum => |info| field_type = info.tag_type,
                            else => {},
                        }

                        // In C, pointers are opaque so we compare only the field sizes,
                        comptime var c_field_type = @TypeOf(@field(@as(c_type, undefined), field.name));
                        switch (@typeInfo(c_field_type)) {
                            .Pointer => |info| {
                                comptime assert(info.size == .C);
                                comptime assert(@sizeOf(c_field_type) == @sizeOf(field_type));
                            },
                            else => comptime assert(c_field_type == field_type),
                        }
                    }
                },
            },
            else => |i| @compileLog("TODO", i),
        }
    }
}
