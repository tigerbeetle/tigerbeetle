const std = @import("std");
const assert = std.debug.assert;

const exports = @import("tb_client.zig").exports;
const c = @cImport(@cInclude("tb_client.h"));

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

    comptime for (.{
        .{ exports.tb_account_t, "tb_account_t" },
        .{ exports.tb_transfer_t, "tb_transfer_t" },
        .{ exports.tb_account_flags, "TB_ACCOUNT_FLAGS" },
        .{ exports.tb_transfer_flags, "TB_TRANSFER_FLAGS" },
        .{ exports.tb_create_account_result, "TB_CREATE_ACCOUNT_RESULT" },
        .{ exports.tb_create_transfer_result, "TB_CREATE_TRANSFER_RESULT" },
        .{ exports.tb_create_accounts_result_t, "tb_create_accounts_result_t" },
        .{ exports.tb_create_transfers_result_t, "tb_create_transfers_result_t" },
        .{ exports.tb_account_filter_t, "tb_account_filter_t" },
        .{ exports.tb_account_filter_flags, "TB_ACCOUNT_FILTER_FLAGS" },
        .{ exports.tb_account_balance_t, "tb_account_balance_t" },

        .{ u128, "tb_uint128_t" },
        .{ exports.tb_client_t, "tb_client_t" },
        .{ exports.tb_packet_t, "tb_packet_t" },
        .{ exports.tb_init_status, "TB_INIT_STATUS" },
        .{ exports.tb_client_status, "TB_CLIENT_STATUS" },
        .{ exports.tb_packet_status, "TB_PACKET_STATUS" },
        .{ exports.tb_operation, "TB_OPERATION" },
        .{ exports.tb_register_log_callback_status, "TB_REGISTER_LOG_CALLBACK_STATUS" },
        .{ exports.tb_log_level, "TB_LOG_LEVEL" },
    }) |c_export| {
        const ty: type = c_export[0];
        const c_type_name = @as([]const u8, c_export[1]);
        const c_type: type = @field(c, c_type_name);

        switch (@typeInfo(ty)) {
            .int => assert(ty == c_type),
            .pointer => assert(@sizeOf(ty) == @sizeOf(c_type)),
            .@"enum" => {
                const prefix_offset = std.mem.lastIndexOfScalar(u8, c_type_name, '_').?;
                var c_enum_prefix: []const u8 = c_type_name[0 .. prefix_offset + 1];
                assert(c_type == c_uint);

                // TB_STATUS and TB_OPERATION are special cases in naming
                if (std.mem.eql(u8, c_type_name, "TB_STATUS") or
                    std.mem.eql(u8, c_type_name, "TB_OPERATION"))
                {
                    c_enum_prefix = c_type_name ++ "_";
                }

                // Compare the enum int values in C to the enum int values in Zig.
                for (std.meta.fields(ty)) |field| {
                    if (std.mem.startsWith(u8, field.name, "deprecated_")) continue;
                    const c_enum_field = to_uppercase(to_snakecase(field.name));
                    const c_value = @field(c, c_enum_prefix ++ c_enum_field);

                    const zig_value = @intFromEnum(@field(ty, field.name));
                    assert(zig_value == c_value);
                }
            },
            .@"struct" => |type_info| switch (type_info.layout) {
                .auto => @compileError("struct must be extern or packed to be used in C"),
                .@"packed" => {
                    const prefix_offset = std.mem.lastIndexOfScalar(u8, c_type_name, '_').?;
                    const c_enum_prefix = c_type_name[0 .. prefix_offset + 1];
                    assert(c_type == c_uint);

                    for (std.meta.fields(ty)) |field| {
                        if (!std.mem.eql(u8, field.name, "padding")) {
                            // Get the bit value in the C enum.
                            const c_enum_field = to_uppercase(to_snakecase(field.name));
                            const c_value = @field(c, c_enum_prefix ++ c_enum_field);

                            // Compare the bit value to the packed struct's field.
                            var instance = std.mem.zeroes(ty);
                            @field(instance, field.name) = true;
                            assert(@as(type_info.backing_integer.?, @bitCast(instance)) == c_value);
                        }
                    }
                },
                .@"extern" => {
                    // Ensure structs are effectively the same.
                    assert(@sizeOf(ty) == @sizeOf(c_type));
                    if (@alignOf(ty) != @alignOf(c_type)) {
                        @compileLog(ty, c_type);
                    }
                    assert(@alignOf(ty) == @alignOf(c_type));

                    for (std.meta.fields(ty)) |field| {
                        // In C, packed structs and enums are replaced with integers.
                        var field_type = field.type;
                        switch (@typeInfo(field_type)) {
                            .@"struct" => |info| {
                                assert(info.layout == .@"packed");
                                assert(@sizeOf(field_type) <= @sizeOf(u128));
                                field_type = std.meta.Int(.unsigned, @bitSizeOf(field_type));
                            },
                            .@"enum" => |info| field_type = info.tag_type,
                            .bool => field_type = u8,
                            else => {},
                        }

                        // In C, pointers are opaque so we compare only the field sizes,
                        const c_field_type = @TypeOf(@field(@as(c_type, undefined), field.name));
                        switch (@typeInfo(c_field_type)) {
                            .pointer => |info| {
                                assert(info.size == .c);
                                assert(@sizeOf(c_field_type) == @sizeOf(field_type));
                            },
                            else => assert(c_field_type == field_type),
                        }
                    }
                },
            },
            else => |i| @compileLog("TODO", i),
        }
    };
}
