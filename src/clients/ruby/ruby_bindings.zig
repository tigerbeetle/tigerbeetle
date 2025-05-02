const std = @import("std");
const vsr = @import("vsr");
const exports = vsr.tb_client.exports;
const assert = std.debug.assert;

const constants = vsr.constants;
const IO = vsr.io.IO;

const Tracer = vsr.trace.TracerType(vsr.time.Time);
const Storage = vsr.storage.StorageType(IO, Tracer);
const StateMachine = vsr.state_machine.StateMachineType(Storage, constants.state_machine_config);
const tb = vsr.tigerbeetle;

/// VSR type mappings: these will always be the same regardless of state machine.
const mappings_vsr = .{
    .{ exports.tb_operation, "Operation" },
    .{ exports.tb_packet_status, "PacketStatus" },
    .{ exports.tb_packet_t, "Packet" },
    .{ exports.tb_client_t, "Client" },
    .{ exports.tb_init_status, "InitStatus" },
    .{ exports.tb_client_status, "ClientStatus" },
    .{ exports.tb_log_level, "LogLevel" },
    .{ exports.tb_register_log_callback_status, "RegisterLogCallbackStatus" },
};

/// State machine specific mappings: in future, these should be pulled automatically from the state
/// machine.
const mappings_state_machine = .{
    .{ tb.AccountFlags, "AccountFlags" },
    .{ tb.TransferFlags, "TransferFlags" },
    .{ tb.AccountFilterFlags, "AccountFilterFlags" },
    .{ tb.QueryFilterFlags, "QueryFilterFlags" },
    .{ tb.Account, "Account" },
    .{ tb.Transfer, "Transfer" },
    .{ tb.CreateAccountResult, "CreateAccountResult" },
    .{ tb.CreateTransferResult, "CreateTransferResult" },
    .{ tb.CreateAccountsResult, "CreateAccountsResult" },
    .{ tb.CreateTransfersResult, "CreateTransfersResult" },
    .{ tb.AccountFilter, "AccountFilter" },
    .{ tb.AccountBalance, "AccountBalance" },
    .{ tb.QueryFilter, "QueryFilter" },
};

const mappings_all = mappings_vsr ++ mappings_state_machine;

const Buffer = struct {
    inner: std.ArrayList(u8),

    pub fn init(allocator: std.mem.Allocator) Buffer {
        return .{
            .inner = std.ArrayList(u8).init(allocator),
        };
    }

    pub fn print(self: *Buffer, comptime format: []const u8, args: anytype) void {
        self.inner.writer().print(format, args) catch unreachable;
    }
};

fn mapping_name_from_type(mappings: anytype, Type: type) ?[]const u8 {
    comptime for (mappings) |mapping| {
        const ZigType, const ruby_name = mapping;

        if (Type == ZigType) {
            return ruby_name;
        }
    };
    return null;
}

fn zig_to_ruby(comptime Type: type) []const u8 {
    switch (@typeInfo(Type)) {
        .Array => |info| {
            return std.fmt.comptimePrint("[{s}, {d}]", .{
                comptime zig_to_ruby(info.child),
                info.len,
            });
        },
        .Enum => return comptime mapping_name_from_type(mappings_state_machine, Type).?,
        .Struct => return comptime mapping_name_from_type(mappings_state_machine, Type).?,
        .Bool => return ":bool",
        .Int => |info| {
            assert(info.signedness == .unsigned);

            return switch (info.bits) {
                8 => ":uint8",
                16 => ":uint16",
                32 => ":uint32",
                64 => ":uint64",
                128 => "UINT128",
                else => @compileError("invalid int type"),
            };
        },
        .Optional => |info| switch (@typeInfo(info.child)) {
            .Pointer => return zig_to_ruby(info.child),
            else => @compileError("Unsupported optional type: " ++ @typeName(Type)),
        },
        .Pointer => |info| {
            assert(info.size == .One);
            assert(!info.is_allowzero);

            return ":pointer";
        },
        .Void => return ":void",
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

fn ruby_ffi_enum_type(comptime Type: type) []const u8 {
    return switch (@bitSizeOf(Type)) {
        8 => "FFI::Type::UINT8",
        16 => "FFI::Type::UINT16",
        32 => "FFI::Type::UINT32",
        64 => "FFI::Type::UINT64",
        128 => "TBClient::UINT128",
        else => @compileError("invalid int type"),
    };
}

fn emit_enum(
    buffer: *Buffer,
    comptime Type: type,
    comptime type_info: anytype,
    comptime ruby_name: []const u8,
    comptime skip_fields: []const []const u8,
) !void {
    if (@typeInfo(Type) == .Enum) {
        buffer.print("  {s} = enum({s}, [\n", .{ ruby_name, comptime ruby_ffi_enum_type(Type) });
    } else {
        // Packed structs.
        assert(@typeInfo(Type) == .Struct and @typeInfo(Type).Struct.layout == .@"packed");

        buffer.print("  {s} = bitmask({s}, [\n", .{ ruby_name, comptime ruby_ffi_enum_type(Type) });
    }

    inline for (type_info.fields, 0..) |field, i| {
        if (comptime std.mem.startsWith(u8, field.name, "deprecated_")) continue;
        comptime var skip = false;
        inline for (skip_fields) |sf| {
            skip = skip or comptime std.mem.eql(u8, sf, field.name);
        }

        if (!skip) {
            const field_name = to_uppercase(field.name);
            if (@typeInfo(Type) == .Enum) {
                buffer.print("    :{s}, {d},\n", .{
                    @as([]const u8, &field_name),
                    @intFromEnum(@field(Type, field.name)),
                });
            } else {
                // Packed structs.
                buffer.print("    :{s}, 1 << {d},\n", .{
                    @as([]const u8, &field_name),
                    i,
                });
            }
        }
    }

    buffer.print("  ])\n\n", .{});
}

fn emit_ruby_ffi_struct(
    buffer: *Buffer,
    comptime type_info: std.builtin.Type.Struct,
    comptime c_name: []const u8,
) !void {
    buffer.print(
        \\  class {s} < FFI::Struct
        \\    layout(
        \\
    , .{c_name});

    inline for (type_info.fields) |field| {
        const ruby_name_map = comptime mapping_name_from_type(mappings_all, field.type);
        if (ruby_name_map) |ruby_name| {
            buffer.print("      {s}: {s},\n", .{
                field.name,
                ruby_name,
            });
        } else {
            buffer.print("      {s}: {s},\n", .{
                field.name,
                zig_to_ruby(field.type),
            });
        }
    }

    buffer.print(
        \\    )
        \\  end
        \\
        \\
    , .{});
}

pub fn main() !void {
    @setEvalBranchQuota(100_000);

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var buffer = Buffer.init(allocator);
    buffer.print(
        \\##########################################################
        \\## This file was auto-generated by tb_client_header.zig ##
        \\##              Do not manually modify.                 ##
        \\##########################################################
        \\
        \\require "ffi"
        \\require "tb_client/shared_lib"
        \\
        \\module TBClient
        \\  extend FFI::Library
        \\
        \\  ffi_lib SharedLib.path
        \\
        \\  class UINT128 < FFI::Struct
        \\    layout(low: :uint64, high: :uint64)
        \\  end
        \\
        \\
    , .{});

    // Emit enum and bitmask declarations.
    inline for (mappings_all) |type_mapping| {
        const ZigType, const ruby_name = type_mapping;

        switch (@typeInfo(ZigType)) {
            .Struct => |info| switch (info.layout) {
                .auto => @compileError("Invalid C struct type: " ++ @typeName(ZigType)),
                .@"packed" => try emit_enum(&buffer, ZigType, info, ruby_name, &.{"padding"}),
                .@"extern" => continue,
            },
            .Enum => |info| {
                comptime var skip: []const []const u8 = &.{};
                if (ZigType == exports.tb_operation) {
                    skip = &.{ "reserved", "root", "register" };
                }

                try emit_enum(&buffer, ZigType, info, ruby_name, skip);
            },
            else => buffer.print("{s} = {s}\n\n", .{
                ruby_name,
                zig_to_ruby(ZigType),
            }),
        }
    }

    // Emit FFI::Struct declarations.
    inline for (mappings_all) |type_mapping| {
        const ZigType, const ruby_name = type_mapping;

        switch (@typeInfo(ZigType)) {
            .Struct => |info| switch (info.layout) {
                .auto => @compileError("Invalid C struct type: " ++ @typeName(ZigType)),
                .@"packed" => continue,
                .@"extern" => try emit_ruby_ffi_struct(
                    &buffer,
                    info,
                    ruby_name,
                ),
            },
            else => continue,
        }
    }

    buffer.print("end\n", .{});

    try std.io.getStdOut().writeAll(buffer.inner.items);
}
