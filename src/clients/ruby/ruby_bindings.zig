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

/// Resolves a Zig Type into a string representing the name of a corresponding Ruby FFI Type.
/// If the type ZigType is a TB struct then it returns the Ruby class name
/// It is used to generate the FFI::Struct layout
fn zig_to_rb_ffi_type(comptime Type: type) []const u8 {
    switch (@typeInfo(Type)) {
        .Array => |info| {
            return std.fmt.comptimePrint("[{s}, {d}]", .{
                comptime zig_to_rb_ffi_type(info.child),
                info.len,
            });
        },
        .Enum, .Struct => return comptime mapping_name_from_type(mappings_all, Type).?,
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
            .Pointer => return zig_to_rb_ffi_type(info.child),
            else => @compileError("Unsupported optional type: " ++ @typeName(Type)),
        },
        .Pointer => |info| {
            assert(info.size == .One);
            assert(!info.is_allowzero);

            if (Type == *anyopaque) {
                return ":pointer";
            }
            @compileError("Unhandled type: " ++ @typeName(Type));
        },
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

fn ffi_int_type(comptime Type: type) []const u8 {
    switch (@typeInfo(Type)) {
        .Enum => {
            return switch (std.meta.Tag(Type)) {
                u8 => "UINT8",
                u16 => "UINT16",
                u32 => "UINT32",
                u64 => "UINT64",
                c_int => "INT",
                else => @compileError("Could not set enum type: " ++ @typeName(Type) ++ " found type: " ++ @typeName(std.meta.Tag(Type))),
            };
        },
        .Struct => {
            return switch (@bitSizeOf(Type)) {
                8 => "UINT8",
                16 => "UINT16",
                32 => "UINT32",
                64 => "UINT64",
                else => @compileError("Could not set enum for packed type: " ++ @typeName(Type)),
            };
        },
        else => @compileError("Could not set enum type: " ++ @typeName(Type)),
    }
}

fn emit_enum(
    buffer: *Buffer,
    comptime Type: type,
    comptime type_info: anytype,
    comptime ruby_name: []const u8,
    comptime skip_fields: []const []const u8,
) !void {
    const underlying_type = comptime ffi_int_type(Type);
    if (@typeInfo(Type) == .Enum) {
        buffer.print("  {s} = enum(FFI::Type::{s}, [\n", .{ ruby_name, underlying_type });
    } else {
        // Packed structs.
        assert(@typeInfo(Type) == .Struct and @typeInfo(Type).Struct.layout == .@"packed");

        buffer.print("  {s} = bitmask(FFI::Type::{s}, [\n", .{ ruby_name, underlying_type });
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
                buffer.print("    :{s}, 1 << {},\n", .{
                    @as([]const u8, &field_name),
                    i,
                });
            }
        }
    }

    buffer.print("  ])\n\n", .{});
}

fn emit_rb_ffi_struct(
    buffer: *Buffer,
    comptime type_info: anytype,
    comptime ruby_name: []const u8,
) !void {
    buffer.print(
        \\  class {s} < FFI::Struct
        \\    layout(
        \\
    , .{
        .type_name = ruby_name,
    });

    inline for (type_info.fields) |field| {
        buffer.print("      {s}: {s},\n", .{
            field.name,
            zig_to_rb_ffi_type(field.type),
        });
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
        \\#######################################################
        \\## This file was auto-generated by ruby_bindings.zig ##
        \\##              Do not manually modify.              ##
        \\#######################################################
        \\
        \\# frozen_string_literal: true
        \\
        \\require "ffi"
        \\
        \\require_relative "tb_client/version"
        \\require_relative "tb_client/shared_lib"
        \\
        \\module TBClient
        \\  extend FFI::Library
        \\
        \\  ffi_lib SharedLib.path
        \\
        \\
    , .{});

    // Emit enum and direct declarations.
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
            else => {},
        }
    }

    buffer.print(
        \\  class UINT128 < FFI::Struct
        \\    layout(lo: :uint64, hi: :uint64)
        \\  end
        \\
        \\
    , .{});

    // Emit FFI::Struct declarations
    inline for (mappings_all) |type_mapping| {
        const ZigType, const ruby_name = type_mapping;

        switch (@typeInfo(ZigType)) {
            .Struct => |info| switch (info.layout) {
                .@"extern" => try emit_rb_ffi_struct(&buffer, info, ruby_name),
                else => {},
            },
            else => {},
        }
    }

    // Emit function declarations corresponding to the underlying libtbclient exported functions.
    buffer.print(
        \\  callback :on_completion, [:uint, Packet.by_ref, :uint64, :pointer, :uint32], :void
        \\  callback :log_handler, [LogLevel, :pointer, :uint32], :void
        \\
        \\  attach_function :tb_client_init, [Client.by_ref, :pointer, :string, :uint32, :uint, :on_completion], InitStatus
        \\  attach_function :tb_client_submit, [Client.by_ref, Packet.by_ref], ClientStatus
        \\  attach_function :tb_client_deinit, [Client.by_ref], ClientStatus
        \\  attach_function :tb_client_register_log_callback, [:log_handler, :bool], RegisterLogCallbackStatus
        \\end
        \\
    , .{});

    try std.io.getStdOut().writeAll(buffer.inner.items);
}
