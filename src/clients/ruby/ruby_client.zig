const std = @import("std");
const vsr = @import("vsr");
const exports = vsr.tb_client.exports;
const assert = std.debug.assert;

const converter = @import("converter.zig");

const constants = vsr.constants;
const IO = vsr.io.IO;

const Tracer = vsr.trace.TracerType(vsr.time.Time);
const Storage = vsr.storage.StorageType(IO, Tracer);
const StateMachine = vsr.state_machine.StateMachineType(Storage, constants.state_machine_config);
const tb = vsr.tigerbeetle;

const ruby = @cImport(@cInclude("ruby.h"));

/// VSR type mappings: these will always be the same regardless of state machine.
const mappings_vsr = .{
    .{ exports.tb_operation, "Operation" },
    .{ exports.tb_packet_status, "PacketStatus" },
    // .{ exports.tb_packet_t, "Packet" }, // Not used in Ruby bindings, so not included.
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
    .{ tb.AccountFilter, "AccountFilter" },
    .{ tb.AccountBalance, "AccountBalance" },
    .{ tb.QueryFilter, "QueryFilter" },
};

const mappings_all = mappings_vsr ++ mappings_state_machine;

export fn initialize_ruby_client() callconv(.C) void {
    const m_tiger_beetle = ruby.rb_define_module("TigerBeetle");
    const m_bindings = ruby.rb_define_module_under(m_tiger_beetle, "Bindings");

    inline for (mappings_all) |type_mapping| {
        const ZigType = type_mapping[0];
        const ruby_name = type_mapping[1];

        switch (@typeInfo(ZigType)) {
            .Enum => {
                convert_enum_to_ruby_const(m_bindings, ZigType, ruby_name);
            },
            .Struct => |info| switch (info.layout) {
                .@"packed" => convert_enum_to_ruby_const(m_bindings, ZigType, ruby_name),
                .@"extern" => convert_struct_to_ruby_class(m_bindings, ZigType, ruby_name),
                else => @compileError("Unsupported struct: " ++ info),
            },
            else => @compileError("Unsupported Zig type for Ruby mapping: " ++ @typeInfo(ZigType)),
        }
    }
}

fn to_upper_case(comptime input: []const u8) [input.len + 1:0]u8 {
    var result: [input.len + 1:0]u8 = undefined;
    _ = std.ascii.upperString(result[0..input.len], input);
    result[input.len] = 0; // null terminator
    return result;
}

fn convert_enum_to_ruby_const(module: ruby.VALUE, comptime ZigType: type, comptime ruby_name: []const u8) void {
    const ruby_enum = ruby.rb_define_module_under(module, ruby_name.ptr);
    switch (@typeInfo(ZigType)) {
        .Enum => |enum_info| {
            inline for (enum_info.fields) |field| {
                if (comptime std.mem.startsWith(u8, field.name, "deprecated_")) {
                    continue;
                }
                const enum_value = @field(ZigType, field.name);
                const ruby_value = @intFromEnum(enum_value);

                const ruby_const_name = to_upper_case(field.name);

                _ = ruby.rb_define_const(ruby_enum, &ruby_const_name, ruby.UINT2NUM(ruby_value));
            }
        },
        .Struct => |struct_info| {
            const layout = struct_info.layout;
            assert(layout == .@"packed");

            inline for (struct_info.fields, 0..) |field, i| {
                if (comptime std.mem.startsWith(u8, field.name, "deprecated_")) {
                    continue;
                }
                const ruby_const_name = to_upper_case(field.name);
                _ = ruby.rb_define_const(ruby_enum, &ruby_const_name, ruby.UINT2NUM(1 << i));
            }
        },
        else => @compileError("Invalid conversion to enum: " ++ ZigType),
    }
}

fn convert_struct_to_ruby_class(module: ruby.VALUE, comptime ZigType: type, comptime ruby_name: []const u8) void {
    if (@typeInfo(ZigType) != .Struct) {
        @compileError("Expected a struct type for Ruby class conversion, got: " ++ @typeInfo(ZigType));
    }

    const rb_class = ruby.rb_define_class_under(module, ruby_name.ptr, ruby.rb_cObject);

    const c_struct = comptime converter.to_ruby_class(ZigType);
    c_struct.init_methods(rb_class);
}
