const std = @import("std");
const vsr = @import("vsr");
const tb_client = vsr.tb_client;
const exports = tb_client.exports;
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
                converter.to_ruby_module(m_bindings, ZigType, ruby_name);
            },
            .Struct => |info| switch (info.layout) {
                .@"packed" => converter.to_ruby_module(m_bindings, ZigType, ruby_name),
                .@"extern" => convert_struct_to_ruby_class(m_bindings, ZigType, ruby_name),
                else => @compileError("Unsupported struct: " ++ info),
            },
            else => @compileError("Unsupported Zig type for Ruby mapping: " ++ @typeInfo(ZigType)),
        }
    }
}

fn convert_struct_to_ruby_class(module: ruby.VALUE, comptime ZigType: type, comptime ruby_name: []const u8) void {
    if (@typeInfo(ZigType) != .Struct) {
        @compileError("Expected a struct type for Ruby class conversion, got: " ++ @typeInfo(ZigType));
    }

    const rb_class = ruby.rb_define_class_under(module, ruby_name.ptr, ruby.rb_cObject);

    const c_struct = comptime converter.to_ruby_class(ZigType);
    c_struct.init_methods(rb_class);

    if (ZigType == exports.tb_client_t) {
        const client_struct = init_client_fn(c_struct.rb_data_type);
        _ = ruby.rb_define_method(rb_class, "init", client_struct.init, -1);
    }
}

fn init_client_fn(rb_type: ruby.rb_data_type_t) type {
    return struct {
        fn init(argc: c_int, argv: [*]ruby.VALUE, self: ruby.VALUE) callconv(.C) ruby.VALUE {
            var kwargs = ruby.Qnil;
            ruby.rb_scan_args(argc, argv, ":", &kwargs);

            if (ruby.NIL_P(kwargs)) {
                ruby.rb_raise(ruby.rb_eArgError, "Expected arguments: { addresses: String, cluster_id: Integer }");
                return ruby.Qnil;
            }

            const rb_addresses = ruby.rb_hash_aref(kwargs, ruby.rb_intern("addresses"));
            if (ruby.NIL_P(rb_addresses)) {
                ruby.rb_raise(ruby.rb_eArgError, "Missing required argument: addresses");
                return ruby.Qnil;
            }
            const rb_cluster_id = ruby.rb_hash_aref(kwargs, ruby.rb_intern("cluster_id"));
            if (ruby.NIL_P(rb_cluster_id)) {
                ruby.rb_raise(ruby.rb_eArgError, "Missing required argument: cluster_id");
                return ruby.Qnil;
            }
            ruby.Check_Type(rb_addresses, ruby.T_STRING);
            ruby.Check_Type(rb_cluster_id, ruby.T_FIXNUM);

            const cluster_id = converter.rb_int_to_u128(rb_cluster_id);

            // Get the pointer to the client struct from the Ruby object
            const client: *exports.tb_client_t = @ptrCast(@alignCast(ruby.rb_check_typeddata(self, &rb_type)));

            // Convert cluster_id to [16]u8 array
            var cluster_id_bytes: [16]u8 = undefined;
            std.mem.writeInt(u128, &cluster_id_bytes, cluster_id, .little);

            // TODO: We need to handle the async callback properly
            // For now, let's use a dummy callback for synchronous init
            const completion_ctx: usize = 0;
            const completion_callback: exports.tb_completion_t = null;

            const status = exports.init(
                client,
                &cluster_id_bytes,
                @ptrCast(ruby.RSTRING_PTR(rb_addresses)),
                @intCast(ruby.RSTRING_LEN(rb_addresses)),
                completion_ctx,
                completion_callback,
            );

            if (status != .success) {
                const error_msg = switch (status) {
                    .unexpected => "Unexpected error",
                    .out_of_memory => "Out of memory",
                    .address_invalid => "Invalid address",
                    .address_limit_exceeded => "Address limit exceeded",
                    .system_resources => "System resources error",
                    .network_subsystem => "Network subsystem error",
                    else => "Unknown error",
                };
                ruby.rb_raise(ruby.rb_eRuntimeError, "Failed to initialize client: %s", error_msg);
                return ruby.Qnil;
            }

            return self; // Return self to allow method chaining.
        }
    };
}
