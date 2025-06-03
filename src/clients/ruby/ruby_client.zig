const std = @import("std");
const vsr = @import("vsr");

const tb_client = vsr.tb_client;
const exports = tb_client.exports;
const tb_packet_t = exports.tb_packet_t;

const assert = std.debug.assert;

const converter = @import("converter.zig");

const constants = vsr.constants;
const IO = vsr.io.IO;

const Tracer = vsr.trace.TracerType(vsr.time.Time);
const Storage = vsr.storage.StorageType(IO, Tracer);
const StateMachine = vsr.state_machine.StateMachineType(Storage, constants.state_machine_config);
const tb = vsr.tigerbeetle;

const ruby = @cImport(@cInclude("ruby.h"));

const global_allocator = std.heap.c_allocator;

const MAX_BATCH_SIZE = 8192; // Maximum size of a batch in bytes

fn build_rb_setup_struct(comptime ZigType: type, comptime ruby_name: []const u8) type {
    return switch (@typeInfo(ZigType)) {
        .Enum => {
            return converter.to_ruby_enum(ZigType, ruby_name);
        },
        .Struct => |info| switch (info.layout) {
            .@"packed" => {
                return converter.to_ruby_enum(ZigType, ruby_name);
            },
            .@"extern" => {
                return converter.to_ruby_class(ZigType, ruby_name);
            },
            else => @compileError("Unsupported struct layout: " ++ info),
        },
        else => @compileError("Unsupported Zig type for Ruby mapping: " ++ @typeInfo(ZigType)),
    };
}
/// VSR type mappings: these will always be the same regardless of state machine.
const mappings_vsr = .{
    .{ exports.tb_operation, build_rb_setup_struct(exports.tb_operation, "Operation") },
    .{ exports.tb_packet_status, build_rb_setup_struct(exports.tb_packet_status, "PacketStatus") },
    // .{ exports.tb_packet_t, "Packet" }, // Not used in Ruby bindings, so not included.
    .{ exports.tb_client_t, build_rb_setup_struct(exports.tb_client_t, "Client") },
    .{ exports.tb_init_status, build_rb_setup_struct(exports.tb_init_status, "InitStatus") },
    .{ exports.tb_client_status, build_rb_setup_struct(exports.tb_client_status, "ClientStatus") },
    .{ exports.tb_log_level, build_rb_setup_struct(exports.tb_log_level, "LogLevel") },
    .{ exports.tb_register_log_callback_status, build_rb_setup_struct(exports.tb_register_log_callback_status, "RegisterLogCallbackStatus") },
};

/// State machine specific mappings: in future, these should be pulled automatically from the state
/// machine.
const mappings_state_machine = .{
    .{ tb.AccountFlags, build_rb_setup_struct(tb.AccountFlags, "AccountFlags") },
    .{ tb.TransferFlags, build_rb_setup_struct(tb.TransferFlags, "TransferFlags") },
    .{ tb.AccountFilterFlags, build_rb_setup_struct(tb.AccountFilterFlags, "AccountFilterFlags") },
    .{ tb.QueryFilterFlags, build_rb_setup_struct(tb.QueryFilterFlags, "QueryFilterFlags") },
    .{ tb.Account, build_rb_setup_struct(tb.Account, "Account") },
    .{ tb.Transfer, build_rb_setup_struct(tb.Transfer, "Transfer") },
    .{ tb.CreateAccountResult, build_rb_setup_struct(tb.CreateAccountResult, "CreateAccountResult") },
    .{ tb.CreateTransferResult, build_rb_setup_struct(tb.CreateTransferResult, "CreateTransferResult") },
    .{ tb.AccountFilter, build_rb_setup_struct(tb.AccountFilter, "AccountFilter") },
    .{ tb.AccountBalance, build_rb_setup_struct(tb.AccountBalance, "AccountBalance") },
    .{ tb.QueryFilter, build_rb_setup_struct(tb.QueryFilter, "QueryFilter") },
    .{ tb.CreateAccountsResult, build_rb_setup_struct(tb.CreateAccountsResult, "CreateAccountsResult") },
};

const mappings_all = mappings_vsr ++ mappings_state_machine;

pub export fn initialize_ruby_client() callconv(.C) void {
    const m_tiger_beetle = ruby.rb_define_module("TigerBeetle");
    const m_bindings = ruby.rb_define_module_under(m_tiger_beetle, "Bindings");

    inline for (mappings_all) |type_mapping| {
        const setup_struct = type_mapping[1];
        setup_struct.init_methods(m_bindings);
    }

    const client_struct = tb_client_struct();
    const rb_client = ruby.rb_const_get(m_bindings, ruby.rb_intern("Client"));
    _ = ruby.rb_define_method(rb_client, "init", @ptrCast(&client_struct.init), 2);
    _ = ruby.rb_define_method(rb_client, "submit", @ptrCast(&client_struct.submit), 2);
}

fn tb_client_struct() type {
    const rb_client_type_t: *const ruby.rb_data_type_t = comptime blk: {
        for (mappings_all) |type_mapping| {
            if (type_mapping[0] == exports.tb_client_t) {
                break :blk type_mapping[1].get_rb_data_type_ptr();
            }
        }
    };
    return struct {
        const ResultContext = struct {
            mutex: std.Thread.Mutex = std.Thread.Mutex{},
            condition: std.Thread.Condition = std.Thread.Condition{},
            result_data: ?[]u8 = null,
            result_len: u32 = 0,
            waiting: bool = true,
            operation: exports.tb_operation,
            result_error: ?Error = null,

            const Error = error{
                MemoryAllocationFailed,
            };
        };

        fn on_completion(
            completion_ctx: usize,
            packet: *tb_packet_t,
            timestamp: u64,
            result_ptr: [*]const u8,
            result_len: u32,
        ) callconv(.C) void {
            _ = completion_ctx;
            _ = timestamp;

            const result_context: *ResultContext = @ptrCast(@alignCast(packet.user_data));
            result_context.mutex.lock();
            defer result_context.mutex.unlock();
            result_context.waiting = false;
            result_context.result_len = result_len;
            if (result_len > 0) {
                const data_alloc = global_allocator.alloc(u8, result_len) catch {
                    result_context.result_error = ResultContext.Error.MemoryAllocationFailed;
                    return;
                };
                result_context.result_data = data_alloc;
                @memcpy(data_alloc, result_ptr[0..result_len]);
            } else {
                result_context.result_data = null;
            }
            result_context.condition.signal();
        }

        fn init(self: ruby.VALUE, rb_addresses: ruby.VALUE, rb_cluster_id: ruby.VALUE) callconv(.C) ruby.VALUE {
            if (ruby.NIL_P(rb_addresses) or !ruby.RB_TYPE_P(rb_addresses, ruby.T_STRING)) {
                ruby.rb_raise(ruby.rb_eArgError, "addresses must be a non-nil String");
                return ruby.Qnil;
            }
            if (ruby.NIL_P(rb_cluster_id)) {
                ruby.rb_raise(ruby.rb_eArgError, "cluster_id must be a non-nil Integer");
                return ruby.Qnil;
            }

            const cluster_id: [16]u8 = @bitCast(converter.rb_int_to_u128(rb_cluster_id));

            if (!ruby.RB_TYPE_P(self, ruby.T_DATA)) {
                ruby.rb_raise(ruby.rb_eTypeError, "Expected a Client object");
                return ruby.Qnil;
            }

            const client: *exports.tb_client_t = @ptrCast(@alignCast(ruby.rb_check_typeddata(self, rb_client_type_t)));

            const status = exports.init(
                client,
                &cluster_id,
                @ptrCast(ruby.RSTRING_PTR(rb_addresses)),
                @intCast(ruby.RSTRING_LEN(rb_addresses)),
                0,
                @ptrCast(&on_completion),
            );

            return ruby.INT2NUM(@intFromEnum(status));
        }

        fn submit(self: ruby.VALUE, rb_operation: ruby.VALUE, rb_data: ruby.VALUE) callconv(.C) ruby.VALUE {
            if (ruby.NIL_P(rb_operation) or !ruby.RB_TYPE_P(rb_operation, ruby.T_FIXNUM)) {
                ruby.rb_raise(ruby.rb_eArgError, "operation must be a non-nil Integer object");
                return ruby.Qnil;
            }
            if (ruby.NIL_P(rb_data) or !ruby.RB_TYPE_P(rb_data, ruby.T_ARRAY)) {
                ruby.rb_raise(ruby.rb_eArgError, "data must be a non-nil Array object");
                return ruby.Qnil;
            }

            const operation_int = ruby.NUM2INT(rb_operation);
            const operation_enum: exports.tb_operation = @enumFromInt(operation_int);
            var data: packet_data = undefined;
            data.size = 0;
            data.data = null;

            parse_data(operation_enum, rb_data, &data);

            const client: *exports.tb_client_t = @ptrCast(@alignCast(ruby.rb_check_typeddata(self, rb_client_type_t)));

            var result_context = ResultContext{
                .operation = operation_enum,
            };

            var packet = exports.tb_packet_t{
                .user_data = @ptrCast(&result_context),
                .data = @constCast(@ptrCast(data.data)),
                .data_size = data.size,
                .user_tag = 0, // Set by the client internally
                .operation = @intFromEnum(operation_enum),
                .status = exports.tb_packet_status.ok, // Initial status
            };

            const status = exports.submit(client, &packet);

            if (status != exports.tb_client_status.ok) {
                var buf: [256]u8 = undefined;
                const msg = std.fmt.bufPrintZ(&buf, "Failed to submit packet: {}", .{@intFromEnum(status)}) catch {
                    ruby.rb_raise(ruby.rb_eArgError, "Failed to submit packet");
                    return ruby.Qnil;
                };
                ruby.rb_raise(ruby.rb_eRuntimeError, msg.ptr);
                return ruby.Qnil;
            }

            result_context.mutex.lock();
            defer result_context.mutex.unlock();

            while (result_context.waiting) {
                result_context.condition.wait(&result_context.mutex);
            }

            if (result_context.result_error) |e| {
                switch (e) {
                    ResultContext.Error.MemoryAllocationFailed => ruby.rb_raise(ruby.rb_eRuntimeError, "Memory allocation failed"),
                }
                return ruby.Qnil;
            }
            if (result_context.result_len == 0) {
                return ruby.rb_ary_new();
            }

            if (result_context.result_data) |result| {
                const m_tiger_beetle = ruby.rb_const_get(ruby.rb_cObject, ruby.rb_intern("TigerBeetle"));
                const m_bindings = ruby.rb_const_get(m_tiger_beetle, ruby.rb_intern("Bindings"));
                switch (result_context.operation) {
                    .lookup_accounts => {
                        const rb_account_class_name = comptime blk: {
                            for (mappings_state_machine) |type_mapping| {
                                if (type_mapping[0] == tb.Account) {
                                    break :blk type_mapping[1].rb_class_name;
                                }
                            }
                        };
                        const rb_account_class = ruby.rb_const_get(m_bindings, ruby.rb_intern(&rb_account_class_name[0]));
                        const num_accounts = result_context.result_len / @sizeOf(tb.Account);
                        const rb_result = ruby.rb_ary_new2(@intCast(num_accounts));
                        const rb_data_account_t: *const ruby.rb_data_type_t = comptime blk: {
                            for (mappings_state_machine) |type_mapping| {
                                if (type_mapping[0] == tb.Account) {
                                    break :blk type_mapping[1].get_rb_data_type_ptr();
                                }
                            }
                        };
                        for (0..num_accounts) |i| {
                            const account_data = result[i * @sizeOf(tb.Account) .. (i + 1) * @sizeOf(tb.Account)];
                            const rb_account = ruby.rb_class_new_instance(0, null, rb_account_class);
                            const account: *tb.Account = @ptrCast(@alignCast(ruby.rb_check_typeddata(rb_account, rb_data_account_t)));

                            @memcpy(@as([*]u8, @ptrCast(account))[0..@sizeOf(tb.Account)], account_data);

                            _ = ruby.rb_ary_push(rb_result, rb_account);
                        }

                        return rb_result;
                    },
                    .create_accounts => {
                        const rb_account_results_class_name = comptime blk: {
                            for (mappings_state_machine) |type_mapping| {
                                if (type_mapping[0] == tb.CreateAccountsResult) {
                                    break :blk type_mapping[1].rb_class_name;
                                }
                            }
                        };
                        const rb_create_account_result_class = ruby.rb_const_get(m_bindings, ruby.rb_intern(&rb_account_results_class_name[0]));
                        const num_results = result_context.result_len / @sizeOf(tb.CreateAccountsResult);
                        const rb_result = ruby.rb_ary_new2(@intCast(num_results));
                        const rb_data_create_account_result_t: *const ruby.rb_data_type_t = comptime blk: {
                            for (mappings_state_machine) |type_mapping| {
                                if (type_mapping[0] == tb.CreateAccountsResult) {
                                    break :blk type_mapping[1].get_rb_data_type_ptr();
                                }
                            }
                        };
                        for (0..num_results) |i| {
                            const result_data = result[i * @sizeOf(tb.CreateAccountsResult) .. (i + 1) * @sizeOf(tb.CreateAccountsResult)];
                            const rb_result_instance = ruby.rb_class_new_instance(0, null, rb_create_account_result_class);
                            const create_account_result: *tb.CreateAccountsResult = @ptrCast(@alignCast(ruby.rb_check_typeddata(rb_result_instance, rb_data_create_account_result_t)));

                            @memcpy(@as([*]u8, @ptrCast(create_account_result))[0..@sizeOf(tb.CreateAccountsResult)], result_data);

                            _ = ruby.rb_ary_push(rb_result, rb_result_instance);
                        }

                        return rb_result;
                    },
                    else => {
                        ruby.rb_raise(ruby.rb_eRuntimeError, "Unsupported operation for result data");
                        return ruby.Qnil;
                    },
                }
            } else {
                ruby.rb_raise(ruby.rb_eRuntimeError, "No result data available");
                return ruby.Qfalse;
            }
        }

        const packet_data = struct {
            size: u32,
            data: ?[*]const u8 = null,
        };

        fn parse_data(operation: exports.tb_operation, rb_data: ruby.VALUE, out_data: *packet_data) void {
            if (ruby.NIL_P(rb_data) or !ruby.RB_TYPE_P(rb_data, ruby.T_ARRAY)) {
                ruby.rb_raise(ruby.rb_eArgError, "data must be a non-nil Array object");
                return;
            }

            const data_array_len = ruby.RARRAY_LEN(rb_data);
            if (data_array_len == 0) {
                ruby.rb_raise(ruby.rb_eArgError, "data array cannot be empty");
                return;
            } else if (data_array_len > MAX_BATCH_SIZE) {
                ruby.rb_raise(ruby.rb_eArgError, "data array size exceeds maximum allowed size of {}", .{MAX_BATCH_SIZE});
                return;
            }

            switch (operation) {
                .lookup_accounts => {
                    out_data.size = @intCast(data_array_len * @sizeOf(u128));
                    const data_alloc = global_allocator.alloc(u8, out_data.size) catch {
                        ruby.rb_raise(ruby.rb_eRuntimeError, "Failed to allocate memory for data");
                        return;
                    };
                    out_data.data = data_alloc.ptr;

                    var i: usize = 0;
                    while (i < data_array_len) : (i += 1) {
                        const rb_account_id = ruby.rb_ary_entry(rb_data, @intCast(i));
                        const account_id = converter.rb_int_to_u128(rb_account_id);

                        const dest_slice = data_alloc[i * @sizeOf(u128) .. (i + 1) * @sizeOf(u128)];
                        @memcpy(dest_slice, std.mem.asBytes(&account_id));
                    }
                },
                .create_accounts => {
                    out_data.size = @intCast(data_array_len * @sizeOf(tb.Account));
                    const data_alloc = global_allocator.alloc(u8, out_data.size) catch {
                        ruby.rb_raise(ruby.rb_eRuntimeError, "Failed to allocate memory for data");
                        return;
                    };
                    out_data.data = data_alloc.ptr;

                    const rb_data_account_t: *const ruby.rb_data_type_t = comptime blk: {
                        for (mappings_state_machine) |type_mapping| {
                            if (type_mapping[0] == tb.Account) {
                                break :blk type_mapping[1].get_rb_data_type_ptr();
                            }
                        }
                    };
                    var i: usize = 0;
                    while (i < data_array_len) : (i += 1) {
                        const rb_account = ruby.rb_ary_entry(rb_data, @intCast(i));
                        const accounts_result: *tb.Account = @ptrCast(@alignCast(ruby.rb_check_typeddata(rb_account, rb_data_account_t)));
                        const dest_slice = data_alloc[i * @sizeOf(tb.Account) .. (i + 1) * @sizeOf(tb.Account)];
                        @memcpy(dest_slice, std.mem.asBytes(accounts_result));
                    }
                },
                else => {
                    var buf: [256]u8 = undefined;
                    const msg = std.fmt.bufPrintZ(&buf, "Unsupported operation for data parsing: {d}", .{@intFromEnum(operation)}) catch {
                        ruby.rb_raise(ruby.rb_eArgError, "Unsupported operation for data parsing");
                        return;
                    };
                    ruby.rb_raise(ruby.rb_eArgError, msg.ptr);
                    return;
                },
            }
        }
    };
}
