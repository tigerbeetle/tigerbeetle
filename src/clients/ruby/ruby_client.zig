const std = @import("std");
const vsr = @import("vsr");

const tb_client = vsr.tb_client;
const exports = tb_client.exports;
const tb_packet_t = exports.tb_packet_t;

const assert = std.debug.assert;

const constants = vsr.constants;
const IO = vsr.io.IO;

const Tracer = vsr.trace.TracerType(vsr.time.Time);
const Storage = vsr.storage.StorageType(IO, Tracer);
const StateMachine = vsr.state_machine.StateMachineType(Storage, constants.state_machine_config);
const tb = vsr.tigerbeetle;

const ruby = @cImport(@cInclude("ruby.h"));

const global_allocator = std.heap.c_allocator;

const MAX_BATCH_SIZE = 8192;

const SKIP_PREFIXES = [_][]const u8{ "reserved", "opaque", "deprecated" };

const mappings_vsr = .{
    .{ exports.tb_operation, build_rb_setup_struct(exports.tb_operation, "Operation") },
    .{ exports.tb_packet_status, build_rb_setup_struct(exports.tb_packet_status, "PacketStatus") },
    .{ exports.tb_client_t, build_rb_setup_struct(exports.tb_client_t, "Client") },
    .{ exports.tb_init_status, build_rb_setup_struct(exports.tb_init_status, "InitStatus") },
    .{ exports.tb_client_status, build_rb_setup_struct(exports.tb_client_status, "ClientStatus") },
    .{ exports.tb_log_level, build_rb_setup_struct(exports.tb_log_level, "LogLevel") },
    .{ exports.tb_register_log_callback_status, build_rb_setup_struct(exports.tb_register_log_callback_status, "RegisterLogCallbackStatus") },
};

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

    const rb_client = ruby.rb_const_get(m_bindings, ruby.rb_intern("Client"));
    tb_client_struct().init_methods(rb_client);
}

fn convert_to_ruby_class(comptime ZigType: type, comptime ruby_name: []const u8) type {
    if (@typeInfo(ZigType) != .Struct) {
        @compileError("Expected a struct type for Ruby C struct conversion, got: " ++ @typeInfo(ZigType));
    }

    return struct {
        const Self = @This();
        const type_info = @typeInfo(ZigType);
        const type_name = @typeName(ZigType) ++ "\x00";

        pub const rb_class_name = ruby_name ++ "\x00";

        const rb_data_type = ruby.rb_data_type_t{
            .wrap_struct_name = &type_name[0],
            .function = .{
                .dmark = null,
                .dfree = free_fn,
                .dsize = size_fn,
            },
            .data = null,
            .flags = ruby.RUBY_TYPED_FREE_IMMEDIATELY,
        };

        pub fn get_rb_data_type_ptr() *const ruby.rb_data_type_t {
            return &rb_data_type;
        }

        fn alloc_fn(self: ruby.VALUE) callconv(.C) ruby.VALUE {
            return ruby.rb_data_typed_object_zalloc(self, @sizeOf(ZigType), &rb_data_type);
        }

        fn free_fn(ptr: ?*anyopaque) callconv(.C) void {
            if (ptr) |p| {
                ruby.xfree(p);
            }
        }

        fn size_fn(ptr: ?*const anyopaque) callconv(.C) usize {
            _ = ptr;
            return @sizeOf(ZigType);
        }

        fn convert_to_ruby_hash(self: ruby.VALUE) callconv(.C) ruby.VALUE {
            const wrapper: *ZigType = @ptrCast(@alignCast(ruby.rb_check_typeddata(self, &rb_data_type)));
            const hash: ruby.VALUE = ruby.rb_hash_new();

            inline for (type_info.Struct.fields) |field| {
                if (comptime skip_field(field.name)) {
                    continue;
                }
                const field_value = @field(wrapper.*, field.name);
                const field_name_cstr = field.name ++ "\x00";
                const key_id = ruby.rb_intern(&field_name_cstr[0]);
                const key_symbol = ruby.ID2SYM(key_id); // Convert ID to symbol

                const value = zig_type_to_rb_value(@TypeOf(field_value), field_value);

                _ = ruby.rb_hash_aset(hash, key_symbol, value);
            }

            return hash;
        }

        pub fn init_methods(parent: ruby.VALUE) void {
            const class = ruby.rb_define_class_under(parent, &rb_class_name[0], ruby.rb_cObject);
            ruby.rb_define_alloc_func(class, alloc_fn);
            ruby.rb_define_method(class, "initialize", @ptrCast(&rb_initialize), -1);
            ruby.rb_define_method(class, "to_h", @ptrCast(&convert_to_ruby_hash), 0);

            define_getters_and_setters(class);
        }

        fn define_getters_and_setters(class: ruby.VALUE) void {
            const fields = type_info.Struct.fields;

            inline for (fields) |field| {
                if (comptime skip_field(field.name)) {
                    continue;
                }

                const getter_name = field.name ++ "\x00";
                const setter_name = field.name ++ "=\x00";

                ruby.rb_define_method(class, &getter_name[0], @ptrCast(&make_getter_fn(field.name)), 0);
                ruby.rb_define_method(class, &setter_name[0], @ptrCast(&make_setter_fn(field.name)), 1);
            }
        }

        // This defines the Object.new function
        fn rb_initialize(argc: c_int, argv: [*]ruby.VALUE, self: ruby.VALUE) callconv(.C) ruby.VALUE {
            _ = ruby.rb_check_typeddata(self, &rb_data_type);

            var kwargs: ruby.VALUE = ruby.Qnil;
            _ = ruby.rb_scan_args(argc, argv, ":", &kwargs);
            if (ruby.NIL_P(kwargs)) {
                return self;
            }

            _ = ruby.rb_hash_foreach(kwargs, initialize_keyword_parameter_value, self);

            return self;
        }

        // ruby getter function to fetch the value fromt he tb struct and return it as a ruby value
        fn make_getter_fn(comptime field_name: []const u8) fn (ruby.VALUE) callconv(.C) ruby.VALUE {
            return struct {
                fn get(self: ruby.VALUE) callconv(.C) ruby.VALUE {
                    const wrapper: *ZigType = @ptrCast(@alignCast(ruby.rb_check_typeddata(self, &rb_data_type)));
                    const field_value = @field(wrapper.*, field_name);
                    return zig_type_to_rb_value(@TypeOf(field_value), field_value);
                }
            }.get;
        }

        // ruby setter function to set the value in the tb struct from a ruby value
        fn make_setter_fn(comptime field_name: []const u8) fn (ruby.VALUE, ruby.VALUE) callconv(.C) ruby.VALUE {
            return struct {
                fn set(self: ruby.VALUE, value: ruby.VALUE) callconv(.C) ruby.VALUE {
                    const wrapper: *ZigType = @ptrCast(@alignCast(ruby.rb_check_typeddata(self, &rb_data_type)));
                    const FieldType = @TypeOf(@field(wrapper.*, field_name));
                    @field(wrapper.*, field_name) = rb_value_to_zig_type(FieldType, value);
                    return value;
                }
            }.set;
        }

        fn initialize_keyword_parameter_value(key: ruby.VALUE, value: ruby.VALUE, self: ruby.VALUE) callconv(.C) c_int {
            if (!ruby.SYMBOL_P(key)) {
                const key_class = ruby.rb_funcall(key, ruby.rb_intern("class"), 0);
                const class_str = ruby.rb_funcall(key_class, ruby.rb_intern("to_s"), 0);
                ruby.rb_raise(ruby.rb_eArgError, "keyword arguments must use symbol keys, got %s", ruby.RSTRING_PTR(class_str));
                return 1;
            }

            const key_str = ruby.rb_sym2str(key);
            const setter_str = ruby.rb_str_plus(key_str, ruby.rb_str_new2("="));
            const setter_cstr = ruby.RSTRING_PTR(setter_str);
            const setter_id = ruby.rb_intern(setter_cstr);

            if (ruby.rb_respond_to(self, setter_id) == 0) {
                ruby.rb_raise(ruby.rb_eNoMethodError, "undefined method '%s' for object", setter_cstr);
                return 1;
            }

            _ = ruby.rb_funcall(self, setter_id, 1, value);
            return 0;
        }
    };
}

fn convert_to_module_enum(comptime ZigType: type, comptime ruby_name: []const u8) type {
    if (@typeInfo(ZigType) != .Enum and @typeInfo(ZigType) != .Struct) {
        @compileError("Expected an enum or struct type for Ruby module conversion, got: " ++ @typeInfo(ZigType));
    }

    return struct {
        const Self = @This();
        const class_name = ruby_name ++ "\x00";

        pub fn init_methods(module: ruby.VALUE) void {
            const ruby_enum = ruby.rb_define_module_under(module, &class_name[0]);

            switch (@typeInfo(ZigType)) {
                .Enum => |enum_info| {
                    _ = ruby.rb_define_const(ruby_enum, "ENUM_PACKED", ruby.Qfalse);

                    inline for (enum_info.fields) |field| {
                        if (comptime skip_field(field.name)) {
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
                    if (layout != .@"packed") {
                        @compileError("Only packed structs can be converted to Ruby enums: " ++ @typeName(ZigType));
                    }
                    _ = ruby.rb_define_const(ruby_enum, "ENUM_PACKED", ruby.Qtrue);

                    inline for (struct_info.fields, 0..) |field, i| {
                        if (comptime skip_field(field.name)) {
                            continue;
                        }
                        const ruby_const_name = to_upper_case(field.name);
                        _ = ruby.rb_define_const(ruby_enum, &ruby_const_name, ruby.UINT2NUM(1 << i));
                    }
                },
                else => @compileError("Invalid conversion to enum: " ++ ZigType),
            }
        }
    };
}

fn to_upper_case(comptime input: []const u8) [input.len + 1:0]u8 {
    var result: [input.len + 1:0]u8 = undefined;
    _ = std.ascii.upperString(result[0..input.len], input);
    result[input.len] = 0; // null terminator
    return result;
}

fn build_rb_setup_struct(comptime ZigType: type, comptime ruby_name: []const u8) type {
    return switch (@typeInfo(ZigType)) {
        .Enum => {
            return convert_to_module_enum(ZigType, ruby_name);
        },
        .Struct => |info| switch (info.layout) {
            .@"packed" => {
                return convert_to_module_enum(ZigType, ruby_name);
            },
            .@"extern" => {
                return convert_to_ruby_class(ZigType, ruby_name);
            },
            else => @compileError("Unsupported struct layout: " ++ info),
        },
        else => @compileError("Unsupported Zig type for Ruby mapping: " ++ @typeInfo(ZigType)),
    };
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
        pub fn init_methods(rb_client: ruby.VALUE) void {
            _ = ruby.rb_define_method(rb_client, "init", @ptrCast(&init), 2);
            _ = ruby.rb_define_method(rb_client, "deinit", @ptrCast(&deinit), 0);
            _ = ruby.rb_define_method(rb_client, "submit", @ptrCast(&submit), 2);
        }

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

            const cluster_id: [16]u8 = @bitCast(rb_int_to_u128(rb_cluster_id));

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

        fn deinit(self: ruby.VALUE) callconv(.C) ruby.VALUE {
            if (!ruby.RB_TYPE_P(self, ruby.T_DATA)) {
                ruby.rb_raise(ruby.rb_eTypeError, "Expected a Client object");
                return ruby.Qnil;
            }

            const client: *exports.tb_client_t = @ptrCast(@alignCast(ruby.rb_check_typeddata(self, rb_client_type_t)));

            const status = exports.deinit(client);
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
                        const account_id = rb_int_to_u128(rb_account_id);

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

fn skip_field(comptime field_name: []const u8) bool {
    inline for (SKIP_PREFIXES) |prefix| {
        if (comptime std.mem.startsWith(u8, field_name, prefix)) {
            return true;
        }
    }
    return false;
}

fn rb_int_to_u128(value: ruby.VALUE) u128 {
    if (!ruby.RB_INTEGER_TYPE_P(value)) {
        ruby.rb_raise(ruby.rb_eTypeError, "expected Integer");
        return 0;
    }

    if (ruby.RTEST(ruby.rb_funcall(value, ruby.rb_intern("negative?"), 0))) {
        ruby.rb_raise(ruby.rb_eRangeError, "negative numbers not supported");
        return 0;
    }

    var result: u128 = 0;
    const overflow = ruby.rb_integer_pack(value, // val: the Ruby VALUE
        &result, // words: pointer to output buffer
        1, // numwords: we want 1 chunk of 16 bytes
        @sizeOf(u128), // wordsize: 16 bytes
        0, // nails: 0 (no nail bits)
        ruby.INTEGER_PACK_NATIVE_BYTE_ORDER // flags
    );

    if (overflow == 2) {
        ruby.rb_raise(ruby.rb_eRangeError, "integer too big for u128");
        return 0;
    }

    return result;
}

fn u128_to_rb_int(value: u128) ruby.VALUE {
    if (value == 0) {
        return ruby.UINT2NUM(0);
    }

    return ruby.rb_integer_unpack(&value, // ruby value
        1, // numwords: we want 1 chunk of 16 bytes
        @sizeOf(u128), // wordsize: 16 bytes
        0, // nails: 0 (no nail bits)
        ruby.INTEGER_PACK_NATIVE_BYTE_ORDER // flags
    );
}

fn uint_to_rb_value(comptime T: type, value: T) ruby.VALUE {
    if (@typeInfo(T) != .Int) {
        @compileError("Expected an integer type for Ruby conversion, got: " ++ @typeInfo(T));
    }

    return switch (T) {
        u8, u16, u32 => ruby.UINT2NUM(value),
        u64 => ruby.ULONG2NUM(value),
        u128 => u128_to_rb_int(value),
        else => @compileError("Unsupported integer size: " ++ @typeName(T)),
    };
}

fn zig_type_to_rb_value(comptime T: type, value: T) ruby.VALUE {
    return switch (@typeInfo(T)) {
        .Int => uint_to_rb_value(T, value),
        .Enum => |info| {
            const tag_type = info.tag_type;
            return switch (@typeInfo(tag_type)) {
                .Int => {
                    const int_value = @intFromEnum(value);
                    return uint_to_rb_value(tag_type, int_value);
                },
                else => @compileError("Unsupported enum type: " ++ @typeName(tag_type)),
            };
        },
        .Struct => |info| {
            if (info.layout == .@"packed") {
                if (info.backing_integer) |backing_type| {
                    const int_value: backing_type = @bitCast(value);
                    return uint_to_rb_value(backing_type, int_value);
                } else {
                    @compileError("Packed struct has no backing integer type");
                }
            }
            @compileError("Unsupported struct layout for Ruby conversion: " ++ @typeName(T));
        },
        else => {
            @compileError("Unsupported type for Ruby conversion: " ++ @typeName(T));
        },
    };
}

fn rb_value_to_uint(comptime T: type, value: ruby.VALUE) T {
    if (@typeInfo(T) != .Int) {
        @compileError("Expected an integer type for Ruby conversion, got: " ++ @typeInfo(T));
    }

    return switch (T) {
        u8 => @intCast(ruby.NUM2UINT(value)),
        u16 => @intCast(ruby.NUM2UINT(value)),
        u32 => ruby.NUM2UINT(value),
        u64 => @intCast(ruby.NUM2ULONG(value)),
        u128 => rb_int_to_u128(value),
        else => @compileError("Unsupported integer type: " ++ @typeName(T)),
    };
}

fn rb_value_to_zig_type(comptime T: type, value: ruby.VALUE) T {
    return switch (@typeInfo(T)) {
        .Int => rb_value_to_uint(T, value),
        .Enum => |info| {
            const tag_type = info.tag_type;
            return switch (@typeInfo(tag_type)) {
                .Int => {
                    const int_value = rb_value_to_uint(tag_type, value);
                    return @enumFromInt(int_value);
                },
                else => @compileError("Unsupported enum type: " ++ @typeName(tag_type)),
            };
        },
        .Struct => |info| {
            if (info.layout == .@"packed") {
                if (info.backing_integer) |backing_type| {
                    const int_value = rb_value_to_uint(backing_type, value);
                    return @bitCast(int_value);
                } else {
                    @compileError("Packed struct has no backing integer type");
                }
            }
            @compileLog("Unsupported struct layout for Ruby conversion: " ++ @typeName(T));
        },
        else => {
            @compileError("Unsupported type for Ruby conversion: " ++ @typeName(T));
        },
    };
}
