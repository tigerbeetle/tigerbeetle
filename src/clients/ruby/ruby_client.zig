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

const ruby = @cImport(@cInclude("ruby.h"));

const MAX_BATCH_SIZE = 8192;

const SKIP_PREFIXES = [_][]const u8{ "reserved", "opaque", "deprecated" };

const Operation = exports.tb_operation;

const mappings_vsr = .{
    .{ Operation, build_rb_setup_struct(Operation, "Operation") },
    .{ exports.tb_packet_status, build_rb_setup_struct(exports.tb_packet_status, "PacketStatus") },
    .{ exports.tb_client_t, build_rb_setup_struct(exports.tb_client_t, "Client") },
    .{ exports.tb_init_status, build_rb_setup_struct(exports.tb_init_status, "InitStatus") },
    .{ exports.tb_client_status, build_rb_setup_struct(exports.tb_client_status, "ClientStatus") },
    .{ exports.tb_log_level, build_rb_setup_struct(exports.tb_log_level, "LogLevel") },
    .{ exports.tb_register_log_callback_status, build_rb_setup_struct(exports.tb_register_log_callback_status, "RegisterLogCallbackStatus") },
};

const mappings_state_machine = .{
    .{ exports.tb_account_t, build_rb_setup_struct(exports.tb_account_t, "Account") },
    .{ exports.tb_transfer_t, build_rb_setup_struct(exports.tb_transfer_t, "Transfer") },
    .{ exports.tb_account_flags, build_rb_setup_struct(exports.tb_account_flags, "AccountFlags") },
    .{ exports.tb_transfer_flags, build_rb_setup_struct(exports.tb_transfer_flags, "TransferFlags") },
    .{ exports.tb_create_account_result, build_rb_setup_struct(exports.tb_create_account_result, "CreateAccountResult") },
    .{ exports.tb_create_transfer_result, build_rb_setup_struct(exports.tb_create_transfer_result, "CreateTransferResult") },
    .{ exports.tb_create_accounts_result_t, build_rb_setup_struct(exports.tb_create_accounts_result_t, "CreateAccountsResult") },
    .{ exports.tb_create_transfers_result_t, build_rb_setup_struct(exports.tb_create_transfers_result_t, "CreateTransfersResult") },
    .{ exports.tb_account_filter_t, build_rb_setup_struct(exports.tb_account_filter_t, "AccountFilter") },
    .{ exports.tb_account_filter_flags, build_rb_setup_struct(exports.tb_account_filter_flags, "AccountFilterFlags") },
    .{ exports.tb_account_balance_t, build_rb_setup_struct(exports.tb_account_balance_t, "AccountBalance") },
    .{ exports.tb_query_filter_t, build_rb_setup_struct(exports.tb_query_filter_t, "QueryFilter") },
    .{ exports.tb_query_filter_flags, build_rb_setup_struct(exports.tb_query_filter_flags, "QueryFilterFlags") },
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
                    @field(wrapper.*, field_name) = rb_value_to_zig_type(FieldType, value) catch {
                        // We have already triggered the error, just need to return for the compiler
                        return ruby.Qnil;
                    };
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

fn type_mapping_from_zig_type(ZigType: type) type {
    return comptime blk: {
        for (mappings_all) |type_mapping| {
            if (type_mapping[0] == ZigType) {
                break :blk type_mapping[1];
            }
        }
        @compileError("No mappings found for " ++ @typeName(ZigType));
    };
}

const ResultContext = struct {
    mutex: std.Thread.Mutex = std.Thread.Mutex{},
    condition: std.Thread.Condition = std.Thread.Condition{},

    result_data: ?[]u8 = null,
    result_len: u32 = 0,
    waiting: bool = true,
    operation: Operation,
    result_error: ?Error = null,
};

fn tb_client_struct() type {
    const Client = exports.tb_client_t;
    const Packet = exports.tb_packet_t;

    const rb_client_type_t: *const ruby.rb_data_type_t = comptime type_mapping_from_zig_type(Client).get_rb_data_type_ptr();
    const c_allocator = std.heap.c_allocator;

    return struct {
        pub fn init_methods(rb_client: ruby.VALUE) void {
            _ = ruby.rb_define_method(rb_client, "init", @ptrCast(&init), 2);
            _ = ruby.rb_define_method(rb_client, "deinit", @ptrCast(&deinit), 0);
            _ = ruby.rb_define_method(rb_client, "submit", @ptrCast(&submit), 2);
        }

        fn on_completion(
            completion_ctx: usize,
            packet: *Packet,
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
                const data_alloc = c_allocator.alloc(u8, result_len) catch {
                    result_context.result_error = error.OutOfMemory;
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

            const cluster_id: [16]u8 = @bitCast(rb_int_to_u128(rb_cluster_id) catch {
                // rb_int_to_u128 will raise the ruby error if it fails
                return ruby.Qnil;
            });
            if (!ruby.RB_TYPE_P(self, ruby.T_DATA)) {
                ruby.rb_raise(ruby.rb_eTypeError, "Expected a Client object");
                return ruby.Qnil;
            }

            const client: *Client = @ptrCast(@alignCast(ruby.rb_check_typeddata(self, rb_client_type_t)));

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

            const client: *Client = @ptrCast(@alignCast(ruby.rb_check_typeddata(self, rb_client_type_t)));

            const status = exports.deinit(client);
            return ruby.INT2NUM(@intFromEnum(status));
        }

        const OperationParsers = .{
            .{ .operation = Operation.lookup_accounts, .parser = create_parser(u128, exports.tb_account_t) },
            .{ .operation = Operation.create_accounts, .parser = create_parser(exports.tb_account_t, exports.tb_create_accounts_result_t) },
            .{ .operation = Operation.lookup_transfers, .parser = create_parser(u128, exports.tb_transfer_t) },
            .{ .operation = Operation.create_transfers, .parser = create_parser(exports.tb_transfer_t, exports.tb_create_transfers_result_t) },
        };

        fn get_parser(operation: Operation) error{UnsupportedOp}!Parser {
            inline for (OperationParsers) |op| {
                if (op.operation == operation) {
                    return op.parser;
                }
            }
            return error.UnsupportedOp;
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

            const operation_int: u8 = @intCast(ruby.NUM2INT(rb_operation));
            const operation_enum: Operation = @enumFromInt(operation_int);

            const parser = get_parser(operation_enum) catch {
                ruby.rb_raise(ruby.rb_eArgError, "Unsupported operation: %d", operation_int);
                return ruby.Qnil;
            };

            const parsed_data: ParsedData = parser.from_ruby(c_allocator, rb_data) catch {
                // #from_ruby will raise the ruby error if it fails
                return ruby.Qnil;
            };
            defer c_allocator.free(parsed_data.data.?[0..parsed_data.size]);

            const client: *Client = @ptrCast(@alignCast(ruby.rb_check_typeddata(self, rb_client_type_t)));

            var result_context = ResultContext{
                .operation = operation_enum,
            };

            var packet = Packet{
                .user_data = @ptrCast(&result_context),
                .data = @constCast(@ptrCast(parsed_data.data)),
                .data_size = parsed_data.size,
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
                    Error.OutOfMemory => ruby.rb_raise(ruby.rb_eRuntimeError, "Out of memory while processing request"),
                    else => unreachable,
                }
                return ruby.Qnil;
            }

            return parser.to_ruby(&result_context);
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

fn rb_int_to_u128(value: ruby.VALUE) Error!u128 {
    if (!ruby.RB_INTEGER_TYPE_P(value)) {
        ruby.rb_raise(ruby.rb_eArgError, "expected Integer");
        return Error.ArgError;
    }

    if (ruby.RTEST(ruby.rb_funcall(value, ruby.rb_intern("negative?"), 0))) {
        ruby.rb_raise(ruby.rb_eRangeError, "negative numbers not supported");
        return Error.ArgError;
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
        return Error.ArgError;
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

fn rb_value_to_uint(comptime T: type, value: ruby.VALUE) Error!T {
    if (@typeInfo(T) != .Int) {
        @compileError("Expected an integer type for Ruby conversion, got: " ++ @typeInfo(T));
    }

    return switch (T) {
        u8 => @intCast(ruby.NUM2UINT(value)),
        u16 => @intCast(ruby.NUM2UINT(value)),
        u32 => ruby.NUM2UINT(value),
        u64 => @intCast(ruby.NUM2ULONG(value)),
        u128 => try rb_int_to_u128(value),
        else => @compileError("Unsupported integer type: " ++ @typeName(T)),
    };
}

fn rb_value_to_zig_type(comptime T: type, value: ruby.VALUE) Error!T {
    return switch (@typeInfo(T)) {
        .Int => try rb_value_to_uint(T, value),
        .Enum => |info| {
            const tag_type = info.tag_type;
            return switch (@typeInfo(tag_type)) {
                .Int => {
                    const int_value = try rb_value_to_uint(tag_type, value);
                    return @enumFromInt(int_value);
                },
                else => @compileError("Unsupported enum type: " ++ @typeName(tag_type)),
            };
        },
        .Struct => |info| {
            if (info.layout == .@"packed") {
                if (info.backing_integer) |backing_type| {
                    const int_value = try rb_value_to_uint(backing_type, value);
                    return @bitCast(int_value);
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

const Error = error{
    ArgError,
    OutOfMemory,
    UnsupportedOp,
};

const ParsedData = struct {
    size: u32,
    data: ?[*]const u8 = null,
};

const Parser = struct {
    from_ruby: *const fn (std.mem.Allocator, ruby.VALUE) Error!ParsedData,
    to_ruby: *const fn (*ResultContext) ruby.VALUE,
};

fn create_parser(comptime InputType: type, comptime OutputType: type) Parser {
    const impl = struct {
        fn from_ruby(allocator: std.mem.Allocator, rb_data: ruby.VALUE) Error!ParsedData {
            if (ruby.NIL_P(rb_data) or !ruby.RB_TYPE_P(rb_data, ruby.T_ARRAY)) {
                ruby.rb_raise(ruby.rb_eArgError, "data must be a non-nil Array object");
                return Error.ArgError;
            }

            const data_array_len = ruby.RARRAY_LEN(rb_data);
            if (data_array_len == 0) {
                ruby.rb_raise(ruby.rb_eArgError, "data array cannot be empty");
                return Error.ArgError;
            } else if (data_array_len > MAX_BATCH_SIZE) {
                ruby.rb_raise(ruby.rb_eArgError, "data array size exceeds maximum allowed size of {}", .{MAX_BATCH_SIZE});
                return Error.ArgError;
            }

            var out_data: ParsedData = .{
                .size = 0,
                .data = null,
            };
            out_data.size = @intCast(data_array_len * @sizeOf(InputType));
            const data_block = allocator.alloc(u8, out_data.size) catch {
                ruby.rb_raise(ruby.rb_eRuntimeError, "Failed to allocate memory for data");
                return Error.OutOfMemory;
            };
            out_data.data = data_block.ptr;

            switch (@typeInfo(InputType)) {
                .Int => {
                    var i: usize = 0;
                    while (i < data_array_len) : (i += 1) {
                        const rb_item = ruby.rb_ary_entry(rb_data, @intCast(i));
                        const zig_value: InputType = try rb_value_to_uint(InputType, rb_item);

                        const dest_slice = data_block[i * @sizeOf(InputType) .. (i + 1) * @sizeOf(InputType)];
                        @memcpy(dest_slice, std.mem.asBytes(&zig_value));
                    }
                },
                .Struct => {
                    const rb_type_t: *const ruby.rb_data_type_t = comptime type_mapping_from_zig_type(InputType).get_rb_data_type_ptr();
                    var i: usize = 0;
                    while (i < data_array_len) : (i += 1) {
                        const rb_item = ruby.rb_ary_entry(rb_data, @intCast(i));
                        const zig_item: *InputType = @ptrCast(@alignCast(ruby.rb_check_typeddata(rb_item, rb_type_t)));
                        const dest_slice = data_block[i * @sizeOf(InputType) .. (i + 1) * @sizeOf(InputType)];
                        @memcpy(dest_slice, std.mem.asBytes(zig_item));
                    }
                },
                else => @compileError("Unable to handle type " ++ @typeName(InputType)),
            }

            return out_data;
        }

        fn to_ruby(ctx: *ResultContext) ruby.VALUE {
            if (ctx.result_len == 0) {
                return ruby.rb_ary_new();
            }

            if (ctx.result_data == null) {
                @panic("Result data is null, expected non-null data for Ruby conversion");
            }

            const result = ctx.result_data.?;
            const m_tiger_beetle = ruby.rb_const_get(ruby.rb_cObject, ruby.rb_intern("TigerBeetle"));
            const m_bindings = ruby.rb_const_get(m_tiger_beetle, ruby.rb_intern("Bindings"));

            const rb_type_struct = comptime type_mapping_from_zig_type(OutputType);
            const rb_class_name = rb_type_struct.rb_class_name;
            const rb_class = ruby.rb_const_get(m_bindings, ruby.rb_intern(&rb_class_name[0]));

            const result_size = ctx.result_len / @sizeOf(OutputType);
            const rb_result_array = ruby.rb_ary_new2(@intCast(result_size));

            const rb_data_t: *const ruby.rb_data_type_t = rb_type_struct.get_rb_data_type_ptr();

            for (0..result_size) |i| {
                const rb_instance = ruby.rb_class_new_instance(0, null, rb_class);

                const data_block = result[i * @sizeOf(OutputType) .. (i + 1) * @sizeOf(OutputType)];
                const data: *OutputType = @ptrCast(@alignCast(ruby.rb_check_typeddata(rb_instance, rb_data_t)));

                @memcpy(@as([*]u8, @ptrCast(data))[0..@sizeOf(OutputType)], data_block);

                _ = ruby.rb_ary_push(rb_result_array, rb_instance);
            }

            std.debug.print("Returning Ruby array with {d} items\n", .{result_size});
            return rb_result_array;
        }
    };

    return Parser{
        .from_ruby = impl.from_ruby,
        .to_ruby = impl.to_ruby,
    };
}
