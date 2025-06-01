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

const global_allocator = std.heap.c_allocator;

// Structure to hold completion callback context for operations
const OperationContext = struct {
    packet: exports.tb_packet_t,
    result_data: ?[]u8 = null,
    result_len: u32 = 0,
    completed: bool = false,
};

// Global map to track operations by user_data
var operations_map = std.AutoHashMap(usize, *OperationContext).init(global_allocator);
var operations_mutex = std.Thread.Mutex{};
var user_data_counter: std.atomic.Value(usize) = std.atomic.Value(usize).init(1);

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
        _ = ruby.rb_define_method(rb_class, "submit", submit, -1);
    }
}

// Global completion callback that dispatches based on packet user_data
fn on_completion(
    completion_ctx: usize,
    packet: *exports.tb_packet_t,
    timestamp: u64,
    result_ptr: [*]const u8,
    result_len: u32,
) callconv(.C) void {
    _ = timestamp;
    _ = completion_ctx; // This is the client context passed during init

    // Look up the operation context from our map
    operations_mutex.lock();
    const context_opt = operations_map.get(packet.user_data);
    operations_mutex.unlock();

    if (context_opt) |context| {
        // Copy packet status
        context.packet.status = packet.status;

        // Copy result data if any
        if (result_len > 0 and packet.status == @intFromEnum(exports.tb_packet_status.ok)) {
            context.result_data = global_allocator.alloc(u8, result_len) catch null;
            if (context.result_data) |data| {
                @memcpy(data, result_ptr[0..result_len]);
                context.result_len = result_len;
            }
        }

        // Mark completion
        @atomicStore(bool, &context.completed, true, .release);
    }
}

// Submit method for client operations
fn submit(argc: c_int, argv: [*]ruby.VALUE, self: ruby.VALUE) callconv(.C) ruby.VALUE {
    if (argc != 2) {
        ruby.rb_raise(ruby.rb_eArgError, "wrong number of arguments (expected 2: operation, data)");
        return ruby.Qnil;
    }

    const rb_operation = argv[0];
    const rb_data = argv[1];

    // Get operation enum value
    ruby.Check_Type(rb_operation, ruby.T_FIXNUM);
    const operation = @as(exports.tb_operation, @enumFromInt(ruby.NUM2INT(rb_operation)));

    // Check that data is an array
    ruby.Check_Type(rb_data, ruby.T_ARRAY);
    const data_len = ruby.RARRAY_LEN(rb_data);

    if (data_len == 0) {
        return ruby.rb_ary_new(); // Return empty array for empty input
    }

    // Get the client pointer using the same rb_data_type from init
    const client_data_type = converter.to_ruby_class(exports.tb_client_t);
    const client: *exports.tb_client_t = @ptrCast(@alignCast(ruby.rb_check_typeddata(self, &client_data_type.rb_data_type)));

    // Determine data size based on operation
    const item_size = switch (operation) {
        .create_accounts => @sizeOf(tb.Account),
        .create_transfers => @sizeOf(tb.Transfer),
        .lookup_accounts => @sizeOf(u128),
        .lookup_transfers => @sizeOf(u128),
        .get_account_transfers => @sizeOf(tb.AccountFilter),
        .get_account_balances => @sizeOf(tb.AccountFilter),
        .query_accounts => @sizeOf(tb.QueryFilter),
        .query_transfers => @sizeOf(tb.QueryFilter),
        else => {
            ruby.rb_raise(ruby.rb_eArgError, "Invalid operation");
            return ruby.Qnil;
        },
    };

    // Allocate buffer for input data
    const data_size = data_len * item_size;
    const data_buffer = global_allocator.alloc(u8, data_size) catch {
        ruby.rb_raise(ruby.rb_eNoMemError, "Failed to allocate data buffer");
        return ruby.Qnil;
    };
    defer global_allocator.free(data_buffer);

    // Convert Ruby objects to C structs
    for (0..data_len) |i| {
        const rb_item = ruby.rb_ary_entry(rb_data, @intCast(i));
        const item_ptr = data_buffer.ptr + (i * item_size);

        switch (operation) {
            .create_accounts, .lookup_accounts, .query_accounts => {
                // For account operations, convert Ruby Account to C struct
                // This assumes the Ruby object has the appropriate structure
                _ = rb_item;
                _ = item_ptr;
                // TODO: Implement conversion based on operation type
            },
            .create_transfers, .lookup_transfers, .query_transfers => {
                // For transfer operations
                _ = rb_item;
                _ = item_ptr;
                // TODO: Implement conversion based on operation type
            },
            .get_account_transfers, .get_account_balances => {
                // For filter operations
                _ = rb_item;
                _ = item_ptr;
                // TODO: Implement conversion based on operation type
            },
            else => unreachable,
        }
    }

    // Create operation context on heap so it remains valid during async operation
    const context = global_allocator.create(OperationContext) catch {
        ruby.rb_raise(ruby.rb_eNoMemError, "Failed to allocate operation context");
        return ruby.Qnil;
    };
    defer global_allocator.destroy(context);

    // Generate unique user_data
    const user_data = user_data_counter.fetchAdd(1, .monotonic);

    context.* = OperationContext{
        .packet = .{
            .next = null,
            .user_data = user_data,
            .status = @intFromEnum(exports.tb_packet_status.ok),
            .operation = @intFromEnum(operation),
            .data_size = @intCast(data_size),
            .data = data_buffer.ptr,
        },
        .result_data = null,
        .result_len = 0,
        .completed = false,
    };

    // Register the operation in our map
    operations_mutex.lock();
    operations_map.put(user_data, context) catch {
        operations_mutex.unlock();
        ruby.rb_raise(ruby.rb_eNoMemError, "Failed to register operation");
        return ruby.Qnil;
    };
    operations_mutex.unlock();
    defer {
        operations_mutex.lock();
        _ = operations_map.remove(user_data);
        operations_mutex.unlock();
    }

    // Submit the operation
    const submit_status = exports.submit(client, &context.packet);

    if (submit_status != .ok) {
        ruby.rb_raise(ruby.rb_eRuntimeError, "Failed to submit operation");
        return ruby.Qnil;
    }

    // Wait for completion
    while (!@atomicLoad(bool, &context.completed, .acquire)) {
        std.time.sleep(100_000); // 0.1ms
    }
    defer if (context.result_data) |data| global_allocator.free(data);

    // Check for errors
    if (context.packet.status != @intFromEnum(exports.tb_packet_status.ok)) {
        const error_msg = switch (@as(exports.tb_packet_status, @enumFromInt(context.packet.status))) {
            .too_much_data => "Too much data",
            .invalid_operation => "Invalid operation",
            .invalid_data_size => "Invalid data size",
            .out_of_memory => "Out of memory",
            else => "Unknown error",
        };
        ruby.rb_raise(ruby.rb_eRuntimeError, "Operation failed: %s", error_msg);
        return ruby.Qnil;
    }

    // Process results
    const result_array = ruby.rb_ary_new();

    if (context.result_data) |result_data| {
        // Determine result item size based on operation
        const result_item_size = switch (operation) {
            .create_accounts => @sizeOf(tb.CreateAccountResult),
            .create_transfers => @sizeOf(tb.CreateTransferResult),
            .lookup_accounts => @sizeOf(tb.Account),
            .lookup_transfers => @sizeOf(tb.Transfer),
            .get_account_transfers, .query_transfers => @sizeOf(tb.Transfer),
            .get_account_balances => @sizeOf(tb.AccountBalance),
            .query_accounts => @sizeOf(tb.Account),
            else => unreachable,
        };

        const result_count = context.result_len / result_item_size;

        for (0..result_count) |i| {
            const item_ptr = result_data.ptr + (i * result_item_size);
            // TODO: Convert C struct to Ruby object based on operation type
            _ = item_ptr;
            // For now, just add a placeholder
            ruby.rb_ary_push(result_array, ruby.Qnil);
        }
    }

    return result_array;
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

            const status = exports.init(
                client,
                &cluster_id_bytes,
                @ptrCast(ruby.RSTRING_PTR(rb_addresses)),
                @intCast(ruby.RSTRING_LEN(rb_addresses)),
                @intFromPtr(client), // Pass client pointer as context
                on_completion,
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

            // TigerBeetle init is synchronous, no need to wait

            return self; // Return self to allow method chaining.
        }
    };
}
