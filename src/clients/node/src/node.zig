const std = @import("std");
const assert = std.debug.assert;
const allocator = std.heap.c_allocator;

const c = @import("c.zig");
const translate = @import("translate.zig");
const tb = struct {
    pub usingnamespace @import("../../../tigerbeetle.zig");
    pub usingnamespace @import("../../c/tb_client.zig");
};

const Account = tb.Account;
const AccountFlags = tb.AccountFlags;
const Transfer = tb.Transfer;
const TransferFlags = tb.TransferFlags;
const CreateAccountsResult = tb.CreateAccountsResult;
const CreateTransfersResult = tb.CreateTransfersResult;

const Storage = @import("../../../storage.zig").Storage;
const StateMachine = @import("../../../state_machine.zig").StateMachineType(Storage, constants.state_machine_config);
const Operation = StateMachine.Operation;
const constants = @import("../../../constants.zig");
const vsr = @import("../../../vsr.zig");

pub const std_options = struct {
    // Since this is running in application space, log only critical messages to reduce noise.
    pub const log_level: std.log.Level = .err;
};

/// N-API will call this constructor automatically to register the module.
export fn napi_register_module_v1(env: c.napi_env, exports: c.napi_value) c.napi_value {
    translate.register_function(env, exports, "init", init) catch return null;
    translate.register_function(env, exports, "deinit", deinit) catch return null;
    translate.register_function(env, exports, "submit", submit) catch return null;
    return exports;
}

// Add-on code

fn init(env: c.napi_env, info: c.napi_callback_info) callconv(.C) c.napi_value {
    var argc: usize = 1;
    var argv: [1]c.napi_value = undefined;
    if (c.napi_get_cb_info(env, info, &argc, &argv, null, null) != c.napi_ok) {
        translate.throw(env, "Failed to get args for init().") catch return null;
    }
    if (argc != 1) {
        translate.throw(env, "Function init() requires exactly 1 argument.") catch return null;
    }

    const cluster = translate.u32_from_object(env, argv[0], "cluster_id") catch return null;
    const concurrency = translate.u32_from_object(env, argv[0], "concurrency") catch return null;
    const addresses = translate.slice_from_object(
        env,
        argv[0],
        "replica_addresses",
    ) catch return null;

    return create(env, cluster, concurrency, addresses) catch null;
}

fn deinit(env: c.napi_env, info: c.napi_callback_info) callconv(.C) c.napi_value {
    var argc: usize = 1;
    var argv: [1]c.napi_value = undefined;
    if (c.napi_get_cb_info(env, info, &argc, &argv, null, null) != c.napi_ok) {
        translate.throw(env, "Failed to get args for deinit().") catch return null;
    }
    if (argc != 1) {
        translate.throw(env, "Function deinit() requires exactly 1 argument.") catch return null;
    }

    destroy(env, argv[0]) catch {};
    return null;
}

fn submit(env: c.napi_env, info: c.napi_callback_info) callconv(.C) c.napi_value {
    var argc: usize = 4;
    var argv: [4]c.napi_value = undefined;
    if (c.napi_get_cb_info(env, info, &argc, &argv, null, null) != c.napi_ok) {
        translate.throw(env, "Failed to get args.") catch return null;
    }
    if (argc != 4) {
        translate.throw(env, "Function submit() requires exactly 4 arguments.") catch return null;
    }

    const operation_int = translate.u32_from_value(env, argv[1], "operation") catch return null;
    if (!@as(vsr.Operation, @enumFromInt(operation_int)).valid(StateMachine)) {
        translate.throw(env, "Unknown operation.") catch return null;
    }

    var is_array: bool = undefined;
    if (c.napi_is_array(env, argv[2], &is_array) != c.napi_ok) {
        translate.throw(env, "Failed to check array argument type.") catch return null;
    }
    if (!is_array) {
        translate.throw(env, "Array argument must be an [object Array].") catch return null;
    }

    var callback_type: c.napi_valuetype = undefined;
    if (c.napi_typeof(env, argv[3], &callback_type) != c.napi_ok) {
        translate.throw(env, "Failed to check callback argument type.") catch return null;
    }
    if (callback_type != c.napi_function) {
        translate.throw(env, "Callback argument must be a Function.") catch return null;
    }

    request(
        env,
        argv[0],
        @enumFromInt(@as(u8, @intCast(operation_int))),
        argv[2],
        argv[3],
    ) catch {};
    return null;
}

// tb_client Logic

fn create(env: c.napi_env, cluster_id: u32, concurrency: u32, addresses: []const u8) !c.napi_value {
    var tsfn_name: c.napi_value = undefined;
    if (c.napi_create_string_utf8(env, "tb_client", c.NAPI_AUTO_LENGTH, &tsfn_name) != c.napi_ok) {
        return translate.throw(env, "Failed to create resource name for thread-safe function.");
    }

    var on_completion_tsfn: c.napi_threadsafe_function = undefined;
    if (c.napi_create_threadsafe_function(
        env,
        null, // No javascript function to call directly from here.
        null, // No async resource.
        tsfn_name,
        0, // Max queue size of 0 means no limit.
        1, // Number of acquires/threads that will be calling this TSFN.
        null, // No finalization data.
        null, // No finalization callback.
        null, // No custom context.
        on_completion_js, // Function to call on JS thread when TSFN is called.
        &on_completion_tsfn, // TSFN out handle.
    ) != c.napi_ok) {
        return translate.throw(env, "Failed to create thread-safe function.");
    }
    errdefer if (c.napi_release_threadsafe_function(
        on_completion_tsfn,
        c.napi_tsfn_abort,
    ) != c.napi_ok) {
        std.log.warn("Failed to release allocated thread-safe function on error.", .{});
    };

    if (c.napi_acquire_threadsafe_function(on_completion_tsfn) != c.napi_ok) {
        return translate.throw(env, "Failed to acquire reference to thread-safe function.");
    }

    const on_completion_ctx = @intFromPtr(on_completion_tsfn);
    const client = tb.init(
        allocator,
        cluster_id,
        addresses,
        concurrency,
        on_completion_ctx,
        on_completion,
    ) catch |err| switch (err) {
        error.OutOfMemory => return translate.throw(env, "Failed to allocate memory for Client."),
        error.Unexpected => return translate.throw(env, "Unexpected error occured on Client."),
        error.AddressInvalid => return translate.throw(env, "Invalid replica address."),
        error.AddressLimitExceeded => return translate.throw(env, "Too many replica addresses."),
        error.ConcurrencyMaxInvalid => return translate.throw(env, "Concurrency is too high."),
        error.SystemResources => return translate.throw(env, "Failed to reserve system resources."),
        error.NetworkSubsystemFailed => return translate.throw(env, "Network stack failure."),
    };
    errdefer tb.deinit(client);

    return try translate.create_external(env, client);
}

fn destroy(env: c.napi_env, context: c.napi_value) !void {
    const client_ptr = try translate.value_external(
        env,
        context,
        "Failed to get client context pointer.",
    );
    const client: tb.tb_client_t = @ptrCast(@alignCast(client_ptr.?));
    defer tb.deinit(client);

    const on_completion_ctx = tb.completion_context(client);
    const on_completion_tsfn: c.napi_threadsafe_function = @ptrFromInt(on_completion_ctx);

    if (c.napi_release_threadsafe_function(on_completion_tsfn, c.napi_tsfn_abort) != c.napi_ok) {
        return translate.throw(env, "Failed to release allocated thread-safe function on error.");
    }
}

fn request(
    env: c.napi_env,
    context: c.napi_value,
    op: Operation,
    array: c.napi_value,
    callback: c.napi_value,
) !void {
    const client_ptr = try translate.value_external(
        env,
        context,
        "Failed to get client context pointer.",
    );
    const client: tb.tb_client_t = @ptrCast(@alignCast(client_ptr.?));

    const packet = blk: {
        var packet_ptr: ?*tb.tb_packet_t = undefined;
        switch (tb.acquire_packet(client, &packet_ptr)) {
            .ok => break :blk packet_ptr.?,
            .shutdown => return translate.throw(env, "Client was prematurely shutdown."),
            .concurrency_max_exceeded => return translate.throw(env, "Too many concurrent requests."),
        }
    };
    errdefer tb.release_packet(client, packet);

    var callback_ref: c.napi_ref = undefined;
    if (c.napi_create_reference(env, callback, 1, &callback_ref) != c.napi_ok) {
        return translate.throw(env, "Failed to create reference to callback.");
    }
    errdefer translate.delete_reference(env, callback_ref) catch {
        std.log.warn("Failed to delete reference to callback on error.", .{});
    };

    const data = switch (op) {
        .create_accounts => try decode_array_into_data(Account, CreateAccountsResult, env, array),
        .create_transfers => try decode_array_into_data(Transfer, CreateTransfersResult, env, array),
        .lookup_accounts => try decode_array_into_data(u128, Account, env, array),
        .lookup_transfers => try decode_array_into_data(u128, Transfer, env, array),
    };

    packet.* = .{
        .next = null,
        .user_data = callback_ref,
        .operation = @intFromEnum(op),
        .status = .ok,
        .data_size = @intCast(data.len),
        .data = data.ptr,
    };

    tb.submit(client, packet);
}

fn on_completion(
    on_completion_ctx: usize,
    client: tb.tb_client_t,
    packet: *tb.tb_packet_t,
    result_ptr: ?[*]const u8,
    result_len: u32,
) callconv(.C) void {
    switch (packet.status) {
        .ok => {},
        .too_much_data => unreachable, // We limit packet data size during request().
        .invalid_operation => unreachable, // We check the operation during request().
        .invalid_data_size => unreachable, // We set correct data size during request().
    }

    // Copy the results given to use into the packet dataa.
    const events = @as([*]align(16) u8, @ptrCast(@alignCast(packet.data.?)))[0..packet.data_size];
    const results = result_ptr.?[0..result_len];
    const result_data = switch (@as(Operation, @enumFromInt(packet.operation))) {
        .create_accounts => encode_results_into_data(Account, CreateAccountsResult, results, events),
        .create_transfers => encode_results_into_data(Transfer, CreateTransfersResult, results, events),
        .lookup_accounts => encode_results_into_data(u128, Account, results, events),
        .lookup_transfers => encode_results_into_data(u128, Transfer, results, events),
    };

    // Update the packet data size with the results and stuff client in packet.next to avoid alloc.
    assert(@intFromPtr(result_data.ptr) == @intFromPtr(events.ptr));
    packet.data_size = @intCast(result_data.len);
    @as(*usize, @ptrCast(&packet.next)).* = @intFromPtr(client);

    // Process the packet on the JS thread.
    const on_completion_tsfn: c.napi_threadsafe_function = @ptrFromInt(on_completion_ctx);
    switch (c.napi_call_threadsafe_function(on_completion_tsfn, packet, c.napi_tsfn_nonblocking)) {
        c.napi_ok => {},
        c.napi_queue_full => @panic("ThreadSafe Function queue is full when created with no limit."),
        else => unreachable,
    }
}

fn on_completion_js(
    env: c.napi_env,
    unused_js_cb: c.napi_value,
    unused_context: ?*anyopaque,
    packet_argument: ?*anyopaque,
) callconv(.C) void {
    _ = unused_js_cb;
    _ = unused_context;

    const packet: *tb.tb_packet_t = @ptrCast(@alignCast(packet_argument.?));
    assert(packet.status == .ok);

    // Extract all the packet information and release it back to the client.
    const client: tb.tb_client_t = @ptrFromInt(@as(*usize, @ptrCast(&packet.next)).*);
    const callback_ref: c.napi_ref = @ptrCast(@alignCast(packet.user_data.?));
    const data = @as([*]align(16) u8, @ptrCast(@alignCast(packet.data.?)))[0..packet.data_size];
    const op: Operation = @enumFromInt(packet.operation);
    tb.release_packet(client, packet);

    // Parse Result array out of packet data, freeing it in the process.
    var callback_error: c.napi_value = null;
    const callback_result = (switch (op) {
        .create_accounts => encode_data_into_array(Account, CreateAccountsResult, env, data),
        .create_transfers => encode_data_into_array(Transfer, CreateTransfersResult, env, data),
        .lookup_accounts => encode_data_into_array(u128, Account, env, data),
        .lookup_transfers => encode_data_into_array(u128, Transfer, env, data),
    }) catch |err| switch (err) {
        error.ExceptionThrown => blk: {
            if (c.napi_get_and_clear_last_exception(env, &callback_error) != c.napi_ok) {
                std.log.warn("Failed to capture callback error from thrown Exception.", .{});
            }
            break :blk @as(c.napi_value, null);
        },
    };

    // Make sure to delete the callback reference once we're done calling it.
    defer if (c.napi_delete_reference(env, callback_ref) != c.napi_ok) {
        std.log.warn("Failed to delete reference to user's JS callback.", .{});
    };

    const scope = translate.scope(env, "Failed to get \"this\" for results callback.") catch return;
    const callback = translate.reference_value(
        env,
        callback_ref,
        "Failed to get callback reference.",
    ) catch return;

    var args = [_]c.napi_value{ callback_error, callback_result };
    translate.call_function(env, scope, callback, @intCast(args.len), &args) catch return;
}

// (De)Serialization

fn decode_array_into_data(
    comptime Event: type,
    comptime Result: type,
    env: c.napi_env,
    array: c.napi_value,
) ![]u8 {
    const array_length = try translate.array_length(env, array);
    if (array_length < 1) {
        return translate.throw(env, "Batch must contain at least one event.");
    }

    // Allocate enough memory to contain the Events and the Results.
    const alloc_size = @max(@sizeOf(Event) * array_length, @sizeOf(Result) * array_length);
    if (@sizeOf(vsr.Header) + alloc_size > constants.message_size_max) {
        return translate.throw(env, "Batch is larger than the maximum message size.");
    }

    const alloc_align = @max(@alignOf(Event), @alignOf(Result));
    comptime assert(alloc_align <= 16); // Assume 16-byte alignment. Makes free() simpler.

    const data = allocator.alignedAlloc(u8, 16, alloc_size) catch |err| switch (err) {
        error.OutOfMemory => return translate.throw(env, "Batch allocation ran out of memory."),
    };
    errdefer allocator.free(data);

    // Parse the event objects from the array.
    const events = std.mem.bytesAsSlice(Event, data[0 .. @sizeOf(Event) * array_length]);
    for (events, 0..) |*event, i| {
        const object = try translate.array_element(env, array, @intCast(i));
        switch (Event) {
            Account, Transfer => {
                inline for (std.meta.fields(Event)) |field| {
                    const FieldInt = switch (@typeInfo(field.type)) {
                        .Struct => |info| info.backing_integer.?,
                        else => field.type,
                    };

                    const value = try @field(translate, @typeName(FieldInt) ++ "_from_object")(
                        env,
                        object,
                        @ptrCast(field.name ++ "\x00"),
                    );

                    if (std.mem.eql(u8, field.name, "timestamp") and value != 0) {
                        return translate.throw(
                            env,
                            "Timestamp should be set to 0 as this will be set correctly by Replica.",
                        );
                    }

                    @field(event, field.name) = switch (@typeInfo(field.type)) {
                        .Struct => @as(field.type, @bitCast(value)),
                        else => value,
                    };
                }
            },
            u128 => event.* = try translate.u128_from_value(env, object, "lookup"),
            else => @compileError("invalid Event type"),
        }
    }

    // Return only the memory for the Events. The total size of the bytes allocation can be inferred
    // via `bytes[0..max(bytes.len, events.len * ResultFrom(Event))]`.
    return std.mem.sliceAsBytes(events);
}

fn encode_results_into_data(
    comptime Event: type,
    comptime Result: type,
    result_bytes: []const u8,
    event_data: []align(16) u8,
) []u8 {
    // Event data should have been allocated with enough space to also contain the Results.
    const event_count = @divExact(event_data.len, @sizeOf(Event));
    const results = std.mem.bytesAsSlice(Result, event_data.ptr[0 .. @sizeOf(Result) * event_count]);

    const packet_results = std.mem.bytesAsSlice(Result, result_bytes);
    assert(packet_results.len == results.len);
    @memcpy(results, packet_results);

    return std.mem.sliceAsBytes(results);
}

fn encode_data_into_array(
    comptime Event: type,
    comptime Result: type,
    env: c.napi_env,
    result_data: []align(16) u8,
) !c.napi_value {
    // Make sure to deallocate the data memory at the end.
    const results = std.mem.bytesAsSlice(Result, result_data);
    defer {
        const alloc_size = @max(@sizeOf(Result) * results.len, @sizeOf(Event) * results.len);
        const data: []align(16) u8 = @alignCast(result_data.ptr[0..alloc_size]);
        allocator.free(data);
    }

    const array = try translate.create_array(
        env,
        @as(u32, @intCast(results.len)),
        "Failed to allocate array for results.",
    );

    for (results, 0..) |*result, i| {
        const object = try translate.create_object(
            env,
            "Failed to create " ++ @typeName(Result) ++ " object.",
        );

        inline for (std.meta.fields(Result)) |field| {
            const FieldInt = switch (@typeInfo(field.type)) {
                .Struct => |info| info.backing_integer.?,
                .Enum => |info| info.tag_type,
                else => field.type,
            };

            const value: FieldInt = switch (@typeInfo(field.type)) {
                .Struct => @bitCast(@field(result, field.name)),
                .Enum => @intFromEnum(@field(result, field.name)),
                else => @field(result, field.name),
            };

            try @field(translate, @typeName(FieldInt) ++ "_into_object")(
                env,
                object,
                @ptrCast(field.name ++ "\x00"),
                value,
                "Failed to set property \"" ++ field.name ++ "\" of " ++ @typeName(Result) ++ " object",
            );

            try translate.set_array_element(
                env,
                array,
                @intCast(i),
                object,
                "Failed to set element in results array.",
            );
        }
    }

    return array;
}
