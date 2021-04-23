const std = @import("std");
const assert = std.debug.assert;

const Account = @import("./tigerbeetle.zig").Account;
const AccountFlags = @import("./tigerbeetle.zig").AccountFlags;
const Transfer = @import("./tigerbeetle.zig").Transfer;
const TransferFlags = @import("./tigerbeetle.zig").TransferFlags;
const CommitFlags = @import("./tigerbeetle.zig").CommitFlags;
const Commit = @import("./tigerbeetle.zig").Commit;
const Operation = @import("./state_machine.zig").Operation;
const MessageBus = @import("message_bus.zig").MessageBus;
const Client = @import("client.zig").Client;
const IO = @import("io.zig").IO;

const c = @cImport({
    @cInclude("node_api.h");
});

/// N-API will call this constructor automatically to register the module.
export fn napi_register_module_v1(env: c.napi_env, exports: c.napi_value) c.napi_value {
    var function: c.napi_value = undefined;
    if (c.napi_create_function(env, null, 0, batch, null, &function) != .napi_ok) {
        _ = c.napi_throw_error(env, null, "Failed to create function batch().");
        return null;
    }

    if (c.napi_set_named_property(env, exports, "batch", function) != .napi_ok) {
        _ = c.napi_throw_error(env, null, "Failed to add function to exports");
        return null;
    }

    var init_function: c.napi_value = undefined;
    if (c.napi_create_function(env, null, 0, init, null, &init_function) != .napi_ok) {
        _ = c.napi_throw_error(env, null, "Failed to create function init().");
        return null;
    }

    if (c.napi_set_named_property(env, exports, "init", init_function) != .napi_ok) {
        _ = c.napi_throw_error(env, null, "Failed to add function to exports");
        return null;
    }

    return exports;
}

//
// Helper functions to encode and decode JS object
//
const ThrowError = error{ExceptionThrown};

fn throw(env: c.napi_env, comptime message: [*:0]const u8) ThrowError {
    var result = c.napi_throw_error(env, null, message);
    switch (result) {
        .napi_ok => {},
        else => unreachable,
    }

    return error.ExceptionThrown;
}

fn decode_u128_from_object(env: c.napi_env, object: c.napi_value, comptime key: [*:0]const u8) !u128 {
    var property: c.napi_value = undefined;
    if (c.napi_get_named_property(env, object, key, &property) != .napi_ok) {
        return throw(env, key ++ " must be defined");
    }

    var is_buffer: bool = undefined;
    assert(c.napi_is_buffer(env, property, &is_buffer) == .napi_ok);
    if (!is_buffer) return throw(env, key ++ " must be a Buffer");

    var data: ?*c_void = null;
    var data_length: usize = undefined;
    assert(c.napi_get_buffer_info(env, property, &data, &data_length) == .napi_ok);
    if (data_length != @sizeOf(u128)) return throw(env, key ++ " must be 128-bit");

    return @ptrCast(*u128, @alignCast(@alignOf(u128), data.?)).*;
}

fn decode_u64_from_object(env: c.napi_env, object: c.napi_value, comptime key: [*:0]const u8) !u64 {
    var property: c.napi_value = undefined;
    if (c.napi_get_named_property(env, object, key, &property) != .napi_ok) {
        return throw(env, key ++ " must be defined");
    }

    var result: u64 = undefined;
    var lossless: bool = undefined;
    switch (c.napi_get_value_bigint_uint64(env, property, &result, &lossless)) {
        .napi_ok => {},
        .napi_bigint_expected => return throw(env, key ++ " must be an unsigned 64-bit BigInt"),
        else => unreachable,
    }
    if (!lossless) return throw(env, key ++ " conversion was lossy");

    return result;
}

fn decode_buffer_from_object(env: c.napi_env, object: c.napi_value, comptime key: [*:0]const u8) ![]const u8 {
    var is_buffer: bool = undefined;
    assert(c.napi_is_buffer(env, property, &is_buffer) == .napi_ok);
    if (!is_buffer) return throw(env, key ++ " must be a buffer");

    var data: ?*c_void = null;
    var data_length: usize = undefined;
    assert(c.napi_get_buffer_info(env, property, &data, &data_length) == .napi_ok);

    if (data_length < 1) return throw(env, key ++ " must not be empty");

    return @ptrCast([*]u8, data.?)[0..data_length];
}

fn decode_u32(env: c.napi_env, value: c.napi_value, comptime key: [*:0]const u8) !u32 {
    var result: u32 = undefined;
    // TODO Check whether this will coerce signed numbers to a u32:
    // In that case we need to use the appropriate napi method to do more type checking here.
    // We want to make sure this is: unsigned, and an integer.
    switch (c.napi_get_value_uint32(env, value, &result)) {
        .napi_ok => {},
        .napi_number_expected => return throw(env, key ++ " must be a number"),
        else => unreachable,
    }
    return result;
}

fn decode_context(env: c.napi_env, value: c.napi_value) !*Client {
    var result: ?*c_void = null;
    if (c.napi_get_value_external(env, value, &result) != .napi_ok) {
        return throw(env, "Failed to get Client context pointer.");
    }
    return @ptrCast(*Client, @alignCast(@alignOf(Client), result.?));
}

/// This will create a reference in V8 with a ref_count of 1.
/// This reference will be destroyed when we return the server response to JS.
fn decode_callback(env: c.napi_env, value: c.napi_value) !usize {
    var callback_type: c.napi_valuetype = undefined;
    if (c.napi_typeof(env, value, &callback_type) != .napi_ok) {
        return throw(env, "Failed to check callback type.");
    }
    if (callback_type != .napi_function) return throw(env, "Callback must be a Function.");

    var callback_reference: c.napi_ref = undefined;
    if (c.napi_create_reference(env, value, 1, &callback_reference) != .napi_ok) {
        return throw(env, "Failed to create reference to callback.");
    }
    return @ptrToInt(callback_reference);
}

fn decode_from_object(comptime T: type, env: c.napi_env, object: c.napi_value) !T {
    return switch (T) {
        Commit => Commit{
            .id = try decode_u128_from_object(env, object, "id"),
            .custom_1 = try decode_u128_from_object(env, object, "custom_1"),
            .custom_2 = try decode_u128_from_object(env, object, "custom_2"),
            .custom_3 = try decode_u128_from_object(env, object, "custom_3"),
            .flags = @bitCast(CommitFlags, try decode_u64_from_object(env, object, "flags")),
        },
        Transfer => Transfer{
            .id = try decode_u128_from_object(env, object, "id"),
            .debit_account_id = try decode_u128_from_object(env, object, "debit_account_id"),
            .credit_account_id = try decode_u128_from_object(env, object, "credit_account_id"),
            .custom_1 = try decode_u128_from_object(env, object, "custom_1"),
            .custom_2 = try decode_u128_from_object(env, object, "custom_2"),
            .custom_3 = try decode_u128_from_object(env, object, "custom_3"),
            .flags = @bitCast(TransferFlags, try decode_u64_from_object(env, object, "flags")),
            .amount = try decode_u64_from_object(env, object, "amount"),
            .timeout = try decode_u64_from_object(env, object, "timeout"),
        },
        Account => Account{
            .id = try decode_u128_from_object(env, object, "id"),
            .custom = try decode_u128_from_object(env, object, "custom"),
            .flags = @bitCast(AccountFlags, try decode_u64_from_object(env, object, "flags")),
            .unit = try decode_u64_from_object(env, object, "unit"),
            .debit_reserved = try decode_u64_from_object(env, object, "debit_reserved"),
            .debit_accepted = try decode_u64_from_object(env, object, "debit_accepted"),
            .credit_reserved = try decode_u64_from_object(env, object, "credit_reserved"),
            .credit_accepted = try decode_u64_from_object(env, object, "credit_accepted"),
            .debit_reserved_limit = try decode_u64_from_object(env, object, "debit_reserved_limit"),
            .debit_accepted_limit = try decode_u64_from_object(env, object, "debit_accepted_limit"),
            .credit_reserved_limit = try decode_u64_from_object(env, object, "credit_reserved_limit"),
            .credit_accepted_limit = try decode_u64_from_object(env, object, "credit_accepted_limit"),
        },
        else => unreachable,
    };
}

fn decode_from_array(comptime T: type, env: c.napi_env, object: c.napi_value) ![]u8 {
    var is_array: bool = undefined;
    assert(c.napi_is_array(env, object, &is_array) == .napi_ok);
    if (!is_array) return throw(env, "Batch must be an Array.");

    var array_length: u32 = undefined;
    assert(c.napi_get_array_length(env, object, &array_length) == .napi_ok);
    if (array_length < 1) return throw(env, "Batch must contain at least one event.");

    const allocator = std.heap.c_allocator;
    const events = allocator.alloc(T, array_length) catch {
        return throw(env, "Failed to allocate memory for batch in client.");
    };
    errdefer allocator.free(events);

    var i: u32 = 0;
    while (i < array_length) : (i += 1) {
        var entry: c.napi_value = undefined;
        if (c.napi_get_element(env, object, i, &entry) != .napi_ok) {
            return throw(env, "Failed to get array element.");
        }
        events[i] = try decode_from_object(T, env, entry);
    }

    for (events) |event| std.debug.print("Decoded event {}\n", .{event});

    return std.mem.sliceAsBytes(events);
}

fn decode_slice_from_array(env: c.napi_env, object: c.napi_value, operation: Operation) ![]u8 {
    const allocator = std.heap.c_allocator;
    return switch (operation) {
        .reserved, .init => {
            std.debug.print("Reserved operation {}", .{operation});
            return throw(env, "Reserved operation.");
        },
        .create_accounts => try decode_from_array(Account, env, object),
        .create_transfers => try decode_from_array(Transfer, env, object),
        .commit_transfers => try decode_from_array(Commit, env, object),
        .lookup_accounts => try decode_from_array(u128, env, object),
    };
}

fn init(env: c.napi_env, info: c.napi_callback_info) callconv(.C) c.napi_value {
    var argc: usize = 20;
    var argv: [20]c.napi_value = undefined;
    if (c.napi_get_cb_info(env, info, &argc, &argv, null, null) != .napi_ok) {
        throw(env, "Failed to get args.") catch return null;
    }
    if (argc != 1) throw(env, "Function init() must receive 1 argument exactly.") catch return null;

    const id = decode_u128_from_object(env, argv[0], "client_id") catch return null;
    const cluster = decode_u128_from_object(env, argv[0], "cluster_id") catch return null;
    // TODO: parse replica_addresses from args
    const address = std.net.Address.parseIp4("127.0.0.1", 3001) catch {
        throw(env, "Failed to parse arguments.") catch return null;
    };

    var configuration: [1]std.net.Address = undefined;
    configuration[0] = address;

    // TODO We need to share this IO instance across multiple clients to stay under kernel limits:
    // This needs to become a global which we initialize within the N-API module constructor up top.
    var io = IO.init(32, 0) catch {
        throw(env, "Failed to initialize IO.") catch return null;
    };

    const allocator = std.heap.c_allocator;

    // TODO Move these initializers into a separate function that returns a Zig error.
    // We can then use errdefer to make sure we deinit() everything correctly.
    // At the same time, we can then catch and throw a JS exception in one place.
    var message_bus: MessageBus = undefined;
    message_bus.init(allocator, cluster, configuration[0..1], .{ .client = id }, &io) catch {
        throw(env, "Failed to initialize MessageBus.") catch return null;
    };
    var client = Client.init(
        allocator,
        id,
        cluster,
        "127.0.0.1", // TODO Decode configuration as an ascii string from config object.
        &message_bus,
    ) catch {
        throw(env, "Failed to initialize Client.") catch return null;
    };
    message_bus.process = .{ .client = &client };

    var context: c.napi_value = null;
    if (c.napi_create_external(env, @ptrCast(*c_void, &client), null, null, &context) != .napi_ok) {
        client.deinit();
        message_bus.deinit();
        throw(env, "Failed to initialize Client.") catch return null;
    }

    return context;
}

fn batch(env: c.napi_env, info: c.napi_callback_info) callconv(.C) c.napi_value {
    var argc: usize = 20;
    var argv: [20]c.napi_value = undefined;
    if (c.napi_get_cb_info(env, info, &argc, &argv, null, null) != .napi_ok) {
        throw(env, "Failed to get args.") catch return null;
    }

    const allocator = std.heap.c_allocator;
    if (argc != 4) throw(env, "Function batch() requires 4 arguments exactly.") catch return null;

    const client = decode_context(env, argv[0]) catch return null;
    const operation_int = decode_u32(env, argv[1], "operation") catch return null;
    const user_callback = decode_callback(env, argv[3]) catch return null;

    if (operation_int >= @typeInfo(Operation).Enum.fields.len) {
        throw(env, "Unknown operation.") catch return null;
    }

    const operation = @intToEnum(Operation, @intCast(u8, operation_int));
    const events = decode_slice_from_array(env, argv[2], operation) catch |err| switch (err) {
        error.ExceptionThrown => {
            return null;
        },
    };
    defer allocator.free(events);

    // TODO: map env and user_callback
    // const userdata =

    // client.batch(operation, events, userdata, &on_result);
    client.hello_world();

    return null;
}

// func on_result (userdata: u64, results: []const u8) void {
//     // retrieve callback
// }
