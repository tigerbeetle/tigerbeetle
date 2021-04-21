const std = @import("std");
const assert = std.debug.assert;

const Account = @import("./tigerbeetle.zig").Account;
const AccountFlags = @import("./tigerbeetle.zig").AccountFlags;
const Transfer = @import("./tigerbeetle.zig").Transfer;
const TransferFlags = @import("./tigerbeetle.zig").TransferFlags;
const CommitFlags = @import("./tigerbeetle.zig").CommitFlags;
const Commit = @import("./tigerbeetle.zig").Commit;
const Operation = @import("./state_machine.zig").Operation;

const c = @cImport({
    @cInclude("node_api.h");
});

//
// Register module
//
export fn napi_register_module_v1(env: c.napi_env, exports: c.napi_value) c.napi_value {
    var function: c.napi_value = undefined;
    if (c.napi_create_function(env, null, 0, batch, null, &function) != .napi_ok) {
        _ = c.napi_throw_error(env, null, "Failed to create function");
        return null;
    }

    if (c.napi_set_named_property(env, exports, "batch", function) != .napi_ok) {
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

    if (data_length > 0) return throw(env, key ++ " must not be empty");

    return @ptrCast([*]u8, data.?)[0..data_length];
}

fn decode_u32(env: c.napi_env, value: c.napi_value, comptime message: [*:0]const u8) !u32 {
    var result: u32 = undefined;
    switch (c.napi_get_value_uint32(env, value, &result)) {
        .napi_ok => {},
        .napi_bigint_expected => return throw(env, message),
        else => unreachable,
    }

    return result;
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
    while (i < array_length) : (i+=1) {
        var entry: c.napi_value = undefined;
        if (c.napi_get_element(env, object, i, &entry) != .napi_ok) {
            return throw(env, "Failed to get array element.");
        }

        events[i] = try decode_from_object(T, env, entry);
    }

    for (events) |event| std.debug.print("Decoded event {}\n", .{event});

    return std.mem.sliceAsBytes(events);
}

fn decode_slice_from_array(env: c.napi_env, object: c.napi_value, operation: Operation) ![]u8
{
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

fn batch (
    env: c.napi_env,
    info: c.napi_callback_info,
) callconv(.C) c.napi_value {
    var argc: usize = 20;
    var argv: [20]c.napi_value = undefined;
    if (c.napi_get_cb_info(env, info, &argc, &argv, null, null) != .napi_ok) {
        throw(env, "Failed to get args.") catch return null;
    }

    const allocator = std.heap.c_allocator;
    if (argc != 4) throw(env, "Function batch() must receive 4 arguments exactly.") catch return null;

    // TODO: context will be napi_external
    var context = decode_u32(env, argv[0], "Failed to parse \"context\".") catch return null;
    var operation_int = decode_u32(env, argv[1], "Failed to parse \"operation\".") catch return null;
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

    // call out to client.zig

    return null;
}
