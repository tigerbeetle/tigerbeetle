const std = @import("std");
const assert = std.debug.assert;

const Account = @import("./tigerbeetle.zig").Account;

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
fn decode_u128_from_object(env: c.napi_env, object: c.napi_value, comptime key: [*:0]const u8, result: *u128) bool {
    var property: c.napi_value = undefined;
    if (c.napi_get_named_property(env, object, key, &property) != .napi_ok) {
        assert(c.napi_throw_error(env, null, key ++ " must be defined") == .napi_ok);
        return false;
    }

    var is_buffer: bool = undefined;
    assert(c.napi_is_buffer(env, property, &is_buffer) == .napi_ok);
    if (!is_buffer) {
        assert(c.napi_throw_error(env, null, key ++ " must be a Buffer") == .napi_ok);
        return false;
    }

    var data: ?*c_void = null;
    var data_length: usize = undefined;
    assert(c.napi_get_buffer_info(env, property, &data, &data_length) == .napi_ok);

    if (data_length != @sizeOf(u128)) {
        assert(c.napi_throw_error(env, null, key ++ " must be 128-bit") == .napi_ok);
        return false;
    }

    result.* = @ptrCast(*u128, @alignCast(@alignOf(u128), data.?)).*;
    return true;
}

fn decode_u64_from_object(env: c.napi_env, object: c.napi_value, comptime key: [*:0]const u8, result: *u64) bool {
    var property: c.napi_value = undefined;
    if (c.napi_get_named_property(env, object, key, &property) != .napi_ok) {
        assert(c.napi_throw_error(env, null, key ++ " must be defined") == .napi_ok);
        return false;
    }

    var lossless: bool = undefined;
    switch (c.napi_get_value_bigint_uint64(env, property, result, &lossless)) {
        .napi_ok => {},
        .napi_bigint_expected => {
            _ = c.napi_throw_error(env, null, key ++ " must be an unsigned 64-bit BigInt");
            return false;
        },
        else => unreachable,
    }
    if (!lossless) {
        assert(c.napi_throw_error(env, null, key ++ " conversion was lossy") == .napi_ok);
        return false;
    }

    return true;
}

fn decode_buffer_from_object(env: c.napi_env, object: c.napi_value, comptime key: [*:0]const u8, result: *u64) bool {
    var is_buffer: bool = undefined;
    assert(c.napi_is_buffer(env, property, &is_buffer) == .napi_ok);
    if (!is_buffer) {
        _ = c.napi_throw_error(env, null, key ++ " must be a buffer");
        return false;
    }

    var data: ?*c_void = null;
    var data_length: usize = undefined;
    assert(c.napi_get_buffer_info(env, property, &data, &data_length) == .napi_ok);

    if (data_length != @sizeOf(u128)) {
        _ = c.napi_throw_error(env, null, key ++ " must be 128-bit");
        return false;
    }

    result.* = @ptrCast(*u128, @alignCast(@alignOf(u128), data.?)).*;
    return true;
}

fn decode_u32(env: c.napi_env, value: c.napi_value, result: *u32, comptime error_message: [*:0]const u8) bool {
    var lossless: bool = undefined;
    switch (c.napi_get_value_uint32(env, value, result)) {
        .napi_ok => {},
        .napi_bigint_expected => {
            _ = c.napi_throw_error(env, null, error_message);
            return false;
        },
        else => unreachable,
    }

    return true;
}

fn transform_to_account(
    env: c.napi_env,
    object: c.napi_value,
    account: *Account,
) bool {
    var id: u128 = undefined;
    var custom: u128 = undefined;
    var unit: u64 = undefined;
    var debit_reserved: u64 = undefined;
    var debit_accepted: u64 = undefined;
    var credit_reserved: u64 = undefined;
    var credit_accepted: u64 = undefined;
    var debit_reserved_limit: u64 = undefined;
    var debit_accepted_limit: u64 = undefined;
    var credit_reserved_limit: u64 = undefined;
    var credit_accepted_limit: u64 = undefined;

    if (!decode_u128_from_object(env, object, "id", &id) or
        !decode_u128_from_object(env, object, "custom", &custom) or
        !decode_u64_from_object(env, object, "unit", &unit) or
        !decode_u64_from_object(env, object, "debit_reserved", &debit_reserved) or
        !decode_u64_from_object(env, object, "debit_accepted", &debit_accepted) or
        !decode_u64_from_object(env, object, "credit_reserved", &credit_reserved) or
        !decode_u64_from_object(env, object, "credit_accepted", &credit_accepted) or
        !decode_u64_from_object(env, object, "debit_reserved_limit", &debit_reserved_limit) or
        !decode_u64_from_object(env, object, "debit_accepted_limit", &debit_accepted_limit) or
        !decode_u64_from_object(env, object, "credit_reserved_limit", &credit_reserved_limit) or
        !decode_u64_from_object(env, object, "credit_accepted_limit", &credit_accepted_limit)
    )
    {
        return false;
    }

    account.* = Account{
        .id = id,
        .custom = custom,
        .flags = .{},
        .unit = unit,
        .debit_reserved = debit_reserved,
        .debit_accepted = debit_accepted,
        .credit_reserved = credit_reserved,
        .credit_accepted = credit_accepted,
        .debit_reserved_limit = debit_reserved_limit,
        .debit_accepted_limit = debit_accepted_limit,
        .credit_reserved_limit = credit_reserved_limit,
        .credit_accepted_limit = credit_accepted_limit,
    };
    std.debug.print("decoded account: {}\n", .{account});

    return true;
}

fn transform_to_account_array(
    env: c.napi_env,
    object: c.napi_value,
    result: *const []Account,
) bool
{
    var isArray: bool = undefined;
    assert(c.napi_is_array(env, object, &isArray) == .napi_ok);
    if (!isArray) {
        assert(c.napi_throw_error(env, null, "Batch must be an array of TigerBeetle commands.") == .napi_ok);
        return false;   
    }

    var array_length: u32 = undefined;
    assert(c.napi_get_array_length(env, object, &array_length) == .napi_ok);
    assert(array_length >= 0);

    var i: u32 = 0;
    while (i < array_length) {
        var entry: c.napi_value = undefined;
        if (c.napi_get_element(env, object, i, &entry) != .napi_ok) {
            assert(c.napi_throw_error(env, null, "Failed to get entry from batch of Create Account.") == .napi_ok);
            return false;
        }
        if (!transform_to_account(env, entry, &result.*[i])) {
            assert(c.napi_throw_error(env, null, "Failed to transform to account") == .napi_ok);
            return false;
        }
        i += 1;
    }

    return true;
}

fn batch (
    env: c.napi_env,
    info: c.napi_callback_info,
) callconv(.C) c.napi_value {
    var argc: usize = 20;
    var argv: [20]c.napi_value = undefined;
    if (c.napi_get_cb_info(env, info, &argc, &argv, null, null) != .napi_ok) {
        assert(c.napi_throw_error(env, null, "Failed to get args") == .napi_ok);
        return null;
    }

    if (argc != 4) {
        assert(c.napi_throw_error(env, null, "batch expects 4 arguments.") == .napi_ok);
        return null;
    }

    var context: u32 = undefined;
    var operation: u32 = undefined;
    if (!decode_u32(env, argv[0], &context, "Failed to parse context argument.") or
        !decode_u32(env, argv[1], &operation, "Failed to parse operation argument.")
    ) {
        return null;
    }

    var batch_size: u32 = undefined;
    assert(c.napi_get_array_length(env, argv[2], &batch_size) == .napi_ok);
    std.debug.print("Received batch of size: {}\n", .{ batch_size });
    assert(batch_size >= 0);

    // TODO: should we just call the callback here?
    // if (batch_size == 0) {

    // }

    // TODO: better way to do this?
    const allocator = std.heap.page_allocator;
    var length: u32 = undefined;
    switch (operation) {
        0 => {
            length = batch_size * @sizeOf(Account);
        },
        else => {
            std.debug.print("Unknown operation {}", .{operation});
            assert(c.napi_throw_error(env, null, "Unknown operation.") == .napi_ok);
            return null;
        },
    }

    const commands: []u8 = allocator.alloc(u8, length) catch |err| {
        assert(c.napi_throw_error(env, null, "Failed to allocate memory for TigerBeetle commands in client.") == .napi_ok);
        return null;
    };
    defer allocator.free(commands);

    switch (operation) {
        0 => {
            const accountCommands = @bitCast([]Account, commands);
            if(!transform_to_account_array(env, argv[2], &accountCommands)) {
                return null;
            }
        },
        else => {
            assert(c.napi_throw_error(env, null, "Unknown operation.") == .napi_ok);
            return null;
        },
    }

    // call out to client.zig

    return null;
}
