const std = @import("std");
const assert = std.debug.assert;
const c = @import("c.zig");

pub fn register_function(
    env: c.napi_env,
    exports: c.napi_value,
    comptime name: [*:0]const u8,
    function: fn (env: c.napi_env, info: c.napi_callback_info) callconv(.C) c.napi_value,
) !void {
    var napi_function: c.napi_value = undefined;
    if (c.napi_create_function(env, null, 0, function, null, &napi_function) != .napi_ok) {
        return throw(env, "Failed to create function " ++ name ++ "().");
    }

    if (c.napi_set_named_property(env, exports, name, napi_function) != .napi_ok) {
        return throw(env, "Failed to add " ++ name ++ "() to exports.");
    }
}

pub fn throw(env: c.napi_env, comptime message: [*:0]const u8) error{ExceptionThrown} {
    var result = c.napi_throw_error(env, null, message);
    switch (result) {
        .napi_ok, .napi_pending_exception => {},
        else => unreachable,
    }

    return error.ExceptionThrown;
}

pub fn capture_undefined(env: c.napi_env) !c.napi_value {
    var result: c.napi_value = undefined;
    if (c.napi_get_undefined(env, &result) != .napi_ok) {
        return throw(env, "Failed to capture the value of \"undefined\".");
    }

    return result;
}

pub fn set_instance_data(
    env: c.napi_env,
    data: *c_void,
    finalize_callback: fn (env: c.napi_env, data: ?*c_void, hint: ?*c_void) callconv(.C) void,
) !void {
    if (c.napi_set_instance_data(env, data, finalize_callback, null) != .napi_ok) {
        return throw(env, "Failed to initialize environment.");
    }
}

pub fn create_external(env: c.napi_env, context: *c_void) !c.napi_value {
    var result: c.napi_value = null;
    if (c.napi_create_external(env, context, null, null, &result) != .napi_ok) {
        return throw(env, "Failed to create external for client context.");
    }

    return result;
}

pub fn value_external(
    env: c.napi_env,
    val: c.napi_value,
    comptime error_message: [*:0]const u8,
) !?*c_void {
    var result: ?*c_void = undefined;
    if (c.napi_get_value_external(env, val, &result) != .napi_ok) {
        return throw(env, error_message);
    }

    return result;
}

pub const UserData = packed struct {
    env: c.napi_env,
    callback_reference: c.napi_ref,
};

/// This will create a reference in V8 with a ref_count of 1.
/// This reference will be destroyed when we return the server response to JS.
pub fn user_data_from_value(env: c.napi_env, val: c.napi_value) !UserData {
    var callback_type: c.napi_valuetype = undefined;
    if (c.napi_typeof(env, val, &callback_type) != .napi_ok) {
        return throw(env, "Failed to check callback type.");
    }
    if (callback_type != .napi_function) return throw(env, "Callback must be a Function.");

    var callback_reference: c.napi_ref = undefined;
    if (c.napi_create_reference(env, val, 1, &callback_reference) != .napi_ok) {
        return throw(env, "Failed to create reference to callback.");
    }

    return UserData{
        .env = env,
        .callback_reference = callback_reference,
    };
}

pub fn globals(env: c.napi_env) !?*c_void {
    var data: ?*c_void = null;
    if (c.napi_get_instance_data(env, &data) != .napi_ok) {
        return throw(env, "Failed to decode globals.");
    }

    return data;
}

pub fn buffer_from_object(
    env: c.napi_env,
    object: c.napi_value,
    comptime key: [*:0]const u8,
) ![]const u8 {
    var property: c.napi_value = undefined;
    if (c.napi_get_named_property(env, object, key, &property) != .napi_ok) {
        return throw(env, key ++ " must be defined");
    }

    var is_buffer: bool = undefined;
    assert(c.napi_is_buffer(env, property, &is_buffer) == .napi_ok);

    if (!is_buffer) return throw(env, key ++ " must be a buffer");

    var data: ?*c_void = null;
    var data_length: usize = undefined;
    assert(c.napi_get_buffer_info(env, property, &data, &data_length) == .napi_ok);

    if (data_length < 1) return throw(env, key ++ " must not be empty");

    return @ptrCast([*]u8, data.?)[0..data_length];
}

pub fn u128_from_object(env: c.napi_env, object: c.napi_value, comptime key: [:0]const u8) !u128 {
    var property: c.napi_value = undefined;
    if (c.napi_get_named_property(env, object, key, &property) != .napi_ok) {
        return throw(env, key ++ " must be defined");
    }

    return u128_from_value(env, property, key);
}

pub fn u64_from_object(env: c.napi_env, object: c.napi_value, comptime key: [*:0]const u8) !u64 {
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

pub fn u128_from_value(env: c.napi_env, value: c.napi_value, comptime name: [:0]const u8) !u128 {
    // A BigInt's value (using ^ to mean exponent) is (words[0] * (2^64)^0 + words[1] * (2^64)^1 + ...)
    var sign_bit: c_int = undefined;
    var words: [2]u64 align(@alignOf(u128)) = undefined;
    var word_count: usize = 2;
    switch (c.napi_get_value_bigint_words(env, value, &sign_bit, &word_count, &words)) {
        .napi_ok => {},
        .napi_bigint_expected => return throw(env, name ++ " must be a BigInt"),
        else => unreachable,
    }
    if (sign_bit != 0) return throw(env, name ++ " must be positive");
    if (word_count > 2) return throw(env, name ++ " must fit in 128 bits");

    // V8 says that the words are little endian. If we were on a big endian machine
    // we would need to convert, but big endian is not supported by tigerbeetle.
    if (word_count == 0) return 0;
    if (word_count == 1) return words[0];
    assert(word_count == 2);
    return @ptrCast(*u128, &words).*;
}

pub fn u32_from_value(env: c.napi_env, val: c.napi_value, comptime key: [*:0]const u8) !u32 {
    var result: u32 = undefined;
    // TODO Check whether this will coerce signed numbers to a u32:
    // In that case we need to use the appropriate napi method to do more type checking here.
    // We want to make sure this is: unsigned, and an integer.
    switch (c.napi_get_value_uint32(env, val, &result)) {
        .napi_ok => {},
        .napi_number_expected => return throw(env, key ++ " must be a number"),
        else => unreachable,
    }
    return result;
}

pub fn u128_into_object(
    env: c.napi_env,
    object: c.napi_value,
    comptime key: [*:0]const u8,
    val: u128,
    comptime error_message: [*:0]const u8,
) !void {
    // A BigInt's value (using ^ to mean exponent) is (words[0] * (2^64)^0 + words[1] * (2^64)^1 + ...)

    // V8 says that the words are little endian. If we were on a big endian machine
    // we would need to convert, but big endian is not supported by tigerbeetle.
    var bigint: c.napi_value = undefined;
    if (c.napi_create_bigint_words(env, 0, 2, @ptrCast(*const [2]u64, &val), &bigint) != .napi_ok) {
        return throw(env, error_message);
    }

    if (c.napi_set_named_property(env, object, key, bigint) != .napi_ok) {
        return throw(env, error_message);
    }
}

pub fn u64_into_object(
    env: c.napi_env,
    object: c.napi_value,
    comptime key: [*:0]const u8,
    val: u64,
    comptime error_message: [*:0]const u8,
) !void {
    var result: c.napi_value = undefined;
    if (c.napi_create_bigint_uint64(env, val, &result) != .napi_ok) {
        return throw(env, error_message);
    }

    if (c.napi_set_named_property(env, object, key, result) != .napi_ok) {
        return throw(env, error_message);
    }
}

pub fn u32_into_object(
    env: c.napi_env,
    object: c.napi_value,
    comptime key: [*:0]const u8,
    val: u32,
    comptime error_message: [*:0]const u8,
) !void {
    var result: c.napi_value = undefined;
    if (c.napi_create_uint32(env, val, &result) != .napi_ok) {
        return throw(env, error_message);
    }

    if (c.napi_set_named_property(env, object, key, result) != .napi_ok) {
        return throw(env, error_message);
    }
}

pub fn create_object(env: c.napi_env, comptime error_message: [*:0]const u8) !c.napi_value {
    var result: c.napi_value = undefined;
    if (c.napi_create_object(env, &result) != .napi_ok) {
        return throw(env, error_message);
    }

    return result;
}

fn create_buffer(
    env: c.napi_env,
    val: []const u8,
    comptime error_message: [*:0]const u8,
) !c.napi_value {
    var data: ?*c_void = undefined;
    var result: c.napi_value = undefined;
    if (c.napi_create_buffer(env, val.len, &data, &result) != .napi_ok) {
        return throw(env, error_message);
    }

    std.mem.copy(u8, @ptrCast([*]u8, data.?)[0..val.len], val[0..val.len]);

    return result;
}

pub fn create_array(
    env: c.napi_env,
    length: u32,
    comptime error_message: [*:0]const u8,
) !c.napi_value {
    var result: c.napi_value = undefined;
    if (c.napi_create_array_with_length(env, length, &result) != .napi_ok) {
        return throw(env, error_message);
    }

    return result;
}

pub fn set_array_element(
    env: c.napi_env,
    array: c.napi_value,
    index: u32,
    val: c.napi_value,
    comptime error_message: [*:0]const u8,
) !void {
    if (c.napi_set_element(env, array, index, val) != .napi_ok) {
        return throw(env, error_message);
    }
}

pub fn array_element(env: c.napi_env, array: c.napi_value, index: u32) !c.napi_value {
    var element: c.napi_value = undefined;
    if (c.napi_get_element(env, array, index, &element) != .napi_ok) {
        return throw(env, "Failed to get array element.");
    }

    return element;
}

pub fn array_length(env: c.napi_env, array: c.napi_value) !u32 {
    var is_array: bool = undefined;
    assert(c.napi_is_array(env, array, &is_array) == .napi_ok);
    if (!is_array) return throw(env, "Batch must be an Array.");

    var length: u32 = undefined;
    assert(c.napi_get_array_length(env, array, &length) == .napi_ok);

    return length;
}

pub fn delete_reference(env: c.napi_env, reference: c.napi_ref) !void {
    if (c.napi_delete_reference(env, reference) != .napi_ok) {
        return throw(env, "Failed to delete callback reference.");
    }
}

pub fn create_error(env: c.napi_env, comptime message: [*:0]const u8) !c.napi_value {
    var napi_string: napi.value = undefined;
    if (c.napi_create_string_utf8(env, message, std.mem.len(message), &napi_string) != .napi_ok) {
        return napi.throw(env, "Failed to encode napi utf8.");
    }

    var napi_error: c.napi_value = undefined;
    if (c.napi_create_error(env, null, napi_string, &napi_error) != .napi_ok) {
        return napi.throw(env, "Failed to create Error.");
    }

    return napi_error;
}

pub fn call_function(
    env: c.napi_env,
    this: c.napi_value,
    callback: c.napi_value,
    argc: usize,
    argv: [*]c.napi_value,
) !void {
    if (c.napi_call_function(env, this, callback, argc, argv, null) != .napi_ok) {
        return throw(env, "Failed to invoke results callback.");
    }
}

pub fn scope(env: c.napi_env, comptime error_message: [*:0]const u8) !c.napi_value {
    var result: c.napi_value = undefined;
    if (c.napi_get_global(env, &result) != .napi_ok) {
        return throw(env, error_message);
    }

    return result;
}

pub fn reference_value(
    env: c.napi_env,
    callback_reference: c.napi_ref,
    comptime error_message: [*:0]const u8,
) !c.napi_value {
    var result: c.napi_value = undefined;
    if (c.napi_get_reference_value(env, callback_reference, &result) != .napi_ok) {
        return throw(env, error_message);
    }

    return result;
}
