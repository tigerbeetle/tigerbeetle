const std = @import("std");
const assert = std.debug.assert;
const c = @import("c.zig");

pub fn register_function(
    env: c.napi_env,
    exports: c.napi_value,
    comptime name: [:0]const u8,
    function: fn (env: c.napi_env, info: c.napi_callback_info) callconv(.C) c.napi_value,
) !void {
    var napi_function: c.napi_value = undefined;
    if (c.napi_create_function(env, null, 0, function, null, &napi_function) != c.napi_ok) {
        return throw(env, "Failed to create function " ++ name ++ "().");
    }

    if (c.napi_set_named_property(env, exports, name, napi_function) != c.napi_ok) {
        return throw(env, "Failed to add " ++ name ++ "() to exports.");
    }
}

const TranslationError = error{ExceptionThrown};
pub fn throw(env: c.napi_env, comptime message: [:0]const u8) TranslationError {
    var result = c.napi_throw_error(env, null, message);
    switch (result) {
        c.napi_ok, c.napi_pending_exception => {},
        else => unreachable,
    }

    return TranslationError.ExceptionThrown;
}

pub fn capture_undefined(env: c.napi_env) !c.napi_value {
    var result: c.napi_value = undefined;
    if (c.napi_get_undefined(env, &result) != c.napi_ok) {
        return throw(env, "Failed to capture the value of \"undefined\".");
    }

    return result;
}

pub fn set_instance_data(
    env: c.napi_env,
    data: *anyopaque,
    finalize_callback: fn (env: c.napi_env, data: ?*anyopaque, hint: ?*anyopaque) callconv(.C) void,
) !void {
    if (c.napi_set_instance_data(env, data, finalize_callback, null) != c.napi_ok) {
        return throw(env, "Failed to initialize environment.");
    }
}

pub fn create_external(env: c.napi_env, context: *anyopaque) !c.napi_value {
    var result: c.napi_value = null;
    if (c.napi_create_external(env, context, null, null, &result) != c.napi_ok) {
        return throw(env, "Failed to create external for client context.");
    }

    return result;
}

pub fn value_external(
    env: c.napi_env,
    value: c.napi_value,
    comptime error_message: [:0]const u8,
) !?*anyopaque {
    var result: ?*anyopaque = undefined;
    if (c.napi_get_value_external(env, value, &result) != c.napi_ok) {
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
pub fn user_data_from_value(env: c.napi_env, value: c.napi_value) !UserData {
    var callback_type: c.napi_valuetype = undefined;
    if (c.napi_typeof(env, value, &callback_type) != c.napi_ok) {
        return throw(env, "Failed to check callback type.");
    }
    if (callback_type != c.napi_function) return throw(env, "Callback must be a Function.");

    var callback_reference: c.napi_ref = undefined;
    if (c.napi_create_reference(env, value, 1, &callback_reference) != c.napi_ok) {
        return throw(env, "Failed to create reference to callback.");
    }

    return UserData{
        .env = env,
        .callback_reference = callback_reference,
    };
}

pub fn globals(env: c.napi_env) !?*anyopaque {
    var data: ?*anyopaque = null;
    if (c.napi_get_instance_data(env, &data) != c.napi_ok) {
        return throw(env, "Failed to decode globals.");
    }

    return data;
}

pub fn slice_from_object(
    env: c.napi_env,
    object: c.napi_value,
    comptime key: [:0]const u8,
) ![]const u8 {
    var property: c.napi_value = undefined;
    if (c.napi_get_named_property(env, object, key, &property) != c.napi_ok) {
        return throw(env, key ++ " must be defined");
    }

    return slice_from_value(env, property, key);
}

pub fn slice_from_value(
    env: c.napi_env,
    value: c.napi_value,
    comptime key: [:0]const u8,
) ![]u8 {
    var is_buffer: bool = undefined;
    assert(c.napi_is_buffer(env, value, &is_buffer) == c.napi_ok);

    if (!is_buffer) return throw(env, key ++ " must be a buffer");

    var data: ?*anyopaque = null;
    var data_length: usize = undefined;
    assert(c.napi_get_buffer_info(env, value, &data, &data_length) == c.napi_ok);

    if (data_length < 1) return throw(env, key ++ " must not be empty");

    return @ptrCast([*]u8, data.?)[0..data_length];
}

pub fn bytes_from_object(
    env: c.napi_env,
    object: c.napi_value,
    comptime length: u8,
    comptime key: [:0]const u8,
) ![length]u8 {
    var property: c.napi_value = undefined;
    if (c.napi_get_named_property(env, object, key, &property) != c.napi_ok) {
        return throw(env, key ++ " must be defined");
    }

    const data = try slice_from_value(env, property, key);
    if (data.len != length) {
        return throw(env, key ++ " has incorrect length.");
    }

    // Copy this out of V8 as the underlying data lifetime is not guaranteed.
    var result: [length]u8 = undefined;
    std.mem.copy(u8, result[0..], data[0..]);

    return result;
}

pub fn bytes_from_buffer(
    env: c.napi_env,
    buffer: c.napi_value,
    output: []u8,
    comptime key: [:0]const u8,
) !usize {
    const data = try slice_from_value(env, buffer, key);
    if (data.len < 1) {
        return throw(env, key ++ " must not be empty.");
    }
    if (data.len > output.len) {
        return throw(env, key ++ " exceeds max message size.");
    }

    // Copy this out of V8 as the underlying data lifetime is not guaranteed.
    std.mem.copy(u8, output[0..], data[0..]);

    return data.len;
}

pub fn u128_from_object(env: c.napi_env, object: c.napi_value, comptime key: [:0]const u8) !u128 {
    var property: c.napi_value = undefined;
    if (c.napi_get_named_property(env, object, key, &property) != c.napi_ok) {
        return throw(env, key ++ " must be defined");
    }

    return u128_from_value(env, property, key);
}

pub fn u64_from_object(env: c.napi_env, object: c.napi_value, comptime key: [:0]const u8) !u64 {
    var property: c.napi_value = undefined;
    if (c.napi_get_named_property(env, object, key, &property) != c.napi_ok) {
        return throw(env, key ++ " must be defined");
    }

    return u64_from_value(env, property, key);
}

pub fn u32_from_object(env: c.napi_env, object: c.napi_value, comptime key: [:0]const u8) !u32 {
    var property: c.napi_value = undefined;
    if (c.napi_get_named_property(env, object, key, &property) != c.napi_ok) {
        return throw(env, key ++ " must be defined");
    }

    return u32_from_value(env, property, key);
}

pub fn u16_from_object(env: c.napi_env, object: c.napi_value, comptime key: [:0]const u8) !u16 {
    const result = try u32_from_object(env, object, key);
    if (result > 65535) {
        return throw(env, key ++ " must be a u16.");
    }

    return @intCast(u16, result);
}

pub fn u128_from_value(env: c.napi_env, value: c.napi_value, comptime name: [:0]const u8) !u128 {
    // A BigInt's value (using ^ to mean exponent) is (words[0] * (2^64)^0 + words[1] * (2^64)^1 + ...)

    // V8 says that the words are little endian. If we were on a big endian machine
    // we would need to convert, but big endian is not supported by tigerbeetle.
    var result: u128 = 0;
    var sign_bit: c_int = undefined;
    const words = @ptrCast(*[2]u64, &result);
    var word_count: usize = 2;
    switch (c.napi_get_value_bigint_words(env, value, &sign_bit, &word_count, words)) {
        c.napi_ok => {},
        c.napi_bigint_expected => return throw(env, name ++ " must be a BigInt"),
        else => unreachable,
    }
    if (sign_bit != 0) return throw(env, name ++ " must be positive");
    if (word_count > 2) return throw(env, name ++ " must fit in 128 bits");

    return result;
}

pub fn u64_from_value(env: c.napi_env, value: c.napi_value, comptime name: [:0]const u8) !u64 {
    var result: u64 = undefined;
    var lossless: bool = undefined;
    switch (c.napi_get_value_bigint_uint64(env, value, &result, &lossless)) {
        c.napi_ok => {},
        c.napi_bigint_expected => return throw(env, name ++ " must be an unsigned 64-bit BigInt"),
        else => unreachable,
    }
    if (!lossless) return throw(env, name ++ " conversion was lossy");

    return result;
}

pub fn u32_from_value(env: c.napi_env, value: c.napi_value, comptime name: [:0]const u8) !u32 {
    var result: u32 = undefined;
    // TODO Check whether this will coerce signed numbers to a u32:
    // In that case we need to use the appropriate napi method to do more type checking here.
    // We want to make sure this is: unsigned, and an integer.
    switch (c.napi_get_value_uint32(env, value, &result)) {
        c.napi_ok => {},
        c.napi_number_expected => return throw(env, name ++ " must be a number"),
        else => unreachable,
    }
    return result;
}

pub fn byte_slice_into_object(
    env: c.napi_env,
    object: c.napi_value,
    comptime key: [:0]const u8,
    value: []const u8,
    comptime error_message: [:0]const u8,
) !void {
    var result: c.napi_value = undefined;
    // create a copy that is managed by V8.
    if (c.napi_create_buffer_copy(env, value.len, value.ptr, null, &result) != c.napi_ok) {
        return throw(env, error_message ++ " Failed to allocate Buffer in V8.");
    }

    if (c.napi_set_named_property(env, object, key, result) != c.napi_ok) {
        return throw(env, error_message);
    }
}

pub fn u128_into_object(
    env: c.napi_env,
    object: c.napi_value,
    comptime key: [:0]const u8,
    value: u128,
    comptime error_message: [:0]const u8,
) !void {
    // A BigInt's value (using ^ to mean exponent) is (words[0] * (2^64)^0 + words[1] * (2^64)^1 + ...)

    // V8 says that the words are little endian. If we were on a big endian machine
    // we would need to convert, but big endian is not supported by tigerbeetle.
    var bigint: c.napi_value = undefined;
    if (c.napi_create_bigint_words(
        env,
        0,
        2,
        @ptrCast(*const [2]u64, &value),
        &bigint,
    ) != c.napi_ok) {
        return throw(env, error_message);
    }

    if (c.napi_set_named_property(env, object, key, bigint) != c.napi_ok) {
        return throw(env, error_message);
    }
}

pub fn u64_into_object(
    env: c.napi_env,
    object: c.napi_value,
    comptime key: [:0]const u8,
    value: u64,
    comptime error_message: [:0]const u8,
) !void {
    var result: c.napi_value = undefined;
    if (c.napi_create_bigint_uint64(env, value, &result) != c.napi_ok) {
        return throw(env, error_message);
    }

    if (c.napi_set_named_property(env, object, key, result) != c.napi_ok) {
        return throw(env, error_message);
    }
}

pub fn u32_into_object(
    env: c.napi_env,
    object: c.napi_value,
    comptime key: [:0]const u8,
    value: u32,
    comptime error_message: [:0]const u8,
) !void {
    var result: c.napi_value = undefined;
    if (c.napi_create_uint32(env, value, &result) != c.napi_ok) {
        return throw(env, error_message);
    }

    if (c.napi_set_named_property(env, object, key, result) != c.napi_ok) {
        return throw(env, error_message);
    }
}

pub fn u16_into_object(
    env: c.napi_env,
    object: c.napi_value,
    comptime key: [:0]const u8,
    value: u16,
    comptime error_message: [:0]const u8,
) !void {
    var result: c.napi_value = undefined;
    if (c.napi_create_uint16(env, value, &result) != c.napi_ok) {
        return throw(env, error_message);
    }

    if (c.napi_set_named_property(env, object, key, result) != c.napi_ok) {
        return throw(env, error_message);
    }
}

pub fn create_object(env: c.napi_env, comptime error_message: [:0]const u8) !c.napi_value {
    var result: c.napi_value = undefined;
    if (c.napi_create_object(env, &result) != c.napi_ok) {
        return throw(env, error_message);
    }

    return result;
}

fn create_buffer(
    env: c.napi_env,
    value: []const u8,
    comptime error_message: [:0]const u8,
) !c.napi_value {
    var data: ?*anyopaque = undefined;
    var result: c.napi_value = undefined;
    if (c.napi_create_buffer(env, value.len, &data, &result) != c.napi_ok) {
        return throw(env, error_message);
    }

    std.mem.copy(u8, @ptrCast([*]u8, data.?)[0..value.len], value[0..value.len]);

    return result;
}

pub fn create_array(
    env: c.napi_env,
    length: u32,
    comptime error_message: [:0]const u8,
) !c.napi_value {
    var result: c.napi_value = undefined;
    if (c.napi_create_array_with_length(env, length, &result) != c.napi_ok) {
        return throw(env, error_message);
    }

    return result;
}

pub fn set_array_element(
    env: c.napi_env,
    array: c.napi_value,
    index: u32,
    value: c.napi_value,
    comptime error_message: [:0]const u8,
) !void {
    if (c.napi_set_element(env, array, index, value) != c.napi_ok) {
        return throw(env, error_message);
    }
}

pub fn array_element(env: c.napi_env, array: c.napi_value, index: u32) !c.napi_value {
    var element: c.napi_value = undefined;
    if (c.napi_get_element(env, array, index, &element) != c.napi_ok) {
        return throw(env, "Failed to get array element.");
    }

    return element;
}

pub fn array_length(env: c.napi_env, array: c.napi_value) !u32 {
    var is_array: bool = undefined;
    assert(c.napi_is_array(env, array, &is_array) == c.napi_ok);
    if (!is_array) return throw(env, "Batch must be an Array.");

    var length: u32 = undefined;
    assert(c.napi_get_array_length(env, array, &length) == c.napi_ok);

    return length;
}

pub fn delete_reference(env: c.napi_env, reference: c.napi_ref) !void {
    if (c.napi_delete_reference(env, reference) != c.napi_ok) {
        return throw(env, "Failed to delete callback reference.");
    }
}

pub fn create_error(
    env: c.napi_env,
    comptime message: [:0]const u8,
) TranslationError!c.napi_value {
    var napi_string: c.napi_value = undefined;
    if (c.napi_create_string_utf8(env, message, std.mem.len(message), &napi_string) != c.napi_ok) {
        return TranslationError.ExceptionThrown;
    }

    var napi_error: c.napi_value = undefined;
    if (c.napi_create_error(env, null, napi_string, &napi_error) != c.napi_ok) {
        return TranslationError.ExceptionThrown;
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
    const result = c.napi_call_function(env, this, callback, argc, argv, null);
    switch (result) {
        c.napi_ok => {},
        // the user's callback may throw a JS exception or call other functions that do so. We
        // therefore don't throw another error.
        c.napi_pending_exception => {},
        else => return throw(env, "Failed to invoke results callback."),
    }
}

pub fn scope(env: c.napi_env, comptime error_message: [:0]const u8) !c.napi_value {
    var result: c.napi_value = undefined;
    if (c.napi_get_global(env, &result) != c.napi_ok) {
        return throw(env, error_message);
    }

    return result;
}

pub fn reference_value(
    env: c.napi_env,
    callback_reference: c.napi_ref,
    comptime error_message: [:0]const u8,
) !c.napi_value {
    var result: c.napi_value = undefined;
    if (c.napi_get_reference_value(env, callback_reference, &result) != c.napi_ok) {
        return throw(env, error_message);
    }

    return result;
}
