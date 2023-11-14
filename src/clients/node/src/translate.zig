const std = @import("std");
const assert = std.debug.assert;
const c = @import("c.zig");

pub fn register_function(
    env: c.napi_env,
    exports: c.napi_value,
    comptime name: [:0]const u8,
    function: *const fn (env: c.napi_env, info: c.napi_callback_info) callconv(.C) c.napi_value,
) !void {
    var napi_function: c.napi_value = undefined;
    if (c.napi_create_function(env, null, 0, function, null, &napi_function) != c.napi_ok) {
        return throw(env, "Failed to create function " ++ name ++ "().");
    }

    if (c.napi_set_named_property(env, exports, @as([*c]const u8, @ptrCast(name)), napi_function) != c.napi_ok) {
        return throw(env, "Failed to add " ++ name ++ "() to exports.");
    }
}

const TranslationError = error{ExceptionThrown};
pub fn throw(env: c.napi_env, comptime message: [:0]const u8) TranslationError {
    var result = c.napi_throw_error(env, null, @as([*c]const u8, @ptrCast(message)));
    switch (result) {
        c.napi_ok, c.napi_pending_exception => {},
        else => unreachable,
    }

    return TranslationError.ExceptionThrown;
}

pub fn capture_null(env: c.napi_env) !c.napi_value {
    var result: c.napi_value = undefined;
    if (c.napi_get_null(env, &result) != c.napi_ok) {
        return throw(env, "Failed to capture the value of \"null\".");
    }

    return result;
}

pub fn extract_args(env: c.napi_env, info: c.napi_callback_info, comptime args: struct {
    count: usize,
    function: []const u8,
}) ![args.count]c.napi_value {
    var argc = args.count;
    var argv: [args.count]c.napi_value = undefined;
    if (c.napi_get_cb_info(env, info, &argc, &argv, null, null) != c.napi_ok) {
        return throw(
            env,
            std.fmt.comptimePrint("Failed to get args for {s}()\x00", .{args.function}),
        );
    }

    if (argc != args.count) {
        return throw(
            env,
            std.fmt.comptimePrint("Function {s}() requires exactly {} arguments.\x00", .{
                args.function,
                args.count,
            }),
        );
    }

    return argv;
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

    return @as([*]u8, @ptrCast(data.?))[0..data_length];
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
    if (result > std.math.maxInt(u16)) {
        return throw(env, key ++ " must be a u16.");
    }

    return @as(u16, @intCast(result));
}

pub fn u128_from_value(env: c.napi_env, value: c.napi_value, comptime name: [:0]const u8) !u128 {
    // A BigInt's value (using ^ to mean exponent) is (words[0] * (2^64)^0 + words[1] * (2^64)^1 + ...)

    // V8 says that the words are little endian. If we were on a big endian machine
    // we would need to convert, but big endian is not supported by tigerbeetle.
    var result: u128 = 0;
    var sign_bit: c_int = undefined;
    const words = @as(*[2]u64, @ptrCast(&result));
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
        @as(*const [2]u64, @ptrCast(&value)),
        &bigint,
    ) != c.napi_ok) {
        return throw(env, error_message);
    }

    if (c.napi_set_named_property(env, object, @as([*c]const u8, @ptrCast(key)), bigint) != c.napi_ok) {
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

    if (c.napi_set_named_property(env, object, @as([*c]const u8, @ptrCast(key)), result) != c.napi_ok) {
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

    if (c.napi_set_named_property(env, object, @as([*c]const u8, @ptrCast(key)), result) != c.napi_ok) {
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
    try u32_into_object(env, object, key, value, error_message);
}

pub fn create_object(env: c.napi_env, comptime error_message: [:0]const u8) !c.napi_value {
    var result: c.napi_value = undefined;
    if (c.napi_create_object(env, &result) != c.napi_ok) {
        return throw(env, error_message);
    }

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

pub fn call_function(
    env: c.napi_env,
    this: c.napi_value,
    callback: c.napi_value,
    args: []c.napi_value,
) !c.napi_value {
    var result: c.napi_value = undefined;
    switch (c.napi_call_function(env, this, callback, args.len, args.ptr, &result)) {
        c.napi_ok => {},
        // the user's callback may throw a JS exception or call other functions that do so. We
        // therefore don't throw another error.
        c.napi_pending_exception => {},
        else => return throw(env, "Failed to invoke results callback."),
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
