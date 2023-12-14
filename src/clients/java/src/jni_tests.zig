///! This test hosts an in-process JVM
///! using the JNI Invocation API.
const std = @import("std");
const jni = @import("jni.zig");
const testing = std.testing;

const JavaVM = jni.JavaVM;
const JNIEnv = jni.JNIEnv;

test "JNI: check jvm" {
    const env: *JNIEnv = get_testing_env();

    var jvm: *JavaVM = undefined;
    const get_java_vm_result = env.get_java_vm(&jvm);
    try testing.expectEqual(jni.JNIResultType.ok, get_java_vm_result);

    var vm_buf: [2]*jni.JavaVM = undefined;
    var vm_len: jni.JSize = 0;
    const get_created_java_vm_result = JavaVM.get_created_java_vm(
        &vm_buf,
        @as(jni.JSize, @intCast(vm_buf.len)),
        &vm_len,
    );

    try testing.expectEqual(jni.JNIResultType.ok, get_created_java_vm_result);
    try testing.expect(vm_len == 1);
    try testing.expectEqual(jvm, vm_buf[0]);
}

test "JNI: GetVersion" {
    const env: *JNIEnv = get_testing_env();

    const version = env.get_version();
    try testing.expect(version >= jni.jni_version_10);
}

test "JNI: FindClass" {
    const env: *JNIEnv = get_testing_env();

    const object_class = env.find_class("java/lang/Object");
    try testing.expect(object_class != null);
    defer env.delete_local_ref(object_class);

    const not_found = env.find_class("no/such/Class");
    defer env.exception_clear();

    try testing.expect(not_found == null);
    try testing.expect(env.exception_check() == .jni_true);
}

test "JNI: GetSuperclass" {
    const env: *JNIEnv = get_testing_env();

    const object_class = env.find_class("java/lang/Object");
    try testing.expect(object_class != null);
    defer env.delete_local_ref(object_class);

    try testing.expect(env.get_super_class(object_class) == null);

    const string_class = env.find_class("java/lang/String");
    try testing.expect(string_class != null);
    defer env.delete_local_ref(string_class);

    const super_class = env.get_super_class(string_class);
    try testing.expect(super_class != null);
    defer env.delete_local_ref(super_class);

    try testing.expect(env.is_same_object(object_class, super_class) == .jni_true);
}

test "JNI: IsAssignableFrom" {
    const env: *JNIEnv = get_testing_env();

    const object_class = env.find_class("java/lang/Object");
    try testing.expect(object_class != null);
    defer env.delete_local_ref(object_class);

    try testing.expect(env.get_super_class(object_class) == null);

    const string_class = env.find_class("java/lang/String");
    try testing.expect(string_class != null);
    defer env.delete_local_ref(string_class);

    const long_class = env.find_class("java/lang/Long");
    try testing.expect(long_class != null);
    defer env.delete_local_ref(long_class);

    try testing.expect(env.is_assignable_from(long_class, object_class) == .jni_true);
    try testing.expect(env.is_assignable_from(long_class, string_class) == .jni_false);
}

test "JNI: GetModule" {
    const env: *JNIEnv = get_testing_env();

    const exception_class = env.find_class("java/lang/Exception");
    try testing.expect(exception_class != null);
    defer env.delete_local_ref(exception_class);

    const module = env.get_module(exception_class);
    try testing.expect(module != null);
    defer env.delete_local_ref(module);
}

test "JNI: Throw" {
    const env: *JNIEnv = get_testing_env();

    const exception_class = env.find_class("java/lang/Exception");
    try testing.expect(exception_class != null);
    defer env.delete_local_ref(exception_class);

    const exception = env.alloc_object(exception_class);
    try testing.expect(exception != null);
    defer env.delete_local_ref(exception);

    const throw_result = env.throw(exception);
    try testing.expectEqual(jni.JNIResultType.ok, throw_result);
    defer env.exception_clear();

    try testing.expect(env.exception_check() == .jni_true);
}

test "JNI: ThrowNew" {
    const env: *JNIEnv = get_testing_env();

    const exception_class = env.find_class("java/lang/Exception");
    try testing.expect(exception_class != null);
    defer env.delete_local_ref(exception_class);

    try testing.expect(env.exception_check() == .jni_false);

    const throw_new_result = env.throw_new(exception_class, "");
    try testing.expectEqual(jni.JNIResultType.ok, throw_new_result);
    defer env.exception_clear();

    try testing.expect(env.exception_check() == .jni_true);
}

test "JNI: ExceptionOccurred" {
    const env: *JNIEnv = get_testing_env();

    try testing.expect(env.exception_occurred() == null);

    const exception_class = env.find_class("java/lang/Exception");
    try testing.expect(exception_class != null);
    defer env.delete_local_ref(exception_class);

    const throw_new_result = env.throw_new(exception_class, "");
    try testing.expectEqual(jni.JNIResultType.ok, throw_new_result);

    const exception_occurred = env.exception_occurred();
    try testing.expect(exception_occurred != null);
    defer env.delete_local_ref(exception_occurred);

    env.exception_clear();

    try testing.expect(env.exception_occurred() == null);
}

test "JNI: ExceptionDescribe" {
    const env: *JNIEnv = get_testing_env();

    try testing.expect(env.exception_check() == .jni_false);

    const exception_class = env.find_class("java/lang/Exception");
    try testing.expect(exception_class != null);
    defer env.delete_local_ref(exception_class);

    const throw_new_result = env.throw_new(
        exception_class,
        "EXCEPTION DESCRIBED CORRECTLY",
    );
    try testing.expectEqual(jni.JNIResultType.ok, throw_new_result);
    try testing.expect(env.exception_check() == .jni_true);
    defer env.exception_clear();

    env.exception_describe();
}

test "JNI: ExceptionClear" {
    const env: *JNIEnv = get_testing_env();

    // Asserting that calling it is a no-op here:
    try testing.expect(env.exception_check() == .jni_false);
    env.exception_clear();
    try testing.expect(env.exception_check() == .jni_false);

    const exception_class = env.find_class("java/lang/Exception");
    try testing.expect(exception_class != null);
    defer env.delete_local_ref(exception_class);

    const throw_new_result = env.throw_new(exception_class, "");
    try testing.expectEqual(jni.JNIResultType.ok, throw_new_result);

    // Asserting that calling it clears the current exception:
    try testing.expect(env.exception_check() == .jni_true);
    env.exception_clear();
    try testing.expect(env.exception_check() == .jni_false);
}

test "JNI: ExceptionCheck" {
    const env: *JNIEnv = get_testing_env();

    try testing.expect(env.exception_check() == .jni_false);

    const object_class = env.find_class("java/lang/Object");
    try testing.expect(object_class != null);
    defer env.delete_local_ref(object_class);

    const to_string = env.get_method_id(object_class, "toString", "()Ljava/lang/String;");
    try testing.expect(to_string != null);

    // Expected null reference exception:
    const result = env.call_object_method(null, to_string, null);
    try testing.expect(result == null);

    try testing.expect(env.exception_check() == .jni_true);
    env.exception_clear();
    try testing.expect(env.exception_check() == .jni_false);
}

test "JNI: References" {
    const env: *JNIEnv = get_testing_env();

    const boolean_class = env.find_class("java/lang/Boolean");
    try testing.expect(boolean_class != null);
    defer env.delete_local_ref(boolean_class);

    const obj = env.alloc_object(boolean_class);
    defer env.delete_local_ref(obj);
    try testing.expect(obj != null);
    try testing.expect(env.get_object_ref_type(obj) == .local);

    // Local ref:
    {
        const local_ref = env.new_local_ref(obj);
        try testing.expect(local_ref != null);

        try testing.expect(env.is_same_object(obj, local_ref) == .jni_true);
        try testing.expect(env.get_object_ref_type(local_ref) == .local);

        env.delete_local_ref(local_ref);
        try testing.expect(env.new_local_ref(local_ref) == null);
    }

    // Global ref:
    {
        const global_ref = env.new_global_ref(obj);
        try testing.expect(global_ref != null);

        try testing.expect(env.is_same_object(obj, global_ref) == .jni_true);
        try testing.expect(env.get_object_ref_type(global_ref) == .global);

        env.delete_global_ref(global_ref);
        try testing.expect(env.get_object_ref_type(global_ref) == .invalid);
    }

    // Weak global ref:
    {
        const weak_global_ref = env.new_weak_global_ref(obj);
        try testing.expect(weak_global_ref != null);

        try testing.expect(env.is_same_object(obj, weak_global_ref) == .jni_true);
        try testing.expect(env.get_object_ref_type(weak_global_ref) == .weak_global);

        env.delete_weak_global_ref(weak_global_ref);
        try testing.expect(env.get_object_ref_type(weak_global_ref) == .invalid);
    }
}

test "JNI: LocalFrame" {
    const env: *JNIEnv = get_testing_env();

    // Creating a new local frame.
    const push_local_frame_result = env.push_local_frame(1);
    try testing.expectEqual(jni.JNIResultType.ok, push_local_frame_result);

    const ensure_local_capacity_result = env.ensure_local_capacity(10);
    try testing.expectEqual(jni.JNIResultType.ok, ensure_local_capacity_result);

    const boolean_class = env.find_class("java/lang/Boolean");
    try testing.expect(boolean_class != null);
    defer env.delete_local_ref(boolean_class);

    const local_ref = env.alloc_object(boolean_class);
    try testing.expect(local_ref != null);

    // All local references must be invalidated after this frame being droped,
    // except by the frame result.
    const pop_local_frame_result = env.pop_local_frame(local_ref);
    try testing.expect(pop_local_frame_result != null);
    defer env.delete_local_ref(pop_local_frame_result);

    const valid_reference = env.get_object_ref_type(pop_local_frame_result);
    try testing.expect(valid_reference == .local);
    try testing.expect(pop_local_frame_result != local_ref);
}

test "JNI: AllocObject" {
    const env: *JNIEnv = get_testing_env();

    // Concrete type:
    {
        const boolean_class = env.find_class("java/lang/Boolean");
        try testing.expect(boolean_class != null);
        defer env.delete_local_ref(boolean_class);

        const obj = env.alloc_object(boolean_class);
        defer env.delete_local_ref(obj);
        try testing.expect(obj != null);
    }

    // Interface:
    {
        const serializable_interface = env.find_class("java/io/Serializable");
        try testing.expect(serializable_interface != null);
        defer env.delete_local_ref(serializable_interface);

        const obj = env.alloc_object(serializable_interface);
        defer env.exception_clear();
        try testing.expect(obj == null);
        try testing.expect(env.exception_check() == .jni_true);
    }

    // Abstract class:
    {
        const calendar_abstract_class = env.find_class("java/util/Calendar");
        try testing.expect(calendar_abstract_class != null);
        defer env.delete_local_ref(calendar_abstract_class);

        const obj = env.alloc_object(calendar_abstract_class);
        defer env.exception_clear();
        try testing.expect(obj == null);
        try testing.expect(env.exception_check() == .jni_true);
    }
}

test "JNI: NewObject" {
    const env: *JNIEnv = get_testing_env();

    const string_buffer_class = env.find_class("java/lang/StringBuffer");
    try testing.expect(string_buffer_class != null);
    defer env.delete_local_ref(string_buffer_class);

    const capacity_ctor = env.get_method_id(string_buffer_class, "<init>", "(I)V");
    try testing.expect(capacity_ctor != null);

    const capacity: jni.JInt = 42;
    const obj = env.new_object(
        string_buffer_class,
        capacity_ctor,
        &[_]jni.JValue{jni.JValue.to_jvalue(capacity)},
    );
    try testing.expect(obj != null);
    defer env.delete_local_ref(obj);
}

test "JNI: IsInstanceOf" {
    const env: *JNIEnv = get_testing_env();

    const boolean_class = env.find_class("java/lang/Boolean");
    try testing.expect(boolean_class != null);
    defer env.delete_local_ref(boolean_class);

    const obj = env.alloc_object(boolean_class);
    defer env.delete_local_ref(obj);
    try testing.expect(obj != null);

    try testing.expect(env.is_instance_of(obj, boolean_class) == .jni_true);

    const long_class = env.find_class("java/lang/Long");
    try testing.expect(long_class != null);
    defer env.delete_local_ref(long_class);

    try testing.expect(env.is_instance_of(obj, long_class) == .jni_false);
}

test "JNI: GetFieldId" {
    const env: *JNIEnv = get_testing_env();

    const boolean_class = env.find_class("java/lang/Boolean");
    try testing.expect(boolean_class != null);
    defer env.delete_local_ref(boolean_class);

    const value_field_id = env.get_field_id(boolean_class, "value", "Z");
    try testing.expect(value_field_id != null);

    const invalid_field = env.get_field_id(boolean_class, "not_a_valid_field", "I");
    defer env.exception_clear();
    try testing.expect(invalid_field == null);
    try testing.expect(env.exception_check() == .jni_true);
}

test "JNI: Get<Type>Field, Set<Type>Field" {
    const env: *JNIEnv = get_testing_env();

    // Boolean:
    {
        const boolean_class = env.find_class("java/lang/Boolean");
        try testing.expect(boolean_class != null);
        defer env.delete_local_ref(boolean_class);

        const value_field_id = env.get_field_id(boolean_class, "value", "Z");
        try testing.expect(value_field_id != null);

        const obj = env.alloc_object(boolean_class);
        defer env.delete_local_ref(obj);
        try testing.expect(obj != null);

        const value_before = env.get_boolean_field(obj, value_field_id);
        try testing.expect(value_before == .jni_false);

        env.set_boolean_field(obj, value_field_id, .jni_true);
        try testing.expect(env.exception_check() == .jni_false);

        const value_after = env.get_boolean_field(obj, value_field_id);
        try testing.expect(value_after == .jni_true);
    }

    // Byte:
    {
        const byte_class = env.find_class("java/lang/Byte");
        try testing.expect(byte_class != null);
        defer env.delete_local_ref(byte_class);

        const value_field_id = env.get_field_id(byte_class, "value", "B");
        try testing.expect(value_field_id != null);

        const obj = env.alloc_object(byte_class);
        defer env.delete_local_ref(obj);
        try testing.expect(obj != null);

        const value_before = env.get_byte_field(obj, value_field_id);
        try testing.expect(value_before == 0);

        env.set_byte_field(obj, value_field_id, 127);
        try testing.expect(env.exception_check() == .jni_false);

        const value_after = env.get_byte_field(obj, value_field_id);
        try testing.expect(value_after == 127);
    }

    // Char:
    {
        const char_class = env.find_class("java/lang/Character");
        try testing.expect(char_class != null);
        defer env.delete_local_ref(char_class);

        const value_field_id = env.get_field_id(char_class, "value", "C");
        try testing.expect(value_field_id != null);

        const obj = env.alloc_object(char_class);
        defer env.delete_local_ref(obj);
        try testing.expect(obj != null);

        const value_before = env.get_char_field(obj, value_field_id);
        try testing.expect(value_before == 0);

        env.set_char_field(obj, value_field_id, 'A');
        try testing.expect(env.exception_check() == .jni_false);

        const value_after = env.get_char_field(obj, value_field_id);
        try testing.expect(value_after == 'A');
    }

    // Short:
    {
        const short_class = env.find_class("java/lang/Short");
        try testing.expect(short_class != null);
        defer env.delete_local_ref(short_class);

        const value_field_id = env.get_field_id(short_class, "value", "S");
        try testing.expect(value_field_id != null);

        const obj = env.alloc_object(short_class);
        defer env.delete_local_ref(obj);
        try testing.expect(obj != null);

        const value_before = env.get_short_field(obj, value_field_id);
        try testing.expect(value_before == 0);

        env.set_short_field(obj, value_field_id, 9999);
        try testing.expect(env.exception_check() == .jni_false);

        const value_after = env.get_short_field(obj, value_field_id);
        try testing.expect(value_after == 9999);
    }

    // Int:
    {
        const int_class = env.find_class("java/lang/Integer");
        try testing.expect(int_class != null);
        defer env.delete_local_ref(int_class);

        const value_field_id = env.get_field_id(int_class, "value", "I");
        try testing.expect(value_field_id != null);

        const obj = env.alloc_object(int_class);
        defer env.delete_local_ref(obj);
        try testing.expect(obj != null);

        const value_before = env.get_int_field(obj, value_field_id);
        try testing.expect(value_before == 0);

        env.set_int_field(obj, value_field_id, 999_999);
        try testing.expect(env.exception_check() == .jni_false);

        const value_after = env.get_int_field(obj, value_field_id);
        try testing.expect(value_after == 999_999);
    }

    // Long:
    {
        const long_class = env.find_class("java/lang/Long");
        try testing.expect(long_class != null);
        defer env.delete_local_ref(long_class);

        const value_field_id = env.get_field_id(long_class, "value", "J");
        try testing.expect(value_field_id != null);

        const obj = env.alloc_object(long_class);
        defer env.delete_local_ref(obj);
        try testing.expect(obj != null);

        const value_before = env.get_long_field(obj, value_field_id);
        try testing.expect(value_before == 0);

        env.set_long_field(obj, value_field_id, 9_999_999_999);
        try testing.expect(env.exception_check() == .jni_false);

        const value_after = env.get_long_field(obj, value_field_id);
        try testing.expect(value_after == 9_999_999_999);
    }

    // Float:
    {
        const float_class = env.find_class("java/lang/Float");
        try testing.expect(float_class != null);
        defer env.delete_local_ref(float_class);

        const value_field_id = env.get_field_id(float_class, "value", "F");
        try testing.expect(value_field_id != null);

        const obj = env.alloc_object(float_class);
        defer env.delete_local_ref(obj);
        try testing.expect(obj != null);

        const value_before = env.get_float_field(obj, value_field_id);
        try testing.expect(value_before == 0);

        env.set_float_field(obj, value_field_id, 9.99);
        try testing.expect(env.exception_check() == .jni_false);

        const value_after = env.get_float_field(obj, value_field_id);
        try testing.expect(value_after == 9.99);
    }

    // Double:
    {
        const double_class = env.find_class("java/lang/Double");
        try testing.expect(double_class != null);
        defer env.delete_local_ref(double_class);

        const value_field_id = env.get_field_id(double_class, "value", "D");
        try testing.expect(value_field_id != null);

        const obj = env.alloc_object(double_class);
        defer env.delete_local_ref(obj);
        try testing.expect(obj != null);

        const value_before = env.get_double_field(obj, value_field_id);
        try testing.expect(value_before == 0);

        env.set_double_field(obj, value_field_id, 9.99);
        try testing.expect(env.exception_check() == .jni_false);

        const value_after = env.get_double_field(obj, value_field_id);
        try testing.expect(value_after == 9.99);
    }

    // Object:
    {
        const object_class = env.find_class("java/lang/Throwable");
        try testing.expect(object_class != null);
        defer env.delete_local_ref(object_class);

        const value_field_id = env.get_field_id(object_class, "cause", "Ljava/lang/Throwable;");
        try testing.expect(value_field_id != null);

        const obj = env.alloc_object(object_class);
        defer env.delete_local_ref(obj);
        try testing.expect(obj != null);

        const value_before = env.get_object_field(obj, value_field_id);
        try testing.expect(value_before == null);

        env.set_object_field(obj, value_field_id, obj);
        try testing.expect(env.exception_check() == .jni_false);

        const value_after = env.get_object_field(obj, value_field_id);
        try testing.expect(value_after != null);
    }
}

test "JNI: GetMethodId" {
    const env: *JNIEnv = get_testing_env();

    const object_class = env.find_class("java/lang/Throwable");
    try testing.expect(object_class != null);
    defer env.delete_local_ref(object_class);

    const method_id = env.get_method_id(object_class, "toString", "()Ljava/lang/String;");
    try testing.expect(method_id != null);

    const invalid_method = env.get_method_id(object_class, "invalid_method", "()V");
    defer env.exception_clear();
    try testing.expect(invalid_method == null);
    try testing.expect(env.exception_check() == .jni_true);
}

test "JNI: Call<Type>Method,CallNonVirtual<Type>Method" {
    const env: *JNIEnv = get_testing_env();

    const buffer_class = env.find_class("java/nio/ByteBuffer");
    try testing.expect(buffer_class != null);
    defer env.delete_local_ref(buffer_class);

    const direct_buffer_class = env.find_class("java/nio/DirectByteBuffer");
    try testing.expect(direct_buffer_class != null);
    defer env.delete_local_ref(direct_buffer_class);

    const element: u8 = 42;
    var native_buffer = [_]u8{element} ** 256;
    const buffer = env.new_direct_byte_buffer(&native_buffer, @as(jni.JSize, @intCast(native_buffer.len)));
    try testing.expect(buffer != null);
    defer env.delete_local_ref(buffer);

    // Byte:
    {
        const method_id = env.get_method_id(buffer_class, "get", "()B");
        try testing.expect(method_id != null);

        const non_virtual_method_id = env.get_method_id(direct_buffer_class, "get", "()B");
        try testing.expect(non_virtual_method_id != null);

        const expected = @as(jni.JByte, @bitCast(element));

        const read = env.call_byte_method(buffer, method_id, null);
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expectEqual(expected, read);

        const read_non_virtual =
            env.call_nonvirtual_byte_method(buffer, direct_buffer_class, non_virtual_method_id, null);
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expectEqual(expected, read_non_virtual);
    }

    // Short:
    {
        const method_id = env.get_method_id(buffer_class, "getShort", "()S");
        try testing.expect(method_id != null);

        const non_virtual_method_id = env.get_method_id(direct_buffer_class, "getShort", "()S");
        try testing.expect(non_virtual_method_id != null);

        const Packed = packed struct { a: u8, b: u8 };
        const expected = @as(jni.JShort, @bitCast(Packed{
            .a = element,
            .b = element,
        }));

        const read = env.call_short_method(buffer, method_id, null);
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expectEqual(expected, read);

        const read_non_virtual =
            env.call_nonvirtual_short_method(buffer, direct_buffer_class, non_virtual_method_id, null);
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expectEqual(expected, read_non_virtual);
    }

    // Char:
    {
        const method_id = env.get_method_id(buffer_class, "getChar", "()C");
        try testing.expect(method_id != null);

        const non_virtual_method_id = env.get_method_id(direct_buffer_class, "getChar", "()C");
        try testing.expect(non_virtual_method_id != null);

        const Packed = packed struct { a: u8, b: u8 };
        const expected = @as(jni.JChar, @bitCast(Packed{
            .a = element,
            .b = element,
        }));

        const read = env.call_char_method(buffer, method_id, null);
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expectEqual(expected, read);

        const read_non_virtual =
            env.call_nonvirtual_char_method(buffer, direct_buffer_class, non_virtual_method_id, null);
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expectEqual(expected, read_non_virtual);
    }

    // Int:
    {
        const method_id = env.get_method_id(buffer_class, "getInt", "()I");
        try testing.expect(method_id != null);

        const non_virtual_method_id = env.get_method_id(direct_buffer_class, "getInt", "()I");
        try testing.expect(non_virtual_method_id != null);

        const Packed = packed struct { a: u8, b: u8, c: u8, d: u8 };
        const expected = @as(jni.JInt, @bitCast(Packed{
            .a = element,
            .b = element,
            .c = element,
            .d = element,
        }));

        const read = env.call_int_method(buffer, method_id, null);
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expectEqual(expected, read);

        const read_non_virtual =
            env.call_nonvirtual_int_method(buffer, direct_buffer_class, non_virtual_method_id, null);
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expectEqual(expected, read_non_virtual);
    }

    // Long:
    {
        const method_id = env.get_method_id(buffer_class, "getLong", "()J");
        try testing.expect(method_id != null);

        const non_virtual_method_id = env.get_method_id(direct_buffer_class, "getLong", "()J");
        try testing.expect(non_virtual_method_id != null);

        const Packed = packed struct { a: u8, b: u8, c: u8, d: u8, e: u8, f: u8, g: u8, h: u8 };
        const expected = @as(jni.JLong, @bitCast(Packed{
            .a = element,
            .b = element,
            .c = element,
            .d = element,
            .e = element,
            .f = element,
            .g = element,
            .h = element,
        }));

        const read = env.call_long_method(buffer, method_id, null);
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expectEqual(expected, read);

        const read_non_virtual =
            env.call_nonvirtual_long_method(buffer, direct_buffer_class, non_virtual_method_id, null);
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expectEqual(expected, read_non_virtual);
    }

    // Float:
    {
        const method_id = env.get_method_id(buffer_class, "getFloat", "()F");
        try testing.expect(method_id != null);

        const non_virtual_method_id = env.get_method_id(direct_buffer_class, "getFloat", "()F");
        try testing.expect(non_virtual_method_id != null);

        const Packed = packed struct { a: u8, b: u8, c: u8, d: u8 };
        const expected = @as(jni.JFloat, @bitCast(Packed{
            .a = element,
            .b = element,
            .c = element,
            .d = element,
        }));

        const read = env.call_float_method(buffer, method_id, null);
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expectEqual(expected, read);

        const read_non_virtual =
            env.call_nonvirtual_float_method(buffer, direct_buffer_class, non_virtual_method_id, null);
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expectEqual(expected, read_non_virtual);
    }

    // Double:
    {
        const method_id = env.get_method_id(buffer_class, "getDouble", "()D");
        try testing.expect(method_id != null);

        const non_virtual_method_id = env.get_method_id(direct_buffer_class, "getDouble", "()D");
        try testing.expect(non_virtual_method_id != null);

        const Packed = packed struct { a: u8, b: u8, c: u8, d: u8, e: u8, f: u8, g: u8, h: u8 };
        const expected = @as(jni.JDouble, @bitCast(Packed{
            .a = element,
            .b = element,
            .c = element,
            .d = element,
            .e = element,
            .f = element,
            .g = element,
            .h = element,
        }));

        const read = env.call_double_method(buffer, method_id, null);
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expectEqual(expected, read);

        const read_non_virtual =
            env.call_nonvirtual_double_method(buffer, direct_buffer_class, non_virtual_method_id, null);
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expectEqual(expected, read_non_virtual);
    }

    // Object:
    {
        const method_id = env.get_method_id(buffer_class, "put", "(B)Ljava/nio/ByteBuffer;");
        try testing.expect(method_id != null);

        const non_virtual_method_id = env.get_method_id(direct_buffer_class, "put", "(B)Ljava/nio/ByteBuffer;");
        try testing.expect(non_virtual_method_id != null);

        const put = env.call_object_method(buffer, method_id, &[_]jni.JValue{
            jni.JValue.to_jvalue(@as(jni.JByte, 0)),
        });
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expect(env.is_same_object(buffer, put) == .jni_true);
        defer env.delete_local_ref(put);

        const put_non_virtual = env.call_nonvirtual_object_method(
            buffer,
            direct_buffer_class,
            non_virtual_method_id,
            &[_]jni.JValue{jni.JValue.to_jvalue(@as(jni.JByte, 0))},
        );
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expect(env.is_same_object(buffer, put_non_virtual) == .jni_true);
        defer env.delete_local_ref(put_non_virtual);
    }
}

test "JNI: GetStaticFieldId" {
    const env: *JNIEnv = get_testing_env();

    const boolean_class = env.find_class("java/lang/Boolean");
    try testing.expect(boolean_class != null);
    defer env.delete_local_ref(boolean_class);

    const field_id = env.get_static_field_id(boolean_class, "serialVersionUID", "J");
    try testing.expect(field_id != null);

    const invalid_field_id = env.get_static_field_id(boolean_class, "invalid_field", "J");
    defer env.exception_clear();
    try testing.expect(invalid_field_id == null);
    try testing.expect(env.exception_check() == .jni_true);
}

test "JNI: GetStatic<Type>Field, SetStatic<Type>Field" {
    const env: *JNIEnv = get_testing_env();

    // Byte:
    {
        const class = env.find_class("java/lang/Byte");
        try testing.expect(class != null);
        defer env.delete_local_ref(class);

        const field_id = env.get_static_field_id(class, "MIN_VALUE", "B");
        try testing.expect(field_id != null);

        const before = env.get_static_byte_field(class, field_id);
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expect(before == -128);

        env.set_static_byte_field(class, field_id, -127);
        try testing.expect(env.exception_check() == .jni_false);

        const after = env.get_static_byte_field(class, field_id);
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expect(after == -127);
    }

    // Char:
    {
        const class = env.find_class("java/lang/Character");
        try testing.expect(class != null);
        defer env.delete_local_ref(class);

        const field_id = env.get_static_field_id(class, "MIN_VALUE", "C");
        try testing.expect(field_id != null);

        const before = env.get_static_char_field(class, field_id);
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expect(before == 0);

        env.set_static_char_field(class, field_id, 1);
        try testing.expect(env.exception_check() == .jni_false);

        const after = env.get_static_char_field(class, field_id);
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expect(after == 1);
    }

    // Short:
    {
        const class = env.find_class("java/lang/Short");
        try testing.expect(class != null);
        defer env.delete_local_ref(class);

        const field_id = env.get_static_field_id(class, "MIN_VALUE", "S");
        try testing.expect(field_id != null);

        const before = env.get_static_short_field(class, field_id);
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expect(before == -32768);

        env.set_static_short_field(class, field_id, -32767);
        try testing.expect(env.exception_check() == .jni_false);

        const after = env.get_static_short_field(class, field_id);
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expect(after == -32767);
    }

    // Int:
    {
        const class = env.find_class("java/lang/Integer");
        try testing.expect(class != null);
        defer env.delete_local_ref(class);

        const field_id = env.get_static_field_id(class, "MIN_VALUE", "I");
        try testing.expect(field_id != null);

        const before = env.get_static_int_field(class, field_id);
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expect(before == -2147483648);

        env.set_static_int_field(class, field_id, -2147483647);
        try testing.expect(env.exception_check() == .jni_false);

        const after = env.get_static_int_field(class, field_id);
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expect(after == -2147483647);
    }

    // Long:
    {
        const class = env.find_class("java/lang/Long");
        try testing.expect(class != null);
        defer env.delete_local_ref(class);

        const field_id = env.get_static_field_id(class, "MIN_VALUE", "J");
        try testing.expect(field_id != null);

        const before = env.get_static_long_field(class, field_id);
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expect(before == -9223372036854775808);

        env.set_static_long_field(class, field_id, -9223372036854775807);
        try testing.expect(env.exception_check() == .jni_false);

        const after = env.get_static_long_field(class, field_id);
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expect(after == -9223372036854775807);
    }

    // Float:
    {
        const class = env.find_class("java/lang/Float");
        try testing.expect(class != null);
        defer env.delete_local_ref(class);

        const field_id = env.get_static_field_id(class, "MIN_VALUE", "F");
        try testing.expect(field_id != null);

        const before = env.get_static_float_field(class, field_id);
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expect(before == 1.4E-45);

        env.set_static_float_field(class, field_id, 1.4E-44);
        try testing.expect(env.exception_check() == .jni_false);

        const after = env.get_static_float_field(class, field_id);
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expect(after == 1.4E-44);
    }

    // Double:
    {
        const class = env.find_class("java/lang/Double");
        try testing.expect(class != null);
        defer env.delete_local_ref(class);

        const field_id = env.get_static_field_id(class, "MIN_VALUE", "D");
        try testing.expect(field_id != null);

        const before = env.get_static_double_field(class, field_id);
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expect(before == 4.9E-324);

        env.set_static_double_field(class, field_id, 4.9E-323);
        try testing.expect(env.exception_check() == .jni_false);

        const after = env.get_static_double_field(class, field_id);
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expect(after == 4.9E-323);
    }
}

test "JNI: GetStaticMethodId" {
    const env: *JNIEnv = get_testing_env();

    const boolean_class = env.find_class("java/lang/Boolean");
    try testing.expect(boolean_class != null);
    defer env.delete_local_ref(boolean_class);

    const method_id = env.get_static_method_id(boolean_class, "valueOf", "(Z)Ljava/lang/Boolean;");
    try testing.expect(method_id != null);

    const invalid_method_id = env.get_static_method_id(boolean_class, "invalid_method", "()J");
    defer env.exception_clear();
    try testing.expect(invalid_method_id == null);
    try testing.expect(env.exception_check() == .jni_true);
}

test "JNI: CallStatic<Type>Method" {
    const env: *JNIEnv = get_testing_env();

    const str = env.new_string_utf("1");
    try testing.expect(str != null);
    defer env.delete_local_ref(str);

    // Boolean:
    {
        const class = env.find_class("java/lang/Boolean");
        try testing.expect(class != null);
        defer env.delete_local_ref(class);

        const method_id = env.get_static_method_id(class, "parseBoolean", "(Ljava/lang/String;)Z");
        try testing.expect(method_id != null);

        const ret = env.call_static_boolean_method(class, method_id, &[_]jni.JValue{jni.JValue.to_jvalue(str)});
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expect(ret == .jni_false);
    }

    // Byte:
    {
        const class = env.find_class("java/lang/Byte");
        try testing.expect(class != null);
        defer env.delete_local_ref(class);

        const method_id = env.get_static_method_id(class, "parseByte", "(Ljava/lang/String;)B");
        try testing.expect(method_id != null);

        const ret = env.call_static_byte_method(class, method_id, &[_]jni.JValue{jni.JValue.to_jvalue(str)});
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expect(ret == 1);
    }

    // Char:
    {
        const class = env.find_class("java/lang/Character");
        try testing.expect(class != null);
        defer env.delete_local_ref(class);

        const method_id = env.get_static_method_id(class, "toLowerCase", "(C)C");
        try testing.expect(method_id != null);

        var ret = env.call_static_char_method(class, method_id, &[_]jni.JValue{jni.JValue.to_jvalue(@as(jni.JChar, 'A'))});
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expect(ret == 'a');
    }

    // Short:
    {
        const class = env.find_class("java/lang/Short");
        try testing.expect(class != null);
        defer env.delete_local_ref(class);

        const method_id = env.get_static_method_id(class, "parseShort", "(Ljava/lang/String;)S");
        try testing.expect(method_id != null);

        const ret = env.call_static_short_method(class, method_id, &[_]jni.JValue{jni.JValue.to_jvalue(str)});
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expect(ret == 1);
    }

    // Int:
    {
        const class = env.find_class("java/lang/Integer");
        try testing.expect(class != null);
        defer env.delete_local_ref(class);

        const method_id = env.get_static_method_id(class, "parseInt", "(Ljava/lang/String;)I");
        try testing.expect(method_id != null);

        var ret = env.call_static_int_method(class, method_id, &[_]jni.JValue{jni.JValue.to_jvalue(str)});
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expect(ret == 1);
    }

    // Long:
    {
        const class = env.find_class("java/lang/Long");
        try testing.expect(class != null);
        defer env.delete_local_ref(class);

        const method_id = env.get_static_method_id(class, "parseLong", "(Ljava/lang/String;)J");
        try testing.expect(method_id != null);

        const ret = env.call_static_long_method(class, method_id, &[_]jni.JValue{jni.JValue.to_jvalue(str)});
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expect(ret == 1);
    }

    // Float:
    {
        const class = env.find_class("java/lang/Float");
        try testing.expect(class != null);
        defer env.delete_local_ref(class);

        const method_id = env.get_static_method_id(class, "parseFloat", "(Ljava/lang/String;)F");
        try testing.expect(method_id != null);

        const ret = env.call_static_float_method(class, method_id, &[_]jni.JValue{jni.JValue.to_jvalue(str)});
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expect(ret == 1.0);
    }

    // Double:
    {
        const class = env.find_class("java/lang/Double");
        try testing.expect(class != null);
        defer env.delete_local_ref(class);

        const method_id = env.get_static_method_id(class, "parseDouble", "(Ljava/lang/String;)D");
        try testing.expect(method_id != null);

        var ret = env.call_static_double_method(class, method_id, &[_]jni.JValue{jni.JValue.to_jvalue(str)});
        try testing.expect(env.exception_check() == .jni_false);
        try testing.expect(ret == 1.0);
    }

    // Object:
    {
        const class = env.find_class("java/lang/String");
        try testing.expect(class != null);
        defer env.delete_local_ref(class);

        const method_id = env.get_static_method_id(class, "valueOf", "(Ljava/lang/Object;)Ljava/lang/String;");
        try testing.expect(method_id != null);

        const ret = env.call_static_object_method(class, method_id, &[_]jni.JValue{jni.JValue.to_jvalue(str)});
        try testing.expect(env.exception_check() == .jni_false);
        defer env.delete_local_ref(ret);
        try testing.expect(env.is_instance_of(ret, class) == .jni_true);
    }

    // Void:
    {
        const class = env.find_class("java/lang/System");
        try testing.expect(class != null);
        defer env.delete_local_ref(class);

        const method_id = env.get_static_method_id(class, "gc", "()V");
        try testing.expect(method_id != null);

        env.call_static_void_method(class, method_id, null);
        try testing.expect(env.exception_check() == .jni_false);
    }
}

test "JNI: strings" {
    const env: *JNIEnv = get_testing_env();

    const content: []const u16 = std.unicode.utf8ToUtf16LeStringLiteral("Hello utf16")[0..];
    const string = env.new_string(
        content.ptr,
        @as(jni.JSize, @intCast(content.len)),
    );
    try testing.expect(string != null);
    defer env.delete_local_ref(string);

    const len = env.get_string_length(string);
    try testing.expectEqual(content.len, @as(usize, @intCast(len)));

    const address = env.get_string_chars(string, null) orelse {
        try testing.expect(false);
        unreachable;
    };
    defer env.release_string_chars(string, address);

    try testing.expectEqualSlices(u16, content[0..], address[0..@as(usize, @intCast(len))]);
}

test "JNI: strings utf" {
    const env: *JNIEnv = get_testing_env();

    const content = "Hello utf8";
    const string = env.new_string_utf(content);
    try testing.expect(string != null);
    defer env.delete_local_ref(string);

    const len = env.get_string_utf_length(string);
    try testing.expectEqual(content.len, @as(usize, @intCast(len)));

    const address = env.get_string_utf_chars(string, null) orelse {
        try testing.expect(false);
        unreachable;
    };
    defer env.release_string_utf_chars(string, address);

    try testing.expectEqualSlices(u8, content[0..], address[0..@as(usize, @intCast(len))]);
}

test "JNI: GetStringRegion" {
    const env: *JNIEnv = get_testing_env();

    const content: []const u16 = std.unicode.utf8ToUtf16LeStringLiteral("ABCDEFGHIJKLMNOPQRSTUVXYZ")[0..];
    const string = env.new_string(
        content.ptr,
        @as(jni.JSize, @intCast(content.len)),
    );
    try testing.expect(string != null);
    defer env.delete_local_ref(string);

    var buff: [10]jni.JChar = undefined;
    env.get_string_region(string, 5, 10, &buff);
    try testing.expect(env.exception_check() == .jni_false);
    try testing.expectEqualSlices(u16, content[5..][0..10], &buff);
}

test "JNI: GetStringUTFRegion" {
    const env: *JNIEnv = get_testing_env();

    const content = "ABCDEFGHIJKLMNOPQRSTUVXYZ";
    const string = env.new_string_utf(content);
    try testing.expect(string != null);
    defer env.delete_local_ref(string);

    // From: https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getstringutfregion.
    // The resulting number modified UTF-8 encoding characters may be greater than
    // the given len argument. GetStringUTFLength() may be used to determine the
    // maximum size of the required character buffer.

    var buff: [content.len]u8 = undefined;
    env.get_string_utf_region(string, 5, 10, &buff);
    try testing.expect(env.exception_check() == .jni_false);
    try testing.expectEqualSlices(u8, content[5..][0..10], buff[0..10]);
}

test "JNI: GetStringCritical" {
    const env: *JNIEnv = get_testing_env();

    const content: []const u16 = std.unicode.utf8ToUtf16LeStringLiteral("ABCDEFGHIJKLMNOPQRSTUVXYZ")[0..];
    const str = env.new_string(content.ptr, @as(jni.JSize, @intCast(content.len)));
    try testing.expect(str != null);
    defer env.delete_local_ref(str);

    const len = env.get_string_length(str);
    try testing.expectEqual(content.len, @as(usize, @intCast(len)));

    const region = env.get_string_critical(str, null) orelse {
        try testing.expect(false);
        unreachable;
    };
    defer env.release_string_critical(str, region);

    try testing.expectEqualSlices(u16, content, region[0..@as(usize, @intCast(len))]);
}

test "JNI: DirectByteBuffer" {
    const env: *JNIEnv = get_testing_env();

    var native_buffer = blk: {
        var array: [32]u8 = undefined;
        var value: u8 = array.len;
        for (&array) |*byte| {
            value -= 1;
            byte.* = value;
        }
        break :blk array;
    };

    const buffer = env.new_direct_byte_buffer(&native_buffer, native_buffer.len);
    try testing.expect(buffer != null);
    defer env.delete_local_ref(buffer);

    const capacity = env.get_direct_buffer_capacity(buffer);
    try testing.expect(capacity == native_buffer.len);

    const address = env.get_direct_buffer_address(buffer) orelse {
        try testing.expect(false);
        unreachable;
    };

    try testing.expectEqualSlices(u8, &native_buffer, address[0..@as(usize, @intCast(capacity))]);
}

test "JNI: object array" {
    const env: *JNIEnv = get_testing_env();

    const object_class = env.find_class("java/lang/Object");
    try testing.expect(object_class != null);
    defer env.delete_local_ref(object_class);

    const array = env.new_object_array(32, object_class, null);
    try testing.expect(array != null);
    defer env.delete_local_ref(array);

    // ArrayIndexOutOfBoundsException:
    env.set_object_array_element(array, -1, null);
    try testing.expect(env.exception_check() == .jni_true);
    env.exception_clear();

    env.set_object_array_element(array, 32, null);
    try testing.expect(env.exception_check() == .jni_true);
    env.exception_clear();

    // Valid indexes:
    var index: jni.JInt = 0;
    while (index < 32) : (index += 1) {
        const obj_before = env.get_object_array_element(array, index);
        try testing.expect(obj_before == null);

        const obj = env.alloc_object(object_class);
        try testing.expect(obj != null);
        defer env.delete_local_ref(obj);

        env.set_object_array_element(array, index, obj);
        try testing.expect(env.exception_check() == .jni_false);

        const obj_after = env.get_object_array_element(array, index);
        try testing.expect(obj_after != null);
        defer env.delete_local_ref(obj_after);

        try testing.expect(env.is_same_object(obj, obj_after) == .jni_true);
    }
}

test "JNI: primitive arrays" {
    const ArrayTest = struct {
        fn ArrayTestType(comptime PrimitiveType: type) type {
            return struct {
                fn cast(value: anytype) PrimitiveType {
                    return switch (PrimitiveType) {
                        jni.JBoolean => switch (value) {
                            0 => jni.JBoolean.jni_false,
                            else => jni.JBoolean.jni_true,
                        },
                        jni.JFloat, jni.JDouble => @as(PrimitiveType, @floatFromInt(value)),
                        else => @as(PrimitiveType, @intCast(value)),
                    };
                }

                fn get_array_elements(env: *jni.JNIEnv, array: jni.JArray) ?[*]PrimitiveType {
                    return switch (PrimitiveType) {
                        jni.JBoolean => env.get_boolean_array_elements(array, null),
                        jni.JByte => env.get_byte_array_elements(array, null),
                        jni.JShort => env.get_short_array_elements(array, null),
                        jni.JChar => env.get_char_array_elements(array, null),
                        jni.JInt => env.get_int_array_elements(array, null),
                        jni.JLong => env.get_long_array_elements(array, null),
                        jni.JFloat => env.get_float_array_elements(array, null),
                        jni.JDouble => env.get_double_array_elements(array, null),
                        else => unreachable,
                    };
                }

                fn release_array_elements(env: *jni.JNIEnv, array: jni.JArray, elements: [*]PrimitiveType) void {
                    switch (PrimitiveType) {
                        jni.JBoolean => env.release_boolean_array_elements(array, elements, .default),
                        jni.JByte => env.release_byte_array_elements(array, elements, .default),
                        jni.JShort => env.release_short_array_elements(array, elements, .default),
                        jni.JChar => env.release_char_array_elements(array, elements, .default),
                        jni.JInt => env.release_int_array_elements(array, elements, .default),
                        jni.JLong => env.release_long_array_elements(array, elements, .default),
                        jni.JFloat => env.release_float_array_elements(array, elements, .default),
                        jni.JDouble => env.release_double_array_elements(array, elements, .default),
                        else => unreachable,
                    }
                }

                fn get_array_region(env: *jni.JNIEnv, array: jni.JArray, start: jni.JSize, len: jni.JSize, buf: [*]PrimitiveType) void {
                    switch (PrimitiveType) {
                        jni.JBoolean => env.get_boolean_array_region(array, start, len, buf),
                        jni.JByte => env.get_byte_array_region(array, start, len, buf),
                        jni.JShort => env.get_short_array_region(array, start, len, buf),
                        jni.JChar => env.get_char_array_region(array, start, len, buf),
                        jni.JInt => env.get_int_array_region(array, start, len, buf),
                        jni.JLong => env.get_long_array_region(array, start, len, buf),
                        jni.JFloat => env.get_float_array_region(array, start, len, buf),
                        jni.JDouble => env.get_double_array_region(array, start, len, buf),
                        else => unreachable,
                    }
                }

                fn set_array_region(env: *jni.JNIEnv, array: jni.JArray, start: jni.JSize, len: jni.JSize, buf: [*]PrimitiveType) void {
                    switch (PrimitiveType) {
                        jni.JBoolean => env.set_boolean_array_region(array, start, len, buf),
                        jni.JByte => env.set_byte_array_region(array, start, len, buf),
                        jni.JShort => env.set_short_array_region(array, start, len, buf),
                        jni.JChar => env.set_char_array_region(array, start, len, buf),
                        jni.JInt => env.set_int_array_region(array, start, len, buf),
                        jni.JLong => env.set_long_array_region(array, start, len, buf),
                        jni.JFloat => env.set_float_array_region(array, start, len, buf),
                        jni.JDouble => env.set_double_array_region(array, start, len, buf),
                        else => unreachable,
                    }
                }

                pub fn assert(env: *JNIEnv) !void {
                    const lenght = 32;

                    const array = switch (PrimitiveType) {
                        jni.JBoolean => env.new_boolean_array(lenght),
                        jni.JByte => env.new_byte_array(lenght),
                        jni.JChar => env.new_char_array(lenght),
                        jni.JShort => env.new_short_array(lenght),
                        jni.JInt => env.new_int_array(lenght),
                        jni.JLong => env.new_long_array(lenght),
                        jni.JFloat => env.new_float_array(lenght),
                        jni.JDouble => env.new_double_array(lenght),
                        else => unreachable,
                    };

                    try testing.expect(array != null);
                    defer env.delete_local_ref(array);

                    const len = env.get_array_length(array);
                    try testing.expect(len == lenght);

                    // Change the array:
                    {
                        const elements = get_array_elements(env, array) orelse {
                            try testing.expect(false);
                            unreachable;
                        };
                        defer release_array_elements(env, array, elements);

                        for (elements[0..@as(usize, @intCast(len))], 0..) |*element, i| {
                            try testing.expectEqual(cast(0), element.*);
                            element.* = cast(i);
                        }
                    }

                    // Check changes:
                    {
                        const elements = get_array_elements(env, array) orelse {
                            try testing.expect(false);
                            unreachable;
                        };
                        defer release_array_elements(env, array, elements);

                        for (elements[0..@as(usize, @intCast(len))], 0..) |element, i| {
                            try testing.expectEqual(cast(i), element);
                        }
                    }

                    // ArrayRegion:
                    {
                        var buffer: [10]PrimitiveType = undefined;

                        // ArrayIndexOutOfBoundsException:
                        get_array_region(env, array, -1, 10, &buffer);
                        try testing.expect(env.exception_check() == .jni_true);
                        env.exception_clear();

                        get_array_region(env, array, 0, 200, &buffer);
                        try testing.expect(env.exception_check() == .jni_true);
                        env.exception_clear();

                        // Correct bounds:
                        get_array_region(env, array, 5, 10, &buffer);
                        try testing.expect(env.exception_check() == .jni_false);

                        for (&buffer, 0..) |*element, i| {
                            try testing.expectEqual(cast(i + 5), element.*);
                            element.* = cast(i);
                        }

                        // ArrayIndexOutOfBoundsException:
                        set_array_region(env, array, -1, 10, &buffer);
                        try testing.expect(env.exception_check() == .jni_true);
                        env.exception_clear();

                        set_array_region(env, array, 0, 200, &buffer);
                        try testing.expect(env.exception_check() == .jni_true);
                        env.exception_clear();

                        // Correct bounds:
                        set_array_region(env, array, 5, 10, &buffer);
                    }

                    // Check changes
                    {
                        var buffer: [10]PrimitiveType = undefined;
                        get_array_region(env, array, 5, 10, &buffer);
                        try testing.expect(env.exception_check() == .jni_false);

                        for (buffer, 0..) |element, i| {
                            try testing.expectEqual(cast(i), element);
                        }
                    }

                    // Critical
                    {
                        const critical = env.get_primitive_array_critical(array, null) orelse {
                            try testing.expect(false);
                            unreachable;
                        };
                        defer env.release_primitive_array_critical(array, critical, .default);

                        const elements: [*]PrimitiveType = @ptrCast(@alignCast(critical));
                        for (elements[0..@intCast(len)], 0..) |*element, i| {
                            element.* = cast(i + 10);
                        }
                    }

                    // Check changes
                    {
                        const critical = env.get_primitive_array_critical(array, null) orelse {
                            try testing.expect(false);
                            unreachable;
                        };
                        defer env.release_primitive_array_critical(array, critical, .default);

                        const elements: [*]PrimitiveType = @ptrCast(@alignCast(critical));
                        for (elements[0..@intCast(len)], 0..) |element, i| {
                            try testing.expectEqual(cast(i + 10), element);
                        }
                    }
                }
            };
        }
    }.ArrayTestType;

    const env: *JNIEnv = get_testing_env();

    try ArrayTest(jni.JBoolean).assert(env);
    try ArrayTest(jni.JByte).assert(env);
    try ArrayTest(jni.JChar).assert(env);
    try ArrayTest(jni.JShort).assert(env);
    try ArrayTest(jni.JInt).assert(env);
    try ArrayTest(jni.JLong).assert(env);
    try ArrayTest(jni.JFloat).assert(env);
    try ArrayTest(jni.JDouble).assert(env);
}

const get_testing_env = struct {
    var init = std.once(jvm_create);
    var env: *JNIEnv = undefined;

    fn jvm_create() void {
        var jvm: *jni.JavaVM = undefined;
        var args = jni.JavaVMInitArgs{
            .version = jni.jni_version_10,
            .options_len = 0,
            .options = null,
            .ignore_unrecognized = .jni_true,
        };
        const jni_result = JavaVM.create_java_vm(&jvm, &env, &args);
        std.debug.assert(jni_result == .ok);
    }

    pub fn get_env() *JNIEnv {
        init.call();
        return env;
    }
}.get_env;
