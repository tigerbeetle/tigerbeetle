///! Based on Java Native Interface Specification and Java Invocation API.
///! https://docs.oracle.com/en/java/javase/17/docs/specs/jni.
///!
///! We don't rely on @import("jni.h") to translate the JNI declarations by several factors:
///! - Ability to rewrite it using our naming convention and idiomatic Zig pointers
///!   such as ?*, [*], [*:0], ?[*].
///! - Great stability of the JNI specification makes manual implementation a viable option.
///! - Licensing issues redistributing the jni.h file (or even translating it).
///! - To avoid duplicated function definitions by using comptime generated signatures
///!   when calling the function table.
///! - To mitigate the risk of human error by using explicit vtable indexes instead of a
///!   struct of function pointers sensitive to the fields ordering.
///!
///! Additionally, each function is unit tested against a real JVM to validate if they are
///! calling the correct vtable entry with the expected arguments.
const std = @import("std");

/// JNI calling convention.
pub const JNICALL: std.builtin.CallingConvention = .C;

// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getversion.
pub const jni_version_1_1: JInt = 0x00010001;
pub const jni_version_1_2: JInt = 0x00010002;
pub const jni_version_1_4: JInt = 0x00010004;
pub const jni_version_1_6: JInt = 0x00010006;
pub const jni_version_1_8: JInt = 0x00010008;
pub const jni_version_9: JInt = 0x00090000;
pub const jni_version_10: JInt = 0x000a0000;

/// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#return-codes.
pub const JNIResultType = enum(JInt) {
    /// Success.
    ok = 0,

    /// Unknown error.
    unknown = -1,

    /// Thread detached from the VM.
    thread_detached = -2,

    /// JNI version error.
    bad_version = -3,

    /// Not enough memory.
    out_of_memory = -4,

    /// VM already created.
    vm_already_exists = -5,

    /// Invalid arguments.
    invalid_arguments = -6,

    /// Non exhaustive enum.
    _,
};

// Primitive types:
// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/types.html#primitive-types.

pub const JBoolean = enum(u8) { jni_true = 1, jni_false = 0 };
pub const JByte = i8;
pub const JChar = u16;
pub const JShort = i16;
pub const JInt = i32;
pub const JLong = i64;
pub const JFloat = f32;
pub const JDouble = f64;
pub const JSize = JInt;

// Reference types:
// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/types.html#reference-types.

pub const JObject = ?*opaque {};
pub const JClass = JObject;
pub const JThrowable = JObject;
pub const JString = JObject;
pub const JArray = JObject;
pub const JBooleanArray = JArray;
pub const JByteArray = JArray;
pub const JCharArray = JArray;
pub const JShortArray = JArray;
pub const JIntArray = JArray;
pub const JLongArray = JArray;
pub const JFloatArray = JArray;
pub const JDoubleArray = JArray;
pub const JObjectArray = JArray;
pub const JWeakReference = JObject;

// Method and field IDs:
// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/types.html#field-and-method-ids.

pub const JFieldID = ?*opaque {};
pub const JMethodID = ?*opaque {};

/// This union is used as the element type in argument arrays.
/// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/types.html#the-value-type.
pub const JValue = extern union {
    object: JObject,
    boolean: JBoolean,
    byte: JByte,
    char: JChar,
    short: JShort,
    int: JInt,
    long: JLong,
    float: JFloat,
    double: JDouble,

    pub fn to_jvalue(value: anytype) JValue {
        return switch (@TypeOf(value)) {
            JBoolean => .{ .boolean = value },
            JByte => .{ .byte = value },
            JChar => .{ .char = value },
            JShort => .{ .short = value },
            JInt => .{ .int = value },
            JLong => .{ .long = value },
            JFloat => .{ .float = value },
            JDouble => .{ .double = value },
            JObject => .{ .object = value },
            else => unreachable,
        };
    }
};

/// Mode has no effect if elems is not a copy of the elements in array.
/// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#releaseprimitivetypearrayelements-routines.
pub const JArrayReleaseMode = enum(JInt) {
    /// Copy back the content and free the elems buffer.
    default = 0,

    /// Copy back the content but do not free the elems buffer.
    commit = 1,

    /// Free the buffer without copying back the possible changes.
    abort = 2,
};

/// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getobjectreftype.
pub const JObjectRefType = enum(JInt) {
    invalid = 0,
    local = 1,
    global = 2,
    weak_global = 3,
    _,
};

/// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#registernatives.
pub const JNINativeMethod = extern struct {
    name: [*:0]const u8,
    signature: [*:0]const u8,
    fn_ptr: ?*anyopaque,
};

pub const JNIEnv = opaque {
    /// Each function is accessible at a fixed offset through the JNIEnv argument.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#interface-function-table.
    const FunctionTable = enum(usize) {
        /// Index 4: GetVersion
        get_version = 4,

        /// Index 5: DefineClass
        define_class = 5,

        /// Index 6: FindClass
        find_class = 6,

        /// Index 7: FromReflectedMethod
        from_reflected_method = 7,

        /// Index 8: FromReflectedField
        from_feflected_field = 8,

        /// Index 9: ToReflectedMethod
        to_reflected_method = 9,

        /// Index 10: GetSuperclass
        get_super_class = 10,

        /// Index 11: IsAssignableFrom
        is_assignable_from = 11,

        /// Index 12: ToReflectedField
        to_reflected_field = 12,

        /// Index 13: Throw
        throw = 13,

        /// Index 14: ThrowNew
        throw_new = 14,

        /// Index 15: ExceptionOccurred
        exception_occurred = 15,

        /// Index 16: ExceptionDescribe
        exception_describe = 16,

        /// Index 17: ExceptionClear
        exception_clear = 17,

        /// Index 18: FatalError
        fatal_error = 18,

        /// Index 19: PushLocalFrame
        push_local_frame = 19,

        /// Index 20: PopLocalFrame
        pop_local_frame = 20,

        /// Index 21: NewGlobalRef
        new_global_ref = 21,

        /// Index 22: DeleteGlobalRef
        delete_global_ref = 22,

        /// Index 23: DeleteLocalRef
        delete_local_ref = 23,

        /// Index 24: IsSameObject
        is_same_object = 24,

        /// Index 25: NewLocalRef
        new_local_ref = 25,

        /// Index 26: EnsureLocalCapacity
        ensure_local_capacity = 26,

        /// Index 27: AllocObject
        alloc_object = 27,

        /// Index 28: NewObject
        /// Index 29: NewObjectV
        /// Index 30: NewObjectA
        new_object = 30,

        /// Index 31: GetObjectClass
        get_object_class = 31,

        /// Index 32: IsInstanceOf
        is_instance_of = 32,

        /// Index 33: GetMethodID
        get_method_id = 33,

        /// Index 34: CallObjectMethod (omitted)
        /// Index 35: CallObjectMethodV (omitted)
        /// Index 36: CallObjectMethodA
        call_object_method = 36,

        /// Index 37: CallBooleanMethod (omitted)
        /// Index 38: CallBooleanMethodV (omitted)
        /// Index 39: CallBooleanMethodA
        call_boolean_method = 39,

        /// Index 40: CallByteMethod (omitted)
        /// Index 41: CallByteMethodV (omitted)
        /// Index 42: CallByteMethodA
        call_byte_method = 42,

        /// Index 43: CallCharMethod (omitted)
        /// Index 44: CallCharMethodV (omitted)
        /// Index 45: CallCharMethodA
        call_char_method = 45,

        /// Index 46: CallShortMethod (omitted)
        /// Index 47: CallShortMethodV (omitted)
        /// Index 48: CallShortMethodA
        call_short_method = 48,

        /// Index 49: CallIntMethod (omitted)
        /// Index 50: CallIntMethodV (omitted)
        /// Index 51: CallIntMethodA
        call_int_method = 51,

        /// Index 52: CallLongMethod (omitted)
        /// Index 53: CallLongMethodV (omitted)
        /// Index 54: CallLongMethodA
        call_long_method = 54,

        /// Index 55: CallFloatMethod (omitted)
        /// Index 56: CallFloatMethodV (omitted)
        /// Index 57: CallFloatMethodA
        call_float_method = 57,

        /// Index 58: CallDoubleMethod (omitted)
        /// Index 59: CallDoubleMethodV (omitted)
        /// Index 60: CallDoubleMethodA
        call_double_method = 60,

        /// Index 61: CallVoidMethod (omitted)
        /// Index 62: CallVoidMethodV (omitted)
        /// Index 63: CallVoidMethodA
        call_void_method = 63,

        /// Index 64: CallNonvirtualObjectMethod (omitted)
        /// Index 65: CallNonvirtualObjectMethodV (omitted)
        /// Index 66: CallNonvirtualObjectMethodA (omitted)
        call_nonvirtual_object_method = 66,

        /// Index 67: CallNonvirtualBooleanMethod (omitted)
        /// Index 68: CallNonvirtualBooleanMethodV (omitted)
        /// Index 69: CallNonvirtualBooleanMethodA (omitted)
        call_nonvirtual_boolean_method = 69,

        /// Index 70: CallNonvirtualByteMethod (omitted)
        /// Index 71: CallNonvirtualByteMethodV (omitted)
        /// Index 72: CallNonvirtualByteMethodA (omitted)
        call_nonvirtual_byte_method = 72,

        /// Index 73: CallNonvirtualCharMethod (omitted)
        /// Index 74: CallNonvirtualCharMethodV (omitted)
        /// Index 75: CallNonvirtualCharMethodA (omitted)
        call_nonvirtual_char_method = 75,

        /// Index 76: CallNonvirtualShortMethod (omitted)
        /// Index 77: CallNonvirtualShortMethodV (omitted)
        /// Index 78: CallNonvirtualShortMethodA (omitted)
        call_nonvirtual_short_method = 78,

        /// Index 79: CallNonvirtualIntMethod (omitted)
        /// Index 80: CallNonvirtualIntMethodV (omitted)
        /// Index 81: CallNonvirtualIntMethodA (omitted)
        call_nonvirtual_int_method = 81,

        /// Index 82: CallNonvirtualLongMethod (omitted)
        /// Index 83: CallNonvirtualLongMethodV (omitted)
        /// Index 84: CallNonvirtualLongMethodA (omitted)
        call_nonvirtual_long_method = 84,

        /// Index 85: CallNonvirtualFloatMethod (omitted)
        /// Index 86: CallNonvirtualFloatMethodV (omitted)
        /// Index 87: CallNonvirtualFloatMethodA (omitted)
        call_nonvirtual_float_method = 87,

        /// Index 88: CallNonvirtualDoubleMethod (omitted)
        /// Index 89: CallNonvirtualDoubleMethodV (omitted)
        /// Index 90: CallNonvirtualDoubleMethodA (omitted)
        call_nonvirtual_double_method = 90,

        /// Index 91: CallNonvirtualVoidMethod (omitted)
        /// Index 92: CallNonvirtualVoidMethodV (omitted)
        /// Index 93: CallNonvirtualVoidMethodA (omitted)
        call_nonvirtual_void_method = 93,

        /// Index 94: GetFieldID
        get_field_id = 94,

        /// Index 95: GetObjectField
        get_object_field = 95,

        /// Index 96: GetBooleanField
        get_boolean_field = 96,

        /// Index 97: GetByteField
        get_byte_field = 97,

        /// Index 98: GetCharField
        get_char_field = 98,

        /// Index 99: GetShortField
        get_short_field = 99,

        /// Index 100: GetIntField
        get_int_field = 100,

        /// Index 101: GetLongField
        get_long_field = 101,

        /// Index 102: GetFloatField
        get_float_field = 102,

        /// Index 103: GetDoubleField
        get_double_field = 103,

        /// Index 104: SetObjectField
        set_object_field = 104,

        /// Index 105: SetBooleanField
        set_boolean_field = 105,

        /// Index 106: SetByteField
        set_byte_field = 106,

        /// Index 107: SetCharField
        set_char_field = 107,

        /// Index 108: SetShortField
        set_short_field = 108,

        /// Index 109: SetIntField
        set_int_field = 109,

        /// Index 110: SetLongField
        set_long_field = 110,

        /// Index 111: SetFloatField
        set_float_field = 111,

        /// Index 112: SetDoubleField
        set_double_field = 112,

        /// Index 113: GetStaticMethodID
        get_static_method_id = 113,

        /// Index 114: CallStaticObjectMethod (omitted)
        /// Index 115: CallStaticObjectMethodV (omitted)
        /// Index 116: CallStaticObjectMethodA
        call_static_object_method = 116,

        /// Index 117: CallStaticBooleanMethod (omitted)
        /// Index 118: CallStaticBooleanMethodV (omitted)
        /// Index 119: CallStaticBooleanMethodA
        call_static_boolean_method = 119,

        /// Index 120: CallStaticBooleanMethod (omitted)
        /// Index 121: CallStaticBooleanMethodV (omitted)
        /// Index 122: CallStaticBooleanMethodA
        call_static_byte_method = 122,

        /// Index 123: CallStaticCharMethod (omitted)
        /// Index 124: CallStaticCharMethodV (omitted)
        /// Index 125: CallStaticCharMethodA
        call_static_char_method = 125,

        /// Index 126: CallStaticCharMethod (omitted)
        /// Index 127: CallStaticCharMethodV (omitted)
        /// Index 128: CallStaticCharMethodA
        call_static_short_method = 128,

        /// Index 129: CallStaticIntMethod (omitted)
        /// Index 130: CallStaticIntMethodV (omitted)
        /// Index 131: CallStaticIntMethodA
        call_static_int_method = 131,

        /// Index 132: CallStaticLongMethod (omitted)
        /// Index 133: CallStaticLongMethodV (omitted)
        /// Index 134: CallStaticLongMethodA
        call_static_long_method = 134,

        /// Index 135: CallStaticFloatMethod (omitted)
        /// Index 136: CallStaticFloatMethodV (omitted)
        /// Index 137: CallStaticFloatMethodA
        call_static_float_method = 137,

        /// Index 138: CallStaticDoubleMethod (omitted)
        /// Index 139: CallStaticDoubleMethodV (omitted)
        /// Index 140: CallStaticDoubleMethodA
        call_static_double_method = 140,

        /// Index 141: CallStaticVoidMethod (omitted)
        /// Index 142: CallStaticVoidMethodV (omitted)
        /// Index 143: CallStaticVoidMethodA
        call_static_void_method = 143,

        /// Index 144: GetStaticFieldID
        get_static_field_id = 144,

        /// Index 145: GetStaticObjectField
        get_static_object_field = 145,

        /// Index 146: GetStaticBooleanField
        get_static_boolean_field = 146,

        /// Index 147: GetStaticByteField
        get_static_byte_field = 147,

        /// Index 148: GetStaticCharField
        get_static_char_field = 148,

        /// Index 149: GetStaticShortField
        get_static_short_field = 149,

        /// Index 150: GetStaticIntField
        get_static_int_field = 150,

        /// Index 151: GetStaticLongField
        get_static_long_field = 151,

        /// Index 152: GetStaticFloatField
        /// Returns the value of a static field of an object.
        get_static_float_field = 152,

        /// Index 153: GetStaticDoubleField
        get_static_double_field = 153,

        /// Index 154: SetStaticObjectField
        set_static_object_field = 154,

        /// Index 155: SetStaticBooleanField
        set_static_boolean_field = 155,

        /// Index 156: SetStaticByteField
        /// Sets the value of a static field of an object.
        set_static_byte_field = 156,

        /// Index 157: SetStaticCharField
        set_static_char_field = 157,

        /// Index 158: SetStaticShortField
        set_static_short_field = 158,

        /// Index 159: SetStaticIntField
        set_static_int_field = 159,

        /// Index 160: SetStaticLongField
        set_static_long_field = 160,

        /// Index 161: SetStaticFloatField
        set_static_float_field = 161,

        /// Index 162: SetStaticDoubleField
        set_static_double_field = 162,

        /// Index 163: NewString
        new_string = 163,

        /// Index 164: GetStringLength
        get_string_length = 164,

        /// Index 165: GetStringChars
        get_string_chars = 165,

        /// Index 166: ReleaseStringChars
        release_string_chars = 166,

        /// Index 167: NewStringUTF
        new_string_utf = 167,

        /// Index 168: GetStringUTFLength
        get_string_utf_length = 168,

        /// Index 169: GetStringUTFChars
        get_string_utf_chars = 169,

        /// Index 170: ReleaseStringUTFChars
        release_string_utf_chars = 170,

        /// Index 171: GetArrayLength
        get_array_length = 171,

        /// Index 172: NewObjectArray
        new_object_array = 172,

        /// Index 173: GetObjectArrayElement
        get_object_array_element = 173,

        /// Index 174: SetObjectArrayElement
        set_object_array_element = 174,

        /// Index 175: NewBooleanArray
        new_boolean_array = 175,

        /// Index 176: NewByteArray
        new_byte_array = 176,

        /// Index 177: NewCharArray
        new_char_array = 177,

        /// Index 178: NewShortArray
        new_short_array = 178,

        /// Index 179: NewIntArray
        new_int_array = 179,

        /// Index 180: NewLongArray
        new_long_array = 180,

        /// Index 181: NewFloatArray
        new_float_array = 181,

        /// Index 182: NewDoubleArray
        new_double_array = 182,

        /// Index 183: GetBooleanArrayElements
        get_boolean_array_elements = 183,

        /// Index 184: GetByteArrayElements
        get_byte_array_elements = 184,

        /// Index 185: GetCharArrayElements
        get_char_array_elements = 185,

        /// Index 186: GetShortArrayElements
        get_short_array_elements = 186,

        /// Index 187: GetIntArrayElements
        get_int_array_elements = 187,

        /// Index 188: GetLongArrayElements
        get_long_array_elements = 188,

        /// Index 189: GetFloatArrayElements
        get_float_array_elements = 189,

        /// Index 190: GetDoubleArrayElements
        get_double_array_elements = 190,

        /// Index 191: ReleaseBooleanArrayElements
        release_boolean_array_elements = 191,

        /// Index 192: ReleaseByteArrayElements
        release_byte_array_elements = 192,

        /// Index 193: ReleaseCharArrayElements
        release_char_array_elements = 193,

        /// Index 194: ReleaseShortArrayElements
        release_short_array_elements = 194,

        /// Index 195: ReleaseIntArrayElements
        release_int_array_elements = 195,

        /// Index 196: ReleaseLongArrayElements
        release_long_array_elements = 196,

        /// Index 197: ReleaseFloatArrayElements
        release_float_array_elements = 197,

        /// Index 198: ReleaseDoubleArrayElements
        release_double_array_elements = 198,

        /// Index 199: GetBooleanArrayRegion
        get_boolean_array_region = 199,

        /// Index 200: GetByteArrayRegion
        get_byte_array_region = 200,

        /// Index 201: GetCharArrayRegion
        get_char_array_region = 201,

        /// Index 202: GetShortArrayRegion
        get_short_array_region = 202,

        /// Index 203: GetIntArrayRegion
        get_int_array_region = 203,

        /// Index 204: GetLongArrayRegion
        get_long_array_region = 204,

        /// Index 205: GetFloatArrayRegion
        get_float_array_region = 205,

        /// Index 206: GetDoubleArrayRegion
        get_double_array_region = 206,

        /// Index 207: SetBooleanArrayRegion
        set_boolean_array_region = 207,

        /// Index 208: SetByteArrayRegion
        set_byte_array_region = 208,

        /// Index 209: SetCharArrayRegion
        set_char_array_region = 209,

        /// Index 210: SetShortArrayRegion
        set_short_array_region = 210,

        /// Index 211: SetIntArrayRegion
        set_int_array_region = 211,

        /// Index 212: SetLongArrayRegion
        set_long_array_region = 212,

        /// Index 213: SetFloatArrayRegion
        set_float_array_region = 213,

        /// Index 214: SetDoubleArrayRegion
        set_double_array_region = 214,

        /// Index 215: RegisterNatives
        register_natives = 215,

        /// Index 216: UnregisterNatives
        unregister_natives = 216,

        /// Index 217: MonitorEnter
        monitor_enter = 217,

        /// Index 218: MonitorExit
        monitor_exit = 218,

        /// GetJavaVM - Index 219 in the JNIEnv interface function table
        get_java_vm = 219,

        /// Index 220: GetStringRegion
        get_string_region = 220,

        /// Index 221: GetStringUTFRegion
        get_string_utf_region = 221,

        /// Index 222: GetPrimitiveArrayCritical
        get_primitive_array_critical = 222,

        /// Index 223: ReleasePrimitiveArrayCritical
        release_primitive_array_critical = 223,

        /// Index 224: GetStringCritical
        get_string_critical = 224,

        /// Index 225: ReleaseStringCritical
        release_string_critical = 225,

        /// Index 226: NewWeakGlobalRef
        new_weak_global_ref = 226,

        /// Index 227: DeleteWeakGlobalRef
        delete_weak_global_ref = 227,

        /// Index 228: ExceptionCheck
        exception_check = 228,

        /// Index 229: NewDirectByteBuffer
        new_direct_byte_buffer = 229,

        /// Index 230: GetDirectBufferAddress
        get_direct_buffer_address = 230,

        /// GetDirectBufferCapacity - Index 231 in the JNIEnv interface function tabl
        get_direct_buffer_capacity = 231,

        /// Index 232: GetObjectRefType
        get_object_ref_type = 232,

        /// Index 233: GetModule
        get_module = 233,
    };

    usingnamespace JniInterface(JNIEnv);

    /// Returns the major version number in the higher 16 bits
    /// and the minor version number in the lower 16 bits.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getversion.
    pub inline fn get_version(
        env: *JNIEnv,
    ) JInt {
        return JNIEnv.interface_call(
            env,
            .get_version,
            .{},
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#defineclass.
    pub inline fn define_class(
        env: *JNIEnv,
        name: [*:0]const u8,
        loader: JObject,
        buf: [*]const JByte,
        buf_len: JSize,
    ) JClass {
        return JNIEnv.interface_call(
            env,
            .define_class,
            .{ name, loader, buf, buf_len },
        );
    }

    /// The name argument is a fully-qualified class name or an array type signature.
    /// For example "java/lang/String"
    /// Returns a class object from a fully-qualified name, or NULL if the class cannot be found.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#findclass.
    pub inline fn find_class(
        env: *JNIEnv,
        name: [*:0]const u8,
    ) JClass {
        return JNIEnv.interface_call(
            env,
            .find_class,
            .{name},
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#fromreflectedmethod
    pub inline fn from_reflected_method(
        env: *JNIEnv,
        jobject: JObject,
    ) JMethodID {
        return JNIEnv.interface_call(
            env,
            .from_reflected_method,
            .{jobject},
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#fromfeflectedfield
    pub inline fn from_feflected_field(
        env: *JNIEnv,
        jobject: JObject,
    ) JFieldID {
        return JNIEnv.interface_call(
            env,
            .from_feflected_field,
            .{jobject},
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#toreflectedmethod
    pub inline fn to_reflected_method(
        env: *JNIEnv,
        cls: JClass,
        method_id: JMethodID,
        is_static: JBoolean,
    ) JObject {
        return JNIEnv.interface_call(
            env,
            .to_reflected_method,
            .{ cls, method_id, is_static },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getsuperclass.
    pub inline fn get_super_class(
        env: *JNIEnv,
        clazz: JClass,
    ) JClass {
        return JNIEnv.interface_call(
            env,
            .get_super_class,
            .{clazz},
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#isassignablefrom.
    pub inline fn is_assignable_from(
        env: *JNIEnv,
        clazz_1: JClass,
        clazz_2: JClass,
    ) JBoolean {
        return JNIEnv.interface_call(
            env,
            .is_assignable_from,
            .{ clazz_1, clazz_2 },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#toreflectedfield.
    pub inline fn to_reflected_field(
        env: *JNIEnv,
        cls: JClass,
        field_id: JFieldID,
        is_static: JBoolean,
    ) JObject {
        return JNIEnv.interface_call(
            env,
            .to_reflected_field,
            .{ cls, field_id, is_static },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#throw.
    pub inline fn throw(
        env: *JNIEnv,
        obj: JThrowable,
    ) JNIResultType {
        return JNIEnv.interface_call(
            env,
            .throw,
            .{obj},
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#thrownew.
    pub inline fn throw_new(
        env: *JNIEnv,
        clazz: JClass,
        message: [*:0]const u8,
    ) JNIResultType {
        return JNIEnv.interface_call(
            env,
            .throw_new,
            .{ clazz, message },
        );
    }

    /// Returns the exception object that is currently in the process of being thrown,
    /// or NULL if no exception is currently being thrown.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#exceptionoccurred.
    pub inline fn exception_occurred(
        env: *JNIEnv,
    ) JThrowable {
        return JNIEnv.interface_call(
            env,
            .exception_occurred,
            .{},
        );
    }

    /// Prints an exception and a backtrace of the stack to a system error-reporting channel,
    /// such as stderr. This is a convenience routine provided for debugging.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#exceptiondescribe.
    pub inline fn exception_describe(
        env: *JNIEnv,
    ) void {
        JNIEnv.interface_call(
            env,
            .exception_describe,
            .{},
        );
    }

    /// Clears any exception that is currently being thrown.
    /// If no exception is currently being thrown, this routine has no effect.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#exceptionclear.
    pub inline fn exception_clear(
        env: *JNIEnv,
    ) void {
        JNIEnv.interface_call(
            env,
            .exception_clear,
            .{},
        );
    }

    /// Raises a fatal error and does not expect the VM to recover.
    /// This function does not return.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#fatalerror.
    pub inline fn fatal_error(
        env: *JNIEnv,
        msg: [*:0]const u8,
    ) noreturn {
        JNIEnv.interface_call(
            env,
            .fatal_error,
            .{msg},
        );
        unreachable;
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#pushlocalframe.
    pub inline fn push_local_frame(
        env: *JNIEnv,
        capacity: JInt,
    ) JNIResultType {
        return JNIEnv.interface_call(
            env,
            .push_local_frame,
            .{capacity},
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#poplocalframe.
    pub inline fn pop_local_frame(
        env: *JNIEnv,
        result: JObject,
    ) JObject {
        return JNIEnv.interface_call(
            env,
            .pop_local_frame,
            .{result},
        );
    }

    /// Returns a global reference to the given obj.
    /// May return NULL if:
    /// - obj refers to null;
    /// - the system has run out of memory;
    /// - obj was a weak global reference and has already been garbage collected.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#newglobalref.
    pub inline fn new_global_ref(
        env: *JNIEnv,
        obj: JObject,
    ) JObject {
        return JNIEnv.interface_call(
            env,
            .new_global_ref,
            .{obj},
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#deleteglobalref.
    pub inline fn delete_global_ref(
        env: *JNIEnv,
        global_ref: JObject,
    ) void {
        JNIEnv.interface_call(
            env,
            .delete_global_ref,
            .{global_ref},
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#deletelocalref.
    pub inline fn delete_local_ref(
        env: *JNIEnv,
        local_ref: JObject,
    ) void {
        JNIEnv.interface_call(
            env,
            .delete_local_ref,
            .{local_ref},
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#issameobject.
    pub inline fn is_same_object(
        env: *JNIEnv,
        ref_1: JObject,
        ref_2: JObject,
    ) JBoolean {
        return JNIEnv.interface_call(
            env,
            .is_same_object,
            .{ ref_1, ref_2 },
        );
    }

    /// Returns NULL if ref refers to null.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#newlocalref.
    pub inline fn new_local_ref(
        env: *JNIEnv,
        ref: JObject,
    ) JObject {
        return JNIEnv.interface_call(
            env,
            .new_local_ref,
            .{ref},
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#ensurelocalcapacity.
    pub inline fn ensure_local_capacity(
        env: *JNIEnv,
        capacity: JInt,
    ) JNIResultType {
        return JNIEnv.interface_call(
            env,
            .ensure_local_capacity,
            .{capacity},
        );
    }

    /// Allocates a new Java object *without* invoking the constructor.
    /// Returns a reference to the object.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#allocobject.
    pub inline fn alloc_object(
        env: *JNIEnv,
        clazz: JClass,
    ) JObject {
        return JNIEnv.interface_call(
            env,
            .alloc_object,
            .{clazz},
        );
    }

    /// Returns a Java object, or NULL if the object cannot be constructed.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#newobject.
    pub inline fn new_object(
        env: *JNIEnv,
        clazz: JClass,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) JObject {
        return JNIEnv.interface_call(
            env,
            .new_object,
            .{ clazz, method_id, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getobjectclass.
    pub inline fn get_object_class(
        env: *JNIEnv,
        jobject: JObject,
    ) JClass {
        return JNIEnv.interface_call(
            env,
            .get_object_class,
            .{jobject},
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#isinstanceof.
    pub inline fn is_instance_of(
        env: *JNIEnv,
        jobject: JObject,
        clazz: JClass,
    ) JBoolean {
        return JNIEnv.interface_call(
            env,
            .is_instance_of,
            .{ jobject, clazz },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getmethodid.
    pub inline fn get_method_id(
        env: *JNIEnv,
        clazz: JClass,
        name: [*:0]const u8,
        sig: [*:0]const u8,
    ) JMethodID {
        return JNIEnv.interface_call(
            env,
            .get_method_id,
            .{ clazz, name, sig },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#calltypemethod-routines-calltypemethoda-routines-calltypemethodv-routines.
    pub inline fn call_object_method(
        env: *JNIEnv,
        obj: JObject,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) JObject {
        return JNIEnv.interface_call(
            env,
            .call_object_method,
            .{ obj, method_id, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#calltypemethod-routines-calltypemethoda-routines-calltypemethodv-routines.
    pub inline fn call_boolean_method(
        env: *JNIEnv,
        obj: JObject,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) JBoolean {
        return JNIEnv.interface_call(
            env,
            .call_boolean_method,
            .{ obj, method_id, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#calltypemethod-routines-calltypemethoda-routines-calltypemethodv-routines.
    pub inline fn call_byte_method(
        env: *JNIEnv,
        obj: JObject,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) JByte {
        return JNIEnv.interface_call(
            env,
            .call_byte_method,
            .{ obj, method_id, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#calltypemethod-routines-calltypemethoda-routines-calltypemethodv-routines.
    pub inline fn call_char_method(
        env: *JNIEnv,
        obj: JObject,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) JChar {
        return JNIEnv.interface_call(
            env,
            .call_char_method,
            .{ obj, method_id, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#calltypemethod-routines-calltypemethoda-routines-calltypemethodv-routines.
    pub inline fn call_short_method(
        env: *JNIEnv,
        obj: JObject,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) JShort {
        return JNIEnv.interface_call(
            env,
            .call_short_method,
            .{ obj, method_id, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#calltypemethod-routines-calltypemethoda-routines-calltypemethodv-routines.
    pub inline fn call_int_method(
        env: *JNIEnv,
        obj: JObject,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) JInt {
        return JNIEnv.interface_call(
            env,
            .call_int_method,
            .{ obj, method_id, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#calltypemethod-routines-calltypemethoda-routines-calltypemethodv-routines.
    pub inline fn call_long_method(
        env: *JNIEnv,
        obj: JObject,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) JLong {
        return JNIEnv.interface_call(
            env,
            .call_long_method,
            .{ obj, method_id, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#calltypemethod-routines-calltypemethoda-routines-calltypemethodv-routines.
    pub inline fn call_float_method(
        env: *JNIEnv,
        obj: JObject,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) JFloat {
        return JNIEnv.interface_call(
            env,
            .call_float_method,
            .{ obj, method_id, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#calltypemethod-routines-calltypemethoda-routines-calltypemethodv-routines.
    pub inline fn call_double_method(
        env: *JNIEnv,
        obj: JObject,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) JDouble {
        return JNIEnv.interface_call(
            env,
            .call_double_method,
            .{ obj, method_id, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#calltypemethod-routines-calltypemethoda-routines-calltypemethodv-routines.
    pub inline fn call_void_method(
        env: *JNIEnv,
        obj: JObject,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) void {
        JNIEnv.interface_call(
            env,
            .call_void_method,
            .{ obj, method_id, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#callnonvirtualtypemethod-routines-callnonvirtualtypemethoda-routines-callnonvirtualtypemethodv-routines.
    pub inline fn call_nonvirtual_object_method(
        env: *JNIEnv,
        obj: JObject,
        clazz: JClass,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) JObject {
        return JNIEnv.interface_call(
            env,
            .call_nonvirtual_object_method,
            .{ obj, clazz, method_id, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#callnonvirtualtypemethod-routines-callnonvirtualtypemethoda-routines-callnonvirtualtypemethodv-routines.
    pub inline fn call_nonvirtual_boolean_method(
        env: *JNIEnv,
        obj: JObject,
        clazz: JClass,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) JBoolean {
        return JNIEnv.interface_call(
            env,
            .call_nonvirtual_boolean_method,
            .{ obj, clazz, method_id, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#callnonvirtualtypemethod-routines-callnonvirtualtypemethoda-routines-callnonvirtualtypemethodv-routines.
    pub inline fn call_nonvirtual_byte_method(
        env: *JNIEnv,
        obj: JObject,
        clazz: JClass,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) JByte {
        return JNIEnv.interface_call(
            env,
            .call_nonvirtual_byte_method,
            .{ obj, clazz, method_id, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#callnonvirtualtypemethod-routines-callnonvirtualtypemethoda-routines-callnonvirtualtypemethodv-routines.
    pub inline fn call_nonvirtual_char_method(
        env: *JNIEnv,
        obj: JObject,
        clazz: JClass,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) JChar {
        return JNIEnv.interface_call(
            env,
            .call_nonvirtual_char_method,
            .{ obj, clazz, method_id, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#callnonvirtualtypemethod-routines-callnonvirtualtypemethoda-routines-callnonvirtualtypemethodv-routines.
    pub inline fn call_nonvirtual_short_method(
        env: *JNIEnv,
        obj: JObject,
        clazz: JClass,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) JShort {
        return JNIEnv.interface_call(
            env,
            .call_nonvirtual_short_method,
            .{ obj, clazz, method_id, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#callnonvirtualtypemethod-routines-callnonvirtualtypemethoda-routines-callnonvirtualtypemethodv-routines.
    pub inline fn call_nonvirtual_int_method(
        env: *JNIEnv,
        obj: JObject,
        clazz: JClass,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) JInt {
        return JNIEnv.interface_call(
            env,
            .call_nonvirtual_int_method,
            .{ obj, clazz, method_id, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#callnonvirtualtypemethod-routines-callnonvirtualtypemethoda-routines-callnonvirtualtypemethodv-routines.
    pub inline fn call_nonvirtual_long_method(
        env: *JNIEnv,
        obj: JObject,
        clazz: JClass,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) JLong {
        return JNIEnv.interface_call(
            env,
            .call_nonvirtual_long_method,
            .{ obj, clazz, method_id, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#callnonvirtualtypemethod-routines-callnonvirtualtypemethoda-routines-callnonvirtualtypemethodv-routines.
    pub inline fn call_nonvirtual_float_method(
        env: *JNIEnv,
        obj: JObject,
        clazz: JClass,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) JFloat {
        return JNIEnv.interface_call(
            env,
            .call_nonvirtual_float_method,
            .{ obj, clazz, method_id, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#callnonvirtualtypemethod-routines-callnonvirtualtypemethoda-routines-callnonvirtualtypemethodv-routines.
    pub inline fn call_nonvirtual_double_method(
        env: *JNIEnv,
        obj: JObject,
        clazz: JClass,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) JDouble {
        return JNIEnv.interface_call(
            env,
            .call_nonvirtual_double_method,
            .{ obj, clazz, method_id, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#callnonvirtualtypemethod-routines-callnonvirtualtypemethoda-routines-callnonvirtualtypemethodv-routines.
    pub inline fn call_nonvirtual_void_method(
        env: *JNIEnv,
        obj: JObject,
        clazz: JClass,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) void {
        JNIEnv.interface_call(
            env,
            .call_nonvirtual_void_method,
            .{ obj, clazz, method_id, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getfieldid.
    pub inline fn get_field_id(
        env: *JNIEnv,
        clazz: JClass,
        name: [*:0]const u8,
        sig: [*:0]const u8,
    ) JFieldID {
        return JNIEnv.interface_call(
            env,
            .get_field_id,
            .{ clazz, name, sig },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#gettypefield-routines.
    pub inline fn get_object_field(
        env: *JNIEnv,
        obj: JObject,
        field_id: JFieldID,
    ) JObject {
        return JNIEnv.interface_call(
            env,
            .get_object_field,
            .{ obj, field_id },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#gettypefield-routines.
    pub inline fn get_boolean_field(
        env: *JNIEnv,
        obj: JObject,
        field_id: JFieldID,
    ) JBoolean {
        return JNIEnv.interface_call(
            env,
            .get_boolean_field,
            .{ obj, field_id },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#gettypefield-routines.
    pub inline fn get_byte_field(
        env: *JNIEnv,
        obj: JObject,
        field_id: JFieldID,
    ) JByte {
        return JNIEnv.interface_call(
            env,
            .get_byte_field,
            .{ obj, field_id },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#gettypefield-routines.
    pub inline fn get_char_field(
        env: *JNIEnv,
        obj: JObject,
        field_id: JFieldID,
    ) JChar {
        return JNIEnv.interface_call(
            env,
            .get_char_field,
            .{ obj, field_id },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#gettypefield-routines.
    pub inline fn get_short_field(
        env: *JNIEnv,
        obj: JObject,
        field_id: JFieldID,
    ) JShort {
        return JNIEnv.interface_call(
            env,
            .get_short_field,
            .{ obj, field_id },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#gettypefield-routines.
    pub inline fn get_int_field(
        env: *JNIEnv,
        obj: JObject,
        field_id: JFieldID,
    ) JInt {
        return JNIEnv.interface_call(
            env,
            .get_int_field,
            .{ obj, field_id },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#gettypefield-routines.
    pub inline fn get_long_field(
        env: *JNIEnv,
        obj: JObject,
        field_id: JFieldID,
    ) JLong {
        return JNIEnv.interface_call(
            env,
            .get_long_field,
            .{ obj, field_id },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#gettypefield-routines.
    pub inline fn get_float_field(
        env: *JNIEnv,
        obj: JObject,
        field_id: JFieldID,
    ) JFloat {
        return JNIEnv.interface_call(
            env,
            .get_float_field,
            .{ obj, field_id },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#gettypefield-routines.
    pub inline fn get_double_field(
        env: *JNIEnv,
        obj: JObject,
        field_id: JFieldID,
    ) JDouble {
        return JNIEnv.interface_call(
            env,
            .get_double_field,
            .{ obj, field_id },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#settypefield-routines.
    pub inline fn set_object_field(
        env: *JNIEnv,
        obj: JObject,
        field_id: JFieldID,
        value: JObject,
    ) void {
        JNIEnv.interface_call(
            env,
            .set_object_field,
            .{ obj, field_id, value },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#settypefield-routines.
    pub inline fn set_boolean_field(
        env: *JNIEnv,
        obj: JObject,
        field_id: JFieldID,
        value: JBoolean,
    ) void {
        JNIEnv.interface_call(
            env,
            .set_boolean_field,
            .{ obj, field_id, value },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#settypefield-routines.
    pub inline fn set_byte_field(
        env: *JNIEnv,
        obj: JObject,
        field_id: JFieldID,
        value: JByte,
    ) void {
        JNIEnv.interface_call(
            env,
            .set_byte_field,
            .{ obj, field_id, value },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#settypefield-routines.
    pub inline fn set_char_field(
        env: *JNIEnv,
        obj: JObject,
        field_id: JFieldID,
        value: JChar,
    ) void {
        JNIEnv.interface_call(
            env,
            .set_char_field,
            .{ obj, field_id, value },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#settypefield-routines.
    pub inline fn set_short_field(
        env: *JNIEnv,
        obj: JObject,
        field_id: JFieldID,
        value: JShort,
    ) void {
        JNIEnv.interface_call(
            env,
            .set_short_field,
            .{ obj, field_id, value },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#settypefield-routines.
    pub inline fn set_int_field(
        env: *JNIEnv,
        obj: JObject,
        field_id: JFieldID,
        value: JInt,
    ) void {
        JNIEnv.interface_call(
            env,
            .set_int_field,
            .{ obj, field_id, value },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#settypefield-routines.
    pub inline fn set_long_field(
        env: *JNIEnv,
        obj: JObject,
        field_id: JFieldID,
        value: JLong,
    ) void {
        JNIEnv.interface_call(
            env,
            .set_long_field,
            .{ obj, field_id, value },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#settypefield-routines.
    pub inline fn set_float_field(
        env: *JNIEnv,
        obj: JObject,
        field_id: JFieldID,
        value: JFloat,
    ) void {
        JNIEnv.interface_call(
            env,
            .set_float_field,
            .{ obj, field_id, value },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#settypefield-routines.
    pub inline fn set_double_field(
        env: *JNIEnv,
        obj: JObject,
        field_id: JFieldID,
        value: JDouble,
    ) void {
        JNIEnv.interface_call(
            env,
            .set_double_field,
            .{ obj, field_id, value },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getstaticmethodid.
    pub inline fn get_static_method_id(
        env: *JNIEnv,
        clazz: JClass,
        name: [*:0]const u8,
        sig: [*:0]const u8,
    ) JMethodID {
        return JNIEnv.interface_call(
            env,
            .get_static_method_id,
            .{ clazz, name, sig },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#callstatictypemethod-routines-callstatictypemethoda-routines-callstatictypemethodv-routines.
    pub inline fn call_static_object_method(
        env: *JNIEnv,
        clazz: JClass,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) JObject {
        return JNIEnv.interface_call(
            env,
            .call_static_object_method,
            .{ clazz, method_id, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#callstatictypemethod-routines-callstatictypemethoda-routines-callstatictypemethodv-routines.
    pub inline fn call_static_boolean_method(
        env: *JNIEnv,
        clazz: JClass,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) JBoolean {
        return JNIEnv.interface_call(
            env,
            .call_static_boolean_method,
            .{ clazz, method_id, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#callstatictypemethod-routines-callstatictypemethoda-routines-callstatictypemethodv-routines.
    pub inline fn call_static_byte_method(
        env: *JNIEnv,
        clazz: JClass,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) JByte {
        return JNIEnv.interface_call(
            env,
            .call_static_byte_method,
            .{ clazz, method_id, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#callstatictypemethod-routines-callstatictypemethoda-routines-callstatictypemethodv-routines.
    pub inline fn call_static_char_method(
        env: *JNIEnv,
        clazz: JClass,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) JChar {
        return JNIEnv.interface_call(
            env,
            .call_static_char_method,
            .{ clazz, method_id, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#callstatictypemethod-routines-callstatictypemethoda-routines-callstatictypemethodv-routines.
    pub inline fn call_static_short_method(
        env: *JNIEnv,
        clazz: JClass,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) JShort {
        return JNIEnv.interface_call(
            env,
            .call_static_short_method,
            .{ clazz, method_id, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#callstatictypemethod-routines-callstatictypemethoda-routines-callstatictypemethodv-routines.
    pub inline fn call_static_int_method(
        env: *JNIEnv,
        clazz: JClass,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) JInt {
        return JNIEnv.interface_call(
            env,
            .call_static_int_method,
            .{ clazz, method_id, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#callstatictypemethod-routines-callstatictypemethoda-routines-callstatictypemethodv-routines.
    pub inline fn call_static_long_method(
        env: *JNIEnv,
        clazz: JClass,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) JLong {
        return JNIEnv.interface_call(
            env,
            .call_static_long_method,
            .{ clazz, method_id, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#callstatictypemethod-routines-callstatictypemethoda-routines-callstatictypemethodv-routines.
    pub inline fn call_static_float_method(
        env: *JNIEnv,
        clazz: JClass,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) JFloat {
        return JNIEnv.interface_call(
            env,
            .call_static_float_method,
            .{ clazz, method_id, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#callstatictypemethod-routines-callstatictypemethoda-routines-callstatictypemethodv-routines.
    pub inline fn call_static_double_method(
        env: *JNIEnv,
        clazz: JClass,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) JDouble {
        return JNIEnv.interface_call(
            env,
            .call_static_double_method,
            .{ clazz, method_id, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#callstatictypemethod-routines-callstatictypemethoda-routines-callstatictypemethodv-routines.
    pub inline fn call_static_void_method(
        env: *JNIEnv,
        clazz: JClass,
        method_id: JMethodID,
        args: ?[*]const JValue,
    ) void {
        JNIEnv.interface_call(
            env,
            .call_static_void_method,
            .{ clazz, method_id, args },
        );
    }

    /// Returns a field ID, or NULL if the specified static field cannot be found.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getstaticfieldid.
    pub inline fn get_static_field_id(
        env: *JNIEnv,
        clazz: JClass,
        name: [*:0]const u8,
        sig: [*:0]const u8,
    ) JFieldID {
        return JNIEnv.interface_call(
            env,
            .get_static_field_id,
            .{ clazz, name, sig },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getstatictypefield-routines.
    pub inline fn get_static_object_field(
        env: *JNIEnv,
        clazz: JClass,
        field_id: JFieldID,
    ) JObject {
        return JNIEnv.interface_call(
            env,
            .get_static_object_field,
            .{ clazz, field_id },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getstatictypefield-routines.
    pub inline fn get_static_boolean_field(
        env: *JNIEnv,
        clazz: JClass,
        field_id: JFieldID,
    ) JBoolean {
        return JNIEnv.interface_call(
            env,
            .get_static_boolean_field,
            .{ clazz, field_id },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getstatictypefield-routines.
    pub inline fn get_static_byte_field(
        env: *JNIEnv,
        clazz: JClass,
        field_id: JFieldID,
    ) JByte {
        return JNIEnv.interface_call(
            env,
            .get_static_byte_field,
            .{ clazz, field_id },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getstatictypefield-routines.
    pub inline fn get_static_char_field(
        env: *JNIEnv,
        clazz: JClass,
        field_id: JFieldID,
    ) JChar {
        return JNIEnv.interface_call(
            env,
            .get_static_char_field,
            .{ clazz, field_id },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getstatictypefield-routines.
    pub inline fn get_static_short_field(
        env: *JNIEnv,
        clazz: JClass,
        field_id: JFieldID,
    ) JShort {
        return JNIEnv.interface_call(
            env,
            .get_static_short_field,
            .{ clazz, field_id },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getstatictypefield-routines.
    pub inline fn get_static_int_field(
        env: *JNIEnv,
        clazz: JClass,
        field_id: JFieldID,
    ) JInt {
        return JNIEnv.interface_call(
            env,
            .get_static_int_field,
            .{ clazz, field_id },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getstatictypefield-routines.
    pub inline fn get_static_long_field(
        env: *JNIEnv,
        clazz: JClass,
        field_id: JFieldID,
    ) JLong {
        return JNIEnv.interface_call(
            env,
            .get_static_long_field,
            .{ clazz, field_id },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getstatictypefield-routines.
    pub inline fn get_static_float_field(
        env: *JNIEnv,
        clazz: JClass,
        field_id: JFieldID,
    ) JFloat {
        return JNIEnv.interface_call(
            env,
            .get_static_float_field,
            .{ clazz, field_id },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getstatictypefield-routines.
    pub inline fn get_static_double_field(
        env: *JNIEnv,
        clazz: JClass,
        field_id: JFieldID,
    ) JDouble {
        return JNIEnv.interface_call(
            env,
            .get_static_double_field,
            .{ clazz, field_id },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getstatictypefield-routines.
    pub inline fn set_static_object_field(
        env: *JNIEnv,
        clazz: JClass,
        field_id: JFieldID,
        value: JObject,
    ) void {
        JNIEnv.interface_call(
            env,
            .set_static_object_field,
            .{ clazz, field_id, value },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#setstatictypefield-routines.
    pub inline fn set_static_boolean_field(
        env: *JNIEnv,
        clazz: JClass,
        field_id: JFieldID,
        value: JBoolean,
    ) void {
        JNIEnv.interface_call(
            env,
            .set_static_boolean_field,
            .{ clazz, field_id, value },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#setstatictypefield-routines.
    pub inline fn set_static_byte_field(
        env: *JNIEnv,
        clazz: JClass,
        field_id: JFieldID,
        value: JByte,
    ) void {
        JNIEnv.interface_call(
            env,
            .set_static_byte_field,
            .{ clazz, field_id, value },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#setstatictypefield-routines.
    pub inline fn set_static_char_field(
        env: *JNIEnv,
        clazz: JClass,
        field_id: JFieldID,
        value: JChar,
    ) void {
        JNIEnv.interface_call(
            env,
            .set_static_char_field,
            .{ clazz, field_id, value },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#setstatictypefield-routines.
    pub inline fn set_static_short_field(
        env: *JNIEnv,
        clazz: JClass,
        field_id: JFieldID,
        value: JShort,
    ) void {
        JNIEnv.interface_call(
            env,
            .set_static_short_field,
            .{ clazz, field_id, value },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#setstatictypefield-routines.
    pub inline fn set_static_int_field(
        env: *JNIEnv,
        clazz: JClass,
        field_id: JFieldID,
        value: JInt,
    ) void {
        JNIEnv.interface_call(
            env,
            .set_static_int_field,
            .{ clazz, field_id, value },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#setstatictypefield-routines.
    pub inline fn set_static_long_field(
        env: *JNIEnv,
        clazz: JClass,
        field_id: JFieldID,
        value: JLong,
    ) void {
        JNIEnv.interface_call(
            env,
            .set_static_long_field,
            .{ clazz, field_id, value },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#setstatictypefield-routines.
    pub inline fn set_static_float_field(
        env: *JNIEnv,
        clazz: JClass,
        field_id: JFieldID,
        value: JFloat,
    ) void {
        JNIEnv.interface_call(
            env,
            .set_static_float_field,
            .{ clazz, field_id, value },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#setstatictypefield-routines.
    pub inline fn set_static_double_field(
        env: *JNIEnv,
        clazz: JClass,
        field_id: JFieldID,
        value: JDouble,
    ) void {
        JNIEnv.interface_call(
            env,
            .set_static_double_field,
            .{ clazz, field_id, value },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#newstring.
    pub inline fn new_string(
        env: *JNIEnv,
        unicode_chars: [*]const JChar,
        size: JSize,
    ) JString {
        return JNIEnv.interface_call(
            env,
            .new_string,
            .{ unicode_chars, size },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getstringlength.
    pub inline fn get_string_length(
        env: *JNIEnv,
        string: JString,
    ) JSize {
        return JNIEnv.interface_call(
            env,
            .get_string_length,
            .{string},
        );
    }

    /// Returns a pointer to the array of Unicode characters of the string.
    /// This pointer is valid until release_string_chars is called.
    /// If is_copy is not NULL, then *is_copy is set to true if a copy is made,
    /// or it is set to false if no copy is made.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getstringchars.
    pub inline fn get_string_chars(
        env: *JNIEnv,
        string: JString,
        is_copy: ?*JBoolean,
    ) ?[*]const JChar {
        return JNIEnv.interface_call(
            env,
            .get_string_chars,
            .{ string, is_copy },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#releasestringchars.
    pub inline fn release_string_chars(
        env: *JNIEnv,
        string: JString,
        chars: [*]const JChar,
    ) void {
        JNIEnv.interface_call(
            env,
            .release_string_chars,
            .{ string, chars },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#newstringutf.
    pub inline fn new_string_utf(
        env: *JNIEnv,
        bytes: [*:0]const u8,
    ) JString {
        return JNIEnv.interface_call(
            env,
            .new_string_utf,
            .{bytes},
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getstringutflength.
    pub inline fn get_string_utf_length(
        env: *JNIEnv,
        string: JString,
    ) JSize {
        return JNIEnv.interface_call(
            env,
            .get_string_utf_length,
            .{string},
        );
    }

    /// Returns a pointer to an array of bytes representing the string in modified UTF-8 encoding.
    /// This array is valid until it is released by release_string_utf_chars.
    /// If isCopy is not NULL, then *is_copy is set to true if a copy is made,
    /// or it is set to false if no copy is made.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getstringutfchars.
    pub inline fn get_string_utf_chars(
        env: *JNIEnv,
        string: JString,
        is_copy: ?*JBoolean,
    ) ?[*:0]const u8 {
        return JNIEnv.interface_call(
            env,
            .get_string_utf_chars,
            .{ string, is_copy },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#releasestringutfchars.
    pub inline fn release_string_utf_chars(
        env: *JNIEnv,
        string: JString,
        utf: [*:0]const u8,
    ) void {
        JNIEnv.interface_call(
            env,
            .release_string_utf_chars,
            .{ string, utf },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getarraylength.
    pub inline fn get_array_length(
        env: *JNIEnv,
        array: JArray,
    ) JSize {
        return JNIEnv.interface_call(
            env,
            .get_array_length,
            .{array},
        );
    }

    /// Constructs a new array holding objects in class elementClass.
    /// All elements are initially set to initialElement.
    /// Returns a Java array object, or NULL if the array cannot be constructed.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#newobjectarray.
    pub inline fn new_object_array(
        env: *JNIEnv,
        length: JSize,
        element_class: JClass,
        initial_element: JObject,
    ) JObjectArray {
        return JNIEnv.interface_call(
            env,
            .new_object_array,
            .{ length, element_class, initial_element },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getobjectarrayelement.
    pub inline fn get_object_array_element(
        env: *JNIEnv,
        array: JObjectArray,
        index: JSize,
    ) JObject {
        return JNIEnv.interface_call(
            env,
            .get_object_array_element,
            .{ array, index },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#setobjectarrayelement.
    pub inline fn set_object_array_element(
        env: *JNIEnv,
        array: JObjectArray,
        index: JSize,
        value: JObject,
    ) void {
        JNIEnv.interface_call(
            env,
            .set_object_array_element,
            .{ array, index, value },
        );
    }

    /// Returns a Java array, or NULL if the array cannot be constructed.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#newprimitivetypearray-routines.
    pub inline fn new_boolean_array(
        env: *JNIEnv,
        length: JSize,
    ) JBooleanArray {
        return JNIEnv.interface_call(
            env,
            .new_boolean_array,
            .{length},
        );
    }

    /// Returns a Java array, or NULL if the array cannot be constructed.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#newprimitivetypearray-routines.
    pub inline fn new_byte_array(
        env: *JNIEnv,
        length: JSize,
    ) JByteArray {
        return JNIEnv.interface_call(
            env,
            .new_byte_array,
            .{length},
        );
    }

    /// Returns a Java array, or NULL if the array cannot be constructed.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#newprimitivetypearray-routines.
    pub inline fn new_char_array(
        env: *JNIEnv,
        length: JSize,
    ) JCharArray {
        return JNIEnv.interface_call(
            env,
            .new_char_array,
            .{length},
        );
    }

    /// Returns a Java array, or NULL if the array cannot be constructed.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#newprimitivetypearray-routines.
    pub inline fn new_short_array(
        env: *JNIEnv,
        length: JSize,
    ) JShortArray {
        return JNIEnv.interface_call(
            env,
            .new_short_array,
            .{length},
        );
    }

    /// Returns a Java array, or NULL if the array cannot be constructed.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#newprimitivetypearray-routines.
    pub inline fn new_int_array(
        env: *JNIEnv,
        length: JSize,
    ) JIntArray {
        return JNIEnv.interface_call(
            env,
            .new_int_array,
            .{length},
        );
    }

    /// Returns a Java array, or NULL if the array cannot be constructed.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#newprimitivetypearray-routines.
    pub inline fn new_long_array(
        env: *JNIEnv,
        length: JSize,
    ) JLongArray {
        return JNIEnv.interface_call(
            env,
            .new_long_array,
            .{length},
        );
    }

    /// Returns a Java array, or NULL if the array cannot be constructed.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#newprimitivetypearray-routines.
    pub inline fn new_float_array(
        env: *JNIEnv,
        length: JSize,
    ) JFloatArray {
        return JNIEnv.interface_call(
            env,
            .new_float_array,
            .{length},
        );
    }

    /// Returns a Java array, or NULL if the array cannot be constructed.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#newprimitivetypearray-routines.
    pub inline fn new_double_array(
        env: *JNIEnv,
        length: JSize,
    ) JDoubleArray {
        return JNIEnv.interface_call(
            env,
            .new_double_array,
            .{length},
        );
    }

    /// Returns the body of the primitive array.
    /// The result is valid until the corresponding release function is called.
    /// If isCopy is not NULL, then *is_copy is set to true if a copy is made,
    /// or it is set to false if no copy is made.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getprimitivetypearrayelements-routines.
    pub inline fn get_boolean_array_elements(
        env: *JNIEnv,
        array: JBooleanArray,
        is_copy: ?*JBoolean,
    ) ?[*]JBoolean {
        return JNIEnv.interface_call(
            env,
            .get_boolean_array_elements,
            .{ array, is_copy },
        );
    }

    /// Returns the body of the primitive array.
    /// The result is valid until the corresponding release function is called.
    /// If isCopy is not NULL, then *is_copy is set to true if a copy is made,
    /// or it is set to false if no copy is made.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getprimitivetypearrayelements-routines.
    pub inline fn get_byte_array_elements(
        env: *JNIEnv,
        array: JByteArray,
        is_copy: ?*JBoolean,
    ) ?[*]JByte {
        return JNIEnv.interface_call(
            env,
            .get_byte_array_elements,
            .{ array, is_copy },
        );
    }

    /// Returns the body of the primitive array.
    /// The result is valid until the corresponding release function is called.
    /// If isCopy is not NULL, then *is_copy is set to true if a copy is made,
    /// or it is set to false if no copy is made.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getprimitivetypearrayelements-routines.
    pub inline fn get_char_array_elements(
        env: *JNIEnv,
        array: JCharArray,
        is_copy: ?*JBoolean,
    ) ?[*]JChar {
        return JNIEnv.interface_call(
            env,
            .get_char_array_elements,
            .{ array, is_copy },
        );
    }

    /// Returns the body of the primitive array.
    /// The result is valid until the corresponding release function is called.
    /// If isCopy is not NULL, then *is_copy is set to true if a copy is made,
    /// or it is set to false if no copy is made.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getprimitivetypearrayelements-routines.
    pub inline fn get_short_array_elements(
        env: *JNIEnv,
        array: JShortArray,
        is_copy: ?*JBoolean,
    ) ?[*]JShort {
        return JNIEnv.interface_call(
            env,
            .get_short_array_elements,
            .{ array, is_copy },
        );
    }

    /// Returns the body of the primitive array.
    /// The result is valid until the corresponding release function is called.
    /// If isCopy is not NULL, then *is_copy is set to true if a copy is made,
    /// or it is set to false if no copy is made.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getprimitivetypearrayelements-routines.
    pub inline fn get_int_array_elements(
        env: *JNIEnv,
        array: JIntArray,
        is_copy: ?*JBoolean,
    ) ?[*]JInt {
        return JNIEnv.interface_call(
            env,
            .get_int_array_elements,
            .{ array, is_copy },
        );
    }

    /// Returns the body of the primitive array.
    /// The result is valid until the corresponding release function is called.
    /// If isCopy is not NULL, then *is_copy is set to true if a copy is made,
    /// or it is set to false if no copy is made.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getprimitivetypearrayelements-routines.
    pub inline fn get_long_array_elements(
        env: *JNIEnv,
        array: JLongArray,
        is_copy: ?*JBoolean,
    ) ?[*]JLong {
        return JNIEnv.interface_call(
            env,
            .get_long_array_elements,
            .{ array, is_copy },
        );
    }

    /// Returns the body of the primitive array.
    /// The result is valid until the corresponding release function is called.
    /// If isCopy is not NULL, then *is_copy is set to true if a copy is made,
    /// or it is set to false if no copy is made.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getprimitivetypearrayelements-routines.
    pub inline fn get_float_array_elements(
        env: *JNIEnv,
        array: JFloatArray,
        is_copy: ?*JBoolean,
    ) ?[*]JFloat {
        return JNIEnv.interface_call(
            env,
            .get_float_array_elements,
            .{ array, is_copy },
        );
    }

    /// Index 190: GetDoubleArrayElements
    /// Returns the body of the primitive array.
    /// The result is valid until the corresponding release function is called.
    /// If isCopy is not NULL, then *is_copy is set to true if a copy is made,
    /// or it is set to false if no copy is made.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getprimitivetypearrayelements-routines.
    pub inline fn get_double_array_elements(
        env: *JNIEnv,
        array: JDoubleArray,
        is_copy: ?*JBoolean,
    ) ?[*]JDouble {
        return JNIEnv.interface_call(
            env,
            .get_double_array_elements,
            .{ array, is_copy },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#releaseprimitivetypearrayelements-routines.
    pub inline fn release_boolean_array_elements(
        env: *JNIEnv,
        array: JBooleanArray,
        elems: [*]JBoolean,
        mode: JArrayReleaseMode,
    ) void {
        JNIEnv.interface_call(
            env,
            .release_boolean_array_elements,
            .{ array, elems, mode },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#releaseprimitivetypearrayelements-routines.
    pub inline fn release_byte_array_elements(
        env: *JNIEnv,
        array: JByteArray,
        elems: [*]JByte,
        mode: JArrayReleaseMode,
    ) void {
        JNIEnv.interface_call(
            env,
            .release_byte_array_elements,
            .{ array, elems, mode },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#releaseprimitivetypearrayelements-routines.
    pub inline fn release_char_array_elements(
        env: *JNIEnv,
        array: JCharArray,
        elems: [*]JChar,
        mode: JArrayReleaseMode,
    ) void {
        JNIEnv.interface_call(
            env,
            .release_char_array_elements,
            .{ array, elems, mode },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#releaseprimitivetypearrayelements-routines.
    pub inline fn release_short_array_elements(
        env: *JNIEnv,
        array: JShortArray,
        elems: [*]JShort,
        mode: JArrayReleaseMode,
    ) void {
        JNIEnv.interface_call(
            env,
            .release_short_array_elements,
            .{ array, elems, mode },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#releaseprimitivetypearrayelements-routines.
    pub inline fn release_int_array_elements(
        env: *JNIEnv,
        array: JIntArray,
        elems: [*]JInt,
        mode: JArrayReleaseMode,
    ) void {
        JNIEnv.interface_call(
            env,
            .release_int_array_elements,
            .{ array, elems, mode },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#releaseprimitivetypearrayelements-routines.
    pub inline fn release_long_array_elements(
        env: *JNIEnv,
        array: JLongArray,
        elems: [*]JLong,
        mode: JArrayReleaseMode,
    ) void {
        JNIEnv.interface_call(
            env,
            .release_long_array_elements,
            .{ array, elems, mode },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#releaseprimitivetypearrayelements-routines.
    pub inline fn release_float_array_elements(
        env: *JNIEnv,
        array: JFloatArray,
        elems: [*]JFloat,
        mode: JArrayReleaseMode,
    ) void {
        JNIEnv.interface_call(
            env,
            .release_float_array_elements,
            .{ array, elems, mode },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#releaseprimitivetypearrayelements-routines.
    pub inline fn release_double_array_elements(
        env: *JNIEnv,
        array: JDoubleArray,
        elems: [*]JDouble,
        mode: JArrayReleaseMode,
    ) void {
        JNIEnv.interface_call(
            env,
            .release_double_array_elements,
            .{ array, elems, mode },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getprimitivetypearrayregion-routines.
    pub inline fn get_boolean_array_region(
        env: *JNIEnv,
        array: JBooleanArray,
        start: JSize,
        len: JSize,
        buf: [*]JBoolean,
    ) void {
        JNIEnv.interface_call(
            env,
            .get_boolean_array_region,
            .{ array, start, len, buf },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getprimitivetypearrayregion-routines.
    pub inline fn get_byte_array_region(
        env: *JNIEnv,
        array: JByteArray,
        start: JSize,
        len: JSize,
        buf: [*]JByte,
    ) void {
        JNIEnv.interface_call(
            env,
            .get_byte_array_region,
            .{ array, start, len, buf },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getprimitivetypearrayregion-routines.
    pub inline fn get_char_array_region(
        env: *JNIEnv,
        array: JCharArray,
        start: JSize,
        len: JSize,
        buf: [*]JChar,
    ) void {
        JNIEnv.interface_call(
            env,
            .get_char_array_region,
            .{ array, start, len, buf },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getprimitivetypearrayregion-routines.
    pub inline fn get_short_array_region(
        env: *JNIEnv,
        array: JShortArray,
        start: JSize,
        len: JSize,
        buf: [*]JShort,
    ) void {
        JNIEnv.interface_call(
            env,
            .get_short_array_region,
            .{ array, start, len, buf },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getprimitivetypearrayregion-routines.
    pub inline fn get_int_array_region(
        env: *JNIEnv,
        array: JIntArray,
        start: JSize,
        len: JSize,
        buf: [*]JInt,
    ) void {
        JNIEnv.interface_call(
            env,
            .get_int_array_region,
            .{ array, start, len, buf },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getprimitivetypearrayregion-routines.
    pub inline fn get_long_array_region(
        env: *JNIEnv,
        array: JLongArray,
        start: JSize,
        len: JSize,
        buf: [*]JLong,
    ) void {
        JNIEnv.interface_call(
            env,
            .get_long_array_region,
            .{ array, start, len, buf },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getprimitivetypearrayregion-routines.
    pub inline fn get_float_array_region(
        env: *JNIEnv,
        array: JFloatArray,
        start: JSize,
        len: JSize,
        buf: [*]JFloat,
    ) void {
        JNIEnv.interface_call(
            env,
            .get_float_array_region,
            .{ array, start, len, buf },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getprimitivetypearrayregion-routines.
    pub inline fn get_double_array_region(
        env: *JNIEnv,
        array: JDoubleArray,
        start: JSize,
        len: JSize,
        buf: [*]JDouble,
    ) void {
        JNIEnv.interface_call(
            env,
            .get_double_array_region,
            .{ array, start, len, buf },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#setprimitivetypearrayregion-routines.
    pub inline fn set_boolean_array_region(
        env: *JNIEnv,
        array: JBooleanArray,
        start: JSize,
        len: JSize,
        buf: [*]const JBoolean,
    ) void {
        JNIEnv.interface_call(
            env,
            .set_boolean_array_region,
            .{ array, start, len, buf },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#setprimitivetypearrayregion-routines.
    pub inline fn set_byte_array_region(
        env: *JNIEnv,
        array: JByteArray,
        start: JSize,
        len: JSize,
        buf: [*]const JByte,
    ) void {
        JNIEnv.interface_call(
            env,
            .set_byte_array_region,
            .{ array, start, len, buf },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#setprimitivetypearrayregion-routines.
    pub inline fn set_char_array_region(
        env: *JNIEnv,
        array: JCharArray,
        start: JSize,
        len: JSize,
        buf: [*]const JChar,
    ) void {
        JNIEnv.interface_call(
            env,
            .set_char_array_region,
            .{ array, start, len, buf },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#setprimitivetypearrayregion-routines.
    pub inline fn set_short_array_region(
        env: *JNIEnv,
        array: JShortArray,
        start: JSize,
        len: JSize,
        buf: [*]const JShort,
    ) void {
        JNIEnv.interface_call(
            env,
            .set_short_array_region,
            .{ array, start, len, buf },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#setprimitivetypearrayregion-routines.
    pub inline fn set_int_array_region(
        env: *JNIEnv,
        array: JIntArray,
        start: JSize,
        len: JSize,
        buf: [*]const JInt,
    ) void {
        JNIEnv.interface_call(
            env,
            .set_int_array_region,
            .{ array, start, len, buf },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#setprimitivetypearrayregion-routines.
    pub inline fn set_long_array_region(
        env: *JNIEnv,
        array: JLongArray,
        start: JSize,
        len: JSize,
        buf: [*]const JLong,
    ) void {
        JNIEnv.interface_call(
            env,
            .set_long_array_region,
            .{ array, start, len, buf },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#setprimitivetypearrayregion-routines.
    pub inline fn set_float_array_region(
        env: *JNIEnv,
        array: JFloatArray,
        start: JSize,
        len: JSize,
        buf: [*]const JFloat,
    ) void {
        JNIEnv.interface_call(
            env,
            .set_float_array_region,
            .{ array, start, len, buf },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#setprimitivetypearrayregion-routines.
    pub inline fn set_double_array_region(
        env: *JNIEnv,
        array: JDoubleArray,
        start: JSize,
        len: JSize,
        buf: [*]const JDouble,
    ) void {
        JNIEnv.interface_call(
            env,
            .set_double_array_region,
            .{ array, start, len, buf },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#registernatives.
    pub inline fn register_natives(
        env: *JNIEnv,
        clazz: JClass,
        methods: [*]const JNINativeMethod,
        methods_len: JInt,
    ) JNIResultType {
        return JNIEnv.interface_call(
            env,
            .register_natives,
            .{ clazz, methods, methods_len },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#unregisternatives.
    pub inline fn unregister_natives(
        env: *JNIEnv,
        clazz: JClass,
    ) JNIResultType {
        return JNIEnv.interface_call(
            env,
            .unregister_natives,
            .{clazz},
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#monitorenter.
    pub inline fn monitor_enter(
        env: *JNIEnv,
        obj: JObject,
    ) JNIResultType {
        return JNIEnv.interface_call(
            env,
            .monitor_enter,
            .{obj},
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#monitorexit.
    pub inline fn monitor_exit(
        env: *JNIEnv,
        obj: JObject,
    ) JNIResultType {
        return JNIEnv.interface_call(
            env,
            .monitor_exit,
            .{obj},
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getjavavm.
    pub inline fn get_java_vm(
        env: *JNIEnv,
        vm: **JavaVM,
    ) JNIResultType {
        return JNIEnv.interface_call(
            env,
            .get_java_vm,
            .{vm},
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getstringregion.
    pub inline fn get_string_region(
        env: *JNIEnv,
        string: JString,
        start: JSize,
        len: JSize,
        buf: [*]JChar,
    ) void {
        JNIEnv.interface_call(
            env,
            .get_string_region,
            .{ string, start, len, buf },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getstringutfregion.
    pub inline fn get_string_utf_region(
        env: *JNIEnv,
        string: JString,
        start: JSize,
        len: JSize,
        buf: [*]u8,
    ) void {
        JNIEnv.interface_call(
            env,
            .get_string_utf_region,
            .{ string, start, len, buf },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getprimitivearraycritical.
    pub inline fn get_primitive_array_critical(
        env: *JNIEnv,
        array: JArray,
        is_copy: ?*JBoolean,
    ) ?*anyopaque {
        return JNIEnv.interface_call(
            env,
            .get_primitive_array_critical,
            .{ array, is_copy },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#releaseprimitivearraycritical.
    pub inline fn release_primitive_array_critical(
        env: *JNIEnv,
        array: JArray,
        c_array: *anyopaque,
        mode: JArrayReleaseMode,
    ) void {
        JNIEnv.interface_call(
            env,
            .release_primitive_array_critical,
            .{ array, c_array, mode },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getstringcritical-releasestringcritical.
    pub inline fn get_string_critical(
        env: *JNIEnv,
        string: JString,
        is_copy: ?*JBoolean,
    ) ?[*]const JChar {
        return JNIEnv.interface_call(
            env,
            .get_string_critical,
            .{ string, is_copy },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getstringcritical-releasestringcritical.
    pub inline fn release_string_critical(
        env: *JNIEnv,
        string: JString,
        chars: [*]const JChar,
    ) void {
        JNIEnv.interface_call(
            env,
            .release_string_critical,
            .{ string, chars },
        );
    }

    /// Returns NULL if obj refers to null, or if the VM runs out of memory.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#newweakglobalref.
    pub inline fn new_weak_global_ref(
        env: *JNIEnv,
        obj: JObject,
    ) JWeakReference {
        return JNIEnv.interface_call(
            env,
            .new_weak_global_ref,
            .{obj},
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#deleteweakglobalref.
    pub inline fn delete_weak_global_ref(
        env: *JNIEnv,
        ref: JWeakReference,
    ) void {
        JNIEnv.interface_call(
            env,
            .delete_weak_global_ref,
            .{ref},
        );
    }

    /// Returns true when there is a pending exception; otherwise, returns false.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#exceptioncheck.
    pub inline fn exception_check(
        env: *JNIEnv,
    ) JBoolean {
        return JNIEnv.interface_call(
            env,
            .exception_check,
            .{},
        );
    }

    /// Allocates and returns a direct java.nio.ByteBuffer.
    /// Returns NULL if an exception occurs,
    /// or if JNI access to direct buffers is not supported by this virtual machine.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#newdirectbytebuffer.
    pub inline fn new_direct_byte_buffer(
        env: *JNIEnv,
        address: *anyopaque,
        capacity: JLong,
    ) JObject {
        return JNIEnv.interface_call(
            env,
            .new_direct_byte_buffer,
            .{ address, capacity },
        );
    }

    /// Returns the starting address of the memory region referenced by the buffer.
    /// Returns NULL if the memory region is undefined,
    /// if the given object is not a direct java.nio.Buffer,
    /// or if JNI access to direct buffers is not supported by this virtual machine.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getdirectbufferaddress.
    pub inline fn get_direct_buffer_address(
        env: *JNIEnv,
        buf: JObject,
    ) ?[*]u8 {
        return JNIEnv.interface_call(
            env,
            .get_direct_buffer_address,
            .{buf},
        );
    }

    /// Returns the capacity of the memory region associated with the buffer.
    /// Returns -1 if the given object is not a direct java.nio.Buffer,
    /// if the object is an unaligned view buffer and the processor architecture
    /// does not support unaligned access,
    /// or if JNI access to direct buffers is not supported by this virtual machine.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getdirectbuffercapacity.
    pub inline fn get_direct_buffer_capacity(
        env: *JNIEnv,
        buf: JObject,
    ) JLong {
        return JNIEnv.interface_call(
            env,
            .get_direct_buffer_capacity,
            .{buf},
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getobjectreftype.
    pub inline fn get_object_ref_type(
        env: *JNIEnv,
        obj: JObject,
    ) JObjectRefType {
        return JNIEnv.interface_call(
            env,
            .get_object_ref_type,
            .{obj},
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/functions.html#getmodule.
    pub inline fn get_module(
        env: *JNIEnv,
        clazz: JClass,
    ) JObject {
        return JNIEnv.interface_call(
            env,
            .get_module,
            .{clazz},
        );
    }
};

/// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/invocation.html#jni_createjavavm.
pub const JavaVMOption = extern struct {
    /// The option as a string in the default platform encoding.
    option_string: [*:0]const u8,
    extra_info: ?*anyopaque = null,
};

/// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/invocation.html#jni_createjavavm.
pub const JavaVMInitArgs = extern struct {
    version: JInt,
    options_len: JInt,
    options: ?[*]JavaVMOption,
    ignore_unrecognized: JBoolean,
};

/// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/invocation.html#attachcurrentthread.
pub const JavaVMAttachArgs = extern struct {
    version: JInt,
    name: [*:0]const u8,
    group: JObject,
};

/// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/invocation.html
pub const JavaVM = opaque {
    /// Each function is accessible at a fixed offset through the JavaVM argument.
    /// The JavaVM type is a pointer to the invocation API function table.
    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/invocation.html#invocation-api-functions.
    const FunctionTable = enum(usize) {
        /// Index 3: DestroyJavaVM.
        destroy_java_vm = 3,

        /// Index 4: AttachCurrentThread.
        attach_current_thread = 4,

        /// Index 5: DetachCurrentThread.
        detach_current_thread = 5,

        /// Index 6: GetEnv.
        get_env = 6,

        /// Index 7: AttachCurrentThreadAsDaemon.
        attach_current_thread_as_daemon = 7,
    };

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/invocation.html#jni_getdefaultjavavminitargs.
    pub const get_default_java_vm_init_args = struct {
        extern "jvm" fn JNI_GetDefaultJavaVMInitArgs(
            vm_args: ?*JavaVMInitArgs,
        ) callconv(.C) JNIResultType;
    }.JNI_GetDefaultJavaVMInitArgs;

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/invocation.html#jni_get_createdjavavm.
    pub const get_created_java_vm = struct {
        extern "jvm" fn JNI_GetCreatedJavaVMs(
            vm_buf: [*]*JavaVM,
            buf_len: JSize,
            vm_len: ?*JSize,
        ) callconv(.C) JNIResultType;
    }.JNI_GetCreatedJavaVMs;

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/invocation.html#jni_createjavavm.
    pub const create_java_vm = struct {
        extern "jvm" fn JNI_CreateJavaVM(
            jvm: **JavaVM,
            env: **JNIEnv,
            args: *JavaVMInitArgs,
        ) callconv(.C) JNIResultType;
    }.JNI_CreateJavaVM;

    usingnamespace JniInterface(JavaVM);

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/invocation.html#destroyjavavm.
    pub inline fn destroy_java_vm(
        vm: *JavaVM,
    ) JNIResultType {
        return JavaVM.interface_call(
            vm,
            .destroy_java_vm,
            .{},
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/invocation.html#attachcurrentthread.
    pub inline fn attach_current_thread(
        vm: *JavaVM,
        env: **JNIEnv,
        args: ?*JavaVMAttachArgs,
    ) JNIResultType {
        return JavaVM.interface_call(
            vm,
            .attach_current_thread,
            .{ env, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/invocation.html#detachcurrentthread.
    pub inline fn detach_current_thread(
        vm: *JavaVM,
    ) JNIResultType {
        return JavaVM.interface_call(
            vm,
            .detach_current_thread,
            .{},
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/invocation.html#attach_currentthreadasdaemon.
    pub inline fn attach_current_thread_as_daemon(
        vm: *JavaVM,
        env: **JNIEnv,
        args: ?*JavaVMAttachArgs,
    ) JNIResultType {
        return JavaVM.interface_call(
            vm,
            .attach_current_thread_as_daemon,
            .{ env, args },
        );
    }

    /// https://docs.oracle.com/en/java/javase/17/docs/specs/jni/invocation.html#getenv
    pub inline fn get_env(
        vm: *JavaVM,
        env: **JNIEnv,
        version: JInt,
    ) JNIResultType {
        return JavaVM.interface_call(
            vm,
            .get_env,
            .{ env, version },
        );
    }
};

/// Invokes a function at the offset of the vtable, allowing to utilize the function pointer
/// without the need to declare a VTable layout, where the ordering of fields defines the ABI.
/// The function index is stored within T.FunctionTable enum, which only holds the index value.
/// The function signature is declared as an inline function within the T opaque type.
fn JniInterface(comptime T: type) type {
    return struct {
        fn JniFn(comptime function: T.FunctionTable) type {
            const Fn = @TypeOf(@field(T, @tagName(function)));
            var fn_info = @typeInfo(Fn);
            switch (fn_info) {
                .Fn => {
                    fn_info.Fn.calling_convention = JNICALL;
                    return @Type(fn_info);
                },
                else => @compileError("Expected " ++ @tagName(function) ++ " to be a function"),
            }
        }

        pub inline fn interface_call(
            self: *T,
            comptime function: T.FunctionTable,
            args: anytype,
        ) return_type: {
            const type_info = @typeInfo(JniFn(function));
            break :return_type type_info.Fn.return_type.?;
        } {
            const Fn = JniFn(function);
            const VTable = extern struct {
                functions: [*]const *const anyopaque,
            };

            const vtable: *VTable = @ptrCast(@alignCast(self));
            const fn_ptr: *const Fn = @ptrCast(@alignCast(
                vtable.functions[@intFromEnum(function)],
            ));
            return @call(.auto, fn_ptr, .{self} ++ args);
        }
    };
}
