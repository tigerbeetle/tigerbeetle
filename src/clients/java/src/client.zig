///! Java Native Interfaces for TigerBeetle Client
///! Please refer to the JNI Best Practices Guide:
///! https://developer.ibm.com/articles/j-jni/

// IMPORTANT: Running code from a native thread, the JVM will
// never automatically free local references until the thread detaches.
// To avoid leaks, we *ALWAYS* free all references we acquire,
// even local references, so we don't need to distinguish if the code
// is called from the JVM or the native thread via callback.

const std = @import("std");
const builtin = @import("builtin");
const jni = @import("jni.zig");
const tb = @import("../../c/tb_client.zig");

const log = std.log.scoped(.tb_client_jni);
const assert = std.debug.assert;

const jni_version = jni.jni_version_10;

const global_allocator = if (builtin.link_libc)
    std.heap.c_allocator
else
    @compileError("tb_client must be built with libc");

/// Context for a client instance.
const Context = struct {
    jvm: *jni.JavaVM,
    client: tb.tb_client_t,
};

/// NativeClient implementation.
const NativeClient = struct {
    /// On JVM loads this library.
    fn on_load(vm: *jni.JavaVM) jni.JInt {
        var env = JNIHelper.get_env(vm);
        ReflectionHelper.load(env);
        return jni_version;
    }

    /// On JVM unloads this library.
    fn on_unload(vm: *jni.JavaVM) void {
        var env = JNIHelper.get_env(vm);
        ReflectionHelper.unload(env);
    }

    /// Native clientInit and clientInitEcho implementation.
    fn client_init(
        comptime echo_client: bool,
        env: *jni.JNIEnv,
        cluster_id: u128,
        addresses_obj: jni.JString,
        max_concurrency: u32,
    ) ?*Context {
        const addresses = JNIHelper.get_string_utf(env, addresses_obj) orelse {
            ReflectionHelper.initialization_exception_throw(
                env,
                tb.tb_status_t.address_invalid,
            );
            return null;
        };
        defer env.release_string_utf_chars(addresses_obj, addresses.ptr);

        var context = global_allocator.create(Context) catch {
            ReflectionHelper.initialization_exception_throw(env, tb.tb_status_t.out_of_memory);
            return null;
        };
        errdefer global_allocator.destroy(context);

        const init_fn = if (echo_client) tb.init_echo else tb.init;
        const client = init_fn(
            global_allocator,
            cluster_id,
            addresses,
            max_concurrency,
            @intFromPtr(context),
            on_completion,
        ) catch |err| {
            const status = tb.init_error_to_status(err);
            ReflectionHelper.initialization_exception_throw(env, status);
            return null;
        };

        context.* = .{
            .jvm = JNIHelper.get_java_vm(env),
            .client = client,
        };
        return context;
    }

    /// Native clientDeinit implementation.
    fn client_deinit(context: *Context) void {
        defer global_allocator.destroy(context);
        tb.deinit(context.client);
    }

    /// Native submit implementation.
    fn submit(
        env: *jni.JNIEnv,
        context: *Context,
        request_obj: jni.JObject,
    ) tb.tb_packet_acquire_status_t {
        assert(request_obj != null);

        const operation = ReflectionHelper.get_request_operation(env, request_obj);
        const send_buffer: []u8 = ReflectionHelper.get_send_buffer_slice(env, request_obj) orelse {
            ReflectionHelper.assertion_error_throw(
                env,
                "Request.sendBuffer is null or invalid",
            );
            return undefined;
        };

        var out_packet: ?*tb.tb_packet_t = null;
        const acquire_status = tb.acquire_packet(context.client, &out_packet);

        if (out_packet) |packet| {
            assert(acquire_status == .ok);

            // Holds a global reference to prevent GC before the callback.
            const global_ref = JNIHelper.new_global_reference(env, request_obj);

            packet.operation = operation;
            packet.user_data = global_ref;
            packet.data = send_buffer.ptr;
            packet.data_size = @as(u32, @intCast(send_buffer.len));
            packet.next = null;
            packet.status = .ok;
            tb.submit(context.client, packet);
        } else {
            assert(acquire_status != .ok);
        }

        return acquire_status;
    }

    /// Completion callback, always called from the native thread.
    fn on_completion(
        context_ptr: usize,
        client: tb.tb_client_t,
        packet: *tb.tb_packet_t,
        result_ptr: ?[*]const u8,
        result_len: u32,
    ) callconv(.C) void {
        _ = client;
        var context = @as(*Context, @ptrFromInt(context_ptr));
        var env = JNIHelper.attach_current_thread(context.jvm);

        // Retrieves the request instance, and drops the GC reference.
        assert(packet.user_data != null);
        var request_obj = @as(jni.JObject, @ptrCast(packet.user_data));
        defer env.delete_global_ref(request_obj);

        const packet_operation = packet.operation;
        const packet_status = packet.status;
        if (result_len > 0) {
            switch (packet_status) {
                .ok => if (result_ptr) |ptr| {
                    // Copying the reply before releasing the packet.
                    ReflectionHelper.set_reply_buffer(
                        env,
                        request_obj,
                        ptr[0..@as(usize, @intCast(result_len))],
                    );
                },
                else => {},
            }
        }
        tb.release_packet(context.client, packet);

        ReflectionHelper.end_request(
            env,
            request_obj,
            packet_operation,
            packet_status,
        );
    }
};

// Declares and exports all functions using the JNI naming/calling convention.
comptime {
    // https://docs.oracle.com/en/java/javase/17/docs/specs/jni/design.html#compiling-loading-and-linking-native-methods.
    const prefix = "Java_com_tigerbeetle_NativeClient_";

    const Exports = struct {
        fn on_load(vm: *jni.JavaVM) callconv(jni.JNICALL) jni.JInt {
            return NativeClient.on_load(vm);
        }

        fn on_unload(vm: *jni.JavaVM) callconv(jni.JNICALL) void {
            NativeClient.on_unload(vm);
        }

        fn client_init(
            env: *jni.JNIEnv,
            class: jni.JClass,
            cluster_id: jni.JByteArray,
            addresses: jni.JString,
            max_concurrency: jni.JInt,
        ) callconv(jni.JNICALL) jni.JLong {
            _ = class;
            assert(env.get_array_length(cluster_id) == 16);

            const cluster_id_elements = env.get_byte_array_elements(cluster_id, null).?;
            defer env.release_byte_array_elements(cluster_id, cluster_id_elements, .abort);

            var context = NativeClient.client_init(
                false,
                env,
                @as(u128, @bitCast(cluster_id_elements[0..16].*)),
                addresses,
                @as(u32, @bitCast(max_concurrency)),
            );
            return @as(jni.JLong, @bitCast(@intFromPtr(context)));
        }

        fn client_init_echo(
            env: *jni.JNIEnv,
            class: jni.JClass,
            cluster_id: jni.JByteArray,
            addresses: jni.JString,
            max_concurrency: jni.JInt,
        ) callconv(jni.JNICALL) jni.JLong {
            _ = class;
            assert(env.get_array_length(cluster_id) == 16);

            const cluster_id_elements = env.get_byte_array_elements(cluster_id, null).?;
            defer env.release_byte_array_elements(cluster_id, cluster_id_elements, .abort);

            var context = NativeClient.client_init(
                true,
                env,
                @as(u128, @bitCast(cluster_id_elements[0..16].*)),
                addresses,
                @as(u32, @bitCast(max_concurrency)),
            );
            return @as(jni.JLong, @bitCast(@intFromPtr(context)));
        }

        fn client_deinit(
            env: *jni.JNIEnv,
            class: jni.JClass,
            context_handle: jni.JLong,
        ) callconv(jni.JNICALL) void {
            _ = env;
            _ = class;
            NativeClient.client_deinit(@as(*Context, @ptrFromInt(@as(usize, @bitCast(context_handle)))));
        }

        fn submit(
            env: *jni.JNIEnv,
            class: jni.JClass,
            context_handle: jni.JLong,
            request_obj: jni.JObject,
        ) callconv(jni.JNICALL) jni.JInt {
            _ = class;
            assert(context_handle != 0);
            const packet_acquire_status = NativeClient.submit(
                env,
                @as(*Context, @ptrFromInt(@as(usize, @bitCast(context_handle)))),
                request_obj,
            );

            return @as(jni.JInt, @intCast(@intFromEnum(packet_acquire_status)));
        }
    };

    @export(Exports.on_load, .{ .name = "JNI_OnLoad", .linkage = .Strong });
    @export(Exports.on_unload, .{ .name = "JNI_OnUnload", .linkage = .Strong });

    @export(Exports.client_init, .{ .name = prefix ++ "clientInit", .linkage = .Strong });
    @export(Exports.client_init_echo, .{ .name = prefix ++ "clientInitEcho", .linkage = .Strong });
    @export(Exports.client_deinit, .{ .name = prefix ++ "clientDeinit", .linkage = .Strong });
    @export(Exports.submit, .{ .name = prefix ++ "submit", .linkage = .Strong });
}

/// Reflection helper and metadata cache.
const ReflectionHelper = struct {
    var initialization_exception_class: jni.JClass = null;
    var initialization_exception_ctor_id: jni.JMethodID = null;
    var assertion_error_class: jni.JClass = null;

    var request_class: jni.JClass = null;
    var request_send_buffer_field_id: jni.JFieldID = null;
    var request_send_buffer_len_field_id: jni.JFieldID = null;
    var request_reply_buffer_field_id: jni.JFieldID = null;
    var request_operation_method_id: jni.JMethodID = null;
    var request_end_request_method_id: jni.JMethodID = null;

    pub fn load(env: *jni.JNIEnv) void {
        // Asserting we are not initialized yet:
        assert(initialization_exception_class == null);
        assert(initialization_exception_ctor_id == null);
        assert(assertion_error_class == null);
        assert(request_class == null);
        assert(request_send_buffer_field_id == null);
        assert(request_send_buffer_len_field_id == null);
        assert(request_reply_buffer_field_id == null);
        assert(request_operation_method_id == null);
        assert(request_end_request_method_id == null);

        initialization_exception_class = JNIHelper.find_class(
            env,
            "com/tigerbeetle/InitializationException",
        );
        initialization_exception_ctor_id = JNIHelper.find_method(
            env,
            initialization_exception_class,
            "<init>",
            "(I)V",
        );

        assertion_error_class = JNIHelper.find_class(
            env,
            "com/tigerbeetle/AssertionError",
        );

        request_class = JNIHelper.find_class(
            env,
            "com/tigerbeetle/Request",
        );
        request_send_buffer_field_id = JNIHelper.find_field(
            env,
            request_class,
            "sendBuffer",
            "Ljava/nio/ByteBuffer;",
        );
        request_send_buffer_len_field_id = JNIHelper.find_field(
            env,
            request_class,
            "sendBufferLen",
            "J",
        );
        request_reply_buffer_field_id = JNIHelper.find_field(
            env,
            request_class,
            "replyBuffer",
            "[B",
        );
        request_operation_method_id = JNIHelper.find_method(
            env,
            request_class,
            "getOperation",
            "()B",
        );
        request_end_request_method_id = JNIHelper.find_method(
            env,
            request_class,
            "endRequest",
            "(BB)V",
        );

        // Asserting we are full initialized:
        assert(initialization_exception_class != null);
        assert(initialization_exception_ctor_id != null);
        assert(assertion_error_class != null);
        assert(request_class != null);
        assert(request_send_buffer_field_id != null);
        assert(request_send_buffer_len_field_id != null);
        assert(request_reply_buffer_field_id != null);
        assert(request_operation_method_id != null);
        assert(request_end_request_method_id != null);
    }

    pub fn unload(env: *jni.JNIEnv) void {
        env.delete_global_ref(initialization_exception_class);
        env.delete_global_ref(assertion_error_class);
        env.delete_global_ref(request_class);

        initialization_exception_class = null;
        initialization_exception_ctor_id = null;
        assertion_error_class = null;
        request_class = null;
        request_send_buffer_field_id = null;
        request_send_buffer_len_field_id = null;
        request_reply_buffer_field_id = null;
        request_operation_method_id = null;
        request_end_request_method_id = null;
    }

    pub fn initialization_exception_throw(env: *jni.JNIEnv, status: tb.tb_status_t) void {
        assert(initialization_exception_class != null);
        assert(initialization_exception_ctor_id != null);

        var exception = env.new_object(
            initialization_exception_class,
            initialization_exception_ctor_id,
            &[_]jni.JValue{jni.JValue.to_jvalue(@as(jni.JInt, @bitCast(@intFromEnum(status))))},
        ) orelse {
            // It's unexpected here: we did not initialize correctly or the JVM is out of memory.
            JNIHelper.vm_panic(env, "Unexpected error creating a new InitializationException.", .{});
        };
        defer env.delete_local_ref(exception);

        const jni_result = env.throw(exception);
        JNIHelper.check_jni_result(
            env,
            jni_result,
            "Unexpected error throwing InitializationException.",
            .{},
        );

        assert(env.exception_check() == .jni_true);
    }

    pub fn assertion_error_throw(env: *jni.JNIEnv, message: [:0]const u8) void {
        assert(assertion_error_class != null);

        const jni_result = env.throw_new(assertion_error_class, message.ptr);
        JNIHelper.check_jni_result(
            env,
            jni_result,
            "Unexpected error throwing AssertionError.",
            .{},
        );
        assert(env.exception_check() == .jni_true);
    }

    pub fn get_send_buffer_slice(env: *jni.JNIEnv, this_obj: jni.JObject) ?[]u8 {
        assert(this_obj != null);
        assert(request_send_buffer_field_id != null);
        assert(request_send_buffer_len_field_id != null);

        var buffer_obj = env.get_object_field(this_obj, request_send_buffer_field_id) orelse
            return null;
        defer env.delete_local_ref(buffer_obj);

        var direct_buffer: []u8 = JNIHelper.get_direct_buffer(env, buffer_obj) orelse
            return null;

        var buffer_len = env.get_long_field(this_obj, request_send_buffer_len_field_id);
        if (buffer_len < 0 or buffer_len > direct_buffer.len)
            return null;

        return direct_buffer[0..@as(usize, @intCast(buffer_len))];
    }

    pub fn set_reply_buffer(env: *jni.JNIEnv, this_obj: jni.JObject, reply: []const u8) void {
        assert(this_obj != null);
        assert(request_reply_buffer_field_id != null);
        assert(reply.len > 0);

        var reply_buffer_obj = env.new_byte_array(
            @as(jni.JInt, @intCast(reply.len)),
        ) orelse {
            // Cannot allocate an array, it's likely the JVM has run out of resources.
            // Printing the buffer size here just to help diagnosing how much memory was required.
            JNIHelper.vm_panic(
                env,
                "Unexpected error calling NewByteArray len={}",
                .{reply.len},
            );
        };
        defer env.delete_local_ref(reply_buffer_obj);

        env.set_byte_array_region(
            reply_buffer_obj,
            0,
            @as(jni.JInt, @intCast(reply.len)),
            @as([*]const jni.JByte, @ptrCast(reply.ptr)),
        );

        if (env.exception_check() == .jni_true) {
            // Since out-of-bounds isn't expected here, we can only panic if it fails.
            JNIHelper.vm_panic(
                env,
                "Unexpected exception calling JNIEnv.SetByteArrayRegion len={}",
                .{reply.len},
            );
        }

        // Setting the request with the reply.
        env.set_object_field(
            this_obj,
            request_reply_buffer_field_id,
            reply_buffer_obj,
        );
    }

    pub fn get_request_operation(env: *jni.JNIEnv, this_obj: jni.JObject) u8 {
        assert(this_obj != null);
        assert(request_class != null);
        assert(request_operation_method_id != null);

        const value = env.call_nonvirtual_byte_method(
            this_obj,
            request_class,
            request_operation_method_id,
            null,
        );

        if (env.exception_check() == .jni_true) {
            // This method isn't expected to throw any exception.
            JNIHelper.vm_panic(
                env,
                "Unexpected exception calling NativeClient.getOperation",
                .{},
            );
        }
        return @as(u8, @bitCast(value));
    }

    pub fn end_request(
        env: *jni.JNIEnv,
        this_obj: jni.JObject,
        packet_operation: u8,
        packet_status: tb.tb_packet_status_t,
    ) void {
        assert(this_obj != null);
        assert(request_class != null);
        assert(request_end_request_method_id != null);

        env.call_nonvirtual_void_method(
            this_obj,
            request_class,
            request_end_request_method_id,
            &[_]jni.JValue{
                jni.JValue.to_jvalue(@as(jni.JByte, @bitCast(packet_operation))),
                jni.JValue.to_jvalue(@as(jni.JByte, @bitCast(@intFromEnum(packet_status)))),
            },
        );

        if (env.exception_check() == .jni_true) {
            // The "endRequest" method isn't expected to throw any exception,
            // We can't rethrow here, since this function is called from the native callback.
            JNIHelper.vm_panic(
                env,
                "Unexpected exception calling NativeClient.endRequest",
                .{},
            );
        }
    }
};

/// Common functions for handling errors and results in JNI calls.
const JNIHelper = struct {
    pub inline fn get_env(vm: *jni.JavaVM) *jni.JNIEnv {
        var env: *jni.JNIEnv = undefined;
        const jni_result = vm.get_env(&env, jni_version);
        if (jni_result != .ok) {
            const message = "Unexpected result calling JavaVM.GetEnv";
            log.err(
                message ++ "; Error = {} ({s})",
                .{ @intFromEnum(jni_result), @tagName(jni_result) },
            );
            @panic("JNI: " ++ message);
        }

        return env;
    }

    pub inline fn attach_current_thread(jvm: *jni.JavaVM) *jni.JNIEnv {
        var env: *jni.JNIEnv = undefined;
        const jni_result = jvm.attach_current_thread_as_daemon(&env, null);
        if (jni_result != .ok) {
            const message = "Unexpected result calling JavaVM.AttachCurrentThreadAsDaemon";
            log.err(
                message ++ "; Error = {} ({s})",
                .{ @intFromEnum(jni_result), @tagName(jni_result) },
            );
            @panic("JNI: " ++ message);
        }

        return env;
    }

    pub inline fn get_java_vm(env: *jni.JNIEnv) *jni.JavaVM {
        var jvm: *jni.JavaVM = undefined;
        const jni_result = env.get_java_vm(&jvm);
        check_jni_result(
            env,
            jni_result,
            "Unexpected result calling JNIEnv.GetJavaVM",
            .{},
        );

        return jvm;
    }

    pub inline fn vm_panic(
        env: *jni.JNIEnv,
        comptime fmt: []const u8,
        args: anytype,
    ) noreturn {
        env.exception_describe();
        log.err(fmt, args);

        var buf: [256]u8 = undefined;
        const message = std.fmt.bufPrintZ(&buf, fmt, args) catch |err| switch (err) {
            error.NoSpaceLeft => blk: {
                buf[255] = 0;
                break :blk @as([:0]const u8, @ptrCast(buf[0..255]));
            },
        };

        env.fatal_error(message.ptr);
    }

    pub inline fn check_jni_result(
        env: *jni.JNIEnv,
        jni_result: jni.JNIResultType,
        comptime fmt: []const u8,
        args: anytype,
    ) void {
        if (jni_result != .ok) {
            vm_panic(
                env,
                fmt ++ "; Error = {} ({s})",
                args ++ .{ @intFromEnum(jni_result), @tagName(jni_result) },
            );
        }
    }

    pub inline fn find_class(env: *jni.JNIEnv, comptime class_name: [:0]const u8) jni.JClass {
        var class_obj = env.find_class(class_name.ptr) orelse {
            vm_panic(
                env,
                "Unexpected result calling JNIEnv.FindClass for {s}",
                .{class_name},
            );
        };
        defer env.delete_local_ref(class_obj);

        return env.new_global_ref(class_obj) orelse {
            vm_panic(
                env,
                "Unexpected result calling JNIEnv.NewGlobalRef for {s}",
                .{class_name},
            );
        };
    }

    pub inline fn find_field(
        env: *jni.JNIEnv,
        class: jni.JClass,
        comptime name: [:0]const u8,
        comptime signature: [:0]const u8,
    ) jni.JFieldID {
        return env.get_field_id(class, name.ptr, signature.ptr) orelse
            vm_panic(
            env,
            "Field could not be found {s} {s}",
            .{ name, signature },
        );
    }

    pub inline fn find_method(
        env: *jni.JNIEnv,
        class: jni.JClass,
        comptime name: [:0]const u8,
        comptime signature: [:0]const u8,
    ) jni.JMethodID {
        return env.get_method_id(class, name.ptr, signature.ptr) orelse
            vm_panic(
            env,
            "Method could not be found {s} {s}",
            .{ name, signature },
        );
    }

    pub inline fn get_direct_buffer(
        env: *jni.JNIEnv,
        buffer_obj: jni.JObject,
    ) ?[]u8 {
        var buffer_capacity = env.get_direct_buffer_capacity(buffer_obj);
        if (buffer_capacity < 0) return null;

        var buffer_address = env.get_direct_buffer_address(buffer_obj) orelse return null;
        return buffer_address[0..@as(u32, @intCast(buffer_capacity))];
    }

    pub inline fn new_global_reference(env: *jni.JNIEnv, obj: jni.JObject) jni.JObject {
        return env.new_global_ref(obj) orelse {
            // NewGlobalRef fails only when the JVM runs out of memory.
            JNIHelper.vm_panic(env, "Unexpected result calling JNIEnv.NewGlobalRef", .{});
        };
    }

    pub inline fn get_string_utf(env: *jni.JNIEnv, string: jni.JString) ?[:0]const u8 {
        if (string == null) return null;

        const address = env.get_string_utf_chars(string, null) orelse return null;
        const length = env.get_string_utf_length(string);
        if (length < 0) return null;

        return @ptrCast(address[0..@as(usize, @intCast(length))]);
    }
};
