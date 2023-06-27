///! Java Native Interfaces for TigerBeetle Client
///! Please refer to the JNI Best Practices Guide:
///! https://developer.ibm.com/articles/j-jni/
const std = @import("std");
const builtin = @import("builtin");
const jui = @import("jui");
const tb = @import("../../c/tb_client.zig");

const log = std.log.scoped(.tb_client_jni);

const assert = std.debug.assert;
const jni_version = jui.JNIVersion{ .major = 10, .minor = 0 };

const global_allocator = if (builtin.link_libc)
    std.heap.c_allocator
else if (builtin.target.os.tag == .windows)
    (struct {
        var gpa = std.heap.HeapAllocator.init();
    }).gpa.allocator()
else
    @compileError("tb_client must be built with libc");

/// Reflection helper and cache.
const ReflectionHelper = struct {
    var initialization_exception_class: jui.jclass = null;
    var initialization_exception_ctor_id: jui.jmethodID = null;

    var request_class: jui.jclass = null;
    var request_send_buffer_field_id: jui.jfieldID = null;
    var request_send_buffer_len_field_id: jui.jfieldID = null;
    var request_reply_buffer_field_id: jui.jfieldID = null;
    var request_operation_method_id: jui.jmethodID = null;
    var request_end_request_method_id: jui.jmethodID = null;

    pub fn load(env: *jui.JNIEnv) !void {

        // Asserting we are not initialized yet:
        assert(initialization_exception_class == null);
        assert(initialization_exception_ctor_id == null);
        assert(request_class == null);
        assert(request_send_buffer_field_id == null);
        assert(request_send_buffer_len_field_id == null);
        assert(request_reply_buffer_field_id == null);
        assert(request_operation_method_id == null);
        assert(request_end_request_method_id == null);

        initialization_exception_class = find_class(env, "com/tigerbeetle/InitializationException");
        initialization_exception_ctor_id = try env.getMethodId(initialization_exception_class, "<init>", "(I)V");

        request_class = find_class(env, "com/tigerbeetle/Request");
        request_send_buffer_field_id = try env.getFieldId(request_class, "sendBuffer", "Ljava/nio/ByteBuffer;");
        request_send_buffer_len_field_id = try env.getFieldId(request_class, "sendBufferLen", "J");
        request_reply_buffer_field_id = try env.getFieldId(request_class, "replyBuffer", "[B");
        request_operation_method_id = try env.getMethodId(request_class, "getOperation", "()B");
        request_end_request_method_id = try env.getMethodId(request_class, "endRequest", "(BB)V");

        // Asserting we are full initialized:
        assert(initialization_exception_class != null);
        assert(initialization_exception_ctor_id != null);
        assert(request_class != null);
        assert(request_send_buffer_field_id != null);
        assert(request_send_buffer_len_field_id != null);
        assert(request_reply_buffer_field_id != null);
        assert(request_operation_method_id != null);
        assert(request_end_request_method_id != null);
    }

    inline fn find_class(env: *jui.JNIEnv, comptime class_name: [:0]const u8) jui.jclass {
        var class_obj = env.findClass(class_name) catch |err| {
            log.err("Unexpected error loading " ++ class_name ++ " {}", .{err});
            @panic("JNI: Unexpected error loading " ++ class_name);
        } orelse {
            @panic("JNI: Class " ++ class_name ++ " not found");
        };
        defer env.deleteReference(.local, class_obj);

        return env.newReference(.global, class_obj) catch {
            @panic("JNI: Unexpected error creating a global reference for " ++ class_name);
        };
    }

    pub fn unload(env: *jui.JNIEnv) void {
        env.deleteReference(.global, initialization_exception_class);
        env.deleteReference(.global, request_class);

        initialization_exception_class = null;
        initialization_exception_ctor_id = null;
        request_class = null;
        request_send_buffer_field_id = null;
        request_send_buffer_len_field_id = null;
        request_reply_buffer_field_id = null;
        request_operation_method_id = null;
        request_end_request_method_id = null;
    }

    pub fn throwInitializationException(env: *jui.JNIEnv, status: tb.tb_status_t) void {
        assert(initialization_exception_class != null);
        assert(initialization_exception_ctor_id != null);

        var exception = env.newObject(
            initialization_exception_class,
            initialization_exception_ctor_id,
            &[_]jui.jvalue{jui.jvalue.toJValue(@bitCast(jui.jint, @enumToInt(status)))},
        ) catch {
            // It's unexpected here: we did not initialize correctly or the JVM is out of memory.
            @panic("JNI: Unexpected error creating a new exception.");
        };

        assert(exception != null);
        defer env.deleteReference(.local, exception);

        env.throw(exception) catch {
            @panic("JNI: Unexpected error throwing an exception.");
        };
        assert(env.hasPendingException());
    }

    pub fn get_send_buffer(env: *jui.JNIEnv, this_obj: jui.jobject) ?[]u8 {
        assert(this_obj != null);
        assert(request_send_buffer_field_id != null);
        assert(request_send_buffer_len_field_id != null);

        var buffer_obj = env.getField(.object, this_obj, request_send_buffer_field_id) orelse return null;
        defer env.deleteReference(.local, buffer_obj);

        var buffer_len = env.getField(.long, this_obj, request_send_buffer_len_field_id);
        var address = env.getDirectBufferAddress(buffer_obj);

        // The buffer can be larger than the actual message content,
        return address[0..@intCast(usize, buffer_len)];
    }

    pub fn set_reply_buffer(env: *jui.JNIEnv, this_obj: jui.jobject, reply: []const u8) void {
        assert(this_obj != null);
        assert(request_reply_buffer_field_id != null);
        assert(reply.len > 0);

        var reply_buffer_obj = env.newPrimitiveArray(
            .byte,
            @intCast(c_int, reply.len),
        ) catch {
            // Cannot allocate an array, it's likely the JVM has run out of resources
            // Printing the buffer size here just to help diagnosing how much memory was required
            env.describeException();
            log.err("Unexpected error allocating a new array len={}", .{reply.len});
            @panic("JNI: Unexpected error creating a new array.");
        };
        defer env.deleteReference(.local, reply_buffer_obj);

        env.setPrimitiveArrayRegion(
            .byte,
            reply_buffer_obj,
            0,
            @intCast(c_int, reply.len),
            @ptrCast([*]const jui.jbyte, reply.ptr),
        ) catch {
            // Since out-of-bounds isn't expected here, we can only panic if it fails.
            env.describeException();
            @panic("JNI: Unexpected error copying array.");
        };

        // Setting the request with the reply.
        env.setField(
            .object,
            this_obj,
            request_reply_buffer_field_id,
            reply_buffer_obj,
        );
    }

    pub fn operation(env: *jui.JNIEnv, this_obj: jui.jobject) u8 {
        assert(this_obj != null);
        assert(request_class != null);
        assert(request_operation_method_id != null);

        const value = env.callNonVirtualMethod(
            .byte,
            this_obj,
            request_class,
            request_operation_method_id,
            null,
        ) catch {
            // The "getOperation" method isn't expected to throw any exception,
            env.describeException();
            @panic("JNI: Unexpected error calling getOperation method");
        };

        return @bitCast(u8, value);
    }

    pub fn end_request(
        env: *jui.JNIEnv,
        this_obj: jui.jobject,
        packet_operation: u8,
        packet_status: tb.tb_packet_status_t,
    ) void {
        assert(this_obj != null);
        assert(request_class != null);
        assert(request_end_request_method_id != null);

        env.callNonVirtualMethod(
            .void,
            this_obj,
            request_class,
            request_end_request_method_id,
            &[_]jui.jvalue{
                jui.jvalue.toJValue(@bitCast(jui.jbyte, packet_operation)),
                jui.jvalue.toJValue(@bitCast(jui.jbyte, @enumToInt(packet_status))),
            },
        ) catch {
            // The "endRequest" method isn't expected to throw any exception,
            // We can't rethrow here, since this function is called from the native callback.
            env.describeException();
            @panic("JNI: Unexpected error calling endRequest method");
        };
    }
};

/// JNI context for a client instance.
const JNIContext = struct {
    const Self = @This();

    jvm: *jui.JavaVM,
    client: tb.tb_client_t,

    pub inline fn release_packet(self: *Self, packet: *tb.tb_packet_t) void {
        tb.release_packet(self.client, packet);
    }

    pub inline fn acquire_packet(
        self: *Self,
        out_packet: *?*tb.tb_packet_t,
    ) tb.tb_packet_acquire_status_t {
        return tb.acquire_packet(self.client, out_packet);
    }
};

/// NativeClient implementation.
const NativeClient = struct {
    /// On JVM loads this library.
    fn on_load(vm: *jui.JavaVM) jui.jint {
        var env = vm.getEnv(jni_version) catch |err| {
            log.err("Unexpected JNI failure calling JavaVM.getEnv {}", .{err});
            @panic("JNI: Unexpected JNI failure calling JavaVM.getEnv");
        };

        ReflectionHelper.load(env) catch |err| {
            env.describeException();
            log.err("Unexpected JNI failure loading the native module {}", .{err});
            @panic("JNI: Unexpected JNI failure loading the native module");
        };

        return @bitCast(jui.jint, jni_version);
    }

    /// On JVM unloads this library.
    fn on_unload(vm: *jui.JavaVM) void {
        var env = vm.getEnv(jni_version) catch |err| {
            log.err("Unexpected JNI failure calling JavaVM.getEnv {}", .{err});
            @panic("JNI: Unexpected JNI failure calling JavaVM.getEnv");
        };

        ReflectionHelper.unload(env);
    }

    /// JNI clientInit and clientInitEcho implementation.
    fn client_init(
        comptime echo_client: bool,
        env: *jui.JNIEnv,
        cluster_id: u32,
        addresses_obj: jui.jstring,
        max_concurrency: u32,
    ) *JNIContext {
        assert(addresses_obj != null);

        var addresses_return = jui.JNIEnv.getStringUTFChars(env, addresses_obj) catch {
            ReflectionHelper.throwInitializationException(env, tb.tb_status_t.address_invalid);
            return undefined;
        };
        const addresses_len = @intCast(usize, env.getStringUTFLength(addresses_obj));
        const addresses = addresses_return.chars[0..addresses_len];
        defer env.releaseStringUTFChars(addresses_obj, addresses_return.chars);

        var context = global_allocator.create(JNIContext) catch {
            ReflectionHelper.throwInitializationException(env, tb.tb_status_t.out_of_memory);
            return undefined;
        };
        errdefer global_allocator.destroy(context);

        const init_fn = if (echo_client) tb.init_echo else tb.init;
        const client = init_fn(
            global_allocator,
            cluster_id,
            addresses,
            max_concurrency,
            @ptrToInt(context),
            on_completion,
        ) catch |err| {
            const status = tb.init_error_to_status(err);
            ReflectionHelper.throwInitializationException(env, status);
            return undefined;
        };

        var jvm = env.getJavaVM() catch |err| {
            log.err("Unexpected JNI failure retrieving the JVM {}", .{err});
            @panic("JNI: Unexpected JNI failure retrieving the JVM");
        };

        context.* = .{
            .jvm = jvm,
            .client = client,
        };
        return context;
    }

    /// JNI clientDeinit implementation.
    fn client_deinit(context: *JNIContext) void {
        defer global_allocator.destroy(context);
        tb.deinit(context.client);
    }

    /// JNI submit implementation.
    fn submit(
        env: *jui.JNIEnv,
        context: *JNIContext,
        request_obj: jui.jobject,
    ) tb.tb_packet_acquire_status_t {
        assert(request_obj != null);

        // Holds a global reference to prevent GC during the callback.
        var global_ref = env.newReference(.global, request_obj) catch {
            // NewGlobalRef fails only when the JVM runs out of memory.
            @panic("JNI: Unexpected error creating a global reference");
        };

        assert(global_ref != null);
        errdefer env.deleteReference(.global, global_ref);

        var send_buffer = ReflectionHelper.get_send_buffer(env, request_obj) orelse {
            // It is unexpected to the buffer be null here
            // The java side must allocate a new buffer prior to invoking "submit".
            @panic("JNI: Request buffer cannot be null");
        };

        var out_packet: ?*tb.tb_packet_t = null;
        const acquire_status = context.acquire_packet(&out_packet);

        if (out_packet) |packet| {
            assert(acquire_status == .ok);
            packet.operation = ReflectionHelper.operation(env, request_obj);
            packet.user_data = global_ref;
            packet.data = send_buffer.ptr;
            packet.data_size = @intCast(u32, send_buffer.len);
            packet.next = null;
            packet.status = .ok;

            tb.submit(context.client, packet);
        } else {
            assert(acquire_status != .ok);
        }

        return acquire_status;
    }

    /// Completion callback.
    fn on_completion(
        context_ptr: usize,
        client: tb.tb_client_t,
        packet: *tb.tb_packet_t,
        result_ptr: ?[*]const u8,
        result_len: u32,
    ) callconv(.C) void {
        _ = client;

        var context = @intToPtr(*JNIContext, context_ptr);
        
        var env = context.jvm.attachCurrentThreadAsDaemon() catch |err| {
            log.err("Unexpected error attaching the native thread as daemon {}", .{err});
            @panic("JNI: Unexpected error attaching the native thread as daemon");
        };

        // Retrieves the request instance, and drops the GC reference.
        assert(packet.user_data != null);
        var request_obj = @ptrCast(jui.jobject, packet.user_data);
        defer env.deleteReference(.global, request_obj);

        const packet_operation = packet.operation;
        const packet_status = packet.status;
        if (result_len > 0) {
            switch (packet_status) {
                .ok => if (result_ptr) |ptr| {
                    // Copying the reply before releasing the packet.
                    ReflectionHelper.set_reply_buffer(
                        env,
                        request_obj,
                        ptr[0..@intCast(usize, result_len)],
                    );
                },
                else => {},
            }
        }
        context.release_packet(packet);

        ReflectionHelper.end_request(
            env,
            request_obj,
            packet_operation,
            packet_status,
        );
    }
};

/// Export function using the JNI calling convention.
const Exports = struct {
    pub fn on_load(vm: *jui.JavaVM) callconv(.C) jui.jint {
        return NativeClient.on_load(vm);
    }

    pub fn on_unload(vm: *jui.JavaVM) callconv(.C) void {
        NativeClient.on_unload(vm);
    }

    pub fn client_init(
        env: *jui.JNIEnv,
        class: jui.jclass,
        cluster_id: jui.jint,
        addresses: jui.jstring,
        max_concurrency: jui.jint,
    ) callconv(.C) jui.jlong {
        _ = class;
        var context = NativeClient.client_init(
            false,
            env,
            @bitCast(u32, cluster_id),
            addresses,
            @bitCast(u32, max_concurrency),
        );
        return @bitCast(jui.jlong, @ptrToInt(context));
    }

    pub fn client_init_echo(
        env: *jui.JNIEnv,
        class: jui.jclass,
        cluster_id: jui.jint,
        addresses: jui.jstring,
        max_concurrency: jui.jint,
    ) callconv(.C) jui.jlong {
        _ = class;
        var context = NativeClient.client_init(
            true,
            env,
            @bitCast(u32, cluster_id),
            addresses,
            @bitCast(u32, max_concurrency),
        );
        return @bitCast(jui.jlong, @ptrToInt(context));
    }

    pub fn client_deinit(
        env: *jui.JNIEnv,
        class: jui.jclass,
        context_handle: jui.jlong,
    ) callconv(.C) void {
        _ = env;
        _ = class;
        NativeClient.client_deinit(@intToPtr(*JNIContext, @bitCast(usize, context_handle)));
    }

    pub fn submit(
        env: *jui.JNIEnv,
        class: jui.jclass,
        context_handle: jui.jlong,
        request_obj: jui.jobject,
    ) callconv(.C) jui.jint {
        _ = class;
        assert(context_handle != 0);
        const packet_acquire_status = NativeClient.submit(
            env,
            @intToPtr(*JNIContext, @bitCast(usize, context_handle)),
            request_obj,
        );

        return @intCast(jui.jint, @enumToInt(packet_acquire_status));
    }
};

comptime {
    jui.exportUnder("com.tigerbeetle.NativeClient", .{
        .onLoad = Exports.on_load,
        .onUnload = Exports.on_unload,
        .clientInit = Exports.client_init,
        .clientInitEcho = Exports.client_init_echo,
        .clientDeinit = Exports.client_deinit,
        .submit = Exports.submit,
    });
}

test {
    // Most of these functions have asserts that ensure the correct
    // behavior when called from tests implemented on the Java side.

    // Although it would be interesting to implement tests from here in order to
    // check the correctness of things such as reflection and exceptions,
    // it will require the test to host an in-process JVM
    // using the JNI Invocation API:
    // https://docs.oracle.com/javase/7/docs/technotes/guides/jni/spec/invocation.html
}
