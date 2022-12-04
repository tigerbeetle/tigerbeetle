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
    var request_buffer_field_id: jui.jfieldID = null;
    var request_buffer_len_field_id: jui.jfieldID = null;
    var request_operation_method_id: jui.jmethodID = null;
    var request_end_request_method_id: jui.jmethodID = null;
    var request_release_permit_method_id: jui.jmethodID = null;

    pub fn load(env: *jui.JNIEnv) !void {

        // Asserting we are not initialized yet:
        assert(initialization_exception_class == null);
        assert(initialization_exception_ctor_id == null);
        assert(request_class == null);
        assert(request_buffer_field_id == null);
        assert(request_buffer_len_field_id == null);
        assert(request_operation_method_id == null);
        assert(request_end_request_method_id == null);
        assert(request_release_permit_method_id == null);

        initialization_exception_class = find_class(env, "com/tigerbeetle/InitializationException");
        initialization_exception_ctor_id = try env.getMethodId(initialization_exception_class, "<init>", "(I)V");

        request_class = find_class(env, "com/tigerbeetle/Request");
        request_buffer_field_id = try env.getFieldId(request_class, "buffer", "Ljava/nio/ByteBuffer;");
        request_buffer_len_field_id = try env.getFieldId(request_class, "bufferLen", "J");
        request_operation_method_id = try env.getMethodId(request_class, "getOperation", "()B");
        request_end_request_method_id = try env.getMethodId(request_class, "endRequest", "(BLjava/nio/ByteBuffer;JB)V");
        request_release_permit_method_id = try env.getMethodId(request_class, "releasePermit", "()V");

        // Asserting we are full initialized:
        assert(initialization_exception_class != null);
        assert(initialization_exception_ctor_id != null);
        assert(request_class != null);
        assert(request_buffer_field_id != null);
        assert(request_buffer_len_field_id != null);
        assert(request_operation_method_id != null);
        assert(request_end_request_method_id != null);
        assert(request_release_permit_method_id != null);
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
        request_buffer_field_id = null;
        request_buffer_len_field_id = null;
        request_operation_method_id = null;
        request_end_request_method_id = null;
        request_release_permit_method_id = null;
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

    pub fn buffer(env: *jui.JNIEnv, this_obj: jui.jobject) ?[]u8 {
        assert(this_obj != null);
        assert(request_buffer_field_id != null);
        assert(request_buffer_len_field_id != null);

        var buffer_obj = env.getField(.object, this_obj, request_buffer_field_id) orelse return null;
        defer env.deleteReference(.local, buffer_obj);

        var buffer_len = env.getField(.long, this_obj, request_buffer_len_field_id);
        var address = env.getDirectBufferAddress(buffer_obj);

        // The buffer can be larger than the actual message content,
        return address[0..@intCast(usize, buffer_len)];
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

    pub fn end_request(env: *jui.JNIEnv, this_obj: jui.jobject, result: ?[]const u8, packet: *tb.tb_packet_t) void {
        assert(this_obj != null);
        assert(request_class != null);
        assert(request_end_request_method_id != null);

        var buffer_obj: jui.jobject = if (result) |value| blk: {
            // force casting from []const to *anyopaque
            // it is ok here, since the ByteBuffer is always used as readonly here
            var erased = @intToPtr(*anyopaque, @ptrToInt(value.ptr));
            var local_ref = env.newDirectByteBuffer(erased, value.len) catch {
                // Cannot allocate a ByteBuffer, it's likely the JVM has run out of resources
                // Printing the buffer size here just to help diagnosing how much memory was required
                env.describeException();
                log.err("Unexpected error allocating a new direct ByteBuffer len={}", .{value.len});
                @panic("JNI: Unexpected error allocating a new direct ByteBuffer");
            };

            assert(local_ref != null);
            break :blk local_ref;
        } else null;
        defer if (buffer_obj != null) env.deleteReference(.local, buffer_obj);

        env.callNonVirtualMethod(
            .@"void",
            this_obj,
            request_class,
            request_end_request_method_id,
            &[_]jui.jvalue{
                jui.jvalue.toJValue(@bitCast(jui.jbyte, packet.operation)),
                jui.jvalue.toJValue(buffer_obj),
                jui.jvalue.toJValue(@bitCast(jui.jlong, @ptrToInt(packet))),
                jui.jvalue.toJValue(@bitCast(jui.jbyte, packet.status)),
            },
        ) catch {
            // The "endRequest" method isn't expected to throw any exception,
            // We can't rethrow here, since this function is called from the native callback.
            env.describeException();
            @panic("JNI: Unexpected error calling endRequest method");
        };
    }

    pub fn release_permit(env: *jui.JNIEnv, this_obj: jui.jobject) void {
        assert(this_obj != null);
        assert(request_class != null);
        assert(request_release_permit_method_id != null);

        env.callNonVirtualMethod(
            .void,
            this_obj,
            request_class,
            request_release_permit_method_id,
            null,
        ) catch {
            // The "releasePermit" method isn't expected to throw any exception,
            // We can't rethrow here, since this function is called from the native callback.
            env.describeException();
            @panic("JNI: Unexpected error calling releasePermit method");
        };
    }
};

/// JNI context for a client instance.
const JNIContext = struct {
    const Self = @This();
    const Atomic = std.atomic.Atomic;

    jvm: *jui.JavaVM,
    client: tb.tb_client_t,
    packets: Atomic(?*tb.tb_packet_t),

    pub fn release_packet(self: *Self, packet: *tb.tb_packet_t) void {
        var head = self.packets.load(.Monotonic);
        while (true) {
            packet.next = head;
            head = self.packets.tryCompareAndSwap(
                head,
                packet,
                .Release,
                .Monotonic,
            ) orelse break;
        }
    }

    pub fn acquire_packet(self: *Self) ?*tb.tb_packet_t {
        var head = self.packets.load(.Monotonic);
        while (true) {
            var next = (head orelse return null).next;
            head = self.packets.tryCompareAndSwap(
                head,
                next,
                .Release,
                .Monotonic,
            ) orelse {
                head.?.next = null;
                return head;
            };
        }
    }
};

/// NativeClient implementation.
const NativeClient = struct {

    /// On JVM loads this library.
    fn on_load(vm: *jui.JavaVM) !jui.jint {
        var env = try vm.getEnv(jni_version);
        try ReflectionHelper.load(env);

        return @bitCast(jui.jint, jni_version);
    }

    /// On JVM unloads this library.
    fn on_unload(vm: *jui.JavaVM) !void {
        var env = try vm.getEnv(jni_version);
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

        var out_client: tb.tb_client_t = undefined;
        var out_packets: tb.tb_packet_list_t = undefined;

        var addresses_return = jui.JNIEnv.getStringUTFChars(env, addresses_obj) catch {
            ReflectionHelper.throwInitializationException(env, tb.tb_status_t.address_invalid);
            return undefined;
        };
        const addresses_len = @intCast(usize, env.getStringUTFLength(addresses_obj));
        var addresses_chars = std.meta.assumeSentinel(addresses_return.chars[0..addresses_len], 0);
        defer env.releaseStringUTFChars(addresses_obj, addresses_return.chars);

        var context = global_allocator.create(JNIContext) catch {
            ReflectionHelper.throwInitializationException(env, tb.tb_status_t.out_of_memory);
            return undefined;
        };
        errdefer global_allocator.destroy(context);

        const init_fn = if (echo_client) tb.tb_client_init_echo else tb.tb_client_init;
        var status = init_fn(
            &out_client,
            &out_packets,
            cluster_id,
            addresses_chars,
            @intCast(u32, addresses_len),
            max_concurrency,
            @ptrToInt(context),
            on_completion,
        );

        if (status == .success) {
            var jvm = env.getJavaVM() catch |err| {
                log.err("Unexpected JNI failure retrieving the JVM {}", .{err});
                @panic("JNI: Unexpected JNI failure retrieving the JVM");
            };

            context.* = .{
                .jvm = jvm,
                .client = out_client,
                .packets = std.atomic.Atomic(?*tb.tb_packet_t).init(out_packets.head),
            };
            return context;
        } else {
            ReflectionHelper.throwInitializationException(env, status);
            return undefined;
        }
    }

    /// JNI clientDeinit implementation.
    fn client_deinit(context: *JNIContext) void {
        defer global_allocator.destroy(context);
        tb.tb_client_deinit(context.client);
    }

    /// JNI submit implementation.
    fn submit(
        env: *jui.JNIEnv,
        context: *JNIContext,
        request_obj: jui.jobject,
    ) void {
        assert(request_obj != null);

        // Holds a global reference to prevent GC during the callback.
        var global_ref = env.newReference(.global, request_obj) catch {
            // NewGlobalRef fails only when the JVM runs out of memory.
            @panic("JNI: Unexpected error creating a global reference");
        };

        assert(global_ref != null);
        errdefer env.deleteReference(.global, global_ref);

        var buffer = ReflectionHelper.buffer(env, request_obj) orelse {
            // It is unexpected to the buffer be null here
            // The java side must allocate a new buffer prior to invoking "submit".
            @panic("JNI: Request buffer cannot be null");
        };

        var packet = context.acquire_packet() orelse {
            // It is unexpected to not have any packet available here.
            // The java side syncronize how many threads can access.
            @panic("JNI: No available packets");
        };
        packet.operation = ReflectionHelper.operation(env, request_obj);
        packet.user_data = global_ref;
        packet.data = buffer.ptr;
        packet.data_size = @intCast(u32, buffer.len);
        packet.next = null;
        packet.status = .ok;

        var packet_list = tb.tb_packet_list_t.from(packet);
        tb.tb_client_submit(context.client, &packet_list);
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

        var result: ?[]const u8 = switch (packet.status) {
            .ok => if (result_ptr) |ptr| ptr[0..@intCast(usize, result_len)] else null,
            else => null,
        };

        defer {
            context.release_packet(packet);
            ReflectionHelper.release_permit(env, request_obj);
        }

        ReflectionHelper.end_request(env, request_obj, result, packet);
    }
};

/// Export function using the JNI calling convention.
const Exports = struct {
    pub fn on_load(vm: *jui.JavaVM) callconv(.C) jui.jint {
        return jui.wrapErrors(NativeClient.on_load, .{vm});
    }

    pub fn on_unload(vm: *jui.JavaVM) callconv(.C) void {
        jui.wrapErrors(NativeClient.on_unload, .{vm});
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
    ) callconv(.C) void {
        _ = class;
        assert(context_handle != 0);
        NativeClient.submit(
            env,
            @intToPtr(*JNIContext, @bitCast(usize, context_handle)),
            request_obj,
        );
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
