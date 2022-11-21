///! Java Native Interfaces for TigerBeetle Client
///! Please refer to the JNI Best Practices Guide
///! https://developer.ibm.com/articles/j-jni/
const std = @import("std");
const builtin = @import("builtin");
const jui = @import("jui");
const tb = @import("tigerbeetle");

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

/// Reflection helper and cache for the com.tigerbeetle.Client class
const ClientReflection = struct {
    var context_handle_field_id: jui.jfieldID = null;

    pub fn load(env: *jui.JNIEnv) !void {

        // Asserting we are not initialized yet
        assert(context_handle_field_id == null);

        var class_obj = try env.findClass("com/tigerbeetle/Client");
        assert(class_obj != null);
        defer env.deleteReference(.local, class_obj);

        context_handle_field_id = try env.getFieldId(class_obj, "contextHandle", "J");

        // Asserting we are full initialized
        assert(context_handle_field_id != null);
    }

    pub fn unload() void {
        context_handle_field_id = null;
    }

    pub inline fn set_context(env: *jui.JNIEnv, this_obj: jui.jobject, context: *const JNIContext) void {
        assert(this_obj != null);
        assert(context_handle_field_id != null);

        env.setField(.long, this_obj, context_handle_field_id, @bitCast(jui.jlong, @ptrToInt(context)));
    }
};

/// Reflection helper and cache for the com.tigerbeetle.Request class
const RequestReflection = struct {
    var class: jui.jclass = null;
    var request_buffer_field_id: jui.jfieldID = null;
    var request_buffer_len_field_id: jui.jfieldID = null;
    var request_operation_field_id: jui.jfieldID = null;
    var request_end_request_method_id: jui.jmethodID = null;
    var request_release_permit_method_id: jui.jmethodID = null;

    pub fn load(env: *jui.JNIEnv) !void {

        // Asserting we are not initialized yet
        assert(request_buffer_field_id == null);
        assert(request_buffer_len_field_id == null);
        assert(request_operation_field_id == null);
        assert(request_end_request_method_id == null);

        class = blk: {
            var class_obj = try env.findClass("com/tigerbeetle/Request");
            assert(class_obj != null);
            defer env.deleteReference(.local, class_obj);

            break :blk env.newReference(.global, class_obj) catch {
                // NewGlobalRef fails only when the JVM runs out of memory
                @panic("JNI: Error creating a global reference");
            };
        };

        request_buffer_field_id = try env.getFieldId(class, "buffer", "Ljava/nio/ByteBuffer;");
        request_buffer_len_field_id = try env.getFieldId(class, "bufferLen", "J");
        request_operation_field_id = try env.getFieldId(class, "operation", "B");
        request_end_request_method_id = try env.getMethodId(class, "endRequest", "(BLjava/nio/ByteBuffer;JB)V");
        request_release_permit_method_id = try env.getMethodId(class, "releasePermit", "()V");

        // Asserting we are full initialized
        assert(class != null);
        assert(request_buffer_field_id != null);
        assert(request_buffer_len_field_id != null);
        assert(request_operation_field_id != null);
        assert(request_end_request_method_id != null);
        assert(request_release_permit_method_id != null);
    }

    pub fn unload(env: *jui.JNIEnv) void {
        env.deleteReference(.global, class);

        class = null;
        request_buffer_field_id = null;
        request_buffer_len_field_id = null;
        request_operation_field_id = null;
        request_end_request_method_id = null;
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
        assert(request_operation_field_id != null);

        return @bitCast(u8, env.getField(.byte, this_obj, request_operation_field_id));
    }

    pub fn end_request(env: *jui.JNIEnv, this_obj: jui.jobject, result: ?[]const u8, packet: *tb.tb_packet_t) void {
        assert(this_obj != null);
        assert(request_end_request_method_id != null);

        var buffer_obj: jui.jobject = if (result) |value| blk: {
            // force casting from []const to *anyopaque
            // it is ok here, since the ByteBuffer is always used as readonly here
            var erased = @intToPtr(*anyopaque, @ptrToInt(value.ptr));
            var local_ref = env.newDirectByteBuffer(erased, value.len) catch {
                // Cannot allocate a ByteBuffer, it's likely the JVM has run out of resources
                // Printing the buffer size here just to help diagnosing how much memory was required
                env.describeException();
                log.err("Error allocating a new direct ByteBuffer len={}", .{value.len});
                @panic("JNI: Error allocating a new direct ByteBuffer");
            };

            assert(local_ref != null);
            break :blk local_ref;
        } else null;
        defer if (buffer_obj != null) env.deleteReference(.local, buffer_obj);

        env.callNonVirtualMethod(
            .@"void",
            this_obj,
            class,
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
            @panic("JNI: Error calling endRequest method");
        };
    }

    pub fn release_permit(env: *jui.JNIEnv, this_obj: jui.jobject) void {
        assert(this_obj != null);
        assert(request_release_permit_method_id != null);

        env.callNonVirtualMethod(.void, this_obj, class, request_release_permit_method_id, null,) catch {
            // The "releasePermit" method isn't expected to throw any exception,
            // We can't rethrow here, since this function is called from the native callback.
            env.describeException();
            @panic("JNI: Error calling releasePermit method");
        };
    }    
};

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

/// Native implementation
const JNIClient = struct {

    /// On JVM loads this library
    fn on_load(vm: *jui.JavaVM) !jui.jint {
        var env = try vm.getEnv(jni_version);

        try ClientReflection.load(env);
        try RequestReflection.load(env);

        return @bitCast(jui.jint, jni_version);
    }

    /// On JVM unloads this library
    fn on_unload(vm: *jui.JavaVM) !void {
        var env = try vm.getEnv(jni_version);

        ClientReflection.unload();
        RequestReflection.unload(env);
    }

    /// JNI Client.clientInit native implementation
    fn client_init(
        env: *jui.JNIEnv,
        this_obj: jui.jobject,
        cluster_id: u32,
        addresses_obj: jui.jstring,
        max_concurrency: u32,
    ) !tb.tb_status_t {
        assert(this_obj != null);
        assert(addresses_obj != null);

        var out_client: tb.tb_client_t = undefined;
        var out_packets: tb.tb_packet_list_t = undefined;

        const addresses_len = @intCast(usize, env.getStringUTFLength(addresses_obj));
        var addresses_return = try jui.JNIEnv.getStringUTFChars(env, addresses_obj);
        var addresses_chars = std.meta.assumeSentinel(addresses_return.chars[0..addresses_len], 0);
        defer env.releaseStringUTFChars(addresses_obj, addresses_chars);

        var context = try global_allocator.create(JNIContext);
        errdefer global_allocator.destroy(context);

        var status = tb.tb_client_init(
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
            context.* = .{
                .jvm = try env.getJavaVM(),
                .client = out_client,
                .packets = std.atomic.Atomic(?*tb.tb_packet_t).init(out_packets.head)
            };

            ClientReflection.set_context(env, this_obj, context);
        }

        return status;
    }

    /// JNI Client.clientDeinit native implementation
    fn client_deinit(context: *JNIContext) void { 
        defer global_allocator.destroy(context);
        tb.tb_client_deinit(context.client);
    }

    /// JNI Client.submit native implementation
    fn submit(
        env: *jui.JNIEnv,
        context: *JNIContext,
        request_obj: jui.jobject,
    ) void {
        assert(request_obj != null);

        // Holds a global reference to prevent GC during the callback
        var global_ref = env.newReference(.global, request_obj) catch {
            // NewGlobalRef fails only when the JVM runs out of memory
            @panic("JNI: Error creating a global reference");
        };

        assert(global_ref != null);
        errdefer env.deleteReference(.global, global_ref);

        var buffer = RequestReflection.buffer(env, request_obj) orelse {
            // It is unexpected to the buffer be null here
            // The java side must allocate a new buffer prior to invoking "submit".
            @panic("JNI: Request buffer cannot be null");
        };

        var packet = context.acquire_packet() orelse {
            // It is unexpected to not have any packet available here.
            // The java side syncronize how many threads can access.
            @panic("JNI: No available packets");            
        };
        packet.operation = RequestReflection.operation(env, request_obj);
        packet.user_data = global_ref;
        packet.data = buffer.ptr;
        packet.data_size = @intCast(u32, buffer.len);
        packet.next = null;
        packet.status = .ok;

        var packet_list = tb.tb_packet_list_t.from(packet);
        tb.tb_client_submit(context.client, &packet_list);
    }

    /// Completion callback
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
            log.err("Error {s} attaching the native thread as daemon", .{@errorName(err)});
            @panic("JNI: Error attaching the native thread as daemon");
        };

        // Retrieves the request instance, and drops the GC reference
        assert(packet.user_data != null);
        var request_obj = @ptrCast(jui.jobject, packet.user_data);
        defer env.deleteReference(.global, request_obj);

        var result: ?[]const u8 = switch (packet.status) {
            .ok => if (result_ptr) |ptr| ptr[0..@intCast(usize, result_len)] else null,
            else => null,
        };

        defer {
            context.release_packet(packet);
            RequestReflection.release_permit(env, request_obj);
        }

        RequestReflection.end_request(env, request_obj, result, packet);
    }
};

/// Export function using the JNI calling convention
const Exports = struct {
    pub fn on_load_export(vm: *jui.JavaVM) callconv(.C) jui.jint {
        return jui.wrapErrors(JNIClient.on_load, .{vm});
    }

    pub fn on_unload_export(vm: *jui.JavaVM) callconv(.C) void {
        jui.wrapErrors(JNIClient.on_unload, .{vm});
    }

    pub fn client_init_export(
        env: *jui.JNIEnv,
        this: jui.jobject,
        cluster_id: jui.jint,
        addresses: jui.jstring,
        max_concurrency: jui.jint,
    ) callconv(.C) jui.jint {
        var status = jui.wrapErrors(
            JNIClient.client_init,
            .{
                env,
                this,
                @bitCast(u32, cluster_id),
                addresses,
                @bitCast(u32, max_concurrency),
            },
        );

        return @bitCast(jui.jint, status);
    }

    pub fn client_deinit_export(env: *jui.JNIEnv, this: jui.jobject, context_handle: jui.jlong) callconv(.C) void {
        _ = env;
        _ = this;
        JNIClient.client_deinit(@intToPtr(*JNIContext, @bitCast(usize, context_handle)));
    }

    pub fn submit_export(env: *jui.JNIEnv, this_obj: jui.jobject, context_handle: jui.jlong, request_obj: jui.jobject) callconv(.C) void {
        _ = this_obj;
        assert(context_handle != 0);

        JNIClient.submit(
            env,
            @intToPtr(*JNIContext, @bitCast(usize, context_handle)),
            request_obj,
        );
    }
};

comptime {
    jui.exportUnder("com.tigerbeetle.Client", .{
        .onLoad = Exports.on_load_export,
        .onUnload = Exports.on_unload_export,
        .clientInit = Exports.client_init_export,
        .clientDeinit = Exports.client_deinit_export,
        .submit = Exports.submit_export,
    });
}

test {
    // Most of these functions have asserts that ensure the correct
    // behavior when called from tests implemented on the Java side.

    // Although it would be interesting to implement tests from here in order to
    // check the correctness of things such as reflection and exceptions,
    // it will require the test to host an in-process JVM
    // using the JNI Invocation API
    // https://docs.oracle.com/javase/7/docs/technotes/guides/jni/spec/invocation.html
}
