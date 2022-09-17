///! Java Native Interfaces for TigerBeetle Client
///! Please refer to the JNI Best Practices Guide
///! https://developer.ibm.com/articles/j-jni/
const std = @import("std");
const builtin = @import("builtin");
const jui = @import("jui");
const tb = @import("tigerbeetle");

const assert = std.debug.assert;
const jni_version = jui.JNIVersion{ .major = 10, .minor = 0 };

/// Reflection helper and cache for the com.tigerbeetle.Client class
const ClientReflection = struct {
    var client_handle_field_id: jui.jfieldID = null;
    var packets_head_field_id: jui.jfieldID = null;
    var packets_tail_field_id: jui.jfieldID = null;

    pub fn load(env: *jui.JNIEnv) !void {

        // Asserting we are not initialized yet
        assert(client_handle_field_id == null);
        assert(packets_head_field_id == null);
        assert(packets_tail_field_id == null);

        var class_obj = try env.findClass("com/tigerbeetle/Client");
        assert(class_obj != null);
        defer env.deleteReference(.local, class_obj);

        client_handle_field_id = try env.getFieldId(class_obj, "clientHandle", "J");
        packets_head_field_id = try env.getFieldId(class_obj, "packetsHead", "J");
        packets_tail_field_id = try env.getFieldId(class_obj, "packetsTail", "J");

        // Asserting we are full initialized
        assert(client_handle_field_id != null);
        assert(packets_head_field_id != null);
        assert(packets_tail_field_id != null);
    }

    pub fn unload() void {
        client_handle_field_id = null;
        packets_head_field_id = null;
        packets_tail_field_id = null;
    }

    pub inline fn set_tb_client(env: *jui.JNIEnv, this_obj: jui.jobject, tb_client: tb.tb_client_t) void {
        assert(this_obj != null);
        assert(client_handle_field_id != null);

        env.setField(.long, this_obj, client_handle_field_id, @bitCast(jui.jlong, @ptrToInt(tb_client)));
    }

    pub inline fn set_packet_list(env: *jui.JNIEnv, this_obj: jui.jobject, packet_list: *const tb.tb_packet_list_t) void {
        assert(this_obj != null);
        assert(packets_head_field_id != null);
        assert(packets_tail_field_id != null);

        env.setField(.long, this_obj, packets_head_field_id, @bitCast(jui.jlong, @ptrToInt(packet_list.head)));
        env.setField(.long, this_obj, packets_tail_field_id, @bitCast(jui.jlong, @ptrToInt(packet_list.tail)));
    }
};

/// Reflection helper and cache for the com.tigerbeetle.Request class
const RequestReflection = struct {
    var request_buffer_field_id: jui.jfieldID = null;
    var request_buffer_len_field_id: jui.jfieldID = null;
    var request_operation_field_id: jui.jfieldID = null;
    var request_end_request_method_id: jui.jmethodID = null;

    pub fn load(env: *jui.JNIEnv) !void {

        // Asserting we are not initialized yet
        assert(request_buffer_field_id == null);
        assert(request_buffer_len_field_id == null);
        assert(request_operation_field_id == null);
        assert(request_end_request_method_id == null);

        var class_obj = try env.findClass("com/tigerbeetle/Request");
        assert(class_obj != null);
        defer env.deleteReference(.local, class_obj);

        request_buffer_field_id = try env.getFieldId(class_obj, "buffer", "Ljava/nio/ByteBuffer;");
        request_buffer_len_field_id = try env.getFieldId(class_obj, "bufferLen", "J");
        request_operation_field_id = try env.getFieldId(class_obj, "operation", "B");
        request_end_request_method_id = try env.getMethodId(class_obj, "endRequest", "(BLjava/nio/ByteBuffer;JB)V");

        // Asserting we are full initialized
        assert(request_buffer_field_id != null);
        assert(request_buffer_len_field_id != null);
        assert(request_operation_field_id != null);
        assert(request_end_request_method_id != null);
    }

    pub fn unload() void {
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
                // We can't throw an exception here, since this function is called from the native callback.
                std.log.err("JNI: Error allocating a new direct ByteBuffer len={}", .{value.len});
                env.describeException();
                assert(false);
                unreachable;
            };

            assert(local_ref != null);
            break :blk local_ref;
        } else null;
        defer if (buffer_obj != null) env.deleteReference(.local, buffer_obj);

        env.callMethod(
            .@"void",
            this_obj,
            request_end_request_method_id,
            &[_]jui.jvalue{
                jui.jvalue.toJValue(@bitCast(jui.jbyte, packet.operation)),
                jui.jvalue.toJValue(buffer_obj),
                jui.jvalue.toJValue(@bitCast(jui.jlong, @ptrToInt(packet))),
                jui.jvalue.toJValue(@bitCast(jui.jbyte, packet.status)),
            },
        ) catch |err| {
            // The "endRequest" method isn't expected to throw any exception,
            // We can't rethrow here, since this function is called from the native callback.
            std.log.err("JNI: Error calling endRequest method {s}.", .{@errorName(err)});
            env.describeException();
            assert(false);
        };
    }
};

/// Reflection helper and cache for the com.tigerbeetle.AssertionError class
const AssertionErrorReflection = struct {
    var class_obj: jui.jclass = null;

    pub fn load(env: *jui.JNIEnv) !void {

        // Asserting we are not initialized yet
        assert(class_obj == null);

        class_obj = class: {
            var local_ref = try env.findClass("com/tigerbeetle/AssertionError");
            assert(local_ref != null);
            defer env.deleteReference(.local, local_ref);

            break :class try env.newReference(.global, local_ref);
        };

        assert(class_obj != null);
    }

    pub fn unload(env: *jui.JNIEnv) void {
        env.deleteReference(.global, class_obj);

        class_obj = null;
    }

    /// Throws a new com.tigerbeetle.AssertionError
    /// Note that we can only throw an exception when the caller is the Java side,
    /// never when a function is called from the native callback
    pub fn throw(env: *jui.JNIEnv, message: [:0]const u8) void {
        env.throwNew(class_obj, message) catch |err| {
            // This is an indication that something unrrecorveable hapened
            std.log.err("JNI: Error throwing a new AssertionError {s}", .{@errorName(err)});
            std.log.err("{s}", .{message});
            env.describeException();
            assert(false);
        };
    }
};

/// Native implementation
const JNIClient = struct {

    /// On JVM loads this library
    fn on_load(vm: *jui.JavaVM) !jui.jint {
        var env = try vm.getEnv(jni_version);

        try ClientReflection.load(env);
        try RequestReflection.load(env);
        try AssertionErrorReflection.load(env);

        return @bitCast(jui.jint, jni_version);
    }

    /// On JVM unloads this library
    fn on_unload(vm: *jui.JavaVM) !void {
        var env = try vm.getEnv(jni_version);
        ClientReflection.unload();
        RequestReflection.unload();
        AssertionErrorReflection.unload(env);
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

        var jvm = try env.getJavaVM();

        const addresses_len = @intCast(usize, env.getStringUTFLength(addresses_obj));
        var addresses_return = try jui.JNIEnv.getStringUTFChars(env, addresses_obj);
        var addresses_chars = std.meta.assumeSentinel(addresses_return.chars[0..addresses_len], 0);
        defer env.releaseStringUTFChars(addresses_obj, addresses_chars);

        var status = tb.tb_client_init(
            &out_client,
            &out_packets,
            cluster_id,
            addresses_chars,
            @intCast(u32, addresses_len),
            max_concurrency,
            @ptrToInt(jvm),
            on_completion,
        );

        if (status == .success) {
            ClientReflection.set_tb_client(env, this_obj, out_client);
            ClientReflection.set_packet_list(env, this_obj, &out_packets);
        }

        return status;
    }

    /// JNI Client.clientDeinit native implementation
    fn client_deinit(client: tb.tb_client_t) void {
        tb.tb_client_deinit(client);
    }

    /// JNI Client.submit native implementation
    fn submit(
        env: *jui.JNIEnv,
        request_obj: jui.jobject,
        client: tb.tb_client_t,
        packet: *tb.tb_packet_t,
    ) void {
        assert(request_obj != null);

        // Holds a global reference to prevent GC during the callback
        var global_ref = env.newReference(.global, request_obj) catch {
            // NewGlobalRef fails only when the JVM runs out of memory
            std.log.err("JNI: Error creating a global reference.", .{});
            assert(false);
            unreachable;
        };

        assert(global_ref != null);
        errdefer env.deleteReference(.global, global_ref);

        var buffer = RequestReflection.buffer(env, request_obj) orelse {
            // It is unexpeted to the buffer be null here
            // The java side must allocate a new buffer prior to invoking "submit".
            // This exception is going to be handled by the Java caller.
            AssertionErrorReflection.throw(env, "Request buffer cannot be null.");
            return;
        };

        packet.operation = RequestReflection.operation(env, request_obj);
        packet.user_data = @ptrToInt(global_ref);
        packet.data = buffer.ptr;
        packet.data_size = @intCast(u32, buffer.len);
        packet.next = null;
        packet.status = .ok;

        var packet_list = tb.tb_packet_list_t.from(packet);
        tb.tb_client_submit(client, &packet_list);
    }

    /// Completion callback
    fn on_completion(
        context: usize,
        client: tb.tb_client_t,
        packet: *tb.tb_packet_t,
        result_ptr: ?[*]const u8,
        result_len: u32,
    ) callconv(.C) void {
        _ = client;

        var jvm = @intToPtr(*jui.JavaVM, context);
        var env = jvm.attachCurrentThreadAsDaemon() catch |err| {
            // We can't throw an exception here, since this function is called from the native callback.
            std.log.err("JNI: Error attaching the native thread as daemon {s}.", .{@errorName(err)});
            assert(false);
            unreachable;
        };

        // Retrieves the request instance, and drops the GC reference
        assert(packet.user_data != 0);
        var request_obj = @intToPtr(jui.jobject, packet.user_data);
        defer env.deleteReference(.global, request_obj);

        var result: ?[]const u8 = switch (packet.status) {
            .ok => if (result_ptr) |ptr| ptr[0..@intCast(usize, result_len)] else null,
            else => null,
        };

        RequestReflection.end_request(env, request_obj, result, packet);
    }

    /// JNI Client.popPacket native implementation
    fn pop_packet(
        env: *jui.JNIEnv,
        client_obj: jui.jobject,
        packet_list: *tb.tb_packet_list_t,
    ) *tb.tb_packet_t {
        assert(client_obj != null);

        var packet = packet_list.pop() orelse {
            // It is unexpeted to packet_list be empty.
            // The java side must syncronize how many threads call this function.
            // This exception is going to be handled by the Java caller.
            // Returning undefined is ok here, since the return value will be discarded anyway.
            AssertionErrorReflection.throw(env, "Packet list cannot be empty.");
            return undefined;
        };

        ClientReflection.set_packet_list(env, client_obj, packet_list);
        return packet;
    }

    /// JNI Client.pushPacket native implementation
    fn push_packet(
        env: *jui.JNIEnv,
        client_obj: jui.jobject,
        packet_list: *tb.tb_packet_list_t,
        packet: *tb.tb_packet_t,
    ) void {
        packet_list.push(tb.tb_packet_list_t.from(packet));
        ClientReflection.set_packet_list(env, client_obj, packet_list);
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

    pub fn client_deinit_export(env: *jui.JNIEnv, this: jui.jobject, client_handle: jui.jlong) callconv(.C) void {
        _ = env;
        _ = this;
        JNIClient.client_deinit(@intToPtr(tb.tb_client_t, @bitCast(usize, client_handle)));
    }

    pub fn submit_export(env: *jui.JNIEnv, this_obj: jui.jobject, client_handle: jui.jlong, request_obj: jui.jobject, packet: jui.jlong) callconv(.C) void {
        _ = this_obj;
        assert(client_handle != 0);
        assert(packet != 0);

        JNIClient.submit(
            env,
            request_obj,
            @intToPtr(tb.tb_client_t, @bitCast(usize, client_handle)),
            @intToPtr(*tb.tb_packet_t, @bitCast(usize, packet)),
        );
    }

    pub fn pop_packet_export(env: *jui.JNIEnv, this_obj: jui.jobject, packets_head: jui.jlong, packets_tail: jui.jlong) callconv(.C) jui.jlong {
        var packet_list = tb.tb_packet_list_t{
            .head = @intToPtr(?*tb.tb_packet_t, @bitCast(usize, packets_head)),
            .tail = @intToPtr(?*tb.tb_packet_t, @bitCast(usize, packets_tail)),
        };

        var packet = JNIClient.pop_packet(
            env,
            this_obj,
            &packet_list,
        );
        return @bitCast(jui.jlong, @ptrToInt(packet));
    }

    pub fn push_packet_export(env: *jui.JNIEnv, this_obj: jui.jobject, packets_head: jui.jlong, packets_tail: jui.jlong, packet: jui.jlong) callconv(.C) void {
        assert(packet != 0);

        var packet_list = tb.tb_packet_list_t{
            .head = @intToPtr(?*tb.tb_packet_t, @bitCast(usize, packets_head)),
            .tail = @intToPtr(?*tb.tb_packet_t, @bitCast(usize, packets_tail)),
        };

        JNIClient.push_packet(
            env,
            this_obj,
            &packet_list,
            @intToPtr(*tb.tb_packet_t, @bitCast(usize, packet)),
        );
    }
};

comptime {
    jui.exportUnder("com.tigerbeetle.Client", .{
        .onLoad = Exports.on_load_export,
        .onUnload = Exports.on_unload_export,
        .clientInit = Exports.client_init_export,
        .clientDeinit = Exports.client_deinit_export,
        .popPacket = Exports.pop_packet_export,
        .pushPacket = Exports.push_packet_export,
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
