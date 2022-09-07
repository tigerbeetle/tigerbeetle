///! Java Native Interfaces for TigerBeetle Client
///! Please refer to the JNI Best Practice Guide
///! https://developer.ibm.com/articles/j-jni/
const std = @import("std");
const builtin = @import("builtin");
const jui = @import("jui");
const tb = @import("tb_client.zig");

const assert = std.debug.assert;
const jni_version = jui.JNIVersion{ .major = 10, .minor = 0 };

const ClientReflection = struct {
    var class: jui.jclass = null;

    var client_handle_field_id: jui.jfieldID = null;
    var packets_head_field_id: jui.jfieldID = null;
    var packets_tail_field_id: jui.jfieldID = null;

    pub fn load(env: *jui.JNIEnv) !void {

        // Asserting we are not initialized yet
        assert(class == null);
        assert(client_handle_field_id == null);
        assert(packets_head_field_id == null);
        assert(packets_tail_field_id == null);

        class = class: {
            var local_ref = try env.findClass("com/tigerbeetle/Client");
            assert(local_ref != null);
            defer env.deleteReference(.local, local_ref);

            break :class try env.newReference(.global, local_ref);
        };

        client_handle_field_id = try env.getFieldId(class, "clientHandle", "J");
        packets_head_field_id = try env.getFieldId(class, "packetsHead", "J");
        packets_tail_field_id = try env.getFieldId(class, "packetsTail", "J");

        // Asserting we are full initialized
        assert(class != null);
        assert(client_handle_field_id != null);
        assert(packets_head_field_id != null);
        assert(packets_tail_field_id != null);
    }

    pub fn unload(env: *jui.JNIEnv) void {
        env.deleteReference(.global, class);

        class = null;
        client_handle_field_id = null;
        packets_head_field_id = null;
        packets_tail_field_id = null;
    }

    pub inline fn setTbClient(env: *jui.JNIEnv, this_obj: jui.jobject, client: tb.Client) void {
        assert(this_obj != null);
        assert(client_handle_field_id != null);

        env.setField(.long, this_obj, client_handle_field_id, @bitCast(jui.jlong, @ptrToInt(client)));
    }

    pub inline fn setPacketList(env: *jui.JNIEnv, this_obj: jui.jobject, packet_list: *const tb.Packet.List) void {
        assert(this_obj != null);
        assert(packets_head_field_id != null);
        assert(packets_tail_field_id != null);

        env.setField(.long, this_obj, packets_head_field_id, @bitCast(jui.jlong, @ptrToInt(packet_list.head)));
        env.setField(.long, this_obj, packets_tail_field_id, @bitCast(jui.jlong, @ptrToInt(packet_list.tail)));
    }
};

const RequestReflection = struct {
    var class: jui.jclass = null;

    var request_buffer_field_id: jui.jfieldID = null;
    var request_buffer_len_field_id: jui.jfieldID = null;
    var request_operation_field_id: jui.jfieldID = null;
    var request_end_request_method_id: jui.jmethodID = null;

    pub fn load(env: *jui.JNIEnv) !void {

        // Asserting we are not initialized yet
        assert(class == null);
        assert(request_buffer_field_id == null);
        assert(request_buffer_len_field_id == null);
        assert(request_operation_field_id == null);
        assert(request_end_request_method_id == null);

        class = class: {
            var local_ref = try env.findClass("com/tigerbeetle/Request");
            assert(local_ref != null);
            defer env.deleteReference(.local, local_ref);

            break :class try env.newReference(.global, local_ref);
        };

        request_buffer_field_id = try env.getFieldId(class, "buffer", "Ljava/nio/ByteBuffer;");
        request_buffer_len_field_id = try env.getFieldId(class, "bufferLen", "J");
        request_operation_field_id = try env.getFieldId(class, "operation", "B");
        request_end_request_method_id = try env.getMethodId(class, "endRequest", "(BLjava/nio/ByteBuffer;JB)V");

        // Asserting we are full initialized
        assert(class != null);
        assert(request_buffer_field_id != null);
        assert(request_buffer_len_field_id != null);
        assert(request_operation_field_id != null);
        assert(request_end_request_method_id != null);
    }

    pub fn unload(env: *jui.JNIEnv) void {
        env.deleteReference(.global, class);

        class = null;
        request_buffer_field_id = null;
        request_buffer_len_field_id = null;
        request_operation_field_id = null;
        request_end_request_method_id = null;
    }

    pub fn getBuffer(env: *jui.JNIEnv, this_obj: jui.jobject) []u8 {
        assert(this_obj != null);
        assert(request_buffer_field_id != null);
        assert(request_buffer_len_field_id != null);

        var buffer_obj = env.getField(.object, this_obj, request_buffer_field_id);
        defer env.deleteReference(.local, buffer_obj);

        assert(buffer_obj != null);

        var buffer_len = env.getField(.long, this_obj, request_buffer_len_field_id);
        var address = env.getDirectBufferAddress(buffer_obj);

        // The buffer can be larger than the actual message content,
        return address[0..@intCast(usize, buffer_len)];
    }

    pub fn getOperation(env: *jui.JNIEnv, this_obj: jui.jobject) u8 {
        assert(this_obj != null);
        assert(request_operation_field_id != null);

        return @bitCast(u8, env.getField(.byte, this_obj, request_operation_field_id));
    }

    pub fn endRequest(env: *jui.JNIEnv, this_obj: jui.jobject, result: ?[]const u8, packet: *tb.Packet) void {
        assert(this_obj != null);
        assert(request_end_request_method_id != null);

        var buffer_obj: jui.jobject = if (result) |value| blk: {
            // force cast from *const to *anyopaque
            // it is ok here, since the ByteBuffer is always readonly
            var erased = @intToPtr(*anyopaque, @ptrToInt(value.ptr));
            var buffer = env.newDirectByteBuffer(erased, value.len) catch {
                std.log.err("JNI Request.endRequest newDirectByteBuffer len={}", .{value.len});
                env.describeException();
                unreachable;
            };

            assert(buffer != null);
            break :blk buffer;
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
        ) catch {
            // This method isn't expected to throw any exception,
            std.log.err("JNI: Request.endRequest callMethod", .{});
            env.describeException();
            unreachable;
        };
    }
};

/// On JVM loads this library
fn onLoad(vm: *jui.JavaVM) !jui.jint {
    var env = try vm.getEnv(jni_version);

    try ClientReflection.load(env);
    try RequestReflection.load(env);

    return @bitCast(jui.jint, jni_version);
}

/// On JVM unloads this library
fn onUnload(vm: *jui.JavaVM) !void {
    var env = try vm.getEnv(jni_version);
    ClientReflection.unload(env);
    RequestReflection.unload(env);
}

fn clientInit(
    env: *jui.JNIEnv,
    this_obj: jui.jobject,
    cluster_id: u32,
    addresses_obj: jui.jstring,
    max_concurrency: u32,
) !tb.TBStatus {
    assert(this_obj != null);
    assert(addresses_obj != null);

    var out_client: tb.Client = undefined;
    var out_packets: tb.Packet.List = undefined;

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
        onCompletion,
    );

    if (status == .success) {
        ClientReflection.setTbClient(env, this_obj, out_client);
        ClientReflection.setPacketList(env, this_obj, &out_packets);
    }

    return status;
}

fn clientDeinit(client: tb.Client) void {
    tb.tb_client_deinit(client);
}

fn submit(
    env: *jui.JNIEnv,
    request_obj: jui.jobject,
    client: tb.Client,
    packet: *tb.Packet,
) void {
    assert(request_obj != null);

    // Holds a global reference to prevent GC during the callback
    var global_ref = env.newReference(.global, request_obj) catch |err| {
        std.log.err("JNI: Request.submit newReference {s}", .{@errorName(err)});
        env.describeException();
        unreachable;
    };

    assert(global_ref != null);
    errdefer env.deleteReference(.global, global_ref);

    var buffer = RequestReflection.getBuffer(env, request_obj);

    packet.operation = RequestReflection.getOperation(env, request_obj);
    packet.user_data = @ptrToInt(global_ref);
    packet.data = buffer.ptr;
    packet.data_size = @intCast(u32, buffer.len);
    packet.next = null;
    packet.status = .ok;

    var packet_list = tb.Packet.List.from(packet);
    tb.tb_client_submit(client, &packet_list);
}

fn onCompletion(
    context: usize,
    client: tb.Client,
    packet: *tb.Packet,
    result_ptr: ?[*]const u8,
    result_len: u32,
) callconv(.C) void {
    _ = client;

    var jvm = @intToPtr(*jui.JavaVM, context);
    var env = jvm.attachCurrentThreadAsDaemon() catch {
        // There is no way to recover here
        // since we can't access the JNIEnv, we can't throw a proper Exception.
        std.log.err("JNI: Request.onCompletion attachCurrentThreadAsDaemon", .{});
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

    RequestReflection.endRequest(env, request_obj, result, packet);
}

fn popPacket(
    env: *jui.JNIEnv,
    client_obj: jui.jobject,
    packet_list: *tb.Packet.List,
) *tb.Packet {
    assert(client_obj != null);

    var packet = packet_list.pop() orelse {

        // It is unexpeted to packet_list be empty
        // The java side must syncronize how many threads call this function
        std.log.err("JNI: Client.popPacket empty", .{});
        unreachable;
    };
    ClientReflection.setPacketList(env, client_obj, packet_list);

    return packet;
}

fn pushPacket(
    env: *jui.JNIEnv,
    client_obj: jui.jobject,
    packet_list: *tb.Packet.List,
    packet: *tb.Packet,
) void {
    packet_list.push(packet);
    ClientReflection.setPacketList(env, client_obj, packet_list);
}

/// Exports entrypoints to JNI
const exports = struct {
    pub fn onLoadExport(vm: *jui.JavaVM) callconv(.C) jui.jint {
        return jui.wrapErrors(onLoad, .{vm});
    }

    pub fn onUnloadExport(vm: *jui.JavaVM) callconv(.C) void {
        jui.wrapErrors(onUnload, .{vm});
    }

    pub fn clientInitExport(
        env: *jui.JNIEnv,
        this: jui.jobject,
        cluster_id: jui.jint,
        addresses: jui.jstring,
        max_concurrency: jui.jint,
    ) callconv(.C) jui.jint {
        var status = jui.wrapErrors(
            clientInit,
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

    pub fn clientDeinitExport(env: *jui.JNIEnv, this: jui.jobject, client_handle: jui.jlong) callconv(.C) void {
        _ = env;
        _ = this;
        clientDeinit(@intToPtr(tb.Client, @bitCast(usize, client_handle)));
    }

    pub fn submitExport(env: *jui.JNIEnv, this_obj: jui.jobject, client_handle: jui.jlong, request_obj: jui.jobject, packet: jui.jlong) callconv(.C) void {
        _ = this_obj;
        assert(client_handle != 0);
        assert(packet != 0);

        submit(
            env,
            request_obj,
            @intToPtr(tb.Client, @bitCast(usize, client_handle)),
            @intToPtr(*tb.Packet, @bitCast(usize, packet)),
        );
    }

    pub fn popPacketExport(env: *jui.JNIEnv, this_obj: jui.jobject, packets_head: jui.jlong, packets_tail: jui.jlong) callconv(.C) jui.jlong {
        var packet_list = tb.Packet.List{
            .head = @intToPtr(?*tb.Packet, @bitCast(usize, packets_head)),
            .tail = @intToPtr(?*tb.Packet, @bitCast(usize, packets_tail)),
        };

        var packet = popPacket(
            env,
            this_obj,
            &packet_list,
        );
        return @bitCast(jui.jlong, @ptrToInt(packet));
    }

    pub fn pushPacketExport(env: *jui.JNIEnv, this_obj: jui.jobject, packets_head: jui.jlong, packets_tail: jui.jlong, packet: jui.jlong) callconv(.C) void {
        assert(packet != 0);

        var packet_list = tb.Packet.List{
            .head = @intToPtr(?*tb.Packet, @bitCast(usize, packets_head)),
            .tail = @intToPtr(?*tb.Packet, @bitCast(usize, packets_tail)),
        };

        pushPacket(
            env,
            this_obj,
            &packet_list,
            @intToPtr(*tb.Packet, @bitCast(usize, packet)),
        );
    }
};

comptime {
    jui.exportUnder("com.tigerbeetle.Client", .{
        .onLoad = exports.onLoadExport,
        .onUnload = exports.onUnloadExport,
        .clientInit = exports.clientInitExport,
        .clientDeinit = exports.clientDeinitExport,
        .popPacket = exports.popPacketExport,
        .pushPacket = exports.pushPacketExport,
        .submit = exports.submitExport,
    });
}
