const std = @import("std");
const builtin = @import("builtin");
const jui = @import("jui");
const tb = @import("tb_client.zig");

const ClientReflection = struct {
    var class: jui.jclass = undefined;

    var client_handle_field_id: jui.jfieldID = undefined;
    var packets_head_field_id: jui.jfieldID = undefined;
    var packets_tail_field_id: jui.jfieldID = undefined;

    pub fn load(env: *jui.JNIEnv) !void {
        class = try env.findClass("com/tigerbeetle/Client");
        client_handle_field_id = try env.getFieldId(class, "clientHandle", "J");
        packets_head_field_id = try env.getFieldId(class, "packetsHead", "J");
        packets_tail_field_id = try env.getFieldId(class, "packetsTail", "J");
    }

    pub inline fn setTbClient(env: *jui.JNIEnv, this_object: jui.jobject, client: tb.Client) void {
        env.setField(.long, this_object, client_handle_field_id, @bitCast(jui.jlong, @ptrToInt(client)));
    }

    pub inline fn getTbClient(env: *jui.JNIEnv, this_object: jui.jobject) tb.Client {
        var ptr = env.getField(.long, this_object, client_handle_field_id);
        return @intToPtr(tb.Client, @bitCast(usize, ptr));
    }

    pub inline fn setPacketList(env: *jui.JNIEnv, this_object: jui.jobject, packet_list: tb.Packet.List) void {
        env.setField(.long, this_object, packets_head_field_id, @bitCast(jui.jlong, @ptrToInt(packet_list.head)));
        env.setField(.long, this_object, packets_tail_field_id, @bitCast(jui.jlong, @ptrToInt(packet_list.tail)));
    }

    pub inline fn getPacketList(env: *jui.JNIEnv, this_object: jui.jobject) tb.Packet.List {
        var head = env.getField(.long, this_object, packets_head_field_id);
        var tail = env.getField(.long, this_object, packets_tail_field_id);

        return .{
            .head = @intToPtr(?*tb.Packet, @bitCast(usize, head)),
            .tail = @intToPtr(?*tb.Packet, @bitCast(usize, tail)),
        };
    }
};

const RequestReflection = struct {
    var class: jui.jclass = undefined;

    var request_body_field_id: jui.jfieldID = undefined;
    var request_body_len_field_id: jui.jfieldID = undefined;
    var request_operation_field_id: jui.jfieldID = undefined;
    var request_end_request_method_id: jui.jmethodID = undefined;

    pub fn load(env: *jui.JNIEnv) !void {
        class = try env.findClass("com/tigerbeetle/Request");
        request_body_field_id = try env.getFieldId(class, "body", "Ljava/nio/ByteBuffer;");
        request_body_len_field_id = try env.getFieldId(class, "bodyLen", "J");
        request_operation_field_id = try env.getFieldId(class, "operation", "B");
        request_end_request_method_id = try env.getMethodId(class, "endRequest", "(Ljava/nio/ByteBuffer;B)V");
    }

    pub fn getBody(env: *jui.JNIEnv, this_object: jui.jobject) []u8 {
        var body_obj = env.getField(.object, this_object, request_body_field_id);
        var body_len = env.getField(.long, this_object, request_body_len_field_id);
        var address = env.getDirectBufferAddress(body_obj);

        // The buffer can be larger than the actual message content,
        return address[0..@intCast(usize, body_len)];
    }

    pub fn getOperation(env: *jui.JNIEnv, this_object: jui.jobject) u8 {
        return @bitCast(u8, env.getField(.byte, this_object, request_operation_field_id));
    }

    pub fn endRequest(env: *jui.JNIEnv, this_object: jui.jobject, result: ?[]const u8, status: tb.Packet.Status) void {
        var buffer: jui.jobject = if (result) |value| blk: {
            // force cast from *const to *anyopaque
            // it is ok here, since the result is always readonly
            var erased = @intToPtr(*anyopaque, @ptrToInt(value.ptr));
            break :blk env.newDirectByteBuffer(erased, value.len) catch null;
        } else null;
        defer if (buffer != null) env.deleteReference(.local, buffer);

        env.callMethod(
            .@"void",
            this_object,
            request_end_request_method_id,
            &[_]jui.jvalue{
                jui.jvalue.toJValue(buffer),
                jui.jvalue.toJValue(@bitCast(jui.jbyte, status)),
            },
        ) catch return;
    }
};

/// On JVM loads this library
fn onLoad(vm: *jui.JavaVM) !jui.jint {
    const version = jui.JNIVersion{ .major = 10, .minor = 0 };
    var env = try vm.getEnv(version);

    try ClientReflection.load(env);
    try RequestReflection.load(env);

    return @bitCast(jui.jint, version);
}

/// On JVM unloads this library
fn onUnload(vm: *jui.JavaVM) void {
    _ = vm;
}

/// Called from JNI
/// Initialize tb_client
fn clientInit(
    env: *jui.JNIEnv,
    this_object: jui.jobject,
    cluster_id: u32,
    addresses: [:0]const u8,
    max_concurrency: u32,
) !tb.TBStatus {
    var out_client: tb.Client = undefined;
    var out_packets: tb.Packet.List = undefined;

    var jvm = try env.getJavaVM();

    var status = tb.tb_client_init(
        &out_client,
        &out_packets,
        cluster_id,
        addresses.ptr,
        @intCast(u32, addresses.len),
        max_concurrency,
        @ptrToInt(jvm),
        onCompletion,
    );

    if (status == .success) {
        ClientReflection.setTbClient(env, this_object, out_client);
        ClientReflection.setPacketList(env, this_object, out_packets);
    }

    return status;
}

fn clientDeinit(env: *jui.JNIEnv, client_obj: jui.jobject) void {
    const client = ClientReflection.getTbClient(env, client_obj);
    tb.tb_client_deinit(client);
}

fn submit(env: *jui.JNIEnv, client_obj: jui.jobject, request_obj: jui.jobject) void {
    var packets = createPacketFromRequest(env, client_obj, request_obj);
    const client = ClientReflection.getTbClient(env, client_obj);

    tb.tb_client_submit(client, &packets);
}

fn createPacketFromRequest(env: *jui.JNIEnv, client_obj: jui.jobject, request_obj: jui.jobject) tb.Packet.List {
    var packets = ClientReflection.getPacketList(env, client_obj);

    if (packets.pop()) |head| {

        // Holds a global reference to prevent GC during the callback
        var global_ref = env.newReference(.global, request_obj) catch unreachable;
        errdefer env.deleteReference(.global, global_ref);

        var body = RequestReflection.getBody(env, request_obj);

        head.operation = RequestReflection.getOperation(env, request_obj);
        head.user_data = @ptrToInt(global_ref);
        head.data = body.ptr;
        head.data_size = @intCast(u32, body.len);
        head.next = null;
        head.status = .ok;

        return .{
            .head = head,
            .tail = head,
        };
    }

    // TODO: Add wait here!
    unreachable;
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
        // There is no way to recover here, just @panic
        // since we can't access the JNIEnv, we can't throw a proper Exception.
        @panic("Cannot attach client thread as a JVM daemon");
    };

    // Retrieves the request instance, and drops the GC reference
    var request_obj = @intToPtr(jui.jobject, packet.user_data);
    defer env.deleteReference(.global, request_obj);

    var result: ?[]const u8 = switch (packet.status) {
        .ok => if (result_ptr) |ptr| ptr[0..@intCast(usize, result_len)] else null,
        else => null,
    };

    RequestReflection.endRequest(env, request_obj, result, packet.status);
}

/// Exports entrypoints to JNI
const exports = struct {
    pub fn onLoadExport(vm: *jui.JavaVM) callconv(.C) jui.jint {
        return jui.wrapErrors(onLoad, .{vm});
    }

    pub fn onUnloadExport(vm: *jui.JavaVM) callconv(.C) void {
        return jui.wrapErrors(onUnload, .{vm});
    }

    pub fn clientInitExport(
        env: *jui.JNIEnv,
        this: jui.jobject,
        cluster_id: jui.jint,
        addresses: jui.jstring,
        max_concurrency: jui.jint,
    ) callconv(.C) jui.jint {
        var str = fromJString(env, addresses);
        defer env.releaseStringUTFChars(addresses, str);

        var status = jui.wrapErrors(
            clientInit,
            .{
                env,
                this,
                @bitCast(u32, cluster_id),
                str,
                @bitCast(u32, max_concurrency),
            },
        );

        return @bitCast(jui.jint, status);
    }

    pub fn clientDeinitExport(env: *jui.JNIEnv, this: jui.jobject) callconv(.C) void {
        jui.wrapErrors(
            clientDeinit,
            .{
                env,
                this,
            },
        );
    }

    pub fn submitExport(env: *jui.JNIEnv, this: jui.jobject, request: jui.jobject) callconv(.C) void {
        jui.wrapErrors(
            submit,
            .{
                env,
                this,
                request,
            },
        );
    }

    fn fromJString(env: *jui.JNIEnv, str: jui.jstring) [:0]const u8 {
        const len = @intCast(usize, env.getStringUTFLength(str));

        var ret = jui.wrapErrors(
            jui.JNIEnv.getStringUTFChars,
            .{
                env,
                str,
            },
        );

        return std.meta.assumeSentinel(ret.chars[0..len], 0);
    }
};

comptime {
    jui.exportUnder("com.tigerbeetle.Client", .{
        .onLoad = exports.onLoadExport,
        .onUnload = exports.onUnloadExport,
        .clientInit = exports.clientInitExport,
        .clientDeinit = exports.clientDeinitExport,
        .submit = exports.submitExport,
    });
}
