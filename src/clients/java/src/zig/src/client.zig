const std = @import("std");
const builtin = @import("builtin");
const jui = @import("jui");
const tb = @import("tb_client.zig");

var jvm: *jui.JavaVM = undefined;

// Cache classes and field IDs used by the JNI side
var client_class: jui.jclass = undefined;
var client_handle_field_id: jui.jfieldID = undefined;
var packets_head_field_id: jui.jfieldID = undefined;
var packets_tail_field_id: jui.jfieldID = undefined;

var request_class: jui.jclass = undefined;
var request_body_field_id: jui.jfieldID = undefined;
var request_body_len_field_id: jui.jfieldID = undefined;
var request_operation_field_id: jui.jfieldID = undefined;
var request_end_request_method_id: jui.jmethodID = undefined;

/// On JVM loads this library
fn onLoad(vm: *jui.JavaVM) !jui.jint {
    const version = jui.JNIVersion{ .major = 10, .minor = 0 };
    var env = try vm.getEnv(version);

    client_class = try env.findClass("com/tigerbeetle/Client");
    client_handle_field_id = try env.getFieldId(client_class, "clientHandle", "J");
    packets_head_field_id = try env.getFieldId(client_class, "packetsHead", "J");
    packets_tail_field_id = try env.getFieldId(client_class, "packetsTail", "J");

    request_class = try env.findClass("com/tigerbeetle/Request");
    request_body_field_id = try env.getFieldId(request_class, "body", "Ljava/nio/ByteBuffer;");
    request_body_len_field_id = try env.getFieldId(request_class, "bodyLen", "J");
    request_operation_field_id = try env.getFieldId(request_class, "operation", "B");
    request_end_request_method_id = try env.getMethodId(request_class, "endRequest", "(Ljava/nio/ByteBuffer;B)V");

    // It is safe to cache the global vm pointer
    // Just one JVM per process is allowed
    jvm = vm;

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

    var status = tb.tb_client_init(
        &out_client,
        &out_packets,
        cluster_id,
        addresses.ptr,
        @intCast(u32, addresses.len),
        max_concurrency,
        0,
        onCompletion,
    );

    if (status == .success) {
        setTbClient(env, this_object, out_client);
        setPacketList(env, this_object, out_packets);
    }

    return status;
}

fn clientDeinit(env: *jui.JNIEnv, client_obj: jui.jobject) void {
    const client = getTbClient(env, client_obj);
    tb.tb_client_deinit(client);
}

fn submit(env: *jui.JNIEnv, client_obj: jui.jobject, request_obj: jui.jobject) void {
    const client = getTbClient(env, client_obj);
    var packets = createPacketFromRequest(env, client_obj, request_obj);

    tb.tb_client_submit(client, &packets);
}

inline fn setTbClient(env: *jui.JNIEnv, this_object: jui.jobject, client: tb.Client) void {
    env.setField(.long, this_object, client_handle_field_id, @bitCast(jui.jlong, @ptrToInt(client)));
}

inline fn getTbClient(env: *jui.JNIEnv, this_object: jui.jobject) tb.Client {
    var ptr = env.getField(.long, this_object, client_handle_field_id);
    return @intToPtr(tb.Client, @bitCast(usize, ptr));
}

inline fn setPacketList(env: *jui.JNIEnv, this_object: jui.jobject, packet_list: tb.Packet.List) void {
    env.setField(.long, this_object, packets_head_field_id, @bitCast(jui.jlong, @ptrToInt(packet_list.head)));
    env.setField(.long, this_object, packets_tail_field_id, @bitCast(jui.jlong, @ptrToInt(packet_list.tail)));
}

inline fn getPacketList(env: *jui.JNIEnv, this_object: jui.jobject) tb.Packet.List {
    var head = env.getField(.long, this_object, packets_head_field_id);
    var tail = env.getField(.long, this_object, packets_tail_field_id);

    return .{
        .head = @intToPtr(?*tb.Packet, @bitCast(usize, head)),
        .tail = @intToPtr(?*tb.Packet, @bitCast(usize, tail)),
    };
}

fn createPacketFromRequest(env: *jui.JNIEnv, client_obj: jui.jobject, request_obj: jui.jobject) tb.Packet.List {
    var packets = getPacketList(env, client_obj);

    if (packets.pop()) |head| {
        var body_obj = env.getField(.object, request_obj, request_body_field_id);
        var body_len = env.getField(.long, request_obj, request_body_len_field_id);
        var body = env.getDirectBufferAddress(body_obj);

        // Holds a global reference to prevent GC during the callback
        var global_ref = env.newReference(.global, request_obj) catch unreachable;

        head.operation = @intCast(u8, env.getField(.byte, request_obj, request_operation_field_id));
        head.user_data = @ptrToInt(global_ref);
        head.data = body.ptr;
        head.data_size = @intCast(u32, body_len);
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
    _ = context;
    _ = client;

    var env = jvm.attachCurrentThreadAsDaemon() catch {
        // There is no way to recover here, just @panic
        // since we can't access the JNIEnv, we can't throw a proper Exception.
        @panic("Cannot attach client thread as a JVM daemon");
    };

    // Retrieves the request instance, and drops the GC reference
    var request_obj = @intToPtr(jui.jobject, packet.user_data);
    defer env.deleteReference(.global, request_obj);

    var buffer = if (packet.status == tb.Packet.Status.ok and result_ptr != null) blk: {

        // force cast from *const to *anyopaque
        // it is ok here, since the result is always readonly

        var values = @intToPtr([*]u32, @ptrToInt(result_ptr.?));
        std.log.err("HERE >> result_len = {}", .{result_len});
        std.log.err("HERE >> results = {},{}", .{ values[0], values[1] });

        var erased = @intToPtr(*anyopaque, @ptrToInt(result_ptr.?));
        break :blk env.newDirectByteBuffer(erased, @intCast(usize, result_len)) catch null;
    } else null;

    env.callMethod(
        .@"void",
        request_obj,
        request_end_request_method_id,
        &[_]jui.jvalue{
            jui.jvalue.toJValue(buffer),
            jui.jvalue.toJValue(@bitCast(jui.jbyte, packet.status)),
        },
    ) catch |err|
        {
        std.log.err("Error {s}", .{@errorName(err)});
    };

    std.log.err("here 13", .{});
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
