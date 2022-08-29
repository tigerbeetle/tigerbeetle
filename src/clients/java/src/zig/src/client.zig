const std = @import("std");
const builtin = @import("builtin");
const jui = @import("jui");
const tb = @import("tb_client.zig");

const class_name = "com/tigerbeetle/Client";

var class: jui.jclass = undefined;
var client_handle_field_id: jui.jfieldID = undefined;
var packets_head_field_id: jui.jfieldID = undefined;
var packets_tail_field_id: jui.jfieldID = undefined;

/// On JVM loads this library
fn onLoad(vm: *jui.JavaVM) !jui.jint {
    const version = jui.JNIVersion{ .major = 10, .minor = 0 };
    var env = try vm.getEnv(version);

    class = try env.findClass(class_name);
    client_handle_field_id = try env.getFieldId(class, "clientHandle", "J");
    packets_head_field_id = try env.getFieldId(class, "packetsHead", "J");
    packets_tail_field_id = try env.getFieldId(class, "packetsTail", "J");

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
) !tb.tb_status_t {
    var out_client: tb.tb_client_t = undefined;
    var out_packets: tb.tb_packet_list_t = undefined;

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

    if (status == tb.tb_status_t.success) {
        setTbClient(env, this_object, out_client);
        setPacketList(env, this_object, out_packets);
    }

    return status;
}

fn clientDeinit(env: *jui.JNIEnv, this_object: jui.jobject, client: jui.jlong) void {
    _ = env;
    _ = this_object;
    tb.tb_client_deinit(@intToPtr(tb.tb_client_t, @bitCast(usize, client)));
}

inline fn setTbClient(env: *jui.JNIEnv, this_object: jui.jobject, client: tb.tb_client_t) void {
    env.setField(.long, this_object, client_handle_field_id, @bitCast(jui.jlong, @ptrToInt(client)));
}

inline fn getTbClient(env: *jui.JNIEnv, this_object: jui.jobject) tb.tb_client {
    var ptr = env.getField(.long, this_object, client_handle_field_id);
    return @intToPtr(tb.tb_client_t, @bitCast(usize, ptr));
}

inline fn setPacketList(env: *jui.JNIEnv, this_object: jui.jobject, packet_list: tb.tb_packet_list_t) void {
    env.setField(.long, this_object, packets_head_field_id, @bitCast(jui.jlong, @ptrToInt(packet_list.head)));
    env.setField(.long, this_object, packets_tail_field_id, @bitCast(jui.jlong, @ptrToInt(packet_list.tail)));
}

inline fn getPacketList(env: *jui.JNIEnv, this_object: jui.jobject) tb.tb_packet_list_t {
    var head = env.getField(.long, this_object, packets_head_field_id);
    var tail = env.getField(.long, this_object, packets_tail_field_id);

    return .{
        .head = @intToPtr(?*tb.tb_packet_t, @bitCast(usize, head)),
        .tail = @intToPtr(?*tb.tb_packet_t, @bitCast(usize, tail)),
    };
}

fn onCompletion(
    context: usize,
    client: tb.tb_client_t,
    packet: *tb.tb_packet_t,
    result_ptr: ?[*]const u8,
    result_len: u32,
) callconv(.C) void {
    _ = context;
    _ = client;
    _ = packet;
    _ = result_ptr;
    _ = result_len;
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
        this_object: jui.jobject,
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
                this_object,
                @bitCast(u32, cluster_id),
                str,
                @bitCast(u32, max_concurrency),
            },
        );

        return @bitCast(jui.jint, status);
    }

    pub fn clientDeinitExport(env: *jui.JNIEnv, this_object: jui.jobject, client: jui.jlong) callconv(.C) void {
        jui.wrapErrors(
            clientDeinit,
            .{
                env,
                this_object,
                client,
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
    });
}
