const std = @import("std");
const builtin = @import("builtin");
const tb = @import("tb_client.zig");

comptime {
    if (!builtin.link_libc) {
        @compileError("Must be built with libc to export tb_client symbols");
    }

    @export(init, .{ .name = "tb_client_init", .linkage = .Strong });
    @export(init_echo, .{ .name = "tb_client_init_echo", .linkage = .Strong });
    @export(tb.completion_context, .{ .name = "tb_client_completion_context", .linkage = .Strong });
    @export(tb.acquire_packet, .{ .name = "tb_client_acquire_packet", .linkage = .Strong });
    @export(tb.release_packet, .{ .name = "tb_client_release_packet", .linkage = .Strong });
    @export(tb.submit, .{ .name = "tb_client_submit", .linkage = .Strong });
    @export(tb.deinit, .{ .name = "tb_client_deinit", .linkage = .Strong });
}

fn init(
    out_client: *tb.tb_client_t,
    cluster_id: u128,
    addresses_ptr: [*:0]const u8,
    addresses_len: u32,
    packets_count: u32,
    on_completion_ctx: usize,
    on_completion_fn: tb.tb_completion_t,
) callconv(.C) tb.tb_status_t {
    const addresses = @as([*]const u8, @ptrCast(addresses_ptr))[0..addresses_len];
    const client = tb.init(
        std.heap.c_allocator,
        cluster_id,
        addresses,
        packets_count,
        on_completion_ctx,
        on_completion_fn,
    ) catch |err| return tb.init_error_to_status(err);

    out_client.* = client;
    return .success;
}

fn init_echo(
    out_client: *tb.tb_client_t,
    cluster_id: u128,
    addresses_ptr: [*:0]const u8,
    addresses_len: u32,
    packets_count: u32,
    on_completion_ctx: usize,
    on_completion_fn: tb.tb_completion_t,
) callconv(.C) tb.tb_status_t {
    const addresses = @as([*]const u8, @ptrCast(addresses_ptr))[0..addresses_len];
    const client = tb.init_echo(
        std.heap.c_allocator,
        cluster_id,
        addresses,
        packets_count,
        on_completion_ctx,
        on_completion_fn,
    ) catch |err| return tb.init_error_to_status(err);

    out_client.* = client;
    return .success;
}
