const std = @import("std");
const builtin = @import("builtin");

// TODO: Move this back to src/clients/c when there's a better solution for main_pkg_path=src/
const vsr = @import("vsr.zig");
const tb = vsr.tb_client;

comptime {
    if (!builtin.link_libc) {
        @compileError("Must be built with libc to export tb_client symbols");
    }

    @export(init, .{ .name = "tb_client_init", .linkage = .strong });
    @export(init_echo, .{ .name = "tb_client_init_echo", .linkage = .strong });
    @export(tb.completion_context, .{ .name = "tb_client_completion_context", .linkage = .strong });
    @export(tb.submit, .{ .name = "tb_client_submit", .linkage = .strong });
    @export(tb.deinit, .{ .name = "tb_client_deinit", .linkage = .strong });
}

fn init(
    out_client: *tb.tb_client_t,
    cluster_id: u128,
    addresses_ptr: [*:0]const u8,
    addresses_len: u32,
    on_completion_ctx: usize,
    on_completion_fn: tb.tb_completion_t,
) callconv(.C) tb.tb_status_t {
    const addresses = @as([*]const u8, @ptrCast(addresses_ptr))[0..addresses_len];
    const client = tb.init(
        std.heap.c_allocator,
        cluster_id,
        addresses,
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
    on_completion_ctx: usize,
    on_completion_fn: tb.tb_completion_t,
) callconv(.C) tb.tb_status_t {
    const addresses = @as([*]const u8, @ptrCast(addresses_ptr))[0..addresses_len];
    const client = tb.init_echo(
        std.heap.c_allocator,
        cluster_id,
        addresses,
        on_completion_ctx,
        on_completion_fn,
    ) catch |err| return tb.init_error_to_status(err);

    out_client.* = client;
    return .success;
}
