const std = @import("std");
const builtin = @import("builtin");
const tb = @import("tb_client.zig");

comptime {
    if (!builtin.link_libc) {
        @compileError("Must be built with libc to export tb_client symbols");
    }

    @export(ffi_init, .{ .name = "tb_client_init", .linkage = .Strong });
    @export(ffi_init_echo, .{ .name = "tb_client_init_echo", .linkage = .Strong });
    @export(tb.acquire_packet, .{ .name = "tb_client_acquire_packet", .linkage = .Strong });
    @export(tb.release_packet, .{ .name = "tb_client_release_packet", .linkage = .Strong });
    @export(tb.submit, .{ .name = "tb_client_submit", .linkage = .Strong });
    @export(tb.deinit, .{ .name = "tb_client_deinit", .linkage = .Strong });
}

fn ffi_allocator() std.mem.Allocator {
    return if (builtin.is_test)
        std.testing.allocator
    else if (builtin.link_libc)
        std.heap.c_allocator
    else if (builtin.target.os.tag == .windows)
        (struct {
            var gpa = std.heap.HeapAllocator.init();
        }).gpa.allocator()
    else
        @compileError("tb_client must be built with libc");
}

fn ffi_init(
    out_client: *tb.tb_client_t,
    cluster_id: u32,
    addresses_ptr: [*:0]const u8,
    addresses_len: u32,
    packets_count: u32,
    on_completion_ctx: usize,
    on_completion_fn: tb.tb_completion_t,
) callconv(.C) tb.tb_status_t {
    const addresses = @as([*]const u8, @ptrCast(addresses_ptr))[0..addresses_len];
    const client = init(
        ffi_allocator(),
        cluster_id,
        addresses,
        packets_count,
        on_completion_ctx,
        on_completion_fn,
    ) catch |err| return init_error_to_status(err);

    out_client.* = client;
    return .success;
}

fn ffi_init_echo(
    out_client: *tb_client_t,
    cluster_id: u32,
    addresses_ptr: [*:0]const u8,
    addresses_len: u32,
    packets_count: u32,
    on_completion_ctx: usize,
    on_completion_fn: tb_completion_t,
) callconv(.C) tb_status_t {
    const addresses = @as([*]const u8, @ptrCast(addresses_ptr))[0..addresses_len];
    const client = init_echo(
        ffi_allocator(),
        cluster_id,
        addresses,
        packets_count,
        on_completion_ctx,
        on_completion_fn,
    ) catch |err| return init_error_to_status(err);

    out_client.* = client;
    return .success;
}
