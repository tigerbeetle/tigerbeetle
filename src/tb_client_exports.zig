const std = @import("std");
const stdx = @import("stdx.zig");
const builtin = @import("builtin");

// TODO: Move this back to src/clients/c when there's a better solution for main_pkg_path=src/
const vsr = @import("vsr.zig");
const tb = vsr.tb_client;

pub const std_options = .{
    .log_level = vsr.constants.log_level,
    .logFn = tb.Logging.application_logger,
};

comptime {
    if (!builtin.link_libc) {
        @compileError("Must be built with libc to export tb_client symbols");
    }

    @export(
        tb.register_log_callback,
        .{ .name = "tb_client_register_log_callback", .linkage = .strong },
    );
    @export(init, .{ .name = "tb_client_init", .linkage = .strong });
    @export(init_echo, .{ .name = "tb_client_init_echo", .linkage = .strong });
    @export(tb.completion_context, .{ .name = "tb_client_completion_context", .linkage = .strong });
    @export(tb.submit, .{ .name = "tb_client_submit", .linkage = .strong });
    @export(tb.deinit, .{ .name = "tb_client_deinit", .linkage = .strong });
}

fn init(
    out_client: *tb.tb_client_t,
    cluster_id_ptr: *const [16]u8,
    addresses_ptr: [*:0]const u8,
    addresses_len: u32,
    on_completion_ctx: usize,
    on_completion_fn: tb.tb_completion_t,
) callconv(.C) tb.tb_status_t {
    const addresses = @as([*]const u8, @ptrCast(addresses_ptr))[0..addresses_len];

    // Passing u128 by value is prone to ABI issues. Pass as a [16]u8, and explicitly copy into
    // memory we know will be aligned correctly. Don't just use bytesToValue here, as that keeps
    // pointer alignment, and will result in a potentially unaligned access of a
    // `*align(1) const u128`.
    const cluster_id: u128 = blk: {
        var cluster_id: u128 = undefined;
        stdx.copy_disjoint(.exact, u8, std.mem.asBytes(&cluster_id), cluster_id_ptr);

        break :blk cluster_id;
    };

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
    cluster_id_ptr: *const [16]u8,
    addresses_ptr: [*:0]const u8,
    addresses_len: u32,
    on_completion_ctx: usize,
    on_completion_fn: tb.tb_completion_t,
) callconv(.C) tb.tb_status_t {
    const addresses = @as([*]const u8, @ptrCast(addresses_ptr))[0..addresses_len];

    // See explanation in init().
    const cluster_id: u128 = blk: {
        var cluster_id: u128 = undefined;
        stdx.copy_disjoint(.exact, u8, std.mem.asBytes(&cluster_id), cluster_id_ptr);

        break :blk cluster_id;
    };

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
