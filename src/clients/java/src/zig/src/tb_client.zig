const std = @import("std");
const builtin = @import("builtin");

// TODO: Reorganize tb_client to allow reference as a package instead of a static library
// Those decls should be avoided by using the tb_client.zig from tigerbeetle's submodule

pub const tb_packet_status_t = enum(u8) {
    ok,
    too_much_data,
    invalid_operation,
    invalid_data_size,
};

pub const tb_packet_t = extern struct {
    next: ?*tb_packet_t,
    user_data: usize,
    operation: u8,
    status: tb_packet_status_t,
    data_size: u32,
    data: [*]const u8,
};

pub const tb_packet_list_t = extern struct {
    head: ?*tb_packet_t = null,
    tail: ?*tb_packet_t = null,
};

pub const tb_client_t = *anyopaque;

pub const tb_status_t = enum(c_int) {
    success = 0,
    unexpected,
    out_of_memory,
    invalid_address,
    system_resources,
    network_subsystem,
};

pub const tb_completion_t = fn (
    context: usize,
    client: tb_client_t,
    packet: *tb_packet_t,
    result_ptr: ?[*]const u8,
    result_len: u32,
) callconv(.C) void;

pub extern fn tb_client_init(
    out_client: *tb_client_t,
    out_packets: *tb_packet_list_t,
    cluster_id: u32,
    addresses_ptr: [*:0]const u8,
    addresses_len: u32,
    num_packets: u32,
    on_completion_ctx: usize,
    on_completion_fn: tb_completion_t,
) callconv(.C) tb_status_t;

pub extern fn tb_client_submit(
    client: tb_client_t,
    packets: *tb_packet_list_t,
) callconv(.C) void;

pub extern fn tb_client_deinit(
    client: tb_client_t,
) callconv(.C) void;
