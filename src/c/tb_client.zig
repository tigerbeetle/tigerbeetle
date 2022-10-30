const std = @import("std");
const builtin = @import("builtin");

pub const tb_packet_t = @import("tb_client/packet.zig").Packet;
pub const tb_packet_list_t = tb_packet_t.List;
pub const tb_packet_status_t = tb_packet_t.Status;

pub const tb_client_t = *anyopaque;
pub const tb_status_t = enum(c_int) {
    success = 0,
    unexpected,
    out_of_memory,
    invalid_address,
    system_resources,
    network_subsystem,
};

pub const tb_completion_t = *const fn (
    context: usize,
    client: tb_client_t,
    packet: *tb_packet_t,
    result_ptr: ?[*]const u8,
    result_len: u32,
) callconv(.C) void;

const ContextType = @import("tb_client/context.zig").ContextType;
const ContextImplementation = @import("tb_client/context.zig").ContextImplementation;

pub fn context_to_client(implementation: *ContextImplementation) tb_client_t {
    return @ptrCast(tb_client_t, implementation);
}

fn client_to_context(tb_client: tb_client_t) *ContextImplementation {
    return @ptrCast(*ContextImplementation, @alignCast(@alignOf(ContextImplementation), tb_client));
}

const DefaultContext = blk: {
    const config = @import("../config.zig");
    const Storage = @import("../storage.zig").Storage;
    const MessageBus = @import("../message_bus.zig").MessageBusClient;
    const StateMachine = @import("../state_machine.zig").StateMachineType(Storage, .{
        .message_body_size_max = config.message_body_size_max,
    });
    break :blk ContextType(StateMachine, MessageBus);
};

// const TestingContext = blk: {
//     const MessageBus = @import("test_message_bus.zig").MessageBusClient;
//     const StateMachine = @import("../state_machine.zig").StateMachine;
//     break :blk ContextType(StateMachine, MessageBus);
// };

// Pick the most suitable allocator
const global_allocator = if (builtin.link_libc)
    std.heap.c_allocator
else if (builtin.target.os.tag == .windows)
    (struct {
        var gpa = std.heap.HeapAllocator.init();
    }).gpa.allocator()
else
    @compileError("tb_client must be built with libc");

pub export fn tb_client_init(
    out_client: *tb_client_t,
    out_packets: *tb_packet_list_t,
    cluster_id: u32,
    addresses_ptr: [*:0]const u8,
    addresses_len: u32,
    num_packets: u32,
    on_completion_ctx: usize,
    on_completion_fn: tb_completion_t,
) tb_status_t {
    const init_fn = DefaultContext.init;
    // if (addresses_len == 0) {
    //     init_fn = TestingContext.init;
    // }

    return (init_fn)(
        global_allocator,
        out_client,
        out_packets,
        cluster_id,
        addresses_ptr,
        addresses_len,
        num_packets,
        on_completion_ctx,
        on_completion_fn,
    );
}

pub export fn tb_client_submit(
    client: tb_client_t,
    packets: *tb_packet_list_t,
) void {
    const context = client_to_context(client);
    (context.submit_fn)(context, packets);
}

pub export fn tb_client_deinit(
    client: tb_client_t,
) void {
    const context = client_to_context(client);
    (context.deinit_fn)(context);
}
