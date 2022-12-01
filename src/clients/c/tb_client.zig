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
    address_invalid,
    address_limit_exceeded,
    packets_count_invalid,
    system_resources,
    network_subsystem,
};

pub const tb_operation_t = StateMachine.Operation;
pub const tb_completion_t = fn (
    context: usize,
    client: tb_client_t,
    packet: *tb_packet_t,
    result_ptr: ?[*]const u8,
    result_len: u32,
) callconv(.C) void;

const constants = @import("../../constants.zig");
const Storage = @import("../../storage.zig").Storage;
const MessageBus = @import("../../message_bus.zig").MessageBusClient;
const StateMachine = @import("../../state_machine.zig").StateMachineType(Storage, .{
    .message_body_size_max = constants.message_body_size_max,
});

const ContextType = @import("tb_client/context.zig").ContextType;
const ContextImplementation = @import("tb_client/context.zig").ContextImplementation;

const DefaultContext = blk: {
    const Client = @import("../../vsr/client.zig").Client(StateMachine, MessageBus);
    break :blk ContextType(Client);
};

const TestingContext = blk: {
    const EchoClient = @import("tb_client/echo_client.zig").EchoClient(StateMachine, MessageBus);
    break :blk ContextType(EchoClient);
};

pub fn context_to_client(implementation: *ContextImplementation) tb_client_t {
    return @ptrCast(tb_client_t, implementation);
}

fn client_to_context(tb_client: tb_client_t) *ContextImplementation {
    return @ptrCast(*ContextImplementation, @alignCast(@alignOf(ContextImplementation), tb_client));
}

// Only export the functions if we're compiling libtb_client.
// If it's only being imported by another zig file, then exporting the functions
// will force them to be evaluated/codegen/linked and trigger unexpected comptime paths.
comptime {
    if (builtin.link_libc) {
        @export(tb_client_init, .{ .name = "tb_client_init", .linkage = .Strong });
        @export(tb_client_init_echo, .{ .name = "tb_client_init_echo", .linkage = .Strong });
    }
}

pub fn tb_client_init(
    out_client: *tb_client_t,
    out_packets: *tb_packet_list_t,
    cluster_id: u32,
    addresses_ptr: [*:0]const u8,
    addresses_len: u32,
    packets_count: u32,
    on_completion_ctx: usize,
    on_completion_fn: tb_completion_t,
) callconv(.C) tb_status_t {
    return init(
        DefaultContext,
        out_client,
        out_packets,
        cluster_id,
        addresses_ptr,
        addresses_len,
        packets_count,
        on_completion_ctx,
        on_completion_fn,
    );
}

pub fn tb_client_init_echo(
    out_client: *tb_client_t,
    out_packets: *tb_packet_list_t,
    cluster_id: u32,
    addresses_ptr: [*:0]const u8,
    addresses_len: u32,
    packets_count: u32,
    on_completion_ctx: usize,
    on_completion_fn: tb_completion_t,
) callconv(.C) tb_status_t {
    return init(
        TestingContext,
        out_client,
        out_packets,
        cluster_id,
        addresses_ptr,
        addresses_len,
        packets_count,
        on_completion_ctx,
        on_completion_fn,
    );
}

fn init(
    comptime Context: type,
    out_client: *tb_client_t,
    out_packets: *tb_packet_list_t,
    cluster_id: u32,
    addresses_ptr: [*:0]const u8,
    addresses_len: u32,
    packets_count: u32,
    on_completion_ctx: usize,
    on_completion_fn: tb_completion_t,
) tb_status_t {
    // Pick the most suitable allocator for the platform.
    const allocator = if (builtin.is_test)
        std.testing.allocator
    else if (builtin.link_libc)
        std.heap.c_allocator
    else if (builtin.target.os.tag == .windows)
        (struct {
            var gpa = std.heap.HeapAllocator.init();
        }).gpa.allocator()
    else
        @compileError("tb_client must be built with libc");

    const addresses = @ptrCast([*]const u8, addresses_ptr)[0..addresses_len];
    const context = Context.init(
        allocator,
        cluster_id,
        addresses,
        packets_count,
        on_completion_ctx,
        on_completion_fn,
    ) catch |err| switch (err) {
        error.Unexpected => return .unexpected,
        error.OutOfMemory => return .out_of_memory,
        error.AddressInvalid => return .address_invalid,
        error.AddressLimitExceeded => return .address_limit_exceeded,
        error.PacketsCountInvalid => return .packets_count_invalid,
        error.SystemResources => return .system_resources,
        error.NetworkSubsystemFailed => return .network_subsystem,
    };

    out_client.* = context_to_client(&context.implementation);
    var list = tb_packet_list_t{};
    for (context.packets) |*packet| {
        list.push(tb_packet_list_t.from(packet));
    }

    out_packets.* = list;
    return .success;
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
