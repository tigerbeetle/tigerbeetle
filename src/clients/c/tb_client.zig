const std = @import("std");
const builtin = @import("builtin");

// When referenced from unit_test.zig, there is no vsr import module. So use relative path instead.
pub const vsr = if (@import("root") == @This()) @import("vsr") else @import("../../vsr.zig");

pub const tb_packet_t = @import("tb_client/packet.zig").Packet;
pub const tb_packet_status_t = tb_packet_t.Status;

pub const tb_client_t = *anyopaque;
pub const tb_status_t = enum(c_int) {
    success = 0,
    unexpected,
    out_of_memory,
    address_invalid,
    address_limit_exceeded,
    system_resources,
    network_subsystem,
};

pub const tb_operation_t = StateMachine.Operation;
pub const tb_completion_t = *const fn (
    context: usize,
    client: tb_client_t,
    packet: *tb_packet_t,
    result_ptr: ?[*]const u8,
    result_len: u32,
) callconv(.C) void;

const constants = @import("../../constants.zig");
const IO = @import("../../io.zig").IO;
const Storage = @import("../../storage.zig").Storage(IO);
const MessageBus = @import("../../message_bus.zig").MessageBusClient;
const StateMachineType = @import("../../state_machine.zig").StateMachineType;
const StateMachine = StateMachineType(Storage, constants.state_machine_config);

const ContextType = @import("tb_client/context.zig").ContextType;
const ContextImplementation = @import("tb_client/context.zig").ContextImplementation;
pub const InitError = @import("tb_client/context.zig").Error;

const DefaultContext = blk: {
    const Client = @import("../../vsr/client.zig").Client(StateMachine, MessageBus);
    break :blk ContextType(Client);
};

const TestingContext = blk: {
    const EchoClient = @import("tb_client/echo_client.zig").EchoClient(StateMachine, MessageBus);
    break :blk ContextType(EchoClient);
};

pub fn context_to_client(implementation: *ContextImplementation) tb_client_t {
    return @ptrCast(implementation);
}

fn client_to_context(tb_client: tb_client_t) *ContextImplementation {
    return @ptrCast(@alignCast(tb_client));
}

pub fn init_error_to_status(err: InitError) tb_status_t {
    return switch (err) {
        error.Unexpected => .unexpected,
        error.OutOfMemory => .out_of_memory,
        error.AddressInvalid => .address_invalid,
        error.AddressLimitExceeded => .address_limit_exceeded,
        error.SystemResources => .system_resources,
        error.NetworkSubsystemFailed => .network_subsystem,
    };
}

pub fn init(
    allocator: std.mem.Allocator,
    cluster_id: u128,
    addresses: []const u8,
    on_completion_ctx: usize,
    on_completion_fn: tb_completion_t,
) InitError!tb_client_t {
    const context = try DefaultContext.init(
        allocator,
        cluster_id,
        addresses,
        on_completion_ctx,
        on_completion_fn,
    );

    return context_to_client(&context.implementation);
}

pub fn init_echo(
    allocator: std.mem.Allocator,
    cluster_id: u128,
    addresses: []const u8,
    on_completion_ctx: usize,
    on_completion_fn: tb_completion_t,
) InitError!tb_client_t {
    const context = try TestingContext.init(
        allocator,
        cluster_id,
        addresses,
        on_completion_ctx,
        on_completion_fn,
    );

    return context_to_client(&context.implementation);
}

pub fn completion_context(client: tb_client_t) callconv(.C) usize {
    const context = client_to_context(client);
    return context.completion_ctx;
}

pub fn submit(
    client: tb_client_t,
    packet: *tb_packet_t,
) callconv(.C) void {
    const context = client_to_context(client);
    (context.submit_fn)(context, packet);
}

pub fn deinit(
    client: tb_client_t,
) callconv(.C) void {
    const context = client_to_context(client);
    (context.deinit_fn)(context);
}
