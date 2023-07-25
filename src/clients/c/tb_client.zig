const std = @import("std");
const builtin = @import("builtin");

pub const tb_packet_t = @import("tb_client/packet.zig").Packet;
pub const tb_packet_status_t = tb_packet_t.Status;
pub const tb_packet_acquire_status_t = @import("tb_client/context.zig").PacketAcquireStatus;

pub const tb_client_t = *anyopaque;
pub const tb_status_t = enum(c_int) {
    success = 0,
    unexpected,
    out_of_memory,
    address_invalid,
    address_limit_exceeded,
    concurrency_max_invalid,
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
const Storage = @import("../../storage.zig").Storage;
const MessageBus = @import("../../message_bus.zig").MessageBusClient;
const StateMachine = @import("../../state_machine.zig").StateMachineType(Storage, constants.state_machine_config);

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
    return @ptrCast(tb_client_t, implementation);
}

fn client_to_context(tb_client: tb_client_t) *ContextImplementation {
    return @ptrCast(*ContextImplementation, @alignCast(@alignOf(ContextImplementation), tb_client));
}

pub fn init_error_to_status(err: InitError) tb_status_t {
    return switch (err) {
        error.Unexpected => .unexpected,
        error.OutOfMemory => .out_of_memory,
        error.AddressInvalid => .address_invalid,
        error.AddressLimitExceeded => .address_limit_exceeded,
        error.ConcurrencyMaxInvalid => .concurrency_max_invalid,
        error.SystemResources => .system_resources,
        error.NetworkSubsystemFailed => .network_subsystem,
    };
}

const ffi = struct {
    pub fn c_init(
        out_client: *tb_client_t,
        cluster_id: u32,
        addresses_ptr: [*:0]const u8,
        addresses_len: u32,
        packets_count: u32,
        on_completion_ctx: usize,
        on_completion_fn: tb_completion_t,
    ) callconv(.C) tb_status_t {
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
        const client = init(
            allocator,
            cluster_id,
            addresses,
            packets_count,
            on_completion_ctx,
            on_completion_fn,
        ) catch |err| return init_error_to_status(err);

        out_client.* = client;
        return .success;
    }

    pub fn c_init_echo(
        out_client: *tb_client_t,
        cluster_id: u32,
        addresses_ptr: [*:0]const u8,
        addresses_len: u32,
        packets_count: u32,
        on_completion_ctx: usize,
        on_completion_fn: tb_completion_t,
    ) callconv(.C) tb_status_t {
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
        const client = init_echo(
            allocator,
            cluster_id,
            addresses,
            packets_count,
            on_completion_ctx,
            on_completion_fn,
        ) catch |err| return init_error_to_status(err);

        out_client.* = client;
        return .success;
    }
};

// Only export the functions if we're compiling libtb_client.
// If it's only being imported by another zig file, then exporting the functions
// will force them to be evaluated/codegen/linked and trigger unexpected comptime paths.
comptime {
    if (builtin.link_libc) {
        @export(ffi.c_init, .{ .name = "tb_client_init", .linkage = .Strong });
        @export(ffi.c_init_echo, .{ .name = "tb_client_init_echo", .linkage = .Strong });
        @export(acquire_packet, .{ .name = "tb_client_acquire_packet", .linkage = .Strong });
        @export(release_packet, .{ .name = "tb_client_release_packet", .linkage = .Strong });
        @export(submit, .{ .name = "tb_client_submit", .linkage = .Strong });
        @export(deinit, .{ .name = "tb_client_deinit", .linkage = .Strong });
    }
}

pub fn init(
    allocator: std.mem.Allocator,
    cluster_id: u32,
    addresses: []const u8,
    packets_count: u32,
    on_completion_ctx: usize,
    on_completion_fn: tb_completion_t,
) InitError!tb_client_t {
    const context = try DefaultContext.init(
        allocator,
        cluster_id,
        addresses,
        packets_count,
        on_completion_ctx,
        on_completion_fn,
    );

    return context_to_client(&context.implementation);
}

pub fn init_echo(
    allocator: std.mem.Allocator,
    cluster_id: u32,
    addresses: []const u8,
    packets_count: u32,
    on_completion_ctx: usize,
    on_completion_fn: tb_completion_t,
) InitError!tb_client_t {
    const context = try TestingContext.init(
        allocator,
        cluster_id,
        addresses,
        packets_count,
        on_completion_ctx,
        on_completion_fn,
    );

    return context_to_client(&context.implementation);
}

pub fn acquire_packet(
    client: tb_client_t,
    out_packet: *?*tb_packet_t,
) callconv(.C) tb_packet_acquire_status_t {
    const context = client_to_context(client);
    return (context.acquire_packet_fn)(context, out_packet);
}

pub fn release_packet(
    client: tb_client_t,
    packet: *tb_packet_t,
) callconv(.C) void {
    const context = client_to_context(client);
    return (context.release_packet_fn)(context, packet);
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
