const std = @import("std");
const ThreadType = @import("thread.zig").ThreadType;

const api = @import("../tb_client.zig");
const tb_status_t = api.tb_status_t;
const tb_client_t = api.tb_client_t;
const tb_completion_t = api.tb_completion_t;
const tb_packet_t = api.tb_packet_t;
const tb_packet_list_t = api.tb_packet_list_t;

pub const ContextImplementation = struct {
    submit_fn: *const fn (*ContextImplementation, *tb_packet_list_t) void,
    deinit_fn: *const fn (*ContextImplementation) void,
};

pub fn ContextType(
    comptime StateMachine: type,
    comptime MessageBus: type,
) type {
    return struct {
        const Context = @This();
        const Thread = ThreadType(StateMachine, MessageBus);

        allocator: std.mem.Allocator,
        on_completion_ctx: usize,
        on_completion_fn: tb_completion_t,
        implementation: ContextImplementation,
        thread: Thread,

        pub fn init(
            allocator: std.mem.Allocator,
            out_tb_client: *tb_client_t,
            out_packets: *tb_packet_list_t,
            cluster_id: u32,
            addresses_ptr: [*:0]const u8,
            addresses_len: u32,
            num_packets: u32,
            on_completion_ctx: usize,
            on_completion_fn: tb_completion_t,
        ) tb_status_t {
            const context = allocator.create(Context) catch return .out_of_memory;
            context.allocator = allocator;
            context.on_completion_ctx = on_completion_ctx;
            context.on_completion_fn = on_completion_fn;

            out_tb_client.* = api.context_to_client(&context.implementation);
            context.implementation = .{
                .submit_fn = Context.on_submit,
                .deinit_fn = Context.on_deinit,
            };

            const addresses = @ptrCast([*]const u8, addresses_ptr)[0..addresses_len];
            context.thread.init(
                allocator,
                cluster_id,
                addresses,
                num_packets,
                Context.on_completion,
            ) catch |err| {
                allocator.destroy(context);
                return switch (err) {
                    error.Unexpected => .unexpected,
                    error.OutOfMemory => .out_of_memory,
                    error.InvalidAddress => .invalid_address,
                    error.SystemResources => .system_resources,
                    error.NetworkSubsystemFailed => .network_subsystem,
                };
            };

            var list = tb_packet_list_t{};
            for (context.thread.packets) |*packet| {
                list.push(tb_packet_list_t.from(packet));
            }

            out_packets.* = list;
            return .success;
        }

        fn on_submit(implementation: *ContextImplementation, packets: *tb_packet_list_t) void {
            const context = @fieldParentPtr(Context, "implementation", implementation);
            context.thread.submit(packets.*);
        }

        fn on_deinit(implementation: *ContextImplementation) void {
            const context = @fieldParentPtr(Context, "implementation", implementation);
            context.thread.deinit();
            context.allocator.destroy(context);
        }

        fn on_completion(thread: *Thread, packet: *tb_packet_t, result: ?[]const u8) void {
            const context = @fieldParentPtr(Context, "thread", thread);
            const tb_client = api.context_to_client(&context.implementation);

            context.on_completion_fn(
                context.on_completion_ctx,
                tb_client,
                packet,
                if (result) |r| r.ptr else null,
                if (result) |r| @intCast(u32, r.len) else 0,
            );
        }
    };
}
