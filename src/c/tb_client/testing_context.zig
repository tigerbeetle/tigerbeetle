const std = @import("std");
const os = std.os;
const assert = std.debug.assert;

const config = @import("../../config.zig");
const log = std.log.scoped(.tb_client);

const vsr = @import("../../vsr.zig");
const Header = vsr.Header;

const IO = @import("../../io.zig").IO;
const message_pool = @import("../../message_pool.zig");

const Packet = @import("packet.zig").Packet;
const Signal = @import("signal.zig").Signal;
const ThreadType = @import("thread.zig").ThreadType;
const ContextImplementation = @import("context.zig").ContextImplementation;
const Error = @import("context.zig").Error;

const api = @import("../tb_client.zig");
const tb_status_t = api.tb_status_t;
const tb_client_t = api.tb_client_t;
const tb_completion_t = api.tb_completion_t;
const tb_packet_t = api.tb_packet_t;
const tb_packet_list_t = api.tb_packet_list_t;

pub const TestingContext = struct {
    const Context = @This();
    const Thread = ThreadType(Context);

    const PacketError = error{
        InvalidDataSize,
        TooManyOutstandingRequests,
    };

    allocator: std.mem.Allocator,
    packets: []Packet,
    echo_stack: Packet.Stack,
    io: IO,

    on_completion_ctx: usize,
    on_completion_fn: tb_completion_t,
    implementation: ContextImplementation,
    thread: Thread,
    available_messages: usize,

    pub fn create(
        allocator: std.mem.Allocator,
        cluster_id: u32,
        addresses: []const u8,
        num_packets: u32,
        on_completion_ctx: usize,
        on_completion_fn: tb_completion_t,
    ) Error!*Context {
        _ = cluster_id;
        _ = addresses;

        var context = allocator.create(Context) catch |err| {
            log.err("failed to create context: {}", .{err});
            return err;
        };
        errdefer context.destroy();

        context.allocator = allocator;

        log.debug("init: allocating tb_packets.", .{});
        context.packets = context.allocator.alloc(Packet, num_packets) catch |err| {
            log.err("failed to allocate tb_packets: {}", .{err});
            return err;
        };
        errdefer context.allocator.free(context.packets);

        log.debug("init: initializing IO.", .{});
        context.io = IO.init(32, 0) catch |err| {
            log.err("failed to initialize IO: {}.", .{err});
            return switch (err) {
                error.ProcessFdQuotaExceeded => error.SystemResources,
                error.Unexpected => error.Unexpected,
                else => unreachable,
            };
        };
        errdefer context.io.deinit();

        context.echo_stack = .{};
        context.available_messages = message_pool.messages_max_client;
        context.on_completion_ctx = on_completion_ctx;
        context.on_completion_fn = on_completion_fn;
        context.implementation = .{
            .submit_fn = Context.on_submit,
            .deinit_fn = Context.on_deinit,
        };

        log.debug("init: initializing thread.", .{});
        context.thread.init(
            context,
        ) catch |err| {
            log.err("failed to initalize thread: {}", .{err});
            return err;
        };
        errdefer context.thread.deinit(context.allocator);

        return context;
    }

    pub fn destroy(self: *Context) void {
        self.thread.deinit();
        self.allocator.free(self.packets);
        self.allocator.destroy(self);
    }

    pub fn tick(self: *Context) void {
        _ = self;
    }

    pub fn run(self: *Context) void {
        while (!self.thread.signal.is_shutdown()) {
            self.io.run_for_ns(config.tick_ms * std.time.ns_per_ms) catch |err| {
                log.warn("run_for_ns: {}", .{err});
                std.debug.panic("io.run_for_ns(): {}", .{err});
            };
            while (self.echo_stack.pop()) |packet| {
                const result = blk: {
                    // We don't use a message_pool, yet we simulate the "TooManyOutstandingRequests" behavior:
                    const current_queue_size = message_pool.messages_max_client - self.available_messages;
                    break :blk if (packet.data_size == 0)
                        error.InvalidDataSize
                    else if (current_queue_size > config.client_request_queue_max)
                        error.TooManyOutstandingRequests
                    else
                        packet.data[0..packet.data_size];
                };

                self.on_complete(packet, result);
            }
        }
    }

    pub fn request(self: *Context, packet: *Packet) void {
        assert(self.available_messages > 0);
        self.available_messages -= 1;

        self.echo_stack.push(Packet.List.from(packet));
    }

    fn on_complete(
        self: *Context,
        packet: *Packet,
        result: PacketError![]const u8,
    ) void {
        assert(self.available_messages < message_pool.messages_max_client);
        self.available_messages += 1;

        const tb_client = api.context_to_client(&self.implementation);
        const bytes = result catch |err| {
            packet.status = switch (err) {
                // If there's too many requests, (re)try submitting the packet later
                error.TooManyOutstandingRequests => {
                    return self.thread.submit(Packet.List.from(packet));
                },
                error.InvalidDataSize => .invalid_data_size,
            };
            return (self.on_completion_fn)(self.on_completion_ctx, tb_client, packet, null, 0);
        };

        packet.status = .ok;
        (self.on_completion_fn)(self.on_completion_ctx, tb_client, packet, bytes.ptr, @intCast(u32, bytes.len));
    }

    fn on_submit(implementation: *ContextImplementation, packets: *tb_packet_list_t) void {
        const context = @fieldParentPtr(Context, "implementation", implementation);
        context.thread.submit(packets.*);
    }

    fn on_deinit(implementation: *ContextImplementation) void {
        const context = @fieldParentPtr(Context, "implementation", implementation);
        context.destroy();
    }
};
