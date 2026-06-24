const std = @import("std");
const assert = std.debug.assert;
const vsr = @import("../../vsr.zig");
const constants = @import("../../constants.zig");

const MessagePool = @import("../../message_pool.zig").MessagePool;
const MessageNetwork = @import("../../message_bus.zig").MessageNetwork;
const HeaderCallbackResult = @import("../../message_bus.zig").HeaderCallbackResult;
const Header = vsr.Header;
const HeaderEncrypted = vsr.HeaderEncrypted;
const ProcessType = vsr.ProcessType;

const Network = @import("network.zig").Network;

pub const Process = union(ProcessType) {
    replica: u8,
    client: u128,
};

pub const MessageBus = struct {
    network: *Network,
    pool: *MessagePool,

    process: Process,

    resume_scheduled: bool = false,
    header_callback: *const fn (
        context: *anyopaque,
        header: HeaderEncrypted,
    ) anyerror!u32,

    message_callback: *const fn (
        context: *anyopaque,
        message: []const u8,
    ) anyerror!vsr.Peer,

    recv_buffer: []u8,
    recv_size: u32 = 0,

    pub const Options = struct {
        network: *Network,
    };

    pub fn init(
        gpa: std.mem.Allocator,
        process: Process,
        message_pool: *MessagePool,
        header_callback: *const fn (
            context: *anyopaque,
            header: HeaderEncrypted,
        ) anyerror!u32,
        message_callback: *const fn (
            context: *anyopaque,
            message: []const u8,
        ) anyerror!vsr.Peer,
        options: Options,
    ) !MessageBus {
        const recv_buffer = try gpa.alloc(u8, constants.message_size_max);
        errdefer gpa.free(recv_buffer);

        return MessageBus{
            .network = options.network,
            .pool = message_pool,
            .process = process,
            .header_callback = header_callback,
            .message_callback = message_callback,
            .recv_buffer = recv_buffer,
        };
    }

    pub fn deinit(bus: *MessageBus, gpa: std.mem.Allocator) void {
        bus.resume_scheduled = false;
        // NB: Network keeps a reference to a message bus even when a replica is de-initialized,
        // so we don't assign bus.* to undefined here.
        gpa.free(bus.recv_buffer);
    }

    pub fn trace_gauge(_: *MessageBus) void {}

    pub fn listen(_: *MessageBus) !void {}

    pub fn tick(_: *MessageBus) void {}

    pub fn tick_client(bus: *MessageBus) void {
        bus.tick();
    }

    pub fn get_message(
        bus: *MessageBus,
        comptime command: ?vsr.Command,
    ) MessagePool.GetMessageType(command) {
        return bus.pool.get_message(command);
    }

    /// `@TypeOf(message)` is one of:
    /// - `*Message`
    /// - `MessageType(command)` for any `command`.
    pub fn unref(bus: *MessageBus, message: anytype) void {
        bus.pool.unref(message);
    }

    pub fn send_message_to_client(
        bus: *MessageBus,
        client: u128,
        size: u32,
    ) ?[]u8 {
        _ = bus;
        _ = client;
        _ = size;
        return null;
        // assert(bus.process == .replica);
        //
        // bus.network.send_message(message, .{
        //     .source = bus.process,
        //     .target = .{ .client = client_id },
        // });
    }
    pub fn send_message_handshake(
        _: *MessageBus,
        _: u128,
        _: u32,
    ) ?[]u8 {
        return null;
    }

    pub fn send_message_to_replica(
        bus: *MessageBus,
        replica: u8,
        size: u32,
    ) ?[]u8 {
        _ = bus;
        _ = replica;
        _ = size;
        return null;
        // Messages sent by a process to itself should never be passed to the message bus
        // if (bus.process == .replica) assert(replica != bus.process.replica);
        //
        // bus.network.send_message(message, .{
        //     .source = bus.process,
        //     .target = .{ .replica = replica },
        // });
    }
};
