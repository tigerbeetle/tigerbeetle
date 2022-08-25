const std = @import("std");
const assert = std.debug.assert;

const config = @import("../config.zig");

const MessagePool = @import("../message_pool.zig").MessagePool;
const Message = MessagePool.Message;
const Header = @import("../vsr.zig").Header;
const ProcessType = @import("../vsr.zig").ProcessType;

const Network = @import("network.zig").Network;

const log = std.log.scoped(.message_bus);

pub const Process = union(ProcessType) {
    replica: u8,
    client: u128,
};

pub const MessageBus = struct {
    network: *Network,
    pool: *MessagePool,

    cluster: u32,
    process: Process,

    /// The callback to be called when a message is received.
    on_message_callback: *const fn (message_bus: *MessageBus, message: *Message) void,

    pub const Options = struct {
        network: *Network,
    };

    pub fn init(
        _: std.mem.Allocator,
        cluster: u32,
        process: Process,
        message_pool: *MessagePool,
        on_message_callback: *const fn (message_bus: *MessageBus, message: *Message) void,
        options: Options,
    ) !MessageBus {
        return MessageBus{
            .network = options.network,
            .pool = message_pool,
            .cluster = cluster,
            .process = process,
            .on_message_callback = on_message_callback,
        };
    }

    /// TODO
    pub fn deinit(_: *MessageBus, _: std.mem.Allocator) void {}

    pub fn tick(_: *MessageBus) void {}

    pub fn get_message(bus: *MessageBus) *Message {
        return bus.pool.get_message();
    }

    pub fn unref(bus: *MessageBus, message: *Message) void {
        bus.pool.unref(message);
    }

    pub fn send_message_to_replica(bus: *MessageBus, replica: u8, message: *Message) void {
        // Messages sent by a process to itself should never be passed to the message bus
        if (bus.process == .replica) assert(replica != bus.process.replica);

        bus.network.send_message(message, .{
            .source = bus.process,
            .target = .{ .replica = replica },
        });
    }

    /// Try to send the message to the client with the given id.
    /// If the client is not currently connected, the message is silently dropped.
    pub fn send_message_to_client(bus: *MessageBus, client_id: u128, message: *Message) void {
        assert(bus.process == .replica);

        bus.network.send_message(message, .{
            .source = bus.process,
            .target = .{ .client = client_id },
        });
    }
};
