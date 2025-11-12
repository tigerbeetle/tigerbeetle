const std = @import("std");
const assert = std.debug.assert;

const MessagePool = @import("../../message_pool.zig").MessagePool;
const Message = MessagePool.Message;
const MessageBuffer = @import("../../message_buffer.zig").MessageBuffer;
const vsr = @import("../../vsr.zig");
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

    buffer: ?MessageBuffer,
    suspended: bool = false,
    resume_scheduled: bool = false,
    /// The callback to be called when a message is received.
    on_messages_callback: *const fn (message_bus: *MessageBus, buffer: *MessageBuffer) void,

    pub const Options = struct {
        network: *Network,
    };

    pub fn init(
        _: std.mem.Allocator,
        process: Process,
        message_pool: *MessagePool,
        on_messages_callback: *const fn (message_bus: *MessageBus, buffer: *MessageBuffer) void,
        options: Options,
    ) !MessageBus {
        return MessageBus{
            .network = options.network,
            .pool = message_pool,
            .process = process,
            .buffer = MessageBuffer.init(message_pool),
            .on_messages_callback = on_messages_callback,
        };
    }

    pub fn deinit(bus: *MessageBus, _: std.mem.Allocator) void {
        bus.buffer.?.deinit(bus.pool);
        bus.buffer = null;
        bus.resume_scheduled = false;
        // NB: Network keeps a reference to a message bus even when a replica is de-initialized,
        // so we don't assign bus.* to undefined here.
    }

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

    pub fn resume_needed(bus: *MessageBus) bool {
        return bus.suspended;
    }

    pub fn resume_receive(bus: *MessageBus) void {
        bus.suspended = false;
        bus.resume_scheduled = true;
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
