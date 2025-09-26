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

    buffers: []MessageBuffer,
    buffers_suspended: []bool,

    /// The callback to be called when a message is received.
    on_messages_callback: *const fn (
        message_bus: *MessageBus,
        buffer: *MessageBuffer,
        peer: vsr.Peer,
    ) void,

    pub const Options = struct {
        network: *Network,
        process_count: u8,
    };

    pub fn init(
        allocator: std.mem.Allocator,
        process: Process,
        message_pool: *MessagePool,
        on_messages_callback: *const fn (
            message_bus: *MessageBus,
            buffer: *MessageBuffer,
            peer: vsr.Peer,
        ) void,
        options: Options,
    ) !MessageBus {
        const buffers = try allocator.alloc(MessageBuffer, options.process_count);
        errdefer allocator.free(buffers);

        for (buffers, 0..) |*buffer, index| {
            errdefer for (buffer[0..index]) |*b| b.deinit(allocator);
            buffer.* = MessageBuffer.init(message_pool);

            errdefer buffer.deinit(allocator);
        }

        const buffers_suspended = try allocator.alloc(bool, options.process_count);
        errdefer allocator.free(buffers_suspended);
        @memset(buffers_suspended, false);

        return MessageBus{
            .network = options.network,
            .pool = message_pool,
            .process = process,
            .buffers = buffers,
            .buffers_suspended = buffers_suspended,
            .on_messages_callback = on_messages_callback,
        };
    }

    pub fn deinit(bus: *MessageBus, allocator: std.mem.Allocator) void {
        for (bus.buffers) |*buffer| {
            buffer.*.deinit(bus.pool);
        }
        allocator.free(bus.buffers);
        allocator.free(bus.buffers_suspended);

        // NB: Network keeps a reference to a message bus even when a replica is de-initialized,
        // so we don't assign bus.* to undefined here.
    }

    pub fn tick(_: *MessageBus) void {}

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
        for (bus.buffers_suspended) |suspended| {
            if (suspended) return true;
        } else return false;
    }

    pub fn resume_receive(bus: *MessageBus) void {
        for (bus.buffers_suspended, 0..) |*suspended, index| {
            if (suspended.*) {
                const process = Network.raw_process_to_process(bus.network.processes.items[index]);
                bus.on_messages_callback(
                    bus,
                    &bus.buffers[index],
                    bus.network.process_to_peer(process),
                );
                if (!bus.buffers[index].has_message()) suspended.* = false;
            }
        }
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
