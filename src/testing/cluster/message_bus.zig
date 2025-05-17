const std = @import("std");
const assert = std.debug.assert;

const MessagePool = @import("../../message_pool.zig").MessagePool;
const Message = MessagePool.Message;
const Header = vsr.Header;
const vsr = @import("../../vsr.zig");
const stdx = @import("../../stdx.zig");
const ProcessType = vsr.ProcessType;
const RingBufferType = stdx.RingBufferType;
const constants = vsr.constants;

const Network = @import("network.zig").Network;

pub const Process = union(ProcessType) {
    replica: u8,
    client: u128,
};

pub const MessageBus = struct {
    network: *Network,
    pool: *MessagePool,

    cluster: u128,
    process: Process,

    receive_queue: RingBufferType(*Message, .{
        .array = receive_queue_capacity,
    }) = .{ .buffer = undefined },

    pub const receive_queue_capacity = 4;
    pub const Options = struct {
        network: *Network,
    };

    pub fn init(
        _: std.mem.Allocator,
        cluster: u128,
        process: Process,
        message_pool: *MessagePool,
        options: Options,
    ) !MessageBus {
        return MessageBus{
            .network = options.network,
            .pool = message_pool,
            .cluster = cluster,
            .process = process,
        };
    }

    /// TODO
    pub fn deinit(_: *MessageBus, _: std.mem.Allocator) void {}

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

    pub fn peek_message(bus: *const MessageBus) ?Header {
        const messsage = bus.receive_queue.head() orelse return null;
        return messsage.header.*;
    }

    pub fn receive_message(bus: *MessageBus) ?*Message {
        const message = bus.receive_queue.pop() orelse return null;
        defer bus.network.message_pool.unref(message);

        const target_message = bus.get_message(null);

        stdx.copy_disjoint(.exact, u8, target_message.buffer, message.buffer);

        if (target_message.header.command == .request or
            target_message.header.command == .prepare or
            target_message.header.command == .block)
        {
            const sector_ceil = vsr.sector_ceil(target_message.header.size);
            if (target_message.header.size != sector_ceil) {
                assert(target_message.header.size < sector_ceil);
                assert(target_message.buffer.len == constants.message_size_max);
                @memset(target_message.buffer[target_message.header.size..sector_ceil], 0);
            }
        }

        return target_message;
    }
};
