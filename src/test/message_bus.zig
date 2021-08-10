const std = @import("std");
const assert = std.debug.assert;

const config = @import("../config.zig");

const MessagePool = @import("../message_pool.zig").MessagePool;
const Message = MessagePool.Message;
const Header = @import("../vr.zig").Header;

const Network = @import("network.zig").Network;

const log = std.log.scoped(.message_bus);

pub const Process = union(enum) {
    replica: u8,
    client: u128,
};

pub const MessageBus = struct {
    network: *Network,
    pool: MessagePool,

    cluster: u32,
    process: Process,

    /// The callback to be called when a message is received. Use set_on_message() to set
    /// with type safety for the context pointer.
    on_message_callback: ?fn (context: ?*c_void, message: *Message) void = null,
    on_message_context: ?*c_void = null,

    pub fn init(
        allocator: *std.mem.Allocator,
        cluster: u32,
        process: Process,
        network: *Network,
    ) !MessageBus {
        return MessageBus{
            .pool = try MessagePool.init(allocator),
            .network = network,
            .cluster = cluster,
            .process = process,
        };
    }

    /// TODO
    pub fn deinit(bus: *MessageBus) void {}

    pub fn set_on_message(
        bus: *MessageBus,
        comptime Context: type,
        context: Context,
        comptime on_message: fn (context: Context, message: *Message) void,
    ) void {
        assert(bus.on_message_callback == null);
        assert(bus.on_message_context == null);

        bus.on_message_callback = struct {
            fn wrapper(_context: ?*c_void, message: *Message) void {
                on_message(@intToPtr(Context, @ptrToInt(_context)), message);
            }
        }.wrapper;
        bus.on_message_context = context;
    }

    pub fn tick(self: *MessageBus) void {}

    pub fn get_message(bus: *MessageBus) ?*Message {
        return bus.pool.get_message();
    }

    pub fn unref(bus: *MessageBus, message: *Message) void {
        bus.pool.unref(message);
    }

    pub fn can_send_to_replica(bus: *MessageBus, replica: u8) bool {
        // TODO: This isn't safety-critical and is only used as an optimization
        // in the VR protocol implementation. Therefore it's fine to always
        // return true for now, but we should ideally have more correct logic
        // in the future to make the testing as realistic as possible.
        return true;
    }

    pub fn send_header_to_replica(bus: *MessageBus, replica: u8, header: Header) void {
        assert(header.size == @sizeOf(Header));

        if (!bus.can_send_to_replica(replica)) {
            log.debug("cannot send to replica {}, dropping", .{replica});
            return;
        }

        const message = bus.pool.get_header_only_message() orelse {
            log.debug("no header only message available, " ++
                "dropping message to replica {}", .{replica});
            return;
        };
        defer bus.unref(message);
        message.header.* = header;

        const body = message.buffer[@sizeOf(Header)..message.header.size];
        // The order matters here because checksum depends on checksum_body:
        message.header.set_checksum_body(body);
        message.header.set_checksum();

        bus.send_message_to_replica(replica, message);
    }

    pub fn send_message_to_replica(bus: *MessageBus, replica: u8, message: *Message) void {
        // Messages sent by a process to itself should never be passed to the message bus
        if (bus.process == .replica) assert(replica != bus.process.replica);

        bus.network.send_message(message, .{
            .source = bus.process,
            .target = .{ .replica = replica },
        });
    }

    pub fn send_header_to_client(bus: *MessageBus, client_id: u128, header: Header) void {
        assert(header.size == @sizeOf(Header));

        const message = bus.pool.get_header_only_message() orelse {
            log.debug("no header only message available, " ++
                "dropping message to client {}", .{client_id});
            return;
        };
        defer bus.unref(message);
        message.header.* = header;

        const body = message.buffer[@sizeOf(Header)..message.header.size];
        // The order matters here because checksum depends on checksum_body:
        message.header.set_checksum_body(body);
        message.header.set_checksum();

        bus.send_message_to_client(client_id, message);
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
