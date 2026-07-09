const std = @import("std");
const assert = std.debug.assert;
const vsr = @import("../../vsr.zig");

const MessagePool = @import("../../message_pool.zig").MessagePool;
const Message = MessagePool.Message;
const HeaderCallbackResult = @import("../../message_bus.zig").HeaderCallbackResult;
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
    ) anyerror!HeaderCallbackResult,

    message_callback: *const fn (
        context: *anyopaque,
        message: []const u8,
    ) anyerror!vsr.Peer,

    handshakes: std.AutoArrayHashMapUnmanaged(u128, Process) = .{},

    send_tokens_outstanding: u64 = 0,

    pub const Options = struct {
        network: *Network,
    };

    // For the real message bus, which is asynchronous, we could avoid splitting
    // acquiring the buffer and sending the buffer into two steps, because it is asynchronous.
    // For the testing MessageBus, we cannot do that because it is synchronous.
    pub const MessageToken = struct {
        target: []u8,
        other: Process,
        message: *MessagePool.Message,
        bus: *MessageBus,

        fn create(bus: *MessageBus, other: Process, size: u32) MessageToken {
            const message = bus.network.message_pool.get_message(null);
            bus.send_tokens_outstanding += 1;

            return .{
                .target = message.buffer[0..size],
                .other = other,
                .message = message,
                .bus = bus,
            };
        }

        pub fn send(token: MessageToken) void {
            assert(token.bus.send_tokens_outstanding > 0);
            token.bus.send_tokens_outstanding -= 1;

            token.bus.network.packet_simulator.submit_packet(
                token.message,
                .{
                    .source = token.bus.network.process_to_address(token.bus.process),
                    .target = token.bus.network.process_to_address(token.other),
                },
            );
        }
    };

    pub fn init(
        gpa: std.mem.Allocator,
        process: Process,
        message_pool: *MessagePool,
        header_callback: *const fn (
            context: *anyopaque,
            header: HeaderEncrypted,
        ) anyerror!HeaderCallbackResult,
        message_callback: *const fn (
            context: *anyopaque,
            message: []const u8,
        ) anyerror!vsr.Peer,
        options: Options,
    ) !MessageBus {
        var bus: MessageBus = .{
            .network = options.network,
            .pool = message_pool,
            .process = process,
            .header_callback = header_callback,
            .message_callback = message_callback,
        };

        try bus.handshakes.ensureTotalCapacity(
            gpa,
            options.network.options.client_count + options.network.options.node_count,
        );
        errdefer bus.handshakes.deinit(gpa);

        return bus;
    }

    pub fn deinit(bus: *MessageBus, gpa: std.mem.Allocator) void {
        assert(bus.send_tokens_outstanding == 0);
        bus.handshakes.deinit(gpa);

        bus.resume_scheduled = false;
        // NB: Network keeps a reference to a message bus even when a replica is de-initialized,
        // so we don't assign bus.* to undefined here.
    }

    pub fn trace_gauge(_: *MessageBus) void {}

    pub fn listen(_: *MessageBus) !void {}

    pub fn tick(bus: *MessageBus) void {
        assert(bus.send_tokens_outstanding == 0);
    }

    pub fn tick_client(bus: *MessageBus) void {
        bus.tick();
    }

    pub fn insert_existing(bus: *MessageBus, other: *MessageBus) void {
        if (bus == other) {
            return;
        }
        assert(bus.network == other.network);
        assert(std.meta.eql(bus.process, other.process));

        bus.resume_scheduled = other.resume_scheduled;
        bus.send_tokens_outstanding = other.send_tokens_outstanding;

        assert(bus.handshakes.capacity() == other.handshakes.capacity());
        bus.handshakes.clearRetainingCapacity();
        var handshakes_iterator = other.handshakes.iterator();
        while (handshakes_iterator.next()) |handshake| {
            bus.handshakes.putAssumeCapacity(handshake.key_ptr.*, handshake.value_ptr.*);
        }
    }

    pub fn message_from_network(
        bus: *MessageBus,
        message_encrypted: *Message,
        source: Process,
    ) void {
        const header_encrypted = std.mem.bytesAsValue(
            vsr.HeaderEncrypted,
            message_encrypted.buffer[0..@sizeOf(vsr.HeaderEncrypted)],
        );

        const result = bus.header_callback(bus, header_encrypted.*) catch {
            std.log.warn("message_from_network: drop message", .{});
            return;
        };

        if (result.handshake_id) |handshake_id| {
            if (bus.handshakes.count() == bus.handshakes.capacity()) {
                const handshake_id_existing = bus.handshakes.keys()[0];
                std.log.warn(
                    "message_from_network: evicting handshake id ({d})",
                    .{handshake_id_existing},
                );
                bus.handshakes.orderedRemoveAt(0);
            }

            const gop = bus.handshakes.getOrPutAssumeCapacity(handshake_id);
            if (!gop.found_existing) {
                gop.value_ptr.* = source;
            }
        }

        _ = bus.message_callback(
            bus,
            message_encrypted.buffer[0..result.message_size],
        ) catch unreachable;
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

    pub fn send_message_to_client(bus: *MessageBus, client_id: u128, size: u32) ?MessageToken {
        assert(bus.process == .replica);
        return MessageToken.create(bus, .{ .client = client_id }, size);
    }

    pub fn send_message_handshake(
        bus: *MessageBus,
        handshake_id: u128,
        size: u32,
    ) ?MessageToken {
        assert(bus.handshakes.capacity() > 0);

        if (bus.handshakes.get(handshake_id)) |process| {
            return MessageToken.create(bus, process, size);
        } else {
            std.log.warn(
                "send_message_to_handshake: no connection for handshake_id={}",
                .{handshake_id},
            );
            return null;
        }
    }

    pub fn send_message_to_replica(
        bus: *MessageBus,
        replica: u8,
        size: u32,
    ) ?MessageToken {
        return MessageToken.create(bus, .{ .replica = replica }, size);
    }
};
