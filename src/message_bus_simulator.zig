const std = @import("std");
const assert = std.debug.assert;

const NetworkSimulator = @import("network_simulator.zig").NetworkSimulator;
const NetworkSimulatorOptions = @import("network_simulator.zig").NetworkSimulatorOptions;

pub const MessageBusSimulatorOptions = struct {
    replica_count: u8,
    client_count: u8,

    network_simulator_options: NetworkSimulatorOptions,
};

pub const MessageBusSimulator = struct {
    const MessageCallback = struct {
        callback: ?fn (context: ?*c_void, message: *Message) void,
        context: ?*c_void,
    };

    options: MessageBusSimulatorOptions,
    network_simulator: NetworkSimulator,
    pool: MessagePool,

    on_message_callbacks: []MessageCallback,

    client_ids: std.ArrayListUnmanaged(u128),

    pub fn init(
        allocator: *std.mem.Allocator,
        options: MessageBusSimulatorOptions,
    ) !MessageBusSimulator {
        assert(options.client_count + options.replica_count <= std.math.maxInt(u8));

        const on_message_callbacks = try allocator.alloc(
            MessageCallback,
            options.replica_count + options.client_count,
        );
        errdefer allocator.free(on_message_callbacks);
        std.mem.set(MessageCallback, on_message_callbacks, .{ .callback = null, .context = null });

        const client_ids = try std.ArrayListUnmanaged(u128).initCapacity(allocator, options.client_count);
        errdefer cliend_ids.deinit();

        const network_simulator = try NetworkSimulator.init(allocator, options.network_simulator_options);
        errdefer network_simulator.deinit(allocator);

        return MessageBusSimulator{
            .options = options,
            .network_simulator = network_simulator,
            .on_message_callbacks = on_message_callbacks,
            .client_ids = client_ids,
        };
    }

    pub fn deinit(bus: *MessageBusSimulator, allocator: *std.mem.Allocator) void {
        allocator.free(on_message_callbacks);
        cliend_ids.deinit(allocator);
        network_simulator.deinit(allocator);
    }

    pub fn add_replica(
        bus: *MessageBusSimulator,
        comptime Context: type,
        context: Context,
        comptime on_message: fn (context: Context, message: *Message) void,
        replica: u8,
    ) void {
        assert(bus.on_message_callbacks[replica].callback == null);
        bus.on_message_callbacks[replica] = .{
            .callback = struct {
                fn wrapper(_context: ?*c_void, message: *Message) void {
                    on_message(@intToPtr(Context, @ptrToInt(_context)), message);
                }
            }.wrapper,
            .context = context,
        };
    }

    pub fn add_client(
        bus: *MessageBusSimulator,
        comptime Context: type,
        context: Context,
        comptime on_message: fn (context: Context, message: *Message) void,
        client: u128,
    ) void {
        assert(std.mem.indexOfScalar(u128, self.client_ids.items, client) == null);

        bus.client_ids.appendAssumeCapacity(client);

        const address = @intCast(u8, bus.client_ids.items.len) + bus.config.replica_count;

        assert(bus.on_message_callbacks[address].callback == null);
        bus.on_message_callbacks[address] = .{
            .callback = struct {
                fn wrapper(_context: ?*c_void, message: *Message) void {
                    on_message(@intToPtr(Context, @ptrToInt(_context)), message);
                }
            }.wrapper,
            .context = context,
        };
    }

    // copied

    pub fn get_message(bus: *Self) ?*Message {
        return bus.pool.get_message();
    }

    pub fn unref(bus: *Self, message: *Message) void {
        bus.pool.unref(message);
    }

    /// Returns true if the target replica is connected and has space in its send queue.
    pub fn can_send_to_replica(bus: *Self, replica: u8) bool {
        if (process_type == .replica and replica == bus.process.replica) {
            return !bus.process.send_queue.full();
        } else {
            const connection = bus.replicas[replica] orelse return false;
            return connection.state == .connected and !connection.send_queue.full();
        }
    }

    pub fn send_header_to_replica(bus: *Self, replica: u8, header: Header) void {
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

    pub fn send_message_to_replica(bus: *Self, replica: u8, message: *Message) void {
        // Messages sent by a process to itself are delivered directly in flush():
        if (process_type == .replica and replica == bus.process.replica) {
            bus.process.send_queue.push(message.ref()) catch |err| switch (err) {
                error.NoSpaceLeft => {
                    bus.unref(message);
                    log.notice("process' message queue full, dropping message", .{});
                },
            };
        } else if (bus.replicas[replica]) |connection| {
            connection.send_message(bus, message);
        } else {
            log.debug("no active connection to replica {}, " ++
                "dropping message with header {}", .{ replica, message.header });
        }
    }

    pub fn send_header_to_client(bus: *Self, client_id: u128, header: Header) void {
        assert(header.size == @sizeOf(Header));

        // TODO Do not allocate a message if we know we cannot send to the client.

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
    pub fn send_message_to_client(bus: *Self, client_id: u128, message: *Message) void {
        comptime assert(process_type == .replica);

        if (bus.process.clients.get(client_id)) |connection| {
            connection.send_message(bus, message);
        } else {
            log.debug("no connection to client {x}", .{client_id});
        }
    }

    /// Deliver messages the replica has sent to itself.
    pub fn flush_send_queue(bus: *Self) void {
        comptime assert(process_type == .replica);

        // There are currently 3 cases in which a replica will send a message to itself:
        // 1. In on_request, the leader sends a prepare to itself, and
        //    subsequent prepare timeout retries will never resend to self.
        // 2. In on_prepare, after writing to storage, the leader sends a
        //    prepare_ok back to itself.
        // 3. In on_start_view_change, after receiving a quorum of start_view_change
        //    messages, the new leader sends a do_view_change to itself.
        // Therefore we should never enter an infinite loop here. To catch the case in which we
        // do so that we can learn from it, assert that we never iterate more than 100 times.
        var i: usize = 0;
        while (i < 100) : (i += 1) {
            if (bus.process.send_queue.empty()) return;

            var copy = bus.process.send_queue;
            bus.process.send_queue = .{};

            while (copy.pop()) |message| {
                defer bus.unref(message);
                bus.on_message_callback.?(bus.on_message_context, message);
            }
        }
        unreachable;
    }
};
