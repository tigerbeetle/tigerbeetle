const std = @import("std");
const math = std.math;
const mem = std.mem;
const assert = std.debug.assert;

const config = @import("../config.zig");
const vsr = @import("../vsr.zig");
const util = @import("../util.zig");

const MessagePool = @import("../message_pool.zig").MessagePool;
const Message = MessagePool.Message;

const MessageBus = @import("message_bus.zig").MessageBus;
const Process = @import("message_bus.zig").Process;

const PacketSimulator = @import("packet_simulator.zig").PacketSimulator;
const PacketSimulatorOptions = @import("packet_simulator.zig").PacketSimulatorOptions;
const PacketSimulatorPath = @import("packet_simulator.zig").Path;

const log = std.log.scoped(.network);

pub const NetworkOptions = struct {
    packet_simulator_options: PacketSimulatorOptions,
};

pub const Network = struct {
    pub const Packet = struct {
        network: *Network,
        message: *Message,

        pub fn deinit(packet: *const Packet) void {
            packet.network.message_pool.unref(packet.message);
        }
    };

    pub const Path = struct {
        source: Process,
        target: Process,
    };

    allocator: std.mem.Allocator,

    options: NetworkOptions,
    packet_simulator: PacketSimulator(Packet),

    // TODO(Zig) If this stored a ?*MessageBus, then a process's bus could be set to `null` while
    // the replica is crashed, and replaced when it is destroy. Zig complains:
    //
    //   ./src/test/message_bus.zig:20:24: error: struct 'test.message_bus.MessageBus' depends on itself
    //   pub const MessageBus = struct {
    //                          ^
    //   ./src/test/message_bus.zig:21:5: note: while checking this field
    //       network: *Network,
    //       ^
    buses: std.ArrayListUnmanaged(*MessageBus),
    processes: std.ArrayListUnmanaged(u128),
    /// A pool of messages that are in the network (sent, but not yet delivered).
    message_pool: MessagePool,

    pub fn init(
        allocator: std.mem.Allocator,
        replica_count: u8,
        client_count: u8,
        options: NetworkOptions,
    ) !Network {
        const process_count = client_count + replica_count;
        assert(process_count <= std.math.maxInt(u8));

        var buses = try std.ArrayListUnmanaged(*MessageBus).initCapacity(allocator, process_count);
        errdefer buses.deinit(allocator);

        var processes = try std.ArrayListUnmanaged(u128).initCapacity(allocator, process_count);
        errdefer processes.deinit(allocator);

        var packet_simulator = try PacketSimulator(Packet).init(
            allocator,
            options.packet_simulator_options,
        );
        errdefer packet_simulator.deinit(allocator);

        // Count:
        // - replica → replica paths (excluding self-loops)
        // - replica → client paths
        // - client → replica paths
        // but not client→client paths; clients never message one another.
        const path_count = @as(usize, replica_count) * @as(usize, replica_count - 1) +
            2 * @as(usize, replica_count) * @as(usize, client_count);
        const message_pool = try MessagePool.init_capacity(
            allocator,
            // +1 so we can allocate an extra packet when all packet queues are at capacity,
            // so that `PacketSimulator.submit_packet` can choose which packet to drop.
            1 + @as(usize, options.packet_simulator_options.path_maximum_capacity) *
                path_count,
        );
        errdefer message_pool.deinit(allocator);

        return Network{
            .allocator = allocator,
            .options = options,
            .packet_simulator = packet_simulator,
            .buses = buses,
            .processes = processes,
            .message_pool = message_pool,
        };
    }

    pub fn deinit(network: *Network) void {
        network.buses.deinit(network.allocator);
        network.processes.deinit(network.allocator);
        network.packet_simulator.deinit(network.allocator);
        network.message_pool.deinit(network.allocator);
    }

    pub fn link(network: *Network, process: Process, message_bus: *MessageBus) void {
        const raw_process = switch (process) {
            .replica => |replica| replica,
            .client => |client| blk: {
                assert(client >= config.replicas_max);
                break :blk client;
            },
        };

        for (network.processes.items) |existing_process, i| {
            if (existing_process == raw_process) {
                network.buses.items[i] = message_bus;
                break;
            }
        } else {
            network.processes.appendAssumeCapacity(raw_process);
            network.buses.appendAssumeCapacity(message_bus);
        }
        assert(network.processes.items.len == network.buses.items.len);
    }

    pub fn send_message(network: *Network, message: *Message, path: Path) void {
        log.debug("send_message: {} > {}: {}", .{
            path.source,
            path.target,
            message.header.command,
        });

        const network_message = network.message_pool.get_message();
        defer network.message_pool.unref(network_message);

        util.copy_disjoint(.exact, u8, network_message.buffer, message.buffer);

        network.packet_simulator.submit_packet(
            .{
                .message = network_message.ref(),
                .network = network,
            },
            deliver_message,
            .{
                .source = network.process_to_address(path.source),
                .target = network.process_to_address(path.target),
            },
        );
    }

    fn process_to_address(network: *Network, process: Process) u8 {
        for (network.processes.items) |p, i| {
            if (std.meta.eql(raw_process_to_process(p), process)) return @intCast(u8, i);
        }
        unreachable;
    }

    pub fn get_message_bus(network: *Network, process: Process) *MessageBus {
        return network.buses.items[network.process_to_address(process)];
    }

    fn deliver_message(packet: Packet, path: PacketSimulatorPath) void {
        const network = packet.network;

        const target_bus = network.buses.items[path.target];
        const target_message = target_bus.get_message();
        defer target_bus.unref(target_message);

        util.copy_disjoint(.exact, u8, target_message.buffer, packet.message.buffer);

        const process_path = .{
            .source = raw_process_to_process(network.processes.items[path.source]),
            .target = raw_process_to_process(network.processes.items[path.target]),
        };

        log.debug("deliver_message: {} > {}: {}", .{
            process_path.source,
            process_path.target,
            packet.message.header.command,
        });

        if (target_message.header.command == .request or
            target_message.header.command == .prepare)
        {
            const sector_ceil = vsr.sector_ceil(target_message.header.size);
            if (target_message.header.size != sector_ceil) {
                assert(target_message.header.size < sector_ceil);
                assert(target_message.buffer.len == config.message_size_max + config.sector_size);
                mem.set(u8, target_message.buffer[target_message.header.size..sector_ceil], 0);
            }
        }

        target_bus.on_message_callback(target_bus, target_message);
    }

    fn raw_process_to_process(raw: u128) Process {
        switch (raw) {
            0...(config.replicas_max - 1) => return .{ .replica = @intCast(u8, raw) },
            else => {
                assert(raw >= config.replicas_max);
                return .{ .client = raw };
            },
        }
    }
};
