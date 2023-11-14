const std = @import("std");
const math = std.math;
const mem = std.mem;
const assert = std.debug.assert;

const constants = @import("../../constants.zig");
const vsr = @import("../../vsr.zig");
const stdx = @import("../../stdx.zig");

const MessagePool = @import("../../message_pool.zig").MessagePool;
const Message = MessagePool.Message;

const MessageBus = @import("message_bus.zig").MessageBus;
const Process = @import("message_bus.zig").Process;

const PacketSimulatorType = @import("../packet_simulator.zig").PacketSimulatorType;
const PacketSimulatorOptions = @import("../packet_simulator.zig").PacketSimulatorOptions;
const PacketSimulatorPath = @import("../packet_simulator.zig").Path;

const log = std.log.scoped(.network);

pub const NetworkOptions = PacketSimulatorOptions;
pub const LinkFilter = @import("../packet_simulator.zig").LinkFilter;

pub const Network = struct {
    pub const Packet = struct {
        network: *Network,
        message: *Message,

        pub fn clone(packet: *const Packet) Packet {
            return Packet{
                .network = packet.network,
                .message = packet.message.ref(),
            };
        }

        pub fn deinit(packet: *const Packet) void {
            packet.network.message_pool.unref(packet.message);
        }

        pub fn command(packet: *const Packet) vsr.Command {
            return packet.message.header.command;
        }
    };

    pub const Path = struct {
        source: Process,
        target: Process,
    };

    /// Core is a strongly-connected component of replicas containing a view change quorum.
    /// It is used to define and check liveness --- if a core exists, it should converge
    /// to normal status in a bounded number of ticks.
    ///
    /// At the moment, we require core members to have direct bidirectional connectivity, but this
    /// could be relaxed in the future to indirect connectivity.
    pub const Core = std.StaticBitSet(constants.members_max);

    allocator: std.mem.Allocator,

    options: NetworkOptions,
    packet_simulator: PacketSimulatorType(Packet),

    // TODO(Zig) If this stored a ?*MessageBus, then a process's bus could be set to `null` while
    // the replica is crashed, and replaced when it is destroyed. But Zig complains:
    //
    //   ./src/test/message_bus.zig:20:24: error: struct 'test.message_bus.MessageBus' depends on itself
    //   pub const MessageBus = struct {
    //                          ^
    //   ./src/test/message_bus.zig:21:5: note: while checking this field
    //       network: *Network,
    //       ^
    buses: std.ArrayListUnmanaged(*MessageBus),
    buses_enabled: std.ArrayListUnmanaged(bool),
    processes: std.ArrayListUnmanaged(u128),
    /// A pool of messages that are in the network (sent, but not yet delivered).
    message_pool: MessagePool,

    pub fn init(
        allocator: std.mem.Allocator,
        options: NetworkOptions,
    ) !Network {
        const process_count = options.client_count + options.node_count;

        var buses = try std.ArrayListUnmanaged(*MessageBus).initCapacity(allocator, process_count);
        errdefer buses.deinit(allocator);

        var buses_enabled = try std.ArrayListUnmanaged(bool).initCapacity(allocator, process_count);
        errdefer buses_enabled.deinit(allocator);

        var processes = try std.ArrayListUnmanaged(u128).initCapacity(allocator, process_count);
        errdefer processes.deinit(allocator);

        var packet_simulator = try PacketSimulatorType(Packet).init(allocator, options);
        errdefer packet_simulator.deinit(allocator);

        // Count:
        // - replica → replica paths (excluding self-loops)
        // - replica → client paths
        // - client → replica paths
        // but not client→client paths; clients never message one another.
        const node_count = @as(usize, options.node_count);
        const client_count = @as(usize, options.client_count);
        const path_count = node_count * (node_count - 1) + 2 * node_count * client_count;
        const message_pool = try MessagePool.init_capacity(
            allocator,
            // +1 so we can allocate an extra packet when all packet queues are at capacity,
            // so that `PacketSimulator.submit_packet` can choose which packet to drop.
            1 + @as(usize, options.path_maximum_capacity) * path_count +
                options.recorded_count_max,
        );
        errdefer message_pool.deinit(allocator);

        return Network{
            .allocator = allocator,
            .options = options,
            .packet_simulator = packet_simulator,
            .buses = buses,
            .buses_enabled = buses_enabled,
            .processes = processes,
            .message_pool = message_pool,
        };
    }

    pub fn deinit(network: *Network) void {
        network.buses.deinit(network.allocator);
        network.buses_enabled.deinit(network.allocator);
        network.processes.deinit(network.allocator);
        network.packet_simulator.deinit(network.allocator);
        network.message_pool.deinit(network.allocator);
    }

    pub fn tick(network: *Network) void {
        network.packet_simulator.tick();
    }

    pub fn transition_to_liveness_mode(network: *Network, core: Core) void {
        assert(core.count() > 0);

        network.packet_simulator.options.packet_loss_probability = 0;
        network.packet_simulator.options.packet_replay_probability = 0;
        network.packet_simulator.options.partition_probability = 0;
        network.packet_simulator.options.unpartition_probability = 0;

        var it_source = core.iterator(.{});
        while (it_source.next()) |replica_source| {
            var it_target = core.iterator(.{});
            while (it_target.next()) |replica_target| {
                if (replica_target != replica_source) {
                    network.link_filter(.{
                        .source = .{ .replica = @as(u8, @intCast(replica_source)) },
                        .target = .{ .replica = @as(u8, @intCast(replica_target)) },
                    }).* = LinkFilter.initFull();
                }
            }
        }
    }

    pub fn link(network: *Network, process: Process, message_bus: *MessageBus) void {
        const raw_process = switch (process) {
            .replica => |replica| replica,
            .client => |client| blk: {
                assert(client >= constants.members_max);
                break :blk client;
            },
        };

        for (network.processes.items, 0..) |existing_process, i| {
            if (existing_process == raw_process) {
                network.buses.items[i] = message_bus;
                break;
            }
        } else {
            // PacketSimulator assumes that replicas go first.
            switch (process) {
                .replica => assert(network.processes.items.len < network.options.node_count),
                .client => assert(network.processes.items.len >= network.options.node_count),
            }
            network.processes.appendAssumeCapacity(raw_process);
            network.buses.appendAssumeCapacity(message_bus);
            network.buses_enabled.appendAssumeCapacity(true);
        }
        assert(network.processes.items.len == network.buses.items.len);
    }

    pub fn process_enable(network: *Network, process: Process) void {
        assert(!network.buses_enabled.items[network.process_to_address(process)]);
        network.buses_enabled.items[network.process_to_address(process)] = true;
    }

    pub fn process_disable(network: *Network, process: Process) void {
        assert(network.buses_enabled.items[network.process_to_address(process)]);
        network.buses_enabled.items[network.process_to_address(process)] = false;
    }

    pub fn link_filter(network: *Network, path: Path) *LinkFilter {
        return network.packet_simulator.link_filter(.{
            .source = network.process_to_address(path.source),
            .target = network.process_to_address(path.target),
        });
    }

    pub fn link_record(network: *Network, path: Path) *LinkFilter {
        return network.packet_simulator.link_record(.{
            .source = network.process_to_address(path.source),
            .target = network.process_to_address(path.target),
        });
    }

    pub fn replay_recorded(network: *Network) void {
        return network.packet_simulator.replay_recorded();
    }

    pub fn send_message(network: *Network, message: *Message, path: Path) void {
        log.debug("send_message: {} > {}: {}", .{
            path.source,
            path.target,
            message.header.command,
        });

        const network_message = network.message_pool.get_message(null);
        defer network.message_pool.unref(network_message);

        stdx.copy_disjoint(.exact, u8, network_message.buffer, message.buffer);

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

    fn process_to_address(network: *const Network, process: Process) u8 {
        for (network.processes.items, 0..) |p, i| {
            if (std.meta.eql(raw_process_to_process(p), process)) {
                switch (process) {
                    .replica => assert(i < network.options.node_count),
                    .client => assert(i >= network.options.node_count),
                }
                return @as(u8, @intCast(i));
            }
        }
        log.err("no such process: {} (have {any})", .{ process, network.processes.items });
        unreachable;
    }

    pub fn get_message_bus(network: *Network, process: Process) *MessageBus {
        return network.buses.items[network.process_to_address(process)];
    }

    fn deliver_message(packet: Packet, path: PacketSimulatorPath) void {
        const network = packet.network;
        const process_path = .{
            .source = raw_process_to_process(network.processes.items[path.source]),
            .target = raw_process_to_process(network.processes.items[path.target]),
        };

        if (!network.buses_enabled.items[path.target]) {
            log.debug("deliver_message: {} > {}: {} (dropped; target is down)", .{
                process_path.source,
                process_path.target,
                packet.message.header.command,
            });
            return;
        }

        const target_bus = network.buses.items[path.target];
        const target_message = target_bus.get_message(null);
        defer target_bus.unref(target_message);

        stdx.copy_disjoint(.exact, u8, target_message.buffer, packet.message.buffer);

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
                assert(target_message.buffer.len == constants.message_size_max);
                @memset(target_message.buffer[target_message.header.size..sector_ceil], 0);
            }
        }

        target_bus.on_message_callback(target_bus, target_message);
    }

    fn raw_process_to_process(raw: u128) Process {
        switch (raw) {
            0...(constants.members_max - 1) => return .{ .replica = @as(u8, @intCast(raw)) },
            else => {
                assert(raw >= constants.members_max);
                return .{ .client = raw };
            },
        }
    }
};
