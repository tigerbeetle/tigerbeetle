const std = @import("std");
const mem = std.mem;
const assert = std.debug.assert;
const maybe = stdx.maybe;

const constants = @import("../../constants.zig");
const vsr = @import("../../vsr.zig");
const stdx = @import("stdx");
const Ratio = stdx.PRNG.Ratio;

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
    const PacketSimulator = PacketSimulatorType(*Message);

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
    pub const Core = stdx.BitSetType(constants.members_max);

    allocator: std.mem.Allocator,

    options: NetworkOptions,
    packet_simulator: PacketSimulator,

    buses: std.ArrayListUnmanaged(*MessageBus),
    buses_enabled: std.ArrayListUnmanaged(bool),
    processes: std.ArrayListUnmanaged(u128),
    /// A pool of messages that are in the network (sent, but not yet delivered).
    message_pool: MessagePool,
    message_summary: MessageSummary,

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

        var packet_simulator = try PacketSimulator.init(allocator, options, .{
            .packet_command = &packet_command,
            .packet_clone = &packet_clone,
            .packet_deinit = &packet_deinit,
            .packet_deliver = &packet_deliver,
        });
        errdefer packet_simulator.deinit(allocator);

        // Count:
        // - replica → replica paths (excluding self-loops)
        // - replica → client paths
        // - client → replica paths
        // but not client→client paths; clients never message one another.
        const node_count: u32 = options.node_count;
        const client_count: u32 = options.client_count;
        const path_count: u32 = node_count * (node_count - 1) + 2 * node_count * client_count;
        const message_pool = try MessagePool.init_capacity(
            allocator,
            // +1 so we can allocate an extra packet when all packet queues are at capacity,
            // so that `PacketSimulator.submit_packet` can choose which packet to drop.
            1 + options.path_maximum_capacity * path_count + options.recorded_count_max,
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
            .message_summary = .{},
        };
    }

    pub fn deinit(network: *Network) void {
        network.buses.deinit(network.allocator);
        network.buses_enabled.deinit(network.allocator);
        network.processes.deinit(network.allocator);
        network.packet_simulator.deinit(network.allocator);
        network.message_pool.deinit(network.allocator);
    }

    pub fn step(network: *Network) bool {
        var advanced = false;
        for (network.buses.items) |bus| {
            if (bus.resume_scheduled) {
                bus.resume_scheduled = false;
                bus.on_messages_callback(bus, &bus.buffer.?);
                if (bus.buffer.?.has_message()) {
                    bus.suspended = true;
                }
                advanced = true;
            }
        }
        return network.packet_simulator.step() or advanced;
    }

    pub fn tick(network: *Network) void {
        network.packet_simulator.tick();
    }

    pub fn transition_to_liveness_mode(network: *Network, core: Core) void {
        assert(core.count() > 0);

        network.packet_simulator.options.one_way_delay_min = .ms(1);
        network.packet_simulator.options.one_way_delay_mean = .ms(1);
        network.packet_simulator.options.packet_loss_probability = Ratio.zero();
        network.packet_simulator.options.packet_replay_probability = Ratio.zero();
        network.packet_simulator.options.partition_probability = Ratio.zero();
        network.packet_simulator.options.unpartition_probability = Ratio.zero();

        var it_source = core.iterate();
        while (it_source.next()) |replica_source| {
            var it_target = core.iterate();
            while (it_target.next()) |replica_target| {
                if (replica_target != replica_source) {
                    const path = Path{
                        .source = .{ .replica = @intCast(replica_source) },
                        .target = .{ .replica = @intCast(replica_target) },
                    };

                    // The Simulator doesn't use link_drop_packet_fn(), and replica_test.zig doesn't
                    // use transition_to_liveness_mode().
                    assert(network.link_drop_packet_fn(path).* == null);
                    network.link_filter(path).* = LinkFilter.initFull();
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

    pub fn link_clear(network: *Network, path: Path) void {
        network.packet_simulator.link_clear(.{
            .source = network.process_to_address(path.source),
            .target = network.process_to_address(path.target),
        });
    }

    pub fn link_filter(network: *Network, path: Path) *LinkFilter {
        return network.packet_simulator.link_filter(.{
            .source = network.process_to_address(path.source),
            .target = network.process_to_address(path.target),
        });
    }

    pub fn link_drop_packet_fn(network: *Network, path: Path) *?PacketSimulator.LinkDropPacketFn {
        return network.packet_simulator.link_drop_packet_fn(.{
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
        network.message_summary.add(message.header);
        log.debug("send_message: {} > {}: {}", .{
            path.source,
            path.target,
            message.header.command,
        });

        switch (message.header.peer_type()) {
            .unknown => {},
            .client_likely => |client_id| {
                // Requests may be forwarded by replicas, but peer_type always returns client ID,
                // as it is useful for the production MessageBus. Specifically, a replica that
                // receives a request from a client can immediately cache the connection in the
                // client map, instead of waiting for an infrequent PingClient message to do so.
                assert(message.header.command == .request);
                if (path.source == .client) assert(path.source.client == client_id);
            },
            .client => |client_id| assert(std.meta.eql(path.source, .{ .client = client_id })),
            .replica => |index| assert(std.meta.eql(path.source, .{ .replica = index })),
        }

        const network_message = network.message_pool.get_message(null);
        defer network.message_pool.unref(network_message);

        stdx.copy_disjoint(.exact, u8, network_message.buffer, message.buffer);

        network.packet_simulator.submit_packet(
            network_message.ref(),
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
                return @intCast(i);
            }
        }
        log.err("no such process: {} (have {any})", .{ process, network.processes.items });
        unreachable;
    }

    pub fn get_message_bus(network: *Network, process: Process) *MessageBus {
        return network.buses.items[network.process_to_address(process)];
    }

    fn packet_command(_: *PacketSimulator, message: *Message) vsr.Command {
        return message.header.command;
    }

    fn packet_clone(_: *PacketSimulator, message: *Message) *Message {
        return message.ref();
    }

    fn packet_deinit(packet_simulator: *PacketSimulator, message: *Message) void {
        const network: *Network = @fieldParentPtr("packet_simulator", packet_simulator);
        network.message_pool.unref(message);
    }

    fn packet_deliver(
        packet_simulator: *PacketSimulator,
        message: *Message,
        path: PacketSimulatorPath,
    ) void {
        const network: *Network = @fieldParentPtr("packet_simulator", packet_simulator);
        const process_path = .{
            .source = raw_process_to_process(network.processes.items[path.source]),
            .target = raw_process_to_process(network.processes.items[path.target]),
        };

        if (!network.buses_enabled.items[path.target]) {
            log.debug("deliver_message: {} > {}: {} (dropped; target is down)", .{
                process_path.source,
                process_path.target,
                message.header.command,
            });
            return;
        }

        log.debug("deliver_message: {} > {}: {}", .{
            process_path.source,
            process_path.target,
            message.header.command,
        });

        const target_bus = network.buses.items[path.target];
        assert(target_bus.buffer != null);

        if (target_bus.buffer.?.receive_size + message.header.size > constants.message_size_max) {
            log.debug("deliver_message: {} > {}: {} (dropped; buffer is full)", .{
                process_path.source,
                process_path.target,
                message.header.command,
            });
            return;
        }

        stdx.copy_disjoint(
            .inexact,
            u8,
            target_bus.buffer.?.recv_slice(),
            message.buffer[0..message.header.size],
        );
        target_bus.buffer.?.recv_advance(message.header.size);
        target_bus.on_messages_callback(target_bus, &target_bus.buffer.?);
        assert(target_bus.buffer != null);
        assert(target_bus.buffer.?.invalid == null);
        maybe(target_bus.buffer.?.receive_size > 0);
        maybe(target_bus.buffer.?.process_size > 0);
        if (target_bus.buffer.?.has_message()) {
            target_bus.suspended = true;
        }
    }

    fn raw_process_to_process(raw: u128) Process {
        switch (raw) {
            0...(constants.members_max - 1) => return .{ .replica = @intCast(raw) },
            else => {
                assert(raw >= constants.members_max);
                return .{ .client = raw };
            },
        }
    }
};

pub const MessageSummary = struct {
    map: Map = Map.initFill(.{ .count = 0, .size = 0 }),

    const Map = std.EnumArray(vsr.Command, struct { count: u32, size: u64 });

    pub fn add(summary: *MessageSummary, header: *const vsr.Header) void {
        const entry = summary.map.getPtr(header.command);
        entry.count += 1;
        entry.size += header.size;
    }

    pub fn format(
        summary: MessageSummary,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;

        const slice = comptime std.enums.values(vsr.Command);
        var commands = slice[0..slice.len].*;
        std.mem.sort(vsr.Command, &commands, summary.map, greater_than);

        var total_count: u32 = 0;
        var total_size: u64 = 0;

        for (commands) |command| {
            const message_summary = summary.map.get(command);
            total_count += message_summary.count;
            total_size += message_summary.size;
            if (message_summary.count > 0) {
                try writer.print("{s:<24} {d:>7} {:>10.2}\n", .{
                    @tagName(command),
                    message_summary.count,
                    std.fmt.fmtIntSizeBin(message_summary.size),
                });
            }
        }
        try writer.print("{s:<24} {d:>7} {:>10.2}\n", .{
            "total",
            total_count,
            std.fmt.fmtIntSizeBin(total_size),
        });
    }

    fn greater_than(map: Map, lhs: vsr.Command, rhs: vsr.Command) bool {
        return map.get(lhs).count > map.get(rhs).count;
    }
};
