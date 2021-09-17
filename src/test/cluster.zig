const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const config = @import("../config.zig");

const StateChecker = @import("state_checker.zig").StateChecker;

const MessagePool = @import("../message_pool.zig").MessagePool;
const Message = MessagePool.Message;

const Network = @import("network.zig").Network;
const NetworkOptions = @import("network.zig").NetworkOptions;

pub const StateMachine = @import("state_machine.zig").StateMachine;
const MessageBus = @import("message_bus.zig").MessageBus;
const Storage = @import("storage.zig").Storage;
const Time = @import("time.zig").Time;

const vsr = @import("../vsr.zig");
pub const Replica = vsr.Replica(StateMachine, MessageBus, Storage, Time);
pub const Client = vsr.Client(StateMachine, MessageBus);

pub const ClusterOptions = struct {
    cluster: u32,

    replica_count: u8,
    client_count: u8,

    seed: u64,

    network_options: NetworkOptions,
    storage_options: Storage.Options,
};

pub const Cluster = struct {
    allocator: *mem.Allocator,
    options: ClusterOptions,

    state_machines: []StateMachine,
    storages: []Storage,
    times: []Time,
    replicas: []Replica,

    clients: []Client,
    network: Network,

    // TODO: Initializing these fields in main() is a bit ugly
    state_checker: StateChecker = undefined,
    on_change_state: fn (replica: *Replica) void = undefined,

    pub fn create(allocator: *mem.Allocator, prng: *std.rand.Random, options: ClusterOptions) !*Cluster {
        const cluster = try allocator.create(Cluster);
        errdefer allocator.destroy(cluster);

        {
            const state_machines = try allocator.alloc(StateMachine, options.replica_count);
            errdefer allocator.free(state_machines);

            const storages = try allocator.alloc(Storage, options.replica_count);
            errdefer allocator.free(storages);

            const times = try allocator.alloc(Time, options.replica_count);
            errdefer allocator.free(times);

            const replicas = try allocator.alloc(Replica, options.replica_count);
            errdefer allocator.free(replicas);

            const clients = try allocator.alloc(Client, options.client_count);
            errdefer allocator.free(clients);

            const network = try Network.init(
                allocator,
                options.replica_count,
                options.client_count,
                options.network_options,
            );
            errdefer network.deinit();

            cluster.* = .{
                .allocator = allocator,
                .options = options,
                .state_machines = state_machines,
                .storages = storages,
                .times = times,
                .replicas = replicas,
                .clients = clients,
                .network = network,
            };
        }

        var buffer: [config.replicas_max]Storage.FaultyAreas = undefined;
        const faulty_areas = Storage.generate_faulty_areas(prng, config.journal_size_max, options.replica_count, &buffer);

        for (cluster.replicas) |*replica, replica_index| {
            cluster.times[replica_index] = .{
                .resolution = config.tick_ms * std.time.ns_per_ms,
                .offset_type = .linear,
                .offset_coefficient_A = 0,
                .offset_coefficient_B = 0,
            };
            cluster.state_machines[replica_index] = StateMachine.init(options.seed);
            cluster.storages[replica_index] = try Storage.init(
                allocator,
                config.journal_size_max,
                options.storage_options,
                @intCast(u8, replica_index),
                faulty_areas[replica_index],
            );
            const message_bus = try cluster.network.init_message_bus(
                options.cluster,
                .{ .replica = @intCast(u8, replica_index) },
            );

            replica.* = try Replica.init(
                allocator,
                options.cluster,
                options.replica_count,
                @intCast(u8, replica_index),
                &cluster.times[replica_index],
                &cluster.storages[replica_index],
                message_bus,
                &cluster.state_machines[replica_index],
            );
            message_bus.set_on_message(*Replica, replica, Replica.on_message);
        }

        for (cluster.clients) |*client| {
            const client_id = prng.int(u128);
            const client_message_bus = try cluster.network.init_message_bus(
                options.cluster,
                .{ .client = client_id },
            );
            client.* = try Client.init(
                allocator,
                client_id,
                options.cluster,
                options.replica_count,
                client_message_bus,
            );
            client_message_bus.set_on_message(*Client, client, Client.on_message);
        }

        return cluster;
    }

    pub fn destroy(cluster: *Cluster) void {
        for (cluster.clients) |*client| client.deinit();
        cluster.allocator.free(cluster.clients);

        for (cluster.replicas) |*replica| replica.deinit(cluster.allocator);
        cluster.allocator.free(cluster.replicas);

        for (cluster.storages) |*storage| storage.deinit(cluster.allocator);
        cluster.allocator.free(cluster.storages);

        cluster.network.deinit();

        cluster.allocator.destroy(cluster);
    }

    /// Reset a replica to its initial state, simulating a random crash/panic.
    /// Leave the persistent storage untouched, and leave any currently
    /// inflight messages to/from the replica in the network.
    pub fn simulate_replica_crash(cluster: *Cluster, replica_index: u8) !void {
        const replica = &cluster.replicas[replica_index];
        replica.deinit(cluster.allocator);

        cluster.storages[replica_index].reset();
        cluster.state_machines[replica_index] = StateMachine.init(cluster.options.seed);

        // The message bus and network should be left alone, as messages
        // may still be inflight to/from this replica. However, we should
        // do a check to ensure that we aren't leaking any messages when
        // deinitializing the replica above.
        const packet_simulator = &cluster.network.packet_simulator;
        // The same message may be used for multiple network packets, so simply counting how
        // many packets are inflight from the replica is insufficient, we need to dedup them.
        var messages_in_network_set = std.AutoHashMap(*Message, void).init(cluster.allocator);
        defer messages_in_network_set.deinit();

        var target: u8 = 0;
        while (target < packet_simulator.options.node_count) : (target += 1) {
            const path = .{ .source = replica_index, .target = target };
            const queue = packet_simulator.path_queue(path);
            var it = queue.iterator();
            while (it.next()) |data| {
                try messages_in_network_set.put(data.packet.message, {});
            }
        }

        const messages_in_network = messages_in_network_set.count();

        var messages_in_pool: usize = 0;
        const message_bus = cluster.network.get_message_bus(.{ .replica = replica_index });
        {
            var it = message_bus.pool.free_list;
            while (it) |message| : (it = message.next) messages_in_pool += 1;
        }
        {
            var it = message_bus.pool.header_only_free_list;
            while (it) |message| : (it = message.next) messages_in_pool += 1;
        }

        const total_messages = config.message_bus_messages_max + config.message_bus_headers_max;
        assert(messages_in_network + messages_in_pool == total_messages);

        replica.* = try Replica.init(
            cluster.allocator,
            cluster.options.cluster,
            cluster.options.replica_count,
            @intCast(u8, replica_index),
            &cluster.times[replica_index],
            &cluster.storages[replica_index],
            message_bus,
            &cluster.state_machines[replica_index],
        );
        message_bus.set_on_message(*Replica, replica, Replica.on_message);
        replica.on_change_state = cluster.on_change_state;
    }
};
