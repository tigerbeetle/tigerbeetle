const std = @import("std");
const mem = std.mem;

const config = @import("../config.zig");

const StateChecker = @import("state_checker.zig").StateChecker;

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
    replicas: []Replica,

    clients: []Client,
    network: Network,

    state_checker: StateChecker = undefined,

    pub fn create(allocator: *mem.Allocator, prng: *std.rand.Random, options: ClusterOptions) !*Cluster {
        const cluster = try allocator.create(Cluster);
        errdefer allocator.destroy(cluster);

        {
            const state_machines = try allocator.alloc(StateMachine, options.replica_count);
            errdefer allocator.free(state_machines);

            const storages = try allocator.alloc(Storage, options.replica_count);
            errdefer allocator.free(storages);

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
                .replicas = replicas,
                .clients = clients,
                .network = network,
            };
        }

        var buffer: [config.replicas_max]Storage.FaultyAreas = undefined;
        const faulty_areas = Storage.generate_faulty_areas(prng, config.journal_size_max, options.replica_count, &buffer);

        for (cluster.replicas) |*replica, replica_index| {
            const time: Time = .{
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
                time,
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

        for (cluster.replicas) |*replica| replica.deinit();
        cluster.allocator.free(cluster.replicas);

        for (cluster.state_machines) |*state_machine| state_machine.deinit();
        cluster.allocator.free(cluster.state_machines);

        for (cluster.storages) |*storage| storage.deinit(cluster.allocator);
        cluster.allocator.free(cluster.storages);

        cluster.network.deinit();

        cluster.allocator.destroy(cluster);
    }
};
