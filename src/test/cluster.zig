const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const config = @import("../config.zig");

const StateChecker = @import("state_checker.zig").StateChecker;

const message_pool = @import("../message_pool.zig");
const MessagePool = message_pool.MessagePool;
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
    health_options: HealthOptions,
};

pub const HealthOptions = struct {
    /// Probability per tick that a crash will occur.
    crash_probability: f64,
    /// Minimum duration of a crash.
    crash_stability: u32,
    /// Probability per tick that a crashed replica will recovery.
    restart_probability: f64,
    /// Minimum time a replica is up until it is crashed again.
    restart_stability: u32,
};

pub const ReplicaHealth = union(enum) {
    /// When >0, the replica cannot crash.
    /// When =0, the replica may crash.
    up: u32,
    /// When >0, this is the ticks remaining until recovery is possible.
    /// When =0, the replica may recover.
    down: u32,
};

pub const Cluster = struct {
    allocator: mem.Allocator,
    options: ClusterOptions,

    state_machines: []StateMachine,
    storages: []Storage,
    times: []Time,
    replicas: []Replica,
    health: []ReplicaHealth,

    clients: []Client,
    network: Network,

    // TODO: Initializing these fields in main() is a bit ugly
    state_checker: StateChecker = undefined,
    on_change_state: fn (replica: *Replica) void = undefined,

    pub fn create(allocator: mem.Allocator, prng: std.rand.Random, options: ClusterOptions) !*Cluster {
        assert(options.replica_count > 0);
        assert(options.health_options.crash_probability < 1.0);
        assert(options.health_options.crash_probability >= 0.0);
        assert(options.health_options.restart_probability < 1.0);
        assert(options.health_options.restart_probability >= 0.0);

        const cluster = try allocator.create(Cluster);
        errdefer allocator.destroy(cluster);

        const state_machines = try allocator.alloc(StateMachine, options.replica_count);
        errdefer allocator.free(state_machines);

        const storages = try allocator.alloc(Storage, options.replica_count);
        errdefer allocator.free(storages);

        const times = try allocator.alloc(Time, options.replica_count);
        errdefer allocator.free(times);

        const replicas = try allocator.alloc(Replica, options.replica_count);
        errdefer allocator.free(replicas);

        const health = try allocator.alloc(ReplicaHealth, options.replica_count);
        errdefer allocator.free(health);
        mem.set(ReplicaHealth, health, .{ .up = 0 });

        const clients = try allocator.alloc(Client, options.client_count);
        errdefer allocator.free(clients);

        var network = try Network.init(
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
            .health = health,
            .clients = clients,
            .network = network,
        };

        var buffer: [config.replicas_max]Storage.FaultyAreas = undefined;
        const faulty_areas = Storage.generate_faulty_areas(prng, config.journal_size_max, options.replica_count, &buffer);
        assert(faulty_areas.len == options.replica_count);

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

        {
            // Format the WAL (equivalent to "tigerbeetle init ...").
            for (cluster.storages) |storage| {
                const write_size = vsr.format_journal(options.cluster, 0, storage.memory);
                assert(write_size == storage.memory.len);
                assert(write_size == config.journal_size_max);
            }
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
        cluster.allocator.free(cluster.health);

        for (cluster.storages) |*storage| storage.deinit(cluster.allocator);
        cluster.allocator.free(cluster.storages);

        cluster.network.deinit();

        cluster.allocator.destroy(cluster);
    }

    /// Reset a replica to its initial state, simulating a random crash/panic.
    /// Leave the persistent storage untouched, and leave any currently
    /// inflight messages to/from the replica in the network.
    ///
    /// Returns whether the replica was crashed.
    pub fn crash_replica(cluster: *Cluster, replica_index: u8) !bool {
        const replica = &cluster.replicas[replica_index];
        if (replica.op == 0) {
            // Only crash when `replica.op > 0` — an empty WAL would skip recovery after a crash.
            return false;
        }

        // Ensure that the replica can eventually recover without this replica.
        // Verify that each op is recoverable by the current healthy cluster (minus the replica we
        // are trying to crash).
        // TODO Remove this workaround when VSR recovery protocol is disabled.
        if (cluster.options.replica_count != 1) {
            var parent: u128 = undefined;
            const cluster_op_max = op_max: {
                var v: ?u32 = null;
                var op_max: ?u64 = null;
                for (cluster.replicas) |other_replica, i| {
                    if (cluster.health[i] == .down) continue;
                    if (other_replica.status == .recovering) continue;

                    if (v == null or other_replica.view_normal > v.? or
                        (other_replica.view_normal == v.? and other_replica.op > op_max.?))
                    {
                        v = other_replica.view_normal;
                        op_max = other_replica.op;
                        parent = other_replica.journal.header_with_op(op_max.?).?.checksum;
                    }
                }
                break :op_max op_max.?;
            };

            // TODO This workaround doesn't handle log wrapping correctly.
            assert(cluster_op_max < config.journal_slot_count);

            var op: u64 = cluster_op_max + 1;
            while (op > 0) {
                op -= 1;

                var cluster_op_known: bool = false;
                for (cluster.replicas) |other_replica, i| {
                    // Ignore replicas that are ineligible to assist recovery.
                    if (replica_index == i) continue;
                    if (cluster.health[i] == .down) continue;
                    if (other_replica.status == .recovering) continue;

                    if (other_replica.journal.header_with_op_and_checksum(op, parent)) |header| {
                        parent = header.parent;
                        if (!other_replica.journal.dirty.bit(.{ .index = op })) {
                            // The op is recoverable if this replica crashes.
                            break;
                        }
                        cluster_op_known = true;
                    }
                } else {
                    if (op == cluster_op_max and !cluster_op_known) {
                        // The replica can crash; it will be able to truncate the last op.
                    } else {
                        // The op isn't recoverable if this replica is crashed.
                        return false;
                    }
                }
            }

            // We can't crash this replica because without it we won't be able to repair a broken
            // hash chain.
            if (parent != 0) return false;
        }

        cluster.health[replica_index] = .{ .down = cluster.options.health_options.crash_stability };

        // Reset the storage before the replica so that pending writes can (partially) finish.
        cluster.storages[replica_index].reset();
        replica.deinit(cluster.allocator);
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

        const total_messages = message_pool.messages_max_replica;
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
        return true;
    }

    /// Returns the number of replicas capable of helping a crashed node recover (i.e. with
    /// replica.status=normal).
    pub fn replica_normal_count(cluster: *Cluster) u8 {
        var count: u8 = 0;
        for (cluster.replicas) |*replica| {
            if (replica.status == .normal) count += 1;
        }
        return count;
    }

    pub fn replica_up_count(cluster: *const Cluster) u8 {
        var count: u8 = 0;
        for (cluster.health) |health| {
            if (health == .up) {
                count += 1;
            }
        }
        return count;
    }
};
