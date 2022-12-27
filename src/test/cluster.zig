const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const constants = @import("../constants.zig");
const message_pool = @import("../message_pool.zig");
const MessagePool = message_pool.MessagePool;
const Message = MessagePool.Message;

pub const StateMachine = constants.StateMachineType(Storage, .{
    .message_body_size_max = constants.message_body_size_max,
});
const Storage = @import("storage.zig").Storage;
const Time = @import("time.zig").Time;
const IdPermutation = @import("id.zig").IdPermutation;

const MessageBus = @import("cluster/message_bus.zig").MessageBus;
const Network = @import("cluster/network.zig").Network;
const NetworkOptions = @import("cluster/network.zig").NetworkOptions;
const StateChecker = @import("cluster/state_checker.zig").StateChecker;
const StorageChecker = @import("cluster/storage_checker.zig").StorageChecker;

const vsr = @import("../vsr.zig");
pub const Replica = vsr.ReplicaType(StateMachine, MessageBus, Storage, Time);
pub const ReplicaFormat = vsr.ReplicaFormatType(Storage);
pub const Client = vsr.Client(StateMachine, MessageBus);
const SuperBlock = vsr.SuperBlockType(Storage);
const superblock_zone_size = @import("../vsr/superblock.zig").superblock_zone_size;

pub const ClusterOptions = struct {
    cluster_id: u32,
    replica_count: u8,
    client_count: u8,
    storage_size_limit: u64,

    seed: u64,
    /// Includes command=register messages.
    on_client_reply: fn(cluster: *Cluster, client: usize, request: *Message, reply: *Message) void,

    network_options: NetworkOptions,
    storage_options: Storage.Options,
    health_options: HealthOptions,
    state_machine_options: StateMachine.Options,
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

/// Integer values represent exit codes.
// TODO This doesn't really belong in Cluster, but it is needed here so that StateChecker failures
// use the particular exit code.
pub const Failure = enum(u8) {
    //ok = 0,
    /// Any assertion crash will be given an exit code of 127 by default.
    crash = 127,
    liveness = 128,
    correctness = 129,
};

/// Shift the id-generating index because the simulator network expects client ids to never collide
/// with a replica index.
const client_id_permutation_shift = constants.replicas_max;

pub const Cluster = struct {
    allocator: mem.Allocator,
    options: ClusterOptions,

    storages: []Storage,
    replicas: []Replica,
    replica_pools: []MessagePool,
    replica_health: []ReplicaHealth,

    clients: []Client,
    client_pools: []MessagePool,
    client_id_permutation: IdPermutation,

    network: *Network,
    state_checker: StateChecker,
    storage_checker: StorageChecker,

    pub fn create(allocator: mem.Allocator, options: ClusterOptions) !*Cluster {
        assert(options.replica_count >= 1);
        assert(options.replica_count <= 6);
        assert(options.client_count > 0);
        assert(options.storage_size_limit % constants.sector_size == 0);
        assert(options.storage_size_limit <= constants.storage_size_max);
        assert(options.health_options.crash_probability < 1.0);
        assert(options.health_options.crash_probability >= 0.0);
        assert(options.health_options.restart_probability < 1.0);
        assert(options.health_options.restart_probability >= 0.0);

        var prng = std.rand.DefaultPrng.init(options.seed);
        const random = prng.random();

        const cluster = try allocator.create(Cluster);
        errdefer allocator.destroy(cluster);

        // TODO(Zig) Client.init()'s MessagePool.Options require a reference to the network — use
        // @returnAddress() instead.
        var network = try allocator.create(Network);
        errdefer allocator.destroy(network);

        network.* = try Network.init(
            allocator,
            options.replica_count,
            options.client_count,
            options.network_options,
        );
        errdefer network.deinit();

        const storages = try allocator.alloc(Storage, options.replica_count);
        errdefer allocator.free(storages);

        var replica_pools = try allocator.alloc(MessagePool, options.replica_count);
        errdefer allocator.free(replica_pools);

        for (replica_pools) |*pool, i| {
            errdefer for (replica_pools[0..i]) |*p| p.deinit(allocator);
            pool.* = try MessagePool.init(allocator, .replica);
        }
        errdefer for (replica_pools) |*pool| pool.deinit(allocator);

        const replicas = try allocator.alloc(Replica, options.replica_count);
        errdefer allocator.free(replicas);

        const replica_health = try allocator.alloc(ReplicaHealth, options.replica_count);
        errdefer allocator.free(replica_health);
        mem.set(ReplicaHealth, replica_health, .{ .up = 0 });

        var client_pools = try allocator.alloc(MessagePool, options.client_count);
        errdefer allocator.free(client_pools);

        for (client_pools) |*pool, i| {
            errdefer for (client_pools[0..i]) |*p| p.deinit(allocator);
            pool.* = try MessagePool.init(allocator, .client);
        }
        errdefer for (replica_pools) |*pool| pool.deinit(allocator);

        const client_id_permutation = IdPermutation.generate(random);
        var clients = try allocator.alloc(Client, options.client_count);
        errdefer allocator.free(clients);

        for (clients) |*client, i| {
            errdefer for (clients[0..i]) |*c| c.deinit(allocator);
            client.* = try Client.init(
                allocator,
                client_id_permutation.encode(i + client_id_permutation_shift),
                options.cluster_id,
                options.replica_count,
                &client_pools[i],
                .{ .network = network },
            );
            network.link(client.message_bus.process, &client.message_bus);
        }
        errdefer for (clients) |*c| c.deinit(allocator);

        var state_checker =
            try StateChecker.init(allocator, options.cluster_id, replicas, clients);
        errdefer state_checker.deinit();

        var storage_checker = StorageChecker.init(allocator);
        errdefer storage_checker.deinit();

        cluster.* = .{
            .allocator = allocator,
            .options = options,
            .storages = storages,
            .replicas = replicas,
            .replica_pools = replica_pools,
            .replica_health = replica_health,
            .clients = clients,
            .client_pools = client_pools,
            .client_id_permutation = client_id_permutation,
            .network = network,
            .state_checker = state_checker,
            .storage_checker = storage_checker,
        };

        var buffer: [constants.replicas_max]Storage.FaultyAreas = undefined;
        const faulty_wal_areas = Storage.generate_faulty_wal_areas( // TODO
            random,
            constants.journal_size_max,
            options.replica_count,
            &buffer,
        );
        assert(faulty_wal_areas.len == options.replica_count);

        for (cluster.storages) |*storage, replica_index| {
            var storage_options = options.storage_options;
            storage_options.replica_index = @intCast(u8, replica_index);
            storage_options.faulty_wal_areas = faulty_wal_areas[replica_index];
            storage.* = try Storage.init(allocator, options.storage_size_limit, storage_options);
            // Disable most faults at startup, so that the replicas don't get stuck in recovery mode.
            storage.faulty = replica_index >= vsr.quorums(options.replica_count).view_change;
        }
        errdefer for (cluster.storages) |*storage| storage.deinit(allocator);

        // Format each replica's storage (equivalent to "tigerbeetle format ...").
        for (cluster.storages) |*storage, replica_index| {
            var superblock = try SuperBlock.init(allocator, .{
                .storage = storage,
                .message_pool = &cluster.replica_pools[replica_index],
                .storage_size_limit = options.storage_size_limit,
            });
            defer superblock.deinit(allocator);

            try vsr.format(
                Storage,
                allocator,
                options.cluster_id,
                @intCast(u8, replica_index),
                storage,
                &superblock,
            );
        }

        for (cluster.replicas) |_, replica_index| {
            try cluster.open_replica(@intCast(u8, replica_index), .{
                .resolution = constants.tick_ms * std.time.ns_per_ms,
                .offset_type = .linear,
                .offset_coefficient_A = 0,
                .offset_coefficient_B = 0,
            });
        }
        errdefer for (cluster.replicas) |*replica| replica.deinit(allocator);

        return cluster;
    }

    pub fn destroy(cluster: *Cluster) void {
        cluster.storage_checker.deinit();
        cluster.state_checker.deinit();
        cluster.network.deinit();
        for (cluster.clients) |*client| client.deinit(cluster.allocator);
        for (cluster.client_pools) |*pool| pool.deinit(cluster.allocator);
        for (cluster.replicas) |*replica| replica.deinit(cluster.allocator);
        for (cluster.replica_pools) |*pool| pool.deinit(cluster.allocator);
        for (cluster.storages) |*storage| storage.deinit(cluster.allocator);

        cluster.allocator.free(cluster.clients);
        cluster.allocator.free(cluster.client_pools);
        cluster.allocator.free(cluster.replicas);
        cluster.allocator.free(cluster.replica_health);
        cluster.allocator.free(cluster.replica_pools);
        cluster.allocator.free(cluster.storages);
        cluster.allocator.destroy(cluster.network);
        cluster.allocator.destroy(cluster);
    }

    pub fn tick(cluster: *Cluster) void {
        cluster.network.tick();

        for (cluster.clients) |*client| client.tick();
        for (cluster.storages) |*storage| storage.tick();
        for (cluster.replicas) |*replica, i| {
            switch (cluster.replica_health[i]) {
                .up => replica.tick(),
                // Keep ticking the time so that it won't have diverged too far to synchronize
                // when the replica restarts.
                .down => replica.clock.time.tick(),
            }
            on_replica_change_state(replica);
        }
    }

    pub fn restart_replica(cluster: *Cluster, replica_index: u8) void {
        assert(cluster.replica_health[replica_index] == .down);

        cluster.network.packet_simulator.fault_replica(replica_index, .enabled);
        cluster.replica_health[replica_index] = .{
            .up = cluster.options.health_options.restart_stability,
        };
    }

    /// Reset a replica to its initial state, simulating a random crash/panic.
    /// Leave the persistent storage untouched, and leave any currently
    /// inflight messages to/from the replica in the network.
    ///
    /// Returns whether the replica was crashed.
    pub fn crash_replica(cluster: *Cluster, replica_index: u8) !bool {
        assert(cluster.replica_health[replica_index] == .up);

        const replica = &cluster.replicas[replica_index];
        if (replica.op == 0) {
            // Only crash when `replica.op > 0` — an empty WAL would skip recovery after a crash.
            return false;
        }

        // TODO Remove this workaround when VSR recovery protocol is disabled.
        for (replica.journal.prepare_inhabited) |inhabited, i| {
            if (i == 0) {
                // Ignore the root header.
            } else {
                if (inhabited) break;
            }
        } else {
            // Only crash when at least one header has been written to the WAL.
            // An empty WAL would skip recovery after a crash.
            return false;
        }

        // Ensure that the cluster can eventually recover without this replica.
        // Verify that each op is recoverable by the current healthy cluster (minus the replica we
        // are trying to crash).
        // TODO Remove this workaround when VSR recovery protocol is disabled.
        if (cluster.options.replica_count != 1) {
            var parent: u128 = undefined;
            const cluster_op_max = op_max: {
                var v: ?u32 = null;
                var op_max: ?u64 = null;
                for (cluster.replicas) |other_replica, i| {
                    if (cluster.replica_health[i] == .down) continue;
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

            // This whole workaround doesn't handle log wrapping correctly.
            // If the log has wrapped, don't crash the replica.
            if (cluster_op_max >= constants.journal_slot_count) {
                return false;
            }

            var op: u64 = cluster_op_max + 1;
            while (op > 0) {
                op -= 1;

                var cluster_op_known: bool = false;
                for (cluster.replicas) |other_replica, i| {
                    // Ignore replicas that are ineligible to assist recovery.
                    if (replica_index == i) continue;
                    if (cluster.replica_health[i] == .down) continue;
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

        cluster.replica_health[replica_index] = .{ .down = cluster.options.health_options.crash_stability };

        // Reset the storage before the replica so that pending writes can (partially) finish.
        cluster.storages[replica_index].reset();
        const replica_time = replica.time;
        replica.deinit(cluster.allocator);
        cluster.network.packet_simulator.fault_replica(replica_index, .disabled);

        // Ensure that none of the replica's messages leaked when it was deinitialized.
        var messages_in_pool: usize = 0;
        const message_bus = cluster.network.get_message_bus(.{ .replica = replica_index });
        {
            var it = message_bus.pool.free_list;
            while (it) |message| : (it = message.next) messages_in_pool += 1;
        }
        assert(messages_in_pool == message_pool.messages_max_replica);

        // Logically it would make more sense to run this during restart, not immediately following
        // the crash. But having it here allows the replica's MessageBus to initialize and begin
        // queueing packets.
        //
        // Pass the old replica's Time through to the new replica. It will continue to be tick
        // while the replica is crashed, to ensure the clocks don't desyncronize too far to recover.
        try cluster.open_replica(replica_index, replica_time);

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
        for (cluster.replica_health) |health| {
            if (health == .up) {
                count += 1;
            }
        }
        return count;
    }

    fn open_replica(cluster: *Cluster, replica_index: u8, time: Time) !void {
        var replica = &cluster.replicas[replica_index];
        try replica.open(
            cluster.allocator,
            .{
                .replica_count = @intCast(u8, cluster.replicas.len),
                .storage = &cluster.storages[replica_index],
                // TODO Test restarting with a higher storage limit.
                .storage_size_limit = cluster.options.storage_size_limit,
                .message_pool = &cluster.replica_pools[replica_index],
                .time = time,
                .state_machine_options = cluster.options.state_machine_options,
                .message_bus_options = .{ .network = cluster.network },
            },
        );
        assert(replica.cluster == cluster.options.cluster_id);
        assert(replica.replica == replica_index);
        assert(replica.replica_count == cluster.replicas.len);

        replica.context = cluster;
        replica.on_change_state = on_replica_change_state;
        replica.on_compact = on_replica_compact;
        replica.on_checkpoint = on_replica_checkpoint;
        cluster.network.link(replica.message_bus.process, &replica.message_bus);
    }

    pub fn request(
        cluster: *Cluster,
        client_index: usize,
        request_operation: StateMachine.Operation,
        request_message: *Message,
        request_body_size: usize,
    ) void {
        // TODO(Zig) Move these into init when `@returnAddress()` is available. They only needs to
        // be set once, it just requires a stable pointer to the Cluster.
        cluster.clients[client_index].on_reply_context = cluster;
        cluster.clients[client_index].on_reply_callback = client_on_reply;

        cluster.clients[client_index].request(
            undefined,
            request_callback,
            request_operation,
            request_message,
            request_body_size,
        );
    }

    /// The `request_callback` is not used — Cluster uses `Client.on_reply_{context,callback}`
    /// instead because:
    /// - Cluster needs access to the request
    /// - Cluster needs access to the reply message (not just the body)
    /// - Cluster needs to know about command=register messages
    ///
    /// See `on_reply`.
    fn request_callback(
        user_data: u128,
        operation: StateMachine.Operation,
        result: Client.Error![]const u8,
    ) void {
        _ = user_data;
        _ = operation;
        _ = result catch |err| switch (err) {
            error.TooManyOutstandingRequests => unreachable,
        };
    }

    fn client_on_reply(client: *Client, request_message: *Message, reply_message: *Message) void {
        const cluster = @ptrCast(*Cluster, @alignCast(@alignOf(Cluster), client.on_reply_context.?));
        assert(reply_message.header.cluster == cluster.options.cluster_id);
        assert(reply_message.header.invalid() == null);
        assert(reply_message.header.client == client.id);
        assert(reply_message.header.request == request_message.header.request);
        assert(reply_message.header.command == .reply);
        assert(reply_message.header.operation == request_message.header.operation);

        const client_index = for (cluster.clients) |*c, i| {
            if (client == c) break i;
        } else unreachable;

        cluster.options.on_client_reply(cluster, client_index, request_message, reply_message);
    }
};

fn on_replica_change_state(replica: *const Replica) void {
    const cluster = @ptrCast(*Cluster, @alignCast(@alignOf(Cluster), replica.context.?));
    cluster.state_checker.check_state(replica.replica) catch |err| {
        fatal(.correctness, "state checker error: {}", .{err});
    };
}

fn on_replica_compact(replica: *const Replica) void {
    const cluster = @ptrCast(*Cluster, @alignCast(@alignOf(Cluster), replica.context.?));
    cluster.storage_checker.replica_compact(replica) catch |err| {
        fatal(.correctness, "storage checker error: {}", .{err});
    };
}

fn on_replica_checkpoint(replica: *const Replica) void {
    const cluster = @ptrCast(*Cluster, @alignCast(@alignOf(Cluster), replica.context.?));
    cluster.storage_checker.replica_checkpoint(replica) catch |err| {
        fatal(.correctness, "storage checker error: {}", .{err});
    };
}

/// Print an error message and then exit with an exit code.
fn fatal(failure: Failure, comptime fmt_string: []const u8, args: anytype) noreturn {
    std.log.scoped(.state_checker).err(fmt_string, args);
    std.os.exit(@enumToInt(failure));
}
