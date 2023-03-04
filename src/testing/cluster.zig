const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const constants = @import("../constants.zig");
const message_pool = @import("../message_pool.zig");
const MessagePool = message_pool.MessagePool;
const Message = MessagePool.Message;

const Storage = @import("storage.zig").Storage;
const StorageFaultAtlas = @import("storage.zig").ClusterFaultAtlas;
const Time = @import("time.zig").Time;
const IdPermutation = @import("id.zig").IdPermutation;

const MessageBus = @import("cluster/message_bus.zig").MessageBus;
const Network = @import("cluster/network.zig").Network;
const NetworkOptions = @import("cluster/network.zig").NetworkOptions;
const StateCheckerType = @import("cluster/state_checker.zig").StateCheckerType;
const StorageCheckerType = @import("cluster/storage_checker.zig").StorageCheckerType;

const vsr = @import("../vsr.zig");
pub const ReplicaFormat = vsr.ReplicaFormatType(Storage);
const SuperBlock = vsr.SuperBlockType(Storage);
const superblock_zone_size = @import("../vsr/superblock.zig").superblock_zone_size;

pub const ReplicaHealth = enum { up, down };

/// Integer values represent exit codes.
// TODO This doesn't really belong in Cluster, but it is needed here so that StateChecker failures
// use the particular exit code.
pub const Failure = enum(u8) {
    /// Any assertion crash will be given an exit code of 127 by default.
    crash = 127,
    liveness = 128,
    correctness = 129,
};

/// Shift the id-generating index because the simulator network expects client ids to never collide
/// with a replica index.
const client_id_permutation_shift = constants.nodes_max;

pub fn ClusterType(comptime StateMachineType: fn (comptime Storage: type, comptime constants: anytype) type) type {
    return struct {
        const Self = @This();

        pub const StateMachine = StateMachineType(Storage, .{
            .message_body_size_max = constants.message_body_size_max,
            .lsm_batch_multiple = constants.lsm_batch_multiple,
        });
        pub const Replica = vsr.ReplicaType(StateMachine, MessageBus, Storage, Time);
        pub const Client = vsr.Client(StateMachine, MessageBus);
        pub const StateChecker = StateCheckerType(Client, Replica);
        pub const StorageChecker = StorageCheckerType(Replica);

        pub const Options = struct {
            cluster_id: u32,
            replica_count: u8,
            standby_count: u8,
            client_count: u8,
            storage_size_limit: u64,
            storage_fault_atlas: StorageFaultAtlas.Options,
            seed: u64,

            network: NetworkOptions,
            storage: Storage.Options,
            state_machine: StateMachine.Options,
        };

        allocator: mem.Allocator,
        options: Options,
        on_client_reply: fn (
            cluster: *Self,
            client: usize,
            request: *Message,
            reply: *Message,
        ) void,

        network: *Network,
        storages: []Storage,
        storage_fault_atlas: *StorageFaultAtlas,
        /// NB: includes both active replicas and standbys.
        replicas: []Replica,
        replica_pools: []MessagePool,
        replica_health: []ReplicaHealth,
        replica_count: u8,
        standby_count: u8,

        clients: []Client,
        client_pools: []MessagePool,
        client_id_permutation: IdPermutation,

        state_checker: StateChecker,
        storage_checker: StorageChecker,

        context: ?*anyopaque = null,

        pub fn init(
            allocator: mem.Allocator,
            /// Includes command=register messages.
            on_client_reply: fn (
                cluster: *Self,
                client: usize,
                request: *Message,
                reply: *Message,
            ) void,
            options: Options,
        ) !*Self {
            assert(options.replica_count >= 1);
            assert(options.replica_count <= 6);
            assert(options.client_count > 0);
            assert(options.storage_size_limit % constants.sector_size == 0);
            assert(options.storage_size_limit <= constants.storage_size_max);
            assert(options.storage.replica_index == null);
            assert(options.storage.fault_atlas == null);

            const node_count = options.replica_count + options.standby_count;

            var prng = std.rand.DefaultPrng.init(options.seed);
            const random = prng.random();

            // TODO(Zig) Client.init()'s MessagePool.Options require a reference to the network — use
            // @returnAddress() instead.
            var network = try allocator.create(Network);
            errdefer allocator.destroy(network);

            network.* = try Network.init(
                allocator,
                options.network,
            );
            errdefer network.deinit();

            // TODO(Zig) @returnAddress()
            var storage_fault_atlas = try allocator.create(StorageFaultAtlas);
            errdefer allocator.destroy(storage_fault_atlas);

            storage_fault_atlas.* = StorageFaultAtlas.init(
                options.replica_count,
                random,
                options.storage_fault_atlas,
            );

            const storages = try allocator.alloc(Storage, node_count);
            errdefer allocator.free(storages);

            for (storages) |*storage, replica_index| {
                var storage_options = options.storage;
                storage_options.replica_index = @intCast(u8, replica_index);
                storage_options.fault_atlas = storage_fault_atlas;
                storage.* = try Storage.init(allocator, options.storage_size_limit, storage_options);
                // Disable most faults at startup, so that the replicas don't get stuck recovering_head.
                storage.faulty = replica_index >= vsr.quorums(options.replica_count).view_change;
            }
            errdefer for (storages) |*storage| storage.deinit(allocator);

            var replica_pools = try allocator.alloc(MessagePool, node_count);
            errdefer allocator.free(replica_pools);

            for (replica_pools) |*pool, i| {
                errdefer for (replica_pools[0..i]) |*p| p.deinit(allocator);
                pool.* = try MessagePool.init(allocator, .replica);
            }
            errdefer for (replica_pools) |*pool| pool.deinit(allocator);

            const replicas = try allocator.alloc(Replica, node_count);
            errdefer allocator.free(replicas);

            const replica_health = try allocator.alloc(ReplicaHealth, node_count);
            errdefer allocator.free(replica_health);
            mem.set(ReplicaHealth, replica_health, .up);

            var client_pools = try allocator.alloc(MessagePool, options.client_count);
            errdefer allocator.free(client_pools);

            for (client_pools) |*pool, i| {
                errdefer for (client_pools[0..i]) |*p| p.deinit(allocator);
                pool.* = try MessagePool.init(allocator, .client);
            }
            errdefer for (client_pools) |*pool| pool.deinit(allocator);

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

            // Format each replica's storage (equivalent to "tigerbeetle format ...").
            for (storages) |*storage, replica_index| {
                var superblock = try SuperBlock.init(allocator, .{
                    .storage = storage,
                    .message_pool = &replica_pools[replica_index],
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

            // We must heap-allocate the cluster since its pointer will be attached to the replica.
            // TODO(Zig) @returnAddress().
            var cluster = try allocator.create(Self);
            errdefer allocator.destroy(cluster);

            cluster.* = Self{
                .allocator = allocator,
                .options = options,
                .on_client_reply = on_client_reply,
                .network = network,
                .storages = storages,
                .storage_fault_atlas = storage_fault_atlas,
                .replicas = replicas,
                .replica_pools = replica_pools,
                .replica_health = replica_health,
                .replica_count = options.replica_count,
                .standby_count = options.standby_count,
                .clients = clients,
                .client_pools = client_pools,
                .client_id_permutation = client_id_permutation,
                .state_checker = state_checker,
                .storage_checker = storage_checker,
            };

            for (cluster.replicas) |_, replica_index| {
                try cluster.open_replica(@intCast(u8, replica_index), .{
                    .resolution = constants.tick_ms * std.time.ns_per_ms,
                    .offset_type = .linear,
                    .offset_coefficient_A = 0,
                    .offset_coefficient_B = 0,
                });
            }
            errdefer for (cluster.replicas) |*replica| replica.deinit(allocator);

            for (clients) |*client| {
                client.on_reply_context = cluster;
                client.on_reply_callback = client_on_reply;
            }

            return cluster;
        }

        pub fn deinit(cluster: *Self) void {
            cluster.storage_checker.deinit();
            cluster.state_checker.deinit();
            cluster.network.deinit();
            for (cluster.clients) |*client| client.deinit(cluster.allocator);
            for (cluster.client_pools) |*pool| pool.deinit(cluster.allocator);
            for (cluster.replicas) |*replica, i|{
                if (cluster.replica_health[i] == .up) replica.deinit(cluster.allocator);
            }
            for (cluster.replica_pools) |*pool| pool.deinit(cluster.allocator);
            for (cluster.storages) |*storage| storage.deinit(cluster.allocator);

            cluster.allocator.free(cluster.clients);
            cluster.allocator.free(cluster.client_pools);
            cluster.allocator.free(cluster.replicas);
            cluster.allocator.free(cluster.replica_health);
            cluster.allocator.free(cluster.replica_pools);
            cluster.allocator.free(cluster.storages);
            cluster.allocator.destroy(cluster.storage_fault_atlas);
            cluster.allocator.destroy(cluster.network);
            cluster.allocator.destroy(cluster);
        }

        pub fn tick(cluster: *Self) void {
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

        /// Returns whether the replica was crashed.
        /// Returns an error when the replica was unable to recover (open).
        pub fn restart_replica(cluster: *Self, replica_index: u8) !void {
            assert(cluster.replica_health[replica_index] == .down);

            // Pass the old replica's Time through to the new replica. It will continue to tick while
            // the replica is crashed, to ensure the clocks don't desyncronize too far to recover.
            try cluster.open_replica(replica_index, cluster.replicas[replica_index].time);
            cluster.network.process_enable(.{ .replica = replica_index });
            cluster.replica_health[replica_index] = .up;
        }

        /// Reset a replica to its initial state, simulating a random crash/panic.
        /// Leave the persistent storage untouched, and leave any currently
        /// inflight messages to/from the replica in the network.
        pub fn crash_replica(cluster: *Self, replica_index: u8) void {
            assert(cluster.replica_health[replica_index] == .up);

            // Reset the storage before the replica so that pending writes can (partially) finish.
            cluster.storages[replica_index].reset();

            cluster.replicas[replica_index].deinit(cluster.allocator);
            cluster.network.process_disable(.{ .replica = replica_index });
            cluster.replica_health[replica_index] = .down;

            // Ensure that none of the replica's messages leaked when it was deinitialized.
            var messages_in_pool: usize = 0;
            const message_bus = cluster.network.get_message_bus(.{ .replica = replica_index });
            {
                var it = message_bus.pool.free_list;
                while (it) |message| : (it = message.next) messages_in_pool += 1;
            }
            assert(messages_in_pool == message_pool.messages_max_replica);
        }

        fn open_replica(cluster: *Self, replica_index: u8, time: Time) !void {
            var replica = &cluster.replicas[replica_index];
            try replica.open(
                cluster.allocator,
                .{
                    .replica_count = cluster.replica_count,
                    .standby_count = cluster.standby_count,
                    .storage = &cluster.storages[replica_index],
                    // TODO Test restarting with a higher storage limit.
                    .storage_size_limit = cluster.options.storage_size_limit,
                    .message_pool = &cluster.replica_pools[replica_index],
                    .time = time,
                    .state_machine_options = cluster.options.state_machine,
                    .message_bus_options = .{ .network = cluster.network },
                },
            );
            assert(replica.cluster == cluster.options.cluster_id);
            assert(replica.replica == replica_index);
            assert(replica.replica_count == cluster.replica_count);
            assert(replica.standby_count == cluster.standby_count);

            replica.context = cluster;
            replica.on_change_state = on_replica_change_state;
            replica.on_compact = on_replica_compact;
            replica.on_checkpoint = on_replica_checkpoint;
            cluster.network.link(replica.message_bus.process, &replica.message_bus);
        }

        pub fn request(
            cluster: *Self,
            client_index: usize,
            request_operation: StateMachine.Operation,
            request_message: *Message,
            request_body_size: usize,
        ) void {
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
            const cluster = @ptrCast(*Self, @alignCast(@alignOf(Self), client.on_reply_context.?));
            assert(reply_message.header.cluster == cluster.options.cluster_id);
            assert(reply_message.header.invalid() == null);
            assert(reply_message.header.client == client.id);
            assert(reply_message.header.request == request_message.header.request);
            assert(reply_message.header.command == .reply);
            assert(reply_message.header.operation == request_message.header.operation);

            const client_index = for (cluster.clients) |*c, i| {
                if (client == c) break i;
            } else unreachable;

            cluster.on_client_reply(cluster, client_index, request_message, reply_message);
        }

        fn on_replica_change_state(replica: *const Replica) void {
            const cluster = @ptrCast(*Self, @alignCast(@alignOf(Self), replica.context.?));
            if (cluster.replica_health[replica.replica] == .down) return;

            cluster.state_checker.check_state(replica.replica) catch |err| {
                fatal(.correctness, "state checker error: {}", .{err});
            };
        }

        fn on_replica_compact(replica: *const Replica) void {
            const cluster = @ptrCast(*Self, @alignCast(@alignOf(Self), replica.context.?));
            cluster.storage_checker.replica_compact(replica) catch |err| {
                fatal(.correctness, "storage checker error: {}", .{err});
            };
        }

        fn on_replica_checkpoint(replica: *const Replica) void {
            const cluster = @ptrCast(*Self, @alignCast(@alignOf(Self), replica.context.?));
            cluster.storage_checker.replica_checkpoint(replica) catch |err| {
                fatal(.correctness, "storage checker error: {}", .{err});
            };
        }

        /// Print an error message and then exit with an exit code.
        fn fatal(failure: Failure, comptime fmt_string: []const u8, args: anytype) noreturn {
            std.log.scoped(.state_checker).err(fmt_string, args);
            std.os.exit(@enumToInt(failure));
        }
    };
}
