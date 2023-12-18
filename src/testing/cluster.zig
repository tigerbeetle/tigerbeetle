const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const log = std.log.scoped(.cluster);

const constants = @import("../constants.zig");
const message_pool = @import("../message_pool.zig");
const MessagePool = message_pool.MessagePool;
const Message = MessagePool.Message;

const AOF = @import("aof.zig").AOF;
const Storage = @import("storage.zig").Storage;
const StorageFaultAtlas = @import("storage.zig").ClusterFaultAtlas;
const Time = @import("time.zig").Time;
const IdPermutation = @import("id.zig").IdPermutation;

const MessageBus = @import("cluster/message_bus.zig").MessageBus;
const Network = @import("cluster/network.zig").Network;
const NetworkOptions = @import("cluster/network.zig").NetworkOptions;
const StateCheckerType = @import("cluster/state_checker.zig").StateCheckerType;
const StorageChecker = @import("cluster/storage_checker.zig").StorageChecker;
const SyncCheckerType = @import("cluster/sync_checker.zig").SyncCheckerType;
const GridChecker = @import("cluster/grid_checker.zig").GridChecker;
const ManifestCheckerType = @import("cluster/manifest_checker.zig").ManifestCheckerType;

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
const client_id_permutation_shift = constants.members_max;

// TODO(Zig): Once Zig is upgraded from 0.11, change StateMachineType from anytype back to
// fn (comptime Storage: type, comptime constants: anytype) type.
pub fn ClusterType(comptime StateMachineType: anytype) type {
    return struct {
        const Self = @This();

        pub const StateMachine = StateMachineType(Storage, constants.state_machine_config);
        pub const Replica = vsr.ReplicaType(StateMachine, MessageBus, Storage, Time, AOF);
        pub const Client = vsr.Client(StateMachine, MessageBus);
        pub const StateChecker = StateCheckerType(Client, Replica);
        pub const SyncChecker = SyncCheckerType(Replica);
        pub const ManifestChecker = ManifestCheckerType(StateMachine.Forest);

        pub const Options = struct {
            cluster_id: u128,
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
        on_client_reply: *const fn (
            cluster: *Self,
            client: usize,
            request: *Message.Request,
            reply: *Message.Reply,
        ) void,

        network: *Network,
        storages: []Storage,
        storage_fault_atlas: *StorageFaultAtlas,
        aofs: []AOF,
        /// NB: includes both active replicas and standbys.
        replicas: []Replica,
        replica_pools: []MessagePool,
        replica_health: []ReplicaHealth,
        replica_count: u8,
        replica_diverged: std.StaticBitSet(constants.members_max) =
            std.StaticBitSet(constants.members_max).initEmpty(),
        standby_count: u8,

        clients: []Client,
        client_pools: []MessagePool,
        client_id_permutation: IdPermutation,

        state_checker: StateChecker,
        storage_checker: StorageChecker,
        sync_checker: SyncChecker,
        grid_checker: *GridChecker,
        manifest_checker: ManifestChecker,

        context: ?*anyopaque = null,

        pub fn init(
            allocator: mem.Allocator,
            /// Includes command=register messages.
            on_client_reply: *const fn (
                cluster: *Self,
                client: usize,
                request: *Message.Request,
                reply: *Message.Reply,
            ) void,
            options: Options,
        ) !*Self {
            assert(options.replica_count >= 1);
            assert(options.replica_count <= 6);
            assert(options.client_count > 0);
            assert(options.storage_size_limit % constants.sector_size == 0);
            assert(options.storage_size_limit <= constants.storage_size_limit_max);
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

            var storage_fault_atlas = try allocator.create(StorageFaultAtlas);
            errdefer allocator.destroy(storage_fault_atlas);

            storage_fault_atlas.* = StorageFaultAtlas.init(
                options.replica_count,
                random,
                options.storage_fault_atlas,
            );

            var grid_checker = try allocator.create(GridChecker);
            errdefer allocator.destroy(grid_checker);

            grid_checker.* = GridChecker.init(allocator);
            errdefer grid_checker.deinit();

            const storages = try allocator.alloc(Storage, node_count);
            errdefer allocator.free(storages);

            for (storages, 0..) |*storage, replica_index| {
                errdefer for (storages[0..replica_index]) |*s| s.deinit(allocator);
                var storage_options = options.storage;
                storage_options.replica_index = @as(u8, @intCast(replica_index));
                storage_options.fault_atlas = storage_fault_atlas;
                storage_options.grid_checker = grid_checker;
                storage.* = try Storage.init(allocator, options.storage_size_limit, storage_options);
                // Disable most faults at startup, so that the replicas don't get stuck recovering_head.
                storage.faulty = replica_index >= vsr.quorums(options.replica_count).view_change;
            }
            errdefer for (storages) |*storage| storage.deinit(allocator);

            const aofs = try allocator.alloc(AOF, node_count);
            errdefer allocator.free(aofs);

            for (aofs, 0..) |*aof, i| {
                errdefer for (aofs[0..i]) |*a| a.deinit(allocator);
                aof.* = try AOF.init(allocator);
            }
            errdefer for (aofs) |*aof| aof.deinit(allocator);

            var replica_pools = try allocator.alloc(MessagePool, node_count);
            errdefer allocator.free(replica_pools);

            for (replica_pools, 0..) |*pool, i| {
                errdefer for (replica_pools[0..i]) |*p| p.deinit(allocator);
                pool.* = try MessagePool.init(allocator, .replica);
            }
            errdefer for (replica_pools) |*pool| pool.deinit(allocator);

            const replicas = try allocator.alloc(Replica, node_count);
            errdefer allocator.free(replicas);

            const replica_health = try allocator.alloc(ReplicaHealth, node_count);
            errdefer allocator.free(replica_health);
            @memset(replica_health, .up);

            var client_pools = try allocator.alloc(MessagePool, options.client_count);
            errdefer allocator.free(client_pools);

            for (client_pools, 0..) |*pool, i| {
                errdefer for (client_pools[0..i]) |*p| p.deinit(allocator);
                pool.* = try MessagePool.init(allocator, .client);
            }
            errdefer for (client_pools) |*pool| pool.deinit(allocator);

            const client_id_permutation = IdPermutation.generate(random);
            var clients = try allocator.alloc(Client, options.client_count);
            errdefer allocator.free(clients);

            for (clients, 0..) |*client, i| {
                errdefer for (clients[0..i]) |*c| c.deinit(allocator);
                client.* = try Client.init(
                    allocator,
                    client_id_permutation.encode(i + client_id_permutation_shift),
                    options.cluster_id,
                    options.replica_count,
                    &client_pools[i],
                    .{ .network = network },
                );
            }
            errdefer for (clients) |*client| client.deinit(allocator);

            var state_checker = try StateChecker.init(allocator, .{
                .cluster_id = options.cluster_id,
                .replicas = replicas,
                .replica_count = options.replica_count,
                .clients = clients,
            });
            errdefer state_checker.deinit();

            var storage_checker = try StorageChecker.init(allocator);
            errdefer storage_checker.deinit(allocator);

            var sync_checker = SyncChecker.init(allocator);
            errdefer sync_checker.deinit();

            var manifest_checker = ManifestChecker.init(allocator);
            errdefer manifest_checker.deinit();

            // Format each replica's storage (equivalent to "tigerbeetle format ...").
            for (storages, 0..) |*storage, replica_index| {
                var superblock = try SuperBlock.init(allocator, .{
                    .storage = storage,
                    .storage_size_limit = options.storage_size_limit,
                });
                defer superblock.deinit(allocator);

                try vsr.format(
                    Storage,
                    allocator,
                    .{
                        .cluster = options.cluster_id,
                        .replica = @as(u8, @intCast(replica_index)),
                        .replica_count = options.replica_count,
                    },
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
                .aofs = aofs,
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
                .sync_checker = sync_checker,
                .grid_checker = grid_checker,
                .manifest_checker = manifest_checker,
            };

            for (cluster.replicas, 0..) |_, replica_index| {
                errdefer for (replicas[0..replica_index]) |*r| r.deinit(allocator);
                // Nonces are incremented on restart, so spread them out across 128 bit space
                // to avoid collisions.
                const nonce = 1 + @as(u128, replica_index) << 64;
                try cluster.open_replica(@as(u8, @intCast(replica_index)), nonce, .{
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
                network.link(client.message_bus.process, &client.message_bus);
            }

            return cluster;
        }

        pub fn deinit(cluster: *Self) void {
            cluster.manifest_checker.deinit();
            cluster.sync_checker.deinit();
            cluster.storage_checker.deinit(cluster.allocator);
            cluster.state_checker.deinit();
            cluster.network.deinit();
            for (cluster.clients) |*client| client.deinit(cluster.allocator);
            for (cluster.client_pools) |*pool| pool.deinit(cluster.allocator);
            for (cluster.replicas, 0..) |*replica, i| {
                switch (cluster.replica_health[i]) {
                    .up => replica.deinit(cluster.allocator),
                    .down => {},
                }
            }
            for (cluster.replica_pools) |*pool| pool.deinit(cluster.allocator);
            for (cluster.storages) |*storage| storage.deinit(cluster.allocator);
            for (cluster.aofs) |*aof| aof.deinit(cluster.allocator);

            cluster.grid_checker.deinit(); // (Storage references this.)

            cluster.allocator.free(cluster.clients);
            cluster.allocator.free(cluster.client_pools);
            cluster.allocator.free(cluster.replicas);
            cluster.allocator.free(cluster.replica_health);
            cluster.allocator.free(cluster.replica_pools);
            cluster.allocator.free(cluster.storages);
            cluster.allocator.free(cluster.aofs);
            cluster.allocator.destroy(cluster.grid_checker);
            cluster.allocator.destroy(cluster.storage_fault_atlas);
            cluster.allocator.destroy(cluster.network);
            cluster.allocator.destroy(cluster);
        }

        pub fn tick(cluster: *Self) void {
            cluster.network.tick();

            for (cluster.clients) |*client| client.tick();
            for (cluster.storages) |*storage| storage.tick();
            for (cluster.replicas, 0..) |*replica, i| {
                switch (cluster.replica_health[i]) {
                    .up => {
                        replica.tick();
                        cluster.sync_checker.check_sync_stage(replica);
                        cluster.state_checker.check_state(replica.replica) catch |err| {
                            fatal(.correctness, "state checker error: {}", .{err});
                        };
                    },
                    // Keep ticking the time so that it won't have diverged too far to synchronize
                    // when the replica restarts.
                    .down => replica.clock.time.tick(),
                }
            }
        }

        /// Causes the checkpoint identifier of each replica to diverge.
        /// This simulates a storage determinism bug.
        ///
        /// (Replica must be running and also between compaction beats for this to run.)
        // TODO Test the occasional divergence in the VOPR.
        pub fn diverge(cluster: *Self, replica_index: u8) void {
            assert(cluster.replica_health[replica_index] == .up);

            cluster.replica_diverged.set(replica_index);

            const replica = &cluster.replicas[replica_index];
            assert(replica.commit_stage == .idle);

            const reservation = replica.grid.free_set.reserve(1).?;
            defer replica.grid.free_set.forfeit(reservation);

            cluster.storages[replica_index].options.grid_checker = null;

            // We don't need to actually use the block for the storage to diverge —
            // it is marked as acquired in the superblock free set.
            _ = replica.grid.free_set.acquire(reservation).?;
        }

        /// Returns an error when the replica was unable to recover (open).
        pub fn restart_replica(cluster: *Self, replica_index: u8) !void {
            assert(cluster.replica_health[replica_index] == .down);

            var nonce = cluster.replicas[replica_index].nonce + 1;
            // Pass the old replica's Time through to the new replica. It will continue to tick while
            // the replica is crashed, to ensure the clocks don't desynchronize too far to recover.
            var time = cluster.replicas[replica_index].time;
            try cluster.open_replica(replica_index, nonce, time);
            cluster.network.process_enable(.{ .replica = replica_index });
            cluster.replica_health[replica_index] = .up;
            cluster.log_replica(.recover, replica_index);
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
            cluster.log_replica(.crash, replica_index);

            // Ensure that none of the replica's messages leaked when it was deinitialized.
            var messages_in_pool: usize = 0;
            const message_bus = cluster.network.get_message_bus(.{ .replica = replica_index });
            {
                var it = message_bus.pool.free_list;
                while (it) |message| : (it = message.next) messages_in_pool += 1;
            }
            assert(messages_in_pool == message_pool.messages_max_replica);
        }

        fn open_replica(cluster: *Self, replica_index: u8, nonce: u128, time: Time) !void {
            var replica = &cluster.replicas[replica_index];
            try replica.open(
                cluster.allocator,
                .{
                    .node_count = cluster.options.replica_count + cluster.options.standby_count,
                    .storage = &cluster.storages[replica_index],
                    .aof = &cluster.aofs[replica_index],
                    // TODO Test restarting with a higher storage limit.
                    .storage_size_limit = cluster.options.storage_size_limit,
                    .message_pool = &cluster.replica_pools[replica_index],
                    .nonce = nonce,
                    .time = time,
                    .state_machine_options = cluster.options.state_machine,
                    .message_bus_options = .{ .network = cluster.network },
                },
            );
            assert(replica.cluster == cluster.options.cluster_id);
            assert(replica.replica == replica_index);
            assert(replica.replica_count == cluster.replica_count);
            assert(replica.standby_count == cluster.standby_count);

            replica.test_context = cluster;
            replica.event_callback = on_replica_event;
            cluster.network.link(replica.message_bus.process, &replica.message_bus);
        }

        pub fn request(
            cluster: *Self,
            client_index: usize,
            request_operation: StateMachine.Operation,
            request_message: *Message,
            request_body_size: usize,
        ) void {
            const client = &cluster.clients[client_index];
            const message = request_message.build(.request);

            message.header.* = .{
                .client = client.id,
                .request = undefined, // Set by client.raw_request.
                .cluster = client.cluster,
                .command = .request,
                .operation = vsr.Operation.from(StateMachine, request_operation),
                .size = @intCast(@sizeOf(vsr.Header) + request_body_size),
            };

            client.raw_request(
                undefined,
                request_callback,
                message,
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
            result: []const u8,
        ) void {
            _ = user_data;
            _ = operation;
            _ = result;
        }

        fn client_on_reply(
            client: *Client,
            request_message: *Message.Request,
            reply_message: *Message.Reply,
        ) void {
            const cluster: *Self = @ptrCast(@alignCast(client.on_reply_context.?));
            assert(reply_message.header.invalid() == null);
            assert(reply_message.header.cluster == cluster.options.cluster_id);
            assert(reply_message.header.client == client.id);
            assert(reply_message.header.request == request_message.header.request);
            assert(reply_message.header.command == .reply);
            assert(reply_message.header.operation == request_message.header.operation);

            const client_index = for (cluster.clients, 0..) |*c, i| {
                if (client == c) break i;
            } else unreachable;

            cluster.on_client_reply(cluster, client_index, request_message, reply_message);
        }

        fn on_replica_event(replica: *const Replica, event: vsr.ReplicaEvent) void {
            const cluster: *Self = @ptrCast(@alignCast(replica.test_context.?));
            assert(cluster.replica_health[replica.replica] == .up);

            switch (event) {
                .message_sent => |message| {
                    cluster.state_checker.on_message(message);
                },
                .state_machine_opened => {
                    if (!cluster.replica_diverged.isSet(replica.replica)) {
                        cluster.manifest_checker.forest_open(&replica.state_machine.forest);
                    }
                },
                .committed => {
                    cluster.log_replica(.commit, replica.replica);
                    cluster.state_checker.check_state(replica.replica) catch |err| {
                        fatal(.correctness, "state checker error: {}", .{err});
                    };
                },
                .compaction_completed => {},
                .checkpoint_commenced => {
                    cluster.log_replica(.checkpoint_commenced, replica.replica);
                },
                .checkpoint_completed => {
                    cluster.log_replica(.checkpoint_completed, replica.replica);
                    if (cluster.replica_diverged.isSet(replica.replica)) {
                        log.debug("{}: on_checkpoint: skipping StorageChecker; diverged", .{
                            replica.replica,
                        });
                    } else {
                        cluster.manifest_checker.forest_checkpoint(&replica.state_machine.forest);
                        cluster.storage_checker.replica_checkpoint(
                            &replica.superblock,
                        ) catch |err| {
                            fatal(.correctness, "storage checker error: {}", .{err});
                        };
                    }
                },
                .checkpoint_divergence_detected => |event_data| {
                    // If the replica diverged, ensure that it was deliberate.
                    assert(cluster.replica_diverged.isSet(event_data.replica));
                },
                .sync_stage_changed => switch (replica.syncing) {
                    .requesting_checkpoint => {
                        cluster.log_replica(.sync_commenced, replica.replica);
                        cluster.sync_checker.replica_sync_start(replica);
                    },
                    .idle => {
                        cluster.log_replica(.sync_completed, replica.replica);
                        cluster.sync_checker.replica_sync_done(replica);
                        if (cluster.replica_diverged.isSet(replica.replica)) {
                            cluster.replica_diverged.unset(replica.replica);
                            log.debug("{}: on_sync_done: clearing deviation", .{replica.replica});
                        }
                    },
                    else => {},
                },
            }
        }

        /// Print an error message and then exit with an exit code.
        fn fatal(failure: Failure, comptime fmt_string: []const u8, args: anytype) noreturn {
            std.log.scoped(.state_checker).err(fmt_string, args);
            std.os.exit(@intFromEnum(failure));
        }

        /// Print the current state of the cluster, intended for printf debugging.
        pub fn log_cluster(cluster: *const Self) void {
            var replica: u8 = 0;
            while (replica < cluster.replicas.len) : (replica += 1) {
                cluster.log_replica(.commit, replica);
            }
        }

        fn log_replica(
            cluster: *const Self,
            event: enum(u8) {
                crash = '$',
                recover = '^',
                commit = ' ',
                checkpoint_commenced = '[',
                checkpoint_completed = ']',
                sync_commenced = '<',
                sync_completed = '>',
            },
            replica_index: u8,
        ) void {
            const replica = &cluster.replicas[replica_index];

            var statuses = [_]u8{' '} ** constants.members_max;
            if (cluster.replica_health[replica_index] == .down) {
                statuses[replica_index] = '#';
            } else {
                statuses[replica_index] = switch (replica.status) {
                    .normal => @as(u8, '.'),
                    .view_change => @as(u8, 'v'),
                    .recovering => @as(u8, 'r'),
                    .recovering_head => @as(u8, 'h'),
                };
            }

            const role: u8 = role: {
                if (cluster.replica_health[replica_index] == .down) break :role '#';
                if (replica.syncing != .idle) break :role '~';
                if (replica.standby()) break :role '|';
                if (replica.primary_index(replica.view) == replica.replica) break :role '/';
                break :role '\\';
            };

            var info_buffer: [128]u8 = undefined;
            var info: []u8 = "";
            var pipeline_buffer: [16]u8 = undefined;
            var pipeline: []u8 = "";

            if (cluster.replica_health[replica_index] == .up) {
                var journal_op_min: u64 = std.math.maxInt(u64);
                var journal_op_max: u64 = 0;
                for (replica.journal.headers) |*header| {
                    if (header.operation != .reserved) {
                        if (journal_op_min > header.op) journal_op_min = header.op;
                        if (journal_op_max < header.op) journal_op_max = header.op;
                    }
                }

                var wal_op_min: u64 = std.math.maxInt(u64);
                var wal_op_max: u64 = 0;
                for (cluster.storages[replica_index].wal_prepares()) |*prepare| {
                    if (prepare.header.valid_checksum() and
                        prepare.header.command == .prepare)
                    {
                        if (wal_op_min > prepare.header.op) wal_op_min = prepare.header.op;
                        if (wal_op_max < prepare.header.op) wal_op_max = prepare.header.op;
                    }
                }

                info = std.fmt.bufPrint(&info_buffer, "" ++
                    "{[view]:>4}V " ++
                    "{[op_checkpoint]:>3}/{[commit_min]:_>3}/{[commit_max]:_>3}C " ++
                    "{[journal_op_min]:>3}:{[journal_op_max]:_>3}Jo " ++
                    "{[journal_faulty]:>2}/{[journal_dirty]:_>2}J! " ++
                    "{[wal_op_min]:>3}:{[wal_op_max]:_>3}Wo " ++
                    "<{[sync_op_min]:_>3}:{[sync_op_max]:_>3}> " ++
                    "{[grid_blocks_free]?:>7}Gf " ++
                    "{[grid_blocks_global]:>2}G! " ++
                    "{[grid_blocks_repair]:>3}G?", .{
                    .view = replica.view,
                    .op_checkpoint = replica.op_checkpoint(),
                    .commit_min = replica.commit_min,
                    .commit_max = replica.commit_max,
                    .journal_op_min = journal_op_min,
                    .journal_op_max = journal_op_max,
                    .journal_dirty = replica.journal.dirty.count,
                    .journal_faulty = replica.journal.faulty.count,
                    .wal_op_min = wal_op_min,
                    .wal_op_max = wal_op_max,
                    .sync_op_min = replica.superblock.working.vsr_state.sync_op_min,
                    .sync_op_max = replica.superblock.working.vsr_state.sync_op_max,
                    .grid_blocks_free = if (replica.grid.free_set.opened)
                        replica.grid.free_set.count_free()
                    else
                        null,
                    .grid_blocks_global = replica.grid.read_global_queue.count,
                    .grid_blocks_repair = replica.grid.blocks_missing.faulty_blocks.count(),
                }) catch unreachable;

                if (replica.pipeline == .queue) {
                    pipeline = std.fmt.bufPrint(&pipeline_buffer, "{:>2}/{}Pp {:>2}/{}Rq", .{
                        replica.pipeline.queue.prepare_queue.count,
                        constants.pipeline_prepare_queue_max,
                        replica.pipeline.queue.request_queue.count,
                        constants.pipeline_request_queue_max,
                    }) catch unreachable;
                }
            }

            log.info("{[replica]: >2} {[event]c} {[role]c} {[statuses]s}" ++
                "  {[info]s}  {[pipeline]s}", .{
                .replica = replica.replica,
                .event = @intFromEnum(event),
                .role = role,
                .statuses = statuses[0 .. cluster.replica_count + cluster.standby_count],
                .info = info,
                .pipeline = pipeline,
            });
        }
    };
}
