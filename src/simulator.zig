const std = @import("std");
const stdx = @import("./stdx.zig");
const builtin = @import("builtin");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const mem = std.mem;

const tb = @import("tigerbeetle.zig");
const constants = @import("constants.zig");
const schema = @import("lsm/schema.zig");
const vsr = @import("vsr.zig");
const Header = vsr.Header;

const vsr_simulator_options = @import("vsr_simulator_options");
const state_machine = vsr_simulator_options.state_machine;
const StateMachineType = switch (state_machine) {
    .accounting => @import("state_machine.zig").StateMachineType,
    .testing => @import("testing/state_machine.zig").StateMachineType,
};

const Client = @import("testing/cluster.zig").Client;
const Cluster = @import("testing/cluster.zig").ClusterType(StateMachineType);
const StateMachine = Cluster.StateMachine;
const Failure = @import("testing/cluster.zig").Failure;
const PartitionMode = @import("testing/packet_simulator.zig").PartitionMode;
const PartitionSymmetry = @import("testing/packet_simulator.zig").PartitionSymmetry;
const Core = @import("testing/cluster/network.zig").Network.Core;
const ReplySequence = @import("testing/reply_sequence.zig").ReplySequence;
const IdPermutation = @import("testing/id.zig").IdPermutation;
const Message = @import("message_pool.zig").MessagePool.Message;

pub const output = std.log.scoped(.cluster);
const log = std.log.scoped(.simulator);

pub const std_options = struct {
    /// The -Dsimulator-log=<full|short> build option selects two logging modes.
    /// In "short" mode, only state transitions are printed (see `Cluster.log_replica`).
    /// "full" mode is the usual logging according to the level.
    pub const log_level: std.log.Level = if (vsr_simulator_options.log == .short) .info else .debug;
    pub const logFn = log_override;
};

// Uncomment if you need per-scope control over the log levels.
// pub const scope_levels = [_]std.log.ScopeLevel{
//     .{ .scope = .cluster, .level = .info },
//     .{ .scope = .replica, .level = .debug },
// };

pub const tigerbeetle_config = @import("config.zig").configs.test_min;

const cluster_id = 0;

pub fn main() !void {
    // This must be initialized at runtime as stderr is not comptime known on e.g. Windows.
    log_buffer.unbuffered_writer = std.io.getStdErr().writer();

    // TODO Use std.testing.allocator when all deinit() leaks are fixed.
    const allocator = std.heap.page_allocator;

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    // Skip argv[0] which is the name of this executable:
    assert(args.skip());

    const seed_random = std.crypto.random.int(u64);
    const seed = seed_from_arg: {
        const seed_argument = args.next() orelse break :seed_from_arg seed_random;
        break :seed_from_arg parse_seed(seed_argument);
    };

    if (builtin.mode == .ReleaseFast or builtin.mode == .ReleaseSmall) {
        // We do not support ReleaseFast or ReleaseSmall because they disable assertions.
        @panic("the simulator must be run with -OReleaseSafe");
    }

    if (seed == seed_random) {
        if (builtin.mode != .ReleaseSafe) {
            // If no seed is provided, than Debug is too slow and ReleaseSafe is much faster.
            @panic("no seed provided: the simulator must be run with -OReleaseSafe");
        }
        if (vsr_simulator_options.log != .short) {
            output.warn("no seed provided: full debug logs are enabled, this will be slow", .{});
        }
    }

    var prng = std.rand.DefaultPrng.init(seed);
    const random = prng.random();

    const replica_count = 1 + random.uintLessThan(u8, constants.replicas_max);
    const standby_count = random.uintAtMost(u8, constants.standbys_max);
    const node_count = replica_count + standby_count;
    const client_count = 1 + random.uintLessThan(u8, constants.clients_max);

    const cluster_options = Cluster.Options{
        .cluster_id = cluster_id,
        .replica_count = replica_count,
        .standby_count = standby_count,
        .client_count = client_count,
        .storage_size_limit = vsr.sector_floor(
            constants.storage_size_limit_max - random.uintLessThan(u64, constants.storage_size_limit_max / 10),
        ),
        .seed = random.int(u64),
        .network = .{
            .node_count = node_count,
            .client_count = client_count,

            .seed = random.int(u64),
            .one_way_delay_mean = 3 + random.uintLessThan(u16, 10),
            .one_way_delay_min = random.uintLessThan(u16, 3),
            .packet_loss_probability = random.uintLessThan(u8, 30),
            .path_maximum_capacity = 2 + random.uintLessThan(u8, 19),
            .path_clog_duration_mean = random.uintLessThan(u16, 500),
            .path_clog_probability = random.uintLessThan(u8, 2),
            .packet_replay_probability = random.uintLessThan(u8, 50),

            .partition_mode = random_partition_mode(random),
            .partition_symmetry = random_partition_symmetry(random),
            .partition_probability = random.uintLessThan(u8, 3),
            .unpartition_probability = 1 + random.uintLessThan(u8, 10),
            .partition_stability = 100 + random.uintLessThan(u32, 100),
            .unpartition_stability = random.uintLessThan(u32, 20),
        },
        .storage = .{
            .seed = random.int(u64),
            .read_latency_min = random.uintLessThan(u16, 3),
            .read_latency_mean = 3 + random.uintLessThan(u16, 10),
            .write_latency_min = random.uintLessThan(u16, 3),
            .write_latency_mean = 3 + random.uintLessThan(u16, 100),
            .read_fault_probability = random.uintLessThan(u8, 10),
            .write_fault_probability = random.uintLessThan(u8, 10),
            .crash_fault_probability = 80 + random.uintLessThan(u8, 21),
        },
        .storage_fault_atlas = .{
            .faulty_superblock = true,
            .faulty_wal_headers = replica_count > 1,
            .faulty_wal_prepares = replica_count > 1,
            .faulty_client_replies = replica_count > 1,
            // >2 instead of >1 because in R=2, a lagging replica may sync to the leading replica,
            // but then the leading replica may have the only copy of a block in the cluster.
            .faulty_grid = replica_count > 2,
        },
        .state_machine = switch (state_machine) {
            .testing => .{ .lsm_forest_node_count = 4096 },
            .accounting => .{
                .lsm_forest_node_count = 4096,
                .cache_entries_accounts = 2048,
                .cache_entries_transfers = 2048,
                .cache_entries_posted = 2048,
            },
        },
    };

    const workload_options = StateMachine.Workload.Options.generate(random, .{
        .client_count = client_count,
        // TODO(DJ) Once Workload no longer needs in_flight_max, make stalled_queue_capacity private.
        // Also maybe make it dynamic (computed from the client_count instead of clients_max).
        .in_flight_max = ReplySequence.stalled_queue_capacity,
    });

    const simulator_options = Simulator.Options{
        .cluster = cluster_options,
        .workload = workload_options,
        // TODO Swarm testing: Test long+few crashes and short+many crashes separately.
        .replica_crash_probability = 0.00002,
        .replica_crash_stability = random.uintLessThan(u32, 1_000),
        .replica_restart_probability = 0.0002,
        .replica_restart_stability = random.uintLessThan(u32, 1_000),
        .requests_max = constants.journal_slot_count * 3,
        .request_probability = 1 + random.uintLessThan(u8, 99),
        .request_idle_on_probability = random.uintLessThan(u8, 20),
        .request_idle_off_probability = 10 + random.uintLessThan(u8, 10),
    };

    output.info(
        \\
        \\          SEED={}
        \\
        \\          replicas={}
        \\          standbys={}
        \\          clients={}
        \\          request_probability={}%
        \\          idle_on_probability={}%
        \\          idle_off_probability={}%
        \\          one_way_delay_mean={} ticks
        \\          one_way_delay_min={} ticks
        \\          packet_loss_probability={}%
        \\          path_maximum_capacity={} messages
        \\          path_clog_duration_mean={} ticks
        \\          path_clog_probability={}%
        \\          packet_replay_probability={}%
        \\          partition_mode={}
        \\          partition_symmetry={}
        \\          partition_probability={}%
        \\          unpartition_probability={}%
        \\          partition_stability={} ticks
        \\          unpartition_stability={} ticks
        \\          read_latency_min={}
        \\          read_latency_mean={}
        \\          write_latency_min={}
        \\          write_latency_mean={}
        \\          read_fault_probability={}%
        \\          write_fault_probability={}%
        \\          crash_probability={d}%
        \\          crash_stability={} ticks
        \\          restart_probability={d}%
        \\          restart_stability={} ticks
    , .{
        seed,
        cluster_options.replica_count,
        cluster_options.standby_count,
        cluster_options.client_count,
        simulator_options.request_probability,
        simulator_options.request_idle_on_probability,
        simulator_options.request_idle_off_probability,
        cluster_options.network.one_way_delay_mean,
        cluster_options.network.one_way_delay_min,
        cluster_options.network.packet_loss_probability,
        cluster_options.network.path_maximum_capacity,
        cluster_options.network.path_clog_duration_mean,
        cluster_options.network.path_clog_probability,
        cluster_options.network.packet_replay_probability,
        cluster_options.network.partition_mode,
        cluster_options.network.partition_symmetry,
        cluster_options.network.partition_probability,
        cluster_options.network.unpartition_probability,
        cluster_options.network.partition_stability,
        cluster_options.network.unpartition_stability,
        cluster_options.storage.read_latency_min,
        cluster_options.storage.read_latency_mean,
        cluster_options.storage.write_latency_min,
        cluster_options.storage.write_latency_mean,
        cluster_options.storage.read_fault_probability,
        cluster_options.storage.write_fault_probability,
        simulator_options.replica_crash_probability,
        simulator_options.replica_crash_stability,
        simulator_options.replica_restart_probability,
        simulator_options.replica_restart_stability,
    });

    var simulator = try Simulator.init(allocator, random, simulator_options);
    defer simulator.deinit(allocator);

    // Safety: replicas crash and restart; at any given point in time arbitrarily many replicas may
    // be crashed, but each replica restarts eventually. The cluster must process all requests
    // without split-brain.
    const ticks_max_requests = 40_000_000;
    var tick_total: u64 = 0;
    var tick: u64 = 0;
    while (tick < ticks_max_requests) : (tick += 1) {
        const requests_replied_old = simulator.requests_replied;
        simulator.tick();
        tick_total += 1;
        if (simulator.requests_replied > requests_replied_old) {
            tick = 0;
        }
        if (simulator.requests_replied == simulator.options.requests_max) {
            break;
        }
    } else {
        output.info(
            "no liveness, final cluster state (requests_max={} requests_replied={}):",
            .{ simulator.options.requests_max, simulator.requests_replied },
        );
        simulator.cluster.log_cluster();
        output.err("you can reproduce this failure with seed={}", .{seed});
        fatal(.liveness, "unable to complete requests_committed_max before ticks_max", .{});
    }

    simulator.transition_to_liveness_mode();

    // Liveness: a core set of replicas is up and fully connected. The rest of replicas might be
    // crashed or partitioned permanently. The core should converge to the same state.
    const ticks_max_convergence = 5_000_000;
    tick = 0;
    while (tick < ticks_max_convergence) : (tick += 1) {
        simulator.tick();
        tick_total += 1;
        if (simulator.done()) {
            break;
        }
    }

    if (simulator.done()) {
        const commits = simulator.cluster.state_checker.commits.items;
        const last_checksum = commits[commits.len - 1].header.checksum;
        for (simulator.cluster.aofs, 0..) |*aof, replica_index| {
            if (simulator.core.isSet(replica_index)) {
                try aof.validate(last_checksum);
            } else {
                try aof.validate(null);
            }
        }
    } else {
        if (simulator.core_missing_primary()) {
            stdx.unimplemented("repair requires reachable primary");
        } else if (simulator.core_missing_quorum()) {
            output.warn("no liveness, core replicas cannot view-change", .{});
        } else if (simulator.core_missing_prepare()) |header| {
            output.warn("no liveness, op={} is not available in core", .{header.op});
        } else if (try simulator.core_missing_blocks(allocator)) |blocks| {
            output.warn("no liveness, {} blocks are not available in core", .{blocks});
        } else if (simulator.core_missing_checkpoint()) {
            output.warn("no liveness, core can't find canonical checkpoint", .{});
        } else {
            output.info("no liveness, final cluster state (core={b}):", .{simulator.core.mask});
            simulator.cluster.log_cluster();
            output.err("you can reproduce this failure with seed={}", .{seed});
            fatal(.liveness, "no state convergence", .{});
        }
    }

    output.info("\n          PASSED ({} ticks)", .{tick_total});
}

pub const Simulator = struct {
    pub const Options = struct {
        cluster: Cluster.Options,
        workload: StateMachine.Workload.Options,

        /// Probability per tick that a crash will occur.
        replica_crash_probability: f64,
        /// Minimum duration of a crash.
        replica_crash_stability: u32,
        /// Probability per tick that a crashed replica will recovery.
        replica_restart_probability: f64,
        /// Minimum time a replica is up until it is crashed again.
        replica_restart_stability: u32,

        /// The total number of requests to send. Does not count `register` messages.
        requests_max: usize,
        request_probability: u8, // percent
        request_idle_on_probability: u8, // percent
        request_idle_off_probability: u8, // percent
    };

    random: std.rand.Random,
    options: Options,
    cluster: *Cluster,
    workload: StateMachine.Workload,

    /// Protect a replica from fast successive crash/restarts.
    replica_stability: []usize,
    reply_sequence: ReplySequence,

    /// Fully-connected subgraph of replicas for liveness checking.
    core: Core = Core.initEmpty(),

    /// Total number of requests sent, including those that have not been delivered.
    /// Does not include `register` messages.
    requests_sent: usize = 0,
    requests_replied: usize = 0,
    requests_idle: bool = false,

    pub fn init(allocator: std.mem.Allocator, random: std.rand.Random, options: Options) !Simulator {
        assert(options.replica_crash_probability < 100.0);
        assert(options.replica_crash_probability >= 0.0);
        assert(options.replica_restart_probability < 100.0);
        assert(options.replica_restart_probability >= 0.0);
        assert(options.requests_max > 0);
        assert(options.request_probability > 0);
        assert(options.request_probability <= 100);
        assert(options.request_idle_on_probability <= 100);
        assert(options.request_idle_off_probability > 0);
        assert(options.request_idle_off_probability <= 100);

        var cluster = try Cluster.init(allocator, on_cluster_reply, options.cluster);
        errdefer cluster.deinit();

        var workload = try StateMachine.Workload.init(allocator, random, options.workload);
        errdefer workload.deinit(allocator);

        var replica_stability = try allocator.alloc(usize, options.cluster.replica_count + options.cluster.standby_count);
        errdefer allocator.free(replica_stability);
        @memset(replica_stability, 0);

        var reply_sequence = try ReplySequence.init(allocator);
        errdefer reply_sequence.deinit(allocator);

        return Simulator{
            .random = random,
            .options = options,
            .cluster = cluster,
            .workload = workload,
            .replica_stability = replica_stability,
            .reply_sequence = reply_sequence,
        };
    }

    pub fn deinit(simulator: *Simulator, allocator: std.mem.Allocator) void {
        allocator.free(simulator.replica_stability);
        simulator.reply_sequence.deinit(allocator);
        simulator.workload.deinit(allocator);
        simulator.cluster.deinit();
    }

    pub fn done(simulator: *const Simulator) bool {
        assert(simulator.core.count() > 0);
        assert(simulator.requests_sent == simulator.options.requests_max);
        assert(simulator.reply_sequence.empty());
        for (simulator.cluster.clients) |*client| {
            assert(client.request_queue.count == 0);
        }

        for (simulator.cluster.replicas) |*replica| {
            if (simulator.core.isSet(replica.replica)) {
                if (!simulator.cluster.state_checker.replica_convergence(replica.replica)) {
                    return false;
                }
            }
        }

        simulator.cluster.state_checker.assert_cluster_convergence();

        // Check whether the replica is still repairing prepares/tables/replies.
        for (simulator.cluster.replicas) |*replica| {
            if (simulator.core.isSet(replica.replica)) {
                for (replica.op_checkpoint() + 1..replica.op + 1) |op| {
                    const header = replica.journal.header_with_op(op).?;
                    if (!replica.journal.has_clean(header)) return false;
                }
                // It's okay for a replica to miss some prepares older than the current checkpoint.
                maybe(replica.journal.faulty.count > 0);

                if (!replica.sync_content_done()) return false;
            }
        }

        return true;
    }

    pub fn tick(simulator: *Simulator) void {
        // TODO(Zig): Remove (see on_cluster_reply()).
        simulator.cluster.context = simulator;

        simulator.cluster.tick();
        simulator.tick_requests();
        simulator.tick_crash();
    }

    pub fn transition_to_liveness_mode(simulator: *Simulator) void {
        simulator.core = random_core(
            simulator.random,
            simulator.options.cluster.replica_count,
            simulator.options.cluster.standby_count,
        );
        log.debug("transition_to_liveness_mode: core={b}", .{simulator.core.mask});

        var it = simulator.core.iterator(.{});
        while (it.next()) |replica_index| {
            const fault = false;
            if (simulator.cluster.replica_health[replica_index] == .down) {
                simulator.restart_replica(@as(u8, @intCast(replica_index)), fault);
            }
        }

        simulator.cluster.network.transition_to_liveness_mode(simulator.core);
        simulator.options.replica_crash_probability = 0;
        simulator.options.replica_restart_probability = 0;
    }

    // If a primary ends up being outside of a core, and is only partially connected to the core,
    // the core might fail to converge, as parts of the repair protocol rely on primary-sent
    // `.start_view_change` messages. Until we fix this issue, we special-case this scenario in
    // VOPR and don't treat it as a liveness failure.
    //
    // TODO: make sure that .recovering_head replicas can transition to normal even without direct
    // connection to the primary
    pub fn core_missing_primary(simulator: *const Simulator) bool {
        assert(simulator.core.count() > 0);

        for (simulator.cluster.replicas) |*replica| {
            if (simulator.cluster.replica_health[replica.replica] == .up and
                replica.status == .normal and replica.primary() and
                !simulator.core.isSet(replica.replica))
            {
                // `replica` considers itself a primary, check that at least part of the core thinks
                // so as well.
                var it = simulator.core.iterator(.{});
                while (it.next()) |replica_core_index| {
                    if (simulator.cluster.replicas[replica_core_index].view == replica.view) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /// The core contains at least a view-change quorum of replicas. But if one or more of those
    /// replicas are in status=recovering_head (due to corruption or state sync), then that may be
    /// insufficient.
    /// TODO: State sync can trigger recovering_head without any crashes, and we should be able to
    /// recover in that case.
    /// (See https://github.com/tigerbeetle/tigerbeetle/pull/933#discussion_r1245440623)
    pub fn core_missing_quorum(simulator: *const Simulator) bool {
        assert(simulator.core.count() > 0);

        var core_replicas: usize = 0;
        var core_recovering_head: usize = 0;
        for (simulator.cluster.replicas) |*replica| {
            if (simulator.core.isSet(replica.replica) and !replica.standby()) {
                core_replicas += 1;
                core_recovering_head += @intFromBool(replica.status == .recovering_head);
            }
        }

        if (core_recovering_head == 0) return false;

        const quorums = vsr.quorums(simulator.options.cluster.replica_count);
        assert(quorums.view_change > core_replicas - core_recovering_head);
        return true;
    }

    // Returns a header for a prepare which can't be repaired by the core due to storage faults.
    //
    // When generating a FaultAtlas, we don't try to protect core from excessive errors. Instead,
    // if the core gets stuck, we verify that this is indeed due to storage faults.
    pub fn core_missing_prepare(simulator: *const Simulator) ?vsr.Header.Prepare {
        assert(simulator.core.count() > 0);

        var missing_op: ?u64 = null;
        for (simulator.cluster.replicas) |replica| {
            if (simulator.core.isSet(replica.replica)) {
                assert(simulator.cluster.replica_health[replica.replica] == .up);
                if (replica.commit_min < replica.op) {
                    if (missing_op == null or missing_op.? > replica.commit_min + 1) {
                        missing_op = replica.commit_min + 1;
                    }
                }
            }
        }

        if (missing_op == null) return null;

        const missing_header = simulator.cluster.state_checker.header_with_op(missing_op.?);

        for (simulator.cluster.replicas) |replica| {
            if (simulator.core.isSet(replica.replica) and !replica.standby()) {
                if (replica.journal.has_clean(&missing_header)) {
                    // Prepare *was* found on an active core replica, so the header isn't
                    // actually missing.
                    return null;
                }
            }
        }

        return missing_header;
    }

    /// Check whether the cluster is stuck because the entire core is missing the same block[s].
    pub fn core_missing_blocks(
        simulator: *const Simulator,
        allocator: std.mem.Allocator,
    ) error{OutOfMemory}!?usize {
        assert(simulator.core.count() > 0);

        var blocks_missing = std.ArrayList(struct {
            replica: u8,
            address: u64,
            checksum: u128,
        }).init(allocator);
        defer blocks_missing.deinit();

        // Find all blocks that any replica in the core is missing.
        for (simulator.cluster.replicas) |replica| {
            if (!simulator.core.isSet(replica.replica)) continue;

            const storage = &simulator.cluster.storages[replica.replica];
            var fault_iterator = replica.grid.read_global_queue.peek();
            while (fault_iterator) |faulty_read| : (fault_iterator = faulty_read.next) {
                try blocks_missing.append(.{
                    .replica = replica.replica,
                    .address = faulty_read.address,
                    .checksum = faulty_read.checksum,
                });

                log.debug("{}: core_missing_blocks: " ++
                    "missing address={} checksum={} corrupt={} (remote read)", .{
                    replica.replica,
                    faulty_read.address,
                    faulty_read.checksum,
                    storage.area_faulty(.{ .grid = .{ .address = faulty_read.address } }),
                });
            }

            var repair_iterator = replica.grid.blocks_missing.faulty_blocks.iterator();
            while (repair_iterator.next()) |fault| {
                try blocks_missing.append(.{
                    .replica = replica.replica,
                    .address = fault.key_ptr.*,
                    .checksum = fault.value_ptr.checksum,
                });

                log.debug("{}: core_missing_blocks: " ++
                    "missing address={} checksum={} corrupt={} (GridBlocksMissing)", .{
                    replica.replica,
                    fault.key_ptr.*,
                    fault.value_ptr.checksum,
                    storage.area_faulty(.{ .grid = .{ .address = fault.key_ptr.* } }),
                });
            }
        }

        // Check whether every replica in the core is missing the blocks.
        // (If any core replica has the block, then that is a bug, since it should have repaired.)
        for (blocks_missing.items) |block_missing| {
            for (simulator.cluster.replicas) |replica| {
                const storage = &simulator.cluster.storages[replica.replica];

                // A replica might actually have the block that it is requesting, but not know.
                // This can occur after state sync: if we compact and create a table, but then skip
                // over that table via state sync, we will try to sync the table anyway.
                if (replica.replica == block_missing.replica) continue;

                if (!simulator.core.isSet(replica.replica)) continue;
                if (replica.standby()) continue;
                if (storage.area_faulty(.{
                    .grid = .{ .address = block_missing.address },
                })) continue;

                const block = storage.grid_block(block_missing.address) orelse continue;
                const block_header = schema.header_from_block(block);
                if (block_header.checksum == block_missing.checksum) {
                    log.err("{}: core_missing_blocks: found address={} checksum={}", .{
                        replica.replica,
                        block_missing.address,
                        block_missing.checksum,
                    });
                    @panic("block found in core");
                }
            }
        }

        if (blocks_missing.items.len == 0) {
            return null;
        } else {
            return blocks_missing.items.len;
        }
    }

    // Check if the core is stuck because replicas can't converge to a canonical checkpoint. This
    // can happen in two scenarios:
    //
    // A) There is no canonical checkpoint at all --- one replica is ahead of others by a
    //    checkpoint, but there are no commits on top of that checkpoint.
    // B) During view change, there is a canonical checkpoint, but replicas can't learn that it is
    //    in fact canonical, because commit messages don't flow, so commit_max is not advanced,
    //    and there are not enough replicas at a checkpoint for the quorum condition.
    //
    // TODO: both of the above genuine liveness issues to be fixed with async checkpoints, which
    // would allow to sync to the _previous_ checkpoint, guaranteed to be canonical.
    fn core_missing_checkpoint(simulator: *const Simulator) bool {
        var core_replicas_total: u8 = 0;
        var core_replicas_normal: u8 = 0;
        var commit_max: u64 = 0;
        var checkpoint_max: u64 = 0;
        var core_checkpoints = stdx.BoundedArray(u128, constants.replicas_max){};
        for (simulator.cluster.replicas) |replica| {
            if (!simulator.core.isSet(replica.replica)) continue;
            if (replica.standby()) continue;
            core_replicas_total += 1;
            core_replicas_normal += @intFromBool(replica.status == .normal);
            commit_max = @max(commit_max, replica.commit_max);
            checkpoint_max = @max(
                checkpoint_max,
                replica.superblock.working.vsr_state.checkpoint.commit_min,
            );
            core_checkpoints.append_assume_capacity(replica.superblock.working.checkpoint_id());
        }

        assert(core_replicas_normal == core_replicas_total or core_replicas_normal == 0);

        for (0..core_checkpoints.count()) |i| {
            for (0..i) |j| {
                if (core_checkpoints.get(i) != core_checkpoints.get(j)) {
                    if (core_replicas_normal == core_replicas_total) {
                        // Case A) all replicas are normal, but commit_max is exactly checkpoint
                        // trigger.
                        return vsr.Checkpoint.trigger_for_checkpoint(checkpoint_max) == commit_max;
                    } else {
                        // Case B) all replicas are in a view change or recovering head, they
                        // can't learn which checkpoint is canonical without commit messages.
                        return true;
                    }
                }
            }
        }

        return false;
    }

    fn on_cluster_reply(
        cluster: *Cluster,
        reply_client: usize,
        request: *Message.Request,
        reply: *Message.Reply,
    ) void {
        // TODO(Zig) Use @returnAddress to initialzie the cluster, then this can just use @fieldParentPtr().
        const simulator: *Simulator = @ptrCast(@alignCast(cluster.context.?));
        simulator.reply_sequence.insert(reply_client, request, reply);

        while (simulator.reply_sequence.peek()) |commit| {
            defer simulator.reply_sequence.next();

            const commit_client = simulator.cluster.clients[commit.client_index];
            assert(commit.reply.references == 1);
            assert(commit.reply.header.command == .reply);
            assert(commit.reply.header.client == commit_client.id);
            assert(commit.reply.header.request == commit.request.header.request);
            assert(commit.reply.header.operation == commit.request.header.operation);

            assert(commit.request.references == 1);
            assert(commit.request.header.command == .request);
            assert(commit.request.header.client == commit_client.id);

            log.debug("consume_stalled_replies: op={} operation={} client={} request={}", .{
                commit.reply.header.op,
                commit.reply.header.operation,
                commit.request.header.client,
                commit.request.header.request,
            });

            if (!commit.request.header.operation.vsr_reserved()) {
                simulator.requests_replied += 1;
                simulator.workload.on_reply(
                    commit.client_index,
                    commit.reply.header.operation.cast(StateMachine),
                    commit.reply.header.timestamp,
                    commit.request.body(),
                    commit.reply.body(),
                );
            }
        }
    }

    /// Maybe send a request from one of the cluster's clients.
    fn tick_requests(simulator: *Simulator) void {
        if (simulator.requests_idle) {
            if (chance(simulator.random, simulator.options.request_idle_off_probability)) {
                simulator.requests_idle = false;
            }
        } else {
            if (chance(simulator.random, simulator.options.request_idle_on_probability)) {
                simulator.requests_idle = true;
            }
        }

        if (simulator.requests_idle) return;
        if (simulator.requests_sent == simulator.options.requests_max) return;
        if (!chance(simulator.random, simulator.options.request_probability)) return;

        const client_index =
            simulator.random.uintLessThan(usize, simulator.options.cluster.client_count);
        var client = &simulator.cluster.clients[client_index];

        // Messages aren't added to the ReplySequence until a reply arrives.
        // Before sending a new message, make sure there will definitely be room for it.
        var reserved: usize = 0;
        for (simulator.cluster.clients) |*c| {
            // Count the number of clients that are still waiting for a `register` to complete,
            // since they may start one at any time.
            reserved += @intFromBool(c.session == 0);
            // Count the number of requests queued.
            reserved += c.request_queue.count;
        }
        // +1 for the potential request — is there room in the sequencer's queue?
        if (reserved + 1 > simulator.reply_sequence.free()) return;

        // Make sure that there is capacity in the client's request queue.
        if (client.messages_available == 0) return;
        const request_message = client.get_message();
        errdefer client.release(request_message);

        const request_metadata = simulator.workload.build_request(
            client_index,
            request_message.buffer[@sizeOf(vsr.Header)..constants.message_size_max],
        );
        assert(request_metadata.size <= constants.message_size_max - @sizeOf(vsr.Header));

        simulator.cluster.request(
            client_index,
            request_metadata.operation,
            request_message,
            request_metadata.size,
        );
        // Since we already checked the client's request queue for free space, `client.request()`
        // should always queue the request.
        assert(request_message == client.request_queue.tail_ptr().?.message.base());
        assert(request_message.header.size == @sizeOf(vsr.Header) + request_metadata.size);
        assert(request_message.header.into(.request).?.operation.cast(StateMachine) ==
            request_metadata.operation);

        simulator.requests_sent += 1;
        assert(simulator.requests_sent <= simulator.options.requests_max);
    }

    fn tick_crash(simulator: *Simulator) void {
        const recoverable_count_min =
            vsr.quorums(simulator.options.cluster.replica_count).view_change;

        var recoverable_count: usize = 0;
        for (simulator.cluster.replicas, 0..) |*replica, i| {
            recoverable_count += @intFromBool(simulator.cluster.replica_health[i] == .up and
                !replica.standby() and
                replica.status != .recovering_head and
                replica.syncing == .idle);
        }

        for (simulator.cluster.replicas) |*replica| {
            simulator.replica_stability[replica.replica] -|= 1;
            const stability = simulator.replica_stability[replica.replica];
            if (stability > 0) continue;

            const replica_storage = &simulator.cluster.storages[replica.replica];
            switch (simulator.cluster.replica_health[replica.replica]) {
                .up => {
                    const replica_writes = replica_storage.writes.count();
                    const crash_probability = simulator.options.replica_crash_probability *
                        @as(f64, if (replica_writes == 0) 1.0 else 10.0);
                    if (!chance_f64(simulator.random, crash_probability)) continue;

                    recoverable_count -= @intFromBool(!replica.standby() and
                        replica.status != .recovering_head and
                        replica.syncing == .idle);

                    log.debug("{}: crash replica", .{replica.replica});
                    simulator.cluster.crash_replica(replica.replica);

                    simulator.replica_stability[replica.replica] =
                        simulator.options.replica_crash_stability;
                },
                .down => {
                    if (!chance_f64(
                        simulator.random,
                        simulator.options.replica_restart_probability,
                    )) {
                        continue;
                    }

                    const fault = recoverable_count >= recoverable_count_min or replica.standby();
                    simulator.restart_replica(replica.replica, fault);
                },
            }
        }
    }

    fn restart_replica(simulator: *Simulator, replica_index: u8, fault: bool) void {
        assert(simulator.cluster.replica_health[replica_index] == .down);

        const replica_storage = &simulator.cluster.storages[replica_index];

        if (!fault) {
            // The journal writes redundant headers of faulty ops as zeroes to ensure
            // that they remain faulty after a crash/recover. Since that fault cannot
            // be disabled by `storage.faulty`, we must manually repair it here to
            // ensure a cluster cannot become stuck in status=recovering_head.
            // See recover_slots() for more detail.
            const headers_offset = vsr.Zone.wal_headers.offset(0);
            const headers_size = vsr.Zone.wal_headers.size().?;
            const headers_bytes = replica_storage.memory[headers_offset..][0..headers_size];
            for (
                mem.bytesAsSlice(vsr.Header.Prepare, headers_bytes),
                replica_storage.wal_prepares(),
            ) |*wal_header, *wal_prepare| {
                if (wal_header.checksum == 0) {
                    wal_header.* = wal_prepare.header;
                }
            }
        }

        log.debug("{}: restart replica (faults={})", .{
            replica_index,
            fault,
        });

        replica_storage.faulty = fault;
        simulator.cluster.restart_replica(replica_index) catch unreachable;

        const replica: *const Cluster.Replica = &simulator.cluster.replicas[replica_index];
        if (replica.status == .recovering_head) {
            // Even with faults disabled, a replica that was syncing before it crashed
            // (or just recently finished syncing before it crashed) may wind up in
            // status=recovering_head.
            assert(fault or replica.op < replica.op_checkpoint());
        }

        replica_storage.faulty = true;
        simulator.replica_stability[replica_index] =
            simulator.options.replica_restart_stability;
    }
};

/// Print an error message and then exit with an exit code.
fn fatal(failure: Failure, comptime fmt_string: []const u8, args: anytype) noreturn {
    output.err(fmt_string, args);
    std.os.exit(@intFromEnum(failure));
}

/// Returns true, `p` percent of the time, else false.
fn chance(random: std.rand.Random, p: u8) bool {
    assert(p <= 100);
    return random.uintLessThanBiased(u8, 100) < p;
}

/// Returns true, `p` percent of the time, else false.
fn chance_f64(random: std.rand.Random, p: f64) bool {
    assert(p <= 100.0);
    return random.float(f64) * 100.0 < p;
}

/// Returns a random partitioning mode.
fn random_partition_mode(random: std.rand.Random) PartitionMode {
    const typeInfo = @typeInfo(PartitionMode).Enum;
    var enumAsInt = random.uintAtMost(typeInfo.tag_type, typeInfo.fields.len - 1);
    return @as(PartitionMode, @enumFromInt(enumAsInt));
}

fn random_partition_symmetry(random: std.rand.Random) PartitionSymmetry {
    const typeInfo = @typeInfo(PartitionSymmetry).Enum;
    var enumAsInt = random.uintAtMost(typeInfo.tag_type, typeInfo.fields.len - 1);
    return @as(PartitionSymmetry, @enumFromInt(enumAsInt));
}

/// Returns a random fully-connected subgraph which includes at least view change
/// quorum of active replicas.
fn random_core(random: std.rand.Random, replica_count: u8, standby_count: u8) Core {
    assert(replica_count > 0);
    assert(replica_count <= constants.replicas_max);
    assert(standby_count <= constants.standbys_max);

    const quorum_view_change = vsr.quorums(replica_count).view_change;
    const replica_core_count = random.intRangeAtMost(u8, quorum_view_change, replica_count);
    const standby_core_count = random.intRangeAtMost(u8, 0, standby_count);

    var result: Core = Core.initEmpty();

    var need = replica_core_count;
    var left = replica_count;
    var replica: u8 = 0;
    while (replica < replica_count + standby_count) : (replica += 1) {
        if (random.uintLessThan(u8, left) < need) {
            result.set(replica);
            need -= 1;
        }
        left -= 1;

        if (replica == replica_count - 1) {
            // Having selected active replicas, switch to selection of standbys.
            assert(left == 0);
            assert(need == 0);
            assert(result.count() == replica_core_count);
            assert(result.count() >= quorum_view_change);
            left = standby_count;
            need = standby_core_count;
        }
    }
    assert(left == 0);
    assert(need == 0);
    assert(result.count() == replica_core_count + standby_core_count);

    return result;
}

pub fn parse_seed(bytes: []const u8) u64 {
    if (bytes.len == 40) {
        // Normally, a seed is specified as a base-10 integer. However, as a special case, we allow
        // using a Git hash (a hex string 40 character long). This is used by our CI, which passes
        // current commit hash as a seed --- that way, we run simulator on CI, we run it with
        // different, "random" seeds, but the the failures remain reproducible just from the commit
        // hash!
        const commit_hash = std.fmt.parseUnsigned(u160, bytes, 16) catch |err| switch (err) {
            error.Overflow => unreachable,
            error.InvalidCharacter => @panic("commit hash seed contains an invalid character"),
        };
        return @truncate(commit_hash);
    }

    return std.fmt.parseUnsigned(u64, bytes, 10) catch |err| switch (err) {
        error.Overflow => @panic("seed exceeds a 64-bit unsigned integer"),
        error.InvalidCharacter => @panic("seed contains an invalid character"),
    };
}

var log_buffer: std.io.BufferedWriter(4096, std.fs.File.Writer) = .{
    // This is initialized in main(), as std.io.getStdErr() is not comptime known on e.g. Windows.
    .unbuffered_writer = undefined,
};

fn log_override(
    comptime level: std.log.Level,
    comptime scope: @TypeOf(.EnumLiteral),
    comptime format: []const u8,
    args: anytype,
) void {
    if (vsr_simulator_options.log == .short and scope != .cluster) return;

    const prefix_default = "[" ++ @tagName(level) ++ "] " ++ "(" ++ @tagName(scope) ++ "): ";
    const prefix = if (vsr_simulator_options.log == .short) "" else prefix_default;

    // Print the message to stderr using a buffer to avoid many small write() syscalls when
    // providing many format arguments. Silently ignore failure.
    log_buffer.writer().print(prefix ++ format ++ "\n", args) catch {};

    // Flush the buffer before returning to ensure, for example, that a log message
    // immediately before a failing assertion is fully printed.
    log_buffer.flush() catch {};
}
