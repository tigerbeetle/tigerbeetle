const std = @import("std");
const stdx = @import("./stdx.zig");
const builtin = @import("builtin");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const mem = std.mem;
const ratio = stdx.PRNG.ratio;
const Ratio = stdx.PRNG.Ratio;

const constants = @import("constants.zig");
const flags = @import("./flags.zig");
const schema = @import("lsm/schema.zig");
const vsr = @import("vsr.zig");
const Header = vsr.Header;

const vsr_vopr_options = @import("vsr_vopr_options");
const state_machine = vsr_vopr_options.state_machine;
const StateMachineType = switch (state_machine) {
    .accounting => @import("state_machine.zig").StateMachineType,
    .testing => @import("testing/state_machine.zig").StateMachineType,
};

const Cluster = @import("testing/cluster.zig").ClusterType(StateMachineType);
const Release = @import("testing/cluster.zig").Release;
const StateMachine = Cluster.StateMachine;
const Failure = @import("testing/cluster.zig").Failure;
const PartitionMode = @import("testing/packet_simulator.zig").PartitionMode;
const PartitionSymmetry = @import("testing/packet_simulator.zig").PartitionSymmetry;
const Core = @import("testing/cluster/network.zig").Network.Core;
const ReplySequence = @import("testing/reply_sequence.zig").ReplySequence;
const Message = @import("message_pool.zig").MessagePool.Message;

const releases = [_]Release{
    .{
        .release = vsr.Release.from(.{ .major = 0, .minor = 0, .patch = 1 }),
        .release_client_min = vsr.Release.from(.{ .major = 0, .minor = 0, .patch = 1 }),
    },
    .{
        .release = vsr.Release.from(.{ .major = 0, .minor = 0, .patch = 2 }),
        .release_client_min = vsr.Release.from(.{ .major = 0, .minor = 0, .patch = 1 }),
    },
    .{
        .release = vsr.Release.from(.{ .major = 0, .minor = 0, .patch = 3 }),
        .release_client_min = vsr.Release.from(.{ .major = 0, .minor = 0, .patch = 1 }),
    },
};

const log = std.log.scoped(.simulator);

pub const std_options: std.Options = .{
    // The -vopr-log=<full|short> build option selects two logging modes.
    // In "short" mode, only state transitions are printed (see `Cluster.log_replica`).
    // "full" mode is the usual logging according to the level.
    .log_level = if (vsr_vopr_options.log == .short) .info else .debug,
    .logFn = log_override,

    // Uncomment if you need per-scope control over the log levels.
    // pub const log_scope_levels: []const std.log.ScopeLevel = &.{
    //     .{ .scope = .cluster, .level = .info },
    //     .{ .scope = .replica, .level = .debug },
    // };
};

pub const tigerbeetle_config = @import("config.zig").configs.test_min;

const cluster_id = 0;
const MiB = 1024 * 1024;

const CLIArgs = struct {
    // "lite" mode runs a small cluster and only looks for crashes.
    lite: bool = false,
    performance: bool = false,
    // Feel free to add more runtime overrides here!
    ticks_max_requests: u32 = 40_000_000,
    ticks_max_convergence: u32 = 10_000_000,
    packet_loss_ratio: ?Ratio = null,

    positional: struct {
        seed: ?[]const u8 = null,
    },
};

pub fn main() !void {
    // This must be initialized at runtime as stderr is not comptime known on e.g. Windows.
    log_buffer.unbuffered_writer = std.io.getStdErr().writer();

    // TODO Use std.testing.allocator when all deinit() leaks are fixed.
    const allocator = std.heap.page_allocator;

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    const cli_args = flags.parse(&args, CLIArgs);
    if (cli_args.lite and cli_args.performance) {
        return vsr.fatal(.cli, "--lite and --performance are mutually exclusive", .{});
    }
    log_performance_mode = cli_args.performance;

    const seed_random = std.crypto.random.int(u64);
    const seed = seed_from_arg: {
        const seed_argument = cli_args.positional.seed orelse break :seed_from_arg seed_random;
        break :seed_from_arg vsr.testing.parse_seed(seed_argument);
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
        if (vsr_vopr_options.log != .short) {
            log.warn("no seed provided: full debug logs are enabled, this will be slow", .{});
        }
    }

    var prng = stdx.PRNG.from_seed(seed);

    var options = if (cli_args.lite)
        options_lite(&prng)
    else if (cli_args.performance)
        options_performance(&prng)
    else
        options_swarm(&prng);

    if (cli_args.packet_loss_ratio) |packet_loss_ratio| {
        options.network.packet_loss_probability = packet_loss_ratio;
    }

    log.info(
        \\
        \\          SEED={}
        \\
        \\          replicas={}
        \\          standbys={}
        \\          clients={}
        \\          request_probability={}
        \\          idle_on_probability={}
        \\          idle_off_probability={}
        \\          one_way_delay_mean={} ticks
        \\          one_way_delay_min={} ticks
        \\          packet_loss_probability={}
        \\          path_maximum_capacity={} messages
        \\          path_clog_duration_mean={} ticks
        \\          path_clog_probability={}
        \\          packet_replay_probability={}
        \\          partition_mode={s}
        \\          partition_symmetry={s}
        \\          partition_probability={}
        \\          unpartition_probability={}
        \\          partition_stability={} ticks
        \\          unpartition_stability={} ticks
        \\          read_latency_min={}
        \\          read_latency_mean={}
        \\          write_latency_min={}
        \\          write_latency_mean={}
        \\          read_fault_probability={}
        \\          write_fault_probability={}
        \\          crash_probability={}
        \\          crash_stability={} ticks
        \\          restart_probability={}
        \\          restart_stability={} ticks
    , .{
        seed,
        options.cluster.replica_count,
        options.cluster.standby_count,
        options.cluster.client_count,
        options.request_probability,
        options.request_idle_on_probability,
        options.request_idle_off_probability,
        options.network.one_way_delay_mean,
        options.network.one_way_delay_min,
        options.network.packet_loss_probability,
        options.network.path_maximum_capacity,
        options.network.path_clog_duration_mean,
        options.network.path_clog_probability,
        options.network.packet_replay_probability,
        @tagName(options.network.partition_mode),
        @tagName(options.network.partition_symmetry),
        options.network.partition_probability,
        options.network.unpartition_probability,
        options.network.partition_stability,
        options.network.unpartition_stability,
        options.storage.read_latency_min,
        options.storage.read_latency_mean,
        options.storage.write_latency_min,
        options.storage.write_latency_mean,
        options.storage.read_fault_probability,
        options.storage.write_fault_probability,
        options.replica_crash_probability,
        options.replica_crash_stability,
        options.replica_restart_probability,
        options.replica_restart_stability,
    });

    var simulator = try Simulator.init(allocator, &prng, options);
    defer simulator.deinit(allocator);

    // Warm-up the cluster before performance testing to get past the initial view change.
    if (cli_args.performance) {
        simulator.options.request_probability = ratio(0, 100);
        for (0..500) |_| simulator.tick();
        simulator.options.request_probability = options.request_probability;
    }

    for (0..simulator.cluster.clients.len) |client_index| {
        simulator.cluster.register(client_index);
    }

    // Safety: replicas crash and restart; at any given point in time arbitrarily many replicas may
    // be crashed, but each replica restarts eventually. The cluster must process all requests
    // without split-brain.
    var tick_total: u64 = 0;
    var tick: u64 = 0;
    while (tick < cli_args.ticks_max_requests) : (tick += 1) {
        const requests_replied_old = simulator.requests_replied;
        simulator.tick();
        tick_total += 1;
        if (simulator.requests_replied > requests_replied_old) {
            tick = 0;
        }
        const requests_done = simulator.requests_replied == simulator.options.requests_max;
        const upgrades_done =
            for (simulator.cluster.replicas, simulator.cluster.replica_health) |*replica, health|
        {
            if (health == .down) continue;
            const release_latest = releases[simulator.replica_releases_limit - 1].release;
            if (replica.release.value == release_latest.value) {
                break true;
            }
        } else false;

        if (requests_done and upgrades_done) break;
    } else {
        if (cli_args.lite) return;

        // Safety mode ran out of ticks without completing its requests, so now we check whether it
        // was correct to do so.
        //
        // Heal current network partitions, disable future process, storage, and network faults
        // on all replicas. Run this fully-connected core of replicas for 500_000 ticks, which
        // should enough to ensure all faulty grid blocks, headers, or prepares that can be repaired
        // are repaired. After this, all we must be left with are correlated faults.
        {
            var replica: u8 = 0;
            var core: Core = .{};
            while (replica < simulator.options.cluster.replica_count) : (replica += 1) {
                core.set(replica);
            }

            simulator.transition_to_liveness_mode(core);

            tick = 0;
            while (tick < 500_000) : (tick += 1) simulator.tick();
        }

        log.info(
            "no liveness, final cluster state (requests_max={} requests_replied={}):",
            .{ simulator.options.requests_max, simulator.requests_replied },
        );
        simulator.cluster.log_cluster();
        if (try simulator.cluster_recoverable(allocator)) {
            log.err("you can reproduce this failure with seed={}", .{seed});
            fatal(.liveness, "unable to complete requests_committed_max before ticks_max", .{});
        } else return;
    }

    if (cli_args.lite or cli_args.performance) {
        // Don't care about convergence.
    } else {
        // Liveness: a core set of replicas is up and fully connected. The rest of replicas might be
        // crashed or partitioned permanently. The core should converge to the same state.
        const core = random_core(
            simulator.prng,
            simulator.options.cluster.replica_count,
            simulator.options.cluster.standby_count,
        );
        simulator.transition_to_liveness_mode(core);

        tick = 0;
        while (tick < cli_args.ticks_max_convergence) : (tick += 1) {
            simulator.tick();
            tick_total += 1;
            if (simulator.pending() == null) {
                break;
            }
        }

        if (simulator.pending()) |reason| {
            if (try simulator.cluster_recoverable(allocator)) {
                log.info("no liveness, final cluster state (core={b}):", .{simulator.core.bits});
                simulator.cluster.log_cluster();
                log.err("you can reproduce this failure with seed={}", .{seed});
                fatal(.liveness, "no state convergence: {s}", .{reason});
            }
        } else {
            const commits = simulator.cluster.state_checker.commits.items;
            const last_checksum = commits[commits.len - 1].header.checksum;
            for (simulator.cluster.aofs, 0..) |*aof, replica_index| {
                if (simulator.core.is_set(replica_index)) {
                    try aof.validate(last_checksum);
                } else {
                    try aof.validate(null);
                }
            }
        }
    }

    if (cli_args.performance) {
        log.info("\nMessages:\n{}", .{simulator.cluster.network.message_summary});
    } else {
        log.debug("\nMessages:\n{}", .{simulator.cluster.network.message_summary});
    }

    log.info("\n          PASSED ({} ticks)", .{tick_total});
}

fn options_swarm(prng: *stdx.PRNG) Simulator.Options {
    const replica_count = prng.range_inclusive(u8, 1, constants.replicas_max);
    const standby_count = prng.int_inclusive(u8, constants.standbys_max);
    const node_count = replica_count + standby_count;
    // -1 since otherwise it is possible that all clients will evict each other.
    // (Due to retried register messages from the first set of evicted clients.
    // See the "Cluster: eviction: session_too_low" replica test for a related scenario.)
    const client_count = prng.range_inclusive(u8, 1, constants.clients_max * 2 - 1);

    const batch_size_limit_min = comptime batch_size_limit_min: {
        var event_size_max: u32 = @sizeOf(vsr.RegisterRequest);
        for (std.enums.values(StateMachine.Operation)) |operation| {
            const event_size = @sizeOf(StateMachine.EventType(operation));
            event_size_max = @max(event_size_max, event_size);
        }
        break :batch_size_limit_min event_size_max;
    };
    const batch_size_limit: u32 = if (prng.boolean())
        constants.message_body_size_max
    else
        prng.range_inclusive(u32, batch_size_limit_min, constants.message_body_size_max);

    const storage_size_limit = vsr.sector_floor(
        200 * MiB - prng.int_inclusive(u64, 20 * MiB),
    );

    const cluster_options: Cluster.Options = .{
        .cluster_id = cluster_id,
        .replica_count = replica_count,
        .standby_count = standby_count,
        .client_count = client_count,
        .storage_size_limit = storage_size_limit,
        .seed = prng.int(u64),
        .releases = &releases,
        .client_release = releases[0].release,

        .state_machine = switch (state_machine) {
            .testing => .{
                .batch_size_limit = batch_size_limit,
                .lsm_forest_node_count = 4096,
            },
            .accounting => .{
                .batch_size_limit = batch_size_limit,
                .lsm_forest_compaction_block_count = prng.int_inclusive(u32, 256) +
                    StateMachine.Forest.Options.compaction_block_count_min,
                .lsm_forest_node_count = 4096,
                .cache_entries_accounts = if (prng.boolean()) 256 else 0,
                .cache_entries_transfers = if (prng.boolean()) 256 else 0,
                .cache_entries_posted = if (prng.boolean()) 256 else 0,
            },
        },
    };

    const network_options: Cluster.NetworkOptions = .{
        .node_count = node_count,
        .client_count = client_count,

        .seed = prng.int(u64),

        .one_way_delay_mean = prng.range_inclusive(u16, 3, 10),
        .one_way_delay_min = prng.int_inclusive(u16, 3),
        .packet_loss_probability = ratio(prng.int_inclusive(u8, 30), 100),
        .path_maximum_capacity = prng.range_inclusive(u8, 2, 20),
        .path_clog_duration_mean = prng.int_inclusive(u16, 500),
        .path_clog_probability = ratio(prng.int_inclusive(u8, 2), 100),
        .packet_replay_probability = ratio(prng.int_inclusive(u8, 50), 100),

        .partition_mode = prng.enum_uniform(PartitionMode),
        .partition_symmetry = prng.enum_uniform(PartitionSymmetry),
        .partition_probability = ratio(prng.int_inclusive(u8, 3), 100),
        .unpartition_probability = ratio(prng.range_inclusive(u8, 1, 10), 100),
        .partition_stability = 100 + prng.int_inclusive(u32, 100),
        .unpartition_stability = prng.int_inclusive(u32, 20),
    };

    const read_latency_min = prng.range_inclusive(u16, 0, 3);
    const write_latency_min = prng.range_inclusive(u16, 0, 3);
    const storage_options: Cluster.Storage.Options = .{
        .seed = prng.int(u64),
        .read_latency_min = read_latency_min,
        .read_latency_mean = prng.range_inclusive(u16, read_latency_min, 10),
        .write_latency_min = write_latency_min,
        .write_latency_mean = prng.range_inclusive(u16, write_latency_min, 100),
        .read_fault_probability = ratio(prng.range_inclusive(u8, 0, 10), 100),
        .write_fault_probability = ratio(prng.range_inclusive(u8, 0, 10), 100),
        .write_misdirect_probability = ratio(prng.range_inclusive(u8, 0, 10), 100),
        .crash_fault_probability = ratio(prng.range_inclusive(u8, 80, 100), 100),
    };
    const storage_fault_atlas: Cluster.StorageFaultAtlas.Options = .{
        .faulty_superblock = true,
        .faulty_wal_headers = replica_count > 1,
        .faulty_wal_prepares = replica_count > 1,
        .faulty_client_replies = replica_count > 1,
        // >2 instead of >1 because in R=2, a lagging replica may sync to the leading replica,
        // but then the leading replica may have the only copy of a block in the cluster.
        .faulty_grid = replica_count > 2,
    };

    const workload_options = StateMachine.Workload.Options.generate(prng, .{
        .batch_size_limit = batch_size_limit,
        .client_count = client_count,
        // TODO(DJ) Once Workload no longer needs in_flight_max, make stalled_queue_capacity
        // private. Also maybe make it dynamic (computed from the client_count instead of
        // clients_max).
        .in_flight_max = ReplySequence.stalled_queue_capacity,
    });

    return .{
        .cluster = cluster_options,
        .network = network_options,
        .storage = storage_options,
        .storage_fault_atlas = storage_fault_atlas,
        .workload = workload_options,
        // TODO Swarm testing: Test long+few crashes and short+many crashes separately.
        .replica_crash_probability = ratio(2, 10_000_000),
        .replica_crash_stability = prng.int_inclusive(u32, 1_000),
        .replica_restart_probability = ratio(2, 1_000_000),
        .replica_restart_stability = prng.int_inclusive(u32, 1_000),

        .replica_pause_probability = ratio(8, 10_000_000),
        .replica_pause_stability = prng.int_inclusive(u32, 1_000),
        .replica_unpause_probability = ratio(8, 1_000_000),
        .replica_unpause_stability = prng.int_inclusive(u32, 1_000),

        .replica_release_advance_probability = ratio(1, 1_000_000),
        .replica_release_catchup_probability = ratio(1, 100_000),

        .requests_max = constants.journal_slot_count * 3,
        .request_probability = ratio(prng.range_inclusive(u8, 1, 100), 100),
        .request_idle_on_probability = ratio(prng.range_inclusive(u8, 0, 20), 100),
        .request_idle_off_probability = ratio(prng.range_inclusive(u8, 10, 20), 100),
    };
}

fn options_lite(prng: *stdx.PRNG) Simulator.Options {
    var base = options_swarm(prng);
    base.cluster.replica_count = 3;
    base.cluster.standby_count = 0;
    base.network.node_count = 3;
    return base;
}

fn options_performance(prng: *stdx.PRNG) Simulator.Options {
    const cluster_options: Cluster.Options = .{
        .cluster_id = cluster_id,
        .replica_count = 6,
        .standby_count = 0,
        .client_count = 4,
        .storage_size_limit = vsr.sector_floor(200 * MiB),
        .seed = prng.int(u64),
        .releases = releases[0..1],
        .client_release = releases[0].release,

        .state_machine = switch (state_machine) {
            .testing => .{
                .batch_size_limit = constants.message_body_size_max,
                .lsm_forest_node_count = 4096,
            },
            .accounting => .{
                .batch_size_limit = constants.message_body_size_max,
                .lsm_forest_compaction_block_count = 128 +
                    StateMachine.Forest.Options.compaction_block_count_min,
                .lsm_forest_node_count = 4096,
                .cache_entries_accounts = 256,
                .cache_entries_transfers = 0,
                .cache_entries_posted = 0,
            },
        },
    };

    const network_options: Cluster.NetworkOptions = .{
        .node_count = cluster_options.replica_count,
        .client_count = cluster_options.client_count,

        .seed = prng.int(u64),

        .one_way_delay_mean = 5,
        .one_way_delay_min = 0,
        .packet_loss_probability = ratio(0, 100),
        .path_maximum_capacity = 10,
        .path_clog_duration_mean = 200,
        .path_clog_probability = ratio(0, 100),
        .packet_replay_probability = ratio(0, 100),

        .partition_mode = .none,
        .partition_symmetry = .symmetric,
        .partition_probability = ratio(0, 100),
        .unpartition_probability = ratio(0, 100),
        .partition_stability = 100,
        .unpartition_stability = 10,
    };

    const storage_options: Cluster.Storage.Options = .{
        .seed = prng.int(u64),
        .read_latency_min = 0,
        .read_latency_mean = 0,
        .write_latency_min = 0,
        .write_latency_mean = 0,
        .read_fault_probability = ratio(0, 100),
        .write_fault_probability = ratio(0, 100),
        .write_misdirect_probability = ratio(0, 100),
        .crash_fault_probability = ratio(0, 100),
    };
    const storage_fault_atlas: Cluster.StorageFaultAtlas.Options = .{
        .faulty_superblock = false,
        .faulty_wal_headers = false,
        .faulty_wal_prepares = false,
        .faulty_client_replies = false,
        .faulty_grid = false,
    };

    var workload_prng = stdx.PRNG.from_seed(92); // Fix workload for perf testing.
    const workload_options = StateMachine.Workload.Options.generate(&workload_prng, .{
        .batch_size_limit = constants.message_body_size_max,
        .client_count = cluster_options.client_count,
        .in_flight_max = ReplySequence.stalled_queue_capacity,
    });

    return .{
        .cluster = cluster_options,
        .network = network_options,
        .storage = storage_options,
        .storage_fault_atlas = storage_fault_atlas,
        .workload = workload_options,
        .replica_crash_probability = ratio(0, 100),
        .replica_crash_stability = 500,
        .replica_restart_probability = ratio(0, 100),
        .replica_restart_stability = 500,

        .replica_pause_probability = ratio(0, 100),
        .replica_pause_stability = 500,
        .replica_unpause_probability = ratio(0, 100),
        .replica_unpause_stability = 500,

        .replica_release_advance_probability = ratio(0, 100),
        .replica_release_catchup_probability = ratio(0, 100),

        .requests_max = constants.journal_slot_count * 8,
        .request_probability = ratio(100, 100),
        .request_idle_on_probability = ratio(0, 100),
        .request_idle_off_probability = ratio(100, 100),
    };
}

pub const Simulator = struct {
    pub const Options = struct {
        cluster: Cluster.Options,
        network: Cluster.NetworkOptions,
        storage: Cluster.Storage.Options,
        storage_fault_atlas: Cluster.StorageFaultAtlas.Options,

        workload: StateMachine.Workload.Options,

        /// Probability per tick that a crash will occur.
        replica_crash_probability: Ratio,
        /// Minimum duration of a crash.
        replica_crash_stability: u32,
        /// Probability per tick that a crashed replica will recovery.
        replica_restart_probability: Ratio,
        /// Minimum time a replica is up until it is crashed again.
        replica_restart_stability: u32,

        replica_pause_probability: Ratio,
        replica_pause_stability: u32,
        replica_unpause_probability: Ratio,
        replica_unpause_stability: u32,

        /// Probability per tick that a healthy replica will be crash-upgraded.
        /// This probability is set to 0 during liveness mode.
        replica_release_advance_probability: Ratio,
        /// Probability that a crashed with an outdated version will be upgraded as it restarts.
        /// This helps ensure that when the cluster upgrades, that replicas without the newest
        /// version don't take too long to receive that new version.
        /// This probability is set to 0 during liveness mode.
        replica_release_catchup_probability: Ratio,

        /// The total number of requests to send. Does not count `register` messages.
        requests_max: usize,
        request_probability: Ratio,
        request_idle_on_probability: Ratio,
        request_idle_off_probability: Ratio,
    };

    prng: *stdx.PRNG,
    options: Options,
    cluster: *Cluster,
    workload: StateMachine.Workload,

    // The number of releases in each replica's "binary".
    replica_releases: []usize,
    /// The maximum number of releases available in any replica's "binary".
    /// (i.e. the maximum of any `replica_releases`.)
    replica_releases_limit: usize = 1,

    /// Protect a replica from fast successive crash/restarts.
    replica_crash_stability: []usize,
    reply_sequence: ReplySequence,
    reply_op_next: u64 = 1, // Skip the root op.

    /// Fully-connected subgraph of replicas for liveness checking.
    core: Core = .{},

    /// Total number of requests sent, including those that have not been delivered.
    /// Does not include `register` messages.
    requests_sent: usize = 0,
    /// Total number of replies received by non-evicted clients.
    /// Does not include `register` messages.
    requests_replied: usize = 0,
    requests_idle: bool = false,

    pub fn init(
        allocator: std.mem.Allocator,
        prng: *stdx.PRNG,
        options: Options,
    ) !Simulator {
        assert(options.requests_max > 0);
        assert(options.request_probability.numerator > 0);
        assert(options.request_idle_off_probability.numerator > 0);

        var cluster = try Cluster.init(allocator, .{
            .cluster = options.cluster,
            .network = options.network,
            .storage = options.storage,
            .storage_fault_atlas = options.storage_fault_atlas,
            .callbacks = .{
                .on_cluster_reply = on_cluster_reply,
                .on_client_reply = on_client_reply,
            },
        });
        errdefer cluster.deinit();

        var workload = try StateMachine.Workload.init(allocator, prng, options.workload);
        errdefer workload.deinit(allocator);

        const replica_releases = try allocator.alloc(
            usize,
            options.cluster.replica_count + options.cluster.standby_count,
        );
        errdefer allocator.free(replica_releases);
        @memset(replica_releases, 1);

        const replica_crash_stability = try allocator.alloc(
            usize,
            options.cluster.replica_count + options.cluster.standby_count,
        );
        errdefer allocator.free(replica_crash_stability);
        @memset(replica_crash_stability, 0);

        var reply_sequence = try ReplySequence.init(allocator);
        errdefer reply_sequence.deinit(allocator);

        return Simulator{
            .prng = prng,
            .options = options,
            .cluster = cluster,
            .workload = workload,
            .replica_releases = replica_releases,
            .replica_crash_stability = replica_crash_stability,
            .reply_sequence = reply_sequence,
        };
    }

    pub fn deinit(simulator: *Simulator, allocator: std.mem.Allocator) void {
        allocator.free(simulator.replica_releases);
        allocator.free(simulator.replica_crash_stability);
        simulator.reply_sequence.deinit(allocator);
        simulator.workload.deinit(allocator);
        simulator.cluster.deinit();
    }

    pub fn pending(simulator: *const Simulator) ?[]const u8 {
        assert(simulator.core.count() > 0);
        assert(simulator.requests_sent - simulator.requests_cancelled() ==
            simulator.options.requests_max);
        assert(simulator.reply_sequence.empty());
        for (
            simulator.cluster.clients,
            simulator.cluster.client_eviction_reasons,
        ) |*client, reason| {
            if (reason == null) {
                if (client.request_inflight) |request| {
                    // Registration isn't counted by requests_sent, so an operation=register may
                    // still be in-flight. Any other requests should already be complete before
                    // done() is called.
                    assert(request.message.header.operation == .register);
                    return "pending register request";
                }
            }
        }

        // Even though there are no client requests in progress, the cluster may be upgrading.
        const release_max = simulator.core_release_max();
        for (simulator.cluster.replicas) |*replica| {
            if (simulator.core.is_set(replica.replica)) {
                // (If down, the replica is waiting to be upgraded.)
                maybe(simulator.cluster.replica_health[replica.replica] == .down);

                if (replica.release.value != release_max.value) return "pending upgrade";
            }
        }

        for (simulator.cluster.replicas) |*replica| {
            if (simulator.core.is_set(replica.replica)) {
                if (!simulator.cluster.state_checker.replica_convergence(replica.replica)) {
                    return "pending replica convergence";
                }
            }
        }

        simulator.cluster.state_checker.assert_cluster_convergence();

        // Check whether the replica is still repairing prepares/tables/replies.
        const commit_max: u64 = simulator.cluster.state_checker.commits.items.len - 1;
        for (simulator.cluster.replicas) |*replica| {
            if (simulator.core.is_set(replica.replica)) {
                for (replica.op_checkpoint() + 1..commit_max + 1) |op| {
                    const header = simulator.cluster.state_checker.header_with_op(op);
                    if (!replica.journal.has_prepare(&header)) return "pending journal";
                }
                // It's okay for a replica to miss some prepares older than the current checkpoint.
                maybe(replica.journal.faulty.count > 0);

                if (!replica.sync_content_done()) return "pending sync content";
            }
        }

        // Expect that all core replicas have arrived at an identical (non-divergent) checkpoint.
        var checkpoint_id: ?u128 = null;
        for (simulator.cluster.replicas) |*replica| {
            if (simulator.core.is_set(replica.replica)) {
                const replica_checkpoint_id = replica.superblock.working.checkpoint_id();
                if (checkpoint_id) |id| {
                    assert(checkpoint_id == id);
                } else {
                    checkpoint_id = replica_checkpoint_id;
                }
            }
        }
        assert(checkpoint_id != null);

        return null;
    }

    pub fn tick(simulator: *Simulator) void {
        // TODO(Zig): Remove (see on_cluster_reply()).
        simulator.cluster.context = simulator;

        simulator.cluster.tick();
        simulator.tick_requests();
        simulator.tick_crash();
        simulator.tick_pause();
    }

    pub fn cluster_recoverable(simulator: *Simulator, allocator: std.mem.Allocator) !bool {
        if (simulator.core_missing_primary()) {
            unimplemented("repair requires reachable primary");
        } else if (simulator.core_missing_quorum()) {
            log.warn("no liveness, core replicas cannot view-change", .{});
        } else if (try simulator.core_missing_prepare(allocator)) |header| {
            log.warn("no liveness, op={} is not available in core", .{header.op});
        } else if (try simulator.core_missing_blocks(allocator)) |blocks| {
            log.warn("no liveness, {} blocks are not available in core", .{blocks});
        } else if (simulator.core_missing_reply()) |header| {
            log.warn("no liveness, reply op={} is not available in core", .{header.op});
        } else {
            return true;
        }

        return false;
    }

    /// Executes the following:
    /// * Restart any core replicas that are down at the moment
    /// * Heal all network partitions between core replicas
    /// * Disable storage faults on the core replicas
    /// * For all failures involving non-core replicas, make those failures permanent.
    ///
    /// See https://tigerbeetle.com/blog/2023-07-06-simulation-testing-for-liveness for broader
    /// context.
    pub fn transition_to_liveness_mode(simulator: *Simulator, core: Core) void {
        log.debug("transition_to_liveness_mode: core={b}", .{core.bits});
        assert(simulator.core.count() == 0);
        defer assert(simulator.core.count() > 0);

        simulator.core = core;

        var it = core.iterate();
        while (it.next()) |replica_index| {
            const fault = false;
            if (simulator.cluster.replica_health[replica_index] == .down) {
                simulator.replica_restart(@intCast(replica_index), fault);
            }

            const replica_health = simulator.cluster.replica_health[replica_index];
            if (replica_health == .up and replica_health.up.paused) {
                simulator.cluster.replica_unpause(@intCast(replica_index));
            }

            simulator.cluster.storages[replica_index].transition_to_liveness_mode();
        }

        simulator.cluster.network.transition_to_liveness_mode(simulator.core);
        simulator.options.replica_crash_probability = ratio(0, 100);
        simulator.options.replica_restart_probability = ratio(0, 100);
        simulator.options.replica_pause_probability = ratio(0, 100);
        simulator.options.replica_release_advance_probability = ratio(0, 100);
        simulator.options.replica_release_catchup_probability = ratio(0, 100);
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
                !simulator.core.is_set(replica.replica))
            {
                // `replica` considers itself a primary, check that at least part of the core thinks
                // so as well.
                var it = simulator.core.iterate();
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
    /// replicas are in status=recovering_head (due to corruption), then that may be insufficient.
    pub fn core_missing_quorum(simulator: *const Simulator) bool {
        assert(simulator.core.count() > 0);

        var core_replicas: u8 = 0;
        var core_recovering_head: u8 = 0;
        for (simulator.cluster.replicas) |*replica| {
            if (simulator.core.is_set(replica.replica) and !replica.standby()) {
                core_replicas += 1;
                core_recovering_head += @intFromBool(replica.status == .recovering_head);
            }
        }

        if (core_recovering_head == 0) return false;

        const quorums = vsr.quorums(simulator.options.cluster.replica_count);
        return quorums.view_change > core_replicas - core_recovering_head;
    }

    fn core_repairable_replica(
        simulator: *const Simulator,
        comptime Replica: type,
        replica: *const Replica,
    ) bool {
        if (!simulator.core.is_set(replica.replica)) return false;
        if (replica.standby()) return false;
        switch (replica.status) {
            .normal => return true,
            .recovering_head => return false,
            // Lagging replicas do not initiate WAL repair during view change.
            .view_change => return !vsr.Checkpoint.durable(
                replica.op_checkpoint_next(),
                replica.commit_max,
            ),
            .recovering => unreachable,
        }
    }

    // Returns a header for a prepare which can't be repaired by the core due to storage faults.
    //
    // If a replica cannot make progress on committing, then it may be stuck while repairing either
    // missing headers *or* prepares (see `repair` in replica.zig). This function checks for both.
    //
    // When generating a FaultAtlas, we don't try to protect core from excessive errors. Instead,
    // if the core gets stuck, we verify that this is indeed due to storage faults.
    pub fn core_missing_prepare(
        simulator: *const Simulator,
        allocator: std.mem.Allocator,
    ) error{OutOfMemory}!?vsr.Header.Prepare {
        assert(simulator.core.count() > 0);
        const replica_count = simulator.options.cluster.replica_count;

        var cluster_op_head: u64 = 0;
        var cluster_commit_max: u64 = 0;
        var cluster_log_view: u32 = 0;
        // maxInt(u16) is more than enough to accommodate `requests_max`.
        var cluster_op_repair_min: u64 = std.math.maxInt(u16);

        for (simulator.cluster.replicas) |replica| {
            if (!simulator.core_repairable_replica(Cluster.Replica, &replica)) continue;

            cluster_commit_max = @max(cluster_commit_max, replica.commit_max);
            cluster_log_view = @max(cluster_log_view, replica.log_view);
            cluster_op_head = @max(cluster_op_head, replica.op);
            cluster_op_repair_min = @min(cluster_op_repair_min, replica.op_repair_min());
        }
        assert(cluster_commit_max <= cluster_op_head);

        // Use replicas with the largest log_view to infer uncommitted headers. Replicas with a
        // smaller log_view may have an outdated version of the same uncommitted op. There may be
        // at most a pipeline of uncommitted headers in the cluster.
        const pipeline_max = constants.pipeline_prepare_queue_max;
        var uncommitted_headers = [_]?vsr.Header.Prepare{null} ** pipeline_max;
        if (cluster_commit_max < cluster_op_head) {
            for (simulator.cluster.replicas) |replica| {
                if (!simulator.core_repairable_replica(Cluster.Replica, &replica)) continue;

                if (replica.log_view < cluster_log_view) continue;
                for (cluster_commit_max + 1..cluster_op_head + 1) |op| {
                    if (replica.journal.header_with_op(op)) |header| {
                        if (uncommitted_headers[op % pipeline_max]) |header_existing| {
                            assert(header_existing.op == header.op);
                            assert(header_existing.view == header.view);
                            assert(header_existing.checksum == header.checksum);
                        } else {
                            uncommitted_headers[op % pipeline_max] = header.*;
                        }
                    }
                }
            }
        }

        const ReplicaSet = stdx.BitSetType(constants.replicas_max);
        var replicas_missing_ops = try allocator.alloc(
            ReplicaSet,
            cluster_op_head - cluster_op_repair_min + 1,
        );
        errdefer allocator.free(replicas_missing_ops);

        for (replicas_missing_ops, cluster_op_repair_min..) |*replicas_missing_op, op| {
            replicas_missing_op.* = .{};
            const header = blk: {
                if (op > cluster_commit_max) {
                    const uncommitted_header = uncommitted_headers[op % pipeline_max].?;
                    assert(uncommitted_header.op == op);
                    break :blk uncommitted_header;
                } else {
                    break :blk simulator.cluster.state_checker.header_with_op(op);
                }
            };
            for (simulator.cluster.replicas) |replica| {
                // Replicas should be able to repair using any other replica in the core.
                if (replica.standby()) continue;
                if (!simulator.core.is_set(replica.replica) or
                    !replica.journal.has_prepare(&header))
                {
                    replicas_missing_op.set(replica.replica);
                }
            }
        }

        // Check whether any of the uncommitted headers is corrupted on more than a nack
        // quorum of replicas. If so, the cluster cannot initiate repair or commit (see the
        // awaiting_repair and complete_invalid cases in the DVCQuorum).
        const nack_quorum = vsr.quorums(replica_count).nack_prepare;
        for (cluster_commit_max..cluster_op_head + 1) |op| {
            if (replicas_missing_ops[op - cluster_op_repair_min].count() >= nack_quorum) {
                const header = blk: {
                    if (op > cluster_commit_max) {
                        const uncommitted_header = uncommitted_headers[op % pipeline_max].?;
                        assert(uncommitted_header.op == op);
                        break :blk uncommitted_header;
                    } else {
                        break :blk simulator.cluster.state_checker.header_with_op(op);
                    }
                };
                return header;
            }
        }

        for (simulator.cluster.replicas) |replica| {
            if (!simulator.core_repairable_replica(Cluster.Replica, &replica)) continue;

            // Check prepares between (commit_min, commit_max], replicas repair these first
            // as commit progress depends on them.
            if (replica.commit_min < replica.commit_max) {
                for (replica.commit_min + 1..replica.commit_max + 1) |op| {
                    if (replicas_missing_ops[op - cluster_op_repair_min].count() == replica_count) {
                        return simulator.cluster.state_checker.header_with_op(op);
                    }
                }
            }

            // Check prepares between [op_repair_min, commit_min] as view changing replicas
            // cannot step up as primary unless they have all prepares intact.
            if (replica.op_repair_min() <= replica.commit_min) {
                for (replica.op_repair_min()..replica.commit_min + 1) |op| {
                    if (replicas_missing_ops[op - cluster_op_repair_min].count() == replica_count) {
                        return simulator.cluster.state_checker.header_with_op(op);
                    }
                }
            }
        }
        return null;
    }

    /// Check whether the cluster is stuck because the entire core is missing the same block[s].
    pub fn core_missing_blocks(
        simulator: *const Simulator,
        allocator: std.mem.Allocator,
    ) error{OutOfMemory}!?usize {
        assert(simulator.core.count() > 0);

        const FaultyReplicas = stdx.BitSetType(constants.members_max);
        var blocks_missing = std.AutoArrayHashMap(
            struct { address: u64, checksum: u128 },
            FaultyReplicas,
        ).init(allocator);
        defer blocks_missing.deinit();

        // Find all blocks that any replica in the core is missing.
        for (simulator.cluster.replicas) |replica| {
            if (!simulator.core.is_set(replica.replica)) continue;

            const storage = &simulator.cluster.storages[replica.replica];

            var fault_iterator = replica.grid.read_global_queue.peek();
            while (fault_iterator) |faulty_read| : (fault_iterator = faulty_read.next) {
                const v = try blocks_missing.getOrPut(.{
                    .address = faulty_read.address,
                    .checksum = faulty_read.checksum,
                });

                if (!v.found_existing) v.value_ptr.* = .{};
                v.value_ptr.set(replica.replica);

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
                const v = try blocks_missing.getOrPut(.{
                    .address = fault.key_ptr.*,
                    .checksum = fault.value_ptr.checksum,
                });

                if (!v.found_existing) v.value_ptr.* = .{};
                v.value_ptr.set(replica.replica);

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
        var blocks_missing_iterator = blocks_missing.iterator();
        while (blocks_missing_iterator.next()) |block_missing_and_faulty_replicas| {
            const block_missing = block_missing_and_faulty_replicas.key_ptr;
            const faulty_replicas = block_missing_and_faulty_replicas.value_ptr;
            for (simulator.cluster.replicas) |replica| {
                const storage = &simulator.cluster.storages[replica.replica];

                // A replica might actually have the block that it is requesting, but not know.
                // This can occur after state sync: if we compact and create a table, but then skip
                // over that table via state sync, we will try to sync the table anyway.
                if (faulty_replicas.is_set(replica.replica)) continue;

                if (!simulator.core.is_set(replica.replica)) continue;
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

        if (blocks_missing.count() == 0) {
            return null;
        } else {
            return blocks_missing.count();
        }
    }

    /// Check whether the cluster is stuck because the entire core is missing the same reply[s].
    pub fn core_missing_reply(simulator: *const Simulator) ?vsr.Header.Reply {
        assert(simulator.core.count() > 0);

        var replies_latest = [_]?vsr.Header.Reply{null} ** constants.clients_max;
        for (simulator.cluster.replicas) |replica| {
            for (replica.client_sessions.entries, 0..) |entry, entry_slot| {
                if (entry.session != 0) {
                    if (replies_latest[entry_slot] == null or
                        replies_latest[entry_slot].?.op < entry.header.op)
                    {
                        replies_latest[entry_slot] = entry.header;
                    }
                }
            }
        }

        for (replies_latest, 0..) |reply_or_empty, reply_slot| {
            const reply = reply_or_empty orelse continue;
            const reply_in_core = for (simulator.cluster.replicas) |replica| {
                const storage = &simulator.cluster.storages[replica.replica];
                const storage_replies = storage.client_replies();
                const storage_reply = storage_replies[reply_slot];

                if (simulator.core.is_set(replica.replica) and !replica.standby()) {
                    if (storage_reply.header.checksum == reply.checksum and
                        !storage.area_faulty(.{ .client_replies = .{ .slot = reply_slot } }))
                    {
                        break true;
                    }
                }
            } else false;
            if (!reply_in_core) return reply;
        }
        return null;
    }

    fn core_release_max(simulator: *const Simulator) vsr.Release {
        assert(simulator.core.count() > 0);

        var release_max: vsr.Release = vsr.Release.zero;
        for (simulator.cluster.replicas) |*replica| {
            if (simulator.core.is_set(replica.replica)) {
                release_max = release_max.max(replica.release);
                if (replica.upgrade_release) |release| {
                    release_max = release_max.max(release);
                }
            }
        }
        assert(release_max.value > 0);
        return release_max;
    }

    fn on_cluster_reply(
        cluster: *Cluster,
        reply_client: ?usize,
        prepare: *const Message.Prepare,
        reply: *const Message.Reply,
    ) void {
        assert((reply_client == null) == (prepare.header.client == 0));

        const simulator: *Simulator = @ptrCast(@alignCast(cluster.context.?));

        if (reply.header.op < simulator.reply_op_next) return;
        if (simulator.reply_sequence.contains(reply)) return;

        simulator.reply_sequence.insert(reply_client, prepare, reply);

        while (!simulator.reply_sequence.empty()) {
            const op = simulator.reply_op_next;
            const prepare_header = simulator.cluster.state_checker.commits.items[op].header;
            assert(prepare_header.op == op);

            if (simulator.reply_sequence.peek(op)) |commit| {
                defer simulator.reply_sequence.next();

                simulator.reply_op_next += 1;

                assert(commit.reply.references == 1);
                assert(commit.reply.header.op == op);
                assert(commit.reply.header.command == .reply);
                assert(commit.reply.header.request == commit.prepare.header.request);
                assert(commit.reply.header.operation == commit.prepare.header.operation);
                assert(commit.prepare.references == 1);
                assert(commit.prepare.header.checksum == prepare_header.checksum);
                assert(commit.prepare.header.command == .prepare);

                log.debug("consume_stalled_replies: op={} operation={} client={} request={}", .{
                    commit.reply.header.op,
                    commit.reply.header.operation,
                    commit.prepare.header.client,
                    commit.prepare.header.request,
                });

                if (prepare_header.operation == .pulse) {
                    simulator.workload.on_pulse(
                        prepare_header.operation.cast(StateMachine),
                        prepare_header.timestamp,
                    );
                }

                if (!commit.prepare.header.operation.vsr_reserved()) {
                    simulator.workload.on_reply(
                        commit.client_index.?,
                        commit.reply.header.operation.cast(StateMachine),
                        commit.reply.header.timestamp,
                        commit.prepare.body_used(),
                        commit.reply.body_used(),
                    );
                }
            }
        }
    }

    fn on_client_reply(
        cluster: *Cluster,
        reply_client: usize,
        request: *const Message.Request,
        reply: *const Message.Reply,
    ) void {
        _ = reply;

        const simulator: *Simulator = @ptrCast(@alignCast(cluster.context.?));
        assert(simulator.cluster.client_eviction_reasons[reply_client] == null);

        if (!request.header.operation.vsr_reserved()) {
            simulator.requests_replied += 1;
        }
    }

    /// Maybe send a request from one of the cluster's clients.
    fn tick_requests(simulator: *Simulator) void {
        if (simulator.requests_idle) {
            if (simulator.prng.chance(simulator.options.request_idle_off_probability)) {
                simulator.requests_idle = false;
            }
        } else {
            if (simulator.prng.chance(simulator.options.request_idle_on_probability)) {
                simulator.requests_idle = true;
            }
        }

        if (simulator.requests_idle) return;
        if (simulator.requests_sent - simulator.requests_cancelled() ==
            simulator.options.requests_max) return;
        if (!simulator.prng.chance(simulator.options.request_probability)) return;

        const client_index = index: {
            const client_count = simulator.options.cluster.client_count;
            const client_index_base =
                simulator.prng.int_inclusive(usize, client_count - 1);
            for (0..client_count) |offset| {
                const client_index = (client_index_base + offset) % client_count;
                if (simulator.cluster.client_eviction_reasons[client_index] == null) {
                    break :index client_index;
                }
            } else {
                for (0..client_count) |index| {
                    assert(simulator.cluster.client_eviction_reasons[index] != null);
                    assert(simulator.cluster.client_eviction_reasons[index] == .no_session or
                        simulator.cluster.client_eviction_reasons[index] == .session_too_low);
                }
                unimplemented("client replacement; all clients were evicted");
            }
        };

        var client = &simulator.cluster.clients[client_index];

        // Messages aren't added to the ReplySequence until a reply arrives.
        // Before sending a new message, make sure there will definitely be room for it.
        var reserved: usize = 0;
        for (simulator.cluster.clients) |*c| {
            // Count the number of clients that are still waiting for a `register` to complete,
            // since they may start one at any time.
            reserved += @intFromBool(c.session == 0);
            // Count the number of non-register requests queued.
            reserved += @intFromBool(c.request_inflight != null);
        }
        // +1 for the potential request — is there room in the sequencer's queue?
        if (reserved + 1 > simulator.reply_sequence.free()) return;

        // Make sure that the client is ready to send a new request.
        if (client.request_inflight != null) return;
        const request_message = client.get_message();
        errdefer client.release_message(request_message);

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
        assert(request_message == client.request_inflight.?.message.base());
        assert(request_message.header.size == @sizeOf(vsr.Header) + request_metadata.size);
        assert(request_message.header.into(.request).?.operation.cast(StateMachine) ==
            request_metadata.operation);

        simulator.requests_sent += 1;
        assert(simulator.requests_sent - simulator.requests_cancelled() <=
            simulator.options.requests_max);
    }

    fn tick_crash(simulator: *Simulator) void {
        for (simulator.cluster.replicas) |*replica| {
            simulator.replica_crash_stability[replica.replica] -|= 1;
            if (simulator.replica_crash_stability[replica.replica] > 0) continue;

            switch (simulator.cluster.replica_health[replica.replica]) {
                .up => |up| {
                    if (!up.paused) simulator.tick_crash_up(replica);
                },
                .down => simulator.tick_crash_down(replica),
            }
        }
    }

    fn tick_crash_up(simulator: *Simulator, replica: *Cluster.Replica) void {
        const replica_storage = &simulator.cluster.storages[replica.replica];
        const replica_writes = replica_storage.writes.count();

        const crash_upgrade =
            simulator.replica_releases[replica.replica] < releases.len and
            simulator.prng.chance(simulator.options.replica_release_advance_probability);
        if (crash_upgrade) simulator.replica_upgrade(replica.replica);

        var crash_probability = simulator.options.replica_crash_probability;
        if (replica_writes > 0) crash_probability.numerator *= 10;

        const crash_random = simulator.prng.chance(crash_probability);

        if (!crash_upgrade and !crash_random) return;

        log.debug("{}: crash replica", .{replica.replica});
        simulator.cluster.replica_crash(replica.replica);

        simulator.replica_crash_stability[replica.replica] =
            simulator.options.replica_crash_stability;
    }

    fn tick_crash_down(simulator: *Simulator, replica: *Cluster.Replica) void {
        // If we are in liveness mode, we need to make sure that all replicas
        // (eventually) make it to the same release.
        const restart_upgrade =
            simulator.replica_releases[replica.replica] <
            simulator.replica_releases_limit and
            (simulator.core.is_set(replica.replica) or
            simulator.prng.chance(simulator.options.replica_release_catchup_probability));
        if (restart_upgrade) simulator.replica_upgrade(replica.replica);

        const restart_random =
            simulator.prng.chance(simulator.options.replica_restart_probability);

        if (!restart_upgrade and !restart_random) return;

        const recoverable_count_min =
            vsr.quorums(simulator.options.cluster.replica_count).view_change;

        var recoverable_count: usize = 0;
        for (simulator.cluster.replicas, 0..) |*r, i| {
            recoverable_count += @intFromBool(simulator.cluster.replica_health[i] == .up and
                !r.standby() and
                r.status != .recovering_head and
                r.syncing == .idle);
        }

        // To improve VOPR utilization, try to prevent the replica from going into
        // `.recovering_head` state if the replica is needed to form a quorum.
        const fault = recoverable_count >= recoverable_count_min or replica.standby();
        simulator.replica_restart(replica.replica, fault);
        maybe(!fault and replica.status == .recovering_head);
    }

    fn replica_restart(simulator: *Simulator, replica_index: u8, fault: bool) void {
        assert(simulator.cluster.replica_health[replica_index] == .down);

        const replica_storage = &simulator.cluster.storages[replica_index];
        const replica: *const Cluster.Replica = &simulator.cluster.replicas[replica_index];

        {
            // If the entire Zone.wal_headers is corrupted, the replica becomes permanently
            // unavailable (returns `WALInvalid` from `open`). In the simulator, there are only two
            // WAL sectors, which could both get corrupted when a replica crashes while writing them
            // simultaneously. Repair both sectors so that even if one of them becomes corrupted on
            // startup, the replica still remains operational.
            //
            // In production `journal_iops_write_max < header_sector_count`, which makes is
            // impossible to get torn writes for all journal header sectors at the same time.
            const header_sector_offset =
                @divExact(vsr.Zone.wal_headers.start(), constants.sector_size);
            const header_sector_count =
                @divExact(constants.journal_size_headers, constants.sector_size);
            for (0..header_sector_count) |header_sector_index| {
                replica_storage.faults.unset(header_sector_offset + header_sector_index);
            }
            // TODO Clear misdirects? Waiting for a seed to confirm.
        }

        var header_prepare_view_mismatch: bool = false;
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
                } else {
                    if (wal_header.view != wal_prepare.header.view) {
                        header_prepare_view_mismatch = true;
                    }
                }
            }
        }

        const replica_releases_count = simulator.replica_releases[replica_index];
        log.debug("{}: restart replica (faults={} releases={})", .{
            replica_index,
            fault,
            replica_releases_count,
        });

        var replica_releases = vsr.ReleaseList{};
        for (0..replica_releases_count) |i| {
            replica_releases.append_assume_capacity(releases[i].release);
        }

        replica_storage.faulty = fault;
        simulator.cluster.replica_restart(
            replica_index,
            &replica_releases,
        ) catch unreachable;

        if (replica.status == .recovering_head) {
            // Even with faults disabled, a replica may wind up in status=recovering_head.
            assert(fault or header_prepare_view_mismatch);
        }

        replica_storage.faulty = true;
        simulator.replica_crash_stability[replica_index] =
            simulator.options.replica_restart_stability;
    }

    fn replica_upgrade(simulator: *Simulator, replica_index: u8) void {
        simulator.replica_releases[replica_index] =
            @min(simulator.replica_releases[replica_index] + 1, releases.len);
        simulator.replica_releases_limit =
            @max(simulator.replica_releases[replica_index], simulator.replica_releases_limit);
    }

    // Randomly pause replicas. A paused replica doesn't tick and doesn't complete any asynchronous
    // work. The goals of pausing are:
    // - catch more interesting interleaving of events,
    // - simulate real-world scenario of VM migration.
    fn tick_pause(simulator: *Simulator) void {
        for (
            simulator.cluster.replicas,
            simulator.replica_crash_stability,
            0..,
        ) |*replica, *stability, replica_index| {
            stability.* -|= 1;
            if (stability.* > 0) continue;

            if (simulator.cluster.replica_health[replica.replica] == .down) continue;
            const paused = simulator.cluster.replica_health[replica.replica].up.paused;
            const pause = simulator.prng.chance(simulator.options.replica_pause_probability);
            const unpause = simulator.prng.chance(simulator.options.replica_unpause_probability);

            if (!paused and pause) {
                simulator.cluster.replica_pause(@intCast(replica_index));
                stability.* = simulator.options.replica_pause_stability;
            } else if (paused and unpause) {
                simulator.cluster.replica_unpause(@intCast(replica_index));
                stability.* = simulator.options.replica_unpause_stability;
            }
        }
    }

    fn requests_cancelled(simulator: *const Simulator) u32 {
        var count: u32 = 0;
        for (
            simulator.cluster.clients,
            simulator.cluster.client_eviction_reasons,
        ) |*client, reason| {
            count += @intFromBool(reason != null and
                client.request_inflight != null and
                client.request_inflight.?.message.header.operation != .register);
        }
        return count;
    }
};

/// Print an error message and then exit with an exit code.
fn fatal(failure: Failure, comptime fmt_string: []const u8, args: anytype) noreturn {
    log.err(fmt_string, args);
    std.process.exit(@intFromEnum(failure));
}

/// Signal that something is not yet fully implemented, and abort the process.
///
/// In VOPR, this will exit with status 0, to make it easy to find "real" failures by running
/// the simulator in a loop.
fn unimplemented(comptime message: []const u8) noreturn {
    const full_message = "unimplemented: " ++ message;
    log.info(full_message, .{});
    log.info("not crashing in VOPR", .{});
    std.process.exit(0);
}

/// Returns a random fully-connected subgraph which includes at least view change
/// quorum of active replicas.
fn random_core(prng: *stdx.PRNG, replica_count: u8, standby_count: u8) Core {
    assert(replica_count > 0);
    assert(replica_count <= constants.replicas_max);
    assert(standby_count <= constants.standbys_max);

    const quorum_view_change = vsr.quorums(replica_count).view_change;
    const replica_core_count = prng.range_inclusive(u8, quorum_view_change, replica_count);
    const standby_core_count = prng.range_inclusive(u8, 0, standby_count);

    var result: Core = .{};

    var combination = stdx.PRNG.Combination.init(.{
        .total = replica_count,
        .sample = replica_core_count,
    });
    for (0..replica_count) |replica| {
        if (combination.take(prng)) result.set(replica);
    }
    assert(combination.done());

    combination = stdx.PRNG.Combination.init(.{
        .total = standby_count,
        .sample = standby_core_count,
    });
    for (replica_count..replica_count + standby_count) |replica| {
        if (combination.take(prng)) result.set(replica);
    }
    assert(combination.done());

    assert(result.count() == replica_core_count + standby_core_count);

    return result;
}

var log_buffer: std.io.BufferedWriter(4096, std.fs.File.Writer) = .{
    // This is initialized in main(), as std.io.getStdErr() is not comptime known on e.g. Windows.
    .unbuffered_writer = undefined,
};

var log_performance_mode: bool = false;

fn log_override(
    comptime level: std.log.Level,
    comptime scope: @TypeOf(.EnumLiteral),
    comptime format: []const u8,
    args: anytype,
) void {
    if (vsr_vopr_options.log == .short) {
        if (scope != .cluster and scope != .simulator) return;
        if (log_performance_mode and scope != .simulator) return;
    }

    const prefix_default = "[" ++ @tagName(level) ++ "] " ++ "(" ++ @tagName(scope) ++ "): ";
    const prefix = if (vsr_vopr_options.log == .short) "" else prefix_default;

    // Print the message to stderr using a buffer to avoid many small write() syscalls when
    // providing many format arguments. Silently ignore failure.
    log_buffer.writer().print(prefix ++ format ++ "\n", args) catch {};

    // Flush the buffer before returning to ensure, for example, that a log message
    // immediately before a failing assertion is fully printed.
    log_buffer.flush() catch {};
}
