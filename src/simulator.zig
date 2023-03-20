const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const mem = std.mem;

const tb = @import("tigerbeetle.zig");
const constants = @import("constants.zig");
const vsr = @import("vsr.zig");
const Header = vsr.Header;

const state_machine = @import("vsr_simulator_options").state_machine;
const StateMachineType = switch (state_machine) {
    .accounting => @import("state_machine.zig").StateMachineType,
    .testing => @import("testing/state_machine.zig").StateMachineType,
};

const Client = @import("testing/cluster.zig").Client;
const Cluster = @import("testing/cluster.zig").ClusterType(StateMachineType);
const Replica = @import("testing/cluster.zig").Replica;
const StateMachine = Cluster.StateMachine;
const Failure = @import("testing/cluster.zig").Failure;
const PartitionMode = @import("testing/packet_simulator.zig").PartitionMode;
const PartitionSymmetry = @import("testing/packet_simulator.zig").PartitionSymmetry;
const ReplySequence = @import("testing/reply_sequence.zig").ReplySequence;
const IdPermutation = @import("testing/id.zig").IdPermutation;
const Message = @import("message_pool.zig").MessagePool.Message;

/// The `log` namespace in this root file is required to implement our custom `log` function.
const output = std.log.scoped(.state_checker);

/// Set this to `false` if you want to see how literally everything works.
/// This will run much slower but will trace all logic across the cluster.
const log_state_transitions_only = true;

const log_simulator = std.log.scoped(.simulator);

pub const tigerbeetle_config = @import("config.zig").configs.test_min;

/// You can fine tune your log levels even further (debug/info/warn/err):
pub const log_level: std.log.Level = if (log_state_transitions_only) .info else .debug;

const cluster_id = 0;

pub fn main() !void {
    // This must be initialized at runtime as stderr is not comptime known on e.g. Windows.
    log_buffer.unbuffered_writer = std.io.getStdErr().writer();

    // TODO Use std.testing.allocator when all deinit() leaks are fixed.
    const allocator = std.heap.page_allocator;

    var args = std.process.args();

    // Skip argv[0] which is the name of this executable:
    _ = args_next(&args, allocator);

    const seed_random = std.crypto.random.int(u64);
    const seed = seed_from_arg: {
        const arg_two = args_next(&args, allocator) orelse break :seed_from_arg seed_random;
        defer allocator.free(arg_two);
        break :seed_from_arg parse_seed(arg_two);
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
        if (log_level == .debug) {
            output.warn("no seed provided: full debug logs are enabled, this will be slow", .{});
        }
    }

    var prng = std.rand.DefaultPrng.init(seed);
    const random = prng.random();

    const replica_count = 3;
    const standby_count = 0;
    const node_count = replica_count + standby_count;
    const client_count = 1;

    // const quorums = vsr.quorums(replica_count);
    const replicas_dead_max = 1;
    // A cluster-of-2 is special-cased to mirror the special case in replica.zig.
    // See repair_prepare()/on_nack_prepare().
    const storage_faults_max = 0;

    const cluster_options = Cluster.Options{
        .cluster_id = cluster_id,
        .replica_count = replica_count,
        .standby_count = standby_count,
        .client_count = client_count,
        .storage_size_limit = vsr.sector_floor(
            constants.storage_size_max - random.uintLessThan(u64, constants.storage_size_max / 10),
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
            .faults_max = storage_faults_max,
            .faulty_superblock = true,
            .faulty_wal_headers = replica_count > 1,
            .faulty_wal_prepares = replica_count > 1,
        },
        .state_machine = switch (state_machine) {
            .testing => .{},
            .accounting => .{
                .lsm_forest_node_count = 4096,
                .cache_entries_accounts = if (random.boolean()) 0 else 2048,
                .cache_entries_transfers = 0,
                .cache_entries_posted = if (random.boolean()) 0 else 2048,
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
        .replica_crash_probability = 0.0002,
        .replica_crash_stability = random.uintLessThan(u32, 1_000),
        .replica_death_probability = 50.0,
        .replica_restart_probability = 0.0002,
        .replica_restart_stability = random.uintLessThan(u32, 1_000),
        .replicas_dead_max = replicas_dead_max,
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
        \\          death_probability={d}%
        \\          deaths_max={}
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
        simulator_options.replica_crash_probability * 100,
        simulator_options.replica_crash_stability,
        simulator_options.replica_restart_probability * 100,
        simulator_options.replica_restart_stability,
        simulator_options.replica_death_probability,
        simulator_options.replicas_dead_max,
    });

    var simulator = try Simulator.init(allocator, random, simulator_options);
    defer simulator.deinit(allocator);

    const ticks_max = 200_000_000;
    var tick: u64 = 0;
    while (tick < ticks_max) : (tick += 1) {
        simulator.tick();
        if (simulator.done()) break;
    } else {
        output.err("you can reproduce this failure with seed={}", .{seed});
        fatal(.liveness, "unable to complete requests_committed_max before ticks_max", .{});
    }
    assert(simulator.done());

    output.info("\n          PASSED ({} ticks)", .{tick});
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
        /// Probability that, given that a crash occured, the replica remains permanently unavailable.
        replica_death_probability: f64,
        /// Maximum number of permanent crashes allowed.
        replicas_dead_max: u8,

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
    /// How many non-standby replicas crashed permanently.
    replicas_dead: u8 = 0,
    reply_sequence: ReplySequence,

    /// Total number of requests sent, including those that have not been delivered.
    /// Does not include `register` messages.
    requests_sent: usize = 0,
    requests_idle: bool = false,

    /// Dead (unrestartable) replicas are indicated by a flag value in stability.
    const death_stability = std.math.maxInt(usize);

    pub fn init(allocator: std.mem.Allocator, random: std.rand.Random, options: Options) !Simulator {
        assert(options.replica_crash_probability < 100.0);
        assert(options.replica_crash_probability >= 0.0);
        assert(options.replica_restart_probability < 100.0);
        assert(options.replica_restart_probability >= 0.0);
        assert(options.replica_death_probability < 100.0);
        assert(options.replica_death_probability >= 0.0);
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
        std.mem.set(usize, replica_stability, 0);

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

    pub fn done(simulator: *Simulator) bool {
        assert(simulator.requests_sent <= simulator.options.requests_max);

        for (simulator.cluster.replicas) |*replica| {
            const down = simulator.cluster.replica_health[replica.replica] == .down;
            const dead = simulator.replica_stability[replica.replica] == death_stability;
            if (down and !dead) return false;

            if (!dead) {
                if (!simulator.cluster.state_checker.replica_convergence(replica.replica)) {
                    return false;
                }
            }
        }

        simulator.cluster.state_checker.assert_cluster_convergence();

        if (!simulator.reply_sequence.empty()) return false;
        if (simulator.requests_sent < simulator.options.requests_max) return false;

        for (simulator.cluster.clients) |*client| {
            if (client.request_queue.count > 0) return false;
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

    fn on_cluster_reply(
        cluster: *Cluster,
        reply_client: usize,
        request: *Message,
        reply: *Message,
    ) void {
        // TODO(Zig) Use @returnAddress to initialzie the cluster, then this can just use @fieldParentPtr().
        const simulator = @ptrCast(*Simulator, @alignCast(@alignOf(Simulator), cluster.context.?));
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

            log_simulator.debug("consume_stalled_replies: op={} operation={} client={} request={}", .{
                commit.reply.header.op,
                commit.reply.header.operation,
                commit.request.header.client,
                commit.request.header.request,
            });

            if (commit.request.header.operation != .register) {
                simulator.workload.on_reply(
                    commit.client_index,
                    commit.reply.header.operation,
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

        // Make sure that there is capacity in the client's request queue so that we never trigger
        // error.TooManyOutstandingRequests.
        if (client.request_queue.count + 1 > constants.client_request_queue_max) return;

        // Messages aren't added to the ReplySequence until a reply arrives.
        // Before sending a new message, make sure there will definitely be room for it.
        var reserved: usize = 0;
        for (simulator.cluster.clients) |*c| {
            // Count the number of clients that are still waiting for a `register` to complete,
            // since they may start one at any time.
            reserved += @boolToInt(c.session == 0);
            // Count the number of requests queued.
            reserved += c.request_queue.count;
        }
        // +1 for the potential request — is there room in the sequencer's queue?
        if (reserved + 1 > simulator.reply_sequence.free()) return;

        var request_message = client.get_message();
        defer client.unref(request_message);

        const request_metadata = simulator.workload.build_request(
            client_index,
            @alignCast(
                @alignOf(vsr.Header),
                request_message.buffer[@sizeOf(vsr.Header)..constants.message_size_max],
            ),
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
        assert(request_message == client.request_queue.tail_ptr().?.message);
        assert(request_message.header.size == @sizeOf(vsr.Header) + request_metadata.size);
        assert(request_message.header.operation.cast(StateMachine) == request_metadata.operation);

        simulator.requests_sent += 1;
        assert(simulator.requests_sent <= simulator.options.requests_max);
    }

    fn tick_crash(simulator: *Simulator) void {
        const recoverable_count_min =
            vsr.quorums(simulator.options.cluster.replica_count).view_change;
        var recoverable_count: usize = 0;
        for (simulator.cluster.replicas) |*replica| {
            recoverable_count += @boolToInt(
                replica.status != .recovering_head and !replica.standby() and
                    simulator.replica_stability[replica.replica] != death_stability,
            );
        }

        for (simulator.cluster.replicas) |*replica| {
            if (simulator.replica_stability[replica.replica] == death_stability) continue;

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

                    const death_probability = simulator.options.replica_death_probability;

                    log_simulator.debug("{}: crash replica", .{replica.replica});
                    simulator.cluster.crash_replica(replica.replica);

                    recoverable_count -=
                        @boolToInt(replica.status == .recovering_head and !replica.standby());

                    simulator.replica_stability[replica.replica] =
                        simulator.options.replica_crash_stability;

                    if (replica.standby() or simulator.replicas_dead < simulator.options.replicas_dead_max) {
                        if (chance_f64(simulator.random, death_probability)) {
                            log_simulator.debug("{}: kill replica", .{replica.replica});
                            simulator.replicas_dead += @boolToInt(!replica.standby());
                            simulator.replica_stability[replica.replica] = death_stability;
                        }
                    }
                },
                .down => {
                    if (!chance_f64(
                        simulator.random,
                        simulator.options.replica_restart_probability,
                    )) {
                        continue;
                    }

                    const fault = recoverable_count > recoverable_count_min or replica.standby();
                    if (!fault) {
                        // The journal writes redundant headers of faulty ops as zeroes to ensure
                        // that they remain faulty after a crash/recover. Since that fault cannot
                        // be disabled by `storage.faulty`, we must manually repair it here to
                        // ensure a cluster cannot become stuck in status=recovering_head.
                        // See recover_slots() for more detail.
                        const offset = vsr.Zone.wal_headers.offset(0);
                        const size = vsr.Zone.wal_headers.size().?;
                        const headers_bytes = replica_storage.memory[offset..][0..size];
                        const headers = mem.bytesAsSlice(vsr.Header, headers_bytes);
                        for (headers) |*h, slot| {
                            if (h.checksum == 0) h.* = replica_storage.wal_prepares()[slot].header;
                        }
                    }

                    log_simulator.debug("{}: restart replica (faults={})", .{
                        replica.replica,
                        fault,
                    });

                    replica_storage.faulty = fault;
                    simulator.cluster.restart_replica(replica.replica) catch unreachable;
                    assert(replica.status != .recovering_head or fault);

                    replica_storage.faulty = true;
                    simulator.replica_stability[replica.replica] =
                        simulator.options.replica_restart_stability;
                },
            }
        }
    }
};

fn dump(simulator: *const Simulator) void {
    output.err("dump:\n", .{});
    for (simulator.cluster.replicas) |replica| {
        if (simulator.cluster.replica_health[replica.replica] == .down) continue;
        output.err("{}: view={} op={} commit_min={} chck={:3} trgr={:3} status={} view_headers_op={}", .{
            replica.replica,
            replica.view,
            replica.op,
            replica.commit_min,
            replica.op_checkpoint(),
            replica.op_checkpoint_trigger(),
            replica.status,
            replica.view_headers.array.get(0).op,
        });
    }
}

/// Print an error message and then exit with an exit code.
fn fatal(failure: Failure, comptime fmt_string: []const u8, args: anytype) noreturn {
    output.err(fmt_string, args);
    std.os.exit(@enumToInt(failure));
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

/// Returns the next argument for the simulator or null (if none available)
fn args_next(args: *std.process.ArgIterator, allocator: std.mem.Allocator) ?[:0]const u8 {
    const err_or_bytes = args.next(allocator) orelse return null;
    return err_or_bytes catch @panic("Unable to extract next value from args");
}

/// Returns a random partitioning mode.
fn random_partition_mode(random: std.rand.Random) PartitionMode {
    const typeInfo = @typeInfo(PartitionMode).Enum;
    var enumAsInt = random.uintAtMost(typeInfo.tag_type, typeInfo.fields.len - 1);
    return @intToEnum(PartitionMode, enumAsInt);
}

fn random_partition_symmetry(random: std.rand.Random) PartitionSymmetry {
    const typeInfo = @typeInfo(PartitionSymmetry).Enum;
    var enumAsInt = random.uintAtMost(typeInfo.tag_type, typeInfo.fields.len - 1);
    return @intToEnum(PartitionSymmetry, enumAsInt);
}

pub fn parse_seed(bytes: []const u8) u64 {
    return std.fmt.parseUnsigned(u64, bytes, 10) catch |err| switch (err) {
        error.Overflow => @panic("seed exceeds a 64-bit unsigned integer"),
        error.InvalidCharacter => @panic("seed contains an invalid character"),
    };
}

var log_buffer: std.io.BufferedWriter(4096, std.fs.File.Writer) = .{
    // This is initialized in main(), as std.io.getStdErr() is not comptime known on e.g. Windows.
    .unbuffered_writer = undefined,
};

pub fn log(
    comptime level: std.log.Level,
    comptime scope: @TypeOf(.EnumLiteral),
    comptime format: []const u8,
    args: anytype,
) void {
    if (log_state_transitions_only and scope != .state_checker) return;

    const prefix_default = "[" ++ @tagName(level) ++ "] " ++ "(" ++ @tagName(scope) ++ "): ";
    const prefix = if (log_state_transitions_only) "" else prefix_default;

    // Print the message to stderr using a buffer to avoid many small write() syscalls when
    // providing many format arguments. Silently ignore failure.
    log_buffer.writer().print(prefix ++ format ++ "\n", args) catch {};

    // Flush the buffer before returning to ensure, for example, that a log message
    // immediately before a failing assertion is fully printed.
    log_buffer.flush() catch {};
}
