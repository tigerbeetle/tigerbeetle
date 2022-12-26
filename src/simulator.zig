const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const mem = std.mem;

const tb = @import("tigerbeetle.zig");
const constants = @import("constants.zig");
const vsr = @import("vsr.zig");
const Header = vsr.Header;

const Client = @import("test/cluster.zig").Client;
const Cluster = @import("test/cluster.zig").Cluster;
const ClusterOptions = @import("test/cluster.zig").ClusterOptions;
const Replica = @import("test/cluster.zig").Replica;
const StateMachine = @import("test/cluster.zig").StateMachine;
const StateChecker = @import("test/state_checker.zig").StateChecker;
const StorageChecker = @import("test/storage_checker.zig").StorageChecker;
const PartitionMode = @import("test/packet_simulator.zig").PartitionMode;
const MessageBus = @import("test/message_bus.zig").MessageBus;
const Conductor = @import("test/conductor.zig").Conductor;
const IdPermutation = @import("test/id.zig").IdPermutation;
const Message = @import("message_pool.zig").MessagePool.Message;

/// The `log` namespace in this root file is required to implement our custom `log` function.
const output = std.log.scoped(.state_checker);

/// Set this to `false` if you want to see how literally everything works.
/// This will run much slower but will trace all logic across the cluster.
const log_state_transitions_only = builtin.mode != .Debug;

const log_simulator = std.log.scoped(.simulator);

pub const tigerbeetle_config = @import("config.zig").configs.test_min;

/// You can fine tune your log levels even further (debug/info/warn/err):
pub const log_level: std.log.Level = if (log_state_transitions_only) .info else .debug;

const cluster_id = 0;

var cluster: *Cluster = undefined;
var conductor: *Conductor = undefined;
var state_checker: *StateChecker = undefined;
var storage_checker: *StorageChecker = undefined;

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

    const replica_count = 1 + random.uintLessThan(u8, constants.replicas_max);
    const client_count = 1 + random.uintLessThan(u8, constants.clients_max);

    const ticks_max = 50_000_000;
    const request_probability = 1 + random.uintLessThan(u8, 99);
    const idle_on_probability = random.uintLessThan(u8, 20);
    const idle_off_probability = 10 + random.uintLessThan(u8, 10);

    // The maximum number of transitions from calling `client.request()`, not including
    // `register` messages.
    const requests_committed_max: usize = constants.journal_slot_count * 3;

    const cluster_options: ClusterOptions = .{
        .cluster_id = cluster_id,
        .replica_count = replica_count,
        .client_count = client_count,
        .storage_size_limit = vsr.sector_floor(
            constants.storage_size_max - random.uintLessThan(u64, constants.storage_size_max / 10),
        ),
        .seed = random.int(u64),
        .on_client_reply = on_cluster_reply,
        .on_replica_change_state = on_replica_change_state,
        .on_replica_compact = on_replica_compact,
        .on_replica_checkpoint = on_replica_checkpoint,
        .network_options = .{
            .packet_simulator_options = .{
                .replica_count = replica_count,
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
                .partition_probability = random.uintLessThan(u8, 3),
                .unpartition_probability = 1 + random.uintLessThan(u8, 10),
                .partition_stability = 100 + random.uintLessThan(u32, 100),
                .unpartition_stability = random.uintLessThan(u32, 20),
            },
        },
        .storage_options = .{
            .seed = random.int(u64),
            .read_latency_min = random.uintLessThan(u16, 3),
            .read_latency_mean = 3 + random.uintLessThan(u16, 10),
            .write_latency_min = random.uintLessThan(u16, 3),
            .write_latency_mean = 3 + random.uintLessThan(u16, 100),
            .read_fault_probability = random.uintLessThan(u8, 10),
            .write_fault_probability = random.uintLessThan(u8, 10),
            .crash_fault_probability = 80 + random.uintLessThan(u8, 21),
            .faulty_superblock = true,
        },
        .health_options = .{
            .crash_probability = 0.000001,
            .crash_stability = random.uintLessThan(u32, 1_000),
            .restart_probability = 0.0001,
            .restart_stability = random.uintLessThan(u32, 1_000),
        },
        // TODO(dj) SimulatorType(StateMachine).init(state_machine_options)
        .state_machine_options = switch (constants.state_machine) {
            .testing => .{},
            .accounting => .{
                .lsm_forest_node_count = 4096,
                .cache_entries_accounts = if (random.boolean()) 0 else 2048,
                .cache_entries_transfers = 0,
                .cache_entries_posted = if (random.boolean()) 0 else 2048,
            },
        },
    };

    output.info(
        \\
        \\          SEED={}
        \\
        \\          replicas={}
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
        replica_count,
        client_count,
        request_probability,
        idle_on_probability,
        idle_off_probability,
        cluster_options.network_options.packet_simulator_options.one_way_delay_mean,
        cluster_options.network_options.packet_simulator_options.one_way_delay_min,
        cluster_options.network_options.packet_simulator_options.packet_loss_probability,
        cluster_options.network_options.packet_simulator_options.path_maximum_capacity,
        cluster_options.network_options.packet_simulator_options.path_clog_duration_mean,
        cluster_options.network_options.packet_simulator_options.path_clog_probability,
        cluster_options.network_options.packet_simulator_options.packet_replay_probability,
        cluster_options.network_options.packet_simulator_options.partition_mode,
        cluster_options.network_options.packet_simulator_options.partition_probability,
        cluster_options.network_options.packet_simulator_options.unpartition_probability,
        cluster_options.network_options.packet_simulator_options.partition_stability,
        cluster_options.network_options.packet_simulator_options.unpartition_stability,
        cluster_options.storage_options.read_latency_min,
        cluster_options.storage_options.read_latency_mean,
        cluster_options.storage_options.write_latency_min,
        cluster_options.storage_options.write_latency_mean,
        cluster_options.storage_options.read_fault_probability,
        cluster_options.storage_options.write_fault_probability,
        cluster_options.health_options.crash_probability * 100,
        cluster_options.health_options.crash_stability,
        cluster_options.health_options.restart_probability * 100,
        cluster_options.health_options.restart_stability,
    });

    cluster = try Cluster.create(allocator, cluster_options);
    defer cluster.destroy();

    const workload_options = StateMachine.Workload.Options.generate(random, .{
        .client_count = client_count,
        .in_flight_max = Conductor.stalled_queue_capacity,
    });

    var workload = try StateMachine.Workload.init(allocator, random, workload_options);
    defer workload.deinit(allocator);

    conductor = try allocator.create(Conductor);
    defer allocator.destroy(conductor);

    conductor.* = try Conductor.init(allocator, random, cluster, &workload, .{
        .requests_max = requests_committed_max,
        .request_probability = request_probability,
        .idle_on_probability = idle_on_probability,
        .idle_off_probability = idle_off_probability,
    });
    defer conductor.deinit(allocator);

    state_checker = try allocator.create(StateChecker);
    defer allocator.destroy(state_checker);

    state_checker.* = try StateChecker.init(
        allocator,
        cluster_id,
        cluster.replicas,
        cluster.clients,
    );
    defer state_checker.deinit();

    storage_checker = try allocator.create(StorageChecker);
    defer allocator.destroy(storage_checker);

    storage_checker.* = StorageChecker.init(allocator);
    defer storage_checker.deinit();

    // The minimum number of healthy replicas required for a crashed replica to be able to recover.
    const replica_normal_min = replicas: {
        if (replica_count == 1) {
            // A cluster of 1 can crash safely (as long as there is no disk corruption) since it
            // does not run the recovery protocol.
            break :replicas 0;
        } else {
            break :replicas cluster.replicas[0].quorum_view_change;
        }
    };

    var tick: u64 = 0;
    while (tick < ticks_max) : (tick += 1) {
        const health_options = &cluster.options.health_options;
        // The maximum number of replicas that can crash, with the cluster still able to recover.
        var crashes = cluster.replica_normal_count() -| replica_normal_min;

        for (cluster.storages) |*storage, replica| {
            if (cluster.replicas[replica].journal.status == .recovered) {
                // TODO Remove this workaround when VSR recovery protocol is disabled.
                // When only the minimum number of replicas are healthy (no more crashes allowed),
                // disable storage faults on all healthy replicas.
                //
                // This is a workaround to avoid the deadlock that occurs when (for example) in a
                // cluster of 3 replicas, one is down, another has a corrupt prepare, and the last does
                // not have the prepare. The two healthy replicas can never complete a view change,
                // because two replicas are not enough to nack, and the unhealthy replica cannot
                // complete the VSR recovery protocol either.
                if (cluster.replica_health[replica] == .up and crashes == 0) {
                    if (storage.faulty) {
                        log_simulator.debug("{}: disable storage faults", .{replica});
                        storage.faulty = false;
                    }
                } else {
                    // When a journal recovers for the first time, enable its storage faults.
                    // Future crashes will recover in the presence of faults.
                    if (!storage.faulty) {
                        log_simulator.debug("{}: enable storage faults", .{replica});
                        storage.faulty = true;
                    }
                }
            }
        }

        for (cluster.replicas) |*replica, index| {
            switch (cluster.replica_health[replica.replica]) {
                .up => |*ticks| {
                    ticks.* -|= 1;
                    replica.tick();
                    cluster.storages[index].tick();

                    state_checker.check_state(replica.replica) catch |err| {
                        fatal(.correctness, "state checker error: {}", .{err});
                    };

                    if (ticks.* != 0) continue;
                    if (crashes == 0) continue;
                    if (cluster.storages[replica.replica].writes.count() == 0) {
                        if (!chance_f64(random, health_options.crash_probability)) continue;
                    } else {
                        if (!chance_f64(random, health_options.crash_probability * 10.0)) continue;
                    }

                    const replica_crashed = try cluster.crash_replica(replica.replica);
                    if (!replica_crashed) continue;
                    log_simulator.debug("{}: crash replica", .{replica.replica});
                    crashes -= 1;
                },
                .down => |*ticks| {
                    ticks.* -|= 1;
                    assert(replica.status == .recovering);
                    if (ticks.* == 0 and chance_f64(random, health_options.restart_probability)) {
                        cluster.restart_replica(replica.replica);
                        log_simulator.debug("{}: restart replica", .{replica.replica});
                    }
                },
            }
        }

        cluster.tick();
        conductor.tick();

        if (state_checker.convergence() and conductor.done() and
            cluster.replica_up_count() == replica_count)
        {
            break;
        }
    } else {
        output.err("you can reproduce this failure with seed={}", .{seed});
        fatal(.liveness, "unable to complete requests_committed_max before ticks_max", .{});
    }

    assert(state_checker.convergence());
    assert(conductor.done());

    output.info("\n          PASSED ({} ticks)", .{tick});
}

pub const ExitCode = enum(u8) {
    ok = 0,
    crash = 127, // Any assertion crash will be given an exit code of 127 by default.
    liveness = 128,
    correctness = 129,
};

/// Print an error message and then exit with an exit code.
fn fatal(exit_code: ExitCode, comptime fmt_string: []const u8, args: anytype) noreturn {
    output.err(fmt_string, args);
    std.os.exit(@enumToInt(exit_code));
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

fn on_cluster_reply(_: *Cluster, client: usize, request: *Message, reply: *Message) void {
    conductor.reply(client, request, reply);
}

fn on_replica_change_state(replica: *const Replica) void {
    state_checker.check_state(replica.replica) catch |err| {
        fatal(.correctness, "state checker error: {}", .{err});
    };
}

fn on_replica_compact(replica: *const Replica) void {
    storage_checker.replica_compact(replica) catch |err| {
        fatal(.correctness, "storage checker error: {}", .{err});
    };
}

fn on_replica_checkpoint(replica: *const Replica) void {
    storage_checker.replica_checkpoint(replica) catch |err| {
        fatal(.correctness, "storage checker error: {}", .{err});
    };
}

/// Returns a random partitioning mode.
fn random_partition_mode(random: std.rand.Random) PartitionMode {
    const typeInfo = @typeInfo(PartitionMode).Enum;
    var enumAsInt = random.uintAtMost(typeInfo.tag_type, typeInfo.fields.len - 1);
    return @intToEnum(PartitionMode, enumAsInt);
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
