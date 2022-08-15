const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const mem = std.mem;

const config = @import("config.zig");
const vsr = @import("vsr.zig");
const Header = vsr.Header;

const Client = @import("test/cluster.zig").Client;
const Cluster = @import("test/cluster.zig").Cluster;
const ClusterOptions = @import("test/cluster.zig").ClusterOptions;
const Replica = @import("test/cluster.zig").Replica;
const StateChecker = @import("test/state_checker.zig").StateChecker;
const StateMachine = @import("test/cluster.zig").StateMachine;
const PartitionMode = @import("test/packet_simulator.zig").PartitionMode;

/// The `log` namespace in this root file is required to implement our custom `log` function.
const output = std.log.scoped(.state_checker);

/// Set this to `false` if you want to see how literally everything works.
/// This will run much slower but will trace all logic across the cluster.
const log_state_transitions_only = builtin.mode != .Debug;

const log_health = std.log.scoped(.health);
const log_faults = std.log.scoped(.faults);

/// You can fine tune your log levels even further (debug/info/notice/warn/err/crit/alert/emerg):
pub const log_level: std.log.Level = if (log_state_transitions_only) .info else .debug;

/// Modifies compile-time constants on "config.zig".
pub const deployment_environment = .simulation;

var cluster: *Cluster = undefined;

pub fn main() !void {

    comptime {
        assert(config.deployment_environment == .simulation);
    }

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

    const replica_count = 1 + random.uintLessThan(u8, config.replicas_max);
    const client_count = 1 + random.uintLessThan(u8, config.clients_max);
    const node_count = replica_count + client_count;

    const ticks_max = 50_000_000;
    const request_probability = 1 + random.uintLessThan(u8, 99);
    const idle_on_probability = random.uintLessThan(u8, 20);
    const idle_off_probability = 10 + random.uintLessThan(u8, 10);

    const cluster_options: ClusterOptions = .{
        .cluster = 0,
        .replica_count = replica_count,
        .client_count = client_count,
        .seed = random.int(u64),
        .on_change_state = on_change_replica,
        .network_options = .{
            .packet_simulator_options = .{
                .replica_count = replica_count,
                .client_count = client_count,
                .node_count = node_count,

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
        },
        .health_options = .{
            .crash_probability = 0.0001,
            .crash_stability = random.uintLessThan(u32, 1_000),
            .restart_probability = 0.01,
            .restart_stability = random.uintLessThan(u32, 1_000),
        },
        .state_machine_options = .{
            .seed = random.int(u64),
            .prefetch_mean = 5 + random.uintLessThan(u64, 10),
            .compact_mean = 5 + random.uintLessThan(u64, 10),
            .checkpoint_mean = 5 + random.uintLessThan(u64, 10),
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
        \\          prefetch_mean={} ticks
        \\          compact_mean={} ticks
        \\          checkpoint_mean={} ticks
        \\
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
        cluster_options.state_machine_options.prefetch_mean,
        cluster_options.state_machine_options.compact_mean,
        cluster_options.state_machine_options.checkpoint_mean,
    });

    cluster = try Cluster.create(allocator, random, cluster_options);
    defer cluster.destroy();

    var requests_sent: u64 = 0;
    var idle = false;

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

    // Disable most faults at startup, so that the replicas don't get stuck in recovery mode.
    for (cluster.storages) |*storage, i| {
        storage.faulty = replica_normal_min <= i;
    }

    // The maximum number of transitions from calling `client.request()`, not including
    // `register` messages.
    // TODO When storage is supported, run more transitions than fit in the journal.
    const committed_requests_max = config.journal_slot_count / 2;
    var tick: u64 = 0;
    while (tick < ticks_max) : (tick += 1) {
        const health_options = &cluster.options.health_options;
        // The maximum number of replicas that can crash, with the cluster still able to recover.
        var crashes = cluster.replica_normal_count() -| replica_normal_min;

        for (cluster.storages) |*storage, replica| {
            if (cluster.replicas[replica].journal.recovered) {
                // TODO Remove this workaround when VSR recovery protocol is disabled.
                // When only the minimum number of replicas are healthy (no more crashes allowed),
                // disable storage faults on all healthy replicas.
                //
                // This is a workaround to avoid the deadlock that occurs when (for example) in a
                // cluster of 3 replicas, one is down, another has a corrupt prepare, and the last does
                // not have the prepare. The two healthy replicas can never complete a view change,
                // because two replicas are not enough to nack, and the unhealthy replica cannot
                // complete the VSR recovery protocol either.
                if (cluster.health[replica] == .up and crashes == 0) {
                    if (storage.faulty) {
                        log_faults.debug("{}: disable storage faults", .{replica});
                        storage.faulty = false;
                    }
                } else {
                    // When a journal recovers for the first time, enable its storage faults.
                    // Future crashes will recover in the presence of faults.
                    if (!storage.faulty) {
                        log_faults.debug("{}: enable storage faults", .{replica});
                        storage.faulty = true;
                    }
                }
            }
            storage.tick();
        }

        for (cluster.replicas) |*replica| {
            switch (cluster.health[replica.replica]) {
                .up => |*ticks| {
                    ticks.* -|= 1;
                    replica.tick();
                    cluster.state_checker.check_state(replica.replica) catch |err| {
                        fatal(.correctness, "state checker error: {}", .{err});
                    };

                    if (ticks.* != 0) continue;
                    if (crashes == 0) continue;
                    if (cluster.storages[replica.replica].writes.count() == 0) {
                        if (!chance_f64(random, health_options.crash_probability)) continue;
                    } else {
                        if (!chance_f64(random, health_options.crash_probability * 10.0)) continue;
                    }

                    if (!try cluster.crash_replica(replica.replica)) continue;
                    log_health.debug("{}: crash replica", .{replica.replica});
                    crashes -= 1;
                },
                .down => |*ticks| {
                    ticks.* -|= 1;
                    // Keep ticking the time so that it won't have diverged too far to synchronize
                    // when the replica restarts.
                    replica.clock.time.tick();
                    assert(replica.status == .recovering);
                    if (ticks.* == 0 and chance_f64(random, health_options.restart_probability)) {
                        cluster.health[replica.replica] = .{ .up = health_options.restart_stability };
                        log_health.debug("{}: restart replica", .{replica.replica});
                    }
                },
            }
        }

        cluster.network.packet_simulator.tick(cluster.health);

        for (cluster.clients) |*client| client.tick();

        if (cluster.state_checker.committed_requests == committed_requests_max) {
            if (cluster.state_checker.convergence() and
                cluster.replica_up_count() == replica_count)
            {
                break;
            }
            continue;
        } else {
            assert(cluster.state_checker.committed_requests < committed_requests_max);
        }

        if (requests_sent < committed_requests_max) {
            if (idle) {
                if (chance(random, idle_off_probability)) idle = false;
            } else {
                if (chance(random, request_probability)) {
                    if (send_request(random)) requests_sent += 1;
                }
                if (chance(random, idle_on_probability)) idle = true;
            }
        }
    }

    if (cluster.state_checker.committed_requests < committed_requests_max) {
        output.err("you can reproduce this failure with seed={}", .{seed});
        fatal(.liveness, "unable to complete committed_requests_max before ticks_max", .{});
    }

    assert(cluster.state_checker.convergence());

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
fn chance(random: std.rand.Random, p: u8) bool {
    assert(p <= 100);
    return random.uintLessThan(u8, 100) < p;
}

/// Returns true, `p` percent of the time, else false.
fn chance_f64(random: std.rand.Random, p: f64) bool {
    assert(p <= 100.0);
    return random.float(f64) < p;
}

/// Returns the next argument for the simulator or null (if none available)
fn args_next(args: *std.process.ArgIterator, allocator: std.mem.Allocator) ?[:0]const u8 {
    const err_or_bytes = args.next(allocator) orelse return null;
    return err_or_bytes catch @panic("Unable to extract next value from args");
}

fn on_change_replica(replica: *Replica) void {
    cluster.state_checker.check_state(replica.replica) catch |err| {
        fatal(.correctness, "state checker error: {}", .{err});
    };
}

fn send_request(random: std.rand.Random) bool {
    const client_index = random.uintLessThan(u8, cluster.options.client_count);

    const client = &cluster.clients[client_index];
    if (client.request_queue.full()) return false;

    const message = client.get_message();
    defer client.unref(message);

    const body_size_max = config.message_size_max - @sizeOf(Header);
    const body_size: u32 = switch (random.uintLessThan(u8, 100)) {
        0...10 => 0,
        11...89 => random.uintLessThan(u32, body_size_max),
        90...99 => body_size_max,
        else => unreachable,
    };

    const body = message.buffer[@sizeOf(Header)..][0..body_size];
    if (chance(random, 10)) {
        std.mem.set(u8, body, 0);
    } else {
        random.bytes(body);
    }

    std.log.scoped(.test_client).debug("{}: send_request: sending bytes={x}", .{
        client_index,
        body_size,
    });
    client.request(0, client_callback, .random, message, body_size);

    return true;
}

fn client_callback(
    user_data: u128,
    operation: StateMachine.Operation,
    results: Client.Error![]const u8,
) void {
    _ = operation;
    _ = results catch unreachable;

    assert(user_data == 0);
}

/// Returns a random partitioning mode, excluding .custom
fn random_partition_mode(random: std.rand.Random) PartitionMode {
    const typeInfo = @typeInfo(PartitionMode).Enum;
    var enumAsInt = random.uintAtMost(typeInfo.tag_type, typeInfo.fields.len - 2);
    if (enumAsInt >= @enumToInt(PartitionMode.custom)) enumAsInt += 1;
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
