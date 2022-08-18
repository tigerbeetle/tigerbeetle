const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const mem = std.mem;

const config = @import("config.zig");

const Client = @import("test/cluster.zig").Client;
const Cluster = @import("test/cluster.zig").Cluster;
const Header = @import("vsr.zig").Header;
pub const Replica = @import("test/cluster.zig").Replica;
const StateChecker = @import("test/state_checker.zig").StateChecker;
const StateMachine = @import("test/cluster.zig").StateMachine;
const PartitionMode = @import("test/packet_simulator.zig").PartitionMode;

/// The `log` namespace in this root file is required to implement our custom `log` function.
const output = std.log.scoped(.state_checker);

/// Set this to `false` if you want to see how literally everything works.
/// This will run much slower but will trace all logic across the cluster.
const log_state_transitions_only = builtin.mode != .Debug;

const log_health = std.log.scoped(.health);

/// You can fine tune your log levels even further (debug/info/notice/warn/err/crit/alert/emerg):
pub const log_level: std.log.Level = if (log_state_transitions_only) .info else .debug;

var prng: std.rand.DefaultPrng = undefined;

const ticks_max = 100_000_000;

/// `on_change_replica` and `send_request` need access to the cluster
var cluster_ptr: *Cluster = undefined;

pub const Simulator = struct {
    cluster: *Cluster,

    random: std.rand.Random,

    replica_count: u8 = 0,
    client_count: u8 = 0,
    node_count: u8 = 0,

    request_probability: u8 = 0,
    idle_on_probability: u8 = 0,
    idle_off_probability: u8 = 0,

    replica_normal_min: u8 = 0,
    requests_sent: u64 = 0,
    idle: bool = false,
    running: bool = false,

    pub fn init(allocator: std.mem.Allocator, seed: u64) !Simulator {
        prng = std.rand.DefaultPrng.init(seed);
        const random = prng.random();

        var self = Simulator{
            .cluster = undefined,
            .random = random,
        };

        self.replica_count = 1 + self.random.uintLessThan(u8, config.replicas_max);
        self.client_count = 1 + self.random.uintLessThan(u8, config.clients_max);
        self.node_count = self.replica_count + self.client_count;

        self.request_probability = 1 + self.random.uintLessThan(u8, 99);
        self.idle_on_probability = self.random.uintLessThan(u8, 20);
        self.idle_off_probability = 10 + self.random.uintLessThan(u8, 10);

        self.cluster = try Cluster.create(allocator, self.random, .{
            .cluster = 0,
            .replica_count = self.replica_count,
            .client_count = self.client_count,
            .seed = self.random.int(u64),
            .network_options = .{
                .packet_simulator_options = .{
                    .replica_count = self.replica_count,
                    .client_count = self.client_count,
                    .node_count = self.node_count,

                    .seed = self.random.int(u64),
                    .one_way_delay_mean = 3 + self.random.uintLessThan(u16, 10),
                    .one_way_delay_min = self.random.uintLessThan(u16, 3),
                    .packet_loss_probability = self.random.uintLessThan(u8, 30),
                    .path_maximum_capacity = 2 + self.random.uintLessThan(u8, 19),
                    .path_clog_duration_mean = self.random.uintLessThan(u16, 500),
                    .path_clog_probability = self.random.uintLessThan(u8, 2),
                    .packet_replay_probability = self.random.uintLessThan(u8, 50),

                    .partition_mode = random_partition_mode(self.random),
                    .partition_probability = self.random.uintLessThan(u8, 3),
                    .unpartition_probability = 1 + self.random.uintLessThan(u8, 10),
                    .partition_stability = 100 + self.random.uintLessThan(u32, 100),
                    .unpartition_stability = self.random.uintLessThan(u32, 20),
                },
            },
            .storage_options = .{
                .seed = self.random.int(u64),
                .read_latency_min = self.random.uintLessThan(u16, 3),
                .read_latency_mean = 3 + self.random.uintLessThan(u16, 10),
                .write_latency_min = self.random.uintLessThan(u16, 3),
                .write_latency_mean = 3 + self.random.uintLessThan(u16, 100),
                .read_fault_probability = self.random.uintLessThan(u8, 10),
                .write_fault_probability = self.random.uintLessThan(u8, 10),
            },
            .health_options = .{
                .crash_probability = 0.0001,
                .crash_stability = self.random.uintLessThan(u32, 1_000),
                .restart_probability = 0.01,
                .restart_stability = self.random.uintLessThan(u32, 1_000),
            },
        });
        cluster_ptr = self.cluster;
        errdefer self.cluster.destroy();

        self.cluster.state_checker = try StateChecker.init(allocator, self.cluster);
        errdefer self.cluster.state_checker.deinit();

        for (self.cluster.replicas) |*replica| {
            replica.on_change_state = on_change_replica;
        }
        self.cluster.on_change_state = on_change_replica;

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
            \\
        , .{
            seed,
            self.replica_count,
            self.client_count,
            self.request_probability,
            self.idle_on_probability,
            self.idle_off_probability,
            self.cluster.options.network_options.packet_simulator_options.one_way_delay_mean,
            self.cluster.options.network_options.packet_simulator_options.one_way_delay_min,
            self.cluster.options.network_options.packet_simulator_options.packet_loss_probability,
            self.cluster.options.network_options.packet_simulator_options.path_maximum_capacity,
            self.cluster.options.network_options.packet_simulator_options.path_clog_duration_mean,
            self.cluster.options.network_options.packet_simulator_options.path_clog_probability,
            self.cluster.options.network_options.packet_simulator_options.packet_replay_probability,
            self.cluster.options.network_options.packet_simulator_options.partition_mode,
            self.cluster.options.network_options.packet_simulator_options.partition_probability,
            self.cluster.options.network_options.packet_simulator_options.unpartition_probability,
            self.cluster.options.network_options.packet_simulator_options.partition_stability,
            self.cluster.options.network_options.packet_simulator_options.unpartition_stability,
            self.cluster.options.storage_options.read_latency_min,
            self.cluster.options.storage_options.read_latency_mean,
            self.cluster.options.storage_options.write_latency_min,
            self.cluster.options.storage_options.write_latency_mean,
            self.cluster.options.storage_options.read_fault_probability,
            self.cluster.options.storage_options.write_fault_probability,
            self.cluster.options.health_options.crash_probability * 100,
            self.cluster.options.health_options.crash_stability,
            self.cluster.options.health_options.restart_probability * 100,
            self.cluster.options.health_options.restart_stability,
        });

        self.requests_sent = 0;
        self.idle = false;
        self.running = true;

        // The minimum number of healthy replicas required for a crashed replica to be able to recover.
        self.replica_normal_min = replicas: {
            if (self.replica_count == 1) {
                // A cluster of 1 can crash safely (as long as there is no disk corruption) since it
                // does not run the recovery protocol.
                break :replicas 0;
            } else {
                break :replicas self.cluster.replicas[0].quorum_view_change;
            }
        };

        // Disable most faults at startup, so that the replicas don't get stuck in recovery mode.
        for (self.cluster.storages) |*storage, i| {
            storage.faulty = self.replica_normal_min <= i;
        }

        return self;
    }

    pub fn deinit(self: Simulator) void {
        self.cluster.state_checker.deinit();
        self.cluster.destroy();
    }

    pub fn tick(self: *Simulator) !void {
        const health_options = &self.cluster.options.health_options;
        // The maximum number of replicas that can crash, with the cluster still able to recover.
        var crashes = self.cluster.replica_normal_count() -| self.replica_normal_min;

        for (self.cluster.storages) |*storage, replica| {
            if (self.cluster.replicas[replica].journal.recovered) {

                // TODO Remove this workaround when VSR recovery protocol is disabled.
                // When only the minimum number of replicas are healthy (no more crashes allowed),
                // disable storage faults on all healthy replicas.
                //
                // This is a workaround to avoid the deadlock that occurs when (for example) in a
                // cluster of 3 replicas, one is down, another has a corrupt prepare, and the last does
                // not have the prepare. The two healthy replicas can never complete a view change,
                // because two replicas are not enough to nack, and the unhealthy replica cannot
                // complete the VSR recovery protocol either.
                if (self.cluster.health[replica] == .up and crashes == 0) {
                    storage.faulty = false;
                } else {
                    // When a journal recovers for the first time, enable its storage faults.
                    // Future crashes will recover in the presence of faults.
                    storage.faulty = true;
                }
            }
            storage.tick();
        }

        for (self.cluster.replicas) |*replica| {
            switch (self.cluster.health[replica.replica]) {
                .up => |*ticks| {
                    ticks.* -|= 1;
                    replica.tick();
                    self.cluster.state_checker.check_state(replica.replica);

                    if (ticks.* != 0) continue;
                    if (crashes == 0) continue;
                    if (self.cluster.storages[replica.replica].writes.count() == 0) {
                        if (!chance_f64(self.random, health_options.crash_probability)) continue;
                    } else {
                        if (!chance_f64(self.random, health_options.crash_probability * 10.0)) continue;
                    }

                    if (!try self.cluster.crash_replica(replica.replica)) continue;
                    log_health.debug("crash replica={}", .{replica.replica});
                    crashes -= 1;
                },
                .down => |*ticks| {
                    ticks.* -|= 1;
                    // Keep ticking the time so that it won't have diverged too far to synchronize
                    // when the replica restarts.
                    replica.clock.time.tick();
                    assert(replica.status == .recovering);
                    if (ticks.* == 0 and chance_f64(self.random, health_options.restart_probability)) {
                        self.cluster.health[replica.replica] = .{ .up = health_options.restart_stability };
                        log_health.debug("restart replica={}", .{replica.replica});
                    }
                },
            }
        }

        self.cluster.network.packet_simulator.tick(self.cluster.health);

        for (self.cluster.clients) |*client| client.tick();

        // TODO When storage is supported, run more transitions than fit in the journal.
        const transitions_max = config.journal_slot_count / 2;
        if (self.cluster.state_checker.transitions == transitions_max) {
            if (self.cluster.state_checker.convergence() and
                self.cluster.replica_up_count() == self.replica_count)
            {
                self.running = false;
                return;
            }
            return;
        } else {
            assert(self.cluster.state_checker.transitions < transitions_max);
        }

        if (self.requests_sent < transitions_max) {
            if (self.idle) {
                if (chance(self.random, self.idle_off_probability)) self.idle = false;
            } else {
                if (chance(self.random, self.request_probability)) {
                    if (send_request(self.random)) self.requests_sent += 1;
                }
                if (chance(self.random, self.idle_on_probability)) self.idle = true;
            }
        }
    }
};

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

    var simulator = try Simulator.init(allocator, seed);
    defer simulator.deinit();

    var ticks: u64 = 0;
    while (ticks < ticks_max and simulator.running) : (ticks += 1) {
        try simulator.tick();
    }

    // TODO When storage is supported, run more transitions than fit in the journal.
    const transitions_max = config.journal_slot_count / 2;
    if (simulator.cluster.state_checker.transitions < transitions_max) {
        output.err("you can reproduce this failure with seed={}", .{seed});
        @panic("unable to complete transitions_max before ticks_max");
    }

    assert(simulator.cluster.state_checker.convergence());

    output.info("\n          PASSED ({} ticks)", .{ticks});
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
    const cluster = cluster_ptr;
    assert(cluster.state_machines[replica.replica].state == replica.state_machine.state);
    cluster.state_checker.check_state(replica.replica);
}

fn send_request(random: std.rand.Random) bool {
    const cluster = cluster_ptr;
    const client_index = random.uintLessThan(u8, cluster.options.client_count);

    const client = &cluster.clients[client_index];
    const checker_request_queue = &cluster.state_checker.client_requests[client_index];

    // Ensure that we don't shortchange testing of the full client request queue length:
    assert(client.request_queue.buffer.len <= checker_request_queue.buffer.len);
    if (client.request_queue.full()) return false;
    if (checker_request_queue.full()) return false;

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

    // While hashing the client ID with the request body prevents input collisions across clients,
    // it's still possible for the same client to generate the same body, and therefore input hash.
    const client_input = StateMachine.hash(client.id, body);
    checker_request_queue.push_assume_capacity(client_input);
    std.log.scoped(.test_client).debug("client {} sending input={x}", .{
        client_index,
        client_input,
    });

    client.request(0, client_callback, .hash, message, body_size);

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

fn parse_seed(bytes: []const u8) u64 {
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
