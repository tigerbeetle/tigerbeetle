const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const config = @import("config.zig");

const Client = @import("test/cluster.zig").Client;
const Cluster = @import("test/cluster.zig").Cluster;
const Header = @import("vsr.zig").Header;
const Replica = @import("test/cluster.zig").Replica;
const StateChecker = @import("test/state_checker.zig").StateChecker;
const StateMachine = @import("test/cluster.zig").StateMachine;
const PartitionMode = @import("test/packet_simulator.zig").PartitionMode;

/// The `log` namespace in this root file is required to implement our custom `log` function.
const output = std.log.scoped(.state_checker);

/// Set this to `false` if you want to see how literally everything works.
/// This will run much slower but will trace all logic across the cluster.
const log_state_transitions_only = std.builtin.mode != .Debug;

/// You can fine tune your log levels even further (debug/info/notice/warn/err/crit/alert/emerg):
pub const log_level: std.log.Level = if (log_state_transitions_only) .info else .debug;

var cluster: *Cluster = undefined;

pub fn main() !void {
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

    if (std.builtin.mode == .ReleaseFast or std.builtin.mode == .ReleaseSmall) {
        // We do not support ReleaseFast or ReleaseSmall because they disable assertions.
        @panic("the simulator must be run with -OReleaseSafe");
    }

    if (seed == seed_random) {
        if (std.builtin.mode != .ReleaseSafe) {
            // If no seed is provided, than Debug is too slow and ReleaseSafe is much faster.
            @panic("no seed provided: the simulator must be run with -OReleaseSafe");
        }
        if (log_level == .debug) {
            output.warn("no seed provided: full debug logs are enabled, this will be slow", .{});
        }
    }

    var prng = std.rand.DefaultPrng.init(seed);
    const random = &prng.random;

    const replica_count = 1 + prng.random.uintLessThan(u8, config.replicas_max);
    const client_count = 1 + prng.random.uintLessThan(u8, config.clients_max);
    const node_count = replica_count + client_count;

    const ticks_max = 100_000_000;
    const transitions_max = config.journal_size_max / config.message_size_max;
    const request_probability = 1 + prng.random.uintLessThan(u8, 99);
    const idle_on_probability = prng.random.uintLessThan(u8, 20);
    const idle_off_probability = 10 + prng.random.uintLessThan(u8, 10);

    cluster = try Cluster.create(allocator, &prng.random, .{
        .cluster = 0,
        .replica_count = replica_count,
        .client_count = client_count,
        .seed = prng.random.int(u64),
        .network_options = .{
            .packet_simulator_options = .{
                .replica_count = replica_count,
                .client_count = client_count,
                .node_count = node_count,

                .seed = prng.random.int(u64),
                .one_way_delay_mean = 3 + prng.random.uintLessThan(u16, 10),
                .one_way_delay_min = prng.random.uintLessThan(u16, 3),
                .packet_loss_probability = prng.random.uintLessThan(u8, 30),

                .partition_mode = random_partition_mode(random),
                .partition_probability = prng.random.uintLessThan(u8, 3),
                .unpartition_probability = 1 + prng.random.uintLessThan(u8, 10),
                .partition_stability = 100 + prng.random.uintLessThan(u32, 100),
                .unpartition_stability = prng.random.uintLessThan(u32, 20),

                .path_maximum_capacity = 20 + prng.random.uintLessThan(u8, 20),
                .path_clog_duration_mean = prng.random.uintLessThan(u16, 500),
                .path_clog_probability = prng.random.uintLessThan(u8, 2),
                .packet_replay_probability = prng.random.uintLessThan(u8, 50),
            },
        },
        .storage_options = .{
            .seed = prng.random.int(u64),
            .read_latency_min = prng.random.uintLessThan(u16, 3),
            .read_latency_mean = 3 + prng.random.uintLessThan(u16, 10),
            .write_latency_min = prng.random.uintLessThan(u16, 3),
            .write_latency_mean = 3 + prng.random.uintLessThan(u16, 10),
            .read_fault_probability = prng.random.uintLessThan(u8, 10),
            .write_fault_probability = prng.random.uintLessThan(u8, 10),
        },
    });
    defer cluster.destroy();

    cluster.state_checker = try StateChecker.init(allocator, cluster);
    defer cluster.state_checker.deinit();

    for (cluster.replicas) |*replica| {
        replica.on_change_state = on_change_replica;
    }
    cluster.on_change_state = on_change_replica;

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
        \\          read_latency_min={}
        \\          read_latency_mean={}
        \\          write_latency_min={}
        \\          write_latency_mean={}
        \\          read_fault_probability={}%
        \\          write_fault_probability={}%
        \\
    , .{
        seed,
        replica_count,
        client_count,
        request_probability,
        idle_on_probability,
        idle_off_probability,
        cluster.options.network_options.packet_simulator_options.one_way_delay_mean,
        cluster.options.network_options.packet_simulator_options.one_way_delay_min,
        cluster.options.network_options.packet_simulator_options.packet_loss_probability,
        cluster.options.network_options.packet_simulator_options.path_maximum_capacity,
        cluster.options.network_options.packet_simulator_options.path_clog_duration_mean,
        cluster.options.network_options.packet_simulator_options.path_clog_probability,
        cluster.options.network_options.packet_simulator_options.packet_replay_probability,
        cluster.options.storage_options.read_latency_min,
        cluster.options.storage_options.read_latency_mean,
        cluster.options.storage_options.write_latency_min,
        cluster.options.storage_options.write_latency_mean,
        cluster.options.storage_options.read_fault_probability,
        cluster.options.storage_options.write_fault_probability,
    });

    var requests_sent: u64 = 0;
    var idle = false;

    var tick: u64 = 0;
    while (tick < ticks_max) : (tick += 1) {
        for (cluster.storages) |*storage| storage.tick();

        for (cluster.replicas) |*replica, i| {
            replica.tick();
            cluster.state_checker.check_state(@intCast(u8, i));
        }

        cluster.network.packet_simulator.tick();

        for (cluster.clients) |*client| client.tick();

        if (cluster.state_checker.transitions == transitions_max) {
            if (cluster.state_checker.convergence()) break;
            continue;
        } else {
            assert(cluster.state_checker.transitions < transitions_max);
        }

        if (requests_sent < transitions_max) {
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

    if (cluster.state_checker.transitions < transitions_max) {
        output.emerg("you can reproduce this failure with seed={}", .{seed});
        @panic("unable to complete transitions_max before ticks_max");
    }

    assert(cluster.state_checker.convergence());

    output.info("\n          PASSED", .{});
}

/// Returns true, `p` percent of the time, else false.
fn chance(random: *std.rand.Random, p: u8) bool {
    assert(p <= 100);
    return random.uintLessThan(u8, 100) < p;
}

/// Returns the next argument for the simulator or null (if none available)
fn args_next(args: *std.process.ArgIterator, allocator: *std.mem.Allocator) ?[:0]const u8 {
    const err_or_bytes = args.next(allocator) orelse return null;
    return err_or_bytes catch @panic("Unable to extract next value from args");
}

fn on_change_replica(replica: *Replica) void {
    assert(cluster.state_machines[replica.replica].state == replica.state_machine.state);
    cluster.state_checker.check_state(replica.replica);
}

fn send_request(random: *std.rand.Random) bool {
    const client_index = random.uintLessThan(u8, cluster.options.client_count);

    const client = &cluster.clients[client_index];
    const checker_request_queue = &cluster.state_checker.client_requests[client_index];

    // Ensure that we don't shortchange testing of the full client request queue length:
    assert(client.request_queue.buffer.len <= checker_request_queue.buffer.len);
    if (client.request_queue.full()) return false;
    if (checker_request_queue.full()) return false;

    const message = client.get_message() orelse return false;
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
    checker_request_queue.push(client_input) catch unreachable;
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
    assert(user_data == 0);
}

/// Returns a random partitioning mode, excluding .custom
fn random_partition_mode(random: *std.rand.Random) PartitionMode {
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

pub fn log(
    comptime level: std.log.Level,
    comptime scope: @TypeOf(.EnumLiteral),
    comptime format: []const u8,
    args: anytype,
) void {
    if (log_state_transitions_only and scope != .state_checker) return;

    const prefix_default = "[" ++ @tagName(level) ++ "] " ++ "(" ++ @tagName(scope) ++ "): ";
    const prefix = if (log_state_transitions_only) "" else prefix_default;

    // Print the message to stdout, silently ignoring any errors
    const held = std.debug.getStderrMutex().acquire();
    defer held.release();
    const stderr = std.io.getStdErr().writer();
    nosuspend stderr.print(prefix ++ format ++ "\n", args) catch return;
}
