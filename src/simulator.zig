const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const config = @import("config.zig");

const Client = @import("test/cluster.zig").Client;
const Cluster = @import("test/cluster.zig").Cluster;
const Header = @import("vr.zig").Header;
const Replica = @import("test/cluster.zig").Replica;
const StateChecker = @import("test/state_checker.zig").StateChecker;
const StateMachine = @import("test/cluster.zig").StateMachine;

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
    const ga = std.testing.allocator;

    var args = std.process.args();

    // Skip argv[0] which is the name of this executable:
    _ = args_next(&args, ga);

    const seed_random = std.crypto.random.int(u64);
    const seed = if (args_next(&args, ga)) |seed_from_arg| parse_seed(seed_from_arg, ga) else seed_random;

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

    const ticks_max = 10_000_000;
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
                .node_count = node_count,
                .seed = prng.random.int(u64),
                .one_way_delay_mean = 3 + prng.random.uintLessThan(u16, 10),
                .one_way_delay_min = prng.random.uintLessThan(u16, 3),
                .packet_loss_probability = prng.random.uintLessThan(u8, 50),
                .path_maximum_capacity = 1 + prng.random.uintLessThan(u8, 20),
                .path_clog_duration_mean = prng.random.uintLessThan(u16, 1000),
                .path_clog_probability = prng.random.uintLessThan(u8, 5),
                .packet_replay_probability = prng.random.uintLessThan(u8, 50),
            },
        },
    });
    defer cluster.destroy();

    cluster.state_checker = try StateChecker.init(allocator, cluster);
    defer cluster.state_checker.deinit();

    for (cluster.replicas) |*replica| {
        replica.on_change_state = on_change_replica;
    }

    output.info("\n" ++
        "          SEED={}\n\n" ++
        "          replicas={}\n" ++
        "          clients={}\n" ++
        "          request_probability={}%\n" ++
        "          idle_on_probability={}%\n" ++
        "          idle_off_probability={}%\n" ++
        "          one_way_delay_mean={} ticks\n" ++
        "          one_way_delay_min={} ticks\n" ++
        "          packet_loss_probability={}%\n" ++
        "          path_maximum_capacity={} messages\n" ++
        "          path_clog_duration_mean={} ticks\n" ++
        "          path_clog_probability={}%\n" ++
        "          packet_replay_probability={}%\n", .{
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
    });

    var requests_sent: u64 = 0;
    var idle = false;

    var tick: u64 = 0;
    while (tick < ticks_max) : (tick += 1) {
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
    return random.uintAtMost(u8, 100) <= p;
}

//returns the next argument for the simulator or null (if none avail)
fn args_next(args: *std.process.ArgIterator, ga: *std.mem.Allocator) ?[:0]const u8 {
    return if (args.next(ga)) |err_or_bytes|
        err_or_bytes catch @panic("Unable to extract next value from args")
    else
        null;
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
    checker_request_queue.push(StateMachine.hash(client.id, body)) catch unreachable;

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

fn parse_seed(bytes: []const u8, ga: *std.mem.Allocator) u64 {
    defer ga.free(bytes);
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
