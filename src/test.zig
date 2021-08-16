const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const config = @import("config.zig");

const Cluster = @import("test/cluster.zig").Cluster;
const StateChecker = @import("test/state_checker.zig").StateChecker;

const StateMachine = @import("test/state_machine.zig").StateMachine;
const MessageBus = @import("test/message_bus.zig").MessageBus;

const vr = @import("vr.zig");
const Header = vr.Header;
const Client = vr.Client(StateMachine, MessageBus);

const log = std.log.scoped(.fuzz);

test "VR" {
    std.testing.log_level = .notice;

    // TODO: use std.testing.allocator when all leaks are fixed.
    const allocator = std.heap.page_allocator;
    var prng = std.rand.DefaultPrng.init(0xABEE11E);
    const random = &prng.random;

    const cluster = try Cluster.create(allocator, &prng.random, .{
        .cluster = 42,
        .replica_count = 3,
        .client_count = 1,
        .seed = prng.random.int(u64),
        .network_options = .{
            .after_on_message = StateChecker.after_on_message,
            .packet_simulator_options = .{
                .node_count = 4,
                .prng_seed = prng.random.int(u64),
                .one_way_delay_mean = 25,
                .one_way_delay_min = 10,
                .packet_loss_probability = 10,
                .path_maximum_capacity = 20,
                .path_clog_duration_mean = 200,
                .path_clog_probability = 2,
                .packet_replay_probability = 2,
            },
        },
    });
    defer cluster.destroy();

    cluster.state_checker = try StateChecker.init(allocator, cluster);
    defer cluster.state_checker.deinit();

    var tick: u64 = 0;
    while (tick < 1_000_000) : (tick += 1) {
        for (cluster.replicas) |*replica, i| {
            replica.tick();
            cluster.state_checker.check_state(@intCast(u8, i));
        }

        cluster.network.packet_simulator.tick();

        for (cluster.clients) |*client| client.tick();

        if (chance(random, 5)) maybe_send_random_request(cluster, random);
    }
}

fn chance(random: *std.rand.Random, p: u8) bool {
    assert(p < 100);
    return random.uintLessThan(u8, 100) < p;
}

fn maybe_send_random_request(cluster: *Cluster, random: *std.rand.Random) void {
    const client_index = random.uintLessThan(u8, cluster.options.client_count);

    const client = &cluster.clients[client_index];
    const checker_request_queue = &cluster.state_checker.client_requests[client_index];

    // Ensure that we don't shortchange testing of the full client request queue length:
    assert(client.request_queue.buffer.len <= checker_request_queue.buffer.len);
    if (client.request_queue.full()) return;
    if (checker_request_queue.full()) return;

    const message = client.get_message() orelse {
        log.notice("no message available to send request, dropping", .{});
        return;
    };
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
}

fn client_callback(
    user_data: u128,
    operation: StateMachine.Operation,
    results: Client.Error![]const u8,
) void {
    assert(user_data == 0);
}
