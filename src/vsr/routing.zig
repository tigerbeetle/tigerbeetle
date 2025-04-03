//! TigerBeetle replication routing protocol.
//!
//! Eight fallacies of distributed computing:
//!
//! 1. The network is reliable;
//! 2. Latency is zero;
//! 3. Bandwidth is infinite;
//! 4. The network is secure;
//! 5. Topology doesn't change;
//! 6. There is one administrator;
//! 7. Transport cost is zero;
//! 8. The network is homogeneous;
//!
//! Robust tail principle:
//! - The same code should handle slow nodes and crashed nodes.
//! - A single crashed node should not cause retries and risk metastability.
//!
//! Algorithm:
//!
//! The replication route is V-shaped. Primary is in the middle, and it tosses each prepare at two
//! of its neighbors. Backups forward prepares to at most one further backup. Neighbors of the
//! primary have at least one more neighbor (in a six-replica cluster). If any single node fails,
//! the primary still gets a replication quorum. If the primary and backups disagree about the
//! replication route, the primary still gets a replication quorum.
//!
//! Because topology changes, routes are dynamic. The primary broadcasts the current route in the
//! ping message. It's enough if the routing information is only eventually consistent.
//!
//! To select the best route, primary uses outcome-focused explore-exploit approach. Every once in a
//! while the primary tries an alternative route. The primary captures replication latency for a
//! route (that is, the arrival time of prepare_ok messages). If the latency for an alternative
//! route is sufficiently better than current latency, the route is switched. Note that latency
//! includes both network and disk latency.
//!
//! The experiment schedule is defined randomly. All replicas share the same RNG seed, so no
//! coordination is needed to launch an experiment!
//!
//! To remove outliers, the experiment take two ops. op and op|1 are either both experimental, or
//! both non-experimental. Experiment's cost is the average of two costs. The cost for active route
//! is exponential moving average.
const std = @import("std");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const constants = @import("../constants.zig");
const stdx = @import("../stdx.zig");
const Ratio = stdx.PRNG.Ratio;
const ratio = stdx.PRNG.ratio;

const history_max = constants.pipeline_prepare_queue_max * 2;

// This constant serves two purposes:
// - First, as a cap for all latencies, to make sure that u64 computations can't overflow.
// - Second, to simplify cost function computations, replica we haven't heard from has this latency.
const latency_max_ns = 10 * std.time.ns_per_hour;

replica: u8,
replica_count: u8,

view: u32,
active: Route,
active_cost_ema: ?Cost,

experiment_chance: Ratio = ratio(1, 20),
best_alternative: ?struct {
    route: Route,
    cost_avg: Cost,
},

history: [history_max]?OpHistory,

pub const Route = stdx.BoundedArrayType(u8, constants.replicas_max);
const OpHistory = struct {
    op: u64,
    prepare: u64,
    prepare_ok: [constants.replicas_max]u64 = .{latency_max_ns} ** constants.replicas_max,
    present: stdx.BitSetType(constants.replicas_max) = .{},

    fn record(
        op_history: *OpHistory,
        replica: u8,
        now_ns: u64,
    ) void {
        assert(now_ns >= op_history.prepare);
        if (op_history.present.is_set(replica)) {
            assert(op_history.prepare_ok[replica] <= now_ns);
            return;
        }
        assert(!op_history.present.is_set(replica));
        assert(op_history.prepare_ok[replica] == latency_max_ns);

        const latency_ns = @min(now_ns - op_history.prepare, latency_max_ns);
        op_history.prepare_ok[replica] = latency_ns;
        op_history.present.set(replica);
    }
};

const Cost = struct {
    // Left-biased median latency matches replication quorum latency, and directly contributes to
    // user-visible latency.
    median: u64,
    // Maximum latency corresponds to the time when a prepare is fully replicated, and is important
    // for the overall health of the cluster.
    maximum: u64,
    // Worst-case latency tracks the length of the critical path, but we want non-critical paths to
    // be as short as possible.
    sum: u64,

    fn less(lhs: Cost, rhs: Cost) bool {
        // For sum, smaller than 5% improvement is considered insignificant.
        // For median and maximum the threshold is 10%.
        //
        // Why 5%: If replicas are on a ring in terms of distance, the sum for optimal path is
        //
        //   1 → 2
        //  ↗
        // ♔       3
        //  ↘     ↗
        //   5 → 4
        //
        // sum = (1 + 1) + (1 + 1 + 1)   (prepares)
        //       + (1 + 2) + (1 + 2 + 3) (prepare ok)
        //     = 14
        //
        // The sum for next best path is
        //
        //   1 → 2
        //  ↗
        // ♔       3
        //  ↘  ↗  ↙
        //   5   4
        //
        // sum = (1 + 1) + (1 + 2 + 1)   (prepares, two replicas transposed)
        //       + (1 + 2) + (1 + 2 + 3) (prepare ok)
        //     = 15
        //
        // The difference between 14 and 15 is less than 10% but more than 5%.
        //
        // Why 10%: just feels like a reasonable number! median varies more than sum, so we need
        // larger tolerance there.
        inline for (.{
            .{ "median", ratio(1, 10) },
            .{ "maximum", ratio(1, 10) },
            .{ "sum", ratio(1, 20) },
        }) |field_threshold| {
            const field, const threshold = field_threshold;
            if (less_signifiantly(@field(lhs, field), @field(rhs, field), threshold)) return true;
            if (@field(lhs, field) > @field(rhs, field)) return false;
        }
        return false;
    }

    // Returns true if lhs + lhs⋅threshold < rhs.
    fn less_signifiantly(lhs: u64, rhs: u64, thershold: Ratio) bool {
        assert(thershold.numerator < thershold.denominator);
        return lhs * (thershold.numerator + thershold.denominator) < rhs * thershold.denominator;
    }

    fn avg(lhs: Cost, rhs: Cost) Cost {
        return .{
            .median = @divFloor(lhs.median + rhs.median, 2),
            .maximum = @divFloor(lhs.maximum + rhs.maximum, 2),
            .sum = @divFloor(lhs.sum + rhs.sum, 2),
        };
    }

    fn ema_add(lhs: ?Cost, rhs: Cost) Cost {
        if (lhs == null) return rhs;
        return .{
            .median = ema_add_u64(lhs.?.median, rhs.median),
            .maximum = ema_add_u64(lhs.?.maximum, rhs.maximum),
            .sum = ema_add_u64(lhs.?.sum, rhs.sum),
        };
    }

    fn ema_add_u64(lhs: u64, rhs: u64) u64 {
        return @divFloor((lhs * 4) + rhs, 5);
    }
};

const Routing = @This();

pub fn init(options: struct {
    replica: u8,
    replica_count: u8,
}) Routing {
    assert(options.replica < options.replica_count);
    assert(options.replica_count <= constants.replicas_max);

    return .{
        .replica = options.replica,
        .replica_count = options.replica_count,

        .view = 0,
        .active = view_route_default(0, options.replica_count),
        .active_cost_ema = null,

        .best_alternative = null,

        .history = .{null} ** history_max,
    };
}

pub fn view_change(routing: *Routing, view: u32) void {
    assert(view > routing.view or (view == 0 and routing.view == 0));
    assert(routing.history_empty());
    routing.view = view;
    routing.active = view_route_default(view, routing.replica_count);
}

fn view_route_default(view: u32, replica_count: u8) Route {
    var route: Route = .{};
    for (0..replica_count) |replica| {
        route.append_assume_capacity(@intCast(replica));
    }

    // Rotate primary to the midpoint;
    const primary: u8 = @intCast(view % replica_count);
    const midpoint = @divFloor(replica_count, 2);
    const rotation = (replica_count + primary - midpoint) % replica_count;
    std.mem.rotate(u8, route.slice(), rotation);
    assert(route.slice()[midpoint] == primary);

    return route;
}

const RouteNext = stdx.BoundedArrayType(u8, 2);
pub fn route_next(routing: *const Routing, op: u64) RouteNext {
    const route = routing.op_route(op);
    const primary: u8 = @intCast(routing.view % routing.replica_count);
    const index = std.mem.indexOfScalar(u8, route.const_slice(), routing.replica).?;
    const index_primary = std.mem.indexOfScalar(u8, route.const_slice(), primary).?;

    var result: RouteNext = .{};
    if (index > 0 and index <= index_primary) {
        result.append_assume_capacity(route.const_slice()[index - 1]);
    }
    if (index < routing.replica_count - 1 and index >= index_primary) {
        result.append_assume_capacity(route.const_slice()[index + 1]);
    }
    return result;
}

pub fn route_activate(routing: *Routing, route: Route) void {
    assert(routing.history_empty());
    routing.active = route;
}

pub fn route_improvement(routing: *const Routing) ?Route {
    const alternative = routing.best_alternative orelse return null;
    const active_cost = routing.active_cost_ema orelse return null;
    return if (Cost.less(alternative.cost_avg, active_cost)) alternative.route else null;
}

pub fn op_prepare(routing: *Routing, op: u64, now_ns: u64) void {
    if (routing.slot(op).*) |previous| {
        assert(previous.op < op);
        op_finalize(routing, previous, .evicted);
    }

    routing.slot(op).* = .{
        .op = op,
        .prepare = now_ns,
    };
}

pub fn op_prepare_ok(routing: *Routing, op: u64, replica: u8, realtime_ns: u64) void {
    maybe(replica == routing.replica);
    const latencies: *OpHistory = if (routing.slot(op).*) |*l| l else {
        // The op was prepared by a different primary in an older view.
        return;
    };
    if (latencies.*.op > op) return;

    latencies.record(replica, realtime_ns);
    if (latencies.present.count() == routing.replica_count) {
        routing.op_finalize(latencies.*, .replicated_fully);
    }
}

fn op_finalize(
    routing: *Routing,
    latencies: OpHistory,
    reason: enum { evicted, replicated_fully },
) void {
    if (routing.op_route_alternative(latencies.op)) |alternative| {
        const op_pair = latencies.op ^ 1;
        assert(std.meta.eql(routing.op_route_alternative(op_pair), alternative));

        const latencies_pair = routing.slot(op_pair).* orelse return;
        if (latencies_pair.op != op_pair) return;

        const latencies_a = latencies;
        const latencies_b = latencies_pair;

        const cost_a = routing.history_cost(&latencies_a);
        const cost_b = routing.history_cost(&latencies_b);

        const complete_a = latencies_a.present.count() == routing.replica_count;
        const complete_b = latencies_b.present.count() == routing.replica_count;

        if (reason == .evicted and complete_a and complete_b) {
            // Already accounted for on .replicated_fully.
            return;
        }

        const cost_avg = Cost.avg(cost_a, cost_b);
        if (reason == .evicted or (complete_a and complete_b)) {
            if (routing.best_alternative == null or
                Cost.less(cost_avg, routing.best_alternative.?.cost_avg))
            {
                routing.best_alternative = .{
                    .route = alternative,
                    .cost_avg = cost_avg,
                };
            }
        }
    } else {
        if (reason == .evicted and latencies.present.count() == routing.replica_count) {
            // Already accounted for on .replicated_fully.
            return;
        }
        const cost = routing.history_cost(&latencies);
        routing.active_cost_ema = Cost.ema_add(
            routing.active_cost_ema,
            cost,
        );
    }
}

fn op_route(routing: *const Routing, op: u64) Route {
    if (routing.op_route_alternative(op)) |alternative| {
        return alternative;
    }
    return routing.active;
}

fn op_route_alternative(routing: *const Routing, op: u64) ?Route {
    var prng = stdx.PRNG.from_seed(op | 1);
    if (prng.chance(routing.experiment_chance)) {
        return random_route(&prng, routing.view, routing.replica_count);
    }
    return null;
}

fn history_empty(routing: *Routing) bool {
    if (routing.active_cost_ema != null) return false;
    for (routing.history) |latency| {
        if (latency != null) return false;
    }
    return true;
}

pub fn history_reset(routing: *Routing) void {
    routing.history = .{null} ** history_max;
    routing.active_cost_ema = null;
}

const Quorum = struct {
    latency_to_quorum: u64,
    latency_to_full: u64,
    latency_sum: u64,
};

fn history_cost(routing: *const Routing, op_history: *const OpHistory) Cost {
    var latencies_buffer = op_history.prepare_ok;
    const latencies = latencies_buffer[0..routing.replica_count];
    // Use a simpler sort for code size.
    assert(latencies.len < 16);
    std.sort.insertion(u64, latencies, {}, std.sort.asc(u64));

    const median = latencies[@divFloor(routing.replica_count - 1, 2)]; // Left leaning median.
    const maximum = latencies[routing.replica_count - 1];
    var sum: u64 = 0;
    for (latencies) |latency| sum += latency;

    assert(median <= maximum);
    assert(maximum <= sum);

    return .{ .median = median, .maximum = maximum, .sum = sum };
}

fn slot(routing: *Routing, op: u64) *?OpHistory {
    return &routing.history[op % history_max];
}

fn random_route(prng: *stdx.PRNG, view: u32, replica_count: u8) Route {
    var route: Route = .{};
    for (0..replica_count) |replica| {
        route.append_assume_capacity(@intCast(replica));
    }
    prng.shuffle(u8, route.slice());

    const primary: u8 = @intCast(view % replica_count);
    const primary_index = std.mem.indexOfScalar(u8, route.slice(), primary).?;
    std.mem.swap(
        u8,
        &route.slice()[primary_index],
        &route.slice()[@divFloor(replica_count, 2)],
    );

    return route;
}

test "Routing finds best route" {
    // This fuzzer arranges replicas into a "ring" physical topology according to a random
    // permutation, and checks that we are able to infer the permutation using our cost function.
    // It checks that the happy path works as intended.
    const Environment = struct {
        const Path = @import("../testing/packet_simulator.zig").Path;
        const Packet = union(enum) { prepare: u64, prepare_ok: u64 };
        const PacketSimulator = @import("../testing/packet_simulator.zig")
            .PacketSimulatorType(Packet);

        replica_count: u8,
        view: u32,
        primary: u8,
        replicas: []Routing,
        prepare_ok_count: u8 = 0,
        packet_simulator: PacketSimulator,
        permutation: []u8,

        const Environment = @This();

        pub fn init(gpa: std.mem.Allocator, seed: u64) !Environment {
            var prng = stdx.PRNG.from_seed(seed);

            const replica_count = prng.range_inclusive(u8, 1, constants.replicas_max);
            var packet_simulator = try PacketSimulator.init(gpa, .{
                .node_count = replica_count,
                .client_count = 0,
                .seed = seed,
                .one_way_delay_mean = 0,
                .one_way_delay_min = 0,
                .path_maximum_capacity = replica_count,
                .path_clog_duration_mean = 0,
                .path_clog_probability = ratio(0, 100),
            }, .{
                .packet_command = packet_command,
                .packet_clone = packet_clone,
                .packet_deinit = packet_deinit,
                .packet_deliver = packet_deliver,
                .packet_delay = packet_delay,
            });
            errdefer packet_simulator.deinit(gpa);

            const permuation: []u8 = try gpa.alloc(u8, replica_count);
            errdefer gpa.free(permuation);

            for (0..replica_count) |i| permuation[i] = @intCast(i);
            prng.shuffle(u8, permuation);

            const replicas: []Routing = try gpa.alloc(Routing, replica_count);
            errdefer gpa.free(replicas);

            for (replicas[0..replica_count], 0..) |*replica, replica_index| {
                replica.* = Routing.init(.{
                    .replica = @intCast(replica_index),
                    .replica_count = replica_count,
                });
            }

            const view = prng.range_inclusive(u32, 0, 32);
            const primary = view % replica_count;

            return .{
                .replica_count = replica_count,
                .view = view,
                .primary = @intCast(primary),
                .replicas = replicas,
                .packet_simulator = packet_simulator,
                .permutation = permuation,
            };
        }

        pub fn deinit(env: *Environment, gpa: std.mem.Allocator) void {
            gpa.free(env.replicas);
            gpa.free(env.permutation);
            env.packet_simulator.deinit(gpa);
            env.* = undefined;
        }

        pub fn now_ns(env: *const Environment) u64 {
            return env.packet_simulator.ticks * constants.tick_ms * std.time.ns_per_ms;
        }

        fn ring_index(env: *const Environment, replica: u8) i8 {
            return @intCast(env.permutation[replica]);
        }

        fn distance(env: *const Environment, source: u8, target: u8) u8 {
            return @min(
                @abs(env.ring_index(source) - env.ring_index(target)),
                env.replica_count - @abs(env.ring_index(target) - env.ring_index(source)),
            );
        }

        fn total_route_distance(env: *const Environment, route: Route) u8 {
            if (env.replica_count == 1) return 0;
            var result: u8 = 0;
            for (
                route.const_slice()[0 .. env.replica_count - 1],
                route.const_slice()[1..env.replica_count],
            ) |a, b| {
                result += env.distance(a, b);
            }
            return result;
        }

        fn packet_command(_: *PacketSimulator, _: Packet) @import("../vsr.zig").Command {
            return .ping; // Doesn't matter.
        }
        fn packet_clone(_: *PacketSimulator, packet: Packet) Packet {
            return packet;
        }
        fn packet_deinit(_: *PacketSimulator, _: Packet) void {}

        fn packet_deliver(packet_simulator: *PacketSimulator, packet: Packet, path: Path) void {
            const env: *Environment = @fieldParentPtr("packet_simulator", packet_simulator);

            switch (packet) {
                .prepare => |op| {
                    if (path.target == env.primary) {
                        // Initial prepare injected by the fuzzer.
                        assert(path.source == env.primary);
                        assert(env.prepare_ok_count == 0);
                        env.replicas[env.primary].op_prepare(op, env.now_ns());
                    }
                    env.packet_simulator.submit_packet(.{ .prepare_ok = op }, .{
                        .source = path.target,
                        .target = env.primary,
                    });

                    const targets = env.replicas[path.target].route_next(op);
                    for (targets.const_slice()) |target_next| {
                        assert(target_next < env.replica_count);
                        env.packet_simulator.submit_packet(.{ .prepare = op }, .{
                            .source = path.target,
                            .target = target_next,
                        });
                    }
                },
                .prepare_ok => |op| {
                    assert(path.target == env.primary);
                    env.prepare_ok_count += 1;
                    env.replicas[env.primary].op_prepare_ok(op, path.source, env.now_ns());
                },
            }
        }

        fn packet_delay(packet_simulator: *PacketSimulator, _: Packet, path: Path) u64 {
            const env: *Environment = @fieldParentPtr("packet_simulator", packet_simulator);
            return env.distance(path.source, path.target);
        }
    };

    for (0..10) |seed| {
        var env = try Environment.init(std.testing.allocator, seed);
        defer env.deinit(std.testing.allocator);

        for (env.replicas) |*replica| {
            replica.view_change(env.view);
        }

        // Napkin math:
        // For 6 replicas, there are (replica_count - 1)! = 5! = 120 routes.
        // Two of the routes are optimal, which gives 1/60 chance of success per experiment.
        // Experiment runs on a pair of ops with probability 1/20, so we expect 40 ops per
        // experiment. 10k ops gives us 250 experiments.
        //
        // Probability that we don't select the best root in under 10_000 ops is (59/60)**250≈0.015.
        var op_improvement: usize = 0;
        for (1..10_000) |op| {
            env.prepare_ok_count = 0;
            env.packet_simulator.submit_packet(.{ .prepare = op }, .{
                .source = env.primary,
                .target = env.primary,
            });

            for (0..1_000) |_| {
                while (env.packet_simulator.step()) {}
                env.packet_simulator.tick();
                if (env.prepare_ok_count == env.replica_count) break;
            } else @panic("loop outrun safety counter");

            if (env.replicas[env.primary].route_improvement()) |improvement| {
                op_improvement = op;
                const active = env.replicas[env.primary].active;
                assert(env.total_route_distance(active) > env.replica_count - 1);
                maybe(env.total_route_distance(improvement) > env.total_route_distance(active));

                for (env.replicas) |*replica| {
                    replica.history_reset();
                    replica.route_activate(improvement);
                }
            }
        }
        assert(env.total_route_distance(env.replicas[env.primary].active) == env.replica_count - 1);
    }
}

test "Routing fuzz" {
    // This fuzzer doesn't try to be particuarlily realistic, and just hammers the API with a random
    // sequence of calls, to make sure nothing breaks.
    const Environment = struct {
        const fuzz = @import("../testing/fuzz.zig");

        steps: u32 = 10_000,
        prng: stdx.PRNG,

        routing: Routing,
        pipeline_length: u32,
        view: u32,
        op: u64,
        commit_max: u64,
        time: u64,

        const Environment = @This();

        fn init(seed: u64) Environment {
            var prng = stdx.PRNG.from_seed(seed);
            const replica_count = prng.range_inclusive(u8, 1, constants.replicas_max);
            const replica = prng.int_inclusive(u8, replica_count - 1);

            const pipeline_length = prng.range_inclusive(u32, 1, constants.pipeline_prepare_queue_max);

            return .{
                .prng = prng,
                .routing = Routing.init(.{
                    .replica = replica,
                    .replica_count = replica_count,
                }),
                .pipeline_length = pipeline_length,
                .op = 0,
                .commit_max = 0,
                .view = 0,
                .time = 0,
            };
        }

        fn run(env: *Environment) !void {
            for (0..env.steps) |_| {
                env.time += env.prng.int_inclusive(u64, 100);
                try env.run_step();
            }
        }

        fn run_step(env: *Environment) !void {
            const Actions = enum {
                view_change,
                prepare,
                prepare_ok,
                reroute,
            };

            const action = env.prng.enum_weighted(Actions, .{
                .view_change = 1,
                .prepare = 5,
                .prepare_ok = 10,
                .reroute = 1,
            });
            switch (action) {
                .view_change => {
                    env.view += env.prng.range_inclusive(u8, 1, 2 * env.routing.replica_count);
                    env.op = env.prng.range_inclusive(u64, env.commit_max, env.op);
                    env.routing.history_reset();
                    env.routing.view_change(env.view);
                },
                .prepare => {
                    if (env.primary()) {
                        if (env.op - env.commit_max < env.pipeline_length) {
                            const op = env.op;
                            env.op += 1;
                            env.tick();
                            env.routing.op_prepare(op, env.time);
                            env.verify_route(op);
                        }
                    } else {
                        const op = env.prng.range_inclusive(
                            u64,
                            env.commit_max,
                            env.commit_max + env.pipeline_length,
                        );
                        env.verify_route(op);
                    }
                },
                .prepare_ok => {
                    const op = if (env.prng.chance(ratio(8, 10)))
                        env.prng.range_inclusive(
                            u64,
                            env.commit_max -| env.pipeline_length,
                            env.op,
                        )
                    else
                        env.prng.int_inclusive(
                            u64,
                            env.op + 2 * env.pipeline_length,
                        );
                    const backup = env.prng.int_inclusive(u8, env.routing.replica_count - 1);
                    env.tick();
                    env.routing.op_prepare_ok(op, backup, env.time);
                    if (env.prng.boolean()) {
                        env.commit_max = @min(env.op, env.commit_max + 1);
                    }
                },
                .reroute => {
                    if (env.primary()) {
                        if (env.routing.route_improvement()) |improvement| {
                            env.routing.history_reset();
                            env.routing.route_activate(improvement);
                        }
                    } else {
                        const route = random_route(&env.prng, env.view, env.routing.replica_count);
                        env.routing.route_activate(route);
                    }
                },
            }
        }

        fn verify_route(env: *const Environment, op: u64) void {
            var visited: stdx.BitSetType(constants.replicas_max) = .{};
            for (0..env.routing.replica_count) |replica_usize| {
                const replica: u8 = @intCast(replica_usize);
                var routing = env.routing;
                routing.replica = replica;

                const route = routing.route_next(op);
                assert(route.count() <= 2);
                if (replica == env.view % routing.replica_count) {
                    switch (routing.replica_count) {
                        0 => unreachable,
                        1 => assert(route.count() == 0),
                        2 => assert(route.count() == 1),
                        else => assert(route.count() == 2),
                    }
                } else {
                    assert(route.count() <= 1);
                }
                for (route.const_slice()) |next| {
                    assert(next != replica);
                    assert(!visited.is_set(next));
                    visited.set(next);
                }
            }
            for (0..constants.replicas_max) |replica| {
                assert(visited.is_set(replica) ==
                    (replica < env.routing.replica_count and
                    replica != env.view % env.routing.replica_count));
            }
        }

        fn tick(env: *Environment) void {
            env.time += fuzz.random_int_exponential(&env.prng, u64, 10);
        }

        fn primary(env: *const Environment) bool {
            return env.routing.replica == env.view % env.routing.replica_count;
        }
    };

    var env = Environment.init(92);
    try env.run();
    assert(env.op > 1_000);
    assert(env.commit_max > 1_000);
}
