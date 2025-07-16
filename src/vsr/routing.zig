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
//! To remove outliers, the experiment take two ops. op and op^1 are either both experimental, or
//! both non-experimental. Experimental route cost is the average of two costs. Active route cost
//! is maintained as exponential weighted moving average.
const std = @import("std");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const constants = @import("../constants.zig");
const stdx = @import("../stdx.zig");
const Ratio = stdx.PRNG.Ratio;
const ratio = stdx.PRNG.ratio;
const Instant = stdx.Instant;
const Duration = stdx.Duration;

const history_max = constants.pipeline_prepare_queue_max * 2;

// This constant serves two purposes:
// - First, as a cap for all latencies, to make sure that u64 computations can't overflow.
// - Second, to simplify cost function computations, replica we haven't heard from has this latency.
const latency_max: Duration = .{ .ns = 10 * std.time.ns_per_hour };

replica: u8,
replica_count: u8,
standby_count: u8,
view: u32,

/// The route we currently use.
a: Route,
a_cost: ?Cost, // Computed as exponential weighted moving average.

/// The best alternative, which might be better or worse than a.
b: Route,
b_cost: ?Cost, // Computed as an average of two experiments.

experiment_chance: Ratio = ratio(1, 20),

history: [history_max]OpHistory,

pub const Route = stdx.BoundedArrayType(u8, constants.replicas_max);

const OpHistory = struct {
    op: u64,
    prepare: Instant,
    prepare_ok: [constants.replicas_max]Duration = @splat(latency_max),
    present: stdx.BitSetType(constants.replicas_max) = .{},

    fn root() OpHistory {
        return .{
            .op = 0,
            .prepare = .{ .ns = 0 },
        };
    }

    fn record_prepare_ok(
        op_history: *OpHistory,
        replica: u8,
        now: Instant,
    ) void {
        const latency = now.duration_since(op_history.prepare);
        if (op_history.present.is_set(replica)) {
            assert(op_history.prepare_ok[replica].ns <= latency.ns);
            return;
        }
        assert(!op_history.present.is_set(replica));
        assert(op_history.prepare_ok[replica].ns == latency_max.ns);

        op_history.prepare_ok[replica] = Duration.min(latency, latency_max);
        op_history.present.set(replica);
    }
};

const Cost = struct {
    // Left-biased median latency matches replication quorum latency, and directly contributes to
    // user-visible latency.
    median: Duration,
    // Maximum latency corresponds to the time when a prepare is fully replicated, and is important
    // for the overall health of the cluster.
    maximum: Duration,
    // Worst-case latency tracks the length of the critical path, but we want non-critical paths to
    // be as short as possible.
    sum: Duration,

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
            if (less_significantly(@field(lhs, field), @field(rhs, field), threshold)) return true;
            if (@field(lhs, field).ns > @field(rhs, field).ns) return false;
        }
        return false;
    }

    // Returns true if lhs + lhs⋅threshold < rhs.
    fn less_significantly(lhs: Duration, rhs: Duration, threshold: Ratio) bool {
        assert(threshold.numerator < threshold.denominator);
        return lhs.ns * (threshold.numerator + threshold.denominator) <
            rhs.ns * threshold.denominator;
    }

    fn average(lhs: Cost, rhs: Cost) Cost {
        return .{
            .median = .{ .ns = @divFloor(lhs.median.ns + rhs.median.ns, 2) },
            .maximum = .{ .ns = @divFloor(lhs.maximum.ns + rhs.maximum.ns, 2) },
            .sum = .{ .ns = @divFloor(lhs.sum.ns + rhs.sum.ns, 2) },
        };
    }

    fn ewma_add(old: Cost, new: Cost) Cost {
        return .{
            .median = ewma_add_duration(old.median, new.median),
            .maximum = ewma_add_duration(old.maximum, new.maximum),
            .sum = ewma_add_duration(old.sum, new.sum),
        };
    }

    fn ewma_add_duration(old: Duration, new: Duration) Duration {
        return .{
            .ns = @divFloor((old.ns * 4) + new.ns, 5),
        };
    }
};

const Routing = @This();

pub fn init(options: struct {
    replica: u8,
    replica_count: u8,
    standby_count: u8,
}) Routing {
    assert(options.replica < options.replica_count + options.standby_count);
    assert(options.replica_count <= constants.replicas_max);
    assert(options.standby_count <= constants.standbys_max);

    const route = route_view_default(0, options.replica_count);

    return .{
        .replica = options.replica,
        .replica_count = options.replica_count,
        .standby_count = options.standby_count,

        .view = 0,
        .a = route,
        .a_cost = null,

        .b = route,
        .b_cost = null,

        .history = @splat(.root()),
    };
}

pub fn view_change(routing: *Routing, view: u32) void {
    assert(view > routing.view or (view == 0 and routing.view == 0));
    assert(routing.history_empty());
    routing.view = view;

    const route = route_view_default(view, routing.replica_count);
    assert(routing.route_valid(route));

    routing.a = route;
    assert(routing.a_cost == null);

    routing.b = route;
    assert(routing.b_cost == null);
}

fn route_valid(routing: *const Routing, route: Route) bool {
    if (route.count() != routing.replica_count) return false;

    for (route.const_slice()) |replica| {
        if (replica >= routing.replica_count) return false;
    }
    for (0..routing.replica_count) |i| {
        for (0..i) |j| {
            if (route.get(i) == route.get(j)) return false;
        }
    }

    const primary_index: u8 = @intCast(routing.view % routing.replica_count);
    const primary_position = @divFloor(route.count(), 2);
    if (route.get(primary_position) != primary_index) return false;

    return true;
}

fn route_view_default(view: u32, replica_count: u8) Route {
    var route: Route = .{};
    for (0..replica_count) |replica| {
        route.push(@intCast(replica));
    }

    // Rotate primary to the midpoint;
    const primary_index: u8 = @intCast(view % replica_count);
    const midpoint = @divFloor(replica_count, 2);
    const rotation = (replica_count + primary_index - midpoint) % replica_count;
    std.mem.rotate(u8, route.slice(), rotation);
    assert(route.slice()[midpoint] == primary_index);

    return route;
}

fn route_random(prng: *stdx.PRNG, view: u32, replica_count: u8) Route {
    var route: Route = .{};
    for (0..replica_count) |replica| {
        route.push(@intCast(replica));
    }
    prng.shuffle(u8, route.slice());

    const primary_index: u8 = @intCast(view % replica_count);
    const primary_position = std.mem.indexOfScalar(u8, route.slice(), primary_index).?;
    std.mem.swap(
        u8,
        &route.slice()[primary_position],
        &route.slice()[@divFloor(replica_count, 2)],
    );

    return route;
}

pub fn route_encode(routing: *const Routing, route: Route) u64 {
    comptime assert(constants.replicas_max <= @sizeOf(u64));
    assert(routing.route_valid(route));
    var code: u64 = 0;
    for (0..@sizeOf(u64)) |index| {
        const byte: u64 = if (index < routing.replica_count)
            route.get(index)
        else
            @as(u8, 0xFF);
        const shift: u6 = @bitSizeOf(u8) * @as(u6, @intCast(index));
        code |= byte << shift;
    }
    assert(code != 0);
    return code;
}

// Positive space testing --- encode every single route!
test route_encode {
    const Gen = @import("../testing/exhaustigen.zig");

    for (1..constants.replicas_max + 1) |replica_count| {
        var g: Gen = .{};

        while (!g.done()) {
            var pool: stdx.BoundedArrayType(u8, constants.replicas_max) = .{};
            for (0..replica_count) |i| pool.push(@intCast(i));

            var route: stdx.BoundedArrayType(u8, constants.replicas_max) = .{};
            assert(route.count() == 0);
            assert(pool.count() == replica_count);
            for (0..replica_count) |_| {
                const index = g.index(pool.const_slice());
                route.push(pool.get(index));
                _ = pool.ordered_remove(index);
            }
            assert(route.count() == replica_count);
            assert(pool.count() == 0);

            const primary_index = route.get(@divFloor(route.count(), 2));
            var routing = Routing.init(.{
                .replica = primary_index,
                .replica_count = @intCast(replica_count),
                .standby_count = 0,
            });
            routing.view_change(primary_index);

            const code = routing.route_encode(route);
            const route_decoded = routing.route_decode(code).?;

            assert(std.mem.eql(u8, route.const_slice(), route_decoded.const_slice()));
        }
    }
}

pub fn route_decode(routing: *const Routing, code: u64) ?Route {
    var route: Route = .{};
    for (0..@sizeOf(u64)) |index| {
        const shift: u6 = @bitSizeOf(u8) * @as(u6, @intCast(index));
        const byte: u64 = (code >> shift) & 0xFF;
        if (index < routing.replica_count) {
            if (byte < routing.replica_count) {
                route.push(@intCast(byte));
            } else {
                return null;
            }
        } else {
            if (byte == 0xFF) {
                // "Blanks" are filled with all ones,
            } else {
                return null;
            }
        }
    }

    if (!routing.route_valid(route)) return null;

    return route;
}

// Negative space testing, check that if a 'random' number decodes, it decodes to a route.
// It is possible to write an exhaustigen test here, but it takes 30s in debug, which is too slow.
test route_decode {
    var prng = stdx.PRNG.from_seed(92);
    var valid: u32 = 0;
    for (0..200_000) |_| {
        const replica_count = prng.range_inclusive(u8, 1, constants.replicas_max);

        var code_bytes: [8]u8 = @splat(0);
        for (&code_bytes) |*byte| {
            byte.* = if (prng.chance(ratio(replica_count + 1, 8)))
                prng.int_inclusive(u8, constants.replicas_max + 1)
            else
                0xFF;
        }
        var code: u64 = @bitCast(code_bytes);

        if (prng.chance(ratio(1, 20))) {
            code ^= @as(u64, 1) << prng.int_inclusive(u6, @bitSizeOf(u64) - 1);
        }

        var routing = Routing.init(.{
            .replica = prng.int_inclusive(u8, replica_count - 1),
            .replica_count = replica_count,
            .standby_count = prng.int_inclusive(u8, constants.standbys_max),
        });
        routing.view_change(prng.int_inclusive(u32, 10_000));

        if (routing.route_decode(code)) |route| {
            valid += 1;
            const code_encoded = routing.route_encode(route);
            assert(code == code_encoded);
        }
    }
    assert(valid > 50);
}

pub fn route_activate(routing: *Routing, route: Route) void {
    assert(routing.history_empty());
    assert(routing.route_valid(route));
    routing.a = route;
    assert(routing.a_cost == null);
}

pub fn route_improvement(routing: *const Routing) ?Route {
    const a_cost = routing.a_cost orelse return null;
    const b_cost = routing.b_cost orelse return null;
    return if (Cost.less(b_cost, a_cost)) routing.b else null;
}

const NextHop = stdx.BoundedArrayType(u8, 2);
pub fn op_next_hop(routing: *const Routing, op: u64) NextHop {
    const route = routing.op_route(op);
    assert(routing.route_valid(route));

    const primary_index: u8 = @intCast(routing.view % routing.replica_count);
    const primary_position = @divFloor(route.count(), 2);
    assert(route.get(primary_position) == primary_index);

    var result: NextHop = .{};

    if (routing.replica < routing.replica_count) {
        // Normal replication: replicate to 0-2 other replicas using a dynamic route.

        const replica_position = std.mem.indexOfScalar(u8, route.const_slice(), routing.replica).?;

        if (replica_position <= primary_position and replica_position > 0) {
            result.push(route.const_slice()[replica_position - 1]);
        }
        if (replica_position >= primary_position and replica_position < routing.replica_count - 1) {
            result.push(route.const_slice()[replica_position + 1]);
        }

        assert(result.count() <= 2);
        assert((result.count() == 2) ==
            (replica_position == primary_position and routing.replica_count >= 3));
        if (routing.standby_count > 0) {
            if (replica_position == 0) {
                const first_standby = routing.replica_count;
                result.push(first_standby);
            }
        }
    } else {
        // Standby replication uses static ring topology.
        if (routing.replica + 1 < routing.replica_count + routing.standby_count) {
            result.push(routing.replica + 1);
        }
    }

    return result;
}

pub fn op_prepare(routing: *Routing, op: u64, now: Instant) void {
    assert(routing.primary());
    assert(op != 0); // Root ops is never prepared.
    const slot = op % history_max;
    if (routing.history[slot].op != 0) {
        routing.op_finalize(routing.history[slot].op, .evicted);
    }

    routing.history[slot] = .{
        .op = op,
        .prepare = now,
    };
}

pub fn op_prepare_ok(routing: *Routing, op: u64, replica: u8, now: Instant) void {
    assert(routing.primary());
    // Replicas can ack the root op after repair. While we can prevent replicas from sending such
    // prepare_ok that will make the protocol more complex. Instead, ignore op=0 here and treat it
    // as empty slot elsewhere.
    if (op == 0) return;
    maybe(replica == routing.replica);
    const slot = op % history_max;
    if (routing.history[slot].op != op) return;

    routing.history[slot].record_prepare_ok(replica, now);
    if (routing.history[slot].present.count() == routing.replica_count) {
        routing.op_finalize(op, .replicated_fully);
    }
}

fn op_finalize(
    routing: *Routing,
    op: u64,
    reason: enum { evicted, replicated_fully },
) void {
    assert(routing.primary());
    assert(op != 0);
    assert(routing.history[op % history_max].op == op);
    assert(routing.history[op % history_max].present.count() <= routing.replica_count);
    if (reason == .replicated_fully) {
        assert(routing.history[op % history_max].present.count() == routing.replica_count);
    }

    if (routing.op_route_b(op)) |route_b| {
        var replicated_fully_count: u8 = 0;
        var cost_average: ?Cost = null;

        for ([2]u64{ op, op ^ 1 }) |experiment| {
            assert(std.meta.eql(routing.op_route_b(experiment), route_b));

            const slot = experiment % history_max;
            if (routing.history[slot].op != experiment) {
                // Don't have data for the other experiment yet.
                return;
            }

            replicated_fully_count +=
                @intFromBool(routing.history[slot].present.count() == routing.replica_count);
            const new = routing.history_cost(experiment);
            cost_average = if (cost_average) |old| Cost.average(old, new) else new;
        }
        assert(cost_average != null);

        if ((reason == .replicated_fully and replicated_fully_count == 2) or
            (reason == .evicted and replicated_fully_count < 2))
        {
            if (routing.b_cost == null or Cost.less(cost_average.?, routing.b_cost.?)) {
                routing.b = route_b;
                routing.b_cost = cost_average.?;
            }
        }
    } else {
        const slot = op % history_max;

        if (reason == .replicated_fully or
            (reason == .evicted and routing.history[slot].present.count() < routing.replica_count))
        {
            const new = routing.history_cost(op);
            routing.a_cost = if (routing.a_cost) |old| Cost.ewma_add(old, new) else new;
        }
    }
}

fn op_route(routing: *const Routing, op: u64) Route {
    return routing.op_route_b(op) orelse routing.a;
}

fn op_route_b(routing: *const Routing, op: u64) ?Route {
    var prng = stdx.PRNG.from_seed(op | 1);
    if (prng.chance(routing.experiment_chance)) {
        const route = route_random(&prng, routing.view, routing.replica_count);
        assert(routing.route_valid(route));
        return route;
    }
    return null;
}

fn history_empty(routing: *Routing) bool {
    if (routing.a_cost != null) return false;
    if (routing.b_cost != null) return false;
    for (routing.history) |h| {
        if (h.op != 0) return false;
    }
    return true;
}

pub fn history_reset(routing: *Routing) void {
    routing.history = @splat(.root());
    routing.a_cost = null;
    routing.b_cost = null;
}

fn history_cost(routing: *const Routing, op: u64) Cost {
    const slot = op % history_max;
    assert(routing.history[slot].op == op);
    assert(routing.history[slot].present.count() <= routing.replica_count);

    var latencies_buffer: [constants.replicas_max]Duration = routing.history[slot].prepare_ok;
    const latencies = latencies_buffer[0..routing.replica_count];
    // Use a simpler sort for code size.
    assert(latencies.len < 16);
    std.sort.insertion(Duration, latencies, {}, Duration.sort.asc);

    const median = latencies[@divFloor(routing.replica_count - 1, 2)]; // Left leaning median.
    const maximum = latencies[routing.replica_count - 1];
    var sum: Duration = .{ .ns = 0 };
    for (latencies) |latency| sum.ns += latency.ns;

    assert(median.ns <= maximum.ns);
    assert(maximum.ns <= sum.ns);

    return .{ .median = median, .maximum = maximum, .sum = sum };
}

fn primary(routing: *const Routing) bool {
    const primary_index: u8 = @intCast(routing.view % routing.replica_count);
    return routing.replica == primary_index;
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
                    .standby_count = 0,
                });
            }

            const view = prng.range_inclusive(u32, 0, 32);
            const primary_index = view % replica_count;

            return .{
                .replica_count = replica_count,
                .view = view,
                .primary = @intCast(primary_index),
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

        pub fn now(env: *const Environment) Instant {
            return .{ .ns = env.packet_simulator.ticks * constants.tick_ms * std.time.ns_per_ms };
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
                        env.replicas[env.primary].op_prepare(op, env.now());
                    }
                    env.packet_simulator.submit_packet(.{ .prepare_ok = op }, .{
                        .source = path.target,
                        .target = env.primary,
                    });

                    const targets = env.replicas[path.target].op_next_hop(op);
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
                    env.replicas[env.primary].op_prepare_ok(op, path.source, env.now());
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

            if (env.replicas[env.primary].route_improvement()) |b| {
                op_improvement = op;
                const a = env.replicas[env.primary].a;
                assert(env.total_route_distance(a) > env.replica_count - 1);
                maybe(env.total_route_distance(b) > env.total_route_distance(a));

                for (env.replicas) |*replica| {
                    replica.history_reset();
                    replica.route_activate(b);
                }
            }
        }
        assert(env.total_route_distance(env.replicas[env.primary].a) == env.replica_count - 1);
    }
}

test "Routing fuzz" {
    // This fuzzer doesn't try to be realistic, and just hammers the API
    // with a random sequence of calls, to make sure nothing breaks.
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
            const standby_count = prng.range_inclusive(u8, 0, constants.standbys_max);
            const replica = prng.int_inclusive(u8, replica_count - 1);

            const pipeline_length = prng.range_inclusive(
                u32,
                1,
                constants.pipeline_prepare_queue_max,
            );

            return .{
                .prng = prng,
                .routing = Routing.init(.{
                    .replica = replica,
                    .replica_count = replica_count,
                    .standby_count = standby_count,
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
                            env.op += 1;
                            const op = env.op;
                            env.tick();
                            env.routing.op_prepare(op, .{ .ns = env.time });
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
                            @max(1, env.commit_max -| env.pipeline_length),
                            env.op,
                        )
                    else
                        env.prng.range_inclusive(
                            u64,
                            1,
                            env.op + 2 * env.pipeline_length,
                        );
                    const backup = env.prng.int_inclusive(u8, env.routing.replica_count - 1);
                    env.tick();
                    env.routing.op_prepare_ok(op, backup, .{ .ns = env.time });
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
                        const route = route_random(&env.prng, env.view, env.routing.replica_count);
                        env.routing.route_activate(route);
                    }
                },
            }
        }

        fn verify_route(env: *const Environment, op: u64) void {
            var visited: stdx.BitSetType(constants.replicas_max) = .{};
            const member_count = env.routing.replica_count + env.routing.standby_count;
            for (0..member_count) |replica_usize| {
                const replica: u8 = @intCast(replica_usize);
                var routing = env.routing;
                routing.replica = replica;

                const route = routing.op_next_hop(op);
                assert(route.count() <= 2);
                if (replica == env.view % routing.replica_count) {
                    switch (routing.replica_count) {
                        0 => unreachable,
                        1 => assert((route.count() == 0) == (routing.standby_count == 0)),
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
                    (replica < member_count and
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
