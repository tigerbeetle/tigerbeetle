const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.clock);

const config = @import("../config.zig");

const clock_offset_tolerance_max: u64 = config.clock_offset_tolerance_max_ms * std.time.ns_per_ms;
const epoch_max: u64 = config.clock_epoch_max_ms * std.time.ns_per_ms;
const window_min: u64 = config.clock_synchronization_window_min_ms * std.time.ns_per_ms;
const window_max: u64 = config.clock_synchronization_window_max_ms * std.time.ns_per_ms;

const Marzullo = @import("../marzullo.zig").Marzullo;

pub fn Clock(comptime Time: type) type {
    return struct {
        const Self = @This();

        const Sample = struct {
            /// The relative difference between our wall clock reading and that of the remote clock source.
            clock_offset: i64,
            one_way_delay: u64,
        };

        const Epoch = struct {
            /// The best clock offset sample per remote clock source (with minimum one way delay) collected
            /// over the course of a window period of several seconds.
            sources: []?Sample,

            /// The total number of samples learned while synchronizing this epoch.
            samples: usize,

            /// The monotonic clock timestamp when this epoch began. We use this to measure elapsed time.
            monotonic: u64,

            /// The wall clock timestamp when this epoch began. We add the elapsed monotonic time to this
            /// plus the synchronized clock offset to arrive at a synchronized realtime timestamp. We
            /// capture this realtime when starting the epoch, before we take any samples, to guard against
            /// any jumps in the system's realtime clock from impacting our measurements.
            realtime: i64,

            /// Once we have enough source clock offset samples in agreement, the epoch is synchronized.
            /// We then have lower and upper bounds on the true cluster time, and can install this epoch for
            /// subsequent clock readings. This epoch is then valid for several seconds, while clock drift
            /// has not had enough time to accumulate into any significant clock skew, and while we collect
            /// samples for the next epoch to refresh and replace this one.
            synchronized: ?Marzullo.Interval,

            /// A guard to prevent synchronizing too often without having learned any new samples.
            learned: bool = false,

            fn elapsed(epoch: *Epoch, clock: *Self) u64 {
                return clock.monotonic() - epoch.monotonic;
            }

            fn reset(epoch: *Epoch, clock: *Self) void {
                std.mem.set(?Sample, epoch.sources, null);
                // A replica always has zero clock offset and network delay to its own system time reading:
                epoch.sources[clock.replica] = Sample{
                    .clock_offset = 0,
                    .one_way_delay = 0,
                };
                epoch.samples = 1;
                epoch.monotonic = clock.monotonic();
                epoch.realtime = clock.realtime();
                epoch.synchronized = null;
                epoch.learned = false;
            }

            fn sources_sampled(epoch: *Epoch) usize {
                var count: usize = 0;
                for (epoch.sources) |sampled| {
                    if (sampled != null) count += 1;
                }
                return count;
            }
        };

        allocator: *std.mem.Allocator,

        /// The index of the replica using this clock to provide synchronized time.
        replica: u8,

        /// The underlying time source for this clock (system time or deterministic time).
        time: Time,

        /// An epoch from which the clock can read synchronized clock timestamps within safe bounds.
        /// At least `config.clock_synchronization_window_min_ms` is needed for this to be ready to use.
        epoch: Epoch,

        /// The next epoch (collecting samples and being synchronized) to replace the current epoch.
        window: Epoch,

        /// A static allocation to convert window samples into tuple bounds for Marzullo's algorithm.
        marzullo_tuples: []Marzullo.Tuple,

        /// A kill switch to revert to unsynchronized realtime.
        synchronization_disabled: bool,

        pub fn init(
            allocator: *std.mem.Allocator,
            /// The size of the cluster, i.e. the number of clock sources (including this replica).
            replica_count: u8,
            replica: u8,
            time: Time,
        ) !Self {
            assert(replica_count > 0);
            assert(replica < replica_count);

            var epoch: Epoch = undefined;
            epoch.sources = try allocator.alloc(?Sample, replica_count);
            errdefer allocator.free(epoch.sources);

            var window: Epoch = undefined;
            window.sources = try allocator.alloc(?Sample, replica_count);
            errdefer allocator.free(window.sources);

            // There are two Marzullo tuple bounds (lower and upper) per source clock offset sample:
            var marzullo_tuples = try allocator.alloc(Marzullo.Tuple, replica_count * 2);
            errdefer allocator.free(marzullo_tuples);

            var self = Self{
                .allocator = allocator,
                .replica = replica,
                .time = time,
                .epoch = epoch,
                .window = window,
                .marzullo_tuples = marzullo_tuples,
                .synchronization_disabled = replica_count == 1, // A cluster of one cannot synchronize.
            };

            // Reset the current epoch to be unsynchronized,
            self.epoch.reset(&self);
            // and open a new epoch window to start collecting samples...
            self.window.reset(&self);

            return self;
        }

        /// Called by `Replica.on_pong()` with:
        /// * the index of the `replica` that has replied to our ping with a pong,
        /// * our monotonic timestamp `m0` embedded in the ping we sent, carried over into this pong,
        /// * the remote replica's `realtime()` timestamp `t1`, and
        /// * our monotonic timestamp `m2` as captured by our `Replica.on_pong()` handler.
        pub fn learn(self: *Self, replica: u8, m0: u64, t1: i64, m2: u64) void {
            if (self.synchronization_disabled) return;

            // A network routing fault must have replayed one of our outbound messages back against us:
            if (replica == self.replica) return;

            // Our m0 and m2 readings should always be monotonically increasing.
            // This condition should never be true. Reject this as a bad sample:
            if (m0 >= m2) return;

            // We may receive delayed packets after a reboot, in which case m0/m2 may be invalid:
            if (m0 < self.window.monotonic) return;
            if (m2 < self.window.monotonic) return;
            const elapsed: u64 = m2 - self.window.monotonic;
            if (elapsed > window_max) return;

            const round_trip_time: u64 = m2 - m0;
            const one_way_delay: u64 = round_trip_time / 2;
            const t2: i64 = self.window.realtime + @intCast(i64, elapsed);
            const clock_offset: i64 = t1 + @intCast(i64, one_way_delay) - t2;
            const asymmetric_delay = self.estimate_asymmetric_delay(
                replica,
                one_way_delay,
                clock_offset,
            );
            const clock_offset_corrected = clock_offset + asymmetric_delay;

            log.info("learn: replica={} m0={} t1={} m2={} t2={} one_way_delay={} " ++
                "asymmetric_delay={} clock_offset={}", .{
                replica,
                m0,
                t1,
                m2,
                t2,
                one_way_delay,
                asymmetric_delay,
                clock_offset_corrected,
            });

            // The less network delay, the more likely we have an accurante clock offset measurement:
            self.window.sources[replica] = minimum_one_way_delay(self.window.sources[replica], Sample{
                .clock_offset = clock_offset_corrected,
                .one_way_delay = one_way_delay,
            });

            self.window.samples += 1;

            // We decouple calls to `synchronize()` so that it's not triggered by these network events.
            // Otherwise, excessive duplicate network packets would burn the CPU.
            self.window.learned = true;
        }

        /// Called by `Replica.on_ping_timeout()` to provide `m0` when we decide to send a ping.
        /// Called by `Replica.on_pong()` to provide `m2` when we receive a pong.
        pub fn monotonic(self: *Self) u64 {
            return self.time.monotonic();
        }

        /// Called by `Replica.on_ping()` when responding to a ping with a pong.
        /// This should never be used by the state machine, only for measuring clock offsets.
        pub fn realtime(self: *Self) i64 {
            return self.time.realtime();
        }

        /// Called by `StateMachine.prepare_timestamp()` when the leader wants to timestamp a batch.
        /// If the leader's clock is not synchronized with the cluster, it must wait until it is.
        /// Returns the system time clamped to be within our synchronized lower and upper bounds.
        /// This is complementary to NTP and allows clusters with very accurate time to make use of it,
        /// while providing guard rails for when NTP is partitioned or unable to correct quickly enough.
        pub fn realtime_synchronized(self: *Self) ?i64 {
            if (self.synchronization_disabled) {
                return self.realtime();
            } else if (self.epoch.synchronized) |interval| {
                const elapsed = @intCast(i64, self.epoch.elapsed(self));
                return std.math.clamp(
                    self.realtime(),
                    self.epoch.realtime + elapsed + interval.lower_bound,
                    self.epoch.realtime + elapsed + interval.upper_bound,
                );
            } else {
                return null;
            }
        }

        pub fn tick(self: *Self) void {
            self.time.tick();

            if (self.synchronization_disabled) return;
            self.synchronize();
            // Expire the current epoch if successive windows failed to synchronize:
            // Gradual clock drift prevents us from using an epoch for more than a few tens of seconds.
            if (self.epoch.elapsed(self) >= epoch_max) {
                log.alert("no agreement on cluster time (partitioned or too many clock faults)", .{});
                self.epoch.reset(self);
            }
        }

        /// Estimates the asymmetric delay for a sample compared to the previous window, according to
        /// Algorithm 1 from Section 4.2, "A System for Clock Synchronization in an Internet of Things".
        fn estimate_asymmetric_delay(
            self: *Self,
            replica: u8,
            one_way_delay: u64,
            clock_offset: i64,
        ) i64 {
            const error_margin = 10 * std.time.ns_per_ms;

            if (self.epoch.sources[replica]) |epoch| {
                if (one_way_delay <= epoch.one_way_delay) {
                    return 0;
                } else if (clock_offset > epoch.clock_offset + error_margin) {
                    // The asymmetric error is on the forward network path.
                    return 0 - @intCast(i64, one_way_delay - epoch.one_way_delay);
                } else if (clock_offset < epoch.clock_offset - error_margin) {
                    // The asymmetric error is on the reverse network path.
                    return 0 + @intCast(i64, one_way_delay - epoch.one_way_delay);
                } else {
                    return 0;
                }
            } else {
                return 0;
            }
        }

        fn synchronize(self: *Self) void {
            assert(self.window.synchronized == null);

            // Wait until the window has enough accurate samples:
            const elapsed = self.window.elapsed(self);
            if (elapsed < window_min) return;
            if (elapsed >= window_max) {
                // We took too long to synchronize the window, expire stale samples...
                const sources_sampled = self.window.sources_sampled();
                if (sources_sampled <= @divTrunc(self.window.sources.len, 2)) {
                    log.crit("synchronization window failed, partitioned (sources={} samples={})", .{
                        sources_sampled,
                        self.window.samples,
                    });
                } else {
                    log.crit("synchronization window failed, no agreement (sources={} samples={})", .{
                        sources_sampled,
                        self.window.samples,
                    });
                }
                self.window.reset(self);
                return;
            }

            if (!self.window.learned) return;
            // Do not reset `learned` any earlier than this (before we have attempted to synchronize).
            self.window.learned = false;

            // Starting with the most clock offset tolerance, while we have a majority, find the best
            // smallest interval with the least clock offset tolerance, reducing tolerance at each step:
            var tolerance: u64 = clock_offset_tolerance_max;
            var terminate = false;
            var rounds: usize = 0;
            // Do at least one round if tolerance=0 and cap the number of rounds to avoid runaway loops.
            while (!terminate and rounds < 64) : (tolerance /= 2) {
                if (tolerance == 0) terminate = true;
                rounds += 1;

                const interval = Marzullo.smallest_interval(self.window_tuples(tolerance));
                const majority = interval.sources_true > @divTrunc(self.window.sources.len, 2);
                if (!majority) break;

                // The new interval may reduce the number of `sources_true` while also decreasing error.
                // In other words, provided we maintain a majority, we prefer tighter tolerance bounds.
                self.window.synchronized = interval;
            }

            // Wait for more accurate samples or until we timeout the window for lack of majority:
            if (self.window.synchronized == null) return;

            var new_window = self.epoch;
            new_window.reset(self);
            self.epoch = self.window;
            self.window = new_window;

            self.after_synchronization();
        }

        fn after_synchronization(self: *Self) void {
            const new_interval = self.epoch.synchronized.?;

            log.info("replica={} synchronized: truechimers={}/{} clock_offset={}..{} accuracy={}", .{
                self.replica,
                new_interval.sources_true,
                self.epoch.sources.len,
                fmtDurationSigned(new_interval.lower_bound),
                fmtDurationSigned(new_interval.upper_bound),
                fmtDurationSigned(new_interval.upper_bound - new_interval.lower_bound),
            });

            const elapsed = @intCast(i64, self.epoch.elapsed(self));
            const system = self.realtime();
            const lower = self.epoch.realtime + elapsed + new_interval.lower_bound;
            const upper = self.epoch.realtime + elapsed + new_interval.upper_bound;
            const cluster = std.math.clamp(system, lower, upper);

            if (system == cluster) {} else if (system < lower) {
                const delta = lower - system;
                if (delta < std.time.ns_per_ms) {
                    log.info("replica={} system time is {} behind", .{
                        self.replica,
                        fmtDurationSigned(delta),
                    });
                } else {
                    log.err("replica={} system time is {} behind, clamping system time to cluster time", .{
                        self.replica,
                        fmtDurationSigned(delta),
                    });
                }
            } else {
                const delta = system - upper;
                if (delta < std.time.ns_per_ms) {
                    log.info("replica={} system time is {} ahead", .{
                        self.replica,
                        fmtDurationSigned(delta),
                    });
                } else {
                    log.err("replica={} system time is {} ahead, clamping system time to cluster time", .{
                        self.replica,
                        fmtDurationSigned(delta),
                    });
                }
            }
        }

        fn window_tuples(self: *Self, tolerance: u64) []Marzullo.Tuple {
            assert(self.window.sources[self.replica].?.clock_offset == 0);
            assert(self.window.sources[self.replica].?.one_way_delay == 0);
            var count: usize = 0;
            for (self.window.sources) |sampled, source| {
                if (sampled) |sample| {
                    self.marzullo_tuples[count] = Marzullo.Tuple{
                        .source = @intCast(u8, source),
                        .offset = sample.clock_offset - @intCast(i64, sample.one_way_delay + tolerance),
                        .bound = .lower,
                    };
                    count += 1;
                    self.marzullo_tuples[count] = Marzullo.Tuple{
                        .source = @intCast(u8, source),
                        .offset = sample.clock_offset + @intCast(i64, sample.one_way_delay + tolerance),
                        .bound = .upper,
                    };
                    count += 1;
                }
            }
            return self.marzullo_tuples[0..count];
        }

        fn minimum_one_way_delay(a: ?Sample, b: ?Sample) ?Sample {
            if (a == null) return b;
            if (b == null) return a;
            if (a.?.one_way_delay < b.?.one_way_delay) return a;
            // Choose B if B's one way delay is less or the same (we assume B is the newer sample):
            return b;
        }
    };
}

/// Return a Formatter for a signed number of nanoseconds according to magnitude:
/// [#y][#w][#d][#h][#m]#[.###][n|u|m]s
pub fn fmtDurationSigned(ns: i64) std.fmt.Formatter(formatDurationSigned) {
    return .{ .data = ns };
}

fn formatDurationSigned(
    ns: i64,
    comptime fmt: []const u8,
    options: std.fmt.FormatOptions,
    writer: anytype,
) !void {
    if (ns < 0) {
        try writer.print("-{}", .{std.fmt.fmtDuration(@intCast(u64, -ns))});
    } else {
        try writer.print("{}", .{std.fmt.fmtDuration(@intCast(u64, ns))});
    }
}

const testing = std.testing;
const OffsetType = @import("../time.zig").OffsetType;
const DeterministicTime = @import("../time.zig").DeterministicTime;
const DeterministicClock = Clock(DeterministicTime);

const ClockUnitTestContainer = struct {
    const Self = @This();
    clock: DeterministicClock,
    rtt: u64 = 300 * std.time.ns_per_ms,
    owd: u64 = 150 * std.time.ns_per_ms,
    learn_interval: u64 = 5,

    pub fn init(
        allocator: *std.mem.Allocator,
        offset_type: OffsetType,
        offset_coefficient_A: i64,
        offset_coefficient_B: u64,
    ) !Self {
        const time: DeterministicTime = .{
            .resolution = std.time.ns_per_s / 2,
            .offset_type = offset_type,
            .offset_coefficient_A = offset_coefficient_A,
            .offset_coefficient_B = offset_coefficient_B,
        };
        const self: Self = .{
            .clock = try DeterministicClock.init(allocator, 3, 0, time),
        };
        return self;
    }

    pub fn run_till_tick(self: *Self, tick: u64) void {
        while (self.clock.time.ticks < tick) {
            self.clock.time.tick();

            if (@mod(self.clock.time.ticks, self.learn_interval) == 0) {
                const on_pong_time = self.clock.monotonic();
                const m0 = on_pong_time - self.rtt;
                const t1 = @intCast(i64, on_pong_time - self.owd);

                self.clock.learn(1, m0, t1, on_pong_time);
                self.clock.learn(2, m0, t1, on_pong_time);
            }

            self.clock.synchronize();
        }
    }

    const AssertionPoint = struct {
        tick: u64,
        expected_offset: i64,
    };
    pub fn ticks_to_perform_assertions(self: *Self) [3]AssertionPoint {
        var ret: [3]AssertionPoint = undefined;
        switch (self.clock.time.offset_type) {
            .linear => {
                // For the first (OWD/drift per tick) ticks, the offset < OWD. This means that the
                // Marzullo interval is [0,0] (the offset and OWD are 0 for a replica w.r.t. itself).
                // Therefore the offset of `clock.realtime_synchronised` will be the analytically prescribed
                // offset at the start of the window.
                // Beyond this, the offset > OWD and the Marzullo interval will be from replica 1 and
                // replica 2. The `clock.realtime_synchronized` will be clamped to the lower bound.
                // Therefore the `clock.realtime_synchronized` will be offset by the OWD.
                var threshold = self.owd / @intCast(u64, self.clock.time.offset_coefficient_A);
                ret[0] = .{
                    .tick = threshold,
                    .expected_offset = self.clock.time.offset(threshold - self.learn_interval),
                };
                ret[1] = .{
                    .tick = threshold + 100,
                    .expected_offset = @intCast(i64, self.owd),
                };
                ret[2] = .{
                    .tick = threshold + 200,
                    .expected_offset = @intCast(i64, self.owd),
                };
            },
            .periodic => {
                ret[0] = .{
                    .tick = self.clock.time.offset_coefficient_B / 4,
                    .expected_offset = @intCast(i64, self.owd),
                };
                ret[1] = .{
                    .tick = self.clock.time.offset_coefficient_B / 2,
                    .expected_offset = 0,
                };
                ret[2] = .{
                    .tick = self.clock.time.offset_coefficient_B * 3 / 4,
                    .expected_offset = -@intCast(i64, self.owd),
                };
            },
            .step => {
                ret[0] = .{
                    .tick = self.clock.time.offset_coefficient_B - 10,
                    .expected_offset = 0,
                };
                ret[1] = .{
                    .tick = self.clock.time.offset_coefficient_B + 10,
                    .expected_offset = -@intCast(i64, self.owd),
                };
                ret[2] = .{
                    .tick = self.clock.time.offset_coefficient_B + 10,
                    .expected_offset = -@intCast(i64, self.owd),
                };
            },
        }

        return ret;
    }
};

test "ideal clocks get clamped to cluster time" {
    std.testing.log_level = .crit;
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = &arena.allocator;

    var ideal_constant_drift_clock = try ClockUnitTestContainer.init(
        allocator,
        OffsetType.linear,
        std.time.ns_per_ms, // loses 1ms per tick
        0,
    );
    var linear_clock_assertion_points = ideal_constant_drift_clock.ticks_to_perform_assertions();
    for (linear_clock_assertion_points) |point| {
        ideal_constant_drift_clock.run_till_tick(point.tick);
        try testing.expectEqual(
            point.expected_offset,
            @intCast(i64, ideal_constant_drift_clock.clock.monotonic()) -
                ideal_constant_drift_clock.clock.realtime_synchronized().?,
        );
    }

    var ideal_periodic_drift_clock = try ClockUnitTestContainer.init(
        allocator,
        OffsetType.periodic,
        std.time.ns_per_s, // loses up to 1s
        200, // period of 200 ticks
    );
    var ideal_periodic_drift_clock_assertion_points =
        ideal_periodic_drift_clock.ticks_to_perform_assertions();
    for (ideal_periodic_drift_clock_assertion_points) |point| {
        ideal_periodic_drift_clock.run_till_tick(point.tick);
        try testing.expectEqual(
            point.expected_offset,
            @intCast(i64, ideal_periodic_drift_clock.clock.monotonic()) -
                ideal_periodic_drift_clock.clock.realtime_synchronized().?,
        );
    }

    var ideal_jumping_clock = try ClockUnitTestContainer.init(
        allocator,
        OffsetType.step,
        -5 * std.time.ns_per_day, // jumps 5 days ahead.
        49, // after 49 ticks
    );
    var ideal_jumping_clock_assertion_points = ideal_jumping_clock.ticks_to_perform_assertions();
    for (ideal_jumping_clock_assertion_points) |point| {
        ideal_jumping_clock.run_till_tick(point.tick);
        try testing.expectEqual(
            point.expected_offset,
            @intCast(i64, ideal_jumping_clock.clock.monotonic()) -
                ideal_jumping_clock.clock.realtime_synchronized().?,
        );
    }
}

const MockNetworkOptions = @import("../mock_network.zig").MockNetworkOptions;
const MockNetwork = @import("../mock_network.zig").MockNetwork;
const ClockSimulator = struct {
    const Packet = struct {
        m0: u64,
        t1: ?i64,
        clock_simulator: *ClockSimulator,
    };

    const Options = struct {
        ping_timeout: u32,
        clock_count: u8,
        network_options: MockNetworkOptions,
    };

    allocator: *std.mem.Allocator,
    options: Options,
    ticks: u64 = 0,
    network: MockNetwork(Packet),
    clocks: []DeterministicClock,

    pub fn init(allocator: *std.mem.Allocator, options: Options) !ClockSimulator {
        var self = ClockSimulator{
            .allocator = allocator,
            .options = options,
            .network = try MockNetwork(Packet).init(allocator, options.network_options),
            .clocks = try allocator.alloc(DeterministicClock, options.clock_count),
        };

        for (self.clocks) |*clock, index| {
            clock.* = try self.create_clock(@intCast(u8, index));
        }

        return self;
    }

    fn create_clock(self: *ClockSimulator, replica: u8) !DeterministicClock {
        const time: DeterministicTime = .{
            .resolution = std.time.ns_per_s / 2, // delta_t = 0.5s
            .offset_type = OffsetType.periodic,
            .offset_coefficient_A = std.time.ns_per_s, // loses up to 1s.
            .offset_coefficient_B = 200, // cycles every 200 ticks
        };

        return try DeterministicClock.init(self.allocator, self.options.clock_count, replica, time);
    }

    pub fn tick(self: *ClockSimulator) void {
        self.ticks += 1;
        self.network.tick();
        for (self.clocks) |*clock| {
            clock.tick();
        }

        for (self.clocks) |*clock| {
            if (clock.time.ticks % self.options.ping_timeout == 0) {
                const m0 = clock.monotonic();
                for (self.clocks) |_, target| {
                    if (target != clock.replica) {
                        self.network.submit_packet(
                            .{
                                .m0 = m0,
                                .t1 = null,
                                .clock_simulator = self,
                            },
                            ClockSimulator.handle_packet,
                            @intCast(u8, target),
                            clock.replica,
                            .forward,
                        );
                    }
                }
            }
        }
    }

    fn handle_packet(packet: Packet, to: u8, from: u8) void {
        const self = packet.clock_simulator;
        const target = &self.clocks[to];

        if (packet.t1) |t1| {
            target.learn(
                from,
                packet.m0,
                t1,
                target.monotonic(),
            );
        } else {
            self.network.submit_packet(
                .{
                    .m0 = packet.m0,
                    .t1 = target.realtime(),
                    .clock_simulator = self,
                },
                ClockSimulator.handle_packet,
                from, // send the packet back to where it came from.
                to,
                .reverse,
            );
        }
    }
};

test "fuzz test" {
    std.testing.log_level = .emerg; // silence all clock logs
    var arena_allocator = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena_allocator.deinit();
    const allocator = &arena_allocator.allocator;
    const ticks_max: u64 = 1_000_000;
    const clock_count: u8 = 3;
    const SystemTime = @import("../time.zig").Time;
    var system_time = SystemTime{};
    var prng_seed = @intCast(u64, system_time.realtime());

    var simulator = try ClockSimulator.init(allocator, .{
        .network_options = .{
            .node_count = clock_count,
            .prng_seed = prng_seed,
            .forward_delay_mean = 50,
            .min_forward_delay = 5,
            .reverse_delay_mean = 25,
            .min_reverse_delay = 10,
            .packet_loss_probability = 10,
            .path_maximum_capacity = 20,
            .path_clog_duration_mean = 200,
            .path_clog_probability = 2,
            .packet_replay_probability = 2,
        },
        .clock_count = clock_count,
        .ping_timeout = 20,
    });

    while (simulator.ticks < ticks_max) {
        simulator.tick();
    }

    var test_delta_time: u64 = std.time.ns_per_s / 2;
    var sync_errors: [clock_count]?i64 = undefined;
    var test_time: u64 = ticks_max * test_delta_time;
    var minimum_sync_error: ?u64 = null;
    var index: u8 = 0;

    std.debug.print("prng seed={} max ticks={}\n", .{ prng_seed, ticks_max });

    while (index < clock_count) : (index += 1) {
        const clock = &simulator.clocks[index];
        if (clock.realtime_synchronized()) |synced_time| {
            var err: i64 = @intCast(i64, test_time) - synced_time;
            var absolute_error: u64 = if (err < 0) @intCast(u64, -err) else @intCast(u64, err);
            minimum_sync_error = if (minimum_sync_error == null or
                minimum_sync_error.? > absolute_error) absolute_error else minimum_sync_error;

            std.debug.print("clock={} is {} behind\n", .{ index, fmtDurationSigned(err) });
        } else {
            std.debug.print("clock={} failed to synchronize.\n", .{index});
        }
    }

    if (minimum_sync_error) |err| {
        std.debug.print(
            "minimum absolute synchronization error={}\n",
            .{fmtDurationSigned(@intCast(i64, err))},
        );
    } else {
        std.debug.print("All clocks failed to synchronize.\n", .{});
    }
}
