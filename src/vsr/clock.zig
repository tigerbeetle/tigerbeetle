const std = @import("std");
const assert = std.debug.assert;
const fmt = std.fmt;

const log = @import("../stdx.zig").log.scoped(.clock);
const constants = @import("../constants.zig");

const clock_offset_tolerance_max: u64 = constants.clock_offset_tolerance_max_ms * std.time.ns_per_ms;
const epoch_max: u64 = constants.clock_epoch_max_ms * std.time.ns_per_ms;
const window_min: u64 = constants.clock_synchronization_window_min_ms * std.time.ns_per_ms;
const window_max: u64 = constants.clock_synchronization_window_max_ms * std.time.ns_per_ms;

const Marzullo = @import("marzullo.zig").Marzullo;

pub fn ClockType(comptime Time: type) type {
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
                @memset(epoch.sources, null);
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

        /// The index of the replica using this clock to provide synchronized time.
        replica: u8,

        /// The underlying time source for this clock (system time or deterministic time).
        time: *Time,

        /// An epoch from which the clock can read synchronized clock timestamps within safe bounds.
        /// At least `constants.clock_synchronization_window_min_ms` is needed for this to be ready
        /// to use.
        epoch: Epoch,

        /// The next epoch (collecting samples and being synchronized) to replace the current epoch.
        window: Epoch,

        /// A static allocation to convert window samples into tuple bounds for Marzullo's algorithm.
        marzullo_tuples: []Marzullo.Tuple,

        /// A kill switch to revert to unsynchronized realtime.
        synchronization_disabled: bool,

        pub fn init(
            allocator: std.mem.Allocator,
            /// The size of the cluster, i.e. the number of clock sources (including this replica).
            replica_count: u8,
            replica: u8,
            time: *Time,
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

        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            allocator.free(self.epoch.sources);
            allocator.free(self.window.sources);
            allocator.free(self.marzullo_tuples);
        }

        /// Called by `Replica.on_pong()` with:
        /// * the index of the `replica` that has replied to our ping with a pong,
        /// * our monotonic timestamp `m0` embedded in the ping we sent, carried over into this pong,
        /// * the remote replica's `realtime()` timestamp `t1`, and
        /// * our monotonic timestamp `m2` as captured by our `Replica.on_pong()` handler.
        pub fn learn(self: *Self, replica: u8, m0: u64, t1: i64, m2: u64) void {
            assert(replica != self.replica);

            if (self.synchronization_disabled) return;

            // Our m0 and m2 readings should always be monotonically increasing if not equal.
            // Crucially, it is possible for a very fast network to have m0 == m2, especially where
            // `constants.tick_ms` is at a more course granularity. We must therefore tolerate RTT=0
            // or otherwise we would have a liveness bug simply because we would be throwing away
            // perfectly good clock samples.
            // This condition should never be true. Reject this as a bad sample:
            if (m0 > m2) {
                log.warn("{}: learn: m0={} > m2={}", .{ self.replica, m0, m2 });
                return;
            }

            // We may receive delayed packets after a reboot, in which case m0/m2 may be invalid:
            if (m0 < self.window.monotonic) {
                log.warn("{}: learn: m0={} < window.monotonic={}", .{
                    self.replica,
                    m0,
                    self.window.monotonic,
                });
                return;
            }

            if (m2 < self.window.monotonic) {
                log.warn("{}: learn: m2={} < window.monotonic={}", .{
                    self.replica,
                    m2,
                    self.window.monotonic,
                });
                return;
            }

            const elapsed: u64 = m2 - self.window.monotonic;
            if (elapsed > window_max) {
                log.warn("{}: learn: elapsed={} > window_max={}", .{
                    self.replica,
                    elapsed,
                    window_max,
                });
                return;
            }

            const round_trip_time: u64 = m2 - m0;
            const one_way_delay: u64 = round_trip_time / 2;
            const t2: i64 = self.window.realtime + @as(i64, @intCast(elapsed));
            const clock_offset: i64 = t1 + @as(i64, @intCast(one_way_delay)) - t2;
            const asymmetric_delay = self.estimate_asymmetric_delay(
                replica,
                one_way_delay,
                clock_offset,
            );
            const clock_offset_corrected = clock_offset + asymmetric_delay;

            log.debug("{}: learn: replica={} m0={} t1={} m2={} t2={} one_way_delay={} " ++
                "asymmetric_delay={} clock_offset={}", .{
                self.replica,
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
            self.window.sources[replica] = minimum_one_way_delay(
                self.window.sources[replica],
                Sample{
                    .clock_offset = clock_offset_corrected,
                    .one_way_delay = one_way_delay,
                },
            );

            self.window.samples += 1;

            // We decouple calls to `synchronize()` so that it's not triggered by these network events.
            // Otherwise, excessive duplicate network packets would burn the CPU.
            self.window.learned = true;
        }

        /// Called by `Replica.on_ping_timeout()` to provide `m0` when we decide to send a ping.
        /// Called by `Replica.on_pong()` to provide `m2` when we receive a pong.
        /// Called by `Replica.on_commit_message_timeout()` to allow backups to discard
        //  duplicate/misdirected heartbeats.
        pub fn monotonic(self: *Self) u64 {
            return self.time.monotonic();
        }

        /// Called by `Replica.on_ping()` when responding to a ping with a pong.
        /// This should never be used by the state machine, only for measuring clock offsets.
        pub fn realtime(self: *Self) i64 {
            return self.time.realtime();
        }

        /// Called by `StateMachine.prepare_timestamp()` when the primary wants to timestamp a batch.
        /// If the primary's clock is not synchronized with the cluster, it must wait until it is.
        /// Returns the system time clamped to be within our synchronized lower and upper bounds.
        /// This is complementary to NTP and allows clusters with very accurate time to make use of it,
        /// while providing guard rails for when NTP is partitioned or unable to correct quickly enough.
        pub fn realtime_synchronized(self: *Self) ?i64 {
            if (self.synchronization_disabled) {
                return self.realtime();
            } else if (self.epoch.synchronized) |interval| {
                const elapsed = @as(i64, @intCast(self.epoch.elapsed(self)));
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
            // Gradual clock drift prevents us from using an epoch for more than a few seconds.
            if (self.epoch.elapsed(self) >= epoch_max) {
                log.err(
                    "{}: no agreement on cluster time (partitioned or too many clock faults)",
                    .{self.replica},
                );
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
            // Note that `one_way_delay` may be 0 for very fast networks.

            const error_margin = 10 * std.time.ns_per_ms;

            if (self.epoch.sources[replica]) |epoch| {
                if (one_way_delay <= epoch.one_way_delay) {
                    return 0;
                } else if (clock_offset > epoch.clock_offset + error_margin) {
                    // The asymmetric error is on the forward network path.
                    return 0 - @as(i64, @intCast(one_way_delay - epoch.one_way_delay));
                } else if (clock_offset < epoch.clock_offset - error_margin) {
                    // The asymmetric error is on the reverse network path.
                    return 0 + @as(i64, @intCast(one_way_delay - epoch.one_way_delay));
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
                    log.err("{}: synchronization failed, partitioned (sources={} samples={})", .{
                        self.replica,
                        sources_sampled,
                        self.window.samples,
                    });
                } else {
                    log.err("{}: synchronization failed, no agreement (sources={} samples={})", .{
                        self.replica,
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

            log.debug("{}: synchronized: truechimers={}/{} clock_offset={}..{} accuracy={}", .{
                self.replica,
                new_interval.sources_true,
                self.epoch.sources.len,
                fmt.fmtDurationSigned(new_interval.lower_bound),
                fmt.fmtDurationSigned(new_interval.upper_bound),
                fmt.fmtDurationSigned(new_interval.upper_bound - new_interval.lower_bound),
            });

            const elapsed = @as(i64, @intCast(self.epoch.elapsed(self)));
            const system = self.realtime();
            const lower = self.epoch.realtime + elapsed + new_interval.lower_bound;
            const upper = self.epoch.realtime + elapsed + new_interval.upper_bound;
            const cluster = std.math.clamp(system, lower, upper);

            if (system == cluster) {} else if (system < lower) {
                const delta = lower - system;
                if (delta < std.time.ns_per_ms) {
                    log.debug("{}: system time is {} behind", .{
                        self.replica,
                        fmt.fmtDurationSigned(delta),
                    });
                } else {
                    log.err("{}: system time is {} behind, clamping system time to cluster time", .{
                        self.replica,
                        fmt.fmtDurationSigned(delta),
                    });
                }
            } else {
                const delta = system - upper;
                if (delta < std.time.ns_per_ms) {
                    log.debug("{}: system time is {} ahead", .{
                        self.replica,
                        fmt.fmtDurationSigned(delta),
                    });
                } else {
                    log.err("{}: system time is {} ahead, clamping system time to cluster time", .{
                        self.replica,
                        fmt.fmtDurationSigned(delta),
                    });
                }
            }
        }

        fn window_tuples(self: *Self, tolerance: u64) []Marzullo.Tuple {
            assert(self.window.sources[self.replica].?.clock_offset == 0);
            assert(self.window.sources[self.replica].?.one_way_delay == 0);
            var count: usize = 0;
            for (self.window.sources, 0..) |sampled, source| {
                if (sampled) |sample| {
                    self.marzullo_tuples[count] = Marzullo.Tuple{
                        .source = @as(u8, @intCast(source)),
                        .offset = sample.clock_offset - @as(i64, @intCast(sample.one_way_delay + tolerance)),
                        .bound = .lower,
                    };
                    count += 1;
                    self.marzullo_tuples[count] = Marzullo.Tuple{
                        .source = @as(u8, @intCast(source)),
                        .offset = sample.clock_offset + @as(i64, @intCast(sample.one_way_delay + tolerance)),
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

const testing = std.testing;
const OffsetType = @import("../testing/time.zig").OffsetType;
const DeterministicTime = @import("../testing/time.zig").Time;
const DeterministicClock = ClockType(DeterministicTime);

const ClockUnitTestContainer = struct {
    const Self = @This();
    time: DeterministicTime,
    clock: DeterministicClock,
    rtt: u64 = 300 * std.time.ns_per_ms,
    owd: u64 = 150 * std.time.ns_per_ms,
    learn_interval: u64 = 5,

    pub fn init(
        self: *Self,
        allocator: std.mem.Allocator,
        offset_type: OffsetType,
        offset_coefficient_A: i64,
        offset_coefficient_B: i64,
    ) !void {
        // TODO(Zig) Use @returnAddress() when available.
        self.* = .{
            .time = .{
                .resolution = std.time.ns_per_s / 2,
                .offset_type = offset_type,
                .offset_coefficient_A = offset_coefficient_A,
                .offset_coefficient_B = offset_coefficient_B,
            },
            .clock = try DeterministicClock.init(allocator, 3, 0, &self.time),
        };
    }

    pub fn run_till_tick(self: *Self, tick: u64) void {
        while (self.clock.time.ticks < tick) {
            self.clock.time.tick();

            if (@mod(self.clock.time.ticks, self.learn_interval) == 0) {
                const on_pong_time = self.clock.monotonic();
                const m0 = on_pong_time - self.rtt;
                const t1 = @as(i64, @intCast(on_pong_time - self.owd));

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
                var threshold = self.owd / @as(u64, @intCast(self.clock.time.offset_coefficient_A));
                ret[0] = .{
                    .tick = threshold,
                    .expected_offset = self.clock.time.offset(threshold - self.learn_interval),
                };
                ret[1] = .{
                    .tick = threshold + 100,
                    .expected_offset = @as(i64, @intCast(self.owd)),
                };
                ret[2] = .{
                    .tick = threshold + 200,
                    .expected_offset = @as(i64, @intCast(self.owd)),
                };
            },
            .periodic => {
                ret[0] = .{
                    .tick = @as(u64, @intCast(@divTrunc(self.clock.time.offset_coefficient_B, 4))),
                    .expected_offset = @as(i64, @intCast(self.owd)),
                };
                ret[1] = .{
                    .tick = @as(u64, @intCast(@divTrunc(self.clock.time.offset_coefficient_B, 2))),
                    .expected_offset = 0,
                };
                ret[2] = .{
                    .tick = @as(u64, @intCast(@divTrunc(self.clock.time.offset_coefficient_B * 3, 4))),
                    .expected_offset = -@as(i64, @intCast(self.owd)),
                };
            },
            .step => {
                ret[0] = .{
                    .tick = @as(u64, @intCast(self.clock.time.offset_coefficient_B - 10)),
                    .expected_offset = 0,
                };
                ret[1] = .{
                    .tick = @as(u64, @intCast(self.clock.time.offset_coefficient_B + 10)),
                    .expected_offset = -@as(i64, @intCast(self.owd)),
                };
                ret[2] = .{
                    .tick = @as(u64, @intCast(self.clock.time.offset_coefficient_B + 10)),
                    .expected_offset = -@as(i64, @intCast(self.owd)),
                };
            },
            .non_ideal => unreachable, // use ideal clocks for the unit tests
        }

        return ret;
    }
};

test "ideal clocks get clamped to cluster time" {
    // Silence all clock logs.
    const level = std.testing.log_level;
    std.testing.log_level = std.log.Level.err;
    defer std.testing.log_level = level;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var ideal_constant_drift_clock: ClockUnitTestContainer = undefined;
    try ideal_constant_drift_clock.init(
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
            @as(i64, @intCast(ideal_constant_drift_clock.clock.monotonic())) -
                ideal_constant_drift_clock.clock.realtime_synchronized().?,
        );
    }

    var ideal_periodic_drift_clock: ClockUnitTestContainer = undefined;
    try ideal_periodic_drift_clock.init(
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
            @as(i64, @intCast(ideal_periodic_drift_clock.clock.monotonic())) -
                ideal_periodic_drift_clock.clock.realtime_synchronized().?,
        );
    }

    var ideal_jumping_clock: ClockUnitTestContainer = undefined;
    try ideal_jumping_clock.init(
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
            @as(i64, @intCast(ideal_jumping_clock.clock.monotonic())) -
                ideal_jumping_clock.clock.realtime_synchronized().?,
        );
    }
}

const PacketSimulatorOptions = @import("../testing/packet_simulator.zig").PacketSimulatorOptions;
const PacketSimulatorType = @import("../testing/packet_simulator.zig").PacketSimulatorType;
const Path = @import("../testing/packet_simulator.zig").Path;
const Command = @import("../vsr.zig").Command;
const ClockSimulator = struct {
    const Packet = struct {
        m0: u64,
        t1: ?i64,
        clock_simulator: *ClockSimulator,

        pub fn clone(packet: *const Packet) Packet {
            return packet.*;
        }

        /// PacketSimulator requires this function, but we don't actually have anything to deinit.
        pub fn deinit(packet: *const Packet) void {
            _ = packet;
        }

        pub fn command(_: *const Packet) Command {
            return .ping; // Value doesn't matter.
        }
    };

    const Options = struct {
        ping_timeout: u32,
        clock_count: u8,
        network_options: PacketSimulatorOptions,
    };

    allocator: std.mem.Allocator,
    options: Options,
    ticks: u64 = 0,
    network: PacketSimulatorType(Packet),
    times: []DeterministicTime,
    clocks: []DeterministicClock,
    prng: std.rand.DefaultPrng,

    pub fn init(allocator: std.mem.Allocator, options: Options) !ClockSimulator {
        var network = try PacketSimulatorType(Packet).init(allocator, options.network_options);
        errdefer network.deinit(allocator);

        var times = try allocator.alloc(DeterministicTime, options.clock_count);
        errdefer allocator.free(times);

        var clocks = try allocator.alloc(DeterministicClock, options.clock_count);
        errdefer allocator.free(clocks);

        var prng = std.rand.DefaultPrng.init(options.network_options.seed);

        for (clocks, 0..) |*clock, replica| {
            errdefer for (clocks[0..replica]) |*c| c.deinit(allocator);

            const amplitude = prng.random().intRangeAtMost(i64, -10, 10) * std.time.ns_per_s;
            const phase = prng.random().intRangeAtMost(i64, 100, 1000) +
                @as(i64, @intFromFloat(prng.random().floatNorm(f64) * 50));
            times[replica] = .{
                .resolution = std.time.ns_per_s / 2, // delta_t = 0.5s
                .offset_type = OffsetType.non_ideal,
                .offset_coefficient_A = amplitude,
                .offset_coefficient_B = phase,
                .offset_coefficient_C = 10,
            };

            clock.* = try DeterministicClock.init(
                allocator,
                options.clock_count,
                @as(u8, @intCast(replica)),
                &times[replica],
            );
            errdefer clock.deinit(allocator);
        }
        errdefer for (clocks) |*clock| clock.deinit(allocator);

        return ClockSimulator{
            .allocator = allocator,
            .options = options,
            .network = network,
            .times = times,
            .clocks = clocks,
            .prng = prng,
        };
    }

    pub fn deinit(self: *ClockSimulator) void {
        for (self.clocks) |*clock| clock.deinit(self.allocator);
        self.allocator.free(self.clocks);
        self.allocator.free(self.times);
        self.network.deinit(self.allocator);
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
                for (self.clocks, 0..) |_, target| {
                    if (target != clock.replica) {
                        self.network.submit_packet(
                            .{
                                .m0 = m0,
                                .t1 = null,
                                .clock_simulator = self,
                            },
                            ClockSimulator.handle_packet,
                            .{
                                .source = clock.replica,
                                .target = @as(u8, @intCast(target)),
                            },
                        );
                    }
                }
            }
        }
    }

    fn handle_packet(packet: Packet, path: Path) void {
        const self = packet.clock_simulator;
        const target = &self.clocks[path.target];

        if (packet.t1) |t1| {
            target.learn(
                path.source,
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
                .{
                    // send the packet back to where it came from.
                    .source = path.target,
                    .target = path.source,
                },
            );
        }
    }
};

test "clock: fuzz test" {
    // Silence all clock logs.
    const level = std.testing.log_level;
    std.testing.log_level = std.log.Level.err;
    defer std.testing.log_level = level;

    const ticks_max: u64 = 1_000_000;
    const clock_count: u8 = 3;
    const SystemTime = @import("../testing/time.zig").Time;
    var system_time = SystemTime{
        .resolution = constants.tick_ms * std.time.ns_per_ms,
        .offset_type = .linear,
        .offset_coefficient_A = 0,
        .offset_coefficient_B = 0,
    };
    var seed = @as(u64, @intCast(system_time.realtime()));
    var min_sync_error: u64 = 1_000_000_000;
    var max_sync_error: u64 = 0;
    var max_clock_offset: u64 = 0;
    var min_clock_offset: u64 = 1_000_000_000;
    var simulator = try ClockSimulator.init(std.testing.allocator, .{
        .network_options = .{
            .node_count = 3,
            .client_count = 0,
            .seed = seed,

            .one_way_delay_mean = 25,
            .one_way_delay_min = 10,
            .packet_loss_probability = 10,
            .path_maximum_capacity = 20,
            .path_clog_duration_mean = 200,
            .path_clog_probability = 2,
            .packet_replay_probability = 2,

            .partition_mode = .isolate_single,
            .partition_probability = 25,
            .unpartition_probability = 5,
            .partition_stability = 100,
            .unpartition_stability = 10,
        },
        .clock_count = clock_count,
        .ping_timeout = 20,
    });
    defer simulator.deinit();

    var clock_ticks_without_synchronization = [_]u32{0} ** clock_count;
    while (simulator.ticks < ticks_max) {
        simulator.tick();

        for (simulator.clocks, 0..) |*clock, index| {
            var offset = clock.time.offset(simulator.ticks);
            var abs_offset = if (offset >= 0) @as(u64, @intCast(offset)) else @as(u64, @intCast(-offset));
            max_clock_offset = if (abs_offset > max_clock_offset) abs_offset else max_clock_offset;
            min_clock_offset = if (abs_offset < min_clock_offset) abs_offset else min_clock_offset;

            var synced_time = clock.realtime_synchronized() orelse {
                clock_ticks_without_synchronization[index] += 1;
                continue;
            };

            for (simulator.clocks, 0..) |*other_clock, other_clock_index| {
                if (index == other_clock_index) continue;
                var other_clock_sync_time = other_clock.realtime_synchronized() orelse {
                    continue;
                };
                var err: i64 = synced_time - other_clock_sync_time;
                var abs_err: u64 = if (err >= 0) @as(u64, @intCast(err)) else @as(u64, @intCast(-err));
                max_sync_error = if (abs_err > max_sync_error) abs_err else max_sync_error;
                min_sync_error = if (abs_err < min_sync_error) abs_err else min_sync_error;
            }
        }
    }

    log.info("seed={}, max ticks={}, clock count={}\n", .{
        seed,
        ticks_max,
        clock_count,
    });
    log.info("absolute clock offsets with respect to test time:\n", .{});
    log.info("maximum={}\n", .{fmt.fmtDurationSigned(@as(i64, @intCast(max_clock_offset)))});
    log.info("minimum={}\n", .{fmt.fmtDurationSigned(@as(i64, @intCast(min_clock_offset)))});
    log.info("\nabsolute synchronization errors between clocks:\n", .{});
    log.info("maximum={}\n", .{fmt.fmtDurationSigned(@as(i64, @intCast(max_sync_error)))});
    log.info("minimum={}\n", .{fmt.fmtDurationSigned(@as(i64, @intCast(min_sync_error)))});
    log.info("clock ticks without synchronization={d}\n", .{
        clock_ticks_without_synchronization,
    });
}
