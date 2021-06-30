const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.clock);

const config = @import("../config.zig");

const clock_offset_tolerance_max: u64 = config.clock_offset_tolerance_max_ms * std.time.ns_per_ms;
const epoch_max: u64 = config.clock_epoch_max_ms * std.time.ns_per_ms;
const window_min: u64 = config.clock_synchronization_window_min_ms * std.time.ns_per_ms;
const window_max: u64 = config.clock_synchronization_window_max_ms * std.time.ns_per_ms;

const Marzullo = @import("../marzullo.zig").Marzullo;
const Time = @import("../time.zig").Time;

const Sample = struct {
    /// The relative difference between our wall clock reading and that of the remote clock source.
    clock_offset: i64,
    one_way_delay: u64,
};

const Epoch = struct {
    const Self = @This();

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

    fn elapsed(self: *Self, clock: *Clock) u64 {
        return clock.monotonic() - self.monotonic;
    }

    fn reset(self: *Self, clock: *Clock) void {
        std.mem.set(?Sample, self.sources, null);
        // A replica always has zero clock offset and network delay to its own system time reading:
        self.sources[clock.replica] = Sample{
            .clock_offset = 0,
            .one_way_delay = 0,
        };
        self.samples = 1;
        self.monotonic = clock.monotonic();
        self.realtime = clock.realtime();
        self.synchronized = null;
        self.learned = false;
    }

    fn sources_sampled(self: *Self) usize {
        var count: usize = 0;
        for (self.sources) |sampled| {
            if (sampled != null) count += 1;
        }
        return count;
    }
};

pub const Clock = struct {
    const Self = @This();

    allocator: *std.mem.Allocator,

    /// The index of the replica using this clock to provide synchronized time.
    replica: u8,

    /// The underlying time source for this clock (system time or deterministic time).
    time: *Time,

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
        time: *Time,
    ) !Clock {
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

        var self = Clock{
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

        log.debug("learn: replica={} m0={} t1={} m2={} t2={} one_way_delay={} " ++
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

        log.info("synchronized: truechimers={}/{} clock_offset={}..{} accuracy={}", .{
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

        if (system == cluster) {
            log.info("system time is within cluster time", .{});
        } else if (system < lower) {
            const delta = lower - system;
            if (delta < std.time.ns_per_ms) {
                log.info("system time is {} behind", .{fmtDurationSigned(delta)});
            } else {
                log.err("system time is {} behind, clamping system time to cluster time", .{
                    fmtDurationSigned(delta),
                });
            }
        } else {
            const delta = system - upper;
            if (delta < std.time.ns_per_ms) {
                log.info("system time is {} ahead", .{fmtDurationSigned(delta)});
            } else {
                log.err("system time is {} ahead, clamping system time to cluster time", .{
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

// TODO Use tracing analysis to test a simulated trace, comparing against known values for accuracy.
