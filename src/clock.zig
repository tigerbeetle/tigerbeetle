const std = @import("std");
const assert = std.debug.assert;

const Marzullo = @import("marzullo.zig").Marzullo;

const Sample = struct {
    clock_offset: i64,
    one_way_delay: u64,
};

pub const Clock = struct {
    allocator: *std.mem.Allocator,
    replica: u8,
    sources: []?Sample,
    clock_offset_max: u64,
    window_duration_min: u64,
    window_duration_max: u64,
    window_epoch_monotonic: u64,
    window_epoch_realtime: i64,
    marzullo_tuples: []Marzullo.Tuple,
    epoch_monotonic: ?u64 = null,
    epoch_realtime: ?i64 = null,
    epoch_clock_offset: ?i64 = null,

    pub fn init(
        allocator: *std.mem.Allocator,
        replica_count: u8,
        replica: u8,
    ) !Clock {
        assert(replica_count > 0);
        assert(replica < replica_count);

        var sources = try allocator.alloc(?Sample, replica_count);
        errdefer allocator.free(sources);

        // There are two Marzullo tuple bounds (lower and upper) per source clock offset sample:
        var marzullo_tuples = try allocator.alloc(Marzullo.Tuple, sources.len * 2);
        errdefer allocator.free(marzullo_tuples);

        var self = Clock{
            .allocator = allocator,
            .replica = replica,
            .sources = sources,
            .clock_offset_max = 10 * std.time.ns_per_s,
            .window_duration_min = 2 * std.time.ns_per_s,
            .window_duration_max = 8 * std.time.ns_per_s,
            .window_epoch_monotonic = undefined,
            .window_epoch_realtime = undefined,
            .marzullo_tuples = marzullo_tuples,
        };
        self.open_new_window();
        return self;
    }

    /// Called by `Replica.on_pong()` with:
    /// * the index of the `replica` we pinged that has now ponged back,
    /// * our monotonic timestamp `m0` embedded in the ping we sent, copied over to the pong reply,
    /// * the remote replica's `realtime()` timestamp `t1`, and
    /// * our monotonic timestamp `m2` as captured by the `Replica.on_pong()` handler.
    pub fn learn_from_roundtrip(self: *Clock, replica: u8, m0: u64, t1: i64, m2: u64) void {
        assert(replica != self.replica);

        if (m0 >= m2) return;
        // TODO We may receive delayed packets after a reboot, in which case m0/m2 will be invalid.
        // We should add a 128-bit identifier to ping/pongs to bind them to the current window.
        if (m0 < self.window_epoch_monotonic) return;
        if (m2 < self.window_epoch_monotonic) return;
        const window_elapsed: u64 = m2 - self.window_epoch_monotonic;
        if (window_elapsed > self.window_duration_max) return;

        const round_trip_time: u64 = m2 - m0;
        const one_way_delay: u64 = round_trip_time / 2;
        const t2: i64 = self.window_epoch_realtime + @intCast(i64, window_elapsed);
        const clock_offset: i64 = t1 + @intCast(i64, one_way_delay) - t2;

        // TODO Correct asymmetric error when we can see it's there.
        // "A System for Clock Synchronization in an Internet of Things"

        self.sources[replica] = choose_minimum_one_way_delay(self.sources[replica], Sample{
            .clock_offset = clock_offset,
            .one_way_delay = one_way_delay,
        });

        self.close_window();
    }

    /// Called by `Replica.on_ping_timeout()` to provide `m0` when a replica decides to send a ping.
    /// Called by `Replica.on_pong()` to provide `m2` when the replica receives the pong.
    pub fn monotonic(self: *Clock) u64 {
        return monotonic_timestamp();
    }

    /// Called by `Replica.on_ping()` when responding to a ping with a pong.
    /// We use synchronized time if possible, so that the cluster remembers true time as a whole.
    /// We fall back to our wall clock if we do not have synchronized time.
    pub fn realtime(self: *Clock) i64 {
        if (self.synchronized()) |realtime| {
            return realtime;
        } else {
            return realtime_timestamp();
        }
    }

    /// Called by `StateMachine.prepare_timestamp()` when the leader wants to timestamp a batch.
    /// If the leader's clock is not synchronized with the cluster, it will wait until it is.
    pub fn realtime_synchronized(self: *Clock) ?i64 {
        if (self.epoch_clock_offset) |clock_offset| {
            // TODO We can alternatively compare our estimated time with NTP and prefer NTP
            // provided it is within our synchronized bounds.
            // This means that installations with more accurate time sources will use those, and we
            // have guardrails and defense in depth if those time sources fail.
            const epoch_elapsed = monotonic_timestamp() - self.epoch_monotonic.?;
            return self.epoch_realtime.? + clock_offset + @intCast(i64, epoch_elapsed);
        } else {
            return null;
        }
    }

    pub fn tick(self: *Clock) void {
        self.close_window();
    }

    /// Round trip delay is the biggest component of estimation error, so we choose the smallest.
    fn choose_minimum_one_way_delay(a: ?Sample, b: ?Sample) ?Sample {
        if (a == null) return b;
        if (b == null) return a;
        if (a.?.one_way_delay < b.?.one_way_delay) return a;
        // Choose B if B's one way delay is less or the same (we assume B is the newer sample):
        return b;
    }

    fn close_window(self: *Clock) void {
        const window_elapsed = monotonic_timestamp() - self.window_epoch_monotonic;
        if (window_elapsed < self.window_duration_min) return;
        if (window_elapsed >= self.window_duration_max) {
            // TODO Warn that our window expired.
            self.open_new_window();
            return;
        }

        if (self.synchronize_window()) |interval| {
            assert(interval.sources_true > @divTrunc(self.sources.len, 2));

            self.epoch_monotonic = self.window_epoch_monotonic;
            self.epoch_realtime = self.window_epoch_realtime;
            self.epoch_clock_offset = interval.lower_bound;

            // TODO Add debug logs.
            self.open_new_window();
        }
    }

    fn open_new_window(self: *Clock) void {
        std.mem.set(?Sample, self.sources, null);
        self.sources[self.replica] = Sample{
            .clock_offset = 0,
            .one_way_delay = 0,
        };
        self.window_epoch_monotonic = monotonic_timestamp();
        self.window_epoch_realtime = realtime_timestamp();
        std.mem.set(Marzullo.Tuple, self.marzullo_tuples, undefined);
    }

    fn synchronize_window(self: *Clock) ?Marzullo.Interval {
        const window_elapsed = monotonic_timestamp() - self.window_epoch_monotonic;
        if (window_elapsed < self.window_duration_min) return null;

        // Starting with the most clock offset tolerance, while we have a majority, find the best
        // smallest interval with the least clock offset tolerance, reducing tolerance at each step:
        var best: ?Marzullo.Interval = null;

        var tolerance = self.clock_offset_max;
        while (tolerance > 0) : (tolerance /= 16) {
            var interval = Marzullo.smallest_interval(self.window_tuples(tolerance));
            const majority = interval.sources_true > @divTrunc(self.sources.len, 2);
            if (!majority) break;
            best = interval;
        }

        return best;
    }

    fn window_tuples(self: *Clock, clock_offset_max: u64) []Marzullo.Tuple {
        assert(self.sources[self.replica].?.clock_offset == 0);
        assert(self.sources[self.replica].?.one_way_delay == 0);
        var count: usize = 0;
        for (self.sources) |sampled, source| {
            if (sampled) |sample| {
                const range = @intCast(i64, sample.one_way_delay) + @intCast(i64, clock_offset_max);
                self.marzullo_tuples[count] = Marzullo.Tuple{
                    .source = @intCast(u8, source),
                    .offset = sample.clock_offset - range,
                    .bound = .lower,
                };
                count += 1;
                self.marzullo_tuples[count] = Marzullo.Tuple{
                    .source = @intCast(u8, source),
                    .offset = sample.clock_offset + range,
                    .bound = .upper,
                };
                count += 1;
            }
        }
        return self.marzullo_tuples[0..count];
    }
};

// TODO Each Clock instance should accept a Time instance that can be ticked deterministically for
// testing purposes, and which will expose the following two timestamps:
// This is the same pattern we use for Journal (all the logic) and Storage (the raw read/write).

/// A timestamp to measure elapsed time, meaningful only on the same system, not across reboots.
/// Always use a monotonic timestamp if the goal is to measure elapsed time.
/// This clock is not affected by discontinuous jumps in the system time, for example if the
/// system administrator manually changes the clock.
fn monotonic_timestamp() u64 {
    // The true monotonic clock on Linux is not in fact CLOCK_MONOTONIC:
    // CLOCK_MONOTONIC excludes elapsed time while the system is suspended (e.g. VM migration).
    // CLOCK_BOOTTIME is the same as CLOCK_MONOTONIC but includes elapsed time during a suspend.
    // For more detail and why CLOCK_MONOTONIC_RAW is even worse than CLOCK_MONOTONIC,
    // see https://github.com/ziglang/zig/pull/933#discussion_r656021295.
    var ts: std.os.timespec = undefined;
    std.os.clock_gettime(std.os.CLOCK_BOOTTIME, &ts) catch unreachable;
    const m = @intCast(u64, ts.tv_sec) * @as(u64, std.time.ns_per_s) + @intCast(u64, ts.tv_nsec);
    assert(m >= monotonic_timestamp_guard);
    monotonic_timestamp_guard = m;
    return m;
}

/// Hardware and/or software bugs can mean that the monotonic clock may regress.
/// One example (of many): https://bugzilla.redhat.com/show_bug.cgi?id=448449
var monotonic_timestamp_guard: u64 = 0;

/// A timestamp to measure real (i.e. wall clock) time, meaningful across systems, and reboots.
/// This clock is affected by discontinuous jumps in the system time.
fn realtime_timestamp() i64 {
    var ts: std.os.timespec = undefined;
    std.os.clock_gettime(std.os.CLOCK_REALTIME, &ts) catch unreachable;
    return @intCast(i64, ts.tv_sec) * @as(i64, std.time.ns_per_s) + @intCast(i64, ts.tv_nsec);
}
