const std = @import("std");
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const IO = @import("../io.zig").IO;
const StatsD = @import("statsd.zig").StatsD;

const EventTiming = @import("event.zig").EventTiming;
const EventMetric = @import("event.zig").EventMetric;
const EventTimingAggregate = @import("event.zig").EventTimingAggregate;
const EventMetricAggregate = @import("event.zig").EventMetricAggregate;

pub const Metrics = struct {
    pub const StatsDOptions = union(enum) {
        log,
        udp: struct {
            io: *IO,
            address: std.net.Address,
        },
    };

    events_metric: []?EventMetricAggregate,
    events_timing: []?EventTimingAggregate,

    statsd: StatsD,

    pub fn init(allocator: std.mem.Allocator, options: struct {
        cluster: u128,
        replica: u8,
        statsd: StatsDOptions,
    }) !Metrics {
        const events_metric = try allocator.alloc(?EventMetricAggregate, EventMetric.slot_count);
        errdefer allocator.free(events_metric);

        @memset(events_metric, null);

        const events_timing = try allocator.alloc(?EventTimingAggregate, EventTiming.slot_count);
        errdefer allocator.free(events_timing);

        @memset(events_timing, null);

        const statsd = try switch (options.statsd) {
            .log => StatsD.init_log(allocator, options.cluster, options.replica),
            .udp => |statsd_options| StatsD.init_udp(
                allocator,
                options.cluster,
                options.replica,
                statsd_options.io,
                statsd_options.address,
            ),
        };
        errdefer statsd.deinit(allocator);

        return .{
            .events_metric = events_metric,
            .events_timing = events_timing,
            .statsd = statsd,
        };
    }

    pub fn deinit(self: *Metrics, allocator: std.mem.Allocator) void {
        self.statsd.deinit(allocator);
        allocator.free(self.events_timing);
        allocator.free(self.events_metric);

        self.* = undefined;
    }

    /// Gauges work on a last-set wins. Multiple calls to .record_gauge() followed by an emit will
    /// result in only the last value being submitted.
    pub fn record_gauge(self: *Metrics, event_metric: EventMetric, value: u64) void {
        const timing_slot = event_metric.slot();
        self.events_metric[timing_slot] = .{
            .event = event_metric,
            .value = value,
        };
    }

    // Timing works by storing the min, max, sum and count of each value provided. The avg is
    // calculated from sum and count at emit time.
    //
    // When these are emitted upstream (via statsd, currently), upstream must apply different
    // aggregations:
    // * min/max/avg are considered gauges for aggregation: last value wins.
    // * sum/count are considered counters for aggregation: they are added to the existing values.
    //
    // This matches the default behavior of the `g` and `c` statsd types respectively.
    pub fn record_timing(self: *Metrics, event_timing: EventTiming, duration_us: u64) void {
        const timing_slot = event_timing.slot();

        if (self.events_timing[timing_slot]) |*event_timing_existing| {
            const timing_existing = event_timing_existing.values;
            // Certain high cardinality data (eg, op) _can_ differ.
            // TODO: Maybe assert and gate on constants.verify

            if (constants.verify) {
                assert(std.meta.eql(event_timing_existing.event, event_timing));
            }

            event_timing_existing.values = .{
                .duration_min_us = @min(timing_existing.duration_min_us, duration_us),
                .duration_max_us = @max(timing_existing.duration_max_us, duration_us),
                .duration_sum_us = timing_existing.duration_sum_us +| duration_us,
                .count = timing_existing.count +| 1,
            };
        } else {
            self.events_timing[timing_slot] = .{
                .event = event_timing,
                .values = .{
                    .duration_min_us = duration_us,
                    .duration_max_us = duration_us,
                    .duration_sum_us = duration_us,
                    .count = 1,
                },
            };
        }
    }

    pub fn emit(self: *Metrics) void {
        self.statsd.emit(self.events_metric, self.events_timing);

        // For statsd, the right thing is to reset metrics between emitting. For something like
        // Prometheus, this would have to be removed.
        @memset(self.events_metric, null);
        @memset(self.events_timing, null);
    }
};

test "timing overflow" {
    var metrics = try Metrics.init(std.testing.allocator, .{
        .statsd = .log,
        .cluster = 0,
        .replica = 0,
    });
    defer metrics.deinit(std.testing.allocator);

    const event: EventTiming = .replica_aof_write;
    const value = std.math.maxInt(u64) - 1;
    metrics.record_timing(event, value);
    metrics.record_timing(event, value);

    const aggregate = metrics.events_timing[event.slot()].?;

    assert(aggregate.values.count == 2);
    assert(aggregate.values.duration_min_us == value);
    assert(aggregate.values.duration_max_us == value);
    assert(aggregate.values.duration_sum_us == std.math.maxInt(u64));
}
