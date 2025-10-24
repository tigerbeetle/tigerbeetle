//! Log IO/CPU event spans for analysis/visualization.
//!
//! Example:
//!
//!     $ ./tigerbeetle start --experimental --trace=trace.json
//!
//! or:
//!
//!     $ ./tigerbeetle benchmark --trace=trace.json
//!
//! The trace JSON output is compatible with:
//! - https://ui.perfetto.dev/
//! - https://gravitymoth.com/spall/spall.html
//! - chrome://tracing/
//!
//! Example integrations:
//!
//!     // Trace a synchronous event.
//!     // The second argument is a `anytype` struct, corresponding to the struct argument to
//!     // `log.debug()`.
//!     tree.grid.trace.start(.{ .compact_mutable = .{ .tree = tree.config.name } });
//!     defer tree.grid.trace.stop(.{ .compact_mutable = .{ .tree = tree.config.name } });
//!
//! Note that only one of each Event can be running at a time:
//!
//!     // good
//!     trace.start(.{.foo = .{}});
//!     trace.stop(.{ .foo = .{} });
//!     trace.start(.{ .bar = .{} });
//!     trace.stop(.{ .bar = .{} });
//!
//!     // good
//!     trace.start(.{ .foo = .{} });
//!     trace.start(.{ .bar = .{} });
//!     trace.stop(.{ .foo = .{} });
//!     trace.stop(.{ .bar = .{} });
//!
//!     // bad
//!     trace.start(.{ .foo = .{} });
//!     trace.start(.{ .foo = .{} });
//!
//!     // bad
//!     trace.stop(.{ .foo = .{} });
//!     trace.start(.{ .foo = .{} });
//!
//! If an event is is cancelled rather than properly stopped, use .reset():
//! - Reset is safe to call regardless of whether the event is currently started.
//! - For events with multiple instances (e.g. IO reads and writes), .reset() will
//!   cancel all running traces of the same event.
//!
//!     // good
//!     trace.start(.{ .foo = .{} });
//!     trace.cancel(.foo);
//!     trace.start(.{ .foo = .{} });
//!     trace.stop(.{ .foo = .{} });
//!
//! Notes:
//! - When enabled, traces are written to stdout (as opposed to logs, which are written to stderr).
//! - The JSON output is a "[" followed by a comma-separated list of JSON objects. The JSON array is
//!   never closed with a "]", but Chrome, Spall, and Perfetto all handle this.
//! - Event pairing (start/stop) is asserted at runtime.
//! - `trace.start()/.stop()/.reset()` will `log.debug()` regardless of whether tracing is enabled.
//!
//! The JSON output looks like:
//!
//!     {
//!         // Process id:
//!         // The replica index is encoded as the "process id" of trace events, so events from
//!         // multiple replicas of a cluster can be unified to visualize them on the same timeline.
//!         "pid": 0,
//!
//!         // Thread id:
//!         "tid": 0,
//!
//!         // Category.
//!         "cat": "replica_commit",
//!
//!         // Phase.
//!         "ph": "B",
//!
//!         // Timestamp:
//!         // Microseconds since program start.
//!         "ts": 934327,
//!
//!         // Event name:
//!         // Includes the event name and a *low cardinality subset* of the second argument to
//!         // `trace.start()`. (Low-cardinality part so that tools like Perfetto can distinguish
//!         // events usefully.)
//!         "name": "replica_commit stage='next_pipeline'",
//!
//!         // Extra event arguments. (Encoded from the second argument to `trace.start()`).
//!         "args": {
//!             "stage": "next_pipeline",
//!             "op": 1
//!         },
//!     },
//!
const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.trace);

const stdx = @import("stdx");
const KiB = stdx.KiB;
const Duration = stdx.Duration;
const IO = @import("io.zig").IO;
const Time = @import("time.zig").Time;
const StatsD = @import("trace/statsd.zig").StatsD;
pub const Event = @import("trace/event.zig").Event;
pub const EventMetric = @import("trace/event.zig").EventMetric;
pub const EventTracing = @import("trace/event.zig").EventTracing;
pub const EventTiming = @import("trace/event.zig").EventTiming;
pub const EventTimingAggregate = @import("trace/event.zig").EventTimingAggregate;
pub const EventMetricAggregate = @import("trace/event.zig").EventMetricAggregate;

const trace_span_size_max = 1 * KiB;

pub const Tracer = @This();

time: Time,
process_id: ProcessID,
options: Options,
buffer: []u8,
statsd: StatsD,

events_started: [EventTracing.stack_count]?stdx.Instant = @splat(null),
events_metric: []?EventMetricAggregate,
events_timing: []?EventTimingAggregate,

time_start: stdx.Instant,

log_trace: bool,

pub const ProcessID = union(enum) {
    unknown,
    replica: struct {
        cluster: u128,
        replica: u8,
    },

    pub fn format(
        self: ProcessID,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;
        try switch (self) {
            .unknown => writer.writeByte('_'),
            .replica => |replica| try writer.print("{d}", .{replica.replica}),
        };
    }

    pub fn json(self: ProcessID) u8 {
        return switch (self) {
            .unknown => 0,
            .replica => |replica| replica.replica,
        };
    }
};

pub const Options = struct {
    /// The tracer still validates start/stop state even when writer=null.
    writer: ?std.io.AnyWriter = null,
    statsd_options: union(enum) {
        log,
        udp: struct {
            io: *IO,
            address: std.net.Address,
        },
    } = .log,
    log_trace: bool = true,
};

pub fn init(
    allocator: std.mem.Allocator,
    time: Time,
    process_id: ProcessID,
    options: Options,
) !Tracer {
    if (options.writer) |writer| {
        try writer.writeAll("[\n");
    }

    const buffer = try allocator.alloc(u8, trace_span_size_max);
    errdefer allocator.free(buffer);

    var statsd = try switch (options.statsd_options) {
        .log => StatsD.init_log(allocator, process_id),
        .udp => |statsd_options| StatsD.init_udp(
            allocator,
            process_id,
            statsd_options.io,
            statsd_options.address,
        ),
    };
    errdefer statsd.deinit(allocator);

    const events_metric =
        try allocator.alloc(?EventMetricAggregate, EventMetric.slot_count);
    errdefer allocator.free(events_metric);
    @memset(events_metric, null);

    const events_timing =
        try allocator.alloc(?EventTimingAggregate, EventTiming.slot_count);
    errdefer allocator.free(events_timing);
    @memset(events_timing, null);

    return .{
        .time = time,
        .process_id = process_id,
        .options = options,
        .buffer = buffer,
        .statsd = statsd,

        .events_metric = events_metric,
        .events_timing = events_timing,

        .time_start = time.monotonic(),

        .log_trace = options.log_trace,
    };
}

pub fn deinit(tracer: *Tracer, allocator: std.mem.Allocator) void {
    allocator.free(tracer.events_timing);
    allocator.free(tracer.events_metric);
    tracer.statsd.deinit(allocator);
    allocator.free(tracer.buffer);
    tracer.* = undefined;
}

/// We learn the cluster id and replica index after opening the datafile.
pub fn set_replica(tracer: *Tracer, options: struct { cluster: u128, replica: u8 }) void {
    const process_id: ProcessID = .{ .replica = .{
        .cluster = options.cluster,
        .replica = options.replica,
    } };
    tracer.process_id = process_id;
    tracer.statsd.process_id = process_id;
}

/// Gauges work on a last-set wins. Multiple calls to .gauge() followed by an emit will / result in
/// only the last value being submitted.
pub fn gauge(tracer: *Tracer, event: EventMetric, value: u64) void {
    const timing_slot = event.slot();
    tracer.events_metric[timing_slot] = .{
        .event = event,
        .value = value,
    };
}

/// Counters are cumulative values that only increase.
pub fn count(tracer: *Tracer, event: EventMetric, value: u64) void {
    const timing_slot = event.slot();
    if (tracer.events_metric[timing_slot]) |*metric| {
        metric.value +|= value;
    } else {
        tracer.events_metric[timing_slot] = .{
            .event = event,
            .value = value,
        };
    }
}

pub fn start(tracer: *Tracer, event: Event) void {
    const event_tracing = event.as(EventTracing);
    const event_timing = event.as(EventTiming);
    const stack = event_tracing.stack();

    const time_now = tracer.time.monotonic();

    assert(tracer.events_started[stack] == null);
    tracer.events_started[stack] = time_now;

    if (tracer.log_trace) {
        log.debug(
            "{}: {s}({}): start: {}",
            .{ tracer.process_id, @tagName(event), event_tracing, event_timing },
        );
    }

    const writer = tracer.options.writer orelse return;
    const time_elapsed = time_now.duration_since(tracer.time_start);

    var buffer_stream = std.io.fixedBufferStream(tracer.buffer);

    // String tid's would be much more useful.
    // They are supported by both Chrome and Perfetto, but rejected by Spall.
    buffer_stream.writer().print("{{" ++
        "\"pid\":{[process_id]}," ++
        "\"tid\":{[thread_id]}," ++
        "\"ph\":\"{[event]c}\"," ++
        "\"ts\":{[timestamp]}," ++
        "\"cat\":\"{[category]s}\"," ++
        "\"name\":\"{[category]s} {[event_tracing]} {[event_timing]}\"," ++
        "\"args\":{[args]s}" ++
        "}},\n", .{
        .process_id = tracer.process_id.json(),
        .thread_id = event_tracing.stack(),
        .category = @tagName(event),
        .event = 'B',
        .timestamp = time_elapsed.to_us(),
        .event_tracing = event_tracing,
        .event_timing = event_timing,
        .args = std.json.Formatter(Event){ .value = event, .options = .{} },
    }) catch {
        log.err("{}: {s}({}): event too large: {}", .{
            tracer.process_id,
            @tagName(event),
            event_tracing,
            event_timing,
        });
        return;
    };

    writer.writeAll(buffer_stream.getWritten()) catch |err| {
        std.debug.panic("Tracer.start: {}\n", .{err});
    };
}

pub fn stop(tracer: *Tracer, event: Event) void {
    const us_log_threshold_ns = 5 * std.time.ns_per_ms;

    const event_tracing = event.as(EventTracing);
    const event_timing = event.as(EventTiming);
    const stack = event_tracing.stack();

    const event_start = tracer.events_started[stack].?;
    const event_end = tracer.time.monotonic();
    const event_duration = event_end.duration_since(event_start);

    assert(tracer.events_started[stack] != null);
    tracer.events_started[stack] = null;

    if (tracer.log_trace) {
        // Double leading space to align with 'start: '.
        log.debug("{}: {s}({}): stop:  {} (duration={}{s})", .{
            tracer.process_id,
            @tagName(event),
            event_tracing,
            event_timing,
            if (event_duration.ns < us_log_threshold_ns)
                event_duration.to_us()
            else
                event_duration.to_ms(),
            if (event_duration.ns < us_log_threshold_ns) "us" else "ms",
        });
    }

    tracer.timing(event_timing, event_duration);

    tracer.write_stop(stack, event_end.duration_since(tracer.time_start));
}

pub fn cancel(tracer: *Tracer, event_tag: Event.Tag) void {
    const stack_base = EventTracing.stack_bases.get(event_tag);
    const cardinality = EventTracing.stack_limits.get(event_tag);
    const event_end = tracer.time.monotonic();
    for (stack_base..stack_base + cardinality) |stack| {
        if (tracer.events_started[stack]) |_| {
            if (tracer.log_trace) {
                log.debug("{}: {s}: cancel", .{ tracer.process_id, @tagName(event_tag) });
            }

            const event_duration = event_end.duration_since(tracer.time_start);

            tracer.events_started[stack] = null;
            tracer.write_stop(@intCast(stack), event_duration);
        }
    }
}

fn write_stop(tracer: *Tracer, stack: u32, time_elapsed: stdx.Duration) void {
    const writer = tracer.options.writer orelse return;
    var buffer_stream = std.io.fixedBufferStream(tracer.buffer);

    buffer_stream.writer().print(
        "{{" ++
            "\"pid\":{[process_id]}," ++
            "\"tid\":{[thread_id]}," ++
            "\"ph\":\"{[event]c}\"," ++
            "\"ts\":{[timestamp]}" ++
            "}},\n",
        .{
            .process_id = tracer.process_id.json(),
            .thread_id = stack,
            .event = 'E',
            .timestamp = time_elapsed.to_us(),
        },
    ) catch unreachable;

    writer.writeAll(buffer_stream.getWritten()) catch |err| {
        std.debug.panic("Tracer.stop: {}\n", .{err});
    };
}

pub fn emit_metrics(tracer: *Tracer) void {
    tracer.start(.metrics_emit);
    defer tracer.stop(.metrics_emit);

    const metrics_statsd_packets = tracer.statsd.emit(
        tracer.events_metric,
        tracer.events_timing,
    ) catch |err| switch (err) {
        error.Busy, error.UnknownProcess => return,
    };

    // For statsd, the right thing is to reset metrics between emitting. For something like
    // Prometheus, this would have to be removed.
    @memset(tracer.events_metric, null);
    @memset(tracer.events_timing, null);

    tracer.gauge(.metrics_statsd_packets, metrics_statsd_packets);
}

// Timing works by storing the min, max, sum and count of each value provided. The avg is calculated
// from sum and count at emit time.
//
// When these are emitted upstream (via statsd, currently), upstream must apply different
// aggregations:
// * min/max/avg are considered gauges for aggregation: last value wins.
// * sum/count are considered counters for aggregation: they are added to the existing values.
//
// This matches the default behavior of the `g` and `c` statsd types respectively.
pub fn timing(tracer: *Tracer, event_timing: EventTiming, duration: Duration) void {
    const timing_slot = event_timing.slot();

    if (tracer.events_timing[timing_slot]) |*event_timing_existing| {
        assert(std.meta.eql(event_timing_existing.event, event_timing));

        const timing_existing = event_timing_existing.values;
        event_timing_existing.values = .{
            .duration_min = timing_existing.duration_min.min(duration),
            .duration_max = timing_existing.duration_min.max(duration),
            .duration_sum = .{ .ns = timing_existing.duration_sum.ns +| duration.ns },
            .count = timing_existing.count +| 1,
        };
    } else {
        tracer.events_timing[timing_slot] = .{
            .event = event_timing,
            .values = .{
                .duration_min = duration,
                .duration_max = duration,
                .duration_sum = duration,
                .count = 1,
            },
        };
    }
}

const fixtures = @import("testing/fixtures.zig");

test "trace json" {
    const Snap = stdx.Snap;
    const snap = Snap.snap_fn("src");
    const gpa = std.testing.allocator;

    var trace_buffer: std.ArrayListUnmanaged(u8) = .empty;
    defer trace_buffer.deinit(gpa);

    var time_sim = fixtures.init_time(.{});

    var trace = try fixtures.init_tracer(gpa, time_sim.time(), .{
        .writer = trace_buffer.writer(gpa).any(),
        .process_id = .unknown,
    });
    defer trace.deinit(gpa);

    // Check that JSON is valid even while process id not known.
    trace.start(.metrics_emit);
    time_sim.ticks += 1;
    trace.stop(.metrics_emit);

    trace.set_replica(.{ .cluster = 1, .replica = 1 });

    trace.start(.{ .replica_commit = .{ .stage = .idle, .op = 123 } });
    time_sim.ticks += 1;
    trace.start(.{ .compact_beat = .{ .tree = @enumFromInt(1), .level_b = 1 } });
    time_sim.ticks += 2;
    trace.stop(.{ .compact_beat = .{ .tree = @enumFromInt(1), .level_b = 1 } });
    time_sim.ticks += 3;
    trace.stop(.{ .replica_commit = .{ .stage = .idle, .op = 456 } });

    try snap(@src(),
        \\[
        \\{"pid":0,"tid":208,"ph":"B","ts":0,"cat":"metrics_emit","name":"metrics_emit  ","args":""},
        \\{"pid":0,"tid":208,"ph":"E","ts":10000},
        \\{"pid":1,"tid":0,"ph":"B","ts":10000,"cat":"replica_commit","name":"replica_commit  stage=idle","args":{"stage":"idle","op":123}},
        \\{"pid":1,"tid":8,"ph":"B","ts":20000,"cat":"compact_beat","name":"compact_beat  tree=Account.id","args":{"tree":"Account.id","level_b":1}},
        \\{"pid":1,"tid":8,"ph":"E","ts":40000},
        \\{"pid":1,"tid":0,"ph":"E","ts":70000},
        \\
    ).diff(trace_buffer.items);
}

test "timing overflow" {
    const gpa = std.testing.allocator;

    var time_sim = fixtures.init_time(.{});
    var trace = try fixtures.init_tracer(gpa, time_sim.time(), .{});
    defer trace.deinit(gpa);

    trace.set_replica(.{ .cluster = 0, .replica = 0 });

    const event: EventTiming = .replica_aof_write;
    const value: Duration = .{ .ns = std.math.maxInt(u64) - 1 };
    trace.timing(event, value);
    trace.timing(event, value);

    const aggregate = trace.events_timing[event.slot()].?;

    assert(aggregate.values.count == 2);
    assert(aggregate.values.duration_min.ns == value.ns);
    assert(aggregate.values.duration_max.ns == value.ns);
    assert(aggregate.values.duration_sum.ns == std.math.maxInt(u64));
}
