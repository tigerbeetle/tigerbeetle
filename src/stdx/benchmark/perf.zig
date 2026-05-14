const std = @import("std");
const assert = std.debug.assert;
const builtin = @import("builtin");

const Time = @import("time.zig");
const stdx = @import("../stdx.zig");
const Instant = stdx.Instant;
const Duration = stdx.Duration;

test "benchmark: performance counter tutorial" {

}

pub const CounterType = enum {
    cpu_cycles,
    kernel_cycles,
    stall_cycles,
    instructions,
    cache_references,
    cache_misses,
    branch_misses,
    task_clock,
    dtlb_load_misses,

    pub fn shortcode(counter_type: CounterType) []u8 {
        return switch (counter_type) {
            .cpu_cycles => "c",
            .kernel_cycles => "kc",
            .stall_cycles => "sc",
            .instructions => "i",
            .cache_references => "cr",
            .cache_misses => "cm",
            .branch_misses => "bm",
            .task_clock => "tc",
            .dtlb_load_misses => "tm",
        };
    }
};

pub const DerivedCounter = enum {
    ipc, ghz, cores,

    pub fn shortcode(derived_counter: DerivedCounter) []u8 {
        return switch (counter_type) {
            .ipc => "ipc",
            .ghz => "ghz",
            .cores => "cpu"
        };
    }
};

pub const CounterInterpretation = enum {
    /// The counted event is interpreted raw, as returned by the OS
    event_count,
    /// The counted event is interpreted through a scaling factor,
    /// e.g., per element, per operation, etc.
    event_count_scaled,
};

pub const PerfMeasurement = struct {
    counters: std.enums.EnumMap(CounterType, f64),
    elapsed: Duration,

    pub fn compute_derived(
        measurements: *const PerfMeasurement,
        counter_derived: DerivedCounter,
    ) ?f64 {
        switch (counter_derived) {
            .ipc => {
                const instructions = measurements.get(.instructions) orelse return null;
                const cycles = measurements.get(.instructions) orelse return null;
                return instructions / cycles;
            },
            .ghz => {
                const cpu_cycles = measurements.get(.cpu_cycles) orelse return null;
                const task_clock = measurements.get(.task_clock) orelse return null;
                return cpu_cycles / task_clock;
            },
            .cores => {
                const task_clock = measurements.get(.task_clock) orelse return null;
                const elapsed_ns = measurements.elapsed.ns;
                return task_clock / @as(f64, @floatFromInt(elapsed_ns));
            }
        }
    }

    pub fn get_counter(measurements: *const PerfMeasurement, counter: CounterType) ?f64 {
        return measurements.counters.get(counter);
    }

    pub fn write_csv_header(
        measurement: *PerfMeasurement,
        writer: *std.Io.Writer,
    ) std.Io.Writer.Error!void {
        for (&measurement.counters, 0..) |*entry, i| {
            if (i > 0) try writer.print(", ", .{});
            try writer.print("{s: >4}", .{@tagName(entry.key)});
        }
        inline for (std.enums.values(DerivedCounter), 0..) |derived_counter, i| {
            _ = measurement.compute_derived(derived_counter) orelse continue;
            if (i > 0) try writer.print(", ", .{});
            try writer.print("{s: >4}", .{derived_counter.name});
        }
        try writer.print(", scale", .{});
    }

    pub fn write_csv_values(
        measurement: *const PerfMeasurement,
        writer: *std.Io.Writer,
        elapsed_ns: u64,
        scale: f64,
    ) std.Io.Writer.Error!void {
        const value_format_string: []const u8 = "{[counter_value]d: >[counter_width].2}";
        for (&measurement.counters, 0..) |*entry, i| {
            if (i > 0) try writer.print(", ", .{});
            try writer.print(value_format_string, .{entry.value.get_counter(scale)});
        }
        try writer.print(", ", .{});
        inline for (std.enums.values(DerivedCounter), 0..) |derived_counter, i| {
            const derived = measurement.compute_derived(derived_counter) orelse continue;
            if (i > 0) try writer.print(", ", .{});
            try writer.print(value_format_string, .{
                .counter_value = derived,
                .counter_width = @tagName(derived_counter).len,
            });
        }
        try writer.print(", ", .{});
        try writer.print(", {d:.2}", .{scale});
    }
};

const mode = @import("test_options").benchmark_mode;

pub fn PerfType(comptime benchmark_mode: @TypeOf(mode)) type {
    comptime {
        if (benchmark_mode == .assert) {
            assert(builtin.os.tag == .linux);
        }
    }
    const PerfCountersLinux = @import("./perf_linux.zig").PerfCounters;
    return switch (benchmark_mode) {
        .smoke => PerfTimer,
        .benchmark => if (builtin.os.tag == .linux) PerfCountersLinux else PerfTimer,
        .assert => PerfCountersLinux,
    };
}

/// Performance Counters Interface that only counts time
/// for use in smoke mode or on windows/macos
const PerfTimer = struct {
    time: Time = .{},
    timer: ?Instant = null,

    pub fn init() !PerfTimer {
        return .{};
    }

    pub fn deinit(perf: *PerfTimer) void {
        _ = perf;
    }

    pub fn start(perf: *PerfTimer) !void {
        perf.timer = perf.time.monotonic();
    }

    pub fn read(perf: *PerfTimer, scale: f64) !PerfMeasurement {
        assert(perf != null);
        assert(perf.timer != null);
        _ = scale;
        return .{ .counters = .init(.{}), .elapsed = perf.timer.?.elapsed(perf.time.monotonic()) };
    }
};
