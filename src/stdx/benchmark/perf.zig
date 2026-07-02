const std = @import("std");
const assert = std.debug.assert;
const builtin = @import("builtin");

const stdx = @import("../stdx.zig");
const Duration = stdx.Duration;

test "benchmark: performance counter tutorial" {
    if (comptime builtin.target.os.tag != .linux) {
        return;
    }

    var perf = try PerfCounters.init();
    defer perf.deinit();

    const scale = 1_000_000;
    var checksum: u64 = 1;

    try perf.start();

    for (0..scale) |i| {
        checksum *= i;
    }

    const measurements = try perf.read(scale, checksum);
    try measurements.print_csv(.header);
}

pub const CounterType = enum {
    cycles_cpu,
    cycles_kernel,
    cycles_stall,
    instructions,
    cache_references,
    cache_misses,
    branch_misses,
    task_clock,
    dtlb_load_misses,

    pub fn shortcode(counter_type: CounterType) []u8 {
        return switch (counter_type) {
            .cycles_cpu => "c",
            .cycles_kernel => "kc",
            .cycles_stall => "sc",
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
    ipc,
    ghz,
    cores,

    pub fn shortcode(derived_counter: DerivedCounter) []u8 {
        return switch (derived_counter) {
            .ipc => "ipc",
            .ghz => "ghz",
            .cores => "cpu",
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
    counters: std.enums.EnumArray(CounterType, f64),
    elapsed: Duration,
    checksum: u64,
    scale: f64,

    pub fn compute_derived(
        measurement: *const PerfMeasurement,
        counter_derived: DerivedCounter,
    ) ?f64 {
        switch (counter_derived) {
            .ipc => {
                const instructions = measurement.get_counter(.instructions);
                const cycles_cpu = measurement.get_counter(.cycles_cpu);
                return instructions / cycles_cpu;
            },
            .ghz => {
                const cycles_cpu = measurement.get_counter(.cycles_cpu);
                const task_clock = measurement.get_counter(.task_clock);
                return cycles_cpu / task_clock;
            },
            .cores => {
                const task_clock = measurement.get_counter(.task_clock);
                const elapsed_ns = measurement.elapsed.ns;
                return task_clock / @as(f64, @floatFromInt(elapsed_ns));
            },
        }
    }

    pub fn get_counter(measurement: *const PerfMeasurement, counter: CounterType) f64 {
        return measurement.counters.get(counter);
    }

    pub fn print_csv(
        measurement: *const PerfMeasurement,
        mode: enum { header, noheader },
    ) !void {
        const writer = std.io.getStdErr().writer();
        switch (mode) {
            .header => try measurement.write_csv_header(writer),
            .noheader => {},
        }
        try measurement.write_csv_values(writer);
    }

    pub fn write_csv_header(
        measurement: *const PerfMeasurement,
        writer: anytype,
    ) !void {
        try writer.print("elapsed_ms, ", .{});
        for (std.enums.values(CounterType), 0..) |counter, i| {
            if (i > 0) try writer.print(", ", .{});
            try writer.print("{s: >4}", .{@tagName(counter)});
        }
        try writer.print(", ", .{});
        for (std.enums.values(DerivedCounter), 0..) |derived_counter, i| {
            if (i > 0) try writer.print(", ", .{});
            try writer.print("{s: >4}", .{@tagName(derived_counter)});
        }
        try writer.print(", {[scale]s: <[scale_width]}, {[checksum]s: <[checksum_width]]}\n", .{
            .scale = "scale",
            .scale_width = @as(usize, @intFromFloat(std.math.log10(measurement.scale)) + 1),
            .checksum = "checksum",
            .checksum_width = @as(usize, @intFromFloat(std.math.log(u64, 16, measurement.checksum)) + 1),
        });
    }

    pub fn write_csv_values(
        measurement: *const PerfMeasurement,
        writer: anytype,
    ) !void {
        try writer.print("{d: >10}, ", .{measurement.elapsed.ns / std.time.ns_per_ms});
        const value_format_string: []const u8 = "{[counter_value]d: >[counter_width].2}";
        for (std.enums.values(CounterType), 0..) |counter, i| {
            if (i > 0) try writer.print(", ", .{});
            try writer.print(value_format_string, .{
                .counter_value = measurement.get_counter(counter),
                .counter_width = @tagName(counter).len,
            });
        }
        try writer.print(", ", .{});
        for (std.enums.values(DerivedCounter), 0..) |derived_counter, i| {
            const derived = measurement.compute_derived(derived_counter) orelse continue;
            if (i > 0) try writer.print(", ", .{});
            try writer.print(value_format_string, .{
                .counter_value = derived,
                .counter_width = @tagName(derived_counter).len,
            });
        }
        try writer.print(", {d: >5.2}, {X}", .{ measurement.scale, measurement.checksum });
    }
};

const PerfCountersLinux = @import("./perf_linux.zig").PerfCounters;
const PerfCounters = switch (builtin.target.os.tag) {
    .linux => PerfCountersLinux,
    else => @compileError("PerfCounters only supported on linux"),
};
