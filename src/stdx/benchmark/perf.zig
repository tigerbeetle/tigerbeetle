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

    const scale = 1_000_000_000;
    var checksum: u128 = 0;

    try perf.start();

    for (1..scale) |i| {
        checksum += i * i;
    }

    const measurements = try perf.read(scale, @truncate(checksum));
    try measurements.print_csv(.header, .{
        .op = "*",
        .context = "test",
    });
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
    /// The counter counts raw events, as returned by the OS
    event_count,
    /// The event count is interpreted through a scaling factor,
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
    ) f64 {
        switch (counter_derived) {
            .ipc => {
                const instructions = measurement.get_counter(.instructions);
                const cycles_cpu = measurement.get_counter(.cycles_cpu);
                assert(cycles_cpu > 0);
                assert(instructions > 0);
                return instructions / cycles_cpu;
            },
            .ghz => {
                const cycles_cpu = measurement.get_counter(.cycles_cpu);
                const task_clock = measurement.get_counter(.task_clock);
                assert(cycles_cpu > 0);
                assert(task_clock > 0);
                return cycles_cpu / task_clock;
            },
            .cores => {
                const task_clock = measurement.get_counter(.task_clock);
                const elapsed_ns = measurement.elapsed.ns;
                assert(task_clock > 0);
                assert(elapsed_ns > 0);
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
        parameters: anytype,
    ) !void {
        const writer = std.io.getStdErr().writer();
        switch (mode) {
            .header => try measurement.write_csv_header(writer, parameters),
            .noheader => {},
        }
        try measurement.write_csv_values(writer, parameters);
    }

    pub fn write_csv_header(
        measurement: *const PerfMeasurement,
        writer: anytype,
        parameters: anytype,
    ) !void {
        assert(@typeInfo(@TypeOf(parameters)) == .@"struct");
        var column: u32 = 0;
        inline for (comptime std.meta.fieldNames(@TypeOf(parameters))) |field_name| {
            if (column > 0) try writer.print(", ", .{});
            column += 1;
            try writer.print("{s: >}", .{field_name});
        }
        if (column > 0) try writer.print(", ", .{});
        try writer.print("elapsed_ms", .{});
        for (std.enums.values(CounterType)) |counter| {
            try writer.print(", ", .{});
            try writer.print("{s: >4}", .{@tagName(counter)});
        }
        for (std.enums.values(DerivedCounter)) |derived_counter| {
            try writer.print(", ", .{});
            try writer.print("{s: >4}", .{@tagName(derived_counter)});
        }
        try writer.print(", {[scale]s: >[scale_width]}, {[checksum]s: >[checksum_width]}\n", .{
            .scale = "scale",
            .scale_width = @as(usize, @intFromFloat(std.math.log10(measurement.scale))) + 1,
            .checksum = "checksum",
            .checksum_width = if (measurement.checksum == 0) 1 else @divFloor(
                (64 + 3 - @clz(measurement.checksum)),
                4,
            ),
        });
    }

    pub fn write_csv_values(
        measurement: *const PerfMeasurement,
        writer: anytype,
        parameters: anytype,
    ) !void {
        assert(@typeInfo(@TypeOf(parameters)) == .@"struct");
        var column: u32 = 0;
        inline for (comptime std.meta.fields(@TypeOf(parameters))) |field| {
            if (column > 0) try writer.print(", ", .{});
            column += 1;
            const field_fmt = switch (@typeInfo(field.type)) {
                .int => "{[field_value]d: >[field_width]}",
                .float => "{[field_value]d: >[field_width].2}",
                .pointer => "{[field_value]s: >[field_width]}",
                .bool => "{: >[field_width]}",
                .@"enum" => "{[field_value]any: >[field_width]}",
                else => @panic("Type not supported for serialization"),
            };
            try writer.print(field_fmt, .{
                .field_value = @field(parameters, field.name),
                .field_width = field.name.len,
            });
        }
        if (column > 0) try writer.print(", ", .{});
        try writer.print("{d: >10}", .{measurement.elapsed.ns / std.time.ns_per_ms});
        const value_format_string: []const u8 = "{[counter_value]d: >[counter_width].2}";
        for (std.enums.values(CounterType)) |counter| {
            try writer.print(", ", .{});
            try writer.print(value_format_string, .{
                .counter_value = measurement.get_counter(counter),
                .counter_width = @tagName(counter).len,
            });
        }
        for (std.enums.values(DerivedCounter)) |derived_counter| {
            const derived = measurement.compute_derived(derived_counter);
            try writer.print(", ", .{});
            try writer.print(value_format_string, .{
                .counter_value = derived,
                .counter_width = @tagName(derived_counter).len,
            });
        }
        try writer.print(", {d: >5}, {X: >8}", .{ measurement.scale, measurement.checksum });
    }
};

const PerfCountersLinux = @import("./perf_linux.zig").PerfCounters;
const PerfCounters = switch (builtin.target.os.tag) {
    .linux => PerfCountersLinux,
    else => @compileError("PerfCounters only supported on linux"),
};
