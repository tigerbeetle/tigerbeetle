//! For ad-hoc performance measurements and algorithm optimization, it is often useful to
//! get performance metrics for a small subsection of code. For example:
//!
//! - how many cache misses does this data structure incur per lookup?
//! - in some operation, how many of the cpu cycles are stall cycles?
//! - how many instructions does this algorithm execute per cycle?
//! - how many cpu cycles are spent in the kernel?
//!
//! Whereas external tools such as `perf` allow answering such questions for the entire program,
//! `PerfCounters` allows examining only the subsection of the program we're interested in.
//! It is intended to be used ad-hoc, when needed, not to be permanently included in tests etc.,
//! and currently works only on linux with `kernel.perf_event_paranoid` set to -1.
//!
//! For example, the test below outputs a CSV like this, which directly usable for
//! plotting / analysis (some columns omitted):
//! ```
//! op, context, elapsed_ms, cycles_cpu, instructions,   ipc,  ghz,      scale,         checksum
//!  *,    test,        687,       1.37,         6.34,  4.62, 2.00, 1000000000, 2E5524BA83927700
//! ```
//! Cycle and instruction counters are normalized per operation/element, set by the scale parameter.
//! This normalization allows for more intuition, e.g., "is 6 instructions per element reasonable?".
//! Interface inspired by: https://github.com/viktorleis/perfevent
const std = @import("std");
const assert = std.debug.assert;
const builtin = @import("builtin");

const stdx = @import("../stdx.zig");
const Duration = stdx.Duration;

test "perf: usage example" {
    if (builtin.target.os.tag != .linux) {
        return error.SkipZigTest;
    }

    var perf = PerfCounters.init() catch |err| switch (err) {
        error.PermissionDenied => {
            // Example error message. Disabled since performance counters are expected to be
            // unavailable in CI environments, and the test should be skipped with no output.
            if (!builtin.is_test) {
                std.debug.print(
                    \\Insufficient permmissions for opening linux performance counters. Try running
                    \\   sudo sysctl -w kernel.perf_event_paranoid=-1
                    \\This may not be possible when running in a virtualized environment. 
                    \\
                , .{});
            }
            return error.SkipZigTest;
        },
        else => return err,
    };
    defer perf.deinit();

    var output_memory = std.ArrayList(u8).init(std.testing.allocator);
    defer output_memory.deinit();

    const scale = 1_000;
    try perf.start();

    var checksum: u128 = 0;
    for (1..scale) |i| {
        checksum += i * i;
    }
    const measurement = try perf.lap();

    const Parameters = struct { op: []const u8, context: []const u8 };
    var output: PerfTableType(Parameters) = try .init(output_memory.writer().any());
    try output.row(&measurement, .{ .checksum = @truncate(checksum), .scale = scale }, .{
        .op = "*",
        .context = "test",
    });

    const header, const values = stdx.cut(output_memory.items, "\n").?;
    assert(header.len > 0);
    assert(values.len > 0);
    assert(std.mem.startsWith(u8, header, "  op, context, elapsed_ms"));
    assert(std.mem.startsWith(u8, values, "   *,    test, "));
}

const PerfCountersLinux = @import("./perf_linux.zig").PerfCounters;
pub const PerfCounters = switch (builtin.target.os.tag) {
    .linux => PerfCountersLinux,
    else => @compileError("PerfCounters only supported on linux"),
};

pub const PerfParameters = struct { scale: u64, checksum: u64 };

pub fn PerfTableType(BenchmarkParameters: type) type {
    assert(@typeInfo(BenchmarkParameters) == .@"struct");
    const TabularOutput = @import("./tabular.zig").TabularOutputType(&.{
        BenchmarkParameters,
        struct { elapsed_ms: f64 },
        std.enums.EnumFieldStruct(CounterType, f64, null),
        std.enums.EnumFieldStruct(DerivedCounter, f64, null),
        PerfParameters,
    });

    return struct {
        const PerfTable = @This();

        output: TabularOutput,

        pub fn init(writer: std.io.AnyWriter) !PerfTable {
            return .{ .output = try .init(writer, .{ .header = true }) };
        }

        pub fn row(
            table: *PerfTable,
            measurement: *const PerfMeasurement,
            parameters_perf: PerfParameters,
            parameters_bench: BenchmarkParameters,
        ) !void {
            const elapsed_ns: f64 = @floatFromInt(measurement.elapsed.ns);
            try table.output.write_row(&TabularOutput.row_from_bag(.{
                parameters_bench,
                .{ .elapsed_ms = elapsed_ns / std.time.ns_per_ms },
                measurement.scaled(@floatFromInt(parameters_perf.scale)),
                measurement.compute_derived(),
                parameters_perf,
            }));
        }
    };
}

pub const PerfMeasurement = struct {
    counters: CounterType.CollectionType(),
    elapsed: Duration,

    fn scaled(measurement: *const PerfMeasurement, scale: f64) CounterType.CollectionType() {
        var result: CounterType.CollectionType() = undefined;
        inline for (comptime std.enums.values(CounterType)) |counter_type| {
            const unscaled = @field(measurement.counters, @tagName(counter_type));
            @field(result, @tagName(counter_type)) = unscaled / scale;
        }
        return result;
    }

    fn compute_derived(measurement: *const PerfMeasurement) DerivedCounter.CollectionType() {
        const instructions = measurement.counters.instructions;
        const cycles_cpu = measurement.counters.cycles_cpu;
        const task_clock = measurement.counters.task_clock;
        const elapsed_ns = measurement.elapsed.ns;

        return .{
            .ipc = instructions / cycles_cpu,
            .ghz = cycles_cpu / task_clock,
            .cores = task_clock / @as(f64, @floatFromInt(elapsed_ns)),
        };
    }
};

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

    fn CollectionType() type {
        return std.enums.EnumFieldStruct(CounterType, f64, null);
    }
};

pub const DerivedCounter = enum {
    ipc,
    ghz,
    cores,

    fn CollectionType() type {
        return std.enums.EnumFieldStruct(DerivedCounter, f64, null);
    }
};
