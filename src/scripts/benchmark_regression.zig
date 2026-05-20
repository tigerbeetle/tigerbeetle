//! Standalone comparative benchmarks.
//!
//! This intentionally does not upload to devhub. It checks out a baseline ref, runs the macro
//! benchmark already used by devhub plus a microbenchmark stage, then checks out the original
//! commit and runs the same stages again. The run fails if the original commit is more than
//! `--epsilon-percent` slower than the baseline.

const std = @import("std");

const stdx = @import("stdx");
const Shell = @import("../shell.zig");

const log = std.log;

const Direction = enum {
    higher_is_better,
    lower_is_better,
};

pub const CLIArgs = struct {
    baseline: []const u8 = "main",
    epsilon_percent: u64 = 3,
};

pub fn main(shell: *Shell, _: std.mem.Allocator, cli_args: CLIArgs) !void {
    if (cli_args.epsilon_percent >= 100) return error.InvalidEpsilon;

    const dirty = try shell.exec_stdout("git status --porcelain", .{});
    if (dirty.len != 0) {
        log.err("benchmark-regression requires a clean worktree before checking out refs", .{});
        log.err("git status --porcelain:\n{s}", .{dirty});
        return error.DirtyWorktree;
    }

    const original_sha = try shell.exec_stdout("git rev-parse --verify HEAD", .{});
    const original_branch = try shell.exec_stdout("git rev-parse --abbrev-ref HEAD", .{});
    const original_checkout = if (std.mem.eql(u8, original_branch, "HEAD"))
        original_sha
    else
        original_branch;

    defer {
        shell.exec("git checkout --quiet {ref}", .{ .ref = original_checkout }) catch |err| {
            log.err("failed to restore checkout {s}: {}", .{ original_checkout, err });
        };
    }

    const baseline_sha =
        try shell.exec_stdout("git rev-parse --verify {baseline}", .{ .baseline = cli_args.baseline });

    log.info("baseline: {s} ({s})", .{ cli_args.baseline, baseline_sha });
    log.info("current:  {s}", .{original_sha});

    try shell.exec("git checkout --quiet {ref}", .{ .ref = cli_args.baseline });
    const baseline = try run_benchmarks(shell, "baseline");

    try shell.exec("git checkout --quiet {ref}", .{ .ref = original_sha });
    const current = try run_benchmarks(shell, "current");

    try compare_benchmarks(baseline, current, cli_args.epsilon_percent);
}

const Benchmarks = struct {
    macro: MacroBenchmark,
    micro: MicroBenchmark,
};

const MacroBenchmark = struct {
    tps: u64,
    batch_p100_ms: u64,
    query_p100_ms: u64,
};

const MicroBenchmark = struct {
    total_ns: u64,
    per_element_ns: u64,
};

fn run_benchmarks(shell: *Shell, name: []const u8) !Benchmarks {
    var section = try shell.open_section(name);
    defer section.close();

    return .{
        .macro = try run_macro_benchmark(shell),
        .micro = try run_micro_benchmark(shell),
    };
}

fn run_macro_benchmark(shell: *Shell) !MacroBenchmark {
    var section = try shell.open_section("macro benchmark");
    defer section.close();

    shell.cwd.deleteFile("datafile-benchmark-regression") catch {};
    defer shell.cwd.deleteFile("datafile-benchmark-regression") catch {};

    try shell.exec_zig_options(
        .{ .timeout = .minutes(30) },
        "build -Drelease install",
        .{},
    );
    defer shell.project_root.deleteFile("tigerbeetle") catch {};

    const stdout, _ = try shell.exec_stdout_stderr(
        "./tigerbeetle benchmark --validate --checksum-performance --log-debug-replica " ++
            "--file=datafile-benchmark-regression",
        .{},
    );

    return .{
        .tps = try get_measurement(stdout, "load accepted", "tx/s"),
        .batch_p100_ms = try get_measurement(stdout, "batch latency p100", "ms"),
        .query_p100_ms = try get_measurement(stdout, "query latency p100", "ms"),
    };
}

fn run_micro_benchmark(shell: *Shell) !MicroBenchmark {
    var section = try shell.open_section("micro benchmark");
    defer section.close();

    const stdout, const stderr = try shell.exec_stdout_stderr(
        "./zig/zig build test -- {filter}",
        .{ .filter = "benchmark: k-way" },
    );
    const output = try std.mem.concat(shell.arena.allocator(), u8, &.{ stdout, "\n", stderr });

    return .{
        .total_ns = try get_duration_measurement(output, "total"),
        .per_element_ns = try get_duration_measurement(output, "per element"),
    };
}

fn compare_benchmarks(
    baseline: Benchmarks,
    current: Benchmarks,
    epsilon_percent: u64,
) !void {
    var failed = false;

    failed = compare_measurement(.{
        .name = "macro TPS",
        .unit = "tx/s",
        .baseline = baseline.macro.tps,
        .current = current.macro.tps,
        .direction = .higher_is_better,
        .epsilon_percent = epsilon_percent,
    }) or failed;
    failed = compare_measurement(.{
        .name = "macro batch p100",
        .unit = "ms",
        .baseline = baseline.macro.batch_p100_ms,
        .current = current.macro.batch_p100_ms,
        .direction = .lower_is_better,
        .epsilon_percent = epsilon_percent,
    }) or failed;
    failed = compare_measurement(.{
        .name = "macro query p100",
        .unit = "ms",
        .baseline = baseline.macro.query_p100_ms,
        .current = current.macro.query_p100_ms,
        .direction = .lower_is_better,
        .epsilon_percent = epsilon_percent,
    }) or failed;
    failed = compare_measurement(.{
        .name = "micro k-way total",
        .unit = "ns",
        .baseline = baseline.micro.total_ns,
        .current = current.micro.total_ns,
        .direction = .lower_is_better,
        .epsilon_percent = epsilon_percent,
    }) or failed;
    failed = compare_measurement(.{
        .name = "micro k-way per element",
        .unit = "ns",
        .baseline = baseline.micro.per_element_ns,
        .current = current.micro.per_element_ns,
        .direction = .lower_is_better,
        .epsilon_percent = epsilon_percent,
    }) or failed;

    if (failed) return error.BenchmarkRegression;
}

fn compare_measurement(measurement: struct {
    name: []const u8,
    unit: []const u8,
    baseline: u64,
    current: u64,
    direction: Direction,
    epsilon_percent: u64,
}) bool {
    const regression = is_regression(
        measurement.baseline,
        measurement.current,
        measurement.direction,
        measurement.epsilon_percent,
    );

    const status: []const u8 = if (regression) "REGRESSION" else "ok";
    log.info(
        "{s}: {s}: baseline={} {s}, current={} {s}, epsilon={}%",
        .{
            status,
            measurement.name,
            measurement.baseline,
            measurement.unit,
            measurement.current,
            measurement.unit,
            measurement.epsilon_percent,
        },
    );

    return regression;
}

fn is_regression(
    baseline: u64,
    current: u64,
    direction: Direction,
    epsilon_percent: u64,
) bool {
    if (baseline == 0) {
        return switch (direction) {
            .higher_is_better => false,
            .lower_is_better => current != 0,
        };
    }

    return switch (direction) {
        .higher_is_better => @as(u128, current) * 100 < @as(u128, baseline) * (100 - epsilon_percent),
        .lower_is_better => @as(u128, current) * 100 > @as(u128, baseline) * (100 + epsilon_percent),
    };
}

fn get_measurement(
    benchmark_stdout: []const u8,
    comptime label: []const u8,
    comptime unit: []const u8,
) !u64 {
    errdefer {
        log.err("can't extract '" ++ label ++ "' measurement", .{});
    }

    _, const rest = stdx.cut(benchmark_stdout, label ++ " = ") orelse
        return error.BadMeasurement;
    const value_string, _ = stdx.cut(rest, " " ++ unit) orelse return error.BadMeasurement;

    return try std.fmt.parseInt(u64, value_string, 10);
}

fn get_duration_measurement(output: []const u8, label: []const u8) !u64 {
    var lines = std.mem.splitScalar(u8, output, '\n');
    while (lines.next()) |line| {
        if (!std.mem.endsWith(u8, line, label)) continue;

        const duration_string, const line_label = stdx.cut(line, " ") orelse
            return error.BadDurationMeasurement;
        if (!std.mem.eql(u8, line_label, label)) continue;

        return try parse_duration_ns(duration_string);
    }

    log.err("can't extract '{s}' duration measurement", .{label});
    return error.BadDurationMeasurement;
}

fn parse_duration_ns(duration: []const u8) !u64 {
    const Unit = struct {
        suffix: []const u8,
        multiplier: u64,
    };
    const units = [_]Unit{
        .{ .suffix = "ns", .multiplier = 1 },
        .{ .suffix = "us", .multiplier = std.time.ns_per_us },
        .{ .suffix = "\xc2\xb5s", .multiplier = std.time.ns_per_us },
        .{ .suffix = "ms", .multiplier = std.time.ns_per_ms },
        .{ .suffix = "s", .multiplier = std.time.ns_per_s },
    };

    for (units) |unit| {
        if (std.mem.endsWith(u8, duration, unit.suffix)) {
            return parse_decimal_scaled(
                duration[0 .. duration.len - unit.suffix.len],
                unit.multiplier,
            );
        }
    }

    log.err("unknown duration unit: {s}", .{duration});
    return error.BadDuration;
}

fn parse_decimal_scaled(decimal: []const u8, multiplier: u64) !u64 {
    const integer_part, const fractional_part = if (stdx.cut(decimal, ".")) |parts|
        parts
    else
        .{ decimal, "" };

    if (integer_part.len == 0) return error.BadDuration;
    const integer = try std.fmt.parseInt(u128, integer_part, 10);

    var result = integer * multiplier;
    if (fractional_part.len > 0) {
        var scale: u128 = 1;
        for (fractional_part) |digit| {
            if (!std.ascii.isDigit(digit)) return error.BadDuration;
            scale *= 10;
        }
        const fractional = try std.fmt.parseInt(u128, fractional_part, 10);
        result += fractional * multiplier / scale;
    }

    return std.math.cast(u64, result) orelse error.Overflow;
}
