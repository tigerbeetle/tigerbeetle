//! Standalone comparative benchmarks.
//!
//! This intentionally does not upload to devhub. It creates a temporary worktree for a baseline ref
//! and a second temporary worktree for the current commit, builds their benchmark artifacts in
//! parallel, and then runs the same benchmark stages sequentially. The run fails if the current
//! commit is more than `--epsilon-percent` slower than the baseline.

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
        log.err("benchmark-regression requires a clean worktree", .{});
        log.err("git status --porcelain:\n{s}", .{dirty});
        return error.DirtyWorktree;
    }

    const original_sha = try shell.exec_stdout("git rev-parse --verify HEAD", .{});
    const baseline_sha =
        try shell.exec_stdout("git rev-parse --verify {baseline}", .{ .baseline = cli_args.baseline });

    log.info("baseline: {s} ({s})", .{ cli_args.baseline, baseline_sha });
    log.info("current:  {s}", .{original_sha});

    const worktrees = try create_worktrees(shell, .{
        .baseline_sha = baseline_sha,
        .current_sha = original_sha,
    });
    defer worktrees.remove(shell);

    try build_worktrees(worktrees);

    const baseline = try run_benchmarks(shell, "baseline", worktrees.baseline_path);
    const current = try run_benchmarks(shell, "current", worktrees.current_path);

    try compare_benchmarks(baseline, current, cli_args.epsilon_percent);
}

const Worktrees = struct {
    root_path: []const u8,
    baseline_path: []const u8,
    current_path: []const u8,

    fn remove(worktrees: Worktrees, shell: *Shell) void {
        shell.exec("git worktree remove --force {path}", .{ .path = worktrees.baseline_path }) catch |err| {
            log.err("failed to remove baseline worktree {s}: {}", .{ worktrees.baseline_path, err });
        };
        shell.exec("git worktree remove --force {path}", .{ .path = worktrees.current_path }) catch |err| {
            log.err("failed to remove current worktree {s}: {}", .{ worktrees.current_path, err });
        };
        shell.project_root.deleteTree(worktrees.root_path) catch |err| {
            log.err("failed to remove benchmark worktree root {s}: {}", .{ worktrees.root_path, err });
        };
    }
};

fn create_worktrees(
    shell: *Shell,
    refs: struct {
        baseline_sha: []const u8,
        current_sha: []const u8,
    },
) !Worktrees {
    var section = try shell.open_section("create worktrees");
    defer section.close();

    const root_path = try shell.create_tmp_dir();
    const baseline_path = try std.fs.path.join(
        shell.arena.allocator(),
        &.{ root_path, "baseline" },
    );
    const current_path = try std.fs.path.join(
        shell.arena.allocator(),
        &.{ root_path, "current" },
    );

    try shell.exec(
        "git worktree add --detach {path} {ref}",
        .{ .path = baseline_path, .ref = refs.baseline_sha },
    );
    errdefer shell.exec("git worktree remove --force {path}", .{ .path = baseline_path }) catch {};

    try shell.exec(
        "git worktree add --detach {path} {ref}",
        .{ .path = current_path, .ref = refs.current_sha },
    );
    errdefer shell.exec("git worktree remove --force {path}", .{ .path = current_path }) catch {};

    return .{
        .root_path = root_path,
        .baseline_path = baseline_path,
        .current_path = current_path,
    };
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

fn run_benchmarks(shell: *Shell, name: []const u8, worktree_path: []const u8) !Benchmarks {
    var section = try shell.open_section(name);
    defer section.close();

    try shell.pushd(worktree_path);
    defer shell.popd();

    return .{
        .macro = try run_macro_benchmark(shell),
        .micro = try run_micro_benchmark(shell),
    };
}

const BuildContext = struct {
    name: []const u8,
    path: []const u8,
    zig_exe: []const u8,
    env_map: *std.process.EnvMap,
};

fn build_worktrees(worktrees: Worktrees) !void {
    var section = try std.time.Timer.start();
    defer log.info("build benchmark artifacts: {}", .{std.fmt.fmtDuration(section.read())});

    const zig_exe_env = std.process.getEnvVarOwned(
        std.heap.page_allocator,
        "ZIG_EXE",
    ) catch |err| switch (err) {
        error.EnvironmentVariableNotFound => null,
        else => |e| return e,
    };
    defer if (zig_exe_env) |zig_exe| std.heap.page_allocator.free(zig_exe);

    const zig_exe = zig_exe_env orelse "./zig/zig";

    var env_map = try std.process.getEnvMap(std.heap.page_allocator);
    defer env_map.deinit();

    const baseline_context: BuildContext = .{
        .name = "baseline",
        .path = worktrees.baseline_path,
        .zig_exe = zig_exe,
        .env_map = &env_map,
    };
    const current_context: BuildContext = .{
        .name = "current",
        .path = worktrees.current_path,
        .zig_exe = zig_exe,
        .env_map = &env_map,
    };

    try build_worktrees_step("release binary", .release, &.{ baseline_context, current_context });
    try build_worktrees_step("k-way microbenchmark", .micro, &.{ baseline_context, current_context });
}

const BuildStep = enum {
    release,
    micro,
};

fn build_worktrees_step(
    step_name: []const u8,
    build_step: BuildStep,
    contexts: *const [2]BuildContext,
) !void {
    var timer = try std.time.Timer.start();

    log.info("building {s}", .{step_name});

    var children: [2]std.process.Child = undefined;
    var children_spawned: usize = 0;
    errdefer for (children[0..children_spawned]) |*child| {
        _ = child.kill() catch {};
    };

    for (&children, contexts) |*child, *context| {
        const argv_release = [_][]const u8{ context.zig_exe, "build", "-Drelease", "install" };
        const argv_micro = [_][]const u8{
            context.zig_exe,
            "build",
            "test:unit:build",
            "--",
            "benchmark: k-way",
        };
        const argv: []const []const u8 = switch (build_step) {
            .release => &argv_release,
            .micro => &argv_micro,
        };

        child.* = std.process.Child.init(argv, std.heap.page_allocator);
        child.cwd = context.path;
        child.env_map = context.env_map;
        child.stdin_behavior = .Ignore;
        child.stdout_behavior = .Inherit;
        child.stderr_behavior = .Inherit;
        try child.spawn();
        children_spawned += 1;
    }

    var failed = false;
    for (&children, contexts) |*child, *context| {
        const term = try child.wait();
        switch (term) {
            .Exited => |code| if (code != 0) {
                log.err("{s}: {s} build failed with exit code {}", .{
                    context.name,
                    step_name,
                    code,
                });
                failed = true;
            },
            else => {
                log.err("{s}: {s} build failed with {}", .{
                    context.name,
                    step_name,
                    term,
                });
                failed = true;
            },
        }
    }

    log.info("{s}: {}", .{ step_name, std.fmt.fmtDuration(timer.read()) });
    if (failed) return error.BuildFailed;
}

fn run_macro_benchmark(shell: *Shell) !MacroBenchmark {
    var section = try shell.open_section("macro benchmark");
    defer section.close();

    shell.cwd.deleteFile("datafile-benchmark-regression") catch {};
    defer shell.cwd.deleteFile("datafile-benchmark-regression") catch {};

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
        "./zig-out/bin/test-unit {filter}",
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
