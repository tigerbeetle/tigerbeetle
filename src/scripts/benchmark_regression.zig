//! Standalone comparative benchmarks.
//!
//! This intentionally does not upload to devhub. It creates a temporary worktree for a baseline ref
//! and a second temporary worktree for the current commit, builds their benchmark artifacts in
//! parallel, and then runs the same benchmark stages sequentially. The run fails if the current
//! commit is more than `--epsilon-percent` slower than the baseline.

const std = @import("std");
const assert = std.debug.assert;

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
    expected_regression_label: []const u8 = "expected_performance_regression",
};

pub fn main(shell: *Shell, _: std.mem.Allocator, cli_args: CLIArgs) !void {
    assert(cli_args.epsilon_percent < 100);

    const dirty = try shell.exec_stdout("git status --porcelain", .{});
    if (dirty.len != 0) {
        log.err("benchmark-regression requires a clean worktree", .{});
        log.err("git status --porcelain:\n{s}", .{dirty});
        return error.DirtyWorktree;
    }

    const github_event = try read_github_event(shell);
    const ref_baseline = ref_baseline: {
        if (try merge_group_base_sha(shell, github_event)) |base_sha| {
            log.info("using merge_group.base_sha as baseline", .{});
            break :ref_baseline base_sha;
        }
        break :ref_baseline cli_args.baseline;
    };

    const sha_worktree = try shell.exec_stdout("git rev-parse --verify HEAD", .{});
    const sha_baseline =
        try shell.exec_stdout("git rev-parse --verify {ref_baseline}", .{ .ref_baseline = ref_baseline });

    log.info("baseline: {s} ({s})", .{ ref_baseline, sha_baseline });
    log.info("worktree:  {s}", .{sha_worktree});

    const worktrees = try create_worktrees(shell, .{
        .sha_baseline = sha_baseline,
        .sha_worktree = sha_worktree,
    });
    defer worktrees.remove(shell);

    try build_worktrees(shell, worktrees);

    const baseline = try run_benchmarks(shell, "baseline", worktrees.path_baseline);
    const worktree = try run_benchmarks(shell, "worktree", worktrees.path_worktree);

    const failed = compare_benchmarks(baseline, worktree, cli_args.epsilon_percent);
    if (failed) {
        if (try has_github_label(shell, github_event, cli_args.expected_regression_label)) {
            log.warn(
                "benchmark regression accepted because GitHub label '{s}' is present",
                .{cli_args.expected_regression_label},
            );
        } else {
            return error.BenchmarkRegression;
        }
    }
}

const Worktrees = struct {
    path_root: []const u8,
    path_baseline: []const u8,
    path_worktree: []const u8,

    fn remove(worktrees: Worktrees, shell: *Shell) void {
        shell.exec("git worktree remove --force {path}", .{ .path = worktrees.path_baseline }) catch |err| {
            log.err("failed to remove baseline worktree {s}: {}", .{ worktrees.path_baseline, err });
        };
        shell.exec("git worktree remove --force {path}", .{ .path = worktrees.path_worktree }) catch |err| {
            log.err("failed to remove comparison worktree {s}: {}", .{ worktrees.path_worktree, err });
        };
        shell.project_root.deleteTree(worktrees.path_root) catch |err| {
            log.err("failed to remove benchmark worktree root {s}: {}", .{ worktrees.path_root, err });
        };
    }
};

fn create_worktrees(
    shell: *Shell,
    refs: struct {
        sha_baseline: []const u8,
        sha_worktree: []const u8,
    },
) !Worktrees {
    var section = try shell.open_section("create worktrees");
    defer section.close();

    const path_root = try shell.create_tmp_dir();
    const path_baseline = try std.fs.path.join(
        shell.arena.allocator(),
        &.{ path_root, "baseline" },
    );
    const path_worktree = try std.fs.path.join(
        shell.arena.allocator(),
        &.{ path_root, "current" },
    );

    try shell.exec(
        "git worktree add --detach {path} {ref}",
        .{ .path = path_baseline, .ref = refs.sha_baseline },
    );
    errdefer shell.exec("git worktree remove --force {path}", .{ .path = path_baseline }) catch {};

    try shell.exec(
        "git worktree add --detach {path} {ref}",
        .{ .path = path_worktree, .ref = refs.sha_worktree },
    );
    errdefer shell.exec("git worktree remove --force {path}", .{ .path = path_worktree }) catch {};

    return .{
        .path_root = path_root,
        .path_baseline = path_baseline,
        .path_worktree = path_worktree,
    };
}

const Benchmarks = struct {
    macro: MacroBenchmark,
    micro: ?MicroBenchmark,
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
};

fn build_worktrees(shell: *Shell, worktrees: Worktrees) !void {
    var section = try shell.open_section("build benchmark artifacts");
    defer section.close();

    const context_baseline: BuildContext = .{
        .name = "baseline",
        .path = worktrees.path_baseline,
    };
    const context_worktree: BuildContext = .{
        .name = "current",
        .path = worktrees.path_worktree,
    };

    try build_worktrees_step(shell, "release binary", .release, &context_baseline, &context_worktree);
    try build_worktrees_step(shell, "k-way microbenchmark", .micro, &context_baseline, &context_worktree);
}

const BuildStep = enum {
    release,
    micro,
};

fn build_worktrees_step(
    shell: *Shell,
    step_name: []const u8,
    comptime build_step: BuildStep,
    context_baseline: *const BuildContext,
    context_worktree: *const BuildContext
) !void {
    var timer = try std.time.Timer.start();

    log.info("building {s}", .{step_name});

    const cmd = switch (build_step) {
        .release => "build -Drelease install",
        .micro => "build test:unit:build -- benchmark: ",
    };

    try shell.pushd(context_baseline.path);
    defer shell.popd();

    var child_baseline: std.process.Child = try shell.spawn_zig(.{}, cmd, .{});
    errdefer {
        _ = child_baseline.kill() catch unreachable;
    }

    try shell.pushd(context_worktree.path);
    defer shell.popd();

    var child_worktree: std.process.Child = try shell.spawn_zig(.{}, cmd, .{});
    errdefer {
        _ = child_worktree.kill() catch unreachable;
    }

    const term_baseline = try child_worktree.wait();
    const term_worktree = try child_baseline.wait();

    inline for (.{ term_baseline, term_worktree }) |term| {
        switch (term) {
            .Exited => |code| if (code != 0) return error.ExecNonZeroExitStatus,
            else => return error.ExecFailed,
        }
    }

    log.info("{s}: {}", .{ step_name, std.fmt.fmtDuration(timer.read()) });
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

fn run_micro_benchmark(shell: *Shell) !?MicroBenchmark {
    var section = try shell.open_section("micro benchmark");
    defer section.close();

    const stdout, const stderr = try shell.exec_stdout_stderr("./zig-out/bin/test-unit", .{});
    const output = try std.mem.concat(shell.arena.allocator(), u8, &.{ stdout, "\n", stderr });
    if (!has_duration_measurement(output, "total") or
        !has_duration_measurement(output, "per element"))
    {
        log.warn("skipping micro benchmark comparison; benchmark output was not found", .{});
        return null;
    }

    return .{
        .total_ns = try get_duration_measurement(output, "total"),
        .per_element_ns = try get_duration_measurement(output, "per element"),
    };
}

fn compare_benchmarks(
    baseline: Benchmarks,
    current: Benchmarks,
    epsilon_percent: u64,
) bool {
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

    if (baseline.micro != null and current.micro != null) {
        failed = compare_measurement(.{
            .name = "micro k-way total",
            .unit = "ns",
            .baseline = baseline.micro.?.total_ns,
            .current = current.micro.?.total_ns,
            .direction = .lower_is_better,
            .epsilon_percent = epsilon_percent,
        }) or failed;
        failed = compare_measurement(.{
            .name = "micro k-way per element",
            .unit = "ns",
            .baseline = baseline.micro.?.per_element_ns,
            .current = current.micro.?.per_element_ns,
            .direction = .lower_is_better,
            .epsilon_percent = epsilon_percent,
        }) or failed;
    } else if (baseline.micro == null and current.micro == null) {
        log.warn("skipping micro k-way comparison; benchmark is missing from both refs", .{});
    } else if (baseline.micro == null) {
        log.warn("skipping micro k-way comparison; benchmark is missing from baseline", .{});
    } else {
        log.warn("skipping micro k-way comparison; benchmark is missing from current", .{});
    }

    return failed;
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

fn has_duration_measurement(output: []const u8, label: []const u8) bool {
    var lines = std.mem.splitScalar(u8, output, '\n');
    while (lines.next()) |line| {
        if (!std.mem.endsWith(u8, line, label)) continue;

        _, const line_label = stdx.cut(line, " ") orelse continue;
        if (std.mem.eql(u8, line_label, label)) return true;
    }

    return false;
}

fn has_github_label(
    shell: *Shell,
    event: ?GhEvent,
    expected_label: []const u8,
) !bool {
    if (has_github_event_label(event, expected_label)) {
        return true;
    }
    if (try has_github_merge_group_label(shell, event, expected_label)) {
        return true;
    }

    return try has_github_pr_label(shell, expected_label);
}

fn merge_group_base_sha(shell: *Shell, event: ?GhEvent) !?[]const u8 {
    if (!std.mem.eql(u8, shell.env_get_option("GITHUB_EVENT_NAME") orelse "", "merge_group")) {
        return null;
    }

    const merge_group = (event orelse return null).merge_group orelse return null;
    if (merge_group.base_sha.len == 0) return null;

    return merge_group.base_sha;
}

const GhLabel = struct {
    name: []const u8 = "",
};

const GhLabeled = struct {
    labels: []const GhLabel = &.{},
};

const GhMergeGroup = struct {
    base_sha: []const u8 = "",
    head_sha: []const u8 = "",
};

const GhRepository = struct {
    full_name: []const u8 = "",
};

const GhEvent = struct {
    pull_request: ?GhLabeled = null,
    issue: ?GhLabeled = null,
    merge_group: ?GhMergeGroup = null,
    repository: ?GhRepository = null,
};

fn read_github_event(shell: *Shell) !?GhEvent {
    const event_path = shell.env_get_option("GITHUB_EVENT_PATH") orelse return null;
    const event_text = read_file_absolute_alloc(
        shell.arena.allocator(),
        event_path,
        16 * stdx.MiB,
    ) catch |err| {
        log.warn("could not read GITHUB_EVENT_PATH={s}: {}", .{ event_path, err });
        return null;
    };

    return std.json.parseFromSliceLeaky(
        GhEvent,
        shell.arena.allocator(),
        event_text,
        .{ .ignore_unknown_fields = true },
    ) catch |err| {
        log.warn("could not parse GITHUB_EVENT_PATH={s}: {}", .{ event_path, err });
        return null;
    };
}

fn has_github_event_label(event: ?GhEvent, expected_label: []const u8) bool {
    const event_unwrapped = event orelse return false;

    if (event_unwrapped.pull_request) |pull_request| {
        if (has_label_name(pull_request.labels, expected_label)) return true;
    }
    if (event_unwrapped.issue) |issue| {
        if (has_label_name(issue.labels, expected_label)) return true;
    }

    return false;
}

fn has_github_merge_group_label(
    shell: *Shell,
    event: ?GhEvent,
    expected_label: []const u8,
) !bool {
    if (!std.mem.eql(u8, shell.env_get_option("GITHUB_EVENT_NAME") orelse "", "merge_group")) {
        return false;
    }
    if (!has_github_token(shell)) {
        return false;
    }

    const event_unwrapped = event orelse return false;
    const merge_group = event_unwrapped.merge_group orelse return false;
    const repository = if (event_unwrapped.repository) |repository|
        repository.full_name
    else
        shell.env_get_option("GITHUB_REPOSITORY") orelse return false;
    if (merge_group.base_sha.len == 0 or merge_group.head_sha.len == 0 or repository.len == 0) {
        return false;
    }

    const commits_text = shell.exec_stdout(
        "git rev-list --reverse {base}..{head}",
        .{ .base = merge_group.base_sha, .head = merge_group.head_sha },
    ) catch |err| {
        log.warn("could not enumerate merge_group commits: {}", .{err});
        return false;
    };

    var pull_requests_seen = std.AutoHashMap(u32, void).init(shell.arena.allocator());
    var commits = std.mem.splitScalar(u8, commits_text, '\n');
    while (commits.next()) |commit| {
        if (commit.len == 0) continue;

        const pull_requests_text = shell.exec_stdout(
            "gh api {endpoint} --jq {query}",
            .{
                .endpoint = try shell.fmt(
                    "repos/{s}/commits/{s}/pulls",
                    .{ repository, commit },
                ),
                .query = ".[].number",
            },
        ) catch |err| {
            log.warn("could not resolve pull requests for commit {s}: {}", .{ commit, err });
            continue;
        };

        var pull_requests = std.mem.splitScalar(u8, pull_requests_text, '\n');
        while (pull_requests.next()) |pull_request| {
            if (pull_request.len == 0) continue;
            const pull_request_number = std.fmt.parseInt(u32, pull_request, 10) catch |err| {
                log.warn("could not parse pull request number '{s}': {}", .{ pull_request, err });
                continue;
            };
            if (pull_requests_seen.contains(pull_request_number)) continue;
            try pull_requests_seen.put(pull_request_number, {});

            if (try has_github_pr_number_label(shell, pull_request_number, expected_label)) {
                return true;
            }
        }
    }

    return false;
}

fn has_github_pr_label(shell: *Shell, expected_label: []const u8) !bool {
    if (!has_github_token(shell)) {
        return false;
    }

    const labels_text = if (shell.env_get_option("GITHUB_HEAD_REF")) |head_ref|
        shell.exec_stdout(
            "gh pr view {head_ref} --json labels --jq {query}",
            .{ .head_ref = head_ref, .query = ".labels[].name" },
        )
    else
        shell.exec_stdout(
            "gh pr view --json labels --jq {query}",
            .{ .query = ".labels[].name" },
        );

    const labels = labels_text catch |err| {
        log.warn("could not read GitHub PR labels with gh: {}", .{err});
        return false;
    };

    return has_label_line(labels, expected_label);
}

fn has_github_pr_number_label(
    shell: *Shell,
    pull_request_number: u32,
    expected_label: []const u8,
) !bool {
    const labels = shell.exec_stdout(
        "gh pr view {pull_request_number} --json labels --jq {query}",
        .{ .pull_request_number = pull_request_number, .query = ".labels[].name" },
    ) catch |err| {
        log.warn("could not read labels for pull request #{}: {}", .{ pull_request_number, err });
        return false;
    };

    return has_label_line(labels, expected_label);
}

fn has_label_line(labels: []const u8, expected_label: []const u8) bool {
    var lines = std.mem.splitScalar(u8, labels, '\n');
    while (lines.next()) |label| {
        if (std.mem.eql(u8, label, expected_label)) return true;
    }

    return false;
}

fn has_github_token(shell: *Shell) bool {
    return shell.env_get_option("GH_TOKEN") != null or
        shell.env_get_option("GITHUB_TOKEN") != null;
}

fn has_label_name(labels: []const GhLabel, expected_label: []const u8) bool {
    for (labels) |label| {
        if (std.mem.eql(u8, label.name, expected_label)) return true;
    }

    return false;
}

fn read_file_absolute_alloc(
    allocator: std.mem.Allocator,
    path: []const u8,
    max_bytes: usize,
) ![]const u8 {
    if (!std.fs.path.isAbsolute(path)) return error.PathNotAbsolute;

    const file = try std.fs.openFileAbsolute(path, .{});
    defer file.close();

    return try file.readToEndAlloc(allocator, max_bytes);
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
