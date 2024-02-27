/// Runs a set of macro-benchmarks whose result is displayed at <https://tigerbeetle.github.io>.
///
/// Specifically:
///
/// - This script is run by the CI infrastructure on every merge to main.
/// - It runs a set of "benchmarks", where a "benchmark" can be anything (eg, measuring the size of
///   the binary).
/// - The results of all measurements are serialized as a single JSON object, `Run`.
/// - The key part: this JSON is then stored in a "distributed database" for our visualization
///   front-end to pick up. This "database" is just a newline-delimited JSON file in a git repo
const std = @import("std");

const stdx = @import("../stdx.zig");
const Shell = @import("../shell.zig");

const log = std.log;

pub const CliArgs = struct {
    sha: []const u8,
};

pub fn main(shell: *Shell, gpa: std.mem.Allocator, cli_args: CliArgs) !void {
    _ = gpa;

    const commit_timestamp_str =
        try shell.exec_stdout("git show -s --format=%ct {sha}", .{ .sha = cli_args.sha });
    const commit_timestamp = try std.fmt.parseInt(u64, commit_timestamp_str, 10);

    var timer = try std.time.Timer.start();
    try shell.zig("build -Drelease -Dconfig=production install", .{});
    const build_time_ms = timer.lap() / std.time.ns_per_ms;

    const executable_size_bytes = (try shell.cwd.statFile("tigerbeetle")).size;

    const benchmark_result = try shell.exec_stdout("./tigerbeetle benchmark", .{});
    const tps = try get_measurement(benchmark_result, "load accepted", "tx/s");
    const batch_p90_ms = try get_measurement(benchmark_result, "batch latency p90", "ms");
    const query_p90_ms = try get_measurement(benchmark_result, "query latency p90", "ms");
    const rss_bytes = try get_measurement(benchmark_result, "rss", "bytes");

    try upload_run(shell, Run{
        // Use commit timestamp, rather wall clock time here. That way, it is possible to re-bench
        // mark the entire history while getting a comparable time series.
        .timestamp = commit_timestamp,
        .revision = cli_args.sha,
        .measurements = &[_]Measurement{
            .{ .label = "build time", .value = build_time_ms, .unit = "ms" },
            .{ .label = "executable size", .value = executable_size_bytes, .unit = "bytes" },
            .{ .label = "TPS", .value = tps, .unit = "count" },
            .{ .label = "batch p90", .value = batch_p90_ms, .unit = "ms" },
            .{ .label = "query p90", .value = query_p90_ms, .unit = "ms" },
            .{ .label = "RSS", .value = rss_bytes, .unit = "bytes" },
        },
    });

    upload_nyrkio(shell, NyrkioRun{
        .timestamp = commit_timestamp,
        .attributes = .{
            .git_repo = "https://github.com/tigerbeetle/tigerbeetle",
            .git_commit = cli_args.sha,
            .branch = "main",
        },
        .metrics = &[_]NyrkioRun.Metric{
            .{ .name = "build time", .value = build_time_ms, .unit = "ms" },
            .{ .name = "executable size", .value = executable_size_bytes, .unit = "bytes" },
            .{ .name = "TPS", .value = tps, .unit = "count" },
            .{ .name = "batch p90", .value = batch_p90_ms, .unit = "ms" },
            .{ .name = "query p90", .value = query_p90_ms, .unit = "ms" },
            .{ .name = "RSS", .value = rss_bytes, .unit = "bytes" },
        },
    }) catch |err| {
        log.err("failed to upload Nyrkiö metrics: {}", .{err});
    };
}

fn get_measurement(
    benchmark_stdout: []const u8,
    comptime label: []const u8,
    comptime unit: []const u8,
) !u64 {
    errdefer {
        std.log.err("can't extract '" ++ label ++ "' measurement", .{});
    }

    var cut = stdx.cut(benchmark_stdout, label ++ " = ") orelse return error.BadMeasurement;
    cut = stdx.cut(cut.suffix, " " ++ unit) orelse return error.BadMeasurement;

    return try std.fmt.parseInt(u64, cut.prefix, 10);
}

const Measurement = struct {
    label: []const u8,
    value: u64,
    unit: []const u8,
};

const Run = struct {
    timestamp: u64,
    revision: []const u8,
    measurements: []const Measurement,
};

fn upload_run(shell: *Shell, run: Run) !void {
    const token = try shell.env_get("DEVHUBDB_PAT");
    try shell.exec(
        \\git clone --depth 1
        \\  https://oauth2:{token}@github.com/tigerbeetle/devhubdb.git
        \\  devhubdb
    , .{
        .token = token,
    });

    try shell.pushd("./devhubdb");
    defer shell.popd();

    {
        const file = try shell.cwd.openFile("./devhub/data.json", .{
            .mode = .write_only,
        });
        defer file.close();

        try file.seekFromEnd(0);
        try std.json.stringify(run, .{}, file.writer());
        try file.writeAll("\n");
    }

    try shell.exec("git add ./devhub/data.json", .{});
    try shell.git_env_setup();
    try shell.exec("git commit -m 📈", .{});
    try shell.exec("git push", .{});
}

const NyrkioRun = struct {
    const Metric = struct {
        name: []const u8,
        unit: []const u8,
        value: u64,
    };
    timestamp: u64,
    metrics: []const Metric,
    attributes: struct {
        git_repo: []const u8,
        branch: []const u8,
        git_commit: []const u8,
    },
};

fn upload_nyrkio(shell: *Shell, run: NyrkioRun) !void {
    const token = try shell.env_get("NYRKIO_TOKEN");
    const payload = try std.json.stringifyAlloc(shell.arena.allocator(), [_]NyrkioRun{run}, .{});
    try shell.exec(
        \\curl -s -X POST --fail-with-body
        \\    -H {content_type}
        \\    -H {authorization}
        \\    https://nyrkio.com/api/v0/result/devhub
        \\    -d {payload}
    , .{
        .content_type = "Content-type: application/json",
        .authorization = try shell.print("Authorization: Bearer {s}", .{token}),
        .payload = payload,
    });
}
