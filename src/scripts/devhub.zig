//! Runs a set of macro-benchmarks whose result is displayed at <https://tigerbeetle.github.io>.
//!
//! Specifically:
//!
//! - This script is run by the CI infrastructure on every merge to main.
//! - It runs a set of "benchmarks", where a "benchmark" can be anything (eg, measuring the size of
//!   the binary).
//! - The results of all measurements are serialized as a single JSON object, `Run`.
//! - The key part: this JSON is then stored in a "distributed database" for our visualization
//!   front-end to pick up. This "database" is just a newline-delimited JSON file in a git repo
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

    // Only build the TigerBeetle binary to test build speed and build size. Throw it away once
    // done, and use a release build from `dist/` to run the benchmark.
    var timer = try std.time.Timer.start();
    try shell.zig("build -Drelease -Dconfig=production install", .{});
    const build_time_ms = timer.lap() / std.time.ns_per_ms;
    const executable_size_bytes = (try shell.cwd.statFile("tigerbeetle")).size;
    try shell.project_root.deleteFile("tigerbeetle");

    try shell.zig(
        \\build scripts -- release --build --run-number=192 --sha={sha}
        \\    --language=zig
    , .{ .sha = cli_args.sha });
    try shell.exec("unzip dist/tigerbeetle/tigerbeetle-x86_64-linux.zip", .{});

    const benchmark_result = try shell.exec_stdout(
        "./tigerbeetle benchmark --validate --checksum-performance",
        .{},
    );
    const tps = try get_measurement(benchmark_result, "load accepted", "tx/s");
    const batch_p100_ms = try get_measurement(benchmark_result, "batch latency p100", "ms");
    const query_p100_ms = try get_measurement(benchmark_result, "query latency p100", "ms");
    const rss_bytes = try get_measurement(benchmark_result, "rss", "bytes");
    const datafile_bytes = try get_measurement(benchmark_result, "datafile", "bytes");
    const datafile_empty_bytes = try get_measurement(benchmark_result, "datafile empty", "bytes");
    const checksum_message_size_max_us = try get_measurement(
        benchmark_result,
        "checksum message size max",
        "us",
    );

    const batch = MetricBatch{
        .timestamp = commit_timestamp,
        .attributes = .{
            .git_repo = "https://github.com/tigerbeetle/tigerbeetle",
            .git_commit = cli_args.sha,
            .branch = "main",
        },
        .metrics = &[_]Metric{
            .{ .name = "build time", .value = build_time_ms, .unit = "ms" },
            .{ .name = "executable size", .value = executable_size_bytes, .unit = "bytes" },
            .{ .name = "TPS", .value = tps, .unit = "count" },
            .{ .name = "batch p100", .value = batch_p100_ms, .unit = "ms" },
            .{ .name = "query p100", .value = query_p100_ms, .unit = "ms" },
            .{ .name = "RSS", .value = rss_bytes, .unit = "bytes" },
            .{ .name = "datafile", .value = datafile_bytes, .unit = "bytes" },
            .{ .name = "datafile empty", .value = datafile_empty_bytes, .unit = "bytes" },
            .{
                .name = "checksum(message_size_max)",
                .value = checksum_message_size_max_us,
                .unit = "us",
            },
        },
    };

    try upload_run(shell, &batch);

    upload_nyrkio(shell, &batch) catch |err| {
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

fn upload_run(shell: *Shell, batch: *const MetricBatch) !void {
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

    for (0..32) |_| {
        try shell.exec("git fetch origin", .{});
        try shell.exec("git reset --hard origin/main", .{});

        {
            const file = try shell.cwd.openFile("./devhub/data.json", .{
                .mode = .write_only,
            });
            defer file.close();

            try file.seekFromEnd(0);
            try std.json.stringify(batch, .{}, file.writer());
            try file.writeAll("\n");
        }

        try shell.exec("git add ./devhub/data.json", .{});
        try shell.git_env_setup();
        try shell.exec("git commit -m 📈", .{});
        if (shell.exec("git push", .{})) {
            log.info("metrics uploaded", .{});
            break;
        } else |_| {
            log.info("conflict, retrying", .{});
        }
    } else {
        log.err("can't push new data to devhub", .{});
        return error.CanNotPush;
    }
}

const Metric = struct {
    name: []const u8,
    unit: []const u8,
    value: u64,
};

const MetricBatch = struct {
    timestamp: u64,
    metrics: []const Metric,
    attributes: struct {
        git_repo: []const u8,
        branch: []const u8,
        git_commit: []const u8,
    },
};

fn upload_nyrkio(shell: *Shell, batch: *const MetricBatch) !void {
    const token = try shell.env_get("NYRKIO_TOKEN");
    const payload = try std.json.stringifyAlloc(
        shell.arena.allocator(),
        [_]*const MetricBatch{batch}, // Nyrkiö needs an _array_ of batches.
        .{},
    );
    try shell.exec(
        \\curl -s -X POST --fail-with-body
        \\    -H {content_type}
        \\    -H {authorization}
        \\    https://nyrkio.com/api/v0/result/devhub
        \\    -d {payload}
    , .{
        .content_type = "Content-type: application/json",
        .authorization = try shell.fmt("Authorization: Bearer {s}", .{token}),
        .payload = payload,
    });
}
