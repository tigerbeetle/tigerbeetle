//! Runs a set of macro-benchmarks whose result is displayed at <https://devhub.tigerbeetle.com/>.
//!
//! Specifically:
//!
//! - This script is run by the CPO infrastructure.
//! - It runs the regular DevHub benchmark batch, then a long-running benchmark.
//! - The results of all measurements are serialized as a single JSON object, `Run`.
//! - The key part: this JSON is then stored in a "distributed database" for our visualization
//!   front-end to pick up. This "database" is just a newline-delimited JSON file in a git repo
//!
//! To generate a DEVHUBDB_PAT (used by cfo and CI):
//! 1. Go to https://github.com/settings/personal-access-tokens/new
//! 2. Fill out token name (e.g. "cfo/ci devhubdb token").
//! 3. Resource owner: "tigerbeetle"
//! 4. Expiry: "366 days" (maximum available)
//! 5. Repository access: "Only select repositories"
//! 6. Select repositories: "tigerbeetle/devhubdb"
//! 7. Add permissions: "Metadata"
//! 8. Add permissions: "Contents"; Access: "Read and write".
//! 9. "Generate token".
//! 10. (Copy token.)
//!
//! To update token in TigerBeetle CI:
//! 1. https://github.com/tigerbeetle/tigerbeetle/settings/environments
//! 2. Click "devhub".
//! 3. Environment Secrets > Edit DEVHUBDB_PAT
//! 4. Paste token; "Update secret"
//!
//! (Also need to update the DEVHUBDB_PAT environment variable passed to the CFO supervisors.)
const std = @import("std");
const assert = std.debug.assert;

const stdx = @import("stdx");
const Shell = @import("../shell.zig");
const Release = @import("../multiversion.zig").Release;

const log = std.log;

pub const CLIArgs = struct {
    sha: ?[]const u8 = null,
    skip_kcov: bool = false,
};

pub fn main(shell: *Shell, _: std.mem.Allocator, cli_args: CLIArgs) !void {
    const sha = cli_args.sha orelse try shell.exec_stdout("git rev-parse HEAD", .{});
    try cpo_metrics(shell, sha);

    if (!cli_args.skip_kcov) {
        try devhub_coverage(shell);
    } else {
        log.info("--skip-kcov enabled, not computing coverage.", .{});
    }
}

fn devhub_coverage(shell: *Shell) !void {
    var section = try shell.open_section("coverage");
    defer section.close();

    const kcov_version = shell.exec_stdout("kcov --version", .{}) catch {
        return error.NoKcov;
    };
    log.info("kcov version {s}", .{kcov_version});

    try shell.exec_zig("build test:unit:build", .{});
    try shell.exec_zig("build vopr:build", .{});
    try shell.exec_zig("build fuzz:build", .{});

    // Put results into src/devhub, as that folder is deployed as GitHub pages.
    try shell.project_root.deleteTree("./src/devhub/coverage");
    try shell.project_root.makePath("./src/devhub/coverage");

    const kcov: []const []const u8 = &.{ "kcov", "--include-path=./src", "./src/devhub/coverage" };
    inline for (.{
        "{kcov} ./zig-out/bin/test-unit",
        "{kcov} ./zig-out/bin/fuzz --events-max=500000 lsm_tree 92",
        "{kcov} ./zig-out/bin/fuzz --events-max=500000 lsm_forest 92",
        "{kcov} ./zig-out/bin/vopr 92",
    }) |command| {
        try shell.exec(command, .{ .kcov = kcov });
    }

    var coverage_dir = try shell.cwd.openDir("./src/devhub/coverage", .{ .iterate = true });
    defer coverage_dir.close();

    // kcov adds some symlinks to the output, which prevents upload to GitHub actions from working.
    var it = coverage_dir.iterate();
    while (try it.next()) |entry| {
        if (entry.kind == .sym_link) {
            try coverage_dir.deleteFile(entry.name);
        }
    }
}

fn cpo_metrics(shell: *Shell, sha: []const u8) !void {
    var section = try shell.open_section("metrics");
    defer section.close();

    const commit_timestamp_str =
        try shell.exec_stdout("git show -s --format=%ct {sha}", .{ .sha = sha });
    const commit_timestamp = try std.fmt.parseInt(u64, commit_timestamp_str, 10);

    try cpo_short_benchmark(shell, sha, commit_timestamp);

    try cpo_long_benchmark(shell, sha, commit_timestamp);
}

fn cpo_short_benchmark(shell: *Shell, sha: []const u8, commit_timestamp: u64) !void {

    // Only build the TigerBeetle binary to test build speed and build size. Keep the release build
    // to run both cpo benchmarks.
    var timer = try std.time.Timer.start();
    const build_time_debug_ms = blk: {
        timer.reset();
        try shell.exec_zig("build install", .{});
        defer shell.project_root.deleteFile("tigerbeetle") catch unreachable;

        break :blk timer.read() / std.time.ns_per_ms;
    };

    const build_time_ms, const executable_size_bytes = blk: {
        timer.reset();
        try shell.project_root.deleteTree(".zig-cache/tmp/devhub_cache");
        try shell.exec_zig("build -Drelease install", .{});

        break :blk .{
            timer.lap() / std.time.ns_per_ms,
            (try shell.cwd.statFile("tigerbeetle")).size,
        };
    };
    defer shell.project_root.deleteFile("tigerbeetle") catch {};
    // `--log-debug-replica` is explicitly enabled, to measure the performance hit from debug
    // logging and count the log lines.
    // TODO: make useful benchmark (tbid etc.) and remove performance.
    const benchmark_result, const benchmark_stderr = try shell.exec_stdout_stderr(
        "./tigerbeetle benchmark --validate --checksum-performance --log-debug-replica " ++
            "--file=datafile-devhub",
        .{},
    );

    const integrity_time_ms = blk: {
        timer.reset();

        try shell.exec(
            "./tigerbeetle inspect integrity datafile-devhub",
            .{},
        );

        break :blk timer.read() / std.time.ns_per_ms;
    };

    shell.cwd.deleteFile("datafile-devhub") catch unreachable;

    const replica_log_lines = std.mem.count(u8, benchmark_stderr, "\n");
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
    const format_time_ms = blk: {
        timer.reset();

        try shell.exec(
            "./tigerbeetle format --cluster=0 --replica=0 --replica-count=1 datafile-devhub",
            .{},
        );

        break :blk timer.read() / std.time.ns_per_ms;
    };
    defer shell.cwd.deleteFile("datafile-devhub") catch unreachable;

    const stats_count = blk: {
        const stats_inspect_result = try shell.exec_stdout("./tigerbeetle inspect metrics", .{});
        var stats_count: u32 = 0;
        var lines = std.mem.splitScalar(
            u8,
            stats_inspect_result,
            '\n',
        );
        while (lines.next()) |line| {
            // line looks like
            // timing: compact_mutable_suffix(tree)=136
            if (line.len != 0) {
                _, const value_string = stdx.cut(line, "=").?;
                stats_count += try std.fmt.parseInt(u32, value_string, 10);
            }
        }
        break :blk stats_count;
    };

    const startup_time_ms, const repl_single_command_ms = blk: {
        timer.reset();

        var process = try shell.spawn(
            .{
                .stdin_behavior = .Pipe,
                .stdout_behavior = .Pipe,
                .stderr_behavior = .Ignore,
            },
            "./tigerbeetle start --addresses=0 --cache-grid=8GiB datafile-devhub",
            .{},
        );

        defer {
            process.stdin.?.close();
            process.stdin = null;
            _ = process.wait() catch {};
        }

        var port_buffer: [std.fmt.count("{}\n", .{std.math.maxInt(u16)})]u8 = undefined;
        const port_buffer_len = try process.stdout.?.readAll(&port_buffer);
        const port = try std.fmt.parseInt(u16, port_buffer[0 .. port_buffer_len - 1], 10);

        // TODO: This sends a ping manually; once register connection speed has been fixed, this can
        // use the benchmark or repl via CLI.
        //
        // Use Header directly with a blocking TCP connection here, to avoid pulling in half of TB.
        const Header = @import("../vsr/message_header.zig").Header;

        var ping = Header.PingClient{
            .command = .ping_client,
            .cluster = 0,
            .release = Release.minimum,
            .client = 1,
            .ping_timestamp_monotonic = 0,
        };
        ping.set_checksum_body(&[0]u8{});
        ping.set_checksum();

        // The release of the built binary is not readily available, since it's set by
        // `zig build scripts -- release`. Instead, the ping above is sent with
        // .release == Release.minimum. This will always be below a release build's
        // release_client_min, so expect the eviction.
        var eviction: Header.Eviction = undefined;

        const peer = try std.net.Address.parseIp4("127.0.0.1", port);
        const stream = try std.net.tcpConnectToAddress(peer);
        defer stream.close();

        var writer = stream.writer();
        try writer.writeAll(std.mem.asBytes(&ping)[0..@sizeOf(Header)]);

        const reader = stream.reader();
        _ = try reader.readAll(std.mem.asBytes(&eviction)[0..@sizeOf(Header)]);

        assert(eviction.command == .eviction);
        assert(eviction.valid_checksum());
        assert(eviction.valid_checksum_body(&[0]u8{}));

        const startup_time_ms = timer.read() / std.time.ns_per_ms;

        // While there's a running instance, check how long the repl takes to connect and run a
        // command.
        timer.reset();

        try shell.exec(
            "./tigerbeetle repl --addresses={port} --cluster=0 --command={command}",
            .{ .port = port, .command = "create_accounts id=1 ledger=1 code=1" },
        );

        const repl_single_command_ms = timer.read() / std.time.ns_per_ms;

        break :blk .{ startup_time_ms, repl_single_command_ms };
    };

    const batch = MetricBatch{
        .timestamp = commit_timestamp,
        .attributes = .{
            .git_repo = "https://github.com/tigerbeetle/tigerbeetle",
            .git_commit = sha,
            .branch = "main",
        },
        .metrics = &[_]Metric{
            .{ .name = "executable size", .value = executable_size_bytes, .unit = "bytes" },
            .{ .name = "TPS", .value = tps, .unit = "count" },
            .{ .name = "batch p100", .value = batch_p100_ms, .unit = "ms" },
            .{ .name = "query p100", .value = query_p100_ms, .unit = "ms" },
            .{ .name = "RSS", .value = rss_bytes, .unit = "bytes" },
            .{ .name = "datafile", .value = datafile_bytes, .unit = "bytes" },
            .{ .name = "datafile empty", .value = datafile_empty_bytes, .unit = "bytes" },
            .{ .name = "replica log lines", .value = replica_log_lines, .unit = "count" },
            .{
                .name = "checksum(message_size_max)",
                .value = checksum_message_size_max_us,
                .unit = "us",
            },
            .{ .name = "build time debug", .value = build_time_debug_ms, .unit = "ms" },
            .{ .name = "build time", .value = build_time_ms, .unit = "ms" },
            .{ .name = "format time", .value = format_time_ms, .unit = "ms" },
            .{ .name = "startup time - 8GiB grid cache", .value = startup_time_ms, .unit = "ms" },
            .{ .name = "stats count", .value = stats_count, .unit = "count" },
            .{ .name = "repl single command", .value = repl_single_command_ms, .unit = "ms" },
            .{ .name = "inspect integrity time", .value = integrity_time_ms, .unit = "ms" },
        },
    };

    for (batch.metrics) |metric| {
        log.info("{s} = {} {s}", .{ metric.name, metric.value, metric.unit });
    }

    upload_run(shell, &batch, "./short/data.json") catch |err| {
        log.err("failed to upload devhubdb metrics: {}", .{err});
    };
}

fn cpo_long_benchmark(shell: *Shell, sha: []const u8, commit_timestamp: u64) !void {
    var section = try shell.open_section("long benchmark");
    defer section.close();

    const block_device = "/dev/md0";

    // overwrite the first 96 bytes for block device
    try shell.exec(
        \\dd if=/dev/zero of={block_device} bs=1K count=96
    , .{ .block_device = block_device });

    try shell.project_root.deleteTree(".zig-cache/tmp/devhub_cache");
    try shell.exec_zig("build -Drelease install", .{});
    defer shell.project_root.deleteFile("tigerbeetle") catch {};

    const benchmark_result = try shell.exec_stdout(
        "./tigerbeetle benchmark --transfer-count=1_000_000 --file={block_device}",
        .{ .block_device = block_device },
    );

    // TODO: track more metrics here.
    const batch_p100_ms = try get_measurement(benchmark_result, "batch latency p100", "ms");
    const tps = try get_measurement(benchmark_result, "load accepted", "tx/s");

    const batch = MetricBatch{
        .timestamp = commit_timestamp,
        .attributes = .{
            .git_repo = "https://github.com/tigerbeetle/tigerbeetle",
            .git_commit = sha,
            .branch = "main",
        },
        .metrics = &[_]Metric{
            .{ .name = "long TPS", .value = tps, .unit = "count" },
            .{ .name = "long batch p100", .value = batch_p100_ms, .unit = "ms" },
        },
    };

    for (batch.metrics) |metric| {
        log.info("{s} = {} {s}", .{ metric.name, metric.value, metric.unit });
    }

    upload_run(shell, &batch, "./long/data.json") catch |err| {
        log.err("failed to upload long benchmark devhubdb metrics: {}", .{err});
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

    _, const rest = stdx.cut(benchmark_stdout, label ++ " = ") orelse
        return error.BadMeasurement;
    const value_string, _ = stdx.cut(rest, " " ++ unit) orelse return error.BadMeasurement;

    return try std.fmt.parseInt(u64, value_string, 10);
}

fn upload_run(shell: *Shell, batch: *const MetricBatch, data_file: []const u8) !void {
    const token = shell.env_get_option("DEVHUBDB_PAT");
    try shell.cwd.deleteTree("./devhubdb");
    try shell.exec(
        \\git clone --single-branch --depth 1
        \\  https://oauth2:{token}@github.com/tigerbeetle/devhubdb-cpo.git
        \\  devhubdb
    , .{
        .token = token orelse "",
    });

    try shell.pushd("./devhubdb");
    defer shell.popd();

    for (0..32) |_| {
        try shell.exec("git fetch origin main", .{});
        try shell.exec("git reset --hard origin/main", .{});

        {
            const file = try shell.cwd.openFile(data_file, .{
                .mode = .write_only,
            });
            defer file.close();

            try file.seekFromEnd(0);
            try std.json.stringify(batch, .{}, file.writer());
            try file.writeAll("\n");
        }

        try shell.exec("git add {data_file}", .{ .data_file = data_file });
        try shell.git_env_setup(.{ .use_hostname = false });
        try shell.exec("git commit -m 📈", .{});
        if (token) |_| {
            if (shell.exec("git push", .{})) {
                log.info("metrics uploaded", .{});
                break;
            } else |_| {
                log.info("conflict, retrying", .{});
            }
        } else {
            return error.NoToken;
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
