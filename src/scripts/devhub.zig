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
const changelog = @import("./changelog.zig");
const Release = @import("../multiversion.zig").Release;

const MiB = stdx.MiB;

const log = std.log;

pub const CLIArgs = struct {
    sha: []const u8,
    skip_kcov: bool = false,
};

pub fn main(shell: *Shell, _: std.mem.Allocator, cli_args: CLIArgs) !void {
    try devhub_metrics(shell, cli_args);

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

fn devhub_metrics(shell: *Shell, cli_args: CLIArgs) !void {
    var section = try shell.open_section("metrics");
    defer section.close();

    const commit_timestamp_str =
        try shell.exec_stdout("git show -s --format=%ct {sha}", .{ .sha = cli_args.sha });
    const commit_timestamp = try std.fmt.parseInt(u64, commit_timestamp_str, 10);

    // Only build the TigerBeetle binary to test build speed and build size. Throw it away once
    // done, and use a release build from `zig-out/dist/` to run the benchmark.
    var timer = try std.time.Timer.start();

    const build_time_debug_ms = blk: {
        timer.reset();
        try shell.exec_zig("build install", .{});
        defer shell.project_root.deleteFile("tigerbeetle") catch unreachable;

        break :blk timer.read() / std.time.ns_per_ms;
    };

    const build_time_ms, const executable_size_bytes = blk: {
        timer.reset();
        try shell.project_root.deleteTree(".zig-cache");
        try shell.exec_zig("build -Drelease install", .{});
        defer shell.project_root.deleteFile("tigerbeetle") catch unreachable;

        break :blk .{
            timer.lap() / std.time.ns_per_ms,
            (try shell.cwd.statFile("tigerbeetle")).size,
        };
    };

    // When doing a release, the latest release in the changelog on main will be newer than the
    // latest release on GitHub. In this case, don't pass in --no-changelog - as doing that causes
    // the release code to try and look for a version which doesn't yet exist!
    const no_changelog_flag = blk: {
        const changelog_text = try shell.project_root.readFileAlloc(
            shell.arena.allocator(),
            "CHANGELOG.md",
            1 * MiB,
        );
        var changelog_iteratator = changelog.ChangelogIterator.init(changelog_text);

        const last_release_changelog = changelog_iteratator.next_changelog().?.release orelse
            break :blk true;
        const last_release_published = try Release.parse(try shell.exec_stdout(
            "gh release list --json tagName --jq {query} --limit 1",
            .{ .query = ".[].tagName" },
        ));

        if (Release.less_than({}, last_release_published, last_release_changelog)) {
            break :blk false;
        } else {
            break :blk true;
        }
    };

    if (no_changelog_flag) {
        try shell.exec_zig(
            \\build scripts -- release --build --no-changelog --sha={sha}
            \\    --language=zig --devhub
        , .{ .sha = cli_args.sha });
    } else {
        try shell.exec_zig(
            \\build scripts -- release --build --sha={sha}
            \\    --language=zig --devhub
        , .{ .sha = cli_args.sha });
    }
    try shell.project_root.deleteFile("tigerbeetle");

    try shell.unzip_executable(
        "zig-out/dist/tigerbeetle/tigerbeetle-x86_64-linux.zip",
        "tigerbeetle",
    );

    // `--log-debug-replica` is explicitly enabled, to measure the performance hit from debug
    // logging and count the log lines.
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

    const ci_pipeline_duration_s: ?u64 = blk: {
        const times_gh = try shell.exec_stdout("gh run list -c {sha} -e merge_group " ++
            "--json startedAt,updatedAt -L 1 --template {template}", .{
            .sha = cli_args.sha,
            .template = "{{range .}}{{.startedAt}} {{.updatedAt}}{{end}}",
        });
        const iso8601_started_at, const iso8601_updated_at =
            stdx.cut(times_gh, " ") orelse break :blk null;

        const epoch_started_at = try shell.iso8601_to_timestamp_seconds(iso8601_started_at);
        const epoch_updated_at = try shell.iso8601_to_timestamp_seconds(iso8601_updated_at);

        break :blk epoch_updated_at - epoch_started_at;
    } orelse blk: {
        // Return 0 instead of null when running locally or without DEVHUBDB_PAT set - the results
        // won't be uploaded, and this allows the rest of the code to succeed.
        if ((shell.env_get("DEVHUBDB_PAT") catch null) == null) {
            break :blk 0;
        } else {
            break :blk null;
        }
    };

    const batch = MetricBatch{
        .timestamp = commit_timestamp,
        .attributes = .{
            .git_repo = "https://github.com/tigerbeetle/tigerbeetle",
            .git_commit = cli_args.sha,
            .branch = "main",
        },
        .metrics = &[_]Metric{
            .{ .name = "ci pipeline duration", .value = ci_pipeline_duration_s.?, .unit = "s" },
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

    upload_run(shell, &batch) catch |err| {
        log.err("failed to upload devhubdb metrics: {}", .{err});
    };

    upload_nyrkio(shell, &batch) catch |err| {
        log.err("failed to upload Nyrkiö metrics: {}", .{err});
    };

    for (batch.metrics) |metric| {
        std.log.info("{s} = {} {s}", .{ metric.name, metric.value, metric.unit });
    }
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

fn upload_run(shell: *Shell, batch: *const MetricBatch) !void {
    const token = try shell.env_get("DEVHUBDB_PAT");
    try shell.exec(
        \\git clone --single-branch --depth 1
        \\  https://oauth2:{token}@github.com/tigerbeetle/devhubdb.git
        \\  devhubdb
    , .{
        .token = token,
    });

    try shell.pushd("./devhubdb");
    defer shell.popd();

    for (0..32) |_| {
        try shell.exec("git fetch origin main", .{});
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
        try shell.git_env_setup(.{ .use_hostname = false });
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
    const url = "https://nyrkio.com/api/v0/result/devhub";
    const token = try shell.env_get("NYRKIO_TOKEN");
    const payload = try std.json.stringifyAlloc(
        shell.arena.allocator(),
        [_]*const MetricBatch{batch}, // Nyrkiö needs an _array_ of batches.
        .{},
    );
    _ = try shell.http_post(url, payload, .{
        .content_type = .json,
        .authorization = try shell.fmt("Bearer {s}", .{token}),
    });
}
