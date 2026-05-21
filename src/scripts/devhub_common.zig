const std = @import("std");

const stdx = @import("stdx");
const Shell = @import("../shell.zig");

const log = std.log;

pub fn devhub_coverage(shell: *Shell) !void {
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

pub fn get_measurement(
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

pub fn benchmark_metric(
    benchmark_stdout: []const u8,
    comptime name: []const u8,
    comptime label: []const u8,
    comptime benchmark_unit: []const u8,
    comptime metric_unit: []const u8,
) !Metric {
    return .{
        .name = name,
        .value = try get_measurement(benchmark_stdout, label, benchmark_unit),
        .unit = metric_unit,
    };
}

pub fn timer_ms(timer: *std.time.Timer) u64 {
    return (stdx.Duration{ .ns = timer.read() }).to_ms();
}

pub fn log_metrics(batch: *const MetricBatch) void {
    for (batch.metrics) |metric| {
        log.info("{s} = {} {s}", .{ metric.name, metric.value, metric.unit });
    }
}

pub fn upload_run(
    shell: *Shell,
    batch: *const MetricBatch,
    options: struct {
        repo: []const u8,
        data_file: []const u8,
    },
) !void {
    const token = shell.env_get_option("DEVHUBDB_PAT");
    try shell.cwd.deleteTree("./devhubdb");
    try shell.exec(
        \\git clone --single-branch --depth 1
        \\  https://oauth2:{token}@github.com/tigerbeetle/{repo}.git
        \\  devhubdb
    , .{
        .token = token orelse "",
        .repo = options.repo,
    });

    try shell.pushd("./devhubdb");
    defer shell.popd();

    for (0..32) |_| {
        try shell.exec("git fetch origin main", .{});
        try shell.exec("git reset --hard origin/main", .{});

        {
            const file = try shell.cwd.openFile(options.data_file, .{
                .mode = .write_only,
            });
            defer file.close();

            try file.seekFromEnd(0);
            try std.json.stringify(batch, .{}, file.writer());
            try file.writeAll("\n");
        }

        try shell.exec("git add {data_file}", .{ .data_file = options.data_file });
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

pub const Metric = struct {
    name: []const u8,
    unit: []const u8,
    value: u64,
};

pub const MetricBatch = struct {
    timestamp: u64,
    metrics: []const Metric,
    attributes: struct {
        git_repo: []const u8,
        branch: []const u8,
        git_commit: []const u8,
    },
};
