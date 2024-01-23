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

const Shell = @import("../shell.zig");

pub const CliArgs = struct {
    sha: []const u8,
};

pub fn main(shell: *Shell, gpa: std.mem.Allocator, cli_args: CliArgs) !void {
    _ = gpa;

    var timer = try std.time.Timer.start();
    try shell.zig("build install -Drelease", .{});
    const build_time_ms = timer.lap() / std.time.ns_per_ms;

    const executable_size_bytes = (try shell.cwd.statFile("tigerbeetle")).size;

    const run = Run{
        .timestamp = std.time.timestamp(),
        .revision = cli_args.sha,
        .measurements = &[_]Measurement{
            .{ .label = "build time", .value = build_time_ms, .unit = "ms" },
            .{ .label = "executable size", .value = executable_size_bytes, .unit = "bytes" },
        },
    };

    const token = try shell.env_get("WORKBENCHDB_PAT");
    try shell.exec(
        \\git clone --depth 1
        \\  https://oauth2:{token}@github.com/tigerbeetle/workbenchdb.git
        \\  workbenchdb
    , .{
        .token = token,
    });

    try shell.pushd("./workbenchdb");
    defer shell.popd();

    {
        const file = try shell.cwd.openFile("./workbench/data.json", .{
            .mode = .write_only,
        });
        defer file.close();

        try file.seekFromEnd(0);
        try std.json.stringify(run, .{}, file.writer());
        try file.writeAll("\n");
    }

    try shell.exec("git add ./workbench/data.json", .{});
    try shell.git_env_setup();
    try shell.exec("git commit -m 📈", .{});
    try shell.exec("git push", .{});
}

const Measurement = struct {
    label: []const u8,
    value: u64,
    unit: []const u8,
};

const Run = struct {
    timestamp: i64,
    revision: []const u8,
    measurements: []const Measurement,
};
