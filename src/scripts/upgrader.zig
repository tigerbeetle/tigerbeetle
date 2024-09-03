/// Smoke test for upgrade procedure:
/// - given two multiversion tigerbeetle binaries, old and new
/// - format three data files using old
/// - spawn a benchmark load using old against a local cluster
/// - spawn three replicas at old
/// - for some time in a loop, crash, restart, or upgrade a random replica; then
/// - crash all replicas and restart them at the new version
/// - join the benchmark load, to make sure it exits with zero.
const std = @import("std");
const builtin = @import("builtin");
const log = std.log;
const assert = std.debug.assert;

const stdx = @import("../stdx.zig");
const flags = @import("../flags.zig");
const fatal = flags.fatal;
const Shell = @import("../shell.zig");

pub const CLIArgs = struct {
    old: []const u8,
    new: []const u8,
};

const replica_count = 3;

pub fn main(shell: *Shell, gpa: std.mem.Allocator, cli_args: CLIArgs) !void {
    _ = gpa;

    const seed = std.crypto.random.int(u64);
    log.info("seed = {}", .{seed});
    var rng = std.rand.DefaultPrng.init(seed);
    const random = rng.random();

    try shell.exec("{tigerbeetle} version", .{ .tigerbeetle = cli_args.old });
    try shell.exec("{tigerbeetle} version", .{ .tigerbeetle = cli_args.new });

    shell.project_root.deleteTree(".zig-cache/upgrader") catch {};
    try shell.project_root.makePath(".zig-cache/upgrader");

    for (0..replica_count) |replica_index| {
        try shell.exec(
            \\{tigerbeetle} format --cluster=0 --replica={replica} --replica-count=3
            \\    .zig-cache/upgrader/0_{replica}.tigerbeetle
        , .{
            .tigerbeetle = cli_args.old,
            .replica = replica_index,
        });
    }

    log.info("cluster at --addresses=127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003", .{});

    var load = try shell.spawn(.{ .stderr_behavior = .Inherit },
        \\{tigerbeetle} benchmark
        \\    --print-batch-timings
        \\    --transfer-count=2_000_000
        \\    --addresses=127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003
    , .{
        .tigerbeetle = cli_args.old,
    });

    var replicas: [replica_count]?std.process.Child = .{null} ** replica_count;
    var replicas_upgraded: [replica_count]bool = .{false} ** replica_count;
    defer {
        for (&replicas) |*replica_maybe| {
            if (replica_maybe.*) |*replica| {
                _ = replica.kill() catch {};
            }
        }
    }

    for (&replicas, 0..replica_count) |*replica, replica_index| {
        replica.* = try spawn(shell, .{
            .tigerbeetle = cli_args.old,
            .replica = replica_index,
        });
    }

    for (0..50) |_| {
        std.time.sleep(2 * std.time.ns_per_s);
        const replica_index = random.uintLessThan(u8, replica_count);
        const crash = random.uintLessThan(u8, 4) == 0;
        const restart = random.uintLessThan(u8, 2) == 0;
        const upgrade = random.uintLessThan(u8, 2) == 0;
        if (replicas[replica_index] == null) {
            if (restart) {
                replicas[replica_index] = try spawn(shell, .{
                    .tigerbeetle = if (replicas_upgraded[replica_index])
                        cli_args.new
                    else
                        cli_args.old,
                    .replica = replica_index,
                });
            }
        } else {
            if (crash) {
                _ = replicas[replica_index].?.kill() catch {};
                replicas[replica_index] = null;
            } else if (upgrade and !replicas_upgraded[replica_index]) {
                replicas_upgraded[replica_index] = true;
                _ = replicas[replica_index].?.kill() catch {};
                replicas[replica_index] = try spawn(shell, .{
                    .tigerbeetle = cli_args.new,
                    .replica = replica_index,
                });
            }
        }
    }
    for (0..replica_count) |replica_index| {
        if (replicas[replica_index]) |*replica| {
            _ = replica.kill() catch {};
            replicas[replica_index] = null;
        }
    }
    for (0..replica_count) |replica_index| {
        replicas[replica_index] = try spawn(shell, .{
            .tigerbeetle = cli_args.new,
            .replica = replica_index,
        });
    }

    log.info("all upgraded", .{});
    const term = try load.wait();
    assert(term.Exited == 0);
    log.info("success, cluster is functional after upgrade", .{});
}

fn spawn(shell: *Shell, options: struct {
    tigerbeetle: []const u8,
    replica: usize,
}) !std.process.Child {
    return try shell.spawn(.{
        .stderr_behavior = .Inherit,
    },
        \\{tigerbeetle} start
        \\    --addresses=127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003
        \\    .zig-cache/upgrader/0_{replica}.tigerbeetle
    , .{
        .tigerbeetle = options.tigerbeetle,
        .replica = options.replica,
    });
}
