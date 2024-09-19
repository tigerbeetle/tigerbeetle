//! The systest supervisor is a script that runs:
//!
//! * a set of TigerBeetle replicas, forming a cluster
//! * a workload that runs commands and queries against the cluster, verifying its correctness
//!   (whatever that means is up to the workload)
//! * a nemesis, which injects various kinds of _faults_.
//!
//! Right now the replicas and workload run as child processes, while the nemesis wreaks havoc
//! in the main loop. After some (configurable) amount of time, the supervisor terminates the
//! workload and replicas, unless the workload exits on its own.
//!
//! If the workload exits successfully, or is actively terminated, the whole systest exits
//! successfully.
//!
//! To launch a test, run this command from the repository root:
//!
//!     $ unshare --user  -f --pid zig build scripts -- systest --tigerbeetle-executable=./tigerbeetle
//!
//! To capture its logs, for instance to run grep afterwards, redirect stderr to a file.
//!
//!     $ unshare --user  -f --pid zig build scripts -- systest --tigerbeetle-executable=./tigerbeetle 2> /tmp/systest.log
//!
//! TODO:
//!
//! * build tigerbeetle at start of script
//! * full partitioning
//! * better workload(s)
//! * filesystem fault injection?
//! * cluster membership changes?

const std = @import("std");
const builtin = @import("builtin");
const flags = @import("../../../flags.zig");
const Shell = @import("../../../shell.zig");
const LoggedProcess = @import("./process.zig").LoggedProcess;
const Replica = @import("./replica.zig");
const Nemesis = @import("./nemesis.zig");
const log = std.log.default;

const assert = std.debug.assert;

const replica_count = 3;
const replica_ports = [replica_count]u16{ 3000, 3001, 3002 };

pub const CLIArgs = struct {
    tigerbeetle_executable: []const u8,
    test_duration_minutes: u16 = 10,
};

pub fn main(shell: *Shell, allocator: std.mem.Allocator, args: CLIArgs) !void {
    const tmp_dir = try shell.create_tmp_dir();
    defer shell.cwd.deleteDir(tmp_dir) catch {};

    log.info("supervisor: starting test with target runtime of {d}m", .{args.test_duration_minutes});
    const test_duration_ns = @as(u64, @intCast(args.test_duration_minutes)) * std.time.ns_per_min;
    const time_start = std.time.nanoTimestamp();

    var replicas: [replica_count]Replica = undefined;
    for (0..replica_count) |i| {
        const name = try shell.fmt("replica {d}", .{i});
        const datafile = try shell.fmt("{s}/1_{d}.tigerbeetle", .{ tmp_dir, i });

        // Format datafile
        try shell.exec(
            \\{tigerbeetle} format 
            \\  --cluster=1
            \\  --replica={index}
            \\  --replica-count={replica_count} 
            \\  {datafile}
        , .{
            .tigerbeetle = args.tigerbeetle_executable,
            .index = i,
            .replica_count = replica_count,
            .datafile = datafile,
        });

        // Start replica
        const addresses = try shell.fmt("--addresses={s}", .{
            try comma_separate_ports(shell.arena.allocator(), &replica_ports),
        });
        const argv = try shell.arena.allocator().dupe([]const u8, &.{
            args.tigerbeetle_executable,
            "start",
            addresses,
            datafile,
        });

        var process = try LoggedProcess.init(allocator, name, argv, .{});
        errdefer process.deinit();

        replicas[i] = .{ .name = name, .port = replica_ports[i], .process = process };
        try process.start();
    }

    // Start workload
    const workload = try start_workload(shell, allocator);
    errdefer workload.deinit();

    // Start nemesis (fault injector)
    var prng = std.rand.DefaultPrng.init(0);
    const nemesis = try Nemesis.init(shell, allocator, prng.random(), &replicas);
    defer nemesis.deinit();

    if (true) {
        @panic("uh oh");
    }

    // Let the workload finish by itself, or kill it after we've run for the required duration.
    // Note that the nemesis is blocking in this loop.
    const workload_result = term: {
        while (std.time.nanoTimestamp() - time_start < test_duration_ns) {
            // Try to do something funky in the nemesis, and if it fails, wait for a while
            // before trying again.
            if (!try nemesis.wreak_havoc()) {
                std.time.sleep(100 * std.time.ns_per_ms);
            }
            if (workload.state() == .completed) {
                log.info("supervisor: workload completed by itself", .{});
                break :term try workload.wait();
            }
        }

        log.info("supervisor: terminating workload due to max duration", .{});
        break :term try workload.terminate();
    };

    workload.deinit();

    for (replicas) |replica| {
        _ = try replica.process.terminate();
        replica.process.deinit();
    }

    switch (workload_result) {
        .Exited => |code| {
            if (code == 128 + std.posix.SIG.TERM) {
                log.info("supervisor: workload terminated as requested", .{});
            } else if (code == 0) {
                log.info("supervisor: workload exited successfully", .{});
            } else {
                log.info("supervisor: workload exited unexpectedly with code {d}", .{code});
                std.process.exit(1);
            }
        },
        else => {
            log.info("supervisor: unexpected workload result: {any}", .{workload_result});
            unreachable;
        },
    }
}

fn start_workload(shell: *Shell, allocator: std.mem.Allocator) !*LoggedProcess {
    const name = "workload";
    const client_jar = "src/clients/java/target/tigerbeetle-java-0.0.1-SNAPSHOT.jar";
    const workload_jar = "src/testing/systest/workload/target/workload-0.0.1-SNAPSHOT.jar";

    const class_path = try shell.fmt("{s}:{s}", .{ client_jar, workload_jar });
    const argv = try shell.arena.allocator().dupe([]const u8, &.{
        "java",
        "-ea",
        "-cp",
        class_path,
        "Main",
    });

    var env = try std.process.getEnvMap(shell.arena.allocator());
    try env.put("REPLICAS", try comma_separate_ports(shell.arena.allocator(), &replica_ports));

    var process = try LoggedProcess.init(allocator, name, argv, .{ .env = &env });
    try process.start();
    return process;
}

fn comma_separate_ports(allocator: std.mem.Allocator, ports: []const u16) ![]const u8 {
    assert(ports.len > 0);

    var out = std.ArrayList(u8).init(allocator);
    const writer = out.writer();

    try std.fmt.format(writer, "{d}", .{ports[0]});
    for (ports[1..]) |port| {
        try writer.writeByte(',');
        try std.fmt.format(writer, "{d}", .{port});
    }

    return out.toOwnedSlice();
}

test "comma-separates ports" {
    const formatted = try comma_separate_ports(std.testing.allocator, &.{ 3000, 3001, 3002 });
    defer std.testing.allocator.free(formatted);

    try std.testing.expectEqualStrings("3000,3001,3002", formatted);
}
