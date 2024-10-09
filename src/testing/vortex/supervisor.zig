//! The Vortex _supervisor_ is a program that runs:
//!
//! * a set of TigerBeetle replicas, forming a cluster
//! * a workload that runs commands and queries against the cluster, verifying its correctness
//!   (whatever that means is up to the workload)
//!
//! The replicas and workload run as child processes, while the supervisor restarts terminated
//! replicas and injects crashes and network faults. After some configurable amount of time, the
//! supervisor terminates the workload and replicas, unless the workload exits on its own or if
//! any of the replicas exit unexpectedly.
//!
//! If the workload exits successfully, or is actively terminated, the whole vortex exits
//! successfully.
//!
//! To launch a one-minute smoke test, run this command from the repository root:
//!
//!     $ zig build test:integration -- "vortex smoke"
//!
//! If you need more control, you can run this program directly:
//!
//!     $ zig build vortex
//!     $ unshare -nfr zig-out/bin/vortex supervisor --tigerbeetle-executable=./tigerbeetle
//!
//! Run a longer test:
//!
//!     $ unshare -nfr zig-out/bin/vortex supervisor --tigerbeetle-executable=./tigerbeetle \
//!         --test-duration-minutes=10
//!
//! To capture its logs, for instance to run grep afterwards, redirect stderr to a file.
//!
//!     $ unshare -nfr zig-out/bin/vortex supervisor --tigerbeetle-executable=./tigerbeetle \
//!         2> /tmp/vortex.log
//!
//! If you have permissions troubles with Ubuntu, see:
//! https://github.com/YoYoGames/GameMaker-Bugs/issues/6015#issuecomment-2135552784
//!
//! Further possible work:
//!
//! * full partitioning
//! * filesystem faults
//! * clock faults
//! * upgrades (replicas and clients)
//! * liveness checks
//! * multiple drivers? could use a special multiplexer driver that delegates to others

const std = @import("std");
const builtin = @import("builtin");
const LoggedProcess = @import("./logged_process.zig");
const Replica = @import("./replica.zig");
const NetworkFaults = @import("./network_faults.zig");
const arbitrary = @import("./arbitrary.zig");

const log = std.log.scoped(.supervisor);

const process = std.process;
const assert = std.debug.assert;

const cluster_id = 1;
const replica_count = 3;
const replica_ports = [replica_count]u16{ 3000, 3001, 3002 };
const replica_addresses = comma_separate_ports(&replica_ports);

pub const CLIArgs = struct {
    tigerbeetle_executable: []const u8,
    test_duration_minutes: u16 = 10,
};

pub fn main(allocator: std.mem.Allocator, args: CLIArgs) !void {
    if (builtin.os.tag == .windows) {
        log.err("vortex is not supported for Windows", .{});
        return error.NotSupported;
    }

    const tmp_dir = try create_tmp_dir(allocator);
    defer allocator.free(tmp_dir);
    defer std.fs.cwd().deleteTree(tmp_dir) catch |err| {
        log.err("failed deleting temporary directory ({s}): {any}", .{ tmp_dir, err });
    };

    // Check that we are running as root.
    const user_id = std.os.linux.getuid();
    if (user_id != 0) {
        log.err(
            "this script needs to be run in a separate namespace using 'unshare -nfr'",
            .{},
        );
        process.exit(1);
    }

    try NetworkFaults.setup(allocator);

    log.info(
        "starting test with target runtime of {d}m",
        .{args.test_duration_minutes},
    );
    const test_duration_ns = @as(u64, @intCast(args.test_duration_minutes)) * std.time.ns_per_min;
    const test_deadline = std.time.nanoTimestamp() + test_duration_ns;

    var replicas: [replica_count]*Replica = undefined;
    inline for (0..replica_count) |i| {
        var datafile_buf: [1024]u8 = undefined;
        const datafile = try std.fmt.bufPrint(
            datafile_buf[0..],
            "{s}/{d}_{d}.tigerbeetle",
            .{ tmp_dir, cluster_id, i },
        );

        // Format each replica's datafile.
        const result = try process.Child.run(.{ .allocator = allocator, .argv = &.{
            args.tigerbeetle_executable,
            "format",
            std.fmt.comptimePrint("--cluster={d}", .{cluster_id}),
            std.fmt.comptimePrint("--replica={d}", .{i}),
            std.fmt.comptimePrint("--replica-count={d}", .{replica_count}),
            datafile,
        } });
        defer allocator.free(result.stderr);
        defer allocator.free(result.stdout);

        switch (result.term) {
            .Exited => |code| {
                if (code != 0) {
                    log.err("failed formatting datafile: {s}", .{result.stderr});
                    process.exit(1);
                }
            },
            else => {
                log.err("failed formatting datafile: {s}", .{result.stderr});
                process.exit(1);
            },
        }

        // Start replica.
        var replica = try Replica.create(
            allocator,
            args.tigerbeetle_executable,
            replica_addresses,
            datafile,
        );
        errdefer replica.destroy();

        try replica.start();

        replicas[i] = replica;
    }

    defer {
        for (replicas) |replica| {
            // We might have terminated the replica and never restarted it,
            // so we need to check its state.
            if (replica.state() == .running) {
                _ = replica.terminate() catch {};
            }
            replica.destroy();
        }
    }

    const workload = try start_workload(allocator);
    defer {
        if (workload.state() == .running) {
            _ = workload.terminate(std.posix.SIG.KILL) catch {};
        }
        workload.destroy(allocator);
    }

    const seed = std.crypto.random.int(u64);
    var prng = std.rand.DefaultPrng.init(seed);
    const random = prng.random();

    const workload_result = while (std.time.nanoTimestamp() < test_deadline) {
        // Things we do in this main loop, except supervising the replicas.
        const Action = enum { sleep, terminate_replica, mutate_network };
        switch (arbitrary.weighted(random, Action, .{
            .sleep = 4,
            .terminate_replica = 1,
            .mutate_network = 2,
        }).?) {
            .sleep => std.time.sleep(5 * std.time.ns_per_s),
            .terminate_replica => try terminate_random_replica(random, &replicas),
            .mutate_network => {
                const weights = NetworkFaults.adjusted_weights(.{
                    .network_delay_add = 1,
                    .network_delay_remove = 10,
                    .network_loss_add = 1,
                    .network_loss_remove = 10,
                });
                if (arbitrary.weighted(random, NetworkFaults.Action, weights)) |action| {
                    try NetworkFaults.execute(allocator, random, action);
                } else {
                    std.time.sleep(100 * std.time.ns_per_ms);
                }
            },
        }

        // Restart any (by the nemesis) terminated replicas. Any other termination reason
        // than SIGKILL is considered unexpected and fails the test.
        for (replicas, 0..) |replica, index| {
            if (replica.state() == .terminated) {
                const replica_result = try replica.process.?.wait();
                switch (replica_result) {
                    .Signal => |signal| {
                        switch (signal) {
                            std.posix.SIG.KILL => {
                                log.info(
                                    "restarting terminated replica {d}",
                                    .{index},
                                );
                                try replica.start();
                            },
                            else => {
                                log.err(
                                    "replica {d} exited unexpectedly with on signal {d}",
                                    .{ index, signal },
                                );
                                process.exit(1);
                            },
                        }
                    },
                    else => {
                        log.err("unexpected replica result: {any}", .{replica_result});
                        return error.TestFailed;
                    },
                }
            }
        }

        // We do not expect the workload to terminate on its own, but if it does,
        // we end the test.
        if (workload.state() == .completed) {
            log.info("workload terminated by itself", .{});
            break try workload.wait();
        }
    } else blk: {
        // If the workload doesn't complete by itself, terminate it after we've run for the
        // required duration.
        log.info("terminating workload due to max duration", .{});
        break :blk if (workload.state() == .running)
            try workload.terminate(std.posix.SIG.KILL)
        else
            try workload.wait();
    };

    switch (workload_result) {
        .Signal => |signal| {
            switch (signal) {
                std.posix.SIG.KILL => log.info(
                    "workload terminated as requested",
                    .{},
                ),
                else => {
                    log.err(
                        "workload exited unexpectedly with signal {d}",
                        .{signal},
                    );
                    process.exit(1);
                },
            }
        },
        else => {
            log.err("unexpected workload result: {any}", .{workload_result});
            return error.TestFailed;
        },
    }
}

fn terminate_random_replica(random: std.Random, replicas: []*Replica) !void {
    if (random_replica_in_state(random, replicas, .running)) |found| {
        log.info("terminating replica {d}", .{found.index});
        _ = try found.replica.terminate();
        log.info("replica {d} terminated", .{found.index});
    } else return error.NoRunningReplica;
}

const RandomReplica = struct { replica: *Replica, index: u8 };

fn random_replica_in_state(
    random: std.Random,
    replicas: []*Replica,
    state: Replica.State,
) ?RandomReplica {
    var matching: [replica_count]RandomReplica = undefined;
    var count: u8 = 0;

    for (replicas, 0..) |replica, index| {
        if (replica.state() == state) {
            matching[count] = .{ .replica = replica, .index = @intCast(index) };
            count += 1;
        }
    }
    if (count == 0) {
        return null;
    } else {
        return matching[random.uintLessThan(usize, count)];
    }
}

fn start_workload(allocator: std.mem.Allocator) !*LoggedProcess {
    const argv = &.{
        "zig-out/bin/vortex",
        "workload",
        std.fmt.comptimePrint("--cluster-id={d}", .{cluster_id}),
        std.fmt.comptimePrint("--addresses={s}", .{replica_addresses}),
        "--driver-command=zig-out/bin/vortex driver",
    };

    return try LoggedProcess.spawn(allocator, argv);
}

pub fn comma_separate_ports(comptime ports: []const u16) []const u8 {
    assert(ports.len > 0);

    var out: []const u8 = std.fmt.comptimePrint("{d}", .{ports[0]});

    inline for (ports[1..]) |port| {
        out = out ++ std.fmt.comptimePrint(",{d}", .{port});
    }

    assert(std.mem.count(u8, out, ",") == ports.len - 1);
    return out;
}

// Create a new Vortex-specific temporary directory. Caller owns the returned memory.
fn create_tmp_dir(allocator: std.mem.Allocator) ![]const u8 {
    const root = process.getEnvVarOwned(allocator, "TMPDIR") catch try allocator.dupe(u8, "/tmp");
    defer allocator.free(root);

    const path = try std.fmt.allocPrint(allocator, "{s}/vortex-{d}", .{
        root,
        std.crypto.random.int(u64),
    });
    errdefer allocator.free(path);

    try std.fs.makeDirAbsolute(path);

    return path;
}
