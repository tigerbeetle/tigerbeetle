//! The systest supervisor is a script that runs:
//!
//! * a set of TigerBeetle replicas, forming a cluster
//! * a workload that runs commands and queries against the cluster, verifying its correctness
//!   (whatever that means is up to the workload)
//!
//! Right now the replicas and workload run as child processes, while the supervisor restarts
//! terminated replicas and injects crashes and network faults. After some configurable amount of
//! time, the supervisor terminates the workload and replicas, unless the workload exits on its own
//! or if any of the replicas exit unexpectedly.
//!
//! If the workload exits successfully, or is actively terminated, the whole systest exits
//! successfully.
//!
//! To launch a one-minute smoke test, run this command from the repository root:
//!
//!     $ zig build test:integration -- "systest smoke"
//!
//! If you need more control, you can run this script directly:
//!
//!     $ unshare -nfr zig build scripts -- systest --tigerbeetle-executable=./tigerbeetle
//!
//! NOTE: This requires that the Java client and Java workload are built first.
//!
//! Run a longer test:
//!
//!     $ unshare -nfr zig build scripts -- systest --tigerbeetle-executable=./tigerbeetle \
//!         --test-duration-minutes=10
//!
//! To capture its logs, for instance to run grep afterwards, redirect stderr to a file.
//!
//!     $ unshare -nfr zig build scripts -- systest --tigerbeetle-executable=./tigerbeetle \
//!         2> /tmp/systest.log
//!
//! If you have permissions troubles with Ubuntu, see:
//! https://github.com/YoYoGames/GameMaker-Bugs/issues/6015#issuecomment-2135552784
//!
//! TODO:
//!
//! * full partitioning
//! * better workload(s)
//! * filesystem fault injection?
//! * cluster membership changes?
//! * upgrades?

const std = @import("std");
const builtin = @import("builtin");
const Shell = @import("../../../shell.zig");
const LoggedProcess = @import("./logged_process.zig");
const Replica = @import("./replica.zig");
const NetworkFaults = @import("./network_faults.zig");
const arbitrary = @import("./arbitrary.zig");
const log = std.log.scoped(.systest);

const assert = std.debug.assert;

const replica_count = 3;
const replica_ports = [replica_count]u16{ 3000, 3001, 3002 };

pub const CLIArgs = struct {
    tigerbeetle_executable: []const u8,
    test_duration_minutes: u16 = 10,
};

pub fn main(shell: *Shell, allocator: std.mem.Allocator, args: CLIArgs) !void {
    if (builtin.os.tag == .windows) {
        log.err("systest is not supported for Windows", .{});
        return error.NotSupported;
    }

    const tmp_dir = try shell.create_tmp_dir();
    defer shell.cwd.deleteTree(tmp_dir) catch {};

    // Check that we are running as root.
    if (!std.mem.eql(u8, try shell.exec_stdout("id -u", .{}), "0")) {
        log.err(
            "this script needs to be run in a separate namespace using 'unshare -nfr'",
            .{},
        );
        std.process.exit(1);
    }

    try NetworkFaults.setup(allocator);

    log.info(
        "starting test with target runtime of {d}m",
        .{args.test_duration_minutes},
    );
    const test_duration_ns = @as(u64, @intCast(args.test_duration_minutes)) * std.time.ns_per_min;
    const test_deadline = std.time.nanoTimestamp() + test_duration_ns;

    var replicas: [replica_count]*Replica = undefined;
    for (0..replica_count) |i| {
        const datafile = try shell.fmt("{s}/1_{d}.tigerbeetle", .{ tmp_dir, i });

        // Format each replica's datafile.
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

        // Start replica.
        var replica = try Replica.create(
            allocator,
            args.tigerbeetle_executable,
            &replica_ports,
            @intCast(i),
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

    // Start workload.
    const workload = try start_workload(shell, allocator);
    defer {
        if (workload.state() == .running) {
            _ = workload.terminate() catch {};
        }
        workload.destroy(allocator);
    }

    const seed = std.crypto.random.int(u64);
    var prng = std.rand.DefaultPrng.init(seed);
    const random = prng.random();

    const workload_result =
        while (std.time.nanoTimestamp() < test_deadline)
    {
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
        for (replicas) |replica| {
            if (replica.state() == .terminated) {
                const replica_result = try replica.process.?.wait();
                switch (replica_result) {
                    .Signal => |signal| {
                        switch (signal) {
                            std.posix.SIG.KILL => {
                                log.info(
                                    "restarting terminated replica {d}",
                                    .{replica.replica_index},
                                );
                                try replica.start();
                            },
                            else => {
                                log.err(
                                    "replica {d} exited unexpectedly with on signal {d}",
                                    .{ replica.replica_index, signal },
                                );
                                std.process.exit(1);
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

        // Let the workload finish by itself if possible.
        if (workload.state() == .completed) {
            log.info("workload completed by itself", .{});
            break try workload.wait();
        }
    } else blk: {
        // If the workload doesn't complete by itself, kill it after we've run for the
        // required duration.
        log.info("terminating workload due to max duration", .{});
        break :blk try workload.terminate();
    };

    switch (workload_result) {
        .Signal => |signal| {
            switch (signal) {
                std.posix.SIG.KILL => log.info(
                    "workload terminated (SIGKILL) as requested",
                    .{},
                ),
                else => {
                    log.err(
                        "workload exited unexpectedly with on signal {d}",
                        .{signal},
                    );
                    std.process.exit(1);
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
    if (random_replica_in_state(random, replicas, .running)) |replica| {
        log.info("terminating replica {d}", .{replica.replica_index});
        _ = try replica.terminate();
        log.info("replica {d} terminating", .{replica.replica_index});
    } else return error.NoRunningReplica;
}

fn random_replica_in_state(
    random: std.Random,
    replicas: []*Replica,
    state: Replica.State,
) ?*Replica {
    var matching: [replica_count]*Replica = undefined;
    var count: u8 = 0;

    for (replicas) |replica| {
        if (replica.state() == state) {
            matching[count] = replica;
            count += 1;
        }
    }
    if (count == 0) {
        return null;
    } else {
        return matching[random.uintLessThan(usize, count)];
    }
}

fn start_workload(shell: *Shell, allocator: std.mem.Allocator) !*LoggedProcess {
    const client_jar = "src/clients/java/target/tigerbeetle-java-0.0.1-SNAPSHOT.jar";
    const workload_jar = "src/testing/systest/workload/target/workload-0.0.1-SNAPSHOT.jar";

    const class_path = try shell.fmt("{s}:{s}", .{ client_jar, workload_jar });
    const replicas = try LoggedProcess.comma_separate_ports(
        shell.arena.allocator(),
        &replica_ports,
    );
    const argv = &.{ "java", "-ea", "-cp", class_path, "Main", replicas };

    return try LoggedProcess.spawn(allocator, argv);
}
