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
//!     $ unshare --net --fork --map-root-user --pid \
//!         zig-out/bin/vortex supervisor --tigerbeetle-executable=./tigerbeetle
//!
//! Run a longer test:
//!
//!     $ unshare --net --fork --map-root-user --pid \
//!         zig-out/bin/vortex supervisor --tigerbeetle-executable=./tigerbeetle \
//!         --test-duration-minutes=10
//!
//! To capture its logs, for instance to run grep afterwards, redirect stderr to a file.
//!
//!     $ unshare --net --fork --map-root-user --pid \
//!         zig-out/bin/vortex supervisor --tigerbeetle-executable=./tigerbeetle \
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
const NetworkFaults = @import("./network_faults.zig");
const arbitrary = @import("./arbitrary.zig");

const log = std.log.scoped(.supervisor);

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
    defer {
        std.fs.cwd().deleteTree(tmp_dir) catch |err| {
            log.err("failed deleting temporary directory ({s}): {any}", .{ tmp_dir, err });
        };
        allocator.free(tmp_dir);
    }

    // Check that we are running as root.
    const user_id = std.os.linux.getuid();
    if (user_id != 0) {
        log.err(
            \\ this script needs to be run in a separate namespace using
            \\ 'unshare --net --fork --map-root-user --pid'
        , .{});
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
    defer {
        for (replicas) |replica| {
            // We might have terminated the replica and never restarted it,
            // so we need to check its state.
            if (replica.state() != .terminated) {
                _ = replica.terminate() catch {};
            }
            replica.destroy();
        }
    }

    var datafile_buffers: [replica_count][std.fs.max_path_bytes]u8 = undefined;
    inline for (0..replica_count) |replica_index| {
        const datafile = try std.fmt.bufPrint(
            datafile_buffers[replica_index][0..],
            "{s}/{d}_{d}.tigerbeetle",
            .{ tmp_dir, cluster_id, replica_index },
        );

        // Format each replica's datafile.
        const result = try std.process.Child.run(.{ .allocator = allocator, .argv = &.{
            args.tigerbeetle_executable,
            "format",
            std.fmt.comptimePrint("--cluster={d}", .{cluster_id}),
            std.fmt.comptimePrint("--replica={d}", .{replica_index}),
            std.fmt.comptimePrint("--replica-count={d}", .{replica_count}),
            datafile,
        } });
        defer {
            allocator.free(result.stderr);
            allocator.free(result.stdout);
        }

        if (!std.meta.eql(result.term, .{ .Exited = 0 })) {
            log.err("failed formatting datafile: {s}", .{result.stderr});
            std.process.exit(1);
        }

        // Start replica.
        var replica = try Replica.create(
            allocator,
            args.tigerbeetle_executable,
            datafile,
        );
        errdefer replica.destroy();

        try replica.start();

        replicas[replica_index] = replica;
    }

    const workload = try start_workload(allocator);
    defer {
        if (workload.state() == .running) {
            _ = workload.terminate() catch {};
        }
        workload.destroy(allocator);
    }

    const seed = std.crypto.random.int(u64);
    var prng = std.rand.DefaultPrng.init(seed);
    const random = prng.random();

    const workload_result = while (std.time.nanoTimestamp() < test_deadline) {
        // Things we do in this main loop, except supervising the replicas.
        const Action = enum {
            sleep,
            replica_terminate,
            replica_stop,
            replica_resume,
            mutate_network,
        };

        const running_replica = random_replica_in_state(random, &replicas, .running);
        const stopped_replica = random_replica_in_state(random, &replicas, .stopped);

        switch (arbitrary.weighted(random, Action, .{
            .sleep = 20,
            .replica_terminate = if (running_replica != null) 1 else 0,
            .replica_stop = if (running_replica != null) 1 else 0,
            .replica_resume = if (stopped_replica != null) 5 else 0,
            .mutate_network = 3,
        }).?) {
            .sleep => std.time.sleep(5 * std.time.ns_per_s),
            .replica_terminate => {
                if (running_replica) |found| {
                    log.info("terminating replica {d}", .{found.index});
                    _ = try found.replica.terminate();
                }
            },
            .replica_stop => {
                if (running_replica) |found| {
                    log.info("stopping replica {d}", .{found.index});
                    _ = try found.replica.stop();
                }
            },
            .replica_resume => {
                if (stopped_replica) |found| {
                    log.info("resuming replica {d}", .{found.index});
                    _ = try found.replica.cont();
                }
            },
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
                                    "replica {d} terminated unexpectedly with signal {d}",
                                    .{ index, signal },
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

        // We do not expect the workload to terminate on its own, but if it does,
        // we end the test.
        if (workload.state() == .terminated) {
            log.info("workload terminated by itself", .{});
            break try workload.wait();
        }
    } else blk: {
        // If the workload doesn't terminate by itself, we terminate it after we've run for the
        // required duration.
        log.info("terminating workload due to max duration", .{});
        break :blk if (workload.state() == .running)
            try workload.terminate()
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
    var vortex_path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const vortex_path = try std.fs.selfExePath(&vortex_path_buffer);

    var driver_command_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const driver_command = try std.fmt.bufPrint(
        &driver_command_buffer,
        "--driver-command={s} driver",
        .{vortex_path},
    );

    const argv = &.{
        vortex_path,
        "workload",
        std.fmt.comptimePrint("--cluster-id={d}", .{cluster_id}),
        std.fmt.comptimePrint("--addresses={s}", .{replica_addresses}),
        driver_command,
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
    const working_directory = std.fs.cwd();

    const path = try std.fmt.allocPrint(allocator, ".zig-cache/vortex-{d}", .{
        std.crypto.random.int(u64),
    });
    defer allocator.free(path);

    try working_directory.makePath(path);

    return working_directory.realpathAlloc(allocator, path);
}

const Replica = struct {
    pub const State = enum(u8) { initial, running, stopped, terminated };

    allocator: std.mem.Allocator,
    executable_path: []const u8,
    datafile: []const u8,
    process: ?*LoggedProcess,

    pub fn create(
        allocator: std.mem.Allocator,
        executable_path: []const u8,
        datafile: []const u8,
    ) !*Replica {
        const self = try allocator.create(Replica);
        errdefer allocator.destroy(self);

        self.* = .{
            .allocator = allocator,
            .executable_path = executable_path,
            .datafile = datafile,
            .process = null,
        };
        return self;
    }

    pub fn destroy(self: *Replica) void {
        assert(self.state() == .initial or self.state() == .terminated);
        const allocator = self.allocator;
        if (self.process) |process| {
            process.destroy(allocator);
        }
        allocator.destroy(self);
    }

    pub fn state(self: *Replica) State {
        if (self.process) |process| {
            switch (process.state()) {
                .running => return .running,
                .stopped => return .stopped,
                .terminated => return .terminated,
            }
        } else return .initial;
    }

    pub fn start(self: *Replica) !void {
        assert(self.state() != .running);
        defer assert(self.state() == .running);

        if (self.process) |process| {
            process.destroy(self.allocator);
        }

        const argv = &.{
            self.executable_path,
            "start",
            "--addresses=" ++ replica_addresses,
            self.datafile,
        };

        self.process = try LoggedProcess.spawn(self.allocator, argv);
    }

    pub fn terminate(
        self: *Replica,
    ) !std.process.Child.Term {
        assert(self.state() == .running or self.state() == .stopped);
        defer assert(self.state() == .terminated);

        assert(self.process != null);
        return try self.process.?.terminate();
    }

    pub fn stop(
        self: *Replica,
    ) !void {
        assert(self.state() == .running);
        defer assert(self.state() == .stopped);

        assert(self.process != null);
        return try self.process.?.stop();
    }

    pub fn cont(
        self: *Replica,
    ) !void {
        assert(self.state() == .stopped);
        defer assert(self.state() == .running);

        assert(self.process != null);
        return try self.process.?.cont();
    }
};
