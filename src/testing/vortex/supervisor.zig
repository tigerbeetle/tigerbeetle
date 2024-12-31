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
//! If you need more control, you can run this program directly. Using `unshare` is recommended to
//! be sure all child processes are killed, and to get network sandboxing.
//!
//!     $ zig build
//!     $ zig build vortex
//!     $ unshare --net --fork --map-root-user --pid bash -c 'ip link set up dev lo ; \
//!         ./zig-out/bin/vortex supervisor --tigerbeetle-executable=./zig-out/bin/tigerbeetle'
//!
//! Other options:
//!
//! * Control the test duration by adding the `--test-duration-minutes=N` option (it's 10 minutes
//!   by default).
//! * Enable replica debug logging with `--log-debug`.
//!
//! If you have permissions troubles with unshare and Ubuntu, see:
//! https://github.com/YoYoGames/GameMaker-Bugs/issues/6015#issuecomment-2135552784
//!
//! Further possible work:
//!
//! * full partitioning
//! * filesystem faults
//! * clock faults
//! * upgrades (replicas and clients)
//! * multiple drivers? could use a special multiplexer driver that delegates to others

const std = @import("std");
const stdx = @import("../../stdx.zig");
const builtin = @import("builtin");
const IO = @import("../../io.zig").IO;
const RingBufferType = @import("../../ring_buffer.zig").RingBufferType;
const LoggedProcess = @import("./logged_process.zig");
const faulty_network = @import("./faulty_network.zig");
const arbitrary = @import("./arbitrary.zig");
const constants = @import("./constants.zig");
const Progress = @import("./workload.zig").Progress;

const log = std.log.scoped(.supervisor);

const assert = std.debug.assert;

const replica_ports_actual = [constants.replica_count]u16{ 4000, 4001, 4002 };
const replica_ports_proxied = [constants.replica_count]u16{ 3000, 3001, 3002 };

// Calculate replica addresses (comma-separated) for each replica. Because
// we want replicas to communicate over the proxies, we use those ports
// for their peers, but we must use the replica's actual port for itself
// (because that's the port it listens to).
const replica_addresses_for_replicas = blk: {
    var result: [constants.replica_count][]const u8 = undefined;
    for (0..constants.replica_count) |replica_self| {
        var ports: [constants.replica_count]u16 = undefined;
        for (0..constants.replica_count) |replica_other| {
            ports[replica_other] = if (replica_self == replica_other)
                replica_ports_actual[replica_other]
            else
                replica_ports_proxied[replica_other];
        }
        result[replica_self] = comma_separate_ports(&ports);
    }
    break :blk result;
};

const replica_addresses_for_clients = comma_separate_ports(&replica_ports_proxied);

// For the Chrome trace file, we need to assign process IDs to all logical
// processes in the Vortex test. The replicas get their replica indices, and
// the other ones are assigned manually here.
const vortex_process_ids = .{
    .supervisor = constants.replica_count,
    .workload = constants.replica_count + 1,
    .network = constants.replica_count + 2,
};
comptime {
    // Check that the assigned process IDs are sequential and start at the right number.
    for (
        std.meta.fields(@TypeOf(vortex_process_ids)),
        constants.replica_count..,
    ) |field, value_expected| {
        const value_actual = @field(vortex_process_ids, field.name);
        assert(value_actual == value_expected);
    }
}

pub const CLIArgs = struct {
    tigerbeetle_executable: []const u8,
    test_duration_minutes: u16 = 10,
    driver_command: ?[]const u8 = null,
    disable_faults: bool = false,
    log_debug: bool = false,
};

pub fn main(allocator: std.mem.Allocator, args: CLIArgs) !void {
    if (builtin.os.tag == .windows) {
        log.err("vortex is not supported for Windows", .{});
        return error.NotSupported;
    }

    var io = try IO.init(128, 0);

    const tmp_dir = try create_tmp_dir(allocator);
    defer {
        std.fs.cwd().deleteTree(tmp_dir) catch |err| {
            log.err("failed deleting temporary directory ({s}): {any}", .{ tmp_dir, err });
        };
        allocator.free(tmp_dir);
    }

    var trace_file_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const trace_file = try std.fmt.bufPrint(&trace_file_buffer, "{s}/vortex.trace", .{tmp_dir});
    var trace = try TraceWriter.from_file(trace_file);
    defer trace.deinit();

    inline for (std.meta.fields(@TypeOf(vortex_process_ids))) |field| {
        try trace.process_name_assign(@field(vortex_process_ids, field.name), field.name);
    }

    log.info(
        "starting test with target runtime of {d}m",
        .{args.test_duration_minutes},
    );

    const seed = std.crypto.random.int(u64);
    var prng = std.rand.DefaultPrng.init(seed);
    const random = prng.random();

    var replicas: [constants.replica_count]*Replica = undefined;
    var replicas_initialized: usize = 0;
    defer {
        for (replicas[0..replicas_initialized]) |replica| {
            // We might have terminated the replica and never restarted it,
            // so we need to check its state.
            if (replica.state() != .terminated) {
                _ = replica.terminate() catch {};
            }
            replica.destroy();
        }
    }

    var datafile_buffers: [constants.replica_count][std.fs.max_path_bytes]u8 = undefined;
    inline for (0..constants.replica_count) |replica_index| {
        const datafile = try std.fmt.bufPrint(
            datafile_buffers[replica_index][0..],
            "{s}/{d}_{d}.tigerbeetle",
            .{ tmp_dir, constants.cluster_id, replica_index },
        );

        // Format each replica's datafile.
        const result = try std.process.Child.run(.{ .allocator = allocator, .argv = &.{
            args.tigerbeetle_executable,
            "format",
            std.fmt.comptimePrint("--cluster={d}", .{constants.cluster_id}),
            std.fmt.comptimePrint("--replica={d}", .{replica_index}),
            std.fmt.comptimePrint("--replica-count={d}", .{constants.replica_count}),
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
            @intCast(replica_index),
            datafile,
            args.log_debug,
        );

        replicas[replica_index] = replica;
        replicas_initialized += 1;

        try trace.process_name_assign(
            replica_index,
            std.fmt.comptimePrint("replica {d}", .{replica_index}),
        );

        try replica.start();
    }

    // Construct mappings between proxy and underlying replicas.
    var mappings: [constants.replica_count]faulty_network.Mapping = undefined;
    inline for (0..constants.replica_count) |replica_index| {
        mappings[replica_index] = .{
            .origin = .{ .in = std.net.Ip4Address.init(
                .{ 0, 0, 0, 0 },
                replica_ports_proxied[replica_index],
            ) },
            .remote = .{ .in = std.net.Ip4Address.init(
                .{ 0, 0, 0, 0 },
                replica_ports_actual[replica_index],
            ) },
        };
    }

    // Start fault-injecting network (a set of proxies).
    var network = try faulty_network.Network.listen(
        allocator,
        &io,
        mappings[0..],
        random,
    );
    defer network.destroy(allocator);

    const workload = try Workload.spawn(allocator, args, &io, &trace);
    defer {
        if (workload.process.state() == .running) {
            _ = workload.process.terminate() catch {};
        }
        workload.destroy(allocator);
    }

    const supervisor = try Supervisor.create(allocator, .{
        .io = &io,
        .network = network,
        .replicas = replicas,
        .workload = workload,
        .trace = &trace,
        .random = random,
        .test_duration_minutes = args.test_duration_minutes,
        .disable_faults = args.disable_faults,
    });
    defer supervisor.destroy(allocator);

    try supervisor.run();
}

const Supervisor = struct {
    io: *IO,
    network: *faulty_network.Network,
    replicas: [constants.replica_count]*Replica,
    workload: *Workload,
    trace: *TraceWriter,
    random: std.Random,
    test_deadline: i128,
    disable_faults: bool,

    running_replicas_buffer: [constants.replica_count]ReplicaWithIndex = undefined,
    terminated_replicas_buffer: [constants.replica_count]ReplicaWithIndex = undefined,
    stopped_replicas_buffer: [constants.replica_count]ReplicaWithIndex = undefined,

    fn create(allocator: std.mem.Allocator, options: struct {
        io: *IO,
        network: *faulty_network.Network,
        replicas: [constants.replica_count]*Replica,
        workload: *Workload,
        trace: *TraceWriter,
        random: std.Random,
        test_duration_minutes: u16,
        disable_faults: bool,
    }) !*Supervisor {
        const test_duration_ns = @as(u64, @intCast(options.test_duration_minutes)) *
            std.time.ns_per_min;
        const test_deadline = std.time.nanoTimestamp() + test_duration_ns;

        const supervisor = try allocator.create(Supervisor);
        errdefer allocator.destroy(supervisor);

        supervisor.* = .{
            .io = options.io,
            .network = options.network,
            .replicas = options.replicas,
            .workload = options.workload,
            .trace = options.trace,
            .random = options.random,
            .test_deadline = test_deadline,
            .disable_faults = options.disable_faults,
        };
        return supervisor;
    }
    fn destroy(supervisor: *Supervisor, allocator: std.mem.Allocator) void {
        allocator.destroy(supervisor);
    }

    fn run(supervisor: *Supervisor) !void {
        var sleep_deadline: u64 = 0;
        // This represents the start timestamp of a period where we have an acceptable number of
        // process faults, such that we require liveness (that requests are finished within a
        // certain time period). If null, it means we're in a period of too many faults, thus
        // enforcing no such requirement.
        var acceptable_faults_start_ns: ?u64 = null;
        const workload_result = while (std.time.nanoTimestamp() < supervisor.test_deadline) {
            supervisor.network.tick();
            try supervisor.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
            const now: u64 = @intCast(std.time.nanoTimestamp());

            const running_replicas = replicas_in_state(
                &supervisor.replicas,
                &supervisor.running_replicas_buffer,
                .running,
            );
            const terminated_replicas = replicas_in_state(
                &supervisor.replicas,
                &supervisor.terminated_replicas_buffer,
                .terminated,
            );
            const stopped_replicas = replicas_in_state(
                &supervisor.replicas,
                &supervisor.stopped_replicas_buffer,
                .stopped,
            );

            const faulty_replica_count = terminated_replicas.len + stopped_replicas.len;

            if (acceptable_faults_start_ns) |start_ns| {
                const deadline = start_ns + constants.liveness_requirement_seconds *
                    std.time.ns_per_s;
                // If we've been in a state with an acceptable number of faults for the required
                // amount of time, we should have seen finished requests.
                const no_finished_requests =
                    now > deadline and supervisor.workload.requests_finished.empty();
                // Also, those that do finish should not have too long durations, counting from the
                // start of the acceptably-faulty period.
                const too_slow_request = supervisor.workload.find_slow_request_since(start_ns);

                errdefer {
                    const start_us = @divFloor(start_ns, std.time.ns_per_us);
                    supervisor.trace.write(.{
                        .name = .liveness_required,
                        .timestamp_micros = start_us,
                        .duration = @as(u64, @intCast(std.time.microTimestamp())) - start_us,
                        .process_id = vortex_process_ids.supervisor,
                        .phase = .complete,
                    }, .{}) catch {};
                    supervisor.trace.write(.{
                        .name = .test_failure,
                        .process_id = vortex_process_ids.supervisor,
                        .phase = .instant,
                    }, .{}) catch {};
                }

                if (no_finished_requests) {
                    log.err("liveness check: no finished requests after {d} seconds", .{
                        constants.liveness_requirement_seconds,
                    });
                    return error.TestFailed;
                }

                if (too_slow_request) |_| {
                    log.err("liveness check: too slow request", .{});
                    return error.TestFailed;
                }
            }

            // Check if `acceptable_faults_start_ns` should change state. If so, we reset the max
            // request duration too.
            // NOTE: Network faults are currently global, so we relax the requirement in such cases.
            if (faulty_replica_count <= constants.liveness_faulty_replicas_max and
                supervisor.network.faults.is_healed())
            {
                // We have an acceptable number of faults, so we require liveness (after some time).
                if (acceptable_faults_start_ns == null) {
                    acceptable_faults_start_ns = @intCast(std.time.nanoTimestamp());
                    supervisor.workload.requests_finished.clear();
                }
            } else {
                // We have too many faults to require liveness.
                if (acceptable_faults_start_ns) |start_ns| {
                    acceptable_faults_start_ns = null;
                    supervisor.workload.requests_finished.clear();

                    // Record the previously passed liveness requirement period in the trace:
                    const duration_ns = @as(u64, @intCast(std.time.nanoTimestamp())) - start_ns;
                    if (@divFloor(duration_ns, std.time.ns_per_s) >
                        constants.liveness_requirement_seconds)
                    {
                        try supervisor.trace.write(.{
                            .name = .liveness_required,
                            .timestamp_micros = @divFloor(start_ns, std.time.ns_per_us),
                            .duration = @divFloor(duration_ns, std.time.ns_per_us),
                            .process_id = vortex_process_ids.supervisor,
                            .phase = .complete,
                        }, .{});
                    }
                }
            }

            if (now < sleep_deadline) continue;

            if (!supervisor.disable_faults) {
                const Action = enum {
                    sleep,
                    replica_terminate,
                    replica_restart,
                    replica_stop,
                    replica_resume,
                    network_delay,
                    network_lose,
                    network_corrupt,
                    network_heal,
                    quiesce,
                };

                switch (arbitrary.weighted(supervisor.random, Action, .{
                    .sleep = 10,
                    .replica_terminate = if (running_replicas.len > 0) 4 else 0,
                    .replica_restart = if (terminated_replicas.len > 0) 3 else 0,
                    .replica_stop = if (running_replicas.len > 0) 3 else 0,
                    .replica_resume = if (stopped_replicas.len > 0) 10 else 0,
                    .network_delay = if (supervisor.network.faults.delay == null) 3 else 0,
                    .network_lose = if (supervisor.network.faults.lose == null) 3 else 0,
                    .network_corrupt = if (supervisor.network.faults.corrupt == null) 3 else 0,
                    .network_heal = if (!supervisor.network.faults.is_healed()) 10 else 0,
                    .quiesce = if (faulty_replica_count > 0 or
                        !supervisor.network.faults.is_healed()) 1 else 0,
                }).?) {
                    .sleep => {
                        const duration =
                            supervisor.random.uintLessThan(u64, 10 * std.time.ns_per_s);
                        log.info("sleeping for {}", .{std.fmt.fmtDuration(duration)});
                        sleep_deadline = now + duration;
                    },
                    .replica_terminate => {
                        const pick = arbitrary.element(
                            supervisor.random,
                            ReplicaWithIndex,
                            running_replicas,
                        );
                        log.info("terminating replica {d}", .{pick.index});
                        try supervisor.trace.write(.{
                            .name = .terminated,
                            .process_id = pick.index,
                            .phase = .begin,
                        }, .{});
                        _ = try pick.replica.terminate();
                    },
                    .replica_restart => {
                        const pick = arbitrary.element(
                            supervisor.random,
                            ReplicaWithIndex,
                            terminated_replicas,
                        );
                        log.info("restarting replica {d}", .{pick.index});
                        try supervisor.trace.write(.{
                            .name = .terminated,
                            .process_id = pick.index,
                            .phase = .end,
                        }, .{});
                        _ = try pick.replica.start();
                    },
                    .replica_stop => {
                        const pick = arbitrary.element(
                            supervisor.random,
                            ReplicaWithIndex,
                            running_replicas,
                        );
                        log.info("stopping replica {d}", .{pick.index});
                        try supervisor.trace.write(.{
                            .name = .stopped,
                            .process_id = pick.index,
                            .phase = .begin,
                        }, .{});
                        _ = try pick.replica.stop();
                    },
                    .replica_resume => {
                        const pick = arbitrary.element(
                            supervisor.random,
                            ReplicaWithIndex,
                            stopped_replicas,
                        );
                        log.info("resuming replica {d}", .{pick.index});
                        try supervisor.trace.write(.{
                            .name = .stopped,
                            .process_id = pick.index,
                            .phase = .end,
                        }, .{});
                        _ = try pick.replica.cont();
                    },
                    .network_delay => {
                        if (!supervisor.network.faults.is_healed()) {
                            try supervisor.trace.write(.{
                                .name = .network_faults,
                                .process_id = vortex_process_ids.network,
                                .phase = .end,
                            }, .{});
                        }
                        const time_ms = supervisor.random.intRangeAtMost(u32, 10, 500);
                        supervisor.network.faults.delay = .{
                            .time_ms = time_ms,
                            .jitter_ms = @min(time_ms, 50),
                        };
                        log.info("injecting network delays: {any}", .{supervisor.network.faults});
                        try supervisor.trace.write(.{
                            .name = .network_faults,
                            .process_id = vortex_process_ids.network,
                            .phase = .begin,
                        }, supervisor.network.faults);
                    },
                    .network_lose => {
                        if (!supervisor.network.faults.is_healed()) {
                            try supervisor.trace.write(.{
                                .name = .network_faults,
                                .process_id = vortex_process_ids.network,
                                .phase = .end,
                            }, .{});
                        }
                        supervisor.network.faults.lose = .{
                            .percentage = supervisor.random.intRangeAtMost(u8, 1, 10),
                        };
                        log.info("injecting network loss: {any}", .{supervisor.network.faults});
                        try supervisor.trace.write(.{
                            .name = .network_faults,
                            .process_id = vortex_process_ids.network,
                            .phase = .begin,
                        }, supervisor.network.faults);
                    },
                    .network_corrupt => {
                        if (!supervisor.network.faults.is_healed()) {
                            try supervisor.trace.write(.{
                                .name = .network_faults,
                                .process_id = vortex_process_ids.network,
                                .phase = .end,
                            }, .{});
                        }
                        supervisor.network.faults.corrupt = .{
                            .percentage = supervisor.random.intRangeAtMost(u8, 1, 10),
                        };
                        log.info("injecting network corruption: {any}", .{
                            supervisor.network.faults,
                        });
                        try supervisor.trace.write(.{
                            .name = .network_faults,
                            .process_id = vortex_process_ids.network,
                            .phase = .begin,
                        }, supervisor.network.faults);
                    },
                    .network_heal => {
                        log.info("healing network", .{});
                        supervisor.network.faults.heal();
                        try supervisor.trace.write(.{
                            .name = .network_faults,
                            .process_id = vortex_process_ids.network,
                            .phase = .end,
                        }, .{});
                    },
                    .quiesce => {
                        const duration = supervisor.random.intRangeAtMost(
                            u64,
                            constants.liveness_requirement_seconds,
                            constants.liveness_requirement_seconds * 2,
                        ) * std.time.ns_per_s;
                        sleep_deadline = now + duration;

                        supervisor.network.faults.heal();
                        try supervisor.trace.write(.{
                            .name = .network_faults,
                            .process_id = vortex_process_ids.network,
                            .phase = .end,
                        }, .{});
                        for (stopped_replicas) |stopped| {
                            _ = try stopped.replica.cont();
                            try supervisor.trace.write(.{
                                .name = .stopped,
                                .process_id = stopped.index,
                                .phase = .end,
                            }, .{});
                        }
                        for (terminated_replicas) |terminated| {
                            _ = try terminated.replica.start();
                            try supervisor.trace.write(.{
                                .name = .terminated,
                                .process_id = terminated.index,
                                .phase = .end,
                            }, .{});
                        }

                        log.info("going into {} quiescence (no faults)", .{
                            std.fmt.fmtDuration(duration),
                        });
                    },
                }
            }

            // Check for terminated replicas. Any other termination reason
            // than SIGKILL is considered unexpected and fails the test.
            for (supervisor.replicas, 0..) |replica, index| {
                if (replica.state() == .terminated) {
                    const replica_result = try replica.process.?.wait();
                    switch (replica_result) {
                        .Signal => |signal| {
                            switch (signal) {
                                std.posix.SIG.KILL => {},
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
            if (supervisor.workload.process.state() == .terminated) {
                log.info("workload terminated by itself", .{});
                break try supervisor.workload.process.wait();
            }
        } else blk: {
            // If the workload doesn't terminate by itself, we terminate it after we've run for the
            // required duration.
            log.info("terminating workload due to max duration", .{});
            break :blk if (supervisor.workload.process.state() == .running)
                try supervisor.workload.process.terminate()
            else
                try supervisor.workload.process.wait();
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
};

const ReplicaWithIndex = struct { replica: *Replica, index: u8 };

fn replicas_in_state(
    replicas: []*Replica,
    buffer: []ReplicaWithIndex,
    state: Replica.State,
) []ReplicaWithIndex {
    var count: u8 = 0;

    for (replicas, 0..) |replica, index| {
        if (replica.state() == state) {
            buffer[count] = .{ .replica = replica, .index = @intCast(index) };
            count += 1;
        }
    }
    return buffer[0..count];
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
    replica_index: u8,
    datafile: []const u8,
    log_debug: bool,
    process: ?*LoggedProcess,

    pub fn create(
        allocator: std.mem.Allocator,
        executable_path: []const u8,
        replica_index: u8,
        datafile: []const u8,
        log_debug: bool,
    ) !*Replica {
        const self = try allocator.create(Replica);
        errdefer allocator.destroy(self);

        self.* = .{
            .allocator = allocator,
            .executable_path = executable_path,
            .replica_index = replica_index,
            .datafile = datafile,
            .log_debug = log_debug,
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

        var addresses_buffer: [128]u8 = undefined;
        const addresses_arg = try std.fmt.bufPrint(
            addresses_buffer[0..],
            "--addresses={s}",
            .{replica_addresses_for_replicas[self.replica_index]},
        );

        var argv: stdx.BoundedArrayType([]const u8, 16) = .{};
        argv.append_slice_assume_capacity(&.{
            self.executable_path,
            "start",
        });
        if (self.log_debug) {
            argv.append_slice_assume_capacity(&.{
                "--log-debug",
                "--experimental",
            });
        }
        argv.append_slice_assume_capacity(&.{
            addresses_arg,
            self.datafile,
        });

        self.process = try LoggedProcess.spawn(self.allocator, argv.const_slice(), .{});
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

const Workload = struct {
    pub const State = enum(u8) { running, terminated };

    const RequestInfo = struct {
        timestamp_start_micros: u64,
        timestamp_end_micros: u64,
    };

    const Requests = RingBufferType(RequestInfo, .{ .array = 1024 * 16 });

    io: *IO,
    process: *LoggedProcess,
    trace: *TraceWriter,

    read_buffer: [@sizeOf(Progress)]u8 = undefined,
    read_completion: IO.Completion = undefined,
    read_progress: usize = 0,

    requests_finished: Requests = Requests.init(),

    pub fn spawn(
        allocator: std.mem.Allocator,
        args: CLIArgs,
        io: *IO,
        trace: *TraceWriter,
    ) !*Workload {
        var vortex_path_buffer: [std.fs.max_path_bytes]u8 = undefined;
        const vortex_path = try std.fs.selfExePath(&vortex_path_buffer);

        var driver_command_default_buffer: [std.fs.max_path_bytes]u8 = undefined;
        const driver_command_default = try std.fmt.bufPrint(
            &driver_command_default_buffer,
            "{s} driver",
            .{vortex_path},
        );

        const driver_command_selected = args.driver_command orelse driver_command_default;
        log.info("launching workload with driver: {s}", .{driver_command_selected});

        var driver_command_arg_buffer: [std.fs.max_path_bytes]u8 = undefined;
        const driver_command_arg = try std.fmt.bufPrint(
            &driver_command_arg_buffer,
            "--driver-command={s}",
            .{driver_command_selected},
        );

        const argv = &.{
            vortex_path,
            "workload",
            std.fmt.comptimePrint("--cluster-id={d}", .{constants.cluster_id}),
            std.fmt.comptimePrint("--addresses={s}", .{replica_addresses_for_clients}),
            driver_command_arg,
        };

        const workload = try allocator.create(Workload);
        errdefer allocator.destroy(workload);

        const process = try LoggedProcess.spawn(allocator, argv, .{
            .stdout_behavior = .Pipe,
        });
        errdefer process.destroy(allocator);

        workload.* = .{
            .io = io,
            .process = process,
            .trace = trace,
        };

        // Kick off read loop.
        workload.read();

        return workload;
    }

    pub fn destroy(workload: *Workload, allocator: std.mem.Allocator) void {
        assert(workload.process.state() == .terminated);
        workload.process.destroy(allocator);
        allocator.destroy(workload);
    }

    fn read(workload: *Workload) void {
        assert(workload.process.state() == .running);

        workload.io.read(
            *Workload,
            workload,
            on_read,
            &workload.read_completion,
            workload.process.child.stdout.?.handle,
            workload.read_buffer[workload.read_progress..workload.read_buffer.len],
            0,
        );
    }

    fn on_read(
        workload: *Workload,
        _: *IO.Completion,
        result: IO.ReadError!usize,
    ) void {
        if (workload.process.state() != .running) return;

        const count = result catch |err| {
            log.err("couldn't read from workload stdout: {}", .{err});
            return;
        };

        workload.read_progress += count;

        if (workload.read_progress >= workload.read_buffer.len) {
            const progress = std.mem.bytesAsValue(Progress, workload.read_buffer[0..]);
            const request_info: RequestInfo = .{
                .timestamp_start_micros = progress.timestamp_start_micros,
                .timestamp_end_micros = progress.timestamp_end_micros,
            };
            workload.read_progress = 0;
            workload.requests_finished.push(request_info) catch
                log.warn("requests_finished is full", .{});

            workload.trace.write(.{
                .name = .request,
                .process_id = vortex_process_ids.workload,
                .timestamp_micros = progress.timestamp_start_micros,
                .duration = progress.timestamp_end_micros - progress.timestamp_start_micros,
                .phase = .complete,
            }, .{ .event_count = progress.event_count }) catch {};
        }

        workload.read();
    }

    fn find_slow_request_since(workload: *const Workload, start_ns: u64) ?RequestInfo {
        var it = workload.requests_finished.iterator();
        while (it.next()) |request| {
            assert(request.timestamp_start_micros < request.timestamp_end_micros);
            // If a request started before the acceptably-faulty period, we ignore that part of
            // its duration.
            const duration_adjusted_micros = request.timestamp_end_micros -|
                @max(request.timestamp_start_micros, @divFloor(start_ns, 1000));
            if (duration_adjusted_micros > constants.liveness_requirement_micros) return request;
        }
        return null;
    }
};

// Based on:
// https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/preview?tab=t.0
const TraceEvent = struct {
    const EventName = enum {
        terminated,
        stopped,
        network_faults,
        request,
        liveness_required,
        test_failure,
    };
    const Phase = enum { instant, complete, begin, end };

    process_id: u8,
    thread_id: u32 = 0,
    timestamp_micros: ?u64 = null,
    duration: ?u64 = null,
    phase: Phase,
    name: ?EventName = null,
};

const TraceWriter = struct {
    trace_file: std.fs.File,
    stream: std.json.WriteStream(
        std.io.GenericWriter(std.fs.File, std.fs.File.WriteError, std.fs.File.write),
        .{ .checked_to_fixed_depth = 16 },
    ),
    timestamp_start: u64,

    fn from_file(trace_file_path: []const u8) !TraceWriter {
        const trace_file = try std.fs.cwd().createFile(trace_file_path, .{ .mode = 0o666 });
        errdefer trace_file.close();

        var stream = std.json.writeStreamMaxDepth(trace_file.writer(), .{}, 16);
        try stream.beginArray();

        return .{
            .trace_file = trace_file,
            .stream = stream,
            .timestamp_start = @intCast(std.time.microTimestamp()),
        };
    }

    fn deinit(writer: *TraceWriter) void {
        writer.stream.endArray() catch {};
        writer.trace_file.close();
    }

    fn process_name_assign(writer: *TraceWriter, process_id: u8, name: []const u8) !void {
        try writer.stream.beginObject();

        try writer.stream.objectField("ph");
        try writer.stream.write("M");

        try writer.stream.objectField("pid");
        try writer.stream.write(process_id);

        try writer.stream.objectField("name");
        try writer.stream.write("process_name");

        try writer.stream.objectField("args");
        try writer.stream.write(.{ .name = name });

        try writer.stream.endObject();
    }

    fn write(writer: *TraceWriter, event: TraceEvent, metadata: anytype) !void {
        try writer.stream.beginObject();

        try writer.stream.objectField("pid");
        try writer.stream.write(event.process_id);

        try writer.stream.objectField("tid");
        try writer.stream.write(event.thread_id);

        try writer.stream.objectField("ts");
        const timestamp = event.timestamp_micros orelse
            @as(u64, @intCast(std.time.microTimestamp()));
        try writer.stream.write(timestamp - writer.timestamp_start);

        if (event.duration) |dur| {
            try writer.stream.objectField("dur");
            try writer.stream.write(dur);
        }

        try writer.stream.objectField("ph");
        try writer.stream.write(switch (event.phase) {
            .instant => "i",
            .complete => "X",
            .begin => "B",
            .end => "E",
        });

        if (event.name) |name| {
            try writer.stream.objectField("name");
            try writer.stream.write(@tagName(name));
        }

        const datetime = stdx.DateTimeUTC.from_timestamp_ms(timestamp / 1000);
        var datetime_buffer: [24]u8 = undefined;
        var datetime_stream = std.io.fixedBufferStream(&datetime_buffer);
        stdx.DateTimeUTC.format(datetime, "", .{}, datetime_stream.writer()) catch return;

        try writer.stream.objectField("args");
        try writer.stream.write(.{
            .metadata = metadata,
            .timestamp = datetime_stream.getWritten(),
        });

        try writer.stream.endObject();
    }
};
