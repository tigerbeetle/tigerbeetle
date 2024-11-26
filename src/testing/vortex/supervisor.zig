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
//! Control the test duration by adding the `--test-duration-minutes=N` option (it's 10 minutes by
//! default).
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
//! * liveness checks
//! * multiple drivers? could use a special multiplexer driver that delegates to others

const std = @import("std");
const builtin = @import("builtin");
const IO = @import("../../io.zig").IO;
const LoggedProcess = @import("./logged_process.zig");
const faulty_network = @import("./faulty_network.zig");
const arbitrary = @import("./arbitrary.zig");
const constants = @import("./constants.zig");
const Stats = @import("./workload.zig").Stats;

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

pub const CLIArgs = struct {
    tigerbeetle_executable: []const u8,
    test_duration_minutes: u16 = 10,
    driver_command: ?[]const u8 = null,
    disable_faults: bool = false,
    trace: ?[]const u8 = null,
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

    var trace = if (args.trace) |trace_file_path|
        try TraceWriter.from_file(trace_file_path)
    else
        .noop;
    defer trace.deinit();

    try trace.begin();
    defer trace.end() catch {};

    log.info(
        "starting test with target runtime of {d}m",
        .{args.test_duration_minutes},
    );
    const test_duration_ns = @as(u64, @intCast(args.test_duration_minutes)) * std.time.ns_per_min;
    const test_deadline = std.time.nanoTimestamp() + test_duration_ns;

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
        );

        replicas[replica_index] = replica;
        replicas_initialized += 1;

        try trace.process_name(
            replica_index,
            std.fmt.comptimePrint("Replica {d}", .{replica_index}),
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
    try trace.process_name(4, "Network");

    const workload = try Workload.spawn(allocator, args, &io, &trace);
    defer {
        if (workload.process.state() == .running) {
            _ = workload.process.terminate() catch {};
        }
        workload.destroy(allocator);
    }

    var sleep_deadline: u64 = 0;

    const workload_result = while (std.time.nanoTimestamp() < test_deadline) {
        network.tick();
        try io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);

        const now: u64 = @intCast(std.time.nanoTimestamp());
        if (now < sleep_deadline) continue;

        if (!args.disable_faults) {
            // Things we do in this main loop, except supervising the replicas.
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

            var running_replicas_buffer: [constants.replica_count]ReplicaWithIndex = undefined;
            const running_replicas = replicas_in_state(
                &replicas,
                &running_replicas_buffer,
                .running,
            );
            var terminated_replicas_buffer: [constants.replica_count]ReplicaWithIndex = undefined;
            const terminated_replicas = replicas_in_state(
                &replicas,
                &terminated_replicas_buffer,
                .terminated,
            );
            var stopped_replicas_buffer: [constants.replica_count]ReplicaWithIndex = undefined;
            const stopped_replicas = replicas_in_state(
                &replicas,
                &stopped_replicas_buffer,
                .stopped,
            );

            switch (arbitrary.weighted(random, Action, .{
                .sleep = 10,
                .replica_terminate = if (running_replicas.len > 0 and
                    terminated_replicas.len == 0 and
                    stopped_replicas.len == 0) 3 else 0,
                .replica_restart = if (terminated_replicas.len > 0) 3 else 0,
                .replica_stop = if (running_replicas.len > 0 and
                    terminated_replicas.len == 0 and
                    stopped_replicas.len == 0) 2 else 0,
                .replica_resume = if (stopped_replicas.len > 0) 10 else 0,
                .network_delay = if (network.faults.delay == null) 3 else 0,
                .network_lose = if (network.faults.lose == null) 3 else 0,
                .network_corrupt = if (network.faults.corrupt == null) 3 else 0,
                .network_heal = if (!network.faults.is_healed()) 10 else 0,
                .quiesce = if (terminated_replicas.len > 0 or
                    stopped_replicas.len > 0 or
                    !network.faults.is_healed()) 1 else 0,
            }).?) {
                .sleep => {
                    const duration = random.uintLessThan(u64, 10 * std.time.ns_per_s);
                    log.info("sleeping for {}", .{std.fmt.fmtDuration(duration)});
                    sleep_deadline = now + duration;
                },
                .replica_terminate => {
                    const pick = arbitrary.element(random, ReplicaWithIndex, running_replicas);
                    log.info("terminating replica {d}", .{pick.index});
                    try trace.write(.{
                        .name = "terminated",
                        .pid = pick.index,
                        .ts = @intCast(std.time.microTimestamp()),
                        .ph = .B,
                    }, .{});
                    _ = try pick.replica.terminate();
                },
                .replica_restart => {
                    const pick = arbitrary.element(random, ReplicaWithIndex, terminated_replicas);
                    log.info("restarting replica {d}", .{pick.index});
                    try trace.write(.{
                        .name = "terminated",
                        .pid = pick.index,
                        .ts = @intCast(std.time.microTimestamp()),
                        .ph = .E,
                    }, .{});
                    _ = try pick.replica.start();
                },
                .replica_stop => {
                    const pick = arbitrary.element(random, ReplicaWithIndex, running_replicas);
                    log.info("stopping replica {d}", .{pick.index});
                    try trace.write(.{
                        .name = "stopped",
                        .pid = pick.index,
                        .ts = @intCast(std.time.microTimestamp()),
                        .ph = .B,
                    }, .{});
                    _ = try pick.replica.stop();
                },
                .replica_resume => {
                    const pick = arbitrary.element(random, ReplicaWithIndex, stopped_replicas);
                    log.info("resuming replica {d}", .{pick.index});
                    try trace.write(.{
                        .name = "stopped",
                        .pid = pick.index,
                        .ts = @intCast(std.time.microTimestamp()),
                        .ph = .E,
                    }, .{});
                    _ = try pick.replica.cont();
                },
                .network_delay => {
                    if (!network.faults.is_healed()) {
                        try trace.write(.{
                            .name = "faults",
                            .pid = 4,
                            .ts = @intCast(std.time.microTimestamp()),
                            .ph = .E,
                        }, .{});
                    }
                    const time_ms = random.intRangeAtMost(u32, 10, 500);
                    network.faults.delay = .{
                        .time_ms = time_ms,
                        .jitter_ms = @min(time_ms, 50),
                    };
                    log.info("injecting network delays: {any}", .{network.faults});
                    try trace.write(.{
                        .name = "faults",
                        .pid = 4,
                        .ts = @intCast(std.time.microTimestamp()),
                        .ph = .B,
                    }, network.faults);
                },
                .network_lose => {
                    if (!network.faults.is_healed()) {
                        try trace.write(.{
                            .name = "faults",
                            .pid = 4,
                            .ts = @intCast(std.time.microTimestamp()),
                            .ph = .E,
                        }, .{});
                    }
                    network.faults.lose = .{
                        .percentage = random.intRangeAtMost(u8, 1, 10),
                    };
                    log.info("injecting network loss: {any}", .{network.faults});
                    try trace.write(.{
                        .name = "faults",
                        .pid = 4,
                        .ts = @intCast(std.time.microTimestamp()),
                        .ph = .B,
                    }, network.faults);
                },
                .network_corrupt => {
                    if (!network.faults.is_healed()) {
                        try trace.write(.{
                            .name = "faults",
                            .pid = 4,
                            .ts = @intCast(std.time.microTimestamp()),
                            .ph = .E,
                        }, .{});
                    }
                    network.faults.corrupt = .{
                        .percentage = random.intRangeAtMost(u8, 1, 10),
                    };
                    log.info("injecting network corruption: {any}", .{network.faults});
                    try trace.write(.{
                        .name = "faults",
                        .pid = 4,
                        .ts = @intCast(std.time.microTimestamp()),
                        .ph = .B,
                    }, network.faults);
                },
                .network_heal => {
                    log.info("healing network", .{});
                    network.faults.heal();
                    try trace.write(.{
                        .name = "faults",
                        .pid = 4,
                        .ts = @intCast(std.time.microTimestamp()),
                        .ph = .E,
                    }, .{});
                },
                .quiesce => {
                    const duration = random.intRangeAtMost(u64, 60, 120) * std.time.ns_per_s;
                    sleep_deadline = now + duration;

                    network.faults.heal();
                    try trace.write(.{
                        .name = "faults",
                        .pid = 4,
                        .ts = @intCast(std.time.microTimestamp()),
                        .ph = .E,
                    }, .{});
                    for (stopped_replicas) |stopped| {
                        _ = try stopped.replica.cont();
                        try trace.write(.{
                            .name = "stopped",
                            .pid = stopped.index,
                            .ts = @intCast(std.time.microTimestamp()),
                            .ph = .E,
                        }, .{});
                    }
                    for (terminated_replicas) |terminated| {
                        _ = try terminated.replica.start();
                        try trace.write(.{
                            .name = "terminated",
                            .pid = terminated.index,
                            .ts = @intCast(std.time.microTimestamp()),
                            .ph = .E,
                        }, .{});
                    }

                    log.info("going into {} quiescence (no faults)", .{
                        std.fmt.fmtDuration(duration),
                    });
                },
            }
        }

        // CHeck for terminated replicas. Any other termination reason
        // than SIGKILL is considered unexpected and fails the test.
        for (replicas, 0..) |replica, index| {
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
        if (workload.process.state() == .terminated) {
            log.info("workload terminated by itself", .{});
            break try workload.process.wait();
        }
    } else blk: {
        // If the workload doesn't terminate by itself, we terminate it after we've run for the
        // required duration.
        log.info("terminating workload due to max duration", .{});
        break :blk if (workload.process.state() == .running)
            try workload.process.terminate()
        else
            try workload.process.wait();
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
    process: ?*LoggedProcess,

    pub fn create(
        allocator: std.mem.Allocator,
        executable_path: []const u8,
        replica_index: u8,
        datafile: []const u8,
    ) !*Replica {
        const self = try allocator.create(Replica);
        errdefer allocator.destroy(self);

        self.* = .{
            .allocator = allocator,
            .executable_path = executable_path,
            .replica_index = replica_index,
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

        var addresses_buffer: [128]u8 = undefined;
        const addresses_arg = try std.fmt.bufPrint(
            addresses_buffer[0..],
            "--addresses={s}",
            .{replica_addresses_for_replicas[self.replica_index]},
        );

        const argv = &.{
            self.executable_path,
            "start",
            addresses_arg,
            self.datafile,
        };

        self.process = try LoggedProcess.spawn(self.allocator, argv, .{});
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

    io: *IO,
    process: *LoggedProcess,
    trace: *TraceWriter,

    read_buffer: [@sizeOf(Stats)]u8 = undefined,
    read_completion: IO.Completion = undefined,
    read_progress: usize = 0,

    pub fn spawn(
        allocator: std.mem.Allocator,
        args: CLIArgs,
        io: *IO,
        trace: *TraceWriter,
    ) !*Workload {
        const workload = try allocator.create(Workload);
        errdefer allocator.destroy(workload);

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

        workload.* = .{
            .io = io,
            .process = try LoggedProcess.spawn(allocator, argv, .{
                .stdout_behavior = .Pipe,
            }),
            .trace = trace,
        };

        try trace.process_name(5, "Workload");

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
        const count = result catch |err| {
            log.err("couldn't read from workload stdout: {}", .{err});
            return;
        };

        workload.read_progress += count;

        if (workload.read_progress >= workload.read_buffer.len) {
            const stats = std.mem.bytesAsValue(Stats, workload.read_buffer[0..]);
            workload.read_progress = 0;

            workload.trace.write(.{
                .name = "request",
                .pid = 5,
                .ts = stats.timestamp_start_micros,
                .dur = stats.timestamp_end_micros - stats.timestamp_start_micros,
                .ph = .X,
            }, .{ .event_count = stats.event_count }) catch {};
        }

        workload.read();
    }
};

const TraceEvent = struct {
    const Phase = enum { X, B, E };

    pid: u8,
    tid: u32 = 0,
    ts: u64,
    dur: ?u64 = null,
    ph: Phase,
    name: ?[]const u8 = null,
};

const TraceWriter = union(enum) {
    chrome_tracing: struct {
        trace_file: std.fs.File,
        stream: std.json.WriteStream(
            std.io.GenericWriter(std.fs.File, std.fs.File.WriteError, std.fs.File.write),
            .{ .checked_to_fixed_depth = 16 },
        ),
        timestamp_start: u64,
    },
    noop: void,

    fn from_file(trace_file_path: []const u8) !TraceWriter {
        const trace_file = try std.fs.cwd().createFile(trace_file_path, .{ .mode = 0o666 });
        return .{
            .chrome_tracing = .{
                .trace_file = trace_file,
                .stream = std.json.writeStreamMaxDepth(trace_file.writer(), .{}, 16),
                .timestamp_start = @intCast(std.time.microTimestamp()),
            },
        };
    }

    fn noop() !TraceWriter {
        return .noop;
    }

    // The following pointer switching shenanigans are required due to:
    // https://github.com/ziglang/zig/issues/13856

    fn deinit(writer: *TraceWriter) void {
        switch (writer.*) {
            .chrome_tracing => |*file_writer| file_writer.trace_file.close(),
            .noop => {},
        }
    }

    fn begin(writer: *TraceWriter) !void {
        switch (writer.*) {
            .chrome_tracing => |*file_writer| try file_writer.stream.beginArray(),
            .noop => {},
        }
    }

    fn end(writer: *TraceWriter) !void {
        switch (writer.*) {
            .chrome_tracing => |*file_writer| try file_writer.stream.endArray(),
            .noop => {},
        }
    }

    fn process_name(writer: *TraceWriter, pid: u8, name: []const u8) !void {
        switch (writer.*) {
            .chrome_tracing => |*file_writer| {
                try file_writer.stream.beginObject();

                try file_writer.stream.objectField("ph");
                try file_writer.stream.write("M");

                try file_writer.stream.objectField("pid");
                try file_writer.stream.write(pid);

                try file_writer.stream.objectField("name");
                try file_writer.stream.write("process_name");

                try file_writer.stream.objectField("args");
                try file_writer.stream.write(.{ .name = name });

                try file_writer.stream.endObject();
            },
            .noop => {},
        }
    }

    fn write(writer: *TraceWriter, event: TraceEvent, args: anytype) !void {
        switch (writer.*) {
            .chrome_tracing => |*file_writer| {
                try file_writer.stream.beginObject();

                try file_writer.stream.objectField("pid");
                try file_writer.stream.write(event.pid);

                try file_writer.stream.objectField("tid");
                try file_writer.stream.write(event.tid);

                try file_writer.stream.objectField("ts");
                try file_writer.stream.write(event.ts - file_writer.timestamp_start);

                if (event.dur) |dur| {
                    try file_writer.stream.objectField("dur");
                    try file_writer.stream.write(dur);
                }

                try file_writer.stream.objectField("ph");
                try file_writer.stream.write(event.ph);

                if (event.name) |name| {
                    try file_writer.stream.objectField("name");
                    try file_writer.stream.write(name);
                }

                try file_writer.stream.objectField("args");
                try file_writer.stream.write(args);

                try file_writer.stream.endObject();
            },
            .noop => {},
        }
    }
};
