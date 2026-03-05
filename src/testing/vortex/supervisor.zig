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
//! To launch a one-second smoke test, run this command from the repository root:
//!
//!     $ zig build test:integration -- "vortex smoke"
//!
//! If you need more control, you can run this program directly.
//!
//!     $ zig build vortex -- supervisor
//!
//! Other options:
//!
//! * Set the test duration by adding the `--test-duration=XmYs` option (it's 1 minute by default).
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
//! * upgrade clients
//! * multiple drivers? could use a special multiplexer driver that delegates to others

const std = @import("std");
const stdx = @import("stdx");
const builtin = @import("builtin");
const IO = @import("../../io.zig").IO;
const RingBufferType = stdx.RingBufferType;
const LoggedProcess = @import("./logged_process.zig");
const faulty_network = @import("./faulty_network.zig");
const constants = @import("constants.zig");
const Progress = @import("./workload.zig").Progress;
const ratio = stdx.PRNG.ratio;
const Shell = @import("../../shell.zig");

const log = std.log.scoped(.supervisor);
const dependencies_path: []const u8 = @import("vortex_options").dependencies_path;
const dependencies_count: u32 = @import("vortex_options").dependencies_count;

const assert = std.debug.assert;
const maybe = stdx.maybe;

pub const CLIArgs = struct {
    test_duration: stdx.Duration = .minutes(1),
    driver_command: ?[]const u8 = null,
    replica_count: u8 = 1,
    disable_faults: bool = false,
    output_directory: ?[]const u8 = null,
    log_debug: bool = false,
    /// Log file path.
    log: ?[]const u8 = null,

    @"--": void,
    /// Vortex is non-deterministic, but providing a seed can still help constrain the scenario.
    seed: ?u64 = null,
};

pub fn main(allocator: std.mem.Allocator, args: CLIArgs) !void {
    if (builtin.os.tag == .windows) {
        log.err("vortex is not supported on Windows", .{});
        return error.NotSupported;
    }

    if (args.log) |log_path| {
        const log_file = try std.fs.cwd().createFile(log_path, .{});
        defer log_file.close();

        // Redirect stderr to the file.
        try std.posix.dup2(log_file.handle, std.posix.STDERR_FILENO);
    }

    if (builtin.os.tag == .linux) {
        // Relaunch in fresh pid / network namespaces.
        try stdx.unshare.maybe_unshare_and_relaunch(allocator, .{
            .pid = true,
            .network = true,
        });
    } else {
        log.warn("vortex may spawn runaway processes when run on a non-Linux OS", .{});
        log.warn("vortex may encounter port collisions non-Linux OS", .{});
    }

    const shell = try Shell.create(allocator);
    defer shell.destroy();

    comptime assert(dependencies_count > 0);
    if (dependencies_count == 1) {
        log.warn("not testing upgrades", .{});
    }

    // Executables are ordered from oldest to newest.
    var server_executables_all: [dependencies_count][]const u8 = undefined;
    var driver_executables_all: [dependencies_count][]const u8 = undefined;
    for (&server_executables_all, &driver_executables_all, 0..) |*server, *driver, i| {
        server.* = try shell.fmt(
            "{s}/tigerbeetle-{d}",
            .{ dependencies_path, dependencies_count - i - 1 },
        );
        driver.* = try shell.fmt(
            "{s}/vortex-driver-zig-{d}",
            .{ dependencies_path, dependencies_count - i - 1 },
        );
    }

    // By default, the shell uses project root as cwd, but we want to use the actual process cwd.
    try shell.pushd_dir(std.fs.cwd());
    defer shell.popd();

    var io = try IO.init(128, 0);

    const output_directory_relative = args.output_directory orelse try shell.create_tmp_dir();
    defer {
        if (args.output_directory == null) {
            shell.cwd.deleteTree(output_directory_relative) catch |err| {
                log.err("error deleting tree: {}", .{err});
            };
        }
    }

    const output_directory =
        try shell.cwd.realpathAlloc(shell.arena.allocator(), output_directory_relative);

    log.info("output directory: {s}", .{output_directory});
    log.info("starting test with target runtime of {}", .{args.test_duration});

    const seed = args.seed orelse std.crypto.random.int(u64);
    var prng = stdx.PRNG.from_seed(seed);

    // Even if we have past versions available, only use them sometimes.
    const release_count = prng.range_inclusive(u32, 1, dependencies_count);
    const server_executables = server_executables_all[dependencies_count - release_count..];
    const driver_executables = driver_executables_all[dependencies_count - release_count..];
    assert(server_executables.len == release_count);
    assert(driver_executables.len == release_count);

    var network = try faulty_network.Network.listen(
        allocator,
        &prng,
        &io,
        constants.vortex.replica_ports_actual[0..args.replica_count],
    );
    defer network.destroy(allocator);

    var replicas: [constants.vsr.replicas_max]*Replica = undefined;
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

    var datafile_buffers: [constants.vsr.replicas_max][std.fs.max_path_bytes]u8 = undefined;
    for (0..args.replica_count) |replica_index| {
        const datafile = try std.fmt.bufPrint(
            datafile_buffers[replica_index][0..],
            "{s}/{d}_{d}.tigerbeetle",
            .{ output_directory, constants.vortex.cluster_id, replica_index },
        );

        shell.exec(
            \\{tigerbeetle_executable} format
            \\    --cluster={cluster}
            \\    --replica={replica_index}
            \\    --replica-count={replica_count}
            \\    {datafile}
        , .{
            .tigerbeetle_executable = server_executables[0],
            .cluster = constants.vortex.cluster_id,
            .replica_index = replica_index,
            .replica_count = args.replica_count,
            .datafile = datafile,
        }) catch |err| {
            log.err("failed formatting datafile: {}", .{err});
            return error.SetupFailed;
        };

        var replica_ports: [constants.vsr.replicas_max]u16 = undefined;
        for (replica_ports[0..args.replica_count], 0..) |*replica_port, i| {
            if (replica_index == i) {
                replica_port.* = network.proxies[i].remote_address.getPort();
            } else {
                replica_port.* = network.proxies[i].origin_address.getPort();
            }
        }

        var replica = try Replica.create(
            allocator,
            server_executables,
            try shell.fmt("{s}/tigerbeetle-R{d:0>2}", .{ output_directory, replica_index }),
            args.replica_count,
            @intCast(replica_index),
            replica_ports,
            datafile,
            args.log_debug,
        );

        replicas[replica_index] = replica;
        replicas_initialized += 1;

        try replica.start();
    }

    var proxy_ports: [constants.vsr.replicas_max]u16 = undefined;
    for (proxy_ports[0..args.replica_count], 0..) |*port, i| {
        port.* = network.proxies[i].origin_address.getPort();
    }

    log.info("launching workload with driver: {s}", .{driver_executables[0]});

    const workload = try Workload.spawn(
        allocator,
        &io,
        proxy_ports[0..args.replica_count],
        // TODO Take client_release_min into account.
        driver_executables[0],
    );
    defer {
        if (workload.process.state == .running) {
            _ = workload.process.terminate() catch {};
        }
        workload.destroy(allocator);
    }

    const supervisor = try Supervisor.create(allocator, .{
        .io = &io,
        .network = network,
        .replicas = replicas[0..args.replica_count],
        .workload = workload,
        .prng = &prng,
        .test_duration = args.test_duration,
        .faulty = !args.disable_faults,
    });
    defer supervisor.destroy(allocator);

    try supervisor.run();
}

const Supervisor = struct {
    io: *IO,
    network: *faulty_network.Network,
    replicas: []*Replica,
    workload: *Workload,
    prng: *stdx.PRNG,
    test_deadline: i128,
    faulty: bool,

    release_index: u32,
    release_count: u32,

    fn create(allocator: std.mem.Allocator, options: struct {
        io: *IO,
        network: *faulty_network.Network,
        replicas: []*Replica,
        workload: *Workload,
        prng: *stdx.PRNG,
        test_duration: stdx.Duration,
        faulty: bool,
    }) !*Supervisor {
        assert(options.replicas.len > 0);

        const supervisor = try allocator.create(Supervisor);
        errdefer allocator.destroy(supervisor);

        supervisor.* = .{
            .io = options.io,
            .network = options.network,
            .replicas = options.replicas,
            .workload = options.workload,
            .prng = options.prng,
            .test_deadline = std.time.nanoTimestamp() + options.test_duration.ns,
            .faulty = options.faulty,
            .release_index = 0,
            .release_count = @intCast(options.replicas[0].executable_paths.len),
        };
        return supervisor;
    }

    fn destroy(supervisor: *Supervisor, allocator: std.mem.Allocator) void {
        allocator.destroy(supervisor);
    }

    fn run(supervisor: *Supervisor) !void {
        var running_replicas_buffer: [constants.vsr.replicas_max]ReplicaWithIndex = undefined;
        var terminated_replicas_buffer: [constants.vsr.replicas_max]ReplicaWithIndex = undefined;
        var paused_replicas_buffer: [constants.vsr.replicas_max]ReplicaWithIndex = undefined;

        var sleep_deadline: u64 = 0;
        // This represents the start timestamp of a period where we have an acceptable number of
        // process faults, such that we require liveness (that requests are finished within a
        // certain time period). If null, it means we're in a period of too many faults, thus
        // enforcing no such requirement.
        var acceptable_faults_start_ns: ?u64 = null;
        // How many replicas can be faulty while still expecting the cluster to
        // make progress (based on 2f+1).
        const liveness_faulty_replicas_max = @divFloor(supervisor.replicas.len - 1, 2);
        const workload_result = while (std.time.nanoTimestamp() < supervisor.test_deadline) {
            supervisor.network.tick();
            try supervisor.io.run_for_ns(constants.vsr.tick_ms * std.time.ns_per_ms);
            const now: u64 = @intCast(std.time.nanoTimestamp());

            const running_replicas =
                replicas_in_state(supervisor.replicas, &running_replicas_buffer, .running);
            const terminated_replicas =
                replicas_in_state(supervisor.replicas, &terminated_replicas_buffer, .terminated);
            const paused_replicas =
                replicas_in_state(supervisor.replicas, &paused_replicas_buffer, .paused);

            const faulty_replica_count = terminated_replicas.len + paused_replicas.len;

            if (acceptable_faults_start_ns) |start_ns| {
                const deadline = start_ns + constants.vortex.liveness_requirement_seconds *
                    std.time.ns_per_s;
                // If we've been in a state with an acceptable number of faults for the required
                // amount of time, we should have seen finished requests.
                const no_finished_requests =
                    now > deadline and supervisor.workload.requests_finished.empty();
                // Also, those that do finish should not have too long durations, counting from the
                // start of the acceptably-faulty period.
                const too_slow_request = supervisor.workload.find_slow_request_since(start_ns);

                if (no_finished_requests) {
                    fatal(.liveness, "liveness check: no finished requests after {d} seconds", .{
                        constants.vortex.liveness_requirement_seconds,
                    });
                }

                if (too_slow_request) |_| {
                    fatal(.request_slow, "liveness check: too slow request", .{});
                }
            }

            // Check if `acceptable_faults_start_ns` should change state. If so, we reset the max
            // request duration too.
            // NOTE: Network faults are currently global, so we relax the requirement in such cases.
            if (faulty_replica_count <= liveness_faulty_replicas_max and
                supervisor.network.faults.is_healed())
            {
                // We have an acceptable number of faults, so we require liveness (after some time).
                if (acceptable_faults_start_ns == null) {
                    acceptable_faults_start_ns = @intCast(std.time.nanoTimestamp());
                    supervisor.workload.requests_finished.clear();
                }
            } else {
                // We have too many faults to require liveness.
                if (acceptable_faults_start_ns) |_| {
                    acceptable_faults_start_ns = null;
                    supervisor.workload.requests_finished.clear();
                }
            }

            if (sleep_deadline < now and supervisor.faulty) {
                const Action = enum {
                    sleep,
                    replica_terminate,
                    replica_restart,
                    replica_pause,
                    replica_resume,
                    replica_upgrade,
                    cluster_upgrade,
                    network_delay,
                    network_corrupt,
                    network_heal,
                    quiesce,
                };

                switch (supervisor.prng.enum_weighted(Action, .{
                    .sleep = 10,
                    .replica_terminate = if (running_replicas.len > 0) 4 else 0,
                    .replica_restart = if (terminated_replicas.len > 0) 3 else 0,
                    .replica_pause = if (running_replicas.len > 0) 3 else 0,
                    .replica_resume = if (paused_replicas.len > 0) 10 else 0,
                    .replica_upgrade = if (supervisor.cluster_upgrading() != null) 15 else 0,
                    .cluster_upgrade = if (supervisor.release_index + 1 <
                        supervisor.release_count) 5 else 0,
                    .network_delay = if (supervisor.network.faults.delay == null) 3 else 0,
                    .network_corrupt = if (supervisor.network.faults.corrupt == null) 3 else 0,
                    .network_heal = if (!supervisor.network.faults.is_healed()) 10 else 0,
                    .quiesce = if (faulty_replica_count > 0 or
                        !supervisor.network.faults.is_healed()) 1 else 0,
                })) {
                    .sleep => {
                        const duration =
                            supervisor.prng.int_inclusive(u64, 10 * std.time.ns_per_s);
                        log.info("sleeping for {}", .{std.fmt.fmtDuration(duration)});
                        sleep_deadline = now + duration;
                    },
                    .replica_terminate => {
                        const pick =
                            running_replicas[supervisor.prng.index(running_replicas)];
                        _ = try pick.replica.terminate();
                    },
                    .replica_restart => {
                        const pick =
                            terminated_replicas[supervisor.prng.index(terminated_replicas)];
                        try pick.replica.start();
                    },
                    .replica_pause => {
                        const pick =
                            running_replicas[supervisor.prng.index(running_replicas)];
                        try pick.replica.pause();
                    },
                    .replica_resume => {
                        const pick =
                            paused_replicas[supervisor.prng.index(paused_replicas)];
                        try pick.replica.unpause();
                    },
                    .replica_upgrade => {
                        const replica_index = supervisor.cluster_upgrading().?;
                        const replica = supervisor.replicas[replica_index];
                        replica.upgrade(supervisor.release_index) catch |err| {
                            std.debug.panic("error swapping executable: {}", .{err});
                        };
                        log.info(
                            "{}: upgrading replica to {}",
                            .{ replica_index, supervisor.release_index },
                        );
                    },
                    .cluster_upgrade => {
                        assert(supervisor.release_index + 1 < supervisor.release_count);
                        supervisor.release_index += 1;
                        log.info("upgrading cluster to {}", .{supervisor.release_index});
                    },
                    .network_delay => {
                        const time_ms = supervisor.prng.range_inclusive(u32, 10, 500);
                        supervisor.network.faults.delay = .{
                            .time_ms = time_ms,
                            .jitter_ms = @min(time_ms, 50),
                        };
                        log.info("injecting network delays: {any}", .{supervisor.network.faults});
                    },
                    .network_corrupt => {
                        supervisor.network.faults.corrupt =
                            ratio(supervisor.prng.range_inclusive(u8, 1, 10), 100);
                        log.info("injecting network corruption: {any}", .{
                            supervisor.network.faults,
                        });
                    },
                    .network_heal => {
                        log.info("healing network", .{});
                        supervisor.network.faults.heal();
                    },
                    .quiesce => {
                        const duration = supervisor.prng.range_inclusive(
                            u64,
                            constants.vortex.liveness_requirement_seconds,
                            constants.vortex.liveness_requirement_seconds * 2,
                        ) * std.time.ns_per_s;
                        sleep_deadline = now + duration;

                        supervisor.network.faults.heal();
                        for (paused_replicas) |paused| try paused.replica.unpause();
                        for (terminated_replicas) |terminated| try terminated.replica.start();

                        log.info("going into {} quiescence (no faults)", .{
                            std.fmt.fmtDuration(duration),
                        });
                    },
                }
            }

            // Check for replicas that have exited.
            for (supervisor.replicas, 0..) |replica, replica_index| {
                if (replica.state() != .terminated) {
                    if (replica.process.?.wait_nonblocking()) |term| {
                        // Replicas shouldn't exit on their own, even with code=0.
                        maybe(std.meta.eql(term, .{ .Exited = 0 }));

                        log.err(
                            "{}: replica terminated unexpectedly with {}",
                            .{ replica_index, term },
                        );
                        if (std.meta.eql(term, .{ .Signal = std.posix.SIG.KILL })) {
                            // If one of the replica dies to SIGKILL, it is likely an OOM.
                            // Bubble that up to CFO so that this Vortex run is counted as neither a
                            // success or failure.
                            std.posix.exit(@intCast(128 + term.Signal));
                        } else {
                            fatal(.replica_exit_result, "replica exited with: {}", .{term});
                        }
                    }
                }
            }

            if (supervisor.workload.process.wait_nonblocking()) |code| {
                fatal(
                    .workload_exit_early,
                    "workload terminated by itself: code={}",
                    .{code},
                );
            }
        } else blk: {
            log.info("terminating workload due to max duration", .{});
            break :blk try supervisor.workload.process.terminate();
        };

        switch (workload_result) {
            .Signal => |signal| {
                switch (signal) {
                    std.posix.SIG.KILL => log.info("workload terminated as requested", .{}),
                    else => {
                        fatal(
                            .workload_exit_result,
                            "workload exited unexpectedly with signal {d}",
                            .{signal},
                        );
                    },
                }
            },
            else => {
                fatal(
                    .workload_exit_result,
                    "unexpected workload result: {any}",
                    .{workload_result},
                );
            },
        }
    }

    fn cluster_upgrading(supervisor: *const Supervisor) ?u8 {
        const index_base = supervisor.prng.index(supervisor.replicas);
        for (0..supervisor.replicas.len) |index_offset| {
            const replica_index = (index_base + index_offset) % supervisor.replicas.len;
            const replica = supervisor.replicas[replica_index];
            if (replica.executable_index < supervisor.release_index) {
                return @intCast(replica_index);
            }
        }
        return null;
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

fn comma_separate_ports(allocator: std.mem.Allocator, ports: []const u16) ![]const u8 {
    assert(ports.len > 0);

    var out = std.ArrayList(u8).init(allocator);
    errdefer out.deinit();

    const writer = out.writer();
    try writer.print("{d}", .{ports[0]});
    for (ports[1..]) |port| try writer.print(",{d}", .{port});

    return out.toOwnedSlice();
}

test comma_separate_ports {
    const formatted = try comma_separate_ports(std.testing.allocator, &.{ 3000, 3001, 3002 });
    defer std.testing.allocator.free(formatted);

    try std.testing.expectEqualStrings("3000,3001,3002", formatted);
}

const Replica = struct {
    pub const State = enum(u8) { initial, running, paused, terminated };

    allocator: std.mem.Allocator,
    executable_paths: []const []const u8,
    executable_index: u32 = 0,
    /// The path of this replica's executable. Executables from `executable_paths` are copied to
    /// this location.
    executable_target: []const u8,
    replica_count: u8,
    replica_index: u8,
    replica_ports: [constants.vsr.replicas_max]u16,
    datafile: []const u8,
    log_debug: bool,
    process: ?*LoggedProcess,

    pub fn create(
        allocator: std.mem.Allocator,
        executable_paths: []const []const u8,
        executable_target: []const u8,
        replica_count: u8,
        replica_index: u8,
        replica_ports: [constants.vsr.replicas_max]u16,
        datafile: []const u8,
        log_debug: bool,
    ) !*Replica {
        assert(replica_index < replica_count);
        assert(executable_paths.len > 0);
        assert(std.fs.path.isAbsolute(executable_target));
        for (executable_paths) |exe| assert(std.fs.path.isAbsolute(exe));

        const self = try allocator.create(Replica);
        errdefer allocator.destroy(self);

        self.* = .{
            .allocator = allocator,
            .executable_paths = executable_paths,
            .executable_target = executable_target,
            .replica_count = replica_count,
            .replica_index = replica_index,
            .replica_ports = replica_ports,
            .datafile = datafile,
            .log_debug = log_debug,
            .process = null,
        };

        try std.fs.copyFileAbsolute(executable_paths[0], executable_target, .{});
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
            switch (process.state) {
                .running => return .running,
                .paused => return .paused,
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

        const replica_addresses =
            try comma_separate_ports(self.allocator, self.replica_ports[0..self.replica_count]);
        defer self.allocator.free(replica_addresses);

        var addresses_buffer: [128]u8 = undefined;
        const addresses_arg = try std.fmt.bufPrint(
            addresses_buffer[0..],
            "--addresses={s}",
            .{replica_addresses},
        );

        var argv: stdx.BoundedArrayType([]const u8, 16) = .{};
        argv.push_slice(&.{ self.executable_target, "start" });
        if (self.log_debug) {
            argv.push_slice(&.{ "--log-debug", "--experimental" });
        }
        argv.push_slice(&.{ addresses_arg, self.datafile });

        log.info("{}: starting replica", .{self.replica_index});
        self.process = try LoggedProcess.spawn(self.allocator, argv.const_slice(), .{});
    }

    pub fn terminate(self: *Replica) !std.process.Child.Term {
        assert(self.state() == .running or self.state() == .paused);
        defer assert(self.state() == .terminated);

        log.info("{}: terminating replica", .{self.replica_index});
        return try self.process.?.terminate();
    }

    pub fn pause(self: *Replica) !void {
        assert(self.state() == .running);
        defer assert(self.state() == .paused);

        log.info("{}: pausing replica", .{self.replica_index});
        try self.process.?.pause();
    }

    pub fn unpause(self: *Replica) !void {
        assert(self.state() == .paused);
        defer assert(self.state() == .running);

        log.info("{}: unpausing replica", .{self.replica_index});
        try self.process.?.unpause();
    }

    pub fn upgrade(self: *Replica, release_index: u32) !void {
        assert(self.executable_index < release_index);
        assert(self.executable_paths.len > release_index);
        self.executable_index = release_index;

        try std.fs.copyFileAbsolute(
            self.executable_paths[release_index],
            self.executable_target,
            .{},
        );
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

    read_buffer: [@sizeOf(Progress)]u8 = undefined,
    read_completion: IO.Completion = undefined,
    read_progress: usize = 0,

    requests_finished: Requests = Requests.init(),

    pub fn spawn(
        allocator: std.mem.Allocator,
        io: *IO,
        proxy_ports: []const u16,
        driver_command: []const u8,
    ) !*Workload {
        var vortex_path_buffer: [std.fs.max_path_bytes]u8 = undefined;
        const vortex_path = try std.fs.selfExePath(&vortex_path_buffer);

        var driver_command_arg_buffer: [std.fs.max_path_bytes]u8 = undefined;
        const driver_command_arg = try std.fmt.bufPrint(
            &driver_command_arg_buffer,
            "--driver-command={s}",
            .{driver_command},
        );

        const proxy_addresses = try comma_separate_ports(allocator, proxy_ports);
        defer allocator.free(proxy_addresses);

        const arg_addresses =
            try std.fmt.allocPrint(allocator, "--addresses={s}", .{proxy_addresses});
        defer allocator.free(arg_addresses);

        const argv = &.{
            vortex_path,
            "workload",
            std.fmt.comptimePrint("--cluster={d}", .{constants.vortex.cluster_id}),
            arg_addresses,
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
        };

        // Kick off read loop.
        workload.read();

        return workload;
    }

    pub fn destroy(workload: *Workload, allocator: std.mem.Allocator) void {
        assert(workload.process.state == .terminated);
        workload.process.destroy(allocator);
        allocator.destroy(workload);
    }

    fn read(workload: *Workload) void {
        assert(workload.process.state == .running);

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
        if (workload.process.state != .running) return;

        const count = result catch |err| {
            fatal(.workload_read_error, "couldn't read from workload stdout: {}", .{err});
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

            log.debug("workload: request done duration={}us events={}", .{
                progress.timestamp_end_micros - progress.timestamp_start_micros,
                progress.event_count,
            });
        }

        workload.read();
    }

    fn find_slow_request_since(workload: *const Workload, start_ns: u64) ?RequestInfo {
        var it = workload.requests_finished.iterator();
        while (it.next()) |request| {
            assert(request.timestamp_start_micros < request.timestamp_end_micros);
            // If a request started before the acceptably-faulty period,
            // we ignore that part of its duration.
            const duration_adjusted_micros = request.timestamp_end_micros -|
                @max(request.timestamp_start_micros, @divFloor(start_ns, 1000));
            if (duration_adjusted_micros > constants.vortex.liveness_requirement_micros) {
                return request;
            }
        }
        return null;
    }
};

const FatalReason = enum(u8) {
    workload_exit_early = 10,
    workload_exit_result = 11,
    workload_read_error = 12,
    replica_exit_result = 13,
    liveness = 14,
    request_slow = 15,

    pub fn exit_status(reason: FatalReason) u8 {
        return @intFromEnum(reason);
    }
};

fn fatal(reason: FatalReason, comptime fmt: []const u8, args: anytype) noreturn {
    log.err(fmt, args);
    const status = reason.exit_status();
    assert(status != 0);
    std.process.exit(status);
}
