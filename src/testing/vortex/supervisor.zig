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
const Network = @import("./faulty_network.zig").Network;
const constants = @import("constants.zig");
const Progress = @import("./workload.zig").Progress;
const ratio = stdx.PRNG.ratio;
const Shell = @import("../../shell.zig");

const assert = std.debug.assert;
const maybe = stdx.maybe;
const log = std.log.scoped(.supervisor);

const dependencies_path: []const u8 = @import("vortex_options").dependencies_path;
const dependencies_count: u32 = @import("vortex_options").dependencies_count;

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

    comptime assert(dependencies_count > 0);
    if (dependencies_count == 1) {
        log.warn("not testing upgrades", .{});
    }

    // Executables are ordered from oldest to newest.
    const server_executables_all: [dependencies_count][]const u8 = comptime array: {
        var server_executables_all: [dependencies_count][]const u8 = undefined;
        for (&server_executables_all, 0..) |*server, i| {
            server.* = std.fmt.comptimePrint(
                "{s}/tigerbeetle-{d}",
                .{ dependencies_path, dependencies_count - i - 1 },
            );
        }
        break :array server_executables_all;
    };
    const driver_executables_all: [dependencies_count][]const u8 = comptime array: {
        var driver_executables_all: [dependencies_count][]const u8 = undefined;
        for (&driver_executables_all, 0..) |*server, i| {
            server.* = std.fmt.comptimePrint(
                "{s}/vortex-driver-zig-{d}",
                .{ dependencies_path, dependencies_count - i - 1 },
            );
        }
        break :array driver_executables_all;
    };

    const shell = try Shell.create(allocator);
    defer shell.destroy();

    const output_directory_relative = args.output_directory orelse try shell.create_tmp_dir();
    defer {
        if (args.output_directory == null) {
            shell.cwd.deleteTree(output_directory_relative) catch |err| {
                log.err("error deleting tree: {}", .{err});
            };
        }
    }

    const output_directory =
        try std.fs.cwd().realpathAlloc(shell.arena.allocator(), output_directory_relative);

    const seed = args.seed orelse std.crypto.random.int(u64);
    var prng = stdx.PRNG.from_seed(seed);

    // Even if we have past versions available, only use them sometimes.
    const release_count = prng.range_inclusive(u32, 1, dependencies_count);

    log.info("seed={}", .{seed});
    log.info("output_directory={s}", .{output_directory});
    log.info("duration={}", .{args.test_duration});
    log.info("releases={}/{}", .{ release_count, dependencies_count });

    const supervisor = try Supervisor.create(allocator, .{
        .prng = &prng,
        .shell = shell,
        .replica_count = args.replica_count,
        .output_directory = output_directory,
        .server_executables = &server_executables_all,
        .driver_executables = &driver_executables_all,
        .test_duration = args.test_duration,
        .faulty = !args.disable_faults,
        .log_debug = args.log_debug,
        .release_index = dependencies_count - release_count,
    });
    defer supervisor.destroy(allocator);

    for (0..args.replica_count) |replica_index| {
        try supervisor.replica_install(@intCast(replica_index), supervisor.options.release_index);
        try supervisor.replica_format(@intCast(replica_index));
        try supervisor.replica_start(@intCast(replica_index));
    }

    const test_deadline = std.time.nanoTimestamp() + supervisor.options.test_duration.ns;
    while (std.time.nanoTimestamp() < test_deadline) {
        try supervisor.tick();
    }

    log.info("workload: terminating due to max duration", .{});
    try supervisor.workload_terminate();
    log.info("workload: terminated as requested", .{});
}

const Supervisor = struct {
    allocator: std.mem.Allocator,
    prng: *stdx.PRNG,
    io: *IO,
    shell: *Shell,
    network: *Network,
    workload: *Workload,
    options: Options,

    replica_datafiles: []const []const u8,
    replicas: []*Replica,

    release_index: u32,
    release_count: u32,

    /// This represents the start timestamp of a period where we have an acceptable number of
    /// process faults, such that we require liveness (that requests are finished within a
    /// certain time period). If null, it means we're in a period of too many faults, thus
    /// enforcing no such requirement.
    acceptable_faults_start_ns: ?u64 = null,

    const Options = struct {
        prng: *stdx.PRNG,
        shell: *Shell,
        replica_count: u8,
        output_directory: []const u8,
        server_executables: []const []const u8,
        driver_executables: []const []const u8,
        test_duration: stdx.Duration,
        faulty: bool,
        log_debug: bool,
        release_index: u32,
    };

    fn create(allocator: std.mem.Allocator, options: Options) !*Supervisor {
        assert(options.replica_count > 0);
        assert(options.server_executables.len == options.driver_executables.len);
        assert(options.release_index < options.driver_executables.len);
        for (options.server_executables) |path| assert(std.fs.path.isAbsolute(path));
        for (options.driver_executables) |path| assert(std.fs.path.isAbsolute(path));

        var io = try allocator.create(IO);
        errdefer allocator.destroy(io);

        io.* = try IO.init(128, 0);
        errdefer io.deinit();

        const replica_ports_actual =
            constants.vortex.replica_ports_actual[0..options.replica_count];
        var network = try Network.listen(allocator, options.prng, io, replica_ports_actual);
        errdefer network.destroy(allocator);

        const replica_datafiles = try allocator.alloc([]const u8, options.replica_count);
        errdefer allocator.free(replica_datafiles);

        for (replica_datafiles, 0..) |*datafile, replica_index| {
            datafile.* = try options.shell.fmt(
                "{s}/{d}_{d}.tigerbeetle",
                .{ options.output_directory, constants.vortex.cluster_id, replica_index },
            );
        }

        const replicas = try allocator.alloc(*Replica, options.replica_count);
        errdefer allocator.free(replicas);

        for (replicas, 0..) |*replica, replica_index| {
            errdefer for (replicas[0..replica_index]) |r| r.destroy();

            var replica_ports: [constants.vsr.replicas_max]u16 = undefined;
            for (replica_ports[0..options.replica_count], 0..) |*replica_port, i| {
                if (replica_index == i) {
                    replica_port.* = network.proxies[i].remote_address.getPort();
                } else {
                    replica_port.* = network.proxies[i].origin_address.getPort();
                }
            }

            replica.* = try Replica.create(
                allocator,
                try options.shell.fmt(
                    "{s}/tigerbeetle-R{d:0>2}",
                    .{ options.output_directory, replica_index },
                ),
                options.release_index,
                options.replica_count,
                @intCast(replica_index),
                replica_ports,
            );
        }

        var proxy_ports_all: [constants.vsr.replicas_max]u16 = undefined;
        for (proxy_ports_all[0..options.replica_count], 0..) |*port, i| {
            port.* = network.proxies[i].origin_address.getPort();
        }
        const proxy_ports = proxy_ports_all[0..options.replica_count];

        // TODO Take client_release_min into account for driver.
        const workload_driver = options.driver_executables[options.release_index];
        const workload = try Workload.spawn(allocator, io, proxy_ports, workload_driver);
        errdefer workload.destroy(allocator);

        const supervisor = try allocator.create(Supervisor);
        errdefer allocator.destroy(supervisor);

        supervisor.* = .{
            .allocator = allocator,
            .prng = options.prng,
            .io = io,
            .shell = options.shell,
            .network = network,
            .workload = workload,
            .options = options,
            .replicas = replicas,
            .replica_datafiles = replica_datafiles,
            .release_index = options.release_index,
            .release_count = @intCast(options.server_executables.len),
        };
        return supervisor;
    }

    fn destroy(supervisor: *Supervisor, allocator: std.mem.Allocator) void {
        if (supervisor.workload.process.state == .running) {
            _ = supervisor.workload.process.terminate() catch {};
        }
        supervisor.workload.destroy(allocator);

        for (supervisor.replicas, 0..) |replica, replica_index| {
            // We might have terminated the replica and never restarted it,
            // so we need to check its state.
            if (replica.state() != .terminated) {
                supervisor.replica_terminate(@intCast(replica_index)) catch {};
            }
            replica.destroy();
        }
        allocator.free(supervisor.replicas);
        allocator.free(supervisor.replica_datafiles);
        supervisor.network.destroy(allocator);

        supervisor.io.deinit();
        allocator.destroy(supervisor.io);
        allocator.destroy(supervisor);
    }

    pub fn tick(supervisor: *Supervisor) !void {
        supervisor.network.tick();
        try supervisor.io.run_for_ns(constants.vsr.tick_ms * std.time.ns_per_ms);
        try supervisor.tick_check_liveness();

        if (supervisor.options.faulty) try supervisor.tick_faults();

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
            fatal(.workload_exit_early, "workload terminated by itself: code={}", .{code});
        }
    }

    fn tick_check_liveness(supervisor: *Supervisor) !void {
        if (supervisor.acceptable_faults_start_ns) |start_ns| {
            const now: u64 = @intCast(std.time.nanoTimestamp());
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

        const faulty_replica_count = count: {
            var count: u32 = 0;
            for (supervisor.replicas) |replica| {
                count += @intFromBool(replica.state() != .running);
            }
            break :count count;
        };

        // How many replicas can be faulty while still expecting the cluster to
        // make progress (based on 2f+1).
        const liveness_faulty_replicas_max = @divFloor(supervisor.replicas.len - 1, 2);
        // Check if `acceptable_faults_start_ns` should change state. If so, we reset the max
        // request duration too.
        // NOTE: Network faults are currently global, so we relax the requirement in such cases.
        if (faulty_replica_count <= liveness_faulty_replicas_max and
            supervisor.network.faults.is_healed())
        {
            // We have an acceptable number of faults, so we require liveness (after some time).
            if (supervisor.acceptable_faults_start_ns == null) {
                supervisor.acceptable_faults_start_ns = @intCast(std.time.nanoTimestamp());
                supervisor.workload.requests_finished.clear();
            }
        } else {
            // We have too many faults to require liveness.
            if (supervisor.acceptable_faults_start_ns) |_| {
                supervisor.acceptable_faults_start_ns = null;
                supervisor.workload.requests_finished.clear();
            }
        }
    }

    fn tick_faults(supervisor: *Supervisor) !void {
        assert(supervisor.options.faulty);

        var replicas_running_buffer: [constants.vsr.replicas_max]u8 = undefined;
        var replicas_terminated_buffer: [constants.vsr.replicas_max]u8 = undefined;
        var replicas_paused_buffer: [constants.vsr.replicas_max]u8 = undefined;

        const replicas_running =
            replicas_in_state(supervisor.replicas, &replicas_running_buffer, .running);
        const replicas_terminated =
            replicas_in_state(supervisor.replicas, &replicas_terminated_buffer, .terminated);
        const replicas_paused =
            replicas_in_state(supervisor.replicas, &replicas_paused_buffer, .paused);

        const Action = enum {
            none,
            replica_terminate,
            replica_restart,
            replica_pause,
            replica_resume,
            replica_upgrade,
            cluster_upgrade,
            network_delay,
            network_corrupt,
            network_heal,
            heal,
        };

        // Since "none" dominates the others, the fault values can be thought of as
        // "expected number of occurrences per 2 minutes".
        const minute_ticks = 60 * (std.time.ms_per_s / constants.vsr.tick_ms);
        switch (supervisor.prng.enum_weighted(Action, .{
            .none = 2 * minute_ticks,
            .replica_terminate = if (replicas_running.len > 0) 4 else 0,
            .replica_restart = if (replicas_terminated.len > 0) 3 else 0,
            .replica_pause = if (replicas_running.len > 0) 3 else 0,
            .replica_resume = if (replicas_paused.len > 0) 10 else 0,
            .replica_upgrade = if (supervisor.cluster_upgrading() != null) 15 else 0,
            .cluster_upgrade = if (supervisor.release_index + 1 <
                supervisor.release_count) 2 else 0,
            .network_delay = if (supervisor.network.faults.delay == null) 3 else 0,
            .network_corrupt = if (supervisor.network.faults.corrupt == null) 3 else 0,
            .network_heal = if (!supervisor.network.faults.is_healed()) 10 else 0,
            .heal = if (faulty_replica_count > 0 or
                !supervisor.network.faults.is_healed()) 1 else 0,
        })) {
            .none => {},
            .replica_terminate => {
                const pick = replicas_running[supervisor.prng.index(replicas_running)];
                try supervisor.replica_terminate(pick);
            },
            .replica_restart => {
                const pick = replicas_terminated[supervisor.prng.index(replicas_terminated)];
                try supervisor.replica_start(pick);
            },
            .replica_pause => {
                const pick = replicas_running[supervisor.prng.index(replicas_running)];
                try supervisor.replica_pause(pick);
            },
            .replica_resume => {
                const pick = replicas_paused[supervisor.prng.index(replicas_paused)];
                try supervisor.replica_unpause(pick);
            },
            .replica_upgrade => {
                try supervisor.replica_install(
                    supervisor.cluster_upgrading().?,
                    supervisor.release_index,
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
                log.info("injecting network corruption: {any}", .{ supervisor.network.faults });
            },
            .network_heal => {
                log.info("healing network", .{});
                supervisor.network.faults.heal();
            },
            .heal => {
                supervisor.network.faults.heal();
                for (replicas_paused) |index| try supervisor.replica_unpause(index);
                for (replicas_terminated) |index| try supervisor.replica_start(index);
                log.info("healing all faults", .{});
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

    pub fn replica_format(supervisor: *Supervisor, replica_index: u8) !void {
        assert(supervisor.replicas[replica_index].state() == .terminated);

        const server_executable = supervisor.options.server_executables[supervisor.release_index];
        supervisor.shell.exec(
            \\{tigerbeetle_executable} format
            \\    --cluster={cluster}
            \\    --replica={replica_index}
            \\    --replica-count={replica_count}
            \\    {datafile}
        , .{
            .tigerbeetle_executable = server_executable,
            .cluster = constants.vortex.cluster_id,
            .replica_index = replica_index,
            .replica_count = supervisor.replicas.len,
            .datafile = supervisor.replica_datafiles[replica_index],
        }) catch |err| {
            log.err("{}: failed formatting datafile: {}", .{ replica_index, err });
            return err;
        };
    }

    pub fn replica_start(supervisor: *Supervisor, replica_index: u8) !void {
        log.info("{}: starting replica", .{replica_index});

        const replica = supervisor.replicas[replica_index];
        assert(replica.state() == .terminated);
        defer assert(replica.state() == .running);

        const replica_addresses = try comma_separate_ports(
            supervisor.allocator,
            replica.replica_ports[0..supervisor.options.replica_count],
        );
        defer supervisor.allocator.free(replica_addresses);

        var addresses_buffer: [128]u8 = undefined;
        const addresses_arg =
            try std.fmt.bufPrint(addresses_buffer[0..], "--addresses={s}", .{replica_addresses});

        var argv: stdx.BoundedArrayType([]const u8, 16) = .{};
        argv.push_slice(&.{ replica.executable_target, "start" });
        if (supervisor.options.log_debug) {
            argv.push_slice(&.{ "--log-debug", "--experimental" });
        }
        argv.push_slice(&.{ addresses_arg, supervisor.replica_datafiles[replica_index] });

        assert(replica.process == null);
        replica.process = try LoggedProcess.spawn(supervisor.allocator, argv.const_slice(), .{});
    }

    pub fn replica_terminate(supervisor: *Supervisor, replica_index: u8) !void {
        log.info("{}: terminating replica", .{replica_index});

        const replica = supervisor.replicas[replica_index];
        assert(replica.state() == .running or replica.state() == .paused);
        defer assert(replica.state() == .terminated);

        _ = try replica.process.?.terminate();
        replica.process.?.destroy(supervisor.allocator);
        replica.process = null;
    }

    pub fn replica_pause(supervisor: *Supervisor, replica_index: u8) !void {
        log.info("{}: pausing replica", .{replica_index});

        const replica = supervisor.replicas[replica_index];
        assert(replica.state() == .running);
        defer assert(replica.state() == .paused);

        try replica.process.?.pause();
    }

    pub fn replica_unpause(supervisor: *Supervisor, replica_index: u8) !void {
        log.info("{}: unpausing replica", .{replica_index});

        const replica = supervisor.replicas[replica_index];
        assert(replica.state() == .paused);
        defer assert(replica.state() == .running);

        try replica.process.?.unpause();
    }

    pub fn replica_install(supervisor: *Supervisor, replica_index: u8, release_index: u32) !void {
        assert(release_index < supervisor.options.server_executables.len);

        log.info(
            "{}: upgrading replica to {}/{}",
            .{ replica_index, release_index, supervisor.options.server_executables.len - 1 },
        );

        supervisor.replicas[replica_index].executable_index = release_index;

        try std.fs.copyFileAbsolute(
            supervisor.options.server_executables[release_index],
            supervisor.replicas[replica_index].executable_target,
            .{},
        );
    }

    pub fn workload_terminate(supervisor: *Supervisor) !void {
        const workload_result = try supervisor.workload.process.terminate();
        if (!std.meta.eql(workload_result, .{ .Signal = std.posix.SIG.KILL })) {
            fatal(.workload_exit_result, "workload: unexpected term: {any}", .{workload_result});
        }
    }
};

fn replicas_in_state(
    replicas: []const *Replica,
    replica_index_buffer: []u8,
    state: LoggedProcess.State,
) []u8 {
    var count: u8 = 0;
    for (replicas, 0..) |replica, index| {
        if (replica.state() == state) {
            replica_index_buffer[count] = @intCast(index);
            count += 1;
        }
    }
    return replica_index_buffer[0..count];
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
    allocator: std.mem.Allocator,
    executable_index: u32,
    /// The path of this replica's executable. Executables from `executable_paths` are copied to
    /// this location.
    executable_target: []const u8,
    replica_count: u8,
    replica_index: u8,
    replica_ports: [constants.vsr.replicas_max]u16,
    process: ?*LoggedProcess,

    pub fn create(
        allocator: std.mem.Allocator,
        executable_target: []const u8,
        executable_index: u32,
        replica_count: u8,
        replica_index: u8,
        replica_ports: [constants.vsr.replicas_max]u16,
    ) !*Replica {
        assert(replica_index < replica_count);
        assert(std.fs.path.isAbsolute(executable_target));

        const self = try allocator.create(Replica);
        errdefer allocator.destroy(self);

        self.* = .{
            .allocator = allocator,
            .executable_target = executable_target,
            .executable_index = executable_index,
            .replica_count = replica_count,
            .replica_index = replica_index,
            .replica_ports = replica_ports,
            .process = null,
        };
        return self;
    }

    pub fn destroy(self: *Replica) void {
        assert(self.state() == .terminated);
        const allocator = self.allocator;
        if (self.process) |process| {
            process.destroy(allocator);
        }
        allocator.destroy(self);
    }

    pub fn state(self: *Replica) LoggedProcess.State {
        if (self.process) |process| {
            switch (process.state) {
                .running => return .running,
                .paused => return .paused,
                .terminated => return .terminated,
            }
        } else return .terminated;
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

        log.info("launching workload with driver: {s}", .{driver_command});

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
