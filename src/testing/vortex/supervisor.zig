//! The Vortex _supervisor_ is a program that runs:
//!
//! * a set of TigerBeetle replicas, forming a cluster
//! * a workload that runs commands and queries against the cluster, verifying its correctness
//!   (whatever that means is up to the workload)
//!
//! The replicas and driver run as child processes, while the supervisor restarts terminated
//! replicas and injects crashes and network faults. After some configurable amount of time, the
//! supervisor terminates the driver and replicas, unless the driver exits on its own or if
//! any of the replicas exit unexpectedly.
//!
//! If no replicas (or the driver) crash, the vortex exits successfully.
//!
//! To launch a one-second smoke test, run this command from the repository root:
//!
//!     $ zig build test:integration -- "vortex smoke"
//!
//! If you need more control, you can run this program directly.
//!
//!     $ zig build vortex
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

    if (dependencies_count == 1 or args.disable_faults or args.driver_command != null) {
        log.warn("not testing upgrades", .{});
    }

    const seed = args.seed orelse std.crypto.random.int(u64);
    var prng = stdx.PRNG.from_seed(seed);

    // Even if we have past versions available, only use them sometimes.
    const release_min = prng.range_inclusive(
        u32,
        if (args.disable_faults or args.driver_command != null) dependencies_count - 1 else 0,
        dependencies_count - 1,
    );

    const supervisor = try Supervisor.create(allocator, .{
        .seed = prng.int(u64),
        .replica_count = args.replica_count,
        .faulty = !args.disable_faults,
        .log_debug = args.log_debug,
    });
    defer supervisor.destroy();

    log.info("seed={}", .{seed});
    log.info("output_directory={s}", .{supervisor.output_directory});
    log.info("duration={}", .{args.test_duration});
    log.info("release={}/{}", .{ release_min, dependencies_count });

    for (0..args.replica_count) |replica_index| {
        try supervisor.replica_install(@intCast(replica_index), release_min);
        try supervisor.replica_format(@intCast(replica_index));
        try supervisor.replica_start(@intCast(replica_index));
    }
    try supervisor.workload_start(
        if (args.driver_command) |driver_command|
            .{ .command = driver_command }
        else
            .{ .release = supervisor.prng.range_inclusive(u32, 0, release_min) },
        .{ .transfer_count = std.math.maxInt(u32) },
    );

    const test_deadline = std.time.nanoTimestamp() + args.test_duration.ns;
    while (std.time.nanoTimestamp() < test_deadline) {
        try supervisor.tick();
    }

    log.info("workload: terminating due to max duration", .{});
    log.info("workload: created accounts={}", .{supervisor.workload.?.model.accounts.count()});
    log.info("workload: created transfers={}", .{supervisor.workload.?.model.transfers_created});
    for (std.enums.values(Workload.Command)) |command| {
        log.info("workload: completed command={s} count={}", .{
            @tagName(command),
            supervisor.workload.?.requests_finished_count.getAssertContains(command),
        });
    }
    supervisor.workload_terminate();
    log.info("done", .{});
}

/// Executables are ordered from oldest to newest.
fn configuration() struct {
    server_executables: [dependencies_count][]const u8,
    driver_executables: [dependencies_count][]const u8,
} {
    comptime assert(dependencies_count > 0);
    const server_executables: [dependencies_count][]const u8 = comptime array: {
        var executables: [dependencies_count][]const u8 = undefined;
        for (&executables, 0..) |*server, i| {
            server.* = std.fmt.comptimePrint(
                "{s}/tigerbeetle-{d}",
                .{ dependencies_path, dependencies_count - i - 1 },
            );
        }
        break :array executables;
    };
    const driver_executables: [dependencies_count][]const u8 = comptime array: {
        var executables: [dependencies_count][]const u8 = undefined;
        for (&executables, 0..) |*server, i| {
            server.* = std.fmt.comptimePrint(
                "{s}/vortex-driver-zig-{d}",
                .{ dependencies_path, dependencies_count - i - 1 },
            );
        }
        break :array executables;
    };
    return .{
        .server_executables = server_executables,
        .driver_executables = driver_executables,
    };
}

pub const Supervisor = struct {
    allocator: std.mem.Allocator,
    prng: stdx.PRNG,
    io: *IO,
    shell: *Shell,
    network: *Network,
    workload: ?*Workload = null,
    options: Options,

    server_executables: [dependencies_count][]const u8,
    driver_executables: [dependencies_count][]const u8,
    release_count: u32,

    output_directory: []const u8,
    replica_datafiles: []const []const u8,
    replicas: []*Replica,

    /// This represents the start timestamp of a period where we have an acceptable number of
    /// process faults, such that we require liveness (that requests are finished within a
    /// certain time period). If null, it means we're in a period of too many faults, thus
    /// enforcing no such requirement.
    acceptable_faults_start_ns: ?u64 = null,

    const Options = struct {
        seed: u64,
        replica_count: u8,
        faulty: bool,
        log_debug: bool,
    };

    pub fn create(allocator: std.mem.Allocator, options: Options) !*Supervisor {
        const dependencies = configuration();
        assert(dependencies.server_executables.len == dependencies.driver_executables.len);
        for (dependencies.server_executables) |path| assert(std.fs.path.isAbsolute(path));
        for (dependencies.driver_executables) |path| assert(std.fs.path.isAbsolute(path));

        assert(options.replica_count > 0);

        const shell = try Shell.create(allocator);
        errdefer shell.destroy();

        const output_directory = try shell.create_tmp_dir();
        errdefer {
            shell.cwd.deleteTree(output_directory) catch |err| {
                log.err("error deleting tree: {}", .{err});
            };
        }

        var prng = stdx.PRNG.from_seed(options.seed);

        var io = try allocator.create(IO);
        errdefer allocator.destroy(io);

        io.* = try IO.init(128, 0);
        errdefer io.deinit();

        const replica_ports_actual =
            constants.vortex.replica_ports_actual[0..options.replica_count];
        var network = try Network.listen(allocator, &prng, io, replica_ports_actual);
        errdefer network.destroy(allocator);

        const replica_datafiles = try allocator.alloc([]const u8, options.replica_count);
        errdefer allocator.free(replica_datafiles);

        for (replica_datafiles, 0..) |*datafile, replica_index| {
            datafile.* = try shell.fmt(
                "{s}/{d}_{d}.tigerbeetle",
                .{ output_directory, constants.vortex.cluster_id, replica_index },
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
                try shell.fmt("{s}/tigerbeetle-R{d:0>2}", .{ output_directory, replica_index }),
                options.replica_count,
                @intCast(replica_index),
                replica_ports,
            );
        }

        const supervisor = try allocator.create(Supervisor);
        errdefer allocator.destroy(supervisor);

        supervisor.* = .{
            .allocator = allocator,
            .prng = prng,
            .io = io,
            .shell = shell,
            .network = network,
            .options = options,
            .output_directory = output_directory,
            .server_executables = dependencies.server_executables,
            .driver_executables = dependencies.driver_executables,
            .release_count = @intCast(dependencies.server_executables.len),
            .replicas = replicas,
            .replica_datafiles = replica_datafiles,
        };
        return supervisor;
    }

    pub fn destroy(supervisor: *Supervisor) void {
        if (supervisor.workload) |workload| {
            workload.destroy(supervisor.allocator);
        }

        for (supervisor.replicas, 0..) |replica, replica_index| {
            // We might have terminated the replica and never restarted it,
            // so we need to check its state.
            if (replica.state() != .terminated) {
                supervisor.replica_terminate(@intCast(replica_index)) catch {};
            }
            replica.destroy();
        }
        supervisor.allocator.free(supervisor.replicas);
        supervisor.allocator.free(supervisor.replica_datafiles);
        supervisor.network.destroy(supervisor.allocator);

        supervisor.io.deinit();
        supervisor.allocator.destroy(supervisor.io);

        supervisor.shell.cwd.deleteTree(supervisor.output_directory) catch |err| {
            log.err("error deleting tree: {}", .{err});
        };
        supervisor.shell.destroy();
        supervisor.allocator.destroy(supervisor);
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
    }

    fn tick_check_liveness(supervisor: *Supervisor) !void {
        const workload = supervisor.workload orelse return;
        if (supervisor.acceptable_faults_start_ns) |start_ns| {
            const now: u64 = @intCast(std.time.nanoTimestamp());
            const deadline = start_ns + constants.vortex.liveness_requirement_seconds *
                std.time.ns_per_s;
            // If we've been in a state with an acceptable number of faults for the required
            // amount of time, we should have seen finished requests.
            const no_finished_requests =
                now > deadline and workload.requests_finished.empty();
            // Also, those that do finish should not have too long durations, counting from the
            // start of the acceptably-faulty period.
            const too_slow_request = workload.find_slow_request_since(start_ns);

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
                workload.requests_finished.clear();
            }
        } else {
            // We have too many faults to require liveness.
            if (supervisor.acceptable_faults_start_ns) |_| {
                supervisor.acceptable_faults_start_ns = null;
                workload.requests_finished.clear();
            }
        }
    }

    fn tick_faults(supervisor: *Supervisor) !void {
        assert(supervisor.options.faulty);

        const prng = &supervisor.prng;
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

        const cluster_release_ = supervisor.cluster_release();
        // Since "none" dominates the others, the fault values can be thought of as
        // "expected number of occurrences per 2 minutes".
        const minute_ticks = 60 * (std.time.ms_per_s / constants.vsr.tick_ms);
        switch (supervisor.prng.enum_weighted(Action, .{
            .none = 2 * minute_ticks,
            .replica_terminate = if (replicas_running.len > 0) 2 else 0,
            .replica_restart = if (replicas_terminated.len > 0) 4 else 0,
            .replica_pause = if (replicas_running.len > 0) 2 else 0,
            .replica_resume = if (replicas_paused.len > 0) 10 else 0,
            .replica_upgrade = if (supervisor.cluster_upgrading()) |_| 15 else 0,
            .cluster_upgrade = if (cluster_release_ + 1 < supervisor.release_count) 2 else 0,
            .network_delay = if (supervisor.network.faults.delay == null) 2 else 0,
            .network_corrupt = if (supervisor.network.faults.corrupt == null) 2 else 0,
            .network_heal = if (supervisor.network.faults.is_healed()) 0 else 10,
            .heal = 10,
        })) {
            .none => {},
            .replica_terminate => {
                try supervisor.replica_terminate(replicas_running[prng.index(replicas_running)]);
            },
            .replica_restart => {
                try supervisor.replica_start(replicas_terminated[prng.index(replicas_terminated)]);
            },
            .replica_pause => {
                try supervisor.replica_pause(replicas_running[prng.index(replicas_running)]);
            },
            .replica_resume => {
                try supervisor.replica_unpause(replicas_paused[prng.index(replicas_paused)]);
            },
            .replica_upgrade => {
                try supervisor.replica_install(supervisor.cluster_upgrading().?, cluster_release_);
            },
            .cluster_upgrade => {
                assert(cluster_release_ + 1 < supervisor.release_count);
                const release_max = supervisor.release_count - 1;
                const release_target = prng.range_inclusive(u32, cluster_release_ + 1, release_max);
                log.info("upgrading cluster to {}..{}", .{ cluster_release_, release_target });

                try supervisor.replica_install(
                    @intCast(prng.index(supervisor.replicas)),
                    release_target,
                );
            },
            .network_delay => {
                const time_ms = prng.range_inclusive(u32, 10, 500);
                supervisor.network.faults.delay = .{
                    .time_ms = time_ms,
                    .jitter_ms = @min(time_ms, 50),
                };
                log.info("injecting network delays: {any}", .{supervisor.network.faults});
            },
            .network_corrupt => {
                supervisor.network.faults.corrupt = ratio(prng.range_inclusive(u8, 1, 10), 100);
                log.info("injecting network corruption: {any}", .{supervisor.network.faults});
            },
            .network_heal => {
                log.info("healing network faults", .{});
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

    fn cluster_release(supervisor: *const Supervisor) u32 {
        var release_max: u32 = 0;
        for (supervisor.replicas) |replica| {
            release_max = @max(release_max, replica.executable_index);
        }
        return release_max;
    }

    fn cluster_upgrading(supervisor: *Supervisor) ?u8 {
        const cluster_release_ = supervisor.cluster_release();
        const index_base = supervisor.prng.index(supervisor.replicas);
        for (0..supervisor.replicas.len) |index_offset| {
            const replica_index = (index_base + index_offset) % supervisor.replicas.len;
            const replica = supervisor.replicas[replica_index];
            if (replica.executable_index < cluster_release_) {
                return @intCast(replica_index);
            }
        }
        return null;
    }

    pub fn replica_format(supervisor: *Supervisor, replica_index: u8) !void {
        assert(supervisor.replicas[replica_index].state() == .terminated);

        const release_index = supervisor.replicas[replica_index].executable_index;
        const server_executable = supervisor.server_executables[release_index];
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

    pub fn replica_reformat(supervisor: *Supervisor, replica_index: u8) !void {
        assert(supervisor.replicas[replica_index].state() == .terminated);

        supervisor.shell.cwd.deleteFile(supervisor.replica_datafiles[replica_index]) catch |err| {
            log.err("{}: failed deleting datafile: {}", .{ replica_index, err });
            return err;
        };

        const release_index = supervisor.replicas[replica_index].executable_index;
        const server_executable = supervisor.server_executables[release_index];
        const child = supervisor.shell.spawn(.{ .stderr_behavior = .Inherit },
            \\{tigerbeetle} recover
            \\    --cluster={cluster_id}
            \\    --replica={replica}
            \\    --replica-count={replica_count}
            \\    --addresses={addresses}
            \\    {datafile}
        , .{
            .tigerbeetle = server_executable,
            .cluster_id = constants.vortex.cluster_id,
            .replica = replica_index,
            .replica_count = supervisor.replicas.len,
            .addresses = supervisor.replicas[replica_index].addresses,
            .datafile = supervisor.replica_datafiles[replica_index],
        }) catch |err| {
            log.err("{}: failed reformatting datafile: {}", .{ replica_index, err });
            return err;
        };

        // Tick supervisor since reformatting requires network progress.
        // (The tick limit is an arbitrary safety counter.)
        const ticks_max = 1500;
        for (0..ticks_max) |_| {
            const result = std.posix.waitpid(child.id, std.posix.W.NOHANG);
            if (result.pid == 0) {
                try supervisor.tick();
            } else {
                assert(result.pid == child.id);

                const status = stdx.term_from_status(result.status);
                if (std.meta.eql(status, .{ .Exited = 0 })) {
                    break;
                } else {
                    log.err("{}: reformat failed: {}", .{ replica_index, status });
                    return error.ReformatFailed;
                }
            }
        } else {
            log.err("{}: reformat did not complete within {} ticks", .{ replica_index, ticks_max });
            return error.ReformatFailed;
        }
    }

    pub fn replica_start(supervisor: *Supervisor, replica_index: u8) !void {
        log.info("{}: starting replica", .{replica_index});

        const replica = supervisor.replicas[replica_index];
        assert(replica.state() == .terminated);
        defer assert(replica.state() == .running);

        var addresses_buffer: [128]u8 = undefined;
        const addresses_arg = try std.fmt.bufPrint(
            addresses_buffer[0..],
            "--addresses={s}",
            .{supervisor.replicas[replica_index].addresses},
        );

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
        assert(release_index < supervisor.release_count);

        log.info(
            "{}: upgrading replica to {}/{}",
            .{ replica_index, release_index, supervisor.release_count - 1 },
        );
        supervisor.replicas[replica_index].executable_index = release_index;

        const upgrade_requires_restart = builtin.os.tag != .linux and
            supervisor.replicas[replica_index].state() != .terminated;
        if (upgrade_requires_restart) {
            try supervisor.replica_terminate(replica_index);
        }

        try std.fs.copyFileAbsolute(
            supervisor.server_executables[release_index],
            supervisor.replicas[replica_index].executable_target,
            .{},
        );

        if (upgrade_requires_restart) {
            try supervisor.replica_start(replica_index);
        }
    }

    pub fn workload_start(
        supervisor: *Supervisor,
        driver: union(enum) {
            command: []const u8,
            release: u32,
        },
        options: struct { transfer_count: u32 },
    ) !void {
        assert(supervisor.workload == null);

        var proxy_ports_all: [constants.vsr.replicas_max]u16 = undefined;
        for (proxy_ports_all[0..supervisor.options.replica_count], 0..) |*port, i| {
            port.* = supervisor.network.proxies[i].origin_address.getPort();
        }
        const proxy_ports = proxy_ports_all[0..supervisor.options.replica_count];

        // TODO Take client_release_min into account for driver.
        const workload_driver = switch (driver) {
            .command => |command| command,
            .release => |release| supervisor.driver_executables[release],
        };
        log.info("launching workload with driver: {s}", .{workload_driver});

        const workload = try Workload.create(
            supervisor.allocator,
            supervisor.io,
            proxy_ports,
            workload_driver,
            .{ .seed = supervisor.prng.int(u64) },
        );
        errdefer workload.destroy(supervisor.allocator);

        try workload.start(.{ .transfer_count = options.transfer_count });
        supervisor.workload = workload;
    }

    pub fn workload_done(supervisor: *Supervisor) bool {
        return supervisor.workload.?.done();
    }

    pub fn workload_terminate(supervisor: *Supervisor) void {
        supervisor.workload.?.destroy(supervisor.allocator);
        supervisor.workload = null;
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
    /// The path of this replica's executable.
    /// Executables from `server_executables` are copied to this location.
    executable_target: []const u8,
    replica_count: u8,
    replica_index: u8,
    replica_ports: [constants.vsr.replicas_max]u16,
    addresses: []const u8,
    process: ?*LoggedProcess,

    pub fn create(
        allocator: std.mem.Allocator,
        executable_target: []const u8,
        replica_count: u8,
        replica_index: u8,
        replica_ports: [constants.vsr.replicas_max]u16,
    ) !*Replica {
        assert(replica_index < replica_count);
        assert(std.fs.path.isAbsolute(executable_target));

        const addresses = try comma_separate_ports(allocator, replica_ports[0..replica_count]);
        errdefer allocator.free(addresses);

        const self = try allocator.create(Replica);
        errdefer allocator.destroy(self);

        self.* = .{
            .allocator = allocator,
            .executable_target = executable_target,
            .executable_index = 0,
            .replica_count = replica_count,
            .replica_index = replica_index,
            .replica_ports = replica_ports,
            .addresses = addresses,
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
        allocator.free(self.addresses);
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
    const Model = @import("./workload.zig").Model;
    const Generator = @import("./workload.zig").Generator;
    const Command = @import("./workload.zig").Command;

    const RequestInfo = struct {
        timestamp_start_micros: u64,
        timestamp_end_micros: u64,
    };

    const Requests = RingBufferType(RequestInfo, .{ .array = 1024 * 16 });

    io: *IO,
    model: Model,
    generator: Generator,
    driver: std.process.Child,

    status: union(enum) {
        idle,
        busy: struct { transfers_max: u32 },
    } = .idle,

    command: ?Command = null,
    request_buffer: []u8,
    request_written: ?u32 = null,
    request_size: ?u32 = null,
    request_start: ?stdx.InstantUnix = null,
    reply_buffer: []u8,

    completion: IO.Completion = undefined,
    read_progress: usize = 0,

    requests_finished: Requests = Requests.init(),
    requests_finished_count: std.enums.EnumMap(Command, u32) = .initFull(0),

    pub fn create(
        allocator: std.mem.Allocator,
        io: *IO,
        proxy_ports: []const u16,
        driver_command: []const u8,
        options: struct { seed: u64 },
    ) !*Workload {
        assert(std.mem.indexOfScalar(u8, driver_command, '"') == null);

        const arg_addresses = try comma_separate_ports(allocator, proxy_ports);
        defer allocator.free(arg_addresses);

        var driver = std.process.Child.init(&.{
            driver_command,
            std.fmt.comptimePrint("{d}", .{constants.vortex.cluster_id}),
            arg_addresses,
        }, allocator);
        driver.stdin_behavior = .Pipe;
        driver.stdout_behavior = .Pipe;
        driver.stderr_behavior = .Inherit;
        try driver.spawn();
        errdefer _ = driver.kill() catch {};

        var model = try Model.init(allocator);
        errdefer model.deinit(allocator);

        const buffer_size = @sizeOf(u8) + @sizeOf(u32) + Generator.buffer_size;
        const request_buffer = try allocator.alloc(u8, buffer_size);
        errdefer allocator.free(request_buffer);

        const reply_buffer = try allocator.alloc(u8, buffer_size);
        errdefer allocator.free(reply_buffer);

        const workload = try allocator.create(Workload);
        errdefer allocator.destroy(workload);

        workload.* = .{
            .io = io,
            .model = model,
            .generator = Generator.init(options.seed),
            .driver = driver,
            .request_buffer = request_buffer,
            .reply_buffer = reply_buffer,
        };
        return workload;
    }

    pub fn destroy(workload: *Workload, allocator: std.mem.Allocator) void {
        const workload_result = workload.driver.kill() catch |err| {
            fatal(.workload_exit_result, "workload: error killing driver: {any}", .{err});
        };
        if (!std.meta.eql(workload_result, .{ .Signal = std.posix.SIG.TERM })) {
            fatal(.workload_exit_result, "workload: unexpected term: {any}", .{workload_result});
        }

        allocator.free(workload.reply_buffer);
        allocator.free(workload.request_buffer);
        workload.model.deinit(allocator);
        allocator.destroy(workload);
    }

    pub fn done(workload: *Workload) bool {
        return workload.status == .idle;
    }

    pub fn start(workload: *Workload, options: struct { transfer_count: u32 }) !void {
        assert(workload.status == .idle);

        workload.status = .{ .busy = .{ .transfers_max = options.transfer_count } };
        workload.driver_request();
    }

    fn driver_request(workload: *Workload) void {
        assert(workload.status == .busy);
        assert(workload.command == null);
        assert(workload.request_written == null);
        assert(workload.request_size == null);
        assert(workload.request_start == null);

        const command = workload.generator.random_command(&workload.model);
        const operation = command.operation();
        var stream = std.io.fixedBufferStream(workload.request_buffer);
        stream.writer().writeInt(u8, @intFromEnum(operation), .little) catch unreachable;

        const request_body_size = workload.generator.random_request(
            &workload.model,
            command,
            workload.request_buffer[stream.pos + @sizeOf(u32) ..],
        );
        const request_body_events_count: u32 =
            @intCast(@divExact(request_body_size, operation.event_size()));
        stream.writer().writeInt(u32, request_body_events_count, .little) catch unreachable;

        log.debug(
            "workload: request start: command={s} body={}",
            .{ @tagName(command), request_body_size },
        );

        workload.command = command;
        workload.request_written = 0;
        workload.request_size = @intCast(stream.pos + request_body_size);
        workload.request_start = stdx.InstantUnix.now();
        workload.driver_request_write();
    }

    fn driver_request_write(workload: *Workload) void {
        assert(workload.status == .busy);
        assert(workload.command != null);
        assert(workload.request_written.? < workload.request_size.?);

        workload.io.write(
            *Workload,
            workload,
            driver_request_write_callback,
            &workload.completion,
            workload.driver.stdin.?.handle,
            workload.request_buffer[workload.request_written.?..workload.request_size.?],
            0,
        );
    }

    fn driver_request_write_callback(
        workload: *Workload,
        completion: *IO.Completion,
        result: IO.WriteError!usize,
    ) void {
        assert(workload.status == .busy);
        assert(&workload.completion == completion);
        assert(workload.command != null);
        assert(workload.read_progress == 0);

        const bytes_written = result catch |err| {
            fatal(.driver_request_error, "error sending to driver: {}", .{err});
        };
        workload.request_written.? += @intCast(bytes_written);

        assert(workload.request_written.? <= workload.request_size.?);
        if (workload.request_written.? == workload.request_size.?) {
            workload.request_written = null;
            workload.driver_response_read();
        } else {
            workload.driver_request_write();
        }
    }

    fn driver_response_read(workload: *Workload) void {
        assert(workload.status == .busy);
        assert(workload.command != null);

        workload.io.read(
            *Workload,
            workload,
            driver_response_read_callback,
            &workload.completion,
            workload.driver.stdout.?.handle,
            workload.reply_buffer[workload.read_progress..],
            0,
        );
    }

    fn driver_response_read_callback(
        workload: *Workload,
        completion: *IO.Completion,
        result: IO.ReadError!usize,
    ) void {
        assert(workload.status == .busy);
        assert(workload.command != null);
        assert(&workload.completion == completion);

        const read_size = result catch |err| {
            fatal(.driver_response_error, "error receiving from driver: {}", .{err});
        };
        workload.read_progress += read_size;

        const read_buffer = workload.reply_buffer[0..workload.read_progress];
        var read_stream = std.io.fixedBufferStream(read_buffer);
        const reader = read_stream.reader();

        if (workload.read_progress < @sizeOf(u32)) return workload.driver_response_read();
        const results_count = reader.readInt(u32, .little) catch unreachable;
        const results_size = results_count * workload.command.?.operation().result_size();
        if (workload.read_progress < read_stream.pos + results_size) {
            return workload.driver_response_read();
        }

        const results_buffer = workload.reply_buffer[read_stream.pos..][0..results_size];
        workload.model.reconcile(
            workload.command.?,
            workload.request_buffer[(@sizeOf(u8) + @sizeOf(u32))..workload.request_size.?],
            results_buffer,
        ) catch |err| {
            fatal(.workload_reconcile, "model reconcile error: {}", .{err});
        };

        const request_commence_us = workload.request_start.?.ns / std.time.ns_per_us;
        const request_complete_us = stdx.InstantUnix.now().ns / std.time.ns_per_us;
        workload.requests_finished.push(.{
            .timestamp_start_micros = request_commence_us,
            .timestamp_end_micros = request_complete_us,
        }) catch log.warn("requests_finished is full", .{});

        workload.requests_finished_count.put(
            workload.command.?,
            workload.requests_finished_count.getAssertContains(workload.command.?) + 1,
        );

        log.info(
            "workload: request done: command={s} duration={}us " ++
                "(accounts_created={d} transfers_created={d})",
            .{
                @tagName(workload.command.?),
                request_complete_us -| request_commence_us,
                workload.model.accounts.count(),
                workload.model.transfers_created,
            },
        );

        workload.command = null;
        workload.request_size = null;
        workload.request_start = null;
        workload.read_progress = 0;
        if (workload.model.transfers_created < workload.status.busy.transfers_max) {
            workload.driver_request();
        } else {
            workload.status = .idle;
        }
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
    workload_reconcile = 13,
    replica_exit_result = 14,
    driver_request_error = 15,
    driver_response_error = 16,
    liveness = 17,
    request_slow = 18,

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
