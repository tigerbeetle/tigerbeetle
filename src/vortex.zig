/// Run a cluster of TigerBeetle replicas, a client driver, a workload, all with fault injection,
/// to test the whole system.
///
/// On Linux, Vortex runs in a Linux namespace where it can control the network.
const std = @import("std");
const stdx = @import("stdx");
const builtin = @import("builtin");
const ratio = stdx.PRNG.ratio;

const Release = @import("multiversion.zig").Release;
const Supervisor = @import("testing/vortex/supervisor.zig").Supervisor;
const Command = @import("testing/vortex/workload.zig").Command;
const dependencies_count: u32 = @import("vortex_options").dependencies_count;

const assert = std.debug.assert;
const log = std.log.scoped(.vortex);

pub const std_options: std.Options = .{
    .log_level = .info,
    .logFn = stdx.log_with_timestamp,
};

const CLIArgs = struct {
    scenario: Scenario = .default,
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

const Scenario = enum {
    default,
    upgrade,
    recover,
};

pub fn main(init: std.process.Init) !void {
    comptime assert(builtin.target.cpu.arch.endian() == .little);

    if (builtin.os.tag == .windows) {
        // Vortex is not currently supported on Windows because of child process management.
        // e.g. waitpid, pause/unpause.
        log.err("vortex is not supported for Windows", .{});
        return error.NotSupported;
    }

    if (builtin.os.tag == .macos) {
        // Vortex is not currently supported on MacOS because io.write() is implemented with
        // pwrite(), which doesn't work on non-seekable streams like child process input/output.
        log.err("vortex is not supported for MacOS", .{});
        return error.NotSupported;
    }
    assert(builtin.os.tag == .linux);

    var gpa_allocator = std.heap.DebugAllocator(.{}){};
    defer switch (gpa_allocator.deinit()) {
        .ok => {},
        .leak => @panic("memory leak"),
    };

    const gpa = gpa_allocator.allocator();

    var flags = stdx.Flags.init(gpa);
    defer flags.deinit(gpa);

    const args = flags.parse(CLIArgs, init.minimal.args);

    if (args.log) |log_path| {
        const log_file = try std.Io.Dir.cwd().createFile(init.io, log_path, .{});
        defer log_file.close(init.io);

        // Redirect stderr to the file.
        switch (std.posix.errno(std.os.linux.dup2(log_file.handle, std.posix.STDERR_FILENO))) {
            .SUCCESS => {},
            else => |err| return stdx.unexpected_errno("dup2", err),
        }
    }

    if (builtin.os.tag == .linux) {
        // Relaunch in fresh pid / network namespaces.
        try stdx.unshare.maybe_unshare_and_relaunch(gpa, init.io, init.minimal.args, .{
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

    const seed = args.seed orelse stdx.crypto_random_int(init.io, u64);
    var prng = stdx.PRNG.from_seed(seed);

    log.info("seed={}", .{seed});
    switch (args.scenario) {
        .default => try scenario_default(gpa, init, &prng, args),
        .upgrade => try scenario_upgrade(gpa, init, &prng),
        .recover => try scenario_recover(gpa, init, &prng),
    }

    log.info("done", .{});
}

fn scenario_default(
    gpa: std.mem.Allocator,
    init: std.process.Init,
    prng: *stdx.PRNG,
    args: CLIArgs,
) !void {
    assert(args.scenario == .default);

    // Even if we have past versions available, only use them sometimes.
    const release_min = prng.range_inclusive(
        u32,
        if (args.disable_faults or args.driver_command != null) dependencies_count - 1 else 0,
        dependencies_count - 1,
    );

    const supervisor = try Supervisor.create(gpa, init.io, init.environ_map, .{
        .seed = prng.int(u64),
        .replica_count = args.replica_count,
        .faulty = !args.disable_faults,
        .log_debug = args.log_debug,
    });
    defer supervisor.destroy();

    log.info("output_directory={s}", .{supervisor.output_directory});
    log.info("duration={f}", .{args.test_duration});
    log.info("releases={f}", .{Release.format_slice(&supervisor.releases)});

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

    var timer = stdx.Timer.init(supervisor.time.interface());
    while (timer.read().ns < args.test_duration.ns) {
        try supervisor.tick();
    }

    log.info("workload: terminating due to max duration", .{});
    log.info("workload: created accounts={}", .{supervisor.workload.?.model.accounts.count()});
    log.info("workload: created transfers={}", .{supervisor.workload.?.model.transfers_created});
    for (std.enums.values(Command)) |command| {
        log.info("workload: completed command={s} count={}", .{
            @tagName(command),
            supervisor.workload.?.requests_finished_count.getAssertContains(command),
        });
    }
    supervisor.workload_terminate();
}

fn scenario_upgrade(gpa: std.mem.Allocator, init: std.process.Init, prng: *stdx.PRNG) !void {
    const replica_count = 3;
    const duration_max = stdx.Duration.seconds(200);
    const tick_ms = 10;
    const ticks_max = duration_max.to_ms() / tick_ms;

    var supervisor = try Supervisor.create(gpa, init.io, init.environ_map, .{
        .seed = prng.int(u64),
        .replica_count = replica_count,
        .faulty = false,
        .log_debug = false,
    });
    defer supervisor.destroy();

    assert(supervisor.release_count > 0);
    const release_past = supervisor.release_count - 2;
    const release_current = supervisor.release_count - 1;

    for (0..replica_count) |replica_index| {
        try supervisor.replica_install(@intCast(replica_index), release_past);
        try supervisor.replica_format(@intCast(replica_index));
    }
    try supervisor.workload_start(.{ .release = release_past }, .{ .transfer_count = 1_000_000 });

    for (0..replica_count) |replica_index| {
        try supervisor.replica_start(@intCast(replica_index));
    }

    // Schedule the replica upgrades.
    var upgrade_tick: [replica_count]u64 = @splat(0);
    for (0..replica_count) |replica_index| {
        upgrade_tick[replica_index] = supervisor.prng.int_inclusive(u64, ticks_max / 2);
    }

    for (0..ticks_max) |tick| {
        try supervisor.tick();

        for (0..replica_count) |replica_index| {
            if (tick == upgrade_tick[replica_index]) {
                try supervisor.replica_install(@intCast(replica_index), release_current);
            }
        }

        const early = tick < ticks_max / 2;
        const replica_index = supervisor.prng.index(supervisor.replicas);
        const crash = early and supervisor.prng.chance(ratio(1, 400));
        const restart = (!early) or supervisor.prng.chance(ratio(1, 200));

        if (supervisor.replicas[replica_index].state == .terminated and restart) {
            try supervisor.replica_start(@intCast(replica_index));
        } else if (supervisor.replicas[replica_index].state == .running and crash) {
            try supervisor.replica_terminate(@intCast(replica_index));
        }
    }

    if (!supervisor.workload_done()) {
        return error.WorkloadIncomplete;
    }
}

fn scenario_recover(gpa: std.mem.Allocator, init: std.process.Init, prng: *stdx.PRNG) !void {
    const replica_count = 3;

    var supervisor = try Supervisor.create(gpa, init.io, init.environ_map, .{
        .seed = prng.int(u64),
        .replica_count = replica_count,
        .faulty = false,
        .log_debug = false,
    });
    defer supervisor.destroy();

    const release_current = supervisor.release_count - 1;

    for (0..replica_count) |replica_index| {
        try supervisor.replica_install(@intCast(replica_index), release_current);
        try supervisor.replica_format(@intCast(replica_index));
        try supervisor.replica_start(@intCast(replica_index));
    }
    try supervisor.workload_start(.{ .release = release_current }, .{ .transfer_count = 100_000 });
    for (0..400) |_| try supervisor.tick();

    try supervisor.replica_terminate(2);
    try supervisor.replica_reformat(2);

    try supervisor.replica_terminate(1);
    try supervisor.replica_start(2);
    for (0..4000) |_| {
        if (supervisor.workload_done()) break;
        try supervisor.tick();
    } else {
        return error.WorkloadIncomplete;
    }
}
