/// Run a cluster of TigerBeetle replicas, a client driver, a workload, all with fault injection,
/// to test the whole system.
///
/// On Linux, Vortex runs in a Linux namespace where it can control the network.
const std = @import("std");
const stdx = @import("stdx");
const builtin = @import("builtin");

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

pub fn main() !void {
    comptime assert(builtin.target.cpu.arch.endian() == .little);

    if (builtin.os.tag == .windows) {
        log.err("vortex is not supported for Windows", .{});
        return error.NotSupported;
    }

    var gpa_allocator = std.heap.GeneralPurposeAllocator(.{}){};
    defer switch (gpa_allocator.deinit()) {
        .ok => {},
        .leak => @panic("memory leak"),
    };

    const allocator = gpa_allocator.allocator();

    var args_iterator = try std.process.argsWithAllocator(allocator);
    defer args_iterator.deinit();

    const args = stdx.flags(&args_iterator, CLIArgs);

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
    log.info("releases={any}", .{supervisor.releases});

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
    for (std.enums.values(Command)) |command| {
        log.info("workload: completed command={s} count={}", .{
            @tagName(command),
            supervisor.workload.?.requests_finished_count.getAssertContains(command),
        });
    }
    supervisor.workload_terminate();
    log.info("done", .{});
}
