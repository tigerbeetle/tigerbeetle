const std = @import("std");
const builtin = @import("builtin");
const flags = @import("../../../flags.zig");
const Shell = @import("../../../shell.zig");
const LoggedProcess = @import("./process.zig").LoggedProcess;

const assert = std.debug.assert;

const replica_count = 3;
const replica_ports = [replica_count]u16{ 3000, 3001, 3002 };

const run_time_ns = 30 * std.time.ns_per_s;
const tick_ns = 50 * std.time.ns_per_ms;
const target_tick_count = @divFloor(run_time_ns, tick_ns);

pub const CLIArgs = struct {
    tigerbeetle_executable: []const u8,
};

const Replica = struct {
    name: []const u8,
    process: *LoggedProcess,
};

pub fn main(shell: *Shell, allocator: std.mem.Allocator, args: CLIArgs) !void {
    const tmp_dir = try shell.create_tmp_dir();
    defer shell.cwd.deleteDir(tmp_dir) catch {};

    var replicas: [replica_count]Replica = undefined;
    for (0..replica_count) |i| {
        const name = try shell.fmt("replica {d}", .{i});
        const datafile = try shell.fmt("{s}/1_{d}.tigerbeetle", .{ tmp_dir, i });

        // Format datafile
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

        // Start replica
        const addresses = try shell.fmt("--addresses={s}", .{
            try comma_separate_ports(shell.arena.allocator(), &replica_ports),
        });
        const argv = try shell.arena.allocator().dupe([]const u8, &.{
            args.tigerbeetle_executable,
            "start",
            addresses,
            datafile,
        });

        var process = try LoggedProcess.init(allocator, name, argv, .{});
        replicas[i] = .{ .name = name, .process = process };
        try process.start();
    }

    // Start workload
    const workload = try start_workload(shell, allocator);

    // Let it finish by itself, or kill it after `target_tick_count` ticks.
    const workload_result = term: {
        for (0..target_tick_count) |tick| {
            const duration_ns = tick * tick_ns;
            if (@rem(duration_ns, std.time.ns_per_s) == 0) {
                // std.debug.print("supervisor: waited for {d}\n", .{@divExact(duration_ns, std.time.ns_per_s)});
            }
            if (workload.state() == .completed) {
                std.debug.print("supervisor: workload completed by itself\n", .{});
                break :term try workload.wait();
            }
            std.time.sleep(tick_ns);
        }

        std.debug.print("supervisor: terminating workload due to max duration\n", .{});
        break :term try workload.terminate();
    };

    workload.deinit();

    for (replicas) |replica| {
        _ = try replica.process.terminate();
        replica.process.deinit();
    }

    switch (workload_result) {
        .Exited => |code| {
            if (code == 128 + std.posix.SIG.TERM) {
                std.debug.print("supervisor: workload terminated as requested\n", .{});
            } else if (code == 0) {
                std.debug.print("supervisor: workload exited successfully\n", .{});
            } else {
                std.debug.print("supervisor: workload exited unexpectedly with code {d}\n", .{code});
                std.process.exit(1);
            }
        },
        else => {
            std.debug.print("supervisor: unexpected workload result: {any}\n", .{workload_result});
            unreachable;
        },
    }
}

fn start_workload(shell: *Shell, allocator: std.mem.Allocator) !*LoggedProcess {
    const name = "workload";
    const client_jar = "src/clients/java/target/tigerbeetle-java-0.0.1-SNAPSHOT.jar";
    const workload_jar = "src/testing/systest/workload/target/workload-0.0.1-SNAPSHOT.jar";

    const class_path = try shell.fmt("{s}:{s}", .{ client_jar, workload_jar });
    const argv = try shell.arena.allocator().dupe([]const u8, &.{
        "java",
        "-ea",
        "-cp",
        class_path,
        "Main",
    });

    var env = try std.process.getEnvMap(shell.arena.allocator());
    try env.put("REPLICAS", try comma_separate_ports(shell.arena.allocator(), &replica_ports));

    var process = try LoggedProcess.init(allocator, name, argv, .{ .env = &env });
    try process.start();
    return process;
}

fn comma_separate_ports(allocator: std.mem.Allocator, ports: []const u16) ![]const u8 {
    assert(ports.len > 0);

    var out = std.ArrayList(u8).init(allocator);
    const writer = out.writer();

    try std.fmt.format(writer, "{d}", .{ports[0]});
    for (ports[1..]) |port| {
        try writer.writeByte(',');
        try std.fmt.format(writer, "{d}", .{port});
    }

    return out.toOwnedSlice();
}

test "comma-separates ports" {
    const formatted = try comma_separate_ports(std.testing.allocator, &.{ 3000, 3001, 3002 });
    defer std.testing.allocator.free(formatted);

    try std.testing.expectEqualStrings("3000,3001,3002", formatted);
}
