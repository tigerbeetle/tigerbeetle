/// This is the main entrypoint for the Vortex testing tools, delegating to these programs:
///
/// * _supervisor_: runs a cluster of multiple TigerBeetle replicas, drivers, and a workload, along
/// with various fault injection, to test the system as a whole.
/// * _driver_: a separate process communicating over stdio, using `tb_client` to send commands
/// and queries to the cluster. Drivers in other languages should be implemented elsewhere.
/// * _workload_: a separate process that, given a driver, runs commands and queries against the
/// cluster, verifying its correctness.
const std = @import("std");
const builtin = @import("builtin");
const flags = @import("flags.zig");

const Supervisor = @import("testing/vortex/supervisor.zig");
const ZigDriver = @import("testing/vortex/zig_driver.zig");
const Workload = @import("testing/vortex/workload.zig");

const assert = std.debug.assert;

const log = std.log.scoped(.vortex);
pub const log_level: std.log.Level = .info;

pub const CLIArgs = union(enum) {
    supervisor: Supervisor.CLIArgs,
    driver: ZigDriver.CLIArgs,
    workload: DriverArgs,
};

const DriverArgs = struct {
    @"cluster-id": u128,
    addresses: []const u8,
    @"driver-command": []const u8,
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

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    switch (flags.parse(&args, CLIArgs)) {
        .supervisor => |supervisor_args| try Supervisor.main(allocator, supervisor_args),
        .driver => |driver_args| try ZigDriver.main(allocator, driver_args),
        .workload => |driver_args| {
            var driver = try start_driver(allocator, driver_args);
            defer {
                _ = driver.kill() catch {};
            }

            try Workload.main(allocator, &.{
                .input = driver.stdin.?,
                .output = driver.stdout.?,
            });
        },
    }
}

fn start_driver(allocator: std.mem.Allocator, args: DriverArgs) !std.process.Child {
    var argv = std.ArrayList([]const u8).init(allocator);
    defer argv.deinit();

    assert(std.mem.indexOf(u8, args.@"driver-command", "\"") == null);
    var cmd_parts = std.mem.split(u8, args.@"driver-command", " ");

    while (cmd_parts.next()) |part| {
        try argv.append(part);
    }

    var cluster_id_argument: [32]u8 = undefined;
    const cluster_id = try std.fmt.bufPrint(cluster_id_argument[0..], "{d}", .{args.@"cluster-id"});

    try argv.append(cluster_id);
    try argv.append(args.addresses);

    var child = std.process.Child.init(argv.items, allocator);
    child.stdin_behavior = .Pipe;
    child.stdout_behavior = .Pipe;
    child.stderr_behavior = .Inherit;

    try child.spawn();

    return child;
}
