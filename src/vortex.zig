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

const log = std.log.scoped(.vortex);
pub const log_level: std.log.Level = .info;

pub const CLIArgs = union(enum) {
    supervisor: Supervisor.CLIArgs,
    driver: ZigDriver.CLIArgs,
    workload: Workload.CLIArgs,
};

pub fn main() !void {
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
        .workload => |workload_args| try Workload.main(allocator, workload_args),
    }
}
