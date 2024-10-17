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
