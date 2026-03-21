/// Run a cluster of TigerBeetle replicas, a client driver, a workload, all with fault injection,
/// to test the whole system.
///
/// On Linux, Vortex runs in a Linux namespace where it can control the network.
const std = @import("std");
const stdx = @import("stdx");
const builtin = @import("builtin");

const Supervisor = @import("testing/vortex/supervisor.zig");

const assert = std.debug.assert;
const log = std.log.scoped(.vortex);

pub const std_options: std.Options = .{
    .log_level = .info,
    .logFn = stdx.log_with_timestamp,
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

    try Supervisor.main(allocator, stdx.flags(&args, Supervisor.CLIArgs));
}
