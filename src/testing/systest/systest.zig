const std = @import("std");
const builtin = @import("builtin");

const Shell = @import("../../shell.zig");
const Supervisor = @import("supervisor.zig");
const ZigDriver = @import("zig_driver.zig");

const log = std.log.scoped(.systest);

pub const CLIArgs = union(enum) {
    supervisor: Supervisor.CLIArgs,
    driver: ZigDriver.CLIArgs,
};

pub fn main(shell: *Shell, allocator: std.mem.Allocator, args: CLIArgs) !void {
    if (builtin.os.tag == .windows) {
        log.err("systest is not supported for Windows", .{});
        return error.NotSupported;
    }

    switch (args) {
        .supervisor => |supervisor_args| try Supervisor.main(shell, allocator, supervisor_args),
        .driver => |driver_args| try ZigDriver.main(allocator, driver_args),
    }
}
