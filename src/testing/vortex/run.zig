const std = @import("std");
const builtin = @import("builtin");

const log = std.log.scoped(.run);

pub fn main(allocator: std.mem.Allocator) !void {
    if (builtin.os.tag != .linux) {
        log.err("The `run` command to set up namespaces is only supported on Linux.", .{});
        return error.NotSupported;
    }

    var args_passthrough = try std.process.argsWithAllocator(allocator);
    defer args_passthrough.deinit();

    var vortex_path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const vortex_path = try std.fs.selfExePath(&vortex_path_buffer);

    var argv: std.ArrayListUnmanaged([]const u8) = .{};
    defer argv.deinit(allocator);

    // Run vortex through linux 'unshare' to put it in a new namespace
    // where we can control the network config.
    const args_unshare = &.{
        "unshare",         "--net", "--fork",
        "--map-root-user", "--pid",
    };

    inline for (args_unshare) |arg| {
        try argv.append(allocator, arg);
    }

    // Skip first two arguments ('vortex run').
    _ = args_passthrough.next();
    _ = args_passthrough.next();

    // Add the `vortex supervisor` command and passthrough args.
    try argv.append(allocator, vortex_path);
    try argv.append(allocator, "supervisor");

    while (args_passthrough.next()) |arg| {
        try argv.append(allocator, arg);
    }

    // Tell the supervisor to set up loopback networking.
    try argv.append(allocator, "--configure-namespace");

    // NB: We do an exec here on the theory that it simplifies handling
    // of the TERM signal, though in practice vortex just doesn't seem
    // to handle signals well.
    return std.process.execve(allocator, argv.items, null);
}
