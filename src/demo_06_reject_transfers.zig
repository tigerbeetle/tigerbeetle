const std = @import("std");

usingnamespace @import("tigerbeetle.zig");
usingnamespace @import("demo.zig");

pub fn main() !void {
    const fd = try connect(config.port);
    defer std.os.close(fd);

    var commits = [_]Commit{
        Commit{
            .id = 1001,
            .reserved = [_]u8{0} ** 32,
            .code = 0,
            .flags = .{ .reject = true },
        },
    };

    try send(fd, .commit_transfers, commits, CommitTransfersResult);
}
