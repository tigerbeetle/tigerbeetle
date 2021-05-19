const std = @import("std");

usingnamespace @import("tigerbeetle.zig");
usingnamespace @import("demo.zig");

pub fn main() !void {
    const fd = try connect(config.port);
    defer std.os.close(fd);

    var commits = [_]Commit{
        Commit{
            .id = 1001,
            .custom_1 = 0,
            .custom_2 = 0,
            .custom_3 = 0,
            .flags = .{ .reject = true },
        },
    };

    try send(fd, .commit_transfers, commits, CommitTransfersResult);
}
