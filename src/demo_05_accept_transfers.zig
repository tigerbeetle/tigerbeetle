const std = @import("std");

usingnamespace @import("tigerbeetle.zig");
usingnamespace @import("demo.zig");

pub fn main() !void {
    const fd = try connect(config.default_port);
    defer std.os.close(fd);

    var commits = [_]Commit{
        Commit{
            .id = 1001,
            .custom_1 = 0,
            .custom_2 = 0,
            .custom_3 = 0,
            .flags = .{ .accept = true },
        },
        Commit{
            .id = 1002,
            .custom_1 = 0,
            .custom_2 = 0,
            .custom_3 = 0,
            .flags = .{ .accept = true },
        },
    };

    try send(fd, .commit_transfers, commits, CommitTransferResults);
}
