usingnamespace @import("tigerbeetle.zig");
usingnamespace @import("demo.zig");

pub fn main() !void {
    const commits = [_]Commit{
        Commit{
            .id = 1001,
            .reserved = [_]u8{0} ** 32,
            .code = 0,
            .flags = .{ .reject = true },
        },
    };

    try Demo.request(.commit_transfers, commits, Demo.on_commit_transfers);
}
