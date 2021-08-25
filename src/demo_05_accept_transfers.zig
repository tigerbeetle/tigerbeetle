usingnamespace @import("tigerbeetle.zig");
usingnamespace @import("demo.zig");

pub fn main() !void {
    const commits = [_]Commit{
        Commit{
            .id = 1001,
            .reserved = [_]u8{0} ** 32,
            .code = 0,
            .flags = .{},
        },
        Commit{
            .id = 1002,
            .reserved = [_]u8{0} ** 32,
            .code = 0,
            .flags = .{},
        },
    };

    try Demo.request(.commit_transfers, commits, Demo.on_commit_transfers);
}
