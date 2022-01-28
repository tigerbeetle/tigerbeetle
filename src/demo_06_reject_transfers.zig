const tb = @import("tigerbeetle.zig");
const demo = @import("demo.zig");

const Commit = tb.Commit;

pub fn main() !void {
    const commits = [_]Commit{
        Commit{
            .id = 1001,
            .reserved = [_]u8{0} ** 32,
            .code = 0,
            .flags = .{ .reject = true },
        },
    };

    try demo.request(.commit_transfers, commits, demo.on_commit_transfers);
}
