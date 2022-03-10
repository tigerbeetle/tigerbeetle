const tb = @import("tigerbeetle.zig");
const demo = @import("demo.zig");

const Transfer = tb.Transfer;

pub fn main() !void {
    const commits = [_]Transfer{
        Transfer{
            .id = 1001,
            .reserved = [_]u8{0} ** 32,
            .code = 0,
            .flags = .{ .void_pending_transfer = true },
        },
    };

    try demo.request(.create_transfers, commits, demo.on_create_transfers);
}
