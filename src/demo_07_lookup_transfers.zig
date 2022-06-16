const demo = @import("demo.zig");

pub fn main() !void {
    const ids = [_]u128{ 1000, 1001, 1002, 1003, 1004, 1005, 1006 };

    try demo.request(.lookup_transfers, ids, demo.on_lookup_transfers);
}
