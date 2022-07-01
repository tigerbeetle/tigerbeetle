const demo = @import("demo.zig");

pub fn main() !void {
    const ids = [_]u128{ 1, 1001, 1002, 1003, 2001, 2002, 2003 };

    try demo.request(.lookup_transfers, ids, demo.on_lookup_transfers);
}
