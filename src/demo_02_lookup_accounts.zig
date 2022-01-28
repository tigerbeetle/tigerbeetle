const demo = @import("demo.zig");

pub fn main() !void {
    const ids = [_]u128{ 1, 2 };

    try demo.request(.lookup_accounts, ids, demo.on_lookup_accounts);
}
