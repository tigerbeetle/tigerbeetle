usingnamespace @import("tigerbeetle.zig");
usingnamespace @import("demo.zig");

pub fn main() !void {
    const ids = [_]u128{ 1, 2 };

    try Demo.request(.lookup_accounts, ids, Demo.on_lookup_accounts);
}
