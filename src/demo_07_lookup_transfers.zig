usingnamespace @import("tigerbeetle.zig");
usingnamespace @import("demo.zig");

pub fn main() !void {
    const ids = [_]u128{ 1000, 1001, 1002 };

    try Demo.request(.lookup_transfers, ids, Demo.on_lookup_transfers);
}
