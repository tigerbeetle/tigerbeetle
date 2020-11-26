const std = @import("std");

usingnamespace @import("tigerbeetle.zig");
usingnamespace @import("demo.zig");

pub fn main() !void {
    const fd = try connect(config.port);
    defer std.os.close(fd);

    var ids = [_]u128{ 1, 2 };

    try send(fd, .lookup_accounts, ids, Account);
}
