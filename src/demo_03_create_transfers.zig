const std = @import("std");

usingnamespace @import("tigerbeetle.zig");
usingnamespace @import("demo.zig");

pub fn main() !void {
    const fd = try connect(config.port);
    defer std.os.close(fd);

    var transfers = [_]Transfer{
        Transfer{
            .id = 1000,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .user_data = 0,
            .reserved = [_]u8{0} ** 32,
            .timeout = 0,
            .code = 0,
            .flags = .{},
            .amount = 1000,
        },
    };

    try send(fd, .create_transfers, transfers, CreateTransfersResult);
}
