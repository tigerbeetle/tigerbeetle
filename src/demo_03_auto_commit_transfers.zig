const std = @import("std");

usingnamespace @import("tigerbeetle.zig");
usingnamespace @import("demo.zig");

pub fn main() !void {
    const fd = try connect(config.port);
    defer std.os.close(fd);

    var auto_commit_transfers = [_]Transfer{
        Transfer{
            .id = 1000,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .custom_1 = 0,
            .custom_2 = 0,
            .custom_3 = 0,
            .flags = .{
                .accept = true,
                .auto_commit = true,
            },
            .amount = 1000,
            .timeout = 0,
        },
    };

    try send(fd, .create_transfers, auto_commit_transfers, CreateTransferResults);
}
