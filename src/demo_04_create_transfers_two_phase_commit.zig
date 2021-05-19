const std = @import("std");

usingnamespace @import("tigerbeetle.zig");
usingnamespace @import("demo.zig");

pub fn main() !void {
    const fd = try connect(config.port);
    defer std.os.close(fd);

    var transfers = [_]Transfer{
        Transfer{
            .id = 1001,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .user_data = 0,
            .reserved = [_]u8{0} ** 32,
            .timeout = std.time.ns_per_hour,
            .code = 0,
            .flags = .{
                .two_phase_commit = true,
            },
            .amount = 9000,
        },
        Transfer{
            .id = 1002,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .user_data = 0,
            .reserved = [_]u8{0} ** 32,
            .timeout = std.time.ns_per_hour,
            .code = 0,
            .flags = .{
                .two_phase_commit = true,
            },
            .amount = 1,
        },
    };

    try send(fd, .create_transfers, transfers, CreateTransfersResult);
}
