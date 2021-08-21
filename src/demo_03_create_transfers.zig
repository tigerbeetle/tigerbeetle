usingnamespace @import("tigerbeetle.zig");
usingnamespace @import("demo.zig");

pub fn main() !void {
    const transfers = [_]Transfer{
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

    try Demo.request(.create_transfers, transfers, Demo.on_create_transfers);
}
