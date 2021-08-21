const std = @import("std");

usingnamespace @import("tigerbeetle.zig");
usingnamespace @import("demo.zig");

pub fn main() !void {
    const transfers = [_]Transfer{
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
                .linked = true, // Link this transfer with the next transfer 1003.
            },
            .amount = 1,
        },
        Transfer{
            .id = 1003,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .user_data = 0,
            .reserved = [_]u8{0} ** 32,
            .timeout = std.time.ns_per_hour,
            .code = 0,
            .flags = .{
                .two_phase_commit = true,
                // The last transfer in a linked chain has .linked set to false to close the chain.
                // This transfer will succeed or fail together with transfer 1002 above.
            },
            .amount = 1,
        },
    };

    try Demo.request(.create_transfers, transfers, Demo.on_create_transfers);
}
