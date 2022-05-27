const tb = @import("tigerbeetle.zig");
const demo = @import("demo.zig");

const Transfer = tb.Transfer;

pub fn main() !void {
    const commits = [_]Transfer{
        Transfer{
            .id = 1006,
            .pending_id = 1003,
            .ledger = 1,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .user_data = 0,
            .reserved = 0,
            .timeout = 0,
            .code = 0,
            .flags = .{ .void_pending_transfer = true },
            .amount = 0,
        },
    };

    try demo.request(.create_transfers, commits, demo.on_create_transfers);
}
