const tb = @import("tigerbeetle.zig");
const demo = @import("demo.zig");

const Transfer = tb.Transfer;

pub fn main() !void {
    const commits = [_]Transfer{
        Transfer{
            .id = 1004,
            .pending_id = 1001,
            .ledger = 1,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .user_data = 0,
            .reserved = 0,
            .timeout = 0,
            .code = 0,
            .flags = .{ .post_pending_transfer = true }, // Post the pending two-phase transfer.
            .amount = 0, // Inherit the amount from the pending transfer.
        },
        Transfer{
            .id = 1005,
            .pending_id = 1002,
            .ledger = 1,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .user_data = 0,
            .reserved = 0,
            .timeout = 0,
            .code = 0,
            .flags = .{ .post_pending_transfer = true }, // Post the pending two-phase transfer.
            .amount = 0, // Inherit the amount from the pending transfer.
        },
    };

    try demo.request(.create_transfers, commits, demo.on_create_transfers);
}
