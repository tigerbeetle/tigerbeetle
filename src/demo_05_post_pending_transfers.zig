const tb = @import("tigerbeetle.zig");
const demo = @import("demo.zig");

const Transfer = tb.Transfer;

pub fn main() !void {
    const commits = [_]Transfer{
        Transfer{
            .id = 2001,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .user_data = 0,
            .reserved = 0,
            .pending_id = 1001,
            .timeout = 0,
            .ledger = 0, // Honor original Transfer ledger.
            .code = 0, // Honor original Transfer code.
            .flags = .{ .post_pending_transfer = true }, // Post the pending two-phase transfer.
            .amount = 0, // Inherit the amount from the pending transfer.
        },
        Transfer{
            .id = 2002,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .user_data = 0,
            .reserved = 0,
            .pending_id = 1002,
            .timeout = 0,
            .ledger = 0,
            .code = 0,
            .flags = .{ .post_pending_transfer = true }, // Post the pending two-phase transfer.
            .amount = 0, // Inherit the amount from the pending transfer.
        },
    };

    try demo.request(.create_transfers, commits, demo.on_create_transfers);
}
