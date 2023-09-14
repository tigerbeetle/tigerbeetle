const vsr = @import("vsr");
const tb = vsr.tigerbeetle;
const demo = @import("demo.zig");

const Transfer = tb.Transfer;

pub const vsr_options = demo.vsr_options;

pub fn main() !void {
    const commits = [_]Transfer{
        Transfer{
            .id = 2003,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .amount = 0,
            .pending_id = 1003,
            .user_data_128 = 0,
            .user_data_64 = 0,
            .user_data_32 = 0,
            .timeout = 0,
            .ledger = 0,
            .code = 0,
            .flags = .{ .void_pending_transfer = true },
        },
    };

    try demo.request(.create_transfers, commits, demo.on_create_transfers);
}
