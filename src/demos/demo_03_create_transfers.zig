const vsr = @import("vsr");
const tb = vsr.tigerbeetle;
const demo = @import("demo.zig");

const Transfer = tb.Transfer;

pub const vsr_options = demo.vsr_options;

pub fn main() !void {
    const transfers = [_]Transfer{
        Transfer{
            .id = 1,
            .debit_account_id = 2,
            .credit_account_id = 1,
            .amount = 10000, // Let's start with some liquidity in account 1.
            .pending_id = 0,
            .user_data_128 = 0,
            .user_data_64 = 0,
            .user_data_32 = 0,
            .timeout = 0,
            .ledger = 710, // Let's use the ISO-4217 Code Number for ZAR
            .code = 1,
            .flags = .{},
        },
        Transfer{
            .id = 2,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .amount = 1000,
            .pending_id = 0,
            .user_data_128 = 0,
            .user_data_64 = 0,
            .user_data_32 = 0,
            .timeout = 0,
            .ledger = 710, // Let's use the ISO-4217 Code Number for ZAR
            .code = 1,
            .flags = .{},
        },
    };

    try demo.request(.create_transfers, transfers, demo.on_create_transfers);
}
