const std = @import("std");

const vsr = @import("vsr");
const tb = vsr.tigerbeetle;
const demo = @import("demo.zig");

const Transfer = tb.Transfer;

pub const vsr_options = demo.vsr_options;

pub fn main() !void {
    const transfers = [_]Transfer{
        Transfer{
            .id = 1001,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .amount = 8000,
            .pending_id = 0,
            .user_data_128 = 0,
            .user_data_64 = 0,
            .user_data_32 = 0,
            .timeout = std.time.s_per_hour,
            .ledger = 710,
            .code = 1,
            .flags = .{
                .pending = true, // Set this transfer to be two-phase.
            },
        },
        Transfer{
            .id = 1002,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .amount = 500,
            .pending_id = 0,
            .user_data_128 = 0,
            .user_data_64 = 0,
            .user_data_32 = 0,
            .timeout = std.time.s_per_hour,
            .ledger = 710,
            .code = 1,
            .flags = .{
                .pending = true, // Set this transfer to be two-phase.
                .linked = true, // Link this transfer with the next transfer 1003.
            },
        },
        Transfer{
            .id = 1003,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .amount = 500,
            .pending_id = 0,
            .user_data_128 = 0,
            .user_data_64 = 0,
            .user_data_32 = 0,
            .timeout = std.time.s_per_hour,
            .ledger = 710,
            .code = 1,
            .flags = .{
                .pending = true, // Set this transfer to be two-phase.
                // The last transfer in a linked chain has .linked set to false to close the chain.
                // This transfer will succeed or fail together with transfer 1002 above.
            },
        },
    };

    try demo.request(.create_transfers, transfers, demo.on_create_transfers);
}
