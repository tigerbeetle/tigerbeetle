const tb = @import("tigerbeetle.zig");
const demo = @import("demo.zig");

const Transfer = tb.Transfer;

pub fn main() !void {
    const transfer = [_]Transfer{
        Transfer{
            .id = 1,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .user_data = 0,
            .reserved = 0,
            .pending_id = 0,
            .timeout = 0,
            .ledger = 710, // Let's use the ISO-4217 Code Number for ZAR
            .code = 1,
            .flags = .{},
            .amount = 1000,
        },
    };

    try demo.request(.create_transfers, transfer, demo.on_create_transfers);
}
