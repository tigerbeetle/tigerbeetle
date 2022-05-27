const tb = @import("tigerbeetle.zig");
const demo = @import("demo.zig");

const Transfer = tb.Transfer;

pub fn main() !void {
    const transfers = [_]Transfer{
        Transfer{
            .id = 1000,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .user_data = 0,
            .reserved = 0,
            .pending_id = 0,
            .ledger = 0,
            .timeout = 0,
            .code = 0,
            .flags = .{},
            .amount = 1000,
        },
    };

    try demo.request(.create_transfers, transfers, demo.on_create_transfers);
}
