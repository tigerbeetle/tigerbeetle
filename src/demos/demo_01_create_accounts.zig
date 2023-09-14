const vsr = @import("vsr");
const tb = vsr.tigerbeetle;
const demo = @import("demo.zig");

const Account = tb.Account;

pub const vsr_options = demo.vsr_options;

pub fn main() !void {
    const accounts = [_]Account{
        Account{
            .id = 1,
            .debits_pending = 0,
            .debits_posted = 0,
            .credits_pending = 0,
            .credits_posted = 0,
            .user_data_128 = 0,
            .user_data_64 = 0,
            .user_data_32 = 0,
            .reserved = 0,
            .ledger = 710, // Let's use the ISO-4217 Code Number for ZAR
            .code = 1000, // A chart of accounts code to describe this as a clearing account.
            .flags = .{ .debits_must_not_exceed_credits = true },
        },
        Account{
            .id = 2,
            .debits_pending = 0,
            .debits_posted = 0,
            .credits_pending = 0,
            .credits_posted = 0,
            .user_data_128 = 0,
            .user_data_64 = 0,
            .user_data_32 = 0,
            .reserved = 0,
            .ledger = 710, // Let's use the ISO-4217 Code Number for ZAR
            .code = 2000, // A chart of accounts code to describe this as a payable account.
            .flags = .{},
        },
    };

    try demo.request(.create_accounts, accounts, demo.on_create_accounts);
}
