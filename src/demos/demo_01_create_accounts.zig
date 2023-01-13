const vsr = @import("vsr");
const tb = vsr.tigerbeetle;
const demo = @import("demo.zig");

const Account = tb.Account;

pub const vsr_options = demo.vsr_options;

pub fn main() !void {
    const accounts = [_]Account{
        Account{
            .id = 1,
            .user_data = 0,
            .reserved = [_]u8{0} ** 48,
            .ledger = 710, // Let's use the ISO-4217 Code Number for ZAR
            .code = 1000, // A chart of accounts code to describe this as a clearing account.
            .flags = .{ .debits_must_not_exceed_credits = true },
            .debits_pending = 0,
            .debits_posted = 0,
            .credits_pending = 0,
            .credits_posted = 0,
        },
        Account{
            .id = 2,
            .user_data = 0,
            .reserved = [_]u8{0} ** 48,
            .ledger = 710, // Let's use the ISO-4217 Code Number for ZAR
            .code = 2000, // A chart of accounts code to describe this as a payable account.
            .flags = .{},
            .debits_pending = 0,
            .debits_posted = 0,
            .credits_pending = 0,
            .credits_posted = 0,
        },
    };

    try demo.request(.create_accounts, accounts, demo.on_create_accounts);
}
