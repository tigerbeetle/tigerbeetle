const Sample = @import("./docs_types.zig").Sample;

pub const samples = [_]Sample{
    .{
        .proper_name = "Basic",
        .directory = "basic",
        .short_description = "Shows how to move amounts between two accounts.",
        .long_description =
        \\## 1. Create accounts
        \\
        \\This project starts by creating the debit account `DR`
        \\and the credit account `CR`.
        \\
        \\## 2. Create a transfer
        \\
        \\Then it creates a transfer that debits the amount of `10` from
        \\account `DR` and credits account `CR`.
        \\
        \\## 3. Fetch and validate account balances
        \\
        \\Then it fetches both accounts, checks that they both exist, and
        \\verifies that **account `DR`** has:
        \\ * `debits_posted = 10`
        \\ * `credits_posted = 0`
        \\
        \\And that **account `CR`** has:
        \\ * `debits_posted = 0`
        \\ * `credits_posted = 10`
        ,
    },
    .{
        .proper_name = "Two-Phase Transfer",
        .directory = "two-phase",
        .short_description =
        \\Shows how to start a two-phase pending transfer and complete it.
        ,
        .long_description =
        \\## 1. Create accounts
        \\
        \\This project starts by creating the debit account `DR`
        \\and the credit account `CR`.
        \\
        \\## 2. Create pending transfer
        \\
        \\Then it starts a two-phase pending transfer,
        \\debiting the amount of `500` from account `DR`
        \\and crediting account `CR`.
        \\
        \\## 3. Fetch and validate pending account balances
        \\
        \\Then it fetches both accounts and validates that **account `DR`** has:
        \\ * `debits_posted = 0`
        \\ * `credits_posted = 0`
        \\ * `debits_pending = 500`
        \\ * `credits_pending = 0`
        \\
        \\And that **account `CR`** has:
        \\ * `debits_posted = 0`
        \\ * `credits_posted = 0`
        \\ * `debits_pending = 0`
        \\ * `credits_pending = 500`
        \\
        \\This is because a pending transfer only affects **pending** credits
        \\and debits on accounts, not **posted** credits and debits.
        \\
        \\## 4. Post pending transfer
        \\
        \\Then it creates a second transfer that completes the two-phase
        \\transfer by posting it.
        \\
        \\## 5. Fetch and validate final account balances
        \\
        \\Finally, it fetches both accounts, validates that both exist,
        \\and checks that credits and debits for both accounts are now
        \\**posted**, not pending.
        \\
        \\Specifically, **account `DR`** has:
        \\ * `debits_posted = 500`
        \\ * `credits_posted = 0`
        \\ * `debits_pending = 0`
        \\ * `credits_pending = 0`
        \\
        \\And **account `CR`** has:
        \\ * `debits_posted = 0`
        \\ * `credits_posted = 500`
        \\ * `debits_pending = 0`
        \\ * `credits_pending = 0`
        ,
    },
};
