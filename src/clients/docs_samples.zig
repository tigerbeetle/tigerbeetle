const Sample = @import("./docs_types.zig").Sample;

pub const samples = [_]Sample{
    .{
        .proper_name = "Basic",
        .directory = "basic",
        .short_description = "Create two accounts and transfer an amount between them.",
        .long_description =
        \\## 1. Create accounts
        \\
        \\This project starts by creating two accounts (`1` and `2`).
        \\
        \\## 2. Create a transfer
        \\
        \\Then it transfers `10` of an amount from account `1` to
        \\account `2`.
        \\
        \\## 3. Fetch and validate account balances
        \\
        \\Then it fetches both accounts, checks they both exist, and
        \\checks that **account `1`** has:
        \\ * `debits_posted = 10`
        \\ * and `credits_posted = 0`
        \\
        \\And that **account `2`** has:
        \\ * `debits_posted= 0`
        \\ * and `credits_posted = 10`
        ,
    },
    .{
        .proper_name = "Two-Phase Transfer",
        .directory = "two-phase",
        .short_description =
        \\Create two accounts and start a pending transfer between
        \\them, then post the transfer.
        ,
        .long_description =
        \\## 1. Create accounts
        \\
        \\This project starts by creating two accounts (`1` and `2`).
        \\
        \\## 2. Create pending transfer
        \\
        \\Then it begins a
        \\pending transfer of `500` of an amount from account `1` to
        \\account `2`.
        \\
        \\## 3. Fetch and validate pending account balances
        \\
        \\Then it fetches both accounts and validates that **account `1`** has:
        \\ * `debits_posted = 0`
        \\ * `credits_posted = 0`
        \\ * `debits_pending = 500`
        \\ * and `credits_pending = 0`
        \\
        \\And that **account `2`** has:
        \\ * `debits_posted = 0`
        \\ * `credits_posted = 0`
        \\ * `debits_pending = 0`
        \\ * and `credits_pending = 500`
        \\
        \\(This is because a pending
        \\transfer only affects **pending** credits and debits on accounts,
        \\not **posted** credits and debits.)
        \\
        \\## 4. Post pending transfer
        \\
        \\Then it creates a second transfer that marks the first
        \\transfer as posted.
        \\
        \\## 5. Fetch and validate transfers
        \\
        \\Then it fetches both transfers, validates
        \\that the two transfers exist, validates that the first
        \\transfer had (and still has) a `pending` flag, and validates
        \\that the second transfer had (and still has) a
        \\`post_pending_transfer` flag.
        \\
        \\## 6. Fetch and validate final account balances
        \\
        \\Finally, it fetches both accounts, validates they both exist,
        \\and checks that credits and debits for both account are now
        \\*posted*, not pending.
        \\
        \\Specifically, that **account `1`** has:
        \\ * `debits_posted = 500`
        \\ * `credits_posted = 0`
        \\ * `debits_pending = 0`
        \\ * and `credits_pending = 0`
        \\
        \\And that **account `2`** has:
        \\ * `debits_posted = 0`
        \\ * `credits_posted = 500`
        \\ * `debits_pending = 0`
        \\ * and `credits_pending = 0`
        ,
    },
    .{
        .proper_name = "Many Two-Phase Transfers",
        .directory = "two-phase-many",
        .short_description =
        \\Create two accounts and start a number of pending transfer
        \\between them, posting and voiding alternating transfers.
        ,
        .long_description =
        \\## 1. Create accounts
        \\
        \\This project starts by creating two accounts (`1` and `2`).
        \\
        \\## 2. Create pending transfers
        \\
        \\Then it begins 5 pending transfers of amounts `100` to
        \\`500`, incrementing by `100` for each transfer.
        \\
        \\## 3. Fetch and validate pending account balances
        \\
        \\Then it fetches both accounts and validates that **account `1`** has:
        \\ * `debits_posted = 0`
        \\ * `credits_posted = 0`
        \\ * `debits_pending = 1500`
        \\ * and `credits_pending = 0`
        \\
        \\And that **account `2`** has:
        \\ * `debits_posted = 0`
        \\ * `credits_posted = 0`
        \\ * `debits_pending = 0`
        \\ * and `credits_pending = 1500`
        \\
        \\(This is because a pending transfer only affects **pending**
        \\credits and debits on accounts, not **posted** credits and
        \\debits.)
        \\
        \\## 4. Post and void alternating transfers
        \\
        \\Then it alternatively posts and voids each transfer,
        \\checking account balances after each transfer.
        \\
        \\## 6. Fetch and validate final account balances
        \\
        \\Finally, it fetches both accounts, validates they both exist,
        \\and checks that credits and debits for both account are now
        \\solely *posted*, not pending.
        \\
        \\Specifically, that **account `1`** has:
        \\ * `debits_posted = 1500`
        \\ * `credits_posted = 0`
        \\ * `debits_pending = 0`
        \\ * and `credits_pending = 0`
        \\
        \\And that **account `2`** has:
        \\ * `debits_posted = 0`
        \\ * `credits_posted = 1500`
        \\ * `debits_pending = 0`
        \\ * and `credits_pending = 0`
        ,
    },
};
