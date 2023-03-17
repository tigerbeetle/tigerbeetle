# Close Account

In accounting, a _closing entry_ calculates the net debit or credit balance for an account and then
credits or debits this balance respectively, to zero the account's balance and move the balance to
another account.

### Example

Given a set of accounts:

| Account | Debits Pending | Debits Posted | Credits Pending | Credits Posted | Flags                            |
| ------: | -------------: | ------------: | --------------: | -------------: | -------------------------------- |
|     `A` |              0 |            10 |               0 |             20 | `debits_must_not_exceed_credits` |
|     `B` |              0 |            30 |               0 |              5 | `credits_must_not_exceed_debits` |
|     `C` |              0 |             0 |               0 |              0 |                                  |

The "closing entries" for accounts `A` and `B` are:

| Debit Account   | Credit Account | Amount  | Amount (recorded) | Flags              | Notes               |
| --------------: | -------------: | ------: | ----------------: | ------------------ | ------------------- |
|             `A` |            `C` | `0`     |                10 | `balancing_debit`  | (close account `A`) |
|             `C` |            `B` | `0`     |                25 | `balancing_credit` | (close account `B`) |

(Pass `0` as the `Transfer.amount` so that the application does not need to know (or query) the balance prior to closing the account.
The stored transfer's `amount` will be set to the actual (non-zero) amount transferred.)

After committing these transfers, `A` and `B`'s balances are zero:

| Account | Debits Pending | Debits Posted | Credits Pending | Credits Posted | Flags                            |
| ------: | -------------: | ------------: | --------------: | -------------: | -------------------------------- |
|     `A` |              0 |            20 |               0 |             20 | `debits_must_not_exceed_credits` |
|     `B` |              0 |            30 |               0 |             30 | `credits_must_not_exceed_debits` |
|     `C` |              0 |            25 |               0 |             10 |                                  |
