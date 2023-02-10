# Close Account

In accounting, a _closing entry_ zeroes a (temporary) account's balance.

### Example

Given a set of accounts:

| Account | Debits Pending | Debits Posted | Credits Pending | Credits Posted | Flags                            |
| ------: | -------------: | ------------: | --------------: | -------------: | -------------------------------- |
|     `A` |              0 |            10 |               0 |             20 | `debits_must_not_exceed_credits` |
|     `B` |              0 |            20 |               0 |             10 | `credits_must_not_exceed_debits` |
|     `C` |              0 |             0 |               0 |              0 |                                  |

The "closing entries" for accounts `A` and `B` are:

| Debit Account   | Credit Account | Amount     | Amount (recorded) | Flags             | Notes               |
| --------------: | -------------: | ---------: | ----------------: | ----------------- | ------------------- |
|             `A` |            `C` | `2^64 - 1` |                10 | `debits_at_most`  | (close account `A`) |
|             `C` |            `B` | `2^64 - 1` |                10 | `credits_at_most` | (close account `B`) |

(Pass `maxInt(u64)` as the `Transfer.amount` so that the application does not need to know (or query) the balance prior to closing the account).

After committing these transfers, `A` and `B`'s balances are zero:

| Account | Debits Pending | Debits Posted | Credits Pending | Credits Posted | Flags                            |
| ------: | -------------: | ------------: | --------------: | -------------: | -------------------------------- |
|     `A` |              0 |            20 |               0 |             20 | `debits_must_not_exceed_credits` |
|     `B` |              0 |            20 |               0 |             20 | `credits_must_not_exceed_debits` |
|     `C` |              0 |            10 |               0 |             10 |                                  |
