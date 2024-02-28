# Complex (Multi-Debit, Multi-Credit) Journal Entries

TigerBeetle is designed for maximum performance. In order to keep it lean, the database only supports simple transfers with a single debit and a single credit.

However, you'll probably run into cases where you want transactions with multiple debits and/or credits. If so, this page is for you!

## 1-to-N

Transactions that involve multiple debits and a single credit OR a single debit and multiple credits are relatively straightforward.

You can use multiple linked transfers as depicted below.

### Single Debit, Multiple Credits

This example debits a single account and credits multiple accounts. It uses the following accounts:

- A _source account_ `A`, on the `USD` ledger.
- Three _destination accounts_ `X`, `Y`, and `Z`, on the `USD` ledger.

| Ledger | Debit Account | Credit Account | Amount | `flags.linked` |
| -----: | ------------: | -------------: | -----: | -------------: |
|    USD |           `A` |            `X` |  10000 |           true |
|    USD |           `A` |            `Y` |     50 |           true |
|    USD |           `A` |            `Z` |     10 |          false |

### Multiple Debits, Single Credit

This example debits multiple accounts and credits a single account. It uses the following accounts:

- Three _source accounts_ `A`, `B`, and `C` on the `USD` ledger.
- A _destination account_ `X` on the `USD` ledger.

| Ledger | Debit Account | Credit Account | Amount | `flags.linked` |
| -----: | ------------: | -------------: | -----: | -------------: |
|    USD |           `A` |            `X` |  10000 |           true |
|    USD |           `B` |            `X` |     50 |           true |
|    USD |           `C` |            `X` |     10 |          false |

## Many-to-Many Transactions

Transactions with multiple debits and multiple credits are a bit more tricky.

To simplify the math, we can use a temporary "control account" as an intermediary.

In this example, we'll use the following accounts:

- Three _source accounts_ `A` and `B` on the `USD` ledger.
- Three _destination accounts_ `X`, `Y`, and `Z`, on the `USD` ledger.
- A _compound entry control account_ `Control` on the `USD` ledger.

| Ledger | Debit Account | Credit Account | Amount | `flags.linked` |
| -----: | ------------: | -------------: | -----: | -------------: |
|    USD |           `A` |      `Control` |  10000 |           true |
|    USD |           `B` |      `Control` |     50 |           true |
|    USD |     `Control` |            `X` |   9000 |           true |
|    USD |     `Control` |            `Y` |   1000 |           true |
|    USD |     `Control` |            `Z` |     50 |          false |

Here, we use two transfers to debit accounts `A` and `B` and credit the `Control` account, and another three transfers to credit accounts `X`, `Y`, and `Z`.

If you look closely at this example, you may notice that we could technically have simply debited `B` and credited `Z` directly because the amounts happen to line up. That is true! However, since you probably don't want to program a lot of heuristics to figure out which debits and credits exactly match up, you'll probably find it easier to set up these complex journal entries using this type of control account.
