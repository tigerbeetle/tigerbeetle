---
sidebar_position: 5
---

# Reversals

[`Transfer`s](../../reference/transfer.md) in TigerBeetle are immutable, so once they are created
they cannot be modified or deleted.

Immutability is useful for creating an auditable log of all of the business events, but it does
raise the question of what to do when you need to reverse a transfer.

What if a customer needs a refund? Or a transfer was executed in error?

## Always Add More Transfers

Reversals in TigerBeetle are handled with more transfers to reverse the effects of the previous
transfer(s).

Two specific recommendations are:

1. You may want to have a [`Transfer.code`](../../reference/transfer.md#code) that indicates a given
   transfer is a reversal, or you may want multiple codes where each one represents a different
   reason why a reversal has taken place.
2. If you use the [`Transfer.user_data_128`](../../reference/transfer.md#user_data_128) to store an
   ID that links multiple transfers within TigerBeetle or points to a
   [record in an external database](../system-architecture.md), you may want to use the same
   `user_data_128` field on the reversal transfer(s), even if they happen at a later point.

### Example

Let's say you had a couple of transfers, from account `A` to accounts `Y` and `Z`:

| Ledger | Debit Account | Credit Account | Amount | `code` | `user_data_128` | `flags.linked` |
| -----: | ------------: | -------------: | -----: | -----: | --------------: | -------------: |
|    USD |           `A` |            `X` |  10000 |    600 |          123456 |           true |
|    USD |           `A` |            `Y` |     50 |   9000 |          123456 |          false |

Now, we need to reverse the effects of these transfers. We would submit the following two
**additional** transfers:

| Ledger | Debit Account | Credit Account | Amount | `code` | `user_data_128` | `flags.linked` |
| -----: | ------------: | -------------: | -----: | -----: | --------------: | -------------: |
|    USD |           `X` |            `A` |  10000 |  10000 |          123456 |           true |
|    USD |           `Y` |            `A` |     50 |  10001 |          123456 |          false |

Note that the codes used here don't have any actual meaning, but you would want to
[enumerate your business events](../data-modeling.md#code) and map each to a numeric code value,
including the initial reasons for transfers and the reasons they might be reversed.
