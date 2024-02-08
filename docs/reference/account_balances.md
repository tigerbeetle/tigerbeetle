---
sidebar_position: 1
---

# Account Balances

An `AccountBalance` is a record storing the [`Account`](./accounts.md)'s balance
at a given point in time.

Only Accounts with the flag [`history`](./accounts.md#flagshistory) set retain the history of balances.

## Fields

### `timestamp`

This is the time the account balance was updated, as nanoseconds since
UNIX epoch.

The timestamp refers to the same [`Transfer.timestamp`](./transfers.md#timestamp)
which changed the [`Account`](./accounts.md).

Constraints:

* Type is 64-bit unsigned integer (8 bytes)

### `debits_pending`

Amount of [pending debits](./accounts.md#debits_pending). 

Constraints:

* Type is 128-bit unsigned integer (16 bytes)

### `debits_posted`

Amount of [posted debits](./accounts.md#debits_posted).

Constraints:

* Type is 128-bit unsigned integer (16 bytes)

### `credits_pending`

Amount of [pending credits](./accounts.md#credits_pending).

Constraints:

* Type is 128-bit unsigned integer (16 bytes)

### `credits_posted`

Amount of [posted credits](./accounts.md#credits_posted).

Constraints:

* Type is 128-bit unsigned integer (16 bytes)

### `reserved`

This space may be used for additional data in the future.

Constraints:

* Type is 56 bytes
* Must be zero