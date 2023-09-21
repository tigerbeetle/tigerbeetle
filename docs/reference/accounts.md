---
sidebar_position: 1
---

# Accounts

An `Account` is a record storing the cumulative effect of committed
[transfers](./transfers.md).

TigerBeetle uses the same data structures internally and
externally. This means that sometimes you need to set temporary values
for fields that TigerBeetle, not you (the user), are responsible.

### Updates

Account fields *cannot be changed by the user* after
creation. However, debits and credits fields are updated by
TigerBeetle as transfers move money to and from an account.

## Fields

### `id`

This is a unique, client-defined identifier for the account.

Constraints:

* Type is 128-bit unsigned integer (16 bytes)
* Must not be zero or `2^128 - 1` (the highest 128-bit unsigned integer)
* Must not conflict with another account

### `debits_pending`

`debits_pending` counts debits reserved by pending transfers. When a pending transfer posts, voids,
or times out, the amount is removed from `debits_pending`.

Money in `debits_pending` is reserved — that is, it cannot be spent until the corresponding pending
transfer resolves.

Constraints:

* Type is 128-bit unsigned integer (16 bytes)
* Must be zero when the account is created

### `debits_posted`

Amount of posted debits.

Constraints:

* Type is 128-bit unsigned integer (16 bytes)
* Must be zero when the account is created

### `credits_pending`

Amount of pending credits.

Constraints:

* Type is 128-bit unsigned integer (16 bytes)
* Must be zero when the account is created

### `credits_posted`

Amount of posted credits.

Constraints:

* Type is 128-bit unsigned integer (16 bytes)
* Must be zero when the account is created

### `user_data_128`

This is an optional 128-bit secondary identifier to link this account to an
external entity or event.

As an example, you might use a [ULID](../design/data-modeling.md#time-based-identifiers)
that ties together a group of accounts.

For more information, see [Data Modeling](../design/data-modeling.md#user_data).

Constraints:

* Type is 128-bit unsigned integer (16 bytes)

### `user_data_64`

This is an optional 64-bit secondary identifier to link this account to an
external entity or event.

As an example, you might use this field store an external timestamp.

For more information, see [Data Modeling](../design/data-modeling.md#user_data).

Constraints:

* Type is 64-bit unsigned integer (8 bytes)

### `user_data_32`

This is an optional 32-bit secondary identifier to link this account to an
external entity or event.

As an example, you might use this field to store a timezone or locale.

For more information, see [Data Modeling](../design/data-modeling.md#user_data).

Constraints:

* Type is 32-bit unsigned integer (4 bytes)

### `reserved`

This space may be used for additional data in the future.

Constraints:

* Type is 4 bytes
* Must be zero

### `ledger`

This is an identifier that partitions the sets of accounts that can
transact with each other. Put another way, money cannot transfer
between two accounts with different `ledger` values. See:
[`accounts_must_have_the_same_ledger`](./operations/create_transfers.md#accounts_must_have_the_same_ledger).

[Currency exchange](../recipes/currency-exchange.md) is implemented with two or more linked
transfers.

In a typical use case:
* Map each asset or currency tracked within the database to a distinct ledger. And,
* Tag each account with the `ledger` indicating the currency in which the balance is denominated.

Constraints:
* Type is 32-bit unsigned integer (4 bytes)
* Must not be zero

### `code`

This is a user-defined enum denoting the category of the account.

As an example, you might use codes `1000`-`3340` to indicate asset
accounts in general, where `1001` is Bank Account and `1002` is Money
Market Account and `2003` is Motor Vehicles and so on.

Constraints:

* Type is 16-bit unsigned integer (2 bytes)
* Must not be zero

### `flags`

A bitfield that toggles additional behavior.

Constraints:

* Type is 16-bit unsigned integer (2 bytes)
* Some flags are mutually exclusive; see
  [`flags_are_mutually_exclusive`](./operations/create_accounts.md#flags_are_mutually_exclusive).

#### `flags.linked`

When the `linked` flag is specified, it links an account with the next
account in the batch, to create a chain of accounts, of arbitrary
length, which all succeed or fail in creation together. The tail of a
chain is denoted by the first account without this flag. The last
account in a batch may therefore never have `flags.linked` set as
this would leave a chain open-ended (see `linked_event_chain_open`).

Multiple chains or individual accounts may coexist within a batch to
succeed or fail independently. Accounts within a chain are executed
in order, or are rolled back on error, so that the effect of each
account in the chain is visible to the next, and so that the chain is
either visible or invisible as a unit to subsequent accounts after the
chain. The account that was the first to break the chain will have a
unique error result. Other accounts in the chain will have their error
result set to
[`linked_event_failed`](./operations/create_accounts.md#linked_event_failed).

After the link has executed, the association of each event is lost.
To save the association, it must be
[encoded into the data model](../design/data-modeling.md).

#### `flags.debits_must_not_exceed_credits`

When set, transfers will be rejected that would cause this account's
debits to exceed credits. Specifically when `account.debits_pending +
account.debits_posted + transfer.amount > account.credits_posted`.

This cannot be set when `credits_must_not_exceed_debits` is also set.

#### `flags.credits_must_not_exceed_debits`

When set, transfers will be rejected that would cause this account's
credits to exceed debits. Specifically when `account.credits_pending +
account.credits_posted + transfer.amount > account.debits_posted`.

This cannot be set when `debits_must_not_exceed_credits` is also set.

### `timestamp`

This is the time the account was created, as nanoseconds since
UNIX epoch.

It is set by TigerBeetle to the moment the account arrives at
the cluster.

Additionally, all timestamps are unique, immutable and [totally
ordered](http://book.mixu.net/distsys/time.html). So an account that
is created before another account is guaranteed to have an earlier
timestamp. In other systems this is also called a "physical"
timestamp, "ingestion" timestamp, "record" timestamp, or "system"
timestamp.

Constraints:

* Type is 64-bit unsigned integer (8 bytes)
* User sets to zero on creation

## Internals

If you're curious and want to learn more, you can find the source code
for this struct in
[src/tigerbeetle.zig](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/tigerbeetle.zig). Search
for `const Account = extern struct {`.

You can find the source code for creating an account in
[src/state_machine.zig](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/state_machine.zig). Search
for `fn create_account(`.
