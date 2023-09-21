---
sidebar_position: 2
---

# Transfers

A `transfer` is an immutable record of a financial transaction between
two accounts.

In TigerBeetle, financial transactions are called "transfers" instead of "transactions" because
the latter term is heavily overloaded in the context of databases.

### Updates

Transfers *cannot be modified* after creation.

## Modes

Fields used by each mode of transfer:

| Field                         | Single-Phase | Pending  | Post-Pending | Void-Pending |
| ----------------------------- | ------------ | -------- | ------------ | ------------ |
| `id`                          | required     | required | required     | required     |
| `debit_account_id`            | required     | required | optional     | optional     |
| `credit_account_id`           | required     | required | optional     | optional     |
| `amount`                      | required     | required | optional     | optional     |
| `pending_id`                  | none         | none     | required     | required     |
| `user_data_128`               | optional     | optional | optional     | optional     |
| `user_data_64`                | optional     | optional | optional     | optional     |
| `user_data_32`                | optional     | optional | optional     | optional     |
| `timeout`                     | none         | optional | none         | none         |
| `ledger`                      | required     | required | optional     | optional     |
| `code`                        | required     | required | optional     | optional     |
| `flags.linked`                | optional     | optional | optional     | optional     |
| `flags.pending`               | false        | true     | false        | false        |
| `flags.post_pending_transfer` | false        | false    | true         | false        |
| `flags.void_pending_transfer` | false        | false    | false        | true         |
| `flags.balancing_debit`       | optional     | optional | false        | false        |
| `flags.balancing_credit`      | optional     | optional | false        | false        |
| `timestamp`                   | none         | none     | none         | none         |

TigerBeetle uses the same data structures internally and
externally. This means that sometimes you need to set temporary values
for fields that TigerBeetle, not you (the user), are responsible.

### Single-phase transfer

Single-phase transfers post funds to accounts immediately when they are created.

### Two-phase transfer

A pending transfer followed by a post-pending transfer, void-pending transfer, or a timeout is
called a "two-phase transfer". Unlike a single-phase transfer, a two-phase transfer moves funds in
stages:

1. Reserve funds
2. Resolve funds (post, void, or timeout)

Attempting to resolve a pending transfer more than once will return the applicable error result:
- [`pending_transfer_already_posted`](../reference/operations/create_transfers.md#pending_transfer_already_posted)
- [`pending_transfer_already_voided`](../reference/operations/create_transfers.md#pending_transfer_already_voided)
- [`pending_transfer_expired`](../reference/operations/create_transfers.md#pending_transfer_expired)

#### Pending transfer

A pending transfer, denoted by [`flags.pending`](#flagspending),
reserves its `amount` in the debit/credit accounts'
[`debits_pending`](./accounts.md#debits_pending)/[`credits_pending`](./accounts.md#credits_pending)
fields respectively, leaving `debits_posted`/`credits_posted` unmodified.

#### Post-pending transfer

A post-pending transfer, denoted by [`flags.post_pending_transfer`](#flagspost_pending_transfer),
causes the corresponding pending transfer (referenced by [`pending_id`](#pending_id)) to "post",
transferring some or all of the pending transfer's reserved amount to its destination, and restoring
(voiding) the remainder (if any) to its origin accounts.

* If the posted `amount` is 0, the full pending transfer's amount is
  posted.
* If the posted `amount` is nonzero, then only this amount is posted,
  and the remainder is restored to its original accounts. It must be
  less than or equal to the pending transfer's amount.

Additionally, when `flags.post_pending_transfer` is set:

* `pending_id` must reference a [pending transfer](#pending-transfer).
* `flags.void_pending_transfer` must not be set.

And the following fields may either be zero, otherwise must match the
value of the pending transfer's field:

* `debit_account_id`
* `credit_account_id`
* `ledger`
* `code`

#### Void-pending transfer

A void-pending transfer, denoted by [`flags.void_pending_transfer`](#flagsvoid_pending_transfer),
causes the pending transfer (referenced by [`pending_id`](#pending_id)) to void. The pending amount
is restored to its original accounts.
Additionally, when this field is set:

* `pending_id` must reference a [pending transfer](#pending-transfer).
* `flags.post_pending_transfer` must not be set.

And the following fields may either be zero, otherwise must match the
value of the pending transfer's field:

* `debit_account_id`
* `credit_account_id`
* `amount`
* `ledger`
* `code`

#### Read more

See the [Two-phase transfers](../design/two-phase-transfers.md) guide
for details, examples, and sample code.

## Fields

### `id`

This is a unique identifier for the transaction.

As an example, you might generate a [ULID](../design/data-modeling.md#time-based-identifiers) to
identify each transaction.

Constraints:

* Type is 128-bit unsigned integer (16 bytes)
* Must not be zero or `2^128 - 1`
* Must not conflict with another transfer

### `debit_account_id`

This refers to the account to debit the transfer's [`amount`](#amount).

Constraints:

* Type is 128-bit unsigned integer (16 bytes)
* When `flags.post_pending_transfer` and `flags.void_pending_transfer` are unset:
  * Must match an existing account
  * Must not be the same as `credit_account_id`
* When `flags.post_pending_transfer` or `flags.void_pending_transfer` are set:
  - If `debit_account_id` is zero, it will be automatically set to the pending transfer's
    `debit_account_id`.
  - If `debit_account_id` is nonzero, it must match the corresponding pending transfer's
    `debit_account_id`.

### `credit_account_id`

This refers to the account to credit the transfer's [`amount`](#amount).

Constraints:

* Type is 128-bit unsigned integer (16 bytes)
* When `flags.post_pending_transfer` and `flags.void_pending_transfer` are unset:
  * Must match an existing account
  * Must not be the same as `debit_account_id`
* When `flags.post_pending_transfer` or `flags.void_pending_transfer` are set:
  - If `credit_account_id` is zero, it will be automatically set to the pending transfer's
    `credit_account_id`.
  - If `credit_account_id` is nonzero, it must match the corresponding pending transfer's
    `credit_account_id`.

### `amount`

This is how much should be debited from the `debit_account_id` account
and credited to the `credit_account_id` account.

- When `flags.balancing_debit` is set, this is the maximum amount that will be debited/credited,
  where the actual transfer amount is determined by the debit account's constraints.
- When `flags.balancing_credit` is set, this is the maximum amount that will be debited/credited,
  where the actual transfer amount is determined by the credit account's constraints.

Constraints:

* Type is 128-bit unsigned integer (16 bytes)
* When `flags.post_pending_transfer` is set:
  * If `amount` is zero, it will be automatically be set to the pending transfer's `amount`.
  * If `amount` is nonzero, it must be less than or equal to the pending transfer's `amount`.
* When `flags.void_pending_transfer` is set:
  * If `amount` is zero, it will be automatically be set to the pending transfer's `amount`.
  * If `amount` is nonzero, it must be equal to the pending transfer's `amount`.
* When `flags.balancing_debit` and/or `flags.balancing_credit` is set, if `amount` is zero,
  it will automatically be set to the maximum amount that does not violate the corresponding
  account limits. (Equivalent to setting `amount = 2^128 - 1`).
* When all of the following flags are not set, `amount` must be nonzero:
  * `flags.post_pending_transfer`
  * `flags.void_pending_transfer`
  * `flags.balancing_debit`
  * `flags.balancing_credit`

#### Examples

- For representing fractional amounts (e.g. `$12.34`), see
  [Fractional Amounts](../recipes/fractional-amounts.md).
- For balancing transfers, see [Close Account](../recipes/close-account.md).

### `pending_id`

If this transfer will post or void a pending transfer, `pending_id`
references that pending transfer. If this is not a post or void
transfer, it must be zero.

See also:
* [Pending Transfer](#pending-transfer)
* [Post-Pending Transfer](#post-pending-transfer)
* [Void-Pending Transfer](#void-pending-transfer)

Constraints:

* Type is 128-bit unsigned integer (16 bytes)
* Must be zero if neither void nor pending transfer flag is set
* Must match an existing transfer's [`id`](#id) if non-zero

### `user_data_128`

This is an optional 128-bit secondary identifier to link this transfer to an
external entity or event.

As an example, you might use a [ULID](../design/data-modeling.md#time-based-identifiers)
that ties together a group of transfers.

For more information, see [Data Modeling](../design/data-modeling.md#user_data).

Constraints:

* Type is 128-bit unsigned integer (16 bytes)

### `user_data_64`

This is an optional 64-bit secondary identifier to link this transfer to an
external entity or event.

As an example, you might use this field store an external timestamp.

For more information, see [Data Modeling](../design/data-modeling.md#user_data).

Constraints:

* Type is 64-bit unsigned integer (8 bytes)

### `user_data_32`

This is an optional 32-bit secondary identifier to link this transfer to an
external entity or event.

As an example, you might use this field to store a timezone or locale.

For more information, see [Data Modeling](../design/data-modeling.md#user_data).

Constraints:

* Type is 32-bit unsigned integer (4 bytes)

### `timeout`

This is the interval in seconds after a
[`pending`](#flagspending) transfer's [arrival at the cluster](#timestamp)
that it may be posted or voided. Zero denotes absence of timeout.

If the timeout expires and the pending transfer has not already been
posted or voided, the pending transfer is voided automatically.

Non-pending transfers cannot have a timeout.

Constraints:

* Type is 32-bit unsigned integer (4 bytes)
* Must be zero if `flags.pending` is *not* set

### `ledger`

This is an identifier that partitions the sets of accounts that can
transact with each other. Put another way, money cannot transfer
between two accounts with different `ledger` values. See:
[`accounts_must_have_the_same_ledger`](./operations/create_transfers.md#accounts_must_have_the_same_ledger).

[Currency exchange](../recipes/currency-exchange.md) is implemented with two or more linked
transfers.

Constraints:

* Type is 32-bit unsigned integer (4 bytes)
* When `flags.post_pending_transfer` or `flags.void_pending_transfer` is set:
  * If `ledger` is zero, it will be automatically be set to the pending transfer's `ledger`.
  * If `ledger` is nonzero, it must match the `ledger` value on the pending transfer's
    `debit_account_id` **and** `credit_account_id`.
* When `flags.post_pending_transfer` and `flags.void_pending_transfer` are not set:
  * `ledger` must not be zero.
  * `ledger` must match the `ledger` value on the accounts referenced in
    `debit_account_id` **and** `credit_account_id`.

### `code`

This is a user-defined enum denoting the reason for (or category of) the
transfer.

Constraints:

* Type is 16-bit unsigned integer (2 bytes)
* When `flags.post_pending_transfer` or `flags.void_pending_transfer` is set:
  * If `code` is zero, it will be automatically be set to the pending transfer's `code`.
  * If `code` is nonzero, it must match the pending transfer's `code`.
* When `flags.post_pending_transfer` and `flags.void_pending_transfer` are not set, `code` must not
  be zero.

### `flags`

This specifies (optional) transfer behavior.

Constraints:

* Type is 16-bit unsigned integer (2 bytes)
* Some flags are mutually exclusive; see
  [`flags_are_mutually_exclusive`](./operations/create_transfers.md#flags_are_mutually_exclusive).

#### `flags.linked`

When the `linked` flag is specified, it links a transfer with the next
transfer in the batch, to create a chain of transfers, of arbitrary
length, which all succeed or fail in creation together. The tail of a
chain is denoted by the first transfer without this flag. The last
transfer in a batch may therefore never have `flags.linked` set as
this would leave a chain open-ended (see `linked_event_chain_open`).

Multiple chains of individual transfers may coexist within a batch to
succeed or fail independently. Transfers within a chain are executed
within order, or are rolled back on error, so that the effect of each
transfer in the chain is visible to the next, and so that the chain is
either visible or invisible as a unit to subsequent transfers after the
chain. The transfer that was the first to break the chain will have a
unique error result. Other transfers in the chain will have their error
result set to
[`linked_event_failed`](./operations/create_transfers.md#linked_event_failed).

Consider this set of transfers as part of a batch:

| Transfer | Index within batch | flags.linked |
|----------|--------------------|--------------|
| `A`      | `0`                | `false`      |
| `B`      | `1`                | `true`       |
| `C`      | `2`                | `true`       |
| `D`      | `3`                | `false`      |
| `E`      | `4`                | `false`      |

If any of transfers `B`, `C`, or `D` fail (for example, due to
[`exceeds_credits`](./operations/create_transfers.md#exceeds_credits),
then `B`, `C`, and `D` will all fail. They are linked.

Transfers `A` and `E` fail or succeed independently of `B`, `C`, `D`,
and each other.

After the link has executed, the association of each event is lost.
To save the association, it must be
[encoded into the data model](../design/data-modeling.md).

##### Examples

- [Currency Exchange](../recipes/currency-exchange.md)

#### `flags.pending`

Mark the transfer as a [pending transfer](#pending-transfer).

#### `flags.post_pending_transfer`

Mark the transfer as a [post-pending transfer](#post-pending-transfer).

#### `flags.void_pending_transfer`

Mark the transfer as a [void-pending transfer](#void-pending-transfer).

#### `flags.balancing_debit`

Transfer at most [`amount`](#amount) — automatically transferring less than `amount` as necessary
such that `debit_account.debits_pending + debit_account.debits_posted ≤ debit_account.credits_posted`.
If `amount` is set to `0`, transfer at most `2^64 - 1` (i.e. as much as possible).

If the highest amount transferable is `0`, returns
[`exceeds_credits`](./operations/create_transfers.md#exceeds_credits).

Retrying a balancing transfer will return
[`exists_with_different_amount`](./operations/create_transfers.md#exists_with_different_amount)
if the amount of the retry differs from the amount that was actually transferred.

The `amount` of the recorded transfer is set to the actual amount that was transferred, which is
less than or equal to the amount that was passed to `create_transfers`.

`flags.balancing_debit` is exclusive with the `flags.post_pending_transfer`/`flags.void_pending_transfer`
flags because posting or voiding a pending transfer will never exceed/overflow either account's limits.

`flags.balancing_debit` is compatible with (and orthogonal to) `flags.balancing_credit`.

##### Examples

- [Close Account](../recipes/close-account.md)

#### `flags.balancing_credit`

Transfer at most [`amount`](#amount) — automatically transferring less than `amount` as necessary
such that `credit_account.credits_pending + credit_account.credits_posted ≤ credit_account.debits_posted`.
If `amount` is set to `0`, transfer at most `2^64 - 1` (i.e. as much as possible).

If the highest amount transferable is `0`, returns
[`exceeds_debits`](./operations/create_transfers.md#exceeds_debits).

Retrying a balancing transfer will return
[`exists_with_different_amount`](./operations/create_transfers.md#exists_with_different_amount)
if the amount of the retry differs from the amount that was actually transferred.

The `amount` of the recorded transfer is set to the actual amount that was transferred, which is
less than or equal to the amount that was passed to `create_transfers`.

`flags.balancing_credit` is exclusive with the `flags.post_pending_transfer`/`flags.void_pending_transfer`
flags because posting or voiding a pending transfer will never exceed/overflow either account's limits.

`flags.balancing_credit` is compatible with (and orthogonal to) `flags.balancing_debit`.

##### Examples

- [Close Account](../recipes/close-account.md)

### `timestamp`

This is the time the transfer was created, as nanoseconds since
UNIX epoch.

It is set by TigerBeetle to the moment the transfer arrives at
the cluster.

Additionally, all timestamps are unique, immutable and [totally
ordered](http://book.mixu.net/distsys/time.html). So a transfer that
is created before another transfer is guaranteed to have an earlier
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
for `const Transfer = extern struct {`.

You can find the source code for creating a transfer in
[src/state_machine.zig](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/state_machine.zig). Search
for `fn create_transfer(`.
