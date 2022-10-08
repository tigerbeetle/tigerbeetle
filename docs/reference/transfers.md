# Transfers

A `transfer` is an immutable record of a financial transaction between
two accounts.

TigerBeetle uses the same data structures internally and
externally. This means that sometimes you need to set temporary values
for fields that TigerBeetle, not you (the user), are responsible.

### Updates

Transfer fields *cannot be changed by the user* after
creation.

## Fields

### `id`

This is a unique identifier for the transaction.

As an example, you might generate a UUID to identify each transaction.

Constraints:

* Type is 128-bit unsigned integer (16 bytes)
* Must not be zero or `2^128 - 1`
* Must be unique

### `debit_account_id`

This refers to the account to debit the transfer's [`amount`](#amount).

Constraints:

* Type is 128-bit unsigned integer (16 bytes)
* Must match an existing account
* Must not be the same as `credit_account_id`

### `credit_account_id`

This refers to the account to credit the transfer's [`amount`](#amount).

Constraints:

* Type is 128-bit unsigned integer (16 bytes)
* Must match an existing account
* Must not be the same as `debit_account_id`

### `user_data`

This is an optional secondary identifier to link this transfer to an
external entity.

As an example, you might use a UUID that ties together a group of
transfers.

Constraints:

* Type is 128-bit unsigned integer (16 bytes)

### `reserved`

This space may be used for additional data in the future.

Constraints:

* Type is 128-bit unsigned integer (16 bytes)
* Must be zero

### `pending_id`

If this transfer will post or void a pending transfer, `pending_id`
references that pending transfer. If this is not a post or void
transfer, it must be zero.

See also:
* [`flags.post_pending_transfer`](#flags.post_pending_transfer)
* [`flags.void_pending_transfer`](#flags.void_pending_transfer)

Constraints:

* Type is 128-bit unsigned integer (16 bytes)
* Must be zero if neither void nor pending transfer flag is set
* Must match an existing transfer's [`id`](#id) if non-zero

### `timeout`

This is the interval (in nanoseconds) after a
[`pending`](#flags.pending) transfer's creation that it may be posted
or voided.

If the timeout expires and the pending transfer has not already been
posted or voided, the pending transfer is voided automatically.

Non-pending transfers cannot have a timeout.

Constraints:

* Type is 64-bit unsigned integer (8 bytes)
* Must be zero if `flags.pending` is *not* set
* Must be non-zero if `flags.pending` *is* set

### `ledger`

This is an identifier that partitions the sets of accounts that can
transact with each other. Put another way, money cannot transfer
between two accounts with different `ledger` values. See:
`errors.accounts_must_have_the_same_ledger`.

Constraints:

* Type is 32-bit unsigned integer (4 bytes)
* Must not be zero
* Must match the `ledger` value on the accounts referenced in `debit_account_id` **and** `credit_account_id`

### `code`

This is a user-defined enum denoting the reason for (or category of) the
transfer.

Constraints:

* Type is 16-bit unsigned integer (2 bytes)
* Must not be zero

### `flags`

This specifies (optional) transfer behavior.

Constraints:

* Type is 16-bit unsigned integer (2 bytes)

#### `flags.linked`

When the linked flag is specified, it links a transfer with the next
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
result set to `linked_event_failed`.

Consider this set of transfers as part of a batch:
	
| Transfer | Index within batch | flags.linked |
|----------|--------------------|--------------|
| `A`      | `0`                | `false`      |
| `B`      | `1`                | `true`       |
| `C`      | `2`                | `true`       |
| `D`      | `3`                | `false`      |
| `E`      | `4`                | `false`      |

If any of transfers `B`, `C`, or `D` fail (for example, due to
`error.exceeds_credits`), then `B`, `C`, and `D` will all fail. They
are linked.

Transfers `A` and `E` fail or succeed independently of `B`, `C`, and
`D`.

#### `flags.pending`

A `pending` transfer reserves its [`amount`](#amount) in the
debit/credit accounts'
[`debits_pending`](./accounts.md#debits_pending)/[`credits_pending`](./accounts.md#credits_pending)
fields respectively. The pending transfer is complete when the first of the
following events occur:

* If a corresponding
  [`post_pending_transfer`](#flags.post_pending_transfer) is
  committed, some or all of the pending transfer's reserved funds are
  posted, and the remainder (if any) is restored to the original
  accounts.
* If a corresponding
  [`void_pending_transfer`](#flags.void_pending_transfer) is
  committed, the pending transfer's reserved funds are restored to
  their original accounts.
* If the pending transfer's timeout expires, the pending transfer's
  reserved funds are restored to their original accounts. (When `flags.pending`
  is set, the [`timeout`](#timeout) field must be non-zero.)

#### `flags.post_pending_transfer`

This flag causes the pending transfer (referred to by
[`pending_id`](#pending_id)) to "post", transferring some or all of the money the
pending transfer reserved to its destination, and restoring the
remainder (if any) to its origin accounts. The `amount` of a
`post_pending_transfer` must be less-than or equal-to the pending
transfer's amount.

* If the posted `amount` is 0, the full pending transfer's amount is
  posted.
* If the posted `amount` is nonzero, then only this amount is posted,
  and the remainder is restored to its original accounts. It must be
  less than or equal to the pending transfer's amount.

Additionally, when `flags.post_pending_transfer` is set:

* `pending_id` must reference a pending transfer.
* `flags.void_pending_transfer` must not be set.

And the following fields may either be zero, otherwise must match the
value of the pending transfer's field:

* `debit_account_id`
* `credit_account_id`
* `ledger`
* `code`

#### `flags.void_pending_transfer`

This flag causes the pending transfer (referred to by
[`pending_id`](#pending_id)) to void. The pending transfer's
referenced debit/credit accounts'
[`debits_pending`](./accounts.md#debits_pending)/[`credits_pending`](./accounts.md#credits_pending)
fields .

Additionally, when this field is set:

* `pending_id` must reference a pending transfer.
* `flags.post_pending_transfer` must not be set.

And the following fields may either be zero, otherwise must match the
value of the pending transfer's field:

* `debit_account_id`
* `credit_account_id`
* `ledger`
* `code`
* `amount`

### `amount`

This is how much should be debited from the `debit_account_id` account
and credited to the `credit_account_id` account.

* Type is 64-bit unsigned integer (8 bytes)
* Must not be zero, unless marking a pending transfer as posted

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
[src/tigerbeetle.zig](https://github.com/tigerbeetledb/tigerbeetle/blob/main/src/tigerbeetle.zig). Search
for `const Transfer = extern struct {`.

You can find the source code for creating a transfer in
[src/state_machine.zig](https://github.com/tigerbeetledb/tigerbeetle/blob/main/src/state_machine.zig). Search
for `fn create_transfer(`.
