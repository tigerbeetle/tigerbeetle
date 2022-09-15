# Transfers

TigerBeetle uses the same data structures internally and
externally. This means that sometimes you need to set temporary values
for fields that TigerBeetle, not you (the user), are responsible.

### Updates

Transfer fields *cannot be changed by the user* after
creation. However, debits and credits fields are updated by
TigerBeetle as transfers move money to and from an account.

## Fields

### `id`

This is the unique identifier, the primary key, of the transaction.

As an example, you might generate a UUID (encoded as an integer) to
identify each transaction.

Constraints:

* Type is 128-bit unsigned integer (16 bytes)
* Must not be zero or `2^128 - 1`
* Must be unique

### `debit_account_id`

The account to pull an amount from.

Constraints:

* Type is 128-bit unsigned integer (16 bytes)
* Must match an existing account
* Must not be the same as `credit_account_id`

### `credit_account_id`

The account to send an amount to.

Constraints:

* Type is 128-bit unsigned integer (16 bytes)
* Must match an existing account
* Must not be the same as `debit_account_id`

### `user_data`

This is an optional second identifier to link this transfer to an
external entity.

As an example, you might use a UUID (encoded as an integer) that
ties together a group of transfers.

Constraints:

* Type is 128-bit unsigned integer (16 bytes)
* May be zero

### `reserved`

Reserved for future use.

Constraints:

* Type is 128-bit unsigned integer (16 bytes)
* Must be zero

### `pending_id`

If this transfer will post or void a pending transfer, this field is
for the id of that pending transfer. (You specify whether it will
post or void a pending transfer in the [`flags`](#flags) field
documented below).

Constraints:

* Type is 128-bit unsigned integer (16 bytes)
* Must be zero if neither void nor pending transfer flag is set
* Must match an existing transfer if non-zero

### `timeout`

This field relates to the [`pending` flag](#pending-flag) described
below. It is a duration expressed in nanoseconds.

Constraints:

* Type is 64-bit integer (8 bytes)
* Must be zero if `pending` flag is *not* set
* Must be non-zero if `pending` flag *is* set

### `ledger`

Identifier used to enforce transfers between the same ledger.

Constraints:

* Type is 32-bit unsigned integer (4 bytes)
* Must not be zero
* Must match the `ledger` value on the accounts referenced in `debit_account_id` **and** `credit_account_id`

### `code`

A user-defined enum representing the reason for (or type of) the
transfer.

Constraints:

* Type is 16-bit unsigned integer (2 bytes)
* Must not be zero

### `flags`

Specifies behavior during transfers.

Constraints:

* Type is 16-bit unsigned integer (2 bytes)

#### `linked` flag

When the linked flag is specified, it links an transfer with the next
transfer in the batch, to create a chain of transfer, of arbitrary
length, which all succeed or fail in creation together. The tail of a
chain is denoted by the first transfer without this flag. The last
transfer in a batch may therefore never have the `linked` flag set as
this would leave a chain open-ended.

Multiple chains or individual transfers may coexist within a batch to
succeed or fail independently. Transfers within a chain are executed
within order, or are rolled back on error, so that the effect of each
transfer in the chain is visible to the next, and so that the chain is
either visible or invisible as a unit to subsequent transfers after the
chain. The transfer that was the first to break the chain will have a
unique error result. Other transfers in the chain will have their error
result set to `linked_event_failed`.

#### `pending` flag

This flag  commit for this transfer. The transfer is
not complete until a transaction with the same id is sent with the
`post_pending_transfer` or `void_pending_transfer` flag set (both
described below).

When this flag is on, the [`timeout`](timeout)

#### `post_pending_transfer` flag

#### `void_pending_transfer` flag


### `amount`

How much should be sent from the `debit_account_id` account to the
`credit_account_id` account.

* Type is 64-bit unsigned integer (8 bytes)
* Must not be zero

### `timestamp`

Time the account was created. This is set by TigerBeetle. The format
is UNIX timestamp in nanoseconds.

It is set by TigerBeetle the moment the account is committed (created
by consensus).

Constraints:

* Type is 64-bit unsigned integer (8 bytes)
* User sets to zero on creation

## Internals

If you're curious and want to learn more, you can find the source code
for this struct in
[src/tigerbeetle.zig](https://github.com/tigerbeetledb/tigerbeetle/blob/main/src/tigerbeetle.zig). Search
for `const Transfer = extern struct {`.
