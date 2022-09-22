# Accounts
An `Account` is a record storing the cumulative effect of committed [transfers](./transfers.md).

`Account`s use double-entry accounting: each tracks debits and credits separately.

TigerBeetle uses the same data structures internally and
externally. This means that sometimes you need to set temporary values
for fields that TigerBeetle, not you (the user), are responsible.

### Updates

Account fields *cannot be changed by the user* after
creation. However, debits and credits fields are updated by
TigerBeetle as transfers move money to and from an account.

## Fields

### `id`

`id` is a unique, client-defined identifier for the account.

The `id` should be a UUID.

Constraints:

* Type is 128-bit unsigned integer (16 bytes)
* Must not be zero or `2^128 - 1` (the highest 128-bit unsigned integer).
* Must not conflict with an another account.

### `user_data`

This is an optional second identifier to link this account to an
external entity.

As an example, you might use a UUID (encoded as an integer) that
ties together a group of accounts.

Constraints:

* Type is 128-bit unsigned integer (16 bytes)
* May be zero

### `reserved`

Reserved for future use.

Constraints:

* Type is 48 bytes
* Must be zero

### `ledger`

Identifier used to enforce transfers between the same ledger.

As an example, you might use `1` to represent USD and `2` to represent
EUR.

Constraints:

* Type is 32-bit unsigned integer (4 bytes)
* Must not be zero

### `code`

A user-defined enum representing the type of the account.

As an example, you might use codes `1000`-`3340` to indicate asset
accounts in general, where `1001` is Bank Account and `1002` is Money
Market Account and `2003` is Motor Vehicles and so on.

Constraints:

* Type is 16-bit unsigned integer (2 bytes)
* Must not be zero

### `flag`

A bitfield that toggles additional behavior.

Constraints:

* Type is 16-bit unsigned integer (2 bytes)

#### `linked` flag

When the linked flag is specified, it links an account with the next
account in the batch, to create a chain of account, of arbitrary
length, which all succeed or fail in creation together. The tail of a
chain is denoted by the first account without this flag. The last
account in a batch may therefore never have the `linked` flag set as
this would leave a chain open-ended.

Multiple chains or individual accounts may coexist within a batch to
succeed or fail independently. Accounts within a chain are executed
within order, or are rolled back on error, so that the effect of each
account in the chain is visible to the next, and so that the chain is
either visible or invisible as a unit to subsequent accounts after the
chain. The account that was the first to break the chain will have a
unique error result. Other accounts in the chain will have their error
result set to `linked_event_failed`.

#### `debits_must_not_exceed_credits` flag

When set, transfers will be rejected that would cause this account's
debits to exceed credits. Specifically when `account.debits_pending +
account.debits_posted + transfer.amount > account.credits_posted`.

This cannot be set when `credits_must_not_exceed_debits` is also set.

#### `credits_must_not_exceed_debits` flag

When set, transfers will be rejected that would cause this account's
credits to exceed debits. Specifically when `account.credits_pending +
account.credits_posted + transfer.amount > account.debits_posted`.

This cannot be set when `debits_must_not_exceed_credits` is also set.

### `debits_pending`

Amount of pending debits.

Constraints:

* Type is 64-bit unsigned integer (8 bytes)
* Must be zero when the account is created

### `debits_posted`

Amount of posted debits.

Constraints:

* Type is 64-bit unsigned integer (8 bytes)
* Must be zero when the account is created

### `credits_pending`

Amount of pending credits.

Constraints:

* Type is 64-bit unsigned integer (8 bytes)
* Must be zero when the account is created

### `credits_posted`

Amount of posted credits.

Constraints:

* Type is 64-bit unsigned integer (8 bytes)
* Must be zero when the account is created

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
for `const Account = extern struct {`.
