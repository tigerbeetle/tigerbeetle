# Accounts

TigerBeetle uses the same data structures internally and
externally. This means that sometimes you need to set temporary values
for fields that TigerBeetle, not you (the user), are responsible.

### Updates

Account fields *cannot be changed by the user* after
creation. However, debits and credits fields are updated by
TigerBeetle as transfers move money to and from an account.

## Fields

### `id`

This is the unique identifier, the primary key, of the account.

As an example, you might use a UUID (encoded as an integer) that ties
the account back to a row in a SQL database.

Constraints:

* Type is 128-bit unsigned integer (16 bytes)
* Must not be zero or `2^128 - 1`
* Must be unique

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

Specifies behavior during transfers.

Constraints:

* Type is 16-bit unsigned integer (2 bytes)

### `debits_pending`

Amount of pending debits.

Constraints:

* Type is 64-bit unsigned integer (8 bytes)

### `debits_posted`

Amount of posted debits.

Constraints:

* Type is 64-bit unsigned integer (8 bytes)

### `credits_pending`

Amount of pending credits.

Constraints:

* Type is 64-bit unsigned integer (8 bytes)

### `credits_posted`

Amount of posted credits.

Constraints:

* Type is 64-bit unsigned integer (8 bytes)

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
