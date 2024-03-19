---
sidebar_position: 1
---

# Data Modeling

TigerBeetle is a domain-specific database — its schema of [`Account`s](../reference/accounts.md) and
[`Transfer`s](../reference/transfers.md) is built-in and fixed. In return for this prescriptive
design, it provides excellent performance, integrated business logic, and powerful invariants.

This section is a sample of techniques for mapping your application's requirements onto
TigerBeetle's data model. Which (if any) of these techniques are suitable is highly
application-specific.

When possible, round trips and coordination can be minimized by encoding application invariants
directly in TigerBeetle rather than implementing them in the application itself (or with a foreign
database). This is useful for both maintaining consistency and performance.

## Debits vs Credits

TigerBeetle tracks each account's cumulative posted debits and cumulative posted credits. In
double-entry accounting, an account balance is the difference between the two — computed as either
`debits - credits` or `credits - debits` depending on the type of account. It is up to the
application to compute the balance from the cumulative debits/credits.

From the database's perspective the distinction is arbitrary, but accounting conventions recommend
using a certain balance type for certain types of accounts.

### Debit Balances

`balance = debits - credits`

By convention, debit balances are used to represent:
- Operator's Assets
- Operator's Expenses

To enforce a positive (non-negative) debit balance, use
[`flags.credits_must_not_exceed_debits`](../reference/accounts.md#flagscredits_must_not_exceed_debits).

### Credit Balances

`balance = credits - debits`

By convention, credit balances are used to represent:
- Operators's Liabilities
- Equity in the Operator's Business
- Operator's Income

To enforce a positive (non-negative) credit balance, use
[`flags.debits_must_not_exceed_credits`](../reference/accounts.md#flagsdebits_must_not_exceed_credits).
For example, a customer account that is represented as an Operator's Liability would use this flag
to ensure that the balance cannot go negative.

### Example

For example, if TigerBeetle is operated by a bank, with customers Alice and Bob, its ledger might
look something like this:

| Account Owner | Debits Posted | Credits Posted | Flags                            |
| :------------ | ------------: | -------------: | :------------------------------- |
| Bank          |            30 |              0 | `credits_must_not_exceed_debits` |
| Alice         |             0 |             20 | `debits_must_not_exceed_credits` |
| Bob           |             0 |             10 | `debits_must_not_exceed_credits` |

- The bank has a total of $30 in assets.
- Alice and Bob have deposited money ($20 and $10 respectively) in the bank — from the bank's
  perspective this is a liability.
- Alice and Bob cannot "overdraw" their account — that is, their balance will never be negative.
- A transfer from Alice to Bob would debit Alice's account and credit Bob's (decreasing the bank's
  liability to Alice while increasing the bank's liability to Bob).

## Fractional Amounts and Asset Scale

To maximize precision and efficiency, [`Account`](../reference/accounts.md) debits/credits
and [`Transfer`](../reference/transfers.md) amounts are unsigned 128-bit integers.
However, currencies are often denominated in fractional amounts.

To represent a fractional amount in TigerBeetle, **map the smallest useful unit of the fractional
currency to 1**. Consider all amounts in TigerBeetle as a multiple of that unit.

Applications may rescale the integer amounts as necessary when rendering or interfacing with other
systems. But when working with fractional amounts, calculations should be performed on the integers
to avoid loss of precision due to floating-point approximations.

TigerBeetle stores information precisely and efficiently, while applications can still
present fractional amounts to their users in a way that they are familiar with seeing them.

### Asset Scale

When the multiplier is a power of 10 (e.g. `10 ^ n`), then the exponent `n` is referred to as an
_asset scale_. For example, representing USD in cents uses an asset scale of `2`.

### Examples

- In USD, `$1` = `100` cents. So for example,
  - The fractional amount `$0.45` is represented as the integer `45`.
  - The fractional amount `$123.00` is represented as the integer `12300`.
  - The fractional amount `$123.45` is represented as the integer `12345`.

### Oversized Amounts

The other direction works as well. If the smallest useful unit of a currency is `10,000,000 ¤`,
then it can be scaled down to the integer `1`.

The 128-bit representation defines the precision, but not the scale.

## `user_data`

`user_data_128`, `user_data_64` and `user_data_32` are the most flexible fields in the schema (for
both [accounts](../reference/accounts.md) and [transfers](../reference/transfers.md)). Each
`user_data` field's contents are arbitrary, interpreted only by the application.

Each `user_data` field is indexed for efficient point and range queries.

While the usage of each field is entirely up to you, one way of thinking about each of the fields
is:

- `user_data_128` - this might store the "who" and/or "what" of a transfer. For example, it could be
  a pointer to a business entity stored within the [control
  plane](https://en.wikipedia.org/wiki/Control_plane) database.
- `user_data_64` - this might store a second timestamp for "when" the transaction originated in the
  real world, rather than when the transfer was [timestamped by
  TigerBeetle](./time.md#why-tigerbeetle-manages-timestamps). This can be used if you need to model
  [bitemporality](https://en.wikipedia.org/wiki/Bitemporal_modeling). Alternatively, if you do not
  need this to be used for a timestamp, you could use this field in place of the `user_data_128` to
  store the "who"/"what".
- `user_data_32` - this might store the "where" of a transfer. For example, it could store the
  jurisdiction where the transaction originated in the real world. In certain cases, such as for
  cross-border remittances, it might not be enough to have the UTC timestamp and you may want to
  know the transfer's locale.

(Note that the [`code`](#code) can be used to encode the "why" of a transfer.)

Any of the `user_data` fields can be used as a group identifier for objects that will be queried
together. For example, for multiple transfers used for [currency
exchange](../recipes/currency-exchange.md).

## `id`

The `id` field uniquely identifies each [`Account`](../reference/accounts.md#id) and
[`Transfer`](../reference/transfers.md#id) within the cluster.

The primary purpose of an `id` is to serve as an "idempotency key" — to avoid executing an event
twice. For example, if a client creates a transfer but the server's reply is lost, the client (or
application) will retry — the database must not transfer the money twice.

Note that `id`s are unique per cluster -- not per ledger. You should attach a separate identifier in
the [`user_data`](#user_data) field if you want to store a connection between multiple `Account`s or
multiple `Transfer`s that are related to one another. For example, different currency `Account`s
belonging to the same user or multiple `Transfer`s that are part of a [currency
exchange](../recipes/currency-exchange.md).

[TigerBeetle Time-Based Identifiers](#tigerbeetle-time-based-identifiers-recommended) are
recommended for most applications.

When selecting an `id` scheme:

- Idempotency is particularly important (and difficult) in the context of [application crash
  recovery](./consistency.md#consistency-with-foreign-databases).
- Be careful to [avoid `id` collisions](https://en.wikipedia.org/wiki/Birthday_problem).
- An account and a transfer may share the same `id` (they belong to different "namespaces"), but
  this is not recommended because other systems (that you may later connect to TigerBeetle) may use
  a single "namespace" for all objects.
- Avoid requiring a central oracle to generate each unique `id` (e.g. an auto-increment field in
  SQL). A central oracle may become a performance bottleneck when creating accounts/transfers.
- Sequences of identifiers with long runs of strictly increasing (or strictly decreasing) values are
  amenable to optimization, leading to higher database throughput.
- Random identifiers are not recommended – they can't take advantage of all of the LSM
  optimizations. (Random identifiers have ~10% lower throughput than strictly-increasing ULIDs).

### TigerBeetle Time-Based Identifiers (Recommended)

TigerBeetle recommends using a specific ID scheme for most applications. It is time-based and
lexicographically sortable. The scheme is inspired by ULIDs and UUIDv7s but is better able to take
advantage of LSM optimizations, which leads to higher database throughput.

TigerBeetle clients include an `id()` function to generate IDs using the recommended scheme.

TigerBeetle IDs consist of:

- 48 bits of (millisecond) timestamp (high-order bits)
- 80 bits of randomness (low-order bits)

When creating multiple objects during the same millisecond, we increment the random bytes rather
than generating new random bytes. Furthermore, it is important that IDs are stored in little-endian
with the random bytes as the lower-order bits and the timestamp as the higher-order bits. These
details ensure that a sequence of objects have strictly increasing IDs according to the server,
which improves database optimization.

Similar to ULIDs and UUIDv7s, these IDs have the following benefits:

- they have an insignificant risk of collision.
- they do not require a central oracle to generate.

### Reuse Foreign Identifier

This technique is most appropriate when integrating TigerBeetle with an existing application where
TigerBeetle accounts or transfers map one-to-one with an entity in the foreign database.

Set `id` to a "foreign key" — that is, reuse an identifier of a corresponding object from another
database. For example, if every user (within the application's database) has a single account, then
the identifier within the foreign database can be used as the `Account.id` within TigerBeetle.

To reuse the foreign identifier, it must conform to TigerBeetle's `id`
[constraints](../reference/accounts.md#id).

## `ledger`

The `ledger` identifier on [`Account`s](../reference/accounts.md#ledger) partitions sets of accounts
that can directly transact with one another. This is used to separate accounts representing
different currencies or other asset types.

[Currency exchange](../recipes/currency-exchange.md) or cross-ledger transfers are implemented with
two or more linked transfers.

You can also use different ledgers to further partition accounts, beyond asset type. For example, if
you have a multi-tenant setup where you are tracking balances for your customers' end-users, you
might have a ledger for each of your customers. If customers have end-user accounts in multiple
currencies, each of your customers would have multiple ledgers.

## `code`

The `code` identifier represents the "why" for an Account or Transfer.

On an [`Account`](../reference/accounts.md#code), the `code` indicates the account type, such as
assets, liabilities, equity, income, or expenses, and subcategories within those classification.

On a [`Transfer`](../reference/transfers.md#code), the `code` indicates why a given transfer is
happening, such as a purchase, refund, currency exchange, etc.

When you start building out your application on top of TigerBeetle, you may find it helpful to list
out all of the known types of accounts and movements of funds and mapping each of these to `code`
numbers or ranges.
