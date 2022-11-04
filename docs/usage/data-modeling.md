# Data Modeling

TigerBeetle is a domain-specific database — its schema of [`Account`s](../reference/accounts.md)
and [`Transfer`s](../reference/transfers.md) is built-in and fixed. In return for this prescriptive
design, it provides excellent performance, integrated business logic, and powerful invariants.
This section is a sample of techniques for mapping an application's requirements onto TigerBeetle's
data model. Which (if any) of these techniques are suitable is highly application-specific.

When possible, round trips and coordination can be minimized by encoding application invariants
directly in TigerBeetle rather than implementing them in the application itself (or with a foreign
database). This is useful for both maintaining consistency and performance.

## Debits vs Credits

TigerBeetle tracks each account's cumulative posted debits and cumulative posted credits.
In double-entry accounting, an account balance is the difference between the two — computed as
either `debits - credits` or `credits - debits` depending on the type of account. It is up to the
application to compute the balance from the cumulative debits/credits.

From the database's perspective the distinction is arbitrary, but by convention:

  - `balance = debits - credits` for accounts representing the database operator's assets.
    - Known as a "debit balance".
    - Accounts with limits use
      [`flags.credits_must_not_exceed_debits`](../reference/accounts.md#flagscredits_must_not_exceed_debits).
  - `balance = credits - debits` for accounts representing the database operator's liabilities.
    - Known as a "credit balance".
    - Accounts with limits use
      [`flags.debits_must_not_exceed_credits`](../reference/accounts.md#flagsdebits_must_not_exceed_credits).
  - A transfer "from" account `A` "to" account `B` credits account `A` and debits account `B`.

### Example

For example, if TigerBeetle is operated by a bank, with customers Alice and Bob, its
ledger might look something like this:

| Account Owner | Debits Posted | Credits Posted | Flags                            |
| :------------ | ------------: | -------------: | :------------------------------- |
| Bank          |            30 |              0 | `credits_must_not_exceed_debits` |
| Alice         |             0 |             20 | `debits_must_not_exceed_credits` |
| Bob           |             0 |             10 | `debits_must_not_exceed_credits` |

- The bank has a total of $30 in assets.
- Alice and Bob have deposited money ($20 and $10 respectively) in the bank — from the bank's
  perspective this is a liability.
- Alice and Bob cannot "overdraw" their account — that is, their balance will never be negative.

## `user_data`

`user_data` is the most flexible field in the schema (for both
[accounts](../reference/accounts.md#user_data) and [transfers](../reference/transfers.md#user_data)).
`user_data`'s contents are arbitrary, interpreted only by the application.

`user_data` is indexed for efficient point and range queries.

Example uses:

- Set `user_data` to a "foreign key" — that is, an identifier of a corresponding object within
  another database.
- Set `user_data` to a group identifier for objects that will be queried together.
- Set `user_data` to a transfer or account `id`.
  (TODO: Can we use this for join queries via the query API, or must the application implement them?)

## `id`

The primary purpose of an `id` (for both [accounts](../reference/accounts.md#id) and
[transfers](../reference/transfers.md#id)) is to serve as an "idempotency key".
The database uses ids to avoid repeating duplicate events.

[Randomly-generated identifiers](#random-identifier) are recommended for most applications.

When selecting an `id` scheme:

- Idempotency is particularly important (and difficult) in the context of
  [application crash recovery](./integration.md#consistency-with-foreign-databases).
- Be careful to [avoid `id` collisions](https://en.wikipedia.org/wiki/Birthday_problem).
- An account and a transfer may share the same `id` — they belong to different "namespaces".
- Avoid requiring a central oracle to generate each unique `id` (e.g. an auto-increment field in SQL).
  A central oracle may become a performance bottleneck when creating accounts/transfers.

### Examples
#### Random Identifier

Randomly-generated identifiers are recommended for most applications.

- Random identifiers require coordination with a secondary database to implement idempotent
  [application crash recovery](./integration.md#consistency-with-foreign-databases).
- Random identifiers have an insignificant risk of collisions.
- Random identifiers do not require a central oracle.
- Only point queries are useful for fetching randomly-generated identifiers.

To maximize id entropy, prefer a cryptographically-secure PRNG (most languages have one in their
cryptography library). We don't recommend UUIDv4 because it uses a few fixed bits.

#### Reuse Foreign Identifier

This technique is most appropriate when integrating TigerBeetle with an existing application
where TigerBeetle accounts or transfers map one-to-one with an entity in the foreign database.

Set `id` to a "foreign key" — that is, reuse an identifier of a corresponding object from another
database. For example, if every user (within the application's database) has a single account, then
the identifier within the foreign database can be used as the `Account.id` within TigerBeetle.

To reuse the foreign identifier, it must conform to TigerBeetle's `id`
[constraints](../reference/accounts.md#id).

Like [randomly-generated identifiers](#random-identifier), this technique requires careful
coordination with the foreign database for idempotent
[application crash recovery](./integration.md#consistency-with-foreign-databases).

### Examples (Advanced)

[Randomly-generated identifiers](#random-identifier) are recommended for most applications.

`id` is mostly accessed by point queries, but it is indexed for efficient iteration by range
queries as well. The schemes described in this section take advantage of that index ordering.

#### Logically Grouped Objects

Often accounts or transfers are logically grouped together from the application's perspective.
For example, a [simple currency exchange](../recipes/currency-exchange.md) transaction is one
logical transfer conducted between four accounts — two physical transfers.

A non-random identifier scheme can:

  - leave `user_data` free for a different purpose, and
  - allow a group's members and roles to be derived by the application code,

without relying on a [foreign database](#reuse-foreign-identifier) to store metadata for each
member of the group.

A group may (but does not necessarily) correspond to objects chained by
[`flags.linked`](../reference/transfers.md#flagslinked).

##### Identifier Offsets

For each group, generate a single "root" `id`, and set group member's `id`s relative to that root.

- From the root, use known offsets to derive member identifiers (e.g. `root + 1`).
- From a group member, use `code`, `ledger`, or both to determine the object's role and derive the
  root identifier.

This technique enables a simple range query to iterate every member of a target group.

If groups are large (or variable-sized), it may be be preferable to rely on
[`user_data` for grouping](#user_data) to sidestep the risk of `id` collisions.

##### Identifier Prefixes

When a group consists of a fixed number of heterogeneous members (each with a distinct role),
`id`s with the same role could be created with a common, application-known prefix.
In this arrangement, the suffix could be randomized, but shared by a group's members.

- A group's role is derived from its `id`'s prefix.
- A group's members are derived by swapping the `id` prefix.

This technique enables a simple range query to iterate every object with a target role.
