# Data Modeling

TigerBeetle is a domain-specific database — its schema of [`Account`s](../reference/accounts.md)
and [`Transfer`s](../reference/transfers.md) is built-in and fixed. In return for this prescriptive
design, it provides excellent performance, integrated business logic, and powerful invariants.
This section is a sample of techniques for mapping an application's requirements onto TigerBeetle's
data model. Which (if any) of these techniques are suitable is highly application-specific.

When possible, encoding application invariants directly in TigerBeetle rather than implementing
them in the application itself (or with a foreign database) minimizes round-trips and coordination.
This is useful for both maintaining consistency and performance.

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

[`Account`](../reference/accounts.md#id) and [`Transfer`](../reference/transfers.md#id) identifiers
must be unique, so randomly-generated UUIDs are the natural choice — but not the only option.

`id` is mostly accessed by point queries, but it is indexed for efficient range queries as well.

When designing any id scheme:

- `id`'s primary purpose is to ensure idempotence of object creation — this can be
  [difficult in the context of application crash recovery](./integration.md#consistency-with-foreign-databases).
- Be careful to [avoid `id` collisions](https://en.wikipedia.org/wiki/Birthday_problem).
- An account and a transfer may share the same `id` — they belong to different "namespaces".
- Avoid requiring a central oracle to generate each unique `id` (e.g. an auto-increment field in SQL).
  A central oracle may become a performance bottleneck when creating accounts/transfers.

### Examples
#### Reuse Foreign Identifier

Set `id` to a "foreign key" — that is, reuse an identifier of a corresponding object from
another database.

For example, if every user (within the application's database) has a single account, then the
identifier within the foreign database can be used as the `Account.id` within TigerBeetle.

To reuse the foreign identifier, it must conform to TigerBeetle's `id`
[constraints](../reference/accounts.md#id).

This technique is most appropriate when integrating TigerBeetle with an existing application.
It is not ideal because it
[complicates consistency around application crash recover](./integration.md#consistency-with-foreign-databases)
and may have a bottleneck at the foreign database.

#### Logically Grouped Objects

Often accounts or transfers are logically grouped together from the application's perspective.
For example, a simple currency exchange transaction is one logical transfer conducted between
four accounts — two physical transfers.

A non-random identifier scheme can:

  - leave `user_data` free for a different purpose, and
  - allow a group's members and roles to be derived by the application code,

without relying on a [foreign database](#reuse-foreign-identifier) to store metadata for each
member of the group.

A group may (but does not necessarily) correspond to objects chained by
[`flags.linked`](../reference/transfers.md#flags.linked).

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
