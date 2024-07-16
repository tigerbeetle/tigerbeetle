# `import_accounts`

Allows the ingestion of historical [`Account`](../account.md)s with their original
[`timestamp`](../account.md#timestamp) into TigerBeetle.

When importing past events, TigerBeetle does not use the real-time clock to assign the account's
timestamp but allows the user to define it, expressing _when_ the account was effectively created
outside TigerBeetle.

To maintain system invariants regarding auditability and traceability, some constraints are necessary for user-defined timestamps:

- Must not be zero

  When importing accounts, all events in the same batch must specify a user-defined timestamp.

- No future timestamps

  The user-defined timestamp must be a past date, never ahead of the cluster real-time clock at the
  time the request arrives.

- Unique timestamps expressed as nanoseconds since the UNIX epoch

  The user-defined timestamp must be **unique** across the entire database, and no two objects can
  have the same timestamp, even different objects like an `Account` and a `Transfer` cannot share
  the same timestamp.

- No timestamp regression

  Timestamps must always be increasing. Even user-defined timestamps that are required to be past
  dates need to be at least one nanosecond ahead of the timestamp of the last object committed by
  the cluster.

The resultant of those constraints, require importing past events such as
`timestamp > last_timestamp_committed and timestamp < now`, which makes the process naturally
restrictive to perform in a cluster during regular operation, where the last timestamp is
frequently being updated. Instead, it's more suitable to be used in a fresh cluster before any
[`create_accounts`](./create_accounts.md) or [`create_transfers`](./create_transfers.md) operation.

## Event

The account to import. See [`Account`](../account.md) for constraints.

## Result

This operation behaves exactly the same as [`create_accounts`](./create_accounts.md), therefore it shares
the same [result codes](./create_accounts.md#result).

The following exception applies:

- [`timestamp_must_be_zero`](./create_accounts.md#timestamp_must_be_zero)

  This constraint does not apply to `import_accounts`.

## Client libraries

For language-specific docs see:

- [.NET library](/src/clients/dotnet/README.md#importing-historical-events)
- [Java library](/src/clients/java/README.md#importing-historical-events)
- [Go library](/src/clients/go/README.md#importing-historical-events)
- [Node.js library](/src/clients/node/README.md#importing-historical-events)
