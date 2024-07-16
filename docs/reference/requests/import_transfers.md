# `import_transfers`

Allows the ingestion of historical [`Transfer`](../transfer.md)s with their original
[`timestamp`](../transfer.md#timestamp) into TigerBeetle.

When importing past events, TigerBeetle does not use the real-time clock to assign the transfer's
timestamp but allows the user to define it, expressing _when_ the transfer was effectively created
outside TigerBeetle.

To maintain system invariants regarding auditability and traceability, some constraints are necessary
for user-defined timestamps:

- Must not be zero

  When importing transfers, all events in the same batch must specify a user-defined timestamp.

- No future timestamps

  The user-defined timestamp must be a past date, never ahead of the cluster real-time clock at the
  time the request arrives.

- Unique timestamps expressed as nanoseconds since the UNIX epoch

  The user-defined timestamp must be **unique** across the entire database, and no two objects can
  have the same timestamp, even different objects like a `Transfer` and an `Account` cannot share
  the same timestamp.

- No timestamp regression

  Timestamps must always be increasing. Even user-defined timestamps that are required to be past
  dates need to be at least one nanosecond ahead of the timestamp of the last object committed by
  the cluster.

- No [`timeout`](../transfer.md#timeout) for [pending](../transfer.md#flagspending) transfers

  It's possible to import pending transfers with a user-defined timestamp, but since it's not
  driven by the real-time clock, it cannot define a timeout for automatic expiration.
  In those cases, the [two-phase post or rollback](../../coding/two-phase-transfers.md) must be
  done manually.

Those constraints require importing past events such as
`timestamp > last_timestamp_committed and timestamp < now`, which makes the process naturally
restrictive to perform in a cluster during regular operation, where the last timestamp is
frequently being updated. Instead, it's more suitable to be used in a fresh cluster before any
[`create_accounts`](./create_accounts.md) or [`create_transfers`](./create_transfers.md) operation.

## Event

The transfer to import. See [`Transfer`](../transfer.md) for constraints.

## Result

This operation behaves exactly the same as [`create_transfers`](./create_transfers.md), therefore it shares
the same [result codes](./create_transfers.md#result).

The following exception applies:

- [`timestamp_must_be_zero`](./create_transfers.md#timestamp_must_be_zero)

  This constraint does not apply to `import_transfers`.

## Client libraries

For language-specific docs see:

- [.NET library](/src/clients/dotnet/README.md#importing-historical-events)
- [Java library](/src/clients/java/README.md#importing-historical-events)
- [Go library](/src/clients/go/README.md#importing-historical-events)
- [Node.js library](/src/clients/node/README.md#importing-historical-events)
