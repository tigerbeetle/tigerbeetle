# `query_accounts`

Query [`Account`](../account.md)s by the intersection of some fields and by timestamp range.

⚠️ It is not possible currently to query more than a full batch (8189) of accounts atomically.
When issuing multiple `query_accounts` calls, it can happen that other operations will interleave
between the calls leading to read skew. Consider using the
[`history`](../account.md#flagshistory) flag to enable atomic lookups.

## Event

The query filter.
See [`QueryFilter`](../query-filter.md) for constraints.

## Result

- Return a (possibly empty) array of [`Account`](../account.md)s that match the filter.
- If any constraint is violated, return nothing.
- By default, `Account`s are sorted chronologically by `timestamp`. You can use the
  [`reversed`](../query-filter.md#flagsreversed) to change this.
- The result is always limited in size. If there are more results, you need to page through them
  using the `QueryFilter`'s [`timestamp_min`](../query-filter.md#timestamp_min) and/or
  [`timestamp_max`](../query-filter.md#timestamp_max).

## Client libraries

For language-specific docs see:

- [.NET library](/src/clients/dotnet/README.md#query-accounts)
- [Java library](/src/clients/java/README.md#query-accounts)
- [Go library](/src/clients/go/README.md#query-accounts)
- [Node.js library](/src/clients/node/README.md#query-accounts)
- [Python library](/src/clients/python/README.md#query-accounts)
