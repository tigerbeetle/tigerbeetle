# `query_transfers`

Query [`Transfer`](../transfer.md)s by the intersection of some fields and by timestamp range.

## Event

The query filter.
See [`QueryFilter`](../query-filter.md) for constraints.

## Result

- Return a (possibly empty) array of [`Transfer`](../transfer.md)s that match the filter.
- If any constraint is violated, return nothing.
- By default, `Transfer`s are sorted chronologically by `timestamp`. You can use the
  [`reversed`](../query-filter.md#flagsreversed) to change this.
- The result is always limited in size. If there are more results, you need to page through them
  using the `QueryFilter`'s [`timestamp_min`](../query-filter.md#timestamp_min) and/or
  [`timestamp_max`](../query-filter.md#timestamp_max).

## Client libraries

For language-specific docs see:

- [.NET library](/src/clients/dotnet/README.md#query-transfers)
- [Java library](/src/clients/java/README.md#query-transfers)
- [Go library](/src/clients/go/README.md#query-transfers)
- [Node.js library](/src/clients/node/README.md#query-transfers)
- [Python library](/src/clients/python/README.md#query-transfers)
