# `get_account_transfers`

Fetch [`Transfer`](../transfer.md)s involving a given [`Account`](../account.md).

## Event

The account filter.
See [`AccountFilter`](../account-filter.md) for constraints.

## Result

- Return a (possibly empty) array of [`Transfer`](../transfer.md)s that match the filter.
- If any constraint is violated, return nothing.
- By default, `Transfer`s are sorted chronologically by `timestamp`. You can use the
  [`reversed`](../account-filter.md#flagsreversed) to change this.
- The result is always limited in size. If there are more results, you need to page through them
  using the `AccountFilter`'s [`timestamp_min`](../account-filter.md#timestamp_min) and/or
  [`timestamp_max`](../account-filter.md#timestamp_max).

## Client libraries

For language-specific docs see:

- [.NET library](/src/clients/dotnet/README.md#get-account-transfers)
- [Java library](/src/clients/java/README.md#get-account-transfers)
- [Go library](/src/clients/go/README.md#get-account-transfers)
- [Node.js library](/src/clients/node/README.md#get-account-transfers)
- [Python library](/src/clients/python/README.md#get-account-transfers)
