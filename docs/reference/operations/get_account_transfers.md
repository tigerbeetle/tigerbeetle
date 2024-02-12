# `get_account_transfers`

Fetch [`Transfer`](../transfers.md)s involving a given [`Account`](../accounts.md).

## Event

The query filter. See [`AccountFilter`](../account_filter.md) for constraints.

## Result

- If any matching transfers exist, return an array of [`Transfer`](../transfers.md)s.  
- If no matching transfers exist, return nothing.  
- If any constraint is violated, return nothing. 

## Client libraries

For language-specific docs see:

* [.NET library](/src/clients/dotnet/README.md#get-account-transfers)
* [Java library](/src/clients/java/README.md#get-account-transfers)
* [Go library](/src/clients/go/README.md#get-account-transfers)
* [Node.js library](/src/clients/node/README.md#get-account-transfers)
