# `get_account_transfers`

Fetch [`Transfer`](../transfers.md)s involving a given [`Account`](../accounts.md).

## Event

The query filter: 

### `account_id`

The unique identifier of the account for which the transfers will be retrieved.

Constraints:

* Type is 128-bit unsigned integer (16 bytes)
* Must not be zero or `2^128 - 1`

### `timestamp`

The [`Transfer.timestamp`](../transfers.md#timestamp) from which results will be returned, exclusive range
applied as:

- Greater than `>` when [reversed](#flagsreversed) is `false`.
- Less than `<` when [reversed](#flagsreversed) is `true`.

This parameter together with [`limit`](#limit) can be used for pagination, passing the last
timestamp seen from the previous results.

Optional, use zero for no filter by timestamp.

Constraints:

* Type is 64-bit unsigned integer (8 bytes)
* Must not be `2^64 - 1`

### `limit`

The maximum number of [`Transfer`](../transfers.md)s that can be returned by this query.

Limited by the [maximum message size](../../design/client-requests.md#batching-events).

Constraints:

* Type is 32-bit unsigned integer (4 bytes)
* Must not be zero

### `flags`

A bitfield that specifies querying behavior.

Constraints:

* Type is 32-bit unsigned integer (4 bytes)

#### `flags.debits`

Whether or not to include [`Transfer`](../transfers.md)s where the field [`debit_account_id`](../transfers.md#debit_account_id)
matches the parameter [`account_id`](#account_id).

#### `flags.credits`

Whether or not to include [`Transfer`](../transfers.md)s where the field [`credit_account_id`](../transfers.md#credit_account_id)
matches the parameter [`account_id`](#account_id).

#### `flags.reversed`

Whether the results are sorted by timestamp in chronological or reverse-chronological order.

## Result

- If any matching transfers exist, return an array of [`Transfer`](../transfers.md)s.  
- If no matching transfers exist, return nothing.  
- If any constraint is violated, return nothing. 

## Client libraries

For language-specific docs see:

* [Looking up transfers using the .NET library](/src/clients/dotnet/README.md#get-account-transfers)
* [Looking up transfers using the Java library](/src/clients/java/README.md#get-account-transfers)
* [Looking up transfers using the Go library](/src/clients/go/README.md#get-account-transfers)
* [Looking up transfers using the Node.js library](/src/clients/node/README.md#get-account-transfers)
