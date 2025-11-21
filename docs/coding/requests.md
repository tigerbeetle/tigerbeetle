# Requests

A _request_ queries or updates the database state.

A request consists of one or more _events_ of the same type sent to the cluster in a single message.
For example, a single request can create multiple transfers but it cannot create both accounts and
transfers.

The cluster commits an entire request at once. Events are applied in series, such that successive
events observe the effects of previous ones and event timestamps are
[totally ordered](./time.md#timestamps-are-totally-ordered).

Each request receives one _reply_ message from the cluster. The reply contains one _result_ for each
event in the request.

## Request Types

- [`create_accounts`](../reference/requests/create_accounts.md): create [`Account`s](../reference/account.md)
- [`create_transfers`](../reference/requests/create_transfers.md): create [`Transfer`s](../reference/transfer.md)
- [`lookup_accounts`](../reference/requests/lookup_accounts.md): fetch `Account`s by `id`
- [`lookup_transfers`](../reference/requests/lookup_transfers.md): fetch `Transfer`s by `id`
- [`get_account_transfers`](../reference/requests/get_account_transfers.md): fetch `Transfer`s by `debit_account_id` or
  `credit_account_id`
- [`get_account_balances`](../reference/requests/get_account_balances.md): fetch the historical account balance by the
  `Account`'s `id`.
- [`query_accounts`](../reference/requests/query_accounts.md): query `Account`s
- [`query_transfers`](../reference/requests/query_transfers.md): query `Transfer`s

_More request types, including more powerful queries, are coming soon!_

## Events and Results

Each request has a corresponding _event_ and _result_ type:

| Request Type            | Event                                                                 | Result                                                                              |
| ----------------------- | --------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| `create_accounts`       | [`Account`](../reference/requests/create_accounts.md#event)           | [`CreateAccountResult`](../reference/requests/create_accounts.md#result)            |
| `create_transfers`      | [`Transfer`](../reference/requests/create_transfers.md#event)         | [`CreateTransferResult`](../reference/requests/create_transfers.md#result)          |
| `lookup_accounts`       | [`Account.id`](../reference/requests/lookup_accounts.md#event)        | [`Account`](../reference/requests/lookup_accounts.md#result) or nothing             |
| `lookup_transfers`      | [`Transfer.id`](../reference/requests/lookup_transfers.md#event)      | [`Transfer`](../reference/requests/lookup_transfers.md#result) or nothing           |
| `get_account_transfers` | [`AccountFilter`](../reference/account-filter.md)                     | [`Transfer`](../reference/requests/get_account_transfers.md#result) or nothing      |
| `get_account_balances`  | [`AccountFilter`](../reference/account-filter.md)                     | [`AccountBalance`](../reference/requests/get_account_balances.md#result) or nothing |
| `query_accounts`        | [`QueryFilter`](../reference/query-filter.md)                         | [`Account`](../reference/requests/lookup_accounts.md#result) or nothing             |
| `query_transfers`       | [`QueryFilter`](../reference/query-filter.md)                         | [`Transfer`](../reference/requests/lookup_transfers.md#result) or nothing           |

### Idempotency

Events that create objects are idempotent. The first event to create an object with a given `id`
will receive the `ok` result. Subsequent events that attempt to create the same object will receive
the `exists` result.

## Batching Events

To achieve high throughput, TigerBeetle amortizes the overhead of consensus and I/O by batching many
events in each request.

In the default configuration, the maximum batch sizes for each request type are:

| Request Type            | Request Batch Size (Events) | Reply Batch Size (Results) |
| ----------------------- | --------------------------: | -------------------------: |
| `lookup_accounts`       |                        8189 |                       8189 |
| `lookup_transfers`      |                        8189 |                       8189 |
| `create_accounts`       |                        8189 |                       8189 |
| `create_transfers`      |                        8189 |                       8189 |
| `get_account_transfers` |                          1† |                       8189 |
| `get_account_balances`  |                          1† |                       8189 |
| `query_accounts`        |                          1† |                       8189 |
| `query_transfers`       |                          1† |                       8189 |

- [Node.js](/src/clients/node/README.md#batching)
- [Go](/src/clients/go/README.md#batching)
- [Java](/src/clients/java/README.md#batching)
- [.NET](/src/clients/dotnet/README.md#batching)
- [Python](/src/clients/python/README.md#batching)

### Automatic Batching

TigerBeetle clients automatically batch operations. There may be instances where your application logic 
makes it hard to fill up the batches that you send to TigerBeetle, for example a multi-threaded web 
server where each HTTP request is handled on a different thread. 

The TigerBeetle client should be shared across threads (or tasks, depending on your paradigm), since 
it automatically groups together batches of small sizes into one request. Since  TigerBeetle clients 
can have [**at most one in-flight request**](../reference/sessions.md), the client 
accumulates smaller batches together while waiting for a reply to the last request.

†: For queries (e.g. `get_account_transfers`, etc) TigerBeetle clients use the query `limit` to
automatically batch queries of the same type together into requests when it knows for sure that all
of their results will fit in a single reply.

## Guarantees

- A request executes within the cluster at most once.
- Requests do not [time out](../reference/sessions.md#retries). Clients will continuously retry requests until
  they receive a reply from the cluster. This is because in the case of a network partition, a lack
  of response from the cluster could either indicate that the request was dropped before it was
  processed or that the reply was dropped after the request was processed. Note that individual
  [pending transfers](./two-phase-transfers.md) within a request may have
  [timeouts](../reference/transfer.md#timeout).
- Requests retried by their original client session receive identical replies.
- Requests retried by a different client (same request body, different session) may receive
  different replies.
- Events within a request are executed in sequence. The effects of a given event are observable when
  the next event within that request is applied.
- Events within a request do not interleave with events from other requests.
- All events within a request batch are committed, or none are. Note that this does not mean that
  all of the events in a batch will succeed, or that all will fail. Events succeed or fail
  independently unless they are explicitly [linked](./linked-events.md)
- Once committed, an event will always be committed — the cluster's state never backtracks.
- Within a cluster, object
  [timestamps are unique and strictly increasing](./time.md#timestamps-are-totally-ordered).
  No two objects within the same cluster will have the same timestamp. Furthermore, the order of the
  timestamps indicates the order in which the objects were committed.
