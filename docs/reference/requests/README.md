# Requests

A _request_ queries or updates the database state.

A request consists of one or more _events_ of the same type sent to the cluster in a single message.
For example, a single request can create multiple transfers but it cannot create both accounts and
transfers.

The cluster commits an entire request at once. Events are applied in series, such that successive
events observe the effects of previous ones and event timestamps are
[totally ordered](../../coding/time.md#timestamps-are-totally-ordered).

Each request receives one _reply_ message from the cluster. The reply contains one _result_ for each
event in the request.

## Request Types

- [`create_accounts`](./create_accounts.md): create [`Account`s](../account.md)
- [`create_transfers`](./create_transfers.md): create [`Transfer`s](../transfer.md)
- [`lookup_accounts`](./lookup_accounts.md): fetch `Account`s by `id`
- [`lookup_transfers`](./lookup_transfers.md): fetch `Transfer`s by `id`
- [`get_account_transfers`](./get_account_transfers.md): fetch `Transfer`s by `debit_account_id` or
  `credit_account_id`
- [`get_account_balances`](./get_account_balances.md): fetch the historical account balance by the
  `Account`'s `id`.

_More request types, including more powerful queries, are coming soon!_

## Events and Results

Each request has a corresponding _event_ and _result_ type:

| Request Type            | Event                                               | Result                                                          |
| ----------------------- | --------------------------------------------------- | --------------------------------------------------------------- |
| `create_accounts`       | [`Account`](./create_accounts.md#Event)             | [`CreateAccountResult`](./create_accounts.md#Result)            |
| `create_transfers`      | [`Transfer`](./create_transfers.md#Event)           | [`CreateTransferResult`](./create_transfers.md#Result)          |
| `lookup_accounts`       | [`Account.id`](./lookup_accounts.md#Event)          | [`Account`](./lookup_accounts.md#Result) or nothing             |
| `lookup_transfers`      | [`Transfer.id`](./lookup_transfers.md#Event)        | [`Transfer`](./lookup_transfers.md#Result) or nothing           |
| `get_account_transfers` | [`AccountFilter`](./get_account_transfers.md#Event) | [`Transfer`](./get_account_transfers.md#Result) or nothing      |
| `get_account_balances`  | [`AccountFilter`](./get_account_balances.md#Event)  | [`AccountBalance`](./get_account_balances.md#Result) or nothing |

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
| `lookup_accounts`       |                        8190 |                       8190 |
| `lookup_transfers`      |                        8190 |                       8190 |
| `create_accounts`       |                        8190 |                       8190 |
| `create_transfers`      |                        8190 |                       8190 |
| `get_account_transfers` |                           1 |                       8190 |
| `get_account_balances`  |                           1 |                       8190 |

TigerBeetle clients automatically batch events. Therefore, it is recommended to share the client
instances between multiple threads or tasks to have events batched transparently.

- [Node](/src/clients/node/README.md#batching)
- [Go](/src/clients/go/README.md#batching)
- [Java](/src/clients/java/README.md#batching)
- [.NET](/src/clients/dotnet/README.md#batching)

## Linked Events

Events within a request [succeed or fail](./create_transfers.md#result) independently unless they
are explicitly linked using the `flags.linked` ([`Account.flags.linked`](../account.md#flagslinked)
or [`Transfer.flags.linked`](../transfer.md#flagslinked)).

When the `linked` flag is specified, it links the outcome of a Transfer or Account creation with the
outcome of the next one in the request. These chains of events will all succeed or fail together.

**The last event in a chain is denoted by the first Transfer or Account without this flag.**

The last Transfer or Account in a request may never have the `flags.linked` set, as it would leave a
chain open-ended. Attempting to do so will result in the
[`linked_event_chain_open`](./create_transfers.md#linked_event_chain_open) error.

Multiple chains of events may coexist within a request to succeed or fail independently.

Events within a chain are executed in order, or are rolled back on error, so that the effect of each
event in the chain is visible to the next. Each chain is either visible or invisible as a unit to
subsequent transfers after the chain. The event that was the first to fail within a chain will have
a unique error result. Other events in the chain will have their error result set to
[`linked_event_failed`](./create_transfers.md#linked_event_failed).

### Linked Transfers Example

Consider this set of Transfers as part of a request:

| Transfer | Index in Request | flags.linked |
| -------- | ---------------- | ------------ |
| `A`      | `0`              | `false`      |
| `B`      | `1`              | `true`       |
| `C`      | `2`              | `true`       |
| `D`      | `3`              | `false`      |
| `E`      | `4`              | `false`      |

If any of transfers `B`, `C`, or `D` fail (for example, due to
[`exceeds_credits`](./create_transfers.md#exceeds_credits)), then `B`, `C`, and `D` will all fail.
They are linked.

Transfers `A` and `E` fail or succeed independently of `B`, `C`, `D`, and each other.

After the chain of linked events has executed, the fact that they were linked will not be saved. To
save the association between Transfers or Accounts, it must be
[encoded into the data model](../../coding/data-modeling.md), for example by adding an ID to one of
the [user data](../../coding/data-modeling.md#user_data) fields.

## Guarantees

- A request executes within the cluster at most once.
- Requests do not [time out](../sessions.md#retries). Clients will continuously retry requests until
  they receive a reply from the cluster. This is because in the case of a network partition, a lack
  of response from the cluster could either indicate that the request was dropped before it was
  processed or that the reply was dropped after the request was processed. Note that individual
  [pending transfers](../../coding/two-phase-transfers.md) within a request may have
  [timeouts](../transfer.md#timeout).
- Requests retried by their original client session receive identical replies.
- Requests retried by a different client (same request body, different session) may receive
  different replies.
- Events within a request are executed in sequence. The effects of a given event are observable when
  the next event within that request is applied.
- Events within a request do not interleave with events from other requests.
- All events within a request batch are committed, or none are. Note that this does not mean that
  all of the events in a batch will succeed, or that all will fail. Events succeed or fail
  independently unless they are explicitly [linked](#linked-events)
- Once committed, an event will always be committed — the cluster's state never backtracks.
- Within a cluster, object
  [timestamps are unique and strictly increasing](../../coding/time.md#timestamps-are-totally-ordered).
  No two objects within the same cluster will have the same timestamp. Furthermore, the order of the
  timestamps indicates the order in which the objects were committed.
