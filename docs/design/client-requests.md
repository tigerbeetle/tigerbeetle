---
sidebar_position: 3
---

# Client Requests

A _request_ is a [batch](#batching-events) of one or more [operation
events](../reference/operations/index.md) sent to the cluster in a single message.

All events within a request batch must be of the same [operation](../reference/operations/index.md)
type. You cannot, for example, create accounts and transfers in the same request.

The cluster commits an entire batch at once. Events are applied in series, such that successive
events observe the effects of previous ones and event timestamps are [totally
ordered](time.md#timestamps-are-totally-ordered).

The cluster returns a single reply for each unique request it commits. The reply contains a
[result](../reference/operations/create_transfers.md#result) for each event in the request.

## Linked Events

Events within a request [succeed or fail](../reference/operations/create_transfers.md#result)
independently unless they are explicitly linked using the `flags.linked`
([`Account.flags.linked`](../reference/accounts.md#flagslinked) or
[`Transfer.flags.linked`](../reference/transfers.md#flagslinked)).

When the `linked` flag is specified, it links the outcome of a Transfer or Account creation with the
outcome of the next one in the request. These chains of events will all succeed or fail together.

**The last event in a chain is denoted by the first Transfer or Account without this flag.**

The last Transfer or Account in a batch may never have the `flags.linked` set, as it would leave a
chain open-ended. Attempting to do so will result in the
[`linked_event_chain_open`](../reference/operations/create_transfers.md#linked_event_chain_open) error.

Multiple chains of events may coexist within a batch to succeed or fail independently. 

Events within a chain are executed in order, or are rolled back on error, so that the effect of each
event in the chain is visible to the next. Each chain is either visible or invisible as a unit to
subsequent transfers after the chain. The event that was the first to fail within a chain will have
a unique error result. Other events in the chain will have their error result set to
[`linked_event_failed`](../reference/operations/create_transfers.md#linked_event_failed).

Consider this set of Transfers as part of a batch:

| Transfer | Index within batch | flags.linked |
|----------|--------------------|--------------|
| `A`      | `0`                | `false`      |
| `B`      | `1`                | `true`       |
| `C`      | `2`                | `true`       |
| `D`      | `3`                | `false`      |
| `E`      | `4`                | `false`      |

If any of transfers `B`, `C`, or `D` fail (for example, due to
[`exceeds_credits`](../reference/operations/create_transfers.md#exceeds_credits), then `B`, `C`, and `D` will
all fail. They are linked.

Transfers `A` and `E` fail or succeed independently of `B`, `C`, `D`, and each other.

After the chain of linked events has executed, the fact that they were linked will not be saved. To
save the association between Transfers or Accounts, it must be [encoded into the data
model](../design/data-modeling.md), for example by adding an ID to one of the [user
data](../design/data-modeling.md#user_data) fields.

## Batching Events

To achieve high throughput, TigerBeetle amortizes the overhead of consensus and I/O by batching many
operation events in each request.

For the best performance, each request should batch as many events as possible. Typically this means
funneling events through fewer client instances (e.g. a single client instance per process).

The maximum number of events per batch depends on the maximum message size and the operation type.
(TODO: Expose each operation's batch sizes in the client).

In the default configuration, the batch sizes are:

| Operation               | Request Batch Size (Events) | Reply Batch Size (Results) |
| ----------------------- | --------------------------: | -------------------------: |
| `lookup_accounts`       |                        8190 |                       8190 |
| `lookup_transfers`      |                        8190 |                       8190 |
| `create_accounts`       |                        8190 |                       8190 |
| `create_transfers`      |                        8190 |                       8190 |
| `get_account_transfers` |                           1 |                       8190 |
| `get_account_balances`  |                           1 |                       8190 |

You can design your application to batch events manually. However, client instances automatically
batch requests of the same operation type. Therefore, sharing the same client instance between
multiple threads or tasks enables events to be batched transparently.

- [Node](/src/clients/node/README.md#batching)
- [Go](/src/clients/go/README.md#batching)
- [Java](/src/clients/java/README.md#batching)
- [.NET](/src/clients/dotnet/README.md#batching)

## Example API Layer Architecture

When building an application on TigerBeetle, you may want to use an intermediate API layer to batch
events.

This is primarily applicable if the number of services that need to query TigerBeetle:

- [exceed `config.clients_max`](./client-sessions.md#eviction), or
- require additional [batching](#batching-events) to optimize throughput.

Rather than each service connecting to TigerBeetle directly, you can set up your application
services to forward their requests to a pool of intermediate services, as illustrated below. This
API layer can coalesce events from many application services into requests, and forward back the
respective replies.

(Note that TigerBeetle does not currently provide such an intermediate service layer or a client to
pool or connect to them.)

One downside of this approach is that events submitted by the application may be applied out of
order. Without this intermediary API layer, TigerBeetle clients ensure that operations are applied
in the order they are submitted. However, if operations are submitted to two different API layer
instances, the operations may reach the TigerBeetle cluster in a different order, or one of the API
instances could crash and restart mid-request.


```mermaid
flowchart LR
    App1[Application service 1]
    App2[Application service 2]
    App3[Application service 3]
    App4[Application service 4]
    Cluster[TigerBeetle cluster]

    App1 <--> API1
    App2 <--> API1
    App3 <--> API2
    App4 <--> API2

    subgraph API
        API1{API 1}
        API2{API 2}
    end

    API1 <--> Cluster
    API2 <--> Cluster
```

### Queues and Workers

If you are making requests to TigerBeetle from workers pulling jobs from a queue, you can batch
requests to TigerBeetle by having the worker act on multiple jobs from the queue at once rather than
one at a time. i.e. pulling multiple jobs from the queue rather than just one.
