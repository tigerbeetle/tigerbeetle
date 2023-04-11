# Design: Queries

## Uses

Account statements (optionally filtered by code, user_data, or range of amounts):

``` sql
select *
from transfer
where (transfer.debit_account_id = ?1 or transfer.credit_account_id = ?1)
and transfer.timestamp < ?2
order by transfer.timestamp desc
limit 100
```

We may also want to include historical balances (https://github.com/tigerbeetledb/tigerbeetle/issues/357):

```
select account_history.*, account_history.transfer.*
from account_history
and (account_history.debit_account_id = ?1 or account_history.credit_account_id = ?1)
and account_history.timestamp < ?2
order by account_history.timestamp desc
limit 100
```

Exporting change data:

``` sql
select * 
from transfer
where transfer.timestamp > ?2
order by transfer.timestamp asc
limit 100
```

``` sql
select * 
from account
where account.timestamp > ?2
order by account.timestamp asc
limit 100
```

Get all pending transfers for an account (requires a transfer_mutable table?):

``` sql
select transfer.*
from transfer
where transfer.flags.post_pending_transfer = true
where not exists (
  select transfer2.*
  from transfer as transfer2
  where transfer2.pending_id = transfer.id
)
```

Get the pending transfer for a transfer, or vice-versa:

``` sql
select transfer.*
from transfer
where transfer.pending_id = ?
```

``` sql
select transfer.pending_id
from transfer
where transfer.id = ?
```

Get accounts/transfer with id/user_data matching a given prefix.

``` sql
select transfer.*
from transfer
where 1000 <= transfer.user_data <= 1999
```

Get accounts/transfer with id/user_data matching a given prefix in the last month.

``` sql
select transfer.*
from transfer
where 1000 <= transfer.user_data <= 1999
and timestamp("2023-Apr-01") <= transfer.timestamp < timestamp("2023-Mar-01")
```

TODO other uses.

## Goals

* Simple semantics.
* Predictable performance.
* PoC by June release.

## Constraints

Constraints inherited from our current architecture:

* Clients can only have one message in flight.
* Messages have a fixed size.
* Each message is executed sequentially => the execution time must be bounded.
* No dynamic memory allocation => the memory usage must be bounded.
* Execution must be deterministic.

The constraints above aren't cast in stone, but departing from them would require substantial changes to the architecture.

## Bounds

To satisfy bounds on time and memory, queries return after *either*:
* They produced enough results to fill a message.
* They exceeded some bound on execution time.
* They exceeded some bound on memory usage.

Execution must be deterministic => execution time bounds must be deterministic => we can't use wall-clock time. Alternatives:
* Limit the number of read calls.
* Limit the number of next calls on query plan operators.

## Pagination

If a single message worth of results is not enough, we need a way to ask for more results. Options:
* Return an opaque pagination token in the results. This limits us to kinds of queries for which we can automatically paginate using a bounded-size token.
* Make the user do their own pagination (eg by setting `transfer.timestamp < ?2` to the min timestamp from the last query). This may result in some queries not being possible to paginate.

Either way, for correct pagination we need to allow specifying the snapshot to query against. 
The replica needs to keep that snapshot alive until the whole query is finished, so we need a way to reserve/lock snapshots and also a way to ensure that they aren't leaked.
We could maybe store them in the client table and free the snapshot if the client is evicted.

## Query operators

Many traditional query operators (eg hashjoin) use an unbounded amount of memory before returning any results. 
We can't do that, because we would not be able to guarantee that we make any forward progress (ie the query might always exceed memory usage bounds before returning any results).
So we are limited to query operators that use bounded memory eg:
* Joining an existing index against an iterator which has the same sort order (sequential reads).
* Joining an existing index against an iterator which has a different sort order (random reads, approx `values_per_data_block` times slower than sequential).
* Grouping/aggregating an iterator by a prefix of it's sort keys (eg sorted by `a,b` and grouped by `a`).

We currently have indexes on timestamp->object and object.field->timestamp. 

## Index selection

The execution time of a query can vary dramatically depending on which indexes we use and in which order.

OLTP databases typically use gathered approximate statistics to make this decision on every query execution.
Concerns:
* The selection is always capable of error (eg https://www.vldb.org/pvldb/vol9/p204-leis.pdf). This makes query performance less predictable - a query might switch from a fast path to a slow path in production (eg consider the complexity of the plan space in https://youtu.be/RQfJkNqmHB4?t=4601).
* Choosing the optimal order between multiple indexes is computationally expensive (roughly O(n!), eg postgres has a fallback to genetic optimization when n is large). The actual runtime can depend on the current statistics. This might hurt latency in general, and also might be a source of unpredictable latency spikes.

Our constraints on pagination and query operators mean that only certain query plans are viable. Eg for `select * from transfers where 100 <= transfer.ledger < 200 order by transfer.timestamp` we can only satisfy `100 <= transfer.ledger < 200` efficiently with a range query on the ledger->timestamp index, but we can only paginate in timestamp order with a range query on the timestamp->object index. We have no efficient query plan for this query!

Here are some kinds of queries we *can* execute.

A:
* Point query on field->timestamp.
* Sequential lookup on timestamp->object.
* Abitrary filters on object.
* Results are in timestamp order.

B:
* Range query on field->timestamp.
* Random(ish) lookup on timestamp->object.
* Abitrary filters on object.
* Results are in field,timestamp order.

## Questions

Do queries have to be replicated, or can they just be executed by the replica that received them?
The query execution itself doesn't need to be replicated, but reserving a snapshot affects grid layout (because we can't overwrite those blocks yet).

Is it ok to just return transfer/account ids and require the client to issue lookups for those?
Or will we need to be able to return some computed values (eg `sum(transfer.amount) grouped by ...`)?

How should we express conditions on nested fields like transfer_flags? Maybe treat them as if they weren't nested, like 'flags_post_pending_transfer".

## Testing

TODO