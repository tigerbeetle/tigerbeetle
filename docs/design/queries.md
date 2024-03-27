# Design: Queries

## Uses

Account statements (https://github.com/tigerbeetledb/tigerbeetle/issues/357):

``` sql
select *
from account_historical
where account_historical.id == ?1
and transfer.timestamp < ?2
order by transfer.timestamp desc
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

Get the balance of an account as of some time T.

``` sql
select account_history.balance
from account_history
where account_history.id = ?1
and account_history.timestamp < ?2
order by account_history.timestamp desc
limit 1
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

For various important use cases we need to support range queries.
We can't compute the intersection of range queries on two different indexes in bounded space.
That leaves us with queries of type B above - a range query on one index to get timestamps, many point lookups on timestamp->object, and then arbitrary filters on object.

We can't sort the output of an arbitrary range query in bounded space.
That leaves us with returning results in the same order as the index we use for the range query.
Since the choice of index affects the sort order of the results, it must be part of the interface.

## Proposal: Queries

A query is against a single grove, either Transfer or Account (or AccountHistory, if we add that grove).

A Transfer/Account query consists of:

* For each field of Transfer/Account, a closed interval of acceptable values. (The default interval in the client api should be the entire range of legal values ie no restriction).
* A choice of one field whose index we will run the query against. The results will be sorted first by this field and then by timestamp.
* An order for the index range query, one of ascending/descending.
* A limit on the number of transfers/accounts to return. (The default limit should be the maximum number of transfers/accounts that can fit in a single response).
* A limit on the number of transfers/accounts to inspect before returning. (This prevents very sparse conditions from blocking the pipeline for unbounded time).

To execute a query:

* Set result_count=0 and inspect_count=0.
* Start a range iterator on the chosen index using the range specified for that field and the order specified.
* For each field/timestamp pair:
  * If the index chosen was not timestamp->object, lookup the associated transfer/account in the timestamp->object index.
  * If the transfer/account meets all the other range conditions, add that transfer/account to the response and increment result_count.
  * Increment inspect_count.
  * See the response.page field to field/timestamp.
  * If result_count or inspect_count have reached their limit, return the response.

The response contains all the transfers/accounts found, and also the last field/timestamp pair that was inspected. 
This allows continuing the query by sending a new query operation with the lower end of the query range for the index field set to field/(timestamp+1).

For queries where the index range is [field/0, field/some_timestamp_in_the_past], the results cannot change over time and so the pagination above is consistent.
This is sufficient for account statements (if we add an account_historical tree) and change data export - the two most critical use cases.
For other queries, to get consistent pagination we will need to support snapshots.

Queries do not need to be replicated since they have no effect on the database.

Of the usecases above, this design does not support:

* 'Account statements' without an account_historical tree. Answering this query using the transfer tree requires intersecting multiple sparse range queries.
* 'Get all pending transfers for an account'. This requires a join, or we could maintain a `transfer_mutable` tree to track this state.
* 'Get accounts/transfer with id/user_data matching a given prefix in the last month.' We can execute this, but inefficiently because it intersects two sparse range queries.
* 'Get the balance of an account as of some time T.' We can execute this, but inefficiently because it intersects two sparse range queries.

__Q:__ Do we want queries to always be executed against a node that believes it is the leader?

__Q:__ Do successive queries need to be executed against the same node to ensure consistency?

__Q:__ Is it ok to just return transfer/account ids and require the client to issue lookups for those, or should we return the whole transfer/account?

__Q:__ How should we express conditions on nested fields like transfer_flags? Maybe treat them as if they weren't nested, like 'flags_post_pending_transfer"?

## Proposal: Snapshots

TODO

* Snapshots affect grid layout because they prevent us from overwriting old blocks. This means that acquiring/releasing snapshots needs to be ordered in the replicated log, even though queries don't need to be.
* How will snapshots be released? It should not be possible for errors in the client to allow an old snapshot to be persisted for a long time.
  * On client disconnect/crash, release all snapshots for that client.
  * In the client api, limit the number of snapshots (to 1?) so that old snapshots must be released before starting new queries.
  * Place a time limit on snapshots, after which querying the snapshot returns an error?

## Testing

TODO
