# Best Practices

The following is a list of the best practices to ensure your solution is making the most of
TigerBeetle's Safety and Performance guarantees.

## Batching

[Batching is the key to TigerBeetle's performance](../concepts/performance.md#batching-batching-batching)
at every layer of the stack, and your application is no exception! When designing your application,
look for opportunities to be passing through large batches of transfers instead of one at a time.

For example does your API have something like `createPayment()` or `createPayments()`?

## 1000:1 Write:Read Ratio

TigerBeetle is heavily optimized for writes. In order to keep the hot path of transaction processing
fast, we recommend your application does on the order of 1000 writes for every read.

Another trick is to make sure that when you do reads, you are getting back many results in one read.
E.g. when you do `lookup_transfers`, are you getting just a few transfers back at a time, or a full
batch of transfers?

For analytical workloads, we recommend using TigerBeetle's Change Data Capture (CDC) with
[AMQP](../operating/cdc.md) or
[Redpanda Connect](https://docs.redpanda.com/redpanda-connect/components/inputs/tigerbeetle_cdc/).
CDC streams your data out of TigerBeetle into external systems which allows for reads that won't
affect the hot path of your application.

## Ordered Transfer Ids

Transfer ids should be ordered for best performance, such as with a time-based id scheme. Internally
this allows TigerBeetle to take advantage of LSM optimizations. Non-ordered ids such as UUID v4 make
operations such as the idempotency check much less efficient, degrading performance as the number of
transfers grows.

[We've implemented our own TigerBeetle ID](./data-modeling.md#tigerbeetle-time-based-identifiers-recommended)
which is available in each of our clients.

> *It's better to start from sortable ids from day 1 of using TigerBeetle, since we use min-max
pruning to optimize lookups in the LSM tree. If ids are truly random over 128 bits, the
ids could end up 'poisoning' the lookup check for all future transfers.*

## Clients: Less is More

TigerBeetle supports a maximum of 64 clients, but we typically recommend users to use between 3-8
clients.

With fewer clients, the TigerBeetle client is able to
[automatically batch events](./requests.md#batching-events). As you increase the number of clients,
the less effective this automatic batching becomes.

## Write Last, Read First

TigerBeetle is the system of record for financial transactions in your stack. It records which
transfers took place and who has what balance. In order to interpret that information, we store
transaction metadata in a general purpose database (OLGP).

_But how do we make sure that these two systems stay consistent with one another?_

We have a simple rule when writing to TigerBeetle: **Write Last, Read First**. Write data
dependencies to OLGP first, then **write last** to TigerBeetle. Then, when reading data back,
**read first** from TigerBeetle, then from OLGP.

By following this principle, if the OLGP fails on the write path, nothing gets written. But it's
safe to retry writes write to TigerBeetle until you get a response.

On the read path, if something exists in TigerBeetle, then _we know it happened_ since TigerBeetle
is the system of record. Since we know that the account or transfer exists in TigerBeetle, we can
also know that the record exists in our OLGP.

For more information on this principle, check out our 
[eponymous blog post](https://tigerbeetle.com/blog/2025-11-06-the-write-last-read-first-rule/).
