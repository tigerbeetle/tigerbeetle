# Online Transaction Processing (OLTP)

Online Transaction Processing (OLTP) is about **recording business transactions in real-time**. This
could be payments, sales, car sharing rides, game scores, or API usage.

## The World is Becoming More Transactional

Historically, general purpose databases like PostgreSQL, MySQL, and SQLite handled OLTP. We refer
to these as Online General Purpose (OLGP) databases.

OLTP workloads have increased by 3-4 orders of magnitude in the last 10 years alone. For example:

- The [UPI](https://en.wikipedia.org/wiki/Unified_Payments_Interface)
  real-time payments switch in India processed 10 billion payments in the year 2019.
  In January 2025 alone, it processed [16.9 billion payments.](https://www.npci.org.in/what-we-do/upi/product-statistics)
- Cleaner energy and smart metering means energy is being traded by the kilowatt-hour.
  Customer billing is every 15 or 30 minutes rather than at the end of the month.
- Serverless APIs charge for usage by the second or per-request, rather than per month. (Today,
  serverless billing at scale is often implemented using [MapReduce](https://en.wikipedia.org/wiki/MapReduce).
  This makes it difficult or impossible to offer customers real-time spending caps.)

OLGP databases already struggle to keep up.

**But TigerBeetle is built to handle the scale of OLTP workloads today and for the decades to come.**
It works well alongside OLGP databases,  which hold infrequently updated data.
TigerBeetle can race ahead, giving your system unparalleled latency and throughput.

## Write-Heavy Workloads

A distinguishing characteristic of OLTP is its focus on _recording_ business transactions. In
contrast, OLGP databases are often designed for read-heavy or balanced workloads.

TigerBeetle is optimized from the ground up for write-heavy workloads. This means it can handle the
increasing scale of OLTP, unlike an OLGP database.

## High Contention on Hot Accounts

Business transactions always involve more than one account. One account gets paid but then there are
fees, taxes, revenue splits, and other costs to account for.

OLTP systems often have accounts involved in a high percentage of all transactions. This is
especially true for accounts that represent the business income or expenses. Locks can be used to
ensure that updates to these 'hot accounts' are consistent. But the resulting contention can bring
the system's performance to a crawl.

TigerBeetle provides strong consistency guarantees without row locks. This sidesteps the issue of
contention on hot accounts. Due to TigerBeetle's use of the system cache, transactions processing
speed even _increases_.

## Business Transactions Don't Shard Well

One of the most common ways to scale systems is to horizontally scale or shard them. This means
different servers process different sets of transactions. Unfortunately, business transactions don't
shard well. Horizontal scaling is a poor fit for OLTP:

- Most accounts cannot be neatly partitioned between shards.
- Transactions between accounts on different shards become more complex and slow.
- Row locks on hot accounts worsen when the transactions must execute across shards.

Another approach to scaling OLTP systems is to use MapReduce for billing. But this makes it hard to
provide real-time balance reporting or spending limits. It also creates a poor user experience
that's hard to fix post system design.

TigerBeetle uses a [single-core design](./performance.md#single-threaded-by-design )
and unique performance optimizations to deliver high throughput. And this without the downsides of
horizontal scaling.

## Bottleneck for Your System

You can only do as much business as your database supports. You need a core OLTP database capable of
handling your transactions on your busiest days. And for decades to come.

TigerBeetle is designed to handle **1 million transactions per second**, to remove the risk of your
business outgrowing your database.

## Next Up: Debit / Credit is the Schema for OLTP

The world is becoming more transactional. OLTP workloads are increasing and we need a database
designed from the ground up to handle them. 
What is the perfect schema and language for this database?
[Debit / Credit](./debit-credit.md).
