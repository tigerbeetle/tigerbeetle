# Performance

How, exactly, is TigerBeetle so fast?

## It's All About The Interface

The key is that TigerBeetle designed specifically for [OLTP](./oltp.md) workloads, as opposed to
OLGP. The prevailing paradigm for OLGP is interactive transactions, where business-logic lives in
the application, and the job of the database is to send the data to the application, holding the
locks while the data is being processed. This works for mixed read-write workload with low
contention, but fails for highly-contended OLTP workloads --- locks over the network are very
expensive!

With TigerBeetle, **all the logic lives inside the database**, obviating the need for locking. Not
only is this very fast, it is also more convenient --- the application can speak
[Debit/Credit](./debit-credit.md) directly, it doesn't need to translate the language of business to
SQL.

## Batching, Batching, Batching

On a busy day in a busy city, taking the subway is faster than using a car. On empty streets, a personal
sports car gives you the best latency, but when the load and contention increase, due to
[**Little's law**](https://en.wikipedia.org/wiki/Little%27s_law), both latency and throughput become abysmal.

TigerBeetle works like a high-speed train --- its interface always deals with _batches_ of
transactions, 8k apiece. Although TigerBeetle is a replicated database using a consensus algorithm,
the cost of replication is paid only once per batch, which means that TigerBeetle runs almost as
fast as an in-memory hash map, all the while providing extreme durability and availability.

What's more, under light load, the batches automatically become smaller, trading unnecessary
throughput for better latency.

## Extreme Engineering

Debit/Credit fixes inefficiency in the interface, pervasive batching amortizes costs, but, to really
hit performance targets, solid engineering is required at every level of the stack.

TigerBeetle is built fully from scratch, without using any dependencies, to make sure that all the
layers are co-designed for OLTP.

TigerBeetle is written in Zig --- a systems programming language which doesn't use garbage
collection and is designed for writing fast code.

Every data structure is hand-crafted with the CPU in mind: a transfer object is 128 bytes in size,
cache-line aligned. Executing a batch of transfers is just one tight CPU loop!

TigerBeetle allocates all the memory statically: it never runs out of memory, it never stalls due to
a GC pause or mutex contention, and it never fragments the memory.

TigerBeetle is designed for io_uring --- a new Linux kernel interface for zero syscall networking
and storage I/O.

These and other performance rules are captured in
[TIGER_STYLE.md](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md) --- the
secret recipe that keeps TigerBeetle fast and safe.

## Single Threaded By Design

TigerBeetle uses a single core by design and uses a single leader node to process events. Adding
more nodes can therefore increase reliability, but not throughput.

For a high-performance database, this may seem like an unusual choice. However, sharding in
financial databases is notoriously difficult, and contention issues often negate the would-be
benefits. Specifically, a small number of hot accounts are often involved in a large proportion of
the transactions, so the shards responsible for those accounts become bottlenecks.

For more details on when single-threaded implementations of algorithms outperform multi-threaded
implementations, see ["Scalability! But at what
COST?](https://www.usenix.org/system/files/conference/hotos15/hotos15-paper-mcsherry.pdf).

## Performance = Flexibility

Is it _really_ necessary to go to such great lengths in the name of performance? It of course
depends on a particular use-case, but it's worth keeping in mind that higher performance can unlock
new use-cases. An OLGP database might be enough to do nightly settlement, but with OLTP real-time
settlement is a no-brainer. If a transaction system just hits its throughput target, 
every unexpected delay or ops accident will lead to missed transactions. If a system operates at one
tenth of capacity, this gives a lot of headroom for operators to deal with the unexpected.

Finally, it is always helpful to think about the future. The future is hard to predict (even the
_present_ is hard to wrap your head around!), but the _option_ to handle significantly more load on
a short notice greatly expands your options.

## Next: Safety

Performance can get you very far very fast, but it is useless if the result is wrong. Business
transaction processing also requires **strong safety guarantees**, to ensure that data cannot be
lost, and **high availability** to ensure that money is not lost due to database downtime. Let's
look at how TigerBeetle ensures [safety](./safety.md).
