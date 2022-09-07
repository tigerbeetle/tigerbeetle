# History

## The Problem - Realtime Processing of Balance Updates

Processing events that impact the balance of an account must be done serially, in the correct order and reliably. For this reason, despite the trend to scale modern applications by distributing work across parallel threads/processes/machines, it remains difficult to scale a ledger or accounting system without sacrificing performance or safety.

For example, processing a balance update event involves:

1. validating the event,
2. processing any business rules that must consider the current balance before processing the event and the new balance after the event,
3. updating the balance
4. persisting the updated balance, and
5. notifying subscribers of the updated balance and that the event has been processed.

While investigating a few existing systems it became clear that the majority of architectures cobble together generic databases (relational or NoSQL, on-disk or in-memory) with business logic enforced in the application code. This separation between data and code, persistence and logic, invites the worst of distributed system problems: network delays, multiple roundtrips for a single balance update, clock skew, or cache incoherency etc.

Furthermore, these systems may achieve performance but at the expense of reliability, especially in the face of hardware failure, corruption and misdirected writes, a compromise that is unacceptable for a system-of-record for financial accounts.

## The Solution - A Purpose-Built Financial Accounting Database

Our survey led us to conclude that, while there are mechanisms available to shard account balances to parallelize updates and improve performance, there are still significant performance gains to be had in designing a database that is purpose built for storing balances and processing updates in a reliable manner.

## ProtoBeetle - 400,000 Transfers per Second

In the month of July 2020, we developed a prototype of TigerBeetle in Node as a performance sketch to measure the basic components of the design (batching, TCP protocol, cryptographic checksums everywhere, fsync journalling, in-memory business logic and hash table operations). **ProtoBeetle ran at 200,000 two-phase transfers per second on our office laptops**, supporting our back-of-the-envelope numbers.

We then integrated ProtoBeetle into [Mojaloop](https://mojaloop.io/) and our reference minimum deployment cluster of **Mojaloop went from 76 TPS on MySQL to 1757 TPS on ProtoBeetle**. A single stateless Mojaloop pod was unable to saturate ProtoBeetle. Most of the throughput was spent converting Mojaloop's individual HTTP requests into TCP batches.

**[Watch a 10-minute talk introducing ProtoBeetle.](https://youtu.be/QOC6PHFPtAM?t=324)**

## AlphaBeetle - 800,000 Transfers per Second

After ProtoBeetle, from September through October 2020, we knuckled down and rewrote TigerBeetle in C/Zig to create the alpha version of TigerBeetle, using [io_uring](https://kernel.dk/io_uring.pdf) as a foundation for fast I/O.

TigerBeetle's Zig implementation of io_uring was [submitted](https://github.com/ziglang/zig/pull/6356) for addition to the Zig standard library.

**[Watch a presentation of TigerBeetle given to the Interledger community on 25 November 2020.](https://www.youtube.com/watch?v=J1OaBRTV2vs)**

## BetaBeetle - High Availability

BetaBeetle, the beta distributed version of TigerBeetle, was developed from January 2021 through August 2021, for strict serializability, fault tolerance and automated leader election with the pioneering [Viewstamped Replication](http://pmg.csail.mit.edu/papers/vr-revisited.pdf) and consensus protocol, plus the CTRL protocol from [Protocol-Aware Recovery for Consensus-Based Storage](https://www.youtube.com/watch?v=fDY6Wi0GcPs).
