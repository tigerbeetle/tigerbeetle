# About TigerBeetle

## Mission

**We want to make it easy for others to build the next generation of financial services and
applications without having to cobble together an accounting or ledger system of record from
scratch.**

TigerBeetle implements the latest research and technology to deliver unprecedented [safety,
durability](./safety.md) and [performance](./performance.md) while reducing operational costs by
orders of magnitude and providing a fantastic developer experience.

## What Is A Tiger Beetle?

Two things got us interested in tiger beetles as a species:

1. Tiger beetles are ridiculously fast... a tiger beetle can run at a speed of 9 km/h, about 125
   body lengths per second. That’s 20 times faster than an Olympic sprinter when you scale speed to
   body length, **a fantastic speed-to-size ratio**. To put this in perspective, a human would need
   to run at 480 miles per hour to keep up.

2. Tiger beetles thrive in different environments, from trees and woodland paths, to sea and lake
   shores, with the largest of tiger beetles living primarily in the dry regions of Southern
   Africa... and that's what we want for TigerBeetle, **something that's fast and safe to deploy
   everywhere**.

## History

### The Problem - Realtime Processing of Balance Updates

Processing events that impact the balance of an account must be done serially, in the correct order
and reliably. For this reason, despite the trend to scale modern applications by distributing work
across parallel threads/processes/machines, it remains difficult to scale a ledger or accounting
system without sacrificing performance or safety.

For example, processing a balance update event involves:

1. validating the event,
2. processing any business rules that must consider the current balance before processing the event
   and the new balance after the event,
3. updating the balance
4. persisting the updated balance, and
5. notifying subscribers of the updated balance and that the event has been processed.

While investigating a few existing systems it became clear that the majority of architectures cobble
together generic databases (relational or NoSQL, on-disk or in-memory) with business logic enforced
in the application code. This separation between data and code, persistence and logic, invites the
worst of distributed system problems: network delays, multiple roundtrips for a single balance
update, clock skew, or cache incoherency etc.

Furthermore, these systems may achieve performance but at the expense of reliability, especially in
the face of hardware failure, corruption and misdirected writes, a compromise that is unacceptable
for a system-of-record for financial accounts.

### The Solution - A Purpose-Built Financial Accounting Database

Our survey led us to conclude that, while there are mechanisms available to shard account balances
to parallelize updates and improve performance, there are still significant performance gains to be
had in designing a database that is purpose built for storing balances and processing updates in a
reliable manner.

### ProtoBeetle - 400,000 Transfers per Second

In the month of July 2020, we developed a prototype of TigerBeetle in Node as a performance sketch
to measure the basic components of the design (batching, TCP protocol, cryptographic checksums
everywhere, fsync journalling, in-memory business logic and hash table operations). **ProtoBeetle
ran at 200,000 two-phase transfers per second on our office laptops**, supporting our
back-of-the-envelope numbers.

We then integrated ProtoBeetle into [Mojaloop](https://mojaloop.io/) and our reference minimum
deployment cluster of **Mojaloop went from 76 TPS on MySQL to 1757 TPS on ProtoBeetle**. A single
stateless Mojaloop pod was unable to saturate ProtoBeetle. Most of the throughput was spent
converting Mojaloop's individual HTTP requests into TCP batches.

**[Watch a 10-minute talk introducing ProtoBeetle.](https://youtu.be/QOC6PHFPtAM?t=324)**

### AlphaBeetle - 800,000 Transfers per Second

After ProtoBeetle, from September through October 2020, we knuckled down and rewrote TigerBeetle in
C/Zig to create the alpha version of TigerBeetle, using [io_uring](https://kernel.dk/io_uring.pdf)
as a foundation for fast I/O.

TigerBeetle's Zig implementation of io_uring was
[submitted](https://github.com/ziglang/zig/pull/6356) for addition to the Zig standard library.

**[Watch a presentation of TigerBeetle given to the Interledger community on 25 November 2020.](https://www.youtube.com/watch?v=J1OaBRTV2vs)**

### BetaBeetle - High Availability

BetaBeetle, the beta distributed version of TigerBeetle, was developed from January 2021 through
August 2021, for strict serializability, fault tolerance and automated leader election with the
pioneering [Viewstamped Replication](http://pmg.csail.mit.edu/papers/vr-revisited.pdf) and consensus
protocol, plus the CTRL protocol from [Protocol-Aware Recovery for Consensus-Based
Storage](https://www.youtube.com/watch?v=fDY6Wi0GcPs).

### TigerBeetle - Into Production

TigerBeetle is [production-ready](./production-ready.md). 

## References

The collection of papers behind TigerBeetle:

- [LMAX - How to Do 100K TPS at Less than 1ms Latency -
  2010](https://www.infoq.com/presentations/LMAX/) - Martin Thompson on mechanical sympathy and why
  a relational database is not the right solution.

- [The LMAX Exchange Architecture - High Throughput, Low Latency and Plain Old Java -
  2014](https://skillsmatter.com/skillscasts/5247-the-lmax-exchange-architecture-high-throughput-low-latency-and-plain-old-java)

  - Sam Adams on the high-level design of LMAX.

- [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf) - A high
  performance alternative to bounded queues for exchanging data between concurrent threads.

- [Evolution of Financial Exchange Architectures -
  2020](https://www.youtube.com/watch?v=qDhTjE0XmkE) - Martin Thompson looks at the evolution of
  financial exchanges and explores the state of the art today.

- [Gray Failure](https://www.microsoft.com/en-us/research/wp-content/uploads/2017/06/paper-1.pdf) -
  "The major availability breakdowns and performance anomalies we see in cloud environments tend to
  be caused by subtle underlying faults, i.e. gray failure rather than fail-stop failure."

- [The Tail at Store: A Revelation from Millions of Hours of Disk and SSD
  Deployments](https://www.usenix.org/system/files/conference/fast16/fast16-papers-hao.pdf) - "We
  find that storage performance instability is not uncommon: 0.2% of the time, a disk is more than
  2x slower than its peer drives in the same RAID group (and 0.6% for SSD). As a consequence, disk
  and SSD-based RAIDs experience at least one slow drive (i.e., storage tail) 1.5% and 2.2% of the
  time."

- [The Tail at
  Scale](https://www2.cs.duke.edu/courses/cps296.4/fall13/838-CloudPapers/dean_longtail.pdf) - "A
  simple way to curb latency variability is to issue the same request to multiple replicas and use
  the results from whichever replica responds first."

- [Viewstamped Replication Revisited](http://pmg.csail.mit.edu/papers/vr-revisited.pdf)

- [Viewstamped Replication: A New Primary Copy Method to Support Highly-Available Distributed
  Systems](http://pmg.csail.mit.edu/papers/vr.pdf)

- [ZFS: The Last Word in File Systems (Jeff Bonwick and Bill
  Moore)](https://www.youtube.com/watch?v=NRoUC9P1PmA) - On disk failure and corruption, the need
  for checksums... and checksums to check the checksums, and the power of copy-on-write for
  crash-safety.

- [An Analysis of Latent Sector Errors in Disk
  Drives](https://research.cs.wisc.edu/wind/Publications/latent-sigmetrics07.pdf)

- [An Analysis of Data Corruption in the Storage
  Stack](https://www.usenix.org/legacy/events/fast08/tech/full_papers/bairavasundaram/bairavasundaram.pdf)

- [A Study of SSD Reliability in Large Scale Enterprise Storage
  Deployments](https://www.usenix.org/system/files/fast20-maneas.pdf)

- [SDC 2018 - Protocol-Aware Recovery for Consensus-Based
  Storage](https://www.youtube.com/watch?v=fDY6Wi0GcPs) - Why replicated state machines need to
  distinguish between a crash and corruption, and why it would be disastrous to truncate the journal
  when encountering a checksum mismatch.

- [Can Applications Recover from fsync
  Failures?](https://www.usenix.org/system/files/atc20-rebello.pdf) - Why we use Direct I/O in
  TigerBeetle and why the kernel page cache is a dangerous way to recover the journal, even when
  restarting from an fsync() failure panic.

- [Coil's Mojaloop Performance Work
  2020](https://docs.mojaloop.io/legacy/discussions/Mojaloop%20Performance%202020.pdf) - By Don
  Changfoot and Joran Dirk Greef, a performance analysis of Mojaloop's central ledger that sparked
  the idea for "an accounting database" as Adrian Hope-Bailie put it. And the rest, as they say, is
  history!
