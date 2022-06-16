# Design Document

This is a living document that keeps a (best effort) record of the design decisions behind TigerBeetle, a distributed financial accounting database. TigerBeetle is under active development. These design components are in various stages of completion and iteration cycle.

## Mission

**We want to make it easy for others to build the next generation of financial services and applications without having to cobble together an accounting or ledger system of record from scratch.**

TigerBeetle implements the latest research and technology to deliver unprecedented safety, durability and performance while reducing operational cost by orders of magnitude and providing a fantastic developer experience.

## Safety

TigerBeetle is designed to a higher safety standard than a general-purpose relational database such as MySQL or an in-memory database such as Redis:

* Strict consistency, CRCs and crash safety are not enough.

* TigerBeetle **detects and repairs disk corruption** ([3.45% per 32 months, per disk](https://research.cs.wisc.edu/wind/Publications/latent-sigmetrics07.pdf)), **detects and repairs misdirected writes** where the disk firmware writes to the wrong sector ([0.042% per 17 months, per disk](https://research.cs.wisc.edu/wind/Publications/latent-sigmetrics07.pdf)), and **prevents data tampering** with hash-chained cryptographic checksums.

* TigerBeetle **uses Direct I/O by design** to side step cache coherency bugs in the kernel page cache after an EIO fsync error.

* TigerBeetle **exceeds the fsync durability of a single disk** and the hardware of a single server because disk firmware can contain bugs and because single server systems fail.

* TigerBeetle **provides strict serializability**, the gold standard of consistency, as a replicated state machine, and as a cluster of TigerBeetle servers (called replicas), for optimal high availability and distributed fault-tolerance.

* TigerBeetle **performs synchronous replication** to a quorum of TigerBeetle servers using the pioneering [Viewstamped Replication](http://pmg.csail.mit.edu/papers/vr-revisited.pdf) and consensus protocol, for low-latency automated leader election and to eliminate the risk of split brain associated with manual failover.

* TigerBeetle is “fault-aware” and **recovers from local storage failures in the context of the global consensus protocol**, providing [more safety than replicated state machines such as ZooKeeper and LogCabin](https://www.youtube.com/watch?v=fDY6Wi0GcPs). For example, TigerBeetle can disentangle corruption in the middle of the committed journal (caused by bitrot) from torn writes at the end of the journal (caused by power failure) to uphold durability guarantees given for committed data and maximize availability.

* TigerBeetle does not depend on synchronized system clocks, does not use leader leases, and **performs leader-based timestamping** so that your application can deal only with safe relative quantities of time with respect to transfer timeouts. To ensure that the leader's clock is within safe bounds of "true time", TigerBeetle combines all the clocks in the cluster to create a fault-tolerant clock that we call ["cluster time"](https://www.tigerbeetle.com/post/three-clocks-are-better-than-one).

## Performance

TigerBeetle provides more performance than a general-purpose relational database such as MySQL or an in-memory database such as Redis:

* TigerBeetle **uses small, simple fixed-size data structures** (accounts and transfers) and a tightly scoped domain.

* TigerBeetle **performs all balance tracking logic in the database**. This is a paradigm shift where we move the code once to the data, not the data back and forth to the code in the critical path. This eliminates the need for complex caching logic outside the database. The “Accounting” business logic is built in to TigerBeetle so that you can **keep your application layer simple, and completely stateless**.

* TigerBeetle **supports batching by design**. You can batch all the transfer prepares or commits that you receive in a fixed 10ms window (or in a dynamic 1ms through 10ms window according to load) and then send them all in a single network request to the database. This enables low-overhead networking, large sequential disk write patterns and amortized fsync and consensus across hundreds and thousands of transfers.

> Everything is a batch. It's your choice whether a batch contains 100 transfers or 10,000 transfers but our measurements show that **latency is _less_ where batch sizes are larger, thanks to Little's Law** (e.g. 50ms for a batch of a hundred transfers vs 20ms for a batch of ten thousand transfers). TigerBeetle is able to amortize the cost of I/O to achieve lower latency, even for fairly large batch sizes, by eliminating the cost of queueing delay incurred by small batches.

* If your system is not under load, TigerBeetle also **optimizes the latency of small batches**. After copying from the kernel's TCP receive buffer (TigerBeetle does not do user-space TCP), TigerBeetle **does zero-copy Direct I/O** from network protocol to disk, and then to state machine and back, to reduce memory pressure and L1-L3 cache pollution.

* TigerBeetle **uses io_uring for zero-syscall networking and storage I/O**. The cost of a syscall in terms of context switches adds up quickly for a few thousand transfers.

* TigerBeetle **does zero-deserialization** by using fixed-size data structures, that are optimized for cache line alignment to **minimize L1-L3 cache misses**.

* TigerBeetle **takes advantage of Heidi Howard's Flexible Quorums** to reduce the cost of **synchronous replication to one (or two) remote replicas at most** (in addition to the leader) with **asynchronous replication** between the remaining followers. This improves write availability, without sacrificing strict serializability or durability. This also reduces server deployment cost by as much as 20% because a 4-node cluster with Flexible Quorums can now provide the same `f=2` guarantee for the replication quorum as a 5-node cluster.

> ["The major availability breakdowns and performance anomalies we see in cloud environments tend to be caused by subtle underlying faults, i.e. gray failure (slowly failing hardware) rather than fail-stop failure."](https://www.microsoft.com/en-us/research/wp-content/uploads/2017/06/paper-1.pdf)

* TigerBeetle **routes around transient gray failure latency spikes**. For example, if a disk write that typically takes 4ms starts taking 4 seconds because the disk is slowly failing, TigerBeetle will use cluster redundancy to mask the gray failure automatically without the user seeing any 4 second latency spike. This is a relatively new performance technique in the literature known as "tail tolerance".

## Developer-Friendly

TigerBeetle does all ledger validation, balance tracking, persistence and replication for you, all you have to do is use the TigerBeetle client to:

1. send in a batch of prepares to TigerBeetle (in a single network request), and then
2. send in a batch of commits to TigerBeetle (in a single network request).

## Architecture

In theory, TigerBeetle is a replicated state machine, that **takes an initial starting state** (account opening balances), and **applies a set of input events** (transfers) in deterministic order, after first replicating these input events safely, to **arrive at a final state** (account closing balances).

In practice, TigerBeetle is based on the [LMAX Exchange Architecture](https://skillsmatter.com/skillscasts/5247-the-lmax-exchange-architecture-high-throughput-low-latency-and-plain-old-java) and makes a few improvements.

We take the same three classic LMAX steps:

1. journal incoming events safely to disk, and replicate to backup nodes, then
2. apply these events to in-memory state, then
3. ACK to the client

And then we introduce something new:

4. delete the local journalling step entirely, and
5. replace it with parallel replication to 3/5 distributed replicas.

Our architecture then becomes three easy steps:

1. replicate incoming events safely to a quorum of distributed replicas, then
2. apply these events to in-memory state, then
3. ACK to the client

That's how TigerBeetle **eliminates gray failure in the leader's local disk**, and how TigerBeetle **eliminates gray failure in the network links to the replication nodes**.

Like LMAX, TigerBeetle uses a thread-per-core design for optimal performance, with strict single-threading to enforce the single writer principle and to avoid the costs of multi-threaded coordinated access to data.

## Data Structures

The best way to understand TigerBeetle is through the data structures it provides. All data structures are **fixed-size** for performance and simplicity, and there are two main kinds of data structures, **events** and **states**.

### Events

Events are **immutable data structures** that **instantiate or mutate state data structures**:

* Events cannot be changed, not even by other events.
* Events cannot be derived, and must therefore be recorded before being executed.
* Events must be executed one after another in deterministic order to ensure replayability.
* Events may depend on past events (should they choose).
* Events cannot depend on future events.
* Events may depend on states being at an exact version (should they choose).
* Events may succeed or fail but the result of an event is never stored in the event, it is stored in the state instantiated or mutated by the event.
* Events only ever have one immutable version, which can be referenced directly by the event's id.
* Events should be retained for auditing purposes. However, events may be drained into a separate cold storage system, once their effect has been captured in a state snapshot, to compact the journal and improve startup times.

**create_transfer**: Create a transfer between accounts (maps to a "prepare"). We group fields in descending order of size to avoid unnecessary struct padding in C implementations.

```
          create_transfer {
                      id: 16 bytes (128-bit)
        debit_account_id: 16 bytes (128-bit)
       credit_account_id: 16 bytes (128-bit)
               user_data: 16 bytes (128-bit) [optional, e.g. opaque third-party identifier to link this transfer (many-to-one) to an external entity]
                reserved: 16 bytes (128-bit) [reserved, for accounting policy primitives]
              pending_id: 16 bytes (128-bit) [optional, required to post or void an existing but pending transfer]
                 timeout:  8 bytes ( 64-bit) [optional, required only for a pending transfer, a quantity of time, i.e. an offset in nanoseconds from timestamp]
                  ledger:  4 bytes ( 32-bit) [required, to enforce isolation by ensuring that all transfers are between accounts of the same ledger]
                    code:  2 bytes ( 16-bit) [required, an opaque chart of accounts code describing the reason for the transfer e.g. deposit, settlement]
                   flags:  2 bytes ( 16-bit) [optional, to modify the usage of the reserved field, and for future feature expansion]
                  amount:  8 bytes ( 64-bit) [required, an unsigned integer in the unit of value of the debit and credit accounts, which must be the same for both accounts]
               timestamp:  8 bytes ( 64-bit) [reserved, assigned by the leader before journalling]
} = 128 bytes (2 CPU cache lines)
```

**create_account**: Create an account.

* We use the terms `credit` and `debit` instead of "payable" or "receivable" since the meaning of a credit balance depends on whether the account is an asset or liability or equity, income or expense.
* A `posted` amount refers to an amount posted by a transfer.
* A `pending` amount refers to an inflight amount yet-to-be-posted by a two-phase transfer only, where the transfer is still pending, and where the transfer timeout has not yet fired. In other words, the transfer amount has been reserved in the pending account balance (to avoid double-spending) but not yet posted to the posted balance. The reserved amount will rollback if the transfer ultimately fails. By default, transfers post automatically, but being able to reserve the amount as pending and then post the amount only later can sometimes be convenient, for example when switching credit card payments.
* The debit balance of an account is given by adding `debits_posted` plus `debits_pending`. Likewise for the credit balance of an account.
* The total balance of an account can be derived by subtracting the total credit balance from the total debit balance.
* We keep both sides of the ledger (debit and credit) separate to avoid dealing with signed numbers, and to preserve more information about the nature of an account. For example, two accounts could have the same balance of 0, but one account could have 1,000,000 units on both sides of the ledger, whereas another account could have 1 unit on both sides, both balancing out to 0.
* Once created, an account may be changed only through transfer events, to keep an immutable paper trail for auditing.

```
           create_account {
                      id: 16 bytes (128-bit)
               user_data: 16 bytes (128-bit) [optional, opaque third-party identifier to link this account (many-to-one) to an external entity]
                reserved: 48 bytes (384-bit) [reserved for future accounting policy primitives]
                  ledger:  4 bytes ( 32-bit) [required, to enforce isolation by ensuring that all transfers are between accounts of the same ledger]
                    code:  2 bytes ( 16-bit) [required, an opaque chart of accounts code describing the reason for the transfer e.g. deposit, settlement]
                   flags:  4 bytes ( 16-bit) [optional, net balance limits: e.g. debits_must_not_exceed_credits or credits_must_not_exceed_debits]
          debits_pending:  8 bytes ( 64-bit)
           debits_posted:  8 bytes ( 64-bit)
         credits_pending:  8 bytes ( 64-bit)
          credits_posted:  8 bytes ( 64-bit)
               timestamp:  8 bytes ( 64-bit) [reserved]
} = 128 bytes (2 CPU cache lines)
```

### States

States are **data structures** that capture the results of events:

* States can always be derived by replaying all events.

TigerBeetle provides **exactly one state data structure**:

* **Account**: An account showing the effect of all transfers.

To simplify, to reduce memory copies and to reuse the wire format of event data structures as much as possible, we reuse our `create_account` event data structure to instantiate the corresponding state data structure.

## Fault Models

We adopt the following fault models with respect to storage, network, memory and processing:

### Storage Fault Model

* Disks experience data corruption with significant and material probability.

* Disk firmware or hardware may cause writes to be misdirected and written to the wrong sector, or not written at all, with low but nevertheless material probability.

* Disk firmware or hardware may cause reads to be misdirected and read from the wrong sector, or not read at all, with low but nevertheless material probability.

* Corruption does not always imply a system crash. Data may be corrupted at any time during its storage lifecycle: before being written, while being written, after being written, and while being read.

* Disk sector writes are not atomic. For example, an Advanced Format 4096 byte sector write to a disk with an emulated logical sector size of 4096 bytes but a physical sector size of 512 bytes is not atomic, and would be split into 8 physical sector writes, which may or may not be atomic. We do not depend on any sector atomicity guarantees from the disk.

* The Linux kernel page cache is not reliable and may misrepresent the state of data on disk after an EIO or latent sector error. See *[Can Applications Recover from fsync Failures?](https://www.usenix.org/system/files/atc20-rebello.pdf)* from the University of Wisconsin – Madison presented at the 2020 USENIX Annual Technical Conference.

* File system metadata (such as the size of a file) is not reliable and may change at any time.

* Disk performance and read and write latencies can at times be volatile, causing latency spikes on the order of seconds. A slow disk does not always indicate a failing disk, and a slow disk may return to median performance levels. For example, an SSD undergoing garbage collection.

### Network Fault Model

* Messages may be lost.

* Messages may be corrupted.

* Messages may be delayed.

* Messages may be replayed.

* TCP checksums are inadequate to prevent checksum collisions.

* Network performance may be asymmetrical for the upload and download paths.

### Memory Fault Model

* Memory is protected with error-correcting codes sufficient for our purpose. We make no further effort to protect against memory faults.

* Non-ECC memory is not supported by TigerBeetle.

### Processing Fault Model

* The system may crash at any time.

* The system may freeze process execution for minutes or hours at a time, for example during a VM migration.

* The system clock may jump backwards or forwards in time, at any time.

* NTP can help, but we cannot depend on NTP for strict serializability.

* NTP may stop working because of a network partition, which may not impact TigerBeetle. We therefore need to detect when a TigerBeetle cluster's clocks are not being synchronized by NTP, so that financial transaction timestamps are accurate and within the operator's tolerance for error.

## Timestamps

**All timestamps in TigerBeetle are strictly reserved** to prevent unexpected distributed system failures due to clock skew. For example, a payer could have all their transfers fail, simply because their clock was behind. This means that only the TigerBeetle leader may assign timestamps to data structures.

Timestamps are assigned at the moment a batch of events is received by the TigerBeetle leader from the application. Within a batch of events, each event timestamp must be **strictly increasing**, so that no two events will ever share the same timestamp and so that timestamps may **serve as sequence numbers**. Timestamps are therefore stored as 64-bit **nanoseconds**, not only for optimal resolution and compatibility with other systems, but to ensure that there are enough distinct timestamps when processing hundreds of events within the same millisecond window.

Should the user need to specify an expiration timestamp, this can be done in terms of **a relative quantity of time**, i.e. an offset relative to the event timestamp that will be assigned by the TigerBeetle leader, rather than in terms of **an absolute moment in time** that is set according to another system's clock.

For example, the user may say to the TigerBeetle leader: "expire this transfer 7 seconds after the timestamp you assign to it when you receive it from me".

The intention of this design decision is to minimize and restrict the blast radius of inaccurate timestamps in the worst case to be limited to only the one way delay in the network link between the client and the leader, instead of the clock skew experienced by all distributed systems participating in two-phase transfers.

## Protocol

The current TCP wire protocol is:

* a fixed-size header that can be used for requests or responses,
* followed by variable-length data.

```
HEADER (128 bytes)
16 bytes CHECKSUM (of remaining HEADER)
16 bytes CHECKSUM BODY
[...see src/vsr.zig for the rest of the Header definition...]
DATA (multiples of 64 bytes)
................................................................................
................................................................................
................................................................................
```

The `DATA` in **the request** for a `create_transfer` command looks like this:

```
{ create_transfer event struct }, { create_transfer event struct } etc.
```

* All event structures are simply appended one after the other in the `DATA`.

The `DATA` in **the response** to a `create_transfer` command looks like this:

```
{ index: integer, error: integer }, { index: integer, error: integer }, etc.
```

* Only failed `create_transfer` events emit an `error` struct in the response. We do this to optimize the common case where most `create_transfer` events succeed.
* The `error` struct includes the `index` into the batch of the `create_transfer` event that failed and a TigerBeetle `error` return code indicating why.
* All other `create_transfer` events succeeded.
* This `error` struct response strategy is the same for `create_account` events.

### Protocol Design Decisions

The header is a multiple of 128 bytes because we want to keep the subsequent data aligned to 64-byte cache line boundaries. We don't want any structure to straddle multiple cache lines unnecessarily for the sake of simplicity with respect to struct alignment and because this can have a performance impact through false sharing.

We order the header struct as we do to keep any C protocol implementations padding-free.

We use BLAKE3 as our checksum, truncating the checksum to 128 bits.

The reason we use two checksums instead of only a single checksum across header and data is that we
need a reliable way to know the size of the data to expect, before we start receiving the data.

Here is an example showing the risk of a single checksum for the recipient:

1. We receive a header with a single checksum protecting both header and data.
2. We extract the SIZE of the data from the header (4 GB in this case).
3. We cannot tell if this SIZE value is corrupt until we receive the data.
4. We wait for 4 GB of data to arrive before calculating/comparing checksums.
5. Except the SIZE was corrupted in transit from 16 MB to 4 GB (2 bit flips).
6. We never detect the corruption, the connection times out and we miss our SLA.

## Why C/Zig?

We want:

* **C ABI compatibility** to embed the TigerBeetle leader library or TigerBeetle network client directly into any language, to match the portability and ease of use of the [SQLite library](https://www.sqlite.org/index.html), the most used database engine in the world.
* **Control of the memory layout, alignment, and padding of data structures** to avoid cache misses and unaligned accesses, and to allow zero-copy parsing of data structures from off the network.
* **Explicit static memory allocation** from the network all the way to the disk with **no hidden memory allocations**.
* **OOM safety** as the TigerBeetle leader library needs to manage GBs of in-memory state without crashing.
* Direct access to **io_uring** for fast, simple networking and file system operations.
* Direct access to **fast CPU instructions** such as `POPCNT`, which are essential for the hash table implementation we want to use.
* Direct access to **existing C libraries** without the overhead of FFI.
* **Strict single-threaded control flow** to eliminate data races by design and to enforce the single writer principle.
* **Compiler support for error sets** to enforce [fine-grained error handling](https://www.eecg.utoronto.ca/~yuan/papers/failure_analysis_osdi14.pdf).
* A developer-friendly and fast build system.

Zig retains C ABI interoperability, offers relief from undefined behavior and makefiles, and provides an order of magnitude improvement in runtime safety and fine-grained error handling. Zig is a good fit with its emphasis on explicit memory allocation and OOM safety. Since Zig is pre-1.0.0 we plan to use only stable language features. It's a great time for TigerBeetle to adopt Zig since our stable roadmaps will probably coincide. We can catch and surf the swell as it breaks.

## What is a tiger beetle?

Two things got us interested in tiger beetles as a species:

1. Tiger beetles are ridiculously fast... a tiger beetle can run at a speed of 9 km/h, about 125 body lengths per second. That’s 20 times faster than an Olympic sprinter when you scale speed to body length, **a fantastic speed-to-size ratio**. To put this in perspective, a human would need to run at 480 miles per hour to keep up.

2. Tiger beetles thrive in different environments, from trees and woodland paths, to sea and lake shores, with the largest of tiger beetles living primarily in the dry regions of Southern Africa... and that's what we want for TigerBeetle, **something that's fast and safe to deploy everywhere**.

## References

The collection of papers behind TigerBeetle:

* [LMAX - How to Do 100K TPS at Less than 1ms Latency - 2010](https://www.infoq.com/presentations/LMAX/) - Martin Thompson on mechanical sympathy, and why a relational database is not the right solution.

* [The LMAX Exchange Architecture - High Throughput, Low Latency and Plain Old Java - 2014](https://skillsmatter.com/skillscasts/5247-the-lmax-exchange-architecture-high-throughput-low-latency-and-plain-old-java) - Sam Adams on the high-level design of LMAX.

* [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf) - A high performance alternative to bounded queues for
exchanging data between concurrent threads.

* [Evolution of Financial Exchange Architectures - 2020](https://www.youtube.com/watch?v=qDhTjE0XmkE) - Martin Thompson looks at the evolution of financial exchanges and explores the state of the art today.

* [Gray Failure](https://www.microsoft.com/en-us/research/wp-content/uploads/2017/06/paper-1.pdf) - "The major availability breakdowns and performance anomalies we see in cloud environments tend to be caused by subtle underlying faults, i.e. gray failure rather than fail-stop failure."

* [The Tail at Store: A Revelation from Millions
of Hours of Disk and SSD Deployments](https://www.usenix.org/system/files/conference/fast16/fast16-papers-hao.pdf) - "We find that storage performance instability is not uncommon: 0.2% of the time, a disk is more than 2x slower than its peer drives in the same RAID group (and 0.6% for SSD). As a consequence, disk and SSD-based RAIDs experience at least one slow drive (i.e., storage tail) 1.5% and 2.2% of the time."

* [The Tail at Scale](https://www2.cs.duke.edu/courses/cps296.4/fall13/838-CloudPapers/dean_longtail.pdf) - "A simple way to curb latency variability is to issue the same request to multiple replicas and use the results from whichever replica responds first."

* [Viewstamped Replication Revisited](http://pmg.csail.mit.edu/papers/vr-revisited.pdf)

* [Viewstamped Replication: A New Primary Copy Method to Support Highly-Available Distributed Systems](http://pmg.csail.mit.edu/papers/vr.pdf)

* [ZFS: The Last Word in File Systems (Jeff Bonwick and Bill Moore)](https://www.youtube.com/watch?v=NRoUC9P1PmA) - On disk failure and corruption, the need for checksums... and checksums to check the checksums, and the power of copy-on-write for crash-safety.

* [An Analysis of Latent Sector Errors in Disk Drives](https://research.cs.wisc.edu/wind/Publications/latent-sigmetrics07.pdf)

* [SDC 2018 - Protocol-Aware Recovery for Consensus-Based Storage](https://www.youtube.com/watch?v=fDY6Wi0GcPs) - Why replicated state machines need to distinguish between a crash and corruption, and why it would be disastrous to truncate the journal when encountering a checksum mismatch.

* [Can Applications Recover from fsync Failures?](https://www.usenix.org/system/files/atc20-rebello.pdf) - Why we use Direct I/O in TigerBeetle and why the kernel page cache is a dangerous way to recover the journal, even when restarting from an fsync() failure panic.

* [Coil's Mojaloop Performance Work 2020](https://docs.mojaloop.io/documentation/discussions/Mojaloop%20Performance%202020.pdf) - By Don Changfoot and Joran Dirk Greef, a performance analysis of Mojaloop's central ledger that sparked the idea for "an accounting database" as Adrian Hope-Bailie put it. And the rest, as they say, is history!
