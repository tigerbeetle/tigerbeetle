# Design

The technical into into TigerBeetle. One day this can become a beautiful paper, but, for now, it is
a bullet list. Be sure to also watch our QCon SF talk: <https://www.infoq.com/presentations/redesign-oltp/>

TigerBeetle tracks financial transactions, using double-entry bookkeeping scheme. This particular
"database" scheme was used centuries before first electronic computers were invented, and is
surprisingly good and flexible for recording all kinds of business events. This problem brings two
requirements:

- Safety --- financial transactions are important. If a system says that it recorded a transfer, it
  is table stakes that:
    - the transfer is recorded correctly (i.e, there actually was enough money in the account),
    - it _stays_ recorded no matter what
- Performance --- the amount of existing transactions in the world grows very fast, and there
  certain domains where the TPS is _already_ limited by available database technology, rather than
  by the size of the domain itself.

The problem also brings a key challenge --- contention. A single transfer transfers value _from_ one
account _to_ a different account, touching two database objects at once. And, in a typical
accounting system, transfer distribution obeys the power law --- few accounts are generating most of
the transfers.

Multi object transaction plus contention is a combination that makes traditional SQL databases
inefficient --- the SQL interface forces holding row-locks over the network. In other words, a
financial transfer implemented as an SQL transaction needs to:

- lock the first account in the database
- lock the second account in the database
- make the decision on whether to allow or reject transfer in the application
- compute the resulting balances in the application
- updates the accounts in the database

Due to the power law distribution, most of the transfers will contend for a shared account, reducing
performance to that of sequential processing over the network.

## Rediscovering OLTP

TigerBeetle begins in the past. When OLTP was originally defined, the T standed for a "business
transaction" --- a record of a transfer between two accounts. It is only later that transactions
became associated with SQL transactions. TigerBeetle re-discovers it's own past by attacking the
problem of business transactions directly. Specifically:

- Because such transactions are contentious, the fastest way to run them is a tight sequential loop
  on a single CPU core.
- To make the loop go fast, there should not be user-defined code supplied over the network
  in-between. For this reason, a transfer between between two accounts is a built-in primitive, and
  the logic for processing the transfer is encoded directly in the database. The primitive is quite
  flexible though! With pending and linked transfers, many patterns can be easily expressed.

## Speed of Light

The simplest way to track transactions is with an in-memory hash-map. This is the "speed-of-light"
limit on how fast you can theoretically go. But in-memory hash map is unsatisfactory, for two
reasons:

- It lacks persistance --- if the program is restarted, it forgets all the previous transfers.
- It is not highly-available --- the computer with the hash map is a single point of failure, if it
  crashes, no transactions can be processed.

How close can we keep to the theoretical limit of a hash map, if we add persistance and
high-availability? Surprisingly, pretty close, and this is TigerBeetle!

## Consensus

Let's tackle high-availability first. We want to survive a crash of any particular machine, meaning
that:

- the data must be _replicated_ across a set of machines,
- but all the machines in the set must agree on what the data is!

TigerBeetle uses the tried and tested approach here --- a replicated state machine. That is, a
machine, with a state, that is replicated.

The ground state of the system is an immutable append-only log --- a sequence of prepare messages.
This log is replicated across across all replicas in the cluster by sending individual prepares. The
log contains transfers.

On top of the log, derived state is constructed --- starting with an empty state, each replica
applies log messages, sequentially, to its local state. Because the transition function is
deterministic, and the log is consistent across the replicas, the result (account balances) is
consistent as well.

## Performance of Consensus

Ensuring that the logs match requires a consensus algorithm. But consensus isn't slow! If the
cluster functions normally, the work of consensus is _just_ replication --- sending prepare to other
replicas, writing&fsyncing it to persistent storage, sending acks back. Consensus proper is needed
_only_ when a primary crashes and the new primary must reconstruct an up-to-date log.

The cost of replication can be further drastically reduced.

_First_, we can do batching. Instead of replicating each individual transfer, TigerBeetle packs
around 8 000 transfers into a single prepare message, and a network round-trip amortizes across them
all. This optimization doesn't affect latency directly, but drastically increases the throughput.

_Second_, while prepares with transfers need to be executed in order, the replication can be
pipelined. That is, the second prepare can start replicating while the previous one hasn't yet
finished its replication. This optimization improves the latency, as prepares don't have to wait on
each-other.

_Finally_, tail-at-scale applies. When replicating, its enough to get response from the quorum of
the fastest replicas. Notably, the primary doesn't have to wait for its own `fsync` to complete if a
quorum of backups `fsync`ed. In pathological cases, this could even make a distributed system
_faster_ than a local one! If `fsync` times are highly variable, then, on average, three fastest
replicas out of six will be faster than any given replica.

In other words, the cost of replication is negligible for throughput, and not that high for latency!





## Why Zig

(see old zig.md)

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
