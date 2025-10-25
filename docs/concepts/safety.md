# Safety

The purpose of a database is to store data: if the database accepted new data, it should be able to
retrieve it later. Surprisingly, many databases don't provide guaranteed durability --- usually the
data is there, but, under certain edge case conditions, it can get lost!

As the purpose of TigerBeetle is to be the system of record for business transaction, associated
with real-world value transfers, it is paramount that the data stored in TigerBeetle is safe.

## Strict Serializability

The easiest way to lose data is by incorrectly using the database, by misconfiguring (or just
misunderstanding) its isolation level. For this reason, TigerBeetle intentionally supports only the
strictest possible isolation level --- **strict serializability**. All transfers are executed
one-by-one, on a single core.

Furthermore, TigerBeetle's state machine is designed according to the
[**end-to-end idempotency principle**](../coding/reliable-transaction-submission.md) ---
each transfer has a unique client-generated `u128` id, and each transfer is processed at most once,
even in the presence of intermediate retry loops.

## High Availability

Some databases rely on a single central server, which puts the data at risk as any single server
might fail catastrophically (e.g. due to a fire in the data center). Primary/backup systems with
ad-hoc failover can lose data due to split-brain.

To avoid these pitfalls, TigerBeetle implements pioneering
[Viewstamped Replication](http://pmg.csail.mit.edu/papers/vr-revisited.pdf) and consensus algorithm,
that guarantees correct, automatic failover. It's worth emphasizing that consensus proper needs only
be engaged during actual failover. During the normal operation, the cost of consensus is just the
cost of replication, which is further minimized because of
[batching](./performance.md#batching-batching-batching), tail latency tolerance, and pipelining.

TigerBeetle does not depend on synchronized system clocks, does not use leader leases, and
**performs leader-based timestamping** so that your application can deal only with safe relative
quantities of time with respect to transfer timeouts. To ensure that the leader's clock is within
safe bounds of "true time", TigerBeetle combines all the clocks in the cluster to create a
fault-tolerant clock that we call
["cluster time"](https://tigerbeetle.com/blog/three-clocks-are-better-than-one/).

For the highest availability, TigerBeetle should be deployed as a cluster of six replicas across three
different cloud providers (two replicas per provider). Because TigerBeetle uses
[Heidi Howard's flexible quorums](https://arxiv.org/pdf/1608.06696v1), this deployment is guaranteed
to tolerate a complete outage of any cloud provider and will likely survive even if one extra
replica fails.

TigerBeetle detects and overcomes
[Gray Failure](https://www.microsoft.com/en-us/research/wp-content/uploads/2017/06/paper-1.pdf)
automatically. If a replica's disk becomes slow or the network interface starts dropping packets,
TigerBeetle automatically adjusts replication topology to ensure that the slow replica doesn't
affect user-visible latencies, while still guaranteeing cluster-wide durability.

## Storage Fault Tolerance

Traditionally, databases assume that disks do not fail, or at least fail politely with a clear error
code. This is usually a reasonable assumption, but edge cases matter.

HDD and SSD hardware can fail. Disks can silently return corrupt data (
[0.031% of SSD disks per year](https://www.usenix.org/system/files/fast20-maneas.pdf),
[1.4% of Enterprise HDD disks per year](https://www.usenix.org/legacy/events/fast08/tech/full_papers/bairavasundaram/bairavasundaram.pdf)),
misdirect IO (
[0.023% of SSD disks per year](https://www.usenix.org/system/files/fast20-maneas.pdf),
[0.466% of Nearline HDD disks per year](https://www.usenix.org/legacy/events/fast08/tech/full_papers/bairavasundaram/bairavasundaram.pdf)),
or just suddenly become extremely slow, without returning an error code (the so called
[gray failure](https://www.microsoft.com/en-us/research/wp-content/uploads/2017/06/paper-1.pdf)).

On top of hardware, software might be buggy or just tricky to use correctly. Handling fsync failures
correctly is [particularly hard](https://www.usenix.org/system/files/atc20-rebello.pdf).

**TigerBeetle assumes that its disk _will_ fail** and takes advantage of replication to proactively
repair replica's local disks.

- All data in TigerBeetle is immutable, checksummed, and [hash-chained](https://csrc.nist.gov/glossary/term/hash_chain), providing a strong guarantee
  that no corruption or tampering happened. In case of a latent sector error, the error is detected
  and repaired without any operator involvement.
- Most consensus implementations lose data or become unavailable if the write-ahead log gets
  corrupted. TigerBeetle uses [Protocol Aware Recovery](https://www.youtube.com/watch?v=fDY6Wi0GcPs)
  to remain available unless the data gets corrupted on every single replica.
- To minimize the impact of software bugs, TigerBeetle puts as little software as possible between
  itself and the disk --- TigerBeetle manages its own page cache, writes data to disk with O_DIRECT
  and can work with a block device directly, no file system is necessary.
- TigerBeetle also tolerates gray failure --- if a disk on a replica becomes very slow, the cluster
  falls back on other replicas for durability.

## Software Reliability

Even the advanced algorithm with a formally proved correctness theorem is useless if the
implementation is buggy. TigerBeetle uses the oldest and the newest software engineering practices
to ensure correctness.

TigerBeetle is written in [Zig](https://ziglang.org) --- a modern systems programming language that
removes many instances of undefined behavior, provides spatial memory safety and encourages simple,
straightforward code.

TigerBeetle adheres to a strict code style,
[TIGER_STYLE](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md), inspired by
[NASA's power of ten](https://spinroot.com/gerard/pdf/P10.pdf). For example, TigerBeetle uses static
memory allocation, which designs away memory fragmentation, out-of-memory errors and
use-after-frees.

TigerBeetle is tested in the VOPR --- a simulated environment where an entire cluster, running real
code, is subjected to all kinds of network, storage and process faults, at 1000x speed. This
simulation can find both logical errors in the algorithms and coding bugs in the source. This
simulator is running 24/7 on 1024 cores, fuzzing the latest version of the database.

It also **runs in your browser**: <https://sim.tigerbeetle.com>!

## Human Fallibility

While, with a lot of care, software can be perfected to become virtually bug-free, humans will
always make mistakes. TigerBeetle takes this into account and tries to protect from operator errors:

- The surface area is intentionally minimized, with little configurability.
- In particular, there's only one isolation level --- strict serializability.
- Upgrades are automatic and atomic, guaranteeing that each transfer is applied by only a single
  version of code.
- TigerBeetle always runs with online verification on, to detect any discrepancies in the data.

## Is TigerBeetle ACID-compliant?

Yes. Let's discuss each part:

### Atomicity

As part of replication, each operation is durably stored in at least a quorum of replicas'
Write-Ahead Logs (WAL) before the primary will acknowledge the operation as committed. WAL entries
are executed through the state machine business logic and the resulting state changes are stored in
TigerBeetle's LSM-Forest local storage engine.

The WAL is what allows TigerBeetle to achieve atomicity and durability since the WAL is the source
of truth. If TigerBeetle crashes, the WAL is replayed at startup from the last checkpoint on disk.

However, financial atomicity goes further than this: events and transfers can be
[linked](../coding/linked-events.md) when created so they all succeed or fail together.

### Consistency

TigerBeetle guarantees strict serializability. And at the cluster level, stale reads are not
possible since all operations (not only writes, but also reads) go through the global consensus
protocol.

However, financial consistency requires more than this. TigerBeetle exposes a double-entry
accounting API to guarantee that money cannot be created or destroyed, but only transferred from one
account to another. And transfer history is immutable.

### Isolation

All client requests (and all events within a client request batch) are executed with the highest
level of isolation, serially through the state machine, one after another, before the next operation
begins. Counterintuitively, the use of batching and serial execution means that TigerBeetle can also
provide this level of isolation optimally, without the cost of locks for all the individual events
within a batch.

### Durability

Up until 2018, traditional DBMS durability has focused on the Crash Consistency Model, however,
Fsyncgate and
[Protocol Aware Recovery](https://www.usenix.org/conference/fast18/presentation/alagappan) have
shown that this model can lead to real data loss for users in the wild. TigerBeetle therefore adopts
an explicit storage fault model, which we then verify and test with incredible levels of corruption,
something which few distributed systems historically were designed to handle. Our emphasis on
protecting Durability is what sets TigerBeetle apart, not only as a ledger but as a DBMS.

However, absolute durability is impossible, because all hardware can ultimately fail. Data we write
today might not be available tomorrow. TigerBeetle embraces limited disk reliability and maximizes
data durability in spite of imperfect disks. We actively work against such entropy by taking
advantage of cluster-wide storage. A record would need to get corrupted on all replicas in a cluster
to get lost, and even in that case the system would safely halt.

## `io_uring` Security

`io_uring` is a relatively new part of the Linux kernel (support for it was added in version 5.1,
which was released in May 2019). Since then, many kernel exploits have been found related to
`io_uring` and in 2023
[Google announced](https://security.googleblog.com/2023/06/learnings-from-kctf-vrps-42-linux.html)
that they were disabling it in ChromeOS, for Android apps, and on Google production servers.

Google's post is primarily about how they secure operating systems and web servers that handle
hostile user content. In the Google blog post, they specifically note:

> we currently consider it safe only for use by trusted components

As a financial system of record, TigerBeetle is a trusted component and it should be running in a
trusted environment.

Furthermore, TigerBeetle only uses 128-byte [`Account`s](../reference/account.md) and
[`Transfer`s](../reference/transfer.md) with pure integer fields. TigerBeetle has no
(de)serialization and does not take user-generated strings, which significantly constrains the
attack surface.

We are confident that `io_uring` is the safest (and most performant) way for TigerBeetle to handle
async I/O. It is significantly easier for the kernel to implement this correctly than for us to
include a userspace multithreaded thread pool (for example, as libuv does).

## Next: Coding

This concludes the discussion of the concepts behind TigerBeetle --- an [OLTP](./oltp.md) database
for recording business transactions in real time, using a
[double-entry bookkeeping](./debit-credit.md) schema, which
[is orders of magnitude faster](./performance.md) and
[keeps the data safe](./safety.md) even when the underlying hardware inevitably fails.

We will now learn [how to build applications on top of TigerBeetle](../coding/).
