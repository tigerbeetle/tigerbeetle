---
sidebar_position: 3
---

## Is TigerBeetle ACID-compliant?

Yes. Let's discuss each part:

### Atomicity

As part of replication, each operation is durably stored in at least a
quorum of replicas' Write-Ahead Logs (WAL) before the primary will
acknowledge the operation as committed. WAL entries are executed
through the state machine business logic and the resulting state
changes are stored in TigerBeetle's LSM-Forest local storage engine.

The WAL is what allows TigerBeetle to achieve atomicity and durability
since the WAL is the source of truth. If TigerBeetle crashes, the WAL
is replayed at startup from the last checkpoint on disk.

However, financial atomicity goes further than this: events and
transfers can be linked when created so they all succeed or fail
together.

### Consistency

TigerBeetle guarantees strict serializability. And at the cluster
level, stale reads are not possible since all operations (not only
writes, but also reads) go through the global consensus protocol.

However, financial consistency requires more than this. TigerBeetle
exposes a double-entry accounting API to guarantee that money cannot
be created or destroyed, but only transferred from one account to
another. And transfer history is immutable. You can read more about
our consistency guarantees [here](./develop/consistency.md).

### Isolation

All client requests (and all events within a client request batch) are
executed with the highest level of isolation, serially through the
state machine, one after another, before the next operation
begins. Counterintuitively, the use of batching and serial execution
means that TigerBeetle can also provide this level of isolation
optimally, without the cost of locks for all the individual events
within a batch.

### Durability

Up until 2018, traditional DBMS durability has focused on the Crash
Consistency Model, however, Fsyncgate and [Protocol Aware
Recovery](https://www.usenix.org/conference/fast18/presentation/alagappan)
have shown that this model can lead to real data loss for users in the
wild. TigerBeetle therefore adopts an explicit storage fault model,
which we then verify and test with incredible levels of corruption,
something which few distributed systems historically were designed to
handle. Our emphasis on protecting Durability is what sets TigerBeetle
apart, not only as a ledger but as a DBMS.

However, absolute durability is impossible, because all hardware can
ultimately fail. Data we write today might not be available
tomorrow. TigerBeetle embraces limited disk reliability and maximizes
data durability in spite of imperfect disks. We actively work against
such entropy by taking advantage of cluster-wide storage. A record
would need to get corrupted on all replicas in a cluster to get lost,
and even in that case the system would safely halt.