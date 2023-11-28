---
sidebar_position: 9
---

# FAQ

## What is double-entry accounting?

Double-entry accounting tracks transfers between accounts. It records
the amount of the transfer along with where the amount came from and
where it goes.

## Why does double-entry accounting matter?

Double-entry gives you something stronger than raw data consistency,
namely financial consistency, by acting as a financial error
correcting code to keep the books balanced.

It does this just like Newton's Third Law—for every movement of money
into an account, there is an equal and opposite movement of money from
a different account. This ensures that money cannot be created or
destroyed, but is merely transferred from one account to another, just
like the law of conservation of energy.

Because it's so simple, it's also highly composable. And so it gives
you tremendous flexibility to model your business, even as product
lines come and go, or as your business evolves. We have often seen
organizations rediscover double-entry accounting over time, requiring
painful database migrations as they evolve toward double-entry.

Double-entry accounting is the language of business, the world
over. It's the tried and tested schema for representing any kind of
business, any kind of transaction. While you can think of double-entry
as the perfect way to describe any kind of financial state transition,
it's also the perfect way to track and “account” for anything, any
kind of business or real world event, not necessarily financial.

"What advantages does he derive from the system of book-keeping by
double-entry? It is among the finest inventions of the human mind;
every prudent master of a house should introduce it into his economy."
— Goethe, 1795

## Why would I want a dedicated distributed database for accounting?

You can do accounting with any database, it's true. Existing companies
use PostgreSQL or MongoDB or any other general-purpose
database. However, this leaves significant work for the developer and
operator. You must figure out high availability, fault-tolerance, and
consistency. You must figure out how to scale as you add users and
transfers. And of course, you must re-implement all the double-entry
and ledger primitives, which are subtle to get right!

TigerBeetle is focused on just one thing: accounting reliably at
scale. TigerBeetle does double-entry accounting and as a database has
only two data types: accounts, and transfers of money between
accounts. TigerBeetle also goes beyond typical ledgers in that it's
not only great at tracking money within a system, but has a built-in
two-phase commit coordinator to help you track money as it moves
between systems—even different companies that have entirely different
ledger architectures! It has built in consensus for high availability,
and recovery mechanisms that surpass most databases today.

Additionally, TigerBeetle was designed precisely for high performance
accounting. We've seen teams struggle to reach on the order of 1,000s
of transfers per second when they've built on PostgreSQL and
MongoDB. It comes down to contention. For example, a business may
have a few million customers but only a few bank accounts which will
contend for row locks and become the bottleneck (as transfers are
serialized through them). With this challenge in mind, we're designing
TigerBeetle to achieve 1,000,000 transfers per second on commodity
hardware.

With financial data, it's also important not to lump it together with
general purpose data. The risk profile is different: retention
policies, compliance requirements, and privacy concerns. In auditing
the term is “separation of concerns”. And the performance
characteristics, auditability, and availability requirements are also
dissimilar from general purpose data. In much the same way that you
would store large objects in an object store, or streaming data in a
queue, you really do want separation of concerns for your financial
data and a mission critical database to store it.

## What is a two-phase transfer?

Single-phase transfers post funds to accounts immediately when they
are created.

In contrast to single-phase transfers, a two-phase transfer moves
funds in stages.

See [Two-phase transfers](./design/two-phase-transfers.md) for more
detail.

## How does TigerBeetle fit into my architecture?

TigerBeetle is a ledger. It is not designed to be a general-purpose
metadata store. But there is no single correct way to use TigerBeetle.

Here is an example of how you might integrate with TigerBeetle in a
payment system.

You might keep account metadata in your existing database (say,
PostgreSQL or MongoDB). Then create an account in TigerBeetle with the
[same identifier](./design/data-modeling.md#reuse-foreign-identifier) to
map it back to the account with metadata in your existing database.

Then as you need to process transfers between accounts, you store
those transfers in TigerBeetle.

TigerBeetle comes with built in business logic like
accepting/rejecting related or linked transfers atomically as a unit,
rejecting transfers if the accounts involved would exceed their net
debit or credit balances, and other business logic that you can
toggle.

When you need to report on transfers, you'll use our query API.
Queries are currently limited to lookups by account ID or transfer
ID. However, we're already indexing all fields and a rich relational
query engine is in the works.

You will also be able to export all historic accounts and transfers.

## Why is TigerBeetle written in Zig?

Coming from C, it's the language we've always wanted—OOM safety,
bounds checking, rich choice of allocators (and test allocators),
checked arithmetic, explicit control flow, explicit control over
memory layout and alignment, no hidden allocations, no macros, first
class C ABI interoperability, and an insanely good compiler. Comptime
is a game changer.

We realized that our roadmap would coincide with Zig's in terms of
stability. We wanted to invest for the next 20 years and didn't want
to be stuck with C/C++ or compiler/language complexity and pay a tax
for the lifetime of the project.

## Is TigerBeetle ready for production?

Not yet! TigerBeetle was started in 2020 and you're watching us build
the plane while we're flying it. We are working with design partners
and TigerBeetle is being used in corporate "lab" settings. Likewise,
we encourage you to [try out
TigerBeetle](https://github.com/tigerbeetle/tigerbeetle#quickstart),
[follow our
development](https://github.com/tigerbeetle/tigerbeetle#short-term-roadmap),
and [give feedback](https://join.slack.com/t/tigerbeetle/shared_invite/zt-1gf3qnvkz-GwkosudMCM3KGbGiSu87RQ)!

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
our consistency guarantees [here](./design/consistency.md).

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
