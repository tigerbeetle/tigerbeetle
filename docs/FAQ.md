---
sidebar_position: 6
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

## What is two-phase commit?

This is a reference to the [two-phase commit protocol for distributed
transactions](https://en.wikipedia.org/wiki/Two-phase_commit_protocol).

Single-phase transfers post funds to accounts immediately when they
are created.

In contrast to single-phase transfers, a two-phase transfer moves
funds in stages:

1. First, the pending transfer reserves funds. While reserved,
they cannot be used by either the payer or payee.

2. Later (in a future "commit", i.e. a separate request), the
application creates another transfer — either a post-pending transfer
or a void-pending transfer. The former moves all (or part) of the
reserved funds to the destination; the latter reverts them to the
original account. The pending transfer's amount is reserved in a way
that this second step will never cause the account's configured
balance invariants (e.g. debits < credits) to be broken.

## How does TigerBeetle fit into my architecture?

TigerBeetle is a ledger. It is not designed to be a general-purpose
metadata store. But there is no single correct way to use TigerBeetle.

Here is an example of how you might integrate with TigerBeetle in a
payment system.

You might keep account metadata in your existing database (say,
PostgreSQL or MongoDB). Then create an account in TigerBeetle with a
[random id](./usage/data-modeling.md#random-identifier) mapping it
back to the account with metadata in your existing database.

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
TigerBeetle](https://github.com/tigerbeetledb/tigerbeetle#quickstart),
[follow our
development](https://github.com/tigerbeetledb/tigerbeetle#short-term-roadmap),
and [give feedback](https://join.slack.com/t/tigerbeetle/shared_invite/zt-1gf3qnvkz-GwkosudMCM3KGbGiSu87RQ)!
