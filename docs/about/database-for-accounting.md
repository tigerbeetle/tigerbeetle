---
sidebar_position: 2
---

## A Database for Accounting

## Why Would I Want A Dedicated Distributed Database For Accounting?

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

## How Does TigerBeetle Fit Into My Architecture?

TigerBeetle is a ledger. It is not designed to be a general-purpose
metadata store. But there is no single correct way to use TigerBeetle.

Here is an example of how you might integrate with TigerBeetle in a
payment system.

You might keep account metadata in your existing database (say,
PostgreSQL or MongoDB). Then create an account in TigerBeetle with the
[same identifier](./develop/data-modeling.md#reuse-foreign-identifier) to
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
