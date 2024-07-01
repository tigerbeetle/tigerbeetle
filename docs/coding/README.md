# Developing Applications on TigerBeetle

TigerBeetle is a domain-specific, [Online Transaction Processing (OLTP)](../about/oltp.md) database.
It has a fixed schema consisting of [`Account`s](../reference/account.md) and
[`Transfer`s](../reference/transfer.md). In return for this prescriptive design, it provides
excellent performance, integrated business logic, and powerful invariants.

To help you get started building on TigerBeetle, here is a quick overview of the most important
concepts:

## Data Model Overview

- [`Account`s](../reference/account.md) track balances for users or other entities.
- [`Transfer`s](../reference/transfer.md) move funds between `Account`s.
- Account balances and transfer amounts are represented as debits and credits. Double-entry
  bookkeeping ensures your accounting maintains consistency.
- Accounts are partitioned into [Ledgers](./data-modeling.md#ledgers), which may represent different
  currencies, assets, liabilities, etc. or they may be used to support multitenancy. Only accounts
  on the same ledger can transact directly, but you can use atomically
  [linked transfers](../reference/requests/README.md#linked-events) to implement
  [cross-currency transactions](./recipes/currency-exchange.md).
- TigerBeetle has first-class support for [two-phase transfers](./two-phase-transfers.md), which can
  hold funds in a pending state and can be used to synchronize transfers with external systems.

## TigerBeetle in Your System Architecture

TigerBeetle is an Online Transaction Processing (OLTP) database built for safety and performance.

It is not a general purpose database like PostgreSQL or MySQL. Instead, TigerBeetle works alongside
your general purpose database and should be used to handle the hot path of transaction processing.

For more information relevant to integrating TigerBeetle into your system, take a look at the
following pages:

- [TigerBeetle in Your System Architecture](./system-architecture.md)
- [Time](./time.md)

## Advanced Recipes

Depending on your use case, you may find these additional design patterns helpful:

- [Currency Exchange](./recipes/currency-exchange.md)
- [Multi-Debit, Multi-Credit Transfers](./recipes/multi-debit-credit-transfers.md)
- [Closing Accounts](./recipes/close-account.md)
- [Balance-Conditional Transfers](./recipes/balance-conditional-transfers.md)
- [Balance Bounds](./recipes/balance-bounds.md)
- [Correcting Transfers](./recipes/correcting-transfers.md)
- [Rate Limiting](./recipes/rate-limiting.md)

## Want Help Developing on TigerBeetle?

### Ask the Community

Have questions about TigerBeetle's data model, how to design your application on top of it, or
anything else?

Come join our [Community Slack](https://slack.tigerbeetle.com/invite) and ask any and all questions
you might have!

### Dedicated Consultation

Would you like the TigerBeetle team to help you design your chart of accounts and leverage the power
of TigerBeetle in your architecture?

Let us help you get it right. Contact our CEO, Joran Dirk Greef, at <joran@tigerbeetle.com> to set
up a call.
