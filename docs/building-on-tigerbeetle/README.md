# Building on TigerBeetle

TigerBeetle is a domain-specific database — its schema of [`Account`s](../reference/accounts.md) and
[`Transfer`s](../reference/transfers.md) is built-in and fixed. In return for this prescriptive
design, it provides excellent performance, integrated business logic, and powerful invariants.

To help you get started building on TigerBeetle, here is a quick overview of the most important
concepts:

- the two main data structures in TB are Accounts and Transfers. Accounts track balances

## Accounts

## Transfers

## Debits and Credits

- double entry bookkeeping - every transfer debits one account and increases another
- note about multi-debits / credits

## Ledgers

- not represented in TB directly
- metadata is stored outside of TB
- asset scale
- link to currency exchange
- can be one ledger per currency or type of value, or can do subledgers for customers in a
  multi-tenant setup
- one way to think about it is groups of accounts that cannot transfer between one another directly
