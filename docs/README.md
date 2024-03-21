---
slug: /
sidebar_position: 1
---

# TigerBeetle Docs

TigerBeetle is a distributed financial accounting database designed
for mission critical safety and performance.

## Getting Started

Head over to [Getting Started](./getting-started/README.md) to
[download](./getting-started/download.md) TigerBeetle, [run a
cluster](./getting-started/single-binary.md), and try [creating accounts and transfers using the
CLI](./getting-started/cli-repl.md).

## Designing for TigerBeetle

When integrating TigerBeetle into your project, consider the
following:

- [TigerBeetle and consistency](./building-on-tigerbeetle/consistency.md)
- [Modeling application data within TigerBeetle](./building-on-tigerbeetle/data-modeling.md)
- [How two-phase transfers work](./building-on-tigerbeetle/two-phase-transfers.md)
- [How client sessions work](./building-on-tigerbeetle/client-sessions.md)
- [How client requests work](./building-on-tigerbeetle/client-requests.md)

## Recipes

Here are some common use-cases for TigerBeetle:

- [Closing accounts](./recipes/close-account.md)
- [Currency exchange](./recipes/currency-exchange.md)
- [Multi-debit/credit transfers](./recipes/multi-debit-credit-transfers.md)

## Client libraries

We officially support the following libraries for communicating with a
TigerBeetle cluster:

- [.NET](/src/clients/dotnet/README.md)
- [Go](/src/clients/go/README.md)
- [Java](/src/clients/java/README.md)
- [Node.js](/src/clients/node/README.md)

### Sample code

Within each client library README you will find links to sample code.

## Reference

To understand TigerBeetle's data model, see:

- [Accounts](./reference/accounts.md)
- [Transfers](./reference/transfers.md)
- And [Operations](./reference/operations/index.md)
  - [`create_accounts`](./reference/operations/create_accounts.md)
  - [`create_transfers`](./reference/operations/create_transfers.md)
  - [`lookup_accounts`](./reference/operations/lookup_accounts.md)
  - [`lookup_transfers`](./reference/operations/lookup_transfers.md)

## Contributing

- [Watch our talks, listen to our podcasts](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TALKS.md)
- [Read HISTORY.md](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/HISTORY.md)
- [Read DESIGN.md](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/DESIGN.md)
- [Read DEEP_DIVE.md](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/DEEP_DIVE.md)
- [Read TIGER_STYLE.md](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md)
- [Read HACKING.md](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/HACKING.md)
- And explore internal READMEs within the [TigerBeetle src directory](https://github.com/tigerbeetle/tigerbeetle/tree/main/src)

## Community

- [Twitter](https://twitter.com/tigerbeetledb)
- [GitHub](https://github.com/tigerbeetle/tigerbeetle)
- [Slack](https://slack.tigerbeetle.com/invite)
- [Monthly Newsletter](https://mailchi.mp/8e9fa0f36056/subscribe-to-tigerbeetle)
- [YouTube](https://www.youtube.com/@tigerbeetledb)
