---
slug: /
sidebar_position: 1
---

# TigerBeetle Docs

TigerBeetle is a distributed financial accounting database designed for mission critical safety and
performance.

## Quick Start

[Download TigerBeetle](./quick-start/README.md) and run a
[single-node](./quick-start/single-node.md) or [multi-node cluster](./quick-start/cluster.md) in
under 2 minutes.

Then, [create accounts and transfers using the REPL](./getting-started/cli-repl.md) to get a feel
for what TigerBeetle can do.

## Developing on TigerBeetle

Read more about [Developing on TigerBeetle](./develop/README.md) to understand the
data model, learn how to integrate TigerBeetle into your system, and see some advanced design
patterns.

## Client libraries

We officially support the following libraries for communicating with a
TigerBeetle cluster:

- [.NET](/src/clients/dotnet/README.md)
- [Go](/src/clients/go/README.md)
- [Java](/src/clients/java/README.md)
- [Node.js](/src/clients/node/README.md)

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
  - [`get_account_balances`](./reference/operations/get_account_balances.md)
  - [`get_account_transfers`](./reference/operations/get_account_transfers.md)

## Dive Deeper

Learn more about TigerBeetle's [mission, history](./about/README.md),
[architecture](./about/architecture.md), approach to [safety](./about/safety.md) and
[performance](./about/performance.md) in the [About section](./about/README.md).

## Contributing

- [Watch our talks, listen to our podcasts](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TALKS.md)
- [Read HACKING.md](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/HACKING.md)
- And explore internal READMEs within the [TigerBeetle src directory](https://github.com/tigerbeetle/tigerbeetle/tree/main/src)

## Community

- [𝕏](https://twitter.com/tigerbeetledb)
- [GitHub](https://github.com/tigerbeetle/tigerbeetle)
- [Slack](https://slack.tigerbeetle.com/invite)
- [Monthly Newsletter](https://mailchi.mp/8e9fa0f36056/subscribe-to-tigerbeetle)
- [YouTube](https://www.youtube.com/@tigerbeetledb)
