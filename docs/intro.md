---
slug: /
sidebar_position: 1
---

# TigerBeetle Docs

TigerBeetle is a distributed financial accounting database designed
for mission critical safety and performance.

## Quickstart

First, get TigerBeetle running:

* [Run a single-node cluster with a single binary](./quick-start/single-binary.md)
* Or [run a three-node cluster with a single binary](./quick-start/single-binary-three.md)
* Or [run a single-node cluster with Docker](./quick-start/with-docker.md)
* Or [run a three-node cluster with docker-compose](./quick-start/with-docker-compose.md)

Then, try creating accounts and transfers [using the TigerBeetle CLI client](./quick-start/cli-client.md).

## Designing for TigerBeetle

When integrating TigerBeetle into your project, consider the
following:

* [TigerBeetle and consistency](./design/consistency.md)
* [Modeling application data within TigerBeetle](./design/data-modeling.md)
* [How two-phase transfers work](./design/two-phase-transfers.md)
* [How client sessions work](./design/client-sessions.md)
* [How client requests work](./design/client-requests.md)

## Recipes

Here are some common use-cases for TigerBeetle:

* [Closing accounts](./recipes/close-account.md)
* [Currency exchange](./recipes/currency-exchange.md)
* [Fractional amounts](./recipes/fractional-amounts.md)

## Client libraries

We officially support the following libraries for communicating with a
TigerBeetle cluster:

* [.NET](/src/clients/dotnet/README.md)
* [Go](/src/clients/go/README.md)
* [Java](/src/clients/java/README.md)
* [Node.js](/src/clients/node/README.md)

### Sample code

Within each client library README you will find links to sample code.

## Reference

To understand TigerBeetle's data model, see:

* [Accounts](./reference/accounts.md)
* [Transfers](./reference/transfers.md)
* And [Operations](./reference/operations/index.md)
  - [`create_accounts`](./reference/operations/create_accounts.md)
  - [`create_transfers`](./reference/operations/create_transfers.md)
  - [`lookup_accounts`](./reference/operations/lookup_accounts.md)
  - [`lookup_transfers`](./reference/operations/lookup_transfers.md)

## Contributing

* [Watch our talks, listen to our podcasts](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TALKS.md)
* [Read HISTORY.md](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/HISTORY.md)
* [Read DESIGN.md](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/DESIGN.md)
* [Read DEEP_DIVE.md](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/DEEP_DIVE.md)
* [Read TIGER_STYLE.md](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md)
* [Read HACKING.md](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/HACKING.md)
* And explore internal READMEs within the [TigerBeetle src directory](https://github.com/tigerbeetle/tigerbeetle/tree/main/src)

## Community

* [Twitter](https://twitter.com/tigerbeetledb)
* [GitHub](https://github.com/tigerbeetle/tigerbeetle)
* [Slack](https://join.slack.com/t/tigerbeetle/shared_invite/zt-1gf3qnvkz-GwkosudMCM3KGbGiSu87RQ)
* [Monthly Newsletter](https://mailchi.mp/8e9fa0f36056/subscribe-to-tigerbeetle)
* [YouTube](https://www.youtube.com/@tigerbeetledb)
