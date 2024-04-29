# Getting Started

## 1. Download

You can find more options to [download TigerBeetle here](./download.md), but you can use the
bootstrap script to get started quickly:

### macOS/Linux

```shell
git clone https://github.com/tigerbeetle/tigerbeetle && cd tigerbeetle && ./bootstrap.sh
```

### Windows

```powershell
git clone https://github.com/tigerbeetle/tigerbeetle && cd tigerbeetle && .\bootstrap.ps1
```

## 2. Create and Run a Cluster

Try running a single-node or multi-node cluster using the prebuilt binary:

- [Single-node cluster](./single-binary.md)
- [Three-node cluster](./single-binary-three.md)

You can also run these same tutorials in [Docker](./docker/). However, it is not recommended.
TigerBeetle is distributed as a single binary application that should be easy to use directly on the
target system. Using Docker as an abstraction layer adds complexity without much benefit in this
case.

## 3. Create Accounts and Transfers with the CLI

When you have a cluster running, try creating some accounts and transfers
[using the TigerBeetle CLI](./repl.md).
