---
sidebar_position: 2
---

# Three-node cluster with a single binary

First, download a prebuilt copy of TigerBeetle.

On macOS/Linux:

```console
git clone https://github.com/tigerbeetle/tigerbeetle; ./tigerbeetle/bootstrap.sh
```

On Windows:

```console
git clone https://github.com/tigerbeetle/tigerbeetle; .\tigerbeetle\bootstrap.ps1
```

Want to build from source locally? Add `-build` as an argument to the bootstrap script.

## Running TigerBeetle

Now create the TigerBeetle data file for each replica

```console
./tigerbeetle format --cluster=0 --replica=0 --replica-count=3 0_0.tigerbeetle
./tigerbeetle format --cluster=0 --replica=1 --replica-count=3 0_1.tigerbeetle
./tigerbeetle format --cluster=0 --replica=2 --replica-count=3 0_2.tigerbeetle
```

And start each server in a new terminal window:

```console
./tigerbeetle start --addresses=3000,3001,3002 0_0.tigerbeetle
```

```console
./tigerbeetle start --addresses=3000,3001,3002 0_1.tigerbeetle
```

```console
./tigerbeetle start --addresses=3000,3001,3002 0_2.tigerbeetle
```

TigerBeetle uses the `--replica` that's stored in the data file as an index into the `--addresses`
provided.

### Connect with the CLI

Now you can connect to the running server with any client. For a quick
start, try creating accounts and transfers [using the TigerBeetle CLI
client](./cli-client.md).
