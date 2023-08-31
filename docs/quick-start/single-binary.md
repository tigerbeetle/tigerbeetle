---
sidebar_position: 1
---

# Single-node cluster with a single binary

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

Now create the TigerBeetle data file.

```console
./tigerbeetle format --cluster=0 --replica=0 --replica-count=1 0_0.tigerbeetle
```
```console
info(io): creating "0_0.tigerbeetle"...
info(io): allocating 660.140625MiB...
```

And start the server.

```console
./tigerbeetle start --addresses=3000 0_0.tigerbeetle
```
```console
info(io): opening "0_0.tigerbeetle"...
info(main): 0: cluster=0: listening on 127.0.0.1:3000
```

### Connect with the CLI

Now you can connect to the running server with any client. For a quick
start, try creating accounts and transfers [using the TigerBeetle CLI
client](./cli-client.md).
