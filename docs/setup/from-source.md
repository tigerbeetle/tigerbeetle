---
sidebar_position: 3
---

# Single-node cluster from source

To build from source, clone the repo and run the install script.

```bash
$ git clone https://github.com/tigerbeetledb/tigerbeetle.git
$ cd tigerbeetle
$ scripts/install.sh
```

Don't worry, this will only make changes within the `tigerbeetle`
directory. No global changes.

Then create the TigerBeetle data file.

```bash
$ ./tigerbeetle format --cluster=0 --replica=0 0_0.tigerbeetle
info(io): creating "0_0.tigerbeetle"...
info(io): allocating 660.140625MiB...
```

And start the server.

```bash
$ ./tigerbeetle start --addresses=3000 0_0.tigerbeetle
info(io): opening "0_0.tigerbeetle"...
info(main): 0: cluster=0: listening on 127.0.0.1:3000
```

Now you can connect to the running server with any client. For a quick
start, try [creating accounts and transfers in the Node
CLI](../usage/node-cli).
