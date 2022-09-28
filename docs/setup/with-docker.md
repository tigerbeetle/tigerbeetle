---
sidebar_position: 2
---

# Single-node cluster with Docker

First provision TigerBeetle's data directory.

```bash
$ docker run -v $(pwd)/data:/data ghcr.io/coilhq/tigerbeetle \
    format --cluster=0 --replica=0 /data/0_0.tigerbeetle
info(io): creating "0_0.tigerbeetle"...
info(io): allocating 660.140625MiB...
```

Then run the server.

```bash
$ docker run -p 3000:3000 -v $(pwd)/data:/data ghcr.io/coilhq/tigerbeetle \
    start --addresses=0.0.0.0:3000 /data/0_0.tigerbeetle
info(io): opening "0_0.tigerbeetle"...
info(main): 0: cluster=0: listening on 0.0.0.0:3000
```

Now you can connect to the running server with any client. For a quick
start, try [creating accounts and transfers in the Node
CLI](../usage/node-cli).
