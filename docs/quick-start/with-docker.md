---
sidebar_position: 3
---

# Single-node cluster with Docker

First provision TigerBeetle's data directory.

```console
docker run -v $(pwd)/data:/data ghcr.io/tigerbeetle/tigerbeetle \
    format --cluster=0 --replica=0 --replica-count=1 /data/0_0.tigerbeetle
```
```console
info(io): creating "0_0.tigerbeetle"...
info(io): allocating 660.140625MiB...
```

Then run the server.

```console
docker run -p 3000:3000 -v $(pwd)/data:/data ghcr.io/tigerbeetle/tigerbeetle \
    start --addresses=0.0.0.0:3000 /data/0_0.tigerbeetle
```
```console
info(io): opening "0_0.tigerbeetle"...
info(main): 0: cluster=0: listening on 0.0.0.0:3000
```

### Connect with the CLI

Now you can connect to the running server with any client. For a quick
start, try creating accounts and transfers [using the TigerBeetle CLI
client](./cli-client.md).

## `error: SystemResources` on macOS

If you get `error: SystemResources` when running TigerBeetle in Docker
on macOS, you will need to do one of the following:

1. Run `docker run` with `--cap-add IPC_LOCK`
2. Run `docker run` with `--ulimit memlock=-1:-1`
3. Or modify the defaults in `$HOME/.docker/daemon.json` and restart the Docker for Mac application:

```json
{
  ... other settings ...
  "default-ulimits": {
    "memlock": {
      "Hard": -1,
      "Name": "memlock",
      "Soft": -1
    }
  },
  ... other settings ...
}
```

See https://github.com/tigerbeetle/tigerbeetle/issues/92 for discussion.

## Debugging panics

If TigerBeetle panics and you can reproduce the panic, you can get a
better stack trace by switching to a debug image (by using the `:debug`
Docker image tag).

```console
docker run -p 3000:3000 -v $(pwd)/data:/data ghcr.io/tigerbeetle/tigerbeetle:debug \
    start --addresses=0.0.0.0:3000 /data/0_0.tigerbeetle
```
