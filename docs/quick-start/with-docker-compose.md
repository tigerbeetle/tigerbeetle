---
sidebar_position: 4
---

# Three-node cluster with Docker Compose

First, provision the data file for each node:

```console
docker run -v $(pwd)/data:/data ghcr.io/tigerbeetle/tigerbeetle format --cluster=0 --replica=0 --replica-count=3 /data/0_0.tigerbeetle
docker run -v $(pwd)/data:/data ghcr.io/tigerbeetle/tigerbeetle format --cluster=0 --replica=1 --replica-count=3 /data/0_1.tigerbeetle
docker run -v $(pwd)/data:/data ghcr.io/tigerbeetle/tigerbeetle format --cluster=0 --replica=2 --replica-count=3 /data/0_2.tigerbeetle
```

Then create a docker-compose.yml file:

```docker-compose
version: "3.7"

##
# Note: this example might only work with linux + using `network_mode:host` because of 2 reasons:
#
# 1. When specifying an internal docker network, other containers are only available using dns based routing:
#    e.g. from tigerbeetle_0, the other replicas are available at `tigerbeetle_1:3002` and
#    `tigerbeetle_2:3003` respectively.
#
# 2. Tigerbeetle performs some validation of the ip address provided in the `--addresses` parameter
#    and won't let us specify a custom domain name.
#
# The workaround for now is to use `network_mode:host` in the containers instead of specifying our
# own internal docker network
##

services:
  tigerbeetle_0:
    image: ghcr.io/tigerbeetle/tigerbeetle
    command: "start --addresses=0.0.0.0:3001,0.0.0.0:3002,0.0.0.0:3003 /data/0_0.tigerbeetle"
    network_mode: host
    volumes:
      - ./data:/data

  tigerbeetle_1:
    image: ghcr.io/tigerbeetle/tigerbeetle
    command: "start --addresses=0.0.0.0:3001,0.0.0.0:3002,0.0.0.0:3003 /data/0_1.tigerbeetle"
    network_mode: host
    volumes:
      - ./data:/data

  tigerbeetle_2:
    image: ghcr.io/tigerbeetle/tigerbeetle
    command: "start --addresses=0.0.0.0:3001,0.0.0.0:3002,0.0.0.0:3003 /data/0_2.tigerbeetle"
    network_mode: host
    volumes:
      - ./data:/data
```

And run it:

```console
docker-compose up
```
```console
docker-compose up
Starting tigerbeetle_0   ... done
Starting tigerbeetle_2   ... done
Recreating tigerbeetle_1 ... done
Attaching to tigerbeetle_0, tigerbeetle_2, tigerbeetle_1
tigerbeetle_1    | info(io): opening "0_1.tigerbeetle"...
tigerbeetle_2    | info(io): opening "0_2.tigerbeetle"...
tigerbeetle_0    | info(io): opening "0_0.tigerbeetle"...
tigerbeetle_0    | info(main): 0: cluster=0: listening on 0.0.0.0:3001
tigerbeetle_2    | info(main): 2: cluster=0: listening on 0.0.0.0:3003
tigerbeetle_1    | info(main): 1: cluster=0: listening on 0.0.0.0:3002
tigerbeetle_0    | info(message_bus): connected to replica 1
tigerbeetle_0    | info(message_bus): connected to replica 2
tigerbeetle_1    | info(message_bus): connected to replica 2
tigerbeetle_1    | info(message_bus): connection from replica 0
tigerbeetle_2    | info(message_bus): connection from replica 0
tigerbeetle_2    | info(message_bus): connection from replica 1
tigerbeetle_0    | info(clock): 0: system time is 83ns ahead
tigerbeetle_2    | info(clock): 2: system time is 83ns ahead
tigerbeetle_1    | info(clock): 1: system time is 78ns ahead

... and so on ...
```

### Connect with the CLI

Now you can connect to the running server with any client. For a quick
start, try creating accounts and transfers [using the TigerBeetle CLI
client](./cli-client.md).

## `error: SystemResources` on macOS

If you get `error: SystemResources` when running TigerBeetle in Docker
on macOS, you will need to add the `IPC_LOCK` capability.

```yaml
... rest of docker-compose.yml ...

services:
  tigerbeetle_0:
    image: ghcr.io/tigerbeetle/tigerbeetle
    command: "start --addresses=0.0.0.0:3001,0.0.0.0:3002,0.0.0.0:3003 /data/0_0.tigerbeetle"
    network_mode: host
    cap_add:       # HERE
      - IPC_LOCK   # HERE
    volumes:
      - ./data:/data

... rest of docker-compose.yml ...
```

See https://github.com/tigerbeetle/tigerbeetle/issues/92 for discussion.

## `exited with code 137`

If you see this error without any logs from TigerBeetle, it is likely
that the Linux OOMKiller is killing the process. If you are running
Docker inside a virtual machine (such as is required on Docker or
Podman for macOS), try increasing the virtual machine memory limit.

Alternatively, in a development environment, you can lower the size of
the cache so TigerBeetle uses less memory. For example, set
`--cache-grid=512MB` when running `tigerbeetle start`.

## Debugging panics

If TigerBeetle panics and you can reproduce the panic, you can get a
better stack trace by switching to a debug image (by using the `:debug`
Docker image tag).

```bash
ghcr.io/tigerbeetle/tigerbeetle:debug
```
