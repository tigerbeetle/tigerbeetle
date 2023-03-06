---
sidebar_position: 3
---

# 3-node cluster with Docker Compose

First, provision the data file for each node:

```
$ docker run -v $(pwd)/data:/data ghcr.io/tigerbeetledb/tigerbeetle format --cluster=0 --replica=0 --replica-count=3 /data/0_0.tigerbeetle
$ docker run -v $(pwd)/data:/data ghcr.io/tigerbeetledb/tigerbeetle format --cluster=0 --replica=1 --replica-count=3 /data/0_1.tigerbeetle
$ docker run -v $(pwd)/data:/data ghcr.io/tigerbeetledb/tigerbeetle format --cluster=0 --replica=2 --replica-count=3 /data/0_2.tigerbeetle
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
    image: ghcr.io/tigerbeetledb/tigerbeetle
    command: "start --addresses=0.0.0.0:3001,0.0.0.0:3002,0.0.0.0:3003 /data/0_0.tigerbeetle"
    network_mode: host
    volumes:
      - ./data:/data

  tigerbeetle_1:
    image: ghcr.io/tigerbeetledb/tigerbeetle
    command: "start --addresses=0.0.0.0:3001,0.0.0.0:3002,0.0.0.0:3003 /data/0_1.tigerbeetle"
    network_mode: host
    volumes:
      - ./data:/data

  tigerbeetle_2:
    image: ghcr.io/tigerbeetledb/tigerbeetle
    command: "start --addresses=0.0.0.0:3001,0.0.0.0:3002,0.0.0.0:3003 /data/0_2.tigerbeetle"
    network_mode: host
    volumes:
      - ./data:/data
```

And run it:

```
$ docker-compose up
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


Now you can connect to the running server with any client. For a quick
start, try [creating accounts and transfers in the Node
CLI](../usage/node-cli).

## `error: SystemResources` on macOS

If you get `error: SystemResources` when running TigerBeetle in Docker
on macOS, you will need to add the `IPC_LOCK` capability.

```yaml
... rest of docker-compose.yml ...

services:
  tigerbeetle_0:
    image: ghcr.io/tigerbeetledb/tigerbeetle
    command: "start --addresses=0.0.0.0:3001,0.0.0.0:3002,0.0.0.0:3003 /data/0_0.tigerbeetle"
    network_mode: host
    cap_add:       # HERE
      - IPC_LOCK   # HERE
    volumes:
      - ./data:/data

... rest of docker-compose.yml ...
```

See https://github.com/tigerbeetledb/tigerbeetle/issues/92 for discussion.

## Debugging panics

If TigerBeetle panics and you can reproduce the panic, you can get a
better stack trace by switching to a debug image (by using the `:debug`
Docker image tag).

```bash
ghcr.io/tigerbeetledb/tigerbeetle:debug
```
