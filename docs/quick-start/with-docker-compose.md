---
sidebar_position: 4
---

# Three-node cluster with Docker Compose

First, create the following `tigerbeetle-entrypoint.sh` file:

```sh
#!/bin/sh

##
# This is used as an entrypoint script in an init container.
# It provisions data files if they don't already exist, based on a few flags.
#
# | Flag | Accepts  | Comment                                      |
# | ---- | -------- | -------------------------------------------- |
# | `-f` | `string` | The directory to store the data files within |
# | `-c` | `number` | The cluster number                           |
# | `-r` | `number` | The amount of replicas                       |
#
# The files generated are named using a template "{cluster}_{replica}.tigerbeetle".
#
# When the script is executed with "-f /data -c 0 -r 3" flags, it generates 3 data files.
#
# /data/
# ├─ 0_0.tigerbeetle
# ├─ 0_1.tigerbeetle
# ├─ 0_2.tigerbeetle
##

while getopts "f:c:r:" opt; do
  case $opt in
    f)
      folder="$OPTARG"
      ;;
    c)
      cluster="$OPTARG"
      ;;
    r)
      replica_count="$OPTARG"
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
  esac
done

if [ -n "$folder" ]; then
    if [ ! -d "$folder" ]; then
        echo "The folder $folder does not exist."
        exit 1
    fi
else
    echo "Option -f is required."
    exit 1
fi

if [ -z "$cluster" ]; then
  echo "Option -c is required."
  exit 1
fi

if [ -z "$replica_count" ]; then
  echo "Option -r is required."
  exit 1
fi

counter=0
while [ "$counter" -lt "$replica_count" ]; do
  
  target="${folder}/${cluster}_${counter}.tigerbeetle"
  
  if [ -e "$target" ]; then
    echo "$target already exists"
  else
    /tigerbeetle format --cluster="$cluster" --replica="$counter" --replica-count="$replica_count" "$target"
  fi
  
  counter=$((counter + 1))
done
```

Then create a `docker-compose.yml` file:

```yaml
version: "3.7"

services:
  # this the init container that runs the entrypoint script.
  tigerbeetle-init:
    image: ghcr.io/tigerbeetle/tigerbeetle
    volumes:
      - tigerbeetle-data:/data
      - ./tigerbeetle-entrypoint.sh:/tigerbeetle-entrypoint.sh
    entrypoint: /tigerbeetle-entrypoint.sh -f /data -c 0 -r 3

  0_0-tigerbeetle:
    image: ghcr.io/tigerbeetle/tigerbeetle
    volumes:
      - tigerbeetle-data:/data
    # make sure to add additional addresses to the --addresses flag, if you add more nodes
    # also notice the data file specified is "/data/0_0.tigerbeetle"
    command: "start --addresses=172.16.100.10:3000,172.16.100.20:3000,172.16.100.30:3000 /data/0_0.tigerbeetle"
    networks:
      tigerbeetle:
        # the specific ip for this node
        ipv4_address: 172.16.100.10
    depends_on:
      tigerbeetle-init:
        # this condition ensures that this instance only starts up, when the tigerbeetle-init container has finished
        condition: service_completed_successfully

  0_1-tigerbeetle:
    image: ghcr.io/tigerbeetle/tigerbeetle
    volumes:
      - tigerbeetle-data:/data
    # make sure to add additional addresses to the --addresses flag, if you add more nodes
    # also notice the data file specified is "/data/0_1.tigerbeetle"
    command: "start --addresses=172.16.100.10:3000,172.16.100.20:3000,172.16.100.30:3000 /data/0_1.tigerbeetle"
    networks:
      tigerbeetle:
        # the specific ip for this node
        ipv4_address: 172.16.100.20
    depends_on:
      tigerbeetle-init:
        # this condition ensures that this instance only starts up, when the tigerbeetle-init container has finished
        condition: service_completed_successfully

  0_2-tigerbeetle:
    image: ghcr.io/tigerbeetle/tigerbeetle
    volumes:
      - tigerbeetle-data:/data
    # make sure to add additional addresses to the --addresses flag, if you add more nodes
    # also notice the data file specified is "/data/0_2.tigerbeetle"
    command: "start --addresses=172.16.100.10:3000,172.16.100.20:3000,172.16.100.30:3000 /data/0_2.tigerbeetle"
    networks:
      tigerbeetle:
        # the specific ip for this node
        ipv4_address: 172.16.100.30
    depends_on:
      tigerbeetle-init:
        # this condition ensures that this instance only starts up, when the tigerbeetle-init container has finished
        condition: service_completed_successfully

volumes:
  # using a volume allows you to persist the data files
  tigerbeetle-data:


networks:
  tigerbeetle:
    ipam:
      config:
        # you may want to use a different CIDR subnet range
        - subnet: 172.16.100.0/24
```

And run it:

```console
docker-compose up
```

```console
docker compose up
[+] Running 7/7
 ✔ Network tigerbeetle-docker_tigerbeetle           Created                                                                                                                                       0.0s 
 ✔ Network tigerbeetle-docker_default               Created                                                                                                                                       0.0s 
 ✔ Volume "tigerbeetle-docker_tigerbeetle-data"     Created                                                                                                                                       0.0s 
 ✔ Container tigerbeetle-docker-tigerbeetle-init-1  Created                                                                                                                                       0.1s 
 ✔ Container tigerbeetle-docker-0_0-tigerbeetle-1   Created                                                                                                                                       0.1s 
 ✔ Container tigerbeetle-docker-0_1-tigerbeetle-1   Created                                                                                                                                       0.1s 
 ✔ Container tigerbeetle-docker-0_2-tigerbeetle-1   Created                                                                                                                                       0.1s 
Attaching to 0_0-tigerbeetle-1, 0_1-tigerbeetle-1, 0_2-tigerbeetle-1, tigerbeetle-init-1
tigerbeetle-init-1  | info(io): creating "0_0.tigerbeetle"...
tigerbeetle-init-1  | info(io): allocating 1.0322265625GiB...
tigerbeetle-init-1  | info(main): 0: formatted: cluster=0 replica_count=3
tigerbeetle-init-1  | info(io): creating "0_1.tigerbeetle"...
tigerbeetle-init-1  | info(io): allocating 1.0322265625GiB...
tigerbeetle-init-1  | info(main): 1: formatted: cluster=0 replica_count=3
tigerbeetle-init-1  | info(io): creating "0_2.tigerbeetle"...
tigerbeetle-init-1  | info(io): allocating 1.0322265625GiB...
tigerbeetle-init-1  | info(main): 2: formatted: cluster=0 replica_count=3
0_0-tigerbeetle-1   | info(io): opening "0_0.tigerbeetle"...
0_1-tigerbeetle-1   | info(io): opening "0_1.tigerbeetle"...
0_2-tigerbeetle-1   | info(io): opening "0_2.tigerbeetle"...
0_0-tigerbeetle-1   | info(main): 0: cluster=0: listening on 172.16.100.10:3000
0_1-tigerbeetle-1   | info(main): 1: cluster=0: listening on 172.16.100.20:3000
0_2-tigerbeetle-1   | info(main): 2: cluster=0: listening on 172.16.100.30:3000
0_0-tigerbeetle-1   | info(message_bus): connected to replica 1
0_1-tigerbeetle-1   | info(message_bus): connection from replica 0
0_0-tigerbeetle-1   | info(message_bus): connected to replica 2
0_2-tigerbeetle-1   | info(message_bus): connection from replica 0
0_1-tigerbeetle-1   | info(message_bus): connected to replica 2
0_2-tigerbeetle-1   | info(message_bus): connection from replica 1

... and so on ...
```

### Connect with the CLI

Now you can connect to the running server with any client. For a quick
start, try creating accounts and transfers [using the TigerBeetle CLI
client](./cli-repl.md).

## `error: SystemResources` on macOS

If you get `error: SystemResources` when running TigerBeetle in Docker
on macOS, you will need to add the `IPC_LOCK` capability.

```yaml
... rest of docker-compose.yml ...

  0_0-tigerbeetle:
    image: ghcr.io/tigerbeetle/tigerbeetle
    volumes:
      - tigerbeetle-data:/data
    command: "start --addresses=172.16.100.10:3000,172.16.100.20:3000,172.16.100.30:3000 /data/0_0.tigerbeetle"
    networks:
      tigerbeetle:
        ipv4_address: 172.16.100.10
    cap_add:       # HERE
      - IPC_LOCK   # HERE
    depends_on:
      tigerbeetle-init:
        condition: service_completed_successfully

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
