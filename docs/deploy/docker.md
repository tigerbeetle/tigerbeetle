---
sidebar_position: 4
---

# Docker

TigerBeetle can be run using Docker. However, it is not recommended.

TigerBeetle is distributed as a single, small, statically-linked binary. It
should be easy to run directly on the target machine. Using Docker as an
abstraction adds complexity while providing relatively little in this case.

## Image

The Docker image is available from the Github Container Registry:

<https://github.com/tigerbeetle/tigerbeetle/pkgs/container/tigerbeetle>

## Troubleshooting

### `exited with code 137`

If you see this error without any logs from TigerBeetle, it is likely that the
Linux OOMKiller is killing the process. If you are running Docker inside a
virtual machine (such as is required on Docker or Podman for macOS), try
increasing the virtual machine memory limit.

Alternatively, in a development environment, you can lower the size of the cache
so TigerBeetle uses less memory. For example, set `--cache-grid=512MiB` when
running `tigerbeetle start`.

### Debugging panics

If TigerBeetle panics and you can reproduce the panic, you can get a better
stack trace by switching to a debug image (by using the `:debug` Docker image
tag).

```console
docker run -p 3000:3000 -v $(pwd)/data:/data ghcr.io/tigerbeetle/tigerbeetle:debug \
    start --addresses=0.0.0.0:3000 /data/0_0.tigerbeetle
```

### On MacOS

#### `error: SystemResources`

If you get `error: SystemResources` when running TigerBeetle in Docker on macOS,
you will need to do one of the following:

1. Run `docker run` with `--cap-add IPC_LOCK`
2. Run `docker run` with `--ulimit memlock=-1:-1`
3. Or modify the defaults in `$HOME/.docker/daemon.json` and restart the Docker
   for Mac application:

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

If you are running TigerBeetle with Docker Compose, you will need to add the
`IPC_LOCK` capability like this:

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
