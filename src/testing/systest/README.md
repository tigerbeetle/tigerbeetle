# systest

## Images

The system test, running in Antithesis, consists of 3 images:

- `replica`: A TigerBeetle replica.
- `workload`:
    - Generate (possibly invalid) operations (create account/transfer) in a loop
    - Track some data about accounts after every successful operation
    - Query accounts and check basic consistency properties
- `config`: Package the `docker-compose.yaml` and any additional environment configuration for Antithesis.

## Resources

- [Antithesis documentation](https://antithesis.com/docs/index.html)
- [instrumentation.h](https://drive.google.com/file/d/1D7FPHL54znblGol4vMw8uwMFpLkaOePX/view) - currently not used.

## Usage

```bash
zig build scripts -- antithesis --tag=<tag>
```

### Local Test

In the output of the above build script, there's something like the following:

> To debug the docker compose config locally, run:
> 
>     cd /tmp/tmp.xxxxxxxxxx && TAG=<tag> docker compose up

Run that command to locally start the systest using docker compose.

Clean up afterwards:

```bash
docker compose rm
```

### Push Containers

Authorization must be configured first:

```bash
cat /some/path/to/tigerbeetle.key.json | docker login -u _json_key https://us-central1-docker.pkg.dev/ --password-stdin
```

Push the containers to Antithesis:

```bash
zig build scripts -- antithesis --tag=<tag> --push
```
