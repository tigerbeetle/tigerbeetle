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

# Usage

### Local Test

To test locally (run from repository root):

```bash
./src/testing/systest/scripts/build.sh <tag>

# Run containers.
cd src/testing/systest/config/
docker compose up
^C # shut it down when you're pleased
```

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
./src/testing/systest/scripts/push.sh <tag>
```
