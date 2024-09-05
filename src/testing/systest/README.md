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
```

The test is done when the `workload` container prints `workload done` and exits.
The `replica` and `api` containers do not exit.

Clean up afterwards:

```bash
docker-compose rm
```

### Push Containers

Push the containers to Antithesis (authorization must be already configured) - this happens automatically in CI:

```bash
./src/testing/systest/scripts/push.sh <tag>
```
