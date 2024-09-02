# Overview

## Images

The system test, running in Antithesis, consists of 3 images:

- `replica`: A TigerBeetle replica.
- `workload`:
    - Generate messages (create/fetch account/transfer) in a loop
    - Check results for basic problems
- `config`: Package the `docker-compose.yaml` and any additional environment configuration for Antithesis.

## Resources

- [Antithesis documentation](https://antithesis.com/docs/index.html)
- [instrumentation.h](https://drive.google.com/file/d/1D7FPHL54znblGol4vMw8uwMFpLkaOePX/view) - currently not used.

# Usage

### Local Test

To test locally (run from `tigerbeetle/`):

```bash
zig build system_test_workload install
./tools/system_test/scripts/build.sh <tag>

# Run containers.
cd tools/system_test/config/
docker-compose up
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
./scripts/push.sh <tag>
```
