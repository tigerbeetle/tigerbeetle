# Overview
### Images

The Antithesis test consists of 4 images:

- `replica`: A TigerBeetle replica.
- `api`
    - Runs a `MessageBusReplica` to receive messages from the `workload`.
    - Runs a TigerBeetle `Client` to send messages (from the `workload`) to `replica`s.
- `workload`:
    - Generate messages (create/fetch account/transfer).
    - Forward the messages to the `api` services using `Client` instances.
    - To conclude the test, verify that all account balances match the expected values.
- `config`: Package the `docker-compose.yaml` and any additional environment configuration for Antithesis.

### Resources

- [Antithesis documentation](https://antithesis.com/docs/index.html)
- [instrumentation.h](https://drive.google.com/file/d/1D7FPHL54znblGol4vMw8uwMFpLkaOePX/view) - currently not used.

### Faults

- Faults are injected in the network between `replica` and `replica`.
- Faults are injected in the network between `replica` and `api`.
- Faults are _not_ injected in the network between `api` and `workload`.
- The `replica` and `api` containers may be killed and restarted.
- The `workload` container may _not_ be killed and restarted.

### Antithesis notes

- Containers currently have a 5GB memory limit.
- Supported faults:
    - Partition network: split all nodes into 2 groups.
    - Swizzle network: target 1 node, partition it from the rest of the cluster.
    - Pause, kill, and then restart containers.
    - (Antithesis does not currently support disk fault injection).
- Partitions
  - Probabilities of partitions vary. During partitioning events, links between groups will be affected.
  - Antithesis doesn't support asymmetric partitions. (Possibly supported in the future).
  - For any links that are affected, 3 events that can happen:
      - drop packets
      - queue packets then send all
      - slow the packets (change latency)
- Containers with an IP address ending in 129 or above are exempt from being killed.
- Connections between containers with IPs ending in 129 or above are exempt from network faults.
- `libvoidstar.so` requires glibc v2.17 or later
- Antithesis recommends not enabling the LLVM instrumentation on the `workload`, to allow them to focus on the `api` and `replica` containers.
- A custom patch (`zig.patch`) is required to enable LLVM instrumentation. A custom Zig compiler is built automatically with this in CI.

# Usage

### Local Test

To test locally (run from `tigerbeetle/`):

```bash
# This builds without libvoidstar linked in. Libvoidstar has trace_pc_guard hooks that are used by Antithesis when running
# on their Hypervisor. To enable libvoidstar linking, pass `-Dantithesis` to `zig build`.
# Build and tag the containers.
zig build antithesis_workload antithesis_api install
./tools/antithesis/scripts/build.sh <tag>

# Run containers.
cd tools/antithesis/config/
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
