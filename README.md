# tigerbeetle

*TigerBeetle is a financial accounting database designed for mission critical safety and performance to power the future of financial services.*

**Take part in TigerBeetle's $20k consensus challenge: [Viewstamped Replication Made Famous](https://github.com/coilhq/viewstamped-replication-made-famous)**

Watch an introduction to TigerBeetle on [Zig SHOWTIME](https://www.youtube.com/watch?v=BH2jvJ74npM) for our design decisions regarding performance, safety, and financial accounting primitives:

[![A million financial transactions per second in Zig](https://img.youtube.com/vi/BH2jvJ74npM/0.jpg)](https://www.youtube.com/watch?v=BH2jvJ74npM)

Read more about the [history](./docs/HISTORY.md) of TigerBeetle, the problem of balance tracking at scale and the solution of a purpose-built financial accounting database.

## TigerBeetle (under active development)

The production version of **TigerBeetle is now under active development**. Our [DESIGN doc](docs/DESIGN.md) provides an overview of TigerBeetle's data structures and our [project board](https://github.com/coilhq/tigerbeetle/projects?type=classic) provides a glimpse of where we want to go.

## QuickStart

**Prerequisites:** The current beta version of TigerBeetle targets macOS and Linux and takes advantage of the latest asynchronous IO capabilities of the Linux kernel v5.6 and newer, via [io_uring](https://kernel.dk/io_uring.pdf). As such it can only be used on macOS or on recent versions of Linux with an updated kernel.

```bash
git clone https://github.com/coilhq/tigerbeetle.git
cd tigerbeetle
scripts/install.sh
```

## Launch a Local Cluster

Launch a TigerBeetle cluster on your local machine by running each of these commands in a new terminal tab:

```
./tigerbeetle format --cluster=0 --replica=0 0_0.tigerbeetle
./tigerbeetle format --cluster=0 --replica=1 0_1.tigerbeetle
./tigerbeetle format --cluster=0 --replica=2 0_2.tigerbeetle

./tigerbeetle start --addresses=3001,3002,3003 0_0.tigerbeetle
./tigerbeetle start --addresses=3001,3002,3003 0_1.tigerbeetle
./tigerbeetle start --addresses=3001,3002,3003 0_2.tigerbeetle
```

Run the TigerBeetle binary to see all command line arguments:

```bash
./tigerbeetle --help
```

## Clients

* [tigerbeetle-node](https://github.com/coilhq/tigerbeetle-node) is a TigerBeetle Node.js client written in TypeScript (and Zig with [Node's N-API](https://nodejs.org/api/n-api.html) for ABI stability).

* [client.zig](./src/vr/client.zig) is a TigerBeetle Zig client.

* [demo.zig](./src/demo.zig) is a lightweight TigerBeetle client for demonstration purposes only, which we used to create [six demos you can work your way through and modify](./docs/DEEP_DIVE.md) to explore TigerBeetle's commands.

## Community

[Join the TigerBeetle community in Discord.](https://discord.com/invite/uWCGp46uG5)

## Benchmark

With TigerBeetle installed, you are ready to benchmark!

```bash
scripts/benchmark.sh
```

*If you encounter any benchmark errors, please send us the resulting `benchmark.log`.*

## Tests

### Unit Tests

To run the unit tests:

```bash
zig/zig build test
```

The [QuickStart](#quickstart) step above will install Zig for you to the root of the `tigerbeetle` directory.

### Simulation Tests

To run TigerBeetle's long-running simulation, called *The VOPR*:

```bash
zig/zig build vopr
```

Pass the `--send` flag to the VOPR to report discovered bugs to the [VOPR Hub](src/vopr_hub/README.md). The VOPR Hub will automatically replay, deduplicate, and create GitHub issues as needed.

```bash
zig/zig build vopr -- --send
```

Run the VOPR using a specific seed. This will run in `Debug` mode by default but you can also include `--build-mode` to run in ReleaseSafe mode.

```bash
zig/zig build vopr -- --seed=123 --build-mode=ReleaseSafe
```

To view all the available command line arguments simply use the `--help` flag.

```bash
zig/zig build vopr -- --help
```

*The VOPR* stands for *The Viewstamped Operation Replicator* and was inspired by the movie WarGames, by our love of fuzzing over the years, by [Dropbox's Nucleus testing](https://dropbox.tech/infrastructure/-testing-our-new-sync-engine), and by [FoundationDB's deterministic simulation testing](https://www.youtube.com/watch?v=OJb8A6h9jQQ).

*The VOPR* is [a deterministic simulator](src/simulator.zig) that can fuzz many clusters of TigerBeetle servers and clients interacting through TigerBeetle's Viewstamped Replication consensus protocol, but all within a single developer machine process, with [a network simulator](src/test/packet_simulator.zig) to simulate all kinds of network faults, and with an in-memory [storage simulator](src/test/storage.zig) to simulate all kinds of storage faults, to explore and test TigerBeetle against huge state spaces in a short amount of time, by literally speeding up the passing of time within the simulation itself.

Beyond being a deterministic simulator, *The VOPR* also features [a state checker](src/test/state_checker.zig) that can hook into all the replicas, and check all their state transitions the instant they take place, using cryptographic hash chaining to prove causality and check that all interim state transitions are valid, based on any of the set of inflight client requests at the time, without divergent states, and then check for convergence to the highest state at the end of the simulation, to distinguish between correctness or liveness bugs.

Check out TigerBeetle's [Viewstamped Replication Made Famous](https://github.com/coilhq/viewstamped-replication-made-famous#how-can-i-run-the-implementation-how-many-batteries-are-included-do-you-mean-i-can-even-run-the-vopr) bug bounty challenge repository for more details on how to run *The VOPR* and interpret its output.

## Performance Demos

Along the way, we also put together a series of performance demos and sketches to get you comfortable building TigerBeetle, show how low-level code can sometimes be easier than high-level code, help you understand some of the key components within TigerBeetle, and enable back-of-the-envelope calculations to motivate design decisions.

You may be interested in:

* [demos/protobeetle](./demos/protobeetle), how batching changes everything.
* [demos/bitcast](./demos/bitcast), how Zig makes zero-overhead network deserialization easy, fast and safe.
* [demos/io_uring](./demos/io_uring), how ring buffers can eliminate kernel syscalls, reduce server hardware requirements by a factor of two, and change the way we think about event loops.
* [demos/hash_table](./demos/hash_table), how linear probing compares with cuckoo probing, and what we look for in a hash table that needs to scale to millions (and billions) of account transfers.

## License

Copyright 2020-2022 Coil Technologies, Inc

Licensed under the Apache License, Version 2.0 (the "License"); you may not use these files except in compliance with the License. You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
