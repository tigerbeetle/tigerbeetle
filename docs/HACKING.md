# Hacking on TigerBeetle

**Prerequisites:** The current beta version of TigerBeetle targets macOS and Linux and takes advantage of the latest asynchronous IO capabilities of the Linux kernel v5.6 and newer, via [io_uring](https://kernel.dk/io_uring.pdf). As such it can only be used on macOS or on recent versions of Linux with an updated kernel.

## Setup

First grab the sources and run the setup script:

```bash
$ git clone https://github.com/tigerbeetledb/tigerbeetle.git
$ cd tigerbeetle
$ scripts/install.sh
```

## Benchmark

With TigerBeetle installed, you are ready to benchmark!

```bash
scripts/benchmark.sh
```

*If you encounter any benchmark errors, please send us the resulting `benchmark.log`.*

## Running the server

Launch a TigerBeetle cluster on your local machine by running each of these commands in a new terminal tab:

```
$ ./tigerbeetle format --cluster=0 --replica=0 0_0.tigerbeetle
$ ./tigerbeetle format --cluster=0 --replica=1 0_1.tigerbeetle
$ ./tigerbeetle format --cluster=0 --replica=2 0_2.tigerbeetle

$ ./tigerbeetle start --addresses=3001,3002,3003 0_0.tigerbeetle
$ ./tigerbeetle start --addresses=3001,3002,3003 0_1.tigerbeetle
$ ./tigerbeetle start --addresses=3001,3002,3003 0_2.tigerbeetle
```

Run the TigerBeetle binary to see all command line arguments:

```bash
./tigerbeetle --help
```

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
