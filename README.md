# tigerbeetle

*TigerBeetle is a purpose-built accounting database, designed for high-throughput low-latency two-phase prepare/commit transfers between accounts.*

## The Problem - Realtime Processing of Balance Updates

Processing events that impact the balance on an account must be done serially, in the correct order and reliably. Despite the move to distribute work across parallel processes/threads in modern applications, it is not easy to horizontally scale a ledger or accounting system because of this.

The workflow for processing a balance update event is:
1. Validate the event
2. Process any business rules that must consider the current balance before processing the event and the new balance after the event
3. Update the balance
4. Persist the updated balance
5. Notify subscribers of the updated balance/ that the event has been processed

While investigating the options available to handle this it became clear that the majority of architectures cobble together existing generic data stores (relational or NoSQL data stores) with business logic enforced in the application code. 

Further, these systems often achieve performance at the expense of reliability or robustness. This is a compromise that is unacceptable for a system-of-record for financial accounts.

## The Solution - A Purpose-Built Accounting Database

Our research led us to conclude that, while there are mechanisms available to shard account balances to parallelize updates and improve performance, there are still significant performance gains to be had in designing a database that is purpose built for storing balances and processing updates in a reliable and robust manner.

## ProtoBeetle - 200,000 transfers per second

In the month of July 2020, we developed a prototype of TigerBeetle in Node as a performance sketch to measure the basic components of the design (batching, TCP protocol, cryptographic checksums everywhere, fsync journalling, in-memory business logic and hash table operations). **ProtoBeetle ran at 200,000 transfers per second on our office laptops**, supporting our back-of-the-envelope numbers.

We then integrated ProtoBeetle into Mojaloop and our reference minimum deployment cluster of **Mojaloop went from 76 TPS on MySQL to 1757 TPS on ProtoBeetle**. A single stateless Mojaloop pod was unable to saturate ProtoBeetle. Most of the throughput was spent converting Mojaloop's individual HTTP requests into TCP batches.

**You can watch [our 10-minute talk introducing ProtoBeetle](https://youtu.be/QOC6PHFPtAM?t=324).**

## AlphaBeetle - 500,000 transfers per second

After ProtoBeetle, from September through October 2020, we knuckled down and rewrote TigerBeetle in C/Zig to create the alpha version of TigerBeetle, using io_uring as a foundation for fast I/O.

TigerBeetle's Zig implementation of io_uring was [submitted](https://github.com/ziglang/zig/pull/6356) for addition to the Zig standard library.

## BetaBeetle (under active development)

The beta version of **TigerBeetle is now under active development** and [our design document](./docs/DESIGN.md) details our design decisions regarding performance and safety, and where we want to go regarding accounting features.

## QuickStart

The current beta version of TigerBeetle targets Linux and takes advantage of the latest asynchronous IO capabilities of the Linux kernel v5.6 and newer, via io_uring. As such it can only be used on recent versions of Linux with an updated kernel.

Later portable versions of TigerBeetle may supplement io_uring with kqueue for macOS and FreeBSD support, or IOCP for Windows support.

Once you have [upgraded your kernel on Ubuntu](./docs/UPGRADE_UBUNTU_KERNEL.md) and [installed Zig](./docs/INSTALL_ZIG.md), you can launch the TigerBeetle server:

```bash
./tigerbeetle
```

## Benchmark

With a fresh running TigerBeetle server, you are ready to benchmark!

```bash
zig run src/benchmark.zig -O ReleaseSafe
```

After each run of the benchmark, you will need to delete TigerBeetle's `journal` data file and restart the server.

## Clients

* [client](./src/client) is a TigerBeetle client written in Typescript (pending a few updates to support recent network protocol changes).
* [toy.zig](./src/toy.zig) is a TigerBeetle client written in Zig (working).

## Performance Demos

Along the way, we have also put together a series of performance demos and sketches to get you comfortable building TigerBeetle, show how low-level code can sometimes be easier than high-level code, help you understand some of the key components within TigerBeetle, and enable back-of-the-envelope calculations to motivate design decisions.

You may be interested in:

* [demos/protobeetle](./demos/protobeetle), how batching changes everything.
* [demos/bitcast](./demos/bitcast), how Zig makes zero-overhead network deserialization easy, fast and safe.
* [demos/io_uring](./demos/io_uring), how ring buffers can eliminate kernel syscalls, reduce server hardware requirements by a factor of two, and change the way we think about event loops.
* [demos/hash_table](./demos/hash_table), how linear probing compares with cuckoo probing, and what we look for in a hash table that needs to scale to millions (and billions) of account transfers.
