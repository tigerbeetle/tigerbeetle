# tiger-beetle

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

Our research led us to the thesis that, while there are mechanisms available to shard account balances to parallelize updates and improve performance, there is still a lot of performance gains to be had in designing a database that is purpose built for storing balances and processing updates in a reliable and robust manner.

In the month of July 2020, we developed a prototype of TigerBeetle in Node as a performance sketch to measure the basic components of the design (batching, TCP protocol, cryptographic checksums everywhere, fsync journalling, in-memory business logic and hash table operations). **ProtoBeetle ran at 200,000 transfers a second on our office laptops**, supporting our back-of-the-envelope numbers.

We then integrated ProtoBeetle into Mojaloop and our reference minimum deployment cluster of **Mojaloop went from 76 TPS on MySQL to 1757 TPS on ProtoBeetle**. A single stateless Mojaloop pod was unable to saturate ProtoBeetle. Most of the throughput was spent converting Mojaloop's individual HTTP requests into TCP batches.

**TigerBeetle is now under active development** in C/Zig and the [DESIGN](./DESIGN.md) document is intended as a design document showing our design decisions regarding performance and safety, and where we want to go regarding accounting features.

## Usage

> More instructions to come soon

TigerBeetle takes advantage of the latest asynchronous IO capabilities of the Linux kernel v5.8 and newer, io_uring. As such it can only be used on recent versions of Linux or on machines with an updated kernel.

For details of how to upgrade the kernel on Ubuntu see [this document](./UPGRADE_UBUNTU_KERNEL.md).

## Progress

- [client](./client) is a TigerBeetle client written in Typescript
- [io_uring.zig](./io_uring.zig) is a Zig implementation of io_uring which has been [submitted](https://github.com/ziglang/zig/pull/6356) for addition to the Zig standard library
- [demos/io_uring](./demos/io_uring) are some demos of the io_uring implementation
- [proto-beetle](./proto-beetle) is the initial prototype of TigerBeetle written entirely in Javascript