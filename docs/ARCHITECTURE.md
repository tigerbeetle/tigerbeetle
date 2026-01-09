# TigerBeetle Architecture

This document is a technical overview of the internals of TigerBeetle.  It includes a problem
statement, overview, motivation for design decisions and list of references that have inspired us.

## Problem Statement

TigerBeetle is a database for workloads that:

- are very contended and parallelize/shard poorly
- consist mostly of writes
- need very high throughput and moderately low latency
- require strong consistency guarantees
- demand very high levels of durability.

At present, TigerBeetle is focused on financial transactions (transfers). Each transfer touches
two accounts. Accounts are Pareto-distributed which means a few accounts are responsible for the bulk of
the transfers. Multi-object transactions in combination with contention make traditional database
architecture inefficient. Locking the two accounts and then crossing the network to the
application to compute the balances' update is slow. Due to contention, this work also can't be
efficiently distributed across separate machines.

TigerBeetle implements accounting logic but the underlying state machine can be swapped. TigerBeetle
does double-entry bookkeeping but _can_ do anything that has the same requirements: safety and
performance under extreme contention.

## Overview

TigerBeetle is a distributed system. It is a cluster of six replicas. The state of each replica is
a single file on disk, called the data file (see [./data_file.md](./data_file.md)). Replicas use a
consensus protocol (see [./vsr.md](./vsr.md)) to ensure that their respective data files store
identical data.

Specifically, TigerBeetle is a replicated state machine. The ground state of the system is an
immutable, hash-chained, append-only log of prepares. Each prepare is a batch of 8 thousand
individual transfer objects. The primary replica:

- accepts requests from the clients
- decides on the order in which requests shall be processed
- converts the next request to a prepare
- appends the prepare to the Write Ahead Log (WAL), assigning it the next sequence number and
  adding a checksum "pointer" to the previous log entry
- replicates prepare across backups.

When a backup receives a prepare, it writes it to the WAL section of the data file and sends a
`prepare_ok` message to the primary. Once the primary receives a quorum of `prepare_ok` messages,
the prepare is considered committed. This means it can no longer be removed from or reordered in
the log.

Replicas execute committed prepares in sequence number order by applying a batch of transfers to
local state. Because all replicas start with the same (empty) state and the state transition function
is deterministic, the replicas arrive at the same state.

If a primary fails, the consensus algorithm proper ensures that a different replica becomes a
primary. The new primary correctly reconstructs the latest state of the log.

The derived state of the system is the append-only log of immutable transfers and the current
balances of all accounts. Past transfers are stored for idempotence. Physically on disk, the state
is stored as a collection (forest) of LSM trees ([./lsm.md](./lsm.md)). Each LSM tree is a sorted
collection of objects. For example, transfer objects are stored in a tree sorted by a unique
timestamp which allows for efficient lookup. Auxiliary index trees are used to speed up other kinds
of lookups. For example, there's a tree which stores a tuple of each transfer's debit account id and
transfer's timestamp sorted by account id. This tree allows looking up all the timestamps for
transfers from a specific account. Knowing the timestamp, it is then possible to retrieve the
transfer object itself.

The forest of LSM trees is implemented as an on-disk functional data structure. The data is
organized as a tree of on-disk blocks (a block is 0.5 MiB in size). Blocks refer to other blocks by
their on-disk address and checksum of their content. From a root block, called the superblock,
the rest of the state is reachable.

When applying prepares to local state, replicas don't overwrite any existing blocks in the data
file and instead write new blocks only. This maintains the state of the superblock in memory. Once in
a while, the superblock is atomically flushed to storage forming a new checkpoint. When a replica
crashes and restarts it loses access to its previous in-memory superblock. It reads the previous
checkpoint/superblock from storage and reconstructs the lost state by replaying the suffix of the
log of prepares after that checkpoint. Determinism guarantees that the replica ends up in the exact
same state.

TigerBeetle assumes that replica's storage can fail. If a replica writes a prepare to the WAL or a
block of an LSM trees and the corresponding `fsync` returns `0` it could still be the case that when
reading this data later it will be found to be corrupted. Given that the system is already replicated
for high-availability, it would be wasteful not to use redundancy to repair local storage failures. So
this is exactly what TigerBeetle does.

## Design Decisions

### Intentionality in the Design

A lot of software is designed primarily via empirical feedback loop. You apply a small change to the
software, judge the results and then revert or double down. For TigerBeetle, we are trying something
different. We think from the first principles what the right solution _should_ be then use
"experiments" to confirm or disprove the mental model.

For example, our [TigerStyle](../TIGER_STYLE.md) is an explicitly engineered engineering process.

### Systems Thinking

TigerBeetle is designed to be a part of a larger data processing system. What happens outside of
TigerBeetle is as important as what's inside:

- Each transfer carries an end-to-end idempotency key: a unique 128-bit ID generated and persisted
  by the end application (e.g. a mobile phone or a website).
- Applications do not submit transfers to TigerBeetle directly, going instead through an API
  gateway.
- The gateway provides an HTTP API for potentially untrusted clients.
- The gateway aggregates individual transfers from separate applications into large batches.
- Gateways are stateless and horizontally scalable. All state is managed by TigerBeetle.
- End-to-end idempotency keys guarantee that each transfer is processed at most once, even if, due
  to retry and load-balancing logic, it gets routed through several gateways.
- TigerBeetle records high-volume business transactions using a debit-credit schema, but transactions
  include a `user_data` field for linking up with a general purpose database (see
  [system architecture](https://docs.tigerbeetle.com/coding/system-architecture/)).

### As Fast as a Hash Table

Here's a mental model for TigerBeetle: a good way to solve financial transactions is an in-memory
hash map that stores accounts keyed by their IDs. Processing a transfer is then two hash-map
lookups, a balance check, and two balance updates. This is the "speed-of-light" for the problem ---
any other solution wouldn't be significantly faster.

TigerBeetle improves on the in-memory hash-table across two axes:

**Persistence and High Availability:** an in-memory hash table is good until you need to reboot the
computer. Data in TigerBeetle is stored on disk so power cycles are safe. What's more, the data is
replicated across six replicas. Even if some of them are down, the cluster as a whole remains
available.

**Large Data Sets:** an in-memory hash table is good until your data stops fitting in RAM.
TigerBeetle allows working with larger-than-memory datasets. To keep optimal performance, the
hot path of the dataset should still fit within RAM.

### Don't Waste Durability

The function of consensus algorithm is to convert durability into availability. Data placed on
durable storage is valuable and should be utilized fully.

If an individual block on any replica bit rots, it is wasteful not to repair this block using
equivalent data from other replicas in the cluster.

### Non-Interactive Transactions

The primary paradigm for OLGP databases ([Online General Purpose](https://docs.tigerbeetle.com/concepts/oltp/))
is interactive transactions. To implement a bank transfer in a general-purpose relational database,
the application:

1. Opens a transaction
2. Fetches balances from the database
3. Computes the balance update in the application
4. Sends updates to the database
5. Commits the transaction.

Crucially, step 2 acquires a lock on the balances which is not released until step 5. A lock is
held over a network round-trip. This approach gives good performance if most transactions are
independent (e.g. are mostly reads).

For financial accounting, the opposite is true. Most transactions are writes and conflict due
to popular accounts. So TigerBeetle executes a transaction directly inside the database
to avoid moving data to code over the network.

### Single Thread

TigerBeetle is single-threaded. There are both positive and negative reasons for this design choice.

The primary negative reason is that the underlying workload is inherently contentious. Some
accounts are just way more popular than others. Transfers between hot accounts inherently
sequentialize the system. Trying to make transactions parallel doesn't make it faster. The
overhead of synchronization tends to dominate useful work. Channeling [Frank
McSherry's paper](https://www.usenix.org/system/files/conference/hotos15/hotos15-paper-mcsherry.pdf),
our claim is that financial transaction processing is an infinite-COST problem.

The positive reason for using a single thread is that CPUs are quite fast and the reports
of Moore's Law demise are somewhat overstated. A single core can easily scale to 1mil TPS if you:

- use the core efficiently, keep it busy with useful work and move everything else off the hot path
- do your homework: cache line aligned data structures, memory prefetching, SIMD and other
  performance engineering 101.

Additionally, keeping the system single threaded greatly simplifies the programming model and makes
testing much more efficient.

### Static Memory Allocation

We state that TigerBeetle doesn't use `malloc` and `free` and instead does "static memory allocation".
This is somewhat idiosyncratic use of the term so let's be precise about what TigerBeetle does and
does not do.

When TigerBeetle starts, for every "object type" in the system it computes the worst-case upper
bound for the number of objects needed, based on CLI arguments. Then TigerBeetle allocates exactly
that number of objects and enters the main event loop. After startup, no new objects are created.
Therefore no dynamic memory allocation or deallocation is needed.

This is different from truly static allocation of some embedded systems. TigerBeetle doesn't use
global statics (`.bss` section) for allocation and memory usage depends on the runtime CLI arguments.

This is also different from arena allocation. Some systems allocate a fixed-sized arena at the start
and fail with an 'out of memory' error if the limit is ever exceeded. That is, while the memory
usage is bounded, there's no guarantee that enough memory is reserved. In TigerBeetle, the amount of
memory used is a consequence of everything having an explicit upper bound.

Knowing the limits ensures that the system continues to function correctly even when overloaded. For
example, TigerBeetle doesn't need to have _explicit_ code for handling backpressure. If everything
has a limit, there's nothing to grow without bound to begin with. Backpressure arises from the entire
system of components needing to honor each-other's limits.

Another interesting consequence of static limits is runway concurrency. In highly concurrent
applications, there are more concurrent tasks than the number of underlying resources available. This
leads to oversubscription. A concurrent task is usually a closure allocated somewhere on the heap
and registered with an event loop (a `Box<dyn Future>`). TigerBeetle _can't_ heap allocate
structures at runtime. Each TigerBeetle "future" is represented by an explicit struct which is
statically allocated as a field of a component that owns the structure. There's always a natural
limit of how many concurrent tasks can be in flight!

In summary, **static allocation is a forcing function to ensure that everything has a limit**,
and a natural consequence of these limits.

Somewhat surprisingly, our experience is that static allocation also simplifies the system greatly.
You spend more time thinking up-front but after the initial design the interplay tends to just work
out.

Static allocation makes the system faster and greatly reduces latency variation. This is another
consequence of knowing the limits and "physics" of the underlying system, but it is not the main reason
for choosing static allocation.

### No Dependencies

TigerBeetle avoids dependencies. It depends on:

- the Linux kernel API (in particular, on io_uring) to make hardware do things
- the Zig compiler, to convert from human-readable source code to machine code
- parts of Zig standard library, for various basic appliances like sorting algorithms or hash maps.

The usefulness of dependencies is generally inversely-proportional to the lifetime of the project.
For longer lived projects, it makes sense to control more of the underlying moving parts. TigerBeetle
is explicitly engineered for the long-term. It makes sense to start building all the necessary
infrastructure today. This removes temptation to compromise --- static allocation is done throughout
the stack because we wrote most of the stack. It is a pleasant intentional coincidence that Zig's
standard library APIs are compatible with static allocation.

Avoiding dependencies also acts as a forcing function for keeping the code simple and easy to
understand. Extra complexity can't sneak into the codebase hidden by a nifty API. It turns out that
most of the code in the database isn't _that_ hard.

### Zig

As follows from the [static allocation](#static-memory-allocation) section, TigerBeetle doesn't need
a garbage collector. While it is possible to do static allocation in a GC language, it makes it much
harder to guarantee that no allocation happens. This significantly narrows down the choice
of programming languages. Of the languages that remain, Zig makes the most sense, although Rust is a
close contender.

Both Zig and Rust provide spatial memory safety. Rust has better temporal and thread safety but
static allocation and single-threaded execution reduce the relative importance of these benefits.
Additionally, mere memory safety would be a low bar for TigerBeetle. General correctness is table
stakes. Requiring a comprehensive testing strategy leaves little space for bugs to escape testing
but be caught by the Rust-style type system.

The primary benefit of Zig is the favorable ratio of expressivity to language complexity.
"Expressivity" here means ability to produce the desired machine code versus source-level
abstractions. Zig is a DSL for machine code. Its comptime features makes it very easy to _directly_
express what you want the computer to do. This comes at the cost of missing declaration-site
interfaces but it's less important in a zero-dependency context.

Zig provides excellent control over layout, alignment and padding. Alignment-carrying pointer types
prevent subtle errors. Idiomatic Zig collections don't have an allocator as a parameter of
constructor. Instead they explicitly pass allocator to the specific methods that require allocation.
This is a perfect fit for the memory management strategy used in TigerBeetle. Cross compilation
that works and direct (glibc-less) bindings to the kernel help keep dependency count down.

Most importantly, this is all possible using very frugal language machinery. Zig lends itself to
low-abstraction first-order code that does the work directly. This makes it easy to author
and debug performance-oriented code.

### Determinism

A meta principle above "static allocation" is determinism. Determinism means that given the same input
the software gives the same logical result and arrives at it using the same physical path. In general,
everything in TigerBeetle is deterministic. There's no single reason to demand determinism but it
consistently simplifies and improves the system. Here are some of the places where determinism leads
to big advantages:

- Simplifies the implementation of a replicated state machine. It reduces the
  problem of synchronizing mutable state to a much simpler problem of synchronizing an immutable,
  append-only, hash-chained log.
- Allows for physical as opposed to logical repair. Consider the case where a single byte of the
  storage of a particular LSM tree gets corrupted. If the replicas only guarantee logical consistency,
  repairing this byte might entail re-transmitting the entire tree. This is slow and might fail in
  presence of uncorrelated faults.

  If on top of logical consistency the replicas guarantee that the bytes on disk representing the data
  are _also_ the same it becomes sufficient for repair to transfer just a single disk block containing
  the problematic byte. This is the approach taken by TigerBeetle. It is guaranteed that replicas in the
  cluster converge on a byte-for-byte identical LSM tree structure.
- Physical repair in turn massively simplifies error handling. The function that reads data from
  storage doesn't have an error condition. It _always_ receives the block with the requested
  checksum but the block can be transparently read from a different replica.
- Physical determinism requires that LSM compaction work is scheduled deterministically. Compaction work
  is evenly spread throughout the operation, bounding the worst-case latencies.
- Determinism supercharges randomized testing. Any test failure can be reliably reproduced by
  sharing a seed that lead to the failure.

### Simulation Testing

TigerBeetle uses a variety of techniques to ensure that the code is correct – from example-based
tests to strict style guides. The most important technique deployed is simulation testing, as seen
on [Sim TigerBeetle](https://sim.tigerbeetle.com).

TigerBeetle's simulator, the VOPR ([./vopr.md](./vopr.md)), can run an entire cluster on a single
thread, injecting various storage faults and infinitely speeding up time. VOPR combines a smart
workload generator, swarm testing and a thousand CPU cores. It makes it easy to exercise all the
possible behaviors of the system.

Crucially, unlike formal proofs and model checking, the simulation testing exercises a specific
implementation. Tools like TLA are invaluable to debug an algorithm. They are of little help if you
want to check if your code implements the algorithm correctly or verify underlying _assumptions_.

_The VOPR_ stands for _The Viewstamped Operation Replicator_ and was inspired by the movie WarGames,
by our love of fuzzing over the years, by
[Dropbox's Nucleus testing](https://dropbox.tech/infrastructure/-testing-our-new-sync-engine),
and by [FoundationDB's deterministic simulation testing](https://www.youtube.com/watch?v=OJb8A6h9jQQ).

### Mechanical Sympathy

Mechanical Sympathy is the idea that although a CPU is a general purpose device and can execute
anything, it is often possible to re-formulate a particular algorithm in a CPU-friendly manner. This
makes it much faster to run. Small details matter a lot for speed and sometimes these small
details guide larger architecture.

In the context of TigerBeetle, mechanical sympathy spans all four primary colors of computation:

- Network
- Storage
- Memory
- CPU

**Network** has limited bandwidth. This means that if one node in the network requires much higher
bandwidth than the rest, the network will be underutilized. For this reason the primary doesn't
broadcast prepares to all backups. Instead, it sends each prepare to just two backups, relying on
the backups to forward it further.

**Storage** is typically capable of sustaining several parallel IO operations and TigerBeetle tries to
keep it saturated. For example, when compaction needs to read a table from disk it enqueues writes
for several of the table's blocks at a time. At the same time, care is taken to not oversubscribe
the storage and there is a limit on the maximum number of concurrent IO operations.

**Memory** is not fast. Operations that miss the cache and hit the memory are dramatically slower.
TigerBeetle data structures are compact, to maximize the chance that the data is cached, and
organized around cache lines. All data for a particular operation can be found in a single cache
line as opposed to being spread out in memory. For example, the size of a Transfer object is two cache lines.

**CPU** can both sprint and parkour. But it is so much better at sprinting! CPU is very fast at straight
line code and can sweep several lanes at once with SIMD but `if`s (especially unpredictable ones)
make it stagger. When processing events, where each event is either a new account or a new transfer,
TigerBeetle lifts the branching up. Events come in batches and each batch is homogeneous ---
either all transfers, or all accounts. The event-kind branching is moved out of the inner loop.

### Batching

One of the most effective optimization principles is the idea of amortizing the overhead. If you
have to do a costly operation, make sure that its results "pay for" many smaller useful operation.
TigerBeetle uses batching at many different levels:

- Individual transfers are aggregated into a prepare. This amortizes replication and prefetch IO
  overhead.
- Changes from 32 prepares are aggregated in memory before being written to disk together
- Changes from 1024 prepares are aggregated before the checkpoint is atomically advanced by overwriting
  the superblock
- The LSM tree operates mostly in terms of tables and value blocks, each aggregating many individual
  records.

### LSM

TigerBeetle organizes data on disk as a collection of
[Log Structured Merge Trees](https://www.youtube.com/watch?v=hkMkBZn2mGs). In the context of
TigerBeetle, LSM has several attractive properties:

- Particularly good at writes and TigerBeetle's workload is write-heavy
- Organized around batches of data, just like the rest of TigerBeetle
- Keeps hot data near the root while colder data sinks down to the lower level. This matches
  Pareto-distributed workload.

Often, a database is just a single LSM tree and individual "tables" are constructed logically by
using key prefixes. This is not the case for TigerBeetle and instead many individual trees are used
(the LSM forest). These trees store fixed-sized values. For example, in the Transfer object tree the
value is 128 bytes and for id tree the size of value is 32 bytes (`u128` id, `u64` timestamp, `u64`
padding). Each tree can be specialized for specific value size. This improves performance
and storage efficiency. Zig's `comptime` makes it particularly easy to configure the LSM Forest
through meta programming.

### Grid

The bulk of the data file is organized as a uniform grid of equally-sized blocks. Each block is
0.5MiB large. Although LSM trees are type-specialized, they all use the same grid of blocks. Various
auxiliary persistent data structures (for example, `ManifestLog`) are also built on top of the `Grid`.

Each `Grid` block is also a valid network [`Message`](#message-passing). Blocks are
[hash-chained](#hash-chaining) and [deterministic](#determinism) across replicas. This allows for
physical repair --- if a block gets corrupted, TigerBeetle uses its checksum to transparently
request the data from a peer replica.

### Control Plane / Data Plane Separation

Batching is a special case of the more general principle of separating control plane and data plane.
Imagine yourself as a pilot in a cabin of a powerful jet. The cabin has all sorts of dials, levers
and buttons. By pressing the buttons, you control the jet engines. Most of the work of moving the
plane is done by the engines, the data plane. The pilot doesn't do the heavy lifting, they direct
the power of the engine.

In the context of TigerBeetle, deciding which prepare to apply is 'control plane' and going through
each individual transfer is 'data plane'. In general, control plane is O(1) to data plane's O(N).

It is important to separate the two modes of operation. Keeping control plane `if`s outside
of data plane `for`s keeps the inner loop free of branching. This improves performance. Conversely,
the overhead of control plane is small so it can use very aggressive assertions (up to spending O(N)
time to verify O(1) operation).

### Synchronous Execution

StateMachine's `commit` function, the one that actually implements double-entry accounting, is
synchronous. It takes a slice of transfers as input and decides in a single tight CPU loop per
transfer whether it is applicable. Then it computes the desired balance change and records
a result in the output slice. Crucially, the `commit` function itself doesn't do any reading from
storage. This is the key to performance.

All IO happens in the separate prefetch phase. Given a batch of transfers, it is possible to predict
which accounts need to be fetched without actually executing the transfers. What's more, while commit
execution has to happen sequentially, all prefetch IO can happen in parallel.

### Embracing Concurrency

TigerBeetle uses sequential execution as a simple and performant way to achieve strict
serializability semantics. This doesn't mean that _everything_ has to be sequential. For example, as
the previous section demonstrates, it is possible to simultaneously:

- fetch data from storage in parallel
- apply double-entry accounting rules to transfers on the CPU sequentially.

This pattern generalizes: TigerBeetle embraces concurrency. Sequential execution is the exception.

- In the VSR protocol, prepares are replicated concurrently. When a primary receives a client request,
  assigns it an op-number and starts a replication loop - it doesn't wait for the replication to finish
  before starting to work on the next request. _Execution_ of requests has to be sequential but
  replication can be concurrent.

- Similarly, for a new prepare the primary concurrently:
  - writes it to its local storage
  - starts the replication loop.

  The prepare is considered committed when the primary receives a quorum of `prepare_ok`from a set
  of replicas. This quorum doesn't need to include the primary. It can be the case that the primary
  concurrently executes a prepare while still writing the corresponding message to the (WAL).

- A similar pipelining structure works in LSM compaction. Compaction is a batched two-way merge
  operation. It merges two sorted sequences of values, where values come from blocks on disk. The
  resulting sequence of values is likewise split into blocks which are written to disk. Although the
  merge needs to be sequential, it is possible to fetch several blocks from disk at the same time.
  With some more legwork, it is possible to structure compaction so that reading blocks of values
  from disk, merging them in memory, and writing the resulting blocks to disk happen concurrently.

### io_uring

TigerBeetle uses io_uring exclusively for IO. It is a perfect interface for TigerBeetle as it
combines [batching](#batching) and [concurrency](#embracing-concurrency). At micro level, the
code in TigerBeetle isn't a good fit for coroutines or threads. The concurrency is very
fine-grained at the level of individual syscall. For example, for pipelined compaction the natural
way to write code is to issue two concurrent syscalls for reading the data from the corresponding
levels. That's more or less exactly what io_uring exposes as an interface to the programmer.

io_uring is _also_ the only reasonable way to have truly asynchronous disk io on Linux and comes
with improved throughput to boot. But these benefits are secondary to the interface being a natural
fit for the problem.

io_uring interacts with static allocation in an interesting way. Any asynchronous operation requires
saving a resumption context somewhere. Usually the context is heap allocated, but this is
incompatible with static allocation. In TigerBeetle, each component stores its own resumption
contexts. For example, the `Journal` holds a fixed-size array of `Write` structures with callback
contexts. The contexts from different components are organized by `IO` into a single intrusive
linked list. This way, `IO` can manage arbitrary many in-flight IO operations without a hard-coded
upper bound. And yet, the total amount of concurrency is limited. The limit is implicit, it is the
sum of per-component explicit limits.

### Time

Accounting business logic needs access to wall-clock time for transfer timeouts. The state machine
can't access OS to get time directly as that would violate determinism. Instead, the TigerBeetle
primary injects a specific timestamp into the logic of the state machine when it converts a request to a
prepare.

The ultimate source of time for the TigerBeetle cluster is Network Time Protocol (NTP). A potential
failure mode with NTP is primary partitioned from NTP servers. To make sure that the primary's clock
is within an acceptable error margin, the cluster aggregates timing information from a replication quorum
of replicas. TigerBeetle also guarantees that the time observed by the state machine is strictly monotonic.

This high-quality time implementation is utilized for business logic (timeouts) and plays a key role
in the internal implementation. Every object in TigerBeetle has a globally unique `u64` creation
timestamp which plays the role of synthetic primary key.

### Direct IO

TigerBeetle bypasses the operating system's page cache and uses Direct IO. Normally, when an
application writes to a file, the operating system only updates the in-memory cache and the data
gets to disk later. This is a good default for the vast majority of the applications but TigerBeetle is
an exception. It instructs the operating system to read directly from and write directly to the disk,
bypassing any caches (refer to this [excellent article](https://transactional.blog/how-to-learn/disk-io)
for the overview of the relevant OS APIs).

Bypassing page cache is required for correctness. While operating systems provide an `fsync` API to
flush page cache to disk, it doesn't allow handling errors reliably:
[Can Applications Recover from fsync Failures?](https://www.usenix.org/system/files/atc20-rebello.pdf)

The second reason to bypass the cache is the general principle of avoiding dependencies and reducing
assumptions. Concretely, TigerBeetle require neither the OS to provide page cache nor a
file system by virtue of using only a single file. As a consequence, TigerBeetle can run directly
against a block device.

### Flexible Quorums

There's something odd about a TigerBeetle cluster. We recommend using an even number of `6` replicas.
Usually, consensus implementations use `2f + 1` nodes, `3` or `5`, to have a clear majority for
quorums. TigerBeetle uses so-called flexible quorums. For `6` replicas, the replication quorum is
only `3`. It is enough for only a half of the cluster to persist a prepare durably to disk
for it to be considered logically committed. On the other hand, for changing views (that is, rotating
the role of the primary to a different replica), at least `4` replicas are needed. Because any
subset of three replicas and any subset of four replicas intersect, the new primary is guaranteed to
know all potentially committed prepares.

The upshot here is that it's enough for `3` replicas, including the primary, to be online to keep
the cluster available. If in a cluster of `6` three replicas crash simultaneously, there's a
50% chance that the cluster remains available. If the replicas are crashing one by one the chance
is higher. When only `4` replicas remain, the chance that the next one to crash would be a primary
is only 25%.

### Protocol Aware Recovery

TigerBeetle assumes that the disk can fail and that the data can be physically lost eventually even
if the original `write` and `fsync` completed successfully. If that happens, TigerBeetle
transparently repairs faulty disk sectors using identical data present on the other replicas.

Faulty storage can not be fully encapsulated by the storage interface and requires consensus
cooperation to resolve. Here's a useful counter-example.

The primary accepts a request from the client, converts it to a prepare by assigning it a specific
operation number and starts the replication procedure. During replication, the primary successfully
appends the prepare to its local WAL but fails to broadcast it to other replicas. Now, for whatever
reason, the primary restarts, the cluster switches to a different primary and the prepare gets corrupted
in the original primary's WAL. As the request was prepared, the prepare should make it into the new view.
But because there's only one copy of prepare in the cluster, and it got corrupted, it is impossible
to execute this prepare.

The key difficulty: _potentially_ committed prepares are not necessarily replicated to a full
replication quorum. As such, their durability is reduced. Corruption of a potentially committed
prepare requires special handling. The solution is to use the NACK protocol.

During a view change, participating replicas can positively state that they _never_ accepted a particular
prepare. If at least `4` out of `6` replicas NACK the prepare then it can be inferred that the prepare
was never replicated fully and it can be safely discarded _even if it is corrupted_.

### Hash-Chaining

Similar to how batching tends to improve performance across the board, a universal improvement for
safety is the idea of hash-chaining:

- compute a checksum for each data unit
- include this checksum with some parent data, which is also checksummed.

This is the same structure as in git commits, but applied more generally.

Hash chaining **binds intent**: if you know the checksum, then, on receiving any data, you get
strong guarantees that this is exactly the data you were looking for. You get protection both from
hardware errors and corruption, as well as programming errors that lead to value confusion.
TigerBeetle hash-chains:

**Blocks**. LSM is a functional tree of blocks. Parent blocks (index blocks) contain "pointers" to
child blocks (value blocks). A block "pointer" is a pair of an `u64` block address and an `u128`
checksum. On disk, an array of child pointers is stored as Struct-of-Arrays (SoA). Pointers to the
index blocks themselves are stored in manifest log blocks (see [./data_file.md](./data_file.md) for
a more thorough overview). Whenever a replica reads a block from disk, it already knows its
checksum: checksums are stored outside of blocks themselves. This is important to protect from
misdirected IO: one failure mode for disks is to store correct data at a wrong offset, a failure
which cannot be detected using only internal checksums. External checksums also make transparent
repair possible: if a replica fails to read a block from its local storage, it doesn't report an
error and instead automatically requests other replicas to send the block, using the checksum as an
identifier. The root checksum is stored on disk in the superblock, where the hash-chain starts. To
protect the integrity of the superblock itself, it is physically duplicated across four copies on disk.
Superblock changes over time, and each _version_ of a superblock includes a checksum of the previous
version --- if two versions co-exist on disk at the same time, their ordering is constrained weakly
by a sequence number and strongly by hash-chaining.

**Prepares**. Prepare message (units of replication, Write Ahead Log (WAL) and consensus) are hash
chained. This gives strong ordering guarantees for two adjacent prepares, and, via a transitive
closure, gives a global consistency guarantee, that the entire sequence of prepares from the
beginning of history to the latest prepare is valid. Hash-chaining improves WAL repair. Backups need
extra care (and, during rare view change, an extra message from the primary) to ensure that the
latest prepare in their log is correct, but any prepares before that can be repaired by following
the hash chain.

**Requests**. Each client request includes a checksum of the reply to the previous request. While
these checksums are not as crucial for data validation, they provide a strong proof that the proper
ordering of requests is observed.

### Message Passing

TigerBeetle is agnostic of the underlying transport protocol and requires only a very weak message
passing semantics. TigerBeetle assumes that messages might be dropped, duplicated, reordered, and
corrupted (in non-byzantine way). Although specific `MessageBus` is not tested in
[VOPR](#simulation-testing), bugs in `MessageBus` are unlikely to affect correctness, because the
contract for the transport layer is intentionally very weak.

### Adaptive Routing

As per the
[8 Fallacies of Distributed Computing](https://en.wikipedia.org/wiki/Fallacies_of_distributed_computing)
the network topology may change over time. Therefore, replication must dynamically adapt to
changes in the environment. Following the insight from
[Performance-Oriented Congestion Control](https://www.usenix.org/system/files/conference/nsdi15/nsdi15-paper-dong.pdf),
a robust adaptive routing algorithm is based on direct observation of outcomes, rather than on
indirect modeling. Instead of inferring topology information based on measuring individual
ping-pong roundtrips, TigerBeetle dedicates a fraction of bandwidth for running experiments,
sending an individual prepare over a random alternative replication path. If an experimental route
proves more efficient, the primary changes the replication path for subsequent prepares.

## Conclusion

TigerBeetle is designed to deliver mission-critical safety and 1000x performance, and power the
world's transactions. Presently focused on financial transactions, it fundamentally solves the
challenge of cost-efficient OLTP at scale in a world becoming exponentially more transactional and
real-time.

## References

The collection of logical and magical art behind TigerBeetle:

- [LMAX - How to Do 100K TPS at Less than 1ms Latency -
  2010](https://www.infoq.com/presentations/LMAX/) - Martin Thompson on mechanical sympathy and why
  a relational database is not the right solution.

- [The LMAX Exchange Architecture - High Throughput, Low Latency and Plain Old Java -
  2014](https://skillsmatter.com/skillscasts/5247-the-lmax-exchange-architecture-high-throughput-low-latency-and-plain-old-java)
  - Sam Adams on the high-level design of LMAX.

- [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf) - A high
  performance alternative to bounded queues for exchanging data between concurrent threads.

- [Evolution of Financial Exchange Architectures -
  2020](https://www.youtube.com/watch?v=qDhTjE0XmkE) - Martin Thompson looks at the evolution of
  financial exchanges and explores the state of the art today.

- [Gray Failure](https://www.microsoft.com/en-us/research/wp-content/uploads/2017/06/paper-1.pdf) -
  "The major availability breakdowns and performance anomalies we see in cloud environments tend to
  be caused by subtle underlying faults, i.e. gray failure rather than fail-stop failure."

- [The Tail at Store: A Revelation from Millions of Hours of Disk and SSD
  Deployments](https://www.usenix.org/system/files/conference/fast16/fast16-papers-hao.pdf) - "We
  find that storage performance instability is not uncommon: 0.2% of the time, a disk is more than
  2x slower than its peer drives in the same RAID group (and 0.6% for SSD). As a consequence, disk
  and SSD-based RAIDs experience at least one slow drive (i.e., storage tail) 1.5% and 2.2% of the
  time."

- [The Tail at
  Scale](https://www2.cs.duke.edu/courses/cps296.4/fall13/838-CloudPapers/dean_longtail.pdf) - "A
  simple way to curb latency variability is to issue the same request to multiple replicas and use
  the results from whichever replica responds first."

- [Viewstamped Replication Revisited](http://pmg.csail.mit.edu/papers/vr-revisited.pdf)

- [Viewstamped Replication: A New Primary Copy Method to Support Highly-Available Distributed
  Systems](http://pmg.csail.mit.edu/papers/vr.pdf)

- [Flexible Paxos: Quorum intersection revisited](https://arxiv.org/pdf/1608.06696v1)

- [Scalability! But at what COST?](https://www.usenix.org/system/files/conference/hotos15/hotos15-paper-mcsherry.pdf)

- [ZFS: The Last Word in File Systems (Jeff Bonwick and Bill
  Moore)](https://www.youtube.com/watch?v=NRoUC9P1PmA) - On disk failure and corruption, the need
  for checksums... and checksums to check the checksums, and the power of copy-on-write for
  crash-safety.

- [An Analysis of Latent Sector Errors in Disk
  Drives](https://research.cs.wisc.edu/wind/Publications/latent-sigmetrics07.pdf)

- [An Analysis of Data Corruption in the Storage
  Stack](https://www.usenix.org/legacy/events/fast08/tech/full_papers/bairavasundaram/bairavasundaram.pdf)

- [A Study of SSD Reliability in Large Scale Enterprise Storage
  Deployments](https://www.usenix.org/system/files/fast20-maneas.pdf)

- [SDC 2018 - Protocol-Aware Recovery for Consensus-Based Storage](https://www.youtube.com/watch?v=fDY6Wi0GcPs)
  ([pdf](https://www.usenix.org/conference/fast18/presentation/alagappan)) - Why replicated state
  machines need to distinguish between a crash and corruption, and why it would be disastrous to
  truncate the journal when encountering a checksum mismatch.

- [Can Applications Recover from fsync
  Failures?](https://www.usenix.org/system/files/atc20-rebello.pdf) - Why we use Direct I/O in
  TigerBeetle and why the kernel page cache is a dangerous way to recover the journal, even when
  restarting from an fsync() failure panic.

- [Coil's Mojaloop Performance Work
  2020](https://docs.mojaloop.io/legacy/discussions/Mojaloop%20Performance%202020.pdf) - By Don
  Changfoot and Joran Dirk Greef, a performance analysis of Mojaloop's central ledger that sparked
  the idea for "an accounting database" as Adrian Hope-Bailie put it. And the rest, as they say, is
  history!

- [Swarm Testing](https://users.cs.utah.edu/~regehr/papers/swarm12.pdf)

- [PCC: Re-architecting Congestion Control for Consistent High Performance](https://www.usenix.org/system/files/conference/nsdi15/nsdi15-paper-dong.pdf)
