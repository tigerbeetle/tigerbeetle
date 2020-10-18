# hash-table

The hash table data structure is the performance core of TigerBeetle's state machine and needs to support:

* fast negative lookups for duplicate transfer checks (with a minimum of probing in the worst case),
* fast inserts of new transfers,
* fast positive lookups/updates of existing transfers for processing commits,
* fast positive lookups/updates of accounts for referential integrity and balance updates,
* predictable performance with a minimum of variance in probing, even at
* high load factors > 80% for efficient memory usage.

This is a performance demo comparing ProtoBeetle's [`@ronomon/hash-table`](https://github.com/ronomon/hash-table) with Zig's std lib HashMap.

## Node.js

On a 2020 MacBook Air 1,1 GHz Quad-Core Intel Core i5, Node.js can insert **2.4 million transfers per second**:

```bash
$ npm install --no-save @ronomon/hash-table
$ node benchmark.js
1000000 hash table insertions in 1004ms // V8 optimizing...
1000000 hash table insertions in 468ms
1000000 hash table insertions in 432ms
1000000 hash table insertions in 427ms
1000000 hash table insertions in 445ms
```

## Zig

On the same development machine, not a production server, Zig's std lib HashMap can insert **12.6 million transfers per second**:

```bash
$ zig run benchmark.zig --release-safe
1000000 hash table insertions in 90ms
1000000 hash table insertions in 79ms
1000000 hash table insertions in 82ms
1000000 hash table insertions in 89ms
1000000 hash table insertions in 100ms
```

## Comparison

Both hash tables:

* allow memory to be reserved and allocated upfront to avoid hash table resizes, and
* use open addressing to store transfers in a single large buffer to avoid the pointer chasing associated with chaining using linked lists.

However:

* [`@ronomon/hash-table`](https://github.com/ronomon/hash-table) uses cuckoo probing with bloom filters to guarantee at most 2 cache misses in the worst case for high load factors, and at most 1 cache miss for a negative lookup, whereas
* Zig's std lib HashMap has access to fast native unboxed 64-bit integer types for fast 64-bit hash functions, and uses linear probing for access locality but increases the likelihood of worst case cache misses during long probe sequences for high load factors.

What we don't see in the rough benchmarks above is the performance of the hash table:

* after zeroing all pages in the buffer to eliminate page faults,
* for varying load factors (25%, 50%, 70%, 80%, 90%), and
* for varying numbers of transfers (10,000, 100,000, 1,000,000, 10,000,000).
