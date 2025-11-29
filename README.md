# tigerbeetle

# Idea

Current DesignThe Groove object cache is a hybrid composed of:

    A set-associative cache, which receives prefetches and updates.
    A stash map sized for an entire bar, which catches evictions.

With rollbacks in the picture, this layering makes consistency difficult to maintain.
The main reason for this is that BOTH are used during a beat.Additionally, the cache is sized for the entire bar (lsm_compaction_ops beats) and only compacted at the end of the bar. As a result, everything prefetched or written during that bar must remain resident. This leads to several costs:

    A larger working set
    Extra stash bookkeeping

New DesignThe new design makes the cache batch-/beat-aware and significantly simplifies the hot path. Conceptually, it remains two structures:
a tiny, per-beat hash table and a locality cache, but we only use the per-beat hash table during a beat:1. Per-beat batch table
A dedicated, non-evicting table for the current beat. Prefetches and updates go here, eliminating mid-beat eviction risk.
It remains small and cache-friendly.
At the end of each beat, it is flushed into the locality cache and then cleared.
This means we have NO evictions during prefetch from the per-beat table!2. Simpler rollbacks
Because new data lives only in the batch table during a single beat, rollback logic is isolated to the per-beat table.
Again, NO complicated logic to keep stash and set associative cache in sync.3. Consistency
During a beat, we touch only the per-beat table. At the end, we transfer its contents to the locality cache.
This separation makes consistency handling much simpler, as the phases where we touch each part is strictly separated (and could be asserted).
E.g. only in prefetch we read from the locality cache and at the end of a beat we write the results back to update it.4. Per-beat sizing
Capacity now depends on the current beat (prefetch + mutable entries), not the full bar.
This reduces memory pressure and shrinks probe lengths.5. Performance
You might think now that we copy more since we insert every value now in the per-beath hashtable where before we only (in the best case) read the set associate cache, but it turns out this is negligible since the dense per beat HT is fully in cache and thus copies are very efficient.  I implemented a PoC of this design, and it outperforms the current version in end-to-end benchmarks by 6-10%. (Also it passes the fuzzers etc.)In my eyes this design is a big win for simplicity (+performance), as it separates the concerns nicely: a per-beat table that never misses for the processing and locality cache to exploit skew. Also it keeps the set associative cache which is pretty nice since it has little metadata when scaled up.

# Ressources 

https://reiner.org/cuckoo-hashing#large-out-of-cache-tables-successful-lookups
https://github.com/reinerp/cuckoo-hashing-benchmark/tree/072b9c1124e51b27ab6070bae6ada87b9483cf0d




*TigerBeetle is the financial transactions database designed for mission critical safety and performance to power the next 30 years of [OLTP](https://docs.tigerbeetle.com/concepts/oltp).*

## Documentation

* <https://docs.tigerbeetle.com>
* [The Primeagen](https://www.youtube.com/watch?v=sC1B3d9C_sI) video introduction to our
  design decisions regarding performance, safety, and debit/credit primitives.
* [Redesigning OLTP for a New Order of Magnitude (QCon SF)](https://www.infoq.com/presentations/redesign-oltp/)
  talk with a deeper dive into TigerBeetle’s local storage engine and global consensus protocol.
* [TIGER_STYLE.md](./docs/TIGER_STYLE.md), the engineering methodology behind TigerBeetle.
* [Slack](https://slack.tigerbeetle.com/join), say hello!

## Start

Run a single-replica cluster on Linux (or [other platforms](https://docs.tigerbeetle.com/start/)):

```console
$ curl -Lo tigerbeetle.zip https://linux.tigerbeetle.com && unzip tigerbeetle.zip
$ ./tigerbeetle version
$ ./tigerbeetle format --cluster=0 --replica=0 --replica-count=1 --development 0_0.tigerbeetle
$ ./tigerbeetle start --addresses=3000 --development 0_0.tigerbeetle
```

Connect to the cluster and make a transfer:

```console
$ ./tigerbeetle repl --cluster=0 --addresses=3000
> create_accounts id=1 code=10 ledger=700,
                  id=2 code=10 ledger=700;
> create_transfers id=1 debit_account_id=1 credit_account_id=2 amount=10 ledger=700 code=10;
> lookup_accounts id=1, id=2;
{
  "id": "1",
  "user_data": "0",
  "ledger": "700",
  "code": "10",
  "flags": "",
  "debits_pending": "0",
  "debits_posted": "10",
  "credits_pending": "0",
  "credits_posted": "0"
}
{
  "id": "2",
  "user_data": "0",
  "ledger": "700",
  "code": "10",
  "flags": "",
  "debits_pending": "0",
  "debits_posted": "0",
  "credits_pending": "0",
  "credits_posted": "10"
}
```

Want to learn more? See <https://docs.tigerbeetle.com>.
