# Changelog

Subscribe to the [tracking issue #2231](https://github.com/tigerbeetle/tigerbeetle/issues/2231)
to receive notifications about breaking changes!

## TigerBeetle 0.16.67

Released: 2025-12-12

### Safety And Performance

- [#3415](https://github.com/tigerbeetle/tigerbeetle/pull/3415)

  Speed up VOPR storage and network during liveness mode for fast convergence, to avoid false
  positives.

- [#3402](https://github.com/tigerbeetle/tigerbeetle/pull/3402)

  Don't discard high 16 bits of timestamp when generating ID's with the Node.js client.

- [#3411](https://github.com/tigerbeetle/tigerbeetle/pull/3411)

  Relax VOPR condition for truncating acked ops to prevent false positives.

- [#3405](https://github.com/tigerbeetle/tigerbeetle/pull/3405)

  Implement a micro-benchmarking harness, to protect the benchmarks from bitrot.

### Features

- [#3386](https://github.com/tigerbeetle/tigerbeetle/pull/3386)

  Add more documentation details around query multibatching.

### Internals

- [#3414](https://github.com/tigerbeetle/tigerbeetle/pull/3414)

  Make `Pending!?Value` the canonical stream signature. This type is chosen because it is the
  orthogonal composition of iteration and asynchrony.

- [#3417](https://github.com/tigerbeetle/tigerbeetle/pull/3417)

  Prevent "out (disk) of space" failures during CI.

- [#3396](https://github.com/tigerbeetle/tigerbeetle/pull/3396)

  Make DSL for parsing positional CLI flags more natural.

- [#3375](https://github.com/tigerbeetle/tigerbeetle/pull/3375)

  Remove dead code from zig-zag merge join.

- [#3389](https://github.com/tigerbeetle/tigerbeetle/pull/3389)

  Improve how Vörtex handles SIGTERM.

- [#3391](https://github.com/tigerbeetle/tigerbeetle/pull/3391)
  [#3394](https://github.com/tigerbeetle/tigerbeetle/pull/3394)

  Improve the way we handle ratios in stdx, and refactor `parse_flag_value`.

- [#3395](https://github.com/tigerbeetle/tigerbeetle/pull/3395)

  Update TigerStyle with more context for the line and function length limits.

- [#3390](https://github.com/tigerbeetle/tigerbeetle/pull/3390)

  Improve some spelling and grammar in `start.md`. Thanks @elness!

### TigerTracks 🎧

- [The Ominous Blue](https://open.spotify.com/track/32ivi8KXX5qNqntYKfLMQT?si=b9295603d04741e7)

## TigerBeetle 0.16.66

Released: 2025-11-21

### Safety And Performance

- [#3379](https://github.com/tigerbeetle/tigerbeetle/pull/3379)

  Fix non-monotonic ID generation in the Python client. Thank you for spotting this, @rbino!

### Internals

- [#3371](https://github.com/tigerbeetle/tigerbeetle/pull/3371)

  Introduce `op_checkpoint_sync` to simplify assertions during state sync.

- [#3353](https://github.com/tigerbeetle/tigerbeetle/pull/3353)

  Run Vörtex in CFO (Continuous Fuzzing Orchestrator).

- [#3380](https://github.com/tigerbeetle/tigerbeetle/pull/3380)
- [#3384](https://github.com/tigerbeetle/tigerbeetle/pull/3384)

  Various fixes for Vörtex in CFO.

### TigerTracks 🎧

- [The Flute Song](https://open.spotify.com/track/49fOKvQVojIKvJKQhQj2nA)

## TigerBeetle 0.16.65

Released: 2025-11-14

### Safety And Performance

- [#3377](https://github.com/tigerbeetle/tigerbeetle/pull/3377)

  Fix overzealous assertion that didn't account for state sync while accepting start view headers.

### Internals

- [#3369](https://github.com/tigerbeetle/tigerbeetle/pull/3369),
  [#3370](https://github.com/tigerbeetle/tigerbeetle/pull/3370),
  [#3374](https://github.com/tigerbeetle/tigerbeetle/pull/3374)

  Various MessageBus refactors to reduce code bloat.

  Earlier, the connection and message passing logic was spread out across the MessageBus and
  Connection types. Now, all that logic is contained within the MessageBus type, with Connection
  only maintaining connection state between two peers.

- [#3367](https://github.com/tigerbeetle/tigerbeetle/pull/3367)

  Fix bug in cut_suffix wherein the suffix itself was being returned instead of the prefix.

- [#3365](https://github.com/tigerbeetle/tigerbeetle/pull/3365)

  Make IO file descriptor a Zig optional, as opposed to using INVALID_SOCKET.

- [#3364](https://github.com/tigerbeetle/tigerbeetle/pull/3364)

  Add support for canceling _individual_ inflight IO operations on Linux.

  Earlier, Linux IO only allowed canceling _all_ asynchronous in-flight operations.

### TigerTracks 🎧

- [Elegia](https://open.spotify.com/track/5wZtbH2PjVZ5W1Akn5z2uA?si=6d0b2a9c09494e9b)

## TigerBeetle 0.16.64

Released: 2025-11-07

### Internals

- [#3344](https://github.com/tigerbeetle/tigerbeetle/pull/3344),
  [#3362](https://github.com/tigerbeetle/tigerbeetle/pull/3362)

  Parametrize code over `Operation` rather than the entire `StateMachine`. Previously, client code
  used to have a dependency on the implementation code of the concrete state machine, whereas it
  only needs to know the types of the operations involved.

- [#3358](https://github.com/tigerbeetle/tigerbeetle/pull/3358),
  [#3356](https://github.com/tigerbeetle/tigerbeetle/pull/3356),
  [#3357](https://github.com/tigerbeetle/tigerbeetle/pull/3357)

  [Remove](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md#safety) `usize`
  from constants, scans and CLI arguments.

- [#3351](https://github.com/tigerbeetle/tigerbeetle/pull/3351)

  Remove `StateMachine`'s dependency on a specific `StateMachineConfig` and rather use the global
  constants.

- [#3343](https://github.com/tigerbeetle/tigerbeetle/pull/3343)

  Simplify the Rust client's build script and bring it inline with other clients by making it no
  longer driver TigerBeetle's build.

### TigerTracks 🎧

- [Hallo My Maatjie](https://www.youtube.com/watch?v=t88Abqb1Qp8)

## TigerBeetle 0.16.63

Released: 2025-10-31

### Safety And Performance

- [#3294](https://github.com/tigerbeetle/tigerbeetle/pull/3294),
  [#3336](https://github.com/tigerbeetle/tigerbeetle/pull/3336)

  Allow backups to accept prepares from the next checkpoint when they replace already committed
  prepares.

- [#3323](https://github.com/tigerbeetle/tigerbeetle/pull/3323)

  Assert the size of `Headers` and `StartView` messages.

- [#3318](https://github.com/tigerbeetle/tigerbeetle/pull/3318)

  Correctly reset the `pulse_next_timestamp`.

- [#3316](https://github.com/tigerbeetle/tigerbeetle/pull/3316)

  Disable `config.verify` in release builds, while promoting several assertions gated by `verify`
  to regular assertions.

- [#3335](https://github.com/tigerbeetle/tigerbeetle/pull/3335)

  Fix an unhandled error on unreachable networks that could cause liveness issues.
  Also ban all usage of `posix.send()` in favor of `posix.sendto()`.

- [#3334](https://github.com/tigerbeetle/tigerbeetle/pull/3334)

  Free up about 300MiB of memory by sharing the same buffer for in-place radix sort.

### Features

- [#3302](https://github.com/tigerbeetle/tigerbeetle/pull/3302)

  Clarify that the `TBID`, which is a `u128` number, only specifies the bit layout.

- [#3291](https://github.com/tigerbeetle/tigerbeetle/pull/3291)

  Introduces `tigerbeetle inspect integrity` to verify offline that a datafile is uncorrupted.

- [#3254](https://github.com/tigerbeetle/tigerbeetle/pull/3254)

  Prepare the Rust client for publication.

### Internals

- [#3312](https://github.com/tigerbeetle/tigerbeetle/pull/3312),
  [#3324](https://github.com/tigerbeetle/tigerbeetle/pull/3324)

  Improve duration parsing by removing ambiguous units and adding fuzz tests.

- [#3313](https://github.com/tigerbeetle/tigerbeetle/pull/3313)

  Update data file documentation to correctly state the grid block size as 512 KiB.

- [#3311](https://github.com/tigerbeetle/tigerbeetle/pull/3311)

  Clarify the release process in case a version of TigerBeetle is skipped.

- [#3317](https://github.com/tigerbeetle/tigerbeetle/pull/3317)

  Fix invalid payload references after modifying the active tag in a tagged union.

- [#3339](https://github.com/tigerbeetle/tigerbeetle/pull/3339)

  Remove unnecessary deduplication logic when scanning from memory tables,
  since [#2592](https://github.com/tigerbeetle/tigerbeetle/pull/2592) already
  introduced deduplication during sorting.

- [#3346](https://github.com/tigerbeetle/tigerbeetle/pull/3346)

  Reserve the maximum release version (65535.x.x) for testing clusters (`cluster_id` zero).

- [#3342](https://github.com/tigerbeetle/tigerbeetle/pull/3342),
  [#3338](https://github.com/tigerbeetle/tigerbeetle/pull/3338),
  [#3330](https://github.com/tigerbeetle/tigerbeetle/pull/3330),
  [#3347](https://github.com/tigerbeetle/tigerbeetle/pull/3347),
  [#3341](https://github.com/tigerbeetle/tigerbeetle/pull/3341)

  Various Vortex and CFO improvements.

- [#3332](https://github.com/tigerbeetle/tigerbeetle/pull/3332),
  [#3331](https://github.com/tigerbeetle/tigerbeetle/pull/3331)

  Fix the build command for clients in HACKING.md and other typos.
  Thanks @gharbi-mohamed-dev!

### TigerTracks 🎧

- [NINETY-TWO](https://www.youtube.com/watch?v=qbXSXDv6gWU)

## TigerBeetle 0.16.62

Released: 2025-10-17

### Safety And Performance

- [#3304](https://github.com/tigerbeetle/tigerbeetle/pull/3304)

  Enable VOPR to detect when the message limit is exceeded.

- [#3307](https://github.com/tigerbeetle/tigerbeetle/pull/3307)

  Enable unit tests for deprecated operations.

- [#3309](https://github.com/tigerbeetle/tigerbeetle/pull/3309)

  Improve the release process by publishing to npm via trusted publishers.

### Features

- [#3299](https://github.com/tigerbeetle/tigerbeetle/pull/3299)

  Rephrase the "debits first" explanation in documentation.

### Internals

- [#3300](https://github.com/tigerbeetle/tigerbeetle/pull/3300)

  Refactor the message bus to save memory and tighten explicit `SendQueue` limits.

- [#3296](https://github.com/tigerbeetle/tigerbeetle/pull/3296)

  Implement `BoundedArray` from scratch.

- [#3292](https://github.com/tigerbeetle/tigerbeetle/pull/3292)

  Remove needless use of bounded array from REPL.

- [#3308](https://github.com/tigerbeetle/tigerbeetle/pull/3308)

  Refactor and cleanup Vortex.

### TigerTracks 🎧

- [no goodbye](https://www.youtube.com/watch?v=oWDzTvjoDn4)

## TigerBeetle (unreleased)

Released: 2025-10-10

### Features

- [#3299](https://github.com/tigerbeetle/tigerbeetle/pull/3299)

  Rephrase the "debits first" explanation in documentation.

### Internals

- [#3296](https://github.com/tigerbeetle/tigerbeetle/pull/3296)

  Implement `BoundedArray` from scratch.

- [#3292](https://github.com/tigerbeetle/tigerbeetle/pull/3292)

  Remove needless use of bounded array from REPL.

### TigerTracks 🎧

- [voyager](https://www.youtube.com/watch?v=BLVI-RS9srI)

## TigerBeetle 0.16.61

Released: 2025-10-03

### Safety And Performance

- [#3263](https://github.com/tigerbeetle/tigerbeetle/pull/3263)

  Speed up cluster repair by adapting the pace and distribution of repair requests according to
  observed network conditions.

- [#3282](https://github.com/tigerbeetle/tigerbeetle/pull/3282)

  Speed up cluster repair by not delaying execution of committed prepares until the log is fully
  repaired.

- [#3249](https://github.com/tigerbeetle/tigerbeetle/pull/3249),
  [#3293](https://github.com/tigerbeetle/tigerbeetle/pull/3293),
  [#3289](https://github.com/tigerbeetle/tigerbeetle/pull/3289)

  Add a dedicated fuzzer for `MessageBus`.

### Features

- [#3212](https://github.com/tigerbeetle/tigerbeetle/pull/3212)

  Add `--log-trace` for extra verbose logging.

- [#3286](https://github.com/tigerbeetle/tigerbeetle/pull/3286)

  After successfully adding a test that tests tests in
  [#3136](https://github.com/tigerbeetle/tigerbeetle/pull/3136), we doubled down on this strategy
  and are adding a metric for tracking metrics.

### Internals

- [#3115](https://github.com/tigerbeetle/tigerbeetle/pull/3115)

  Use ISO4217 three-letter codes when writing about currencies (so, `USD` over `$`).

- [#3284](https://github.com/tigerbeetle/tigerbeetle/pull/3284)

  Clean up Adaptive Replication Routing implementation.

- [#3287](https://github.com/tigerbeetle/tigerbeetle/pull/3287)

  Speed up `zig build test` by removing false build-time dependencies.

### TigerTracks 🎧

- [Сколько тебя](https://open.spotify.com/track/480AuAqeCkgwY46HocIbXk?si=510095bd23c74336)

## TigerBeetle 0.16.60

Released: 2025-09-26

### Safety And Performance

- [#3270](https://github.com/tigerbeetle/tigerbeetle/pull/3270)

  Remove a copy from the StateMachine.

- [#3273](https://github.com/tigerbeetle/tigerbeetle/pull/3273)

  Update the k-way-merge to use the new `from_seed_testing()`.

- [#3277](https://github.com/tigerbeetle/tigerbeetle/pull/3277)

  Improve metrics to use a reduce the worst case packet count (and benefit from a small memory
  saving while we're at it).

### Features

- [#3278](https://github.com/tigerbeetle/tigerbeetle/pull/3278)

  Clarified documentation on closing accounts and two-phase transfers.
  Thanks @raui100!

### Internals

- [#3275](https://github.com/tigerbeetle/tigerbeetle/pull/3275)

  Refactor references to old time types to use the Instant and Duration types.

- [#3274](https://github.com/tigerbeetle/tigerbeetle/pull/3274)

  Added documentation for our CI entrypoint: `zig build ci`.

### TigerTracks 🎧

- [Undefeated](https://open.spotify.com/track/5fwKEMTyS0FqLk7KVdGQwl?si=9d23f68ec7a542fe)

## TigerBeetle 0.16.59

Released: 2025-09-19

### Safety And Performance

- [#3257](https://github.com/tigerbeetle/tigerbeetle/pull/3257)

  Introduce Least Significant Digit (LSD) radix sort in stdx.

- [#3268](https://github.com/tigerbeetle/tigerbeetle/pull/3268)

  Use radix sort in the memory tables to get more performance improvements.

- [#3250](https://github.com/tigerbeetle/tigerbeetle/pull/3250)

  Reduce tail latencies by tracking sorted runs and use k-way merge to sort them.

  Collectively, these changes result in the following performance improvements on modern servers
  (Hetzner AX102):

  | Metric | Before | After |
  |--------|--------|-------|
  | Load accepted (tx/s) | 414,375 | 606,258 |
  | Batch latency p100 | 115ms | 75ms |

### Internals

- [#3262](https://github.com/tigerbeetle/tigerbeetle/pull/3262)

  Use Zig's new `std.testing.random_seed` to introduce genuine randomness in tests.

- [#3260](https://github.com/tigerbeetle/tigerbeetle/pull/3260)

  Fix a crash due to corruption and misdirection found by the WIP message bus fuzzer.

### TigerTracks 🎧

- [Autobahn](https://open.spotify.com/track/31uidLEHAcF8Cw1cX1VCS8?si=3cefba6d35124ed9)

## TigerBeetle 0.16.58

Released: 2025-09-12

### Safety And Performance

- [#3248](https://github.com/tigerbeetle/tigerbeetle/pull/3248)

  Fix a bug where grid.cancel was erroneously being invoked during commit_stage=checkpoint_durable.

- [#3245](https://github.com/tigerbeetle/tigerbeetle/pull/3245)

  Remove peer type from MessageBuffer, maintaining it only at MessageBus level.

  This solves a bug introduced by [#3206](https://github.com/tigerbeetle/tigerbeetle/pull/3206),
  due to divergent peer state between MessageBus and MessageBuffer.

- [#3226](https://github.com/tigerbeetle/tigerbeetle/pull/3226),
  [#3230](https://github.com/tigerbeetle/tigerbeetle/pull/3230),
  [#3223](https://github.com/tigerbeetle/tigerbeetle/pull/3223),
  [#3222](https://github.com/tigerbeetle/tigerbeetle/pull/3222)

  Low-level LSM performance improvements. The k-way merge iterator now uses a tournament tree
  instead of a heap, we skip sorting of the mutable table if it's not needed, and we skip binary
  search if possible by min/max key ranges.

- [#3237](https://github.com/tigerbeetle/tigerbeetle/pull/3237)

  Use a different PRNG seed for Replica each time, rather than a fixed seed of the replica ID. This
  PRNG controls things like exponential backoff jitter and the order of the blocks on which the grid
  scrubber runs.

### Features

- [#3253](https://github.com/tigerbeetle/tigerbeetle/pull/3253)

  Introduce `--requests-per-second-limit` to throttle CDC requests to TigerBeetle.

  Usage for this option is orthogonal to `--idle-interval-ms` and `--event-count-max`, allowing
  fine-tuning for low latency without overflowing the AMQP target queue.

- [#3206](https://github.com/tigerbeetle/tigerbeetle/pull/3206)

  In order to avoid bimodality if a replica is down (eg, a client sends a request, doesn't hear
  anything, eventually times out and tries a different replica) clients now proactively send their
  requests to the primary and a randomly selected replica.

  Backups can also send replies directly to clients, meaning that a client could be completely
  partitioned from the primary, but still remain available.

### Internals

- [#3251](https://github.com/tigerbeetle/tigerbeetle/pull/3251)

  Mark grid cache blocks as MADV_DONTDUMP, making core dump size tractable even with large caches.

- [#3227](https://github.com/tigerbeetle/tigerbeetle/pull/3227),
  [#3231](https://github.com/tigerbeetle/tigerbeetle/pull/3231),
  [#3242](https://github.com/tigerbeetle/tigerbeetle/pull/3242)

  Preparation for the Zig 0.15.1 upgrade.

- [#3236](https://github.com/tigerbeetle/tigerbeetle/pull/3236),
  [#3229](https://github.com/tigerbeetle/tigerbeetle/pull/3229),
  [#3198](https://github.com/tigerbeetle/tigerbeetle/pull/3198)

  A host of multiversion improvements! Multiversioning is now a proper interface, rather than being
  scattered about. Additionally, Windows support is now significantly more robust, and will be
  integration tested like the other platforms in the next release.

### TigerTracks 🎧

- [See You Again](https://www.youtube.com/watch?v=RgKAFK5djSk)

## TigerBeetle 0.16.57

Released: 2025-08-29

### Safety And Performance

- [#3200](https://github.com/tigerbeetle/tigerbeetle/pull/3200)

  Improved performance in `set_associative_cache.zig` by using Fastrange and SIMD.

- [#3201](https://github.com/tigerbeetle/tigerbeetle/pull/3201)

  Improved performance and better codegen in `aegis.zig` by avoiding aliasing.

- [#3205](https://github.com/tigerbeetle/tigerbeetle/pull/3205)

  Retain table repair progress across checkpoint boundaries,
  to reduce the time to complete state sync for active clusters.

- [#3216](https://github.com/tigerbeetle/tigerbeetle/pull/3216)

  Use the `NOSIGNAL` flag with both asynchronous `send` and synchronous `send_now`.
  This avoids receiving a possible `SIGPIPE` signal raised by the kernel.

- [#3189](https://github.com/tigerbeetle/tigerbeetle/pull/3189)

  Improve the Rust client API to make the returned future thread-safe.
  Thanks @michabp!

- [#3208](https://github.com/tigerbeetle/tigerbeetle/pull/3208)

  Fix the Go client to make the subfolder with external files required by CGO
  compatible with `go vendor`.
  Thanks @itzloop.

- [#3203](https://github.com/tigerbeetle/tigerbeetle/pull/3203)

  Fix the Python client to make `close()` async on `ClientAsync`.

### Internals

- [#3215](https://github.com/tigerbeetle/tigerbeetle/pull/3215)

  Removed support for _closed loop replication_ in repair and sync protocols.

- [#3210](https://github.com/tigerbeetle/tigerbeetle/pull/3210),
  [#3211](https://github.com/tigerbeetle/tigerbeetle/pull/3211)

  Ban equality and inequality comparisons with `error` values, as they may silently
  perform an untyped comparison.
  Enforce handling errors with `switch` blocks instead.

- [#3207](https://github.com/tigerbeetle/tigerbeetle/pull/3207)

  Consolidate all Windows APIs we use (not present in Zig’s `std`) under `stdx.windows`.

- [#3199](https://github.com/tigerbeetle/tigerbeetle/pull/3199),
  [#3194](https://github.com/tigerbeetle/tigerbeetle/pull/3194)

  Improve upgrade tests.

- [#3202](https://github.com/tigerbeetle/tigerbeetle/pull/3202),
  [#3140](https://github.com/tigerbeetle/tigerbeetle/pull/3140)

  Improve the documentation to explain adaptive routing and add notes about
  using CDC in production.

- [#3196](https://github.com/tigerbeetle/tigerbeetle/pull/3196)

  Moves the `unshare` code into `stdx.unshare` and uses it for both Vortex and CFO.
  Also fixes how `vortex run` was handling the child process error code.

### TigerTracks 🎧

- [American Pie - Catch 22 😈](https://www.youtube.com/watch?v=9SzrN3oGCCw)

## TigerBeetle 0.16.56

Released: 2025-08-22

### Safety And Performance

- [#3185](https://github.com/tigerbeetle/tigerbeetle/pull/3185)

  Improve the speed of trailer repairs by initiating repair requests more proactively.

- [#3017](https://github.com/tigerbeetle/tigerbeetle/pull/3017)

  Add [Vortex](https://tigerbeetle.com/blog/2025-02-13-a-descent-into-the-vortex/) to CI to test clients (Java, Zig, Rust).

- [#3193](https://github.com/tigerbeetle/tigerbeetle/pull/3193)

  Ensure only the primary responds to VSR repeat requests.

### Features

- [#3014](https://github.com/tigerbeetle/tigerbeetle/pull/3014)

  Add a typed Python client (thanks @stenczelt).

### Internals

- [#3195](https://github.com/tigerbeetle/tigerbeetle/pull/3195)

  Simplify budgeting for VSR repairs.

- [#3181](https://github.com/tigerbeetle/tigerbeetle/pull/3181)

  Refactor `ReleaseList` to contain the release logic.

- [#3190](https://github.com/tigerbeetle/tigerbeetle/pull/3190)

  Improve naming for journal and grid message budgets.

### TigerTracks 🎧

- [Boiler Room](https://www.youtube.com/watch?v=bk6Xst6euQk)

## TigerBeetle 0.16.55

Released: 2025-08-15

### Safety And Performance

- [3187](https://github.com/tigerbeetle/tigerbeetle/pull/3187)

  Make repair timeout reliably fire in a loaded cluster processing small batches.

- [#2863](https://github.com/tigerbeetle/tigerbeetle/pull/2863)

  Make `tigerbeetle format` concurrent and only write essential data.
  This speeds up the time to format considerably.

- [#3145](https://github.com/tigerbeetle/tigerbeetle/pull/3145)

  Cache prepares from the future, to help avoid needing to repair the WAL near checkpoints when a
  backup is a little behind primary.

### Features

- [#3174](https://github.com/tigerbeetle/tigerbeetle/pull/3174)

  Don't unlink data file on formatting failure.

- [#3173](https://github.com/tigerbeetle/tigerbeetle/pull/3173)

  Use correct default statsd port (8125).

- [#3154](https://github.com/tigerbeetle/tigerbeetle/pull/3154)

  Remove translation logic from old checkpoint state to new. Note that this means that
  `tigerbeetle inspect` will no longer decode superblocks from 0.16.25 or older, until
  they are upgraded to at least 0.16.26.

### Internals

- [#3186](https://github.com/tigerbeetle/tigerbeetle/pull/3186)

  Improvements to the balance bounds, rate limiting, and two phase transfers recipes. Thanks @snth!

- [#3150](https://github.com/tigerbeetle/tigerbeetle/pull/3150)

  Use true quine to generate unit tests.

- [#3160](https://github.com/tigerbeetle/tigerbeetle/pull/3160)

  Drop `SigIllHandler`. This was supposed to print a nice error message on unsupported
  architectures, but we hit `SigIll` in Zig's `_start`, before we get to our `main`.

- [#3148](https://github.com/tigerbeetle/tigerbeetle/pull/3148)

  Add constants for KiB thru PiB.

### TigerTracks 🎧

- [Heavyweight](https://www.youtube.com/watch?v=9Axg_e8astI)

## TigerBeetle 0.16.54

Released: 2025-08-08

### Safety And Performance

- [#3123](https://github.com/tigerbeetle/tigerbeetle/pull/3123)

  Speed up repair by removing a round-trip to fetch headers.

- [#3134](https://github.com/tigerbeetle/tigerbeetle/pull/3134)

  Check checksums when downloading Zig during the build.

### Features

- [#2993](https://github.com/tigerbeetle/tigerbeetle/pull/2993)

  Add documentation for Rust client library.

- [#2989](https://github.com/tigerbeetle/tigerbeetle/pull/2989)

  Test that release artifacts are fully reproducible.

### Internals

- [#3136](https://github.com/tigerbeetle/tigerbeetle/pull/3136)

  Add a test to test that tests include all the tests.

- [#3143](https://github.com/tigerbeetle/tigerbeetle/pull/3143)

  Remove local variable aliasing as per TigerStyle.

- [#3124](https://github.com/tigerbeetle/tigerbeetle/pull/3124)

  `@splat` all the things.

- [#3135](https://github.com/tigerbeetle/tigerbeetle/pull/3135)

  Use double-entry accounting for allocations.

- [#3129](https://github.com/tigerbeetle/tigerbeetle/pull/3129)

  Remove `git-review`.

- [#3131](https://github.com/tigerbeetle/tigerbeetle/pull/3131),
  [#3130](https://github.com/tigerbeetle/tigerbeetle/pull/3130)

  Show total number of VOPR runs for release.

### TigerTracks 🎧

- [War Pigs](https://www.youtube.com/watch?v=IB6jbWoGtlA&list=RDIB6jbWoGtlA)

## TigerBeetle 0.16.53

Released: 2025-08-01

### Safety And Performance

- [#3090](https://github.com/tigerbeetle/tigerbeetle/pull/3090),
  [#3116](https://github.com/tigerbeetle/tigerbeetle/pull/3116)

  Allowing EWAH to decode bigger free set into smaller. This fixes the `--limit-storage` flag.

- [#3089](https://github.com/tigerbeetle/tigerbeetle/pull/3089)

  Fix Node.js v24 client.

### Features

- [#3119](https://github.com/tigerbeetle/tigerbeetle/pull/3119)

  Add compaction/checkpoint/journal slot count to `tigerbeetle inspect`.

### Internals

- [#3121](https://github.com/tigerbeetle/tigerbeetle/pull/3121)

  During tests, verify that grid read errors correspond to either storage faults or ongoing state
  sync.

- [#3122](https://github.com/tigerbeetle/tigerbeetle/pull/3122)

  Teach snaptest how to decode/encode hex & zon.

- [#3110](https://github.com/tigerbeetle/tigerbeetle/pull/3110)

  Fix typo in `manifest_log_fuzz`.

- [#3113](https://github.com/tigerbeetle/tigerbeetle/pull/3113)

  Test `CreateTransfersResult.exists` in VOPR.

- [#3117](https://github.com/tigerbeetle/tigerbeetle/pull/3117),
  [#3120](https://github.com/tigerbeetle/tigerbeetle/pull/3120)

  `stdx` refactoring.

### TigerTracks 🎧

- [Summer Eyes](https://www.youtube.com/watch?v=4Kc1Cks29-w)

## TigerBeetle 0.16.52

Released: 2025-07-25

### Safety And Performance

- [#3093](https://github.com/tigerbeetle/tigerbeetle/pull/3093)

  Improve repair performance by tracking requested prepares so each is repaired exactly once per
  timeout.

### Internals

- [#2956](https://github.com/tigerbeetle/tigerbeetle/pull/2956)

  In VOPR, model events using nanosecond-resolution timestamps to uncover more interesting
  interleaving.

- [#3088](https://github.com/tigerbeetle/tigerbeetle/pull/3088)
  [#3100](https://github.com/tigerbeetle/tigerbeetle/pull/3100)
  [#3102](https://github.com/tigerbeetle/tigerbeetle/pull/3102)

  Initialize `IO` and `Tracer` early. Avoid comptime type specialization.

- [#3101](https://github.com/tigerbeetle/tigerbeetle/pull/3101)

  Update TigerStyle with additional conventions for naming things and ordering struct fields.

- [#3105](https://github.com/tigerbeetle/tigerbeetle/pull/3105)

  Add `--requests-max` CLI flag to VOPR.

- [#3107](https://github.com/tigerbeetle/tigerbeetle/pull/3107)

  In DevHub, use `font-size-adjust` to better match sizes of sans and monospace text.

- [#3108](https://github.com/tigerbeetle/tigerbeetle/pull/3108)

  Introduce a fixtures module for fuzzing and testing to avoid code duplication.

### TigerTracks 🎧

- [Changes](https://open.spotify.com/track/2wNEcJHnFxoKZIrxjxF5jL)

## TigerBeetle 0.16.51

Released: 2025-07-18


### Safety And Performance

- [#3096](https://github.com/tigerbeetle/tigerbeetle/pull/3096)

  Fix incorrect assert in the commit stall logic.

  This assert could cause the primary to crash while it is injecting a commit stall, if an old
  primary has committed ahead of it.

- [#3008](https://github.com/tigerbeetle/tigerbeetle/pull/3008)

  Improve compaction scheduling algorithm to be more performant and memory efficient.

  Earlier, during each beat, we used to compact each active tree and level, leading to multiple
  context switches. Now, we compact each tree and level to completion before moving on to the next.

### Features

- [#3086](https://github.com/tigerbeetle/tigerbeetle/pull/3086)

  Add metrics that track the time taken to complete read and write IO.

### Internals

- [#3087](https://github.com/tigerbeetle/tigerbeetle/pull/3087)

  Remove comptime type specialization on Time and replace it with a runtime interface.

- [#3085](https://github.com/tigerbeetle/tigerbeetle/pull/3085)

  Change all instances of data block -> value block, more aptly named for blocks containing values.

- [#3084](https://github.com/tigerbeetle/tigerbeetle/pull/3084)

  Update [architecture documentation](docs/internals/ARCHITECTURE.md#systems-thinking) to explain
  how to correctly integrate TigerBeetle into a larger data processing system.

- [#3083](https://github.com/tigerbeetle/tigerbeetle/pull/3083)

  Fix example for voiding pending transfers in the dotnet, go, node, and python clients.

- [#3082](https://github.com/tigerbeetle/tigerbeetle/pull/3082),
  [#3091](https://github.com/tigerbeetle/tigerbeetle/pull/3091)

  Fix broken link to the Viewstamped Replication paper and some typos in the documentation.

- [#3078](https://github.com/tigerbeetle/tigerbeetle/pull/3078)

  Fix VOPR false positive where we erroneously find two different versions of an uncommitted header.

- [#2990](https://github.com/tigerbeetle/tigerbeetle/pull/2990)

  Refine BoundedArrayType API, renaming functions to be shorter and consistent with Queue and Stack.

### TigerTracks 🎧

- [LOVE.](https://open.spotify.com/track/6PGoSes0D9eUDeeAafB2As?si=18f44aa580644f66)

## TigerBeetle 0.16.50

Released: 2025-07-13

### Internals

- [#3076](https://github.com/tigerbeetle/tigerbeetle/pull/3076)

  Cleanup Zig TODO items that have been resolved with the recent upgrade to Zig 0.14.1.

- [#3071](https://github.com/tigerbeetle/tigerbeetle/pull/3071)

  Always copy fields from `vsr_options` to `build_options`, since Zig 0.14.1 removed anonymous
  structs. Thanks @rbino!

- [#3075](https://github.com/tigerbeetle/tigerbeetle/pull/3075),
  [#3074](https://github.com/tigerbeetle/tigerbeetle/pull/3074)

  Use realtime to enforce budget and refresh timeouts in the CFO.

- [#3072](https://github.com/tigerbeetle/tigerbeetle/pull/3072)

  Remove `unwind_tables` from release builds and strip client libraries to reduce binary size.

### TigerTracks 🎧

- [Washday Blues](https://www.youtube.com/watch?v=a77xKtyVKMw)

## TigerBeetle 0.16.49

Released: 2025-07-04

### Safety And Performance

- [#3064](https://github.com/tigerbeetle/tigerbeetle/pull/3064)

  Fix a division by zero when logging CDC metrics, and increase resolution to nanoseconds.

- [#3050](https://github.com/tigerbeetle/tigerbeetle/pull/3050)

  Apply backpressure at primary to mitigate an issue with lagging backups.

### Internals

- [#2705](https://github.com/tigerbeetle/tigerbeetle/pull/2705)

  Upgrade to Zig 0.14.1.

- [#3068](https://github.com/tigerbeetle/tigerbeetle/pull/3068)

  Fix a typo that caused probabilities to be parsed as hexadecimal.

### TigerTracks 🎧

- [Dance of Maria](https://open.spotify.com/track/0f7iz1qAWSz61BdHTXbzvC?si=d1znWf4XR1Gev1RZsgtPpQ)

## TigerBeetle 0.16.48

Released: 2025-07-01

### Internals

- [#3062](https://github.com/tigerbeetle/tigerbeetle/pull/3062)

  Updates the publishing process for the Java client to conform to the Maven Central Repository
  due to the [OSSRH service end-of-life](https://central.sonatype.org/news/20250326_ossrh_sunset/).

- [#3048](https://github.com/tigerbeetle/tigerbeetle/pull/3048),
  [#3047](https://github.com/tigerbeetle/tigerbeetle/pull/3047)

  Fixes and improvements for tracing and metrics.

### TigerTracks 🎧

- [All Shook Up](https://www.youtube.com/watch?v=23zLefwiii4&list=RD23zLefwiii4)

## TigerBeetle 0.16.47

Released: 2025-06-27

Note: This release is missing some client libraries in their respective package managers.

### Safety And Performance

- [#3032](https://github.com/tigerbeetle/tigerbeetle/pull/3032)

  Fix ABI assertions in Rust client.

- [#3039](https://github.com/tigerbeetle/tigerbeetle/pull/3039)

  Swarm test different replication configurations in VOPR.

- [#3053](https://github.com/tigerbeetle/tigerbeetle/pull/3053)

  Supports CDC processing for transfers created by versions earlier than `0.16.29`.
  Fixes a liveness bug that would crash the replica if a CDC query encountered objects
  created with a schema before [#2507](https://github.com/tigerbeetle/tigerbeetle/pull/2507).

### Features

- [#3038](https://github.com/tigerbeetle/tigerbeetle/pull/3038)

  Add `client_request_round_trip` metric to track end-to-end client request latency.

- [#3043](https://github.com/tigerbeetle/tigerbeetle/pull/3043)

  Support `--clients` alongside `--transfer-batch-delay-us` in `tigerbeetle benchmark`.

- [#3056](https://github.com/tigerbeetle/tigerbeetle/pull/3056)

  The command `tigerbeetle inspect constants` prints VSR queue sizes.

### Internals

- [#3045](https://github.com/tigerbeetle/tigerbeetle/pull/3045)

  Define timeouts in terms of `tick_ms`.

- [#3042](https://github.com/tigerbeetle/tigerbeetle/pull/3042)

  Disable "hint" argument for mmap call, which was observed to cause stack overflow.

### TigerTracks 🎧

- [Wishmaster](https://www.youtube.com/watch?v=XCGQiGEYl4Y)

## TigerBeetle 0.16.46

Released: 2025-06-19

### Safety And Performance

- [#3030](https://github.com/tigerbeetle/tigerbeetle/pull/3030)

  Always build tb_client for Rust client in release mode.

### Internals

- [#3031](https://github.com/tigerbeetle/tigerbeetle/pull/3031)

  Prioritize more important fuzzers in CFO.

### TigerTracks 🎧

- [Geef Mij Maar Amsterdam](https://open.spotify.com/track/2eiYJEuVh8axfumgEGvyPz?si=9821ae99edf24415)

## TigerBeetle 0.16.45

Released: 2025-06-13

This release changes the CDC message header to use AMQP signed integers.
The new encoding will be handled transparently by RabbitMQ/AMQP clients. However, code changes
might be necessary if the consumer explicitly relies on the unsigned data type.

### Safety And Performance

- [#3023](https://github.com/tigerbeetle/tigerbeetle/pull/3023)

  Fix a liveness bug related to when replicas are syncing.

- [#3022](https://github.com/tigerbeetle/tigerbeetle/pull/3022)

  Fix a crash related to timing when measuring commit timing.

### Features

- [#2907](https://github.com/tigerbeetle/tigerbeetle/pull/2907)

  Add documentation on how to monitor TigerBeetle, track requests end-to-end for better monitoring.

- [#3019](https://github.com/tigerbeetle/tigerbeetle/pull/3019),
  [#3029](https://github.com/tigerbeetle/tigerbeetle/pull/3029)

  Improves compatibility of our new CDC connector by supporting AMQP signed integer types, and
  fixes an assertion that previously overlooked the possibility of receiving an asynchronous
  `basic_ack` while publishing a batch of messages.
  Thanks @alvinyan-bond for your feedback!

### Internals

- [#3018](https://github.com/tigerbeetle/tigerbeetle/pull/3018)

  Add a recovery smoke test.

### TigerTracks 🎧

- [The Grid](https://open.spotify.com/track/64VYy2f9QBx26P1YjNQrEc?si=d09a228548b14989)

## TigerBeetle 0.16.44

Released: 2025-06-06

### Features

- [#3006](https://github.com/tigerbeetle/tigerbeetle/pull/3006)

  Improve logging for missing replies by including the op number.

### Internals

- [#3011](https://github.com/tigerbeetle/tigerbeetle/pull/3011)

  DevHub now displays how many fuzz runs are executed per minute (VPM).

- [#3010](https://github.com/tigerbeetle/tigerbeetle/pull/3010)

  Remove `cluster` from the MessageBus as part of the MessageBuffer rework.

- [#2992](https://github.com/tigerbeetle/tigerbeetle/pull/2992)

  Handle all message padding uniformly.

- [#3001](https://github.com/tigerbeetle/tigerbeetle/pull/3001)

  Limit the fuzzer processes to 20GiB of RAM.

- [#3009](https://github.com/tigerbeetle/tigerbeetle/pull/3009)

  Prevent stack probing from actually using all of the stack due to unexpected inlining.

### TigerTracks 🎧

- [Don Toliver - Lose My Mind (feat. Doja Cat)](https://www.youtube.com/watch?v=WWEs82u37Mw)

## TigerBeetle 0.16.43

Released: 2025-05-30

This release includes the `tigerbeetle recover` subcommand, which can be used to _safely_ recover a
replica that is permanantly lost.

Additionally, it includes Change Data Capture (CDC) support to stream TigerBeetle state to Advanced
Message Queuing Protocol (AMQP) targets, such as RabbitMQ and other compatible brokers.

Check out the [documentation](https://docs.tigerbeetle.com/operating/) to learn about how to use
CDC and `tigerbeetle recover`!


### Safety And Performance

- [#2996](https://github.com/tigerbeetle/tigerbeetle/pull/2996)

  Add the `tigerbeetle recover` subcommand, to safely recover a replica that is permanantly lost
  (e.g. if the SSD fails).

  Earlier, the only way to recover a permanantly lost replica was using the `tigerbeetle format`
  command. Howerver, this was unsafe, as a newly-formatted replica may nack prepares which its
  previous incarnation acked -- a correctness bug.

- [#2880](https://github.com/tigerbeetle/tigerbeetle/pull/2880)

  Implement an adaptive replication routing protocol to handle changes in network topology.

  To select the best route, primary uses outcome-focused explore-exploit approach. Every once in a
  while, the primary tries an alternative route, and replaces the current route if the alternative
  provides better replication latency.

- [#2970](https://github.com/tigerbeetle/tigerbeetle/pull/2970),
  [#3002](https://github.com/tigerbeetle/tigerbeetle/pull/3002)

  Replica pulls messages from the MessageBus, as opposed to the MessageBus pushing messages.

  Earlier, replicas had to process _every_ message that the bus pushed. This could lead to messages
  being dropped due to lack of available disk read/write IOPs. Now, a replica can "suspend" certain
  messages and return to them later when it has enough IOPs.

### Features

- [#2917](https://github.com/tigerbeetle/tigerbeetle/pull/2917)

  CDC support to stream TigerBeetle state to AMQP targets, such as RabbitMQ and other compatible
  brokers.

  We implement the AMQP 0.9.1 specification instead of AMQP 1.0 as it is simpler and more widely
  supported (e.g., RabbitMQ only recently added native AMQP 1.0 support).

### Internals

- [#2982](https://github.com/tigerbeetle/tigerbeetle/pull/2982)

  Unify the production and testing AOF code paths to make sure the production AOF is rigorously
  fuzzed by the VOPR.

- [#2991](https://github.com/tigerbeetle/tigerbeetle/pull/2991)

  Reduce code duplication while erasing IO callbacks' type.

- [#2998](https://github.com/tigerbeetle/tigerbeetle/pull/2998)

  Track debug build times on DevHub.

- [#2987](https://github.com/tigerbeetle/tigerbeetle/pull/2987),
  [#2988](https://github.com/tigerbeetle/tigerbeetle/pull/2988),
  [#2997](https://github.com/tigerbeetle/tigerbeetle/pull/2997),
  [#2999](https://github.com/tigerbeetle/tigerbeetle/pull/2999)

  Miscellaneous improvements and fixes to CI and release.

### TigerTracks 🎧

- [16 CARRIAGES](https://open.spotify.com/track/6XXxKsu3RJeN3ZvbMYrgQW?si=aa3fcce771d542ac)

## TigerBeetle 0.16.42

Released: 2025-05-23

### Safety And Performance

- [#2980](https://github.com/tigerbeetle/tigerbeetle/pull/2980)

  Fix assert in `fulfill_block`, if a replica receives a block that it didn't ask for from a newer
  replica.

### Internals

- [#2973](https://github.com/tigerbeetle/tigerbeetle/pull/2973)

  Extract the parsing parts of MessageBus into a sans-IO style ReceiveBuffer, that'll be used for
  the [upcoming](https://github.com/tigerbeetle/tigerbeetle/pull/2970) pull based MessageBus.

- [#2979](https://github.com/tigerbeetle/tigerbeetle/pull/2979)

  Speed up the LSM scan fuzzer.

### TigerTracks 🎧

- [Knowing Me, Knowing You](https://www.youtube.com/watch?v=iUrzicaiRLU)

## TigerBeetle 0.16.41

Released: 2025-05-16

### Safety And Performance

- [#2972](https://github.com/tigerbeetle/tigerbeetle/pull/2972)

  Implement request throttling for grid repair, making state sync less chatty over the network.

- [#2957](https://github.com/tigerbeetle/tigerbeetle/pull/2957)

  Improved latency and throughput in prepare repair operations.

### Internals

- [#2967](https://github.com/tigerbeetle/tigerbeetle/pull/2967)

  Make the Quick Start page more direct by showing only the installation instructions for Linux
  by default, with Windows and macOS hidden behind click-to-expand sections.

- [#2958](https://github.com/tigerbeetle/tigerbeetle/pull/2958)

  New `tigerbeetle inspect op` command that displays checkpoints and triggers surrounding a given
  `op` number.

### TigerTracks 🎧

- [Paradise City](https://www.youtube.com/watch?v=Rbm6GXllBiw)

## TigerBeetle 0.16.40

Released: 2025-05-09

### Safety And Performance

- [#2959](https://github.com/tigerbeetle/tigerbeetle/pull/2959)

  Fix AOF `unflushed` assertion.

- [#2950](https://github.com/tigerbeetle/tigerbeetle/pull/2950)

  Decouple prepare repair from header breaks. This helps a replica repair faster, and prevents a
  case where we would request a prepare only to discard it when it arrives.

- [#2945](https://github.com/tigerbeetle/tigerbeetle/pull/2945),
  [#2961](https://github.com/tigerbeetle/tigerbeetle/pull/2961)

  Bump default journal write iops. Previously we were inadvertently throttling repair by not
  allocating enough.

- [#2944](https://github.com/tigerbeetle/tigerbeetle/pull/2944)

  Primary now broadcasts `start_view` message on checkpoint durability.
  This will help syncing replicas update to the latest sync target as early as possible.

- [#2952](https://github.com/tigerbeetle/tigerbeetle/pull/2952)

  Add assert to check return value of `next_batch_of_block_requests`.

### Features

- [#2943](https://github.com/tigerbeetle/tigerbeetle/pull/2943),
  [#2949](https://github.com/tigerbeetle/tigerbeetle/pull/2949),
  [#2951](https://github.com/tigerbeetle/tigerbeetle/pull/2951)

  Improve logging, especially of lagging replicas.

### Internals

- [#2937](https://github.com/tigerbeetle/tigerbeetle/pull/2937)

  Add subcommand to `git-review` to send review comments as a email.

- [#2926](https://github.com/tigerbeetle/tigerbeetle/pull/2926)

  Add subcommand to `git-review` to split suggested code changes out of a review commit.

- [#2940](https://github.com/tigerbeetle/tigerbeetle/pull/2940)

  Speed up fuzz smoke tests.

- [#2941](https://github.com/tigerbeetle/tigerbeetle/pull/2941)

  Improve memory usage of tests 10x and test runtime 10x.

- [#2936](https://github.com/tigerbeetle/tigerbeetle/pull/2936)

  Add arbitrary debug output to CFO seeds.

- [#2931](https://github.com/tigerbeetle/tigerbeetle/pull/2931)

  Fix for multiple CFO branches on same commit.

- [#2964](https://github.com/tigerbeetle/tigerbeetle/pull/2964)

  Reduce closed loop replication quorum from `n` to `n-1`.

### TigerTracks 🎧

- [Field of Stars](https://www.youtube.com/watch?v=l1Pqy1tuVLI)

## TigerBeetle 0.16.39

Released: 2025-05-02

Heads up, we are changing our release process! From this point on, a TigerBeetle release is tagged
on Friday, spends a weekend on the
[CFO fleet](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/scripts/cfo.zig), and is
published on Monday. In other words, you'll still be getting a new release every Monday, but the
date of the release will be set to Friday. This setup allows extra time for fuzzers to find problems
in the specific commit we are trying to release.

### Safety And Performance

- [#2928](https://github.com/tigerbeetle/tigerbeetle/pull/2928)

  Continuously fuzz the release branch, in addition to the main branch and the pull requests.

- [#2923](https://github.com/tigerbeetle/tigerbeetle/pull/2923)

  Eagerly deinitialize clients upon eviction, to proactively sever TCP connections to replicas.

### Features

- [#2679](https://github.com/tigerbeetle/tigerbeetle/pull/2679)

  Add an initial Rust client. Note that it is not published to crates.io yet.

### Internals

- [#2906](https://github.com/tigerbeetle/tigerbeetle/pull/2906)

  Reduce verbosity when running full CI suite locally.

### TigerTracks 🎧

- [Тёплые Коты](https://open.spotify.com/track/4UWBuck7q0VeKYiHrwoqnU?si=0fb7145bc0094ba7)

## TigerBeetle 0.16.38

Released: 2025-04-28

### Safety And Performance

- [#2664](https://github.com/tigerbeetle/tigerbeetle/pull/2664)

  Switch to using gpa as backing allocator for clients.

- [#2902](https://github.com/tigerbeetle/tigerbeetle/pull/2902)

  Set socket options for peer connections.

- [#2665](https://github.com/tigerbeetle/tigerbeetle/pull/2665)

  Add `tb_client_init_parameters` and implement for Python client.

### Features

- [#2921](https://github.com/tigerbeetle/tigerbeetle/pull/2921)

  Fix `tigerbeetle inspect grid` for data files with no used blocks.

### Internals

- [#2914](https://github.com/tigerbeetle/tigerbeetle/pull/2914),
  [#2913](https://github.com/tigerbeetle/tigerbeetle/pull/2913)

  CI improvements.

- [#2918](https://github.com/tigerbeetle/tigerbeetle/pull/2918)

  De-genericify `Queue`.

- [#2912](https://github.com/tigerbeetle/tigerbeetle/pull/2912)

  Modernize `stdx.cut` API

- [#2732](https://github.com/tigerbeetle/tigerbeetle/pull/2732),
  [#2915](https://github.com/tigerbeetle/tigerbeetle/pull/2915),
  [#2920](https://github.com/tigerbeetle/tigerbeetle/pull/2920)

  Trial a git-native + offline-first code review interface.

- [#2924](https://github.com/tigerbeetle/tigerbeetle/pull/2924),
  [#2911](https://github.com/tigerbeetle/tigerbeetle/pull/2911),
  [#2910](https://github.com/tigerbeetle/tigerbeetle/pull/2910),
  [#2909](https://github.com/tigerbeetle/tigerbeetle/pull/2909)

  Devhub improvements. In particular, ensure that a failing canary fuzzer is obvious.

- [#2905](https://github.com/tigerbeetle/tigerbeetle/pull/2905)

  Compile scripts for `zig build ci`.

### TigerTracks 🎧

- [Eventide](https://www.youtube.com/watch?v=uMedRcXMDz0)

## TigerBeetle 0.16.37

Released: 2025-04-21

### Safety And Performance

- [#2896](https://github.com/tigerbeetle/tigerbeetle/pull/2896)

  Fix a bug where VOPR latencies were computed incorrectly when the minimum
  and mean were equal.

### Internals

- [#2893](https://github.com/tigerbeetle/tigerbeetle/pull/2893)

  Improve binary size and compilation time by making implementation of intrusive stack non-generic.

- [#2898](https://github.com/tigerbeetle/tigerbeetle/pull/2898)

  Make the Docs website pass the W3C HTML validation test.

- [#2883](https://github.com/tigerbeetle/tigerbeetle/pull/2883)

  Fix a memory leak in VOPR and apply idiomatic naming conventions for the allocator.

- [#2901](https://github.com/tigerbeetle/tigerbeetle/pull/2901)

  Fix a panic where the VOPR attempts to print a deinitialized packet when debug
  logs are enabled.

### TigerTracks 🎧

- [Money](https://www.youtube.com/watch?v=mSNTa9kmUsk)

## TigerBeetle 0.16.36

Released: 2025-04-14

### Safety And Performance

- [#2891](https://github.com/tigerbeetle/tigerbeetle/pull/2891)

  Fix journal disjoint-buffer assertion.

- [#2887](https://github.com/tigerbeetle/tigerbeetle/pull/2887)

  Make object cache optional. This improves throughput by ~10%, as we can omit the object cache from
  the account events groove, which never uses it.

### Internals

- [#2888](https://github.com/tigerbeetle/tigerbeetle/pull/2888)

  Rename FIFO to Queue

- [#2882](https://github.com/tigerbeetle/tigerbeetle/pull/2882)

  Add `zig build ci` to help run CI checks locally.

### TigerTracks 🎧

- [The Great Gig in the Sky](https://www.youtube.com/watch?v=2PMnJ_Luk_o)

## TigerBeetle 0.16.35

Released: 2025-04-07

Please note that after [#2787](https://github.com/tigerbeetle/tigerbeetle/pull/2787), which adds
batching support for all operations, the `batch_max` limit has changed from 8190 to 8189
accounts/transfers per batch.

### Safety And Performance

- [#2787](https://github.com/tigerbeetle/tigerbeetle/pull/2787)

  Add support for batching multiple independent requests of the same operation within a single VSR
  message, amortizing network and consensus costs.

  Earlier, we batched only create_accounts and create_transfers. Now, we can batch any operation!

### Internals

- [#2878](https://github.com/tigerbeetle/tigerbeetle/pull/2878)

  Use TigerBeetle's time abstraction as opposed to raw OS time in the tracer.

- [#2881](https://github.com/tigerbeetle/tigerbeetle/pull/2881)

  Allow custom network packet delay functions in the VOPR. This allows us to simulate latencies for
  different network topologies, for example the ring/star topology.

- [#2866](https://github.com/tigerbeetle/tigerbeetle/pull/2866),
  [#2874](https://github.com/tigerbeetle/tigerbeetle/pull/2874)

  Fix a VOPR false positive wherein we were accessing a crashed replica's uninitialized memory.

- [#2869](https://github.com/tigerbeetle/tigerbeetle/pull/2869)

  Improve error thrown when an invalid --account/transfer-batch-size is passed to the benchmark CLI.

- [#2876](https://github.com/tigerbeetle/tigerbeetle/pull/2876),
  [#2877](https://github.com/tigerbeetle/tigerbeetle/pull/2877)

  Minor refactors to VSR and VOPR.

- [#2872](https://github.com/tigerbeetle/tigerbeetle/pull/2872),
  [#2873](https://github.com/tigerbeetle/tigerbeetle/pull/2873)

  Fixes and improvemenents in the documentation.

### TigerTracks 🎧

- [All Things Must Pass](https://open.spotify.com/track/1AGridgU0QAWZykQReGWk5?si=5be31abf23d24ef3)

## TigerBeetle 0.16.34

Released: 2025-03-31

### Safety And Performance

- [#2861](https://github.com/tigerbeetle/tigerbeetle/pull/2861)

  Add basic fuzzing for the state machine.

- [#2846](https://github.com/tigerbeetle/tigerbeetle/pull/2846)

  Re-do tickless VOPR to simulate fast IOPs.

- [#2852](https://github.com/tigerbeetle/tigerbeetle/pull/2852)

  Allow simulating one-replica-down scenario in the VOPR.

- [#2858](https://github.com/tigerbeetle/tigerbeetle/pull/2858)

  Print dropped packets in the VOPR.

- [#2850](https://github.com/tigerbeetle/tigerbeetle/pull/2850)

  Various changes for VOPR performance mode.

- [#2821](https://github.com/tigerbeetle/tigerbeetle/pull/2821)

  A quicker request protocol for VSR.

- [#2848](https://github.com/tigerbeetle/tigerbeetle/pull/2848)

  Account for pulses when computing the size of the request queue in VSR.

- [#2853](https://github.com/tigerbeetle/tigerbeetle/pull/2853)

  Fix a possible panic in the Node.js client by handling the "too much data" error.

- [#2860](https://github.com/tigerbeetle/tigerbeetle/pull/2860)

  Fix a possible replica crash if negative timestamps would be provided to AccountFilter or
  QueryFilter.

### Features

- [#2830](https://github.com/tigerbeetle/tigerbeetle/pull/2830)

  Allow `tigerbeetle inspect` to run on open data files. This helps with getting an idea what's
  going on on a running cluster without needing to shut it down first.

### Internals

- [#2833](https://github.com/tigerbeetle/tigerbeetle/pull/2833)

  Vendor our own BitSet in stdx, TigerBeetle's extended standard library.
  This change reduces the Linux binary size by 38KiB.

- [#2862](https://github.com/tigerbeetle/tigerbeetle/pull/2862)

  Track the REPL execution time on [DevHub](https://devhub.tigerbeetle.com).

- [#2859](https://github.com/tigerbeetle/tigerbeetle/pull/2859)

  Fix the wording in the [correcting transfers
  example](https://docs.tigerbeetle.com/coding/recipes/correcting-transfers/#example).
  Thanks @shraddha38!

- [#2845](https://github.com/tigerbeetle/tigerbeetle/pull/2845)

  Remove the global allocator from the fuzzers and pass the allocator explicitly to align more with
  TigerStyle.

- [#2854](https://github.com/tigerbeetle/tigerbeetle/pull/2854)

  Block merges in the CI based on DevHub pipeline results.

- [#2840](https://github.com/tigerbeetle/tigerbeetle/pull/2840)

  Add replica/lsm/grid/journal metrics to VSR.

### TigerTracks 🎧

- [Pushing Ownwards](https://www.youtube.com/watch?v=a7AhS0SxE1s)

## TigerBeetle 0.16.33

Released: 2025-03-24

Note that [#2824](https://github.com/tigerbeetle/tigerbeetle/pull/2824) bumps the oldest supported
client version to 0.16.4, removing backward compatibility with various deprecated features. Please
make sure that all of your clients are running on at least 0.16.4 before upgrading to this release!

### Safety And Performance

- [#2835](https://github.com/tigerbeetle/tigerbeetle/pull/2835)

  Add performance mode to the VOPR, which tracks the number and aggregate size of each kind of
  message during a run.

### Features

- [#2824](https://github.com/tigerbeetle/tigerbeetle/pull/2824)

  Bump the oldest supported client version to 0.16.4, removing backward compatibility with various
  deprecated features.

### Internals

- [#2826](https://github.com/tigerbeetle/tigerbeetle/pull/2826)

  Simplify the idiom around adding elements to lists with comptime known lengths.

- [#2827](https://github.com/tigerbeetle/tigerbeetle/pull/2827)

  Better styling for links and block quotes in the documentation.

- [#2831](https://github.com/tigerbeetle/tigerbeetle/pull/2831)

  Change debug multiversion builds to encapsulate two versions as opposed to five.

- [#2832](https://github.com/tigerbeetle/tigerbeetle/pull/2832)

  Improve CFO efficacy by retaining Zig build cache across fuzzing iterations.

- [#2836](https://github.com/tigerbeetle/tigerbeetle/pull/2836)

  Update TigerStyle to add a new rule about using long form arguments in scripts (--force over -f).

- [#2834](https://github.com/tigerbeetle/tigerbeetle/pull/2834),
  [#2837](https://github.com/tigerbeetle/tigerbeetle/pull/2837),
  [#2839](https://github.com/tigerbeetle/tigerbeetle/pull/2839),
  [#2841](https://github.com/tigerbeetle/tigerbeetle/pull/2841),

  Fix various VOPR false positives.

### TigerTracks 🎧

- [On The Way Home](https://open.spotify.com/track/4Fz1WWr5o0OrlIcZxcyZtK?si=4e48d9e99383409a)

## TigerBeetle 0.16.32

Released: 2025-03-17

### Safety And Performance

- [#2798](https://github.com/tigerbeetle/tigerbeetle/pull/2798),
  [#2815](https://github.com/tigerbeetle/tigerbeetle/pull/2815)

  Vendor the PRNG, and tweak the API to be less wordy.

  PRNG algorithms tend to change, often for reasons not applicable to TigerBeetle. We need neither
  the fastest, nor the most secure PRNG, and it's better if we rotate our PRNG algorithm at our own
  pace.

- [#2813](https://github.com/tigerbeetle/tigerbeetle/pull/2813)

  Fix a case where, if a busy cluster's pipeline is full, the prepare_timeout was never reset.

  This improves performance of a local benchmark for a 6-replica cluster with one replica down and 4
  clients almost ~4x.

### Internals

- [#2804](https://github.com/tigerbeetle/tigerbeetle/pull/2804),
  [#2806](https://github.com/tigerbeetle/tigerbeetle/pull/2806),
  [#2803](https://github.com/tigerbeetle/tigerbeetle/pull/2803)

  Add syntax highlighting to docs code snippets (thanks @nilskch!), fix Python example code (thanks
  @IvoCrnkovic!) and update our HACKING.md with current practices.

- [#2819](https://github.com/tigerbeetle/tigerbeetle/pull/2819),
  [#2814](https://github.com/tigerbeetle/tigerbeetle/pull/2814),
  [#2812](https://github.com/tigerbeetle/tigerbeetle/pull/2812)

  Prepare for the [Zig 0.14 upgrade](https://github.com/tigerbeetle/tigerbeetle/pull/2705) by
  applying as many changes that still work on 0.13 as possible. This includes vendoring AEGIS, to
  keep hash stability, as 0.14 changes the implementation.

### TigerTracks 🎧

- [Get Ready](https://open.spotify.com/track/4tvOVmc2jorV20Z2hFDtDg?si=15eb0ed7536b4ab3)

## TigerBeetle 0.16.31

Released: 2025-03-09

### Safety And Performance

- [#2790](https://github.com/tigerbeetle/tigerbeetle/pull/2790)

  Use LIFO instead of FIFO for free blocks during compaction for better temporal locality.

- [#2799](https://github.com/tigerbeetle/tigerbeetle/pull/2799)

  Disallow converting negative big integers to `UInt128` in the Java and Go clients, as they
  would be incorrectly interpreted as unsigned big integers when converted back.

- [#2801](https://github.com/tigerbeetle/tigerbeetle/pull/2801)

  Remove spurious `write_reply_next()` after read completes in `client_replies`,
  as reads and writes can happen concurrently.

- [#2783](https://github.com/tigerbeetle/tigerbeetle/pull/2783)

  Assert that `cache_map.stash` is not using any element beyond its defined capacity.

### Features

- [#2725](https://github.com/tigerbeetle/tigerbeetle/pull/2725)

  Various REPL fixes and improvements.

- [#2773](https://github.com/tigerbeetle/tigerbeetle/pull/2773)

  Add single-page mode to the documentation website.

### Internals

- [#2791](https://github.com/tigerbeetle/tigerbeetle/pull/2791),
  [#2792](https://github.com/tigerbeetle/tigerbeetle/pull/2792),
  [#2793](https://github.com/tigerbeetle/tigerbeetle/pull/2793),
  [#2794](https://github.com/tigerbeetle/tigerbeetle/pull/2794),
  [#2795](https://github.com/tigerbeetle/tigerbeetle/pull/2795),
  [#2796](https://github.com/tigerbeetle/tigerbeetle/pull/2796),
  [#2807](https://github.com/tigerbeetle/tigerbeetle/pull/2807)

  Miscellaneous documentation typo fixes, clarifications, and references.

- [#2788](https://github.com/tigerbeetle/tigerbeetle/pull/2788),
  [#2802](https://github.com/tigerbeetle/tigerbeetle/pull/2802)

  Ban qualified `std.debug.assert` and `@memcpy`.

- [#2776](https://github.com/tigerbeetle/tigerbeetle/pull/2776),
  [#2800](https://github.com/tigerbeetle/tigerbeetle/pull/2800)

  Fix code comment typos.

- [#2595](https://github.com/tigerbeetle/tigerbeetle/pull/2595)

  Add tracking of format time and startup time to the [devhub](https://devhub.tigerbeetle.com/).

### TigerTracks 🎧

- [The Hardest Button To Button](https://www.youtube.com/watch?v=K4dx42YzQCE)

## TigerBeetle 0.16.30

Released: 2025-03-03

Note: Before performing this upgrade, please make sure to check that no replicas are lagging and
state syncing.

You can ensure this by temporarily pausing load to the TigerBeetle cluster and waiting for all
replicas to catch up. If some replicas in your cluster were indeed lagging, you should see
`on_repair_sync_timeout: request sync; lagging behind cluster` in the logs, followed by
`sync: ops=`, which indicates the end of state sync. If you don't see the former in the logs, then
you are already safe to upgrade!

This is to work around an issue in the upgrade between 0.16.25 → 0.16.26, wherein a state syncing
replica goes into a crash loop when it upgrades to 0.16.26. If one of your replicas has already hit
this crash loop, please reach out to us on the Community Slack so we can help you safely revive it.


### Safety And Performance

- [#2774](https://github.com/tigerbeetle/tigerbeetle/pull/2774)

  Fix TOCTOU bug in our hybrid set-associative cache and hash map structure, wherein a promotion
  to the cache coupled with an eviction from the cache could lead to invalid pointer references.

- [#2771](https://github.com/tigerbeetle/tigerbeetle/pull/2771)

  Add logic to crash replica upon receiving unknown commands from clients and other replicas.

- [#2766](https://github.com/tigerbeetle/tigerbeetle/pull/2766)

  Fix upgrade bug wherein a replica does not detect a change in the binary if it is replaced during
  its initialization in `Replica.open()`.

- [#2761](https://github.com/tigerbeetle/tigerbeetle/pull/2761)

  Alternate replication direction for even and odd ops to better detect breaks in the ring topology.

### Internals

- [#2770](https://github.com/tigerbeetle/tigerbeetle/pull/2770),
  [#2781](https://github.com/tigerbeetle/tigerbeetle/pull/2781),
  [#2779](https://github.com/tigerbeetle/tigerbeetle/pull/2779),
  [#2778](https://github.com/tigerbeetle/tigerbeetle/pull/2778),
  [#2769](https://github.com/tigerbeetle/tigerbeetle/pull/2769),

  Add new docs content to `TigerBeetle Architecture`, fix miscellaneous typos and references.

- [#2760](https://github.com/tigerbeetle/tigerbeetle/pull/2760)

  Simplify idiom around a replica sending messages to itself.

- [#2764](https://github.com/tigerbeetle/tigerbeetle/pull/2764)

  Refactor MessageBus to remove platform-specific IO logic.

### TigerTracks 🎧

- [For Crying Out Loud](https://open.spotify.com/track/4nsd2DbMYqRwkvIQ51r4cp?si=411872dc9c444535)

## TigerBeetle 0.16.29

Released: 2025-02-24

Note: Before performing this upgrade, please make sure to check that no replicas are lagging and
state syncing.

You can ensure this by temporarily pausing load to the TigerBeetle cluster and waiting for all
replicas to catch up. If some replicas in your cluster were indeed lagging, you should see
`on_repair_sync_timeout: request sync; lagging behind cluster` in the logs, followed by
`sync: ops=`, which indicates the end of state sync. If you don't see the former in the logs, then
you are already safe to upgrade!

This is to work around an issue in the upgrade between 0.16.25 → 0.16.26, wherein a state syncing
replica goes into a crash loop when it upgrades to 0.16.26. If one of your replicas has already hit
this crash loop, please reach out to us on the Community Slack so we can help you safely revive it.


### Safety And Performance

- [#2763](https://github.com/tigerbeetle/tigerbeetle/pull/2763)

  Explicitly ignore deprecated protocol messages.

- [#2758](https://github.com/tigerbeetle/tigerbeetle/pull/2758)

  Fix a crash when, during upgrade, replica's binary is changed the second time.

### Features

- [#2698](https://github.com/tigerbeetle/tigerbeetle/pull/2698)

  Implement metrics, using statsd format.

- [#2507](https://github.com/tigerbeetle/tigerbeetle/pull/2507)

  Add new indexes to the account balances to enable CDC.

- [#2521](https://github.com/tigerbeetle/tigerbeetle/pull/2521),
  [#2751](https://github.com/tigerbeetle/tigerbeetle/pull/2751),
  [#2756](https://github.com/tigerbeetle/tigerbeetle/pull/2756),
  [#2757](https://github.com/tigerbeetle/tigerbeetle/pull/2757),
  [#2754](https://github.com/tigerbeetle/tigerbeetle/pull/2754)

  Restructure documentation.

- [#2727](https://github.com/tigerbeetle/tigerbeetle/pull/2727)

  Implement more standard shortcuts for REPL.

### Internals

- [#2742](https://github.com/tigerbeetle/tigerbeetle/pull/2742)

  Refactor C client API to remove internal mutex.

- [#2747](https://github.com/tigerbeetle/tigerbeetle/pull/2747)

  Don't use deprecated Node.js APIs in the samples.

- [#2748](https://github.com/tigerbeetle/tigerbeetle/pull/2748),
  [#2744](https://github.com/tigerbeetle/tigerbeetle/pull/2744)

  Improve documentation search.

- [#2734](https://github.com/tigerbeetle/tigerbeetle/pull/2734)

  Switch to vale for documentation spell checking.


### TigerTracks 🎧

- [Зов Крови](https://open.spotify.com/track/6YS6ZOCL6KX9kDRQRzD9s0?si=46730a9a7a7842ab)

## TigerBeetle 0.16.28

Released: 2025-02-17

Note: Before performing this upgrade, please make sure to check that no replicas are lagging and
state syncing.

You can ensure this by temporarily pausing load to the TigerBeetle cluster and waiting for all
replicas to catch up. If some replicas in your cluster were indeed lagging, you should see
`on_repair_sync_timeout: request sync; lagging behind cluster` in the logs, followed by
`sync: ops=`, which indicates the end of state sync. If you don't see the former in the logs, then
you are already safe to upgrade!

This is to work around an issue in the upgrade between 0.16.25 → 0.16.26, wherein a state syncing
replica goes into a crash loop when it upgrades to 0.16.26. If one of your replicas has already hit
this crash loop, please reach out to us on the Community Slack so we can help you safely revive it.

### Safety And Performance

- [#2677](https://github.com/tigerbeetle/tigerbeetle/pull/2677)

  Test misdirected writes in the VOPR.

- [#2711](https://github.com/tigerbeetle/tigerbeetle/pull/2711)

  Fix a recovery correctness bug caused by a misdirected write in the WAL
  (discovered by the VOPR in #2677).

- [#2728](https://github.com/tigerbeetle/tigerbeetle/pull/2728)

  Refactor the tb_client packet interface, hiding private members in an opaque field.
  Add assertions to enforce expectations for each packet field.

- [#2717](https://github.com/tigerbeetle/tigerbeetle/pull/2717)

  Fix a Node.js client crash when it was closed with outstanding requests.

- [#2720](https://github.com/tigerbeetle/tigerbeetle/pull/2720)

  Flush loopback queue before queueing another prepare_ok.

- [#2730](https://github.com/tigerbeetle/tigerbeetle/pull/2730)

  Fuzzer weights are now configurable.

- [#2702](https://github.com/tigerbeetle/tigerbeetle/pull/2702)

  The REPL now uses `StaticAllocator` on init and deinit.

### Features

- [#2716](https://github.com/tigerbeetle/tigerbeetle/pull/2716)

  `tigerbeetle inspect constants` now prints a napkin math estimate for the memory usage.

### Internals

- [#2719](https://github.com/tigerbeetle/tigerbeetle/pull/2719),
  [#2718](https://github.com/tigerbeetle/tigerbeetle/pull/2718),
  [#2736](https://github.com/tigerbeetle/tigerbeetle/pull/2736)

  Update docs with new talks, an updated illustration, and a new Slack invite link.

- [#2724](https://github.com/tigerbeetle/tigerbeetle/pull/2724)

  Don't expose VSR module to dependents in build.zig.

### TigerTracks 🎧

- [Formula 06](https://open.spotify.com/track/7rzRbj2WnmxcE5iQfGbhKN)

## TigerBeetle 0.16.27

Released: 2025-02-10

Note: Before performing this upgrade, please make sure to check that no replicas are lagging and
state syncing.

You can ensure this by temporarily pausing load to the TigerBeetle cluster and waiting for all
replicas to catch up. If some replicas in your cluster were indeed lagging, you should see
`on_repair_sync_timeout: request sync; lagging behind cluster` in the logs, followed by
`sync: ops=`, which indicates the end of state sync. If you don't see the former in the logs, then
you are already safe to upgrade!

This is to work around an issue in the upgrade between 0.16.25 → 0.16.26, wherein a state syncing
replica goes into a crash loop when it upgrades to 0.16.26. If one of your replicas has already hit
this crash loop, please reach out to us on the Community Slack so we can help you safely revive it.

### Safety And Performance

- [#2700](https://github.com/tigerbeetle/tigerbeetle/pull/2700)

  Remove redundant calls to `IO.init()` and `IO.deinit()` during the `format` and `start` commands.

  These redundant calls could lead to an assertion error in the Zig standard library when a failure
  occurs after the second `IO.init()`.

### Features

- [#2701](https://github.com/tigerbeetle/tigerbeetle/pull/2701)

  Enhance the docs search bar with arrow-key navigation over search results, and folder collapse
  using the enter key.

### Internals
- [#2713](https://github.com/tigerbeetle/tigerbeetle/pull/2713)

  Add [talks](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TALKS.md) from
  SystemsDistributed '23, P99 CONF '23, Money2020 '24, and SYCL '24.

- [#2710](https://github.com/tigerbeetle/tigerbeetle/pull/2710)

  Improve CPU utilization of the CFO by spawning fuzzers more frequently.

  Motivated by the measurement that the cumulative CPU was (on average) 40-45% idle.

- [#2709](https://github.com/tigerbeetle/tigerbeetle/pull/2709)

  Assert zeroed padding for WAL prepares in the VOPR.

- [#2703](https://github.com/tigerbeetle/tigerbeetle/pull/2703)

  Remove the deprecated version of the start_view message.

  As part of [#2600](https://github.com/tigerbeetle/tigerbeetle/pull/2600), we rolled out a new
  on-disk format for the CheckpointState. To avoid bumping the VSR version, we made it so that
  replicas temporarily send two versions of the start_view message, with both the old and new
  CheckpointState formats.

- [#2697](https://github.com/tigerbeetle/tigerbeetle/pull/2697)

  Add fair scheduler to the CFO to avoid starvation of short running fuzzers.

  Earlier, long running LSM fuzzers ended up spending more than their fair share of time on the CPU,
  with only 1-10% of CFO time being spent on short running VOPR fuzzers.


### TigerTracks 🎧

- [Neon](https://open.spotify.com/track/7Kohy4v3KLWfUXlv9N3feB?si=a1bb8f16679a44d3)

## TigerBeetle 0.16.26

Released: 2025-02-03

Note: Before performing this upgrade, please make sure to check that no replicas are lagging and
state syncing.

You can ensure this by temporarily pausing load to the TigerBeetle cluster and waiting for all
replicas to catch up. If some replicas in your cluster were indeed lagging, you should see
`on_repair_sync_timeout: request sync; lagging behind cluster` in the logs, followed by
`sync: ops=`, which indicates the end of state sync. If you don't see the former in the logs, then
you are already safe to upgrade!

This is to work around an issue in the upgrade between 0.16.25 → 0.16.26, wherein a state syncing
replica goes into a crash loop when it upgrades to 0.16.26. If one of your replicas has already hit
this crash loop, please reach out to us on the Community Slack so we can help you safely revive it.

### Safety And Performance

- [#2681](https://github.com/tigerbeetle/tigerbeetle/pull/2681)

  Consider blocks and prepares with nonzero padding to be corrupt.
  Previously blocks asserted zero padding, which can fail due to bitrot.

  Also fix a similar bug in the superblock copy index handling. The copy index is not covered
  by a checksum, so we must treat it carefully to avoid propagating bad data if it is corrupt.

  VOPR now injects single-bit errors into storage rather than whole-sector errors.

- [#2600](https://github.com/tigerbeetle/tigerbeetle/pull/2600)

  The current checkpoint process immediately frees all blocks released in the previous checkpoint.
  This can lead to cluster unavailability by prematurely freeing and overwriting released blocks.

  To fix this, delay freeing blocks until the checkpoint is durable on a commit-quorum, ensuring
  data integrity and preventing single-replica failures (in a 3 node cluster) from impacting
  availability.

- [#2692](https://github.com/tigerbeetle/tigerbeetle/pull/2692)

  When state syncing, replicas would send prepare_oks only up to a point, to ensure they don't
  falsely contribute to the durability of a non-durable checkpoint they've synced to.

  However, the logic to send these prepare_oks after state sync has finished was missing, which
  could lead to a situation where a primary was unavailable to advance. Add in the ability to send
  these prepare_oks after syncing.

- [#2689](https://github.com/tigerbeetle/tigerbeetle/pull/2689)

  Recently, tb_client was reworked to use OS native signals instead of a socket for delivering
  cross thread events.

  Fix some incorrect asserts, and add a fuzz test.

### Features

- [#2694](https://github.com/tigerbeetle/tigerbeetle/pull/2694),
  [#2695](https://github.com/tigerbeetle/tigerbeetle/pull/2695),
  [#2686](https://github.com/tigerbeetle/tigerbeetle/pull/2686),
  [#2688](https://github.com/tigerbeetle/tigerbeetle/pull/2688),
  [#2685](https://github.com/tigerbeetle/tigerbeetle/pull/2685),
  [#2676](https://github.com/tigerbeetle/tigerbeetle/pull/2676),
  [#2684](https://github.com/tigerbeetle/tigerbeetle/pull/2684)

  A few fixes and an "Edit this page" button for our new docs!

### Internals

- [#2674](https://github.com/tigerbeetle/tigerbeetle/pull/2674)

  Allocate the reply buffer in the Go client once the reply has been received. This can save up to
  1MB of memory.

- [#2680](https://github.com/tigerbeetle/tigerbeetle/pull/2680)

  Refactor parts of our CFO, the process responsible for running fuzzers and the VOPR and sending
  the results to devhub, to better handle OOM in subprocesses and reduce false fuzz failures.

### TigerTracks 🎧

- [Like a Prayer](https://open.spotify.com/track/1z3ugFmUKoCzGsI6jdY4Ci)

## TigerBeetle 0.16.25

Released: 2025-01-27

### Safety And Performance

- [#2653](https://github.com/tigerbeetle/tigerbeetle/pull/2653)

  Avoid considering just-repaired journal headers as faulty during WAL recovery.

- [#2655](https://github.com/tigerbeetle/tigerbeetle/pull/2655)

  Reduce the minimum exponential backoff delay from 100ms (which is too pessimistic) to 10ms,
  a value more appropriate for fast networks.

- [#2662](https://github.com/tigerbeetle/tigerbeetle/pull/2662)

  Introduce a new `CheckpointState` format on disk, which ensures that blocks released during
  a checkpoint are freed only when the next checkpoint is durable, solving a known
  [liveness issue](https://github.com/tigerbeetle/tigerbeetle/pull/2600).
  The previous format is still supported and will be removed in a future release to ensure the
  proper upgrade path.

- [#2605](https://github.com/tigerbeetle/tigerbeetle/pull/2605)

  Demote a clock skew _warning_ to a _debug message_ when the ping time is legitimately behind
  the window. On the other hand, assert that the monotonic clock is within the window.

- [#2659](https://github.com/tigerbeetle/tigerbeetle/pull/2659)

  Workaround to prevent the initialization value for the AOF message from being embedded as a
  binary resource, saving `constants.message_size_max` bytes in the executable size!

- [#2654](https://github.com/tigerbeetle/tigerbeetle/pull/2654)

  Fix a VOPR false positive where it erroneously infers that a replica has lost a prepare that
  it has acknowledged.

### Features

- [#2479](https://github.com/tigerbeetle/tigerbeetle/pull/2479),
  [#2663](https://github.com/tigerbeetle/tigerbeetle/pull/2663)

  New statically generated docs website, featuring many UX improvements while removing tons of
  dependencies! Check it out at https://docs.tigerbeetle.com/

### Internals

- [#2661](https://github.com/tigerbeetle/tigerbeetle/pull/2661)

  Make the CFO utilize all cores all the time for running tests, pushing updates every 5 minutes.

### TigerTracks 🎧

- [Correnteza](https://www.youtube.com/watch?v=6m4DSpZgxZw)

## TigerBeetle 0.16.23

Released: 2025-01-20

## TigerBeetle (unreleased)

Released: 2025-01-20

(Unreleased due to CI flake.)

### Safety And Performance

- [#2652](https://github.com/tigerbeetle/tigerbeetle/pull/2652)

  Fix Python client initialization with long address strings.

- [#2614](https://github.com/tigerbeetle/tigerbeetle/pull/2614)

  Prevent Client IO thread starvation.

- [#2583](https://github.com/tigerbeetle/tigerbeetle/pull/2583)

  Set prepare/request timeout RTTs dynamically. Previously our backoff was almost always much too
  high, leading to much lower throughput when ring replication failed.

### Features

- [#2580](https://github.com/tigerbeetle/tigerbeetle/pull/2580)

  Add `--clients` flag to `tigerbeetle benchmark` for generating load from multiple concurrent
  clients.

### Internals

- [#2651](https://github.com/tigerbeetle/tigerbeetle/pull/2651)

  For test/verify only functions, assert `constants.verify` at compile-time, not runtime.

- [#2650](https://github.com/tigerbeetle/tigerbeetle/pull/2650)

  Fix flaky Java client test `testConcurrentInterruptedTasks`.

- [#2646](https://github.com/tigerbeetle/tigerbeetle/pull/2646)

  Fix VOPR false positive due to `smallest_missing_prepare_between` bug.

- [#2611](https://github.com/tigerbeetle/tigerbeetle/pull/2611)

  In clients, add `Event`s for efficient cross-thread notification.

- [#2644](https://github.com/tigerbeetle/tigerbeetle/pull/2644),
  [#2648](https://github.com/tigerbeetle/tigerbeetle/pull/2648)

  Devhub fixes due to Ubuntu/kcov.

### TigerTracks 🎧

- [Have You Ever Seen The Rain?](https://www.youtube.com/watch?v=bO28lB1uwp4)

## TigerBeetle 0.16.21

Released: 2025-01-13

Happy 2025!

### Safety And Performance

- [#2637](https://github.com/tigerbeetle/tigerbeetle/pull/2637)

  Fix multiple VOPR false positives

- [#2632](https://github.com/tigerbeetle/tigerbeetle/pull/2632)

  Disable costly cache map verification: trust, verify, but mind big-O!

- [#2629](https://github.com/tigerbeetle/tigerbeetle/pull/2629)

  Improve state sync performance by getting rid of `awaiting_checkpoint` state.

- [#2624](https://github.com/tigerbeetle/tigerbeetle/pull/2624)

  Improve VOPR coverage when running out of IOPs.

- [#2593](https://github.com/tigerbeetle/tigerbeetle/pull/2593),
  [#2623](https://github.com/tigerbeetle/tigerbeetle/pull/2623),
  [#2625](https://github.com/tigerbeetle/tigerbeetle/pull/2625),
  [#2626](https://github.com/tigerbeetle/tigerbeetle/pull/2626),
  [#2627](https://github.com/tigerbeetle/tigerbeetle/pull/2627),
  [#2628](https://github.com/tigerbeetle/tigerbeetle/pull/2628)

  Improve WAL repair performance.

- [#2613](https://github.com/tigerbeetle/tigerbeetle/pull/2613),
  [#2616](https://github.com/tigerbeetle/tigerbeetle/pull/2616),
  [#2620](https://github.com/tigerbeetle/tigerbeetle/pull/2620),
  [#2622](https://github.com/tigerbeetle/tigerbeetle/pull/2622)

  Improve replication performance.

- [#2618](https://github.com/tigerbeetle/tigerbeetle/pull/2618)

  Add simple metrics to the state machine.

### Features

- [#2615](https://github.com/tigerbeetle/tigerbeetle/pull/2615)

  Greatly improve performance of append-only file (AOF). Note that this changes format of AOF on
  disk.

- [#2641](https://github.com/tigerbeetle/tigerbeetle/pull/2641)

  Add `tigerbeetle inspect constants` command to visualize important compile-time parameters.

- [#2630](https://github.com/tigerbeetle/tigerbeetle/pull/2630)

  Use asynchronous disk IO on Windows (as a reminder, at the moment TigerBeetle server is considered
  to production-ready only on Linux).

- [#2635](https://github.com/tigerbeetle/tigerbeetle/pull/2635)

  Add experimental alternative replication topologies (star and closed loop).

### Internals

- [#2634](https://github.com/tigerbeetle/tigerbeetle/pull/2634)

  Move RingBufferType into stdx, TigerBeetle's extended standard library.

- [#2639](https://github.com/tigerbeetle/tigerbeetle/pull/2639)

  Run `go vet` on CI.

- [#2631](https://github.com/tigerbeetle/tigerbeetle/pull/2631)

  Fix tracing compatibility with perfetto.

- [#2564](https://github.com/tigerbeetle/tigerbeetle/pull/2564),
  [#2559](https://github.com/tigerbeetle/tigerbeetle/pull/2559).

  Make it easier to investigate Vortex runs.

- [#2610](https://github.com/tigerbeetle/tigerbeetle/pull/2610)

  Correctly calculate the number of results in the Go client. Previously, the answer was correct
  despite the logic being wrong!

### TigerTracks 🎧

- [Blush Response](https://open.spotify.com/track/0cSnUM2fNEx4pAkNfWpdkU?si=9d6eaaacfc5e467b)

## TigerBeetle 0.16.20

Released: 2024-12-27

### Safety And Performance

- [#2608](https://github.com/tigerbeetle/tigerbeetle/pull/2608)

  Improve replication reliability for tiny messages.

### Features

- [#2607](https://github.com/tigerbeetle/tigerbeetle/pull/2607)

  Add info-level logging for basic progress events.

- [#2603](https://github.com/tigerbeetle/tigerbeetle/pull/2603)

  Add logging and runtime configuration parameters for state sync.

### Internals

- [#2604](https://github.com/tigerbeetle/tigerbeetle/pull/2604)

  Fix `fuzz_lsm_scan` checkpoint schedule.

### TigerTracks 🎧

- [Street Spirit](https://open.spotify.com/track/2QwObYJWyJTiozvs0RI7CF?si=d0c0c7d6f9e946a4)

## TigerBeetle 0.16.19

Released: 2024-12-22

### Safety And Performance

- [#2592](https://github.com/tigerbeetle/tigerbeetle/pull/2592)

  Coalesce LSM tables in memory before writing them to disk. This significantly improves workloads
  with small batch sizes, that would otherwise churn in the top level of the LSM while incurring
  heavy write amplification.

### Features

- [#2590](https://github.com/tigerbeetle/tigerbeetle/pull/2590)

  TigerBeetle recently gained the ability to do runtime debug logging with `--log-debug`. Extend
  that to other subcommands - not just `start`.

  Additionally, the Python client now has logs integrated with Python's native `logging` module,
  which means no more printing to stderr!

### Internals

- [#2591](https://github.com/tigerbeetle/tigerbeetle/pull/2591)

  Fix broken links to TigerBeetle blog posts, thanks @PThorpe92!

### TigerTracks 🎧

- [Unwritten](https://www.youtube.com/watch?v=b7k0a5hYnSI)

## TigerBeetle 0.16.18

Released: 2024-12-19

### Safety And Performance

- [#2584](https://github.com/tigerbeetle/tigerbeetle/pull/2584)

  Our repair can create a feedback loop. Repair requests prepares and headers, but, upon receiving
  back, we re-trigger repair, which could lead to duplicate repair work.

  To avoid that, make sure that we are not sending more than two repair messages per replica per our
  repair timeout.

- [#2586](https://github.com/tigerbeetle/tigerbeetle/pull/2586)

  Trace AOF write duration

- [#2582](https://github.com/tigerbeetle/tigerbeetle/pull/2582)

  Timeouts with exponential backoff should reset to their original delay when the timeout is
  stopped.

- [#2577](https://github.com/tigerbeetle/tigerbeetle/pull/2577)

  Assert against ABA problem during commit.

- [#2578](https://github.com/tigerbeetle/tigerbeetle/pull/2578)

  Add retry for `flock`. `flock`s are cleaned up by the kernel when the file descriptor is closed,
  but since that file descriptor is used by io_uring, it actually outlives the process itself.

- [#2579](https://github.com/tigerbeetle/tigerbeetle/pull/2579)

  Fix `mlock` flag value.

- [#2571](https://github.com/tigerbeetle/tigerbeetle/pull/2571)

  Don't crash if a round of view changes happened while repairing the pipeline.

### Features

- [#2423](https://github.com/tigerbeetle/tigerbeetle/pull/2423)

  Tab completion in REPL

- [#2566](https://github.com/tigerbeetle/tigerbeetle/pull/2566)

  Fix `--account-count-hot` flag.

- [#2576](https://github.com/tigerbeetle/tigerbeetle/pull/2576)

  Fix multi-debit recipe.

### TigerTracks 🎧

- [By The Way](https://www.youtube.com/watch?v=qxQnSH3x3sg)

## TigerBeetle (unreleased)

Released: 2024-12-16

### Safety And Performance

- [#2537](https://github.com/tigerbeetle/tigerbeetle/pull/2537)

  Exit cleanly if the database grows to the maximum size.

- [#2511](https://github.com/tigerbeetle/tigerbeetle/pull/2511)

  Simulate virtual machine migration in the VOPR.

- [#2561](https://github.com/tigerbeetle/tigerbeetle/pull/2561)

  Test that Java client behaves correctly when its thread is interrupted.

### Features

- [#2540](https://github.com/tigerbeetle/tigerbeetle/pull/2540)

  `tigerbeetle format` now automatically generates a random cluster id if it isn't passed on the
  command line. To avoid operational errors, it is important that each cluster gets a globally
  unique id.

- [#2558](https://github.com/tigerbeetle/tigerbeetle/pull/2558),
  [#2552](https://github.com/tigerbeetle/tigerbeetle/pull/2552)

  Improve error reporting when a client gets evicted due to a mismatched version.

### Internals

- [#2542](https://github.com/tigerbeetle/tigerbeetle/pull/2542)

  Switch CLI client to static allocation.

- [#2562](https://github.com/tigerbeetle/tigerbeetle/pull/2562),
  [#2560](https://github.com/tigerbeetle/tigerbeetle/pull/2560)

  Run benchmark under sudo to enable memory locking for accurate RSS stats.

- [#2556](https://github.com/tigerbeetle/tigerbeetle/pull/2556),
  [#2555](https://github.com/tigerbeetle/tigerbeetle/pull/2555)

  Remove many false negatives from unused imports tidy check.

### TigerTracks 🎧

- [You Bring Out The Boogie In Me](https://youtu.be/0AsLHBAbByk)

## TigerBeetle 0.16.17

Released: 2024-12-09

This release includes a correctness fix for the preview API `get_account_balances`,
`get_account_transfers`, `query_accounts`, and `query_transfers`. If your application uses these
features it might be affected.

### Safety And Performance

- [#2544](https://github.com/tigerbeetle/tigerbeetle/pull/2544)

  Fix correctness bug which affected the preview `get_account_balances`, `get_account_transfers`,
  `query_accounts`, and `query_transfers` queries. Specifically, if several filters are used (that
  is, several fields in `QueryFilter` or `AccountFilter` are set), then some objects might be
  missing from the result set.

  The underlying data is safely stored in the database, so re-running these queries using the new
  version of TigerBeetle will give correct results.

- [#2550](https://github.com/tigerbeetle/tigerbeetle/pull/2550)

  Fix panic in the node client which occurred on eviction.

- [#2534](https://github.com/tigerbeetle/tigerbeetle/pull/2534)

  Fix incorrect ABI used on aarch64 for the client library. To prevent such issues from cropping up
  in the future, add aarch64 testing to CI.

- [#2520](https://github.com/tigerbeetle/tigerbeetle/pull/2520)

  Add extra assertions to verify object cache consistency.

- [#2485](https://github.com/tigerbeetle/tigerbeetle/pull/2485)

  Implement network fault injection in Vörtex, our non-deterministic whole system simulator.

### Features

- [#2539](https://github.com/tigerbeetle/tigerbeetle/pull/2539)

  Log level can now be specified at runtime.

- [#2538](https://github.com/tigerbeetle/tigerbeetle/pull/2538)

  Log messages now include UTC timestamp (formatted as per [RFC 3339](https://www.rfc-editor.org/rfc/rfc3339)).

### Internals

- [#2543](https://github.com/tigerbeetle/tigerbeetle/pull/2543)

  Relax compiler requirements for building TigerBeetle. While we only support building using one
  specific version of Zig, the one downloaded via `./zig/download.sh`, you can try using other
  versions.

- [#2533](https://github.com/tigerbeetle/tigerbeetle/pull/2533)

  Document how to handle errors during release. TigerBeetle's release process is complicated, as we
  need to release, in lockstep, both the `tigerbeetle` binary and all the related client libraries.
  The release process is intentionally designed to be "highly-available", such that a failure of any
  aspect of a release can be safely detected, isolated, and repaired. But, so far, this wasn't
  clearly spelled out in the documentation!

### TigerTracks 🎧

- [The Beetle's Buzz](https://soundcloud.com/kprotty/the-beetles-buzz)

## TigerBeetle 0.16.16

Released: 2024-12-02

The highlight of today's release is the new official Python client, implemented in
[#2527](https://github.com/tigerbeetle/tigerbeetle/pull/2527), and
[#2487](https://github.com/tigerbeetle/tigerbeetle/pull/2487). Please kick the tires!

### Safety And Performance

- [#2517](https://github.com/tigerbeetle/tigerbeetle/pull/2517)

  Require that all replicas in a cluster have the latest TigerBeetle binary as a precondition for an
  upgrade.

- [#2506](https://github.com/tigerbeetle/tigerbeetle/pull/2506)

  Fix several bugs when handling misdirected writes. That is, situations when the disk reports
  a write as successful, despite the write ending up in the wrong place on the disk!

- [#2478](https://github.com/tigerbeetle/tigerbeetle/pull/2478)

  Make sure that TigerBeetle memory is not swappable, otherwise a storage fault can occur in a
  currently swapped-out page, circumventing TigerBeetle guarantees.

- [#2522](https://github.com/tigerbeetle/tigerbeetle/pull/2522)

  REPL correctly emits errors when several objects are used as an argument of an operation that
  only works for a single object, like `get_account_transfers`.

- [#2502](https://github.com/tigerbeetle/tigerbeetle/pull/2502)

  Add randomized integration tests for the Java client.

### Features

- [#2527](https://github.com/tigerbeetle/tigerbeetle/pull/2527),
  [#2487](https://github.com/tigerbeetle/tigerbeetle/pull/2487)

  The Python client!

### Internals

- [#2526](https://github.com/tigerbeetle/tigerbeetle/pull/2526)

  Ignore OOM failures during fuzzing. Fuzzers normally don't use that much memory, but, depending on
  random parameters selected by swarm testing, there are big outliers. If all concurrent fuzzers hit
  a seed that requires a lot of memory, a fuzzing machine runs out of physical RAM. Handle such
  errors and don't treat them as fuzzing failures.

- [#2510](https://github.com/tigerbeetle/tigerbeetle/pull/2510)

  Track the number of untriaged issues on [DevHub](https://tigerbeetle.github.io/tigerbeetle/).

### TigerTracks 🎧

- [Always Look On The Bright Side of Life](https://www.youtube.com/watch?v=X_-q9xeOgG4)

## TigerBeetle 0.16.14

Released: 2024-11-25

### Safety And Performance

- [#2501](https://github.com/tigerbeetle/tigerbeetle/pull/2501)

  Call `DetachCurrentThread` when the Java client is closed. The underlying Zig TigerBeetle client
  runs in a separate thread internally, and a handler to this thread was being leaked.

  This is not noticeable in normal operation, but could impact long running processes that
  create and close clients frequently.

- [#2492](https://github.com/tigerbeetle/tigerbeetle/pull/2492)

  Document that it's not possible to currently look up or query more than a full batch of accounts
  atomically without using the history flag and querying balances.

- [#2434](https://github.com/tigerbeetle/tigerbeetle/pull/2434)

  Add the ability to check timestamp order - and verify they are monotonically increasing - for
  accounts and transfers inside Vortex.

### Internals

- [#2455](https://github.com/tigerbeetle/tigerbeetle/pull/2455)

  Recently the VOPR has gotten too good, and it's very tempting to switch to an empirical mode of
  coding: write some code and let the VOPR figure out whether it is correct or not.

  This is suboptimal - silence of the VOPR doesn't guarantee total absence of bugs and safety comes
  in layers and cross checks. Just formal or informal reasoning is not enough, we need both.

  Document this in TigerStyle.

- [#2472](https://github.com/tigerbeetle/tigerbeetle/pull/2472)

  Previously, the Zig part of languages clients logged directly to stderr using Zig's `std.log`, but
  since directly outputting to stderr is considered rude for a library, logging was disabled.

  This PR adds in scaffolding for sending these logs to the client language to be handled there,
  tying in with native log libraries (e.g., Log4j). No languages use it yet, however.

  Additionally, log `warn` and `err` directly to stderr, if there's no handler.

### TigerTracks 🎧

- [Hello](https://www.youtube.com/watch?v=kK42LZqO0wA)

## TigerBeetle 0.16.13

Released: 2024-11-18

### Safety And Performance

- [#2461](https://github.com/tigerbeetle/tigerbeetle/pull/2461)

  Fix a broken assert when a recently-state-synced replica that has not completed journal repair
  receives an old `commit` message.

- [#2474](https://github.com/tigerbeetle/tigerbeetle/pull/2474)

  Retry `EAGAIN` on (disk) reads. This is essential for running TigerBeetle on XFS, since XFS
  returns `EAGAIN` unexpectedly.

- [#2476](https://github.com/tigerbeetle/tigerbeetle/pull/2476)

  Fix a message bus crash when a client reconnects to a replica without the replica receiving a
  disconnect for the first connection.

- [#2475](https://github.com/tigerbeetle/tigerbeetle/pull/2475)

  Save 256KiB of RAM by not having a prefetch cache for historical balances.
  (Historical balances are never prefetched, so this cache was unused.)

- [#2482](https://github.com/tigerbeetle/tigerbeetle/pull/2482)

  Update hardware requirements in the documentation to include the recommended network bandwidth,
  advice for very large data files, and farther emphasis on the importance of ECC RAM.

- [#2484](https://github.com/tigerbeetle/tigerbeetle/pull/2484)

  Don't panic the client when the client's session is
  [evicted](https://docs.tigerbeetle.com/reference/sessions/#eviction).
  Instead, report an error any time a new batch is submitted to the evicted client.
  (How the error is reported depends on the client language – e.g. Java throws an exception, whereas
  Node.js rejects the `Promise`).

  Note that if running clients are evicted, that typically indicates that there are too many clients
  running – check out the
  [suggested system architecture](https://docs.tigerbeetle.com/coding/system-architecture/).

### Features

- [#2464](https://github.com/tigerbeetle/tigerbeetle/pull/2464)

  Add REPL interactivity. Also change the REPL from dynamic to static allocation.
  Thanks @wpaulino!

### Internals

- [#2481](https://github.com/tigerbeetle/tigerbeetle/pull/2481)

  Expose the VSR timestamp to the client. (This is an experimental feature which will be removed
  soon – don't use this!)

### TigerTracks 🎧

- [Olympic Airways](https://www.youtube.com/watch?v=4BcNLA2KB2c)

## TigerBeetle 0.16.12

Released: 2024-11-11

### Safety And Performance

- [#2435](https://github.com/tigerbeetle/tigerbeetle/pull/2435)

  Fix an attempt to access uninitialized fields of `tb_packet_t` when `tb_client_deinit` aborts
  pending requests. Also add Java unit tests to reproduce the problem and validate the fix.

- [#2437](https://github.com/tigerbeetle/tigerbeetle/pull/2437)

  Fix a liveness issue where the cluster gets stuck despite sufficient durability, caused by buggy
  logic for cycling through faulty blocks during repair. Now, we divide the request buffer between
  the `read_global_queue` and `faulty_blocks`, ensuring that we always request blocks from both.

### Features

- [#2462](https://github.com/tigerbeetle/tigerbeetle/pull/2462)

  Update DevHub styling.

- [#2424](https://github.com/tigerbeetle/tigerbeetle/pull/2424)

  Vortex can now not only crash replicas (by killing and restarting the process) but also stop and
  resume them.

### Internals

- [#2458](https://github.com/tigerbeetle/tigerbeetle/pull/2458)

  Fix the Dotnet walkthrough example that misused `length` instead of the final index when slicing
  an array. Thanks @tenatus for reporting it!

- [#2453](https://github.com/tigerbeetle/tigerbeetle/pull/2453)

  Fix a CI failure caused by concurrent processes trying to create the `fs_supports_direct_io`
  probe file in the same path.

- [#2441](https://github.com/tigerbeetle/tigerbeetle/pull/2441)

  Replace curl shell invocation with Zig's http client. 😎

- [#2454](https://github.com/tigerbeetle/tigerbeetle/pull/2454)

  Properly handle "host unreachable" (`EHOSTUNREACH`) on Linux, instead of returning unexpected
  error.

- [#2443](https://github.com/tigerbeetle/tigerbeetle/pull/2443),
  [#2451](https://github.com/tigerbeetle/tigerbeetle/pull/2451),
  [#2459](https://github.com/tigerbeetle/tigerbeetle/pull/2459),
  [#2460](https://github.com/tigerbeetle/tigerbeetle/pull/2460),
  [#2465](https://github.com/tigerbeetle/tigerbeetle/pull/2465)

  Various code refactorings to improve naming conventions, readability, and organization.

### TigerTracks 🎧

- [Scatman (Ski-ba-bop-ba-dop-bop)](https://www.youtube.com/watch?v=ZhSY7vYbXkk&t=106s)

## TigerBeetle (unreleased)

Released: 2024-11-04

### Safety And Performance

- [#2356](https://github.com/tigerbeetle/tigerbeetle/pull/2356)

  Add "Vortex" – a full-system integration test.
  Notably, unlike the VOPR this test suite covers the language clients.

- [#2430](https://github.com/tigerbeetle/tigerbeetle/pull/2430)

  Cancel in-flight async (Linux) IO before freeing memory. This was not an issue on the replica
  side, as replicas only stop when their process stops. However, clients may be closed
  without the process also ending. If IO is still in flight when this occurs, we must ensure that
  all IO is cancelled before the client's buffers are freed, to guard against a use-after-free.

  This PR also fixes an unrelated assertion failure that triggered when closing a client that
  had already closed its socket.

- [#2432](https://github.com/tigerbeetle/tigerbeetle/pull/2432)

  On startup and after checkpoint, assert that number of blocks acquired by the free set is
  consistent with the number of blocks we see acquired via the manifest and checkpoint trailers.

- [#2436](https://github.com/tigerbeetle/tigerbeetle/pull/2436)

  Reject connections from unknown replicas.

- [#2438](https://github.com/tigerbeetle/tigerbeetle/pull/2438)

  Fix multiversion builds on MacOS.

- [#2442](https://github.com/tigerbeetle/tigerbeetle/pull/2442)

  On an unrecognized error code from the OS, print that error before we panic.
  (This was already the policy in `Debug` builds, but now it includes `ReleaseSafe` as well.)

- [#2444](https://github.com/tigerbeetle/tigerbeetle/pull/2444)

  Fix a panic involving an in-flight write to an old reply after state sync.

### Internals

- [#2440](https://github.com/tigerbeetle/tigerbeetle/pull/2440)

  Expose reply timestamp from `vsr.Client`.
  (Note that this is not yet surfaced by language clients).

### TigerTracks 🎧

- [Everything In Its Right Place](https://www.youtube.com/watch?v=onRk0sjSgFU)

## TigerBeetle 0.16.11

Released: 2024-10-28

### Safety And Performance

- [#2428](https://github.com/tigerbeetle/tigerbeetle/pull/2428)

  Make `Grid.reserve()` abort rather than returning null.
  When `Grid.reserve()` aborts, that indicates that the data file size limit would be exceeded by
  the reservation. We were already panicking in this case by unwrapping the result, but now it has
  a useful error message.

- [#2416](https://github.com/tigerbeetle/tigerbeetle/pull/2416)

  Improve availability and performance by sending `start_view` message earlier in the new-primary
  recovery – as soon as the journal headers are repaired.

- [#2360](https://github.com/tigerbeetle/tigerbeetle/pull/2360)

  Refactor compaction to clarify the scheduling logic, schedule more aggressively, and make it
  easier to run multiple compactions concurrently. This also improved the benchmark performance.

### Features

- [#2425](https://github.com/tigerbeetle/tigerbeetle/pull/2425)

  Support multiversion (non-automatic) upgrades when the replica is started with `--development`
  or `--experimental`.

### Internals

- [#2427](https://github.com/tigerbeetle/tigerbeetle/pull/2427)

  Allow a release's Git tag and `config.process.release` to differ. This simplifies the release
  process for hotfixes, when the Git tag is bumped but the `config.process.release` is unchanged.

### TigerTracks 🎧

- [Stuck in a Timeloop](https://www.youtube.com/watch?v=FWBjzQnDb8o)

## TigerBeetle 0.16.10

Released: 2024-10-21

### Safety And Performance

- [#2414](https://github.com/tigerbeetle/tigerbeetle/pull/2414)

  Improve performance & availability during view change by ensuring a replica only repairs the
  portion of the WAL that is *required* to become primary, instead of repairing it in its entirety.

- [#2412](https://github.com/tigerbeetle/tigerbeetle/pull/2412)

  Add a unit test for Zig's stdlib sort.

  Stable sort is critical for compaction correctness. Zig stdlib does have a sort fuzz test, but it
  doesn't cover the presorted subarray case, and doesn't check arrays much larger than the sort
  algorithm's on-stack cache.

- [#2413](https://github.com/tigerbeetle/tigerbeetle/pull/2413)

  Fix a bug in the MessageBus wherein connections weren't being terminated during client teardown.

### Internals


- [#2405](https://github.com/tigerbeetle/tigerbeetle/pull/2405)

  Fix a bug in the benchmark wherein the usage of `--account-count-hot` was broken when used in
  conjunction with the `uniform` distribution.

- [#2409](https://github.com/tigerbeetle/tigerbeetle/pull/2409)

  Revamp the `core_missing_prepares` liveness-mode check to correctly check for the prepares that a
  replica should repair (after [#2414](https://github.com/tigerbeetle/tigerbeetle/pull/2414)).


### TigerTracks 🎧

- [Last Train Home](https://open.spotify.com/track/0tgBtQ0ISnMQOKorrN9HLX?si=8a05ab879730455b)

## TigerBeetle 0.16.9

Released: 2024-10-15

### Safety And Performance

- [#2394](https://github.com/tigerbeetle/tigerbeetle/pull/2394),
  [#2401](https://github.com/tigerbeetle/tigerbeetle/pull/2401)

  TigerBeetle clients internally batch operations for improved performance. Fix a bug where an
  unclosed link chain could be batched before another linked chain, causing them to be treated as
  one long linked chain. Additionally, prevent non-batchable requests from sharing packets entirely.

- [#2398](https://github.com/tigerbeetle/tigerbeetle/pull/2398)

  `AMOUNT_MAX` is used as a sentinel value for things like balancing transfers to specify moving
  as much as possible. Correct and fix its value in the Java client. Thanks @tKe!

### Features

- [#2274](https://github.com/tigerbeetle/tigerbeetle/pull/2274)

  Improve the benchmark by adding Zipfian distributed random numbers, to better simulate realistic
  conditions and as a precursor to approximating YCSB.

- [#2393](https://github.com/tigerbeetle/tigerbeetle/pull/2393)

  Previously, TigerBeetle's clients disallowed empty batches locally, before the request was even
  sent to the cluster. However, this is actually a valid protocol message - even if it's not used by
  the current state machine - so allow empty batches to be sent from clients.

- [#2384](https://github.com/tigerbeetle/tigerbeetle/pull/2384)

  Revamp client documentation so that each snippet is self-contained, and standardize it across all
  languages.

### Internals

- [#2404](https://github.com/tigerbeetle/tigerbeetle/pull/2404)

  Give the [DevHub](https://tigerbeetle.github.io/tigerbeetle/) a fresh coat of paint, and fix
  passing seeds being blue in dark mode.

- [#2408](https://github.com/tigerbeetle/tigerbeetle/pull/2408),
  [#2400](https://github.com/tigerbeetle/tigerbeetle/pull/2400),
  [#2391](https://github.com/tigerbeetle/tigerbeetle/pull/2391),
  [#2399](https://github.com/tigerbeetle/tigerbeetle/pull/2399),
  [#2402](https://github.com/tigerbeetle/tigerbeetle/pull/2402),
  [#2385](https://github.com/tigerbeetle/tigerbeetle/pull/2385)

  Improve VOPR logging and fix a few failing seeds.

### TigerTracks 🎧

- [99 Luftballons](https://www.youtube.com/watch?v=Fpu5a0Bl8eY)

## TigerBeetle 0.16.8

Released: 2024-10-07

### Safety And Performance

- [#2359](https://github.com/tigerbeetle/tigerbeetle/pull/2359)

  Significantly reduced P100 latency by incrementally spreading the mutable table's sort during
  compaction. This leverages the optimization of sort algorithms for processing sequences of
  already sorted sub-arrays.

- [#2367](https://github.com/tigerbeetle/tigerbeetle/pull/2367)

  Improve the workload generator to support concurrent tests with different ledgers.

- [#2382](https://github.com/tigerbeetle/tigerbeetle/pull/2382),
  [#2363](https://github.com/tigerbeetle/tigerbeetle/pull/2363)

  Fix VOPR seeds.
  For more awesome details about the backstory and solutions to these issues,
  please refer to the PR.

### Features

- [#2358](https://github.com/tigerbeetle/tigerbeetle/pull/2358)

  Update the REPL to support representing the maximum integer value as `-0`,
  serving as the `AMOUNT_MAX` sentinel.
  Additionally, other negative values such as `-1` can be used to represent `maxInt - 1`.

  Also, include support for hexadecimal numbers for more convenient inputting of GUID/UUID
  literals (e.g. `0xa1a2a3a4_b1b2_c1c2_d1d2_e1e2e3e4e5e6`).

  Allow the `timestamp` field to be set, enabling the REPL to be used for `imported` events.

### Internals

- [#2376](https://github.com/tigerbeetle/tigerbeetle/pull/2376)

  Use `zig fetch` as a replacement for downloading files, removing dependence on external tools.

- [#2383](https://github.com/tigerbeetle/tigerbeetle/pull/2383)

  Port of Rust's [`dbg!`](https://doc.rust-lang.org/std/macro.dbg.html) macro to Zig,
  and the corresponding CI validation to prevent code using it from being merged into `main`! 😎

- [#2370](https://github.com/tigerbeetle/tigerbeetle/pull/2370),
  [#2373](https://github.com/tigerbeetle/tigerbeetle/pull/2373)

  Verify the release versions included in the multiversion binary pack at build time (not only
  during runtime) and improve the `tigerbeetle version --verbose` command's `multiversion` output.

- [#2369](https://github.com/tigerbeetle/tigerbeetle/pull/2369)

  Fix a multiversioning issue where the binary size exceeded the read buffer, failing to parse the
  executable header.

- [#2380](https://github.com/tigerbeetle/tigerbeetle/pull/2380)

  Consistently use `transient_error` instead of `transient_failure` and cleanup the StateMachine
  code.

- [#2379](https://github.com/tigerbeetle/tigerbeetle/pull/2379)

  Add missing links to the operations `query_accounts` and `query_transfers` in the documentation
  and include the declaration for `QueryFilter` and `QueryFilterFlags` in the `tb_client.h` header.

- [#2333](https://github.com/tigerbeetle/tigerbeetle/pull/2333)

  Clearer error message when the replica crashes due to a data file being too large, instructing
  the operator to increase the memory allocated for the manifest log.

### TigerTracks 🎧

- [Creep](https://www.youtube.com/watch?v=XFkzRNyygfk)

## TigerBeetle 0.16.7

Released: 2024-10-04

Note: this is an extra release to correct an availability issue in the upgrade path for `0.16.4`.
Specifically, the combination of `tigerbeetle 0.16.4` and a client at `0.16.3` can lead to an
assertion failure and a server crash. No data is lost, but the server becomes unavailable.

It is recommended to upgrade to `0.16.7`, but this is only _required_ if you are running older
clients. To upgrade, replace the binary on disk, and manually restart the replica.

Note that although the release is tagged at `0.16.7`, the binary advertises itself as `0.16.4`.

### Safety And Performance

- [#2377](https://github.com/tigerbeetle/tigerbeetle/pull/2378)

  Fix an assertion which was incorrect when a pre-transient-error client retried a transient error,
  and that transient error condition since disappeared. This mirrors #2345 which handles the case
  when it is still failing.

## TigerBeetle 0.16.6

Released: 2024-10-04

Note: this is an extra release to correct a potential issue in the upgrade path for `0.16.4`.
Specifically, the combination of `tigerbeetle 0.16.2` and newer, and any client before `0.16.2` can
lead to an assertion failure and a server crash. No data is lost, but the server becomes
unavailable.

It is recommended to upgrade to `0.16.6`, but this is only _required_ if you are running older
clients. To upgrade, replace the binary on disk, and manually restart the replica.

Note that although the release is tagged at `0.16.6`, the binary advertises itself as `0.16.4`.

### Safety And Performance

- [#2377](https://github.com/tigerbeetle/tigerbeetle/pull/2377)

  Correctly parse AccountFilter from pre `0.16.2` clients.

## TigerBeetle 0.16.5

Released: 2024-10-03

Note: this is an extra release to correct an availability issue in the upgrade path for `0.16.4`.
Specifically, the combination of `tigerbeetle 0.16.4` and a client at `0.16.3` can lead to an
assertion failure and a server crash. No data is lost, but the server becomes unavailable.

It is recommended to upgrade to `0.16.5`, but this is only _required_ if you are running older
clients. To upgrade, replace the binary on disk, and manually restart the replica.

Note that although the release is tagged at `0.16.5`, the binary advertises itself as `0.16.4`.

### Safety And Performance

- [#2345](https://github.com/tigerbeetle/tigerbeetle/pull/2345)

  Fix an assertion which was incorrect when a pre-transient-error client retried a transient error.

## TigerBeetle 0.16.4

Released: 2024-09-30

This release introduces "transient errors": error codes for `create_transfers` which depend on the
state of the database (e.g. `exceeds_credits`). Going forward, a transfer that fails with a
transient error will not succeed if retried.

See the [API tracking issue](https://github.com/tigerbeetle/tigerbeetle/issues/2231#issuecomment-2377879726)
and the [documentation](https://docs.tigerbeetle.com/reference/requests/create_transfers)
for more details.

### Safety And Performance

- [#2345](https://github.com/tigerbeetle/tigerbeetle/pull/2345)

  Reduce chance of `recovering_head` status by recovering from torn writes in the WAL.
  This improves the availability of the cluster, as `recovering_head` replicas cannot participate in
  consensus until after they repair.

### Features

- [#2335](https://github.com/tigerbeetle/tigerbeetle/pull/2335)

  Ensure idempotence for `create_transfers`' "transient errors" with new result code
  `id_already_failed`. In particular, this guards against surprising behavior when the client is
  running in a [stateless API service](https://docs.tigerbeetle.com/coding/system-architecture/).

### Internals

- [#2334](https://github.com/tigerbeetle/tigerbeetle/pull/2334),
  [#2362](https://github.com/tigerbeetle/tigerbeetle/pull/2362)

  Fix VOPR false positives.

- [#2115](https://github.com/tigerbeetle/tigerbeetle/pull/2115)

  Fix multiple commands for non-interactive REPL.

### TigerTracks 🎧

- [Lookin' Out My Back Door](https://www.youtube.com/watch?v=Aae_RHRptRg)

## TigerBeetle 0.16.3

Released: 2024-09-23

### Safety And Performance

- [#2313](https://github.com/tigerbeetle/tigerbeetle/pull/2313)

  Improve cluster availability by more aggressive recovery for crashes that happen while a replica
  is checkpointing.

- [#2328](https://github.com/tigerbeetle/tigerbeetle/pull/2328)

  Add a more efficient recipe for balance-conditional transfers. A balance-conditional transfer
  is a transfer that succeeds only if the source account has more than a threshold amount of funds
  in it.

### Features

- [#2327](https://github.com/tigerbeetle/tigerbeetle/pull/2327)

  Add a new recipe for enforcing `debits_must_not_exceed_credits` on some subset of transfers (this
  is a special case of a balance-conditional transfer, with the threshold value being equal to
  transferred amount).

### Internals

- [#2330](https://github.com/tigerbeetle/tigerbeetle/pull/2330)

  Add `triaged` issue label to prevent newly opened issues from slipping through the cracks.

- [#2332](https://github.com/tigerbeetle/tigerbeetle/pull/2332)

  Add CI check for dead code.

- [#2323](https://github.com/tigerbeetle/tigerbeetle/pull/2323)

  Cleanup the source tree by removing top-level `tools` directory.

- [#2316](https://github.com/tigerbeetle/tigerbeetle/pull/2316)

  Make sure that process-spawning API used for build-time "scripting" consistently reports
  errors when the subprocess fails or hangs.

### TigerTracks 🎧

- [Prelude in G Major](https://open.spotify.com/track/70FROKEHubzMxSstCgaZZl?si=5bb7f8c6decc46aa)

## TigerBeetle 0.16.2

Released: 2024-09-16

### Safety And Performance

- [#2312](https://github.com/tigerbeetle/tigerbeetle/pull/2312)

  Tighten up the VSR assertions so the transition to `.recovering_head` can only be called from the
  `.recovering` status.

- [#2311](https://github.com/tigerbeetle/tigerbeetle/pull/2311)

  Make the primary abdicate if it is unable to process requests due to a broken clock.

- [#2260](https://github.com/tigerbeetle/tigerbeetle/pull/2260)

  Smoke integration test using the real multiversion binary.

- [#2270](https://github.com/tigerbeetle/tigerbeetle/pull/2270)

  Workload generator based on the Java client to be used in integration tests (i.e. Antithesis).

### Features

- [#2298](https://github.com/tigerbeetle/tigerbeetle/pull/2298),
  [#2307](https://github.com/tigerbeetle/tigerbeetle/pull/2307),
  [#2304](https://github.com/tigerbeetle/tigerbeetle/pull/2304),
  [#2309](https://github.com/tigerbeetle/tigerbeetle/pull/2309),
  [#2321](https://github.com/tigerbeetle/tigerbeetle/pull/2321)

  Remove `Tracy` integration and dependencies.
  Add JSON traces for events with multiple running instances, such as IO, lookups, and scans.

- [#2300](https://github.com/tigerbeetle/tigerbeetle/pull/2300)

  Add the ability to filter by `user_data_{128,64,32}` and `code` in `get_account_transfers`
  and `get_account_balances`.

### Internals

- [#2314](https://github.com/tigerbeetle/tigerbeetle/pull/2314),
  [#2315](https://github.com/tigerbeetle/tigerbeetle/pull/2315)

  Mute the log on stderr when building client libraries.
  Reduce the log's severity of some entries logged as `.err` to `.warn` for less noise when
  running with `log_level = .err`.

- [#2310](https://github.com/tigerbeetle/tigerbeetle/pull/2310)

  Document and explain how time works in TigerBeetle ⏱️.

- [#2291](https://github.com/tigerbeetle/tigerbeetle/pull/2291)

  Refactor `Forest.compact` and remove some dead code.

- [#2294](https://github.com/tigerbeetle/tigerbeetle/pull/2294)

  Rewrite `commit_dispatch`, a chain of asynchronous stages calling each other, as a state machine
  implementation that resembles linear control flow that is much easier to read.

- [#2306](https://github.com/tigerbeetle/tigerbeetle/pull/2306)

  Fix the Node.js example that was using an incorrect enum flag.
  Thanks for the heads up @jorispz!

- [#2319](https://github.com/tigerbeetle/tigerbeetle/pull/2319)

  Update outdated scripts in `HACKING.md`.

- [#2317](https://github.com/tigerbeetle/tigerbeetle/pull/2317)

  Use git timestamps to build Docker images.
  This is a requirement for being deterministic in CI.

- [#2318](https://github.com/tigerbeetle/tigerbeetle/pull/2318)

  Devhub link to pending code reviews.

### TigerTracks 🎧

- [What's On Your Mind](https://www.youtube.com/watch?v=Z5WRKnCRPHA)

## TigerBeetle 0.16.1

Released: 2024-09-09

### Safety And Performance

- [#2284](https://github.com/tigerbeetle/tigerbeetle/pull/2284)

  Improve view change efficiency; new heuristic for lagging replicas to forfeit view change.

  A lagging replicas first gives a more up-to-date replica a chance to become primary by forfeiting
  view change. If the more up-to-date replica cannot step up as primary, the lagging replica
  attempts to step up as primary.

- [#2275](https://github.com/tigerbeetle/tigerbeetle/pull/2275),
  [#2254](https://github.com/tigerbeetle/tigerbeetle/pull/2254),
  [#2269](https://github.com/tigerbeetle/tigerbeetle/pull/2269),
  [#2272](https://github.com/tigerbeetle/tigerbeetle/pull/2272)

  Complete rollout of the new state sync protocol.

  Remove in-code remnants of the old state sync protocol. Replicas now panic if they receive
  messages belonging to the old protocol.


### Internals

- [#2283](https://github.com/tigerbeetle/tigerbeetle/pull/2283)

  Improve log warnings for client eviction due to its version being too low/high.

- [#2288](https://github.com/tigerbeetle/tigerbeetle/pull/2288)

  Fix VOPR false positive wherein checkpoint was being updated twice in the upgrade path.

- [#2286](https://github.com/tigerbeetle/tigerbeetle/pull/2286)

  Fix typos found using [codespell](https://github.com/codespell-project/codespell).

- [#2282](https://github.com/tigerbeetle/tigerbeetle/pull/2282)

  Document example for debiting multiple accounts and crediting a single account wherein the total
  amount to transfer to the credit account is known, but the balances of the individual debit
  accounts are not known.

- [#2281](https://github.com/tigerbeetle/tigerbeetle/pull/2281)

  Document the behavior of `user_data_128/user_data_64/user_data_32` in the presence of pending
  transfers.

- [#2279](https://github.com/tigerbeetle/tigerbeetle/pull/2279)

  Inline Dockerfile in the release code, removing tools/docker/Dockerfile.

- [#2280](https://github.com/tigerbeetle/tigerbeetle/pull/2280),
  [#2285](https://github.com/tigerbeetle/tigerbeetle/pull/2285)

  Add [`kcov`](https://simonkagstrom.github.io/kcov/) code coverage to
  [DevHub](https://tigerbeetle.github.io/tigerbeetle/).

- [#2266](https://github.com/tigerbeetle/tigerbeetle/pull/2266),
  [#2293](https://github.com/tigerbeetle/tigerbeetle/pull/2293)

  Add support for tracing IO & CPU events. This allows for coarse-grained performance analysis,
  for example collectively profiling IO and CPU performance (as opposed to IO or CPU in isolation).

- [#2273](https://github.com/tigerbeetle/tigerbeetle/pull/2273)

  Remove explicit header sector locks, using a common locking path for prepare and header sectors.

- [#2258](https://github.com/tigerbeetle/tigerbeetle/pull/2258)

  Change CliArgs -> CLIArgs in accordance with TigerStyle.

- [#2263](https://github.com/tigerbeetle/tigerbeetle/pull/2263)

  Vendor `llvm-objcopy` in the [dependencies](https://github.com/tigerbeetle/dependencies)
  repository in accordance with our "no dependencies" policy. This ensures users don't have to
  manually install LLVM.

- [#2297](https://github.com/tigerbeetle/tigerbeetle/pull/2297)

  Assign correct date to the release binary date; it was earlier set to the epoch ("Jan 1 1970").

- [#2289](https://github.com/tigerbeetle/tigerbeetle/pull/2289)

  Introduce fatal errors for crashing the replica process in the face of uncorrectable errors (for
  example, insufficient memory/storage).

- [#2287](https://github.com/tigerbeetle/tigerbeetle/pull/2287)

  Add formatting check in the CI for the Go client.

- [#2278](https://github.com/tigerbeetle/tigerbeetle/pull/2278),
  [#2292](https://github.com/tigerbeetle/tigerbeetle/pull/2292)

  Reduce dimensionality of configuration modes.

  Removes the development configuration which was used to run the replica with asserts enabled,
  enabling asserts for the production configuration instead. Additionally, removes the -Dconfig CLI
  option, making production configuration the default.


### TigerTracks 🎧

- [Fire on the Mountain](https://open.spotify.com/track/4DpBfWl3q8e0gGB76lAaox?si=dc8e84acf3de464a)

## TigerBeetle 0.16.0

Released: 2024-09-02

This release is 0.16.0 as it includes a new breaking API change around zero amount transfers, as
well as the behavior around posting a full pending transfer amount or balancing as much as possible.
These are all gated by the client's release version.

If you're running a client older than 0.16.0, you'll see the old behavior where zero amount
transfers are disallowed, but on newer clients these are supported and will create a transfer with
an amount of 0.

Additionally, the sentinel value to representing posting the full amount of a pending transfer, or
doing a balancing transfer for as much as possible has changed. It's no longer 0, but instead
`AMOUNT_MAX`.

See the [**tracking issue**](https://github.com/tigerbeetle/tigerbeetle/issues/2231#issuecomment-2305132591) for more details.

### Safety And Performance

- [#2221](https://github.com/tigerbeetle/tigerbeetle/pull/2221)

  Change how replicas that haven't finished syncing send a `prepare_ok` message,
  preventing them from falsely contributing to the durability of a checkpoint, which could
  potentially cause liveness issues in the event of storage faults.

- [#2255](https://github.com/tigerbeetle/tigerbeetle/pull/2255)

  The new state sync protocol regressed the behavior where the replica would try to repair the WAL
  before switching to state sync, and this puts the old behavior back in.

  [WAL repair](https://docs.tigerbeetle.com/about/internals/vsr#protocol-repair-wal) is used when
  the lagging replica's log still intersects with the cluster's current log, while
  [state sync](https://docs.tigerbeetle.com/about/internals/sync) is used when the logs no
  longer intersect.

- [#2244](https://github.com/tigerbeetle/tigerbeetle/pull/2244)

  Try to repair (but not commit) prepares, even if we don't have all the headers between checkpoint
  and head.

  This makes things consistent between the normal and repair paths, and improves concurrency while
  repairing.

- [#2253](https://github.com/tigerbeetle/tigerbeetle/pull/2253)

  Reject prepares on the primary if its view isn't durable, much like solo clusters.

  This solves a failing VOPR seed wherein a primary accepting prepares before making its log_view
  durable exposes a break in its hash chain.

- [#2259](https://github.com/tigerbeetle/tigerbeetle/pull/2259),
  [#2246](https://github.com/tigerbeetle/tigerbeetle/pull/2246)

  A few `sysctl`s and security frameworks (e.g., seccomp) might block io_uring. Print out a more
  helpful error message, rather than a generic "permission denied" or "system outdated".


### Features

- [#2171](https://github.com/tigerbeetle/tigerbeetle/pull/2171)

  Add the new `imported` flag to allow user-defined timestamps when creating
  `Account`s and `Transfer`s from historical events.

- [#2220](https://github.com/tigerbeetle/tigerbeetle/pull/2220),
  [#2237](https://github.com/tigerbeetle/tigerbeetle/pull/2237),
  [#2238](https://github.com/tigerbeetle/tigerbeetle/pull/2238),
  [#2239](https://github.com/tigerbeetle/tigerbeetle/pull/2239)

  Allow `Transfer`s with `amount=0` and change behavior for _balancing_ and _post-pending_
  transfers, introducing the constant `AMOUNT_MAX` to replace the use of the zero sentinel when
  representing the maximum/original value in such cases.  Note that this is a
  [**breaking change**](https://github.com/tigerbeetle/tigerbeetle/issues/2231#issuecomment-2305132591).

  Also, explicitly define _optional indexes_, which previously were determined simply by not
  indexing zeroed values.

- [#2234](https://github.com/tigerbeetle/tigerbeetle/pull/2234)

  Introduce a new flag, `Account.flags.closed`, which causes an account to reject any further
  transfers, except for voiding two-phase transfers that are still pending.

  The account flag can be set during creation or through a closing transfer. In the latter case,
  closed account can be re-opened by voiding or expiring the closing transfer.


### Internals

- [#2211](https://github.com/tigerbeetle/tigerbeetle/pull/2211)

  Deprecates the old state sync protocol, no longer supporting both protocols simultaneously.
  As planned for this release, it only ignores old messages, allowing replicas to upgrade normally.
  In the next release, replicas would panic if they receive an old message.

- [#2233](https://github.com/tigerbeetle/tigerbeetle/pull/2233)

  Move multiversion build logic into `build.zig` from `release.zig`. This makes it much easier to
  build multiversion binaries as part of a regular `zig build`, without having to invoke CI or
  release process specific code that's normally part of `release.zig`.

  It also makes it possible to build multiversion binaries on platforms that aren't x86_64 Linux.

- [#2215](https://github.com/tigerbeetle/tigerbeetle/pull/2215)

  Refactor the _Multiversion_ API, bringing it in line with pre-existing code patterns.

- [#2251](https://github.com/tigerbeetle/tigerbeetle/pull/2251)

  Previously, TigerBeetle release numbers were based on a finicky conversion of GitHub's internal
  action run number to a version number.

  This was error prone, and difficult to reason about before hand (what would the given version
  number for a release be?). Instead, make it so this very changelog is the source of truth for
  the version number which is explicitly set.

- [#2252](https://github.com/tigerbeetle/tigerbeetle/pull/2252)

  Change `init` function signatures to allow for in-place initialization. This addresses the silent
  stack growth caused by intermediate copy/move allocations during the initialization of large
  objects.

  Specifically, the `Forest` struct can grow indefinitely depending on the number of
  `Grooves`/`IndexTrees` needed to support the StateMachine's custom logic, causing TigerBeetle to
  crash during startup due to stack-overflow.

- [#2265](https://github.com/tigerbeetle/tigerbeetle/pull/2265)

  Don't cancel in-progress GitHub actions on the main branch. In particular, this ensures that the
  devhub records the benchmark measurements for every merge to main, even if those merges occur in
  quick succession.

- [#2218](https://github.com/tigerbeetle/tigerbeetle/pull/2218)

  Make the experimental feature `aof` (append-only file) a runtime flag instead of a build-time
  setting. This simplifies operations, allowing the use of the same standard release binary in
  environments that require `aof`.

- [#2228](https://github.com/tigerbeetle/tigerbeetle/pull/2228)

  Renames the LSM constant `lsm_batch_multiple` to `lsm_compaction_ops`, providing clearer meaning
  on how it relates to the pace at which LSM tree compaction is triggered.

- [#2240](https://github.com/tigerbeetle/tigerbeetle/pull/2240)

  Add support for indexing flags, namely the new `imported` flag.

### TigerTracks 🎧

- [I Want To Break Free](https://open.spotify.com/track/7iAqvWLgZzXvH38lA06QZg?si=a5ad69b31f3a45dd)
- [Used To Love Her](https://www.youtube.com/watch?v=FDIvIb06abI)

## TigerBeetle 0.15.6

Released: 2024-08-19

### Safety And Performance

- [#1951](https://github.com/tigerbeetle/tigerbeetle/pull/1951),
  [#2212](https://github.com/tigerbeetle/tigerbeetle/pull/2212)

  Add new state sync protocol, fixing a couple of liveness issues.
  State sync is now performed as part of the view change.

- [#2207](https://github.com/tigerbeetle/tigerbeetle/pull/2207)

  Major state sync performance improvements.

### Features

- [#2224](https://github.com/tigerbeetle/tigerbeetle/pull/2224),
  [#2225](https://github.com/tigerbeetle/tigerbeetle/pull/2225),
  [#2226](https://github.com/tigerbeetle/tigerbeetle/pull/2226)

  Ensure `u128` (and related type) consistency across client implementations.

- [#2213](https://github.com/tigerbeetle/tigerbeetle/pull/2213)

  Fix multiversioning builds for aarch64 macOS.

### Internals

- [#2210](https://github.com/tigerbeetle/tigerbeetle/pull/2210)

  Automatically include oldest supported releases in release notes.

- [#2214](https://github.com/tigerbeetle/tigerbeetle/pull/2214)

  Refactor `build.zig` to break up the biggest function in the codebase.

- [#2178](https://github.com/tigerbeetle/tigerbeetle/pull/2178)

  Minor improvements to zig install scripts.

### TigerTracks 🎧

- [End of the Line](https://www.youtube.com/watch?v=UMVjToYOjbM)

## TigerBeetle 0.15.5

Released: 2024-08-12

Highlight of this release is fully rolled-out support for multiversion binaries. This means that,
from now on, the upgrade procedure is going to be as simple as dropping the new version of
`tigerbeetle` binary onto the servers. TigerBeetle will take care of restarting the cluster at the
new version when it is appropriate. See <https://docs.tigerbeetle.com/operating/upgrading> for
reference documentation.

Note that the upgrade procedure from `0.15.3` and `0.15.4` is a bit more involved.

- When upgrading from `0.15.3`, you'll need to stop and restart `tigerbeetle` binary manually.
- When upgrading from `0.15.4`, the binary will stop automatically by hitting an `assert`. You
  should restart it after that.

### Safety And Performance

- [#2174](https://github.com/tigerbeetle/tigerbeetle/pull/2174)
  [#2190](https://github.com/tigerbeetle/tigerbeetle/pull/2190),

  Test client eviction in the VOPR.

- [#2187](https://github.com/tigerbeetle/tigerbeetle/pull/2187)

  Add integration tests for upgrades.

- [#2188](https://github.com/tigerbeetle/tigerbeetle/pull/2188)

  Add more hardening parameters to the suggested systemd unit definition.

### Features

- [#2180](https://github.com/tigerbeetle/tigerbeetle/pull/2180),
  [#2185](https://github.com/tigerbeetle/tigerbeetle/pull/2185),
  [#2189](https://github.com/tigerbeetle/tigerbeetle/pull/2189),
  [#2196](https://github.com/tigerbeetle/tigerbeetle/pull/2196)

  Make the root directory smaller by getting rid of `scripts` and `.gitattributes` entries.
  Root directory is the first thing you see when opening the repository, this space shouldn't be
  wasted!

- [#2199](https://github.com/tigerbeetle/tigerbeetle/pull/2199),
  [#2165](https://github.com/tigerbeetle/tigerbeetle/pull/2165),
  [#2198](https://github.com/tigerbeetle/tigerbeetle/pull/2198),
  [#2184](https://github.com/tigerbeetle/tigerbeetle/pull/2184).

  Complete the integration of multiversion binaries with the release infrastructure. From now on,
  the upgrade procedure is as simple as replacing the binary on disk with a new version. TigerBeetle
  will take care of safely and seamlessly restarting the cluster when appropriate itself.

- [#2181](https://github.com/tigerbeetle/tigerbeetle/pull/2181)

  Prepare to rollout the new state sync protocol. Stay tuned
  for the next release!

### Internals

- [#2179](https://github.com/tigerbeetle/tigerbeetle/pull/2179),
  [#2200](https://github.com/tigerbeetle/tigerbeetle/pull/2200)

  Simplify iteration over an LSM tree during scans.

- [#2182](https://github.com/tigerbeetle/tigerbeetle/pull/2182)

  Fix addresses logging in the client regressed by
  [#2164](https://github.com/tigerbeetle/tigerbeetle/pull/2164).

- [#2193](https://github.com/tigerbeetle/tigerbeetle/pull/2193)

  Modernize scripts to generate client bindings to follow modern idioms for `build.zig`.

- [#2195](https://github.com/tigerbeetle/tigerbeetle/pull/2195)

  Fix typo in the currency exchange example.


### TigerTracks 🎧

- [High Hopes](https://open.spotify.com/track/236mI0lz8JdQjlmijARSwY?si=38f80fc31cfc4876)

## 2024-08-05 (No release: Queued up to improve multiversion upgrade flow)

### Safety And Performance

- [#2162](https://github.com/tigerbeetle/tigerbeetle/pull/2162)

  Past release checksums are further validated when printing multi-version information.

- [#2143](https://github.com/tigerbeetle/tigerbeetle/pull/2143)

  Write Ahead Log (WAL) appending was decoupled from WAL replication, tightening asserts.

- [#2153](https://github.com/tigerbeetle/tigerbeetle/pull/2153),
  [#2170](https://github.com/tigerbeetle/tigerbeetle/pull/2170)

  VSR eviction edge cases receive more hardening.

- [#2175](https://github.com/tigerbeetle/tigerbeetle/pull/2175)

  Fix account overflows when doing a balance transfer for remaining funds (`amount=0`).

- [#2168](https://github.com/tigerbeetle/tigerbeetle/pull/2168),
  [#2164](https://github.com/tigerbeetle/tigerbeetle/pull/2164),
  [#2152](https://github.com/tigerbeetle/tigerbeetle/pull/2152),
  [#2122](https://github.com/tigerbeetle/tigerbeetle/pull/2122)

  Command line argument parsing no longer dynamically allocates and handles error handling paths
  more explicitly.

### Internals

- [#2169](https://github.com/tigerbeetle/tigerbeetle/pull/2169)

  Golang's tests for the CI were re-enabled for ARM64 macOS.

- [#2159](https://github.com/tigerbeetle/tigerbeetle/pull/2159)

  This is a CHANGELOG entry about fixing a previous CHANGELOG entry.

### TigerTracks 🎧

- [Ramble On](https://www.youtube.com/watch?v=EYeG3QrvkEE)

## 2024-07-29

### Safety And Performance

- [#2140](https://github.com/tigerbeetle/tigerbeetle/pull/2140),
  [#2154](https://github.com/tigerbeetle/tigerbeetle/pull/2154)

  Fix a bug where MessageBus sees block/reply messages (due to state sync or repair) and peer_type
  says they are always from replica 0 (since Header.Block.replica == 0 always). So, if they are
  being sent by a non-R0 replica, it drops the messages with "message from unexpected peer".

  This leads to a replica being stuck in state sync and unable to progress.

- [#2137](https://github.com/tigerbeetle/tigerbeetle/pull/2137)

  It was possible for a prepare to exist in a mixture of WALs and checkpoints, which could
  compromise physical durability under storage fault conditions, since the data is present across a
  commit-quorum of replicas in different forms.

  Rather, ensure a prepare in the WAL is only overwritten if it belongs to a commit-quorum of
  checkpoints.

- [#2127](https://github.com/tigerbeetle/tigerbeetle/pull/2127),
  [#2096](https://github.com/tigerbeetle/tigerbeetle/pull/2096)

  A few CI changes: run tests in CI for x86_64 macOS, add in client tests on macOS and run the
  benchmark with `--validate` in CI.

- [#2117](https://github.com/tigerbeetle/tigerbeetle/pull/2117)

  TigerBeetle reserves the most significant bit of the timestamp as the tombstone flag, so indicate
  and assert that timestamp_max is a `maxInt(u63)`.

- [#2123](https://github.com/tigerbeetle/tigerbeetle/pull/2123),
  [#2125](https://github.com/tigerbeetle/tigerbeetle/pull/2125)

  Internally, TigerBeetle uses AEGIS-128L for checksumming - hardware AES is a prerequisite for
  performance. Due to a build system bug, releases being built with a specified (`-Dtarget=`) target
  would only be built with baseline CPU features, and thus use the software AES implementation.

  Enforce at comptime that hardware acceleration is available, fix the build system bug, log
  checksum performance on our [devhub](https://tigerbeetle.github.io/tigerbeetle/) and build client
  libraries with hardware acceleration too.

- [#2139](https://github.com/tigerbeetle/tigerbeetle/pull/2139)

  TigerBeetle would wait until all repairable headers are fixed before trying to commits prepares,
  but if all the headers after the checkpoint are present then we can start committing even if
  some headers from before the checkpoint are missing.

- [#2141](https://github.com/tigerbeetle/tigerbeetle/pull/2141)

  Clarify that the order of replicas in `--addresses` is important. Currently, the order of replicas
  as specified has a direct impact on how messages are routed between them. Having a differing order
  leads to significantly degraded performance.

- [#2120](https://github.com/tigerbeetle/tigerbeetle/pull/2120)

  The state machine depended on `prepare_timestamp` to evaluate `pulse()`, but in an idle cluster,
  `prepare_timestamp` would only be set if pulse returned true! Thanks @ikolomiets for reporting.

- [#2028](https://github.com/tigerbeetle/tigerbeetle/pull/2028)

  Add a fuzzer for scans.

- [#2109](https://github.com/tigerbeetle/tigerbeetle/pull/2109)

  Fuzz `storage.zig`, by using a mocked IO layer.

### Features

- [#2070](https://github.com/tigerbeetle/tigerbeetle/pull/2070)

  Certain workloads (for example, sending in tiny batches) can cause high amounts of space
  amplification in TigerBeetle, leading to data file sizes that are much larger than optimal.

  This introduces a stopgap fix, greedily coalescing tables in level 0 of the LSM, which improves
  space amplification dramatically.

- [#2003](https://github.com/tigerbeetle/tigerbeetle/pull/2003)

  Add a data file inspector tool to the TigerBeetle CLI, handy for development and debugging alike.
  You can run it with `tigerbeetle inspect --help`.

- [#2136](https://github.com/tigerbeetle/tigerbeetle/pull/2136),
  [#2013](https://github.com/tigerbeetle/tigerbeetle/pull/2013),
  [#2126](https://github.com/tigerbeetle/tigerbeetle/pull/2126)

  TigerBeetle clusters can now be [upgraded](https://docs.tigerbeetle.com/operating/upgrading)!

- [#2095](https://github.com/tigerbeetle/tigerbeetle/pull/2095)

  Add a custom formatter for displaying units in error messages. Thanks @tensorush!

### Internals

- [#1380](https://github.com/tigerbeetle/tigerbeetle/pull/1380)

  Allows for language clients to manage their own `Packet` memory, removing the need for tb_client
  to do so and thus removing the concepts of acquire/release_packet and concurrency_max.

- [#2148](https://github.com/tigerbeetle/tigerbeetle/pull/2148)

  Add function length limits to our internal tidy tests.

- [#2116](https://github.com/tigerbeetle/tigerbeetle/pull/2116),
  [#2114](https://github.com/tigerbeetle/tigerbeetle/pull/2114),
  [#2111](https://github.com/tigerbeetle/tigerbeetle/pull/2111),
  [#2132](https://github.com/tigerbeetle/tigerbeetle/pull/2132),
  [#2131](https://github.com/tigerbeetle/tigerbeetle/pull/2131),
  [#2124](https://github.com/tigerbeetle/tigerbeetle/pull/2124)

  Lots of small [CFO](https://tigerbeetle.github.io/tigerbeetle/) improvements.

### TigerTracks 🎧

- [Here I Go Again](https://www.youtube.com/watch?v=WyF8RHM1OCg)

## 2024-07-15 (No release: Queued up for upcoming multi-version binary release)

### Safety And Performance

- [#2078](https://github.com/tigerbeetle/tigerbeetle/pull/2078)

  Fix an incorrect `assert` that was too tight, crashing the replica after state sync,
  when the replica's operation number lags behind checkpoint.

- [#2103](https://github.com/tigerbeetle/tigerbeetle/pull/2103),
  [#2056](https://github.com/tigerbeetle/tigerbeetle/pull/2056),
  [#2072](https://github.com/tigerbeetle/tigerbeetle/pull/2072)

  Fixes and improvements to tests and simulator.

- [#2088](https://github.com/tigerbeetle/tigerbeetle/pull/2088)

  Improve the benchmark to verify the state after execution and enable tests in Windows CI!

- [#2090](https://github.com/tigerbeetle/tigerbeetle/pull/2090)

  Call `fs_sync` on macOS/Darwin after each write to properly deal with Darwin's `O_DSYNC` which
  [doesn't behave like `O_DSYNC` on Linux](https://x.com/TigerBeetleDB/status/1536628729031581697).

### Features

- [#2080](https://github.com/tigerbeetle/tigerbeetle/pull/2080)

  New operations `query accounts` and `query transfers` as a stopgap API to add some degree of
  user-defined query capabilities.
  This is an experimental feature meant to be replaced by a proper querying API.


### Internals

- [#2067](https://github.com/tigerbeetle/tigerbeetle/pull/2067)

  Simplify the comptime configuration by merging `config.test_min` and `config.fuzz_min`.

- [#2091](https://github.com/tigerbeetle/tigerbeetle/pull/2091)

  Fixed many typos and misspellings, thanks to [Jora Troosh](https://github.com/tensorush).

- [#2099](https://github.com/tigerbeetle/tigerbeetle/pull/2099),
  [#2097](https://github.com/tigerbeetle/tigerbeetle/pull/2097),
  [#2098](https://github.com/tigerbeetle/tigerbeetle/pull/2098),
  [#2100](https://github.com/tigerbeetle/tigerbeetle/pull/2100),
  [#2092](https://github.com/tigerbeetle/tigerbeetle/pull/2092),
  [#2094](https://github.com/tigerbeetle/tigerbeetle/pull/2094),
  [#2089](https://github.com/tigerbeetle/tigerbeetle/pull/2089),
  [#2073](https://github.com/tigerbeetle/tigerbeetle/pull/2073),
  [#2087](https://github.com/tigerbeetle/tigerbeetle/pull/2087),
  [#2086](https://github.com/tigerbeetle/tigerbeetle/pull/2086),
  [#2083](https://github.com/tigerbeetle/tigerbeetle/pull/2083),
  [#2085](https://github.com/tigerbeetle/tigerbeetle/pull/2085)

  Multiple and varied changes to conform **all** line lengths to not more than 100 columns,
  according to
  [TigerStyle](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md#style-by-the-numbers)!

- [#2081](https://github.com/tigerbeetle/tigerbeetle/pull/2081)

  Run `kcov` during CI as a code coverage sanity check. No automated action is taken regarding the
  results. We're not focused on tracking the quantitative coverage metric, but rather on surfacing
  blind spots qualitatively.

### TigerTracks 🎧

- [Sultans Of Swing](https://www.youtube.com/watch?v=h0ffIJ7ZO4U)

## 2024-07-08 (No release: Queued up for upcoming multi-version binary release)

### Safety And Performance

- [#2035](https://github.com/tigerbeetle/tigerbeetle/pull/2035),
  [#2042](https://github.com/tigerbeetle/tigerbeetle/pull/2042),
  [#2069](https://github.com/tigerbeetle/tigerbeetle/pull/2069)

  Strengthen LSM assertions.

- [#2077](https://github.com/tigerbeetle/tigerbeetle/pull/2077)

  Use flexible quorums for clock synchronization.

### Features

- [#2037](https://github.com/tigerbeetle/tigerbeetle/pull/2037)

  Improve and clarify balancing transfer `amount` validation.

### Internals

- [#2063](https://github.com/tigerbeetle/tigerbeetle/pull/2063)

  Add chaitanyabhandari to the list of release managers.

- [#2075](https://github.com/tigerbeetle/tigerbeetle/pull/2075)

  Update TigerStyle with advice for splitting long functions.

- [#2068](https://github.com/tigerbeetle/tigerbeetle/pull/2068),
  [#2074](https://github.com/tigerbeetle/tigerbeetle/pull/2074)

  Fix flaky tests.

- [#1995](https://github.com/tigerbeetle/tigerbeetle/pull/1995)

  Add `--security-opt seccomp=unconfined` to Docker commands in docs, since newer versions of Docker
  block access to io_uring.

- [#2047](https://github.com/tigerbeetle/tigerbeetle/pull/2047),
  [#2064](https://github.com/tigerbeetle/tigerbeetle/pull/2064),
  [#2079](https://github.com/tigerbeetle/tigerbeetle/pull/2079)

  Clean up github actions workflows.

- [#2071](https://github.com/tigerbeetle/tigerbeetle/pull/2071)

  Make cfo supervisor robust to network errors.

### TigerTracks 🎧

- [Линия жизни](https://open.spotify.com/track/2dpGc40PtSLEeNAGrTnJGI?si=9c3d6e45632147c4)

## 2024-07-01 (No release: Queued up for upcoming multi-version binary release)

### Safety And Performance

- [#2058](https://github.com/tigerbeetle/tigerbeetle/pull/2058)

  `tigerbeetle benchmark` command can now simulate few "hot" accounts which account for most of
  transfers, the distribution expected in a typical deployment.

### Features

- [#2040](https://github.com/tigerbeetle/tigerbeetle/pull/2040)

  Add a recipe for accounts with bounded balance

### Internals

- [#2033](https://github.com/tigerbeetle/tigerbeetle/pull/2033),
  [#2041](https://github.com/tigerbeetle/tigerbeetle/pull/2041)

  Rewrite `build.zig` to introduce a more regular naming scheme for top-level steps.

- [#2057](https://github.com/tigerbeetle/tigerbeetle/pull/2057)

  Our internal dashboard, [devhub](https://tigerbeetle.github.io/tigerbeetle/) now has dark mode 😎.

- [#2052](https://github.com/tigerbeetle/tigerbeetle/pull/2052),
  [#2032](https://github.com/tigerbeetle/tigerbeetle/pull/2032),
  [#2044](https://github.com/tigerbeetle/tigerbeetle/pull/2044)

  Ensure that the generated `tb_client.h` C header is in sync with Zig code.


### TigerTracks 🎧

- [Wish You Were Here](https://open.spotify.com/track/7aE5WXu5sFeNRh3Z05wwu4?si=317f6e0302cc4040)

## 2024-06-24 (No release: Queued up for upcoming multi-version binary release)

### Safety And Performance

- [#2034](https://github.com/tigerbeetle/tigerbeetle/pull/2034),
  [#2022](https://github.com/tigerbeetle/tigerbeetle/pull/2022),
  [#2023](https://github.com/tigerbeetle/tigerbeetle/pull/2023)

  Fuzzer Fixing For Fun! Particularly around random number generation and number sequences.

- [#2004](https://github.com/tigerbeetle/tigerbeetle/pull/2004)

  Add simulator coverage for `get_account_transfers` and `get_account_balances`.

### Features

- [#2010](https://github.com/tigerbeetle/tigerbeetle/pull/2010)

  Reduce the default `--limit-pipeline-requests` value, dropping RSS memory consumption.

### Internals

- [#2024](https://github.com/tigerbeetle/tigerbeetle/pull/2024),
  [#2018](https://github.com/tigerbeetle/tigerbeetle/pull/2018),
  [#2027](https://github.com/tigerbeetle/tigerbeetle/pull/2027)

  Build system simplifications.

- [#2026](https://github.com/tigerbeetle/tigerbeetle/pull/2026),
  [#2020](https://github.com/tigerbeetle/tigerbeetle/pull/2020),
  [#2030](https://github.com/tigerbeetle/tigerbeetle/pull/2030),
  [#2031](https://github.com/tigerbeetle/tigerbeetle/pull/2031),
  [#2008](https://github.com/tigerbeetle/tigerbeetle/pull/2008)

  Tidying up (now) unused symbols and functionality.

- [#2016](https://github.com/tigerbeetle/tigerbeetle/pull/2016)

  Rename docs section from "Develop" to "Coding".

### TigerTracks 🎧

- [On The Riverbank](https://open.spotify.com/track/0zfluauTutYrU13nEV2zyc?si=5278f387bfdd4dbc)

## 2024-06-17 (No release: Queued up for upcoming multi-version binary release)

### Safety And Performance

- [#2000](https://github.com/tigerbeetle/tigerbeetle/pull/2000)

  Fix a case where an early return could result in a partially inserted transfer persisting.

- [#2011](https://github.com/tigerbeetle/tigerbeetle/pull/2011),
  [#2009](https://github.com/tigerbeetle/tigerbeetle/pull/2009),
  [#1981](https://github.com/tigerbeetle/tigerbeetle/pull/1981)

  Big improvements to allowing TigerBeetle to run with less memory! You can now run TigerBeetle
  in `--development` mode by default with an RSS of under 1GB. Most of these gains came from #1981
  which allows running with a smaller runtime request size.

- [#2014](https://github.com/tigerbeetle/tigerbeetle/pull/2014),
  [#2012](https://github.com/tigerbeetle/tigerbeetle/pull/2012),
  [#2006](https://github.com/tigerbeetle/tigerbeetle/pull/2006)

  Devhub improvements - make it harder to miss failures due to visualization bugs, show the PR
  author in fuzzer table and color canary "failures" as success.

### Features

- [#2001](https://github.com/tigerbeetle/tigerbeetle/pull/2001)

  Add `--account-batch-size` to the benchmark, mirroring `--transfer-batch-size`.

- [#2017](https://github.com/tigerbeetle/tigerbeetle/pull/2017),
  [#1992](https://github.com/tigerbeetle/tigerbeetle/pull/1992),
  [#1993](https://github.com/tigerbeetle/tigerbeetle/pull/1993)

  Rename the Deploy section to Operating, add a new correcting transfer recipe, and note that
  `lookup_accounts` shouldn't be used before creating transfers to avoid potential TOCTOUs.

### Internals

- [#1878](https://github.com/tigerbeetle/tigerbeetle/pull/1878),
  [#1997](https://github.com/tigerbeetle/tigerbeetle/pull/1997)

  ⚡ Update Zig from 0.11.0 to 0.13.0! As part of this, replace non-mutated `var`s with `const`.

- [#1999](https://github.com/tigerbeetle/tigerbeetle/pull/1999)

  Similar to #1991, adds the async `io_uring_prep_statx` syscall for Linux's IO implementation,
  allowing non-blocking `statx()`s while serving requests - to determine when the binary on
  disk has changed.


### TigerTracks 🎧

- [Canon in D](https://www.youtube.com/watch?v=Ptk_1Dc2iPY)

## 2024-06-10 (No release: Queued up for upcoming multi-version binary release)

### Safety And Performance

- [#1986](https://github.com/tigerbeetle/tigerbeetle/pull/1986)

  Refactor an internal iterator to expose a mutable pointer instead of calling `@constCast` on it.
  There was a comment justifying the operation's safety, but it turned out to be safer to expose
  it as a mutable pointer (avoiding misusage from the origin) rather than performing an unsound
  mutation over a constant pointer.

- [#1985](https://github.com/tigerbeetle/tigerbeetle/pull/1985)

  Implement a random Grid/Scrubber tour origin, where each replica starts scrubbing the local
  storage in a different place, covering more blocks across the entire cluster.

- [#1990](https://github.com/tigerbeetle/tigerbeetle/pull/1990)

  Model and calculate the probability of data loss in terms of the Grid/Scrubber cycle interval,
  allowing to reduce the read bandwidth dedicated for scrubbing.

- [#1987](https://github.com/tigerbeetle/tigerbeetle/pull/1987)

  Fix a simulator bug where all the WAL sectors get corrupted when a replica crashes while writing
  them simultaneously.

### Internals

- [#1991](https://github.com/tigerbeetle/tigerbeetle/pull/1991)

  As part of multiversioning binaries, adds the async `io_uring_prep_openat`syscall for Linux's IO
  implementation, allowing non-blocking `open()`s while serving requests (which will be necessary
  during upgrade checks).

- [#1982](https://github.com/tigerbeetle/tigerbeetle/pull/1982)

  Require the `--experimental` flag when starting TigerBeetle with flags that aren't considered
  stable, that is, flags not explicitly documented in the help message, limiting the surface area
  for future compatibility.

### TigerTracks 🎧

- [O Rappa - A feira](https://www.youtube.com/watch?v=GmaFGnUnM1U)

## 2024-06-03 (No release: Queued up for upcoming multi-version binary release)

### Safety And Performance

- [#1980](https://github.com/tigerbeetle/tigerbeetle/pull/1980)

  Fix crash when upgrading solo replica.

- [#1952](https://github.com/tigerbeetle/tigerbeetle/pull/1952)

  Pin points crossing Go client FFI boundary to prevent memory corruption.

### Internals

- [#1931](https://github.com/tigerbeetle/tigerbeetle/pull/1931),
  [#1933](https://github.com/tigerbeetle/tigerbeetle/pull/1933)

  Improve Go client tests.

- [#1946](https://github.com/tigerbeetle/tigerbeetle/pull/1946)

  Add `vsr.Client.register()`.

## 2024-05-27 (No release: Queued up for upcoming multi-version binary release)

### Features

- [#1975](https://github.com/tigerbeetle/tigerbeetle/pull/1975)

  Build our .NET client for .NET 8, the current LTS version. Thanks @woksin!

### Internals

- [#1971](https://github.com/tigerbeetle/tigerbeetle/pull/1971)

  Document recovery case `@L` in VSR.

- [#1965](https://github.com/tigerbeetle/tigerbeetle/pull/1965)

  We implicitly supported underscores in numerical CLI flags. Add tests to make this explicit.

- [#1974](https://github.com/tigerbeetle/tigerbeetle/pull/1974),
  [#1970](https://github.com/tigerbeetle/tigerbeetle/pull/1970)

  Add the size of an empty data file to [devhub](https://tigerbeetle.github.io/tigerbeetle/),
  tweak the benchmark to always generate the same sized batches, and speed up loading the
  devhub itself.

### TigerTracks 🎧

- [Fight Song](https://www.youtube.com/watch?v=xo1VInw-SKc)

## 2024-05-20 (No release: Queued up for upcoming multi-version binary release)

### Safety And Performance

- [#1938](https://github.com/tigerbeetle/tigerbeetle/pull/1938)

  Ease restriction which guarded against unnecessary pulses.

### Internals

- [#1949](https://github.com/tigerbeetle/tigerbeetle/pull/1949),
  [#1964](https://github.com/tigerbeetle/tigerbeetle/pull/1964)

  Docs fixes and cleanup.

- [#1957](https://github.com/tigerbeetle/tigerbeetle/pull/1957)

  Fix determinism bug in test workload checker.

- [#1955](https://github.com/tigerbeetle/tigerbeetle/pull/1955)

  Expose `ticks_max` as runtime CLI argument.

- [#1956](https://github.com/tigerbeetle/tigerbeetle/pull/1956),
  [#1959](https://github.com/tigerbeetle/tigerbeetle/pull/1959),
  [#1960](https://github.com/tigerbeetle/tigerbeetle/pull/1960),
  [#1963](https://github.com/tigerbeetle/tigerbeetle/pull/1963)

  Devhub/benchmark improvements.

## 2024-05-13 (No release: Queued up for upcoming multi-version binary release)

### Safety And Performance
- [#1918](https://github.com/tigerbeetle/tigerbeetle/pull/1918),
  [#1916](https://github.com/tigerbeetle/tigerbeetle/pull/1916),
  [#1913](https://github.com/tigerbeetle/tigerbeetle/pull/1913),
  [#1921](https://github.com/tigerbeetle/tigerbeetle/pull/1921),
  [#1922](https://github.com/tigerbeetle/tigerbeetle/pull/1922),
  [#1920](https://github.com/tigerbeetle/tigerbeetle/pull/1920),
  [#1945](https://github.com/tigerbeetle/tigerbeetle/pull/1945),
  [#1941](https://github.com/tigerbeetle/tigerbeetle/pull/1941),
  [#1934](https://github.com/tigerbeetle/tigerbeetle/pull/1934),
  [#1927](https://github.com/tigerbeetle/tigerbeetle/pull/1927)

  Lots of CFO enhancements - the CFO can now do simple minimization, fuzz PRs and orchestrate the
  VOPR directly. See the output on our [devhub](https://tigerbeetle.github.io/tigerbeetle/)!

- [#1948](https://github.com/tigerbeetle/tigerbeetle/pull/1948),
  [#1929](https://github.com/tigerbeetle/tigerbeetle/pull/1929),
  [#1924](https://github.com/tigerbeetle/tigerbeetle/pull/1924)

  Fix a bug in the VOPR, add simple minimization, and remove the voprhub code. Previously, the
  voprhub is what took care of running the VOPR. Now, it's handled by the CFO and treated much
  the same as other fuzzers.

- [#1947](https://github.com/tigerbeetle/tigerbeetle/pull/1947)

  Prevent time-travel in our replica test code.

- [#1943](https://github.com/tigerbeetle/tigerbeetle/pull/1943)

  Fix a fuzzer bug around checkpoint / commit ratios.

### Features

- [#1898](https://github.com/tigerbeetle/tigerbeetle/pull/1898)

  Add the ability to limit the VSR pipeline size at runtime to save memory.

### Internals
- [#1925](https://github.com/tigerbeetle/tigerbeetle/pull/1925)

  Fix path handling on Windows by switching to `NtCreateFile`. Before, TigerBeetle would silently
  treat all paths as relative on Windows.

- [#1917](https://github.com/tigerbeetle/tigerbeetle/pull/1917)

  In preparation for multiversion binaries, make `release_client_min` a parameter, set by
  `release.zig`. This allows us to ensure backwards compatibility with older clients.

- [#1827](https://github.com/tigerbeetle/tigerbeetle/pull/1827)

  Add some additional asserts around block lifetimes in compaction.

- [#1939](https://github.com/tigerbeetle/tigerbeetle/pull/1939)

  Fix parsing of multiple CLI positional fields.

- [#1923](https://github.com/tigerbeetle/tigerbeetle/pull/1923)

  Remove `main_pkg_path = src/` early, to help us be compatible with Zig 0.12.

- [#1937](https://github.com/tigerbeetle/tigerbeetle/pull/1937),
  [#1912](https://github.com/tigerbeetle/tigerbeetle/pull/1912),
  [#1852](https://github.com/tigerbeetle/tigerbeetle/pull/1852)

  Docs organization and link fixes.

### TigerTracks 🎧

- [Thank You (Not So Bad)](https://www.youtube.com/watch?v=fQWNeIiFf_s)

## 2024-05-06 (No release: Queued up for upcoming multi-version binary release)

### Safety And Performance

- [#1906](https://github.com/tigerbeetle/tigerbeetle/pull/1906),
  [#1904](https://github.com/tigerbeetle/tigerbeetle/pull/1904),
  [#1903](https://github.com/tigerbeetle/tigerbeetle/pull/1903),
  [#1901](https://github.com/tigerbeetle/tigerbeetle/pull/1901),
  [#1899](https://github.com/tigerbeetle/tigerbeetle/pull/1899),
  [#1886](https://github.com/tigerbeetle/tigerbeetle/pull/1886)

  Fixes and performance improvements to fuzzers.

- [#1897](https://github.com/tigerbeetle/tigerbeetle/pull/1897)

  Reduces cache size for the `--development` flag, which was originally created to bypass direct
  I/O requirements but can also aggregate other convenient options for non-production environments.

- [#1895](https://github.com/tigerbeetle/tigerbeetle/pull/1895)

  Reduction in memory footprint, calculating the maximum number of messages from runtime-known
  configurations.

### Features

- [#1896](https://github.com/tigerbeetle/tigerbeetle/pull/1896)

  Removes the `bootstrap.{sh,bat}` scripts, replacing them with a more transparent instruction for
  downloading the binary release or building from source.

- [#1890](https://github.com/tigerbeetle/tigerbeetle/pull/1890)

  Nicely handles "illegal instruction" crashes, printing a friendly message when the CPU running a
  binary release is too old and does not support some modern instructions such as AES-NI and AVX2.

### Internals

- [#1892](https://github.com/tigerbeetle/tigerbeetle/pull/1892)

  Include micro-benchmarks as part of the unit tests, so there's no need for a special case in the
  CI while we still compile and check them.

- [#1902](https://github.com/tigerbeetle/tigerbeetle/pull/1902)

  A TigerStyle addition on "why prefer a explicitly sized integer over `usize`".

- [#1894](https://github.com/tigerbeetle/tigerbeetle/pull/1894)

  Rename "Getting Started" to "Quick Start" for better organization and clarifications.

- [#1900](https://github.com/tigerbeetle/tigerbeetle/pull/1900)

  While TigerBeetle builds are deterministic, Zip files include a timestamp that makes the build
  output non-deterministic! This PR sets an explicit timestamp for entirely reproducible releases.

- [1909](https://github.com/tigerbeetle/tigerbeetle/pull/1909)

  Extracts the zig compiler path into a `ZIG_EXE` environment variable, allowing easier sharing of
  the same compiler across multiple git work trees.

### TigerTracks 🎧

- [Thank You](https://www.youtube.com/watch?v=1TO48Cnl66w)

## 2024-04-29 (No release: Queued up for upcoming multi-version binary release)

### Safety And Performance

- [#1883](https://github.com/tigerbeetle/tigerbeetle/pull/1883)

  Move message allocation farther down into the `tigerbeetle start` code path.
  `tigerbeetle format` is now faster, since it no longer allocates these messages.

- [#1880](https://github.com/tigerbeetle/tigerbeetle/pull/1880)

  Reduce the connection limit, which was unnecessarily high.

### Features

- [#1848](https://github.com/tigerbeetle/tigerbeetle/pull/1848)

  Implement zig-zag merge join for merging index scans.
  (Note that this functionality is not yet exposed to TigerBeetle's API.)

- [#1882](https://github.com/tigerbeetle/tigerbeetle/pull/1882)

  Print memory usage more accurately during `tigerbeetle start`.

### Internals

- [#1874](https://github.com/tigerbeetle/tigerbeetle/pull/1874)

  Fix blob-size CI check with respect to shallow clones.

- [#1870](https://github.com/tigerbeetle/tigerbeetle/pull/1870),
  [#1869](https://github.com/tigerbeetle/tigerbeetle/pull/1869)

  Add more fuzzers to CFO (Continuous Fuzzing Orchestrator).

- [#1868](https://github.com/tigerbeetle/tigerbeetle/pull/1868),
  [#1875](https://github.com/tigerbeetle/tigerbeetle/pull/1875)

  Improve fuzzer performance.

- [#1864](https://github.com/tigerbeetle/tigerbeetle/pull/1864)

  On the devhub, show at most one failing seed per fuzzer.

- [#1820](https://github.com/tigerbeetle/tigerbeetle/pull/1820),
  [#1867](https://github.com/tigerbeetle/tigerbeetle/pull/1867),
  [#1877](https://github.com/tigerbeetle/tigerbeetle/pull/1877),
  [#1873](https://github.com/tigerbeetle/tigerbeetle/pull/1873),
  [#1853](https://github.com/tigerbeetle/tigerbeetle/pull/1853),
  [#1872](https://github.com/tigerbeetle/tigerbeetle/pull/1872),
  [#1845](https://github.com/tigerbeetle/tigerbeetle/pull/1845),
  [#1871](https://github.com/tigerbeetle/tigerbeetle/pull/1871)

  Documentation improvements.

### TigerTracks 🎧

- [The Core](https://open.spotify.com/track/62DOxN9FeTsR0J0ccnBhMu?si=5b0a7b8974d54e4d)

## 2024-04-22 (No release: Queued up for upcoming multi-version binary release)

### Safety And Performance

- [#1851](https://github.com/tigerbeetle/tigerbeetle/pull/1851)

  Implement grid scrubbing --- a background job that periodically reads the entire data file,
  verifies its correctness and repairs any corrupted blocks.

- [#1855](https://github.com/tigerbeetle/tigerbeetle/pull/1855),
  [#1854](https://github.com/tigerbeetle/tigerbeetle/pull/1854).

  Turn on continuous fuzzing and integrate it with
  [devhub](https://tigerbeetle.github.io/tigerbeetle/).

### Internals

- [#1849](https://github.com/tigerbeetle/tigerbeetle/pull/1849)

  Improve navigation on the docs website.

### TigerTracks 🎧

A very special song from our friend [MEGAHIT](https://www.megahit.hu)!

- [TigerBeetle](https://open.spotify.com/track/66pxevn7ImjMDozcs1TE3Q?si=dfbbf7b80179481e)

## 2024-04-15 (No release: Queued up for upcoming multi-version binary release)

### Safety And Performance

- [#1810](https://github.com/tigerbeetle/tigerbeetle/pull/1810)

  Incrementally recompute the number values to compact in the storage engine. This smooths out I/O
  latency, giving a nice bump to transaction throughput under load.

### Features

- [#1843](https://github.com/tigerbeetle/tigerbeetle/pull/1843)

  Add `--development` flag to `format` and `start` commands in production binaries to downgrade
  lack of Direct I/O support from a hard error to a warning.

  TigerBeetle uses Direct I/O for certain safety guarantees, but this feature is not available on
  all development environments due to varying file systems. This serves as a compromise between
  providing a separate development release binary and strictly requiring Direct I/O to be present.

### Internals

- [#1833](https://github.com/tigerbeetle/tigerbeetle/pull/1833)

  Add fixed upper bound to loop in the StorageChecker.

- [#1836](https://github.com/tigerbeetle/tigerbeetle/pull/1836)

  Orchestrate continuous fuzzing of tigerbeetle components straight from the build system! This
  gives us some flexibility on configuring our set of machines which test and report errors.

- [#1842](https://github.com/tigerbeetle/tigerbeetle/pull/1842),
  [#1844](https://github.com/tigerbeetle/tigerbeetle/pull/1844),
  [#1832](https://github.com/tigerbeetle/tigerbeetle/pull/1832)

  Styling updates and fixes.

### TigerTracks 🎧

- [CHERRY PEPSI](https://www.youtube.com/watch?v=D5Avlh980k4)


## 2024-04-08 (No release: Queued up for upcoming multi-version binary release)

### Safety And Performance

- [#1821](https://github.com/tigerbeetle/tigerbeetle/pull/1821)

  Fix a case the VOPR found where a replica recovers into `recovering_head` unexpectedly.

### Features

- [#1565](https://github.com/tigerbeetle/tigerbeetle/pull/1565)

  Improve CLI errors around sizing by providing human readable (1057MiB vs 1108344832) values.

- [#1818](https://github.com/tigerbeetle/tigerbeetle/pull/1818),
  [#1831](https://github.com/tigerbeetle/tigerbeetle/pull/1831),
  [#1829](https://github.com/tigerbeetle/tigerbeetle/pull/1829),
  [#1817](https://github.com/tigerbeetle/tigerbeetle/pull/1817),
  [#1826](https://github.com/tigerbeetle/tigerbeetle/pull/1826),
  [#1825](https://github.com/tigerbeetle/tigerbeetle/pull/1825)

  Documentation improvements.

### Internals

- [#1806](https://github.com/tigerbeetle/tigerbeetle/pull/1806)

  Additional LSM compaction comments and assertions.

- [#1824](https://github.com/tigerbeetle/tigerbeetle/pull/1824)

  Clarify some scan internals and add additional assertions.

- [#1828](https://github.com/tigerbeetle/tigerbeetle/pull/1828)

  Some of our comments had duplicate words - thanks @divdeploy for
  <!-- vale Vale.Repetition = NO -->for<!-- vale Vale.Repetition = YES --> noticing!

### TigerTracks 🎧

- [All The Small Things](https://www.youtube.com/watch?v=Sn0gVjPrUj0)

## 2024-04-01 (Placeholder: no release yet)

### Safety And Performance

- [#1766](https://github.com/tigerbeetle/tigerbeetle/pull/1766)

  Reject incoming client requests that have an unexpected message length.

- [#1768](https://github.com/tigerbeetle/tigerbeetle/pull/1768)

  Fix message alignment.

- [#1772](https://github.com/tigerbeetle/tigerbeetle/pull/1772),
  [#1786](https://github.com/tigerbeetle/tigerbeetle/pull/1786)

  `StorageChecker` now verifies grid determinism at bar boundaries.

- [#1776](https://github.com/tigerbeetle/tigerbeetle/pull/1776)

  Fix VOPR liveness false positive when standby misses an op.

- [#1814](https://github.com/tigerbeetle/tigerbeetle/pull/1814)

  Assert that the type-erased LSM block metadata matches the comptime one, specialized over `Tree`.

- [#1797](https://github.com/tigerbeetle/tigerbeetle/pull/1797)

  Use a FIFO as a block_pool instead of trying to slice arrays during compaction.

### Features

- [#1774](https://github.com/tigerbeetle/tigerbeetle/pull/1774)

  Implement `get_account_transfers` and `get_account_balances` in the REPL.

- [#1781](https://github.com/tigerbeetle/tigerbeetle/pull/1781),
  [#1784](https://github.com/tigerbeetle/tigerbeetle/pull/1784),
  [#1765](https://github.com/tigerbeetle/tigerbeetle/pull/1765),
  [#1816](https://github.com/tigerbeetle/tigerbeetle/pull/1816),
  [#1808](https://github.com/tigerbeetle/tigerbeetle/pull/1808),
  [#1802](https://github.com/tigerbeetle/tigerbeetle/pull/1802),
  [#1798](https://github.com/tigerbeetle/tigerbeetle/pull/1798),
  [#1793](https://github.com/tigerbeetle/tigerbeetle/pull/1793),
  [#1805](https://github.com/tigerbeetle/tigerbeetle/pull/1805)

  Documentation improvements.

- [#1813](https://github.com/tigerbeetle/tigerbeetle/pull/1813)

  Improve Docker experience by handling `SIGTERM` through [tini](https://github.com/krallin/tini).

- [#1800](https://github.com/tigerbeetle/tigerbeetle/pull/1800)

  For reproducible benchmarks, allow setting `--seed` on the CLI.

### Internals

- [#1640](https://github.com/tigerbeetle/tigerbeetle/pull/1640),
  [#1782](https://github.com/tigerbeetle/tigerbeetle/pull/1782),
  [#1788](https://github.com/tigerbeetle/tigerbeetle/pull/1788)

  Move `request_queue` outside of `vsr.Client`.

- [#1775](https://github.com/tigerbeetle/tigerbeetle/pull/1775)

  Extract `CompactionPipeline` to a dedicated function.

- [#1773](https://github.com/tigerbeetle/tigerbeetle/pull/1773)

  Replace compaction interface with comptime dispatch.

- [#1796](https://github.com/tigerbeetle/tigerbeetle/pull/1796)

  Remove the duplicated `CompactionInfo` value stored in `PipelineSlot`,
  referencing it from the `Compaction` by its coordinates.

- [#1809](https://github.com/tigerbeetle/tigerbeetle/pull/1809),
  [#1807](https://github.com/tigerbeetle/tigerbeetle/pull/1807)

  CLI output improvements.

- [#1804](https://github.com/tigerbeetle/tigerbeetle/pull/1804),
  [#1812](https://github.com/tigerbeetle/tigerbeetle/pull/1812),
  [#1799](https://github.com/tigerbeetle/tigerbeetle/pull/1799),
  [#1767](https://github.com/tigerbeetle/tigerbeetle/pull/1767)

  Improvements in the client libraries CI.

- [#1771](https://github.com/tigerbeetle/tigerbeetle/pull/1771),
  [#1770](https://github.com/tigerbeetle/tigerbeetle/pull/1770),
  [#1792](https://github.com/tigerbeetle/tigerbeetle/pull/1792)

  Metrics adjustments for Devhub and Nyrkio integration.

- [#1811](https://github.com/tigerbeetle/tigerbeetle/pull/1811),
  [#1803](https://github.com/tigerbeetle/tigerbeetle/pull/1803),
  [#1801](https://github.com/tigerbeetle/tigerbeetle/pull/1801),
  [#1762](https://github.com/tigerbeetle/tigerbeetle/pull/1762)

  Various bug fixes in the build script and removal of the "Do not use in production" warning.

## 2024-03-19

- Bump version to 0.15.x
- Starting with 0.15.x, TigerBeetle is ready for production use, preserves durability and
  provides a forward upgrade path through storage stability.

### Safety And Performance
- [#1755](https://github.com/tigerbeetle/tigerbeetle/pull/1755)

  Set TigerBeetle's block size to 512KB.

  Previously, we used to have a block size of 1MB to help with approximate pacing. Now that pacing
  can be tuned independently of block size, reduce this value (but not too much - make the roads
  wider than you think) to help with read amplification on queries.

### TigerTracks 🎧

- [Immigrant Song - Live 1972](https://open.spotify.com/track/2aH2dcPnwoQwhLsXFezU2r?si=eead2c0cd17c429e)

## 2024-03-18

### Safety And Performance

- [#1660](https://github.com/tigerbeetle/tigerbeetle/pull/1660)

  Implement compaction pacing: traditionally LSM databases run compaction on a background thread.
  In contrast compaction in tigerbeetle is deterministically interleaved with normal execution
  process, to get predictable latencies and to guarantee that ingress can never outrun compaction.

  In this PR, this "deterministic scheduling" is greatly improved, slicing compaction work into
  smaller bites which are more evenly distributed across a bar of batched requests.

- [#1722](https://github.com/tigerbeetle/tigerbeetle/pull/1722)

  Include information about tigerbeetle version into the VSR protocol and the data file.

- [#1732](https://github.com/tigerbeetle/tigerbeetle/pull/1732),
  [#1743](https://github.com/tigerbeetle/tigerbeetle/pull/1743),
  [#1742](https://github.com/tigerbeetle/tigerbeetle/pull/1742),
  [#1720](https://github.com/tigerbeetle/tigerbeetle/pull/1720),
  [#1719](https://github.com/tigerbeetle/tigerbeetle/pull/1719),
  [#1705](https://github.com/tigerbeetle/tigerbeetle/pull/1705),
  [#1708](https://github.com/tigerbeetle/tigerbeetle/pull/1708),
  [#1707](https://github.com/tigerbeetle/tigerbeetle/pull/1707),
  [#1723](https://github.com/tigerbeetle/tigerbeetle/pull/1723),
  [#1706](https://github.com/tigerbeetle/tigerbeetle/pull/1706),
  [#1700](https://github.com/tigerbeetle/tigerbeetle/pull/1700),
  [#1696](https://github.com/tigerbeetle/tigerbeetle/pull/1696),
  [#1686](https://github.com/tigerbeetle/tigerbeetle/pull/1686).

  Many availability issues found by the simulator fixed!

- [#1734](https://github.com/tigerbeetle/tigerbeetle/pull/1734)

  Fix a buffer leak when `get_account_balances` is called on an invalid account.


### Features

- [#1671](https://github.com/tigerbeetle/tigerbeetle/pull/1671),
  [#1713](https://github.com/tigerbeetle/tigerbeetle/pull/1713),
  [#1709](https://github.com/tigerbeetle/tigerbeetle/pull/1709),
  [#1688](https://github.com/tigerbeetle/tigerbeetle/pull/1688),
  [#1691](https://github.com/tigerbeetle/tigerbeetle/pull/1691),
  [#1690](https://github.com/tigerbeetle/tigerbeetle/pull/1690).

  Many improvements to the documentation!

- [#1733](https://github.com/tigerbeetle/tigerbeetle/pull/1733)

  Rename `get_account_history` to `get_account_balances`.

- [#1657](https://github.com/tigerbeetle/tigerbeetle/pull/1657)

  Automatically expire pending transfers.

- [#1682](https://github.com/tigerbeetle/tigerbeetle/pull/1682)

  Implement in-place upgrades, so that the version of tigerbeetle binary can be updated without
  recreating the data file from scratch.

- [#1674](https://github.com/tigerbeetle/tigerbeetle/pull/1674)

  Consistently use `MiB` rather than `MB` in the CLI interface.

- [#1678](https://github.com/tigerbeetle/tigerbeetle/pull/1678)

  Mark `--standby` and `benchmark` CLI arguments as experimental.

### Internals

- [#1726](https://github.com/tigerbeetle/tigerbeetle/pull/1726)

  Unify PostedGroove and the index pending_status.

- [#1681](https://github.com/tigerbeetle/tigerbeetle/pull/1681)

  Include an entire header into checkpoint state to ease recovery after state sync.


### TigerTracks 🎧

- [Are You Gonna Go My Way](https://open.spotify.com/track/4LQOa4kXu0QAD88nMpr4fA?si=f7faf501919942b5)

## 2024-03-11

### Safety And Performance

- [#1663](https://github.com/tigerbeetle/tigerbeetle/pull/1663)

  Fetching account history and transfers now has unit tests, helping detect and fix a reported bug
  with posting and voiding transfers.

### Internals

- [#1648](https://github.com/tigerbeetle/tigerbeetle/pull/1648),
  [#1665](https://github.com/tigerbeetle/tigerbeetle/pull/1665),
  [#1654](https://github.com/tigerbeetle/tigerbeetle/pull/1654),
  [#1651](https://github.com/tigerbeetle/tigerbeetle/pull/1651)

  Testing and Timer logic was subject to some spring cleaning.

### Features

- [#1656](https://github.com/tigerbeetle/tigerbeetle/pull/1656),
  [#1659](https://github.com/tigerbeetle/tigerbeetle/pull/1659),
  [#1666](https://github.com/tigerbeetle/tigerbeetle/pull/1666),
  [#1667](https://github.com/tigerbeetle/tigerbeetle/pull/1667),
  [#1667](https://github.com/tigerbeetle/tigerbeetle/pull/1670)

  Preparation for in-place upgrade support.

- [#1633](https://github.com/tigerbeetle/tigerbeetle/pull/1633),
  [#1661](https://github.com/tigerbeetle/tigerbeetle/pull/1661),
  [#1652](https://github.com/tigerbeetle/tigerbeetle/pull/1652),
  [#1647](https://github.com/tigerbeetle/tigerbeetle/pull/1647),
  [#1637](https://github.com/tigerbeetle/tigerbeetle/pull/1637),
  [#1638](https://github.com/tigerbeetle/tigerbeetle/pull/1638),
  [#1655](https://github.com/tigerbeetle/tigerbeetle/pull/1655)

  [Documentation](https://docs.tigerbeetle.com/) has received some very welcome organizational
  and clarity changes. Go check them out!

### TigerTracks 🎧

- [Você Chegou](https://open.spotify.com/track/5Ns9a6JKX4sdUlaAh4SSGy)

## 2024-03-04

### Safety And Performance

- [#1584](https://github.com/tigerbeetle/tigerbeetle/pull/1584)
  Lower our memory usage by removing a redundant stash and not requiring a non-zero object cache
  size for Grooves.

  The object cache is designed to help things like Account lookups, where the positive case can
  skip all the prefetch machinery, but it doesn't make as much sense for other Grooves.

- [#1581](https://github.com/tigerbeetle/tigerbeetle/pull/1581)
  [#1611](https://github.com/tigerbeetle/tigerbeetle/pull/1611)

  Hook [nyrkiö](https://nyrkio.com/) up to our CI! You can find our dashboard
  [here](https://nyrkio.com/public/https%3A%2F%2Fgithub.com%2Ftigerbeetle%2Ftigerbeetle/main/devhub)
  in addition to our [devhub](https://tigerbeetle.github.io/tigerbeetle/).

- [#1635](https://github.com/tigerbeetle/tigerbeetle/pull/1635)
  [#1634](https://github.com/tigerbeetle/tigerbeetle/pull/1634)
  [#1623](https://github.com/tigerbeetle/tigerbeetle/pull/1623)
  [#1619](https://github.com/tigerbeetle/tigerbeetle/pull/1619)
  [#1609](https://github.com/tigerbeetle/tigerbeetle/pull/1609)
  [#1608](https://github.com/tigerbeetle/tigerbeetle/pull/1608)
  [#1595](https://github.com/tigerbeetle/tigerbeetle/pull/1595)

  Lots of small VSR changes, including a VOPR crash fix.

- [#1598](https://github.com/tigerbeetle/tigerbeetle/pull/1598)

  Fix a VOPR failure where state sync would cause a break in the hash chain.

### Internals

- [#1599](https://github.com/tigerbeetle/tigerbeetle/pull/1599)
  [#1597](https://github.com/tigerbeetle/tigerbeetle/pull/1597)

  Use Expand-Archive over unzip in PowerShell - thanks @felipevalerio for reporting!

- [#1607](https://github.com/tigerbeetle/tigerbeetle/pull/1607)
  [#1620](https://github.com/tigerbeetle/tigerbeetle/pull/1620)

  Implement [explicit coverage marks](https://ferrous-systems.com/blog/coverage-marks/).

- [#1621](https://github.com/tigerbeetle/tigerbeetle/pull/1621)
  [#1625](https://github.com/tigerbeetle/tigerbeetle/pull/1625)
  [#1622](https://github.com/tigerbeetle/tigerbeetle/pull/1622)
  [#1600](https://github.com/tigerbeetle/tigerbeetle/pull/1600)
  [#1605](https://github.com/tigerbeetle/tigerbeetle/pull/1605)
  [#1618](https://github.com/tigerbeetle/tigerbeetle/pull/1618)
  [#1606](https://github.com/tigerbeetle/tigerbeetle/pull/1606)

  Minor doc fixups.

- [#1636](https://github.com/tigerbeetle/tigerbeetle/pull/1636)
  [#1626](https://github.com/tigerbeetle/tigerbeetle/pull/1626)

  Default the VOPR to short log, and fix a false assertion in the liveness checker.

- [#1596](https://github.com/tigerbeetle/tigerbeetle/pull/1596)

  Fix a memory leak in our Java tests.

### TigerTracks 🎧

- [Auffe aufn Berg](https://www.youtube.com/watch?v=eRbkRNaqy9Y)

## 2024-02-26

### Safety And Performance

- [#1591](https://github.com/tigerbeetle/tigerbeetle/pull/1591)
  [#1589](https://github.com/tigerbeetle/tigerbeetle/pull/1589)
  [#1579](https://github.com/tigerbeetle/tigerbeetle/pull/1579)
  [#1576](https://github.com/tigerbeetle/tigerbeetle/pull/1576)

  Rework the log repair logic to never repair beyond a "confirmed" checkpoint, fixing a
  [liveness issue](https://github.com/tigerbeetle/tigerbeetle/issues/1378) where it was impossible
  for the primary to repair its entire log, even with a quorum of replicas at a recent checkpoint.

- [#1572](https://github.com/tigerbeetle/tigerbeetle/pull/1572)

  Some Java unit tests created native client instances without the proper deinitialization,
  causing an `OutOfMemoryError` during CI.

- [#1569](https://github.com/tigerbeetle/tigerbeetle/pull/1569)
  [#1570](https://github.com/tigerbeetle/tigerbeetle/pull/1570)

  Fix Vopr's false alarms.

### Internals

- [#1585](https://github.com/tigerbeetle/tigerbeetle/pull/1585)

  Document how assertions should be used, especially those with complexity _O(n)_ under
  the `constants.verify` conditional.

- [#1580](https://github.com/tigerbeetle/tigerbeetle/pull/1580)

  Harmonize and automate the logging pattern by using the `@src` built-in to retrieve the
  function name.

- [#1568](https://github.com/tigerbeetle/tigerbeetle/pull/1568)

  Include the benchmark smoke as part of the `zig build test` command rather than a special case
  during CI.

- [#1574](https://github.com/tigerbeetle/tigerbeetle/pull/1574)

  Remove unused code coverage metrics from the CI.

- [#1575](https://github.com/tigerbeetle/tigerbeetle/pull/1575)
  [#1573](https://github.com/tigerbeetle/tigerbeetle/pull/1573)
  [#1582](https://github.com/tigerbeetle/tigerbeetle/pull/1582)

  Re-enable Windows CI 🎉.

### TigerTracks 🎧

- [Dos Margaritas](https://www.youtube.com/watch?v=Ts_7BYubYws)

  [(_versión en español_)](https://www.youtube.com/watch?v=B_VLegyguoI)

## 2024-02-19

### Safety And Performance

- [#1533](https://github.com/tigerbeetle/tigerbeetle/pull/1533)

  DVCs implicitly nack missing prepares from old log-views.

  (This partially addresses a liveness issue in the view change.)

- [#1552](https://github.com/tigerbeetle/tigerbeetle/pull/1552)

  When a replica joins a view by receiving an SV message, some of the SV's headers may be too far
  ahead to insert into the journal. (That is, they are beyond the replica's checkpoint trigger.)

  During a view change, those headers are now eligible to be DVC headers.

  (This partially addresses a liveness issue in the view change.)

- [#1560](https://github.com/tigerbeetle/tigerbeetle/pull/1560)

  Fixes a bug in the C client that wasn't handling `error.TooManyOutstanding` correctly.

### Internals

- [#1482](https://github.com/tigerbeetle/tigerbeetle/pull/1482)

  Bring back Windows tests for .NET client in CI.

- [#1540](https://github.com/tigerbeetle/tigerbeetle/pull/1540)

  Add script to scaffold changelog updates.

- [#1542](https://github.com/tigerbeetle/tigerbeetle/pull/1542),
  [#1553](https://github.com/tigerbeetle/tigerbeetle/pull/1553),
  [#1559](https://github.com/tigerbeetle/tigerbeetle/pull/1559),
  [#1561](https://github.com/tigerbeetle/tigerbeetle/pull/1561)

  Improve CI/test error reporting.

- [#1551](https://github.com/tigerbeetle/tigerbeetle/pull/1551)

  Draw devhub graph as line graph.

- [#1554](https://github.com/tigerbeetle/tigerbeetle/pull/1554)

  Simplify command to run a single test.

- [#1555](https://github.com/tigerbeetle/tigerbeetle/pull/1555)

  Add client batching integration tests.

- [#1557](https://github.com/tigerbeetle/tigerbeetle/pull/1557)

  Format default values into the CLI help message.

- [#1558](https://github.com/tigerbeetle/tigerbeetle/pull/1558)

  Track commit timestamp to enable retrospective benchmarking in the devhub.

- [#1562](https://github.com/tigerbeetle/tigerbeetle/pull/1562),
  [#1563](https://github.com/tigerbeetle/tigerbeetle/pull/1563)

  Improve CI/test performance.

- [#1567](https://github.com/tigerbeetle/tigerbeetle/pull/1567)

  Guarantee that the test runner correctly reports "zero tests run" when run with a filter that
  matches no tests.

### TigerTracks 🎧

- [Eye Of The Tiger](https://www.youtube.com/watch?v=btPJPFnesV4)

  (Hat tip to [iofthetiger](https://ziggit.dev/t/iofthetiger/3065)!)

## 2024-02-12

### Safety And Performance

- [#1519](https://github.com/tigerbeetle/tigerbeetle/pull/1519)

  Reduce checkpoint latency by checkpointing the grid concurrently with other trailers.

- [#1515](https://github.com/tigerbeetle/tigerbeetle/pull/1515)

  Fix a logical race condition (which was caught by an assert) when reading and writing client
  replies concurrently.


- [#1522](https://github.com/tigerbeetle/tigerbeetle/pull/1522)

  Double check that both checksum and request number match between a request and the corresponding
  reply.

- [#1520](https://github.com/tigerbeetle/tigerbeetle/pull/1520)

  Optimize fields with zero value by not adding them to an index.

### Features

- [#1526](https://github.com/tigerbeetle/tigerbeetle/pull/1526),
  [#1531](https://github.com/tigerbeetle/tigerbeetle/pull/1531).

  Introduce `get_account_history` operation for querying the historical balances of a given account.

- [#1523](https://github.com/tigerbeetle/tigerbeetle/pull/1523)

  Add helper function for generating approximately monotonic IDs to various language clients.

### TigerTracks 🎧

- [Musique à Grande Vitesse](https://open.spotify.com/album/0pmrBIfqDn65p4FX9ubqXn?si=aLliiV5dSOeeId57jtaHhw)

## 2024-02-05

### Safety And Performance

- [#1489](https://github.com/tigerbeetle/tigerbeetle/pull/1489),
  [#1496](https://github.com/tigerbeetle/tigerbeetle/pull/1496),
  [#1501](https://github.com/tigerbeetle/tigerbeetle/pull/1501).

  Harden VSR against edge cases.

- [#1508](https://github.com/tigerbeetle/tigerbeetle/pull/1508),
  [#1509](https://github.com/tigerbeetle/tigerbeetle/pull/1509).

  Allows VSR to perform checkpoint steps concurrently to reduce latency spikes.

- [#1505](https://github.com/tigerbeetle/tigerbeetle/pull/1505)

  Removed unused indexes on account balances for a nice bump in throughput and lower memory usage.

- [#1512](https://github.com/tigerbeetle/tigerbeetle/pull/1512)

  Only zero-out the parts necessary for correctness of fresh storage buffers. "Defense in Depth"
  without sacrificing performance!

### Features

- [#1491](https://github.com/tigerbeetle/tigerbeetle/pull/1491),
  [#1503](https://github.com/tigerbeetle/tigerbeetle/pull/1503).

  TigerBeetle's [dev workbench](https://tigerbeetle.github.io/tigerbeetle/) now also tracks
  memory usage (RSS), throughput, and latency benchmarks over time!

### Internals

- [#1481](https://github.com/tigerbeetle/tigerbeetle/pull/1481),
  [#1493](https://github.com/tigerbeetle/tigerbeetle/pull/1493),
  [#1495](https://github.com/tigerbeetle/tigerbeetle/pull/1495),
  [#1498](https://github.com/tigerbeetle/tigerbeetle/pull/1498).

  Simplify assertions and tests for VSR and Replica.

- [#1497](https://github.com/tigerbeetle/tigerbeetle/pull/1497),
  [#1502](https://github.com/tigerbeetle/tigerbeetle/pull/1502),
  [#1504](https://github.com/tigerbeetle/tigerbeetle/pull/1504).

  .NET CI fixups

- [#1485](https://github.com/tigerbeetle/tigerbeetle/pull/1485),
  [#1499](https://github.com/tigerbeetle/tigerbeetle/pull/1499),
  [#1504](https://github.com/tigerbeetle/tigerbeetle/pull/1504).

  Spring Cleaning

### TigerTracks 🎧

- [Bone Dry](https://open.spotify.com/track/0adZjn5WV3b0BcZbvSi0y9)

## 2024-01-29

### Safety And Performance

- [#1446](https://github.com/tigerbeetle/tigerbeetle/pull/1446)

  Panic on checkpoint divergence. Previously, if a replica's state on disk diverged, we'd
  use state sync to bring it in line. Now, we don't allow any storage engine nondeterminism
  (mixed version clusters are forbidden) and panic if we encounter any.

- [#1476](https://github.com/tigerbeetle/tigerbeetle/pull/1476)

  Fix a liveness issues when starting a view across checkpoints in an idle cluster.

- [#1460](https://github.com/tigerbeetle/tigerbeetle/pull/1460)

  Stop an isolated replica from locking a standby out of a cluster.

### Features

- [#1470](https://github.com/tigerbeetle/tigerbeetle/pull/1470)

  Change `get_account_transfers` to use `timestamp_min` and `timestamp_max` to allow filtering by
  timestamp ranges.

- [#1463](https://github.com/tigerbeetle/tigerbeetle/pull/1463)

  Allow setting `--addresses=0` when starting TigerBeetle to enable a mode helpful for integration
  tests:
  * A free port will be picked automatically.
  * The port, and only the port, will be printed to stdout which will then be closed.
  * TigerBeetle will [exit when its stdin is closed](https://matklad.github.io/2023/10/11/unix-structured-concurrency.html).

- [#1402](https://github.com/tigerbeetle/tigerbeetle/pull/1402)

  TigerBeetle now has a [dev workbench](https://tigerbeetle.github.io/tigerbeetle/)! Currently we
  track our build times and executable size over time.

- [#1461](https://github.com/tigerbeetle/tigerbeetle/pull/1461)

  `tigerbeetle client ...` is now `tigerbeetle repl ...`.

### Internals

- [#1480](https://github.com/tigerbeetle/tigerbeetle/pull/1480)

  Deprecate support and testing for Node.js 16, which is EOL.

- [#1477](https://github.com/tigerbeetle/tigerbeetle/pull/1477),
  [#1469](https://github.com/tigerbeetle/tigerbeetle/pull/1469),
  [#1475](https://github.com/tigerbeetle/tigerbeetle/pull/1475),
  [#1457](https://github.com/tigerbeetle/tigerbeetle/pull/1457),
  [#1452](https://github.com/tigerbeetle/tigerbeetle/pull/1452).

  Improve VOPR & VSR logging, docs, assertions and tests.

- [#1474](https://github.com/tigerbeetle/tigerbeetle/pull/1474)

  Improve integration tests around Node.js and `pending_transfer_expired` - thanks to our friends at
  Rafiki for reporting!

### TigerTracks 🎧

- [Paint It, Black](https://www.youtube.com/watch?v=170sceOWWXc)

## 2024-01-22

### Safety And Performance

- [#1438](https://github.com/tigerbeetle/tigerbeetle/pull/1438)

  Avoid an extra copy of data when encoding the superblock during checkpoint.

- [#1429](https://github.com/tigerbeetle/tigerbeetle/pull/1429)

  Use more precise upper bounds for static memory allocation, reducing memory usage by about 200MiB.

- [#1439](https://github.com/tigerbeetle/tigerbeetle/pull/1439)

  When reading data past the end of the file, defensively zero-out the result buffer.

### Features

- [#1443](https://github.com/tigerbeetle/tigerbeetle/pull/1443)

  Upgrade C# client API to use `Span<T>`.

- [#1347](https://github.com/tigerbeetle/tigerbeetle/pull/1347)

  Add ID generation function to the Java client. TigerBeetle doesn't assign any meaning to IDs and
  can use anything as long as it is unique. However, for optimal performance it is best if these
  client-generated IDs are approximately monotonic. This can be achieved by, for example, using
  client's current timestamp for high order bits of an ID. The new helper does just that.

### Internals

- [#1437](https://github.com/tigerbeetle/tigerbeetle/pull/1437),
  [#1435](https://github.com/tigerbeetle/tigerbeetle/pull/1435),
  [d7c3f46](https://github.com/tigerbeetle/tigerbeetle/commit/d7c3f4654ea7c65b6d141be33dadd29e869c3984).

  Rewrite git history to remove large files accidentally added to the repository during early quick
  prototyping phase. To make this durable, add CI checks for unwanted files. The original history
  is available at:

  <https://github.com/tigerbeetle/tigerbeetle-history-archive>

- [#1421](https://github.com/tigerbeetle/tigerbeetle/pull/1421),
  [#1401](https://github.com/tigerbeetle/tigerbeetle/pull/1401).

  New tips for the style guide:

  - [write code top-down](https://www.teamten.com/lawrence/programming/write-code-top-down.html)
  - [pair up assertions](https://tigerbeetle.com/blog/2023-12-27-it-takes-two-to-contract)

### TigerTracks 🎧

- [Don't Take No For An Answer](https://youtu.be/BUDe0bJAHjY?si=_rdqeGRgRoA9HQnV)

## 2024-01-15

Welcome to 2024!

### Safety And Performance

- [#1425](https://github.com/tigerbeetle/tigerbeetle/pull/1425),
  [#1412](https://github.com/tigerbeetle/tigerbeetle/pull/1412),
  [#1410](https://github.com/tigerbeetle/tigerbeetle/pull/1410),
  [#1408](https://github.com/tigerbeetle/tigerbeetle/pull/1408),
  [#1395](https://github.com/tigerbeetle/tigerbeetle/pull/1395).

  Run more fuzzers directly in CI as a part of not rocket science package.

- [#1413](https://github.com/tigerbeetle/tigerbeetle/pull/1413)

   Formalize some ad-hoc testing practices as proper integration tests (that is, tests that interact
   with a `tigerbeetle` binary through IPC).

- [#1404](https://github.com/tigerbeetle/tigerbeetle/pull/1404)

   Add a lint check for unused Zig files.

- [#1390](https://github.com/tigerbeetle/tigerbeetle/pull/1390)

  Improve cluster availability by including conservative information about the current view into
  ping-pong messages. In particular, prevent the cluster from getting stuck when all replicas become
  primaries for different views.

- [#1365](https://github.com/tigerbeetle/tigerbeetle/pull/1365)

  Test both the latest and the oldest supported Java version on CI.

- [#1389](https://github.com/tigerbeetle/tigerbeetle/pull/1389)

  Fix a data race on close in the Java client.

### Features

- [#1403](https://github.com/tigerbeetle/tigerbeetle/pull/1403)

  Make binaries on Linux about six times smaller (12MiB -> 2MiB). Turns `tigerbeetle` was
  accidentally including 10 megabytes worth of debug info! Note that unfortunately stripping _all_
  debug info also prevents getting a nice stack trace in case of a crash. We are working on finding
  the minimum amount of debug information required to get _just_ the stack traces.

- [#1423](https://github.com/tigerbeetle/tigerbeetle/pull/1423),
  [#1426](https://github.com/tigerbeetle/tigerbeetle/pull/1426).

  Cleanup error handling API for Java client to never surface internal errors as checked exceptions.

- [#1405](https://github.com/tigerbeetle/tigerbeetle/pull/1405)

  Add example for setting up TigerBeetle as a systemd service.

- [#1400](https://github.com/tigerbeetle/tigerbeetle/pull/1400)

  Drop support for .NET Standard 2.1.

- [#1397](https://github.com/tigerbeetle/tigerbeetle/pull/1397)

  Don't exit repl on `help` command.

### Internals

- [#1422](https://github.com/tigerbeetle/tigerbeetle/pull/1422),
  [#1420](https://github.com/tigerbeetle/tigerbeetle/pull/1420),
  [#1417](https://github.com/tigerbeetle/tigerbeetle/pull/1417)

  Overhaul documentation-testing infrastructure to reduce code duplication.

- [#1398](https://github.com/tigerbeetle/tigerbeetle/pull/1398)

  Don't test Node.js client on platforms for which there are no simple upstream installation
  scripts.

- [#1388](https://github.com/tigerbeetle/tigerbeetle/pull/1388)

  Use histogram in the benchmark script to reduce memory usage.

### TigerTracks 🎧

- [Stripped](https://open.spotify.com/track/20BDMQu40KIUxUeFusq6eq)

## 2023-12-20

_“The exception confirms the rule in cases not excepted."_ ― Cicero.

Due to significant commits we had this last week, we decided to make an exception
in our release schedule and cut one more release in 2023!

Still, **the TigerBeetle team wishes everyone happy holidays!** 🎁

### Internals

- [#1362](https://github.com/tigerbeetle/tigerbeetle/pull/1362),
  [#1367](https://github.com/tigerbeetle/tigerbeetle/pull/1367),
  [#1374](https://github.com/tigerbeetle/tigerbeetle/pull/1374),
  [#1375](https://github.com/tigerbeetle/tigerbeetle/pull/1375)

  Some CI-related stuff plus the `-Drelease` flag, which will bring back the joy of
  using the compiler from the command line 🤓.

- [#1373](https://github.com/tigerbeetle/tigerbeetle/pull/1373)

  Added value count to `TableInfo`, allowing future optimizations for paced compaction.

### Safety And Performance

- [#1346](https://github.com/tigerbeetle/tigerbeetle/pull/1346)

  The simulator found a failure when the WAL gets corrupted near a checkpoint boundary, leading us
  to also consider scenarios where corrupted blocks in the grid end up "intersecting" with
  corruption in the WAL, making the state unrecoverable where it should be. We fixed it by
  extending the durability of "prepares", evicting them from the WAL only when there's a quorum of
  checkpoints covering this "prepare".

- [#1366](https://github.com/tigerbeetle/tigerbeetle/pull/1366)

  Fix a unit test that regressed after we changed an undesirable behavior that allowed `prefetch`
  to invoke its callback synchronously.

- [#1381](https://github.com/tigerbeetle/tigerbeetle/pull/1381)

  Relaxed a simulator's verification, allowing replicas of the core cluster to be missing some
  prepares, as long as they are from a past checkpoint.

### Features

- [#1054](https://github.com/tigerbeetle/tigerbeetle/pull/1054)

  A highly anticipated feature lands on TigerBeetle: it's now possible to retrieve the transfers
  involved with a given account by using the new operation `get_account_transfers`.

  Note that this feature itself is an ad-hoc API intended to be replaced once we have a proper
  Querying API. The real improvement of this PR is the implementation of range queries, enabling
  us to land exciting new features on the next releases.

- [#1368](https://github.com/tigerbeetle/tigerbeetle/pull/1368)

  Bump the client's maximum limit and the default value of `concurrency_max` to fully take
  advantage of the batching logic.

### TigerTracks 🎧

- [Everybody needs somebody](https://www.youtube.com/watch?v=m1M5Tc7eLCo)

## 2023-12-18

*As the last release of the year 2023, the TigerBeetle team wishes everyone happy holidays!* 🎁

### Internals

- [#1359](https://github.com/tigerbeetle/tigerbeetle/pull/1359)

  We've established a rotation between the team for handling releases. As the one writing these
  release notes, I am now quite aware.

- [#1357](https://github.com/tigerbeetle/tigerbeetle/pull/1357)

  Fix panic in JVM unit test on Java 21. We test JNI functions even if they're not used by the Java
  client and the semantics have changed a bit since Java 11.

- [#1351](https://github.com/tigerbeetle/tigerbeetle/pull/1351),
  [#1356](https://github.com/tigerbeetle/tigerbeetle/pull/1356),
  [#1360](https://github.com/tigerbeetle/tigerbeetle/pull/1360)

  Move client sessions from the Superblock (database metadata) into the Grid (general storage). This
  simplifies control flow for various sub-components like Superblock checkpointing and Replica state
  sync.

### Safety And Performance

- [#1352](https://github.com/tigerbeetle/tigerbeetle/pull/1352)

  An optimization for removes on secondary indexes makes a return. Now tombstone values in the LSM
  can avoid being compacted all the way down to the lowest level if they can be cancelled out by
  inserts.

- [#1257](https://github.com/tigerbeetle/tigerbeetle/pull/1257)

  Clients automatically batch pending similar requests 🎉! If a tigerbeetle client submits a
  request, and one with the same operation is currently in-flight, they will be grouped and
  processed together where possible (currently, only for `CreateAccount` and `CreateTransfers`).
  This should [greatly improve the performance](https://github.com/tigerbeetle/tigerbeetle/pull/1257#issuecomment-1812648270)
  of workloads which submit a single operation at a time.

### TigerTracks 🎧

- [Carouselambra](https://open.spotify.com/track/0YZKbKo9i91i7LD0m1KASq)

## 2023-12-11

### Safety And Performance

- [#1339](https://github.com/tigerbeetle/tigerbeetle/pull/1339)

  Defense in depth: add checkpoint ID to prepare messages. Checkpoint ID is a hash that covers, via
  hash chaining, the entire state stored in the data file. Verifying that checkpoint IDs match
  provides a direct strong cryptographic guarantee that the state is the same across replicas, on
  top of existing guarantee that the sequence of events leading to the state is identical.

### Internals

- [#1343](https://github.com/tigerbeetle/tigerbeetle/pull/1343),
  [#1341](https://github.com/tigerbeetle/tigerbeetle/pull/1341),
  [#1340](https://github.com/tigerbeetle/tigerbeetle/pull/1340)

  Gate the main branch on more checks: unit-tests for Node.js and even more fuzzers.

- [#1332](https://github.com/tigerbeetle/tigerbeetle/pull/1332),
  [#1348](https://github.com/tigerbeetle/tigerbeetle/pull/1348)

  Code cleanups after removal of storage size limit.

### TigerTracks 🎧

- [Concrete Reservation](https://open.spotify.com/track/1Li9HBLXG2LJSeD4fEhtcd?si=64611215922a4436)

## 2023-12-04

### Safety And Performance

- [#1330](https://github.com/tigerbeetle/tigerbeetle/pull/1330),
  [#1319](https://github.com/tigerbeetle/tigerbeetle/pull/1319)

  Fix free set index. The free set is a bitset of free blocks in the grid. To speed up block
  allocation, the free set also maintains an index --- a coarser-grained bitset where a single bit
  corresponds to 1024 blocks. Maintaining consistency between a data structure and its index is
  hard, and thorough assertions are crucial. When moving free set to the grid, we discovered that,
  in fact, we don't have enough assertions in this area and, as a result, even have a bug!
  Assertions added, bug removed!

- [#1323](https://github.com/tigerbeetle/tigerbeetle/pull/1323),
  [#1336](https://github.com/tigerbeetle/tigerbeetle/pull/1336),
  [#1324](https://github.com/tigerbeetle/tigerbeetle/pull/1324)

  LSM tree fuzzer found a couple of bugs in its own code.

### Features

- [#1331](https://github.com/tigerbeetle/tigerbeetle/pull/1331),
  [#1322](https://github.com/tigerbeetle/tigerbeetle/pull/1322),
  [#1328](https://github.com/tigerbeetle/tigerbeetle/pull/1328)

  Remove format-time limit on the size of the data file. Before, the maximum size of the data file
  affected the layout of the superblock, and there wasn't any good way to increase this limit, short
  of recreating the cluster from scratch. Now, this limit only applies to the in-memory data
  structures: when a data files grows large, it is sufficient to just restart its replica with a
  larger amount of RAM.

- [#1321](https://github.com/tigerbeetle/tigerbeetle/pull/1321).

  We finally have the "installation" page in our docs!

### Internals

- [#1334](https://github.com/tigerbeetle/tigerbeetle/pull/1334)

  Use Zig's new `if (@inComptime())` builtin to compute checksum of an empty byte slice at compile
  time.

- [#1315](https://github.com/tigerbeetle/tigerbeetle/pull/1315)

  Fix unit tests for the Go client and add them to
  [not rocket science](https://graydon2.dreamwidth.org/1597.html)
  set of checks.

### TigerTracks 🎧

- [Times Like These](https://www.youtube.com/watch?v=cvCUXXsP5WE)

## 2023-11-27

### Internals

- [#1306](https://github.com/tigerbeetle/tigerbeetle/pull/1306),
  [#1308](https://github.com/tigerbeetle/tigerbeetle/pull/1308)

  When validating our releases, use the `release` branch instead of `main` to ensure everything is
  in sync, and give the Java validation some retry logic to allow for delays in publishing to
  Central.

- [#1310](https://github.com/tigerbeetle/tigerbeetle/pull/1310)

  Pad storage checksums from 128-bit to 256-bit. These are currently unused, but we're reserving
  the space for AEAD tags in future.

- [#1312](https://github.com/tigerbeetle/tigerbeetle/pull/1312)

  Remove a trailing comma in our Java client sample code.

- [#1313](https://github.com/tigerbeetle/tigerbeetle/pull/1313)

  Switch `bootstrap.sh` to use spaces only for indentation and ensure it's checked by our
  shellcheck lint.

- [#1314](https://github.com/tigerbeetle/tigerbeetle/pull/1314)

  Update our `DESIGN.md` to better reflect storage fault probabilities and add in a reference.

- [#1316](https://github.com/tigerbeetle/tigerbeetle/pull/1316)

  Add `CHANGELOG.md` validation to our tidy lint script. We now check line length limits and
  trailing whitespace.

- [#1317](https://github.com/tigerbeetle/tigerbeetle/pull/1317)

  In keeping with TigerStyle rename `reserved_nonce` to `nonce_reserved`.

- [#1318](https://github.com/tigerbeetle/tigerbeetle/pull/1318)

  Note in TigerStyle that callbacks go last in the list of parameters.

- [#1325](https://github.com/tigerbeetle/tigerbeetle/pull/1325)

  Add an exception for line length limits if there's a link in said line.

### TigerTracks 🎧

- [Space Trash](https://www.youtube.com/watch?v=tmcVAJd87Wk)

## 2023-11-20

### Safety And Performance

- [#1300](https://github.com/tigerbeetle/tigerbeetle/pull/1300)

  Recursively check for padding in structs used for data serialization, ensuring that no
  uninitialized bytes can be stored or transmitted over the network. Previously, we checked only
  if the struct had no padding, but not its fields.

### Internals

- [#1299](https://github.com/tigerbeetle/tigerbeetle/pull/1299)

  Minor adjustments in the release process, making it easier to track updates in the documentation
  website when a new version is released, even if there are no changes in the documentation itself.

- [#1301](https://github.com/tigerbeetle/tigerbeetle/pull/1301)

  Fix outdated documentation regarding 128-bit balances.

- [#1302](https://github.com/tigerbeetle/tigerbeetle/pull/1302)

  Fix a [bug](https://github.com/tigerbeetle/tigerbeetle/issues/1290) discovered and reported
  during the [Hackathon 2023](https://github.com/tigerbeetle/hackathon-2023), where the Node.js
  client's error messages were truncated due to an incorrect string concatenation adding a null
  byte `0x00` in the middle of the string.

- [#1291](https://github.com/tigerbeetle/tigerbeetle/pull/1291)

  Update the Node.js samples instructions, guiding the user to install all dependencies before
  the sample project.

- [#1295](https://github.com/tigerbeetle/tigerbeetle/pull/1295)

  We've doubled the `Header`s size to 256 bytes, paving the way for future improvements that will
  require extra space. Concurrently, this change also refactors a great deal of code.
  Some of the `Header`'s fields are shared by all messages, however, each `Command` also requires
  specific pieces of information that are only used by its kind of message, and it was necessary to
  repurpose and reinterpret fields so that the same header could hold different data depending on
  the context. Now, commands have their own specialized data type containing the fields that are
  only pertinent to the context, making the API much safer and intent-clear.

- [#1304](https://github.com/tigerbeetle/tigerbeetle/pull/1304)

  With larger headers (see #1295) we have enough room to make the cluster ID a 128-bit integer,
  allowing operators to generate random cluster IDs without the cost of having a centralized ID
  coordinator. Also updates the documentation and sample programs to reflect the new maximum batch
  size, which was reduced from 8191 to 8190 items after we doubled the header.

### TigerTracks 🎧

- [She smiled sweetly](https://www.youtube.com/watch?v=fB1EpEFz6Lg)

## 2023-11-13

### Safety And Performance

- [#1264](https://github.com/tigerbeetle/tigerbeetle/pull/1264)

  Implement last-mile release artifact verification in CI.

- [#1268](https://github.com/tigerbeetle/tigerbeetle/pull/1268)

  Bump the simulator's safety phase max-ticks to avoid false positives from the liveness check.

- [#1270](https://github.com/tigerbeetle/tigerbeetle/pull/1270)

  Fix a crash caused by a race between a commit and a repair acquiring a client-reply `Write`.

- [#1278](https://github.com/tigerbeetle/tigerbeetle/pull/1278)

  Fix a crash caused by a race between state (table) sync and a move-table compaction.

  Both bugs didn't stand a chance in the [Line of Fire](https://www.youtube.com/watch?v=pq-G3EWO9XM)
  of our deterministic simulator!

### Internals

- [#1244](https://github.com/tigerbeetle/tigerbeetle/pull/1244)

  Specify which CPU features are supported in builds.

- [#1275](https://github.com/tigerbeetle/tigerbeetle/pull/1275)

  Improve `shell.zig`'s directory handling, to guard against mistakes with respect to the current
  working directory.

- [#1277](https://github.com/tigerbeetle/tigerbeetle/pull/1277)

  Interpret a git hash as a VOPR seed, to enable reproducible simulator smoke tests in CI.

- [#1288](https://github.com/tigerbeetle/tigerbeetle/pull/1288)

  Explicitly target glibc 2.7 when building client libraries, to make sure TigerBeetle clients are
  compatible with older distributions.

## 2023-11-06

### Safety And Performance

- [#1263](https://github.com/tigerbeetle/tigerbeetle/pull/1263)

  Revive the TigerBeetle [VOPRHub](https://github.com/tigerbeetle-vopr)! Some previous changes left
  it on it's [Last Stand](https://open.spotify.com/track/1ibHApXtb0pgplmNDRLHrJ), but the bot is
  back in business finding liveness bugs:
  [#1266](https://github.com/tigerbeetle/tigerbeetle/issues/1266)

### Features

- [#1260](https://github.com/tigerbeetle/tigerbeetle/pull/1260)

  Set the latest Docker image to track the latest release. Avoids language clients going out of sync
  with your default docker replica installations.

### Internals

- [#1261](https://github.com/tigerbeetle/tigerbeetle/pull/1261)

  Move website doc generation for https://docs.tigerbeetle.com/ into the main repo.

- [#1265](https://github.com/tigerbeetle/tigerbeetle/pull/1265),
  [#1243](https://github.com/tigerbeetle/tigerbeetle/pull/1243)

  Addressed some release quirks with the .NET and Go client builds.

## 2023-10-30

### Safety And Performance

- [#1251](https://github.com/tigerbeetle/tigerbeetle/pull/1251)

  Prove a tighter upper bound for the size of manifest log. With this new bound, manifest log is
  guaranteed to fit in allocated memory and is smaller. Additionally, manifest log compaction is
  paced depending on the current length of the log, balancing throughput and time-to-recovery.

- [#1198](https://github.com/tigerbeetle/tigerbeetle/pull/1198)

  Recommend using [ULID](https://github.com/ulid/spec) for event IDs. ULIDs are approximately
  sorted, which significantly improves common-case performance.

### Internals

- [#1218](https://github.com/tigerbeetle/tigerbeetle/pull/1218)

  Rewrite Node.js client implementation to use the common C client underneath. While clients for
  other languages already use the underlying C library, the Node.js client duplicated some code for
  historical reasons, but now we can leave that duplication in the past. [This Is A
  Photograph](https://www.youtube.com/watch?v=X0i7whWLW8M).

## 2023-10-25

### Safety And Performance

- [#1240](https://github.com/tigerbeetle/tigerbeetle/pull/1240)

  Increase block size to reduce latencies due to compaction work. Today, we use a simplistic
  schedule for compaction, which causes latency spikes at the end of the bar. While the future
  solution will implement a smarter compaction pacing to distribute the work more evenly, we can
  get a quick win by tweaking the block and the bar size, which naturally evens out latency spikes.

- [#1246](https://github.com/tigerbeetle/tigerbeetle/pull/1246)

  The new release process changed the names of the published artifacts (the version is no longer
  included in the name). This broke our quick start scripts, which we have fixed. Note that we are
  in the process of rolling out the new release process, so some unexpected breakage is expected.

- [#1239](https://github.com/tigerbeetle/tigerbeetle/pull/1239),
  [#1243](https://github.com/tigerbeetle/tigerbeetle/pull/1243)

  Speed up secondary index maintenance by statically distinguishing between insertions and
  updates. [Faster than the speed of night!](https://open.spotify.com/track/30oZqbcUROFLSru3WcN3bx)

### Features

- [#1245](https://github.com/tigerbeetle/tigerbeetle/pull/1245)

  Include Docker images in the release.

### Internals

- [#1234](https://github.com/tigerbeetle/tigerbeetle/pull/1234)

  Simplify superblock layout by using a linked list of blocks for manifest log, so that the
  superblock needs to store only two block references.

  P.S. Note the PR number!

## 2023-10-23

This is the start of the changelog. A lot happened before this point and is lost in the mist of git
history, but any notable change from this point on shall be captured by this document.

### Safety And Performance

- [#1225](https://github.com/tigerbeetle/tigerbeetle/pull/1225)

  Remove bloom filters. TigerBeetle implements more targeted optimizations for
  both positive and negative lookups, making bloom filters a net loss.

### Features

- [#1228](https://github.com/tigerbeetle/tigerbeetle/pull/1228)

  Increase alignment of data blocks to 128KiB (from 512 bytes). Larger alignment gives operators
  better control over physical layout of data on disk.

### Internals

- [#1201](https://github.com/tigerbeetle/tigerbeetle/pull/1201),
  [#1232](https://github.com/tigerbeetle/tigerbeetle/pull/1232)

  Overhaul of CI and release infrastructure. CI and releases are now driven by Zig code. The main
  branch is gated on integration tests for all clients.

  This is done in preparation for the first TigerBeetle release.

## Prehistory

For archeological inquiries, check out the state of the repository at the time of the first
changelog:

[https://github.com/tigerbeetle/tigerbeetle/](
https://github.com/tigerbeetle/tigerbeetle/tree/d2d6484188ecc57680e8bde446b5d09b6f2d83ca)
