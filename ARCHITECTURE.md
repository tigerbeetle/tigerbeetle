LMAX ARCHITECTURE:

1. We receive an incoming batch of prepares (1000 prepares is optimal for write throughput, nice 64kb writes).
2. We checksum and write the batch of prepares to a journal on disk and then fsync.
3. We then run business logic and apply the batch to in-memory state.
4. We ACK the entire batch back to Mojaloop.
5. Out of the critical path, we periodically flush a snapshot of in-memory state to disk and fsync.
6. At startup, we replay the journal after latest snapshot.

We don't depend on snapshots, only the journal. The snaphots just speed up our startup time.
