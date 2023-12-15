---
sidebar_position: 4
---

# State Sync

State sync synchronizes the state of a lagging/divergent replica with the healthy cluster.

State sync is used when when a lagging replica's log no longer intersects with the cluster's current log —
[WAL repair](./vsr.md#protocol-repair-wal) cannot catch the replica up.

(VRR refers to state sync as "state transfer", but we already have [transfers](../reference/transfers.md) elsewhere.)

In the context of state sync, "state" refers to:
1. the superblock `vsr_state.checkpoint`
2. the grid (manifest, free set, and client sessions blocks)
3. the grid (LSM table data; acquired blocks only)
4. client replies

State sync consists of four protocols:
- [Sync Superblock](./vsr.md#protocol-sync-superblock) (syncs 1)
- [Repair Grid](./vsr.md#protocol-repair-grid) (syncs 2)
- [Sync Forest](./vsr.md#protocol-sync-forest) (syncs 3)
- [Sync Client Replies](./vsr.md#protocol-sync-client-replies) (syncs 4)

The target of superblock-sync is the latest checkpoint of the healthy cluster.
When we catch up to the latest checkpoint (or very close to it), then we can transition back to a healthy state.

## Glossary

Replica roles:

- _syncing replica_: A replica performing superblock-sync. (Any step within *1*-*10* of the [sync algorithm](#algorithm))
- _healthy replica_: A replica _not_ performing superblock-sync — part of the active cluster.
- _divergent replica_: A replica with a checkpoint that is (and can never be) canonical.

Checkpoints:

- [_checkpoint id_/_checkpoint identifier_](#checkpoint-identifier): Uniquely identifies a particular checkpoint reproducibly across replicas.
- [_canonical checkpoint_](#canonical-checkpoint): Any checkpoint which either: _A_: has any ops committed atop it by the primary, or _B_: a majority quorum has reached.
- [_sync target_](#sync-target): The checkpoint identifier of the target of superblock-sync. Every sync target is a canonical checkpoint.

## Algorithm

0. [Sync is needed](#0-scenarios).
1. [Trigger sync](#1-triggers).
2. Wait for non-grid commit operation to finish.
3. Wait for grid IO to finish. (See `Grid.cancel()`.)
4. Wait for a usable sync target to arrive. (Usually we already have one.)
5. Begin [sync-superblock protocol](./vsr.md#protocol-sync-superblock).
6. [Request superblock checkpoint state](#6-request-superblock-checkpoint-state).
7. Update the superblock headers with:
    - Bump `vsr_state.checkpoint.commit_min`/`vsr_state.checkpoint.commit_min_checksum` to the sync target op/op-checksum.
    - Bump `vsr_state.checkpoint.previous_checkpoint_id` to the checkpoint id that is previous to our sync target (i.e. it isn't _our_ previous checkpoint).
    - Bump `replica.commit_min`. (If `replica.commit_min` exceeds `replica.op`, transition to `status=recovering_head`).
    - Set `vsr_state.sync_op_min` to the minimum op which has not been repaired.
    - Set `vsr_state.sync_op_max` to the maximum op which has not been repaired.
8. Sync-superblock protocol is done.
9. Repair [replies](./vsr.md#protocol-sync-client-replies), [free set, client sessions, and manifest blocks](./vsr.md#protocol-repair-grid), and [table blocks](./vsr.md#protocol-sync-forest) that were created within the `sync_op_{min,max}` range.
10. Update the superblock with:
    - Set `vsr_state.sync_op_min = 0`
    - Set `vsr_state.sync_op_max = 0`

If a newer sync target is discovered during steps *5*-*6* or *9*, go to step *4*.

If the replica starts up with `vsr_state.sync_op_max ≠ 0`, go to step *9*.

### 0: Scenarios

Scenarios requiring state sync:

1. A replica was down/partitioned/slow for a while and the rest of the cluster moved on.
  The lagging replica is too far behind to catch up via WAL repair.
2. A replica was just formatted and is being added to the cluster (i.e. via [reconfiguration](./vsr.md#protocol-reconfiguration)).
  The new replica is too far behind to catch up via WAL repair.
3. A replica's state diverged from the cluster ([storage nondeterminism](#storage-determinism)).

Causes of number 3:
- A storage determinism bug.
- An upgraded replica (e.g. a canary) running a different version of the code from the remainder of the cluster, which unexpectedly changes its history.
  (The change either has a bug or should have been gated behind a feature flag.)

### 1: Triggers

State sync is initially triggered by any of the following:

- The replica discovers the canonical checkpoint for its current wrap, and that it [doesn't match](#storage-determinism) its own current checkpoint.
- The replica receives a SV which indicates that it has lagged so far behind the cluster that its log cannot possibly intersect.
- `repair_sync_timeout` fires, and:
    - a WAL or grid repair is in progress and,
    - the replica's checkpoint is lagging behind the cluster's (far enough that the repair may never complete).

### 6: Request Superblock Checkpoint State

The syncing replica sends `command=request_sync_checkpoint` messages (with the sync target identifier attached to each) until it receives a `command=sync_checkpoint` with a matching checkpoint identifier.

## Concepts
### Syncing Replica

Syncing replicas may:

- [write prepares to their WAL](#syncing-replicas-write-prepares-to-their-wal)
- assist with grid repair
- join new views
- send a `do_view_change`

Syncing replicas must not:

- [ack](#syncing-replicas-dont-ack-prepares)
- commit prepares
- be a primary

#### Syncing Replicas write prepares to their WAL.

When the replica completes superblock-sync, an up-to-date WAL and journal allow it to quickly catch up (i.e. commit) to the current cluster state.

#### Syncing Replicas don't ack prepares.

If syncing replicas _did_ ack prepares:

Consider a cluster of 3 replicas:
- the _primary_,
- a _normal backup_, and
- a _syncing backup_.

1. _Primary_ prepares many ops...
2. _Syncing backup_ prepares and acknowledges all of those messages.
3. _Normal backup_ is partitioned — its not seeing any of these prepares.
4. _Primary_ is receiving `prepare_ok`s from the _syncing backup_, so it is committing.
5. _Primary_ eventually checkpoints.
6. (This cycle repeats — _primary_ keeps preparing/committing, _syncing backup_ keeps preparing, and _normal backup_ is still partitioned.)

But now _primary_ is so far ahead that the _normal backup_ needs to sync!
Having 2/3 replicas syncing means that a single grid-block corruption on the primary could make the cluster permanently unavailable.

### Checkpoint Identifier

A _checkpoint id_ is a hash of the superblock `CheckpointState`.

A checkpoint identifier is attached to the following message types:
- `command=commit`: Current checkpoint identifier of sender.
- `command=ping`: Current checkpoint identifier of sender.
- `command=prepare`: The attached checkpoint id is the checkpoint id during which the corresponding prepare was originally prepared.
- `command=prepare_ok`: The attached checkpoint id is the checkpoint id during which the corresponding prepare was originally prepared.
- `command=request_sync_checkpoint`: Requested checkpoint identifier.
- `command=sync_checkpoint`: Current checkpoint identifier of sender.

### Canonical Checkpoint

A _canonical_ checkpoint is a checkpoint:
1. with an op committed atop it by the primary (discovery via `command=commit`), or
2. that a majority quorum of replicas have reached (discovery via `command=ping`), or
3. (when `R=2`: that a single replica has reached).

The primary ignores `command=prepare`s which have a different checkpoint id attached than they expect.
This means that if a replica's history diverges (due to nondeterminism), the diverging replica is effectively excluded from participating in consensus until it has performed superblock-sync.
See [Storage Determinism](#storage-determinism).

This bounds the "distance" that a history can diverge by.
No replica can diverge from the canonical history by more than one checkpoint.
Every checkpoint's previous (i.e. parent) checkpoint is canonical.

### Sync Target

A _sync target_ is the [checkpoint identifier](#checkpoint-identifier) of the checkpoint that the superblock-syn is syncing towards.

Not all checkpoint identifiers are valid sync targets.

- Every sync target **must** be a [canonical checkpoint](#canonical-checkpoint).
  (TODO: Once prepares include the checkpoint identifier, this requirement can be removed.)
- Every sync target op **must** either:
  - be greater-than-or-equal-to the replica's current checkpoint op.
  - be equal to the replica's current checkpoint op, but with a different target id (if our checkpoint diverged).

### Storage Determinism

When everything works, storage is deterministic.
But we must tolerate non-determinism too, in case of bugs.

At the limit, all 6 replicas could diverge.
This puts the cluster in a very precarious position:
All it would take is a single corruption to be permanently unavailable!
(We need to pick one "winner", and we can't just read all of every single replica's disks to find which (if any) is completely intact).

So we must bound nondeterminism within our fault model.

We require that at least [`quorum_replication`](./vsr.md#quorums) histories are identical.
[Canonical Checkpoint](#canonical-checkpoint) describes how this can (in part, though not completely) be enforced automatically.
If more histories diverge, the cluster will be unavailable (unable to commit), and require operator intervention to recover (e.g. by cloning data files).

### Progress Tracking

If a state-syncing replica crashes before completing sync, we don't want to restart from scratch.
(This is mainly important for tables — the manifest is smaller.)

Progress is tracked implicitly: If a table index block is present on disk, we implicitly assume that all of its data blocks have already been written too.
That is, "table index block in grid" implies "table's referenced data blocks are in grid".

To enforce this invariant:
1. When syncing table blocks, don't write an index block until all of its data blocks are written.
2. A history cannot diverge from the canonical history by more than one checkpoint.
3. A replica never syncs towards a checkpoint from its past.
