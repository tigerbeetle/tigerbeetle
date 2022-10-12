# Tree
## Compaction

Tree compaction runs to the sound of music!

Compacting LSM trees involves merging and moving tables into the next levels as needed.
To avoid write amplification stalls and bound latency, compaction is done incrementally.

A full compaction phase is denoted as a bar or measure, using terms from music notation.
Each bar consists of `lsm_batch_multiple` beats or "compaction ticks" of work.
A compaction beat is started asynchronously with `compact()` which takes a callback and
the op that was just committed.

A bar is split in half according to the "first" down beat and "middle" down beat.
The first half of the bar compacts even levels while the latter compacts odd levels.
Mutable table changes are sorted and compacted into the immutable table.
The immutable table is compacted into level 0 during the odd level half of the bar.

At any given point, there are at most levels/2 compactions running concurrently.
The source level is denoted as `level_a` and the target level as `level_b`.
The last level in the LSM tree has no target level so it is never a source level.

Assuming a bar/`lsm_batch_multiple` of 4, the invariants can be described as follows:

* At the end of every beat, there is space in mutable table for the next beat.
* The manifest info for the compaction's input tables is updated at the end of the half-bar.
* The manifest info for the compaction's output tables is updated during the compaction.
* The manifest is compacted at the end of every beat.
* The compactions' output tables are not visible until the compaction has finished.

1. (first) down beat of the bar:
    * Assert no compactions are currently running.
    * Allow level visible table counts to overflow if needed.
    * Start even level compactions if there's any tables to compact.

2. (second) up beat of the bar:
    * Finish ticking running even-level compactions.
    * Assert on callback completion that all compactions are complete.

3. (third) down beat of the bar:
    * Assert no compactions are currently running.
    * Start odd level compactions if there are any tables to compact, and only if we must.
    * Compact the immutable table if it contains any sorted values (it could be empty).

4. (fourth) last beat of the bar:
    * Finish ticking running odd-level and immutable table compactions.
    * Assert on callback completion that all compactions are complete.
    * Assert on callback completion that no level's visible table count overflows.
    * Flush, clear, and sort mutable table values into immutable table for next bar.


## Snapshots
### Snapshots and Compaction

Consider the half-bar compaction beginning at op=`X` (`12`), with `lsm_batch_multiple=M` (`8`).
Each half-bar contains `N=M/2` (`4`) beats. The next half-bar begins at `Y=X+N` (`16`).

During the compaction `X` (op=`X…Y-1`; `12…15`), each commit prefetches from the snapshot equal to
the first op of the compaction. As shown, they continue to query the old (input) tables.

During the compaction `X`:
- `snapshot_max` of each input table is truncated to `Y-1` (`15`).
- `snapshot_min` of each output table is initialized to `Y` (`16`).

```
  0   4   8  12  16  20  24  (op, snapshot)
  ┼───┬───┼───┬───┼───┬───┼
              ####
  ····────────X────────····  (input  tables, before compaction)
  ····────────────           (input  tables,  after compaction)
                  Y────····  (output tables,  after compaction)
```

Beginning from the next op after the compaction (`Y`; `16`):
- The output tables of the above compaction `X` are visible.
- The input tables of the above compaction `X` are invisible.
- Therefore, it will lookup from the output tables, but ignore the input tables.
- Commits must not prefetch from the output tables of `X` before the compaction half-bar
  has finished, since those tables are incomplete.

At this point the input tables can be removed if they are invisible to all persistent snapshots.


### Snapshot Values

The on-disk tables visible to a snapshot `B` do not contain the updates from the commit with op `B`.

Consider the following diagram (`lsm_batch_multiple=8`):

```
  0   4   8  12  16  20  24  28  (op, snapshot)
  ┼───┬───┼───┬───┼───┬───┼───┬
          ,,,,,,,,........
          ↑A      ↑B      ↑C
```

Compaction is driven by the commits of ops `B→C` (`16…23`):
- Updates from ops `A→B` (`8…15`) are in the immutable table.
- Updates from ops `B→C` (`16…23`) are added to the mutable table (by the respective commit).
- `lookup_snapshot_max` is `B` (`16`).
  (Lookups against this snapshot read from the mutable and immutable tables (`A→C`; `8…23`) —
  that is, each commit can see all previous commits' updates.)

At the end of the last beat of the compaction bar (`23`):
- Updates from ops `A→B` (`8…15`) are on disk (level 0).
- Updates from ops `B→C` (`16…23`) are moved from the mutable table to the immutable table.
- `lookup_snapshot_max` is `C` (`24`).

### Snapshot Coordination

Because the `Forest` and `Groove` coordinate `compact()` calls across all trees, snapshots are coordinated across all trees.
