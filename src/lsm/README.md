# Glossary

- _bar_/_measure_: `lsm_batch_multiple` beats; unit of incremental compaction.
- _beat_: `op % lsm_batch_multiple`; Single step of an incremental compaction.
- _groove_: A collection of LSM trees, storing objects and their indices.
- _immutable table_: in-memory table; one per tree. Used to periodically flush the mutable table to
  disk.
- _level_: Between `0` and `lsm_levels - 1` (usually `lsm_levels = 7`).
- _forest_: a collection of grooves.
- _manifest_: index of table and level metadata; one per tree.
- _mutable table_: in-memory table; one per tree. All tree updates are applied only to this table.
- _snapshot_: sequence number which selects the queryable partition of on-disk tables.

# Tree
## Tables

A tree is a hierarchy of in-memory and on-disk tables. There are three categories of tables:

- The [mutable table](table_mutable.zig) is an in-memory table.
  - Each tree has a single mutable table.
  - All tree updates, inserts, and removes are applied to the mutable table.
  - The mutable table's size is allocated to accommodate a full bar of updates.
- The [immutable table](table_immutable.zig) is an in-memory table.
  - Each tree has a single immutable table.
  - The mutable table's contents are periodically moved to the immutable table,
    where they are stored while being flushed to level `0`.
- Level `0` … level `config.lsm_levels - 1` each contain an exponentially increasing number of
  on-disk tables.
  - Each tree has as many as `config.lsm_growth_factor ^ (level + 1)` tables per level.
    (`config.lsm_growth_factor` is typically 8).
  - Within a given level and snapshot, the tables' key ranges are [disjoint](manifest_level.zig).

## Compaction

Tree compaction runs to the sound of music!

Compacting LSM trees involves merging and moving tables into the next levels as needed.
To avoid write amplification stalls and bound latency, compaction is done incrementally.

A full compaction phase is denoted as a bar or measure, using terms from music notation.
Each bar consists of `lsm_batch_multiple` beats or "compaction ticks" of work.
A compaction tick executes asynchronously immediately after every commit, with
`beat = commit.op % lsm_batch_multiple`.

A bar is split in half according to the "first" beat and "middle" beat.
The first half of the bar compacts even levels while the latter compacts odd levels.
Mutable table changes are sorted and compacted into the immutable table.
The immutable table is compacted into level 0 during the odd level half of the bar.

At any given point, there are at most `levels/2` compactions running concurrently.
The source level is denoted as `level_a` and the target level as `level_b`.
The last level in the LSM tree has no target level so it is never a source level.
Each compaction compacts a [single table](#table-selection) from `level_a` into all tables in
`level_b` which intersect the `level_a` table's key range.

Invariants:
* At the end of every beat, there is space in mutable table for the next beat.
* The manifest is compacted at the end of every beat.
* The compactions' output tables are not [visible](#snapshots-and-compaction) until the compaction has finished.

1. First half-bar, first beat ("first beat"):
    * Assert no compactions are currently running.
    * Allow the per-level table limits to overflow if needed (for example, if we may compact a table
      from level `A` to level `B`, where level `B` is already full).
    * Start compactions from even levels that have reached their table limit.

2. First half-bar, last beat:
    * Finish ticking any incomplete even-level compactions.
    * Assert on callback completion that all compactions are complete.

3. Second half-bar, first beat ("middle beat"):
    * Assert no compactions are currently running.
    * Start odd level compactions if there are any tables to compact.
    * Start compactions from odd levels that have reached their table limit.
    * Compact the immutable table if it contains any sorted values (it might be empty).

4. Second half-bar, last beat:
    * Finish ticking any incomplete odd-level and immutable table compactions.
    * Assert on callback completion that all compactions are complete.
    * Assert on callback completion that no level's table count overflows.
    * Flush, clear, and sort mutable table values into immutable table for next bar.
    * Remove input tables that are invisible to all current and persisted snapshots.

### Compaction Selection Policy

Compaction targets the table from level `A` which overlaps the fewest tables of level `B`.

For example, in the following table (with `lsm_growth_factor=2`), each table is depicted as the range of keys it includes. The tables with uppercase letters would be chosen for compaction next.

```
Level 0   A─────────────H       l───────────────────────────z
Level 1   a───────e             L─M   o───────s   u───────y
Level 2     b───d e─────h i───k l───n o─p q───s   u─v w─────z
(Keys)    a b c d e f g h i j k l m n o p q r s t u v w x y z
```

Links:
- [`Manifest.compaction_table`](manifest.zig)
- [Constructing and Analyzing the LSM Compaction Design Space](http://vldb.org/pvldb/vol14/p2216-sarkar.pdf) describes the tradeoffs of various data movement policies. TigerBeetle implements the "least overlapping with parent" policy.
- [Option of Compaction Priority](https://rocksdb.org/blog/2016/01/29/compaction_pri.html)

## Snapshots

Each table has a minimum and maximum integer snapshot (`snapshot_min` and `snapshot_max`).

Each query targets a particular snapshot. A table `T` is "visible" to a snapshot `S` when

```
T.snapshot_min ≤ S ≤ T.snapshot_max
```

and is "invisible" to the snapshot otherwise.

Compaction does not modify tables in place — it copies data. Snapshots control and distinguish which copies are useful, and which can be deleted. Snapshots can also be persisted, enabling queries against past states of the tree (unimplemented; future work).

### Snapshots and Compaction

Consider the half-bar compaction beginning at op=`X` (`12`), with `lsm_batch_multiple=M` (`8`).
Each half-bar contains `N=M/2` (`4`) beats. The next half-bar begins at `Y=X+N` (`16`).

During the half-bar compaction `X` (op=`X…Y-1`; `12…15`), each commit prefetches from the snapshot
equal to the first op of the compaction. As shown, they continue to query the old (input) tables.

During the half-bar compaction `X`:
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
- Callers must not query from the output tables of `X` before the compaction half-bar has finished
  (i.e. before the end of beat `Y-1` (`15`)), since those tables are incomplete.

At this point the input tables can be removed if they are invisible to all persistent snapshots.

### Snapshot Queries

Each tree tracks the highest snapshot safe to query from (`tree.lookup_snapshot_max`), to ensure that
an ongoing compaction's incomplete output tables are not visible. Queries targeting
`tree.lookup_snapshot_max` always read from the mutable and immutable tables — so each commit can
see all previous commits' updates.)

TODO(Persistent Snapshots): Expand this section.

### Snapshot Values

The on-disk tables visible to a snapshot `B` do not contain the updates from the commit with op `B`.
Rather, the snapshot `B` corresponds to the TODO

Consider the following diagram (`lsm_batch_multiple=8`):

```
0   4   8  12  16  20  24  28  (op, snapshot)
┼───┬───┼───┬───┼───┬───┼───┬
        ,,,,,,,,........
        ↑A      ↑B      ↑C
```

Compaction is driven by the commits of ops `B→C` (`16…23`):
- Updates from ops `0→A` (`0…7`) are on-disk.
- Updates from ops `A→B` (`8…15`) are in the immutable table.
  - These updates were moved to the immutable table from the immutable table at the end of op `B-1`
    (`15`).
  - These updates will exist in the immutable table until it is reset at the end of op `C-1` (`23`).
- Updates from ops `B→C` (`16…23`) are added to the mutable table (by the respective commit).
- `tree.lookup_snapshot_max` is `B` (`16`).

At the end of the last beat of the compaction bar (`23`):
- Updates from ops `0→B` (`0…15`) are on disk.
- Updates from ops `B→C` (`16…23`) are moved from the mutable table to the immutable table.
- `tree.lookup_snapshot_max` is `C` (`24`).
