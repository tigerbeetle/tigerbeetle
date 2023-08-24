---
sidebar_position: 5
---

> “Just show me the tables already!”
> — probably not Fred Brooks

TigerBeetle stores all data inside a single file, called the data file (conventional extension is
`.tigerbeetle`).

The data file is divided into several zones, with the main ones being:

- write-ahead log
- superblock
- grid


The grid form the bulk of the data file. It is an array of 64KiB blocks:

```zig
pub const Block = [constants.block_size]u8;
pub const BlockPtr = *align(constants.sector_size) Block;
```

The grid serves as a raw storage layer. Higher level data structured (notably, LSM) are mapped to
physical grid blocks. A grid block is identified by a pair of a `u64` index and `u128` checksum:

```zig
pub const BlockReference = struct {
    index: u64,
    checksum: u128,
}
```

Block checksum is stored outside of the block itself, to protect from misdirected writes. So, to
read a block, you need to know block's index and checksum from "elsewhere", where "elsewhere" is
either a different block, or the superblock. Overall, the grid is used to implement purely
functional, persistent (in both senses), garbage collected data structure which is updated
atomically by updating the pointer to the root node.

Superblock is what holds this logical "root pointer". Superblock is located at a fixed position in
the data file, so, when a replica starts up, it can read the superblock, read root block indexes and
hashes from the superblock, and through those get access to the rest of the data in the grid.
Besides the logical root, superblock also stores a compressed bitset of all grid blocks which are
not currently allocated.

Superblock is updated atomically and relatively infrequently. So, the normal mode of operation is
that the replica starts up, reads the current superblock and free set to memory, then proceeds
allocating and writing new grid blocks, picking up free entries from the bit set. That is, although
the replica does write freshly allocated grid blocks to disk immediately, it does not update the
superblock on disk (so the logical state reachable from the superblock stays the same). Only after a
relatively large amount of new grid blocks written, replica atomically writes the new superblock,
with a new free set and a new logical "root pointer". To implement atomic update of the superblock,
the superblock is physically stored as 4 distinct copies on disk. After startup, replica picks the
latest superblock which has at least 2 copies written. Picking just the latest copy would be wrong
--- unlike the grid blocks, the superblock stores its own checksum, and is vulnerable to misdirected
reads (i.e., a misdirected read can hide the sole latest copy).

Because the superblock (and hence, logical grid state) is updated infrequently and in bursts, it
can't be the whole state. The rest of the state is stored in the write ahead log. WAL is a ring
buffer with prepares, and represents the logical diff which should be applied to the state
represented by superblock/grid to get the actual current state of the system. When a replica
processes a prepare it, roughly:

* writes it to WAL on disk
* applies changes from prepare to the in-memory data structure representing the current state
* applies changes from the prepare to the pending on-disk state by allocating and writing fresh grid
  blocks

When enough prepares are received, the superblock is updated to point the accumulated-so-far new
disk state.

This covers how the three major areas of the data file -- the write-ahead log, the superblock and
the grid -- work together to represent abstract persistent logical state.

Concretely, the sate of TigerBeetle is a collection (forest) of LSM trees. Each LSM tree stores a
set of objects. Objects are:

* fixed in size,
* small (hundreds of bytes),
* sorted by key,
* which is embedded in the object itself.

To start from the middle, objects are arranged in tables on disk. Each table represents a sorted
array of objects which is stored in multiple blocks. Specifically:

* table's data blocks are just sorted array of objects
* table's index block stores pointers to the data blocks, as well as boundary keys.

```zig
const TableDataBlock = struct {
    values_sorted: [value_count_max]Value,
};

const TableIndexBlock = struct {
    data_block_checksums: [data_block_count_max]u128,
    data_block_indexes:   [data_block_count_max]u64,
    data_block_key_max:   [data_block_count_max]Key,
};

const TableInfo = struct {
    index_block_index: u64,
    index_block_checksum: u128,
    key_min: Key,
    key_max: Key,
};
```

To lookup an object in a table, binary search the index block to locate the data block which should
hold the object, then binary search inside the data block.

Table size is limited by a single index block which can hold only so many references to data blocks.
Tables are arranged in levels. Each subsequent level contains exponentially more tables.

Tables in a single level are pairwise disjoint. Tables in different layers overlap, but the key LSM:
invariant is observed: values in shallow layers override values in deeper layers. This means that
all modification happen to the first (purely in-memory) level.

Asynchronous compaction process rebalances layers. Compaction removes one table from level A, finds
all tables from level A+1 that intersect that table, removes all those tables from level A+1 and
inserts the result of the intersection.

Schematically, effect of compaction can be represented as a sequence of events:

```zig
const Label = struct {
    level: u7,
    event: enum(u1) { insert, remove },
}

const CompactionOperation = struct {
    label: Label
    table: TableInfo, // points to table's index block
}
```

What's more, the current state of a tree can be represented as a sequence of such table creation and
removal events. And that's exactly how it is represented in a data file!

Specifically, each LSM tree is a collection of layers which is stored implicitly as log of events.
The log consist of a sequence of `ManifestLogBLocks`:

```zig
const ManifestLogBlock = struct {
  labels: [entry_count_max]Label,
  tables: [entry_count_max]TableInfo,
}
```

The superblock then stores all mainfest log blocks for all trees:

```zig
const Superblock = {
  tree_id: [count]u128,
  manifest_log_block_index: [count]u64,
  manifest_log_block_checksum: [count]u128,
}
```

Tying everything together:

State is represented as a collection of LSM trees. Superblock is the root of all state. For each LSM
tree, superblock contains the pointers to the blocks constituting tree's compaction log. Compaction
log describes a sequence of tables additions and deletions. By replaying compaction log, it is
possible to reconstruct in memory, the tables which constitute a level. A table is a pointer to its
index block. The index block is a sorted array of pointers to data blocks.
