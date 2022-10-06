//! An LSM tree.
//!
//!
//! These charts depict `compaction_op` and `prefetch_snapshot_max` over a series of
//! commits and compactions (with lsm_batch_multiple=8).
//!
//! Legend:
//!
//!   ┼   full measure (first beat start)
//!   ┬   half measure (third beat start)
//!   →   compaction_op
//!   ←   prefetch_snapshot_max (prefetch uses this as the current snapshot)
//!   ↔   prefetch_snapshot_max == compaction_op (same op)
//!   .   op is in mutable table (in memory)
//!   ,   op is in immutable table (in memory)
//!   #   op is on disk
//!   ✓   checkpoint() may follow compact()
//!
//!   0 2 4 6 8 0 2 4 6
//!   ┼───┬───┼───┬───┼
//!   ↔       ╷       ╷     init(superblock.commit_min=0)⎤ Compaction is effectively a noop for the
//!   ←→      ╷       ╷     commit;compact( 1) start     ⎥ first measure because there are no tables
//!   ←.→     ╷       ╷     commit;compact( 2) start     ⎥ on disk yet, and no immutable table to
//!   ←..→    ╷       ╷     commit;compact( 3) start    ⎤⎥ flush.
//!   ...↔    ╷       ╷            compact( 3)       end⎦⎥
//!   ...←→   ╷       ╷     commit;compact( 4) start/end ⎥ This is applicable:
//!   ...←.→  ╷       ╷     commit;compact( 5) start/end ⎥ - when the LSM is starting on a freshly
//!   ...←..→ ╷       ╷     commit;compact( 6) start/end ⎥   formatted data file, and also
//!   ...←...→╷       ╷     commit;compact( 7) start    ⎤⎥ - when the LSM is recovering from a crash
//!   ,,,,,,,↔╷       ╷  ✓         compact( 7)       end⎦⎦   before any checkpoint (see below).
//!   ,,,,,,,←→       ╷     commit;compact( 8) start/end
//!   ,,,,,,,←.→      ╷     commit;compact( 9) start/end
//!   ,,,,,,,←..→     ╷     commit;compact(10) start/end
//!   ,,,,,,,←...→    ╷     commit;compact(11) start    ⎤
//!   ,,,,,,,,...↔    ╷            compact(11)       end⎦
//!   ,,,,,,,,...←→   ╷     commit;compact(12) start/end
//!   ,,,,,,,,╷..←.→  ╷     commit;compact(13) start/end
//!   ,,,,,,,,...←→ → ╷     commit;compact(14) start/end
//!   ,,,,,,,,╷..←...→╷     commit;compact(15) start    ⎤
//!   ########,,,,,,,↔╷  ✓         compact(15)       end⎦
//!   ########,,,,,,,←→     commit;compact(16) start/end
//!   ┼───┬───┼───┬───┼
//!   0 2 4 6 8 0 2 4 6
//!   ┼───┬───┼───┬───┼
//!   ########       ↔╷     init(superblock.commit_min=7)  Recover with a checkpoint taken at op=15.
//!   ########.      ↔╷     commit        ( 8) start/end ⎤ At op 15, ops 8…15 are in memory, so they
//!   ########..     ↔╷     commit        ( 9) start/end ⎥ were dropped by the crash.
//!   ########...    ↔╷     commit        (10) start/end ⎥
//!   ########....   ↔╷     commit        (11) start    ⎤⎥ But compaction is not run for ops 8…15
//!   ########....   ↔╷                   (11)       end⎦⎥ because it was already performed
//!   ########.....  ↔╷     commit        (12) start/end ⎥ before the checkpoint.
//!   ########╷..... ↔╷     commit        (13) start/end ⎥
//!   ########.......↔╷     commit        (14) start/end ⎥ We can begin to compact again at op 16,
//!   ########╷......↔╷     commit        (15) start    ⎤⎥ because those compactions (if previously
//!   ########,,,,,,,↔╷  ✓                (15)       end⎦⎦ performed) are not included in the
//!   ########,,,,,,,←→     commit;compact(16) start/end   checkpoint.
//!   ┼───┬───┼───┬───┼
//!   0 2 4 6 8 0 2 4 6
//!
//! Notice how in the checkpoint recovery example above, we are careful not to `compact(op)` twice
//! for any op (even if we crash/recover), since that could lead to differences between replicas'
//! storage. The last measure of `commit()`s is always only in memory, so it is safe to repeat.
//!
const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;
const os = std.os;

const log = std.log.scoped(.tree);

const config = @import("../config.zig");
const div_ceil = @import("../util.zig").div_ceil;
const eytzinger = @import("eytzinger.zig").eytzinger;
const vsr = @import("../vsr.zig");
const binary_search = @import("binary_search.zig");
const bloom_filter = @import("bloom_filter.zig");

const CompositeKey = @import("composite_key.zig").CompositeKey;
const NodePool = @import("node_pool.zig").NodePool(config.lsm_manifest_node_size, 16);
const RingBuffer = @import("../ring_buffer.zig").RingBuffer;

/// We reserve maxInt(u64) to indicate that a table has not been deleted.
/// Tables that have not been deleted have snapshot_max of maxInt(u64).
/// Since we ensure and assert that a query snapshot never exactly matches
/// the snapshot_min/snapshot_max of a table, we must use maxInt(u64) - 1
/// to query all non-deleted tables.
pub const snapshot_latest: u64 = math.maxInt(u64) - 1;

// StateMachine:
//
// /// state machine will pass this on to all object stores
// /// Read I/O only
// pub fn read(batch, callback) void
//
// /// write the ops in batch to the memtable/objcache, previously called commit()
// pub fn write(batch) void
//
// /// Flush in memory state to disk, perform merges, etc
// /// Only function that triggers Write I/O in LSMs, as well as some Read
// /// Make as incremental as possible, don't block the main thread, avoid high latency/spikes
// pub fn flush(callback) void
//
// /// Write manifest info for all object stores into buffer
// pub fn encode_superblock(buffer) void
//
// /// Restore all in-memory state from the superblock data
// pub fn decode_superblock(buffer) void
//

/// The maximum number of tables for a single tree.
pub const table_count_max = table_count_max_for_tree(config.lsm_growth_factor, config.lsm_levels);

pub fn TreeType(comptime TreeTable: type, comptime Storage: type, comptime tree_name: []const u8) type {
    const Key = TreeTable.Key;
    const Value = TreeTable.Value;
    const compare_keys = TreeTable.compare_keys;
    const tombstone = TreeTable.tombstone;

    const tree_hash = blk: {
        // Blake3 hash does alot at comptime..
        @setEvalBranchQuota(tree_name.len * 1024);

        var hash: u256 = undefined;
        std.crypto.hash.Blake3.hash(tree_name, std.mem.asBytes(&hash), .{});
        break :blk @truncate(u128, hash);
    };

    return struct {
        const Tree = @This();

        // Expose the Table & hash for the Groove.
        pub const Table = TreeTable;
        pub const name = tree_name;
        pub const hash = tree_hash;

        const Grid = @import("grid.zig").GridType(Storage);
        const Manifest = @import("manifest.zig").ManifestType(Table, Storage);
        pub const TableMutable = @import("table_mutable.zig").TableMutableType(Table);
        const TableImmutable = @import("table_immutable.zig").TableImmutableType(Table);

        const CompactionType = @import("compaction.zig").CompactionType;
        const TableIteratorType = @import("table_iterator.zig").TableIteratorType;
        const TableImmutableIteratorType = @import("table_immutable.zig").TableImmutableIteratorType;

        const CompactionTable = CompactionType(Table, Storage, TableIteratorType);
        const CompactionTableImmutable = CompactionType(Table, Storage, TableImmutableIteratorType);

        grid: *Grid,
        options: Options,

        table_mutable: TableMutable,
        table_immutable: TableImmutable,

        manifest: Manifest,

        compaction_table_immutable: CompactionTableImmutable,

        /// The number of Compaction instances is divided by two as, at any given compaction tick,
        /// we're only compacting either even or odd levels but never both.
        /// Uses divFloor as the last level, even with odd lsm_levels, doesn't compact to anything.
        /// (e.g. floor(5/2) = 2 for levels 0->1, 2->3 when even and immut->0, 1->2, 3->4 when odd).
        /// This means, that for odd lsm_levels, the last CompactionTable is unused.
        compaction_table: [@divFloor(config.lsm_levels, 2)]CompactionTable,

        compaction_op: u64,

        /// The highest compaction_op whose corresponding half-compaction has completed.
        ///
        /// This corresponds to the highest snapshot that is safe to prefetch from.
        /// See also: `compaction_snapshot_for_op`.
        ///
        /// Invariants:
        /// * is one less than a multiple of a half-measure (unless prefetch_snapshot_max=0).
        /// * is equal to `compaction_op` while no compaction is in progress
        ///   (at init; between half-measures).
        /// * is less than `compaction_op` while any compaction is in progress.
        prefetch_snapshot_max: u64,

        compaction_io_pending: usize,
        compaction_callback: ?fn (*Tree) void,

        checkpoint_callback: ?fn (*Tree) void,
        open_callback: ?fn (*Tree) void,

        pub const Options = struct {
            /// The maximum number of keys that may be committed per batch.
            ///
            /// In general, the commit count max for a field depends on the field's object —
            /// how many objects might be inserted/updated/removed by a batch:
            ///   (config.message_size_max - sizeOf(vsr.header))
            /// For example, there are at most 8191 transfers in a batch.
            /// So commit_entries_max=8191 for transfer objects and indexes.
            ///
            /// However, if a transfer is ever mutated, then this will double commit_entries_max
            /// since the old index might need to be removed, and the new index inserted.
            ///
            /// A way to see this is by looking at the state machine. If a transfer is inserted,
            /// how many accounts and transfer put/removes will be generated?
            ///
            /// This also means looking at the state machine operation that will generate the
            /// most put/removes in the worst case.
            /// For example, create_accounts will put at most 8191 accounts.
            /// However, create_transfers will put 2 accounts (8191 * 2) for every transfer, and
            /// some of these accounts may exist, requiring a remove/put to update the index.
            commit_entries_max: u32,
        };

        pub fn init(
            allocator: mem.Allocator,
            node_pool: *NodePool,
            grid: *Grid,
            values_cache: ?*TableMutable.ValuesCache,
            options: Options,
        ) !Tree {
            assert(options.commit_entries_max > 0);
            assert(grid.superblock.opened);

            var table_mutable = try TableMutable.init(allocator, values_cache, options.commit_entries_max);
            errdefer table_mutable.deinit(allocator);

            var table_immutable = try TableImmutable.init(allocator, options.commit_entries_max);
            errdefer table_immutable.deinit(allocator);

            assert(table_immutable.value_count_max == table_mutable.value_count_max);

            var manifest = try Manifest.init(allocator, node_pool, grid, tree_hash);
            errdefer manifest.deinit(allocator);

            var compaction_table_immutable = try CompactionTableImmutable.init(allocator);
            errdefer compaction_table_immutable.deinit(allocator);

            var compaction_table: [@divFloor(config.lsm_levels, 2)]CompactionTable = undefined;
            for (compaction_table) |*compaction, i| {
                errdefer for (compaction_table[0..i]) |*c| c.deinit(allocator);
                compaction.* = try CompactionTable.init(allocator);
            }
            errdefer for (compaction_table) |*c| c.deinit(allocator);

            const commit_min = grid.superblock.working.vsr_state.commit_min;
            // Compaction is one measure ahead of superblock's commit_min.
            const compaction_op = if (commit_min == 0) 0 else commit_min + config.lsm_batch_multiple;

            return Tree{
                .grid = grid,
                .options = options,
                .table_mutable = table_mutable,
                .table_immutable = table_immutable,
                .manifest = manifest,
                .compaction_table_immutable = compaction_table_immutable,
                .compaction_table = compaction_table,
                .compaction_op = compaction_op,
                .prefetch_snapshot_max = compaction_op,
                .compaction_io_pending = 0,
                .compaction_callback = null,
                .checkpoint_callback = null,
                .open_callback = null,
            };
        }

        pub fn deinit(tree: *Tree, allocator: mem.Allocator) void {
            tree.compaction_table_immutable.deinit(allocator);
            for (tree.compaction_table) |*compaction| compaction.deinit(allocator);

            // TODO Consider whether we should release blocks acquired from Grid.block_free_set.
            tree.table_mutable.deinit(allocator);
            tree.table_immutable.deinit(allocator);
            tree.manifest.deinit(allocator);
        }

        pub fn put(tree: *Tree, value: *const Value) void {
            tree.table_mutable.put(value);
        }

        pub fn remove(tree: *Tree, value: *const Value) void {
            tree.table_mutable.remove(value);
        }

        /// Returns the value from the mutable or immutable table (possibly a tombstone),
        /// if one is available for the specified snapshot.
        pub fn lookup_from_memory(
            tree: *Tree,
            snapshot: u64,
            key: Key,
        ) ?*const Value {
            assert(snapshot <= tree.prefetch_snapshot_max);

            if (snapshot == tree.prefetch_snapshot_max) {
                if (tree.table_mutable.get(key)) |value| return value;
            } else {
                // The mutable table is converted to an immutable table when a snapshot is created.
                // This means that a past snapshot will never be able to see the mutable table.
                // This simplifies the mutable table and eliminates compaction for duplicate puts.
            }

            if (!tree.table_immutable.free and tree.table_immutable.snapshot_min < snapshot) {
                if (tree.table_immutable.get(key)) |value| return value;
            }

            return null;
        }

        /// Call this function only after checking `lookup_from_memory()`.
        pub fn lookup_from_levels(
            tree: *Tree,
            callback: fn (*LookupContext, ?*const Value) void,
            context: *LookupContext,
            snapshot: u64,
            key: Key,
        ) void {
            assert(snapshot <= tree.prefetch_snapshot_max);
            if (config.verify) {
                // The caller is responsible for checking the mutable table.
                assert(tree.lookup_from_memory(snapshot, key) == null);
            }

            var index_block_count: u8 = 0;
            var index_block_addresses: [config.lsm_levels]u64 = undefined;
            var index_block_checksums: [config.lsm_levels]u128 = undefined;
            {
                var it = tree.manifest.lookup(snapshot, key);
                while (it.next()) |table| : (index_block_count += 1) {
                    assert(table.visible(snapshot));
                    assert(compare_keys(table.key_min, key) != .gt);
                    assert(compare_keys(table.key_max, key) != .lt);

                    index_block_addresses[index_block_count] = table.address;
                    index_block_checksums[index_block_count] = table.checksum;
                }
            }

            if (index_block_count == 0) {
                callback(context, null);
                return;
            }

            // Hash the key to the fingerprint only once and reuse for all bloom filter checks.
            const fingerprint = bloom_filter.Fingerprint.create(mem.asBytes(&key));

            context.* = .{
                .tree = tree,
                .completion = undefined,

                .key = key,
                .fingerprint = fingerprint,

                .index_block_count = index_block_count,
                .index_block_addresses = index_block_addresses,
                .index_block_checksums = index_block_checksums,

                .callback = callback,
            };

            context.read_index_block();
        }

        pub const LookupContext = struct {
            const Read = Grid.Read;
            const BlockPtrConst = Grid.BlockPtrConst;

            tree: *Tree,
            completion: Read,

            key: Key,
            fingerprint: bloom_filter.Fingerprint,

            /// This value is an index into the index_block_addresses/checksums arrays.
            index_block: u8 = 0,
            index_block_count: u8,
            index_block_addresses: [config.lsm_levels]u64,
            index_block_checksums: [config.lsm_levels]u128,

            data_block: ?struct {
                address: u64,
                checksum: u128,
            } = null,

            callback: fn (*Tree.LookupContext, ?*const Value) void,

            fn read_index_block(context: *LookupContext) void {
                assert(context.data_block == null);
                assert(context.index_block < context.index_block_count);
                assert(context.index_block_count > 0);
                assert(context.index_block_count <= config.lsm_levels);

                context.tree.grid.read_block(
                    read_index_block_callback,
                    &context.completion,
                    context.index_block_addresses[context.index_block],
                    context.index_block_checksums[context.index_block],
                    .index,
                );
            }

            fn read_index_block_callback(completion: *Read, index_block: BlockPtrConst) void {
                const context = @fieldParentPtr(LookupContext, "completion", completion);
                assert(context.data_block == null);
                assert(context.index_block < context.index_block_count);
                assert(context.index_block_count > 0);
                assert(context.index_block_count <= config.lsm_levels);

                const blocks = Table.index_blocks_for_key(index_block, context.key);

                context.data_block = .{
                    .address = blocks.data_block_address,
                    .checksum = blocks.data_block_checksum,
                };

                context.tree.grid.read_block(
                    read_filter_block_callback,
                    completion,
                    blocks.filter_block_address,
                    blocks.filter_block_checksum,
                    .filter,
                );
            }

            fn read_filter_block_callback(completion: *Read, filter_block: BlockPtrConst) void {
                const context = @fieldParentPtr(LookupContext, "completion", completion);
                assert(context.data_block != null);
                assert(context.index_block < context.index_block_count);
                assert(context.index_block_count > 0);
                assert(context.index_block_count <= config.lsm_levels);

                const filter_bytes = Table.filter_block_filter_const(filter_block);
                if (bloom_filter.may_contain(context.fingerprint, filter_bytes)) {
                    context.tree.grid.read_block(
                        read_data_block_callback,
                        completion,
                        context.data_block.?.address,
                        context.data_block.?.checksum,
                        .data,
                    );
                } else {
                    // The key is not present in this table, check the next level.
                    context.advance_to_next_level();
                }
            }

            fn read_data_block_callback(completion: *Read, data_block: BlockPtrConst) void {
                const context = @fieldParentPtr(LookupContext, "completion", completion);
                assert(context.data_block != null);
                assert(context.index_block < context.index_block_count);
                assert(context.index_block_count > 0);
                assert(context.index_block_count <= config.lsm_levels);

                if (Table.data_block_search(data_block, context.key)) |value| {
                    context.callback(context, unwrap_tombstone(value));
                } else {
                    // The key is not present in this table, check the next level.
                    context.advance_to_next_level();
                }
            }

            fn advance_to_next_level(context: *LookupContext) void {
                assert(context.data_block != null);
                assert(context.index_block < context.index_block_count);
                assert(context.index_block_count > 0);
                assert(context.index_block_count <= config.lsm_levels);

                context.index_block += 1;
                if (context.index_block == context.index_block_count) {
                    context.callback(context, null);
                    return;
                }
                assert(context.index_block < context.index_block_count);

                context.data_block = null;
                context.read_index_block();
            }
        };

        /// Returns null if the value is null or a tombstone, otherwise returns the value.
        /// We use tombstone values internally, but expose them as null to the user.
        /// This distinction enables us to cache a null result as a tombstone in our hash maps.
        inline fn unwrap_tombstone(value: ?*const Value) ?*const Value {
            return if (value == null or tombstone(value.?)) null else value.?;
        }

        pub fn open(tree: *Tree, callback: fn (*Tree) void) void {
            assert(tree.open_callback == null);
            tree.open_callback = callback;

            tree.manifest.open(manifest_open_callback);
        }

        fn manifest_open_callback(manifest: *Manifest) void {
            const tree = @fieldParentPtr(Tree, "manifest", manifest);
            assert(tree.open_callback != null);

            const callback = tree.open_callback.?;
            tree.open_callback = null;
            callback(tree);
        }

        // Tree compaction runs to the sound of music!
        //
        // Compacting LSM trees involves merging and moving tables into the next levels as needed.
        // To avoid write amplification stalls and bound latency, compaction is done incrementally.
        //
        // A full compaction phase is denoted as a bar or measure, using terms from music notation.
        // Each measure consists of `lsm_batch_multiple` beats or "compaction ticks" of work.
        // A compaction beat is started asynchronously with `compact` which takes a callback.
        // After `compact_tick` is called, `compact_cpu` should be called to enable pipelining.
        // The compaction beat completes when the `compact_tick` callback is invoked.
        //
        // A measure is split in half according to the "first" down beat and "middle" down beat.
        // The first half of the measure compacts even levels while the latter compacts odd levels.
        // Mutable table changes are sorted and compacted into the immutable table.
        // The immutable table is compacted into level 0 during the odd level half of the measure.
        //
        // At any given point, there's only levels/2 max compactions happening concurrently.
        // The source level is denoted as `level_a` with the target level being `level_b`.
        // The last level in the LSM tree has no target level so it's not compaction-from.
        //
        // Assuming a measure/`lsm_batch_multiple` of 4, the invariants can be described as follows:
        //  * assert: at the end of every beat, there's space in mutable table for the next beat.
        //  * manifest info for the tables compacted are updating during the compaction.
        //  * manifest is compacted at the end of every beat.
        //
        //  - (first) down beat of the measure:
        //      * assert: no compactions are currently running.
        //      * allow level visible table counts to overflow if needed.
        //      * start even level compactions if there's any tables to compact.
        //
        //  - (second) up beat of the measure:
        //      * finish ticking running even-level compactions.
        //      * assert: on callback completion, all compactions should be completed.
        //
        //  - (third) down beat of the measure:
        //      * assert: no compactions are currently running.
        //      * start odd level compactions if there are any tables to compact, and only if we must.
        //      * compact the immutable table if it contains any sorted values (it could be empty).
        //
        //  - (fourth) last beat of the measure:
        //      * finish ticking running odd-level and immutable table compactions.
        //      * assert: on callback completion, all compactions should be completed.
        //      * assert: on callback completion, all level visible table counts shouldn't overflow.
        //      * flush, clear, and sort mutable table values into immutable table for next measure.

        const half_measure_beat_count = @divExact(config.lsm_batch_multiple, 2);

        const CompactionTableContext = struct {
            compaction: *CompactionTable,
            level_a: u8,
            level_b: u8,
        };

        const CompactionTableIterator = struct {
            tree: *Tree,
            index: u8 = 0,

            fn next(it: *CompactionTableIterator) ?CompactionTableContext {
                assert(it.tree.compaction_callback != null);

                const compaction_beat = it.tree.compaction_op % config.lsm_batch_multiple;
                const even_levels = compaction_beat < half_measure_beat_count;
                const level_a = (it.index * 2) + @boolToInt(!even_levels);
                const level_b = level_a + 1;

                if (level_a >= config.lsm_levels - 1) return null;
                assert(level_b < config.lsm_levels);

                defer it.index += 1;
                return CompactionTableContext{
                    .compaction = &it.tree.compaction_table[it.index],
                    .level_a = level_a,
                    .level_b = level_b,
                };
            }
        };

        /// Since concurrent compactions into and out of a level may contend for the same range:
        ///
        /// 1. compact level 0 to 1, level 2 to 3, level 4 to 5 etc., and then
        /// 2. compact the immutable table to level 0, level 1 to 2, level 3 to 4 etc.
        ///
        /// This order (even levels, then odd levels) is significant, since it reduces the number of
        /// level 0 tables that overlap with the immutable table, reducing write amplification.
        ///
        /// We therefore take the measure, during which all compactions run, and divide by two,
        /// running the compactions from even levels in the first half measure, and then the odd.
        ///
        /// Compactions start on the down beat of a half measure, using 0-based beats.
        /// For example, if there are 4 beats in a measure, start on beat 0 or beat 2.
        pub fn compact(tree: *Tree, callback: fn (*Tree) void, op: u64) void {
            assert(tree.compaction_callback == null);
            assert(op != 0);
            assert(op <= tree.compaction_op + 1);
            assert(op > tree.grid.superblock.working.vsr_state.commit_min);

            if (tree.grid.superblock.working.vsr_state.op_compacted(op)) {
                // We recovered from a checkpoint, and must avoid replaying one measure of
                // compactions that were applied before the checkpoint.
                assert(tree.compaction_op == tree.prefetch_snapshot_max);
                assert(tree.compaction_op >= op);
                assert(tree.compaction_op - op <= config.lsm_batch_multiple);

                if (tree.compaction_op == op) {
                    // This is the last op of the skipped compaction measure.
                    // Prepare the immutable table for the next measure — since this state is
                    // in-memory, it cannot be skipped.
                    tree.compact_mutable_table_into_immutable();
                }

                // TODO Defer this callback until tick() to avoid stack growth.
                callback(tree);
                return;
            }
            assert(op == tree.compaction_op + 1);

            tree.compact_start(callback, op);
            tree.compact_drive();
        }

        fn compact_start(tree: *Tree, callback: fn (*Tree) void, op: u64) void {
            assert(tree.compaction_io_pending == 0);
            assert(tree.compaction_callback == null);

            assert(tree.compaction_op + 1 == op);
            assert(tree.prefetch_snapshot_max < op);
            tree.compaction_op = op;
            tree.compaction_callback = callback;

            const compaction_beat = tree.compaction_op % config.lsm_batch_multiple;
            const start = (compaction_beat == 0) or
                (compaction_beat == half_measure_beat_count);

            const snapshot = compaction_snapshot_for_op(op);
            assert(snapshot < snapshot_latest);
            assert(snapshot == 0 or (snapshot + 1) % half_measure_beat_count == 0);

            log.debug(tree_name ++ ": compact_start: op={d} snapshot={d} beat={d}/{d}", .{
                op,
                snapshot,
                compaction_beat + 1,
                config.lsm_batch_multiple,
            });

            // Try to start compacting the immutable table.
            const even_levels = compaction_beat < half_measure_beat_count;
            if (even_levels) {
                assert(tree.compaction_table_immutable.status == .idle);
            } else {
                if (start) tree.compact_start_table_immutable(snapshot);
            }

            // Try to start compacting the other levels.
            var it = CompactionTableIterator{ .tree = tree };
            while (it.next()) |context| {
                if (start) tree.compact_start_table(snapshot, context);
            }
        }

        fn compact_start_table_immutable(tree: *Tree, snapshot: u64) void {
            const compaction_beat = tree.compaction_op % config.lsm_batch_multiple;
            assert(compaction_beat == half_measure_beat_count);

            // Do not start compaction if the immutable table does not require compaction.
            if (tree.table_immutable.free) return;

            const values_count = tree.table_immutable.values.len;
            assert(values_count > 0);

            const level_b: u8 = 0;
            const table_a: ?*const Manifest.TableInfo = null;
            const range = tree.manifest.compaction_range(
                level_b,
                tree.table_immutable.key_min(),
                tree.table_immutable.key_max(),
            );

            assert(range.table_count >= 1);
            assert(compare_keys(range.key_min, tree.table_immutable.key_min()) != .gt);
            assert(compare_keys(range.key_max, tree.table_immutable.key_max()) != .lt);

            log.debug(tree_name ++
                ": compacting immutable table to level 0 " ++
                "(values.len={d} snapshot_min={d} compaction.snapshot={d} table_count={d})", .{
                tree.table_immutable.values.len,
                tree.table_immutable.snapshot_min,
                snapshot,
                range.table_count,
            });

            tree.compaction_table_immutable.start(
                tree.grid,
                &tree.manifest,
                snapshot,
                range,
                table_a,
                level_b,
                .{ .table = &tree.table_immutable },
            );
        }

        fn compact_start_table(tree: *Tree, snapshot: u64, context: CompactionTableContext) void {
            assert(context.level_a < config.lsm_levels);
            assert(context.level_b < config.lsm_levels);
            assert(context.level_a + 1 == context.level_b);

            // Do not start compaction if level A does not require compaction.
            const table_range = tree.manifest.compaction_table(context.level_a) orelse return;
            const table = table_range.table;
            const range = table_range.range;

            assert(range.table_count >= 1);
            assert(compare_keys(table.key_min, table.key_max) != .gt);
            assert(compare_keys(range.key_min, table.key_min) != .gt);
            assert(compare_keys(range.key_max, table.key_max) != .lt);

            log.debug(tree_name ++ ": compacting {d} tables from level {d} to level {d}", .{
                range.table_count,
                context.level_a,
                context.level_b,
            });

            context.compaction.start(
                tree.grid,
                &tree.manifest,
                snapshot,
                table_range.range,
                table_range.table,
                context.level_b,
                .{
                    .grid = tree.grid,
                    .address = table.address,
                    .checksum = table.checksum,
                },
            );
        }

        fn compact_drive(tree: *Tree) void {
            assert(tree.compaction_io_pending <= 2 + tree.compaction_table.len);
            assert(tree.compaction_callback != null);

            // Always start one fake io_pending that is resolved right after
            // to handle the case where this compaction tick triggers no IO.
            // (For example, ticking the immutable table, or level B is already done).
            tree.compaction_io_pending += 1;
            defer tree.compact_tick_done();

            // Try to tick the immutable table compaction:
            const compaction_beat = tree.compaction_op % config.lsm_batch_multiple;
            const even_levels = compaction_beat < half_measure_beat_count;
            if (even_levels) {
                assert(tree.compaction_table_immutable.status == .idle);
            } else {
                tree.compact_tick(&tree.compaction_table_immutable);
            }

            // Try to tick the compaction for each level:
            var it = CompactionTableIterator{ .tree = tree };
            while (it.next()) |context| {
                tree.compact_tick(context.compaction);
            }
        }

        fn compact_tick(tree: *Tree, compaction: anytype) void {
            if (compaction.status != .processing) return;
            tree.compaction_io_pending += 1;

            const compaction_beat = tree.compaction_op % config.lsm_batch_multiple;
            const even_levels = compaction_beat < half_measure_beat_count;
            assert(compaction.level_b < config.lsm_levels);
            assert(compaction.level_b % 2 == @boolToInt(even_levels));

            if (@TypeOf(compaction.*) == CompactionTableImmutable) {
                assert(compaction.level_b == 0);
                log.debug(tree_name ++ ": compact_tick() for immutable table to level 0", .{});
                compaction.compact_tick(Tree.compact_tick_callback_table_immutable);
            } else {
                assert(@TypeOf(compaction.*) == CompactionTable);
                log.debug(tree_name ++ ": compact_tick() for level {d} to level {d}", .{
                    compaction.level_b - 1,
                    compaction.level_b,
                });
                compaction.compact_tick(Tree.compact_tick_callback_table);
            }
        }

        fn compact_tick_callback_table_immutable(compaction: *CompactionTableImmutable) void {
            assert(compaction.status == .processing or compaction.status == .done);
            assert(compaction.level_b < config.lsm_levels);
            assert(compaction.level_b == 0);

            const tree = @fieldParentPtr(Tree, "compaction_table_immutable", compaction);
            const compaction_beat = tree.compaction_op % config.lsm_batch_multiple;
            assert(compaction_beat >= half_measure_beat_count);

            log.debug(tree_name ++ ": compact_tick() complete for immutable table to level 0", .{});
            tree.compact_tick_done();
        }

        fn compact_tick_callback_table(compaction: *CompactionTable) void {
            assert(compaction.status == .processing or compaction.status == .done);
            assert(compaction.level_b < config.lsm_levels);
            assert(compaction.level_b > 0);

            const table_offset = @divFloor(compaction.level_b - 1, 2);
            const table_ptr = @ptrCast([*]CompactionTable, compaction) - table_offset;

            const table_size = @divFloor(config.lsm_levels, 2);
            const table: *[table_size]CompactionTable = table_ptr[0..table_size];

            log.debug(tree_name ++ ": compact_tick() complete for level {d} to level {d}", .{
                compaction.level_b - 1,
                compaction.level_b,
            });

            const tree = @fieldParentPtr(Tree, "compaction_table", table);
            tree.compact_tick_done();
        }

        fn compact_tick_done(tree: *Tree) void {
            assert(tree.compaction_io_pending <= 2 + tree.compaction_table.len);
            assert(tree.compaction_callback != null);

            // compact_done() is called after all compact_tick()'s complete.
            tree.compaction_io_pending -= 1;
            if (tree.compaction_io_pending == 0) tree.compact_done();
        }

        /// Called at the end of each compaction tick.
        fn compact_done(tree: *Tree) void {
            assert(tree.compaction_io_pending == 0);
            assert(tree.compaction_callback != null);

            const compaction_beat = tree.compaction_op % config.lsm_batch_multiple;
            const even_levels = compaction_beat < half_measure_beat_count;
            const compacted_levels_even = compaction_beat == half_measure_beat_count - 1;
            const compacted_levels_odd = compaction_beat == config.lsm_batch_multiple - 1;
            if (!compacted_levels_even and !compacted_levels_odd) {
                tree.compact_finish();
                return;
            }

            // At the end of the second and fourth beat:
            // 1. Tick the Compactions until all have completed.
            // 2. Remove invisible tables from the manifest.
            // 3. Compact the manifest.
            // Then at the end of the fourth beat, freeze the mutable table.
            assert(compacted_levels_even or compacted_levels_odd);
            assert(compacted_levels_even != compacted_levels_odd);

            const still_compacting = blk: {
                if (even_levels) {
                    assert(tree.compaction_table_immutable.status == .idle);
                } else {
                    if (tree.compaction_table_immutable.status == .processing) break :blk true;
                }

                var it = CompactionTableIterator{ .tree = tree };
                while (it.next()) |context| {
                    if (context.compaction.status == .processing) break :blk true;
                }
                break :blk false;
            };

            if (still_compacting) {
                // We are at the end of a half-measure, but the compactions have not finished.
                // We keep ticking them until they finish.
                log.debug(tree_name ++ ": compact_done: driving outstanding compactions", .{});
                tree.compact_drive();
                return;
            }

            // All compactions have finished for the current half-measure.
            // We couldn't remove the (invisible) input tables until now because prefetch()
            // needs a complete set of tables for lookups to avoid missing data.

            assert(tree.compaction_op > tree.prefetch_snapshot_max);
            assert(tree.compaction_op - tree.prefetch_snapshot_max <= half_measure_beat_count);
            assert((tree.compaction_op + 1) % half_measure_beat_count == 0);
            tree.prefetch_snapshot_max = tree.compaction_op;

            // Reset the immutable table Compaction.
            // Also clear any invisible tables it created during compaction.
            if (!even_levels) {
                switch (tree.compaction_table_immutable.status) {
                    // The compaction wasn't started for this half measure.
                    .idle => assert(tree.table_immutable.free),
                    .processing => unreachable,
                    .done => {
                        tree.compaction_table_immutable.reset();
                        tree.table_immutable.clear();
                        tree.manifest.remove_invisible_tables(
                            tree.compaction_table_immutable.level_b,
                            tree.compaction_table_immutable.snapshot,
                            tree.compaction_table_immutable.range.key_min,
                            tree.compaction_table_immutable.range.key_max,
                        );
                    },
                }
            }

            // Reset all the other Compactions.
            // Also clear any invisible tables they created during compaction.
            var it = CompactionTableIterator{ .tree = tree };
            while (it.next()) |context| {
                switch (context.compaction.status) {
                    .idle => {}, // The compaction wasn't started for this half measure.
                    .processing => unreachable,
                    .done => {
                        context.compaction.reset();
                        tree.manifest.remove_invisible_tables(
                            context.compaction.level_b,
                            context.compaction.snapshot,
                            context.compaction.range.key_min,
                            context.compaction.range.key_max,
                        );
                    },
                }
            }

            assert(tree.compaction_table_immutable.status == .idle);
            it = CompactionTableIterator{ .tree = tree };
            while (it.next()) |context| {
                assert(context.compaction.status == .idle);
            }

            // At the end of the fourth/last beat:
            // - Assert all visible tables haven't overflowed their max per level.
            // - Convert mutable table to immutable table for next measure.
            if (compacted_levels_odd) {
                tree.manifest.assert_level_table_counts();
                tree.compact_mutable_table_into_immutable();
            }

            // At the end of the second/fourth beat:
            // - Compact the manifest before invoking the compact() callback.
            tree.manifest.compact(compact_manifest_callback);
        }

        fn compact_mutable_table_into_immutable(tree: *Tree) void {
            // Ensure mutable table can be flushed into immutable table.
            if (tree.table_mutable.count() == 0) return;
            assert(tree.table_immutable.free);

            // Sort the mutable table values directly into the immutable table's array.
            const values_max = tree.table_immutable.values_max();
            const values = tree.table_mutable.sort_into_values_and_clear(values_max);
            assert(values.ptr == values_max.ptr);

            // Take a manifest snapshot and setup the immutable table with the sorted values.
            const snapshot_min = tree.compaction_table_immutable.snapshot;
            tree.table_immutable.reset_with_sorted_values(snapshot_min, values);

            assert(tree.table_mutable.count() == 0);
            assert(!tree.table_immutable.free);
        }

        fn compact_manifest_callback(manifest: *Manifest) void {
            const tree = @fieldParentPtr(Tree, "manifest", manifest);
            assert(tree.compaction_io_pending == 0);
            assert(tree.compaction_callback != null);
            tree.compact_finish();
        }

        /// Called at the end of each compaction beat.
        fn compact_finish(tree: *Tree) void {
            assert(tree.compaction_io_pending == 0);
            assert(tree.table_mutable.can_commit_batch(tree.options.commit_entries_max));

            // Invoke the compact() callback after the manifest compacts at the end of the beat.
            const callback = tree.compaction_callback.?;
            tree.compaction_callback = null;
            callback(tree);
        }

        pub fn checkpoint(tree: *Tree, callback: fn (*Tree) void) void {
            // Assert no outstanding compact_tick() work.
            assert(tree.compaction_io_pending == 0);
            assert(tree.compaction_callback == null);
            assert(tree.compaction_op > 0);
            assert(tree.compaction_op == tree.prefetch_snapshot_max);
            // Don't re-run the checkpoint we recovered from.
            assert(!tree.grid.superblock.working.vsr_state.op_compacted(tree.compaction_op));

            // Assert that this is the last beat in the compaction measure.
            const compaction_beat = tree.compaction_op % config.lsm_batch_multiple;
            const last_beat_in_measure = config.lsm_batch_multiple - 1;
            assert(last_beat_in_measure == compaction_beat);

            // Assert no outstanding compactions.
            assert(tree.compaction_table_immutable.status == .idle);
            for (tree.compaction_table) |*compaction| {
                assert(compaction.status == .idle);
            }

            // Assert all manifest levels haven't overflowed their table counts.
            tree.manifest.assert_level_table_counts();

            // Assert that we're checkpointing only after invisible tables have been removed.
            if (config.verify) {
                const snapshot = compaction_snapshot_for_op(tree.compaction_op);
                tree.manifest.assert_no_invisible_tables(snapshot);
            }

            // Start an asynchronous checkpoint on the manifest.
            assert(tree.checkpoint_callback == null);
            tree.checkpoint_callback = callback;
            tree.manifest.checkpoint(manifest_checkpoint_callback);
        }

        fn manifest_checkpoint_callback(manifest: *Manifest) void {
            const tree = @fieldParentPtr(Tree, "manifest", manifest);
            assert(tree.checkpoint_callback != null);

            const callback = tree.checkpoint_callback.?;
            tree.checkpoint_callback = null;
            callback(tree);
        }

        pub const RangeQuery = union(enum) {
            bounded: struct {
                start: Key,
                end: Key,
            },
            open: struct {
                start: Key,
                order: enum {
                    ascending,
                    descending,
                },
            },
        };

        pub const RangeQueryIterator = struct {
            tree: *Tree,
            snapshot: u64,
            query: RangeQuery,

            pub fn next(callback: fn (result: ?Value) void) void {
                _ = callback;
            }
        };

        pub fn range_query(
            tree: *Tree,
            /// The snapshot timestamp, if any
            snapshot: ?u64,
            query: RangeQuery,
        ) RangeQueryIterator {
            _ = tree;
            _ = snapshot;
            _ = query;
        }
    };
}

/// Returns the target snapshot of a compaction tick at the given op.
///
/// This snapshot will be:
/// - the `snapshot_min` of a compaction's output tables, and
/// - the `snapshot_max` of a compaction's input tables.
///
/// The target snapshot of a compaction is the last op of the preceding half-measure.
///
/// At the start of the current batch, mutable table inserts from the previous batch
/// would be in the immutable table. This means the current batch compaction will
/// actually be flushing to disk (levels) mutable table updates from the previous batch.
///
/// -1 as the ops are zero based so the "last" op from previous batch is reflected.
pub fn compaction_snapshot_for_op(op: u64) u64 {
    // Each half-measure has a snapshot instead of each full measure, because this allows the
    // output tables of the first half-measure's compaction to be prefetched against earlier —
    // hopefully while they are still warm in the cache from being written.
    return std.mem.alignBackward(op, @divExact(config.lsm_batch_multiple, 2)) -| 1;
}

/// The total number of tables that can be supported by the tree across so many levels.
pub fn table_count_max_for_tree(growth_factor: u32, levels_count: u32) u32 {
    assert(growth_factor >= 4);
    assert(growth_factor <= 16); // Limit excessive write amplification.
    assert(levels_count >= 2);
    assert(levels_count <= 10); // Limit excessive read amplification.
    assert(levels_count <= config.lsm_levels);

    var count: u32 = 0;
    var level: u32 = 0;
    while (level < levels_count) : (level += 1) {
        count += table_count_max_for_level(growth_factor, level);
    }
    return count;
}

/// The total number of tables that can be supported by the level alone.
pub fn table_count_max_for_level(growth_factor: u32, level: u32) u32 {
    assert(level >= 0);
    assert(level < config.lsm_levels);

    return math.pow(u32, growth_factor, level + 1);
}

test "table_count_max_for_level/tree" {
    const expectEqual = std.testing.expectEqual;

    try expectEqual(@as(u32, 8), table_count_max_for_level(8, 0));
    try expectEqual(@as(u32, 64), table_count_max_for_level(8, 1));
    try expectEqual(@as(u32, 512), table_count_max_for_level(8, 2));
    try expectEqual(@as(u32, 4096), table_count_max_for_level(8, 3));
    try expectEqual(@as(u32, 32768), table_count_max_for_level(8, 4));
    try expectEqual(@as(u32, 262144), table_count_max_for_level(8, 5));
    try expectEqual(@as(u32, 2097152), table_count_max_for_level(8, 6));

    try expectEqual(@as(u32, 8 + 64), table_count_max_for_tree(8, 2));
    try expectEqual(@as(u32, 72 + 512), table_count_max_for_tree(8, 3));
    try expectEqual(@as(u32, 584 + 4096), table_count_max_for_tree(8, 4));
    try expectEqual(@as(u32, 4680 + 32768), table_count_max_for_tree(8, 5));
    try expectEqual(@as(u32, 37448 + 262144), table_count_max_for_tree(8, 6));
    try expectEqual(@as(u32, 299592 + 2097152), table_count_max_for_tree(8, 7));
}

pub fn main() !void {
    const testing = std.testing;
    const allocator = testing.allocator;

    const IO = @import("../io.zig").IO;
    const Storage = @import("../storage.zig").Storage;
    const Grid = @import("grid.zig").GridType(Storage);

    const data_file_size_min = @import("../vsr/superblock.zig").data_file_size_min;

    const dir_fd = try IO.open_dir(".");
    const storage_fd = try IO.open_file(dir_fd, "test_tree", data_file_size_min, true);
    defer std.fs.cwd().deleteFile("test_tree") catch {};

    var io = try IO.init(128, 0);
    defer io.deinit();

    var storage = try Storage.init(&io, storage_fd);
    defer storage.deinit();

    const Key = CompositeKey(u128);
    const Table = @import("table.zig").TableType(
        Key,
        Key.Value,
        Key.compare_keys,
        Key.key_from_value,
        Key.sentinel_key,
        Key.tombstone,
        Key.tombstone_from_key,
    );

    const Tree = TreeType(Table, Storage, @typeName(Table) ++ "_test");

    // Check out our spreadsheet to see how we calculate node_count for a forest of trees.
    const node_count = 1024;
    var node_pool = try NodePool.init(allocator, node_count);
    defer node_pool.deinit(allocator);

    var value_cache = Tree.ValueCache{};
    try value_cache.ensureTotalCapacity(allocator, 10000);
    defer value_cache.deinit(allocator);

    const batch_size_max = config.message_size_max - @sizeOf(vsr.Header);
    const commit_entries_max = @divFloor(batch_size_max, 128);

    var sort_buffer = try allocator.allocAdvanced(
        u8,
        16,
        // This must be the greatest commit_entries_max and value_size across trees:
        commit_entries_max * config.lsm_batch_multiple * 128,
        .exact,
    );
    defer allocator.free(sort_buffer);

    // TODO Initialize SuperBlock:
    var superblock: vsr.SuperBlockType(Storage) = undefined;

    var grid = try Grid.init(allocator, &superblock);
    defer grid.deinit(allocator);

    var tree = try Tree.init(
        allocator,
        &node_pool,
        &grid,
        &value_cache,
        .{
            .prefetch_entries_max = commit_entries_max * 2,
            .commit_entries_max = commit_entries_max,
        },
    );
    defer tree.deinit(allocator);

    testing.refAllDecls(@This());

    // TODO: more references
    _ = Table;
    _ = Table.Builder.data_block_finish;

    // TODO: more references
    _ = Tree.CompactionTable;

    _ = tree.prefetch_enqueue;
    _ = tree.prefetch;
    _ = tree.prefetch_key;
    _ = tree.get;
    _ = tree.put;
    _ = tree.remove;
    _ = tree.lookup;
    _ = tree.compact_tick;

    _ = Tree.Manifest.LookupIterator.next;
    _ = tree.manifest;
    _ = tree.manifest.lookup;
    _ = tree.manifest.insert_tables;

    std.debug.print("table_count_max={}\n", .{table_count_max});
}
