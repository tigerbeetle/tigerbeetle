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
const SuperBlockType = vsr.SuperBlockType;

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
// /// Flush in memory state to disk, preform merges, etc
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

pub const table_count_max = table_count_max_for_tree(config.lsm_growth_factor, config.lsm_levels);

pub fn TreeType(comptime Table: type, comptime Storage: type, comptime tree_name: []const u8) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const compare_keys = Table.compare_keys;
    const tombstone = Table.tombstone;
    const tombstone_from_key = Table.tombstone_from_key;

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
        pub const TableType = Table;
        pub const name = tree_name;
        pub const hash = tree_hash;

        const Grid = @import("grid.zig").GridType(Storage);
        const Manifest = @import("manifest.zig").ManifestType(Table, Storage);
        const TableMutable = @import("table_mutable.zig").TableMutableType(Table);
        const TableImmutable = @import("table_immutable.zig").TableImmutableType(Table);

        const CompactionType = @import("compaction.zig").CompactionType;
        const TableIteratorType = @import("table_iterator.zig").TableIteratorType;
        const TableImmutableIteratorType = @import("table_immutable.zig").TableImmutableIteratorType;

        pub const ValueCache = std.HashMapUnmanaged(Value, void, Table.HashMapContextValue, 70);

        const CompactionTable = CompactionType(Table, Storage, TableIteratorType);
        const CompactionTableImmutable = CompactionType(Table, Storage, TableImmutableIteratorType);

        grid: *Grid,
        options: Options,

        /// TODO(ifreund) Replace this with SetAssociativeCache:
        /// A set associative cache of values shared by trees with the same key/value sizes.
        /// This is used to accelerate point lookups and is not used for range queries.
        /// Secondary index trees used only for range queries can therefore set this to null.
        /// The value type will be []u8 and this will be shared by trees with the same value size.
        value_cache: ?*ValueCache,

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
        compaction_io_pending: usize,
        compaction_callback: ?*const fn (*Tree) void,

        checkpoint_callback: ?*const fn (*Tree) void,
        open_callback: ?*const fn (*Tree) void,

        pub const Options = struct {
            /// The maximum number of keys that may be committed per batch.
            commit_count_max: u32,
        };

        pub fn init(
            allocator: mem.Allocator,
            node_pool: *NodePool,
            grid: *Grid,
            value_cache: ?*ValueCache,
            options: Options,
        ) !Tree {
            var table_mutable = try TableMutable.init(allocator, options.commit_count_max);
            errdefer table_mutable.deinit(allocator);

            var table_immutable = try TableImmutable.init(allocator, options.commit_count_max);
            errdefer table_immutable.deinit(allocator);

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

            return Tree{
                .grid = grid,
                .options = options,
                .value_cache = value_cache,
                .table_mutable = table_mutable,
                .table_immutable = table_immutable,
                .manifest = manifest,
                .compaction_table_immutable = compaction_table_immutable,
                .compaction_table = compaction_table,
                .compaction_op = 0,
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

        /// Get a cached value/tombstone for the given key.
        /// Returns null if no value/tombstone for the given key is cached.
        pub fn get_cached(tree: *const Tree, key: Key) ?*const Value {
            const value = tree.table_mutable.get(key) orelse
                tree.value_cache.?.getKeyPtr(tombstone_from_key(key));

            return value;
        }

        pub fn put(tree: *Tree, value: *const Value) void {
            tree.table_mutable.put(value);
        }

        pub fn remove(tree: *Tree, value: *const Value) void {
            tree.table_mutable.remove(value);
        }

        pub fn lookup(
            tree: *Tree,
            callback: *const fn (*LookupContext, ?*const Value) void,
            context: *LookupContext,
            snapshot: u64,
            key: Key,
        ) void {
            assert(snapshot <= snapshot_latest);
            if (snapshot == snapshot_latest) {
                // The mutable table is converted to an immutable table when a snapshot is created.
                // This means that a snapshot will never be able to see the mutable table.
                // This simplifies the mutable table and eliminates compaction for duplicate puts.
                // The value cache is only used for the latest snapshot for simplicity.
                // Earlier snapshots will still be able to utilize the block cache.
                if (tree.table_mutable.get(key) orelse
                    tree.value_cache.?.getKeyPtr(tombstone_from_key(key))) |value|
                {
                    callback(context, unwrap_tombstone(value));
                    return;
                }
            }

            if (!tree.table_immutable.free and tree.table_immutable.snapshot_min < snapshot) {
                if (tree.table_immutable.get(key)) |value| {
                    callback(context, unwrap_tombstone(value));
                    return;
                }
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

            callback: *const fn (*Tree.LookupContext, ?*const Value) void,

            fn finish(context: *LookupContext, value: ?*const Value) void {
                const callback = context.callback;
                context.* = undefined;
                callback(context, value);
            }

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
                    context.finish(unwrap_tombstone(value));
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
                    context.finish(null);
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

        pub fn open(tree: *Tree, callback: *const fn (*Tree) void) void {
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
        // A compaction beat is started asynchronously with `compact_io` which takes a callback.
        // After `compact_io` is called, `compact_cpu` should be called to enable pipelining.
        // The compaction beat completes when the `compact_io` callback is invoked.
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
        //      * compact immutable table if contains any sorted values (could be empty).
        //      * allow level visible table counts to overflow if needed.
        //      * start even level compactions if there's any tables to compact.
        //
        //  - (second) up beat of the measure:
        //      * finish ticking running even-level compactions.
        //      * assert: on callback completion, all compactions should be completed.
        //
        //  - (third) down beat of the measure:
        //      * assert: no compactions are currently running.
        //      * start odd level and immutable table compactions.
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
        pub fn compact(tree: *Tree, callback: *const fn (*Tree) void, op: u64) void {
            tree.compact_start(callback, op);
            tree.compact_drive();
        }

        fn compact_drive(tree: *Tree) void {
            tree.compact_io();
            // tree.manifest.manifest_log.superblock.storage.tick();
            tree.compact_cpu();
        }

        fn compact_start(tree: *Tree, callback: *const fn (*Tree) void, op: u64) void {
            assert(tree.compaction_io_pending == 0);
            assert(tree.compaction_callback == null);

            if (op > 0) assert(op > tree.compaction_op);
            tree.compaction_op = op;
            tree.compaction_callback = callback;

            const compaction_beat = tree.compaction_op % config.lsm_batch_multiple;
            const start = (compaction_beat == 0) or
                (compaction_beat == half_measure_beat_count);

            // The target snapshot of a compaction is actually the previous batch minus one.
            //
            // At the start of the current batch, mutable table inserts from the previous batch
            // would be in the immutable table. This means the current batch compaction will
            // actually be flushing to disk (levels) mutable table updates from the previous batch.
            //
            // -1 as the ops are zero based so the "last" op from previous batch is reflected.
            const snapshot = std.mem.alignBackward(op, config.lsm_batch_multiple) -| 1;
            assert(snapshot != snapshot_latest);

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
                if (start) tree.compact_io_start_table_immutable(snapshot);
            }

            // Try to start compacting the other levels.
            var it = CompactionTableIterator{ .tree = tree };
            while (it.next()) |context| {
                if (start) tree.compact_io_start_table(snapshot, context);
            }
        }

        fn compact_io_start_table_immutable(tree: *Tree, snapshot: u64) void {
            const compaction_beat = tree.compaction_op % config.lsm_batch_multiple;
            assert(compaction_beat == half_measure_beat_count);

            // Do not start compaction if the immutable table does not require compaction.
            if (tree.table_immutable.free) return;

            const values_count = tree.table_immutable.values.len;
            assert(values_count > 0);

            const level_b: u8 = 0;
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
                level_b,
                range,
                snapshot,
                .{ .table = &tree.table_immutable },
            );
        }

        fn compact_io_start_table(tree: *Tree, snapshot: u64, context: CompactionTableContext) void {
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
                context.level_b,
                table_range.range,
                snapshot,
                .{
                    .grid = tree.grid,
                    .address = table.address,
                    .checksum = table.checksum,
                },
            );
        }

        fn compact_io(tree: *Tree) void {
            assert(tree.compaction_io_pending <= 2 + tree.compaction_table.len);
            assert(tree.compaction_callback != null);

            // Try to tick the cpu portion of the immutable table compaction:
            const compaction_beat = tree.compaction_op % config.lsm_batch_multiple;
            const even_levels = compaction_beat < half_measure_beat_count;
            if (even_levels) {
                assert(tree.compaction_table_immutable.status == .idle);
            } else {
                if (tree.compaction_table_immutable.status == .compacting) {
                    tree.compact_io_tick(&tree.compaction_table_immutable);
                }
            }

            // Try to tick the cpu portion of the level compactions:
            var it = CompactionTableIterator{ .tree = tree };
            while (it.next()) |context| {
                if (context.compaction.status == .compacting) {
                    assert(context.compaction.level_b == context.level_b);
                    tree.compact_io_tick(context.compaction);
                }
            }

            // Always start one io_pending that is resolved in compact_cpu()
            // to handle the case of no level or immutable table being selected for compaction
            tree.compaction_io_pending += 1;
            assert(tree.compaction_io_pending <= 2 + tree.compaction_table.len);
        }

        fn compact_io_tick(tree: *Tree, compaction: anytype) void {
            tree.compaction_io_pending += 1;

            const compaction_beat = tree.compaction_op % config.lsm_batch_multiple;
            const even_levels = compaction_beat < half_measure_beat_count;
            assert(compaction.level_b < config.lsm_levels);
            assert(compaction.level_b % 2 == @boolToInt(even_levels));

            if (@TypeOf(compaction.*) == CompactionTableImmutable) {
                assert(compaction.level_b == 0);
                compaction.tick_io(Tree.compact_io_tick_callback_table_immutable);
                log.debug(tree_name ++ ": queued compaction for immutable table to level 0", .{});
            } else {
                compaction.tick_io(Tree.compact_io_tick_callback_table);
                log.debug(tree_name ++ ": queued compaction for level {d} to level {d}", .{
                    compaction.level_b - 1,
                    compaction.level_b,
                });
            }
        }

        fn compact_io_tick_callback_table_immutable(compaction: *CompactionTableImmutable) void {
            assert(compaction.status == .compacting or compaction.status == .done);
            assert(compaction.level_b < config.lsm_levels);
            assert(compaction.level_b == 0);

            const tree = @fieldParentPtr(Tree, "compaction_table_immutable", compaction);
            const compaction_beat = tree.compaction_op % config.lsm_batch_multiple;
            assert(compaction_beat >= half_measure_beat_count);

            log.debug(tree_name ++ ": compact_io complete for immutable table to level 0", .{});
            tree.compact_io_tick_done();
        }

        fn compact_io_tick_callback_table(compaction: *CompactionTable) void {
            assert(compaction.status == .compacting or compaction.status == .done);
            assert(compaction.level_b < config.lsm_levels);
            assert(compaction.level_b > 0);

            const table_offset = @divFloor(compaction.level_b - 1, 2);
            const table_ptr = @ptrCast([*]CompactionTable, compaction) - table_offset;

            const table_size = @divFloor(config.lsm_levels, 2);
            const table: *[table_size]CompactionTable = table_ptr[0..table_size];

            const tree = @fieldParentPtr(Tree, "compaction_table", table);
            log.debug(tree_name ++ ": compact_io complete for level {d} to level {d}", .{
                compaction.level_b - 1,
                compaction.level_b,
            });

            tree.compact_io_tick_done();
        }

        fn compact_io_tick_done(tree: *Tree) void {
            assert(tree.compaction_io_pending <= 2 + tree.compaction_table.len);
            assert(tree.compaction_callback != null);

            // compact_done() is called after all compact_io_tick()'s complete.
            // This function can be triggered asynchronously or by compact_cpu() below.
            tree.compaction_io_pending -= 1;
            if (tree.compaction_io_pending == 0) tree.compact_done();
        }

        fn compact_cpu(tree: *Tree) void {
            assert(tree.compaction_io_pending <= 2 + tree.compaction_table.len);
            assert(tree.compaction_callback != null);

            // Try to tick the cpu portion of the immutable table compaction:
            const compaction_beat = tree.compaction_op % config.lsm_batch_multiple;
            const even_levels = compaction_beat < half_measure_beat_count;
            if (even_levels) {
                assert(tree.compaction_table_immutable.status == .idle);
            } else {
                if (tree.compaction_table_immutable.status == .compacting) {
                    tree.compaction_table_immutable.tick_cpu();
                }
            }

            // Try to tick the cpu portion of the level compactions:
            var it = CompactionTableIterator{ .tree = tree };
            while (it.next()) |context| {
                if (context.compaction.status == .compacting) {
                    assert(context.compaction.level_b == context.level_b);
                    context.compaction.tick_cpu();
                }
            }

            // Resolve the io_pending added by compact_io(). This may trigger compact_done().
            tree.compact_io_tick_done();
        }

        fn compact_done(tree: *Tree) void {
            assert(tree.compaction_io_pending == 0);
            assert(tree.compaction_callback != null);

            var still_compacting = false;

            // Mark immutable compaction that reported done in their callback as "completed".
            const compaction_beat = tree.compaction_op % config.lsm_batch_multiple;
            const even_levels = compaction_beat < half_measure_beat_count;
            if (even_levels) {
                assert(tree.compaction_table_immutable.status == .idle);
            } else {
                if (tree.compaction_table_immutable.status == .done) {
                    tree.compaction_table_immutable.reset();
                    tree.table_immutable.clear();
                } else if (tree.compaction_table_immutable.status == .compacting) {
                    still_compacting = true;
                }
            }

            // Mark compactions that reported done in their callback as "completed" (done = null).
            var it = CompactionTableIterator{ .tree = tree };
            while (it.next()) |context| {
                if (context.compaction.status == .done) {
                    assert(context.compaction.level_b == context.level_b);
                    context.compaction.reset();
                } else if (context.compaction.status == .compacting) {
                    still_compacting = true;
                }
            }

            // At the end of every beat, ensure mutable table can be flushed to immutable table.
            assert(tree.table_mutable.can_commit_batch(tree.options.commit_count_max));

            // At end of second/half measure:
            // - assert: even compactions from previous tick are finished.
            // - remove tables made invisible during compaction of even levels.
            if (compaction_beat == half_measure_beat_count - 1) {
                if (still_compacting) {
                    log.debug(tree_name ++ ": compact_done: driving outstanding compactions", .{});
                    return tree.compact_drive();
                }

                log.debug(tree_name ++ ": compact_done: compacted even levels", .{});

                it = CompactionTableIterator{ .tree = tree };
                while (it.next()) |context| {
                    assert(context.compaction.status == .idle);
                    tree.manifest.remove_invisible_tables(
                        context.level_a,
                        context.compaction.snapshot,
                        context.compaction.range.key_min,
                        context.compaction.range.key_max,
                    );
                }
            }

            // At end of fourth/last measure:
            // - assert: immutable table and odd level compactions from previous tick are finished.
            // - remove tables made invisible during compaction of odd levels.
            // - assert: all visible levels haven't overflowed their max.
            // - convert mutable table to immutable tables for next measure.
            if (compaction_beat == config.lsm_batch_multiple - 1) {
                if (still_compacting) {
                    log.debug(tree_name ++ ": compact_done: driving outstanding compactions", .{});
                    return tree.compact_drive();
                }

                // TODO Make log message more accurate according to what was compacted.
                log.debug(tree_name ++ ": compact_done: compacted immutable table and odd levels", .{});

                assert(tree.compaction_table_immutable.status == .idle);
                it = CompactionTableIterator{ .tree = tree };
                while (it.next()) |context| {
                    assert(context.compaction.status == .idle);
                    tree.manifest.remove_invisible_tables(
                        context.level_a,
                        context.compaction.snapshot,
                        context.compaction.range.key_min,
                        context.compaction.range.key_max,
                    );
                }

                tree.manifest.assert_level_table_counts();
                tree.compact_mutable_table_into_immutable();
            }

            // At the end of every beat, call manifest.compact before invoking the compact callback.
            tree.manifest.compact(compact_manifest_callback);
        }

        fn compact_manifest_callback(manifest: *Manifest) void {
            const tree = @fieldParentPtr(Tree, "manifest", manifest);
            assert(tree.compaction_io_pending == 0);
            assert(tree.compaction_callback != null);

            // Invoke the compact_io() callback after the manifest compacts at the end of the beat.
            const callback = tree.compaction_callback.?;
            tree.compaction_callback = null;
            callback(tree);
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

        pub fn checkpoint(tree: *Tree, callback: *const fn (*Tree) void) void {
            // Assert no outstanding compact_io() work..
            assert(tree.compaction_io_pending == 0);
            assert(tree.compaction_callback == null);

            // Avoid checkpointing if this is not the last beat in the compaction measure.
            const compaction_beat = tree.compaction_op % config.lsm_batch_multiple;
            const last_beat_in_measure = config.lsm_batch_multiple - 1;
            if (compaction_beat != last_beat_in_measure) return callback(tree);

            // Assert no outstanding compactions.
            assert(tree.compaction_table_immutable.status == .idle);
            for (tree.compaction_table) |*compaction| {
                assert(compaction.status == .idle);
            }

            // Assert all manifest levels haven't overflowed their table counts.
            tree.manifest.assert_level_table_counts();

            // Assert that we're checkpointing only after invisible tables have been removed.
            if (config.verify) {
                tree.manifest.assert_no_invisible_tables(tree.compaction_op);
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

            pub fn next(callback: *const fn (result: ?Value) void) void {
                _ = callback;
            }
        };

        pub fn range_query(
            tree: *Tree,
            /// The snapshot timestamp, if any
            snapshot: u64,
            query: RangeQuery,
        ) RangeQueryIterator {
            _ = tree;
            _ = snapshot;
            _ = query;
        }
    };
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
    const commit_count_max = @divFloor(batch_size_max, 128);

    var sort_buffer = try allocator.allocAdvanced(
        u8,
        16,
        // This must be the greatest commit_count_max and value_size across trees:
        commit_count_max * config.lsm_batch_multiple * 128,
        .exact,
    );
    defer allocator.free(sort_buffer);

    // TODO Initialize SuperBlock:
    var superblock: SuperBlockType(Storage) = undefined;

    var grid = try Grid.init(allocator, &superblock);
    defer grid.deinit(allocator);

    var tree = try Tree.init(
        allocator,
        &node_pool,
        &grid,
        &value_cache,
        .{
            .prefetch_count_max = commit_count_max * 2,
            .commit_count_max = commit_count_max,
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
    _ = tree.compact_io;
    _ = tree.compact_cpu;

    _ = Tree.Manifest.LookupIterator.next;
    _ = tree.manifest;
    _ = tree.manifest.lookup;
    _ = tree.manifest.insert_tables;

    std.debug.print("table_count_max={}\n", .{table_count_max});
}
