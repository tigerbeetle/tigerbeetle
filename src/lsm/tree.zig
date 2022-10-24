//! An LSM tree.
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
const bloom_filter = @import("bloom_filter.zig");

const NodePool = @import("node_pool.zig").NodePool(config.lsm_manifest_node_size, 16);
const RingBuffer = @import("../ring_buffer.zig").RingBuffer;

/// We reserve maxInt(u64) to indicate that a table has not been deleted.
/// Tables that have not been deleted have snapshot_max of maxInt(u64).
/// Since we ensure and assert that a query snapshot never exactly matches
/// the snapshot_min/snapshot_max of a table, we must use maxInt(u64) - 1
/// to query all non-deleted tables.
pub const snapshot_latest: u64 = math.maxInt(u64) - 1;

const half_bar_beat_count = @divExact(config.lsm_batch_multiple, 2);

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

        /// While a compaction is running, this is the op of the last compact().
        /// While no compaction is running, this is the op of the last compact() to complete.
        /// (When recovering from a checkpoint, compaction_op starts at op_checkpoint).
        compaction_op: u64,

        /// The maximum snapshot which is safe to prefetch from.
        /// The minimum snapshot which can see the mutable table.
        ///
        /// This field ensures that the tree never queries the output tables of a running
        /// compaction; they are incomplete.
        ///
        /// See lookup_snapshot_max_for_checkpoint().
        ///
        /// Invariants:
        /// * `lookup_snapshot_max = compaction_op` while any compaction beat is in progress.
        /// * `lookup_snapshot_max = compaction_op + 1` after a compaction beat finishes.
        /// * `lookup_snapshot_max ≥ op_checkpoint + 1 + lsm_batch_multiple`
        ///    when `op_checkpoint ≠ 0`.
        lookup_snapshot_max: u64,

        compaction_io_pending: usize,
        compaction_callback: ?*const fn (*Tree) void,

        checkpoint_callback: ?*const fn (*Tree) void,
        open_callback: ?*const fn (*Tree) void,

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

            // Compaction is one bar ahead of superblock's commit_min.
            const op_checkpoint = grid.superblock.working.vsr_state.commit_min;
            const lookup_snapshot_max = lookup_snapshot_max_for_checkpoint(op_checkpoint);
            const compaction_op = op_checkpoint;

            return Tree{
                .grid = grid,
                .options = options,
                .table_mutable = table_mutable,
                .table_immutable = table_immutable,
                .manifest = manifest,
                .compaction_table_immutable = compaction_table_immutable,
                .compaction_table = compaction_table,
                .compaction_op = compaction_op,
                .lookup_snapshot_max = lookup_snapshot_max,
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
        pub fn lookup_from_memory(tree: *Tree, snapshot: u64, key: Key) ?*const Value {
            assert(tree.lookup_snapshot_max >= snapshot);

            if (tree.lookup_snapshot_max == snapshot) {
                if (tree.table_mutable.get(key)) |value| return value;
            } else {
                // The mutable table is converted to an immutable table when a snapshot is created.
                // This means that a past snapshot will never be able to see the mutable table.
                // This simplifies the mutable table and eliminates compaction for duplicate puts.
            }

            if (!tree.table_immutable.free and tree.table_immutable.snapshot_min <= snapshot) {
                if (tree.table_immutable.get(key)) |value| return value;
            } else {
                // If the immutable table is invisible, then the mutable table is also invisible.
                assert(tree.table_immutable.free or snapshot != tree.lookup_snapshot_max);
            }

            return null;
        }

        /// Call this function only after checking `lookup_from_memory()`.
        pub fn lookup_from_levels(
            tree: *Tree,
            callback: *const fn (*LookupContext, ?*const Value) void,
            context: *LookupContext,
            snapshot: u64,
            key: Key,
        ) void {
            assert(tree.lookup_snapshot_max >= snapshot);
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

            callback: *const fn (*Tree.LookupContext, ?*const Value) void,

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
        pub inline fn unwrap_tombstone(value: ?*const Value) ?*const Value {
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
                const even_levels = compaction_beat < half_bar_beat_count;
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
        /// We therefore take the bar, during which all compactions run, and divide by two,
        /// running the compactions from even levels in the first half bar, and then the odd.
        ///
        /// Compactions start on the down beat of a half bar, using 0-based beats.
        /// For example, if there are 4 beats in a bar, start on beat 0 or beat 2.
        pub fn compact(tree: *Tree, callback: *const fn (*Tree) void, op: u64) void {
            assert(tree.compaction_callback == null);
            assert(op != 0);
            assert(op == tree.compaction_op + 1);
            assert(op > tree.grid.superblock.working.vsr_state.commit_min);

            tree.compaction_op = op;

            if (tree.grid.superblock.working.vsr_state.op_compacted(op)) {
                // We recovered from a checkpoint, and must avoid replaying one bar of
                // compactions that were applied before the checkpoint. Repeating these ops'
                // compactions would actually perform different compactions than before,
                // causing the storage state of the replica to diverge from the cluster.
                // See also: lookup_snapshot_max_for_checkpoint().

                if (tree.compaction_op + 1 == tree.lookup_snapshot_max) {
                    // This is the last op of the skipped compaction bar.
                    // Prepare the immutable table for the next bar — since this state is
                    // in-memory, it cannot be skipped.
                    tree.compact_mutable_table_into_immutable();
                }

                // TODO Defer this callback until tick() to avoid stack growth.
                callback(tree);
                return;
            }
            assert(op == tree.lookup_snapshot_max);

            tree.compact_start(callback);
            tree.compact_drive();
        }

        fn compact_start(tree: *Tree, callback: *const fn (*Tree) void) void {
            assert(tree.compaction_io_pending == 0);
            assert(tree.compaction_callback == null);

            tree.compaction_callback = callback;

            const compaction_beat = tree.compaction_op % config.lsm_batch_multiple;
            const start = (compaction_beat == 0) or
                (compaction_beat == half_bar_beat_count);

            const op_min = compaction_op_min(tree.compaction_op);
            assert(op_min < snapshot_latest);
            assert(op_min % half_bar_beat_count == 0);

            log.debug(tree_name ++ ": compact_start: op={d} op_min={d} beat={d}/{d}", .{
                tree.compaction_op,
                op_min,
                compaction_beat + 1,
                config.lsm_batch_multiple,
            });

            // Try to start compacting the immutable table.
            const even_levels = compaction_beat < half_bar_beat_count;
            if (even_levels) {
                assert(tree.compaction_table_immutable.status == .idle);
            } else {
                if (start) tree.compact_start_table_immutable(op_min);
            }

            // Try to start compacting the other levels.
            var it = CompactionTableIterator{ .tree = tree };
            while (it.next()) |context| {
                if (start) tree.compact_start_table(op_min, context);
            }
        }

        fn compact_start_table_immutable(tree: *Tree, op_min: u64) void {
            const compaction_beat = tree.compaction_op % config.lsm_batch_multiple;
            assert(compaction_beat == half_bar_beat_count);

            // Do not start compaction if the immutable table does not require compaction.
            if (tree.table_immutable.free) return;

            assert(tree.table_immutable.snapshot_min % half_bar_beat_count == 0);

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
                "(values.len={d} snapshot_min={d} compaction.op_min={d} table_count={d})", .{
                tree.table_immutable.values.len,
                tree.table_immutable.snapshot_min,
                op_min,
                range.table_count,
            });

            tree.compaction_table_immutable.start(
                tree.grid,
                &tree.manifest,
                op_min,
                range,
                table_a,
                level_b,
                .{ .table = &tree.table_immutable },
            );
        }

        fn compact_start_table(tree: *Tree, op_min: u64, context: CompactionTableContext) void {
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
                op_min,
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
            const even_levels = compaction_beat < half_bar_beat_count;
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
            const even_levels = compaction_beat < half_bar_beat_count;
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
            assert(compaction_beat >= half_bar_beat_count);

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
            assert(tree.compaction_op == tree.lookup_snapshot_max);

            const compaction_beat = tree.compaction_op % config.lsm_batch_multiple;
            const even_levels = compaction_beat < half_bar_beat_count;
            const compacted_levels_even = compaction_beat == half_bar_beat_count - 1;
            const compacted_levels_odd = compaction_beat == config.lsm_batch_multiple - 1;
            if (!compacted_levels_even and !compacted_levels_odd) {
                // TODO(Deterministic Beats): Remove this when compact_done() is called exactly
                // once when the beat finishes.
                tree.lookup_snapshot_max = tree.compaction_op + 1;

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
                // We are at the end of a half-bar, but the compactions have not finished.
                // We keep ticking them until they finish.
                log.debug(tree_name ++ ": compact_done: driving outstanding compactions", .{});
                tree.compact_drive();
                return;
            }

            // TODO(Deterministic Beats): Move this to the top of the function when compact_done()
            // is called exactly once when the beat finishes.
            tree.lookup_snapshot_max = tree.compaction_op + 1;

            // All compactions have finished for the current half-bar.
            // We couldn't remove the (invisible) input tables until now because prefetch()
            // needs a complete set of tables for lookups to avoid missing data.

            // Reset the immutable table Compaction.
            // Also clear any tables made invisible by the compaction.
            if (!even_levels) {
                switch (tree.compaction_table_immutable.status) {
                    // The compaction wasn't started for this half bar.
                    .idle => assert(tree.table_immutable.free),
                    .processing => unreachable,
                    .done => {
                        tree.compaction_table_immutable.reset();
                        tree.table_immutable.clear();
                        tree.manifest.remove_invisible_tables(
                            tree.compaction_table_immutable.level_b,
                            tree.lookup_snapshot_max,
                            tree.compaction_table_immutable.range.key_min,
                            tree.compaction_table_immutable.range.key_max,
                        );
                    },
                }
            }

            // Reset all the other Compactions.
            // Also clear any tables made invisible by the compactions.
            var it = CompactionTableIterator{ .tree = tree };
            while (it.next()) |context| {
                switch (context.compaction.status) {
                    .idle => {}, // The compaction wasn't started for this half bar.
                    .processing => unreachable,
                    .done => {
                        context.compaction.reset();
                        tree.manifest.remove_invisible_tables(
                            context.compaction.level_b,
                            tree.lookup_snapshot_max,
                            context.compaction.range.key_min,
                            context.compaction.range.key_max,
                        );
                        if (context.compaction.level_b > 0) {
                            tree.manifest.remove_invisible_tables(
                                context.compaction.level_b - 1,
                                tree.lookup_snapshot_max,
                                context.compaction.range.key_min,
                                context.compaction.range.key_max,
                            );
                        }
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
            // - Convert mutable table to immutable table for next bar.
            if (compacted_levels_odd) {
                tree.manifest.assert_level_table_counts();
                tree.compact_mutable_table_into_immutable();
            }

            // At the end of the second/fourth beat:
            // - Compact the manifest before invoking the compact() callback.
            tree.manifest.compact(compact_manifest_callback);
        }

        /// Called after the last beat of a full compaction bar.
        fn compact_mutable_table_into_immutable(tree: *Tree) void {
            assert(tree.table_immutable.free);
            assert((tree.compaction_op + 1) % config.lsm_batch_multiple == 0);
            assert(tree.compaction_op + 1 == tree.lookup_snapshot_max);

            if (tree.table_mutable.count() == 0) return;

            // Sort the mutable table values directly into the immutable table's array.
            const values_max = tree.table_immutable.values_max();
            const values = tree.table_mutable.sort_into_values_and_clear(values_max);
            assert(values.ptr == values_max.ptr);

            // The immutable table must be visible to the next bar — setting its snapshot_min to
            // lookup_snapshot_max guarantees.
            //
            // In addition, the immutable table is conceptually an output table of this compaction
            // bar, and now its snapshot_min matches the snapshot_min of the Compactions' output
            // tables.
            tree.table_immutable.reset_with_sorted_values(tree.lookup_snapshot_max, values);

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

        pub fn checkpoint(tree: *Tree, callback: *const fn (*Tree) void) void {
            // Assert no outstanding compact_tick() work..
            assert(tree.compaction_io_pending == 0);
            assert(tree.compaction_callback == null);
            assert(tree.compaction_op > 0);
            assert(tree.compaction_op + 1 == tree.lookup_snapshot_max);

            // Assert that this is the last beat in the compaction bar.
            const compaction_beat = tree.compaction_op % config.lsm_batch_multiple;
            const last_beat_in_bar = config.lsm_batch_multiple - 1;
            assert(last_beat_in_bar == compaction_beat);

            // Assert no outstanding compactions.
            assert(tree.compaction_table_immutable.status == .idle);
            for (tree.compaction_table) |*compaction| {
                assert(compaction.status == .idle);
            }

            // Assert all manifest levels haven't overflowed their table counts.
            tree.manifest.assert_level_table_counts();

            // Assert that we're checkpointing only after invisible tables have been removed.
            if (config.verify) {
                tree.manifest.assert_no_invisible_tables(compaction_op_min(tree.compaction_op));
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
            snapshot: ?u64,
            query: RangeQuery,
        ) RangeQueryIterator {
            _ = tree;
            _ = snapshot;
            _ = query;
        }
    };
}

/// Returns the first op of the compaction (Compaction.op_min) for a given op/beat.
///
/// After this compaction finishes:
/// - `op_min + half_bar_beat_count - 1` will be the input tables' snapshot_max.
/// - `op_min + half_bar_beat_count` will be the output tables' snapshot_min.
///
/// Each half-bar has a separate op_min (for deriving the output snapshot_min) instead of each full
/// bar because this allows the output tables of the first half-bar's compaction to be prefetched
/// against earlier — hopefully while they are still warm in the cache from being written.
pub fn compaction_op_min(op: u64) u64 {
    return op - op % half_bar_beat_count;
}

/// These charts depict the commit/compact ops and `lookup_snapshot_max` over a series of
/// commits and compactions (with lsm_batch_multiple=8).
///
/// Legend:
///
///   ┼   full bar (first half-bar start)
///   ┬   half bar (second half-bar start)
///   $   lookup_snapshot_max (prefetch reads from the current snapshot)
///       This is incremented at the end of each compact().
///   .   op is in mutable table (in memory)
///   ,   op is in immutable table (in memory)
///   #   op is on disk
///   ✓   checkpoint() may follow compact()
///
///   0 2 4 6 8 0 2 4 6
///   ┼───┬───┼───┬───┼
///   .$      ╷       ╷     init(superblock.commit_min=0)⎤ Compaction is effectively a noop for the
///   .$      ╷       ╷     commit;compact( 1) start/end ⎥ first bar because there are no tables on
///   ..$     ╷       ╷     commit;compact( 2) start/end ⎥ disk yet, and no immutable table to
///   ...$    ╷       ╷     commit;compact( 3) start/end ⎥ flush.
///   ....$   ╷       ╷     commit;compact( 4) start/end ⎥
///   .....$  ╷       ╷     commit;compact( 5) start/end ⎥ This applies:
///   ......$ ╷       ╷     commit;compact( 6) start/end ⎥ - when the LSM is starting on a freshly
///   .......$╷       ╷     commit;compact( 7) start    ⎤⎥   formatted data file, and also
///   ,,,,,,,,$       ╷  ✓         compact( 7)       end⎦⎦ - when the LSM is recovering from a crash
///   ,,,,,,,,$       ╷     commit;compact( 8) start/end     (see below).
///   ,,,,,,,,.$      ╷     commit;compact( 9) start/end
///   ,,,,,,,,..$     ╷     commit;compact(10) start/end
///   ,,,,,,,,...$    ╷     commit;compact(11) start/end
///   ,,,,,,,,....$   ╷     commit;compact(12) start/end
///   ,,,,,,,,.....$  ╷     commit;compact(13) start/end
///   ,,,,,,,,......$ ╷     commit;compact(14) start/end
///   ,,,,,,,,.......$╷     commit;compact(15) start    ⎤
///   ########,,,,,,,,$  ✓         compact(15)       end⎦
///   ########,,,,,,,,$     commit;compact(16) start/end
///   ┼───┬───┼───┬───┼
///   0 2 4 6 8 0 2 4 6
///   ┼───┬───┼───┬───┼                                    Recover with a checkpoint taken at op 15.
///   ########        $     init(superblock.commit_min=7)  At op 15, ops 8…15 are in memory, so they
///   ########.       $     commit        ( 8) start/end ⎤ were dropped by the crash.
///   ########..      $     commit        ( 9) start/end ⎥
///   ########...     $     commit        (10) start/end ⎥ But compaction is not run for ops 8…15
///   ########....    $     commit        (11) start/end ⎥ because it was already performed
///   ########.....   $     commit        (12) start/end ⎥ before the checkpoint.
///   ########......  $     commit        (13) start/end ⎥
///   ########....... $     commit        (14) start/end ⎥ We can begin to compact again at op 16,
///   ########........$     commit        (15) start    ⎤⎥ because those compactions (if previously
///   ########,,,,,,,,$  ✓                (15)       end⎦⎦ performed) are not included in the
///   ########,,,,,,,,$     commit;compact(16) start/end   checkpoint.
///   ┼───┬───┼───┬───┼
///   0 2 4 6 8 0 2 4 6
///
/// Notice how in the checkpoint recovery example above, we are careful not to `compact(op)` twice
/// for any op (even if we crash/recover), since that could lead to differences between replicas'
/// storage. The last bar of `commit()`s is always only in memory, so it is safe to repeat.
///
/// Additionally, while skipping compactions during recovery, we use a `lookup_snapshot_max`
/// different than the original compactions — the old tables may have been removed during the
/// checkpoint.
fn lookup_snapshot_max_for_checkpoint(op_checkpoint: u64) u64 {
    if (op_checkpoint == 0) {
        // Start from 1 because we never commit op 0.
        return 1;
    } else {
        return op_checkpoint + config.lsm_batch_multiple + 1;
    }
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
