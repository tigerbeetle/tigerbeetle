//! An LSM tree.
const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;
const os = std.os;

const log = std.log.scoped(.tree);
const tracer = @import("../tracer.zig");

const constants = @import("../constants.zig");
const div_ceil = @import("../stdx.zig").div_ceil;
const eytzinger = @import("eytzinger.zig").eytzinger;
const vsr = @import("../vsr.zig");
const bloom_filter = @import("bloom_filter.zig");

const CompositeKey = @import("composite_key.zig").CompositeKey;
const NodePool = @import("node_pool.zig").NodePool(constants.lsm_manifest_node_size, 16);
const RingBuffer = @import("../ring_buffer.zig").RingBuffer;

/// We reserve maxInt(u64) to indicate that a table has not been deleted.
/// Tables that have not been deleted have snapshot_max of maxInt(u64).
/// Since we ensure and assert that a query snapshot never exactly matches
/// the snapshot_min/snapshot_max of a table, we must use maxInt(u64) - 1
/// to query all non-deleted tables.
pub const snapshot_latest: u64 = math.maxInt(u64) - 1;

const half_bar_beat_count = @divExact(constants.lsm_batch_multiple, 2);

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
pub const table_count_max = table_count_max_for_tree(
    constants.lsm_growth_factor,
    constants.lsm_levels,
);

/// The upper-bound count of input tables to a single tree's compaction.
///
/// - +1 from level A.
/// - +lsm_growth_factor from level B. The A-input table cannot overlap with an extra B-input table
///   because input table selection is least-overlap. If the input table overlaps on one or both
///   edges, there must be another table with less overlap to select.
pub const compaction_tables_input_max = 1 + constants.lsm_growth_factor;

/// The upper-bound count of output tables from a single tree's compaction.
/// In the "worst" case, no keys are overwritten/merged, and no tombstones are dropped.
pub const compaction_tables_output_max = compaction_tables_input_max;

/// The maximum number of concurrent compactions (per tree).
pub const compactions_max = div_ceil(constants.lsm_levels, 2);

pub fn TreeType(
    comptime TreeTable: type,
    comptime Storage: type,
    comptime tree_name: [:0]const u8,
) type {
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
        pub const TableMutable = @import("table_mutable.zig").TableMutableType(Table, tree_name);
        const TableImmutable = @import("table_immutable.zig").TableImmutableType(Table);

        const CompactionType = @import("compaction.zig").CompactionType;
        const Compaction = CompactionType(Table, Tree, Storage, tree_name);

        grid: *Grid,
        options: Options,

        table_mutable: TableMutable,
        table_immutable: TableImmutable,
        values_cache: ?*TableMutable.ValuesCache,

        manifest: Manifest,

        compaction_table_immutable: Compaction,

        /// The number of Compaction instances is divided by two as, at any given compaction tick,
        /// we're only compacting either even or odd levels but never both.
        /// Uses divFloor as the last level, even with odd lsm_levels, doesn't compact to anything.
        /// (e.g. floor(5/2) = 2 for levels 0->1, 2->3 when even and immut->0, 1->2, 3->4 when odd).
        /// This means, that for odd lsm_levels, the last CompactionTable is unused.
        compaction_table: [@divFloor(constants.lsm_levels, 2)]Compaction,

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
        compaction_callback: union(enum) {
            none,
            /// We're at the end of a half-bar.
            /// Call this callback when all current compactions finish.
            awaiting: fn (*Tree) void,
            /// We're at the end of some other beat.
            /// Call this on the next tick.
            next_tick: fn (*Tree) void,
        },
        compaction_next_tick: Grid.NextTick = undefined,

        checkpoint_callback: ?fn (*Tree) void,
        open_callback: ?fn (*Tree) void,

        tracer_slot: ?tracer.SpanStart = null,
        filter_block_hits: u64 = 0,
        filter_block_misses: u64 = 0,

        pub const Options = struct {
            /// The number of objects to cache in the set-associative value cache.
            cache_entries_max: u32 = 0,
        };

        pub fn init(
            allocator: mem.Allocator,
            node_pool: *NodePool,
            grid: *Grid,
            options: Options,
        ) !Tree {
            assert(grid.superblock.opened);

            var values_cache: ?*TableMutable.ValuesCache = null;

            if (options.cache_entries_max > 0) {
                // Cache is heap-allocated to pass a pointer into the mutable table.
                values_cache = try allocator.create(TableMutable.ValuesCache);
            }
            errdefer if (values_cache) |c| allocator.destroy(c);

            if (options.cache_entries_max > 0) {
                values_cache.?.* = try TableMutable.ValuesCache.init(
                    allocator,
                    options.cache_entries_max,
                );
            }
            errdefer if (values_cache) |c| c.deinit(allocator);

            var table_mutable = try TableMutable.init(allocator, values_cache);
            errdefer table_mutable.deinit(allocator);

            var table_immutable = try TableImmutable.init(allocator);
            errdefer table_immutable.deinit(allocator);

            var manifest = try Manifest.init(allocator, node_pool, grid, tree_hash);
            errdefer manifest.deinit(allocator);

            var compaction_table_immutable = try Compaction.init(allocator);
            errdefer compaction_table_immutable.deinit(allocator);

            var compaction_table: [@divFloor(constants.lsm_levels, 2)]Compaction = undefined;
            {
                comptime var i: usize = 0;
                inline while (i < compaction_table.len) : (i += 1) {
                    errdefer for (compaction_table[0..i]) |*c| c.deinit(allocator);
                    compaction_table[i] = try Compaction.init(allocator);
                }
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
                .values_cache = values_cache,
                .manifest = manifest,
                .compaction_table_immutable = compaction_table_immutable,
                .compaction_table = compaction_table,
                .compaction_op = compaction_op,
                .lookup_snapshot_max = lookup_snapshot_max,
                .compaction_io_pending = 0,
                .compaction_callback = .none,
                .checkpoint_callback = null,
                .open_callback = null,
            };
        }

        pub fn deinit(tree: *Tree, allocator: mem.Allocator) void {
            assert(tree.tracer_slot == null);

            tree.compaction_table_immutable.deinit(allocator);
            for (tree.compaction_table) |*compaction| compaction.deinit(allocator);

            // TODO Consider whether we should release blocks acquired from Grid.block_free_set.
            tree.table_mutable.deinit(allocator);
            tree.table_immutable.deinit(allocator);
            tree.manifest.deinit(allocator);

            if (tree.values_cache) |cache| {
                cache.deinit(allocator);
                allocator.destroy(cache);
            }
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
            callback: fn (*LookupContext, ?*const Value) void,
            context: *LookupContext,
            snapshot: u64,
            key: Key,
        ) void {
            assert(tree.lookup_snapshot_max >= snapshot);
            if (constants.verify) {
                // The caller is responsible for checking the mutable table.
                assert(tree.lookup_from_memory(snapshot, key) == null);
            }

            var index_block_count: u8 = 0;
            var index_block_addresses: [constants.lsm_levels]u64 = undefined;
            var index_block_checksums: [constants.lsm_levels]u128 = undefined;
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
            index_block_addresses: [constants.lsm_levels]u64,
            index_block_checksums: [constants.lsm_levels]u128,

            data_block: ?struct {
                address: u64,
                checksum: u128,
            } = null,

            callback: fn (*Tree.LookupContext, ?*const Value) void,

            fn read_index_block(context: *LookupContext) void {
                assert(context.data_block == null);
                assert(context.index_block < context.index_block_count);
                assert(context.index_block_count > 0);
                assert(context.index_block_count <= constants.lsm_levels);

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
                assert(context.index_block_count <= constants.lsm_levels);

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
                assert(context.index_block_count <= constants.lsm_levels);

                const filter_bytes = Table.filter_block_filter_const(filter_block);
                if (bloom_filter.may_contain(context.fingerprint, filter_bytes)) {
                    context.tree.filter_block_hits += 1;
                    tracer.plot(
                        .{ .filter_block_hits = .{ .tree_name = tree_name } },
                        @intToFloat(f64, context.tree.filter_block_hits),
                    );

                    context.tree.grid.read_block(
                        read_data_block_callback,
                        completion,
                        context.data_block.?.address,
                        context.data_block.?.checksum,
                        .data,
                    );
                } else {
                    context.tree.filter_block_misses += 1;
                    tracer.plot(
                        .{ .filter_block_misses = .{ .tree_name = tree_name } },
                        @intToFloat(f64, context.tree.filter_block_misses),
                    );

                    // The key is not present in this table, check the next level.
                    context.advance_to_next_level();
                }
            }

            fn read_data_block_callback(completion: *Read, data_block: BlockPtrConst) void {
                const context = @fieldParentPtr(LookupContext, "completion", completion);
                assert(context.data_block != null);
                assert(context.index_block < context.index_block_count);
                assert(context.index_block_count > 0);
                assert(context.index_block_count <= constants.lsm_levels);

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
                assert(context.index_block_count <= constants.lsm_levels);

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

        const CompactionTableContext = struct {
            compaction: *Compaction,
            index: u8,
            level_a: u8,
            level_b: u8,
        };

        const CompactionTableIterator = struct {
            tree: *Tree,
            index: u8 = 0,

            fn next(it: *CompactionTableIterator) ?CompactionTableContext {
                const compaction_beat = it.tree.compaction_op % constants.lsm_batch_multiple;
                const even_levels = compaction_beat < half_bar_beat_count;
                const level_a = (it.index * 2) + @boolToInt(!even_levels);
                const level_b = level_a + 1;

                if (level_a >= constants.lsm_levels - 1) return null;
                assert(level_b < constants.lsm_levels);

                defer it.index += 1;
                return CompactionTableContext{
                    .compaction = &it.tree.compaction_table[it.index],
                    .index = it.index,
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
        pub fn compact(tree: *Tree, callback: fn (*Tree) void, op: u64) void {
            assert(tree.compaction_callback == .none);
            assert(op != 0);
            assert(op == tree.compaction_op + 1);
            assert(op > tree.grid.superblock.working.vsr_state.commit_min);

            tracer.start(
                &tree.tracer_slot,
                .{ .tree_compaction_beat = .{ .tree_name = tree_name } },
                @src(),
            );

            tree.compaction_op = op;

            if (op < constants.lsm_batch_multiple) {
                // There is nothing to compact for the first measure.
                // We skip the main compaction code path first compaction bar entirely because it
                // is a special case — its first beat is 1, not 0.

                tree.lookup_snapshot_max = op + 1;
                if (op + 1 == constants.lsm_batch_multiple) {
                    tree.compaction_callback = .{ .next_tick = callback };
                    tree.compact_mutable_table_into_immutable(compact_finish_next_tick);
                    return;
                }

                tree.compaction_callback = .{ .next_tick = callback };
                tree.grid.on_next_tick(compact_finish_next_tick, &tree.compaction_next_tick, .main_thread);
                return;
            }

            if (tree.grid.superblock.working.vsr_state.op_compacted(op)) {
                // We recovered from a checkpoint, and must avoid replaying one bar of
                // compactions that were applied before the checkpoint. Repeating these ops'
                // compactions would actually perform different compactions than before,
                // causing the storage state of the replica to diverge from the cluster.
                // See also: lookup_snapshot_max_for_checkpoint().

                if (op + 1 == tree.lookup_snapshot_max) {
                    // This is the last op of the skipped compaction bar.
                    // Prepare the immutable table for the next bar — since this state is
                    // in-memory, it cannot be skipped.
                    tree.compaction_callback = .{ .next_tick = callback };
                    tree.compact_mutable_table_into_immutable(compact_finish_next_tick);
                    return;
                }

                tree.compaction_callback = .{ .next_tick = callback };
                tree.grid.on_next_tick(compact_finish_next_tick, &tree.compaction_next_tick, .main_thread);
                return;
            }
            assert(op == tree.lookup_snapshot_max);

            const op_min = compaction_op_min(tree.compaction_op);
            assert(op_min < snapshot_latest);
            assert(op_min % half_bar_beat_count == 0);

            const compaction_beat = tree.compaction_op % constants.lsm_batch_multiple;
            log.debug(tree_name ++ ": compact: op={d} op_min={d} beat={d}/{d}", .{
                tree.compaction_op,
                op_min,
                compaction_beat + 1,
                constants.lsm_batch_multiple,
            });

            const BeatKind = enum { half_bar_start, half_bar_middle, half_bar_end };
            const beat_kind = if (compaction_beat == 0 or
                compaction_beat == half_bar_beat_count)
                BeatKind.half_bar_start
            else if (compaction_beat == half_bar_beat_count - 1 or
                compaction_beat == constants.lsm_batch_multiple - 1)
                BeatKind.half_bar_end
            else
                BeatKind.half_bar_middle;

            switch (beat_kind) {
                .half_bar_start => {
                    if (constants.verify) {
                        tree.manifest.verify(tree.compaction_op);
                    }

                    tree.manifest.reserve();

                    // Maybe start compacting the immutable table.
                    const even_levels = compaction_beat < half_bar_beat_count;
                    if (even_levels) {
                        assert(tree.compaction_table_immutable.state == .idle);
                    } else {
                        tree.compact_start_table_immutable(op_min);
                    }

                    // Maybe start compacting the other levels.
                    var it = CompactionTableIterator{ .tree = tree };
                    while (it.next()) |context| {
                        tree.compact_start_table(op_min, context);
                    }

                    tree.lookup_snapshot_max = tree.compaction_op + 1;

                    tree.compaction_callback = .{ .next_tick = callback };
                    tree.grid.on_next_tick(compact_finish_next_tick, &tree.compaction_next_tick, .main_thread);
                },
                .half_bar_middle => {
                    tree.lookup_snapshot_max = tree.compaction_op + 1;

                    tree.compaction_callback = .{ .next_tick = callback };
                    tree.grid.on_next_tick(compact_finish_next_tick, &tree.compaction_next_tick, .main_thread);
                },
                .half_bar_end => {
                    // At the end of a half-bar, we have to wait for all compactions to finish.
                    // (We'll update `tree.lookup_snapshot_max` in `compact_finish_join`.)
                    tree.compaction_callback = .{ .awaiting = callback };
                    tree.compact_finish_join();
                },
            }
        }

        fn compact_start_table_immutable(tree: *Tree, op_min: u64) void {
            const compaction_beat = tree.compaction_op % constants.lsm_batch_multiple;
            assert(compaction_beat == half_bar_beat_count);

            // Do not start compaction if the immutable table does not require compaction.
            if (tree.table_immutable.free) return;

            assert(tree.table_immutable.snapshot_min % half_bar_beat_count == 0);

            const values_count = tree.table_immutable.values.len;
            assert(values_count > 0);

            const level_b: u8 = 0;
            const range = tree.manifest.compaction_range(
                level_b,
                tree.table_immutable.key_min(),
                tree.table_immutable.key_max(),
            );

            assert(range.table_count >= 1);
            assert(range.table_count <= compaction_tables_input_max);
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

            tree.compaction_io_pending += 1;
            tree.compaction_table_immutable.start(.{
                .grid = tree.grid,
                .tree = tree,
                .op_min = op_min,
                .table_info_a = .{ .immutable = tree.table_immutable.values },
                .level_b = level_b,
                .range_b = range,
                .callback = compact_table_finish,
            });
        }

        fn compact_start_table(tree: *Tree, op_min: u64, context: CompactionTableContext) void {
            const compaction_beat = tree.compaction_op % half_bar_beat_count;
            assert(compaction_beat == 0);

            assert(context.level_a < constants.lsm_levels);
            assert(context.level_b < constants.lsm_levels);
            assert(context.level_a + 1 == context.level_b);

            // Do not start compaction if level A does not require compaction.
            const table_range = tree.manifest.compaction_table(context.level_a) orelse return;
            const table = table_range.table;

            assert(table_range.range.table_count >= 1);
            assert(table_range.range.table_count <= compaction_tables_input_max);
            assert(compare_keys(table.key_min, table.key_max) != .gt);
            assert(compare_keys(table_range.range.key_min, table.key_min) != .gt);
            assert(compare_keys(table_range.range.key_max, table.key_max) != .lt);

            log.debug(tree_name ++ ": compacting {d} tables from level {d} to level {d}", .{
                table_range.range.table_count,
                context.level_a,
                context.level_b,
            });

            tree.compaction_io_pending += 1;
            context.compaction.start(.{
                .grid = tree.grid,
                .tree = tree,
                .op_min = op_min,
                .table_info_a = .{ .disk = table_range.table },
                .level_b = context.level_b,
                .range_b = table_range.range,
                .callback = compact_table_finish,
            });
        }

        fn compact_table_finish(compaction: *Compaction) void {
            if (compaction.context.level_b == 0) {
                log.debug(tree_name ++ ": compacted immutable table to level {d}", .{
                    compaction.context.level_b,
                });
            } else {
                log.debug(tree_name ++ ": compacted {d} tables from level {d} to level {d}", .{
                    compaction.context.range_b.table_count,
                    compaction.context.level_b - 1,
                    compaction.context.level_b,
                });
            }

            const tree = compaction.context.tree;
            tree.compaction_io_pending -= 1;
            tree.compact_finish_join();
        }

        /// This is called:
        /// * When a compaction finishes.
        /// * When we reach the end of a half-bar.
        /// But this function only does anything on the last call -
        //  when all compactions have finished AND we've reached the end of the half-bar.
        fn compact_finish_join(tree: *Tree) void {
            // If some compactions are still running, we're not finished.
            if (tree.compaction_io_pending > 0) return;

            // If we haven't yet reached the end of the half-bar, we're not finished.
            if (tree.compaction_callback != .awaiting) return;

            log.debug(tree_name ++ ": finished all compactions", .{});

            tree.lookup_snapshot_max = tree.compaction_op + 1;

            // All compactions have finished for the current half-bar.
            // We couldn't remove the (invisible) input tables until now because prefetch()
            // needs a complete set of tables for lookups to avoid missing data.

            // Reset the immutable table Compaction.
            // Also clear any tables made invisible by the compaction.
            const compaction_beat = tree.compaction_op % constants.lsm_batch_multiple;
            const even_levels = compaction_beat < half_bar_beat_count;
            const compacted_levels_odd = compaction_beat == constants.lsm_batch_multiple - 1;
            if (!even_levels) {
                switch (tree.compaction_table_immutable.state) {
                    // The compaction wasn't started for this half bar.
                    .idle => assert(tree.table_immutable.free),
                    .done => {
                        tree.manifest.remove_invisible_tables(
                            tree.compaction_table_immutable.context.level_b,
                            tree.lookup_snapshot_max,
                            tree.compaction_table_immutable.context.range_b.key_min,
                            tree.compaction_table_immutable.context.range_b.key_max,
                        );
                        tree.compaction_table_immutable.reset();
                        tree.table_immutable.clear();
                    },
                    else => unreachable,
                }
            }

            // Reset all the other Compactions.
            // Also clear any tables made invisible by the compactions.
            var it = CompactionTableIterator{ .tree = tree };
            while (it.next()) |context| {
                switch (context.compaction.state) {
                    .idle => {}, // The compaction wasn't started for this half bar.
                    .done => {
                        tree.manifest.remove_invisible_tables(
                            context.compaction.context.level_b,
                            tree.lookup_snapshot_max,
                            context.compaction.context.range_b.key_min,
                            context.compaction.context.range_b.key_max,
                        );
                        if (context.compaction.context.level_b > 0) {
                            tree.manifest.remove_invisible_tables(
                                context.compaction.context.level_b - 1,
                                tree.lookup_snapshot_max,
                                context.compaction.context.range_b.key_min,
                                context.compaction.context.range_b.key_max,
                            );
                        }
                        context.compaction.reset();
                    },
                    else => unreachable,
                }
            }

            assert(tree.compaction_table_immutable.state == .idle);
            it = CompactionTableIterator{ .tree = tree };
            while (it.next()) |context| {
                assert(context.compaction.state == .idle);
            }

            if (compacted_levels_odd) {
                // Assert all visible tables haven't overflowed their max per level.
                tree.manifest.assert_level_table_counts();

                // Convert mutable table to immutable table for next bar, then compact the manifest.
                tree.compact_mutable_table_into_immutable(compact_manifest_next_tick);
                return;
            }

            // Compact the manifest.
            compact_manifest_next_tick(&tree.compaction_next_tick);
        }

        /// Called after the last beat of a full compaction bar
        fn compact_mutable_table_into_immutable(
            tree: *Tree,
            comptime next_tick_callback: fn (*Grid.NextTick) void,
        ) void {
            assert(tree.table_immutable.free);
            assert((tree.compaction_op + 1) % constants.lsm_batch_multiple == 0);
            assert(tree.compaction_op + 1 == tree.lookup_snapshot_max);

            // Nothing to compact.
            if (tree.table_mutable.count() == 0) {
                tree.grid.on_next_tick(next_tick_callback, &tree.compaction_next_tick, .main_thread);
                return;
            }

            const sort_callback = struct {
                fn callback(next_tick: *Grid.NextTick) void {
                    const tree_ = @fieldParentPtr(Tree, "compaction_next_tick", next_tick);
                    assert(tree_.grid.context() == .background_thread);

                    // Sort the mutable table values directly into the immutable table's array.
                    const values_max = tree_.table_immutable.values_max();
                    const values = tree_.table_mutable.sort_into_values_and_clear(values_max);
                    assert(values.ptr == values_max.ptr);

                    // The immutable table must be visible to the next bar — setting its
                    // snapshot_min to lookup_snapshot_max guarantees.
                    //
                    // In addition, the immutable table is conceptually an output table of this
                    // compaction bar, and now its snapshot_min matches the snapshot_min of the
                    // Compactions' output tables.
                    tree_.table_immutable.reset_with_sorted_values(tree_.lookup_snapshot_max, values);
                    assert(tree_.table_mutable.count() == 0);
                    assert(!tree_.table_immutable.free);

                    // Now that the CPU work is finished, switch back to main thread.
                    tree_.grid.on_next_tick(next_tick_callback, &tree_.compaction_next_tick, .main_thread);
                }
            }.callback;

            // Run sorting in a background thread asynchronously to caller.
            tree.grid.on_next_tick(sort_callback, &tree.compaction_next_tick, .background_thread);
        }

        fn compact_manifest_next_tick(next_tick: *Grid.NextTick) void {
            const tree = @fieldParentPtr(Tree, "compaction_next_tick", next_tick);
            tree.manifest.compact(compact_manifest_callback);
        }

        fn compact_manifest_callback(manifest: *Manifest) void {
            const tree = @fieldParentPtr(Tree, "manifest", manifest);
            tree.compact_finish();
        }

        fn compact_finish_next_tick(next_tick: *Grid.NextTick) void {
            const tree = @fieldParentPtr(Tree, "compaction_next_tick", next_tick);
            assert(tree.compaction_callback == .next_tick);

            tracer.end(
                &tree.tracer_slot,
                .{ .tree_compaction_beat = .{ .tree_name = tree_name } },
            );

            const callback = tree.compaction_callback.next_tick;
            tree.compaction_callback = .none;
            callback(tree);
        }

        fn compact_finish(tree: *Tree) void {
            assert(tree.compaction_io_pending == 0);
            assert(tree.compaction_callback == .awaiting);

            if (constants.verify) {
                tree.manifest.verify(tree.lookup_snapshot_max);
            }

            tracer.end(
                &tree.tracer_slot,
                .{ .tree_compaction_beat = .{ .tree_name = tree_name } },
            );

            const callback = tree.compaction_callback.awaiting;
            tree.compaction_callback = .none;
            callback(tree);
        }

        pub fn checkpoint(tree: *Tree, callback: fn (*Tree) void) void {
            // Assert no outstanding compact_tick() work.
            assert(tree.compaction_io_pending == 0);
            assert(tree.compaction_callback == .none);
            assert(tree.compaction_op > 0);
            assert(tree.compaction_op + 1 == tree.lookup_snapshot_max);
            // Don't re-run the checkpoint we recovered from.
            assert(!tree.grid.superblock.working.vsr_state.op_compacted(tree.compaction_op));

            // Assert that this is the last beat in the compaction bar.
            const compaction_beat = tree.compaction_op % constants.lsm_batch_multiple;
            const last_beat_in_bar = constants.lsm_batch_multiple - 1;
            assert(last_beat_in_bar == compaction_beat);

            // Assert no outstanding compactions.
            assert(tree.compaction_table_immutable.state == .idle);
            for (tree.compaction_table) |*compaction| {
                assert(compaction.state == .idle);
            }

            // Assert all manifest levels haven't overflowed their table counts.
            tree.manifest.assert_level_table_counts();

            // Assert that we're checkpointing only after invisible tables have been removed.
            if (constants.verify) {
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
        return op_checkpoint + constants.lsm_batch_multiple + 1;
    }
}

/// The total number of tables that can be supported by the tree across so many levels.
pub fn table_count_max_for_tree(growth_factor: u32, levels_count: u32) u32 {
    assert(growth_factor >= 4);
    assert(growth_factor <= 16); // Limit excessive write amplification.
    assert(levels_count >= 2);
    assert(levels_count <= 10); // Limit excessive read amplification.
    assert(levels_count <= constants.lsm_levels);

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
    assert(level < constants.lsm_levels);

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
