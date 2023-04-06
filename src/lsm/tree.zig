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

pub fn TreeType(comptime TreeTable: type, comptime Storage: type, comptime tree_name: [:0]const u8) type {
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
        const Compaction = CompactionType(Table, Tree, Storage);
        const CompactionRange = Manifest.CompactionRange;

        grid: *Grid,
        options: Options,

        table_mutable: TableMutable,
        table_immutable: TableImmutable,
        values_cache: ?*TableMutable.ValuesCache,

        manifest: Manifest,

        compaction: Compaction,
        compaction_ranges: [constants.lsm_levels]?CompactionRange,

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

        compaction_level_b: ?u8,
        compaction_callback: union(enum) {
            none,
            /// We're at the end of a bar.
            /// Call this callback when all current compactions finish.
            awaiting: fn (*Tree) void,
            /// We're at the end of some other beat.
            /// Call this on the next tick.
            next_tick: fn (*Tree) void,
        },
        compaction_next_tick: Grid.NextTick = undefined,

        checkpoint_callback: ?fn (*Tree) void,
        open_callback: ?fn (*Tree) void,

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

            var compaction = try Compaction.init(allocator, tree_name);
            errdefer compaction.deinit(allocator);

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
                .compaction = compaction,
                .compaction_ranges = .{null} ** constants.lsm_levels,
                .compaction_op = compaction_op,
                .lookup_snapshot_max = lookup_snapshot_max,
                .compaction_level_b = null,
                .compaction_callback = .none,
                .checkpoint_callback = null,
                .open_callback = null,
            };
        }

        pub fn deinit(tree: *Tree, allocator: mem.Allocator) void {
            tree.compaction.deinit(allocator);

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

        pub fn compact(tree: *Tree, callback: fn (*Tree) void, op: u64) void {
            assert(tree.compaction_callback == .none);
            assert(op != 0);
            assert(op == tree.compaction_op + 1);
            assert(op > tree.grid.superblock.working.vsr_state.commit_min);

            tree.compaction_op = op;

            if (op < constants.lsm_batch_multiple) {
                // There is nothing to compact for the first measure.
                // We skip the main compaction code path first compaction bar entirely because it
                // is a special case — its first beat is 1, not 0.

                tree.compaction_callback = .{ .next_tick = callback };
                tree.grid.on_next_tick(compact_finish_next_tick, &tree.compaction_next_tick);
                return;
            }

            if (tree.grid.superblock.working.vsr_state.op_compacted(op)) {
                // We recovered from a checkpoint, and must avoid replaying one bar of
                // compactions that were applied before the checkpoint. Repeating these ops'
                // compactions would actually perform different compactions than before,
                // causing the storage state of the replica to diverge from the cluster.
                // See also: lookup_snapshot_max_for_checkpoint().

                tree.compaction_callback = .{ .next_tick = callback };
                tree.grid.on_next_tick(compact_finish_next_tick, &tree.compaction_next_tick);
                return;
            }
            assert(op == tree.lookup_snapshot_max);

            const op_min = compaction_op_min(tree.compaction_op);
            const compaction_beat = tree.compaction_op % constants.lsm_batch_multiple;
            log.debug(tree_name ++ ": compact: op={d} op_min={d} beat={d}/{d}", .{
                tree.compaction_op,
                op_min,
                compaction_beat + 1,
                constants.lsm_batch_multiple,
            });

            const BeatKind = enum { bar_start, bar_middle, bar_end };
            const beat_kind = if (compaction_beat == 0)
                BeatKind.bar_start
            else if (compaction_beat == constants.lsm_batch_multiple - 1)
                BeatKind.bar_end
            else
                BeatKind.bar_middle;

            switch (beat_kind) {
                .bar_start => {
                    // Start compaction.
                    if (constants.verify) {
                        tree.manifest.verify(tree.compaction_op);
                    }
                    tree.manifest.reserve();
                    assert(tree.compaction_level_b == null);
                    tree.compaction_level_b = constants.lsm_levels - 1;
                    tree.compact_table_start();

                    // Return on next tick.
                    tree.compaction_callback = .{ .next_tick = callback };
                    tree.grid.on_next_tick(compact_finish_next_tick, &tree.compaction_next_tick);
                },
                .bar_middle => {
                    // Return on next tick.
                    tree.compaction_callback = .{ .next_tick = callback };
                    tree.grid.on_next_tick(compact_finish_next_tick, &tree.compaction_next_tick);
                },
                .bar_end => {
                    // Return when compaction is finished.
                    tree.compaction_callback = .{ .awaiting = callback };
                    tree.compact_finish_join();
                },
            }
        }

        fn compact_table_start(tree: *Tree) void {
            const level_b = tree.compaction_level_b.?;
            assert(level_b < constants.lsm_levels);

            const op_min = compaction_op_min(tree.compaction_op);
            assert(op_min < snapshot_latest);
            assert(op_min % constants.lsm_batch_multiple == 0);

            if (level_b == 0) {
                const compaction_needed = !tree.table_immutable.free;
                if (!compaction_needed) {
                    tree.compact_table_finish();
                    return;
                }

                const table_info_a = .{ .immutable = tree.table_immutable.values };
                const range_b = tree.manifest.compaction_range(
                    level_b,
                    tree.table_immutable.key_min(),
                    tree.table_immutable.key_max(),
                );
                tree.compaction_ranges[level_b] = range_b;
                assert(compare_keys(range_b.key_min, tree.table_immutable.key_min()) != .gt);
                assert(compare_keys(range_b.key_max, tree.table_immutable.key_max()) != .lt);

                assert(range_b.table_count >= 1);
                assert(range_b.table_count <= compaction_tables_input_max);

                log.debug(tree_name ++
                    ": compacting immutable table to level 0 " ++
                    "(values.len={d} snapshot_min={d} compaction.op_min={d} table_count={d})", .{
                    tree.table_immutable.values.len,
                    tree.table_immutable.snapshot_min,
                    op_min,
                    range_b.table_count,
                });

                tree.compaction.start(.{
                    .grid = tree.grid,
                    .tree = tree,
                    .op_min = op_min,
                    .table_info_a = table_info_a,
                    .level_b = level_b,
                    .range_b = range_b,
                    .callback = compact_table_cleanup,
                });
            } else {
                const table_range = tree.manifest.compaction_table(level_b - 1);

                const compaction_needed = table_range != null;
                if (!compaction_needed) {
                    tree.compact_table_finish();
                    return;
                }

                const table = table_range.?.table;
                const table_info_a = .{ .disk = table };
                const range_b = table_range.?.range;
                tree.compaction_ranges[level_b] = range_b;
                assert(compare_keys(table.key_min, table.key_max) != .gt);
                assert(compare_keys(range_b.key_min, table.key_min) != .gt);
                assert(compare_keys(range_b.key_max, table.key_max) != .lt);

                assert(range_b.table_count >= 1);
                assert(range_b.table_count <= compaction_tables_input_max);

                log.debug(tree_name ++ ": compacting {d} tables from level {d} to level {d}", .{
                    range_b.table_count,
                    level_b - 1,
                    level_b,
                });

                tree.compaction.start(.{
                    .grid = tree.grid,
                    .tree = tree,
                    .op_min = op_min,
                    .table_info_a = table_info_a,
                    .level_b = level_b,
                    .range_b = range_b,
                    .callback = compact_table_cleanup,
                });
            }
        }

        fn compact_table_cleanup(compaction: *Compaction) void {
            assert(compaction.state == .done);
            compaction.reset();

            const tree = compaction.context.tree;
            tree.compact_table_finish();
        }

        fn compact_table_finish(tree: *Tree) void {
            if (tree.compaction_level_b.? == 0) {
                // All levels have been compacted.
                log.debug(tree_name ++ ": finished all compactions", .{});
                tree.compaction_level_b = null;
                tree.compact_finish_join();
            } else {
                // Compact the next level.
                tree.compaction_level_b.? -= 1;
                tree.compact_table_start();
            }
        }

        /// This is called:
        /// * When all compactions finishes.
        /// * When we reach the end of a bar.
        /// But this function only does anything on the last call -
        //  when all compactions have finished AND we've reached the end of the bar.
        fn compact_finish_join(tree: *Tree) void {
            // If some compactions are still running, we're not finished.
            if (tree.compaction_level_b != null) return;

            // If we haven't yet reached the end of the bar, we're not finished.
            if (tree.compaction_callback != .awaiting) return;

            log.debug(tree_name ++ ": finished waiting on compactions", .{});

            // Any tables that were compacted will become invisible at the end of this beat.
            // We know that prefetching has finished for this bar, so these tables are now
            // safe to remove.
            var level_b: u8 = 0;
            while (level_b < constants.lsm_levels) : (level_b += 1) {
                if (tree.compaction_ranges[level_b]) |range_b| {
                    tree.manifest.remove_invisible_tables(
                        level_b,
                        tree.lookup_snapshot_max + 1,
                        range_b.key_min,
                        range_b.key_max,
                    );
                    if (level_b > 0) {
                        tree.manifest.remove_invisible_tables(
                            level_b - 1,
                            tree.lookup_snapshot_max + 1,
                            range_b.key_min,
                            range_b.key_max,
                        );
                    }
                    tree.compaction_ranges[level_b] = null;
                }
            }

            // Assert that the cleanup above caught all invisible tables.
            tree.manifest.assert_no_invisible_tables(tree.lookup_snapshot_max + 1);

            // Assert all visible tables haven't overflowed their max per level.
            tree.manifest.assert_level_table_counts();

            // Compact the manifest.
            // TODO(jamii) Could we do this before the end of the bar?
            tree.manifest.compact(compact_manifest_callback);
        }

        fn compact_manifest_callback(manifest: *Manifest) void {
            const tree = @fieldParentPtr(Tree, "manifest", manifest);

            if (constants.verify) {
                tree.manifest.verify(snapshot_latest);
            }

            tree.compact_finish();
        }

        fn compact_finish_next_tick(next_tick: *Grid.NextTick) void {
            const tree = @fieldParentPtr(Tree, "compaction_next_tick", next_tick);
            assert(tree.compaction_callback == .next_tick);

            const callback = tree.compaction_callback.next_tick;
            tree.compaction_callback = .none;
            callback(tree);
        }

        fn compact_finish(tree: *Tree) void {
            assert(tree.compaction_callback == .awaiting);

            const callback = tree.compaction_callback.awaiting;
            tree.compaction_callback = .none;
            callback(tree);
        }

        /// Called at the end of each beat.
        pub fn op_done(tree: *Tree, op: u64) void {
            assert(op != 0);
            assert(op > tree.grid.superblock.working.vsr_state.commit_min);

            // If we recovered from a checkpoint, we must avoid replaying one bar of
            // compactions that were applied before the checkpoint. Repeating these ops'
            // compactions would actually perform different compactions than before,
            // causing the storage state of the replica to diverge from the cluster.
            const recovering = tree.grid.superblock.working.vsr_state.op_compacted(op);

            if (recovering) {
                // During recovery, `tree.lookup_snapshot_max` is set to one bar after the
                // checkpoint. See also `lookup_snapshot_max_for_checkpoint`.
            } else {
                assert(op == tree.lookup_snapshot_max);
                tree.lookup_snapshot_max = op + 1;
            }

            const bar_done = if (recovering)
                op + 1 == tree.lookup_snapshot_max
            else
                op % constants.lsm_batch_multiple == constants.lsm_batch_multiple - 1;

            if (bar_done) {
                // Ensure the mutable table has space before the next bar starts.
                tree.compact_mutable_table_into_immutable();
            }
        }

        /// Called after the last beat of a bar.
        fn compact_mutable_table_into_immutable(tree: *Tree) void {
            assert((tree.compaction_op + 1) % constants.lsm_batch_multiple == 0);
            assert(tree.compaction_op + 1 == tree.lookup_snapshot_max);

            tree.table_immutable.clear();

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

        pub fn checkpoint(tree: *Tree, callback: fn (*Tree) void) void {
            // Assert no outstanding compact work.
            assert(tree.compaction.state == .idle);
            assert(tree.compaction_level_b == null);
            assert(tree.compaction_callback == .none);
            assert(tree.compaction_op > 0);
            assert(tree.compaction_op + 1 == tree.lookup_snapshot_max);
            // Don't re-run the checkpoint we recovered from.
            assert(!tree.grid.superblock.working.vsr_state.op_compacted(tree.compaction_op));

            // Assert that this is the last beat in the compaction bar.
            const compaction_beat = tree.compaction_op % constants.lsm_batch_multiple;
            const last_beat_in_bar = constants.lsm_batch_multiple - 1;
            assert(last_beat_in_bar == compaction_beat);

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
/// - `op_min + lsm_batch_multiple - 1` will be the input tables' snapshot_max.
/// - `op_min + lsm_batch_multiple` will be the output tables' snapshot_min.
pub fn compaction_op_min(op: u64) u64 {
    return op - (op % constants.lsm_batch_multiple);
}

/// TODO(jamii) Update
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
