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

const CompositeKey = @import("composite_key.zig").CompositeKey;
const NodePool = @import("node_pool.zig").NodePool(config.lsm_manifest_node_size, 16);
const RingBuffer = @import("../ring_buffer.zig").RingBuffer;
const SuperBlockType = @import("superblock.zig").SuperBlockType;

/// We reserve maxInt(u64) to indicate that a table has not been deleted.
/// Tables that have not been deleted have snapshot_max of maxInt(u64).
/// Since we ensure and assert that a query snapshot never exactly matches
/// the snapshot_min/snapshot_max of a table, we must use maxInt(u64) - 1
/// to query all non-deleted tables.
pub const snapshot_latest = math.maxInt(u64) - 1;

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

pub fn TreeType(comptime Table: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const key_from_value = Table.key_from_value;
    const compare_keys = Table.compare_keys;
    const tombstone = Table.tombstone;
    const tombstone_from_key = Table.tombstone_from_key;

    return struct {
        const Tree = @This();

        const Grid = @import("grid.zig").GridType(Table.Storage);
        const Manifest = @import("manifest.zig").ManifestType(Table);
        const TableMutable = @import("table_mutable.zig").TableMutableType(Table);
        const TableImmutable = @import("table_immutable.zig").TableImmutableType(Table);

        const CompactionType = @import("compaction.zig").CompactionType;
        const TableIteratorType = @import("table_iterator.zig").TableIteratorType;
        const TableImmutableIteratorType = @import("table_immutable.zig").TableImmutableIteratorType;

        pub const PrefetchKeys = std.AutoHashMapUnmanaged(Key, void);
        pub const PrefetchValues = std.HashMapUnmanaged(Value, void, Table.HashMapContextValue, 70);

        pub const ValueCache = std.HashMapUnmanaged(Value, void, Table.HashMapContextValue, 70);

        // NOTE these are above their fields as zig-fmt rejects decls in the middle of fields
        const CompactionTable = CompactionType(Table, TableIteratorType);
        const CompactionTableImmutable = CompactionType(Table, TableImmutableIteratorType);

        grid: *Grid,
        options: Options,

        /// Keys enqueued to be prefetched.
        /// Prefetching ensures that point lookups against the latest snapshot are synchronous.
        /// This shields state machine implementations from the challenges of concurrency and I/O,
        /// and enables simple state machine function signatures that commit writes atomically.
        prefetch_keys: PrefetchKeys,

        prefetch_keys_iterator: ?PrefetchKeys.KeyIterator = null,

        /// A separate hash map for prefetched values not found in the mutable table or value cache.
        /// This is required for correctness, to not evict other prefetch hits from the value cache.
        prefetch_values: PrefetchValues,

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

        compaction_tick: u64,
        compaction_io_pending: usize,
        compaction_callback: ?fn (*Tree) void,

        pub const Options = struct {
            /// The maximum number of keys that may need to be prefetched before commit.
            prefetch_count_max: u32,

            /// The maximum number of keys that may be committed per batch.
            commit_count_max: u32,
        };

        pub fn init(
            allocator: mem.Allocator,
            grid: *Grid,
            node_pool: *NodePool,
            value_cache: ?*ValueCache,
            options: Options,
        ) !Tree {
            if (value_cache == null) {
                assert(options.prefetch_count_max == 0);
            } else {
                assert(options.prefetch_count_max > 0);
            }

            var prefetch_keys = PrefetchKeys{};
            try prefetch_keys.ensureTotalCapacity(allocator, options.prefetch_count_max);
            errdefer prefetch_keys.deinit(allocator);

            var prefetch_values = PrefetchValues{};
            try prefetch_values.ensureTotalCapacity(allocator, options.prefetch_count_max);
            errdefer prefetch_values.deinit(allocator);

            var table_mutable = try TableMutable.init(allocator, options.commit_count_max);
            errdefer table_mutable.deinit(allocator);

            var table_immutable = try TableImmutable.init(allocator, options.commit_count_max);
            errdefer table_immutable.deinit(allocator);

            var manifest = try Manifest.init(allocator, node_pool);
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
                .prefetch_keys = prefetch_keys,
                .prefetch_values = prefetch_values,
                .value_cache = value_cache,
                .table_mutable = table_mutable,
                .table_immutable = table_immutable,
                .manifest = manifest,
                .compaction_table_immutable = compaction_table_immutable,
                .compaction_table = compaction_table,
                .compaction_tick = 0,
                .compaction_io_pending = 0,
                .compaction_callback = null,
            };
        }

        pub fn deinit(tree: *Tree, allocator: mem.Allocator) void {
            tree.compaction_table_immutable.deinit(allocator);
            for (tree.compaction_table) |*compaction| compaction.deinit(allocator);

            // TODO Consider whether we should release blocks acquired from Grid.block_free_set.
            tree.prefetch_keys.deinit(allocator);
            tree.prefetch_values.deinit(allocator);
            tree.table_mutable.deinit(allocator);
            tree.table_immutable.deinit(allocator);
            tree.manifest.deinit(allocator);
        }

        pub fn get(tree: *Tree, key: Key) ?*const Value {
            // Ensure that prefetch() was called and completed if any keys were enqueued:
            assert(tree.prefetch_keys.count() == 0);
            assert(tree.prefetch_keys_iterator == null);

            const value = tree.table_mutable.get(key) orelse
                tree.value_cache.?.getKeyPtr(tombstone_from_key(key)) orelse
                tree.prefetch_values.getKeyPtr(tombstone_from_key(key));

            return unwrap_tombstone(value);
        }

        pub fn put(tree: *Tree, value: Value) void {
            tree.table_mutable.put(value);
        }

        pub fn remove(tree: *Tree, key: Key) void {
            tree.table_mutable.remove(key);
        }

        pub fn lookup(tree: *Tree, snapshot: u64, key: Key, callback: fn (value: ?*const Value) void) void {
            assert(tree.prefetch_keys.count() == 0);
            assert(tree.prefetch_keys_iterator == null);

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
                    callback(unwrap_tombstone(value));
                    return;
                }
            }

            // Hash the key to the fingerprint only once and reuse for all bloom filter checks.
            if (!tree.table_immutable.free and tree.table_immutable.snapshot_min < snapshot) {
                if (tree.table_immutable.get(key)) |value| {
                    callback(unwrap_tombstone(value));
                    return;
                }
            }

            var it = tree.manifest.lookup(snapshot, key);
            if (it.next()) |info| {
                assert(info.visible(snapshot));
                assert(compare_keys(key, info.key_min) != .lt);
                assert(compare_keys(key, info.key_max) != .gt);

                // TODO
            } else {
                callback(null);
                return;
            }
        }

        /// Returns null if the value is null or a tombstone, otherwise returns the value.
        /// We use tombstone values internally, but expose them as null to the user.
        /// This distinction enables us to cache a null result as a tombstone in our hash maps.
        inline fn unwrap_tombstone(value: ?*const Value) ?*const Value {
            return if (value == null or tombstone(value.?)) null else value.?;
        }

        // TODO Notes for compaction
        //
        // if no compactions, should we start one?
        //  - (highlevel) impl note: replica.zig: compact all tress in forest + compact manifest log
        //  - note: even-tables (level-a): 0-2-4-6 odd-tables (level-b): immut-1-3-5-7
        //          this avoids having 1 table being involved in multiple compactioss (t in 0-1 & 1-2)
        //          so even-tables compact in 0-1, 2-3, 4-5, 6-7
        //          and odd-tables compact in immut-0, 1-2, 3-4, 5-6, (ignore) 7
        //  - note: beats of the bar are completed when all compaction callbacks are joined
        //  - note: assert: at the end of every beat; theres space in mut table for next commit/beat
        //  - only start compaction if mutable_table is dirty (was changed)
        //
        //  - assuming a batch_count_multiple=4 (i.e. 4-measure bar of compaction ticks):
        //      - (first) down beat of the bar:
        //          - assert: no compactions are currently running
        //          - assert: mutable table can be empty with immut containing any sorted values
        //          - TODO think through mutable table being converted at the end of the bar?
        //
        //          - check if even levels needs to compact or have space for one more table:
        //              - if so, "start" (choose which table best to compact) and tick compactions
        //              - note: allow level table_count_max to overflow.
        //          - after end of down beat:
        //              - one batch committed to mut table (at most 1/4 full)
        //              - effectively: even compactions started are at least half-way complete
        //
        //      - second+ beat of the bar:
        //          - don't start any new compactions, but tick existing started even ones
        //          - after end of second beat:
        //              - assert: even compactions are "finished"
        //              - atomically update manifest table-infos that were compacted/modified
        //
        //      - (third) back beat of the bar:
        //          - swap to the odd table and do same as down beat
        //
        //      - fourth beat of the bar:
        //          - swapped to the odd tables, so do the same as second beat
        //          - after end of fourth beat:
        //              - assert: all levels.visible haven't overflowed (<= their max)
        //              - convert mutable table to immut for next measure
        //
        // - glossary for stuff + "bar" -> "measure"
        // - compaction_tick -> beat

        pub fn compact_io(tree: *Tree, op: u64, callback: fn (*Tree) void) void {
            const snapshot = op;
            assert(snapshot != snapshot_latest);

            // Make sure there's no pending compact_io() then register the callback.
            assert(tree.compaction_io_pending == 0);
            assert(tree.compaction_callback == null);

            // Start a compaction tick based on the snapshot
            tree.compaction_tick = (op % config.lsm_batch_multiple) + 1;
            assert(tree.compaction_tick <= config.lsm_batch_multiple);

            tree.compaction_callback = callback;
            log.debug("{*}: compaction tick {}/{} started", .{
                tree,
                tree.compaction_tick,
                config.lsm_batch_multiple,
            });

            const half_measure = @divExact(config.lsm_batch_multiple, 2);
            const even_levels = tree.compaction_tick <= half_measure;

            // We should start the compactions either on the first tick (1)
            // or the middle tick (measure/2+1) of the measure.
            const do_start = (tree.compaction_tick == 1) or
                (tree.compaction_tick == half_measure + 1);

            if (even_levels) {
                // Make sure the immutable table only runs when we're compacting odd tables.
                assert(tree.compaction_table_immutable.status == .idle);
            } else {
                if (do_start) tree.compact_io_table_immutable_start(snapshot);
                if (tree.compaction_table_immutable.status == .compacting) {
                    tree.compact_io_tick(&tree.compaction_table_immutable);
                }
            }

            for (tree.compaction_table) |*compaction, index| {
                const level_a = @intCast(u8, (index * 2) + @boolToInt(!even_levels));
                const level_b = level_a + 1;

                // Do not compact if level A is the last level.
                if (level_b == config.lsm_levels) break;
                assert(level_b < config.lsm_levels);

                if (do_start) tree.compact_io_table_start(level_a, level_b, snapshot);
                if (compaction.status == .compacting) tree.compact_io_tick(compaction);
            }
        }

        fn compact_io_table_immutable_start(tree: *Tree, snapshot: u64) void {
            const half_measure = @divExact(config.lsm_batch_multiple, 2);
            assert(tree.compaction_tick == half_measure + 1);

            // Do not start compaction if the immutable table does not require compaction.
            if (tree.table_immutable.free) return;

            const values_count = tree.table_immutable.values.len;
            assert(values_count > 0);

            const level_b: u8 = 0;

            // TODO(King) Encapsulate these as table.key_min() and table.key_max():
            const key_min = key_from_value(&tree.table_immutable.values[0]);
            const key_max = key_from_value(&tree.table_immutable.values[values_count - 1]);

            // TODO(Joran) Get overlapping range with B before deciding to drop_tombstones.

            // TODO(Joran) Check all subsequent levels.
            const drop_tombstones = tree.manifest.overlap_any(level_b, key_min, key_max);

            log.debug("{*}: started compacting immutable table to level 0", .{tree});
            tree.compaction_table_immutable.start(
                tree.grid,
                &tree.manifest,
                level_b,
                snapshot,
                drop_tombstones,
                .{ .table = &tree.table_immutable },
                .{
                    .level = level_b,
                    .key_min = key_min,
                    .key_max = key_max,
                    .grid = tree.grid,
                    .manifest = &tree.manifest,
                    .table_info_callback = undefined, // TODO Should be in Compaction?
                },
            );
        }

        fn compact_io_table_start(tree: *Tree, level_a: u8, level_b: u8, snapshot: u64) void {
            // Choose a table in level A with the least overlap with tables in level B.
            const table_range = tree.manifest.choose_table_for_compaction(level_a) orelse return;
            const table = table_range.table;
            const range = table_range.range;

            // TODO(King) Handle cases
            // - if drop_tomstones -> always start compaction
            // - if range.table_count == 1 -> set_snapshot(level_a) then insert_table(level_b)
            // - if compaction doesn't update table infos -> make sure handled correctly

            assert(compare_keys(range.key_min, table.key_min) != .gt);
            assert(compare_keys(range.key_max, table.key_max) != .lt);

            // TODO(Joran) Check all subsequent levels.
            const drop_tombstones = (level_b == config.lsm_levels - 1) or
                tree.manifest.overlap_any(level_b, range.key_min, range.key_max);

            log.debug("{*}: started compacting level {d} to level {d}", .{
                tree,
                level_a,
                level_b,
            });

            const compaction = &tree.compaction_table[@divFloor(level_a, 2)];
            compaction.start(
                tree.grid,
                &tree.manifest,
                level_b,
                snapshot,
                drop_tombstones,
                .{
                    .grid = tree.grid,
                    .address = table.address,
                    .checksum = table.checksum,
                },
                .{
                    .level = level_b,
                    .key_min = range.key_min,
                    .key_max = range.key_max,
                    .grid = tree.grid,
                    .manifest = &tree.manifest,
                    .table_info_callback = undefined, // TODO Should be in Compaction?
                },
            );
        }

        fn compact_io_tick(tree: *Tree, compaction: anytype) void {
            // TODO(King) relate the compaction.level_b with the beat
            assert(compaction.status == .compacting);
            tree.compaction_io_pending += 1;

            if (@TypeOf(compaction.*) == CompactionTableImmutable) {
                assert(compaction.level_b == 0);
                compaction.tick_io(Tree.compact_io_table_immutable_callback);
                log.debug("{*}: queued compaction for immutable table to level 0", .{tree});
            } else {
                compaction.tick_io(Tree.compact_io_table_callback);
                log.debug("{*}: queued compaction for level {d} to level {d}", .{
                    tree,
                    compaction.level_b - 1,
                    compaction.level_b,
                });
            }
        }

        fn compact_io_table_immutable_callback(compaction: *CompactionTableImmutable) void {
            assert(compaction.status != .idle);
            assert(compaction.level_b < config.lsm_levels);
            assert(compaction.level_b == 0);

            const tree = @fieldParentPtr(Tree, "compaction_table_immutable", compaction);
            assert(tree.compaction_tick > @divExact(config.lsm_batch_multiple, 2));

            log.debug("{*} compact_io complete for immutable table to level 0", .{tree});
            tree.compact_io_done();
        }

        fn compact_io_table_callback(compaction: *CompactionTable) void {
            assert(compaction.status != .idle);
            assert(compaction.level_b < config.lsm_levels);
            assert(compaction.level_b > 0);

            const table_offset = @divFloor(compaction.level_b - 1, 2);
            const table_ptr = @ptrCast([*]CompactionTable, compaction) - table_offset;

            const table_size = @divFloor(config.lsm_levels, 2);
            const table: *[table_size]CompactionTable = table_ptr[0..table_size];

            const tree = @fieldParentPtr(Tree, "compaction_table", table);
            log.debug("{*} compact_io complete for level {d} to level {d}", .{
                tree,
                compaction.level_b - 1,
                compaction.level_b,
            });

            tree.compact_io_done();
        }

        fn compact_io_done(tree: *Tree) void {
            assert(tree.compaction_io_pending <= tree.compaction_table.len + 1);
            assert(tree.compaction_callback != null);
            assert(tree.compaction_tick != 0);

            // compact_done() is called after all compaction tick_io()'s complete.
            // This function can be triggered asynchronously or by compact_cpu below in tick_cpu.
            tree.compaction_io_pending -= 1;
            if (tree.compaction_io_pending == 0) tree.compact_done();
        }

        pub fn compact_cpu(tree: *Tree) void {
            // Make sure a compact_io() was started.
            assert(tree.compaction_io_pending <= tree.compaction_table.len + 1);
            assert(tree.compaction_callback != null);
            assert(tree.compaction_tick != 0);

            const half_measure = @divExact(config.lsm_batch_multiple, 2);
            const even_levels = tree.compaction_tick <= half_measure;

            // Tick the immutable table compaction if it's still running.
            if (even_levels) {
                assert(tree.compaction_table_immutable.status == .idle);
            } else {
                if (tree.compaction_table_immutable.status == .compacting) {
                    tree.compaction_table_immutable.tick_cpu();
                }
            }

            // Tick the table compactions when they're still running.
            for (tree.compaction_table) |*compaction, index| {
                const level = @intCast(u8, (index * 2) + @boolToInt(!even_levels));
                const level_b = level + 1;

                assert(level_b <= config.lsm_levels);
                if (level_b >= config.lsm_levels) break;

                if (compaction.status == .compacting) {
                    assert(compaction.level_b == level_b);
                    compaction.tick_cpu();
                }
            }
        }

        fn compact_done(tree: *Tree) void {
            assert(tree.compaction_io_pending <= tree.compaction_table.len + 1);
            assert(tree.compaction_callback != null);
            assert(tree.compaction_tick != 0);

            // Invoke the compact_io() callback after everything below.
            const callback = tree.compaction_callback.?;
            tree.compaction_callback = null;
            defer callback(tree);

            const half_measure = @divExact(config.lsm_batch_multiple, 2);
            const even_levels = tree.compaction_tick <= half_measure;

            // Mark immutable compaction that reported done in their callback as "completed".
            if (even_levels) {
                assert(tree.compaction_table_immutable.status == .idle);
            } else {
                if (tree.compaction_table_immutable.status == .done) {
                    tree.compaction_table_immutable.reset();
                }
            }

            // Mark compactions that reported done in their callback as "completed" (done = null).
            for (tree.compaction_table) |*compaction, index| {
                const level = @intCast(u8, (index * 2) + @boolToInt(!even_levels));
                const level_b = level + 1;

                assert(level_b <= config.lsm_levels);
                if (level_b >= config.lsm_levels) break;

                assert(compaction.level_b == level_b);
                if (compaction.status == .done) compaction.reset();
            }

            // At the end of every beat, ensure mutable table can be flushed to immutable table.
            assert(tree.table_mutable.can_commit_batch(tree.options.commit_count_max));

            // At end of second measure:
            // - assert: even compactions from previous tick are finished.
            // - update manifest table info that were compacted.
            if (tree.compaction_tick == half_measure) {
                log.debug("{*}: finished compacting even levels", .{tree});

                for (tree.compaction_table) |*compaction, index| {
                    const level = @intCast(u8, (index * 2) + @boolToInt(!even_levels));
                    const level_b = level + 1;

                    assert(level_b <= config.lsm_levels);
                    if (level_b >= config.lsm_levels) break;

                    assert(compaction.level_b == level);
                    assert(compaction.status == .idle);
                }
            }

            // At end of fourth measure:
            // - reset tick to zero which indicates an entire measure is over.
            // - assert: all visible levels haven't overflowed their max.
            // - convert mutable table to immutable tables for next measure.
            if (tree.compaction_tick == config.lsm_batch_multiple) {
                log.debug("{*}: finished compacting immutable table and odd levels", .{tree});
                tree.compaction_tick = 0;
                tree.manifest.assert_visible_tables_are_in_range();

                // Ensure mutable table can be flushed into immutable table.
                assert(tree.table_mutable.count() > 0);
                assert(tree.table_immutable.free);

                // Sort the mutable table values directly into the immutable table's array.
                const values_max = tree.table_immutable.values_max();
                const values = tree.table_mutable.sort_into_values_and_clear(values_max);
                assert(values.ptr == values_max.ptr);

                // Take a manifest snapshot and setup the immutable table with the sorted values.
                const snapshot_min = tree.manifest.take_snapshot();
                tree.table_immutable.reset_with_sorted_values(snapshot_min, values);

                assert(tree.table_mutable.count() == 0);
                assert(!tree.table_immutable.free);
            }
        }

        pub fn checkpoint(tree: *Tree, callback: fn (*Tree) void) void {
            // TODO Call tree.manifest.checkpoint() in parallel.
            _ = tree;
            _ = callback;
        }

        /// This should be called by the state machine for every key that must be prefetched.
        pub fn prefetch_enqueue(tree: *Tree, key: Key) void {
            assert(tree.value_cache != null);
            assert(tree.prefetch_keys_iterator == null);

            if (tree.table_mutable.get(key) != null) return;
            if (tree.value_cache.?.contains(tombstone_from_key(key))) return;

            // We tolerate duplicate keys enqueued by the state machine.
            // For example, if all unique operations require the same two dependencies.
            tree.prefetch_keys.putAssumeCapacity(key, {});
        }

        // get rid of compact_io() skip check
        // alloc 5 -> default:1, compact_snapshot:1+5=6
        // prefetch uses default:1 -> commit (create_snapshot -> default:1) -> compact (default=2)
        // prefetch uses default:2 -> commit (lookup_account -> default:2) -> compact (default=3)
        // prefetch uses default:3 -> commit (create_snapshot -> default:3) -> compact (default=4)
        //  -> (half measure):
        //      checkpoint super-block
        //      set_snapshot_max(compact_snapshot:6)
        //      alloc 5 -> default:7, compact_snapshot:7+5=12

        /// Ensure keys enqueued by `prefetch_enqueue()` are in the cache when the callback returns.
        pub fn prefetch(tree: *Tree, callback: fn () void) void {
            assert(tree.value_cache != null);
            assert(tree.prefetch_keys_iterator == null);

            // Ensure that no stale values leak through from the previous prefetch batch.
            tree.prefetch_values.clearRetainingCapacity();
            assert(tree.prefetch_values.count() == 0);

            tree.prefetch_keys_iterator = tree.prefetch_keys.keyIterator();

            // After finish:
            _ = callback;

            // Ensure that no keys leak through into the next prefetch batch.
            tree.prefetch_keys.clearRetainingCapacity();
            assert(tree.prefetch_keys.count() == 0);

            tree.prefetch_keys_iterator = null;
        }

        fn prefetch_key(tree: *Tree, key: Key) bool {
            assert(tree.value_cache != null);

            if (config.verify) {
                assert(tree.table_mutable.get(key) == null);
                assert(!tree.value_cache.?.contains(tombstone_from_key(key)));
            }

            return true; // TODO
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

    const data_file_size_min = @import("superblock.zig").data_file_size_min;

    const dir_fd = try IO.open_dir(".");
    const storage_fd = try IO.open_file(dir_fd, "test_tree", data_file_size_min, true);
    defer std.fs.cwd().deleteFile("test_tree") catch {};

    var io = try IO.init(128, 0);
    defer io.deinit();

    var storage = try Storage.init(&io, storage_fd);
    defer storage.deinit();

    const Key = CompositeKey(u128);
    const Table = @import("table.zig").TableType(
        Storage,
        Key,
        Key.Value,
        Key.compare_keys,
        Key.key_from_value,
        Key.sentinel_key,
        Key.tombstone,
        Key.tombstone_from_key,
    );

    const Tree = TreeType(Table);

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
        &grid,
        &node_pool,
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
