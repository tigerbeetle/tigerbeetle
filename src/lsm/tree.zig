const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;
const os = std.os;

const config = @import("../config.zig");
const div_ceil = @import("../util.zig").div_ceil;
const eytzinger = @import("eytzinger.zig").eytzinger;
const vsr = @import("../vsr.zig");
const binary_search = @import("binary_search.zig");
const bloom_filter = @import("bloom_filter.zig");

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

    const block_size = config.block_size;
    const BlockPtr = *align(config.sector_size) [block_size]u8;
    const BlockPtrConst = *align(config.sector_size) const [block_size]u8;

    assert(@alignOf(Key) == 8 or @alignOf(Key) == 16);
    // TODO(ifreund) What are our alignment expectations for Value?

    // There must be no padding in the Key/Value types to avoid buffer bleeds.
    assert(@bitSizeOf(Key) == @sizeOf(Key) * 8);
    assert(@bitSizeOf(Value) == @sizeOf(Value) * 8);

    const key_size = @sizeOf(Key);
    const value_size = @sizeOf(Value);

    // We can relax these if necessary. These impact our calculation of the superblock trailer size.
    assert(key_size >= 8);
    assert(key_size <= 32);

    return struct {
        const Tree = @This();

        const Grid = @import("grid.zig").GridType(Table.Storage);
        const Manifest = @import("manifest.zig").ManifestType(Table);
        const MutableTable = @import("mutable_table.zig").MutableTableType(Table);
        const ImmutableTable = @import("immutable_table.zig").ImmutableTableType(Table);

        const CompactionType = @import("compaction.zig").CompactionType;
        const TableIteratorType = @import("table_iterator.zig").TableIteratorType;

        pub const PrefetchKeys = std.AutoHashMapUnmanaged(Key, void);
        pub const PrefetchValues = std.HashMapUnmanaged(Value, void, Table.HashMapContextValue, 70);

        pub const ValueCache = std.HashMapUnmanaged(Value, void, Table.HashMapContextValue, 70);

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

        mutable_table: MutableTable,
        immutable_table: ImmutableTable,

        manifest: Manifest,

        /// The number of Compaction instances is less than the number of levels
        /// as the last level doesn't pair up to compact into another.
        /// The first compaction level is from the immutable-table into a table (level 0 is special)
        /// while the remaining levels are from one on-disk table into another. 
        compactions: [config.lsm_levels - 1]Compaction,

        const TableCompaction = CompactionType(Table, TableIteratorType);
        const ImmutableTableCompaction = CompactionType(Table, ImmutableTable.IteratorType);

        /// A wrapper around both types of CompactionType's
        const Compaction = union(enum) {
            from_immutable: ImmutableTableCompaction,
            from_table: TableCompaction,

            pub fn init(
                level: usize,
                allocator: mem.Allocator, 
                manifest: *Manifest, 
                grid: *Grid,
            ) !Compaction {
                return switch (level) {
                    0 => Compaction{
                        .from_immutable = try ImmutableTableCompaction.init(allocator, manifest, grid),
                    },
                    else => Compaction{
                        .from_table = try TableCompaction.init(allocator, manifest, grid),
                    },
                };
            }

            fn call(compaction: *Compaction, comptime func_name: []const u8, args: anytype) void {
                switch (compaction) {
                    .from_immutable => |*c| @call(.{}, @field(c, func_name), args),
                    .from_table => |*c| @call(.{}, @field(c, func_name), args),
                }
            }

            pub fn deinit(compaction: *Compaction, allocator: mem.Allocator) void {
                compaction.call("deinit", .{allocator});
            }

            pub fn start(
                compaction: *Compaction,
                level_a: u64,
                level_b: u32,
                level_b_key_min: Key,
                level_b_key_max: Key,
                drop_tombstones: bool,
            ) void {
                compaction.call("start", .{
                    level_a, 
                    level_b, 
                    level_b_key_min, 
                    level_b_key_max,
                    drop_tombstones,
                });
            }

            pub fn tick_io(compaction: *Compaction, callback: Callback) void {
                compaction.call("tick_io", .{callback});
            }

            pub fn tick_cpu(compaction: *Compaction) void {
                compaction.call("tick_cpu", .{});
            }
        };

        pub const Options = struct {
            /// The maximum number of keys that may need to be prefetched before commit.
            prefetch_count_max: u32,

            /// The maximum number of keys that may be committed per batch.
            commit_count_max: u32,
        };

        pub fn init(
            tree: *Tree,
            allocator: mem.Allocator,
            grid: *Grid,
            node_pool: *NodePool,
            value_cache: ?*ValueCache,
            options: Options,
        ) !void {
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

            var mutable_table = try MutableTable.init(allocator, options.commit_count_max);
            errdefer mutable_table.deinit(allocator);

            var table = try ImmutableTable.init(allocator, options.commit_count_max);
            errdefer table.deinit(allocator);

            var manifest = try Manifest.init(allocator, node_pool);
            errdefer manifest.deinit(allocator);

            tree.* = Tree{
                .grid = grid,
                .options = options,
                .prefetch_keys = prefetch_keys,
                .prefetch_values = prefetch_values,
                .value_cache = value_cache,
                .mutable_table = mutable_table,
                .table = table,
                .manifest = manifest,
                .compactions = compactions,
            };

            for (tree.compactions) |*compaction, level| {
                errdefer for (tree.compactions[0..level]) |*c| c.deinit(allocator);
                compaction.* = try Compaction.init(level, allocator, &tree.manifest, tree.grid);
            }
        }

        pub fn deinit(tree: *Tree, allocator: mem.Allocator) void {
            for (tree.compactions) |*compaction| compaction.deinit(allocator);

            // TODO Consider whether we should release blocks acquired from Grid.block_free_set.
            tree.prefetch_keys.deinit(allocator);
            tree.prefetch_values.deinit(allocator);
            tree.mutable_table.deinit(allocator);
            tree.table.deinit(allocator);
            tree.manifest.deinit(allocator);
        }

        pub fn get(tree: *Tree, key: Key) ?*const Value {
            // Ensure that prefetch() was called and completed if any keys were enqueued:
            assert(tree.prefetch_keys.count() == 0);
            assert(tree.prefetch_keys_iterator == null);

            const value = tree.mutable_table.get(key) orelse
                tree.value_cache.?.getKeyPtr(tombstone_from_key(key)) orelse
                tree.prefetch_values.getKeyPtr(tombstone_from_key(key));

            return unwrap_tombstone(value);
        }

        pub fn put(tree: *Tree, value: Value) void {
            tree.mutable_table.put(value);
        }

        pub fn remove(tree: *Tree, key: Key) void {
            tree.mutable_table.remove(key);
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
                if (tree.mutable_table.get(key) orelse
                    tree.value_cache.?.getKeyPtr(tombstone_from_key(key))) |value|
                {
                    callback(unwrap_tombstone(value));
                    return;
                }
            }

            // Hash the key to the fingerprint only once and reuse for all bloom filter checks.
            const fingerprint = bloom_filter.Fingerprint.create(mem.asBytes(&key));

            if (!tree.table.free and tree.table.info.visible(snapshot)) {
                if (tree.table.get(key, fingerprint)) |value| {
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
            return if (value == null or tombstone(value.?.*)) null else value.?;
        }

        pub fn compact(tree: *Tree, callback: fn (*Tree) void) void {
            // Convert the mutable table to an immutable table if necessary:
            if (tree.mutable_table.cannot_commit_batch(tree.options.commit_count_max)) {
                assert(tree.mutable_table.count() > 0);
                assert(tree.immutable_table.free);

                // sort the mutable_table values directly into the immutable_table's array.
                const values_max = tree.immutable_table.values_max();
                const values = tree.mutable_table.sort_into_values_and_clear(values_max);
                assert(values.ptr == values_max.ptr);

                // take a manifest snapshot and setup the immutable_table with the sorted values.
                const snapshot_min = tree.manifest.take_snapshot();
                tree.immutable_table.reset_with_sorted_values(snapshot_min, values);

                assert(tree.mutable_table.count() == 0);
                assert(!tree.immutable_table.free);
            }

            // if compactions are started, tick them or start some
            // scatter-gather/join compaction callbacks -> function callback

            // if (!tree.table.free) {
            //     tree.table.flush.tick(callback);
            // } else {
            //     callback(tree);
            // }

            // TODO Call tree.manifest.flush()
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

            if (tree.mutable_table.get(key) != null) return;
            if (tree.value_cache.?.contains(tombstone_from_key(key))) return;

            // We tolerate duplicate keys enqueued by the state machine.
            // For example, if all unique operations require the same two dependencies.
            tree.prefetch_keys.putAssumeCapacity(key, {});
        }

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
                assert(tree.mutable_table.get(key) == null);
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

    // In the worst case, when compacting level 0 we may need to pick all overlapping tables.
    // We therefore do not grow the size of level 1 since that would further amplify this cost.
    if (level == 0) return growth_factor;
    if (level == 1) return growth_factor;

    return math.pow(u32, growth_factor, level);
}

test "table count max" {
    const expectEqual = std.testing.expectEqual;

    try expectEqual(@as(u32, 8), table_count_max_for_level(8, 0));
    try expectEqual(@as(u32, 8), table_count_max_for_level(8, 1));
    try expectEqual(@as(u32, 64), table_count_max_for_level(8, 2));
    try expectEqual(@as(u32, 512), table_count_max_for_level(8, 3));
    try expectEqual(@as(u32, 4096), table_count_max_for_level(8, 4));
    try expectEqual(@as(u32, 32768), table_count_max_for_level(8, 5));
    try expectEqual(@as(u32, 262144), table_count_max_for_level(8, 6));
    try expectEqual(@as(u32, 2097152), table_count_max_for_level(8, 7));

    try expectEqual(@as(u32, 8 + 8), table_count_max_for_tree(8, 2));
    try expectEqual(@as(u32, 16 + 64), table_count_max_for_tree(8, 3));
    try expectEqual(@as(u32, 80 + 512), table_count_max_for_tree(8, 4));
    try expectEqual(@as(u32, 592 + 4096), table_count_max_for_tree(8, 5));
    try expectEqual(@as(u32, 4688 + 32768), table_count_max_for_tree(8, 6));
    try expectEqual(@as(u32, 37456 + 262144), table_count_max_for_tree(8, 7));
    try expectEqual(@as(u32, 299600 + 2097152), table_count_max_for_tree(8, 8));
}

pub fn main() !void {
    const testing = std.testing;
    const allocator = testing.allocator;

    const IO = @import("../io.zig").IO;
    const Storage = @import("../storage.zig").Storage;
    const Grid = @import("grid.zig").GridType(Storage);

    const data_file_size_min = @import("superblock.zig").data_file_size_min;

    const storage_fd = try Storage.open("test_tree", data_file_size_min, true);
    defer std.fs.cwd().deleteFile("test_tree") catch {};

    var io = try IO.init(128, 0);
    defer io.deinit();

    var storage = try Storage.init(&io, storage_fd);
    defer storage.deinit();

    const Key = CompositeKey(u128);

    const Tree = TreeType(
        Storage,
        Key,
        Key.Value,
        Key.compare_keys,
        Key.key_from_value,
        Key.sentinel_key,
        Key.tombstone,
        Key.tombstone_from_key,
    );

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
        commit_count_max * config.lsm_mutable_table_batch_multiple * 128,
        .exact,
    );
    defer allocator.free(sort_buffer);

    // TODO Initialize SuperBlock:
    var superblock: SuperBlockType(Storage) = undefined;

    var grid = try Grid.init(allocator, &superblock);
    defer grid.deinit(allocator);

    var tree: Tree = undefined;
    try tree.init(
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

    _ = Tree.Table;
    _ = Tree.Table.create_from_sorted_values;
    _ = Tree.Table.get;
    _ = Tree.Table.FlushIterator;
    _ = Tree.Table.FlushIterator.tick;
    _ = Tree.TableIteratorType;
    _ = Tree.LevelIteratorType;
    _ = Tree.Manifest.LookupIterator.next;
    _ = Tree.Compaction;
    _ = Tree.Table.Builder.data_block_finish;
    _ = tree.prefetch_enqueue;
    _ = tree.prefetch;
    _ = tree.prefetch_key;
    _ = tree.get;
    _ = tree.put;
    _ = tree.remove;
    _ = tree.lookup;
    _ = tree.manifest;
    _ = tree.manifest.lookup;
    _ = tree.flush;
    _ = Tree.Table.filter_blocks_used;
    _ = tree.manifest.insert_tables;
    _ = Tree.Compaction.tick_io;
    _ = Tree.Compaction.tick_cpu;
    _ = Tree.Compaction.tick_cpu_merge;

    std.debug.print("table_count_max={}\n", .{table_count_max});
}
