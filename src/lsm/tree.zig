const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;
const os = std.os;

/// Whether to perform slow, intensive online verification of data.
pub const verify = true;

const config = @import("../config.zig");
const div_ceil = @import("../util.zig").div_ceil;
const eytzinger = @import("eytzinger.zig").eytzinger;
const vsr = @import("../vsr.zig");
const binary_search = @import("binary_search.zig");
const bloom_filter = @import("bloom_filter.zig");

const BlockFreeSet = @import("block_free_set.zig").BlockFreeSet;
const Direction = @import("direction.zig").Direction;
const CompositeKey = @import("composite_key.zig").CompositeKey;
const KWayMergeIterator = @import("k_way_merge.zig").KWayMergeIterator;
const ManifestLevel = @import("manifest_level.zig").ManifestLevel;
const NodePool = @import("node_pool.zig").NodePool(config.lsm_manifest_node_size, 16);
const RingBuffer = @import("../ring_buffer.zig").RingBuffer;
const SegmentedArray = @import("segmented_array.zig").SegmentedArray;
const SegmentedArrayCursor = @import("segmented_array.zig").Cursor;
const SuperBlock = @import("superblock.zig").SuperBlock;

/// We reserve maxInt(u64) to indicate that a table has not been deleted.
/// Tables that have not been deleted have snapshot_max of maxInt(u64).
/// Since we ensure and assert that a query snapshot never exactly matches
/// the snaphshot_min/snapshot_max of a table, we must use maxInt(u64) - 1
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

pub fn Tree(
    comptime Storage: type,
    /// Key sizes of 8, 16, 32, etc. are supported with alignment 8 or 16.
    comptime Key: type,
    comptime Value: type,
    /// Returns the sort order between two keys.
    comptime compare_keys: fn (Key, Key) callconv(.Inline) math.Order,
    /// Returns the key for a value. For example, given `object` returns `object.id`.
    /// Since most objects contain an id, this avoids duplicating the key when storing the value.
    comptime key_from_value: fn (Value) callconv(.Inline) Key,
    /// Must compare greater than all other keys.
    comptime sentinel_key: Key,
    /// Returns whether a value is a tombstone value.
    comptime tombstone: fn (Value) callconv(.Inline) bool,
    /// Returns a tombstone value representation for a key.
    comptime tombstone_from_key: fn (Key) callconv(.Inline) Value,
) type {
    const Block = Storage.Block;
    const BlockPtr = Storage.BlockPtr;
    const BlockPtrConst = Storage.BlockPtrConst; // TODO Use this more where we can.

    const block_size = Storage.block_size;

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
        const TreeGeneric = @This();

        const HashMapContextValue = struct {
            pub fn eql(_: HashMapContextValue, a: Value, b: Value) bool {
                return compare_keys(key_from_value(a), key_from_value(b)) == .eq;
            }

            pub fn hash(_: HashMapContextValue, value: Value) u64 {
                const key = key_from_value(value);
                return std.hash_map.getAutoHashFn(Key, HashMapContextValue)(.{}, key);
            }
        };

        const HashMapContextBlock = struct {
            pub fn eql(_: HashMapContextBlock, a: Block, b: Block) bool {
                const x = Table.block_address(a);
                const y = Table.block_address(b);

                assert(x != 0);
                assert(y != 0);

                return x == y;
            }

            pub fn hash(_: HashMapContextBlock, block: Block) u64 {
                const address = Table.block_address(block);
                assert(address != 0);
                return std.hash_map.getAutoHashFn(u64, HashMapContextBlock)(.{}, address);
            }
        };

        pub const Manifest = struct {
            pub const TableInfo = extern struct {
                checksum: u128,
                address: u64,
                flags: u64 = 0,

                /// The minimum snapshot that can see this table (with exclusive bounds).
                /// This value is set to the current snapshot tick on table creation.
                snapshot_min: u64,

                /// The maximum snapshot that can see this table (with exclusive bounds).
                /// This value is set to the current snapshot tick on table deletion.
                snapshot_max: u64 = math.maxInt(u64),

                key_min: Key,
                key_max: Key,

                comptime {
                    assert(@sizeOf(TableInfo) == 48 + key_size * 2);
                    assert(@alignOf(TableInfo) == 16);
                }

                pub fn visible(table: *const TableInfo, snapshot: u64) bool {
                    assert(table.address != 0);
                    assert(table.snapshot_min < table.snapshot_max);
                    assert(snapshot <= snapshot_latest);

                    assert(snapshot != table.snapshot_min);
                    assert(snapshot != table.snapshot_max);

                    return table.snapshot_min < snapshot and snapshot < table.snapshot_max;
                }
            };

            /// Level 0 is special since tables can overlap the same key range.
            /// Here, we simply store tables in reverse order of precedence (i.e. newest first).
            const Conjoint = SegmentedArray(TableInfo, NodePool, table_count_max);

            /// Levels beyond level 0 have tables with disjoint key ranges.
            /// Here, we use a structure with indexes over the segmented array for performance.
            const Disjoint = ManifestLevel(NodePool, Key, TableInfo, compare_keys);

            const Level = union(LevelTag) {
                conjoint: Conjoint,
                disjoint: Disjoint,
            };

            const LevelTag = enum {
                conjoint,
                disjoint,
            };

            node_pool: *NodePool,

            levels: [config.lsm_levels]Level,

            pub fn init(allocator: mem.Allocator, node_pool: *NodePool) !Manifest {
                var levels: [config.lsm_levels]Level = undefined;

                levels[0] = .{ .conjoint = try Conjoint.init(allocator) };
                errdefer levels[0].conjoint.deinit(allocator, node_pool);

                for (levels[1..]) |*level, i| {
                    errdefer for (levels[1..i]) |*l| l.disjoint.deinit(allocator, node_pool);

                    level.* = .{ .disjoint = try Disjoint.init(allocator) };
                }
                errdefer for (levels[1..]) |*l| l.disjoint.deinit(allocator, node_pool);

                return Manifest{
                    .node_pool = node_pool,
                    .levels = levels,
                };
            }

            pub fn deinit(manifest: *Manifest, allocator: mem.Allocator) void {
                manifest.levels[0].conjoint.deinit(allocator, manifest.node_pool);
                for (manifest.levels[1..]) |*l| l.disjoint.deinit(allocator, manifest.node_pool);
            }

            pub const LookupIterator = struct {
                manifest: *Manifest,
                snapshot: u64,
                key: Key,
                level: u8 = 0,
                inner: ?Conjoint.Iterator = null,
                precedence: ?u64 = null,

                pub fn next(it: *LookupIterator) ?*const TableInfo {
                    while (it.level < config.lsm_levels) : (it.level += 1) {
                        switch (it.manifest.levels[it.level]) {
                            .conjoint => |*level| {
                                assert(it.level == 0);

                                if (it.inner == null) it.inner = level.iterator(0, 0, .ascending);
                                while (it.inner.?.next()) |table| {
                                    if (it.precedence) |p| assert(p > table.snapshot_min);
                                    it.precedence = table.snapshot_min;

                                    if (!table.visible(it.snapshot)) continue;
                                    if (compare_keys(it.key, table.key_min) == .lt) continue;
                                    if (compare_keys(it.key, table.key_max) == .gt) continue;

                                    return table;
                                }
                                assert(it.inner.?.done);
                                it.inner = null;
                            },
                            .disjoint => |*level| {
                                assert(it.level > 0);

                                var inner = level.iterator(it.snapshot, it.key, it.key, .ascending);
                                if (inner.next()) |table| {
                                    if (it.precedence) |p| assert(p > table.snapshot_min);
                                    it.precedence = table.snapshot_min;

                                    assert(table.visible(it.snapshot));
                                    assert(compare_keys(it.key, table.key_min) != .lt);
                                    assert(compare_keys(it.key, table.key_max) != .gt);
                                    assert(inner.next() == null);

                                    it.level += 1;
                                    return table;
                                }
                            },
                        }
                    }

                    assert(it.level == config.lsm_levels);
                    return null;
                }
            };

            pub fn lookup(manifest: *Manifest, snapshot: u64, key: Key) LookupIterator {
                return .{
                    .manifest = manifest,
                    .snapshot = snapshot,
                    .key = key,
                };
            }
        };

        /// Range queries are not supported on the MutableTable, it must first be made immutable.
        pub const MutableTable = struct {
            const load_factor = 50;
            const Values = std.HashMapUnmanaged(Value, void, HashMapContextValue, load_factor);

            value_count_max: u32,
            values: Values = .{},

            pub fn init(allocator: mem.Allocator, commit_count_max: u32) !MutableTable {
                comptime assert(config.lsm_mutable_table_batch_multiple > 0);
                assert(commit_count_max > 0);

                const value_count_max = commit_count_max * config.lsm_mutable_table_batch_multiple;

                var values: Values = .{};
                try values.ensureTotalCapacity(allocator, value_count_max);
                errdefer values.deinit(allocator);

                return MutableTable{
                    .value_count_max = value_count_max,
                    .values = values,
                };
            }

            pub fn deinit(table: *MutableTable, allocator: mem.Allocator) void {
                table.values.deinit(allocator);
            }

            pub fn get(table: *MutableTable, key: Key) ?*const Value {
                return table.values.getKeyPtr(tombstone_from_key(key));
            }

            pub fn put(table: *MutableTable, value: Value) void {
                table.values.putAssumeCapacity(value, {});
                // The hash map's load factor may allow for more capacity because of rounding:
                assert(table.values.count() <= table.value_count_max);
            }

            pub fn remove(table: *MutableTable, key: Key) void {
                table.values.putAssumeCapacity(tombstone_from_key(key), {});
                assert(table.values.count() <= table.value_count_max);
            }

            fn cannot_commit_batch(table: *MutableTable, batch_count: u32) bool {
                assert(table.values.count() <= table.value_count_max);
                assert(batch_count <= table.value_count_max);

                return table.values.count() + batch_count > table.value_count_max;
            }

            /// The returned slice is invalidated whenever this is called for any tree.
            fn as_sorted_values(
                table: *MutableTable,
                sort_buffer: []align(@alignOf(Value)) u8,
            ) []const Value {
                const sort_buffer_count_max = @divFloor(sort_buffer.len, @sizeOf(Value));
                assert(sort_buffer_count_max >= table.value_count_max);

                assert(table.values.count() > 0);
                assert(table.values.count() <= table.value_count_max);
                assert(table.values.count() <= sort_buffer_count_max);

                const values_max = mem.bytesAsSlice(
                    Value,
                    sort_buffer[0 .. table.value_count_max * @sizeOf(Value)],
                );

                var i: usize = 0;
                var it = table.values.keyIterator();
                while (it.next()) |value| : (i += 1) {
                    values_max[i] = value.*;
                }
                const values = values_max[0..i];
                assert(values.len == table.values.count());

                std.sort.sort(Value, values, {}, sort_values_by_key_in_ascending_order);

                return values;
            }

            fn sort_values_by_key_in_ascending_order(_: void, a: Value, b: Value) bool {
                return compare_keys(key_from_value(a), key_from_value(b)) == .lt;
            }
        };

        pub const Table = struct {
            const address_size = @sizeOf(u64);
            const checksum_size = @sizeOf(u128);
            const table_size_max = config.lsm_table_size_max;
            const table_block_count_max = @divExact(table_size_max, block_size);
            const block_body_size = block_size - @sizeOf(vsr.Header);

            const layout = blk: {
                assert(config.lsm_table_block_size % config.sector_size == 0);
                assert(math.isPowerOfTwo(table_size_max));
                assert(math.isPowerOfTwo(block_size));

                // Searching the values array is more expensive than searching the per-block index
                // as the larger values size leads to more cache misses. We can therefore speed
                // up lookups by making the per block index larger at the cost of reducing the
                // number of values that may be stored per block.
                //
                // X = values per block
                // Y = keys per block
                //
                // R = config.lsm_value_to_key_layout_ratio_min
                //
                // To maximize:
                //     Y
                // Given constraints:
                //     body >= X * value_size + Y * key_size
                //     (X * value_size) / (Y * key_size) >= R
                //     X >= Y
                //
                // Plots of above constraints:
                //     https://www.desmos.com/calculator/elqqaalgbc
                //
                // body - X * value_size = Y * key_size
                // Y = (body - X * value_size) / key_size
                //
                // (X * value_size) / (body - X * value_size) = R
                // (X * value_size) = R * body - R * X * value_size
                // (R + 1) * X * value_size = R * body
                // X = R * body / ((R + 1)* value_size)
                //
                // Y = (body - (R * body / ((R + 1) * value_size)) * value_size) / key_size
                // Y = (body - (R / (R + 1)) * body) / key_size
                // Y = body / ((R + 1) * key_size)
                var block_keys_layout_count = math.min(
                    block_body_size / ((config.lsm_value_to_key_layout_ratio_min + 1) * key_size),
                    block_body_size / (value_size + key_size),
                );

                // Round to the next lowest power of two. This speeds up lookups in the Eytzinger
                // layout and should help ensure better alignment for the following values.
                // We could round to the nearest power of two, but then we would need
                // care to avoid breaking e.g. the X >= Y invariant above.
                block_keys_layout_count = math.floorPowerOfTwo(u64, block_keys_layout_count);

                // If the index is smaller than 16 keys then there are key sizes >= 4 such that
                // the total index size is not 64 byte cache line aligned.
                assert(@sizeOf(Key) >= 4);
                assert(@sizeOf(Key) % 4 == 0);
                if (block_keys_layout_count < config.cache_line_size / 4) {
                    block_keys_layout_count = 0;
                }
                assert((block_keys_layout_count * key_size) % config.cache_line_size == 0);

                const block_key_layout_size = block_keys_layout_count * key_size;
                const block_key_count = block_keys_layout_count - 1;

                const block_value_count_max =
                    (block_body_size - block_key_layout_size) / value_size;

                const data_index_entry_size = key_size + address_size + checksum_size;
                const filter_index_entry_size = address_size + checksum_size;

                // TODO audit/tune this number for split block bloom filters
                const filter_bytes_per_key = 2;
                const data_blocks_per_filter_block = filter.filter_size /
                    (block_value_count_max * filter_bytes_per_key);

                var data_index_size = 0;
                var filter_index_size = 0;
                var data_blocks = table_block_count_max - index_block_count;
                var filter_blocks = 0;
                while (true) : (data_blocks -= 1) {
                    data_index_size = data_index_entry_size * data_blocks;

                    filter_blocks = div_ceil(data_blocks, data_blocks_per_filter_block);
                    filter_index_size = filter_index_entry_size * filter_blocks;

                    const index_size = @sizeOf(vsr.Header) + data_index_size + filter_index_size;
                    const total_block_count = index_block_count + data_blocks + filter_blocks;
                    if (index_size <= block_size and total_block_count <= table_block_count_max) {
                        break;
                    }
                }

                const total_block_count = index_block_count + data_blocks + filter_blocks;
                assert(total_block_count <= table_block_count_max);

                break :blk .{
                    .block_key_count = block_key_count,
                    .block_key_layout_size = block_key_layout_size,
                    .block_value_count_max = block_value_count_max,

                    .data_blocks_per_filter_block = data_blocks_per_filter_block,

                    .data_block_count_max = data_blocks,
                    .filter_block_count_max = filter_blocks,
                };
            };

            const index_block_count = 1;
            const filter_block_count_max = layout.filter_block_count_max;
            const data_block_count_max = layout.data_block_count_max;

            const index = struct {
                const size = @sizeOf(vsr.Header) + filter_checksums_size + data_checksums_size +
                    keys_size + filter_addresses_size + data_addresses_size;

                const filter_checksums_offset = @sizeOf(vsr.Header);
                const filter_checksums_size = filter_block_count_max * checksum_size;

                const data_checksums_offset = filter_checksums_offset + filter_checksums_size;
                const data_checksums_size = data_block_count_max * checksum_size;

                const keys_offset = data_checksums_offset + data_checksums_size;
                const keys_size = data_block_count_max * key_size;

                const filter_addresses_offset = keys_offset + keys_size;
                const filter_addresses_size = filter_block_count_max * address_size;

                const data_addresses_offset = filter_addresses_offset + filter_addresses_size;
                const data_addresses_size = data_block_count_max * address_size;

                const padding_offset = data_addresses_offset + data_addresses_size;
                const padding_size = block_size - padding_offset;
            };

            const filter = struct {
                const filter_offset = @sizeOf(vsr.Header);
                const filter_size = block_size - filter_offset;

                const padding_offset = filter_offset + filter_size;
                const padding_size = block_size - padding_offset;
            };

            const data = struct {
                const key_count = layout.block_key_count;
                const value_count_max = layout.block_value_count_max;

                const key_layout_offset = @sizeOf(vsr.Header);
                const key_layout_size = layout.block_key_layout_size;

                const values_offset = key_layout_offset + key_layout_size;
                const values_size = value_count_max * value_size;

                const padding_offset = values_offset + values_size;
                const padding_size = block_size - padding_offset;
            };

            const compile_log_layout = false;
            comptime {
                if (compile_log_layout) {
                    @compileError(std.fmt.comptimePrint(
                        \\
                        \\
                        \\lsm parameters:
                        \\    key size: {}
                        \\    value size: {}
                        \\    table size max: {}
                        \\    block size: {}
                        \\layout:
                        \\    index block count: {}
                        \\    filter block count max: {}
                        \\    data block count max: {}
                        \\index:
                        \\    size: {}
                        \\    filter_checksums_offset: {}
                        \\    filter_checksums_size: {}
                        \\    data_checksums_offset: {}
                        \\    data_checksums_size: {}
                        \\    keys_offset: {}
                        \\    keys_size: {}
                        \\    filter_addresses_offset: {}
                        \\    filter_addresses_size: {}
                        \\    data_addresses_offset: {}
                        \\    data_addresses_size: {}
                        \\filter:
                        \\    filter_offset: {}
                        \\    filter_size: {}
                        \\data:
                        \\    key_count: {}
                        \\    value_count_max: {}
                        \\    key_layout_offset: {}
                        \\    key_layout_size: {}
                        \\    values_offset: {}
                        \\    values_size: {}
                        \\    padding_offset: {}
                        \\    padding_size: {}
                        \\
                    ,
                        .{
                            key_size,
                            value_size,
                            table_size_max,
                            block_size,

                            index_block_count,
                            filter_block_count_max,
                            data_block_count_max,

                            index.size,
                            index.filter_checksums_offset,
                            index.filter_checksums_size,
                            index.data_checksums_offset,
                            index.data_checksums_size,
                            index.keys_offset,
                            index.keys_size,
                            index.filter_addresses_offset,
                            index.filter_addresses_size,
                            index.data_addresses_offset,
                            index.data_addresses_size,

                            filter.filter_offset,
                            filter.filter_size,

                            data.key_count,
                            data.value_count_max,
                            data.key_layout_offset,
                            data.key_layout_size,
                            data.values_offset,
                            data.values_size,
                            data.padding_offset,
                            data.padding_size,
                        },
                    ));
                }
            }

            comptime {
                assert(index_block_count > 0);
                assert(filter_block_count_max > 0);
                assert(data_block_count_max > 0);
                assert(index_block_count + filter_block_count_max +
                    data_block_count_max <= table_block_count_max);
                const filter_bytes_per_key = 2;
                assert(filter_block_count_max * filter.filter_size >=
                    data_block_count_max * data.value_count_max * filter_bytes_per_key);

                assert(index.size == @sizeOf(vsr.Header) +
                    data_block_count_max * (key_size + address_size + checksum_size) +
                    filter_block_count_max * (address_size + checksum_size));
                assert(index.size == index.data_addresses_offset + index.data_addresses_size);
                assert(index.size <= block_size);
                assert(index.keys_size > 0);
                assert(index.keys_size % key_size == 0);
                assert(@divExact(index.data_addresses_size, @sizeOf(u64)) == data_block_count_max);
                assert(@divExact(index.filter_addresses_size, @sizeOf(u64)) == filter_block_count_max);
                assert(@divExact(index.data_checksums_size, @sizeOf(u128)) == data_block_count_max);
                assert(@divExact(index.filter_checksums_size, @sizeOf(u128)) == filter_block_count_max);
                assert(block_size == index.padding_offset + index.padding_size);
                assert(block_size == index.size + index.padding_size);

                // Split block bloom filters require filters to be a multiple of 32 bytes as they
                // use 256 bit blocks.
                assert(filter.filter_size % 32 == 0);
                assert(block_size == filter.padding_offset + filter.padding_size);
                assert(block_size == @sizeOf(vsr.Header) + filter.filter_size + filter.padding_size);

                if (data.key_count > 0) {
                    assert(data.key_count >= 3);
                    assert(math.isPowerOfTwo(data.key_count + 1));
                    assert(data.key_count + 1 == @divExact(data.key_layout_size, key_size));
                    assert(data.values_size / data.key_layout_size >=
                        config.lsm_value_to_key_layout_ratio_min);
                } else {
                    assert(data.key_count == 0);
                    assert(data.key_layout_size == 0);
                    assert(data.values_offset == data.key_layout_offset);
                }

                assert(data.value_count_max > 0);
                assert(data.value_count_max >= data.key_count);
                assert(@divExact(data.values_size, value_size) == data.value_count_max);
                assert(data.values_offset % config.cache_line_size == 0);
                // You can have any size value you want, as long as it fits
                // neatly into the CPU cache lines :)
                assert((data.value_count_max * value_size) % config.cache_line_size == 0);

                assert(data.padding_size >= 0);
                assert(block_size == @sizeOf(vsr.Header) + data.key_layout_size +
                    data.values_size + data.padding_size);
                assert(block_size == data.padding_offset + data.padding_size);

                // We expect no block padding at least for TigerBeetle's objects and indexes:
                if ((key_size == 8 and value_size == 128) or
                    (key_size == 8 and value_size == 64) or
                    (key_size == 16 and value_size == 16) or
                    (key_size == 32 and value_size == 32))
                {
                    assert(data.padding_size == 0);
                }
            }

            value_count_max: u32,
            blocks: []align(config.sector_size) [block_size]u8,

            /// Whether the in-memory table is free to accept the mutable table, or else flushing.
            /// As soon as the flush completes, the table will be invalidated and marked as free.
            free: bool,
            info: Manifest.TableInfo,
            flush: FlushIterator,

            pub fn init(allocator: mem.Allocator, commit_count_max: u32) !Table {
                // The in-memory immutable table is the same size as the mutable table:
                const value_count_max = commit_count_max * config.lsm_mutable_table_batch_multiple;

                const block_count = index_block_count + filter_block_count_max +
                    div_ceil(value_count_max, data.value_count_max);

                const blocks = try allocator.allocAdvanced(
                    [block_size]u8,
                    config.sector_size,
                    block_count,
                    .exact,
                );
                errdefer allocator.free(blocks);

                return Table{
                    .value_count_max = value_count_max,
                    .blocks = blocks,
                    .free = true,
                    .info = undefined,
                    .flush = undefined,
                };
            }

            pub fn deinit(table: *Table, allocator: mem.Allocator) void {
                allocator.free(table.blocks);
            }

            pub fn create_from_sorted_values(
                table: *Table,
                storage: *Storage,
                snapshot_min: u64,
                sorted_values: []const Value,
            ) void {
                assert(table.free);
                assert(snapshot_min > 0);
                assert(sorted_values.len > 0);
                assert(sorted_values.len <= data.value_count_max * data_block_count_max);
                assert(sorted_values.len <= table.value_count_max);

                var filter_blocks_index: u32 = 0;
                const filter_blocks = table.blocks[index_block_count..][0..filter_block_count_max];

                var builder: Builder = .{
                    .storage = storage,
                    .index_block = &table.blocks[0],
                    .filter_block = &filter_blocks[0],
                    .data_block = undefined,
                };
                filter_blocks_index += 1;

                var stream = sorted_values;

                // Do not slice by data_block_count_max as the mutable table may have less blocks.
                const data_blocks = table.blocks[index_block_count + filter_block_count_max ..];
                assert(data_blocks.len <= data_block_count_max);

                for (data_blocks) |*data_block| {
                    // TODO Fix compiler to see that this @alignCast is unnecessary:
                    const data_block_aligned = @alignCast(config.sector_size, data_block);
                    builder.data_block = data_block_aligned;

                    const slice = stream[0..math.min(data.value_count_max, stream.len)];
                    stream = stream[slice.len..];

                    builder.data_block_append_slice(slice);
                    builder.data_block_finish();

                    if (builder.filter_block_full() or stream.len == 0) {
                        builder.filter_block_finish();
                        builder.filter_block = @alignCast(
                            config.sector_size,
                            &filter_blocks[filter_blocks_index],
                        );
                        filter_blocks_index += 1;
                    }

                    if (stream.len == 0) break;

                    assert(data_block_values_used(data_block_aligned).len == data.value_count_max);
                } else {
                    // We must always copy *all* values from sorted_values into the table,
                    // which will result in breaking from the loop as `stream.len` is 0.
                    // This is the case even when all data blocks are completely filled.
                    unreachable;
                }
                assert(stream.len == 0);
                assert(filter_blocks_index <= filter_block_count_max);

                table.* = .{
                    .value_count_max = table.value_count_max,
                    .blocks = table.blocks,
                    .free = false,
                    .info = builder.index_block_finish(snapshot_min),
                    .flush = .{ .storage = storage },
                };
            }

            fn blocks_used(table: *Table) u32 {
                assert(!table.free);

                return Table.index_blocks_used(&table.blocks[0]);
            }

            fn filter_blocks_used(table: *Table) u32 {
                assert(!table.free);

                const index_block: BlockPtr = table.blocks[0];
                return Builder.index_filter_blocks_used(index_block);
            }

            // TODO(ifreund) This would be great to unit test.
            fn get(table: *Table, key: Key) ?*const Value {
                assert(!table.free);
                assert(table.info.address != 0);
                assert(table.info.snapshot_max == math.maxInt(u64));

                if (compare_keys(key, table.info.key_min) == .lt) return null;
                if (compare_keys(key, table.info.key_max) == .gt) return null;

                const index_block = &table.blocks[0];

                const i = index_data_block_for_key(index_block, key);

                const meta_block_count = index_block_count + filter_block_count_max;
                const data_blocks_used = index_data_blocks_used(index_block);
                const data_block = @alignCast(
                    config.sector_size,
                    &table.blocks[meta_block_count..][0..data_blocks_used][i],
                );

                if (verify) {
                    // TODO What else do we expect?
                    assert(compare_keys(key, index_data_keys(index_block)[i]) != .gt);
                    assert(index_data_addresses(index_block)[i] != 0);
                }

                // TODO(ifreund) Check the filter block before searching in the data block

                assert(@divExact(data.key_layout_size, key_size) == data.key_count + 1);
                const key_layout_bytes = @alignCast(
                    @alignOf(Key),
                    data_block[data.key_layout_offset..][0..data.key_layout_size],
                );
                const key_layout = mem.bytesAsValue([data.key_count + 1]Key, key_layout_bytes);

                const e = eytzinger(data.key_count, data.value_count_max);
                const values = e.search_values(
                    Key,
                    Value,
                    compare_keys,
                    key_layout,
                    data_block_values_used(data_block),
                    key,
                );

                switch (values.len) {
                    0 => return null,
                    else => {
                        const result = binary_search.binary_search_values(
                            Key,
                            Value,
                            key_from_value,
                            compare_keys,
                            values,
                            key,
                        );
                        if (result.exact) {
                            return &values[result.index];
                        } else {
                            return null;
                        }
                    },
                }
            }

            const Builder = struct {
                storage: *Storage,
                key_min: Key = undefined,
                key_max: Key = undefined,

                index_block: BlockPtr,
                filter_block: BlockPtr,
                data_block: BlockPtr,

                data_block_count: u32 = 0,
                value: u32 = 0,

                filter_block_count: u32 = 0,
                data_blocks_in_filter: u32 = 0,

                pub fn init(allocator: mem.Allocator) Builder {
                    _ = allocator;

                    // TODO
                }

                pub fn deinit(builder: *Builder, allocator: mem.Allocator) void {
                    _ = builder;
                    _ = allocator;

                    // TODO
                }

                pub fn data_block_append(builder: *Builder, value: Value) void {
                    const values_max = data_block_values(builder.data_block);
                    assert(values_max.len == data.value_count_max);

                    values_max[builder.value] = value;
                    builder.value += 1;

                    const fingerprint = bloom_filter.Fingerprint.create(key_from_value(value));
                    bloom_filter.add(fingerprint, filter_block_filter(builder.filter_block));
                }

                pub fn data_block_append_slice(builder: *Builder, values: []const Value) void {
                    assert(values.len > 0);
                    assert(builder.value + values.len <= data.value_count_max);

                    const values_max = data_block_values(builder.data_block);
                    assert(values_max.len == data.value_count_max);

                    mem.copy(Value, values_max[builder.value..], values);
                    builder.value += @intCast(u32, values.len);

                    for (values) |value| {
                        const key = key_from_value(value);
                        const fingerprint = bloom_filter.Fingerprint.create(mem.asBytes(&key));
                        bloom_filter.add(fingerprint, filter_block_filter(builder.filter_block));
                    }
                }

                pub fn data_block_full(builder: Builder) bool {
                    return builder.value == data.value_count_max;
                }

                pub fn data_block_finish(builder: *Builder) void {
                    // For each block we write the sorted values, initialize the Eytzinger layout,
                    // complete the block header, and add the block's max key to the table index.

                    assert(builder.value > 0);

                    const block = builder.data_block;
                    const values_max = data_block_values(block);
                    assert(values_max.len == data.value_count_max);

                    const values = values_max[0..builder.value];

                    if (verify) {
                        var a = values[0];
                        for (values[1..]) |b| {
                            assert(compare_keys(key_from_value(a), key_from_value(b)) == .lt);
                            a = b;
                        }
                    }

                    assert(@divExact(data.key_layout_size, key_size) == data.key_count + 1);
                    const key_layout_bytes = @alignCast(
                        @alignOf(Key),
                        block[data.key_layout_offset..][0..data.key_layout_size],
                    );
                    const key_layout = mem.bytesAsValue([data.key_count + 1]Key, key_layout_bytes);

                    const e = eytzinger(data.key_count, data.value_count_max);
                    e.layout_from_keys_or_values(
                        Key,
                        Value,
                        key_from_value,
                        sentinel_key,
                        values,
                        key_layout,
                    );

                    const values_padding = mem.sliceAsBytes(values_max[builder.value..]);
                    const block_padding = block[data.padding_offset..][0..data.padding_size];
                    mem.set(u8, values_padding, 0);
                    mem.set(u8, block_padding, 0);

                    const header_bytes = block[0..@sizeOf(vsr.Header)];
                    const header = mem.bytesAsValue(vsr.Header, header_bytes);

                    const address = builder.storage.block_free_set.acquire().?;

                    header.* = .{
                        .cluster = builder.storage.cluster,
                        .op = address,
                        .request = @intCast(u32, values.len),
                        .size = block_size - @intCast(u32, values_padding.len - block_padding.len),
                        .command = .block,
                    };

                    header.set_checksum_body(block[@sizeOf(vsr.Header)..header.size]);
                    header.set_checksum();

                    const key_max = key_from_value(values[values.len - 1]);

                    const current = builder.data_block_count;
                    index_data_keys(builder.index_block)[current] = key_max;
                    index_data_addresses(builder.index_block)[current] = address;
                    index_data_checksums(builder.index_block)[current] = header.checksum;

                    if (current == 0) builder.key_min = key_from_value(values[0]);
                    builder.key_max = key_max;

                    if (current == 0 and values.len == 1) {
                        assert(compare_keys(builder.key_min, builder.key_max) != .gt);
                    } else {
                        assert(compare_keys(builder.key_min, builder.key_max) == .lt);
                    }

                    builder.data_block_count += 1;
                    builder.value = 0;

                    builder.data_blocks_in_filter += 1;
                }

                /// Returns true if there is space for at least one more data block in the filter.
                pub fn filter_block_full(builder: Builder) bool {
                    assert(builder.data_blocks_in_filter <= layout.data_blocks_per_filter_block);
                    return builder.data_blocks_in_filter == layout.data_blocks_per_filter_block;
                }

                pub fn filter_block_finish(builder: *Builder) void {
                    const address = builder.storage.block_free_set.acquire().?;

                    const header_bytes = builder.filter_block[0..@sizeOf(vsr.Header)];
                    const header = mem.bytesAsValue(vsr.Header, header_bytes);
                    header.* = .{
                        .cluster = builder.storage.cluster,
                        .op = address,
                        .size = block_size - filter.padding_size,
                        .command = .block,
                    };

                    header.set_checksum_body(builder.filter_block[@sizeOf(vsr.Header)..header.size]);
                    header.set_checksum();

                    const current = builder.filter_block_count;
                    index_filter_addresses(builder.index_block)[current] = address;
                    index_filter_checksums(builder.index_block)[current] = header.checksum;

                    builder.filter_block_count += 1;
                    builder.data_blocks_in_filter = 0;
                }

                pub fn index_block_full(builder: Builder) bool {
                    return builder.data_block_count == data_block_count_max;
                }

                pub fn index_block_finish(builder: *Builder, snapshot_min: u64) Manifest.TableInfo {
                    assert(builder.data_block_count > 0);
                    assert(builder.value == 0);
                    assert(builder.filter_block_count > builder.data_block_count /
                        layout.data_blocks_per_filter_block);
                    assert(builder.data_blocks_in_filter == 0);

                    const index_block = builder.index_block;

                    const index_data_keys_padding = index_data_keys(index_block)[builder.data_block_count..];
                    const index_data_keys_padding_bytes = mem.sliceAsBytes(index_data_keys_padding);
                    mem.set(u8, index_data_keys_padding_bytes, 0);
                    mem.set(u64, index_data_addresses(index_block)[builder.data_block_count..], 0);
                    mem.set(u128, index_data_checksums(index_block)[builder.data_block_count..], 0);

                    mem.set(u64, index_filter_addresses(index_block)[builder.filter_block_count..], 0);
                    mem.set(u128, index_filter_checksums(index_block)[builder.filter_block_count..], 0);

                    mem.set(u8, index_block[index.padding_offset..][0..index.padding_size], 0);

                    const header_bytes = index_block[0..@sizeOf(vsr.Header)];
                    const header = mem.bytesAsValue(vsr.Header, header_bytes);

                    const address = builder.storage.block_free_set.acquire().?;

                    header.* = .{
                        .cluster = builder.storage.cluster,
                        .op = address,
                        .commit = builder.filter_block_count,
                        .request = builder.data_block_count,
                        .offset = snapshot_min,
                        .size = index.size,
                        .command = .block,
                    };
                    header.set_checksum_body(index_block[@sizeOf(vsr.Header)..header.size]);
                    header.set_checksum();

                    const info: Manifest.TableInfo = .{
                        .checksum = header.checksum,
                        .address = address,
                        .snapshot_min = snapshot_min,
                        .key_min = builder.key_min,
                        .key_max = builder.key_max,
                    };

                    assert(info.snapshot_max == math.maxInt(u64));

                    // Reset the builder to its initial state, leaving the buffers untouched.
                    builder.* = .{
                        .storage = builder.storage,
                        .key_min = undefined,
                        .key_max = undefined,
                        .index_block = builder.index_block,
                        .filter_block = builder.filter_block,
                        .data_block = builder.data_block,
                    };

                    return info;
                }
            };

            inline fn index_data_keys(index_block: BlockPtr) []Key {
                return mem.bytesAsSlice(Key, index_block[index.keys_offset..][0..index.keys_size]);
            }

            inline fn index_data_keys_const(index_block: BlockPtrConst) []const Key {
                return mem.bytesAsSlice(Key, index_block[index.keys_offset..][0..index.keys_size]);
            }

            inline fn index_data_keys_used_const(index_block: BlockPtrConst) []const Key {
                return index_data_keys_const(index_block)[0..index_data_blocks_used(index_block)];
            }

            inline fn index_data_addresses(index_block: BlockPtr) []u64 {
                return mem.bytesAsSlice(
                    u64,
                    index_block[index.data_addresses_offset..][0..index.data_addresses_size],
                );
            }

            inline fn index_data_checksums(index_block: BlockPtr) []u128 {
                return mem.bytesAsSlice(
                    u128,
                    index_block[index.data_checksums_offset..][0..index.data_checksums_size],
                );
            }

            inline fn index_filter_addresses(index_block: BlockPtr) []u64 {
                return mem.bytesAsSlice(
                    u64,
                    index_block[index.filter_addresses_offset..][0..index.filter_addresses_size],
                );
            }

            inline fn index_filter_checksums(index_block: BlockPtr) []u128 {
                return mem.bytesAsSlice(
                    u128,
                    index_block[index.filter_checksums_offset..][0..index.filter_checksums_size],
                );
            }

            inline fn index_snapshot_min(index_block: BlockPtrConst) u32 {
                const header = mem.bytesAsValue(vsr.Header, index_block[0..@sizeOf(vsr.Header)]);
                return @intCast(u32, header.offset);
            }

            inline fn index_blocks_used(index_block: BlockPtrConst) u32 {
                return 1 + index_filter_blocks_used(index_block) +
                    index_data_blocks_used(index_block);
            }

            inline fn index_filter_blocks_used(index_block: BlockPtrConst) u32 {
                const header = mem.bytesAsValue(vsr.Header, index_block[0..@sizeOf(vsr.Header)]);
                const used = @intCast(u32, header.commit);
                assert(used <= filter_block_count_max);
                return used;
            }

            inline fn index_data_blocks_used(index_block: BlockPtrConst) u32 {
                const header = mem.bytesAsValue(vsr.Header, index_block[0..@sizeOf(vsr.Header)]);
                const used = @intCast(u32, header.request);
                assert(used <= data_block_count_max);
                return used;
            }

            /// Returns the zero-based index of the data block that may contain the key.
            /// May be called on an index block only when the key is already in range of the table.
            inline fn index_data_block_for_key(index_block: BlockPtrConst, key: Key) u32 {
                // Because we store key_max in the index block we can use the raw binary search
                // here and avoid the extra comparison. If the search finds an exact match, we
                // want to return that data block. If the search does not find an exact match
                // it returns the index of the next greatest key, which again is the index of the
                // data block that may contain the key.
                const data_block_index = binary_search.binary_search_keys_raw(
                    Key,
                    compare_keys,
                    Table.index_data_keys_used_const(index_block),
                    key,
                );
                assert(data_block_index < index_data_blocks_used(index_block));
                return data_block_index;
            }

            inline fn data_block_values(data_block: BlockPtr) []Value {
                return mem.bytesAsSlice(
                    Value,
                    data_block[data.values_offset..][0..data.values_size],
                );
            }

            inline fn data_block_values_used(data_block: BlockPtr) []const Value {
                const header = mem.bytesAsValue(vsr.Header, data_block[0..@sizeOf(vsr.Header)]);
                // TODO we should be able to cross-check this with the header size
                // for more safety.
                const used = @intCast(u32, header.request);
                assert(used <= data.value_count_max);
                return data_block_values(data_block)[0..used];
            }

            // TODO(ifreund): Should we use `BlockPtrConst` as we had before?
            // The reason for `Block` is so that HashMapContext's can call this without alignCast.
            inline fn block_address(block: Block) u64 {
                const header = mem.bytesAsValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);
                const address = header.op;
                // TODO(ifreund) We had an intCast(u32) here before but it didn't make sense?
                return address;
            }

            inline fn filter_block_filter(filter_block: BlockPtr) []u8 {
                return filter_block[filter.filter_offset..][0..filter.filter_size];
            }

            pub const FlushIterator = struct {
                const Callback = fn () void;

                storage: *Storage,
                write: Storage.Write = undefined,

                /// The index of the block that is currently being written/will be written next.
                block: u32 = 0,
                blocks_max: u32 = undefined,
                callback: Callback = undefined,

                pub fn flush(it: *FlushIterator, blocks_max: ?u32, callback: Callback) void {
                    it.blocks_max = blocks_max orelse math.maxInt(u32);
                    it.callback = callback;
                    it.flush_internal();
                }

                fn flush_internal(it: *FlushIterator) void {
                    const table = @fieldParentPtr(Table, "flush_iterator", it);

                    const index_header = mem.bytesAsValue(
                        vsr.Header,
                        table.blocks[0][0..@sizeOf(vsr.Header)],
                    );

                    const filter_blocks_used = index_header.commit;
                    const data_blocks_used = index_header.request;
                    const total_blocks_used = 1 + filter_blocks_used + data_blocks_used;

                    if (it.block == total_blocks_used) {
                        it.flush_complete();
                        return;
                    }
                    assert(it.block < total_blocks_used);

                    if (it.blocks_max == 0) {
                        it.flush_complete();
                        return;
                    }

                    if (it.block == 1 + filter_blocks_used) {
                        // TODO(ifreund) When skipping over unused filter blocks, let's assert
                        // here that they all have a zero address, with Builder zeroing the address.
                        // This way, if there's any mismatch, we'll catch it.
                        // This is one of the tricky things about how we have a full allocation of
                        // blocks, but don't always use all the allocated filter blocks.
                        const filter_blocks_unused = filter_block_count_max - filter_blocks_used;
                        it.block += @intCast(u32, filter_blocks_unused);
                    }

                    const block = @alignCast(config.sector_size, &table.blocks[it.block]);
                    const header = mem.bytesAsValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);
                    const address = header.op;
                    it.storage.write_block(on_flush, &it.write, block, address);
                }

                fn flush_complete(it: *FlushIterator) void {
                    const callback = it.callback;
                    it.blocks_max = undefined;
                    it.callback = undefined;
                    callback();
                }

                fn on_flush(write: *Storage.Write) void {
                    const it = @fieldParentPtr(FlushIterator, "write", write);
                    it.block += 1;
                    it.blocks_max -= 1;
                    it.flush_internal();
                }
            };
        };

        /// Compact tables in level A with overlapping tables in level B.
        pub const Compaction = struct {
            pub const Callback = fn (it: *Compaction, done: bool) void;

            const level_0_table_count_max = table_count_max_for_level(config.lsm_growth_factor, 0);
            const level_a_table_count_max = level_0_table_count_max;

            const LevelAIterator = TableIterator(Compaction, on_io_done);
            const LevelBIterator = LevelIterator(Compaction, on_io_done);

            const MergeIterator = KWayMergeIterator(
                Compaction,
                Key,
                Value,
                key_from_value,
                compare_keys,
                // Add one for the level B iterator:
                level_a_table_count_max + 1,
                stream_peek,
                stream_pop,
                stream_precedence,
            );

            const BlockWrite = struct {
                block: BlockPtr,
                submit: bool,
                write: Storage.Write,
            };

            storage: *Storage,
            ticks: u32 = 0,
            io_pending: u32 = 0,
            callback: ?Callback = null,
            /// This is an implementation detail, the caller should use the done
            /// argument of the Callback to know when the compaction has finished.
            last_tick: bool = false,

            /// Addresses of all source tables in level A:
            level_a_table_count: u32,
            level_a_iterators_max: [level_a_table_count_max]LevelAIterator,

            level_b_iterator: LevelBIterator,

            merge_iterator: MergeIterator,

            table_builder: Table.Builder,

            index: BlockWrite,
            filter: BlockWrite,
            data: BlockWrite,

            pub fn init(allocator: mem.Allocator) Compaction {
                // TODO
                _ = allocator;
            }

            pub fn deinit(compaction: *Compaction) void {
                // TODO
                _ = compaction;
            }

            pub fn start(
                compaction: *Compaction,
                level_a_tables: []u64,
                level_b: u32,
                level_b_key_min: Key,
                level_b_key_max: Key,
            ) void {
                _ = level_b;
                _ = level_b_key_min;
                _ = level_b_key_max;

                assert(compaction.io_pending == 0);
                // There are at least 2 table inputs to the compaction.
                assert(level_a_tables.len + 1 >= 2);
            }

            pub fn tick(compaction: *Compaction, callback: Callback) void {
                assert(!compaction.last_tick);
                assert(compaction.io_pending == 0);
                assert(compaction.callback == null);
                compaction.callback = callback;

                // Submit all read/write I/O before starting the CPU intensive k way merge.
                // This allows the I/O to happen in parallel with the CPU work.
                if (compaction.ticks >= 0) compaction.tick_read();
                if (compaction.ticks >= 2) compaction.tick_write();

                if (compaction.ticks == 1) {
                    // We can't initialize the k way merge until we have at least one
                    // value to peek() from each read stream.
                    const k = compaction.level_a_table_count + 1;
                    assert(k >= 2);
                    compaction.merge_iterator = MergeIterator.init(compaction, k, .ascending);
                }

                if (compaction.ticks >= 1) {
                    if (compaction.merge_iterator.empty()) {
                        assert(compaction.ticks >= 2);
                        assert(!compaction.last_tick);
                        compaction.last_tick = true;
                    } else {
                        compaction.tick_merge();
                    }
                }

                compaction.ticks += 1;

                // We will always start I/O if the compaction has not yet been completed.
                // The callbacks for this I/O must fire asynchronously.
                assert(compaction.io_pending > 0);
            }

            fn tick_read(compaction: *Compaction) void {
                for (compaction.level_a_iterators()) |*it| {
                    if (it.tick()) compaction.io_pending += 1;
                }
                if (compaction.level_b_iterator.tick()) compaction.io_pending += 1;

                if (compaction.last_tick) assert(compaction.io_pending == 0);
            }

            fn tick_write(compaction: *Compaction) void {
                assert(compaction.ticks >= 2);
                assert(compaction.data.submit);

                compaction.maybe_submit_write(compaction.data, on_block_write("data"));
                compaction.maybe_submit_write(compaction.filter, on_block_write("filter"));
                compaction.maybe_submit_write(compaction.index, on_block_write("index"));

                assert(compaction.io_pending > 0);
                assert(!compaction.data.submit);
                assert(!compaction.filter.submit);
                assert(!compaction.index.submit);
            }

            fn tick_merge(compaction: *Compaction) void {
                assert(!compaction.last_tick);
                assert(compaction.ticks >= 1);
                assert(!compaction.merge_iterator.empty());

                assert(!compaction.data.submit);
                assert(!compaction.filter.submit);
                assert(!compaction.index.submit);

                while (!compaction.table_builder.data_block_full()) {
                    const value = compaction.merge_iterator.pop() orelse {
                        compaction.assert_read_iterators_empty();
                        break;
                    };
                    compaction.table_builder.data_block_append(value);
                }
                compaction.table_builder.data_block_finish();
                swap_buffers(&compaction.data, &compaction.table_builder.data_block);

                if (!compaction.merge_iterator.empty()) {
                    const values_used = Table.data_block_values_used(compaction.data.block).len;
                    assert(values_used == Table.data.value_count_max);
                }

                if (compaction.table_builder.filter_block_full() or
                    compaction.table_builder.index_block_full() or
                    compaction.merge_iterator.empty())
                {
                    compaction.table_builder.filter_block_finish();
                    swap_buffers(&compaction.filter, &compaction.table_builder.filter_block);
                }

                if (compaction.table_builder.index_block_full() or
                    compaction.merge_iterator.empty())
                {
                    const info = compaction.table_builder.index_block_finish();
                    swap_buffers(&compaction.index, &compaction.table_builder.index_block);

                    // TODO store info in the manifest at some point. We may need to wait
                    // until the table has been fully written to disk, or until the
                    // compaction finishes. Figure this out when implementing the Manifest.
                    _ = info;
                }

                assert(compaction.data.submit);
            }

            fn on_io_done(compaction: *Compaction) void {
                compaction.io_pending -= 1;
                if (compaction.io_pending == 0) {
                    const callback = compaction.callback.?;
                    compaction.callback = null;
                    callback(compaction, compaction.last_tick);
                }
            }

            fn maybe_submit_write(
                compaction: *Compaction,
                block_write: *BlockWrite,
                callback: fn (*Storage.Write) void,
            ) void {
                if (block_write.submit) {
                    block_write.submit = false;

                    compaction.io_pending += 1;
                    compaction.storage.write_block(
                        callback,
                        &block_write.write,
                        block_write.block,
                        Table.block_address(block_write.block),
                    );
                }
            }

            fn on_block_write(comptime field: []const u8) fn (*Storage.Write) void {
                return struct {
                    fn callback(write: *Storage.Write) void {
                        const block_write = @fieldParentPtr(BlockWrite, "write", write);
                        const compaction = @fieldParentPtr(Compaction, field, block_write);
                        on_io_done(compaction);
                    }
                }.callback;
            }

            fn swap_buffers(block_write: *BlockWrite, filled_block: *BlockPtr) void {
                mem.swap(*BlockPtr, &block_write.block, filled_block);
                assert(!block_write.submit);
                block_write.submit = true;
            }

            fn level_a_iterators(compaction: *Compaction) []LevelAIterator {
                return compaction.level_a_iterators_max[0..compaction.level_a_table_count];
            }

            fn assert_read_iterators_empty(compaction: Compaction) void {
                for (compaction.level_a_iterators()) |it| {
                    assert(it.buffered_all_values());
                    assert(it.peek() == null);
                }
                assert(compaction.level_b_iterator.buffered_all_values());
                assert(compaction.level_b_iterator.peek() == null);
            }

            fn stream_peek(compaction: *Compaction, stream_id: u32) ?Key {
                if (stream_id == 0) {
                    return compaction.level_b_iterator.peek();
                } else {
                    return compaction.level_a_iterators()[stream_id + 1].peek();
                }
            }

            fn stream_pop(compaction: *Compaction, stream_id: u32) Value {
                if (stream_id == 0) {
                    return compaction.level_b_iterator.pop();
                } else {
                    return compaction.level_a_iterators()[stream_id + 1].pop();
                }
            }

            /// Returns true if stream a has higher precedence than stream b.
            /// This is used to deduplicate values across streams.
            ///
            /// This assumes that all overlapping tables in level A at the time the compaction was
            /// started are included in the compaction. If this is not the case, the older table
            /// in a pair of overlapping tables could be left in level A and shadow the newer table
            /// in level B, resulting in data loss/invalid data.
            fn stream_precedence(compaction: *Compaction, a: u32, b: u32) bool {
                assert(a != b);
                // A stream_id of 0 indicates the level B iterator.
                // All tables in level A have higher precedence.
                if (a == 0) return false;
                if (b == 0) return true;

                const it_a = compaction.level_a_iterators()[a + 1];
                const it_b = compaction.level_a_iterators()[b + 1];

                const snapshot_min_a = Table.index_snapshot_min(it_a.index);
                const snapshot_min_b = Table.index_snapshot_min(it_b.index);

                assert(snapshot_min_a != snapshot_min_b);
                return snapshot_min_a > snapshot_min_b;
            }
        };

        fn LevelIterator(comptime Parent: type, comptime read_done: fn (*Parent) void) type {
            return struct {
                const LevelIteratorGeneric = @This();

                const ValuesRingBuffer = RingBuffer(Value, Table.data.value_count_max, .pointer);
                const TablesRingBuffer = RingBuffer(
                    TableIterator(LevelIteratorGeneric, on_read_done),
                    2,
                    .array,
                );

                storage: *Storage,
                parent: *Parent,
                level: u32,
                key_min: Key,
                key_max: Key,
                values: ValuesRingBuffer,
                tables: TablesRingBuffer,

                fn init(allocator: mem.Allocator) !LevelIteratorGeneric {
                    var values = try ValuesRingBuffer.init(allocator);
                    errdefer values.deinit(allocator);

                    var table_a = try TableIterator.init(allocator);
                    errdefer table_a.deinit(allocator);

                    var table_b = try TableIterator.init(allocator);
                    errdefer table_b.deinit(allocator);

                    return LevelIteratorGeneric{
                        .storage = undefined,
                        .parent = undefined,
                        .level = undefined,
                        .key_min = undefined,
                        .key_max = undefined,
                        .values = values,
                        .tables = .{
                            .buffer = .{
                                table_a,
                                table_b,
                            },
                        },
                    };
                }

                fn deinit(it: *LevelIteratorGeneric, allocator: mem.Allocator) void {
                    it.values.deinit(allocator);
                    for (it.tables.buffer) |*table| table.deinit(allocator);
                    it.* = undefined;
                }

                fn reset(
                    it: *LevelIteratorGeneric,
                    storage: *Storage,
                    parent: *Parent,
                    level: u32,
                    key_min: Key,
                    key_max: Key,
                ) void {
                    it.* = .{
                        .storage = storage,
                        .parent = parent,
                        .level = level,
                        .key_min = key_min,
                        .key_max = key_max,
                        .values = .{ .buffer = it.values.buffer },
                        .tables = .{ .buffer = it.tables.buffer },
                    };
                    assert(it.values.empty());
                    assert(it.tables.empty());
                }

                fn tick(it: *LevelIteratorGeneric) bool {
                    if (it.buffered_enough_values()) return false;

                    if (it.tables.tail_ptr()) |tail| {
                        // Buffer values as necessary for the current tail.
                        if (tail.tick()) return true;
                        // Since buffered_enough_values() was false above and tick did not start
                        // new I/O, the tail table must have already buffered all values.
                        // This is critical to ensure no values are skipped during iteration.
                        assert(tail.buffered_all_values());
                    }

                    if (it.tables.next_tail_ptr()) |next_tail| {
                        read_next_table(next_tail);
                        it.tables.advance_tail();
                        return true;
                    } else {
                        const table = it.tables.head_ptr().?;
                        while (table.peek() != null) {
                            it.values.push(table.pop()) catch unreachable;
                        }
                        it.tables.advance_head();

                        read_next_table(it.tables.next_tail_ptr().?);
                        it.tables.advance_tail();
                        return true;
                    }
                }

                fn read_next_table(table: *TableIterator(LevelIteratorGeneric, on_read_done)) void {
                    // TODO Implement get_next_address()
                    //const address = table.parent.manifest.get_next_address() orelse return false;
                    if (true) @panic("implement get_next_address()");
                    const address = 0;
                    table.reset(address);
                    const read_pending = table.tick();
                    assert(read_pending);
                }

                fn on_read_done(it: *LevelIteratorGeneric) void {
                    if (!it.tick()) {
                        assert(it.buffered_enough_values());
                        read_done(it.parent);
                    }
                }

                /// Returns true if all remaining values in the level have been buffered.
                fn buffered_all_values(it: LevelIteratorGeneric) bool {
                    _ = it;

                    // TODO look at the manifest to determine this.
                    return false;
                }

                fn buffered_value_count(it: LevelIteratorGeneric) u32 {
                    var value_count = @intCast(u32, it.values.count);
                    var tables_it = it.tables.iterator();
                    while (tables_it.next()) |table| {
                        value_count += table.buffered_value_count();
                    }
                    return value_count;
                }

                fn buffered_enough_values(it: LevelIteratorGeneric) bool {
                    return it.buffered_all_values() or
                        it.buffered_value_count() >= Table.data.value_count_max;
                }

                fn peek(it: LevelIteratorGeneric) ?Key {
                    if (it.values.head()) |value| return key_from_value(value);

                    const table = it.tables.head_ptr_const() orelse {
                        assert(it.buffered_all_values());
                        return null;
                    };

                    return table.peek().?;
                }

                /// This is only safe to call after peek() has returned non-null.
                fn pop(it: *LevelIteratorGeneric) Value {
                    if (it.values.pop()) |value| return value;

                    const table = it.tables.head_ptr().?;
                    const value = table.pop();

                    if (table.peek() == null) it.tables.advance_head();

                    return value;
                }
            };
        }

        fn TableIterator(comptime Parent: type, comptime read_done: fn (*Parent) void) type {
            _ = read_done; // TODO

            return struct {
                const TableIteratorGeneric = @This();

                const ValuesRingBuffer = RingBuffer(Value, Table.data.value_count_max, .pointer);

                storage: *Storage,
                parent: *Parent,
                read_table_index: bool,
                address: u64,
                checksum: u128,

                index: BlockPtr,
                /// The index of the current block in the table index block.
                block: u32,

                /// This ring buffer is used to hold not yet popped values in the case that we run
                /// out of blocks in the blocks ring buffer but haven't buffered a full block of
                /// values in memory. In this case, we copy values from the head of blocks to this
                /// ring buffer to make that block available for reading further values.
                /// Thus, we guarantee that iterators will always have at least a block's worth of
                /// values buffered. This simplifies the peek() interface as null always means that
                /// iteration is complete.
                values: ValuesRingBuffer,

                blocks: RingBuffer(BlockPtr, 2, .array),
                /// The index of the current value in the head of the blocks ring buffer.
                value: u32,

                read: Storage.Read = undefined,
                /// This field is only used for safety checks, it does not affect the behavior.
                read_pending: bool = false,

                fn init(allocator: mem.Allocator) !TableIteratorGeneric {
                    const index = try allocator.alignedAlloc(u8, config.sector_size, block_size);
                    errdefer allocator.free(index);

                    const values = try ValuesRingBuffer.init(allocator);
                    errdefer values.deinit(allocator);

                    const block_a = try allocator.alignedAlloc(u8, config.sector_size, block_size);
                    errdefer allocator.free(block_a);

                    const block_b = try allocator.alignedAlloc(u8, config.sector_size, block_size);
                    errdefer allocator.free(block_b);

                    return .{
                        .storage = undefined,
                        .parent = undefined,
                        .read_table_index = undefined,
                        // Use 0 so that we can assert(address != 0) in tick().
                        .address = 0,
                        .checksum = undefined,
                        .index = index[0..block_size],
                        .block = undefined,
                        .values = values,
                        .blocks = .{
                            .buffer = .{
                                block_a[0..block_size],
                                block_b[0..block_size],
                            },
                        },
                        .value = undefined,
                    };
                }

                fn deinit(it: *TableIteratorGeneric, allocator: mem.Allocator) void {
                    assert(!it.read_pending);

                    allocator.free(it.index);
                    it.values.deinit(allocator);
                    for (it.blocks.buffer) |block| allocator.free(block);
                    it.* = undefined;
                }

                fn reset(
                    it: *TableIteratorGeneric,
                    storage: *Storage,
                    parent: *Parent,
                    address: u64,
                    checksum: u128,
                ) void {
                    assert(!it.read_pending);
                    it.* = .{
                        .storage = storage,
                        .parent = parent,
                        .read_table_index = true,
                        .address = address,
                        .checksum = checksum,
                        .index = it.index,
                        .block = 0,
                        .values = .{ .buffer = it.values.buffer },
                        .blocks = .{ .buffer = it.blocks.buffer },
                        .value = 0,
                    };
                    assert(it.values.empty());
                    assert(it.blocks.empty());
                }

                /// Try to buffer at least a full block of values to be peek()'d.
                /// A full block may not always be buffered if all 3 blocks are partially full
                /// or if the end of the table is reached.
                /// Returns true if an IO operation was started. If this returns true,
                /// then read_done() will be called on completion.
                fn tick(it: *TableIteratorGeneric) bool {
                    assert(!it.read_pending);
                    assert(it.address != 0);

                    if (it.read_table_index) {
                        assert(!it.read_pending);
                        it.read_pending = true;
                        it.storage.read_block(
                            on_read_table_index,
                            &it.read,
                            it.index,
                            it.address,
                            it.checksum,
                        );
                        return true;
                    }

                    if (it.buffered_enough_values()) return false;

                    if (it.blocks.next_tail()) |next_tail| {
                        it.read_next_data_block(next_tail);
                        return true;
                    } else {
                        const values = Table.data_block_values_used(it.blocks.head().?);
                        const values_remaining = values[it.value..];
                        it.values.push_slice(values_remaining) catch unreachable;
                        it.value = 0;
                        it.blocks.advance_head();
                        it.read_next_data_block(it.blocks.next_tail().?);
                        return true;
                    }
                }

                fn read_next_data_block(it: *TableIteratorGeneric, block: BlockPtr) void {
                    assert(!it.read_table_index);
                    assert(it.block < Table.index_data_blocks_used(it.index));

                    const addresses = Table.index_data_addresses(it.index);
                    const checksums = Table.index_data_checksums(it.index);
                    const address = addresses[it.block];
                    const checksum = checksums[it.block];

                    assert(!it.read_pending);
                    it.read_pending = true;
                    it.storage.read_block(on_read, &it.read, block, address, checksum);
                }

                fn on_read_table_index(read: *Storage.Read) void {
                    const it = @fieldParentPtr(TableIteratorGeneric, "read", read);
                    assert(it.read_pending);
                    it.read_pending = false;

                    assert(it.read_table_index);
                    it.read_table_index = false;

                    const read_pending = it.tick();
                    // After reading the table index, we always read at least one data block.
                    assert(read_pending);
                }

                fn on_read(read: *Storage.Read) void {
                    const it = @fieldParentPtr(TableIteratorGeneric, "read", read);
                    assert(it.read_pending);
                    it.read_pending = false;

                    assert(!it.read_table_index);

                    it.blocks.advance_tail();
                    it.block += 1;

                    if (!it.tick()) {
                        assert(it.buffered_enough_values());
                        read_done(it.parent);
                    }
                }

                /// Return true if all remaining values in the table have been buffered in memory.
                fn buffered_all_values(it: TableIteratorGeneric) bool {
                    assert(!it.read_pending);

                    const data_blocks_used = Table.index_data_blocks_used(it.index);
                    assert(it.block <= data_blocks_used);
                    return it.block == data_blocks_used;
                }

                fn buffered_value_count(it: TableIteratorGeneric) u32 {
                    assert(!it.read_pending);

                    var value_count = it.values.count;
                    var blocks_it = it.blocks.iterator();
                    while (blocks_it.next()) |block| {
                        value_count += Table.data_block_values_used(block).len;
                    }
                    // We do this subtraction last to avoid underflow.
                    value_count -= it.value;

                    return @intCast(u32, value_count);
                }

                fn buffered_enough_values(it: TableIteratorGeneric) bool {
                    assert(!it.read_pending);

                    return it.buffered_all_values() or
                        it.buffered_value_count() >= Table.data.value_count_max;
                }

                fn peek(it: TableIteratorGeneric) ?Key {
                    assert(!it.read_pending);
                    assert(!it.read_table_index);

                    if (it.values.head()) |value| return key_from_value(value);

                    const block = it.blocks.head() orelse {
                        assert(it.block == Table.index_data_blocks_used(it.index));
                        return null;
                    };

                    const values = Table.data_block_values_used(block);
                    return key_from_value(values[it.value]);
                }

                /// This is only safe to call after peek() has returned non-null.
                fn pop(it: *TableIteratorGeneric) Value {
                    assert(!it.read_pending);
                    assert(!it.read_table_index);

                    if (it.values.pop()) |value| return value;

                    const block = it.blocks.head().?;

                    const values = Table.data_block_values_used(block);
                    const value = values[it.value];

                    it.value += 1;
                    if (it.value == values.len) {
                        it.value = 0;
                        it.blocks.advance_head();
                    }

                    return value;
                }
            };
        }

        pub const PrefetchKeys = std.AutoHashMapUnmanaged(Key, void);
        pub const PrefetchValues = std.HashMapUnmanaged(Value, void, HashMapContextValue, 70);

        // TODO(ifreund) Replace both of these types with SetAssociativeCache:
        pub const ValueCache = std.HashMapUnmanaged(Value, void, HashMapContextValue, 70);
        pub const BlockCache = std.HashMapUnmanaged(Block, void, HashMapContextBlock, 70);

        storage: *Storage,

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

        /// A set associative cache of values shared by trees with the same key/value sizes.
        /// This is used to accelerate point lookups and is not used for range queries.
        /// Secondary index trees used only for range queries can therefore set this to null.
        value_cache: ?*ValueCache,

        /// A set associative cache of blocks shared by all trees.
        block_cache: *BlockCache,

        mutable_table: MutableTable,
        table: Table,

        manifest: Manifest,

        pub const Options = struct {
            /// The maximum number of keys that may need to be prefetched before commit.
            prefetch_count_max: u32,

            /// The maximum number of keys that may be committed per batch.
            commit_count_max: u32,
        };

        pub fn init(
            allocator: mem.Allocator,
            storage: *Storage,
            node_pool: *NodePool,
            value_cache: ?*ValueCache,
            block_cache: *BlockCache,
            options: Options,
        ) !TreeGeneric {
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

            var table = try Table.init(allocator, options.commit_count_max);
            errdefer table.deinit(allocator);

            var manifest = try Manifest.init(allocator, node_pool);
            errdefer manifest.deinit(allocator);

            return TreeGeneric{
                .storage = storage,
                .options = options,
                .prefetch_keys = prefetch_keys,
                .prefetch_values = prefetch_values,
                .value_cache = value_cache,
                .block_cache = block_cache,
                .mutable_table = mutable_table,
                .table = table,
                .manifest = manifest,
            };
        }

        pub fn deinit(tree: *TreeGeneric, allocator: mem.Allocator) void {
            // TODO Consider whether we should release blocks acquired from Storage.block_free_set.
            tree.prefetch_keys.deinit(allocator);
            tree.prefetch_values.deinit(allocator);
            tree.mutable_table.deinit(allocator);
            tree.table.deinit(allocator);
            tree.manifest.deinit(allocator);
        }

        pub fn get(tree: *TreeGeneric, key: Key) ?*const Value {
            // Ensure that prefetch() was called and completed if any keys were enqueued:
            assert(tree.prefetch_keys.count() == 0);
            assert(tree.prefetch_keys_iterator == null);

            const value = tree.mutable_table.get(key) orelse
                tree.value_cache.?.getKeyPtr(tombstone_from_key(key)) orelse
                tree.prefetch_values.getKeyPtr(tombstone_from_key(key));

            return unwrap_tombstone(value);
        }

        pub fn put(tree: *TreeGeneric, value: Value) void {
            tree.mutable_table.put(value);
        }

        pub fn remove(tree: *TreeGeneric, key: Key) void {
            tree.mutable_table.remove(key);
        }

        pub fn lookup(
            tree: *TreeGeneric,
            snapshot: u64,
            key: Key,
            callback: fn (value: ?*const Value) void,
        ) void {
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

            if (!tree.table.free and tree.table.info.visible(snapshot)) {
                if (tree.table.get(key)) |value| {
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

        pub fn flush(
            tree: *TreeGeneric,
            callback: fn () void,
        ) void {
            _ = tree;
            _ = callback;
        }

        /// This should be called by the state machine for every key that must be prefetched.
        pub fn prefetch_enqueue(tree: *TreeGeneric, key: Key) void {
            assert(tree.value_cache != null);
            assert(tree.prefetch_keys_iterator == null);

            if (tree.mutable_table.get(key) != null) return;
            if (tree.value_cache.?.contains(tombstone_from_key(key))) return;

            // We tolerate duplicate keys enqueued by the state machine.
            // For example, if all unique operations require the same two dependencies.
            tree.prefetch_keys.putAssumeCapacity(key, {});
        }

        /// Ensure keys enqueued by `prefetch_enqueue()` are in the cache when the callback returns.
        pub fn prefetch(tree: *TreeGeneric, callback: fn () void) void {
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

        fn prefetch_key(tree: *TreeGeneric, key: Key) bool {
            assert(tree.value_cache != null);

            if (verify) {
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
            tree: *TreeGeneric,
            snapshot: u64,
            query: RangeQuery,

            pub fn next(callback: fn (result: ?Value) void) void {
                _ = callback;
            }
        };

        pub fn range_query(
            tree: *TreeGeneric,
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

test {
    const testing = std.testing;
    const allocator = testing.allocator;

    const IO = @import("../io.zig").IO;
    const Storage = @import("../storage.zig").Storage;

    const dir_path = ".";
    const dir_fd = os.openZ(dir_path, os.O.CLOEXEC | os.O.RDONLY, 0) catch |err| {
        std.debug.print("failed to open directory '{s}': {}", .{ dir_path, err });
        return;
    };

    const storage_fd = try Storage.open(
        dir_fd,
        "lsm",
        config.journal_size_max,
        false, // Set this to true the first time to create the data file.
    );

    var io = try IO.init(128, 0);
    defer io.deinit();

    const cluster = 32;

    var storage = try Storage.init(allocator, &io, cluster, config.journal_size_max, storage_fd);
    defer storage.deinit(allocator);

    const Key = CompositeKey(u128);

    const TestTree = Tree(
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

    var value_cache = TestTree.ValueCache{};
    try value_cache.ensureTotalCapacity(allocator, 10000);
    defer value_cache.deinit(allocator);

    var block_cache = TestTree.BlockCache{};
    try block_cache.ensureTotalCapacity(allocator, 100);
    defer block_cache.deinit(allocator);

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

    var tree = try TestTree.init(
        allocator,
        &storage,
        &node_pool,
        &value_cache,
        &block_cache,
        .{
            .prefetch_count_max = commit_count_max * 2,
            .commit_count_max = commit_count_max,
        },
    );
    defer tree.deinit(allocator);

    testing.refAllDecls(@This());

    _ = TestTree.Table;
    _ = TestTree.Table.create_from_sorted_values;
    _ = TestTree.Table.get;
    _ = TestTree.Table.FlushIterator;
    _ = TestTree.Table.FlushIterator.tick;
    _ = TestTree.TableIterator;
    _ = TestTree.LevelIterator;
    _ = TestTree.Manifest.LookupIterator.next;
    _ = TestTree.Compaction;
    _ = TestTree.Table.Builder.data_block_finish;
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

    std.debug.print("table_count_max={}\n", .{table_count_max});
}
