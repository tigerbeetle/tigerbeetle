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
const KWayMergeIterator = @import("k_way_merge.zig").KWayMergeIterator;
const NodePool = @import("node_pool.zig").NodePool(config.lsm_manifest_node_size, 16);
const RingBuffer = @import("../ring_buffer.zig").RingBuffer;
const SuperBlockType = @import("superblock.zig").SuperBlockType;

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

pub fn TreeType(
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
    const Grid = @import("grid.zig").GridType(Storage);
    const Manifest = @import("manifest.zig").ManifestType(Key, Value, compare_keys);
    const MutableTable = @import("mutable_table.zig").MutableTableType(
        Storage,
        Key,
        Value, 
        compare_keys, 
        key_from_value, 
        sentinel_key,
    );
    const ImmutableTable = @import("immutable_table.zig").ImmutableTableType(
        Storage,
        Key,
        Value, 
        compare_keys, 
        key_from_value, 
        sentinel_key,
    );
    const Table = @import("table.zig").Table(
        Storage,
        Key,
        Value, 
        compare_keys, 
        key_from_value, 
        sentinel_key,
    );

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

        const HashMapContextValue = struct {
            pub fn eql(_: HashMapContextValue, a: Value, b: Value) bool {
                return compare_keys(key_from_value(a), key_from_value(b)) == .eq;
            }

            pub fn hash(_: HashMapContextValue, value: Value) u64 {
                const key = key_from_value(value);
                return std.hash_map.getAutoHashFn(Key, HashMapContextValue)(.{}, key);
            }
        };

        pub fn CompactionType(comptime LevelAIteratorType: anytype) type {
            // LevelAIterator requires:
            // - init(allocator) void
            // - deinit(allocator) void
            // - tick() bool
            // - peek() ?Key
            // - pop() Value

            return struct {
                const Compaction = @This();

                pub const Callback = fn (it: *Compaction, done: bool) void;

                //const LevelAIterator = TableIteratorType(Compaction, io_callback);
                const LevelAIterator = LevelAIteratorType(Compaction);
                const LevelBIterator = LevelIteratorType(Compaction, io_callback);

                const k = 2;
                const MergeIterator = KWayMergeIterator(
                    Compaction,
                    Key,
                    Value,
                    key_from_value,
                    compare_keys,
                    k,
                    stream_peek,
                    stream_pop,
                    stream_precedence,
                );

                const BlockWrite = struct {
                    block: BlockPtr,
                    write: Grid.Write = undefined,
                    ready: bool = false,
                };

                manifest: *Manifest,
                grid: *Grid,

                ticks: u32 = 0,
                io_pending: u32 = 0,
                drop_tombstones: bool = false,
                callback: ?Callback = null,

                /// Private:
                /// The caller must use the Callback's `done` argument to know when compaction is done,
                /// because a write I/O may yet follow even after the merge is done.
                merge_done: bool = false,

                level_a_iterator: LevelAIterator,
                level_b_iterator: LevelBIterator,
                merge_iterator: MergeIterator,
                table_builder: Table.Builder,

                index: BlockWrite,
                filter: BlockWrite,
                data: BlockWrite,

                pub fn init(allocator: mem.Allocator, manifest: *Manifest, grid: *Grid) !Compaction {
                    var level_a_iterator = try LevelAIterator.init(allocator);
                    errdefer level_a_iterator.deinit(allocator);

                    var level_b_iterator = try LevelBIterator.init(allocator);
                    errdefer level_b_iterator.deinit(allocator);

                    var table_builder = try Table.Builder.init(allocator);
                    errdefer table_builder.deinit(allocator);

                    const index = BlockWrite{ .block = try allocate_block(allocator) };
                    errdefer allocator.free(index.block);

                    const filter = BlockWrite{ .block = try allocate_block(allocator) };
                    errdefer allocator.free(filter.block);

                    const data = BlockWrite{ .block = try allocate_block(allocator) };
                    errdefer allocator.free(data.block);

                    return Compaction{
                        .manifest = manifest,
                        .grid = grid,

                        .level_a_iterator = level_a_iterator,
                        .level_b_iterator = level_b_iterator,
                        .merge_iterator = undefined, // This must be initialized at tick 1.
                        .table_builder = table_builder,

                        .index = index,
                        .filter = filter,
                        .data = data,
                    };
                }

                fn allocate_block(allocator: mem.Allocator) !BlockPtr {
                    return allocator.alignedAlloc(u8, config.sector_size, config.block_size);
                }

                pub fn deinit(compaction: *Compaction, allocator: mem.Allocator) void {
                    compaction.level_a_iterator.deinit(allocator);
                    compaction.level_b_iterator.deinit(allocator);
                    compaction.table_builder.deinit(allocator);

                    allocator.free(compaction.index.block);
                    allocator.free(compaction.filter.block);
                    allocator.free(compaction.data.block);
                }

                pub fn start(
                    compaction: *Compaction,
                    level_a: u64,
                    level_b: u32,
                    level_b_key_min: Key,
                    level_b_key_max: Key,
                    drop_tombstones: bool,
                ) void {
                    // There are at least 2 table inputs to the compaction.
                    assert(level_a_tables.len + 1 >= 2);

                    _ = level_b;
                    _ = level_b_key_min;
                    _ = level_b_key_max;
                    _ = drop_tombstones;

                    compaction.ticks = 0;
                    assert(compaction.io_pending == 0);
                    compaction.drop_tombstones = drop_tombstones;
                    assert(compaction.callback == null);
                    compaction.merge_done = false;

                    // TODO Reset iterators and builder.
                    compaction.merge_iterator = undefined;

                    assert(!compaction.data.ready);
                    assert(!compaction.filter.ready);
                    assert(!compaction.index.ready);
                }

                /// Submits all read/write I/O before starting the CPU-intensive k-way merge.
                /// This allows the I/O to happen in parallel with the merge.
                ///
                /// The caller must call:
                ///
                /// 1. tick_io() across all trees,
                /// 2. io.submit() to submit these I/O operations to the kernel,
                /// 3. tick_cpu() across all trees.
                pub fn tick_io(compaction: *Compaction, callback: Callback) void {
                    assert(!compaction.merge_done);
                    assert(compaction.io_pending == 0);
                    assert(compaction.callback == null);

                    compaction.callback = callback;

                    if (compaction.ticks >= 0) compaction.tick_io_read();
                    if (compaction.ticks >= 2) compaction.tick_io_write();

                    // All values may be eclipsed by tombstones, with no write I/O pending here.
                }

                pub fn tick_cpu(compaction: *Compaction) void {
                    assert(!compaction.merge_done);
                    assert(compaction.io_pending >= 0);
                    assert(compaction.callback != null);

                    if (compaction.ticks == 1) {
                        // We cannot initialize the merge until we can peek() a value from each stream,
                        // which depends on tick 0 (to read blocks) having happened.
                        compaction.merge_iterator = MergeIterator.init(compaction, k, .ascending);
                    }

                    if (compaction.ticks >= 1) {
                        if (compaction.merge_iterator.empty()) {
                            assert(!compaction.merge_done);

                            // We must distinguish between merge_iterator.empty() and merge_done.
                            // The former cannot be accessed before MergeIterator.init() on tick 1.
                            compaction.merge_done = true;
                        } else {
                            compaction.tick_cpu_merge();
                        }
                    }

                    compaction.ticks += 1;

                    // Normally, a tick completes only after a read/write I/O.
                    // However, the compaction may drop only tombstones, resulting in no write I/O.
                    if (compaction.io_pending == 0) compaction.tick_done();
                }

                fn tick_done(compaction: *Compaction) void {
                    assert(compaction.io_pending == 0);

                    const callback = compaction.callback.?;
                    compaction.callback = null;

                    callback(compaction, compaction.merge_done);
                }

                fn tick_io_read(compaction: *Compaction) void {
                    assert(compaction.callback != null);

                    if (compaction.level_a_iterator.tick()) compaction.io_pending += 1;
                    if (compaction.level_b_iterator.tick()) compaction.io_pending += 1;

                    if (compaction.merge_done) assert(compaction.io_pending == 0);
                }

                fn tick_io_write(compaction: *Compaction) void {
                    assert(compaction.callback != null);
                    assert(compaction.ticks >= 2);
                    // There may be no data block to write if all values are eclipsed by tombstones.
                    assert(compaction.data.ready or !compaction.data.ready);

                    compaction.write_block_if_ready(&compaction.data, write_block_callback("data"));
                    compaction.write_block_if_ready(&compaction.filter, write_block_callback("filter"));
                    compaction.write_block_if_ready(&compaction.index, write_block_callback("index"));

                    assert(!compaction.data.ready);
                    assert(!compaction.filter.ready);
                    assert(!compaction.index.ready);
                }

                fn tick_cpu_merge(compaction: *Compaction) void {
                    assert(compaction.callback != null);
                    assert(compaction.ticks >= 1);
                    assert(!compaction.merge_done);
                    assert(!compaction.merge_iterator.empty());

                    assert(!compaction.data.ready);
                    assert(!compaction.filter.ready);
                    assert(!compaction.index.ready);

                    var tombstones_dropped: u32 = 0;
                    while (!compaction.table_builder.data_block_full()) {
                        const value = compaction.merge_iterator.pop() orelse {
                            compaction.assert_read_iterators_empty();
                            break;
                        };
                        if (compaction.drop_tombstones and tombstone(value)) {
                            tombstones_dropped += 1;
                        } else {
                            compaction.table_builder.data_block_append(value);
                        }
                    }

                    if (compaction.table_builder.data_block_empty()) {
                        assert(compaction.drop_tombstones);
                        assert(tombstones_dropped > 0);
                    } else {
                        compaction.table_builder.data_block_finish();
                        swap_buffers(&compaction.data, &compaction.table_builder.data_block);
                        assert(compaction.data.ready);

                        if (!compaction.merge_iterator.empty()) {
                            // Ensure that the block was filled completely.
                            const values_used = Table.data_block_values_used(compaction.data.block).len;
                            assert(values_used == Table.data.value_count_max);
                        }
                    }

                    if (compaction.table_builder.filter_block_full() or
                        compaction.table_builder.index_block_full() or
                        compaction.merge_iterator.empty())
                    {
                        if (compaction.table_builder.filter_block_empty()) {
                            assert(compaction.drop_tombstones);
                            assert(tombstones_dropped > 0);
                        } else {
                            compaction.table_builder.filter_block_finish();
                            swap_buffers(&compaction.filter, &compaction.table_builder.filter_block);
                            assert(compaction.filter.ready);
                        }
                    }

                    if (compaction.table_builder.index_block_full() or
                        compaction.merge_iterator.empty())
                    {
                        if (compaction.table_builder.index_block_empty()) {
                            assert(compaction.drop_tombstones);
                            assert(tombstones_dropped > 0);
                        } else {
                            const snapshot_min = compaction.manifest.take_snapshot();
                            const info = compaction.table_builder.index_block_finish(snapshot_min);
                            swap_buffers(&compaction.index, &compaction.table_builder.index_block);
                            assert(compaction.index.ready);

                            // TODO Push to an array as we must wait until the compaction has finished
                            // so that we can update the manifest atomically.
                            // Otherwise, interleaved state machine operations will see side-effects.
                            _ = info;
                        }
                    }
                }

                fn io_callback(compaction: *Compaction) void {
                    compaction.io_pending -= 1;

                    if (compaction.io_pending == 0) compaction.tick_done();
                }

                fn write_block_if_ready(
                    compaction: *Compaction,
                    block_write: *BlockWrite,
                    callback: fn (*Grid.Write) void,
                ) void {
                    if (block_write.ready) {
                        block_write.ready = false;

                        compaction.io_pending += 1;
                        compaction.grid.write_block(
                            callback,
                            &block_write.write,
                            block_write.block,
                            Table.block_address(block_write.block),
                        );
                    }
                }

                fn write_block_callback(comptime field: []const u8) fn (*Grid.Write) void {
                    return struct {
                        fn callback(write: *Grid.Write) void {
                            const block_write = @fieldParentPtr(BlockWrite, "write", write);
                            const compaction = @fieldParentPtr(Compaction, field, block_write);

                            io_callback(compaction);
                        }
                    }.callback;
                }

                fn swap_buffers(block_write: *BlockWrite, block_ready: *BlockPtr) void {
                    mem.swap(BlockPtr, &block_write.block, block_ready);

                    assert(!block_write.ready);
                    block_write.ready = true;
                }

                fn assert_read_iterators_empty(compaction: Compaction) void {
                    assert(compaction.level_a_iterator.buffered_all_values());
                    assert(compaction.level_a_iterator.peek() == null);

                    assert(compaction.level_b_iterator.buffered_all_values());
                    assert(compaction.level_b_iterator.peek() == null);
                }

                fn stream_peek(compaction: *Compaction, stream_id: u32) ?Key {
                    assert(stream_id <= 1);

                    if (stream_id == 0) {
                        return compaction.level_a_iterator.peek();
                    } else {
                        return compaction.level_b_iterator.peek();
                    }
                }

                fn stream_pop(compaction: *Compaction, stream_id: u32) Value {
                    assert(stream_id <= 1);

                    if (stream_id == 0) {
                        return compaction.level_a_iterator.pop();
                    } else {
                        return compaction.level_b_iterator.pop();
                    }
                }

                /// Returns true if stream A has higher precedence than stream B.
                /// This is used to deduplicate values across streams.
                ///
                /// This assumes that all overlapping tables in level A at the time the compaction was
                /// started are included in the compaction. If this is not the case, the older table
                /// in a pair of overlapping tables could be left in level A and shadow the newer table
                /// in level B, resulting in data loss/invalid data.
                fn stream_precedence(compaction: *Compaction, a: u32, b: u32) bool {
                    _ = compaction;

                    assert(a + b == 1);

                    // A stream_id of 0 indicates the level A iterator.
                    // All tables in level A have higher precedence.
                    return a == 0;
                }
            };
        }

        fn LevelIteratorType(comptime Parent: type, comptime read_done: fn (*Parent) void) type {
            return struct {
                const LevelIterator = @This();
                const TableIterator = TableIteratorType(LevelIterator, on_read_done);

                const ValuesRingBuffer = RingBuffer(Value, Table.data.value_count_max, .pointer);
                const TablesRingBuffer = RingBuffer(TableIterator, 2, .array);

                grid: *Grid,
                parent: *Parent,
                level: u32,
                key_min: Key,
                key_max: Key,
                values: ValuesRingBuffer,
                tables: TablesRingBuffer,

                fn init(allocator: mem.Allocator) !LevelIterator {
                    var values = try ValuesRingBuffer.init(allocator);
                    errdefer values.deinit(allocator);

                    var table_a = try TableIterator.init(allocator);
                    errdefer table_a.deinit(allocator);

                    var table_b = try TableIterator.init(allocator);
                    errdefer table_b.deinit(allocator);

                    return LevelIterator{
                        .grid = undefined,
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

                fn deinit(it: *LevelIterator, allocator: mem.Allocator) void {
                    it.values.deinit(allocator);
                    for (it.tables.buffer) |*table| table.deinit(allocator);
                    it.* = undefined;
                }

                fn reset(
                    it: *LevelIterator,
                    grid: *Grid,
                    parent: *Parent,
                    level: u32,
                    key_min: Key,
                    key_max: Key,
                ) void {
                    it.* = .{
                        .grid = grid,
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

                fn tick(it: *LevelIterator) bool {
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

                fn read_next_table(table: *TableIterator) void {
                    // TODO Implement get_next_address()
                    //const address = table.parent.manifest.get_next_address() orelse return false;
                    if (true) @panic("implement get_next_address()");
                    const address = 0;
                    table.reset(address);
                    const read_pending = table.tick();
                    assert(read_pending);
                }

                fn on_read_done(it: *LevelIterator) void {
                    if (!it.tick()) {
                        assert(it.buffered_enough_values());
                        read_done(it.parent);
                    }
                }

                /// Returns true if all remaining values in the level have been buffered.
                fn buffered_all_values(it: LevelIterator) bool {
                    _ = it;

                    // TODO look at the manifest to determine this.
                    return false;
                }

                fn buffered_value_count(it: LevelIterator) u32 {
                    var value_count = @intCast(u32, it.values.count);
                    var tables_it = it.tables.iterator();
                    while (tables_it.next()) |table| {
                        value_count += table.buffered_value_count();
                    }
                    return value_count;
                }

                fn buffered_enough_values(it: LevelIterator) bool {
                    return it.buffered_all_values() or
                        it.buffered_value_count() >= Table.data.value_count_max;
                }

                fn peek(it: LevelIterator) ?Key {
                    if (it.values.head()) |value| return key_from_value(value);

                    const table = it.tables.head_ptr_const() orelse {
                        assert(it.buffered_all_values());
                        return null;
                    };

                    return table.peek().?;
                }

                /// This is only safe to call after peek() has returned non-null.
                fn pop(it: *LevelIterator) Value {
                    if (it.values.pop()) |value| return value;

                    const table = it.tables.head_ptr().?;
                    const value = table.pop();

                    if (table.peek() == null) it.tables.advance_head();

                    return value;
                }
            };
        }

        fn TableIteratorType(comptime Parent: type, comptime read_done: fn (*Parent) void) type {
            _ = read_done; // TODO

            return struct {
                const TableIterator = @This();

                const ValuesRingBuffer = RingBuffer(Value, Table.data.value_count_max, .pointer);

                grid: *Grid,
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

                read: Grid.Read = undefined,
                /// This field is only used for safety checks, it does not affect the behavior.
                read_pending: bool = false,

                fn init(allocator: mem.Allocator) !TableIterator {
                    const index = try allocator.alignedAlloc(u8, config.sector_size, block_size);
                    errdefer allocator.free(index);

                    const values = try ValuesRingBuffer.init(allocator);
                    errdefer values.deinit(allocator);

                    const block_a = try allocator.alignedAlloc(u8, config.sector_size, block_size);
                    errdefer allocator.free(block_a);

                    const block_b = try allocator.alignedAlloc(u8, config.sector_size, block_size);
                    errdefer allocator.free(block_b);

                    return .{
                        .grid = undefined,
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

                fn deinit(it: *TableIterator, allocator: mem.Allocator) void {
                    assert(!it.read_pending);

                    allocator.free(it.index);
                    it.values.deinit(allocator);
                    for (it.blocks.buffer) |block| allocator.free(block);
                    it.* = undefined;
                }

                fn reset(
                    it: *TableIterator,
                    grid: *Grid,
                    parent: *Parent,
                    address: u64,
                    checksum: u128,
                ) void {
                    assert(!it.read_pending);
                    it.* = .{
                        .grid = grid,
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
                fn tick(it: *TableIterator) bool {
                    assert(!it.read_pending);
                    assert(it.address != 0);

                    if (it.read_table_index) {
                        assert(!it.read_pending);
                        it.read_pending = true;
                        it.grid.read_block(
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

                fn read_next_data_block(it: *TableIterator, block: BlockPtr) void {
                    assert(!it.read_table_index);
                    assert(it.block < Table.index_data_blocks_used(it.index));

                    const addresses = Table.index_data_addresses(it.index);
                    const checksums = Table.index_data_checksums(it.index);
                    const address = addresses[it.block];
                    const checksum = checksums[it.block];

                    assert(!it.read_pending);
                    it.read_pending = true;
                    it.grid.read_block(on_read, &it.read, block, address, checksum);
                }

                fn on_read_table_index(read: *Grid.Read) void {
                    const it = @fieldParentPtr(TableIterator, "read", read);
                    assert(it.read_pending);
                    it.read_pending = false;

                    assert(it.read_table_index);
                    it.read_table_index = false;

                    const read_pending = it.tick();
                    // After reading the table index, we always read at least one data block.
                    assert(read_pending);
                }

                fn on_read(read: *Grid.Read) void {
                    const it = @fieldParentPtr(TableIterator, "read", read);
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
                fn buffered_all_values(it: TableIterator) bool {
                    assert(!it.read_pending);

                    const data_blocks_used = Table.index_data_blocks_used(it.index);
                    assert(it.block <= data_blocks_used);
                    return it.block == data_blocks_used;
                }

                fn buffered_value_count(it: TableIterator) u32 {
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

                fn buffered_enough_values(it: TableIterator) bool {
                    assert(!it.read_pending);

                    return it.buffered_all_values() or
                        it.buffered_value_count() >= Table.data.value_count_max;
                }

                fn peek(it: TableIterator) ?Key {
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
                fn pop(it: *TableIterator) Value {
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

        pub const ValueCache = std.HashMapUnmanaged(Value, void, HashMapContextValue, 70);

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
        compactions: [config.lsm_levels - 1]TableCompaction, 

        const TableCompaction = CompactionType(TableIteratorType);
        const ImmutableTableCompaction = CompactionType(ImmutableTable.IteratorType);

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

            for (tree.compactions) |*compaction| {
                complaction.* = Compaction.init(allocator, &tree.manifest, tree.grid);
            }
        }

        pub fn deinit(tree: *Tree, allocator: mem.Allocator) void {
            for (tree.complactions) |*compaction| {
                complaction.deinit(allocator);
            }

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

                const values_max = tree.immutable_table.values_max();
                const values = tree.mutable_table.sort_into_values_and_clear(values_max);
                assert(values.ptr == values_max.ptr);

                tree.immutable_table.create_from_sorted_values(
                    tree.manifest.take_snapshot(),
                    values,
                );

                assert(tree.mutable_table.count() == 0);
                assert(!tree.immutable_table.free);
            }

            // if compactions are started, tick them or start some
            // scatter-gather compaction callbacks -> callback

            // if (!tree.table.free) {
            //     tree.table.flush.tick(callback);
            // } else {
            //     callback(tree);
            // }

            // TODO Call tree.manifest.flush() in parallel.
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
