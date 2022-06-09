const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const config = @import("../config.zig");

const KWayMergeIterator = @import("k_way_merge.zig").KWayMergeIterator;

pub fn CompactionType(
    comptime Table: type,
    comptime LevelAIterator: type,
) type {
    const Key = Table.Key;
    const Value = Table.Value;

    return struct {
        const Compaction = @This();
        
        const Grid = @import("grid.zig").GridType(Table.Storage);
        const Manifest = @import("manifest.zig").ManifestType(Table);
        const LevelBIterator = @import("level_iterator.zig").LevelIterator;

        pub const Callback = fn (it: *Compaction, done: bool) void;

        const k = 2;
        const MergeIterator = KWayMergeIterator(
            Compaction,
            Key,
            Value,
            Table.key_from_value,
            Table.compare_keys,
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