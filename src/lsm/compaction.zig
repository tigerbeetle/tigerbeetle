const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const config = @import("../config.zig");

const GridType = @import("grid.zig").GridType;
const ManifestType = @import("manifest.zig").ManifestType;
const KWayMergeIterator = @import("k_way_merge.zig").KWayMergeIterator;
const LevelIteratorType = @import("level_iterator.zig").LevelIteratorType;

pub fn CompactionType(
    comptime Table: type,
    comptime IteratorAType: anytype, // fn (Table: type) type
) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const BlockPtr = Table.BlockPtr;
    const tombstone = Table.tombstone;
    const compare_keys = Table.compare_keys;

    return struct {
        const Compaction = @This();

        const Grid = GridType(Table.Storage);
        const Manifest = ManifestType(Table);
        const TableInfo = Manifest.TableInfo;

        const IteratorA = IteratorAType(Table);
        const IteratorB = LevelIteratorType(Table);

        pub const Callback = fn (it: *Compaction) void;

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

        const Status = enum {
            idle,
            compacting,
            done,
        };

        const BlockWrite = struct {
            block: BlockPtr,
            write: Grid.Write = undefined,
            ready: bool = false,
        };

        const TableInfoBuffer = struct {
            array: []TableInfo,
            count: usize = 0,

            /// The average number of tables involved in a compaction is the 1 table from level A,
            /// plus the growth_factor number of tables from level B, plus 1 on either side,
            /// since the overlap may not be perfectly aligned to table boundaries.
            /// However, the worst case number of tables may approach all tables in level B,
            /// since key ranges may be skewed and not evenly distributed across a level.
            const count_max = 1 + config.lsm_growth_factor + 2;

            fn init(allocator: mem.Allocator) !TableInfoBuffer {
                const array = try allocator.alloc(TableInfo, count_max);
                errdefer allocator.free(array);

                return TableInfoBuffer{ .array = array };
            }

            fn deinit(buffer: *TableInfoBuffer, allocator: mem.Allocator) void {
                allocator.free(buffer.array);
            }

            fn full(buffer: *const TableInfoBuffer) bool {
                assert(buffer.count <= count_max);
                assert(buffer.count <= buffer.array.len);
                return buffer.count == buffer.array.len;
            }

            /// Asserts that tables are pushed in sort order.
            fn push(buffer: *TableInfoBuffer, table: *const TableInfo) void {
                assert(!buffer.full());

                if (buffer.count > 0) {
                    const tail = &buffer.array[buffer.count - 1];
                    assert(compare_keys(tail.key_max, table.key_min) == .lt);
                }

                buffer.array[buffer.count] = table.*;
                buffer.counter += 1;
            }

            fn drain(buffer: *TableInfoBuffer) []TableInfo {
                assert(buffer.count <= count_max);
                assert(buffer.count <= buffer.array.len);

                defer buffer.count = 0;
                return buffer.array.ptr[0..buffer.count];
            }
        };

        status: Status,

        grid: *Grid,
        manifest: *Manifest,
        level_b: u8,
        snapshot: u64,
        drop_tombstones: bool,

        callback: ?Callback = null,
        ticks: u32 = 0,
        io_pending: u32 = 0,

        iterator_a: IteratorA,
        iterator_b: IteratorB,

        /// Private:
        /// The caller must use the Callback's `done` argument to know when compaction is done,
        /// because a write I/O may yet follow even after the merge is done.
        merge_done: bool = false,
        merge_iterator: MergeIterator,
        table_builder: Table.Builder,

        index: BlockWrite,
        filter: BlockWrite,
        data: BlockWrite,

        remove_level_a: ?*const TableInfo = null,
        remove_level_b: TableInfoBuffer,
        insert_level_b: TableInfoBuffer,

        pub fn init(allocator: mem.Allocator) !Compaction {
            var iterator_a = try IteratorA.init(allocator);
            errdefer iterator_a.deinit(allocator);

            var iterator_b = try IteratorB.init(allocator);
            errdefer iterator_b.deinit(allocator);

            var table_builder = try Table.Builder.init(allocator);
            errdefer table_builder.deinit(allocator);

            const index = BlockWrite{ .block = try allocate_block(allocator) };
            errdefer allocator.free(index.block);

            const filter = BlockWrite{ .block = try allocate_block(allocator) };
            errdefer allocator.free(filter.block);

            const data = BlockWrite{ .block = try allocate_block(allocator) };
            errdefer allocator.free(data.block);

            var remove_level_b = try TableInfoBuffer.init(allocator);
            errdefer remove_level_b.deinit(allocator);

            var insert_level_b = try TableInfoBuffer.init(allocator);
            errdefer insert_level_b.deinit(allocator);

            return Compaction{
                // Provided on start()
                .level_b = undefined,
                .status = .idle,
                .manifest = undefined,
                .grid = undefined,

                .iterator_a = iterator_a,
                .iterator_b = iterator_b,
                .merge_iterator = undefined, // This must be initialized at tick 1.
                .table_builder = table_builder,

                .index = index,
                .filter = filter,
                .data = data,

                .remove_level_b = remove_level_b,
                .insert_level_b = insert_level_b,
            };
        }

        fn allocate_block(allocator: mem.Allocator) !BlockPtr {
            const block = try allocator.alignedAlloc(u8, config.sector_size, config.block_size);
            return block[0..config.block_size];
        }

        pub fn deinit(compaction: *Compaction, allocator: mem.Allocator) void {
            compaction.iterator_a.deinit(allocator);
            compaction.iterator_b.deinit(allocator);
            compaction.table_builder.deinit(allocator);
            compaction.remove_level_b.deinit(allocator);
            compaction.insert_level_b.deinit(allocator);

            allocator.free(compaction.index.block);
            allocator.free(compaction.filter.block);
            allocator.free(compaction.data.block);
        }

        pub fn start(
            compaction: *Compaction,
            grid: *Grid,
            manifest: *Manifest,
            level_b: u8,
            snapshot: u64,
            drop_tombstones: bool,
            iterator_a_context: IteratorA.Context,
            iterator_b_context: IteratorB.Context,
        ) void {
            assert(compaction.status == .idle);
            assert(compaction.callback == null);
            assert(compaction.io_pending == 0);
            assert(level_b < config.lsm_levels);
            assert(level_b < config.lsm_levels - 1 or drop_tombstones);

            compaction.* = .{
                .status = .compacting,

                .grid = grid,
                .manifest = manifest,
                .level_b = level_b,
                .snapshot = snapshot,
                .drop_tombstones = drop_tombstones,

                .iterator_a = compaction.iterator_a,
                .iterator_b = compaction.iterator_b,

                .merge_iterator = undefined,
                .table_builder = compaction.table_builder,

                .index = compaction.index,
                .filter = compaction.filter,
                .data = compaction.data,

                .remove_level_b = compaction.remove_level_b,
                .insert_level_b = compaction.insert_level_b,
            };

            // TODO Reset iterators and builder.
            compaction.iterator_a.start(iterator_a_context, iterator_a_read_done);
            compaction.iterator_b.start(iterator_b_context, iterator_b_read_done);

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
        /// 2. IO.tick() to submit these I/O operations to the kernel,
        /// 3. tick_cpu() across all trees.
        pub fn tick_io(compaction: *Compaction, callback: Callback) void {
            assert(compaction.status == .compacting);
            assert(compaction.callback == null);
            assert(compaction.io_pending == 0);
            assert(!compaction.merge_done);

            compaction.callback = callback;

            if (compaction.ticks >= 0) compaction.tick_io_read();
            if (compaction.ticks >= 2) compaction.tick_io_write();

            // All values may be eclipsed by tombstones, with no write I/O pending here.
        }

        pub fn tick_cpu(compaction: *Compaction) void {
            assert(compaction.status == .compacting);
            assert(compaction.callback != null);
            assert(compaction.io_pending >= 0);
            assert(!compaction.merge_done);

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
            assert(compaction.status == .compacting);
            assert(compaction.callback != null);
            assert(compaction.io_pending == 0);

            if (compaction.merge_done) compaction.status = .done;

            const callback = compaction.callback.?;
            compaction.callback = null;
            callback(compaction);
        }

        pub fn reset(compaction: *Compaction) void {
            assert(compaction.status == .done);
            assert(compaction.callback == null);
            assert(compaction.io_pending == 0);

            compaction.status = .idle;
        }

        fn tick_io_read(compaction: *Compaction) void {
            assert(compaction.callback != null);

            if (compaction.iterator_a.tick()) compaction.io_pending += 1;
            if (compaction.iterator_b.tick()) compaction.io_pending += 1;

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
                if (compaction.drop_tombstones and tombstone(&value)) {
                    tombstones_dropped += 1;
                } else {
                    compaction.table_builder.data_block_append(&value);
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
                    const snapshot_min = compaction.snapshot;
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

        fn iterator_a_read_done(iterator_a: *IteratorA) void {
            const compaction = @fieldParentPtr(Compaction, "iterator_a", iterator_a);
            compaction.io_callback();
        }

        fn iterator_b_read_done(iterator_b: *IteratorB) void {
            const compaction = @fieldParentPtr(Compaction, "iterator_b", iterator_b);
            compaction.io_callback();
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
            assert(compaction.iterator_a.buffered_all_values());
            assert(compaction.iterator_a.peek() == null);

            assert(compaction.iterator_b.buffered_all_values());
            assert(compaction.iterator_b.peek() == null);
        }

        fn stream_peek(compaction: *Compaction, stream_id: u32) ?Key {
            assert(stream_id <= 1);

            if (stream_id == 0) {
                return compaction.iterator_a.peek();
            } else {
                return compaction.iterator_b.peek();
            }
        }

        fn stream_pop(compaction: *Compaction, stream_id: u32) Value {
            assert(stream_id <= 1);

            if (stream_id == 0) {
                return compaction.iterator_a.pop();
            } else {
                return compaction.iterator_b.pop();
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
