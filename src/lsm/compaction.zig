const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const config = @import("../config.zig");

const GridType = @import("grid.zig").GridType;
const ManifestType = @import("manifest.zig").ManifestType;
const KWayMergeIterator = @import("k_way_merge.zig").KWayMergeIterator;
const TableIteratorType = @import("table_iterator.zig").TableIteratorType;
const LevelIteratorType = @import("level_iterator.zig").LevelIteratorType;

pub fn CompactionType(
    comptime Table: type,
    comptime Storage: type,
    comptime IteratorAType: anytype, // fn (Table: type, Storage: type) type
) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const tombstone = Table.tombstone;
    const compare_keys = Table.compare_keys;

    return struct {
        const Compaction = @This();

        const Grid = GridType(Storage);
        const BlockPtr = Grid.BlockPtr;
        const Manifest = ManifestType(Table, Storage);
        const TableInfo = Manifest.TableInfo;

        const IteratorA = IteratorAType(Table, Storage);
        const IteratorB = LevelIteratorType(Table, Storage);

        pub const Callback = *const fn (it: *Compaction) void;

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

        const BlockPtrConst = *align(config.sector_size) const [config.block_size]u8;
        const BlockWrite = struct {
            block: BlockPtr,
            write: Grid.Write = undefined,
            ready: bool = false,
        };

        const TableInfoBuffer = @import("manifest.zig").TableInfoBufferType(Table, .ascending);

        status: Status,

        grid: *Grid,
        manifest: *Manifest,
        level_b: u8,
        range: Manifest.CompactionRange,
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
        update_level_b: TableInfoBuffer,
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

            // The average number of tables involved in a compaction is the 1 table from level A,
            // plus the growth_factor number of tables from level B, plus 1 on either side,
            // since the overlap may not be perfectly aligned to table boundaries.
            // However, the worst case number of tables may approach all tables in level B,
            // since key ranges may be skewed and not evenly distributed across a level.
            const table_buffer_count_max = 1 + config.lsm_growth_factor + 2;

            var update_level_b = try TableInfoBuffer.init(allocator, table_buffer_count_max);
            errdefer update_level_b.deinit(allocator);

            var insert_level_b = try TableInfoBuffer.init(allocator, table_buffer_count_max);
            errdefer insert_level_b.deinit(allocator);

            return Compaction{
                .status = .idle,

                // Assigned by start():
                .grid = undefined,
                .manifest = undefined,
                .level_b = undefined,
                .range = undefined,
                .snapshot = undefined,
                .drop_tombstones = undefined,

                .iterator_a = iterator_a,
                .iterator_b = iterator_b,

                .merge_iterator = undefined, // This can only be initialized on tick 1.
                .table_builder = table_builder,

                .index = index,
                .filter = filter,
                .data = data,

                .update_level_b = update_level_b,
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
            compaction.update_level_b.deinit(allocator);
            compaction.insert_level_b.deinit(allocator);

            allocator.free(compaction.index.block);
            allocator.free(compaction.filter.block);
            allocator.free(compaction.data.block);
        }

        pub fn start(
            compaction: *Compaction,
            grid: *Grid,
            manifest: *Manifest,
            // TODO level_a_table: ?TableInfo,
            level_b: u8,
            range: Manifest.CompactionRange,
            snapshot: u64,
            iterator_a_context: IteratorA.Context,
        ) void {
            assert(compaction.status == .idle);
            assert(compaction.callback == null);
            assert(compaction.io_pending == 0);
            assert(level_b < config.lsm_levels);
            assert(range.table_count > 0);

            const drop_tombstones = manifest.compaction_must_drop_tombstones(level_b, range);
            assert(drop_tombstones or level_b < config.lsm_levels - 1);

            compaction.* = .{
                .status = .compacting,

                .grid = grid,
                .manifest = manifest,
                .level_b = level_b,
                .range = range,
                .snapshot = snapshot,
                .drop_tombstones = drop_tombstones,

                .iterator_a = compaction.iterator_a,
                .iterator_b = compaction.iterator_b,

                .merge_iterator = undefined,
                .table_builder = compaction.table_builder,

                .index = compaction.index,
                .filter = compaction.filter,
                .data = compaction.data,

                .update_level_b = compaction.update_level_b,
                .insert_level_b = compaction.insert_level_b,
            };

            assert(!compaction.data.ready);
            assert(!compaction.filter.ready);
            assert(!compaction.index.ready);

            // TODO Reset builder.

            // TODO: Enable when move_table() can fetch TableInfo from address/checksum.
            //
            // Perform a "compaction move" to the next level inline if certain factors allow:
            // - Can only do the specialization if there's a single table to compact.
            // - Must be compacting from a table iterator which has an address and checksum.
            // - Cannot drop tombstones as then we have to go through the normal compaction path.
            // - Cannot be performing the immutable table -> level 0 compaction
            //   as it requires the table being moved to reside on disk (tracked by manifest).
            if (false and IteratorA.Context == TableIteratorType(Table, Storage)) {
                if (!drop_tombstones and range.table_count == 1) {
                    assert(compaction.level_b != 0);
                    assert(compaction.status == .compacting);

                    const level_a = level_b - 1;
                    assert(level_a < config.lsm_levels - 1);

                    compaction.manifest.move_table(
                        level_a,
                        level_b,
                        snapshot,
                        iterator_a_context.address,
                        iterator_a_context.checksum,
                    );

                    compaction.status = .done;
                    return;
                }
            }

            const iterator_b_context = .{
                .grid = grid,
                .manifest = manifest,
                .level = level_b,
                .snapshot = snapshot,
                .key_min = range.key_min,
                .key_max = range.key_max,
                .direction = .ascending,
                .table_info_callback = iterator_b_table_info_callback, // TODO
            };

            compaction.iterator_a.start(iterator_a_context, iterator_a_callback);
            compaction.iterator_b.start(iterator_b_context, iterator_b_callback);
        }

        fn iterator_b_table_info_callback(
            iterator_b: *IteratorB,
            table: *const TableInfo,
            index_block: BlockPtrConst,
        ) void {
            const compaction = @fieldParentPtr(Compaction, "iterator_b", iterator_b);
            compaction.queue_manifest_update(&compaction.update_level_b, table);

            // Release the table's block addresses if it's invisible to the compaction.
            if (table.invisible(&.{compaction.snapshot})) {
                compaction.grid.release(Table.index_block_address(index_block));

                for (Table.index_filter_addresses_used(index_block)) |address| {
                    compaction.grid.release(address);
                }

                for (Table.index_data_addresses_used(index_block)) |address| {
                    compaction.grid.release(address);
                }
            }
        }

        fn queue_manifest_update(
            compaction: *Compaction,
            buffer: *TableInfoBuffer,
            table: *const TableInfo,
        ) void {
            assert(buffer == &compaction.update_level_b or buffer == &compaction.insert_level_b);
            if (buffer.full()) compaction.update_manifest(buffer);
            buffer.push(table);
        }

        fn update_manifest(compaction: *Compaction, buffer: *TableInfoBuffer) void {
            assert(buffer == &compaction.update_level_b or buffer == &compaction.insert_level_b);

            const tables: []const TableInfo = buffer.drain();
            if (tables.len == 0) return;

            for (tables) |table| {
                assert(compare_keys(table.key_min, compaction.range.key_min) != .lt);
                assert(compare_keys(table.key_max, compaction.range.key_max) != .gt);
            }

            if (buffer == &compaction.update_level_b) {
                compaction.manifest.update_tables(compaction.level_b, compaction.snapshot, tables);
            } else {
                compaction.manifest.insert_tables(compaction.level_b, tables);
            }
        }

        /// Compaction.ticks stages at which each action may be performed.
        const pipeline_tick_read = 0;
        const pipeline_tick_merge = 1;
        const pipeline_tick_write = 2;

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

            if (compaction.ticks >= pipeline_tick_read) compaction.tick_io_read();
            if (compaction.ticks >= pipeline_tick_write) compaction.tick_io_write();

            // All values may be eclipsed by tombstones, with no write I/O pending here.
        }

        pub fn tick_cpu(compaction: *Compaction) void {
            assert(compaction.status == .compacting);
            assert(compaction.callback != null);
            assert(compaction.io_pending >= 0);
            assert(!compaction.merge_done);

            if (compaction.ticks == pipeline_tick_merge) {
                // We cannot initialize the merge until we can peek() a value from each stream,
                // which depends on tick 0 (to read blocks) having happened.
                compaction.merge_iterator = MergeIterator.init(compaction, k, .ascending);
            }

            if (compaction.ticks >= pipeline_tick_merge) {
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

            // Consume the callback and invoke it one finished updating state below.
            const callback = compaction.callback.?;
            compaction.callback = null;
            defer callback(compaction);

            // Once merge completes, the compaction is now officially over.
            if (compaction.merge_done) {
                compaction.status = .done;

                // Flush updates to the table infos discovered during compaction
                // TODO Handle compaction.remove_level_a
                compaction.update_manifest(&compaction.update_level_b);
                compaction.update_manifest(&compaction.insert_level_b);
            }
        }

        pub fn reset(compaction: *Compaction) void {
            assert(compaction.callback == null);
            assert(compaction.io_pending == 0);

            assert(compaction.status == .done);
            compaction.status = .idle;

            assert(compaction.update_level_b.drain().len == 0);
            assert(compaction.insert_level_b.drain().len == 0);
        }

        fn tick_io_read(compaction: *Compaction) void {
            assert(compaction.callback != null);

            if (compaction.iterator_a.tick()) compaction.io_pending += 1;
            if (compaction.iterator_b.tick()) compaction.io_pending += 1;

            if (compaction.merge_done) assert(compaction.io_pending == 0);
        }

        fn tick_io_write(compaction: *Compaction) void {
            assert(compaction.callback != null);
            assert(compaction.ticks >= pipeline_tick_write);
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
            assert(compaction.ticks >= pipeline_tick_merge);
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
                compaction.table_builder.data_block_finish(.{
                    .cluster = compaction.grid.superblock.working.cluster,
                    .address = compaction.grid.acquire(),
                });
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
                    compaction.table_builder.filter_block_finish(.{
                        .cluster = compaction.grid.superblock.working.cluster,
                        .address = compaction.grid.acquire(),
                    });
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
                    const table = compaction.table_builder.index_block_finish(.{
                        .cluster = compaction.grid.superblock.working.cluster,
                        .address = compaction.grid.acquire(),
                        .snapshot_min = snapshot_min,
                    });
                    compaction.queue_manifest_update(&compaction.insert_level_b, &table);

                    swap_buffers(&compaction.index, &compaction.table_builder.index_block);
                    assert(compaction.index.ready);
                }
            }
        }

        fn iterator_a_callback(iterator_a: *IteratorA) void {
            const compaction = @fieldParentPtr(Compaction, "iterator_a", iterator_a);
            compaction.io_callback();
        }

        fn iterator_b_callback(iterator_b: *IteratorB) void {
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
            callback: *const fn (*Grid.Write) void,
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

        fn write_block_callback(comptime field: []const u8) *const fn (*Grid.Write) void {
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
