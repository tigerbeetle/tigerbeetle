const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const log = std.log.scoped(.compaction);
const config = @import("../config.zig");

const GridType = @import("grid.zig").GridType;
const ManifestType = @import("manifest.zig").ManifestType;
const KWayMergeIterator = @import("k_way_merge.zig").KWayMergeIterator;
const TableIteratorType = @import("table_iterator.zig").TableIteratorType;
const LevelIteratorType = @import("level_iterator.zig").LevelIteratorType;

pub fn CompactionType(
    comptime Table: type,
    comptime Storage: type,
    comptime IteratorAType: anytype,
) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const tombstone = Table.tombstone;
    const compare_keys = Table.compare_keys;

    return struct {
        const Compaction = @This();

        const Grid = GridType(Storage);
        const BlockPtr = Grid.BlockPtr;
        const BlockPtrConst = Grid.BlockPtrConst;
        const BlockWrite = struct {
            write: Grid.Write = undefined,
            block: BlockPtr = undefined,
            ready: bool = false,
        };

        const Manifest = ManifestType(Table, Storage);
        const TableInfo = Manifest.TableInfo;
        const TableInfoBuffer = TableInfo.BufferType(.ascending);

        const IteratorA = IteratorAType(Table, Storage);
        const IteratorB = LevelIteratorType(Table, Storage);

        const k = 2;
        const MergeIterator = KWayMergeIterator(
            Compaction,
            Table.Key,
            Table.Value,
            Table.key_from_value,
            Table.compare_keys,
            k,
            MergeStreamSelector.peek,
            MergeStreamSelector.pop,
            MergeStreamSelector.precedence,
        );

        const MergeStreamSelector = struct {
            fn peek(compaction: *Compaction, stream_id: u32) ?Key {
                return switch (stream_id) {
                    0 => compaction.iterator_a.peek(),
                    1 => compaction.iterator_b.peek(),
                    else => unreachable,
                };
            }

            fn pop(compaction: *Compaction, stream_id: u32) Value {
                return switch (stream_id) {
                    0 => compaction.iterator_a.pop(),
                    1 => compaction.iterator_b.pop(),
                    else => unreachable,
                };
            }

            /// Returns true if stream A has higher precedence than stream B.
            /// This is used to deduplicate values across streams.
            ///
            /// This assumes that all overlapping tables in level A at the time the compaction was
            /// started are included in the compaction. If this is not the case, the older table
            /// in a pair of overlapping tables could be left in level A and shadow the newer table
            /// in level B, resulting in data loss/invalid data.
            fn precedence(compaction: *Compaction, stream_a: u32, stream_b: u32) bool {
                _ = compaction;
                assert(stream_a + stream_b == 1);

                // All tables in iterator_a (stream=0) have a higher precedence.
                return stream_a == 0;
            }
        };

        pub const Callback = fn (it: *Compaction) void;

        const Status = enum {
            idle,
            processing,
            done,
        };

        grid: *Grid,
        range: Manifest.CompactionRange,
        snapshot: u64,
        drop_tombstones: bool,

        status: Status,
        callback: ?Callback = null,
        io_pending: u32 = 0,

        iterator_a: IteratorA,
        iterator_b: IteratorB,

        merge_status: Status,
        merge_iterator: MergeIterator,

        table_builder: Table.Builder,
        index: BlockWrite,
        filter: BlockWrite,
        data: BlockWrite,

        manifest: *Manifest,
        level_b: u8,
        remove_level_a: ?TableInfo,
        update_level_b: TableInfoBuffer,
        insert_level_b: TableInfoBuffer,

        pub fn init(allocator: mem.Allocator) !Compaction {
            var iterator_a = try IteratorA.init(allocator);
            errdefer iterator_a.deinit(allocator);

            var iterator_b = try IteratorB.init(allocator);
            errdefer iterator_b.deinit(allocator);

            var table_builder = try Table.Builder.init(allocator);
            errdefer table_builder.deinit(allocator);

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
                // Assigned by start()
                .grid = undefined,
                .range = undefined,
                .snapshot = undefined,
                .drop_tombstones = undefined,

                .status = .idle,
                .iterator_a = iterator_a,
                .iterator_b = iterator_b,

                .merge_status = .idle,
                .merge_iterator = undefined, // Assigned by start()

                .table_builder = table_builder,
                .index = .{},
                .filter = .{},
                .data = .{},

                // Assigned by start()
                .manifest = undefined,
                .level_b = undefined,
                .remove_level_a = null,
                .update_level_b = update_level_b,
                .insert_level_b = insert_level_b,
            };
        }

        pub fn deinit(compaction: *Compaction, allocator: mem.Allocator) void {
            compaction.insert_level_b.deinit(allocator);
            compaction.update_level_b.deinit(allocator);

            compaction.table_builder.deinit(allocator);

            compaction.iterator_b.deinit(allocator);
            compaction.iterator_a.deinit(allocator);
        }

        pub fn start(
            compaction: *Compaction,
            grid: *Grid,
            table: ?*const TableInfo,
            range: Manifest.CompactionRange,
            snapshot: u64,
            manifest: *Manifest,
            level_b: u8,
            iterator_a_context: IteratorA.Context,
        ) void {
            assert(compaction.status == .idle);
            assert(compaction.callback == null);
            assert(compaction.io_pending == 0);

            assert(level_b < config.lsm_levels);
            assert(range.table_count > 0);

            // Any level may choose to drop tombstones, but the last level must do so 
            // as it would have nowhere else to move/compact the tables to.
            const drop_tombstones = manifest.compaction_must_drop_tombstones(level_b, range);
            assert(drop_tombstones or level_b < config.lsm_levels - 1);

            compaction.* = .{
                .grid = grid,
                .range = range,
                .snapshot = snapshot,
                .drop_tombstones = drop_tombstones,

                .status = .processing,
                .iterator_a = compaction.iterator_a,
                .iterator_b = compaction.iterator_b,

                .merge_status = .idle,
                .merge_iterator = undefined,

                .table_builder = compaction.table_builder,
                .index = compaction.index,
                .filter = compaction.filter,
                .data = compaction.data,

                .manifest = manifest,
                .level_b = level_b,
                .remove_level_a = if (table) |level_a_table| level_a_table.* else null,
                .update_level_b = compaction.update_level_b,
                .insert_level_b = compaction.insert_level_b,
            };

            // TODO: reset TableBuilder if needed
            assert(!compaction.index.ready);
            assert(!compaction.filter.ready);
            assert(!compaction.data.ready);

            const iterator_b_context = .{
                .grid = grid,
                .manifest = manifest,
                .level = level_b,
                .snapshot = snapshot,
                .key_min = range.key_min,
                .key_max = range.key_max,
                .direction = .ascending,
                .table_info_callback = iterator_b_table_info_callback,
            };

            compaction.iterator_a.start(iterator_a_context, iterator_a_io_callback);
            compaction.iterator_b.start(iterator_b_context, iterator_b_io_callback);
        }

        fn iterator_a_io_callback(iterator_a: *IteratorA) void {
            const compaction = @fieldParentPtr(Compaction, "iterator_a", iterator_a);
            compaction.io_finish();
        }

        fn iterator_b_io_callback(iterator_b: *IteratorB) void {
            const compaction = @fieldParentPtr(Compaction, "iterator_b", iterator_b);
            compaction.io_finish();
        }

        fn iterator_b_table_info_callback(
            iterator_b: *IteratorB,
            table: *const TableInfo,
            index_block: BlockPtrConst,
        ) void {
            const compaction = @fieldParentPtr(Compaction, "iterator_b", iterator_b);
            assert(compaction.status == .processing);
            assert(compaction.callback != null);
            assert(compaction.merge_status != .done);

            // Tables discovered by iterator_b that are visible
            compaction.queue_manifest_update(&compaction.update_level_b, table);

            // Release the table's block addresses in the Grid as it will be made invisible.
            // This is safe to do so as iterator_b makes a copy of the block before calling us.
            const grid = compaction.grid;
            for (Table.index_data_addresses_used(index_block)) |address| grid.release(address);
            for (Table.index_filter_addresses_used(index_block)) |address| grid.release(address);
            grid.release(Table.index_block_address(index_block));
        }

        /// Enqueues the table to be applied to the manifest on the given table buffer.
        /// If the buffer is `update_level_b`, the table will be provided to `update_tables`.
        /// If the buffer is `insert_level_b`, the table will be provided to `insert_tables`.
        fn queue_manifest_update(
            compaction: *Compaction,
            buffer: *TableInfoBuffer,
            table: *const TableInfo,
        ) void {
            assert(compaction.status == .processing);
            assert(compaction.callback != null);
            assert(compaction.merge_status != .done);

            assert(buffer == &compaction.update_level_b or buffer == &compaction.insert_level_b);
            if (buffer.full()) compaction.apply_manifest_updates(buffer);
            buffer.push(table);
        }
        
        /// Drains all enqueued tables from the given buffer and applies them to the manifest.
        /// If the buffer is `update_level_b`, the table will be provided to `update_tables`.
        /// If the buffer is `insert_level_b`, the table will be provided to `insert_tables`.
        fn apply_manifest_updates(compaction: *Compaction, buffer: *TableInfoBuffer) void {
            assert(compaction.status == .processing);
            assert(compaction.callback != null);
            assert(compaction.merge_status != .done);

            assert(buffer == &compaction.update_level_b or buffer == &compaction.insert_level_b);

            const tables: []TableInfo = buffer.drain();
            if (tables.len == 0) return;

            // Double check that the tables queued are in the compaction's key range.
            for (tables) |table| {
                assert(compare_keys(compaction.range.key_min, table.key_max) != .gt);
                assert(compare_keys(compaction.range.key_max, table.key_min) != .lt);
            }

            if (buffer == &compaction.update_level_b) {
                compaction.manifest.update_tables(compaction.level_b, compaction.snapshot, tables);
            } else {
                compaction.manifest.insert_tables(compaction.level_b, tables);
            }
        }

        pub fn compact_tick(compaction: *Compaction, callback: Callback) void {
            assert(compaction.status == .processing);
            assert(compaction.callback == null);
            assert(compaction.merge_status != .done);
            assert(compaction.io_pending == 0);

            compaction.callback = callback;
            
            // Generate fake IO to make sure io_pending doesn't reach zero multiple times from
            // IO being completed inline down below.
            // The fake IO is immediately resolved and triggers the cpu_merge_start if all 
            // IO completes inline or if no IO was started.
            compaction.io_start();
            defer compaction.io_finish();

            // Start reading blocks from the iterator to merge them.
            if (compaction.iterator_a.tick()) compaction.io_start();
            if (compaction.iterator_b.tick()) compaction.io_start();
            
            // Start writing blocks prepared by the merge iterator from a previous compact_tick().
            compaction.io_write_start("data");
            compaction.io_write_start("filter");
            compaction.io_write_start("index");
        }

        fn io_write_start(compaction: *Compaction, comptime field: []const u8) void {
            const write_callback = struct {
                fn callback(write: *Grid.Write) void {
                    const block_write = @fieldParentPtr(BlockWrite, "write", write);
                    block_write.block = undefined;

                    const _compaction = @fieldParentPtr(Compaction, field, block_write);
                    _compaction.io_finish();
                }
            }.callback;

            const block_write: *BlockWrite = &@field(compaction, field);
            if (block_write.ready) {
                block_write.ready = false;

                compaction.io_start();
                compaction.grid.write_block(
                    write_callback,
                    &block_write.write,
                    block_write.block,
                    Table.block_address(block_write.block),
                );
            }
        }

        fn io_start(compaction: *Compaction) void {
            assert(compaction.status == .processing);
            assert(compaction.callback != null);
            assert(compaction.merge_status != .done);

            compaction.io_pending += 1;
        }

        fn io_finish(compaction: *Compaction) void {
            assert(compaction.status == .processing);
            assert(compaction.callback != null);
            assert(compaction.merge_status != .done);
            assert(compaction.io_pending > 0);

            compaction.io_pending -= 1;
            if (compaction.io_pending == 0) compaction.cpu_merge_start();
        }

        fn cpu_merge_start(compaction: *Compaction) void {
            assert(compaction.status == .processing);
            assert(compaction.callback != null);
            assert(compaction.merge_status != .done);
            assert(compaction.io_pending == 0);

            // Create the merge iterator only when we can peek() from the read iterators.
            // This happens after IO for the first reads complete.
            if (compaction.merge_status == .idle) {
                compaction.merge_iterator = MergeIterator.init(compaction, k, .ascending);
                compaction.merge_status = .processing;
            }

            assert(compaction.merge_status == .processing);
            assert(!compaction.data.ready);
            assert(!compaction.filter.ready);
            assert(!compaction.index.ready);

            if (!compaction.merge_iterator.empty()) {
                compaction.cpu_merge();
            } else {
                compaction.cpu_merge_finish();
            }

            // TODO Implement pacing here by deciding if we should do another compact_tick()
            // instead of invoking the callback, using compaction.range.table_count as the heuristic.     

            const callback = compaction.callback.?;
            compaction.callback = null;
            callback(compaction);
        }

        fn cpu_merge(compaction: *Compaction) void {
            // Ensure this is the result of a compact_tick() call that finished processing IO.
            assert(compaction.status == .processing);
            assert(compaction.callback != null);
            assert(compaction.io_pending == 0);

            // Ensure there are values to merge and that is it safe to do so.
            assert(compaction.merge_status == .processing);
            assert(!compaction.merge_iterator.empty());
            assert(!compaction.data.ready);
            assert(!compaction.filter.ready);
            assert(!compaction.index.ready);

            // Build up the data/filter/index blocks with values merged from the read iterators.
            var tombstones_dropped: u32 = 0;
            while (!compaction.table_builder.data_block_full()) {
                const value = compaction.merge_iterator.pop() orelse break;
                if (compaction.drop_tombstones and tombstone(&value)) {
                    tombstones_dropped += 1;
                } else {
                    compaction.table_builder.data_block_append(&value);
                }
            }

            // Finalize the data block and prepare it to be written out.
            blk: {
                // An empty data block means all the values read were tombstones that were dropped.
                if (compaction.table_builder.data_block_empty()) {
                    assert(compaction.drop_tombstones);
                    assert(tombstones_dropped > 0);
                    break :blk;
                }

                compaction.table_builder.data_block_finish(.{
                    .cluster = compaction.grid.superblock.working.cluster,
                    .address = compaction.grid.acquire(),
                });

                // Mark the finished data block as ready to write for the next compact_tick() call.
                compaction.data.block = compaction.table_builder.data_block;
                assert(!compaction.data.ready);
                compaction.data.ready = true;

                // The merge iterator having pending values should mean the data block is full.
                if (!compaction.merge_iterator.empty()) {
                    const values_used = Table.data_block_values_used(compaction.data.block).len;
                    assert(values_used == Table.data.value_count_max);
                }
            }

            // Finalize the filter block if it (or the index block) are full or if there's no more values.
            if (compaction.table_builder.filter_block_full() or
                compaction.table_builder.index_block_full() or
                compaction.merge_iterator.empty())
            blk: {
                // An empty filter block means all the values read were tombstones that were dropped.
                if (compaction.table_builder.filter_block_empty()) {
                    assert(compaction.drop_tombstones);
                    assert(tombstones_dropped > 0);
                    break :blk;
                }

                compaction.table_builder.filter_block_finish(.{
                    .cluster = compaction.grid.superblock.working.cluster,
                    .address = compaction.grid.acquire(),
                });

                // Mark the finished filter block as ready to write for the next compact_tick() call.
                compaction.filter.block = compaction.table_builder.filter_block;
                assert(!compaction.filter.ready);
                compaction.filter.ready = true;
            }

            // Finalize the index block if it's full or if there's no more values.
            if (compaction.table_builder.index_block_full() or
                compaction.merge_iterator.empty())
            blk: {
                // An empty index block means all the values read were tombstones that were dropped.
                if (compaction.table_builder.index_block_empty()) {
                    assert(compaction.drop_tombstones);
                    assert(tombstones_dropped > 0);
                    break :blk;
                }

                // Finish the merged table and queue it for insertion.
                const table = compaction.table_builder.index_block_finish(.{
                    .cluster = compaction.grid.superblock.working.cluster,
                    .address = compaction.grid.acquire(),
                    .snapshot_min = compaction.snapshot,
                });
                compaction.queue_manifest_update(&compaction.insert_level_b, &table);

                // Mark the finished index block as ready to write for the next compact_tick() call.
                compaction.index.block = compaction.table_builder.index_block;
                assert(!compaction.index.ready);
                compaction.index.ready = true;
            }
        }

        fn cpu_merge_finish(compaction: *Compaction) void {
            // Ensure this is the result of a compact_tick() call that finished processing IO.
            assert(compaction.status == .processing);
            assert(compaction.callback != null);
            assert(compaction.io_pending == 0);

            // Ensure merging is truly finished.
            assert(compaction.merge_status == .processing);
            assert(compaction.merge_iterator.empty());
            assert(!compaction.data.ready);
            assert(!compaction.filter.ready);
            assert(!compaction.index.ready);

            // Double check the iterators are finished as well.
            assert(compaction.iterator_a.buffered_all_values());
            assert(compaction.iterator_a.peek() == null);
            assert(compaction.iterator_b.buffered_all_values());
            assert(compaction.iterator_b.peek() == null);

            // Apply any pending manifest changes queued up during the merge.
            compaction.apply_manifest_updates(&compaction.update_level_b);
            compaction.apply_manifest_updates(&compaction.insert_level_b);

            // Remove the level_a table if it was provided given it's now been merged into level_b.
            // TODO: Release the grid blocks associated with level_a as well
            if (compaction.level_b != 0) {
                const level_a = compaction.level_b - 1;

                if (compaction.remove_level_a) |*level_a_table| {
                    const tables = @as(*[1]TableInfo, level_a_table);
                    compaction.manifest.update_tables(level_a, compaction.snapshot, tables);
                }
            }

            // Finally, mark Compaction as officially complete and ready to be reset().
            compaction.merge_status = .done;
            compaction.status = .done;
        }

        pub fn reset(compaction: *Compaction) void {
            assert(compaction.status == .done);
            assert(compaction.callback == null);
            assert(compaction.io_pending == 0);
            assert(compaction.merge_status == .done);

            // Double check that manifest updates have been applied.
            assert(compaction.update_level_b.drain().len == 0);
            assert(compaction.insert_level_b.drain().len == 0);

            compaction.status = .idle;
            compaction.merge_status = .idle;
        }
    };
}