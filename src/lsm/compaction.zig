//! Compaction moves or merges a table's values into the next level.
//!
//!
//! Compaction overview:
//!
//! 1. Given:
//!
//!   - levels A and B, where A+1=B
//!   - a single table in level A ("table A")
//!   - all tables from level B which intersect table A's key range ("tables B")
//!     (This can include anything between 0 tables and all of level B's tables.)
//!
//! 2. If table A's key range is disjoint from the keys in level B, move table A into level B.
//!    All done! (But if the key ranges intersect, jump to step 3).
//!
//! 3. Create an iterator from the sort-merge of table A and the concatenation of tables B.
//!    If the same key exists in level A and B, take A's and discard B's. †
//!
//! 4. Write the sort-merge iterator into a sequence of new tables on disk.
//!
//! 5. Update the old level-B tables in the Manifest with their new `snapshot_max` so that they
//!    become invisible to subsequent read transactions.
//!
//! 6. Insert the new level-B tables into the Manifest.
//!
//!
//! † When A's value is a tombstone, there is a special case for garbage collection. When either:
//! * level B is the final level, or
//! * A's key does not exist in B or any deeper level,
//! then the tombstone is omitted from the compacted output (see: `compaction_must_drop_tombstones`).
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

    return struct {
        const Compaction = @This();

        const Grid = GridType(Storage);
        const BlockPtr = Grid.BlockPtr;
        const BlockPtrConst = Grid.BlockPtrConst;
        const BlockWrite = struct {
            write: Grid.Write = undefined,
            block: BlockPtr = undefined,
            writable: bool = false,
        };

        const Manifest = ManifestType(Table, Storage);
        const TableInfo = Manifest.TableInfo;

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
            fn peek(compaction: *const Compaction, stream_id: u32) ?Key {
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
            fn precedence(compaction: *const Compaction, stream_a: u32, stream_b: u32) bool {
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

        merge_done: bool,
        merge_iterator: ?MergeIterator,

        table_builder: Table.Builder,
        index: BlockWrite,
        filter: BlockWrite,
        data: BlockWrite,

        manifest: *Manifest,
        level_b: u8,
        remove_level_a: ?TableInfo,

        pub fn init(allocator: mem.Allocator) !Compaction {
            var iterator_a = try IteratorA.init(allocator);
            errdefer iterator_a.deinit(allocator);

            var iterator_b = try IteratorB.init(allocator);
            errdefer iterator_b.deinit(allocator);

            var table_builder = try Table.Builder.init(allocator);
            errdefer table_builder.deinit(allocator);

            return Compaction{
                // Assigned by start()
                .grid = undefined,
                .range = undefined,
                .snapshot = undefined,
                .drop_tombstones = undefined,

                .status = .idle,
                .iterator_a = iterator_a,
                .iterator_b = iterator_b,

                .merge_done = false,
                .merge_iterator = null,

                .table_builder = table_builder,
                .index = .{},
                .filter = .{},
                .data = .{},

                // Assigned by start()
                .manifest = undefined,
                .level_b = undefined,
                .remove_level_a = null,
            };
        }

        pub fn deinit(compaction: *Compaction, allocator: mem.Allocator) void {
            compaction.table_builder.deinit(allocator);

            compaction.iterator_b.deinit(allocator);
            compaction.iterator_a.deinit(allocator);
        }

        /// table_a is null when level A is 0.
        pub fn start(
            compaction: *Compaction,
            grid: *Grid,
            manifest: *Manifest,
            snapshot: u64,
            range: Manifest.CompactionRange,
            table_a: ?*const TableInfo,
            level_b: u8,
            iterator_a_context: IteratorA.Context,
        ) void {
            assert(compaction.status == .idle);
            assert(compaction.callback == null);
            assert(compaction.io_pending == 0);
            assert(!compaction.merge_done and compaction.merge_iterator == null);

            assert(range.table_count > 0);

            assert(level_b < config.lsm_levels);
            assert((table_a == null) == (level_b == 0));

            // Levels may choose to drop tombstones if keys aren't included in the lower levels.
            // This invariant is always true for the last level as it doesn't have any lower ones.
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

                .merge_done = false,
                .merge_iterator = null,

                .table_builder = compaction.table_builder,
                .index = compaction.index,
                .filter = compaction.filter,
                .data = compaction.data,

                .manifest = manifest,
                .level_b = level_b,
                .remove_level_a = if (table_a) |table| table.* else null,
            };

            assert(!compaction.index.writable);
            assert(!compaction.filter.writable);
            assert(!compaction.data.writable);

            // TODO Implement manifest.move_table() optimization if there's only range.table_count == 1.
            // This would do update_tables + insert_tables inline without going through the iterators.

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
            assert(!compaction.merge_done);

            // Tables discovered by iterator_b that are visible
            var table_copy = table.*;
            compaction.manifest.update_table(compaction.level_b, compaction.snapshot, &table_copy);

            // Release the table's block addresses in the Grid as it will be made invisible.
            // This is safe; iterator_b makes a copy of the block before calling us.
            const grid = compaction.grid;
            for (Table.index_data_addresses_used(index_block)) |address| grid.release(address);
            for (Table.index_filter_addresses_used(index_block)) |address| grid.release(address);
            grid.release(Table.index_block_address(index_block));
        }

        pub fn compact_tick(compaction: *Compaction, callback: Callback) void {
            assert(compaction.status == .processing);
            assert(compaction.callback == null);
            assert(compaction.io_pending == 0);
            assert(!compaction.merge_done);

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
            compaction.io_write_start(.data);
            compaction.io_write_start(.filter);
            compaction.io_write_start(.index);
        }

        const BlockWriteField = enum { data, filter, index };

        fn io_write_start(compaction: *Compaction, comptime field: BlockWriteField) void {
            const write_callback = struct {
                fn callback(write: *Grid.Write) void {
                    const block_write = @fieldParentPtr(BlockWrite, "write", write);
                    block_write.block = undefined;

                    const _compaction = @fieldParentPtr(Compaction, @tagName(field), block_write);
                    _compaction.io_finish();
                }
            }.callback;

            const block_write: *BlockWrite = &@field(compaction, @tagName(field));
            if (block_write.writable) {
                block_write.writable = false;

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
            assert(!compaction.merge_done);

            compaction.io_pending += 1;
        }

        fn io_finish(compaction: *Compaction) void {
            assert(compaction.status == .processing);
            assert(compaction.callback != null);
            assert(compaction.io_pending > 0);
            assert(!compaction.merge_done);

            compaction.io_pending -= 1;
            if (compaction.io_pending == 0) compaction.cpu_merge_start();
        }

        fn cpu_merge_start(compaction: *Compaction) void {
            assert(compaction.status == .processing);
            assert(compaction.callback != null);
            assert(compaction.io_pending == 0);
            assert(!compaction.merge_done);

            // Create the merge iterator only when we can peek() from the read iterators.
            // This happens after IO for the first reads complete.
            if (compaction.merge_iterator == null) {
                compaction.merge_iterator = MergeIterator.init(compaction, k, .ascending);
                assert(!compaction.merge_iterator.?.empty());
            }

            assert(!compaction.data.writable);
            assert(!compaction.filter.writable);
            assert(!compaction.index.writable);

            if (!compaction.merge_iterator.?.empty()) {
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
            assert(!compaction.merge_done);

            // Ensure there are values to merge and that is it safe to do so.
            const merge_iterator = &compaction.merge_iterator.?;
            assert(!merge_iterator.empty());
            assert(!compaction.data.writable);
            assert(!compaction.filter.writable);
            assert(!compaction.index.writable);

            // Build up a data block with values merged from the read iterators.
            // This skips tombstone values if compaction was started with the intent to drop them.
            while (!compaction.table_builder.data_block_full()) {
                const value = merge_iterator.pop() orelse break;
                if (compaction.drop_tombstones and tombstone(&value)) continue;
                compaction.table_builder.data_block_append(&value);
            }

            // Finalize the data block if it's full or if it contains pending values when there's
            // no more left to merge.
            if (compaction.table_builder.data_block_full() or
                (merge_iterator.empty() and !compaction.table_builder.data_block_empty()))
            {
                compaction.table_builder.data_block_finish(.{
                    .cluster = compaction.grid.superblock.working.cluster,
                    .address = compaction.grid.acquire(),
                });

                // Mark the finished data block as writable for the next compact_tick() call.
                compaction.data.block = compaction.table_builder.data_block;
                assert(!compaction.data.writable);
                compaction.data.writable = true;
            }

            // Finalize the filter block if it's full or if it contains pending data blocks
            // when there's no more merged values to fill them.
            if (compaction.table_builder.filter_block_full() or
                (merge_iterator.empty() and !compaction.table_builder.filter_block_empty()))
            {
                compaction.table_builder.filter_block_finish(.{
                    .cluster = compaction.grid.superblock.working.cluster,
                    .address = compaction.grid.acquire(),
                });

                // Mark the finished filter block as writable for the next compact_tick() call.
                compaction.filter.block = compaction.table_builder.filter_block;
                assert(!compaction.filter.writable);
                compaction.filter.writable = true;
            }

            // Finalize the index block if it's full or if it contains pending data blocks
            // when there's no more merged values to fill them.
            if (compaction.table_builder.index_block_full() or
                (merge_iterator.empty() and !compaction.table_builder.index_block_empty()))
            {
                const table = compaction.table_builder.index_block_finish(.{
                    .cluster = compaction.grid.superblock.working.cluster,
                    .address = compaction.grid.acquire(),
                    .snapshot_min = compaction.snapshot,
                });
                compaction.manifest.insert_table(compaction.level_b, &table);

                // Mark the finished index block as writable for the next compact_tick() call.
                compaction.index.block = compaction.table_builder.index_block;
                assert(!compaction.index.writable);
                compaction.index.writable = true;
            }
        }

        fn cpu_merge_finish(compaction: *Compaction) void {
            // Ensure this is the result of a compact_tick() call that finished processing IO.
            assert(compaction.status == .processing);
            assert(compaction.callback != null);
            assert(compaction.io_pending == 0);
            assert(!compaction.merge_done);

            // Ensure merging is truly finished.
            assert(compaction.merge_iterator.?.empty());
            assert(!compaction.data.writable);
            assert(!compaction.filter.writable);
            assert(!compaction.index.writable);

            // Double check the iterators are finished as well.
            assert(compaction.iterator_a.buffered_all_values());
            assert(compaction.iterator_a.peek() == null);
            assert(compaction.iterator_b.buffered_all_values());
            assert(compaction.iterator_b.peek() == null);

            // Remove the level_a table if it was provided given it's now been merged into level_b.
            // TODO: Release the grid blocks associated with level_a as well
            if (compaction.level_b != 0) {
                const level_a = compaction.level_b - 1;

                if (compaction.remove_level_a) |*level_a_table| {
                    compaction.manifest.update_table(level_a, compaction.snapshot, level_a_table);
                    assert(level_a_table.snapshot_max == compaction.snapshot);
                }
            }

            // Finally, mark Compaction as officially complete and ready to be reset().
            compaction.merge_iterator = null;
            compaction.merge_done = true;
            compaction.status = .done;
        }

        pub fn reset(compaction: *Compaction) void {
            assert(compaction.status == .done);
            assert(compaction.callback == null);
            assert(compaction.io_pending == 0);
            assert(compaction.merge_done);

            compaction.status = .idle;
            compaction.merge_done = false;
        }
    };
}
