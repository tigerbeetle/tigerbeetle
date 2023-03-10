//! Compaction moves or merges a table's values into the next level.
//!
//! Each Compaction is paced to run in one half-bar.
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
//! 5. Update the input tables in the Manifest with their new `snapshot_max` so that they become
//!    invisible to subsequent read transactions.
//!
//! 6. Insert the new level-B tables into the Manifest.
//!
//! † When A's value is a tombstone, there is a special case for garbage collection. When either:
//! * level B is the final level, or
//! * A's key does not exist in B or any deeper level,
//! then the tombstone is omitted from the compacted output (see: `compaction_must_drop_tombstones`).
//!
const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const log = std.log.scoped(.compaction);
const tracer = @import("../tracer.zig");

const constants = @import("../constants.zig");

const GridType = @import("grid.zig").GridType;
const ManifestType = @import("manifest.zig").ManifestType;
const MergeIteratorType = @import("merge_iterator.zig").MergeIteratorType;
const TableIteratorType = @import("table_iterator.zig").TableIteratorType;
const LevelIteratorType = @import("level_iterator.zig").LevelIteratorType;

pub fn CompactionType(
    comptime Table: type,
    comptime Storage: type,
    comptime IteratorAType: anytype,
) type {
    const tombstone = Table.tombstone;

    return struct {
        const Compaction = @This();

        const Grid = GridType(Storage);
        const BlockPtr = Grid.BlockPtr;
        const BlockPtrConst = Grid.BlockPtrConst;
        const BlockWrite = struct {
            write: Grid.Write = undefined,
            block: *BlockPtr = undefined,
            state: BlockState = .building,
        };
        const BlockState = enum {
            building,
            writable,
            writing,
        };

        const Manifest = ManifestType(Table, Storage);
        const TableInfo = Manifest.TableInfo;

        const IteratorA = IteratorAType(Table, Storage);
        const IteratorB = LevelIteratorType(Table, Storage);

        const MergeIterator = MergeIteratorType(
            Table,
            IteratorA,
            IteratorB,
        );

        pub const Callback = fn (it: *Compaction) void;

        const Status = enum {
            idle,
            processing,
            done,
        };

        /// Used only for debugging/tracing.
        tree_name: []const u8,

        grid: *Grid,
        grid_reservation: Grid.Reservation,
        range: Manifest.CompactionRange,

        /// `op_min` is the first op/beat of this compaction's half-bar.
        /// `op_min` is used as a snapshot — the compaction's input tables must be visible
        /// to `op_min`.
        ///
        /// After this compaction finishes:
        /// - `op_min + half_bar_beat_count - 1` will be the input tables' snapshot_max.
        /// - `op_min + half_bar_beat_count` will be the output tables' snapshot_min.
        op_min: u64,
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
        level_a_input: ?TableInfo,

        tables_output_count: usize = 0,

        tracer_slot: ?tracer.SpanStart = null,

        tick_count_remaining: u8 = 0,

        pub fn init(allocator: mem.Allocator, tree_name: [:0]const u8) !Compaction {
            var iterator_a = try IteratorA.init(allocator);
            errdefer iterator_a.deinit(allocator);

            var iterator_b = try IteratorB.init(allocator);
            errdefer iterator_b.deinit(allocator);

            var table_builder = try Table.Builder.init(allocator);
            errdefer table_builder.deinit(allocator);

            return Compaction{
                .tree_name = tree_name,

                // Assigned by start()
                .grid = undefined,
                .grid_reservation = undefined,
                .range = undefined,
                .op_min = undefined,
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
                .level_a_input = null,
            };
        }

        pub fn deinit(compaction: *Compaction, allocator: mem.Allocator) void {
            compaction.table_builder.deinit(allocator);

            compaction.iterator_b.deinit(allocator);
            compaction.iterator_a.deinit(allocator);
        }

        /// The compaction's input tables are:
        /// * table_a (which is null when level B is 0), and
        /// * any level-B tables visible to `op_min` within `range`.
        pub fn start(
            compaction: *Compaction,
            grid: *Grid,
            manifest: *Manifest,
            op_min: u64,
            range: Manifest.CompactionRange,
            table_a: ?*const TableInfo,
            level_b: u8,
            iterator_a_context: IteratorA.Context,
        ) void {
            assert(compaction.status == .idle);
            assert(compaction.callback == null);
            assert(compaction.io_pending == 0);
            assert(!compaction.merge_done and compaction.merge_iterator == null);
            assert(compaction.tracer_slot == null);

            assert(op_min % @divExact(constants.lsm_batch_multiple, 2) == 0);
            assert(range.table_count > 0);
            if (table_a) |t| assert(t.visible(op_min));

            assert(level_b < constants.lsm_levels);
            assert((level_b == 0) == (table_a == null));

            // Levels may choose to drop tombstones if keys aren't included in the lower levels.
            // This invariant is always true for the last level as it doesn't have any lower ones.
            const drop_tombstones = manifest.compaction_must_drop_tombstones(level_b, range);
            assert(drop_tombstones or level_b < constants.lsm_levels - 1);

            compaction.* = .{
                .tree_name = compaction.tree_name,

                .grid = grid,
                // Reserve enough blocks to write our output tables in the worst case, where:
                // - no tombstones are dropped,
                // - no values are overwritten,
                // - and all tables are full.
                //
                // We must reserve before doing any async work so that the block acquisition order
                // is deterministic (relative to other concurrent compactions).
                // TODO The replica must stop accepting requests if it runs out of blocks/capacity,
                // rather than panicking here.
                // TODO(Compaction Pacing): Reserve smaller increments, at the start of each beat.
                // (And likewise release the reservation at the end of each beat, instead of at the
                // end of each half-bar).
                // TODO(Move Table) Don't reserve these when we just move the table to the next level.
                .grid_reservation = grid.reserve(range.table_count * Table.block_count_max).?,
                .range = range,
                .op_min = op_min,
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
                .level_a_input = if (table_a) |table| table.* else null,
            };

            assert(compaction.index.state == .building);
            assert(compaction.filter.state == .building);
            assert(compaction.data.state == .building);

            // TODO Implement manifest.move_table() optimization if there's only range.table_count == 1.
            // This would do update_tables + insert_tables inline without going through the iterators.

            const iterator_b_context = .{
                .grid = grid,
                .manifest = manifest,
                .level = level_b,
                .snapshot = op_min,
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
            assert(table.visible(compaction.op_min));

            // Tables discovered by iterator_b that are visible at the start of compaction.
            var table_copy = table.*;
            compaction.manifest.update_table(
                compaction.level_b,
                snapshot_max_for_table_input(compaction.op_min),
                &table_copy,
            );

            // Release the table's block addresses in the Grid as it will be made invisible.
            // This is safe; iterator_b makes a copy of the block before calling us.
            const grid = compaction.grid;
            for (Table.index_data_addresses_used(index_block)) |address| {
                grid.release(address);
            }
            for (Table.index_filter_addresses_used(index_block)) |address| {
                grid.release(address);
            }
            grid.release(Table.index_block_address(index_block));
        }

        pub fn compact_tick_batch(compaction: *Compaction, tick_count_max: u8, callback: Callback) void {
            assert(compaction.status == .processing);
            assert(compaction.callback == null);
            assert(compaction.io_pending == 0);
            assert(!compaction.merge_done);
            assert(tick_count_max > 0);

            compaction.tick_count_remaining = tick_count_max;
            compaction.callback = callback;

            compaction.compact_tick();
        }

        fn compact_tick(compaction: *Compaction) void {
            assert(compaction.tick_count_remaining > 0);
            compaction.tick_count_remaining -= 1;

            tracer.start(
                &compaction.tracer_slot,
                .{ .tree_compaction_tick = .{
                    .tree_name = compaction.tree_name,
                    .level_b = compaction.level_b,
                } },
                @src(),
            );

            // Generate fake IO to make sure io_pending doesn't reach zero multiple times from
            // IO being completed inline down below.
            // The fake IO is immediately resolved and triggers the cpu_merge_start if all
            // IO completes inline or if no IO was started.
            compaction.io_start();
            defer compaction.io_finish();

            // Start reading blocks from the iterators to merge them.
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

                    assert(block_write.state == .writing);
                    block_write.state = .building;

                    if (constants.verify) {
                        // We've finished writing so the block should now be zeroed.
                        assert(mem.allEqual(u8, block_write.block.*, 0));
                    }
                    block_write.block = undefined;

                    const _compaction = @fieldParentPtr(Compaction, @tagName(field), block_write);
                    _compaction.io_finish();
                }
            }.callback;

            const block_write: *BlockWrite = &@field(compaction, @tagName(field));
            if (block_write.state == .writable) {
                block_write.state = .writing;

                compaction.io_start();
                compaction.grid.write_block(
                    write_callback,
                    &block_write.write,
                    block_write.block,
                    Table.block_address(block_write.block.*),
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

            var tracer_slot: ?tracer.SpanStart = null;
            tracer.start(
                &tracer_slot,
                .{ .tree_compaction_merge = .{
                    .tree_name = compaction.tree_name,
                    .level_b = compaction.level_b,
                } },
                @src(),
            );

            // Create the merge iterator only when we can peek() from the read iterators.
            // This happens after IO for the first reads complete.
            if (compaction.merge_iterator == null) {
                compaction.merge_iterator = MergeIterator.init(
                    &compaction.iterator_a,
                    &compaction.iterator_b,
                );
                assert(!compaction.merge_iterator.?.empty());
            }

            assert(compaction.data.state == .building);
            assert(compaction.filter.state == .building);
            assert(compaction.index.state == .building);

            if (!compaction.merge_iterator.?.empty()) {
                compaction.cpu_merge();
            } else {
                compaction.cpu_merge_finish();
            }

            tracer.end(
                &tracer_slot,
                .{ .tree_compaction_merge = .{
                    .tree_name = compaction.tree_name,
                    .level_b = compaction.level_b,
                } },
            );
            tracer.end(
                &compaction.tracer_slot,
                .{ .tree_compaction_tick = .{
                    .tree_name = compaction.tree_name,
                    .level_b = compaction.level_b,
                } },
            );

            if (compaction.status == .processing and
                compaction.tick_count_remaining > 0)
            {
                compaction.compact_tick();
            } else {
                const callback = compaction.callback.?;
                compaction.callback = null;
                callback(compaction);
            }
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
            assert(compaction.data.state == .building);
            assert(compaction.filter.state == .building);
            assert(compaction.index.state == .building);

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
                compaction.table_builder.filter_block_full() or
                compaction.table_builder.index_block_full() or
                (merge_iterator.empty() and !compaction.table_builder.data_block_empty()))
            {
                compaction.table_builder.data_block_finish(.{
                    .cluster = compaction.grid.superblock.working.cluster,
                    .address = compaction.grid.acquire(compaction.grid_reservation),
                });

                // Mark the finished data block as writable for the next compact_tick() call.
                compaction.data.block = &compaction.table_builder.data_block;
                assert(compaction.data.state == .building);
                compaction.data.state = .writable;
            }

            // Finalize the filter block if it's full or if it contains pending data blocks
            // when there's no more merged values to fill them.
            if (compaction.table_builder.filter_block_full() or
                compaction.table_builder.index_block_full() or
                (merge_iterator.empty() and !compaction.table_builder.filter_block_empty()))
            {
                compaction.table_builder.filter_block_finish(.{
                    .cluster = compaction.grid.superblock.working.cluster,
                    .address = compaction.grid.acquire(compaction.grid_reservation),
                });

                // Mark the finished filter block as writable for the next compact_tick() call.
                compaction.filter.block = &compaction.table_builder.filter_block;
                assert(compaction.filter.state == .building);
                compaction.filter.state = .writable;
            }

            // Finalize the index block if it's full or if it contains pending data blocks
            // when there's no more merged values to fill them.
            if (compaction.table_builder.index_block_full() or
                (merge_iterator.empty() and !compaction.table_builder.index_block_empty()))
            {
                const table = compaction.table_builder.index_block_finish(.{
                    .cluster = compaction.grid.superblock.working.cluster,
                    .address = compaction.grid.acquire(compaction.grid_reservation),
                    .snapshot_min = snapshot_min_for_table_output(compaction.op_min),
                    // TODO(Persistent Snapshots) set snapshot_max to the minimum snapshot_max of
                    // all the (original) input tables.
                });
                compaction.manifest.insert_table(compaction.level_b, &table);

                // Mark the finished index block as writable for the next compact_tick() call.
                compaction.index.block = &compaction.table_builder.index_block;
                assert(compaction.index.state == .building);
                compaction.index.state = .writable;

                compaction.tables_output_count += 1;
                assert(compaction.tables_output_count <= compaction.range.table_count);
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
            assert(compaction.data.state == .building);
            assert(compaction.filter.state == .building);
            assert(compaction.index.state == .building);

            // Double check the iterators are finished as well.
            const stream_empty = struct {
                fn empty(it: anytype) bool {
                    _ = it.peek() catch |err| switch (err) {
                        error.Drained => {},
                        error.Empty => {
                            assert(it.buffered_all_values());
                            return true;
                        },
                    };
                    return false;
                }
            }.empty;
            assert(stream_empty(&compaction.iterator_a));
            assert(stream_empty(&compaction.iterator_b));

            // Mark the level_a table as invisible if it was provided;
            // it has been merged into level_b.
            // TODO: Release the grid blocks associated with level_a as well
            if (compaction.level_a_input) |*level_a_table| {
                const level_a = compaction.level_b - 1;
                const snapshot_max = snapshot_max_for_table_input(compaction.op_min);
                compaction.manifest.update_table(level_a, snapshot_max, level_a_table);
                assert(level_a_table.snapshot_max == snapshot_max);
            } else {
                assert(compaction.level_b == 0);
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
            assert(compaction.tracer_slot == null);

            // TODO(Beat Pacing) This should really be where the compaction callback is invoked,
            // but currently that can occur multiple times per beat.
            compaction.grid.forfeit(compaction.grid_reservation);

            compaction.status = .idle;
            compaction.merge_done = false;
        }
    };
}

fn snapshot_max_for_table_input(op_min: u64) u64 {
    assert(op_min % @divExact(constants.lsm_batch_multiple, 2) == 0);
    return op_min + @divExact(constants.lsm_batch_multiple, 2) - 1;
}

fn snapshot_min_for_table_output(op_min: u64) u64 {
    assert(op_min % @divExact(constants.lsm_batch_multiple, 2) == 0);
    return op_min + @divExact(constants.lsm_batch_multiple, 2);
}
