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
const Allocator = std.mem.Allocator;

const log = std.log.scoped(.compaction);
const tracer = @import("../tracer.zig");

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");

const stdx = @import("../stdx.zig");
const GridType = @import("grid.zig").GridType;
const alloc_block = @import("grid.zig").alloc_block;
const TableInfoType = @import("manifest.zig").TableInfoType;
const ManifestType = @import("manifest.zig").ManifestType;
const TableDataIteratorType = @import("table_data_iterator.zig").TableDataIteratorType;
const LevelDataIteratorType = @import("level_data_iterator.zig").LevelDataIteratorType;

pub fn CompactionType(
    comptime Table: type,
    comptime Tree: type,
    comptime Storage: type,
    comptime tree_name: [:0]const u8,
) type {
    return struct {
        const Compaction = @This();

        const Grid = GridType(Storage);
        const BlockPtr = Grid.BlockPtr;
        const BlockPtrConst = Grid.BlockPtrConst;

        const TableInfo = TableInfoType(Table);
        const Manifest = ManifestType(Table, Storage, tree_name);
        const CompactionRange = Manifest.CompactionRange;
        const TableDataIterator = TableDataIteratorType(Storage);
        const LevelDataIterator = LevelDataIteratorType(Table, Storage, tree_name);

        const Key = Table.Key;
        const Value = Table.Value;
        const compare_keys = Table.compare_keys;
        const key_from_value = Table.key_from_value;
        const tombstone = Table.tombstone;

        pub const TableInfoA = union(enum) {
            immutable: []const Value,
            disk: TableInfo,
        };

        pub const Context = struct {
            grid: *Grid,
            tree: *Tree,
            /// `op_min` is the first op/beat of this compaction's half-bar.
            /// `op_min` is used as a snapshot — the compaction's input tables must be visible
            /// to `op_min`.
            ///
            /// After this compaction finishes:
            /// - `op_min + half_bar_beat_count - 1` will be the input tables' snapshot_max.
            /// - `op_min + half_bar_beat_count` will be the output tables' snapshot_min.
            op_min: u64,
            table_info_a: TableInfoA,
            level_b: u8,
            range_b: CompactionRange,
            callback: fn (*Compaction) void,
        };

        const InputLevel = enum(u1) {
            a = 0,
            b = 1,
        };

        // Passed by `init`.
        tree_name: []const u8,

        // Allocated during `init`.
        iterator_a: TableDataIterator,
        iterator_b: LevelDataIterator,
        index_block_a: BlockPtr,
        data_blocks: [2]BlockPtr,
        table_builder: Table.Builder,
        last_keys_in: [2]?Key = .{ null, null },

        // Passed by `start`.
        context: Context,

        grid_reservation: ?Grid.Reservation,
        drop_tombstones: bool,

        // These point inside either `data_blocks` or `context.table_info_a.immutable`.
        values_in: [2][]const Value,

        input_state: enum {
            remaining,
            exhausted,
        },
        state: union(enum) {
            idle,
            compacting,
            iter_init_a,
            iter_next: InputLevel,
            writing: struct {
                pending: usize,
            },
            next_tick,
            done,
        },

        next_tick: Grid.NextTick = undefined,
        read: Grid.Read = undefined,
        data_block_address: ?u64 = null,
        write_data_block: Grid.Write = undefined,
        write_filter_block: Grid.Write = undefined,
        write_index_block: Grid.Write = undefined,

        tracer_slot: ?tracer.SpanStart,
        iter_tracer_slot: ?tracer.SpanStart,

        pub fn init(allocator: Allocator) !Compaction {
            var iterator_a = try TableDataIterator.init(allocator);
            errdefer iterator_a.deinit(allocator);

            var iterator_b = try LevelDataIterator.init(allocator);
            errdefer iterator_b.deinit(allocator);

            const index_block_a = try alloc_block(allocator);
            errdefer allocator.free(index_block_a);

            var data_blocks: [2]Grid.BlockPtr = undefined;

            data_blocks[0] = try alloc_block(allocator);
            errdefer allocator.free(data_blocks[0]);

            data_blocks[1] = try alloc_block(allocator);
            errdefer allocator.free(data_blocks[1]);

            var table_builder = try Table.Builder.init(allocator);
            errdefer table_builder.deinit(allocator);

            return Compaction{
                .tree_name = tree_name,

                .iterator_a = iterator_a,
                .iterator_b = iterator_b,
                .index_block_a = index_block_a,
                .data_blocks = data_blocks,
                .table_builder = table_builder,

                .context = undefined,
                .grid_reservation = null,
                .drop_tombstones = undefined,

                .values_in = .{ &.{}, &.{} },

                .input_state = .remaining,
                .state = .idle,

                .tracer_slot = null,
                .iter_tracer_slot = null,
            };
        }

        pub fn deinit(compaction: *Compaction, allocator: Allocator) void {
            compaction.table_builder.deinit(allocator);
            for (compaction.data_blocks) |data_block| allocator.free(data_block);
            allocator.free(compaction.index_block_a);
            compaction.iterator_b.deinit(allocator);
            compaction.iterator_a.deinit(allocator);
        }

        pub fn reset(compaction: *Compaction) void {
            assert(compaction.state == .done);
            compaction.state = .idle;
            if (compaction.grid_reservation) |grid_reservation| {
                compaction.context.grid.forfeit(grid_reservation);
                compaction.grid_reservation = null;
            }
        }

        /// The compaction's input tables are:
        /// * `context.table_a_info` (which is `.immutable` when `context_level_b` is 0), and
        /// * Any level_b tables visible to `context.op_min` within `context.range_b`.
        pub fn start(
            compaction: *Compaction,
            context: Context,
        ) void {
            assert(compaction.state == .idle);
            assert(compaction.grid_reservation == null);

            tracer.start(
                &compaction.tracer_slot,
                .{ .tree_compaction = .{
                    .tree_name = compaction.tree_name,
                    .level_b = context.level_b,
                } },
                @src(),
            );

            const can_move_table =
                context.table_info_a == .disk and
                context.range_b.table_count == 1;

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
            const grid_reservation = if (can_move_table)
                null
            else
                context.grid.reserve(
                    context.range_b.table_count * Table.block_count_max,
                ).?;

            // Levels may choose to drop tombstones if keys aren't included in the lower levels.
            // This invariant is always true for the last level as it doesn't have any lower ones.
            const drop_tombstones = context.tree.manifest.compaction_must_drop_tombstones(
                context.level_b,
                context.range_b,
            );
            assert(drop_tombstones or context.level_b < constants.lsm_levels - 1);

            compaction.* = .{
                .tree_name = compaction.tree_name,

                .iterator_a = compaction.iterator_a,
                .iterator_b = compaction.iterator_b,
                .index_block_a = compaction.index_block_a,
                .data_blocks = compaction.data_blocks,
                .table_builder = compaction.table_builder,

                .context = context,

                .values_in = .{ &.{}, &.{} },

                .grid_reservation = grid_reservation,
                .drop_tombstones = drop_tombstones,
                .input_state = .remaining,
                .state = .compacting,

                .tracer_slot = compaction.tracer_slot,
                .iter_tracer_slot = compaction.iter_tracer_slot,
            };

            if (can_move_table) {
                // If we can just move the table, don't bother with compaction.

                log.debug(
                    "{s}: Moving table: level_b={}",
                    .{ compaction.tree_name, context.level_b },
                );

                const snapshot_max = snapshot_max_for_table_input(context.op_min);
                const level_b = context.level_b;
                const level_a = level_b - 1;

                const table_a = &context.table_info_a.disk;
                assert(table_a.snapshot_max >= snapshot_max);

                context.tree.manifest.move_table(level_a, level_b, table_a);

                compaction.state = .next_tick;
                compaction.context.grid.on_next_tick(
                    done_on_next_tick,
                    &compaction.next_tick,
                    .main_thread,
                );
            } else {
                // Otherwise, start merging.

                log.debug(
                    "{s}: Merging table: level_b={}",
                    .{ compaction.tree_name, context.level_b },
                );

                compaction.iterator_b.start(.{
                    .grid = context.grid,
                    .manifest = &context.tree.manifest,
                    .level = context.level_b,
                    .snapshot = context.op_min,
                    .key_min = context.range_b.key_min,
                    .key_max = context.range_b.key_max,
                    .read_name = "Compaction(" ++ tree_name ++ ").iterator_b(read_block_validate)",
                });

                switch (context.table_info_a) {
                    .immutable => |values| {
                        compaction.values_in[0] = values;
                        compaction.loop_start();
                    },
                    .disk => |table_info| {
                        compaction.state = .iter_init_a;
                        compaction.context.grid.read_block(
                            on_iter_init_a,
                            &compaction.read,
                            table_info.address,
                            table_info.checksum,
                            .index,
                            "Compaction(" ++ tree_name ++ ").start(read_block_validate)",
                        );
                    },
                }
            }
        }

        fn on_iter_init_a(read: *Grid.Read, index_block: BlockPtrConst) void {
            const compaction = @fieldParentPtr(Compaction, "read", read);
            assert(compaction.state == .iter_init_a);

            // `index_block` is only valid for this callback, so copy its contents.
            // TODO(jamii) This copy can be avoided if we bypass the cache.
            stdx.copy_disjoint(.exact, u8, compaction.index_block_a, index_block);
            compaction.iterator_a.start(.{
                .grid = compaction.context.grid,
                .addresses = Table.index_data_addresses_used(compaction.index_block_a),
                .checksums = Table.index_data_checksums_used(compaction.index_block_a),
                .read_name = "Compaction(" ++ tree_name ++ ").iterator_a(read_block_validate)",
            });

            compaction.state = .compacting;
            compaction.loop_start();
        }

        fn loop_start(compaction: *Compaction) void {
            assert(compaction.state == .compacting);

            tracer.start(
                &compaction.iter_tracer_slot,
                .{ .tree_compaction_iter = .{
                    .tree_name = compaction.tree_name,
                    .level_b = compaction.context.level_b,
                } },
                @src(),
            );

            compaction.iter_check(.a);
        }

        /// If `values_in[index]` is empty and more values are available, read them.
        fn iter_check(compaction: *Compaction, input_level: InputLevel) void {
            assert(compaction.state == .compacting);

            if (compaction.values_in[@enumToInt(input_level)].len > 0) {
                // Still have values on this input_level, no need to refill.
                compaction.iter_check_finish(input_level);
            } else if (input_level == .a and compaction.context.table_info_a == .immutable) {
                // No iterator to call next on.
                compaction.iter_check_finish(input_level);
            } else {
                compaction.state = .{ .iter_next = input_level };
                switch (input_level) {
                    .a => compaction.iterator_a.next(iter_next_a),
                    .b => compaction.iterator_b.next(.{
                        .on_index = on_index_block,
                        .on_data = iter_next_b,
                    }),
                }
            }
        }

        fn on_index_block(
            iterator_b: *LevelDataIterator,
            table_info: TableInfo,
            index_block: BlockPtrConst,
        ) void {
            const compaction = @fieldParentPtr(Compaction, "iterator_b", iterator_b);
            assert(std.meta.eql(compaction.state, .{ .iter_next = .b }));

            // Tables that we've compacted should become invisible at the end of this half-bar.
            var table_copy = table_info;
            compaction.context.tree.manifest.update_table(
                compaction.context.level_b,
                snapshot_max_for_table_input(compaction.context.op_min),
                &table_copy,
            );

            // Release the table's block addresses in the Grid as it will be made invisible.
            // This is safe; iterator_b makes a copy of the block before calling us.
            const grid = compaction.context.grid;
            for (Table.index_data_addresses_used(index_block)) |address| {
                grid.release(address);
            }
            for (Table.index_filter_addresses_used(index_block)) |address| {
                grid.release(address);
            }
            grid.release(Table.index_block_address(index_block));
        }

        fn iter_next_a(iterator_a: *TableDataIterator, data_block: ?BlockPtrConst) void {
            const compaction = @fieldParentPtr(Compaction, "iterator_a", iterator_a);
            assert(std.meta.eql(compaction.state, .{ .iter_next = .a }));
            compaction.iter_next(data_block);
        }

        fn iter_next_b(iterator_b: *LevelDataIterator, data_block: ?BlockPtrConst) void {
            const compaction = @fieldParentPtr(Compaction, "iterator_b", iterator_b);
            assert(std.meta.eql(compaction.state, .{ .iter_next = .b }));
            compaction.iter_next(data_block);
        }

        fn iter_next(compaction: *Compaction, data_block: ?BlockPtrConst) void {
            assert(compaction.state == .iter_next);
            const input_level = compaction.state.iter_next;
            const index = @enumToInt(input_level);

            if (data_block) |block| {
                // `data_block` is only valid for this callback, so copy its contents.
                // TODO(jamii) This copy can be avoided if we bypass the cache.
                stdx.copy_disjoint(.exact, u8, compaction.data_blocks[index], block);
                compaction.values_in[index] =
                    Table.data_block_values_used(compaction.data_blocks[index]);

                // Assert that we're reading data blocks in key order.
                const values_in = compaction.values_in[index];
                if (values_in.len > 0) {
                    const first_key = key_from_value(&values_in[0]);
                    const last_key = key_from_value(&values_in[values_in.len - 1]);
                    if (compaction.last_keys_in[index]) |last_key_prev| {
                        assert(compare_keys(last_key_prev, first_key) == .lt);
                    }
                    if (values_in.len > 1) {
                        assert(compare_keys(first_key, last_key) == .lt);
                    }
                    compaction.last_keys_in[index] = last_key;
                }
            } else {
                // If no more data blocks available, just leave `values_in[index]` empty.
            }

            compaction.state = .compacting;
            compaction.iter_check_finish(input_level);
        }

        fn iter_check_finish(compaction: *Compaction, input_level: InputLevel) void {
            switch (input_level) {
                .a => compaction.iter_check(.b),
                .b => compaction.compact(),
            }
        }

        fn compact(compaction: *Compaction) void {
            assert(compaction.state == .compacting);
            assert(compaction.table_builder.value_count < Table.layout.block_value_count_max);

            const values_in = compaction.values_in;

            var tracer_slot: ?tracer.SpanStart = null;
            tracer.start(
                &tracer_slot,
                .{ .tree_compaction_merge = .{
                    .tree_name = compaction.tree_name,
                    .level_b = compaction.context.level_b,
                } },
                @src(),
            );

            if (values_in[0].len == 0 and values_in[1].len == 0) {
                compaction.input_state = .exhausted;
            } else if (values_in[0].len == 0) {
                compaction.copy(.b);
            } else if (values_in[1].len == 0) {
                if (compaction.drop_tombstones) {
                    compaction.copy_drop_tombstones();
                } else {
                    compaction.copy(.a);
                }
            } else {
                compaction.merge();
            }

            tracer.end(
                &tracer_slot,
                .{ .tree_compaction_merge = .{
                    .tree_name = compaction.tree_name,
                    .level_b = compaction.context.level_b,
                } },
            );

            compaction.write_blocks();
        }

        fn copy(compaction: *Compaction, input_level: InputLevel) void {
            assert(compaction.state == .compacting);
            assert(compaction.values_in[@enumToInt(input_level) +% 1].len == 0);
            assert(compaction.table_builder.value_count < Table.layout.block_value_count_max);

            const values_in = compaction.values_in[@enumToInt(input_level)];
            const values_out = compaction.table_builder.data_block_values();
            var values_out_index = compaction.table_builder.value_count;

            assert(values_in.len > 0);

            const len = @minimum(values_in.len, values_out.len - values_out_index);
            assert(len > 0);
            stdx.copy_disjoint(
                .exact,
                Value,
                values_out[values_out_index..][0..len],
                values_in[0..len],
            );

            compaction.values_in[@enumToInt(input_level)] = values_in[len..];
            compaction.table_builder.value_count += @intCast(u32, len);
        }

        fn copy_drop_tombstones(compaction: *Compaction) void {
            assert(compaction.state == .compacting);
            assert(compaction.values_in[1].len == 0);
            assert(compaction.table_builder.value_count < Table.layout.block_value_count_max);

            // Copy variables locally to ensure a tight loop.
            const values_in_a = compaction.values_in[0];
            const values_out = compaction.table_builder.data_block_values();
            var values_in_a_index: usize = 0;
            var values_out_index = compaction.table_builder.value_count;

            // Merge as many values as possible.
            while (values_in_a_index < values_in_a.len and
                values_out_index < values_out.len)
            {
                const value_a = &values_in_a[values_in_a_index];
                values_in_a_index += 1;
                if (tombstone(value_a)) {
                    continue;
                }
                values_out[values_out_index] = value_a.*;
                values_out_index += 1;
            }

            // Copy variables back out.
            compaction.values_in[0] = values_in_a[values_in_a_index..];
            compaction.table_builder.value_count = values_out_index;
        }

        fn merge(compaction: *Compaction) void {
            assert(compaction.values_in[0].len > 0);
            assert(compaction.values_in[1].len > 0);
            assert(compaction.table_builder.value_count < Table.layout.block_value_count_max);

            // Copy variables locally to ensure a tight loop.
            const values_in_a = compaction.values_in[0];
            const values_in_b = compaction.values_in[1];
            const values_out = compaction.table_builder.data_block_values();
            var values_in_a_index: usize = 0;
            var values_in_b_index: usize = 0;
            var values_out_index = compaction.table_builder.value_count;

            // Merge as many values as possible.
            while (values_in_a_index < values_in_a.len and
                values_in_b_index < values_in_b.len and
                values_out_index < values_out.len)
            {
                const value_a = &values_in_a[values_in_a_index];
                const value_b = &values_in_b[values_in_b_index];
                switch (compare_keys(key_from_value(value_a), key_from_value(value_b))) {
                    .lt => {
                        values_in_a_index += 1;
                        if (compaction.drop_tombstones and
                            tombstone(value_a))
                        {
                            continue;
                        }
                        values_out[values_out_index] = value_a.*;
                        values_out_index += 1;
                    },
                    .gt => {
                        values_in_b_index += 1;
                        values_out[values_out_index] = value_b.*;
                        values_out_index += 1;
                    },
                    .eq => {
                        values_in_a_index += 1;
                        values_in_b_index += 1;
                        if (Table.usage == .secondary_index) {
                            if (tombstone(value_a)) {
                                assert(!tombstone(value_b));
                                continue;
                            }
                            if (tombstone(value_b)) {
                                assert(!tombstone(value_a));
                                continue;
                            }
                        } else if (compaction.drop_tombstones) {
                            if (tombstone(value_a)) {
                                continue;
                            }
                        }
                        values_out[values_out_index] = value_a.*;
                        values_out_index += 1;
                    },
                }
            }

            // Copy variables back out.
            compaction.values_in[0] = values_in_a[values_in_a_index..];
            compaction.values_in[1] = values_in_b[values_in_b_index..];
            compaction.table_builder.value_count = values_out_index;
        }

        fn write_blocks(compaction: *Compaction) void {
            assert(compaction.state == .compacting);
            const input_exhausted = compaction.input_state == .exhausted;
            const table_builder = &compaction.table_builder;
            const grid = compaction.context.grid;

            // Flush the data block if needed on a background thread.
            if (table_builder.data_block_full() or
                // If the filter or index blocks need to be flushed,
                // the data block has to be flushed first.
                table_builder.filter_block_full() or
                table_builder.index_block_full() or
                // If the input is exhausted then we need to flush all blocks before finishing.
                (input_exhausted and !table_builder.data_block_empty()))
            {
                assert(compaction.data_block_address == null);
                compaction.data_block_address = grid.acquire(compaction.grid_reservation.?);
                grid.on_next_tick(finish_data_block, &compaction.next_tick, .background_thread);
                return;
            }

            finish_remaining_blocks(&compaction.next_tick);
        }

        fn finish_data_block(next_tick: *Grid.NextTick) void {
            const compaction = @fieldParentPtr(Compaction, "next_tick", next_tick);
            const grid = compaction.context.grid;
            assert(grid.context() == .background_thread);

            vsr.checksum_context = "Compaction(" ++ tree_name ++ ").data_block_finish";
            compaction.table_builder.data_block_finish(.{
                .cluster = compaction.context.grid.superblock.working.cluster,
                .address = compaction.data_block_address.?,
            });
            vsr.checksum_context = null;

            // Finish the remaining blocks on the main thread as they interact with grid/manifest.
            grid.on_next_tick(
                finish_remaining_blocks,
                &compaction.next_tick,
                .main_thread,
            );
        }

        fn finish_remaining_blocks(next_tick: *Grid.NextTick) void {
            const compaction = @fieldParentPtr(Compaction, "next_tick", next_tick);
            const input_exhausted = compaction.input_state == .exhausted;
            const table_builder = &compaction.table_builder;

            compaction.state = .{ .writing = .{ .pending = 0 } };

            // Check if the data block was flushed and write it out:
            if (compaction.data_block_address != null) {
                compaction.data_block_address = null;
                WriteBlock(.data).write_block(compaction);
            }

            // Flush the filter block if needed.
            if (table_builder.filter_block_full() or
                // If the index block need to be flushed,
                // the filter block has to be flushed first.
                table_builder.index_block_full() or
                // If the input is exhausted then we need to flush all blocks before finishing.
                (input_exhausted and !table_builder.filter_block_empty()))
            {
                {
                    const addr = compaction.context.grid.acquire(compaction.grid_reservation.?);

                    vsr.checksum_context = "Compaction(" ++ tree_name ++ ").filter_block_finish";
                    defer vsr.checksum_context = null;

                    table_builder.filter_block_finish(.{
                        .cluster = compaction.context.grid.superblock.working.cluster,
                        .address = addr,
                    });
                }
                WriteBlock(.filter).write_block(compaction);
            }

            // Flush the index block if needed.
            if (table_builder.index_block_full() or
                // If the input is exhausted then we need to flush all blocks before finishing.
                (input_exhausted and !table_builder.index_block_empty()))
            {
                const table = blk: {
                    const addr = compaction.context.grid.acquire(compaction.grid_reservation.?);

                    vsr.checksum_context = "Compaction(" ++ tree_name ++ ").index_block_finish";
                    defer vsr.checksum_context = null;

                    break :blk table_builder.index_block_finish(.{
                        .cluster = compaction.context.grid.superblock.working.cluster,
                        .address = addr,
                        .snapshot_min = snapshot_min_for_table_output(compaction.context.op_min),
                    });
                };
                // Make this table visible at the end of this half-bar.
                compaction.context.tree.manifest.insert_table(compaction.context.level_b, &table);
                WriteBlock(.index).write_block(compaction);
            }

            if (compaction.state.writing.pending == 0) {
                compaction.write_finish();
            }
        }

        const WriteBlockField = enum { data, filter, index };
        fn WriteBlock(comptime write_block_field: WriteBlockField) type {
            return struct {
                fn write_block(compaction: *Compaction) void {
                    assert(compaction.state == .writing);

                    const write = switch (write_block_field) {
                        .data => &compaction.write_data_block,
                        .filter => &compaction.write_filter_block,
                        .index => &compaction.write_index_block,
                    };
                    const block = switch (write_block_field) {
                        .data => &compaction.table_builder.data_block,
                        .filter => &compaction.table_builder.filter_block,
                        .index => &compaction.table_builder.index_block,
                    };
                    compaction.state.writing.pending += 1;
                    compaction.context.grid.write_block(
                        on_write,
                        write,
                        block,
                        Table.block_address(block.*),
                    );
                }

                fn on_write(write: *Grid.Write) void {
                    const compaction = @fieldParentPtr(
                        Compaction,
                        switch (write_block_field) {
                            .data => "write_data_block",
                            .filter => "write_filter_block",
                            .index => "write_index_block",
                        },
                        write,
                    );
                    assert(compaction.state == .writing);
                    compaction.state.writing.pending -= 1;
                    if (compaction.state.writing.pending == 0) {
                        compaction.write_finish();
                    }
                }
            };
        }

        fn write_finish(compaction: *Compaction) void {
            assert(compaction.state == .writing);
            assert(compaction.state.writing.pending == 0);

            tracer.end(
                &compaction.iter_tracer_slot,
                .{ .tree_compaction_iter = .{
                    .tree_name = compaction.tree_name,
                    .level_b = compaction.context.level_b,
                } },
            );

            switch (compaction.input_state) {
                .remaining => {
                    compaction.state = .next_tick;
                    compaction.context.grid.on_next_tick(
                        loop_on_next_tick,
                        &compaction.next_tick,
                        .main_thread,
                    );
                },
                .exhausted => {
                    // Mark the level_a table as invisible if it was provided;
                    // it has been merged into level_b.
                    // TODO: Release the grid blocks associated with level_a as well
                    switch (compaction.context.table_info_a) {
                        .immutable => {},
                        .disk => |table| {
                            const level_a = compaction.context.level_b - 1;
                            const snapshot_max = snapshot_max_for_table_input(
                                compaction.context.op_min,
                            );
                            var table_copy = table;
                            compaction.context.tree.manifest.update_table(
                                level_a,
                                snapshot_max,
                                &table_copy,
                            );
                            assert(table_copy.snapshot_max == snapshot_max);
                        },
                    }

                    compaction.state = .next_tick;
                    compaction.context.grid.on_next_tick(
                        done_on_next_tick,
                        &compaction.next_tick,
                        .main_thread,
                    );
                },
            }
        }

        fn loop_on_next_tick(next_tick: *Grid.NextTick) void {
            const compaction = @fieldParentPtr(Compaction, "next_tick", next_tick);
            assert(compaction.state == .next_tick);
            assert(compaction.input_state == .remaining);

            compaction.state = .compacting;
            compaction.loop_start();
        }

        fn done_on_next_tick(next_tick: *Grid.NextTick) void {
            const compaction = @fieldParentPtr(Compaction, "next_tick", next_tick);
            assert(compaction.state == .next_tick);

            compaction.state = .done;

            tracer.end(
                &compaction.tracer_slot,
                .{ .tree_compaction = .{
                    .tree_name = compaction.tree_name,
                    .level_b = compaction.context.level_b,
                } },
            );

            const callback = compaction.context.callback;
            callback(compaction);
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
