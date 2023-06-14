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
const FIFO = @import("../fifo.zig").FIFO;

const constants = @import("../constants.zig");

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
) type {
    return struct {
        const Compaction = @This();

        const Grid = GridType(Storage);
        const BlockPtr = Grid.BlockPtr;
        const BlockPtrConst = Grid.BlockPtrConst;

        const TableInfo = TableInfoType(Table);
        const Manifest = ManifestType(Table, Storage);
        const CompactionRange = Manifest.CompactionRange;
        const TableDataIterator = TableDataIteratorType(Storage);
        const LevelDataIterator = LevelDataIteratorType(Table, Storage);

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

            /// Callback gets called each time a unit of compaction is done.
            /// It needs to look at .state to determine what it needs to do.
            callback: fn (*Compaction) void,
        };

        const InputLevel = enum(u1) {
            a = 0,
            b = 1,
        };

        const CompactionWrite = struct { write: Grid.Write, compaction: *Compaction, block: BlockPtr, next: ?*CompactionWrite = null, };

        // Passed by `init`.
        tree_name: []const u8,

        // Allocated during `init`.
        iterator_a: TableDataIterator,
        iterator_b: LevelDataIterator,
        index_block_a: BlockPtr,
        data_blocks: [2]BlockPtr,
        table_builder: Table.Builder,
        last_keys_in: [2]?Key = .{ null, null },
        compaction_writes: []CompactionWrite,
        compaction_writes_fifo: FIFO(CompactionWrite) = .{ .name = "compaction_writes" },

        /// Manifest log appends are queued up until `finish()` is explicitly called to ensure
        /// they are applied deterministically relative to other concurrent compactions.
        manifest_entries: std.BoundedArray(struct {
            operation: enum {
                insert_to_level_b,
                update_in_level_a,
                update_in_level_b,
                move_to_level_b,
            },
            table: TableInfo,
        }, manifest_entries_max: {
            // Worst-case manifest updates:
            // See docs/internals/lsm.md "Compaction Table Overlap" for more detail.
            var count = 0;
            count += 1; // Update the input table from level A.
            count += constants.lsm_growth_factor; // Update the input tables from level B.
            count += constants.lsm_growth_factor + 1; // Insert the output tables to level B.
            // (In the move-table case, only a single TableInfo is inserted, and none are updated.)
            break :manifest_entries_max count;
        }) = .{ .buffer = undefined },

        // Passed by `start`.
        context: Context,

        /// The number of blocks, per beat, we need to process before calling our callback.
        target_blocks_written: usize = 0,
        data_blocks_written: usize = 0,

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
            next_tick,
            compacting,
            iterator_init_a,
            iterator_next: InputLevel,
            tables_writing_done,
            applied_to_manifest,
        },
        tables_writing: struct { pending: usize, want_finish: bool } = undefined,
        tables_writing_init: bool = false,
        next_tick: Grid.NextTick = undefined,
        read: Grid.Read = undefined,

        tracer_slot: ?tracer.SpanStart,
        iterator_tracer_slot: ?tracer.SpanStart,

        timer: std.time.Timer = undefined,

        pub fn init(allocator: Allocator, tree_name: []const u8) !Compaction {
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

            var compaction_writes = try allocator.alloc(CompactionWrite, 128);
            errdefer allocator.free(compaction_writes);

            const compaction_write_blocks = try allocator.alloc(BlockPtr, compaction_writes.len);
            errdefer allocator.free(compaction_write_blocks);

            for (compaction_write_blocks) |*compaction_write_block, i| {
                errdefer for (compaction_write_blocks[0..i]) |block| allocator.free(block);
                compaction_write_block.* = try alloc_block(allocator);
            }

            var compaction_writes_fifo: FIFO(CompactionWrite) = .{ .name = "compaction_writes" };
            for (compaction_writes) |*compaction_write, i| {
                compaction_write.next = null;
                compaction_write.block = compaction_write_blocks[i];
                compaction_writes_fifo.push(compaction_write);
            }

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
                .iterator_tracer_slot = null,
                .timer = std.time.Timer.start() catch @panic("foo"),
                .compaction_writes = compaction_writes,
                .compaction_writes_fifo = compaction_writes_fifo,
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
            assert(compaction.state == .applied_to_manifest);

            compaction.state = .idle;
            compaction.manifest_entries.len = 0;
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
            assert(compaction.manifest_entries.len == 0);

            // tracer.start(
            //     &compaction.tracer_slot,
            //     .{ .tree_compaction = .{
            //         .tree_name = compaction.tree_name,
            //         .level_b = context.level_b,
            //     } },
            //     @src(),
            // );

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

            // // TODO - this needs to account for data_block_count / how many input data blocks we have.
            // // Output of compaction can be worst case input + output - this is a giant hack
            // const blocks_required = (tree.table_immutable.values.len + tree.compaction_currently_stored) / Table.layout.block_value_count_max;
            // const ticks_available = 28;

            const max_blocks_total = context.range_b.table_count * Table.block_count_max;
            const ticks_available = 31; // TODO pull from constants
            const target_blocks_written = stdx.div_ceil(max_blocks_total, ticks_available);
            // std.log.debug("{s}: Max blocks: {}, Ticks: {}, To do this tick: {}", .{compaction.tree_name, max_blocks_total, ticks_available, target_blocks_written});
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
                .target_blocks_written = target_blocks_written,
                .compaction_writes = compaction.compaction_writes,
                .compaction_writes_fifo = compaction.compaction_writes_fifo,

                .tracer_slot = compaction.tracer_slot,
                .iterator_tracer_slot = compaction.iterator_tracer_slot,
                .timer = compaction.timer,
            };

            if (can_move_table) {
                // If we can just move the table, don't bother with compaction.

                log.debug(
                    "{s}: Moving table: level_b={}",
                    .{ compaction.tree_name, context.level_b },
                );

                const snapshot_max = snapshot_max_for_table_input(context.op_min);
                const table_a = &context.table_info_a.disk;
                assert(table_a.snapshot_max >= snapshot_max);

                compaction.manifest_entries.appendAssumeCapacity(.{
                    .operation = .move_to_level_b,
                    .table = table_a.*,
                });

                compaction.state = .next_tick;
                compaction.context.grid.on_next_tick(done_on_next_tick, &compaction.next_tick);
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
                });

                switch (context.table_info_a) {
                    .immutable => |values| {
                        compaction.values_in[0] = values;
                        compaction.loop_start();
                    },
                    .disk => |table_info| {
                        compaction.state = .iterator_init_a;
                        compaction.context.grid.read_block(
                            on_iterator_init_a,
                            &compaction.read,
                            table_info.address,
                            table_info.checksum,
                            .index,
                        );
                    },
                }
            }
        }

        fn on_iterator_init_a(read: *Grid.Read, index_block: BlockPtrConst) void {
            const compaction = @fieldParentPtr(Compaction, "read", read);
            assert(compaction.state == .iterator_init_a);

            // `index_block` is only valid for this callback, so copy its contents.
            // TODO(jamii) This copy can be avoided if we bypass the cache.
            stdx.copy_disjoint(.exact, u8, compaction.index_block_a, index_block);
            compaction.iterator_a.start(.{
                .grid = compaction.context.grid,
                .addresses = Table.index_data_addresses_used(compaction.index_block_a),
                .checksums = Table.index_data_checksums_used(compaction.index_block_a),
            });

            compaction.loop_start();
        }

        pub fn loop_start(compaction: *Compaction) void {
            compaction.state = .compacting;

            // tracer.start(
            //     &compaction.iterator_tracer_slot,
            //     .{ .tree_compaction_iter = .{
            //         .tree_name = compaction.tree_name,
            //         .level_b = compaction.context.level_b,
            //     } },
            //     @src(),
            // );

            compaction.iterator_check(.a);
        }

        /// If `values_in[index]` is empty and more values are available, read them.
        fn iterator_check(compaction: *Compaction, input_level: InputLevel) void {
            assert(compaction.state == .compacting);

            if (compaction.values_in[@enumToInt(input_level)].len > 0) {
                // Still have values on this input_level, no need to refill.
                compaction.iterator_check_finish(input_level);
            } else if (input_level == .a and compaction.context.table_info_a == .immutable) {
                // No iterator to call next on.
                compaction.iterator_check_finish(input_level);
            } else {
                compaction.state = .{ .iterator_next = input_level };
                switch (input_level) {
                    .a => compaction.iterator_a.next(iterator_next_a),
                    .b => compaction.iterator_b.next(.{
                        .on_index = on_index_block,
                        .on_data = iterator_next_b,
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
            assert(std.meta.eql(compaction.state, .{ .iterator_next = .b }));

            // Tables that we've compacted should become invisible at the end of this half-bar.
            compaction.manifest_entries.appendAssumeCapacity(.{
                .operation = .update_in_level_b,
                .table = table_info,
            });

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

        fn iterator_next_a(iterator_a: *TableDataIterator, data_block: ?BlockPtrConst) void {
            const compaction = @fieldParentPtr(Compaction, "iterator_a", iterator_a);
            assert(std.meta.eql(compaction.state, .{ .iterator_next = .a }));
            compaction.iterator_next(data_block);
        }

        fn iterator_next_b(iterator_b: *LevelDataIterator, data_block: ?BlockPtrConst) void {
            const compaction = @fieldParentPtr(Compaction, "iterator_b", iterator_b);
            assert(std.meta.eql(compaction.state, .{ .iterator_next = .b }));
            compaction.iterator_next(data_block);
        }

        fn iterator_next(compaction: *Compaction, data_block: ?BlockPtrConst) void {
            assert(compaction.state == .iterator_next);
            const input_level = compaction.state.iterator_next;
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
            compaction.iterator_check_finish(input_level);
        }

        fn iterator_check_finish(compaction: *Compaction, input_level: InputLevel) void {
            switch (input_level) {
                .a => compaction.iterator_check(.b),
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

            // compaction.timer.reset();
            // var mode: enum {skipped, copy, copy_drop_tombstones, merge} = undefined;

            if (values_in[0].len == 0 and values_in[1].len == 0) {
                compaction.input_state = .exhausted;
                // mode = .skipped;
            } else if (values_in[0].len == 0) {
                compaction.copy(.b);
                // mode = .copy;
            } else if (values_in[1].len == 0) {
                if (compaction.drop_tombstones) {
                    compaction.copy_drop_tombstones();
                    // mode = .copy_drop_tombstones;
                } else {
                    compaction.copy(.a);
                    // mode = .copy;
                }
            } else {
                compaction.merge();
                // mode = .merge;
            }

            // const time = compaction.timer.read();
            // std.log.debug("{s}: Took {}ns to generate a data block by {}", .{compaction.tree_name, time, mode});

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

            if (!compaction.tables_writing_init) {
                compaction.tables_writing = .{ .pending = 0, .want_finish = false };
                compaction.tables_writing_init = true;
            }

            // Flush the data block if needed.
            if (table_builder.data_block_full() or
                // If the filter or index blocks need to be flushed,
                // the data block has to be flushed first.
                table_builder.filter_block_full() or
                table_builder.index_block_full() or
                // If the input is exhausted then we need to flush all blocks before finishing.
                (input_exhausted and !table_builder.data_block_empty()))
            {
                const addr = compaction.context.grid.acquire(compaction.grid_reservation.?);
                table_builder.data_block_finish(.{
                    .cluster = compaction.context.grid.superblock.working.cluster,
                    .address = addr,
                });
                std.log.debug("{s}: Prewrite, should be in addr: {}", .{ compaction.tree_name, addr });
                WriteBlock(.data).write_block(compaction);
                compaction.data_blocks_written += 1;
            }

            // Flush the filter block if needed.
            if (table_builder.filter_block_full() or
                // If the index block need to be flushed,
                // the filter block has to be flushed first.
                table_builder.index_block_full() or
                // If the input is exhausted then we need to flush all blocks before finishing.
                (input_exhausted and !table_builder.filter_block_empty()))
            {
                const addr = compaction.context.grid.acquire(compaction.grid_reservation.?);
                table_builder.filter_block_finish(.{
                    .cluster = compaction.context.grid.superblock.working.cluster,
                    .address = addr,
                });
                std.log.debug("{s}: Prewrite, should be in addr: {}", .{ compaction.tree_name, addr });
                WriteBlock(.filter).write_block(compaction);
            }

            // Flush the index block if needed.
            if (table_builder.index_block_full() or
                // If the input is exhausted then we need to flush all blocks before finishing.
                (input_exhausted and !table_builder.index_block_empty()))
            {
                const addr = compaction.context.grid.acquire(compaction.grid_reservation.?);
                const table = table_builder.index_block_finish(.{
                    .cluster = compaction.context.grid.superblock.working.cluster,
                    .address = addr,
                    .snapshot_min = snapshot_min_for_table_output(compaction.context.op_min),
                });
                // Make this table visible at the end of this half-bar.
                compaction.manifest_entries.appendAssumeCapacity(.{
                    .operation = .insert_to_level_b,
                    .table = table,
                });
                std.log.debug("{s}: Prewrite, should be in addr: {}", .{ compaction.tree_name, addr });
                WriteBlock(.index).write_block(compaction);
            }

            compaction.write_finish();
        }

        const WriteBlockField = enum { data, filter, index };
        fn WriteBlock(comptime write_block_field: WriteBlockField) type {
            return struct {
                fn write_block(compaction: *Compaction) void {

                    const block = switch (write_block_field) {
                        .data => compaction.table_builder.data_block,
                        .filter => compaction.table_builder.filter_block,
                        .index => compaction.table_builder.index_block,
                    };
                    const compaction_write = compaction.compaction_writes_fifo.pop().?;
                    stdx.copy_disjoint(.exact, u8, compaction_write.block, block);
                    compaction_write.compaction = compaction;
                    // Could just use fifo count
                    compaction.tables_writing.pending += 1;
                    // std.log.info("{s}: On writing, just wrote {}, pending is {}...", .{ compaction.tree_name, Table.block_address(compaction_write.block), compaction.tables_writing.pending });
                    compaction.context.grid.write_block(
                        on_write,
                        &compaction_write.write,
                        &compaction_write.block,
                        Table.block_address(compaction_write.block),
                    );
                }

                fn on_write(write: *Grid.Write) void {
                    const compaction_write = @fieldParentPtr(CompactionWrite, "write", write);
                    const compaction = compaction_write.compaction;
                    compaction.tables_writing.pending -= 1;
                    compaction.compaction_writes_fifo.push(compaction_write);
                    // std.log.info("{s}: On write callback, just wrote {}, pending is {}...", .{ compaction.tree_name, write.address, compaction.tables_writing.pending });

                    if (compaction.tables_writing.pending == 0 and compaction.tables_writing.want_finish) {
                        compaction.state = .next_tick;
                        compaction.context.grid.on_next_tick(done_on_next_tick, &compaction.next_tick);
                    }
                }
            };
        }

        fn write_finish(compaction: *Compaction) void {
            // assert(compaction.state == .tables_writing);

            // tracer.end(
            //     &compaction.iterator_tracer_slot,
            //     .{ .tree_compaction_iter = .{
            //         .tree_name = compaction.tree_name,
            //         .level_b = compaction.context.level_b,
            //     } },
            // );

            switch (compaction.input_state) {
                .remaining => {
                    compaction.state = .next_tick;
                    compaction.context.grid.on_next_tick(loop_on_next_tick, &compaction.next_tick);
                },
                .exhausted => {
                    // Mark the level_a table as invisible if it was provided;
                    // it has been merged into level_b.
                    // TODO: Release the grid blocks associated with level_a as well
                    switch (compaction.context.table_info_a) {
                        .immutable => {},
                        .disk => |table| compaction.manifest_entries.appendAssumeCapacity(.{
                            .operation = .update_in_level_a,
                            .table = table,
                        }),
                    }

                    if (compaction.tables_writing.pending > 0) {
                        compaction.tables_writing.want_finish = true;
                    } else {
                        compaction.state = .next_tick;
                        compaction.context.grid.on_next_tick(done_on_next_tick, &compaction.next_tick);
                    }
                },
            }
        }

        fn loop_on_next_tick(next_tick: *Grid.NextTick) void {
            const compaction = @fieldParentPtr(Compaction, "next_tick", next_tick);
            assert(compaction.state == .next_tick);
            assert(compaction.input_state == .remaining);

            if (compaction.data_blocks_written == compaction.target_blocks_written) {
                const callback = compaction.context.callback;
                std.log.info("{s}: Calling callback since we've done our blocks_outstanding work", .{compaction.tree_name});
                callback(compaction);
            } else {
                std.log.info("{s}: data_blocks_written is {}, target_blocks_written is {}", .{compaction.tree_name, compaction.data_blocks_written, compaction.target_blocks_written});
                compaction.loop_start();
            }
        }

        fn done_on_next_tick(next_tick: *Grid.NextTick) void {
            const compaction = @fieldParentPtr(Compaction, "next_tick", next_tick);
            assert(compaction.state == .next_tick);

            const callback = compaction.context.callback;
            callback(compaction);

            compaction.tables_writing_init = false;
            compaction.state = .tables_writing_done;

            // tracer.end(
            //     &compaction.tracer_slot,
            //     .{ .tree_compaction = .{
            //         .tree_name = compaction.tree_name,
            //         .level_b = compaction.context.level_b,
            //     } },
            // );

            // std.log.debug("{s}: Calling callback since we've exhausted all work", .{compaction.tree_name});
        }

        pub fn apply_to_manifest(compaction: *Compaction) void {
            assert(compaction.state == .tables_writing_done);

            compaction.state = .applied_to_manifest;

            // Each compaction's manifest (log) updates are deferred to the end of the last
            // half-beat to ensure they are ordered deterministically relative to one
            // another.
            // TODO: If compaction is sequential, deferring manifest updates is unnecessary.
            const manifest = &compaction.context.tree.manifest;
            const level_b = compaction.context.level_b;
            const snapshot_max = snapshot_max_for_table_input(compaction.context.op_min);
            for (compaction.manifest_entries.slice()) |*entry| {
                switch (entry.operation) {
                    .insert_to_level_b => manifest.insert_table(level_b, &entry.table),
                    .update_in_level_a => manifest.update_table(level_b - 1, snapshot_max, &entry.table),
                    .update_in_level_b => manifest.update_table(level_b, snapshot_max, &entry.table),
                    .move_to_level_b => manifest.move_table(level_b - 1, level_b, &entry.table),
                }
            }
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
