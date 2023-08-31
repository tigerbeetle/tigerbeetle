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

const stdx = @import("../stdx.zig");
const GridType = @import("../vsr/grid.zig").GridType;
const allocate_block = @import("../vsr/grid.zig").allocate_block;
const TableInfoType = @import("manifest.zig").TableInfoType;
const ManifestType = @import("manifest.zig").ManifestType;
const schema = @import("schema.zig");
const TableDataIteratorType = @import("table_data_iterator.zig").TableDataIteratorType;
const LevelTableValueBlockIteratorType = @import("level_data_iterator.zig").LevelTableValueBlockIteratorType;

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
        const LevelTableValueBlockIterator = LevelTableValueBlockIteratorType(Table, Storage);

        const Key = Table.Key;
        const Value = Table.Value;
        const compare_keys = Table.compare_keys;
        const key_from_value = Table.key_from_value;
        const tombstone = Table.tombstone;

        const TableInfoReference = Manifest.TableInfoReference;

        pub const TableInfoA = union(enum) {
            immutable: []const Value,
            disk: TableInfoReference,
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
            callback: *const fn (*Compaction) void,
        };

        const InputLevel = enum(u1) {
            a = 0,
            b = 1,
        };

        // Passed by `init`.
        tree_config: Tree.Config,

        // Allocated during `init`.
        iterator_a: TableDataIterator,
        iterator_b: LevelTableValueBlockIterator,
        table_builder: Table.Builder,
        index_block_a: BlockPtr,
        index_block_b: BlockPtr,
        data_blocks: [2]BlockPtr,
        last_keys_in: [2]?Key = .{ null, null },

        /// Manifest log appends are queued up until `finish()` is explicitly called to ensure
        /// they are applied deterministically relative to other concurrent compactions.
        manifest_entries: stdx.BoundedArray(struct {
            operation: enum {
                insert_to_level_b,
                move_to_level_b,
            },
            table: TableInfo,
        }, manifest_entries_max: {
            // Worst-case manifest updates:
            // See docs/internals/lsm.md "Compaction Table Overlap" for more detail.
            var count = 0;
            count += constants.lsm_growth_factor + 1; // Insert the output tables to level B.
            // (In the move-table case, only a single TableInfo is inserted, and none are updated.)
            break :manifest_entries_max count;
        }) = .{},

        // Passed by `start`.
        context: Context,

        // Whether this compaction will use the move-table optimization.
        // Specifically, this field is set to True if the optimal compaction
        // table in level A can simply be moved to level B.
        move_table: bool,
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
            tables_writing: struct { pending: usize },
            tables_writing_done,
            applied_to_manifest,
        },

        next_tick: Grid.NextTick = undefined,
        read: Grid.Read = undefined,
        write_data_block: Grid.Write = undefined,
        write_filter_block: Grid.Write = undefined,
        write_index_block: Grid.Write = undefined,

        tracer_slot: ?tracer.SpanStart,
        iterator_tracer_slot: ?tracer.SpanStart,

        pub fn init(allocator: Allocator, tree_config: Tree.Config) !Compaction {
            var iterator_a = TableDataIterator.init();
            errdefer iterator_a.deinit();

            var iterator_b = LevelTableValueBlockIterator.init();
            errdefer iterator_b.deinit();

            const index_block_a = try allocate_block(allocator);
            errdefer allocator.free(index_block_a);

            const index_block_b = try allocate_block(allocator);
            errdefer allocator.free(index_block_b);

            var data_blocks: [2]Grid.BlockPtr = undefined;

            data_blocks[0] = try allocate_block(allocator);
            errdefer allocator.free(data_blocks[0]);

            data_blocks[1] = try allocate_block(allocator);
            errdefer allocator.free(data_blocks[1]);

            var table_builder = try Table.Builder.init(allocator);
            errdefer table_builder.deinit(allocator);

            return Compaction{
                .tree_config = tree_config,

                .iterator_a = iterator_a,
                .iterator_b = iterator_b,
                .index_block_a = index_block_a,
                .index_block_b = index_block_b,
                .data_blocks = data_blocks,
                .table_builder = table_builder,

                .context = undefined,
                .grid_reservation = null,
                .drop_tombstones = undefined,

                .values_in = .{ &.{}, &.{} },

                .input_state = .remaining,
                .state = .idle,
                .move_table = undefined,
                .tracer_slot = null,
                .iterator_tracer_slot = null,
            };
        }

        pub fn deinit(compaction: *Compaction, allocator: Allocator) void {
            compaction.table_builder.deinit(allocator);
            for (compaction.data_blocks) |data_block| allocator.free(data_block);
            allocator.free(compaction.index_block_a);
            allocator.free(compaction.index_block_b);
            compaction.iterator_b.deinit();
            compaction.iterator_a.deinit();
        }

        pub fn reset(compaction: *Compaction) void {
            compaction.* = .{
                .tree_config = compaction.tree_config,

                .iterator_a = compaction.iterator_a,
                .iterator_b = compaction.iterator_b,
                .index_block_a = compaction.index_block_a,
                .index_block_b = compaction.index_block_b,
                .data_blocks = compaction.data_blocks,
                .table_builder = compaction.table_builder,

                .context = undefined,
                .move_table = undefined,
                .grid_reservation = null,
                .drop_tombstones = undefined,

                .values_in = .{ &.{}, &.{} },

                .input_state = .remaining,
                .state = .idle,

                .tracer_slot = null,
                .iterator_tracer_slot = null,
            };

            compaction.iterator_a.reset();
            compaction.iterator_b.reset();
            compaction.table_builder.reset();

            // Zero the blocks because allocate_block() returns a zeroed block.
            @memset(compaction.index_block_a, 0);
            @memset(compaction.index_block_b, 0);
            for (compaction.data_blocks) |data_block| {
                @memset(data_block, 0);
            }
        }

        pub fn transition_to_idle(compaction: *Compaction) void {
            assert(compaction.state == .applied_to_manifest);

            compaction.state = .idle;
            compaction.manifest_entries.clear();
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
            assert(compaction.manifest_entries.empty());

            tracer.start(
                &compaction.tracer_slot,
                .{ .tree_compaction = .{
                    .tree_name = compaction.tree_config.name,
                    .level_b = context.level_b,
                } },
                @src(),
            );

            const move_table =
                context.table_info_a == .disk and
                context.range_b.tables.empty();

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
            const grid_reservation = if (move_table)
                null
            else
                // +1 to count the input table from level A.
                context.grid.reserve(
                    (context.range_b.tables.count() + 1) * Table.block_count_max,
                ).?;

            // Levels may choose to drop tombstones if keys aren't included in the lower levels.
            // This invariant is always true for the last level as it doesn't have any lower ones.
            const drop_tombstones = context.tree.manifest.compaction_must_drop_tombstones(
                context.level_b,
                context.range_b,
            );
            assert(drop_tombstones or context.level_b < constants.lsm_levels - 1);

            compaction.* = .{
                .tree_config = compaction.tree_config,

                .iterator_a = compaction.iterator_a,
                .iterator_b = compaction.iterator_b,
                .index_block_a = compaction.index_block_a,
                .index_block_b = compaction.index_block_b,
                .data_blocks = compaction.data_blocks,
                .table_builder = compaction.table_builder,

                .context = context,

                .values_in = .{ &.{}, &.{} },

                .grid_reservation = grid_reservation,
                .drop_tombstones = drop_tombstones,
                .input_state = .remaining,
                .state = .compacting,

                .tracer_slot = compaction.tracer_slot,
                .iterator_tracer_slot = compaction.iterator_tracer_slot,
                .move_table = move_table,
            };

            if (move_table) {
                // If we can just move the table, don't bother with compaction.

                log.debug(
                    "{s}: Moving table: level_b={}",
                    .{ compaction.tree_config.name, context.level_b },
                );

                const snapshot_max = snapshot_max_for_table_input(context.op_min);
                const table_a = context.table_info_a.disk.table_info;
                assert(table_a.snapshot_max >= snapshot_max);

                compaction.manifest_entries.append_assume_capacity(.{
                    .operation = .move_to_level_b,
                    .table = table_a.*,
                });

                compaction.state = .next_tick;
                compaction.context.grid.on_next_tick(done_on_next_tick, &compaction.next_tick);
            } else {
                // Otherwise, start merging.

                log.debug(
                    "{s}: Merging table: level_b={}",
                    .{ compaction.tree_config.name, context.level_b },
                );

                compaction.iterator_b.start(.{
                    .grid = context.grid,
                    .level = context.level_b,
                    .snapshot = context.op_min,
                    .tables = compaction.context.range_b.tables.const_slice(),
                    .index_block = compaction.index_block_b,
                    .direction = .ascending,
                });

                switch (context.table_info_a) {
                    .immutable => |values| {
                        compaction.values_in[0] = values;
                        compaction.loop_start();
                    },
                    .disk => |table_ref| {
                        compaction.state = .iterator_init_a;
                        compaction.context.grid.read_block(
                            .{ .from_local_or_global_storage = on_iterator_init_a },
                            &compaction.read,
                            table_ref.table_info.address,
                            table_ref.table_info.checksum,
                            .{ .cache_read = true, .cache_write = true },
                        );
                    },
                }
            }
        }

        fn on_iterator_init_a(read: *Grid.Read, index_block: BlockPtrConst) void {
            const compaction = @fieldParentPtr(Compaction, "read", read);
            assert(compaction.state == .iterator_init_a);
            assert(compaction.tree_config.id == schema.TableIndex.tree_id(index_block));

            // `index_block` is only valid for this callback, so copy its contents.
            // TODO(jamii) This copy can be avoided if we bypass the cache.
            stdx.copy_disjoint(.exact, u8, compaction.index_block_a, index_block);

            const index_schema_a = schema.TableIndex.from(compaction.index_block_a);
            compaction.iterator_a.start(.{
                .grid = compaction.context.grid,
                .addresses = index_schema_a.data_addresses_used(compaction.index_block_a),
                .checksums = index_schema_a.data_checksums_used(compaction.index_block_a),
                .direction = .ascending,
            });
            compaction.release_table_blocks(compaction.index_block_a);
            compaction.state = .compacting;
            compaction.loop_start();
        }

        fn loop_start(compaction: *Compaction) void {
            assert(compaction.state == .compacting);

            tracer.start(
                &compaction.iterator_tracer_slot,
                .{ .tree_compaction_iter = .{
                    .tree_name = compaction.tree_config.name,
                    .level_b = compaction.context.level_b,
                } },
                @src(),
            );

            compaction.iterator_check(.a);
        }

        /// If `values_in[index]` is empty and more values are available, read them.
        fn iterator_check(compaction: *Compaction, input_level: InputLevel) void {
            assert(compaction.state == .compacting);

            if (compaction.values_in[@intFromEnum(input_level)].len > 0) {
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

        fn on_index_block(iterator_b: *LevelTableValueBlockIterator) void {
            const compaction = @fieldParentPtr(Compaction, "iterator_b", iterator_b);
            assert(std.meta.eql(compaction.state, .{ .iterator_next = .b }));
            compaction.release_table_blocks(compaction.index_block_b);
        }

        // TODO: Support for LSM snapshots would require us to only remove blocks
        // that are invisible.
        fn release_table_blocks(compaction: *Compaction, index_block: BlockPtrConst) void {
            // Release the table's block addresses in the Grid as it will be made invisible.
            // This is safe; compaction.index_block_b holds a copy of the index block for a
            // table in Level B. Additionally, compaction.index_block_a holds
            // a copy of the index block for the Level A table being compacted.

            const grid = compaction.context.grid;
            const index_schema = schema.TableIndex.from(index_block);
            for (index_schema.data_addresses_used(index_block)) |address| grid.release(address);
            for (index_schema.filter_addresses_used(index_block)) |address| grid.release(address);
            grid.release(Table.block_address(index_block));
        }

        fn iterator_next_a(iterator_a: *TableDataIterator, data_block: ?BlockPtrConst) void {
            const compaction = @fieldParentPtr(Compaction, "iterator_a", iterator_a);
            assert(std.meta.eql(compaction.state, .{ .iterator_next = .a }));
            compaction.iterator_next(data_block);
        }

        fn iterator_next_b(iterator_b: *LevelTableValueBlockIterator, data_block: ?BlockPtrConst) void {
            const compaction = @fieldParentPtr(Compaction, "iterator_b", iterator_b);
            assert(std.meta.eql(compaction.state, .{ .iterator_next = .b }));
            compaction.iterator_next(data_block);
        }

        fn iterator_next(compaction: *Compaction, data_block: ?BlockPtrConst) void {
            assert(compaction.state == .iterator_next);
            const input_level = compaction.state.iterator_next;
            const index = @intFromEnum(input_level);

            if (data_block) |block| {
                // `data_block` is only valid for this callback, so copy its contents.
                // TODO(jamii) This copy can be avoided if we bypass the cache.
                stdx.copy_disjoint(.exact, u8, compaction.data_blocks[index], block);
                compaction.values_in[index] =
                    Table.data_block_values_used(compaction.data_blocks[index]);

                // Assert that we're reading data blocks in key order.
                const values_in = compaction.values_in[index];
                assert(values_in.len > 0);
                if (constants.verify) {
                    for (values_in[0 .. values_in.len - 1], values_in[1..]) |*value, *value_next| {
                        assert(compare_keys(key_from_value(value), key_from_value(value_next)) == .lt);
                    }
                }
                const first_key = key_from_value(&values_in[0]);
                const last_key = key_from_value(&values_in[values_in.len - 1]);
                if (compaction.last_keys_in[index]) |last_key_prev| {
                    assert(compare_keys(last_key_prev, first_key) == .lt);
                }
                if (values_in.len > 1) {
                    assert(compare_keys(first_key, last_key) == .lt);
                }
                compaction.last_keys_in[index] = last_key;
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
                    .tree_name = compaction.tree_config.name,
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
                    .tree_name = compaction.tree_config.name,
                    .level_b = compaction.context.level_b,
                } },
            );

            compaction.write_blocks();
        }

        fn copy(compaction: *Compaction, input_level: InputLevel) void {
            assert(compaction.state == .compacting);
            assert(compaction.values_in[@intFromEnum(input_level) +% 1].len == 0);
            assert(compaction.table_builder.value_count < Table.layout.block_value_count_max);

            const values_in = compaction.values_in[@intFromEnum(input_level)];
            const values_out = compaction.table_builder.data_block_values();
            var values_out_index = compaction.table_builder.value_count;

            assert(values_in.len > 0);

            const len = @min(values_in.len, values_out.len - values_out_index);
            assert(len > 0);
            stdx.copy_disjoint(
                .exact,
                Value,
                values_out[values_out_index..][0..len],
                values_in[0..len],
            );

            compaction.values_in[@intFromEnum(input_level)] = values_in[len..];
            compaction.table_builder.value_count += @as(u32, @intCast(len));
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

            compaction.state = .{ .tables_writing = .{ .pending = 0 } };

            // Flush the data block if needed.
            if (table_builder.data_block_full() or
                // If the filter or index blocks need to be flushed,
                // the data block has to be flushed first.
                table_builder.filter_block_full() or
                table_builder.index_block_full() or
                // If the input is exhausted then we need to flush all blocks before finishing.
                (input_exhausted and !table_builder.data_block_empty()))
            {
                table_builder.data_block_finish(.{
                    .cluster = compaction.context.grid.superblock.working.cluster,
                    .address = compaction.context.grid.acquire(compaction.grid_reservation.?),
                    .snapshot_min = snapshot_min_for_table_output(compaction.context.op_min),
                    .tree_id = compaction.tree_config.id,
                });
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
                table_builder.filter_block_finish(.{
                    .cluster = compaction.context.grid.superblock.working.cluster,
                    .address = compaction.context.grid.acquire(compaction.grid_reservation.?),
                    .snapshot_min = snapshot_min_for_table_output(compaction.context.op_min),
                    .tree_id = compaction.tree_config.id,
                });
                WriteBlock(.filter).write_block(compaction);
            }

            // Flush the index block if needed.
            if (table_builder.index_block_full() or
                // If the input is exhausted then we need to flush all blocks before finishing.
                (input_exhausted and !table_builder.index_block_empty()))
            {
                const table = table_builder.index_block_finish(.{
                    .cluster = compaction.context.grid.superblock.working.cluster,
                    .address = compaction.context.grid.acquire(compaction.grid_reservation.?),
                    .snapshot_min = snapshot_min_for_table_output(compaction.context.op_min),
                    .tree_id = compaction.tree_config.id,
                });
                // Make this table visible at the end of this half-bar.
                compaction.manifest_entries.append_assume_capacity(.{
                    .operation = .insert_to_level_b,
                    .table = table,
                });
                WriteBlock(.index).write_block(compaction);
            }

            if (compaction.state.tables_writing.pending == 0) {
                compaction.write_finish();
            }
        }

        const WriteBlockField = enum { data, filter, index };
        fn WriteBlock(comptime write_block_field: WriteBlockField) type {
            return struct {
                fn write_block(compaction: *Compaction) void {
                    assert(compaction.state == .tables_writing);

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
                    compaction.state.tables_writing.pending += 1;
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
                    assert(compaction.state == .tables_writing);
                    compaction.state.tables_writing.pending -= 1;
                    if (compaction.state.tables_writing.pending == 0) {
                        compaction.write_finish();
                    }
                }
            };
        }

        fn write_finish(compaction: *Compaction) void {
            assert(compaction.state == .tables_writing);
            assert(compaction.state.tables_writing.pending == 0);

            tracer.end(
                &compaction.iterator_tracer_slot,
                .{ .tree_compaction_iter = .{
                    .tree_name = compaction.tree_config.name,
                    .level_b = compaction.context.level_b,
                } },
            );

            switch (compaction.input_state) {
                .remaining => {
                    compaction.state = .next_tick;
                    compaction.context.grid.on_next_tick(loop_on_next_tick, &compaction.next_tick);
                },
                .exhausted => {
                    compaction.state = .next_tick;
                    compaction.context.grid.on_next_tick(done_on_next_tick, &compaction.next_tick);
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

            compaction.state = .tables_writing_done;

            tracer.end(
                &compaction.tracer_slot,
                .{ .tree_compaction = .{
                    .tree_name = compaction.tree_config.name,
                    .level_b = compaction.context.level_b,
                } },
            );

            const callback = compaction.context.callback;
            callback(compaction);
        }

        pub fn apply_to_manifest(compaction: *Compaction) void {
            assert(compaction.state == .tables_writing_done);
            compaction.state = .applied_to_manifest;

            // Each compaction's manifest updates are deferred to the end of the last
            // half-beat to ensure:
            // - manifest log updates are ordered deterministically relative to one another, and
            // - manifest updates are not visible until after the blocks are all on disk.
            const manifest = &compaction.context.tree.manifest;
            const level_b = compaction.context.level_b;
            const snapshot_max = snapshot_max_for_table_input(compaction.context.op_min);

            if (compaction.move_table) {
                // If no compaction is required, don't update snapshot_max.
            } else {
                // These updates MUST precede insert_table() and move_table() since they use
                // references to modify the ManifestLevel in-place.
                switch (compaction.context.table_info_a) {
                    .immutable => {},
                    .disk => |table_info| {
                        manifest.update_table(level_b - 1, snapshot_max, table_info);
                    },
                }
                for (compaction.context.range_b.tables.const_slice()) |table| {
                    manifest.update_table(level_b, snapshot_max, table);
                }
            }

            for (compaction.manifest_entries.slice()) |*entry| {
                switch (entry.operation) {
                    .insert_to_level_b => manifest.insert_table(level_b, &entry.table),
                    .move_to_level_b => manifest.move_table(level_b - 1, level_b, &entry.table),
                }
            }
        }
    };
}

fn snapshot_max_for_table_input(op_min: u64) u64 {
    return snapshot_min_for_table_output(op_min) - 1;
}

pub fn snapshot_min_for_table_output(op_min: u64) u64 {
    assert(op_min > 0);
    assert(op_min % @divExact(constants.lsm_batch_multiple, 2) == 0);
    return op_min + @divExact(constants.lsm_batch_multiple, 2);
}
