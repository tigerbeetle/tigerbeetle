//! Compaction moves or merges a table's values from the previous level.
//!
//! Each Compaction is paced to run in an arbitrary amount of beats, by the forest.
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
//! then the tombstone is omitted from the compacted output, see: `compaction_must_drop_tombstones`.
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
const FIFO = @import("../fifo.zig").FIFO;
const GridType = @import("../vsr/grid.zig").GridType;
const BlockPtr = @import("../vsr/grid.zig").BlockPtr;
const BlockPtrConst = @import("../vsr/grid.zig").BlockPtrConst;
const allocate_block = @import("../vsr/grid.zig").allocate_block;
const TableInfoType = @import("manifest.zig").TreeTableInfoType;
const ManifestType = @import("manifest.zig").ManifestType;
const schema = @import("schema.zig");

/// The upper-bound count of input tables to a single tree's compaction.
///
/// - +1 from level A.
/// - +lsm_growth_factor from level B. The A-input table cannot overlap with an extra B-input table
///   because input table selection is least-overlap. If the input table overlaps on one or both
///   edges, there must be another table with less overlap to select.
pub const compaction_tables_input_max = 1 + constants.lsm_growth_factor;

/// The upper-bound count of output tables from a single tree's compaction.
/// In the "worst" case, no keys are overwritten/merged, and no tombstones are dropped.
pub const compaction_tables_output_max = compaction_tables_input_max;

const half_bar_beat_count = @divExact(constants.lsm_batch_multiple, 2);

/// Information used when scheduling compactions. Kept unspecialized to make the forest
/// code easier.
pub const CompactionInfo = struct {
    /// How many values, across all input tables, need to be processed.
    /// This is the total – it is fixed for the duration of the compaction.
    compaction_tables_value_count: usize,

    // Keys are integers in TigerBeetle, with a maximum size of u256. Store these
    // here, instead of Key, to keep this unspecialized.
    target_key_min: u256,
    target_key_max: u256,

    /// Are we doing a move_table? In which case, certain things like grid reservation
    /// must be skipped by the caller.
    move_table: bool,

    tree_id: u16,
    level_b: u8,
};

/// A bar is exhausted when all input values have been merged.
/// A beat is exhausted when all of the input values scheduled for this beat (rounded up to the
/// nearest full data block) have been merged.
///
/// Invariant: If `bar` is exhausted, then `beat` is always exhausted.
pub const Exhausted = struct { bar: bool, beat: bool };
const BlipCallback = *const fn (*anyopaque, ?Exhausted) void;
pub const BlipStage = enum { read, merge, write, drained };

// The following types need to specialize on Grid, but are used both by CompactionType and the
// forest.
pub fn CompactionHelperType(comptime Grid: type) type {
    return struct {
        pub const CompactionBlocks = struct {
            // Index blocks are global, and shared between blips. The index reads happen
            // as a mini-stage before reads kick off.
            source_index_block_a: *CompactionBlock,
            source_index_block_b: *CompactionBlock,

            /// For each source level, we have a buffer of CompactionBlocks.
            source_value_blocks: [2]BlockFIFO,

            /// We only have one buffer of output CompactionBlocks.
            target_value_blocks: BlockFIFO,
        };

        pub const CompactionBlock = struct {
            block: BlockPtr,

            // CompactionBlocks are stored in buffers where we need a pointer to get back to our
            // parent.
            target: ?*anyopaque = null,

            // TODO: This could be a union to save a bit of memory and add a bit of safety.
            read: Grid.Read = undefined,
            write: Grid.Write = undefined,

            stage: enum { free, pending, ready, ioing, standalone } = .free,
            next: ?*CompactionBlock = null,
        };

        pub const CompactionBlockFIFO = FIFO(CompactionBlock);

        pub const BlockFIFO = struct {
            /// Invariant: a CompactionBlock resides in the FIFO corresponding to its `stage`.
            free: CompactionBlockFIFO,
            pending: CompactionBlockFIFO,
            ready: CompactionBlockFIFO,
            ioing: CompactionBlockFIFO,

            /// The (constant) total number of blocks in all four FIFOs.
            count: usize,

            /// All blocks start in free.
            pub fn init(block_pool: *CompactionBlockFIFO, count: usize) BlockFIFO {
                assert(count > 0);
                assert(count % 2 == 0);
                assert(block_pool.count >= count);

                var free: CompactionBlockFIFO = .{
                    .name = "free",
                    .verify_push = false,
                };

                for (0..count) |_| {
                    const block = block_pool.pop().?;
                    assert(block.stage == .free);
                    free.push(block);
                }

                return .{
                    .free = free,
                    .pending = .{ .name = "pending", .verify_push = false },
                    .ready = .{ .name = "ready", .verify_push = false },
                    .ioing = .{ .name = "ioing", .verify_push = false },
                    .count = count,
                };
            }

            /// Return all the blocks to the pool they came from.
            pub fn deinit(self: *BlockFIFO, block_pool: *CompactionBlockFIFO) void {
                assert(self.free.count == self.count);
                assert(self.pending.count == 0);
                assert(self.ready.count == 0);
                assert(self.ioing.count == 0);

                const block_pool_count_start = block_pool.count;
                defer assert(block_pool.count - block_pool_count_start == self.count);

                while (self.free.pop()) |block| {
                    block_pool.push(block);
                }
            }

            pub fn ready_peek(self: *BlockFIFO) ?*CompactionBlock {
                const value = self.ready.peek() orelse return null;
                assert(value.stage == .ready);
                return value;
            }

            pub fn free_to_pending(self: *BlockFIFO) ?*CompactionBlock {
                const value = self.free.pop() orelse return null;
                assert(value.stage == .free);
                value.stage = .pending;
                self.pending.push(value);

                return value;
            }

            pub fn pending_to_ready(self: *BlockFIFO) ?*CompactionBlock {
                const value = self.pending.pop() orelse return null;
                assert(value.stage == .pending);
                value.stage = .ready;
                self.ready.push(value);

                return value;
            }

            pub fn pending_to_free(self: *BlockFIFO) ?*CompactionBlock {
                const value = self.pending.pop() orelse return null;
                assert(value.stage == .pending);
                value.stage = .free;
                self.free.push(value);

                return value;
            }

            pub fn ready_to_ioing(self: *BlockFIFO) ?*CompactionBlock {
                const value = self.ready.pop() orelse return null;
                assert(value.stage == .ready);
                value.stage = .ioing;
                self.ioing.push(value);

                return value;
            }

            pub fn ready_to_free(self: *BlockFIFO) ?*CompactionBlock {
                const value = self.ready.pop() orelse return null;
                assert(value.stage == .ready);
                value.stage = .free;
                self.free.push(value);

                return value;
            }

            pub fn ioing_to_free(self: *BlockFIFO) ?*CompactionBlock {
                const value = self.ioing.pop() orelse return null;
                assert(value.stage == .ioing);
                value.stage = .free;
                self.free.push(value);

                return value;
            }
        };
    };
}

pub fn CompactionType(
    comptime Table: type,
    comptime Tree: type,
    comptime Storage: type,
) type {
    return struct {
        const Helpers = CompactionHelperType(Grid);
        const Compaction = @This();

        const Grid = GridType(Storage);
        pub const Tree_ = Tree;

        const Manifest = ManifestType(Table, Storage);
        const TableInfo = TableInfoType(Table);
        const TableInfoReference = Manifest.TableInfoReference;
        const CompactionRange = Manifest.CompactionRange;

        const Key = Table.Key;
        const Value = Table.Value;
        const key_from_value = Table.key_from_value;
        const tombstone = Table.tombstone;

        const TableInfoA = union(enum) {
            immutable: []Value,
            disk: TableInfoReference,
        };

        const Position = struct {
            index_block: usize = 0,
            value_block: usize = 0,
            value_block_index: usize = 0,

            pub fn format(
                self: @This(),
                comptime _: []const u8,
                _: std.fmt.FormatOptions,
                writer: anytype,
            ) !void {
                return writer.print("Position{{ .index_block = {}, " ++
                    ".value_block = {}, .value_block_index = {} }}", .{
                    self.index_block,
                    self.value_block,
                    self.value_block_index,
                });
            }
        };

        const Bar = struct {
            tree: *Tree,

            /// `op_min` is the first op/beat of this compaction's half-bar.
            /// `op_min` is used as a snapshot — the compaction's input tables must be visible
            /// to `op_min`.
            ///
            /// After this compaction finishes:
            /// - `op_min + half_bar_beat_count - 1` will be the input tables' snapshot_max.
            /// - `op_min + half_bar_beat_count` will be the output tables' snapshot_min.
            op_min: u64,

            /// Whether this compaction will use the move-table optimization.
            /// Specifically, this field is set to True if the optimal compaction
            /// table in level A can simply be moved to level B.
            move_table: bool,

            table_info_a: TableInfoA,
            range_b: CompactionRange,

            /// Levels may choose to drop tombstones if keys aren't included in the lower levels.
            /// This invariant is always true for the last level as it doesn't have any lower ones.
            drop_tombstones: bool,
            /// Number of beats we should aim to finish this compaction in. It might be fewer, but
            /// it'll never be more.
            beats_max: ?u64,
            beats_finished: u64 = 0,
            /// The total number of source values for this compaction.
            /// This is fixed for the duration of the compaction.
            compaction_tables_value_count: u64,

            // The total number of source values processed by this compaction across the bar. Must
            // equal compaction_tables_value_count by bar_apply_to_manifest(). Tracked
            // independently by both the read and merge stages to ensure no values are dropped.
            source_values_read_count: u64 = 0,
            source_values_merge_count: u64 = 0,

            // The total number of target values generated by this compaction across the bar. Must
            // be equal to one another by bar_apply_to_manifest(), with the same reasoning as the
            // source_values_* above.
            target_values_merge_count: u64 = 0,
            target_values_write_count: u64 = 0,

            /// When level_b == 0, it means level_a is the immutable table, which is special in a
            /// few ways:
            /// * It uses an iterator interface, as opposed to raw blocks like the rest.
            /// * It is responsible for keeping track of its own position, across beats.
            /// * It encompasses all possible values, so we don't need to worry about reading more.
            source_a_immutable_block: ?*Helpers.CompactionBlock = null,
            source_a_immutable_values: ?[]const Value = null,
            source_a_values_consumed_for_fill: usize = 0,
            source_a_position: Position = .{},

            /// level_b always comes from disk.
            source_b_position: Position = .{},

            /// At least 2 output index blocks needs to span beat boundaries, otherwise it wouldn't
            /// be possible to pace at a more granular level than target tables. (That is, each beat
            /// would need to write at least a full table.)
            target_index_blocks: ?Helpers.BlockFIFO,

            /// Manifest log appends are queued up until `finish()` is explicitly called to ensure
            /// they are applied deterministically relative to other concurrent compactions.
            // Worst-case manifest updates:
            // See docs/about/internals/lsm.md "Compaction Table Overlap" for more detail.
            manifest_entries: stdx.BoundedArray(struct {
                operation: enum {
                    insert_to_level_b,
                    move_to_level_b,
                },
                table: TableInfo,
            }, compaction_tables_output_max) = .{},

            table_builder: Table.Builder = .{},
        };

        const Beat = struct {
            const Read = struct {
                callback: BlipCallback,
                ptr: *anyopaque,

                pending_reads_index: usize = 0,
                pending_reads_data: usize = 0,

                next_tick: Grid.NextTick = undefined,
                timer: std.time.Timer,
                timer_read: usize = 0,
            };
            const Merge = struct {
                callback: BlipCallback,
                ptr: *anyopaque,

                next_tick: Grid.NextTick = undefined,
                timer: std.time.Timer,
            };
            const Write = struct {
                callback: BlipCallback,
                ptr: *anyopaque,

                pending_writes: usize = 0,

                next_tick: Grid.NextTick = undefined,
                timer: std.time.Timer,
                timer_write: usize = 0,
            };

            grid_reservation: Grid.Reservation,
            value_count_per_beat: u64,

            // TODO: This is now always 0 / 1 so get rid of it and just use index_read_done?
            index_blocks_read_b: usize = 0,
            index_read_done: bool = false,
            blocks: ?Helpers.CompactionBlocks = null,
            source_a_len_after_set: u64 = 0,
            source_b_len_after_set: u64 = 0,

            source_values_processed: u64 = 0,
            source_a_values: ?[]const Value = null,
            source_b_values: ?[]const Value = null,

            // Unlike other places where we can use a single state enum, a single Compaction
            // instance is _expected_ to be reading, writing and merging all at once. These
            // are not disjoint states!
            //
            // {read,merge,write} are considered inactive if their context is null.
            read: ?Read = null,
            merge: ?Merge = null,
            write: ?Write = null,

            fn activate_and_assert(
                self: *Beat,
                stage: BlipStage,
                callback: BlipCallback,
                ptr: *anyopaque,
            ) void {
                switch (stage) {
                    .read => {
                        assert(self.read == null);

                        self.read = .{
                            .callback = callback,
                            .ptr = ptr,
                            .timer = std.time.Timer.start() catch unreachable,
                        };
                        self.read.?.timer.reset();
                    },
                    .merge => {
                        assert(self.merge == null);

                        self.merge = .{
                            .callback = callback,
                            .ptr = ptr,
                            .timer = std.time.Timer.start() catch unreachable,
                        };
                        self.merge.?.timer.reset();
                    },
                    .write => {
                        assert(self.write == null);

                        self.write = .{
                            .callback = callback,
                            .ptr = ptr,
                            .timer = std.time.Timer.start() catch unreachable,
                        };
                        self.write.?.timer.reset();
                    },
                    .drained => unreachable,
                }
            }

            fn deactivate_assert_and_callback(
                self: *Beat,
                stage: BlipStage,
                exhausted: ?Exhausted,
            ) void {
                switch (stage) {
                    .read => {
                        assert(self.read != null);
                        assert(self.read.?.pending_reads_index == 0);
                        assert(self.read.?.pending_reads_data == 0);

                        const callback = self.read.?.callback;
                        const ptr = self.read.?.ptr;

                        self.read = null;

                        callback(ptr, exhausted);
                    },
                    .merge => {
                        assert(self.merge != null);

                        const callback = self.merge.?.callback;
                        const ptr = self.merge.?.ptr;

                        self.merge = null;

                        callback(ptr, exhausted);
                    },
                    .write => {
                        assert(self.write != null);
                        assert(self.write.?.pending_writes == 0);

                        const callback = self.write.?.callback;
                        const ptr = self.write.?.ptr;

                        self.write = null;

                        callback(ptr, exhausted);
                    },
                    .drained => unreachable,
                }
            }

            fn assert_all_inactive(self: *Beat) void {
                assert(self.read == null);
                assert(self.merge == null);
                assert(self.write == null);
            }
        };

        // Passed by `init`.
        tree_config: Tree.Config,
        level_b: u8,
        grid: *Grid,

        // Populated by {bar,beat}_setup.
        bar: ?Bar,
        beat: ?Beat,

        pub fn init(tree_config: Tree.Config, grid: *Grid, level_b: u8) !Compaction {
            assert(level_b < constants.lsm_levels);

            return Compaction{
                .tree_config = tree_config,
                .grid = grid,
                .level_b = level_b,

                .bar = null,
                .beat = null,
            };
        }

        pub fn deinit(compaction: *Compaction) void {
            _ = compaction;
            // TODO: Assert things here - compaction doesn't own anything that needs to be
            // deallocated.
        }

        pub fn reset(compaction: *Compaction) void {
            compaction.* = .{
                .tree_config = compaction.tree_config,
                .grid = compaction.grid,
                .level_b = compaction.level_b,

                .bar = null,
                .beat = null,
            };
        }

        pub fn assert_between_bars(compaction: *const Compaction) void {
            assert(compaction.bar == null);
            assert(compaction.beat == null);
        }

        /// Perform the bar-wise setup, and returns the compaction work that needs to be done for
        /// scheduling decisions. Returns null if there's no compaction work, or if move_table
        /// is happening (since it only touches the manifest).
        pub fn bar_setup(compaction: *Compaction, tree: *Tree, op: u64) ?CompactionInfo {
            assert(compaction.bar == null);
            assert(compaction.beat == null);

            // level_b 0 is special; unlike all the others which have level_a on disk, level 0's
            // level_a comes from the immutable table. This means that blip_read will be a partial,
            // no-op, and that the minimum input blocks are lowered by one.
            if (compaction.level_b == 0) {
                // Do not start compaction if the immutable table does not require compaction.
                if (tree.table_immutable.mutability.immutable.flushed) {
                    return null;
                }

                const table_immutable_values_count = tree.table_immutable.count();
                assert(table_immutable_values_count > 0);
                assert(table_immutable_values_count <= Table.value_count_max);

                const range_b = tree.manifest.immutable_table_compaction_range(
                    tree.table_immutable.key_min(),
                    tree.table_immutable.key_max(),
                    .{ .value_count = tree.table_immutable.count() },
                );

                // +1 to count the immutable table (level A).
                assert(range_b.tables.count() + 1 <= compaction_tables_input_max);
                assert(range_b.key_min <= tree.table_immutable.key_min());
                assert(tree.table_immutable.key_max() <= range_b.key_max);

                log.debug("{s}: compacting immutable table to level 0 " ++
                    "(snapshot_min={d} compaction.op_min={d} table_count={d} values={d})", .{
                    tree.config.name,
                    tree.table_immutable.mutability.immutable.snapshot_min,
                    op,
                    range_b.tables.count() + 1,
                    table_immutable_values_count,
                });

                var compaction_tables_value_count: usize = table_immutable_values_count;
                for (range_b.tables.const_slice()) |*table| {
                    compaction_tables_value_count += table.table_info.value_count;
                }

                compaction.bar = .{
                    .tree = tree,
                    .op_min = compaction_op_min(op),

                    .move_table = false,
                    .table_info_a = .{ .immutable = tree.table_immutable.values_used() },
                    .range_b = range_b,
                    .drop_tombstones = tree.manifest.compaction_must_drop_tombstones(
                        compaction.level_b,
                        range_b,
                    ),

                    .compaction_tables_value_count = compaction_tables_value_count,

                    .target_index_blocks = null,
                    .beats_max = null,
                };
            } else {
                const level_a = compaction.level_b - 1;

                // Do not start compaction if level A does not require compaction.
                const table_range = tree.manifest.compaction_table(level_a) orelse return null;

                const table_a = table_range.table_a.table_info;
                const range_b = table_range.range_b;

                assert(range_b.tables.count() + 1 <= compaction_tables_input_max);
                assert(table_a.key_min <= table_a.key_max);
                assert(range_b.key_min <= table_a.key_min);
                assert(table_a.key_max <= range_b.key_max);

                log.debug("{s}: compacting {d} tables from level {d} to level {d}", .{
                    tree.config.name,
                    range_b.tables.count() + 1,
                    level_a,
                    compaction.level_b,
                });

                var compaction_tables_value_count: usize = table_a.value_count;
                for (range_b.tables.const_slice()) |*table| {
                    compaction_tables_value_count += table.table_info.value_count;
                }

                compaction.bar = .{
                    .tree = tree,
                    .op_min = compaction_op_min(op),

                    .move_table = range_b.tables.empty(),
                    .table_info_a = .{ .disk = table_range.table_a },
                    .range_b = range_b,
                    .drop_tombstones = tree.manifest.compaction_must_drop_tombstones(
                        compaction.level_b,
                        range_b,
                    ),

                    .compaction_tables_value_count = compaction_tables_value_count,

                    .target_index_blocks = null,
                    .beats_max = null,
                };

                // Append the entries to the manifest update queue here and now if we're doing
                // move table. They'll be applied later by bar_apply_to_manifest.
                if (compaction.bar.?.move_table) {
                    log.debug(
                        "{s}: Moving table: level_b={}",
                        .{ compaction.tree_config.name, compaction.level_b },
                    );

                    const snapshot_max = snapshot_max_for_table_input(compaction.bar.?.op_min);
                    assert(table_a.snapshot_max >= snapshot_max);

                    compaction.bar.?.manifest_entries.append_assume_capacity(.{
                        .operation = .move_to_level_b,
                        .table = table_a.*,
                    });

                    // If we move the table, we've processed all the values in it.
                    compaction.bar.?.source_values_read_count =
                        compaction.bar.?.compaction_tables_value_count;
                    compaction.bar.?.source_values_merge_count =
                        compaction.bar.?.compaction_tables_value_count;
                    compaction.bar.?.target_values_merge_count =
                        compaction.bar.?.compaction_tables_value_count;
                    compaction.bar.?.target_values_write_count =
                        compaction.bar.?.compaction_tables_value_count;
                }
            }

            // The last level must always drop tombstones.
            assert(compaction.bar.?.drop_tombstones or
                compaction.level_b < constants.lsm_levels - 1);

            return .{
                .compaction_tables_value_count = compaction.bar.?.compaction_tables_value_count,
                .target_key_min = compaction.bar.?.range_b.key_min,
                .target_key_max = compaction.bar.?.range_b.key_max,
                .move_table = compaction.bar.?.move_table,
                .tree_id = tree.config.id,
                .level_b = compaction.level_b,
            };
        }

        /// Setup the per beat budget, as well as the output index blocks. Done in a separate step
        /// to bar_setup() since the forest requires information from that step to calculate how it
        /// should split the work, and if there's move table, target_index_blocks must be len 0.
        /// beats_max is the number of beats that this compaction will have available to do its
        /// work.
        /// A compaction may be done before beats_max, if eg tables are mostly empty.
        /// Output index blocks are special, and are allocated at a bar level unlike all the
        /// other blocks which are done at a beat level. This is because while we can ensure we
        /// fill a value block, index blocks are too infrequent (one per table) to divide
        /// compaction by.
        pub fn bar_setup_budget(
            compaction: *Compaction,
            beats_max: u64,
            target_index_blocks: Helpers.BlockFIFO,
            source_a_immutable_block: *Helpers.CompactionBlock,
        ) void {
            // Limited to half bars for now.
            assert(beats_max <= @divExact(constants.lsm_batch_multiple, 2));
            assert(beats_max > 0);

            assert(compaction.bar != null);
            assert(compaction.beat == null);

            const bar = &compaction.bar.?;
            assert(!bar.move_table);

            assert(bar.beats_max == null);

            bar.beats_max = beats_max;
            bar.target_index_blocks = target_index_blocks;
            assert(target_index_blocks.count > 0);

            // TODO: Actually, assert this is only non-null when level_b == 0, otherwise it should
            // be null!
            assert(source_a_immutable_block.stage == .free);
            source_a_immutable_block.stage = .standalone;
            bar.source_a_immutable_block = source_a_immutable_block;

            log.debug("bar_setup_budget({s}): bar.compaction_tables_value_count={}", .{
                compaction.tree_config.name,
                bar.compaction_tables_value_count,
            });
        }

        /// Reserve blocks from the grid for this beat's worth of work, in the semi-worst case:
        /// - no tombstones are dropped,
        /// - no values are overwritten,
        /// - but, we know exact input value counts, so table fullness *is* accounted for.
        ///
        /// We must reserve before doing any async work so that the block acquisition order
        /// is deterministic (relative to other concurrent compactions).
        pub fn beat_grid_reserve(
            compaction: *Compaction,
        ) void {
            assert(compaction.bar != null);
            assert(compaction.beat == null);

            const bar = &compaction.bar.?;

            // If we're move_table, only the manifest is being updated, *not* the grid.
            assert(!bar.move_table);

            assert(bar.beats_max != null);

            // Calculate how many values we have to compact each beat, to self-correct our pacing.
            // Pacing will have imperfections due to rounding up to fill target value blocks and
            // immutable table filtering duplicate values.
            const beats_remaining = bar.beats_max.? - bar.beats_finished;
            const value_count_per_beat = stdx.div_ceil(
                bar.compaction_tables_value_count - bar.source_values_merge_count,
                beats_remaining,
            );
            assert(bar.compaction_tables_value_count > bar.source_values_merge_count);
            assert(beats_remaining > 0);
            assert(bar.source_values_merge_count + value_count_per_beat * beats_remaining >=
                bar.compaction_tables_value_count);

            // The +1 is for imperfections in pacing our immutable table, which might cause us
            // to overshoot by a single block (limited to 1 due to how the immutable table values
            // are consumed.)
            const value_blocks_per_beat = stdx.div_ceil(
                value_count_per_beat,
                Table.layout.block_value_count_max,
            ) + 1;

            // The +1 is in case we had a partially finished index block from a previous beat.
            const index_blocks_per_beat = stdx.div_ceil(
                value_blocks_per_beat,
                Table.data_block_count_max,
            ) + 1;
            const total_blocks_per_beat = index_blocks_per_beat + value_blocks_per_beat;

            // TODO The replica must stop accepting requests if it runs out of blocks/capacity,
            // rather than panicking here.
            // (actually, we want to still panic but with something nicer like vsr.fail)
            const grid_reservation = compaction.grid.reserve(total_blocks_per_beat).?;
            log.debug("beat_grid_reserve({s}): total_blocks_per_beat={} " ++
                "index_blocks_per_beat={} value_blocks_per_beat={} " ++
                "beat.value_count_per_beat={} ", .{
                compaction.tree_config.name,
                total_blocks_per_beat,
                index_blocks_per_beat,
                value_blocks_per_beat,
                value_count_per_beat,
            });

            compaction.beat = .{
                .grid_reservation = grid_reservation,
                .value_count_per_beat = value_count_per_beat,
            };
        }

        pub fn beat_blocks_assign(compaction: *Compaction, blocks: Helpers.CompactionBlocks) void {
            assert(compaction.bar != null);
            assert(compaction.beat != null);
            assert(compaction.beat.?.blocks == null);
            assert(!compaction.bar.?.move_table);
            assert(compaction.bar.?.table_builder.data_block_empty());

            assert(blocks.source_value_blocks[0].count > 0);
            assert(blocks.source_value_blocks[1].count > 0);
            assert(blocks.target_value_blocks.count > 0);

            assert(blocks.source_index_block_a.stage == .free);
            assert(blocks.source_index_block_b.stage == .free);

            blocks.source_index_block_a.stage = .standalone;
            blocks.source_index_block_b.stage = .standalone;

            compaction.beat.?.blocks = blocks;
        }

        // Our blip pipeline is 3 stages long, and split into read, merge and write stages. The
        // merge stage has a data dependency on both the read (source) and write (target) stages.
        //
        // Within a single compaction, the pipeline looks something like:
        // --------------------------------------------------
        // | R     | M       | W      | R      |           |
        // --------------------------------------------------
        // |       | R       | M      | W      |           |
        // --------------------------------------------------
        // |       |         | W      | C → E  | W         |
        // --------------------------------------------------
        //
        // Where → E means that the merge step indicated our work was complete for either this beat
        // or bar.
        //
        // At the moment, the forest won't pipeline different compactions from other tree-levels
        // together. It _can_ do this, but it requires a bit more thought in how memory is managed.
        //
        // IO work is always submitted to the kernel _before_ entering blip_merge().
        //
        // TODO: even without a threadpool, we can likely drive better performance by doubling up
        // the stages. The reason for this is that we expect blip_merge() to be quite a bit quicker
        // than blip_write().

        /// Perform read IO to fill our source_index_block_{a,b} and source_value_blocks with as
        /// many blocks as we can, given their sizes, and where we are in the amount of work we
        /// need to do this beat.
        pub fn blip_read(compaction: *Compaction, callback: BlipCallback, ptr: *anyopaque) void {
            log.debug("blip_read({s}): scheduling read IO", .{compaction.tree_config.name});

            assert(compaction.bar != null);
            assert(compaction.beat != null);
            assert(compaction.bar.?.move_table == false);

            const beat = &compaction.beat.?;

            beat.activate_and_assert(.read, callback, ptr);

            if (!beat.index_read_done) {
                compaction.blip_read_index();
            } else {
                compaction.blip_read_data();
            }
        }

        fn blip_read_index(compaction: *Compaction) void {
            assert(compaction.bar != null);
            assert(compaction.beat != null);

            const bar = &compaction.bar.?;
            const beat = &compaction.beat.?;
            const blocks = &beat.blocks.?;

            assert(!beat.index_read_done);
            assert(beat.index_blocks_read_b == 0);

            assert(beat.read != null);
            const read = &beat.read.?;

            // TODO: We only support 2 index blocks (1 for table a, 1 for table b) and don't
            // support spanning value reads across them for now.
            // TODO: index_block_a will always point to source_index_block_a (even though if our
            // source is immutable this isn't needed! Future optimization)...
            // index_block_b will be the index block of the table we're currently merging with.
            switch (bar.table_info_a) {
                .disk => |table_ref| {
                    blocks.source_index_block_a.target = compaction;
                    compaction.grid.read_block(
                        .{ .from_local_or_global_storage = blip_read_index_callback },
                        &blocks.source_index_block_a.read,
                        table_ref.table_info.address,
                        table_ref.table_info.checksum,
                        .{ .cache_read = true, .cache_write = true },
                    );
                    read.pending_reads_index += 1;
                },
                .immutable => {
                    // Immutable values come from the in memory immutable table - no need to read.
                },
            }

            if (bar.range_b.tables.count() == 0) assert(compaction.level_b == 0);
            assert(bar.source_b_position.index_block <= bar.range_b.tables.count());

            if (bar.range_b.tables.count() > 0 and
                bar.source_b_position.index_block < bar.range_b.tables.count())
            {
                const table_ref = bar.range_b.tables.get(bar.source_b_position.index_block);
                blocks.source_index_block_b.target = compaction;
                compaction.grid.read_block(
                    .{ .from_local_or_global_storage = blip_read_index_callback },
                    &blocks.source_index_block_b.read,
                    table_ref.table_info.address,
                    table_ref.table_info.checksum,
                    .{ .cache_read = true, .cache_write = true },
                );
                read.pending_reads_index += 1;
                beat.index_blocks_read_b += 1;
            }

            log.debug("blip_read({s}): scheduled {} index reads", .{
                compaction.tree_config.name,
                read.pending_reads_index,
            });

            // Either we have pending index reads, in which case blip_read_data gets called by
            // blip_read_index_callback once all reads are done, or we don't, in which case call it
            // here.
            // TODO: Should we switch this at the read_block() level?
            if (read.pending_reads_index == 0) {
                beat.index_read_done = true;

                compaction.blip_read_data();
            }
        }

        fn blip_read_index_callback(grid_read: *Grid.Read, index_block: BlockPtrConst) void {
            const parent: *Helpers.CompactionBlock = @fieldParentPtr("read", grid_read);
            const compaction: *Compaction = @alignCast(@ptrCast(parent.target));
            assert(compaction.bar != null);
            assert(compaction.beat != null);
            assert(compaction.tree_config.id == Table.index.block_metadata(index_block).tree_id);

            const beat = &compaction.beat.?;

            assert(beat.read != null);
            const read = &beat.read.?;

            read.pending_reads_index -= 1;
            read.timer_read += 1;

            stdx.copy_disjoint(.exact, u8, parent.block, index_block);

            if (read.pending_reads_index != 0) return;

            beat.index_read_done = true;

            compaction.blip_read_data();
        }

        fn blip_read_data(compaction: *Compaction) void {
            assert(compaction.bar != null);
            assert(compaction.beat != null);

            const bar = &compaction.bar.?;
            const beat = &compaction.beat.?;

            assert(beat.index_read_done);
            assert(beat.read != null);
            const read = &beat.read.?;

            assert(read.pending_reads_index == 0);
            assert(read.pending_reads_data == 0);

            // TODO: The code for reading table_a and table_b are almost identical,
            // only differing in _a vs _b and [0] vs [1]...

            // Read data for table a - which we'll only have if compaction.level_b > 0.
            assert(bar.source_a_position.index_block <= 1);
            if (bar.table_info_a == .disk and bar.source_a_position.index_block == 0) {
                assert(beat.blocks.?.source_value_blocks[0].pending.empty());

                var i: usize = bar.source_a_position.value_block +
                    beat.blocks.?.source_value_blocks[0].ready.count;

                const index_block = beat.blocks.?.source_index_block_a.block;
                const index_schema = schema.TableIndex.from(index_block);

                const value_blocks_used = index_schema.data_blocks_used(index_block);
                const value_block_addresses = index_schema.data_addresses_used(index_block);
                const value_block_checksums = index_schema.data_checksums_used(index_block);

                var free_blocks_used: usize = 0;
                // Read at most half of the blocks, to smooth out reading over multiple steps.
                const half_blocks_count = stdx.div_ceil(
                    beat.blocks.?.source_value_blocks[0].count,
                    2,
                );
                // Once our read buffer is full, end the loop.
                while (i < value_blocks_used and free_blocks_used < half_blocks_count) {
                    const source_value_block =
                        beat.blocks.?.source_value_blocks[0].free_to_pending() orelse break;

                    source_value_block.target = compaction;
                    compaction.grid.read_block(
                        .{ .from_local_or_global_storage = blip_read_data_callback },
                        &source_value_block.read,
                        value_block_addresses[i],
                        value_block_checksums[i].value,
                        .{ .cache_read = true, .cache_write = true },
                    );

                    read.pending_reads_data += 1;
                    free_blocks_used += 1;
                    i += 1;
                }
            }

            // Read data for our tables in range b, which will always come from disk.
            const table_b_count = bar.range_b.tables.count();
            assert(table_b_count == 0 or
                bar.source_b_position.index_block == table_b_count or
                beat.index_blocks_read_b == 1);
            assert(bar.source_b_position.index_block <= table_b_count);

            if (table_b_count > 0 and bar.source_b_position.index_block < table_b_count) {
                assert(beat.blocks.?.source_value_blocks[1].pending.empty());

                var i: usize = bar.source_b_position.value_block +
                    beat.blocks.?.source_value_blocks[1].ready.count;

                // TODO: Getting this right, while spanning multiple tables, turned out to be
                // tricky. Now, we're required to blip again when a table is finished.
                const index_block = beat.blocks.?.source_index_block_b.block;
                const index_schema = schema.TableIndex.from(index_block);

                const value_blocks_used = index_schema.data_blocks_used(index_block);
                const value_block_addresses = index_schema.data_addresses_used(index_block);
                const value_block_checksums = index_schema.data_checksums_used(index_block);

                var free_blocks_used: usize = 0;
                const half_blocks_count = stdx.div_ceil(
                    beat.blocks.?.source_value_blocks[1].count,
                    2,
                );
                while (i < value_blocks_used and free_blocks_used < half_blocks_count) {
                    const maybe_source_value_block =
                        beat.blocks.?.source_value_blocks[1].free_to_pending();
                    free_blocks_used += 1;

                    // Once our read buffer is full, break out of the outer loop.
                    if (maybe_source_value_block == null) break;

                    const source_value_block = maybe_source_value_block.?;

                    source_value_block.target = compaction;
                    compaction.grid.read_block(
                        .{ .from_local_or_global_storage = blip_read_data_callback },
                        &source_value_block.read,
                        value_block_addresses[i],
                        value_block_checksums[i].value,
                        .{ .cache_read = true, .cache_write = true },
                    );

                    read.pending_reads_data += 1;
                    i += 1;
                }
            }

            log.debug("blip_read({s}): scheduled {} data reads.", .{
                compaction.tree_config.name,
                read.pending_reads_data,
            });

            // Either we have pending data reads, in which case blip_read_next_tick gets called by
            // blip_read_data_callback once all reads are done, or we don't, in which case call it
            // here via next_tick.
            if (read.pending_reads_data == 0) {
                compaction.grid.on_next_tick(blip_read_next_tick, &read.next_tick);
            }
        }

        fn blip_read_data_callback(grid_read: *Grid.Read, value_block: BlockPtrConst) void {
            const parent: *Helpers.CompactionBlock = @fieldParentPtr("read", grid_read);
            const compaction: *Compaction = @alignCast(@ptrCast(parent.target));
            assert(compaction.tree_config.id == Table.data.block_metadata(value_block).tree_id);

            assert(compaction.bar != null);
            assert(compaction.beat != null);

            const beat = &compaction.beat.?;

            assert(beat.read != null);
            const read = &beat.read.?;

            read.pending_reads_data -= 1;
            read.timer_read += 1;

            // TODO: This copies the block, we should try instead to steal it for the duration of
            // the compaction...
            stdx.copy_disjoint(.exact, u8, parent.block, value_block);

            // Join on all outstanding reads before continuing.
            if (read.pending_reads_data != 0) return;

            // Unlike the blip_write which has to make use of an io'ing stage, the only thing
            // that transitions read blocks to pending is blip_read_data, so it's safe here.
            while (beat.blocks.?.source_value_blocks[0].pending_to_ready() != null) {}
            while (beat.blocks.?.source_value_blocks[1].pending_to_ready() != null) {}

            // Call the next tick handler directly. This callback is invoked async, so it's safe
            // from stack overflows.
            blip_read_next_tick(&read.next_tick);
        }

        fn blip_read_next_tick(next_tick: *Grid.NextTick) void {
            // TODO(zig): Address usage of @fieldParentPtr to optional fields.
            const beat_read: *Beat.Read = @fieldParentPtr("next_tick", next_tick);
            const read: *?Beat.Read = @ptrCast(beat_read);
            const beat: *Beat = @fieldParentPtr("read", read);

            const duration = read.*.?.timer.read();
            log.debug("blip_read(): took {} to read {} blocks", .{
                std.fmt.fmtDuration(duration),
                read.*.?.timer_read,
            });

            beat.deactivate_assert_and_callback(.read, null);
        }

        /// Perform CPU merge work, to transform our source tables to our target tables.
        ///
        /// blip_merge is also responsible for signalling when to stop blipping entirely. A
        /// sequence of blips is over when one of the following condition are met, considering we
        /// don't want to output partial value blocks unless we have to:
        ///
        /// * We have reached our value_count_per_beat. Finish up the next value block, and we're
        ///   done.
        /// * We have no more source values remaining, at all - the bar is done. This will likely
        ///   result in a partially full value block, but that's OK (end of a table).
        /// * We have no more output value blocks remaining in our buffer - we might need more
        ///   blips, but that's up to the forest to orchestrate.
        /// * We have no more output index blocks remaining in our buffer - we might have a partial
        ///   value block here, but that's OK (end of a table).
        ///
        /// This is not to be confused with blip_merge itself finishing; this can happen at any time
        /// because we need more input values, and that's OK. We hold on to our buffers for a beat.
        pub fn blip_merge(compaction: *Compaction, callback: BlipCallback, ptr: *anyopaque) void {
            log.debug("blip_merge({s}) starting", .{compaction.tree_config.name});

            assert(compaction.bar != null);
            assert(compaction.beat != null);
            assert(compaction.bar.?.move_table == false);

            const bar = &compaction.bar.?;
            const beat = &compaction.beat.?;

            beat.activate_and_assert(.merge, callback, ptr);
            const merge = &beat.merge.?;

            var source_exhausted_bar = false;
            var source_exhausted_beat = false;

            assert(bar.table_builder.value_count < Table.layout.block_value_count_max);

            const blocks = &beat.blocks.?;

            var target_value_blocks = &blocks.target_value_blocks;
            var target_index_blocks = &bar.target_index_blocks.?;

            // Loop through the CPU work until we have nothing left.
            // TODO: Put better bounds on this, to get rid of while(true).
            while (true) {
                // Set the index block if needed.
                if (bar.table_builder.state == .no_blocks) {
                    if (target_index_blocks.ready.count ==
                        @divExact(target_index_blocks.count, 2))
                    {
                        break;
                    }

                    const index_block = target_index_blocks.free_to_pending().?.block;
                    // TODO: We don't need to zero the whole block; just the part of the padding
                    // that's not covered by alignment.
                    @memset(index_block, 0);
                    bar.table_builder.set_index_block(index_block);
                }

                // Set the value block if needed.
                if (bar.table_builder.state == .index_block) {
                    if (target_value_blocks.ready.count ==
                        @divExact(target_value_blocks.count, 2))
                    {
                        break;
                    }

                    const value_block = target_value_blocks.free_to_pending().?.block;
                    // TODO: We don't need to zero the whole block; just the part of the padding
                    // that's not covered by alignment.
                    @memset(value_block, 0);
                    bar.table_builder.set_data_block(value_block);
                }

                // Try to refill our sources.
                const source_a_filled = compaction.set_source_a();
                const source_b_filled = compaction.set_source_b();

                if (source_a_filled == .need_read or source_b_filled == .need_read) {
                    log.debug("blip_merge({s}): need to read more blocks.", .{
                        compaction.tree_config.name,
                    });

                    // This will get set again on the next iteration of the loop.
                    source_exhausted_beat = false;

                    break;
                }

                const source_a_exhausted = source_a_filled == .exhausted;
                const source_b_exhausted = source_b_filled == .exhausted;

                log.debug("blip_merge({s}): source_a_exhausted={} source_b_exhausted={} " ++
                    "bar.source_values_merge_count={} bar.compaction_tables_value_count={}", .{
                    compaction.tree_config.name,
                    source_a_exhausted,
                    source_b_exhausted,
                    bar.source_values_merge_count,
                    bar.compaction_tables_value_count,
                });

                // It's important here to take note of what these checks mean: they apply when a
                // source is _completely_ exhausted; ie, there's no more data on disk so the mode
                // is switched.
                // TODO: Assert the state transitions - if source_a_exhausted, it can never be
                // unexhausted for that bar. (etc)
                const table_builder_count = bar.table_builder.value_count;
                if (source_a_exhausted and !source_b_exhausted) {
                    compaction.copy(.b);
                } else if (source_b_exhausted and !source_a_exhausted) {
                    if (bar.drop_tombstones) {
                        compaction.copy_drop_tombstones();
                    } else {
                        compaction.copy(.a);
                    }
                } else if (!source_a_exhausted and !source_b_exhausted) {
                    compaction.merge_values();
                }
                bar.target_values_merge_count += bar.table_builder.value_count -
                    table_builder_count;

                const source_values_merge_count_a = compaction.update_position_a();
                const source_values_merge_count_b = compaction.update_position_b();
                const source_values_merge_count = source_values_merge_count_a +
                    source_values_merge_count_b;
                std.log.debug("blip_merge({s}): source_values_merge_count_a={} " ++
                    "source_values_merge_count_b={}", .{
                    compaction.tree_config.name,
                    source_values_merge_count_a,
                    source_values_merge_count_b,
                });

                beat.source_values_processed += source_values_merge_count;
                bar.source_values_merge_count += source_values_merge_count;

                // Sanity check. If our sources are exhausted but our values processed sum doesn't
                // match the total values we had to process we have a bug somewhere.
                if (source_a_exhausted and source_b_exhausted) {
                    assert(bar.source_values_merge_count == bar.compaction_tables_value_count);
                }

                // beat.source_values_processed can overrun, but we can never do more work for a
                // bar than what we know we have.
                assert(bar.source_values_merge_count <= bar.compaction_tables_value_count);

                // When checking if we're done, there are two things we need to consider:
                // 1. Have we finished our input entirely? If so, we flush what we have - it's
                //    likely to be a partial block but that's OK.
                // 2. Have we reached our value_count_per_beat? If so, we'll flush at the next
                //    complete value block.
                //
                // This means that we'll potentially overrun our value_count_per_beat by up to
                // a full value block.
                source_exhausted_bar = bar.source_values_merge_count ==
                    bar.compaction_tables_value_count;
                source_exhausted_beat = beat.source_values_processed >= beat.value_count_per_beat;

                log.debug("blip_merge({s}): beat.source_values_processed={} " ++
                    "beat.value_count_per_beat={}. (source_exhausted_bar={}, " ++
                    "source_exhausted_beat={})", .{
                    compaction.tree_config.name,
                    beat.source_values_processed,
                    beat.value_count_per_beat,
                    source_exhausted_bar,
                    source_exhausted_beat,
                });

                switch (compaction.check_and_finish_blocks(source_exhausted_bar)) {
                    .unfinished_value_block => {},
                    .finished_value_block => if (source_exhausted_beat) break,
                }
            }

            const d = merge.timer.read();
            log.debug("blip_merge(): took {} to merge blocks", .{std.fmt.fmtDuration(d)});

            if (source_exhausted_bar) {
                assert(compaction.set_source_a() == .exhausted);
                assert(compaction.set_source_b() == .exhausted);
                assert(bar.source_values_read_count == bar.source_values_merge_count);

                // Sanity check our primary condition.
                assert(bar.source_values_merge_count == bar.compaction_tables_value_count);

                if (bar.table_info_a == .immutable) {
                    assert(bar.table_info_a.immutable.len == 0);
                } else {
                    assert(blocks.source_value_blocks[0].ready.count == 0);
                    compaction.release_table_blocks(blocks.source_index_block_a.block);
                }

                assert(blocks.source_value_blocks[1].ready.count == 0);
                // table_b's release_table_blocks gets called in update_position_b.
                // TODO: Maybe we should move table_a's there too...?
            }

            beat.deactivate_assert_and_callback(.merge, .{
                .bar = source_exhausted_bar,
                .beat = source_exhausted_bar or source_exhausted_beat,
            });
        }

        fn set_source_a(compaction: *Compaction) enum { filled, need_read, exhausted } {
            const bar = &compaction.bar.?;
            const beat = &compaction.beat.?;

            if (bar.table_info_a == .immutable) {
                // Immutable table can never .need_read, since all its values come from memory.
                assert(bar.source_a_immutable_block != null);
                stdx.maybe(bar.source_a_immutable_values == null);
                assert(compaction.level_b == 0);

                // If our immutable values 'block' is empty, refill it from its iterator.
                // TODO: Cleanup the logic here!
                var updated_fill_count = false;
                if (bar.source_a_immutable_values == null or
                    bar.source_a_immutable_values.?.len == 0)
                {
                    if (bar.table_info_a.immutable.len > 0) {
                        const values = Table.data_block_values(
                            bar.source_a_immutable_block.?.block,
                        );
                        const immutable_remaining_before_fill = bar.table_info_a.immutable.len;
                        const filled = compaction.fill_immutable_values(values);
                        bar.source_a_values_consumed_for_fill =
                            immutable_remaining_before_fill - bar.table_info_a.immutable.len;
                        updated_fill_count = true;
                        bar.source_a_immutable_values = values[0..filled];
                        log.debug("set_source_a({s}): refilled immutable block. {} values out, " ++
                            "{} values consumed", .{
                            compaction.tree_config.name,
                            filled,
                            bar.source_a_values_consumed_for_fill,
                        });
                    }
                }

                beat.source_a_values = bar.source_a_immutable_values.?;
                if (bar.source_a_immutable_values.?.len == 0) {
                    if (!updated_fill_count) {
                        bar.source_a_values_consumed_for_fill = 0;
                    }
                    return .exhausted;
                }

                return .filled;
            } else {
                const blocks = &beat.blocks.?;
                defer beat.source_a_len_after_set = beat.source_a_values.?.len;

                log.debug("set_source_a({s}): bar.source_a_position = {}", .{
                    compaction.tree_config.name,
                    bar.source_a_position,
                });

                // Unlike with range_b, where we can have an empty index block, with table_a if
                // we're not coming from the immutable table we have an index block by definition.
                if (bar.source_a_position.index_block == 1) {
                    beat.source_a_values = &.{};
                    return .exhausted;
                }

                if (beat.source_a_values != null and beat.source_a_values.?.len > 0) return .filled;
                if (blocks.source_value_blocks[0].ready.empty()) return .need_read;

                const current_value_block = blocks.source_value_blocks[0].ready_peek().?.block;

                // Verify this block is indeed the correct one.
                const index_block = blocks.source_index_block_a.block;
                const index_schema = schema.TableIndex.from(index_block);
                const value_block_checksums = index_schema.data_checksums_used(index_block);
                const current_value_block_header = schema.header_from_block(current_value_block);
                assert(value_block_checksums[bar.source_a_position.value_block].value ==
                    current_value_block_header.checksum);

                beat.source_a_values = Table.data_block_values_used(
                    current_value_block,
                )[bar.source_a_position.value_block_index..];

                return .filled;
            }
        }

        fn set_source_b(compaction: *Compaction) enum { filled, need_read, exhausted } {
            const bar = &compaction.bar.?;
            const beat = &compaction.beat.?;
            const blocks = &beat.blocks.?;

            log.debug("set_source_b({s}): bar.source_b_position={} " ++
                "bar.range_b.tables.count()={}", .{
                compaction.tree_config.name,
                bar.source_b_position,
                bar.range_b.tables.count(),
            });

            defer beat.source_b_len_after_set = beat.source_b_values.?.len;

            if (bar.range_b.tables.empty()) {
                beat.source_b_values = &.{};
                return .exhausted;
            }
            if (bar.source_b_position.index_block == bar.range_b.tables.count()) {
                beat.source_b_values = &.{};
                return .exhausted;
            }
            if (beat.source_b_values != null and beat.source_b_values.?.len > 0) return .filled;
            if (blocks.source_value_blocks[1].ready.empty()) return .need_read;

            const current_value_block = blocks.source_value_blocks[1].ready_peek().?.block;

            // Verify this block is indeed the correct one.
            const index_block = blocks.source_index_block_b.block;
            const index_schema = schema.TableIndex.from(index_block);
            const value_block_checksums = index_schema.data_checksums_used(index_block);
            const current_value_block_header = schema.header_from_block(current_value_block);
            assert(value_block_checksums[bar.source_b_position.value_block].value ==
                current_value_block_header.checksum);

            beat.source_b_values = Table.data_block_values_used(
                current_value_block,
            )[bar.source_b_position.value_block_index..];

            return .filled;
        }

        fn update_position_a(compaction: *Compaction) usize {
            const bar = &compaction.bar.?;
            const beat = &compaction.beat.?;
            const blocks = &beat.blocks.?;

            if (bar.table_info_a == .immutable) {
                bar.source_a_immutable_values = beat.source_a_values;

                if (beat.source_a_values != null and beat.source_a_values.?.len == 0) {
                    bar.source_values_read_count +=
                        bar.source_a_values_consumed_for_fill;
                    return bar.source_a_values_consumed_for_fill;
                }
            } else {
                if (beat.source_a_values != null and beat.source_a_values.?.len > 0) {
                    const values_consumed = beat.source_a_len_after_set -
                        beat.source_a_values.?.len;
                    bar.source_a_position.value_block_index += values_consumed;

                    return values_consumed;
                }

                if (bar.source_a_position.index_block == 1) {
                    return 0;
                }

                bar.source_a_position.value_block_index = 0;
                bar.source_a_position.value_block += 1;

                const old_block = blocks.source_value_blocks[0].ready_to_free().?;
                bar.source_values_read_count += Table.data_block_values_used(old_block.block).len;

                const index_block = blocks.source_index_block_a.block;
                const index_schema = schema.TableIndex.from(index_block);
                const value_blocks_used = index_schema.data_blocks_used(index_block);

                if (bar.source_a_position.value_block == value_blocks_used) {
                    bar.source_a_position.value_block = 0;
                    bar.source_a_position.index_block += 1;
                }

                return beat.source_a_len_after_set;
            }

            return 0;
        }

        fn update_position_b(compaction: *Compaction) usize {
            const bar = &compaction.bar.?;
            const beat = &compaction.beat.?;
            const blocks = &beat.blocks.?;

            if (beat.source_b_values != null and beat.source_b_values.?.len > 0) {
                const values_consumed = beat.source_b_len_after_set - beat.source_b_values.?.len;
                bar.source_b_position.value_block_index += values_consumed;

                return values_consumed;
            }
            if (bar.range_b.tables.empty()) return 0;
            if (bar.source_b_position.index_block == bar.range_b.tables.count()) return 0;

            bar.source_b_position.value_block_index = 0;
            bar.source_b_position.value_block += 1;
            const old_block = blocks.source_value_blocks[1].ready_to_free().?;
            bar.source_values_read_count += Table.data_block_values_used(old_block.block).len;

            const index_block = blocks.source_index_block_b.block;
            const index_schema = schema.TableIndex.from(index_block);
            const value_blocks_used = index_schema.data_blocks_used(index_block);

            if (bar.source_b_position.value_block == value_blocks_used) {
                bar.source_b_position.value_block = 0;
                bar.source_b_position.index_block += 1;

                // Release the index block and any value blocks it referenced.
                compaction.release_table_blocks(blocks.source_index_block_b.block);

                // TODO: Perhaps this logic should be in read rather?
                if (bar.source_b_position.index_block < bar.range_b.tables.count()) {
                    beat.index_read_done = false;
                    beat.index_blocks_read_b = 0;
                }
            }
            return beat.source_b_len_after_set;
        }

        /// Copies values to `target` from our immutable table input. In the process, merge values
        /// with identical keys (last one wins) and collapse tombstones for secondary indexes.
        /// Return the number of values written to the target and updates immutable table slice to
        /// the non-processed remainder.
        fn fill_immutable_values(compaction: *Compaction, target: []Value) usize {
            const bar = &compaction.bar.?;

            var source = bar.table_info_a.immutable;
            assert(source.len > 0);

            if (constants.verify) {
                // The input may have duplicate keys (last one wins), but keys must be
                // non-decreasing.
                // A source length of 1 is always non-decreasing.
                for (source[0 .. source.len - 1], source[1..source.len]) |*value, *value_next| {
                    assert(key_from_value(value) <= key_from_value(value_next));
                }
            }

            var source_index: usize = 0;
            var target_index: usize = 0;
            while (target_index < target.len and source_index < source.len) {
                target[target_index] = source[source_index];

                // If we're at the end of the source, there is no next value, so the next value
                // can't be equal.
                const value_next_equal = source_index + 1 < source.len and
                    key_from_value(&source[source_index]) ==
                    key_from_value(&source[source_index + 1]);

                if (value_next_equal) {
                    if (Table.usage == .secondary_index) {
                        // Secondary index optimization --- cancel out put and remove.
                        // NB: while this prevents redundant tombstones from getting to disk, we
                        // still spend some extra CPU work to sort the entries in memory. Ideally,
                        // we annihilate tombstones immediately, before sorting, but that's tricky
                        // to do with scopes.
                        assert(tombstone(&source[source_index]) !=
                            tombstone(&source[source_index + 1]));
                        source_index += 2;
                        target_index += 0;
                    } else {
                        // The last value in a run of duplicates needs to be the one that ends up in
                        // target.
                        source_index += 1;
                        target_index += 0;
                    }
                } else {
                    source_index += 1;
                    target_index += 1;
                }
            }

            // At this point, source_index and target_index are actually counts.
            // source_index will always be incremented after the final iteration as part of the
            // continue expression.
            // target_index will always be incremented, since either source_index runs out first
            // so value_next_equal is false, or a new value is hit, which will increment it.
            const source_count = source_index;
            const target_count = target_index;
            assert(target_count <= source_count);
            bar.table_info_a.immutable =
                bar.table_info_a.immutable[source_count..];

            if (target_count == 0) {
                assert(Table.usage == .secondary_index);
                return 0;
            }

            if (constants.verify) {
                // Our output must be strictly increasing.
                // An output length of 1 is always strictly increasing.
                for (
                    target[0 .. target_count - 1],
                    target[1..target_count],
                ) |*value, *value_next| {
                    assert(key_from_value(value_next) > key_from_value(value));
                }
            }

            assert(target_count > 0);
            return target_count;
        }

        fn check_and_finish_blocks(compaction: *Compaction, force_flush: bool) enum {
            unfinished_value_block,
            finished_value_block,
        } {
            const bar = &compaction.bar.?;
            const beat = &compaction.beat.?;

            const target_value_blocks = &beat.blocks.?.target_value_blocks;
            const target_index_blocks = &bar.target_index_blocks.?;

            assert(beat.merge != null);

            const table_builder = &bar.table_builder;

            var finished_value_block = false;
            assert(bar.table_builder.state == .index_and_data_block);

            const release =
                compaction.grid.superblock.working.vsr_state.checkpoint.release;

            // Flush the value block if needed.
            if (table_builder.data_block_full() or
                table_builder.index_block_full() or
                (force_flush and !table_builder.data_block_empty()))
            {
                log.debug("blip_merge({s}): finished target value block", .{
                    compaction.tree_config.name,
                });
                table_builder.data_block_finish(.{
                    .cluster = compaction.grid.superblock.working.cluster,
                    .release = release,
                    .address = compaction.grid.acquire(compaction.beat.?.grid_reservation),
                    .snapshot_min = snapshot_min_for_table_output(bar.op_min),
                    .tree_id = compaction.tree_config.id,
                });

                assert(target_value_blocks.pending.count == 1);
                _ = target_value_blocks.pending_to_ready().?;
                finished_value_block = true;
            } else if (force_flush and table_builder.data_block_empty()) {
                assert(target_value_blocks.pending.count == 1);

                _ = target_value_blocks.pending_to_free().?;
                table_builder.state = .index_block;
                finished_value_block = true;
            }

            // Flush the index block if needed.
            if (table_builder.index_block_full() or
                // If the input is exhausted then we need to flush all blocks before finishing.
                (force_flush and !table_builder.index_block_empty()))
            {
                log.debug("blip_merge({s}): finished target index block", .{
                    compaction.tree_config.name,
                });
                const table = table_builder.index_block_finish(.{
                    .cluster = compaction.grid.superblock.working.cluster,
                    .release = release,
                    .address = compaction.grid.acquire(compaction.beat.?.grid_reservation),
                    .snapshot_min = snapshot_min_for_table_output(bar.op_min),
                    .tree_id = compaction.tree_config.id,
                });

                assert(target_index_blocks.pending.count == 1);
                _ = target_index_blocks.pending_to_ready().?;

                // Make this table visible at the end of this bar.
                bar.manifest_entries.append_assume_capacity(.{
                    .operation = .insert_to_level_b,
                    .table = table,
                });
            } else if (force_flush and table_builder.index_block_empty()) {
                assert(target_index_blocks.pending.count == 1);

                _ = target_index_blocks.pending_to_free().?;
                table_builder.state = .no_blocks;
            }

            if (finished_value_block) return .finished_value_block;
            return .unfinished_value_block;
        }

        // TODO: Support for LSM snapshots would require us to only remove blocks
        // that are invisible.
        fn release_table_blocks(compaction: *Compaction, index_block: BlockPtrConst) void {
            // Release the table's block addresses in the Grid as it will be made invisible.
            // This is safe; compaction.index_block_b holds a copy of the index block for a
            // table in Level B. Additionally, compaction.index_block_a holds
            // a copy of the index block for the Level A table being compacted.
            log.debug("release_table_blocks({s})", .{compaction.tree_config.name});

            const grid = compaction.grid;
            const index_schema = schema.TableIndex.from(index_block);
            for (index_schema.data_addresses_used(index_block)) |address| grid.release(address);
            grid.release(Table.block_address(index_block));
        }

        /// Perform write IO to write our target_index_blocks and target_value_blocks to disk.
        pub fn blip_write(compaction: *Compaction, callback: BlipCallback, ptr: *anyopaque) void {
            assert(compaction.bar != null);
            assert(compaction.beat != null);
            assert(compaction.bar.?.move_table == false);

            const bar = &compaction.bar.?;
            const beat = &compaction.beat.?;

            beat.activate_and_assert(.write, callback, ptr);

            assert(beat.write != null);
            const write = &beat.write.?;

            log.debug("blip_write({s}): scheduling IO", .{compaction.tree_config.name});

            assert(write.pending_writes == 0);

            // Write any complete index blocks.
            while (bar.target_index_blocks.?.ready_to_ioing()) |target_index_block| {
                target_index_block.target = compaction;
                compaction.grid.create_block(
                    blip_write_callback,
                    &target_index_block.write,
                    &target_index_block.block,
                );
                write.pending_writes += 1;
            }

            // Write any complete value blocks.
            while (beat.blocks.?.target_value_blocks.ready_to_ioing()) |target_value_block| {
                target_value_block.target = compaction;

                // It would be nice to set this in the callback, but create_block consumes our
                // block...
                bar.target_values_write_count +=
                    Table.data_block_values_used(target_value_block.block).len;

                compaction.grid.create_block(
                    blip_write_callback,
                    &target_value_block.write,
                    &target_value_block.block,
                );
                write.pending_writes += 1;
            }

            const d = write.timer.read();
            log.debug("blip_write({s}): took {} to schedule {} blocks", .{
                compaction.tree_config.name,
                std.fmt.fmtDuration(d),
                write.pending_writes,
            });
            write.timer.reset();

            // TODO: The big idea is to make compaction pacing explicit and asserted behaviour
            // rather than just an implicit property of the code. We should add asserts around how
            // much work we do per beat.

            if (write.pending_writes == 0) {
                compaction.grid.on_next_tick(blip_write_next_tick, &write.next_tick);
            }
        }

        fn blip_write_callback(grid_write: *Grid.Write) void {
            const parent: *Helpers.CompactionBlock = @fieldParentPtr("write", grid_write);
            const compaction: *Compaction = @alignCast(@ptrCast(parent.target));
            assert(compaction.bar != null);
            assert(compaction.beat != null);

            const beat = &compaction.beat.?;

            assert(beat.write != null);
            const write = &beat.write.?;
            write.pending_writes -= 1;
            write.timer_write += 1;

            // Join on all outstanding writes before continuing.
            if (write.pending_writes != 0) return;

            var freed: usize = 0;
            while (beat.blocks.?.target_value_blocks.ioing_to_free() != null) freed += 1;
            while (compaction.bar.?.target_index_blocks.?.ioing_to_free() != null) freed += 1;
            assert(freed > 0);

            // Call the next tick handler directly. This callback is invoked async, so it's safe
            // from stack overflows.
            blip_write_next_tick(&write.next_tick);
        }

        fn blip_write_next_tick(next_tick: *Grid.NextTick) void {
            // TODO(zig): Address usage of @fieldParentPtr to optional fields.
            const write: *?Beat.Write = @ptrCast(
                @as(*Beat.Write, @fieldParentPtr("next_tick", next_tick)),
            );
            const beat: *Beat = @fieldParentPtr("write", write);

            const duration = write.*.?.timer.read();
            log.debug("blip_write(): took {} to write {} blocks", .{
                std.fmt.fmtDuration(duration),
                write.*.?.timer_write,
            });
            beat.deactivate_assert_and_callback(.write, null);
        }

        /// Return our blocks to the block pool.
        pub fn beat_blocks_unassign(
            compaction: *Compaction,
            block_pool: *Helpers.CompactionBlockFIFO,
        ) void {
            assert(compaction.bar != null);
            assert(compaction.beat != null);
            assert(compaction.beat.?.blocks != null);
            assert(!compaction.bar.?.move_table);

            const blocks = &compaction.beat.?.blocks.?;

            assert(blocks.source_index_block_a.stage == .standalone);
            assert(blocks.source_index_block_b.stage == .standalone);

            blocks.source_index_block_a.stage = .free;
            blocks.source_index_block_b.stage = .free;

            block_pool.push(blocks.source_index_block_a);
            block_pool.push(blocks.source_index_block_b);

            // source_value_blocks[*] can finish with blocks in the ready state. This is because
            // these are reads that we're throwing away as we didn't need them.
            // TODO(metric): Track this.
            while (blocks.source_value_blocks[0].ready_to_free() != null) {}
            while (blocks.source_value_blocks[1].ready_to_free() != null) {}

            blocks.source_value_blocks[0].deinit(block_pool);
            blocks.source_value_blocks[1].deinit(block_pool);
            blocks.target_value_blocks.deinit(block_pool);

            compaction.beat.?.blocks = null;
        }

        pub fn beat_grid_forfeit(compaction: *Compaction) void {
            assert(compaction.bar != null);
            assert(compaction.beat != null);
            assert(compaction.bar.?.move_table == false);

            const bar = &compaction.bar.?;
            const beat = &compaction.beat.?;

            assert(beat.blocks == null);

            beat.assert_all_inactive();
            assert(bar.table_builder.data_block_empty());

            log.debug("beat_grid_forfeit({s}): forfeiting {}", .{
                compaction.tree_config.name,
                beat.grid_reservation,
            });
            compaction.grid.forfeit(beat.grid_reservation);

            // Our beat is done!
            bar.beats_finished += 1;
            compaction.beat = null;
        }

        pub fn bar_blocks_unassign(
            compaction: *Compaction,
            block_pool: *Helpers.CompactionBlockFIFO,
        ) void {
            assert(compaction.beat == null);
            assert(compaction.bar != null);

            const bar = &compaction.bar.?;

            if (bar.move_table) {
                assert(bar.target_index_blocks == null);
                assert(bar.source_a_immutable_block == null);
            } else {
                bar.target_index_blocks.?.deinit(block_pool);
                bar.target_index_blocks = null;

                assert(bar.source_a_immutable_block.?.stage == .standalone);
                bar.source_a_immutable_block.?.stage = .free;
                block_pool.push(bar.source_a_immutable_block.?);
                bar.source_a_immutable_block = null;
            }
        }

        /// Apply the changes that have been accumulated in memory to the manifest, remove any
        /// tables that are now invisible, and set compaction.bar to null to indicate that it's
        /// finished.
        pub fn bar_apply_to_manifest(compaction: *Compaction) void {
            assert(compaction.beat == null);
            assert(compaction.bar != null);

            const bar = &compaction.bar.?;

            log.debug("bar_apply_to_manifest({s}): level_b={} source_values_merge_count={} " ++
                "compaction_tables_value_count={} move_table={}", .{
                compaction.tree_config.name,
                compaction.level_b,
                bar.source_values_merge_count,
                bar.compaction_tables_value_count,
                bar.move_table,
            });

            // Assert blocks have been released back to the pipeline.
            assert(bar.target_index_blocks == null);
            assert(bar.source_a_immutable_block == null);

            // Assert our input has been fully exhausted.
            assert(bar.source_values_read_count > 0);
            assert(bar.source_values_merge_count == bar.compaction_tables_value_count);
            assert(bar.source_values_read_count == bar.source_values_merge_count);

            // Assert we've written all the values we've merged.
            // TODO: Can we assert target_values_merge_count > 0 here?
            assert(bar.target_values_merge_count == bar.target_values_write_count);

            // Assert we've finished within the number of beats we were allocated.
            // TODO(metric): Track the delta between target and actual.
            if (!bar.move_table)
                assert(bar.beats_finished <= bar.beats_max.?);

            // Mark the immutable table as flushed, if we were compacting into level 0.
            if (compaction.level_b == 0 and bar.table_info_a.immutable.len == 0) {
                bar.tree.table_immutable.mutability.immutable.flushed = true;
            }

            // Each compaction's manifest updates are deferred to the end of the last
            // bar to ensure:
            // - manifest log updates are ordered deterministically relative to one another, and
            // - manifest updates are not visible until after the blocks are all on disk.
            const manifest = &bar.tree.manifest;
            const level_b = compaction.level_b;
            const snapshot_max = snapshot_max_for_table_input(bar.op_min);

            var manifest_removed_value_count: u64 = 0;
            var manifest_added_value_count: u64 = 0;

            if (bar.move_table) {
                // If no compaction is required, don't update snapshot_max.
            } else {
                // These updates MUST precede insert_table() and move_table() since they use
                // references to modify the ManifestLevel in-place.
                switch (bar.table_info_a) {
                    .immutable => {
                        if (bar.table_info_a.immutable.len == 0) {
                            manifest_removed_value_count = bar.tree.table_immutable.count();
                        }
                    },
                    .disk => |table_info| {
                        manifest_removed_value_count += table_info.table_info.value_count;
                        manifest.update_table(level_b - 1, snapshot_max, table_info);
                    },
                }
                for (bar.range_b.tables.const_slice()) |table| {
                    manifest_removed_value_count += table.table_info.value_count;
                    manifest.update_table(level_b, snapshot_max, table);
                }
            }

            for (bar.manifest_entries.slice()) |*entry| {
                switch (entry.operation) {
                    .insert_to_level_b => {
                        manifest.insert_table(level_b, &entry.table);
                        manifest_added_value_count += entry.table.value_count;
                    },
                    .move_to_level_b => {
                        manifest.move_table(level_b - 1, level_b, &entry.table);
                        manifest_removed_value_count += entry.table.value_count;
                        manifest_added_value_count += entry.table.value_count;
                    },
                }
            }
            std.log.debug("bar_apply_to_manifest({s}): manifest_removed_value_count={} " ++
                "manifest_added_value_count={} source_values_read_count={} " ++
                "target_values_merge_count={}", .{
                compaction.tree_config.name,
                manifest_removed_value_count,
                manifest_added_value_count,
                bar.source_values_read_count,
                bar.target_values_merge_count,
            });
            assert(bar.source_values_read_count == manifest_removed_value_count);
            assert(bar.target_values_merge_count == manifest_added_value_count);

            // Hide any tables that are now invisible.
            manifest.remove_invisible_tables(
                level_b,
                &.{},
                bar.range_b.key_min,
                bar.range_b.key_max,
            );
            if (level_b > 0) {
                manifest.remove_invisible_tables(
                    level_b - 1,
                    &.{},
                    bar.range_b.key_min,
                    bar.range_b.key_max,
                );
            }

            // Our bar is done!
            compaction.bar = null;
        }

        // TODO: Add benchmarks for these CPU merge methods.
        fn copy(compaction: *Compaction, source: enum { a, b }) void {
            const bar = &compaction.bar.?;
            const beat = &compaction.beat.?;
            if (source == .a) assert(!bar.drop_tombstones);

            assert(bar.table_builder.value_count < Table.layout.block_value_count_max);

            log.debug("blip_merge({s}): merging via copy({s})", .{
                compaction.tree_config.name,
                @tagName(source),
            });

            // Copy variables locally to ensure a tight loop - TODO: Actually benchmark this.
            const source_local = if (source == .a)
                beat.source_a_values.?
            else
                beat.source_b_values.?;
            const values_out = bar.table_builder.data_block_values();
            const values_out_index = bar.table_builder.value_count;

            assert(source_local.len > 0);

            const len = @min(source_local.len, values_out.len - values_out_index);
            assert(len > 0);
            stdx.copy_disjoint(
                .exact,
                Value,
                values_out[values_out_index..][0..len],
                source_local[0..len],
            );

            if (source == .a) {
                beat.source_a_values = source_local[len..];
            } else {
                beat.source_b_values = source_local[len..];
            }
            bar.table_builder.value_count += @as(u32, @intCast(len));
        }

        /// Copy values from table_a to table_b, dropping tombstones as we go.
        fn copy_drop_tombstones(compaction: *Compaction) void {
            const bar = &compaction.bar.?;
            const beat = &compaction.beat.?;
            assert(bar.drop_tombstones);

            log.debug("blip_merge({s}: merging via copy_drop_tombstones()", .{
                compaction.tree_config.name,
            });

            // Copy variables locally to ensure a tight loop - TODO: Actually benchmark this.
            const source_a_local = beat.source_a_values.?;
            const values_out = bar.table_builder.data_block_values();
            var values_in_a_index: usize = 0;

            assert(source_a_local.len > 0);
            assert(beat.source_b_values.?.len == 0);
            assert(bar.table_builder.value_count < Table.layout.block_value_count_max);

            var values_out_index = bar.table_builder.value_count;

            // Merge as many values as possible.
            while (values_in_a_index < source_a_local.len and
                values_out_index < values_out.len)
            {
                const value_a = &source_a_local[values_in_a_index];

                values_in_a_index += 1;
                // TODO: What's the impact of this check? We could invert it since Table.usage
                // is comptime known.
                if (tombstone(value_a)) {
                    assert(Table.usage != .secondary_index);
                    continue;
                }
                values_out[values_out_index] = value_a.*;
                values_out_index += 1;
            }

            // Copy variables back out.
            beat.source_a_values = source_a_local[values_in_a_index..];
            bar.table_builder.value_count = values_out_index;
        }

        /// Merge values from table_a and table_b, with table_a taking precedence. Tombstones may
        /// or may not be dropped depending on bar.drop_tombstones.
        fn merge_values(compaction: *Compaction) void {
            const bar = &compaction.bar.?;
            const beat = &compaction.beat.?;

            log.debug("blip_merge({s}: merging via merge_values()", .{compaction.tree_config.name});

            // Copy variables locally to ensure a tight loop - TODO: Actually benchmark this.
            const source_a_local = beat.source_a_values.?;
            const source_b_local = beat.source_b_values.?;

            const values_out = bar.table_builder.data_block_values();
            var source_a_index: usize = 0;
            var source_b_index: usize = 0;
            var values_out_index = bar.table_builder.value_count;

            assert(source_a_local.len > 0);
            assert(source_b_local.len > 0);
            assert(bar.table_builder.value_count < Table.layout.block_value_count_max);

            // Merge as many values as possible.
            while (source_a_index < source_a_local.len and
                source_b_index < source_b_local.len and
                values_out_index < values_out.len)
            {
                const value_a = &source_a_local[source_a_index];
                const value_b = &source_b_local[source_b_index];
                switch (std.math.order(key_from_value(value_a), key_from_value(value_b))) {
                    .lt => {
                        source_a_index += 1;
                        if (bar.drop_tombstones and
                            tombstone(value_a))
                        {
                            assert(Table.usage != .secondary_index);
                            continue;
                        }
                        values_out[values_out_index] = value_a.*;
                        values_out_index += 1;
                    },
                    .gt => {
                        source_b_index += 1;
                        values_out[values_out_index] = value_b.*;
                        values_out_index += 1;
                    },
                    .eq => {
                        source_a_index += 1;
                        source_b_index += 1;

                        if (Table.usage == .secondary_index) {
                            // Secondary index optimization --- cancel out put and remove.
                            assert(tombstone(value_a) != tombstone(value_b));
                            continue;
                        } else if (bar.drop_tombstones) {
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
            beat.source_a_values = source_a_local[source_a_index..];
            beat.source_b_values = source_b_local[source_b_index..];
            bar.table_builder.value_count = values_out_index;
        }
    };
}

pub fn snapshot_max_for_table_input(op_min: u64) u64 {
    return snapshot_min_for_table_output(op_min) - 1;
}

pub fn snapshot_min_for_table_output(op_min: u64) u64 {
    assert(op_min > 0);
    assert(op_min % @divExact(constants.lsm_batch_multiple, 2) == 0);
    return op_min + @divExact(constants.lsm_batch_multiple, 2);
}

/// Returns the first op of the compaction (Compaction.op_min) for a given op/beat.
///
/// After this compaction finishes:
/// - `op_min + half_bar_beat_count - 1` will be the input tables' snapshot_max.
/// - `op_min + half_bar_beat_count` will be the output tables' snapshot_min.
///
/// Each half-bar has a separate op_min (for deriving the output snapshot_min) instead of each full
/// bar because this allows the output tables of the first half-bar's compaction to be prefetched
/// against earlier — hopefully while they are still warm in the cache from being written.
///
///
/// These charts depict the commit/compact ops over a series of
/// commits and compactions (with lsm_batch_multiple=8).
///
/// Legend:
///
///   ┼   full bar (first half-bar start)
///   ┬   half bar (second half-bar start)
///       This is incremented at the end of each compact().
///   .   op is in mutable table (in memory)
///   ,   op is in immutable table (in memory)
///   #   op is on disk
///   ✓   checkpoint() may follow compact()
///
///   0 2 4 6 8 0 2 4 6
///   ┼───┬───┼───┬───┼
///   .       ╷       ╷     init(superblock.commit_min=0)⎤ Compaction is effectively a noop for the
///   ..      ╷       ╷     commit;compact( 1) start/end ⎥ first bar because there are no tables on
///   ...     ╷       ╷     commit;compact( 2) start/end ⎥ disk yet, and no immutable table to
///   ....    ╷       ╷     commit;compact( 3) start/end ⎥ flush.
///   .....   ╷       ╷     commit;compact( 4) start/end ⎥
///   ......  ╷       ╷     commit;compact( 5) start/end ⎥ This applies:
///   ....... ╷       ╷     commit;compact( 6) start/end ⎥ - when the LSM is starting on a freshly
///   ........╷       ╷     commit;compact( 7) start    ⎤⎥   formatted data file, and also
///   ,,,,,,,,.       ╷  ✓         compact( 7)       end⎦⎦ - when the LSM is recovering from a crash
///   ,,,,,,,,.       ╷     commit;compact( 8) start/end     (see below).
///   ,,,,,,,,..      ╷     commit;compact( 9) start/end
///   ,,,,,,,,...     ╷     commit;compact(10) start/end
///   ,,,,,,,,....    ╷     commit;compact(11) start/end
///   ,,,,,,,,.....   ╷     commit;compact(12) start/end
///   ,,,,,,,,......  ╷     commit;compact(13) start/end
///   ,,,,,,,,....... ╷     commit;compact(14) start/end
///   ,,,,,,,,........╷     commit;compact(15) start    ⎤
///   ########,,,,,,,,╷  ✓         compact(15)       end⎦
///   ########,,,,,,,,.     commit;compact(16) start/end
///   ┼───┬───┼───┬───┼
///   0 2 4 6 8 0 2 4 6
///   ┼───┬───┼───┬───┼                                    Recover with a checkpoint taken at op 15.
///   ########        ╷     init(superblock.commit_min=7)  At op 15, ops 8…15 are in memory, so they
///   ########.       ╷     commit        ( 8) start/end ⎤ were dropped by the crash.
///   ########..      ╷     commit        ( 9) start/end ⎥
///   ########...     ╷     commit        (10) start/end ⎥ But compaction is not run for ops 8…15
///   ########....    ╷     commit        (11) start/end ⎥ because it was already performed
///   ########.....   ╷     commit        (12) start/end ⎥ before the checkpoint.
///   ########......  ╷     commit        (13) start/end ⎥
///   ########....... ╷     commit        (14) start/end ⎥ We can begin to compact again at op 16,
///   ########........╷     commit        (15) start    ⎤⎥ because those compactions (if previously
///   ########,,,,,,,,╷  ✓                (15)       end⎦⎦ performed) are not included in the
///   ########,,,,,,,,.     commit;compact(16) start/end   checkpoint.
///   ┼───┬───┼───┬───┼
///   0 2 4 6 8 0 2 4 6
///
/// Notice how in the checkpoint recovery example above, we are careful not to `compact(op)` twice
/// for any op (even if we crash/recover), since that could lead to differences between replicas'
/// storage. The last bar of `commit()`s is always only in memory, so it is safe to repeat.
pub fn compaction_op_min(op: u64) u64 {
    assert(op >= half_bar_beat_count);
    return op - op % half_bar_beat_count;
}
