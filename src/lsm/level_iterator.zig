const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const constants = @import("../constants.zig");

const stdx = @import("../stdx.zig");
const ManifestType = @import("manifest.zig").ManifestType;
const alloc_block = @import("grid.zig").alloc_block;
const GridType = @import("grid.zig").GridType;
const TableDataIteratorType = @import("table_data_iterator.zig").TableDataIteratorType;

// Iterates over the data blocks in a level B table. References to the level B
// tables to be iterated over are passed via the `tables` field in the context,
// in the start() function.
// * Assumption: This iterator works under the assumption that the references to
// the tables are stable. For references fetched from the ManifestLevel, this
// assumption only holds true till move_table & insert_table are called as a part
// of apply_to_manifest. This is because move_table & insert_table are both operations
// that insert/remove tables from the ManifestLevel segmented arrays, which could cause
// rearrangements in the memory layout of the arrays. These rearrangements could
// cause our table references to get invalidated.
pub fn LevelIteratorType(comptime Table: type, comptime Storage: type) type {
    return struct {
        const LevelBDataIterator = @This();

        const Grid = GridType(Storage);
        const BlockPtr = Grid.BlockPtr;
        const BlockPtrConst = Grid.BlockPtrConst;
        const Manifest = ManifestType(Table, Storage);
        const TableInfo = Manifest.TableInfo;
        const TableDataIterator = TableDataIteratorType(Storage);

        pub const Context = struct { grid: *Grid, level: u8, snapshot: u64, tables: std.BoundedArray(
            *TableInfo,
            constants.lsm_growth_factor,
        ) };

        pub const IndexCallback = fn (
            it: *LevelBDataIterator,
            index_block: BlockPtrConst,
        ) void;
        pub const DataCallback = fn (it: *LevelBDataIterator, data_block: ?BlockPtrConst) void;
        pub const Callback = struct {
            on_index: IndexCallback,
            on_data: DataCallback,
        };

        /// Passed by `start`.
        context: Context,

        /// Internal state.
        table_data_iterator: TableDataIterator,
        table_index: usize = 0,
        // Local copy of index block, for use in `table_data_iterator`.
        index_block: BlockPtr,
        read: Grid.Read = undefined,
        next_tick: Grid.NextTick = undefined,

        callback: union(enum) {
            none,
            level_next: Callback,
            table_next: Callback,
        },

        pub fn init(
            allocator: mem.Allocator,
        ) !LevelBDataIterator {
            var table_data_iterator = try TableDataIterator.init(allocator);
            errdefer table_data_iterator.deinit(allocator);

            const index_block = try alloc_block(allocator);
            errdefer allocator.free(index_block);

            return LevelBDataIterator{
                .context = undefined,
                .table_data_iterator = table_data_iterator,
                .index_block = index_block,
                .callback = .none,
            };
        }

        pub fn deinit(it: *LevelBDataIterator, allocator: mem.Allocator) void {
            allocator.free(it.index_block);
            it.table_data_iterator.deinit(allocator);
            it.* = undefined;
        }

        pub fn start(
            it: *LevelBDataIterator,
            context: Context,
        ) void {
            assert(it.callback == .none);
            it.* = .{
                .context = context,
                .table_data_iterator = it.table_data_iterator,
                .index_block = it.index_block,
                .callback = .none,
            };
            it.table_data_iterator.start(.{
                .grid = context.grid,
                .addresses = &.{},
                .checksums = &.{},
            });
        }

        /// *Will* call `callback.on_data` once with the next data block,
        /// or with null if there are no more data blocks in the range.
        pub fn next(it: *LevelBDataIterator, callback: Callback) void {
            assert(it.callback == .none);
            // If this is the last table that we're iterating and it.table_data_iterator.empty()
            // is true, it.table_data_iterator.next takes care of calling callback.on_data with
            // a null data block.
            if (it.table_data_iterator.empty() and it.table_index < it.context.tables.len) {
                // Refill `table_data_iterator` before calling `table_next`.
                const table_info = it.context.tables.slice()[it.table_index];
                it.callback = .{ .level_next = callback };
                it.context.grid.read_block(
                    on_level_next,
                    &it.read,
                    table_info.address,
                    table_info.checksum,
                    .index,
                );
            } else {
                it.table_next(callback);
            }
        }

        fn on_level_next(
            read: *Grid.Read,
            index_block: ?BlockPtrConst,
        ) void {
            const it = @fieldParentPtr(LevelBDataIterator, "read", read);
            assert(it.table_data_iterator.empty());
            const callback = it.callback.level_next;
            it.callback = .none;

            if (index_block) |block| {
                // `index_block` is only valid for this callback, so copy it's contents.
                // TODO(jamii) This copy can be avoided if we bypass the cache.
                stdx.copy_disjoint(.exact, u8, it.index_block, block);
                it.table_data_iterator.start(.{
                    .grid = it.context.grid,
                    .addresses = Table.index_data_addresses_used(it.index_block),
                    .checksums = Table.index_data_checksums_used(it.index_block),
                });
                callback.on_index(it, block);
            }
            it.table_index += 1;
            it.table_next(callback);
        }

        fn table_next(it: *LevelBDataIterator, callback: Callback) void {
            assert(it.callback == .none);

            it.callback = .{ .table_next = callback };
            it.table_data_iterator.next(on_table_next);
        }

        fn on_table_next(table_data_iterator: *TableDataIterator, data_block: ?Grid.BlockPtrConst) void {
            const it = @fieldParentPtr(LevelBDataIterator, "table_data_iterator", table_data_iterator);
            const callback = it.callback.table_next;
            it.callback = .none;
            callback.on_data(it, data_block);
        }
    };
}