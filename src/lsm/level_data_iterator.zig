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
const LevelIndexIteratorType = @import("level_index_iterator.zig").LevelIndexIteratorType;

/// A LevelDataIterator iterates the data blocks of every table in a key range in ascending key order.
pub fn LevelDataIteratorType(comptime Table: type, comptime Storage: type, comptime tree_name: [:0]const u8) type {
    return struct {
        const LevelDataIterator = @This();

        const Key = Table.Key;
        const Grid = GridType(Storage);
        const BlockPtr = Grid.BlockPtr;
        const BlockPtrConst = Grid.BlockPtrConst;
        const Manifest = ManifestType(Table, Storage, tree_name);
        const TableInfo = Manifest.TableInfo;
        const LevelIndexIterator = LevelIndexIteratorType(Table, Storage, tree_name);
        const TableDataIterator = TableDataIteratorType(Storage);

        pub const Context = struct {
            grid: *Grid,
            manifest: *Manifest,
            level: u8,
            snapshot: u64,
            key_min: Key,
            key_max: Key,
            read_name: [*:0]const u8,
        };

        pub const IndexCallback = fn (
            it: *LevelDataIterator,
            table_info: TableInfo,
            index_block: BlockPtrConst,
        ) void;
        pub const DataCallback = fn (it: *LevelDataIterator, data_block: ?BlockPtrConst) void;
        pub const Callback = struct {
            on_index: IndexCallback,
            on_data: DataCallback,
        };

        /// Passed by `start`.
        context: Context,

        /// Internal state.
        level_index_iterator: LevelIndexIterator,
        table_data_iterator: TableDataIterator,

        // Local copy of index block, for use in `table_data_iterator`.
        index_block: BlockPtr,

        callback: union(enum) {
            none,
            level_next: Callback,
            table_next: Callback,
        },

        pub fn init(
            allocator: mem.Allocator,
        ) !LevelDataIterator {
            var level_index_iterator = try LevelIndexIterator.init(allocator);
            errdefer level_index_iterator.deinit(allocator);

            var table_data_iterator = try TableDataIterator.init(allocator);
            errdefer table_data_iterator.deinit(allocator);

            const index_block = try alloc_block(allocator);
            errdefer allocator.free(index_block);

            return LevelDataIterator{
                .context = undefined,
                .level_index_iterator = level_index_iterator,
                .table_data_iterator = table_data_iterator,
                .index_block = index_block,
                .callback = .none,
            };
        }

        pub fn deinit(it: *LevelDataIterator, allocator: mem.Allocator) void {
            allocator.free(it.index_block);
            it.table_data_iterator.deinit(allocator);
            it.level_index_iterator.deinit(allocator);
            it.* = undefined;
        }

        pub fn start(
            it: *LevelDataIterator,
            context: Context,
        ) void {
            assert(it.callback == .none);
            it.* = .{
                .context = context,
                .level_index_iterator = it.level_index_iterator,
                .table_data_iterator = it.table_data_iterator,
                .index_block = it.index_block,
                .callback = .none,
            };
            it.level_index_iterator.start(.{
                .grid = context.grid,
                .manifest = context.manifest,
                .level = context.level,
                .snapshot = context.snapshot,
                .key_min = context.key_min,
                .key_max = context.key_max,
                .direction = .ascending,
                .read_name = context.read_name,
            });
            it.table_data_iterator.start(.{
                .grid = context.grid,
                .addresses = &.{},
                .checksums = &.{},
                .read_name = context.read_name,
            });
        }

        /// *May* call `callback.on_index` once with the next index block,
        /// if we've finished the previous index block,
        /// or with null if there are no more index blocks in the range.
        ///
        /// *Will* call `callback.on_data` once with the next data block,
        /// or with null if there are no more data blocks in the range.
        ///
        /// For both callbacks, the block is only valid for the duration of the callback.
        pub fn next(it: *LevelDataIterator, callback: Callback) void {
            assert(it.callback == .none);

            if (it.table_data_iterator.empty()) {
                // Refill `table_data_iterator` before calling `table_next`.
                it.callback = .{ .level_next = callback };
                it.level_index_iterator.next(on_level_next);
            } else {
                it.table_next(callback);
            }
        }

        fn on_level_next(
            level_index_iterator: *LevelIndexIterator,
            table_info: ?TableInfo,
            index_block: ?BlockPtrConst,
        ) void {
            const it = @fieldParentPtr(LevelDataIterator, "level_index_iterator", level_index_iterator);
            assert(it.table_data_iterator.empty());
            const callback = it.callback.level_next;
            it.callback = .none;

            if (index_block) |block| {
                // `index_block` is only valid for this callback, so copy it's contents.
                // TODO(jamii) This copy can be avoided if we bypass the cache.
                stdx.copy_disjoint(.exact, u8, it.index_block, block);

                // TODO(jamii):
                // For compacion we want all data blocks from these tables.
                // For range queries, we only want data blocks that might contain the search range.
                // So filter those out here.
                it.table_data_iterator.start(.{
                    .grid = it.context.grid,
                    .addresses = Table.index_data_addresses_used(it.index_block),
                    .checksums = Table.index_data_checksums_used(it.index_block),
                    .read_name = it.context.read_name,
                });

                const on_index = callback.on_index;
                on_index(it, table_info.?, block);
            } else {
                // If there are no more index blocks, we can just leave `table_data_iterator` empty.
            }

            it.table_next(callback);
        }

        fn table_next(it: *LevelDataIterator, callback: Callback) void {
            assert(it.callback == .none);

            it.callback = .{ .table_next = callback };
            it.table_data_iterator.next(on_table_next);
        }

        fn on_table_next(table_data_iterator: *TableDataIterator, data_block: ?Grid.BlockPtrConst) void {
            const it = @fieldParentPtr(LevelDataIterator, "table_data_iterator", table_data_iterator);
            const callback = it.callback.table_next;
            it.callback = .none;

            const on_data = callback.on_data;
            on_data(it, data_block);
        }
    };
}
