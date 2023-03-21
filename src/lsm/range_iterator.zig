const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const constants = @import("../constants.zig");

const stdx = @import("../stdx.zig");
const ManifestType = @import("manifest.zig").ManifestType;
const alloc_block = @import("grid.zig").alloc_block;
const GridType = @import("grid.zig").GridType;
const TableIteratorType = @import("table_iterator.zig").TableIteratorType;
const LevelIteratorType = @import("level_iterator.zig").LevelIteratorType;

/// A RangeIterator iterates the data blocks of every table in a key range.
pub fn RangeIteratorType(comptime Table: type, comptime Storage: type) type {
    return struct {
        const RangeIterator = @This();

        const Key = Table.Key;
        const Grid = GridType(Storage);
        const BlockPtr = Grid.BlockPtr;
        const BlockPtrConst = Grid.BlockPtrConst;
        const Manifest = ManifestType(Table, Storage);
        const TableInfo = Manifest.TableInfo;
        const LevelIterator = LevelIteratorType(Table, Storage);
        const TableIterator = TableIteratorType(Storage);

        pub const Context = struct {
            grid: *Grid,
            manifest: *Manifest,
            level: u8,
            snapshot: u64,
            key_min: Key,
            key_max: Key,
        };

        pub const IndexCallback = fn (
            it: *RangeIterator,
            table_info: ?TableInfo,
            index_block: ?BlockPtrConst,
        ) void;
        pub const DataCallback = fn (it: *RangeIterator, data_block: ?BlockPtrConst) void;
        pub const Callback = struct {
            on_index: IndexCallback,
            on_data: DataCallback,
        };

        /// Passed by `start`.
        context: Context,

        /// Internal state.
        level_iterator: LevelIterator,
        table_iterator: TableIterator,

        callback: union(enum) {
            none,
            level_next: Callback,
            table_next: Callback,
        },

        pub fn init(
            allocator: mem.Allocator,
        ) !RangeIterator {
            _ = allocator; // TODO(jamii) Will need this soon for pipelining.
            return RangeIterator{
                .context = undefined,
                .level_iterator = try LevelIterator.init(allocator),
                .table_iterator = try TableIterator.init(allocator),
                .callback = .none,
            };
        }

        pub fn deinit(it: *RangeIterator, allocator: mem.Allocator) void {
            it.table_iterator.deinit(allocator);
            it.level_iterator.deinit(allocator);
            it.* = undefined;
        }

        pub fn start(
            it: *RangeIterator,
            context: Context,
        ) void {
            assert(it.callback == .none);
            it.* = .{
                .context = context,
                .level_iterator = it.level_iterator,
                .table_iterator = it.table_iterator,
                .callback = .none,
            };
            it.level_iterator.start(.{
                .grid = context.grid,
                .manifest = context.manifest,
                .level = context.level,
                .snapshot = context.snapshot,
                .key_min = context.key_min,
                .key_max = context.key_max,
                .direction = .ascending,
            });
            it.table_iterator.start(.{
                .grid = context.grid,
                .addresses = &.{},
                .checksums = &.{},
            });
        }

        /// *May* call `callback.on_index` once with the next index block,
        /// if we've finished the previous index block,
        /// or with null if there are no more index blocks in the range.
        ///
        /// *Will* call `callback.on_data` once with the next data block,
        /// or with null if there are no more data blocks in the range.
        pub fn next(it: *RangeIterator, callback: Callback) void {
            assert(it.callback == .none);

            if (it.table_iterator.empty()) {
                // Refill `table_iterator` before calling `table_next`.
                it.callback = .{ .level_next = callback };
                it.level_iterator.next(on_level_next);
            } else {
                it.table_next(callback);
            }
        }

        fn on_level_next(
            level_iterator: *LevelIterator,
            table_info: ?TableInfo,
            index_block: ?BlockPtrConst,
        ) void {
            const it = @fieldParentPtr(RangeIterator, "level_iterator", level_iterator);
            assert(it.callback == .level_next);
            const callback = it.callback.level_next;
            it.callback = .none;

            if (index_block) |block| {
                // TODO(jamii):
                // For compacion we want all data blocks from these tables.
                // For range queries, we only want data blocks that might contain the search range.
                // So filter those out here.
                it.table_iterator.start(.{
                    .grid = it.context.grid,
                    .addresses = Table.index_data_addresses_used(block),
                    .checksums = Table.index_data_checksums_used(block),
                });

                const on_index = callback.on_index;
                on_index(it, table_info.?, index_block.?);
            } else {
                // If there are no more index blocks, we can just leave `table_iterator` empty.
            }

            it.table_next(callback);
        }

        fn table_next(it: *RangeIterator, callback: Callback) void {
            assert(it.callback == .none);

            it.callback = .{ .table_next = callback };
            it.table_iterator.next(on_table_next);
        }

        fn on_table_next(table_iterator: *TableIterator, data_block: ?Grid.BlockPtrConst) void {
            const it = @fieldParentPtr(RangeIterator, "table_iterator", table_iterator);
            assert(it.callback == .table_next);
            const callback = it.callback.table_next;
            it.callback = .none;

            const on_data = callback.on_data;
            on_data(it, data_block);
        }
    };
}
