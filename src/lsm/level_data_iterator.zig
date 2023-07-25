const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;
const schema = @import("schema.zig");

const constants = @import("../constants.zig");

const stdx = @import("../stdx.zig");
const ManifestType = @import("manifest.zig").ManifestType;
const allocate_block = @import("grid.zig").allocate_block;
const GridType = @import("grid.zig").GridType;
const Direction = @import("direction.zig").Direction;
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
pub fn LevelTableValueBlockIteratorType(comptime Table: type, comptime Storage: type) type {
    return struct {
        const LevelTableValueBlockIterator = @This();

        const Grid = GridType(Storage);
        const BlockPtr = Grid.BlockPtr;
        const BlockPtrConst = Grid.BlockPtrConst;
        const Manifest = ManifestType(Table, Storage);
        const TableInfo = Manifest.TableInfo;
        const TableDataIterator = TableDataIteratorType(Storage);

        pub const Context = struct {
            grid: *Grid,
            level: u8,
            snapshot: u64,
            index_block: BlockPtr,

            // `tables` contains TableInfo references from ManifestLevel.
            tables: union(enum) {
                compaction: []const Manifest.TableInfoReference,
                scan: ?*const Manifest.TableInfo,

                inline fn len(self: @This()) usize {
                    return switch (self) {
                        .compaction => |slice| slice.len,
                        .scan => |table_info| @boolToInt(table_info != null),
                    };
                }

                inline fn get(self: @This(), index: usize) *const Manifest.TableInfo {
                    return switch (self) {
                        .compaction => |slice| slice[index].table_info,
                        .scan => |table_info| blk: {
                            assert(index == 0);
                            break :blk table_info.?;
                        },
                    };
                }
            },

            direction: Direction,
        };

        pub const DataBlockAddresses = struct {
            /// Table data block addresses.
            addresses: []const u64,
            /// Table data block checksums.
            checksums: []const u128,
        };
        pub const IndexCallback = *const fn (it: *LevelTableValueBlockIterator) DataBlockAddresses;
        pub const DataCallback = *const fn (it: *LevelTableValueBlockIterator, data_block: ?BlockPtrConst) void;
        pub const Callback = struct {
            on_index: IndexCallback,
            on_data: DataCallback,
        };

        /// Passed by `start`.
        context: Context,

        /// Internal state.
        table_data_iterator: TableDataIterator,
        table_index: usize = 0,

        read: Grid.Read = undefined,
        next_tick: Grid.NextTick = undefined,

        callback: union(enum) {
            none,
            level_next: Callback,
            table_next: Callback,
        },

        pub fn init() LevelTableValueBlockIterator {
            var table_data_iterator = TableDataIterator.init();
            errdefer table_data_iterator.deinit();

            return LevelTableValueBlockIterator{
                .context = undefined,
                .table_data_iterator = table_data_iterator,
                .callback = .none,
            };
        }

        pub fn deinit(it: *LevelTableValueBlockIterator) void {
            it.table_data_iterator.deinit();
            it.* = undefined;
        }

        pub fn reset(it: *LevelTableValueBlockIterator) void {
            it.table_data_iterator.reset();
            it.* = .{
                .context = undefined,
                .table_data_iterator = it.table_data_iterator,
                .callback = .none,
            };
        }

        pub fn start(
            it: *LevelTableValueBlockIterator,
            context: Context,
        ) void {
            assert(it.callback == .none);
            assert(context.level < constants.lsm_levels);

            it.* = .{
                .context = context,
                .table_data_iterator = it.table_data_iterator,
                .callback = .none,
            };
            it.table_data_iterator.start(.{
                .grid = context.grid,
                .addresses = &.{},
                .checksums = &.{},
                .direction = context.direction,
            });
        }

        /// *Will* call `callback.on_data` once with the next data block,
        /// or with null if there are no more data blocks in the range.
        pub fn next(it: *LevelTableValueBlockIterator, callback: Callback) void {
            assert(it.callback == .none);
            // If this is the last table that we're iterating and it.table_data_iterator.empty()
            // is true, it.table_data_iterator.next takes care of calling callback.on_data with
            // a null data block.
            if (it.table_data_iterator.empty() and it.table_index < it.context.tables.len()) {
                // Refill `table_data_iterator` before calling `table_next`.
                const table_info = it.context.tables.get(it.table_index);
                it.callback = .{ .level_next = callback };
                it.context.grid.read_block_from_cache_or_storage(
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
            index_block: BlockPtrConst,
        ) void {
            const it = @fieldParentPtr(LevelTableValueBlockIterator, "read", read);
            assert(it.table_data_iterator.empty());

            const callback = it.callback.level_next;
            it.callback = .none;
            // `index_block` is only valid for this callback, so copy it's contents.
            // TODO(jamii) This copy can be avoided if we bypass the cache.
            stdx.copy_disjoint(.exact, u8, it.context.index_block, index_block);
            const data_block_addresses = callback.on_index(it);
            assert(data_block_addresses.addresses.len == data_block_addresses.checksums.len);
            it.table_data_iterator.start(.{
                .grid = it.context.grid,
                .addresses = data_block_addresses.addresses,
                .checksums = data_block_addresses.checksums,
                .direction = it.context.direction,
            });

            it.table_index += 1;
            it.table_next(callback);
        }

        fn table_next(it: *LevelTableValueBlockIterator, callback: Callback) void {
            assert(it.callback == .none);
            it.callback = .{ .table_next = callback };
            it.table_data_iterator.next(on_table_next);
        }

        fn on_table_next(
            table_data_iterator: *TableDataIterator,
            data_block: ?Grid.BlockPtrConst,
        ) void {
            const it = @fieldParentPtr(
                LevelTableValueBlockIterator,
                "table_data_iterator",
                table_data_iterator,
            );
            const callback = it.callback.table_next;
            it.callback = .none;
            callback.on_data(it, data_block);
        }
    };
}
