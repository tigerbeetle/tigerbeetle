const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;
const schema = @import("schema.zig");

const constants = @import("../constants.zig");

const stdx = @import("../stdx.zig");
const ManifestType = @import("manifest.zig").ManifestType;
const allocate_block = @import("../vsr/grid.zig").allocate_block;
const GridType = @import("../vsr/grid.zig").GridType;
const BlockPtr = @import("../vsr/grid.zig").BlockPtr;
const BlockPtrConst = @import("../vsr/grid.zig").BlockPtrConst;
const Direction = @import("../direction.zig").Direction;
const TableValueIteratorType = @import("table_data_iterator.zig").TableValueIteratorType;

//TODO: rename to 'scan_table_index_iterator.zig'.

/// A TableIndexIterator loads the `index_block` and iterates over all `value_blocks`
/// that match the range query (specified by the callback return `ValueBlocksToLoad`),
/// in ascending or descending key order.
pub fn TableIndexIteratorType(comptime Table: type, comptime Storage: type) type {
    return struct {
        const TableIndexIterator = @This();

        const Grid = GridType(Storage);
        const Manifest = ManifestType(Table, Storage);
        const TableInfo = Manifest.TableInfo;
        const TableValueIterator = TableValueIteratorType(Storage);

        pub const Context = struct {
            grid: *Grid,
            level: u8,
            snapshot: u64,
            /// The `index_block` address and checksum, or `null` to initialize an empty iterator.
            index_block: ?struct {
                // TODO: This buffer can be avoided if we bypass the cache.
                buffer: BlockPtr,
                address: u64,
                checksum: u128,
            },

            direction: Direction,
        };

        pub const ValueBlocksToLoad = union(enum) {
            none,
            range: struct { start: usize, end: usize },
        };
        pub const IndexCallback = *const fn (
            it: *TableIndexIterator,
            index_block: BlockPtrConst,
        ) ValueBlocksToLoad;
        pub const DataCallback = *const fn (
            it: *TableIndexIterator,
            value_block: ?BlockPtrConst,
        ) void;
        pub const Callback = struct {
            on_index: IndexCallback,
            on_value: DataCallback,
        };

        /// Passed by `init`.
        context: Context,

        state: union(enum) {
            idle,
            iterating_values: TableValueIterator,
        },

        read: Grid.Read = undefined,
        next_tick: Grid.NextTick = undefined,

        callback: ?Callback,

        pub fn init(
            it: *TableIndexIterator,
            context: Context,
        ) void {
            assert(context.level < constants.lsm_levels);
            it.* = .{
                .context = context,
                .state = .idle,
                .callback = null,
            };
        }

        /// *Will* call `callback.on_data` once with the next data block,
        /// or with null if there are no more data blocks in the range.
        pub fn next(it: *TableIndexIterator, callback: Callback) void {
            assert(it.callback == null);
            switch (it.state) {
                .idle => {
                    if (it.context.index_block) |index_block| {
                        // Reading the index blocks from the table info:
                        it.callback = callback;
                        it.context.grid.read_block(
                            .{ .from_local_or_global_storage = index_block_callback },
                            &it.read,
                            index_block.address,
                            index_block.checksum,
                            .{ .cache_read = true, .cache_write = true },
                        );
                    } else {
                        // If there's no table_info to iterate, then using an empty iterator that
                        // will call the callback with a null value block.
                        var table_value_iterator: TableValueIterator = undefined;
                        table_value_iterator.init(.{
                            .grid = it.context.grid,
                            .addresses = &.{},
                            .checksums = &.{},
                            .direction = it.context.direction,
                        });

                        it.state = .{
                            .iterating_values = table_value_iterator,
                        };
                        it.value_next(callback);
                    }
                },
                .iterating_values => it.value_next(callback),
            }
        }

        fn index_block_callback(
            read: *Grid.Read,
            index_block: BlockPtrConst,
        ) void {
            const it: *TableIndexIterator = @fieldParentPtr("read", read);
            assert(it.state == .idle);
            assert(it.callback != null);
            assert(it.context.index_block != null);

            const callback = it.callback.?;
            it.callback = null;

            // `index_block` is only valid for this callback, so copy it's contents.
            const buffer = it.context.index_block.?.buffer;
            stdx.copy_disjoint(.exact, u8, buffer, index_block);

            const blocks_to_load = callback.on_index(it, buffer);

            const index_schema = schema.TableIndex.from(buffer);
            const data_addresses = index_schema.data_addresses_used(buffer);
            const data_checksums = index_schema.data_checksums_used(buffer);
            assert(data_addresses.len == data_checksums.len);

            switch (blocks_to_load) {
                // In case of no data blocks to load, it's safe to call `on_value` synchronously
                // since it's already being called from a callback.
                .none => callback.on_value(it, null),
                .range => |value| {
                    var table_value_iterator: TableValueIterator = undefined;
                    table_value_iterator.init(.{
                        .grid = it.context.grid,
                        .addresses = data_addresses[value.start..value.end],
                        .checksums = data_checksums[value.start..value.end],
                        .direction = it.context.direction,
                    });

                    it.state = .{ .iterating_values = table_value_iterator };
                    it.value_next(callback);
                },
            }
        }

        fn value_next(it: *TableIndexIterator, callback: Callback) void {
            assert(it.state == .iterating_values);
            assert(it.callback == null);

            it.callback = callback;
            it.state.iterating_values.next(value_next_callback);
        }

        fn value_next_callback(
            table_value_iterator: *TableValueIterator,
            data_block: ?BlockPtrConst,
        ) void {
            // TODO(zig): Replace state with `?TableIndexIterator` when `@fieldParentPtr` can
            // be applied to resolve a pointer to a nullable field.
            const State = std.meta.FieldType(TableIndexIterator, .state);
            const state: *State = @fieldParentPtr(
                @tagName(.iterating_values),
                table_value_iterator,
            );
            const it: *TableIndexIterator = @fieldParentPtr("state", state);

            assert(it.callback != null);
            const callback = it.callback.?;
            it.callback = null;

            callback.on_value(it, data_block);
        }
    };
}
