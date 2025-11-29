const std = @import("std");
const assert = std.debug.assert;

const schema = @import("schema.zig");

const GridType = @import("../vsr/grid.zig").GridType;
const BlockPtrConst = @import("../vsr/grid.zig").BlockPtrConst;
const Direction = @import("../direction.zig").Direction;

/// A TableValueIterator iterates a table's value blocks in ascending or descending key order.
pub fn TableValueIteratorType(comptime Storage: type) type {
    return struct {
        const TableValueIterator = @This();

        const Grid = GridType(Storage);

        pub const Callback = *const fn (it: *TableValueIterator, value_block: BlockPtrConst) void;

        pub const Context = struct {
            grid: *Grid,
            /// Table value block addresses.
            addresses: []const u64,
            /// Table value block checksums.
            checksums: []const schema.Checksum,
            direction: Direction,
        };

        context: Context,
        callback: ?Callback,
        read: Grid.Read,

        pub fn init(
            it: *TableValueIterator,
            context: Context,
        ) void {
            assert(context.addresses.len == context.checksums.len);
            it.* = .{
                .context = context,
                .callback = null,
                .read = undefined,
            };
        }

        pub fn empty(it: *const TableValueIterator) bool {
            assert(it.context.addresses.len == it.context.checksums.len);
            return it.context.addresses.len == 0;
        }

        /// Calls `callback` with the next value block.
        /// Not expected to be called in an empty iterator.
        /// The block is only valid for the duration of the callback.
        pub fn next_value_block(it: *TableValueIterator, callback: Callback) void {
            assert(it.callback == null);
            assert(!it.empty());

            const address = it.context.direction.slice_peek(it.context.addresses).*;
            const checksum = it.context.direction.slice_peek(it.context.checksums).*;
            assert(checksum.padding == 0);

            it.callback = callback;
            it.context.grid.read_block(
                .{ .from_local_or_global_storage = read_block_callback },
                &it.read,
                address,
                checksum.value,
                .{ .cache_read = true, .cache_write = true },
            );
        }

        fn read_block_callback(read: *Grid.Read, block: BlockPtrConst) void {
            const it: *TableValueIterator = @fieldParentPtr("read", read);
            assert(it.callback != null);
            assert(it.context.addresses.len == it.context.checksums.len);

            const callback = it.callback.?;
            it.callback = null;

            const address, it.context.addresses =
                it.context.direction.slice_pop(it.context.addresses);
            const checksum, it.context.checksums =
                it.context.direction.slice_pop(it.context.checksums);

            const header = schema.header_from_block(block);
            assert(header.address == address);
            assert(header.checksum == checksum.value);
            assert(it.context.addresses.len == it.context.checksums.len);
            callback(it, block);
        }
    };
}
