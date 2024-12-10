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

            const index: usize = switch (it.context.direction) {
                .ascending => 0,
                .descending => it.context.addresses.len - 1,
            };

            assert(it.context.checksums[index].padding == 0);

            it.callback = callback;
            it.context.grid.read_block(
                .{ .from_local_or_global_storage = read_block_callback },
                &it.read,
                it.context.addresses[index],
                it.context.checksums[index].value,
                .{ .cache_read = true, .cache_write = true },
            );
        }

        fn read_block_callback(read: *Grid.Read, block: BlockPtrConst) void {
            const it: *TableValueIterator = @fieldParentPtr("read", read);
            assert(it.callback != null);
            assert(it.context.addresses.len == it.context.checksums.len);

            const callback = it.callback.?;
            it.callback = null;

            switch (it.context.direction) {
                .ascending => {
                    const header = schema.header_from_block(block);
                    assert(header.address == it.context.addresses[0]);
                    assert(header.checksum == it.context.checksums[0].value);

                    it.context.addresses = it.context.addresses[1..];
                    it.context.checksums = it.context.checksums[1..];
                },
                .descending => {
                    const index_last = it.context.checksums.len - 1;
                    const header = schema.header_from_block(block);
                    assert(header.address == it.context.addresses[index_last]);
                    assert(header.checksum == it.context.checksums[index_last].value);

                    it.context.addresses = it.context.addresses[0..index_last];
                    it.context.checksums = it.context.checksums[0..index_last];
                },
            }

            assert(it.context.addresses.len == it.context.checksums.len);
            callback(it, block);
        }
    };
}
