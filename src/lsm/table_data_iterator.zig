const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const schema = @import("schema.zig");

const stdx = @import("../stdx.zig");
const GridType = @import("../vsr/grid.zig").GridType;
const BlockPtrConst = @import("../vsr/grid.zig").BlockPtrConst;
const Direction = @import("../direction.zig").Direction;

/// A TableDataIterator iterates a table's data blocks in ascending key order.
pub fn TableDataIteratorType(comptime Storage: type) type {
    return struct {
        const TableDataIterator = @This();

        const Grid = GridType(Storage);

        pub const Callback = *const fn (it: *TableDataIterator, data_block: ?BlockPtrConst) void;

        pub const Context = struct {
            grid: *Grid,
            /// Table data block addresses.
            addresses: []const u64,
            /// Table data block checksums.
            checksums: []const schema.Checksum,
            direction: Direction,
        };

        context: Context,

        callback: union(enum) {
            none,
            read: Callback,
            next_tick: Callback,
        },

        read: Grid.Read,
        next_tick: Grid.NextTick,

        pub fn init() TableDataIterator {
            var it: TableDataIterator = undefined;
            it.reset();
            return it;
        }

        pub fn deinit(it: *TableDataIterator) void {
            it.* = undefined;
        }

        pub fn reset(it: *TableDataIterator) void {
            it.* = .{
                .context = .{
                    .grid = undefined,
                    // The zero-init here is important.
                    // In other places we assume that we can call `next` on a fresh TableDataIterator
                    // and get `null` rather than UB.
                    .addresses = &.{},
                    .checksums = &.{},
                    .direction = .ascending,
                },
                .callback = .none,
                .read = undefined,
                .next_tick = undefined,
            };
        }

        pub fn start(
            it: *TableDataIterator,
            context: Context,
        ) void {
            assert(it.callback == .none);
            assert(context.addresses.len == context.checksums.len);

            it.* = .{
                .context = context,
                .callback = .none,
                .read = undefined,
                .next_tick = undefined,
            };
        }

        pub fn empty(it: *const TableDataIterator) bool {
            assert(it.context.addresses.len == it.context.checksums.len);
            return it.context.addresses.len == 0;
        }

        /// Calls `callback` with either the next data block or null.
        /// The block is only valid for the duration of the callback.
        pub fn next(it: *TableDataIterator, callback: Callback) void {
            assert(it.callback == .none);
            assert(it.context.addresses.len == it.context.checksums.len);

            if (it.context.addresses.len > 0) {
                const index: usize = switch (it.context.direction) {
                    .ascending => 0,
                    .descending => it.context.addresses.len - 1,
                };

                assert(it.context.checksums[index].padding == 0);

                it.callback = .{ .read = callback };
                it.context.grid.read_block(
                    .{ .from_local_or_global_storage = on_read },
                    &it.read,
                    it.context.addresses[index],
                    it.context.checksums[index].value,
                    .{ .cache_read = true, .cache_write = true },
                );
            } else {
                it.callback = .{ .next_tick = callback };
                it.context.grid.on_next_tick(on_next_tick, &it.next_tick);
            }
        }

        fn on_read(read: *Grid.Read, block: BlockPtrConst) void {
            const it = @fieldParentPtr(TableDataIterator, "read", read);
            assert(it.callback == .read);
            assert(it.context.addresses.len == it.context.checksums.len);

            const callback = it.callback.read;
            it.callback = .none;

            switch (it.context.direction) {
                .ascending => {
                    if (constants.verify) {
                        const header = schema.header_from_block(block);
                        assert(header.address == it.context.addresses[0]);
                        assert(header.checksum == it.context.checksums[0].value);
                    }

                    it.context.addresses = it.context.addresses[1..];
                    it.context.checksums = it.context.checksums[1..];
                },
                .descending => {
                    const index_last = it.context.checksums.len - 1;
                    if (constants.verify) {
                        const header = schema.header_from_block(block);
                        assert(header.address == it.context.addresses[index_last]);
                        assert(header.checksum == it.context.checksums[index_last].value);
                    }

                    it.context.addresses = it.context.addresses[0..index_last];
                    it.context.checksums = it.context.checksums[0..index_last];
                },
            }

            assert(it.context.addresses.len == it.context.checksums.len);
            callback(it, block);
        }

        fn on_next_tick(next_tick: *Grid.NextTick) void {
            const it = @fieldParentPtr(TableDataIterator, "next_tick", next_tick);
            assert(it.callback == .next_tick);
            assert(it.empty());

            const callback = it.callback.next_tick;
            it.callback = .none;
            callback(it, null);
        }
    };
}
