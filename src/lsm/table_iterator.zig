const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const constants = @import("../constants.zig");

const stdx = @import("../stdx.zig");
const GridType = @import("grid.zig").GridType;
const alloc_block = @import("grid.zig").alloc_block;

/// A TableIterator iterates a table's data blocks in ascending-key order.
pub fn TableIteratorType(comptime Storage: type) type {
    return struct {
        const TableIterator = @This();

        const Grid = GridType(Storage);
        const BlockPtr = Grid.BlockPtr;
        const BlockPtrConst = Grid.BlockPtrConst;

        pub const Callback = fn (it: *TableIterator, data_block: ?BlockPtrConst) void;

        pub const Context = struct {
            grid: *Grid,
            /// Table data block addresses.
            addresses: []const u64,
            /// Table data block checksums.
            checksums: []const u128,
        };

        // Allocated during `init`.
        data_block: BlockPtr,

        // Passed by `start`.
        context: Context,

        // Internal state.
        callback: union(enum) {
            none,
            read: Callback,
            next_tick: Callback,
        },

        read: Grid.Read,
        next_tick: Grid.NextTick,

        pub fn init(allocator: mem.Allocator) !TableIterator {
            const data_block = try alloc_block(allocator);
            errdefer allocator.free(data_block);

            return TableIterator{
                .data_block = data_block,
                .context = .{
                    .grid = undefined,
                    // The zero-init here is important.
                    // In other places we assume that we can call `next` on a fresh TableIterator
                    // and get `null` rather than UB.
                    .addresses = &.{},
                    .checksums = &.{},
                },
                .callback = .none,
                .read = undefined,
                .next_tick = undefined,
            };
        }

        pub fn deinit(it: *TableIterator, allocator: mem.Allocator) void {
            allocator.free(it.data_block);
            it.* = undefined;
        }

        pub fn start(
            it: *TableIterator,
            context: Context,
        ) void {
            assert(it.callback == .none);
            assert(context.addresses.len == context.checksums.len);

            it.* = .{
                .data_block = it.data_block,
                .context = context,
                .callback = .none,
                .read = undefined,
                .next_tick = undefined,
            };
        }

        pub fn empty(it: *const TableIterator) bool {
            return it.context.addresses.len == 0;
        }

        pub fn next(it: *TableIterator, callback: Callback) void {
            assert(it.callback == .none);

            if (it.context.addresses.len > 0) {
                const address = it.context.addresses[0];
                const checksum = it.context.checksums[0];
                it.callback = .{ .read = callback };
                it.context.grid.read_block(
                    .{ .block = it.data_block },
                    on_read,
                    &it.read,
                    address,
                    checksum,
                    .data,
                );
            } else {
                it.callback = .{ .next_tick = callback };
                it.context.grid.on_next_tick(on_next_tick, &it.next_tick);
            }
        }

        fn on_read(read: *Grid.Read, data_block: Grid.BlockPtrConst) void {
            const it = @fieldParentPtr(TableIterator, "read", read);
            assert(it.callback == .read);

            const callback = it.callback.read;
            it.callback = .none;
            it.context.addresses = it.context.addresses[1..];
            it.context.checksums = it.context.checksums[1..];

            callback(it, data_block);
        }

        fn on_next_tick(next_tick: *Grid.NextTick) void {
            const it = @fieldParentPtr(TableIterator, "next_tick", next_tick);
            assert(it.callback == .next_tick);

            const callback = it.callback.next_tick;
            it.callback = .none;
            callback(it, null);
        }
    };
}
