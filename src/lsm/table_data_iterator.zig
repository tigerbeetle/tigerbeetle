const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const constants = @import("../constants.zig");

const stdx = @import("../stdx.zig");
const GridType = @import("grid.zig").GridType;

/// A TableDataIterator iterates a table's data blocks in ascending key order.
pub fn TableDataIteratorType(comptime Storage: type) type {
    return struct {
        const TableDataIterator = @This();

        const Grid = GridType(Storage);
        const BlockPtrConst = Grid.BlockPtrConst;

        pub const Callback = fn (it: *TableDataIterator, data_block: ?BlockPtrConst) void;

        pub const Context = struct {
            grid: *Grid,
            /// Table data block addresses.
            addresses: []const u64,
            /// Table data block checksums.
            checksums: []const u128,
            read_name: [*:0]const u8,
        };

        context: Context,

        callback: union(enum) {
            none,
            read: Callback,
            next_tick: Callback,
        },

        read: Grid.Read,
        next_tick: Grid.NextTick,

        pub fn init(allocator: mem.Allocator) !TableDataIterator {
            _ = allocator; // TODO(jamii) Will need this soon for pipelining.
            return TableDataIterator{
                .context = .{
                    .grid = undefined,
                    // The zero-init here is important.
                    // In other places we assume that we can call `next` on a fresh TableDataIterator
                    // and get `null` rather than UB.
                    .addresses = &.{},
                    .checksums = &.{},
                    .read_name = "FORGOT TO INIT",
                },
                .callback = .none,
                .read = undefined,
                .next_tick = undefined,
            };
        }

        pub fn deinit(it: *TableDataIterator, allocator: mem.Allocator) void {
            _ = allocator; // TODO(jamii) Will need this soon for pipelining.
            it.* = undefined;
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

            if (it.context.addresses.len > 0) {
                const address = it.context.addresses[0];
                const checksum = it.context.checksums[0];
                it.callback = .{ .read = callback };
                it.context.grid.read_block(on_read, &it.read, address, checksum, .data, it.context.read_name);
            } else {
                it.callback = .{ .next_tick = callback };
                it.context.grid.on_next_tick(on_next_tick, &it.next_tick, .main_thread);
            }
        }

        fn on_read(read: *Grid.Read, block: Grid.BlockPtrConst) void {
            const it = @fieldParentPtr(TableDataIterator, "read", read);
            assert(it.callback == .read);

            const callback = it.callback.read;
            it.callback = .none;
            it.context.addresses = it.context.addresses[1..];
            it.context.checksums = it.context.checksums[1..];

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
