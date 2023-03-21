const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const constants = @import("../constants.zig");

const Direction = @import("direction.zig").Direction;
const ManifestType = @import("manifest.zig").ManifestType;
const GridType = @import("grid.zig").GridType;

/// A LevelIterator iterates the index blocks of every table in a key range.
pub fn LevelIteratorType(comptime Table: type, comptime Storage: type) type {
    return struct {
        const LevelIterator = @This();
        const Key = Table.Key;
        const Grid = GridType(Storage);
        const BlockPtrConst = Grid.BlockPtrConst;
        const Manifest = ManifestType(Table, Storage);
        const TableInfo = Manifest.TableInfo;

        pub const Context = struct {
            grid: *Grid,
            manifest: *Manifest,
            level: u8,
            snapshot: u64,
            key_min: Key,
            key_max: Key,
            direction: Direction,
        };

        pub const Callback = fn (
            it: *LevelIterator,
            table_info: ?TableInfo,
            index_block: ?BlockPtrConst,
        ) void;

        /// Passed by `start`.
        context: Context,

        /// The key_max (when .ascending) or key_min (when .descending) of the last table iterated.
        /// Used to get the next table from the manifest.
        key_exclusive: ?Key,

        callback: union(enum) {
            none,
            read: struct {
                callback: Callback,
                table_info: TableInfo,
            },
            next_tick: Callback,
        },

        read: Grid.Read = undefined,
        next_tick: Grid.NextTick = undefined,

        pub fn init(allocator: mem.Allocator) !LevelIterator {
            _ = allocator; // TODO(jamii) Will need this soon for pipelining.
            return LevelIterator{
                .context = undefined,
                .key_exclusive = null,
                .callback = .none,
            };
        }

        pub fn deinit(it: *LevelIterator, allocator: mem.Allocator) void {
            _ = allocator; // TODO(jamii) Will need this soon for pipelining.
            it.* = undefined;
        }

        pub fn start(it: *LevelIterator, context: Context) void {
            assert(it.callback == .none);
            if (context.direction == .descending) {
                @panic("TODO Implement descending direction for LevelIterator.");
            }

            it.* = .{
                .context = context,
                .key_exclusive = null,
                .callback = .none,
            };
        }

        pub fn next(it: *LevelIterator, callback: Callback) void {
            assert(it.callback == .none);

            // NOTE We must ensure that between calls to `next`,
            //      no changes are made to the manifest that are visible to `it.context.snapshot`.
            const next_table_info = it.context.manifest.next_table(
                it.context.level,
                it.context.snapshot,
                it.context.key_min,
                it.context.key_max,
                it.key_exclusive,
                it.context.direction,
            );
            if (next_table_info) |table_info| {
                it.key_exclusive = switch (it.context.direction) {
                    .ascending => table_info.key_max,
                    .descending => table_info.key_min,
                };
                it.callback = .{
                    .read = .{
                        .callback = callback,
                        // Copy table_info so we can hold on to it across `read_block`.
                        .table_info = table_info.*,
                    },
                };
                it.context.grid.read_block(
                    on_read,
                    &it.read,
                    table_info.address,
                    table_info.checksum,
                    .index,
                );
            } else {
                it.callback = .{ .next_tick = callback };
                it.context.grid.on_next_tick(on_next_tick, &it.next_tick);
            }
        }

        fn on_read(read: *Grid.Read, block: Grid.BlockPtrConst) void {
            const it = @fieldParentPtr(LevelIterator, "read", read);
            assert(it.callback == .read);

            const callback = it.callback.read.callback;
            const table_info = it.callback.read.table_info;
            it.callback = .none;

            callback(it, table_info, block);
        }

        fn on_next_tick(next_tick: *Grid.NextTick) void {
            const it = @fieldParentPtr(LevelIterator, "next_tick", next_tick);
            assert(it.callback == .next_tick);

            const callback = it.callback.next_tick;
            it.callback = .none;
            callback(it, null, null);
        }
    };
}
