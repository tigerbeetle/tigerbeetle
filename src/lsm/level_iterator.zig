const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const config = @import("../config.zig");

const Direction = @import("direction.zig").Direction;
const TableIteratorType = @import("table_iterator.zig").TableIteratorType;
const RingBuffer = @import("../ring_buffer.zig").RingBuffer;
const ManifestType = @import("manifest.zig").ManifestType;
const GridType = @import("grid.zig").GridType;

/// A LevelIterator sequentially iterates the values of every table in the level.
pub fn LevelIteratorType(comptime Table: type, comptime Storage: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const key_from_value = Table.key_from_value;

    return struct {
        const LevelIterator = @This();
        const TableIterator = TableIteratorType(Table, Storage);

        const Grid = GridType(Storage);
        const Manifest = ManifestType(Table, Storage);

        const BlockPtrConst = *align(config.sector_size) const [config.block_size]u8;

        const TableInfo = Manifest.TableInfo;
        const TableInfoCallback = *const fn (
            it: *LevelIterator,
            table: *const TableInfo,
            index_block: BlockPtrConst,
        ) void;

        const TableIteratorScope = struct {
            it: *LevelIterator = undefined,
            table_iterator: TableIterator,
        };

        const ValuesRingBuffer = RingBuffer(Value, Table.data.value_count_max, .pointer);
        const TablesRingBuffer = RingBuffer(TableIteratorScope, 2, .array);

        grid: *Grid,
        manifest: *Manifest,
        callback: *const fn (*LevelIterator) void,
        table_info: ?*const TableInfo,
        table_info_callback: TableInfoCallback,
        level: u8,
        snapshot: u64,

        /// A lower bound on the iteration range.
        key_min: Key,

        /// An upper bound on the iteration range.
        key_max: Key,

        /// The key_max (when .ascending) or key_min (when .descending) of the last table iterated,
        /// used to get the next table from the manifest.
        key_exclusive: ?Key = null,

        // TODO LevelIterator needs to support .descending `direction` when used for range queries.
        direction: Direction,

        manifest_iterated: bool = false,

        /// Buffered values that precede the iterators in `tables`.
        values: ValuesRingBuffer,
        tables: TablesRingBuffer,

        pub fn init(allocator: mem.Allocator) !LevelIterator {
            var values = try ValuesRingBuffer.init(allocator);
            errdefer values.deinit(allocator);

            var table_a = try TableIterator.init(allocator);
            errdefer table_a.deinit(allocator);

            var table_b = try TableIterator.init(allocator);
            errdefer table_b.deinit(allocator);

            return LevelIterator{
                .grid = undefined,
                .manifest = undefined,
                .callback = undefined,
                .table_info = null,
                .table_info_callback = undefined,
                .level = undefined,
                .snapshot = undefined,
                .key_min = undefined,
                .key_max = undefined,
                .direction = undefined,
                .values = values,
                .tables = .{
                    .buffer = .{
                        .{ .table_iterator = table_a },
                        .{ .table_iterator = table_b },
                    },
                },
            };
        }

        pub fn deinit(it: *LevelIterator, allocator: mem.Allocator) void {
            it.values.deinit(allocator);
            for (it.tables.buffer) |*scope| scope.table_iterator.deinit(allocator);
            it.* = undefined;
        }

        pub const Context = struct {
            grid: *Grid,
            manifest: *Manifest,
            level: u8,
            snapshot: u64,
            key_min: Key,
            key_max: Key,
            direction: Direction,
            table_info_callback: TableInfoCallback,
        };

        pub fn start(
            it: *LevelIterator,
            context: Context,
            callback: *const fn (*LevelIterator) void,
        ) void {
            if (context.direction == .descending) {
                @panic("TODO Implement descending direction for LevelIterator.");
            }

            it.* = .{
                .grid = context.grid,
                .manifest = context.manifest,
                .callback = callback,
                .table_info = null,
                .table_info_callback = context.table_info_callback,
                .level = context.level,
                .snapshot = context.snapshot,
                .key_min = context.key_min,
                .key_max = context.key_max,
                .direction = context.direction,
                .values = .{ .buffer = it.values.buffer },
                .tables = .{ .buffer = it.tables.buffer },
            };

            assert(it.key_exclusive == null);
            assert(it.manifest_iterated == false);
            assert(it.values.empty());
            assert(it.tables.empty());
        }

        /// Returns true if an IO operation was started. If this returns true, then `callback()`
        /// will be called on completion.
        pub fn tick(it: *LevelIterator) bool {
            if (it.buffered_enough_values()) return false;

            if (it.tables.tail_ptr()) |scope| {
                assert(scope.it == it);

                const tail = &scope.table_iterator;
                // Buffer values as necessary for the current tail.
                if (tail.tick()) return true;
                // Since buffered_enough_values() was false above and tick did not start
                // new I/O, the tail table must have already buffered all values.
                // This is critical to ensure no values are skipped during iteration.
                assert(tail.buffered_all_values());
            }

            const table = it.next_table_info();
            if (table == null) return false;

            const table_iterator = it.next_table_iterator();
            assert(it.tables.tail_ptr().?.it == it);

            assert(it.table_info == null);
            it.table_info = table;

            const table_iterator_context = .{
                .grid = it.grid,
                .address = table.?.address,
                .checksum = table.?.checksum,
                .index_block_callback = table_iterator_index_callback,
            };
            table_iterator.start(table_iterator_context, table_iterator_callback);

            assert(table_iterator.tick());
            return true;
        }

        fn next_table_info(it: *LevelIterator) ?*const TableInfo {
            if (it.manifest_iterated) return null;

            const next_table = it.manifest.next_table(
                it.level,
                it.snapshot,
                it.key_min,
                it.key_max,
                it.key_exclusive,
                it.direction,
            );

            if (next_table) |table| {
                it.key_exclusive = switch (it.direction) {
                    .ascending => table.key_max,
                    .descending => table.key_min,
                };
                return table;
            } else {
                assert(!it.manifest_iterated);
                it.manifest_iterated = true;
                return null;
            }
        }

        fn next_table_iterator(it: *LevelIterator) *TableIterator {
            if (it.tables.full()) {
                const table = &it.tables.head_ptr().?.table_iterator;
                while (true) {
                    _ = table.peek() catch break;
                    it.values.push_assume_capacity(table.pop());
                }
                it.tables.advance_head();
            }

            const scope = it.tables.next_tail_ptr().?;
            it.tables.advance_tail();
            scope.it = it;
            return &scope.table_iterator;
        }

        fn table_iterator_index_callback(
            table_iterator: *TableIterator,
            index_block: BlockPtrConst,
        ) void {
            const scope = @fieldParentPtr(TableIteratorScope, "table_iterator", table_iterator);
            const it = scope.it;

            assert(it.table_info != null);
            const table = it.table_info.?;
            it.table_info = null;

            it.table_info_callback(it, table, index_block);
        }

        fn table_iterator_callback(table_iterator: *TableIterator) void {
            const scope = @fieldParentPtr(TableIteratorScope, "table_iterator", table_iterator);
            const it = scope.it;

            if (!it.tick()) {
                assert(it.buffered_enough_values());
                it.callback(it);
            }
        }

        /// Returns true if all remaining values in the level have been buffered.
        pub fn buffered_all_values(it: LevelIterator) bool {
            // Iterating the manifest is necessary but not sufficient since I/O may yet be required.
            if (!it.manifest_iterated) return false;

            var table_iterators = it.tables.iterator();
            while (table_iterators.next()) |scope| {
                if (!scope.table_iterator.buffered_all_values()) return false;
            }

            return true;
        }

        fn buffered_value_count(it: LevelIterator) u32 {
            var value_count = @intCast(u32, it.values.count);

            var table_iterators = it.tables.iterator();
            while (table_iterators.next()) |scope| {
                value_count += scope.table_iterator.buffered_value_count();
            }

            return value_count;
        }

        fn buffered_enough_values(it: LevelIterator) bool {
            return it.buffered_all_values() or
                it.buffered_value_count() >= Table.data.value_count_max;
        }

        /// Returns either:
        /// - the next Key, if available.
        /// - error.Empty when there are no values remaining to iterate.
        /// - error.Drained when the iterator isn't empty, but the values 
        ///   still need to be buffered into memory via tick().
        pub fn peek(it: LevelIterator) error{ Empty, Drained }!Key {
            if (it.values.head_ptr_const()) |value| return key_from_value(value);

            const scope = it.tables.head_ptr_const() orelse {
                // Even if there are no values available to peek, some may be unbuffered.
                // We call buffered_all_values() to distinguish between the iterator
                // being empty and needing to tick() to refill values.
                if (!it.buffered_all_values()) return error.Drained;
                return error.Empty;
            };

            return scope.table_iterator.peek();
        }

        /// This may only be called after peek() returns a Key (and not Empty or Drained)
        pub fn pop(it: *LevelIterator) Value {
            if (it.values.pop()) |value| return value;

            const table_iterator = &it.tables.head_ptr().?.table_iterator;
            const value = table_iterator.pop();

            _ = table_iterator.peek() catch |err| switch (err) {
                error.Empty => it.tables.advance_head(),
                error.Drained => {},
            };

            return value;
        }
    };
}
