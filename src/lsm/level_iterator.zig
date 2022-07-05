const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const config = @import("../config.zig");

const TableIteratorType = @import("table_iterator.zig").TableIteratorType;
const RingBuffer = @import("../ring_buffer.zig").RingBuffer;
const ManifestType = @import("manifest.zig").ManifestType;
const GridType = @import("grid.zig").GridType;

pub fn LevelIteratorType(comptime Table: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const key_from_value = Table.key_from_value;

    return struct {
        const LevelIterator = @This();
        const TableIterator = TableIteratorType(Table);

        const Grid = GridType(Table.Storage);
        const Manifest = ManifestType(Table);

        const TableInfo = Manifest.TableInfo;
        const TableInfoCallback = fn (it: *LevelIterator, table: *const TableInfo) void;

        const TableIteratorScope = struct {
            it: *LevelIterator = undefined,
            table_iterator: TableIterator,
        };

        const ValuesRingBuffer = RingBuffer(Value, Table.data.value_count_max, .pointer);
        const TablesRingBuffer = RingBuffer(TableIteratorScope, 2, .array);

        grid: *Grid,
        manifest: *Manifest,
        callback: fn (*LevelIterator) void,
        table_info_callback: TableInfoCallback,
        level: u8,

        /// The key_max (when .ascending) or key_min (when .descending) of the last table iterated.
        /// This enables us to get the next table from the manifest.
        key: ?Key = null,
        /// A lower bound on the iteration range.
        key_min: Key,
        /// An upper bound on the iteration range.
        key_max: Key,

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
                .table_info_callback = undefined,
                .level = undefined,
                .key_min = undefined,
                .key_max = undefined,
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
            key_min: Key,
            key_max: Key,
            table_info_callback: TableInfoCallback,
        };

        pub fn start(it: *LevelIterator, context: Context, callback: fn (*LevelIterator) void) void {
            it.* = .{
                .grid = context.grid,
                .manifest = context.manifest,
                .callback = callback,
                .table_info_callback = context.table_info_callback,
                .level = context.level,
                .key_min = context.key_min,
                .key_max = context.key_max,
                .values = .{ .buffer = it.values.buffer },
                .tables = .{ .buffer = it.tables.buffer },
            };

            assert(it.key == null);
            assert(it.values.empty());
            assert(it.tables.empty());
        }

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

            const table_iterator = it.tables_advance_tail();
            assert(it.tables.tail_ptr().?.it == it);

            var table = it.get_next_table_info();
            it.table_info_callback(it, table);

            const table_iterator_context = .{
                .grid = it.grid,
                .address = table.address,
                .checksum = table.checksum,
            };
            table_iterator.start(table_iterator_context, table_iterator_callback);

            assert(table_iterator.tick());
            return true;
        }

        fn tables_advance_tail(it: *LevelIterator) *TableIterator {
            if (it.tables.next_tail_ptr() == null) {
                const table = &it.tables.head_ptr().?.table_iterator;
                while (table.peek() != null) {
                    it.values.push(table.pop()) catch unreachable;
                }
                it.tables.advance_head();
            }

            const scope = it.tables.next_tail_ptr().?;
            it.tables.advance_tail();
            scope.it = it;
            return &scope.table_iterator;
        }

        fn get_next_table_info(it: *LevelIterator) *const TableInfo {
            _ = it;
            @panic("TODO(Joran): find next TableInfo using it.(level|key_min|key_max)");
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
            _ = it;

            // TODO look at the manifest to determine this.
            return false;
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

        pub fn peek(it: LevelIterator) ?Key {
            if (it.values.head_ptr_const()) |value| return key_from_value(value);

            const scope = it.tables.head_ptr_const() orelse {
                assert(it.buffered_all_values());
                return null;
            };
            return scope.table_iterator.peek().?;
        }

        /// This may only be called after peek() has returned non-null.
        pub fn pop(it: *LevelIterator) Value {
            if (it.values.pop()) |value| return value;

            const table_iterator = &it.tables.head_ptr().?.table_iterator;
            const value = table_iterator.pop();
            if (table_iterator.peek() == null) it.tables.advance_head();
            return value;
        }
    };
}
