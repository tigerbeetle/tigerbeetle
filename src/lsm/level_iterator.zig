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

        const TableIteratorRef = struct {
            table: TableIterator,
            level: *LevelIterator,
        };

        const ValuesRingBuffer = RingBuffer(Value, Table.data.value_count_max, .pointer);
        const TablesRingBuffer = RingBuffer(TableIteratorRef, 2, .array);

        grid: *Grid,
        read_done: fn (*LevelIterator) void = null,
        level: u8,
        key_min: Key,
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
                .read_done = undefined,
                .level = undefined,
                .key_min = undefined,
                .key_max = undefined,
                .values = values,
                .tables = .{
                    .buffer = .{
                        .{ .table = table_a, .level = undefined },
                        .{ .table = table_b, .level = undefined },
                    },
                },
            };
        }

        pub fn deinit(it: *LevelIterator, allocator: mem.Allocator) void {
            it.values.deinit(allocator);
            for (it.tables.buffer) |*table_ref| table_ref.table.deinit(allocator);
            it.* = undefined;
        }

        pub const Context = struct {
            level: u8,
            key_min: Key,
            key_max: Key,
        };

        pub fn reset(
            it: *LevelIterator,
            grid: *Grid,
            manifest: *Manifest,
            read_done: fn (*LevelIterator) void,
            context: Context,
        ) void {
            it.* = .{
                .grid = grid,
                .read_done = read_done,
                .level = context.level,
                .key_min = context.key_min,
                .key_max = context.key_max,
                .values = .{ .buffer = it.values.buffer },
                .tables = .{ .buffer = it.tables.buffer },
            };

            // TODO use correct context for TableIterator reset
            for (it.tables.buffer) |*table_ref| {
                table_ref.level = it;
                table_ref.table.reset(
                    grid,
                    manifest,
                    on_table_read_done,
                    TableIterator.Context{},
                );
            }

            assert(it.values.empty());
            assert(it.tables.empty());
        }

        pub fn tick(it: *LevelIterator) bool {
            if (it.buffered_enough_values()) return false;

            if (it.tables.tail_ptr()) |tail_ref| {
                const tail = &tail_ref.table;
                // Buffer values as necessary for the current tail.
                if (tail.tick()) return true;
                // Since buffered_enough_values() was false above and tick did not start
                // new I/O, the tail table must have already buffered all values.
                // This is critical to ensure no values are skipped during iteration.
                assert(tail.buffered_all_values());
            }

            if (it.tables.next_tail_ptr()) |next_tail_ref| {
                read_next_table(&next_tail_ref.table);
                it.tables.advance_tail();
                return true;
            }

            const table = &it.tables.head_ptr().?.table;
            while (table.peek() != null) {
                it.values.push(table.pop()) catch unreachable;
            }
            it.tables.advance_head();

            read_next_table(&it.tables.next_tail_ptr().?.table);
            it.tables.advance_tail();
            return true;
        }

        fn read_next_table(table: *TableIterator) void {
            // TODO Implement get_next_address()
            //const address = table.parent.manifest.get_next_address() orelse return false;
            if (true) @panic("implement get_next_address()");
            const address = 0;
            table.reset(address);
            const read_pending = table.tick();
            assert(read_pending);
        }

        fn on_table_read_done(table: *TableIterator) void {
            const table_ref = @fieldParentPtr(TableIteratorRef, "table", table);
            table_ref.level.on_read_done();
        }

        fn on_read_done(it: *LevelIterator) void {
            if (!it.tick()) {
                assert(it.buffered_enough_values());
                it.read_done(it);
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
            var tables_it = it.tables.iterator();
            while (tables_it.next()) |table| {
                value_count += table.buffered_value_count();
            }
            return value_count;
        }

        fn buffered_enough_values(it: LevelIterator) bool {
            return it.buffered_all_values() or
                it.buffered_value_count() >= Table.data.value_count_max;
        }

        pub fn peek(it: LevelIterator) ?Key {
            if (it.values.head()) |value| return key_from_value(value);

            const table_ref = it.tables.head_ptr_const() orelse {
                assert(it.buffered_all_values());
                return null;
            };

            return table_ref.table.peek().?;
        }

        /// This is only safe to call after peek() has returned non-null.
        pub fn pop(it: *LevelIterator) Value {
            if (it.values.pop()) |value| return value;

            const table = &it.tables.head_ptr().?.table;
            const value = table.pop();

            if (table.peek() == null) it.tables.advance_head();

            return value;
        }
    };
}
