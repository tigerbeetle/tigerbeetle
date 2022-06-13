const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const config = @import("../config.zig");

const TableIteratorType = @import("table_iterator.zig").TableIterator;
const RingBuffer = @import("../ring_buffer.zig").RingBuffer;
const GridType = @import("grid.zig").GridType;

fn LevelIteratorType(comptime Table: type, comptime Parent: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const key_from_value = Table.key_from_value;

    return struct {
        const LevelIterator = @This();
        const TableIterator = TableIteratorType(Table, LevelIterator);

        const Grid = GridType(Table.Storage);

        const ValuesRingBuffer = RingBuffer(Value, Table.data.value_count_max, .pointer);
        const TablesRingBuffer = RingBuffer(TableIterator, 2, .array);

        grid: *Grid,
        parent: *Parent,
        read_done: fn (*Parent) void,
        level: u32,
        key_min: Key,
        key_max: Key,
        values: ValuesRingBuffer,
        tables: TablesRingBuffer,

        pub fn init(allocator: mem.Allocator, read_done: fn (*Parent) void) !LevelIterator {
            var values = try ValuesRingBuffer.init(allocator);
            errdefer values.deinit(allocator);

            var table_a = try TableIterator.init(allocator, on_read_done);
            errdefer table_a.deinit(allocator);

            var table_b = try TableIterator.init(allocator, on_read_done);
            errdefer table_b.deinit(allocator);

            return LevelIterator{
                .grid = undefined,
                .parent = undefined,
                .read_done = read_done,
                .level = undefined,
                .key_min = undefined,
                .key_max = undefined,
                .values = values,
                .tables = .{
                    .buffer = .{
                        table_a,
                        table_b,
                    },
                },
            };
        }

        pub fn deinit(it: *LevelIterator, allocator: mem.Allocator) void {
            it.values.deinit(allocator);
            for (it.tables.buffer) |*table| table.deinit(allocator);
            it.* = undefined;
        }

        fn reset(
            it: *LevelIterator,
            grid: *Grid,
            parent: *Parent,
            level: u32,
            key_min: Key,
            key_max: Key,
        ) void {
            it.* = .{
                .grid = grid,
                .parent = parent,
                .read_done = it.read_done,
                .level = level,
                .key_min = key_min,
                .key_max = key_max,
                .values = .{ .buffer = it.values.buffer },
                .tables = .{ .buffer = it.tables.buffer },
            };
            assert(it.values.empty());
            assert(it.tables.empty());
        }

        pub fn tick(it: *LevelIterator) bool {
            if (it.buffered_enough_values()) return false;

            if (it.tables.tail_ptr()) |tail| {
                // Buffer values as necessary for the current tail.
                if (tail.tick()) return true;
                // Since buffered_enough_values() was false above and tick did not start
                // new I/O, the tail table must have already buffered all values.
                // This is critical to ensure no values are skipped during iteration.
                assert(tail.buffered_all_values());
            }

            if (it.tables.next_tail_ptr()) |next_tail| {
                read_next_table(next_tail);
                it.tables.advance_tail();
                return true;
            } else {
                const table = it.tables.head_ptr().?;
                while (table.peek() != null) {
                    it.values.push(table.pop()) catch unreachable;
                }
                it.tables.advance_head();

                read_next_table(it.tables.next_tail_ptr().?);
                it.tables.advance_tail();
                return true;
            }
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

        fn on_read_done(it: *LevelIterator) void {
            if (!it.tick()) {
                assert(it.buffered_enough_values());
                it.read_done(it.parent);
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

            const table = it.tables.head_ptr_const() orelse {
                assert(it.buffered_all_values());
                return null;
            };

            return table.peek().?;
        }

        /// This is only safe to call after peek() has returned non-null.
        pub fn pop(it: *LevelIterator) Value {
            if (it.values.pop()) |value| return value;

            const table = it.tables.head_ptr().?;
            const value = table.pop();

            if (table.peek() == null) it.tables.advance_head();

            return value;
        }
    };
}
