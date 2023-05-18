const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const snapshot_latest = @import("tree.zig").snapshot_latest;
const TableSetType = @import("table_set.zig").TableSetType;

pub fn TableImmutableType(comptime Table: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const value_count_max = Table.value_count_max;

    return struct {
        const TableImmutable = @This();

        const Values = TableSetType(Table);

        snapshot_min: u64,
        values: Values,
        free: bool,

        pub fn init(allocator: mem.Allocator) !TableImmutable {
            var values = try Values.init(allocator);
            errdefer values.deinit(allocator);

            return TableImmutable{
                .snapshot_min = undefined,
                .values = values,
                .free = true,
            };
        }

        pub fn deinit(table: *TableImmutable, allocator: mem.Allocator) void {
            table.values.deinit(allocator);
        }

        pub inline fn key_range(table: *const TableImmutable) Values.KeyRange {
            assert(!table.free);
            assert(table.values.count() > 0);
            return table.values.key_range() orelse unreachable;
        }

        pub inline fn count(table: *const TableImmutable) u32 {
            const value_count = table.values.count();
            assert(value_count <= value_count_max);
            return value_count;
        }

        pub fn clear(table: *TableImmutable) void {
            table.snapshot_min = undefined;
            table.values.clear();
            table.free = true;
        }

        pub fn reset_with_sorted_values(table: *TableImmutable, snapshot_min: u64) void {
            assert(table.free);
            assert(snapshot_min > 0);
            assert(snapshot_min < snapshot_latest);

            assert(table.values.count() > 0);
            assert(table.values.count() <= value_count_max);
            assert(table.values.count() <= Table.data.block_value_count_max * Table.data_block_count_max);

            table.snapshot_min = snapshot_min;
            table.free = false;
        }

        // TODO(ifreund) This would be great to unit test.
        pub fn get(table: *const TableImmutable, key: Key) ?*const Value {
            assert(!table.free);
            return table.values.find(key);
        }
    };
}
